package externalbackup

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/pterodactyl/wings/remote"
)

// watcher counts bytes as they leave the temporary archive. It is also the only
// place a cancelled run gets noticed while data is still flowing, because
// neither the rate limiter nor the protocol adapters poll the context.
type watcher struct {
	ctx    context.Context
	r      io.Reader
	onRead func(n int64)
}

func (w *watcher) Read(p []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}

	// The rate limiter can park a read until tokens arrive. Keeping the chunks
	// small bounds how long a cancelled task lingers before it really stops.
	if len(p) > 32*1024 {
		p = p[:32*1024]
	}

	n, err := w.r.Read(p)
	if n > 0 && w.onRead != nil {
		w.onRead(int64(n))
	}

	return n, err
}

// setSourceTotal records how much data the archive is expected to consume.
func (t *Task) setSourceTotal(total int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if total > 0 {
		t.sourceTotal = total
	}
}

func (t *Task) addSource(n int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.archiveBytes += n
	if t.archiveBytes > t.sourceTotal {
		// The total is an estimate, so let it grow rather than pin the bar at 100
		// while the archive is still being written.
		t.sourceTotal = t.archiveBytes
	}
}

func (t *Task) addUploaded(n int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.uploadedBytes += n
}

// setPhase moves the task to a new phase and publishes the sample immediately so
// the panel sees the transition without waiting for the next tick.
func (t *Task) setPhase(status string, archiveBytes, uploadedBytes int64) {
	t.mu.Lock()

	t.status = status
	if archiveBytes > 0 {
		t.archiveBytes = archiveBytes
		t.sourceTotal = archiveBytes
	}
	if uploadedBytes > 0 {
		t.uploadedBytes = uploadedBytes
	}

	t.mu.Unlock()

	t.sendStatus()
}

// percent maps the three phases onto one 0-100 bar. The panel only ever moves
// its own progress forward, so overshooting a boundary is harmless but going
// backwards would freeze the bar.
func (t *Task) percent() int {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.percentLocked()
}

func (t *Task) percentLocked() int {
	switch t.status {
	case StatusSuccess:
		return 100
	case StatusPruning:
		partial := archivingWeight + uploadingWeight + pruningWeight*0.5

		return int(partial)
	case StatusUploading:
		if t.archiveBytes <= 0 {
			return int(archivingWeight)
		}

		ratio := float64(t.uploadedBytes) / float64(t.archiveBytes)
		if ratio > 1 {
			ratio = 1
		}

		return int(archivingWeight + uploadingWeight*ratio)
	default:
		if t.sourceTotal <= 0 {
			return int(archivingWeight * 0.1)
		}

		ratio := float64(t.archiveBytes) / float64(t.sourceTotal)
		if ratio > 1 {
			ratio = 1
		}

		return int(archivingWeight * ratio)
	}
}

// sampleSpeed recomputes the throughput over the window since the last sample.
func (t *Task) sampleSpeed() int64 {
	now := time.Now()

	t.mu.Lock()
	defer t.mu.Unlock()

	elapsed := now.Sub(t.lastSampleAt).Seconds()
	if elapsed <= 0 {
		return t.speedKbps
	}

	delta := t.uploadedBytes - t.lastSampleVal
	if delta < 0 {
		delta = 0
	}

	t.speedKbps = int64(float64(delta) / elapsed / 1024)
	t.lastSampleAt = now
	t.lastSampleVal = t.uploadedBytes

	return t.speedKbps
}

func (t *Task) snapshot() remote.ExternalBackupStatus {
	t.mu.Lock()
	status := t.status
	archiveBytes := t.archiveBytes
	uploadedBytes := t.uploadedBytes
	message := t.err
	progress := t.percentLocked()
	t.mu.Unlock()

	if status == StatusFailed || status == StatusCancelled {
		progress = 100
	}

	return remote.ExternalBackupStatus{
		Status:        status,
		Progress:      progress,
		ArchiveBytes:  archiveBytes,
		UploadedBytes: uploadedBytes,
		SpeedKbps:     t.sampleSpeed(),
		Error:         message,
	}
}

// sendStatus pushes one sample to the panel. A failed report never aborts the
// run: the upload matters more than the progress bar.
func (t *Task) sendStatus() {
	data := t.snapshot()

	if err := t.client.SendExternalBackupStatus(t.ctx, t.serverID, t.runID, data); err != nil {
		t.logContext.WithField("error", err).Warn("externalbackup: failed to report progress to the panel")
	}
}

// reportLoop keeps the panel supplied with samples while the task works. It ends
// with the task, so it never outlives a finished run.
func (t *Task) reportLoop(wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(reportInterval)
	defer ticker.Stop()

	for {
		select {
		case <-t.done:
			return
		case <-ticker.C:
			t.sendStatus()
		}
	}
}

// finish is the single exit point: it records the terminal state, says so to the
// panel and to the log, and releases the cancellation.
func (t *Task) finish(status string, cause error) {
	t.mu.Lock()
	t.status = status
	if cause != nil {
		t.err = cause.Error()
	}
	t.mu.Unlock()

	t.sendStatus()

	if cause != nil {
		t.logContext.WithField("error", cause).WithField("status", status).
			Warn("external backup did not finish")
	} else {
		t.logContext.WithField("status", status).Info("external backup finished")
	}

	t.cancel()
}
