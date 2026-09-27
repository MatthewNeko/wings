// Package decompressor tracks batches of archive extractions so the panel can
// show progress while several archives are being unpacked at once. It mirrors
// the shape of router/downloader progress events, but keeps its own registry
// since a batch has a very different lifecycle from a remote download.
package decompressor

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"emperror.dev/errors"
	"github.com/apex/log"
	"github.com/google/uuid"

	"github.com/pterodactyl/wings/server"
)

// Status values reported through the progress endpoints.
const (
	StatusPending   = "pending"
	StatusRunning   = "running"
	StatusCompleted = "completed"
	StatusFailed    = "failed"
	StatusCancelled = "cancelled"
)

// retainFinished is how long a finished batch stays queryable. This gives the
// panel time to read the final state before it disappears from memory.
const retainFinished = 3 * time.Minute

// FileState is the progress of a single archive within a batch.
type FileState struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	// Bytes written to the filesystem so far for this archive.
	Bytes int64 `json:"bytes"`
	// Total is the expected uncompressed size, or 0 when it could not be
	// determined before extraction started.
	Total int64  `json:"total"`
	Error string `json:"error,omitempty"`
}

// BatchState is the snapshot handed back to anyone polling for progress.
type BatchState struct {
	ID         string       `json:"id"`
	Root       string       `json:"root"`
	Status     string       `json:"status"`
	File       string       `json:"file_name"`
	FileIndex  int          `json:"file_index"`
	TotalFiles int          `json:"total_files"`
	Bytes      int64        `json:"bytes"`
	Total      int64        `json:"total_bytes"`
	Progress   float64      `json:"progress"`
	Speed      int64        `json:"speed"`
	Error      string       `json:"error,omitempty"`
	Files      []*FileState `json:"files"`
	Timestamp  int64        `json:"timestamp"`
	ElapsedSec int64        `json:"elapsed_seconds"`
}

// Batch is a single running (or finished) group of extractions.
type Batch struct {
	id       string
	serverID string
	root     string

	// ctx is canceled when the batch is abandoned so the filesystem layer stops
	// reading the archive, and done is what the extraction callbacks watch.
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	mu     sync.Mutex
	files  []*FileState
	index  int
	status string
	err    string
	// stopped records that a cancel was requested, so the extraction loop can
	// wind down exactly once and report the right final status.
	stopped    bool
	stopStatus string
	startedAt  time.Time
	finishedAt time.Time
	lastAt     time.Time
	lastBytes  int64
	speed      int64
}

// manager holds every batch this daemon knows about.
type manager struct {
	mu      sync.RWMutex
	batches map[string]*Batch
}

var instance = &manager{batches: make(map[string]*Batch)}

// archiveExtractor is the slice of the server filesystem this package needs to
// unpack archives. It exists so the batch bookkeeping can be tested without
// standing up a real server environment.
type archiveExtractor interface {
	UncompressedSize(ctx context.Context, dir string, file string) (int64, error)
	DecompressFileWithProgress(ctx context.Context, dir string, file string, written func(n int64), cancel <-chan struct{}) error
}

// Start begins extracting names inside root and returns as soon as the batch is
// registered. The extraction itself runs in the background.
//
// It deliberately does not hang off the request context: a batch has to keep
// going after the panel stops watching it, otherwise a slow HTTP connection
// would leave archives half extracted.
func Start(s *server.Server, root string, names []string) *Batch {
	b := newBatch(s.ID(), root, names)

	instance.add(b)

	go b.run(s.Filesystem())

	return b
}

// newBatch returns a batch queued up for the given list of archives.
func newBatch(serverID string, root string, names []string) *Batch {
	ctx, cancel := context.WithCancel(context.Background())

	b := &Batch{
		id:        uuid.New().String(),
		serverID:  serverID,
		root:      root,
		ctx:       ctx,
		cancel:    cancel,
		done:      make(chan struct{}),
		status:    StatusPending,
		startedAt: time.Now(),
		lastAt:    time.Now(),
	}
	for _, n := range names {
		b.files = append(b.files, &FileState{Name: n, Status: StatusPending})
	}

	return b
}

// Get returns a batch by ID, or nil when it is not being tracked.
func Get(id string) *Batch { return instance.get(id) }

// List returns snapshots of every batch belonging to a server, newest first.
func List(serverID string) []*BatchState { return instance.list(serverID) }

// Cancel asks a running batch to stop. It reports whether the batch existed.
func Cancel(id string) bool {
	b := instance.get(id)
	if b == nil {
		return false
	}

	b.stop(StatusCancelled)

	return true
}

// ID returns the tracking identifier for this batch.
func (b *Batch) ID() string { return b.id }

// ServerID returns the UUID of the server the archives belong to.
func (b *Batch) ServerID() string { return b.serverID }

func (b *Batch) run(x archiveExtractor) {
	// A panic in one of the archive readers should not take the whole daemon
	// down with it, so surface it as a failure on the batch instead.
	defer func() {
		if r := recover(); r != nil {
			log.WithField("batch_id", b.id).WithField("panic", r).Error("panic while decompressing a batch")
			b.finish(StatusFailed, fmt.Errorf("unexpected error while decompressing: %v", r))
		}
	}()

	lg := log.WithField("batch_id", b.id)

	for i := 0; i < len(b.files); i++ {
		if !b.begin(i) {
			return
		}

		name := b.files[i].Name

		// The uncompressed size is only knowable by looking inside the archive,
		// which is skipped for anything that takes too long to enumerate. A zero
		// total just means progress falls back to counting finished archives.
		if total, err := x.UncompressedSize(b.ctx, b.root, name); err == nil && total > 0 {
			b.setTotal(i, total)
		}

		err := x.DecompressFileWithProgress(b.ctx, b.root, name, func(n int64) {
			b.addBytes(i, n)
		}, b.done)
		if err == nil {
			b.complete(i)

			continue
		}

		lg.WithField("file", name).WithField("error", err).Warn("failed to decompress file in batch")

		// Cancellation is the only condition that stops the batch: every other
		// failure is recorded against that one archive so the rest still get
		// their turn. A full disk, for example, fails each remaining archive in
		// turn, which tells the user exactly what did and did not unpack.
		if errors.Is(err, context.Canceled) {
			b.finish(StatusCancelled, nil)

			return
		}

		b.failFile(i, err.Error())
	}

	b.finish(StatusCompleted, nil)
}

// begin marks the archive at index as running. It returns false when the batch
// was cancelled before this file got a chance to start.
func (b *Batch) begin(index int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.stopped {
		b.finishLocked(b.stopStatus, nil)

		return false
	}

	b.status = StatusRunning
	b.index = index
	b.files[index].Status = StatusRunning

	return true
}

func (b *Batch) setTotal(index int, total int64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.files[index].Total = total
}

// addBytes accounts newly written bytes and refreshes the rolling speed.
func (b *Batch) addBytes(index int, n int64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.stopped {
		return
	}

	b.files[index].Bytes += n

	if now := time.Now(); now.Sub(b.lastAt) >= time.Second {
		seconds := now.Sub(b.lastAt).Seconds()
		b.speed = int64(float64(b.files[index].Bytes-b.lastBytes) / seconds)
		b.lastAt = now
		b.lastBytes = b.files[index].Bytes
	}
}

func (b *Batch) complete(index int) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Snap the counter to the full size so an archive whose entries were
	// skipped (ignored paths, for example) still finishes at 100%.
	if f := b.files[index]; f.Total > f.Bytes {
		f.Bytes = f.Total
	}
	b.files[index].Status = StatusCompleted
}

func (b *Batch) failFile(index int, msg string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.files[index].Status = StatusFailed
	b.files[index].Error = msg
}

// stop cancels a batch and, when it was still in progress, records why the
// remaining archives were not extracted.
//
// It only signals the extraction loop and never sets the final status itself:
// whoever is actually running the batch closes it out, which keeps a cancel that
// lands after completion from rewriting history.
func (b *Batch) stop(status string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.stopped || !b.isActive() {
		return
	}

	b.stopped = true
	b.stopStatus = status

	close(b.done)
	b.cancel()
}

func (b *Batch) finish(status string, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.finishLocked(status, err)
}

// finishLocked records the outcome of a batch. Callers must hold the lock.
func (b *Batch) finishLocked(status string, err error) {
	if !b.isActive() {
		return
	}

	cancelled := status == StatusCancelled
	for _, f := range b.files {
		if f.Status == StatusPending || f.Status == StatusRunning {
			if cancelled {
				f.Status = StatusCancelled
			} else {
				f.Status = StatusFailed
			}
		}
	}

	b.status = status
	b.speed = 0
	if err != nil {
		b.err = err.Error()
	}
	if b.finishedAt.IsZero() {
		b.finishedAt = time.Now()
	}
}

// isActive reports whether the batch can still make progress. Callers must hold
// the lock.
func (b *Batch) isActive() bool {
	return b.status == StatusPending || b.status == StatusRunning
}

// State returns a snapshot of the batch safe for concurrent readers.
func (b *Batch) State() *BatchState {
	b.mu.Lock()
	defer b.mu.Unlock()

	state := &BatchState{
		ID:         b.id,
		Root:       b.root,
		Status:     b.status,
		FileIndex:  b.index,
		TotalFiles: len(b.files),
		Error:      b.err,
		Timestamp:  time.Now().Unix(),
		Speed:      b.speed,
	}

	var completed int
	for i, f := range b.files {
		c := *f
		state.Files = append(state.Files, &c)

		state.Bytes += c.Bytes
		state.Total += c.Total

		if c.Status == StatusCompleted || c.Status == StatusFailed {
			completed++
		}
		if i == b.index && (c.Status == StatusRunning || c.Status == StatusPending) {
			state.File = c.Name
		}
	}

	// Prefer byte based progress, but fall back to counting finished archives
	// whenever the uncompressed totals could not be determined up front.
	if state.Total > 0 {
		state.Progress = float64(state.Bytes) / float64(state.Total)
	} else if state.TotalFiles > 0 {
		state.Progress = float64(completed) / float64(state.TotalFiles)
	}
	if state.Progress > 1 {
		state.Progress = 1
	}
	if b.status == StatusCompleted {
		state.Progress = 1
	}

	if !b.finishedAt.IsZero() {
		state.ElapsedSec = int64(b.finishedAt.Sub(b.startedAt).Round(time.Second).Seconds())
	} else {
		state.ElapsedSec = int64(time.Since(b.startedAt).Round(time.Second).Seconds())
	}

	return state
}

func (m *manager) add(b *Batch) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.prune()
	m.batches[b.id] = b
}

func (m *manager) get(id string) *Batch {
	// Progress polling lands here, so this is also where finished batches that
	// outlived their retention window get dropped again.
	m.mu.Lock()
	defer m.mu.Unlock()

	m.prune()

	return m.batches[id]
}

func (m *manager) list(serverID string) []*BatchState {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.prune()

	states := make([]*BatchState, 0, len(m.batches))
	for _, b := range m.batches {
		if b.serverID == serverID {
			states = append(states, b.State())
		}
	}

	sort.Slice(states, func(i, j int) bool { return states[i].Timestamp > states[j].Timestamp })

	return states
}

// prune drops batches that have been finished for longer than the retention
// window. Callers must hold the write lock.
func (m *manager) prune() {
	for id, b := range m.batches {
		b.mu.Lock()
		finished := b.finishedAt
		b.mu.Unlock()

		if !finished.IsZero() && time.Since(finished) > retainFinished {
			delete(m.batches, id)
		}
	}
}
