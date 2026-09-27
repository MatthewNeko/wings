package decompressor

import (
	"context"
	"sync"
	"testing"
	"time"

	"emperror.dev/errors"
)

// fakeExtractor stands in for the server filesystem so the batch bookkeeping can
// be exercised without a real server environment.
type fakeExtractor struct {
	totals map[string]int64
	errs   map[string]error

	// hold, when set, blocks every archive until the batch is cancelled, which
	// lets cancellation tests observe a stable mid-run state.
	hold chan string

	mu      sync.Mutex
	started []string
}

func (f *fakeExtractor) UncompressedSize(_ context.Context, _ string, file string) (int64, error) {
	return f.totals[file], nil
}

func (f *fakeExtractor) DecompressFileWithProgress(_ context.Context, _ string, file string, written func(int64), cancel <-chan struct{}) error {
	f.mu.Lock()
	f.started = append(f.started, file)
	f.mu.Unlock()

	if f.hold != nil {
		select {
		case <-cancel:
			return context.Canceled
		case f.hold <- file:
		}
	}

	if err := f.errs[file]; err != nil {
		return err
	}

	if total := f.totals[file]; total > 0 && written != nil {
		written(total)
	}

	return nil
}

// order returns the archives the extractor was asked to unpack, in order.
func (f *fakeExtractor) order() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]string(nil), f.started...)
}

// waitUntil polls cond until it passes, failing the test when it never does.
func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}

		time.Sleep(time.Millisecond)
	}

	t.Fatal("condition was not met before the timeout")
}

func TestBatchTracksProgressAcrossArchives(t *testing.T) {
	x := &fakeExtractor{totals: map[string]int64{"a.zip": 100, "b.zip": 60}}
	b := newBatch("server-uuid", "/", []string{"a.zip", "b.zip"})

	go b.run(x)
	waitUntil(t, func() bool { return b.State().Status == StatusCompleted })

	state := b.State()
	if state.Bytes != 160 || state.Total != 160 {
		t.Errorf("expected 160/160 bytes, got %d/%d", state.Bytes, state.Total)
	}
	if state.Progress != 1 {
		t.Errorf("expected a finished batch to report full progress, got %f", state.Progress)
	}
	if state.TotalFiles != 2 {
		t.Errorf("expected 2 archives in the batch, got %d", state.TotalFiles)
	}
	for _, f := range state.Files {
		if f.Status != StatusCompleted {
			t.Errorf("archive %s finished as %s", f.Name, f.Status)
		}
	}

	got := x.order()
	if len(got) != 2 || got[0] != "a.zip" || got[1] != "b.zip" {
		t.Errorf("archives were not extracted in the given order: %v", got)
	}
}

func TestBatchKeepsGoingAfterAFailedArchive(t *testing.T) {
	x := &fakeExtractor{
		totals: map[string]int64{"a.zip": 10, "b.zip": 10, "c.zip": 10},
		errs:   map[string]error{"b.zip": errors.New("boom")},
	}
	b := newBatch("server-uuid", "/", []string{"a.zip", "b.zip", "c.zip"})

	go b.run(x)
	waitUntil(t, func() bool { return b.State().Status == StatusCompleted })

	state := b.State()
	if len(state.Files) != 3 {
		t.Fatalf("expected 3 archives to be reported, got %d", len(state.Files))
	}

	expected := map[string]string{"a.zip": StatusCompleted, "b.zip": StatusFailed, "c.zip": StatusCompleted}
	for _, f := range state.Files {
		if want, ok := expected[f.Name]; !ok || f.Status != want {
			t.Errorf("archive %s reported %s, expected %s", f.Name, f.Status, expected[f.Name])
		}

		if f.Name == "b.zip" && f.Error != "boom" {
			t.Errorf("failure reason for b.zip was %q, expected the underlying error", f.Error)
		}
	}

	if got := len(x.order()); got != 3 {
		t.Errorf("expected every archive to be attempted, got %d", got)
	}
}

func TestBatchStopsWhenCancelled(t *testing.T) {
	x := &fakeExtractor{
		totals: map[string]int64{"a.zip": 10, "b.zip": 10},
		hold:   make(chan string),
	}
	b := newBatch("server-uuid", "/", []string{"a.zip", "b.zip"})

	go b.run(x)
	waitUntil(t, func() bool { return len(x.order()) == 1 })

	b.stop(StatusCancelled)
	waitUntil(t, func() bool { return b.State().Status == StatusCancelled })

	state := b.State()
	if got := len(x.order()); got != 1 {
		t.Errorf("cancelling should stop the queue, but %d archives were attempted", got)
	}
	for _, f := range state.Files {
		if f.Status != StatusCancelled {
			t.Errorf("archive %s finished as %s, expected it to be cancelled", f.Name, f.Status)
		}
	}

	// Cancelling twice must not rewrite the outcome of a finished batch.
	b.stop(StatusCancelled)
	if b.State().Status != StatusCancelled {
		t.Error("a finished batch should stay finished after another cancel")
	}
}

// When the archives cannot be enumerated ahead of time the batch still has to
// report something meaningful, so progress falls back to counting archives.
func TestBatchFallsBackToCountingArchives(t *testing.T) {
	x := &fakeExtractor{totals: map[string]int64{"a.zip": 0, "b.zip": 0, "c.zip": 0, "d.zip": 0}}
	b := newBatch("server-uuid", "/", []string{"a.zip", "b.zip", "c.zip", "d.zip"})

	go b.run(x)
	waitUntil(t, func() bool { return b.State().Status == StatusCompleted })

	state := b.State()
	if state.Total != 0 {
		t.Errorf("expected no byte totals to be known, got %d", state.Total)
	}
	if state.Progress != 1 {
		t.Errorf("expected a finished batch to report full progress, got %f", state.Progress)
	}
}

func TestBatchStateReportsCurrentArchive(t *testing.T) {
	b := newBatch("server-uuid", "/plugins", []string{"a.zip", "b.zip", "c.zip", "d.zip"})

	b.mu.Lock()
	b.status = StatusRunning
	b.index = 1
	b.files[0].Status = StatusCompleted
	b.files[0].Bytes = b.files[0].Total
	b.mu.Unlock()

	state := b.State()
	if state.File != "b.zip" {
		t.Errorf("expected the running archive to be reported, got %q", state.File)
	}
	if state.Root != "/plugins" || state.ID != b.id || b.ServerID() != "server-uuid" {
		t.Error("batch identity is not reported back to callers")
	}
	if state.Progress != 0.25 {
		t.Errorf("expected one archive of four to report 25%%, got %f", state.Progress)
	}

	// The snapshot must not hand out the live counters, or a caller reading it
	// after the batch moved on would see values shift underneath it.
	state.Files[0].Status = StatusPending
	if b.State().Files[0].Status != StatusCompleted {
		t.Error("progress snapshots should be copies of the tracked state")
	}
}
