// Package compressor tracks background archive creation. These tests exercise that
// bookkeeping against a fake filesystem so cancellation, failures, and the per-server
// registry can be checked without standing up a real server.
package compressor

import (
	"context"
	"io/fs"
	"sync"
	"testing"
	"time"

	"emperror.dev/errors"

	"github.com/pterodactyl/wings/internal/ufs"
)

// fakeArchiver stands in for the server filesystem so the job bookkeeping can be
// exercised without a real server environment.
type fakeArchiver struct {
	total int64
	err   error

	// hold, when set, keeps the archive writer busy until the job is cancelled,
	// which lets the cancellation tests observe a stable mid-run state.
	hold chan struct{}

	mu sync.Mutex
	n  int
}

func (f *fakeArchiver) CompressSourceSize(_ context.Context, _ string, _ []string) int64 {
	f.mu.Lock()
	f.n++
	f.mu.Unlock()

	return f.total
}

func (f *fakeArchiver) CompressFilesWithProgress(ctx context.Context, _ string, _ []string, written func(n int64)) (ufs.FileInfo, error) {
	if f.hold != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-f.hold:
		}
	}

	if f.err != nil {
		return nil, f.err
	}

	if f.total > 0 && written != nil {
		written(f.total)
	}

	return fakeFileInfo{name: "archive-test.tar.gz"}, nil
}

// calls reports how many times the archiver was asked to do any work.
func (f *fakeArchiver) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.n
}

// fakeFileInfo only has to be good enough for the job to read the archive name.
type fakeFileInfo struct {
	fs.FileInfo

	name string
}

func (f fakeFileInfo) Name() string { return f.name }

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

func TestJobTracksProgress(t *testing.T) {
	x := &fakeArchiver{total: 4096}
	j := newJob("server-uuid", "/plugins", []string{"a.txt", "b.txt"})

	j.run(x)

	state := j.State()
	if state.Status != StatusCompleted {
		t.Fatalf("job finished as %s, expected completion", state.Status)
	}
	if state.Bytes != 4096 || state.Total != 4096 {
		t.Errorf("expected 4096/4096 bytes, got %d/%d", state.Bytes, state.Total)
	}
	if state.Progress != 1 {
		t.Errorf("expected a finished job to report full progress, got %f", state.Progress)
	}
	if state.Archive != "archive-test.tar.gz" {
		t.Errorf("expected the created archive to be reported, got %q", state.Archive)
	}
	if state.TotalFiles != 2 || state.Root != "/plugins" {
		t.Errorf("job inputs are not reported back: %d files in %q", state.TotalFiles, state.Root)
	}
	if state.ID != j.ID() || j.ServerID() != "server-uuid" {
		t.Error("job identity is not reported back to callers")
	}
}

// A source tree that is too large to enumerate inside the walk budget reports no
// total at all, and the job still has to finish cleanly rather than divide by zero.
func TestJobWithoutATotalStillCompletes(t *testing.T) {
	x := &fakeArchiver{total: 0}
	j := newJob("server-uuid", "/", []string{"huge"})

	j.run(x)

	state := j.State()
	if state.Status != StatusCompleted || state.Total != 0 {
		t.Fatalf("unexpected outcome: %s with total %d", state.Status, state.Total)
	}
	if state.Progress != 1 {
		t.Errorf("expected a finished job to report full progress, got %f", state.Progress)
	}
}

func TestJobReportsFailures(t *testing.T) {
	x := &fakeArchiver{total: 10, err: errors.New("no space left on device")}
	j := newJob("server-uuid", "/", []string{"a.txt"})

	j.run(x)

	state := j.State()
	if state.Status != StatusFailed {
		t.Fatalf("job finished as %s, expected failure", state.Status)
	}
	if state.Error != "no space left on device" {
		t.Errorf("failure reason was %q, expected the underlying error", state.Error)
	}
	if state.Archive != "" {
		t.Errorf("a failed job should not name an archive, got %q", state.Archive)
	}
}

func TestJobStopsWhenCancelled(t *testing.T) {
	x := &fakeArchiver{total: 1 << 20, hold: make(chan struct{})}
	j := newJob("server-uuid", "/", []string{"a.txt"})

	done := make(chan struct{})
	go func() {
		defer close(done)
		j.run(x)
	}()

	// Wait until the job is genuinely mid-flight, otherwise the cancel can land
	// before it has left the pending state.
	waitUntil(t, func() bool { return j.State().Status == StatusRunning })

	j.stop(StatusCancelled)
	waitUntil(t, func() bool { return j.State().Status == StatusCancelled })
	<-done

	state := j.State()
	if state.Archive != "" {
		t.Errorf("a cancelled job should not name an archive, got %q", state.Archive)
	}

	// Cancelling twice must not rewrite the outcome of a finished job.
	j.stop(StatusCancelled)
	if j.State().Status != StatusCancelled {
		t.Error("a finished job should stay finished after another cancel")
	}
}

// A cancel that lands before the archiver is even asked to run has to short circuit
// the job instead of packing files nobody wants any more.
func TestJobCancelledBeforeItStartsDoesNotCompress(t *testing.T) {
	x := &fakeArchiver{total: 10}
	j := newJob("server-uuid", "/", []string{"a.txt"})

	j.stop(StatusCancelled)
	j.run(x)

	if got := x.calls(); got != 0 {
		t.Errorf("expected no compression to run, got %d calls", got)
	}
	if j.State().Status != StatusCancelled {
		t.Errorf("job finished as %s, expected cancellation", j.State().Status)
	}
}

func TestJobStateIsACopy(t *testing.T) {
	x := &fakeArchiver{total: 10}
	j := newJob("server-uuid", "/", []string{"a.txt", "b.txt"})
	j.run(x)

	state := j.State()
	state.Bytes = 0
	state.Status = StatusFailed

	fresh := j.State()
	if fresh.Bytes != 10 || fresh.Status != StatusCompleted {
		t.Error("progress snapshots should be copies of the tracked state")
	}
}

func TestRegistryTracksJobsPerServer(t *testing.T) {
	first := newJob("server-a", "/", []string{"a.txt"})
	second := newJob("server-b", "/", []string{"b.txt"})

	instance.add(first)
	instance.add(second)
	t.Cleanup(func() {
		instance.mu.Lock()
		defer instance.mu.Unlock()

		delete(instance.jobs, first.id)
		delete(instance.jobs, second.id)
	})

	if got := instance.get(first.id); got != first {
		t.Error("registry did not return the stored job")
	}
	if got := instance.get("does-not-exist"); got != nil {
		t.Error("registry returned a job that was never added")
	}

	states := instance.list("server-a")
	if len(states) != 1 || states[0].ID != first.id {
		t.Fatalf("expected only server-a jobs, got %d", len(states))
	}
	if len(instance.list("server-c")) != 0 {
		t.Error("registry leaked jobs across servers")
	}

	if !Cancel(first.id) {
		t.Error("cancelling a tracked job reported failure")
	}
	if Cancel("does-not-exist") {
		t.Error("cancelling an unknown job reported success")
	}
}
