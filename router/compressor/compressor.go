// Package compressor tracks background archive creation so the panel can report
// progress while a server's files are being packed. It keeps its own registry
// rather than sharing router/decompressor: a compress request produces exactly one
// archive, so there is no batch to walk through and no per-file state to keep.
package compressor

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"emperror.dev/errors"
	"github.com/apex/log"
	"github.com/google/uuid"

	"github.com/pterodactyl/wings/internal/ufs"
	"github.com/pterodactyl/wings/server"
)

// Status values reported through the progress endpoint.
const (
	StatusPending   = "pending"
	StatusRunning   = "running"
	StatusCompleted = "completed"
	StatusFailed    = "failed"
	StatusCancelled = "cancelled"
)

// retainFinished is how long a finished job stays queryable. This gives the panel
// time to read the final state, including the name of the archive that was created,
// before it disappears from memory.
const retainFinished = 3 * time.Minute

// State is the snapshot handed back to anyone polling for progress.
type State struct {
	ID         string   `json:"id"`
	Root       string   `json:"root"`
	Status     string   `json:"status"`
	Files      []string `json:"files"`
	TotalFiles int      `json:"total_files"`
	// Archive is the name of the generated tar.gz, and stays empty until the
	// compression actually finishes.
	Archive    string  `json:"archive_name"`
	Bytes      int64   `json:"bytes"`
	Total      int64   `json:"total_bytes"`
	Progress   float64 `json:"progress"`
	Speed      int64   `json:"speed"`
	Error      string  `json:"error,omitempty"`
	Timestamp  int64   `json:"timestamp"`
	ElapsedSec int64   `json:"elapsed_seconds"`
}

// Job is a single running (or finished) compression.
type Job struct {
	id       string
	serverID string
	root     string
	files    []string

	// ctx is canceled when the job is abandoned so the filesystem layer stops
	// feeding data into the archive, and done is what the writer callbacks watch.
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	mu         sync.Mutex
	status     string
	err        string
	archive    string
	bytes      int64
	total      int64
	stopped    bool
	stopStatus string
	startedAt  time.Time
	finishedAt time.Time
	lastAt     time.Time
	lastBytes  int64
	speed      int64
}

// manager holds every compression job this daemon knows about.
type manager struct {
	mu   sync.RWMutex
	jobs map[string]*Job
}

var instance = &manager{jobs: make(map[string]*Job)}

// archiveCompressor is the slice of the server filesystem this package needs to
// build an archive. It exists so the job bookkeeping can be tested without standing
// up a real server environment.
type archiveCompressor interface {
	CompressSourceSize(ctx context.Context, dir string, paths []string) int64
	CompressFilesWithProgress(ctx context.Context, dir string, paths []string, written func(n int64)) (ufs.FileInfo, error)
}

// Start begins compressing names inside root and returns as soon as the job is
// registered. The compression itself runs in the background.
//
// It deliberately does not hang off the request context: the archive has to finish
// even if the panel stops watching it, otherwise a slow HTTP connection would leave
// a half written tar.gz behind for no reason the user can see.
func Start(s *server.Server, root string, names []string) *Job {
	j := newJob(s.ID(), root, names)

	instance.add(j)

	go j.run(s.Filesystem())

	return j
}

func newJob(serverID string, root string, names []string) *Job {
	ctx, cancel := context.WithCancel(context.Background())

	return &Job{
		id:        uuid.New().String(),
		serverID:  serverID,
		root:      root,
		files:     names,
		ctx:       ctx,
		cancel:    cancel,
		done:      make(chan struct{}),
		status:    StatusPending,
		startedAt: time.Now(),
		lastAt:    time.Now(),
	}
}

// Get returns a job by ID, or nil when it is not being tracked.
func Get(id string) *Job { return instance.get(id) }

// List returns snapshots of every job belonging to a server, newest first.
func List(serverID string) []*State { return instance.list(serverID) }

// Cancel asks a running job to stop. It reports whether the job existed.
func Cancel(id string) bool {
	j := instance.get(id)
	if j == nil {
		return false
	}

	j.stop(StatusCancelled)

	return true
}

// ID returns the tracking identifier for this job.
func (j *Job) ID() string { return j.id }

// ServerID returns the UUID of the server the archive belongs to.
func (j *Job) ServerID() string { return j.serverID }

func (j *Job) run(x archiveCompressor) {
	// A panic while packing files should not take the whole daemon down with it, so
	// surface it as a failure on the job instead.
	defer func() {
		if r := recover(); r != nil {
			log.WithField("compress_id", j.id).WithField("panic", r).Error("panic while compressing files")
			j.finish(StatusFailed, fmt.Errorf("unexpected error while compressing: %v", r))
		}
	}()

	if !j.begin() {
		return
	}

	// A tree that is too large to enumerate inside the walk budget reports zero, in
	// which case the caller gets an indeterminate bar rather than a fake percentage.
	if total := x.CompressSourceSize(j.ctx, j.root, j.files); total > 0 {
		j.setTotal(total)
	}

	info, err := x.CompressFilesWithProgress(j.ctx, j.root, j.files, func(n int64) {
		j.addBytes(n)
	})
	if err != nil {
		lg := log.WithField("compress_id", j.id).WithField("error", err)

		if j.wasStopped() || errors.Is(err, context.Canceled) {
			lg.Debug("compression cancelled")
			j.finish(StatusCancelled, nil)

			return
		}

		lg.Warn("failed to compress files")
		j.finish(StatusFailed, err)

		return
	}

	j.complete(info)
}

// begin moves the job into the running state. It returns false when the job was
// cancelled before compression had a chance to start.
func (j *Job) begin() bool {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.stopped {
		j.finishLocked(j.stopStatus)

		return false
	}

	j.status = StatusRunning

	return true
}

func (j *Job) setTotal(total int64) {
	j.mu.Lock()
	defer j.mu.Unlock()

	j.total = total
}

// addBytes accounts newly consumed source bytes and refreshes the rolling speed.
func (j *Job) addBytes(n int64) {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.stopped {
		return
	}

	j.bytes += n

	if now := time.Now(); now.Sub(j.lastAt) >= time.Second {
		seconds := now.Sub(j.lastAt).Seconds()
		j.speed = int64(float64(j.bytes-j.lastBytes) / seconds)
		j.lastAt = now
		j.lastBytes = j.bytes
	}
}

// complete records a finished archive. A nil info is allowed for callers that only
// want to close the job out, in which case the archive name stays unknown.
func (j *Job) complete(info ufs.FileInfo) {
	j.mu.Lock()
	defer j.mu.Unlock()

	if info != nil {
		j.archive = info.Name()
	}
	// The source walk and the bytes handed to the tar writer are two different
	// counts over the same data, so they will not agree exactly. Snapping the
	// counter to the total keeps a completed job at 100%.
	if j.total > j.bytes {
		j.bytes = j.total
	}

	j.finishLocked(StatusCompleted)
}

// stop cancels a job and records why it ended. It only signals the compression loop
// and never sets the final status itself, which keeps a cancel that lands after
// completion from rewriting history.
func (j *Job) stop(status string) {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.stopped || !j.isActive() {
		return
	}

	j.stopped = true
	j.stopStatus = status

	close(j.done)
	j.cancel()
}

func (j *Job) wasStopped() bool {
	j.mu.Lock()
	defer j.mu.Unlock()

	return j.stopped
}

func (j *Job) finish(status string, err error) {
	j.mu.Lock()
	defer j.mu.Unlock()

	if err != nil {
		j.err = err.Error()
	}
	j.finishLocked(status)
}

// finishLocked records the outcome of a job. Callers must hold the lock.
func (j *Job) finishLocked(status string) {
	if !j.isActive() {
		return
	}

	j.status = status
	j.speed = 0
	if j.finishedAt.IsZero() {
		j.finishedAt = time.Now()
	}
}

// isActive reports whether the job can still make progress. Callers must hold the lock.
func (j *Job) isActive() bool {
	return j.status == StatusPending || j.status == StatusRunning
}

// State returns a snapshot of the job safe for concurrent readers.
func (j *Job) State() *State {
	j.mu.Lock()
	defer j.mu.Unlock()

	state := &State{
		ID:         j.id,
		Root:       j.root,
		Status:     j.status,
		Files:      j.files,
		TotalFiles: len(j.files),
		Archive:    j.archive,
		Bytes:      j.bytes,
		Total:      j.total,
		Error:      j.err,
		Timestamp:  time.Now().Unix(),
		Speed:      j.speed,
	}

	if state.Total > 0 {
		state.Progress = float64(state.Bytes) / float64(state.Total)
	}
	if state.Progress > 1 {
		state.Progress = 1
	}
	if j.status == StatusCompleted {
		state.Progress = 1
	}

	if !j.finishedAt.IsZero() {
		state.ElapsedSec = int64(j.finishedAt.Sub(j.startedAt).Round(time.Second).Seconds())
	} else {
		state.ElapsedSec = int64(time.Since(j.startedAt).Round(time.Second).Seconds())
	}

	return state
}

func (m *manager) add(j *Job) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.prune()
	m.jobs[j.id] = j
}

func (m *manager) get(id string) *Job {
	// Progress polling lands here, so this is also where finished jobs that outlived
	// their retention window get dropped again.
	m.mu.Lock()
	defer m.mu.Unlock()

	m.prune()

	return m.jobs[id]
}

func (m *manager) list(serverID string) []*State {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.prune()

	states := make([]*State, 0, len(m.jobs))
	for _, j := range m.jobs {
		if j.serverID == serverID {
			states = append(states, j.State())
		}
	}

	sort.Slice(states, func(i, j int) bool { return states[i].Timestamp > states[j].Timestamp })

	return states
}

// prune drops jobs that have been finished for longer than the retention window.
// Callers must hold the write lock.
func (m *manager) prune() {
	for id, j := range m.jobs {
		j.mu.Lock()
		finished := j.finishedAt
		j.mu.Unlock()

		if !finished.IsZero() && time.Since(finished) > retainFinished {
			delete(m.jobs, id)
		}
	}
}
