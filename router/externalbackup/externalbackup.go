// Package externalbackup runs the "copy a server to the user's own storage"
// feature on the daemon: archive the selected files, upload the tar.gz under a
// speed limit, then prune old archives down to the count the panel asked for.
//
// Progress goes back to the panel over the remote API rather than a websocket,
// because these runs can last hours and the panel is the thing showing them.
package externalbackup

import (
	"context"
	"sync"
	"time"

	"emperror.dev/errors"
	"github.com/apex/log"

	"github.com/pterodactyl/wings/remote"
	"github.com/pterodactyl/wings/server"
	"github.com/pterodactyl/wings/server/filesystem"
)

// Status values mirror Pterodactyl\Models\ExternalBackupRun on the panel. Keep
// the two lists in sync or the status endpoint will reject the report.
const (
	StatusArchiving = "archiving"
	StatusUploading = "uploading"
	StatusPruning   = "pruning"
	StatusSuccess   = "success"
	StatusFailed    = "failed"
	StatusCancelled = "cancelled"
)

// The panel shows one 0-100 bar for the whole run. Archiving is the heavy phase
// on large servers, so it owns the biggest slice of the bar.
const (
	archivingWeight float64 = 50
	uploadingWeight float64 = 45
	pruningWeight   float64 = 5
)

// reportInterval is how often a running task pushes a sample to the panel. The
// panel advances its own progress from these, so this also caps how fast the
// visible bar can move.
const reportInterval = 3 * time.Second

// Task is one external backup owned by this daemon.
type Task struct {
	runID      string
	serverID   string
	client     remote.Client
	fsys       *filesystem.Filesystem
	logContext *log.Entry

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	mu            sync.Mutex
	status        string
	err           string
	archiveBytes  int64
	uploadedBytes int64
	sourceBytes   int64
	sourceTotal   int64
	speedKbps     int64
	startedAt     time.Time
	lastSampleAt  time.Time
	lastSampleVal int64
}

// manager keeps every task this daemon knows about, keyed by run UUID.
type manager struct {
	mu    sync.RWMutex
	tasks map[string]*Task
}

var instance = &manager{tasks: make(map[string]*Task)}

// Start takes the cancel window and disk guard rails from the panel, then runs
// the job in the background. It returns as soon as the task is registered.
//
// Only one external backup per server may run at a time: two archives reading
// the same data directory at once would fight over the disk and could double the
// peak space used in the backup directory.
func Start(s *server.Server, client remote.Client, runID string, timeout time.Duration) error {
	instance.mu.Lock()
	defer instance.mu.Unlock()

	if existing, ok := instance.tasks[runID]; ok {
		if existing.active() {
			return errors.Errorf("externalbackup: run %s is already in progress on this daemon", runID)
		}

		delete(instance.tasks, runID)
	}

	for _, t := range instance.tasks {
		if t.serverID == s.ID() && t.active() {
			return errors.Errorf("externalbackup: server %s already has an external backup running", s.ID())
		}
	}

	if timeout <= 0 {
		timeout = 4 * time.Hour
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t := &Task{
		runID:        runID,
		serverID:     s.ID(),
		client:       client,
		fsys:         s.Filesystem(),
		logContext:   log.WithFields(log.Fields{"server": s.ID(), "external_backup": runID}),
		ctx:          ctx,
		cancel:       cancel,
		done:         make(chan struct{}),
		status:       StatusArchiving,
		startedAt:    time.Now(),
		lastSampleAt: time.Now(),
	}

	instance.tasks[runID] = t

	go t.run()

	return nil
}

// Cancel stops a running task. It reports whether anything was actually stopped,
// so the caller can answer 404 for a run this daemon never started.
func Cancel(runID string) bool {
	instance.mu.RLock()
	t, ok := instance.tasks[runID]
	instance.mu.RUnlock()

	if !ok || !t.active() {
		return false
	}

	t.cancel()

	return true
}

// active reports whether the task is still doing work. Callers must hold the
// manager lock or be inside the task itself.
func (t *Task) active() bool {
	select {
	case <-t.done:
		return false
	default:
		return true
	}
}
