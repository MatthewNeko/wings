package externalbackup

import (
	"context"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"emperror.dev/errors"
	"github.com/juju/ratelimit"

	"github.com/pterodactyl/wings/config"
	"github.com/pterodactyl/wings/remote"
	"github.com/pterodactyl/wings/server/filesystem"
	"github.com/pterodactyl/wings/server/remotestore"
)

// run drives one task end to end. It is the only place that reports a terminal
// status, so every exit path has to come through here.
func (t *Task) run() {
	defer close(t.done)

	var wg sync.WaitGroup
	wg.Add(1)
	go t.reportLoop(&wg)

	init, err := t.client.GetExternalBackupInit(t.ctx, t.serverID, t.runID)
	if err != nil {
		wg.Wait()
		t.finish(StatusFailed, errors.WrapIf(err, "externalbackup: unable to load the task from the panel"))

		return
	}

	if init.RemotePath == "" {
		wg.Wait()
		t.finish(StatusFailed, errors.New("externalbackup: the panel did not provide a target file name"))

		return
	}

	t.logContext.WithField("target", init.RemotePath).Info("external backup started")

	err = t.execute(t.ctx, init)

	// The reporting loop has to stop before the terminal sample is sent,
	// otherwise a tick could land after it and drag the bar back to a running
	// phase.
	t.cancel()
	wg.Wait()

	status := StatusFailed
	if err == nil {
		status = StatusSuccess
	} else if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		status = StatusCancelled
	}

	t.finish(status, err)
}

func (t *Task) execute(ctx context.Context, init remote.ExternalBackupInit) error {
	// The archive is temporary and sits where the local backup adapter puts its
	// files, so the configured write limit and disk guards apply to it as well.
	dst := filepath.Join(config.Get().System.BackupDirectory, t.runID+".external-backup.tar.gz")
	defer func() {
		if err := os.Remove(dst); err != nil && !os.IsNotExist(err) {
			t.logContext.WithField("error", err).Warn("externalbackup: failed to remove the temporary archive")
		}
	}()

	uploader, err := t.connect(ctx, init)
	if err != nil {
		return err
	}
	defer func() {
		_ = uploader.Close()
	}()

	if err := t.archive(ctx, init, dst); err != nil {
		return err
	}

	st, err := os.Stat(dst)
	if err != nil {
		return errors.WrapIf(err, "externalbackup: the generated archive went missing")
	}

	// The tar is finished, so its real size replaces the source-size estimate the
	// archiving phase was measured against.
	t.setPhase(StatusUploading, st.Size(), 0)

	if err := t.upload(ctx, uploader, init, dst, st.Size()); err != nil {
		return err
	}

	t.setPhase(StatusPruning, st.Size(), st.Size())
	t.prune(uploader, init)

	return nil
}

// connect dials the remote storage. It happens before archiving so a bad
// password fails in seconds instead of after a long backup was written to disk.
func (t *Task) connect(ctx context.Context, init remote.ExternalBackupInit) (remotestore.Uploader, error) {
	uploader, err := remotestore.New(ctx, storageFrom(init.Storage))
	if err != nil {
		return nil, errors.WrapIf(err, "externalbackup: unable to reach the remote storage")
	}

	return uploader, nil
}

// archive writes the tar.gz to dst, measuring progress by how much source data
// was consumed because that is the only total knowable up front.
func (t *Task) archive(ctx context.Context, init remote.ExternalBackupInit, dst string) error {
	a := &filesystem.Archive{
		Filesystem: t.fsys,
		Files:      init.IncludePaths,
		Ignore:     strings.Join(init.ExcludePaths, "\n"),
	}

	// Nothing was excluded on purpose: fall back to the server's own
	// .pteroignore so an external backup skips what a normal backup skips.
	if a.Ignore == "" {
		a.Ignore = t.ignoredFromPteroignore()
	}

	total, err := t.fsys.DiskUsage(false)
	if err != nil {
		total = 0
	}
	if len(init.IncludePaths) > 0 {
		total = t.fsys.CompressSourceSize(ctx, "", init.IncludePaths)
	}

	t.setSourceTotal(total)
	t.sendStatus()

	a.OnWrite = func(n int64) { t.addSource(n) }
	if err := a.Create(ctx, dst); err != nil {
		return errors.WrapIf(err, "externalbackup: archiving the server files failed")
	}

	return nil
}

// upload streams dst out to the remote storage under the speed limit the user
// configured.
func (t *Task) upload(ctx context.Context, uploader remotestore.Uploader, init remote.ExternalBackupInit, src string, size int64) error {
	f, err := os.Open(src)
	if err != nil {
		return errors.WrapIf(err, "externalbackup: unable to open the generated archive")
	}
	defer f.Close()

	var r io.Reader = f
	if burst := init.SpeedLimitKbps * 1024; burst > 0 {
		r = ratelimit.Reader(f, ratelimit.NewBucketWithRate(float64(burst), burst/2))
	}

	r = &watcher{ctx: ctx, r: r, onRead: t.addUploaded}

	if err := uploader.Upload(ctx, init.RemotePath, r, size); err != nil {
		return errors.WrapIf(err, "externalbackup: uploading the archive failed")
	}

	return nil
}

// prune drops the oldest remote archives once the new one is safely stored.
//
// Only names carrying this run's server-uuid segment are candidates, so a shared
// directory holding unrelated files is never touched. If the panel changes its
// naming scheme this stops matching and prunes nothing, which is the safe way to
// fail.
func (t *Task) prune(uploader remotestore.Uploader, init remote.ExternalBackupInit) {
	if init.KeepCount <= 0 {
		return
	}

	base := path.Base(init.RemotePath)
	marker := uuidMarker(base)
	if marker == "" {
		return
	}

	dir := path.Dir(init.RemotePath)
	if dir == "." || dir == "/" {
		dir = strings.Trim(init.Storage.RemotePath, "/")
	}

	objects, err := uploader.List(t.ctx, dir)
	if err != nil {
		t.logContext.WithField("error", err).Warn("externalbackup: unable to list remote archives, skipping pruning")

		return
	}

	matches := make([]remotestore.Object, 0, len(objects))
	for _, o := range objects {
		name := path.Base(o.Path)
		if !strings.HasSuffix(name, ".tar.gz") || !strings.Contains(name, marker) || name == base {
			continue
		}

		matches = append(matches, o)
	}

	// The trailing "20260927-021500" is chronological and survives a server
	// rename, unlike sorting the whole file name.
	sort.Slice(matches, func(i, j int) bool {
		return stampOf(matches[i].Path) > stampOf(matches[j].Path)
	})

	if len(matches) < init.KeepCount {
		return
	}

	// keep_count counts what stays on the drive, and the archive just uploaded
	// already fills one of those slots.
	for _, o := range matches[max(init.KeepCount-1, 0):] {
		if err := uploader.Delete(t.ctx, o.Path); err != nil {
			t.logContext.WithField("error", err).WithField("remote_path", o.Path).
				Warn("externalbackup: failed to delete an old remote archive")

			continue
		}

		t.logContext.WithField("remote_path", o.Path).Info("externalbackup: pruned an old remote archive")
	}
}

// ignoredFromPteroignore mirrors what Server.Backup does so both routes agree on
// what a server's own ignore file means.
func (t *Task) ignoredFromPteroignore() string {
	f, st, err := t.fsys.File(".pteroignore")
	if err != nil {
		return ""
	}
	defer f.Close()

	if st.Mode()&os.ModeSymlink != 0 || st.Size() > 32*1024 {
		return ""
	}

	b, err := io.ReadAll(f)
	if err != nil {
		return ""
	}

	return string(b)
}

// uuidMarker pulls the server-uuid segment out of a panel generated archive name
// shaped like "slug-1a2b3c4d-20260927-021500.tar.gz".
func uuidMarker(base string) string {
	parts := strings.Split(base, "-")
	if len(parts) < 4 {
		return ""
	}

	return "-" + parts[len(parts)-3] + "-"
}

// stampOf returns the "20260927-021500" tail of an archive path.
func stampOf(p string) string {
	parts := strings.Split(path.Base(p), "-")
	if len(parts) < 2 {
		return ""
	}

	return strings.Join(parts[len(parts)-2:], "-")
}
