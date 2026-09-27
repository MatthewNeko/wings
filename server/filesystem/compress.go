package filesystem

import (
	"context"
	"fmt"
	"io"
	iofs "io/fs"
	"path"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"emperror.dev/errors"
	"github.com/mholt/archives"
	"golang.org/x/text/encoding/simplifiedchinese"

	"github.com/pterodactyl/wings/internal/ufs"
	"github.com/pterodactyl/wings/server/filesystem/archiverext"
)

// CompressFiles compresses all the files matching the given paths in the
// specified directory. This function also supports passing nested paths to only
// compress certain files and folders when working in a larger directory. This
// effectively creates a local backup, but rather than ignoring specific files
// and folders, it takes an allow-list of files and folders.
//
// All paths are relative to the dir that is passed in as the first argument,
// and the compressed file will be placed at that location named
// `archive-{date}.tar.gz`.
func (fs *Filesystem) CompressFiles(dir string, paths []string) (ufs.FileInfo, error) {
	return fs.compressFiles(context.Background(), dir, paths, nil)
}

// CompressFilesWithProgress behaves like CompressFiles, but reports the number of
// source bytes handed to the archive after each write, and gives up as soon as ctx
// is cancelled. A cancelled compression leaves a truncated tar.gz behind that looks
// exactly like a real archive in the file list, so it is removed before returning.
func (fs *Filesystem) CompressFilesWithProgress(ctx context.Context, dir string, paths []string, written func(n int64)) (ufs.FileInfo, error) {
	return fs.compressFiles(ctx, dir, paths, written)
}

func (fs *Filesystem) compressFiles(ctx context.Context, dir string, paths []string, written func(n int64)) (ufs.FileInfo, error) {
	a := &Archive{Filesystem: fs, BaseDirectory: dir, Files: paths}
	a.OnWrite = written
	d := path.Join(
		dir,
		fmt.Sprintf("archive-%s.tar.gz", strings.ReplaceAll(time.Now().Format(time.RFC3339), ":", "")),
	)
	f, err := fs.unixFS.OpenFile(d, ufs.O_WRONLY|ufs.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	cw := ufs.NewCountedWriter(f)
	if err := a.Stream(ctx, cw); err != nil {
		if ctx.Err() != nil {
			_ = fs.unixFS.Remove(d)
		}
		return nil, err
	}
	if !fs.unixFS.CanFit(cw.BytesWritten()) {
		_ = fs.unixFS.Remove(d)
		return nil, newFilesystemError(ErrCodeDiskSpace, nil)
	}
	fs.unixFS.Add(cw.BytesWritten())
	return f.Stat()
}

// archiveSizeBudget caps how long CompressSourceSize will spend enumerating the
// source tree. It mirrors the budget used when inspecting an archive: bailing out
// early only means the caller gets a partial total, which progress reporting
// already copes with.
const archiveSizeBudget = 5 * time.Second

// CompressSourceSize returns the combined size of the files that an archive built
// from paths inside dir would contain. Paths may name files or directories and are
// resolved the same way Archive.Stream resolves them. This is best effort: entries
// that cannot be read are skipped, and the walk gives up once the budget is spent.
// A zero total simply means progress has to fall back to an indeterminate bar.
func (fs *Filesystem) CompressSourceSize(ctx context.Context, dir string, paths []string) int64 {
	var (
		total    int64
		deadline = time.Now().Add(archiveSizeBudget)
	)

	for _, p := range fs.normalizeArchivePaths(dir, paths) {
		if ctx.Err() != nil {
			break
		}

		fd, name, closeFd, err := fs.unixFS.SafePath(p)
		if err != nil {
			// Anything the archiver cannot reach will not end up in the archive
			// either, so it does not belong in the total.
			continue
		}

		_ = fs.unixFS.WalkDirat(fd, name, func(_ int, _, _ string, d ufs.DirEntry, err error) error {
			if err != nil || d == nil || d.IsDir() {
				return nil
			}

			info, err := d.Info()
			if err != nil {
				return nil
			}
			total += info.Size()

			if time.Now().After(deadline) {
				return iofs.SkipAll
			}

			return nil
		})
		closeFd()
	}

	return total
}

// normalizeArchivePaths turns the root and file list a compress request carries into
// the paths Archive.Stream will actually walk, so a size estimate and the resulting
// archive always describe the same set of files.
func (fs *Filesystem) normalizeArchivePaths(dir string, paths []string) []string {
	base := strings.TrimPrefix(dir, "/")

	out := make([]string, 0, len(paths))
	for _, p := range paths {
		if root := fs.Path(); strings.HasPrefix(p, root) {
			p = strings.TrimPrefix(strings.TrimPrefix(p, root), "/")
		}
		out = append(out, path.Join(base, p))
	}

	return out
}

func (fs *Filesystem) archiverFileSystem(ctx context.Context, p string) (iofs.FS, error) {
	f, err := fs.unixFS.Open(p)
	if err != nil {
		return nil, err
	}
	// Do not use defer to close `f`, it will likely be used later.

	format, _, err := archives.Identify(ctx, filepath.Base(p), f)
	if err != nil && !errors.Is(err, archives.NoMatch) {
		_ = f.Close()
		return nil, err
	}

	// Reset the file reader.
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	if format != nil {
		switch ff := format.(type) {
		case archives.Zip:
			// Use our custom ZipFS wrapper that handles GBK-encoded filenames
			// This is more performant than ArchiveFS, because it caches content information
			// and can open several content files concurrently because of io.ReaderAt requirement.
			return archiverext.NewZipFS(f, info.Size())
		case archives.Extraction:
			return &archives.ArchiveFS{Stream: io.NewSectionReader(f, 0, info.Size()), Format: ff, Context: ctx}, nil
		case archives.Compression:
			return archiverext.FileFS{File: f, Compression: ff}, nil
		}
	}
	_ = f.Close()
	return nil, archives.NoMatch
}

// scanArchive walks the archive located at dir/file and calls visit with the
// size of every entry in it. Visitors keep their own running totals because the
// callers care about different things, and a directory entry is not something
// worth accounting for uniformly: some archive readers report a directory as
// being as large as everything inside it.
//
// Enumerating very large archives can take a long time, so the walk is bounded
// by a five second budget. Once that budget is exhausted the partial total is
// reported to the visitor without an error, which is still usable for both space
// checks and progress reporting.
func (fs *Filesystem) scanArchive(ctx context.Context, dir string, file string, visit func(size int64, isDir bool) error) error {
	fsys, err := fs.archiverFileSystem(ctx, filepath.Join(dir, file))
	if err != nil {
		if errors.Is(err, archives.NoMatch) {
			return newFilesystemError(ErrCodeUnknownArchive, err)
		}
		return err
	}

	// Close the filesystem after we're done to release file handles
	if closer, ok := fsys.(io.Closer); ok {
		defer closer.Close()
	}

	// Create a context with timeout to prevent long delays on large archives
	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	err = iofs.WalkDir(fsys, ".", func(_ string, d iofs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		select {
		case <-timeoutCtx.Done():
			// Stop walking if the timeout is reached or context is canceled.
			// We'll check below whether to ignore the error (for timeouts)
			// or propagate it (for cancellations).
			return timeoutCtx.Err()
		default:
			info, err := d.Info()
			if err != nil {
				return err
			}

			return visit(info.Size(), d.IsDir())
		}
	})

	// If the error is a timeout, ignore it and report what we walked so far.
	// Space is still checked incrementally during the actual extraction.
	if errors.Is(err, context.DeadlineExceeded) {
		return nil
	}

	return err
}

// SpaceAvailableForDecompression looks through a given archive and determines
// if decompressing it would put the server over its allocated disk space limit.
// To avoid long delays on large archives, this function will timeout after 5 seconds
// and allow decompression to proceed (space will still be checked incrementally during extraction).
func (fs *Filesystem) SpaceAvailableForDecompression(ctx context.Context, dir string, file string) error {
	// Don't waste time trying to determine this if we know the server will have the space for
	// it since there is no limit.
	if fs.MaxDisk() <= 0 {
		return nil
	}

	// Accumulate every entry, directories included, so the check stays as
	// conservative as it has always been.
	var size atomic.Int64

	return fs.scanArchive(ctx, dir, file, func(entrySize int64, _ bool) error {
		if !fs.unixFS.CanFit(size.Add(entrySize)) {
			return newFilesystemError(ErrCodeDiskSpace, nil)
		}

		return nil
	})
}

// UncompressedSize returns the amount of bytes the contents of the archive at
// dir/file would occupy once fully extracted. Archives that cannot be enumerated
// within the scan budget return the partial total rather than an error, since an
// approximate total is still useful for reporting progress.
func (fs *Filesystem) UncompressedSize(ctx context.Context, dir string, file string) (int64, error) {
	var size atomic.Int64

	err := fs.scanArchive(ctx, dir, file, func(entrySize int64, isDir bool) error {
		// Directories hold no data of their own, and counting them here would
		// leave extraction progress stuck well below 100%.
		if !isDir {
			size.Add(entrySize)
		}

		return nil
	})

	return size.Load(), err
}

// DecompressFile will decompress a file in a given directory by using the
// archiver tool to infer the file type and go from there. This will walk over
// all the files within the given archive and ensure that there is not a
// zip-slip attack being attempted by validating that the final path is within
// the server data directory.
func (fs *Filesystem) DecompressFile(ctx context.Context, dir string, file string) error {
	return fs.DecompressFileWithProgress(ctx, dir, file, nil, nil)
}

// DecompressFileWithProgress behaves exactly like DecompressFile, except that
// every chunk of data written to the filesystem is reported back through the
// written callback. This is what allows long extractions to surface progress to
// the panel. The callback may be invoked from multiple goroutines at once, so
// implementations need to be safe for concurrent use.
//
// Extraction is abandoned as soon as cancel is closed. This is only used by the
// batch endpoint: the single archive endpoint passes nil so that an extraction
// keeps running even if the panel stops watching it.
func (fs *Filesystem) DecompressFileWithProgress(ctx context.Context, dir string, file string, written func(n int64), cancel <-chan struct{}) error {
	f, err := fs.unixFS.Open(filepath.Join(dir, file))
	if err != nil {
		return err
	}
	defer f.Close()

	// Identify the type of archive we are dealing with.
	format, input, err := archives.Identify(ctx, filepath.Base(file), f)
	if err != nil {
		if errors.Is(err, archives.NoMatch) {
			return newFilesystemError(ErrCodeUnknownArchive, err)
		}
		return err
	}

	return fs.extractStream(ctx, extractStreamOptions{
		FileName:  file,
		Directory: dir,
		Format:    format,
		Reader:    input,
		Written:   written,
		Cancel:    cancel,
	})
}

// ExtractStreamUnsafe .
func (fs *Filesystem) ExtractStreamUnsafe(ctx context.Context, dir string, r io.Reader) error {
	format, input, err := archives.Identify(ctx, "archive.tar.gz", r)
	if err != nil {
		if errors.Is(err, archives.NoMatch) {
			return newFilesystemError(ErrCodeUnknownArchive, err)
		}
		return err
	}
	return fs.extractStream(ctx, extractStreamOptions{
		Directory: dir,
		Format:    format,
		Reader:    input,
	})
}

type extractStreamOptions struct {
	// The directory to extract the archive to.
	Directory string
	// File name of the archive.
	FileName string
	// Format of the archive.
	Format archives.Format
	// Reader for the archive.
	Reader io.Reader
	// Written, when set, receives the size of every chunk written to the
	// filesystem while the archive is being extracted.
	Written func(n int64)
	// Cancel, when set, aborts the extraction as soon as it is closed.
	Cancel <-chan struct{}
}

// countingReader reports the number of bytes that flow through it. It is used
// to track extraction progress without changing how the archive formats read
// their contents.
type countingReader struct {
	reader  io.Reader
	written func(n int64)
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.reader.Read(p)
	if n > 0 {
		c.written(int64(n))
	}

	return n, err
}

// cancelled reports whether the caller asked for this extraction to be stopped.
// A nil Cancel channel simply never fires, which keeps callers that do not care
// about cancellation working as they always have.
func (e extractStreamOptions) cancelled() error {
	select {
	case <-e.Cancel:
		return context.Canceled
	default:
		return nil
	}
}

// decodeFilename attempts to decode a filename from an archive, automatically
// detecting and converting GBK-encoded filenames to UTF-8. This is necessary
// because many Windows applications in China create ZIP files with GBK-encoded
// filenames instead of UTF-8.
//
// The function uses a simple but effective heuristic:
// - If the filename is valid UTF-8, return it as-is
// - If the filename is not valid UTF-8, attempt to decode it as GBK
// - If GBK decoding fails, return the original filename
func decodeFilename(filename string) string {
	// Check if it's already valid UTF-8
	if utf8.ValidString(filename) {
		// Valid UTF-8, return as-is
		return filename
	}

	// Not valid UTF-8, try to decode as GBK
	decoded, err := simplifiedchinese.GBK.NewDecoder().String(filename)
	if err != nil {
		// GBK decoding failed, return original
		return filename
	}

	// Successfully decoded from GBK
	return decoded
}

func (fs *Filesystem) extractStream(ctx context.Context, opts extractStreamOptions) error {
	// See if it's a compressed archive, such as TAR or a ZIP
	ex, ok := opts.Format.(archives.Extractor)
	if !ok {
		// If not, check if it's a single-file compression, such as
		// .log.gz, .sql.gz, and so on
		de, ok := opts.Format.(archives.Decompressor)
		if !ok {
			return nil
		}

		// Strip the compression suffix
		p := filepath.Join(opts.Directory, strings.TrimSuffix(opts.FileName, opts.Format.Extension()))

		// Make sure it's not ignored
		if err := fs.IsIgnored(p); err != nil {
			return nil
		}

		reader, err := de.OpenReader(opts.Reader)
		if err != nil {
			return err
		}
		defer reader.Close()

		// Open the file for creation/writing
		f, err := fs.unixFS.OpenFile(p, ufs.O_WRONLY|ufs.O_CREATE, 0o644)
		if err != nil {
			return err
		}
		defer f.Close()

		// Read in 4 KB chunks
		buf := make([]byte, 4096)
		for {
			if err := opts.cancelled(); err != nil {
				return err
			}

			n, err := reader.Read(buf)
			if n > 0 {

				// Check quota before writing the chunk
				if quotaErr := fs.HasSpaceFor(int64(n)); quotaErr != nil {
					return quotaErr
				}

				// Write the chunk
				if _, writeErr := f.Write(buf[:n]); writeErr != nil {
					return writeErr
				}

				// Add to quota
				fs.addDisk(int64(n))

				if opts.Written != nil {
					opts.Written(int64(n))
				}
			}

			if err != nil {
				// EOF are expected
				if err == io.EOF {
					break
				}

				// Return any other
				return err
			}
		}

		return nil
	}

	// Decompress and extract archive
	return ex.Extract(ctx, opts.Reader, func(ctx context.Context, f archives.FileInfo) error {
		if err := opts.cancelled(); err != nil {
			return err
		}

		// Decode the filename, converting from GBK to UTF-8 if necessary.
		decodedName := decodeFilename(f.NameInArchive)
		p := filepath.Join(opts.Directory, decodedName)

		// If it is ignored, just don't do anything with the entry and skip over it.
		if err := fs.IsIgnored(p); err != nil {
			return nil
		}

		// Create directories explicitly; an empty one has no file to create it
		// implicitly and would otherwise be dropped during extraction.
		if f.IsDir() {
			if err := fs.unixFS.MkdirAll(p, 0o755); err != nil {
				return wrapError(err, opts.FileName)
			}
			return nil
		}
		r, err := f.Open()
		if err != nil {
			return err
		}
		defer r.Close()

		// Route the entry through a counting reader so callers can follow how
		// far the extraction has progressed.
		var src io.Reader = r
		if opts.Written != nil {
			src = &countingReader{reader: r, written: opts.Written}
		}

		if err := fs.Write(p, src, f.Size(), f.Mode()); err != nil {
			return wrapError(err, opts.FileName)
		}
		// Update the file modification time to the one set in the archive.
		if err := fs.Chtimes(p, f.ModTime(), f.ModTime()); err != nil {
			return wrapError(err, opts.FileName)
		}
		return nil
	})
}
