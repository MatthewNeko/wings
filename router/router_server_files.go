package router

import (
	"bufio"
	"context"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"emperror.dev/errors"
	"github.com/apex/log"
	"github.com/gin-gonic/gin"
	"golang.org/x/sync/errgroup"

	"github.com/pterodactyl/wings/config"
	"github.com/pterodactyl/wings/internal/models"
	"github.com/pterodactyl/wings/router/compressor"
	"github.com/pterodactyl/wings/router/decompressor"
	"github.com/pterodactyl/wings/router/downloader"
	"github.com/pterodactyl/wings/router/middleware"
	"github.com/pterodactyl/wings/router/tokens"
	"github.com/pterodactyl/wings/server"
	"github.com/pterodactyl/wings/server/filesystem"
)

// getServerFileContents returns the contents of a file on the server.
func getServerFileContents(c *gin.Context) {
	s := middleware.ExtractServer(c)
	p := strings.TrimLeft(c.Query("file"), "/")
	f, st, err := s.Filesystem().File(p)
	if err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}
	defer f.Close()
	// Don't allow a named pipe to be opened.
	//
	// @see https://github.com/pterodactyl/panel/issues/4059
	if st.Mode()&os.ModeNamedPipe != 0 {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
			"error": "Cannot open files of this type.",
		})
		return
	}

	c.Header("X-Mime-Type", st.Mimetype)
	c.Header("Content-Length", strconv.Itoa(int(st.Size())))
	// If a download parameter is included in the URL go ahead and attach the necessary headers
	// so that the file can be downloaded.
	if c.Query("download") != "" {
		c.Header("Content-Disposition", "attachment; filename="+strconv.Quote(st.Name()))
		c.Header("Content-Type", "application/octet-stream")
	}
	defer c.Writer.Flush()
	// If you don't do a limited reader here you will trigger a panic on write when
	// a different server process writes content to the file after you've already
	// determined the file size. This could lead to some weird content output but
	// it would technically be accurate based on the content at the time of the request.
	//
	// "http: wrote more than the declared Content-Length"
	//
	// @see https://github.com/pterodactyl/panel/issues/3131
	r := io.LimitReader(f, st.Size())
	if _, err = bufio.NewReader(r).WriteTo(c.Writer); err != nil {
		// Pretty sure this will unleash chaos on the response, but its a risk we can
		// take since a panic will at least be recovered and this should be incredibly
		// rare?
		middleware.CaptureAndAbort(c, err)
		return
	}
}

// Returns the contents of a directory for a server.
func getServerListDirectory(c *gin.Context) {
	s := ExtractServer(c)
	dir := c.Query("directory")
	if stats, err := s.Filesystem().ListDirectory(dir); err != nil {
		middleware.CaptureAndAbort(c, err)
	} else {
		c.JSON(http.StatusOK, stats)
	}
}

type renameFile struct {
	To   string `json:"to"`
	From string `json:"from"`
}

// Renames (or moves) files for a server.
func putServerRenameFiles(c *gin.Context) {
	s := ExtractServer(c)

	var data struct {
		Root  string       `json:"root"`
		Files []renameFile `json:"files"`
	}
	// BindJSON sends 400 if the request fails, all we need to do is return
	if err := c.BindJSON(&data); err != nil {
		return
	}

	if len(data.Files) == 0 {
		c.AbortWithStatusJSON(http.StatusUnprocessableEntity, gin.H{
			"error": "没有提供要移动或重命名的文件。",
		})
		return
	}

	g, ctx := errgroup.WithContext(c.Request.Context())
	// Loop over the array of files passed in and perform the move or rename action against each.
	for _, p := range data.Files {
		pf := path.Join(data.Root, p.From)
		pt := path.Join(data.Root, p.To)

		g.Go(func() error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				fs := s.Filesystem()
				// Ignore renames on a file that is on the denylist (both as the rename from or
				// the rename to value).
				if err := fs.IsIgnored(pf, pt); err != nil {
					return err
				}
				if err := fs.Rename(pf, pt); err != nil {
					// Return nil if the error is an is not exists.
					if errors.Is(err, os.ErrNotExist) {
						s.Log().WithField("error", err).
							WithField("from_path", pf).
							WithField("to_path", pt).
							Warn("重命名失败：源或目标不存在")
						return nil
					}
					return err
				}
				return nil
			}
		})
	}

	if err := g.Wait(); err != nil {
		if errors.Is(err, os.ErrExist) {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
				"error": "无法移动或重命名文件，目标已存在。",
			})
			return
		}

		middleware.CaptureAndAbort(c, err)
		return
	}

	c.Status(http.StatusNoContent)
}

// Copies a server file.
func postServerCopyFile(c *gin.Context) {
	s := ExtractServer(c)

	var data struct {
		Location string `json:"location"`
	}
	// BindJSON sends 400 if the request fails, all we need to do is return
	if err := c.BindJSON(&data); err != nil {
		return
	}

	if err := s.Filesystem().IsIgnored(data.Location); err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}
	if err := s.Filesystem().Copy(data.Location); err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}

	c.Status(http.StatusNoContent)
}

// Deletes files from a server.
func postServerDeleteFiles(c *gin.Context) {
	s := ExtractServer(c)

	var data struct {
		Root  string   `json:"root"`
		Files []string `json:"files"`
	}

	if err := c.BindJSON(&data); err != nil {
		return
	}

	if len(data.Files) == 0 {
		c.AbortWithStatusJSON(http.StatusUnprocessableEntity, gin.H{
			"error": "没有指定要删除的文件。",
		})
		return
	}

	g, ctx := errgroup.WithContext(context.Background())

	// Loop over the array of files passed in and delete them. If any of the file deletions
	// fail just abort the process entirely.
	for _, p := range data.Files {
		pi := path.Join(data.Root, p)

		g.Go(func() error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				return s.Filesystem().Delete(pi)
			}
		})
	}

	if err := g.Wait(); err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}

	c.Status(http.StatusNoContent)
}

// Writes the contents of the request to a file on a server.
func postServerWriteFile(c *gin.Context) {
	s := ExtractServer(c)

	f := c.Query("file")
	f = "/" + strings.TrimLeft(f, "/")

	if err := s.Filesystem().IsIgnored(f); err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}

	// A content length of -1 means the actual length is unknown.
	if c.Request.ContentLength == -1 {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
			"error": "Missing Content-Length",
		})
		return
	}

	if err := s.Filesystem().Write(f, c.Request.Body, c.Request.ContentLength, 0o644); err != nil {
		if filesystem.IsErrorCode(err, filesystem.ErrCodeIsDirectory) {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
				"error": "无法写入文件，名称与现有目录的名称存在冲突。",
			})
			return
		}

		middleware.CaptureAndAbort(c, err)
		return
	}

	c.Status(http.StatusNoContent)
}

// Returns all of the currently in-progress file downloads and their current download
// progress. The progress is also pushed out via a websocket event allowing you to just
// call this once to get current downloads, and then listen to targeted websocket events
// with the current progress for everything.
func getServerPullingFiles(c *gin.Context) {
	s := ExtractServer(c)
	c.JSON(http.StatusOK, gin.H{
		"downloads": downloader.ByServer(s.ID()),
	})
}

// Writes the contents of the remote URL to a file on a server.
func postServerPullRemoteFile(c *gin.Context) {
	s := ExtractServer(c)
	var data struct {
		// Deprecated
		Directory  string `binding:"required_without=RootPath,omitempty" json:"directory"`
		RootPath   string `binding:"required_without=Directory,omitempty" json:"root"`
		URL        string `binding:"required" json:"url"`
		FileName   string `json:"file_name"`
		UseHeader  bool   `json:"use_header"`
		Foreground bool   `json:"foreground"`
	}
	if err := c.BindJSON(&data); err != nil {
		return
	}

	// Handle the deprecated Directory field in the struct until it is removed.
	if data.Directory != "" && data.RootPath == "" {
		data.RootPath = data.Directory
	}

	u, err := url.Parse(data.URL)
	if err != nil {
		if e, ok := err.(*url.Error); ok {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
				"error": "解析该 URL 时发生错误: " + e.Err.Error(),
			})
			return
		}
		middleware.CaptureAndAbort(c, err)
		return
	}

	if err := s.Filesystem().HasSpaceErr(true); err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}
	// Do not allow more than three simultaneous remote file downloads at one time.
	if len(downloader.ByServer(s.ID())) >= 3 {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
			"error": "该服务器已达到同时下载 3 个远程文件的限制。 请等待现有任务完成后再次重试。",
		})
		return
	}

	dl := downloader.New(s, downloader.DownloadRequest{
		Directory: data.RootPath,
		URL:       u,
		FileName:  data.FileName,
		UseHeader: data.UseHeader,
	})

	download := func() error {
		s.Log().WithField("download_id", dl.Identifier).WithField("url", u.String()).Info("starting pull of remote file to disk")
		if err := dl.Execute(); err != nil {
			s.Log().WithField("download_id", dl.Identifier).WithField("error", err).Error("failed to pull remote file")
			return err
		} else {
			s.Log().WithField("download_id", dl.Identifier).Info("completed pull of remote file")
		}
		return nil
	}
	if !data.Foreground {
		go func() {
			_ = download()
		}()
		c.JSON(http.StatusAccepted, gin.H{
			"identifier": dl.Identifier,
		})
		return
	}

	if err := download(); err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}

	st, err := s.Filesystem().Stat(dl.Path())
	if err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}
	c.JSON(http.StatusOK, &st)
}

// Stops a remote file download if it exists and belongs to this server.
func deleteServerPullRemoteFile(c *gin.Context) {
	s := ExtractServer(c)
	if dl := downloader.ByID(c.Param("download")); dl != nil && dl.BelongsTo(s) {
		dl.Cancel()
	}
	c.Status(http.StatusNoContent)
}

// Create a directory on a server.
func postServerCreateDirectory(c *gin.Context) {
	s := ExtractServer(c)

	var data struct {
		Name string `json:"name"`
		Path string `json:"path"`
	}
	// BindJSON sends 400 if the request fails, all we need to do is return
	if err := c.BindJSON(&data); err != nil {
		return
	}

	if err := s.Filesystem().CreateDirectory(data.Name, data.Path); err != nil {
		if err.Error() == "not a directory" {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
				"error": "Part of the path being created is not a directory (ENOTDIR).",
			})
			return
		}

		middleware.CaptureAndAbort(c, err)
		return
	}

	c.Status(http.StatusNoContent)
}

// postServerCompressFiles packs the given paths into a tar.gz archive. By default
// it blocks until the archive is written and returns the resulting file stats, which
// is what older panel versions expect. Passing "background": true instead registers
// the job and returns immediately, letting the caller follow along through
// getCompressProgress.
func postServerCompressFiles(c *gin.Context) {
	s := ExtractServer(c)

	var data struct {
		RootPath   string   `json:"root"`
		Files      []string `json:"files"`
		Background bool     `json:"background"`
	}

	if err := c.BindJSON(&data); err != nil {
		return
	}

	files := cleanArchivePaths(data.Files)
	if len(files) == 0 {
		c.AbortWithStatusJSON(http.StatusUnprocessableEntity, gin.H{
			"error": "No files were passed through to be compressed.",
		})
		return
	}

	if !s.Filesystem().HasSpaceAvailable(true) {
		c.AbortWithStatusJSON(http.StatusConflict, gin.H{
			"error": "This server does not have enough available disk space to generate a compressed archive.",
		})
		return
	}

	// Background mode lets the panel show progress for archives large enough that
	// holding an HTTP request open for the whole compression would be risky.
	if data.Background {
		c.JSON(http.StatusAccepted, compressor.Start(s, data.RootPath, files).State())

		return
	}

	f, err := s.Filesystem().CompressFiles(data.RootPath, files)
	if err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}

	c.JSON(http.StatusOK, &filesystem.Stat{
		FileInfo: f,
		Mimetype: "application/tar+gzip",
	})
}

// getCompressProgress returns the progress of a single compression job, or every
// job tracked for this server when no job ID is provided.
func getCompressProgress(c *gin.Context) {
	s := middleware.ExtractServer(c)

	id := c.Query("job_id")
	if id == "" {
		c.JSON(http.StatusOK, gin.H{"jobs": compressor.List(s.ID())})

		return
	}

	j := extractCompressJob(c, s.ID(), id)
	if j == nil {
		return
	}

	c.JSON(http.StatusOK, j.State())
}

// deleteCompressProgress cancels a running compression. The archive is left in an
// unusable state on purpose: the filesystem layer removes the truncated tar.gz, so
// the only visible result is that packing simply stops.
func deleteCompressProgress(c *gin.Context) {
	j := extractCompressJob(c, middleware.ExtractServer(c).ID(), c.Param("job_id"))
	if j == nil {
		return
	}

	compressor.Cancel(j.ID())

	c.JSON(http.StatusOK, gin.H{
		"message": "Compression cancelled successfully",
		"id":      j.ID(),
	})
}

// extractCompressJob resolves a job ID for the server on the request and aborts
// with the matching error when that is not possible. It returns nil once an error
// response has been written.
func extractCompressJob(c *gin.Context, serverID string, id string) *compressor.Job {
	if id == "" {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "No job ID was provided."})

		return nil
	}

	j := compressor.Get(id)
	if j == nil {
		c.AbortWithStatusJSON(http.StatusNotFound, gin.H{"error": "Compression job not found."})

		return nil
	}

	// Never hand out progress for an archive belonging to another server.
	if j.ServerID() != serverID {
		c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "Compression job does not belong to this server."})

		return nil
	}

	return j
}

// postServerDecompressFiles unpacks archives that exist on the server into the
// provided root path. A single "file" is extracted synchronously, while a "files"
// array starts a background batch that reports progress through
// getDecompressProgress.
func postServerDecompressFiles(c *gin.Context) {
	var data struct {
		RootPath string   `json:"root"`
		File     string   `json:"file"`
		Files    []string `json:"files"`
	}
	if err := c.BindJSON(&data); err != nil {
		return
	}

	s := middleware.ExtractServer(c)

	// A list of archives is handled as a batch: the extraction runs in the
	// background and the caller polls for progress. This keeps the panel from
	// holding a request open for as long as the biggest archive takes.
	if files := cleanArchivePaths(data.Files); len(files) > 0 {
		c.JSON(http.StatusAccepted, decompressor.Start(s, data.RootPath, files).State())

		return
	}

	lg := middleware.ExtractLogger(c).WithFields(log.Fields{"root_path": data.RootPath, "file": data.File})

	// Check if there's enough space for decompression. This uses a 5-second timeout
	// to avoid delays on large archives - if it times out, decompression proceeds
	// with incremental space checking during extraction.
	err := s.Filesystem().SpaceAvailableForDecompression(c.Request.Context(), data.RootPath, data.File)
	if err != nil {
		if filesystem.IsErrorCode(err, filesystem.ErrCodeUnknownArchive) {
			lg.WithField("error", err).Warn("failed to decompress file: unknown archive format")
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "The archive provided is in a format Wings does not understand."})
			return
		}
		if filesystem.IsErrorCode(err, filesystem.ErrCodeDiskSpace) {
			lg.WithField("error", err).Warn("failed to decompress file: not enough disk space")
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "There is not enough disk space available to decompress this archive."})
			return
		}
		middleware.CaptureAndAbort(c, err)
		return
	}

	lg.Info("starting file decompression")
	if err := s.Filesystem().DecompressFile(c.Request.Context(), data.RootPath, data.File); err != nil {
		if filesystem.IsErrorCode(err, filesystem.ErrCodeUnknownArchive) {
			lg.WithField("error", err).Warn("failed to decompress file: unknown archive format")
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "The archive provided is in a format Wings does not understand."})
			return
		}
		if filesystem.IsErrorCode(err, filesystem.ErrCodeDiskSpace) {
			lg.WithField("error", err).Warn("failed to decompress file: not enough disk space")
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "There is not enough disk space available to decompress this archive."})
			return
		}
		// If the file is busy for some reason just return a nicer error to the user since there is not
		// much we specifically can do. They'll need to stop the running server process in order to overwrite
		// a file like this.
		if strings.Contains(err.Error(), "text file busy") {
			lg.WithField("error", errors.WithStackIf(err)).Warn("failed to decompress file: text file busy")
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
				"error": "One or more files this archive is attempting to overwrite are currently in use by another process. Please try again.",
			})
			return
		}
		middleware.CaptureAndAbort(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

// cleanArchivePaths strips the empty entries out of a list of archive names so
// a sloppy payload does not kick off a pointless extraction job.
func cleanArchivePaths(files []string) []string {
	if len(files) == 0 {
		return nil
	}

	cleaned := make([]string, 0, len(files))
	for _, f := range files {
		if f = strings.TrimSpace(f); f != "" {
			cleaned = append(cleaned, f)
		}
	}

	return cleaned
}

// getDecompressProgress returns the progress of a single decompression batch,
// or every batch tracked for this server when no batch ID is provided.
func getDecompressProgress(c *gin.Context) {
	s := middleware.ExtractServer(c)

	id := c.Query("batch_id")
	if id == "" {
		c.JSON(http.StatusOK, gin.H{"batches": decompressor.List(s.ID())})

		return
	}

	b := extractDecompressBatch(c, s.ID(), id)
	if b == nil {
		return
	}

	c.JSON(http.StatusOK, b.State())
}

// deleteDecompressProgress cancels a running decompression batch. Archives that
// were already extracted are left untouched, the batch simply stops working
// through whatever was still queued.
func deleteDecompressProgress(c *gin.Context) {
	b := extractDecompressBatch(c, middleware.ExtractServer(c).ID(), c.Param("batch_id"))
	if b == nil {
		return
	}

	decompressor.Cancel(b.ID())

	c.JSON(http.StatusOK, gin.H{
		"message": "Decompression batch cancelled successfully",
		"id":      b.ID(),
	})
}

// extractDecompressBatch resolves a batch ID for the server on the request and
// aborts the request with the matching error when that is not possible. It
// returns nil once an error response has been written.
func extractDecompressBatch(c *gin.Context, serverID string, id string) *decompressor.Batch {
	if id == "" {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "No batch ID was provided."})

		return nil
	}

	b := decompressor.Get(id)
	if b == nil {
		c.AbortWithStatusJSON(http.StatusNotFound, gin.H{"error": "Decompression batch not found."})

		return nil
	}

	// Never hand out progress for an archive belonging to another server.
	if b.ServerID() != serverID {
		c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "Decompression batch does not belong to this server."})

		return nil
	}

	return b
}

type chmodFile struct {
	File string `json:"file"`
	Mode string `json:"mode"`
}

var errInvalidFileMode = errors.New("invalid file mode")

func postServerChmodFile(c *gin.Context) {
	s := ExtractServer(c)

	var data struct {
		Root  string      `json:"root"`
		Files []chmodFile `json:"files"`
	}

	if err := c.BindJSON(&data); err != nil {
		log.Debug(err.Error())
		return
	}

	if len(data.Files) == 0 {
		c.AbortWithStatusJSON(http.StatusUnprocessableEntity, gin.H{
			"error": "No files to chmod were provided.",
		})
		return
	}

	g, ctx := errgroup.WithContext(context.Background())

	// Loop over the array of files passed in and perform the move or rename action against each.
	for _, p := range data.Files {
		g.Go(func() error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				mode, err := strconv.ParseUint(p.Mode, 8, 32)
				if err != nil {
					return errInvalidFileMode
				}

				if err := s.Filesystem().Chmod(path.Join(data.Root, p.File), os.FileMode(mode)); err != nil {
					// Return nil if the error is an is not exists.
					// NOTE: os.IsNotExist() does not work if the error is wrapped.
					if errors.Is(err, os.ErrNotExist) {
						return nil
					}

					return err
				}

				return nil
			}
		})
	}

	if err := g.Wait(); err != nil {
		if errors.Is(err, errInvalidFileMode) {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
				"error": "Invalid file mode.",
			})
			return
		}

		middleware.CaptureAndAbort(c, err)
		return
	}

	c.Status(http.StatusNoContent)
}

func postServerUploadFiles(c *gin.Context) {
	manager := middleware.ExtractManager(c)

	token := tokens.UploadPayload{}
	if err := tokens.ParseToken([]byte(c.Query("token")), &token); err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}

	s, ok := manager.Get(token.ServerUuid)
	if !ok || !token.IsUniqueRequest() {
		c.AbortWithStatusJSON(http.StatusNotFound, gin.H{
			"error": "The requested resource was not found on this server.",
		})
		return
	}

	form, err := c.MultipartForm()
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
			"error": "Failed to get multipart form data from request.",
		})
		return
	}

	headers, ok := form.File["files"]
	if !ok {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
			"error": "No files were found on the request body.",
		})
		return
	}

	directory := c.Query("directory")

	maxFileSize := config.Get().Api.UploadLimit
	maxFileSizeBytes := maxFileSize * 1024 * 1024
	var totalSize int64
	for _, header := range headers {
		if header.Size > maxFileSizeBytes {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
				"error": "File " + header.Filename + " is larger than the maximum file upload size of " + strconv.FormatInt(maxFileSize, 10) + " MB.",
			})
			return
		}
		totalSize += header.Size
	}

	for _, header := range headers {
		// We run this in a different method so I can use defer without any of
		// the consequences caused by calling it in a loop.
		if err := handleFileUpload(filepath.Join(directory, header.Filename), s, header); err != nil {
			middleware.CaptureAndAbort(c, err)
			return
		} else {
			s.SaveActivity(s.NewRequestActivity(token.UserUuid, c.ClientIP()), server.ActivityFileUploaded, models.ActivityMeta{
				"file":      header.Filename,
				"directory": filepath.Clean(directory),
			})
		}
	}
}

func handleFileUpload(p string, s *server.Server, header *multipart.FileHeader) error {
	file, err := header.Open()
	if err != nil {
		return err
	}
	defer file.Close()

	if err := s.Filesystem().IsIgnored(p); err != nil {
		return err
	}

	if err := s.Filesystem().Write(p, file, header.Size, 0o644); err != nil {
		return err
	}
	return nil
}
