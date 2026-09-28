package router

import (
	"net/http"
	"path/filepath"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/pterodactyl/wings/router/downloader"
	"github.com/pterodactyl/wings/router/middleware"
)

// getDownloadProgress returns the current progress for a specific download or all downloads
func getDownloadProgress(c *gin.Context) {
	s := middleware.ExtractServer(c)

	// Get download ID from query parameter
	downloadID := c.Query("download_id")

	if downloadID != "" {
		// Return progress for specific download
		progress := downloader.GetProgress(downloadID)
		if progress != nil {
			c.JSON(http.StatusOK, progress)
			return
		}

		// Progress not found, check if download exists in cache
		dl := downloader.ByID(downloadID)
		if dl == nil {
			c.AbortWithStatusJSON(http.StatusNotFound, gin.H{
				"error": "Download not found",
			})
			return
		}

		// 下载对象还在内存里，说明 Execute 还没返回，进度事件只是被清理掉了。
		// 这里不能硬说 completed：面板会照着这句话把没下完的文件记成成功。
		c.JSON(http.StatusOK, gin.H{
			"id":        downloadID,
			"file_name": filepath.Base(dl.Path()),
			"progress":  dl.Progress(),
			"status":    "downloading",
			"timestamp": time.Now().Unix(),
		})
		return
	}

	// Return all active downloads for this server
	downloads := downloader.ByServer(s.ID())
	var progressList []*downloader.ProgressEvent
	for _, dl := range downloads {
		if progress := downloader.GetProgress(dl.Identifier); progress != nil {
			progressList = append(progressList, progress)
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"downloads": progressList,
	})
}

// listActiveDownloads returns all active downloads for a server
func listActiveDownloads(c *gin.Context) {
	s := middleware.ExtractServer(c)

	// Get all active downloads for this server
	downloads := downloader.ByServer(s.ID())

	type DownloadInfo struct {
		Identifier string  `json:"identifier"`
		FileName   string  `json:"file_name"`
		Progress   float64 `json:"progress"`
		Status     string  `json:"status"`
		Total      int64   `json:"total,omitempty"`
		Bytes      int64   `json:"bytes,omitempty"`
		Speed      int64   `json:"speed,omitempty"`
	}

	var downloadList []DownloadInfo
	for _, dl := range downloads {
		progress := downloader.GetProgress(dl.Identifier)
		if progress != nil {
			downloadList = append(downloadList, DownloadInfo{
				Identifier: dl.Identifier,
				FileName:   progress.FileName,
				Progress:   progress.Progress,
				Status:     progress.Status,
				Total:      progress.Total,
				Bytes:      progress.Bytes,
				Speed:      progress.Speed,
			})
		} else {
			// Download exists but hasn't started yet or progress was cleaned up
			// Return basic info from the download object
			downloadList = append(downloadList, DownloadInfo{
				Identifier: dl.Identifier,
				FileName:   filepath.Base(dl.Path()),
				Progress:   dl.Progress(),
				Status:     "pending",
			})
		}
	}

	// Return empty array instead of null if no downloads
	if downloadList == nil {
		downloadList = []DownloadInfo{}
	}

	c.JSON(http.StatusOK, gin.H{
		"downloads": downloadList,
	})
}

// deleteDownloadProgress cancels an active download
func deleteDownloadProgress(c *gin.Context) {
	s := middleware.ExtractServer(c)

	// Get download ID from path parameter
	downloadID := c.Param("download_id")
	if downloadID == "" {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
			"error": "Download ID is required",
		})
		return
	}

	// Find the download
	dl := downloader.ByID(downloadID)
	if dl == nil {
		c.AbortWithStatusJSON(http.StatusNotFound, gin.H{
			"error": "Download not found",
		})
		return
	}

	// Verify the download belongs to this server
	if !dl.BelongsTo(s) {
		c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
			"error": "Download does not belong to this server",
		})
		return
	}

	// Cancel the download
	dl.Cancel()

	c.JSON(http.StatusOK, gin.H{
		"message":     "Download cancelled successfully",
		"download_id": downloadID,
	})
}
