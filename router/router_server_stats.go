package router

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/pterodactyl/wings/router/middleware"
	"github.com/pterodactyl/wings/server"
)

// resourceHistoryPoint is a single aggregated bucket as returned to the Panel.
type resourceHistoryPoint struct {
	// Timestamp is the unix timestamp, in UTC, of the start of this bucket.
	Timestamp int64 `json:"timestamp"`

	// CPU usage as a percentage of a single CPU core.
	CpuAvg float64 `json:"cpu_avg"`
	CpuMax float64 `json:"cpu_max"`

	// Memory usage in bytes.
	MemoryAvg int64 `json:"memory_avg"`
	MemoryMax int64 `json:"memory_max"`

	// Traffic transferred during this bucket, in bytes.
	RxBytes int64 `json:"rx_bytes"`
	TxBytes int64 `json:"tx_bytes"`

	// Peak transfer rate observed during this bucket, in bytes per second. Docker only
	// publishes stats about once a second, so these are one second averages.
	RxPeak int64 `json:"rx_peak"`
	TxPeak int64 `json:"tx_peak"`

	// The exact moment, as a unix timestamp in UTC, at which each of the peaks above was
	// observed. This is what lets the Panel point at a specific second rather than only at
	// the bucket a peak happened in.
	CpuPeakAt    int64 `json:"cpu_peak_at"`
	MemoryPeakAt int64 `json:"memory_peak_at"`
	RxPeakAt     int64 `json:"rx_peak_at"`
	TxPeakAt     int64 `json:"tx_peak_at"`

	// Samples is the number of resource samples aggregated into this bucket.
	Samples int `json:"samples"`
}

// getServerResourceHistory returns the aggregated historical resource usage for a server. The
// Panel calls this endpoint to render the CPU, memory, and network usage graphs.
//
// The data is served from the local database and is aggregated into fixed width buckets, so
// the response size stays bounded regardless of how far back the requested range reaches. A
// range that contains no buckets means the server was not running during that window.
//
// GET /api/servers/:server/stats/history?range=7d
func getServerResourceHistory(c *gin.Context) {
	s := middleware.ExtractServer(c)

	name := c.DefaultQuery("range", server.DefaultResourceHistoryRange)
	r, ok := server.LookupResourceHistoryRange(name)
	if !ok {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
			"error": "The requested range is not valid, should be one of \"1h\", \"6h\", \"24h\", \"7d\", \"30d\".",
		})
		return
	}

	rows, err := server.QueryResourceHistory(s.ID(), r)
	if err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}

	points := make([]resourceHistoryPoint, 0, len(rows))
	for _, row := range rows {
		points = append(points, resourceHistoryPoint{
			Timestamp:    row.Bucket,
			CpuAvg:       row.CpuAvg,
			CpuMax:       row.CpuMax,
			CpuPeakAt:    row.CpuPeakAt,
			MemoryAvg:    row.MemoryAvg,
			MemoryMax:    row.MemoryMax,
			MemoryPeakAt: row.MemoryPeakAt,
			RxBytes:      row.RxBytes,
			TxBytes:      row.TxBytes,
			RxPeak:       row.RxPeak,
			RxPeakAt:     row.RxPeakAt,
			TxPeak:       row.TxPeak,
			TxPeakAt:     row.TxPeakAt,
			Samples:      row.Samples,
		})
	}

	now := time.Now().UTC()

	c.JSON(http.StatusOK, gin.H{
		"object":       "resource_history",
		"server":       s.ID(),
		"range":        r.Name,
		"resolution":   r.Resolution,
		"from":         now.Add(-r.Duration).Unix(),
		"to":           now.Unix(),
		"collected_at": now.Unix(),
		"summary":      server.SummarizeResourceHistory(rows, r),
		"points":       points,
	})
}
