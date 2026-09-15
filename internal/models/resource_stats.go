package models

import "time"

// Resolution values used when aggregating resource usage. Each resolution has its own
// retention policy which is controlled from the Wings configuration file.
const (
	// ResourceStatMinuteResolution aggregates a single row per minute of server uptime.
	// This is the resolution used for the short term, high detail graphs.
	ResourceStatMinuteResolution = 60

	// ResourceStatHourResolution aggregates a single row per hour of server uptime. This is
	// the resolution used for the long term graphs such as the seven day view.
	ResourceStatHourResolution = 3600
)

// ResourceStat is a single aggregated bucket of resource usage for a server. Wings keeps one
// row per resolution per time bucket on the local SQLite database, and the Panel reads these
// rows in order to render the historical resource usage graphs.
//
// Buckets are only created for periods during which the server was actually running, so a gap
// in the returned rows means the server was offline at that time.
type ResourceStat struct {
	ID int `gorm:"primaryKey;not null" json:"-"`

	// Server is the UUID of the server this bucket belongs to.
	Server string `gorm:"type:uuid;not null;uniqueIndex:idx_resource_stat_bucket" json:"server"`

	// Resolution is the width of this bucket in seconds, either 60 or 3600.
	Resolution int `gorm:"not null;uniqueIndex:idx_resource_stat_bucket" json:"resolution"`

	// Bucket is the unix timestamp, in UTC, of the start of this bucket.
	Bucket int64 `gorm:"not null;uniqueIndex:idx_resource_stat_bucket" json:"bucket"`

	// CpuAvg is the mean CPU usage across every sample in this bucket, expressed as a
	// percentage of a single CPU core.
	CpuAvg float64 `gorm:"not null" json:"cpu_avg"`

	// CpuMax is the highest CPU usage observed for this bucket.
	CpuMax float64 `gorm:"not null" json:"cpu_max"`

	// CpuPeakAt is the unix timestamp, in UTC, at which CpuMax was observed. A value of zero
	// means no peak was recorded for this bucket.
	//
	// The default is required so that these columns can be added to an existing database by
	// AutoMigrate; SQLite refuses to add a NOT NULL column without one.
	CpuPeakAt int64 `gorm:"not null;default:0" json:"cpu_peak_at"`

	// MemoryAvg and MemoryMax are the mean and peak memory usage for this bucket, in bytes.
	MemoryAvg int64 `gorm:"not null" json:"memory_avg"`
	MemoryMax int64 `gorm:"not null" json:"memory_max"`

	// MemoryPeakAt is the unix timestamp, in UTC, at which MemoryMax was observed.
	MemoryPeakAt int64 `gorm:"not null;default:0" json:"memory_peak_at"`

	// RxBytes and TxBytes are the total amount of traffic transferred during this bucket, in
	// bytes. These are derived from the difference between two consecutive samples of the
	// cumulative container counters.
	RxBytes int64 `gorm:"not null" json:"rx_bytes"`
	TxBytes int64 `gorm:"not null" json:"tx_bytes"`

	// RxPeak and TxPeak are the highest observed transfer rates during this bucket, in
	// bytes per second. Because Docker only publishes stats about once a second these are one
	// second averages rather than truly instantaneous rates.
	RxPeak int64 `gorm:"not null" json:"rx_peak"`
	TxPeak int64 `gorm:"not null" json:"tx_peak"`

	// RxPeakAt and TxPeakAt are the unix timestamps, in UTC, at which the transfer rate peaks
	// were observed. These are what allow the Panel to report exactly when a server hit its
	// peak bandwidth rather than only the bucket it happened in.
	RxPeakAt int64 `gorm:"not null;default:0" json:"rx_peak_at"`
	TxPeakAt int64 `gorm:"not null;default:0" json:"tx_peak_at"`

	// Samples is the number of resource samples that were merged into this bucket. A value of
	// zero indicates that the row carries no usage data.
	Samples int `gorm:"not null" json:"samples"`

	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
}
