package server

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"emperror.dev/errors"
	"github.com/apex/log"
	"gorm.io/gorm/clause"

	"github.com/pterodactyl/wings/config"
	"github.com/pterodactyl/wings/environment"
	"github.com/pterodactyl/wings/internal/database"
	"github.com/pterodactyl/wings/internal/models"
	"github.com/pterodactyl/wings/system"
)

// maxPendingBuckets caps the number of completed buckets that may be waiting to be written to
// the database. This is purely a safety valve so that a persistently failing database cannot
// cause memory usage to grow without bound; the oldest buckets are discarded first.
const maxPendingBuckets = 512

// resourceBucket accumulates every resource sample that falls within a single time bucket.
// Two instances are maintained per server: one at minute resolution and one at hour resolution.
type resourceBucket struct {
	// resolution is the width of this bucket in seconds.
	resolution int
	// start is the unix timestamp, in UTC, of the beginning of this bucket.
	start int64

	cpuSum    float64
	cpuMax    float64
	memorySum uint64
	memoryMax uint64

	// Traffic transferred during this bucket, derived from the delta between two samples.
	rxBytes uint64
	txBytes uint64

	// Highest observed transfer rate during this bucket, in bytes per second.
	rxPeak uint64
	txPeak uint64

	// The exact moment at which each peak was observed. Docker only publishes stats about once
	// a second, so a rate is inherently a one second average; recording when the peak happened
	// is what allows the Panel to report a precise time for it.
	cpuPeakAt    time.Time
	memoryPeakAt time.Time
	rxPeakAt     time.Time
	txPeakAt     time.Time

	samples int

	// State required to derive per-second rates from the cumulative container counters.
	lastRx uint64
	lastTx uint64
	lastAt time.Time
}

// alignBucket rounds the given time down to the nearest multiple of the resolution in seconds.
func alignBucket(resolution int, at time.Time) int64 {
	return at.UTC().Unix() / int64(resolution) * int64(resolution)
}

func newResourceBucket(resolution int, at time.Time) *resourceBucket {
	return &resourceBucket{resolution: resolution, start: alignBucket(resolution, at)}
}

// clone returns a copy of the bucket which is safe to read outside of the recorder lock.
func (b *resourceBucket) clone() *resourceBucket {
	c := *b
	return &c
}

// add merges a single sample into this bucket. Samples are expected to arrive in chronological
// order for a single bucket.
func (b *resourceBucket) add(stats environment.Stats, at time.Time) {
	b.samples++
	b.cpuSum += stats.CpuAbsolute
	if stats.CpuAbsolute > b.cpuMax {
		b.cpuMax = stats.CpuAbsolute
		b.cpuPeakAt = at
	}
	b.memorySum += stats.Memory
	if stats.Memory > b.memoryMax {
		b.memoryMax = stats.Memory
		b.memoryPeakAt = at
	}

	// Docker exposes cumulative counters for the lifetime of the container, so the amount of
	// traffic transferred during this bucket has to be derived from the difference between
	// two consecutive samples. The very first sample of a bucket establishes the baseline and
	// therefore contributes no traffic on its own.
	if !b.lastAt.IsZero() {
		rx := counterDelta(b.lastRx, stats.Network.RxBytes)
		tx := counterDelta(b.lastTx, stats.Network.TxBytes)
		b.rxBytes += rx
		b.txBytes += tx

		// The rate is the traffic transferred divided by the exact wall clock time between the
		// two samples, rather than by an assumed sampling interval. Using the real elapsed time
		// keeps the reported peak honest when Docker delivers stats late or unevenly.
		if elapsed := at.Sub(b.lastAt).Seconds(); elapsed > 0 {
			if v := uint64(float64(rx) / elapsed); v > b.rxPeak {
				b.rxPeak = v
				b.rxPeakAt = at
			}
			if v := uint64(float64(tx) / elapsed); v > b.txPeak {
				b.txPeak = v
				b.txPeakAt = at
			}
		}
	}

	b.lastRx = stats.Network.RxBytes
	b.lastTx = stats.Network.TxBytes
	b.lastAt = at
}

// toModel converts the accumulated data into a database row.
func (b *resourceBucket) toModel(server string) models.ResourceStat {
	row := models.ResourceStat{
		Server:       server,
		Resolution:   b.resolution,
		Bucket:       b.start,
		CpuMax:       b.cpuMax,
		CpuPeakAt:    unixOrZero(b.cpuPeakAt),
		MemoryMax:    int64(b.memoryMax),
		MemoryPeakAt: unixOrZero(b.memoryPeakAt),
		RxBytes:      int64(b.rxBytes),
		TxBytes:      int64(b.txBytes),
		RxPeak:       int64(b.rxPeak),
		RxPeakAt:     unixOrZero(b.rxPeakAt),
		TxPeak:       int64(b.txPeak),
		TxPeakAt:     unixOrZero(b.txPeakAt),
		Samples:      b.samples,
	}

	if b.samples > 0 {
		row.CpuAvg = b.cpuSum / float64(b.samples)
		row.MemoryAvg = int64(b.memorySum / uint64(b.samples))
	}

	return row
}

// unixOrZero converts a timestamp into unix seconds, using zero to represent "never observed"
// so that the value can be stored in a non-nullable column.
func unixOrZero(at time.Time) int64 {
	if at.IsZero() {
		return 0
	}
	return at.UTC().Unix()
}

// counterDelta returns the difference between two readings of a cumulative counter. A value
// lower than the previous reading indicates that the container was restarted and began
// counting from zero again, in which case the current value is the delta.
func counterDelta(previous, current uint64) uint64 {
	if current < previous {
		return current
	}
	return current - previous
}

// ResourceRecorder collects resource usage samples for a single server, aggregates them into
// minute and hour buckets, and periodically persists the aggregated buckets to the local
// database. The Panel reads those rows back when rendering the resource history graphs.
//
// The recorder deliberately only accumulates data while the server is running: Wings stops
// emitting resource events for a stopped container, so an offline server simply produces no
// buckets and therefore shows up as a gap in the graphs.
type ResourceRecorder struct {
	mu sync.Mutex

	// server is the UUID of the server this recorder belongs to.
	server string

	// The currently open buckets. A nil bucket indicates that the server has produced no
	// samples during the current bucket window.
	minute *resourceBucket
	hour   *resourceBucket

	// pending holds completed buckets that have not yet been written to the database.
	pending []*resourceBucket

	// lastSample is used to throttle samples to the configured interval.
	lastSample time.Time

	// active tracks whether the recorder has been started and sampling is enabled.
	active *system.AtomicBool
	once   sync.Once
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newResourceRecorder(uuid string) *ResourceRecorder {
	return &ResourceRecorder{
		server: uuid,
		active: system.NewAtomicBool(false),
	}
}

// Enabled returns true when this recorder is actively collecting data.
func (r *ResourceRecorder) Enabled() bool {
	return r != nil && r.active.Load()
}

// Start begins the background flush loop for this recorder. It is safe to call this more than
// once; only the first call has any effect.
func (r *ResourceRecorder) Start(ctx context.Context) {
	if r == nil {
		return
	}

	cfg := config.Get().System.Stats
	if !cfg.Enabled {
		return
	}

	r.once.Do(func() {
		ctx, cancel := context.WithCancel(ctx)

		r.mu.Lock()
		r.cancel = cancel
		r.mu.Unlock()

		r.active.Store(true)

		r.wg.Add(1)
		go r.loop(ctx)
	})
}

// Stop halts the background flush loop and writes any remaining data. It is safe to call this
// on a recorder that was never started.
func (r *ResourceRecorder) Stop() {
	if r == nil {
		return
	}

	r.mu.Lock()
	cancel := r.cancel
	r.mu.Unlock()
	if cancel == nil {
		return
	}

	cancel()
	r.wg.Wait()

	// The loop performs a final flush as it exits before signalling that it has stopped,
	// however flushing again here is cheap and guarantees that nothing collected before the
	// shutdown is still held in memory by the time this returns.
	r.flush()

	r.active.Store(false)
}

// Add merges a single resource usage sample into the recorder. This is called for every
// resource event emitted by the container environment, so it must remain cheap: all of the
// work performed here is limited to in-memory aggregation.
func (r *ResourceRecorder) Add(stats environment.Stats) {
	if !r.Enabled() {
		return
	}

	interval := time.Duration(config.Get().System.Stats.SampleInterval) * time.Second
	if interval < 0 {
		interval = 0
	}

	at := time.Now().UTC()

	r.mu.Lock()
	defer r.mu.Unlock()

	// Throttle samples down to the configured interval. The counters tracked on the bucket
	// reflect the last accepted sample, so a skipped sample simply produces a slightly wider
	// window on the next accepted one, keeping the derived transfer rate accurate.
	if !r.lastSample.IsZero() && at.Sub(r.lastSample) < interval {
		return
	}
	r.lastSample = at

	r.minute = r.accumulate(r.minute, models.ResourceStatMinuteResolution, stats, at)
	r.hour = r.accumulate(r.hour, models.ResourceStatHourResolution, stats, at)
}

// accumulate adds a sample to the bucket for the given resolution, rolling the bucket over
// first if the sample belongs to a new time window.
func (r *ResourceRecorder) accumulate(bucket *resourceBucket, resolution int, stats environment.Stats, at time.Time) *resourceBucket {
	start := alignBucket(resolution, at)
	if bucket == nil {
		bucket = newResourceBucket(resolution, at)
	} else if bucket.start != start {
		r.enqueue(bucket)

		// Carry the counter baseline across the boundary. Without this the first sample of
		// every bucket would only establish a new baseline, which both loses the traffic that
		// was transferred between the two samples and creates a blind spot where a genuine
		// peak can never be detected.
		next := newResourceBucket(resolution, at)
		next.lastRx = bucket.lastRx
		next.lastTx = bucket.lastTx
		next.lastAt = bucket.lastAt
		bucket = next
	}

	bucket.add(stats, at)
	return bucket
}

// enqueue marks a bucket as complete so that it is written out by the next flush.
func (r *ResourceRecorder) enqueue(bucket *resourceBucket) {
	if bucket == nil || bucket.samples < 1 {
		return
	}

	r.pending = append(r.pending, bucket)
	if len(r.pending) > maxPendingBuckets {
		r.pending = r.pending[len(r.pending)-maxPendingBuckets:]
	}
}

// rotateLocked closes any bucket that has moved out of its time window. This handles servers
// that have been stopped, where no further samples arrive to trigger a rollover naturally.
func (r *ResourceRecorder) rotateLocked(now time.Time) {
	if r.minute != nil && r.minute.start != alignBucket(models.ResourceStatMinuteResolution, now) {
		r.enqueue(r.minute)
		r.minute = nil
	}
	if r.hour != nil && r.hour.start != alignBucket(models.ResourceStatHourResolution, now) {
		r.enqueue(r.hour)
		r.hour = nil
	}
}

// loop periodically rolls over buckets, persists aggregated data, and prunes expired rows.
func (r *ResourceRecorder) loop(ctx context.Context) {
	defer r.wg.Done()

	interval := time.Duration(clamp(config.Get().System.Stats.FlushInterval, 5, 3600)) * time.Second
	flush := time.NewTicker(interval)
	defer flush.Stop()

	prune := time.NewTicker(time.Hour)
	defer prune.Stop()

	// Start each recorder at a random point in the flush interval so that a node hosting many
	// servers does not write every one of them to the database at the same instant.
	if jitter := time.Duration(rand.Int63n(int64(interval))); jitter > 0 {
		select {
		case <-ctx.Done():
			r.flush()
			return
		case <-time.After(jitter):
		}
	}

	for {
		select {
		case <-ctx.Done():
			r.flush()
			return
		case <-flush.C:
			r.flush()
		case <-prune.C:
			r.prune()
		}
	}
}

// flush writes every completed bucket, along with the currently open ones, to the database.
//
// Open buckets are written as well so that the graphs stay fresh and so that at most one flush
// interval worth of data is lost if Wings is terminated unexpectedly. Writes use replace
// semantics, which makes repeatedly persisting the same open bucket idempotent.
func (r *ResourceRecorder) flush() {
	if !r.Enabled() || !database.IsInitialized() {
		return
	}

	r.mu.Lock()
	r.rotateLocked(time.Now().UTC())

	rows := make([]models.ResourceStat, 0, len(r.pending)+2)
	for _, b := range r.pending {
		rows = append(rows, b.toModel(r.server))
	}
	r.pending = r.pending[:0]

	for _, b := range []*resourceBucket{r.minute, r.hour} {
		if b != nil && b.samples > 0 {
			rows = append(rows, b.clone().toModel(r.server))
		}
	}
	r.mu.Unlock()

	if len(rows) == 0 {
		return
	}

	tx := database.Instance().Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "server"},
			{Name: "resolution"},
			{Name: "bucket"},
		},
		DoUpdates: clause.AssignmentColumns([]string{
			"cpu_avg",
			"cpu_max",
			"memory_avg",
			"memory_max",
			"rx_bytes",
			"tx_bytes",
			"rx_peak",
			"tx_peak",
			"samples",
			"updated_at",
		}),
	}).CreateInBatches(rows, 50)
	if tx.Error != nil {
		log.WithField("server", r.server).
			WithField("error", errors.WithStack(tx.Error)).
			Error("resource stats: failed to persist aggregated resource usage")
	}
}

// prune deletes collected data that has fallen outside of the configured retention windows.
func (r *ResourceRecorder) prune() {
	if !database.IsInitialized() {
		return
	}

	cfg := config.Get().System.Stats
	now := time.Now().UTC()

	windows := []struct {
		resolution int
		hours      int
	}{
		{models.ResourceStatMinuteResolution, cfg.MinuteRetentionHours},
		{models.ResourceStatHourResolution, cfg.HourRetentionHours},
	}

	for _, w := range windows {
		if w.hours <= 0 {
			continue
		}

		cutoff := now.Add(-time.Duration(w.hours) * time.Hour).Unix()
		tx := database.Instance().
			Where("server = ? AND resolution = ? AND bucket < ?", r.server, w.resolution, cutoff).
			Delete(&models.ResourceStat{})
		if tx.Error != nil {
			log.WithField("server", r.server).
				WithField("error", errors.WithStack(tx.Error)).
				Error("resource stats: failed to prune expired resource usage")
		}
	}
}

// PurgeResourceStats removes every bucket that has been recorded for the given server. This is
// called when a server is deleted so that its history does not linger in the database.
func PurgeResourceStats(uuid string) error {
	if !database.IsInitialized() {
		return nil
	}

	tx := database.Instance().Where("server = ?", uuid).Delete(&models.ResourceStat{})
	if tx.Error != nil {
		return errors.WithStack(tx.Error)
	}
	return nil
}

// ResourceHistoryRange describes one of the predefined windows of resource history that the
// Panel is allowed to request.
type ResourceHistoryRange struct {
	// Name is the identifier used by the caller, e.g. "7d".
	Name string
	// Resolution is the bucket width, in seconds, that this range is served from.
	Resolution int
	// Duration is how far back in time this range extends.
	Duration time.Duration
}

// DefaultResourceHistoryRange is served when an unknown range is requested.
const DefaultResourceHistoryRange = "7d"

var resourceHistoryRanges = map[string]ResourceHistoryRange{
	"1h":  {Name: "1h", Resolution: models.ResourceStatMinuteResolution, Duration: time.Hour},
	"6h":  {Name: "6h", Resolution: models.ResourceStatMinuteResolution, Duration: 6 * time.Hour},
	"24h": {Name: "24h", Resolution: models.ResourceStatMinuteResolution, Duration: 24 * time.Hour},
	"7d":  {Name: "7d", Resolution: models.ResourceStatHourResolution, Duration: 7 * 24 * time.Hour},
	"30d": {Name: "30d", Resolution: models.ResourceStatHourResolution, Duration: 30 * 24 * time.Hour},
}

// LookupResourceHistoryRange resolves a range identifier into its definition.
func LookupResourceHistoryRange(name string) (ResourceHistoryRange, bool) {
	r, ok := resourceHistoryRanges[name]
	return r, ok
}

// QueryResourceHistory returns every bucket recorded for the given server within the supplied
// range, ordered from oldest to newest.
func QueryResourceHistory(uuid string, r ResourceHistoryRange) ([]models.ResourceStat, error) {
	if !database.IsInitialized() {
		return nil, errors.New("server/metrics: database has not been initialized")
	}

	from := alignBucket(r.Resolution, time.Now().UTC().Add(-r.Duration))

	var rows []models.ResourceStat
	tx := database.Instance().
		Where("server = ? AND resolution = ? AND bucket >= ?", uuid, r.Resolution, from).
		Order("bucket ASC").
		Find(&rows)
	if tx.Error != nil {
		return nil, errors.WithStack(tx.Error)
	}

	return rows, nil
}

// ResourceHistorySummary is a condensed overview of every bucket contained in a range.
type ResourceHistorySummary struct {
	// CpuPeak is the highest CPU usage seen anywhere in the range, as a percentage of a
	// single CPU core.
	CpuPeak float64 `json:"cpu_peak"`

	// CpuAvg is the sample weighted mean CPU usage across the range.
	CpuAvg float64 `json:"cpu_avg"`

	// MemoryPeak is the highest memory usage seen anywhere in the range, in bytes.
	MemoryPeak int64 `json:"memory_peak_bytes"`

	// MemoryAvg is the sample weighted mean memory usage across the range, in bytes.
	MemoryAvg int64 `json:"memory_avg_bytes"`

	// RxBytes and TxBytes are the total traffic transferred across the range, in bytes.
	RxBytes int64 `json:"rx_bytes"`
	TxBytes int64 `json:"tx_bytes"`

	// RxPeak and TxPeak are the highest observed transfer rates across the range, in bytes
	// per second.
	RxPeak int64 `json:"rx_peak"`
	TxPeak int64 `json:"tx_peak"`

	// The exact moment, as a unix timestamp in UTC, at which each of the peaks above was
	// observed. A value of zero means the peak predates peak timestamps being recorded.
	CpuPeakAt    int64 `json:"cpu_peak_at"`
	MemoryPeakAt int64 `json:"memory_peak_at"`
	RxPeakAt     int64 `json:"rx_peak_at"`
	TxPeakAt     int64 `json:"tx_peak_at"`

	// Samples is the total number of samples that were aggregated into the range.
	Samples int `json:"samples"`

	// ActiveBuckets counts the buckets that contain data, while TotalBuckets counts every
	// bucket the requested range spans. Comparing the two yields how long the server was
	// actually running during the window.
	ActiveBuckets int `json:"active_buckets"`
	TotalBuckets  int `json:"total_buckets"`
}

// SummarizeResourceHistory condenses a set of buckets into a single overview.
func SummarizeResourceHistory(rows []models.ResourceStat, r ResourceHistoryRange) ResourceHistorySummary {
	s := ResourceHistorySummary{
		TotalBuckets: int(r.Duration / (time.Duration(r.Resolution) * time.Second)),
	}

	var cpuWeighted, memoryWeighted float64
	for _, row := range rows {
		if row.Samples < 1 {
			continue
		}

		s.ActiveBuckets++

		if row.CpuMax > s.CpuPeak {
			s.CpuPeak = row.CpuMax
			s.CpuPeakAt = row.CpuPeakAt
		}
		cpuWeighted += row.CpuAvg * float64(row.Samples)

		if row.MemoryMax > s.MemoryPeak {
			s.MemoryPeak = row.MemoryMax
			s.MemoryPeakAt = row.MemoryPeakAt
		}
		memoryWeighted += float64(row.MemoryAvg) * float64(row.Samples)

		s.RxBytes += row.RxBytes
		s.TxBytes += row.TxBytes
		if row.RxPeak > s.RxPeak {
			s.RxPeak = row.RxPeak
			s.RxPeakAt = row.RxPeakAt
		}
		if row.TxPeak > s.TxPeak {
			s.TxPeak = row.TxPeak
			s.TxPeakAt = row.TxPeakAt
		}
		s.Samples += row.Samples
	}

	if s.Samples > 0 {
		s.CpuAvg = cpuWeighted / float64(s.Samples)
		s.MemoryAvg = int64(memoryWeighted / float64(s.Samples))
	}

	return s
}

// clamp restricts a value to the inclusive range defined by min and max.
func clamp(v, min, max int) int {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}
