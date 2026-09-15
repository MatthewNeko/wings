package server

import (
	"path/filepath"
	"testing"
	"time"

	. "github.com/franela/goblin"

	"github.com/pterodactyl/wings/config"
	"github.com/pterodactyl/wings/environment"
	"github.com/pterodactyl/wings/internal/database"
	"github.com/pterodactyl/wings/internal/models"
)

// sample builds a resource sample with the given values at the given moment.
func sample(cpu float64, memory, rx, tx uint64) environment.Stats {
	return environment.Stats{
		CpuAbsolute: cpu,
		Memory:      memory,
		Network:     environment.NetworkStats{RxBytes: rx, TxBytes: tx},
	}
}

func TestResourceBucketAggregation(t *testing.T) {
	g := Goblin(t)

	g.Describe("resourceBucket#add", func() {
		g.It("aggregates usage and records when each peak happened", func() {
			base := time.Now().UTC().Truncate(time.Minute)
			b := newResourceBucket(models.ResourceStatMinuteResolution, base)

			// Deliberately make the peaks for CPU, memory, inbound and outbound traffic occur
			// at four different samples, so a peak timestamp cannot be right by accident.
			b.add(sample(10, 100, 1000, 0), base)
			b.add(sample(50, 200, 1500, 100), base.Add(1*time.Second))
			b.add(sample(20, 150, 1600, 100), base.Add(2*time.Second))
			b.add(sample(30, 250, 2000, 300), base.Add(3*time.Second))

			g.Assert(b.samples).Equal(4)
			g.Assert(b.cpuMax).Equal(50.0)
			g.Assert(b.memoryMax).Equal(uint64(250))
			g.Assert(b.cpuPeakAt.Unix()).Equal(base.Add(1 * time.Second).Unix())
			g.Assert(b.memoryPeakAt.Unix()).Equal(base.Add(3 * time.Second).Unix())

			// The counters are cumulative, so only the deltas between samples count as
			// traffic and the very first sample acts purely as a baseline.
			g.Assert(b.rxBytes).Equal(uint64(1000))
			g.Assert(b.txBytes).Equal(uint64(300))
			g.Assert(b.rxPeak).Equal(uint64(500))
			g.Assert(b.txPeak).Equal(uint64(200))
			g.Assert(b.rxPeakAt.Unix()).Equal(base.Add(1 * time.Second).Unix())
			g.Assert(b.txPeakAt.Unix()).Equal(base.Add(3 * time.Second).Unix())

			row := b.toModel("server-uuid")
			g.Assert(row.CpuAvg).Equal(27.5)
			g.Assert(row.CpuMax).Equal(50.0)
			g.Assert(row.MemoryAvg).Equal(int64(175))
			g.Assert(row.MemoryMax).Equal(int64(250))
			g.Assert(row.RxBytes).Equal(int64(1000))
			g.Assert(row.TxBytes).Equal(int64(300))
			g.Assert(row.RxPeak).Equal(int64(500))
			g.Assert(row.TxPeak).Equal(int64(200))
			g.Assert(row.Samples).Equal(4)

			// The peak timestamps have to survive the round trip into the database row as
			// unix seconds, which is what the Panel reports to the user.
			g.Assert(row.CpuPeakAt).Equal(base.Add(1 * time.Second).Unix())
			g.Assert(row.MemoryPeakAt).Equal(base.Add(3 * time.Second).Unix())
			g.Assert(row.RxPeakAt).Equal(base.Add(1 * time.Second).Unix())
			g.Assert(row.TxPeakAt).Equal(base.Add(3 * time.Second).Unix())
		})

		g.It("treats a falling counter as a container restart", func() {
			base := time.Now().UTC().Truncate(time.Minute)
			b := newResourceBucket(models.ResourceStatMinuteResolution, base)

			b.add(sample(1, 1, 5000, 5000), base)
			// The container restarted, so the counters began again from near zero. The delta
			// must be the new value rather than a huge negative number.
			b.add(sample(1, 1, 10, 20), base.Add(1*time.Second))

			g.Assert(b.rxBytes).Equal(uint64(10))
			g.Assert(b.txBytes).Equal(uint64(20))
			g.Assert(b.rxPeak).Equal(uint64(10))
			g.Assert(b.txPeak).Equal(uint64(20))
		})

		g.It("carries the counter baseline across a bucket rollover", func() {
			r := newResourceRecorder("server-uuid")
			base := time.Now().UTC().Truncate(time.Minute)

			bucket := r.accumulate(nil, models.ResourceStatMinuteResolution, sample(5, 100, 1000, 1000), base)
			g.Assert(bucket.start).Equal(base.Unix())

			// The next sample lands in the following minute. The traffic transferred between
			// the two samples belongs to the new bucket, and without carrying the baseline it
			// would be discarded along with any peak it contains.
			next := base.Add(time.Minute)
			bucket = r.accumulate(bucket, models.ResourceStatMinuteResolution, sample(5, 100, 4000, 2000), next)

			g.Assert(bucket.start).Equal(next.Unix())
			g.Assert(bucket.rxBytes).Equal(uint64(3000))
			g.Assert(bucket.txBytes).Equal(uint64(1000))
			// The rate is spread over the full minute that separated the two samples.
			g.Assert(bucket.rxPeak).Equal(uint64(50))
			g.Assert(bucket.txPeak).Equal(uint64(16))
			g.Assert(len(r.pending)).Equal(1)
		})
	})
}

func TestResourceRecorderPersistence(t *testing.T) {
	g := Goblin(t)

	dir := t.TempDir()

	c, err := config.NewAtPath(filepath.Join(dir, "config.yml"))
	if err != nil {
		t.Fatalf("failed to build a test configuration: %s", err)
	}
	c.System.RootDirectory = dir
	// config.Set derives the JWT signer from the node token and panics on an empty key, so the
	// test configuration has to carry one.
	c.AuthenticationToken = "metrics-test-token"
	config.Set(c)

	if err := database.Initialize(); err != nil {
		t.Fatalf("failed to initialize the test database: %s", err)
	}

	const uuid = "df1e5c70-0000-4000-8000-000000000001"

	g.Describe("ResourceRecorder#flush", func() {
		g.It("persists aggregated buckets and reuses the row for an open bucket", func() {
			r := newResourceRecorder(uuid)
			r.active.Store(true)

			base := time.Now().UTC().Truncate(time.Minute)

			// One completed bucket, plus one that is still open and being refined.
			closed := newResourceBucket(models.ResourceStatMinuteResolution, base.Add(-time.Minute))
			closed.add(sample(20, 400, 0, 0), base.Add(-time.Minute))
			closed.add(sample(60, 800, 1000, 2000), base.Add(-time.Minute+time.Second))
			r.pending = append(r.pending, closed)

			r.minute = newResourceBucket(models.ResourceStatMinuteResolution, base)
			r.minute.add(sample(10, 200, 0, 0), base)
			r.minute.add(sample(30, 600, 2000, 1000), base.Add(time.Second))

			r.flush()

			rows, err := QueryResourceHistory(uuid, resourceHistoryRanges["1h"])
			g.Assert(err == nil).IsTrue()
			g.Assert(len(rows)).Equal(2)

			g.Assert(rows[0].CpuMax).Equal(60.0)
			g.Assert(rows[0].RxPeak).Equal(int64(1000))
			g.Assert(rows[1].CpuMax).Equal(30.0)
			g.Assert(rows[1].RxPeak).Equal(int64(2000))

			// Written again before the open bucket rolls over, which must update the existing
			// row instead of inserting a duplicate.
			r.flush()

			rows, err = QueryResourceHistory(uuid, resourceHistoryRanges["1h"])
			g.Assert(err == nil).IsTrue()
			g.Assert(len(rows)).Equal(2)

			summary := SummarizeResourceHistory(rows, resourceHistoryRanges["1h"])
			// 60 + 30 CPU peaks, and the weighted average of 40 (2 samples) and 20 (2 samples).
			g.Assert(summary.CpuPeak).Equal(60.0)
			g.Assert(summary.CpuAvg).Equal(30.0)
			g.Assert(summary.MemoryPeak).Equal(int64(800))
			g.Assert(summary.RxPeak).Equal(int64(2000))
			g.Assert(summary.TxPeak).Equal(int64(2000))
			g.Assert(summary.RxBytes).Equal(int64(3000))
			g.Assert(summary.TxBytes).Equal(int64(3000))
			g.Assert(summary.Samples).Equal(4)
			g.Assert(summary.ActiveBuckets).Equal(2)

			// The overall peak must be attributed to the exact sample that produced it.
			g.Assert(summary.RxPeakAt).Equal(base.Add(time.Second).Unix())
			g.Assert(summary.CpuPeakAt).Equal(base.Add(-time.Minute + time.Second).Unix())

			// Purging has to take every bucket for the server with it.
			g.Assert(PurgeResourceStats(uuid) == nil).IsTrue()

			rows, err = QueryResourceHistory(uuid, resourceHistoryRanges["1h"])
			g.Assert(err == nil).IsTrue()
			g.Assert(len(rows)).Equal(0)
		})
	})
}
