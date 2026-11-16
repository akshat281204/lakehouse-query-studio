package catalog

import (
	"testing"
	"time"
)

func i64(v int64) *int64 { return &v }

func TestBuildSummaryCountsPercentilesBucketsAndReasons(t *testing.T) {
	now := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)
	ago := func(d time.Duration) time.Time { return now.Add(-d) }
	rows := []SummaryRow{
		{CreatedAt: ago(10 * time.Minute), Status: QuerySucceeded, DurationMs: i64(10), RowsReturned: i64(5), BytesStaged: i64(100)},
		{CreatedAt: ago(20 * time.Minute), Status: QuerySucceeded, DurationMs: i64(40), RowsReturned: i64(7), BytesStaged: i64(100)},
		{CreatedAt: ago(30 * time.Minute), Status: QuerySucceeded, DurationMs: i64(900), RowsReturned: i64(1)},
		{CreatedAt: ago(40 * time.Minute), Status: QuerySucceeded, DurationMs: i64(70000)}, // slower than every bound
		{CreatedAt: ago(50 * time.Minute), Status: QueryFailed, FailureReason: "engine_error"},
		{CreatedAt: ago(50 * time.Minute), Status: QueryFailed, FailureReason: "engine_error"},
		{CreatedAt: ago(55 * time.Minute), Status: QueryTimedOut},
		{CreatedAt: ago(5 * time.Minute), Status: QueryCancelled},
		{CreatedAt: ago(1 * time.Minute), Status: QueryRunning},
		{CreatedAt: ago(1 * time.Minute), Status: QueryQueued},
		{CreatedAt: ago(3 * time.Hour), Status: QuerySucceeded, DurationMs: i64(5)}, // outside a 1h window
	}
	s := BuildSummary(rows, time.Hour, 6, now)
	q := s.Queries
	if q.Total != 10 || q.Succeeded != 4 || q.Failed != 2 || q.TimedOut != 1 || q.Cancelled != 1 || q.Running != 1 || q.Queued != 1 {
		t.Fatalf("counts: %+v", q)
	}
	if q.RowsReturned != 13 || q.BytesStaged != 200 {
		t.Fatalf("rows/bytes: %+v", q)
	}
	if q.P50Ms != 40 || q.P95Ms != 70000 || q.MaxMs != 70000 {
		t.Fatalf("percentiles: %+v", q)
	}
	if want := (10.0 + 40 + 900 + 70000) / 4; q.AvgMs != want {
		t.Fatalf("avg %v want %v", q.AvgMs, want)
	}
	// Histogram: 10 -> <=10, 40 -> <=50, 900 -> <=1000, 70000 -> overflow.
	h := map[float64]int{}
	for _, b := range s.Latency {
		h[b.LeMs] += b.Count
	}
	if h[10] != 1 || h[50] != 1 || h[1000] != 1 || h[0] != 1 {
		t.Fatalf("histogram: %v", s.Latency)
	}
	total := 0
	for _, b := range s.Latency {
		total += b.Count
	}
	if total != q.Succeeded {
		t.Fatalf("histogram holds %d, want %d succeeded", total, q.Succeeded)
	}
	// Timeline buckets partition the window and add up.
	var ok, bad, can int
	for _, b := range s.Timeline {
		ok, bad, can = ok+b.Succeeded, bad+b.Failed, can+b.Cancelled
	}
	if len(s.Timeline) != 6 || ok != 4 || bad != 3 || can != 1 {
		t.Fatalf("timeline: len=%d ok=%d bad=%d can=%d", len(s.Timeline), ok, bad, can)
	}
	if !s.Timeline[0].Start.Equal(now.Add(-time.Hour)) {
		t.Fatalf("first bucket starts %v", s.Timeline[0].Start)
	}
	if len(s.FailureReasons) != 2 || s.FailureReasons[0].Reason != "engine_error" || s.FailureReasons[0].Count != 2 || s.FailureReasons[1].Reason != "timeout" {
		t.Fatalf("reasons: %+v", s.FailureReasons)
	}
}

func TestBuildSummaryOfNothingIsWellFormed(t *testing.T) {
	s := BuildSummary(nil, 24*time.Hour, 24, time.Now())
	if s.Queries.Total != 0 || len(s.Timeline) != 24 || len(s.Latency) != len(LatencyBounds)+1 || s.FailureReasons == nil {
		t.Fatalf("%+v", s)
	}
	if s.Queries.P95Ms != 0 || s.Queries.AvgMs != 0 {
		t.Fatalf("empty window must report zeros, not NaN: %+v", s.Queries)
	}
}

func TestBuildSummaryKeepsBoundaryRowsInTheLastBucket(t *testing.T) {
	now := time.Now().UTC()
	s := BuildSummary([]SummaryRow{{CreatedAt: now, Status: QuerySucceeded}}, time.Hour, 4, now)
	if s.Timeline[3].Succeeded != 1 {
		t.Fatalf("a row created exactly now belongs in the last bucket: %+v", s.Timeline)
	}
}
