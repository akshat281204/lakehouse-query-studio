package catalog

import (
	"math"
	"sort"
	"time"
)

// SummaryRow is the slice of a query job the dashboard summary needs.
type SummaryRow struct {
	CreatedAt     time.Time
	Status        string
	DurationMs    *int64
	FailureReason string
	RowsReturned  *int64
	BytesStaged   *int64
}

// LatencyBounds are the histogram upper bounds in milliseconds.
var LatencyBounds = []float64{10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000}

// QueryTotals are counts and latency percentiles for a window. Latency
// percentiles are computed over SUCCEEDED queries only.
type QueryTotals struct {
	Total        int     `json:"total"`
	Succeeded    int     `json:"succeeded"`
	Failed       int     `json:"failed"`
	Cancelled    int     `json:"cancelled"`
	TimedOut     int     `json:"timedOut"`
	Queued       int     `json:"queued"`
	Running      int     `json:"running"`
	RowsReturned int64   `json:"rowsReturned"`
	BytesStaged  int64   `json:"bytesStaged"`
	AvgMs        float64 `json:"avgMs"`
	P50Ms        float64 `json:"p50Ms"`
	P95Ms        float64 `json:"p95Ms"`
	P99Ms        float64 `json:"p99Ms"`
	MaxMs        float64 `json:"maxMs"`
}

// TimelineBucket is one time slice of query activity.
type TimelineBucket struct {
	Start     time.Time `json:"start"`
	Succeeded int       `json:"succeeded"`
	Failed    int       `json:"failed"` // FAILED and TIMED_OUT
	Cancelled int       `json:"cancelled"`
	P50Ms     float64   `json:"p50Ms"`
}

// HistogramBucket counts succeeded queries with latency <= LeMs (and above the
// previous bound). The last bucket has LeMs 0 and means "slower than every bound".
type HistogramBucket struct {
	LeMs  float64 `json:"leMs"`
	Count int     `json:"count"`
}

// ReasonCount is a failure reason and how often it occurred.
type ReasonCount struct {
	Reason string `json:"reason"`
	Count  int    `json:"count"`
}

// IngestTotals counts ingestion jobs created in the window.
type IngestTotals struct {
	Total     int `json:"total"`
	Succeeded int `json:"succeeded"`
	Failed    int `json:"failed"`
	Active    int `json:"active"`
}

// Summary is the data behind the dashboard overview.
type Summary struct {
	WindowSeconds  int               `json:"windowSeconds"`
	GeneratedAt    time.Time         `json:"generatedAt"`
	Queries        QueryTotals       `json:"queries"`
	Timeline       []TimelineBucket  `json:"timeline"`
	Latency        []HistogramBucket `json:"latencyHistogram"`
	FailureReasons []ReasonCount     `json:"failureReasons"`
	Ingestions     IngestTotals      `json:"ingestions"`
	Datasets       int               `json:"datasets"`
	// Truncated is true when the window held more jobs than were aggregated.
	Truncated bool `json:"truncated"`
}

// Percentile returns the nearest-rank percentile of sorted values.
func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(p / 100 * float64(len(sorted))))
	if rank < 1 {
		rank = 1
	}
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return sorted[rank-1]
}

// BuildSummary aggregates rows created within [now-window, now] into the
// query part of a Summary. buckets is the number of timeline slices.
func BuildSummary(rows []SummaryRow, window time.Duration, buckets int, now time.Time) Summary {
	if buckets < 1 {
		buckets = 1
	}
	start := now.Add(-window)
	width := window / time.Duration(buckets)
	s := Summary{WindowSeconds: int(window.Seconds()), GeneratedAt: now}
	s.Timeline = make([]TimelineBucket, buckets)
	for i := range s.Timeline {
		s.Timeline[i].Start = start.Add(time.Duration(i) * width)
	}
	s.Latency = make([]HistogramBucket, len(LatencyBounds)+1)
	for i, b := range LatencyBounds {
		s.Latency[i].LeMs = b
	}

	var durations []float64
	perBucket := make([][]float64, buckets)
	reasons := map[string]int{}
	var sum float64
	for _, r := range rows {
		if r.CreatedAt.Before(start) || r.CreatedAt.After(now) {
			continue
		}
		s.Queries.Total++
		idx := int(r.CreatedAt.Sub(start) / width)
		if idx >= buckets {
			idx = buckets - 1
		}
		if idx < 0 {
			idx = 0
		}
		switch r.Status {
		case QuerySucceeded:
			s.Queries.Succeeded++
			s.Timeline[idx].Succeeded++
			if r.RowsReturned != nil {
				s.Queries.RowsReturned += *r.RowsReturned
			}
			if r.DurationMs != nil {
				d := float64(*r.DurationMs)
				durations = append(durations, d)
				perBucket[idx] = append(perBucket[idx], d)
				sum += d
				placed := false
				for i, b := range LatencyBounds {
					if d <= b {
						s.Latency[i].Count++
						placed = true
						break
					}
				}
				if !placed {
					s.Latency[len(LatencyBounds)].Count++
				}
			}
		case QueryFailed:
			s.Queries.Failed++
			s.Timeline[idx].Failed++
			if r.FailureReason != "" {
				reasons[r.FailureReason]++
			}
		case QueryTimedOut:
			s.Queries.TimedOut++
			s.Timeline[idx].Failed++
			reasons["timeout"]++
		case QueryCancelled:
			s.Queries.Cancelled++
			s.Timeline[idx].Cancelled++
		case QueryQueued:
			s.Queries.Queued++
		case QueryRunning:
			s.Queries.Running++
		}
		if r.BytesStaged != nil {
			s.Queries.BytesStaged += *r.BytesStaged
		}
	}
	sort.Float64s(durations)
	if n := len(durations); n > 0 {
		s.Queries.AvgMs = sum / float64(n)
		s.Queries.P50Ms = percentile(durations, 50)
		s.Queries.P95Ms = percentile(durations, 95)
		s.Queries.P99Ms = percentile(durations, 99)
		s.Queries.MaxMs = durations[n-1]
	}
	for i, d := range perBucket {
		sort.Float64s(d)
		s.Timeline[i].P50Ms = percentile(d, 50)
	}
	for r, c := range reasons {
		s.FailureReasons = append(s.FailureReasons, ReasonCount{Reason: r, Count: c})
	}
	sort.Slice(s.FailureReasons, func(i, j int) bool {
		if s.FailureReasons[i].Count != s.FailureReasons[j].Count {
			return s.FailureReasons[i].Count > s.FailureReasons[j].Count
		}
		return s.FailureReasons[i].Reason < s.FailureReasons[j].Reason
	})
	if s.FailureReasons == nil {
		s.FailureReasons = []ReasonCount{}
	}
	return s
}
