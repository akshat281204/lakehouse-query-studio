package bench

import (
	"math"
	"sort"
	"time"
)

// Summary describes a set of latency samples in milliseconds.
type Summary struct {
	N      int     `json:"n"`
	MinMs  float64 `json:"min_ms"`
	P50Ms  float64 `json:"p50_ms"`
	P95Ms  float64 `json:"p95_ms"`
	P99Ms  float64 `json:"p99_ms"`
	MaxMs  float64 `json:"max_ms"`
	MeanMs float64 `json:"mean_ms"`
}

// Percentile returns the nearest-rank percentile (0 < p <= 100) of sorted
// values. With few samples high percentiles equal the maximum, which Summary
// callers must report together with N.
func Percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return math.NaN()
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

// Summarize computes the summary of durations.
func Summarize(ds []time.Duration) Summary {
	if len(ds) == 0 {
		return Summary{}
	}
	ms := make([]float64, len(ds))
	var sum float64
	for i, d := range ds {
		ms[i] = float64(d) / float64(time.Millisecond)
		sum += ms[i]
	}
	sort.Float64s(ms)
	return Summary{N: len(ms), MinMs: ms[0], P50Ms: Percentile(ms, 50), P95Ms: Percentile(ms, 95),
		P99Ms: Percentile(ms, 99), MaxMs: ms[len(ms)-1], MeanMs: sum / float64(len(ms))}
}
