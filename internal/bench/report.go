package bench

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/akshat281204/lakehouse-query-studio/internal/query"
)

// RunConfig is the configuration a result was produced with.
type RunConfig struct {
	Scale             float64      `json:"scale_factor_like"`
	Seed              uint64       `json:"seed"`
	Iterations        int          `json:"warm_iterations"`
	ConcurrencyLevels []int        `json:"concurrency_levels"`
	ConcurrencyJobs   int          `json:"concurrency_queries_per_level"`
	IngestRows        int          `json:"ingest_rows"`
	ObjectStore       string       `json:"object_store"`
	Limits            query.Limits `json:"query_limits"`
	LimitsNote        string       `json:"limits_note"`
}

// PartitionComparison compares the flat and hive-partitioned layouts for one query pair.
type PartitionComparison struct {
	Pair             string  `json:"pair"`
	FlatP50Ms        float64 `json:"flat_p50_ms"`
	PartitionedP50Ms float64 `json:"partitioned_p50_ms"`
	FlatExecP50Ms    float64 `json:"flat_execute_p50_ms"`
	PartExecP50Ms    float64 `json:"partitioned_execute_p50_ms"`
	FlatBytesStaged  int64   `json:"flat_bytes_staged"`
	PartBytesStaged  int64   `json:"partitioned_bytes_staged"`
	Verdict          string  `json:"verdict"`
	// ExecVerdict judges engine execution time alone (staging excluded), which
	// is far less noisy than end-to-end time when staging is disk-bound.
	ExecVerdict string `json:"execute_only_verdict"`
}

func execVerdict(a, b QueryResult) string {
	x, y := a.WarmExecute, b.WarmExecute
	switch {
	case x.P50Ms == 0 || y.P50Ms == 0:
		return "not measured"
	case relDiff(x.P50Ms, y.P50Ms) < within || absDiff(x.P50Ms, y.P50Ms) < minAbsMs:
		return "no measurable execute-time difference"
	}
	v := fmt.Sprintf("partitioned execute %.0f%% slower (p50 %.0f vs %.0f ms)", (y.P50Ms/x.P50Ms-1)*100, y.P50Ms, x.P50Ms)
	if y.P50Ms < x.P50Ms {
		v = fmt.Sprintf("partitioned execute %.0f%% faster (p50 %.0f vs %.0f ms)", (1-y.P50Ms/x.P50Ms)*100, y.P50Ms, x.P50Ms)
	}
	if x.P95Ms > 2*x.P50Ms || y.P95Ms > 2*y.P50Ms {
		v += "; INCONCLUSIVE: high variance"
	}
	return v
}

// Report is the complete, self-describing output of a benchmark run.
type Report struct {
	GeneratedAt  time.Time             `json:"generated_at"`
	Environment  Environment           `json:"environment"`
	Config       RunConfig             `json:"config"`
	Dataset      *Dataset              `json:"dataset"`
	Queries      []QueryResult         `json:"queries"`
	Concurrency  []ConcurrencyResult   `json:"concurrency"`
	Ingest       []IngestResult        `json:"ingestion"`
	Partitioning []PartitionComparison `json:"partitioning"`
	Limitations  []string              `json:"limitations"`
}

// A difference between two medians is only reported when it exceeds BOTH
// within (relative) and minAbsMs (absolute): single-machine runs of a few
// iterations at millisecond resolution cannot rank closer results.
const (
	within   = 0.10
	minAbsMs = 5.0
)

func compare(a, b QueryResult, label string) PartitionComparison {
	c := PartitionComparison{Pair: label, FlatP50Ms: a.Warm.P50Ms, PartitionedP50Ms: b.Warm.P50Ms,
		FlatExecP50Ms: a.WarmExecute.P50Ms, PartExecP50Ms: b.WarmExecute.P50Ms,
		FlatBytesStaged: a.BytesStaged, PartBytesStaged: b.BytesStaged}
	switch {
	case a.Warm.P50Ms == 0 || b.Warm.P50Ms == 0:
		c.Verdict = "not measured"
	case relDiff(a.Warm.P50Ms, b.Warm.P50Ms) < within || absDiff(a.Warm.P50Ms, b.Warm.P50Ms) < minAbsMs:
		c.Verdict = fmt.Sprintf("no measurable end-to-end difference (under %.0f%% or %.0f ms)", within*100, minAbsMs)
	case b.Warm.P50Ms < a.Warm.P50Ms:
		c.Verdict = fmt.Sprintf("partitioned layout %.0f%% faster end to end (p50)", (1-b.Warm.P50Ms/a.Warm.P50Ms)*100)
	default:
		c.Verdict = fmt.Sprintf("partitioned layout %.0f%% slower end to end (p50)", (b.Warm.P50Ms/a.Warm.P50Ms-1)*100)
	}
	if a.BytesStaged == b.BytesStaged {
		c.Verdict += "; bytes staged identical (the whole dataset is downloaded either way)"
	}
	return c
}

func absDiff(a, b float64) float64 {
	if a > b {
		return a - b
	}
	return b - a
}

func relDiff(a, b float64) float64 {
	m := a
	if b > m {
		m = b
	}
	if m == 0 {
		return 0
	}
	d := a - b
	if d < 0 {
		d = -d
	}
	return d / m
}

// highVariance reports whether a result's tail is too far from its median for
// a median comparison to mean much.
func highVariance(q QueryResult) bool { return q.Warm.P50Ms > 0 && q.Warm.P95Ms > 2*q.Warm.P50Ms }

func signedGap(flat, part float64) float64 { // positive = partitioned faster
	if flat == 0 {
		return 0
	}
	return (flat - part) / flat
}

// Derive fills the partitioning comparison from the query results. The
// full-scan pair is a CONTROL: partition pruning cannot help it, so any gap it
// shows is layout/noise, and the filter pair's gap only counts as pruning to
// the extent it exceeds the control's.
func (r *Report) Derive() {
	byID := map[string]QueryResult{}
	for _, q := range r.Queries {
		byID[q.ID] = q
	}
	pairs := [][3]string{
		{"q2-year-flat", "q2p-year-partitioned", "filter on partition column (l_year = 1995)"},
		{"q3-full-aggregate", "q3p-full-partitioned", "full scan, no pruning possible (control)"},
	}
	var cmps []PartitionComparison
	var vari []bool
	for _, p := range pairs {
		a, okA := byID[p[0]]
		b, okB := byID[p[1]]
		if okA && okB {
			c := compare(a, b, p[2])
			c.ExecVerdict = execVerdict(a, b)
			cmps = append(cmps, c)
			vari = append(vari, highVariance(a) || highVariance(b))
		}
	}
	if len(cmps) == 2 {
		filterGap := signedGap(cmps[0].FlatP50Ms, cmps[0].PartitionedP50Ms)
		controlGap := signedGap(cmps[1].FlatP50Ms, cmps[1].PartitionedP50Ms)
		if absF(filterGap-controlGap) < within && (absF(filterGap) >= within) {
			cmps[0].Verdict += fmt.Sprintf("; NOT attributable to pruning: the unprunable control query shows the same gap (%.0f%% vs %.0f%%)",
				filterGap*100, controlGap*100)
		}
		if absF(controlGap) >= within && absDiff(cmps[1].FlatP50Ms, cmps[1].PartitionedP50Ms) >= minAbsMs {
			cmps[1].Verdict += "; the control differs, so layout or measurement noise affects this comparison"
		}
	}
	for i := range cmps {
		if i < len(vari) && vari[i] {
			cmps[i].Verdict += "; INCONCLUSIVE: high variance (p95 > 2x p50)"
		}
	}
	r.Partitioning = cmps
}

func absF(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}

// Markdown renders the report as GitHub-flavoured Markdown.
func (r *Report) Markdown(w io.Writer) {
	e := r.Environment
	fmt.Fprintf(w, "### Environment\n\n| | |\n|---|---|\n")
	fmt.Fprintf(w, "| Generated | %s |\n| OS / arch | %s / %s |\n| CPU | %s (%d logical) |\n| Memory | %.1f GiB |\n",
		r.GeneratedAt.Format(time.RFC3339), e.GOOS, e.GOARCH, e.CPUModel, e.LogicalCPUs, float64(e.MemoryBytes)/(1<<30))
	fmt.Fprintf(w, "| Go | %s |\n| DuckDB | %s |\n| Commit | `%s` (dirty: %v) |\n| Object store | %s |\n| OS cache | %s |\n\n",
		e.GoVersion, e.DuckDB, e.Commit, e.Dirty, r.Config.ObjectStore, e.OSCache)

	c := r.Config
	fmt.Fprintf(w, "### Configuration\n\nScale %.3g (TPC-H-like, seed %d), %d warm iterations per query. Query limits: timeout %s, %d threads, memory %s, max rows %d, max result bytes %d, max scan bytes %d. %s\n\n",
		c.Scale, c.Seed, c.Iterations, c.Limits.Timeout, c.Limits.Threads, c.Limits.MemoryLimit, c.Limits.MaxRows, c.Limits.MaxResultBytes, c.Limits.MaxScanBytes, c.LimitsNote)

	fmt.Fprintf(w, "### Dataset\n\n| Table | Rows | Files | Size |\n|---|---:|---:|---:|\n")
	for _, t := range r.Dataset.Tables {
		fmt.Fprintf(w, "| %s | %d | %d | %.1f MiB |\n", t.Name, t.Rows, t.Files, float64(t.Bytes)/(1<<20))
	}

	fmt.Fprintf(w, "\n### Query latency (single client, end to end, ms)\n\n")
	fmt.Fprintf(w, "n = %d warm runs per query; with so few samples p95/p99 are at or near the maximum. Every result was checked against ground truth computed by the generator.\n\n", c.Iterations)
	fmt.Fprintf(w, "| Query | first | p50 | p95 | p99 | max | stage p50 | execute p50 | spool p50 | rows | staged MiB |\n|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, q := range r.Queries {
		fmt.Fprintf(w, "| %s | %.0f | %.0f | %.0f | %.0f | %.0f | %.0f | %.0f | %.0f | %d%s | %.1f |\n",
			q.ID, q.FirstMs, q.Warm.P50Ms, q.Warm.P95Ms, q.Warm.P99Ms, q.Warm.MaxMs, q.WarmStage.P50Ms,
			q.WarmExecute.P50Ms, q.WarmSpool.P50Ms, q.RowsReturned, map[bool]string{true: " (trunc.)"}[q.Truncated],
			float64(q.BytesStaged)/(1<<20))
	}

	fmt.Fprintf(w, "\n### Throughput under concurrency\n\n| Workers | Queries | Wall s | Queries/s | p50 ms | p95 ms | p99 ms | Failed |\n|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, k := range r.Concurrency {
		fmt.Fprintf(w, "| %d | %d | %.2f | %.2f | %.0f | %.0f | %.0f | %d |\n", k.Workers, k.Queries, k.WallSeconds,
			k.QueriesPerS, k.Latency.P50Ms, k.Latency.P95Ms, k.Latency.P99Ms, k.Failed)
	}

	fmt.Fprintf(w, "\n### Partitioned vs unpartitioned\n\n| Comparison | flat p50 ms | partitioned p50 ms | flat execute p50 | partitioned execute p50 | bytes staged (flat / partitioned) | End-to-end verdict | Execute-only verdict |\n|---|---:|---:|---:|---:|---|---|---|\n")
	for _, p := range r.Partitioning {
		fmt.Fprintf(w, "| %s | %.0f | %.0f | %.0f | %.0f | %d / %d | %s | %s |\n", p.Pair, p.FlatP50Ms, p.PartitionedP50Ms,
			p.FlatExecP50Ms, p.PartExecP50Ms, p.FlatBytesStaged, p.PartBytesStaged, p.Verdict, p.ExecVerdict)
	}

	fmt.Fprintf(w, "\n### Ingestion throughput (CSV -> Parquet)\n\n| Rows | Input MiB | Output MiB | Files | Seconds | Rows/s | Input MiB/s | Partition |\n|---:|---:|---:|---:|---:|---:|---:|---|\n")
	for _, i := range r.Ingest {
		part := i.PartitionColumn
		if part == "" {
			part = "none"
		}
		fmt.Fprintf(w, "| %d | %.1f | %.1f | %d | %.2f | %.0f | %.1f | %s |\n", i.Rows, float64(i.InputBytes)/(1<<20),
			float64(i.OutputBytes)/(1<<20), i.OutputFiles, i.Seconds, i.RowsPerSecond, i.InputMiBPerSec, part)
	}
	fmt.Fprintf(w, "\n### Limitations of this run\n\n")
	for _, l := range r.Limitations {
		fmt.Fprintf(w, "- %s\n", l)
	}
	_ = strings.TrimSpace
}
