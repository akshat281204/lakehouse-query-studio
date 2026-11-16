package bench

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/akshat281204/lakehouse-query-studio/internal/query"
)

// OptimizationVariant is one runner configuration measured on one query.
type OptimizationVariant struct {
	Name        string  `json:"name"`
	Pruning     bool    `json:"file_pruning"`
	CacheBytes  int64   `json:"stage_cache_bytes"`
	FirstRunMs  float64 `json:"first_run_ms"`
	Total       Summary `json:"warm_total"`
	Stage       Summary `json:"warm_stage"`
	Execute     Summary `json:"warm_execute"`
	BytesStaged int64   `json:"bytes_staged"`
	FilesListed int     `json:"files_listed"`
	FilesStaged int     `json:"files_staged"`
	FilesPruned int     `json:"files_pruned"`
	CacheHits   int     `json:"cache_hits_per_run"`
	PruneWork   bool    `json:"pruning_had_work"`
}

// OptimizationResult compares the variants on one statement. Answers are
// verified identical across variants, so a faster variant cannot be wrong.
type OptimizationResult struct {
	ID             string                `json:"id"`
	Description    string                `json:"description"`
	SQL            string                `json:"sql"`
	RowsReturned   int64                 `json:"rows_returned"`
	AnswersMatch   bool                  `json:"answers_identical_across_variants"`
	Variants       []OptimizationVariant `json:"variants"`
	PruningVerdict string                `json:"pruning_verdict"`
	CacheVerdict   string                `json:"cache_verdict"`
}

type variantSpec struct {
	name    string
	pruning bool
	cache   int64
}

// OptimizationQuery is a statement measured by MeasureOptimizations.
type OptimizationQuery struct {
	ID, Desc, SQL string
}

// OptimizationQueries are chosen to show where pruning can and cannot help:
// a filter on the hive partition column, a range on the column the flat files
// are clustered by, a filter on a column spread across every file (a control
// that cannot prune), and an unfiltered scan.
func OptimizationQueries() []OptimizationQuery {
	return []OptimizationQuery{
		{"o1-partition-column", "filter on the hive partition column (l_year = 1995), partitioned layout",
			"SELECT l_returnflag, count(*) AS n FROM lineitem_part WHERE l_year = 1995 GROUP BY l_returnflag ORDER BY l_returnflag"},
		{"o2-clustered-range", "range on the column the flat files are clustered by (l_orderkey), flat layout",
			"SELECT count(*) AS n, sum(l_extendedprice) AS rev FROM lineitem WHERE l_orderkey BETWEEN 1 AND 20000"},
		{"o3-spread-column", "filter on a column present in every file (l_year), flat layout: cannot prune",
			"SELECT l_returnflag, count(*) AS n FROM lineitem WHERE l_year = 1995 GROUP BY l_returnflag ORDER BY l_returnflag"},
		{"o4-point-lookup", "single-order lookup on the clustered key, orders table",
			"SELECT o_orderkey, o_custkey, o_totalprice FROM orders WHERE o_orderkey = 17"},
		{"o5-full-scan", "unfiltered aggregate: nothing to prune; shows the cache alone",
			"SELECT l_returnflag, l_linestatus, count(*) AS n, sum(l_quantity) AS qty FROM lineitem GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus"},
	}
}

// MeasureOptimizations runs q under three runner configurations, interleaved
// (variant a, b, c, a, b, c, ...) so drift in machine state hits all equally:
//
//	baseline            no pruning, no cache (what every earlier result used)
//	+ file pruning      skip files whose statistics or directory names rule them out
//	+ pruning + cache   additionally keep staged objects on local disk
//
// Warm cache numbers are the repeated-identical-access best case.
func (h *Harness) MeasureOptimizations(ctx context.Context, q OptimizationQuery, iterations int, cacheBytes int64) (OptimizationResult, error) {
	res := OptimizationResult{ID: q.ID, Description: q.Desc, SQL: q.SQL}
	specs := []variantSpec{
		{"baseline (no pruning, no cache)", false, 0},
		{"file pruning", true, 0},
		{"file pruning + stage cache", true, cacheBytes},
	}
	saved := h.Runner
	defer func() { h.Runner = saved }()

	runners := make([]*runnerHandle, len(specs))
	for i, sp := range specs {
		r, err := h.newRunner(!sp.pruning, sp.cache)
		if err != nil {
			return res, err
		}
		runners[i] = &runnerHandle{spec: sp, r: r}
	}

	// Warm-up run per variant: verifies the answer against the baseline's and
	// populates the cache and the OS page cache.
	var want []string
	for i, rh := range runners {
		h.Runner = rh.r
		s, rows, err := h.Once(ctx, q.SQL)
		if err != nil {
			return res, fmt.Errorf("%s [%s]: %w", q.ID, rh.spec.name, err)
		}
		rh.firstMs = float64(s.Total) / float64(time.Millisecond)
		got := canonical(rows)
		if i == 0 {
			want, res.RowsReturned = got, s.Rows
		} else if fmt.Sprint(got) != fmt.Sprint(want) {
			return res, fmt.Errorf("%s: WRONG RESULT: %q returned a different answer than the baseline", q.ID, rh.spec.name)
		}
	}
	res.AnswersMatch = true

	for it := 0; it < iterations; it++ {
		for _, rh := range runners {
			h.Runner = rh.r
			s, _, err := h.Once(ctx, q.SQL)
			if err != nil {
				return res, fmt.Errorf("%s [%s]: %w", q.ID, rh.spec.name, err)
			}
			rh.tot, rh.stg, rh.exe = append(rh.tot, s.Total), append(rh.stg, s.Stage), append(rh.exe, s.Execute)
			rh.last = s
		}
	}
	for _, rh := range runners {
		res.Variants = append(res.Variants, OptimizationVariant{
			Name: rh.spec.name, Pruning: rh.spec.pruning, CacheBytes: rh.spec.cache, FirstRunMs: rh.firstMs,
			Total: Summarize(rh.tot), Stage: Summarize(rh.stg), Execute: Summarize(rh.exe),
			BytesStaged: rh.last.BytesStaged, FilesListed: rh.last.FilesListed,
			FilesStaged: rh.last.FilesStaged, FilesPruned: rh.last.FilesPruned, CacheHits: rh.last.CacheHits,
			PruneWork: rh.last.PruneWork,
		})
	}
	res.derive()
	return res, nil
}

type runnerHandle struct {
	spec          variantSpec
	r             *query.Runner
	tot, stg, exe []time.Duration
	last          Sample
	firstMs       float64
}

func canonical(rows []map[string]any) []string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		keys := make([]string, 0, len(r))
		for k := range r {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		s := ""
		for _, k := range keys {
			s += fmt.Sprintf("%s=%v;", k, r[k])
		}
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

// derive writes verdicts using the same rule as the layout comparison: a
// difference between medians is only reported when it exceeds both 10% and 5 ms.
func (r *OptimizationResult) derive() {
	if len(r.Variants) != 3 {
		return
	}
	base, prune, cache := r.Variants[0], r.Variants[1], r.Variants[2]
	r.PruningVerdict = verdict("file pruning", base.Total.P50Ms, prune.Total.P50Ms)
	switch {
	case !prune.PruneWork:
		// Both variants ran the same work; whatever gap the medians show is
		// the measurement noise floor, not an effect of pruning.
		r.PruningVerdict = fmt.Sprintf("pruning had nothing to decide here (no usable filter, or a single file), so baseline and pruning variants ran identical work; the p50 gap (%.0f vs %.0f ms) is measurement noise", base.Total.P50Ms, prune.Total.P50Ms)
	case prune.FilesPruned == 0:
		r.PruningVerdict += "; footers were checked but no files could be ruled out, so none were skipped"
	default:
		saved := base.BytesStaged - prune.BytesStaged
		r.PruningVerdict += fmt.Sprintf("; staged %d of %d files, %.1f MiB less (%.0f%% of the baseline bytes)",
			prune.FilesStaged, prune.FilesListed, float64(saved)/(1<<20), 100*float64(saved)/float64(max(base.BytesStaged, 1)))
	}
	r.CacheVerdict = verdict("stage cache (on top of pruning)", prune.Total.P50Ms, cache.Total.P50Ms)
	if cache.CacheHits > 0 {
		r.CacheVerdict += fmt.Sprintf("; %d of %d staged files served from the cache", cache.CacheHits, cache.FilesStaged)
	}
	for _, v := range r.Variants {
		if v.Total.P50Ms > 0 && v.Total.P95Ms > 2*v.Total.P50Ms {
			r.PruningVerdict += "; INCONCLUSIVE: high variance (p95 > 2x p50)"
			break
		}
	}
}

func verdict(what string, before, after float64) string {
	switch {
	case before == 0 || after == 0:
		return what + ": not measured"
	case relDiff(before, after) < within || absDiff(before, after) < minAbsMs:
		return fmt.Sprintf("%s: no measurable end-to-end difference (under %.0f%% or %.0f ms; p50 %.0f vs %.0f ms)", what, within*100, minAbsMs, before, after)
	case after < before:
		return fmt.Sprintf("%s: %.0f%% faster end to end (p50 %.0f vs %.0f ms)", what, (1-after/before)*100, after, before)
	}
	return fmt.Sprintf("%s: %.0f%% slower end to end (p50 %.0f vs %.0f ms)", what, (after/before-1)*100, after, before)
}
