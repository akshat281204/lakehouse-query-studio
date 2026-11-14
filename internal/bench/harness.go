package bench

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/akshat281204/lakehouse-query-studio/internal/catalog"
	"github.com/akshat281204/lakehouse-query-studio/internal/objectstore"
	"github.com/akshat281204/lakehouse-query-studio/internal/parquet"
	"github.com/akshat281204/lakehouse-query-studio/internal/query"
)

// Harness runs the real query path (validator, job store, runner, staging,
// DuckDB, spooling) in one process against an object store.
type Harness struct {
	Store   *catalog.MemoryQueryStore
	Objects objectstore.ReadWriter
	Memory  *objectstore.Memory // non-nil when Objects is in-memory
	Runner  *query.Runner
	Dataset *Dataset
	Limits  query.Limits

	val     *query.Validator
	tmp     string
	tmu     sync.Mutex // guards timings: the hook runs on worker goroutines
	timings []query.Timings
}

// Config configures a harness.
type Config struct {
	Limits  query.Limits
	Workers int
	// Objects, when set, is used instead of an in-memory store (for a real
	// S3/MinIO run). The dataset is then loaded into it.
	Objects objectstore.ReadWriter
}

// NewHarness generates the dataset at scale and wires the query path.
func NewHarness(ctx context.Context, scale float64, cfg Config) (*Harness, error) {
	h := &Harness{Store: catalog.NewMemoryQueryStore(), Limits: cfg.Limits}
	if cfg.Objects != nil {
		h.Objects = cfg.Objects
	} else {
		h.Memory = objectstore.NewMemory()
		h.Objects = h.Memory
	}
	ds, err := Load(ctx, h.Objects, scale)
	if err != nil {
		return nil, fmt.Errorf("load workload: %w", err)
	}
	h.Dataset = ds
	for _, r := range ds.Relations() {
		h.Store.AddRelation(r)
	}
	if h.val, err = query.NewValidator(); err != nil {
		return nil, err
	}
	if h.tmp, err = os.MkdirTemp("", "lqs-bench-"); err != nil {
		return nil, err
	}
	rc := query.DefaultRunnerConfig()
	rc.TmpDir = h.tmp
	rc.Limits = cfg.Limits
	if cfg.Workers > 0 {
		rc.Workers = cfg.Workers
	}
	rc.Poll = time.Millisecond
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h.Runner, err = query.NewRunner(h.Store, h.Objects, query.NewDuckDB(), h.val, "bench", rc, log)
	if err != nil {
		return nil, err
	}
	h.Runner.OnTimings = func(t query.Timings) {
		h.tmu.Lock()
		h.timings = append(h.timings, t)
		h.tmu.Unlock()
	}
	return h, nil
}

// Close releases temporary resources.
func (h *Harness) Close() {
	_ = h.val.Close()
	_ = os.RemoveAll(h.tmp)
}

func (h *Harness) submit(ctx context.Context, stmt string) (string, error) {
	v, err := h.val.Validate(ctx, stmt)
	if err != nil {
		return "", err
	}
	bindings := map[string]string{}
	for _, t := range v.Tables {
		rel, err := h.Store.ResolveRelation(ctx, t.Name)
		if err != nil {
			return "", err
		}
		bindings[t.Name] = rel.VersionID
	}
	id, _ := catalog.NewUUID()
	_, err = h.Store.Submit(ctx, catalog.SubmitQuery{ID: id, Statement: stmt, StatementHash: query.StatementHash(stmt), Bindings: bindings})
	return id, err
}

// Sample is one measured execution.
type Sample struct {
	Total       time.Duration
	Stage       time.Duration
	Execute     time.Duration
	Spool       time.Duration
	Rows        int64
	BytesStaged int64
	ResultBytes int64
	Truncated   bool
}

// Once runs a single query to completion and returns its measurements and
// (for correctness checks) the returned rows.
func (h *Harness) Once(ctx context.Context, stmt string) (Sample, []map[string]any, error) {
	id, err := h.submit(ctx, stmt)
	if err != nil {
		return Sample{}, nil, err
	}
	h.tmu.Lock()
	h.timings = h.timings[:0]
	h.tmu.Unlock()
	start := time.Now()
	if ran, err := h.Runner.RunOnce(ctx); err != nil || !ran {
		return Sample{}, nil, fmt.Errorf("runner: ran=%v err=%v", ran, err)
	}
	total := time.Since(start)
	job, err := h.Store.GetQuery(ctx, id)
	if err != nil {
		return Sample{}, nil, err
	}
	if job.Status != catalog.QuerySucceeded {
		return Sample{}, nil, fmt.Errorf("query %s: %s %s", job.Status, job.FailureReason, job.ErrorSummary)
	}
	s := Sample{Total: total, Rows: *job.RowsReturned, Truncated: job.Truncated, ResultBytes: *job.ResultBytes, BytesStaged: *job.BytesStaged}
	h.tmu.Lock()
	if len(h.timings) == 1 {
		t := h.timings[0]
		s.Stage, s.Execute, s.Spool = t.Stage, t.Execute, t.Spool
	}
	h.tmu.Unlock()
	rr, err := parquet.ReadRows(ctx, h.Objects, job.ResultRef, *job.ResultBytes, 0, 100_000)
	if err != nil {
		return Sample{}, nil, err
	}
	_ = h.Objects.Delete(ctx, job.ResultRef)
	return s, rr.Rows, nil
}

// QueryResult is the measurement of one query across iterations.
type QueryResult struct {
	ID           string  `json:"id"`
	Description  string  `json:"description"`
	SQL          string  `json:"sql"`
	RowsReturned int64   `json:"rows_returned"`
	Truncated    bool    `json:"truncated"`
	BytesStaged  int64   `json:"bytes_staged"`
	ResultBytes  int64   `json:"result_bytes"`
	FirstMs      float64 `json:"first_run_ms"`
	Warm         Summary `json:"warm_total"`
	WarmStage    Summary `json:"warm_stage"`
	WarmExecute  Summary `json:"warm_execute"`
	WarmSpool    Summary `json:"warm_spool"`
	Verified     bool    `json:"result_verified"`
}

// MeasureQuery runs q once ("first": first execution after generation or the
// previous query, OS cache not dropped) and then iterations more times
// ("warm"), verifying the first result against ground truth.
func (h *Harness) MeasureQuery(ctx context.Context, q Query, iterations int) (QueryResult, error) {
	res := QueryResult{ID: q.ID, Description: q.Desc, SQL: q.SQL}
	first, rows, err := h.Once(ctx, q.SQL)
	if err != nil {
		return res, fmt.Errorf("%s: %w", q.ID, err)
	}
	if err := q.Check(rows, first.Rows, h.Dataset.Truth); err != nil {
		return res, fmt.Errorf("%s: WRONG RESULT: %w", q.ID, err)
	}
	res.Verified = true
	res.FirstMs = float64(first.Total) / float64(time.Millisecond)
	res.RowsReturned, res.Truncated, res.BytesStaged, res.ResultBytes = first.Rows, first.Truncated, first.BytesStaged, first.ResultBytes
	var tot, stg, exe, spl []time.Duration
	for i := 0; i < iterations; i++ {
		s, _, err := h.Once(ctx, q.SQL)
		if err != nil {
			return res, fmt.Errorf("%s: %w", q.ID, err)
		}
		tot, stg, exe, spl = append(tot, s.Total), append(stg, s.Stage), append(exe, s.Execute), append(spl, s.Spool)
	}
	res.Warm, res.WarmStage, res.WarmExecute, res.WarmSpool = Summarize(tot), Summarize(stg), Summarize(exe), Summarize(spl)
	return res, nil
}

// MeasurePair measures two queries with their warm runs interleaved (a, b, a,
// b, ...), so slow drift in machine state (page cache, disk, other load) hits
// both equally instead of favouring whichever ran second.
func (h *Harness) MeasurePair(ctx context.Context, a, b Query, iterations int) (QueryResult, QueryResult, error) {
	type acc struct{ tot, stg, exe, spl []time.Duration }
	measure := func(q Query) (QueryResult, error) {
		res := QueryResult{ID: q.ID, Description: q.Desc, SQL: q.SQL}
		first, rows, err := h.Once(ctx, q.SQL)
		if err != nil {
			return res, fmt.Errorf("%s: %w", q.ID, err)
		}
		if err := q.Check(rows, first.Rows, h.Dataset.Truth); err != nil {
			return res, fmt.Errorf("%s: WRONG RESULT: %w", q.ID, err)
		}
		res.Verified, res.FirstMs = true, float64(first.Total)/float64(time.Millisecond)
		res.RowsReturned, res.Truncated, res.BytesStaged, res.ResultBytes = first.Rows, first.Truncated, first.BytesStaged, first.ResultBytes
		return res, nil
	}
	ra, err := measure(a)
	if err != nil {
		return ra, ra, err
	}
	rb, err := measure(b)
	if err != nil {
		return ra, rb, err
	}
	var aa, ab acc
	for i := 0; i < iterations; i++ {
		for _, p := range []struct {
			q *Query
			a *acc
		}{{&a, &aa}, {&b, &ab}} {
			s, _, err := h.Once(ctx, p.q.SQL)
			if err != nil {
				return ra, rb, fmt.Errorf("%s: %w", p.q.ID, err)
			}
			p.a.tot, p.a.stg, p.a.exe, p.a.spl = append(p.a.tot, s.Total), append(p.a.stg, s.Stage), append(p.a.exe, s.Execute), append(p.a.spl, s.Spool)
		}
	}
	ra.Warm, ra.WarmStage, ra.WarmExecute, ra.WarmSpool = Summarize(aa.tot), Summarize(aa.stg), Summarize(aa.exe), Summarize(aa.spl)
	rb.Warm, rb.WarmStage, rb.WarmExecute, rb.WarmSpool = Summarize(ab.tot), Summarize(ab.stg), Summarize(ab.exe), Summarize(ab.spl)
	return ra, rb, nil
}

// ConcurrencyResult is a throughput measurement at one worker count.
type ConcurrencyResult struct {
	Workers     int     `json:"workers"`
	Queries     int     `json:"queries"`
	WallSeconds float64 `json:"wall_seconds"`
	QueriesPerS float64 `json:"queries_per_second"`
	Latency     Summary `json:"latency_submit_to_done"`
	Failed      int     `json:"failed"`
}

// MeasureConcurrency submits n copies of stmts (round-robin) at once and
// drains them with `workers` concurrent query slots. Latency includes queue
// wait, which is what a user sees.
func (h *Harness) MeasureConcurrency(ctx context.Context, stmts []string, n, workers int) (ConcurrencyResult, error) {
	h.Runner.Cfg.Workers = workers
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		id, err := h.submit(ctx, stmts[i%len(stmts)])
		if err != nil {
			return ConcurrencyResult{}, err
		}
		ids = append(ids, id)
	}
	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	start := time.Now()
	go func() { h.Runner.Run(rctx); close(done) }()
	for {
		finished := 0
		for _, id := range ids {
			if j, _ := h.Store.GetQuery(ctx, id); catalog.IsTerminalQuery(j.Status) {
				finished++
			}
		}
		if finished == len(ids) {
			break
		}
		if ctx.Err() != nil {
			return ConcurrencyResult{}, ctx.Err()
		}
		time.Sleep(2 * time.Millisecond)
	}
	wall := time.Since(start)
	cancel()
	<-done

	out := ConcurrencyResult{Workers: workers, Queries: n, WallSeconds: wall.Seconds(), QueriesPerS: float64(n) / wall.Seconds()}
	var lat []time.Duration
	for _, id := range ids {
		j, _ := h.Store.GetQuery(ctx, id)
		if j.Status != catalog.QuerySucceeded {
			out.Failed++
		} else if j.FinishedAt != nil {
			lat = append(lat, j.FinishedAt.Sub(j.CreatedAt))
		}
		if j.ResultRef != "" {
			_ = h.Objects.Delete(ctx, j.ResultRef)
		}
	}
	sort.Slice(lat, func(i, k int) bool { return lat[i] < lat[k] })
	out.Latency = Summarize(lat)
	return out, nil
}

// DuckDBVersion returns the linked DuckDB version.
func DuckDBVersion(ctx context.Context) string {
	req := query.Request{Statement: "SELECT version()", WorkDir: os.TempDir(), Limits: query.DefaultLimits()}
	dir, err := os.MkdirTemp("", "lqs-ver-")
	if err != nil {
		return ""
	}
	defer func() { _ = os.RemoveAll(dir) }()
	req.WorkDir = dir
	res, err := query.NewDuckDB().Execute(ctx, req)
	if err != nil || len(res.Rows) == 0 {
		return ""
	}
	s, _ := res.Rows[0][0].(string)
	return s
}
