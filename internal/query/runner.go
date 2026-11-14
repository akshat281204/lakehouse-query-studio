package query

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/akshat281204/lakehouse-query-studio/internal/catalog"
	"github.com/akshat281204/lakehouse-query-studio/internal/objectstore"
	"github.com/akshat281204/lakehouse-query-studio/internal/parquet"
	"github.com/akshat281204/lakehouse-query-studio/internal/telemetry"
)

// maxStagedFiles bounds how many objects one query may stage.
const maxStagedFiles = 10_000

var (
	errCancelRequested = errors.New("cancel requested")
	errFenced          = errors.New("job fenced")
)

// RunnerConfig holds the worker-side knobs (design §12).
type RunnerConfig struct {
	Workers   int           // concurrent RUNNING queries (default 2)
	Lease     time.Duration // QUERY_LEASE_DURATION (default 120s)
	Heartbeat time.Duration // default Lease/3
	Poll      time.Duration // idle poll interval
	TmpDir    string        // parent of per-query scratch directories
	Limits    Limits
}

// DefaultRunnerConfig returns the design defaults.
func DefaultRunnerConfig() RunnerConfig {
	return RunnerConfig{
		Workers: 2, Lease: 120 * time.Second, Heartbeat: 40 * time.Second,
		Poll: time.Second, TmpDir: os.TempDir(), Limits: DefaultLimits(),
	}
}

// Runner claims durable query jobs and executes them. Run it from the worker
// process. It is the only component that reads dataset objects for queries
// and the only writer of the exports/ prefix.
type Runner struct {
	Store     catalog.QueryStore
	Objects   objectstore.ReadWriter
	Engine    Engine
	Validator *Validator
	Owner     string
	Cfg       RunnerConfig
	Log       *slog.Logger
	// Metrics is optional; every recorded label comes from a closed set.
	Metrics *telemetry.Metrics
	// OnTimings, if set, is called after each executed query with where its
	// time went. Used by the benchmark; carries no statement text.
	OnTimings func(Timings)
}

// Timings breaks one query's wall time into its stages.
type Timings struct {
	Stage       time.Duration // download dataset files to local scratch
	Execute     time.Duration // DuckDB execution and result materialization
	Spool       time.Duration // encode and write the result
	BytesStaged int64
	Err         error
}

// NewRunner builds a Runner, applying defaults for zero config fields.
func NewRunner(store catalog.QueryStore, objects objectstore.ReadWriter, engine Engine, v *Validator,
	owner string, cfg RunnerConfig, log *slog.Logger) (*Runner, error) {
	d := DefaultRunnerConfig()
	if cfg.Workers == 0 {
		cfg.Workers = d.Workers
	}
	if cfg.Lease == 0 {
		cfg.Lease = d.Lease
	}
	if cfg.Heartbeat == 0 {
		cfg.Heartbeat = cfg.Lease / 3
	}
	if cfg.Poll == 0 {
		cfg.Poll = d.Poll
	}
	if cfg.TmpDir == "" {
		cfg.TmpDir = d.TmpDir
	}
	if cfg.Limits.Timeout == 0 {
		cfg.Limits = d.Limits
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Runner{Store: store, Objects: objects, Engine: engine, Validator: v, Owner: owner, Cfg: cfg, Log: log}, nil
}

// Validate rejects configurations outside the design bounds (§12).
func (c RunnerConfig) Validate() error {
	if c.Workers < 1 || c.Workers > 16 {
		return fmt.Errorf("query workers must be between 1 and 16")
	}
	if c.Lease <= 0 || c.Lease > 600*time.Second {
		return fmt.Errorf("query lease must be between 1s and 600s")
	}
	if c.Heartbeat <= 0 || c.Heartbeat >= c.Lease {
		return fmt.Errorf("query heartbeat must be positive and shorter than the lease")
	}
	if c.Limits.Timeout <= 0 || c.Limits.Timeout > 300*time.Second {
		return fmt.Errorf("query timeout must be between 1ns and 300s")
	}
	if c.Limits.MaxRows < 1 || c.Limits.MaxRows > 100_000 {
		return fmt.Errorf("max result rows must be between 1 and 100000")
	}
	if c.Limits.MaxResultBytes < 1 || c.Limits.MaxResultBytes > 1<<30 {
		return fmt.Errorf("max result bytes must be between 1 and 1 GiB")
	}
	if c.Limits.MaxRowBytes < 1 || c.Limits.MaxRowBytes > 16<<20 {
		return fmt.Errorf("max row bytes must be between 1 and 16 MiB")
	}
	return nil
}

// Run claims and executes jobs until ctx is cancelled, then waits for in-flight
// queries to stop. A query interrupted by shutdown is left RUNNING on purpose:
// its lease expires and it is requeued, which is the truthful restart story.
func (r *Runner) Run(ctx context.Context) {
	if n := CleanStaleScratch(r.Cfg.TmpDir, time.Hour); n > 0 {
		r.Log.Warn("removed stale query scratch directories", "count", n)
	}
	if r.Metrics != nil {
		r.Metrics.WorkerSlots.Set(float64(r.Cfg.Workers))
	}
	slots := make(chan struct{}, r.Cfg.Workers)
	var wg sync.WaitGroup
	defer wg.Wait()
	for ctx.Err() == nil {
		if n, err := r.Store.RecoverExpired(ctx); err != nil && ctx.Err() == nil {
			r.Log.Error("query recovery failed", "error", err.Error())
		} else if n > 0 {
			r.Log.Warn("recovered expired queries", "count", n)
		}
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			return
		}
		job, err := r.Store.ClaimQuery(ctx, r.Owner, r.Cfg.Lease)
		if err != nil || job == nil {
			<-slots
			if err != nil && ctx.Err() == nil {
				r.Log.Error("query claim failed", "error", err.Error())
			}
			select {
			case <-ctx.Done():
			case <-time.After(r.Cfg.Poll):
			}
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-slots }()
			r.Process(ctx, *job)
		}()
	}
}

// RunOnce claims and processes a single job synchronously (tests).
func (r *Runner) RunOnce(ctx context.Context) (bool, error) {
	if _, err := r.Store.RecoverExpired(ctx); err != nil {
		return false, err
	}
	job, err := r.Store.ClaimQuery(ctx, r.Owner, r.Cfg.Lease)
	if err != nil || job == nil {
		return false, err
	}
	r.Process(ctx, *job)
	return true, nil
}

// StatementHash is the SHA-256 of the whitespace-trimmed statement.
func StatementHash(statement string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(statement)))
	return hex.EncodeToString(sum[:])
}

// ResultKey is the spool object key for a query attempt (design §4).
func ResultKey(queryID string, attempt int) string {
	return fmt.Sprintf("exports/%s/%s/attempt=%d/result.parquet", catalog.NamespaceDefault, queryID, attempt)
}

// Process runs one claimed job to a terminal state (or abandons it when fenced
// or shutting down). It never logs SQL text or credentials.
func (r *Runner) Process(ctx context.Context, job catalog.QueryJob) {
	start := time.Now()
	if r.Metrics != nil {
		r.Metrics.WorkerInflight.Add(1)
		defer r.Metrics.WorkerInflight.Add(-1)
	}
	jobCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		r.heartbeat(jobCtx, job, cancel)
	}()
	defer func() { cancel(nil); <-hbDone }()

	res, bytesStaged, err := r.safeExecute(jobCtx, job)
	dur := time.Since(start)

	// Why did the context end? Decide before touching the store.
	cause := context.Cause(jobCtx)
	switch {
	case errors.Is(cause, errFenced):
		r.Log.Warn("query fenced, discarding work", "query", job.ID, "attempt", job.Attempt)
		r.count("abandoned", "", dur)
		return
	case ctx.Err() != nil && err != nil:
		r.Log.Info("query interrupted by shutdown; it will be requeued after its lease expires", "query", job.ID)
		r.count("abandoned", "", dur)
		return
	}

	// Store writes use a context detached from cancellation so a user cancel
	// can still be recorded.
	sctx, scancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer scancel()

	switch {
	case err == nil:
		r.succeed(sctx, job, res, bytesStaged, dur)
	case errors.Is(cause, errCancelRequested) || errors.Is(err, ErrCancelled):
		r.finish(sctx, job, catalog.QueryCancelled, "", "cancelled by request", dur)
	case errors.Is(err, ErrTimeout):
		r.finish(sctx, job, catalog.QueryTimedOut, "", "query exceeded the execution timeout", dur)
	case errors.Is(err, ErrDenied):
		r.finish(sctx, job, catalog.QueryFailed, catalog.ReasonDenied, err.Error(), dur)
	case errors.Is(err, ErrLimit):
		r.finish(sctx, job, catalog.QueryFailed, catalog.ReasonResourceExceeded, strip(err, ErrLimit), dur)
	case errors.Is(err, errStorage):
		r.finish(sctx, job, catalog.QueryFailed, catalog.ReasonObjectStore, "dataset storage was unavailable", dur)
	case errors.Is(err, ErrEngine):
		r.finish(sctx, job, catalog.QueryFailed, catalog.ReasonEngineError, strip(err, ErrEngine), dur)
	default:
		r.Log.Error("query failed unexpectedly", "query", job.ID, "error", classifyLog(err))
		r.finish(sctx, job, catalog.QueryFailed, catalog.ReasonInternal, "internal error", dur)
	}
}

var errStorage = errors.New("object store error")

// safeExecute converts a panic anywhere in staging, validation, the cgo
// engine or spooling into a failed job, so one bad query cannot take the
// worker (and every other running query) down with it.
func (r *Runner) safeExecute(ctx context.Context, job catalog.QueryJob) (sp *spooled, staged int64, err error) {
	defer func() {
		if p := recover(); p != nil {
			r.Log.Error("query panicked", "query", job.ID)
			sp, staged, err = nil, 0, fmt.Errorf("internal error")
		}
	}()
	return r.execute(ctx, job)
}

const scratchPrefix = "lqs-query-"

// CleanStaleScratch removes per-query scratch directories older than maxAge
// that a crashed worker left behind (staged dataset bytes must not linger on
// disk). It returns how many it removed.
func CleanStaleScratch(dir string, maxAge time.Duration) int {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	n := 0
	for _, e := range entries {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), scratchPrefix) {
			continue
		}
		info, err := e.Info()
		if err != nil || time.Since(info.ModTime()) < maxAge {
			continue
		}
		if os.RemoveAll(filepath.Join(dir, e.Name())) == nil {
			n++
		}
	}
	return n
}

func strip(err, sentinel error) string {
	msg := strings.TrimPrefix(err.Error(), sentinel.Error()+": ")
	if len(msg) > 300 {
		msg = msg[:300]
	}
	return msg
}

// classifyLog keeps unexpected errors out of logs unless they are plainly safe.
func classifyLog(err error) string {
	var pe *os.PathError
	if errors.As(err, &pe) {
		return "filesystem error"
	}
	return "unexpected error"
}

func (r *Runner) heartbeat(ctx context.Context, job catalog.QueryJob, cancel context.CancelCauseFunc) {
	t := time.NewTicker(r.Cfg.Heartbeat)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			hctx, hcancel := context.WithTimeout(ctx, r.Cfg.Heartbeat)
			cancelReq, err := r.Store.Heartbeat(hctx, job.ID, job.FencingToken, r.Cfg.Lease)
			hcancel()
			switch {
			case errors.Is(err, catalog.ErrFenced):
				cancel(errFenced)
				return
			case err != nil:
				r.Log.Warn("query heartbeat failed", "query", job.ID, "error", "store unavailable")
			case cancelReq:
				cancel(errCancelRequested)
				return
			}
		}
	}
}

// count records a finished attempt in metrics (outcome and reason are closed
// sets; duration is wall time from claim).
func (r *Runner) count(outcome, reason string, dur time.Duration) {
	if r.Metrics == nil {
		return
	}
	r.Metrics.QueryJobs.Inc(outcome)
	if reason != "" {
		r.Metrics.QueryFailures.Inc(reason)
	}
	r.Metrics.QueryDuration.Observe(dur.Seconds())
}

func (r *Runner) succeed(ctx context.Context, job catalog.QueryJob, res *spooled, staged int64, dur time.Duration) {
	err := r.Store.Succeed(ctx, job.ID, job.FencingToken, catalog.QueryResult{
		ResultRef: res.key, RowsReturned: int64(res.rows), Truncated: res.truncated,
		ResultBytes: res.bytes, BytesStaged: staged, PlanSummary: res.plan, Duration: dur,
	})
	if errors.Is(err, catalog.ErrFenced) {
		// Lost the race: our spool is attempt-scoped and never referenced.
		_ = r.Objects.Delete(ctx, res.key)
		r.Log.Warn("query result fenced", "query", job.ID)
		r.count("abandoned", "", dur)
		return
	}
	if err != nil {
		r.Log.Error("recording query success failed", "query", job.ID, "error", "store error")
		return
	}
	r.count("succeeded", "", dur)
	if r.Metrics != nil {
		r.Metrics.QueryRowsReturned.Add(float64(res.rows))
	}
	r.Log.Info("query succeeded", "query", job.ID, "rows", res.rows, "truncated", res.truncated,
		"duration_ms", dur.Milliseconds(), "statement_hash", job.StatementHash)
}

func (r *Runner) finish(ctx context.Context, job catalog.QueryJob, status, reason, summary string, dur time.Duration) {
	err := r.Store.Finish(ctx, job.ID, job.FencingToken, status, reason, summary, dur)
	if errors.Is(err, catalog.ErrFenced) {
		r.Log.Warn("query finish fenced", "query", job.ID)
		r.count("abandoned", "", dur)
		return
	}
	if err != nil {
		r.Log.Error("recording query end failed", "query", job.ID, "error", "store error")
		return
	}
	r.count(map[string]string{catalog.QueryFailed: "failed", catalog.QueryCancelled: "cancelled", catalog.QueryTimedOut: "timed_out"}[status], reason, dur)
	r.Log.Info("query finished", "query", job.ID, "status", status, "reason", reason,
		"duration_ms", dur.Milliseconds(), "statement_hash", job.StatementHash)
}

type spooled struct {
	key       string
	rows      int
	truncated bool
	bytes     int64
	plan      string
}

func (r *Runner) execute(ctx context.Context, job catalog.QueryJob) (*spooled, int64, error) {
	// Re-validate the persisted statement against the §7 boundary: the worker
	// never trusts that the API (or a previous version of this code) did.
	v, err := r.Validator.Validate(ctx, job.Statement)
	if err != nil {
		return nil, 0, err
	}
	byVersion := map[string]*Source{}
	var order []string
	for _, t := range v.Tables {
		id, ok := job.Bindings[t.Name]
		if !ok {
			return nil, 0, denied("a referenced relation was not resolved at submission")
		}
		if _, ok := byVersion[id]; !ok {
			byVersion[id] = &Source{}
			order = append(order, id)
		}
		byVersion[id].Names = append(byVersion[id].Names, t.Name)
	}

	root, err := os.MkdirTemp(r.Cfg.TmpDir, scratchPrefix)
	if err != nil {
		return nil, 0, fmt.Errorf("scratch directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(root) }()
	work := filepath.Join(root, "work")
	if err := os.Mkdir(work, 0o700); err != nil {
		return nil, 0, fmt.Errorf("scratch directory: %w", err)
	}

	var staged int64
	var tm Timings
	if r.OnTimings != nil {
		defer func() { tm.BytesStaged = staged; r.OnTimings(tm) }()
	}
	stageStart := time.Now()
	req := Request{Statement: job.Statement, WorkDir: work, Limits: r.Cfg.Limits}
	for i, id := range order {
		rel, err := r.Store.RelationByVersionID(ctx, id)
		if err != nil {
			if errors.Is(err, catalog.ErrNotFound) {
				return nil, 0, fmt.Errorf("%w: a dataset version is no longer available", ErrEngine)
			}
			return nil, 0, err
		}
		dir := filepath.Join(root, fmt.Sprintf("data%d", i))
		n, err := r.stage(ctx, rel.ObjectPrefix, dir, r.Cfg.Limits.MaxScanBytes-staged)
		if err != nil {
			return nil, 0, err
		}
		staged += n
		src := byVersion[id]
		src.Dir, src.HivePartitioning = dir, rel.HivePartitioned
		req.Sources = append(req.Sources, *src)
	}

	tm.Stage = time.Since(stageStart)
	execStart := time.Now()
	res, err := r.Engine.Execute(ctx, req)
	tm.Execute = time.Since(execStart)
	if err != nil {
		tm.Err = err
		return nil, staged, err
	}
	spoolStart := time.Now()
	sp, err := r.spool(ctx, job, res)
	tm.Spool, tm.Err = time.Since(spoolStart), err
	return sp, staged, err
}

// stage downloads every Parquet object under prefix into dir, preserving the
// relative key so hive partition directories survive. DuckDB never sees
// credentials or object URLs: it reads only these local files.
func (r *Runner) stage(ctx context.Context, prefix, dir string, budget int64) (int64, error) {
	prefix = strings.TrimRight(prefix, "/") + "/"
	var total int64
	files := 0
	token := ""
	for {
		page, err := r.Objects.List(ctx, objectstore.ListOptions{Prefix: prefix, MaxKeys: 1000, Token: token})
		if err != nil {
			if ctx.Err() != nil {
				return 0, ctx.Err()
			}
			return 0, fmt.Errorf("%w: list: %v", errStorage, err)
		}
		for _, o := range page.Objects {
			if !strings.HasSuffix(strings.ToLower(o.Key), ".parquet") || o.Size == 0 {
				continue
			}
			rel := strings.TrimPrefix(o.Key, prefix)
			local, ok := safeJoin(dir, rel)
			if !ok {
				return 0, fmt.Errorf("%w: dataset contains an unsupported object key", ErrEngine)
			}
			files++
			if files > maxStagedFiles {
				return 0, fmt.Errorf("%w: dataset has too many files to query", ErrLimit)
			}
			total += o.Size
			if total > budget {
				return 0, fmt.Errorf("%w: dataset is larger than the per-query scan limit", ErrLimit)
			}
			data, err := r.Objects.Get(ctx, o.Key, o.Size+1)
			if err != nil {
				if ctx.Err() != nil {
					return 0, ctx.Err()
				}
				if errors.Is(err, objectstore.ErrTooLarge) {
					return 0, fmt.Errorf("%w: dataset object changed during staging", ErrEngine)
				}
				return 0, fmt.Errorf("%w: get: %v", errStorage, err)
			}
			if err := os.MkdirAll(filepath.Dir(local), 0o700); err != nil {
				return 0, fmt.Errorf("stage: %w", err)
			}
			if err := os.WriteFile(local, data, 0o600); err != nil {
				return 0, fmt.Errorf("stage: %w", err)
			}
		}
		if !page.Truncated || page.NextToken == "" {
			break
		}
		token = page.NextToken
	}
	if files == 0 {
		return 0, fmt.Errorf("%w: dataset has no Parquet files", ErrEngine)
	}
	return total, nil
}

// safeJoin maps an object-key suffix to a path under dir, rejecting anything
// that could escape it.
func safeJoin(dir, rel string) (string, bool) {
	if rel == "" || strings.ContainsAny(rel, "\x00\\") {
		return "", false
	}
	for _, seg := range strings.Split(rel, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return "", false
		}
	}
	p := filepath.Join(dir, filepath.FromSlash(rel))
	if !strings.HasPrefix(p, filepath.Clean(dir)+string(filepath.Separator)) {
		return "", false
	}
	return p, true
}

// spool encodes the bounded result as Parquet, enforces the finalized-size
// limit on the actual file, and writes it to this attempt's exports key.
func (r *Runner) spool(ctx context.Context, job catalog.QueryJob, res *Result) (*spooled, error) {
	rows, truncated := res.Rows, res.Truncated
	data, err := parquet.Encode(res.Columns, rows)
	if err != nil {
		return nil, fmt.Errorf("encode result: %w", err)
	}
	max := r.Cfg.Limits.MaxResultBytes
	for tries := 0; int64(len(data)) > max; tries++ {
		if tries >= 12 || len(rows) <= 1 {
			return nil, fmt.Errorf("%w: result does not fit the result size limit", ErrLimit)
		}
		keep := int(float64(len(rows)) * float64(max) / float64(len(data)) * 0.95)
		if keep >= len(rows) {
			keep = len(rows) - 1
		}
		rows, truncated = rows[:keep], true
		if data, err = parquet.Encode(res.Columns, rows); err != nil {
			return nil, fmt.Errorf("encode result: %w", err)
		}
	}
	key := ResultKey(job.ID, job.Attempt)
	if err := r.Objects.Put(ctx, key, data); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("%w: put result: %v", errStorage, err)
	}
	return &spooled{key: key, rows: len(rows), truncated: truncated, bytes: int64(len(data)), plan: res.PlanSummary}, nil
}
