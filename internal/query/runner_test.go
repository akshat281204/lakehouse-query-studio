package query

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/akshat281204/lakehouse-query-studio/internal/catalog"
	"github.com/akshat281204/lakehouse-query-studio/internal/objectstore"
	"github.com/akshat281204/lakehouse-query-studio/internal/parquet"
)

type harness struct {
	store   *catalog.MemoryQueryStore
	objects *objectstore.Memory
	runner  *Runner
	rel     catalog.Relation
}

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newHarness(t *testing.T, engine Engine, mutate func(*RunnerConfig)) *harness {
	t.Helper()
	store := catalog.NewMemoryQueryStore()
	objects := objectstore.NewMemory()
	data, err := parquet.Encode([]parquet.WriteColumn{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "label", Type: parquet.TypeString, Nullable: true},
		{Name: "score", Type: parquet.TypeDouble},
	}, [][]any{{int64(1), "a", 1.5}, {int64(2), nil, 2.5}, {int64(3), "c", 3.5}})
	if err != nil {
		t.Fatal(err)
	}
	prefix := "lake/default/sales/version=1/attempt=1/"
	_ = objects.Put(context.Background(), prefix+"part-1.parquet", data)
	rel := catalog.Relation{DatasetName: "sales", Version: 1, ObjectPrefix: prefix, Format: "parquet"}
	store.AddRelation(rel)
	rel, _ = store.ResolveRelation(context.Background(), "sales")

	v, err := NewValidator()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = v.Close() })
	if engine == nil {
		engine = NewDuckDB()
	}
	cfg := DefaultRunnerConfig()
	cfg.TmpDir = t.TempDir()
	cfg.Heartbeat = 20 * time.Millisecond
	cfg.Lease = 2 * time.Second
	cfg.Poll = 10 * time.Millisecond
	cfg.Limits.Timeout = 20 * time.Second
	if mutate != nil {
		mutate(&cfg)
	}
	r, err := NewRunner(store, objects, engine, v, "test-worker", cfg, quietLog())
	if err != nil {
		t.Fatal(err)
	}
	return &harness{store: store, objects: objects, runner: r, rel: rel}
}

func (h *harness) submit(t *testing.T, stmt string) catalog.QueryJob {
	t.Helper()
	id, _ := catalog.NewUUID()
	job, err := h.store.Submit(context.Background(), catalog.SubmitQuery{
		ID: id, Statement: stmt, StatementHash: StatementHash(stmt),
		Bindings: map[string]string{"sales": h.rel.VersionID, "sales_v1": h.rel.VersionID},
	})
	if err != nil {
		t.Fatal(err)
	}
	return job
}

func (h *harness) get(t *testing.T, id string) catalog.QueryJob {
	t.Helper()
	j, err := h.store.GetQuery(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func TestRunnerSucceedsAndSpoolsParquet(t *testing.T) {
	h := newHarness(t, nil, nil)
	job := h.submit(t, "SELECT id, label, score FROM sales ORDER BY id")
	if ran, err := h.runner.RunOnce(context.Background()); err != nil || !ran {
		t.Fatalf("ran=%v err=%v", ran, err)
	}
	got := h.get(t, job.ID)
	if got.Status != catalog.QuerySucceeded {
		t.Fatalf("status=%s reason=%s summary=%s", got.Status, got.FailureReason, got.ErrorSummary)
	}
	if got.RowsReturned == nil || *got.RowsReturned != 3 || got.Truncated || got.ResultBytes == nil || got.BytesStaged == nil || *got.BytesStaged == 0 {
		t.Fatalf("result summary wrong: %+v", got)
	}
	if got.PlanSummary == "" || got.DurationMs == nil || got.Attempt != 1 {
		t.Fatalf("plan/duration/attempt: %+v", got)
	}
	key := ResultKey(job.ID, 1)
	if keys := h.objects.Keys("exports/"); len(keys) != 1 || keys[0] != key {
		t.Fatalf("exports = %v want %s", keys, key)
	}
	rr, err := parquet.ReadRows(context.Background(), h.objects, key, *got.ResultBytes, 0, 10)
	if err != nil || len(rr.Rows) != 3 || rr.Rows[0]["id"] != int64(1) || rr.Rows[1]["label"] != nil {
		t.Fatalf("spool rows: %+v %v", rr, err)
	}
	// Scratch space must be removed.
	if entries, _ := os.ReadDir(h.runner.Cfg.TmpDir); len(entries) != 0 {
		t.Fatalf("scratch left behind: %v", entries)
	}
}

func TestRunnerRevalidatesPersistedStatement(t *testing.T) {
	h := newHarness(t, nil, nil)
	job := h.submit(t, "SELECT * FROM read_csv('/etc/hosts')") // bypasses the API validator
	_, _ = h.runner.RunOnce(context.Background())
	got := h.get(t, job.ID)
	if got.Status != catalog.QueryFailed || got.FailureReason != catalog.ReasonDenied {
		t.Fatalf("status=%s reason=%s", got.Status, got.FailureReason)
	}
	if h.objects.Gets != 0 {
		t.Fatalf("denied query fetched %d objects", h.objects.Gets)
	}
	if strings.Contains(got.ErrorSummary, "/etc/hosts") {
		t.Fatalf("summary echoes statement: %q", got.ErrorSummary)
	}
}

func TestRunnerUnboundRelationIsDenied(t *testing.T) {
	h := newHarness(t, nil, nil)
	job := h.submit(t, "SELECT * FROM other_dataset")
	_, _ = h.runner.RunOnce(context.Background())
	got := h.get(t, job.ID)
	if got.Status != catalog.QueryFailed || got.FailureReason != catalog.ReasonDenied {
		t.Fatalf("status=%s reason=%s", got.Status, got.FailureReason)
	}
}

func TestRunnerEngineErrorRecordedSafely(t *testing.T) {
	h := newHarness(t, nil, nil)
	job := h.submit(t, "SELECT nope FROM sales")
	_, _ = h.runner.RunOnce(context.Background())
	got := h.get(t, job.ID)
	if got.Status != catalog.QueryFailed || got.FailureReason != catalog.ReasonEngineError {
		t.Fatalf("status=%s reason=%s", got.Status, got.FailureReason)
	}
	if got.RowsReturned != nil || got.ResultBytes != nil || len(h.objects.Keys("exports/")) != 0 {
		t.Fatalf("failed query must record no result: %+v", got)
	}
}

func TestRunnerRowLimitTruncation(t *testing.T) {
	h := newHarness(t, nil, func(c *RunnerConfig) { c.Limits.MaxRows = 2 })
	job := h.submit(t, "SELECT id FROM sales ORDER BY id")
	_, _ = h.runner.RunOnce(context.Background())
	got := h.get(t, job.ID)
	if got.Status != catalog.QuerySucceeded || *got.RowsReturned != 2 || !got.Truncated {
		t.Fatalf("%+v", got)
	}
}

func TestRunnerOversizedRowFailsWithoutSpool(t *testing.T) {
	h := newHarness(t, nil, func(c *RunnerConfig) { c.Limits.MaxRowBytes = 100 })
	job := h.submit(t, "SELECT repeat('x', 1000) AS big FROM sales")
	_, _ = h.runner.RunOnce(context.Background())
	got := h.get(t, job.ID)
	if got.Status != catalog.QueryFailed || got.FailureReason != catalog.ReasonResourceExceeded {
		t.Fatalf("status=%s reason=%s", got.Status, got.FailureReason)
	}
	if got.ResultBytes != nil || len(h.objects.Keys("exports/")) != 0 {
		t.Fatal("no spool may exist for an oversized row")
	}
}

func TestRunnerFinalizedSpoolNeverExceedsLimit(t *testing.T) {
	// 400 identical 200-byte rows; the cap sits between the per-row estimate
	// total and the finalized file size so the shrink path must run.
	h := newHarness(t, nil, nil)
	big := make([][]any, 400)
	for i := range big {
		big[i] = []any{int64(i), strings.Repeat("y", 200), 1.0}
	}
	data, _ := parquet.Encode([]parquet.WriteColumn{
		{Name: "id", Type: parquet.TypeInt64}, {Name: "label", Type: parquet.TypeString, Nullable: true}, {Name: "score", Type: parquet.TypeDouble},
	}, big)
	_ = h.objects.Put(context.Background(), h.rel.ObjectPrefix+"part-2.parquet", data)
	h.runner.Cfg.Limits.MaxResultBytes = 20_000
	job := h.submit(t, "SELECT id, label FROM sales")
	_, _ = h.runner.RunOnce(context.Background())
	got := h.get(t, job.ID)
	if got.Status != catalog.QuerySucceeded || !got.Truncated {
		t.Fatalf("%+v", got)
	}
	if *got.ResultBytes > 20_000 {
		t.Fatalf("spool %d bytes exceeds limit", *got.ResultBytes)
	}
	keys := h.objects.Keys("exports/")
	rr, err := parquet.ReadRows(context.Background(), h.objects, keys[0], *got.ResultBytes, 0, 100_000)
	if err != nil || int64(len(rr.Rows)) != *got.RowsReturned {
		t.Fatalf("rows=%d recorded=%d err=%v", len(rr.Rows), *got.RowsReturned, err)
	}
}

func TestRunnerScanLimit(t *testing.T) {
	h := newHarness(t, nil, func(c *RunnerConfig) { c.Limits.MaxScanBytes = 10 })
	job := h.submit(t, "SELECT * FROM sales")
	_, _ = h.runner.RunOnce(context.Background())
	got := h.get(t, job.ID)
	if got.Status != catalog.QueryFailed || got.FailureReason != catalog.ReasonResourceExceeded {
		t.Fatalf("status=%s reason=%s", got.Status, got.FailureReason)
	}
}

func TestRunnerEmptyDatasetFails(t *testing.T) {
	h := newHarness(t, nil, nil)
	_ = h.objects.Delete(context.Background(), h.rel.ObjectPrefix+"part-1.parquet")
	job := h.submit(t, "SELECT * FROM sales")
	_, _ = h.runner.RunOnce(context.Background())
	if got := h.get(t, job.ID); got.Status != catalog.QueryFailed {
		t.Fatalf("status=%s", got.Status)
	}
}

func TestSafeJoinRejectsTraversal(t *testing.T) {
	for _, rel := range []string{"", "../x.parquet", "a/../../x.parquet", "a//b.parquet", "/abs.parquet", "a\\b.parquet", "./x", "a/\x00b"} {
		if _, ok := safeJoin("/tmp/d", rel); ok {
			t.Errorf("%q accepted", rel)
		}
	}
	if p, ok := safeJoin("/tmp/d", "region=eu/part-1.parquet"); !ok || p != "/tmp/d/region=eu/part-1.parquet" {
		t.Errorf("hive path: %q %v", p, ok)
	}
}

// blockingEngine blocks until ctx ends, mapping the cause like DuckDB does.
type blockingEngine struct {
	started chan struct{}
	running atomic.Int32
	peak    atomic.Int32
	timeout time.Duration
}

func (b *blockingEngine) Execute(ctx context.Context, _ Request) (*Result, error) {
	n := b.running.Add(1)
	for {
		p := b.peak.Load()
		if n <= p || b.peak.CompareAndSwap(p, n) {
			break
		}
	}
	defer b.running.Add(-1)
	select {
	case b.started <- struct{}{}:
	default:
	}
	var timer <-chan time.Time
	if b.timeout > 0 {
		timer = time.After(b.timeout)
	}
	select {
	case <-ctx.Done():
		return nil, ErrCancelled
	case <-timer:
		return nil, ErrTimeout
	}
}

func TestRunnerUserCancelOfRunningQuery(t *testing.T) {
	eng := &blockingEngine{started: make(chan struct{}, 1)}
	h := newHarness(t, eng, nil)
	job := h.submit(t, "SELECT * FROM sales")
	done := make(chan struct{})
	go func() { _, _ = h.runner.RunOnce(context.Background()); close(done) }()
	<-eng.started
	if _, err := h.store.RequestCancel(context.Background(), job.ID); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("cancel did not stop the query")
	}
	got := h.get(t, job.ID)
	if got.Status != catalog.QueryCancelled || got.FinishedAt == nil {
		t.Fatalf("%+v", got)
	}
	if len(h.objects.Keys("exports/")) != 0 {
		t.Fatal("cancelled query left a spool")
	}
}

func TestRunnerTimeoutIsDistinctFromFailure(t *testing.T) {
	eng := &blockingEngine{started: make(chan struct{}, 1), timeout: 50 * time.Millisecond}
	h := newHarness(t, eng, nil)
	job := h.submit(t, "SELECT * FROM sales")
	_, _ = h.runner.RunOnce(context.Background())
	if got := h.get(t, job.ID); got.Status != catalog.QueryTimedOut {
		t.Fatalf("status=%s", got.Status)
	}
}

func TestRunnerShutdownLeavesJobRunningForRequeue(t *testing.T) {
	eng := &blockingEngine{started: make(chan struct{}, 1)}
	h := newHarness(t, eng, nil)
	job := h.submit(t, "SELECT * FROM sales")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _, _ = h.runner.RunOnce(ctx); close(done) }()
	<-eng.started
	cancel()
	<-done
	got := h.get(t, job.ID)
	if got.Status != catalog.QueryRunning {
		t.Fatalf("shutdown must not record a terminal state, got %s", got.Status)
	}
	// Lease expiry requeues it, and a fresh worker finishes it for real.
	h.store.ExpireLease(job.ID)
	h.runner.Engine = NewDuckDB()
	if ran, err := h.runner.RunOnce(context.Background()); err != nil || !ran {
		t.Fatalf("ran=%v err=%v", ran, err)
	}
	got = h.get(t, job.ID)
	if got.Status != catalog.QuerySucceeded || got.Attempt != 2 {
		t.Fatalf("status=%s attempt=%d", got.Status, got.Attempt)
	}
}

func TestRunnerFencedWorkerWritesNothing(t *testing.T) {
	eng := &blockingEngine{started: make(chan struct{}, 1)}
	h := newHarness(t, eng, nil)
	job := h.submit(t, "SELECT * FROM sales")
	done := make(chan struct{})
	go func() { _, _ = h.runner.RunOnce(context.Background()); close(done) }()
	<-eng.started

	// The worker "partitions": its lease expires and another worker takes over.
	h.store.ExpireLease(job.ID)
	if n, _ := h.store.RecoverExpired(context.Background()); n != 1 {
		t.Fatalf("recovered %d", n)
	}
	second, err := h.store.ClaimQuery(context.Background(), "other", time.Minute)
	if err != nil || second == nil || second.Attempt != 2 {
		t.Fatalf("second claim: %+v %v", second, err)
	}
	select {
	case <-done: // the heartbeat saw ErrFenced and aborted the first worker
	case <-time.After(5 * time.Second):
		t.Fatal("fenced worker kept running")
	}
	got := h.get(t, job.ID)
	if got.Status != catalog.QueryRunning || got.Attempt != 2 || got.FinishedAt != nil {
		t.Fatalf("stale worker changed the job: %+v", got)
	}
	if err := h.store.Succeed(context.Background(), job.ID, job.FencingToken, catalog.QueryResult{}); !errors.Is(err, catalog.ErrFenced) {
		t.Fatalf("stale token accepted: %v", err)
	}
}

func TestRunnerBoundsConcurrency(t *testing.T) {
	eng := &blockingEngine{started: make(chan struct{}, 8), timeout: 150 * time.Millisecond}
	h := newHarness(t, eng, func(c *RunnerConfig) { c.Workers = 2 })
	ids := make([]string, 5)
	for i := range ids {
		ids[i] = h.submit(t, "SELECT * FROM sales").ID
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); h.runner.Run(ctx) }()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		all := true
		for _, id := range ids {
			if !catalog.IsTerminalQuery(h.get(t, id).Status) {
				all = false
			}
		}
		if all {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	wg.Wait()
	if p := eng.peak.Load(); p > 2 || p < 2 {
		t.Fatalf("peak concurrency = %d, want exactly the 2 configured workers", p)
	}
	for _, id := range ids {
		if s := h.get(t, id).Status; s != catalog.QueryTimedOut {
			t.Fatalf("%s: %s", id, s)
		}
	}
}

func TestRecoveryGivesUpAfterMaxAttempts(t *testing.T) {
	h := newHarness(t, nil, nil)
	job := h.submit(t, "SELECT * FROM sales")
	ctx := context.Background()
	for i := 0; i < catalog.MaxQueryAttempts; i++ {
		c, err := h.store.ClaimQuery(ctx, "dying-worker", time.Minute)
		if err != nil || c == nil {
			t.Fatalf("claim %d: %v %v", i, c, err)
		}
		h.store.ExpireLease(job.ID)
		if n, _ := h.store.RecoverExpired(ctx); n != 1 {
			t.Fatalf("recover %d: %d", i, n)
		}
	}
	got := h.get(t, job.ID)
	if got.Status != catalog.QueryFailed || got.FailureReason != catalog.ReasonWorkerInterrupted {
		t.Fatalf("status=%s reason=%s", got.Status, got.FailureReason)
	}
}

func TestQueuedQueryCancelsImmediatelyAndNeverRuns(t *testing.T) {
	h := newHarness(t, nil, nil)
	job := h.submit(t, "SELECT * FROM sales")
	got, err := h.store.RequestCancel(context.Background(), job.ID)
	if err != nil || got.Status != catalog.QueryCancelled {
		t.Fatalf("%+v %v", got, err)
	}
	if ran, _ := h.runner.RunOnce(context.Background()); ran {
		t.Fatal("cancelled query was claimed")
	}
	if _, err := h.store.RequestCancel(context.Background(), job.ID); !errors.Is(err, catalog.ErrInvalidState) {
		t.Fatalf("second cancel: %v", err)
	}
}

func TestQueueDepthLimit(t *testing.T) {
	h := newHarness(t, nil, nil)
	h.store.IngestQueued = 1
	id, _ := catalog.NewUUID()
	_, err := h.store.Submit(context.Background(), catalog.SubmitQuery{ID: id, Statement: "SELECT 1", MaxQueued: 1})
	if !errors.Is(err, catalog.ErrQueueFull) {
		t.Fatalf("want ErrQueueFull, got %v", err)
	}
}

func TestRunnerConfigBounds(t *testing.T) {
	good := DefaultRunnerConfig()
	if err := good.Validate(); err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(*RunnerConfig){
		"workers 0":          func(c *RunnerConfig) { c.Workers = 0 },
		"workers 17":         func(c *RunnerConfig) { c.Workers = 17 },
		"lease too long":     func(c *RunnerConfig) { c.Lease = 601 * time.Second },
		"heartbeat >= lease": func(c *RunnerConfig) { c.Heartbeat = c.Lease },
		"timeout > 300s":     func(c *RunnerConfig) { c.Limits.Timeout = 301 * time.Second },
		"rows > 100k":        func(c *RunnerConfig) { c.Limits.MaxRows = 100_001 },
		"result bytes > 1G":  func(c *RunnerConfig) { c.Limits.MaxResultBytes = 1<<30 + 1 },
		"row bytes > 16M":    func(c *RunnerConfig) { c.Limits.MaxRowBytes = 16<<20 + 1 },
	}
	for name, mut := range cases {
		c := DefaultRunnerConfig()
		mut(&c)
		if err := c.Validate(); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

type panicEngine struct{}

func (panicEngine) Execute(context.Context, Request) (*Result, error) { panic("boom") }

func TestRunnerSurvivesEnginePanic(t *testing.T) {
	h := newHarness(t, panicEngine{}, nil)
	job := h.submit(t, "SELECT * FROM sales")
	if ran, err := h.runner.RunOnce(context.Background()); err != nil || !ran {
		t.Fatalf("ran=%v err=%v", ran, err)
	}
	got := h.get(t, job.ID)
	if got.Status != catalog.QueryFailed || got.FailureReason != catalog.ReasonInternal {
		t.Fatalf("status=%s reason=%s", got.Status, got.FailureReason)
	}
	if strings.Contains(got.ErrorSummary, "boom") {
		t.Fatalf("panic value leaked into the summary: %q", got.ErrorSummary)
	}
	if entries, _ := os.ReadDir(h.runner.Cfg.TmpDir); len(entries) != 0 {
		t.Fatalf("scratch left behind after panic: %v", entries)
	}
}

func TestCleanStaleScratch(t *testing.T) {
	dir := t.TempDir()
	old := dir + "/" + scratchPrefix + "old"
	fresh := dir + "/" + scratchPrefix + "fresh"
	other := dir + "/unrelated"
	for _, d := range []string{old, fresh, other} {
		if err := os.MkdirAll(d+"/data", 0o700); err != nil {
			t.Fatal(err)
		}
	}
	past := time.Now().Add(-3 * time.Hour)
	if err := os.Chtimes(old, past, past); err != nil {
		t.Fatal(err)
	}
	if n := CleanStaleScratch(dir, time.Hour); n != 1 {
		t.Fatalf("removed %d", n)
	}
	for d, want := range map[string]bool{old: false, fresh: true, other: true} {
		if _, err := os.Stat(d); (err == nil) != want {
			t.Errorf("%s exists=%v want %v", d, err == nil, want)
		}
	}
}
