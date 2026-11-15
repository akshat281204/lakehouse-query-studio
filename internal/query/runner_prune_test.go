package query

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/akshat281204/lakehouse-query-studio/internal/catalog"
	"github.com/akshat281204/lakehouse-query-studio/internal/objectstore"
	"github.com/akshat281204/lakehouse-query-studio/internal/parquet"
)

// addFiles puts n extra Parquet files under the relation's prefix. File k holds
// ids [1000*k, 1000*k+2] with region "r<k>", so each file has a tight,
// distinct min/max.
func addFiles(t testing.TB, h *harness, n int) {
	t.Helper()
	for k := 1; k <= n; k++ {
		data, err := parquet.Encode([]parquet.WriteColumn{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "label", Type: parquet.TypeString, Nullable: true},
			{Name: "score", Type: parquet.TypeDouble},
		}, [][]any{
			{int64(1000 * k), fmt.Sprintf("r%d", k), float64(k) + 0.5},
			{int64(1000*k + 1), fmt.Sprintf("r%d", k), float64(k) + 0.6},
			{int64(1000*k + 2), nil, float64(k) + 0.7},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := h.objects.Put(context.Background(), fmt.Sprintf("%spart-%d.parquet", h.rel.ObjectPrefix, k+1), data); err != nil {
			t.Fatal(err)
		}
	}
}

type runOut struct {
	rows []string
	tm   Timings
}

func (h *harness) runCapture(t *testing.T, stmt string) runOut {
	t.Helper()
	var tm Timings
	h.runner.OnTimings = func(x Timings) { tm = x }
	job := h.submit(t, stmt)
	if ran, err := h.runner.RunOnce(context.Background()); err != nil || !ran {
		t.Fatalf("ran=%v err=%v", ran, err)
	}
	got := h.get(t, job.ID)
	if got.Status != catalog.QuerySucceeded {
		t.Fatalf("%q: status=%s reason=%s summary=%s", stmt, got.Status, got.FailureReason, got.ErrorSummary)
	}
	rr, err := parquet.ReadRows(context.Background(), h.objects, ResultKey(job.ID, 1), *got.ResultBytes, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	var rows []string
	for _, r := range rr.Rows {
		rows = append(rows, fmt.Sprint(r))
	}
	sort.Strings(rows)
	return runOut{rows: rows, tm: tm}
}

// The core guarantee: pruning changes how many bytes are staged, never the answer.
func TestPruningNeverChangesResults(t *testing.T) {
	on := newHarness(t, nil, nil)
	off := newHarness(t, nil, func(c *RunnerConfig) { c.DisablePruning = true })
	addFiles(t, on, 5)
	addFiles(t, off, 5)

	cases := []struct {
		sql        string
		wantPruned bool
	}{
		{`SELECT id, label FROM sales WHERE id >= 3000`, true},
		{`SELECT id FROM sales WHERE id = 2001`, true},
		{`SELECT id FROM sales WHERE id BETWEEN 1000 AND 2000 ORDER BY id`, true},
		{`SELECT id FROM sales WHERE id IN (1, 4001)`, true},
		{`SELECT label, count(*) FROM sales WHERE label = 'r3' GROUP BY label`, true},
		{`SELECT id FROM sales WHERE score > 4.0`, true},
		{`SELECT id FROM sales WHERE id < 3 AND label <> 'zzz'`, true},
		{`SELECT id FROM sales WHERE 3000 <= id`, true},
		{`SELECT s.id FROM sales s WHERE s.id > 4500`, true},
		{`SELECT id FROM sales WHERE id = 999999`, true}, // matches nothing: every file ruled out
		{`SELECT id FROM sales WHERE id = 1 OR id = 4001`, false},
		{`SELECT id FROM sales WHERE label IS NULL`, false},
		{`SELECT id FROM sales`, false},
		{`SELECT count(*) FROM sales WHERE id + 1 = 2`, false},
	}
	for _, c := range cases {
		a, b := on.runCapture(t, c.sql), off.runCapture(t, c.sql)
		if fmt.Sprint(a.rows) != fmt.Sprint(b.rows) {
			t.Errorf("%s\n pruned:   %v\n unpruned: %v", c.sql, a.rows, b.rows)
		}
		if c.wantPruned && (a.tm.FilesPruned == 0 || a.tm.BytesStaged >= b.tm.BytesStaged) {
			t.Errorf("%s: expected pruning, pruned=%d staged=%d vs %d", c.sql, a.tm.FilesPruned, a.tm.BytesStaged, b.tm.BytesStaged)
		}
		if !c.wantPruned && a.tm.FilesPruned != 0 {
			t.Errorf("%s: pruned %d files but should not have", c.sql, a.tm.FilesPruned)
		}
		if b.tm.FilesPruned != 0 {
			t.Errorf("%s: pruning disabled but pruned %d", c.sql, b.tm.FilesPruned)
		}
		if a.tm.FilesStaged+a.tm.FilesPruned != a.tm.FilesListed {
			t.Errorf("%s: accounting: staged %d + pruned %d != listed %d", c.sql, a.tm.FilesStaged, a.tm.FilesPruned, a.tm.FilesListed)
		}
	}
}

func TestPruningStagesOnlyMatchingFile(t *testing.T) {
	h := newHarness(t, nil, nil)
	addFiles(t, h, 5)
	out := h.runCapture(t, `SELECT id FROM sales WHERE id = 3001`)
	if len(out.rows) != 1 || out.tm.FilesStaged != 1 || out.tm.FilesListed != 6 || out.tm.FilesPruned != 5 {
		t.Fatalf("rows=%v timings=%+v", out.rows, out.tm)
	}
	if out.tm.BytesPruned <= 0 {
		t.Fatalf("BytesPruned = %d", out.tm.BytesPruned)
	}
}

func TestPrunedFilesDoNotCountAgainstTheScanLimit(t *testing.T) {
	h := newHarness(t, nil, nil)
	addFiles(t, h, 5)
	var one int64
	for _, k := range h.objects.Keys(h.rel.ObjectPrefix) {
		o, _ := h.objects.Get(context.Background(), k, 1<<20)
		one = int64(len(o))
		break
	}
	// A limit that fits one file but not six: an unfiltered query is refused,
	// a selective one that prunes down to a single file runs.
	h.runner.Cfg.Limits.MaxScanBytes = one*2 + 100
	job := h.submit(t, `SELECT id FROM sales`)
	_, _ = h.runner.RunOnce(context.Background())
	if got := h.get(t, job.ID); got.Status != catalog.QueryFailed || got.FailureReason != catalog.ReasonResourceExceeded {
		t.Fatalf("unfiltered: status=%s reason=%s", got.Status, got.FailureReason)
	}
	out := h.runCapture(t, `SELECT id FROM sales WHERE id = 5000`)
	if len(out.rows) != 1 {
		t.Fatalf("rows=%v", out.rows)
	}
}

func TestStageCacheServesRepeatQueriesWithoutDownloading(t *testing.T) {
	h := newHarness(t, nil, func(c *RunnerConfig) { c.StageCacheBytes = 8 << 20 })
	addFiles(t, h, 3)
	first := h.runCapture(t, `SELECT id FROM sales ORDER BY id`)
	if first.tm.CacheHits != 0 || first.tm.FilesStaged != 4 {
		t.Fatalf("first run: %+v", first.tm)
	}
	getsAfterFirst := h.objects.Gets
	second := h.runCapture(t, `SELECT id FROM sales ORDER BY id`)
	if second.tm.CacheHits != 4 {
		t.Fatalf("second run cache hits = %d, want 4", second.tm.CacheHits)
	}
	if h.objects.Gets != getsAfterFirst {
		t.Fatalf("second run downloaded %d objects", h.objects.Gets-getsAfterFirst)
	}
	if fmt.Sprint(first.rows) != fmt.Sprint(second.rows) {
		t.Fatalf("cached answer differs: %v vs %v", first.rows, second.rows)
	}
	if s := h.runner.CacheStats(); s.Hits != 4 || s.Misses != 4 {
		t.Fatalf("cache stats: %+v", s)
	}
	if err := h.runner.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestChangedObjectIsNotServedFromTheCache(t *testing.T) {
	h := newHarness(t, nil, func(c *RunnerConfig) { c.StageCacheBytes = 8 << 20 })
	before := h.runCapture(t, `SELECT id FROM sales ORDER BY id`)
	// Replace the only object with different content (different size).
	data, err := parquet.Encode([]parquet.WriteColumn{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "label", Type: parquet.TypeString, Nullable: true},
		{Name: "score", Type: parquet.TypeDouble},
	}, [][]any{{int64(42), "z", 9.5}})
	if err != nil {
		t.Fatal(err)
	}
	key := h.objects.Keys(h.rel.ObjectPrefix)[0]
	_ = h.objects.Put(context.Background(), key, data)
	after := h.runCapture(t, `SELECT id FROM sales ORDER BY id`)
	if fmt.Sprint(before.rows) == fmt.Sprint(after.rows) || len(after.rows) != 1 || after.tm.CacheHits != 0 {
		t.Fatalf("stale cache: before=%v after=%v hits=%d", before.rows, after.rows, after.tm.CacheHits)
	}
}

// The cost of pruning when it cannot skip anything: 200 files, footer
// statistics already memoised (every query after the first on a dataset).
func BenchmarkPruneObjectsMemoised(b *testing.B) {
	h := newHarness(b, nil, nil)
	addFiles(b, h, 199)
	cons := []Constraint{{Column: "id", Op: "=", Values: []literal{{kind: litInt, i: 0}}}}
	objs := listObjects(b, h)
	prefix := h.rel.ObjectPrefix
	h.runner.pruneObjects(context.Background(), objs, prefix, cons, false) // populate the memo
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.runner.pruneObjects(context.Background(), objs, prefix, cons, false)
	}
}

// The one-time cost: reading and parsing 200 footers with two range reads each.
func BenchmarkPruneObjectsColdFooters(b *testing.B) {
	h := newHarness(b, nil, nil)
	addFiles(b, h, 199)
	cons := []Constraint{{Column: "id", Op: "=", Values: []literal{{kind: litInt, i: 0}}}}
	objs := listObjects(b, h)
	prefix := h.rel.ObjectPrefix
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.runner.stats = statsMemo{}
		h.runner.pruneObjects(context.Background(), objs, prefix, cons, false)
	}
}

func listObjects(tb testing.TB, h *harness) []objectstore.Object {
	tb.Helper()
	page, err := h.objects.List(context.Background(), objectstore.ListOptions{Prefix: h.rel.ObjectPrefix, MaxKeys: 1000})
	if err != nil {
		tb.Fatal(err)
	}
	return page.Objects
}

// BenchmarkTrivialQuery measures a SELECT 1 through the whole runner (claim,
// validate, scratch dir, stage, DuckDB, spool, finish) against in-memory stores,
// and reports where the time goes. It is the floor for any query's latency.
func BenchmarkTrivialQuery(b *testing.B) {
	h := newHarness(b, nil, nil)
	var stage, exec, spool time.Duration
	var n int
	h.runner.OnTimings = func(t Timings) { stage += t.Stage; exec += t.Execute; spool += t.Spool; n++ }
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id, _ := catalog.NewUUID()
		if _, err := h.store.Submit(context.Background(), catalog.SubmitQuery{
			ID: id, Statement: "SELECT 1 AS one", StatementHash: StatementHash("SELECT 1 AS one"), Bindings: map[string]string{},
		}); err != nil {
			b.Fatal(err)
		}
		if ran, err := h.runner.RunOnce(context.Background()); err != nil || !ran {
			b.Fatalf("ran=%v err=%v", ran, err)
		}
	}
	b.StopTimer()
	if n > 0 {
		b.ReportMetric(float64(stage.Microseconds())/float64(n), "stage-us")
		b.ReportMetric(float64(exec.Microseconds())/float64(n), "execute-us")
		b.ReportMetric(float64(spool.Microseconds())/float64(n), "spool-us")
	}
}
