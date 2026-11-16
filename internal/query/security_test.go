package query

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/akshat281204/lakehouse-query-studio/internal/catalog"
	"github.com/akshat281204/lakehouse-query-studio/internal/logging"
	"github.com/akshat281204/lakehouse-query-studio/internal/parquet"
	"github.com/akshat281204/lakehouse-query-studio/internal/telemetry"
)

// Sentinel SQL literals, identifiers, paths and object keys are pushed through
// every outcome (success, engine error, denial, unbound relation, panic,
// cancel, timeout). None may appear in logs or metrics.
func TestSentinelsNeverReachLogsOrMetrics(t *testing.T) {
	var logs bytes.Buffer
	log, err := logging.New(&logs, "debug", "json")
	if err != nil {
		t.Fatal(err)
	}
	m := telemetry.New("test")
	ctx := context.Background()

	h := newHarness(t, nil, nil)
	h.runner.Log, h.runner.Metrics = log, m

	// A dataset whose object key carries a sentinel.
	data, _ := parquet.Encode([]parquet.WriteColumn{{Name: "id", Type: parquet.TypeInt64}}, [][]any{{int64(1)}})
	prefix := "lake/default/SENT-KEY-6/version=1/attempt=1/"
	_ = h.objects.Put(ctx, prefix+"part-0.parquet", data)
	h.store.AddRelation(catalog.Relation{DatasetName: "sentds", Version: 1, ObjectPrefix: prefix, Format: "parquet"})
	rel, _ := h.store.ResolveRelation(ctx, "sentds")

	submit := func(stmt string, bindings map[string]string) {
		id, _ := catalog.NewUUID()
		if _, err := h.store.Submit(ctx, catalog.SubmitQuery{ID: id, Statement: stmt, StatementHash: StatementHash(stmt), Bindings: bindings}); err != nil {
			t.Fatal(err)
		}
	}
	sales := map[string]string{"sales": h.rel.VersionID}
	submit("SELECT 'SENT-LIT-1' AS lit, id FROM sales", sales)
	submit("SELECT SENT_COL_2 FROM sales", sales)
	submit("SELECT * FROM read_csv('/etc/SENT-PATH-3')", nil)
	submit("SELECT * FROM SENT_TABLE_4", nil)
	submit("SELECT id FROM sentds WHERE id <> 0 -- SENT-COMMENT-7", map[string]string{"sentds": rel.VersionID})
	for {
		ran, err := h.runner.RunOnce(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !ran {
			break
		}
	}

	// Panic and timeout paths.
	for _, eng := range []Engine{panicEngine{}, &blockingEngine{started: make(chan struct{}, 1), timeout: 20 * time.Millisecond}} {
		h.runner.Engine = eng
		submit("SELECT 'SENT-LIT-5' FROM sales", sales)
		if _, err := h.runner.RunOnce(ctx); err != nil {
			t.Fatal(err)
		}
	}

	// Cancel path.
	eng := &blockingEngine{started: make(chan struct{}, 1)}
	h.runner.Engine = eng
	id, _ := catalog.NewUUID()
	stmt := "SELECT 'SENT-LIT-8' FROM sales"
	_, _ = h.store.Submit(ctx, catalog.SubmitQuery{ID: id, Statement: stmt, StatementHash: StatementHash(stmt), Bindings: sales})
	done := make(chan struct{})
	go func() { _, _ = h.runner.RunOnce(ctx); close(done) }()
	<-eng.started
	_, _ = h.store.RequestCancel(ctx, id)
	<-done

	var metrics bytes.Buffer
	m.Registry.Render(&metrics)
	outs := map[string]string{"logs": logs.String(), "metrics": metrics.String()}
	for name, out := range outs {
		if out == "" {
			t.Fatalf("%s empty: test would be vacuous", name)
		}
		for _, bad := range []string{"SENT-", "SENT_", "etc/", "read_csv", "SELECT", "version=1", "part-0"} {
			if strings.Contains(out, bad) {
				t.Errorf("%s contain %q:\n%s", name, bad, out)
			}
		}
	}
	// And the runs really happened.
	for _, want := range []string{`outcome="succeeded"`, `outcome="failed"`, `outcome="cancelled"`, `outcome="timed_out"`,
		`reason="query_denied"`, `reason="engine_error"`, `reason="internal"`} {
		if !strings.Contains(outs["metrics"], want) {
			t.Errorf("metrics missing %s:\n%s", want, outs["metrics"])
		}
	}
	if !strings.Contains(outs["logs"], "statement_hash") {
		t.Errorf("logs should carry the statement hash:\n%s", outs["logs"])
	}
}

// A query on one dataset must not be able to reach a sibling prefix, another
// dataset, an upload, or another query's export, even by guessing keys.
func TestStagingNeverCrossesPrefixBoundaries(t *testing.T) {
	h := newHarness(t, nil, nil)
	ctx := context.Background()
	secret, _ := parquet.Encode([]parquet.WriteColumn{{Name: "id", Type: parquet.TypeInt64}, {Name: "label", Type: parquet.TypeString, Nullable: true}, {Name: "score", Type: parquet.TypeDouble}},
		[][]any{{int64(999), "TOP-SECRET", 9.9}})
	// "sales" lives at lake/default/sales/version=1/attempt=1/
	for _, k := range []string{
		"lake/default/sales/version=1/attempt=2/part-9.parquet",    // a losing/stale attempt
		"lake/default/sales/version=2/attempt=1/part-9.parquet",    // another version
		"lake/default/sales-private/version=1/attempt=1/p.parquet", // sibling dataset sharing a name prefix
		"lake/default/sales/version=1/attempt=10/p.parquet",        // attempt=1 is a string prefix of attempt=10
		"uploads/default/sales/job/leak.parquet",
		"exports/default/other/attempt=1/result.parquet",
		"staging/default/sales/job/attempt=1/p.parquet",
	} {
		_ = h.objects.Put(ctx, k, secret)
	}
	job := h.submit(t, "SELECT label FROM sales")
	if _, err := h.runner.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	got := h.get(t, job.ID)
	if got.Status != catalog.QuerySucceeded || *got.RowsReturned != 3 {
		t.Fatalf("%s rows=%v", got.Status, got.RowsReturned)
	}
	rr, _ := parquet.ReadRows(ctx, h.objects, got.ResultRef, *got.ResultBytes, 0, 100)
	for _, row := range rr.Rows {
		if row["label"] == "TOP-SECRET" {
			t.Fatal("a query read an object outside its dataset prefix")
		}
	}
}

// The sealed engine honours the configured budgets; user SQL cannot raise them.
func TestEngineBudgetsAreAppliedAndLocked(t *testing.T) {
	req, _ := fixtureRequest(t, "SELECT current_setting('threads') AS t, current_setting('memory_limit') AS m, current_setting('enable_external_access') AS e")
	req.Limits.Threads = 3
	req.Limits.MemoryLimit = "123MiB"
	res, err := NewDuckDB().Execute(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if res.Rows[0][0] != "3" && res.Rows[0][0] != int64(3) {
		t.Errorf("threads = %v", res.Rows[0][0])
	}
	if s, _ := res.Rows[0][1].(string); !strings.Contains(s, "MiB") && !strings.Contains(s, "MB") {
		t.Errorf("memory_limit = %v", res.Rows[0][1])
	}
	if res.Rows[0][2] != false && res.Rows[0][2] != "false" {
		t.Errorf("enable_external_access = %v", res.Rows[0][2])
	}
	for _, s := range []string{"SET threads=64", "SET memory_limit='64GB'", "RESET threads", "SET temp_directory='/tmp'"} {
		req, _ := fixtureRequest(t, s)
		if _, err := NewDuckDB().Execute(context.Background(), req); err == nil {
			t.Errorf("%q succeeded on a locked configuration", s)
		}
	}
}
