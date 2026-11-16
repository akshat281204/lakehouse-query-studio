package query

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/akshat281204/lakehouse-query-studio/internal/parquet"
)

// writeFixture stages a small Parquet file produced by this repo's writer.
func writeFixture(t *testing.T, dir, name string, rows [][]any) {
	t.Helper()
	data, err := parquet.Encode([]parquet.WriteColumn{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "label", Type: parquet.TypeString, Nullable: true},
		{Name: "score", Type: parquet.TypeDouble},
	}, rows)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(filepath.Join(dir, name)), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func fixtureRequest(t *testing.T, stmt string) (Request, string) {
	t.Helper()
	root := t.TempDir()
	data := filepath.Join(root, "sales")
	work := filepath.Join(root, "work")
	if err := os.MkdirAll(work, 0o700); err != nil {
		t.Fatal(err)
	}
	writeFixture(t, data, "part-1.parquet", [][]any{
		{int64(1), "a", 1.5}, {int64(2), nil, 2.5}, {int64(3), "c", 3.5},
	})
	lim := DefaultLimits()
	lim.Timeout = 20 * time.Second
	return Request{
		Statement: stmt,
		Sources:   []Source{{Names: []string{"sales", "sales_v1"}, Dir: data}},
		WorkDir:   work,
		Limits:    lim,
	}, root
}

func TestExecuteQueriesStagedParquet(t *testing.T) {
	req, _ := fixtureRequest(t, "SELECT id, label, score FROM sales ORDER BY id")
	res, err := NewDuckDB().Execute(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 3 || res.Truncated {
		t.Fatalf("rows=%d truncated=%v", len(res.Rows), res.Truncated)
	}
	if res.Rows[0][0] != int64(1) || res.Rows[1][1] != nil || res.Rows[2][2] != 3.5 {
		t.Fatalf("unexpected rows %v", res.Rows)
	}
	if res.PlanSummary == "" || strings.Contains(res.PlanSummary, "sales/") && strings.Contains(res.PlanSummary, os.TempDir()) {
		t.Fatalf("plan summary missing or leaks paths: %q", res.PlanSummary)
	}
}

func TestExecuteAggregatesAndQualifiedNames(t *testing.T) {
	req, _ := fixtureRequest(t, "SELECT count(*) AS n, sum(score) AS s FROM catalog.sales_v1")
	res, err := NewDuckDB().Execute(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if res.Rows[0][0] != int64(3) || res.Rows[0][1] != 7.5 {
		t.Fatalf("rows %v", res.Rows)
	}
}

func TestExecuteTruncatesAtRowLimit(t *testing.T) {
	req, _ := fixtureRequest(t, "SELECT id FROM sales ORDER BY id")
	req.Limits.MaxRows = 2
	res, err := NewDuckDB().Execute(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 2 || !res.Truncated {
		t.Fatalf("rows=%d truncated=%v", len(res.Rows), res.Truncated)
	}
	req.Limits.MaxRows = 3
	res, err = NewDuckDB().Execute(context.Background(), req)
	if err != nil || len(res.Rows) != 3 || res.Truncated {
		t.Fatalf("exact-limit result must not be truncated: %v %v", res, err)
	}
}

func TestExecuteOversizedRowFails(t *testing.T) {
	req, _ := fixtureRequest(t, "SELECT repeat('x', 5000) AS big")
	req.Limits.MaxRowBytes = 1000
	_, err := NewDuckDB().Execute(context.Background(), req)
	if !errors.Is(err, ErrLimit) {
		t.Fatalf("want ErrLimit, got %v", err)
	}
}

func TestExecuteTotalBytesTruncates(t *testing.T) {
	req, _ := fixtureRequest(t, "SELECT repeat('x', 400) AS s FROM sales")
	req.Limits.MaxResultBytes = 1000
	res, err := NewDuckDB().Execute(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Truncated || len(res.Rows) == 0 || len(res.Rows) >= 3 {
		t.Fatalf("rows=%d truncated=%v", len(res.Rows), res.Truncated)
	}
}

func TestExecuteTimeout(t *testing.T) {
	// Bypasses the validator on purpose: proves the engine itself enforces
	// the deadline.
	req, _ := fixtureRequest(t, "SELECT count(*) FROM range(100000000000) a, range(1000000) b")
	req.Limits.Timeout = 500 * time.Millisecond
	start := time.Now()
	_, err := NewDuckDB().Execute(context.Background(), req)
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("want ErrTimeout, got %v", err)
	}
	if time.Since(start) > 10*time.Second {
		t.Fatalf("timeout took %s", time.Since(start))
	}
}

func TestExecuteCancel(t *testing.T) {
	req, _ := fixtureRequest(t, "SELECT count(*) FROM range(100000000000) a, range(1000000) b")
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(300 * time.Millisecond); cancel() }()
	_, err := NewDuckDB().Execute(ctx, req)
	if !errors.Is(err, ErrCancelled) {
		t.Fatalf("want ErrCancelled, got %v", err)
	}
}

func TestExecuteEngineErrorIsClassifiedAndSanitized(t *testing.T) {
	req, root := fixtureRequest(t, "SELECT nope FROM sales")
	_, err := NewDuckDB().Execute(context.Background(), req)
	if !errors.Is(err, ErrEngine) {
		t.Fatalf("want ErrEngine, got %v", err)
	}
	if strings.Contains(err.Error(), root) || strings.Contains(err.Error(), "\n") {
		t.Fatalf("error leaks path or context: %q", err)
	}
}

// The engine-level layer must hold even when the validator is bypassed: these
// statements go straight to DuckDB on the sealed connection.
func TestEngineDeniesWhatTheValidatorWould(t *testing.T) {
	outside := filepath.Join(t.TempDir(), "outside.csv")
	if err := os.WriteFile(outside, []byte("a\n1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	secretParquet := filepath.Join(t.TempDir(), "other")
	writeFixture(t, secretParquet, "x.parquet", [][]any{{int64(9), "secret", 9.9}})

	stmts := []string{
		"SELECT * FROM read_csv('/etc/hosts')",
		"SELECT * FROM read_csv(" + sqlString(outside) + ")",
		"SELECT * FROM read_parquet(" + sqlString(filepath.Join(secretParquet, "x.parquet")) + ")",
		"SELECT * FROM glob('/*')",
		"SELECT * FROM read_text('/etc/hosts')",
		"COPY (SELECT 1) TO " + sqlString(filepath.Join(t.TempDir(), "o.csv")),
		"ATTACH '/tmp/lqs-attach.db'",
		"INSTALL httpfs",
		"LOAD httpfs",
		"SET enable_external_access=true",
		"SET allowed_directories=['/']",
		"SET lock_configuration=false",
		"PRAGMA enable_external_access",
		"CREATE TABLE t AS SELECT 1",
		"SELECT * FROM read_parquet('s3://bucket/uploads/x.parquet')",
		"SELECT * FROM read_csv('http://127.0.0.1:1/x.csv')",
	}
	for _, s := range stmts {
		req, _ := fixtureRequest(t, s)
		res, err := NewDuckDB().Execute(context.Background(), req)
		if err == nil {
			t.Errorf("%q: engine allowed it (rows=%d)", s, len(res.Rows))
		}
	}
	if _, err := os.Stat("/tmp/lqs-attach.db"); err == nil {
		t.Fatal("ATTACH created a file")
	}
}

func TestEngineDoesNotLeakStateBetweenQueries(t *testing.T) {
	req, _ := fixtureRequest(t, "SELECT count(*) FROM sales")
	if _, err := NewDuckDB().Execute(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	req.Statement = "SELECT count(*) FROM catalog.other"
	if _, err := NewDuckDB().Execute(context.Background(), req); err == nil {
		t.Fatal("unbound relation should not resolve")
	}
}

func TestHivePartitionedSource(t *testing.T) {
	root := t.TempDir()
	data := filepath.Join(root, "ds")
	for _, region := range []string{"eu", "us"} {
		d, _ := parquet.Encode([]parquet.WriteColumn{{Name: "id", Type: parquet.TypeInt64}}, [][]any{{int64(1)}, {int64(2)}})
		p := filepath.Join(data, "region="+region)
		_ = os.MkdirAll(p, 0o700)
		_ = os.WriteFile(filepath.Join(p, "part.parquet"), d, 0o600)
	}
	work := filepath.Join(root, "w")
	_ = os.MkdirAll(work, 0o700)
	res, err := NewDuckDB().Execute(context.Background(), Request{
		Statement: "SELECT region, count(*) AS n FROM ds GROUP BY region ORDER BY region",
		Sources:   []Source{{Names: []string{"ds"}, Dir: data, HivePartitioning: true}},
		WorkDir:   work,
		Limits:    DefaultLimits(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 2 || res.Rows[0][0] != "eu" || res.Rows[1][1] != int64(2) {
		t.Fatalf("rows %v", res.Rows)
	}
}

// Aggregation can still amplify memory through allowed functions. This
// bypasses the validator (repeat() with a huge length is denied there) to prove
// the engine-level budget turns a runaway aggregate into a classified error.
func TestExecuteRunawayAggregateIsContained(t *testing.T) {
	req, _ := fixtureRequest(t, "SELECT list(repeat('x', 1000000)) FROM sales, sales, sales, sales, sales, sales, sales, sales")
	req.Limits.MemoryLimit = "64MiB"
	req.Limits.Timeout = 30 * time.Second
	start := time.Now()
	_, err := NewDuckDB().Execute(context.Background(), req)
	if err == nil {
		t.Fatal("unexpectedly succeeded")
	}
	if !errors.Is(err, ErrLimit) && !errors.Is(err, ErrEngine) && !errors.Is(err, ErrTimeout) {
		t.Fatalf("unclassified error: %v", err)
	}
	t.Logf("contained in %s: %v", time.Since(start), err)
}

// Integer SUMs are HUGEINT in DuckDB. They must come back numeric when they
// fit, and exact (text) when they do not, never rounded.
func TestWideIntegersAndDecimalsKeepUsableTypes(t *testing.T) {
	run := func(stmt string) *Result {
		t.Helper()
		req, _ := fixtureRequest(t, stmt)
		res, err := NewDuckDB().Execute(context.Background(), req)
		if err != nil {
			t.Fatalf("%q: %v", stmt, err)
		}
		return res
	}
	if r := run("SELECT sum(id) AS s FROM sales"); r.Columns[0].Type != parquet.TypeInt64 || r.Rows[0][0] != int64(6) {
		t.Errorf("sum(bigint): %+v %v", r.Columns, r.Rows)
	}
	if r := run("SELECT 170141183460469231731687303715884105727::HUGEINT AS h"); r.Columns[0].Type != parquet.TypeString ||
		r.Rows[0][0] != "170141183460469231731687303715884105727" {
		t.Errorf("huge hugeint must stay exact text: %+v %v", r.Columns, r.Rows)
	}
	if r := run("SELECT 18446744073709551615::UBIGINT AS u"); r.Columns[0].Type != parquet.TypeString || r.Rows[0][0] != "18446744073709551615" {
		t.Errorf("ubigint max: %+v %v", r.Columns, r.Rows)
	}
	if r := run("SELECT 42::UBIGINT AS u, NULL::HUGEINT AS n"); r.Columns[0].Type != parquet.TypeInt64 || r.Rows[0][0] != int64(42) || r.Rows[0][1] != nil {
		t.Errorf("small ubigint / null hugeint: %+v %v", r.Columns, r.Rows)
	}
	if r := run("SELECT sum(1.25::DECIMAL(10,2)) AS d FROM sales"); r.Columns[0].Type != parquet.TypeDouble || r.Rows[0][0] != 3.75 {
		t.Errorf("small decimal: %+v %v", r.Columns, r.Rows)
	}
	if r := run("SELECT 1234567890123456789012345.12345::DECIMAL(30,5) AS d"); r.Columns[0].Type != parquet.TypeString {
		t.Errorf("wide decimal must stay text: %+v %v", r.Columns, r.Rows)
	}
}
