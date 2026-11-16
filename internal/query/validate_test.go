package query

import (
	"context"
	"errors"
	"testing"
)

func newValidator(t *testing.T) *Validator {
	t.Helper()
	v, err := NewValidator()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = v.Close() })
	return v
}

func TestValidateAllowed(t *testing.T) {
	v := newValidator(t)
	ok := []string{
		"SELECT 1",
		"SELECT * FROM sales",
		"select a, lower(b), a + 1, a || 'x' FROM catalog.sales_v2 WHERE a > 1 AND b LIKE 'x%' ORDER BY a LIMIT 5",
		"SELECT count(*), sum(a), avg(a) FROM sales GROUP BY b HAVING count(*) > 1",
		"WITH t AS (SELECT * FROM sales) SELECT * FROM t",
		"SELECT s.id, o.id FROM sales s JOIN orders o ON s.id = o.sid",
		"SELECT * FROM sales WHERE a IN (SELECT a FROM orders)",
		"SELECT CASE WHEN a > 1 THEN 'x' ELSE 'y' END, CAST(a AS VARCHAR), a::DOUBLE, TRY_CAST(a AS INT) FROM sales",
		"SELECT a FROM sales WHERE a BETWEEN 1 AND 3 AND b IS NOT NULL AND c ILIKE '%z' AND d NOT IN (1,2)",
		"SELECT row_number() OVER (PARTITION BY a ORDER BY b), lag(a) OVER (ORDER BY b) FROM sales",
		"SELECT l[1], s.f, m['k'], struct_extract(s, 'f') FROM sales",
		"SELECT unnest(l) FROM sales",
		"SELECT a FROM sales UNION ALL SELECT a FROM orders",
		"SELECT date_trunc('day', ts), extract(year FROM ts), year(ts), strftime(ts, '%Y') FROM sales",
		"SELECT regexp_matches(a, 'x'), a ~ 'x', a SIMILAR TO 'x' FROM sales",
		"SELECT coalesce(a, 0), nullif(a, 0), a // 2, a % 2, a ^ 2, -a FROM sales",
		"SELECT string_agg(a, ','), list(a), quantile_cont(a, 0.5), count(DISTINCT a) FILTER (WHERE a > 1) FROM sales",
		"SELECT * FROM unnest([1,2,3])",
		"SELECT current_date, now(), typeof(a) FROM sales",
		"SELECT a FROM \"my-data\"",
		"SELECT repeat('ab', 3), lpad(a, 10, '0'), rpad(a, 1048576, ' ') FROM sales",
	}
	for _, q := range ok {
		if _, err := v.Validate(context.Background(), q); err != nil {
			t.Errorf("%q: unexpectedly denied: %v", q, err)
		}
	}
}

func TestValidateTables(t *testing.T) {
	v := newValidator(t)
	got, err := v.Validate(context.Background(),
		"WITH t AS (SELECT * FROM sales) SELECT * FROM t JOIN catalog.Orders_v2 o ON true WHERE x IN (SELECT y FROM sales)")
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Tables) != 2 || got.Tables[0].Name != "orders_v2" || got.Tables[1].Name != "sales" {
		t.Fatalf("tables = %+v", got.Tables)
	}
}

func TestValidateDenied(t *testing.T) {
	v := newValidator(t)
	bad := []string{
		"",
		"   ",
		"INSERT INTO t VALUES (1)",
		"UPDATE t SET a = 1",
		"DELETE FROM t",
		"CREATE TABLE t (a int)",
		"CREATE VIEW v AS SELECT 1",
		"DROP TABLE t",
		"ALTER TABLE t ADD COLUMN b int",
		"COPY t TO '/tmp/x.csv'",
		"COPY (SELECT 1) TO '/tmp/x.csv'",
		"EXPORT DATABASE '/tmp/x'",
		"ATTACH '/tmp/x.db'",
		"DETACH x",
		"INSTALL httpfs",
		"LOAD httpfs",
		"FORCE INSTALL httpfs",
		"PRAGMA version",
		"SET threads = 100",
		"RESET threads",
		"CALL pragma_version()",
		"BEGIN",
		"COMMIT",
		"USE memory",
		"EXPLAIN SELECT 1",
		"EXPLAIN ANALYZE SELECT 1",
		"VALUES (1), (2)",
		"SELECT 1; SELECT 2",
		"SELECT 1; DROP TABLE t",
		"SELECT * FROM read_csv('/etc/passwd')",
		"SELECT * FROM read_parquet('s3://bucket/uploads/x.parquet')",
		"SELECT * FROM read_parquet('/tmp/x.parquet')",
		"SELECT * FROM '/etc/passwd'",
		"SELECT * FROM 's3://bucket/x.parquet'",
		"SELECT * FROM 'http://example.com/x.csv'",
		"SELECT * FROM read_json_auto('x.json')",
		"SELECT * FROM glob('/*')",
		"SELECT * FROM range(10)",
		"SELECT * FROM generate_series(1, 10)",
		"SELECT * FROM duckdb_tables()",
		"SELECT * FROM information_schema.tables",
		"SELECT * FROM main.sales",
		"SELECT * FROM memory.main.sales",
		"SELECT * FROM system.main.sales",
		"SELECT read_text('/etc/passwd')",
		"SELECT current_setting('s3_secret_access_key')",
		"SELECT gen_random_uuid()",
		"SELECT random()",
		"SELECT version()",
		"SELECT http_get('http://x')",
		"SELECT getenv('HOME')",
		"SELECT current_user",
		"SELECT * FROM sales, (SELECT read_csv_auto('x'))",
		"SELECT a FROM sales WHERE a IN (SELECT * FROM read_csv('/etc/hosts'))",
		"WITH t AS (SELECT * FROM read_csv('/etc/hosts')) SELECT * FROM t",
		"SELECT other.lower(a) FROM sales",
		"SELECT list_transform(l, x -> x + 1) FROM sales",
		"SELECT struct_pack(a := 1)",
		"SELECT list_concat(l, l) FROM sales",
		"SELECT repeat('x', 4000000000)",
		"SELECT repeat('x', 1048577)",
		"SELECT repeat('x', a) FROM sales",
		"SELECT lpad('x', 4000000000, 'y')",
		"SELECT rpad(label, length(label) * 1000000, 'y') FROM sales",
		"WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t) SELECT n FROM t",
		"SELECT * FROM sales PIVOT (sum(a) FOR b IN ('x'))",
		"SELECT * FROM sales AT (VERSION => 1)",
		"SELECT $1",
		"SELECT ?",
		"SELECT query('select 1')",
		"SELECT * FROM query_table('sales')",
		"SELECT a FROM sales COLLATE nocase",
	}
	for _, q := range bad {
		_, err := v.Validate(context.Background(), q)
		if err == nil {
			t.Errorf("%q: expected denial", q)
			continue
		}
		if !errors.Is(err, ErrDenied) {
			t.Errorf("%q: not an ErrDenied: %v", q, err)
		}
	}
}

func TestValidateLimits(t *testing.T) {
	v := newValidator(t)
	big := "SELECT '" + string(make([]byte, 0)) + "'"
	for len(big) <= MaxSQLBytes {
		big += " || 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'"
	}
	if _, err := v.Validate(context.Background(), big); !errors.Is(err, ErrDenied) {
		t.Fatalf("oversized statement: %v", err)
	}
	deep := "SELECT "
	for i := 0; i < 400; i++ {
		deep += "("
	}
	deep += "1"
	for i := 0; i < 400; i++ {
		deep += ")"
	}
	if _, err := v.Validate(context.Background(), deep); err == nil {
		t.Log("deep nesting accepted (parser flattened it)")
	}
}

func TestDeniedErrorDoesNotEchoStatement(t *testing.T) {
	v := newValidator(t)
	_, err := v.Validate(context.Background(), "SELECT * FROM read_csv('/etc/super-secret-path')")
	if err == nil {
		t.Fatal("expected denial")
	}
	if got := err.Error(); contains(got, "super-secret-path") {
		t.Fatalf("error echoes statement: %s", got)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
