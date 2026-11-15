package query

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/akshat281204/lakehouse-query-studio/internal/parquet"
)

type fileReader struct{ b []byte }

func (r fileReader) Range(_ context.Context, _ string, off, n int64) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(r.b[off : off+n])), nil
}

// DuckDB (an independent Parquet implementation) must read back the same
// statistics our writer emits.
func TestWriterStatisticsAgreeWithDuckDB(t *testing.T) {
	data, err := parquet.Encode([]parquet.WriteColumn{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "price", Type: parquet.TypeDouble, Nullable: true},
		{Name: "region", Type: parquet.TypeString},
	}, [][]any{
		{int64(7), 2.5, "emea"}, {int64(-4), nil, "amer"}, {int64(19), 8.25, "apac"},
	})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "f.parquet")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	rows, err := db.Query(`SELECT path_in_schema, stats_min_value, stats_max_value, stats_null_count FROM parquet_metadata('` + path + `')`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	got := map[string][3]string{}
	for rows.Next() {
		var name, lo, hi string
		var nulls int64
		if err := rows.Scan(&name, &lo, &hi, &nulls); err != nil {
			t.Fatal(err)
		}
		got[name] = [3]string{lo, hi, map[bool]string{true: "1", false: "0"}[nulls == 1]}
	}
	want := map[string][3]string{"id": {"-4", "19", "0"}, "price": {"2.5", "8.25", "1"}, "region": {"amer", "emea", "0"}}
	for k, w := range want {
		if got[k] != w {
			t.Errorf("%s: duckdb sees %v, writer intended %v", k, got[k], w)
		}
	}
}

// Statistics in files written by DuckDB (dictionary pages, compression, several
// row groups) must parse with our reader and merge correctly.
func TestReaderParsesDuckDBWrittenStatistics(t *testing.T) {
	path := filepath.Join(t.TempDir(), "d.parquet")
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	_, err = db.Exec(`COPY (SELECT i::BIGINT AS id, (i * 0.5)::DOUBLE AS price, 'r' || (i % 7)::VARCHAR AS region,
	                     CASE WHEN i % 10 = 0 THEN NULL ELSE i END::INTEGER AS maybe
	                    FROM range(1, 2501) t(i))
	                   TO '` + path + `' (FORMAT parquet, ROW_GROUP_SIZE 1000, COMPRESSION zstd)`)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	st, err := parquet.ReadStats(context.Background(), fileReader{b}, "k", int64(len(b)))
	if err != nil {
		t.Fatal(err)
	}
	if st.NumRows != 2500 {
		t.Fatalf("rows = %d", st.NumRows)
	}
	id := st.Columns["id"]
	if !id.HasMinMax || id.MinInt != 1 || id.MaxInt != 2500 || id.Type != "int64" {
		t.Fatalf("id: %+v", id)
	}
	if p := st.Columns["price"]; !p.HasMinMax || p.MinFloat != 0.5 || p.MaxFloat != 1250 {
		t.Fatalf("price: %+v", p)
	}
	if r := st.Columns["region"]; !r.HasMinMax || r.MinStr != "r0" || r.MaxStr != "r6" || r.Type != "string" {
		t.Fatalf("region: %+v", r)
	}
	m := st.Columns["maybe"]
	if !m.HasMinMax || m.MinInt != 1 || m.MaxInt != 2499 || !m.HasNullCount || m.NullCount != 250 || m.Type != "int32" {
		t.Fatalf("maybe: %+v", m)
	}
}
