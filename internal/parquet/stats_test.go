package parquet

import (
	"bytes"
	"context"
	"math"
	"testing"
)

func statsOf(t *testing.T, cols []WriteColumn, rows [][]any) *FileStats {
	t.Helper()
	data, err := Encode(cols, rows)
	if err != nil {
		t.Fatal(err)
	}
	st, err := ReadStats(context.Background(), bytesReader(data), "k", int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func TestWriterEmitsReadableStatistics(t *testing.T) {
	st := statsOf(t,
		[]WriteColumn{
			{Name: "id", Type: TypeInt64},
			{Name: "price", Type: TypeDouble, Nullable: true},
			{Name: "region", Type: TypeString},
			{Name: "ok", Type: TypeBoolean},
		},
		[][]any{
			{int64(5), 2.5, "emea", true},
			{int64(-3), nil, "amer", true},
			{int64(9), 10.25, "apac", true},
		})
	if st.NumRows != 3 {
		t.Fatalf("rows = %d", st.NumRows)
	}
	id := st.Columns["id"]
	if !id.HasMinMax || id.MinInt != -3 || id.MaxInt != 9 || id.Type != "int64" {
		t.Fatalf("id stats: %+v", id)
	}
	p := st.Columns["price"]
	if !p.HasMinMax || p.MinFloat != 2.5 || p.MaxFloat != 10.25 || !p.HasNullCount || p.NullCount != 1 {
		t.Fatalf("price stats: %+v", p)
	}
	r := st.Columns["region"]
	if !r.HasMinMax || r.MinStr != "amer" || r.MaxStr != "emea" {
		t.Fatalf("region stats: %+v", r)
	}
	b := st.Columns["ok"]
	if !b.HasMinMax || !b.MinBool || !b.MaxBool {
		t.Fatalf("bool stats: %+v", b)
	}
}

func TestStatsOmittedWhenUnsafeOrEmpty(t *testing.T) {
	long := string(bytes.Repeat([]byte("x"), maxStatString+1))
	st := statsOf(t,
		[]WriteColumn{
			{Name: "nan", Type: TypeDouble},
			{Name: "allnull", Type: TypeInt64, Nullable: true},
			{Name: "long", Type: TypeString},
		},
		[][]any{{math.NaN(), nil, long}, {1.0, nil, "a"}})
	if st.Columns["nan"].HasMinMax {
		t.Fatal("NaN column must not publish min/max")
	}
	if c := st.Columns["allnull"]; c.HasMinMax || !c.HasNullCount || c.NullCount != 2 {
		t.Fatalf("all-null column: %+v", c)
	}
	if st.Columns["long"].HasMinMax {
		t.Fatal("over-long string must not publish min/max")
	}
}

// The existing reader and DuckDB must still accept files that now carry
// statistics: the schema read is unaffected.
func TestInspectStillWorksWithStatistics(t *testing.T) {
	data, err := Encode([]WriteColumn{{Name: "a", Type: TypeInt64}}, [][]any{{int64(1)}, {int64(2)}})
	if err != nil {
		t.Fatal(err)
	}
	s, err := Inspect(context.Background(), bytesReader(data), "k", int64(len(data)))
	if err != nil || len(s.Columns) != 1 || s.NumRows != 2 {
		t.Fatalf("inspect: %v %+v", err, s)
	}
}

func TestMergeStatsCombinesRangesAndRefusesPartialKnowledge(t *testing.T) {
	a := &FileStats{NumRows: 3, Columns: map[string]ColumnStats{
		"id":  {Name: "id", Type: "int64", HasMinMax: true, MinInt: 5, MaxInt: 9, HasNullCount: true, NullCount: 1},
		"s":   {Name: "s", Type: "string", HasMinMax: true, MinStr: "b", MaxStr: "d", HasNullCount: true},
		"mix": {Name: "mix", Type: "int64", HasMinMax: true, MinInt: 1, MaxInt: 1},
	}}
	b := &FileStats{NumRows: 2, Columns: map[string]ColumnStats{
		"id":  {Name: "id", Type: "int64", HasMinMax: true, MinInt: -2, MaxInt: 7, HasNullCount: true, NullCount: 2},
		"s":   {Name: "s", Type: "string", HasMinMax: false}, // this file has no statistics for s
		"mix": {Name: "mix", Type: "double", HasMinMax: true, MinFloat: 0, MaxFloat: 1},
		"new": {Name: "new", Type: "int64", HasMinMax: true, MinInt: 1, MaxInt: 2},
	}}
	m := MergeStats([]*FileStats{a, b, nil})
	if m.NumRows != 5 {
		t.Fatalf("rows = %d", m.NumRows)
	}
	if id := m.Columns["id"]; !id.HasMinMax || id.MinInt != -2 || id.MaxInt != 9 || !id.HasNullCount || id.NullCount != 3 {
		t.Fatalf("id: %+v", id)
	}
	if m.Columns["s"].HasMinMax {
		t.Fatal("a column one file has no statistics for must not report a range")
	}
	if m.Columns["mix"].HasMinMax {
		t.Fatal("a column whose type differs between files must not report a range")
	}
	if m.Columns["new"].HasMinMax {
		t.Fatal("a column absent from one file must not report a range")
	}
	if e := MergeStats(nil); e.NumRows != 0 || len(e.Columns) != 0 {
		t.Fatalf("empty merge: %+v", e)
	}
}
