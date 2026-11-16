package query

import (
	"context"
	"math/rand"
	"testing"

	"github.com/akshat281204/lakehouse-query-studio/internal/parquet"
)

func pruningOf(t *testing.T, sql string) *Pruning {
	t.Helper()
	v, err := NewValidator()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = v.Close() }()
	got, err := v.Validate(context.Background(), sql)
	if err != nil {
		t.Fatalf("validate %q: %v", sql, err)
	}
	return got.Pruning
}

func TestExtractPruningShapes(t *testing.T) {
	p := pruningOf(t, `SELECT * FROM sales WHERE region = 'emea' AND year >= 2020 AND price < 1.5 AND id IN (1, 2, 3) AND d BETWEEN 3 AND 9`)
	if p == nil || p.Table != "sales" {
		t.Fatalf("pruning: %+v", p)
	}
	got := map[string]int{}
	for _, c := range p.Constraints {
		got[c.Column+" "+c.Op]++
	}
	for _, want := range []string{"region =", "year >=", "price <", "id in", "d >=", "d <="} {
		if got[want] != 1 {
			t.Errorf("missing constraint %q in %v", want, got)
		}
	}
}

func TestExtractPruningReversedAliasedAndQuoted(t *testing.T) {
	p := pruningOf(t, `SELECT * FROM "demo-sales" s WHERE 5 < s.year AND s.region <> 'x'`)
	if p == nil || p.Table != "demo-sales" || len(p.Constraints) != 2 {
		t.Fatalf("pruning: %+v", p)
	}
	if c := p.Constraints[0]; c.Column != "year" || c.Op != ">" || c.Values[0].i != 5 {
		t.Errorf("reversed comparison: %+v", c)
	}
}

func TestPruningIsOffForShapesItCannotReasonAbout(t *testing.T) {
	for _, sql := range []string{
		`SELECT * FROM sales`, // no filter
		`SELECT * FROM sales WHERE region = 'a' OR region = 'b'`,
		`SELECT * FROM sales WHERE upper(region) = 'A'`,
		`SELECT * FROM sales WHERE region = other`,
		`SELECT * FROM sales a JOIN sales b ON a.id = b.id WHERE a.year = 2020`,
		`WITH x AS (SELECT * FROM sales) SELECT * FROM x WHERE year = 1`,
		`SELECT * FROM sales WHERE year = 1 UNION ALL SELECT * FROM sales WHERE year = 2`,
		`SELECT * FROM (SELECT * FROM sales) WHERE year = 1`,
		`SELECT * FROM sales USING SAMPLE 10 ROWS`,
		`SELECT * FROM sales WHERE year = NULL`,
		`SELECT * FROM sales t WHERE other.year = 1`, // foreign qualifier
	} {
		if p := pruningOf(t, sql); p != nil {
			t.Errorf("expected no pruning for %q, got %+v", sql, p)
		}
	}
}

// DuckDB's parser itself rewrites NOT (a = b) into a <> b, which is equivalent
// (NULL included), so pruning on it is sound.
func TestNotEqualRewriteByTheParserIsUsed(t *testing.T) {
	p := pruningOf(t, `SELECT * FROM sales WHERE NOT (region = 'a')`)
	if p == nil || p.Constraints[0].Op != "<>" {
		t.Fatalf("pruning: %+v", p)
	}
}

func TestOnlyUnderstoodConjunctsAreKept(t *testing.T) {
	p := pruningOf(t, `SELECT * FROM sales WHERE year = 2020 AND (a = 1 OR b = 2) AND lower(c) = 'x'`)
	if p == nil || len(p.Constraints) != 1 || p.Constraints[0].Column != "year" {
		t.Fatalf("pruning: %+v", p)
	}
}

func TestLargeIntegerLiteralsStayExact(t *testing.T) {
	p := pruningOf(t, `SELECT * FROM sales WHERE id = 9007199254740993`) // 2^53 + 1
	if p == nil || p.Constraints[0].Values[0].i != 9007199254740993 {
		t.Fatalf("literal lost precision: %+v", p)
	}
}

// ---- soundness ------------------------------------------------------------

func statsFor(rows []int64, nulls int) *parquet.FileStats {
	cs := parquet.ColumnStats{Name: "x", Type: "int64", HasNullCount: true, NullCount: int64(nulls)}
	if len(rows) > 0 {
		cs.HasMinMax = true
		cs.MinInt, cs.MaxInt = rows[0], rows[0]
		for _, v := range rows {
			cs.MinInt, cs.MaxInt = min(cs.MinInt, v), max(cs.MaxInt, v)
		}
	}
	return &parquet.FileStats{NumRows: int64(len(rows) + nulls), Columns: map[string]parquet.ColumnStats{"x": cs}}
}

func holds(op string, v, lit int64) bool {
	switch op {
	case "=":
		return v == lit
	case "<>":
		return v != lit
	case "<":
		return v < lit
	case "<=":
		return v <= lit
	case ">":
		return v > lit
	case ">=":
		return v >= lit
	}
	return false
}

// If MayMatchFile says "no", no row of the file may satisfy the constraint:
// skipping must never lose a result. Checked against a brute-force oracle.
func TestPruningNeverSkipsAFileThatHasAMatchingInt(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	ops := []string{"=", "<>", "<", "<=", ">", ">="}
	skipped := 0
	for trial := 0; trial < 20000; trial++ {
		n := rng.Intn(6)
		rows := make([]int64, n)
		for i := range rows {
			rows[i] = int64(rng.Intn(21) - 10)
		}
		nulls := rng.Intn(3)
		lit := int64(rng.Intn(25) - 12)
		op := ops[rng.Intn(len(ops))]
		c := Constraint{Column: "x", Op: op, Values: []literal{{kind: litInt, i: lit}}}
		may := MayMatchFile([]Constraint{c}, statsFor(rows, nulls))
		matches := false
		for _, v := range rows {
			if holds(op, v, lit) {
				matches = true
			}
		}
		if !may {
			skipped++
			if matches {
				t.Fatalf("unsound skip: rows=%v nulls=%d x %s %d", rows, nulls, op, lit)
			}
		}
	}
	if skipped == 0 {
		t.Fatal("property test never exercised a skip")
	}
}

func TestPruningInListAndStrings(t *testing.T) {
	st := &parquet.FileStats{NumRows: 4, Columns: map[string]parquet.ColumnStats{
		"region": {Name: "region", Type: "string", HasMinMax: true, MinStr: "emea", MaxStr: "emea", HasNullCount: true},
	}}
	eq := func(s string) []Constraint {
		return []Constraint{{Column: "region", Op: "=", Values: []literal{{kind: litString, s: s}}}}
	}
	if MayMatchFile(eq("amer"), st) || !MayMatchFile(eq("emea"), st) {
		t.Error("string equality")
	}
	in := []Constraint{{Column: "region", Op: "in", Values: []literal{{kind: litString, s: "amer"}, {kind: litString, s: "emea"}}}}
	if !MayMatchFile(in, st) {
		t.Error("IN with a matching member must keep the file")
	}
	in[0].Values = in[0].Values[:1]
	if MayMatchFile(in, st) {
		t.Error("IN with no matching member must skip the file")
	}
	ne := []Constraint{{Column: "region", Op: "<>", Values: []literal{{kind: litString, s: "emea"}}}}
	if MayMatchFile(ne, st) {
		t.Error("<> over a single-valued file equal to the literal must skip")
	}
}

func TestDoublePruningIsWidenedAgainstRounding(t *testing.T) {
	// 0.1+0.2 style rounding: a file whose max is the double nearest 0.3 must
	// still be kept for `x = 0.3`.
	st := &parquet.FileStats{NumRows: 2, Columns: map[string]parquet.ColumnStats{
		"x": {Name: "x", Type: "double", HasMinMax: true, MinFloat: 0.1, MaxFloat: 0.1 + 0.2},
	}}
	c := []Constraint{{Column: "x", Op: "=", Values: []literal{{kind: litFloat, f: 0.3}}}}
	if !MayMatchFile(c, st) {
		t.Fatal("file wrongly excluded by float rounding")
	}
	far := []Constraint{{Column: "x", Op: "=", Values: []literal{{kind: litFloat, f: 5}}}}
	if MayMatchFile(far, st) {
		t.Fatal("a clearly out-of-range double equality must skip")
	}
	if !MayMatchFile([]Constraint{{Column: "x", Op: "<>", Values: []literal{{kind: litFloat, f: 0.1}}}}, st) {
		t.Fatal("<> on doubles must never prune")
	}
}

func TestMixedKindsAndMissingStatsKeepTheFile(t *testing.T) {
	st := &parquet.FileStats{NumRows: 3, Columns: map[string]parquet.ColumnStats{
		"id": {Name: "id", Type: "int64", HasMinMax: true, MinInt: 1, MaxInt: 3},
		"s":  {Name: "s", Type: "string"}, // no min/max
	}}
	for _, c := range []Constraint{
		{Column: "id", Op: "=", Values: []literal{{kind: litFloat, f: 99}}},   // decimal vs int: leave it
		{Column: "id", Op: "=", Values: []literal{{kind: litString, s: "x"}}}, // kind mismatch
		{Column: "s", Op: "=", Values: []literal{{kind: litString, s: "x"}}},  // no stats
		{Column: "absent", Op: "=", Values: []literal{{kind: litInt, i: 1}}},  // unknown column
	} {
		if !MayMatchFile([]Constraint{c}, st) {
			t.Errorf("must keep file for %+v", c)
		}
	}
	if !MayMatchFile([]Constraint{{Column: "id", Op: "=", Values: []literal{{kind: litInt, i: 1}}}}, nil) {
		t.Error("nil stats must keep the file")
	}
}

func TestAllNullColumnNeverMatchesAComparison(t *testing.T) {
	st := &parquet.FileStats{NumRows: 3, Columns: map[string]parquet.ColumnStats{
		"x": {Name: "x", Type: "int64", HasNullCount: true, NullCount: 3},
	}}
	if MayMatchFile([]Constraint{{Column: "x", Op: ">", Values: []literal{{kind: litInt, i: 0}}}}, st) {
		t.Fatal("an all-NULL column cannot satisfy a comparison")
	}
}

func TestHivePartitionPruning(t *testing.T) {
	year := func(op string, n int64) []Constraint {
		return []Constraint{{Column: "year", Op: op, Values: []literal{{kind: litInt, i: n}}}}
	}
	region := func(s string) []Constraint {
		return []Constraint{{Column: "region", Op: "=", Values: []literal{{kind: litString, s: s}}}}
	}
	if MayMatchPartition(year("=", 2021), "year=2020/region=emea/part-0.parquet") {
		t.Error("year=2020 file must be pruned for year = 2021")
	}
	if !MayMatchPartition(year(">=", 2020), "year=2020/part-0.parquet") {
		t.Error("year=2020 file matches year >= 2020")
	}
	if MayMatchPartition(region("amer"), "year=2020/region=emea/part-0.parquet") {
		t.Error("region=emea must be pruned for region = 'amer'")
	}
	// values whose type DuckDB might auto-detect differently are never pruned
	for _, rel := range []string{"year=0042/p.parquet", "year=2020-01-01/p.parquet", "year=__HIVE_DEFAULT_PARTITION__/p.parquet"} {
		if !MayMatchPartition(year("=", 7), rel) {
			t.Errorf("ambiguous partition value %q must keep the file", rel)
		}
	}
	if !MayMatchPartition(year("=", 1), "part-0.parquet") {
		t.Error("an unpartitioned path must keep the file")
	}
}
