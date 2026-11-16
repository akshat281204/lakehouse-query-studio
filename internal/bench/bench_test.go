package bench

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/akshat281204/lakehouse-query-studio/internal/objectstore"
	"github.com/akshat281204/lakehouse-query-studio/internal/query"
)

func TestPercentileNearestRank(t *testing.T) {
	v := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	for p, want := range map[float64]float64{50: 5, 90: 9, 95: 10, 99: 10, 100: 10, 1: 1} {
		if got := Percentile(v, p); got != want {
			t.Errorf("p%v = %v want %v", p, got, want)
		}
	}
	if got := Percentile([]float64{42}, 99); got != 42 {
		t.Errorf("single sample: %v", got)
	}
}

func TestSummarize(t *testing.T) {
	var ds []time.Duration
	for i := 1; i <= 100; i++ {
		ds = append(ds, time.Duration(i)*time.Millisecond)
	}
	s := Summarize(ds)
	if s.N != 100 || s.P50Ms != 50 || s.P95Ms != 95 || s.P99Ms != 99 || s.MaxMs != 100 || s.MinMs != 1 || s.MeanMs != 50.5 {
		t.Fatalf("%+v", s)
	}
	if (Summarize(nil) != Summary{}) {
		t.Fatal("empty summary")
	}
}

func load(t *testing.T, scale float64) (*Dataset, *objectstore.Memory) {
	t.Helper()
	m := objectstore.NewMemory()
	ds, err := Load(context.Background(), m, scale)
	if err != nil {
		t.Fatal(err)
	}
	return ds, m
}

// The workload must be bit-for-bit reproducible: that is what makes a result
// comparable between runs and machines.
func TestWorkloadIsDeterministic(t *testing.T) {
	a, ma := load(t, 0.002)
	b, mb := load(t, 0.002)
	if !reflect.DeepEqual(a.Tables, b.Tables) || !reflect.DeepEqual(a.Truth, b.Truth) {
		t.Fatalf("tables/truth differ between runs")
	}
	for _, k := range ma.Keys("bench/") {
		x, _ := ma.Get(context.Background(), k, 1<<40)
		y, err := mb.Get(context.Background(), k, 1<<40)
		if err != nil || string(x) != string(y) {
			t.Fatalf("object %s differs between runs", k)
		}
	}
}

func TestBothLayoutsHoldTheSameRows(t *testing.T) {
	ds, m := load(t, 0.002)
	flat, part := ds.table("lineitem"), ds.table("lineitem_part")
	if flat.Rows != part.Rows || flat.Rows != ds.Truth.LineitemRows || flat.Rows == 0 {
		t.Fatalf("rows: flat=%d part=%d truth=%d", flat.Rows, part.Rows, ds.Truth.LineitemRows)
	}
	if !part.Hive || flat.Hive {
		t.Fatal("hive flags wrong")
	}
	years := map[string]bool{}
	for _, k := range m.Keys("bench/lineitem_part/") {
		years[k[len("bench/lineitem_part/"):len("bench/lineitem_part/l_year=1992")]] = true
	}
	if len(years) != LastYear-FirstYear+1 {
		t.Fatalf("partition directories: %v", years)
	}
	sz := SizesFor(0.002)
	if ds.table("customer").Rows != int64(sz.Customers) || ds.table("orders").Rows != int64(sz.Orders) {
		t.Fatalf("sizes %+v vs %+v", sz, ds.Tables)
	}
}

// Every benchmark query must pass the production validator and return the
// answer the generator computed independently of DuckDB.
func TestEveryQueryIsValidAndCorrectOnBothLayouts(t *testing.T) {
	ctx := context.Background()
	lim := query.DefaultLimits()
	lim.Timeout = 60 * time.Second
	h, err := NewHarness(ctx, 0.01, Config{Limits: lim})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	for _, q := range Queries() {
		r, err := h.MeasureQuery(ctx, q, 2)
		if err != nil {
			t.Fatalf("%v", err)
		}
		if !r.Verified || r.RowsReturned == 0 || r.Warm.N != 2 {
			t.Errorf("%s: %+v", q.ID, r)
		}
		if r.BytesStaged == 0 {
			t.Errorf("%s: bytes staged not recorded", q.ID)
		}
	}
}

func TestPartitionedAndFlatGiveIdenticalAnswers(t *testing.T) {
	ctx := context.Background()
	h, err := NewHarness(ctx, 0.01, Config{Limits: query.DefaultLimits()})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	_, a, err := h.Once(ctx, "SELECT l_year, l_returnflag, count(*) AS n, sum(l_extendedprice) AS p FROM lineitem GROUP BY l_year, l_returnflag ORDER BY l_year, l_returnflag")
	if err != nil {
		t.Fatal(err)
	}
	_, b, err := h.Once(ctx, "SELECT l_year, l_returnflag, count(*) AS n, sum(l_extendedprice) AS p FROM lineitem_part GROUP BY l_year, l_returnflag ORDER BY l_year, l_returnflag")
	if err != nil {
		t.Fatal(err)
	}
	if len(a) == 0 || !reflect.DeepEqual(a, b) {
		t.Fatalf("layouts disagree:\nflat=%v\npart=%v", a, b)
	}
}

// A wrong answer must be reported as an error, never as a fast result.
func TestWrongAnswersAreRejected(t *testing.T) {
	ctx := context.Background()
	h, err := NewHarness(ctx, 0.005, Config{Limits: query.DefaultLimits()})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	bad := Queries()[1] // q2
	bad.SQL = "SELECT l_returnflag, count(*) AS n FROM lineitem WHERE l_year = 1996 GROUP BY l_returnflag ORDER BY l_returnflag"
	if _, err := h.MeasureQuery(ctx, bad, 1); err == nil {
		t.Fatal("a query returning the wrong answer was accepted")
	}
}

func TestConcurrencyAndIngestMeasurementsRun(t *testing.T) {
	ctx := context.Background()
	h, err := NewHarness(ctx, 0.005, Config{Limits: query.DefaultLimits()})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	var stmts []string
	for _, q := range Queries()[:4] {
		stmts = append(stmts, q.SQL)
	}
	for _, w := range []int{1, 2} {
		r, err := h.MeasureConcurrency(ctx, stmts, 8, w)
		if err != nil {
			t.Fatal(err)
		}
		if r.Failed != 0 || r.Latency.N != 8 || r.QueriesPerS <= 0 {
			t.Fatalf("workers=%d: %+v", w, r)
		}
	}
	ing, err := MeasureIngest(ctx, 5_000, "l_returnflag")
	if err != nil {
		t.Fatal(err)
	}
	if ing.Rows != 5000 || ing.OutputFiles < 3 || ing.RowsPerSecond <= 0 || ing.OutputBytes == 0 {
		t.Fatalf("%+v", ing)
	}
}

func TestComparisonDoesNotOverclaim(t *testing.T) {
	mk := func(p50 float64, staged int64) QueryResult {
		return QueryResult{Warm: Summary{P50Ms: p50}, WarmExecute: Summary{P50Ms: p50}, BytesStaged: staged}
	}
	// 3 ms on 24 ms is 12% relative but below the absolute floor: no claim.
	if v := compare(mk(24, 100), mk(27, 90), "x").Verdict; !strings.Contains(v, "no measurable") {
		t.Errorf("small absolute gap was reported: %s", v)
	}
	// A large gap in both senses is reported, in the right direction.
	if v := compare(mk(1000, 100), mk(500, 90), "x").Verdict; !strings.Contains(v, "50% faster") {
		t.Errorf("real gap missed: %s", v)
	}
	if v := compare(mk(500, 100), mk(1000, 90), "x").Verdict; !strings.Contains(v, "100% slower") {
		t.Errorf("slowdown missed: %s", v)
	}
	// Identical staged bytes are called out.
	if v := compare(mk(100, 5), mk(100, 5), "x").Verdict; !strings.Contains(v, "bytes staged identical") {
		t.Errorf("identical staging not noted: %s", v)
	}
}

func report(flat, part, cflat, cpart, p95 float64) *Report {
	mk := func(id string, p50, p95 float64) QueryResult {
		return QueryResult{ID: id, Warm: Summary{P50Ms: p50, P95Ms: p95}, WarmExecute: Summary{P50Ms: p50}, BytesStaged: 100}
	}
	r := &Report{Queries: []QueryResult{
		mk("q2-year-flat", flat, p95), mk("q2p-year-partitioned", part, p95),
		mk("q3-full-aggregate", cflat, p95), mk("q3p-full-partitioned", cpart, p95)}}
	r.Derive()
	return r
}

// The control shows the same gap as the filter query: that gap is not pruning.
func TestControlGapPreventsAttributingToPruning(t *testing.T) {
	r := report(1000, 700, 1200, 800, 1100)
	v := r.Partitioning[0].Verdict
	if !strings.Contains(v, "NOT attributable to pruning") {
		t.Fatalf("filter gap matched by the control must not be credited to pruning: %s", v)
	}
	if !strings.Contains(r.Partitioning[1].Verdict, "control differs") {
		t.Fatalf("control gap not flagged: %s", r.Partitioning[1].Verdict)
	}
}

func TestRealPruningGapIsReportedWhenControlIsFlat(t *testing.T) {
	r := report(1000, 400, 1000, 1000, 1100)
	if v := r.Partitioning[0].Verdict; !strings.Contains(v, "60% faster") || strings.Contains(v, "NOT attributable") {
		t.Fatalf("a gap the control does not share should be reported: %s", v)
	}
}

func TestHighVarianceIsFlaggedInconclusive(t *testing.T) {
	r := report(1000, 700, 1000, 1000, 5000)
	for _, p := range r.Partitioning {
		if !strings.Contains(p.Verdict, "INCONCLUSIVE") {
			t.Fatalf("%s", p.Verdict)
		}
	}
}

func TestInterleavedPairMeasuresBothAndVerifies(t *testing.T) {
	ctx := context.Background()
	h, err := NewHarness(ctx, 0.005, Config{Limits: query.DefaultLimits()})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	qs := Queries()
	a, b, err := h.MeasurePair(ctx, qs[1], qs[2], 3)
	if err != nil {
		t.Fatal(err)
	}
	if a.Warm.N != 3 || b.Warm.N != 3 || !a.Verified || !b.Verified || a.ID == b.ID {
		t.Fatalf("%+v %+v", a, b)
	}
}

func TestExecuteOnlyVerdict(t *testing.T) {
	mk := func(p50, p95 float64) QueryResult { return QueryResult{WarmExecute: Summary{P50Ms: p50, P95Ms: p95}} }
	if v := execVerdict(mk(100, 120), mk(103, 110)); !strings.Contains(v, "no measurable") {
		t.Error(v)
	}
	if v := execVerdict(mk(100, 120), mk(200, 230)); !strings.Contains(v, "100% slower") || strings.Contains(v, "INCONCLUSIVE") {
		t.Error(v)
	}
	if v := execVerdict(mk(100, 500), mk(50, 60)); !strings.Contains(v, "50% faster") || !strings.Contains(v, "INCONCLUSIVE") {
		t.Error(v)
	}
}
