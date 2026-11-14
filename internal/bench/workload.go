// Package bench generates a deterministic analytical workload and measures the
// query path against it.
//
// The workload is TPC-H-LIKE, not TPC-H: three tables with TPC-H's names,
// cardinality ratios and column shapes, filled from a seeded generator, so a
// run is reproducible bit-for-bit. It is NOT produced by dbgen and its results
// must never be reported as TPC-H results. Dates are ISO strings and money is
// double because the in-repo Parquet writer supports only boolean, int64,
// double and string.
package bench

import (
	"fmt"
	"math/rand/v2"

	"github.com/akshat281204/lakehouse-query-studio/internal/parquet"
)

// Seed is fixed so every run generates identical data.
const Seed = 20240601

// Rows per scale factor, following TPC-H cardinalities.
const (
	customersPerSF = 150_000
	ordersPerSF    = 1_500_000
)

// Years covered by order and ship dates.
const (
	FirstYear = 1992
	LastYear  = 1998
)

var (
	segments   = []string{"AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"}
	priorities = []string{"1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"}
	flags      = []string{"A", "N", "R"}
	statuses   = []string{"O", "F"}
)

// Sizes are the row counts for a scale factor (SF 1 = 150k/1.5M/~6M).
type Sizes struct {
	Customers, Orders int
}

// SizesFor returns table sizes for scale (fractions allowed).
func SizesFor(scale float64) Sizes {
	c := int(float64(customersPerSF) * scale)
	o := int(float64(ordersPerSF) * scale)
	if c < 10 {
		c = 10
	}
	if o < 40 {
		o = 40
	}
	return Sizes{Customers: c, Orders: o}
}

// Column sets. Lineitem carries l_year so the same queries run unchanged on the
// unpartitioned layout; the partitioned layout stores it in the directory name
// instead (hive), so the data is identical, only the layout differs.
var (
	CustomerCols = []parquet.WriteColumn{
		{Name: "c_custkey", Type: parquet.TypeInt64}, {Name: "c_nationkey", Type: parquet.TypeInt64},
		{Name: "c_mktsegment", Type: parquet.TypeString}, {Name: "c_acctbal", Type: parquet.TypeDouble},
	}
	OrdersCols = []parquet.WriteColumn{
		{Name: "o_orderkey", Type: parquet.TypeInt64}, {Name: "o_custkey", Type: parquet.TypeInt64},
		{Name: "o_totalprice", Type: parquet.TypeDouble}, {Name: "o_orderdate", Type: parquet.TypeString},
		{Name: "o_orderpriority", Type: parquet.TypeString},
	}
	LineitemCols = []parquet.WriteColumn{
		{Name: "l_orderkey", Type: parquet.TypeInt64}, {Name: "l_partkey", Type: parquet.TypeInt64},
		{Name: "l_suppkey", Type: parquet.TypeInt64}, {Name: "l_quantity", Type: parquet.TypeDouble},
		{Name: "l_extendedprice", Type: parquet.TypeDouble}, {Name: "l_discount", Type: parquet.TypeDouble},
		{Name: "l_tax", Type: parquet.TypeDouble}, {Name: "l_returnflag", Type: parquet.TypeString},
		{Name: "l_linestatus", Type: parquet.TypeString}, {Name: "l_shipdate", Type: parquet.TypeString},
		{Name: "l_year", Type: parquet.TypeInt64},
	}
)

// LineitemColsNoYear is the partitioned layout's file schema (l_year is in the path).
func LineitemColsNoYear() []parquet.WriteColumn { return LineitemCols[:len(LineitemCols)-1] }

// Truth holds values computed during generation, used to check that the
// benchmark queries return correct answers (a fast wrong answer is not a result).
type Truth struct {
	LineitemRows   int64
	Year1995Rows   int64
	Year1994Filter int64   // Q1 predicate count
	Year1994Rev    float64 // Q1 predicate sum(l_extendedprice)
	CustomerRows   int64
	OrderRows      int64
	SegmentOrders  map[string]int64
}

// Generator produces rows deterministically in chunks.
type Generator struct {
	sizes        Sizes
	rng          *rand.Rand
	Truth        Truth
	custSegments []string
}

// NewGenerator returns a generator for the given sizes.
func NewGenerator(s Sizes) *Generator {
	return &Generator{sizes: s, rng: rand.New(rand.NewPCG(Seed, Seed^0x9e3779b97f4a7c15)),
		Truth: Truth{SegmentOrders: map[string]int64{}}}
}

func (g *Generator) pick(v []string) string { return v[g.rng.IntN(len(v))] }

func date(year, dayOfYear int) string {
	// 28-day months keep dates valid without a calendar; this is a
	// distribution, not a calendar.
	m := (dayOfYear/28)%12 + 1
	d := dayOfYear%28 + 1
	return fmt.Sprintf("%04d-%02d-%02d", year, m, d)
}

// Customers returns all customer rows (small relative to the others).
func (g *Generator) Customers() [][]any {
	seg := make([]string, g.sizes.Customers)
	rows := make([][]any, g.sizes.Customers)
	for i := range rows {
		seg[i] = g.pick(segments)
		rows[i] = []any{int64(i + 1), int64(g.rng.IntN(25)), seg[i], float64(g.rng.IntN(1_000_000))/100 - 999}
	}
	g.Truth.CustomerRows = int64(len(rows))
	g.custSegments = seg
	return rows
}

// Orders returns orders [from, to) (0-based indexes).
func (g *Generator) Orders(from, to int) [][]any {
	rows := make([][]any, 0, to-from)
	for i := from; i < to; i++ {
		cust := g.rng.IntN(g.sizes.Customers)
		year := FirstYear + g.rng.IntN(LastYear-FirstYear+1)
		rows = append(rows, []any{int64(i + 1), int64(cust + 1), float64(g.rng.IntN(50_000_000)) / 100,
			date(year, g.rng.IntN(336)), g.pick(priorities)})
		g.Truth.OrderRows++
		g.Truth.SegmentOrders[g.custSegments[cust]]++
	}
	return rows
}

// Lineitems returns the line items of orders [from, to), 1–7 per order, and
// records the answers the benchmark queries must reproduce. Rows always carry
// l_year as their last column; the partitioned layout drops it and routes by the
// returned years, so both layouts hold identical data.
func (g *Generator) Lineitems(from, to int) (rows [][]any, years []int) {
	for o := from; o < to; o++ {
		n := 1 + g.rng.IntN(7)
		for l := 0; l < n; l++ {
			year := FirstYear + g.rng.IntN(LastYear-FirstYear+1)
			qty := float64(1 + g.rng.IntN(50))
			price := float64(g.rng.IntN(10_000_000)) / 100
			disc := float64(g.rng.IntN(11)) / 100
			row := []any{int64(o + 1), int64(1 + g.rng.IntN(200_000)), int64(1 + g.rng.IntN(10_000)), qty, price, disc,
				float64(g.rng.IntN(9)) / 100, g.pick(flags), g.pick(statuses), date(year, g.rng.IntN(336))}
			row = append(row, int64(year))
			rows = append(rows, row)
			years = append(years, year)

			g.Truth.LineitemRows++
			if year == 1995 {
				g.Truth.Year1995Rows++
			}
			if year == 1994 && disc >= 0.05 && disc <= 0.07 && qty < 24 {
				g.Truth.Year1994Filter++
				g.Truth.Year1994Rev += price
			}
		}
	}
	return rows, years
}
