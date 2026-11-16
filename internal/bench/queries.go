package bench

import (
	"fmt"
	"math"
)

// Query is one benchmark statement. Check validates the returned rows against
// the generator's ground truth, so a fast wrong answer cannot be reported.
type Query struct {
	ID    string                                                  `json:"id"`
	Desc  string                                                  `json:"description"`
	SQL   string                                                  `json:"sql"`
	Check func(rows []map[string]any, total int64, t Truth) error `json:"-"`
}

func num(v any) float64 {
	switch x := v.(type) {
	case int64:
		return float64(x)
	case float64:
		return x
	}
	return math.NaN()
}

// Queries returns the query set. Each is valid under the production SQL
// validator (no features the safety boundary denies).
func Queries() []Query {
	return []Query{
		{
			ID: "q1-selective-filter", Desc: "selective filter + aggregate on lineitem (flat)",
			SQL: "SELECT count(*) AS n, sum(l_extendedprice) AS rev FROM lineitem WHERE l_year = 1994 AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24",
			Check: func(r []map[string]any, _ int64, t Truth) error {
				if len(r) != 1 || num(r[0]["n"]) != float64(t.Year1994Filter) {
					return fmt.Errorf("n = %v, want %d", r, t.Year1994Filter)
				}
				if math.Abs(num(r[0]["rev"])-t.Year1994Rev) > 1e-6*math.Max(1, t.Year1994Rev) {
					return fmt.Errorf("rev = %v, want %v", r[0]["rev"], t.Year1994Rev)
				}
				return nil
			},
		},
		{
			ID: "q2-year-flat", Desc: "group by on one year of lineitem (flat layout)",
			SQL:   "SELECT l_returnflag, count(*) AS n FROM lineitem WHERE l_year = 1995 GROUP BY l_returnflag ORDER BY l_returnflag",
			Check: sumN("n", func(t Truth) int64 { return t.Year1995Rows }),
		},
		{
			ID: "q2p-year-partitioned", Desc: "same as q2 on the hive-partitioned layout (partition pruning)",
			SQL:   "SELECT l_returnflag, count(*) AS n FROM lineitem_part WHERE l_year = 1995 GROUP BY l_returnflag ORDER BY l_returnflag",
			Check: sumN("n", func(t Truth) int64 { return t.Year1995Rows }),
		},
		{
			ID: "q3-full-aggregate", Desc: "full-scan pricing summary grouped by flag and status",
			SQL:   "SELECT l_returnflag, l_linestatus, sum(l_quantity) AS qty, sum(l_extendedprice) AS price, avg(l_discount) AS disc, count(*) AS n FROM lineitem GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus",
			Check: sumN("n", func(t Truth) int64 { return t.LineitemRows }),
		},
		{
			ID: "q3p-full-partitioned", Desc: "same as q3 on the partitioned layout (no pruning possible)",
			SQL:   "SELECT l_returnflag, l_linestatus, sum(l_quantity) AS qty, sum(l_extendedprice) AS price, avg(l_discount) AS disc, count(*) AS n FROM lineitem_part GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus",
			Check: sumN("n", func(t Truth) int64 { return t.LineitemRows }),
		},
		{
			ID: "q4-join", Desc: "orders join customer, aggregate by segment",
			SQL: "SELECT c.c_mktsegment AS seg, count(*) AS n, sum(o.o_totalprice) AS total FROM orders o JOIN customer c ON o.o_custkey = c.c_custkey GROUP BY c.c_mktsegment ORDER BY c.c_mktsegment",
			Check: func(r []map[string]any, _ int64, t Truth) error {
				if len(r) != len(t.SegmentOrders) {
					return fmt.Errorf("%d segments, want %d", len(r), len(t.SegmentOrders))
				}
				for _, row := range r {
					seg, _ := row["seg"].(string)
					if num(row["n"]) != float64(t.SegmentOrders[seg]) {
						return fmt.Errorf("segment %s: n=%v want %d", seg, row["n"], t.SegmentOrders[seg])
					}
				}
				return nil
			},
		},
		{
			ID: "q5-three-way-join", Desc: "lineitem join orders join customer, revenue by segment for one year",
			SQL: "SELECT c.c_mktsegment AS seg, sum(l.l_extendedprice * (1 - l.l_discount)) AS revenue, count(*) AS n FROM lineitem l JOIN orders o ON l.l_orderkey = o.o_orderkey JOIN customer c ON o.o_custkey = c.c_custkey WHERE l.l_year = 1996 GROUP BY c.c_mktsegment ORDER BY c.c_mktsegment",
			Check: func(r []map[string]any, _ int64, _ Truth) error {
				if len(r) == 0 {
					return fmt.Errorf("no rows")
				}
				return nil
			},
		},
		{
			ID: "q6-point-lookup", Desc: "single-order lookup (no index: a scan)",
			SQL: "SELECT o_orderkey, o_custkey, o_totalprice FROM orders WHERE o_orderkey = 17",
			Check: func(r []map[string]any, _ int64, _ Truth) error {
				if len(r) != 1 || num(r[0]["o_orderkey"]) != 17 {
					return fmt.Errorf("rows = %v", r)
				}
				return nil
			},
		},
		{
			ID: "q7-large-result", Desc: "unfiltered projection, result truncated at the row limit",
			SQL: "SELECT l_orderkey, l_extendedprice, l_shipdate FROM lineitem",
			Check: func(_ []map[string]any, total int64, t Truth) error {
				want := int64(10_000)
				if t.LineitemRows < want {
					want = t.LineitemRows
				}
				if total != want {
					return fmt.Errorf("returned %d rows, want %d", total, want)
				}
				return nil
			},
		},
	}
}

func sumN(col string, want func(Truth) int64) func([]map[string]any, int64, Truth) error {
	return func(r []map[string]any, _ int64, t Truth) error {
		var total float64
		for _, row := range r {
			total += num(row[col])
		}
		if total != float64(want(t)) {
			return fmt.Errorf("sum(%s) = %v, want %d", col, total, want(t))
		}
		return nil
	}
}
