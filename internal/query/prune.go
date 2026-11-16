package query

import (
	"encoding/json"
	"math"
	"regexp"
	"strconv"
	"strings"

	"github.com/akshat281204/lakehouse-query-studio/internal/parquet"
)

// File pruning. A statement of the simple shape
//
//	SELECT ... FROM <one dataset> WHERE <c1> AND <c2> AND ...
//
// can skip any file whose footer statistics prove that no row satisfies one of
// the top-level AND conjuncts. Pruning is conservative by construction: it only
// ever skips a file when it is certain no row matches, and any shape, type or
// value it does not fully understand leaves the file in. It never changes a
// result; it only avoids staging bytes that could not contribute.

type litKind int

const (
	litInt litKind = iota + 1
	litFloat
	litString
)

type literal struct {
	kind litKind
	i    int64
	f    float64
	s    string
}

// Constraint is one `column <op> literal(s)` conjunct of a WHERE clause.
// Op is one of = <> < <= > >= or "in" (Values holds the list).
type Constraint struct {
	Column string // lower-cased
	Op     string
	Values []literal
}

// Pruning describes what the planner may prune for one statement.
type Pruning struct {
	// Table is the (lower-cased) relation name the constraints apply to.
	Table       string
	Constraints []Constraint
}

// extractPruning inspects the parsed statement. It returns nil unless the
// statement is a single SELECT over one base table with no WITH clause, no
// sampling and no QUALIFY, so a skipped file can never change a result.
func extractPruning(raw json.RawMessage) *Pruning {
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber() // keep 64-bit integer literals exact
	var node map[string]any
	if err := dec.Decode(&node); err != nil {
		return nil
	}
	if str(node, "type") != "SELECT_NODE" {
		return nil
	}
	if node["sample"] != nil || node["qualify"] != nil {
		return nil
	}
	if cm, ok := node["cte_map"].(map[string]any); ok {
		if entries, _ := cm["map"].([]any); len(entries) > 0 {
			return nil
		}
	}
	from, _ := node["from_table"].(map[string]any)
	if str(from, "type") != "BASE_TABLE" || from["sample"] != nil || from["at_clause"] != nil {
		return nil
	}
	table := strings.ToLower(str(from, "table_name"))
	if table == "" {
		return nil
	}
	alias := strings.ToLower(str(from, "alias"))
	where, _ := node["where_clause"].(map[string]any)
	if where == nil {
		return nil
	}
	var cons []Constraint
	for _, c := range conjuncts(where) {
		if con, ok := constraintOf(c, table, alias); ok {
			cons = append(cons, con)
		}
		cons = append(cons, betweenOf(c, table, alias)...)
	}
	if len(cons) == 0 {
		return nil
	}
	return &Pruning{Table: table, Constraints: cons}
}

// conjuncts flattens nested top-level ANDs. OR, NOT and everything else stay
// opaque single conjuncts (and are then ignored, which is always safe).
func conjuncts(n map[string]any) []map[string]any {
	if str(n, "class") == "CONJUNCTION" && str(n, "type") == "CONJUNCTION_AND" {
		var out []map[string]any
		kids, _ := n["children"].([]any)
		for _, k := range kids {
			if m, ok := k.(map[string]any); ok {
				out = append(out, conjuncts(m)...)
			}
		}
		return out
	}
	return []map[string]any{n}
}

var compareOps = map[string]string{
	"COMPARE_EQUAL": "=", "COMPARE_NOTEQUAL": "<>", "COMPARE_LESSTHAN": "<",
	"COMPARE_LESSTHANOREQUALTO": "<=", "COMPARE_GREATERTHAN": ">",
	"COMPARE_GREATERTHANOREQUALTO": ">=",
}

var flipOp = map[string]string{"=": "=", "<>": "<>", "<": ">", "<=": ">=", ">": "<", ">=": "<="}

func constraintOf(n map[string]any, table, alias string) (Constraint, bool) {
	switch str(n, "class") {
	case "COMPARISON":
		op, ok := compareOps[str(n, "type")]
		if !ok {
			return Constraint{}, false
		}
		l, _ := n["left"].(map[string]any)
		r, _ := n["right"].(map[string]any)
		if col, ok := columnOf(l, table, alias); ok {
			if lit, ok := literalOf(r); ok {
				return Constraint{Column: col, Op: op, Values: []literal{lit}}, true
			}
		}
		if col, ok := columnOf(r, table, alias); ok {
			if lit, ok := literalOf(l); ok {
				return Constraint{Column: col, Op: flipOp[op], Values: []literal{lit}}, true
			}
		}
	case "OPERATOR":
		if str(n, "type") != "COMPARE_IN" {
			return Constraint{}, false
		}
		kids, _ := n["children"].([]any)
		if len(kids) < 2 {
			return Constraint{}, false
		}
		first, _ := kids[0].(map[string]any)
		col, ok := columnOf(first, table, alias)
		if !ok {
			return Constraint{}, false
		}
		vals := make([]literal, 0, len(kids)-1)
		for _, k := range kids[1:] {
			m, _ := k.(map[string]any)
			lit, ok := literalOf(m)
			if !ok {
				return Constraint{}, false // one opaque member: no pruning from this conjunct
			}
			vals = append(vals, lit)
		}
		return Constraint{Column: col, Op: "in", Values: vals}, true
	}
	return Constraint{}, false
}

// BETWEEN is handled separately because it yields two constraints.
func betweenOf(n map[string]any, table, alias string) []Constraint {
	if str(n, "class") != "BETWEEN" {
		return nil
	}
	in, _ := n["input"].(map[string]any)
	lo, _ := n["lower"].(map[string]any)
	hi, _ := n["upper"].(map[string]any)
	col, ok := columnOf(in, table, alias)
	if !ok {
		return nil
	}
	a, ok1 := literalOf(lo)
	b, ok2 := literalOf(hi)
	if !ok1 || !ok2 {
		return nil
	}
	return []Constraint{
		{Column: col, Op: ">=", Values: []literal{a}},
		{Column: col, Op: "<=", Values: []literal{b}},
	}
}

func columnOf(n map[string]any, table, alias string) (string, bool) {
	if str(n, "class") != "COLUMN_REF" {
		return "", false
	}
	names, _ := n["column_names"].([]any)
	switch len(names) {
	case 1:
		s, _ := names[0].(string)
		return strings.ToLower(s), s != ""
	case 2:
		q, _ := names[0].(string)
		c, _ := names[1].(string)
		q = strings.ToLower(q)
		if c == "" || (q != table && (alias == "" || q != alias)) {
			return "", false
		}
		return strings.ToLower(c), true
	}
	return "", false
}

func literalOf(n map[string]any) (literal, bool) {
	if str(n, "class") != "CONSTANT" {
		return literal{}, false
	}
	v, _ := n["value"].(map[string]any)
	if null, _ := v["is_null"].(bool); null || v == nil {
		return literal{}, false
	}
	typ, _ := v["type"].(map[string]any)
	id := str(typ, "id")
	num, _ := v["value"].(json.Number)
	switch id {
	case "TINYINT", "SMALLINT", "INTEGER", "BIGINT":
		i, err := num.Int64()
		if err != nil {
			return literal{}, false
		}
		return literal{kind: litInt, i: i}, true
	case "DOUBLE", "FLOAT":
		f, err := num.Float64()
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
			return literal{}, false
		}
		return literal{kind: litFloat, f: f}, true
	case "DECIMAL":
		info, _ := typ["type_info"].(map[string]any)
		scale, ok := info["scale"].(json.Number)
		if !ok {
			return literal{}, false
		}
		sc, err := scale.Int64()
		i, err2 := num.Int64()
		if err != nil || err2 != nil || sc < 0 || sc > 18 {
			return literal{}, false
		}
		return literal{kind: litFloat, f: float64(i) / math.Pow10(int(sc))}, true
	case "VARCHAR":
		s, ok := v["value"].(string)
		return literal{kind: litString, s: s}, ok
	}
	return literal{}, false
}

// ---- evaluation ----------------------------------------------------------

// ulpWiden is how many units in the last place a floating-point literal is
// widened by before comparing against a double column, so rounding of decimal
// literals (1.1 is not exactly representable) can never wrongly exclude a file.
const ulpWiden = 4

func widen(f float64) (lo, hi float64) {
	lo, hi = f, f
	for i := 0; i < ulpWiden; i++ {
		lo = math.Nextafter(lo, math.Inf(-1))
		hi = math.Nextafter(hi, math.Inf(1))
	}
	return lo, hi
}

// MayMatchFile reports whether a file could contain a row satisfying every
// constraint. It returns false only when the statistics prove at least one
// constraint cannot be met by any row.
func MayMatchFile(cons []Constraint, st *parquet.FileStats) bool {
	if st == nil {
		return true
	}
	for _, c := range cons {
		cs, ok := st.Columns[c.Column]
		if !ok {
			continue
		}
		// A NULL never satisfies a comparison, so a file whose column is
		// entirely NULL cannot match any comparison on it.
		if cs.HasNullCount && st.NumRows > 0 && cs.NullCount == st.NumRows {
			return false
		}
		if !cs.HasMinMax {
			continue
		}
		if !constraintMayMatch(c, cs) {
			return false
		}
	}
	return true
}

func constraintMayMatch(c Constraint, cs parquet.ColumnStats) bool {
	if c.Op == "in" {
		for _, v := range c.Values {
			if valueMayMatch("=", v, cs) {
				return true
			}
		}
		return false
	}
	return valueMayMatch(c.Op, c.Values[0], cs)
}

// valueMayMatch: can any value in [min, max] satisfy `col op lit`?
func valueMayMatch(op string, l literal, cs parquet.ColumnStats) bool {
	switch cs.Type {
	case "int32", "int64":
		if l.kind != litInt {
			return true // mixed int/decimal comparison: leave to the engine
		}
		return cmpRange(op, cmpInt(l.i, cs.MinInt), cmpInt(l.i, cs.MaxInt), cs.MinInt == cs.MaxInt)
	case "double":
		var f float64
		switch l.kind {
		case litInt:
			f = float64(l.i)
		case litFloat:
			f = l.f
		default:
			return true
		}
		lo, hi := widen(f)
		switch op {
		case "=":
			return !(hi < cs.MinFloat || lo > cs.MaxFloat)
		case "<":
			return cs.MinFloat < hi
		case "<=":
			return cs.MinFloat <= hi
		case ">":
			return cs.MaxFloat > lo
		case ">=":
			return cs.MaxFloat >= lo
		}
		return true // "<>" on doubles: never prune
	case "string":
		if l.kind != litString {
			return true
		}
		return cmpRange(op, strings.Compare(l.s, cs.MinStr), strings.Compare(l.s, cs.MaxStr), cs.MinStr == cs.MaxStr)
	}
	return true
}

func cmpInt(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// cmpRange decides `col op lit` over a range given cmp(lit, min), cmp(lit, max)
// (negative / zero / positive) and whether min == max.
func cmpRange(op string, vsMin, vsMax int, point bool) bool {
	switch op {
	case "=":
		return vsMin >= 0 && vsMax <= 0
	case "<>":
		return !point || vsMin != 0
	case "<": // exists col < lit  <=>  min < lit
		return vsMin > 0
	case "<=":
		return vsMin >= 0
	case ">": // exists col > lit  <=>  max > lit
		return vsMax < 0
	case ">=":
		return vsMax <= 0
	}
	return true
}

var (
	hiveInt    = regexp.MustCompile(`^(0|-?[1-9][0-9]{0,17})$`)
	hiveString = regexp.MustCompile(`^[A-Za-z_][A-Za-z_-]*$`)
)

// MayMatchPartition applies the constraints to hive `name=value` directory
// segments in rel (an object key relative to the dataset prefix). Values are
// compared only when DuckDB's type auto-detection cannot change their meaning:
// canonical integers against integer literals, and purely alphabetic strings
// against string literals. Anything else leaves the file in.
func MayMatchPartition(cons []Constraint, rel string) bool {
	segs := strings.Split(rel, "/")
	if len(segs) < 2 {
		return true
	}
	parts := map[string]string{}
	for _, seg := range segs[:len(segs)-1] {
		if k, v, ok := strings.Cut(seg, "="); ok && k != "" {
			parts[strings.ToLower(k)] = v
		}
	}
	for _, c := range cons {
		v, ok := parts[c.Column]
		if !ok {
			continue
		}
		var cs parquet.ColumnStats
		switch {
		case hiveInt.MatchString(v):
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				continue
			}
			cs = parquet.ColumnStats{Type: "int64", HasMinMax: true, MinInt: n, MaxInt: n}
		case hiveString.MatchString(v):
			cs = parquet.ColumnStats{Type: "string", HasMinMax: true, MinStr: v, MaxStr: v}
		default:
			continue
		}
		if !constraintMayMatch(c, cs) {
			return false
		}
	}
	return true
}
