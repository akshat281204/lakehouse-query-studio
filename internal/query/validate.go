package query

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	_ "github.com/marcboeker/go-duckdb/v2" // registers the "duckdb" driver
)

// MaxSQLBytes is the hard cap on submitted statement text (design §12).
const MaxSQLBytes = 64 << 10

// ErrDenied marks SQL rejected by the safety boundary (API code query_denied).
// Its message is safe to show: it never echoes the statement.
var ErrDenied = errors.New("query denied")

func denied(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrDenied, fmt.Sprintf(format, args...))
}

// TableRef is a dataset relation referenced by a statement. Name is either
// `<dataset>` (latest READY version) or `<dataset>_v<n>`; Schema is always
// "" or "catalog".
type TableRef struct {
	Name string
}

// Validated is the outcome of a successful validation.
type Validated struct {
	Tables []TableRef
}

// Validator parses SQL with DuckDB's own parser (json_serialize_sql, which
// returns the AST without binding or executing anything) and walks the tree
// against closed allowlists. It never executes the statement. The parser runs
// on a private in-memory database that holds no data and no credentials.
type Validator struct {
	mu sync.Mutex
	db *sql.DB
}

// NewValidator opens the private parser database.
func NewValidator() (*Validator, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return &Validator{db: db}, nil
}

// Close releases the parser database.
func (v *Validator) Close() error { return v.db.Close() }

// Validate returns the referenced dataset relations or a query-denied error.
func (v *Validator) Validate(ctx context.Context, statement string) (*Validated, error) {
	if strings.TrimSpace(statement) == "" {
		return nil, denied("statement is empty")
	}
	if len(statement) > MaxSQLBytes {
		return nil, denied("statement exceeds %d bytes", MaxSQLBytes)
	}
	if strings.ContainsRune(statement, 0) {
		return nil, denied("statement contains a NUL byte")
	}

	var raw string
	v.mu.Lock()
	err := v.db.QueryRowContext(ctx, "SELECT json_serialize_sql('"+strings.ReplaceAll(statement, "'", "''")+"')::VARCHAR").Scan(&raw)
	v.mu.Unlock()
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, denied("statement could not be parsed")
	}

	var root struct {
		Error      bool              `json:"error"`
		Statements []json.RawMessage `json:"statements"`
	}
	if err := json.Unmarshal([]byte(raw), &root); err != nil {
		return nil, denied("statement could not be parsed")
	}
	if root.Error {
		// json_serialize_sql only serializes SELECT statements, so DDL, DML,
		// COPY, ATTACH, PRAGMA, SET and the rest all land here.
		return nil, denied("only a single read-only SELECT statement is allowed")
	}
	if len(root.Statements) != 1 {
		return nil, denied("exactly one statement is allowed")
	}

	var stmt struct {
		Node json.RawMessage `json:"node"`
	}
	if err := json.Unmarshal(root.Statements[0], &stmt); err != nil {
		return nil, denied("statement could not be parsed")
	}
	var node any
	if err := json.Unmarshal(stmt.Node, &node); err != nil {
		return nil, denied("statement could not be parsed")
	}

	w := &walker{ctes: map[string]bool{}, tables: map[string]bool{}}
	top, _ := node.(map[string]any)
	switch str(top, "type") {
	case "SELECT_NODE", "SET_OPERATION_NODE", "CTE_NODE":
	default:
		return nil, denied("only SELECT statements are allowed")
	}
	w.collectCTEs(node)
	if err := w.walk(node, 0); err != nil {
		return nil, err
	}
	out := &Validated{}
	for name := range w.tables {
		out.Tables = append(out.Tables, TableRef{Name: name})
	}
	sortTables(out.Tables)
	return out, nil
}

func sortTables(t []TableRef) {
	for i := 1; i < len(t); i++ {
		for j := i; j > 0 && t[j].Name < t[j-1].Name; j-- {
			t[j], t[j-1] = t[j-1], t[j]
		}
	}
}

const maxDepth = 200

type walker struct {
	ctes   map[string]bool
	tables map[string]bool
}

func str(m map[string]any, k string) string {
	s, _ := m[k].(string)
	return s
}

// collectCTEs records every CTE name defined anywhere in the statement so a
// reference to one is not mistaken for a dataset.
func (w *walker) collectCTEs(n any) {
	switch x := n.(type) {
	case map[string]any:
		if cm, ok := x["cte_map"].(map[string]any); ok {
			if entries, ok := cm["map"].([]any); ok {
				for _, e := range entries {
					if em, ok := e.(map[string]any); ok {
						if k, ok := em["key"].(string); ok {
							w.ctes[strings.ToLower(k)] = true
						}
					}
				}
			}
		}
		for _, v := range x {
			w.collectCTEs(v)
		}
	case []any:
		for _, v := range x {
			w.collectCTEs(v)
		}
	}
}

func (w *walker) walk(n any, depth int) error {
	if depth > maxDepth {
		return denied("statement is nested too deeply")
	}
	switch x := n.(type) {
	case []any:
		for _, v := range x {
			if err := w.walk(v, depth+1); err != nil {
				return err
			}
		}
	case map[string]any:
		if err := w.checkNode(x); err != nil {
			return err
		}
		for _, v := range x {
			if err := w.walk(v, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

// Node "type" values that appear for expressions, table refs and query nodes.
// Anything not listed is denied (deny by default).
var allowedNodeTypes = map[string]bool{
	// query nodes
	"SELECT_NODE": true, "SET_OPERATION_NODE": true, "RECURSIVE_CTE_NODE": false,
	"CTE_NODE": true,
	// table refs
	"BASE_TABLE": true, "JOIN": true, "SUBQUERY": true, "EMPTY": true, "TABLE_FUNCTION": true,
	// modifiers
	"ORDER_MODIFIER": true, "LIMIT_MODIFIER": true, "DISTINCT_MODIFIER": true,
	"LIMIT_PERCENT_MODIFIER": true,
}

// Expression "class" values that are allowed. LAMBDA, PARAMETER, COLLATE,
// DEFAULT, POSITIONAL_REFERENCE and anything unknown are denied.
var allowedClasses = map[string]bool{
	"COLUMN_REF": true, "CONSTANT": true, "COMPARISON": true, "CONJUNCTION": true,
	"CASE": true, "CAST": true, "BETWEEN": true, "OPERATOR": true, "STAR": true,
	"FUNCTION": true, "WINDOW": true, "SUBQUERY": true,
}

func (w *walker) checkNode(m map[string]any) error {
	if cls := str(m, "class"); cls != "" {
		if !allowedClasses[cls] {
			return denied("expression type %s is not allowed", strings.ToLower(cls))
		}
		switch cls {
		case "FUNCTION":
			if err := checkFunction(m); err != nil {
				return err
			}
		case "WINDOW":
			name := strings.ToLower(str(m, "function_name"))
			if !windowFunctions[name] && !aggregateFunctions[name] {
				return denied("function %q is not allowed", clip(name))
			}
		case "COLUMN_REF":
			if names, ok := m["column_names"].([]any); ok && len(names) == 1 {
				if n, _ := names[0].(string); bareFunctions[strings.ToLower(n)] {
					return denied("%q is not allowed", clip(n))
				}
			}
		case "STAR":
			if _, ok := m["replace_list"]; ok {
				if rl, _ := m["replace_list"].(map[string]any); len(rl) > 0 {
					return denied("star replace lists are not allowed")
				}
			}
		}
	}
	t := str(m, "type")
	switch t {
	case "BASE_TABLE":
		return w.checkBaseTable(m)
	case "TABLE_FUNCTION":
		fn, _ := m["function"].(map[string]any)
		name := strings.ToLower(str(fn, "function_name"))
		if name != "unnest" {
			return denied("table function %q is not allowed", clip(name))
		}
	case "SELECT_NODE", "SET_OPERATION_NODE", "JOIN", "SUBQUERY", "EMPTY",
		"ORDER_MODIFIER", "LIMIT_MODIFIER", "DISTINCT_MODIFIER", "LIMIT_PERCENT_MODIFIER",
		"CTE_NODE":
	default:
		if strings.HasSuffix(t, "_NODE") || strings.HasSuffix(t, "_MODIFIER") {
			if !allowedNodeTypes[t] {
				return denied("%s is not allowed", strings.ToLower(t))
			}
		}
		// Table-ref types appear as the "type" of from_table objects. Unknown
		// ones (PIVOT, EXPRESSION_LIST, SHOW_REF, ...) carry a "from_table"
		// parent; reject any such type here.
		switch t {
		case "PIVOT", "EXPRESSION_LIST", "SHOW_REF", "COLUMN_DATA", "DELIM_GET", "CTE", "BOUND_TABLE_REF":
			return denied("table expression %s is not allowed", strings.ToLower(t))
		}
	}
	return nil
}

// bareFunctions are zero-argument session/system functions DuckDB lets a
// query call without parentheses; the parser reports them as column refs.
var bareFunctions = set(
	"current_user", "session_user", "user", "current_schema", "current_schemas",
	"current_catalog", "current_database", "version",
)

func (w *walker) checkBaseTable(m map[string]any) error {
	name := str(m, "table_name")
	schema := strings.ToLower(str(m, "schema_name"))
	catalog := str(m, "catalog_name")
	if catalog != "" {
		return denied("catalog-qualified table references are not allowed")
	}
	if name == "" {
		return denied("table reference is empty")
	}
	if schema == "" && w.ctes[strings.ToLower(name)] {
		return nil
	}
	if schema != "" && schema != "catalog" {
		return denied("only catalog.<dataset> relations may be referenced")
	}
	if strings.ContainsAny(name, "/\\:") || strings.Contains(name, "..") {
		return denied("table reference is not a dataset name")
	}
	if m["at_clause"] != nil {
		return denied("time travel clauses are not allowed")
	}
	w.tables[strings.ToLower(name)] = true
	return nil
}

func checkFunction(m map[string]any) error {
	switch strings.ToLower(str(m, "schema")) {
	case "", "main", "pg_catalog": // the parser qualifies synthesized calls
	default:
		return denied("schema-qualified function calls are not allowed")
	}
	if c := str(m, "catalog"); c != "" {
		return denied("catalog-qualified function calls are not allowed")
	}
	name := strings.ToLower(str(m, "function_name"))
	if name == "" {
		return denied("function call has no name")
	}
	isOp, _ := m["is_operator"].(bool)
	if isOp {
		if !operatorNames[name] {
			return denied("operator %q is not allowed", clip(name))
		}
		return nil
	}
	if !scalarFunctions[name] && !aggregateFunctions[name] && !windowFunctions[name] && !internalFunctions[name] {
		return denied("function %q is not allowed", clip(name))
	}
	return checkAmplification(name, m)
}

// maxPadLength bounds the constant length argument of repeat/lpad/rpad.
// DuckDB's memory_limit does not bound a single transient string value (a 4 GB
// repeat() was measured materializing under a 64 MiB limit), so functions that
// can mint an arbitrarily large value from a tiny input only accept a small
// constant length.
const maxPadLength = 1 << 20

func checkAmplification(name string, m map[string]any) error {
	switch name {
	case "repeat", "lpad", "rpad":
	default:
		return nil
	}
	args, _ := m["children"].([]any)
	if len(args) < 2 {
		return denied("%s needs a constant length argument", name)
	}
	arg, _ := args[1].(map[string]any)
	if str(arg, "class") != "CONSTANT" {
		return denied("%s length must be a constant of at most %d", name, maxPadLength)
	}
	val, _ := arg["value"].(map[string]any)
	if null, _ := val["is_null"].(bool); null {
		return nil
	}
	n, ok := val["value"].(float64)
	if !ok || n > maxPadLength {
		return denied("%s length must be a constant of at most %d", name, maxPadLength)
	}
	return nil
}

func clip(s string) string {
	if len(s) > 40 {
		return s[:40]
	}
	return s
}
