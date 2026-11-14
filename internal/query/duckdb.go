package query

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/akshat281204/lakehouse-query-studio/internal/parquet"
)

// DuckDB is the single-node DuckDB implementation of Engine. Every query runs
// in its own private in-memory database that is closed afterwards, so no state,
// view or setting can leak between queries. DuckDB holds no object-store
// credentials: datasets are staged to local files by the worker beforehand.
type DuckDB struct{}

// NewDuckDB returns the engine.
func NewDuckDB() *DuckDB { return &DuckDB{} }

func sqlString(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
func sqlIdent(s string) string  { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

// Execute implements Engine.
func (d *DuckDB) Execute(ctx context.Context, req Request) (res *Result, err error) {
	// The cgo driver panics on values it cannot represent (for example a
	// string over 2 GiB). That must fail the query, never kill the worker.
	defer func() {
		if r := recover(); r != nil {
			res, err = nil, fmt.Errorf("%w: a result value could not be read", ErrLimit)
		}
	}()
	lim := req.Limits
	if lim.Timeout <= 0 {
		lim = DefaultLimits()
	}
	ctx, cancel := context.WithTimeout(ctx, lim.Timeout)
	defer cancel()

	// Phase 1 (trusted): build the catalog views in a private database file.
	// Phase 2: reopen it read-only and seal the connection before user SQL.
	dbPath := filepath.Join(req.WorkDir, "engine.duckdb")
	if err := d.build(ctx, dbPath, req); err != nil {
		return nil, err
	}
	db, err := sql.Open("duckdb", dbPath+"?access_mode=read_only")
	if err != nil {
		return nil, fmt.Errorf("%w: open engine", ErrEngine)
	}
	db.SetMaxOpenConns(1)
	defer func() { _ = db.Close() }()
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: open engine", ErrEngine)
	}
	defer func() { _ = conn.Close() }()

	if err := d.seal(ctx, conn, req); err != nil {
		return nil, err
	}
	return d.run(ctx, conn, req, lim)
}

// build is trusted worker code: it binds relation names to staged files as
// views in a fresh database file. No user text reaches this step.
func (d *DuckDB) build(ctx context.Context, dbPath string, req Request) error {
	_ = os.Remove(dbPath)
	_ = os.Remove(dbPath + ".wal")
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return fmt.Errorf("%w: engine setup", ErrEngine)
	}
	defer func() { _ = db.Close() }()
	exec := func(q string) error {
		if _, err := db.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("%w: engine setup", ErrEngine)
		}
		return nil
	}
	if err := exec("CREATE SCHEMA catalog"); err != nil {
		return err
	}
	for _, s := range req.Sources {
		glob := strings.TrimRight(s.Dir, "/") + "/**/*.parquet"
		for _, name := range s.Names {
			q := fmt.Sprintf("CREATE VIEW catalog.%s AS SELECT * FROM read_parquet(%s, hive_partitioning=%t, union_by_name=true)",
				sqlIdent(name), sqlString(glob), s.HivePartitioning)
			if err := exec(q); err != nil {
				return err
			}
		}
	}
	return nil
}

// seal applies budgets and locks the configuration. After lock_configuration
// no statement can change a setting, enable external access, or load an
// extension; the only readable directories are the job's own staged data.
func (d *DuckDB) seal(ctx context.Context, conn *sql.Conn, req Request) error {
	lim := req.Limits
	exec := func(q string) error {
		if _, err := conn.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("%w: engine setup", ErrEngine)
		}
		return nil
	}
	if err := exec("SET schema='catalog'"); err != nil {
		return err
	}
	threads := lim.Threads
	if threads < 1 {
		threads = 1
	}
	mem := lim.MemoryLimit
	if mem == "" {
		mem = "2GiB"
	}
	allowed := []string{req.WorkDir}
	for _, s := range req.Sources {
		allowed = append(allowed, s.Dir)
	}
	quoted := make([]string, len(allowed))
	for i, a := range allowed {
		quoted[i] = sqlString(strings.TrimRight(a, "/") + "/")
	}
	for _, q := range []string{
		fmt.Sprintf("SET threads=%d", threads),
		"SET memory_limit=" + sqlString(mem),
		"SET temp_directory=" + sqlString(strings.TrimRight(req.WorkDir, "/")+"/spill"),
		"SET max_temp_directory_size='2GiB'",
		"SET autoinstall_known_extensions=false",
		"SET autoload_known_extensions=false",
		"SET allow_community_extensions=false",
		"SET allow_unsigned_extensions=false",
		"SET allowed_directories=[" + strings.Join(quoted, ",") + "]",
		"SET enable_external_access=false",
		"SET lock_configuration=true",
	} {
		if err := exec(q); err != nil {
			return err
		}
	}
	return nil
}

func (d *DuckDB) run(ctx context.Context, conn *sql.Conn, req Request, lim Limits) (*Result, error) {
	rows, err := conn.QueryContext(ctx, req.Statement)
	if err != nil {
		return nil, classifyEngineError(ctx, err, req)
	}
	defer func() { _ = rows.Close() }()

	names, err := rows.Columns()
	if err != nil {
		return nil, classifyEngineError(ctx, err, req)
	}
	ctypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, classifyEngineError(ctx, err, req)
	}
	cols := make([]parquet.WriteColumn, len(names))
	kinds := make([]string, len(names))
	seen := map[string]int{}
	for i, n := range names {
		if n == "" {
			n = fmt.Sprintf("column%d", i+1)
		}
		base := n
		if c := seen[strings.ToLower(base)]; c > 0 {
			n = fmt.Sprintf("%s_%d", base, c+1)
		}
		seen[strings.ToLower(base)]++
		kind := parquetKind(ctypes[i].DatabaseTypeName())
		kinds[i] = kind
		cols[i] = parquet.WriteColumn{Name: n, Type: kind, Nullable: true}
	}

	res := &Result{Columns: cols}
	var total int64
	for rows.Next() {
		if len(res.Rows) >= lim.MaxRows {
			res.Truncated = true // the probe row: discarded, never spooled
			break
		}
		raw := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range raw {
			ptrs[i] = &raw[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, classifyEngineError(ctx, err, req)
		}
		row := make([]any, len(cols))
		var rowBytes int64
		for i, v := range raw {
			cv, n := convert(kinds[i], v)
			row[i] = cv
			rowBytes += n
		}
		if rowBytes > lim.MaxRowBytes {
			return nil, fmt.Errorf("%w: a result row exceeds the row size limit", ErrLimit)
		}
		if total+rowBytes > lim.MaxResultBytes {
			res.Truncated = true
			break
		}
		total += rowBytes
		res.Rows = append(res.Rows, row)
	}
	if err := rows.Err(); err != nil {
		return nil, classifyEngineError(ctx, err, req)
	}
	if err := ctx.Err(); err != nil {
		return nil, classifyEngineError(ctx, err, req)
	}
	_ = rows.Close()
	res.PlanSummary = d.plan(ctx, conn, req)
	return res, nil
}

// plan returns a bounded EXPLAIN of the already-validated statement with local
// paths removed.
func (d *DuckDB) plan(ctx context.Context, conn *sql.Conn, req Request) string {
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var key, plan string
	if err := conn.QueryRowContext(pctx, "EXPLAIN "+req.Statement).Scan(&key, &plan); err != nil {
		return ""
	}
	plan = scrubPaths(plan, req)
	if len(plan) > 4096 {
		plan = plan[:4096]
	}
	return plan
}

func scrubPaths(s string, req Request) string {
	for _, src := range req.Sources {
		s = strings.ReplaceAll(s, strings.TrimRight(src.Dir, "/"), "<data>")
	}
	return strings.ReplaceAll(s, strings.TrimRight(req.WorkDir, "/"), "<work>")
}

func parquetKind(dbType string) string {
	switch strings.ToUpper(dbType) {
	case "BOOLEAN":
		return parquet.TypeBoolean
	case "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "UTINYINT", "USMALLINT", "UINTEGER":
		return parquet.TypeInt64
	case "FLOAT", "DOUBLE":
		return parquet.TypeDouble
	default:
		return parquet.TypeString
	}
}

// convert maps a scanned value to the column's spool type and returns its
// approximate encoded size. Types without a native spool encoding (decimals,
// temporals, nested values, blobs, 128-bit integers) are spooled as text.
func convert(kind string, v any) (any, int64) {
	if v == nil {
		return nil, 1
	}
	switch kind {
	case parquet.TypeBoolean:
		if b, ok := v.(bool); ok {
			return b, 1
		}
	case parquet.TypeInt64:
		switch x := v.(type) {
		case int8:
			return int64(x), 8
		case int16:
			return int64(x), 8
		case int32:
			return int64(x), 8
		case int64:
			return x, 8
		case uint8:
			return int64(x), 8
		case uint16:
			return int64(x), 8
		case uint32:
			return int64(x), 8
		case int:
			return int64(x), 8
		}
	case parquet.TypeDouble:
		switch x := v.(type) {
		case float32:
			return float64(x), 8
		case float64:
			return x, 8
		}
	}
	s := textOf(v)
	return s, int64(len(s)) + 4
}

func textOf(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return fmt.Sprintf("\\x%x", x)
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano)
	case *big.Int:
		return x.String()
	case fmt.Stringer:
		return x.String()
	case bool, int8, int16, int32, int64, uint8, uint16, uint32, uint64, float32, float64:
		return fmt.Sprint(x)
	}
	if b, err := json.Marshal(v); err == nil {
		return string(b)
	}
	return fmt.Sprint(v)
}

// classifyEngineError maps a DuckDB/driver error to a sentinel. DuckDB error
// text can embed SQL fragments and file paths, so only an allowlisted class
// of user-actionable errors keeps a sanitized first line; everything else is a
// generic message.
func classifyEngineError(ctx context.Context, err error, req Request) error {
	switch {
	case errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded):
		return ErrTimeout
	case errors.Is(ctx.Err(), context.Canceled) || errors.Is(err, context.Canceled):
		return ErrCancelled
	}
	msg := err.Error()
	low := strings.ToLower(msg)
	switch {
	case strings.Contains(low, "out of memory"), strings.Contains(low, "memory limit"), strings.Contains(low, "max_temp_directory_size"):
		return fmt.Errorf("%w: query exceeded the memory limit", ErrLimit)
	case strings.Contains(low, "interrupt"):
		return ErrCancelled
	}
	for _, p := range []string{"Binder Error", "Catalog Error", "Conversion Error", "Invalid Input Error", "Parser Error"} {
		if strings.HasPrefix(msg, p) {
			line := msg
			if i := strings.IndexByte(line, '\n'); i >= 0 {
				line = line[:i]
			}
			line = scrubPaths(line, req)
			if len(line) > 200 {
				line = line[:200]
			}
			return fmt.Errorf("%w: %s", ErrEngine, line)
		}
	}
	return fmt.Errorf("%w: execution error", ErrEngine)
}
