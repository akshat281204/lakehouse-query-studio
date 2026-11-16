// Package ingest validates uploaded CSV/JSON/JSONL files, infers a flat schema,
// converts the rows to partitioned Parquet parts, and runs the asynchronous
// conversion jobs that publish them as catalog versions.
package ingest

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/akshat281204/lakehouse-query-studio/internal/catalog"
	"github.com/akshat281204/lakehouse-query-studio/internal/parquet"
)

// Limits bound every dimension of an ingestion so no input can exhaust memory.
type Limits struct {
	MaxBytes      int64 // upload size
	MaxRows       int   // total rows
	MaxColumns    int
	PartRows      int // rows per Parquet part
	MaxPartitions int // distinct partition directories
}

// DefaultLimits are the documented v1 limits.
var DefaultLimits = Limits{
	MaxBytes:      64 << 20,
	MaxRows:       1_000_000,
	MaxColumns:    256,
	PartRows:      100_000,
	MaxPartitions: 256,
}

// UserError is an input problem with a message that is safe to show: it never
// contains cell values. Jobs failing with a UserError are not retryable.
type UserError struct{ Msg string }

func (e *UserError) Error() string { return e.Msg }

func userErrf(format string, a ...any) error { return &UserError{Msg: fmt.Sprintf(format, a...)} }

// IsUserError reports whether err is an input validation failure.
func IsUserError(err error) bool {
	var u *UserError
	return errors.As(err, &u)
}

// Table is a typed, flat set of rows ready for Parquet encoding.
type Table struct {
	Columns []parquet.WriteColumn
	Rows    [][]any
}

// Parse validates and parses data in the given catalog.InputFormat* format.
func Parse(format string, data []byte, lim Limits) (*Table, error) {
	if int64(len(data)) > lim.MaxBytes {
		return nil, userErrf("file exceeds the %d MiB upload limit", lim.MaxBytes>>20)
	}
	if !utf8.Valid(data) {
		return nil, userErrf("file is not valid UTF-8")
	}
	data = bytes.TrimPrefix(data, []byte("\xef\xbb\xbf"))
	switch format {
	case catalog.InputFormatCSV:
		return parseCSV(data, lim)
	case catalog.InputFormatJSONL, catalog.InputFormatJSON:
		return parseJSON(format, data, lim)
	default:
		return nil, userErrf("unsupported input format %q", format)
	}
}

// ValidateColumnName enforces the column-name rules for converted datasets.
func ValidateColumnName(name string) error {
	switch {
	case name == "":
		return userErrf("column names must not be empty")
	case len(name) > 128:
		return userErrf("column names must be at most 128 bytes")
	case strings.TrimSpace(name) != name:
		return userErrf("column names must not have leading or trailing whitespace")
	}
	for _, r := range name {
		if r < 0x20 || r == 0x7f || r == utf8.RuneError {
			return userErrf("column names must not contain control characters")
		}
	}
	return nil
}

func validateColumns(names []string, lim Limits) error {
	if len(names) == 0 {
		return userErrf("no columns found")
	}
	if len(names) > lim.MaxColumns {
		return userErrf("more than %d columns", lim.MaxColumns)
	}
	seen := make(map[string]struct{}, len(names))
	for _, n := range names {
		if err := ValidateColumnName(n); err != nil {
			return err
		}
		k := strings.ToLower(n)
		if _, dup := seen[k]; dup {
			return userErrf("duplicate column name (names are compared case-insensitively)")
		}
		seen[k] = struct{}{}
	}
	return nil
}

// kind is the inferred type lattice: bool, int, float, string.
type kind int

const (
	kUnknown kind = iota
	kBool
	kInt
	kFloat
	kString
)

func merge(a, b kind) kind {
	if a == kUnknown {
		return b
	}
	if b == kUnknown || a == b {
		return a
	}
	if (a == kInt && b == kFloat) || (a == kFloat && b == kInt) {
		return kFloat
	}
	return kString
}

func (k kind) parquetType() string {
	switch k {
	case kBool:
		return parquet.TypeBoolean
	case kInt:
		return parquet.TypeInt64
	case kFloat:
		return parquet.TypeDouble
	default:
		return parquet.TypeString
	}
}

func classify(s string) kind {
	switch strings.ToLower(s) {
	case "true", "false":
		return kBool
	}
	if _, err := strconv.ParseInt(s, 10, 64); err == nil {
		return kInt
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil && !math.IsNaN(f) && !math.IsInf(f, 0) {
		return kFloat
	}
	return kString
}

func convertText(s string, k kind) any {
	switch k {
	case kBool:
		return strings.EqualFold(s, "true")
	case kInt:
		n, _ := strconv.ParseInt(s, 10, 64)
		return n
	case kFloat:
		f, _ := strconv.ParseFloat(s, 64)
		return f
	default:
		return s
	}
}

func parseCSV(data []byte, lim Limits) (*Table, error) {
	r := csv.NewReader(bytes.NewReader(data))
	r.FieldsPerRecord = -1 // field counts are validated below with a line number
	header, err := r.Read()
	if err == io.EOF {
		return nil, userErrf("file is empty")
	}
	if err != nil {
		return nil, userErrf("invalid CSV header")
	}
	names := append([]string(nil), header...)
	if err := validateColumns(names, lim); err != nil {
		return nil, err
	}
	n := len(names)
	kinds := make([]kind, n)
	nullable := make([]bool, n)
	var raw [][]string
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			var pe *csv.ParseError
			if errors.As(err, &pe) {
				return nil, userErrf("invalid CSV near line %d (check the quoting)", pe.Line)
			}
			return nil, userErrf("invalid CSV")
		}
		if len(rec) != n {
			line, _ := r.FieldPos(0)
			return nil, userErrf("row at line %d has %d fields, expected %d", line, len(rec), n)
		}
		if len(raw) >= lim.MaxRows {
			return nil, userErrf("more than %d rows", lim.MaxRows)
		}
		for i, v := range rec {
			if strings.ContainsRune(v, 0) {
				return nil, userErrf("values must not contain NUL characters")
			}
			if v == "" {
				nullable[i] = true
				continue
			}
			kinds[i] = merge(kinds[i], classify(v))
		}
		raw = append(raw, rec)
	}
	if len(raw) == 0 {
		return nil, userErrf("file has a header but no data rows")
	}
	t := &Table{Columns: make([]parquet.WriteColumn, n), Rows: make([][]any, len(raw))}
	for i := range names {
		if kinds[i] == kUnknown {
			kinds[i] = kString // entirely empty column
		}
		t.Columns[i] = parquet.WriteColumn{Name: names[i], Type: kinds[i].parquetType(), Nullable: nullable[i]}
	}
	for ri, rec := range raw {
		row := make([]any, n)
		for i, v := range rec {
			if v == "" {
				continue
			}
			row[i] = convertText(v, kinds[i])
		}
		t.Rows[ri] = row
	}
	return t, nil
}

type jsonCell struct {
	kind kind
	text string // canonical text for strings / mixed values
	null bool
}

func parseJSON(format string, data []byte, lim Limits) (*Table, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()

	var records []map[string]any
	if format == catalog.InputFormatJSON {
		tok, err := dec.Token()
		if err == io.EOF {
			return nil, userErrf("file is empty")
		}
		if d, ok := tok.(json.Delim); err != nil || !ok || d != '[' {
			return nil, userErrf("JSON input must be an array of objects")
		}
		for dec.More() {
			rec, err := decodeRecord(dec)
			if err != nil {
				return nil, err
			}
			if len(records) >= lim.MaxRows {
				return nil, userErrf("more than %d rows", lim.MaxRows)
			}
			records = append(records, rec)
		}
		if _, err := dec.Token(); err != nil {
			return nil, userErrf("invalid JSON array")
		}
	} else {
		for {
			rec, err := decodeRecord(dec)
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			if len(records) >= lim.MaxRows {
				return nil, userErrf("more than %d rows", lim.MaxRows)
			}
			records = append(records, rec)
		}
	}
	if len(records) == 0 {
		return nil, userErrf("file contains no records")
	}

	var names []string
	index := map[string]int{}
	for _, rec := range records {
		for _, k := range sortedKeys(rec) {
			if _, ok := index[k]; !ok {
				index[k] = len(names)
				names = append(names, k)
			}
		}
	}
	if err := validateColumns(names, lim); err != nil {
		return nil, err
	}

	kinds := make([]kind, len(names))
	nullable := make([]bool, len(names))
	cells := make([][]jsonCell, len(records))
	for ri, rec := range records {
		row := make([]jsonCell, len(names))
		for i := range row {
			row[i].null = true
		}
		for k, v := range rec {
			i := index[k]
			c, err := classifyJSON(v)
			if err != nil {
				return nil, err
			}
			row[i] = c
		}
		for i, c := range row {
			if c.null {
				nullable[i] = true
				continue
			}
			kinds[i] = merge(kinds[i], c.kind)
		}
		cells[ri] = row
	}

	t := &Table{Columns: make([]parquet.WriteColumn, len(names)), Rows: make([][]any, len(records))}
	for i, n := range names {
		if kinds[i] == kUnknown {
			kinds[i] = kString
		}
		t.Columns[i] = parquet.WriteColumn{Name: n, Type: kinds[i].parquetType(), Nullable: nullable[i]}
	}
	for ri, row := range cells {
		out := make([]any, len(names))
		for i, c := range row {
			if c.null {
				continue
			}
			out[i] = convertText(c.text, kinds[i])
		}
		t.Rows[ri] = out
	}
	return t, nil
}

func decodeRecord(dec *json.Decoder) (map[string]any, error) {
	var rec map[string]any
	if err := dec.Decode(&rec); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, userErrf("invalid JSON record")
	}
	if rec == nil {
		return nil, userErrf("every record must be a JSON object")
	}
	return rec, nil
}

func classifyJSON(v any) (jsonCell, error) {
	switch x := v.(type) {
	case nil:
		return jsonCell{null: true}, nil
	case bool:
		return jsonCell{kind: kBool, text: strconv.FormatBool(x)}, nil
	case json.Number:
		return jsonCell{kind: classify(x.String()), text: x.String()}, nil
	case string:
		if strings.ContainsRune(x, 0) {
			return jsonCell{}, userErrf("values must not contain NUL characters")
		}
		return jsonCell{kind: kString, text: x}, nil
	default:
		return jsonCell{}, userErrf("nested objects and arrays are not supported; flatten them before upload")
	}
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
