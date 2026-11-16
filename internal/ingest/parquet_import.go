package ingest

import (
	"bytes"
	"context"
	"io"
	"strings"

	"github.com/akshat281204/lakehouse-query-studio/internal/catalog"
	"github.com/akshat281204/lakehouse-query-studio/internal/parquet"
)

// ParquetInfo is what an uploaded Parquet file declares about itself.
type ParquetInfo struct {
	Columns []catalog.IncomingColumn
	Rows    int64
}

type uploadBytes []byte

func (b uploadBytes) Range(_ context.Context, _ string, off, n int64) (io.ReadCloser, error) {
	if off < 0 || n < 0 || off > int64(len(b)) {
		return nil, io.ErrUnexpectedEOF
	}
	end := min(off+n, int64(len(b)))
	return io.NopCloser(bytes.NewReader(b[off:end])), nil
}

// ParseParquet validates an uploaded Parquet file for direct import. The file is
// stored byte for byte (never re-encoded), so this only checks that it is
// really a Parquet file whose footer can be read, and that its schema fits the
// same naming and size rules as converted datasets. Messages never echo
// content from the file.
func ParseParquet(data []byte, lim Limits) (*ParquetInfo, error) {
	if int64(len(data)) > lim.MaxBytes {
		return nil, userErrf("file exceeds the %d MiB upload limit", lim.MaxBytes>>20)
	}
	if len(data) < 12 || !bytes.HasPrefix(data, []byte("PAR1")) || !bytes.HasSuffix(data, []byte("PAR1")) {
		return nil, userErrf("the file is not a Parquet file (missing the PAR1 markers)")
	}
	schema, err := parquet.Inspect(context.Background(), uploadBytes(data), "upload", int64(len(data)))
	if err != nil {
		return nil, userErrf("the Parquet file could not be read: its footer or schema is invalid or uses an unsupported feature")
	}
	if len(schema.Columns) == 0 {
		return nil, userErrf("the Parquet file has no columns")
	}
	if len(schema.Columns) > lim.MaxColumns {
		return nil, userErrf("more than %d columns", lim.MaxColumns)
	}
	seen := map[string]bool{}
	info := &ParquetInfo{Rows: schema.NumRows, Columns: make([]catalog.IncomingColumn, len(schema.Columns))}
	for i, c := range schema.Columns {
		if err := ValidateColumnName(c.Name); err != nil {
			return nil, err
		}
		key := strings.ToLower(c.Name)
		if seen[key] {
			return nil, userErrf("column names must be unique ignoring case")
		}
		seen[key] = true
		info.Columns[i] = catalog.IncomingColumn{Name: c.Name, Type: c.Type, Nullable: c.Nullable}
	}
	return info, nil
}

// IncomingColumns lists a parsed table's columns for compatibility checks.
func IncomingColumns(t *Table) []catalog.IncomingColumn {
	out := make([]catalog.IncomingColumn, len(t.Columns))
	for i, c := range t.Columns {
		allNull := len(t.Rows) > 0
		for _, row := range t.Rows {
			if row[i] != nil {
				allNull = false
				break
			}
		}
		out[i] = catalog.IncomingColumn{Name: c.Name, Type: c.Type, Nullable: c.Nullable, AllNull: allNull}
	}
	return out
}

// Conform rearranges a parsed table into the dataset's column order and types so
// it can be appended: int64 values feeding a double column are widened, a
// nullable dataset column the data lacks is filled with NULL. It fails with a
// UserError naming every incompatibility (column names and types only).
func Conform(t *Table, base []catalog.SchemaColumn) (*Table, error) {
	in := IncomingColumns(t)
	if issues := catalog.CheckAppendSchema(base, in, true); len(issues) > 0 {
		return nil, userErrf("the new data does not match the dataset: %s", joinIssues(issues))
	}
	src := make(map[string]int, len(t.Columns))
	for i, c := range t.Columns {
		src[c.Name] = i
	}
	merged := catalog.MergeAppendSchema(base, in)
	out := &Table{Columns: make([]parquet.WriteColumn, len(base)), Rows: make([][]any, len(t.Rows))}
	from := make([]int, len(base))
	widen := make([]bool, len(base))
	for i, b := range base {
		idx, ok := src[b.Name]
		from[i] = -1
		if ok {
			from[i] = idx
			widen[i] = b.Type == parquet.TypeDouble && t.Columns[idx].Type == parquet.TypeInt64
			if in[idx].AllNull {
				from[i] = -1 // no values to carry over: the column is filled with NULL
			}
		}
		out.Columns[i] = parquet.WriteColumn{Name: b.Name, Type: b.Type, Nullable: merged[i].Nullable}
	}
	for r, row := range t.Rows {
		nr := make([]any, len(base))
		for i := range base {
			switch {
			case from[i] < 0:
				nr[i] = nil
			case widen[i]:
				if v, ok := row[from[i]].(int64); ok {
					nr[i] = float64(v)
				}
			default:
				nr[i] = row[from[i]]
			}
		}
		out.Rows[r] = nr
	}
	return out, nil
}

func joinIssues(issues []string) string {
	const max = 5
	if len(issues) > max {
		return strings.Join(issues[:max], "; ") + "; and " + itoa(len(issues)-max) + " more"
	}
	return strings.Join(issues, "; ")
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for ; n > 0; n /= 10 {
		b = append([]byte{byte('0' + n%10)}, b...)
	}
	return string(b)
}

// ParquetSample returns up to n rows of an uploaded Parquet file for display. A
// file the in-repo reader cannot decode (for example a compression codec it does
// not support) is still importable; the returned note says why there is no sample.
func ParquetSample(data []byte, n int) (rows []map[string]any, note string) {
	res, err := parquet.Preview(context.Background(), uploadBytes(data), "upload", int64(len(data)), n)
	if err != nil {
		return nil, "a sample cannot be shown for this file, but it can still be imported"
	}
	return res.Rows, res.Note
}
