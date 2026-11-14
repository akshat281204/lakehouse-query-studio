package parquet

import (
	"context"
	"fmt"
)

// maxReadRows bounds ReadRows so a malformed or hostile footer cannot request
// an unbounded allocation.
const maxReadRows = 1_000_000

// RowsResult is a window of rows read from a flat, single-row-group file.
type RowsResult struct {
	Columns []Column
	Rows    []map[string]any
	NumRows int64
}

// ReadRows decodes rows [offset, offset+limit) of a flat Parquet file written
// by this package's Encode (single row group, one data page per column). It
// reads every needed column page in full, so callers must have bounded the
// file size and row count (the query spool does). Rows come back as JSON-safe
// maps in column order of the returned Columns.
func ReadRows(ctx context.Context, r ObjectReader, key string, size int64, offset, limit int) (*RowsResult, error) {
	if offset < 0 || limit < 0 {
		return nil, fmt.Errorf("%w: negative window", ErrMalformed)
	}
	meta, err := readFileMetaData(ctx, r, key, size)
	if err != nil {
		return nil, err
	}
	cols := buildColumns(meta.schema)
	leaves, flat := flatLeaves(meta.schema)
	out := &RowsResult{Columns: cols, NumRows: meta.numRows}
	if !flat {
		return nil, fmt.Errorf("%w: nested schema", ErrMalformed)
	}
	if meta.numRows > maxReadRows {
		return nil, fmt.Errorf("%w: too many rows", ErrMalformed)
	}
	if len(meta.rowGroups) == 0 || len(leaves) == 0 || offset >= int(meta.numRows) {
		return out, nil
	}
	if len(meta.rowGroups) != 1 {
		return nil, fmt.Errorf("%w: expected one row group", ErrMalformed)
	}
	rg := meta.rowGroups[0]
	if len(rg.columns) != len(leaves) {
		return nil, fmt.Errorf("%w: column count mismatch", ErrMalformed)
	}
	end := offset + limit
	if end > int(rg.numRows) {
		end = int(rg.numRows)
	}
	columns := make([][]any, len(leaves))
	for i := range leaves {
		col, err := readColumnRows(ctx, r, key, rg.columns[i], leaves[i], int(rg.numRows))
		if err != nil {
			return nil, err
		}
		if len(col) < end {
			return nil, fmt.Errorf("%w: short column", ErrMalformed)
		}
		columns[i] = col[offset:end]
	}
	out.Rows = buildRows(cols, columns, end-offset)
	return out, nil
}
