package parquet

import (
	"context"
	"encoding/binary"
	"math"
)

// ColumnStats are the min/max/null statistics of one flat column, merged over
// all row groups of a file. Only types the query pruner can compare exactly are
// decoded: int32/int64 (as int64), double, string and boolean. A column of any
// other type, or one a row group lacks statistics for, has HasMinMax == false,
// which a pruner must treat as "cannot rule this file out".
type ColumnStats struct {
	Name string
	// Type is the display type from the schema: "int32", "int64", "double",
	// "string" or "boolean".
	Type         string
	NullCount    int64
	HasNullCount bool
	HasMinMax    bool

	MinInt, MaxInt     int64
	MinFloat, MaxFloat float64
	MinStr, MaxStr     string
	MinBool, MaxBool   bool
}

// FileStats are the footer statistics of one Parquet file.
type FileStats struct {
	NumRows int64
	Columns map[string]ColumnStats
}

// ReadStats reads only the footer of the object at key. It supports flat
// schemas; for a nested schema it returns the row count and no columns.
func ReadStats(ctx context.Context, r ObjectReader, key string, size int64) (*FileStats, error) {
	meta, err := readFileMetaData(ctx, r, key, size)
	if err != nil {
		return nil, err
	}
	return statsFromMeta(meta), nil
}

func statsFromMeta(meta *fileMetaData) *FileStats {
	out := &FileStats{NumRows: meta.numRows, Columns: map[string]ColumnStats{}}
	if len(meta.schema) == 0 {
		return out
	}
	byName := map[string]schemaElement{}
	for _, e := range meta.schema[1:] {
		if e.numChildren != nil { // nested: leave the file unpruned
			return out
		}
		byName[e.name] = e
	}

	type agg struct {
		cs     ColumnStats
		ok     bool // every non-empty row group supplied min/max
		nulls  bool // every non-empty row group supplied a null count
		seeded bool
	}
	cols := map[string]*agg{}
	for _, g := range meta.rowGroups {
		if g.numRows == 0 {
			continue
		}
		for _, c := range g.columns {
			if len(c.meta.path) != 1 {
				continue
			}
			name := c.meta.path[0]
			elem, known := byName[name]
			if !known {
				continue
			}
			typ := typeString(elem)
			a := cols[name]
			if a == nil {
				a = &agg{cs: ColumnStats{Name: name, Type: typ}, ok: true, nulls: true}
				cols[name] = a
			}
			if c.meta.stats.hasNullCount {
				a.cs.NullCount += c.meta.stats.nullCount
			} else {
				a.nulls = false
			}
			rgMin, rgMax, decoded := decodeMinMax(typ, c.meta.stats)
			if !decoded {
				a.ok = false
				continue
			}
			if !a.seeded {
				a.cs.MinInt, a.cs.MaxInt = rgMin.i, rgMax.i
				a.cs.MinFloat, a.cs.MaxFloat = rgMin.f, rgMax.f
				a.cs.MinStr, a.cs.MaxStr = rgMin.s, rgMax.s
				a.cs.MinBool, a.cs.MaxBool = rgMin.b, rgMax.b
				a.seeded = true
				continue
			}
			a.cs.MinInt, a.cs.MaxInt = min(a.cs.MinInt, rgMin.i), max(a.cs.MaxInt, rgMax.i)
			a.cs.MinFloat, a.cs.MaxFloat = min(a.cs.MinFloat, rgMin.f), max(a.cs.MaxFloat, rgMax.f)
			a.cs.MinStr, a.cs.MaxStr = min(a.cs.MinStr, rgMin.s), max(a.cs.MaxStr, rgMax.s)
			a.cs.MinBool = a.cs.MinBool && rgMin.b
			a.cs.MaxBool = a.cs.MaxBool || rgMax.b
		}
	}
	for name, a := range cols {
		a.cs.HasNullCount = a.nulls
		a.cs.HasMinMax = a.ok && a.seeded
		out.Columns[name] = a.cs
	}
	return out
}

type decoded struct {
	i int64
	f float64
	s string
	b bool
}

// decodeMinMax turns the raw PLAIN-encoded bytes of one chunk's min/max into
// typed values. It reports false for an unsupported type or malformed bytes.
func decodeMinMax(typ string, st chunkStats) (lo, hi decoded, ok bool) {
	if !st.hasMinMax {
		return
	}
	switch typ {
	case "int32":
		if len(st.min) != 4 || len(st.max) != 4 {
			return
		}
		lo.i = int64(int32(binary.LittleEndian.Uint32(st.min)))
		hi.i = int64(int32(binary.LittleEndian.Uint32(st.max)))
	case "int64":
		if len(st.min) != 8 || len(st.max) != 8 {
			return
		}
		lo.i = int64(binary.LittleEndian.Uint64(st.min))
		hi.i = int64(binary.LittleEndian.Uint64(st.max))
	case "double":
		if len(st.min) != 8 || len(st.max) != 8 {
			return
		}
		lo.f = math.Float64frombits(binary.LittleEndian.Uint64(st.min))
		hi.f = math.Float64frombits(binary.LittleEndian.Uint64(st.max))
		if math.IsNaN(lo.f) || math.IsNaN(hi.f) {
			return
		}
	case "string":
		lo.s, hi.s = string(st.min), string(st.max)
	case "boolean":
		if len(st.min) != 1 || len(st.max) != 1 {
			return
		}
		lo.b, hi.b = st.min[0] != 0, st.max[0] != 0
	default:
		return
	}
	return lo, hi, true
}

// MergeStats combines the statistics of several files of one dataset. A column
// keeps min/max (null count) only if every file supplies it; a column missing
// from any file, or typed differently across files, is reported without
// min/max so a reader never mistakes partial knowledge for a range.
func MergeStats(files []*FileStats) *FileStats {
	out := &FileStats{Columns: map[string]ColumnStats{}}
	n := 0
	for _, f := range files {
		if f == nil {
			continue
		}
		n++
		out.NumRows += f.NumRows
	}
	if n == 0 {
		return out
	}
	seen := map[string]int{}
	for _, f := range files {
		if f == nil {
			continue
		}
		for name, c := range f.Columns {
			cur, ok := out.Columns[name]
			seen[name]++
			if !ok {
				out.Columns[name] = c
				continue
			}
			if cur.Type != c.Type {
				cur.HasMinMax, cur.HasNullCount = false, false
				out.Columns[name] = cur
				continue
			}
			cur.HasMinMax = cur.HasMinMax && c.HasMinMax
			cur.HasNullCount = cur.HasNullCount && c.HasNullCount
			if cur.HasMinMax {
				cur.MinInt, cur.MaxInt = min(cur.MinInt, c.MinInt), max(cur.MaxInt, c.MaxInt)
				cur.MinFloat, cur.MaxFloat = min(cur.MinFloat, c.MinFloat), max(cur.MaxFloat, c.MaxFloat)
				cur.MinStr, cur.MaxStr = min(cur.MinStr, c.MinStr), max(cur.MaxStr, c.MaxStr)
				cur.MinBool, cur.MaxBool = cur.MinBool && c.MinBool, cur.MaxBool || c.MaxBool
			}
			cur.NullCount += c.NullCount
			out.Columns[name] = cur
		}
	}
	for name, c := range out.Columns {
		if seen[name] != n { // absent from some file: nothing is known about the whole
			c.HasMinMax, c.HasNullCount = false, false
			out.Columns[name] = c
		}
	}
	return out
}
