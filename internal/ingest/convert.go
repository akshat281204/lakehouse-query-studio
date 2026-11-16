package ingest

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/akshat281204/lakehouse-query-studio/internal/catalog"
	"github.com/akshat281204/lakehouse-query-studio/internal/parquet"
)

// maxPartitionColumns bounds directory nesting depth.
const maxPartitionColumns = 4

// Part is one converted Parquet file, addressed relative to the attempt prefix.
type Part struct {
	RelPath string
	Data    []byte
	Rows    int
}

// nullPartitionToken encodes a null partition value (docs/design.md §4.1).
const nullPartitionToken = "%00"

// EncodePartitionComponent percent-encodes s, leaving only the unreserved set
// [A-Za-z0-9._~-] unescaped, exactly as docs/design.md §4.1 specifies.
func EncodePartitionComponent(s string) string {
	const hex = "0123456789ABCDEF"
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '.', c == '_', c == '~', c == '-':
			b.WriteByte(c)
		default:
			b.WriteByte('%')
			b.WriteByte(hex[c>>4])
			b.WriteByte(hex[c&0x0f])
		}
	}
	return b.String()
}

// ValidatePartitionColumns checks the requested partition columns against the
// inferred schema. The default (none) is an unpartitioned dataset, which avoids
// the small-file explosion of partitioning by accident.
func ValidatePartitionColumns(cols []parquet.WriteColumn, partitions []string) error {
	if len(partitions) > maxPartitionColumns {
		return userErrf("at most %d partition columns are supported", maxPartitionColumns)
	}
	if len(partitions) >= len(cols) && len(partitions) > 0 {
		return userErrf("at least one non-partition column is required")
	}
	byName := make(map[string]parquet.WriteColumn, len(cols))
	for _, c := range cols {
		byName[c.Name] = c
	}
	seen := map[string]bool{}
	for _, p := range partitions {
		c, ok := byName[p]
		if !ok {
			return userErrf("partition column %q is not in the file", p)
		}
		if seen[p] {
			return userErrf("partition column %q is listed twice", p)
		}
		seen[p] = true
		if c.Type == parquet.TypeDouble {
			return userErrf("partition column %q is a floating-point column; partition by an integer, boolean, or string column", p)
		}
	}
	return nil
}

func partitionText(v any) string {
	switch x := v.(type) {
	case nil:
		return nullPartitionToken
	case string:
		return EncodePartitionComponent(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case bool:
		return strconv.FormatBool(x)
	default:
		return EncodePartitionComponent(fmt.Sprint(x))
	}
}

// Convert encodes the table as Parquet parts of at most lim.PartRows rows,
// grouped into Hive-style directories when partition columns are given. Part
// file names are random so concurrent attempts never collide.
func Convert(t *Table, partitions []string, lim Limits) ([]Part, error) {
	if err := ValidatePartitionColumns(t.Columns, partitions); err != nil {
		return nil, err
	}
	idx := make([]int, len(partitions))
	for i, p := range partitions {
		for j, c := range t.Columns {
			if c.Name == p {
				idx[i] = j
			}
		}
	}

	type group struct {
		dir  string
		rows [][]any
	}
	var order []string
	groups := map[string]*group{}
	for _, row := range t.Rows {
		dir := ""
		if len(idx) > 0 {
			var segs []string
			for i, j := range idx {
				segs = append(segs, EncodePartitionComponent(partitions[i])+"="+partitionText(row[j]))
			}
			dir = strings.Join(segs, "/") + "/"
		}
		g, ok := groups[dir]
		if !ok {
			if len(groups) >= lim.MaxPartitions {
				return nil, userErrf("more than %d distinct partition values; choose a lower-cardinality partition column", lim.MaxPartitions)
			}
			g = &group{dir: dir}
			groups[dir] = g
			order = append(order, dir)
		}
		g.rows = append(g.rows, row)
	}

	var parts []Part
	for _, dir := range order {
		g := groups[dir]
		for start := 0; start < len(g.rows); start += lim.PartRows {
			end := min(start+lim.PartRows, len(g.rows))
			data, err := parquet.Encode(t.Columns, g.rows[start:end])
			if err != nil {
				return nil, fmt.Errorf("encode parquet: %w", err)
			}
			id, err := catalog.NewUUID()
			if err != nil {
				return nil, err
			}
			parts = append(parts, Part{RelPath: dir + "part-" + id + ".parquet", Data: data, Rows: end - start})
		}
	}
	return parts, nil
}
