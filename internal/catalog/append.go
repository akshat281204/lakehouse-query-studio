package catalog

import (
	"fmt"
	"strings"
)

// IncomingColumn describes a column of data about to be appended.
type IncomingColumn struct {
	Name     string
	Type     string
	Nullable bool
	// AllNull marks a column with no values at all in the new data (an optional
	// CSV column left empty in this batch). Its inferred type means nothing, so
	// it is compatible with any nullable column of the dataset.
	AllNull bool
}

// CheckAppendSchema compares incoming data with the dataset's current schema
// and returns every reason it cannot be appended (nil when it can). The rules
// are strict on purpose: the dataset's files are read together, so a silent
// type change would corrupt results.
//
//   - every incoming column must exist in the dataset, with exactly the same
//     name (case-sensitive) and type;
//   - a dataset column missing from the incoming data is allowed only if it is
//     nullable (it is filled with NULL);
//   - widenInts additionally lets an incoming int64 column feed a double column
//     (only for CSV/JSON, whose values are converted; a Parquet file is stored
//     as it is, so its types must match exactly).
//
// Messages name columns and types only, never values.
func CheckAppendSchema(base []SchemaColumn, in []IncomingColumn, widenInts bool) []string {
	var issues []string
	byName := make(map[string]IncomingColumn, len(in))
	for _, c := range in {
		byName[c.Name] = c
	}
	known := make(map[string]bool, len(base))
	for _, b := range base {
		known[b.Name] = true
		c, ok := byName[b.Name]
		switch {
		case ok && c.AllNull && !b.Nullable:
			issues = append(issues, fmt.Sprintf("column %q has no values in the new data but is required (not nullable)", b.Name))
		case ok && c.AllNull:
			// no values: any type will do, the column is filled with NULL
		case !ok && !b.Nullable:
			issues = append(issues, fmt.Sprintf("column %q is required (not nullable) but is missing from the new data", b.Name))
		case !ok:
			// nullable and absent: filled with NULL
		case c.Type == b.Type:
		case widenInts && b.Type == "double" && c.Type == "int64":
		default:
			issues = append(issues, fmt.Sprintf("column %q is %s in the new data but %s in the dataset", b.Name, c.Type, b.Type))
		}
	}
	for _, c := range in {
		if known[c.Name] {
			continue
		}
		hint := ""
		for _, b := range base {
			if strings.EqualFold(b.Name, c.Name) {
				hint = fmt.Sprintf(" (the dataset has %q; names are case-sensitive)", b.Name)
				break
			}
		}
		issues = append(issues, fmt.Sprintf("column %q is not in the dataset%s; adding columns is not supported yet", c.Name, hint))
	}
	return issues
}

// MergeAppendSchema returns the schema of the appended version: the dataset's
// columns in their order and types, nullable if the dataset column was, the new
// data's column is, or the new data lacks the column.
func MergeAppendSchema(base []SchemaColumn, in []IncomingColumn) []SchemaColumn {
	byName := make(map[string]IncomingColumn, len(in))
	for _, c := range in {
		byName[c.Name] = c
	}
	out := make([]SchemaColumn, len(base))
	for i, b := range base {
		c, present := byName[b.Name]
		out[i] = SchemaColumn{Name: b.Name, Type: b.Type, IsPartition: b.IsPartition,
			Nullable: b.Nullable || !present || c.Nullable || c.AllNull}
	}
	return out
}
