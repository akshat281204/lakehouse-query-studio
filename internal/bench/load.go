package bench

import (
	"context"
	"fmt"

	"github.com/akshat281204/lakehouse-query-studio/internal/catalog"
	"github.com/akshat281204/lakehouse-query-studio/internal/objectstore"
	"github.com/akshat281204/lakehouse-query-studio/internal/parquet"
)

const chunkRows = 200_000 // rows per Parquet part file

// Table is one generated table (or layout) in the object store.
type Table struct {
	Name   string `json:"name"`
	Prefix string `json:"prefix"`
	Rows   int64  `json:"rows"`
	Files  int    `json:"files"`
	Bytes  int64  `json:"bytes"`
	Hive   bool   `json:"hive_partitioned"`
}

// Dataset is the full generated workload.
type Dataset struct {
	Scale  float64 `json:"scale"`
	Tables []Table `json:"tables"`
	Truth  Truth   `json:"-"`
}

func (d *Dataset) table(name string) *Table {
	for i := range d.Tables {
		if d.Tables[i].Name == name {
			return &d.Tables[i]
		}
	}
	return nil
}

// Relations returns the catalog relations for the generated tables.
func (d *Dataset) Relations() []catalog.Relation {
	var rels []catalog.Relation
	for _, t := range d.Tables {
		rels = append(rels, catalog.Relation{DatasetName: t.Name, Version: 1, ObjectPrefix: t.Prefix,
			Format: "parquet", HivePartitioned: t.Hive})
	}
	return rels
}

func put(ctx context.Context, store objectstore.ReadWriter, t *Table, key string, cols []parquet.WriteColumn, rows [][]any) error {
	data, err := parquet.Encode(cols, rows)
	if err != nil {
		return err
	}
	if err := store.Put(ctx, key, data); err != nil {
		return err
	}
	t.Files++
	t.Bytes += int64(len(data))
	t.Rows += int64(len(rows))
	return nil
}

// Load generates the workload at scale into store: customer, orders, and
// lineitem in two physical layouts holding identical rows — "lineitem" (flat
// files) and "lineitem_part" (hive-partitioned by l_year).
func Load(ctx context.Context, store objectstore.ReadWriter, scale float64) (*Dataset, error) {
	sz := SizesFor(scale)
	g := NewGenerator(sz)
	ds := &Dataset{Scale: scale}
	cust := &Table{Name: "customer", Prefix: "bench/customer"}
	ord := &Table{Name: "orders", Prefix: "bench/orders"}
	flat := &Table{Name: "lineitem", Prefix: "bench/lineitem"}
	part := &Table{Name: "lineitem_part", Prefix: "bench/lineitem_part", Hive: true}

	if err := put(ctx, store, cust, cust.Prefix+"/part-0.parquet", CustomerCols, g.Customers()); err != nil {
		return nil, err
	}
	ordersPerChunk := chunkRows / 4
	for from, i := 0, 0; from < sz.Orders; from, i = from+chunkRows, i+1 {
		to := min(from+chunkRows, sz.Orders)
		if err := put(ctx, store, ord, fmt.Sprintf("%s/part-%d.parquet", ord.Prefix, i), OrdersCols, g.Orders(from, to)); err != nil {
			return nil, err
		}
	}
	for from, i := 0, 0; from < sz.Orders; from, i = from+ordersPerChunk, i+1 {
		to := min(from+ordersPerChunk, sz.Orders)
		rows, years := g.Lineitems(from, to)
		if err := put(ctx, store, flat, fmt.Sprintf("%s/part-%d.parquet", flat.Prefix, i), LineitemCols, rows); err != nil {
			return nil, err
		}
		byYear := map[int][][]any{}
		for r, row := range rows {
			byYear[years[r]] = append(byYear[years[r]], row[:len(row)-1])
		}
		for y := FirstYear; y <= LastYear; y++ {
			if len(byYear[y]) == 0 {
				continue
			}
			key := fmt.Sprintf("%s/l_year=%d/part-%d.parquet", part.Prefix, y, i)
			if err := put(ctx, store, part, key, LineitemColsNoYear(), byYear[y]); err != nil {
				return nil, err
			}
		}
	}
	ds.Tables = []Table{*cust, *ord, *flat, *part}
	ds.Truth = g.Truth
	return ds, nil
}
