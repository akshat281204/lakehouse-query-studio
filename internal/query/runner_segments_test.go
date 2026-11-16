package query

import (
	"context"
	"fmt"
	"testing"

	"github.com/akshat281204/lakehouse-query-studio/internal/catalog"
	"github.com/akshat281204/lakehouse-query-studio/internal/parquet"
)

var segCols = []parquet.WriteColumn{
	{Name: "id", Type: parquet.TypeInt64},
	{Name: "label", Type: parquet.TypeString, Nullable: true},
	{Name: "score", Type: parquet.TypeDouble},
}

// segmentedHarness is the standard harness whose "sales" relation is rebuilt as
// an appended version: the original file plus two more segments, all files named
// part-1.parquet so a collision between segments would lose data.
func segmentedHarness(t *testing.T, mutate func(*RunnerConfig)) *harness {
	t.Helper()
	h := newHarness(t, nil, mutate)
	put := func(prefix string, ids ...int64) {
		var rows [][]any
		for _, id := range ids {
			rows = append(rows, []any{id, fmt.Sprintf("r%d", id), float64(id) + 0.5})
		}
		data, err := parquet.Encode(segCols, rows)
		if err != nil {
			t.Fatal(err)
		}
		_ = h.objects.Put(context.Background(), prefix+"part-1.parquet", data)
	}
	base := h.rel.ObjectPrefix
	seg2, seg3 := "lake/default/sales/version=2/attempt=1/", "lake/default/sales/version=3/attempt=1/"
	put(seg2, 100, 101)
	put(seg3, 200, 201, 202)
	h.store.AddRelation(catalog.Relation{
		DatasetName: "sales", Version: 3, ObjectPrefix: seg3,
		BaseSegments: []string{base, seg2}, Format: "parquet",
	})
	rel, err := h.store.ResolveRelation(context.Background(), "sales")
	if err != nil {
		t.Fatal(err)
	}
	h.rel = rel
	return h
}

func TestARelationOfSeveralSegmentsReadsTheirUnion(t *testing.T) {
	h := segmentedHarness(t, nil)
	if got := h.rel.Prefixes(); len(got) != 3 {
		t.Fatalf("prefixes: %v", got)
	}
	out := h.runCapture(t, `SELECT id FROM sales ORDER BY id`)
	// original 1,2,3 + 100,101 + 200,201,202, though every file has the same name
	if len(out.rows) != 8 {
		t.Fatalf("rows: %v", out.rows)
	}
	if out.tm.FilesStaged != 3 || out.tm.FilesListed != 3 {
		t.Fatalf("timings: %+v", out.tm)
	}
	agg := h.runCapture(t, `SELECT count(*) AS n, sum(id) AS s FROM sales`)
	if fmt.Sprint(agg.rows) != "[map[n:8 s:810]]" {
		t.Fatalf("aggregate: %v", agg.rows)
	}
}

func TestPruningSkipsWholeSegmentsOfAnAppendedVersion(t *testing.T) {
	on := segmentedHarness(t, nil)
	off := segmentedHarness(t, func(c *RunnerConfig) { c.DisablePruning = true })
	for _, sql := range []string{
		`SELECT id FROM sales WHERE id >= 200`,
		`SELECT id FROM sales WHERE id BETWEEN 100 AND 101`,
		`SELECT id FROM sales WHERE id = 2`,
		`SELECT id FROM sales WHERE id = 5000`,
	} {
		a, b := on.runCapture(t, sql), off.runCapture(t, sql)
		if fmt.Sprint(a.rows) != fmt.Sprint(b.rows) {
			t.Errorf("%s: pruned %v unpruned %v", sql, a.rows, b.rows)
		}
	}
	if out := on.runCapture(t, `SELECT id FROM sales WHERE id >= 200`); out.tm.FilesStaged != 1 || out.tm.FilesPruned != 2 {
		t.Fatalf("expected two of three segments pruned: %+v", out.tm)
	}
}

func TestAnOlderVersionStillReadsOnlyItsOwnSegments(t *testing.T) {
	h := segmentedHarness(t, nil)
	// version 1 of "sales" is the original single file, untouched by later appends
	h.store.AddRelation(catalog.Relation{DatasetName: "sales", Version: 1, ObjectPrefix: "lake/default/sales/version=1/attempt=1/", Format: "parquet"})
	v1, err := h.store.ResolveRelation(context.Background(), "sales_v1")
	if err != nil {
		t.Fatal(err)
	}
	if len(v1.Prefixes()) != 1 {
		t.Fatalf("v1 prefixes: %v", v1.Prefixes())
	}
}

func TestSegmentedRelationsWorkWithTheStageCache(t *testing.T) {
	h := segmentedHarness(t, func(c *RunnerConfig) { c.StageCacheBytes = 8 << 20 })
	first := h.runCapture(t, `SELECT id FROM sales ORDER BY id`)
	second := h.runCapture(t, `SELECT id FROM sales ORDER BY id`)
	if second.tm.CacheHits != 3 || fmt.Sprint(first.rows) != fmt.Sprint(second.rows) {
		t.Fatalf("cache hits %d, rows %v vs %v", second.tm.CacheHits, first.rows, second.rows)
	}
}
