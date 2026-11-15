package catalog

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

var baseCols = []SchemaColumn{
	{Name: "id", Type: "int64"},
	{Name: "amount", Type: "double"},
	{Name: "note", Type: "string", Nullable: true},
}

func TestCheckAppendSchemaRules(t *testing.T) {
	ok := []IncomingColumn{{Name: "id", Type: "int64"}, {Name: "amount", Type: "double"}, {Name: "note", Type: "string", Nullable: true}}
	if issues := CheckAppendSchema(baseCols, ok, false); issues != nil {
		t.Fatalf("identical schema: %v", issues)
	}
	// order is irrelevant, a nullable column may be absent
	if issues := CheckAppendSchema(baseCols, []IncomingColumn{{Name: "amount", Type: "double"}, {Name: "id", Type: "int64"}}, false); issues != nil {
		t.Fatalf("reordered + missing nullable: %v", issues)
	}

	cases := map[string]struct {
		in    []IncomingColumn
		widen bool
		want  string
	}{
		"type change":      {[]IncomingColumn{{Name: "id", Type: "string"}, {Name: "amount", Type: "double"}}, false, `column "id" is string in the new data but int64`},
		"missing required": {[]IncomingColumn{{Name: "id", Type: "int64"}}, false, `column "amount" is required`},
		"extra column":     {[]IncomingColumn{{Name: "id", Type: "int64"}, {Name: "amount", Type: "double"}, {Name: "region", Type: "string"}}, false, `column "region" is not in the dataset`},
		"case differs":     {[]IncomingColumn{{Name: "ID", Type: "int64"}, {Name: "amount", Type: "double"}}, false, `the dataset has "id"`},
		"int into double":  {[]IncomingColumn{{Name: "id", Type: "int64"}, {Name: "amount", Type: "int64"}}, false, `column "amount" is int64`},
		"double into int":  {[]IncomingColumn{{Name: "id", Type: "double"}, {Name: "amount", Type: "double"}}, true, `column "id" is double`},
		"parquet int32 id": {[]IncomingColumn{{Name: "id", Type: "int32"}, {Name: "amount", Type: "double"}}, true, `column "id" is int32`},
	}
	for name, c := range cases {
		issues := CheckAppendSchema(baseCols, c.in, c.widen)
		if len(issues) == 0 || !strings.Contains(strings.Join(issues, "|"), c.want) {
			t.Errorf("%s: want issue containing %q, got %v", name, c.want, issues)
		}
	}
	// int64 -> double widening is allowed only when asked for
	widened := []IncomingColumn{{Name: "id", Type: "int64"}, {Name: "amount", Type: "int64"}}
	if issues := CheckAppendSchema(baseCols, widened, true); issues != nil {
		t.Fatalf("widening: %v", issues)
	}
	// messages never echo values: they only mention column names and types
	for _, i := range CheckAppendSchema(baseCols, []IncomingColumn{{Name: "id", Type: "string"}}, false) {
		if strings.ContainsAny(i, "0123456789") && !strings.Contains(i, "int64") {
			t.Errorf("unexpected digits in %q", i)
		}
	}
}

func TestMergeAppendSchemaKeepsBaseTypesAndWidensNullability(t *testing.T) {
	base := []SchemaColumn{{Name: "id", Type: "int64"}, {Name: "amount", Type: "double", IsPartition: true}, {Name: "note", Type: "string"}}
	got := MergeAppendSchema(base, []IncomingColumn{{Name: "amount", Type: "double", Nullable: true}, {Name: "id", Type: "int64"}})
	want := []SchemaColumn{
		{Name: "id", Type: "int64"},
		{Name: "amount", Type: "double", Nullable: true, IsPartition: true}, // the new data has nulls
		{Name: "note", Type: "string", Nullable: true},                      // absent from the new data
	}
	if len(got) != 3 {
		t.Fatalf("%+v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("column %d: got %+v want %+v", i, got[i], want[i])
		}
	}
}

// ---- memory store: the append lifecycle ----------------------------------

func publish(t *testing.T, m *MemoryIngestStore, req CreateConvert, p Publish) Job {
	t.Helper()
	ctx := context.Background()
	if _, _, err := m.CreateConvert(ctx, req); err != nil {
		t.Fatal(err)
	}
	job, err := m.Claim(ctx, "w", 0)
	if err != nil || job == nil {
		t.Fatalf("claim: %v %v", job, err)
	}
	if err := m.Complete(ctx, job.ID, job.FencingToken, p); err != nil {
		t.Fatalf("complete: %v", err)
	}
	got, _ := m.GetJob(ctx, job.ID)
	return got
}

func TestAppendBuildsAVersionFromSegmentsAndSumsRows(t *testing.T) {
	m := NewMemoryIngestStore()
	ctx := context.Background()
	publish(t, m, CreateConvert{JobID: "j1", Name: "sales", InputFormat: "csv"},
		Publish{ObjectPrefix: "lake/default/sales/version=1/attempt=1/", RowCount: 10, Columns: baseCols})
	j2 := publish(t, m, CreateConvert{JobID: "j2", Name: "sales", InputFormat: "csv", Mode: ModeAppend},
		Publish{ObjectPrefix: "lake/default/sales/version=2/attempt=1/", RowCount: 5, Columns: baseCols})
	if j2.Mode != ModeAppend || j2.BaseVersionID == "" {
		t.Fatalf("job: %+v", j2)
	}
	j3 := publish(t, m, CreateConvert{JobID: "j3", Name: "sales", InputFormat: "parquet", Mode: ModeAppend},
		Publish{ObjectPrefix: "lake/default/sales/version=3/attempt=1/", RowCount: 7, Columns: baseCols})

	vs := m.ReadyVersions("sales")
	if len(vs) != 3 {
		t.Fatalf("versions: %d", len(vs))
	}
	v1, v2, v3 := vs[0], vs[1], vs[2]
	if len(v1.BaseSegments) != 0 || *v1.RowCount != 10 {
		t.Fatalf("v1: %+v", v1)
	}
	if got := v2.Prefixes(); len(got) != 2 || got[0] != v1.ObjectPrefix || got[1] != v2.ObjectPrefix || *v2.RowCount != 15 {
		t.Fatalf("v2: %v rows=%d", got, *v2.RowCount)
	}
	if got := v3.Prefixes(); len(got) != 3 || got[0] != v1.ObjectPrefix || got[1] != v2.ObjectPrefix || *v3.RowCount != 22 {
		t.Fatalf("v3: %v rows=%d", got, *v3.RowCount)
	}
	// An earlier version is untouched by later appends.
	if len(v1.BaseSegments) != 0 || len(v2.BaseSegments) != 1 {
		t.Fatal("older versions were modified")
	}
	_ = j3
	_ = ctx
}

func TestAppendNeedsAnExistingDatasetWithAReadyVersion(t *testing.T) {
	m := NewMemoryIngestStore()
	ctx := context.Background()
	if _, _, err := m.CreateConvert(ctx, CreateConvert{JobID: "a", Name: "nope", InputFormat: "csv", Mode: ModeAppend}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("append to a missing dataset must not create it: %v", err)
	}
	if _, ok := m.datasets["nope"]; ok {
		t.Fatal("the dataset was created by a failed append")
	}
	// A dataset whose only version is not READY yet cannot be appended to.
	if _, _, err := m.CreateConvert(ctx, CreateConvert{JobID: "b", Name: "fresh", InputFormat: "csv"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.CreateConvert(ctx, CreateConvert{JobID: "c", Name: "fresh", InputFormat: "csv", Mode: ModeAppend}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("append before any READY version: %v", err)
	}
}

// Two appends based on the same version: the second to finish must not publish,
// or it would silently drop the first one's rows.
func TestConcurrentAppendsCannotLoseAnUpdate(t *testing.T) {
	m := NewMemoryIngestStore()
	ctx := context.Background()
	publish(t, m, CreateConvert{JobID: "j1", Name: "sales", InputFormat: "csv"},
		Publish{ObjectPrefix: "lake/default/sales/version=1/attempt=1/", RowCount: 10, Columns: baseCols})
	for _, id := range []string{"a", "b"} {
		if _, _, err := m.CreateConvert(ctx, CreateConvert{JobID: id, Name: "sales", InputFormat: "csv", Mode: ModeAppend}); err != nil {
			t.Fatal(err)
		}
	}
	ja, _ := m.Claim(ctx, "w", time.Hour)
	jb, _ := m.Claim(ctx, "w", time.Hour)
	if ja.BaseVersionID != jb.BaseVersionID {
		t.Fatal("both appends should share the same base")
	}
	if err := m.Complete(ctx, ja.ID, ja.FencingToken, Publish{ObjectPrefix: "lake/default/sales/version=2/attempt=1/", RowCount: 3, Columns: baseCols}); err != nil {
		t.Fatal(err)
	}
	if err := m.Complete(ctx, jb.ID, jb.FencingToken, Publish{ObjectPrefix: "lake/default/sales/version=3/attempt=1/", RowCount: 4, Columns: baseCols}); !errors.Is(err, ErrBaseChanged) {
		t.Fatalf("the stale append must be refused, got %v", err)
	}
	if got := m.ReadyVersions("sales"); len(got) != 2 || *got[1].RowCount != 13 {
		t.Fatalf("versions after the race: %+v", got)
	}
	// The refused job can fail retryably and a retry appends to the new latest.
	if err := m.Fail(ctx, jb.ID, jb.FencingToken, "base_changed", "the dataset changed", true); err != nil {
		t.Fatal(err)
	}
	retry, err := m.Retry(ctx, jb.ID, "b2")
	if err != nil || retry.Mode != ModeAppend {
		t.Fatalf("retry: %+v %v", retry, err)
	}
	if retry.BaseVersionID == jb.BaseVersionID {
		t.Fatal("the retry must be based on the new latest version, not the stale one")
	}
}

func TestAppendKeepsThePartitionLayoutAndHonoursTheSegmentCap(t *testing.T) {
	m := NewMemoryIngestStore()
	ctx := context.Background()
	publish(t, m, CreateConvert{JobID: "j1", Name: "p", InputFormat: "csv", PartitionColumns: []string{"region"}},
		Publish{ObjectPrefix: "lake/default/p/version=1/attempt=1/", PartitionColumns: []string{"region"}, RowCount: 1, Columns: baseCols})
	// the caller's partition columns are ignored for an append: the dataset's win
	j, _, err := m.CreateConvert(ctx, CreateConvert{JobID: "j2", Name: "p", InputFormat: "csv", Mode: ModeAppend, PartitionColumns: []string{"other"}})
	if err != nil || len(j.PartitionColumns) != 1 || j.PartitionColumns[0] != "region" {
		t.Fatalf("partition columns: %+v %v", j, err)
	}

	// A version already at the segment cap refuses further appends.
	m2 := NewMemoryIngestStore()
	publish(t, m2, CreateConvert{JobID: "x", Name: "big", InputFormat: "csv"}, Publish{ObjectPrefix: "lake/default/big/version=1/attempt=1/", RowCount: 1, Columns: baseCols})
	for _, v := range m2.versions {
		v.BaseSegments = make([]string, MaxAppendSegments-1)
	}
	if _, _, err := m2.CreateConvert(ctx, CreateConvert{JobID: "y", Name: "big", InputFormat: "csv", Mode: ModeAppend}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("segment cap: %v", err)
	}
}

func TestAppendToAHivePartitionedRegisteredDatasetIsRefused(t *testing.T) {
	v := DatasetVersion{ObjectPrefix: "data/events/", PartitionColumns: []string{"year"}}
	if err := CheckAppendable("events", v); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("hive-partitioned registered base: %v", err)
	}
	if err := CheckAppendable("events", DatasetVersion{ObjectPrefix: "data/events/"}); err != nil {
		t.Fatalf("an unpartitioned registered prefix may be appended to: %v", err)
	}
	if err := CheckAppendable("events", DatasetVersion{ObjectPrefix: "lake/default/e/version=1/attempt=1/", PartitionColumns: []string{"year"}}); err != nil {
		t.Fatalf("a partitioned dataset this tool wrote may be appended to: %v", err)
	}
}

func TestAnAllEmptyColumnFitsAnyNullableColumnButNotARequiredOne(t *testing.T) {
	in := []IncomingColumn{{Name: "id", Type: "int64"}, {Name: "amount", Type: "double"}, {Name: "note", Type: "int64", Nullable: true, AllNull: true}}
	if issues := CheckAppendSchema(baseCols, in, true); issues != nil {
		t.Fatalf("an empty optional column must fit: %v", issues)
	}
	empty := []IncomingColumn{{Name: "id", Type: "string", Nullable: true, AllNull: true}, {Name: "amount", Type: "double"}}
	issues := CheckAppendSchema(baseCols, empty, true)
	if len(issues) != 1 || !strings.Contains(issues[0], `column "id" has no values`) {
		t.Fatalf("an empty required column: %v", issues)
	}
	merged := MergeAppendSchema(baseCols, in)
	if merged[2].Type != "string" || !merged[2].Nullable {
		t.Fatalf("merged: %+v", merged[2])
	}
}
