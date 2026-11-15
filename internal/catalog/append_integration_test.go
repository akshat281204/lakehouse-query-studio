//go:build integration

package catalog

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

var itCols = []SchemaColumn{{Name: "id", Type: "int64"}, {Name: "region", Type: "string"}}

func itName(prefix string) string { return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano()) }

func itPublish(t *testing.T, s IngestStore, name, mode, prefix string, rows int64, parts []string) (Job, error) {
	t.Helper()
	ctx := context.Background()
	id, _ := NewUUID()
	if _, _, err := s.CreateConvert(ctx, CreateConvert{JobID: id, Name: name, InputRef: "x", InputFormat: "csv", Mode: mode, PartitionColumns: parts}); err != nil {
		return Job{}, err
	}
	j := claimJob(t, s, id)
	if err := s.Complete(ctx, j.ID, j.FencingToken, Publish{ObjectPrefix: prefix, PartitionColumns: parts, RowCount: rows, Columns: itCols}); err != nil {
		return *j, err
	}
	return *j, nil
}

// claimJob claims jobs until it gets the one with the given id. Jobs queued by
// other tests sharing the database are failed out of the way, not run.
func claimJob(t *testing.T, s IngestStore, id string) *Job {
	t.Helper()
	for i := 0; i < 50; i++ {
		j, err := s.Claim(context.Background(), "it", time.Hour)
		if err != nil || j == nil {
			t.Fatalf("claim: %v %v", j, err)
		}
		if j.ID == id {
			return j
		}
		_ = s.Fail(context.Background(), j.ID, j.FencingToken, "it_skip", "skipped by an unrelated test", false)
	}
	t.Fatal("job never became claimable")
	return nil
}

func TestPostgresAppendBuildsSegmentsAndResolvesThemForQueries(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	if err := Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	s := NewPostgresIngestStore(db)
	q := NewPostgresQueryStore(db)
	name := itName("app")

	if _, err := itPublish(t, s, name, ModeReplace, "lake/default/"+name+"/version=1/attempt=1/", 10, nil); err != nil {
		t.Fatal(err)
	}
	j2, err := itPublish(t, s, name, ModeAppend, "lake/default/"+name+"/version=2/attempt=1/", 5, nil)
	if err != nil || j2.Mode != ModeAppend || j2.BaseVersionID == "" {
		t.Fatalf("append: %+v %v", j2, err)
	}
	if _, err := itPublish(t, s, name, ModeAppend, "lake/default/"+name+"/version=3/attempt=1/", 7, nil); err != nil {
		t.Fatal(err)
	}

	latest, err := q.ResolveRelation(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"lake/default/" + name + "/version=1/attempt=1/", "lake/default/" + name + "/version=2/attempt=1/", "lake/default/" + name + "/version=3/attempt=1/"}
	if got := latest.Prefixes(); fmt.Sprint(got) != fmt.Sprint(want) || latest.Version != 3 {
		t.Fatalf("latest prefixes: %v (v%d)", got, latest.Version)
	}
	v1, err := q.ResolveRelation(ctx, name+"_v1")
	if err != nil || len(v1.Prefixes()) != 1 {
		t.Fatalf("v1 must be untouched by later appends: %v %v", v1.Prefixes(), err)
	}
	v2, _ := q.ResolveRelation(ctx, name+"_v2")
	if len(v2.Prefixes()) != 2 {
		t.Fatalf("v2: %v", v2.Prefixes())
	}
	var rows int64
	if err := db.QueryRow(`SELECT v.row_count FROM dataset_versions v JOIN datasets d ON d.id = v.dataset_id WHERE d.name = $1 AND v.version = 3`, name).Scan(&rows); err != nil || rows != 22 {
		t.Fatalf("row_count = %d (%v), want 10+5+7", rows, err)
	}
	// the dataset's versions also read back through the catalog store
	ver, cols, err := s.ReadySchema(ctx, name)
	if err != nil || ver.Version != 3 || len(ver.BaseSegments) != 2 || len(cols) != 2 {
		t.Fatalf("ReadySchema: %+v %v %v", ver, cols, err)
	}
}

// The publish of an append is fenced on its base: a second append made from the
// same base must be refused, undoing its job update, so it can be failed and retried.
func TestPostgresConcurrentAppendsCannotLoseAnUpdate(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	if err := Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	s := NewPostgresIngestStore(db)
	name := itName("race")
	if _, err := itPublish(t, s, name, ModeReplace, "lake/default/"+name+"/version=1/attempt=1/", 10, nil); err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, id := range []string{"a", "b"} {
		jid, _ := NewUUID()
		ids = append(ids, jid)
		if _, _, err := s.CreateConvert(ctx, CreateConvert{JobID: jid, Name: name, InputRef: "x" + id, InputFormat: "csv", Mode: ModeAppend}); err != nil {
			t.Fatal(err)
		}
	}
	ja := claimJob(t, s, ids[0])
	jb := claimJob(t, s, ids[1])
	if ja == nil || jb == nil || ja.BaseVersionID != jb.BaseVersionID {
		t.Fatalf("both appends must share a base: %+v %+v", ja, jb)
	}
	if err := s.Complete(ctx, ja.ID, ja.FencingToken, Publish{ObjectPrefix: "lake/default/" + name + "/version=2/attempt=1/", RowCount: 3, Columns: itCols}); err != nil {
		t.Fatal(err)
	}
	if err := s.Complete(ctx, jb.ID, jb.FencingToken, Publish{ObjectPrefix: "lake/default/" + name + "/version=3/attempt=1/", RowCount: 4, Columns: itCols}); !errors.Is(err, ErrBaseChanged) {
		t.Fatalf("stale append: %v", err)
	}
	// the refused Complete rolled back: the job is still RUNNING and can be failed retryably
	got, _ := s.GetJob(ctx, jb.ID)
	if got.Status != JobRunning {
		t.Fatalf("a refused publish must not leave the job %s", got.Status)
	}
	if err := s.Fail(ctx, jb.ID, jb.FencingToken, "base_changed", "changed", true); err != nil {
		t.Fatal(err)
	}
	retry, err := s.Retry(ctx, jb.ID, mustUUID(t))
	if err != nil || retry.Mode != ModeAppend || retry.BaseVersionID == jb.BaseVersionID {
		t.Fatalf("retry must re-base on the new latest version: %+v %v", retry, err)
	}
	var n int
	_ = db.QueryRow(`SELECT count(*) FROM dataset_versions v JOIN datasets d ON d.id = v.dataset_id WHERE d.name = $1 AND v.status = 'READY'`, name).Scan(&n)
	if n != 2 {
		t.Fatalf("READY versions = %d, want 2 (the original and the first append)", n)
	}
}

func TestPostgresAppendNeverCreatesADatasetAndNeedsAReadyVersion(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	if err := Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	s := NewPostgresIngestStore(db)
	name := itName("none")
	id, _ := NewUUID()
	if _, _, err := s.CreateConvert(ctx, CreateConvert{JobID: id, Name: name, InputRef: "x", InputFormat: "csv", Mode: ModeAppend}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("append to a missing dataset: %v", err)
	}
	var n int
	_ = db.QueryRow(`SELECT count(*) FROM datasets WHERE name = $1`, name).Scan(&n)
	if n != 0 {
		t.Fatal("a refused append created the dataset")
	}
	// a dataset whose only version is still queued has nothing to append to
	name2 := itName("fresh")
	id2, _ := NewUUID()
	if _, _, err := s.CreateConvert(ctx, CreateConvert{JobID: id2, Name: name2, InputRef: "x", InputFormat: "csv"}); err != nil {
		t.Fatal(err)
	}
	id3, _ := NewUUID()
	if _, _, err := s.CreateConvert(ctx, CreateConvert{JobID: id3, Name: name2, InputRef: "x", InputFormat: "csv", Mode: ModeAppend}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("append with no READY version: %v", err)
	}
}

// Appending to a dataset registered from an existing prefix leaves that prefix
// read-only: the new data goes into a segment of its own, beside it.
func TestPostgresAppendToARegisteredDatasetAddsASegmentBesideItsPrefix(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	if err := Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	store := NewPostgresStore(db)
	s := NewPostgresIngestStore(db)
	q := NewPostgresQueryStore(db)
	name := itName("reg")
	rc := int64(100)
	if _, _, _, err := store.Register(ctx, RegisterRecord{
		Name: name, SourcePrefix: "data/" + name + "/", ObjectPrefix: "data/" + name + "/", Format: FormatParquet, RowCount: &rc,
		Columns: []SchemaColumn{{Name: "id", Type: "int64"}, {Name: "region", Type: "string"}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := itPublish(t, s, name, ModeAppend, "lake/default/"+name+"/version=2/attempt=1/", 5, nil); err != nil {
		t.Fatal(err)
	}
	rel, err := q.ResolveRelation(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"data/" + name + "/", "lake/default/" + name + "/version=2/attempt=1/"}
	if fmt.Sprint(rel.Prefixes()) != fmt.Sprint(want) || rel.HivePartitioned {
		t.Fatalf("prefixes %v hive=%v", rel.Prefixes(), rel.HivePartitioned)
	}
	var rows int64
	_ = db.QueryRow(`SELECT v.row_count FROM dataset_versions v JOIN datasets d ON d.id = v.dataset_id WHERE d.name = $1 AND v.version = 2`, name).Scan(&rows)
	if rows != 105 {
		t.Fatalf("row_count = %d, want 100+5", rows)
	}

	// a Hive-partitioned registered dataset cannot be appended to
	hive := itName("hive")
	if _, _, _, err := store.Register(ctx, RegisterRecord{
		Name: hive, SourcePrefix: "data/" + hive + "/", ObjectPrefix: "data/" + hive + "/", Format: FormatParquet, PartitionColumns: []string{"year"},
		Columns: []SchemaColumn{{Name: "id", Type: "int64"}, {Name: "year", Type: "int64", IsPartition: true}},
	}); err != nil {
		t.Fatal(err)
	}
	id, _ := NewUUID()
	if _, _, err := s.CreateConvert(ctx, CreateConvert{JobID: id, Name: hive, InputRef: "x", InputFormat: "csv", Mode: ModeAppend}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("append to a hive-partitioned registered dataset: %v", err)
	}
}

func mustUUID(t *testing.T) string {
	t.Helper()
	id, err := NewUUID()
	if err != nil {
		t.Fatal(err)
	}
	return id
}
