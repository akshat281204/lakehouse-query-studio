package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/akshat281204/lakehouse-query-studio/internal/catalog"
	"github.com/akshat281204/lakehouse-query-studio/internal/ingest"
	"github.com/akshat281204/lakehouse-query-studio/internal/objectstore"
	"github.com/akshat281204/lakehouse-query-studio/internal/parquet"
)

// publishedDataset makes "sales" a READY dataset in the in-memory store.
func publishedDataset(t *testing.T, jobs *catalog.MemoryIngestStore, partitions []string) {
	t.Helper()
	ctx := context.Background()
	if _, _, err := jobs.CreateConvert(ctx, catalog.CreateConvert{JobID: mustJobID(t), Name: "sales", InputRef: "x", InputFormat: "csv", PartitionColumns: partitions}); err != nil {
		t.Fatal(err)
	}
	j, err := jobs.Claim(ctx, "w", 0)
	if err != nil || j == nil {
		t.Fatal(err)
	}
	cols := []catalog.SchemaColumn{{Name: "id", Type: "int64"}, {Name: "region", Type: "string"}, {Name: "amount", Type: "double", Nullable: true}}
	if err := jobs.Complete(ctx, j.ID, j.FencingToken, catalog.Publish{ObjectPrefix: "lake/default/sales/version=1/attempt=1/", PartitionColumns: partitions, RowCount: 2, Columns: cols}); err != nil {
		t.Fatal(err)
	}
}

func mustJobID(t *testing.T) string {
	t.Helper()
	id, err := catalog.NewUUID()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func parquetBody(t *testing.T, cols []parquet.WriteColumn, rows [][]any) string {
	t.Helper()
	b, err := parquet.Encode(cols, rows)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

var apiSalesCols = []parquet.WriteColumn{
	{Name: "id", Type: parquet.TypeInt64},
	{Name: "region", Type: parquet.TypeString},
	{Name: "amount", Type: parquet.TypeDouble, Nullable: true},
}

func TestParquetUploadIsAcceptedByExtensionAndQueuedAsParquet(t *testing.T) {
	h, _, objs := ingestHandler(ingest.DefaultLimits)
	body := parquetBody(t, apiSalesCols, [][]any{{int64(1), "us", 1.5}})
	rec := do(h, http.MethodPost, "/api/v1/ingestions?name=sales&filename=data.parquet", body)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	var job catalog.Job
	_ = json.Unmarshal(rec.Body.Bytes(), &job)
	if job.InputFormat != catalog.InputFormatParquet || job.Mode != catalog.ModeReplace {
		t.Fatalf("job: %+v", job)
	}
	var stored string
	for k, v := range objs.data {
		if strings.HasPrefix(k, "uploads/") {
			stored = string(v)
		}
	}
	if stored != body {
		t.Fatal("the uploaded file must be stored unchanged")
	}
}

func TestParquetUploadRejectsNonParquetAndPartitioning(t *testing.T) {
	h, jobs, objs := ingestHandler(ingest.DefaultLimits)
	rec := do(h, http.MethodPost, "/api/v1/ingestions?name=sales&format=parquet", "id,region\n1,us\n")
	if rec.Code != http.StatusUnprocessableEntity || errCode(t, rec) != "invalid_input" {
		t.Fatalf("csv text as parquet: %d %s", rec.Code, rec.Body)
	}
	if strings.Contains(rec.Body.String(), "1,us") {
		t.Fatal("the error echoes file content")
	}
	good := parquetBody(t, apiSalesCols, [][]any{{int64(1), "us", 1.5}})
	if rec = do(h, http.MethodPost, "/api/v1/ingestions?name=sales&format=parquet&partitionColumns=region", good); rec.Code != http.StatusBadRequest {
		t.Fatalf("partitioning a parquet import: %d", rec.Code)
	}
	if len(objs.data) != 0 {
		t.Fatalf("rejected uploads left %d objects", len(objs.data))
	}
	if jl, _ := jobs.ListJobs(context.Background(), 10); len(jl) != 0 {
		t.Fatalf("rejected uploads created %d jobs", len(jl))
	}
}

func TestInspectReportsAParquetFileWithoutStoringIt(t *testing.T) {
	h, _, objs := ingestHandler(ingest.DefaultLimits)
	body := parquetBody(t, apiSalesCols, [][]any{{int64(1), "us", 1.5}, {int64(2), "eu", nil}})
	rec := do(h, http.MethodPost, "/api/v1/ingestions/inspect?format=parquet", body)
	var got inspectResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &got)
	if rec.Code != 200 || got.RowCount != 2 || len(got.Columns) != 3 || got.Columns[2].Type != "double" || !got.Columns[2].Nullable {
		t.Fatalf("%d %+v", rec.Code, got)
	}
	if len(got.Sample) != 2 {
		t.Fatalf("sample: %v note %q", got.Sample, got.Note)
	}
	if len(objs.data) != 0 {
		t.Fatal("inspect must not store anything")
	}
}

func TestInspectWithATargetReportsAppendCompatibility(t *testing.T) {
	h, jobs, _ := ingestHandler(ingest.DefaultLimits)
	publishedDataset(t, jobs, nil)
	inspect := func(url, body string) inspectResponse {
		t.Helper()
		rec := do(h, http.MethodPost, url, body)
		var got inspectResponse
		if rec.Code != 200 {
			t.Fatalf("%s: %d %s", url, rec.Code, rec.Body)
		}
		_ = json.Unmarshal(rec.Body.Bytes(), &got)
		return got
	}
	ok := inspect("/api/v1/ingestions/inspect?format=csv&target=sales", "id,region,amount\n3,us,1.5\n")
	if ok.Append == nil || !ok.Append.Compatible || len(ok.Append.Issues) != 0 || ok.Append.LatestVersion != 1 || *ok.Append.DatasetRows != 2 {
		t.Fatalf("compatible: %+v", ok.Append)
	}
	bad := inspect("/api/v1/ingestions/inspect?format=csv&target=sales", "id,region,amount,extra\nx,us,1.5,1\n")
	if bad.Append == nil || bad.Append.Compatible || len(bad.Append.Issues) != 2 {
		t.Fatalf("incompatible: %+v", bad.Append)
	}
	gone := inspect("/api/v1/ingestions/inspect?format=csv&target=nope", "id\n1\n")
	if gone.Append == nil || gone.Append.Compatible || !strings.Contains(gone.Append.Issues[0], "does not exist") {
		t.Fatalf("missing dataset: %+v", gone.Append)
	}
	if none := inspect("/api/v1/ingestions/inspect?format=csv", "id\n1\n"); none.Append != nil {
		t.Fatal("no target, no verdict")
	}
	if rec := do(h, http.MethodPost, "/api/v1/ingestions/inspect?format=csv&target=Bad%20Name", "id\n1\n"); rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid target name: %d", rec.Code)
	}
}

func TestAppendUploadQueuesAnAppendJob(t *testing.T) {
	h, jobs, _ := ingestHandler(ingest.DefaultLimits)
	publishedDataset(t, jobs, nil)
	rec := do(h, http.MethodPost, "/api/v1/ingestions?name=sales&format=csv&mode=append", "id,region,amount\n3,us,\n")
	if rec.Code != http.StatusAccepted {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
	var job catalog.Job
	_ = json.Unmarshal(rec.Body.Bytes(), &job)
	if job.Mode != catalog.ModeAppend || job.Version != 2 {
		t.Fatalf("job: %+v", job)
	}
	pq := parquetBody(t, apiSalesCols, [][]any{{int64(4), "eu", 2.5}})
	if rec = do(h, http.MethodPost, "/api/v1/ingestions?name=sales&mode=append&filename=more.parquet", pq); rec.Code != http.StatusAccepted {
		t.Fatalf("parquet append: %d %s", rec.Code, rec.Body)
	}
}

func TestAppendUploadFailsEarlyAndCleanly(t *testing.T) {
	h, jobs, objs := ingestHandler(ingest.DefaultLimits)
	publishedDataset(t, jobs, nil)
	before := len(objs.data)

	for name, c := range map[string]struct {
		url, body string
		code      int
		contains  string
	}{
		"missing dataset":  {"/api/v1/ingestions?name=nope&format=csv&mode=append", "id\n1\n", 404, "does not exist"},
		"type mismatch":    {"/api/v1/ingestions?name=sales&format=csv&mode=append", "id,region,amount\nabc,us,1\n", 422, `column "id" is string`},
		"extra column":     {"/api/v1/ingestions?name=sales&format=csv&mode=append", "id,region,amount,x\n1,us,1,1\n", 422, `column "x" is not in the dataset`},
		"partition param":  {"/api/v1/ingestions?name=sales&format=csv&mode=append&partitionColumns=region", "id,region,amount\n1,us,1\n", 400, "partitionColumns"},
		"unknown mode":     {"/api/v1/ingestions?name=sales&format=csv&mode=merge", "id\n1\n", 400, "mode must be"},
		"parquet mismatch": {"/api/v1/ingestions?name=sales&format=parquet&mode=append", parquetBody(t, []parquet.WriteColumn{{Name: "id", Type: parquet.TypeString}}, [][]any{{"a"}}), 422, `column "id" is string`},
	} {
		rec := do(h, http.MethodPost, c.url, c.body)
		var eb errorBody
		_ = json.Unmarshal(rec.Body.Bytes(), &eb)
		if rec.Code != c.code || !strings.Contains(eb.Error.Message, c.contains) {
			t.Errorf("%s: %d %s", name, rec.Code, rec.Body)
		}
	}
	if len(objs.data) != before {
		t.Fatalf("rejected appends left %d objects", len(objs.data)-before)
	}
	if jl, _ := jobs.ListJobs(context.Background(), 10); len(jl) != 1 { // only the publishedDataset job
		t.Fatalf("rejected appends created jobs: %d", len(jl))
	}
	if _, ok := mustVersions(jobs, "nope"); ok {
		t.Fatal("a failed append created a dataset")
	}
}

func mustVersions(jobs *catalog.MemoryIngestStore, name string) ([]catalog.DatasetVersion, bool) {
	v := jobs.ReadyVersions(name)
	return v, len(v) > 0
}

// The profile and preview of an appended version cover every segment.
func TestProfileAndPreviewSpanAllSegmentsOfAnAppendedVersion(t *testing.T) {
	mem := objectstore.NewMemory()
	put := func(key string, rows [][]any) {
		b, err := parquet.Encode(profileCols, rows)
		if err != nil {
			t.Fatal(err)
		}
		_ = mem.Put(context.Background(), key, b)
	}
	put("lake/default/sales/version=1/attempt=1/part-a.parquet", [][]any{{int64(1), 1.5, "us", true}, {int64(2), 2.5, "eu", true}})
	put("lake/default/sales/version=2/attempt=1/part-b.parquet", [][]any{{int64(50), 9.5, "ap", false}})
	st := &fakeStore{
		datasets: []catalog.Dataset{{ID: "d1", Name: "sales"}},
		versions: map[string]catalog.DatasetVersion{"d1": {
			ID: "v2", DatasetID: "d1", Version: 2, Status: catalog.VersionReady,
			ObjectPrefix: "lake/default/sales/version=2/attempt=1/", BaseSegments: []string{"lake/default/sales/version=1/attempt=1/"},
		}},
		schema: map[string][]catalog.SchemaColumn{"v2": {
			{Position: 0, Name: "id", Type: "int64"}, {Position: 1, Name: "price", Type: "double", Nullable: true},
			{Position: 2, Name: "region", Type: "string"}, {Position: 3, Name: "ok", Type: "boolean"},
		}},
	}
	h := New(st, mem)
	code, p := getProfile(t, h, "d1")
	if code != 200 || p.Files != 2 || p.Rows != 3 {
		t.Fatalf("%d files=%d rows=%d note=%q", code, p.Files, p.Rows, p.Note)
	}
	if c := col(p, "id"); !c.HasRange || *c.Min != "1" || *c.Max != "50" {
		t.Fatalf("id range must span both segments: %+v", c)
	}
}
