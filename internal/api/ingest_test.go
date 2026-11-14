package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/akshat281204/lakehouse-query-studio/internal/catalog"
	"github.com/akshat281204/lakehouse-query-studio/internal/ingest"
	"github.com/akshat281204/lakehouse-query-studio/internal/objectstore"
)

type rwObjects struct {
	mu   sync.Mutex
	data map[string][]byte
}

func (o *rwObjects) Bucket() string { return "b" }
func (o *rwObjects) List(context.Context, objectstore.ListOptions) (objectstore.ListResult, error) {
	return objectstore.ListResult{}, nil
}
func (o *rwObjects) Range(context.Context, string, int64, int64) (io.ReadCloser, error) {
	return nil, objectstore.ErrNotFound
}
func (o *rwObjects) Put(_ context.Context, k string, d []byte) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.data[k] = append([]byte(nil), d...)
	return nil
}
func (o *rwObjects) Get(_ context.Context, k string, _ int64) ([]byte, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if d, ok := o.data[k]; ok {
		return d, nil
	}
	return nil, objectstore.ErrNotFound
}
func (o *rwObjects) Delete(_ context.Context, k string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.data, k)
	return nil
}

func ingestHandler(limits ingest.Limits) (http.Handler, *catalog.MemoryIngestStore, *rwObjects) {
	jobs := catalog.NewMemoryIngestStore()
	objs := &rwObjects{data: map[string][]byte{}}
	h := New(&fakeStore{}, &fakeObjects{}, WithIngest(IngestDeps{Jobs: jobs, Objects: objs, Limits: limits}))
	return h, jobs, objs
}

func do(h http.Handler, method, url, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, url, bytes.NewBufferString(body)))
	return rec
}

func errCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var body struct {
		Error struct{ Code string } `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("bad error body %q: %v", rec.Body.String(), err)
	}
	return body.Error.Code
}

func TestIngestUnavailableWithoutConfig(t *testing.T) {
	h := New(&fakeStore{}, &fakeObjects{})
	if rec := do(h, http.MethodPost, "/api/v1/ingestions?name=a&format=csv", "a\n1\n"); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d", rec.Code)
	}
}

func TestInspectReturnsSchemaAndSample(t *testing.T) {
	h, _, _ := ingestHandler(ingest.DefaultLimits)
	rec := do(h, http.MethodPost, "/api/v1/ingestions/inspect?format=csv", "id,name\n1,a\n2,\n")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body)
	}
	var got inspectResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &got)
	if got.RowCount != 2 || len(got.Columns) != 2 || got.Columns[0].Type != "int64" || !got.Columns[1].Nullable {
		t.Errorf("inspect = %+v", got)
	}
	if len(got.Sample) != 2 {
		t.Errorf("sample = %v", got.Sample)
	}
}

func TestUploadQueuesJob(t *testing.T) {
	h, jobs, objs := ingestHandler(ingest.DefaultLimits)
	rec := do(h, http.MethodPost, "/api/v1/ingestions?name=sales&filename=../my%20data.csv&partitionColumns=region", "id,region\n1,us\n")
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body)
	}
	var job catalog.Job
	_ = json.Unmarshal(rec.Body.Bytes(), &job)
	if job.Status != catalog.JobQueued || job.DatasetName != "sales" || job.Version != 1 || job.InputFormat != "csv" {
		t.Errorf("job = %+v", job)
	}
	if strings.Contains(rec.Body.String(), "uploads/") {
		t.Error("response leaks the object key")
	}
	if len(objs.data) != 1 {
		t.Fatalf("objects = %d", len(objs.data))
	}
	for k := range objs.data {
		if !strings.HasPrefix(k, "uploads/default/sales/"+job.ID+"/") || strings.Contains(k, "..") || strings.Contains(k, " ") {
			t.Errorf("upload key = %q", k)
		}
	}
	got, err := jobs.GetJob(context.Background(), job.ID)
	if err != nil || got.PartitionColumns[0] != "region" {
		t.Errorf("stored job = %+v, %v", got, err)
	}

	// Poll endpoint.
	rec = do(h, http.MethodGet, "/api/v1/ingestions/"+job.ID, "")
	if rec.Code != http.StatusOK {
		t.Errorf("get job status = %d", rec.Code)
	}
	rec = do(h, http.MethodGet, "/api/v1/ingestions", "")
	var list jobListResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &list)
	if rec.Code != http.StatusOK || len(list.Jobs) != 1 {
		t.Errorf("list = %d %s", rec.Code, rec.Body)
	}
}

func TestUploadRejectsBadInputWithoutSideEffects(t *testing.T) {
	h, jobs, objs := ingestHandler(ingest.DefaultLimits)
	cases := []struct {
		name, url, body string
		status          int
		code            string
	}{
		{"missing name", "/api/v1/ingestions?format=csv", "a\n1\n", 400, "invalid_argument"},
		{"bad name", "/api/v1/ingestions?name=Bad_Name&format=csv", "a\n1\n", 400, "invalid_argument"},
		{"no format", "/api/v1/ingestions?name=x", "a\n1\n", 400, "invalid_argument"},
		{"bad format", "/api/v1/ingestions?name=x&format=xlsx", "a\n1\n", 400, "invalid_argument"},
		{"empty body", "/api/v1/ingestions?name=x&format=csv", "", 400, "invalid_argument"},
		{"ragged csv", "/api/v1/ingestions?name=x&format=csv", "a,b\n1\n", 422, "invalid_input"},
		{"nested json", "/api/v1/ingestions?name=x&format=jsonl", `{"a":{"b":1}}`, 422, "invalid_input"},
		{"unknown partition", "/api/v1/ingestions?name=x&format=csv&partitionColumns=zzz", "a\n1\n", 422, "invalid_input"},
		{"bad partition name", "/api/v1/ingestions?name=x&format=csv&partitionColumns=%20a", "a\n1\n", 400, "invalid_argument"},
	}
	for _, c := range cases {
		rec := do(h, http.MethodPost, c.url, c.body)
		if rec.Code != c.status || errCode(t, rec) != c.code {
			t.Errorf("%s: status/code = %d/%s, want %d/%s (%s)", c.name, rec.Code, errCode(t, rec), c.status, c.code, rec.Body)
		}
	}
	if len(objs.data) != 0 {
		t.Errorf("rejected uploads left objects: %v", objs.data)
	}
	if list, _ := jobs.ListJobs(context.Background(), 10); len(list) != 0 {
		t.Errorf("rejected uploads created jobs: %v", list)
	}
}

func TestUploadEnforcesSizeLimit(t *testing.T) {
	lim := ingest.DefaultLimits
	lim.MaxBytes = 10
	h, _, objs := ingestHandler(lim)
	rec := do(h, http.MethodPost, "/api/v1/ingestions?name=x&format=csv", "a,b\n"+strings.Repeat("1,2\n", 20))
	if rec.Code != http.StatusRequestEntityTooLarge || errCode(t, rec) != "payload_too_large" {
		t.Errorf("status = %d %s", rec.Code, rec.Body)
	}
	if len(objs.data) != 0 {
		t.Error("oversize upload stored")
	}
}

func TestRetryEndpoint(t *testing.T) {
	h, jobs, _ := ingestHandler(ingest.DefaultLimits)
	rec := do(h, http.MethodPost, "/api/v1/ingestions?name=x&format=csv", "a\n1\n")
	var job catalog.Job
	_ = json.Unmarshal(rec.Body.Bytes(), &job)

	// A queued job cannot be retried.
	if rec := do(h, http.MethodPost, "/api/v1/ingestions/"+job.ID+"/retry", ""); rec.Code != http.StatusBadRequest {
		t.Errorf("retry queued status = %d", rec.Code)
	}
	if rec := do(h, http.MethodPost, "/api/v1/ingestions/"+strings.Repeat("0", 8)+"-0000-4000-8000-000000000000/retry", ""); rec.Code != http.StatusNotFound {
		t.Errorf("retry unknown status = %d", rec.Code)
	}

	claimed, _ := jobs.Claim(context.Background(), "w", 1<<40)
	_ = jobs.Fail(context.Background(), claimed.ID, claimed.FencingToken, "object_store_error", "retry", true)
	rec = do(h, http.MethodPost, "/api/v1/ingestions/"+job.ID+"/retry", "")
	if rec.Code != http.StatusAccepted {
		t.Fatalf("retry status = %d %s", rec.Code, rec.Body)
	}
	var retry catalog.Job
	_ = json.Unmarshal(rec.Body.Bytes(), &retry)
	if retry.Version != 2 || retry.RetryOf != job.ID || retry.Status != catalog.JobQueued {
		t.Errorf("retry = %+v", retry)
	}
	if rec := do(h, http.MethodPost, "/api/v1/ingestions/"+job.ID+"/retry", ""); rec.Code != http.StatusConflict {
		t.Errorf("double retry status = %d", rec.Code)
	}
}

func TestGetJobNotFound(t *testing.T) {
	h, _, _ := ingestHandler(ingest.DefaultLimits)
	if rec := do(h, http.MethodGet, "/api/v1/ingestions/nope", ""); rec.Code != http.StatusNotFound {
		t.Errorf("status = %d", rec.Code)
	}
}
