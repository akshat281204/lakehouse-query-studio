package ingest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/akshat281204/lakehouse-query-studio/internal/catalog"
	"github.com/akshat281204/lakehouse-query-studio/internal/objectstore"
	"github.com/akshat281204/lakehouse-query-studio/internal/parquet"
)

// ---- in-memory object store with failure injection ----

type memObjects struct {
	mu      sync.Mutex
	data    map[string][]byte
	putErr  func(key string) error
	putSeen int
}

func newMemObjects() *memObjects { return &memObjects{data: map[string][]byte{}} }

func (m *memObjects) Bucket() string { return "test" }

func (m *memObjects) keys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for k := range m.data {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func (m *memObjects) keysWithPrefix(p string) []string {
	var out []string
	for _, k := range m.keys() {
		if strings.HasPrefix(k, p) {
			out = append(out, k)
		}
	}
	return out
}

func (m *memObjects) List(_ context.Context, opts objectstore.ListOptions) (objectstore.ListResult, error) {
	var res objectstore.ListResult
	for _, k := range m.keysWithPrefix(opts.Prefix) {
		m.mu.Lock()
		res.Objects = append(res.Objects, objectstore.Object{Key: k, Size: int64(len(m.data[k]))})
		m.mu.Unlock()
	}
	return res, nil
}

func (m *memObjects) Range(_ context.Context, key string, off, n int64) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.data[key]
	if !ok {
		return nil, objectstore.ErrNotFound
	}
	end := min(off+n, int64(len(b)))
	return io.NopCloser(bytes.NewReader(b[off:end])), nil
}

func (m *memObjects) Put(_ context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.putSeen++
	if m.putErr != nil {
		if err := m.putErr(key); err != nil {
			return err
		}
	}
	m.data[key] = append([]byte(nil), data...)
	return nil
}

func (m *memObjects) Get(_ context.Context, key string, max int64) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.data[key]
	if !ok {
		return nil, objectstore.ErrNotFound
	}
	if int64(len(b)) > max {
		return nil, objectstore.ErrTooLarge
	}
	return append([]byte(nil), b...), nil
}

func (m *memObjects) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	return nil
}

// ---- parse / convert ----

func TestParseCSVInfersTypes(t *testing.T) {
	csv := "id,price,active,name,empty\n1,1.5,true,alice,\n2,2,false,,\n3,,TRUE,carol,\n"
	tbl, err := Parse(catalog.InputFormatCSV, []byte(csv), DefaultLimits)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	want := []parquet.WriteColumn{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "price", Type: parquet.TypeDouble, Nullable: true},
		{Name: "active", Type: parquet.TypeBoolean},
		{Name: "name", Type: parquet.TypeString, Nullable: true},
		{Name: "empty", Type: parquet.TypeString, Nullable: true},
	}
	for i, w := range want {
		if tbl.Columns[i] != w {
			t.Errorf("column %d = %+v, want %+v", i, tbl.Columns[i], w)
		}
	}
	if len(tbl.Rows) != 3 || tbl.Rows[0][0] != int64(1) || tbl.Rows[1][1] != 2.0 || tbl.Rows[2][1] != nil || tbl.Rows[2][2] != true {
		t.Errorf("rows = %v", tbl.Rows)
	}
}

func TestParseCSVRejections(t *testing.T) {
	cases := map[string]string{
		"empty":         "",
		"header only":   "a,b\n",
		"dup header":    "a,A\n1,2\n",
		"empty name":    "a,\n1,2\n",
		"ragged":        "a,b\n1\n",
		"bad quote":     "a,b\n\"1,2\n",
		"nul":           "a\n1\x002\n",
		"invalid utf8":  "a\n\xff\n",
		"padded header": " a,b\n1,2\n",
	}
	for name, in := range cases {
		_, err := Parse(catalog.InputFormatCSV, []byte(in), DefaultLimits)
		if !IsUserError(err) {
			t.Errorf("%s: err = %v, want UserError", name, err)
		}
	}
	lim := DefaultLimits
	lim.MaxBytes = 4
	if _, err := Parse(catalog.InputFormatCSV, []byte("a,b\n1,2\n"), lim); !IsUserError(err) {
		t.Errorf("oversize: %v", err)
	}
	lim = DefaultLimits
	lim.MaxRows = 1
	if _, err := Parse(catalog.InputFormatCSV, []byte("a\n1\n2\n"), lim); !IsUserError(err) {
		t.Errorf("too many rows: %v", err)
	}
	lim = DefaultLimits
	lim.MaxColumns = 1
	if _, err := Parse(catalog.InputFormatCSV, []byte("a,b\n1,2\n"), lim); !IsUserError(err) {
		t.Errorf("too many columns: %v", err)
	}
}

func TestParseErrorsNeverEchoValues(t *testing.T) {
	_, err := Parse(catalog.InputFormatCSV, []byte("a,b\nsecret-value\n"), DefaultLimits)
	if err == nil || strings.Contains(err.Error(), "secret-value") {
		t.Fatalf("error leaks cell value: %v", err)
	}
}

func TestParseJSONL(t *testing.T) {
	in := `{"id":1,"v":1.5,"s":"x"}` + "\n" + `{"id":2,"s":null,"extra":true}` + "\n"
	tbl, err := Parse(catalog.InputFormatJSONL, []byte(in), DefaultLimits)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	got := map[string]parquet.WriteColumn{}
	for _, c := range tbl.Columns {
		got[c.Name] = c
	}
	if got["id"].Type != parquet.TypeInt64 || got["id"].Nullable {
		t.Errorf("id = %+v", got["id"])
	}
	if got["v"].Type != parquet.TypeDouble || !got["v"].Nullable {
		t.Errorf("v = %+v", got["v"])
	}
	if got["extra"].Type != parquet.TypeBoolean || !got["extra"].Nullable {
		t.Errorf("extra = %+v", got["extra"])
	}
	if got["s"].Type != parquet.TypeString || !got["s"].Nullable {
		t.Errorf("s = %+v", got["s"])
	}
}

func TestParseJSONArrayAndMixedTypes(t *testing.T) {
	in := `[{"a":1},{"a":"two"},{"a":true}]`
	tbl, err := Parse(catalog.InputFormatJSON, []byte(in), DefaultLimits)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if tbl.Columns[0].Type != parquet.TypeString {
		t.Errorf("mixed column type = %s, want string", tbl.Columns[0].Type)
	}
	if tbl.Rows[0][0] != "1" || tbl.Rows[2][0] != "true" {
		t.Errorf("rows = %v", tbl.Rows)
	}
}

func TestParseJSONRejections(t *testing.T) {
	for name, in := range map[string]string{
		"nested object": `{"a":{"b":1}}`,
		"nested array":  `{"a":[1]}`,
		"not object":    `[1,2]`,
		"malformed":     `{"a":`,
		"empty":         ``,
	} {
		format := catalog.InputFormatJSONL
		if name == "not object" {
			format = catalog.InputFormatJSON
		}
		if _, err := Parse(format, []byte(in), DefaultLimits); !IsUserError(err) {
			t.Errorf("%s: err = %v, want UserError", name, err)
		}
	}
	if _, err := Parse(catalog.InputFormatJSON, []byte(`{"a":1}`), DefaultLimits); !IsUserError(err) {
		t.Errorf("object instead of array: %v", err)
	}
}

func TestEncodePartitionComponent(t *testing.T) {
	cases := map[string]string{
		"abc-1._~": "abc-1._~",
		"a/b":      "a%2Fb",
		"a=b":      "a%3Db",
		"100%":     "100%25",
		"a b+c":    "a%20b%2Bc",
		"é":        "%C3%A9",
		"":         "",
	}
	for in, want := range cases {
		if got := EncodePartitionComponent(in); got != want {
			t.Errorf("EncodePartitionComponent(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestConvertUnpartitionedSplitsParts(t *testing.T) {
	tbl, _ := Parse(catalog.InputFormatCSV, []byte("id\n1\n2\n3\n4\n5\n"), DefaultLimits)
	lim := DefaultLimits
	lim.PartRows = 2
	parts, err := Convert(tbl, nil, lim)
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	if len(parts) != 3 {
		t.Fatalf("parts = %d, want 3", len(parts))
	}
	total := 0
	for _, p := range parts {
		total += p.Rows
		if strings.Contains(p.RelPath, "/") {
			t.Errorf("unpartitioned part has directory: %s", p.RelPath)
		}
	}
	if total != 5 {
		t.Errorf("rows = %d", total)
	}
}

func TestConvertPartitioned(t *testing.T) {
	csv := "region,id\nus/east,1\nus/east,2\neu,3\n,4\n"
	tbl, err := Parse(catalog.InputFormatCSV, []byte(csv), DefaultLimits)
	if err != nil {
		t.Fatal(err)
	}
	parts, err := Convert(tbl, []string{"region"}, DefaultLimits)
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	dirs := map[string]int{}
	for _, p := range parts {
		dir := p.RelPath[:strings.LastIndex(p.RelPath, "/")+1]
		dirs[dir] += p.Rows
	}
	want := map[string]int{"region=us%2Feast/": 2, "region=eu/": 1, "region=%00/": 1}
	for d, n := range want {
		if dirs[d] != n {
			t.Errorf("dir %q rows = %d, want %d (all: %v)", d, dirs[d], n, dirs)
		}
	}
	// Partition columns stay in the file so the registered schema is complete.
	r := &memReader{data: parts[0].Data}
	sch, err := parquet.Inspect(context.Background(), r, "k", int64(len(parts[0].Data)))
	if err != nil || len(sch.Columns) != 2 {
		t.Fatalf("Inspect = %+v, %v", sch, err)
	}
}

type memReader struct{ data []byte }

func (m *memReader) Range(_ context.Context, _ string, off, n int64) (io.ReadCloser, error) {
	end := min(off+n, int64(len(m.data)))
	return io.NopCloser(bytes.NewReader(m.data[off:end])), nil
}

func TestConvertPartitionValidation(t *testing.T) {
	tbl, _ := Parse(catalog.InputFormatCSV, []byte("a,b,c\n1,2.5,x\n"), DefaultLimits)
	for name, parts := range map[string][]string{
		"unknown":   {"zzz"},
		"float":     {"b"},
		"duplicate": {"a", "a"},
		"all":       {"a", "b", "c"},
		"too many":  {"a", "b", "c", "d", "e"},
	} {
		if _, err := Convert(tbl, parts, DefaultLimits); !IsUserError(err) {
			t.Errorf("%s: err = %v, want UserError", name, err)
		}
	}
}

func TestConvertCapsDistinctPartitions(t *testing.T) {
	var sb strings.Builder
	sb.WriteString("k,v\n")
	for i := 0; i < 5; i++ {
		sb.WriteString(strings.Repeat("x", i+1) + ",1\n")
	}
	tbl, _ := Parse(catalog.InputFormatCSV, []byte(sb.String()), DefaultLimits)
	lim := DefaultLimits
	lim.MaxPartitions = 3
	if _, err := Convert(tbl, []string{"k"}, lim); !IsUserError(err) {
		t.Errorf("err = %v, want UserError", err)
	}
}

// ---- runner ----

type harness struct {
	store   *catalog.MemoryIngestStore
	objects *memObjects
	runner  *Runner
}

func newHarness() *harness {
	store := catalog.NewMemoryIngestStore()
	objs := newMemObjects()
	r := NewRunner(store, objs, "w1", slog.New(slog.NewTextHandler(io.Discard, nil)))
	return &harness{store: store, objects: objs, runner: r}
}

func (h *harness) submit(t *testing.T, name, format, body string, partitions []string, retain bool) catalog.Job {
	t.Helper()
	id, err := catalog.NewUUID()
	if err != nil {
		t.Fatal(err)
	}
	key := "uploads/default/" + name + "/" + id + "/input"
	if err := h.objects.Put(context.Background(), key, []byte(body)); err != nil {
		t.Fatal(err)
	}
	job, _, err := h.store.CreateConvert(context.Background(), catalog.CreateConvert{
		JobID: id, Name: name, InputRef: key, InputFormat: format, PartitionColumns: partitions, RetainOriginal: retain,
	})
	if err != nil {
		t.Fatal(err)
	}
	return job
}

func (h *harness) runAll(t *testing.T) {
	t.Helper()
	for {
		worked, err := h.runner.RunOnce(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if !worked {
			return
		}
	}
}

func TestRunnerConvertsCSVAndPublishes(t *testing.T) {
	h := newHarness()
	job := h.submit(t, "sales", catalog.InputFormatCSV, "id,region,amount\n1,us,1.5\n2,eu,2.5\n3,us,\n", []string{"region"}, false)
	h.runAll(t)

	got, _ := h.store.GetJob(context.Background(), job.ID)
	if got.Status != catalog.JobSucceeded {
		t.Fatalf("status = %s (%s)", got.Status, got.ErrorSummary)
	}
	ready := h.store.ReadyVersions("sales")
	if len(ready) != 1 || ready[0].RowCount == nil || *ready[0].RowCount != 3 {
		t.Fatalf("ready = %+v", ready)
	}
	v := ready[0]
	if !strings.HasPrefix(v.ObjectPrefix, "lake/default/sales/version=1/attempt=1/") {
		t.Errorf("object prefix = %s", v.ObjectPrefix)
	}
	cols := h.store.Columns(v.ID)
	if len(cols) != 3 || cols[1].Name != "region" || !cols[1].IsPartition || !cols[2].Nullable {
		t.Errorf("columns = %+v", cols)
	}
	lake := h.objects.keysWithPrefix(v.ObjectPrefix)
	if len(lake) != 2 {
		t.Fatalf("lake objects = %v", lake)
	}
	if !strings.Contains(lake[0], "region=eu/") && !strings.Contains(lake[1], "region=eu/") {
		t.Errorf("missing hive partition dir: %v", lake)
	}
	// Written Parquet is readable by the catalog's own preview reader.
	data := h.objects.data[lake[0]]
	res, err := parquet.Preview(context.Background(), &memReader{data: data}, "k", int64(len(data)), 10)
	if err != nil || len(res.Rows) == 0 {
		t.Fatalf("Preview = %+v, %v", res, err)
	}
	// Staging and the original upload are gone after success.
	if left := h.objects.keysWithPrefix("staging/"); len(left) != 0 {
		t.Errorf("staging left behind: %v", left)
	}
	if left := h.objects.keysWithPrefix("uploads/"); len(left) != 0 {
		t.Errorf("upload not released: %v", left)
	}
}

func TestRunnerRetainsOriginalWhenRequested(t *testing.T) {
	h := newHarness()
	h.submit(t, "keep", catalog.InputFormatJSONL, `{"a":1}`+"\n", nil, true)
	h.runAll(t)
	if len(h.objects.keysWithPrefix("uploads/")) != 1 {
		t.Errorf("retained upload missing: %v", h.objects.keys())
	}
}

func TestRunnerInvalidInputLeavesNothingBehind(t *testing.T) {
	h := newHarness()
	job := h.submit(t, "bad", catalog.InputFormatCSV, "a,b\n1\n", nil, false)
	h.runAll(t)

	got, _ := h.store.GetJob(context.Background(), job.ID)
	if got.Status != catalog.JobFailed || got.Retryable || got.FailureReason != ReasonInvalidInput {
		t.Fatalf("job = %+v", got)
	}
	if got.ErrorSummary == "" {
		t.Error("missing actionable summary")
	}
	if len(h.store.ReadyVersions("bad")) != 0 {
		t.Error("failed ingestion exposed a READY version")
	}
	if left := h.objects.keysWithPrefix("lake/"); len(left) != 0 {
		t.Errorf("lake objects left: %v", left)
	}
	if left := h.objects.keysWithPrefix("staging/"); len(left) != 0 {
		t.Errorf("staging left: %v", left)
	}
}

func TestRunnerPartialWriteFailureCleansUpAndIsRetryable(t *testing.T) {
	h := newHarness()
	lim := DefaultLimits
	lim.PartRows = 1
	h.runner.Limits = lim
	job := h.submit(t, "flaky", catalog.InputFormatCSV, "id\n1\n2\n3\n", nil, false)

	// Fail the second lake write: staging succeeded, lake is partially written.
	lakeWrites := 0
	h.objects.putErr = func(key string) error {
		if strings.HasPrefix(key, "lake/") {
			lakeWrites++
			if lakeWrites == 2 {
				return objectstore.ErrUnavailable
			}
		}
		return nil
	}
	h.runAll(t)

	got, _ := h.store.GetJob(context.Background(), job.ID)
	if got.Status != catalog.JobFailed || !got.Retryable || got.FailureReason != ReasonStorage {
		t.Fatalf("job = %+v", got)
	}
	if len(h.store.ReadyVersions("flaky")) != 0 {
		t.Fatal("partial version exposed as READY")
	}
	if left := h.objects.keysWithPrefix("lake/"); len(left) != 0 {
		t.Errorf("partial lake objects left: %v", left)
	}
	if left := h.objects.keysWithPrefix("staging/"); len(left) != 0 {
		t.Errorf("staging left: %v", left)
	}
	if len(h.objects.keysWithPrefix("uploads/")) != 1 {
		t.Error("upload must survive a retryable failure")
	}

	// Retry succeeds on a fresh version without creating duplicate visible ones.
	h.objects.putErr = nil
	id, _ := catalog.NewUUID()
	retry, err := h.store.Retry(context.Background(), job.ID, id)
	if err != nil {
		t.Fatalf("Retry: %v", err)
	}
	if retry.Version != 2 || retry.RetryOf != job.ID {
		t.Errorf("retry = %+v", retry)
	}
	if _, err := h.store.Retry(context.Background(), job.ID, "x"); !errors.Is(err, catalog.ErrAlreadyExists) {
		t.Errorf("second retry err = %v, want ErrAlreadyExists", err)
	}
	h.runAll(t)
	ready := h.store.ReadyVersions("flaky")
	if len(ready) != 1 || ready[0].Version != 2 || *ready[0].RowCount != 3 {
		t.Fatalf("after retry ready = %+v", ready)
	}
}

func TestRunnerMissingUploadFailsNonRetryable(t *testing.T) {
	h := newHarness()
	job := h.submit(t, "gone", catalog.InputFormatCSV, "a\n1\n", nil, false)
	_ = h.objects.Delete(context.Background(), job.InputRef)
	h.runAll(t)
	got, _ := h.store.GetJob(context.Background(), job.ID)
	if got.Status != catalog.JobFailed || got.Retryable || got.FailureReason != ReasonUploadGone {
		t.Fatalf("job = %+v", got)
	}
}

func TestRunnerRecoversAfterWorkerCrash(t *testing.T) {
	h := newHarness()
	now := time.Now().UTC()
	h.store.Now = func() time.Time { return now }
	job := h.submit(t, "crash", catalog.InputFormatCSV, "a\n1\n2\n", nil, false)

	// A worker claims the job and dies without finishing.
	crashed, err := h.store.Claim(context.Background(), "dead", time.Minute)
	if err != nil || crashed == nil || crashed.Attempt != 1 {
		t.Fatalf("claim = %+v, %v", crashed, err)
	}
	// Before the lease expires nobody else may take it.
	if again, _ := h.store.Claim(context.Background(), "w2", time.Minute); again != nil {
		t.Fatal("job claimed while lease is live")
	}
	// After expiry the next worker recovers it as attempt 2.
	now = now.Add(2 * time.Minute)
	h.runAll(t)
	got, _ := h.store.GetJob(context.Background(), job.ID)
	if got.Status != catalog.JobSucceeded || got.Attempt != 2 {
		t.Fatalf("job = %+v", got)
	}
	ready := h.store.ReadyVersions("crash")
	if len(ready) != 1 || !strings.Contains(ready[0].ObjectPrefix, "attempt=2") {
		t.Fatalf("ready = %+v", ready)
	}
	// The dead worker's late commit is fenced.
	err = h.store.Complete(context.Background(), job.ID, crashed.FencingToken, catalog.Publish{ObjectPrefix: "x"})
	if !errors.Is(err, catalog.ErrFenced) {
		t.Errorf("stale Complete err = %v, want ErrFenced", err)
	}
}

func TestRunnerFailsJobAfterRepeatedCrashes(t *testing.T) {
	h := newHarness()
	now := time.Now().UTC()
	h.store.Now = func() time.Time { return now }
	job := h.submit(t, "loop", catalog.InputFormatCSV, "a\n1\n", nil, false)
	for i := 0; i < catalog.MaxAttempts; i++ {
		if c, _ := h.store.Claim(context.Background(), "dead", time.Minute); c == nil {
			t.Fatalf("claim %d returned nil", i)
		}
		now = now.Add(2 * time.Minute)
	}
	if c, _ := h.store.Claim(context.Background(), "w", time.Minute); c != nil {
		t.Fatalf("exhausted job claimed again: %+v", c)
	}
	got, _ := h.store.GetJob(context.Background(), job.ID)
	if got.Status != catalog.JobFailed || !got.Retryable {
		t.Fatalf("job = %+v", got)
	}
}

func TestRunnerRejectsOversizeUpload(t *testing.T) {
	h := newHarness()
	h.runner.Limits.MaxBytes = 8
	job := h.submit(t, "big", catalog.InputFormatCSV, "a,b\n1,2\n3,4\n", nil, false)
	h.runAll(t)
	got, _ := h.store.GetJob(context.Background(), job.ID)
	if got.Status != catalog.JobFailed || got.Retryable {
		t.Fatalf("job = %+v", got)
	}
}
