package demo

import (
	"bytes"
	"context"
	"encoding/csv"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/akshat281204/lakehouse-query-studio/internal/api"
	"github.com/akshat281204/lakehouse-query-studio/internal/catalog"
	"github.com/akshat281204/lakehouse-query-studio/internal/ingest"
	"github.com/akshat281204/lakehouse-query-studio/internal/objectstore"
	"github.com/akshat281204/lakehouse-query-studio/internal/query"
)

type stack struct {
	srv      *httptest.Server
	objects  *objectstore.Faulty
	mem      *objectstore.Memory
	ingStore *catalog.MemoryIngestStore
	ready    func() (int, string)
}

// newStack runs the real API handlers, stores and both runners in-process.
func newStack(t *testing.T) *stack {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	mem := objectstore.NewMemory()
	st := &stack{mem: mem, objects: objectstore.NewFaulty(mem), ingStore: catalog.NewMemoryIngestStore()}
	qStore := catalog.NewMemoryQueryStore()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	ing := ingest.NewRunner(st.ingStore, st.objects, "demo-test", log)
	ing.Poll = 5 * time.Millisecond
	go ing.Run(ctx)

	v, err := query.NewValidator()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = v.Close() })
	cfg := query.DefaultRunnerConfig()
	cfg.TmpDir = t.TempDir()
	cfg.Poll = 5 * time.Millisecond
	cfg.Heartbeat = 50 * time.Millisecond
	cfg.Lease = 5 * time.Second
	bridged := &bridgedQueryStore{MemoryQueryStore: qStore, ing: st.ingStore}
	qr, err := query.NewRunner(bridged, st.objects, query.NewDuckDB(), v, "demo-test", cfg, log)
	if err != nil {
		t.Fatal(err)
	}
	go qr.Run(ctx)

	h := api.New(nil, mem,
		api.WithIngest(api.IngestDeps{Jobs: st.ingStore, Objects: st.objects}),
		api.WithQueries(api.QueryDeps{Store: bridged, Validator: v, Objects: mem, Limits: api.DefaultQueryLimits()}))
	st.ready = func() (int, string) { return 200, `{"status":"ok","dependencies":{}}` }
	mux := http.NewServeMux()
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		code, body := st.ready()
		w.WriteHeader(code)
		_, _ = io.WriteString(w, body)
	})
	mux.Handle("/", h)
	st.srv = httptest.NewServer(mux)
	t.Cleanup(st.srv.Close)
	return st
}

func (s *stack) opts() Options {
	return Options{API: s.srv.URL, Web: "http://localhost:5173", Timeout: 30 * time.Second, Poll: 10 * time.Millisecond, Rows: 800}
}

func TestSampleIsDeterministicAndExpectationMatchesTheCSV(t *testing.T) {
	a, ea := Sample(500)
	b, eb := Sample(500)
	if !bytes.Equal(a, b) || len(ea.Orders) != len(eb.Orders) {
		t.Fatal("sample differs between calls")
	}
	recs, err := csv.NewReader(bytes.NewReader(a)).ReadAll()
	if err != nil || len(recs) != 501 {
		t.Fatalf("%d records, %v", len(recs), err)
	}
	orders := map[string]int64{}
	rev := map[string]float64{}
	for _, r := range recs[1:] {
		q, _ := strconv.Atoi(r[3])
		p, _ := strconv.ParseFloat(r[4], 64)
		orders[r[1]]++
		rev[r[1]] += float64(q) * p
	}
	for region, n := range orders {
		if ea.Orders[region] != n {
			t.Errorf("%s orders %d vs %d", region, ea.Orders[region], n)
		}
		if d := ea.Revenue[region] - rev[region]; d > 0.01 || d < -0.01 {
			t.Errorf("%s revenue %v vs %v", region, ea.Revenue[region], rev[region])
		}
	}
}

func TestDemoRunsEndToEndAndVerifiesTheAnswer(t *testing.T) {
	s := newStack(t)
	var out bytes.Buffer
	if err := Run(context.Background(), s.opts(), &out); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	text := out.String()
	for _, want := range []string{"dataset is READY", "verified against values computed independently",
		"http://localhost:5173/#/query", "REGION", "REVENUE", "Cleanup: `make down`", "Nothing was deleted"} {
		if !strings.Contains(text, want) {
			t.Errorf("output lacks %q:\n%s", want, text)
		}
	}
	jobs, _ := s.ingStore.ListJobs(context.Background(), 10)
	if len(jobs) != 1 || jobs[0].DatasetName != DatasetName || jobs[0].Status != catalog.JobSucceeded {
		t.Fatalf("ingestion jobs: %+v", jobs)
	}
}

func TestDemoIsSafeToRerun(t *testing.T) {
	s := newStack(t)
	for i := 0; i < 3; i++ {
		var out bytes.Buffer
		if err := Run(context.Background(), s.opts(), &out); err != nil {
			t.Fatalf("run %d: %v\n%s", i, err, out.String())
		}
		if i > 0 && !strings.Contains(out.String(), "already loaded; reusing it") {
			t.Fatalf("run %d did not reuse the dataset:\n%s", i, out.String())
		}
	}
	jobs, _ := s.ingStore.ListJobs(context.Background(), 10)
	if len(jobs) != 1 {
		t.Fatalf("reruns created %d ingestion jobs, want exactly 1", len(jobs))
	}
}

func TestDemoNeverDeletesAnything(t *testing.T) {
	s := newStack(t)
	var out bytes.Buffer
	if err := Run(context.Background(), s.opts(), &out); err != nil {
		t.Fatal(err)
	}
	before := len(s.mem.Keys("lake/"))
	if before == 0 {
		t.Fatal("no dataset objects were written")
	}
	if err := Run(context.Background(), s.opts(), io.Discard); err != nil {
		t.Fatal(err)
	}
	if after := len(s.mem.Keys("lake/")); after != before {
		t.Fatalf("lake objects changed on rerun: %d -> %d", before, after)
	}
}

func TestDemoIsBoundedWhenTheStackNeverBecomesReady(t *testing.T) {
	s := newStack(t)
	s.ready = func() (int, string) {
		return 503, `{"status":"not_ready","dependencies":{"objectstore":{"ok":false,"message":"connection refused"}}}`
	}
	o := s.opts()
	o.Timeout = 400 * time.Millisecond
	start := time.Now()
	err := Run(context.Background(), o, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "did not become ready") || !strings.Contains(err.Error(), "objectstore is not ready (connection refused)") {
		t.Fatalf("err = %v", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatalf("took %s", time.Since(start))
	}
	if jobs, _ := s.ingStore.ListJobs(context.Background(), 5); len(jobs) != 0 {
		t.Fatal("the demo uploaded before the stack was ready")
	}
}

func TestDemoReportsAFailedLoad(t *testing.T) {
	s := newStack(t)
	s.objects.SetDown(true) // the upload cannot be stored
	err := Run(context.Background(), s.opts(), io.Discard)
	if err == nil {
		t.Fatal("expected an error when storage is unavailable")
	}
}

func TestDemoWarnsInsteadOfFailingWhenAnotherDatasetHasTheName(t *testing.T) {
	s := newStack(t)
	// A user's own dataset already occupies the name, with different data.
	id, _ := catalog.NewUUID()
	key := "uploads/default/" + DatasetName + "/" + id + "/input"
	_ = s.objects.Put(context.Background(), key, []byte("order_id,region,product,quantity,unit_price,order_date\n1,AMER,X,1,1.00,2025-01-01\n"))
	if _, _, err := s.ingStore.CreateConvert(context.Background(), catalog.CreateConvert{
		JobID: id, Name: DatasetName, InputRef: key, InputFormat: catalog.InputFormatCSV}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for len(s.ingStore.ReadyVersions(DatasetName)) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("pre-existing dataset never became ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	var out bytes.Buffer
	if err := Run(context.Background(), s.opts(), &out); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "differs from the bundled sample") {
		t.Fatalf("expected a warning:\n%s", out.String())
	}
}

// bridgedQueryStore resolves relations from the ingest store on demand, the way
// the real catalog serves both from one database: a version that is READY is
// immediately queryable.
type bridgedQueryStore struct {
	*catalog.MemoryQueryStore
	ing *catalog.MemoryIngestStore
}

func (b *bridgedQueryStore) sync() {
	for _, rv := range b.ing.ReadyVersions(DatasetName) {
		b.AddRelation(catalog.Relation{DatasetName: DatasetName, Version: rv.Version, ObjectPrefix: rv.ObjectPrefix, Format: "parquet"})
	}
}

func (b *bridgedQueryStore) ResolveRelation(ctx context.Context, name string) (catalog.Relation, error) {
	b.sync()
	return b.MemoryQueryStore.ResolveRelation(ctx, name)
}

func (b *bridgedQueryStore) RelationByVersionID(ctx context.Context, id string) (catalog.Relation, error) {
	b.sync()
	return b.MemoryQueryStore.RelationByVersionID(ctx, id)
}
