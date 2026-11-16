package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/akshat281204/lakehouse-query-studio/internal/catalog"
	"github.com/akshat281204/lakehouse-query-studio/internal/objectstore"
)

type fakeStore struct {
	datasets    []catalog.Dataset
	versions    map[string]catalog.DatasetVersion
	schema      map[string][]catalog.SchemaColumn
	registerErr error
}

func (f *fakeStore) Register(context.Context, catalog.RegisterRecord) (catalog.Dataset, catalog.DatasetVersion, []catalog.SchemaColumn, error) {
	if f.registerErr != nil {
		return catalog.Dataset{}, catalog.DatasetVersion{}, nil, f.registerErr
	}
	return catalog.Dataset{}, catalog.DatasetVersion{}, nil, nil
}

func (f *fakeStore) List(context.Context, catalog.ListOptions) (catalog.ListResult, error) {
	return catalog.ListResult{Datasets: f.datasets, Total: len(f.datasets)}, nil
}

func (f *fakeStore) Get(_ context.Context, id string) (catalog.Dataset, error) {
	for _, d := range f.datasets {
		if d.ID == id {
			return d, nil
		}
	}
	return catalog.Dataset{}, catalog.ErrNotFound
}

func (f *fakeStore) GetLatestVersion(_ context.Context, datasetID string) (catalog.DatasetVersion, error) {
	if v, ok := f.versions[datasetID]; ok {
		return v, nil
	}
	return catalog.DatasetVersion{}, catalog.ErrNotFound
}

func (f *fakeStore) GetSchema(_ context.Context, versionID string) ([]catalog.SchemaColumn, error) {
	if cols, ok := f.schema[versionID]; ok {
		return cols, nil
	}
	return nil, nil
}

type fakeObjects struct {
	keys []objectstore.Object
}

func (f *fakeObjects) Bucket() string { return "lakehouse" }

func (f *fakeObjects) List(_ context.Context, opts objectstore.ListOptions) (objectstore.ListResult, error) {
	var out []objectstore.Object
	for _, o := range f.keys {
		if strings.HasPrefix(o.Key, opts.Prefix) {
			out = append(out, o)
		}
	}
	return objectstore.ListResult{Objects: out}, nil
}

func (f *fakeObjects) Range(context.Context, string, int64, int64) (io.ReadCloser, error) {
	return nil, objectstore.ErrNotFound
}

func TestClassify(t *testing.T) {
	cases := []struct {
		err    error
		status int
		code   string
	}{
		{catalog.ErrNotFound, http.StatusNotFound, "not_found"},
		{objectstore.ErrNotFound, http.StatusNotFound, "not_found"},
		{catalog.ErrAlreadyExists, http.StatusConflict, "already_exists"},
		{catalog.ErrInvalidArgument, http.StatusBadRequest, "invalid_argument"},
		{catalog.ErrUnavailable, http.StatusServiceUnavailable, "unavailable"},
		{objectstore.ErrAccessDenied, http.StatusServiceUnavailable, "unavailable"},
		{errors.New("boom"), http.StatusInternalServerError, "internal"},
	}
	for _, c := range cases {
		status, code := classify(c.err)
		if status != c.status || code != c.code {
			t.Errorf("classify(%v) = (%d, %q), want (%d, %q)", c.err, status, code, c.status, c.code)
		}
	}
}

func TestRegisterNilCatalog(t *testing.T) {
	h := New(nil, nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/datasets", strings.NewReader(`{"name":"x","prefix":"p"}`))
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}

func TestRegisterInvalidBody(t *testing.T) {
	store := &fakeStore{}
	objects := &fakeObjects{}
	h := New(store, objects)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/datasets", strings.NewReader(`{not json`))
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	var body errorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid error body: %v", err)
	}
	if body.Error.Code != "invalid_argument" {
		t.Errorf("error code = %q, want invalid_argument", body.Error.Code)
	}
}

func TestRegisterMissingPrefix(t *testing.T) {
	store := &fakeStore{}
	objects := &fakeObjects{} // no parquet objects
	h := New(store, objects)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/datasets", strings.NewReader(`{"name":"sales","prefix":"data/sales"}`))
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body = %s", rec.Code, rec.Body.String())
	}
	var body errorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid error body: %v", err)
	}
	if body.Error.Code != "not_found" {
		t.Errorf("error code = %q, want not_found", body.Error.Code)
	}
}

func TestList(t *testing.T) {
	now := time.Now()
	store := &fakeStore{
		datasets: []catalog.Dataset{
			{ID: "d1", NamespaceSlug: "default", Name: "sales", SourcePrefix: "data/sales", Format: "parquet", Status: "ACTIVE", CreatedAt: now, UpdatedAt: now},
		},
		versions: map[string]catalog.DatasetVersion{
			"d1": {ID: "v1", Version: 1, Status: "READY"},
		},
	}
	h := New(store, &fakeObjects{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/datasets", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}
	var body datasetListResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid body: %v", err)
	}
	if body.Total != 1 || len(body.Datasets) != 1 {
		t.Fatalf("body = %+v", body)
	}
	if body.Datasets[0].Name != "sales" || body.Datasets[0].Version != 1 {
		t.Errorf("item = %+v", body.Datasets[0])
	}
}

func TestGetNotFound(t *testing.T) {
	h := New(&fakeStore{}, &fakeObjects{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/datasets/missing", nil)
	req.SetPathValue("id", "missing")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestSchema(t *testing.T) {
	store := &fakeStore{
		datasets: []catalog.Dataset{{ID: "d1", Name: "sales"}},
		versions: map[string]catalog.DatasetVersion{"d1": {ID: "v1", Version: 1}},
		schema: map[string][]catalog.SchemaColumn{
			"v1": {{Position: 0, Name: "id", Type: "int64", Nullable: false}},
		},
	}
	h := New(store, &fakeObjects{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/datasets/d1/schema", nil)
	req.SetPathValue("id", "d1")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}
	var body schemaResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid body: %v", err)
	}
	if len(body.Columns) != 1 || body.Columns[0].Name != "id" {
		t.Errorf("body = %+v", body)
	}
}

func TestPreviewEmptyPrefix(t *testing.T) {
	store := &fakeStore{
		datasets: []catalog.Dataset{{ID: "d1", Name: "sales"}},
		versions: map[string]catalog.DatasetVersion{"d1": {ID: "v1", Version: 1, ObjectPrefix: "data/sales"}},
	}
	h := New(store, &fakeObjects{}) // no parquet objects
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/datasets/d1/preview", nil)
	req.SetPathValue("id", "d1")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}
