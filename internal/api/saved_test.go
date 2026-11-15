package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/akshat281204/lakehouse-query-studio/internal/catalog"
	"github.com/akshat281204/lakehouse-query-studio/internal/query"
)

func newSavedHandler(t *testing.T) (http.Handler, *catalog.MemorySavedQueryStore) {
	t.Helper()
	v, err := query.NewValidator()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = v.Close() })
	st := catalog.NewMemorySavedQueryStore()
	return New(nil, nil, WithSavedQueries(SavedDeps{Store: st, Validator: v})), st
}

func savedDo(h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	var rd *strings.Reader
	if body != "" {
		rd = strings.NewReader(body)
	} else {
		rd = strings.NewReader("")
	}
	req := httptest.NewRequest(method, path, rd)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestSavedQueriesCRUD(t *testing.T) {
	h, _ := newSavedHandler(t)
	rec := savedDo(h, "POST", "/api/v1/saved-queries", `{"name":"Revenue","description":"by region","statement":"SELECT region, sum(x) FROM sales GROUP BY region"}`)
	if rec.Code != 201 || rec.Header().Get("Location") == "" {
		t.Fatalf("create: %d %s", rec.Code, rec.Body.String())
	}
	var q catalog.SavedQuery
	_ = json.Unmarshal(rec.Body.Bytes(), &q)
	if q.ID == "" || q.Name != "Revenue" || q.Description != "by region" {
		t.Fatalf("created: %+v", q)
	}

	if rec = savedDo(h, "POST", "/api/v1/saved-queries", `{"name":"revenue","statement":"SELECT 1"}`); rec.Code != 409 || qErrCode(t, rec) != "already_exists" {
		t.Fatalf("duplicate name: %d %s", rec.Code, rec.Body.String())
	}

	rec = savedDo(h, "GET", "/api/v1/saved-queries", "")
	var list savedListResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &list)
	if rec.Code != 200 || list.Total != 1 || len(list.SavedQueries) != 1 || list.SavedQueries[0].ID != q.ID {
		t.Fatalf("list: %d %s", rec.Code, rec.Body.String())
	}
	if rec = savedDo(h, "GET", "/api/v1/saved-queries/"+q.ID, ""); rec.Code != 200 {
		t.Fatalf("get: %d", rec.Code)
	}

	rec = savedDo(h, "PUT", "/api/v1/saved-queries/"+q.ID, `{"name":"Revenue v2","description":"","statement":"SELECT 2"}`)
	var up catalog.SavedQuery
	_ = json.Unmarshal(rec.Body.Bytes(), &up)
	if rec.Code != 200 || up.Name != "Revenue v2" || up.Statement != "SELECT 2" {
		t.Fatalf("update: %d %s", rec.Code, rec.Body.String())
	}

	if rec = savedDo(h, "DELETE", "/api/v1/saved-queries/"+q.ID, ""); rec.Code != 204 || rec.Body.Len() != 0 {
		t.Fatalf("delete: %d %q", rec.Code, rec.Body.String())
	}
	if rec = savedDo(h, "GET", "/api/v1/saved-queries/"+q.ID, ""); rec.Code != 404 {
		t.Fatalf("after delete: %d", rec.Code)
	}
	if rec = savedDo(h, "DELETE", "/api/v1/saved-queries/"+q.ID, ""); rec.Code != 404 {
		t.Fatalf("second delete: %d", rec.Code)
	}
	if rec = savedDo(h, "PUT", "/api/v1/saved-queries/"+q.ID, `{"name":"x","statement":"SELECT 1"}`); rec.Code != 404 {
		t.Fatalf("update of a deleted query: %d", rec.Code)
	}
}

// A saved query goes through the same safety boundary as a submitted one, so
// the library can never hold SQL the engine would refuse.
func TestSavedQueriesRefuseUnsafeSQL(t *testing.T) {
	h, st := newSavedHandler(t)
	for _, stmt := range []string{
		"DROP TABLE sales",
		"SELECT * FROM read_csv('/etc/passwd')",
		"COPY sales TO '/tmp/x'",
		"SELECT 1; SELECT 2",
		"ATTACH 'x.db'",
	} {
		b, _ := json.Marshal(map[string]string{"name": "bad", "statement": stmt})
		rec := savedDo(h, "POST", "/api/v1/saved-queries", string(b))
		if rec.Code != 400 || qErrCode(t, rec) != "query_denied" {
			t.Errorf("%q: %d %s", stmt, rec.Code, rec.Body.String())
		}
	}
	if _, total, _ := st.List(context.Background(), 0, 10); total != 0 {
		t.Fatalf("an unsafe statement was stored (%d)", total)
	}
	// Updating a safe query to unsafe SQL is refused too and leaves it unchanged.
	rec := savedDo(h, "POST", "/api/v1/saved-queries", `{"name":"ok","statement":"SELECT 1"}`)
	var q catalog.SavedQuery
	_ = json.Unmarshal(rec.Body.Bytes(), &q)
	if rec = savedDo(h, "PUT", "/api/v1/saved-queries/"+q.ID, `{"name":"ok","statement":"DROP TABLE sales"}`); rec.Code != 400 {
		t.Fatalf("unsafe update: %d", rec.Code)
	}
	if got, _ := st.Get(context.Background(), q.ID); got.Statement != "SELECT 1" {
		t.Fatalf("unsafe update changed the stored statement: %q", got.Statement)
	}
}

func TestSavedQueriesRejectBadInput(t *testing.T) {
	h, _ := newSavedHandler(t)
	for name, body := range map[string]string{
		"not json":      `nope`,
		"unknown field": `{"name":"a","statement":"SELECT 1","admin":true}`,
		"no name":       `{"name":" ","statement":"SELECT 1"}`,
		"no statement":  `{"name":"a","statement":""}`,
		"long name":     `{"name":"` + strings.Repeat("x", 121) + `","statement":"SELECT 1"}`,
		"huge body":     `{"name":"a","statement":"` + strings.Repeat("x", 3*query.MaxSQLBytes) + `"}`,
	} {
		rec := savedDo(h, "POST", "/api/v1/saved-queries", body)
		if rec.Code != 400 {
			t.Errorf("%s: %d %s", name, rec.Code, rec.Body.String())
		}
	}
	if rec := savedDo(h, "GET", "/api/v1/saved-queries/not-a-uuid", ""); rec.Code != 404 && rec.Code != 400 {
		t.Errorf("malformed id: %d", rec.Code)
	}
}

func TestSavedQueriesUnavailableWithoutAStore(t *testing.T) {
	h := New(nil, nil)
	for _, c := range [][2]string{{"GET", "/api/v1/saved-queries"}, {"POST", "/api/v1/saved-queries"}, {"DELETE", "/api/v1/saved-queries/x"}} {
		if rec := savedDo(h, c[0], c[1], `{}`); rec.Code != 503 {
			t.Errorf("%v: %d", c, rec.Code)
		}
	}
}
