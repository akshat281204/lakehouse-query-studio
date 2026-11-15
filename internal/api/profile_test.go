package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/akshat281204/lakehouse-query-studio/internal/catalog"
	"github.com/akshat281204/lakehouse-query-studio/internal/objectstore"
	"github.com/akshat281204/lakehouse-query-studio/internal/parquet"
)

var profileCols = []parquet.WriteColumn{
	{Name: "id", Type: parquet.TypeInt64},
	{Name: "price", Type: parquet.TypeDouble, Nullable: true},
	{Name: "region", Type: parquet.TypeString},
	{Name: "ok", Type: parquet.TypeBoolean},
}

func profileEnv(t *testing.T, files ...[][]any) (http.Handler, *objectstore.Memory) {
	t.Helper()
	mem := objectstore.NewMemory()
	for i, rows := range files {
		data, err := parquet.Encode(profileCols, rows)
		if err != nil {
			t.Fatal(err)
		}
		_ = mem.Put(context.Background(), fmt.Sprintf("lake/default/sales/v1/part-%03d.parquet", i), data)
	}
	st := &fakeStore{
		datasets: []catalog.Dataset{{ID: "d1", Name: "sales"}},
		versions: map[string]catalog.DatasetVersion{"d1": {ID: "v1", DatasetID: "d1", Version: 1, Status: catalog.VersionReady, ObjectPrefix: "lake/default/sales/v1/"}},
		schema: map[string][]catalog.SchemaColumn{"v1": {
			{Position: 0, Name: "id", Type: "int64"}, {Position: 1, Name: "price", Type: "double", Nullable: true},
			{Position: 2, Name: "region", Type: "string"}, {Position: 3, Name: "ok", Type: "boolean"},
		}},
	}
	return New(st, mem), mem
}

func getProfile(t *testing.T, h http.Handler, id string) (int, profileResponse) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/api/v1/datasets/"+id+"/profile", nil))
	var p profileResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &p)
	return rec.Code, p
}

func col(p profileResponse, name string) profileColumn {
	for _, c := range p.Columns {
		if c.Name == name {
			return c
		}
	}
	return profileColumn{}
}

func TestProfileMergesFooterStatisticsAcrossFiles(t *testing.T) {
	h, _ := profileEnv(t,
		[][]any{{int64(5), 2.5, "emea", true}, {int64(9), nil, "amer", true}},
		[][]any{{int64(-3), 10.25, "apac", false}},
	)
	code, p := getProfile(t, h, "d1")
	if code != 200 || p.Rows != 3 || p.Files != 2 {
		t.Fatalf("%d %+v", code, p)
	}
	if c := col(p, "id"); !c.HasRange || *c.Min != "-3" || *c.Max != "9" || *c.NullCount != 0 {
		t.Fatalf("id: %+v", c)
	}
	if c := col(p, "price"); !c.HasRange || *c.Min != "2.5" || *c.Max != "10.25" || *c.NullCount != 1 {
		t.Fatalf("price: %+v", c)
	}
	if c := col(p, "region"); *c.Min != "amer" || *c.Max != "emea" {
		t.Fatalf("region: %+v", c)
	}
	if c := col(p, "ok"); *c.Min != "false" || *c.Max != "true" {
		t.Fatalf("ok: %+v", c)
	}
}

// If any file's footer cannot be read, no range is reported: a range that
// silently ignores a file would be wrong.
func TestProfileReportsNoRangeWhenAFileCannotBeRead(t *testing.T) {
	h, mem := profileEnv(t, [][]any{{int64(1), 1.0, "a", true}})
	_ = mem.Put(context.Background(), "lake/default/sales/v1/part-bad.parquet", []byte("this is not parquet at all"))
	code, p := getProfile(t, h, "d1")
	if code != 200 || p.Files != 2 || !strings.Contains(p.Note, "1 of 2") {
		t.Fatalf("%d %+v", code, p)
	}
	for _, c := range p.Columns {
		if c.HasRange || c.Min != nil || c.Max != nil {
			t.Fatalf("column %s reports a range from partial data: %+v", c.Name, c)
		}
	}
	if len(p.Columns) != 4 || p.Columns[0].Name != "id" {
		t.Fatalf("schema must still be listed: %+v", p.Columns)
	}
}

func TestProfileOfAHugeDatasetIsSchemaOnly(t *testing.T) {
	rows := [][]any{{int64(1), 1.0, "a", true}}
	files := make([][][]any, maxProfileFiles+1)
	for i := range files {
		files[i] = rows
	}
	h, _ := profileEnv(t, files...)
	code, p := getProfile(t, h, "d1")
	if code != 200 || p.Files != maxProfileFiles+1 || !strings.Contains(p.Note, "schema only") || col(p, "id").HasRange {
		t.Fatalf("%d files=%d note=%q", code, p.Files, p.Note)
	}
}

func TestProfileClipsLongStringsAndErrorsForUnknownDatasets(t *testing.T) {
	long := strings.Repeat("é", 100) // 200 bytes: within the writer's statistics cap, over the display clip
	h, _ := profileEnv(t, [][]any{{int64(1), 1.0, long, true}})
	_, p := getProfile(t, h, "d1")
	if c := col(p, "region"); !c.Truncated || len([]rune(*c.Min)) != maxProfileTextLen {
		t.Fatalf("clip: %+v", c)
	}
	if code, _ := getProfile(t, h, "nope"); code != 404 {
		t.Fatalf("unknown dataset: %d", code)
	}
	rec := httptest.NewRecorder()
	New(nil, nil).ServeHTTP(rec, httptest.NewRequest("GET", "/api/v1/datasets/d1/profile", nil))
	if rec.Code != 503 {
		t.Fatalf("no catalog: %d", rec.Code)
	}
}

func TestProfileOfAnEmptyPrefix(t *testing.T) {
	h, _ := profileEnv(t)
	code, p := getProfile(t, h, "d1")
	if code != 200 || p.Files != 0 || p.Note == "" || len(p.Columns) != 4 {
		t.Fatalf("%d %+v", code, p)
	}
}

func TestProfileNoteSaysWhyAColumnHasNoRange(t *testing.T) {
	// a type the profile does not summarise (date) vs a file with no statistics
	_, mem := profileEnv(t, [][]any{{int64(1), 1.0, "a", true}})
	st := &fakeStore{
		datasets: []catalog.Dataset{{ID: "d1", Name: "sales"}},
		versions: map[string]catalog.DatasetVersion{"d1": {ID: "v1", DatasetID: "d1", Version: 1, Status: catalog.VersionReady, ObjectPrefix: "lake/default/sales/v1/"}},
		schema: map[string][]catalog.SchemaColumn{"v1": {
			{Position: 0, Name: "id", Type: "int64"}, {Position: 1, Name: "price", Type: "double", Nullable: true},
			{Position: 2, Name: "region", Type: "string"}, {Position: 3, Name: "ok", Type: "boolean"},
			{Position: 4, Name: "day", Type: "date"},
		}},
	}
	rec := httptest.NewRecorder()
	New(st, mem).ServeHTTP(rec, httptest.NewRequest("GET", "/api/v1/datasets/d1/profile", nil))
	var p profileResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &p)
	if !strings.Contains(p.Note, "1 column(s) have a type whose range is not summarised") || strings.Contains(p.Note, "written before statistics") {
		t.Fatalf("note: %q", p.Note)
	}
}
