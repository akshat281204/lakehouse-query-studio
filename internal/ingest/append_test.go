package ingest

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/akshat281204/lakehouse-query-studio/internal/catalog"
	"github.com/akshat281204/lakehouse-query-studio/internal/parquet"
)

func parquetFile(t *testing.T, cols []parquet.WriteColumn, rows [][]any) []byte {
	t.Helper()
	b, err := parquet.Encode(cols, rows)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

var salesCols = []parquet.WriteColumn{
	{Name: "id", Type: parquet.TypeInt64},
	{Name: "region", Type: parquet.TypeString},
	{Name: "amount", Type: parquet.TypeDouble, Nullable: true},
}

func (h *harness) submitMode(t *testing.T, name, format string, body []byte, mode string, partitions []string) (catalog.Job, error) {
	t.Helper()
	id, _ := catalog.NewUUID()
	key := "uploads/default/" + name + "/" + id + "/input"
	if err := h.objects.Put(context.Background(), key, body); err != nil {
		t.Fatal(err)
	}
	job, _, err := h.store.CreateConvert(context.Background(), catalog.CreateConvert{
		JobID: id, Name: name, InputRef: key, InputFormat: format, PartitionColumns: partitions, Mode: mode,
	})
	return job, err
}

func (h *harness) job(t *testing.T, id string) catalog.Job {
	t.Helper()
	j, err := h.store.GetJob(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func TestParseParquetAcceptsARealFileAndReportsItsSchema(t *testing.T) {
	data := parquetFile(t, salesCols, [][]any{{int64(1), "us", 1.5}, {int64(2), "eu", nil}})
	info, err := ParseParquet(data, DefaultLimits)
	if err != nil || info.Rows != 2 || len(info.Columns) != 3 {
		t.Fatalf("%+v %v", info, err)
	}
	if c := info.Columns[2]; c.Name != "amount" || c.Type != "double" || !c.Nullable {
		t.Fatalf("amount: %+v", c)
	}
}

func TestParseParquetRejectsWhatIsNotAUsableParquetFile(t *testing.T) {
	good := parquetFile(t, salesCols, [][]any{{int64(1), "us", 1.5}})
	dup := parquetFile(t, []parquet.WriteColumn{{Name: "a", Type: parquet.TypeInt64}, {Name: "A", Type: parquet.TypeInt64}}, [][]any{{int64(1), int64(2)}})
	spaced := parquetFile(t, []parquet.WriteColumn{{Name: " a", Type: parquet.TypeInt64}}, [][]any{{int64(1)}})
	for name, data := range map[string][]byte{
		"csv text":        []byte("id,region\n1,us\n"),
		"empty":           nil,
		"truncated":       good[:len(good)/2],
		"head only":       append([]byte("PAR1"), bytes.Repeat([]byte{0}, 40)...),
		"garbage footer":  append(append([]byte("PAR1"), bytes.Repeat([]byte{7}, 64)...), []byte("\x40\x00\x00\x00PAR1")...),
		"duplicate names": dup,
		"padded name":     spaced,
	} {
		if _, err := ParseParquet(data, DefaultLimits); !IsUserError(err) {
			t.Errorf("%s: err = %v, want UserError", name, err)
		}
	}
	lim := DefaultLimits
	lim.MaxBytes = 50
	if _, err := ParseParquet(good, lim); !IsUserError(err) {
		t.Errorf("over the size limit: %v", err)
	}
	lim = DefaultLimits
	lim.MaxColumns = 2
	if _, err := ParseParquet(good, lim); !IsUserError(err) {
		t.Errorf("too many columns: %v", err)
	}
	// errors never echo anything from the file
	_, err := ParseParquet([]byte("secret-value,1\n"), DefaultLimits)
	if err == nil || strings.Contains(err.Error(), "secret-value") {
		t.Fatalf("error echoes content: %v", err)
	}
}

func TestDirectParquetImportStoresTheFileUnchanged(t *testing.T) {
	h := newHarness()
	data := parquetFile(t, salesCols, [][]any{{int64(1), "us", 1.5}, {int64(2), "eu", 2.5}, {int64(3), "us", nil}})
	job, err := h.submitMode(t, "sales", catalog.InputFormatParquet, data, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	h.runAll(t)
	if got := h.job(t, job.ID); got.Status != catalog.JobSucceeded {
		t.Fatalf("status %s (%s)", got.Status, got.ErrorSummary)
	}
	v := h.store.ReadyVersions("sales")[0]
	if *v.RowCount != 3 || len(v.BaseSegments) != 0 || len(v.PartitionColumns) != 0 {
		t.Fatalf("version: %+v", v)
	}
	keys := h.objects.keysWithPrefix(v.ObjectPrefix)
	if len(keys) != 1 || !bytes.Equal(h.objects.data[keys[0]], data) {
		t.Fatalf("the imported file must be stored byte for byte: %v", keys)
	}
	cols := h.store.Columns(v.ID)
	if len(cols) != 3 || cols[0].Type != "int64" || cols[1].Type != "string" || !cols[2].Nullable {
		t.Fatalf("schema: %+v", cols)
	}
}

func TestAnInvalidParquetUploadFailsTheJobWithoutRetryAndLeavesNothingBehind(t *testing.T) {
	h := newHarness()
	job, _ := h.submitMode(t, "sales", catalog.InputFormatParquet, []byte("not parquet at all"), "", nil)
	h.runAll(t)
	got := h.job(t, job.ID)
	if got.Status != catalog.JobFailed || got.FailureReason != ReasonInvalidInput || got.Retryable {
		t.Fatalf("job: %+v", got)
	}
	if n := len(h.objects.keysWithPrefix("lake/")); n != 0 {
		t.Fatalf("left %d objects under lake/", n)
	}
}

func baseDataset(t *testing.T, h *harness, partitions []string) {
	t.Helper()
	if _, err := h.submitMode(t, "sales", catalog.InputFormatCSV, []byte("id,region,amount\n1,us,1.5\n2,eu,2.5\n"), "", partitions); err != nil {
		t.Fatal(err)
	}
	h.runAll(t)
}

func TestAppendingCSVAddsASegmentAndWidensIntegers(t *testing.T) {
	h := newHarness()
	baseDataset(t, h, nil)
	// "amount" has only whole numbers here, so it is inferred as int64: it must be widened.
	job, err := h.submitMode(t, "sales", catalog.InputFormatCSV, []byte("id,region,amount\n3,us,10\n4,eu,20\n5,us,\n"), catalog.ModeAppend, nil)
	if err != nil {
		t.Fatal(err)
	}
	h.runAll(t)
	if got := h.job(t, job.ID); got.Status != catalog.JobSucceeded {
		t.Fatalf("status %s (%s)", got.Status, got.ErrorSummary)
	}
	vs := h.store.ReadyVersions("sales")
	if len(vs) != 2 {
		t.Fatalf("versions: %d", len(vs))
	}
	v1, v2 := vs[0], vs[1]
	if *v2.RowCount != 5 || len(v2.BaseSegments) != 1 || v2.BaseSegments[0] != v1.ObjectPrefix {
		t.Fatalf("v2: rows=%d segments=%v", *v2.RowCount, v2.BaseSegments)
	}
	if *v1.RowCount != 2 || len(v1.BaseSegments) != 0 {
		t.Fatalf("v1 changed: %+v", v1)
	}
	// the new segment's file stores amount as a double
	keys := h.objects.keysWithPrefix(v2.ObjectPrefix)
	if len(keys) != 1 {
		t.Fatalf("segment files: %v", keys)
	}
	sch, err := parquet.Inspect(context.Background(), &memReader{data: h.objects.data[keys[0]]}, "k", int64(len(h.objects.data[keys[0]])))
	if err != nil || sch.Columns[2].Type != "double" || sch.NumRows != 3 {
		t.Fatalf("segment schema: %+v %v", sch, err)
	}
	// and the old version's files are untouched
	if n := len(h.objects.keysWithPrefix(v1.ObjectPrefix)); n != 1 {
		t.Fatalf("v1 files: %d", n)
	}
	if cols := h.store.Columns(v2.ID); len(cols) != 3 || cols[2].Type != "double" || !cols[2].Nullable {
		t.Fatalf("merged schema: %+v", cols)
	}
}

func TestAppendReordersColumnsAndFillsAMissingNullableOne(t *testing.T) {
	h := newHarness()
	// amount has a blank here, so it is nullable and may be left out of an append
	if _, err := h.submitMode(t, "sales", catalog.InputFormatCSV, []byte("id,region,amount\n1,us,1.5\n2,eu,\n"), "", nil); err != nil {
		t.Fatal(err)
	}
	h.runAll(t)
	job, _ := h.submitMode(t, "sales", catalog.InputFormatCSV, []byte("region,id\nap,9\n"), catalog.ModeAppend, nil) // no amount
	h.runAll(t)
	if got := h.job(t, job.ID); got.Status != catalog.JobSucceeded {
		t.Fatalf("status %s (%s)", got.Status, got.ErrorSummary)
	}
	v2 := h.store.ReadyVersions("sales")[1]
	data := h.objects.data[h.objects.keysWithPrefix(v2.ObjectPrefix)[0]]
	res, err := parquet.ReadRows(context.Background(), &memReader{data: data}, "k", int64(len(data)), 0, 10)
	if err != nil || len(res.Rows) != 1 {
		t.Fatalf("%+v %v", res, err)
	}
	if res.Rows[0]["id"] != int64(9) || res.Rows[0]["region"] != "ap" || res.Rows[0]["amount"] != nil {
		t.Fatalf("row: %+v", res.Rows[0])
	}
}

func TestAnIncompatibleAppendFailsWithoutRetryAndLeavesTheDatasetAlone(t *testing.T) {
	h := newHarness()
	baseDataset(t, h, nil)
	for name, csv := range map[string]string{
		"type change":  "id,region,amount\nabc,us,1.5\n",
		"extra column": "id,region,amount,extra\n1,us,1.5,x\n",
		"case differs": "ID,region,amount\n1,us,1.5\n",
		"missing id":   "region,amount\nus,1.5\n",
	} {
		job, err := h.submitMode(t, "sales", catalog.InputFormatCSV, []byte(csv), catalog.ModeAppend, nil)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		h.runAll(t)
		got := h.job(t, job.ID)
		if got.Status != catalog.JobFailed || got.FailureReason != ReasonInvalidInput || got.Retryable {
			t.Errorf("%s: %+v", name, got)
		}
		if !strings.Contains(got.ErrorSummary, "does not match the dataset") {
			t.Errorf("%s: summary %q", name, got.ErrorSummary)
		}
	}
	if vs := h.store.ReadyVersions("sales"); len(vs) != 1 {
		t.Fatalf("a failed append changed the dataset: %d versions", len(vs))
	}
	if n := len(h.objects.keysWithPrefix("lake/default/sales/version=2")) + len(h.objects.keysWithPrefix("lake/default/sales/version=3")); n != 0 {
		t.Fatalf("failed appends left %d objects under lake/", n)
	}
}

func TestParquetAppendRequiresExactlyMatchingTypes(t *testing.T) {
	h := newHarness()
	baseDataset(t, h, nil)
	good := parquetFile(t, salesCols, [][]any{{int64(7), "ap", 7.5}})
	if _, err := h.submitMode(t, "sales", catalog.InputFormatParquet, good, catalog.ModeAppend, nil); err != nil {
		t.Fatal(err)
	}
	// amount as int64 in the file: stored as it is, so it must not be accepted for a double column
	bad := parquetFile(t, []parquet.WriteColumn{{Name: "id", Type: parquet.TypeInt64}, {Name: "region", Type: parquet.TypeString}, {Name: "amount", Type: parquet.TypeInt64, Nullable: true}}, [][]any{{int64(8), "ap", int64(1)}})
	badJob, _ := h.submitMode(t, "sales", catalog.InputFormatParquet, bad, catalog.ModeAppend, nil)
	h.runAll(t)
	if got := h.job(t, badJob.ID); got.Status != catalog.JobFailed || !strings.Contains(got.ErrorSummary, `column "amount" is int64`) {
		t.Fatalf("mismatched parquet: %+v", got)
	}
	vs := h.store.ReadyVersions("sales")
	if len(vs) != 2 || *vs[1].RowCount != 3 { // original 2 + the good file's 1
		t.Fatalf("versions: %+v", vs)
	}
}

func TestAppendToAPartitionedDatasetKeepsItsLayout(t *testing.T) {
	h := newHarness()
	baseDataset(t, h, []string{"region"})
	job, err := h.submitMode(t, "sales", catalog.InputFormatCSV, []byte("id,region,amount\n3,us,3.5\n4,ap,4.5\n"), catalog.ModeAppend, []string{"ignored"})
	if err != nil {
		t.Fatal(err)
	}
	h.runAll(t)
	if got := h.job(t, job.ID); got.Status != catalog.JobSucceeded {
		t.Fatalf("status %s (%s)", got.Status, got.ErrorSummary)
	}
	v2 := h.store.ReadyVersions("sales")[1]
	if len(v2.PartitionColumns) != 1 || v2.PartitionColumns[0] != "region" {
		t.Fatalf("partition columns: %v", v2.PartitionColumns)
	}
	keys := h.objects.keysWithPrefix(v2.ObjectPrefix)
	if len(keys) != 2 || !strings.Contains(strings.Join(keys, " "), "region=ap/") {
		t.Fatalf("appended files must use the dataset's partition directories: %v", keys)
	}
}

// Two appends based on the same version: the one that finishes second must not
// publish (it would drop the first one's rows); it fails retryably and its
// objects are removed, and a retry then appends to the new latest version.
func TestTheSecondOfTwoConcurrentAppendsFailsRetryablyAndARetrySucceeds(t *testing.T) {
	h := newHarness()
	baseDataset(t, h, nil)
	a, _ := h.submitMode(t, "sales", catalog.InputFormatCSV, []byte("id,region,amount\n10,us,1.5\n"), catalog.ModeAppend, nil)
	b, _ := h.submitMode(t, "sales", catalog.InputFormatCSV, []byte("id,region,amount\n20,eu,2.5\n21,eu,2.5\n"), catalog.ModeAppend, nil)
	h.runAll(t)

	if got := h.job(t, a.ID); got.Status != catalog.JobSucceeded {
		t.Fatalf("first append: %+v", got)
	}
	gb := h.job(t, b.ID)
	if gb.Status != catalog.JobFailed || gb.FailureReason != ReasonBaseChanged || !gb.Retryable {
		t.Fatalf("second append: %+v", gb)
	}
	if n := len(h.objects.keysWithPrefix("lake/default/sales/version=3")); n != 0 {
		t.Fatalf("the refused append left %d objects", n)
	}
	if vs := h.store.ReadyVersions("sales"); len(vs) != 2 || *vs[1].RowCount != 3 {
		t.Fatalf("after the race: %+v", vs)
	}

	retry, err := h.store.Retry(context.Background(), b.ID, freshJobID(t))
	if err != nil || retry.Mode != catalog.ModeAppend {
		t.Fatalf("retry: %+v %v", retry, err)
	}
	h.runAll(t)
	if got := h.job(t, retry.ID); got.Status != catalog.JobSucceeded {
		t.Fatalf("retry: %+v", got)
	}
	vs := h.store.ReadyVersions("sales")
	if len(vs) != 3 || *vs[2].RowCount != 5 || len(vs[2].BaseSegments) != 2 {
		t.Fatalf("final: %d versions, last %+v", len(vs), vs[len(vs)-1])
	}
}

func TestAnOptionalColumnLeftEmptyInTheNewBatchStillAppends(t *testing.T) {
	h := newHarness()
	if _, err := h.submitMode(t, "sales", catalog.InputFormatCSV, []byte("id,region,amount\n1,us,1.5\n2,eu,\n"), "", nil); err != nil {
		t.Fatal(err)
	}
	h.runAll(t)
	// every amount is empty here, so a CSV reader would call the column a string
	job, _ := h.submitMode(t, "sales", catalog.InputFormatCSV, []byte("id,region,amount\n3,us,\n4,eu,\n"), catalog.ModeAppend, nil)
	h.runAll(t)
	if got := h.job(t, job.ID); got.Status != catalog.JobSucceeded {
		t.Fatalf("status %s (%s)", got.Status, got.ErrorSummary)
	}
	v2 := h.store.ReadyVersions("sales")[1]
	if cols := h.store.Columns(v2.ID); cols[2].Type != "double" || !cols[2].Nullable {
		t.Fatalf("schema: %+v", cols)
	}
	data := h.objects.data[h.objects.keysWithPrefix(v2.ObjectPrefix)[0]]
	res, err := parquet.ReadRows(context.Background(), &memReader{data: data}, "k", int64(len(data)), 0, 10)
	if err != nil || len(res.Rows) != 2 || res.Rows[0]["amount"] != nil {
		t.Fatalf("%+v %v", res, err)
	}
}

func freshJobID(t *testing.T) string {
	t.Helper()
	id, err := catalog.NewUUID()
	if err != nil {
		t.Fatal(err)
	}
	return id
}
