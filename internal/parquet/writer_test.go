package parquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
)

func TestEncodeRoundTrip(t *testing.T) {
	cols := []WriteColumn{
		{Name: "id", Type: TypeInt64},
		{Name: "amount", Type: TypeDouble, Nullable: true},
		{Name: "label", Type: TypeString, Nullable: true},
		{Name: "ok", Type: TypeBoolean, Nullable: true},
	}
	rows := [][]any{
		{int64(1), 1.5, "one", true},
		{int64(2), nil, "two", false},
		{int64(3), 3.5, nil, nil},
	}
	data, err := Encode(cols, rows)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	r := &memReader{data: data}

	res, err := Preview(context.Background(), r, "k", int64(len(data)), 10)
	if err != nil {
		t.Fatalf("Preview() error = %v", err)
	}
	if res.NumRows != 3 || len(res.Rows) != 3 {
		t.Fatalf("rows = %d/%d, want 3", res.NumRows, len(res.Rows))
	}
	if res.Rows[0]["id"] != int64(1) || res.Rows[0]["label"] != "one" || res.Rows[0]["ok"] != true {
		t.Errorf("row 0 = %v", res.Rows[0])
	}
	if res.Rows[1]["amount"] != nil || res.Rows[1]["ok"] != false {
		t.Errorf("row 1 = %v", res.Rows[1])
	}
	if res.Rows[2]["label"] != nil || res.Rows[2]["ok"] != nil || res.Rows[2]["amount"] != 3.5 {
		t.Errorf("row 2 = %v", res.Rows[2])
	}

	info, err := Inspect(context.Background(), r, "k", int64(len(data)))
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	want := []Column{
		{Name: "id", Type: "int64"},
		{Name: "amount", Type: "double", Nullable: true},
		{Name: "label", Type: "string", Nullable: true},
		{Name: "ok", Type: "boolean", Nullable: true},
	}
	if len(info.Columns) != len(want) {
		t.Fatalf("columns = %+v", info.Columns)
	}
	for i := range want {
		if info.Columns[i].Name != want[i].Name || info.Columns[i].Type != want[i].Type || info.Columns[i].Nullable != want[i].Nullable {
			t.Errorf("column %d = %+v, want %+v", i, info.Columns[i], want[i])
		}
	}
}

func TestEncodeRejectsBadInput(t *testing.T) {
	if _, err := Encode(nil, nil); !errors.Is(err, ErrWrite) {
		t.Errorf("no columns: %v", err)
	}
	if _, err := Encode([]WriteColumn{{Name: "a", Type: "uuid"}}, nil); !errors.Is(err, ErrWrite) {
		t.Errorf("bad type: %v", err)
	}
	if _, err := Encode([]WriteColumn{{Name: "a", Type: TypeInt64}}, [][]any{{nil}}); !errors.Is(err, ErrWrite) {
		t.Errorf("null in required: %v", err)
	}
	if _, err := Encode([]WriteColumn{{Name: "a", Type: TypeInt64}}, [][]any{{"x"}}); !errors.Is(err, ErrWrite) {
		t.Errorf("wrong type: %v", err)
	}
}

type bytesReader []byte

func (b bytesReader) Range(_ context.Context, _ string, off, n int64) (io.ReadCloser, error) {
	if off+n > int64(len(b)) {
		n = int64(len(b)) - off
	}
	return io.NopCloser(bytes.NewReader(b[off : off+n])), nil
}

func TestReadRowsWindow(t *testing.T) {
	cols := []WriteColumn{
		{Name: "id", Type: TypeInt64},
		{Name: "s", Type: TypeString, Nullable: true},
		{Name: "f", Type: TypeDouble, Nullable: true},
		{Name: "b", Type: TypeBoolean},
	}
	var rows [][]any
	for i := 0; i < 250; i++ {
		var s, f any
		if i%3 != 0 {
			s = fmt.Sprintf("v%d", i)
			f = float64(i) / 2
		}
		rows = append(rows, []any{int64(i), s, f, i%2 == 0})
	}
	data, err := Encode(cols, rows)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ReadRows(context.Background(), bytesReader(data), "k", int64(len(data)), 100, 50)
	if err != nil {
		t.Fatal(err)
	}
	if got.NumRows != 250 || len(got.Rows) != 50 {
		t.Fatalf("numRows=%d rows=%d", got.NumRows, len(got.Rows))
	}
	first := got.Rows[0]
	if first["id"] != int64(100) || first["s"] != "v100" || first["f"] != 50.0 || first["b"] != true {
		t.Fatalf("row 100 = %v", first)
	}
	if got.Rows[2]["s"] != nil || got.Rows[2]["id"] != int64(102) {
		t.Fatalf("row 102 should have null s: %v", got.Rows[2])
	}
	tail, err := ReadRows(context.Background(), bytesReader(data), "k", int64(len(data)), 240, 100)
	if err != nil || len(tail.Rows) != 10 {
		t.Fatalf("tail rows=%d err=%v", len(tail.Rows), err)
	}
	past, err := ReadRows(context.Background(), bytesReader(data), "k", int64(len(data)), 999, 10)
	if err != nil || len(past.Rows) != 0 {
		t.Fatalf("past end: %v %v", past, err)
	}
}
