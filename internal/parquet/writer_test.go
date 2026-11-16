package parquet

import (
	"context"
	"errors"
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
