package parquet

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"math"
	"testing"
)

// ---- test helpers: a minimal Thrift compact-protocol writer ----

type cw struct {
	buf    []byte
	lastID int16
	stack  []int16
}

func (w *cw) varint(v uint64) {
	for v >= 0x80 {
		w.buf = append(w.buf, byte(v)|0x80)
		v >>= 7
	}
	w.buf = append(w.buf, byte(v))
}

func (w *cw) zigzag(v int64) { w.varint(uint64(v<<1) ^ uint64(v>>63)) }

func (w *cw) i32(v int32) { w.zigzag(int64(v)) }
func (w *cw) i64(v int64) { w.zigzag(v) }
func (w *cw) b(b byte)    { w.buf = append(w.buf, b) }

func (w *cw) str(s string) {
	w.varint(uint64(len(s)))
	w.buf = append(w.buf, s...)
}

func (w *cw) field(id int16, typ byte) {
	delta := id - w.lastID
	if delta > 0 && delta <= 15 {
		w.b(byte(delta)<<4 | typ)
	} else {
		w.b(typ)
		w.zigzag(int64(id))
	}
	w.lastID = id
}

func (w *cw) stop() {
	w.b(tStop)
	w.lastID = w.stack[len(w.stack)-1]
	w.stack = w.stack[:len(w.stack)-1]
}

func (w *cw) listHeader(size int, elem byte) {
	if size < 15 {
		w.b(byte(size)<<4 | elem)
	} else {
		w.b(0xF0 | elem)
		w.varint(uint64(size))
	}
}

func (w *cw) begin() {
	w.stack = append(w.stack, w.lastID)
	w.lastID = 0
}

func encodeSchemaElement(w *cw, e schemaElement) {
	w.begin()
	if e.typ != nil {
		w.field(1, tI32)
		w.i32(*e.typ)
	}
	if e.typeLength != nil {
		w.field(2, tI32)
		w.i32(*e.typeLength)
	}
	w.field(3, tI32)
	w.i32(e.repetitionType)
	w.field(4, tBinary)
	w.str(e.name)
	if e.numChildren != nil {
		w.field(5, tI32)
		w.i32(*e.numChildren)
	}
	if e.convertedType != nil {
		w.field(6, tI32)
		w.i32(*e.convertedType)
	}
	w.stop()
}

func encodeColumnMetaData(w *cw, m columnMetaData) {
	w.begin()
	w.field(1, tI32)
	w.i32(m.typ)
	w.field(4, tI32)
	w.i32(m.codec)
	w.field(9, tI64)
	w.i64(m.dataPageOffset)
	if m.hasDictionaryPage {
		w.field(11, tI64)
		w.i64(m.dictionaryPageOffset)
	}
	w.stop()
}

func encodeColumnChunk(w *cw, c columnChunk) {
	w.begin()
	w.field(3, tStruct)
	encodeColumnMetaData(w, c.meta)
	w.stop()
}

func encodeRowGroup(w *cw, g rowGroup) {
	w.begin()
	w.field(1, tList)
	w.listHeader(len(g.columns), tStruct)
	for _, c := range g.columns {
		encodeColumnChunk(w, c)
	}
	w.field(3, tI64)
	w.i64(g.numRows)
	w.stop()
}

func encodeFileMetaData(w *cw, m fileMetaData) {
	w.begin()
	w.field(1, tI32)
	w.i32(1)
	w.field(2, tList)
	w.listHeader(len(m.schema), tStruct)
	for _, e := range m.schema {
		encodeSchemaElement(w, e)
	}
	w.field(3, tI64)
	w.i64(m.numRows)
	w.field(4, tList)
	w.listHeader(len(m.rowGroups), tStruct)
	for _, g := range m.rowGroups {
		encodeRowGroup(w, g)
	}
	w.stop()
}

func int32Ptr(v int32) *int32 { return &v }

func plainEncode(typ int32, values []any) []byte {
	var out []byte
	for _, v := range values {
		switch typ {
		case typeInt32:
			var b [4]byte
			binary.LittleEndian.PutUint32(b[:], uint32(v.(int32)))
			out = append(out, b[:]...)
		case typeInt64:
			var b [8]byte
			binary.LittleEndian.PutUint64(b[:], uint64(v.(int64)))
			out = append(out, b[:]...)
		case typeDouble:
			var b [8]byte
			binary.LittleEndian.PutUint64(b[:], math.Float64bits(v.(float64)))
			out = append(out, b[:]...)
		case typeByteArray:
			s := v.(string)
			var b [4]byte
			binary.LittleEndian.PutUint32(b[:], uint32(len(s)))
			out = append(out, b[:]...)
			out = append(out, s...)
		}
	}
	return out
}

func rleEncodeRuns(bitWidth int, values []uint32) []byte {
	var out []byte
	var runValue uint32
	runLen := 0
	flush := func() {
		if runLen == 0 {
			return
		}
		out = appendVarint(out, uint64(runLen)<<1)
		nBytes := (bitWidth + 7) / 8
		for i := 0; i < nBytes; i++ {
			out = append(out, byte(runValue>>uint(8*i)))
		}
		runLen = 0
	}
	for _, v := range values {
		if runLen > 0 && v == runValue {
			runLen++
			continue
		}
		if runLen > 0 {
			flush()
		}
		runValue = v
		runLen = 1
	}
	flush()
	return out
}

func appendVarint(b []byte, v uint64) []byte {
	for v >= 0x80 {
		b = append(b, byte(v)|0x80)
		v >>= 7
	}
	return append(b, byte(v))
}

func buildDataPage(numValues int32, encoding int32, levelBytes, valueBytes []byte) []byte {
	w := &cw{}
	w.begin()
	w.field(1, tI32)
	w.i32(pageDataPage)
	w.field(2, tI32)
	w.i32(int32(len(levelBytes) + len(valueBytes)))
	w.field(3, tI32)
	w.i32(int32(len(levelBytes) + len(valueBytes)))
	w.field(5, tStruct)
	w.begin()
	w.field(1, tI32)
	w.i32(numValues)
	w.field(2, tI32)
	w.i32(encoding)
	w.field(3, tI32)
	w.i32(encodingRLE)
	w.field(4, tI32)
	w.i32(encodingRLE)
	w.stop()
	w.stop()

	body := append([]byte(nil), levelBytes...)
	body = append(body, valueBytes...)
	return append(w.buf, body...)
}

type testColumn struct {
	name     string
	typ      int32
	nullable bool
}

func buildFile(cols []testColumn, rows [][]any) []byte {
	schema := []schemaElement{
		{repetitionType: 0, name: "root", numChildren: int32Ptr(int32(len(cols)))},
	}
	for _, c := range cols {
		rep := int32(0)
		if c.nullable {
			rep = 1
		}
		t := c.typ
		el := schemaElement{typ: &t, repetitionType: rep, name: c.name}
		if c.typ == typeByteArray {
			el.convertedType = int32Ptr(convUTF8)
		}
		schema = append(schema, el)
	}

	numRows := len(rows)
	var pages [][]byte
	var metas []columnMetaData
	offset := int64(4)
	for i, c := range cols {
		var defLevels []uint32
		var values []any
		for _, row := range rows {
			v := row[i]
			if c.nullable {
				if v == nil {
					defLevels = append(defLevels, 0)
					continue
				}
				defLevels = append(defLevels, 1)
			}
			values = append(values, v)
		}
		var levelBytes []byte
		if c.nullable {
			enc := rleEncodeRuns(1, defLevels)
			var l [4]byte
			binary.LittleEndian.PutUint32(l[:], uint32(len(enc)))
			levelBytes = append(l[:], enc...)
		}
		valueBytes := plainEncode(c.typ, values)
		page := buildDataPage(int32(numRows), encodingPlain, levelBytes, valueBytes)
		pages = append(pages, page)
		metas = append(metas, columnMetaData{typ: c.typ, codec: codecUncompressed, dataPageOffset: offset})
		offset += int64(len(page))
	}

	chunks := make([]columnChunk, len(cols))
	for i := range cols {
		chunks[i] = columnChunk{meta: metas[i]}
	}
	rg := rowGroup{numRows: int64(numRows), columns: chunks}

	var fw cw
	encodeFileMetaData(&fw, fileMetaData{schema: schema, numRows: int64(numRows), rowGroups: []rowGroup{rg}})
	footer := fw.buf

	out := []byte("PAR1")
	for _, p := range pages {
		out = append(out, p...)
	}
	out = append(out, footer...)
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(footer)))
	out = append(out, lenBuf[:]...)
	out = append(out, "PAR1"...)
	return out
}

type memReader struct{ data []byte }

func (m *memReader) Range(_ context.Context, _ string, offset, length int64) (io.ReadCloser, error) {
	if offset < 0 || offset >= int64(len(m.data)) {
		return nil, io.EOF
	}
	end := offset + length
	if end > int64(len(m.data)) {
		end = int64(len(m.data))
	}
	return io.NopCloser(bytes.NewReader(m.data[offset:end])), nil
}

// ---- tests ----

func TestParseFileMetaDataMatchesSpec(t *testing.T) {
	// Hand-computed Thrift compact encoding of a FileMetaData with schema
	// [root group -> int32 "a", int32 "b"] and num_rows = 0. This anchors the
	// reader to the spec, independent of the test encoder.
	footer := []byte{
		0x15, 0x02, // field 1: version = 1
		0x19, 0x3C, // field 2: schema, list of 3 structs
		// root
		0x35, 0x00, // field 3: repetition REQUIRED
		0x18, 0x04, 'r', 'o', 'o', 't', // field 4: name "root"
		0x15, 0x04, // field 5: num_children = 2
		0x00,
		// a
		0x15, 0x02, // field 1: type INT32
		0x25, 0x00, // field 3: repetition REQUIRED
		0x18, 0x01, 'a', // name "a"
		0x00,
		// b
		0x15, 0x02,
		0x25, 0x00,
		0x18, 0x01, 'b',
		0x00,
		0x16, 0x00, // field 3: num_rows = 0
		0x00,
	}

	meta, err := parseFileMetaData(footer)
	if err != nil {
		t.Fatalf("parseFileMetaData() error = %v", err)
	}
	cols := buildColumns(meta.schema)
	if len(cols) != 2 {
		t.Fatalf("buildColumns() = %d columns, want 2", len(cols))
	}
	if cols[0].Name != "a" || cols[0].Type != "int32" || cols[0].Nullable {
		t.Errorf("column 0 = %+v", cols[0])
	}
	if cols[1].Name != "b" || cols[1].Type != "int32" {
		t.Errorf("column 1 = %+v", cols[1])
	}
	if meta.numRows != 0 {
		t.Errorf("numRows = %d, want 0", meta.numRows)
	}
}

func TestDecodeRLE(t *testing.T) {
	// RLE runs (header = length<<1): [1,1,1,0,0] with bit width 1.
	got, err := decodeRLE([]byte{0x06, 0x01, 0x04, 0x00}, 1, 5)
	if err != nil {
		t.Fatalf("decodeRLE() error = %v", err)
	}
	want := []uint32{1, 1, 1, 0, 0}
	if len(got) != len(want) {
		t.Fatalf("decodeRLE() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("decodeRLE() = %v, want %v", got, want)
		}
	}
}

func TestDecodeRLEBitPacked(t *testing.T) {
	// One bit-packed group of 8 values of bit width 2 (header = groups<<1|1).
	got, err := decodeRLE([]byte{0x03, 0xE4, 0xE4}, 2, 8)
	if err != nil {
		t.Fatalf("decodeRLE() error = %v", err)
	}
	want := []uint32{0, 1, 2, 3, 0, 1, 2, 3}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("decodeRLE()[%d] = %d, want %d", i, got[i], want[i])
		}
	}
}

func TestSnappyDecodeLiteral(t *testing.T) {
	got, err := snappyDecode([]byte{0x03, 0x08, 'a', 'b', 'c'})
	if err != nil {
		t.Fatalf("snappyDecode() error = %v", err)
	}
	if string(got) != "abc" {
		t.Errorf("snappyDecode() = %q, want abc", got)
	}
}

func TestSnappyDecodeCopy(t *testing.T) {
	src := append([]byte{0x06, 0x08, 'a', 'b', 'c'}, 0x0A, 0x03, 0x00)
	got, err := snappyDecode(src)
	if err != nil {
		t.Fatalf("snappyDecode() error = %v", err)
	}
	if string(got) != "abcabc" {
		t.Errorf("snappyDecode() = %q, want abcabc", got)
	}
}

func TestInspectMalformed(t *testing.T) {
	r := &memReader{data: []byte("not parquet at all, definitely not a footer")}
	if _, err := Inspect(context.Background(), r, "k", int64(len(r.data))); err == nil {
		t.Fatal("Inspect() expected error for malformed input, got nil")
	}
}

func TestPreviewFlatRequired(t *testing.T) {
	cols := []testColumn{
		{name: "id", typ: typeInt64},
		{name: "amount", typ: typeDouble},
	}
	rows := [][]any{
		{int64(1), 1.5},
		{int64(2), 2.5},
	}
	data := buildFile(cols, rows)
	r := &memReader{data: data}

	preview, err := Preview(context.Background(), r, "k", int64(len(data)), 10)
	if err != nil {
		t.Fatalf("Preview() error = %v", err)
	}
	if len(preview.Rows) != 2 {
		t.Fatalf("Preview() returned %d rows, want 2", len(preview.Rows))
	}
	if preview.Rows[0]["id"] != int64(1) || preview.Rows[0]["amount"] != 1.5 {
		t.Errorf("row 0 = %v", preview.Rows[0])
	}
	if preview.Rows[1]["id"] != int64(2) || preview.Rows[1]["amount"] != 2.5 {
		t.Errorf("row 1 = %v", preview.Rows[1])
	}
	if preview.NumRows != 2 || preview.Truncated {
		t.Errorf("NumRows/Truncated = %d/%v", preview.NumRows, preview.Truncated)
	}
}

func TestPreviewOptionalNulls(t *testing.T) {
	cols := []testColumn{
		{name: "id", typ: typeInt32, nullable: true},
		{name: "label", typ: typeByteArray, nullable: true},
	}
	rows := [][]any{
		{int32(1), "one"},
		{nil, "two"},
		{int32(3), nil},
	}
	data := buildFile(cols, rows)
	r := &memReader{data: data}

	preview, err := Preview(context.Background(), r, "k", int64(len(data)), 10)
	if err != nil {
		t.Fatalf("Preview() error = %v", err)
	}
	if len(preview.Rows) != 3 {
		t.Fatalf("Preview() returned %d rows, want 3", len(preview.Rows))
	}
	if preview.Rows[1]["id"] != nil {
		t.Errorf("row 1 id = %v, want nil", preview.Rows[1]["id"])
	}
	if preview.Rows[1]["label"] != "two" {
		t.Errorf("row 1 label = %v, want two", preview.Rows[1]["label"])
	}
	if preview.Rows[2]["label"] != nil {
		t.Errorf("row 2 label = %v, want nil", preview.Rows[2]["label"])
	}
}

func TestPreviewTruncated(t *testing.T) {
	cols := []testColumn{{name: "id", typ: typeInt64}}
	rows := make([][]any, 30)
	for i := range rows {
		rows[i] = []any{int64(i)}
	}
	data := buildFile(cols, rows)
	r := &memReader{data: data}

	preview, err := Preview(context.Background(), r, "k", int64(len(data)), 5)
	if err != nil {
		t.Fatalf("Preview() error = %v", err)
	}
	if len(preview.Rows) != 5 {
		t.Fatalf("Preview() returned %d rows, want 5", len(preview.Rows))
	}
	if !preview.Truncated {
		t.Error("Truncated = false, want true for 30 rows limited to 5")
	}
}

func TestBuildColumnsNested(t *testing.T) {
	schema := []schemaElement{
		{repetitionType: 0, name: "root", numChildren: int32Ptr(2)},
		{repetitionType: 0, name: "id", typ: int32Ptr(typeInt64)},
		{repetitionType: 0, name: "user", numChildren: int32Ptr(2)},
		{repetitionType: 1, name: "name", typ: int32Ptr(typeByteArray), convertedType: int32Ptr(convUTF8)},
		{repetitionType: 0, name: "age", typ: int32Ptr(typeInt32)},
	}
	cols := buildColumns(schema)
	if len(cols) != 3 {
		t.Fatalf("buildColumns() = %d columns, want 3", len(cols))
	}
	if cols[0].Name != "id" {
		t.Errorf("cols[0] = %+v", cols[0])
	}
	if cols[1].Name != "user.name" || cols[1].Type != "string" {
		t.Errorf("cols[1] = %+v", cols[1])
	}
	if cols[2].Name != "user.age" || cols[2].Type != "int32" {
		t.Errorf("cols[2] = %+v", cols[2])
	}
}

func TestBuildColumnsList(t *testing.T) {
	schema := []schemaElement{
		{repetitionType: 0, name: "root", numChildren: int32Ptr(1)},
		{repetitionType: 1, name: "tags", convertedType: int32Ptr(convList), numChildren: int32Ptr(1)},
		{repetitionType: 2, name: "list", numChildren: int32Ptr(1)},
		{repetitionType: 1, name: "element", typ: int32Ptr(typeByteArray), convertedType: int32Ptr(convUTF8)},
	}
	cols := buildColumns(schema)
	if len(cols) != 1 {
		t.Fatalf("buildColumns() = %d columns, want 1", len(cols))
	}
	if cols[0].Name != "tags" || cols[0].Type != "list" {
		t.Errorf("cols[0] = %+v", cols[0])
	}
}
