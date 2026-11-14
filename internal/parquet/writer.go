package parquet

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// Logical column types the writer can produce.
const (
	TypeBoolean = "boolean"
	TypeInt64   = "int64"
	TypeDouble  = "double"
	TypeString  = "string"
)

// ErrWrite is returned for invalid writer input (unknown type, value of the
// wrong Go type, null in a required column, ragged rows).
var ErrWrite = errors.New("parquet: invalid write input")

// WriteColumn describes one flat output column.
type WriteColumn struct {
	Name     string
	Type     string // one of the Type* constants
	Nullable bool
}

// Encode writes rows as a single-row-group, PLAIN-encoded, uncompressed Parquet
// file with a flat schema. Cell values must be nil (nullable columns only),
// bool, int64, float64, or string according to the column type. The result is
// a complete in-memory file; callers bound the row count (the ingestion
// converter caps each part).
func Encode(cols []WriteColumn, rows [][]any) ([]byte, error) {
	if len(cols) == 0 {
		return nil, fmt.Errorf("%w: no columns", ErrWrite)
	}
	physical := make([]int32, len(cols))
	for i, c := range cols {
		switch c.Type {
		case TypeBoolean:
			physical[i] = typeBoolean
		case TypeInt64:
			physical[i] = typeInt64
		case TypeDouble:
			physical[i] = typeDouble
		case TypeString:
			physical[i] = typeByteArray
		default:
			return nil, fmt.Errorf("%w: unsupported type %q", ErrWrite, c.Type)
		}
		if c.Name == "" {
			return nil, fmt.Errorf("%w: empty column name", ErrWrite)
		}
	}

	out := bytes.NewBuffer(make([]byte, 0, 1024))
	out.WriteString("PAR1")

	type chunk struct {
		offset     int64
		totalBytes int64
	}
	chunks := make([]chunk, len(cols))

	for i, c := range cols {
		levels, values, err := splitColumn(c, rows, i)
		if err != nil {
			return nil, err
		}
		valueBytes, err := plainValues(physical[i], values)
		if err != nil {
			return nil, err
		}

		var body []byte
		if c.Nullable {
			enc := rleLevels(levels)
			var l [4]byte
			binary.LittleEndian.PutUint32(l[:], uint32(len(enc)))
			body = append(body, l[:]...)
			body = append(body, enc...)
		}
		body = append(body, valueBytes...)

		hdr := &thriftWriter{}
		hdr.beginStruct()
		hdr.fieldI32(1, pageDataPage)
		hdr.fieldI32(2, int32(len(body)))
		hdr.fieldI32(3, int32(len(body)))
		hdr.fieldStruct(5)
		hdr.fieldI32(1, int32(len(rows)))
		hdr.fieldI32(2, encodingPlain)
		hdr.fieldI32(3, encodingRLE)
		hdr.fieldI32(4, encodingRLE)
		hdr.endStruct()
		hdr.endStruct()

		start := int64(out.Len())
		out.Write(hdr.buf)
		out.Write(body)
		chunks[i] = chunk{offset: start, totalBytes: int64(out.Len()) - start}
	}

	fw := &thriftWriter{}
	fw.beginStruct()
	fw.fieldI32(1, 1)
	// schema
	fw.fieldList(2, len(cols)+1, tStruct)
	fw.beginStruct()
	fw.fieldI32(3, 0) // REQUIRED
	fw.fieldString(4, "root")
	fw.fieldI32(5, int32(len(cols)))
	fw.endStruct()
	for i, c := range cols {
		fw.beginStruct()
		fw.fieldI32(1, physical[i])
		rep := int32(0)
		if c.Nullable {
			rep = 1
		}
		fw.fieldI32(3, rep)
		fw.fieldString(4, c.Name)
		if c.Type == TypeString {
			fw.fieldI32(6, convUTF8)
		}
		fw.endStruct()
	}
	fw.fieldI64(3, int64(len(rows)))
	// row groups
	fw.fieldList(4, 1, tStruct)
	fw.beginStruct()
	fw.fieldList(1, len(cols), tStruct)
	var rgBytes int64
	for i, c := range cols {
		fw.beginStruct()
		fw.fieldI64(2, chunks[i].offset)
		fw.fieldStruct(3)
		fw.fieldI32(1, physical[i])
		fw.fieldList(2, 2, tI32)
		fw.listI32(encodingPlain)
		fw.listI32(encodingRLE)
		fw.fieldList(3, 1, tBinary)
		fw.listString(c.Name)
		fw.fieldI32(4, codecUncompressed)
		fw.fieldI64(5, int64(len(rows)))
		fw.fieldI64(6, chunks[i].totalBytes)
		fw.fieldI64(7, chunks[i].totalBytes)
		fw.fieldI64(9, chunks[i].offset)
		fw.endStruct() // meta_data
		fw.endStruct() // column chunk
		rgBytes += chunks[i].totalBytes
	}
	fw.fieldI64(2, rgBytes)
	fw.fieldI64(3, int64(len(rows)))
	fw.endStruct() // row group
	fw.fieldString(6, "lakehouse-query-studio")
	fw.endStruct()

	out.Write(fw.buf)
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(fw.buf)))
	out.Write(lenBuf[:])
	out.WriteString("PAR1")
	return out.Bytes(), nil
}

// splitColumn returns definition levels (nullable columns only) and the
// non-null values of column idx.
func splitColumn(c WriteColumn, rows [][]any, idx int) (levels []uint32, values []any, err error) {
	for _, row := range rows {
		if idx >= len(row) {
			return nil, nil, fmt.Errorf("%w: ragged row", ErrWrite)
		}
		v := row[idx]
		if v == nil {
			if !c.Nullable {
				return nil, nil, fmt.Errorf("%w: null in required column %q", ErrWrite, c.Name)
			}
			levels = append(levels, 0)
			continue
		}
		if c.Nullable {
			levels = append(levels, 1)
		}
		values = append(values, v)
	}
	return levels, values, nil
}

func plainValues(typ int32, values []any) ([]byte, error) {
	var out []byte
	switch typ {
	case typeBoolean:
		out = make([]byte, (len(values)+7)/8)
		for i, v := range values {
			b, ok := v.(bool)
			if !ok {
				return nil, fmt.Errorf("%w: boolean expected, got %T", ErrWrite, v)
			}
			if b {
				out[i/8] |= 1 << uint(i%8)
			}
		}
	case typeInt64:
		for _, v := range values {
			n, ok := v.(int64)
			if !ok {
				return nil, fmt.Errorf("%w: int64 expected, got %T", ErrWrite, v)
			}
			var b [8]byte
			binary.LittleEndian.PutUint64(b[:], uint64(n))
			out = append(out, b[:]...)
		}
	case typeDouble:
		for _, v := range values {
			f, ok := v.(float64)
			if !ok {
				return nil, fmt.Errorf("%w: float64 expected, got %T", ErrWrite, v)
			}
			var b [8]byte
			binary.LittleEndian.PutUint64(b[:], math.Float64bits(f))
			out = append(out, b[:]...)
		}
	case typeByteArray:
		for _, v := range values {
			s, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("%w: string expected, got %T", ErrWrite, v)
			}
			var b [4]byte
			binary.LittleEndian.PutUint32(b[:], uint32(len(s)))
			out = append(out, b[:]...)
			out = append(out, s...)
		}
	}
	return out, nil
}

// rleLevels encodes 1-bit levels as RLE-hybrid runs (spec-compliant: the
// header is runLength<<1 and the value occupies one byte).
func rleLevels(levels []uint32) []byte {
	var out []byte
	for i := 0; i < len(levels); {
		j := i
		for j < len(levels) && levels[j] == levels[i] {
			j++
		}
		out = appendVarintBytes(out, uint64(j-i)<<1)
		out = append(out, byte(levels[i]))
		i = j
	}
	return out
}

func appendVarintBytes(b []byte, v uint64) []byte {
	for v >= 0x80 {
		b = append(b, byte(v)|0x80)
		v >>= 7
	}
	return append(b, byte(v))
}

// thriftWriter writes the Thrift compact protocol into memory.
type thriftWriter struct {
	buf   []byte
	last  int16
	stack []int16
}

func (w *thriftWriter) varint(v uint64) { w.buf = appendVarintBytes(w.buf, v) }
func (w *thriftWriter) zigzag(v int64)  { w.varint(uint64(v<<1) ^ uint64(v>>63)) }

func (w *thriftWriter) fieldHeader(id int16, typ byte) {
	delta := id - w.last
	if delta > 0 && delta <= 15 {
		w.buf = append(w.buf, byte(delta)<<4|typ)
	} else {
		w.buf = append(w.buf, typ)
		w.zigzag(int64(id))
	}
	w.last = id
}

func (w *thriftWriter) beginStruct() {
	w.stack = append(w.stack, w.last)
	w.last = 0
}

func (w *thriftWriter) endStruct() {
	w.buf = append(w.buf, tStop)
	w.last = w.stack[len(w.stack)-1]
	w.stack = w.stack[:len(w.stack)-1]
}

func (w *thriftWriter) fieldI32(id int16, v int32) {
	w.fieldHeader(id, tI32)
	w.zigzag(int64(v))
}

func (w *thriftWriter) fieldI64(id int16, v int64) {
	w.fieldHeader(id, tI64)
	w.zigzag(v)
}

func (w *thriftWriter) fieldString(id int16, s string) {
	w.fieldHeader(id, tBinary)
	w.varint(uint64(len(s)))
	w.buf = append(w.buf, s...)
}

// fieldStruct writes the header of a nested struct field; the caller then
// writes the struct body (starting with beginStruct semantics handled here).
func (w *thriftWriter) fieldStruct(id int16) {
	w.fieldHeader(id, tStruct)
	w.stack = append(w.stack, w.last)
	w.last = 0
}

func (w *thriftWriter) fieldList(id int16, size int, elem byte) {
	w.fieldHeader(id, tList)
	if size < 15 {
		w.buf = append(w.buf, byte(size)<<4|elem)
	} else {
		w.buf = append(w.buf, 0xF0|elem)
		w.varint(uint64(size))
	}
}

func (w *thriftWriter) listI32(v int32) { w.zigzag(int64(v)) }
func (w *thriftWriter) listString(s string) {
	w.varint(uint64(len(s)))
	w.buf = append(w.buf, s...)
}
