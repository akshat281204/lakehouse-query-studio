// Package parquet provides a dependency-free, bounded reader for the Parquet
// metadata needed to register and preview datasets: the file footer (schema and
// row groups) and a bounded sample of rows for flat schemas.
package parquet

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Parquet primitive types.
const (
	typeBoolean      = 0
	typeInt32        = 1
	typeInt64        = 2
	typeInt96        = 3
	typeFloat        = 4
	typeDouble       = 5
	typeByteArray    = 6
	typeFixedLenByte = 7
)

// Converted type annotations (subset used for display names).
const (
	convUTF8            = 0
	convMap             = 1
	convMapKeyValue     = 2
	convList            = 3
	convDecimal         = 5
	convDate            = 6
	convTimeMillis      = 7
	convTimeMicros      = 8
	convTimestampMillis = 9
	convTimestampMicros = 10
	convUint8           = 11
	convUint16          = 12
	convUint32          = 13
	convUint64          = 14
	convInt8            = 15
	convInt16           = 16
	convInt32           = 17
	convInt64           = 18
	convJSON            = 19
	convBSON            = 20
)

// Compression codecs.
const (
	codecUncompressed = 0
	codecSnappy       = 1
	codecGzip         = 2
)

// Encodings.
const (
	encodingPlain         = 0
	encodingPlainDict     = 2
	encodingRLE           = 3
	encodingRLEDictionary = 8
)

// Page types.
const (
	pageDataPage   = 0
	pageIndexPage  = 1
	pageDictionary = 2
	pageDataPageV2 = 3
)

var (
	// ErrMalformed is returned when a Parquet file cannot be parsed.
	ErrMalformed = errors.New("malformed Parquet file")
	// ErrUnsupported is returned for Parquet features the bounded reader does
	// not support (for example, a nested schema or an unknown codec).
	ErrUnsupported = errors.New("unsupported Parquet feature")
)

// maxFooterSize bounds the Parquet footer we will read and parse.
const maxFooterSize = 8 << 20

// Column is a flattened schema column.
type Column struct {
	Name     string
	Type     string
	Nullable bool
}

// Schema is the flattened schema and row count of a Parquet file.
type Schema struct {
	Columns []Column
	NumRows int64
}

// ObjectReader reads bounded ranges of an object. objectstore.Store satisfies
// it.
type ObjectReader interface {
	Range(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error)
}

type schemaElement struct {
	typ            *int32
	typeLength     *int32
	repetitionType int32
	name           string
	numChildren    *int32
	convertedType  *int32
}

type columnMetaData struct {
	typ                  int32
	codec                int32
	dataPageOffset       int64
	dictionaryPageOffset int64
	hasDictionaryPage    bool
}

type columnChunk struct {
	meta columnMetaData
}

type rowGroup struct {
	numRows int64
	columns []columnChunk
}

type fileMetaData struct {
	schema    []schemaElement
	numRows   int64
	rowGroups []rowGroup
}

// Inspect reads the Parquet footer of the object at key (which has the given
// size in bytes) and returns the flattened schema and total row count. It reads
// only the footer, never the whole object.
func Inspect(ctx context.Context, r ObjectReader, key string, size int64) (*Schema, error) {
	meta, err := readFileMetaData(ctx, r, key, size)
	if err != nil {
		return nil, err
	}
	return &Schema{
		Columns: buildColumns(meta.schema),
		NumRows: meta.numRows,
	}, nil
}

func readFileMetaData(ctx context.Context, r ObjectReader, key string, size int64) (*fileMetaData, error) {
	if size < 12 {
		return nil, fmt.Errorf("%w: file too small", ErrMalformed)
	}
	tail, err := readRange(ctx, r, key, size-8, 8)
	if err != nil {
		return nil, err
	}
	if string(tail[4:]) != "PAR1" {
		return nil, fmt.Errorf("%w: missing trailing magic", ErrMalformed)
	}
	footerLen := int64(binary.LittleEndian.Uint32(tail[0:4]))
	if footerLen <= 0 || footerLen > maxFooterSize || footerLen > size-8 {
		return nil, fmt.Errorf("%w: invalid footer length", ErrMalformed)
	}
	footer, err := readRange(ctx, r, key, size-8-footerLen, footerLen)
	if err != nil {
		return nil, err
	}
	meta, err := parseFileMetaData(footer)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	return meta, nil
}

func readRange(ctx context.Context, r ObjectReader, key string, offset, length int64) ([]byte, error) {
	rc, err := r.Range(ctx, key, offset, length)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()
	b, err := io.ReadAll(io.LimitReader(rc, length))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) != length {
		return nil, fmt.Errorf("%w: short read at offset %d", ErrMalformed, offset)
	}
	return b, nil
}

func parseFileMetaData(b []byte) (*fileMetaData, error) {
	r := newCompactReader(b)
	r.lastID = 0
	meta := &fileMetaData{}
	for {
		id, typ, err := r.readFieldHeader()
		if err != nil {
			return nil, err
		}
		if typ == tStop {
			break
		}
		switch id {
		case 1: // version (i32)
			if err := r.skip(typ); err != nil {
				return nil, err
			}
		case 2: // schema (list<SchemaElement>)
			schema, err := r.readSchemaElements(typ)
			if err != nil {
				return nil, err
			}
			meta.schema = schema
		case 3: // num_rows (i64)
			if typ != tI64 {
				return nil, unexpectedType("num_rows", typ)
			}
			if meta.numRows, err = r.readI64(); err != nil {
				return nil, err
			}
		case 4: // row_groups (list<RowGroup>)
			groups, err := r.readRowGroups(typ)
			if err != nil {
				return nil, err
			}
			meta.rowGroups = groups
		default:
			if err := r.skip(typ); err != nil {
				return nil, err
			}
		}
	}
	if meta.schema == nil {
		return nil, errors.New("missing schema in footer")
	}
	return meta, nil
}

func (r *compactReader) readSchemaElements(typ byte) ([]schemaElement, error) {
	if typ != tList && typ != tSet {
		return nil, unexpectedType("schema", typ)
	}
	n, elemType, err := r.readListHeader()
	if err != nil {
		return nil, err
	}
	if elemType != tStruct {
		return nil, unexpectedType("schema element", elemType)
	}
	out := make([]schemaElement, 0, n)
	for i := 0; i < n; i++ {
		e, err := r.readSchemaElement()
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}

func (r *compactReader) readSchemaElement() (schemaElement, error) {
	r.lastID = 0
	e := schemaElement{repetitionType: 1}
	for {
		id, typ, err := r.readFieldHeader()
		if err != nil {
			return e, err
		}
		if typ == tStop {
			break
		}
		switch id {
		case 1: // type (i32 enum)
			v, err := r.readI32()
			if err != nil {
				return e, err
			}
			e.typ = &v
		case 2: // type_length (i32)
			v, err := r.readI32()
			if err != nil {
				return e, err
			}
			e.typeLength = &v
		case 3: // repetition_type (i32 enum)
			if e.repetitionType, err = r.readI32(); err != nil {
				return e, err
			}
		case 4: // name (string)
			if e.name, err = r.readString(); err != nil {
				return e, err
			}
		case 5: // num_children (i32)
			v, err := r.readI32()
			if err != nil {
				return e, err
			}
			e.numChildren = &v
		case 6: // converted_type (i32 enum)
			v, err := r.readI32()
			if err != nil {
				return e, err
			}
			e.convertedType = &v
		default:
			if err := r.skip(typ); err != nil {
				return e, err
			}
		}
	}
	return e, nil
}

func (r *compactReader) readRowGroups(typ byte) ([]rowGroup, error) {
	if typ != tList && typ != tSet {
		return nil, unexpectedType("row_groups", typ)
	}
	n, elemType, err := r.readListHeader()
	if err != nil {
		return nil, err
	}
	if elemType != tStruct {
		return nil, unexpectedType("row group", elemType)
	}
	out := make([]rowGroup, 0, n)
	for i := 0; i < n; i++ {
		g, err := r.readRowGroup()
		if err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, nil
}

func (r *compactReader) readRowGroup() (rowGroup, error) {
	r.lastID = 0
	g := rowGroup{}
	for {
		id, typ, err := r.readFieldHeader()
		if err != nil {
			return g, err
		}
		if typ == tStop {
			break
		}
		switch id {
		case 1: // columns (list<ColumnChunk>)
			cols, err := r.readColumnChunks(typ)
			if err != nil {
				return g, err
			}
			g.columns = cols
		case 3: // num_rows (i64)
			if typ != tI64 {
				return g, unexpectedType("row group num_rows", typ)
			}
			if g.numRows, err = r.readI64(); err != nil {
				return g, err
			}
		default:
			if err := r.skip(typ); err != nil {
				return g, err
			}
		}
	}
	return g, nil
}

func (r *compactReader) readColumnChunks(typ byte) ([]columnChunk, error) {
	if typ != tList && typ != tSet {
		return nil, unexpectedType("columns", typ)
	}
	n, elemType, err := r.readListHeader()
	if err != nil {
		return nil, err
	}
	if elemType != tStruct {
		return nil, unexpectedType("column chunk", elemType)
	}
	out := make([]columnChunk, 0, n)
	for i := 0; i < n; i++ {
		c, err := r.readColumnChunk()
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, nil
}

func (r *compactReader) readColumnChunk() (columnChunk, error) {
	r.lastID = 0
	c := columnChunk{}
	for {
		id, typ, err := r.readFieldHeader()
		if err != nil {
			return c, err
		}
		if typ == tStop {
			break
		}
		switch id {
		case 3: // meta_data (ColumnMetaData)
			meta, err := r.readColumnMetaData(typ)
			if err != nil {
				return c, err
			}
			c.meta = meta
		default:
			if err := r.skip(typ); err != nil {
				return c, err
			}
		}
	}
	return c, nil
}

func (r *compactReader) readColumnMetaData(typ byte) (columnMetaData, error) {
	if typ != tStruct {
		return columnMetaData{}, unexpectedType("column meta_data", typ)
	}
	r.lastID = 0
	m := columnMetaData{}
	for {
		id, typ, err := r.readFieldHeader()
		if err != nil {
			return m, err
		}
		if typ == tStop {
			break
		}
		switch id {
		case 1: // type (i32)
			if typ != tI32 {
				return m, unexpectedType("column type", typ)
			}
			if m.typ, err = r.readI32(); err != nil {
				return m, err
			}
		case 4: // codec (i32)
			if typ != tI32 {
				return m, unexpectedType("codec", typ)
			}
			if m.codec, err = r.readI32(); err != nil {
				return m, err
			}
		case 9: // data_page_offset (i64)
			if typ != tI64 {
				return m, unexpectedType("data_page_offset", typ)
			}
			if m.dataPageOffset, err = r.readI64(); err != nil {
				return m, err
			}
		case 11: // dictionary_page_offset (i64)
			if typ != tI64 {
				return m, unexpectedType("dictionary_page_offset", typ)
			}
			var v int64
			if v, err = r.readI64(); err != nil {
				return m, err
			}
			m.dictionaryPageOffset = v
			m.hasDictionaryPage = true
		default:
			if err := r.skip(typ); err != nil {
				return m, err
			}
		}
	}
	return m, nil
}

func unexpectedType(what string, typ byte) error {
	return fmt.Errorf("unexpected compact type %d for %s", typ, what)
}

// buildColumns flattens the pre-order schema element list into display columns.
// Structs are flattened to dotted paths; list/map groups are represented as a
// single column (their whole subtree is skipped).
func buildColumns(elems []schemaElement) []Column {
	if len(elems) == 0 {
		return nil
	}
	end := subtreeEnds(elems)
	cols := make([]Column, 0, len(elems))

	var walk func(idx int, prefix string)
	walk = func(idx int, prefix string) {
		e := elems[idx]
		name := prefix + e.name
		if e.typ != nil {
			cols = append(cols, Column{
				Name:     name,
				Type:     typeString(e),
				Nullable: e.repetitionType != 0,
			})
			return
		}
		if isMapGroup(e) || isListGroup(e) {
			t := "list"
			if isMapGroup(e) {
				t = "map"
			}
			cols = append(cols, Column{Name: name, Type: t, Nullable: e.repetitionType != 0})
			return
		}
		children := 0
		if e.numChildren != nil {
			children = int(*e.numChildren)
		}
		child := idx + 1
		for i := 0; i < children; i++ {
			walk(child, prefix+e.name+".")
			child = end[child]
		}
	}

	children := 0
	if elems[0].numChildren != nil {
		children = int(*elems[0].numChildren)
	}
	child := 1
	for i := 0; i < children; i++ {
		walk(child, "")
		child = end[child]
	}
	return cols
}

// subtreeEnds returns, for each schema element index, the index just past its
// subtree. The schema list is in pre-order, so a group's subtree is contiguous.
func subtreeEnds(elems []schemaElement) []int {
	end := make([]int, len(elems))
	var dfs func(i int) int
	dfs = func(i int) int {
		next := i + 1
		if elems[i].numChildren != nil {
			for c := 0; c < int(*elems[i].numChildren); c++ {
				next = dfs(next)
			}
		}
		end[i] = next
		return next
	}
	dfs(0)
	return end
}

func isMapGroup(e schemaElement) bool {
	return e.convertedType != nil && (*e.convertedType == convMap || *e.convertedType == convMapKeyValue)
}

func isListGroup(e schemaElement) bool {
	return e.convertedType != nil && *e.convertedType == convList
}

func typeString(e schemaElement) string {
	typ := int32(typeByteArray)
	if e.typ != nil {
		typ = *e.typ
	}
	conv := int32(-1)
	if e.convertedType != nil {
		conv = *e.convertedType
	}
	switch typ {
	case typeBoolean:
		return "boolean"
	case typeInt32:
		switch conv {
		case convInt8:
			return "int8"
		case convInt16:
			return "int16"
		case convUint8:
			return "uint8"
		case convUint16:
			return "uint16"
		case convUint32:
			return "uint32"
		case convDate:
			return "date"
		case convTimeMillis:
			return "time"
		case convDecimal:
			return "decimal"
		default:
			return "int32"
		}
	case typeInt64:
		switch conv {
		case convInt64:
			return "int64"
		case convUint64:
			return "uint64"
		case convTimestampMillis:
			return "timestamp_ms"
		case convTimestampMicros:
			return "timestamp_us"
		case convTimeMicros:
			return "time_us"
		case convDecimal:
			return "decimal"
		default:
			return "int64"
		}
	case typeInt96:
		return "int96"
	case typeFloat:
		return "float"
	case typeDouble:
		return "double"
	case typeFixedLenByte:
		if conv == convDecimal {
			return "decimal"
		}
		if e.typeLength != nil {
			return fmt.Sprintf("fixed(%d)", *e.typeLength)
		}
		return "fixed"
	default: // typeByteArray
		switch conv {
		case convUTF8:
			return "string"
		case convJSON:
			return "json"
		case convBSON:
			return "bson"
		case convDecimal:
			return "decimal"
		default:
			return "binary"
		}
	}
}
