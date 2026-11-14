package parquet

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
)

const (
	// maxPreviewPageBytes bounds the decompressed size of a single data page the
	// preview reader will process.
	maxPreviewPageBytes = 64 << 20
	// maxPageHeaderBytes bounds the bytes read to parse a page header.
	maxPageHeaderBytes = 64 << 10
	// defaultPreviewRows is the row limit when the caller does not specify one.
	defaultPreviewRows = 20
	// maxPreviewRows is the hard upper bound on preview rows.
	maxPreviewRows = 100
)

// PreviewResult is a bounded sample of a Parquet file's rows.
type PreviewResult struct {
	Columns   []Column
	Rows      []map[string]any
	NumRows   int64
	Truncated bool
	Note      string
}

// Preview reads a bounded sample (up to maxRows rows) of the object at key. It
// reads only the footer and the first data page of each column, never the whole
// object. Nested schemas are reported via Note and produce no rows in this
// phase.
func Preview(ctx context.Context, r ObjectReader, key string, size int64, maxRows int) (*PreviewResult, error) {
	if maxRows <= 0 {
		maxRows = defaultPreviewRows
	}
	if maxRows > maxPreviewRows {
		maxRows = maxPreviewRows
	}

	meta, err := readFileMetaData(ctx, r, key, size)
	if err != nil {
		return nil, err
	}

	cols := buildColumns(meta.schema)
	leaves, flat := flatLeaves(meta.schema)
	out := &PreviewResult{Columns: cols, NumRows: meta.numRows}
	if !flat {
		out.Note = "preview supports flat (non-nested) schemas in this phase"
		return out, nil
	}
	if len(meta.rowGroups) == 0 || len(leaves) == 0 {
		return out, nil
	}

	rg := meta.rowGroups[0]
	if len(rg.columns) != len(leaves) {
		return out, fmt.Errorf("%w: column count mismatch", ErrMalformed)
	}

	rowCount := int(rg.numRows)
	if rowCount > maxRows {
		rowCount = maxRows
	}

	columns := make([][]any, len(leaves))
	for i := range leaves {
		col, err := readColumnRows(ctx, r, key, rg.columns[i], leaves[i], rowCount)
		if err != nil {
			if out.Note == "" {
				out.Note = "some columns could not be previewed"
			}
			columns[i] = make([]any, rowCount)
			continue
		}
		columns[i] = col
		if len(col) < rowCount {
			rowCount = len(col)
		}
	}

	out.Rows = buildRows(cols, columns, rowCount)
	out.Truncated = int64(rowCount) < meta.numRows
	return out, nil
}

type leafColumn struct {
	typ        int32
	typeLength int32
	maxDef     int32
}

func flatLeaves(elems []schemaElement) ([]leafColumn, bool) {
	flat := true
	leaves := make([]leafColumn, 0, len(elems))
	for i := 1; i < len(elems); i++ {
		e := elems[i]
		if e.typ == nil {
			flat = false
			continue
		}
		if e.repetitionType == 2 { // REPEATED implies a nested/list structure
			flat = false
			continue
		}
		tl := int32(0)
		if e.typeLength != nil {
			tl = *e.typeLength
		}
		maxDef := int32(0)
		if e.repetitionType != 0 { // OPTIONAL
			maxDef = 1
		}
		leaves = append(leaves, leafColumn{typ: *e.typ, typeLength: tl, maxDef: maxDef})
	}
	return leaves, flat
}

func buildRows(cols []Column, columns [][]any, rowCount int) []map[string]any {
	rows := make([]map[string]any, rowCount)
	for r := 0; r < rowCount; r++ {
		row := make(map[string]any, len(cols))
		for i := range cols {
			var v any
			if r < len(columns[i]) {
				v = columns[i][r]
			}
			row[cols[i].Name] = toJSON(cols[i], v)
		}
		rows[r] = row
	}
	return rows
}

func toJSON(col Column, v any) any {
	if v == nil {
		return nil
	}
	switch x := v.(type) {
	case bool, int32, int64, float32, float64:
		return x
	case []byte:
		if col.Type == "string" || col.Type == "json" {
			return string(x)
		}
		return hex.EncodeToString(x)
	default:
		return v
	}
}

func readColumnRows(ctx context.Context, r ObjectReader, key string, chunk columnChunk, leaf leafColumn, maxRows int) ([]any, error) {
	var dict []any
	if chunk.meta.hasDictionaryPage && chunk.meta.dictionaryPageOffset > 0 {
		d, err := readDictionaryPage(ctx, r, key, chunk.meta, leaf)
		if err != nil {
			return nil, err
		}
		dict = d
	}

	hdr, data, err := readPage(ctx, r, key, chunk.meta.dataPageOffset, chunk.meta.codec)
	if err != nil {
		return nil, err
	}
	return decodeDataPage(hdr, data, leaf, dict, maxRows)
}

func readDictionaryPage(ctx context.Context, r ObjectReader, key string, meta columnMetaData, leaf leafColumn) ([]any, error) {
	hdr, data, err := readPage(ctx, r, key, meta.dictionaryPageOffset, meta.codec)
	if err != nil {
		return nil, err
	}
	if hdr.dictionaryPage == nil {
		return nil, fmt.Errorf("%w: missing dictionary page", ErrMalformed)
	}
	return decodePlain(data, meta.typ, int(leaf.typeLength), int(hdr.dictionaryPage.numValues))
}

type pageHeader struct {
	typ              int32
	uncompressedSize int32
	compressedSize   int32
	dataPage         *dataPageHeader
	dataPageV2       *dataPageHeaderV2
	dictionaryPage   *dictionaryPageHeader
}

type dataPageHeader struct {
	numValues int32
	encoding  int32
}

type dataPageHeaderV2 struct {
	numRows             int32
	encoding            int32
	defLevelsByteLength int32
	repLevelsByteLength int32
}

type dictionaryPageHeader struct {
	numValues int32
	encoding  int32
}

func parsePageHeader(b []byte) (pageHeader, int, error) {
	r := newCompactReader(b)
	r.lastID = 0
	hdr := pageHeader{}
	for {
		id, typ, err := r.readFieldHeader()
		if err != nil {
			return hdr, 0, err
		}
		if typ == tStop {
			break
		}
		switch id {
		case 1: // type
			if typ != tI32 {
				return hdr, 0, unexpectedType("page type", typ)
			}
			if hdr.typ, err = r.readI32(); err != nil {
				return hdr, 0, err
			}
		case 2: // uncompressed_page_size
			if typ != tI32 {
				return hdr, 0, unexpectedType("uncompressed_page_size", typ)
			}
			if hdr.uncompressedSize, err = r.readI32(); err != nil {
				return hdr, 0, err
			}
		case 3: // compressed_page_size
			if typ != tI32 {
				return hdr, 0, unexpectedType("compressed_page_size", typ)
			}
			if hdr.compressedSize, err = r.readI32(); err != nil {
				return hdr, 0, err
			}
		case 5: // data_page_header
			dp, err := r.readDataPageHeader(typ)
			if err != nil {
				return hdr, 0, err
			}
			hdr.dataPage = &dp
		case 7: // dictionary_page_header
			dp, err := r.readDictionaryPageHeader(typ)
			if err != nil {
				return hdr, 0, err
			}
			hdr.dictionaryPage = &dp
		case 8: // data_page_header_v2
			dp, err := r.readDataPageHeaderV2(typ)
			if err != nil {
				return hdr, 0, err
			}
			hdr.dataPageV2 = &dp
		default:
			if err := r.skip(typ); err != nil {
				return hdr, 0, err
			}
		}
	}
	return hdr, r.pos, nil
}

func (r *compactReader) readDataPageHeader(typ byte) (dataPageHeader, error) {
	if typ != tStruct {
		return dataPageHeader{}, unexpectedType("data_page_header", typ)
	}
	r.lastID = 0
	dp := dataPageHeader{}
	for {
		id, typ, err := r.readFieldHeader()
		if err != nil {
			return dp, err
		}
		if typ == tStop {
			break
		}
		switch id {
		case 1:
			dp.numValues, err = r.readI32()
		case 2:
			dp.encoding, err = r.readI32()
		default:
			err = r.skip(typ)
		}
		if err != nil {
			return dp, err
		}
	}
	return dp, nil
}

func (r *compactReader) readDataPageHeaderV2(typ byte) (dataPageHeaderV2, error) {
	if typ != tStruct {
		return dataPageHeaderV2{}, unexpectedType("data_page_header_v2", typ)
	}
	r.lastID = 0
	dp := dataPageHeaderV2{}
	for {
		id, typ, err := r.readFieldHeader()
		if err != nil {
			return dp, err
		}
		if typ == tStop {
			break
		}
		switch id {
		case 1:
			err = r.skip(typ) // num_values (non-null count; derived from levels)
		case 2:
			err = r.skip(typ) // num_nulls
		case 3:
			dp.numRows, err = r.readI32()
		case 4:
			dp.encoding, err = r.readI32()
		case 5:
			dp.defLevelsByteLength, err = r.readI32()
		case 6:
			dp.repLevelsByteLength, err = r.readI32()
		case 7:
			_, err = r.readBool() // is_compressed: ignored; column codec applies
		default:
			err = r.skip(typ)
		}
		if err != nil {
			return dp, err
		}
	}
	return dp, nil
}

func (r *compactReader) readDictionaryPageHeader(typ byte) (dictionaryPageHeader, error) {
	if typ != tStruct {
		return dictionaryPageHeader{}, unexpectedType("dictionary_page_header", typ)
	}
	r.lastID = 0
	dp := dictionaryPageHeader{}
	for {
		id, typ, err := r.readFieldHeader()
		if err != nil {
			return dp, err
		}
		if typ == tStop {
			break
		}
		switch id {
		case 1:
			dp.numValues, err = r.readI32()
		case 2:
			dp.encoding, err = r.readI32()
		default:
			err = r.skip(typ)
		}
		if err != nil {
			return dp, err
		}
	}
	return dp, nil
}

func readPage(ctx context.Context, r ObjectReader, key string, offset int64, codec int32) (pageHeader, []byte, error) {
	hdrBytes, err := readRangeAtMost(ctx, r, key, offset, maxPageHeaderBytes)
	if err != nil {
		return pageHeader{}, nil, err
	}
	if len(hdrBytes) == 0 {
		return pageHeader{}, nil, fmt.Errorf("%w: empty page header", ErrMalformed)
	}
	hdr, headerLen, err := parsePageHeader(hdrBytes)
	if err != nil {
		return pageHeader{}, nil, err
	}
	if headerLen <= 0 {
		return pageHeader{}, nil, fmt.Errorf("%w: empty page header", ErrMalformed)
	}
	if hdr.compressedSize < 0 || int64(hdr.compressedSize) > maxPreviewPageBytes {
		return pageHeader{}, nil, fmt.Errorf("%w: page compressed size %d exceeds bound", ErrUnsupported, hdr.compressedSize)
	}
	if hdr.uncompressedSize < 0 || int64(hdr.uncompressedSize) > maxPreviewPageBytes {
		return pageHeader{}, nil, fmt.Errorf("%w: page size %d exceeds bound", ErrUnsupported, hdr.uncompressedSize)
	}

	compressed, err := readRange(ctx, r, key, offset+int64(headerLen), int64(hdr.compressedSize))
	if err != nil {
		return pageHeader{}, nil, err
	}
	data, err := decompress(codec, compressed, hdr.uncompressedSize)
	if err != nil {
		return pageHeader{}, nil, err
	}
	return hdr, data, nil
}

func readRangeAtMost(ctx context.Context, r ObjectReader, key string, offset, maxLen int64) ([]byte, error) {
	rc, err := r.Range(ctx, key, offset, maxLen)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()
	return io.ReadAll(io.LimitReader(rc, maxLen))
}

func decompress(codec int32, data []byte, uncompressedSize int32) ([]byte, error) {
	switch codec {
	case codecUncompressed:
		return data, nil
	case codecSnappy:
		out, err := snappyDecode(data)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrMalformed, err)
		}
		return out, nil
	case codecGzip:
		return gzipDecode(data, uncompressedSize)
	default:
		return nil, fmt.Errorf("%w: codec %d", ErrUnsupported, codec)
	}
}

func gzipDecode(data []byte, size int32) ([]byte, error) {
	zr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	defer func() { _ = zr.Close() }()

	limit := int64(size)
	if limit <= 0 || limit > maxPreviewPageBytes {
		limit = maxPreviewPageBytes
	}
	out, err := io.ReadAll(io.LimitReader(zr, limit+1))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	if int64(len(out)) > limit {
		return nil, fmt.Errorf("%w: gzip output exceeds bound", ErrUnsupported)
	}
	return out, nil
}

func decodeDataPage(hdr pageHeader, data []byte, leaf leafColumn, dict []any, maxRows int) ([]any, error) {
	switch hdr.typ {
	case pageDataPage:
		return decodeDataPageV1(hdr.dataPage, data, leaf, dict, maxRows)
	case pageDataPageV2:
		return decodeDataPageV2(hdr.dataPageV2, data, leaf, dict, maxRows)
	default:
		return nil, fmt.Errorf("%w: page type %d", ErrUnsupported, hdr.typ)
	}
}

func decodeDataPageV1(dp *dataPageHeader, data []byte, leaf leafColumn, dict []any, maxRows int) ([]any, error) {
	if dp == nil {
		return nil, fmt.Errorf("%w: missing data page header", ErrMalformed)
	}
	numValues := int(dp.numValues)
	if numValues <= 0 {
		return []any{}, nil
	}
	rowsWanted := numValues
	if rowsWanted > maxRows {
		rowsWanted = maxRows
	}

	var defLevels []uint32
	nonNull := rowsWanted
	valueOffset := 0
	if leaf.maxDef > 0 {
		bw := bitWidthFor(int(leaf.maxDef))
		if len(data) < 4 {
			return nil, fmt.Errorf("%w: truncated definition levels", ErrMalformed)
		}
		levelLen := int(binary.LittleEndian.Uint32(data[:4]))
		if levelLen < 0 || 4+levelLen > len(data) {
			return nil, fmt.Errorf("%w: invalid definition level length", ErrMalformed)
		}
		levels, _, err := decodeRLELevels(data[4:4+levelLen], bw, numValues, maxRows)
		if err != nil {
			return nil, err
		}
		defLevels = levels
		valueOffset = 4 + levelLen
		nonNull = 0
		for _, l := range defLevels {
			if int32(l) == leaf.maxDef {
				nonNull++
			}
		}
	}

	values, err := decodeValues(data[valueOffset:], dp.encoding, dict, leaf.typ, int(leaf.typeLength), nonNull)
	if err != nil {
		return nil, err
	}
	return assembleRows(defLevels, values, leaf.maxDef, rowsWanted), nil
}

func decodeDataPageV2(dp *dataPageHeaderV2, data []byte, leaf leafColumn, dict []any, maxRows int) ([]any, error) {
	if dp == nil {
		return nil, fmt.Errorf("%w: missing data page header", ErrMalformed)
	}
	numRows := int(dp.numRows)
	if numRows <= 0 {
		return []any{}, nil
	}
	repBytes := int(dp.repLevelsByteLength)
	defBytes := int(dp.defLevelsByteLength)
	if repBytes != 0 {
		return nil, fmt.Errorf("%w: nested repetition levels", ErrUnsupported)
	}
	rowsWanted := numRows
	if rowsWanted > maxRows {
		rowsWanted = maxRows
	}

	var defLevels []uint32
	nonNull := rowsWanted
	valueData := data
	if leaf.maxDef > 0 {
		if defBytes < 0 || defBytes > len(data) {
			return nil, fmt.Errorf("%w: invalid definition level length", ErrMalformed)
		}
		levelsData := data[repBytes : repBytes+defBytes]
		bw := bitWidthFor(int(leaf.maxDef))
		levels, _, err := decodeRLELevels(levelsData, bw, numRows, rowsWanted)
		if err != nil {
			return nil, err
		}
		defLevels = levels
		nonNull = 0
		for _, l := range defLevels {
			if int32(l) == leaf.maxDef {
				nonNull++
			}
		}
		valueData = data[repBytes+defBytes:]
	}

	values, err := decodeValues(valueData, dp.encoding, dict, leaf.typ, int(leaf.typeLength), nonNull)
	if err != nil {
		return nil, err
	}
	return assembleRows(defLevels, values, leaf.maxDef, rowsWanted), nil
}

func decodeValues(data []byte, encoding int32, dict []any, typ int32, typeLength int, count int) ([]any, error) {
	switch encoding {
	case encodingPlain:
		return decodePlain(data, typ, typeLength, count)
	case encodingRLEDictionary:
		if dict == nil {
			return nil, fmt.Errorf("%w: missing dictionary", ErrMalformed)
		}
		if len(data) < 1 {
			return nil, fmt.Errorf("%w: missing dictionary index width", ErrMalformed)
		}
		bw := int(data[0])
		if bw > 32 {
			return nil, fmt.Errorf("%w: invalid dictionary index width", ErrMalformed)
		}
		idx, err := decodeRLE(data[1:], bw, count)
		if err != nil {
			return nil, err
		}
		out := make([]any, count)
		for i, j := range idx {
			if int(j) >= len(dict) {
				return nil, fmt.Errorf("%w: dictionary index out of range", ErrMalformed)
			}
			out[i] = dict[j]
		}
		return out, nil
	case encodingPlainDict:
		if dict == nil {
			return nil, fmt.Errorf("%w: missing dictionary", ErrMalformed)
		}
		idx, err := decodePlain(data, typeInt32, 4, count)
		if err != nil {
			return nil, err
		}
		out := make([]any, count)
		for i, v := range idx {
			k, ok := v.(int32)
			if !ok || int(k) >= len(dict) {
				return nil, fmt.Errorf("%w: dictionary index out of range", ErrMalformed)
			}
			out[i] = dict[k]
		}
		return out, nil
	default:
		return nil, fmt.Errorf("%w: encoding %d", ErrUnsupported, encoding)
	}
}

func assembleRows(defLevels []uint32, values []any, maxDef int32, rowsWanted int) []any {
	rows := make([]any, 0, rowsWanted)
	vi := 0
	for i := 0; i < rowsWanted; i++ {
		if defLevels == nil || defLevels[i] == uint32(maxDef) {
			rows = append(rows, values[vi])
			vi++
			continue
		}
		rows = append(rows, nil)
	}
	return rows
}

// decodeRLELevels decodes total levels while retaining only the first keep,
// consuming the entire level stream so callers can locate the following value
// bytes. It returns the retained levels and the consumed byte offset.
func decodeRLELevels(data []byte, bitWidth, total, keep int) ([]uint32, int, error) {
	d := newRLEDecoder(data)
	prefix := make([]uint32, 0, keep)
	remaining := total
	for remaining > 0 {
		header, err := d.readVarint()
		if err != nil {
			return nil, 0, err
		}
		isRLE := header&1 == 1
		runLen := int(header >> 1)
		if isRLE {
			val, err := d.readRLEValue(bitWidth)
			if err != nil {
				return nil, 0, err
			}
			n := runLen
			if n > remaining {
				n = remaining
			}
			for i := 0; i < n; i++ {
				if len(prefix) < keep {
					prefix = append(prefix, val)
				}
			}
			remaining -= n
			continue
		}
		n := runLen * 8
		for i := 0; i < n; i++ {
			v, err := d.readBits(bitWidth)
			if err != nil {
				return nil, 0, err
			}
			if len(prefix) < keep {
				prefix = append(prefix, v)
			}
		}
		remaining -= n
	}
	return prefix, d.consumedBytes(), nil
}
