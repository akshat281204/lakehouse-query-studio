package parquet

import (
	"encoding/binary"
	"errors"
)

// snappyDecode decompresses a raw Snappy block (the block format Parquet uses,
// not the framing format). It is self-contained and dependency-free.
func snappyDecode(src []byte) ([]byte, error) {
	length, n := binary.Uvarint(src)
	if n <= 0 {
		return nil, errors.New("snappy: invalid preamble")
	}
	if length > maxPreviewPageBytes {
		return nil, errors.New("snappy: declared length too large")
	}
	out := make([]byte, 0, length)
	pos := n
	for pos < len(src) {
		tag := src[pos]
		pos++
		switch tag & 0x03 {
		case 0: // literal
			litLen, extra, err := snappyLiteralLen(tag, src[pos:])
			if err != nil {
				return nil, err
			}
			pos += extra
			if pos+litLen > len(src) {
				return nil, errors.New("snappy: truncated literal")
			}
			out = append(out, src[pos:pos+litLen]...)
			pos += litLen
		case 1: // copy, 1-byte offset
			if pos >= len(src) {
				return nil, errors.New("snappy: truncated copy")
			}
			offset := int(tag>>5)<<8 | int(src[pos])
			pos++
			copyLen := int(tag>>2&0x07) + 4
			if err := snappyCopy(&out, offset, copyLen); err != nil {
				return nil, err
			}
		case 2: // copy, 2-byte offset
			if pos+2 > len(src) {
				return nil, errors.New("snappy: truncated copy")
			}
			offset := int(binary.LittleEndian.Uint16(src[pos:]))
			pos += 2
			copyLen := int(tag>>2) + 1
			if err := snappyCopy(&out, offset, copyLen); err != nil {
				return nil, err
			}
		case 3: // copy, 4-byte offset
			if pos+4 > len(src) {
				return nil, errors.New("snappy: truncated copy")
			}
			offset := int(binary.LittleEndian.Uint32(src[pos:]))
			pos += 4
			copyLen := int(tag>>2) + 1
			if err := snappyCopy(&out, offset, copyLen); err != nil {
				return nil, err
			}
		}
	}
	if uint64(len(out)) != length {
		return nil, errors.New("snappy: length mismatch")
	}
	return out, nil
}

func snappyLiteralLen(tag byte, rest []byte) (length, extra int, err error) {
	l := tag >> 2
	if l < 60 {
		return int(l) + 1, 0, nil
	}
	extra = int(l) - 59
	if len(rest) < extra {
		return 0, 0, errors.New("snappy: truncated literal length")
	}
	var v uint64
	for i := 0; i < extra; i++ {
		v |= uint64(rest[i]) << uint(8*i)
	}
	return int(v) + 1, extra, nil
}

func snappyCopy(out *[]byte, offset, length int) error {
	if offset <= 0 || offset > len(*out) {
		return errors.New("snappy: invalid copy offset")
	}
	start := len(*out) - offset
	for i := 0; i < length; i++ {
		*out = append(*out, (*out)[start+i])
	}
	return nil
}
