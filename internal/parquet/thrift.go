package parquet

import (
	"errors"
	"fmt"
)

// Thrift compact protocol field types.
const (
	tStop   = 0
	tTrue   = 1
	tFalse  = 2
	tByte   = 3
	tI16    = 4
	tI32    = 5
	tI64    = 6
	tDouble = 7
	tBinary = 8
	tList   = 9
	tSet    = 10
	tMap    = 11
	tStruct = 12
)

var errTruncated = errors.New("truncated thrift data")

// compactReader reads the Thrift compact protocol from an in-memory buffer.
type compactReader struct {
	buf    []byte
	pos    int
	lastID int16
}

func newCompactReader(b []byte) *compactReader { return &compactReader{buf: b} }

func (r *compactReader) remaining() int { return len(r.buf) - r.pos }

func (r *compactReader) readByte() (byte, error) {
	if r.pos >= len(r.buf) {
		return 0, errTruncated
	}
	b := r.buf[r.pos]
	r.pos++
	return b, nil
}

func (r *compactReader) readBytes(n int) ([]byte, error) {
	if n < 0 || r.pos+n > len(r.buf) {
		return nil, errTruncated
	}
	b := r.buf[r.pos : r.pos+n]
	r.pos += n
	return b, nil
}

// readVarint reads an unsigned LEB128 varint.
func (r *compactReader) readVarint() (uint64, error) {
	var v uint64
	var shift uint
	for {
		b, err := r.readByte()
		if err != nil {
			return 0, err
		}
		v |= uint64(b&0x7f) << shift
		if b&0x80 == 0 {
			return v, nil
		}
		shift += 7
		if shift > 63 {
			return 0, errors.New("varint too long")
		}
	}
}

func (r *compactReader) readZigzag() (int64, error) {
	v, err := r.readVarint()
	if err != nil {
		return 0, err
	}
	return int64(v>>1) ^ -int64(v&1), nil
}

func (r *compactReader) readI32() (int32, error) {
	v, err := r.readZigzag()
	if err != nil {
		return 0, err
	}
	return int32(v), nil
}

func (r *compactReader) readI64() (int64, error) { return r.readZigzag() }

func (r *compactReader) readBool() (bool, error) {
	b, err := r.readByte()
	if err != nil {
		return false, err
	}
	switch b {
	case tTrue:
		return true, nil
	case tFalse:
		return false, nil
	default:
		return false, fmt.Errorf("invalid compact bool type %d", b)
	}
}

func (r *compactReader) readBinary() ([]byte, error) {
	n, err := r.readVarint()
	if err != nil {
		return nil, err
	}
	if n > uint64(r.remaining()) {
		return nil, errTruncated
	}
	return r.readBytes(int(n))
}

func (r *compactReader) readString() (string, error) {
	b, err := r.readBinary()
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// readFieldHeader reads a compact field header and returns the field id and
// type. A STOP byte returns (0, tStop, nil).
func (r *compactReader) readFieldHeader() (int16, byte, error) {
	b, err := r.readByte()
	if err != nil {
		return 0, 0, err
	}
	typ := b & 0x0f
	if typ == tStop {
		return 0, tStop, nil
	}
	delta := int16(b >> 4)
	if delta == 0 {
		v, err := r.readZigzag()
		if err != nil {
			return 0, 0, err
		}
		r.lastID = int16(v)
	} else {
		r.lastID += delta
	}
	return r.lastID, typ, nil
}

// readListHeader reads a compact list/set header and returns the element count
// and element type.
func (r *compactReader) readListHeader() (int, byte, error) {
	b, err := r.readByte()
	if err != nil {
		return 0, 0, err
	}
	size := int(b >> 4)
	elemType := b & 0x0f
	if size == 15 {
		v, err := r.readVarint()
		if err != nil {
			return 0, 0, err
		}
		size = int(v)
	}
	return size, elemType, nil
}

// skip skips a value of the given compact type.
func (r *compactReader) skip(typ byte) error {
	switch typ {
	case tTrue, tFalse, tByte:
		_, err := r.readByte()
		return err
	case tI16:
		_, err := r.readZigzag()
		return err
	case tI32:
		_, err := r.readI32()
		return err
	case tI64:
		_, err := r.readI64()
		return err
	case tDouble:
		_, err := r.readBytes(8)
		return err
	case tBinary:
		_, err := r.readBinary()
		return err
	case tList, tSet:
		size, elemType, err := r.readListHeader()
		if err != nil {
			return err
		}
		for i := 0; i < size; i++ {
			if err := r.skip(elemType); err != nil {
				return err
			}
		}
		return nil
	case tMap:
		return r.skipMap()
	case tStruct:
		return r.skipStruct()
	default:
		return fmt.Errorf("unknown compact type %d", typ)
	}
}

func (r *compactReader) skipMap() error {
	size, err := r.readVarint()
	if err != nil {
		return err
	}
	if size == 0 {
		return nil
	}
	kv, err := r.readByte()
	if err != nil {
		return err
	}
	keyType := kv >> 4
	valType := kv & 0x0f
	for i := uint64(0); i < size; i++ {
		if err := r.skip(keyType); err != nil {
			return err
		}
		if err := r.skip(valType); err != nil {
			return err
		}
	}
	return nil
}

func (r *compactReader) skipStruct() error {
	r.lastID = 0
	for {
		_, typ, err := r.readFieldHeader()
		if err != nil {
			return err
		}
		if typ == tStop {
			return nil
		}
		if err := r.skip(typ); err != nil {
			return err
		}
	}
}
