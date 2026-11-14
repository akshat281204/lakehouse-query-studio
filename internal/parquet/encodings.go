package parquet

import (
	"encoding/binary"
	"errors"
	"math"
	"math/bits"
)

// rleDecoder decodes the Parquet run-length / bit-packed hybrid encoding, which
// is used for definition/repetition levels, boolean values, and dictionary
// indices.
type rleDecoder struct {
	data   []byte
	bitPos int
}

func newRLEDecoder(data []byte) *rleDecoder { return &rleDecoder{data: data} }

func (d *rleDecoder) readVarint() (uint64, error) {
	if d.bitPos%8 != 0 {
		return 0, errors.New("varint not byte-aligned")
	}
	var v uint64
	var shift uint
	for {
		byteIdx := d.bitPos / 8
		if byteIdx >= len(d.data) {
			return 0, errTruncated
		}
		b := d.data[byteIdx]
		d.bitPos += 8
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

func (d *rleDecoder) readBits(n int) (uint32, error) {
	if n == 0 {
		return 0, nil
	}
	if n > 32 {
		return 0, errors.New("bit width too large")
	}
	var v uint32
	for i := 0; i < n; i++ {
		byteIdx := d.bitPos / 8
		if byteIdx >= len(d.data) {
			return 0, errTruncated
		}
		bit := (d.data[byteIdx] >> uint(d.bitPos%8)) & 1
		v |= uint32(bit) << uint(i)
		d.bitPos++
	}
	return v, nil
}

// readRLEValue reads a byte-aligned RLE value of the given bit width.
func (d *rleDecoder) readRLEValue(bw int) (uint32, error) {
	if bw == 0 {
		return 0, nil
	}
	if d.bitPos%8 != 0 {
		d.bitPos = (d.bitPos + 7) / 8 * 8
	}
	nBytes := (bw + 7) / 8
	byteIdx := d.bitPos / 8
	if byteIdx+nBytes > len(d.data) {
		return 0, errTruncated
	}
	var v uint64
	for i := 0; i < nBytes; i++ {
		v |= uint64(d.data[byteIdx+i]) << uint(8*i)
	}
	d.bitPos += nBytes * 8
	if bw < 32 {
		v &= (uint64(1) << uint(bw)) - 1
	}
	return uint32(v), nil
}

// decodeValues decodes count values of the given bit width.
func (d *rleDecoder) decodeValues(bitWidth int, count int) ([]uint32, error) {
	out := make([]uint32, 0, count)
	for len(out) < count {
		header, err := d.readVarint()
		if err != nil {
			return nil, err
		}
		isRLE := header&1 == 1
		runLen := int(header >> 1)
		if isRLE {
			val, err := d.readRLEValue(bitWidth)
			if err != nil {
				return nil, err
			}
			for i := 0; i < runLen && len(out) < count; i++ {
				out = append(out, val)
			}
			continue
		}
		n := runLen * 8
		for i := 0; i < n && len(out) < count; i++ {
			v, err := d.readBits(bitWidth)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
	}
	return out, nil
}

// consumedBytes returns the number of whole bytes consumed so far.
func (d *rleDecoder) consumedBytes() int { return (d.bitPos + 7) / 8 }

// decodeRLE decodes count values of the given bit width from data.
func decodeRLE(data []byte, bitWidth int, count int) ([]uint32, error) {
	d := newRLEDecoder(data)
	return d.decodeValues(bitWidth, count)
}

// bitWidthFor returns the number of bits needed to represent values in
// [0, maxValue].
func bitWidthFor(maxValue int) int {
	if maxValue <= 0 {
		return 0
	}
	return bits.Len32(uint32(maxValue))
}

// decodePlain decodes count values using the PLAIN encoding for the given
// primitive type.
func decodePlain(data []byte, typ int32, typeLength int, count int) ([]any, error) {
	out := make([]any, count)
	if typ == typeBoolean {
		d := newRLEDecoder(data)
		for i := 0; i < count; i++ {
			b, err := d.readBits(1)
			if err != nil {
				return nil, err
			}
			out[i] = b == 1
		}
		return out, nil
	}

	pos := 0
	need := func(n int) ([]byte, error) {
		if n < 0 || pos+n > len(data) {
			return nil, errTruncated
		}
		b := data[pos : pos+n]
		pos += n
		return b, nil
	}

	for i := 0; i < count; i++ {
		switch typ {
		case typeInt32:
			b, err := need(4)
			if err != nil {
				return nil, err
			}
			out[i] = int32(binary.LittleEndian.Uint32(b))
		case typeInt64:
			b, err := need(8)
			if err != nil {
				return nil, err
			}
			out[i] = int64(binary.LittleEndian.Uint64(b))
		case typeInt96:
			b, err := need(12)
			if err != nil {
				return nil, err
			}
			out[i] = append([]byte(nil), b...)
		case typeFloat:
			b, err := need(4)
			if err != nil {
				return nil, err
			}
			out[i] = math.Float32frombits(binary.LittleEndian.Uint32(b))
		case typeDouble:
			b, err := need(8)
			if err != nil {
				return nil, err
			}
			out[i] = math.Float64frombits(binary.LittleEndian.Uint64(b))
		case typeByteArray:
			lb, err := need(4)
			if err != nil {
				return nil, err
			}
			n := int(binary.LittleEndian.Uint32(lb))
			b, err := need(n)
			if err != nil {
				return nil, err
			}
			out[i] = append([]byte(nil), b...)
		case typeFixedLenByte:
			b, err := need(typeLength)
			if err != nil {
				return nil, err
			}
			out[i] = append([]byte(nil), b...)
		default:
			return nil, errors.New("unsupported PLAIN primitive type")
		}
	}
	return out, nil
}
