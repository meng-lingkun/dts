package mysqlbinlog

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
)

// MySQL binary JSON type codes. The row-event JSON column payload begins with
// one of these bytes followed by the type-specific binary representation.
const (
	jsonSmallObject byte = 0x00
	jsonLargeObject byte = 0x01
	jsonSmallArray  byte = 0x02
	jsonLargeArray  byte = 0x03
	jsonLiteral     byte = 0x04
	jsonInt16       byte = 0x05
	jsonUint16      byte = 0x06
	jsonInt32       byte = 0x07
	jsonUint32      byte = 0x08
	jsonInt64       byte = 0x09
	jsonUint64      byte = 0x0a
	jsonDouble      byte = 0x0b
	jsonString      byte = 0x0c
	jsonOpaque      byte = 0x0f
)

func readJSONVarInt(b []byte) (uint64, int, error) {
	var v uint64
	var shift uint
	for i := 0; i < len(b) && i < 10; i++ {
		c := b[i]
		v |= uint64(c&0x7f) << shift
		if c&0x80 == 0 {
			return v, i + 1, nil
		}
		shift += 7
		if shift >= 64 {
			break
		}
	}
	return 0, 0, errors.New("invalid binary JSON variable-length integer")
}

func jsonReadOffset(b []byte, width int) (uint64, error) {
	if len(b) < width {
		return 0, errors.New("short binary JSON offset")
	}
	switch width {
	case 2:
		return uint64(binary.LittleEndian.Uint16(b[:2])), nil
	case 4:
		return uint64(binary.LittleEndian.Uint32(b[:4])), nil
	default:
		return 0, fmt.Errorf("invalid binary JSON offset width %d", width)
	}
}

func jsonInlineType(t byte, large bool) bool {
	switch t {
	case jsonLiteral, jsonInt16, jsonUint16:
		return true
	case jsonInt32, jsonUint32:
		return large
	default:
		return false
	}
}

func decodeJSONInline(t byte, slot []byte, large bool) (any, error) {
	width := 2
	if large {
		width = 4
	}
	if len(slot) < width {
		return nil, errors.New("short binary JSON inline value")
	}
	switch t {
	case jsonLiteral:
		switch slot[0] {
		case 0x00:
			return nil, nil
		case 0x01:
			return true, nil
		case 0x02:
			return false, nil
		default:
			return nil, fmt.Errorf("invalid binary JSON literal %d", slot[0])
		}
	case jsonInt16:
		return int64(int16(binary.LittleEndian.Uint16(slot[:2]))), nil
	case jsonUint16:
		return uint64(binary.LittleEndian.Uint16(slot[:2])), nil
	case jsonInt32:
		if !large {
			return nil, errors.New("INT32 is not inline in small binary JSON container")
		}
		return int64(int32(binary.LittleEndian.Uint32(slot[:4]))), nil
	case jsonUint32:
		if !large {
			return nil, errors.New("UINT32 is not inline in small binary JSON container")
		}
		return uint64(binary.LittleEndian.Uint32(slot[:4])), nil
	default:
		return nil, fmt.Errorf("binary JSON type 0x%x is not inline", t)
	}
}

func decodeJSONOpaque(payload []byte) (any, error) {
	if len(payload) < 2 {
		return nil, errors.New("short MySQL binary JSON OPAQUE value")
	}
	mysqlType := payload[0]
	ln, n, err := readJSONVarInt(payload[1:])
	if err != nil {
		return nil, fmt.Errorf("decode OPAQUE length: %w", err)
	}
	off := 1 + n
	if ln > uint64(len(payload)-off) {
		return nil, errors.New("MySQL binary JSON OPAQUE value exceeds payload")
	}
	data := payload[off : off+int(ln)]
	switch mysqlType {
	case TypeNewDecimal:
		if len(data) < 2 {
			return nil, errors.New("short OPAQUE NEWDECIMAL metadata")
		}
		precision, scale := int(data[0]), int(data[1])
		text, consumed, err := decodeNewDecimal(data[2:], precision, scale)
		if err != nil {
			return nil, fmt.Errorf("decode OPAQUE DECIMAL(%d,%d): %w", precision, scale, err)
		}
		if consumed != len(data)-2 {
			return nil, fmt.Errorf("OPAQUE DECIMAL consumed %d of %d bytes", consumed, len(data)-2)
		}
		return json.Number(text), nil
	case TypeDate, TypeDateTime, TypeTimestamp:
		if len(data) != 8 {
			return nil, fmt.Errorf("OPAQUE temporal type %d requires 8 bytes, got %d", mysqlType, len(data))
		}
		v := int64(binary.LittleEndian.Uint64(data))
		if v < 0 {
			v = -v
		}
		intPart := v >> 24
		ymd := intPart >> 17
		ym := ymd >> 5
		day := int(ymd & 31)
		month := int(ym % 13)
		year := int(ym / 13)
		if mysqlType == TypeDate {
			return fmt.Sprintf("%04d-%02d-%02d", year, month, day), nil
		}
		hms := intPart & ((1 << 17) - 1)
		hour := int(hms >> 12)
		minute := int((hms >> 6) & 63)
		second := int(hms & 63)
		micro := int(v & ((1 << 24) - 1))
		return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%06d", year, month, day, hour, minute, second, micro), nil
	case TypeTime:
		if len(data) != 8 {
			return nil, fmt.Errorf("OPAQUE TIME requires 8 bytes, got %d", len(data))
		}
		v := int64(binary.LittleEndian.Uint64(data))
		neg := v < 0
		if neg {
			v = -v
		}
		intPart := v >> 24
		hour := int((intPart >> 12) & 1023)
		minute := int((intPart >> 6) & 63)
		second := int(intPart & 63)
		micro := int(v & ((1 << 24) - 1))
		prefix := ""
		if neg {
			prefix = "-"
		}
		return fmt.Sprintf("%s%02d:%02d:%02d.%06d", prefix, hour, minute, second, micro), nil
	default:
		return nil, fmt.Errorf("MySQL binary JSON OPAQUE subtype %d is not supported", mysqlType)
	}
}

func decodeJSONValue(t byte, payload []byte, depth int) (any, error) {
	if depth > 100 {
		return nil, errors.New("binary JSON nesting exceeds 100 levels")
	}
	switch t {
	case jsonSmallObject:
		return decodeJSONContainer(payload, false, true, depth+1)
	case jsonLargeObject:
		return decodeJSONContainer(payload, true, true, depth+1)
	case jsonSmallArray:
		return decodeJSONContainer(payload, false, false, depth+1)
	case jsonLargeArray:
		return decodeJSONContainer(payload, true, false, depth+1)
	case jsonLiteral:
		if len(payload) < 1 {
			return nil, errors.New("short binary JSON literal")
		}
		switch payload[0] {
		case 0x00:
			return nil, nil
		case 0x01:
			return true, nil
		case 0x02:
			return false, nil
		default:
			return nil, fmt.Errorf("invalid binary JSON literal %d", payload[0])
		}
	case jsonInt16:
		if len(payload) < 2 {
			return nil, errors.New("short binary JSON int16")
		}
		return int64(int16(binary.LittleEndian.Uint16(payload[:2]))), nil
	case jsonUint16:
		if len(payload) < 2 {
			return nil, errors.New("short binary JSON uint16")
		}
		return uint64(binary.LittleEndian.Uint16(payload[:2])), nil
	case jsonInt32:
		if len(payload) < 4 {
			return nil, errors.New("short binary JSON int32")
		}
		return int64(int32(binary.LittleEndian.Uint32(payload[:4]))), nil
	case jsonUint32:
		if len(payload) < 4 {
			return nil, errors.New("short binary JSON uint32")
		}
		return uint64(binary.LittleEndian.Uint32(payload[:4])), nil
	case jsonInt64:
		if len(payload) < 8 {
			return nil, errors.New("short binary JSON int64")
		}
		return int64(binary.LittleEndian.Uint64(payload[:8])), nil
	case jsonUint64:
		if len(payload) < 8 {
			return nil, errors.New("short binary JSON uint64")
		}
		return binary.LittleEndian.Uint64(payload[:8]), nil
	case jsonDouble:
		if len(payload) < 8 {
			return nil, errors.New("short binary JSON double")
		}
		v := math.Float64frombits(binary.LittleEndian.Uint64(payload[:8]))
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return nil, errors.New("non-finite binary JSON double")
		}
		return v, nil
	case jsonString:
		ln, n, err := readJSONVarInt(payload)
		if err != nil {
			return nil, err
		}
		if ln > uint64(len(payload)-n) {
			return nil, errors.New("binary JSON string exceeds payload")
		}
		return string(payload[n : n+int(ln)]), nil
	case jsonOpaque:
		return decodeJSONOpaque(payload)
	default:
		return nil, fmt.Errorf("unsupported MySQL binary JSON type 0x%x", t)
	}
}

func decodeJSONContainer(data []byte, large, object bool, depth int) (any, error) {
	width := 2
	if large {
		width = 4
	}
	header := width * 2
	if len(data) < header {
		return nil, errors.New("short binary JSON container header")
	}
	count64, err := jsonReadOffset(data[:width], width)
	if err != nil {
		return nil, err
	}
	size64, err := jsonReadOffset(data[width:header], width)
	if err != nil {
		return nil, err
	}
	if size64 > uint64(len(data)) || size64 < uint64(header) {
		return nil, fmt.Errorf("binary JSON container size %d exceeds payload %d", size64, len(data))
	}
	if count64 > 1<<24 {
		return nil, errors.New("binary JSON container element count too large")
	}
	count := int(count64)
	data = data[:int(size64)]

	keyEntrySize := width + 2
	valueEntrySize := width + 1
	keysStart := header
	valuesStart := header
	if object {
		valuesStart += count * keyEntrySize
	}
	entriesEnd := valuesStart + count*valueEntrySize
	if entriesEnd > len(data) {
		return nil, errors.New("binary JSON entry table exceeds container")
	}

	keys := make([]string, count)
	if object {
		for i := 0; i < count; i++ {
			entry := data[keysStart+i*keyEntrySize : keysStart+(i+1)*keyEntrySize]
			off, err := jsonReadOffset(entry[:width], width)
			if err != nil {
				return nil, err
			}
			keyLen := int(binary.LittleEndian.Uint16(entry[width : width+2]))
			if off > uint64(len(data)) || keyLen < 0 || int(off)+keyLen > len(data) {
				return nil, fmt.Errorf("binary JSON key %d outside container", i)
			}
			keys[i] = string(data[int(off) : int(off)+keyLen])
		}
	}

	values := make([]any, count)
	for i := 0; i < count; i++ {
		entry := data[valuesStart+i*valueEntrySize : valuesStart+(i+1)*valueEntrySize]
		t := entry[0]
		slot := entry[1:]
		if jsonInlineType(t, large) {
			v, err := decodeJSONInline(t, slot, large)
			if err != nil {
				return nil, fmt.Errorf("binary JSON element %d inline: %w", i, err)
			}
			values[i] = v
			continue
		}
		off, err := jsonReadOffset(slot, width)
		if err != nil {
			return nil, err
		}
		if off >= uint64(len(data)) {
			return nil, fmt.Errorf("binary JSON element %d offset %d outside container", i, off)
		}
		v, err := decodeJSONValue(t, data[int(off):], depth)
		if err != nil {
			return nil, fmt.Errorf("binary JSON element %d: %w", i, err)
		}
		values[i] = v
	}

	if !object {
		return values, nil
	}
	out := make(map[string]any, count)
	for i := range keys {
		out[keys[i]] = values[i]
	}
	return out, nil
}

// DecodeBinaryJSON converts MySQL's internal binary JSON representation into
// standard JSON text suitable for MySQL, PolarDB-X and PostgreSQL JSON/JSONB.
// Unknown OPAQUE subtypes fail instead of returning corrupted JSON.
func DecodeBinaryJSON(data []byte) (string, error) {
	if len(data) == 0 {
		return "", errors.New("empty MySQL binary JSON")
	}
	v, err := decodeJSONValue(data[0], data[1:], 0)
	if err != nil {
		return "", err
	}
	buf, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(buf), nil
}
