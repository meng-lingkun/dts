package mysqlbinlog

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math"
	"qmigration/backend/internal/domain"
	"strconv"
	"strings"
	"time"
)

// MySQL field type identifiers used in row-based binlog events.
const (
	TypeDecimal    byte = 0
	TypeTiny       byte = 1
	TypeShort      byte = 2
	TypeLong       byte = 3
	TypeFloat      byte = 4
	TypeDouble     byte = 5
	TypeNull       byte = 6
	TypeTimestamp  byte = 7
	TypeLongLong   byte = 8
	TypeInt24      byte = 9
	TypeDate       byte = 10
	TypeTime       byte = 11
	TypeDateTime   byte = 12
	TypeYear       byte = 13
	TypeNewDate    byte = 14
	TypeVarchar    byte = 15
	TypeBit        byte = 16
	TypeTimestamp2 byte = 17
	TypeDateTime2  byte = 18
	TypeTime2      byte = 19
	TypeJSON       byte = 245
	TypeNewDecimal byte = 246
	TypeEnum       byte = 247
	TypeSet        byte = 248
	TypeTinyBlob   byte = 249
	TypeMediumBlob byte = 250
	TypeLongBlob   byte = 251
	TypeBlob       byte = 252
	TypeVarString  byte = 253
	TypeString     byte = 254
	TypeGeometry   byte = 255
)

type DecodedChange struct {
	Before []domain.CDCField `json:"before,omitempty"`
	After  []domain.CDCField `json:"after,omitempty"`
}

// ColumnMetadata expands TABLE_MAP_EVENT's compact metadata stream into one
// uint16 entry per column. It deliberately relies on information_schema column
// metadata for signedness and names, which is more portable across MySQL 5.7,
// 8.x, PolarDB MySQL, TiDB and OceanBase MySQL than optional table-map metadata.
func ColumnMetadata(tm *TableMap) ([]uint16, error) {
	if tm == nil {
		return nil, fmt.Errorf("nil table map")
	}
	out := make([]uint16, len(tm.ColumnTypes))
	p := 0
	need := func(n int) error {
		if p+n > len(tm.ColumnMeta) {
			return fmt.Errorf("table-map metadata truncated at column %d", len(out))
		}
		return nil
	}
	for i, t := range tm.ColumnTypes {
		switch t {
		case TypeString, TypeNewDecimal:
			if err := need(2); err != nil {
				return nil, err
			}
			out[i] = uint16(tm.ColumnMeta[p])<<8 | uint16(tm.ColumnMeta[p+1])
			p += 2
		case TypeVarString, TypeVarchar, TypeBit:
			if err := need(2); err != nil {
				return nil, err
			}
			out[i] = binary.LittleEndian.Uint16(tm.ColumnMeta[p : p+2])
			p += 2
		case TypeBlob, TypeDouble, TypeFloat, TypeGeometry, TypeJSON,
			TypeTime2, TypeDateTime2, TypeTimestamp2:
			if err := need(1); err != nil {
				return nil, err
			}
			out[i] = uint16(tm.ColumnMeta[p])
			p++
		default:
			out[i] = 0
		}
	}
	if p > len(tm.ColumnMeta) {
		return nil, fmt.Errorf("table-map metadata overrun")
	}
	return out, nil
}

func bitSet(bitmap []byte, i int) bool {
	return i >= 0 && i/8 < len(bitmap) && bitmap[i/8]&(1<<uint(i%8)) != 0
}

func presentCount(bitmap []byte, columns int) int {
	n := 0
	for i := 0; i < columns; i++ {
		if bitSet(bitmap, i) {
			n++
		}
	}
	return n
}

func isUnsignedColumn(c domain.ColumnInfo) bool {
	return strings.Contains(strings.ToLower(c.ColumnType), "unsigned")
}

func readLE(data []byte, n int) (uint64, error) {
	if n < 0 || n > 8 || len(data) < n {
		return 0, fmt.Errorf("short little-endian value need=%d have=%d", n, len(data))
	}
	var v uint64
	for i := 0; i < n; i++ {
		v |= uint64(data[i]) << (8 * i)
	}
	return v, nil
}

func readBE(data []byte, n int) (uint64, error) {
	if n < 0 || n > 8 || len(data) < n {
		return 0, fmt.Errorf("short big-endian value need=%d have=%d", n, len(data))
	}
	var v uint64
	for i := 0; i < n; i++ {
		v = v<<8 | uint64(data[i])
	}
	return v, nil
}

func signExtend(v uint64, bits uint) int64 {
	if bits == 0 || bits >= 64 {
		return int64(v)
	}
	sign := uint64(1) << (bits - 1)
	if v&sign != 0 {
		v |= ^((uint64(1) << bits) - 1)
	}
	return int64(v)
}

func fractionalBytes(fsp uint16) int { return int((fsp + 1) / 2) }

func decodeFractionBE(data []byte, fsp uint16) (micro int64, n int, err error) {
	n = fractionalBytes(fsp)
	if len(data) < n {
		return 0, 0, fmt.Errorf("short temporal fraction")
	}
	if n == 0 {
		return 0, 0, nil
	}
	v, err := readBE(data, n)
	if err != nil {
		return 0, 0, err
	}
	switch fsp {
	case 1, 2:
		micro = int64(v) * 10000
	case 3, 4:
		micro = int64(v) * 100
	default:
		micro = int64(v)
	}
	return micro, n, nil
}

func formatFraction(micro int64, fsp uint16) string {
	if fsp == 0 {
		return ""
	}
	if micro < 0 {
		micro = -micro
	}
	s := fmt.Sprintf("%06d", micro)
	if int(fsp) < len(s) {
		s = s[:fsp]
	}
	return "." + s
}

func decodeDatetime2(data []byte, fsp uint16) (string, int, error) {
	need := 5 + fractionalBytes(fsp)
	if len(data) < need {
		return "", 0, fmt.Errorf("short DATETIME2")
	}
	raw, _ := readBE(data[:5], 5)
	intPart := int64(raw) - 0x8000000000
	micro, _, err := decodeFractionBE(data[5:], fsp)
	if err != nil {
		return "", 0, err
	}
	if intPart == 0 {
		return "0000-00-00 00:00:00" + formatFraction(micro, fsp), need, nil
	}
	if intPart < 0 {
		intPart = -intPart
	}
	ymdhms := intPart
	ymd := ymdhms >> 17
	ym := ymd >> 5
	day := int(ymd & 31)
	month := int(ym % 13)
	year := int(ym / 13)
	hms := ymdhms & ((1 << 17) - 1)
	hour := int(hms >> 12)
	minute := int((hms >> 6) & 63)
	second := int(hms & 63)
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d%s", year, month, day, hour, minute, second, formatFraction(micro, fsp)), need, nil
}

func decodeTimestamp2(data []byte, fsp uint16) (string, int, error) {
	need := 4 + fractionalBytes(fsp)
	if len(data) < need {
		return "", 0, fmt.Errorf("short TIMESTAMP2")
	}
	sec := int64(binary.BigEndian.Uint32(data[:4]))
	micro, _, err := decodeFractionBE(data[4:], fsp)
	if err != nil {
		return "", 0, err
	}
	if sec == 0 {
		return "0000-00-00 00:00:00" + formatFraction(micro, fsp), need, nil
	}
	return time.Unix(sec, micro*1000).UTC().Format("2006-01-02 15:04:05") + formatFraction(micro, fsp), need, nil
}

func decodeTime2(data []byte, fsp uint16) (string, int, error) {
	need := 3 + fractionalBytes(fsp)
	if len(data) < need {
		return "", 0, fmt.Errorf("short TIME2")
	}
	raw, _ := readBE(data[:3], 3)
	intPart := int64(raw) - 0x800000
	micro, fracBytes, err := decodeFractionBE(data[3:], fsp)
	if err != nil {
		return "", 0, err
	}
	// Negative fractional values use reverse storage order. Handle the carry so
	// the formatted value matches MySQL's logical TIME value.
	if intPart < 0 && fracBytes > 0 && micro != 0 {
		intPart++
		unit := int64(1)
		switch fsp {
		case 1, 2:
			unit = 256 * 10000
		case 3, 4:
			unit = 65536 * 100
		default:
			unit = 1 << 24
		}
		micro -= unit
	}
	neg := intPart < 0 || micro < 0
	if intPart < 0 {
		intPart = -intPart
	}
	if micro < 0 {
		micro = -micro
	}
	hour := (intPart >> 12) & 1023
	minute := (intPart >> 6) & 63
	second := intPart & 63
	prefix := ""
	if neg {
		prefix = "-"
	}
	return fmt.Sprintf("%s%02d:%02d:%02d%s", prefix, hour, minute, second, formatFraction(micro, fsp)), need, nil
}

var decimalCompressed = [...]int{0, 1, 1, 2, 2, 3, 3, 4, 4, 4}

func decodeDecimalGroup(data []byte, digits int, mask byte) (uint32, int, error) {
	if digits < 0 || digits >= len(decimalCompressed) {
		return 0, 0, fmt.Errorf("invalid decimal group digits %d", digits)
	}
	n := decimalCompressed[digits]
	if len(data) < n {
		return 0, 0, fmt.Errorf("short DECIMAL group")
	}
	var v uint32
	for i := 0; i < n; i++ {
		v = v<<8 | uint32(data[i]^mask)
	}
	return v, n, nil
}

func decodeNewDecimal(data []byte, precision, scale int) (string, int, error) {
	if precision <= 0 || scale < 0 || scale > precision {
		return "", 0, fmt.Errorf("invalid DECIMAL(%d,%d)", precision, scale)
	}
	intDigits := precision - scale
	intGroups, intPartial := intDigits/9, intDigits%9
	fracGroups, fracPartial := scale/9, scale%9
	n := intGroups*4 + decimalCompressed[intPartial] + fracGroups*4 + decimalCompressed[fracPartial]
	if len(data) < n {
		return "", 0, fmt.Errorf("short NEWDECIMAL need=%d have=%d", n, len(data))
	}
	buf := append([]byte(nil), data[:n]...)
	negative := buf[0]&0x80 == 0
	mask := byte(0)
	if negative {
		mask = 0xff
	}
	buf[0] ^= 0x80
	pos := 0
	var b strings.Builder
	if negative {
		b.WriteByte('-')
	}
	leading := true
	if intPartial > 0 {
		v, k, e := decodeDecimalGroup(buf[pos:], intPartial, mask)
		if e != nil {
			return "", 0, e
		}
		pos += k
		if v != 0 {
			b.WriteString(strconv.FormatUint(uint64(v), 10))
			leading = false
		}
	}
	for i := 0; i < intGroups; i++ {
		if pos+4 > len(buf) {
			return "", 0, fmt.Errorf("short DECIMAL integer group")
		}
		v := binary.BigEndian.Uint32(buf[pos:pos+4]) ^ uint32(mask)*0x01010101
		pos += 4
		if leading {
			if v != 0 {
				b.WriteString(strconv.FormatUint(uint64(v), 10))
				leading = false
			}
		} else {
			b.WriteString(fmt.Sprintf("%09d", v))
		}
	}
	if leading {
		b.WriteByte('0')
	}
	if scale > 0 {
		b.WriteByte('.')
		for i := 0; i < fracGroups; i++ {
			if pos+4 > len(buf) {
				return "", 0, fmt.Errorf("short DECIMAL fractional group")
			}
			v := binary.BigEndian.Uint32(buf[pos:pos+4]) ^ uint32(mask)*0x01010101
			pos += 4
			b.WriteString(fmt.Sprintf("%09d", v))
		}
		if fracPartial > 0 {
			v, k, e := decodeDecimalGroup(buf[pos:], fracPartial, mask)
			if e != nil {
				return "", 0, e
			}
			pos += k
			b.WriteString(fmt.Sprintf("%0*d", fracPartial, v))
		}
	}
	return b.String(), n, nil
}

func decodeBlob(data []byte, pack int) ([]byte, int, error) {
	if pack < 1 || pack > 4 {
		return nil, 0, fmt.Errorf("invalid blob length bytes %d", pack)
	}
	l, err := readLE(data, pack)
	if err != nil {
		return nil, 0, err
	}
	if l > uint64(len(data)-pack) {
		return nil, 0, fmt.Errorf("blob length %d exceeds remaining %d", l, len(data)-pack)
	}
	n := pack + int(l)
	return append([]byte(nil), data[pack:n]...), n, nil
}

func decodeString(data []byte, max int) (string, int, error) {
	prefix := 1
	if max >= 256 {
		prefix = 2
	}
	l, err := readLE(data, prefix)
	if err != nil {
		return "", 0, err
	}
	if l > uint64(len(data)-prefix) {
		return "", 0, fmt.Errorf("string length %d exceeds remaining", l)
	}
	n := prefix + int(l)
	return string(data[prefix:n]), n, nil
}

func realStringType(meta uint16) (tp byte, max int) {
	if meta < 256 {
		return TypeString, int(meta)
	}
	b0, b1 := byte(meta>>8), byte(meta)
	if b0&0x30 != 0x30 {
		max = int(uint16(b1) | (uint16((b0&0x30)^0x30) << 4))
		tp = b0 | 0x30
	} else {
		max = int(b1)
		tp = b0
	}
	return
}

func decodeValue(data []byte, tp byte, meta uint16, col domain.ColumnInfo) (domain.CDCField, int, error) {
	f := domain.CDCField{Column: col.Name}
	need := func(n int) error {
		if len(data) < n {
			return fmt.Errorf("column %s type=%d needs %d bytes, have %d", col.Name, tp, n, len(data))
		}
		return nil
	}
	unsigned := isUnsignedColumn(col)
	switch tp {
	case TypeNull:
		f.Null = true
		return f, 0, nil
	case TypeTiny:
		if e := need(1); e != nil {
			return f, 0, e
		}
		if unsigned {
			f.Value = strconv.FormatUint(uint64(data[0]), 10)
		} else {
			f.Value = strconv.FormatInt(int64(int8(data[0])), 10)
		}
		return f, 1, nil
	case TypeShort:
		if e := need(2); e != nil {
			return f, 0, e
		}
		v := binary.LittleEndian.Uint16(data)
		if unsigned {
			f.Value = strconv.FormatUint(uint64(v), 10)
		} else {
			f.Value = strconv.FormatInt(int64(int16(v)), 10)
		}
		return f, 2, nil
	case TypeLong:
		if e := need(4); e != nil {
			return f, 0, e
		}
		v := binary.LittleEndian.Uint32(data)
		if unsigned {
			f.Value = strconv.FormatUint(uint64(v), 10)
		} else {
			f.Value = strconv.FormatInt(int64(int32(v)), 10)
		}
		return f, 4, nil
	case TypeInt24:
		v, e := readLE(data, 3)
		if e != nil {
			return f, 0, e
		}
		if unsigned {
			f.Value = strconv.FormatUint(v, 10)
		} else {
			f.Value = strconv.FormatInt(signExtend(v, 24), 10)
		}
		return f, 3, nil
	case TypeLongLong:
		if e := need(8); e != nil {
			return f, 0, e
		}
		v := binary.LittleEndian.Uint64(data)
		if unsigned {
			f.Value = strconv.FormatUint(v, 10)
		} else {
			f.Value = strconv.FormatInt(int64(v), 10)
		}
		return f, 8, nil
	case TypeFloat:
		if e := need(4); e != nil {
			return f, 0, e
		}
		f.Value = strconv.FormatFloat(float64(math.Float32frombits(binary.LittleEndian.Uint32(data))), 'g', -1, 32)
		return f, 4, nil
	case TypeDouble:
		if e := need(8); e != nil {
			return f, 0, e
		}
		f.Value = strconv.FormatFloat(math.Float64frombits(binary.LittleEndian.Uint64(data)), 'g', -1, 64)
		return f, 8, nil
	case TypeNewDecimal:
		v, n, e := decodeNewDecimal(data, int(meta>>8), int(meta&0xff))
		f.Value = v
		return f, n, e
	case TypeBit:
		bits := int((meta>>8)*8 + (meta & 0xff))
		n := (bits + 7) / 8
		v, e := readBE(data, n)
		if e != nil {
			return f, 0, e
		}
		f.Value = strconv.FormatUint(v, 10)
		return f, n, nil
	case TypeTimestamp:
		if e := need(4); e != nil {
			return f, 0, e
		}
		v := binary.LittleEndian.Uint32(data)
		if v == 0 {
			f.Value = "0000-00-00 00:00:00"
		} else {
			f.Value = time.Unix(int64(v), 0).UTC().Format("2006-01-02 15:04:05")
		}
		return f, 4, nil
	case TypeTimestamp2:
		v, n, e := decodeTimestamp2(data, meta)
		f.Value = v
		return f, n, e
	case TypeDateTime:
		if e := need(8); e != nil {
			return f, 0, e
		}
		v := binary.LittleEndian.Uint64(data)
		if v == 0 {
			f.Value = "0000-00-00 00:00:00"
		} else {
			d, t := v/1000000, v%1000000
			f.Value = fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", d/10000, (d%10000)/100, d%100, t/10000, (t%10000)/100, t%100)
		}
		return f, 8, nil
	case TypeDateTime2:
		v, n, e := decodeDatetime2(data, meta)
		f.Value = v
		return f, n, e
	case TypeTime:
		v, e := readLE(data, 3)
		if e != nil {
			return f, 0, e
		}
		f.Value = fmt.Sprintf("%02d:%02d:%02d", v/10000, (v%10000)/100, v%100)
		return f, 3, nil
	case TypeTime2:
		v, n, e := decodeTime2(data, meta)
		f.Value = v
		return f, n, e
	case TypeDate, TypeNewDate:
		v, e := readLE(data, 3)
		if e != nil {
			return f, 0, e
		}
		if v == 0 {
			f.Value = "0000-00-00"
		} else {
			f.Value = fmt.Sprintf("%04d-%02d-%02d", v/(16*32), (v/32)%16, v%32)
		}
		return f, 3, nil
	case TypeYear:
		if e := need(1); e != nil {
			return f, 0, e
		}
		if data[0] == 0 {
			f.Value = "0"
		} else {
			f.Value = strconv.Itoa(int(data[0]) + 1900)
		}
		return f, 1, nil
	case TypeVarchar, TypeVarString:
		v, n, e := decodeString(data, int(meta))
		f.Value = v
		return f, n, e
	case TypeString:
		real, max := realStringType(meta)
		if real == TypeEnum {
			pack := max
			if pack < 1 || pack > 2 {
				pack = int(meta & 0xff)
			}
			v, e := readLE(data, pack)
			if e != nil {
				return f, 0, e
			}
			f.Value = strconv.FormatUint(v, 10)
			return f, pack, nil
		}
		if real == TypeSet {
			pack := max
			if pack < 1 || pack > 8 {
				pack = int(meta & 0xff)
			}
			v, e := readLE(data, pack)
			if e != nil {
				return f, 0, e
			}
			f.Value = strconv.FormatUint(v, 10)
			return f, pack, nil
		}
		v, n, e := decodeString(data, max)
		f.Value = v
		return f, n, e
	case TypeBlob, TypeTinyBlob, TypeMediumBlob, TypeLongBlob, TypeGeometry:
		pack := int(meta)
		if pack == 0 {
			switch tp {
			case TypeTinyBlob:
				pack = 1
			case TypeMediumBlob:
				pack = 3
			case TypeLongBlob:
				pack = 4
			default:
				pack = 2
			}
		}
		v, n, e := decodeBlob(data, pack)
		f.Value = base64.StdEncoding.EncodeToString(v)
		f.Encoding = "base64"
		return f, n, e
	case TypeJSON:
		v, n, e := decodeBlob(data, int(meta))
		if e != nil {
			return f, n, e
		}
		text, e := DecodeBinaryJSON(v)
		if e != nil {
			return f, n, fmt.Errorf("decode MySQL binary JSON column %s: %w", col.Name, e)
		}
		f.Value = text
		f.Encoding = "json"
		return f, n, nil
	default:
		return f, 0, fmt.Errorf("unsupported MySQL binlog column type %d for %s", tp, col.Name)
	}
}

func decodeImage(data []byte, bitmap []byte, tm *TableMap, columns []domain.ColumnInfo, metas []uint16) ([]domain.CDCField, int, error) {
	if len(columns) != len(tm.ColumnTypes) {
		return nil, 0, fmt.Errorf("column metadata mismatch table=%s.%s db=%d binlog=%d", tm.Schema, tm.Table, len(columns), len(tm.ColumnTypes))
	}
	count := presentCount(bitmap, len(columns))
	nullBytes := (count + 7) / 8
	if len(data) < nullBytes {
		return nil, 0, fmt.Errorf("short row null bitmap")
	}
	nullMap := data[:nullBytes]
	pos := nullBytes
	nullIndex := 0
	fields := make([]domain.CDCField, 0, count)
	for i := range columns {
		if !bitSet(bitmap, i) {
			continue
		}
		if bitSet(nullMap, nullIndex) {
			fields = append(fields, domain.CDCField{Column: columns[i].Name, Null: true})
			nullIndex++
			continue
		}
		nullIndex++
		f, n, err := decodeValue(data[pos:], tm.ColumnTypes[i], metas[i], columns[i])
		if err != nil {
			return nil, 0, err
		}
		pos += n
		fields = append(fields, f)
	}
	return fields, pos, nil
}

func findCDCField(fields []domain.CDCField, column string) (domain.CDCField, bool) {
	for _, field := range fields {
		if strings.EqualFold(field.Column, column) {
			return field, true
		}
	}
	return domain.CDCField{}, false
}

func decodePartialUpdateAfterImage(data []byte, bitmap []byte, tm *TableMap, columns []domain.ColumnInfo, metas []uint16, before []domain.CDCField) ([]domain.CDCField, int, error) {
	options, n, err := readLenEnc(data)
	if err != nil {
		return nil, 0, fmt.Errorf("decode binlog_row_value_options: %w", err)
	}
	if options&^uint64(1) != 0 {
		return nil, 0, fmt.Errorf("unsupported binlog_row_value_options 0x%x", options)
	}
	pos := n
	jsonColumns := 0
	for _, typ := range tm.ColumnTypes {
		if typ == TypeJSON {
			jsonColumns++
		}
	}
	var partialBitmap []byte
	if options&1 != 0 {
		partialBytes := (jsonColumns + 7) / 8
		if len(data)-pos < partialBytes {
			return nil, 0, fmt.Errorf("short partial JSON bitmap")
		}
		partialBitmap = data[pos : pos+partialBytes]
		pos += partialBytes
	}
	count := presentCount(bitmap, len(columns))
	nullBytes := (count + 7) / 8
	if len(data)-pos < nullBytes {
		return nil, 0, fmt.Errorf("short partial-update null bitmap")
	}
	nullMap := data[pos : pos+nullBytes]
	pos += nullBytes
	nullIndex := 0
	partialIndex := 0
	fields := make([]domain.CDCField, 0, count)
	for i := range columns {
		isJSON := tm.ColumnTypes[i] == TypeJSON
		isPartialJSON := false
		if isJSON {
			isPartialJSON = len(partialBitmap) > 0 && bitSet(partialBitmap, partialIndex)
			partialIndex++
		}
		if !bitSet(bitmap, i) {
			continue
		}
		if bitSet(nullMap, nullIndex) {
			fields = append(fields, domain.CDCField{Column: columns[i].Name, Null: true})
			nullIndex++
			continue
		}
		nullIndex++
		if isPartialJSON {
			raw, consumed, e := decodeBlob(data[pos:], int(metas[i]))
			if e != nil {
				return nil, 0, fmt.Errorf("decode partial JSON blob for %s: %w", columns[i].Name, e)
			}
			pos += consumed
			diffs, e := ParseJSONDiffVector(raw)
			if e != nil {
				return nil, 0, fmt.Errorf("decode partial JSON diff vector for %s: %w", columns[i].Name, e)
			}
			old, ok := findCDCField(before, columns[i].Name)
			if !ok || old.Null || old.Encoding != "json" {
				return nil, 0, fmt.Errorf("partial JSON column %s requires a full JSON before-image; enforce binlog_row_image=FULL", columns[i].Name)
			}
			updated, e := ApplyJSONDiffVector(old.Value, diffs)
			if e != nil {
				return nil, 0, fmt.Errorf("apply partial JSON diff vector for %s: %w", columns[i].Name, e)
			}
			fields = append(fields, domain.CDCField{Column: columns[i].Name, Value: updated, Encoding: "json"})
			continue
		}
		f, consumed, e := decodeValue(data[pos:], tm.ColumnTypes[i], metas[i], columns[i])
		if e != nil {
			return nil, 0, e
		}
		pos += consumed
		fields = append(fields, f)
	}
	return fields, pos, nil
}

// DecodeRows converts one ROWS_EVENT_V2 payload into logical row changes.
// It supports multiple rows per event and UPDATE before/after image pairs.
func DecodeRows(tm *TableMap, rows *Rows, columns []domain.ColumnInfo) ([]DecodedChange, error) {
	if tm == nil || rows == nil {
		return nil, fmt.Errorf("nil table map/rows")
	}
	if tm.TableID != rows.TableID {
		return nil, fmt.Errorf("table id mismatch map=%d rows=%d", tm.TableID, rows.TableID)
	}
	metas, err := ColumnMetadata(tm)
	if err != nil {
		return nil, err
	}
	data := rows.RowData
	out := []DecodedChange{}
	for len(data) > 0 {
		before, n, err := decodeImage(data, rows.BeforeBitmap, tm, columns, metas)
		if err != nil {
			return nil, err
		}
		if n <= 0 {
			return nil, fmt.Errorf("row decoder made no progress")
		}
		data = data[n:]
		ch := DecodedChange{}
		if rows.Update {
			ch.Before = before
			var after []domain.CDCField
			var m int
			if rows.EventType == PartialUpdateRowsEvent {
				after, m, err = decodePartialUpdateAfterImage(data, rows.AfterBitmap, tm, columns, metas, before)
			} else {
				after, m, err = decodeImage(data, rows.AfterBitmap, tm, columns, metas)
			}
			if err != nil {
				return nil, err
			}
			if m <= 0 {
				return nil, fmt.Errorf("update after-image made no progress")
			}
			data = data[m:]
			ch.After = after
		} else if rows.EventType == DeleteRowsEventV2 || rows.EventType == DeleteRowsEventV1 {
			ch.Before = before
		} else {
			ch.After = before
		}
		out = append(out, ch)
	}
	return out, nil
}
