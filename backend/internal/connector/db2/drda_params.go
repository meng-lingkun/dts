package db2connector

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"

	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
)

type db2ParamEncoding struct {
	typ    byte
	length uint16
	lob    bool
	clob   bool
}

func db2DecimalShape(col domain.ColumnInfo) (int, int, error) {
	t := db2TargetType(col)
	i := strings.IndexByte(t, '(')
	j := strings.LastIndexByte(t, ')')
	if i < 0 || j <= i {
		return 31, 10, nil
	}
	parts := strings.Split(t[i+1:j], ",")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("DB2 DECIMAL type %q is missing precision/scale", t)
	}
	p, e := strconv.Atoi(strings.TrimSpace(parts[0]))
	if e != nil || p < 1 || p > 31 {
		return 0, 0, fmt.Errorf("DB2 DECIMAL precision in %q is invalid", t)
	}
	s, e := strconv.Atoi(strings.TrimSpace(parts[1]))
	if e != nil || s < 0 || s > p {
		return 0, 0, fmt.Errorf("DB2 DECIMAL scale in %q is invalid", t)
	}
	return p, s, nil
}

func db2ParamEncodingFor(col domain.ColumnInfo, v connector.Value) (db2ParamEncoding, error) {
	if strings.EqualFold(strings.TrimSpace(col.DataType), "vector") {
		spec, err := db2VectorSpecForColumn(col)
		if err != nil {
			return db2ParamEncoding{}, err
		}
		if !v.Null {
			if err := validateDB2VectorString(v.Raw, spec); err != nil {
				return db2ParamEncoding{}, err
			}
		}
		if !v.Null && len(v.Raw) > drdaInlineParam {
			return db2ParamEncoding{typ: drdaNLOBCSBCS, length: 0x8009, lob: true, clob: true}, nil
		}
		return db2ParamEncoding{typ: drdaNVarMix, length: 0x7fff}, nil
	}
	target := strings.ToUpper(db2TargetType(col))
	var e db2ParamEncoding
	switch {
	case strings.HasPrefix(target, "SMALLINT"):
		e = db2ParamEncoding{typ: drdaNSmall, length: 2}
	case strings.HasPrefix(target, "INTEGER"):
		e = db2ParamEncoding{typ: drdaNInteger, length: 4}
	case strings.HasPrefix(target, "BIGINT"):
		e = db2ParamEncoding{typ: drdaNInteger8, length: 8}
	case strings.HasPrefix(target, "DECIMAL"), strings.HasPrefix(target, "NUMERIC"):
		p, s, err := db2DecimalShape(col)
		if err != nil {
			return e, err
		}
		e = db2ParamEncoding{typ: drdaNDecimal, length: uint16(p)<<8 | uint16(s)}
	case strings.HasPrefix(target, "REAL"):
		e = db2ParamEncoding{typ: drdaNFloat4, length: 4}
	case strings.HasPrefix(target, "DOUBLE"), strings.HasPrefix(target, "FLOAT"), strings.HasPrefix(target, "DECFLOAT"):
		e = db2ParamEncoding{typ: drdaNFloat8, length: 8}
	case strings.HasPrefix(target, "BOOLEAN"):
		e = db2ParamEncoding{typ: drdaNBoolean, length: 1}
	case strings.HasPrefix(target, "DATE"):
		e = db2ParamEncoding{typ: drdaNDate, length: 10}
	case strings.HasPrefix(target, "TIME") && !strings.HasPrefix(target, "TIMESTAMP"):
		e = db2ParamEncoding{typ: drdaNTime, length: 8}
	case strings.HasPrefix(target, "TIMESTAMP"):
		e = db2ParamEncoding{typ: drdaNTimestamp, length: 32}
	case strings.HasPrefix(target, "BLOB"):
		if !v.Null && len(v.Raw) > drdaInlineParam {
			e = db2ParamEncoding{typ: drdaNLOBBytes, length: 0x8009, lob: true}
		} else {
			e = db2ParamEncoding{typ: drdaNVarBinary, length: 0x7fff}
		}
	case strings.HasPrefix(target, "VARBINARY"), strings.HasPrefix(target, "BINARY"):
		e = db2ParamEncoding{typ: drdaNVarBinary, length: uint16(max(1, min(32672, extractTypeLength(target))))}
	case strings.HasPrefix(target, "CLOB"):
		if !v.Null && len(v.Raw) > drdaInlineParam {
			e = db2ParamEncoding{typ: drdaNLOBCSBCS, length: 0x8009, lob: true, clob: true}
		} else {
			e = db2ParamEncoding{typ: drdaNVarMix, length: 0x7fff}
		}
	default:
		e = db2ParamEncoding{typ: drdaNVarMix, length: 0x7fff}
	}
	return e, nil
}

func expandDB2Decimal(raw string) (negative bool, whole, frac string, err error) {
	if err = connector.ValidateNumericLiteral([]byte(raw), false); err != nil {
		return false, "", "", err
	}
	raw = strings.TrimSpace(raw)
	if raw[0] == '+' || raw[0] == '-' {
		negative = raw[0] == '-'
		raw = raw[1:]
	}
	exp := 0
	if i := strings.IndexAny(raw, "eE"); i >= 0 {
		exp, err = strconv.Atoi(raw[i+1:])
		if err != nil || exp < -1000 || exp > 1000 {
			return false, "", "", errors.New("DB2 decimal exponent out of supported range")
		}
		raw = raw[:i]
	}
	parts := strings.SplitN(raw, ".", 2)
	intPart := parts[0]
	fracPart := ""
	if len(parts) == 2 {
		fracPart = parts[1]
	}
	if intPart == "" {
		intPart = "0"
	}
	digits := intPart + fracPart
	dot := len(intPart) + exp
	switch {
	case dot <= 0:
		whole = "0"
		frac = strings.Repeat("0", -dot) + digits
	case dot >= len(digits):
		whole = digits + strings.Repeat("0", dot-len(digits))
	case dot > 0:
		whole, frac = digits[:dot], digits[dot:]
	}
	whole = strings.TrimLeft(whole, "0")
	if whole == "" {
		whole = "0"
	}
	return negative, whole, frac, nil
}

func encodeDB2PackedDecimal(raw string, precision, scale int) ([]byte, error) {
	neg, whole, frac, err := expandDB2Decimal(raw)
	if err != nil {
		return nil, err
	}
	if len(frac) > scale {
		for _, ch := range frac[scale:] {
			if ch != '0' {
				return nil, fmt.Errorf("DB2 decimal %q cannot fit DECIMAL(%d,%d) without rounding", raw, precision, scale)
			}
		}
		frac = frac[:scale]
	}
	frac += strings.Repeat("0", scale-len(frac))
	unscaled := whole + frac
	trimmed := strings.TrimLeft(unscaled, "0")
	if trimmed == "" {
		trimmed = "0"
		neg = false
	}
	if len(trimmed) > precision {
		return nil, fmt.Errorf("DB2 decimal %q does not fit DECIMAL(%d,%d)", raw, precision, scale)
	}
	digits := strings.Repeat("0", precision-len(trimmed)) + trimmed
	if len(digits)%2 == 0 {
		digits = "0" + digits
	}
	nibbles := make([]byte, 0, len(digits)+1)
	for i := range digits {
		nibbles = append(nibbles, digits[i]-'0')
	}
	if neg {
		nibbles = append(nibbles, 0x0d)
	} else {
		nibbles = append(nibbles, 0x0c)
	}
	out := make([]byte, 0, len(nibbles)/2)
	for i := 0; i < len(nibbles); i += 2 {
		out = append(out, nibbles[i]<<4|nibbles[i+1])
	}
	return out, nil
}

func encodeDB2Param(dst []byte, enc db2ParamEncoding, col domain.ColumnInfo, v connector.Value, endian binary.ByteOrder) ([]byte, []byte, error) {
	if v.Null {
		return append(dst, 0xff), nil, nil
	}
	if len(v.Raw) > drdaMaxLOBBytes {
		return nil, nil, fmt.Errorf("DB2 parameter %s exceeds %d-byte safety limit", col.Name, drdaMaxLOBBytes)
	}
	dst = append(dst, 0x00)
	raw := strings.TrimSpace(string(v.Raw))
	switch enc.typ {
	case drdaNSmall:
		if err := connector.ValidateNumericLiteral(v.Raw, false); err != nil {
			return nil, nil, err
		}
		n, err := strconv.ParseInt(raw, 10, 16)
		if err != nil {
			return nil, nil, fmt.Errorf("DB2 SMALLINT %q: %w", raw, err)
		}
		var b [2]byte
		endian.PutUint16(b[:], uint16(int16(n)))
		return append(dst, b[:]...), nil, nil
	case drdaNInteger:
		if err := connector.ValidateNumericLiteral(v.Raw, false); err != nil {
			return nil, nil, err
		}
		n, err := strconv.ParseInt(raw, 10, 32)
		if err != nil {
			return nil, nil, fmt.Errorf("DB2 INTEGER %q: %w", raw, err)
		}
		var b [4]byte
		endian.PutUint32(b[:], uint32(int32(n)))
		return append(dst, b[:]...), nil, nil
	case drdaNInteger8:
		if err := connector.ValidateNumericLiteral(v.Raw, false); err != nil {
			return nil, nil, err
		}
		n := new(big.Int)
		if _, ok := n.SetString(raw, 10); !ok || !n.IsInt64() {
			return nil, nil, fmt.Errorf("DB2 BIGINT %q is out of range", raw)
		}
		var b [8]byte
		endian.PutUint64(b[:], uint64(n.Int64()))
		return append(dst, b[:]...), nil, nil
	case drdaNDecimal:
		b, err := encodeDB2PackedDecimal(raw, int(enc.length>>8), int(enc.length&0xff))
		return append(dst, b...), nil, err
	case drdaNFloat4:
		f, err := parseDB2Float(raw, 32)
		if err != nil {
			return nil, nil, err
		}
		var b [4]byte
		endian.PutUint32(b[:], math.Float32bits(float32(f)))
		return append(dst, b[:]...), nil, nil
	case drdaNFloat8:
		f, err := parseDB2Float(raw, 64)
		if err != nil {
			return nil, nil, err
		}
		var b [8]byte
		endian.PutUint64(b[:], math.Float64bits(f))
		return append(dst, b[:]...), nil, nil
	case drdaNBoolean:
		switch strings.ToLower(raw) {
		case "1", "true", "t", "yes", "y":
			return append(dst, 1), nil, nil
		case "0", "false", "f", "no", "n":
			return append(dst, 0), nil, nil
		default:
			return nil, nil, fmt.Errorf("invalid DB2 boolean %q", raw)
		}
	case drdaNDate:
		if len(raw) != 10 {
			return nil, nil, fmt.Errorf("DB2 DATE %q must be YYYY-MM-DD", raw)
		}
		return append(dst, raw...), nil, nil
	case drdaNTime:
		if len(raw) < 8 {
			return nil, nil, fmt.Errorf("DB2 TIME %q is too short", raw)
		}
		raw = raw[:8]
		return append(dst, raw...), nil, nil
	case drdaNTimestamp:
		if len(raw) < 19 || len(raw) > 32 {
			return nil, nil, fmt.Errorf("DB2 TIMESTAMP %q has unsupported length", raw)
		}
		b := make([]byte, 32)
		copy(b, raw)
		for i := len(raw); i < len(b); i++ {
			b[i] = ' '
		}
		return append(dst, b...), nil, nil
	case drdaNVarMix:
		if len(v.Raw) > 0xffff {
			return nil, nil, fmt.Errorf("DB2 inline character parameter %s is too large", col.Name)
		}
		dst = binary.BigEndian.AppendUint16(dst, uint16(len(v.Raw)))
		return append(dst, v.Raw...), nil, nil
	case drdaNVarBinary:
		if len(v.Raw) > 32672 {
			return nil, nil, fmt.Errorf("DB2 inline binary parameter %s is too large", col.Name)
		}
		dst = binary.BigEndian.AppendUint16(dst, uint16(len(v.Raw)))
		return append(dst, v.Raw...), nil, nil
	case drdaNLOBBytes, drdaNLOBCSBCS:
		dst = append(dst, 0x02)
		dst = binary.BigEndian.AppendUint64(dst, uint64(len(v.Raw)))
		ext := make([]byte, 1, len(v.Raw)+1)
		ext[0] = 0x00
		ext = append(ext, v.Raw...)
		return dst, ext, nil
	default:
		return nil, nil, fmt.Errorf("unsupported DB2 parameter FDO type 0x%02x", enc.typ)
	}
}

func parseDB2Float(raw string, bits int) (float64, error) {
	s := strings.ToLower(strings.TrimSpace(raw))
	switch s {
	case "inf", "+inf", "infinity", "+infinity":
		return math.Inf(1), nil
	case "-inf", "-infinity":
		return math.Inf(-1), nil
	case "nan":
		return math.NaN(), nil
	}
	if err := connector.ValidateNumericLiteral([]byte(raw), false); err != nil {
		return 0, err
	}
	return strconv.ParseFloat(raw, bits)
}

func buildDB2SQLDTA(cols []domain.ColumnInfo, row []connector.Value, endian binary.ByteOrder) ([]byte, [][]byte, error) {
	if len(cols) != len(row) {
		return nil, nil, errors.New("DB2 SQLDTA column/value width mismatch")
	}
	if len(cols) > 512 {
		return nil, nil, fmt.Errorf("DB2 prepared statement has %d parameters; safety limit is 512", len(cols))
	}
	encs := make([]db2ParamEncoding, len(cols))
	for i := range cols {
		e, err := db2ParamEncodingFor(cols[i], row[i])
		if err != nil {
			return nil, nil, fmt.Errorf("DB2 parameter %s: %w", cols[i].Name, err)
		}
		encs[i] = e
	}
	var dsc []byte
	first := min(84, len(encs))
	dsc = append(dsc, byte(3+3*first), 0x76, 0xd0)
	for i := 0; i < first; i++ {
		dsc = append(dsc, encs[i].typ, byte(encs[i].length>>8), byte(encs[i].length))
	}
	for off := first; off < len(encs); off += 84 {
		cnt := min(84, len(encs)-off)
		dsc = append(dsc, byte(3+3*cnt), 0x7f, 0x00)
		for i := off; i < off+cnt; i++ {
			dsc = append(dsc, encs[i].typ, byte(encs[i].length>>8), byte(encs[i].length))
		}
	}
	dsc = append(dsc, 0x06, 0x71, 0xe4, 0xd0, 0x00, 0x01)
	dta := []byte{0x00}
	var ext [][]byte
	for i := range cols {
		var x []byte
		var err error
		dta, x, err = encodeDB2Param(dta, encs[i], cols[i], row[i], endian)
		if err != nil {
			return nil, nil, fmt.Errorf("DB2 parameter %s: %w", cols[i].Name, err)
		}
		if x != nil {
			ext = append(ext, x)
		}
	}
	body := join(packDDM(cpFDODSC, dsc), packDDM(cpFDODTA, dta))
	return body, ext, nil
}

func packEXCSQLSTT(database string) []byte {
	return packDDM(cpEXCSQLSTT, join(packPKGNAMCSN(database, 65), packParam(cpRDBCMTOK, []byte{0xf1})))
}

func (c *drdaClient) readUntilCorrelationNoDeadline(finalCorr uint16) ([]dssPacket, error) {
	var out []dssPacket
	for {
		p, err := readDSS(c.conn)
		if err != nil {
			return out, err
		}
		if err = responseErrorClient(c, p); err != nil {
			return out, err
		}
		out = append(out, p)
		if !p.chained && p.corr >= finalCorr {
			return out, nil
		}
	}
}

func (c *drdaClient) execPreparedBatch(ctx context.Context, sql string, cols []domain.ColumnInfo, rows [][]connector.Value) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	defer setDeadline(ctx, c.conn)()
	stmt := packSQLSTT(sql)
	if len(stmt)+6 > drdaMaxSegment {
		return 0, fmt.Errorf("DB2 prepared SQL text is %d bytes; SQL text itself must remain below %d-byte DRDA segment limit", len(stmt)+6, drdaMaxSegment)
	}
	if _, err := sendDSS(c.conn, packPRPSQLSTT(c.database), 1, true, false); err != nil {
		return 0, err
	}
	if _, err := sendDSS(c.conn, stmt, 1, false, false); err != nil {
		return 0, err
	}
	corr := uint16(2)
	for ri, row := range rows {
		body, ext, err := buildDB2SQLDTA(cols, row, c.endian)
		if err != nil {
			return int64(ri), err
		}
		if _, err = sendDSS(c.conn, packEXCSQLSTT(c.database), corr, true, false); err != nil {
			return int64(ri), err
		}
		rowLast := ri == len(rows)-1
		if _, err = sendDSSPayload(c.conn, cpSQLDTA, body, corr, len(ext) > 0, rowLast && len(ext) == 0); err != nil {
			return int64(ri), err
		}
		for i, b := range ext {
			lastExt := i == len(ext)-1
			if _, err = sendDSSPayload(c.conn, cpEXTDTA, b, corr, !lastExt, rowLast && lastExt); err != nil {
				return int64(ri), err
			}
		}
		corr++
	}
	if _, err := c.readUntilCorrelationNoDeadline(corr - 1); err != nil {
		return 0, err
	}
	return int64(len(rows)), nil
}
