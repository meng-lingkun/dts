package oracleconnector

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

const (
	ttcFunctionCall byte = 3
	ttcErrorReturn  byte = 4
	ttcRowHeader    byte = 6
	ttcRowData      byte = 7
	ttcFunctionStat byte = 9
	ttcDescribe     byte = 16
)

const (
	oracleTypeNCHAR        = 1
	oracleTypeNUMBER       = 2
	oracleTypeLONG         = 8
	oracleTypeVARCHAR      = 9
	oracleTypeROWID        = 11
	oracleTypeDATE         = 12
	oracleTypeVarRaw       = 15
	oracleTypeRAW          = 23
	oracleTypeLongRaw      = 24
	oracleTypeCHAR         = 96
	oracleTypeBinaryFloat  = 100
	oracleTypeBinaryDouble = 101
	oracleTypeCLOB         = 112
	oracleTypeBLOB         = 113
	oracleTypeTimestamp    = 180
	oracleTypeTimestampTZ  = 181
	oracleTypeUROWID       = 208
	oracleTypeTimestampLTZ = 231
)

// oracleTTCColumn is intentionally protocol-facing. It records exactly the
// describe metadata needed to compile a later QMigration source row reader;
// it is not yet exposed as Connector metadata until a real Oracle instance is
// qualified end-to-end.
type oracleTTCColumn struct {
	DataType    byte
	Flag        byte
	Precision   byte
	Scale       int
	MaxLen      uint64
	CharsetID   uint64
	CharsetForm byte
	MaxCharLen  uint64
	Nullable    bool
	Name        string
	TypeName    string
}

type oracleTTCQueryResult struct {
	Columns []oracleTTCColumn
	Rows    [][]any
}

// buildTTCSelectRequest emits the OALL8 parse+execute shape used by modern TTC
// clients for a bind-free SELECT. The request remains behind the experimental
// query gate until it is qualified against supported Oracle releases.
func buildTTCSelectRequest(sql string, ttcVersion byte, fetchRows int) ([]byte, error) {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return nil, errors.New("Oracle TTC SELECT is empty")
	}
	if len(sql) > 1<<20 {
		return nil, errors.New("Oracle TTC SELECT exceeds 1 MiB probe limit")
	}
	if fetchRows <= 0 {
		fetchRows = 16
	}
	if fetchRows > 1024 {
		return nil, errors.New("Oracle TTC SELECT fetchRows exceeds probe limit")
	}
	w := &ttcEncoder{}
	w.byte(ttcFunctionCall)
	w.byte(0x5e)
	w.byte(0)
	// parse(0x1) + execute(0x20) + non-PL/SQL(0x8000)
	w.compactUint(0x8021, 4)
	w.compactUint(0, 2) // cursor id; 0 means allocate
	w.byte(1)
	w.compactUint(uint64(len([]byte(sql))), 4)
	w.byte(1)
	w.compactUint(13, 2)
	w.byte(0)
	w.byte(0)
	w.byte(0)
	w.compactUint(uint64(fetchRows), 4)
	w.compactUint(0x7fffffff, 4)
	w.byte(0) // no binds
	w.byte(0)
	w.byte(0)
	w.byte(0)
	w.byte(0)
	w.byte(0)
	w.byte(0) // no define metadata on first parse
	w.byte(0)
	if ttcVersion >= 4 {
		w.byte(0)
		w.byte(0)
		w.byte(1)
	}
	if ttcVersion >= 5 {
		w.byte(0)
		w.byte(0)
		w.byte(0)
		w.byte(0)
		w.byte(0)
	}
	w.clr([]byte(sql))
	// AL8I4 execution vector. SELECT sets parse=1, fetch count and select=1.
	al8i4 := [13]uint64{1, uint64(fetchRows), 0, 0, 0, 0, 0, 1}
	for _, v := range al8i4 {
		w.compactUint(v, 4)
	}
	return append([]byte(nil), w.Bytes()...), nil
}

func parseTTCDescribe(payload []byte, ttcVersion byte) ([]oracleTTCColumn, error) {
	r := newTTCDecoder(payload)
	cols, err := parseTTCDescribeFromDecoder(r, ttcVersion)
	if err != nil {
		return nil, err
	}
	if r.remaining() != 0 {
		return nil, fmt.Errorf("Oracle TTC describe has %d trailing bytes", r.remaining())
	}
	return cols, nil
}

func parseTTCDescribeFromDecoder(r *ttcDecoder, ttcVersion byte) ([]oracleTTCColumn, error) {
	headerLen, err := r.byte()
	if err != nil {
		return nil, err
	}
	if headerLen > 64 {
		return nil, fmt.Errorf("Oracle TTC describe header too large: %d", headerLen)
	}
	if _, err = r.take(int(headerLen)); err != nil {
		return nil, err
	}
	if _, err = r.compactUint(4); err != nil { // max row size
		return nil, err
	}
	count, err := r.compactUint(4)
	if err != nil {
		return nil, err
	}
	if count > 4096 {
		return nil, fmt.Errorf("Oracle TTC describe column count too large: %d", count)
	}
	if count > 0 {
		if _, err = r.byte(); err != nil {
			return nil, err
		}
	}
	cols := make([]oracleTTCColumn, 0, count)
	for i := uint64(0); i < count; i++ {
		col, err := parseTTCColumn(r, ttcVersion)
		if err != nil {
			return nil, fmt.Errorf("Oracle TTC describe column %d: %w", i, err)
		}
		cols = append(cols, col)
	}
	if _, err = r.clr(); err != nil { // describe trailer DLC
		return nil, err
	}
	if ttcVersion >= 3 {
		if _, err = r.compactUint(4); err != nil {
			return nil, err
		}
		if _, err = r.compactUint(4); err != nil {
			return nil, err
		}
	}
	if ttcVersion >= 4 {
		if _, err = r.compactUint(4); err != nil {
			return nil, err
		}
		if _, err = r.compactUint(4); err != nil {
			return nil, err
		}
	}
	if ttcVersion >= 5 {
		if _, err = r.clr(); err != nil {
			return nil, err
		}
	}
	return cols, nil
}

func parseTTCColumn(r *ttcDecoder, ttcVersion byte) (oracleTTCColumn, error) {
	var col oracleTTCColumn
	var err error
	if col.DataType, err = r.byte(); err != nil {
		return col, err
	}
	if col.Flag, err = r.byte(); err != nil {
		return col, err
	}
	if col.Precision, err = r.byte(); err != nil {
		return col, err
	}
	if oracleTypeHasSignedScale(col.DataType) {
		s, err := r.compactInt(2)
		if err != nil {
			return col, err
		}
		col.Scale = int(s)
	} else {
		s, err := r.byte()
		if err != nil {
			return col, err
		}
		col.Scale = int(s)
	}
	if col.MaxLen, err = r.compactUint(4); err != nil {
		return col, err
	}
	if _, err = r.compactUint(4); err != nil { // max array elements
		return col, err
	}
	if _, err = r.compactUint(4); err != nil { // continuation flags
		return col, err
	}
	if _, err = r.clr(); err != nil { // type OID
		return col, err
	}
	if _, err = r.compactUint(2); err != nil { // version
		return col, err
	}
	if col.CharsetID, err = r.compactUint(2); err != nil {
		return col, err
	}
	if col.CharsetForm, err = r.byte(); err != nil {
		return col, err
	}
	if col.MaxCharLen, err = r.compactUint(4); err != nil {
		return col, err
	}
	nullable, err := r.byte()
	if err != nil {
		return col, err
	}
	col.Nullable = nullable != 0
	if _, err = r.byte(); err != nil {
		return col, err
	}
	name, err := r.clr()
	if err != nil {
		return col, err
	}
	if len(name) > 1024 {
		return col, errors.New("Oracle TTC column name too long")
	}
	col.Name = string(name)
	if _, err = r.clr(); err != nil { // schema name / reserved DLC
		return col, err
	}
	typeName, err := r.clr()
	if err != nil {
		return col, err
	}
	if len(typeName) > 1024 {
		return col, errors.New("Oracle TTC type name too long")
	}
	col.TypeName = string(typeName)
	if ttcVersion >= 3 {
		if _, err = r.compactUint(2); err != nil {
			return col, err
		}
	}
	if ttcVersion >= 6 {
		if _, err = r.compactUint(4); err != nil {
			return col, err
		}
	}
	return col, nil
}

func oracleTypeHasSignedScale(t byte) bool {
	switch t {
	case oracleTypeNUMBER, oracleTypeTimestamp, oracleTypeTimestampTZ, oracleTypeTimestampLTZ, 182, 183, 187, 188, 190, 232:
		return true
	default:
		return false
	}
}

func parseTTCRow(payload []byte, cols []oracleTTCColumn) ([]any, error) {
	r := newTTCDecoder(payload)
	row, err := parseTTCRowFromDecoder(r, cols, nil)
	if err != nil {
		return nil, err
	}
	if r.remaining() != 0 {
		return nil, fmt.Errorf("Oracle TTC row has %d trailing bytes", r.remaining())
	}
	return row, nil
}

func decodeOracleTTCScalar(col oracleTTCColumn, b []byte) (any, error) {
	if b == nil {
		return nil, nil
	}
	switch col.DataType {
	case oracleTypeNUMBER:
		return decodeOracleNumberString(b)
	case oracleTypeNCHAR, oracleTypeVARCHAR, oracleTypeCHAR, oracleTypeLONG, oracleTypeUROWID:
		return string(b), nil
	case oracleTypeRAW, oracleTypeVarRaw, oracleTypeLongRaw, oracleTypeBLOB, oracleTypeCLOB:
		return append([]byte(nil), b...), nil
	case oracleTypeDATE:
		return decodeOracleDateString(b)
	case oracleTypeTimestamp, oracleTypeTimestampLTZ:
		return decodeOracleTimestampString(b, false)
	case oracleTypeTimestampTZ:
		return decodeOracleTimestampString(b, true)
	case oracleTypeBinaryFloat:
		if len(b) != 4 {
			return nil, fmt.Errorf("Oracle BINARY_FLOAT length %d, expected 4", len(b))
		}
		return strconv.FormatFloat(float64(math.Float32frombits(binary.BigEndian.Uint32(b))), 'g', -1, 32), nil
	case oracleTypeBinaryDouble:
		if len(b) != 8 {
			return nil, fmt.Errorf("Oracle BINARY_DOUBLE length %d, expected 8", len(b))
		}
		return strconv.FormatFloat(math.Float64frombits(binary.BigEndian.Uint64(b)), 'g', -1, 64), nil
	default:
		// Preserve unsupported values byte-for-byte rather than inventing a
		// lossy conversion. The experimental Full Reader can transport these
		// values without silently changing bytes while type mapping is refined.
		return append([]byte(nil), b...), nil
	}
}

func decodeOracleTimestampString(b []byte, withTZ bool) (string, error) {
	want := 11
	if withTZ {
		want = 13
	}
	if len(b) != want {
		return "", fmt.Errorf("Oracle TIMESTAMP length %d, expected %d", len(b), want)
	}
	base, err := decodeOracleDateString(b[:7])
	if err != nil {
		return "", err
	}
	nanos := binary.BigEndian.Uint32(b[7:11])
	if nanos > 999999999 {
		return "", fmt.Errorf("invalid Oracle TIMESTAMP nanoseconds %d", nanos)
	}
	if nanos != 0 {
		base += "." + strings.TrimRight(fmt.Sprintf("%09d", nanos), "0")
	}
	if withTZ {
		h := int(b[11]) - 20
		m := int(b[12]) - 60
		if h < -12 || h > 14 || m < -59 || m > 59 {
			return "", fmt.Errorf("invalid Oracle TIMESTAMP WITH TIME ZONE offset %d:%d", h, m)
		}
		sign := "+"
		if h < 0 || m < 0 {
			sign = "-"
		}
		if h < 0 {
			h = -h
		}
		if m < 0 {
			m = -m
		}
		base += fmt.Sprintf("%s%02d:%02d", sign, h, m)
	}
	return base, nil
}

func decodeOracleDateString(b []byte) (string, error) {
	if len(b) != 7 {
		return "", fmt.Errorf("Oracle DATE length %d, expected 7", len(b))
	}
	year := (int(b[0])-100)*100 + int(b[1]) - 100
	month, day := int(b[2]), int(b[3])
	hour, minute, second := int(b[4])-1, int(b[5])-1, int(b[6])-1
	if month < 1 || month > 12 || day < 1 || day > 31 || hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 || second > 59 {
		return "", errors.New("invalid Oracle DATE components")
	}
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second), nil
}

// decodeOracleNumberString preserves Oracle NUMBER precision by returning a
// canonical decimal string instead of float64.
func decodeOracleNumberString(b []byte) (string, error) {
	if len(b) == 0 {
		return "", errors.New("empty Oracle NUMBER")
	}
	if len(b) == 1 && b[0] == 0x80 {
		return "0", nil
	}
	negative := b[0] < 0x80
	exponent := 0
	digits := b[1:]
	if negative {
		exponent = 62 - int(b[0])
		if len(digits) > 0 && digits[len(digits)-1] == 102 {
			digits = digits[:len(digits)-1]
		}
	} else {
		exponent = int(b[0]) - 193
	}
	if len(digits) == 0 {
		return "", errors.New("Oracle NUMBER has no digits")
	}
	groups := make([]int, len(digits))
	for i, d := range digits {
		if negative {
			groups[i] = 101 - int(d)
		} else {
			groups[i] = int(d) - 1
		}
		if groups[i] < 0 || groups[i] > 99 {
			return "", errors.New("invalid Oracle NUMBER base-100 digit")
		}
	}
	wholeGroups := exponent + 1
	var whole, frac strings.Builder
	if wholeGroups <= 0 {
		whole.WriteByte('0')
		for i := 0; i < -wholeGroups; i++ {
			frac.WriteString("00")
		}
		for _, g := range groups {
			fmt.Fprintf(&frac, "%02d", g)
		}
	} else {
		for i := 0; i < wholeGroups; i++ {
			g := 0
			if i < len(groups) {
				g = groups[i]
			}
			if i == 0 {
				whole.WriteString(strconv.Itoa(g))
			} else {
				fmt.Fprintf(&whole, "%02d", g)
			}
		}
		for i := wholeGroups; i < len(groups); i++ {
			fmt.Fprintf(&frac, "%02d", groups[i])
		}
	}
	fs := strings.TrimRight(frac.String(), "0")
	out := whole.String()
	if fs != "" {
		out += "." + fs
	}
	if negative && out != "0" {
		out = "-" + out
	}
	return out, nil
}

// executeTTCSelect is the small bounded probe wrapper over the same stream-aware
// SELECT/fetch runtime used by the experimental Oracle Native data plane.
func (c *Connector) executeTTCSelect(ctx context.Context, accepted *acceptedSession, proto ttcProtocolInfo, data ttcDataTypeInfo, sql string) (oracleTTCQueryResult, error) {
	return c.executeTTCSelectBatched(ctx, accepted, proto, data, sql, 16, 16)
}

func parseTTCQuerySummary(payload []byte, ttcVersion byte, proto ttcProtocolInfo) error {
	r := newTTCDecoder(payload)
	hasEOS := len(proto.CompileTimeCaps) > 15 && proto.CompileTimeCaps[15]&1 != 0
	hasFSAP := len(proto.CompileTimeCaps) > 16 && proto.CompileTimeCaps[16]&1 != 0
	if hasEOS {
		if _, err := r.compactUint(4); err != nil {
			return err
		}
	}
	if ttcVersion >= 3 && hasFSAP {
		if _, err := r.compactUint(2); err != nil {
			return err
		}
	}
	if _, err := r.compactUint(4); err != nil {
		return err
	} // current row
	ret, err := r.compactUint(2)
	if err != nil {
		return err
	}
	if ret == 1403 { // no more rows
		return nil
	}
	if ret != 0 {
		return fmt.Errorf("Oracle TTC SELECT failed with ORA-%05d", ret)
	}
	// The remaining OER fields vary by TTC version/capabilities. A zero return
	// is sufficient to terminate this bounded probe, but production execution
	// remains capability-gated until the complete summary is real-Oracle tested.
	return nil
}
