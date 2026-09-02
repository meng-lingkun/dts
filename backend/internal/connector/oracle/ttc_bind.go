package oracleconnector

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// oracleTTCBind is the minimal input-bind descriptor required by the OALL8
// request shape used by QMigration. It intentionally models only scalar input
// binds used by migration DML; output/REF CURSOR bindings are outside the data
// migration contract.
const oracleMaxTTCRequestBytes = 256 << 20

type oracleTTCBind struct {
	DataType    byte
	Flag        byte
	Precision   byte
	Scale       byte
	MaxLen      uint64
	MaxArray    uint64
	ContFlag    uint64
	ToID        []byte
	Version     uint64
	CharsetID   uint64
	CharsetForm byte
	MaxCharLen  uint64
	Value       []byte
}

func (b oracleTTCBind) writeMeta(w *ttcEncoder) {
	w.byte(b.DataType)
	w.byte(b.Flag)
	w.byte(b.Precision)
	w.byte(b.Scale)
	w.compactUint(b.MaxLen, 4)
	w.compactUint(b.MaxArray, 4)
	w.compactUint(b.ContFlag, 4)
	if len(b.ToID) == 0 {
		w.byte(0)
	} else {
		w.compactUint(uint64(len(b.ToID)), 4)
		w.clr(b.ToID)
	}
	w.compactUint(b.Version, 2)
	w.compactUint(b.CharsetID, 2)
	w.byte(b.CharsetForm)
	w.compactUint(b.MaxCharLen, 4)
}

func (b oracleTTCBind) signature() string {
	return fmt.Sprintf("%d/%d/%d/%d/%d/%d/%d/%d/%d/%d", b.DataType, b.Flag, b.Precision, b.Scale, b.MaxLen, b.MaxArray, b.ContFlag, b.Version, b.CharsetID, b.CharsetForm)
}

func oracleBindSignature(rows [][]oracleTTCBind) (string, error) {
	if len(rows) == 0 || len(rows[0]) == 0 {
		return "", nil
	}
	var sb strings.Builder
	for i, b := range rows[0] {
		if i > 0 {
			sb.WriteByte('|')
		}
		sb.WriteString(b.signature())
	}
	for r := 1; r < len(rows); r++ {
		if len(rows[r]) != len(rows[0]) {
			return "", errors.New("Oracle TTC array bind row shape mismatch")
		}
		for i := range rows[r] {
			if rows[r][i].signature() != rows[0][i].signature() {
				return "", fmt.Errorf("Oracle TTC array bind descriptor mismatch at row %d bind %d", r, i)
			}
		}
	}
	return sb.String(), nil
}

func oracleStringInputBind(v []byte, charset uint16) oracleTTCBind {
	// Use a stable max descriptor so the same parsed cursor can be reused for
	// later rows whose actual string lengths differ.
	return oracleTTCBind{DataType: oracleTypeNCHAR, Flag: 3, MaxLen: 32767, ContFlag: 16, CharsetID: uint64(charset), CharsetForm: 1, MaxCharLen: 32767, Value: append([]byte(nil), v...)}
}

func oracleRawInputBind(v []byte) oracleTTCBind {
	return oracleTTCBind{DataType: oracleTypeRAW, Flag: 3, MaxLen: 32767, Value: append([]byte(nil), v...)}
}

func oracleNumberInputBind(raw string) (oracleTTCBind, error) {
	b, err := encodeOracleNumberString(raw)
	if err != nil {
		return oracleTTCBind{}, err
	}
	return oracleTTCBind{DataType: oracleTypeNUMBER, Flag: 3, Precision: 38, MaxLen: 22, Value: b}, nil
}

func oracleNullInputBind(base oracleTTCBind) oracleTTCBind {
	base.Value = nil
	return base
}

// expandOracleDecimal expands a validated decimal/scientific literal without
// converting through float64. Oracle NUMBER is decimal/base-100, so preserving
// the exact decimal digits is required for migration correctness.
func expandOracleDecimal(raw string) (negative bool, whole, frac string, err error) {
	raw = strings.TrimSpace(raw)
	if !oracleNumberLiteralSafe.MatchString(raw) {
		return false, "", "", fmt.Errorf("invalid Oracle NUMBER literal %q", raw)
	}
	if raw[0] == '+' || raw[0] == '-' {
		negative = raw[0] == '-'
		raw = raw[1:]
	}
	exp := 0
	if i := strings.IndexAny(raw, "eE"); i >= 0 {
		exp, err = strconv.Atoi(raw[i+1:])
		if err != nil || exp < -200 || exp > 200 {
			return false, "", "", fmt.Errorf("Oracle NUMBER exponent out of range")
		}
		raw = raw[:i]
	}
	parts := strings.SplitN(raw, ".", 2)
	intPart := parts[0]
	fracPart := ""
	if len(parts) == 2 {
		fracPart = parts[1]
	}
	digits := intPart + fracPart
	dot := len(intPart) + exp
	if dot <= 0 {
		whole = "0"
		frac = strings.Repeat("0", -dot) + digits
	} else if dot >= len(digits) {
		whole = digits + strings.Repeat("0", dot-len(digits))
		frac = ""
	} else {
		whole = digits[:dot]
		frac = digits[dot:]
	}
	whole = strings.TrimLeft(whole, "0")
	frac = strings.TrimRight(frac, "0")
	if whole == "" {
		whole = "0"
	}
	return negative, whole, frac, nil
}

// encodeOracleNumberString is the inverse of decodeOracleNumberString for the
// decimal subset used by migration values. It keeps exact precision and emits
// Oracle's base-100 NUMBER representation instead of relying on NLS-dependent
// implicit string conversion.
func encodeOracleNumberString(raw string) ([]byte, error) {
	negative, whole, frac, err := expandOracleDecimal(raw)
	if err != nil {
		return nil, err
	}
	if whole == "0" && frac == "" {
		return []byte{0x80}, nil
	}
	groups := make([]int, 0, 20)
	exponent := -1
	if whole != "0" {
		padded := whole
		if len(padded)%2 == 1 {
			padded = "0" + padded
		}
		for i := 0; i < len(padded); i += 2 {
			g, _ := strconv.Atoi(padded[i : i+2])
			groups = append(groups, g)
		}
		exponent = len(groups) - 1
	}
	if frac != "" {
		padded := frac
		if len(padded)%2 == 1 {
			padded += "0"
		}
		fg := make([]int, 0, len(padded)/2)
		for i := 0; i < len(padded); i += 2 {
			g, _ := strconv.Atoi(padded[i : i+2])
			fg = append(fg, g)
		}
		if whole == "0" {
			for len(fg) > 0 && fg[0] == 0 {
				exponent--
				fg = fg[1:]
			}
		}
		groups = append(groups, fg...)
	}
	for len(groups) > 0 && groups[len(groups)-1] == 0 {
		groups = groups[:len(groups)-1]
	}
	if len(groups) == 0 {
		return []byte{0x80}, nil
	}
	if len(groups) > 20 {
		return nil, errors.New("Oracle NUMBER exceeds 40 decimal digits")
	}
	out := make([]byte, 0, len(groups)+2)
	if !negative {
		first := 193 + exponent
		if first <= 0 || first > 255 {
			return nil, errors.New("Oracle NUMBER exponent outside wire range")
		}
		out = append(out, byte(first))
		for _, g := range groups {
			out = append(out, byte(g+1))
		}
		return out, nil
	}
	first := 62 - exponent
	if first <= 0 || first > 255 {
		return nil, errors.New("Oracle NUMBER exponent outside wire range")
	}
	out = append(out, byte(first))
	for _, g := range groups {
		out = append(out, byte(101-g))
	}
	if len(groups) < 20 {
		out = append(out, 102)
	}
	return out, nil
}

func writeOracleBindRows(w *ttcEncoder, rows [][]oracleTTCBind) {
	for _, row := range rows {
		w.byte(ttcRowData)
		// TTC sends non-RAW values first and RAW values second.
		for _, b := range row {
			if b.DataType != oracleTypeRAW {
				w.clr(b.Value)
			}
		}
		for _, b := range row {
			if b.DataType == oracleTypeRAW {
				w.clr(b.Value)
			}
		}
	}
}

func buildTTCBindStatementRequest(sql string, ttcVersion byte, rows [][]oracleTTCBind, plsql bool) ([]byte, string, error) {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return nil, "", errors.New("Oracle TTC bind statement is empty")
	}
	if len(sql) > 48<<10 {
		return nil, "", errors.New("Oracle TTC bind SQL exceeds 48 KiB")
	}
	if len(rows) == 0 || len(rows[0]) == 0 {
		return nil, "", errors.New("Oracle TTC bind statement has no input binds")
	}
	if len(rows) > 1024 {
		return nil, "", errors.New("Oracle TTC array bind exceeds 1024 rows")
	}
	sig, err := oracleBindSignature(rows)
	if err != nil {
		return nil, "", err
	}
	bindCount := len(rows[0])
	w := &ttcEncoder{}
	w.byte(ttcFunctionCall)
	w.byte(0x5e)
	w.byte(0)
	exeOp := uint64(0x29) // parse + execute + bind
	if plsql {
		exeOp |= 0x40000
	} else {
		exeOp |= 0x8000
	}
	if len(rows) > 1 {
		exeOp |= 0x80000
	}
	w.compactUint(exeOp, 4)
	w.compactUint(0, 2)
	w.byte(1)
	w.compactUint(uint64(len([]byte(sql))), 4)
	w.byte(1)
	w.compactUint(13, 2)
	w.byte(0)
	w.byte(0)
	w.compactUint(0, 4)
	w.compactUint(0, 4)
	w.compactUint(0x7fffffff, 4)
	w.byte(1)
	w.compactUint(uint64(bindCount), 2)
	w.Write([]byte{0, 0, 0, 0, 0})
	w.byte(0)
	w.byte(0)
	if ttcVersion >= 4 {
		w.Write([]byte{0, 0, 1})
	}
	if ttcVersion >= 5 {
		w.Write([]byte{0, 0, 0, 0, 0})
	}
	w.clr([]byte(sql))
	al8i4 := [13]uint64{}
	al8i4[0] = 1
	al8i4[1] = uint64(len(rows))
	for _, v := range al8i4 {
		w.compactUint(v, 4)
	}
	for _, b := range rows[0] {
		b.writeMeta(w)
	}
	writeOracleBindRows(w, rows)
	if w.Len() > oracleMaxTTCRequestBytes {
		return nil, "", fmt.Errorf("Oracle TTC bind request exceeds %d-byte safety limit: %d bytes", oracleMaxTTCRequestBytes, w.Len())
	}
	return append([]byte(nil), w.Bytes()...), sig, nil
}

func buildTTCPreparedReexecuteRequest(cursorID uint64, rows [][]oracleTTCBind) ([]byte, error) {
	if cursorID == 0 {
		return nil, errors.New("Oracle prepared cursor id is zero")
	}
	if len(rows) == 0 || len(rows[0]) == 0 {
		return nil, errors.New("Oracle prepared re-execute has no bind rows")
	}
	if _, err := oracleBindSignature(rows); err != nil {
		return nil, err
	}
	w := &ttcEncoder{}
	w.Write([]byte{ttcFunctionCall, 4, 0})
	w.compactUint(cursorID, 2)
	w.compactUint(uint64(len(rows)), 2)
	w.compactUint(0, 2)
	w.compactUint(0, 2)
	writeOracleBindRows(w, rows)
	if w.Len() > oracleMaxTTCRequestBytes {
		return nil, fmt.Errorf("Oracle TTC prepared request exceeds %d-byte safety limit: %d bytes", oracleMaxTTCRequestBytes, w.Len())
	}
	return append([]byte(nil), w.Bytes()...), nil
}

type oracleTTCPrepared struct {
	CursorID  uint64
	Signature string
}

func (c *Connector) readTTCStatementSummaryLocked(ctx context.Context) (oracleTTCSummary, error) {
	var zero oracleTTCSummary
	state := oracleTTCQueryState{}
	for packets := 0; packets < 4096; packets++ {
		state.SummarySeen = false
		state.FunctionStat = false
		if err := readAndConsumeTTCQueryData(ctx, c.accepted.Session, c.data, c.proto, &state, 1); err != nil {
			c.resetNativeSessionLocked()
			return zero, fmt.Errorf("Oracle TTC statement response: %w", err)
		}
		if state.SummarySeen || state.FunctionStat {
			return state.LastSummary, nil
		}
	}
	return zero, errors.New("Oracle TTC statement response packet limit exceeded")
}

func (c *Connector) executeTTCBoundLocked(ctx context.Context, sql string, rows [][]oracleTTCBind, plsql bool) (oracleTTCSummary, error) {
	var zero oracleTTCSummary
	if err := c.ensureNativeSessionLocked(ctx); err != nil {
		return zero, err
	}
	if c.prepared == nil {
		c.prepared = map[string]oracleTTCPrepared{}
	}
	sig, err := oracleBindSignature(rows)
	if err != nil {
		return zero, err
	}
	cacheKey := sql + "\x00" + sig
	prepared, cached := c.prepared[cacheKey]
	var req []byte
	if cached && prepared.CursorID != 0 && !plsql {
		req, err = buildTTCPreparedReexecuteRequest(prepared.CursorID, rows)
	} else {
		req, _, err = buildTTCBindStatementRequest(sql, c.data.TTCVersion, rows, plsql)
	}
	if err != nil {
		return zero, err
	}
	if err = c.accepted.Session.WriteData(ctx, 0, req); err != nil {
		c.resetNativeSessionLocked()
		return zero, fmt.Errorf("Oracle TTC bind request: %w", err)
	}
	summary, err := c.readTTCStatementSummaryLocked(ctx)
	if err != nil {
		// Invalid cursor can occur after server-side cursor aging. Retry exactly
		// once through a parse path rather than letting a stale cache poison the
		// migration worker.
		if cached && !plsql && strings.Contains(err.Error(), "ORA-01001") {
			delete(c.prepared, cacheKey)
			full, _, buildErr := buildTTCBindStatementRequest(sql, c.data.TTCVersion, rows, false)
			if buildErr != nil {
				return zero, buildErr
			}
			if writeErr := c.accepted.Session.WriteData(ctx, 0, full); writeErr != nil {
				c.resetNativeSessionLocked()
				return zero, writeErr
			}
			summary, err = c.readTTCStatementSummaryLocked(ctx)
		}
		if err != nil {
			return zero, err
		}
	}
	if !plsql && summary.CursorID != 0 {
		c.prepared[cacheKey] = oracleTTCPrepared{CursorID: summary.CursorID, Signature: sig}
	}
	return summary, nil
}

func (c *Connector) execBound(ctx context.Context, sql string, rows [][]oracleTTCBind, plsql bool) (oracleTTCSummary, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.executeTTCBoundLocked(ctx, sql, rows, plsql)
}
