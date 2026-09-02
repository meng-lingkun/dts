package oracleconnector

import (
	"context"
	"errors"
	"fmt"
)

const oracleMaxTTCResponseBytes = 256 << 20

// oracleTTCRowHeader mirrors the dataset header preceding TTC row-data
// messages. Present is derived from Oracle's optional bit-vector: an empty
// vector means all described columns are present in each following row.
type oracleTTCRowHeader struct {
	ColumnCount int
	RowCount    uint64
	UACBytes    uint64
	Present     []bool
}

func parseTTCRowHeaderFromDecoder(r *ttcDecoder, described int) (oracleTTCRowHeader, error) {
	var out oracleTTCRowHeader
	if r == nil {
		return out, errors.New("nil Oracle TTC row-header decoder")
	}
	if _, err := r.byte(); err != nil { // header flags/version byte
		return out, err
	}
	base, err := r.compactUint(2)
	if err != nil {
		return out, err
	}
	hi, err := r.compactUint(4)
	if err != nil {
		return out, err
	}
	count := base + hi*0x100
	if count > 4096 {
		return out, fmt.Errorf("Oracle TTC row-header column count too large: %d", count)
	}
	out.ColumnCount = int(count)
	if out.RowCount, err = r.compactUint(4); err != nil {
		return out, err
	}
	if out.UACBytes, err = r.compactUint(2); err != nil {
		return out, err
	}
	bits, err := r.clr()
	if err != nil {
		return out, err
	}
	if _, err = r.clr(); err != nil { // reserved/continuation DLC
		return out, err
	}
	if described == 0 {
		described = out.ColumnCount
	}
	if described < 0 || described > 4096 {
		return out, errors.New("invalid Oracle TTC described column count")
	}
	out.Present = make([]bool, described)
	if len(bits) == 0 {
		for i := range out.Present {
			out.Present[i] = true
		}
		return out, nil
	}
	need := (described + 7) / 8
	if len(bits) > need {
		return out, fmt.Errorf("Oracle TTC row-header bit-vector too large: %d > %d", len(bits), need)
	}
	for i := range out.Present {
		byteIndex := i / 8
		if byteIndex < len(bits) {
			out.Present[i] = bits[byteIndex]&(1<<uint(i%8)) != 0
		}
	}
	return out, nil
}

func parseTTCColumnBitVectorFromDecoder(r *ttcDecoder, described int) ([]bool, error) {
	if described <= 0 || described > 4096 {
		return nil, errors.New("Oracle TTC column bit-vector requires described columns")
	}
	if _, err := r.compactUint(2); err != nil { // number of columns sent
		return nil, err
	}
	n := (described + 7) / 8
	b, err := r.take(n)
	if err != nil {
		return nil, err
	}
	out := make([]bool, described)
	for i := range out {
		out[i] = b[i/8]&(1<<uint(i%8)) != 0
	}
	return out, nil
}

// oracleTTCSummary is the OER/end-of-call structure used by SQL, fetch and
// LOB operations. Keeping the cursor id and final return code is required for
// correct fetch continuation; dev12 intentionally parsed only the return code.
type oracleTTCSummary struct {
	EndOfCallStatus uint64
	ECIDSequence    uint64
	CurrentRow      uint64
	RetCode         uint64
	CursorID        uint64
	ErrorPosition   uint64
	Flags           uint64
	WarningFlag     byte
	ErrorMessage    string
}

func oracleTTCHasEOS(proto ttcProtocolInfo) bool {
	return len(proto.CompileTimeCaps) > 15 && proto.CompileTimeCaps[15]&1 != 0
}

func oracleTTCHasFSAP(proto ttcProtocolInfo) bool {
	return len(proto.CompileTimeCaps) > 16 && proto.CompileTimeCaps[16]&1 != 0
}

func parseTTCSummaryFromDecoder(r *ttcDecoder, ttcVersion byte, proto ttcProtocolInfo) (oracleTTCSummary, error) {
	var out oracleTTCSummary
	var err error
	if oracleTTCHasEOS(proto) {
		if out.EndOfCallStatus, err = r.compactUint(4); err != nil {
			return out, err
		}
	}
	if ttcVersion >= 3 && oracleTTCHasFSAP(proto) {
		if out.ECIDSequence, err = r.compactUint(2); err != nil {
			return out, err
		}
	}
	if out.CurrentRow, err = r.compactUint(4); err != nil {
		return out, err
	}
	if out.RetCode, err = r.compactUint(2); err != nil {
		return out, err
	}
	if _, err = r.compactUint(2); err != nil { // array elements with error
		return out, err
	}
	if _, err = r.compactUint(2); err != nil { // array element errno
		return out, err
	}
	if out.CursorID, err = r.compactUint(2); err != nil {
		return out, err
	}
	if out.ErrorPosition, err = r.compactUint(2); err != nil {
		return out, err
	}
	if _, err = r.byte(); err != nil { // SQL type
		return out, err
	}
	if _, err = r.byte(); err != nil { // fatal flag
		return out, err
	}
	if ttcVersion >= 7 {
		if out.Flags, err = r.compactUint(2); err != nil {
			return out, err
		}
		if _, err = r.compactUint(2); err != nil { // user cursor options
			return out, err
		}
	} else {
		f, e := r.byte()
		if e != nil {
			return out, e
		}
		out.Flags = uint64(f)
		if _, err = r.byte(); err != nil {
			return out, err
		}
	}
	if _, err = r.byte(); err != nil { // UPI param
		return out, err
	}
	if out.WarningFlag, err = r.byte(); err != nil {
		return out, err
	}
	for _, width := range []int{4, 2} { // RBA, partition id
		if _, err = r.compactUint(width); err != nil {
			return out, err
		}
	}
	if _, err = r.byte(); err != nil { // table id
		return out, err
	}
	for _, width := range []int{4, 2, 4} { // block, slot, os error
		if _, err = r.compactUint(width); err != nil {
			return out, err
		}
	}
	if _, err = r.byte(); err != nil { // stmt number
		return out, err
	}
	if _, err = r.byte(); err != nil { // call number
		return out, err
	}
	if _, err = r.compactUint(2); err != nil { // pad
		return out, err
	}
	if _, err = r.compactUint(4); err != nil { // successful iterations
		return out, err
	}
	if _, err = r.clr(); err != nil { // reserved DLC
		return out, err
	}
	if ttcVersion < 7 {
		for i := 0; i < 3; i++ {
			if _, err = r.clr(); err != nil {
				return out, err
			}
		}
	} else {
		// Array-bind error vectors are not expected for SELECT/fetch, but parse
		// them completely so a coalesced following TTC message is not mistaken
		// for part of the summary.
		n, e := r.compactUint(2)
		if e != nil {
			return out, e
		}
		if n > 4096 {
			return out, errors.New("Oracle TTC summary bind-error vector too large")
		}
		if n > 0 {
			marker, e := r.byte()
			if e != nil {
				return out, e
			}
			for i := uint64(0); i < n; i++ {
				if marker == 0xfe {
					if _, e = r.byte(); e != nil {
						return out, e
					}
				}
				if _, e = r.compactUint(2); e != nil {
					return out, e
				}
			}
			if marker == 0xfe {
				if _, e = r.byte(); e != nil {
					return out, e
				}
			}
		}
		n, e = r.compactUint(4)
		if e != nil {
			return out, e
		}
		if n > 4096 {
			return out, errors.New("Oracle TTC summary row-offset vector too large")
		}
		if n > 0 {
			marker, e := r.byte()
			if e != nil {
				return out, e
			}
			for i := uint64(0); i < n; i++ {
				if marker == 0xfe {
					if _, e = r.byte(); e != nil {
						return out, e
					}
				}
				if _, e = r.compactUint(4); e != nil {
					return out, e
				}
			}
			if marker == 0xfe {
				if _, e = r.byte(); e != nil {
					return out, e
				}
			}
		}
		n, e = r.compactUint(2)
		if e != nil {
			return out, e
		}
		if n > 4096 {
			return out, errors.New("Oracle TTC summary bind-message vector too large")
		}
		if n > 0 {
			if _, e = r.byte(); e != nil {
				return out, e
			}
			for i := uint64(0); i < n; i++ {
				if _, e = r.compactUint(2); e != nil {
					return out, e
				}
				if _, e = r.clr(); e != nil {
					return out, e
				}
				if _, e = r.byte(); e != nil {
					return out, e
				}
				if _, e = r.byte(); e != nil {
					return out, e
				}
			}
		}
		if out.RetCode, err = r.compactUint(4); err != nil {
			return out, err
		}
		if out.CurrentRow, err = r.compactUint(8); err != nil {
			return out, err
		}
	}
	if out.RetCode != 0 {
		msg, e := r.clr()
		if e != nil {
			return out, e
		}
		out.ErrorMessage = string(msg)
	}
	return out, nil
}

func (s oracleTTCSummary) err() error {
	if s.RetCode == 0 || s.RetCode == 1403 {
		return nil
	}
	if s.ErrorMessage != "" {
		return fmt.Errorf("Oracle TTC SQL failed with ORA-%05d: %s", s.RetCode, s.ErrorMessage)
	}
	return fmt.Errorf("Oracle TTC SQL failed with ORA-%05d", s.RetCode)
}

// oracleTTCQueryRPA is the return-parameter-area message (code 8) that can be
// interleaved with describe/row/summary messages. QueryID is intentionally raw;
// it is observability metadata rather than a migration checkpoint.
type oracleTTCQueryRPA struct {
	SCN      []uint64
	TimeZone []byte
	QueryID  []byte
}

func parseTTCQueryRPAFromDecoder(r *ttcDecoder, ttcVersion byte) (oracleTTCQueryRPA, error) {
	var out oracleTTCQueryRPA
	n, err := r.compactUint(2)
	if err != nil {
		return out, err
	}
	if n > 64 {
		return out, fmt.Errorf("Oracle TTC query RPA SCN vector too large: %d", n)
	}
	out.SCN = make([]uint64, 0, n)
	for i := uint64(0); i < n; i++ {
		v, err := r.compactUint(4)
		if err != nil {
			return out, err
		}
		out.SCN = append(out.SCN, v)
	}
	extra, err := r.compactUint(2)
	if err != nil {
		return out, err
	}
	if extra > 4096 {
		return out, fmt.Errorf("Oracle TTC query RPA extra area too large: %d", extra)
	}
	if extra > 0 {
		if _, err = r.take(int(extra)); err != nil {
			return out, err
		}
	}
	kvCount, err := r.compactUint(2)
	if err != nil {
		return out, err
	}
	if kvCount > 256 {
		return out, errors.New("Oracle TTC query RPA dictionary too large")
	}
	for i := uint64(0); i < kvCount; i++ {
		_, val, flag, err := r.keyVal()
		if err != nil {
			return out, err
		}
		if flag == 163 {
			out.TimeZone = append([]byte(nil), val...)
		}
	}
	if ttcVersion >= 4 {
		l, err := r.compactUint(4)
		if err != nil {
			return out, err
		}
		if l > 4096 {
			return out, errors.New("Oracle TTC query id too large")
		}
		if l > 0 {
			b, err := r.take(int(l))
			if err != nil {
				return out, err
			}
			out.QueryID = append([]byte(nil), b...)
		}
	}
	return out, nil
}

func buildTTCFetchRequest(cursorID uint64, fetchRows int) ([]byte, error) {
	if cursorID == 0 {
		return nil, errors.New("Oracle TTC fetch requires cursor id")
	}
	if fetchRows <= 0 {
		fetchRows = 128
	}
	if fetchRows > 4096 {
		return nil, errors.New("Oracle TTC fetchRows exceeds limit")
	}
	w := &ttcEncoder{}
	w.byte(ttcFunctionCall)
	w.byte(5)
	w.byte(0)
	w.compactUint(cursorID, 2)
	w.compactUint(uint64(fetchRows), 2)
	return append([]byte(nil), w.Bytes()...), nil
}

func buildTTCCursorCloseRequest(cursorID uint64) ([]byte, error) {
	if cursorID == 0 {
		return nil, errors.New("Oracle TTC cursor close requires cursor id")
	}
	w := &ttcEncoder{}
	// TTC cursor-close message used by modern Oracle clients before the
	// follow-up end-to-end-call bookkeeping operation.
	w.Write([]byte{17, 105, 0, 1, 1, 1})
	w.compactUint(cursorID, 4)
	return append([]byte(nil), w.Bytes()...), nil
}

type oracleTTCCursor struct {
	ID        uint64
	Exhausted bool
	Closed    bool
}

func (c *oracleTTCCursor) update(s oracleTTCSummary) {
	if s.CursorID != 0 {
		c.ID = s.CursorID
	}
	if s.RetCode == 1403 {
		c.Exhausted = true
	}
}

func (c *oracleTTCCursor) fetchRequest(rows int) ([]byte, error) {
	if c == nil || c.Closed || c.Exhausted {
		return nil, errors.New("Oracle TTC cursor is not fetchable")
	}
	return buildTTCFetchRequest(c.ID, rows)
}

func (c *oracleTTCCursor) closeRequest() ([]byte, error) {
	if c == nil || c.ID == 0 || c.Closed {
		return nil, errors.New("Oracle TTC cursor is not closable")
	}
	b, err := buildTTCCursorCloseRequest(c.ID)
	if err == nil {
		c.Closed = true
	}
	return b, err
}

func encodeOracleROWIDPart(v uint64, size int) []byte {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	out := make([]byte, size)
	for i := size - 1; i >= 0; i-- {
		out[i] = alphabet[v&0x3f]
		v >>= 6
	}
	return out
}

func parseTTCROWIDFromDecoder(r *ttcDecoder) (any, error) {
	present, err := r.byte()
	if err != nil {
		return nil, err
	}
	if present == 0 {
		return nil, nil
	}
	rba, err := r.compactUint(4)
	if err != nil {
		return nil, err
	}
	partition, err := r.compactUint(2)
	if err != nil {
		return nil, err
	}
	marker, err := r.byte()
	if err != nil {
		return nil, err
	}
	block, err := r.compactUint(4)
	if err != nil {
		return nil, err
	}
	slot, err := r.compactUint(2)
	if err != nil {
		return nil, err
	}
	if rba == 0 && partition == 0 && marker == 0 && block == 0 && slot == 0 {
		return nil, nil
	}
	out := make([]byte, 0, 18)
	out = append(out, encodeOracleROWIDPart(rba, 6)...)
	out = append(out, encodeOracleROWIDPart(partition, 3)...)
	out = append(out, encodeOracleROWIDPart(block, 6)...)
	out = append(out, encodeOracleROWIDPart(slot, 3)...)
	return string(out), nil
}

// oracleTTCLobRef deliberately keeps the opaque server locator separate from
// any inline/prefetch bytes so the experimental Full Reader can materialize the
// locator through the authenticated TTC session without lossy coercion.
type oracleTTCLobRef struct {
	DataType  byte
	CharsetID uint64
	Inline    []byte
	Locator   []byte
}

func buildTTCLobRequest(locator []byte, ttcVersion byte, operation uint64, sourceOffset uint64) ([]byte, error) {
	if len(locator) == 0 {
		return nil, errors.New("Oracle TTC LOB request requires locator")
	}
	if len(locator) > 1<<20 {
		return nil, errors.New("Oracle TTC LOB locator exceeds limit")
	}
	if operation != 1 && operation != 2 {
		return nil, fmt.Errorf("unsupported Oracle TTC LOB operation %d", operation)
	}
	w := &ttcEncoder{}
	w.Write([]byte{ttcFunctionCall, 0x60, 0})
	w.byte(1)
	w.compactUint(uint64(len(locator)), 4)
	w.byte(0)
	w.compactUint(0, 4)
	if ttcVersion < 3 {
		w.compactUint(sourceOffset, 4)
		w.compactUint(0, 4)
	} else {
		w.byte(0)
		w.byte(0)
	}
	w.byte(0) // no charset id
	if ttcVersion < 3 {
		w.byte(1)
	} else {
		w.byte(0)
	}
	w.byte(0) // null O2U
	w.compactUint(operation, 4)
	w.byte(0) // no SCN
	w.compactUint(0, 4)
	if ttcVersion >= 3 {
		w.compactUint(sourceOffset, 8)
		w.compactUint(0, 8)
		w.byte(1) // send amount
	}
	if ttcVersion >= 4 {
		w.Write([]byte{0, 0, 0, 0, 0, 0})
	}
	_, _ = w.Write(locator)
	if ttcVersion < 3 {
		w.compactUint(0, 4)
	} else {
		w.compactUint(0, 8)
	}
	return append([]byte(nil), w.Bytes()...), nil
}

func parseTTCLobChunkFromDecoder(r *ttcDecoder, maxBytes int) ([]byte, error) {
	first, err := r.byte()
	if err != nil {
		return nil, err
	}
	if first == 0 {
		return nil, nil
	}
	if first != 0xfe {
		if int(first) > maxBytes {
			return nil, errors.New("Oracle TTC LOB chunk exceeds limit")
		}
		b, err := r.take(int(first))
		return append([]byte(nil), b...), err
	}
	out := make([]byte, 0)
	for {
		n, err := r.byte()
		if err != nil {
			return nil, err
		}
		if n == 0 {
			return out, nil
		}
		if len(out)+int(n) > maxBytes {
			return nil, errors.New("Oracle TTC LOB chunk stream exceeds limit")
		}
		b, err := r.take(int(n))
		if err != nil {
			return nil, err
		}
		out = append(out, b...)
	}
}

type oracleTTCLobResponse struct {
	Size    uint64
	Data    []byte
	Summary oracleTTCSummary
	Done    bool
}

func consumeTTCLobPacket(payload []byte, locatorLen int, ttcVersion byte, proto ttcProtocolInfo, out *oracleTTCLobResponse, maxBytes int) error {
	if out == nil {
		return errors.New("nil Oracle TTC LOB response")
	}
	r := newTTCDecoder(payload)
	for r.remaining() > 0 {
		code, err := r.byte()
		if err != nil {
			return err
		}
		switch code {
		case ttcErrorReturn:
			s, err := parseTTCSummaryFromDecoder(r, ttcVersion, proto)
			if err != nil {
				return err
			}
			out.Summary = s
			if err := s.err(); err != nil {
				return err
			}
			out.Done = true
		case 8:
			if locatorLen > 0 {
				if _, err := r.take(locatorLen); err != nil {
					return err
				}
			}
			v, err := r.compactUint(map[bool]int{true: 8, false: 4}[ttcVersion >= 3])
			if err != nil {
				return err
			}
			out.Size = v
		case ttcFunctionStat:
			if oracleTTCHasEOS(proto) {
				if _, err := r.compactUint(4); err != nil {
					return err
				}
			}
			out.Done = true
		case 14:
			remaining := maxBytes - len(out.Data)
			if remaining < 0 {
				return errors.New("Oracle TTC LOB response exceeds limit")
			}
			b, err := parseTTCLobChunkFromDecoder(r, remaining)
			if err != nil {
				return err
			}
			out.Data = append(out.Data, b...)
		default:
			return fmt.Errorf("unexpected Oracle TTC LOB response code %d", code)
		}
	}
	return nil
}

func cloneOracleTTCLobResponse(in oracleTTCLobResponse) oracleTTCLobResponse {
	out := in
	out.Data = append([]byte(nil), in.Data...)
	return out
}

func readAndConsumeTTCLobData(ctx context.Context, session *tnsDataSession, locatorLen int, data ttcDataTypeInfo, proto ttcProtocolInfo, out *oracleTTCLobResponse, maxBytes int) error {
	buf := make([]byte, 0, 64<<10)
	for fragments := 0; fragments < 4096; fragments++ {
		flags, payload, err := session.ReadData(ctx)
		if err != nil {
			return err
		}
		if flags != 0 {
			return fmt.Errorf("Oracle TTC LOB DATA flags 0x%x unsupported", flags)
		}
		if len(buf)+len(payload) > oracleMaxTTCResponseBytes {
			return fmt.Errorf("Oracle TTC LOB logical response exceeds %d-byte safety limit", oracleMaxTTCResponseBytes)
		}
		buf = append(buf, payload...)
		tmp := cloneOracleTTCLobResponse(*out)
		err = consumeTTCLobPacket(buf, locatorLen, data.TTCVersion, proto, &tmp, maxBytes)
		if errors.Is(err, errTTCTruncated) {
			continue
		}
		if err != nil {
			return err
		}
		*out = tmp
		return nil
	}
	return errors.New("Oracle TTC LOB response fragment limit exceeded")
}

// executeTTCLobRead materializes an Oracle BLOB/CLOB locator for the
// experimental Native Full Reader. It remains bounded to protect worker memory
// and still requires real-instance version/charset qualification before gates
// can be removed.
func (c *Connector) executeTTCLobRead(ctx context.Context, accepted *acceptedSession, proto ttcProtocolInfo, data ttcDataTypeInfo, ref oracleTTCLobRef, maxBytes int) ([]byte, error) {
	if accepted == nil || accepted.Session == nil {
		return nil, errors.New("Oracle TTC LOB read requires accepted session")
	}
	if maxBytes <= 0 || maxBytes > oracleMaxTTCResponseBytes {
		maxBytes = oracleMaxTTCResponseBytes
	}
	req, err := buildTTCLobRequest(ref.Locator, data.TTCVersion, 2, 1)
	if err != nil {
		return nil, err
	}
	if err = accepted.Session.WriteData(ctx, 0, req); err != nil {
		return nil, fmt.Errorf("Oracle TTC LOB read request: %w", err)
	}
	out := oracleTTCLobResponse{}
	for packets := 0; packets < 4096 && !out.Done; packets++ {
		if err = readAndConsumeTTCLobData(ctx, accepted.Session, len(ref.Locator), data, proto, &out, maxBytes); err != nil {
			return nil, fmt.Errorf("Oracle TTC LOB read response: %w", err)
		}
	}
	if !out.Done {
		return nil, errors.New("Oracle TTC LOB response packet limit exceeded")
	}
	return out.Data, nil
}

func parseTTCRowFromDecoder(r *ttcDecoder, cols []oracleTTCColumn, present []bool) ([]any, error) {
	if len(cols) == 0 {
		return nil, errors.New("Oracle TTC row arrived before describe metadata")
	}
	if len(present) == 0 {
		present = make([]bool, len(cols))
		for i := range present {
			present[i] = true
		}
	}
	if len(present) != len(cols) {
		return nil, errors.New("Oracle TTC row bit-vector does not match described columns")
	}
	row := make([]any, len(cols))
	for i := range cols {
		if !present[i] {
			continue
		}
		if cols[i].DataType == oracleTypeROWID {
			v, err := parseTTCROWIDFromDecoder(r)
			if err != nil {
				return nil, fmt.Errorf("Oracle TTC ROWID column %d: %w", i, err)
			}
			row[i] = v
			continue
		}
		b, err := r.clr()
		if err != nil {
			return nil, fmt.Errorf("Oracle TTC row column %d: %w", i, err)
		}
		if cols[i].DataType == oracleTypeBLOB || cols[i].DataType == oracleTypeCLOB {
			locator, err := r.clr()
			if err != nil {
				return nil, fmt.Errorf("Oracle TTC LOB locator column %d: %w", i, err)
			}
			row[i] = oracleTTCLobRef{DataType: cols[i].DataType, CharsetID: cols[i].CharsetID, Inline: append([]byte(nil), b...), Locator: append([]byte(nil), locator...)}
			continue
		}
		v, err := decodeOracleTTCScalar(cols[i], b)
		if err != nil {
			return nil, fmt.Errorf("Oracle TTC row column %d (%s): %w", i, cols[i].Name, err)
		}
		row[i] = v
		if cols[i].DataType == oracleTypeLONG || cols[i].DataType == oracleTypeLongRaw {
			if _, err = r.compactUint(4); err != nil {
				return nil, err
			}
			if _, err = r.compactUint(4); err != nil {
				return nil, err
			}
		}
	}
	return row, nil
}

type oracleTTCQueryState struct {
	Columns      []oracleTTCColumn
	Rows         [][]any
	Header       oracleTTCRowHeader
	Cursor       oracleTTCCursor
	LastSummary  oracleTTCSummary
	SummarySeen  bool
	FunctionStat bool
	RPA          oracleTTCQueryRPA
}

func cloneOracleTTCQueryState(in oracleTTCQueryState) oracleTTCQueryState {
	out := in
	out.Columns = append([]oracleTTCColumn(nil), in.Columns...)
	out.Rows = append([][]any(nil), in.Rows...)
	out.Header.Present = append([]bool(nil), in.Header.Present...)
	out.RPA.SCN = append([]uint64(nil), in.RPA.SCN...)
	out.RPA.TimeZone = append([]byte(nil), in.RPA.TimeZone...)
	out.RPA.QueryID = append([]byte(nil), in.RPA.QueryID...)
	return out
}

// readAndConsumeTTCQueryData treats TTC as a stream over TNS DATA packets. A
// server may split one describe/row/summary item at an arbitrary byte boundary;
// retrying the decoder against the accumulated fragments keeps those packet
// boundaries invisible to the SQL layer while committing state only after a
// complete parse.
func readAndConsumeTTCQueryData(ctx context.Context, session *tnsDataSession, data ttcDataTypeInfo, proto ttcProtocolInfo, state *oracleTTCQueryState, maxRows int) error {
	if session == nil || state == nil {
		return errors.New("Oracle TTC query stream requires session/state")
	}
	buf := make([]byte, 0, 64<<10)
	for fragments := 0; fragments < 4096; fragments++ {
		flags, payload, err := session.ReadData(ctx)
		if err != nil {
			return err
		}
		if flags != 0 {
			return fmt.Errorf("Oracle TTC DATA flags 0x%x unsupported", flags)
		}
		if len(buf)+len(payload) > oracleMaxTTCResponseBytes {
			return fmt.Errorf("Oracle TTC logical response exceeds %d-byte safety limit", oracleMaxTTCResponseBytes)
		}
		buf = append(buf, payload...)
		tmp := cloneOracleTTCQueryState(*state)
		err = consumeTTCQueryPacket(buf, data, proto, &tmp, maxRows)
		if errors.Is(err, errTTCTruncated) {
			continue
		}
		if err != nil {
			return err
		}
		*state = tmp
		return nil
	}
	return errors.New("Oracle TTC response fragment limit exceeded")
}

func consumeTTCQueryPacket(payload []byte, data ttcDataTypeInfo, proto ttcProtocolInfo, state *oracleTTCQueryState, maxRows int) error {
	if state == nil {
		return errors.New("nil Oracle TTC query state")
	}
	r := newTTCDecoder(payload)
	for r.remaining() > 0 {
		code, err := r.byte()
		if err != nil {
			return err
		}
		switch code {
		case ttcDescribe:
			cols, err := parseTTCDescribeFromDecoder(r, data.TTCVersion)
			if err != nil {
				return err
			}
			state.Columns = cols
		case ttcRowHeader:
			h, err := parseTTCRowHeaderFromDecoder(r, len(state.Columns))
			if err != nil {
				return err
			}
			state.Header = h
		case ttcRowData:
			row, err := parseTTCRowFromDecoder(r, state.Columns, state.Header.Present)
			if err != nil {
				return err
			}
			state.Rows = append(state.Rows, row)
			if maxRows > 0 && len(state.Rows) > maxRows {
				return fmt.Errorf("Oracle TTC SELECT exceeded row limit %d", maxRows)
			}
		case 8:
			v, err := parseTTCQueryRPAFromDecoder(r, data.TTCVersion)
			if err != nil {
				return err
			}
			state.RPA = v
		case 21:
			bits, err := parseTTCColumnBitVectorFromDecoder(r, len(state.Columns))
			if err != nil {
				return err
			}
			state.Header.Present = bits
		case ttcErrorReturn:
			s, err := parseTTCSummaryFromDecoder(r, data.TTCVersion, proto)
			if err != nil {
				return err
			}
			state.LastSummary = s
			state.SummarySeen = true
			state.Cursor.update(s)
			if err := s.err(); err != nil {
				return err
			}
		case ttcFunctionStat:
			if oracleTTCHasEOS(proto) {
				if _, err := r.compactUint(4); err != nil {
					return err
				}
			}
			state.FunctionStat = true
		default:
			return fmt.Errorf("unexpected Oracle TTC SELECT response code %d", code)
		}
	}
	return nil
}

func (c *Connector) executeTTCSelectBatched(ctx context.Context, accepted *acceptedSession, proto ttcProtocolInfo, data ttcDataTypeInfo, sql string, fetchRows, maxRows int) (oracleTTCQueryResult, error) {
	var out oracleTTCQueryResult
	if maxRows <= 0 {
		maxRows = 4096
	}
	if maxRows > 65536 {
		return out, errors.New("Oracle TTC SELECT maxRows exceeds qualification limit")
	}
	req, err := buildTTCSelectRequest(sql, data.TTCVersion, fetchRows)
	if err != nil {
		return out, err
	}
	if err = accepted.Session.WriteData(ctx, 0, req); err != nil {
		return out, fmt.Errorf("Oracle TTC SELECT request: %w", err)
	}
	state := oracleTTCQueryState{}
	for packets := 0; packets < 4096; packets++ {
		state.SummarySeen = false
		state.FunctionStat = false
		if err = readAndConsumeTTCQueryData(ctx, accepted.Session, data, proto, &state, maxRows); err != nil {
			return out, fmt.Errorf("Oracle TTC SELECT response: %w", err)
		}
		if state.SummarySeen {
			if state.Cursor.Exhausted {
				out.Columns, out.Rows = state.Columns, state.Rows
				return out, nil
			}
			if state.Cursor.ID == 0 {
				out.Columns, out.Rows = state.Columns, state.Rows
				return out, nil
			}
			if len(state.Rows) >= maxRows {
				if closeReq, e := state.Cursor.closeRequest(); e == nil {
					_ = accepted.Session.WriteData(ctx, 0, closeReq)
				}
				out.Columns, out.Rows = state.Columns, state.Rows[:maxRows]
				return out, nil
			}
			fetchReq, err := state.Cursor.fetchRequest(fetchRows)
			if err != nil {
				return out, err
			}
			if err = accepted.Session.WriteData(ctx, 0, fetchReq); err != nil {
				return out, fmt.Errorf("Oracle TTC fetch request: %w", err)
			}
			continue
		}
		if state.FunctionStat && state.Cursor.ID == 0 {
			out.Columns, out.Rows = state.Columns, state.Rows
			return out, nil
		}
	}
	return out, errors.New("Oracle TTC SELECT response packet limit exceeded")
}
