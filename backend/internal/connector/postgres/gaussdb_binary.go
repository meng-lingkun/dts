package postgresconnector

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"qmigration/backend/internal/domain"
	"strconv"
	"strings"
	"unicode/utf8"
)

// gaussDBBinaryColumn is one length-delimited value from GaussDB's documented
// mppdb_decoding binary format. Values are still the database type's string
// representation, but the binary envelope preserves embedded NUL/non-UTF8
// bytes and distinguishes NULL from a non-NULL zero-length value.
type gaussDBBinaryColumn struct {
	Name    string
	TypeOID uint32
	Value   []byte
	Null    bool
}

type gaussDBBinaryTuple struct {
	Columns []gaussDBBinaryColumn
}

type gaussDBBinaryRecord struct {
	LSN       uint64
	CSN       uint64
	Kind      byte
	Schema    string
	Table     string
	NewTuple  *gaussDBBinaryTuple
	OldTuple  *gaussDBBinaryTuple
	CommitXID uint64
}

func gaussDBBinaryDecodeQuery(function, slot string, maxChanges int, uptoLSN string, tables []string) (string, error) {
	slot, err := gaussDBSlotName(slot)
	if err != nil {
		return "", err
	}
	if function != "pg_logical_slot_peek_binary_changes" && function != "pg_logical_slot_get_binary_changes" {
		return "", fmt.Errorf("unsupported GaussDB binary logical function %q", function)
	}
	white, err := gaussDBWhiteTableList(tables)
	if err != nil {
		return "", err
	}
	if maxChanges <= 0 {
		maxChanges = 4096
	}
	if maxChanges > 100000 {
		maxChanges = 100000
	}
	upto := "NULL"
	if strings.TrimSpace(uptoLSN) != "" {
		norm := normalizeGaussLSN(uptoLSN)
		if _, err := parseReplicationLSN(norm); err != nil {
			return "", fmt.Errorf("invalid GaussDB LSN %q: %w", uptoLSN, err)
		}
		upto = pgLiteral(norm)
	}
	// encode(bytea,'hex') makes the frontend text protocol unambiguous while
	// preserving the exact documented binary-decoding frame bytes.
	return "SELECT location::text,xid::text,encode(data,'hex') FROM " + function + "(" + pgLiteral(slot) + "," + upto + "," + strconv.Itoa(maxChanges) + ",'skip-empty-xacts','on','include-xids','on','white-table-list'," + pgLiteral(white) + ",'enable-ddl-decoding','false')", nil
}

func gaussLSNFromUint64(v uint64) string {
	return fmt.Sprintf("%X/%X", uint32(v>>32), uint32(v))
}

type gaussBinCursor struct {
	b   []byte
	pos int
	end int
}

func (c *gaussBinCursor) remaining() int { return c.end - c.pos }
func (c *gaussBinCursor) u8() (byte, error) {
	if c.remaining() < 1 {
		return 0, errors.New("truncated uint8")
	}
	v := c.b[c.pos]
	c.pos++
	return v, nil
}
func (c *gaussBinCursor) u16() (uint16, error) {
	if c.remaining() < 2 {
		return 0, errors.New("truncated uint16")
	}
	v := binary.BigEndian.Uint16(c.b[c.pos : c.pos+2])
	c.pos += 2
	return v, nil
}
func (c *gaussBinCursor) u32() (uint32, error) {
	if c.remaining() < 4 {
		return 0, errors.New("truncated uint32")
	}
	v := binary.BigEndian.Uint32(c.b[c.pos : c.pos+4])
	c.pos += 4
	return v, nil
}
func (c *gaussBinCursor) u64() (uint64, error) {
	if c.remaining() < 8 {
		return 0, errors.New("truncated uint64")
	}
	v := binary.BigEndian.Uint64(c.b[c.pos : c.pos+8])
	c.pos += 8
	return v, nil
}
func (c *gaussBinCursor) take(n int) ([]byte, error) {
	if n < 0 || c.remaining() < n {
		return nil, fmt.Errorf("truncated %d-byte field", n)
	}
	v := c.b[c.pos : c.pos+n]
	c.pos += n
	return v, nil
}

func parseGaussBinaryTuple(c *gaussBinCursor) (*gaussDBBinaryTuple, error) {
	attr, err := c.u16()
	if err != nil {
		return nil, err
	}
	if attr > 32768 {
		return nil, fmt.Errorf("GaussDB binary tuple has unreasonable attr count %d", attr)
	}
	t := &gaussDBBinaryTuple{Columns: make([]gaussDBBinaryColumn, 0, int(attr))}
	for i := 0; i < int(attr); i++ {
		nlen, err := c.u16()
		if err != nil {
			return nil, fmt.Errorf("column %d name length: %w", i, err)
		}
		if nlen == 0 {
			return nil, fmt.Errorf("column %d has empty name", i)
		}
		name, err := c.take(int(nlen))
		if err != nil {
			return nil, fmt.Errorf("column %d name: %w", i, err)
		}
		oid, err := c.u32()
		if err != nil {
			return nil, fmt.Errorf("column %s type OID: %w", string(name), err)
		}
		vlen, err := c.u32()
		if err != nil {
			return nil, fmt.Errorf("column %s value length: %w", string(name), err)
		}
		col := gaussDBBinaryColumn{Name: string(name), TypeOID: oid}
		switch vlen {
		case 0xffffffff:
			col.Null = true
		case 0xfffffffe:
			// rowno is only emitted when enable-rowno is explicitly enabled. RC15
			// never enables it because migration tables require a primary key.
			return nil, fmt.Errorf("GaussDB binary tuple unexpectedly contains rowno system column %s", col.Name)
		default:
			if uint64(vlen) > uint64(gaussDBMaxTransactionBytes) {
				return nil, fmt.Errorf("GaussDB binary column %s value exceeds transaction safety limit", col.Name)
			}
			value, err := c.take(int(vlen))
			if err != nil {
				return nil, fmt.Errorf("column %s value: %w", col.Name, err)
			}
			col.Value = append([]byte(nil), value...)
		}
		t.Columns = append(t.Columns, col)
	}
	return t, nil
}

func skipGaussLengthString(c *gaussBinCursor, label string) error {
	n, err := c.u32()
	if err != nil {
		return fmt.Errorf("%s length: %w", label, err)
	}
	if _, err := c.take(int(n)); err != nil {
		return fmt.Errorf("%s: %w", label, err)
	}
	return nil
}

func parseGaussBinaryFrame(frame []byte) (gaussDBBinaryRecord, error) {
	var rec gaussDBBinaryRecord
	if len(frame) < 9 {
		return rec, errors.New("GaussDB binary frame shorter than LSN+type")
	}
	c := &gaussBinCursor{b: frame, end: len(frame)}
	lsn, err := c.u64()
	if err != nil {
		return rec, err
	}
	rec.LSN = lsn
	kind, err := c.u8()
	if err != nil {
		return rec, err
	}
	rec.Kind = kind
	switch kind {
	case 'B':
		csn, err := c.u64() // global CSN
		if err != nil {
			return rec, fmt.Errorf("BEGIN CSN: %w", err)
		}
		rec.CSN = csn
		if _, err := c.u64(); err != nil { // first_lsn
			return rec, fmt.Errorf("BEGIN first_lsn: %w", err)
		}
		for c.remaining() > 0 {
			marker, err := c.u8()
			if err != nil {
				return rec, err
			}
			switch marker {
			case 'T':
				if err := skipGaussLengthString(c, "BEGIN timestamp"); err != nil {
					return rec, err
				}
			case 'N':
				if err := skipGaussLengthString(c, "BEGIN username"); err != nil {
					return rec, err
				}
			default:
				return rec, fmt.Errorf("unsupported GaussDB BEGIN marker %q", marker)
			}
		}
	case 'C':
		for c.remaining() > 0 {
			marker, err := c.u8()
			if err != nil {
				return rec, err
			}
			switch marker {
			case 'X':
				xid, err := c.u64()
				if err != nil {
					return rec, fmt.Errorf("COMMIT xid: %w", err)
				}
				rec.CommitXID = xid
			case 'T':
				if err := skipGaussLengthString(c, "COMMIT timestamp"); err != nil {
					return rec, err
				}
			default:
				return rec, fmt.Errorf("unsupported GaussDB COMMIT marker %q", marker)
			}
		}
	case 'I', 'U', 'D':
		slen, err := c.u16()
		if err != nil {
			return rec, fmt.Errorf("schema length: %w", err)
		}
		schema, err := c.take(int(slen))
		if err != nil {
			return rec, fmt.Errorf("schema: %w", err)
		}
		tlen, err := c.u16()
		if err != nil {
			return rec, fmt.Errorf("table length: %w", err)
		}
		table, err := c.take(int(tlen))
		if err != nil {
			return rec, fmt.Errorf("table: %w", err)
		}
		if len(schema) == 0 || len(table) == 0 {
			return rec, errors.New("GaussDB binary DML has empty schema/table")
		}
		rec.Schema, rec.Table = string(schema), string(table)
		for c.remaining() > 0 {
			marker, err := c.u8()
			if err != nil {
				return rec, err
			}
			tuple, err := parseGaussBinaryTuple(c)
			if err != nil {
				return rec, fmt.Errorf("tuple %q: %w", marker, err)
			}
			switch marker {
			case 'N':
				if rec.NewTuple != nil {
					return rec, errors.New("GaussDB binary DML contains duplicate new tuple")
				}
				rec.NewTuple = tuple
			case 'O':
				if rec.OldTuple != nil {
					return rec, errors.New("GaussDB binary DML contains duplicate old tuple")
				}
				rec.OldTuple = tuple
			default:
				return rec, fmt.Errorf("unsupported GaussDB tuple marker %q", marker)
			}
		}
	default:
		return rec, fmt.Errorf("unsupported GaussDB binary logical record type %q", kind)
	}
	return rec, nil
}

func parseGaussBinaryMessage(raw []byte) ([]gaussDBBinaryRecord, error) {
	var out []gaussDBBinaryRecord
	for off := 0; off < len(raw); {
		if len(raw)-off < 4 {
			return nil, errors.New("GaussDB binary message truncated before frame length")
		}
		n := binary.BigEndian.Uint32(raw[off : off+4])
		off += 4
		if n == 0 {
			// A zero length is the documented batch terminator.
			if off != len(raw) {
				return nil, errors.New("GaussDB binary zero-length batch terminator has trailing bytes")
			}
			break
		}
		if uint64(n) > uint64(gaussDBMaxTransactionBytes) || int(n) > len(raw)-off {
			return nil, fmt.Errorf("invalid GaussDB binary frame length %d", n)
		}
		frame := raw[off : off+int(n)]
		off += int(n)
		rec, err := parseGaussBinaryFrame(frame)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
		if off >= len(raw) {
			return nil, errors.New("GaussDB binary frame missing P/F delimiter")
		}
		delim := raw[off]
		off++
		if delim == 'F' {
			if off != len(raw) {
				return nil, errors.New("GaussDB binary F delimiter has trailing bytes")
			}
			break
		}
		if delim != 'P' {
			return nil, fmt.Errorf("invalid GaussDB binary frame delimiter %q", delim)
		}
	}
	if len(out) == 0 {
		return nil, errors.New("empty GaussDB binary logical message")
	}
	return out, nil
}

func decodeGaussBinaryCell(cell []byte) ([]gaussDBBinaryRecord, error) {
	s := strings.TrimSpace(string(cell))
	if strings.HasPrefix(s, `\x`) || strings.HasPrefix(s, `\X`) {
		s = s[2:]
	}
	if len(s)%2 != 0 {
		return nil, errors.New("GaussDB binary data hex has odd length")
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("decode GaussDB binary data hex: %w", err)
	}
	return parseGaussBinaryMessage(b)
}

func gaussDBBinaryField(col gaussDBBinaryColumn) (domain.CDCField, error) {
	f := domain.CDCField{Column: col.Name}
	if col.Null {
		f.Null = true
		return f, nil
	}
	v := append([]byte(nil), col.Value...)
	// PostgreSQL/GaussDB bytea uses its textual type-output representation
	// inside the logical frame. OID 17 is stable for bytea; require hex output
	// so QMigration never guesses escape-format bytes.
	if col.TypeOID == 17 {
		if len(v) == 0 {
			f.Encoding = "base64"
			f.Value = ""
			return f, nil
		}
		if len(v) < 2 || v[0] != '\\' || (v[1] != 'x' && v[1] != 'X') {
			return f, fmt.Errorf("GaussDB bytea column %s is not in hex output format", col.Name)
		}
		raw, err := hex.DecodeString(string(v[2:]))
		if err != nil {
			return f, fmt.Errorf("decode GaussDB bytea column %s: %w", col.Name, err)
		}
		f.Encoding = "base64"
		f.Value = base64.StdEncoding.EncodeToString(raw)
		return f, nil
	}
	// Length-delimited binary decoding preserves arbitrary bytes. Use base64
	// whenever JSON text transport could otherwise normalize invalid UTF-8 or
	// when a NUL is present; valid UTF-8 remains byte-identical as text.
	if !utf8.Valid(v) || bytes.IndexByte(v, 0) >= 0 {
		f.Encoding = "base64"
		f.Value = base64.StdEncoding.EncodeToString(v)
		return f, nil
	}
	f.Value = string(v)
	return f, nil
}

func gaussDBBinaryTupleFields(t *gaussDBBinaryTuple) ([]domain.CDCField, error) {
	if t == nil {
		return nil, nil
	}
	out := make([]domain.CDCField, 0, len(t.Columns))
	seen := map[string]bool{}
	for _, col := range t.Columns {
		if strings.TrimSpace(col.Name) == "" || seen[strings.ToLower(col.Name)] {
			return nil, fmt.Errorf("GaussDB binary tuple has invalid/duplicate column %q", col.Name)
		}
		seen[strings.ToLower(col.Name)] = true
		f, err := gaussDBBinaryField(col)
		if err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, nil
}

func gaussDBBinaryEvent(rec gaussDBBinaryRecord) (domain.CDCEvent, error) {
	ev := domain.CDCEvent{SourceSchema: rec.Schema, SourceTable: rec.Table}
	before, err := gaussDBBinaryTupleFields(rec.OldTuple)
	if err != nil {
		return ev, err
	}
	after, err := gaussDBBinaryTupleFields(rec.NewTuple)
	if err != nil {
		return ev, err
	}
	switch rec.Kind {
	case 'I':
		if len(after) == 0 || len(before) != 0 {
			return ev, errors.New("GaussDB binary INSERT requires only a new tuple")
		}
		ev.Operation, ev.After = domain.CDCInsert, after
	case 'U':
		if len(after) == 0 || len(before) == 0 {
			return ev, errors.New("GaussDB binary UPDATE requires new and old-key tuples")
		}
		ev.Operation, ev.Before, ev.After = domain.CDCUpdate, before, after
	case 'D':
		if len(before) == 0 || len(after) != 0 {
			return ev, errors.New("GaussDB binary DELETE requires only an old-key tuple")
		}
		ev.Operation, ev.Before = domain.CDCDelete, before
	default:
		return ev, fmt.Errorf("GaussDB record %q is not DML", rec.Kind)
	}
	return ev, nil
}

// ParseGaussDBBinaryRows decodes the documented mppdb_decoding binary
// envelope returned by pg_logical_slot_*_binary_changes. It intentionally
// rejects unsupported frame markers and partial transactions before any source
// slot is advanced.
func ParseGaussDBBinaryRows(rows *RawRows, slot string) ([]GaussDBTransaction, error) {
	if rows == nil {
		return nil, errors.New("nil GaussDB binary logical rows")
	}
	var out []GaussDBTransaction
	var current *GaussDBTransaction
	bytesInTxn := 0
	ordinal := 0
	for i, row := range rows.Rows {
		if len(row) < 3 {
			return nil, fmt.Errorf("GaussDB binary logical row %d has %d columns", i, len(row))
		}
		rowLSN := normalizeGaussLSN(string(row[0]))
		if _, err := parseReplicationLSN(rowLSN); err != nil {
			return nil, fmt.Errorf("GaussDB binary row %d has invalid location %q: %w", i, rowLSN, err)
		}
		xid := strings.TrimSpace(string(row[1]))
		recs, err := decodeGaussBinaryCell(row[2])
		if err != nil {
			return nil, fmt.Errorf("GaussDB binary logical row %d: %w", i, err)
		}
		for _, rec := range recs {
			frameLSN := normalizeGaussLSN(gaussLSNFromUint64(rec.LSN))
			if frameLSN != rowLSN {
				return nil, fmt.Errorf("GaussDB binary frame LSN %s does not match SQL location %s", frameLSN, rowLSN)
			}
			switch rec.Kind {
			case 'B':
				if current != nil {
					return nil, fmt.Errorf("GaussDB binary BEGIN %s before transaction %s committed", xid, current.XID)
				}
				current = &GaussDBTransaction{XID: xid, CSN: rec.CSN}
				bytesInTxn, ordinal = 0, 0
			case 'C':
				if current == nil {
					return nil, fmt.Errorf("GaussDB binary COMMIT %s without BEGIN", xid)
				}
				if xid != "" && current.XID != "" && xid != current.XID {
					return nil, fmt.Errorf("GaussDB binary COMMIT xid %s does not match BEGIN %s", xid, current.XID)
				}
				if rec.CommitXID != 0 && xid != "" && strconv.FormatUint(rec.CommitXID, 10) != xid {
					return nil, fmt.Errorf("GaussDB binary COMMIT payload xid %d does not match SQL xid %s", rec.CommitXID, xid)
				}
				current.CommitLSN = frameLSN
				for j := range current.Events {
					current.Events[j].ID = fmt.Sprintf("gaussdb:%s:%s:%d", current.XID, frameLSN, j)
					current.Events[j].PositionType = "GAUSSDB_LSN"
					current.Events[j].PositionValue = frameLSN
					current.Events[j].Resource = slot
				}
				if len(current.Events) > 0 {
					out = append(out, *current)
				}
				current = nil
			case 'I', 'U', 'D':
				if current == nil {
					return nil, fmt.Errorf("GaussDB binary DML outside BEGIN/COMMIT at %s", frameLSN)
				}
				if xid != "" && current.XID != "" && xid != current.XID {
					return nil, fmt.Errorf("GaussDB binary logical xid changed from %s to %s", current.XID, xid)
				}
				ev, err := gaussDBBinaryEvent(rec)
				if err != nil {
					return nil, fmt.Errorf("GaussDB transaction %s: %w", current.XID, err)
				}
				ordinal++
				if ordinal > gaussDBMaxTransactionEvents {
					return nil, fmt.Errorf("GaussDB transaction %s exceeds %d events", current.XID, gaussDBMaxTransactionEvents)
				}
				for _, f := range append(append([]domain.CDCField{}, ev.Before...), ev.After...) {
					bytesInTxn += len(f.Column) + len(f.Value)
				}
				if bytesInTxn > gaussDBMaxTransactionBytes {
					return nil, fmt.Errorf("GaussDB transaction %s exceeds %d decoded bytes", current.XID, gaussDBMaxTransactionBytes)
				}
				current.Events = append(current.Events, ev)
			default:
				return nil, fmt.Errorf("unsupported GaussDB binary record %q", rec.Kind)
			}
		}
	}
	if current != nil {
		return nil, fmt.Errorf("GaussDB binary logical batch ended before COMMIT for xid %s", current.XID)
	}
	return out, nil
}
