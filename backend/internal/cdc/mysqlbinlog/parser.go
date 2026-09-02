package mysqlbinlog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
)

const HeaderSize = 19

// Event type values follow the MySQL binlog event type enum.
const (
	UnknownEvent            byte = 0
	StartEventV3            byte = 1
	QueryEvent              byte = 2
	StopEvent               byte = 3
	RotateEvent             byte = 4
	FormatDescriptionEvent  byte = 15
	XIDEvent                byte = 16
	TableMapEvent           byte = 19
	WriteRowsEventV1        byte = 23
	UpdateRowsEventV1       byte = 24
	DeleteRowsEventV1       byte = 25
	WriteRowsEventV2        byte = 30
	UpdateRowsEventV2       byte = 31
	DeleteRowsEventV2       byte = 32
	GTIDEvent               byte = 33
	AnonymousGTIDEvent      byte = 34
	PartialUpdateRowsEvent  byte = 39
	TransactionPayloadEvent byte = 40
)

type Header struct {
	Timestamp uint32 `json:"timestamp"`
	Type      byte   `json:"type"`
	ServerID  uint32 `json:"server_id"`
	EventSize uint32 `json:"event_size"`
	LogPos    uint32 `json:"log_pos"`
	Flags     uint16 `json:"flags"`
}

type Event struct {
	Header  Header `json:"header"`
	Payload []byte `json:"-"`
}

type Parser struct {
	// ChecksumBytes is normally 0 or 4 (CRC32). The parser validates framing and
	// strips the checksum bytes but deliberately leaves CRC verification to the
	// replication transport layer, which knows whether checksums were negotiated.
	ChecksumBytes int
}

func (p Parser) Parse(raw []byte) (*Event, error) {
	if len(raw) < HeaderSize {
		return nil, fmt.Errorf("binlog event too short: %d", len(raw))
	}
	h := Header{
		Timestamp: binary.LittleEndian.Uint32(raw[0:4]),
		Type:      raw[4],
		ServerID:  binary.LittleEndian.Uint32(raw[5:9]),
		EventSize: binary.LittleEndian.Uint32(raw[9:13]),
		LogPos:    binary.LittleEndian.Uint32(raw[13:17]),
		Flags:     binary.LittleEndian.Uint16(raw[17:19]),
	}
	if h.EventSize < HeaderSize {
		return nil, fmt.Errorf("invalid binlog event size %d", h.EventSize)
	}
	if int(h.EventSize) != len(raw) {
		return nil, fmt.Errorf("binlog event size mismatch header=%d actual=%d", h.EventSize, len(raw))
	}
	end := len(raw) - p.ChecksumBytes
	if p.ChecksumBytes < 0 || end < HeaderSize {
		return nil, errors.New("invalid binlog checksum length")
	}
	return &Event{Header: h, Payload: append([]byte(nil), raw[HeaderSize:end]...)}, nil
}

type Rotate struct {
	Position uint64 `json:"position"`
	File     string `json:"file"`
}

func ParseRotate(e *Event) (*Rotate, error) {
	if e == nil || e.Header.Type != RotateEvent {
		return nil, errors.New("not a ROTATE_EVENT")
	}
	if len(e.Payload) < 8 {
		return nil, errors.New("ROTATE_EVENT payload too short")
	}
	return &Rotate{Position: binary.LittleEndian.Uint64(e.Payload[:8]), File: string(e.Payload[8:])}, nil
}

type Query struct {
	ThreadID uint32 `json:"thread_id"`
	Schema   string `json:"schema"`
	SQL      string `json:"sql"`
}

func ParseQuery(e *Event) (*Query, error) {
	if e == nil || e.Header.Type != QueryEvent {
		return nil, errors.New("not a QUERY_EVENT")
	}
	p := e.Payload
	if len(p) < 13 {
		return nil, errors.New("QUERY_EVENT payload too short")
	}
	threadID := binary.LittleEndian.Uint32(p[0:4])
	schemaLen := int(p[8])
	statusLen := int(binary.LittleEndian.Uint16(p[11:13]))
	off := 13 + statusLen
	if off+schemaLen+1 > len(p) {
		return nil, errors.New("QUERY_EVENT schema/status length exceeds payload")
	}
	schema := string(p[off : off+schemaLen])
	off += schemaLen + 1 // NUL
	return &Query{ThreadID: threadID, Schema: schema, SQL: string(p[off:])}, nil
}

func IsBeginQuery(q *Query) bool {
	return q != nil && strings.EqualFold(strings.TrimSpace(q.SQL), "BEGIN")
}
func IsCommitQuery(q *Query) bool {
	if q == nil {
		return false
	}
	s := strings.TrimSpace(q.SQL)
	return strings.EqualFold(s, "COMMIT") || strings.EqualFold(s, "ROLLBACK")
}

type XID struct {
	ID uint64 `json:"id"`
}

func ParseXID(e *Event) (*XID, error) {
	if e == nil || e.Header.Type != XIDEvent {
		return nil, errors.New("not an XID_EVENT")
	}
	if len(e.Payload) < 8 {
		return nil, errors.New("XID_EVENT payload too short")
	}
	return &XID{ID: binary.LittleEndian.Uint64(e.Payload[:8])}, nil
}

// readLenEnc reads the compact integer encoding used by table-map and row events.
func readLenEnc(b []byte) (uint64, int, error) {
	if len(b) == 0 {
		return 0, 0, errors.New("missing length-encoded integer")
	}
	switch b[0] {
	case 0xfc:
		if len(b) < 3 {
			return 0, 0, errors.New("short lenenc16")
		}
		return uint64(binary.LittleEndian.Uint16(b[1:3])), 3, nil
	case 0xfd:
		if len(b) < 4 {
			return 0, 0, errors.New("short lenenc24")
		}
		return uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16, 4, nil
	case 0xfe:
		if len(b) < 9 {
			return 0, 0, errors.New("short lenenc64")
		}
		return binary.LittleEndian.Uint64(b[1:9]), 9, nil
	case 0xfb:
		return 0, 1, errors.New("NULL lenenc is invalid in binlog metadata")
	default:
		return uint64(b[0]), 1, nil
	}
}

func readUint48LE(b []byte) uint64 {
	return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 | uint64(b[4])<<32 | uint64(b[5])<<40
}

type TableMap struct {
	TableID     uint64 `json:"table_id"`
	Flags       uint16 `json:"flags"`
	Schema      string `json:"schema"`
	Table       string `json:"table"`
	ColumnTypes []byte `json:"column_types"`
	ColumnMeta  []byte `json:"column_metadata"`
	NullBitmap  []byte `json:"null_bitmap"`
}

func ParseTableMap(e *Event) (*TableMap, error) {
	if e == nil || e.Header.Type != TableMapEvent {
		return nil, errors.New("not a TABLE_MAP_EVENT")
	}
	p := e.Payload
	if len(p) < 10 {
		return nil, errors.New("TABLE_MAP_EVENT payload too short")
	}
	out := &TableMap{TableID: readUint48LE(p[:6]), Flags: binary.LittleEndian.Uint16(p[6:8])}
	off := 8
	schemaLen := int(p[off])
	off++
	if off+schemaLen+1 > len(p) {
		return nil, errors.New("invalid table-map schema length")
	}
	out.Schema = string(p[off : off+schemaLen])
	off += schemaLen + 1
	if off >= len(p) {
		return nil, errors.New("missing table-map table length")
	}
	tableLen := int(p[off])
	off++
	if off+tableLen+1 > len(p) {
		return nil, errors.New("invalid table-map table length")
	}
	out.Table = string(p[off : off+tableLen])
	off += tableLen + 1
	count, n, err := readLenEnc(p[off:])
	if err != nil {
		return nil, err
	}
	off += n
	if count > uint64(len(p)-off) {
		return nil, errors.New("table-map column count exceeds payload")
	}
	out.ColumnTypes = append([]byte(nil), p[off:off+int(count)]...)
	off += int(count)
	metaLen, n, err := readLenEnc(p[off:])
	if err != nil {
		return nil, err
	}
	off += n
	if metaLen > uint64(len(p)-off) {
		return nil, errors.New("table-map metadata exceeds payload")
	}
	out.ColumnMeta = append([]byte(nil), p[off:off+int(metaLen)]...)
	off += int(metaLen)
	nullLen := (int(count) + 7) / 8
	if off+nullLen > len(p) {
		return nil, errors.New("table-map null bitmap exceeds payload")
	}
	out.NullBitmap = append([]byte(nil), p[off:off+nullLen]...)
	return out, nil
}

type Rows struct {
	EventType    byte   `json:"event_type"`
	TableID      uint64 `json:"table_id"`
	Flags        uint16 `json:"flags"`
	ColumnCount  uint64 `json:"column_count"`
	BeforeBitmap []byte `json:"before_bitmap"`
	AfterBitmap  []byte `json:"after_bitmap,omitempty"`
	RowData      []byte `json:"-"`
	Update       bool   `json:"update"`
}

func ParseRows(e *Event) (*Rows, error) {
	if e == nil || !isRows(e.Header.Type) {
		return nil, errors.New("not a ROWS_EVENT")
	}
	p := e.Payload
	if len(p) < 8 {
		return nil, errors.New("ROWS_EVENT payload too short")
	}
	update := e.Header.Type == UpdateRowsEventV1 || e.Header.Type == UpdateRowsEventV2 || e.Header.Type == PartialUpdateRowsEvent
	out := &Rows{EventType: e.Header.Type, TableID: readUint48LE(p[:6]), Flags: binary.LittleEndian.Uint16(p[6:8]), Update: update}
	off := 8
	if e.Header.Type == WriteRowsEventV2 || e.Header.Type == UpdateRowsEventV2 || e.Header.Type == DeleteRowsEventV2 || e.Header.Type == PartialUpdateRowsEvent {
		if len(p) < 10 {
			return nil, errors.New("ROWS_EVENT_V2 payload too short")
		}
		extraLen := int(binary.LittleEndian.Uint16(p[8:10]))
		if extraLen < 2 || 8+extraLen > len(p) {
			return nil, errors.New("invalid rows extra-data length")
		}
		off = 8 + extraLen
	}
	count, n, err := readLenEnc(p[off:])
	if err != nil {
		return nil, err
	}
	off += n
	out.ColumnCount = count
	bitmapLen := (int(count) + 7) / 8
	if off+bitmapLen > len(p) {
		return nil, errors.New("rows before bitmap exceeds payload")
	}
	out.BeforeBitmap = append([]byte(nil), p[off:off+bitmapLen]...)
	off += bitmapLen
	if out.Update {
		if off+bitmapLen > len(p) {
			return nil, errors.New("rows after bitmap exceeds payload")
		}
		out.AfterBitmap = append([]byte(nil), p[off:off+bitmapLen]...)
		off += bitmapLen
	}
	out.RowData = append([]byte(nil), p[off:]...)
	return out, nil
}

func ParseRowsV2(e *Event) (*Rows, error) {
	if e == nil || (e.Header.Type != WriteRowsEventV2 && e.Header.Type != UpdateRowsEventV2 && e.Header.Type != DeleteRowsEventV2 && e.Header.Type != PartialUpdateRowsEvent) {
		return nil, errors.New("not a ROWS_EVENT_V2")
	}
	return ParseRows(e)
}
