package pgoutput

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"qmigration/backend/internal/domain"
	"strconv"
	"time"
)

type Column struct {
	Flags        uint8  `json:"flags"`
	Name         string `json:"name"`
	TypeOID      uint32 `json:"type_oid"`
	TypeModifier int32  `json:"type_modifier"`
}

type Relation struct {
	ID              uint32   `json:"id"`
	Namespace       string   `json:"namespace"`
	Name            string   `json:"name"`
	ReplicaIdentity byte     `json:"replica_identity"`
	Columns         []Column `json:"columns"`
}

type TupleValue struct {
	Kind byte
	Data []byte
}

type XLogData struct {
	WALStart   uint64
	WALEnd     uint64
	ServerTime time.Time
	Plugin     []byte
}

type Transaction struct {
	XID        uint32            `json:"xid"`
	Events     []domain.CDCEvent `json:"events"`
	CommitLSN  uint64            `json:"commit_lsn"`
	EndLSN     uint64            `json:"end_lsn"`
	CommitTime time.Time         `json:"commit_time"`
}

type Decoder struct {
	relations    map[uint32]Relation
	current      *Transaction
	positionType string
	idPrefix     string
}

func NewDecoder() *Decoder { return NewDecoderWithDialect("LSN", "pg") }
func NewDecoderWithDialect(positionType, idPrefix string) *Decoder {
	if positionType == "" {
		positionType = "LSN"
	}
	if idPrefix == "" {
		idPrefix = "pg"
	}
	return &Decoder{relations: map[uint32]Relation{}, positionType: positionType, idPrefix: idPrefix}
}
func (d *Decoder) Relation(id uint32) (Relation, bool) { r, ok := d.relations[id]; return r, ok }

func FormatLSN(v uint64) string { return fmt.Sprintf("%X/%X", uint32(v>>32), uint32(v)) }
func ParseLSN(s string) (uint64, error) {
	var hi, lo uint64
	if _, err := fmt.Sscanf(s, "%X/%X", &hi, &lo); err != nil {
		return 0, err
	}
	if hi > 0xffffffff || lo > 0xffffffff {
		return 0, errors.New("LSN component overflow")
	}
	return hi<<32 | lo, nil
}

func postgresTime(micros int64) time.Time {
	return time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(micros) * time.Microsecond)
}

func ParseCopyData(p []byte) (*XLogData, bool, error) {
	if len(p) == 0 {
		return nil, false, errors.New("empty replication CopyData")
	}
	if p[0] == 'k' { // primary keepalive
		if len(p) < 18 {
			return nil, true, errors.New("short primary keepalive")
		}
		return nil, true, nil
	}
	if p[0] != 'w' {
		return nil, false, fmt.Errorf("unsupported replication CopyData type %q", p[0])
	}
	if len(p) < 25 {
		return nil, false, errors.New("short XLogData")
	}
	return &XLogData{WALStart: binary.BigEndian.Uint64(p[1:9]), WALEnd: binary.BigEndian.Uint64(p[9:17]), ServerTime: postgresTime(int64(binary.BigEndian.Uint64(p[17:25]))), Plugin: append([]byte(nil), p[25:]...)}, false, nil
}

type cursor struct {
	b []byte
	i int
}

func (c *cursor) need(n int) error {
	if n < 0 || c.i+n > len(c.b) {
		return errors.New("short pgoutput message")
	}
	return nil
}
func (c *cursor) u8() (byte, error) {
	if err := c.need(1); err != nil {
		return 0, err
	}
	v := c.b[c.i]
	c.i++
	return v, nil
}
func (c *cursor) u16() (uint16, error) {
	if err := c.need(2); err != nil {
		return 0, err
	}
	v := binary.BigEndian.Uint16(c.b[c.i : c.i+2])
	c.i += 2
	return v, nil
}
func (c *cursor) u32() (uint32, error) {
	if err := c.need(4); err != nil {
		return 0, err
	}
	v := binary.BigEndian.Uint32(c.b[c.i : c.i+4])
	c.i += 4
	return v, nil
}
func (c *cursor) i32() (int32, error) { v, e := c.u32(); return int32(v), e }
func (c *cursor) u64() (uint64, error) {
	if err := c.need(8); err != nil {
		return 0, err
	}
	v := binary.BigEndian.Uint64(c.b[c.i : c.i+8])
	c.i += 8
	return v, nil
}
func (c *cursor) cstr() (string, error) {
	start := c.i
	for c.i < len(c.b) && c.b[c.i] != 0 {
		c.i++
	}
	if c.i >= len(c.b) {
		return "", errors.New("unterminated pgoutput cstring")
	}
	s := string(c.b[start:c.i])
	c.i++
	return s, nil
}
func (c *cursor) bytes(n int) ([]byte, error) {
	if err := c.need(n); err != nil {
		return nil, err
	}
	v := append([]byte(nil), c.b[c.i:c.i+n]...)
	c.i += n
	return v, nil
}

func parseTuple(c *cursor) ([]TupleValue, error) {
	n, err := c.u16()
	if err != nil {
		return nil, err
	}
	out := make([]TupleValue, int(n))
	for i := range out {
		k, err := c.u8()
		if err != nil {
			return nil, err
		}
		out[i].Kind = k
		switch k {
		case 'n', 'u':
		case 't', 'b':
			l, err := c.u32()
			if err != nil {
				return nil, err
			}
			if l > 128<<20 {
				return nil, errors.New("pgoutput tuple field too large")
			}
			v, err := c.bytes(int(l))
			if err != nil {
				return nil, err
			}
			out[i].Data = v
		default:
			return nil, fmt.Errorf("unsupported tuple kind %q", k)
		}
	}
	return out, nil
}

func tupleFields(rel Relation, values []TupleValue, keyOnly bool) []domain.CDCField {
	columns := rel.Columns
	if keyOnly && len(values) != len(rel.Columns) {
		keys := make([]Column, 0, len(rel.Columns))
		for _, col := range rel.Columns {
			if col.Flags&1 != 0 {
				keys = append(keys, col)
			}
		}
		if len(keys) == len(values) {
			columns = keys
		}
	}
	out := make([]domain.CDCField, 0, len(values))
	for i, v := range values {
		if i >= len(columns) || v.Kind == 'u' {
			continue
		}
		f := domain.CDCField{Column: columns[i].Name}
		switch v.Kind {
		case 'n':
			f.Null = true
		case 't':
			f.Value = string(v.Data)
		case 'b':
			f.Value = base64.StdEncoding.EncodeToString(v.Data)
			f.Encoding = "base64"
		}
		out = append(out, f)
	}
	return out
}

func (d *Decoder) decodeRelation(c *cursor) error {
	id, e := c.u32()
	if e != nil {
		return e
	}
	ns, e := c.cstr()
	if e != nil {
		return e
	}
	name, e := c.cstr()
	if e != nil {
		return e
	}
	repl, e := c.u8()
	if e != nil {
		return e
	}
	n, e := c.u16()
	if e != nil {
		return e
	}
	r := Relation{ID: id, Namespace: ns, Name: name, ReplicaIdentity: repl, Columns: make([]Column, int(n))}
	for i := range r.Columns {
		flags, e := c.u8()
		if e != nil {
			return e
		}
		cn, e := c.cstr()
		if e != nil {
			return e
		}
		oid, e := c.u32()
		if e != nil {
			return e
		}
		mod, e := c.i32()
		if e != nil {
			return e
		}
		r.Columns[i] = Column{Flags: flags, Name: cn, TypeOID: oid, TypeModifier: mod}
	}
	d.relations[id] = r
	return nil
}

func (d *Decoder) relationAndTuple(c *cursor) (Relation, []TupleValue, error) {
	id, e := c.u32()
	if e != nil {
		return Relation{}, nil, e
	}
	r, ok := d.relations[id]
	if !ok {
		return Relation{}, nil, fmt.Errorf("unknown pgoutput relation %d", id)
	}
	_, e = c.u8()
	if e != nil {
		return Relation{}, nil, e
	}
	t, e := parseTuple(c)
	return r, t, e
}

// Push accepts a raw PostgreSQL CopyData payload (leading 'w' or 'k'). A
// complete source transaction is returned only after COMMIT, so callers can
// apply and checkpoint atomically.
func (d *Decoder) Push(copyData []byte) (*Transaction, error) {
	x, keepalive, err := ParseCopyData(copyData)
	if err != nil {
		return nil, err
	}
	if keepalive {
		return nil, nil
	}
	if len(x.Plugin) == 0 {
		return nil, errors.New("empty pgoutput plugin message")
	}
	c := &cursor{b: x.Plugin}
	typ, _ := c.u8()
	switch typ {
	case 'B':
		final, e := c.u64()
		if e != nil {
			return nil, e
		}
		commit, e := c.u64()
		if e != nil {
			return nil, e
		}
		xid, e := c.u32()
		if e != nil {
			return nil, e
		}
		_ = final
		d.current = &Transaction{XID: xid, CommitTime: postgresTime(int64(commit))}
	case 'C':
		_, e := c.u8()
		if e != nil {
			return nil, e
		}
		commit, e := c.u64()
		if e != nil {
			return nil, e
		}
		end, e := c.u64()
		if e != nil {
			return nil, e
		}
		tm, e := c.u64()
		if e != nil {
			return nil, e
		}
		if d.current == nil {
			return nil, errors.New("pgoutput COMMIT without BEGIN")
		}
		d.current.CommitLSN = commit
		d.current.EndLSN = end
		d.current.CommitTime = postgresTime(int64(tm))
		pos := FormatLSN(end)
		ts := d.current.CommitTime.UnixMilli()
		for i := range d.current.Events {
			d.current.Events[i].PositionType = d.positionType
			d.current.Events[i].PositionValue = pos
			d.current.Events[i].SourceTimestampMS = ts
		}
		out := d.current
		d.current = nil
		return out, nil
	case 'R':
		return nil, d.decodeRelation(c)
	case 'I':
		r, t, e := d.relationAndTuple(c)
		if e != nil {
			return nil, e
		}
		if d.current == nil {
			return nil, errors.New("pgoutput INSERT outside transaction")
		}
		d.current.Events = append(d.current.Events, domain.CDCEvent{ID: d.idPrefix + ":" + strconv.FormatUint(uint64(x.WALStart), 10), Operation: domain.CDCInsert, SourceSchema: r.Namespace, SourceTable: r.Name, After: tupleFields(r, t, false)})
	case 'U':
		id, e := c.u32()
		if e != nil {
			return nil, e
		}
		r, ok := d.relations[id]
		if !ok {
			return nil, fmt.Errorf("unknown pgoutput relation %d", id)
		}
		tag, e := c.u8()
		if e != nil {
			return nil, e
		}
		var before []TupleValue
		keyOnly := tag == 'K'
		if tag == 'K' || tag == 'O' {
			before, e = parseTuple(c)
			if e != nil {
				return nil, e
			}
			tag, e = c.u8()
			if e != nil {
				return nil, e
			}
		}
		if tag != 'N' {
			return nil, fmt.Errorf("pgoutput UPDATE expected new tuple, got %q", tag)
		}
		after, e := parseTuple(c)
		if e != nil {
			return nil, e
		}
		if d.current == nil {
			return nil, errors.New("pgoutput UPDATE outside transaction")
		}
		d.current.Events = append(d.current.Events, domain.CDCEvent{ID: d.idPrefix + ":" + strconv.FormatUint(uint64(x.WALStart), 10), Operation: domain.CDCUpdate, SourceSchema: r.Namespace, SourceTable: r.Name, Before: tupleFields(r, before, keyOnly), After: tupleFields(r, after, false)})
	case 'D':
		id, e := c.u32()
		if e != nil {
			return nil, e
		}
		r, ok := d.relations[id]
		if !ok {
			return nil, fmt.Errorf("unknown pgoutput relation %d", id)
		}
		tag, e := c.u8()
		if e != nil {
			return nil, e
		}
		if tag != 'K' && tag != 'O' {
			return nil, fmt.Errorf("pgoutput DELETE tuple tag %q", tag)
		}
		before, e := parseTuple(c)
		if e != nil {
			return nil, e
		}
		if d.current == nil {
			return nil, errors.New("pgoutput DELETE outside transaction")
		}
		d.current.Events = append(d.current.Events, domain.CDCEvent{ID: d.idPrefix + ":" + strconv.FormatUint(uint64(x.WALStart), 10), Operation: domain.CDCDelete, SourceSchema: r.Namespace, SourceTable: r.Name, Before: tupleFields(r, before, tag == 'K')})
	case 'Y': // Type message; relation OIDs are sufficient for text-mode replay.
		return nil, nil
	case 'O', 'M', 'T': // origin/message/truncate are not row mutations in V0.5.
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported pgoutput message type %q", typ)
	}
	return nil, nil
}
