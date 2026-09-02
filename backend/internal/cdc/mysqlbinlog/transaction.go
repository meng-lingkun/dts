package mysqlbinlog

import (
	"fmt"
	"strings"
)

type Transaction struct {
	Events   []*Event `json:"events"`
	File     string   `json:"file,omitempty"`
	StartPos uint32   `json:"start_pos,omitempty"`
	EndPos   uint32   `json:"end_pos,omitempty"`
	XID      uint64   `json:"xid,omitempty"`
	GTID     string   `json:"gtid,omitempty"`
}

// Assembler groups row/query events into source transaction boundaries. It is
// intentionally independent from row-value decoding, so the transport can
// safely checkpoint only after a complete XID/COMMIT has been applied.
type Assembler struct {
	file     string
	cur      *Transaction
	nextGTID string
}

func (a *Assembler) File() string        { return a.file }
func (a *Assembler) SetFile(file string) { a.file = file }

func (a *Assembler) Push(e *Event) (*Transaction, error) {
	if e == nil {
		return nil, nil
	}
	if e.Header.Type == GTIDEvent || e.Header.Type == AnonymousGTIDEvent {
		g, err := ParseGTIDEvent(e)
		if err != nil {
			return nil, err
		}
		a.nextGTID = g.String()
		return nil, nil
	}
	if e.Header.Type == RotateEvent {
		r, err := ParseRotate(e)
		if err != nil {
			return nil, err
		}
		a.file = r.File
		return nil, nil
	}
	if e.Header.Type == QueryEvent {
		q, err := ParseQuery(e)
		if err != nil {
			return nil, err
		}
		if IsBeginQuery(q) {
			a.cur = &Transaction{File: a.file, StartPos: e.Header.LogPos, GTID: a.nextGTID}
			return nil, nil
		}
		if strings.EqualFold(strings.TrimSpace(q.SQL), "ROLLBACK") {
			a.cur = nil
			a.nextGTID = ""
			return nil, nil
		}
		if strings.EqualFold(strings.TrimSpace(q.SQL), "COMMIT") {
			if a.cur == nil {
				return nil, nil
			}
			a.cur.EndPos = e.Header.LogPos
			tx := a.cur
			a.cur = nil
			a.nextGTID = ""
			return tx, nil
		}
	}
	if e.Header.Type == XIDEvent {
		x, err := ParseXID(e)
		if err != nil {
			return nil, err
		}
		if a.cur == nil {
			return nil, nil
		}
		a.cur.XID = x.ID
		a.cur.EndPos = e.Header.LogPos
		tx := a.cur
		a.cur = nil
		a.nextGTID = ""
		return tx, nil
	}
	if isRows(e.Header.Type) {
		if a.cur == nil {
			a.cur = &Transaction{File: a.file, StartPos: e.Header.LogPos, GTID: a.nextGTID}
		}
		a.cur.Events = append(a.cur.Events, e)
	}
	return nil, nil
}

func isRows(t byte) bool {
	switch t {
	case WriteRowsEventV1, UpdateRowsEventV1, DeleteRowsEventV1, WriteRowsEventV2, UpdateRowsEventV2, DeleteRowsEventV2, PartialUpdateRowsEvent:
		return true
	default:
		return false
	}
}

func (t Transaction) Position() string {
	if t.File == "" {
		return fmt.Sprintf("%d", t.EndPos)
	}
	return fmt.Sprintf("%s:%d", t.File, t.EndPos)
}

// PendingGTID returns the GTID announced for the next transaction/standalone
// statement. DDL QueryEvents are autocommit statements and therefore need to
// checkpoint this value without passing through a row XID transaction.
func (a *Assembler) PendingGTID() string { return a.nextGTID }

// ConsumePendingGTID clears and returns the GTID for a completed standalone
// statement such as DDL.
func (a *Assembler) ConsumePendingGTID() string {
	v := a.nextGTID
	a.nextGTID = ""
	return v
}
