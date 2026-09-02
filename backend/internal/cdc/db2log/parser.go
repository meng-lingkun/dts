package db2log

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"
	"unicode/utf8"

	"qmigration/backend/internal/domain"
)

const (
	LogTypeAbort           uint16 = 0x0041
	LogTypeCompensation    uint16 = 0x0043
	LogTypeInformational   uint16 = 0x0069
	LogTypeSubtransaction  uint16 = 0x0046
	LogTypeNormal          uint16 = 0x004E
	LogTypeCommit          uint16 = 0x0084
	LogTypeMPPSubCommit    uint16 = 0x0085
	LogTypeMPPCoordCommit  uint16 = 0x0086
	LogTypeHeuristicCommit uint16 = 0x0087
	LogTypeHeuristicAbort  uint16 = 0x0049
)

const (
	LogFlagPropagatable uint16 = 0x0002
)

const (
	// Db2 documents lrIUDflags bit 0x8000 on the delete/insert records that
	// together implement a decomposed update (for example when a row moves to
	// another partition/storage location).  Treat the pair as one logical UPDATE
	// in the CDC stream rather than exposing a transient DELETE+INSERT.
	IUDFlagDecomposedUpdate uint16 = 0x8000
)

const (
	DMSInitializeTable byte = 128
	DMSUndoInsert      byte = 110
	DMSUndoDelete      byte = 111
	DMSUndoUpdate      byte = 112
	DMSInsert          byte = 162
	DMSDelete          byte = 161
	DMSUpdate          byte = 163
	DMSDeleteEmpty     byte = 164
	DMSInsertEmpty     byte = 165
	DMSUndoDeleteEmpty byte = 166
	DMSUndoInsertEmpty byte = 131
	DMSMultiInsert     byte = 167
	DMSUndoMultiInsert byte = 168
	DMSStartOutOfRow   byte = 211
	DMSUndoOutOfRow    byte = 212
	DMSVectorData      byte = 213
)

// Field types in the documented Db2 table description embedded in an
// Initialize Table propagatable log record.
const (
	FieldSmallInt       uint16 = 0x0000
	FieldInteger        uint16 = 0x0001
	FieldDecimal        uint16 = 0x0002
	FieldDouble         uint16 = 0x0003
	FieldReal           uint16 = 0x0004
	FieldBigInt         uint16 = 0x0005
	FieldDecFloat64     uint16 = 0x0006
	FieldDecFloat128    uint16 = 0x0007
	FieldChar           uint16 = 0x0100
	FieldVarchar        uint16 = 0x0101
	FieldLongVarchar    uint16 = 0x0104
	FieldDate           uint16 = 0x0105
	FieldTime           uint16 = 0x0106
	FieldTimestamp      uint16 = 0x0107
	FieldBlob           uint16 = 0x0108
	FieldClob           uint16 = 0x0109
	FieldStruct         uint16 = 0x010D
	FieldBoolean        uint16 = 0x010F
	FieldBinary         uint16 = 0x0110
	FieldVarbinary      uint16 = 0x0111
	FieldXML            uint16 = 0x0112
	FieldGraphic        uint16 = 0x0200
	FieldVargraphic     uint16 = 0x0201
	FieldLongVargraphic uint16 = 0x0202
	FieldDBClob         uint16 = 0x0203
)

const (
	FieldFlagIsNull                   uint16 = 0x0001
	FieldFlagNoNulls                  uint16 = 0x0002
	FieldFlagSystemDefaultCompression uint16 = 0x0080
)

type ByteOrder string

const (
	LittleEndian ByteOrder = "little"
	BigEndian    ByteOrder = "big"
)

func orderOf(v string) (binary.ByteOrder, error) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "little", "le", "little-endian", "":
		return binary.LittleEndian, nil
	case "big", "be", "big-endian":
		return binary.BigEndian, nil
	default:
		return nil, fmt.Errorf("unsupported DB2 log byte order %q", v)
	}
}

type DescriptorField struct {
	Type   uint16 `json:"type"`
	Length uint16 `json:"length"`
	Flags  uint16 `json:"flags"`
	Offset uint16 `json:"offset"`
}
type TableDescriptor struct {
	Fields []DescriptorField `json:"fields"`
}

type Selection struct {
	Schema       string              `json:"schema"`
	Table        string              `json:"table"`
	TablespaceID uint16              `json:"tablespace_id"`
	TableID      uint16              `json:"table_id"`
	Columns      []domain.ColumnInfo `json:"columns"`
	PrimaryKeys  []string            `json:"primary_keys"`
}

func (s Selection) Key() uint32 { return uint32(s.TablespaceID)<<16 | uint32(s.TableID) }

type ParsedRecord struct {
	Function     byte
	TablespaceID uint16
	TableID      uint16
	IUDFlags     uint16
	RID          string
	NewRID       string
	RowOuterType byte
	Descriptor   *TableDescriptor
	Before       []domain.CDCField
	After        []domain.CDCField
	Rows         []ParsedRow
	// AfterInPrecedingInsert is set for the documented UPDATE layout whose new
	// record outer type has bit 0x02.  Db2 states that this after-image is in the
	// preceding INSERT log record for the transaction and that the INSERT row has
	// bit 0x04 set in its outer record type.
	AfterInPrecedingInsert bool
	UnsafeUndo             bool
	OutOfRowStart          bool
}

// ParsedRow is one logical row mutation inside a DMS record. Most DMS records
// contain exactly one row; DMSMultiInsert contains multiple row descriptions in
// one log record and is expanded into one QMigration CDC event per row.
type ParsedRow struct {
	RID    string
	Before []domain.CDCField
	After  []domain.CDCField
}

type LOBManagerRecord struct {
	OperationType      byte
	TablespaceID       uint16
	ObjectID           uint16
	ParentTablespaceID uint16
	ParentTableID      uint16
	Length             uint32
	ByteOffset         uint64
	OriginalOperation  byte
	ColumnID           uint16
	Data               []byte
	AmountOnly         bool
	InformationOnly    bool
}

// XMLManagerRecord is the documented Db2 11.5.8+ CSL manager serialized XML
// record. Multiple records for the same column are concatenated in log order
// before the following DMS row image is decoded.
type XMLManagerRecord struct {
	OperationType      byte
	TablespaceID       uint16
	ObjectID           uint16
	ParentTablespaceID uint16
	ParentTableID      uint16
	ObjectType         byte
	ColumnID           uint16
	Data               []byte
	LRI                LRI
}

// VectorManagerRecord is the documented Db2 12.1.4+ DMS function 213
// serialized VECTOR value. Db2 logs the human-readable representation before
// the following INSERT/UPDATE row so replication consumers do not need to
// reverse-engineer the internal binary VECTOR storage.
type VectorManagerRecord struct {
	TablespaceID uint16
	TableID      uint16
	ColumnID     uint16
	Data         []byte
	LRI          LRI
}

const (
	CSLComponentXML  byte = 15
	CSLXMLSerialized byte = 114
	CSLObjectTypeXML byte = 6
)

const (
	LOBAddData            byte   = 64
	LOBAddAmount          byte   = 65
	LOBDeleteInfo         byte   = 66
	LOBNonUpdateInfo      byte   = 67
	LOBColumnConsolidated uint16 = 0xffff
)

const MaxMultiInsertRows = 100000

func decodeEnvelopeRaw(e RecordEnvelope) ([]byte, error) {
	b, err := base64.StdEncoding.DecodeString(strings.TrimSpace(e.RawBase64))
	if err != nil {
		return nil, fmt.Errorf("decode DB2 raw log at %s: %w", e.LRI.String(), err)
	}
	if len(b) < 40 {
		return nil, fmt.Errorf("DB2 raw log at %s shorter than 40-byte manager header", e.LRI.String())
	}
	bo, err := orderOf(e.ByteOrder)
	if err != nil {
		return nil, err
	}
	declared := int(bo.Uint32(b[0:4]))
	if declared != len(b) {
		return nil, fmt.Errorf("DB2 raw log at %s length mismatch: envelope=%d header=%d", e.LRI.String(), len(b), declared)
	}
	rawType := bo.Uint16(b[4:6])
	if rawType != e.LogType {
		return nil, fmt.Errorf("DB2 raw log at %s type mismatch: envelope=0x%04x header=0x%04x", e.LRI.String(), e.LogType, rawType)
	}
	rawFlags := bo.Uint16(b[6:8])
	if rawFlags != e.Flags {
		return nil, fmt.Errorf("DB2 raw log at %s flags mismatch: envelope=0x%04x header=0x%04x", e.LRI.String(), e.Flags, rawFlags)
	}
	wantTID, err := NormalizeTID(e.TID)
	if err != nil {
		return nil, fmt.Errorf("DB2 raw log at %s has invalid envelope TID: %w", e.LRI.String(), err)
	}
	rawTID := hex.EncodeToString(b[32:38])
	if rawTID != wantTID {
		return nil, fmt.Errorf("DB2 raw log at %s TID mismatch: envelope=%s header=%s", e.LRI.String(), wantTID, rawTID)
	}
	return b, nil
}

// logManagerHeaderLength returns the documented Db2 log-manager header size.
// Normal records use 40 bytes. Compensation records add an extra LSO and use
// 56 bytes; propagatable compensation records add a second LSO and use 64.
func logManagerHeaderLength(e RecordEnvelope, raw []byte) (int, error) {
	if e.LogType != LogTypeCompensation {
		if len(raw) < 40 {
			return 0, errors.New("DB2 log record is truncated before basic manager header")
		}
		return 40, nil
	}
	if e.Flags&LogFlagPropagatable != 0 {
		if len(raw) < 64 {
			return 0, errors.New("DB2 propagatable compensation record is truncated before 64-byte manager header")
		}
		return 64, nil
	}
	if len(raw) < 56 {
		return 0, errors.New("DB2 compensation record is truncated before 56-byte manager header")
	}
	return 56, nil
}

func logPayload(e RecordEnvelope, raw []byte) ([]byte, error) {
	n, err := logManagerHeaderLength(e, raw)
	if err != nil {
		return nil, err
	}
	return raw[n:], nil
}

func ridHex(b []byte) (string, error) {
	if len(b) < 6 {
		return "", errors.New("DB2 RID is truncated")
	}
	return hex.EncodeToString(b[:6]), nil
}

func rowOuterTypeAt(dms []byte, pos int, bo binary.ByteOrder) (byte, int, error) {
	if pos < 0 || pos+4 > len(dms) {
		return 0, pos, errors.New("DB2 row header truncated")
	}
	typ := dms[pos]
	recLen := int(bo.Uint16(dms[pos+2 : pos+4]))
	if recLen < 4 || pos+recLen > len(dms) {
		return 0, pos, fmt.Errorf("DB2 row length %d exceeds log record", recLen)
	}
	return typ, pos + recLen, nil
}

// ParseDataManager parses the documented propagatable Data Manager records
// QMigration needs for row CDC. Unsupported/ambiguous formats fail closed.
func ParseDataManager(e RecordEnvelope, selection *Selection, desc *TableDescriptor) (*ParsedRecord, error) {
	return ParseDataManagerWithLOB(e, selection, desc, nil)
}

func ParseDataManagerWithLOB(e RecordEnvelope, selection *Selection, desc *TableDescriptor, outOfRow map[int][]byte) (*ParsedRecord, error) {
	if e.LogType != LogTypeNormal && e.LogType != LogTypeCompensation {
		return nil, nil
	}
	raw, err := decodeEnvelopeRaw(e)
	if err != nil {
		return nil, err
	}
	bo, err := orderOf(e.ByteOrder)
	if err != nil {
		return nil, err
	}
	dms, err := logPayload(e, raw)
	if err != nil {
		return nil, err
	}
	if len(dms) < 6 || dms[0] != 1 {
		return nil, nil
	}
	fn := dms[1]
	ts := bo.Uint16(dms[2:4])
	tab := bo.Uint16(dms[4:6])
	out := &ParsedRecord{Function: fn, TablespaceID: ts, TableID: tab}
	if fn == DMSInitializeTable {
		if len(dms) < 92 {
			return nil, errors.New("DB2 Initialize Table log record is truncated before table description")
		}
		n := int(bo.Uint32(dms[88:92]))
		if n <= 0 || 92+n > len(dms) {
			return nil, fmt.Errorf("DB2 Initialize Table descriptor length %d exceeds record", n)
		}
		td, err := ParseTableDescriptor(dms[92:92+n], bo)
		if err != nil {
			return nil, err
		}
		out.Descriptor = td
		return out, nil
	}
	if selection == nil || desc == nil {
		return out, nil
	}
	if selection.TablespaceID != ts || selection.TableID != tab {
		return out, nil
	}
	switch fn {
	case DMSInsert:
		if len(dms) < 18 {
			return nil, errors.New("DB2 insert record is truncated before RID")
		}
		out.RID, err = ridHex(dms[12:18])
		if err != nil {
			return nil, err
		}
		out.IUDFlags = bo.Uint16(dms[6:8])
		out.RowOuterType, _, err = rowOuterTypeAt(dms, 20, bo)
		if err != nil {
			return nil, err
		}
		row, _, err := parseRowAtWithLOB(dms, 20, desc, selection.Columns, bo, outOfRow, false)
		if err != nil {
			return nil, err
		}
		out.After = row
		out.Rows = []ParsedRow{{RID: out.RID, After: row}}
	case DMSInsertEmpty:
		if len(dms) < 26 {
			return nil, errors.New("DB2 insert-to-empty-page record is truncated")
		}
		out.RID, err = ridHex(dms[12:18])
		if err != nil {
			return nil, err
		}
		out.IUDFlags = bo.Uint16(dms[6:8])
		out.RowOuterType, _, err = rowOuterTypeAt(dms, 26, bo)
		if err != nil {
			return nil, err
		}
		row, _, err := parseRowAtWithLOB(dms, 26, desc, selection.Columns, bo, outOfRow, false)
		if err != nil {
			return nil, err
		}
		out.After = row
		out.Rows = []ParsedRow{{RID: out.RID, After: row}}
	case DMSDelete:
		if len(dms) < 18 {
			return nil, errors.New("DB2 delete record is truncated before RID")
		}
		out.RID, err = ridHex(dms[12:18])
		if err != nil {
			return nil, err
		}
		out.IUDFlags = bo.Uint16(dms[6:8])
		out.RowOuterType, _, err = rowOuterTypeAt(dms, 20, bo)
		if err != nil {
			return nil, err
		}
		row, _, err := parseRowAtWithLOB(dms, 20, desc, selection.Columns, bo, nil, true)
		if err != nil {
			return nil, err
		}
		out.Before = row
		out.Rows = []ParsedRow{{RID: out.RID, Before: row}}
	case DMSDeleteEmpty:
		if len(dms) < 26 {
			return nil, errors.New("DB2 delete-to-empty-page record is truncated")
		}
		out.RID, err = ridHex(dms[12:18])
		if err != nil {
			return nil, err
		}
		out.RowOuterType, _, err = rowOuterTypeAt(dms, 26, bo)
		if err != nil {
			return nil, err
		}
		row, _, err := parseRowAtWithLOB(dms, 26, desc, selection.Columns, bo, nil, true)
		if err != nil {
			return nil, err
		}
		out.Before = row
		out.Rows = []ParsedRow{{RID: out.RID, Before: row}}
	case DMSUpdate:
		if len(dms) < 18 {
			return nil, errors.New("DB2 update record is truncated before RID")
		}
		out.RID, err = ridHex(dms[12:18])
		if err != nil {
			return nil, err
		}
		out.IUDFlags = bo.Uint16(dms[6:8])
		old, end, err := parseRowAtWithLOB(dms, 20, desc, selection.Columns, bo, nil, true)
		if err != nil {
			return nil, err
		}
		out.Before = old
		// An update carries the new row after a second Data Manager header.
		pos := end
		for pos+6 <= len(dms) && !(dms[pos] == 1 && dms[pos+1] == DMSUpdate) {
			pos++
		}
		if pos+24 > len(dms) {
			return nil, errors.New("DB2 update log does not contain a decodable second row image")
		}
		out.NewRID, err = ridHex(dms[pos+12 : pos+18])
		if err != nil {
			return nil, err
		}
		newType, _, err := rowOuterTypeAt(dms[pos:], 20, bo)
		if err != nil {
			return nil, err
		}
		out.RowOuterType = newType
		if newType&0x02 != 0 && newType&0x04 == 0 {
			out.AfterInPrecedingInsert = true
			out.Rows = []ParsedRow{{RID: out.RID, Before: old}}
			return out, nil
		}
		if newType&0x02 != 0 && newType&0x04 != 0 {
			return nil, fmt.Errorf("DB2 update new row outer type 0x%02x ambiguously sets both indirect 0x02 and complete-record 0x04 bits", newType)
		}
		nw, _, err := parseRowAtWithLOB(dms[pos:], 20, desc, selection.Columns, bo, outOfRow, false)
		if err != nil {
			return nil, err
		}
		out.After = nw
		out.Rows = []ParsedRow{{RID: out.RID, Before: old, After: nw}}
	case DMSUndoInsert:
		out.UnsafeUndo = true
		if len(dms) < 18 {
			return nil, errors.New("DB2 undo-insert record is truncated before RID")
		}
		out.RID, err = ridHex(dms[12:18])
		if err != nil {
			return nil, err
		}
		out.Rows = []ParsedRow{{RID: out.RID}}
	case DMSUndoInsertEmpty:
		out.UnsafeUndo = true
		if len(dms) < 18 {
			return nil, errors.New("DB2 undo-insert-empty-page record is truncated before RID")
		}
		out.RID, err = ridHex(dms[12:18])
		if err != nil {
			return nil, err
		}
		out.Rows = []ParsedRow{{RID: out.RID}}
	case DMSUndoDelete, DMSUndoUpdate:
		// A rollback delete/update compensation only needs the RID to cancel the
		// matching buffered source mutation. Decoding the compensation row image
		// would add an unnecessary format dependency and can reject a rollback we
		// can otherwise reconstruct losslessly.
		out.UnsafeUndo = true
		if len(dms) < 18 {
			return nil, errors.New("DB2 undo delete/update record is truncated before RID")
		}
		out.RID, err = ridHex(dms[12:18])
		if err != nil {
			return nil, err
		}
		out.Rows = []ParsedRow{{RID: out.RID}}
	case DMSUndoDeleteEmpty:
		out.UnsafeUndo = true
		if len(dms) < 18 {
			return nil, errors.New("DB2 undo-delete-empty-page record is truncated before RID")
		}
		out.RID, err = ridHex(dms[12:18])
		if err != nil {
			return nil, err
		}
		out.Rows = []ParsedRow{{RID: out.RID}}
	case DMSUndoOutOfRow:
		// Undo-start-of-out-of-row has no row payload. The reader clears any
		// still-pending LOB group and leaves already-materialized row undo to the
		// corresponding DMS undo record.
		out.UnsafeUndo = false
	case DMSMultiInsert:
		rows, err := parseMultiInsertRowsWithLOB(dms, desc, selection.Columns, bo, outOfRow)
		if err != nil {
			return nil, err
		}
		out.Rows = rows
		if len(rows) == 1 {
			out.RID, out.After = rows[0].RID, rows[0].After
		}
	case DMSUndoMultiInsert:
		out.UnsafeUndo = true
		rows, err := parseUndoMultiInsertRows(dms, bo)
		if err != nil {
			return nil, err
		}
		out.Rows = rows
	case DMSStartOutOfRow:
		out.OutOfRowStart = true
	default:
		return nil, fmt.Errorf("unsupported selected-table DB2 Data Manager function %d", fn)
	}
	return out, nil
}

func parseMultiInsertRows(dms []byte, td *TableDescriptor, cols []domain.ColumnInfo, bo binary.ByteOrder) ([]ParsedRow, error) {
	return parseMultiInsertRowsWithLOB(dms, td, cols, bo, nil)
}

// parseMultiInsertRowsWithLOB reconstructs DMS 167 while preserving the only
// row association QMigration can prove from the documented format. The multi-
// insert record has a RID and row image per row, but LOB/XML/VECTOR manager
// records carry table/column identity rather than a multi-insert row ordinal.
// Therefore one pending out-of-row group can be assigned only when exactly one
// row image declares external data. Multiple external rows are ambiguous and
// must fail closed until a retained real-Db2 trace proves an association rule.
func parseMultiInsertRowsWithLOB(dms []byte, td *TableDescriptor, cols []domain.ColumnInfo, bo binary.ByteOrder, outOfRow map[int][]byte) ([]ParsedRow, error) {
	if len(dms) < 20 {
		return nil, errors.New("DB2 multi-insert record is truncated before row descriptions")
	}
	n := int(bo.Uint16(dms[8:10]))
	if n <= 0 || n > MaxMultiInsertRows {
		return nil, fmt.Errorf("DB2 multi-insert row count %d is outside safety bound 1..%d", n, MaxMultiInsertRows)
	}
	wantRowBytes := int(bo.Uint16(dms[12:14]))
	variableBytes := int(bo.Uint16(dms[14:16]))
	if variableBytes != 0 && 20+variableBytes > len(dms) {
		return nil, fmt.Errorf("DB2 multi-insert variable part length %d exceeds record", variableBytes)
	}
	type rowPlan struct {
		rid      string
		pos      int
		end      int
		requires map[int]bool
	}
	plans := make([]rowPlan, 0, n)
	pos := 20
	rowBytes := 0
	externalRows := 0
	externalIndex := -1
	for i := 0; i < n; i++ {
		if pos+12 > len(dms) {
			return nil, fmt.Errorf("DB2 multi-insert row %d description is truncated", i)
		}
		rid, err := ridHex(dms[pos : pos+6])
		if err != nil {
			return nil, err
		}
		requires, end, err := rowExternalColumns(dms, pos+8, td, cols, bo)
		if err != nil {
			return nil, fmt.Errorf("DB2 multi-insert row %d: %w", i, err)
		}
		rowBytes += end - (pos + 8)
		plans = append(plans, rowPlan{rid: rid, pos: pos + 8, end: end, requires: requires})
		if len(requires) > 0 {
			externalRows++
			externalIndex = i
		}
		pos = end
	}
	if wantRowBytes != 0 && rowBytes != wantRowBytes {
		return nil, fmt.Errorf("DB2 multi-insert row-length sum mismatch: header=%d decoded=%d", wantRowBytes, rowBytes)
	}
	if variableBytes != 0 && pos != 20+variableBytes {
		return nil, fmt.Errorf("DB2 multi-insert variable part mismatch: header end=%d decoded end=%d", 20+variableBytes, pos)
	}
	if externalRows > 1 {
		return nil, fmt.Errorf("DB2 multi-insert has %d rows requiring out-of-row data; documented manager records have no row ordinal, so reconstruction is ambiguous", externalRows)
	}
	if externalRows == 1 && len(outOfRow) == 0 {
		return nil, fmt.Errorf("DB2 multi-insert row %d requires out-of-row data but no matching group was reconstructed", externalIndex)
	}
	if externalRows == 0 && len(outOfRow) > 0 {
		return nil, errors.New("DB2 multi-insert has pending out-of-row data but no row image declares an external value")
	}
	rows := make([]ParsedRow, 0, n)
	for i, plan := range plans {
		values := map[int][]byte(nil)
		if i == externalIndex {
			for col := range outOfRow {
				if !plan.requires[col] {
					return nil, fmt.Errorf("DB2 multi-insert out-of-row column %d is not declared external by row %d", col, i)
				}
			}
			values = outOfRow
		}
		row, end, err := parseRowAtWithLOB(dms, plan.pos, td, cols, bo, values, false)
		if err != nil {
			return nil, fmt.Errorf("DB2 multi-insert row %d: %w", i, err)
		}
		if end != plan.end {
			return nil, fmt.Errorf("DB2 multi-insert row %d changed decoded boundary from %d to %d", i, plan.end, end)
		}
		rows = append(rows, ParsedRow{RID: plan.rid, After: row})
	}
	return rows, nil
}

// rowExternalColumns returns the catalog ordinals whose values are represented
// outside the DMS row. It understands ordinary rows, VALUE COMPRESSION rows,
// and VECTOR columns whose serialized value is supplied by DMS function 213.
func rowExternalColumns(dms []byte, pos int, td *TableDescriptor, cols []domain.ColumnInfo, bo binary.ByteOrder) (map[int]bool, int, error) {
	if pos < 0 || pos+4 > len(dms) {
		return nil, pos, errors.New("DB2 row header truncated")
	}
	typ := dms[pos]
	recLen := int(bo.Uint16(dms[pos+2 : pos+4]))
	if recLen < 4 || pos+recLen > len(dms) {
		return nil, pos, fmt.Errorf("DB2 row length %d exceeds log record", recLen)
	}
	if len(td.Fields) != len(cols) {
		return nil, pos, fmt.Errorf("DB2 descriptor/catalog column count mismatch: descriptor=%d catalog=%d", len(td.Fields), len(cols))
	}
	data := dms[pos+4 : pos+recLen]
	if typ != 0 && typ != 0x10 && typ&0x04 == 0 {
		return nil, pos, fmt.Errorf("DB2 row outer record type 0x%02x is not a complete user image", typ)
	}
	out := map[int]bool{}
	if typ&0x04 != 0 {
		if len(data) < 4 {
			return nil, pos, errors.New("DB2 complete user row is truncated before inner header")
		}
		if data[0]&0x02 != 0 {
			n := int(bo.Uint16(data[2:4]))
			if n != len(td.Fields) || n != len(cols) {
				return nil, pos, fmt.Errorf("DB2 VALUE COMPRESSION partial row is not qualified: row columns=%d descriptor=%d catalog=%d", n, len(td.Fields), len(cols))
			}
			arrBytes := (n + 1) * 2
			if len(data) < 4+arrBytes {
				return nil, pos, errors.New("DB2 VALUE COMPRESSION offset array is truncated")
			}
			formatted := data[4:]
			offs := make([]uint16, n+1)
			for i := range offs {
				offs[i] = bo.Uint16(formatted[i*2 : i*2+2])
			}
			base := 0
			first := int(offs[0] & 0x7fff)
			switch first {
			case 0:
			case arrBytes:
				base = arrBytes
			default:
				return nil, pos, fmt.Errorf("DB2 VALUE COMPRESSION first offset %d is neither data-relative nor formatted-relative", first)
			}
			dataPart := formatted[arrBytes:]
			resolved := make([]int, n+1)
			for i, raw := range offs {
				o := int(raw&0x7fff) - base
				if o < 0 || o > len(dataPart) || (i > 0 && o < resolved[i-1]) {
					return nil, pos, errors.New("DB2 VALUE COMPRESSION offsets are invalid")
				}
				resolved[i] = o
			}
			for i := 0; i < n; i++ {
				spanLen := resolved[i+1] - resolved[i]
				if isVectorColumn(cols[i]) {
					if offs[i]&0x8000 == 0 {
						out[i] = true
					} else if spanLen == 1 && dataPart[resolved[i]] == 0x01 {
						// NULL VECTOR has no DMS 213 record.
					} else {
						out[i] = true
					}
					continue
				}
				if offs[i]&0x8000 != 0 && isVariableField(td.Fields[i].Type) && spanLen == 24 {
					out[i] = true
				}
			}
			return out, pos + recLen, nil
		}
		if data[0]&0x01 == 0 {
			return nil, pos, fmt.Errorf("unsupported DB2 complete user row inner type 0x%02x", data[0])
		}
		data = data[4:]
	}
	for i, f := range td.Fields {
		if isVectorColumn(cols[i]) {
			null, err := fixedRowFieldNull(data, f)
			if err != nil {
				return nil, pos, fmt.Errorf("DB2 VECTOR column %s: %w", cols[i].Name, err)
			}
			if !null {
				out[i] = true
			}
			continue
		}
		if fieldUsesOutOfRow(data, f, bo) {
			out[i] = true
		}
	}
	return out, pos + recLen, nil
}

func parseUndoMultiInsertRows(dms []byte, bo binary.ByteOrder) ([]ParsedRow, error) {
	if len(dms) < 20 {
		return nil, errors.New("DB2 undo-multi-insert record is truncated before rollback descriptions")
	}
	n := int(bo.Uint16(dms[8:10]))
	if n <= 0 || n > MaxMultiInsertRows {
		return nil, fmt.Errorf("DB2 undo-multi-insert row count %d is outside safety bound 1..%d", n, MaxMultiInsertRows)
	}
	need := 20 + n*8
	if need > len(dms) {
		return nil, fmt.Errorf("DB2 undo-multi-insert rollback descriptions need %d bytes, record has %d", need, len(dms))
	}
	rows := make([]ParsedRow, 0, n)
	for pos, i := 20, 0; i < n; i, pos = i+1, pos+8 {
		rid, err := ridHex(dms[pos : pos+6])
		if err != nil {
			return nil, err
		}
		rows = append(rows, ParsedRow{RID: rid})
	}
	return rows, nil
}

func ParseTableDescriptor(b []byte, bo binary.ByteOrder) (*TableDescriptor, error) {
	if len(b) < 4 {
		return nil, errors.New("DB2 table descriptor too short")
	}
	if b[0] != 0 {
		return nil, fmt.Errorf("unsupported DB2 table descriptor record type 0x%02x", b[0])
	}
	n := int(bo.Uint16(b[2:4]))
	if n <= 0 || 4+n*8 > len(b) {
		return nil, fmt.Errorf("DB2 table descriptor column count %d exceeds buffer", n)
	}
	td := &TableDescriptor{Fields: make([]DescriptorField, 0, n)}
	off := 4
	for i := 0; i < n; i++ {
		td.Fields = append(td.Fields, DescriptorField{Type: bo.Uint16(b[off : off+2]), Length: bo.Uint16(b[off+2 : off+4]), Flags: bo.Uint16(b[off+4 : off+6]), Offset: bo.Uint16(b[off+6 : off+8])})
		off += 8
	}
	return td, nil
}

func parseRowAt(dms []byte, pos int, td *TableDescriptor, cols []domain.ColumnInfo, bo binary.ByteOrder) ([]domain.CDCField, int, error) {
	return parseRowAtWithLOB(dms, pos, td, cols, bo, nil, false)
}

func parseRowAtWithLOB(dms []byte, pos int, td *TableDescriptor, cols []domain.ColumnInfo, bo binary.ByteOrder, outOfRow map[int][]byte, allowMissingOutOfRow bool) ([]domain.CDCField, int, error) {
	if pos < 0 || pos+4 > len(dms) {
		return nil, pos, errors.New("DB2 row header truncated")
	}
	typ := dms[pos]
	recLen := int(bo.Uint16(dms[pos+2 : pos+4]))
	if recLen < 4 || pos+recLen > len(dms) {
		return nil, pos, fmt.Errorf("DB2 row length %d exceeds log record", recLen)
	}
	data := dms[pos+4 : pos+recLen]
	if len(td.Fields) != len(cols) {
		return nil, pos, fmt.Errorf("DB2 descriptor/catalog column count mismatch: descriptor=%d catalog=%d", len(td.Fields), len(cols))
	}
	// IBM documents a complete user row as outer type 0/0x10, or an outer
	// record with bit 0x04 set and a qualified inner complete-record type. An
	// outer 0x02 row used by some update relocation/decomposition paths is not
	// itself a complete after-image; decoding it positionally would corrupt CDC.
	if typ != 0 && typ != 0x10 && typ&0x04 == 0 {
		return nil, pos, fmt.Errorf("DB2 row outer record type 0x%02x is not a complete user image", typ)
	}

	// Complete user records can carry a second (inner) record header.  For a
	// VALUE COMPRESSION row the outer 0x04 bit is set and the inner record
	// type has bit 0x02.  IBM documents that format as column-count + offset
	// array + raw column data.  Keep the old direct formatted-record path for
	// outer record types 0/0x10 and accept the inner 0x01 uncompressed form.
	if typ&0x04 != 0 {
		if len(data) < 4 {
			return nil, pos, errors.New("DB2 complete user row is truncated before inner header")
		}
		switch {
		case data[0]&0x02 != 0:
			row, err := decodeValueCompressedRowWithLOB(data, td, cols, bo, outOfRow, allowMissingOutOfRow)
			return row, pos + recLen, err
		case data[0]&0x01 != 0:
			row, err := decodeDescriptorRow(data[4:], td, cols, bo, outOfRow, allowMissingOutOfRow)
			return row, pos + recLen, err
		default:
			return nil, pos, fmt.Errorf("unsupported DB2 complete user row inner type 0x%02x", data[0])
		}
	}
	row, err := decodeDescriptorRow(data, td, cols, bo, outOfRow, allowMissingOutOfRow)
	return row, pos + recLen, err
}

func decodeDescriptorRow(data []byte, td *TableDescriptor, cols []domain.ColumnInfo, bo binary.ByteOrder, outOfRow map[int][]byte, allowMissingOutOfRow bool) ([]domain.CDCField, error) {
	row := make([]domain.CDCField, 0, len(cols))
	for i, f := range td.Fields {
		if isVectorColumn(cols[i]) {
			if raw, ok := outOfRow[i]; ok {
				row = append(row, makeCDCField(cols[i].Name, raw, false, false))
				continue
			}
			null, err := fixedRowFieldNull(data, f)
			if err != nil {
				return nil, fmt.Errorf("DB2 VECTOR column %s: %w", cols[i].Name, err)
			}
			if null {
				row = append(row, domain.CDCField{Column: cols[i].Name, Null: true})
				continue
			}
			if allowMissingOutOfRow {
				continue
			}
			return nil, fmt.Errorf("DB2 VECTOR column %s has no matching function-213 serialized value", cols[i].Name)
		}
		if fieldUsesOutOfRow(data, f, bo) {
			if raw, ok := outOfRow[i]; ok {
				v, binaryValue, err := decodeExternalValue(raw, f)
				if err != nil {
					return nil, fmt.Errorf("DB2 column %s: %w", cols[i].Name, err)
				}
				row = append(row, makeCDCField(cols[i].Name, v, false, binaryValue))
				continue
			}
			if allowMissingOutOfRow {
				continue
			}
			return nil, fmt.Errorf("DB2 column %s uses out-of-row storage but no matching LOB data was reconstructed", cols[i].Name)
		}
		v, null, binaryValue, err := decodeField(data, f, bo)
		if err != nil {
			return nil, fmt.Errorf("DB2 column %s: %w", cols[i].Name, err)
		}
		row = append(row, makeCDCField(cols[i].Name, v, null, binaryValue))
	}
	return row, nil
}

func makeCDCField(name string, v []byte, null, binaryValue bool) domain.CDCField {
	cf := domain.CDCField{Column: name, Null: null}
	if null {
		return cf
	}
	if binaryValue {
		cf.Value = base64.StdEncoding.EncodeToString(v)
		cf.Encoding = "base64"
	} else {
		cf.Value = string(v)
	}
	return cf
}

func isVariableField(t uint16) bool {
	switch t {
	case FieldVarchar, FieldLongVarchar, FieldBlob, FieldClob, FieldVarbinary, FieldXML, FieldVargraphic, FieldLongVargraphic, FieldDBClob:
		return true
	default:
		return false
	}
}

func decodeValueCompressedRow(inner []byte, td *TableDescriptor, cols []domain.ColumnInfo, bo binary.ByteOrder) ([]domain.CDCField, error) {
	return decodeValueCompressedRowWithLOB(inner, td, cols, bo, nil, false)
}

func decodeValueCompressedRowWithLOB(inner []byte, td *TableDescriptor, cols []domain.ColumnInfo, bo binary.ByteOrder, outOfRow map[int][]byte, allowMissingOutOfRow bool) ([]domain.CDCField, error) {
	if len(inner) < 4 {
		return nil, errors.New("DB2 VALUE COMPRESSION inner row header is truncated")
	}
	n := int(bo.Uint16(inner[2:4]))
	// QMigration target apply requires full after-images.  The documented
	// format can describe fewer columns, but until column-id mapping for that
	// partial form is qualified we refuse it instead of guessing positions.
	if n != len(td.Fields) || n != len(cols) {
		return nil, fmt.Errorf("DB2 VALUE COMPRESSION partial row is not qualified: row columns=%d descriptor=%d catalog=%d", n, len(td.Fields), len(cols))
	}
	arrBytes := (n + 1) * 2
	if len(inner) < 4+arrBytes {
		return nil, errors.New("DB2 VALUE COMPRESSION offset array is truncated")
	}
	formatted := inner[4:]
	offRaw := make([]uint16, n+1)
	for i := range offRaw {
		offRaw[i] = bo.Uint16(formatted[i*2 : i*2+2])
	}
	// Db2 documentation describes offsets to the data portion.  Some record
	// descriptions phrase them relative to the formatted record, so accept
	// both unambiguously: first offset 0 => data-relative; first offset equal
	// to offset-array size => formatted-record-relative.  Anything else is
	// rejected rather than heuristically shifted.
	base := 0
	first := int(offRaw[0] & 0x7fff)
	switch first {
	case 0:
		base = 0
	case arrBytes:
		base = arrBytes
	default:
		return nil, fmt.Errorf("DB2 VALUE COMPRESSION first offset %d is neither data-relative nor formatted-relative", first)
	}
	dataPart := formatted[arrBytes:]
	masked := make([]int, n+1)
	for i, raw := range offRaw {
		o := int(raw&0x7fff) - base
		if o < 0 || o > len(dataPart) {
			return nil, fmt.Errorf("DB2 VALUE COMPRESSION offset %d resolves outside %d-byte data portion", int(raw&0x7fff), len(dataPart))
		}
		masked[i] = o
		if i > 0 && masked[i] < masked[i-1] {
			return nil, errors.New("DB2 VALUE COMPRESSION offsets are not monotonic")
		}
	}
	row := make([]domain.CDCField, 0, n)
	for i := 0; i < n; i++ {
		start, end := masked[i], masked[i+1]
		if end < start || end > len(dataPart) {
			return nil, fmt.Errorf("DB2 VALUE COMPRESSION column %s span %d:%d is invalid", cols[i].Name, start, end)
		}
		span := dataPart[start:end]
		flagged := offRaw[i]&0x8000 != 0
		if isVectorColumn(cols[i]) {
			if raw, ok := outOfRow[i]; ok {
				row = append(row, makeCDCField(cols[i].Name, raw, false, false))
				continue
			}
			if flagged && len(span) == 1 && span[0] == 0x01 {
				row = append(row, domain.CDCField{Column: cols[i].Name, Null: true})
				continue
			}
			if allowMissingOutOfRow {
				continue
			}
			return nil, fmt.Errorf("DB2 VECTOR column %s has no matching function-213 serialized value", cols[i].Name)
		}
		if flagged && isVariableField(td.Fields[i].Type) && len(span) == 24 {
			if raw, ok := outOfRow[i]; ok {
				v, binaryValue, err := decodeExternalValue(raw, td.Fields[i])
				if err != nil {
					return nil, fmt.Errorf("DB2 column %s: %w", cols[i].Name, err)
				}
				row = append(row, makeCDCField(cols[i].Name, v, false, binaryValue))
				continue
			}
			if allowMissingOutOfRow {
				continue
			}
			return nil, fmt.Errorf("DB2 column %s uses an out-of-row varying-value descriptor; no matching LOB data was reconstructed", cols[i].Name)
		}
		if flagged {
			if len(span) != 1 {
				return nil, fmt.Errorf("DB2 compressed attribute for column %s has %d bytes, expected 1", cols[i].Name, len(span))
			}
			switch span[0] {
			case 0x01:
				row = append(row, domain.CDCField{Column: cols[i].Name, Null: true})
				continue
			case 0x80:
				v, binaryValue, err := compressedSystemDefault(td.Fields[i])
				if err != nil {
					return nil, fmt.Errorf("DB2 column %s: %w", cols[i].Name, err)
				}
				row = append(row, makeCDCField(cols[i].Name, v, false, binaryValue))
				continue
			default:
				return nil, fmt.Errorf("DB2 compressed attribute for column %s has unsupported value 0x%02x", cols[i].Name, span[0])
			}
		}
		v, binaryValue, err := decodeCompressedRegular(span, td.Fields[i], bo)
		if err != nil {
			return nil, fmt.Errorf("DB2 column %s: %w", cols[i].Name, err)
		}
		row = append(row, makeCDCField(cols[i].Name, v, false, binaryValue))
	}
	return row, nil
}

func compressedSystemDefault(f DescriptorField) ([]byte, bool, error) {
	if f.Flags&FieldFlagSystemDefaultCompression == 0 {
		return nil, false, errors.New("compressed system-default attribute is present but descriptor does not enable COMPRESS SYSTEM DEFAULT")
	}
	switch f.Type {
	case FieldSmallInt, FieldInteger, FieldDecimal, FieldDouble, FieldReal, FieldBigInt, FieldDecFloat64, FieldDecFloat128:
		return []byte("0"), false, nil
	case FieldChar, FieldGraphic:
		// Db2's type default is blanks.  QMigration normalizes fixed CHAR/
		// GRAPHIC values by trimming right-padding in the non-compressed path,
		// therefore the equivalent visible value is the empty string.
		return []byte{}, false, nil
	default:
		return nil, false, fmt.Errorf("COMPRESS SYSTEM DEFAULT reconstruction is not defined for field type 0x%04x", f.Type)
	}
}

func decodeCompressedRegular(span []byte, f DescriptorField, bo binary.ByteOrder) ([]byte, bool, error) {
	need := fixedSize(f)
	switch f.Type {
	case FieldSmallInt:
		if len(span) != 2 {
			return nil, false, fmt.Errorf("SMALLINT compressed length=%d", len(span))
		}
		return []byte(fmt.Sprintf("%d", int16(bo.Uint16(span)))), false, nil
	case FieldInteger:
		if len(span) != 4 {
			return nil, false, fmt.Errorf("INTEGER compressed length=%d", len(span))
		}
		return []byte(fmt.Sprintf("%d", int32(bo.Uint32(span)))), false, nil
	case FieldBigInt:
		if len(span) != 8 {
			return nil, false, fmt.Errorf("BIGINT compressed length=%d", len(span))
		}
		return []byte(fmt.Sprintf("%d", int64(bo.Uint64(span)))), false, nil
	case FieldReal:
		if len(span) != 4 {
			return nil, false, fmt.Errorf("REAL compressed length=%d", len(span))
		}
		return []byte(fmt.Sprintf("%.9g", math.Float32frombits(bo.Uint32(span)))), false, nil
	case FieldDouble:
		if len(span) != 8 {
			return nil, false, fmt.Errorf("DOUBLE compressed length=%d", len(span))
		}
		return []byte(fmt.Sprintf("%.17g", math.Float64frombits(bo.Uint64(span)))), false, nil
	case FieldDecimal:
		if len(span) != need {
			return nil, false, fmt.Errorf("DECIMAL compressed length=%d expected=%d", len(span), need)
		}
		s, err := decodePackedDecimal(span, int(f.Length>>8), int(f.Length&0xff))
		return []byte(s), false, err
	case FieldBoolean:
		if len(span) != 1 {
			return nil, false, fmt.Errorf("BOOLEAN compressed length=%d", len(span))
		}
		if span[0] == 0 {
			return []byte("0"), false, nil
		}
		return []byte("1"), false, nil
	case FieldDate:
		if len(span) != 4 {
			return nil, false, fmt.Errorf("DATE compressed length=%d", len(span))
		}
		s, err := decodePackedDigits(span, 8)
		if err != nil {
			return nil, false, err
		}
		return []byte(s[0:4] + "-" + s[4:6] + "-" + s[6:8]), false, nil
	case FieldTime:
		if len(span) != 3 {
			return nil, false, fmt.Errorf("TIME compressed length=%d", len(span))
		}
		s, err := decodePackedDigits(span, 6)
		if err != nil {
			return nil, false, err
		}
		return []byte(s[0:2] + ":" + s[2:4] + ":" + s[4:6]), false, nil
	case FieldTimestamp:
		if len(span) != 10 {
			return nil, false, fmt.Errorf("TIMESTAMP compressed length=%d", len(span))
		}
		s, err := decodePackedDigits(span, 20)
		if err != nil {
			return nil, false, err
		}
		return []byte(s[0:4] + "-" + s[4:6] + "-" + s[6:8] + " " + s[8:10] + ":" + s[10:12] + ":" + s[12:14] + "." + s[14:20]), false, nil
	case FieldChar, FieldGraphic:
		if len(span) != int(f.Length) {
			return nil, false, fmt.Errorf("fixed character compressed length=%d expected=%d", len(span), f.Length)
		}
		if !utf8.Valid(span) {
			return nil, false, errors.New("non-UTF8 DB2 compressed character row requires a qualified codepage decoder")
		}
		return []byte(strings.TrimRight(string(span), " ")), false, nil
	case FieldBinary:
		if len(span) != int(f.Length) {
			return nil, false, fmt.Errorf("BINARY compressed length=%d expected=%d", len(span), f.Length)
		}
		return append([]byte(nil), span...), true, nil
	case FieldVarchar, FieldLongVarchar, FieldBlob, FieldClob, FieldVarbinary, FieldXML, FieldVargraphic, FieldLongVargraphic, FieldDBClob:
		b := append([]byte(nil), span...)
		switch f.Type {
		case FieldBlob, FieldClob, FieldDBClob:
			if len(b) > 0 && b[0] == 0x80 {
				b = b[:0]
			}
			if len(b) >= 4 && b[0] == 0x69 {
				b = b[4:]
			}
		}
		binaryValue := f.Type == FieldBlob || f.Type == FieldVarbinary
		if !binaryValue && !utf8.Valid(b) {
			return nil, false, errors.New("non-UTF8 DB2 compressed variable value requires a qualified codepage decoder")
		}
		return b, binaryValue, nil
	default:
		return nil, false, fmt.Errorf("unsupported DB2 compressed descriptor field type 0x%04x", f.Type)
	}
}

func fieldUsesOutOfRow(data []byte, f DescriptorField, bo binary.ByteOrder) bool {
	if !isVariableField(f.Type) {
		return false
	}
	off := int(f.Offset)
	if off < 0 || off+4 > len(data) {
		return false
	}
	vo := bo.Uint16(data[off : off+2])
	ln := bo.Uint16(data[off+2 : off+4])
	return vo&0x8000 != 0 && ln == 24
}

func decodeExternalValue(raw []byte, f DescriptorField) ([]byte, bool, error) {
	b := append([]byte(nil), raw...)
	binaryValue := f.Type == FieldBlob || f.Type == FieldVarbinary
	if !binaryValue && !utf8.Valid(b) {
		return nil, false, errors.New("non-UTF8 DB2 out-of-row character value requires a qualified codepage decoder")
	}
	return b, binaryValue, nil
}

func isVectorColumn(col domain.ColumnInfo) bool {
	t := strings.ToLower(strings.TrimSpace(col.DataType))
	ct := strings.ToLower(strings.TrimSpace(col.ColumnType))
	return t == "vector" || strings.HasPrefix(t, "vector(") || strings.HasPrefix(ct, "vector(")
}

func fixedRowFieldNull(data []byte, f DescriptorField) (bool, error) {
	if f.Flags&FieldFlagNoNulls != 0 {
		return false, nil
	}
	off := int(f.Offset)
	fs := fixedSize(f)
	if off < 0 || off+fs >= len(data) {
		return false, fmt.Errorf("nullable field indicator offset %d size %d exceeds row length %d", off, fs, len(data))
	}
	return data[off+fs]&0x01 != 0, nil
}

// ParseVectorManager decodes Db2 12.1.4+ DMS function 213. The payload is the
// string representation required by replication consumers; the internal binary
// VECTOR bytes in the subsequent row are deliberately not interpreted.
func ParseVectorManager(e RecordEnvelope) (*VectorManagerRecord, error) {
	if e.LogType != LogTypeNormal && e.LogType != LogTypeInformational {
		return nil, nil
	}
	raw, err := decodeEnvelopeRaw(e)
	if err != nil {
		return nil, err
	}
	bo, err := orderOf(e.ByteOrder)
	if err != nil {
		return nil, err
	}
	body, err := logPayload(e, raw)
	if err != nil {
		return nil, err
	}
	if len(body) < 6 || body[0] != 1 || body[1] != DMSVectorData {
		return nil, nil
	}
	if len(body) < 16 {
		return nil, errors.New("DB2 VECTOR data log record is truncated")
	}
	n := int(bo.Uint32(body[8:12]))
	if n <= 0 || n > MaxTransactionBytes || 16+n > len(body) {
		return nil, fmt.Errorf("DB2 VECTOR serialized length %d exceeds record/safety bound", n)
	}
	data := append([]byte(nil), body[16:16+n]...)
	if !utf8.Valid(data) {
		return nil, errors.New("DB2 VECTOR serialized value is not UTF-8")
	}
	v := strings.TrimSpace(string(data))
	if len(v) < 2 || v[0] != '[' || v[len(v)-1] != ']' {
		return nil, errors.New("DB2 VECTOR serialized value is not bracket-delimited")
	}
	return &VectorManagerRecord{
		TablespaceID: bo.Uint16(body[2:4]), TableID: bo.Uint16(body[4:6]),
		ColumnID: bo.Uint16(body[6:8]), Data: []byte(v), LRI: e.LRI,
	}, nil
}

// ParseXMLManager decodes the documented component-15 CSL serialized XML
// record. Db2 writes it as informational log type 0x0069 before the DMS row
// record when DATA CAPTURE CHANGES and DB2_DCC_XML_SERIALIZE are enabled.
func ParseXMLManager(e RecordEnvelope) (*XMLManagerRecord, error) {
	if e.LogType != LogTypeInformational {
		return nil, nil
	}
	raw, err := decodeEnvelopeRaw(e)
	if err != nil {
		return nil, err
	}
	bo, err := orderOf(e.ByteOrder)
	if err != nil {
		return nil, err
	}
	body := raw[40:]
	if len(body) < 12 || body[0] != CSLComponentXML {
		return nil, nil
	}
	if body[1] != CSLXMLSerialized {
		return nil, fmt.Errorf("unsupported DB2 CSL manager operation %d", body[1])
	}
	if len(body) < 24 {
		return nil, errors.New("DB2 serialized XML log record is truncated")
	}
	if body[10] != CSLObjectTypeXML {
		return nil, fmt.Errorf("DB2 CSL operation 114 has unexpected object type %d", body[10])
	}
	n := int(bo.Uint32(body[12:16]))
	if n < 0 || n > MaxTransactionBytes || 24+n > len(body) {
		return nil, fmt.Errorf("DB2 serialized XML chunk length %d exceeds record/safety bound", n)
	}
	data := append([]byte(nil), body[24:24+n]...)
	if !utf8.Valid(data) {
		return nil, errors.New("DB2 serialized XML chunk is not UTF-8")
	}
	return &XMLManagerRecord{
		OperationType: body[1], TablespaceID: bo.Uint16(body[2:4]), ObjectID: bo.Uint16(body[4:6]),
		ParentTablespaceID: bo.Uint16(body[6:8]), ParentTableID: bo.Uint16(body[8:10]), ObjectType: body[10],
		ColumnID: bo.Uint16(body[16:18]), Data: data, LRI: e.LRI,
	}, nil
}

// ParseLOBManager decodes the documented component-5 LOB manager records.
// It deliberately ignores information-only delete/non-update records and
// exposes ADD AMOUNT as AmountOnly so callers can fail closed for NOT LOGGED
// LOB columns that have no bytes in the log.
func ParseLOBManager(e RecordEnvelope) (*LOBManagerRecord, error) {
	if e.LogType != LogTypeNormal && e.LogType != LogTypeInformational {
		return nil, nil
	}
	raw, err := decodeEnvelopeRaw(e)
	if err != nil {
		return nil, err
	}
	bo, err := orderOf(e.ByteOrder)
	if err != nil {
		return nil, err
	}
	body := raw[40:]
	if len(body) < 12 || body[0] != 5 {
		return nil, nil
	}
	out := &LOBManagerRecord{
		OperationType:      body[1],
		TablespaceID:       bo.Uint16(body[2:4]),
		ObjectID:           bo.Uint16(body[4:6]),
		ParentTablespaceID: bo.Uint16(body[6:8]),
		ParentTableID:      bo.Uint16(body[8:10]),
	}
	switch out.OperationType {
	case LOBAddData, LOBAddAmount:
		if len(body) < 32 {
			return nil, errors.New("DB2 LOB add record is truncated")
		}
		out.Length = bo.Uint32(body[12:16])
		out.ByteOffset = bo.Uint64(body[16:24])
		out.OriginalOperation = body[25]
		out.ColumnID = bo.Uint16(body[26:28])
		if out.Length > MaxTransactionBytes {
			return nil, fmt.Errorf("DB2 LOB chunk length %d exceeds safety limit", out.Length)
		}
		if out.OperationType == LOBAddAmount {
			out.AmountOnly = true
			return out, nil
		}
		if uint64(32)+uint64(out.Length) > uint64(len(body)) {
			return nil, fmt.Errorf("DB2 LOB chunk length %d exceeds record", out.Length)
		}
		out.Data = append([]byte(nil), body[32:32+int(out.Length)]...)
	case LOBDeleteInfo, LOBNonUpdateInfo:
		out.InformationOnly = true
	default:
		return nil, fmt.Errorf("unsupported DB2 LOB manager operation %d", out.OperationType)
	}
	return out, nil
}

func splitConsolidatedVarying(raw []byte, columns int, bo binary.ByteOrder) (map[int][]byte, error) {
	if len(raw) < 4 || raw[0] != 0x12 {
		return nil, errors.New("DB2 consolidated varying-value LOB has no 0x12 header")
	}
	total := int(raw[1])<<16 | int(raw[2])<<8 | int(raw[3])
	if total != len(raw) {
		return nil, fmt.Errorf("DB2 consolidated varying-value size=%d actual=%d", total, len(raw))
	}
	arrBytes := (columns + 1) * 4
	if len(raw) < 4+arrBytes {
		return nil, errors.New("DB2 consolidated varying-value offset array is truncated")
	}
	arr := raw[4 : 4+arrBytes]
	data := raw[4+arrBytes:]
	decode := func(order binary.ByteOrder) (map[int][]byte, bool) {
		offs := make([]int, columns+1)
		for i := range offs {
			offs[i] = int(order.Uint32(arr[i*4 : i*4+4]))
		}
		base := 0
		switch offs[0] {
		case 0:
			base = 0
		case 4 + arrBytes:
			base = 4 + arrBytes
		case arrBytes:
			base = arrBytes
		default:
			return nil, false
		}
		out := map[int][]byte{}
		prev := -1
		for i, o := range offs {
			o -= base
			if o < 0 || o > len(data) || (prev >= 0 && o < prev) {
				return nil, false
			}
			offs[i] = o
			prev = o
		}
		for i := 0; i < columns; i++ {
			if offs[i+1] > offs[i] {
				out[i] = append([]byte(nil), data[offs[i]:offs[i+1]]...)
			}
		}
		return out, true
	}
	if out, ok := decode(bo); ok {
		return out, nil
	}
	// The structure header explicitly uses big endian for its 3-byte length;
	// accept big-endian offsets as a portability fallback if host order did not
	// form a valid monotonic array.
	if bo != binary.BigEndian {
		if out, ok := decode(binary.BigEndian); ok {
			return out, nil
		}
	}
	return nil, errors.New("DB2 consolidated varying-value offsets are invalid")
}

func fixedSize(f DescriptorField) int {
	switch f.Type {
	case FieldSmallInt:
		return 2
	case FieldInteger, FieldReal, FieldDate:
		return 4
	case FieldBigInt, FieldDouble, FieldDecFloat64:
		return 8
	case FieldDecFloat128:
		return 16
	case FieldTime:
		return 3
	case FieldTimestamp:
		return 10
	case FieldBoolean:
		return 1
	case FieldDecimal:
		return int(f.Length+2) / 2
	case FieldChar, FieldGraphic, FieldBinary:
		return int(f.Length)
	case FieldVarchar, FieldLongVarchar, FieldBlob, FieldClob, FieldVarbinary, FieldXML, FieldVargraphic, FieldLongVargraphic, FieldDBClob:
		return 4
	}
	return int(f.Length)
}

func decodeField(data []byte, f DescriptorField, bo binary.ByteOrder) ([]byte, bool, bool, error) {
	off := int(f.Offset)
	fs := fixedSize(f)
	if off < 0 || off+fs > len(data) {
		return nil, false, false, fmt.Errorf("field offset %d size %d exceeds row length %d", off, fs, len(data))
	}
	nullable := f.Flags&FieldFlagNoNulls == 0
	if nullable {
		ni := off + fs
		if ni >= len(data) {
			return nil, false, false, errors.New("nullable field indicator missing")
		}
		if data[ni]&0x01 != 0 {
			return nil, true, false, nil
		}
	}
	switch f.Type {
	case FieldSmallInt:
		return []byte(fmt.Sprintf("%d", int16(bo.Uint16(data[off:off+2])))), false, false, nil
	case FieldInteger:
		return []byte(fmt.Sprintf("%d", int32(bo.Uint32(data[off:off+4])))), false, false, nil
	case FieldBigInt:
		return []byte(fmt.Sprintf("%d", int64(bo.Uint64(data[off:off+8])))), false, false, nil
	case FieldReal:
		return []byte(fmt.Sprintf("%.9g", math.Float32frombits(bo.Uint32(data[off:off+4])))), false, false, nil
	case FieldDouble:
		return []byte(fmt.Sprintf("%.17g", math.Float64frombits(bo.Uint64(data[off:off+8])))), false, false, nil
	case FieldDecimal:
		s, err := decodePackedDecimal(data[off:off+fs], int(f.Length>>8), int(f.Length&0xff))
		return []byte(s), false, false, err
	case FieldBoolean:
		if data[off] == 0 {
			return []byte("0"), false, false, nil
		}
		return []byte("1"), false, false, nil
	case FieldDate:
		s, err := decodePackedDigits(data[off:off+4], 8)
		if err != nil {
			return nil, false, false, err
		}
		return []byte(s[0:4] + "-" + s[4:6] + "-" + s[6:8]), false, false, nil
	case FieldTime:
		s, err := decodePackedDigits(data[off:off+3], 6)
		if err != nil {
			return nil, false, false, err
		}
		return []byte(s[0:2] + ":" + s[2:4] + ":" + s[4:6]), false, false, nil
	case FieldTimestamp:
		s, err := decodePackedDigits(data[off:off+10], 20)
		if err != nil {
			return nil, false, false, err
		}
		return []byte(s[0:4] + "-" + s[4:6] + "-" + s[6:8] + " " + s[8:10] + ":" + s[10:12] + ":" + s[12:14] + "." + s[14:20]), false, false, nil
	case FieldChar, FieldGraphic:
		b := append([]byte(nil), data[off:off+fs]...)
		if !utf8.Valid(b) {
			return nil, false, false, errors.New("non-UTF8 DB2 character row requires a qualified codepage decoder")
		}
		return []byte(strings.TrimRight(string(b), " ")), false, false, nil
	case FieldBinary:
		return append([]byte(nil), data[off:off+fs]...), false, true, nil
	case FieldVarchar, FieldLongVarchar, FieldBlob, FieldClob, FieldVarbinary, FieldXML, FieldVargraphic, FieldLongVargraphic, FieldDBClob:
		vo := int(bo.Uint16(data[off : off+2]))
		ln := int(bo.Uint16(data[off+2 : off+4]))
		if vo&0x8000 != 0 {
			return nil, false, false, errors.New("out-of-row value detected")
		}
		if vo < 0 || ln < 0 || vo+ln > len(data) {
			return nil, false, false, fmt.Errorf("variable value offset=%d length=%d exceeds row", vo, ln)
		}
		b := append([]byte(nil), data[vo:vo+ln]...)
		switch f.Type {
		case FieldBlob, FieldClob, FieldDBClob:
			if len(b) > 0 && b[0] == 0x80 {
				return []byte{}, false, f.Type == FieldBlob, nil
			}
			if len(b) >= 4 && b[0] == 0x69 {
				b = b[4:]
			}
		}
		binaryValue := f.Type == FieldBlob || f.Type == FieldVarbinary
		if !binaryValue && !utf8.Valid(b) {
			return nil, false, false, errors.New("non-UTF8 DB2 variable character value requires a qualified codepage decoder")
		}
		return b, false, binaryValue, nil
	default:
		return nil, false, false, fmt.Errorf("unsupported DB2 descriptor field type 0x%04x", f.Type)
	}
}

func decodePackedDigits(b []byte, digits int) (string, error) {
	var sb strings.Builder
	for _, x := range b {
		for _, n := range []byte{x >> 4, x & 0xf} {
			if n > 9 {
				return "", fmt.Errorf("invalid packed digit 0x%x", n)
			}
			sb.WriteByte('0' + n)
		}
	}
	s := sb.String()
	if len(s) < digits {
		return "", errors.New("packed digits too short")
	}
	return s[:digits], nil
}
func decodePackedDecimal(b []byte, precision, scale int) (string, error) {
	if len(b) == 0 {
		return "", errors.New("empty packed decimal")
	}
	digits := make([]byte, 0, precision+1)
	neg := false
	for i, x := range b {
		hi, lo := x>>4, x&0xf
		if hi > 9 {
			return "", fmt.Errorf("invalid decimal nibble 0x%x", hi)
		}
		digits = append(digits, '0'+hi)
		if i == len(b)-1 {
			switch lo {
			case 0x0b, 0x0d:
				neg = true
			case 0x0a, 0x0c, 0x0e, 0x0f:
			default:
				return "", fmt.Errorf("invalid decimal sign 0x%x", lo)
			}
		} else {
			if lo > 9 {
				return "", fmt.Errorf("invalid decimal nibble 0x%x", lo)
			}
			digits = append(digits, '0'+lo)
		}
	}
	if len(digits) > precision {
		digits = digits[len(digits)-precision:]
	}
	if len(digits) < precision {
		digits = append(bytesRepeat('0', precision-len(digits)), digits...)
	}
	s := string(digits)
	if scale > 0 {
		if scale >= len(s) {
			s = "0." + strings.Repeat("0", scale-len(s)) + s
		} else {
			s = s[:len(s)-scale] + "." + s[len(s)-scale:]
		}
	}
	if neg {
		s = "-" + s
	}
	return canonicalDecimal(s), nil
}
func bytesRepeat(b byte, n int) []byte {
	if n <= 0 {
		return nil
	}
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}
func canonicalDecimal(s string) string {
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	parts := strings.SplitN(s, ".", 2)
	parts[0] = strings.TrimLeft(parts[0], "0")
	if parts[0] == "" {
		parts[0] = "0"
	}
	if len(parts) == 2 {
		s = parts[0] + "." + parts[1]
	} else {
		s = parts[0]
	}
	if neg && s != "0" {
		if r, ok := new(big.Rat).SetString(s); ok && r.Sign() != 0 {
			s = "-" + s
		}
	}
	return s
}

func DecodeHeaderForValidation(raw []byte, byteOrder string) (uint32, uint16, uint16, string, error) {
	if len(raw) < 40 {
		return 0, 0, 0, "", errors.New("short DB2 log manager header")
	}
	bo, err := orderOf(byteOrder)
	if err != nil {
		return 0, 0, 0, "", err
	}
	ln := bo.Uint32(raw[0:4])
	typ := bo.Uint16(raw[4:6])
	fl := bo.Uint16(raw[6:8])
	return ln, typ, fl, hex.EncodeToString(raw[32:38]), nil
}
