package ticdc

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"qmigration/backend/internal/domain"
)

type canalExtension struct {
	CommitTS    uint64 `json:"commitTs"`
	WatermarkTS uint64 `json:"watermarkTs"`
}

type CanalMessage struct {
	Database  string                       `json:"database"`
	Table     string                       `json:"table"`
	IsDDL     bool                         `json:"isDdl"`
	Type      string                       `json:"type"`
	ES        int64                        `json:"es"`
	TS        int64                        `json:"ts"`
	SQL       string                       `json:"sql"`
	MySQLType map[string]string            `json:"mysqlType"`
	Data      []map[string]json.RawMessage `json:"data"`
	Old       []map[string]json.RawMessage `json:"old"`
	TiDB      canalExtension               `json:"_tidb"`
}

func DecodeCanalJSON(raw []byte, selected map[string]bool) ([]domain.CDCEvent, uint64, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var msg CanalMessage
	if err := dec.Decode(&msg); err != nil {
		return nil, 0, fmt.Errorf("decode TiCDC Canal-JSON: %w", err)
	}
	if dec.More() {
		return nil, 0, errors.New("TiCDC Canal-JSON contains trailing JSON values")
	}
	typ := strings.ToUpper(strings.TrimSpace(msg.Type))
	if typ == "TIDB_WATERMARK" {
		if msg.TiDB.WatermarkTS == 0 {
			return nil, 0, errors.New("TiCDC WATERMARK missing _tidb.watermarkTs")
		}
		e := domain.CDCEvent{Operation: domain.CDCCheckpoint, PositionType: "TIDB_TSO", SourceTimestampMS: sourceMillis(msg.ES, msg.TiDB.WatermarkTS)}
		return []domain.CDCEvent{e}, msg.TiDB.WatermarkTS, nil
	}
	commitTS := msg.TiDB.CommitTS
	if commitTS == 0 {
		return nil, 0, errors.New("TiCDC event missing _tidb.commitTs; enable-tidb-extension=true is required")
	}
	key := strings.ToLower(strings.TrimSpace(msg.Database) + "." + strings.TrimSpace(msg.Table))
	if msg.IsDDL {
		// The dedicated TiCDC changefeed already has exact table-filter rules.
		// DDL events can legitimately carry an empty table (for example schema
		// changes), so only apply an extra local filter when TiCDC supplied a
		// concrete table name.
		if len(selected) > 0 && strings.TrimSpace(msg.Table) != "" && !selected[key] {
			return []domain.CDCEvent{{Operation: domain.CDCCheckpoint, PositionType: "TIDB_TSO", SourceTimestampMS: sourceMillis(msg.ES, commitTS)}}, commitTS, nil
		}
		if strings.TrimSpace(msg.SQL) == "" {
			return nil, 0, errors.New("TiCDC DDL event has empty SQL")
		}
		return []domain.CDCEvent{{Operation: domain.CDCDDL, SourceSchema: msg.Database, SourceTable: msg.Table, SQL: msg.SQL, SourceTimestampMS: sourceMillis(msg.ES, commitTS)}}, commitTS, nil
	}
	if len(selected) > 0 && !selected[key] {
		return []domain.CDCEvent{{Operation: domain.CDCCheckpoint, PositionType: "TIDB_TSO", SourceTimestampMS: sourceMillis(msg.ES, commitTS)}}, commitTS, nil
	}

	var op domain.CDCOperation
	switch typ {
	case "INSERT":
		op = domain.CDCInsert
	case "UPDATE":
		op = domain.CDCUpdate
	case "DELETE":
		op = domain.CDCDelete
	default:
		return nil, 0, fmt.Errorf("unsupported TiCDC Canal-JSON event type %q", msg.Type)
	}
	if len(msg.Data) == 0 {
		return nil, 0, fmt.Errorf("TiCDC %s event has no row data", typ)
	}
	out := make([]domain.CDCEvent, 0, len(msg.Data))
	for i, row := range msg.Data {
		after, err := canalFields(row, msg.MySQLType)
		if err != nil {
			return nil, 0, err
		}
		e := domain.CDCEvent{Operation: op, SourceSchema: msg.Database, SourceTable: msg.Table, SourceTimestampMS: sourceMillis(msg.ES, commitTS)}
		switch op {
		case domain.CDCInsert:
			e.After = after
		case domain.CDCDelete:
			e.Before = after
		case domain.CDCUpdate:
			e.After = after
			// Canal/TiCDC old contains only changed columns. Start from the new
			// image and overlay the old values so QMigration receives a complete
			// before image whenever the sink includes all columns in data.
			beforeMap := cloneRawMap(row)
			if i < len(msg.Old) {
				for k, v := range msg.Old[i] {
					beforeMap[k] = v
				}
			}
			e.Before, err = canalFields(beforeMap, msg.MySQLType)
			if err != nil {
				return nil, 0, err
			}
		}
		out = append(out, e)
	}
	return out, commitTS, nil
}

func cloneRawMap(in map[string]json.RawMessage) map[string]json.RawMessage {
	out := make(map[string]json.RawMessage, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func canalFields(row map[string]json.RawMessage, mysqlTypes map[string]string) ([]domain.CDCField, error) {
	keys := make([]string, 0, len(row))
	for k := range row {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]domain.CDCField, 0, len(keys))
	for _, col := range keys {
		raw := bytes.TrimSpace(row[col])
		if bytes.Equal(raw, []byte("null")) {
			out = append(out, domain.CDCField{Column: col, Null: true})
			continue
		}
		value, err := rawJSONScalar(raw)
		if err != nil {
			return nil, fmt.Errorf("TiCDC column %s: %w", col, err)
		}
		field := domain.CDCField{Column: col, Value: value}
		if isBinaryMySQLType(mysqlTypes[col]) {
			b, err := latin1Bytes(value)
			if err != nil {
				return nil, fmt.Errorf("TiCDC binary column %s: %w", col, err)
			}
			field.Value = base64.StdEncoding.EncodeToString(b)
			field.Encoding = "base64"
		}
		out = append(out, field)
	}
	return out, nil
}

func rawJSONScalar(raw []byte) (string, error) {
	if len(raw) == 0 {
		return "", errors.New("empty JSON value")
	}
	if raw[0] == '"' {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return "", err
		}
		return s, nil
	}
	// Canal-JSON normally serializes data values as strings, but retaining the
	// literal form makes the decoder tolerant of numeric/bool JSON scalars.
	if bytes.Equal(raw, []byte("true")) || bytes.Equal(raw, []byte("false")) || json.Valid(raw) {
		return string(raw), nil
	}
	return "", errors.New("unsupported JSON scalar")
}

func isBinaryMySQLType(t string) bool {
	t = strings.ToLower(strings.TrimSpace(t))
	for _, prefix := range []string{"binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob"} {
		if strings.HasPrefix(t, prefix) {
			return true
		}
	}
	return false
}

func latin1Bytes(s string) ([]byte, error) {
	out := make([]byte, 0, len(s))
	for _, r := range s {
		if r > 255 {
			return nil, fmt.Errorf("rune U+%04X is outside TiCDC binary latin1 range", r)
		}
		out = append(out, byte(r))
	}
	return out, nil
}

func sourceMillis(es int64, tso uint64) int64 {
	if es > 0 {
		return es
	}
	// TiDB TSO = physical milliseconds << 18 | logical.
	return int64(tso >> 18)
}
