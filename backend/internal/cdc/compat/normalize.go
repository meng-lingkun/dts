package compat

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"qmigration/backend/internal/domain"
)

// NormalizeDebezium converts a Debezium JSON envelope (with or without the
// Kafka Connect schema wrapper) into QMigration's stable CDC event contract.
// The final event always carries a durable source position; callers must not
// acknowledge the upstream record when normalization fails.
func NormalizeDebezium(raw []byte) ([]domain.CDCEvent, error) {
	var root map[string]any
	if err := decodeJSON(raw, &root); err != nil {
		return nil, fmt.Errorf("decode Debezium JSON: %w", err)
	}
	payload := root
	if p, ok := asMap(root["payload"]); ok {
		payload = p
	}
	// Debezium schema-change events do not use the normal before/after envelope.
	if ddl := firstString(payload, "ddl"); strings.TrimSpace(ddl) != "" {
		source, _ := asMap(payload["source"])
		e := domain.CDCEvent{
			ID:                eventID(payload, source),
			Operation:         domain.CDCDDL,
			SourceSchema:      sourceSchema(source),
			SourceTable:       firstString(source, "table"),
			SQL:               ddl,
			SourceTimestampMS: firstInt64(payload, "ts_ms", "ts_us", "ts_ns"),
		}
		if e.SourceTimestampMS == 0 {
			e.SourceTimestampMS = firstInt64(source, "ts_ms", "ts_us", "ts_ns")
		}
		setDebeziumPosition(&e, source)
		if e.PositionValue == "" {
			return nil, errors.New("Debezium schema-change event has no durable source position")
		}
		return []domain.CDCEvent{e}, nil
	}

	op := strings.ToLower(strings.TrimSpace(firstString(payload, "op")))
	var operation domain.CDCOperation
	switch op {
	case "c", "r":
		operation = domain.CDCInsert
	case "u":
		operation = domain.CDCUpdate
	case "d":
		operation = domain.CDCDelete
	default:
		return nil, fmt.Errorf("unsupported Debezium operation %q", op)
	}
	source, _ := asMap(payload["source"])
	e := domain.CDCEvent{
		ID:                eventID(payload, source),
		Operation:         operation,
		SourceSchema:      sourceSchema(source),
		SourceTable:       firstString(source, "table"),
		SourceTimestampMS: normalizeTimestamp(firstInt64(payload, "ts_ms", "ts_us", "ts_ns"), payload),
	}
	if e.SourceTimestampMS == 0 {
		e.SourceTimestampMS = normalizeTimestamp(firstInt64(source, "ts_ms", "ts_us", "ts_ns"), source)
	}
	if before, ok := asMap(payload["before"]); ok {
		e.Before = fieldsFromMap(before)
	}
	if after, ok := asMap(payload["after"]); ok {
		e.After = fieldsFromMap(after)
	}
	if operation == domain.CDCInsert && len(e.After) == 0 {
		return nil, errors.New("Debezium INSERT/READ event has no after image")
	}
	if operation == domain.CDCUpdate && (len(e.Before) == 0 || len(e.After) == 0) {
		return nil, errors.New("Debezium UPDATE event requires before and after images; configure a full replica identity/binlog row image")
	}
	if operation == domain.CDCDelete && len(e.Before) == 0 {
		return nil, errors.New("Debezium DELETE event has no before image")
	}
	setDebeziumPosition(&e, source)
	if e.PositionValue == "" {
		return nil, errors.New("Debezium event has no durable source position (GTID/binlog/LSN/SCN/change_lsn)")
	}
	return []domain.CDCEvent{e}, nil
}

// NormalizeCanal converts the common Canal JSON adapter envelope into one or
// more QMigration CDC events. Canal UPDATE "old" values are merged into the
// current row to reconstruct a complete before image.
func NormalizeCanal(raw []byte) ([]domain.CDCEvent, error) {
	var msg struct {
		Data     []map[string]any `json:"data"`
		Database string           `json:"database"`
		ES       int64            `json:"es"`
		ID       json.Number      `json:"id"`
		IsDDL    bool             `json:"isDdl"`
		Old      []map[string]any `json:"old"`
		SQL      string           `json:"sql"`
		Table    string           `json:"table"`
		TS       int64            `json:"ts"`
		Type     string           `json:"type"`
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&msg); err != nil {
		return nil, fmt.Errorf("decode Canal JSON: %w", err)
	}
	position := strings.TrimSpace(msg.ID.String())
	if position == "" || position == "0" {
		return nil, errors.New("Canal event has no durable message id")
	}
	position = position + "@" + strconv.FormatInt(msg.ES, 10)
	timestamp := msg.ES
	if timestamp == 0 {
		timestamp = msg.TS
	}
	kind := strings.ToUpper(strings.TrimSpace(msg.Type))
	if msg.IsDDL || kind == "DDL" || (strings.TrimSpace(msg.SQL) != "" && len(msg.Data) == 0) {
		if strings.TrimSpace(msg.SQL) == "" {
			return nil, errors.New("Canal DDL event has empty sql")
		}
		return []domain.CDCEvent{{
			ID:                "canal:" + position,
			Operation:         domain.CDCDDL,
			SourceSchema:      msg.Database,
			SourceTable:       msg.Table,
			SQL:               msg.SQL,
			PositionType:      "CANAL_ID",
			PositionValue:     position,
			SourceTimestampMS: timestamp,
		}}, nil
	}
	var op domain.CDCOperation
	switch kind {
	case "INSERT":
		op = domain.CDCInsert
	case "UPDATE":
		op = domain.CDCUpdate
	case "DELETE":
		op = domain.CDCDelete
	default:
		return nil, fmt.Errorf("unsupported Canal operation %q", msg.Type)
	}
	if len(msg.Data) == 0 {
		return nil, fmt.Errorf("Canal %s event has no data rows", kind)
	}
	out := make([]domain.CDCEvent, 0, len(msg.Data))
	for i, row := range msg.Data {
		e := domain.CDCEvent{
			ID:                fmt.Sprintf("canal:%s:%d", position, i),
			Operation:         op,
			SourceSchema:      msg.Database,
			SourceTable:       msg.Table,
			SourceTimestampMS: timestamp,
		}
		switch op {
		case domain.CDCInsert:
			e.After = fieldsFromMap(row)
		case domain.CDCDelete:
			e.Before = fieldsFromMap(row)
		case domain.CDCUpdate:
			before := cloneMap(row)
			if i < len(msg.Old) {
				for k, v := range msg.Old[i] {
					before[k] = v
				}
			}
			e.Before = fieldsFromMap(before)
			e.After = fieldsFromMap(row)
		}
		out = append(out, e)
	}
	// One Canal message may contain many rows. Only the final row advances the
	// durable message position so QMigration applies the group atomically.
	last := &out[len(out)-1]
	last.PositionType = "CANAL_ID"
	last.PositionValue = position
	return out, nil
}

func decodeJSON(raw []byte, out any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(out); err != nil {
		return err
	}
	if dec.More() {
		return errors.New("multiple JSON documents in one request")
	}
	return nil
}

func asMap(v any) (map[string]any, bool) {
	m, ok := v.(map[string]any)
	return m, ok
}

func cloneMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func sourceSchema(source map[string]any) string {
	if s := firstString(source, "schema"); s != "" {
		return s
	}
	return firstString(source, "db", "database")
}

func eventID(payload, source map[string]any) string {
	if tx, ok := asMap(payload["transaction"]); ok {
		if id := firstString(tx, "id"); id != "" {
			return id
		}
	}
	if id := firstString(source, "txId", "tx_id", "transaction_id"); id != "" {
		return id
	}
	return ""
}

func firstString(m map[string]any, keys ...string) string {
	for _, k := range keys {
		v, ok := m[k]
		if !ok || v == nil {
			continue
		}
		switch x := v.(type) {
		case string:
			if strings.TrimSpace(x) != "" {
				return x
			}
		case json.Number:
			return x.String()
		case float64:
			return strconv.FormatFloat(x, 'f', -1, 64)
		case int64:
			return strconv.FormatInt(x, 10)
		case int:
			return strconv.Itoa(x)
		}
	}
	return ""
}

func firstInt64(m map[string]any, keys ...string) int64 {
	for _, k := range keys {
		v, ok := m[k]
		if !ok || v == nil {
			continue
		}
		switch x := v.(type) {
		case json.Number:
			n, _ := x.Int64()
			if n != 0 {
				return n
			}
		case float64:
			if x != 0 {
				return int64(x)
			}
		case string:
			n, _ := strconv.ParseInt(x, 10, 64)
			if n != 0 {
				return n
			}
		}
	}
	return 0
}

func normalizeTimestamp(v int64, m map[string]any) int64 {
	if v == 0 {
		return 0
	}
	if _, ok := m["ts_ns"]; ok {
		return v / 1_000_000
	}
	if _, ok := m["ts_us"]; ok {
		return v / 1_000
	}
	return v
}

func setDebeziumPosition(e *domain.CDCEvent, source map[string]any) {
	if gtid := firstString(source, "gtid"); gtid != "" {
		e.PositionType, e.PositionValue = "GTID", gtid
		e.Resource = firstString(source, "file")
		return
	}
	if file := firstString(source, "file"); file != "" {
		if pos := firstString(source, "pos"); pos != "" {
			e.PositionType, e.PositionValue, e.Resource = "BINLOG", file+":"+pos, file
			return
		}
	}
	if lsn := firstString(source, "lsn"); lsn != "" {
		e.PositionType, e.PositionValue = "LSN", lsn
		return
	}
	if changeLSN := firstString(source, "change_lsn", "commit_lsn"); changeLSN != "" {
		e.PositionType, e.PositionValue = "LSN", changeLSN
		return
	}
	if scn := firstString(source, "scn", "commit_scn"); scn != "" {
		e.PositionType, e.PositionValue = "SCN", scn
	}
}

func fieldsFromMap(row map[string]any) []domain.CDCField {
	keys := make([]string, 0, len(row))
	for k := range row {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]domain.CDCField, 0, len(keys))
	for _, k := range keys {
		v := row[k]
		if v == nil {
			out = append(out, domain.CDCField{Column: k, Null: true})
			continue
		}
		var value string
		switch x := v.(type) {
		case string:
			value = x
		case json.Number:
			value = x.String()
		case bool:
			value = strconv.FormatBool(x)
		case float64:
			value = strconv.FormatFloat(x, 'f', -1, 64)
		default:
			b, err := json.Marshal(x)
			if err != nil {
				value = fmt.Sprint(x)
			} else {
				value = string(b)
			}
		}
		out = append(out, domain.CDCField{Column: k, Value: value})
	}
	return out
}
