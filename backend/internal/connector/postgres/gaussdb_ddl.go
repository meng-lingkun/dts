package postgresconnector

import (
	"encoding/json"
	"errors"
	"fmt"
	"qmigration/backend/internal/domain"
	"strconv"
	"strings"
)

type gaussDBDDLTxnSummary struct {
	XID       string
	CommitLSN string
	DDL       []domain.CDCEvent
	// Sequence contains DDL indexes (>=0) and DML placeholders (-1), preserving
	// the text decoder transaction order. Binary DML is substituted only when
	// its cardinality exactly matches these placeholders.
	Sequence []int
	DMLCount int
	HasDML   bool
}

func gaussDBDDLDecodeQuery(function, slot string, maxChanges int, uptoLSN string, tables []string) (string, error) {
	slot, err := gaussDBSlotName(slot)
	if err != nil {
		return "", err
	}
	if function != "pg_logical_slot_peek_changes" && function != "pg_logical_slot_get_changes" {
		return "", fmt.Errorf("unsupported GaussDB DDL logical function %q", function)
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
	return "SELECT location::text,xid::text,data FROM " + function + "(" + pgLiteral(slot) + "," + upto + "," + strconv.Itoa(maxChanges) + ",'skip-empty-xacts','on','include-xids','on','white-table-list'," + pgLiteral(white) + ",'enable-ddl-decoding','true','enable-ddl-json-format','false')", nil
}

func parseGaussDBTDDL(raw string) (string, bool) {
	var v struct {
		TDDL string `json:"TDDL"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(raw)), &v); err != nil || strings.TrimSpace(v.TDDL) == "" {
		return "", false
	}
	return strings.TrimSpace(v.TDDL), true
}

func gaussDBSelectedTableSet(tables []string) (map[string]bool, error) {
	if _, err := gaussDBWhiteTableList(tables); err != nil {
		return nil, err
	}
	out := make(map[string]bool, len(tables))
	for _, table := range tables {
		out[strings.ToLower(strings.TrimSpace(table))] = true
	}
	return out, nil
}

func gaussDBSimpleQualifiedAfter(sql, prefix string) (string, bool) {
	rest := strings.TrimSpace(sql[len(prefix):])
	upper := strings.ToUpper(rest)
	if strings.HasPrefix(upper, "IF EXISTS ") {
		rest = strings.TrimSpace(rest[len("IF EXISTS "):])
	} else if strings.HasPrefix(upper, "IF NOT EXISTS ") {
		rest = strings.TrimSpace(rest[len("IF NOT EXISTS "):])
	}
	if strings.HasPrefix(strings.ToUpper(rest), "ONLY ") {
		rest = strings.TrimSpace(rest[len("ONLY "):])
	}
	end := 0
	for end < len(rest) {
		b := rest[end]
		if (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_' || b == '$' || b == '#' || b == '.' {
			end++
			continue
		}
		break
	}
	if end == 0 {
		return "", false
	}
	name := rest[:end]
	parts := strings.Split(name, ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", false
	}
	return strings.ToLower(name), true
}

func gaussDBSafeDDL(sql string, tables []string) error {
	selected, err := gaussDBSelectedTableSet(tables)
	if err != nil {
		return err
	}
	s := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(sql), ";"))
	if strings.Contains(s, ";") {
		return errors.New("GaussDB CDC DDL must contain exactly one statement")
	}
	upper := strings.ToUpper(s)
	var table string
	switch {
	case strings.HasPrefix(upper, "ALTER TABLE "):
		table, _ = gaussDBSimpleQualifiedAfter(s, s[:len("ALTER TABLE ")])
	case strings.HasPrefix(upper, "TRUNCATE TABLE "):
		table, _ = gaussDBSimpleQualifiedAfter(s, s[:len("TRUNCATE TABLE ")])
	case strings.HasPrefix(upper, "TRUNCATE "):
		table, _ = gaussDBSimpleQualifiedAfter(s, s[:len("TRUNCATE ")])
	case strings.HasPrefix(upper, "CREATE INDEX "), strings.HasPrefix(upper, "CREATE UNIQUE INDEX "):
		// CREATE INDEX is accepted only when the normalized DDL includes a
		// simple schema-qualified ON target from the selected table set.
		on := strings.Index(upper, " ON ")
		if on >= 0 {
			suffix := strings.TrimSpace(s[on+len(" ON "):])
			table, _ = gaussDBSimpleQualifiedAfter(suffix, "")
		}
	default:
		return fmt.Errorf("GaussDB CDC DDL is outside RC16 safe replay subset: %s", firstDDLWords(upper, 4))
	}
	if table == "" || !selected[table] {
		return fmt.Errorf("GaussDB CDC DDL target is not an unambiguous selected table: %q", sql)
	}
	if strings.Contains(upper, " CONCURRENTLY ") {
		return errors.New("GaussDB CDC DDL CONCURRENTLY is not qualified for replay")
	}
	return nil
}

func firstDDLWords(s string, n int) string {
	fields := strings.Fields(s)
	if len(fields) > n {
		fields = fields[:n]
	}
	return strings.Join(fields, " ")
}

func parseGaussDBDDLRows(rows *RawRows, slot string, tables []string) ([]gaussDBDDLTxnSummary, error) {
	if rows == nil {
		return nil, errors.New("nil GaussDB DDL logical rows")
	}
	var out []gaussDBDDLTxnSummary
	var current *gaussDBDDLTxnSummary
	ordinal := 0
	for i, row := range rows.Rows {
		if len(row) < 3 {
			return nil, fmt.Errorf("GaussDB DDL logical row %d has %d columns", i, len(row))
		}
		lsn := normalizeGaussLSN(string(row[0]))
		if _, err := parseReplicationLSN(lsn); err != nil {
			return nil, fmt.Errorf("GaussDB DDL row %d has invalid LSN %q: %w", i, lsn, err)
		}
		xid := strings.TrimSpace(string(row[1]))
		data := strings.TrimSpace(string(row[2]))
		upper := strings.ToUpper(data)
		switch {
		case strings.HasPrefix(upper, "BEGIN"):
			if current != nil {
				return nil, fmt.Errorf("GaussDB DDL BEGIN %s before transaction %s committed", xid, current.XID)
			}
			current = &gaussDBDDLTxnSummary{XID: xid}
			ordinal = 0
		case strings.HasPrefix(upper, "COMMIT"):
			if current == nil {
				return nil, fmt.Errorf("GaussDB DDL COMMIT %s without BEGIN", xid)
			}
			if xid != "" && current.XID != "" && xid != current.XID {
				return nil, fmt.Errorf("GaussDB DDL COMMIT xid %s does not match BEGIN %s", xid, current.XID)
			}
			current.CommitLSN = lsn
			for j := range current.DDL {
				current.DDL[j].ID = fmt.Sprintf("gaussdb:%s:%s:ddl:%d", current.XID, lsn, j)
				current.DDL[j].PositionType = "GAUSSDB_LSN"
				current.DDL[j].PositionValue = lsn
				current.DDL[j].Resource = slot
			}
			out = append(out, *current)
			current = nil
		default:
			if current == nil {
				return nil, fmt.Errorf("GaussDB DDL logical change outside BEGIN/COMMIT at %s", lsn)
			}
			if xid != "" && current.XID != "" && xid != current.XID {
				return nil, fmt.Errorf("GaussDB DDL logical xid changed from %s to %s", current.XID, xid)
			}
			if ddl, ok := parseGaussDBTDDL(data); ok {
				if err := gaussDBSafeDDL(ddl, tables); err != nil {
					return nil, fmt.Errorf("GaussDB transaction %s: %w", current.XID, err)
				}
				ordinal++
				if ordinal > gaussDBMaxTransactionEvents {
					return nil, fmt.Errorf("GaussDB transaction %s exceeds %d events", current.XID, gaussDBMaxTransactionEvents)
				}
				current.DDL = append(current.DDL, domain.CDCEvent{Operation: domain.CDCDDL, SQL: ddl})
				current.Sequence = append(current.Sequence, len(current.DDL)-1)
			} else {
				// We intentionally do not parse textual DML values. This pass
				// exists only to classify transaction shape; byte-safe DML is
				// loaded from the binary function at the same commit boundary.
				current.HasDML = true
				current.DMLCount++
				current.Sequence = append(current.Sequence, -1)
			}
		}
	}
	if current != nil {
		return nil, fmt.Errorf("GaussDB DDL logical batch ended before COMMIT for xid %s", current.XID)
	}
	return out, nil
}

func gaussDBTxnKey(xid, lsn string) string {
	return strings.TrimSpace(xid) + "@" + normalizeGaussLSN(lsn)
}

func mergeGaussDBDDLAndBinary(summaries []gaussDBDDLTxnSummary, binaryTx []GaussDBTransaction) ([]GaussDBTransaction, error) {
	byKey := make(map[string]GaussDBTransaction, len(binaryTx))
	for _, tx := range binaryTx {
		key := gaussDBTxnKey(tx.XID, tx.CommitLSN)
		if _, exists := byKey[key]; exists {
			return nil, fmt.Errorf("duplicate GaussDB binary transaction %s", key)
		}
		byKey[key] = tx
	}
	out := make([]GaussDBTransaction, 0, len(summaries))
	used := map[string]bool{}
	for _, s := range summaries {
		if s.CommitLSN == "" {
			return nil, fmt.Errorf("GaussDB transaction %s has no commit LSN", s.XID)
		}
		key := gaussDBTxnKey(s.XID, s.CommitLSN)
		if !s.HasDML {
			out = append(out, GaussDBTransaction{XID: s.XID, CommitLSN: s.CommitLSN, Events: s.DDL})
			continue
		}
		tx, ok := byKey[key]
		if !ok {
			return nil, fmt.Errorf("GaussDB binary decoder did not return DML transaction %s classified by DDL pass", key)
		}
		if len(tx.Events) != s.DMLCount {
			return nil, fmt.Errorf("GaussDB hybrid transaction %s DML placeholder count %d does not match binary event count %d", key, s.DMLCount, len(tx.Events))
		}
		used[key] = true
		if len(s.DDL) == 0 {
			out = append(out, tx)
			continue
		}
		merged := GaussDBTransaction{XID: s.XID, CSN: tx.CSN, CommitLSN: s.CommitLSN}
		dml := 0
		for _, token := range s.Sequence {
			if token < 0 {
				merged.Events = append(merged.Events, tx.Events[dml])
				dml++
				continue
			}
			if token >= len(s.DDL) {
				return nil, fmt.Errorf("GaussDB hybrid transaction %s has invalid DDL sequence index", key)
			}
			merged.Events = append(merged.Events, s.DDL[token])
		}
		out = append(out, merged)
	}
	for key := range byKey {
		if !used[key] {
			return nil, fmt.Errorf("GaussDB binary decoder returned transaction %s missing from DDL classification pass", key)
		}
	}
	return out, nil
}

func gaussDBTransactionHasDDL(tx GaussDBTransaction) bool {
	for _, ev := range tx.Events {
		if ev.Operation == domain.CDCDDL {
			return true
		}
	}
	return false
}
