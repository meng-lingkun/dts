package postgresconnector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"qmigration/backend/internal/domain"
	"strconv"
	"strings"
)

// GaussDBTransaction is one committed logical-decoding transaction returned by
// GaussDB's pg_logical_slot_peek_changes SQL API.  QMigration applies Events
// first and advances the source slot to CommitLSN only after target commit.
type GaussDBTransaction struct {
	XID       string
	CSN       uint64
	CommitLSN string
	Events    []domain.CDCEvent
}

type gaussDBChange struct {
	TableName   string            `json:"table_name"`
	OpType      string            `json:"op_type"`
	ColumnsName []string          `json:"columns_name"`
	ColumnsType []string          `json:"columns_type"`
	ColumnsVal  []json.RawMessage `json:"columns_val"`
	OldKeysName []string          `json:"old_keys_name"`
	OldKeysType []string          `json:"old_keys_type"`
	OldKeysVal  []json.RawMessage `json:"old_keys_val"`
}

const (
	gaussDBMaxTransactionEvents = 100000
	gaussDBMaxTransactionBytes  = 128 << 20
)

func normalizeGaussLSN(v string) string {
	return strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(v), " ", ""))
}

func gaussDBSlotName(name string) (string, error) {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" || len(name) > 63 || !replicationSlotName.MatchString(name) {
		return "", errors.New("invalid GaussDB logical replication slot name")
	}
	return name, nil
}

func gaussDBWhiteTableList(tables []string) (string, error) {
	if len(tables) == 0 {
		return "", errors.New("GaussDB logical decoding requires selected tables")
	}
	out := make([]string, 0, len(tables))
	seen := map[string]bool{}
	for _, raw := range tables {
		raw = strings.TrimSpace(raw)
		parts := strings.Split(raw, ".")
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" || strings.ContainsAny(raw, "'\" ,;\\") {
			return "", fmt.Errorf("invalid GaussDB logical-decoding table %q", raw)
		}
		key := strings.ToLower(raw)
		if !seen[key] {
			seen[key] = true
			out = append(out, raw)
		}
	}
	return strings.Join(out, ","), nil
}

func gaussDBCreateSlotQuery(slot string) (string, error) {
	slot, err := gaussDBSlotName(slot)
	if err != nil {
		return "", err
	}
	// output_order=0 is mandatory for QMigration. Recent GaussDB releases can
	// create CSN-ordered slots on CNs; pg_logical_slot_get_changes cannot
	// advance those slots on a CN, while QMigration checkpoints are LSN based.
	return "SELECT * FROM pg_create_logical_replication_slot(" + pgLiteral(slot) + ",'mppdb_decoding',0)", nil
}

func gaussDBDecodeQuery(function, slot string, maxChanges int, uptoLSN string, tables []string) (string, error) {
	slot, err := gaussDBSlotName(slot)
	if err != nil {
		return "", err
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
		if _, err := parseReplicationLSN(normalizeGaussLSN(uptoLSN)); err != nil {
			return "", fmt.Errorf("invalid GaussDB LSN %q: %w", uptoLSN, err)
		}
		upto = pgLiteral(normalizeGaussLSN(uptoLSN))
	}
	return "SELECT location::text,xid::text,data FROM " + function + "(" + pgLiteral(slot) + "," + upto + "," + strconv.Itoa(maxChanges) + ",'skip-empty-xacts','on','include-xids','on','white-table-list'," + pgLiteral(white) + ")", nil
}

func parseGaussDBField(column, typ string, raw json.RawMessage) (domain.CDCField, error) {
	f := domain.CDCField{Column: column}
	trimmed := bytes.TrimSpace(raw)
	if bytes.Equal(trimmed, []byte("null")) {
		f.Null = true
		return f, nil
	}
	// GaussDB documents a truncation risk for NUL bytes in JSON logical
	// decoding.  Binary families therefore stay fail-closed in RC14 rather
	// than silently moving a damaged value.
	lowerType := strings.ToLower(strings.TrimSpace(typ))
	if strings.Contains(lowerType, "bytea") || strings.Contains(lowerType, "blob") || strings.Contains(lowerType, "binary") || strings.Contains(lowerType, "raw") {
		return f, fmt.Errorf("GaussDB JSON logical decoding is not qualified for binary column %s type %s", column, typ)
	}
	var s string
	if err := json.Unmarshal(trimmed, &s); err != nil {
		return f, fmt.Errorf("GaussDB logical value %s is not a JSON string/null: %s", column, string(trimmed))
	}
	f.Value = s
	return f, nil
}

func gaussDBFields(names, types []string, vals []json.RawMessage) ([]domain.CDCField, error) {
	if len(names) != len(types) || len(names) != len(vals) {
		return nil, fmt.Errorf("GaussDB logical change column arrays differ: names=%d types=%d values=%d", len(names), len(types), len(vals))
	}
	out := make([]domain.CDCField, 0, len(names))
	for i := range names {
		if strings.TrimSpace(names[i]) == "" {
			return nil, errors.New("GaussDB logical change contains empty column name")
		}
		f, err := parseGaussDBField(names[i], types[i], vals[i])
		if err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, nil
}

func parseGaussDBChange(raw []byte) ([]gaussDBChange, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return nil, errors.New("empty GaussDB logical change")
	}
	var changes []gaussDBChange
	if raw[0] == '[' {
		if err := json.Unmarshal(raw, &changes); err != nil {
			return nil, err
		}
	} else {
		var one gaussDBChange
		if err := json.Unmarshal(raw, &one); err != nil {
			return nil, err
		}
		changes = []gaussDBChange{one}
	}
	if len(changes) == 0 {
		return nil, errors.New("empty GaussDB logical change array")
	}
	return changes, nil
}

func gaussDBChangeEvent(ch gaussDBChange) (domain.CDCEvent, error) {
	parts := strings.SplitN(strings.TrimSpace(ch.TableName), ".", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return domain.CDCEvent{}, fmt.Errorf("invalid GaussDB logical table name %q", ch.TableName)
	}
	after, err := gaussDBFields(ch.ColumnsName, ch.ColumnsType, ch.ColumnsVal)
	if err != nil {
		return domain.CDCEvent{}, err
	}
	before, err := gaussDBFields(ch.OldKeysName, ch.OldKeysType, ch.OldKeysVal)
	if err != nil {
		return domain.CDCEvent{}, err
	}
	ev := domain.CDCEvent{SourceSchema: parts[0], SourceTable: parts[1], Before: before, After: after}
	switch strings.ToUpper(strings.TrimSpace(ch.OpType)) {
	case "INSERT":
		if len(after) == 0 {
			return ev, errors.New("GaussDB INSERT logical change has no after image")
		}
		ev.Operation = domain.CDCInsert
	case "UPDATE":
		if len(after) == 0 || len(before) == 0 {
			return ev, errors.New("GaussDB UPDATE logical change requires old keys and new tuple")
		}
		ev.Operation = domain.CDCUpdate
	case "DELETE":
		if len(before) == 0 {
			return ev, errors.New("GaussDB DELETE logical change has no old key image")
		}
		ev.Operation = domain.CDCDelete
		ev.After = nil
	default:
		return ev, fmt.Errorf("unsupported GaussDB logical operation %q", ch.OpType)
	}
	return ev, nil
}

func isGaussBegin(s string) bool {
	return strings.HasPrefix(strings.ToUpper(strings.TrimSpace(s)), "BEGIN")
}
func isGaussCommit(s string) bool {
	return strings.HasPrefix(strings.ToUpper(strings.TrimSpace(s)), "COMMIT")
}

// ParseGaussDBLogicalRows converts the SQL-function result into complete source
// transactions.  Batches ending without COMMIT fail closed; Huawei documents
// upto_nchanges as stopping only after the current transaction completes, so a
// partial transaction indicates an incompatible server/output mode.
func ParseGaussDBLogicalRows(rows *RawRows, slot string) ([]GaussDBTransaction, error) {
	if rows == nil {
		return nil, errors.New("nil GaussDB logical rows")
	}
	var out []GaussDBTransaction
	var current *GaussDBTransaction
	bytesInTxn := 0
	ordinal := 0
	for i, row := range rows.Rows {
		if len(row) < 3 {
			return nil, fmt.Errorf("GaussDB logical row %d has %d columns", i, len(row))
		}
		lsn := normalizeGaussLSN(string(row[0]))
		xid := strings.TrimSpace(string(row[1]))
		data := strings.TrimSpace(string(row[2]))
		if data == "" {
			continue
		}
		if isGaussBegin(data) {
			if current != nil {
				return nil, fmt.Errorf("GaussDB BEGIN %s before transaction %s committed", xid, current.XID)
			}
			current = &GaussDBTransaction{XID: xid}
			bytesInTxn = 0
			ordinal = 0
			continue
		}
		if isGaussCommit(data) {
			if current == nil {
				return nil, fmt.Errorf("GaussDB COMMIT %s without BEGIN", xid)
			}
			if xid != "" && current.XID != "" && xid != current.XID {
				return nil, fmt.Errorf("GaussDB COMMIT xid %s does not match BEGIN %s", xid, current.XID)
			}
			if _, err := parseReplicationLSN(lsn); err != nil {
				return nil, fmt.Errorf("invalid GaussDB commit LSN %q: %w", lsn, err)
			}
			current.CommitLSN = lsn
			for j := range current.Events {
				current.Events[j].ID = fmt.Sprintf("gaussdb:%s:%s:%d", current.XID, lsn, j)
				current.Events[j].PositionType = "GAUSSDB_LSN"
				current.Events[j].PositionValue = lsn
				current.Events[j].Resource = slot
			}
			if len(current.Events) > 0 {
				out = append(out, *current)
			}
			current = nil
			continue
		}
		if current == nil {
			return nil, fmt.Errorf("GaussDB logical change outside BEGIN/COMMIT at %s", lsn)
		}
		if xid != "" && current.XID != "" && xid != current.XID {
			return nil, fmt.Errorf("GaussDB logical xid changed from %s to %s", current.XID, xid)
		}
		changes, err := parseGaussDBChange([]byte(data))
		if err != nil {
			return nil, fmt.Errorf("decode GaussDB logical JSON at %s: %w", lsn, err)
		}
		bytesInTxn += len(data)
		if bytesInTxn > gaussDBMaxTransactionBytes {
			return nil, fmt.Errorf("GaussDB transaction %s exceeds %d decoded bytes", current.XID, gaussDBMaxTransactionBytes)
		}
		for _, ch := range changes {
			ev, err := gaussDBChangeEvent(ch)
			if err != nil {
				return nil, fmt.Errorf("GaussDB transaction %s: %w", current.XID, err)
			}
			ordinal++
			if ordinal > gaussDBMaxTransactionEvents {
				return nil, fmt.Errorf("GaussDB transaction %s exceeds %d events", current.XID, gaussDBMaxTransactionEvents)
			}
			current.Events = append(current.Events, ev)
		}
	}
	if current != nil {
		return nil, fmt.Errorf("GaussDB logical batch ended before COMMIT for xid %s", current.XID)
	}
	return out, nil
}

func (c *Connector) peekGaussDBBinaryTransactions(ctx context.Context, slot string, maxChanges int, uptoLSN string, tables []string) ([]GaussDBTransaction, error) {
	q, err := gaussDBBinaryDecodeQuery("pg_logical_slot_peek_binary_changes", slot, maxChanges, uptoLSN, tables)
	if err != nil {
		return nil, err
	}
	rows, err := c.QuerySQL(ctx, q)
	if err != nil {
		return nil, err
	}
	return ParseGaussDBBinaryRows(rows, slot)
}

func (c *Connector) PeekGaussDBTransactions(ctx context.Context, slot string, maxChanges int, tables []string) ([]GaussDBTransaction, error) {
	if c.ds.Type != domain.DataSourceGaussDB {
		return nil, errors.New("GaussDB logical decoding requires a gaussdb datasource")
	}
	if !gaussDBCDCEnabled() {
		return nil, errors.New("GaussDB logical CDC gate is not enabled")
	}
	return c.peekGaussDBBinaryTransactions(ctx, slot, maxChanges, "", tables)
}

// PeekGaussDBTransactionsWithDDL performs a text-only classification pass with
// DDL decoding enabled, then fetches byte-safe DML through the binary function
// up to the exact same commit boundary. GaussDB documents that hybrid DDL/DML
// transactions are not fully decodable; callers must enable this path only
// when source policy guarantees DDL runs in independent transactions.
func (c *Connector) PeekGaussDBTransactionsWithDDL(ctx context.Context, slot string, maxChanges int, tables []string) ([]GaussDBTransaction, error) {
	if c.ds.Type != domain.DataSourceGaussDB {
		return nil, errors.New("GaussDB DDL logical decoding requires a gaussdb datasource")
	}
	if !gaussDBCDCEnabled() {
		return nil, errors.New("GaussDB logical CDC gate is not enabled")
	}
	q, err := gaussDBDDLDecodeQuery("pg_logical_slot_peek_changes", slot, maxChanges, "", tables)
	if err != nil {
		return nil, err
	}
	rows, err := c.QuerySQL(ctx, q)
	if err != nil {
		return nil, err
	}
	summaries, err := parseGaussDBDDLRows(rows, slot, tables)
	if err != nil {
		return nil, err
	}
	if len(summaries) == 0 {
		return nil, nil
	}
	lastLSN := summaries[len(summaries)-1].CommitLSN
	binaryTx, err := c.peekGaussDBBinaryTransactions(ctx, slot, 100000, lastLSN, tables)
	if err != nil {
		return nil, err
	}
	return mergeGaussDBDDLAndBinary(summaries, binaryTx)
}

// AcknowledgeGaussDBTransaction advances the slot only after QMigration has
// committed the target transaction.  pg_logical_slot_get_changes with upto_lsn
// consumes the previously peeked source range and therefore provides the same
// apply-before-ACK ordering as the other native CDC readers.
func (c *Connector) AcknowledgeGaussDBTransaction(ctx context.Context, slot, commitLSN string, tables []string) error {
	if c.ds.Type != domain.DataSourceGaussDB {
		return errors.New("GaussDB ACK requires a gaussdb datasource")
	}
	q, err := gaussDBBinaryDecodeQuery("pg_logical_slot_get_binary_changes", slot, 100000, commitLSN, tables)
	if err != nil {
		return err
	}
	rows, err := c.QuerySQL(ctx, q)
	if err != nil {
		return err
	}
	if len(rows.Rows) == 0 {
		return fmt.Errorf("GaussDB slot %s did not advance to %s", slot, commitLSN)
	}
	txs, err := ParseGaussDBBinaryRows(rows, slot)
	if err != nil {
		return fmt.Errorf("verify GaussDB binary ACK: %w", err)
	}
	wanted := normalizeGaussLSN(commitLSN)
	for _, tx := range txs {
		if normalizeGaussLSN(tx.CommitLSN) == wanted {
			return nil
		}
	}
	return fmt.Errorf("GaussDB slot %s ACK response did not contain COMMIT at %s", slot, wanted)
}

// AcknowledgeGaussDBDecodedTransaction advances a previously peeked transaction
// with the same decoding mode that made the transaction observable. DDL-only
// transactions are consumed through the text DDL path; DML transactions remain
// byte-safe and are consumed through the binary path.
func (c *Connector) AcknowledgeGaussDBDecodedTransaction(ctx context.Context, slot string, tx GaussDBTransaction, tables []string) error {
	if !gaussDBTransactionHasDDL(tx) {
		return c.AcknowledgeGaussDBTransaction(ctx, slot, tx.CommitLSN, tables)
	}
	q, err := gaussDBDDLDecodeQuery("pg_logical_slot_get_changes", slot, 100000, tx.CommitLSN, tables)
	if err != nil {
		return err
	}
	rows, err := c.QuerySQL(ctx, q)
	if err != nil {
		return err
	}
	summaries, err := parseGaussDBDDLRows(rows, slot, tables)
	if err != nil {
		return fmt.Errorf("verify GaussDB DDL ACK: %w", err)
	}
	wanted := gaussDBTxnKey(tx.XID, tx.CommitLSN)
	for _, summary := range summaries {
		if gaussDBTxnKey(summary.XID, summary.CommitLSN) == wanted && len(summary.DDL) > 0 && !summary.HasDML {
			return nil
		}
	}
	return fmt.Errorf("GaussDB slot %s DDL ACK response did not contain transaction %s", slot, wanted)
}

// ValidateGaussDBCDCSelection ensures UPDATE/DELETE have a stable key image.
// RC15 uses the documented length-delimited binary logical-decoding functions,
// so binary/NUL-bearing values no longer require a table-level rejection.
func (c *Connector) ValidateGaussDBCDCSelection(ctx context.Context, tables []domain.TableMapping) error {
	if c.ds.Type != domain.DataSourceGaussDB {
		return nil
	}
	if !gaussDBCDCEnabled() {
		return errors.New("GaussDB logical CDC gate is not enabled")
	}
	for _, t := range tables {
		md, err := c.GetTableMetadata(ctx, t.SourceSchema, t.SourceTable)
		if err != nil {
			return fmt.Errorf("GaussDB CDC metadata %s.%s: %w", t.SourceSchema, t.SourceTable, err)
		}
		if len(md.PrimaryKeys) == 0 && md.PrimaryKey == "" {
			return fmt.Errorf("GaussDB CDC table %s.%s requires a primary key for deterministic UPDATE/DELETE apply", t.SourceSchema, t.SourceTable)
		}
	}
	return nil
}

func (c *Connector) ValidateCDCSelection(ctx context.Context, tables []domain.TableMapping) error {
	if c.ds.Type == domain.DataSourceGaussDB {
		return c.ValidateGaussDBCDCSelection(ctx, tables)
	}
	return nil
}
