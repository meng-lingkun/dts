package postgresconnector

import (
	"context"
	"errors"
	"fmt"
	"qmigration/backend/internal/domain"
	"strconv"
	"strings"
)

// OpenGaussTransaction is one complete committed transaction decoded from
// openGauss mppdb_decoding. The source slot is only advanced after QMigration
// has atomically applied the target transaction and persisted OPENGAUSS_LSN.
type OpenGaussTransaction struct {
	XID       string
	CommitLSN string
	Events    []domain.CDCEvent
}

func openGaussSlotName(name string) (string, error) {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" || len(name) > 63 || !replicationSlotName.MatchString(name) {
		return "", errors.New("invalid openGauss logical replication slot name")
	}
	return name, nil
}

func openGaussDecodeQuery(function, slot string, maxChanges int, uptoLSN string, tables []string) (string, error) {
	slot, err := openGaussSlotName(slot)
	if err != nil {
		return "", err
	}
	if function != "pg_logical_slot_peek_changes" && function != "pg_logical_slot_get_changes" {
		return "", fmt.Errorf("unsupported openGauss logical function %q", function)
	}
	white, err := gaussDBWhiteTableList(tables)
	if err != nil {
		return "", errors.New(strings.Replace(err.Error(), "GaussDB", "openGauss", 1))
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
			return "", fmt.Errorf("invalid openGauss LSN %q: %w", uptoLSN, err)
		}
		upto = pgLiteral(norm)
	}
	// openGauss documents mppdb_decoding SQL functions with complete-transaction
	// stopping semantics. QMigration explicitly requests JSON text output,
	// selected-table filtering and XIDs so transaction identity remains stable.
	return "SELECT location::text,xid::text,data FROM " + function + "(" +
		pgLiteral(slot) + "," + upto + "," + strconv.Itoa(maxChanges) +
		",'skip-empty-xacts','1','include-xids','1','white-table-list'," + pgLiteral(white) + ")", nil
}

func ParseOpenGaussLogicalRows(rows *RawRows, slot string) ([]OpenGaussTransaction, error) {
	if rows == nil {
		return nil, errors.New("nil openGauss logical rows")
	}
	var out []OpenGaussTransaction
	var current *OpenGaussTransaction
	bytesInTxn := 0
	ordinal := 0
	for i, row := range rows.Rows {
		if len(row) < 3 {
			return nil, fmt.Errorf("openGauss logical row %d has %d columns", i, len(row))
		}
		lsn := normalizeGaussLSN(string(row[0]))
		xid := strings.TrimSpace(string(row[1]))
		data := strings.TrimSpace(string(row[2]))
		if data == "" {
			continue
		}
		if isGaussBegin(data) {
			if current != nil {
				return nil, fmt.Errorf("openGauss BEGIN %s before transaction %s committed", xid, current.XID)
			}
			current = &OpenGaussTransaction{XID: xid}
			bytesInTxn, ordinal = 0, 0
			continue
		}
		if isGaussCommit(data) {
			if current == nil {
				return nil, fmt.Errorf("openGauss COMMIT %s without BEGIN", xid)
			}
			if xid != "" && current.XID != "" && xid != current.XID {
				return nil, fmt.Errorf("openGauss COMMIT xid %s does not match BEGIN %s", xid, current.XID)
			}
			if _, err := parseReplicationLSN(lsn); err != nil {
				return nil, fmt.Errorf("invalid openGauss commit LSN %q: %w", lsn, err)
			}
			current.CommitLSN = lsn
			for j := range current.Events {
				current.Events[j].ID = fmt.Sprintf("opengauss:%s:%s:%d", current.XID, lsn, j)
				current.Events[j].PositionType = "OPENGAUSS_LSN"
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
			return nil, fmt.Errorf("openGauss logical change outside BEGIN/COMMIT at %s", lsn)
		}
		if xid != "" && current.XID != "" && xid != current.XID {
			return nil, fmt.Errorf("openGauss logical xid changed from %s to %s", current.XID, xid)
		}
		changes, err := parseGaussDBChange([]byte(data))
		if err != nil {
			return nil, fmt.Errorf("decode openGauss logical JSON at %s: %w", lsn, err)
		}
		bytesInTxn += len(data)
		if bytesInTxn > gaussDBMaxTransactionBytes {
			return nil, fmt.Errorf("openGauss transaction %s exceeds %d decoded bytes", current.XID, gaussDBMaxTransactionBytes)
		}
		for _, ch := range changes {
			ev, err := gaussDBChangeEvent(ch)
			if err != nil {
				return nil, fmt.Errorf("openGauss transaction %s: %w", current.XID, err)
			}
			ordinal++
			if ordinal > gaussDBMaxTransactionEvents {
				return nil, fmt.Errorf("openGauss transaction %s exceeds %d events", current.XID, gaussDBMaxTransactionEvents)
			}
			current.Events = append(current.Events, ev)
		}
	}
	if current != nil {
		return nil, fmt.Errorf("openGauss logical batch ended before COMMIT for xid %s", current.XID)
	}
	return out, nil
}

func (c *Connector) PeekOpenGaussTransactions(ctx context.Context, slot string, maxChanges int, tables []string) ([]OpenGaussTransaction, error) {
	if c.ds.Type != domain.DataSourceOpenGauss {
		return nil, errors.New("openGauss logical decoding requires an opengauss datasource")
	}
	if !openGaussCDCEnabled() {
		return nil, errors.New("openGauss logical CDC gate is not enabled")
	}
	q, err := openGaussDecodeQuery("pg_logical_slot_peek_changes", slot, maxChanges, "", tables)
	if err != nil {
		return nil, err
	}
	rows, err := c.QuerySQL(ctx, q)
	if err != nil {
		return nil, err
	}
	return ParseOpenGaussLogicalRows(rows, slot)
}

func (c *Connector) AcknowledgeOpenGaussTransaction(ctx context.Context, slot, commitLSN string, tables []string) error {
	if c.ds.Type != domain.DataSourceOpenGauss {
		return errors.New("openGauss ACK requires an opengauss datasource")
	}
	q, err := openGaussDecodeQuery("pg_logical_slot_get_changes", slot, 100000, commitLSN, tables)
	if err != nil {
		return err
	}
	rows, err := c.QuerySQL(ctx, q)
	if err != nil {
		return err
	}
	if len(rows.Rows) == 0 {
		return fmt.Errorf("openGauss slot %s did not advance to %s", slot, commitLSN)
	}
	txs, err := ParseOpenGaussLogicalRows(rows, slot)
	if err != nil {
		return fmt.Errorf("verify openGauss ACK: %w", err)
	}
	wanted := normalizeGaussLSN(commitLSN)
	for _, tx := range txs {
		if normalizeGaussLSN(tx.CommitLSN) == wanted {
			return nil
		}
	}
	return fmt.Errorf("openGauss slot %s ACK response did not contain COMMIT at %s", slot, wanted)
}

func (c *Connector) ValidateOpenGaussCDCSelection(ctx context.Context, tables []domain.TableMapping) error {
	if c.ds.Type != domain.DataSourceOpenGauss {
		return nil
	}
	if !openGaussCDCEnabled() {
		return errors.New("openGauss logical CDC gate is not enabled")
	}
	for _, t := range tables {
		md, err := c.GetTableMetadata(ctx, t.SourceSchema, t.SourceTable)
		if err != nil {
			return fmt.Errorf("openGauss CDC metadata %s.%s: %w", t.SourceSchema, t.SourceTable, err)
		}
		if len(md.PrimaryKeys) == 0 && md.PrimaryKey == "" {
			return fmt.Errorf("openGauss CDC table %s.%s requires a primary key for deterministic UPDATE/DELETE apply", t.SourceSchema, t.SourceTable)
		}
	}
	return nil
}
