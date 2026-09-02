package oracleconnector

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
)

type OracleLogMinerRecord struct {
	SCN       uint64
	CommitSCN uint64
	XID       string
	Operation string
	Schema    string
	Table     string
	RowID     string
	SQLRedo   string
	SQLUndo   string
	RSID      string
	SSN       string
	CSF       bool
	Info      string
	Status    int64
}

type OracleCDCTransaction struct {
	SCN         string
	TimestampMS int64
	Events      []domain.CDCEvent
}

func (c *Connector) CurrentCDCPosition(ctx context.Context) (*domain.CDCPosition, error) {
	captured := time.Now().UnixMilli()
	rr, err := c.querySQL(ctx, `SELECT CURRENT_SCN FROM V$DATABASE`, 1, 1)
	if err != nil {
		return nil, err
	}
	if len(rr.Rows) == 0 || len(rr.Rows[0]) == 0 {
		return nil, errors.New("Oracle V$DATABASE.CURRENT_SCN returned no row")
	}
	scn := strings.TrimSpace(oracleCellString(rr.Rows[0], 0))
	if scn == "" {
		return nil, errors.New("Oracle CURRENT_SCN is empty")
	}
	return &domain.CDCPosition{DatabaseType: string(domain.DataSourceOracle), PositionType: "ORACLE_SCN", PositionValue: scn, Resource: "DBMS_LOGMNR", SourceTimestampMS: captured}, nil
}

func (c *Connector) ValidateCDCSelection(ctx context.Context, mappings []domain.TableMapping) error {
	if !experimentalOracleLogMinerCDCEnabled() {
		return errors.New("Oracle LogMiner CDC requires QMIGRATION_EXPERIMENTAL_ORACLE_LOGMINER_CDC=1")
	}
	if len(mappings) == 0 {
		return errors.New("Oracle LogMiner CDC requires selected tables")
	}
	for _, m := range mappings {
		if strings.TrimSpace(m.SourceTable) == "" {
			continue
		}
		if _, err := c.GetTableMetadata(ctx, m.SourceSchema, m.SourceTable); err != nil {
			return fmt.Errorf("Oracle CDC metadata %s.%s: %w", m.SourceSchema, m.SourceTable, err)
		}
	}
	return nil
}

func (c *Connector) startLogMiner(ctx context.Context, from, to uint64) error {
	if from >= to {
		return nil
	}
	direct := fmt.Sprintf(`BEGIN DBMS_LOGMNR.START_LOGMNR(STARTSCN=>%d,ENDSCN=>%d,OPTIONS=>DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG+DBMS_LOGMNR.COMMITTED_DATA_ONLY+DBMS_LOGMNR.NO_SQL_DELIMITER); END;`, from+1, to)
	if _, err := c.execSQL(ctx, direct); err == nil {
		return nil
	} else if !strings.Contains(strings.ToUpper(err.Error()), "ORA-01291") {
		return err
	}
	// On traditional 19c deployments START_LOGMNR can require an explicit
	// logfile list. Build the smallest archive+online set covering the window.
	_, _ = c.execSQL(ctx, `BEGIN DBMS_LOGMNR.END_LOGMNR; EXCEPTION WHEN OTHERS THEN NULL; END;`)
	q := fmt.Sprintf(`SELECT NAME FROM V$ARCHIVED_LOG WHERE NAME IS NOT NULL AND FIRST_CHANGE#<=%d AND NEXT_CHANGE#>%d AND ARCHIVED='YES' UNION SELECT LF.MEMBER FROM V$LOG L JOIN V$LOGFILE LF ON LF.GROUP#=L.GROUP# WHERE L.FIRST_CHANGE#<=%d ORDER BY 1`, to, from, to)
	rr, e := c.querySQL(ctx, q, 128, 4096)
	if e != nil {
		return fmt.Errorf("discover Oracle redo logs: %w", e)
	}
	files := []string{}
	for _, row := range rr.Rows {
		if v := oracleCellString(row, 0); v != "" {
			files = append(files, v)
		}
	}
	if len(files) == 0 {
		return errors.New("Oracle LogMiner found no redo/archive log files covering requested SCN window")
	}
	for i, f := range files {
		opt := "DBMS_LOGMNR.ADDFILE"
		if i == 0 {
			opt = "DBMS_LOGMNR.NEW"
		}
		stmt := `BEGIN DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME=>` + oracleString(f) + `,OPTIONS=>` + opt + `); END;`
		if _, e = c.execSQL(ctx, stmt); e != nil {
			return fmt.Errorf("Oracle LogMiner add logfile %s: %w", f, e)
		}
	}
	if _, e = c.execSQL(ctx, direct); e != nil {
		return fmt.Errorf("Oracle LogMiner start: %w", e)
	}
	return nil
}

func (c *Connector) endLogMiner(ctx context.Context) {
	_, _ = c.execSQL(ctx, `BEGIN DBMS_LOGMNR.END_LOGMNR; EXCEPTION WHEN OTHERS THEN NULL; END;`)
}

func selectedOraclePredicate(selected map[string]bool) string {
	if len(selected) == 0 {
		return "1=0"
	}
	keys := make([]string, 0, len(selected))
	for k, v := range selected {
		if v {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	parts := []string{}
	for _, k := range keys {
		dot := strings.Index(k, ".")
		if dot <= 0 || dot == len(k)-1 {
			continue
		}
		s, t := strings.ToUpper(strings.TrimSpace(k[:dot])), strings.ToUpper(strings.TrimSpace(k[dot+1:]))
		parts = append(parts, `(SEG_OWNER=`+oracleString(s)+` AND TABLE_NAME=`+oracleString(t)+`)`)
	}
	if len(parts) == 0 {
		return "1=0"
	}
	if len(parts) == 1 {
		return parts[0]
	}
	return "(" + strings.Join(parts, " OR ") + ")"
}

func (c *Connector) readLogMinerRecords(ctx context.Context, from, to uint64, selected map[string]bool) ([]OracleLogMinerRecord, error) {
	if err := c.startLogMiner(ctx, from, to); err != nil {
		return nil, err
	}
	defer c.endLogMiner(ctx)
	q := `SELECT SCN,COMMIT_SCN,(XIDUSN||'.'||XIDSLT||'.'||XIDSQN),OPERATION,SEG_OWNER,TABLE_NAME,ROW_ID,SQL_REDO,SQL_UNDO,RS_ID,SSN,CSF,INFO,STATUS FROM V$LOGMNR_CONTENTS WHERE COMMIT_SCN>` + strconv.FormatUint(from, 10) + ` AND COMMIT_SCN<=` + strconv.FormatUint(to, 10) + ` AND OPERATION IN ('INSERT','UPDATE','DELETE','DDL') AND ` + selectedOraclePredicate(selected) + ` ORDER BY COMMIT_SCN,SCN,XIDUSN,XIDSLT,XIDSQN,RS_ID,SSN`
	rr, err := c.querySQL(ctx, q, 256, 65536)
	if err != nil {
		return nil, err
	}
	fragments := make([]OracleLogMinerRecord, 0, len(rr.Rows))
	for _, row := range rr.Rows {
		if len(row) < 14 {
			continue
		}
		scn, _ := strconv.ParseUint(strings.TrimSpace(oracleCellString(row, 0)), 10, 64)
		commit, _ := strconv.ParseUint(strings.TrimSpace(oracleCellString(row, 1)), 10, 64)
		if commit == 0 {
			continue
		}
		fragments = append(fragments, OracleLogMinerRecord{
			SCN: scn, CommitSCN: commit, XID: oracleCellString(row, 2), Operation: strings.ToUpper(oracleCellString(row, 3)),
			Schema: oracleCellString(row, 4), Table: oracleCellString(row, 5), RowID: oracleCellString(row, 6),
			SQLRedo: oracleCellString(row, 7), SQLUndo: oracleCellString(row, 8), RSID: oracleCellString(row, 9), SSN: oracleCellString(row, 10),
			CSF: oracleCellInt64(row, 11) == 1, Info: oracleCellString(row, 12), Status: oracleCellInt64(row, 13),
		})
	}
	return coalesceOracleLogMinerFragments(fragments), nil
}

func coalesceOracleLogMinerFragments(in []OracleLogMinerRecord) []OracleLogMinerRecord {
	out := make([]OracleLogMinerRecord, 0, len(in))
	for i := 0; i < len(in); i++ {
		cur := in[i]
		for cur.CSF && i+1 < len(in) {
			next := in[i+1]
			// Oracle identifies one logical row-change by RS_ID + SSN. Fail
			// closed at the fragment boundary rather than accidentally joining
			// SQL from a different redo record.
			if cur.RSID == "" || next.RSID != cur.RSID || next.SSN != cur.SSN {
				break
			}
			cur.SQLRedo += next.SQLRedo
			cur.SQLUndo += next.SQLUndo
			cur.CSF = next.CSF
			i++
		}
		out = append(out, cur)
	}
	return out
}

func oracleLogMinerUserDDL(info string) bool {
	normalized := strings.Join(strings.Fields(strings.ReplaceAll(strings.ToUpper(info), "_", " ")), " ")
	return normalized == "USER DDL"
}

func oracleBinaryColumn(col domain.ColumnInfo) bool {
	dt := strings.ToLower(col.DataType)
	return strings.Contains(dt, "blob") || strings.Contains(dt, "binary") || dt == "raw" || dt == "long raw" || dt == "bytea"
}
func oracleFields(cols []domain.ColumnInfo, vals []connector.Value) []domain.CDCField {
	out := make([]domain.CDCField, 0, len(cols))
	for i, col := range cols {
		f := domain.CDCField{Column: col.Name}
		if i >= len(vals) || vals[i].Null {
			f.Null = true
		} else if oracleBinaryColumn(col) {
			f.Encoding = "base64"
			f.Value = base64.StdEncoding.EncodeToString(vals[i].Raw)
		} else {
			f.Value = string(vals[i].Raw)
		}
		out = append(out, f)
	}
	return out
}

func (c *Connector) flashbackRow(ctx context.Context, meta *domain.TableMetadata, rowID string, scn uint64) ([]connector.Value, bool, error) {
	if meta == nil || len(meta.Columns) == 0 || strings.TrimSpace(rowID) == "" || scn == 0 {
		return nil, false, nil
	}
	cols := make([]string, len(meta.Columns))
	for i, col := range meta.Columns {
		cols[i] = oracleIdent(col.Name)
	}
	q := `SELECT ` + strings.Join(cols, ",") + ` FROM ` + oracleQualified(meta.Schema, meta.Name) + ` AS OF SCN ` + strconv.FormatUint(scn, 10) + ` WHERE ROWID=` + oracleString(rowID) + ` AND ROWNUM <= 1`
	rr, err := c.querySQL(ctx, q, 1, 1)
	if err != nil {
		return nil, false, err
	}
	if len(rr.Rows) == 0 {
		return nil, false, nil
	}
	vals, err := oracleRowValues(rr.Rows[0])
	return vals, true, err
}

func (c *Connector) ReadLogMinerTransactions(ctx context.Context, fromSCN, toSCN string, selected map[string]bool) ([]OracleCDCTransaction, error) {
	from, err := strconv.ParseUint(strings.TrimSpace(fromSCN), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid Oracle start SCN %q: %w", fromSCN, err)
	}
	to, err := strconv.ParseUint(strings.TrimSpace(toSCN), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid Oracle end SCN %q: %w", toSCN, err)
	}
	if to <= from {
		return nil, nil
	}
	records, err := c.readLogMinerRecords(ctx, from, to, selected)
	if err != nil {
		return nil, err
	}
	metaCache := map[string]*domain.TableMetadata{}
	txByKey := map[string]*OracleCDCTransaction{}
	order := []string{}
	for _, rec := range records {
		key := rec.XID + "@" + strconv.FormatUint(rec.CommitSCN, 10)
		tx := txByKey[key]
		if tx == nil {
			tx = &OracleCDCTransaction{SCN: strconv.FormatUint(rec.CommitSCN, 10)}
			txByKey[key] = tx
			order = append(order, key)
		}
		event := domain.CDCEvent{ID: "oracle:" + key + ":" + strconv.FormatUint(rec.SCN, 10) + ":" + rec.RowID, SourceSchema: rec.Schema, SourceTable: rec.Table, PositionType: "ORACLE_SCN", PositionValue: tx.SCN, Resource: "DBMS_LOGMNR"}
		if rec.Operation == "DDL" {
			// Oracle emits internal DDL alongside user-issued DDL. Replaying
			// internal statements can corrupt a target; only executable user DDL
			// is allowed into the CDC stream.
			if !oracleLogMinerUserDDL(rec.Info) {
				continue
			}
			if rec.Status != 0 {
				return nil, fmt.Errorf("Oracle LogMiner USER DDL at SCN %d is not executable (STATUS=%d)", rec.SCN, rec.Status)
			}
			event.Operation = domain.CDCDDL
			event.SQL = rec.SQLRedo
			tx.Events = append(tx.Events, event)
			continue
		}
		mk := strings.ToUpper(rec.Schema) + "." + strings.ToUpper(rec.Table)
		meta := metaCache[mk]
		if meta == nil {
			meta, err = c.GetTableMetadata(ctx, rec.Schema, rec.Table)
			if err != nil {
				return nil, fmt.Errorf("Oracle CDC metadata %s: %w", mk, err)
			}
			metaCache[mk] = meta
		}
		switch rec.Operation {
		case "INSERT":
			event.Operation = domain.CDCInsert
			vals, ok, e := c.flashbackRow(ctx, meta, rec.RowID, rec.CommitSCN)
			if e != nil {
				return nil, e
			}
			if !ok {
				return nil, fmt.Errorf("Oracle LogMiner INSERT %s rowid=%s has no flashback after-image at SCN %d", mk, rec.RowID, rec.CommitSCN)
			}
			event.After = oracleFields(meta.Columns, vals)
		case "UPDATE":
			event.Operation = domain.CDCUpdate
			after, ok, e := c.flashbackRow(ctx, meta, rec.RowID, rec.CommitSCN)
			if e != nil {
				return nil, e
			}
			if !ok {
				return nil, fmt.Errorf("Oracle LogMiner UPDATE %s rowid=%s has no after-image", mk, rec.RowID)
			}
			event.After = oracleFields(meta.Columns, after)
			if rec.CommitSCN > 1 {
				if before, found, e := c.flashbackRow(ctx, meta, rec.RowID, rec.CommitSCN-1); e != nil {
					return nil, e
				} else if found {
					event.Before = oracleFields(meta.Columns, before)
				}
			}
		case "DELETE":
			event.Operation = domain.CDCDelete
			if rec.CommitSCN <= 1 {
				return nil, fmt.Errorf("Oracle DELETE has invalid commit SCN %d", rec.CommitSCN)
			}
			before, ok, e := c.flashbackRow(ctx, meta, rec.RowID, rec.CommitSCN-1)
			if e != nil {
				return nil, e
			}
			if !ok {
				return nil, fmt.Errorf("Oracle LogMiner DELETE %s rowid=%s has no flashback before-image at SCN %d", mk, rec.RowID, rec.CommitSCN-1)
			}
			event.Before = oracleFields(meta.Columns, before)
		default:
			continue
		}
		tx.Events = append(tx.Events, event)
	}
	out := make([]OracleCDCTransaction, 0, len(order))
	for _, k := range order {
		if tx := txByKey[k]; tx != nil && len(tx.Events) > 0 {
			out = append(out, *tx)
		}
	}
	if len(out) == 0 && to > from {
		out = append(out, OracleCDCTransaction{SCN: strconv.FormatUint(to, 10), Events: []domain.CDCEvent{{ID: "oracle-checkpoint:" + strconv.FormatUint(to, 10), Operation: domain.CDCCheckpoint, PositionType: "ORACLE_SCN", PositionValue: strconv.FormatUint(to, 10), Resource: "DBMS_LOGMNR"}}})
	}
	return out, nil
}

var _ connector.CDCSource = (*Connector)(nil)
var _ connector.CDCSelectionValidator = (*Connector)(nil)
