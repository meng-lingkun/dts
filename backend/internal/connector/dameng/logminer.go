package damengconnector

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
)

// DamengCDCTransaction is the committed transaction unit consumed by the
// protocol-independent QMigration CDC runtime. DM's DBMS_LOGMNR only mines
// archived logs, so every checkpoint is an archived DM LSN that can be reopened
// after a Worker restart.
type DamengCDCTransaction struct {
	LSN         string
	TimestampMS int64
	Events      []domain.CDCEvent
}

type dmLogRecord struct {
	SCN       uint64
	CommitLSN uint64
	XID       string
	Operation int
	Schema    string
	Table     string
	RowID     string
	Timestamp int64
	SSN       int
}

type dmRowMutation struct {
	Schema    string
	Table     string
	RowID     string
	FirstSCN  uint64
	LastSCN   uint64
	FirstOp   int
	LastOp    int
	SeenOps   []int
	Timestamp int64
}

const (
	dmOpInsert      = 1
	dmOpDelete      = 2
	dmOpUpdate      = 3
	dmOpBatchUpdate = 4
	dmOpDDL         = 5
	dmOpUnsupported = 255
)

func parsePositiveLSN(raw, label string) (uint64, error) {
	n, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
	if err != nil || n == 0 {
		if err == nil {
			err = errors.New("must be greater than zero")
		}
		return 0, fmt.Errorf("invalid Dameng %s %q: %w", label, raw, err)
	}
	return n, nil
}

func dmSelectedPair(key string) (string, string, error) {
	key = strings.TrimSpace(key)
	i := strings.LastIndex(key, ".")
	if i <= 0 || i == len(key)-1 {
		return "", "", fmt.Errorf("Dameng CDC selected table %q must be schema.table", key)
	}
	return strings.TrimSpace(key[:i]), strings.TrimSpace(key[i+1:]), nil
}

func dmSQLString(v string) string { return "'" + strings.ReplaceAll(v, "'", "''") + "'" }

func dmTimeMillis(v string) int64 {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	layouts := []string{
		"2006-01-02 15:04:05.999999999 -07:00",
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05",
		time.RFC3339Nano,
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, v); err == nil {
			return t.UnixMilli()
		}
	}
	return 0
}

func dmBinaryType(col domain.ColumnInfo) bool {
	t := strings.ToLower(strings.TrimSpace(col.DataType))
	return strings.Contains(t, "blob") || strings.Contains(t, "binary") || t == "image" || t == "varbinary"
}

func dmCDCFields(cols []domain.ColumnInfo, row []connector.Value) ([]domain.CDCField, error) {
	if len(row) != len(cols) {
		return nil, fmt.Errorf("Dameng CDC snapshot returned %d columns; expected %d", len(row), len(cols))
	}
	out := make([]domain.CDCField, len(cols))
	for i, col := range cols {
		out[i].Column = col.Name
		if row[i].Null {
			out[i].Null = true
			continue
		}
		if dmBinaryType(col) {
			out[i].Encoding = "base64"
			out[i].Value = base64.StdEncoding.EncodeToString(row[i].Raw)
		} else {
			out[i].Value = string(row[i].Raw)
		}
	}
	return out, nil
}

func (c *Connector) damengCDCPrechecks(ctx context.Context) []domain.PrecheckItem {
	if !experimentalCDCEnabled() {
		return []domain.PrecheckItem{{Name: "Dameng LogMiner CDC gate", Level: domain.PrecheckFailed, Message: "QMIGRATION_EXPERIMENTAL_DAMENG_LOG_CDC=1 is required"}}
	}
	r, err := c.get(ctx)
	if err != nil {
		return []domain.PrecheckItem{{Name: "Dameng LogMiner SQL session", Level: domain.PrecheckFailed, Message: err.Error()}}
	}
	items := []domain.PrecheckItem{}
	checkParam := func(name string, acceptable func(string) bool, expected string) {
		rows, e := r.Query(ctx, `SELECT PARA_VALUE FROM V$DM_INI WHERE PARA_NAME=?`, name)
		if e != nil || len(rows) == 0 || len(rows[0]) == 0 {
			msg := "parameter is not readable"
			if e != nil {
				msg = e.Error()
			}
			items = append(items, domain.PrecheckItem{Name: "Dameng " + name, Level: domain.PrecheckFailed, Message: msg})
			return
		}
		v := cellString(rows[0], 0)
		level := domain.PrecheckPass
		msg := name + "=" + v
		if !acceptable(v) {
			level = domain.PrecheckFailed
			msg += "; required " + expected
		}
		items = append(items, domain.PrecheckItem{Name: "Dameng " + name, Level: level, Message: msg})
	}
	checkParam("ARCH_INI", func(v string) bool { return strings.TrimSpace(v) == "1" }, "1")
	checkParam("RLOG_APPEND_LOGIC", func(v string) bool {
		n, _ := strconv.Atoi(strings.TrimSpace(v))
		return n >= 1 && n <= 3
	}, "1, 2 or 3")
	// QMigration reconstructs complete before/after row images with AS OF SCN
	// instead of parsing SQL_REDO literals. Flashback must therefore be enabled.
	checkParam("ENABLE_FLASHBACK", func(v string) bool { return strings.TrimSpace(v) == "1" }, "1")
	checkParam("RLOG_LLOG_COMPRESS", func(v string) bool { return strings.TrimSpace(v) == "0" }, "0 for the RC25 qualified CDC contract")
	rows, e := r.Query(ctx, `SELECT MAX(NEXT_CHANGE#) FROM V$ARCHIVED_LOG WHERE DELETED='NO' AND ARCH_TYPE='LOCAL'`)
	if e != nil || len(rows) == 0 || len(rows[0]) == 0 || rows[0][0].Null || strings.TrimSpace(string(rows[0][0].Raw)) == "" {
		msg := "no readable local archived redo log"
		if e != nil {
			msg = e.Error()
		}
		items = append(items, domain.PrecheckItem{Name: "Dameng archived log", Level: domain.PrecheckFailed, Message: msg})
	} else {
		items = append(items, domain.PrecheckItem{Name: "Dameng archived log", Level: domain.PrecheckPass, Message: "V$ARCHIVED_LOG exposes a mineable LSN range"})
	}
	return items
}

// CurrentCDCPosition captures the newest fully archived LSN rather than CUR_LSN.
// DBMS_LOGMNR only analyzes archived logs; using an online LSN here would create
// an un-mineable restart point until the containing redo file is archived.
func (c *Connector) CurrentCDCPosition(ctx context.Context) (*domain.CDCPosition, error) {
	if !experimentalCDCEnabled() {
		return nil, errors.New("Dameng source CDC requires QMIGRATION_EXPERIMENTAL_DAMENG_NATIVE=1 and QMIGRATION_EXPERIMENTAL_DAMENG_LOG_CDC=1")
	}
	for _, item := range c.damengCDCPrechecks(ctx) {
		if item.Level == domain.PrecheckFailed {
			return nil, fmt.Errorf("Dameng CDC precheck %s failed: %s", item.Name, item.Message)
		}
	}
	r, err := c.get(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := r.Query(ctx, `SELECT MAX(NEXT_CHANGE#)-1 FROM V$ARCHIVED_LOG WHERE DELETED='NO' AND ARCH_TYPE='LOCAL'`)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 || len(rows[0]) == 0 || rows[0][0].Null {
		return nil, errors.New("Dameng source CDC requires at least one local archived log in V$ARCHIVED_LOG")
	}
	lsn, err := parsePositiveLSN(cellString(rows[0], 0), "archived LSN")
	if err != nil {
		return nil, err
	}
	return &domain.CDCPosition{DatabaseType: string(domain.DataSourceDameng), PositionType: "DM_LSN", PositionValue: strconv.FormatUint(lsn, 10), Resource: "DBMS_LOGMNR", SourceTimestampMS: time.Now().UnixMilli()}, nil
}

func validateArchiveCoverage(rows [][]connector.Value, from, to uint64) ([]string, error) {
	if to <= from {
		return nil, fmt.Errorf("Dameng archive window is not forward: %d -> %d", from, to)
	}
	type interval struct {
		name        string
		first, next uint64
	}
	ints := make([]interval, 0, len(rows))
	for _, row := range rows {
		if len(row) < 3 {
			continue
		}
		first, e1 := strconv.ParseUint(cellString(row, 1), 10, 64)
		next, e2 := strconv.ParseUint(cellString(row, 2), 10, 64)
		name := cellString(row, 0)
		if e1 != nil || e2 != nil || name == "" || next <= first {
			return nil, fmt.Errorf("invalid Dameng archived-log descriptor name=%q first=%q next=%q", name, cellString(row, 1), cellString(row, 2))
		}
		ints = append(ints, interval{name: name, first: first, next: next})
	}
	if len(ints) == 0 {
		return nil, fmt.Errorf("Dameng archived-log gap: no files cover LSN (%d,%d]", from, to)
	}
	sort.Slice(ints, func(i, j int) bool {
		if ints[i].first == ints[j].first {
			return ints[i].next < ints[j].next
		}
		return ints[i].first < ints[j].first
	})
	need := from + 1
	files := []string{}
	for _, in := range ints {
		if in.next <= need {
			continue
		}
		if in.first > need {
			return nil, fmt.Errorf("Dameng archived-log gap before LSN %d; next file %s starts at %d", need, in.name, in.first)
		}
		files = append(files, in.name)
		if in.next > need {
			need = in.next
		}
		if need > to {
			break
		}
	}
	if need <= to {
		return nil, fmt.Errorf("Dameng archived-log gap: coverage stops before LSN %d", to)
	}
	return files, nil
}

func coalesceDMMutations(records []dmLogRecord) ([]dmRowMutation, error) {
	by := map[string]*dmRowMutation{}
	order := []string{}
	for _, rec := range records {
		if rec.Operation != dmOpInsert && rec.Operation != dmOpDelete && rec.Operation != dmOpUpdate {
			return nil, fmt.Errorf("unsupported Dameng row operation code %d", rec.Operation)
		}
		if strings.TrimSpace(rec.RowID) == "" {
			return nil, fmt.Errorf("Dameng LogMiner %s.%s operation %d at LSN %d has no ROW_ID", rec.Schema, rec.Table, rec.Operation, rec.SCN)
		}
		key := strings.ToLower(rec.Schema + "." + rec.Table + "#" + rec.RowID)
		m := by[key]
		if m == nil {
			m = &dmRowMutation{Schema: rec.Schema, Table: rec.Table, RowID: rec.RowID, FirstSCN: rec.SCN, LastSCN: rec.SCN, FirstOp: rec.Operation, LastOp: rec.Operation, Timestamp: rec.Timestamp}
			by[key] = m
			order = append(order, key)
		} else {
			if m.LastOp == dmOpDelete {
				return nil, fmt.Errorf("Dameng CDC cannot losslessly coalesce operation %d after DELETE for %s.%s ROWID %s in one transaction", rec.Operation, rec.Schema, rec.Table, rec.RowID)
			}
			if rec.Operation == dmOpInsert {
				return nil, fmt.Errorf("Dameng CDC cannot losslessly coalesce a second INSERT for %s.%s ROWID %s in one transaction", rec.Schema, rec.Table, rec.RowID)
			}
			m.LastSCN = rec.SCN
			m.LastOp = rec.Operation
			if rec.Timestamp > m.Timestamp {
				m.Timestamp = rec.Timestamp
			}
		}
		m.SeenOps = append(m.SeenOps, rec.Operation)
	}
	out := make([]dmRowMutation, 0, len(order))
	for _, key := range order {
		out = append(out, *by[key])
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].FirstSCN < out[j].FirstSCN })
	return out, nil
}

func dmNetOperation(m dmRowMutation) (domain.CDCOperation, bool, error) {
	switch m.FirstOp {
	case dmOpInsert:
		switch m.LastOp {
		case dmOpInsert, dmOpUpdate:
			return domain.CDCInsert, true, nil
		case dmOpDelete:
			return "", false, nil // inserted and deleted in one source transaction: no net row effect
		}
	case dmOpUpdate:
		switch m.LastOp {
		case dmOpUpdate:
			return domain.CDCUpdate, true, nil
		case dmOpDelete:
			return domain.CDCDelete, true, nil
		}
	case dmOpDelete:
		if m.LastOp == dmOpDelete {
			return domain.CDCDelete, true, nil
		}
	}
	return "", false, fmt.Errorf("unsupported Dameng CDC operation sequence %v for %s.%s ROWID %s", m.SeenOps, m.Schema, m.Table, m.RowID)
}

func (c *Connector) readRowAtLSN(ctx context.Context, r dmRunner, md *domain.TableMetadata, lsn uint64, rowID string) ([]connector.Value, bool, error) {
	if lsn == 0 {
		return nil, false, errors.New("Dameng flashback LSN must be greater than zero")
	}
	cols := make([]string, len(md.Columns))
	for i, col := range md.Columns {
		cols[i] = selectExpr(col)
	}
	q := `SELECT ` + strings.Join(cols, ",") + ` FROM ` + qName(md.Schema, md.Name) + ` AS OF SCN ` + strconv.FormatUint(lsn, 10) + ` WHERE ROWID=? LIMIT 2`
	rows, err := r.Query(ctx, q, rowID)
	if err != nil {
		return nil, false, fmt.Errorf("Dameng flashback read %s.%s ROWID %s AS OF SCN %d: %w", md.Schema, md.Name, rowID, lsn, err)
	}
	if len(rows) > 1 {
		return nil, false, fmt.Errorf("Dameng ROWID %s matched multiple rows in %s.%s at LSN %d", rowID, md.Schema, md.Name, lsn)
	}
	if len(rows) == 0 {
		return nil, false, nil
	}
	return rows[0], true, nil
}

func (c *Connector) buildDMEvents(ctx context.Context, r dmRunner, commit uint64, mutations []dmRowMutation, metadata map[string]*domain.TableMetadata) ([]domain.CDCEvent, int64, error) {
	if commit <= 1 {
		return nil, 0, fmt.Errorf("Dameng CDC commit LSN %d is too small for before-image flashback", commit)
	}
	out := []domain.CDCEvent{}
	var ts int64
	for _, mut := range mutations {
		op, emit, err := dmNetOperation(mut)
		if err != nil {
			return nil, 0, err
		}
		if !emit {
			continue
		}
		key := strings.ToLower(mut.Schema + "." + mut.Table)
		md := metadata[key]
		if md == nil {
			md, err = c.GetTableMetadata(ctx, mut.Schema, mut.Table)
			if err != nil {
				return nil, 0, err
			}
			if len(md.PrimaryKeys) == 0 {
				return nil, 0, fmt.Errorf("Dameng CDC requires a deterministic primary key on %s.%s", mut.Schema, mut.Table)
			}
			metadata[key] = md
		}
		beforeRow, beforeFound, err := c.readRowAtLSN(ctx, r, md, commit-1, mut.RowID)
		if err != nil {
			return nil, 0, err
		}
		afterRow, afterFound, err := c.readRowAtLSN(ctx, r, md, commit, mut.RowID)
		if err != nil {
			return nil, 0, err
		}
		e := domain.CDCEvent{ID: fmt.Sprintf("dm:%d:%d:%s", commit, mut.FirstSCN, mut.RowID), Operation: op, SourceSchema: mut.Schema, SourceTable: mut.Table, PositionType: "DM_LSN", PositionValue: strconv.FormatUint(commit, 10), Resource: "DBMS_LOGMNR", SourceTimestampMS: mut.Timestamp}
		switch op {
		case domain.CDCInsert:
			if beforeFound || !afterFound {
				return nil, 0, fmt.Errorf("Dameng INSERT snapshot invariant failed for %s.%s ROWID %s at commit LSN %d (before=%t after=%t)", mut.Schema, mut.Table, mut.RowID, commit, beforeFound, afterFound)
			}
			e.After, err = dmCDCFields(md.Columns, afterRow)
		case domain.CDCUpdate:
			if !beforeFound || !afterFound {
				return nil, 0, fmt.Errorf("Dameng UPDATE snapshot invariant failed for %s.%s ROWID %s at commit LSN %d (before=%t after=%t)", mut.Schema, mut.Table, mut.RowID, commit, beforeFound, afterFound)
			}
			e.Before, err = dmCDCFields(md.Columns, beforeRow)
			if err == nil {
				e.After, err = dmCDCFields(md.Columns, afterRow)
			}
		case domain.CDCDelete:
			if !beforeFound || afterFound {
				return nil, 0, fmt.Errorf("Dameng DELETE snapshot invariant failed for %s.%s ROWID %s at commit LSN %d (before=%t after=%t)", mut.Schema, mut.Table, mut.RowID, commit, beforeFound, afterFound)
			}
			e.Before, err = dmCDCFields(md.Columns, beforeRow)
		}
		if err != nil {
			return nil, 0, err
		}
		out = append(out, e)
		if mut.Timestamp > ts {
			ts = mut.Timestamp
		}
	}
	return out, ts, nil
}

func (c *Connector) mineDMLogWindow(ctx context.Context, r dmRunner, mineFrom, ack, to uint64, selected map[string]bool) ([][]connector.Value, error) {
	if mineFrom == 0 || mineFrom > to {
		return nil, fmt.Errorf("invalid Dameng mining window %d..%d", mineFrom, to)
	}
	archiveRows, err := r.Query(ctx, `SELECT NAME,FIRST_CHANGE#,NEXT_CHANGE# FROM V$ARCHIVED_LOG WHERE DELETED='NO' AND ARCH_TYPE='LOCAL' AND NEXT_CHANGE#>? AND FIRST_CHANGE#<=? ORDER BY FIRST_CHANGE#`, mineFrom-1, to)
	if err != nil {
		return nil, err
	}
	files, err := validateArchiveCoverage(archiveRows, mineFrom-1, to)
	if err != nil {
		return nil, err
	}
	for i, name := range files {
		option := `DBMS_LOGMNR.ADDFILE`
		if i == 0 {
			option = `DBMS_LOGMNR."NEW"`
		}
		stmt := `BEGIN DBMS_LOGMNR.ADD_LOGFILE(` + dmSQLString(name) + `, OPTIONS=>` + option + `); END;`
		if _, err := r.Exec(ctx, stmt); err != nil {
			return nil, fmt.Errorf("Dameng DBMS_LOGMNR.ADD_LOGFILE %s: %w", name, err)
		}
	}
	startStmt := fmt.Sprintf(`BEGIN DBMS_LOGMNR.START_LOGMNR(STARTSCN=>%d, ENDSCN=>%d, OPTIONS=>82); END;`, mineFrom, to)
	if _, err := r.Exec(ctx, startStmt); err != nil {
		return nil, fmt.Errorf("Dameng DBMS_LOGMNR.START_LOGMNR: %w", err)
	}
	started := true
	defer func() {
		if started {
			_, _ = r.Exec(context.Background(), `BEGIN DBMS_LOGMNR.END_LOGMNR(); END;`)
		}
	}()

	filters := []string{}
	args := []any{ack, to}
	for key, on := range selected {
		if !on {
			continue
		}
		sch, tab, e := dmSelectedPair(key)
		if e != nil {
			return nil, e
		}
		filters = append(filters, `(UPPER(SEG_OWNER)=? AND UPPER(TABLE_NAME)=?)`)
		args = append(args, strings.ToUpper(sch), strings.ToUpper(tab))
	}
	if len(filters) == 0 {
		return nil, errors.New("Dameng LogMiner CDC has no enabled selected tables")
	}
	// COMMIT/XA_COMMIT rows are included regardless of table so QMigration can
	// discover a transaction whose selected DML started before the acknowledged
	// checkpoint. START_SCN then drives a second, earlier mining pass before any
	// transaction can be emitted.
	q := `SELECT START_SCN,SCN,COMMIT_SCN,XID,OPERATION_CODE,ROLL_BACK,SEG_OWNER,TABLE_NAME,ROW_ID,COMMIT_TIMESTAMP,SSN FROM V$LOGMNR_CONTENTS WHERE COMMIT_SCN>? AND COMMIT_SCN<=? AND (OPERATION_CODE IN (7,38) OR ((` + strings.Join(filters, " OR ") + `) AND OPERATION_CODE IN (1,2,3,4,5,255))) ORDER BY COMMIT_SCN,SCN,SSN`
	rows, err := r.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("query Dameng V$LOGMNR_CONTENTS: %w", err)
	}
	if _, err := r.Exec(ctx, `BEGIN DBMS_LOGMNR.END_LOGMNR(); END;`); err != nil {
		return nil, fmt.Errorf("Dameng DBMS_LOGMNR.END_LOGMNR: %w", err)
	}
	started = false
	return rows, nil
}

// ReadLogMinerTransactions mines one closed archived-LSN window. It deliberately
// does not parse SQL_REDO values into row data. Instead it uses LogMiner only as
// the transaction/ROWID index and reconstructs complete committed before/after
// images through DM flashback queries at commitLSN-1 and commitLSN. This keeps
// numeric/binary values on the normal prepared/native SQL data plane and avoids
// lossy literal parsing.
//
// Long transactions are handled by first including COMMIT/XA_COMMIT markers. If
// any transaction that commits after the durable checkpoint started earlier than
// the current mining window, QMigration reopens LogMiner from the minimum
// START_SCN. Missing retained archives then fail as a gap before any checkpoint
// can advance.
func (c *Connector) ReadLogMinerTransactions(ctx context.Context, fromRaw, toRaw string, selected map[string]bool) ([]DamengCDCTransaction, error) {
	if !experimentalCDCEnabled() {
		return nil, errors.New("Dameng LogMiner CDC experimental gate is disabled")
	}
	from, err := parsePositiveLSN(fromRaw, "start LSN")
	if err != nil {
		return nil, err
	}
	to, err := parsePositiveLSN(toRaw, "end LSN")
	if err != nil {
		return nil, err
	}
	if to <= from {
		return nil, fmt.Errorf("Dameng LogMiner end LSN %d must be after start %d", to, from)
	}
	if len(selected) == 0 {
		return nil, errors.New("Dameng LogMiner CDC requires selected tables")
	}
	r, err := c.get(ctx)
	if err != nil {
		return nil, err
	}
	if err := r.Begin(ctx); err != nil {
		return nil, fmt.Errorf("pin Dameng LogMiner session: %w", err)
	}
	defer r.Rollback(ctx)

	mineFrom := from + 1
	var rows [][]connector.Value
	for attempt := 0; attempt < 4; attempt++ {
		rows, err = c.mineDMLogWindow(ctx, r, mineFrom, from, to, selected)
		if err != nil {
			return nil, err
		}
		minStart := mineFrom
		for _, row := range rows {
			if len(row) < 11 {
				return nil, fmt.Errorf("Dameng V$LOGMNR_CONTENTS row has %d columns; expected 11", len(row))
			}
			op, e := strconv.Atoi(cellString(row, 4))
			if e != nil {
				return nil, fmt.Errorf("invalid Dameng LogMiner operation %q", cellString(row, 4))
			}
			if op != 7 && op != 38 {
				continue
			}
			startLSN, e := strconv.ParseUint(cellString(row, 0), 10, 64)
			if e != nil || startLSN == 0 {
				return nil, fmt.Errorf("Dameng committed transaction has invalid START_SCN %q", cellString(row, 0))
			}
			if startLSN < minStart {
				minStart = startLSN
			}
		}
		if minStart >= mineFrom {
			break
		}
		mineFrom = minStart
		if attempt == 3 {
			return nil, fmt.Errorf("Dameng LogMiner long-transaction rewind did not converge; earliest START_SCN=%d", mineFrom)
		}
	}

	type txKey struct {
		commit uint64
		xid    string
	}
	groups := map[txKey][]dmLogRecord{}
	keys := []txKey{}
	for _, row := range rows {
		if len(row) < 11 {
			return nil, fmt.Errorf("Dameng V$LOGMNR_CONTENTS row has %d columns; expected 11", len(row))
		}
		scn, e1 := strconv.ParseUint(cellString(row, 1), 10, 64)
		commit, e2 := strconv.ParseUint(cellString(row, 2), 10, 64)
		op, e3 := strconv.Atoi(cellString(row, 4))
		if e1 != nil || e2 != nil || e3 != nil || scn == 0 || commit == 0 {
			return nil, fmt.Errorf("invalid Dameng LogMiner row SCN=%q COMMIT_SCN=%q OP=%q", cellString(row, 1), cellString(row, 2), cellString(row, 4))
		}
		if op == 7 || op == 38 {
			continue
		}
		if cellString(row, 5) == "1" {
			continue
		}
		if op == dmOpBatchUpdate || op == dmOpDDL || op == dmOpUnsupported {
			return nil, fmt.Errorf("Dameng CDC selected table %s.%s contains unsupported LogMiner operation code %d at LSN %d; checkpoint is not advanced", cellString(row, 6), cellString(row, 7), op, scn)
		}
		xid := ""
		if !row[3].Null {
			xid = hex.EncodeToString(row[3].Raw)
		}
		if xid == "" {
			return nil, fmt.Errorf("Dameng LogMiner row at LSN %d has no XID", scn)
		}
		rec := dmLogRecord{SCN: scn, CommitLSN: commit, XID: xid, Operation: op, Schema: cellString(row, 6), Table: cellString(row, 7), RowID: cellString(row, 8), Timestamp: dmTimeMillis(cellString(row, 9)), SSN: cellInt(row, 10)}
		k := txKey{commit: commit, xid: xid}
		if _, ok := groups[k]; !ok {
			keys = append(keys, k)
		}
		groups[k] = append(groups[k], rec)
	}
	sort.SliceStable(keys, func(i, j int) bool {
		if keys[i].commit == keys[j].commit {
			return keys[i].xid < keys[j].xid
		}
		return keys[i].commit < keys[j].commit
	})
	metadata := map[string]*domain.TableMetadata{}
	out := make([]DamengCDCTransaction, 0, len(keys)+1)
	last := from
	// DM_LSN is the durable position key used by the shared CDC runtime. More than
	// one source XID may therefore not be emitted as separate QMigration
	// transactions at the same COMMIT_SCN: the second transaction would look like
	// a duplicate checkpoint after restart/apply. Aggregate every committed XID at
	// the same COMMIT_SCN into one target transaction. This preserves the complete
	// source net effect while keeping the durable position a monotonic numeric LSN.
	for i := 0; i < len(keys); {
		commit := keys[i].commit
		commitEvents := []domain.CDCEvent{}
		var commitTS int64
		xids := []string{}
		j := i
		for ; j < len(keys) && keys[j].commit == commit; j++ {
			k := keys[j]
			muts, err := coalesceDMMutations(groups[k])
			if err != nil {
				return nil, err
			}
			events, ts, err := c.buildDMEvents(ctx, r, k.commit, muts, metadata)
			if err != nil {
				return nil, err
			}
			commitEvents = append(commitEvents, events...)
			xids = append(xids, k.xid)
			if ts > commitTS {
				commitTS = ts
			}
		}
		pos := strconv.FormatUint(commit, 10)
		if len(commitEvents) == 0 {
			commitEvents = []domain.CDCEvent{{ID: fmt.Sprintf("dm-checkpoint:%d:%s", commit, strings.Join(xids, ",")), Operation: domain.CDCCheckpoint, PositionType: "DM_LSN", PositionValue: pos, Resource: "DBMS_LOGMNR", SourceTimestampMS: commitTS}}
		}
		for n := range commitEvents {
			commitEvents[n].PositionType = "DM_LSN"
			commitEvents[n].PositionValue = pos
			commitEvents[n].Resource = "DBMS_LOGMNR"
		}
		out = append(out, DamengCDCTransaction{LSN: pos, TimestampMS: commitTS, Events: commitEvents})
		last = commit
		i = j
	}
	if last < to {
		pos := strconv.FormatUint(to, 10)
		out = append(out, DamengCDCTransaction{LSN: pos, Events: []domain.CDCEvent{{ID: "dm-checkpoint:" + pos, Operation: domain.CDCCheckpoint, PositionType: "DM_LSN", PositionValue: pos, Resource: "DBMS_LOGMNR"}}})
	}
	if err := r.Rollback(ctx); err != nil {
		return nil, fmt.Errorf("release Dameng LogMiner pinned session: %w", err)
	}
	return out, nil
}

// OpenValidationSnapshot pins all normal Full/validation SELECTs to one DM LSN.
// DM documents SCN and LSN as equivalent operands for AS OF flashback queries.
func (c *Connector) OpenValidationSnapshot(_ context.Context, position domain.CDCPosition) (connector.DataConnector, error) {
	if !experimentalCDCEnabled() {
		return nil, errors.New("Dameng exact validation snapshot requires the experimental LogMiner CDC gate")
	}
	if !strings.EqualFold(strings.TrimSpace(position.PositionType), "DM_LSN") {
		return nil, fmt.Errorf("Dameng exact validation snapshot requires DM_LSN, got %q", position.PositionType)
	}
	lsn, err := parsePositiveLSN(position.PositionValue, "validation LSN")
	if err != nil {
		return nil, err
	}
	return &Connector{ds: c.ds, validationLSN: strconv.FormatUint(lsn, 10)}, nil
}

func (c *Connector) ValidateCDCSelection(ctx context.Context, mappings []domain.TableMapping) error {
	if !experimentalCDCEnabled() {
		return errors.New("Dameng source CDC experimental gate is disabled")
	}
	if len(mappings) == 0 {
		return errors.New("Dameng CDC requires selected tables")
	}
	for _, item := range c.damengCDCPrechecks(ctx) {
		if item.Level == domain.PrecheckFailed {
			return fmt.Errorf("%s: %s", item.Name, item.Message)
		}
	}
	for _, mapping := range mappings {
		md, err := c.GetTableMetadata(ctx, mapping.SourceSchema, mapping.SourceTable)
		if err != nil {
			return err
		}
		if len(md.PrimaryKeys) == 0 {
			return fmt.Errorf("Dameng CDC table %s.%s has no deterministic primary key", mapping.SourceSchema, mapping.SourceTable)
		}
	}
	return nil
}

var _ connector.CDCSource = (*Connector)(nil)
var _ connector.CDCSelectionValidator = (*Connector)(nil)
var _ connector.ValidationSnapshotConnector = (*Connector)(nil)
