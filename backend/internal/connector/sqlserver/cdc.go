package sqlserverconnector

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
	"sort"
	"strconv"
	"strings"
	"time"
)

// SQLServer CDC integration deliberately uses SQL Server's own CDC LSN/change
// tables through QMigration's native TDS client. No SSIS/Debezium/third-party
// runtime is involved. SQL Server Agent still has to maintain capture jobs on
// the source, which is a server prerequisite rather than a migration engine.

type CDCCapture struct {
	Schema   string
	Table    string
	Instance string
	Columns  []domain.ColumnInfo
}

type CDCChange struct {
	StartLSN    string
	SeqVal      string
	Operation   int
	CommandID   int
	Schema      string
	Table       string
	Columns     []domain.ColumnInfo
	Values      []connector.Value
	TimestampMS int64
	Capture     string
}

func sqlServerCDCEnabled() bool {
	if !experimentalFullEnabled() {
		return false
	}
	v := strings.ToLower(strings.TrimSpace(os.Getenv("QMIGRATION_EXPERIMENTAL_SQLSERVER_CDC")))
	return v == "1" || v == "true" || v == "yes" || v == "on"
}

func normalizeLSN(v string) (string, error) {
	v = strings.TrimSpace(v)
	if len(v) < 4 || !strings.HasPrefix(strings.ToLower(v), "0x") {
		return "", fmt.Errorf("invalid SQL Server LSN %q", v)
	}
	hex := v[2:]
	if len(hex)%2 != 0 || len(hex) > 20 {
		return "", fmt.Errorf("invalid SQL Server LSN length %q", v)
	}
	for _, r := range hex {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')) {
			return "", fmt.Errorf("invalid SQL Server LSN %q", v)
		}
	}
	return "0x" + strings.ToUpper(hex), nil
}

func (c *Connector) CurrentCDCPosition(ctx context.Context) (*domain.CDCPosition, error) {
	if !sqlServerCDCEnabled() {
		return nil, errors.New("QMigration native SQL Server CDC is experimental; set QMIGRATION_EXPERIMENTAL_SQLSERVER_CDC=1 after source CDC is enabled")
	}
	p, err := c.get(ctx)
	if err != nil {
		return nil, err
	}
	enabled, _, _, err := p.query(ctx, "SELECT CONVERT(nvarchar(10),is_cdc_enabled) FROM sys.databases WHERE name=DB_NAME()")
	if err != nil {
		return nil, err
	}
	if len(enabled) == 0 || len(enabled[0]) == 0 || strings.TrimSpace(string(enabled[0][0])) != "1" {
		return nil, errors.New("SQL Server CDC is not enabled for the source database; run sys.sp_cdc_enable_db and enable selected tables")
	}
	rows, _, _, err := p.query(ctx, "SELECT CONVERT(nvarchar(34),sys.fn_varbintohexstr(sys.fn_cdc_get_max_lsn()))")
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 || len(rows[0]) == 0 || len(rows[0][0]) == 0 {
		return nil, errors.New("SQL Server CDC max LSN is unavailable")
	}
	lsn, err := normalizeLSN(string(rows[0][0]))
	if err != nil {
		return nil, err
	}
	return &domain.CDCPosition{DatabaseType: string(domain.DataSourceSQLServer), PositionType: "SQLSERVER_LSN", PositionValue: lsn, SourceTimestampMS: time.Now().UnixMilli()}, nil
}

func (c *Connector) CDCRetentionMinutes(ctx context.Context) (int64, error) {
	p, err := c.get(ctx)
	if err != nil {
		return 0, err
	}
	q := `SELECT CONVERT(nvarchar(40),retention) FROM msdb.dbo.cdc_jobs WHERE database_id=DB_ID() AND job_type=N'cleanup'`
	rows, _, _, err := p.query(ctx, q)
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 || len(rows[0]) == 0 {
		return 0, errors.New("SQL Server CDC cleanup job retention configuration was not found")
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(rows[0][0])), 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("invalid SQL Server CDC retention value %q", string(rows[0][0]))
	}
	return n, nil
}

func sqlServerMinimumRetentionMinutes() int64 {
	v := strings.TrimSpace(os.Getenv("QMIGRATION_SQLSERVER_CDC_MIN_RETENTION_MINUTES"))
	if v == "" {
		return 4320
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		return 4320
	}
	return n
}

func (c *Connector) DiscoverCDCCaptures(ctx context.Context, selected map[string]bool) ([]CDCCapture, error) {
	p, err := c.get(ctx)
	if err != nil {
		return nil, err
	}
	q := `SELECT CONVERT(nvarchar(4000),s.name),CONVERT(nvarchar(4000),t.name),CONVERT(nvarchar(4000),ct.capture_instance) FROM cdc.change_tables ct JOIN sys.tables t ON t.object_id=ct.source_object_id JOIN sys.schemas s ON s.schema_id=t.schema_id ORDER BY s.name,t.name`
	rows, _, _, err := p.query(ctx, q)
	if err != nil {
		return nil, err
	}
	out := []CDCCapture{}
	for _, r := range rows {
		if len(r) < 3 {
			continue
		}
		schema, table, inst := string(r[0]), string(r[1]), string(r[2])
		key := strings.ToLower(schema + "." + table)
		if len(selected) > 0 && !selected[key] {
			continue
		}
		meta, e := c.GetTableMetadata(ctx, schema, table)
		if e != nil {
			return nil, e
		}
		out = append(out, CDCCapture{Schema: schema, Table: table, Instance: inst, Columns: meta.Columns})
	}
	if len(selected) > 0 {
		found := map[string]bool{}
		for _, x := range out {
			found[strings.ToLower(x.Schema+"."+x.Table)] = true
		}
		missing := []string{}
		for k := range selected {
			if !found[strings.ToLower(k)] {
				missing = append(missing, k)
			}
		}
		if len(missing) > 0 {
			sort.Strings(missing)
			return nil, fmt.Errorf("SQL Server CDC capture instance missing for selected tables: %s", strings.Join(missing, ","))
		}
	}
	return out, nil
}

func compareLSN(a, b string) (int, error) {
	na, err := normalizeLSN(a)
	if err != nil {
		return 0, err
	}
	nb, err := normalizeLSN(b)
	if err != nil {
		return 0, err
	}
	if len(na) != len(nb) {
		// SQL Server CDC LSNs are binary(10); left-pad shorter test/legacy values.
		ha, hb := na[2:], nb[2:]
		for len(ha) < 20 {
			ha = "0" + ha
		}
		for len(hb) < 20 {
			hb = "0" + hb
		}
		na, nb = "0x"+ha, "0x"+hb
	}
	switch {
	case na < nb:
		return -1, nil
	case na > nb:
		return 1, nil
	default:
		return 0, nil
	}
}

func (c *Connector) ValidateCDCStart(ctx context.Context, captures []CDCCapture, acknowledged string) error {
	ack, err := normalizeLSN(acknowledged)
	if err != nil {
		return err
	}
	p, err := c.get(ctx)
	if err != nil {
		return err
	}
	for _, cap := range captures {
		q := "SELECT CONVERT(nvarchar(34),sys.fn_varbintohexstr(sys.fn_cdc_get_min_lsn(" + qStr(cap.Instance) + ")))"
		rows, _, _, err := p.query(ctx, q)
		if err != nil {
			return err
		}
		if len(rows) == 0 || len(rows[0]) == 0 || len(rows[0][0]) == 0 {
			continue
		}
		minLSN, err := normalizeLSN(string(rows[0][0]))
		if err != nil {
			return err
		}
		cmp, err := compareLSN(ack, minLSN)
		if err != nil {
			return err
		}
		if cmp < 0 {
			return fmt.Errorf("SQL Server CDC retention gap for %s.%s: acknowledged LSN %s is older than minimum retained LSN %s; resnapshot is required", cap.Schema, cap.Table, ack, minLSN)
		}
	}
	return nil
}

func (c *Connector) NextCDCWindow(ctx context.Context, acknowledged string, maxTransactions int) (fromLSN, toLSN string, empty bool, err error) {
	ack, err := normalizeLSN(acknowledged)
	if err != nil {
		return "", "", false, err
	}
	if maxTransactions <= 0 {
		maxTransactions = 256
	}
	p, err := c.get(ctx)
	if err != nil {
		return "", "", false, err
	}
	q := fmt.Sprintf(`DECLARE @from binary(10)=sys.fn_cdc_increment_lsn(%s); SELECT TOP (%d) CONVERT(nvarchar(34),sys.fn_varbintohexstr(start_lsn)) FROM cdc.lsn_time_mapping WHERE start_lsn>=@from ORDER BY start_lsn`, ack, maxTransactions)
	rows, _, _, err := p.query(ctx, q)
	if err != nil {
		return "", "", false, err
	}
	if len(rows) == 0 {
		return "", "", true, nil
	}
	from, err := normalizeLSN(string(rows[0][0]))
	if err != nil {
		return "", "", false, err
	}
	to, err := normalizeLSN(string(rows[len(rows)-1][0]))
	if err != nil {
		return "", "", false, err
	}
	return from, to, false, nil
}

func cdcSelectExpr(col domain.ColumnInfo) string {
	q := qIdentSafe(col.Name)
	if isBinaryType(col.DataType) {
		return "CONVERT(varbinary(max)," + q + ")"
	}
	return "CONVERT(nvarchar(max)," + q + ")"
}

func (c *Connector) ReadCDCChanges(ctx context.Context, cap CDCCapture, fromLSN, toLSN string) ([]CDCChange, error) {
	from, err := normalizeLSN(fromLSN)
	if err != nil {
		return nil, err
	}
	to, err := normalizeLSN(toLSN)
	if err != nil {
		return nil, err
	}
	p, err := c.get(ctx)
	if err != nil {
		return nil, err
	}
	fn := qIdentSafe("cdc") + "." + qIdentSafe("fn_cdc_get_all_changes_"+cap.Instance)
	parts := []string{
		"CONVERT(nvarchar(34),sys.fn_varbintohexstr(__$start_lsn))",
		"CONVERT(nvarchar(34),sys.fn_varbintohexstr(__$seqval))",
		"CONVERT(nvarchar(10),__$operation)",
		"CONVERT(nvarchar(10),__$command_id)",
		"CONVERT(nvarchar(40),sys.fn_cdc_map_lsn_to_time(__$start_lsn),127)",
	}
	for _, col := range cap.Columns {
		parts = append(parts, cdcSelectExpr(col))
	}
	q := fmt.Sprintf("SELECT %s FROM %s(%s,%s,N'all update old') ORDER BY __$start_lsn,__$seqval,__$operation", strings.Join(parts, ","), fn, from, to)
	rows, nulls, _, err := p.query(ctx, q)
	if err != nil {
		return nil, err
	}
	out := make([]CDCChange, 0, len(rows))
	for ri, row := range rows {
		if len(row) < 5+len(cap.Columns) {
			return nil, fmt.Errorf("SQL Server CDC row column mismatch for %s.%s", cap.Schema, cap.Table)
		}
		op, _ := strconv.Atoi(strings.TrimSpace(string(row[2])))
		cmd, _ := strconv.Atoi(strings.TrimSpace(string(row[3])))
		var ts int64
		if t, e := time.Parse("2006-01-02T15:04:05.9999999", strings.TrimSpace(string(row[4]))); e == nil {
			ts = t.UnixMilli()
		} else if t, e := time.Parse(time.RFC3339Nano, strings.TrimSpace(string(row[4]))); e == nil {
			ts = t.UnixMilli()
		}
		vals := make([]connector.Value, len(cap.Columns))
		for i := range cap.Columns {
			vals[i] = connector.Value{Null: nulls[ri][5+i], Raw: append([]byte(nil), row[5+i]...)}
		}
		start, _ := normalizeLSN(string(row[0]))
		seq, _ := normalizeLSN(string(row[1]))
		out = append(out, CDCChange{StartLSN: start, SeqVal: seq, Operation: op, CommandID: cmd, Schema: cap.Schema, Table: cap.Table, Columns: cap.Columns, Values: vals, TimestampMS: ts, Capture: cap.Instance})
	}
	return out, nil
}

func fieldsFromValues(cols []domain.ColumnInfo, vals []connector.Value) []domain.CDCField {
	out := make([]domain.CDCField, 0, len(cols))
	for i, col := range cols {
		f := domain.CDCField{Column: col.Name}
		if i >= len(vals) || vals[i].Null {
			f.Null = true
		} else if isBinaryType(col.DataType) {
			f.Encoding = "base64"
			f.Value = base64.StdEncoding.EncodeToString(vals[i].Raw)
		} else {
			f.Value = string(vals[i].Raw)
		}
		out = append(out, f)
	}
	return out
}

func ChangesToTransactions(changes []CDCChange) ([]SQLServerCDCTransaction, error) {
	sort.SliceStable(changes, func(i, j int) bool {
		if changes[i].StartLSN != changes[j].StartLSN {
			return changes[i].StartLSN < changes[j].StartLSN
		}
		if changes[i].SeqVal != changes[j].SeqVal {
			return changes[i].SeqVal < changes[j].SeqVal
		}
		return changes[i].Operation < changes[j].Operation
	})
	byLSN := map[string]*SQLServerCDCTransaction{}
	order := []string{}
	oldUpdate := map[string]CDCChange{}
	for _, ch := range changes {
		tx := byLSN[ch.StartLSN]
		if tx == nil {
			tx = &SQLServerCDCTransaction{LSN: ch.StartLSN, TimestampMS: ch.TimestampMS}
			byLSN[ch.StartLSN] = tx
			order = append(order, ch.StartLSN)
		}
		if ch.TimestampMS > tx.TimestampMS {
			tx.TimestampMS = ch.TimestampMS
		}
		key := ch.StartLSN + "|" + ch.SeqVal + "|" + ch.Schema + "|" + ch.Table
		e := domain.CDCEvent{ID: ch.Capture + ":" + ch.StartLSN + ":" + ch.SeqVal + ":" + strconv.Itoa(ch.Operation), SourceSchema: ch.Schema, SourceTable: ch.Table, PositionType: "SQLSERVER_LSN", PositionValue: ch.StartLSN, Resource: ch.Capture, SourceTimestampMS: ch.TimestampMS}
		switch ch.Operation {
		case 1:
			e.Operation = domain.CDCDelete
			e.Before = fieldsFromValues(ch.Columns, ch.Values)
			tx.Events = append(tx.Events, e)
		case 2:
			e.Operation = domain.CDCInsert
			e.After = fieldsFromValues(ch.Columns, ch.Values)
			tx.Events = append(tx.Events, e)
		case 3:
			oldUpdate[key] = ch
		case 4:
			e.Operation = domain.CDCUpdate
			e.After = fieldsFromValues(ch.Columns, ch.Values)
			if old, ok := oldUpdate[key]; ok {
				e.Before = fieldsFromValues(old.Columns, old.Values)
				delete(oldUpdate, key)
			}
			tx.Events = append(tx.Events, e)
		default:
			return nil, fmt.Errorf("unsupported SQL Server CDC operation %d", ch.Operation)
		}
	}
	if len(oldUpdate) > 0 {
		return nil, errors.New("SQL Server CDC update-old row did not have a matching update-new row")
	}
	out := make([]SQLServerCDCTransaction, 0, len(order))
	for _, lsn := range order {
		if tx := byLSN[lsn]; tx != nil && len(tx.Events) > 0 {
			out = append(out, *tx)
		}
	}
	return out, nil
}

type SQLServerCDCTransaction struct {
	LSN         string
	TimestampMS int64
	Events      []domain.CDCEvent
}

var _ connector.CDCSource = (*Connector)(nil)

func (c *Connector) ValidateCDCSelection(ctx context.Context, mappings []domain.TableMapping) error {
	selected := map[string]bool{}
	for _, m := range mappings {
		if strings.TrimSpace(m.SourceTable) == "" {
			continue
		}
		selected[strings.ToLower(strings.TrimSpace(m.SourceSchema)+"."+strings.TrimSpace(m.SourceTable))] = true
	}
	if len(selected) == 0 {
		return errors.New("SQL Server CDC requires at least one selected table")
	}
	_, err := c.DiscoverCDCCaptures(ctx, selected)
	return err
}

var _ connector.CDCSelectionValidator = (*Connector)(nil)
