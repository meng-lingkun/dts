package mysqlconnector

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"qmigration/backend/internal/cdc/obbinlog"
	"qmigration/backend/internal/cdc/ticdc"
	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
)

type Factory struct{}

func NewFactory() *Factory { return &Factory{} }

func (*Factory) Capabilities(t domain.DataSourceType) connector.Descriptor {
	caps := []connector.Capability{
		connector.CapabilityMetadata,
		connector.CapabilityFullRead,
		connector.CapabilityFullWrite,
		connector.CapabilityKeysetBoundary,
		connector.CapabilityPartition,
		connector.CapabilityRuntimeLoad,
		connector.CapabilitySchemaCreate,
		connector.CapabilitySchemaObjects,
		connector.CapabilityPostLoadSchema,
		connector.CapabilityCDCApply,
		connector.CapabilityCDCTransactional,
		connector.CapabilityDDLApply,
		connector.CapabilityPointLookup,
		connector.CapabilityMigrationPrecheck,
	}
	// Source-side CDC is advertised only when the SQL endpoint itself exposes a
	// MySQL-compatible replication stream that the QMigration native binlog reader
	// can consume. TiDB uses TiCDC rather than COM_BINLOG_DUMP, while OceanBase
	// exposes MySQL Binlog through a separate Binlog Service endpoint; neither is
	// safe to infer from the full-load SQL endpoint stored in the datasource.
	// PolarDB-X global binlog and PolarDB MySQL are MySQL-protocol sources, but
	// their version/privilege prerequisites are still enforced by prechecks.
	if mysqlBinlogSourceSupported(t) || t == domain.DataSourceTiDB || t == domain.DataSourceOceanBase {
		caps = append(caps, connector.CapabilityCDCPosition, connector.CapabilityCDCRead)
	}
	if t == domain.DataSourcePolarDBX || t == domain.DataSourceTiDB || t == domain.DataSourceOceanBase {
		caps = append(caps, connector.CapabilityTopology)
	}
	if t == domain.DataSourceTiDB {
		caps = append(caps, connector.CapabilityValidationSnapshot)
	}
	maturity := connector.MaturityNative
	note := "QMigration native MySQL protocol full data plane"
	if t == domain.DataSourceTiDB {
		maturity = connector.MaturityExperimental
		note = "Native MySQL-protocol Full Load/target apply plus QMigration TiCDC OpenAPI + native Kafka Canal-JSON CDC; real TiCDC/Kafka qualification required"
	} else if t == domain.DataSourceOceanBase {
		maturity = connector.MaturityExperimental
		note = "Native MySQL-protocol Full Load/target apply plus OceanBase Binlog Service (MySQL Binlog V4/GTID) through an explicit tenant ODP cdc_url; real ODP/Binlog Service qualification required"
	}
	return connector.Descriptor{Type: t, Protocol: "mysql", Capabilities: caps, Native: true, Maturity: maturity, QualificationRequired: t == domain.DataSourceTiDB || t == domain.DataSourceOceanBase, Note: note}
}

func mysqlBinlogSourceSupported(t domain.DataSourceType) bool {
	switch t {
	case domain.DataSourceMySQL, domain.DataSourceMariaDB, domain.DataSourcePolarDBX, domain.DataSourcePolarDBMySQL:
		return true
	default:
		return false
	}
}
func (f *Factory) New(d domain.DataSource) (connector.Connector, error) {
	if d.Host == "" || d.Port <= 0 {
		return nil, errors.New("invalid mysql endpoint")
	}
	return &Connector{ds: d}, nil
}

type Connector struct {
	ds     domain.DataSource
	mu     sync.Mutex
	client *protocolClient

	// gbaseLayoutValidated caches target table layout validation after SHOW CREATE
	// TABLE proves that a GBase 8a MERGE target is hash-distributed by columns
	// contained in the stable migration key. The cache is connector-local and is
	// discarded on process/connector restart, so every fresh worker revalidates.
	gbaseLayoutMu        sync.Mutex
	gbaseLayoutValidated map[string]bool
}

func (c *Connector) get(ctx context.Context) (*protocolClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.client != nil {
		return c.client, nil
	}
	p, err := dialProtocol(ctx, c.ds)
	if err != nil {
		return nil, err
	}
	c.client = p
	return p, nil
}
func (c *Connector) TestConnection(ctx context.Context) error {
	p, err := c.get(ctx)
	if err != nil {
		return err
	}
	r, err := p.query(ctx, "SELECT 1")
	if err != nil {
		return err
	}
	if len(r.rows) != 1 {
		return errors.New("mysql SELECT 1 returned no row")
	}
	return nil
}
func (c *Connector) GetVersion(ctx context.Context) (string, error) {
	p, err := c.get(ctx)
	if err != nil {
		return "", err
	}
	r, err := p.query(ctx, "SELECT VERSION()")
	if err != nil {
		return "", err
	}
	if len(r.rows) == 0 || len(r.rows[0]) == 0 {
		return p.serverVersion, nil
	}
	return string(r.rows[0][0]), nil
}
func (c *Connector) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.client == nil {
		return nil
	}
	err := c.client.close()
	c.client = nil
	return err
}

// OpenValidationSnapshot pins a fresh TiDB SQL session to the exact TSO stored
// in QMigration's durable TIDB_TSO checkpoint. TiDB's SESSION tidb_snapshot
// makes every subsequent SELECT on that session read the same historical MVCC
// snapshot, so online validation can compare the source at the target's frozen
// apply watermark instead of racing concurrent source writes.
func (c *Connector) OpenValidationSnapshot(ctx context.Context, position domain.CDCPosition) (connector.DataConnector, error) {
	if c.ds.Type != domain.DataSourceTiDB {
		return nil, fmt.Errorf("%s does not implement exact validation snapshots", c.ds.Type)
	}
	if !strings.EqualFold(strings.TrimSpace(position.PositionType), "TIDB_TSO") {
		return nil, fmt.Errorf("TiDB validation snapshot requires TIDB_TSO, got %q", position.PositionType)
	}
	parsed, err := ticdc.ParsePosition(strings.TrimSpace(position.PositionValue))
	if err != nil {
		return nil, fmt.Errorf("parse TiDB validation TSO: %w", err)
	}
	if parsed.TSO == 0 {
		return nil, errors.New("TiDB validation snapshot TSO is zero")
	}
	snapshot := &Connector{ds: c.ds}
	p, err := snapshot.get(ctx)
	if err != nil {
		return nil, err
	}
	tso := strconv.FormatUint(parsed.TSO, 10)
	if _, err := p.exec(ctx, "SET SESSION tidb_snapshot="+quoteSQLString(tso)); err != nil {
		_ = snapshot.Close()
		return nil, fmt.Errorf("set TiDB validation snapshot TSO %s: %w", tso, err)
	}
	check, err := p.query(ctx, "SELECT @@tidb_snapshot")
	if err != nil {
		_ = snapshot.Close()
		return nil, fmt.Errorf("verify TiDB validation snapshot TSO %s: %w", tso, err)
	}
	if len(check.rows) == 0 || len(check.rows[0]) == 0 || strings.TrimSpace(string(check.rows[0][0])) != tso {
		_ = snapshot.Close()
		got := ""
		if len(check.rows) > 0 && len(check.rows[0]) > 0 {
			got = strings.TrimSpace(string(check.rows[0][0]))
		}
		return nil, fmt.Errorf("TiDB validation snapshot verification mismatch: requested=%s got=%q", tso, got)
	}
	return snapshot, nil
}

func (c *Connector) ListSchemas(ctx context.Context) ([]domain.SchemaInfo, error) {
	p, err := c.get(ctx)
	if err != nil {
		return nil, err
	}
	q := "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME NOT IN ('information_schema','mysql','performance_schema','sys') ORDER BY SCHEMA_NAME"
	if c.ds.Type == domain.DataSourceGBase {
		q = "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME NOT IN ('information_schema','gbase','gclusterdb','gctmpdb','performance_schema') ORDER BY SCHEMA_NAME"
	}
	r, err := p.query(ctx, q)
	if err != nil {
		return nil, err
	}
	out := make([]domain.SchemaInfo, 0, len(r.rows))
	for _, row := range r.rows {
		if len(row) > 0 {
			out = append(out, domain.SchemaInfo{Name: string(row[0])})
		}
	}
	return out, nil
}
func (c *Connector) ListTables(ctx context.Context, schema string) ([]domain.TableInfo, error) {
	p, err := c.get(ctx)
	if err != nil {
		return nil, err
	}
	sql := "SELECT TABLE_SCHEMA,TABLE_NAME,COALESCE(TABLE_ROWS,0),COALESCE(DATA_LENGTH,0) FROM information_schema.TABLES WHERE TABLE_SCHEMA=" + quoteSQLString(schema) + " AND TABLE_TYPE='BASE TABLE' ORDER BY TABLE_NAME"
	r, err := p.query(ctx, sql)
	if err != nil {
		return nil, err
	}
	out := make([]domain.TableInfo, 0, len(r.rows))
	for _, row := range r.rows {
		if len(row) < 4 {
			continue
		}
		rows, _ := strconv.ParseInt(string(row[2]), 10, 64)
		size, _ := strconv.ParseInt(string(row[3]), 10, 64)
		out = append(out, domain.TableInfo{Schema: string(row[0]), Name: string(row[1]), Rows: rows, DataLength: size})
	}
	return out, nil
}

func (c *Connector) GetTableMetadata(ctx context.Context, schema, table string) (*domain.TableMetadata, error) {
	p, err := c.get(ctx)
	if err != nil {
		return nil, err
	}
	m := &domain.TableMetadata{Schema: schema, Name: table}
	statsSQL := "SELECT COALESCE(TABLE_ROWS,0),COALESCE(DATA_LENGTH,0) FROM information_schema.TABLES WHERE TABLE_SCHEMA=" + quoteSQLString(schema) + " AND TABLE_NAME=" + quoteSQLString(table) + " LIMIT 1"
	if r, e := p.query(ctx, statsSQL); e == nil && len(r.rows) > 0 && len(r.rows[0]) >= 2 {
		m.EstimatedRows, _ = strconv.ParseInt(string(r.rows[0][0]), 10, 64)
		m.DataLength, _ = strconv.ParseInt(string(r.rows[0][1]), 10, 64)
	}
	colSQL := "SELECT COLUMN_NAME,DATA_TYPE,COLUMN_TYPE,IS_NULLABLE,COALESCE(EXTRA,''),ORDINAL_POSITION,COLUMN_KEY FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=" + quoteSQLString(schema) + " AND TABLE_NAME=" + quoteSQLString(table) + " ORDER BY ORDINAL_POSITION"
	r, err := p.query(ctx, colSQL)
	if err != nil {
		return nil, err
	}
	var pks []domain.ColumnInfo
	for _, row := range r.rows {
		if len(row) < 7 {
			continue
		}
		ord, _ := strconv.Atoi(string(row[5]))
		col := domain.ColumnInfo{Name: string(row[0]), DataType: strings.ToLower(string(row[1])), ColumnType: strings.ToLower(string(row[2])), Nullable: string(row[3]) == "YES", Extra: string(row[4]), Ordinal: ord, PrimaryKey: string(row[6]) == "PRI"}
		m.Columns = append(m.Columns, col)
		if col.PrimaryKey {
			pks = append(pks, col)
		}
	}
	pkOrderSQL := "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA=" + quoteSQLString(schema) + " AND TABLE_NAME=" + quoteSQLString(table) + " AND CONSTRAINT_NAME='PRIMARY' ORDER BY ORDINAL_POSITION"
	if pkRows, e := p.query(ctx, pkOrderSQL); e == nil {
		for _, row := range pkRows.rows {
			if len(row) > 0 {
				m.PrimaryKeys = append(m.PrimaryKeys, string(row[0]))
			}
		}
	}
	if len(m.PrimaryKeys) == 0 {
		for _, pk := range pks {
			m.PrimaryKeys = append(m.PrimaryKeys, pk.Name)
		}
	}
	indexSQL := "SELECT INDEX_NAME,NON_UNIQUE,COLUMN_NAME,SEQ_IN_INDEX FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=" + quoteSQLString(schema) + " AND TABLE_NAME=" + quoteSQLString(table) + " ORDER BY INDEX_NAME,SEQ_IN_INDEX"
	if idxRows, e := p.query(ctx, indexSQL); e == nil {
		byName := map[string]*domain.IndexInfo{}
		order := []string{}
		for _, row := range idxRows.rows {
			if len(row) < 4 {
				continue
			}
			name := string(row[0])
			idx := byName[name]
			if idx == nil {
				idx = &domain.IndexInfo{Name: name, Unique: string(row[1]) == "0", Primary: strings.EqualFold(name, "PRIMARY")}
				byName[name] = idx
				order = append(order, name)
			}
			idx.Columns = append(idx.Columns, string(row[2]))
		}
		for _, name := range order {
			m.Indexes = append(m.Indexes, *byName[name])
		}
	}
	fkSQL := "SELECT CONSTRAINT_NAME,COLUMN_NAME,REFERENCED_TABLE_SCHEMA,REFERENCED_TABLE_NAME,REFERENCED_COLUMN_NAME,ORDINAL_POSITION FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA=" + quoteSQLString(schema) + " AND TABLE_NAME=" + quoteSQLString(table) + " AND REFERENCED_TABLE_NAME IS NOT NULL ORDER BY CONSTRAINT_NAME,ORDINAL_POSITION"
	if fkRows, e := p.query(ctx, fkSQL); e == nil {
		byName := map[string]*domain.ForeignKeyInfo{}
		order := []string{}
		for _, row := range fkRows.rows {
			if len(row) < 5 {
				continue
			}
			name := string(row[0])
			fk := byName[name]
			if fk == nil {
				fk = &domain.ForeignKeyInfo{Name: name, ReferencedSchema: string(row[2]), ReferencedTable: string(row[3])}
				byName[name] = fk
				order = append(order, name)
			}
			fk.Columns = append(fk.Columns, string(row[1]))
			fk.ReferencedColumns = append(fk.ReferencedColumns, string(row[4]))
		}
		for _, name := range order {
			m.ForeignKeys = append(m.ForeignKeys, *byName[name])
		}
	}
	if len(m.PrimaryKeys) == 1 {
		var pk domain.ColumnInfo
		for _, col := range m.Columns {
			if col.Name == m.PrimaryKeys[0] {
				pk = col
				break
			}
		}
		m.PrimaryKey = pk.Name
		m.PrimaryKeyType = pk.ColumnType
		m.PrimaryKeyNumeric = isSignedInteger(pk.DataType, pk.ColumnType)
		if m.PrimaryKeyNumeric {
			rangeSQL := "SELECT MIN(" + quoteIdent(pk.Name) + "),MAX(" + quoteIdent(pk.Name) + ") FROM " + quoteIdent(schema) + "." + quoteIdent(table)
			rr, e := p.query(ctx, rangeSQL)
			if e != nil {
				return nil, e
			}
			if len(rr.rows) > 0 && len(rr.rows[0]) >= 2 && !rr.nulls[0][0] && !rr.nulls[0][1] {
				m.HasRows = true
				m.MinPK, e = strconv.ParseInt(string(rr.rows[0][0]), 10, 64)
				if e != nil {
					return nil, fmt.Errorf("parse min pk: %w", e)
				}
				m.MaxPK, e = strconv.ParseInt(string(rr.rows[0][1]), 10, 64)
				if e != nil {
					return nil, fmt.Errorf("parse max pk: %w", e)
				}
			}
		}
	}
	return m, nil
}
func isSignedInteger(dataType, columnType string) bool {
	switch strings.ToLower(dataType) {
	case "tinyint", "smallint", "mediumint", "int", "integer", "bigint":
		return !strings.Contains(strings.ToLower(columnType), "unsigned")
	default:
		return false
	}
}

func validateMySQLValue(col domain.ColumnInfo, v connector.Value) error {
	if v.Null {
		return nil
	}
	switch strings.ToLower(col.DataType) {
	case "tinyint", "smallint", "mediumint", "int", "integer", "bigint", "decimal", "numeric", "float", "double", "real", "year":
		return connector.ValidateNumericLiteral(v.Raw, false)
	default:
		return nil
	}
}

func validateMySQLValues(cols []domain.ColumnInfo, values []connector.Value) error {
	if len(cols) != len(values) {
		return errors.New("column/value count mismatch")
	}
	for i := range cols {
		if err := validateMySQLValue(cols[i], values[i]); err != nil {
			return fmt.Errorf("column %s: %w", cols[i].Name, err)
		}
	}
	return nil
}

func mysqlValueLiteral(v connector.Value, col domain.ColumnInfo) string {
	if v.Null {
		return "NULL"
	}
	t := strings.ToLower(col.DataType)
	switch t {
	case "tinyint", "smallint", "mediumint", "int", "integer", "bigint", "decimal", "numeric", "float", "double", "real", "year":
		return string(v.Raw)
	case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob", "bit":
		return "X'" + hex.EncodeToString(v.Raw) + "'"
	default:
		return "'" + strings.ReplaceAll(string(v.Raw), "'", "''") + "'"
	}
}

func mysqlKeyColumns(keys []string, columns []domain.ColumnInfo) ([]domain.ColumnInfo, error) {
	byName := make(map[string]domain.ColumnInfo, len(columns))
	for _, col := range columns {
		byName[strings.ToLower(col.Name)] = col
	}
	out := make([]domain.ColumnInfo, len(keys))
	for i, key := range keys {
		col, ok := byName[strings.ToLower(key)]
		if !ok {
			return nil, fmt.Errorf("migration key %s is not in selected columns", key)
		}
		out[i] = col
	}
	return out, nil
}

// PlanKeysetBoundaries uses an ordered NTILE scan to find the first migration
// key in every partition after the first. Boundaries are actual source rows, so
// [lower, upper) chunks are gap-free even for sparse strings/composite keys.
func (c *Connector) PlanKeysetBoundaries(ctx context.Context, req connector.KeysetBoundaryRequest) ([][]connector.Value, error) {
	if req.Partitions <= 1 {
		return nil, nil
	}
	if len(req.Keys) == 0 {
		return nil, errors.New("keyset boundary planning requires migration key columns")
	}
	keyCols, err := mysqlKeyColumns(req.Keys, req.Columns)
	if err != nil {
		return nil, err
	}
	for _, bound := range [][]connector.Value{req.LowerBound, req.UpperBound} {
		if len(bound) > 0 && len(bound) != len(req.Keys) {
			return nil, fmt.Errorf("keyset boundary has %d values for %d keys", len(bound), len(req.Keys))
		}
	}
	for _, bound := range [][]connector.Value{req.LowerBound, req.UpperBound} {
		if len(bound) > 0 {
			if err := validateMySQLValues(keyCols, bound); err != nil {
				return nil, fmt.Errorf("keyset boundary: %w", err)
			}
		}
	}
	p, err := c.get(ctx)
	if err != nil {
		return nil, err
	}
	quoted := make([]string, len(req.Keys))
	for i, key := range req.Keys {
		quoted[i] = quoteIdent(key)
	}
	keyList := strings.Join(quoted, ",")
	orderBy := strings.Join(quoted, ",")
	tupleLiteral := func(values []connector.Value) string {
		right := make([]string, len(values))
		for i := range values {
			right[i] = mysqlValueLiteral(values[i], keyCols[i])
		}
		return "(" + strings.Join(right, ",") + ")"
	}
	conditions := make([]string, 0, 2)
	if len(req.LowerBound) > 0 {
		conditions = append(conditions, "("+keyList+") >= "+tupleLiteral(req.LowerBound))
	}
	if len(req.UpperBound) > 0 {
		conditions = append(conditions, "("+keyList+") < "+tupleLiteral(req.UpperBound))
	}
	where := ""
	if len(conditions) > 0 {
		where = " WHERE " + strings.Join(conditions, " AND ")
	}
	q := "WITH qm_ranked AS (SELECT " + keyList + ", NTILE(" + strconv.Itoa(req.Partitions) + ") OVER (ORDER BY " + orderBy + ") AS qm_bucket FROM " + quoteIdent(req.Schema) + "." + quoteIdent(req.Table) + where + "), " +
		"qm_bounds AS (SELECT " + keyList + ", qm_bucket, ROW_NUMBER() OVER (PARTITION BY qm_bucket ORDER BY " + orderBy + ") AS qm_rn FROM qm_ranked) " +
		"SELECT " + keyList + " FROM qm_bounds WHERE qm_bucket > 1 AND qm_rn = 1 ORDER BY qm_bucket"
	r, err := p.query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("ordered keyset boundary query: %w", err)
	}
	out := make([][]connector.Value, 0, len(r.rows))
	for ri, row := range r.rows {
		if len(row) != len(req.Keys) {
			return nil, fmt.Errorf("boundary query returned %d columns for %d migration keys", len(row), len(req.Keys))
		}
		bound := make([]connector.Value, len(row))
		for i, raw := range row {
			if ri < len(r.nulls) && i < len(r.nulls[ri]) && r.nulls[ri][i] {
				return nil, fmt.Errorf("migration key %s returned NULL boundary", req.Keys[i])
			}
			bound[i] = connector.Value{Raw: append([]byte(nil), raw...)}
		}
		out = append(out, bound)
	}
	return out, nil
}

func (c *Connector) ListTablePartitions(ctx context.Context, schema, table string) ([]string, error) {
	p, err := c.get(ctx)
	if err != nil {
		return nil, err
	}
	q := "SELECT PARTITION_NAME FROM information_schema.PARTITIONS WHERE TABLE_SCHEMA=" + quoteSQLString(schema) + " AND TABLE_NAME=" + quoteSQLString(table) + " AND PARTITION_NAME IS NOT NULL ORDER BY PARTITION_ORDINAL_POSITION"
	r, err := p.query(ctx, q)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(r.rows))
	for i, row := range r.rows {
		if len(row) > 0 && !r.nulls[i][0] && strings.TrimSpace(string(row[0])) != "" {
			out = append(out, string(row[0]))
		}
	}
	return out, nil
}

func (c *Connector) ReadBatch(ctx context.Context, req connector.ReadBatchRequest) (*connector.RowBatch, error) {
	p, err := c.get(ctx)
	if err != nil {
		return nil, err
	}
	if req.Limit <= 0 {
		req.Limit = 500
	}
	cols := make([]string, 0, len(req.Columns))
	selected := make([]domain.ColumnInfo, 0, len(req.Columns))
	indexByName := map[string]int{}
	for _, col := range req.Columns {
		if strings.Contains(strings.ToUpper(col.Extra), "GENERATED") {
			continue
		}
		indexByName[col.Name] = len(cols)
		selected = append(selected, col)
		cols = append(cols, quoteIdent(col.Name))
	}
	if len(cols) == 0 {
		return nil, errors.New("no writable/readable columns")
	}

	var where string
	var orderKeys []string
	if req.UseKeyset {
		keys := append([]string(nil), req.PrimaryKeys...)
		if len(keys) == 0 && req.PrimaryKey != "" {
			keys = []string{req.PrimaryKey}
		}
		if len(keys) == 0 {
			return nil, errors.New("keyset read requires primary key columns")
		}
		for _, bound := range [][]connector.Value{req.Cursor, req.LowerBound, req.UpperBound} {
			if len(bound) > 0 && len(bound) != len(keys) {
				return nil, fmt.Errorf("keyset bound has %d values for %d keys", len(bound), len(keys))
			}
		}
		left := make([]string, 0, len(keys))
		keyCols := make([]domain.ColumnInfo, 0, len(keys))
		for _, key := range keys {
			idx, ok := indexByName[key]
			if !ok {
				return nil, fmt.Errorf("primary key %s is not in selected columns", key)
			}
			orderKeys = append(orderKeys, quoteIdent(key))
			left = append(left, quoteIdent(key))
			keyCols = append(keyCols, selected[idx])
		}
		tuple := func(values []connector.Value) string {
			right := make([]string, len(values))
			for i := range values {
				right[i] = mysqlValueLiteral(values[i], keyCols[i])
			}
			return "(" + strings.Join(right, ",") + ")"
		}
		conditions := make([]string, 0, 2)
		if len(req.Cursor) > 0 {
			conditions = append(conditions, "("+strings.Join(left, ",")+") > "+tuple(req.Cursor))
		} else if len(req.LowerBound) > 0 {
			conditions = append(conditions, "("+strings.Join(left, ",")+") >= "+tuple(req.LowerBound))
		}
		if len(req.UpperBound) > 0 {
			conditions = append(conditions, "("+strings.Join(left, ",")+") < "+tuple(req.UpperBound))
		}
		if len(conditions) > 0 {
			where = " WHERE " + strings.Join(conditions, " AND ")
		}
	} else {
		pkIndex, ok := indexByName[req.PrimaryKey]
		if !ok {
			return nil, errors.New("primary key is not in selected columns")
		}
		_ = pkIndex
		where = " WHERE " + quoteIdent(req.PrimaryKey) + ">=" + strconv.FormatInt(req.StartPK, 10) + " AND " + quoteIdent(req.PrimaryKey) + "<=" + strconv.FormatInt(req.EndPK, 10)
		if req.HasAfter {
			where += " AND " + quoteIdent(req.PrimaryKey) + ">" + strconv.FormatInt(req.AfterPK, 10)
		}
		orderKeys = []string{quoteIdent(req.PrimaryKey)}
	}

	conditions := []string{}
	if strings.HasPrefix(where, " WHERE ") {
		conditions = append(conditions, strings.TrimPrefix(where, " WHERE "))
	}
	if req.HashBuckets > 0 {
		if req.HashBucket < 0 || req.HashBucket >= req.HashBuckets {
			return nil, fmt.Errorf("invalid hash bucket %d/%d", req.HashBucket, req.HashBuckets)
		}
		keys := append([]string(nil), req.PrimaryKeys...)
		if len(keys) == 0 && req.PrimaryKey != "" {
			keys = []string{req.PrimaryKey}
		}
		if len(keys) == 0 {
			return nil, errors.New("hash split requires stable key columns")
		}
		parts := make([]string, len(keys))
		for i, key := range keys {
			parts[i] = "COALESCE(CAST(" + quoteIdent(key) + " AS CHAR),'<NULL>')"
		}
		conditions = append(conditions, "MOD(CRC32(CONCAT_WS('#',"+strings.Join(parts, ",")+")),"+strconv.Itoa(req.HashBuckets)+")="+strconv.Itoa(req.HashBucket))
	}
	if strings.TrimSpace(req.CustomWhere) != "" {
		conditions = append(conditions, "("+strings.TrimSpace(req.CustomWhere)+")")
	}
	where = ""
	if len(conditions) > 0 {
		where = " WHERE " + strings.Join(conditions, " AND ")
	}
	from := quoteIdent(req.Schema) + "." + quoteIdent(req.Table)
	if strings.TrimSpace(req.Partition) != "" {
		from += " PARTITION (" + quoteIdent(strings.TrimSpace(req.Partition)) + ")"
	}
	q := "SELECT " + strings.Join(cols, ",") + " FROM " + from + where + " ORDER BY " + strings.Join(orderKeys, ",") + " LIMIT " + strconv.Itoa(req.Limit)
	r, err := p.query(ctx, q)
	if err != nil {
		return nil, err
	}
	batch := &connector.RowBatch{Rows: make([][]connector.Value, 0, len(r.rows))}
	for ri, row := range r.rows {
		vals := make([]connector.Value, len(row))
		for i, raw := range row {
			vals[i] = connector.Value{Null: r.nulls[ri][i], Raw: raw}
			if !r.nulls[ri][i] {
				batch.Bytes += int64(len(raw))
			}
		}
		batch.Rows = append(batch.Rows, vals)
		if req.UseKeyset {
			batch.LastKey = batch.LastKey[:0]
			for _, key := range req.PrimaryKeys {
				idx := indexByName[key]
				v := vals[idx]
				v.Raw = append([]byte(nil), v.Raw...)
				batch.LastKey = append(batch.LastKey, v)
			}
			if len(req.PrimaryKeys) == 0 && req.PrimaryKey != "" {
				idx := indexByName[req.PrimaryKey]
				v := vals[idx]
				v.Raw = append([]byte(nil), v.Raw...)
				batch.LastKey = []connector.Value{v}
			}
		} else if !r.nulls[ri][indexByName[req.PrimaryKey]] {
			batch.LastPK, err = strconv.ParseInt(string(row[indexByName[req.PrimaryKey]]), 10, 64)
			if err != nil {
				return nil, err
			}
		}
	}
	return batch, nil
}

func (c *Connector) ReadByKey(ctx context.Context, req connector.ReadByKeyRequest) ([]connector.Value, bool, error) {
	if len(req.PrimaryKeys) == 0 || len(req.PrimaryKeys) != len(req.KeyValues) || len(req.PrimaryKeys) != len(req.KeyColumns) {
		return nil, false, errors.New("invalid point lookup primary-key values")
	}
	if len(req.Columns) == 0 {
		return nil, false, errors.New("point lookup requires selected columns")
	}
	if err := validateMySQLValues(req.KeyColumns, req.KeyValues); err != nil {
		return nil, false, fmt.Errorf("point lookup key: %w", err)
	}
	p, err := c.get(ctx)
	if err != nil {
		return nil, false, err
	}
	cols := make([]string, len(req.Columns))
	for i, col := range req.Columns {
		cols[i] = quoteIdent(col.Name)
	}
	where := make([]string, len(req.PrimaryKeys))
	for i, key := range req.PrimaryKeys {
		if req.KeyValues[i].Null {
			return nil, false, fmt.Errorf("point lookup key %s cannot be null", key)
		}
		where[i] = quoteIdent(key) + "=" + formatValue(req.KeyColumns[i], req.KeyValues[i])
	}
	q := "SELECT " + strings.Join(cols, ",") + " FROM " + quoteIdent(req.Schema) + "." + quoteIdent(req.Table) + " WHERE " + strings.Join(where, " AND ") + " LIMIT 1 FOR UPDATE"
	r, err := p.query(ctx, q)
	if err != nil {
		return nil, false, err
	}
	if len(r.rows) == 0 {
		return nil, false, nil
	}
	vals := make([]connector.Value, len(req.Columns))
	for i, raw := range r.rows[0] {
		vals[i] = connector.Value{Null: r.nulls[0][i], Raw: raw}
	}
	return vals, true, nil
}

func (c *Connector) WriteBatch(ctx context.Context, req connector.WriteBatchRequest) (int64, error) {
	if len(req.Rows) == 0 {
		return 0, nil
	}
	if c.ds.Type == domain.DataSourceGBase {
		return c.writeGBaseBatch(ctx, req)
	}
	p, err := c.get(ctx)
	if err != nil {
		return 0, err
	}
	cols := make([]domain.ColumnInfo, 0, len(req.Columns))
	for _, col := range req.Columns {
		if strings.Contains(strings.ToUpper(col.Extra), "GENERATED") {
			continue
		}
		cols = append(cols, col)
	}
	if len(cols) == 0 {
		return 0, errors.New("no target columns")
	}
	ident := make([]string, len(cols))
	for i, col := range cols {
		ident[i] = quoteIdent(col.Name)
	}
	var b strings.Builder
	b.Grow(len(req.Rows) * len(cols) * 16)
	b.WriteString("INSERT INTO ")
	b.WriteString(quoteIdent(req.Schema))
	b.WriteByte('.')
	b.WriteString(quoteIdent(req.Table))
	b.WriteString(" (")
	b.WriteString(strings.Join(ident, ","))
	b.WriteString(") VALUES ")
	for ri, row := range req.Rows {
		if len(row) != len(cols) {
			return 0, fmt.Errorf("row column count %d != %d", len(row), len(cols))
		}
		if err := validateMySQLValues(cols, row); err != nil {
			return 0, fmt.Errorf("target row %d: %w", ri, err)
		}
		if ri > 0 {
			b.WriteByte(',')
		}
		b.WriteByte('(')
		for ci, v := range row {
			if ci > 0 {
				b.WriteByte(',')
			}
			b.WriteString(formatValue(cols[ci], v))
		}
		b.WriteByte(')')
	}
	// Idempotent replay is required because a lease can expire after a partial write.
	b.WriteString(" ON DUPLICATE KEY UPDATE ")
	updates := make([]string, 0, len(cols))
	for _, col := range cols {
		q := quoteIdent(col.Name)
		updates = append(updates, q+"=VALUES("+q+")")
	}
	b.WriteString(strings.Join(updates, ","))
	_, err = p.exec(ctx, b.String())
	if err != nil {
		return 0, err
	}
	return int64(len(req.Rows)), nil
}

var gbaseStageCounter atomic.Uint64

func gbaseTypeArgs(columnType string) []int {
	open := strings.IndexByte(columnType, '(')
	close := strings.IndexByte(columnType, ')')
	if open < 0 || close <= open+1 {
		return nil
	}
	parts := strings.Split(columnType[open+1:close], ",")
	out := make([]int, 0, len(parts))
	for _, part := range parts {
		n, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || n < 0 {
			return nil
		}
		out = append(out, n)
	}
	return out
}

func gbaseTargetType(col domain.ColumnInfo) string {
	ct := strings.ToLower(strings.TrimSpace(col.ColumnType))
	dt := strings.ToLower(strings.TrimSpace(col.DataType))
	if strings.Contains(ct, "character varying") {
		ct = strings.Replace(ct, "character varying", "varchar", 1)
	}
	if strings.HasPrefix(ct, "character(") {
		ct = strings.Replace(ct, "character", "char", 1)
	}
	switch dt {
	case "tinyint", "smallint":
		return dt
	case "mediumint", "integer", "int", "int4":
		return "int"
	case "bigint", "int8":
		return "bigint"
	case "boolean", "bool", "bit":
		return "tinyint"
	case "decimal", "numeric", "number":
		args := gbaseTypeArgs(ct)
		p, scale := 65, 30
		if len(args) > 0 {
			p = args[0]
			scale = 0
			if len(args) > 1 {
				scale = args[1]
			}
		}
		if p <= 0 || p > 65 {
			p = 65
		}
		if scale < 0 {
			scale = 0
		}
		if scale > 30 {
			scale = 30
		}
		if scale > p {
			scale = p
		}
		return fmt.Sprintf("decimal(%d,%d)", p, scale)
	case "float", "real", "float4":
		return "float"
	case "double", "double precision", "float8":
		return "double"
	case "date":
		return "date"
	case "time":
		return "time"
	case "timestamp", "datetime", "timestamp without time zone", "timestamp with time zone":
		return "datetime"
	case "binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob", "bytea", "raw":
		return "longblob"
	case "uuid":
		return "varchar(36)"
	case "char", "varchar", "character", "character varying":
		args := gbaseTypeArgs(ct)
		if len(args) > 0 && args[0] > 0 && args[0] <= 8191 {
			if strings.HasPrefix(ct, "char(") {
				return fmt.Sprintf("char(%d)", args[0])
			}
			return fmt.Sprintf("varchar(%d)", args[0])
		}
		// GBase 8a utf8mb4 VARCHAR is bounded; using LONGTEXT avoids silent
		// truncation for a wider source declaration.
		return "longtext"
	case "text", "tinytext", "mediumtext", "longtext", "json", "jsonb", "clob", "nclob":
		return "longtext"
	}
	if strings.HasPrefix(ct, "varchar(") || strings.HasPrefix(ct, "char(") {
		args := gbaseTypeArgs(ct)
		if len(args) > 0 && args[0] > 0 && args[0] <= 8191 {
			prefix := "varchar"
			if strings.HasPrefix(ct, "char(") {
				prefix = "char"
			}
			return fmt.Sprintf("%s(%d)", prefix, args[0])
		}
		return "longtext"
	}
	if strings.HasPrefix(ct, "decimal(") || strings.HasPrefix(ct, "numeric(") || strings.HasPrefix(ct, "number(") {
		args := gbaseTypeArgs(ct)
		p, scale := 65, 30
		if len(args) > 0 {
			p = args[0]
			scale = 0
			if len(args) > 1 {
				scale = args[1]
			}
		}
		if p <= 0 || p > 65 {
			p = 65
		}
		if scale > 30 {
			scale = 30
		}
		if scale > p {
			scale = p
		}
		return fmt.Sprintf("decimal(%d,%d)", p, scale)
	}
	return "longtext"
}

func gbaseHashEligibleType(targetType string) bool {
	t := strings.ToLower(strings.TrimSpace(targetType))
	for _, prefix := range []string{"tinyint", "smallint", "int", "bigint", "decimal(", "numeric(", "varchar("} {
		if t == prefix || strings.HasPrefix(t, prefix) {
			return true
		}
	}
	return false
}

func gbaseChooseDistributionKeys(columns []domain.ColumnInfo, primaryKeys []string) ([]string, error) {
	if len(primaryKeys) == 0 {
		return nil, errors.New("GBase 8a auto-created target requires a stable migration key for HASH distribution")
	}
	byName := make(map[string]domain.ColumnInfo, len(columns))
	for _, col := range columns {
		if strings.Contains(strings.ToUpper(col.Extra), "GENERATED") {
			continue
		}
		byName[strings.ToLower(col.Name)] = col
	}
	// One stable high-cardinality business key is a safer automatic default than
	// guessing a non-key workload column. GBase supports multiple distribution
	// columns, but choosing all composite PK members can create unnecessary data
	// motion and may include unsupported temporal/LOB types. The complete PK is
	// still used by MERGE; the selected HASH key is a subset of that predicate.
	for _, key := range primaryKeys {
		col, ok := byName[strings.ToLower(key)]
		if !ok {
			return nil, fmt.Errorf("GBase migration key %s is not in target columns", key)
		}
		if gbaseHashEligibleType(gbaseTargetType(col)) {
			return []string{key}, nil
		}
	}
	return nil, fmt.Errorf("GBase 8a auto-target cannot choose a HASH distribution key from migration key %v; pre-create a HASH target whose distribution columns are part of the migration key", primaryKeys)
}

func gbaseDistributionFromDDL(ddl string) (kind string, columns []string) {
	u := strings.ToUpper(ddl)
	if strings.Contains(u, " REPLICATED") || strings.HasSuffix(strings.TrimSpace(u), "REPLICATED") {
		return "replicated", nil
	}
	idx := strings.Index(u, "DISTRIBUTED BY")
	if idx < 0 {
		return "random", nil
	}
	rest := strings.TrimSpace(ddl[idx+len("DISTRIBUTED BY"):])
	upRest := strings.ToUpper(rest)
	if strings.HasPrefix(upRest, "HASH") {
		rest = strings.TrimSpace(rest[len("HASH"):])
	}
	open := strings.IndexByte(rest, '(')
	if open < 0 {
		return "hash", nil
	}
	close := strings.IndexByte(rest[open+1:], ')')
	if close < 0 {
		return "hash", nil
	}
	body := rest[open+1 : open+1+close]
	for _, raw := range strings.Split(body, ",") {
		name := strings.TrimSpace(raw)
		name = strings.Trim(name, "`\\\"' ")
		if name != "" {
			columns = append(columns, name)
		}
	}
	return "hash", columns
}

func (c *Connector) validateGBaseMergeLayout(ctx context.Context, schema, table string, mergeKeys []string) error {
	cacheKey := strings.ToLower(schema + "\x00" + table + "\x00" + strings.Join(mergeKeys, "\x00"))
	c.gbaseLayoutMu.Lock()
	if c.gbaseLayoutValidated != nil && c.gbaseLayoutValidated[cacheKey] {
		c.gbaseLayoutMu.Unlock()
		return nil
	}
	c.gbaseLayoutMu.Unlock()

	p, err := c.get(ctx)
	if err != nil {
		return err
	}
	r, err := p.query(ctx, "SHOW CREATE TABLE "+quoteIdent(schema)+"."+quoteIdent(table))
	if err != nil {
		return fmt.Errorf("inspect GBase target distribution: %w", err)
	}
	if len(r.rows) == 0 || len(r.rows[0]) < 2 {
		return errors.New("SHOW CREATE TABLE returned no GBase target DDL")
	}
	ddl := string(r.rows[0][1])
	kind, distCols := gbaseDistributionFromDDL(ddl)
	if kind != "hash" || len(distCols) == 0 {
		return fmt.Errorf("GBase 8a idempotent MERGE requires a HASH-distributed target; %s.%s is %s/unknown distribution. Pre-create a HASH target or let QMigration create it", schema, table, kind)
	}
	keys := make(map[string]struct{}, len(mergeKeys))
	for _, key := range mergeKeys {
		keys[strings.ToLower(strings.TrimSpace(key))] = struct{}{}
	}
	for _, col := range distCols {
		if _, ok := keys[strings.ToLower(col)]; !ok {
			return fmt.Errorf("GBase 8a MERGE distribution column %s is not part of migration key %v; MERGE ON must include every HASH distribution column", col, mergeKeys)
		}
	}
	c.gbaseLayoutMu.Lock()
	if c.gbaseLayoutValidated == nil {
		c.gbaseLayoutValidated = map[string]bool{}
	}
	c.gbaseLayoutValidated[cacheKey] = true
	c.gbaseLayoutMu.Unlock()
	return nil
}

func (c *Connector) createGBaseTable(ctx context.Context, schema, table string, columns []domain.ColumnInfo, primaryKeys []string) error {
	if schema == "" || table == "" {
		return errors.New("schema/table is empty")
	}
	distKeys, err := gbaseChooseDistributionKeys(columns, primaryKeys)
	if err != nil {
		return err
	}
	p, err := c.get(ctx)
	if err != nil {
		return err
	}
	defs := make([]string, 0, len(columns)+1)
	for _, col := range columns {
		if strings.Contains(strings.ToUpper(col.Extra), "GENERATED") {
			continue
		}
		typ := gbaseTargetType(col)
		if typ == "" {
			return fmt.Errorf("column %s has empty GBase type", col.Name)
		}
		d := quoteIdent(col.Name) + " " + typ
		if !col.Nullable {
			d += " NOT NULL"
		} else {
			d += " NULL"
		}
		// QMigration copies explicit source values. AUTO_INCREMENT is deliberately
		// not enabled on an automatically-created GBase target because GBase 8a
		// commonly rejects explicit inserts into such columns unless a vendor
		// session parameter is enabled. Generator restoration remains manual.
		defs = append(defs, d)
	}
	if len(primaryKeys) > 0 {
		quoted := make([]string, len(primaryKeys))
		for i, key := range primaryKeys {
			quoted[i] = quoteIdent(key)
		}
		defs = append(defs, "PRIMARY KEY ("+strings.Join(quoted, ",")+")")
	}
	if len(defs) == 0 {
		return errors.New("no columns to create")
	}
	quotedDist := make([]string, len(distKeys))
	for i, key := range distKeys {
		// GBase V9 SHOW CREATE TABLE normalizes this form to DISTRIBUTED BY('id').
		// Quote as a string-style identifier to match the vendor syntax accepted by
		// both older V8/V9 examples and current V9 clusters.
		quotedDist[i] = "'" + strings.ReplaceAll(key, "'", "''") + "'"
	}
	q := "CREATE TABLE IF NOT EXISTS " + quoteIdent(schema) + "." + quoteIdent(table) + " (" + strings.Join(defs, ",") + ") ENGINE=EXPRESS DISTRIBUTED BY(" + strings.Join(quotedDist, ",") + ") DEFAULT CHARSET=utf8mb4"
	if _, err = p.exec(ctx, q); err != nil {
		return err
	}
	// CREATE TABLE IF NOT EXISTS may have found a pre-existing table with a
	// different layout. Validate the actual catalog DDL before claiming that the
	// target is safe for retryable staging+MERGE writes.
	return c.validateGBaseMergeLayout(ctx, schema, table, primaryKeys)
}

func (c *Connector) writeGBaseBatch(ctx context.Context, req connector.WriteBatchRequest) (int64, error) {
	if len(req.PrimaryKeys) == 0 {
		return 0, errors.New("GBase 8a target Full Write requires a primary/migration key for idempotent MERGE replay")
	}
	if err := c.validateGBaseMergeLayout(ctx, req.Schema, req.Table, req.PrimaryKeys); err != nil {
		return 0, err
	}
	p, err := c.get(ctx)
	if err != nil {
		return 0, err
	}
	cols := make([]domain.ColumnInfo, 0, len(req.Columns))
	for _, col := range req.Columns {
		if strings.Contains(strings.ToUpper(col.Extra), "GENERATED") {
			continue
		}
		cols = append(cols, col)
	}
	if len(cols) == 0 {
		return 0, errors.New("no target columns")
	}
	for ri, row := range req.Rows {
		if len(row) != len(cols) {
			return 0, fmt.Errorf("row column count %d != %d", len(row), len(cols))
		}
		if err := validateMySQLValues(cols, row); err != nil {
			return 0, fmt.Errorf("target row %d: %w", ri, err)
		}
	}
	byName := make(map[string]domain.ColumnInfo, len(cols))
	for _, col := range cols {
		byName[strings.ToLower(col.Name)] = col
	}
	for _, key := range req.PrimaryKeys {
		if _, ok := byName[strings.ToLower(key)]; !ok {
			return 0, fmt.Errorf("GBase MERGE key %s is not in target columns", key)
		}
	}
	seq := gbaseStageCounter.Add(1)
	base := req.Table
	if len(base) > 24 {
		base = base[:24]
	}
	stage := fmt.Sprintf("_qm_%s_%x_%x", base, uint64(time.Now().UnixNano()), seq)
	if len(stage) > 64 {
		stage = stage[:64]
	}
	qStage := quoteIdent(req.Schema) + "." + quoteIdent(stage)
	qTarget := quoteIdent(req.Schema) + "." + quoteIdent(req.Table)
	if _, err := p.exec(ctx, "CREATE TABLE "+qStage+" LIKE "+qTarget); err != nil {
		return 0, fmt.Errorf("create GBase staging table: %w", err)
	}
	defer func() { _, _ = p.exec(context.Background(), "DROP TABLE IF EXISTS "+qStage) }()

	idents := make([]string, len(cols))
	for i, col := range cols {
		idents[i] = quoteIdent(col.Name)
	}
	var ins strings.Builder
	ins.WriteString("INSERT INTO ")
	ins.WriteString(qStage)
	ins.WriteString(" (")
	ins.WriteString(strings.Join(idents, ","))
	ins.WriteString(") VALUES ")
	for ri, row := range req.Rows {
		if ri > 0 {
			ins.WriteByte(',')
		}
		ins.WriteByte('(')
		for ci, v := range row {
			if ci > 0 {
				ins.WriteByte(',')
			}
			ins.WriteString(formatValue(cols[ci], v))
		}
		ins.WriteByte(')')
	}
	if _, err := p.exec(ctx, ins.String()); err != nil {
		return 0, fmt.Errorf("load GBase staging table: %w", err)
	}
	conds := make([]string, len(req.PrimaryKeys))
	for i, key := range req.PrimaryKeys {
		q := quoteIdent(key)
		conds[i] = "qm_t." + q + "=qm_s." + q
	}
	updates := make([]string, 0, len(cols))
	for _, col := range cols {
		q := quoteIdent(col.Name)
		isKey := false
		for _, key := range req.PrimaryKeys {
			if strings.EqualFold(key, col.Name) {
				isKey = true
				break
			}
		}
		if !isKey {
			updates = append(updates, "qm_t."+q+"=qm_s."+q)
		}
	}
	insertVals := make([]string, len(cols))
	for i, col := range cols {
		insertVals[i] = "qm_s." + quoteIdent(col.Name)
	}
	var merge strings.Builder
	merge.WriteString("MERGE INTO ")
	merge.WriteString(qTarget)
	merge.WriteString(" qm_t USING ")
	merge.WriteString(qStage)
	merge.WriteString(" qm_s ON (")
	merge.WriteString(strings.Join(conds, " AND "))
	merge.WriteByte(')')
	if len(updates) > 0 {
		merge.WriteString(" WHEN MATCHED THEN UPDATE SET ")
		merge.WriteString(strings.Join(updates, ","))
	}
	merge.WriteString(" WHEN NOT MATCHED THEN INSERT (")
	merge.WriteString(strings.Join(idents, ","))
	merge.WriteString(") VALUES (")
	merge.WriteString(strings.Join(insertVals, ","))
	merge.WriteByte(')')
	if _, err := p.exec(ctx, merge.String()); err != nil {
		return 0, fmt.Errorf("GBase MERGE apply: %w", err)
	}
	return int64(len(req.Rows)), nil
}

func (c *Connector) gbasePrechecks(ctx context.Context, needCDC bool) []domain.PrecheckItem {
	items := []domain.PrecheckItem{}
	p, err := c.get(ctx)
	if err != nil {
		return []domain.PrecheckItem{{Name: "gbase8a_connection", Level: domain.PrecheckFailed, Message: err.Error()}}
	}
	if v, err := c.GetVersion(ctx); err != nil {
		items = append(items, domain.PrecheckItem{Name: "gbase8a_version", Level: domain.PrecheckWarning, Message: err.Error()})
	} else {
		items = append(items, domain.PrecheckItem{Name: "gbase8a_version", Level: domain.PrecheckPass, Message: v})
	}
	if r, err := p.query(ctx, "SELECT @@character_set_server,@@character_set_client,@@character_set_results"); err == nil && len(r.rows) > 0 && len(r.rows[0]) >= 3 {
		items = append(items, domain.PrecheckItem{Name: "gbase8a_charset", Level: domain.PrecheckPass, Message: fmt.Sprintf("server=%s client=%s results=%s", r.rows[0][0], r.rows[0][1], r.rows[0][2])})
	} else if err != nil {
		items = append(items, domain.PrecheckItem{Name: "gbase8a_charset", Level: domain.PrecheckWarning, Message: err.Error()})
	}
	items = append(items, domain.PrecheckItem{Name: "gbase8a_full_write_semantics", Level: domain.PrecheckPass, Message: "QMigration target replay uses per-batch staging table + MERGE keyed by the migration primary key; target HASH distribution is validated before MERGE and keyless target Full Write is rejected"})
	items = append(items, domain.PrecheckItem{Name: "gbase8a_distribution_policy", Level: domain.PrecheckWarning, Message: "auto-created target tables use a HASH distribution column selected from the stable migration key because GBase MERGE requires HASH distribution; pre-create a workload-tuned HASH table when another key is required"})
	items = append(items, domain.PrecheckItem{Name: "gbase8a_foreign_keys", Level: domain.PrecheckWarning, Message: "GBase 8a does not provide foreign-key enforcement; QMigration RC18 does not replay source FKs to GBase targets"})
	if needCDC {
		items = append(items, domain.PrecheckItem{Name: "gbase8a_source_cdc", Level: domain.PrecheckFailed, Message: "GBase 8a source CDC is not advertised in RC27 because no retained public row-change feed with exact row images has been qualified; use GBase only as Full source or as an explicitly gated CDC target"})
	}
	return items
}

func formatValue(col domain.ColumnInfo, v connector.Value) string {
	if v.Null {
		return "NULL"
	}
	dt := strings.ToLower(col.DataType)
	raw := string(v.Raw)
	switch dt {
	case "tinyint", "smallint", "mediumint", "int", "integer", "bigint", "decimal", "numeric", "float", "double", "real":
		return raw
	case "binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob", "bit", "bytea":
		return "X'" + fmt.Sprintf("%x", v.Raw) + "'"
	case "boolean", "bool":
		if raw == "1" || strings.EqualFold(raw, "true") || raw == "t" {
			return "1"
		}
		return "0"
	default:
		return quoteSQLString(raw)
	}
}

var _ connector.DataConnector = (*Connector)(nil)
var _ connector.PointLookupConnector = (*Connector)(nil)

func (c *Connector) EnsureSchema(ctx context.Context, schema string) error {
	if schema == "" {
		return errors.New("schema is empty")
	}
	p, err := c.get(ctx)
	if err != nil {
		return err
	}
	q := "CREATE DATABASE IF NOT EXISTS " + quoteIdent(schema) + " CHARACTER SET utf8mb4"
	if c.ds.Type == domain.DataSourceGBase {
		q = "CREATE DATABASE IF NOT EXISTS " + quoteIdent(schema) + " DEFAULT CHARACTER SET utf8mb4"
	}
	_, err = p.exec(ctx, q)
	return err
}

func mysqlTargetType(col domain.ColumnInfo) string {
	ct := strings.ToLower(col.ColumnType)
	dt := strings.ToLower(col.DataType)
	if strings.Contains(ct, "character varying") {
		return strings.Replace(ct, "character varying", "varchar", 1)
	}
	if strings.HasPrefix(ct, "character(") {
		return strings.Replace(ct, "character", "char", 1)
	}
	if strings.HasPrefix(ct, "timestamp without time zone") || strings.HasPrefix(ct, "timestamp with time zone") {
		return "datetime"
	}
	if strings.HasPrefix(ct, "time without time zone") || strings.HasPrefix(ct, "time with time zone") {
		return "time"
	}
	switch dt {
	case "smallint", "int2":
		return "smallint"
	case "integer", "int4":
		return "int"
	case "bigint", "int8":
		return "bigint"
	case "boolean", "bool":
		return "tinyint(1)"
	case "bytea":
		return "longblob"
	case "jsonb", "json":
		return "json"
	case "uuid":
		return "char(36)"
	case "double", "double precision", "float8":
		return "double"
	case "real", "float4":
		return "float"
	case "numeric", "decimal":
		if strings.HasPrefix(ct, "numeric") {
			return strings.Replace(ct, "numeric", "decimal", 1)
		}
		return "decimal(65,10)"
	case "text":
		return "longtext"
	case "date":
		return "date"
	}
	if ct != "" && !strings.Contains(ct, "[]") {
		return ct
	}
	return "longtext"
}

func (c *Connector) CreateTable(ctx context.Context, schema, table string, columns []domain.ColumnInfo, primaryKey string) error {
	keys := []string{}
	if primaryKey != "" {
		keys = append(keys, primaryKey)
	}
	return c.CreateTableWithPrimaryKeys(ctx, schema, table, columns, keys)
}

func (c *Connector) CreateTableWithPrimaryKeys(ctx context.Context, schema, table string, columns []domain.ColumnInfo, primaryKeys []string) error {
	if c.ds.Type == domain.DataSourceGBase {
		return c.createGBaseTable(ctx, schema, table, columns, primaryKeys)
	}
	if schema == "" || table == "" {
		return errors.New("schema/table is empty")
	}
	p, err := c.get(ctx)
	if err != nil {
		return err
	}
	defs := make([]string, 0, len(columns)+1)
	for _, col := range columns {
		if strings.Contains(strings.ToUpper(col.Extra), "GENERATED") {
			continue
		}
		typ := mysqlTargetType(col)
		if typ == "" {
			return fmt.Errorf("column %s has empty type", col.Name)
		}
		d := quoteIdent(col.Name) + " " + typ
		if !col.Nullable {
			d += " NOT NULL"
		} else {
			d += " NULL"
		}
		if strings.Contains(strings.ToLower(col.Extra), "auto_increment") {
			d += " AUTO_INCREMENT"
		}
		defs = append(defs, d)
	}
	if len(primaryKeys) > 0 {
		quoted := make([]string, len(primaryKeys))
		for i, key := range primaryKeys {
			quoted[i] = quoteIdent(key)
		}
		defs = append(defs, "PRIMARY KEY ("+strings.Join(quoted, ",")+")")
	}
	if len(defs) == 0 {
		return errors.New("no columns to create")
	}
	sql := "CREATE TABLE IF NOT EXISTS " + quoteIdent(schema) + "." + quoteIdent(table) + " (" + strings.Join(defs, ",") + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
	_, err = p.exec(ctx, sql)
	return err
}

func (c *Connector) CreateIndex(ctx context.Context, schema, table string, idx domain.IndexInfo) error {
	if idx.Primary || len(idx.Columns) == 0 {
		return nil
	}
	p, err := c.get(ctx)
	if err != nil {
		return err
	}
	name := idx.Name
	if name == "" {
		name = "idx_" + table + "_" + strings.Join(idx.Columns, "_")
	}
	if len(name) > 64 {
		name = name[:64]
	}
	cols := make([]string, len(idx.Columns))
	for i, col := range idx.Columns {
		cols[i] = quoteIdent(col)
	}
	prefix := "CREATE INDEX "
	if idx.Unique {
		prefix = "CREATE UNIQUE INDEX "
	}
	_, err = p.exec(ctx, prefix+quoteIdent(name)+" ON "+quoteIdent(schema)+"."+quoteIdent(table)+" ("+strings.Join(cols, ",")+")")
	return err
}

func (c *Connector) CreateForeignKey(ctx context.Context, schema, table string, fk domain.ForeignKeyInfo) error {
	if len(fk.Columns) == 0 || len(fk.Columns) != len(fk.ReferencedColumns) || fk.ReferencedTable == "" {
		return errors.New("invalid foreign key metadata")
	}
	p, err := c.get(ctx)
	if err != nil {
		return err
	}
	name := fk.Name
	if name == "" {
		name = "fk_" + table + "_" + strings.Join(fk.Columns, "_")
	}
	if len(name) > 64 {
		name = name[:64]
	}
	cols := make([]string, len(fk.Columns))
	refs := make([]string, len(fk.ReferencedColumns))
	for i := range fk.Columns {
		cols[i] = quoteIdent(fk.Columns[i])
		refs[i] = quoteIdent(fk.ReferencedColumns[i])
	}
	refSchema := fk.ReferencedSchema
	if refSchema == "" {
		refSchema = schema
	}
	q := "ALTER TABLE " + quoteIdent(schema) + "." + quoteIdent(table) + " ADD CONSTRAINT " + quoteIdent(name) + " FOREIGN KEY (" + strings.Join(cols, ",") + ") REFERENCES " + quoteIdent(refSchema) + "." + quoteIdent(fk.ReferencedTable) + " (" + strings.Join(refs, ",") + ")"
	_, err = p.exec(ctx, q)
	return err
}

var _ connector.SchemaConnector = (*Connector)(nil)
var _ connector.CompositeSchemaConnector = (*Connector)(nil)
var _ connector.PostLoadSchemaConnector = (*Connector)(nil)

func (c *Connector) binaryLogStatus(ctx context.Context) (*queryResult, int64, error) {
	capturedAt := time.Now().UnixMilli()
	p, err := c.get(ctx)
	if err != nil {
		return nil, capturedAt, err
	}
	r, err := p.query(ctx, "SHOW BINARY LOG STATUS")
	if err != nil {
		r, err = p.query(ctx, "SHOW MASTER STATUS")
	}
	return r, capturedAt, err
}

func (c *Connector) CurrentBinlogPosition(ctx context.Context) (*domain.CDCPosition, error) {
	if !mysqlBinlogSourceSupported(c.ds.Type) {
		return nil, fmt.Errorf("%s does not expose source CDC through QMigration MySQL COM_BINLOG_DUMP", c.ds.Type)
	}
	r, capturedAt, err := c.binaryLogStatus(ctx)
	if err != nil {
		return nil, err
	}
	if len(r.rows) == 0 {
		return nil, errors.New("binary log status returned no row; binlog may be disabled")
	}
	idx := func(name string) int {
		for i, col := range r.columns {
			if strings.EqualFold(col, name) {
				return i
			}
		}
		return -1
	}
	fileI, posI := idx("File"), idx("Position")
	if fileI < 0 || posI < 0 || fileI >= len(r.rows[0]) || posI >= len(r.rows[0]) {
		return nil, errors.New("binary log status did not return File/Position")
	}
	file := string(r.rows[0][fileI])
	pos := string(r.rows[0][posI])
	return &domain.CDCPosition{DatabaseType: string(c.ds.Type), PositionType: "BINLOG", PositionValue: file + ":" + pos, Resource: file, SourceTimestampMS: capturedAt}, nil
}

func (c *Connector) CurrentCDCPosition(ctx context.Context) (*domain.CDCPosition, error) {
	if c.ds.Type == domain.DataSourceTiDB {
		return c.currentTiDBPosition(ctx)
	}
	if c.ds.Type == domain.DataSourceOceanBase {
		return c.currentOceanBasePosition(ctx)
	}
	if !mysqlBinlogSourceSupported(c.ds.Type) {
		return nil, fmt.Errorf("%s source CDC requires a dedicated CDC endpoint adapter", c.ds.Type)
	}
	r, capturedAt, err := c.binaryLogStatus(ctx)
	if err != nil {
		return nil, err
	}
	if len(r.rows) == 0 {
		return nil, errors.New("binary log status returned no row; binlog may be disabled")
	}
	row := r.rows[0]
	idx := func(name string) int {
		for i, c := range r.columns {
			if strings.EqualFold(c, name) {
				return i
			}
		}
		return -1
	}
	fileI, posI, gtidI := idx("File"), idx("Position"), idx("Executed_Gtid_Set")
	out := &domain.CDCPosition{DatabaseType: string(c.ds.Type), SourceTimestampMS: capturedAt}
	if gtidI >= 0 && gtidI < len(row) && len(row[gtidI]) > 0 {
		out.PositionType = "GTID"
		out.PositionValue = string(row[gtidI])
		if fileI >= 0 && fileI < len(row) {
			out.Resource = string(row[fileI])
		}
	} else if fileI >= 0 && posI >= 0 {
		out.PositionType = "BINLOG"
		out.PositionValue = string(row[fileI]) + ":" + string(row[posI])
	} else {
		return nil, errors.New("unrecognized binary log status result")
	}
	return out, nil
}

func parseBinaryLogPosition(dsType domain.DataSourceType, r *queryResult, capturedAt int64) (*domain.CDCPosition, error) {
	if r == nil || len(r.rows) == 0 {
		return nil, errors.New("binary log status returned no row; binlog service may be disabled or not ready")
	}
	row := r.rows[0]
	idx := func(name string) int {
		for i, col := range r.columns {
			if strings.EqualFold(col, name) {
				return i
			}
		}
		return -1
	}
	fileI, posI, gtidI := idx("File"), idx("Position"), idx("Executed_Gtid_Set")
	out := &domain.CDCPosition{DatabaseType: string(dsType), SourceTimestampMS: capturedAt}
	if gtidI >= 0 && gtidI < len(row) && strings.TrimSpace(string(row[gtidI])) != "" {
		out.PositionType = "GTID"
		out.PositionValue = strings.TrimSpace(string(row[gtidI]))
		if fileI >= 0 && fileI < len(row) {
			out.Resource = string(row[fileI])
		}
		return out, nil
	}
	if fileI >= 0 && posI >= 0 && fileI < len(row) && posI < len(row) {
		out.PositionType = "BINLOG"
		out.PositionValue = string(row[fileI]) + ":" + string(row[posI])
		out.Resource = string(row[fileI])
		return out, nil
	}
	return nil, errors.New("unrecognized binary log status result")
}

func (c *Connector) dialOceanBaseBinlog(ctx context.Context) (*protocolClient, obbinlog.Endpoint, obbinlog.Address, error) {
	subDS, ep, err := obbinlog.DataSourceForSubscription(c.ds)
	if err != nil {
		return nil, obbinlog.Endpoint{}, obbinlog.Address{}, err
	}
	var errs []string
	for _, addr := range ep.Addresses() {
		candidate := subDS
		candidate.Host, candidate.Port = addr.Host, addr.Port
		p, e := dialProtocol(ctx, candidate)
		if e == nil {
			return p, ep, addr, nil
		}
		errs = append(errs, addr.String()+": "+e.Error())
	}
	return nil, ep, obbinlog.Address{}, fmt.Errorf("connect OceanBase Binlog subscription endpoint through ODP: %s", strings.Join(errs, "; "))
}

func (c *Connector) currentOceanBasePosition(ctx context.Context) (*domain.CDCPosition, error) {
	p, _, addr, err := c.dialOceanBaseBinlog(ctx)
	if err != nil {
		return nil, err
	}
	defer p.close()
	capturedAt := time.Now().UnixMilli()
	r, err := p.query(ctx, "SHOW BINARY LOG STATUS")
	if err != nil {
		r, err = p.query(ctx, "SHOW MASTER STATUS")
	}
	if err != nil {
		return nil, fmt.Errorf("OceanBase Binlog Service SHOW MASTER STATUS through ODP %s: %w", addr.String(), err)
	}
	pos, err := parseBinaryLogPosition(c.ds.Type, r, capturedAt)
	if err == nil && pos.Resource == "" {
		pos.Resource = addr.String()
	}
	return pos, err
}

func (c *Connector) currentTiDBPosition(ctx context.Context) (*domain.CDCPosition, error) {
	if _, err := ticdc.ParseEndpoint(c.ds.CDCURL); err != nil {
		return nil, err
	}
	p, err := c.get(ctx)
	if err != nil {
		return nil, err
	}
	capturedAt := time.Now().UnixMilli()
	r, err := p.query(ctx, "SELECT TIDB_CURRENT_TSO()")
	if err != nil {
		r, err = p.query(ctx, "SELECT @@tidb_current_ts")
	}
	if err != nil {
		return nil, fmt.Errorf("capture TiDB current TSO: %w", err)
	}
	if len(r.rows) == 0 || len(r.rows[0]) == 0 {
		return nil, errors.New("TiDB current TSO query returned no value")
	}
	tso, err := strconv.ParseUint(strings.TrimSpace(string(r.rows[0][0])), 10, 64)
	if err != nil || tso == 0 {
		return nil, fmt.Errorf("invalid TiDB current TSO %q", string(r.rows[0][0]))
	}
	pos := ticdc.Position{TSO: tso, Offset: 0}
	return &domain.CDCPosition{DatabaseType: string(c.ds.Type), PositionType: "TIDB_TSO", PositionValue: pos.String(), Resource: "TiCDC", SourceTimestampMS: capturedAt}, nil
}

func (c *Connector) MigrationPrechecks(ctx context.Context, needCDC bool) []domain.PrecheckItem {
	if c.ds.Type == domain.DataSourceGBase {
		return c.gbasePrechecks(ctx, needCDC)
	}
	items := []domain.PrecheckItem{}
	p, err := c.get(ctx)
	if err != nil {
		return []domain.PrecheckItem{{Name: "mysql_connection", Level: domain.PrecheckFailed, Message: err.Error()}}
	}
	if needCDC && c.ds.Type == domain.DataSourceTiDB {
		ep, e := ticdc.ParseEndpoint(c.ds.CDCURL)
		if e != nil {
			return append(items, domain.PrecheckItem{Name: "tidb_ticdc_endpoint", Level: domain.PrecheckFailed, Message: e.Error()})
		}
		control := ticdc.NewControlClient(ep, nil)
		probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if e := control.Health(probeCtx); e != nil {
			items = append(items, domain.PrecheckItem{Name: "tidb_ticdc_health", Level: domain.PrecheckFailed, Message: e.Error()})
		} else {
			items = append(items, domain.PrecheckItem{Name: "tidb_ticdc_health", Level: domain.PrecheckPass, Message: ep.ControlURL + " healthy"})
		}
		if e := ticdc.ProbeBrokers(ep, 2*time.Second); e != nil {
			items = append(items, domain.PrecheckItem{Name: "tidb_ticdc_kafka", Level: domain.PrecheckFailed, Message: e.Error()})
		} else {
			items = append(items, domain.PrecheckItem{Name: "tidb_ticdc_kafka", Level: domain.PrecheckPass, Message: "Kafka bootstrap broker reachable"})
		}
		if pos, e := c.currentTiDBPosition(probeCtx); e != nil {
			items = append(items, domain.PrecheckItem{Name: "tidb_tso", Level: domain.PrecheckFailed, Message: e.Error()})
		} else {
			items = append(items, domain.PrecheckItem{Name: "tidb_tso", Level: domain.PrecheckPass, Message: pos.PositionValue})
		}
		return items
	}
	if needCDC && c.ds.Type == domain.DataSourceOceanBase {
		if _, e := obbinlog.ParseEndpoint(c.ds.CDCURL); e != nil {
			return append(items, domain.PrecheckItem{Name: "oceanbase_binlog_odp_endpoint", Level: domain.PrecheckFailed, Message: e.Error()})
		}
		probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		sub, ep, active, e := c.dialOceanBaseBinlog(probeCtx)
		if e != nil {
			return append(items, domain.PrecheckItem{Name: "oceanbase_binlog_odp_connection", Level: domain.PrecheckFailed, Message: e.Error()})
		}
		defer sub.close()
		items = append(items, domain.PrecheckItem{Name: "oceanbase_binlog_odp_connection", Level: domain.PrecheckPass, Message: active.String() + " reachable with tenant datasource credentials; configured endpoints=" + fmt.Sprint(len(ep.Addresses()))})
		if active.Port == 2983 {
			items = append(items, domain.PrecheckItem{Name: "oceanbase_binlog_endpoint_role", Level: domain.PrecheckWarning, Message: "port 2983 is the common Binlog Server management/service port; verify this endpoint is an ODP tenant subscription listener, not the management endpoint"})
		}
		r, e := sub.query(probeCtx, "SHOW MASTER STATUS")
		if e != nil {
			items = append(items, domain.PrecheckItem{Name: "oceanbase_binlog_master_status", Level: domain.PrecheckFailed, Message: e.Error()})
			return items
		}
		pos, e := parseBinaryLogPosition(c.ds.Type, r, time.Now().UnixMilli())
		if e != nil {
			items = append(items, domain.PrecheckItem{Name: "oceanbase_binlog_master_status", Level: domain.PrecheckFailed, Message: e.Error()})
			return items
		}
		items = append(items, domain.PrecheckItem{Name: "oceanbase_binlog_master_status", Level: domain.PrecheckPass, Message: pos.PositionType + " " + pos.PositionValue})
		if logs, qerr := sub.query(probeCtx, "SHOW BINARY LOGS"); qerr != nil {
			items = append(items, domain.PrecheckItem{Name: "oceanbase_binlog_files", Level: domain.PrecheckWarning, Message: qerr.Error()})
		} else if len(logs.rows) == 0 {
			items = append(items, domain.PrecheckItem{Name: "oceanbase_binlog_files", Level: domain.PrecheckWarning, Message: "SHOW BINARY LOGS returned no files; Binlog Service may still be starting"})
		} else {
			items = append(items, domain.PrecheckItem{Name: "oceanbase_binlog_files", Level: domain.PrecheckPass, Message: fmt.Sprintf("%d binlog file(s) visible through ODP", len(logs.rows))})
		}
		items = append(items, domain.PrecheckItem{Name: "oceanbase_binlog_protocol", Level: domain.PrecheckPass, Message: "QMigration will use MySQL Binlog V4 row events and prefer GTID resumable subscription when Executed_Gtid_Set is available"})
		return items
	}
	variable := func(name string) (string, error) {
		r, err := p.query(ctx, "SHOW GLOBAL VARIABLES LIKE "+quoteSQLString(name))
		if err != nil {
			return "", err
		}
		if len(r.rows) == 0 || len(r.rows[0]) < 2 {
			return "", fmt.Errorf("variable %s not found", name)
		}
		return string(r.rows[0][1]), nil
	}
	if r, err := p.query(ctx, "SELECT @@character_set_server,@@collation_server,@@time_zone"); err == nil && len(r.rows) > 0 && len(r.rows[0]) >= 3 {
		items = append(items, domain.PrecheckItem{Name: "mysql_charset_timezone", Level: domain.PrecheckPass, Message: fmt.Sprintf("charset=%s collation=%s timezone=%s", r.rows[0][0], r.rows[0][1], r.rows[0][2])})
	}
	if !needCDC {
		return items
	}
	if c.ds.Type == domain.DataSourcePolarDBX && c.ds.TLSMode != domain.TLSModeDisable {
		items = append(items, domain.PrecheckItem{Name: "polardbx_binlog_tls", Level: domain.PrecheckWarning, Message: "PolarDB-X global Binlog is MySQL-compatible, but TLS support for replication subscriptions is version/deployment dependent; qualify the exact endpoint before production"})
	}
	if v, err := variable("log_bin"); err != nil {
		items = append(items, domain.PrecheckItem{Name: "mysql_log_bin", Level: domain.PrecheckFailed, Message: err.Error()})
	} else if strings.EqualFold(v, "ON") || v == "1" {
		items = append(items, domain.PrecheckItem{Name: "mysql_log_bin", Level: domain.PrecheckPass, Message: "binary logging enabled"})
	} else {
		items = append(items, domain.PrecheckItem{Name: "mysql_log_bin", Level: domain.PrecheckFailed, Message: "log_bin must be ON for incremental migration"})
	}
	if v, err := variable("binlog_format"); err != nil {
		items = append(items, domain.PrecheckItem{Name: "mysql_binlog_format", Level: domain.PrecheckFailed, Message: err.Error()})
	} else if strings.EqualFold(v, "ROW") {
		items = append(items, domain.PrecheckItem{Name: "mysql_binlog_format", Level: domain.PrecheckPass, Message: "ROW"})
	} else {
		items = append(items, domain.PrecheckItem{Name: "mysql_binlog_format", Level: domain.PrecheckFailed, Message: "binlog_format must be ROW, current=" + v})
	}
	if v, err := variable("binlog_row_image"); err == nil {
		level := domain.PrecheckPass
		message := v
		if !strings.EqualFold(v, "FULL") {
			level = domain.PrecheckWarning
			message = v + "; FULL is recommended for generic row-event replay and before/after validation"
		}
		items = append(items, domain.PrecheckItem{Name: "mysql_binlog_row_image", Level: level, Message: message})
	}
	if v, err := variable("binlog_row_value_options"); err == nil {
		level := domain.PrecheckPass
		message := v
		if strings.Contains(strings.ToUpper(v), "PARTIAL_JSON") {
			message = v + "; native CDC will rebuild partial JSON after-images from the FULL before-image"
		}
		items = append(items, domain.PrecheckItem{Name: "mysql_binlog_row_value_options", Level: level, Message: message})
	}
	if v, err := variable("binlog_transaction_compression"); err == nil {
		level := domain.PrecheckPass
		message := v
		if strings.EqualFold(v, "ON") || v == "1" {
			level = domain.PrecheckWarning
			message = v + "; native CDC can decode TRANSACTION_PAYLOAD_EVENT when a zstd executable is available on the native CDC worker; set QMIGRATION_ZSTD_BIN when zstd is not on PATH"
		}
		items = append(items, domain.PrecheckItem{Name: "mysql_binlog_transaction_compression", Level: level, Message: message})
	}
	if v, err := variable("gtid_mode"); err == nil {
		level := domain.PrecheckWarning
		message := v + "; native CDC will use file:position recovery"
		if strings.EqualFold(v, "ON") {
			level = domain.PrecheckPass
			message = "GTID ON; native CDC will prefer COM_BINLOG_DUMP_GTID and durable GTID-set recovery"
		}
		items = append(items, domain.PrecheckItem{Name: "mysql_gtid_mode", Level: level, Message: message})
	}
	if v, err := variable("binlog_expire_logs_seconds"); err == nil {
		seconds, _ := strconv.ParseInt(v, 10, 64)
		level := domain.PrecheckPass
		message := v + " seconds"
		if seconds > 0 && seconds < 86400 {
			level = domain.PrecheckWarning
			message += "; less than 24h may be too short for large full-load migrations"
		}
		items = append(items, domain.PrecheckItem{Name: "mysql_binlog_retention", Level: level, Message: message})
	}
	if grants, err := p.query(ctx, "SHOW GRANTS"); err == nil {
		text := ""
		for _, row := range grants.rows {
			if len(row) > 0 {
				text += " " + strings.ToUpper(string(row[0]))
			}
		}
		if strings.Contains(text, "REPLICATION SLAVE") || strings.Contains(text, "REPLICATION REPLICA") || strings.Contains(text, "ALL PRIVILEGES") {
			items = append(items, domain.PrecheckItem{Name: "mysql_replication_privilege", Level: domain.PrecheckPass, Message: "replication privilege detected"})
		} else {
			items = append(items, domain.PrecheckItem{Name: "mysql_replication_privilege", Level: domain.PrecheckWarning, Message: "REPLICATION SLAVE/REPLICA privilege was not detected in SHOW GRANTS; external CDC may fail to start"})
		}
	}
	return items
}

var _ connector.MigrationPrecheckConnector = (*Connector)(nil)

var _ connector.CDCSource = (*Connector)(nil)

func (c *Connector) DeleteByKey(ctx context.Context, req connector.DeleteByKeyRequest) error {
	keys := append([]string(nil), req.PrimaryKeys...)
	cols := append([]domain.ColumnInfo(nil), req.Columns...)
	vals := append([]connector.Value(nil), req.Values...)
	if len(keys) == 0 && req.PrimaryKey != "" {
		keys = []string{req.PrimaryKey}
		cols = []domain.ColumnInfo{req.Column}
		vals = []connector.Value{req.Value}
	}
	if len(keys) == 0 || len(keys) != len(cols) || len(keys) != len(vals) {
		return errors.New("delete primary key values are incomplete")
	}
	where := make([]string, len(keys))
	for i := range keys {
		if vals[i].Null {
			return fmt.Errorf("delete primary key %s cannot be null", keys[i])
		}
		where[i] = quoteIdent(keys[i]) + "=" + formatValue(cols[i], vals[i])
	}
	p, err := c.get(ctx)
	if err != nil {
		return err
	}
	q := "DELETE FROM " + quoteIdent(req.Schema) + "." + quoteIdent(req.Table) + " WHERE " + strings.Join(where, " AND ") + " LIMIT 1"
	_, err = p.exec(ctx, q)
	return err
}

func (c *Connector) BeginCDCTransaction(ctx context.Context) error {
	p, err := c.get(ctx)
	if err != nil {
		return err
	}
	_, err = p.exec(ctx, "START TRANSACTION")
	return err
}
func (c *Connector) CommitCDCTransaction(ctx context.Context) error {
	p, err := c.get(ctx)
	if err != nil {
		return err
	}
	_, err = p.exec(ctx, "COMMIT")
	return err
}
func (c *Connector) RollbackCDCTransaction(ctx context.Context) error {
	p, err := c.get(ctx)
	if err != nil {
		return err
	}
	_, err = p.exec(ctx, "ROLLBACK")
	return err
}

var _ connector.CDCApplyConnector = (*Connector)(nil)
var _ connector.TransactionalCDCApplyConnector = (*Connector)(nil)

// ExecDDL executes a DDL statement in the requested MySQL schema. The
// migration service only calls this for explicitly enabled same-family CDC DDL
// replay after verifying that schema/table/column mappings are identity maps.
func (c *Connector) ExecDDL(ctx context.Context, schema, ddl string) error {
	p, err := c.get(ctx)
	if err != nil {
		return err
	}
	if strings.TrimSpace(schema) != "" {
		if _, err := p.exec(ctx, "USE "+quoteIdent(schema)); err != nil {
			return err
		}
	}
	_, err = p.exec(ctx, ddl)
	return err
}

var _ connector.DDLApplyConnector = (*Connector)(nil)

// ExecSQL is intentionally exposed for integration/admin workflows inside
// QMigration (for example E2E fixtures). Migration data paths still use the
// typed ReadBatch/WriteBatch APIs.
func (c *Connector) ExecSQL(ctx context.Context, sql string) error {
	p, err := c.get(ctx)
	if err != nil {
		return err
	}
	_, err = p.exec(ctx, sql)
	return err
}

func resultColumnIndex(r *queryResult, name string) int {
	for i, c := range r.columns {
		if strings.EqualFold(strings.TrimSpace(c), name) {
			return i
		}
	}
	return -1
}

func rowString(row [][]byte, idx int) string {
	if idx < 0 || idx >= len(row) || row[idx] == nil {
		return ""
	}
	return string(row[idx])
}

func (c *Connector) SampleRuntimeLoad(ctx context.Context) (domain.DatabaseRuntimeLoad, error) {
	p, err := c.get(ctx)
	if err != nil {
		return domain.DatabaseRuntimeLoad{}, err
	}
	maxRes, err := p.query(ctx, "SELECT @@max_connections")
	if err != nil {
		return domain.DatabaseRuntimeLoad{}, err
	}
	statusRes, err := p.query(ctx, "SHOW GLOBAL STATUS WHERE Variable_name IN ('Threads_connected','Threads_running')")
	if err != nil {
		return domain.DatabaseRuntimeLoad{}, err
	}
	load := domain.DatabaseRuntimeLoad{}
	if len(maxRes.rows) > 0 && len(maxRes.rows[0]) > 0 {
		load.MaxConnections, _ = strconv.ParseInt(string(maxRes.rows[0][0]), 10, 64)
	}
	nameIdx, valueIdx := resultColumnIndex(statusRes, "Variable_name"), resultColumnIndex(statusRes, "Value")
	if nameIdx < 0 {
		nameIdx = 0
	}
	if valueIdx < 0 {
		valueIdx = 1
	}
	for _, row := range statusRes.rows {
		v, _ := strconv.ParseInt(rowString(row, valueIdx), 10, 64)
		switch strings.ToLower(rowString(row, nameIdx)) {
		case "threads_connected":
			load.Connections = v
		case "threads_running":
			load.RunningQueries = v
		}
	}
	if load.MaxConnections > 0 {
		load.ConnectionUsagePct = float64(load.Connections) * 100 / float64(load.MaxConnections)
	}
	return load, nil
}

func mergeTiDBStoreTopologyLabels(labels map[string]string, raw string) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return
	}
	var pairs []struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if json.Unmarshal([]byte(raw), &pairs) == nil {
		for _, pair := range pairs {
			key := strings.ToLower(strings.TrimSpace(pair.Key))
			value := strings.TrimSpace(pair.Value)
			if value == "" {
				continue
			}
			switch key {
			case "region", "zone", "az", "rack", "rack_id":
				labels[key] = value
			}
		}
		return
	}
	var object map[string]string
	if json.Unmarshal([]byte(raw), &object) == nil {
		for key, value := range object {
			key = strings.ToLower(strings.TrimSpace(key))
			value = strings.TrimSpace(value)
			if value == "" {
				continue
			}
			switch key {
			case "region", "zone", "az", "rack", "rack_id":
				labels[key] = value
			}
		}
		return
	}
	// Some TiDB builds expose the LABEL column as a compact key=value list.
	for _, item := range strings.FieldsFunc(strings.Trim(raw, "[]{}"), func(r rune) bool { return r == ',' || r == ';' }) {
		parts := strings.SplitN(strings.TrimSpace(item), "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.ToLower(strings.Trim(strings.TrimSpace(parts[0]), "\\\"'"))
		value := strings.Trim(strings.TrimSpace(parts[1]), "\\\"'")
		switch key {
		case "region", "zone", "az", "rack", "rack_id":
			if value != "" {
				labels[key] = value
			}
		}
	}
}

func (c *Connector) DiscoverTableTopology(ctx context.Context, schema, table string) ([]domain.TopologyPlacement, error) {
	p, err := c.get(ctx)
	if err != nil {
		return nil, err
	}
	switch c.ds.Type {
	case domain.DataSourcePolarDBX:
		ref := quoteIdent(table)
		if strings.TrimSpace(schema) != "" && !strings.EqualFold(schema, c.ds.Database) {
			ref = quoteIdent(schema) + "." + quoteIdent(table)
		}
		r, err := p.query(ctx, "SHOW TOPOLOGY FROM "+ref)
		if err != nil {
			return nil, err
		}
		groupIdx := resultColumnIndex(r, "GROUP_NAME")
		partIdx := resultColumnIndex(r, "PARTITION_NAME")
		groups := map[string]int64{}
		partitionPlacements := []domain.TopologyPlacement{}
		for _, row := range r.rows {
			group := rowString(row, groupIdx)
			part := rowString(row, partIdx)
			if group == "" {
				continue
			}
			groups[group]++
			if part != "" {
				partitionPlacements = append(partitionPlacements, domain.TopologyPlacement{ID: "polardbx-partition:" + part + ":" + group, Kind: "POLARDBX_PARTITION_GROUP", Labels: map[string]string{"polardbx_group": group, "partition_name": part}, Weight: 1})
			}
		}
		if len(partitionPlacements) > 0 {
			return partitionPlacements, nil
		}
		names := make([]string, 0, len(groups))
		for group := range groups {
			names = append(names, group)
		}
		sort.Strings(names)
		out := make([]domain.TopologyPlacement, 0, len(names))
		for _, group := range names {
			out = append(out, domain.TopologyPlacement{ID: "polardbx-group:" + group, Kind: "POLARDBX_GROUP", Labels: map[string]string{"polardbx_group": group}, Weight: groups[group]})
		}
		return out, nil
	case domain.DataSourceTiDB:
		q := "SELECT r.REGION_ID,p.STORE_ID,s.ADDRESS,COALESCE(CAST(s.LABEL AS CHAR),'') AS STORE_LABELS,COALESCE(r.APPROXIMATE_KEYS,0) " +
			"FROM information_schema.TIKV_REGION_STATUS r JOIN information_schema.TIKV_REGION_PEERS p ON p.REGION_ID=r.REGION_ID AND p.IS_LEADER=1 " +
			"JOIN information_schema.TIKV_STORE_STATUS s ON s.STORE_ID=p.STORE_ID " +
			"WHERE r.DB_NAME=" + quoteSQLString(schema) + " AND r.TABLE_NAME=" + quoteSQLString(table) + " AND COALESCE(r.IS_INDEX,0)=0 ORDER BY r.REGION_ID"
		r, err := p.query(ctx, q)
		if err != nil {
			return nil, err
		}
		type storeAgg struct {
			address, rawLabels string
			weight             int64
		}
		stores := map[string]storeAgg{}
		for _, row := range r.rows {
			if len(row) < 3 {
				continue
			}
			storeID := string(row[1])
			a := stores[storeID]
			a.address = string(row[2])
			if len(row) > 3 && len(row[3]) > 0 {
				a.rawLabels = string(row[3])
			}
			if len(row) > 4 {
				w, _ := strconv.ParseInt(string(row[4]), 10, 64)
				if w <= 0 {
					w = 1
				}
				a.weight += w
			} else {
				a.weight++
			}
			stores[storeID] = a
		}
		ids := make([]string, 0, len(stores))
		for id := range stores {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		out := make([]domain.TopologyPlacement, 0, len(ids))
		for _, id := range ids {
			a := stores[id]
			labels := map[string]string{"tidb_store_id": id, "tidb_store_address": a.address}
			if a.rawLabels != "" {
				labels["tidb_store_labels"] = a.rawLabels
				mergeTiDBStoreTopologyLabels(labels, a.rawLabels)
			}
			out = append(out, domain.TopologyPlacement{ID: "tidb-store:" + id, Kind: "TIDB_STORE", Labels: labels, Address: a.address, Weight: a.weight})
		}
		return out, nil
	case domain.DataSourceOceanBase:
		q := "SELECT COALESCE(PARTITION_NAME,''),COALESCE(SUBPARTITION_NAME,''),TABLET_ID,LS_ID,ZONE,SVR_IP,SVR_PORT,ROLE " +
			"FROM oceanbase.DBA_OB_TABLE_LOCATIONS WHERE DATABASE_NAME=" + quoteSQLString(schema) + " AND TABLE_NAME=" + quoteSQLString(table) + " AND TABLE_TYPE='USER TABLE' AND ROLE='LEADER' ORDER BY TABLET_ID"
		r, err := p.query(ctx, q)
		if err != nil {
			return nil, err
		}
		out := []domain.TopologyPlacement{}
		zones := map[string]int64{}
		for _, row := range r.rows {
			if len(row) < 5 {
				continue
			}
			part := string(row[0])
			if len(row) > 1 && string(row[1]) != "" {
				part = string(row[1])
			}
			zone := string(row[4])
			if zone == "" {
				continue
			}
			zones[zone]++
			labels := map[string]string{"ob_zone": zone}
			if part != "" {
				labels["partition_name"] = part
			}
			id := "ob-zone:" + zone
			kind := "OCEANBASE_ZONE"
			if part != "" {
				id = "ob-partition:" + part + ":" + zone
				kind = "OCEANBASE_PARTITION_ZONE"
			}
			out = append(out, domain.TopologyPlacement{ID: id, Kind: kind, Labels: labels, Weight: 1})
		}
		if len(out) > 0 {
			return out, nil
		}
		names := make([]string, 0, len(zones))
		for z := range zones {
			names = append(names, z)
		}
		sort.Strings(names)
		for _, z := range names {
			out = append(out, domain.TopologyPlacement{ID: "ob-zone:" + z, Kind: "OCEANBASE_ZONE", Labels: map[string]string{"ob_zone": z}, Weight: zones[z]})
		}
		return out, nil
	default:
		return nil, nil
	}
}

var _ connector.PartitionConnector = (*Connector)(nil)

// WriteTransactionalGBaseBatch applies GBase 8a CDC rows using only DML on the
// existing target table. Unlike the Full-load staging+MERGE path it never
// creates/drops a staging table, so wrapping this method in START
// TRANSACTION/COMMIT cannot be broken by DDL implicit-commit behavior.
func (c *Connector) WriteTransactionalGBaseBatch(ctx context.Context, req connector.WriteBatchRequest) (int64, error) {
	if c.ds.Type != domain.DataSourceGBase {
		return 0, errors.New("transactional GBase batch requires datasource type gbase")
	}
	if len(req.PrimaryKeys) == 0 {
		return 0, errors.New("GBase 8a transactional CDC requires a primary/migration key")
	}
	if err := c.validateGBaseMergeLayout(ctx, req.Schema, req.Table, req.PrimaryKeys); err != nil {
		return 0, err
	}
	p, err := c.get(ctx)
	if err != nil {
		return 0, err
	}
	cols := make([]domain.ColumnInfo, 0, len(req.Columns))
	for _, col := range req.Columns {
		if !strings.Contains(strings.ToUpper(col.Extra), "GENERATED") {
			cols = append(cols, col)
		}
	}
	if len(cols) == 0 {
		return 0, errors.New("no target columns")
	}
	byName := map[string]int{}
	for i, col := range cols {
		byName[strings.ToLower(col.Name)] = i
	}
	for _, key := range req.PrimaryKeys {
		if _, ok := byName[strings.ToLower(key)]; !ok {
			return 0, fmt.Errorf("GBase transactional key %s is not in target columns", key)
		}
	}
	qTarget := quoteIdent(req.Schema) + "." + quoteIdent(req.Table)
	for ri, row := range req.Rows {
		if len(row) != len(cols) {
			return 0, fmt.Errorf("row column count %d != %d", len(row), len(cols))
		}
		if err := validateMySQLValues(cols, row); err != nil {
			return 0, fmt.Errorf("target row %d: %w", ri, err)
		}
		where := make([]string, 0, len(req.PrimaryKeys))
		for _, key := range req.PrimaryKeys {
			idx := byName[strings.ToLower(key)]
			if row[idx].Null {
				return 0, fmt.Errorf("GBase transactional key %s cannot be NULL", key)
			}
			where = append(where, quoteIdent(cols[idx].Name)+"="+formatValue(cols[idx], row[idx]))
		}
		probe, err := p.query(ctx, "SELECT 1 FROM "+qTarget+" WHERE "+strings.Join(where, " AND ")+" LIMIT 1 FOR UPDATE")
		if err != nil {
			return 0, err
		}
		if len(probe.rows) > 0 {
			sets := make([]string, 0, len(cols)-len(req.PrimaryKeys))
			for i, col := range cols {
				isKey := false
				for _, key := range req.PrimaryKeys {
					if strings.EqualFold(key, col.Name) {
						isKey = true
						break
					}
				}
				if !isKey {
					sets = append(sets, quoteIdent(col.Name)+"="+formatValue(col, row[i]))
				}
			}
			if len(sets) > 0 {
				if _, err = p.exec(ctx, "UPDATE "+qTarget+" SET "+strings.Join(sets, ",")+" WHERE "+strings.Join(where, " AND ")); err != nil {
					return 0, err
				}
			}
		} else {
			idents := make([]string, len(cols))
			vals := make([]string, len(cols))
			for i, col := range cols {
				idents[i] = quoteIdent(col.Name)
				vals[i] = formatValue(col, row[i])
			}
			if _, err = p.exec(ctx, "INSERT INTO "+qTarget+" ("+strings.Join(idents, ",")+") VALUES ("+strings.Join(vals, ",")+")"); err != nil {
				return 0, err
			}
		}
	}
	return int64(len(req.Rows)), nil
}
