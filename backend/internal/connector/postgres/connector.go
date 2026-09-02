package postgresconnector

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Factory struct{}

func NewFactory() *Factory { return &Factory{} }

func (*Factory) Capabilities(t domain.DataSourceType) connector.Descriptor {
	if t == domain.DataSourceGaussDB && !gaussDBNativeEnabled() {
		return connector.Descriptor{Type: t, Protocol: "postgresql", Capabilities: []connector.Capability{connector.CapabilityProtocolProbe}, Native: true, Maturity: connector.MaturityProbeOnly, QualificationRequired: true, Note: "GaussDB PostgreSQL-wire probe only; set QMIGRATION_EXPERIMENTAL_GAUSSDB_NATIVE=1 after qualification"}
	}
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
		connector.CapabilitySequenceState,
		connector.CapabilitySequenceBinding,
		connector.CapabilityMigrationPrecheck,
	}
	// pgoutput/replication-slot semantics are guaranteed only for the PostgreSQL
	// products currently covered by QMigration's native CDC reader.  Protocol-
	// compatible derivatives can still use the same full-load SPI without being
	// incorrectly advertised as pgoutput-compatible.
	if t == domain.DataSourcePostgreSQL || t == domain.DataSourcePolarDBPostgreSQL ||
		(t == domain.DataSourceGaussDB && gaussDBCDCEnabled()) ||
		(t == domain.DataSourceOpenGauss && openGaussCDCEnabled()) ||
		(t == domain.DataSourceKingbase && kingbaseCDCEnabled()) {
		caps = append(caps,
			connector.CapabilityCDCPosition,
			connector.CapabilityCDCCheckpoint,
			connector.CapabilityCDCRead,
		)
	}
	maturity := connector.MaturityNative
	note := "QMigration native PostgreSQL protocol full data plane with pgoutput CDC where advertised"
	if t == domain.DataSourceOpenGauss {
		if openGaussCDCEnabled() {
			maturity = connector.MaturityExperimental
			note = "EXPERIMENTAL openGauss PostgreSQL-wire Full/target + product-native mppdb_decoding SQL logical CDC"
		} else {
			maturity = connector.MaturityNativeFullOnly
			note = "Native PostgreSQL-wire Full Load/target apply; set QMIGRATION_EXPERIMENTAL_OPENGAUSS_LOGICAL_CDC=1 only after qualification"
		}
	}
	if t == domain.DataSourceKingbase {
		if kingbaseCDCEnabled() {
			maturity = connector.MaturityExperimental
			note = "EXPERIMENTAL KingbaseES PostgreSQL-wire Full/target + Kingbase sys_* slot functions with kboutput streaming CDC"
		} else {
			maturity = connector.MaturityNativeFullOnly
			note = "Native PostgreSQL-wire Full Load/target apply; set QMIGRATION_EXPERIMENTAL_KINGBASE_LOGICAL_CDC=1 only after qualification"
		}
	}
	if t == domain.DataSourceGaussDB {
		maturity = connector.MaturityExperimental
		note = "EXPERIMENTAL GaussDB PostgreSQL-wire Full/target data plane; SQL logical-decoding CDC requires QMIGRATION_EXPERIMENTAL_GAUSSDB_LOGICAL_CDC=1"
	}
	qualificationRequired := t == domain.DataSourceGaussDB ||
		(t == domain.DataSourceOpenGauss && openGaussCDCEnabled()) ||
		(t == domain.DataSourceKingbase && kingbaseCDCEnabled())
	return connector.Descriptor{Type: t, Protocol: "postgresql", Capabilities: caps, Native: true, Maturity: maturity, QualificationRequired: qualificationRequired, Note: note}
}

func envOn(name string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(name))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}
func gaussDBNativeEnabled() bool { return envOn("QMIGRATION_EXPERIMENTAL_GAUSSDB_NATIVE") }
func gaussDBCDCEnabled() bool {
	return gaussDBNativeEnabled() && envOn("QMIGRATION_EXPERIMENTAL_GAUSSDB_LOGICAL_CDC")
}
func openGaussCDCEnabled() bool { return envOn("QMIGRATION_EXPERIMENTAL_OPENGAUSS_LOGICAL_CDC") }
func kingbaseCDCEnabled() bool  { return envOn("QMIGRATION_EXPERIMENTAL_KINGBASE_LOGICAL_CDC") }
func (f *Factory) New(d domain.DataSource) (connector.Connector, error) {
	if d.Host == "" || d.Port <= 0 {
		return nil, errors.New("invalid PostgreSQL endpoint")
	}
	return &Connector{ds: d}, nil
}

type Connector struct {
	ds     domain.DataSource
	mu     sync.Mutex
	client *pgClient
}

func (c *Connector) get(ctx context.Context) (*pgClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.client != nil {
		return c.client, nil
	}
	p, err := dialPG(ctx, c.ds)
	if err != nil {
		return nil, err
	}
	c.client = p
	return p, nil
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
func (c *Connector) TestConnection(ctx context.Context) error {
	p, e := c.get(ctx)
	if e != nil {
		return e
	}
	r, e := p.query(ctx, "SELECT 1")
	if e != nil {
		return e
	}
	if len(r.rows) != 1 {
		return errors.New("PostgreSQL SELECT 1 returned no row")
	}
	return nil
}
func (c *Connector) GetVersion(ctx context.Context) (string, error) {
	p, e := c.get(ctx)
	if e != nil {
		return "", e
	}
	r, e := p.query(ctx, "SHOW server_version")
	if e != nil {
		return "", e
	}
	if len(r.rows) > 0 && len(r.rows[0]) > 0 {
		return string(r.rows[0][0]), nil
	}
	return p.serverVersion, nil
}
func pgString(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
func pgIdent(s string) string  { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
func (c *Connector) ListSchemas(ctx context.Context) ([]domain.SchemaInfo, error) {
	p, e := c.get(ctx)
	if e != nil {
		return nil, e
	}
	r, e := p.query(ctx, "SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('pg_catalog','information_schema') AND schema_name NOT LIKE 'pg_toast%' ORDER BY schema_name")
	if e != nil {
		return nil, e
	}
	out := []domain.SchemaInfo{}
	for _, row := range r.rows {
		if len(row) > 0 {
			out = append(out, domain.SchemaInfo{Name: string(row[0])})
		}
	}
	return out, nil
}
func (c *Connector) ListTables(ctx context.Context, schema string) ([]domain.TableInfo, error) {
	p, e := c.get(ctx)
	if e != nil {
		return nil, e
	}
	q := "SELECT schemaname,relname,COALESCE(n_live_tup,0),pg_total_relation_size(quote_ident(schemaname)||'.'||quote_ident(relname)) FROM pg_stat_user_tables WHERE schemaname=" + pgString(schema) + " ORDER BY relname"
	r, e := p.query(ctx, q)
	if e != nil {
		return nil, e
	}
	out := []domain.TableInfo{}
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
	p, e := c.get(ctx)
	if e != nil {
		return nil, e
	}
	m := &domain.TableMetadata{Schema: schema, Name: table}
	stat := "SELECT COALESCE(c.reltuples,0)::bigint,pg_total_relation_size(c.oid) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=" + pgString(schema) + " AND c.relname=" + pgString(table) + " AND c.relkind IN ('r','p')"
	if r, err := p.query(ctx, stat); err == nil && len(r.rows) > 0 && len(r.rows[0]) >= 2 {
		m.EstimatedRows, _ = strconv.ParseInt(string(r.rows[0][0]), 10, 64)
		m.DataLength, _ = strconv.ParseInt(string(r.rows[0][1]), 10, 64)
	} else if err == nil {
		return m, nil
	}
	q := `SELECT a.attname,format_type(a.atttypid,a.atttypmod),CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END,a.attnum,CASE WHEN pk.attnum IS NULL THEN '' ELSE 'PRI' END
FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace
LEFT JOIN (SELECT i.indrelid,unnest(i.indkey) AS attnum FROM pg_index i WHERE i.indisprimary) pk ON pk.indrelid=a.attrelid AND pk.attnum=a.attnum
WHERE n.nspname=` + pgString(schema) + ` AND c.relname=` + pgString(table) + ` AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum`
	r, e := p.query(ctx, q)
	if e != nil {
		return nil, e
	}
	pks := []domain.ColumnInfo{}
	for _, row := range r.rows {
		if len(row) < 5 {
			continue
		}
		ord, _ := strconv.Atoi(string(row[3]))
		ct := strings.ToLower(string(row[1]))
		dt := strings.Fields(ct)
		base := ""
		if len(dt) > 0 {
			base = dt[0]
		}
		col := domain.ColumnInfo{Name: string(row[0]), DataType: base, ColumnType: ct, Nullable: string(row[2]) == "YES", Ordinal: ord, PrimaryKey: string(row[4]) == "PRI"}
		m.Columns = append(m.Columns, col)
		if col.PrimaryKey {
			pks = append(pks, col)
		}
	}
	pkOrder := `SELECT a.attname FROM pg_index i JOIN pg_class c ON c.oid=i.indrelid JOIN pg_namespace n ON n.oid=c.relnamespace CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum,ord) JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=k.attnum WHERE i.indisprimary AND n.nspname=` + pgString(schema) + ` AND c.relname=` + pgString(table) + ` ORDER BY k.ord`
	if pkRows, err := p.query(ctx, pkOrder); err == nil {
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
	idxQ := `SELECT ic.relname,i.indisunique,i.indisprimary,a.attname,k.ord FROM pg_index i JOIN pg_class c ON c.oid=i.indrelid JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_class ic ON ic.oid=i.indexrelid CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum,ord) LEFT JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=k.attnum WHERE n.nspname=` + pgString(schema) + ` AND c.relname=` + pgString(table) + ` AND k.attnum>0 ORDER BY ic.relname,k.ord`
	if idxRows, err := p.query(ctx, idxQ); err == nil {
		byName := map[string]*domain.IndexInfo{}
		order := []string{}
		for _, row := range idxRows.rows {
			if len(row) < 5 {
				continue
			}
			name := string(row[0])
			idx := byName[name]
			if idx == nil {
				idx = &domain.IndexInfo{Name: name, Unique: string(row[1]) == "t" || strings.EqualFold(string(row[1]), "true"), Primary: string(row[2]) == "t" || strings.EqualFold(string(row[2]), "true")}
				byName[name] = idx
				order = append(order, name)
			}
			idx.Columns = append(idx.Columns, string(row[3]))
		}
		for _, name := range order {
			m.Indexes = append(m.Indexes, *byName[name])
		}
	}
	fkQ := `SELECT con.conname,a.attname,fn.nspname,fc.relname,fa.attname,ck.ord FROM pg_constraint con JOIN pg_class c ON c.oid=con.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_class fc ON fc.oid=con.confrelid JOIN pg_namespace fn ON fn.oid=fc.relnamespace CROSS JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS ck(attnum,ord) JOIN LATERAL unnest(con.confkey) WITH ORDINALITY AS fk(attnum,ord) ON fk.ord=ck.ord JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=ck.attnum JOIN pg_attribute fa ON fa.attrelid=fc.oid AND fa.attnum=fk.attnum WHERE con.contype='f' AND n.nspname=` + pgString(schema) + ` AND c.relname=` + pgString(table) + ` ORDER BY con.conname,ck.ord`
	if fkRows, err := p.query(ctx, fkQ); err == nil {
		byName := map[string]*domain.ForeignKeyInfo{}
		order := []string{}
		for _, row := range fkRows.rows {
			if len(row) < 5 {
				continue
			}
			name := string(row[0])
			rel := byName[name]
			if rel == nil {
				rel = &domain.ForeignKeyInfo{Name: name, ReferencedSchema: string(row[2]), ReferencedTable: string(row[3])}
				byName[name] = rel
				order = append(order, name)
			}
			rel.Columns = append(rel.Columns, string(row[1]))
			rel.ReferencedColumns = append(rel.ReferencedColumns, string(row[4]))
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
		m.PrimaryKeyNumeric = pk.DataType == "smallint" || pk.DataType == "integer" || pk.DataType == "bigint" || pk.DataType == "int2" || pk.DataType == "int4" || pk.DataType == "int8"
		if m.PrimaryKeyNumeric {
			rr, e := p.query(ctx, "SELECT MIN("+pgIdent(pk.Name)+"),MAX("+pgIdent(pk.Name)+") FROM "+pgIdent(schema)+"."+pgIdent(table))
			if e != nil {
				return nil, e
			}
			if len(rr.rows) > 0 && len(rr.rows[0]) >= 2 && !rr.nulls[0][0] && !rr.nulls[0][1] {
				m.HasRows = true
				m.MinPK, e = strconv.ParseInt(string(rr.rows[0][0]), 10, 64)
				if e != nil {
					return nil, e
				}
				m.MaxPK, e = strconv.ParseInt(string(rr.rows[0][1]), 10, 64)
				if e != nil {
					return nil, e
				}
			}
		}
	}
	return m, nil
}
func validatePostgresValue(col domain.ColumnInfo, v connector.Value) error {
	if v.Null {
		return nil
	}
	switch strings.ToLower(col.DataType) {
	case "smallint", "integer", "int", "bigint", "int2", "int4", "int8", "numeric", "decimal":
		return connector.ValidateNumericLiteral(v.Raw, false)
	case "real", "double", "double precision", "float4", "float8":
		return connector.ValidateNumericLiteral(v.Raw, true)
	default:
		return nil
	}
}

func validatePostgresValues(cols []domain.ColumnInfo, values []connector.Value) error {
	if len(cols) != len(values) {
		return errors.New("column/value count mismatch")
	}
	for i := range cols {
		if err := validatePostgresValue(cols[i], values[i]); err != nil {
			return fmt.Errorf("column %s: %w", cols[i].Name, err)
		}
	}
	return nil
}

func postgresValueLiteral(v connector.Value, col domain.ColumnInfo) string {
	if v.Null {
		return "NULL"
	}
	t := strings.ToLower(col.DataType)
	switch t {
	case "smallint", "integer", "int", "bigint", "numeric", "decimal", "real", "double precision", "float4", "float8":
		return string(v.Raw)
	case "bytea":
		return "decode('" + hex.EncodeToString(v.Raw) + "','hex')"
	default:
		return "'" + strings.ReplaceAll(string(v.Raw), "'", "''") + "'"
	}
}

func postgresKeyColumns(keys []string, columns []domain.ColumnInfo) ([]domain.ColumnInfo, error) {
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

func (c *Connector) PlanKeysetBoundaries(ctx context.Context, req connector.KeysetBoundaryRequest) ([][]connector.Value, error) {
	if req.Partitions <= 1 {
		return nil, nil
	}
	if len(req.Keys) == 0 {
		return nil, errors.New("keyset boundary planning requires migration key columns")
	}
	keyCols, err := postgresKeyColumns(req.Keys, req.Columns)
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
			if err := validatePostgresValues(keyCols, bound); err != nil {
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
		quoted[i] = pgIdent(key)
	}
	keyList := strings.Join(quoted, ",")
	orderBy := strings.Join(quoted, ",")
	tupleLiteral := func(values []connector.Value) string {
		right := make([]string, len(values))
		for i := range values {
			right[i] = postgresValueLiteral(values[i], keyCols[i])
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
	q := "WITH qm_ranked AS (SELECT " + keyList + ", NTILE(" + strconv.Itoa(req.Partitions) + ") OVER (ORDER BY " + orderBy + ") AS qm_bucket FROM " + pgIdent(req.Schema) + "." + pgIdent(req.Table) + where + "), " +
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
			value := append([]byte(nil), raw...)
			if strings.EqualFold(keyCols[i].DataType, "bytea") && len(value) >= 2 && string(value[:2]) == `\x` {
				decoded, decodeErr := hex.DecodeString(string(value[2:]))
				if decodeErr != nil {
					return nil, fmt.Errorf("decode bytea boundary for %s: %w", req.Keys[i], decodeErr)
				}
				value = decoded
			}
			bound[i] = connector.Value{Raw: value}
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
	q := `SELECT cn.nspname, cc.relname
FROM pg_inherits i
JOIN pg_class cp ON cp.oid=i.inhparent
JOIN pg_namespace pn ON pn.oid=cp.relnamespace
JOIN pg_class cc ON cc.oid=i.inhrelid
JOIN pg_namespace cn ON cn.oid=cc.relnamespace
WHERE pn.nspname=` + pgString(schema) + ` AND cp.relname=` + pgString(table) + ` ORDER BY cn.nspname,cc.relname`
	r, err := p.query(ctx, q)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(r.rows))
	for i, row := range r.rows {
		if len(row) >= 2 && !r.nulls[i][0] && !r.nulls[i][1] {
			out = append(out, string(row[0])+"."+string(row[1]))
		}
	}
	return out, nil
}

func (c *Connector) ReadBatch(ctx context.Context, req connector.ReadBatchRequest) (*connector.RowBatch, error) {
	p, e := c.get(ctx)
	if e != nil {
		return nil, e
	}
	if req.Limit <= 0 {
		req.Limit = 500
	}
	cols := []string{}
	indexByName := map[string]int{}
	for _, col := range req.Columns {
		indexByName[col.Name] = len(cols)
		cols = append(cols, pgIdent(col.Name))
	}
	if len(cols) == 0 {
		return nil, errors.New("no selected columns")
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
			left = append(left, pgIdent(key))
			orderKeys = append(orderKeys, pgIdent(key))
			keyCols = append(keyCols, req.Columns[idx])
		}
		tuple := func(values []connector.Value) string {
			right := make([]string, len(values))
			for i := range values {
				right[i] = postgresValueLiteral(values[i], keyCols[i])
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
		if _, ok := indexByName[req.PrimaryKey]; !ok {
			return nil, errors.New("primary key not selected")
		}
		where = " WHERE " + pgIdent(req.PrimaryKey) + ">=" + strconv.FormatInt(req.StartPK, 10) + " AND " + pgIdent(req.PrimaryKey) + "<=" + strconv.FormatInt(req.EndPK, 10)
		if req.HasAfter {
			where += " AND " + pgIdent(req.PrimaryKey) + ">" + strconv.FormatInt(req.AfterPK, 10)
		}
		orderKeys = []string{pgIdent(req.PrimaryKey)}
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
			parts[i] = "COALESCE(" + pgIdent(key) + "::text,'<NULL>')"
		}
		// md5 is available in core PostgreSQL and yields a stable unsigned 32-bit prefix.
		hashExpr := "((('x'||substr(md5(concat_ws('#'," + strings.Join(parts, ",") + ")),1,8))::bit(32)::bigint) % " + strconv.Itoa(req.HashBuckets) + ")=" + strconv.Itoa(req.HashBucket)
		conditions = append(conditions, hashExpr)
	}
	if strings.TrimSpace(req.CustomWhere) != "" {
		conditions = append(conditions, "("+strings.TrimSpace(req.CustomWhere)+")")
	}
	where = ""
	if len(conditions) > 0 {
		where = " WHERE " + strings.Join(conditions, " AND ")
	}
	from := pgIdent(req.Schema) + "." + pgIdent(req.Table)
	if strings.TrimSpace(req.Partition) != "" {
		part := strings.TrimSpace(req.Partition)
		ps, pt := req.Schema, part
		if dot := strings.Index(part, "."); dot > 0 {
			ps, pt = part[:dot], part[dot+1:]
		}
		from = pgIdent(ps) + "." + pgIdent(pt)
	}
	q := "SELECT " + strings.Join(cols, ",") + " FROM " + from + where + " ORDER BY " + strings.Join(orderKeys, ",") + " LIMIT " + strconv.Itoa(req.Limit)
	r, e := p.query(ctx, q)
	if e != nil {
		return nil, e
	}
	b := &connector.RowBatch{Rows: make([][]connector.Value, 0, len(r.rows))}
	for ri, row := range r.rows {
		vals := make([]connector.Value, len(row))
		for i, v := range row {
			raw := v
			if i < len(req.Columns) && strings.ToLower(req.Columns[i].DataType) == "bytea" && len(v) >= 2 && string(v[:2]) == `\x` {
				if dec, de := hex.DecodeString(string(v[2:])); de == nil {
					raw = dec
				}
			}
			vals[i] = connector.Value{Null: r.nulls[ri][i], Raw: raw}
			if !r.nulls[ri][i] {
				b.Bytes += int64(len(raw))
			}
		}
		b.Rows = append(b.Rows, vals)
		if req.UseKeyset {
			keys := req.PrimaryKeys
			if len(keys) == 0 {
				keys = []string{req.PrimaryKey}
			}
			b.LastKey = b.LastKey[:0]
			for _, key := range keys {
				v := vals[indexByName[key]]
				v.Raw = append([]byte(nil), v.Raw...)
				b.LastKey = append(b.LastKey, v)
			}
		} else if !r.nulls[ri][indexByName[req.PrimaryKey]] {
			b.LastPK, e = strconv.ParseInt(string(row[indexByName[req.PrimaryKey]]), 10, 64)
			if e != nil {
				return nil, e
			}
		}
	}
	return b, nil
}

func pgValue(col domain.ColumnInfo, v connector.Value) string {
	if v.Null {
		return "NULL"
	}
	dt := strings.ToLower(col.DataType)
	raw := string(v.Raw)
	switch dt {
	case "smallint", "integer", "bigint", "int2", "int4", "int8", "numeric", "decimal", "real", "double", "double precision", "float4", "float8":
		return raw
	case "bytea", "binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob":
		return "decode('" + fmt.Sprintf("%x", v.Raw) + "','hex')"
	case "boolean", "bool":
		if raw == "1" || strings.EqualFold(raw, "true") || raw == "t" {
			return "TRUE"
		}
		return "FALSE"
	default:
		return pgString(raw)
	}
}
func (c *Connector) ReadByKey(ctx context.Context, req connector.ReadByKeyRequest) ([]connector.Value, bool, error) {
	if len(req.PrimaryKeys) == 0 || len(req.PrimaryKeys) != len(req.KeyValues) || len(req.PrimaryKeys) != len(req.KeyColumns) {
		return nil, false, errors.New("invalid point lookup primary-key values")
	}
	if len(req.Columns) == 0 {
		return nil, false, errors.New("point lookup requires selected columns")
	}
	if err := validatePostgresValues(req.KeyColumns, req.KeyValues); err != nil {
		return nil, false, fmt.Errorf("point lookup key: %w", err)
	}
	p, err := c.get(ctx)
	if err != nil {
		return nil, false, err
	}
	cols := make([]string, len(req.Columns))
	for i, col := range req.Columns {
		cols[i] = pgIdent(col.Name)
	}
	where := make([]string, len(req.PrimaryKeys))
	for i, key := range req.PrimaryKeys {
		if req.KeyValues[i].Null {
			return nil, false, fmt.Errorf("point lookup key %s cannot be null", key)
		}
		where[i] = pgIdent(key) + "=" + pgValue(req.KeyColumns[i], req.KeyValues[i])
	}
	r, err := p.query(ctx, "SELECT "+strings.Join(cols, ",")+" FROM "+pgIdent(req.Schema)+"."+pgIdent(req.Table)+" WHERE "+strings.Join(where, " AND ")+" LIMIT 1 FOR UPDATE")
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
	p, e := c.get(ctx)
	if e != nil {
		return 0, e
	}
	ids := make([]string, len(req.Columns))
	for i, col := range req.Columns {
		ids[i] = pgIdent(col.Name)
	}
	var b strings.Builder
	b.WriteString("INSERT INTO " + pgIdent(req.Schema) + "." + pgIdent(req.Table) + " (" + strings.Join(ids, ",") + ") VALUES ")
	for ri, row := range req.Rows {
		if len(row) != len(req.Columns) {
			return 0, errors.New("row/column mismatch")
		}
		if err := validatePostgresValues(req.Columns, row); err != nil {
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
			b.WriteString(pgValue(req.Columns[ci], v))
		}
		b.WriteByte(')')
	}
	primaryKeys := append([]string(nil), req.PrimaryKeys...)
	if len(primaryKeys) == 0 && req.PrimaryKey != "" {
		primaryKeys = []string{req.PrimaryKey}
	}
	if len(primaryKeys) > 0 {
		pkSet := map[string]bool{}
		pkIdents := make([]string, len(primaryKeys))
		for i, key := range primaryKeys {
			pkSet[strings.ToLower(key)] = true
			pkIdents[i] = pgIdent(key)
		}
		updates := []string{}
		for _, col := range req.Columns {
			if pkSet[strings.ToLower(col.Name)] {
				continue
			}
			q := pgIdent(col.Name)
			updates = append(updates, q+"=EXCLUDED."+q)
		}
		if len(updates) > 0 {
			b.WriteString(" ON CONFLICT (" + strings.Join(pkIdents, ",") + ") DO UPDATE SET " + strings.Join(updates, ","))
		} else {
			b.WriteString(" ON CONFLICT (" + strings.Join(pkIdents, ",") + ") DO NOTHING")
		}
	}
	if e = p.exec(ctx, b.String()); e != nil {
		return 0, e
	}
	return int64(len(req.Rows)), nil
}
func (c *Connector) EnsureSchema(ctx context.Context, schema string) error {
	p, e := c.get(ctx)
	if e != nil {
		return e
	}
	return p.exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+pgIdent(schema))
}
func pgTargetType(col domain.ColumnInfo) string {
	ct := strings.ToLower(col.ColumnType)
	dt := strings.ToLower(col.DataType)
	if strings.Contains(ct, "unsigned") {
		ct = strings.ReplaceAll(ct, " unsigned", "")
	}
	if strings.HasPrefix(ct, "varchar") || strings.HasPrefix(ct, "char") || strings.HasPrefix(ct, "decimal") || strings.HasPrefix(ct, "numeric") || strings.HasPrefix(ct, "timestamp") || strings.HasPrefix(ct, "time") || strings.HasPrefix(ct, "bit") {
		if strings.HasPrefix(ct, "bit") {
			return "bytea"
		}
		return ct
	}
	switch dt {
	case "tinyint", "smallint":
		return "smallint"
	case "mediumint", "int", "integer":
		return "integer"
	case "bigint":
		return "bigint"
	case "float":
		return "real"
	case "double", "double precision":
		return "double precision"
	case "datetime":
		return "timestamp"
	case "date":
		return "date"
	case "text", "tinytext", "mediumtext", "longtext":
		return "text"
	case "json", "jsonb":
		return "jsonb"
	case "binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob", "bytea":
		return "bytea"
	case "boolean", "bool":
		return "boolean"
	default:
		if ct != "" && (!strings.Contains(ct, "enum") && !strings.Contains(ct, "set")) {
			return ct
		}
		return "text"
	}
}
func (c *Connector) CreateTable(ctx context.Context, schema, table string, cols []domain.ColumnInfo, pk string) error {
	keys := []string{}
	if pk != "" {
		keys = append(keys, pk)
	}
	return c.CreateTableWithPrimaryKeys(ctx, schema, table, cols, keys)
}
func (c *Connector) CreateTableWithPrimaryKeys(ctx context.Context, schema, table string, cols []domain.ColumnInfo, primaryKeys []string) error {
	p, e := c.get(ctx)
	if e != nil {
		return e
	}
	defs := []string{}
	for _, col := range cols {
		d := pgIdent(col.Name) + " " + pgTargetType(col)
		if !col.Nullable {
			d += " NOT NULL"
		}
		defs = append(defs, d)
	}
	if len(primaryKeys) > 0 {
		quoted := make([]string, len(primaryKeys))
		for i, key := range primaryKeys {
			quoted[i] = pgIdent(key)
		}
		defs = append(defs, "PRIMARY KEY ("+strings.Join(quoted, ",")+")")
	}
	return p.exec(ctx, "CREATE TABLE IF NOT EXISTS "+pgIdent(schema)+"."+pgIdent(table)+" ("+strings.Join(defs, ",")+")")
}

var _ connector.DataConnector = (*Connector)(nil)

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
	if len(name) > 63 {
		name = name[:63]
	}
	cols := make([]string, len(idx.Columns))
	for i, col := range idx.Columns {
		cols[i] = pgIdent(col)
	}
	prefix := "CREATE INDEX IF NOT EXISTS "
	if idx.Unique {
		prefix = "CREATE UNIQUE INDEX IF NOT EXISTS "
	}
	return p.exec(ctx, prefix+pgIdent(name)+" ON "+pgIdent(schema)+"."+pgIdent(table)+" ("+strings.Join(cols, ",")+")")
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
	if len(name) > 63 {
		name = name[:63]
	}
	cols := make([]string, len(fk.Columns))
	refs := make([]string, len(fk.ReferencedColumns))
	for i := range fk.Columns {
		cols[i] = pgIdent(fk.Columns[i])
		refs[i] = pgIdent(fk.ReferencedColumns[i])
	}
	refSchema := fk.ReferencedSchema
	if refSchema == "" {
		refSchema = schema
	}
	q := "ALTER TABLE " + pgIdent(schema) + "." + pgIdent(table) + " ADD CONSTRAINT " + pgIdent(name) + " FOREIGN KEY (" + strings.Join(cols, ",") + ") REFERENCES " + pgIdent(refSchema) + "." + pgIdent(fk.ReferencedTable) + " (" + strings.Join(refs, ",") + ")"
	return p.exec(ctx, q)
}

var _ connector.SchemaConnector = (*Connector)(nil)
var _ connector.CompositeSchemaConnector = (*Connector)(nil)
var _ connector.PostLoadSchemaConnector = (*Connector)(nil)

// RawRows is used by the optional PostgreSQL control-plane repository. It is
// intentionally small and keeps SQL execution out of the public migration API.
type RawRows struct {
	Columns []string
	Rows    [][][]byte
	Nulls   [][]bool
}

func (c *Connector) ExecSQL(ctx context.Context, sql string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.client == nil {
		p, err := dialPG(ctx, c.ds)
		if err != nil {
			return err
		}
		c.client = p
	}
	return c.client.exec(ctx, sql)
}
func (c *Connector) QuerySQL(ctx context.Context, sql string) (*RawRows, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.client == nil {
		p, err := dialPG(ctx, c.ds)
		if err != nil {
			return nil, err
		}
		c.client = p
	}
	r, err := c.client.query(ctx, sql)
	if err != nil {
		return nil, err
	}
	return &RawRows{Columns: r.columns, Rows: r.rows, Nulls: r.nulls}, nil
}

func (c *Connector) CurrentCDCPosition(ctx context.Context) (*domain.CDCPosition, error) {
	capturedAt := time.Now().UnixMilli()
	p, e := c.get(ctx)
	if e != nil {
		return nil, e
	}
	query := "SELECT pg_current_wal_lsn()::text"
	positionType := "LSN"
	switch c.ds.Type {
	case domain.DataSourceGaussDB:
		if !gaussDBCDCEnabled() {
			return nil, errors.New("GaussDB source CDC requires QMIGRATION_EXPERIMENTAL_GAUSSDB_NATIVE=1 and QMIGRATION_EXPERIMENTAL_GAUSSDB_LOGICAL_CDC=1")
		}
		query = "SELECT pg_current_xlog_location()::text"
		positionType = "GAUSSDB_LSN"
	case domain.DataSourceOpenGauss:
		if !openGaussCDCEnabled() {
			return nil, errors.New("openGauss source CDC requires QMIGRATION_EXPERIMENTAL_OPENGAUSS_LOGICAL_CDC=1")
		}
		query = "SELECT pg_current_xlog_location()::text"
		positionType = "OPENGAUSS_LSN"
	case domain.DataSourceKingbase:
		if !kingbaseCDCEnabled() {
			return nil, errors.New("KingbaseES source CDC requires QMIGRATION_EXPERIMENTAL_KINGBASE_LOGICAL_CDC=1")
		}
		query = "SELECT sys_current_wal_lsn()::text"
		positionType = "KINGBASE_LSN"
	}
	r, e := p.query(ctx, query)
	if e != nil {
		return nil, e
	}
	if len(r.rows) == 0 || len(r.rows[0]) == 0 {
		return nil, errors.New("current log position returned no row")
	}
	return &domain.CDCPosition{DatabaseType: string(c.ds.Type), PositionType: positionType, PositionValue: strings.ReplaceAll(string(r.rows[0][0]), " ", ""), SourceTimestampMS: capturedAt}, nil
}

func (c *Connector) MigrationPrechecks(ctx context.Context, needCDC bool) []domain.PrecheckItem {
	items := []domain.PrecheckItem{}
	prefix := "postgres"
	switch c.ds.Type {
	case domain.DataSourceGaussDB:
		prefix = "gaussdb"
	case domain.DataSourceOpenGauss:
		prefix = "opengauss"
	case domain.DataSourceKingbase:
		prefix = "kingbase"
	}
	p, err := c.get(ctx)
	if err != nil {
		return []domain.PrecheckItem{{Name: prefix + "_connection", Level: domain.PrecheckFailed, Message: err.Error()}}
	}
	if r, err := p.query(ctx, "SELECT current_setting('server_encoding'),current_setting('TimeZone')"); err == nil && len(r.rows) > 0 && len(r.rows[0]) >= 2 {
		items = append(items, domain.PrecheckItem{Name: prefix + "_encoding_timezone", Level: domain.PrecheckPass, Message: fmt.Sprintf("encoding=%s timezone=%s", r.rows[0][0], r.rows[0][1])})
	}
	if !needCDC {
		return items
	}
	if c.ds.Type == domain.DataSourceGaussDB && !gaussDBCDCEnabled() {
		items = append(items, domain.PrecheckItem{Name: "gaussdb_logical_cdc_gate", Level: domain.PrecheckFailed, Message: "set QMIGRATION_EXPERIMENTAL_GAUSSDB_NATIVE=1 and QMIGRATION_EXPERIMENTAL_GAUSSDB_LOGICAL_CDC=1 only after qualification"})
		return items
	}
	if c.ds.Type == domain.DataSourceOpenGauss && !openGaussCDCEnabled() {
		items = append(items, domain.PrecheckItem{Name: "opengauss_logical_cdc_gate", Level: domain.PrecheckFailed, Message: "set QMIGRATION_EXPERIMENTAL_OPENGAUSS_LOGICAL_CDC=1 only after qualification"})
		return items
	}
	if c.ds.Type == domain.DataSourceKingbase && !kingbaseCDCEnabled() {
		items = append(items, domain.PrecheckItem{Name: "kingbase_logical_cdc_gate", Level: domain.PrecheckFailed, Message: "set QMIGRATION_EXPERIMENTAL_KINGBASE_LOGICAL_CDC=1 only after qualification"})
		return items
	}
	setting := func(name string) (string, error) {
		r, err := p.query(ctx, "SELECT current_setting("+pgString(name)+")")
		if err != nil || len(r.rows) == 0 || len(r.rows[0]) == 0 {
			if err == nil {
				err = fmt.Errorf("setting %s returned no row", name)
			}
			return "", err
		}
		return string(r.rows[0][0]), nil
	}
	if v, err := setting("wal_level"); err != nil {
		items = append(items, domain.PrecheckItem{Name: prefix + "_wal_level", Level: domain.PrecheckFailed, Message: err.Error()})
	} else if strings.EqualFold(v, "logical") {
		items = append(items, domain.PrecheckItem{Name: prefix + "_wal_level", Level: domain.PrecheckPass, Message: "logical"})
	} else {
		items = append(items, domain.PrecheckItem{Name: prefix + "_wal_level", Level: domain.PrecheckFailed, Message: "wal_level must be logical, current=" + v})
	}
	for _, cfg := range []struct{ name, label string }{{"max_replication_slots", prefix + "_replication_slots"}, {"max_wal_senders", prefix + "_wal_senders"}} {
		if v, err := setting(cfg.name); err == nil {
			n, _ := strconv.Atoi(v)
			level := domain.PrecheckPass
			msg := v
			if n <= 0 {
				level = domain.PrecheckFailed
				msg += "; must be > 0 for logical replication"
			}
			items = append(items, domain.PrecheckItem{Name: cfg.label, Level: level, Message: msg})
		} else {
			items = append(items, domain.PrecheckItem{Name: cfg.label, Level: domain.PrecheckFailed, Message: err.Error()})
		}
	}
	if c.ds.Type == domain.DataSourceGaussDB {
		// GaussDB permits logical replication through SYSADMIN/REPLICATION or
		// membership in gs_role_replication.  Keep this precheck fail-closed:
		// an inability to prove the privilege must not be treated as success.
		q := `SELECT CASE WHEN r.rolsuper OR r.rolreplication OR EXISTS (` +
			`SELECT 1 FROM pg_roles gr WHERE gr.rolname='gs_role_replication' ` +
			`AND pg_has_role(current_user,gr.oid,'member')) THEN 't' ELSE 'f' END ` +
			`FROM pg_roles r WHERE r.rolname=current_user`
		if r, err := p.query(ctx, q); err != nil {
			items = append(items, domain.PrecheckItem{Name: "gaussdb_replication_privilege", Level: domain.PrecheckFailed, Message: "cannot verify SYSADMIN/REPLICATION/gs_role_replication permission: " + err.Error()})
		} else if len(r.rows) == 0 || len(r.rows[0]) == 0 {
			items = append(items, domain.PrecheckItem{Name: "gaussdb_replication_privilege", Level: domain.PrecheckFailed, Message: "current GaussDB role was not found in pg_roles"})
		} else {
			ok := string(r.rows[0][0]) == "t" || strings.EqualFold(string(r.rows[0][0]), "true")
			level := domain.PrecheckPass
			msg := "role can use logical replication"
			if !ok {
				level = domain.PrecheckFailed
				msg = "current role requires SYSADMIN/REPLICATION or membership in gs_role_replication"
			}
			items = append(items, domain.PrecheckItem{Name: "gaussdb_replication_privilege", Level: level, Message: msg})
		}
	} else {
		rolesView := "pg_roles"
		label := prefix + "_replication_privilege"
		if c.ds.Type == domain.DataSourceKingbase {
			rolesView = "sys_roles"
		}
		if r, err := p.query(ctx, "SELECT rolreplication::text, rolsuper::text FROM "+rolesView+" WHERE rolname=current_user"); err != nil {
			items = append(items, domain.PrecheckItem{Name: label, Level: domain.PrecheckFailed, Message: "cannot verify replication privilege: " + err.Error()})
		} else if len(r.rows) == 0 || len(r.rows[0]) < 2 {
			items = append(items, domain.PrecheckItem{Name: label, Level: domain.PrecheckFailed, Message: "current role was not found in " + rolesView})
		} else {
			rep := string(r.rows[0][0]) == "t" || strings.EqualFold(string(r.rows[0][0]), "true")
			super := string(r.rows[0][1]) == "t" || strings.EqualFold(string(r.rows[0][1]), "true")
			if rep || super {
				items = append(items, domain.PrecheckItem{Name: label, Level: domain.PrecheckPass, Message: "role can use replication"})
			} else {
				items = append(items, domain.PrecheckItem{Name: label, Level: domain.PrecheckFailed, Message: "current role requires REPLICATION attribute or superuser-equivalent permission"})
			}
		}
	}
	if c.ds.Type == domain.DataSourceOpenGauss {
		if v, err := setting("ssl"); err != nil {
			items = append(items, domain.PrecheckItem{Name: "opengauss_ssl", Level: domain.PrecheckFailed, Message: err.Error()})
		} else if strings.EqualFold(v, "on") || strings.EqualFold(v, "true") {
			items = append(items, domain.PrecheckItem{Name: "opengauss_ssl", Level: domain.PrecheckPass, Message: "ssl=on"})
		} else {
			items = append(items, domain.PrecheckItem{Name: "opengauss_ssl", Level: domain.PrecheckFailed, Message: "openGauss logical decoding requires ssl=on on the source primary"})
		}
	}
	return items
}

var _ connector.MigrationPrecheckConnector = (*Connector)(nil)

var _ connector.CDCSource = (*Connector)(nil)

func pgLiteral(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

func (c *Connector) CreateCDCCheckpoint(ctx context.Context, name string) (*domain.CDCPosition, error) {
	name = strings.ToLower(name)
	if len(name) > 63 {
		name = name[:63]
	}
	if name == "" {
		return nil, errors.New("replication slot name is required")
	}
	capturedAt := time.Now().UnixMilli()
	plugin := "pgoutput"
	positionType := "LSN"
	createFn := "pg_create_logical_replication_slot"
	switch c.ds.Type {
	case domain.DataSourceGaussDB:
		if !gaussDBCDCEnabled() {
			return nil, errors.New("GaussDB logical CDC gate is not enabled")
		}
		plugin = "mppdb_decoding"
		positionType = "GAUSSDB_LSN"
	case domain.DataSourceOpenGauss:
		if !openGaussCDCEnabled() {
			return nil, errors.New("openGauss logical CDC gate is not enabled")
		}
		plugin = "mppdb_decoding"
		positionType = "OPENGAUSS_LSN"
	case domain.DataSourceKingbase:
		if !kingbaseCDCEnabled() {
			return nil, errors.New("KingbaseES logical CDC gate is not enabled")
		}
		createFn = "sys_create_logical_replication_slot"
		plugin = "kboutput"
		positionType = "KINGBASE_LSN"
	}
	query := "SELECT * FROM " + createFn + "(" + pgLiteral(name) + ", " + pgLiteral(plugin) + ")"
	if c.ds.Type == domain.DataSourceGaussDB {
		gaussQuery, qerr := gaussDBCreateSlotQuery(name)
		if qerr != nil {
			return nil, qerr
		}
		query = gaussQuery
	}
	r, err := c.QuerySQL(ctx, query)
	if err != nil {
		return nil, err
	}
	if len(r.Rows) == 0 || len(r.Rows[0]) < 2 {
		return nil, errors.New("pg_create_logical_replication_slot returned no row")
	}
	return &domain.CDCPosition{DatabaseType: string(c.ds.Type), PositionType: positionType, PositionValue: strings.ReplaceAll(string(r.Rows[0][1]), " ", ""), Resource: string(r.Rows[0][0]), SourceTimestampMS: capturedAt}, nil
}
func (c *Connector) DropCDCCheckpoint(ctx context.Context, resource string) error {
	if resource == "" {
		return nil
	}
	// pg_drop_replication_slot fails while the slot is active; callers invoke it
	// only after the managed CDC process has exited.
	dropFn := "pg_drop_replication_slot"
	if c.ds.Type == domain.DataSourceKingbase {
		dropFn = "sys_drop_replication_slot"
	}
	return c.ExecSQL(ctx, "SELECT "+dropFn+"("+pgLiteral(resource)+")")
}

var _ connector.CDCCheckpointSource = (*Connector)(nil)

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
		where[i] = pgIdent(keys[i]) + "=" + pgValue(cols[i], vals[i])
	}
	p, err := c.get(ctx)
	if err != nil {
		return err
	}
	return p.exec(ctx, "DELETE FROM "+pgIdent(req.Schema)+"."+pgIdent(req.Table)+" WHERE "+strings.Join(where, " AND "))
}

func (c *Connector) BeginCDCTransaction(ctx context.Context) error {
	p, err := c.get(ctx)
	if err != nil {
		return err
	}
	return p.exec(ctx, "BEGIN")
}
func (c *Connector) CommitCDCTransaction(ctx context.Context) error {
	p, err := c.get(ctx)
	if err != nil {
		return err
	}
	return p.exec(ctx, "COMMIT")
}
func (c *Connector) RollbackCDCTransaction(ctx context.Context) error {
	p, err := c.get(ctx)
	if err != nil {
		return err
	}
	return p.exec(ctx, "ROLLBACK")
}

var _ connector.CDCApplyConnector = (*Connector)(nil)
var _ connector.PointLookupConnector = (*Connector)(nil)
var _ connector.TransactionalCDCApplyConnector = (*Connector)(nil)

// ExecDDL executes a DDL statement in a selected PostgreSQL schema. General
// PostgreSQL DDL is not emitted by pgoutput, but this primitive is shared by
// external/native schema-event bridges and keeps target execution explicit.
func (c *Connector) ExecDDL(ctx context.Context, schema, ddl string) error {
	p, err := c.get(ctx)
	if err != nil {
		return err
	}
	if strings.TrimSpace(schema) != "" {
		if err := p.exec(ctx, "SET search_path TO "+pgIdent(schema)); err != nil {
			return err
		}
	}
	return p.exec(ctx, ddl)
}

var _ connector.DDLApplyConnector = (*Connector)(nil)

func (c *Connector) SampleRuntimeLoad(ctx context.Context) (domain.DatabaseRuntimeLoad, error) {
	p, err := c.get(ctx)
	if err != nil {
		return domain.DatabaseRuntimeLoad{}, err
	}
	r, err := p.query(ctx, "SELECT current_setting('max_connections'),count(*),count(*) FILTER (WHERE state='active') FROM pg_stat_activity")
	if err != nil {
		return domain.DatabaseRuntimeLoad{}, err
	}
	load := domain.DatabaseRuntimeLoad{}
	if len(r.rows) == 0 || len(r.rows[0]) < 3 {
		return load, nil
	}
	load.MaxConnections, _ = strconv.ParseInt(string(r.rows[0][0]), 10, 64)
	load.Connections, _ = strconv.ParseInt(string(r.rows[0][1]), 10, 64)
	load.RunningQueries, _ = strconv.ParseInt(string(r.rows[0][2]), 10, 64)
	if load.MaxConnections > 0 {
		load.ConnectionUsagePct = float64(load.Connections) * 100 / float64(load.MaxConnections)
	}
	return load, nil
}

var _ connector.PartitionConnector = (*Connector)(nil)
