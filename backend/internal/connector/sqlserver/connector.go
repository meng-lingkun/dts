package sqlserverconnector

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	tdsPrelogin = 0x12
	tdsEOM      = 0x01
)

type Factory struct{}

func NewFactory() *Factory { return &Factory{} }
func (*Factory) Capabilities(t domain.DataSourceType) connector.Descriptor {
	caps := []connector.Capability{connector.CapabilityProtocolProbe}
	note := "QMigration native TDS PRELOGIN/LOGIN7/SQL Batch protocol is implemented; full migration remains experimental until real SQL Server E2E qualification"
	if experimentalFullEnabled() {
		caps = append(caps,
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
		)
		note = "EXPERIMENTAL QMigration native TDS full data plane enabled by QMIGRATION_EXPERIMENTAL_SQLSERVER_NATIVE"
	}
	if sqlServerCDCEnabled() {
		caps = append(caps, connector.CapabilityCDCPosition, connector.CapabilityCDCRead, connector.CapabilityMigrationPrecheck)
		note += "; EXPERIMENTAL native SQL Server CDC/LSN reader enabled by QMIGRATION_EXPERIMENTAL_SQLSERVER_CDC"
	}
	return connector.Descriptor{Type: t, Protocol: "tds", Native: true, Capabilities: caps, Maturity: connector.MaturityExperimental, QualificationRequired: true, Note: note}
}
func (*Factory) New(ds domain.DataSource) (connector.Connector, error) {
	if ds.Host == "" || ds.Port <= 0 {
		return nil, errors.New("invalid SQL Server endpoint")
	}
	return &Connector{ds: ds}, nil
}

type Connector struct {
	ds           domain.DataSource
	mu           sync.Mutex
	client       *tdsClient
	probeVersion string
}

func (c *Connector) get(ctx context.Context) (*tdsClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.client != nil {
		return c.client, nil
	}
	p, err := dialTDS(ctx, c.ds)
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
	if !experimentalFullEnabled() {
		v, err := probeTDS(ctx, c.ds.Host, c.ds.Port)
		if err == nil {
			c.mu.Lock()
			c.probeVersion = v
			c.mu.Unlock()
		}
		return err
	}
	p, err := c.get(ctx)
	if err != nil {
		return err
	}
	rows, _, _, err := p.query(ctx, "SELECT CONVERT(nvarchar(4000),1)")
	if err != nil {
		return err
	}
	if len(rows) != 1 {
		return errors.New("SQL Server SELECT 1 returned no row")
	}
	return nil
}

func (c *Connector) GetVersion(ctx context.Context) (string, error) {
	if !experimentalFullEnabled() {
		c.mu.Lock()
		v := c.probeVersion
		c.mu.Unlock()
		if v != "" {
			return v, nil
		}
		return probeTDS(ctx, c.ds.Host, c.ds.Port)
	}
	p, err := c.get(ctx)
	if err != nil {
		return "", err
	}
	rows, _, _, qerr := p.query(ctx, "SELECT CONVERT(nvarchar(4000),SERVERPROPERTY('ProductVersion'))")
	if qerr == nil && len(rows) > 0 && len(rows[0]) > 0 {
		return string(rows[0][0]), nil
	}
	if p.version != "" {
		return p.version, qerr
	}
	return "tds", qerr
}

func qIdentSafe(v string) string { return "[" + strings.ReplaceAll(v, "]", "]]") + "]" }
func qStr(v string) string       { return "N'" + strings.ReplaceAll(v, "'", "''") + "'" }
func parseInt64(v []byte) int64 {
	n, _ := strconv.ParseInt(strings.TrimSpace(string(v)), 10, 64)
	return n
}
func parseBoolText(v []byte) bool {
	s := strings.TrimSpace(strings.ToLower(string(v)))
	return s == "1" || s == "true" || s == "t"
}
func isBinaryType(dt string) bool {
	switch strings.ToLower(dt) {
	case "binary", "varbinary", "image", "rowversion", "timestamp":
		return true
	}
	return false
}
func isNumericType(dt string) bool {
	switch strings.ToLower(dt) {
	case "tinyint", "smallint", "int", "bigint", "decimal", "numeric", "money", "smallmoney", "real", "float":
		return true
	}
	return false
}
func sqlServerColumnType(dt string, maxLen, precision, scale int) string {
	dt = strings.ToLower(dt)
	switch dt {
	case "varchar", "char", "varbinary", "binary":
		if maxLen > 0 {
			return fmt.Sprintf("%s(%d)", dt, maxLen)
		}
	case "nvarchar", "nchar":
		if maxLen > 0 {
			return fmt.Sprintf("%s(%d)", dt, maxLen/2)
		}
	case "decimal", "numeric":
		if precision > 0 {
			return fmt.Sprintf("%s(%d,%d)", dt, precision, scale)
		}
	case "datetime2", "datetimeoffset", "time":
		if scale >= 0 {
			return fmt.Sprintf("%s(%d)", dt, scale)
		}
	}
	return dt
}
func selectExpr(col domain.ColumnInfo) string {
	q := qIdentSafe(col.Name)
	if isBinaryType(col.DataType) {
		return "CONVERT(varbinary(max)," + q + ")"
	}
	dt := strings.ToLower(col.DataType)
	if dt == "date" || dt == "datetime" || dt == "datetime2" || dt == "smalldatetime" || dt == "datetimeoffset" || dt == "time" {
		return "CONVERT(nvarchar(max)," + q + ",126)"
	}
	return "CONVERT(nvarchar(max)," + q + ")"
}

func (c *Connector) ListSchemas(ctx context.Context) ([]domain.SchemaInfo, error) {
	p, e := c.get(ctx)
	if e != nil {
		return nil, e
	}
	r, _, _, e := p.query(ctx, "SELECT CONVERT(nvarchar(4000),name) FROM sys.schemas WHERE name NOT IN (N'sys',N'INFORMATION_SCHEMA') ORDER BY name")
	if e != nil {
		return nil, e
	}
	out := make([]domain.SchemaInfo, 0, len(r))
	for _, row := range r {
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
	q := `SELECT CONVERT(nvarchar(4000),t.name),CONVERT(nvarchar(4000),COALESCE(SUM(CASE WHEN p.index_id IN (0,1) THEN p.rows ELSE 0 END),0)),CONVERT(nvarchar(4000),COALESCE(SUM(a.total_pages)*8192,0)) FROM sys.tables t JOIN sys.schemas s ON s.schema_id=t.schema_id LEFT JOIN sys.partitions p ON p.object_id=t.object_id LEFT JOIN sys.allocation_units a ON a.container_id=p.partition_id WHERE s.name=` + qStr(schema) + ` GROUP BY t.name ORDER BY t.name`
	r, _, _, e := p.query(ctx, q)
	if e != nil {
		return nil, e
	}
	out := make([]domain.TableInfo, 0, len(r))
	for _, row := range r {
		if len(row) >= 3 {
			out = append(out, domain.TableInfo{Schema: schema, Name: string(row[0]), Rows: parseInt64(row[1]), DataLength: parseInt64(row[2])})
		}
	}
	return out, nil
}
func (c *Connector) GetTableMetadata(ctx context.Context, schemaName, table string) (*domain.TableMetadata, error) {
	p, e := c.get(ctx)
	if e != nil {
		return nil, e
	}
	obj := qStr(schemaName + "." + table)
	cq := `SELECT CONVERT(nvarchar(4000),c.name),CONVERT(nvarchar(4000),ty.name),CONVERT(nvarchar(4000),c.max_length),CONVERT(nvarchar(4000),c.precision),CONVERT(nvarchar(4000),c.scale),CONVERT(nvarchar(4000),c.is_nullable),CONVERT(nvarchar(4000),c.is_identity),CONVERT(nvarchar(4000),c.column_id),CONVERT(nvarchar(4000),c.is_computed),COALESCE(CONVERT(nvarchar(4000),ic.seed_value),N''),COALESCE(CONVERT(nvarchar(4000),ic.increment_value),N'') FROM sys.columns c JOIN sys.types ty ON ty.user_type_id=c.user_type_id LEFT JOIN sys.identity_columns ic ON ic.object_id=c.object_id AND ic.column_id=c.column_id WHERE c.object_id=OBJECT_ID(` + obj + `) ORDER BY c.column_id`
	rows, _, _, e := p.query(ctx, cq)
	if e != nil {
		return nil, e
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("SQL Server table %s.%s not found", schemaName, table)
	}
	meta := &domain.TableMetadata{Schema: schemaName, Name: table}
	for _, r := range rows {
		if len(r) < 11 {
			continue
		}
		maxLen := int(parseInt64(r[2]))
		prec := int(parseInt64(r[3]))
		scale := int(parseInt64(r[4]))
		dt := string(r[1])
		extra := ""
		if parseBoolText(r[6]) {
			seed, increment := strings.TrimSpace(string(r[9])), strings.TrimSpace(string(r[10]))
			if connector.ValidateNumericLiteral([]byte(seed), false) != nil {
				seed = "1"
			}
			if connector.ValidateNumericLiteral([]byte(increment), false) != nil {
				increment = "1"
			}
			extra = "IDENTITY(" + seed + "," + increment + ")"
		}
		if parseBoolText(r[8]) {
			if extra != "" {
				extra += ","
			}
			extra += "COMPUTED_SOURCE"
		}
		columnType := sqlServerColumnType(dt, maxLen, prec, scale)
		if strings.EqualFold(dt, "timestamp") || strings.EqualFold(dt, "rowversion") {
			dt = "rowversion"
			columnType = "varbinary(8)"
			if extra != "" {
				extra += ","
			}
			extra += "ROWVERSION_SOURCE"
		}
		meta.Columns = append(meta.Columns, domain.ColumnInfo{Name: string(r[0]), DataType: dt, ColumnType: columnType, Nullable: parseBoolText(r[5]), Extra: extra, Ordinal: int(parseInt64(r[7]))})
	}
	// Index and primary-key metadata.
	iq := `SELECT CONVERT(nvarchar(4000),i.name),CONVERT(nvarchar(4000),i.is_unique),CONVERT(nvarchar(4000),i.is_primary_key),CONVERT(nvarchar(4000),c.name),CONVERT(nvarchar(4000),ic.key_ordinal) FROM sys.indexes i JOIN sys.index_columns ic ON ic.object_id=i.object_id AND ic.index_id=i.index_id JOIN sys.columns c ON c.object_id=ic.object_id AND c.column_id=ic.column_id WHERE i.object_id=OBJECT_ID(` + obj + `) AND ic.key_ordinal>0 ORDER BY i.index_id,ic.key_ordinal`
	ir, _, _, _ := p.query(ctx, iq)
	idxMap := map[string]*domain.IndexInfo{}
	order := []string{}
	for _, r := range ir {
		if len(r) < 5 {
			continue
		}
		name := string(r[0])
		idx := idxMap[name]
		if idx == nil {
			idx = &domain.IndexInfo{Name: name, Unique: parseBoolText(r[1]), Primary: parseBoolText(r[2])}
			idxMap[name] = idx
			order = append(order, name)
		}
		idx.Columns = append(idx.Columns, string(r[3]))
	}
	for _, name := range order {
		idx := idxMap[name]
		meta.Indexes = append(meta.Indexes, *idx)
		if idx.Primary {
			meta.PrimaryKeys = append(meta.PrimaryKeys, idx.Columns...)
		}
	}
	pkSet := map[string]bool{}
	for _, k := range meta.PrimaryKeys {
		pkSet[strings.ToLower(k)] = true
	}
	for i := range meta.Columns {
		meta.Columns[i].PrimaryKey = pkSet[strings.ToLower(meta.Columns[i].Name)]
	}
	if len(meta.PrimaryKeys) == 1 {
		meta.PrimaryKey = meta.PrimaryKeys[0]
		for _, col := range meta.Columns {
			if strings.EqualFold(col.Name, meta.PrimaryKey) {
				meta.PrimaryKeyType = col.ColumnType
				meta.PrimaryKeyNumeric = isNumericType(col.DataType)
				break
			}
		}
	}
	// Foreign keys.
	fq := `SELECT CONVERT(nvarchar(4000),fk.name),CONVERT(nvarchar(4000),pc.name),CONVERT(nvarchar(4000),rs.name),CONVERT(nvarchar(4000),rt.name),CONVERT(nvarchar(4000),rc.name),CONVERT(nvarchar(4000),fkc.constraint_column_id) FROM sys.foreign_keys fk JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id=fk.object_id JOIN sys.columns pc ON pc.object_id=fkc.parent_object_id AND pc.column_id=fkc.parent_column_id JOIN sys.tables rt ON rt.object_id=fkc.referenced_object_id JOIN sys.schemas rs ON rs.schema_id=rt.schema_id JOIN sys.columns rc ON rc.object_id=fkc.referenced_object_id AND rc.column_id=fkc.referenced_column_id WHERE fk.parent_object_id=OBJECT_ID(` + obj + `) ORDER BY fk.object_id,fkc.constraint_column_id`
	fr, _, _, _ := p.query(ctx, fq)
	fkMap := map[string]*domain.ForeignKeyInfo{}
	fkOrder := []string{}
	for _, r := range fr {
		if len(r) < 5 {
			continue
		}
		name := string(r[0])
		fk := fkMap[name]
		if fk == nil {
			fk = &domain.ForeignKeyInfo{Name: name, ReferencedSchema: string(r[2]), ReferencedTable: string(r[3])}
			fkMap[name] = fk
			fkOrder = append(fkOrder, name)
		}
		fk.Columns = append(fk.Columns, string(r[1]))
		fk.ReferencedColumns = append(fk.ReferencedColumns, string(r[4]))
	}
	for _, n := range fkOrder {
		meta.ForeignKeys = append(meta.ForeignKeys, *fkMap[n])
	}
	// Estimated rows/data size.
	statsQ := `SELECT CONVERT(nvarchar(4000),COALESCE(SUM(CASE WHEN p.index_id IN (0,1) THEN p.rows ELSE 0 END),0)),CONVERT(nvarchar(4000),COALESCE(SUM(a.total_pages)*8192,0)) FROM sys.partitions p LEFT JOIN sys.allocation_units a ON a.container_id=p.partition_id WHERE p.object_id=OBJECT_ID(` + obj + `)`
	if sr, _, _, er := p.query(ctx, statsQ); er == nil && len(sr) > 0 && len(sr[0]) >= 2 {
		meta.EstimatedRows = parseInt64(sr[0][0])
		meta.DataLength = parseInt64(sr[0][1])
		meta.HasRows = meta.EstimatedRows > 0
	}
	if meta.PrimaryKeyNumeric && meta.PrimaryKey != "" {
		mq := `SELECT CONVERT(nvarchar(4000),MIN(` + qIdentSafe(meta.PrimaryKey) + `)),CONVERT(nvarchar(4000),MAX(` + qIdentSafe(meta.PrimaryKey) + `)) FROM ` + qIdentSafe(schemaName) + `.` + qIdentSafe(table)
		if mr, mn, _, er := p.query(ctx, mq); er == nil && len(mr) > 0 && len(mr[0]) >= 2 {
			if len(mn) == 0 || !mn[0][0] {
				meta.MinPK = parseInt64(mr[0][0])
			}
			if len(mn) == 0 || !mn[0][1] {
				meta.MaxPK = parseInt64(mr[0][1])
			}
		}
	}
	return meta, nil
}

func keyLiteral(col domain.ColumnInfo, v connector.Value) string { return formatValue(col, v) }
func lexCompare(keys []string, cols []domain.ColumnInfo, vals []connector.Value, op string) string {
	if len(keys) == 0 || len(keys) != len(cols) || len(keys) != len(vals) {
		return "(1=0)"
	}
	strict := op
	inclusive := false
	switch op {
	case ">=":
		strict, inclusive = ">", true
	case "<=":
		strict, inclusive = "<", true
	case ">", "<":
	default:
		return "(1=0)"
	}
	parts := make([]string, 0, len(keys)+1)
	for i := range keys {
		and := make([]string, 0, i+1)
		for j := 0; j < i; j++ {
			and = append(and, qIdentSafe(keys[j])+"="+keyLiteral(cols[j], vals[j]))
		}
		and = append(and, qIdentSafe(keys[i])+strict+keyLiteral(cols[i], vals[i]))
		parts = append(parts, "("+strings.Join(and, " AND ")+")")
	}
	if inclusive {
		eq := make([]string, len(keys))
		for i := range keys {
			eq[i] = qIdentSafe(keys[i]) + "=" + keyLiteral(cols[i], vals[i])
		}
		parts = append(parts, "("+strings.Join(eq, " AND ")+")")
	}
	return "(" + strings.Join(parts, " OR ") + ")"
}

func sqlServerKeyColumns(keys []string, columns []domain.ColumnInfo) ([]domain.ColumnInfo, error) {
	byName := make(map[string]domain.ColumnInfo, len(columns))
	for _, col := range columns {
		byName[strings.ToLower(col.Name)] = col
	}
	out := make([]domain.ColumnInfo, len(keys))
	for i, key := range keys {
		col, ok := byName[strings.ToLower(key)]
		if !ok {
			return nil, fmt.Errorf("migration key column %s is not present in table metadata", key)
		}
		out[i] = col
	}
	return out, nil
}

// PlanKeysetBoundaries finds real source keys at ordered NTILE boundaries.
// SQL Server lacks row-value tuple comparisons, so lower/upper predicates use
// the same exact lexicographic expansion as ReadBatch. Returned values are the
// converted wire representation consumed by QMigration's durable keyset cursor.
func (c *Connector) PlanKeysetBoundaries(ctx context.Context, req connector.KeysetBoundaryRequest) ([][]connector.Value, error) {
	if req.Partitions <= 1 {
		return nil, nil
	}
	if len(req.Keys) == 0 {
		return nil, errors.New("keyset boundary planning requires migration key columns")
	}
	keyCols, err := sqlServerKeyColumns(req.Keys, req.Columns)
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
			if err := validateSQLServerValues(keyCols, bound); err != nil {
				return nil, fmt.Errorf("keyset boundary: %w", err)
			}
		}
	}
	p, err := c.get(ctx)
	if err != nil {
		return nil, err
	}
	keyNames := make([]string, len(req.Keys))
	converted := make([]string, len(req.Keys))
	for i, key := range req.Keys {
		keyNames[i] = qIdentSafe(key)
		converted[i] = selectExpr(keyCols[i])
	}
	conditions := []string{}
	if len(req.LowerBound) > 0 {
		conditions = append(conditions, lexCompare(req.Keys, keyCols, req.LowerBound, ">="))
	}
	if len(req.UpperBound) > 0 {
		conditions = append(conditions, lexCompare(req.Keys, keyCols, req.UpperBound, "<"))
	}
	where := ""
	if len(conditions) > 0 {
		where = " WHERE " + strings.Join(conditions, " AND ")
	}
	q := "WITH qm_ranked AS (SELECT " + strings.Join(keyNames, ",") + ", NTILE(" + strconv.Itoa(req.Partitions) + ") OVER (ORDER BY " + strings.Join(keyNames, ",") + ") AS qm_bucket FROM " + qIdentSafe(req.Schema) + "." + qIdentSafe(req.Table) + where + "), " +
		"qm_bounds AS (SELECT " + strings.Join(keyNames, ",") + ",qm_bucket,ROW_NUMBER() OVER (PARTITION BY qm_bucket ORDER BY " + strings.Join(keyNames, ",") + ") AS qm_rn FROM qm_ranked) " +
		"SELECT " + strings.Join(converted, ",") + " FROM qm_bounds WHERE qm_bucket>1 AND qm_rn=1 ORDER BY qm_bucket"
	rows, nulls, _, err := p.query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("ordered SQL Server keyset boundary query: %w", err)
	}
	out := make([][]connector.Value, 0, len(rows))
	for ri, row := range rows {
		if len(row) != len(req.Keys) {
			return nil, fmt.Errorf("boundary query returned %d columns for %d migration keys", len(row), len(req.Keys))
		}
		b := make([]connector.Value, len(row))
		for i, raw := range row {
			if ri < len(nulls) && i < len(nulls[ri]) && nulls[ri][i] {
				return nil, fmt.Errorf("migration key %s returned NULL boundary", req.Keys[i])
			}
			b[i] = connector.Value{Raw: append([]byte(nil), raw...)}
		}
		out = append(out, b)
	}
	return out, nil
}

type sqlServerPartitionDescriptor struct {
	Function string `json:"function"`
	Column   string `json:"column"`
	Number   int    `json:"number"`
}

func encodeSQLServerPartition(v sqlServerPartitionDescriptor) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func decodeSQLServerPartition(v string) (sqlServerPartitionDescriptor, error) {
	var out sqlServerPartitionDescriptor
	if err := json.Unmarshal([]byte(strings.TrimSpace(v)), &out); err != nil {
		return out, fmt.Errorf("invalid SQL Server partition descriptor: %w", err)
	}
	if strings.TrimSpace(out.Function) == "" || strings.TrimSpace(out.Column) == "" || out.Number <= 0 {
		return out, errors.New("SQL Server partition descriptor requires function, column and positive partition number")
	}
	return out, nil
}

func (c *Connector) ListTablePartitions(ctx context.Context, schemaName, table string) ([]string, error) {
	p, err := c.get(ctx)
	if err != nil {
		return nil, err
	}
	obj := qStr(schemaName + "." + table)
	q := `SELECT CONVERT(nvarchar(4000),pf.name),CONVERT(nvarchar(4000),c.name),CONVERT(nvarchar(40),p.partition_number)
FROM sys.indexes i
JOIN sys.partition_schemes ps ON ps.data_space_id=i.data_space_id
JOIN sys.partition_functions pf ON pf.function_id=ps.function_id
JOIN sys.index_columns ic ON ic.object_id=i.object_id AND ic.index_id=i.index_id AND ic.partition_ordinal=1
JOIN sys.columns c ON c.object_id=ic.object_id AND c.column_id=ic.column_id
JOIN sys.partitions p ON p.object_id=i.object_id AND p.index_id=i.index_id
WHERE i.object_id=OBJECT_ID(` + obj + `) AND i.index_id IN (0,1)
ORDER BY p.partition_number`
	rows, _, _, err := p.query(ctx, q)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(rows))
	seen := map[int]bool{}
	for _, row := range rows {
		if len(row) < 3 {
			continue
		}
		n := int(parseInt64(row[2]))
		if n <= 0 || seen[n] {
			continue
		}
		seen[n] = true
		encoded, err := encodeSQLServerPartition(sqlServerPartitionDescriptor{Function: string(row[0]), Column: string(row[1]), Number: n})
		if err != nil {
			return nil, err
		}
		out = append(out, encoded)
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
	if len(req.Columns) == 0 {
		return nil, errors.New("no selected columns")
	}
	selected := req.Columns
	exprs := make([]string, len(selected))
	index := map[string]int{}
	for i, col := range selected {
		exprs[i] = selectExpr(col)
		index[strings.ToLower(col.Name)] = i
	}
	keys := append([]string(nil), req.PrimaryKeys...)
	if len(keys) == 0 && req.PrimaryKey != "" {
		keys = []string{req.PrimaryKey}
	}
	keyCols := make([]domain.ColumnInfo, len(keys))
	for i, k := range keys {
		idx, ok := index[strings.ToLower(k)]
		if !ok {
			return nil, fmt.Errorf("key %s not in selected columns", k)
		}
		keyCols[i] = selected[idx]
	}
	for _, bound := range [][]connector.Value{req.Cursor, req.LowerBound, req.UpperBound} {
		if len(bound) > 0 && len(bound) == len(keyCols) {
			if err := validateSQLServerValues(keyCols, bound); err != nil {
				return nil, fmt.Errorf("read keyset value: %w", err)
			}
		}
	}
	conditions := []string{}
	if req.UseKeyset {
		for _, b := range [][]connector.Value{req.Cursor, req.LowerBound, req.UpperBound} {
			if len(b) > 0 && len(b) != len(keys) {
				return nil, errors.New("keyset bound/key count mismatch")
			}
		}
		if len(req.Cursor) > 0 {
			conditions = append(conditions, lexCompare(keys, keyCols, req.Cursor, ">"))
		} else if len(req.LowerBound) > 0 {
			conditions = append(conditions, lexCompare(keys, keyCols, req.LowerBound, ">="))
		}
		if len(req.UpperBound) > 0 {
			conditions = append(conditions, lexCompare(keys, keyCols, req.UpperBound, "<"))
		}
	} else {
		if len(keys) == 0 {
			return nil, errors.New("range read requires primary key")
		}
		pk := qIdentSafe(keys[0])
		conditions = append(conditions, pk+">="+strconv.FormatInt(req.StartPK, 10), pk+"<="+strconv.FormatInt(req.EndPK, 10))
		if req.HasAfter {
			conditions = append(conditions, pk+">"+strconv.FormatInt(req.AfterPK, 10))
		}
	}
	if strings.TrimSpace(req.Partition) != "" {
		part, err := decodeSQLServerPartition(req.Partition)
		if err != nil {
			return nil, err
		}
		conditions = append(conditions, "$PARTITION."+qIdentSafe(part.Function)+"("+qIdentSafe(part.Column)+")="+strconv.Itoa(part.Number))
	}
	if req.HashBuckets > 0 {
		if req.HashBucket < 0 || req.HashBucket >= req.HashBuckets {
			return nil, errors.New("invalid hash bucket")
		}
		parts := make([]string, len(keys))
		for i, k := range keys {
			parts[i] = "COALESCE(CONVERT(nvarchar(4000)," + qIdentSafe(k) + "),N'<NULL>')"
		}
		conditions = append(conditions, "((CONVERT(bigint,CHECKSUM(CONCAT("+strings.Join(parts, ",N'#',")+"))) & 2147483647) % "+strconv.Itoa(req.HashBuckets)+")="+strconv.Itoa(req.HashBucket))
	}
	if strings.TrimSpace(req.CustomWhere) != "" {
		conditions = append(conditions, "("+strings.TrimSpace(req.CustomWhere)+")")
	}
	where := ""
	if len(conditions) > 0 {
		where = " WHERE " + strings.Join(conditions, " AND ")
	}
	orders := make([]string, len(keys))
	for i, k := range keys {
		orders[i] = qIdentSafe(k)
	}
	q := `SELECT TOP (` + strconv.Itoa(req.Limit) + `) ` + strings.Join(exprs, ",") + ` FROM ` + qIdentSafe(req.Schema) + `.` + qIdentSafe(req.Table) + where + ` ORDER BY ` + strings.Join(orders, ",")
	r, nulls, _, e := p.query(ctx, q)
	if e != nil {
		return nil, e
	}
	batch := &connector.RowBatch{Rows: make([][]connector.Value, 0, len(r))}
	for ri, row := range r {
		vals := make([]connector.Value, len(row))
		for i, raw := range row {
			isNull := ri < len(nulls) && i < len(nulls[ri]) && nulls[ri][i]
			vals[i] = connector.Value{Null: isNull, Raw: append([]byte(nil), raw...)}
			if !isNull {
				batch.Bytes += int64(len(raw))
			}
		}
		batch.Rows = append(batch.Rows, vals)
		if req.UseKeyset {
			batch.LastKey = batch.LastKey[:0]
			for _, k := range keys {
				v := vals[index[strings.ToLower(k)]]
				v.Raw = append([]byte(nil), v.Raw...)
				batch.LastKey = append(batch.LastKey, v)
			}
		} else if len(keys) > 0 {
			v := vals[index[strings.ToLower(keys[0])]]
			if !v.Null {
				batch.LastPK, e = strconv.ParseInt(strings.TrimSpace(string(v.Raw)), 10, 64)
				if e != nil {
					return nil, e
				}
			}
		}
	}
	return batch, nil
}
func (c *Connector) ReadByKey(ctx context.Context, req connector.ReadByKeyRequest) ([]connector.Value, bool, error) {
	if len(req.PrimaryKeys) == 0 || len(req.PrimaryKeys) != len(req.KeyValues) || len(req.PrimaryKeys) != len(req.KeyColumns) {
		return nil, false, errors.New("invalid point lookup key")
	}
	if err := validateSQLServerValues(req.KeyColumns, req.KeyValues); err != nil {
		return nil, false, fmt.Errorf("point lookup key: %w", err)
	}
	p, e := c.get(ctx)
	if e != nil {
		return nil, false, e
	}
	exprs := make([]string, len(req.Columns))
	for i, col := range req.Columns {
		exprs[i] = selectExpr(col)
	}
	conds := make([]string, len(req.PrimaryKeys))
	for i, k := range req.PrimaryKeys {
		conds[i] = qIdentSafe(k) + "=" + formatValue(req.KeyColumns[i], req.KeyValues[i])
	}
	q := `SELECT TOP (1) ` + strings.Join(exprs, ",") + ` FROM ` + qIdentSafe(req.Schema) + `.` + qIdentSafe(req.Table) + ` WITH (UPDLOCK,HOLDLOCK) WHERE ` + strings.Join(conds, " AND ")
	r, nulls, _, e := p.query(ctx, q)
	if e != nil {
		return nil, false, e
	}
	if len(r) == 0 {
		return nil, false, nil
	}
	out := make([]connector.Value, len(r[0]))
	for i, v := range r[0] {
		out[i] = connector.Value{Null: len(nulls) > 0 && i < len(nulls[0]) && nulls[0][i], Raw: append([]byte(nil), v...)}
	}
	return out, true, nil
}

func (c *Connector) SampleRuntimeLoad(ctx context.Context) (domain.DatabaseRuntimeLoad, error) {
	p, err := c.get(ctx)
	if err != nil {
		return domain.DatabaseRuntimeLoad{}, err
	}
	q := `SELECT CONVERT(nvarchar(40),@@MAX_CONNECTIONS),CONVERT(nvarchar(40),(SELECT COUNT(*) FROM sys.dm_exec_sessions WHERE is_user_process=1)),CONVERT(nvarchar(40),(SELECT COUNT(*) FROM sys.dm_exec_requests WHERE session_id<>@@SPID))`
	rows, _, _, err := p.query(ctx, q)
	if err != nil {
		return domain.DatabaseRuntimeLoad{}, fmt.Errorf("sample SQL Server runtime load (VIEW SERVER STATE may be required): %w", err)
	}
	load := domain.DatabaseRuntimeLoad{}
	if len(rows) == 0 || len(rows[0]) < 3 {
		return load, nil
	}
	load.MaxConnections = parseInt64(rows[0][0])
	load.Connections = parseInt64(rows[0][1])
	load.RunningQueries = parseInt64(rows[0][2])
	if load.MaxConnections > 0 {
		load.ConnectionUsagePct = float64(load.Connections) * 100 / float64(load.MaxConnections)
	}
	return load, nil
}

func validateSQLServerValue(col domain.ColumnInfo, v connector.Value) error {
	if v.Null {
		return nil
	}
	switch strings.ToLower(col.DataType) {
	case "tinyint", "smallint", "mediumint", "int", "integer", "bigint", "decimal", "numeric", "money", "smallmoney", "real", "float", "double", "double precision":
		return connector.ValidateNumericLiteral(v.Raw, false)
	default:
		return nil
	}
}

func validateSQLServerValues(cols []domain.ColumnInfo, values []connector.Value) error {
	if len(cols) != len(values) {
		return errors.New("column/value count mismatch")
	}
	for i := range cols {
		if err := validateSQLServerValue(cols[i], values[i]); err != nil {
			return fmt.Errorf("column %s: %w", cols[i].Name, err)
		}
	}
	return nil
}

func formatValue(col domain.ColumnInfo, v connector.Value) string {
	if v.Null {
		return "NULL"
	}
	raw := string(v.Raw)
	dt := strings.ToLower(col.DataType)
	if isNumericType(dt) {
		return raw
	}
	if dt == "bit" || dt == "boolean" {
		if raw == "1" || strings.EqualFold(raw, "true") || raw == "t" {
			return "1"
		}
		return "0"
	}
	if isBinaryType(dt) {
		return "0x" + fmt.Sprintf("%x", v.Raw)
	}
	return qStr(raw)
}
func (c *Connector) WriteBatch(ctx context.Context, req connector.WriteBatchRequest) (int64, error) {
	if len(req.Rows) == 0 {
		return 0, nil
	}
	p, e := c.get(ctx)
	if e != nil {
		return 0, e
	}
	cols := req.Columns
	if len(cols) == 0 {
		return 0, errors.New("no target columns")
	}
	ids := make([]string, len(cols))
	for i, col := range cols {
		ids[i] = qIdentSafe(col.Name)
	}
	var values []string
	for _, row := range req.Rows {
		if len(row) != len(cols) {
			return 0, errors.New("row/column mismatch")
		}
		if err := validateSQLServerValues(cols, row); err != nil {
			return 0, fmt.Errorf("target row: %w", err)
		}
		v := make([]string, len(cols))
		for i := range cols {
			v[i] = formatValue(cols[i], row[i])
		}
		values = append(values, "("+strings.Join(v, ",")+")")
	}
	keys := append([]string(nil), req.PrimaryKeys...)
	if len(keys) == 0 && req.PrimaryKey != "" {
		keys = []string{req.PrimaryKey}
	}
	var q string
	if len(keys) == 0 {
		q = "INSERT INTO " + qIdentSafe(req.Schema) + "." + qIdentSafe(req.Table) + " (" + strings.Join(ids, ",") + ") VALUES " + strings.Join(values, ",") + ";"
	} else {
		pkset := map[string]bool{}
		on := make([]string, len(keys))
		for i, k := range keys {
			pkset[strings.ToLower(k)] = true
			on[i] = "T." + qIdentSafe(k) + "=S." + qIdentSafe(k)
		}
		updates := []string{}
		for _, col := range cols {
			if pkset[strings.ToLower(col.Name)] {
				continue
			}
			id := qIdentSafe(col.Name)
			updates = append(updates, "T."+id+"=S."+id)
		}
		q = "MERGE " + qIdentSafe(req.Schema) + "." + qIdentSafe(req.Table) + " WITH (HOLDLOCK) AS T USING (VALUES " + strings.Join(values, ",") + ") AS S (" + strings.Join(ids, ",") + ") ON " + strings.Join(on, " AND ")
		if len(updates) > 0 {
			q += " WHEN MATCHED THEN UPDATE SET " + strings.Join(updates, ",")
		}
		q += " WHEN NOT MATCHED THEN INSERT (" + strings.Join(ids, ",") + ") VALUES ("
		src := make([]string, len(ids))
		for i, id := range ids {
			src[i] = "S." + id
		}
		q += strings.Join(src, ",") + ");"
	}
	identityInsert := sqlServerHasIdentity(cols)
	identitySQL := qIdentSafe(req.Schema) + "." + qIdentSafe(req.Table)
	if identityInsert {
		if _, e = p.exec(ctx, "SET IDENTITY_INSERT "+identitySQL+" ON"); e != nil {
			return 0, e
		}
	}
	_, execErr := p.exec(ctx, q)
	var cleanupErr error
	if identityInsert {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, cleanupErr = p.exec(cleanupCtx, "SET IDENTITY_INSERT "+identitySQL+" OFF")
		cancel()
		if cleanupErr != nil {
			_ = c.Close()
		}
	}
	if execErr != nil {
		if cleanupErr != nil {
			return 0, fmt.Errorf("write batch: %v; identity_insert cleanup: %w", execErr, cleanupErr)
		}
		return 0, execErr
	}
	if cleanupErr != nil {
		return 0, fmt.Errorf("identity_insert cleanup: %w", cleanupErr)
	}
	return int64(len(req.Rows)), nil
}
func (c *Connector) DeleteByKey(ctx context.Context, req connector.DeleteByKeyRequest) error {
	p, e := c.get(ctx)
	if e != nil {
		return e
	}
	keys := append([]string(nil), req.PrimaryKeys...)
	vals := append([]connector.Value(nil), req.Values...)
	cols := append([]domain.ColumnInfo(nil), req.Columns...)
	if len(keys) == 0 && req.PrimaryKey != "" {
		keys = []string{req.PrimaryKey}
		vals = []connector.Value{req.Value}
		cols = []domain.ColumnInfo{req.Column}
	}
	if len(keys) == 0 || len(keys) != len(vals) || len(keys) != len(cols) {
		return errors.New("invalid delete key")
	}
	if err := validateSQLServerValues(cols, vals); err != nil {
		return fmt.Errorf("delete key: %w", err)
	}
	conds := make([]string, len(keys))
	for i, k := range keys {
		conds[i] = qIdentSafe(k) + "=" + formatValue(cols[i], vals[i])
	}
	_, e = p.exec(ctx, "DELETE FROM "+qIdentSafe(req.Schema)+"."+qIdentSafe(req.Table)+" WHERE "+strings.Join(conds, " AND "))
	return e
}
func (c *Connector) BeginCDCTransaction(ctx context.Context) error {
	p, e := c.get(ctx)
	if e != nil {
		return e
	}
	_, e = p.exec(ctx, "BEGIN TRANSACTION")
	return e
}
func (c *Connector) CommitCDCTransaction(ctx context.Context) error {
	p, e := c.get(ctx)
	if e != nil {
		return e
	}
	_, e = p.exec(ctx, "COMMIT TRANSACTION")
	return e
}
func (c *Connector) RollbackCDCTransaction(ctx context.Context) error {
	p, e := c.get(ctx)
	if e != nil {
		return e
	}
	_, e = p.exec(ctx, "IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION")
	return e
}
func (c *Connector) ExecDDL(ctx context.Context, _ string, ddl string) error {
	p, e := c.get(ctx)
	if e != nil {
		return e
	}
	_, e = p.exec(ctx, ddl)
	return e
}
func (c *Connector) EnsureSchema(ctx context.Context, schemaName string) error {
	p, e := c.get(ctx)
	if e != nil {
		return e
	}
	_, e = p.exec(ctx, "IF SCHEMA_ID("+qStr(schemaName)+") IS NULL EXEC(N'CREATE SCHEMA "+strings.ReplaceAll(qIdentSafe(schemaName), "'", "''")+"')")
	return e
}
func sqlServerIdentityClause(col domain.ColumnInfo) (string, bool, error) {
	extra := strings.ToUpper(strings.TrimSpace(col.Extra))
	if !strings.Contains(extra, "IDENTITY") && !strings.Contains(strings.ToLower(col.Extra), "auto_increment") {
		return "", false, nil
	}
	seed, increment := "1", "1"
	if start := strings.Index(extra, "IDENTITY("); start >= 0 {
		rest := extra[start+len("IDENTITY("):]
		if end := strings.IndexByte(rest, ')'); end >= 0 {
			parts := strings.Split(rest[:end], ",")
			if len(parts) == 2 {
				seed, increment = strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
			}
		}
	}
	if err := connector.ValidateNumericLiteral([]byte(seed), false); err != nil {
		return "", false, fmt.Errorf("identity seed: %w", err)
	}
	if err := connector.ValidateNumericLiteral([]byte(increment), false); err != nil {
		return "", false, fmt.Errorf("identity increment: %w", err)
	}
	return " IDENTITY(" + seed + "," + increment + ")", true, nil
}

func sqlServerHasIdentity(cols []domain.ColumnInfo) bool {
	for _, col := range cols {
		if _, ok, _ := sqlServerIdentityClause(col); ok {
			return true
		}
	}
	return false
}

func sqlServerTargetType(col domain.ColumnInfo) string {
	dt := strings.ToLower(strings.TrimSpace(col.DataType))
	ct := strings.ToLower(strings.TrimSpace(col.ColumnType))
	unsigned := strings.Contains(ct, "unsigned")
	length := func(fallback int) int {
		start := strings.IndexByte(ct, '(')
		end := strings.IndexByte(ct, ')')
		if start < 0 || end <= start+1 {
			return fallback
		}
		first := strings.SplitN(ct[start+1:end], ",", 2)[0]
		n, err := strconv.Atoi(strings.TrimSpace(first))
		if err != nil || n <= 0 {
			return fallback
		}
		return n
	}
	precisionScale := func() (int, int, bool) {
		start := strings.IndexByte(ct, '(')
		end := strings.IndexByte(ct, ')')
		if start < 0 || end <= start+1 {
			return 0, 0, false
		}
		parts := strings.Split(ct[start+1:end], ",")
		p, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil || p <= 0 || p > 38 {
			return 0, 0, false
		}
		scale := 0
		if len(parts) > 1 {
			scale, err = strconv.Atoi(strings.TrimSpace(parts[1]))
			if err != nil || scale < 0 || scale > p {
				return 0, 0, false
			}
		}
		return p, scale, true
	}
	switch dt {
	case "tinyint":
		return "tinyint"
	case "smallint", "int2":
		if unsigned {
			return "int"
		}
		return "smallint"
	case "mediumint":
		return "int"
	case "int", "integer", "int4":
		if unsigned {
			return "bigint"
		}
		return "int"
	case "bigint", "int8":
		if unsigned {
			return "decimal(20,0)"
		}
		return "bigint"
	case "decimal", "numeric", "number":
		if p, sc, ok := precisionScale(); ok {
			return fmt.Sprintf("decimal(%d,%d)", p, sc)
		}
		return "decimal(38,10)"
	case "float", "double", "double precision", "float8":
		return "float(53)"
	case "real", "float4":
		return "real"
	case "boolean", "bool", "bit":
		return "bit"
	case "date":
		return "date"
	case "datetime", "datetime2", "timestamp", "timestamp without time zone":
		return "datetime2(6)"
	case "datetimeoffset", "timestamp with time zone", "timestamptz":
		return "datetimeoffset(6)"
	case "time", "time without time zone":
		return "time(6)"
	case "uuid", "uniqueidentifier":
		return "uniqueidentifier"
	case "rowversion":
		return "varbinary(8)"
	case "binary", "varbinary", "bytea", "raw", "blob", "tinyblob", "mediumblob", "longblob", "image":
		if dt == "binary" || dt == "varbinary" {
			n := length(0)
			if n > 0 && n <= 8000 {
				return "varbinary(" + strconv.Itoa(n) + ")"
			}
		}
		return "varbinary(max)"
	case "char", "varchar", "character varying", "nchar", "nvarchar":
		n := length(0)
		if n > 0 && n <= 4000 {
			return "nvarchar(" + strconv.Itoa(n) + ")"
		}
		return "nvarchar(max)"
	case "xml":
		return "xml"
	case "json", "jsonb", "text", "tinytext", "mediumtext", "longtext", "ntext", "clob":
		return "nvarchar(max)"
	default:
		return "nvarchar(max)"
	}
}

func (c *Connector) CreateTable(ctx context.Context, schemaName, table string, cols []domain.ColumnInfo, pk string) error {
	keys := []string{}
	if pk != "" {
		keys = []string{pk}
	}
	return c.CreateTableWithPrimaryKeys(ctx, schemaName, table, cols, keys)
}
func (c *Connector) CreateTableWithPrimaryKeys(ctx context.Context, schemaName, table string, cols []domain.ColumnInfo, keys []string) error {
	p, e := c.get(ctx)
	if e != nil {
		return e
	}
	defs := make([]string, 0, len(cols)+1)
	for _, col := range cols {
		d := qIdentSafe(col.Name) + " " + sqlServerTargetType(col)
		identityClause, _, identityErr := sqlServerIdentityClause(col)
		if identityErr != nil {
			return identityErr
		}
		d += identityClause
		if !col.Nullable {
			d += " NOT NULL"
		}
		defs = append(defs, d)
	}
	if len(keys) > 0 {
		qk := make([]string, len(keys))
		for i, k := range keys {
			qk[i] = qIdentSafe(k)
		}
		defs = append(defs, "PRIMARY KEY ("+strings.Join(qk, ",")+")")
	}
	full := schemaName + "." + table
	q := "IF OBJECT_ID(" + qStr(full) + ",N'U') IS NULL CREATE TABLE " + qIdentSafe(schemaName) + "." + qIdentSafe(table) + " (" + strings.Join(defs, ",") + ")"
	_, e = p.exec(ctx, q)
	return e
}
func (c *Connector) CreateIndex(ctx context.Context, schemaName, table string, index domain.IndexInfo) error {
	if index.Primary || len(index.Columns) == 0 {
		return nil
	}
	p, e := c.get(ctx)
	if e != nil {
		return e
	}
	cols := make([]string, len(index.Columns))
	for i, v := range index.Columns {
		cols[i] = qIdentSafe(v)
	}
	uniq := ""
	if index.Unique {
		uniq = "UNIQUE "
	}
	q := "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID(" + qStr(schemaName+"."+table) + ") AND name=" + qStr(index.Name) + ") CREATE " + uniq + "INDEX " + qIdentSafe(index.Name) + " ON " + qIdentSafe(schemaName) + "." + qIdentSafe(table) + " (" + strings.Join(cols, ",") + ")"
	_, e = p.exec(ctx, q)
	return e
}
func (c *Connector) CreateForeignKey(ctx context.Context, schemaName, table string, fk domain.ForeignKeyInfo) error {
	if len(fk.Columns) == 0 || len(fk.Columns) != len(fk.ReferencedColumns) {
		return errors.New("invalid foreign key")
	}
	p, e := c.get(ctx)
	if e != nil {
		return e
	}
	cols := make([]string, len(fk.Columns))
	refs := make([]string, len(fk.Columns))
	for i := range cols {
		cols[i] = qIdentSafe(fk.Columns[i])
		refs[i] = qIdentSafe(fk.ReferencedColumns[i])
	}
	q := "IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE parent_object_id=OBJECT_ID(" + qStr(schemaName+"."+table) + ") AND name=" + qStr(fk.Name) + ") ALTER TABLE " + qIdentSafe(schemaName) + "." + qIdentSafe(table) + " ADD CONSTRAINT " + qIdentSafe(fk.Name) + " FOREIGN KEY (" + strings.Join(cols, ",") + ") REFERENCES " + qIdentSafe(fk.ReferencedSchema) + "." + qIdentSafe(fk.ReferencedTable) + " (" + strings.Join(refs, ",") + ")"
	_, e = p.exec(ctx, q)
	return e
}
func (c *Connector) MigrationPrechecks(ctx context.Context, cdc bool) []domain.PrecheckItem {
	items := []domain.PrecheckItem{{Name: "SQL Server Native TDS", Level: domain.PrecheckWarning, Message: "experimental native TDS data plane; qualify against the exact SQL Server version before production"}}
	if cdc {
		if !sqlServerCDCEnabled() {
			items = append(items, domain.PrecheckItem{Name: "SQL Server source CDC", Level: domain.PrecheckFailed, Message: "native CDC reader is capability-gated; set QMIGRATION_EXPERIMENTAL_SQLSERVER_CDC=1 after enabling SQL Server CDC"})
		} else if pos, err := c.CurrentCDCPosition(ctx); err != nil {
			items = append(items, domain.PrecheckItem{Name: "SQL Server source CDC", Level: domain.PrecheckFailed, Message: err.Error()})
		} else {
			items = append(items, domain.PrecheckItem{Name: "SQL Server source CDC", Level: domain.PrecheckPass, Message: "database CDC enabled; current LSN=" + pos.PositionValue})
			if retention, rerr := c.CDCRetentionMinutes(ctx); rerr != nil {
				items = append(items, domain.PrecheckItem{Name: "SQL Server CDC retention", Level: domain.PrecheckWarning, Message: "unable to verify msdb.dbo.cdc_jobs retention: " + rerr.Error()})
			} else {
				minimum := sqlServerMinimumRetentionMinutes()
				level := domain.PrecheckPass
				message := fmt.Sprintf("cleanup retention=%d minutes; configured QMigration minimum=%d minutes; durable CDC staging is enabled; retention only needs to cover capture outages/backpressure, subject to this operational safety floor", retention, minimum)
				if retention < minimum {
					level = domain.PrecheckFailed
				}
				items = append(items, domain.PrecheckItem{Name: "SQL Server CDC retention", Level: level, Message: message})
			}
		}
	}
	return items
}

// PRELOGIN helpers are kept small and independently testable because they also
// form the TLS-negotiation boundary for the future native TDS TLS transport.
func buildPreloginPayload(encryption byte) []byte {
	// VERSION starts at offset 11 (6 bytes); ENCRYPTION starts at offset 17.
	return []byte{0x00, 0x00, 0x0b, 0x00, 0x06, 0x01, 0x00, 0x11, 0x00, 0x01, 0xff, 0, 0, 0, 0, 0, 0, encryption}
}
func buildPreloginPacket() []byte {
	payload := buildPreloginPayload(0x00)
	packet := make([]byte, 8+len(payload))
	packet[0], packet[1] = tdsPrelogin, tdsEOM
	binary.BigEndian.PutUint16(packet[2:4], uint16(len(packet)))
	packet[6] = 1
	copy(packet[8:], payload)
	return packet
}
func readTDSPacket(r io.Reader) ([]byte, error) {
	h := make([]byte, 8)
	if _, e := io.ReadFull(r, h); e != nil {
		return nil, e
	}
	ln := int(binary.BigEndian.Uint16(h[2:4]))
	if ln < 8 || ln > 1<<20 {
		return nil, fmt.Errorf("invalid TDS packet length %d", ln)
	}
	b := make([]byte, ln-8)
	_, e := io.ReadFull(r, b)
	return b, e
}
func parsePreloginVersion(body []byte) (string, error) {
	pi, e := parsePrelogin(body)
	return pi.Version, e
}

var _ connector.DataConnector = (*Connector)(nil)
var _ connector.CDCApplyConnector = (*Connector)(nil)
var _ connector.TransactionalCDCApplyConnector = (*Connector)(nil)
var _ connector.PointLookupConnector = (*Connector)(nil)
var _ connector.CompositeSchemaConnector = (*Connector)(nil)
var _ connector.PostLoadSchemaConnector = (*Connector)(nil)

var _ connector.KeysetBoundaryConnector = (*Connector)(nil)
var _ connector.PartitionConnector = (*Connector)(nil)
var _ connector.RuntimeLoadConnector = (*Connector)(nil)
