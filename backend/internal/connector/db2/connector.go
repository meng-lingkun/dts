package db2connector

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/big"
	"os"
	"strconv"
	"strings"
	"sync"

	"qmigration/backend/internal/cdc/db2log"
	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
)

type Factory struct{}

func NewFactory() *Factory { return &Factory{} }
func (*Factory) Capabilities(t domain.DataSourceType) connector.Descriptor {
	caps := []connector.Capability{connector.CapabilityProtocolProbe}
	note := "QMigration native DB2 DRDA/DDM protocol probe is implemented; data-plane capabilities remain gated until explicitly enabled"
	if experimentalDB2NativeEnabled() {
		caps = append(caps,
			connector.CapabilityMetadata,
			connector.CapabilityFullRead,
			connector.CapabilityKeysetBoundary,
			connector.CapabilityFullWrite,
			connector.CapabilitySchemaCreate,
			connector.CapabilitySchemaObjects,
			connector.CapabilityPostLoadSchema,
			connector.CapabilityCDCApply,
			connector.CapabilityCDCTransactional,
			connector.CapabilityDDLApply,
			connector.CapabilityPointLookup,
			connector.CapabilityMigrationPrecheck,
		)
		if experimentalDB2LogCDCEnabled() {
			caps = append(caps, connector.CapabilityCDCPosition, connector.CapabilityCDCRead)
			note = "EXPERIMENTAL QMigration native DB2 LUW Full + db2ReadLog CDC enabled; CDC requires the QMigration DB2 Log Agent on a host with IBM Data Server runtime and remains qualification-gated"
		} else {
			note = "EXPERIMENTAL QMigration native DB2 LUW DRDA full data plane enabled; source CDC requires QMIGRATION_EXPERIMENTAL_DB2_LOG_CDC plus a qualified QMigration DB2 Log Agent"
		}
	}
	return connector.Descriptor{Type: t, Protocol: "drda", Native: true, Capabilities: caps, Maturity: connector.MaturityExperimental, QualificationRequired: true, Note: note}
}
func (*Factory) New(ds domain.DataSource) (connector.Connector, error) {
	if ds.Host == "" || ds.Port <= 0 {
		return nil, errors.New("invalid DB2 endpoint")
	}
	return &Connector{ds: ds}, nil
}
func experimentalDB2NativeEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("QMIGRATION_EXPERIMENTAL_DB2_NATIVE"))) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

func experimentalDB2LogCDCEnabled() bool {
	if !experimentalDB2NativeEnabled() {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(os.Getenv("QMIGRATION_EXPERIMENTAL_DB2_LOG_CDC"))) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

type Connector struct {
	ds                  domain.DataSource
	mu                  sync.Mutex
	client              *drdaClient
	inTransaction       bool
	probeVersion        string
	pendingIdentitySync map[string]identitySyncTarget
}

type identitySyncTarget struct {
	schema  string
	table   string
	columns []domain.ColumnInfo
}

func (c *Connector) get(ctx context.Context) (*drdaClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.client != nil {
		return c.client, nil
	}
	p, e := dialDRDA(ctx, c.ds)
	if e != nil {
		return nil, e
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
	e := c.client.close()
	c.client = nil
	c.inTransaction = false
	return e
}
func (c *Connector) TestConnection(ctx context.Context) error {
	if !experimentalDB2NativeEnabled() {
		v, e := probeDRDA(ctx, c.ds)
		if e == nil {
			c.mu.Lock()
			c.probeVersion = v
			c.mu.Unlock()
		}
		return e
	}
	p, e := c.get(ctx)
	if e != nil {
		return e
	}
	rows, e := p.query(ctx, "VALUES VARCHAR(1)")
	if e != nil {
		return e
	}
	if len(rows) != 1 || len(rows[0]) != 1 || rows[0][0].Null || strings.TrimSpace(string(rows[0][0].Data)) != "1" {
		return errors.New("DB2 native VALUES 1 probe returned unexpected result")
	}
	return nil
}
func (c *Connector) GetVersion(ctx context.Context) (string, error) {
	if !experimentalDB2NativeEnabled() {
		c.mu.Lock()
		v := c.probeVersion
		c.mu.Unlock()
		if v != "" {
			return v, nil
		}
		return probeDRDA(ctx, c.ds)
	}
	p, e := c.get(ctx)
	if e != nil {
		return "", e
	}
	rows, e := p.query(ctx, "SELECT VARCHAR(SERVICE_LEVEL) FROM TABLE(SYSPROC.ENV_GET_INST_INFO()) AS T FETCH FIRST 1 ROW ONLY")
	if e == nil && len(rows) > 0 && len(rows[0]) > 0 && !rows[0][0].Null {
		return strings.TrimSpace(string(rows[0][0].Data)), nil
	}
	return "db2-drda", e
}

func qIdent(s string) string            { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
func qName(schema, table string) string { return qIdent(schema) + "." + qIdent(table) }
func qStr(s string) string              { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
func cellString(row []drdaCell, i int) string {
	if i < 0 || i >= len(row) || row[i].Null {
		return ""
	}
	return strings.TrimSpace(string(row[i].Data))
}
func cellInt64(row []drdaCell, i int) int64 {
	v, _ := strconv.ParseInt(cellString(row, i), 10, 64)
	return v
}
func db2Bool(v string) bool {
	v = strings.ToUpper(strings.TrimSpace(v))
	return v == "Y" || v == "YES" || v == "1" || v == "TRUE"
}

func (c *Connector) ListSchemas(ctx context.Context) ([]domain.SchemaInfo, error) {
	p, e := c.get(ctx)
	if e != nil {
		return nil, e
	}
	rows, e := p.query(ctx, `SELECT SCHEMANAME FROM SYSCAT.SCHEMATA WHERE SCHEMANAME NOT LIKE 'SYS%' AND SCHEMANAME NOT IN ('NULLID','SQLJ') ORDER BY SCHEMANAME`)
	if e != nil {
		return nil, e
	}
	out := make([]domain.SchemaInfo, 0, len(rows))
	for _, r := range rows {
		if len(r) > 0 && !r[0].Null {
			out = append(out, domain.SchemaInfo{Name: strings.TrimSpace(string(r[0].Data))})
		}
	}
	return out, nil
}
func (c *Connector) ListTables(ctx context.Context, schema string) ([]domain.TableInfo, error) {
	p, e := c.get(ctx)
	if e != nil {
		return nil, e
	}
	sql := `SELECT TABNAME, VARCHAR(CARD), VARCHAR(COALESCE(NPAGES,0)) FROM SYSCAT.TABLES WHERE TYPE='T' AND TABSCHEMA=` + qStr(schema) + ` ORDER BY TABNAME`
	rows, e := p.query(ctx, sql)
	if e != nil {
		return nil, e
	}
	out := make([]domain.TableInfo, 0, len(rows))
	for _, r := range rows {
		if len(r) < 3 {
			continue
		}
		card := cellInt64(r, 1)
		if card < 0 {
			card = 0
		}
		out = append(out, domain.TableInfo{Schema: schema, Name: cellString(r, 0), Rows: card, DataLength: 0})
	}
	return out, nil
}
func db2ColumnType(typ string, length, scale int) string {
	t := strings.ToUpper(strings.TrimSpace(typ))
	if strings.HasPrefix(t, "VECTOR") {
		if length > 0 {
			return fmt.Sprintf("VECTOR(%d)", length)
		}
		return "VECTOR"
	}
	switch t {
	case "VARCHAR", "CHAR", "VARGRAPHIC", "GRAPHIC", "VARBINARY", "BINARY":
		if length > 0 {
			return fmt.Sprintf("%s(%d)", t, length)
		}
	case "DECIMAL", "NUMERIC":
		if length > 0 {
			return fmt.Sprintf("%s(%d,%d)", t, length, scale)
		}
	case "CLOB", "BLOB", "DBCLOB":
		if length > 0 {
			return fmt.Sprintf("%s(%d)", t, length)
		}
	}
	return t
}

func db2BaseTypeName(typ string) string {
	t := strings.ToUpper(strings.TrimSpace(typ))
	if strings.HasPrefix(t, "VECTOR") {
		return "vector"
	}
	return strings.ToLower(t)
}

type db2VectorSpec struct {
	dimension  int
	coordinate string
}

func (v db2VectorSpec) typeSQL() string {
	return fmt.Sprintf("VECTOR(%d,%s)", v.dimension, v.coordinate)
}

func db2VectorSpecForColumn(col domain.ColumnInfo) (db2VectorSpec, error) {
	var out db2VectorSpec
	if !strings.EqualFold(strings.TrimSpace(col.DataType), "vector") && !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(col.ColumnType)), "VECTOR") {
		return out, fmt.Errorf("column %s is not VECTOR", col.Name)
	}
	ct := strings.ToUpper(strings.TrimSpace(col.ColumnType))
	i := strings.IndexByte(ct, '(')
	j := strings.LastIndexByte(ct, ')')
	if i < 0 || j <= i {
		return out, fmt.Errorf("DB2 VECTOR column %s is missing dimension/coordinate metadata in %q", col.Name, col.ColumnType)
	}
	parts := strings.Split(ct[i+1:j], ",")
	if len(parts) != 2 {
		return out, fmt.Errorf("DB2 VECTOR column %s type %q must be VECTOR(dimension,coordinate-type)", col.Name, col.ColumnType)
	}
	dim, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil || dim <= 0 {
		return out, fmt.Errorf("DB2 VECTOR column %s has invalid dimension in %q", col.Name, col.ColumnType)
	}
	coord := strings.ToUpper(strings.TrimSpace(parts[1]))
	if coord == "REAL" {
		coord = "FLOAT32"
	}
	switch coord {
	case "INT8":
		if dim > 32672 {
			return out, fmt.Errorf("DB2 VECTOR column %s INT8 dimension %d exceeds 32672", col.Name, dim)
		}
	case "FLOAT32":
		if dim > 8168 {
			return out, fmt.Errorf("DB2 VECTOR column %s FLOAT32 dimension %d exceeds 8168", col.Name, dim)
		}
	default:
		return out, fmt.Errorf("DB2 VECTOR column %s has unsupported coordinate type %q", col.Name, coord)
	}
	return db2VectorSpec{dimension: dim, coordinate: coord}, nil
}

func validateDB2VectorString(raw []byte, spec db2VectorSpec) error {
	s := strings.TrimSpace(string(raw))
	if len(s) < 2 || s[0] != '[' || s[len(s)-1] != ']' {
		return errors.New("DB2 VECTOR value must be bracket-delimited")
	}
	body := strings.TrimSpace(s[1 : len(s)-1])
	if body == "" {
		return errors.New("DB2 VECTOR value has no coordinates")
	}
	parts := strings.Split(body, ",")
	if len(parts) != spec.dimension {
		return fmt.Errorf("DB2 VECTOR value has %d coordinates; target dimension is %d", len(parts), spec.dimension)
	}
	for i, part := range parts {
		v := strings.TrimSpace(part)
		if v == "" {
			return fmt.Errorf("DB2 VECTOR coordinate %d is empty", i+1)
		}
		switch spec.coordinate {
		case "INT8":
			n, err := strconv.ParseInt(v, 10, 8)
			if err != nil || n < -128 || n > 127 {
				return fmt.Errorf("DB2 VECTOR INT8 coordinate %d value %q is invalid", i+1, v)
			}
		case "FLOAT32":
			f, err := strconv.ParseFloat(v, 32)
			if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
				return fmt.Errorf("DB2 VECTOR FLOAT32 coordinate %d value %q is invalid", i+1, v)
			}
		}
	}
	return nil
}
func db2NumericType(t string) bool {
	switch strings.ToUpper(strings.TrimSpace(t)) {
	case "SMALLINT", "INTEGER", "INT", "BIGINT", "DECIMAL", "NUMERIC", "REAL", "DOUBLE", "FLOAT", "DECFLOAT":
		return true
	}
	return false
}
func (c *Connector) GetTableMetadata(ctx context.Context, schema, table string) (*domain.TableMetadata, error) {
	p, e := c.get(ctx)
	if e != nil {
		return nil, e
	}
	colsSQL := `SELECT C.COLNAME,C.TYPENAME,VARCHAR(C.LENGTH),VARCHAR(C.SCALE),C.NULLS,C.IDENTITY,C.GENERATED,VARCHAR(C.COLNO),COALESCE(C.DEFAULT,''),COALESCE(VARCHAR(I.START),''),COALESCE(VARCHAR(I.INCREMENT),'') FROM SYSCAT.COLUMNS C LEFT JOIN SYSCAT.COLIDENTATTRIBUTES I ON I.TABSCHEMA=C.TABSCHEMA AND I.TABNAME=C.TABNAME AND I.COLNAME=C.COLNAME WHERE C.TABSCHEMA=` + qStr(schema) + ` AND C.TABNAME=` + qStr(table) + ` ORDER BY C.COLNO`
	rows, e := p.query(ctx, colsSQL)
	if e != nil {
		return nil, e
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("DB2 table %s.%s not found", schema, table)
	}
	m := &domain.TableMetadata{Schema: schema, Name: table}
	for _, r := range rows {
		if len(r) < 11 {
			continue
		}
		ln := int(cellInt64(r, 2))
		sc := int(cellInt64(r, 3))
		typ := cellString(r, 1)
		extra := ""
		isIdentity := db2Bool(cellString(r, 5))
		gen := strings.ToUpper(strings.TrimSpace(cellString(r, 6)))
		if isIdentity {
			seed, err := db2IdentityInteger(cellString(r, 9), "identity seed")
			if err != nil {
				return nil, fmt.Errorf("DB2 %s.%s column %s: %w", schema, table, cellString(r, 0), err)
			}
			increment, err := db2IdentityInteger(cellString(r, 10), "identity increment")
			if err != nil {
				return nil, fmt.Errorf("DB2 %s.%s column %s: %w", schema, table, cellString(r, 0), err)
			}
			mode := "BY_DEFAULT"
			if gen == "A" {
				mode = "ALWAYS"
			}
			extra = "IDENTITY_" + mode + "(" + seed + "," + increment + ")"
		} else if gen != "" && gen != "N" {
			extra = "GENERATED_" + gen
		}
		m.Columns = append(m.Columns, domain.ColumnInfo{Name: cellString(r, 0), DataType: db2BaseTypeName(typ), ColumnType: db2ColumnType(typ, ln, sc), Nullable: db2Bool(cellString(r, 4)), Extra: extra, Ordinal: int(cellInt64(r, 7)) + 1})
	}
	var vectorCols []int
	for i := range m.Columns {
		if strings.EqualFold(m.Columns[i].DataType, "vector") {
			vectorCols = append(vectorCols, i)
		}
	}
	if len(vectorCols) > 0 {
		// COORDINATETYPE was added to SYSCAT.COLUMNS with VECTOR support in
		// Db2 12.1.2. Query it only when a VECTOR column was actually seen so
		// Db2 11.5 metadata discovery never depends on the newer catalog field.
		vr, err := p.query(ctx, `SELECT C.COLNAME,COALESCE(C.COORDINATETYPE,'') FROM SYSCAT.COLUMNS C WHERE C.TABSCHEMA=`+qStr(schema)+` AND C.TABNAME=`+qStr(table)+` AND C.TYPENAME LIKE 'VECTOR%' ORDER BY C.COLNO`)
		if err != nil {
			return nil, fmt.Errorf("DB2 VECTOR metadata for %s.%s requires SYSCAT.COLUMNS.COORDINATETYPE (Db2 12.1.2+ catalog): %w", schema, table, err)
		}
		coords := map[string]string{}
		for _, r := range vr {
			if len(r) >= 2 {
				coords[strings.ToUpper(cellString(r, 0))] = strings.ToUpper(strings.TrimSpace(cellString(r, 1)))
			}
		}
		for _, i := range vectorCols {
			coord := coords[strings.ToUpper(m.Columns[i].Name)]
			if coord == "" {
				return nil, fmt.Errorf("DB2 VECTOR column %s.%s.%s has no COORDINATETYPE; run db2updv121 when upgrading an older database", schema, table, m.Columns[i].Name)
			}
			dim := extractTypeLength(m.Columns[i].ColumnType)
			m.Columns[i].ColumnType = fmt.Sprintf("VECTOR(%d,%s)", dim, coord)
			spec, err := db2VectorSpecForColumn(m.Columns[i])
			if err != nil {
				return nil, err
			}
			m.Columns[i].ColumnType = spec.typeSQL()
		}
	}
	pkSQL := `SELECT K.COLNAME,VARCHAR(K.COLSEQ) FROM SYSCAT.TABCONST C JOIN SYSCAT.KEYCOLUSE K ON K.TABSCHEMA=C.TABSCHEMA AND K.TABNAME=C.TABNAME AND K.CONSTNAME=C.CONSTNAME WHERE C.TYPE='P' AND C.TABSCHEMA=` + qStr(schema) + ` AND C.TABNAME=` + qStr(table) + ` ORDER BY K.COLSEQ`
	pkRows, e := p.query(ctx, pkSQL)
	if e == nil {
		for _, r := range pkRows {
			if len(r) > 0 {
				m.PrimaryKeys = append(m.PrimaryKeys, cellString(r, 0))
			}
		}
	}
	pkSet := map[string]bool{}
	for _, k := range m.PrimaryKeys {
		pkSet[strings.ToUpper(k)] = true
	}
	for i := range m.Columns {
		m.Columns[i].PrimaryKey = pkSet[strings.ToUpper(m.Columns[i].Name)]
	}
	if len(m.PrimaryKeys) == 1 {
		m.PrimaryKey = m.PrimaryKeys[0]
		for _, col := range m.Columns {
			if strings.EqualFold(col.Name, m.PrimaryKey) {
				m.PrimaryKeyType = col.ColumnType
				m.PrimaryKeyNumeric = db2NumericType(col.DataType)
			}
		}
	}
	idxSQL := `SELECT I.INDNAME,I.UNIQUERULE,K.COLNAME,VARCHAR(K.COLSEQ) FROM SYSCAT.INDEXES I JOIN SYSCAT.INDEXCOLUSE K ON K.INDSCHEMA=I.INDSCHEMA AND K.INDNAME=I.INDNAME WHERE I.TABSCHEMA=` + qStr(schema) + ` AND I.TABNAME=` + qStr(table) + ` AND K.COLORDER<>'I' ORDER BY I.INDNAME,K.COLSEQ`
	ir, e := p.query(ctx, idxSQL)
	if e == nil {
		idxMap := map[string]*domain.IndexInfo{}
		var order []string
		for _, r := range ir {
			if len(r) < 4 {
				continue
			}
			name := cellString(r, 0)
			x := idxMap[name]
			if x == nil {
				x = &domain.IndexInfo{Name: name, Unique: cellString(r, 1) != "D", Primary: cellString(r, 1) == "P"}
				idxMap[name] = x
				order = append(order, name)
			}
			x.Columns = append(x.Columns, cellString(r, 2))
		}
		for _, n := range order {
			m.Indexes = append(m.Indexes, *idxMap[n])
		}
	}
	fkSQL := `SELECT R.CONSTNAME,R.REFTABSCHEMA,R.REFTABNAME,FK.COLNAME,VARCHAR(FK.COLSEQ),PK.COLNAME FROM SYSCAT.REFERENCES R JOIN SYSCAT.KEYCOLUSE FK ON FK.TABSCHEMA=R.TABSCHEMA AND FK.TABNAME=R.TABNAME AND FK.CONSTNAME=R.CONSTNAME JOIN SYSCAT.KEYCOLUSE PK ON PK.TABSCHEMA=R.REFTABSCHEMA AND PK.TABNAME=R.REFTABNAME AND PK.CONSTNAME=R.REFKEYNAME AND PK.COLSEQ=FK.COLSEQ WHERE R.TABSCHEMA=` + qStr(schema) + ` AND R.TABNAME=` + qStr(table) + ` ORDER BY R.CONSTNAME,FK.COLSEQ`
	fr, e := p.query(ctx, fkSQL)
	if e == nil {
		fm := map[string]*domain.ForeignKeyInfo{}
		var order []string
		for _, r := range fr {
			if len(r) < 6 {
				continue
			}
			name := cellString(r, 0)
			f := fm[name]
			if f == nil {
				f = &domain.ForeignKeyInfo{Name: name, ReferencedSchema: cellString(r, 1), ReferencedTable: cellString(r, 2)}
				fm[name] = f
				order = append(order, name)
			}
			f.Columns = append(f.Columns, cellString(r, 3))
			f.ReferencedColumns = append(f.ReferencedColumns, cellString(r, 5))
		}
		for _, n := range order {
			m.ForeignKeys = append(m.ForeignKeys, *fm[n])
		}
	}
	statSQL := `SELECT VARCHAR(CASE WHEN CARD<0 THEN 0 ELSE CARD END) FROM SYSCAT.TABLES WHERE TABSCHEMA=` + qStr(schema) + ` AND TABNAME=` + qStr(table)
	sr, e := p.query(ctx, statSQL)
	if e == nil && len(sr) > 0 {
		m.EstimatedRows = cellInt64(sr[0], 0)
		m.HasRows = m.EstimatedRows > 0
	}
	if m.PrimaryKey != "" && m.PrimaryKeyNumeric {
		mm, qe := p.query(ctx, "SELECT VARCHAR(MIN("+qIdent(m.PrimaryKey)+")),VARCHAR(MAX("+qIdent(m.PrimaryKey)+")) FROM "+qName(schema, table))
		if qe == nil && len(mm) > 0 && len(mm[0]) >= 2 {
			if !mm[0][0].Null {
				m.MinPK = cellInt64(mm[0], 0)
			}
			if !mm[0][1].Null {
				m.MaxPK = cellInt64(mm[0], 1)
			}
		}
	}
	return m, nil
}

func findColumn(cols []domain.ColumnInfo, name string) (domain.ColumnInfo, bool) {
	for _, c := range cols {
		if strings.EqualFold(c.Name, name) {
			return c, true
		}
	}
	return domain.ColumnInfo{}, false
}
func isBinaryColumn(c domain.ColumnInfo) bool {
	t := strings.ToLower(c.DataType + " " + c.ColumnType)
	return strings.Contains(t, "binary") || strings.Contains(t, "blob") || strings.Contains(t, "bytea") || strings.Contains(t, "raw") || strings.Contains(t, "image")
}
func isNumericColumn(c domain.ColumnInfo) bool {
	t := strings.ToLower(c.DataType)
	switch t {
	case "tinyint", "smallint", "integer", "int", "bigint", "decimal", "numeric", "real", "double", "float", "decfloat", "money", "smallmoney", "number":
		return true
	}
	return false
}
func db2ValueLiteral(v connector.Value, col domain.ColumnInfo) (string, error) {
	if v.Null {
		return "NULL", nil
	}
	if strings.EqualFold(strings.TrimSpace(col.DataType), "vector") {
		spec, err := db2VectorSpecForColumn(col)
		if err != nil {
			return "", err
		}
		if err := validateDB2VectorString(v.Raw, spec); err != nil {
			return "", fmt.Errorf("DB2 VECTOR column %s: %w", col.Name, err)
		}
		return fmt.Sprintf("VECTOR(%s,%d,%s)", qStr(strings.TrimSpace(string(v.Raw))), spec.dimension, spec.coordinate), nil
	}
	if isBinaryColumn(col) {
		return "X'" + hex.EncodeToString(v.Raw) + "'", nil
	}
	if isNumericColumn(col) {
		if e := connector.ValidateNumericLiteral(v.Raw, true); e != nil {
			return "", e
		}
		return strings.TrimSpace(string(v.Raw)), nil
	}
	if strings.EqualFold(col.DataType, "boolean") || strings.EqualFold(col.DataType, "bool") {
		s := strings.ToLower(strings.TrimSpace(string(v.Raw)))
		if s == "1" || s == "true" || s == "t" {
			return "TRUE", nil
		}
		if s == "0" || s == "false" || s == "f" {
			return "FALSE", nil
		}
		return "", fmt.Errorf("invalid DB2 boolean %q", s)
	}
	return qStr(string(v.Raw)), nil
}
func lexicographicPredicate(keys []string, cols []domain.ColumnInfo, vals []connector.Value, op string) (string, error) {
	if len(keys) == 0 || len(keys) != len(vals) {
		return "", errors.New("DB2 keyset key/value mismatch")
	}
	var arms []string
	for i := range keys {
		var and []string
		for j := 0; j < i; j++ {
			col, ok := findColumn(cols, keys[j])
			if !ok {
				return "", fmt.Errorf("DB2 key column %s metadata missing", keys[j])
			}
			lit, e := db2ValueLiteral(vals[j], col)
			if e != nil {
				return "", e
			}
			and = append(and, qIdent(keys[j])+"="+lit)
		}
		col, ok := findColumn(cols, keys[i])
		if !ok {
			return "", fmt.Errorf("DB2 key column %s metadata missing", keys[i])
		}
		lit, e := db2ValueLiteral(vals[i], col)
		if e != nil {
			return "", e
		}
		and = append(and, qIdent(keys[i])+op+lit)
		arms = append(arms, "("+strings.Join(and, " AND ")+")")
	}
	return "(" + strings.Join(arms, " OR ") + ")", nil
}
func db2KeyColumns(keys []string, columns []domain.ColumnInfo) ([]domain.ColumnInfo, error) {
	out := make([]domain.ColumnInfo, len(keys))
	for i, key := range keys {
		col, ok := findColumn(columns, key)
		if !ok {
			return nil, fmt.Errorf("DB2 migration key column %s is not present in table metadata", key)
		}
		out[i] = col
	}
	return out, nil
}

func (c *Connector) PlanKeysetBoundaries(ctx context.Context, req connector.KeysetBoundaryRequest) ([][]connector.Value, error) {
	if req.Partitions <= 1 {
		return nil, nil
	}
	if req.Partitions > 1024 {
		return nil, errors.New("DB2 keyset partition count exceeds 1024")
	}
	if len(req.Keys) == 0 {
		return nil, errors.New("DB2 keyset boundary planning requires migration key columns")
	}
	keyCols, err := db2KeyColumns(req.Keys, req.Columns)
	if err != nil {
		return nil, err
	}
	for _, b := range [][]connector.Value{req.LowerBound, req.UpperBound} {
		if len(b) > 0 && len(b) != len(req.Keys) {
			return nil, fmt.Errorf("DB2 keyset boundary has %d values for %d keys", len(b), len(req.Keys))
		}
	}
	var conditions []string
	if len(req.LowerBound) > 0 {
		q, e := lexicographicPredicate(req.Keys, keyCols, req.LowerBound, ">=")
		if e != nil {
			return nil, e
		}
		conditions = append(conditions, q)
	}
	if len(req.UpperBound) > 0 {
		q, e := lexicographicPredicate(req.Keys, keyCols, req.UpperBound, "<")
		if e != nil {
			return nil, e
		}
		conditions = append(conditions, q)
	}
	where := ""
	if len(conditions) > 0 {
		where = " WHERE " + strings.Join(conditions, " AND ")
	}
	keys := make([]string, len(req.Keys))
	selects := make([]string, len(req.Keys))
	for i, key := range req.Keys {
		keys[i] = qIdent(key)
		selects[i] = db2SelectExpr(keyCols[i])
	}
	q := "WITH qm_ranked AS (SELECT " + strings.Join(keys, ",") + ",NTILE(" + strconv.Itoa(req.Partitions) + ") OVER (ORDER BY " + strings.Join(keys, ",") + ") AS qm_bucket FROM " + qName(req.Schema, req.Table) + where + "), " +
		"qm_bounds AS (SELECT " + strings.Join(keys, ",") + ",qm_bucket,ROW_NUMBER() OVER (PARTITION BY qm_bucket ORDER BY " + strings.Join(keys, ",") + ") AS qm_rn FROM qm_ranked) " +
		"SELECT " + strings.Join(selects, ",") + " FROM qm_bounds WHERE qm_bucket>1 AND qm_rn=1 ORDER BY qm_bucket"
	p, err := c.get(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := p.query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("ordered DB2 keyset boundary query: %w", err)
	}
	out := make([][]connector.Value, 0, len(rows))
	for _, row := range rows {
		if len(row) != len(req.Keys) {
			return nil, fmt.Errorf("DB2 boundary query returned %d columns for %d keys", len(row), len(req.Keys))
		}
		b := make([]connector.Value, len(row))
		for i, cell := range row {
			if cell.Null {
				return nil, fmt.Errorf("DB2 migration key %s returned NULL boundary", req.Keys[i])
			}
			b[i] = connector.Value{Raw: append([]byte(nil), cell.Data...)}
		}
		out = append(out, b)
	}
	return out, nil
}

func db2SelectExpr(col domain.ColumnInfo) string {
	t := strings.ToLower(col.DataType)
	if t == "decfloat" {
		return "VARCHAR(" + qIdent(col.Name) + ")"
	}
	if t == "xml" {
		return "XMLSERIALIZE(" + qIdent(col.Name) + " AS CLOB(2G))"
	}
	if t == "vector" {
		// VECTOR uses an out-of-row representation in the Db2 log. Keep Full
		// and CDC values on the same stable serialized string contract.
		return "VECTOR_SERIALIZE(" + qIdent(col.Name) + ")"
	}
	return qIdent(col.Name)
}
func (c *Connector) ReadBatch(ctx context.Context, req connector.ReadBatchRequest) (*connector.RowBatch, error) {
	if req.Limit <= 0 {
		req.Limit = 1000
	}
	if req.Limit > 10000 {
		req.Limit = 10000
	}
	p, e := c.get(ctx)
	if e != nil {
		return nil, e
	}
	selects := make([]string, 0, len(req.Columns))
	for _, col := range req.Columns {
		selects = append(selects, db2SelectExpr(col))
	}
	if len(selects) == 0 {
		return nil, errors.New("DB2 ReadBatch requires columns")
	}
	keys := req.PrimaryKeys
	if len(keys) == 0 && req.PrimaryKey != "" {
		keys = []string{req.PrimaryKey}
	}
	var where []string
	if strings.TrimSpace(req.CustomWhere) != "" {
		where = append(where, "("+req.CustomWhere+")")
	}
	if req.UseKeyset && len(req.Cursor) > 0 {
		q, e := lexicographicPredicate(keys, req.Columns, req.Cursor, ">")
		if e != nil {
			return nil, e
		}
		where = append(where, q)
	}
	if len(req.LowerBound) > 0 {
		q, e := lexicographicPredicate(keys, req.Columns, req.LowerBound, ">=")
		if e != nil {
			return nil, e
		}
		where = append(where, q)
	}
	if len(req.UpperBound) > 0 {
		q, e := lexicographicPredicate(keys, req.Columns, req.UpperBound, "<")
		if e != nil {
			return nil, e
		}
		where = append(where, q)
	}
	if req.HasAfter && req.PrimaryKey != "" {
		col, ok := findColumn(req.Columns, req.PrimaryKey)
		if !ok {
			return nil, fmt.Errorf("DB2 primary key %s metadata missing", req.PrimaryKey)
		}
		lit, e := db2ValueLiteral(connector.Value{Raw: []byte(strconv.FormatInt(req.AfterPK, 10))}, col)
		if e != nil {
			return nil, e
		}
		where = append(where, qIdent(req.PrimaryKey)+">"+lit)
	}
	if req.StartPK != 0 && req.PrimaryKey != "" {
		where = append(where, qIdent(req.PrimaryKey)+">="+strconv.FormatInt(req.StartPK, 10))
	}
	if req.EndPK != 0 && req.PrimaryKey != "" {
		where = append(where, qIdent(req.PrimaryKey)+"<"+strconv.FormatInt(req.EndPK, 10))
	}
	sql := "SELECT " + strings.Join(selects, ",") + " FROM " + qName(req.Schema, req.Table)
	if len(where) > 0 {
		sql += " WHERE " + strings.Join(where, " AND ")
	}
	if len(keys) > 0 {
		ord := make([]string, len(keys))
		for i, k := range keys {
			ord[i] = qIdent(k)
		}
		sql += " ORDER BY " + strings.Join(ord, ",")
	}
	sql += fmt.Sprintf(" FETCH FIRST %d ROWS ONLY", req.Limit)
	rows, e := p.query(ctx, sql)
	if e != nil {
		return nil, e
	}
	out := &connector.RowBatch{}
	for _, r := range rows {
		if len(r) != len(req.Columns) {
			return nil, fmt.Errorf("DB2 row width %d does not match %d columns", len(r), len(req.Columns))
		}
		row := make([]connector.Value, len(r))
		for i, cell := range r {
			row[i] = connector.Value{Null: cell.Null, Raw: append([]byte{}, cell.Data...)}
			if !cell.Null {
				out.Bytes += int64(len(cell.Data))
			}
		}
		out.Rows = append(out.Rows, row)
	}
	if len(out.Rows) > 0 && len(keys) > 0 {
		last := out.Rows[len(out.Rows)-1]
		for _, k := range keys {
			for i, col := range req.Columns {
				if strings.EqualFold(col.Name, k) {
					out.LastKey = append(out.LastKey, last[i])
					break
				}
			}
		}
		if len(keys) == 1 {
			for i, col := range req.Columns {
				if strings.EqualFold(col.Name, keys[0]) {
					out.LastPK, _ = strconv.ParseInt(strings.TrimSpace(string(last[i].Raw)), 10, 64)
					break
				}
			}
		}
	}
	return out, nil
}

type db2IdentitySpec struct {
	enabled   bool
	always    bool
	seed      string
	increment string
}

func db2IdentityInteger(raw, label string) (string, error) {
	v := strings.TrimSpace(raw)
	if v == "" {
		v = "1"
	}
	n := new(big.Int)
	if _, ok := n.SetString(v, 10); !ok {
		return "", fmt.Errorf("%s %q is not an integer", label, v)
	}
	return n.String(), nil
}

func db2IdentitySpecForColumn(col domain.ColumnInfo) (db2IdentitySpec, error) {
	extra := strings.ToUpper(strings.TrimSpace(col.Extra))
	if !strings.Contains(extra, "IDENTITY") && !strings.Contains(strings.ToLower(col.Extra), "auto_increment") {
		return db2IdentitySpec{}, nil
	}
	spec := db2IdentitySpec{enabled: true, seed: "1", increment: "1"}
	prefix := ""
	switch {
	case strings.Contains(extra, "IDENTITY_ALWAYS("):
		spec.always = true
		prefix = "IDENTITY_ALWAYS("
	case strings.Contains(extra, "IDENTITY_BY_DEFAULT("):
		prefix = "IDENTITY_BY_DEFAULT("
	case strings.Contains(extra, "IDENTITY("):
		prefix = "IDENTITY("
	case strings.Contains(extra, "IDENTITY") && strings.Contains(extra, "GENERATED_A"):
		spec.always = true
	}
	if prefix != "" {
		start := strings.Index(extra, prefix)
		rest := extra[start+len(prefix):]
		end := strings.IndexByte(rest, ')')
		if end < 0 {
			return db2IdentitySpec{}, errors.New("DB2 identity metadata is missing closing parenthesis")
		}
		parts := strings.Split(rest[:end], ",")
		if len(parts) != 2 {
			return db2IdentitySpec{}, errors.New("DB2 identity metadata must contain seed and increment")
		}
		spec.seed = strings.TrimSpace(parts[0])
		spec.increment = strings.TrimSpace(parts[1])
	}
	var err error
	if spec.seed, err = db2IdentityInteger(spec.seed, "identity seed"); err != nil {
		return db2IdentitySpec{}, err
	}
	if spec.increment, err = db2IdentityInteger(spec.increment, "identity increment"); err != nil {
		return db2IdentitySpec{}, err
	}
	inc, _ := new(big.Int).SetString(spec.increment, 10)
	if inc.Sign() == 0 {
		return db2IdentitySpec{}, errors.New("DB2 identity increment cannot be zero")
	}
	return spec, nil
}

func db2GeneratedExpressionColumn(col domain.ColumnInfo) bool {
	extra := strings.ToUpper(strings.TrimSpace(col.Extra))
	return strings.Contains(extra, "GENERATED_") && !strings.Contains(extra, "IDENTITY")
}

func db2WritableRow(cols []domain.ColumnInfo, row []connector.Value, pks []string) ([]domain.ColumnInfo, []connector.Value, error) {
	if len(cols) != len(row) {
		return nil, nil, errors.New("DB2 writer column/value width mismatch")
	}
	pkSet := map[string]bool{}
	for _, pk := range pks {
		pkSet[strings.ToUpper(pk)] = true
	}
	outCols := make([]domain.ColumnInfo, 0, len(cols))
	outRow := make([]connector.Value, 0, len(row))
	for i, col := range cols {
		if db2GeneratedExpressionColumn(col) {
			if pkSet[strings.ToUpper(col.Name)] {
				return nil, nil, fmt.Errorf("DB2 generated expression column %s cannot be used as a writable migration key", col.Name)
			}
			continue
		}
		outCols = append(outCols, col)
		outRow = append(outRow, row[i])
	}
	if len(outCols) == 0 {
		return nil, nil, errors.New("DB2 writer has no writable columns after excluding generated expressions")
	}
	return outCols, outRow, nil
}

func db2HasIdentity(cols []domain.ColumnInfo) bool {
	for _, col := range cols {
		if spec, err := db2IdentitySpecForColumn(col); err == nil && spec.enabled {
			return true
		}
	}
	return false
}

func db2TargetType(c domain.ColumnInfo) string {
	t := strings.ToLower(strings.TrimSpace(c.DataType))
	ct := strings.ToLower(c.ColumnType)
	if t == "vector" {
		if spec, err := db2VectorSpecForColumn(c); err == nil {
			return spec.typeSQL()
		}
		return "VECTOR"
	}
	switch t {
	case "tinyint":
		return "SMALLINT"
	case "smallint":
		return "SMALLINT"
	case "int", "integer", "mediumint":
		return "INTEGER"
	case "bigint":
		return "BIGINT"
	case "decimal", "numeric", "number":
		if i := strings.Index(c.ColumnType, "("); i >= 0 {
			return "DECIMAL" + c.ColumnType[i:]
		}
		return "DECIMAL(31,10)"
	case "real":
		return "REAL"
	case "float", "double", "double precision":
		return "DOUBLE"
	case "boolean", "bool", "bit":
		return "BOOLEAN"
	case "date":
		return "DATE"
	case "time":
		return "TIME"
	case "timestamp", "datetime", "datetime2", "smalldatetime", "datetimeoffset":
		return "TIMESTAMP"
	case "blob", "bytea", "image", "raw", "long raw":
		return "BLOB(2G)"
	case "binary", "varbinary":
		if n := extractTypeLength(ct); n > 0 && n <= 32672 {
			return fmt.Sprintf("VARBINARY(%d)", n)
		}
		return "BLOB(2G)"
	case "clob", "text", "mediumtext", "longtext", "json", "jsonb", "xml":
		return "CLOB(2G)"
	case "char", "nchar":
		if n := extractTypeLength(ct); n > 0 && n <= 255 {
			return fmt.Sprintf("CHAR(%d)", n)
		}
		return "VARCHAR(32672)"
	case "varchar", "nvarchar", "varchar2", "nvarchar2", "string", "uuid":
		if n := extractTypeLength(ct); n > 0 && n <= 32672 {
			return fmt.Sprintf("VARCHAR(%d)", n)
		}
		if t == "uuid" {
			return "VARCHAR(36)"
		}
		return "VARCHAR(32672)"
	}
	if strings.Contains(ct, "blob") || strings.Contains(ct, "binary") {
		return "BLOB(2G)"
	}
	if strings.Contains(ct, "clob") || strings.Contains(ct, "text") {
		return "CLOB(2G)"
	}
	return "VARCHAR(32672)"
}
func extractTypeLength(s string) int {
	i := strings.Index(s, "(")
	if i < 0 {
		return 0
	}
	j := strings.Index(s[i+1:], ")")
	if j < 0 {
		return 0
	}
	part := s[i+1 : i+1+j]
	if k := strings.Index(part, ","); k >= 0 {
		part = part[:k]
	}
	n, _ := strconv.Atoi(strings.TrimSpace(part))
	return n
}
func (c *Connector) EnsureSchema(ctx context.Context, schema string) error {
	p, e := c.get(ctx)
	if e != nil {
		return e
	}
	rows, e := p.query(ctx, "SELECT SCHEMANAME FROM SYSCAT.SCHEMATA WHERE SCHEMANAME="+qStr(schema))
	if e == nil && len(rows) > 0 {
		return nil
	}
	return p.exec(ctx, "CREATE SCHEMA "+qIdent(schema), true)
}
func (c *Connector) CreateTable(ctx context.Context, schema, table string, cols []domain.ColumnInfo, pk string) error {
	var pks []string
	if pk != "" {
		pks = []string{pk}
	}
	return c.CreateTableWithPrimaryKeys(ctx, schema, table, cols, pks)
}
func db2ColumnDefinition(col domain.ColumnInfo) (string, error) {
	if db2GeneratedExpressionColumn(col) {
		return "", fmt.Errorf("DB2 target auto-create refuses generated expression column %s because the source generation expression is not available in ColumnInfo", col.Name)
	}
	spec, err := db2IdentitySpecForColumn(col)
	if err != nil {
		return "", fmt.Errorf("DB2 identity column %s: %w", col.Name, err)
	}
	targetType := db2TargetType(col)
	if strings.EqualFold(strings.TrimSpace(col.DataType), "vector") {
		spec, e := db2VectorSpecForColumn(col)
		if e != nil {
			return "", e
		}
		targetType = spec.typeSQL()
	}
	d := qIdent(col.Name) + " " + targetType
	if !col.Nullable || spec.enabled {
		d += " NOT NULL"
	}
	if spec.enabled {
		// Db2 LUW does not accept explicit INSERT values for GENERATED ALWAYS
		// identity columns. During QMigration data propagation, create the
		// target as BY DEFAULT so Full/CDC can preserve the exact source value.
		// FinalizeGeneratedValueModes restores source ALWAYS semantics at the
		// full-only finish or cutover boundary.
		d += " GENERATED BY DEFAULT AS IDENTITY (START WITH " + spec.seed + ", INCREMENT BY " + spec.increment + ")"
	}
	return d, nil
}

func (c *Connector) CreateTableWithPrimaryKeys(ctx context.Context, schema, table string, cols []domain.ColumnInfo, pks []string) error {
	p, e := c.get(ctx)
	if e != nil {
		return e
	}
	if e = c.EnsureSchema(ctx, schema); e != nil {
		return e
	}
	var defs []string
	for _, col := range cols {
		d, err := db2ColumnDefinition(col)
		if err != nil {
			return err
		}
		defs = append(defs, d)
	}
	if len(pks) > 0 {
		q := make([]string, len(pks))
		for i, k := range pks {
			q[i] = qIdent(k)
		}
		defs = append(defs, "PRIMARY KEY ("+strings.Join(q, ",")+")")
	}
	sql := "CREATE TABLE " + qName(schema, table) + " (" + strings.Join(defs, ",") + ")"
	return p.exec(ctx, sql, true)
}
func (c *Connector) CreateIndex(ctx context.Context, schema, table string, idx domain.IndexInfo) error {
	if idx.Primary {
		return nil
	}
	p, e := c.get(ctx)
	if e != nil {
		return e
	}
	cols := make([]string, len(idx.Columns))
	for i, x := range idx.Columns {
		cols[i] = qIdent(x)
	}
	u := ""
	if idx.Unique {
		u = "UNIQUE "
	}
	return p.exec(ctx, "CREATE "+u+"INDEX "+qIdent(idx.Name)+" ON "+qName(schema, table)+" ("+strings.Join(cols, ",")+")", true)
}
func (c *Connector) CreateForeignKey(ctx context.Context, schema, table string, fk domain.ForeignKeyInfo) error {
	p, e := c.get(ctx)
	if e != nil {
		return e
	}
	cols := make([]string, len(fk.Columns))
	refs := make([]string, len(fk.ReferencedColumns))
	for i, x := range fk.Columns {
		cols[i] = qIdent(x)
	}
	for i, x := range fk.ReferencedColumns {
		refs[i] = qIdent(x)
	}
	return p.exec(ctx, "ALTER TABLE "+qName(schema, table)+" ADD CONSTRAINT "+qIdent(fk.Name)+" FOREIGN KEY ("+strings.Join(cols, ",")+") REFERENCES "+qName(fk.ReferencedSchema, fk.ReferencedTable)+" ("+strings.Join(refs, ",")+")", true)
}
func (c *Connector) ExecDDL(ctx context.Context, schema, ddl string) error {
	p, e := c.get(ctx)
	if e != nil {
		return e
	}
	if schema != "" {
		if e = p.exec(ctx, "SET CURRENT SCHEMA = "+qStr(schema), true); e != nil {
			return e
		}
	}
	return p.exec(ctx, ddl, true)
}

func (c *Connector) ListSchemaObjects(ctx context.Context, schema string) ([]domain.SchemaObject, error) {
	p, e := c.get(ctx)
	if e != nil {
		return nil, e
	}
	var out []domain.SchemaObject
	if rows, e := p.query(ctx, `SELECT VIEWNAME,TEXT FROM SYSCAT.VIEWS WHERE VIEWSCHEMA=`+qStr(schema)+` ORDER BY VIEWNAME`); e == nil {
		for _, r := range rows {
			if len(r) >= 2 {
				name, def := cellString(r, 0), cellString(r, 1)
				ddl := def
				if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(def)), "CREATE ") {
					ddl = "CREATE VIEW " + qName(schema, name) + " AS " + def
				}
				out = append(out, domain.SchemaObject{Schema: schema, Name: name, Type: domain.SchemaObjectView, Definition: def, DDL: ddl, DependenciesKnown: false})
			}
		}
	}
	if rows, e := p.query(ctx, `SELECT SEQNAME,VARCHAR(START),VARCHAR(INCREMENT),VARCHAR(MINVALUE),VARCHAR(MAXVALUE),CYCLE FROM SYSCAT.SEQUENCES WHERE SEQSCHEMA=`+qStr(schema)+` ORDER BY SEQNAME`); e == nil {
		for _, r := range rows {
			if len(r) >= 6 {
				name := cellString(r, 0)
				ddl := "CREATE SEQUENCE " + qName(schema, name) + " START WITH " + cellString(r, 1) + " INCREMENT BY " + cellString(r, 2)
				if cellString(r, 5) == "Y" {
					ddl += " CYCLE"
				}
				out = append(out, domain.SchemaObject{Schema: schema, Name: name, Type: domain.SchemaObjectSequence, DDL: ddl, Definition: ddl, DependenciesKnown: true})
			}
		}
	}
	if rows, e := p.query(ctx, `SELECT TRIGNAME,TABNAME,TEXT FROM SYSCAT.TRIGGERS WHERE TRIGSCHEMA=`+qStr(schema)+` ORDER BY TRIGNAME`); e == nil {
		for _, r := range rows {
			if len(r) >= 3 {
				out = append(out, domain.SchemaObject{Schema: schema, Name: cellString(r, 0), Type: domain.SchemaObjectTrigger, RelatedTo: cellString(r, 1), Definition: cellString(r, 2), DDL: cellString(r, 2), DependenciesKnown: false})
			}
		}
	}
	if rows, e := p.query(ctx, `SELECT ROUTINENAME,ROUTINETYPE,TEXT FROM SYSCAT.ROUTINES WHERE ROUTINESCHEMA=`+qStr(schema)+` AND ROUTINETYPE IN ('F','P') ORDER BY ROUTINENAME`); e == nil {
		for _, r := range rows {
			if len(r) >= 3 {
				typ := domain.SchemaObjectProcedure
				if cellString(r, 1) == "F" {
					typ = domain.SchemaObjectFunction
				}
				out = append(out, domain.SchemaObject{Schema: schema, Name: cellString(r, 0), Type: typ, Definition: cellString(r, 2), DDL: cellString(r, 2), DependenciesKnown: false})
			}
		}
	}
	return out, nil
}

func (c *Connector) WriteBatch(ctx context.Context, req connector.WriteBatchRequest) (int64, error) {
	p, e := c.get(ctx)
	if e != nil {
		return 0, e
	}
	if len(req.Rows) == 0 {
		return 0, nil
	}
	pks := req.PrimaryKeys
	if len(pks) == 0 && req.PrimaryKey != "" {
		pks = []string{req.PrimaryKey}
	}
	var writableCols []domain.ColumnInfo
	writableRows := make([][]connector.Value, 0, len(req.Rows))
	for i, row := range req.Rows {
		if len(row) != len(req.Columns) {
			return int64(i), errors.New("DB2 WriteBatch row width mismatch")
		}
		cols, vals, err := db2WritableRow(req.Columns, row, pks)
		if err != nil {
			return int64(i), err
		}
		if writableCols == nil {
			writableCols = cols
		} else if len(cols) != len(writableCols) {
			return int64(i), errors.New("DB2 writable column shape changed inside one batch")
		}
		writableRows = append(writableRows, vals)
	}
	sql, e := buildDB2PreparedUpsert(req.Schema, req.Table, writableCols, pks)
	if e != nil {
		return 0, e
	}
	ownTx := !c.inTransaction
	if ownTx {
		p.inTransaction = true
	}
	written, e := p.execPreparedBatch(ctx, sql, writableCols, writableRows)
	if e != nil {
		if ownTx {
			_ = p.rollback(ctx)
		}
		return written, e
	}
	if ownTx {
		if e = p.commit(ctx); e != nil {
			return written, e
		}
	} else if db2HasIdentity(req.Columns) {
		c.rememberIdentitySync(req.Schema, req.Table, req.Columns)
	}
	return written, nil
}

func buildDB2PreparedUpsert(schema, table string, cols []domain.ColumnInfo, pks []string) (string, error) {
	if len(cols) == 0 {
		return "", errors.New("DB2 prepared upsert has no columns")
	}
	names := make([]string, len(cols))
	params := make([]string, len(cols))
	alwaysIdentity := map[string]bool{}
	for i, col := range cols {
		names[i] = qIdent(col.Name)
		if strings.EqualFold(strings.TrimSpace(col.DataType), "vector") {
			spec, err := db2VectorSpecForColumn(col)
			if err != nil {
				return "", err
			}
			// Full/CDC carry VECTOR_SERIALIZE() text. Convert the bound string
			// back to the native binary VECTOR using Db2's documented constructor.
			// CAST to CLOB lets large vector strings use the existing EXTDTA path.
			params[i] = fmt.Sprintf("VECTOR(CAST(? AS CLOB),%d,%s)", spec.dimension, spec.coordinate)
		} else {
			params[i] = "CAST(? AS " + db2TargetType(col) + ")"
		}
		spec, err := db2IdentitySpecForColumn(col)
		if err != nil {
			return "", fmt.Errorf("DB2 identity column %s: %w", col.Name, err)
		}
		if spec.enabled && spec.always {
			alwaysIdentity[strings.ToUpper(col.Name)] = true
		}
	}
	if len(pks) == 0 {
		return "INSERT INTO " + qName(schema, table) + " (" + strings.Join(names, ",") + ") VALUES (" + strings.Join(params, ",") + ")", nil
	}
	var on []string
	pkSet := map[string]bool{}
	for _, pk := range pks {
		pkSet[strings.ToUpper(pk)] = true
		if _, ok := findColumn(cols, pk); !ok {
			return "", fmt.Errorf("DB2 prepared upsert key column %s is not writable/present", pk)
		}
		on = append(on, "T."+qIdent(pk)+"=S."+qIdent(pk))
	}
	var updates []string
	for _, col := range cols {
		name := strings.ToUpper(col.Name)
		if pkSet[name] || alwaysIdentity[name] {
			continue
		}
		updates = append(updates, "T."+qIdent(col.Name)+"=S."+qIdent(col.Name))
	}
	sql := "MERGE INTO " + qName(schema, table) + " AS T USING (VALUES (" + strings.Join(params, ",") + ")) AS S(" + strings.Join(names, ",") + ") ON " + strings.Join(on, " AND ")
	if len(updates) > 0 {
		sql += " WHEN MATCHED THEN UPDATE SET " + strings.Join(updates, ",")
	}
	sql += " WHEN NOT MATCHED THEN INSERT (" + strings.Join(names, ",") + ") VALUES ("
	ins := make([]string, len(cols))
	for i, col := range cols {
		ins[i] = "S." + qIdent(col.Name)
	}
	sql += strings.Join(ins, ",") + ")"
	return sql, nil
}

func (c *Connector) DeleteByKey(ctx context.Context, req connector.DeleteByKeyRequest) error {
	p, e := c.get(ctx)
	if e != nil {
		return e
	}
	keys := req.PrimaryKeys
	cols := req.Columns
	vals := req.Values
	if len(keys) == 0 && req.PrimaryKey != "" {
		keys = []string{req.PrimaryKey}
		cols = []domain.ColumnInfo{req.Column}
		vals = []connector.Value{req.Value}
	}
	if len(keys) == 0 || len(keys) != len(vals) {
		return errors.New("DB2 delete key mismatch")
	}
	var where []string
	for i, k := range keys {
		col := cols[i]
		lit, e := db2ValueLiteral(vals[i], col)
		if e != nil {
			return e
		}
		where = append(where, qIdent(k)+"="+lit)
	}
	return p.exec(ctx, "DELETE FROM "+qName(req.Schema, req.Table)+" WHERE "+strings.Join(where, " AND "), !c.inTransaction)
}
func (c *Connector) ReadByKey(ctx context.Context, req connector.ReadByKeyRequest) ([]connector.Value, bool, error) {
	p, e := c.get(ctx)
	if e != nil {
		return nil, false, e
	}
	if len(req.PrimaryKeys) == 0 || len(req.PrimaryKeys) != len(req.KeyValues) {
		return nil, false, errors.New("DB2 point lookup key mismatch")
	}
	var where []string
	for i, k := range req.PrimaryKeys {
		col := req.KeyColumns[i]
		lit, e := db2ValueLiteral(req.KeyValues[i], col)
		if e != nil {
			return nil, false, e
		}
		where = append(where, qIdent(k)+"="+lit)
	}
	sels := make([]string, len(req.Columns))
	for i, col := range req.Columns {
		sels[i] = db2SelectExpr(col)
	}
	rows, e := p.query(ctx, "SELECT "+strings.Join(sels, ",")+" FROM "+qName(req.Schema, req.Table)+" WHERE "+strings.Join(where, " AND ")+" FETCH FIRST 1 ROW ONLY")
	if e != nil {
		return nil, false, e
	}
	if len(rows) == 0 {
		return nil, false, nil
	}
	out := make([]connector.Value, len(rows[0]))
	for i, x := range rows[0] {
		out[i] = connector.Value{Null: x.Null, Raw: append([]byte{}, x.Data...)}
	}
	return out, true, nil
}
func identitySyncKey(schema, table string) string {
	return strings.ToUpper(schema) + "\x00" + strings.ToUpper(table)
}

func (c *Connector) rememberIdentitySync(schema, table string, cols []domain.ColumnInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pendingIdentitySync == nil {
		c.pendingIdentitySync = map[string]identitySyncTarget{}
	}
	c.pendingIdentitySync[identitySyncKey(schema, table)] = identitySyncTarget{schema: schema, table: table, columns: append([]domain.ColumnInfo(nil), cols...)}
}

func (c *Connector) drainIdentitySyncTargets() []identitySyncTarget {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]identitySyncTarget, 0, len(c.pendingIdentitySync))
	for _, target := range c.pendingIdentitySync {
		out = append(out, target)
	}
	c.pendingIdentitySync = nil
	return out
}

// SyncGeneratedValueState advances Db2 identity generators after QMigration has
// copied explicit source identity values. Db2 documents that explicit values do
// not advance the internally generated next value, so leaving this unsynchronised
// can cause a duplicate immediately after cutover.
func (c *Connector) SyncGeneratedValueState(ctx context.Context, schema, table string, cols []domain.ColumnInfo) error {
	p, err := c.get(ctx)
	if err != nil {
		return err
	}
	for _, col := range cols {
		spec, err := db2IdentitySpecForColumn(col)
		if err != nil {
			return fmt.Errorf("DB2 identity column %s: %w", col.Name, err)
		}
		if !spec.enabled {
			continue
		}
		inc, _ := new(big.Int).SetString(spec.increment, 10)
		agg := "MAX"
		if inc.Sign() < 0 {
			agg = "MIN"
		}
		rows, err := p.query(ctx, "SELECT VARCHAR("+agg+"("+qIdent(col.Name)+")) FROM "+qName(schema, table))
		if err != nil {
			return fmt.Errorf("DB2 identity state query %s.%s.%s: %w", schema, table, col.Name, err)
		}
		next, _ := new(big.Int).SetString(spec.seed, 10)
		if len(rows) > 0 && len(rows[0]) > 0 && !rows[0][0].Null && strings.TrimSpace(string(rows[0][0].Data)) != "" {
			current, ok := new(big.Int).SetString(strings.TrimSpace(string(rows[0][0].Data)), 10)
			if !ok {
				return fmt.Errorf("DB2 identity %s.%s.%s returned non-integer current value %q", schema, table, col.Name, string(rows[0][0].Data))
			}
			next = new(big.Int).Add(current, inc)
		}
		if err := p.exec(ctx, "ALTER TABLE "+qName(schema, table)+" ALTER COLUMN "+qIdent(col.Name)+" RESTART WITH "+next.String(), true); err != nil {
			return fmt.Errorf("DB2 identity restart %s.%s.%s to %s: %w", schema, table, col.Name, next.String(), err)
		}
	}
	return nil
}

// FinalizeGeneratedValueModes restores source GENERATED ALWAYS semantics after
// explicit identity propagation is finished. Db2 LUW recommends BY DEFAULT for
// data propagation; ALTER COLUMN SET GENERATED ALWAYS is therefore deferred until
// Full-only completion or the cutover critical section.
func db2FinalizeGeneratedStatements(schema, table string, cols []domain.ColumnInfo) ([]string, error) {
	var out []string
	for _, col := range cols {
		spec, err := db2IdentitySpecForColumn(col)
		if err != nil {
			return nil, fmt.Errorf("DB2 identity column %s: %w", col.Name, err)
		}
		if !spec.enabled || !spec.always {
			continue
		}
		out = append(out, "ALTER TABLE "+qName(schema, table)+" ALTER COLUMN "+qIdent(col.Name)+" SET GENERATED ALWAYS")
	}
	return out, nil
}

func (c *Connector) FinalizeGeneratedValueModes(ctx context.Context, schema, table string, cols []domain.ColumnInfo) error {
	p, err := c.get(ctx)
	if err != nil {
		return err
	}
	statements, err := db2FinalizeGeneratedStatements(schema, table, cols)
	if err != nil {
		return err
	}
	for _, sql := range statements {
		if err := p.exec(ctx, sql, true); err != nil {
			return fmt.Errorf("DB2 restore generated-value mode for %s.%s: %w", schema, table, err)
		}
	}
	return nil
}

func (c *Connector) BeginCDCTransaction(ctx context.Context) error {
	if c.inTransaction {
		return errors.New("DB2 CDC transaction already active")
	}
	p, e := c.get(ctx)
	if e != nil {
		return e
	}
	p.inTransaction = true
	c.inTransaction = true
	return nil
}
func (c *Connector) CommitCDCTransaction(ctx context.Context) error {
	if !c.inTransaction {
		return nil
	}
	p, e := c.get(ctx)
	if e != nil {
		return e
	}
	e = p.commit(ctx)
	if e != nil {
		return e
	}
	c.inTransaction = false
	for _, target := range c.drainIdentitySyncTargets() {
		if e := c.SyncGeneratedValueState(ctx, target.schema, target.table, target.columns); e != nil {
			return e
		}
	}
	return nil
}
func (c *Connector) RollbackCDCTransaction(ctx context.Context) error {
	if !c.inTransaction {
		return nil
	}
	p, e := c.get(ctx)
	if e != nil {
		return e
	}
	e = p.rollback(ctx)
	if e == nil {
		c.inTransaction = false
		c.drainIdentitySyncTargets()
	}
	return e
}

// CDCSelection is the catalog identity needed to associate a propagatable Db2
// Data Manager log record with a QMigration table mapping. TBSPACEID/TABLEID
// are intentionally kept out of the generic Connector SPI because they are
// Db2-internal identifiers.
type CDCSelection struct {
	Schema       string
	Table        string
	TablespaceID uint16
	TableID      uint16
	Columns      []domain.ColumnInfo
	PrimaryKeys  []string
}

func (c *Connector) logAgentClient() (*db2log.Client, error) {
	if !experimentalDB2LogCDCEnabled() {
		return nil, errors.New("DB2 source CDC requires QMIGRATION_EXPERIMENTAL_DB2_NATIVE=1 and QMIGRATION_EXPERIMENTAL_DB2_LOG_CDC=1")
	}
	if strings.TrimSpace(c.ds.CDCURL) == "" {
		return nil, errors.New("DB2 source CDC requires cdc_url pointing to the QMigration DB2 Log Agent")
	}
	ca := os.Getenv("QMIGRATION_DB2_LOG_TLS_CA")
	serverName := os.Getenv("QMIGRATION_DB2_LOG_TLS_SERVER_NAME")
	return db2log.NewClient(c.ds.CDCURL, ca, serverName, os.Getenv("QMIGRATION_DB2_LOG_TOKEN"))
}

func (c *Connector) CurrentCDCPosition(ctx context.Context) (*domain.CDCPosition, error) {
	a, err := c.logAgentClient()
	if err != nil {
		return nil, err
	}
	p, err := a.Position(ctx)
	if err != nil {
		return nil, err
	}
	if !p.Recoverable {
		return nil, errors.New("DB2 database is not recoverable; db2ReadLog CDC requires archive logging/recoverability")
	}
	lri := p.NextStartLRI
	if lri.IsZero() {
		lri = p.CurrentEndLRI
	}
	if lri.IsZero() {
		return nil, errors.New("DB2 Log Agent returned an empty current LRI")
	}
	return &domain.CDCPosition{DatabaseType: string(domain.DataSourceDB2), PositionType: "DB2_LRI", PositionValue: lri.String(), Resource: strings.TrimSpace(c.ds.CDCURL)}, nil
}

func (c *Connector) CDCSelections(ctx context.Context, tables []string) ([]CDCSelection, error) {
	p, err := c.get(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]CDCSelection, 0, len(tables))
	for _, raw := range tables {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		schema, table := c.ds.Schema, raw
		if x := strings.LastIndex(raw, "."); x > 0 {
			schema, table = raw[:x], raw[x+1:]
		}
		if strings.TrimSpace(schema) == "" {
			schema = strings.ToUpper(strings.TrimSpace(c.ds.Username))
		}
		q := `SELECT VARCHAR(TBSPACEID),VARCHAR(TABLEID),DATACAPTURE FROM SYSCAT.TABLES WHERE TYPE='T' AND TABSCHEMA=` + qStr(schema) + ` AND TABNAME=` + qStr(table)
		rows, e := p.query(ctx, q)
		if e != nil {
			return nil, e
		}
		if len(rows) != 1 || len(rows[0]) < 3 {
			return nil, fmt.Errorf("DB2 CDC table %s.%s not found", schema, table)
		}
		if !db2Bool(cellString(rows[0], 2)) {
			return nil, fmt.Errorf("DB2 CDC table %s.%s must be ALTERed with DATA CAPTURE CHANGES", schema, table)
		}
		ts := cellInt64(rows[0], 0)
		id := cellInt64(rows[0], 1)
		if ts < 0 || ts > 65535 || id < 0 || id > 65535 {
			return nil, fmt.Errorf("DB2 CDC internal table identity out of range for %s.%s: tbspace=%d tableid=%d", schema, table, ts, id)
		}
		m, e := c.GetTableMetadata(ctx, schema, table)
		if e != nil {
			return nil, e
		}
		if len(m.PrimaryKeys) == 0 {
			return nil, fmt.Errorf("DB2 CDC table %s.%s requires a primary key for deterministic target apply", schema, table)
		}
		// Out-of-row LOB bytes only exist in the propagatable log stream when
		// the LOB column is LOGGED.  Failing here is materially safer than
		// waiting for ADD LOB AMOUNT at runtime, because a NOT LOGGED value
		// cannot be reconstructed after the fact.
		notLogged, e := p.query(ctx, `SELECT COLNAME,TYPENAME FROM SYSCAT.COLUMNS WHERE TABSCHEMA=`+qStr(schema)+` AND TABNAME=`+qStr(table)+` AND LOGGED='N' ORDER BY COLNO`)
		if e != nil {
			return nil, fmt.Errorf("DB2 CDC inspect LOB LOGGED attributes for %s.%s: %w", schema, table, e)
		}
		if len(notLogged) > 0 {
			bad := make([]string, 0, len(notLogged))
			for _, row := range notLogged {
				bad = append(bad, cellString(row, 0)+"("+cellString(row, 1)+")")
			}
			return nil, fmt.Errorf("DB2 CDC table %s.%s has NOT LOGGED LOB column(s) %s; QMigration cannot reconstruct out-of-row values", schema, table, strings.Join(bad, ", "))
		}
		// XML out-of-row replication is available from Db2 11.5.8 when the
		// instance has DB2_DCC_XML_SERIALIZE enabled.  Verify the live value
		// whenever an XML column is selected rather than accepting a task
		// that could later produce an unreconstructable XML after-image.
		hasXML := false
		for _, col := range m.Columns {
			if strings.EqualFold(strings.TrimSpace(col.DataType), "xml") || strings.EqualFold(strings.TrimSpace(col.ColumnType), "xml") {
				hasXML = true
				break
			}
		}
		if hasXML {
			reg, e := p.query(ctx, `SELECT COALESCE(REG_VAR_VALUE,'') FROM TABLE(SYSPROC.ENV_GET_REG_VARIABLES(-1,1)) AS R WHERE REG_VAR_NAME='DB2_DCC_XML_SERIALIZE'`)
			if e != nil {
				return nil, fmt.Errorf("DB2 CDC table %s.%s contains XML but DB2_DCC_XML_SERIALIZE could not be verified: %w", schema, table, e)
			}
			if len(reg) == 0 || !db2Bool(cellString(reg[0], 0)) {
				return nil, fmt.Errorf("DB2 CDC table %s.%s contains XML and requires DB2_DCC_XML_SERIALIZE=YES", schema, table)
			}
		}
		out = append(out, CDCSelection{Schema: schema, Table: table, TablespaceID: uint16(ts), TableID: uint16(id), Columns: m.Columns, PrimaryKeys: m.PrimaryKeys})
	}
	if len(out) == 0 {
		return nil, errors.New("DB2 CDC selection is empty")
	}
	return out, nil
}

func (c *Connector) ValidateCDCSelection(ctx context.Context, mappings []domain.TableMapping) error {
	if !experimentalDB2LogCDCEnabled() {
		return errors.New("DB2 source CDC experimental gates are not enabled")
	}
	tables := make([]string, 0, len(mappings))
	for _, m := range mappings {
		tables = append(tables, m.SourceSchema+"."+m.SourceTable)
	}
	if _, err := c.CDCSelections(ctx, tables); err != nil {
		return err
	}
	a, err := c.logAgentClient()
	if err != nil {
		return err
	}
	if err = a.Health(ctx); err != nil {
		return fmt.Errorf("DB2 Log Agent health: %w", err)
	}
	p, err := a.Position(ctx)
	if err != nil {
		return fmt.Errorf("DB2 Log Agent position: %w", err)
	}
	if !p.Recoverable {
		return errors.New("DB2 Log Agent reports database is not recoverable")
	}
	return nil
}

func (c *Connector) MigrationPrechecks(ctx context.Context, source bool) []domain.PrecheckItem {
	items := []domain.PrecheckItem{}
	if !experimentalDB2NativeEnabled() {
		return append(items, domain.PrecheckItem{Name: "DB2 Native gate", Level: domain.PrecheckFailed, Message: "set QMIGRATION_EXPERIMENTAL_DB2_NATIVE=1 only after qualification"})
	}
	if strings.TrimSpace(c.ds.Database) == "" {
		items = append(items, domain.PrecheckItem{Name: "DB2 database/RDB", Level: domain.PrecheckFailed, Message: "database/RDB name is required"})
	} else {
		items = append(items, domain.PrecheckItem{Name: "DB2 database/RDB", Level: domain.PrecheckPass, Message: c.ds.Database})
	}
	if c.ds.TLSMode == domain.TLSModeDisable {
		items = append(items, domain.PrecheckItem{Name: "DB2 transport security", Level: domain.PrecheckWarning, Message: "TLS is disabled; QMigration prefers SECMEC 9 encrypted credentials but production deployments should qualify a TLS listener"})
	} else {
		items = append(items, domain.PrecheckItem{Name: "DB2 transport security", Level: domain.PrecheckPass, Message: string(c.ds.TLSMode)})
	}
	if source {
		if experimentalDB2LogCDCEnabled() {
			if strings.TrimSpace(c.ds.CDCURL) == "" {
				items = append(items, domain.PrecheckItem{Name: "DB2 source CDC", Level: domain.PrecheckFailed, Message: "cdc_url must point to the QMigration DB2 Log Agent"})
			} else {
				items = append(items, domain.PrecheckItem{Name: "DB2 source CDC", Level: domain.PrecheckPass, Message: "db2ReadLog via QMigration DB2 Log Agent; DATA CAPTURE CHANGES and per-table descriptors are validated before capture"})
			}
		} else {
			items = append(items, domain.PrecheckItem{Name: "DB2 source CDC", Level: domain.PrecheckWarning, Message: "enable QMIGRATION_EXPERIMENTAL_DB2_LOG_CDC only after the DB2 Log Agent is qualified"})
		}
	} else {
		items = append(items, domain.PrecheckItem{Name: "DB2 identity cutover state", Level: domain.PrecheckPass, Message: "QMigration synchronizes identity RESTART WITH state after Full Load and after committed target CDC transactions"})
	}
	return items
}
