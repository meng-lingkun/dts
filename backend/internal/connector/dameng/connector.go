package damengconnector

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
)

// RC13 deliberately keeps the Dameng wire driver replaceable. QMigration owns
// metadata/full-load/schema/target-apply semantics; a DM database/sql driver
// only transports authenticated SQL to DM8. This avoids vendoring/probing a
// proprietary wire protocol while keeping third-party migration runtimes out
// of the data plane.
type Factory struct{}

func NewFactory() *Factory { return &Factory{} }

func (*Factory) Capabilities(t domain.DataSourceType) connector.Descriptor {
	caps := []connector.Capability{connector.CapabilityProtocolProbe}
	note := "TCP probe only; set QMIGRATION_EXPERIMENTAL_DAMENG_NATIVE=1 and register a DM database/sql driver to enable the experimental Dameng data plane"
	if experimentalEnabled() {
		caps = append(caps,
			connector.CapabilityMetadata,
			connector.CapabilityFullRead,
			connector.CapabilityFullWrite,
			connector.CapabilityKeysetBoundary,
			connector.CapabilitySchemaCreate,
			connector.CapabilityPostLoadSchema,
			connector.CapabilityCDCApply,
			connector.CapabilityCDCTransactional,
			connector.CapabilityPointLookup,
			connector.CapabilityMigrationPrecheck,
		)
		note = "EXPERIMENTAL QMigration Dameng metadata/full-load/schema/target-apply data plane; SQL transport requires a registered DM database/sql driver"
		if experimentalCDCEnabled() {
			caps = append(caps, connector.CapabilityCDCPosition, connector.CapabilityCDCRead, connector.CapabilityValidationSnapshot)
			note += "; DBMS_LOGMNR archived-log CDC + DM_LSN flashback validation enabled behind the separate CDC gate"
		} else {
			note += "; source CDC remains disabled until QMIGRATION_EXPERIMENTAL_DAMENG_LOG_CDC=1"
		}
	}
	return connector.Descriptor{
		Type: t, Protocol: "dm-sql", Native: true, Capabilities: caps,
		Maturity: connector.MaturityExperimental, QualificationRequired: true, Note: note,
	}
}

func (*Factory) New(ds domain.DataSource) (connector.Connector, error) {
	if ds.Host == "" || ds.Port <= 0 {
		return nil, errors.New("invalid Dameng endpoint")
	}
	return &Connector{ds: ds}, nil
}

func experimentalEnabled() bool { return envOn("QMIGRATION_EXPERIMENTAL_DAMENG_NATIVE") }
func experimentalCDCEnabled() bool {
	return experimentalEnabled() && envOn("QMIGRATION_EXPERIMENTAL_DAMENG_LOG_CDC")
}
func envOn(name string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(name))) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

// dmRunner is intentionally smaller than database/sql. Tests exercise the
// migration semantics without needing a proprietary driver or live database.
type dmRunner interface {
	Ping(context.Context) error
	Query(context.Context, string, ...any) ([][]connector.Value, error)
	Exec(context.Context, string, ...any) (int64, error)
	Begin(context.Context) error
	Commit(context.Context) error
	Rollback(context.Context) error
	Close() error
}

type sqlRunner struct {
	db *sql.DB
	tx *sql.Tx
}

func (r *sqlRunner) Ping(ctx context.Context) error { return r.db.PingContext(ctx) }
func (r *sqlRunner) activeQuery(ctx context.Context, q string, args ...any) (*sql.Rows, error) {
	if r.tx != nil {
		return r.tx.QueryContext(ctx, q, args...)
	}
	return r.db.QueryContext(ctx, q, args...)
}
func (r *sqlRunner) Query(ctx context.Context, q string, args ...any) ([][]connector.Value, error) {
	rows, err := r.activeQuery(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	out := make([][]connector.Value, 0)
	for rows.Next() {
		holders := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range holders {
			ptrs[i] = &holders[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		vals := make([]connector.Value, len(cols))
		for i, v := range holders {
			if v == nil {
				vals[i].Null = true
				continue
			}
			switch x := v.(type) {
			case []byte:
				vals[i].Raw = append([]byte(nil), x...)
			case string:
				vals[i].Raw = []byte(x)
			case time.Time:
				vals[i].Raw = []byte(x.Format("2006-01-02 15:04:05.999999999 -07:00"))
			case bool:
				if x {
					vals[i].Raw = []byte("1")
				} else {
					vals[i].Raw = []byte("0")
				}
			default:
				vals[i].Raw = []byte(fmt.Sprint(x))
			}
		}
		out = append(out, vals)
	}
	return out, rows.Err()
}
func (r *sqlRunner) Exec(ctx context.Context, q string, args ...any) (int64, error) {
	var res sql.Result
	var err error
	if r.tx != nil {
		res, err = r.tx.ExecContext(ctx, q, args...)
	} else {
		res, err = r.db.ExecContext(ctx, q, args...)
	}
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}
func (r *sqlRunner) Begin(ctx context.Context) error {
	if r.tx != nil {
		return errors.New("Dameng transaction already active")
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err == nil {
		r.tx = tx
	}
	return err
}
func (r *sqlRunner) Commit(ctx context.Context) error {
	if r.tx == nil {
		return errors.New("Dameng transaction not active")
	}
	tx := r.tx
	r.tx = nil
	return tx.Commit()
}
func (r *sqlRunner) Rollback(ctx context.Context) error {
	if r.tx == nil {
		return nil
	}
	tx := r.tx
	r.tx = nil
	return tx.Rollback()
}
func (r *sqlRunner) Close() error {
	if r.tx != nil {
		_ = r.tx.Rollback()
		r.tx = nil
	}
	return r.db.Close()
}

func validateTransportSettings(ds domain.DataSource) error {
	mode := domain.TLSMode(strings.ToUpper(strings.TrimSpace(string(ds.TLSMode))))
	if mode == "" {
		mode = domain.TLSModeDisable
	}
	switch mode {
	case domain.TLSModeDisable:
		if strings.TrimSpace(ds.TLSServerName) != "" || strings.TrimSpace(ds.TLSCACert) != "" || strings.TrimSpace(ds.TLSClientCert) != "" || strings.TrimSpace(ds.TLSClientKey) != "" {
			return errors.New("Dameng TLS material was configured while TLS mode is DISABLE")
		}
		return nil
	case domain.TLSModePreferred, domain.TLSModeRequired:
		return fmt.Errorf("Dameng TLS mode %s is not qualified in RC13; QMigration refuses to silently downgrade the provider transport", mode)
	default:
		return fmt.Errorf("invalid Dameng TLS mode %q", ds.TLSMode)
	}
}

var openRunner = func(ds domain.DataSource) (dmRunner, error) {
	if err := validateTransportSettings(ds); err != nil {
		return nil, err
	}
	if err := loadDriverPlugin(os.Getenv("QMIGRATION_DAMENG_DRIVER_PLUGIN")); err != nil {
		return nil, err
	}
	driver := strings.TrimSpace(ds.DriverClass)
	if driver == "" {
		driver = strings.TrimSpace(os.Getenv("QMIGRATION_DAMENG_SQL_DRIVER"))
	}
	if driver == "" {
		driver = "dm"
	}
	found := false
	for _, d := range sql.Drivers() {
		if d == driver {
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("Dameng database/sql driver %q is not registered in this QMigration binary", driver)
	}
	dsn := strings.TrimSpace(ds.JDBCURL)
	if dsn == "" || strings.HasPrefix(strings.ToLower(dsn), "jdbc:") {
		u := &url.URL{Scheme: "dm", Host: net.JoinHostPort(ds.Host, strconv.Itoa(ds.Port))}
		if ds.Username != "" {
			u.User = url.UserPassword(ds.Username, ds.Password)
		}
		q := u.Query()
		schema := strings.TrimSpace(ds.Schema)
		if schema == "" {
			schema = strings.TrimSpace(ds.Database)
		}
		if schema != "" {
			q.Set("schema", schema)
		}
		q.Set("connectTimeout", "5")
		u.RawQuery = q.Encode()
		dsn = u.String()
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(30 * time.Minute)
	return &sqlRunner{db: db}, nil
}

type Connector struct {
	ds            domain.DataSource
	mu            sync.Mutex
	r             dmRunner
	validationLSN string
}

func (c *Connector) get(ctx context.Context) (dmRunner, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.r != nil {
		return c.r, nil
	}
	r, err := openRunner(c.ds)
	if err != nil {
		return nil, err
	}
	c.r = r
	return r, nil
}
func (c *Connector) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.r == nil {
		return nil
	}
	err := c.r.Close()
	c.r = nil
	return err
}
func (c *Connector) TestConnection(ctx context.Context) error {
	if !experimentalEnabled() {
		d := net.Dialer{Timeout: 5 * time.Second}
		conn, err := d.DialContext(ctx, "tcp", net.JoinHostPort(c.ds.Host, strconv.Itoa(c.ds.Port)))
		if err != nil {
			return err
		}
		return conn.Close()
	}
	r, err := c.get(ctx)
	if err != nil {
		return err
	}
	if err := r.Ping(ctx); err != nil {
		return err
	}
	rows, err := r.Query(ctx, "SELECT 1 FROM DUAL")
	if err != nil {
		return err
	}
	if len(rows) != 1 || len(rows[0]) != 1 || rows[0][0].Null || strings.TrimSpace(string(rows[0][0].Raw)) != "1" {
		return errors.New("Dameng native SELECT 1 probe returned unexpected result")
	}
	return nil
}
func (c *Connector) GetVersion(ctx context.Context) (string, error) {
	if !experimentalEnabled() {
		return "dm-tcp", nil
	}
	r, err := c.get(ctx)
	if err != nil {
		return "", err
	}
	rows, err := r.Query(ctx, "SELECT BANNER FROM SYS.V$VERSION")
	if err != nil {
		return "", err
	}
	if len(rows) == 0 || len(rows[0]) == 0 || rows[0][0].Null {
		return "dm8", nil
	}
	return strings.TrimSpace(string(rows[0][0].Raw)), nil
}

func qIdent(s string) string            { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
func qName(schema, table string) string { return qIdent(schema) + "." + qIdent(table) }
func (c *Connector) readTableExpr(schema, table string) string {
	base := qName(schema, table)
	if strings.TrimSpace(c.validationLSN) != "" {
		base += " AS OF SCN " + c.validationLSN
	}
	return base
}
func (c *Connector) rejectValidationSnapshotWrite() error {
	if c.validationLSN != "" {
		return fmt.Errorf("Dameng validation snapshot at LSN %s is read-only", c.validationLSN)
	}
	return nil
}
func cellString(row []connector.Value, i int) string {
	if i < 0 || i >= len(row) || row[i].Null {
		return ""
	}
	return strings.TrimSpace(string(row[i].Raw))
}
func cellInt(row []connector.Value, i int) int {
	n, _ := strconv.Atoi(cellString(row, i))
	return n
}
func cellInt64(row []connector.Value, i int) int64 {
	n, _ := strconv.ParseInt(cellString(row, i), 10, 64)
	return n
}

func (c *Connector) ListSchemas(ctx context.Context) ([]domain.SchemaInfo, error) {
	r, err := c.get(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := r.Query(ctx, `SELECT NAME FROM SYS.SYSOBJECTS WHERE TYPE$='SCH' AND NAME NOT LIKE 'SYS%' ORDER BY NAME`)
	if err != nil {
		return nil, err
	}
	out := make([]domain.SchemaInfo, 0, len(rows))
	for _, row := range rows {
		if n := cellString(row, 0); n != "" {
			out = append(out, domain.SchemaInfo{Name: n})
		}
	}
	return out, nil
}
func (c *Connector) ListTables(ctx context.Context, schema string) ([]domain.TableInfo, error) {
	r, err := c.get(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := r.Query(ctx, `SELECT TABLE_NAME,COALESCE(NUM_ROWS,0) FROM ALL_TABLES WHERE OWNER=? ORDER BY TABLE_NAME`, strings.ToUpper(schema))
	if err != nil {
		return nil, err
	}
	out := make([]domain.TableInfo, 0, len(rows))
	for _, row := range rows {
		if n := cellString(row, 0); n != "" {
			out = append(out, domain.TableInfo{Schema: schema, Name: n, Rows: cellInt64(row, 1)})
		}
	}
	return out, nil
}

func dmColumnType(dataType string, length, precision, scale int) string {
	t := strings.ToUpper(strings.TrimSpace(dataType))
	switch t {
	case "CHAR", "CHARACTER", "VARCHAR", "VARCHAR2", "NVARCHAR2", "NCHAR", "BINARY", "VARBINARY":
		if length > 0 {
			return fmt.Sprintf("%s(%d)", t, length)
		}
	case "DECIMAL", "DEC", "NUMERIC", "NUMBER":
		if precision > 0 {
			return fmt.Sprintf("%s(%d,%d)", t, precision, scale)
		}
	case "TIMESTAMP", "TIME":
		if scale > 0 {
			return fmt.Sprintf("%s(%d)", t, scale)
		}
	}
	return t
}
func numericType(t string) bool {
	s := strings.ToLower(strings.TrimSpace(t))
	switch s {
	case "tinyint", "smallint", "int", "integer", "bigint", "decimal", "dec", "numeric", "number", "float", "real", "double", "double precision":
		return true
	}
	return false
}

func (c *Connector) GetTableMetadata(ctx context.Context, schema, table string) (*domain.TableMetadata, error) {
	r, err := c.get(ctx)
	if err != nil {
		return nil, err
	}
	owner, name := strings.ToUpper(schema), strings.ToUpper(table)
	rows, err := r.Query(ctx, `SELECT COLUMN_NAME,DATA_TYPE,COALESCE(DATA_LENGTH,0),COALESCE(DATA_PRECISION,0),COALESCE(DATA_SCALE,0),NULLABLE,COLUMN_ID FROM ALL_TAB_COLUMNS WHERE OWNER=? AND TABLE_NAME=? ORDER BY COLUMN_ID`, owner, name)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("Dameng table %s.%s not found or not visible", schema, table)
	}
	md := &domain.TableMetadata{Schema: schema, Name: table, Columns: make([]domain.ColumnInfo, 0, len(rows))}
	for _, row := range rows {
		dt := strings.ToLower(cellString(row, 1))
		ln, pr, sc := cellInt(row, 2), cellInt(row, 3), cellInt(row, 4)
		md.Columns = append(md.Columns, domain.ColumnInfo{Name: cellString(row, 0), DataType: dt, ColumnType: dmColumnType(dt, ln, pr, sc), Nullable: strings.EqualFold(cellString(row, 5), "Y"), Ordinal: cellInt(row, 6)})
	}
	pkRows, err := r.Query(ctx, `SELECT C.INDEX_NAME,CC.COLUMN_NAME FROM ALL_CONSTRAINTS C JOIN ALL_CONS_COLUMNS CC ON C.OWNER=CC.OWNER AND C.CONSTRAINT_NAME=CC.CONSTRAINT_NAME WHERE C.OWNER=? AND C.TABLE_NAME=? AND C.CONSTRAINT_TYPE='P' ORDER BY CC.POSITION`, owner, name)
	if err != nil {
		return nil, err
	}
	pkIndexName := ""
	for _, row := range pkRows {
		if pkIndexName == "" {
			pkIndexName = cellString(row, 0)
		}
		if k := cellString(row, 1); k != "" {
			md.PrimaryKeys = append(md.PrimaryKeys, k)
		}
	}
	if len(md.PrimaryKeys) > 0 {
		md.PrimaryKey = md.PrimaryKeys[0]
	}
	for i := range md.Columns {
		for _, k := range md.PrimaryKeys {
			if strings.EqualFold(md.Columns[i].Name, k) {
				md.Columns[i].PrimaryKey = true
			}
		}
	}
	idxRows, err := r.Query(ctx, `SELECT I.INDEX_NAME,I.UNIQUENESS,IC.COLUMN_NAME,IC.COLUMN_POSITION FROM ALL_INDEXES I JOIN ALL_IND_COLUMNS IC ON I.OWNER=IC.INDEX_OWNER AND I.INDEX_NAME=IC.INDEX_NAME WHERE I.TABLE_OWNER=? AND I.TABLE_NAME=? ORDER BY I.INDEX_NAME,IC.COLUMN_POSITION`, owner, name)
	if err == nil {
		by := map[string]int{}
		for _, row := range idxRows {
			in := cellString(row, 0)
			if in == "" {
				continue
			}
			pos, ok := by[in]
			if !ok {
				md.Indexes = append(md.Indexes, domain.IndexInfo{Name: in, Unique: strings.EqualFold(cellString(row, 1), "UNIQUE")})
				pos = len(md.Indexes) - 1
				by[in] = pos
			}
			md.Indexes[pos].Columns = append(md.Indexes[pos].Columns, cellString(row, 2))
			md.Indexes[pos].Primary = pkIndexName != "" && strings.EqualFold(in, pkIndexName)
		}
	}
	fkRows, err := r.Query(ctx, `SELECT C.CONSTRAINT_NAME,CC.COLUMN_NAME,RC.OWNER,RC.TABLE_NAME,RCC.COLUMN_NAME,CC.POSITION FROM ALL_CONSTRAINTS C JOIN ALL_CONS_COLUMNS CC ON C.OWNER=CC.OWNER AND C.CONSTRAINT_NAME=CC.CONSTRAINT_NAME JOIN ALL_CONSTRAINTS RC ON C.R_OWNER=RC.OWNER AND C.R_CONSTRAINT_NAME=RC.CONSTRAINT_NAME JOIN ALL_CONS_COLUMNS RCC ON RC.OWNER=RCC.OWNER AND RC.CONSTRAINT_NAME=RCC.CONSTRAINT_NAME AND CC.POSITION=RCC.POSITION WHERE C.OWNER=? AND C.TABLE_NAME=? AND C.CONSTRAINT_TYPE='R' ORDER BY C.CONSTRAINT_NAME,CC.POSITION`, owner, name)
	if err == nil {
		by := map[string]int{}
		for _, row := range fkRows {
			n := cellString(row, 0)
			if n == "" {
				continue
			}
			i, ok := by[n]
			if !ok {
				md.ForeignKeys = append(md.ForeignKeys, domain.ForeignKeyInfo{Name: n, ReferencedSchema: cellString(row, 2), ReferencedTable: cellString(row, 3)})
				i = len(md.ForeignKeys) - 1
				by[n] = i
			}
			md.ForeignKeys[i].Columns = append(md.ForeignKeys[i].Columns, cellString(row, 1))
			md.ForeignKeys[i].ReferencedColumns = append(md.ForeignKeys[i].ReferencedColumns, cellString(row, 4))
		}
	}
	statRows, _ := r.Query(ctx, `SELECT COALESCE(NUM_ROWS,0) FROM ALL_TABLES WHERE OWNER=? AND TABLE_NAME=?`, owner, name)
	if len(statRows) > 0 {
		md.EstimatedRows = cellInt64(statRows[0], 0)
		md.HasRows = md.EstimatedRows > 0
	}
	if len(md.PrimaryKeys) == 1 {
		for _, col := range md.Columns {
			if strings.EqualFold(col.Name, md.PrimaryKey) {
				md.PrimaryKeyType = col.DataType
				md.PrimaryKeyNumeric = numericType(col.DataType)
				break
			}
		}
		if md.PrimaryKeyNumeric {
			rr, e := r.Query(ctx, `SELECT MIN(`+qIdent(md.PrimaryKey)+`),MAX(`+qIdent(md.PrimaryKey)+`) FROM `+qName(schema, table))
			if e == nil && len(rr) > 0 && len(rr[0]) >= 2 && !rr[0][0].Null {
				md.MinPK = cellInt64(rr[0], 0)
				md.MaxPK = cellInt64(rr[0], 1)
				md.HasRows = true
			}
		}
	}
	return md, nil
}

func selectExpr(col domain.ColumnInfo) string {
	n := qIdent(col.Name)
	t := strings.ToLower(col.DataType)
	switch {
	case strings.Contains(t, "blob") || strings.Contains(t, "binary") || t == "image":
		return n
	default:
		return n
	}
}
func bindValue(col domain.ColumnInfo, v connector.Value) (any, error) {
	if v.Null {
		return nil, nil
	}
	t := strings.ToLower(col.DataType)
	if numericType(t) {
		if err := connector.ValidateNumericLiteral(v.Raw, false); err != nil {
			return nil, err
		}
		return strings.TrimSpace(string(v.Raw)), nil
	}
	if strings.Contains(t, "blob") || strings.Contains(t, "binary") || t == "image" {
		return append([]byte(nil), v.Raw...), nil
	}
	return string(v.Raw), nil
}

func keyCols(req connector.ReadBatchRequest) ([]domain.ColumnInfo, map[string]int, error) {
	ix := map[string]int{}
	for i, c := range req.Columns {
		ix[strings.ToLower(c.Name)] = i
	}
	keys := append([]string(nil), req.PrimaryKeys...)
	if len(keys) == 0 && req.PrimaryKey != "" {
		keys = []string{req.PrimaryKey}
	}
	out := make([]domain.ColumnInfo, len(keys))
	for i, k := range keys {
		p, ok := ix[strings.ToLower(k)]
		if !ok {
			return nil, nil, fmt.Errorf("key %s not in selected columns", k)
		}
		out[i] = req.Columns[p]
	}
	return out, ix, nil
}
func lexPredicate(keys []string, cols []domain.ColumnInfo, vals []connector.Value, op string, args *[]any) (string, error) {
	if len(keys) != len(vals) || len(keys) != len(cols) {
		return "", errors.New("keyset bound/key count mismatch")
	}
	ors := make([]string, 0, len(keys))
	for i := range keys {
		and := make([]string, 0, i+1)
		for j := 0; j < i; j++ {
			v, e := bindValue(cols[j], vals[j])
			if e != nil {
				return "", e
			}
			and = append(and, qIdent(keys[j])+"=?")
			*args = append(*args, v)
		}
		v, e := bindValue(cols[i], vals[i])
		if e != nil {
			return "", e
		}
		cmp := op
		if i < len(keys)-1 {
			if op == ">=" {
				cmp = ">"
			}
			if op == "<=" {
				cmp = "<"
			}
		}
		and = append(and, qIdent(keys[i])+cmp+"?")
		*args = append(*args, v)
		ors = append(ors, "("+strings.Join(and, " AND ")+")")
	}
	return "(" + strings.Join(ors, " OR ") + ")", nil
}
func (c *Connector) ReadBatch(ctx context.Context, req connector.ReadBatchRequest) (*connector.RowBatch, error) {
	r, err := c.get(ctx)
	if err != nil {
		return nil, err
	}
	if req.Limit <= 0 {
		req.Limit = 500
	}
	if len(req.Columns) == 0 {
		return nil, errors.New("no selected columns")
	}
	cols := make([]string, len(req.Columns))
	for i, col := range req.Columns {
		cols[i] = selectExpr(col)
	}
	keys := append([]string(nil), req.PrimaryKeys...)
	if len(keys) == 0 && req.PrimaryKey != "" {
		keys = []string{req.PrimaryKey}
	}
	kcols, ix, err := keyCols(req)
	if err != nil {
		return nil, err
	}
	cond := []string{}
	args := []any{}
	if req.UseKeyset {
		if len(keys) == 0 {
			return nil, errors.New("Dameng keyset read requires migration key")
		}
		if len(req.Cursor) > 0 {
			p, e := lexPredicate(keys, kcols, req.Cursor, ">", &args)
			if e != nil {
				return nil, e
			}
			cond = append(cond, p)
		} else if len(req.LowerBound) > 0 {
			p, e := lexPredicate(keys, kcols, req.LowerBound, ">=", &args)
			if e != nil {
				return nil, e
			}
			cond = append(cond, p)
		}
		if len(req.UpperBound) > 0 {
			p, e := lexPredicate(keys, kcols, req.UpperBound, "<", &args)
			if e != nil {
				return nil, e
			}
			cond = append(cond, p)
		}
	} else {
		if len(keys) == 0 {
			return nil, errors.New("Dameng range read requires primary key")
		}
		cond = append(cond, qIdent(keys[0])+">=?", qIdent(keys[0])+"<=?")
		args = append(args, req.StartPK, req.EndPK)
		if req.HasAfter {
			cond = append(cond, qIdent(keys[0])+">")
			cond[len(cond)-1] += "?"
			args = append(args, req.AfterPK)
		}
	}
	if strings.TrimSpace(req.CustomWhere) != "" {
		cond = append(cond, "("+strings.TrimSpace(req.CustomWhere)+")")
	}
	where := ""
	if len(cond) > 0 {
		where = " WHERE " + strings.Join(cond, " AND ")
	}
	order := make([]string, len(keys))
	for i, k := range keys {
		order[i] = qIdent(k)
	}
	q := `SELECT ` + strings.Join(cols, ",") + ` FROM ` + c.readTableExpr(req.Schema, req.Table) + where
	if len(order) > 0 {
		q += ` ORDER BY ` + strings.Join(order, ",")
	}
	q += ` LIMIT ` + strconv.Itoa(req.Limit)
	rows, err := r.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	b := &connector.RowBatch{Rows: make([][]connector.Value, 0, len(rows))}
	for _, row := range rows {
		b.Rows = append(b.Rows, row)
		for _, v := range row {
			if !v.Null {
				b.Bytes += int64(len(v.Raw))
			}
		}
		if req.UseKeyset {
			b.LastKey = b.LastKey[:0]
			for _, k := range keys {
				v := row[ix[strings.ToLower(k)]]
				v.Raw = append([]byte(nil), v.Raw...)
				b.LastKey = append(b.LastKey, v)
			}
		} else if len(keys) > 0 {
			v := row[ix[strings.ToLower(keys[0])]]
			if !v.Null {
				b.LastPK, _ = strconv.ParseInt(strings.TrimSpace(string(v.Raw)), 10, 64)
			}
		}
	}
	return b, nil
}

func dmTargetType(col domain.ColumnInfo) (string, error) {
	dt := strings.ToLower(strings.TrimSpace(col.DataType))
	ct := strings.ToUpper(strings.TrimSpace(col.ColumnType))
	if ct != "" && (strings.HasPrefix(ct, "VARCHAR") || strings.HasPrefix(ct, "VARCHAR2") || strings.HasPrefix(ct, "CHAR") || strings.HasPrefix(ct, "DECIMAL") || strings.HasPrefix(ct, "NUMERIC") || strings.HasPrefix(ct, "NUMBER") || strings.HasPrefix(ct, "TIMESTAMP") || strings.HasPrefix(ct, "VARBINARY") || strings.HasPrefix(ct, "BINARY")) {
		return ct, nil
	}
	switch {
	case dt == "tinyint":
		return "SMALLINT", nil
	case dt == "smallint":
		return "SMALLINT", nil
	case dt == "mediumint" || dt == "int" || dt == "integer":
		return "INT", nil
	case dt == "bigint":
		return "BIGINT", nil
	case dt == "decimal" || dt == "numeric" || dt == "number":
		if ct != "" {
			return ct, nil
		}
		return "DECIMAL(38,10)", nil
	case dt == "real" || dt == "float":
		return "FLOAT", nil
	case dt == "double" || dt == "double precision":
		return "DOUBLE", nil
	case dt == "bool" || dt == "boolean" || dt == "bit":
		return "BIT", nil
	case dt == "date":
		return "DATE", nil
	case dt == "time":
		return "TIME(6)", nil
	case strings.Contains(dt, "timestamp") || dt == "datetime":
		return "TIMESTAMP(6)", nil
	case strings.Contains(dt, "blob") || dt == "bytea" || dt == "image" || strings.Contains(dt, "binary"):
		return "BLOB", nil
	case strings.Contains(dt, "clob") || strings.Contains(dt, "text") || dt == "json" || dt == "jsonb" || dt == "xml" || dt == "long":
		return "CLOB", nil
	case dt == "uuid":
		return "VARCHAR(36)", nil
	case strings.Contains(dt, "char") || dt == "varchar" || dt == "string":
		if ct != "" {
			return ct, nil
		}
		return "VARCHAR(4000)", nil
	default:
		return "", fmt.Errorf("unsupported Dameng target type %q for column %s", col.DataType, col.Name)
	}
}

func (c *Connector) EnsureSchema(ctx context.Context, schema string) error {
	r, err := c.get(ctx)
	if err != nil {
		return err
	}
	rows, err := r.Query(ctx, `SELECT NAME FROM SYS.SYSOBJECTS WHERE TYPE$='SCH' AND NAME=?`, strings.ToUpper(schema))
	if err != nil {
		return err
	}
	if len(rows) > 0 {
		return nil
	}
	_, err = r.Exec(ctx, `CREATE SCHEMA `+qIdent(schema))
	return err
}

func (c *Connector) CreateTable(ctx context.Context, schema, table string, cols []domain.ColumnInfo, pk string) error {
	pks := []string{}
	if pk != "" {
		pks = []string{pk}
	}
	return c.CreateTableWithPrimaryKeys(ctx, schema, table, cols, pks)
}

func (c *Connector) CreateTableWithPrimaryKeys(ctx context.Context, schema, table string, cols []domain.ColumnInfo, pks []string) error {
	r, err := c.get(ctx)
	if err != nil {
		return err
	}
	defs := make([]string, 0, len(cols)+1)
	for _, col := range cols {
		typ, e := dmTargetType(col)
		if e != nil {
			return e
		}
		d := qIdent(col.Name) + " " + typ
		if !col.Nullable {
			d += " NOT NULL"
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
	_, err = r.Exec(ctx, `CREATE TABLE `+qName(schema, table)+` (`+strings.Join(defs, ",")+`)`)
	return err
}

func (c *Connector) CreateIndex(ctx context.Context, schema, table string, idx domain.IndexInfo) error {
	if idx.Primary {
		return nil
	}
	r, err := c.get(ctx)
	if err != nil {
		return err
	}
	cols := make([]string, len(idx.Columns))
	for i, x := range idx.Columns {
		cols[i] = qIdent(x)
	}
	unique := ""
	if idx.Unique {
		unique = "UNIQUE "
	}
	_, err = r.Exec(ctx, `CREATE `+unique+`INDEX `+qIdent(idx.Name)+` ON `+qName(schema, table)+` (`+strings.Join(cols, ",")+`)`)
	return err
}

func (c *Connector) CreateForeignKey(ctx context.Context, schema, table string, fk domain.ForeignKeyInfo) error {
	r, err := c.get(ctx)
	if err != nil {
		return err
	}
	cols := make([]string, len(fk.Columns))
	refs := make([]string, len(fk.ReferencedColumns))
	for i, x := range fk.Columns {
		cols[i] = qIdent(x)
	}
	for i, x := range fk.ReferencedColumns {
		refs[i] = qIdent(x)
	}
	_, err = r.Exec(ctx, `ALTER TABLE `+qName(schema, table)+` ADD CONSTRAINT `+qIdent(fk.Name)+` FOREIGN KEY (`+strings.Join(cols, ",")+`) REFERENCES `+qName(fk.ReferencedSchema, fk.ReferencedTable)+` (`+strings.Join(refs, ",")+`)`)
	return err
}

func preparedValues(cols []domain.ColumnInfo, row []connector.Value) ([]any, error) {
	if len(cols) != len(row) {
		return nil, errors.New("column/value count mismatch")
	}
	args := make([]any, len(row))
	for i := range row {
		v, err := bindValue(cols[i], row[i])
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", cols[i].Name, err)
		}
		args[i] = v
	}
	return args, nil
}

func (c *Connector) WriteBatch(ctx context.Context, req connector.WriteBatchRequest) (int64, error) {
	if err := c.rejectValidationSnapshotWrite(); err != nil {
		return 0, err
	}
	r, err := c.get(ctx)
	if err != nil {
		return 0, err
	}
	if len(req.Columns) == 0 {
		return 0, errors.New("no target columns")
	}
	names := make([]string, len(req.Columns))
	ph := make([]string, len(req.Columns))
	for i, col := range req.Columns {
		names[i] = qIdent(col.Name)
		ph[i] = "?"
	}
	keys := append([]string(nil), req.PrimaryKeys...)
	if len(keys) == 0 && req.PrimaryKey != "" {
		keys = []string{req.PrimaryKey}
	}
	var q string
	if len(keys) == 0 {
		q = `INSERT INTO ` + qName(req.Schema, req.Table) + ` (` + strings.Join(names, ",") + `) VALUES (` + strings.Join(ph, ",") + `)`
	} else {
		src := make([]string, len(req.Columns))
		for i, col := range req.Columns {
			src[i] = "? AS " + qIdent(col.Name)
		}
		on := make([]string, len(keys))
		for i, k := range keys {
			on[i] = "T." + qIdent(k) + "=S." + qIdent(k)
		}
		sets := []string{}
		keyset := map[string]bool{}
		for _, k := range keys {
			keyset[strings.ToLower(k)] = true
		}
		for _, col := range req.Columns {
			if !keyset[strings.ToLower(col.Name)] {
				sets = append(sets, "T."+qIdent(col.Name)+"=S."+qIdent(col.Name))
			}
		}
		matched := ""
		if len(sets) > 0 {
			matched = " WHEN MATCHED THEN UPDATE SET " + strings.Join(sets, ",")
		}
		vals := make([]string, len(req.Columns))
		for i, col := range req.Columns {
			vals[i] = "S." + qIdent(col.Name)
		}
		q = `MERGE INTO ` + qName(req.Schema, req.Table) + ` T USING (SELECT ` + strings.Join(src, ",") + ` FROM DUAL) S ON (` + strings.Join(on, " AND ") + `)` + matched + ` WHEN NOT MATCHED THEN INSERT (` + strings.Join(names, ",") + `) VALUES (` + strings.Join(vals, ",") + `)`
	}

	// Database/sql target writes are grouped into one local transaction unless
	// the caller already started a CDC transaction.
	ownTx := false
	if sr, ok := r.(*sqlRunner); ok && sr.tx == nil {
		if err = r.Begin(ctx); err != nil {
			return 0, err
		}
		ownTx = true
	}
	var total int64
	for _, row := range req.Rows {
		args, e := preparedValues(req.Columns, row)
		if e != nil {
			if ownTx {
				_ = r.Rollback(ctx)
			}
			return total, e
		}
		n, e := r.Exec(ctx, q, args...)
		if e != nil {
			if ownTx {
				_ = r.Rollback(ctx)
			}
			return total, e
		}
		if n <= 0 {
			n = 1
		}
		total += n
	}
	if ownTx {
		if err = r.Commit(ctx); err != nil {
			return total, err
		}
	}
	return total, nil
}

func (c *Connector) DeleteByKey(ctx context.Context, req connector.DeleteByKeyRequest) error {
	if err := c.rejectValidationSnapshotWrite(); err != nil {
		return err
	}
	r, err := c.get(ctx)
	if err != nil {
		return err
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
		return errors.New("invalid Dameng delete key")
	}
	conds := make([]string, len(keys))
	args := make([]any, len(keys))
	for i, k := range keys {
		v, e := bindValue(cols[i], vals[i])
		if e != nil {
			return e
		}
		conds[i] = qIdent(k) + "=?"
		args[i] = v
	}
	_, err = r.Exec(ctx, `DELETE FROM `+qName(req.Schema, req.Table)+` WHERE `+strings.Join(conds, " AND "), args...)
	return err
}

func (c *Connector) ReadByKey(ctx context.Context, req connector.ReadByKeyRequest) ([]connector.Value, bool, error) {
	if len(req.PrimaryKeys) == 0 || len(req.PrimaryKeys) != len(req.KeyValues) || len(req.PrimaryKeys) != len(req.KeyColumns) {
		return nil, false, errors.New("invalid Dameng point lookup key")
	}
	r, err := c.get(ctx)
	if err != nil {
		return nil, false, err
	}
	sel := make([]string, len(req.Columns))
	for i, col := range req.Columns {
		sel[i] = selectExpr(col)
	}
	conds := make([]string, len(req.PrimaryKeys))
	args := make([]any, len(req.PrimaryKeys))
	for i, k := range req.PrimaryKeys {
		v, e := bindValue(req.KeyColumns[i], req.KeyValues[i])
		if e != nil {
			return nil, false, e
		}
		conds[i] = qIdent(k) + "=?"
		args[i] = v
	}
	rows, err := r.Query(ctx, `SELECT `+strings.Join(sel, ",")+` FROM `+c.readTableExpr(req.Schema, req.Table)+` WHERE `+strings.Join(conds, " AND ")+` LIMIT 1`, args...)
	if err != nil {
		return nil, false, err
	}
	if len(rows) == 0 {
		return nil, false, nil
	}
	return rows[0], true, nil
}

func (c *Connector) BeginCDCTransaction(ctx context.Context) error {
	if err := c.rejectValidationSnapshotWrite(); err != nil {
		return err
	}
	r, err := c.get(ctx)
	if err != nil {
		return err
	}
	return r.Begin(ctx)
}
func (c *Connector) CommitCDCTransaction(ctx context.Context) error {
	r, err := c.get(ctx)
	if err != nil {
		return err
	}
	return r.Commit(ctx)
}
func (c *Connector) RollbackCDCTransaction(ctx context.Context) error {
	r, err := c.get(ctx)
	if err != nil {
		return err
	}
	return r.Rollback(ctx)
}

func (c *Connector) PlanKeysetBoundaries(ctx context.Context, req connector.KeysetBoundaryRequest) ([][]connector.Value, error) {
	r, err := c.get(ctx)
	if err != nil {
		return nil, err
	}
	if req.Partitions <= 1 || len(req.Keys) == 0 {
		return nil, nil
	}
	keys := make([]string, len(req.Keys))
	for i, k := range req.Keys {
		keys[i] = qIdent(k)
	}
	// ROW_NUMBER selects exactly one deterministic boundary per NTILE instead
	// of returning every row from every bucket.
	inner := `SELECT ` + strings.Join(keys, ",") + `,NTILE(` + strconv.Itoa(req.Partitions) + `) OVER (ORDER BY ` + strings.Join(keys, ",") + `) B FROM ` + qName(req.Schema, req.Table)
	q := `SELECT ` + strings.Join(keys, ",") + ` FROM (SELECT X.*,ROW_NUMBER() OVER (PARTITION BY B ORDER BY ` + strings.Join(keys, ",") + `) RN FROM (` + inner + `) X) Y WHERE B>1 AND RN=1 ORDER BY B LIMIT ` + strconv.Itoa(req.Partitions-1)
	rows, err := r.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	out := make([][]connector.Value, 0, len(rows))
	for _, row := range rows {
		if len(row) < len(req.Keys) {
			continue
		}
		key := make([]connector.Value, len(req.Keys))
		copy(key, row[:len(req.Keys)])
		out = append(out, key)
	}
	return out, nil
}

func (c *Connector) MigrationPrechecks(ctx context.Context, source bool) []domain.PrecheckItem {
	items := []domain.PrecheckItem{}
	if !experimentalEnabled() {
		return append(items, domain.PrecheckItem{Name: "Dameng native gate", Level: domain.PrecheckFailed, Message: "set QMIGRATION_EXPERIMENTAL_DAMENG_NATIVE=1 after qualification"})
	}
	if _, err := c.get(ctx); err != nil {
		return append(items, domain.PrecheckItem{Name: "Dameng SQL driver", Level: domain.PrecheckFailed, Message: err.Error()})
	}
	if source {
		if !experimentalCDCEnabled() {
			items = append(items, domain.PrecheckItem{Name: "Dameng source CDC", Level: domain.PrecheckWarning, Message: "set QMIGRATION_EXPERIMENTAL_DAMENG_LOG_CDC=1 to enable the experimental DBMS_LOGMNR archived-log source reader"})
		} else {
			items = append(items, c.damengCDCPrechecks(ctx)...)
		}
	}
	return items
}

// ExecDDL executes an explicitly selected DDL statement. RC13 does not expose
// source CDC/DDL replay, so this is currently used by qualification/cleanup and
// remains behind the same experimental connector gate.
func (c *Connector) ExecDDL(ctx context.Context, schema, ddl string) error {
	if strings.TrimSpace(ddl) == "" {
		return errors.New("empty Dameng DDL")
	}
	// The statement must be fully qualified when it references schema objects.
	// database/sql may execute consecutive calls on different pooled sessions,
	// so RC13 deliberately does not rely on SET SCHEMA session state here.
	_ = schema
	r, err := c.get(ctx)
	if err != nil {
		return err
	}
	_, err = r.Exec(ctx, ddl)
	return err
}
