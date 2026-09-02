package gbase8sconnector

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
)

// RC19 treats GBase 8s as its own transactional database family. QMigration
// owns catalog discovery, keyset planning, schema creation, Full Write and
// target CDC apply. The supported transport is the vendor GBase Client-SDK
// ODBC driver exposed through a database/sql provider plugin.
type Factory struct{}

func NewFactory() *Factory { return &Factory{} }

func experimentalEnabled() bool { return envOn("QMIGRATION_EXPERIMENTAL_GBASE8S_NATIVE") }
func envOn(name string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(name))) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

func (*Factory) Capabilities(t domain.DataSourceType) connector.Descriptor {
	caps := []connector.Capability{connector.CapabilityProtocolProbe}
	maturity := connector.MaturityProbeOnly
	note := "GBase 8s TCP probe only; set QMIGRATION_EXPERIMENTAL_GBASE8S_NATIVE=1 and configure a GBase Client-SDK ODBC database/sql provider for the experimental data plane"
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
		maturity = connector.MaturityExperimental
		note = "EXPERIMENTAL GBase 8s V8.8 metadata/full/target-apply data plane over vendor CSDK ODBC; source CDC requires the separate CSDK CDC provider gate"
		if experimentalCDCEnabled() {
			caps = append(caps, connector.CapabilityCDCPosition, connector.CapabilityCDCRead)
			note = "EXPERIMENTAL GBase 8s V8.8 Full/target plus source syscdcv1 CSDK CDC provider with GBASE8S_CDC_SEQ restart checkpoints"
		}
	}
	return connector.Descriptor{Type: t, Protocol: "gbase8s-odbc", Native: true, Capabilities: caps, Maturity: maturity, QualificationRequired: true, Note: note}
}

func (*Factory) New(ds domain.DataSource) (connector.Connector, error) {
	if ds.Type != domain.DataSourceGBase8s {
		return nil, errors.New("GBase 8s factory requires datasource type gbase8s")
	}
	if strings.TrimSpace(ds.Host) == "" || ds.Port <= 0 {
		return nil, errors.New("invalid GBase 8s endpoint")
	}
	return &Connector{ds: ds}, nil
}

type runner interface {
	Ping(context.Context) error
	Query(context.Context, string, ...any) ([][]connector.Value, error)
	Exec(context.Context, string, ...any) (int64, error)
	Begin(context.Context) error
	Commit(context.Context) error
	Rollback(context.Context) error
	TxActive() bool
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
		row := make([]connector.Value, len(cols))
		for i, v := range holders {
			if v == nil {
				row[i].Null = true
				continue
			}
			switch x := v.(type) {
			case []byte:
				row[i].Raw = append([]byte(nil), x...)
			case string:
				row[i].Raw = []byte(x)
			case time.Time:
				row[i].Raw = []byte(x.Format("2006-01-02 15:04:05.999999999 -07:00"))
			case bool:
				if x {
					row[i].Raw = []byte("1")
				} else {
					row[i].Raw = []byte("0")
				}
			default:
				row[i].Raw = []byte(fmt.Sprint(x))
			}
		}
		out = append(out, row)
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
	n, e := res.RowsAffected()
	if e != nil {
		return -1, nil
	}
	return n, nil
}
func (r *sqlRunner) Begin(ctx context.Context) error {
	if r.tx != nil {
		return errors.New("GBase 8s transaction already active")
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err == nil {
		r.tx = tx
	}
	return err
}
func (r *sqlRunner) Commit(ctx context.Context) error {
	if r.tx == nil {
		return errors.New("GBase 8s transaction not active")
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
func (r *sqlRunner) TxActive() bool { return r.tx != nil }
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
			return errors.New("GBase 8s TLS material was configured while TLS mode is DISABLE")
		}
		return nil
	case domain.TLSModePreferred, domain.TLSModeRequired:
		return fmt.Errorf("GBase 8s TLS mode %s is not qualified in RC19; configure vendor CSDK SSL separately only after retained qualification", mode)
	default:
		return fmt.Errorf("invalid GBase 8s TLS mode %q", ds.TLSMode)
	}
}

func odbcBraceValue(v string) string {
	return "{" + strings.ReplaceAll(v, "}", "}}") + "}"
}

func containsODBCSecret(raw string) bool {
	u := strings.ToUpper(raw)
	for _, key := range []string{"PWD=", "PASSWORD=", "UID=", "USER=", "USERNAME="} {
		if strings.Contains(u, key) {
			return true
		}
	}
	return false
}

func buildODBCConnectionString(ds domain.DataSource) (string, error) {
	raw := strings.TrimSpace(ds.JDBCURL)
	if strings.HasPrefix(strings.ToLower(raw), "odbc:") {
		raw = strings.TrimSpace(raw[len("odbc:"):])
	}
	if raw == "" {
		raw = strings.TrimSpace(os.Getenv("QMIGRATION_GBASE8S_ODBC_DSN"))
	}
	if raw == "" {
		return "", errors.New("GBase 8s ODBC DSN is required in datasource jdbc_url (optionally prefixed odbc:) or QMIGRATION_GBASE8S_ODBC_DSN; QMigration will not guess DBSERVERNAME/CSDK driver paths")
	}
	if containsODBCSecret(raw) {
		return "", errors.New("GBase 8s jdbc_url/ODBC DSN must not contain UID/PWD credentials; store username/password in the QMigration datasource so they remain encrypted at rest")
	}
	base := strings.TrimSuffix(strings.TrimSpace(raw), ";")
	if !strings.Contains(base, "=") && !strings.Contains(base, ";") {
		base = "DSN=" + odbcBraceValue(base)
	}
	if strings.TrimSpace(ds.Username) == "" {
		return "", errors.New("GBase 8s username is required")
	}
	base += ";UID=" + odbcBraceValue(ds.Username)
	base += ";PWD=" + odbcBraceValue(ds.Password)
	return base, nil
}

var openRunner = func(ds domain.DataSource) (runner, error) {
	if err := validateTransportSettings(ds); err != nil {
		return nil, err
	}
	if err := loadDriverPlugin(os.Getenv("QMIGRATION_GBASE8S_DRIVER_PLUGIN")); err != nil {
		return nil, err
	}
	driver := strings.TrimSpace(ds.DriverClass)
	if driver == "" {
		driver = strings.TrimSpace(os.Getenv("QMIGRATION_GBASE8S_SQL_DRIVER"))
	}
	if driver == "" {
		driver = "odbc"
	}
	found := false
	for _, d := range sql.Drivers() {
		if d == driver {
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("GBase 8s database/sql driver %q is not registered; load QMIGRATION_GBASE8S_DRIVER_PLUGIN or build a binary with the ODBC provider", driver)
	}
	dsn, err := buildODBCConnectionString(ds)
	if err != nil {
		return nil, err
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
	ds domain.DataSource
	mu sync.Mutex
	r  runner
}

func (c *Connector) get(ctx context.Context) (runner, error) {
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
	rows, err := r.Query(ctx, "SELECT DBINFO('version','full') FROM dual")
	if err != nil {
		return err
	}
	if len(rows) != 1 || len(rows[0]) != 1 || rows[0][0].Null || strings.TrimSpace(string(rows[0][0].Raw)) == "" {
		return errors.New("GBase 8s DBINFO version probe returned unexpected result")
	}
	return nil
}
func (c *Connector) GetVersion(ctx context.Context) (string, error) {
	if !experimentalEnabled() {
		return "gbase8s-tcp", nil
	}
	r, err := c.get(ctx)
	if err != nil {
		return "", err
	}
	rows, err := r.Query(ctx, "SELECT DBINFO('version','full') FROM dual")
	if err != nil {
		return "", err
	}
	if len(rows) == 0 || len(rows[0]) == 0 || rows[0][0].Null {
		return "GBase 8s", nil
	}
	return strings.TrimSpace(string(rows[0][0].Raw)), nil
}

var identRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$#]*$`)

func ident(s string) (string, error) {
	s = strings.TrimSpace(s)
	if !identRE.MatchString(s) {
		return "", fmt.Errorf("GBase 8s identifier %q is outside RC19 safe unquoted identifier subset", s)
	}
	return s, nil
}
func tableName(schema, table string) (string, error) {
	s, err := ident(schema)
	if err != nil {
		return "", err
	}
	t, err := ident(table)
	if err != nil {
		return "", err
	}
	return s + "." + t, nil
}
func cellString(row []connector.Value, i int) string {
	if i < 0 || i >= len(row) || row[i].Null {
		return ""
	}
	return strings.TrimSpace(string(row[i].Raw))
}
func cellInt(row []connector.Value, i int) int { n, _ := strconv.Atoi(cellString(row, i)); return n }
func cellInt64(row []connector.Value, i int) int64 {
	n, _ := strconv.ParseInt(cellString(row, i), 10, 64)
	return n
}
func absInt(v int) int {
	if v < 0 {
		return -v
	}
	return v
}

func (c *Connector) ListSchemas(ctx context.Context) ([]domain.SchemaInfo, error) {
	r, err := c.get(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := r.Query(ctx, `SELECT DISTINCT TRIM(owner) FROM systables WHERE tabid>=100 AND tabtype='T' ORDER BY 1`)
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
	if _, err := ident(schema); err != nil {
		return nil, err
	}
	r, err := c.get(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := r.Query(ctx, `SELECT TRIM(tabname),COALESCE(nrows,0) FROM systables WHERE tabid>=100 AND tabtype='T' AND TRIM(owner)=? ORDER BY tabname`, schema)
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

func baseDataType(typeName string) string {
	u := strings.ToUpper(strings.TrimSpace(typeName))
	switch {
	case strings.HasPrefix(u, "DATETIME"):
		return "datetime"
	case strings.HasPrefix(u, "INTERVAL"):
		return "interval"
	case strings.HasPrefix(u, "DOUBLE PRECISION"):
		return "double precision"
	}
	if i := strings.IndexAny(u, "( "); i >= 0 {
		u = u[:i]
	}
	return strings.ToLower(u)
}
func indexParts(row []connector.Value, start int, byNo map[int]string) []string {
	out := []string{}
	for i := start; i < len(row); i++ {
		n := absInt(cellInt(row, i))
		if n == 0 {
			continue
		}
		if name := byNo[n]; name != "" {
			out = append(out, name)
		}
	}
	return out
}
func numericType(t string) bool {
	switch strings.ToLower(strings.TrimSpace(t)) {
	case "smallint", "int", "integer", "bigint", "int8", "serial", "serial8", "bigserial", "decimal", "numeric", "money", "float", "smallfloat", "real", "double", "double precision":
		return true
	}
	return false
}

func (c *Connector) GetTableMetadata(ctx context.Context, schema, table string) (*domain.TableMetadata, error) {
	if _, err := tableName(schema, table); err != nil {
		return nil, err
	}
	r, err := c.get(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := r.Query(ctx, `SELECT ce.colno,TRIM(ce.colname),ce.coltype,ce.collength,TRIM(ce.coltypename) FROM syscolumnsext ce,systables t WHERE ce.tabid=t.tabid AND TRIM(t.owner)=? AND TRIM(t.tabname)=? ORDER BY ce.colno`, schema, table)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("GBase 8s table %s.%s not found or not visible", schema, table)
	}
	md := &domain.TableMetadata{Schema: schema, Name: table, Columns: make([]domain.ColumnInfo, 0, len(rows))}
	byNo := map[int]string{}
	for _, row := range rows {
		no := cellInt(row, 0)
		name := cellString(row, 1)
		coltype := cellInt(row, 2)
		ct := cellString(row, 4)
		if ct == "" {
			ct = fmt.Sprintf("TYPE_%d_LEN_%d", coltype%256, cellInt(row, 3))
		}
		dt := baseDataType(ct)
		extra := ""
		if dt == "serial" || dt == "serial8" || dt == "bigserial" {
			extra = "auto_increment"
		}
		col := domain.ColumnInfo{Name: name, DataType: dt, ColumnType: ct, Nullable: coltype < 256, Extra: extra, Ordinal: no}
		md.Columns = append(md.Columns, col)
		byNo[no] = name
	}
	pkRows, err := r.Query(ctx, `SELECT TRIM(sc.constrname),TRIM(si.idxname),si.part1,si.part2,si.part3,si.part4,si.part5,si.part6,si.part7,si.part8,si.part9,si.part10,si.part11,si.part12,si.part13,si.part14,si.part15,si.part16 FROM sysconstraints sc,sysindexes si,systables t WHERE sc.tabid=t.tabid AND sc.tabid=si.tabid AND sc.idxname=si.idxname AND sc.constrtype='P' AND TRIM(t.owner)=? AND TRIM(t.tabname)=?`, schema, table)
	if err != nil {
		return nil, err
	}
	pkIndex := ""
	if len(pkRows) > 0 {
		pkIndex = cellString(pkRows[0], 1)
		md.PrimaryKeys = indexParts(pkRows[0], 2, byNo)
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
	idxRows, err := r.Query(ctx, `SELECT TRIM(i.idxname),TRIM(i.idxtype),i.part1,i.part2,i.part3,i.part4,i.part5,i.part6,i.part7,i.part8,i.part9,i.part10,i.part11,i.part12,i.part13,i.part14,i.part15,i.part16 FROM sysindexes i,systables t WHERE i.tabid=t.tabid AND TRIM(t.owner)=? AND TRIM(t.tabname)=? ORDER BY i.idxname`, schema, table)
	if err != nil {
		return nil, err
	}
	for _, row := range idxRows {
		cols := indexParts(row, 2, byNo)
		if len(cols) == 0 {
			continue
		}
		name := cellString(row, 0)
		typ := strings.ToUpper(cellString(row, 1))
		md.Indexes = append(md.Indexes, domain.IndexInfo{Name: name, Columns: cols, Unique: strings.HasPrefix(typ, "U"), Primary: pkIndex != "" && strings.EqualFold(name, pkIndex)})
	}
	statRows, err := r.Query(ctx, `SELECT COALESCE(nrows,0) FROM systables WHERE tabid>=100 AND TRIM(owner)=? AND TRIM(tabname)=?`, schema, table)
	if err == nil && len(statRows) > 0 {
		md.EstimatedRows = cellInt64(statRows[0], 0)
		md.HasRows = md.EstimatedRows > 0
	}
	if len(md.PrimaryKeys) == 1 {
		var pkcol *domain.ColumnInfo
		for i := range md.Columns {
			if strings.EqualFold(md.Columns[i].Name, md.PrimaryKeys[0]) {
				pkcol = &md.Columns[i]
				break
			}
		}
		if pkcol != nil {
			md.PrimaryKeyType = pkcol.ColumnType
			md.PrimaryKeyNumeric = numericType(pkcol.DataType)
		}
		if md.PrimaryKeyNumeric {
			tn, _ := tableName(schema, table)
			pk, _ := ident(md.PrimaryKeys[0])
			mm, e := r.Query(ctx, `SELECT MIN(`+pk+`),MAX(`+pk+`) FROM `+tn)
			if e == nil && len(mm) > 0 && len(mm[0]) >= 2 && !mm[0][0].Null && !mm[0][1].Null {
				md.MinPK = cellInt64(mm[0], 0)
				md.MaxPK = cellInt64(mm[0], 1)
				md.HasRows = true
			}
		}
	}
	return md, nil
}

func keyCols(req connector.ReadBatchRequest) ([]domain.ColumnInfo, map[string]int, error) {
	keys := append([]string(nil), req.PrimaryKeys...)
	if len(keys) == 0 && req.PrimaryKey != "" {
		keys = []string{req.PrimaryKey}
	}
	by := map[string]domain.ColumnInfo{}
	ix := map[string]int{}
	for i, c := range req.Columns {
		by[strings.ToLower(c.Name)] = c
		ix[strings.ToLower(c.Name)] = i
	}
	out := make([]domain.ColumnInfo, len(keys))
	for i, k := range keys {
		v, ok := by[strings.ToLower(k)]
		if !ok {
			return nil, nil, fmt.Errorf("GBase 8s key column %s not selected", k)
		}
		out[i] = v
	}
	return out, ix, nil
}
func bindValue(col domain.ColumnInfo, v connector.Value) (any, error) {
	if v.Null {
		return nil, nil
	}
	dt := strings.ToLower(col.DataType)
	if strings.Contains(dt, "blob") || dt == "byte" || strings.Contains(dt, "binary") {
		return append([]byte(nil), v.Raw...), nil
	}
	return string(v.Raw), nil
}
func lexPredicate(keys []string, cols []domain.ColumnInfo, vals []connector.Value, op string, args *[]any) (string, error) {
	if len(vals) != len(keys) || len(cols) != len(keys) {
		return "", errors.New("GBase 8s keyset bound/key count mismatch")
	}
	ors := make([]string, 0, len(keys))
	for i := range keys {
		and := make([]string, 0, i+1)
		for j := 0; j < i; j++ {
			k, e := ident(keys[j])
			if e != nil {
				return "", e
			}
			v, e := bindValue(cols[j], vals[j])
			if e != nil {
				return "", e
			}
			and = append(and, k+"=?")
			*args = append(*args, v)
		}
		k, e := ident(keys[i])
		if e != nil {
			return "", e
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
		and = append(and, k+cmp+"?")
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
	tn, err := tableName(req.Schema, req.Table)
	if err != nil {
		return nil, err
	}
	sels := make([]string, len(req.Columns))
	for i, col := range req.Columns {
		s, er := ident(col.Name)
		if er != nil {
			return nil, er
		}
		sels[i] = s
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
			return nil, errors.New("GBase 8s keyset read requires migration key")
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
			return nil, errors.New("GBase 8s range read requires primary key")
		}
		k, er := ident(keys[0])
		if er != nil {
			return nil, er
		}
		cond = append(cond, k+">=?", k+"<=?")
		args = append(args, req.StartPK, req.EndPK)
		if req.HasAfter {
			cond = append(cond, k+">?")
			args = append(args, req.AfterPK)
		}
	}
	if strings.TrimSpace(req.CustomWhere) != "" {
		cond = append(cond, "("+strings.TrimSpace(req.CustomWhere)+")")
	}
	q := `SELECT FIRST ` + strconv.Itoa(req.Limit) + ` ` + strings.Join(sels, ",") + ` FROM ` + tn
	if len(cond) > 0 {
		q += ` WHERE ` + strings.Join(cond, " AND ")
	}
	if len(keys) > 0 {
		order := make([]string, len(keys))
		for i, k := range keys {
			o, er := ident(k)
			if er != nil {
				return nil, er
			}
			order[i] = o
		}
		q += ` ORDER BY ` + strings.Join(order, ",")
	}
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

func parseTypeArgs(ct string) []int {
	start := strings.Index(ct, "(")
	end := strings.LastIndex(ct, ")")
	if start < 0 || end <= start {
		return nil
	}
	parts := strings.Split(ct[start+1:end], ",")
	out := []int{}
	for _, p := range parts {
		n, e := strconv.Atoi(strings.TrimSpace(p))
		if e == nil {
			out = append(out, n)
		}
	}
	return out
}
func targetType(col domain.ColumnInfo) (string, error) {
	rawType := strings.TrimSpace(col.DataType)
	dt := baseDataType(rawType)
	if dt == "datetime" && strings.Contains(strings.ToLower(rawType), "hour to") {
		return "DATETIME HOUR TO FRACTION(5)", nil
	}
	ct := strings.ToUpper(strings.TrimSpace(col.ColumnType))
	if ct == "" {
		ct = strings.ToUpper(rawType)
	}
	args := parseTypeArgs(ct)
	switch dt {
	case "char", "character", "nchar":
		if len(args) > 0 && args[0] > 0 && args[0] <= 32739 {
			return fmt.Sprintf("CHAR(%d)", args[0]), nil
		}
		return "CHAR(255)", nil
	case "lvarchar":
		if len(args) > 0 && args[0] > 0 && args[0] <= 32739 {
			return fmt.Sprintf("LVARCHAR(%d)", args[0]), nil
		}
		return "LVARCHAR(32739)", nil
	case "varchar", "varchar2", "nvarchar", "nvarchar2", "string":
		if len(args) > 0 && args[0] > 0 && args[0] <= 32739 {
			return fmt.Sprintf("VARCHAR(%d)", args[0]), nil
		}
		return "LVARCHAR(32739)", nil
	case "smallint", "tinyint":
		return "SMALLINT", nil
	case "int", "integer", "serial":
		return "INTEGER", nil
	case "bigint", "int8", "serial8", "bigserial":
		return "BIGINT", nil
	case "decimal", "dec", "numeric", "number", "money":
		p, s := 32, 10
		if len(args) > 0 && args[0] > 0 {
			p = args[0]
			if p > 32 {
				p = 32
			}
		}
		if len(args) > 1 && args[1] >= 0 {
			s = args[1]
			if s > p {
				s = p
			}
		}
		return fmt.Sprintf("DECIMAL(%d,%d)", p, s), nil
	case "smallfloat", "real":
		return "SMALLFLOAT", nil
	case "float", "double", "double precision":
		return "FLOAT", nil
	case "bool", "boolean", "bit":
		return "BOOLEAN", nil
	case "date":
		return "DATE", nil
	case "time":
		return "DATETIME HOUR TO FRACTION(5)", nil
	case "datetime", "timestamp", "timestamp without time zone":
		return "DATETIME YEAR TO FRACTION(5)", nil
	case "byte", "bytea", "binary", "varbinary", "blob", "image":
		return "BLOB", nil
	case "text", "clob", "long", "json", "jsonb", "xml":
		return "CLOB", nil
	case "uuid":
		return "VARCHAR(36)", nil
	default:
		if strings.Contains(dt, "blob") || strings.Contains(dt, "binary") {
			return "BLOB", nil
		}
		if strings.Contains(dt, "char") || strings.Contains(dt, "text") {
			return "LVARCHAR(32739)", nil
		}
		return "", fmt.Errorf("unsupported GBase 8s target type %q for column %s", col.DataType, col.Name)
	}
}
func (c *Connector) EnsureSchema(ctx context.Context, schema string) error {
	s, err := ident(schema)
	if err != nil {
		return err
	}
	r, err := c.get(ctx)
	if err != nil {
		return err
	}
	rows, err := r.Query(ctx, `SELECT FIRST 1 TRIM(owner) FROM systables WHERE tabid>=100 AND TRIM(owner)=?`, s)
	if err != nil {
		return err
	}
	if len(rows) > 0 {
		return nil
	}
	if strings.EqualFold(strings.TrimSpace(c.ds.Username), s) {
		return nil
	}
	return fmt.Errorf("GBase 8s schema/owner %s is not visible; RC19 does not create database users implicitly, pre-create the target owner", s)
}

func (c *Connector) CreateTable(ctx context.Context, schema, table string, cols []domain.ColumnInfo, pk string) error {
	pks := []string{}
	if pk != "" {
		pks = []string{pk}
	}
	return c.CreateTableWithPrimaryKeys(ctx, schema, table, cols, pks)
}
func (c *Connector) CreateTableWithPrimaryKeys(ctx context.Context, schema, table string, cols []domain.ColumnInfo, pks []string) error {
	if err := c.EnsureSchema(ctx, schema); err != nil {
		return err
	}
	tn, err := tableName(schema, table)
	if err != nil {
		return err
	}
	r, err := c.get(ctx)
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return errors.New("no GBase 8s target columns")
	}
	defs := make([]string, 0, len(cols)+1)
	for _, col := range cols {
		name, e := ident(col.Name)
		if e != nil {
			return e
		}
		typ, e := targetType(col)
		if e != nil {
			return e
		}
		d := name + " " + typ
		if !col.Nullable {
			d += " NOT NULL"
		}
		defs = append(defs, d)
	}
	if len(pks) > 0 {
		q := make([]string, len(pks))
		for i, k := range pks {
			x, e := ident(k)
			if e != nil {
				return e
			}
			q[i] = x
		}
		defs = append(defs, "PRIMARY KEY ("+strings.Join(q, ",")+")")
	}
	_, err = r.Exec(ctx, `CREATE TABLE `+tn+` (`+strings.Join(defs, ",")+`)`)
	return err
}
func (c *Connector) CreateIndex(ctx context.Context, schema, table string, idx domain.IndexInfo) error {
	if idx.Primary {
		return nil
	}
	tn, err := tableName(schema, table)
	if err != nil {
		return err
	}
	name, err := ident(idx.Name)
	if err != nil {
		return err
	}
	cols := make([]string, len(idx.Columns))
	for i, x := range idx.Columns {
		cols[i], err = ident(x)
		if err != nil {
			return err
		}
	}
	unique := ""
	if idx.Unique {
		unique = "UNIQUE "
	}
	r, err := c.get(ctx)
	if err != nil {
		return err
	}
	_, err = r.Exec(ctx, "CREATE "+unique+"INDEX "+name+" ON "+tn+" ("+strings.Join(cols, ",")+")")
	return err
}

func (c *Connector) CreateForeignKey(ctx context.Context, schema, table string, fk domain.ForeignKeyInfo) error {
	tn, err := tableName(schema, table)
	if err != nil {
		return err
	}
	rn, err := tableName(fk.ReferencedSchema, fk.ReferencedTable)
	if err != nil {
		return err
	}
	name, err := ident(fk.Name)
	if err != nil {
		return err
	}
	if len(fk.Columns) == 0 || len(fk.Columns) != len(fk.ReferencedColumns) {
		return errors.New("invalid GBase 8s foreign key")
	}
	cols := make([]string, len(fk.Columns))
	refs := make([]string, len(fk.ReferencedColumns))
	for i := range fk.Columns {
		cols[i], err = ident(fk.Columns[i])
		if err != nil {
			return err
		}
		refs[i], err = ident(fk.ReferencedColumns[i])
		if err != nil {
			return err
		}
	}
	r, err := c.get(ctx)
	if err != nil {
		return err
	}
	// GBase 8s follows the Informix-compatible named FK form where the
	// constraint name is placed after the REFERENCES clause.
	_, err = r.Exec(ctx, "ALTER TABLE "+tn+" ADD CONSTRAINT FOREIGN KEY ("+strings.Join(cols, ",")+") REFERENCES "+rn+" ("+strings.Join(refs, ",")+") CONSTRAINT "+name)
	return err
}

func (c *Connector) DropQualificationTable(ctx context.Context, schema, table string) error {
	tn, err := tableName(schema, table)
	if err != nil {
		return err
	}
	r, err := c.get(ctx)
	if err != nil {
		return err
	}
	_, err = r.Exec(ctx, "DROP TABLE "+tn)
	return err
}

func keySpec(req connector.WriteBatchRequest) ([]string, map[string]domain.ColumnInfo, error) {
	keys := append([]string(nil), req.PrimaryKeys...)
	if len(keys) == 0 && req.PrimaryKey != "" {
		keys = []string{req.PrimaryKey}
	}
	if len(keys) == 0 {
		return nil, nil, errors.New("GBase 8s Full/CDC target write requires a stable migration key")
	}
	by := make(map[string]domain.ColumnInfo, len(req.Columns))
	for _, col := range req.Columns {
		by[strings.ToLower(col.Name)] = col
	}
	for _, key := range keys {
		if _, ok := by[strings.ToLower(key)]; !ok {
			return nil, nil, fmt.Errorf("GBase 8s key column %s not in target columns", key)
		}
	}
	return keys, by, nil
}

func preparedValues(cols []domain.ColumnInfo, row []connector.Value) ([]any, error) {
	if len(cols) != len(row) {
		return nil, errors.New("column/value count mismatch")
	}
	out := make([]any, len(row))
	for i := range row {
		v, err := bindValue(cols[i], row[i])
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", cols[i].Name, err)
		}
		out[i] = v
	}
	return out, nil
}

func columnIndex(cols []domain.ColumnInfo, name string) int {
	for i, col := range cols {
		if strings.EqualFold(col.Name, name) {
			return i
		}
	}
	return -1
}

func (c *Connector) WriteBatch(ctx context.Context, req connector.WriteBatchRequest) (int64, error) {
	r, err := c.get(ctx)
	if err != nil {
		return 0, err
	}
	if len(req.Columns) == 0 {
		return 0, errors.New("no target columns")
	}
	tn, err := tableName(req.Schema, req.Table)
	if err != nil {
		return 0, err
	}
	keys, by, err := keySpec(req)
	if err != nil {
		return 0, err
	}

	names := make([]string, len(req.Columns))
	for i, col := range req.Columns {
		names[i], err = ident(col.Name)
		if err != nil {
			return 0, err
		}
	}
	keySet := make(map[string]bool, len(keys))
	for _, key := range keys {
		keySet[strings.ToLower(key)] = true
	}
	sets := make([]string, 0, len(req.Columns))
	for _, col := range req.Columns {
		if keySet[strings.ToLower(col.Name)] {
			continue
		}
		name, _ := ident(col.Name)
		sets = append(sets, name+"=?")
	}
	cond := make([]string, len(keys))
	for i, key := range keys {
		name, _ := ident(key)
		cond[i] = name + "=?"
	}
	placeholders := make([]string, len(names))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	insertSQL := "INSERT INTO " + tn + " (" + strings.Join(names, ",") + ") VALUES (" + strings.Join(placeholders, ",") + ")"
	updateSQL := ""
	if len(sets) > 0 {
		updateSQL = "UPDATE " + tn + " SET " + strings.Join(sets, ",") + " WHERE " + strings.Join(cond, " AND ")
	}
	existsSQL := "SELECT FIRST 1 1 FROM " + tn + " WHERE " + strings.Join(cond, " AND ")

	ownTx := !r.TxActive()
	if ownTx {
		if err := r.Begin(ctx); err != nil {
			return 0, err
		}
	}
	rollback := func() {
		if ownTx {
			_ = r.Rollback(ctx)
		}
	}

	var total int64
	for _, row := range req.Rows {
		args, err := preparedValues(req.Columns, row)
		if err != nil {
			rollback()
			return total, err
		}
		keyArgs := make([]any, len(keys))
		for i, key := range keys {
			idx := columnIndex(req.Columns, key)
			if idx < 0 {
				rollback()
				return total, fmt.Errorf("key %s missing", key)
			}
			keyArgs[i], err = bindValue(by[strings.ToLower(key)], row[idx])
			if err != nil {
				rollback()
				return total, err
			}
		}

		updated := false
		if updateSQL != "" {
			uargs := make([]any, 0, len(args))
			for i, col := range req.Columns {
				if !keySet[strings.ToLower(col.Name)] {
					uargs = append(uargs, args[i])
				}
			}
			uargs = append(uargs, keyArgs...)
			n, err := r.Exec(ctx, updateSQL, uargs...)
			if err != nil {
				rollback()
				return total, err
			}
			updated = n > 0
		}
		// ODBC drivers are allowed to report an unknown/zero affected-row count.
		// Confirm existence before deciding to INSERT, keeping retries idempotent.
		if !updated {
			found, err := r.Query(ctx, existsSQL, keyArgs...)
			if err != nil {
				rollback()
				return total, err
			}
			if len(found) == 0 {
				if _, err := r.Exec(ctx, insertSQL, args...); err != nil {
					rollback()
					return total, err
				}
			}
		}
		total++
	}
	if ownTx {
		if err := r.Commit(ctx); err != nil {
			return total, err
		}
	}
	return total, nil
}

func keyDeleteParts(req connector.DeleteByKeyRequest) ([]string, []domain.ColumnInfo, []connector.Value, error) {
	keys := append([]string(nil), req.PrimaryKeys...)
	cols := append([]domain.ColumnInfo(nil), req.Columns...)
	vals := append([]connector.Value(nil), req.Values...)
	if len(keys) == 0 && req.PrimaryKey != "" {
		keys = []string{req.PrimaryKey}
		cols = []domain.ColumnInfo{req.Column}
		vals = []connector.Value{req.Value}
	}
	if len(keys) == 0 || len(keys) != len(cols) || len(keys) != len(vals) {
		return nil, nil, nil, errors.New("invalid GBase 8s delete key")
	}
	return keys, cols, vals, nil
}

func (c *Connector) TruncateTable(ctx context.Context, schema, table string) error {
	tn, err := tableName(schema, table)
	if err != nil {
		return err
	}
	r, err := c.get(ctx)
	if err != nil {
		return err
	}
	if !r.TxActive() {
		return errors.New("GBase 8s CDC TRUNCATE requires an active target transaction")
	}
	_, err = r.Exec(ctx, "TRUNCATE TABLE "+tn)
	return err
}

func (c *Connector) DeleteByKey(ctx context.Context, req connector.DeleteByKeyRequest) error {
	keys, cols, vals, err := keyDeleteParts(req)
	if err != nil {
		return err
	}
	tn, err := tableName(req.Schema, req.Table)
	if err != nil {
		return err
	}
	cond := make([]string, len(keys))
	args := make([]any, len(keys))
	for i, key := range keys {
		cond[i], err = ident(key)
		if err != nil {
			return err
		}
		cond[i] += "=?"
		args[i], err = bindValue(cols[i], vals[i])
		if err != nil {
			return err
		}
	}
	r, err := c.get(ctx)
	if err != nil {
		return err
	}
	_, err = r.Exec(ctx, "DELETE FROM "+tn+" WHERE "+strings.Join(cond, " AND "), args...)
	return err
}

func (c *Connector) ReadByKey(ctx context.Context, req connector.ReadByKeyRequest) ([]connector.Value, bool, error) {
	if len(req.PrimaryKeys) == 0 || len(req.PrimaryKeys) != len(req.KeyValues) || len(req.PrimaryKeys) != len(req.KeyColumns) {
		return nil, false, errors.New("invalid GBase 8s point lookup key")
	}
	tn, err := tableName(req.Schema, req.Table)
	if err != nil {
		return nil, false, err
	}
	selects := make([]string, len(req.Columns))
	for i, col := range req.Columns {
		selects[i], err = ident(col.Name)
		if err != nil {
			return nil, false, err
		}
	}
	cond := make([]string, len(req.PrimaryKeys))
	args := make([]any, len(req.PrimaryKeys))
	for i, key := range req.PrimaryKeys {
		name, err := ident(key)
		if err != nil {
			return nil, false, err
		}
		cond[i] = name + "=?"
		args[i], err = bindValue(req.KeyColumns[i], req.KeyValues[i])
		if err != nil {
			return nil, false, err
		}
	}
	r, err := c.get(ctx)
	if err != nil {
		return nil, false, err
	}
	rows, err := r.Query(ctx, "SELECT FIRST 1 "+strings.Join(selects, ",")+" FROM "+tn+" WHERE "+strings.Join(cond, " AND "), args...)
	if err != nil {
		return nil, false, err
	}
	if len(rows) == 0 {
		return nil, false, nil
	}
	return rows[0], true, nil
}

func (c *Connector) BeginCDCTransaction(ctx context.Context) error {
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
	if req.Partitions <= 1 || len(req.Keys) == 0 {
		return nil, nil
	}
	tn, err := tableName(req.Schema, req.Table)
	if err != nil {
		return nil, err
	}
	keys := make([]string, len(req.Keys))
	for i, key := range req.Keys {
		keys[i], err = ident(key)
		if err != nil {
			return nil, err
		}
	}
	keyList := strings.Join(keys, ",")
	order := strings.Join(keys, ",")
	inner := "SELECT " + keyList + ",NTILE(" + strconv.Itoa(req.Partitions) + ") OVER (ORDER BY " + order + ") b FROM " + tn
	outer := "SELECT x.*,ROW_NUMBER() OVER (PARTITION BY b ORDER BY " + order + ") rn FROM (" + inner + ") x"
	query := "SELECT FIRST " + strconv.Itoa(req.Partitions-1) + " " + keyList + " FROM (" + outer + ") y WHERE b>1 AND rn=1 ORDER BY b"
	r, err := c.get(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := r.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	out := make([][]connector.Value, 0, len(rows))
	for _, row := range rows {
		if len(row) < len(keys) {
			continue
		}
		value := make([]connector.Value, len(keys))
		copy(value, row[:len(keys)])
		out = append(out, value)
	}
	return out, nil
}

func (c *Connector) MigrationPrechecks(ctx context.Context, needCDC bool) []domain.PrecheckItem {
	if !experimentalEnabled() {
		return []domain.PrecheckItem{{Name: "gbase8s_native_gate", Level: domain.PrecheckFailed, Message: "set QMIGRATION_EXPERIMENTAL_GBASE8S_NATIVE=1 after retained qualification"}}
	}
	r, err := c.get(ctx)
	if err != nil {
		return []domain.PrecheckItem{{Name: "gbase8s_odbc_provider", Level: domain.PrecheckFailed, Message: err.Error()}}
	}
	items := []domain.PrecheckItem{{Name: "gbase8s_odbc_provider", Level: domain.PrecheckPass, Message: "GBase Client-SDK ODBC provider is registered"}}
	if version, err := c.GetVersion(ctx); err != nil {
		items = append(items, domain.PrecheckItem{Name: "gbase8s_version", Level: domain.PrecheckWarning, Message: err.Error()})
	} else {
		items = append(items, domain.PrecheckItem{Name: "gbase8s_version", Level: domain.PrecheckPass, Message: version})
	}
	if database := strings.TrimSpace(c.ds.Database); database != "" {
		rows, err := r.Query(ctx, `SELECT name,is_logging,is_buff_log,is_ansi FROM sysmaster:sysdatabases WHERE TRIM(name)=?`, database)
		if err == nil && len(rows) > 0 {
			logged := cellInt(rows[0], 1)+cellInt(rows[0], 2) > 0 || cellInt(rows[0], 3) > 0
			level := domain.PrecheckPass
			message := "database logging is enabled"
			if !logged {
				level = domain.PrecheckWarning
				message = "database is NO LOG; Full Load is available but source CDC/logical recovery prerequisites are not"
			}
			items = append(items, domain.PrecheckItem{Name: "gbase8s_database_logging", Level: level, Message: message})
		}
	}
	items = append(items, domain.PrecheckItem{Name: "gbase8s_identifier_policy", Level: domain.PrecheckWarning, Message: "RC21 supports the safe unquoted identifier subset [A-Za-z_][A-Za-z0-9_$#]*; quoted/case-sensitive identifiers require later qualification"})
	if needCDC {
		if !experimentalCDCEnabled() {
			items = append(items, domain.PrecheckItem{Name: "gbase8s_source_cdc", Level: domain.PrecheckFailed, Message: "set QMIGRATION_EXPERIMENTAL_GBASE8S_CDC=1 and configure datasource cdc_url for the CSDK CDC provider"})
		} else if a, e := c.cdcAgent(); e != nil {
			items = append(items, domain.PrecheckItem{Name: "gbase8s_source_cdc", Level: domain.PrecheckFailed, Message: e.Error()})
		} else if e = a.Health(ctx); e != nil {
			items = append(items, domain.PrecheckItem{Name: "gbase8s_source_cdc", Level: domain.PrecheckFailed, Message: "CSDK CDC provider health failed: " + e.Error()})
		} else {
			items = append(items, domain.PrecheckItem{Name: "gbase8s_source_cdc", Level: domain.PrecheckPass, Message: "syscdcv1/CSDK CDC provider reachable; selected-table full-row logging/type validation runs before Full"})
		}
	}
	return items
}

var _ connector.DataConnector = (*Connector)(nil)
var _ connector.KeysetBoundaryConnector = (*Connector)(nil)
var _ connector.CompositeSchemaConnector = (*Connector)(nil)
var _ connector.PostLoadSchemaConnector = (*Connector)(nil)
var _ connector.TransactionalCDCApplyConnector = (*Connector)(nil)
var _ connector.TruncateTableConnector = (*Connector)(nil)
var _ connector.PointLookupConnector = (*Connector)(nil)
var _ connector.MigrationPrecheckConnector = (*Connector)(nil)
