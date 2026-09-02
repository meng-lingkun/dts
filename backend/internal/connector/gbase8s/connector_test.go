package gbase8sconnector

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
)

type fakeRunner struct {
	queries []string
	execs   []string
	args    [][]any
	queryFn func(string, []any) ([][]connector.Value, error)
	execFn  func(string, []any) (int64, error)
	begins  int
	commits int
	rolls   int
	active  bool
}

func (f *fakeRunner) Ping(context.Context) error { return nil }
func (f *fakeRunner) Query(_ context.Context, q string, args ...any) ([][]connector.Value, error) {
	f.queries = append(f.queries, q)
	f.args = append(f.args, append([]any(nil), args...))
	if f.queryFn != nil {
		return f.queryFn(q, args)
	}
	return nil, nil
}
func (f *fakeRunner) Exec(_ context.Context, q string, args ...any) (int64, error) {
	f.execs = append(f.execs, q)
	f.args = append(f.args, append([]any(nil), args...))
	if f.execFn != nil {
		return f.execFn(q, args)
	}
	return 1, nil
}
func (f *fakeRunner) Begin(context.Context) error {
	if f.active {
		return errors.New("already active")
	}
	f.active = true
	f.begins++
	return nil
}
func (f *fakeRunner) Commit(context.Context) error {
	if !f.active {
		return errors.New("not active")
	}
	f.active = false
	f.commits++
	return nil
}
func (f *fakeRunner) Rollback(context.Context) error {
	f.active = false
	f.rolls++
	return nil
}
func (f *fakeRunner) TxActive() bool { return f.active }
func (f *fakeRunner) Close() error   { return nil }

func v(s string) connector.Value { return connector.Value{Raw: []byte(s)} }
func nullv() connector.Value     { return connector.Value{Null: true} }

func TestDescriptorGateSeparatesGBase8sFrom8aAndSourceCDC(t *testing.T) {
	t.Setenv("QMIGRATION_EXPERIMENTAL_GBASE8S_NATIVE", "")
	d := NewFactory().Capabilities(domain.DataSourceGBase8s)
	if d.Protocol != "gbase8s-odbc" || d.Has(connector.CapabilityFullRead) || d.Has(connector.CapabilityCDCRead) {
		t.Fatalf("unexpected ungated descriptor: %+v", d)
	}
	t.Setenv("QMIGRATION_EXPERIMENTAL_GBASE8S_NATIVE", "1")
	d = NewFactory().Capabilities(domain.DataSourceGBase8s)
	for _, cap := range []connector.Capability{
		connector.CapabilityMetadata,
		connector.CapabilityFullRead,
		connector.CapabilityFullWrite,
		connector.CapabilityCDCApply,
		connector.CapabilityCDCTransactional,
	} {
		if !d.Has(cap) {
			t.Fatalf("missing %s in %+v", cap, d)
		}
	}
	if d.Has(connector.CapabilityCDCRead) || d.Has(connector.CapabilityCDCPosition) || d.Has(connector.CapabilityCDCCheckpoint) {
		t.Fatalf("RC19 must not advertise GBase 8s source CDC: %+v", d)
	}
}

func TestSafeIdentifierPolicy(t *testing.T) {
	for _, good := range []string{"app", "T_1", "A$B", "A#B"} {
		if _, err := ident(good); err != nil {
			t.Fatalf("%q should be allowed: %v", good, err)
		}
	}
	for _, bad := range []string{"a.b", `"Mixed"`, "a b", "a;drop"} {
		if _, err := ident(bad); err == nil {
			t.Fatalf("%q should be rejected", bad)
		}
	}
}

func TestTargetTypeMappings(t *testing.T) {
	cases := []struct {
		col  domain.ColumnInfo
		want string
	}{
		{domain.ColumnInfo{Name: "v", DataType: "varchar", ColumnType: "VARCHAR(128)"}, "VARCHAR(128)"},
		{domain.ColumnInfo{Name: "d", DataType: "decimal", ColumnType: "DECIMAL(38,12)"}, "DECIMAL(32,12)"},
		{domain.ColumnInfo{Name: "b", DataType: "byte", ColumnType: "BYTE"}, "BLOB"},
		{domain.ColumnInfo{Name: "ts", DataType: "datetime", ColumnType: "DATETIME YEAR TO FRACTION(5)"}, "DATETIME YEAR TO FRACTION(5)"},
	}
	for _, tc := range cases {
		got, err := targetType(tc.col)
		if err != nil || got != tc.want {
			t.Fatalf("%s: got %q err=%v want=%q", tc.col.Name, got, err, tc.want)
		}
	}
}

func TestMetadataCatalogCompositePKAndIndexes(t *testing.T) {
	fr := &fakeRunner{}
	fr.queryFn = func(q string, _ []any) ([][]connector.Value, error) {
		switch {
		case strings.Contains(q, "FROM syscolumnsext"):
			return [][]connector.Value{
				{v("1"), v("id"), v("262"), v("8"), v("BIGINT")},           // 262 => NOT NULL
				{v("2"), v("tenant"), v("269"), v("32"), v("VARCHAR(32)")}, // NOT NULL
				{v("3"), v("payload"), v("13"), v("256"), v("LVARCHAR(256)")},
			}, nil
		case strings.Contains(q, "sc.constrtype='P'"):
			row := []connector.Value{v("pk_orders"), v("idx_pk"), v("1"), v("2")}
			for len(row) < 18 {
				row = append(row, v("0"))
			}
			return [][]connector.Value{row}, nil
		case strings.Contains(q, "FROM sysindexes i"):
			pk := []connector.Value{v("idx_pk"), v("U"), v("1"), v("2")}
			ix := []connector.Value{v("idx_payload"), v("D"), v("3")}
			for len(pk) < 18 {
				pk = append(pk, v("0"))
			}
			for len(ix) < 18 {
				ix = append(ix, v("0"))
			}
			return [][]connector.Value{pk, ix}, nil
		case strings.Contains(q, "COALESCE(nrows,0)"):
			return [][]connector.Value{{v("42")}}, nil
		default:
			return nil, nil
		}
	}
	c := &Connector{ds: domain.DataSource{Type: domain.DataSourceGBase8s}, r: fr}
	md, err := c.GetTableMetadata(context.Background(), "app", "orders")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(md.PrimaryKeys, ",") != "id,tenant" || md.PrimaryKey != "id" {
		t.Fatalf("pk=%v primary=%s", md.PrimaryKeys, md.PrimaryKey)
	}
	if md.Columns[0].Nullable || md.Columns[1].Nullable || !md.Columns[2].Nullable {
		t.Fatalf("nullable parsing wrong: %+v", md.Columns)
	}
	if len(md.Indexes) != 2 || !md.Indexes[0].Primary || !md.Indexes[0].Unique {
		t.Fatalf("indexes=%+v", md.Indexes)
	}
	if md.EstimatedRows != 42 {
		t.Fatalf("estimated rows=%d", md.EstimatedRows)
	}
}

func TestCompositeKeysetReadPreservesCursor(t *testing.T) {
	fr := &fakeRunner{}
	fr.queryFn = func(q string, args []any) ([][]connector.Value, error) {
		if !strings.Contains(q, "SELECT FIRST 10") || !strings.Contains(q, "ORDER BY id,tenant") {
			t.Fatalf("query=%s", q)
		}
		if !strings.Contains(q, "(id>? OR") && !strings.Contains(q, "(id>?") {
			t.Fatalf("missing lexicographic cursor predicate: %s", q)
		}
		if len(args) != 3 { // id>? OR (id=? AND tenant>?)
			t.Fatalf("args=%v", args)
		}
		return [][]connector.Value{{v("2"), v("b"), v("x")}}, nil
	}
	c := &Connector{ds: domain.DataSource{Type: domain.DataSourceGBase8s}, r: fr}
	cols := []domain.ColumnInfo{{Name: "id", DataType: "bigint"}, {Name: "tenant", DataType: "varchar"}, {Name: "payload", DataType: "varchar"}}
	batch, err := c.ReadBatch(context.Background(), connector.ReadBatchRequest{
		Schema: "app", Table: "orders", Columns: cols,
		PrimaryKeys: []string{"id", "tenant"}, UseKeyset: true,
		Cursor: []connector.Value{v("1"), v("a")}, Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.LastKey) != 2 || string(batch.LastKey[0].Raw) != "2" || string(batch.LastKey[1].Raw) != "b" {
		t.Fatalf("last key=%+v", batch.LastKey)
	}
}

func TestWriteBatchAffectedRowsZeroConfirmsExistenceBeforeInsert(t *testing.T) {
	fr := &fakeRunner{}
	exists := false
	fr.execFn = func(q string, _ []any) (int64, error) {
		switch {
		case strings.HasPrefix(q, "UPDATE "):
			return 0, nil // emulate ODBC unknown/zero affected row count
		case strings.HasPrefix(q, "INSERT "):
			if exists {
				t.Fatal("duplicate INSERT attempted")
			}
			exists = true
			return 1, nil
		default:
			return 1, nil
		}
	}
	fr.queryFn = func(q string, _ []any) ([][]connector.Value, error) {
		if strings.HasPrefix(q, "SELECT FIRST 1 1") && exists {
			return [][]connector.Value{{v("1")}}, nil
		}
		return nil, nil
	}
	c := &Connector{ds: domain.DataSource{Type: domain.DataSourceGBase8s}, r: fr}
	cols := []domain.ColumnInfo{{Name: "id", DataType: "bigint"}, {Name: "payload", DataType: "varchar"}}
	for _, payload := range []string{"a", "b"} {
		if _, err := c.WriteBatch(context.Background(), connector.WriteBatchRequest{
			Schema: "app", Table: "orders", Columns: cols, PrimaryKeys: []string{"id"},
			Rows: [][]connector.Value{{v("1"), v(payload)}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	inserts := 0
	for _, q := range fr.execs {
		if strings.HasPrefix(q, "INSERT ") {
			inserts++
		}
	}
	if inserts != 1 || fr.begins != 2 || fr.commits != 2 || fr.rolls != 0 {
		t.Fatalf("inserts=%d begin=%d commit=%d rollback=%d execs=%v", inserts, fr.begins, fr.commits, fr.rolls, fr.execs)
	}
}

func TestWriteBatchRejectsKeyless(t *testing.T) {
	c := &Connector{r: &fakeRunner{}}
	_, err := c.WriteBatch(context.Background(), connector.WriteBatchRequest{
		Schema: "app", Table: "t", Columns: []domain.ColumnInfo{{Name: "v", DataType: "varchar"}}, Rows: [][]connector.Value{{v("x")}},
	})
	if err == nil || !strings.Contains(err.Error(), "stable migration key") {
		t.Fatalf("err=%v", err)
	}
}

func TestTargetTransactionalTruncate(t *testing.T) {
	fr := &fakeRunner{}
	c := &Connector{r: fr}
	if err := c.TruncateTable(context.Background(), "app", "orders"); err == nil || !strings.Contains(err.Error(), "active target transaction") {
		t.Fatalf("truncate outside tx err=%v", err)
	}
	if err := c.BeginCDCTransaction(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := c.TruncateTable(context.Background(), "app", "orders"); err != nil {
		t.Fatal(err)
	}
	if len(fr.execs) != 1 || fr.execs[0] != "TRUNCATE TABLE app.orders" {
		t.Fatalf("execs=%v", fr.execs)
	}
	if err := c.CommitCDCTransaction(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestTargetCDCTransactionAndBinaryDelete(t *testing.T) {
	fr := &fakeRunner{}
	c := &Connector{r: fr}
	if err := c.BeginCDCTransaction(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !fr.active {
		t.Fatal("transaction not active")
	}
	if err := c.DeleteByKey(context.Background(), connector.DeleteByKeyRequest{
		Schema: "app", Table: "t", PrimaryKeys: []string{"id"},
		Columns: []domain.ColumnInfo{{Name: "id", DataType: "byte"}}, Values: []connector.Value{{Raw: []byte{0, 1, 2}}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.CommitCDCTransaction(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fr.commits != 1 || fr.active {
		t.Fatalf("commit state %+v", fr)
	}
}

func TestPrecheckRejectsSourceCDC(t *testing.T) {
	t.Setenv("QMIGRATION_EXPERIMENTAL_GBASE8S_NATIVE", "1")
	fr := &fakeRunner{}
	fr.queryFn = func(q string, _ []any) ([][]connector.Value, error) {
		if strings.Contains(q, "DBINFO") {
			return [][]connector.Value{{v("GBase Database Server Version 8.8")}}, nil
		}
		if strings.Contains(q, "sysmaster:sysdatabases") {
			return [][]connector.Value{{v("app"), v("1"), v("0"), v("0")}}, nil
		}
		return nil, nil
	}
	c := &Connector{ds: domain.DataSource{Database: "app"}, r: fr}
	items := c.MigrationPrechecks(context.Background(), true)
	found := false
	for _, it := range items {
		if it.Name == "gbase8s_source_cdc" {
			found = true
			if it.Level != domain.PrecheckFailed {
				t.Fatalf("item=%+v", it)
			}
		}
	}
	if !found {
		t.Fatalf("items=%+v", items)
	}
}

func TestValidateTransportSettingsFailsClosedOnTLS(t *testing.T) {
	if err := validateTransportSettings(domain.DataSource{TLSMode: domain.TLSModeRequired}); err == nil {
		t.Fatal("expected required TLS to fail closed until CSDK SSL mapping is qualified")
	}
	// avoid accidental dependency on developer environment
	_ = os.Unsetenv("QMIGRATION_GBASE8S_DRIVER_PLUGIN")
}

func TestNullValueHelper(t *testing.T) {
	if !nullv().Null {
		t.Fatal("test helper")
	}
}

func TestCreateForeignKeyUsesInformixCompatibleNamedConstraint(t *testing.T) {
	fr := &fakeRunner{}
	c := &Connector{r: fr}
	err := c.CreateForeignKey(context.Background(), "app", "child", domain.ForeignKeyInfo{
		Name: "fk_child_parent", Columns: []string{"parent_id"}, ReferencedSchema: "app", ReferencedTable: "parent", ReferencedColumns: []string{"id"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(fr.execs) != 1 || fr.execs[0] != "ALTER TABLE app.child ADD CONSTRAINT FOREIGN KEY (parent_id) REFERENCES app.parent (id) CONSTRAINT fk_child_parent" {
		t.Fatalf("execs=%v", fr.execs)
	}
}

func TestBuildODBCConnectionStringKeepsSecretsOutOfPersistedDSN(t *testing.T) {
	ds := domain.DataSource{JDBCURL: "odbc:GBASE8S_APP", Username: "app", Password: "p;a}ss"}
	got, err := buildODBCConnectionString(ds)
	if err != nil {
		t.Fatal(err)
	}
	if got != "DSN={GBASE8S_APP};UID={app};PWD={p;a}}ss}" {
		t.Fatalf("dsn=%q", got)
	}
	if _, err := buildODBCConnectionString(domain.DataSource{JDBCURL: "DSN=x;PWD=plain", Username: "u", Password: "p"}); err == nil {
		t.Fatal("persisted ODBC password should be rejected")
	}
}

func TestTargetTypeAcceptsUniversalConvertedDeclarations(t *testing.T) {
	cases := []struct{ dt, want string }{
		{"varchar(128)", "VARCHAR(128)"},
		{"lvarchar(32739)", "LVARCHAR(32739)"},
		{"datetime hour to fraction(5)", "DATETIME HOUR TO FRACTION(5)"},
		{"datetime year to fraction(5)", "DATETIME YEAR TO FRACTION(5)"},
	}
	for _, tc := range cases {
		got, err := targetType(domain.ColumnInfo{Name: "c", DataType: tc.dt, ColumnType: tc.dt})
		if err != nil || got != tc.want {
			t.Fatalf("%q got=%q err=%v want=%q", tc.dt, got, err, tc.want)
		}
	}
}

func TestSmartLOBCDCTypePolicy(t *testing.T) {
	if !smartLOBCDCType(domain.ColumnInfo{Name: "b", DataType: "blob", ColumnType: "BLOB"}) || !smartLOBCDCType(domain.ColumnInfo{Name: "c", DataType: "clob", ColumnType: "CLOB"}) {
		t.Fatal("BLOB/CLOB must be recognized as smart-LOB CDC types")
	}
	if unsupportedCDCType(domain.ColumnInfo{Name: "b", DataType: "blob", ColumnType: "BLOB"}) {
		t.Fatal("RC28 smart BLOB must not be rejected by the generic complex-type gate")
	}
	for _, col := range []domain.ColumnInfo{
		{Name: "t", DataType: "text", ColumnType: "TEXT"},
		{Name: "b", DataType: "byte", ColumnType: "BYTE"},
		{Name: "x", DataType: "opaque", ColumnType: "OPAQUE"},
	} {
		if !unsupportedCDCType(col) {
			t.Fatalf("%s should remain fail-closed", col.ColumnType)
		}
	}
}
