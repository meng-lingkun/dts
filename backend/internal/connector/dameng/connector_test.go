package damengconnector

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
)

type fakeCall struct {
	SQL  string
	Args []any
}

type fakeRunner struct {
	queries []fakeCall
	execs   []fakeCall
	begins  int
	commits int
	rolls   int
	queryFn func(string, []any) ([][]connector.Value, error)
}

func (f *fakeRunner) Ping(context.Context) error { return nil }
func (f *fakeRunner) Query(_ context.Context, q string, args ...any) ([][]connector.Value, error) {
	f.queries = append(f.queries, fakeCall{SQL: q, Args: append([]any(nil), args...)})
	if f.queryFn != nil {
		return f.queryFn(q, args)
	}
	return nil, nil
}
func (f *fakeRunner) Exec(_ context.Context, q string, args ...any) (int64, error) {
	f.execs = append(f.execs, fakeCall{SQL: q, Args: append([]any(nil), args...)})
	return 1, nil
}
func (f *fakeRunner) Begin(context.Context) error    { f.begins++; return nil }
func (f *fakeRunner) Commit(context.Context) error   { f.commits++; return nil }
func (f *fakeRunner) Rollback(context.Context) error { f.rolls++; return nil }
func (f *fakeRunner) Close() error                   { return nil }

func cv(s string) connector.Value { return connector.Value{Raw: []byte(s)} }
func nullv() connector.Value      { return connector.Value{Null: true} }

func TestDamengCapabilitiesAreGatedAndSourceCDCIsNeverAdvertised(t *testing.T) {
	f := NewFactory()
	t.Setenv("QMIGRATION_EXPERIMENTAL_DAMENG_NATIVE", "")
	d := f.Capabilities(domain.DataSourceDameng)
	if !d.Has(connector.CapabilityProtocolProbe) || d.Has(connector.CapabilityFullRead) {
		t.Fatalf("default descriptor must remain probe only: %+v", d)
	}
	t.Setenv("QMIGRATION_EXPERIMENTAL_DAMENG_NATIVE", "1")
	d = f.Capabilities(domain.DataSourceDameng)
	for _, cap := range []connector.Capability{
		connector.CapabilityMetadata, connector.CapabilityFullRead, connector.CapabilityFullWrite,
		connector.CapabilityKeysetBoundary, connector.CapabilitySchemaCreate, connector.CapabilityPostLoadSchema,
		connector.CapabilityCDCApply, connector.CapabilityCDCTransactional, connector.CapabilityPointLookup,
	} {
		if !d.Has(cap) {
			t.Fatalf("missing %s: %+v", cap, d)
		}
	}
	if d.Has(connector.CapabilityCDCRead) || d.Has(connector.CapabilityCDCPosition) {
		t.Fatalf("RC13 must not advertise Dameng source CDC: %+v", d)
	}
	if d.Maturity != connector.MaturityExperimental || !d.QualificationRequired {
		t.Fatalf("unexpected maturity: %+v", d)
	}
}

func TestDamengNoLongerUsesExternalPlaceholder(t *testing.T) {
	if domain.DataSourceDameng.IsExternalJDBC() {
		t.Fatal("Dameng must use the RC13 QMigration connector, not external JDBC placeholder semantics")
	}
	if domain.DataSourceGaussDB.IsExternalJDBC() {
		t.Fatal("GaussDB must use the RC14 PostgreSQL-wire connector path")
	}
	if domain.DataSourceGBase.IsExternalJDBC() {
		t.Fatal("GBase must use the RC18 native GBase 8a connector path")
	}
}

func TestDamengMetadataCatalogAssembly(t *testing.T) {
	fr := &fakeRunner{}
	fr.queryFn = func(q string, _ []any) ([][]connector.Value, error) {
		switch {
		case strings.Contains(q, "FROM ALL_TAB_COLUMNS"):
			return [][]connector.Value{
				{cv("ID"), cv("BIGINT"), cv("8"), cv("19"), cv("0"), cv("N"), cv("1")},
				{cv("NAME"), cv("VARCHAR"), cv("200"), cv("0"), cv("0"), cv("Y"), cv("2")},
				{cv("AMOUNT"), cv("DECIMAL"), cv("16"), cv("20"), cv("4"), cv("Y"), cv("3")},
			}, nil
		case strings.Contains(q, "CONSTRAINT_TYPE='P'"):
			return [][]connector.Value{{cv("PK_ORDERS"), cv("ID")}}, nil
		case strings.Contains(q, "FROM ALL_INDEXES"):
			return [][]connector.Value{{cv("PK_ORDERS"), cv("UNIQUE"), cv("ID"), cv("1")}, {cv("IDX_NAME"), cv("NONUNIQUE"), cv("NAME"), cv("1")}}, nil
		case strings.Contains(q, "CONSTRAINT_TYPE='R'"):
			return [][]connector.Value{{cv("FK_PARENT"), cv("ID"), cv("APP"), cv("PARENT"), cv("ID"), cv("1")}}, nil
		case strings.Contains(q, "COALESCE(NUM_ROWS"):
			return [][]connector.Value{{cv("42")}}, nil
		case strings.HasPrefix(q, "SELECT MIN"):
			return [][]connector.Value{{cv("1"), cv("99")}}, nil
		default:
			return nil, fmt.Errorf("unexpected query: %s", q)
		}
	}
	c := &Connector{ds: domain.DataSource{Type: domain.DataSourceDameng, Host: "127.0.0.1", Port: 5236}, r: fr}
	md, err := c.GetTableMetadata(context.Background(), "APP", "ORDERS")
	if err != nil {
		t.Fatal(err)
	}
	if len(md.Columns) != 3 || md.Columns[2].ColumnType != "DECIMAL(20,4)" {
		t.Fatalf("columns=%+v", md.Columns)
	}
	if md.PrimaryKey != "ID" || !md.PrimaryKeyNumeric || md.MinPK != 1 || md.MaxPK != 99 || md.EstimatedRows != 42 {
		t.Fatalf("metadata=%+v", md)
	}
	if len(md.Indexes) != 2 || len(md.ForeignKeys) != 1 || md.ForeignKeys[0].ReferencedTable != "PARENT" {
		t.Fatalf("indexes/fks=%+v %+v", md.Indexes, md.ForeignKeys)
	}
	primaryIndexes := 0
	for _, idx := range md.Indexes {
		if idx.Primary {
			primaryIndexes++
			if idx.Name != "PK_ORDERS" {
				t.Fatalf("wrong primary index: %+v", idx)
			}
		}
	}
	if primaryIndexes != 1 {
		t.Fatalf("primary index detection=%d indexes=%+v", primaryIndexes, md.Indexes)
	}
}

func TestDamengCompositeKeysetUsesBindsAndLimit(t *testing.T) {
	fr := &fakeRunner{queryFn: func(q string, args []any) ([][]connector.Value, error) {
		if strings.Contains(q, "alice") || strings.Contains(q, "10") && !strings.Contains(q, "LIMIT 10") {
			t.Fatalf("key values leaked into SQL: %s", q)
		}
		if !strings.Contains(q, `ORDER BY "ID","NAME" LIMIT 10`) {
			t.Fatalf("unexpected query: %s", q)
		}
		if len(args) == 0 {
			t.Fatal("expected bound keyset args")
		}
		return [][]connector.Value{{cv("11"), cv("bob")}}, nil
	}}
	c := &Connector{ds: domain.DataSource{Host: "127.0.0.1", Port: 5236}, r: fr}
	cols := []domain.ColumnInfo{{Name: "ID", DataType: "bigint"}, {Name: "NAME", DataType: "varchar"}}
	b, err := c.ReadBatch(context.Background(), connector.ReadBatchRequest{
		Schema: "APP", Table: "T", Columns: cols, PrimaryKeys: []string{"ID", "NAME"}, UseKeyset: true,
		Cursor: []connector.Value{cv("10"), cv("alice")}, Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(b.Rows) != 1 || len(b.LastKey) != 2 || string(b.LastKey[1].Raw) != "bob" {
		t.Fatalf("batch=%+v", b)
	}
}

func TestDamengMERGEIsBindOnlyAndNumericFailsClosed(t *testing.T) {
	fr := &fakeRunner{}
	c := &Connector{ds: domain.DataSource{Host: "127.0.0.1", Port: 5236}, r: fr}
	cols := []domain.ColumnInfo{{Name: "ID", DataType: "bigint"}, {Name: "NAME", DataType: "varchar"}, {Name: "PAYLOAD", DataType: "blob"}}
	payload := `x'); DROP TABLE APP.T; --`
	written, err := c.WriteBatch(context.Background(), connector.WriteBatchRequest{
		Schema: "APP", Table: "T", Columns: cols, PrimaryKeys: []string{"ID"},
		Rows: [][]connector.Value{{cv("7"), cv(payload), {Raw: []byte{0, 1, 2}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if written != 1 || len(fr.execs) != 1 {
		t.Fatalf("written=%d execs=%d", written, len(fr.execs))
	}
	if strings.Contains(fr.execs[0].SQL, payload) || !strings.Contains(fr.execs[0].SQL, "MERGE INTO") {
		t.Fatalf("row data must stay out of SQL: %s", fr.execs[0].SQL)
	}
	if len(fr.execs[0].Args) != 3 || fr.execs[0].Args[1] != payload {
		t.Fatalf("args=%#v", fr.execs[0].Args)
	}

	_, err = c.WriteBatch(context.Background(), connector.WriteBatchRequest{
		Schema: "APP", Table: "T", Columns: cols[:1], PrimaryKeys: []string{"ID"},
		Rows: [][]connector.Value{{cv("1;DROP TABLE T")}},
	})
	if err == nil || !strings.Contains(err.Error(), "invalid numeric literal") {
		t.Fatalf("expected numeric fail-closed error, got %v", err)
	}
}

func TestDamengTargetTypeCompiler(t *testing.T) {
	cases := []struct {
		col  domain.ColumnInfo
		want string
	}{
		{domain.ColumnInfo{Name: "u", DataType: "uuid"}, "VARCHAR(36)"},
		{domain.ColumnInfo{Name: "j", DataType: "json"}, "CLOB"},
		{domain.ColumnInfo{Name: "b", DataType: "bytea"}, "BLOB"},
		{domain.ColumnInfo{Name: "n", DataType: "decimal", ColumnType: "decimal(20,4)"}, "DECIMAL(20,4)"},
	}
	for _, tc := range cases {
		got, err := dmTargetType(tc.col)
		if err != nil || got != tc.want {
			t.Fatalf("%+v => %s err=%v want=%s", tc.col, got, err, tc.want)
		}
	}
}

func TestDamengNullValuePassesPreparedBinding(t *testing.T) {
	v, err := bindValue(domain.ColumnInfo{Name: "N", DataType: "number"}, nullv())
	if err != nil || v != nil {
		t.Fatalf("v=%#v err=%v", v, err)
	}
}

func TestDamengTransportFailsClosedForUnqualifiedTLS(t *testing.T) {
	for _, mode := range []domain.TLSMode{domain.TLSModePreferred, domain.TLSModeRequired} {
		err := validateTransportSettings(domain.DataSource{TLSMode: mode})
		if err == nil || !strings.Contains(err.Error(), "not qualified") {
			t.Fatalf("mode=%s err=%v", mode, err)
		}
	}
	if err := validateTransportSettings(domain.DataSource{TLSMode: domain.TLSModeDisable, TLSCACert: "ca"}); err == nil {
		t.Fatal("TLS material with DISABLE must fail closed")
	}
	if err := validateTransportSettings(domain.DataSource{TLSMode: domain.TLSModeDisable}); err != nil {
		t.Fatal(err)
	}
}

func TestDamengCDCAndValidationCapabilitiesRequireSeparateGate(t *testing.T) {
	f := NewFactory()
	t.Setenv("QMIGRATION_EXPERIMENTAL_DAMENG_NATIVE", "1")
	t.Setenv("QMIGRATION_EXPERIMENTAL_DAMENG_LOG_CDC", "")
	d := f.Capabilities(domain.DataSourceDameng)
	if d.Has(connector.CapabilityCDCRead) || d.Has(connector.CapabilityCDCPosition) || d.Has(connector.CapabilityValidationSnapshot) {
		t.Fatalf("Dameng CDC capabilities leaked without CDC gate: %+v", d)
	}
	t.Setenv("QMIGRATION_EXPERIMENTAL_DAMENG_LOG_CDC", "1")
	d = f.Capabilities(domain.DataSourceDameng)
	for _, cap := range []connector.Capability{connector.CapabilityCDCRead, connector.CapabilityCDCPosition, connector.CapabilityValidationSnapshot} {
		if !d.Has(cap) {
			t.Fatalf("missing Dameng CDC capability %s: %+v", cap, d)
		}
	}
}

func TestDamengArchiveCoverageFailsClosedOnGap(t *testing.T) {
	rows := [][]connector.Value{
		{cv("a.log"), cv("90"), cv("110")},
		{cv("b.log"), cv("111"), cv("140")},
	}
	if _, err := validateArchiveCoverage(rows, 100, 130); err == nil || !strings.Contains(err.Error(), "gap") {
		t.Fatalf("expected archive gap, got %v", err)
	}
	rows[1][1] = cv("110")
	files, err := validateArchiveCoverage(rows, 100, 130)
	if err != nil || len(files) != 2 {
		t.Fatalf("coverage files=%v err=%v", files, err)
	}
}

func TestDamengMutationCoalescingNetEffect(t *testing.T) {
	recs := []dmLogRecord{
		{SCN: 101, Operation: dmOpInsert, Schema: "APP", Table: "T", RowID: "R1"},
		{SCN: 102, Operation: dmOpUpdate, Schema: "APP", Table: "T", RowID: "R1"},
		{SCN: 103, Operation: dmOpUpdate, Schema: "APP", Table: "T", RowID: "R2"},
		{SCN: 104, Operation: dmOpDelete, Schema: "APP", Table: "T", RowID: "R2"},
		{SCN: 105, Operation: dmOpInsert, Schema: "APP", Table: "T", RowID: "R3"},
		{SCN: 106, Operation: dmOpDelete, Schema: "APP", Table: "T", RowID: "R3"},
	}
	muts, err := coalesceDMMutations(recs)
	if err != nil || len(muts) != 3 {
		t.Fatalf("muts=%+v err=%v", muts, err)
	}
	want := []struct {
		op   domain.CDCOperation
		emit bool
	}{{domain.CDCInsert, true}, {domain.CDCDelete, true}, {"", false}}
	for i := range muts {
		op, emit, err := dmNetOperation(muts[i])
		if err != nil || op != want[i].op || emit != want[i].emit {
			t.Fatalf("mutation %d => op=%s emit=%v err=%v", i, op, emit, err)
		}
	}
}

func TestDamengValidationSnapshotUsesASOFSCNAndIsReadOnly(t *testing.T) {
	t.Setenv("QMIGRATION_EXPERIMENTAL_DAMENG_NATIVE", "1")
	t.Setenv("QMIGRATION_EXPERIMENTAL_DAMENG_LOG_CDC", "1")
	base := &Connector{ds: domain.DataSource{Type: domain.DataSourceDameng, Host: "127.0.0.1", Port: 5236}}
	raw, err := base.OpenValidationSnapshot(context.Background(), domain.CDCPosition{PositionType: "DM_LSN", PositionValue: "12345"})
	if err != nil {
		t.Fatal(err)
	}
	snap := raw.(*Connector)
	fr := &fakeRunner{queryFn: func(q string, _ []any) ([][]connector.Value, error) {
		if !strings.Contains(q, `FROM "APP"."T" AS OF SCN 12345`) {
			t.Fatalf("validation query is not pinned to LSN: %s", q)
		}
		return [][]connector.Value{{cv("1")}}, nil
	}}
	snap.r = fr
	_, err = snap.ReadBatch(context.Background(), connector.ReadBatchRequest{Schema: "APP", Table: "T", Columns: []domain.ColumnInfo{{Name: "ID", DataType: "bigint"}}, PrimaryKeys: []string{"ID"}, UseKeyset: true, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := snap.WriteBatch(context.Background(), connector.WriteBatchRequest{}); err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("expected read-only snapshot, got %v", err)
	}
}

func TestDamengLogMinerRewindsForTransactionStartedBeforeCheckpoint(t *testing.T) {
	t.Setenv("QMIGRATION_EXPERIMENTAL_DAMENG_NATIVE", "1")
	t.Setenv("QMIGRATION_EXPERIMENTAL_DAMENG_LOG_CDC", "1")
	fr := &fakeRunner{}
	logQueries := 0
	fr.queryFn = func(q string, args []any) ([][]connector.Value, error) {
		switch {
		case strings.Contains(q, "FROM V$ARCHIVED_LOG"):
			return [][]connector.Value{{cv("/dm/arch/a.log"), cv("70"), cv("131")}}, nil
		case strings.Contains(q, "FROM V$LOGMNR_CONTENTS"):
			logQueries++
			commit := []connector.Value{cv("80"), cv("120"), cv("120"), {Raw: []byte{1, 2}}, cv("7"), cv("0"), nullv(), nullv(), nullv(), cv("2026-09-01 10:00:00"), cv("2")}
			if logQueries == 1 {
				return [][]connector.Value{commit}, nil
			}
			insert := []connector.Value{cv("80"), cv("90"), cv("120"), {Raw: []byte{1, 2}}, cv("1"), cv("0"), cv("APP"), cv("T"), cv("RID1"), cv("2026-09-01 10:00:00"), cv("1")}
			return [][]connector.Value{insert, commit}, nil
		case strings.Contains(q, "FROM ALL_TAB_COLUMNS"):
			return [][]connector.Value{{cv("ID"), cv("BIGINT"), cv("8"), cv("19"), cv("0"), cv("N"), cv("1")}, {cv("V"), cv("VARCHAR"), cv("64"), cv("0"), cv("0"), cv("Y"), cv("2")}}, nil
		case strings.Contains(q, "CONSTRAINT_TYPE='P'"):
			return [][]connector.Value{{cv("PK_T"), cv("ID")}}, nil
		case strings.Contains(q, "FROM ALL_INDEXES") || strings.Contains(q, "CONSTRAINT_TYPE='R'"):
			return nil, nil
		case strings.Contains(q, "COALESCE(NUM_ROWS"):
			return [][]connector.Value{{cv("1")}}, nil
		case strings.HasPrefix(q, "SELECT MIN"):
			return [][]connector.Value{{cv("1"), cv("1")}}, nil
		case strings.Contains(q, `AS OF SCN 119`) && strings.Contains(q, `ROWID=?`):
			return nil, nil
		case strings.Contains(q, `AS OF SCN 120`) && strings.Contains(q, `ROWID=?`):
			return [][]connector.Value{{cv("1"), cv("final")}}, nil
		default:
			return nil, fmt.Errorf("unexpected query: %s args=%v", q, args)
		}
	}
	c := &Connector{ds: domain.DataSource{Type: domain.DataSourceDameng, Host: "127.0.0.1", Port: 5236}, r: fr}
	txs, err := c.ReadLogMinerTransactions(context.Background(), "100", "130", map[string]bool{"app.t": true})
	if err != nil {
		t.Fatal(err)
	}
	if logQueries != 2 {
		t.Fatalf("expected two mining passes, got %d", logQueries)
	}
	starts := []string{}
	for _, call := range fr.execs {
		if strings.Contains(call.SQL, "START_LOGMNR") {
			starts = append(starts, call.SQL)
		}
	}
	if len(starts) != 2 || !strings.Contains(starts[0], "STARTSCN=>101") || !strings.Contains(starts[1], "STARTSCN=>80") {
		t.Fatalf("unexpected LogMiner rewind starts: %v", starts)
	}
	if len(txs) != 2 || txs[0].LSN != "120" || txs[1].LSN != "130" {
		t.Fatalf("transactions=%+v", txs)
	}
	if len(txs[0].Events) != 1 || txs[0].Events[0].Operation != domain.CDCInsert || len(txs[0].Events[0].After) != 2 || txs[0].Events[0].After[1].Value != "final" {
		t.Fatalf("row transaction=%+v", txs[0])
	}
}

func TestDamengLogMinerAggregatesSameCommitLSNAcrossXIDs(t *testing.T) {
	t.Setenv("QMIGRATION_EXPERIMENTAL_DAMENG_NATIVE", "1")
	t.Setenv("QMIGRATION_EXPERIMENTAL_DAMENG_LOG_CDC", "1")
	fr := &fakeRunner{}
	fr.queryFn = func(q string, args []any) ([][]connector.Value, error) {
		switch {
		case strings.Contains(q, "FROM V$ARCHIVED_LOG"):
			return [][]connector.Value{{cv("/dm/arch/a.log"), cv("100"), cv("131")}}, nil
		case strings.Contains(q, "FROM V$LOGMNR_CONTENTS"):
			return [][]connector.Value{
				{cv("105"), cv("110"), cv("120"), {Raw: []byte{1}}, cv("1"), cv("0"), cv("APP"), cv("T"), cv("RID1"), cv("2026-09-01 10:00:00"), cv("1")},
				{cv("105"), cv("111"), cv("120"), {Raw: []byte{1}}, cv("7"), cv("0"), nullv(), nullv(), nullv(), cv("2026-09-01 10:00:01"), cv("2")},
				{cv("106"), cv("112"), cv("120"), {Raw: []byte{2}}, cv("1"), cv("0"), cv("APP"), cv("T"), cv("RID2"), cv("2026-09-01 10:00:02"), cv("1")},
				{cv("106"), cv("113"), cv("120"), {Raw: []byte{2}}, cv("7"), cv("0"), nullv(), nullv(), nullv(), cv("2026-09-01 10:00:03"), cv("2")},
			}, nil
		case strings.Contains(q, "FROM ALL_TAB_COLUMNS"):
			return [][]connector.Value{{cv("ID"), cv("BIGINT"), cv("8"), cv("19"), cv("0"), cv("N"), cv("1")}, {cv("V"), cv("VARCHAR"), cv("64"), cv("0"), cv("0"), cv("Y"), cv("2")}}, nil
		case strings.Contains(q, "CONSTRAINT_TYPE='P'"):
			return [][]connector.Value{{cv("PK_T"), cv("ID")}}, nil
		case strings.Contains(q, "FROM ALL_INDEXES") || strings.Contains(q, "CONSTRAINT_TYPE='R'"):
			return nil, nil
		case strings.Contains(q, "COALESCE(NUM_ROWS"):
			return [][]connector.Value{{cv("2")}}, nil
		case strings.HasPrefix(q, "SELECT MIN"):
			return [][]connector.Value{{cv("1"), cv("2")}}, nil
		case strings.Contains(q, `AS OF SCN 119`) && strings.Contains(q, `ROWID=?`):
			return nil, nil
		case strings.Contains(q, `AS OF SCN 120`) && strings.Contains(q, `ROWID=?`):
			if len(args) != 1 {
				return nil, fmt.Errorf("expected ROWID bind, args=%v", args)
			}
			switch fmt.Sprint(args[0]) {
			case "RID1":
				return [][]connector.Value{{cv("1"), cv("a")}}, nil
			case "RID2":
				return [][]connector.Value{{cv("2"), cv("b")}}, nil
			default:
				return nil, fmt.Errorf("unexpected ROWID %v", args[0])
			}
		default:
			return nil, fmt.Errorf("unexpected query: %s args=%v", q, args)
		}
	}
	c := &Connector{ds: domain.DataSource{Type: domain.DataSourceDameng, Host: "127.0.0.1", Port: 5236}, r: fr}
	txs, err := c.ReadLogMinerTransactions(context.Background(), "100", "130", map[string]bool{"app.t": true})
	if err != nil {
		t.Fatal(err)
	}
	if len(txs) != 2 || txs[0].LSN != "120" || txs[1].LSN != "130" {
		t.Fatalf("transactions=%+v", txs)
	}
	if len(txs[0].Events) != 2 {
		t.Fatalf("same-COMMIT_SCN XIDs must be one durable transaction, got %+v", txs[0])
	}
	for i, event := range txs[0].Events {
		if event.Operation != domain.CDCInsert || event.PositionValue != "120" {
			t.Fatalf("event %d=%+v", i, event)
		}
	}
}
