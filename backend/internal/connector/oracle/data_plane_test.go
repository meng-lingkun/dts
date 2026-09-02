package oracleconnector

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
)

func TestOracleNativeCapabilityGates(t *testing.T) {
	t.Setenv("QMIGRATION_EXPERIMENTAL_ORACLE_NATIVE", "")
	t.Setenv("QMIGRATION_EXPERIMENTAL_ORACLE_LOGMINER_CDC", "")
	d := NewFactory().Capabilities(domain.DataSourceOracle)
	if len(d.Capabilities) != 1 || !d.Has(connector.CapabilityProtocolProbe) {
		t.Fatalf("default Oracle capabilities leaked: %+v", d.Capabilities)
	}

	t.Setenv("QMIGRATION_EXPERIMENTAL_ORACLE_NATIVE", "1")
	d = NewFactory().Capabilities(domain.DataSourceOracle)
	for _, want := range []connector.Capability{connector.CapabilityMetadata, connector.CapabilityFullRead, connector.CapabilityKeysetBoundary, connector.CapabilityPartition, connector.CapabilityRuntimeLoad, connector.CapabilitySchemaObjects, connector.CapabilityPointLookup, connector.CapabilityMigrationPrecheck} {
		if !d.Has(want) {
			t.Fatalf("native source capability %s missing: %+v", want, d.Capabilities)
		}
	}
	for _, forbidden := range []connector.Capability{connector.CapabilityFullWrite, connector.CapabilitySchemaCreate, connector.CapabilityPostLoadSchema, connector.CapabilityCDCApply, connector.CapabilityCDCTransactional, connector.CapabilityDDLApply, connector.CapabilityCDCRead, connector.CapabilityCDCPosition, connector.CapabilityValidationSnapshot} {
		if d.Has(forbidden) {
			t.Fatalf("unqualified Oracle capability %s leaked: %+v", forbidden, d.Capabilities)
		}
	}

	t.Setenv("QMIGRATION_EXPERIMENTAL_ORACLE_LOGMINER_CDC", "1")
	d = NewFactory().Capabilities(domain.DataSourceOracle)
	if !d.Has(connector.CapabilityCDCRead) || !d.Has(connector.CapabilityCDCPosition) || !d.Has(connector.CapabilityValidationSnapshot) {
		t.Fatalf("LogMiner CDC/exact validation capabilities missing: %+v", d.Capabilities)
	}
	if d.Has(connector.CapabilityFullWrite) || d.Has(connector.CapabilityCDCApply) {
		t.Fatalf("LogMiner gate must not enable Oracle target apply: %+v", d.Capabilities)
	}
}

func TestOracleValidationSnapshotSCNContract(t *testing.T) {
	t.Setenv("QMIGRATION_EXPERIMENTAL_ORACLE_NATIVE", "1")
	t.Setenv("QMIGRATION_EXPERIMENTAL_ORACLE_LOGMINER_CDC", "1")
	c := &Connector{ds: domain.DataSource{Type: domain.DataSourceOracle, Host: "oracle", Port: 1521, Database: "ORCL"}}
	if _, err := c.OpenValidationSnapshot(context.Background(), domain.CDCPosition{PositionType: "GTID", PositionValue: "1"}); err == nil {
		t.Fatal("non-SCN validation position accepted")
	}
	if _, err := c.OpenValidationSnapshot(context.Background(), domain.CDCPosition{PositionType: "ORACLE_SCN", PositionValue: "0"}); err == nil {
		t.Fatal("zero validation SCN accepted")
	}
	snapRaw, err := c.OpenValidationSnapshot(context.Background(), domain.CDCPosition{PositionType: "ORACLE_SCN", PositionValue: "123456"})
	if err != nil {
		t.Fatal(err)
	}
	snap, ok := snapRaw.(*Connector)
	if !ok || snap.validationSCN != "123456" {
		t.Fatalf("snapshot=%T %#v", snapRaw, snapRaw)
	}
	if got := snap.oracleValidationFrom("APP", "ORDERS", "P2026"); got != `"APP"."ORDERS" PARTITION ("P2026") AS OF SCN 123456` {
		t.Fatalf("flashback table reference=%q", got)
	}
	if _, err := snap.WriteBatch(context.Background(), connector.WriteBatchRequest{}); err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("validation snapshot write was not rejected: %v", err)
	}
}

func TestOracleValueLiteralRejectsNumericInjection(t *testing.T) {
	col := domain.ColumnInfo{Name: "ID", DataType: "number", ColumnType: "number(38)"}
	for _, good := range []string{"0", "-1", "+12.50", ".25", "1e10", "-2.5E-3"} {
		got, err := oracleValueLiteral(connector.Value{Raw: []byte(good)}, col)
		if err != nil || got != good {
			t.Fatalf("good numeric %q => %q err=%v", good, got, err)
		}
	}
	for _, bad := range []string{"", "1 OR 1=1", "1;DROP TABLE X", "NaN", "Inf", "1,2"} {
		if got, err := oracleValueLiteral(connector.Value{Raw: []byte(bad)}, col); err == nil {
			t.Fatalf("bad numeric %q accepted as %q", bad, got)
		}
	}
}

func TestOracleLiteralEscapingAndLexicographicPredicate(t *testing.T) {
	textCol := domain.ColumnInfo{Name: "NAME", DataType: "varchar2", ColumnType: "varchar2(100)"}
	lit, err := oracleValueLiteral(connector.Value{Raw: []byte("O'Reilly")}, textCol)
	if err != nil || lit != "'O''Reilly'" {
		t.Fatalf("literal=%q err=%v", lit, err)
	}
	pred, err := oracleLexCompare(
		[]string{"ID", "NAME"},
		[]domain.ColumnInfo{{Name: "ID", DataType: "number"}, textCol},
		[]connector.Value{{Raw: []byte("42")}, {Raw: []byte("O'Reilly")}},
		">=",
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, part := range []string{`"ID">42`, `"ID"=42`, `"NAME">'O''Reilly'`, `"NAME"='O''Reilly'`} {
		if !strings.Contains(pred, part) {
			t.Fatalf("predicate %q missing %q", pred, part)
		}
	}
}

func TestOracleIdentifierAndHashPredicateShape(t *testing.T) {
	if got := oracleIdent(`A"B`); got != `"A""B"` {
		t.Fatalf("identifier=%q", got)
	}
	if got := selectedOraclePredicate(map[string]bool{"app.orders": true, "bad": true, "app.skip": false}); got != `(SEG_OWNER='APP' AND TABLE_NAME='ORDERS')` {
		t.Fatalf("predicate=%q", got)
	}
}

func TestOracleTargetCapabilityGate(t *testing.T) {
	t.Setenv("QMIGRATION_EXPERIMENTAL_ORACLE_NATIVE", "1")
	t.Setenv("QMIGRATION_EXPERIMENTAL_ORACLE_TARGET", "1")
	d := NewFactory().Capabilities(domain.DataSourceOracle)
	for _, want := range []connector.Capability{
		connector.CapabilityFullWrite,
		connector.CapabilitySchemaCreate,
		connector.CapabilityPostLoadSchema,
		connector.CapabilityCDCApply,
		connector.CapabilityCDCTransactional,
		connector.CapabilityDDLApply,
	} {
		if !d.Has(want) {
			t.Fatalf("Oracle target capability %s missing: %+v", want, d.Capabilities)
		}
	}
}

func TestOracleWriteRowPlanUsesBindsAndKeyedLOBStreaming(t *testing.T) {
	req := connector.WriteBatchRequest{
		Schema: "APP", Table: "T",
		PrimaryKeys: []string{"ID"},
		Columns: []domain.ColumnInfo{
			{Name: "ID", DataType: "number", ColumnType: "number(38)"},
			{Name: "NAME", DataType: "varchar2", ColumnType: "varchar2(100)"},
			{Name: "PAYLOAD", DataType: "blob", ColumnType: "blob"},
		},
	}
	row := []connector.Value{{Raw: []byte("42")}, {Raw: []byte("O'Reilly")}, {Raw: []byte(strings.Repeat("x", oracleMaxInlineBindBytes+1))}}
	plan, err := oracleWriteRowPlan(req, row, 873)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Binds) != 3 || len(plan.LargeLOBs) != 1 {
		t.Fatalf("plan binds=%d lobs=%d", len(plan.Binds), len(plan.LargeLOBs))
	}
	if strings.Contains(plan.SQL, "O'Reilly") || !strings.Contains(plan.SQL, ":1") || !strings.Contains(plan.SQL, "EMPTY_BLOB") {
		t.Fatalf("write SQL is not bind-safe: %s", plan.SQL)
	}
}

func TestOracleKeylessLargeLOBUsesTemporaryLOBBlock(t *testing.T) {
	req := connector.WriteBatchRequest{
		Schema: "APP", Table: "T",
		Columns: []domain.ColumnInfo{{Name: "PAYLOAD", DataType: "clob", ColumnType: "clob"}},
	}
	payload := []byte(strings.Repeat("你", oracleMaxInlineBindBytes/3+100))
	plan, err := oracleWriteRowPlan(req, []connector.Value{{Raw: payload}}, 873)
	if err != nil {
		t.Fatal(err)
	}
	if !plan.PLSQL || len(plan.LargeLOBs) != 0 || len(plan.Binds) < 2 {
		t.Fatalf("plan=%+v", plan)
	}
	for _, want := range []string{"DBMS_LOB.CREATETEMPORARY", "DBMS_LOB.WRITEAPPEND", `INSERT INTO "APP"."T"`, "DBMS_LOB.FREETEMPORARY"} {
		if !strings.Contains(plan.SQL, want) {
			t.Fatalf("PL/SQL missing %q: %s", want, plan.SQL)
		}
	}
	if strings.Contains(plan.SQL, "你你你") {
		t.Fatal("large CLOB bytes leaked into SQL text instead of TTC binds")
	}
}

func TestOracleWriteBatchArrayBindAndPreparedReexecute(t *testing.T) {
	t.Setenv("QMIGRATION_EXPERIMENTAL_ORACLE_NATIVE", "1")
	t.Setenv("QMIGRATION_EXPERIMENTAL_ORACLE_TARGET", "1")
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c := &Connector{
		accepted:      &acceptedSession{Session: &tnsDataSession{conn: client}},
		authenticated: true,
		proto:         ttcProtocolInfo{ServerCharset: 873},
		data:          ttcDataTypeInfo{TTCVersion: 12},
	}
	req := connector.WriteBatchRequest{
		Schema: "APP", Table: "T", PrimaryKeys: []string{"ID"},
		Columns: []domain.ColumnInfo{{Name: "ID", DataType: "number"}, {Name: "NAME", DataType: "varchar2"}},
		Rows: [][]connector.Value{
			{{Raw: []byte("1")}, {Raw: []byte("alice")}},
			{{Raw: []byte("2")}, {Raw: []byte("bob")}},
		},
	}
	serverErr := make(chan error, 1)
	go func() {
		s := &tnsDataSession{conn: server}
		for call := 0; call < 2; call++ {
			_, p, err := s.ReadData(ctx)
			if err != nil {
				serverErr <- err
				return
			}
			if call == 0 {
				if len(p) < 4 || p[0] != 3 || p[1] != 0x5e || !bytes.Contains(p, []byte("MERGE INTO")) {
					serverErr <- fmt.Errorf("first bind request=%x", p)
					return
				}
			} else if len(p) < 3 || !bytes.Equal(p[:3], []byte{3, 4, 0}) {
				serverErr <- fmt.Errorf("prepared request=%x", p)
				return
			}
			packet := &ttcEncoder{}
			packet.byte(ttcErrorReturn)
			rowCount := uint64(2)
			if call == 1 {
				rowCount = 1
			}
			packet.Write(fakeTTCSummaryPayload(12, 0, 77, rowCount, ""))
			if err := s.WriteData(ctx, 0, packet.Bytes()); err != nil {
				serverErr <- err
				return
			}
			// COMMIT is a bind-free OALL8 statement.
			_, commit, err := s.ReadData(ctx)
			if err != nil || len(commit) < 2 || commit[0] != 3 || commit[1] != 0x5e {
				serverErr <- fmt.Errorf("commit request=%x err=%v", commit, err)
				return
			}
			packet.Reset()
			packet.byte(ttcErrorReturn)
			packet.Write(fakeTTCSummaryPayload(12, 0, 0, 0, ""))
			if err := s.WriteData(ctx, 0, packet.Bytes()); err != nil {
				serverErr <- err
				return
			}
			// The second call is one row but must reuse the cursor. The server
			// goroutine never mutates req; the caller changes it between writes.
		}
		serverErr <- nil
	}()
	written, err := c.WriteBatch(ctx, req)
	if err != nil || written != 2 {
		t.Fatalf("first write=%d err=%v", written, err)
	}
	req.Rows = req.Rows[:1]
	written, err = c.WriteBatch(ctx, req)
	if err != nil || written != 1 {
		t.Fatalf("second write=%d err=%v", written, err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestOracleKeylessLargeLOBWriteSpansTNSDataPackets(t *testing.T) {
	t.Setenv("QMIGRATION_EXPERIMENTAL_ORACLE_NATIVE", "1")
	t.Setenv("QMIGRATION_EXPERIMENTAL_ORACLE_TARGET", "1")
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c := &Connector{
		accepted:      &acceptedSession{Session: &tnsDataSession{conn: client}},
		authenticated: true,
		proto:         ttcProtocolInfo{ServerCharset: 873},
		data:          ttcDataTypeInfo{TTCVersion: 12},
	}
	payload := []byte(strings.Repeat("x", tnsMaxDataPayloadLen+8192))
	req := connector.WriteBatchRequest{
		Schema: "APP", Table: "NO_KEY_LOB",
		Columns: []domain.ColumnInfo{{Name: "PAYLOAD", DataType: "clob", ColumnType: "clob"}},
		Rows:    [][]connector.Value{{{Raw: payload}}},
	}
	plan, err := oracleWriteRowPlan(req, req.Rows[0], 873)
	if err != nil {
		t.Fatal(err)
	}
	expected, _, err := buildTTCBindStatementRequest(plan.SQL, 12, [][]oracleTTCBind{plan.Binds}, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(expected) <= tnsMaxDataPayloadLen {
		t.Fatalf("test request did not cross TNS packet boundary: %d", len(expected))
	}
	serverErr := make(chan error, 1)
	go func() {
		s := &tnsDataSession{conn: server}
		got := make([]byte, 0, len(expected))
		packets := 0
		for len(got) < len(expected) {
			flags, p, e := s.ReadData(ctx)
			if e != nil {
				serverErr <- e
				return
			}
			if flags != 0 {
				serverErr <- fmt.Errorf("flags=%x", flags)
				return
			}
			packets++
			got = append(got, p...)
		}
		if packets < 2 || !bytes.Equal(got, expected) {
			serverErr <- fmt.Errorf("fragmented request packets=%d got=%d expected=%d", packets, len(got), len(expected))
			return
		}
		packet := &ttcEncoder{}
		packet.byte(ttcErrorReturn)
		packet.Write(fakeTTCSummaryPayload(12, 0, 0, 1, ""))
		if e := s.WriteData(ctx, 0, packet.Bytes()); e != nil {
			serverErr <- e
			return
		}
		_, commit, e := s.ReadData(ctx)
		if e != nil || len(commit) < 2 || commit[0] != 3 || commit[1] != 0x5e {
			serverErr <- fmt.Errorf("commit=%x err=%v", commit, e)
			return
		}
		packet.Reset()
		packet.byte(ttcErrorReturn)
		packet.Write(fakeTTCSummaryPayload(12, 0, 0, 0, ""))
		serverErr <- s.WriteData(ctx, 0, packet.Bytes())
	}()
	written, err := c.WriteBatch(ctx, req)
	if err != nil || written != 1 {
		t.Fatalf("write=%d err=%v", written, err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestOracleCLOBUTF8ChunkBoundaries(t *testing.T) {
	in := []byte(strings.Repeat("你", oracleLOBChunkBytes/3+17))
	chunks, err := splitUTF8Chunks(in, oracleLOBChunkBytes)
	if err != nil || len(chunks) < 2 {
		t.Fatalf("chunks=%d err=%v", len(chunks), err)
	}
	joined := []byte{}
	for _, ch := range chunks {
		if !utf8.Valid(ch) || len(ch) > oracleLOBChunkBytes {
			t.Fatalf("invalid chunk len=%d", len(ch))
		}
		joined = append(joined, ch...)
	}
	if !bytes.Equal(joined, in) {
		t.Fatal("CLOB chunks changed payload")
	}
}
