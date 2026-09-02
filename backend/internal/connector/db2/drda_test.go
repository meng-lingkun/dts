package db2connector

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"

	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
)

func TestEncodeCP500AndDDM(t *testing.T) {
	got, err := encodeCP500("DB2")
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{0xC4, 0xC2, 0xF2}
	if !bytes.Equal(got, want) {
		t.Fatalf("CP500: got %x want %x", got, want)
	}
	if _, err := encodeCP500("数据库"); err == nil {
		t.Fatal("non-ASCII handshake field must fail closed")
	}

	x, err := packEXCSAT()
	if err != nil {
		t.Fatal(err)
	}
	if len(x) < 4 || binary.BigEndian.Uint16(x[2:4]) != cpEXCSAT {
		t.Fatalf("bad EXCSAT: %x", x)
	}
	a, err := packACCSEC("SAMPLE            ", secmecEUSRIDPWD, make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	if binary.BigEndian.Uint16(a[2:4]) != cpACCSEC {
		t.Fatalf("bad ACCSEC: %x", a)
	}
}

func TestPackedDecimalString(t *testing.T) {
	tests := []struct {
		b    []byte
		p, s int
		want string
	}{
		{[]byte{0x12, 0x34, 0x5c}, 5, 2, "123.45"},
		{[]byte{0x00, 0x01, 0x2d}, 5, 2, "-0.12"},
		{[]byte{0x00, 0x00, 0x0c}, 5, 2, "0.00"},
	}
	for _, tc := range tests {
		got, err := packedDecimalString(tc.b, tc.p, tc.s)
		if err != nil {
			t.Fatal(err)
		}
		if got != tc.want {
			t.Fatalf("packed %x: got %q want %q", tc.b, got, tc.want)
		}
	}
	if _, err := packedDecimalString([]byte{0x12, 0x37}, 3, 0); err == nil {
		t.Fatal("bad sign must fail")
	}
}

func TestQRYDSCAndQRYDTA(t *testing.T) {
	desc, err := parseQRYDSC([]byte{9, 0x76, 0xd0, drdaNVarchar, 0, 32, drdaNInteger, 0, 4})
	if err != nil {
		t.Fatal(err)
	}
	if len(desc) != 2 {
		t.Fatalf("desc=%+v", desc)
	}
	body := []byte{0xff, 0, 0, 0, 5, 'h', 'e', 'l', 'l', 'o', 0, 42, 0, 0, 0}
	rows, err := parseQRYDTA(body, desc, binary.LittleEndian)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || string(rows[0][0].Data) != "hello" || string(rows[0][1].Data) != "42" {
		t.Fatalf("rows=%+v", rows)
	}

	nullBody := []byte{0xff, 0, 0xff, 0xff}
	rows, err = parseQRYDTA(nullBody, desc, binary.LittleEndian)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || !rows[0][0].Null || !rows[0][1].Null {
		t.Fatalf("null rows=%+v", rows)
	}
}

func TestEXTDTALOBReplacement(t *testing.T) {
	rows := [][]drdaCell{{{lob: true, inlineLOB: true}, {Data: []byte("x")}}}
	applyEXTDTA(rows, [][]byte{{0x00, 'a', 'b', 'c'}})
	if string(rows[0][0].Data) != "abc" {
		t.Fatalf("LOB=%q", rows[0][0].Data)
	}
}

func TestDB2LiteralFailClosed(t *testing.T) {
	n := domain.ColumnInfo{Name: "n", DataType: "decimal", ColumnType: "DECIMAL(31,10)"}
	if _, err := db2ValueLiteral(connector.Value{Raw: []byte("1 OR 1=1")}, n); err == nil {
		t.Fatal("numeric injection must fail")
	}
	if got, err := db2ValueLiteral(connector.Value{Raw: []byte("12345678901234567890.001")}, n); err != nil || got != "12345678901234567890.001" {
		t.Fatalf("numeric got=%q err=%v", got, err)
	}
	b := domain.ColumnInfo{Name: "b", DataType: "blob", ColumnType: "BLOB"}
	if got, _ := db2ValueLiteral(connector.Value{Raw: []byte{0, 1, 0xff}}, b); got != "X'0001ff'" {
		t.Fatalf("binary=%q", got)
	}
	s := domain.ColumnInfo{Name: "s", DataType: "varchar", ColumnType: "VARCHAR(100)"}
	if got, _ := db2ValueLiteral(connector.Value{Raw: []byte("O'Reilly")}, s); got != "'O''Reilly'" {
		t.Fatalf("string=%q", got)
	}
}

func TestBuildDB2PreparedUpsertCompositePK(t *testing.T) {
	cols := []domain.ColumnInfo{{Name: "tenant_id", DataType: "bigint"}, {Name: "id", DataType: "bigint"}, {Name: "name", DataType: "varchar"}}
	sql, err := buildDB2PreparedUpsert("APP", "T", cols, []string{"tenant_id", "id"})
	if err != nil {
		t.Fatal(err)
	}
	for _, part := range []string{`MERGE INTO "APP"."T"`, `T."tenant_id"=S."tenant_id"`, `T."id"=S."id"`, `WHEN MATCHED THEN UPDATE SET T."name"=S."name"`, `WHEN NOT MATCHED THEN INSERT`} {
		if !strings.Contains(sql, part) {
			t.Fatalf("missing %q in %s", part, sql)
		}
	}
}

func TestDB2SelectExprSerializesVector(t *testing.T) {
	got := db2SelectExpr(domain.ColumnInfo{Name: "embedding", DataType: "VECTOR"})
	if got != `VECTOR_SERIALIZE("embedding")` {
		t.Fatalf("vector select expr=%q", got)
	}
}

func TestDB2VectorTargetDDLAndPreparedUpsert(t *testing.T) {
	col := domain.ColumnInfo{Name: "embedding", DataType: "VECTOR", ColumnType: "VECTOR(3,FLOAT32)", Nullable: true}
	ddl, err := db2ColumnDefinition(col)
	if err != nil {
		t.Fatal(err)
	}
	if ddl != `"embedding" VECTOR(3,FLOAT32)` {
		t.Fatalf("vector ddl=%q", ddl)
	}
	sql, err := buildDB2PreparedUpsert("APP", "V", []domain.ColumnInfo{{Name: "id", DataType: "BIGINT"}, col}, []string{"id"})
	if err != nil {
		t.Fatal(err)
	}
	for _, part := range []string{`VECTOR(CAST(? AS CLOB),3,FLOAT32)`, `T."embedding"=S."embedding"`} {
		if !strings.Contains(sql, part) {
			t.Fatalf("missing %q in %s", part, sql)
		}
	}
}

func TestDB2VectorCatalogTypeShape(t *testing.T) {
	if got := db2ColumnType("VECTOR(FLOAT32)", 1536, 0); got != "VECTOR(1536)" {
		t.Fatalf("catalog vector type=%q", got)
	}
	col := domain.ColumnInfo{Name: "embedding", DataType: "vector", ColumnType: "VECTOR(1536,FLOAT32)"}
	if got := db2TargetType(col); got != "VECTOR(1536,FLOAT32)" {
		t.Fatalf("target vector type=%q", got)
	}
	for _, bad := range []domain.ColumnInfo{
		{Name: "v", DataType: "vector", ColumnType: "VECTOR"},
		{Name: "v", DataType: "vector", ColumnType: "VECTOR(0,INT8)"},
		{Name: "v", DataType: "vector", ColumnType: "VECTOR(8169,FLOAT32)"},
		{Name: "v", DataType: "vector", ColumnType: "VECTOR(2,FLOAT64)"},
	} {
		if _, err := db2VectorSpecForColumn(bad); err == nil {
			t.Fatalf("bad vector metadata accepted: %+v", bad)
		}
	}
}

func TestDB2VectorSpecAndValueValidation(t *testing.T) {
	spec, err := db2VectorSpecForColumn(domain.ColumnInfo{Name: "v", DataType: "vector", ColumnType: "VECTOR(3,REAL)"})
	if err != nil {
		t.Fatal(err)
	}
	if spec.dimension != 3 || spec.coordinate != "FLOAT32" || spec.typeSQL() != "VECTOR(3,FLOAT32)" {
		t.Fatalf("unexpected spec %+v", spec)
	}
	for _, good := range []string{"[1,2.5,-3e-2]", "[ 1 , 0 , -2 ]"} {
		if err := validateDB2VectorString([]byte(good), spec); err != nil {
			t.Fatalf("valid vector %s: %v", good, err)
		}
	}
	for _, bad := range []string{"1,2,3", "[1,2]", "[1,,3]", "[1,nan,3]"} {
		if err := validateDB2VectorString([]byte(bad), spec); err == nil {
			t.Fatalf("invalid vector %s accepted", bad)
		}
	}
	int8spec, err := db2VectorSpecForColumn(domain.ColumnInfo{Name: "iv", DataType: "vector", ColumnType: "VECTOR(2,INT8)"})
	if err != nil {
		t.Fatal(err)
	}
	if err := validateDB2VectorString([]byte("[-128,127]"), int8spec); err != nil {
		t.Fatal(err)
	}
	if err := validateDB2VectorString([]byte("[128,0]"), int8spec); err == nil {
		t.Fatal("out-of-range INT8 coordinate accepted")
	}
}

func TestDB2VectorParamEncodingInlineAndEXTDTA(t *testing.T) {
	col := domain.ColumnInfo{Name: "embedding", DataType: "vector", ColumnType: "VECTOR(2,INT8)"}
	inline, err := db2ParamEncodingFor(col, connector.Value{Raw: []byte("[1,-2]")})
	if err != nil {
		t.Fatal(err)
	}
	if inline.typ != drdaNVarMix || inline.lob {
		t.Fatalf("inline vector encoding=%+v", inline)
	}
	// Use a large but valid INT8 VECTOR so the string crosses the prepared
	// inline threshold and must reuse the CLOB/EXTDTA transport.
	dim := 10000
	bigCol := domain.ColumnInfo{Name: "embedding", DataType: "vector", ColumnType: fmt.Sprintf("VECTOR(%d,INT8)", dim)}
	parts := make([]string, dim)
	for i := range parts {
		parts[i] = "-128"
	}
	raw := []byte("[" + strings.Join(parts, ",") + "]")
	enc, err := db2ParamEncodingFor(bigCol, connector.Value{Raw: raw})
	if err != nil {
		t.Fatal(err)
	}
	if enc.typ != drdaNLOBCSBCS || !enc.lob || !enc.clob {
		t.Fatalf("large vector encoding=%+v len=%d", enc, len(raw))
	}
	body, ext, err := buildDB2SQLDTA([]domain.ColumnInfo{bigCol}, []connector.Value{{Raw: raw}}, binary.LittleEndian)
	if err != nil {
		t.Fatal(err)
	}
	if len(body) == 0 || len(ext) != 1 || !bytes.Equal(ext[0][1:], raw) {
		t.Fatalf("vector EXTDTA mismatch body=%d ext=%d", len(body), len(ext))
	}
}

func TestDB2TargetTypeMapping(t *testing.T) {
	tests := []struct {
		col  domain.ColumnInfo
		want string
	}{
		{domain.ColumnInfo{DataType: "bigint"}, "BIGINT"},
		{domain.ColumnInfo{DataType: "json"}, "CLOB(2G)"},
		{domain.ColumnInfo{DataType: "varbinary", ColumnType: "varbinary(512)"}, "VARBINARY(512)"},
		{domain.ColumnInfo{DataType: "decimal", ColumnType: "decimal(20,4)"}, "DECIMAL(20,4)"},
		{domain.ColumnInfo{DataType: "uuid"}, "VARCHAR(36)"},
	}
	for _, tc := range tests {
		if got := db2TargetType(tc.col); got != tc.want {
			t.Fatalf("%+v -> %s want %s", tc.col, got, tc.want)
		}
	}
}

func TestDRDAHandshakeAndQueryTranscript(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	c := &drdaClient{conn: clientConn, database: "SAMPLE            ", endian: binary.LittleEndian, ds: domain.DataSource{Username: "alice", Password: "topsecret"}}
	serverErr := make(chan error, 1)
	go func() {
		// First chain: EXCSAT + ACCSEC.
		p1, err := readDSS(serverConn)
		if err != nil {
			serverErr <- err
			return
		}
		p2, err := readDSS(serverConn)
		if err != nil {
			serverErr <- err
			return
		}
		if p1.code != cpEXCSAT || p2.code != cpACCSEC {
			serverErr <- &testErr{"unexpected handshake request"}
			return
		}
		serverPriv := big.NewInt(7)
		token := dhPublic(serverPriv)
		if _, err = sendDSS(serverConn, packDDM(cpEXCSATRD, nil), 1, false, false); err != nil {
			serverErr <- err
			return
		}
		accBody := join(packUint(cpSECMEC, secmecEUSRIDPWD, 2), packParam(cpSECTKN, token))
		if _, err = sendDSS(serverConn, packDDM(cpACCSECRD, accBody), 2, false, true); err != nil {
			serverErr <- err
			return
		}

		// Second chain: encrypted SECCHK + ACCRDB. Ensure credentials are not visible.
		p3, err := readDSS(serverConn)
		if err != nil {
			serverErr <- err
			return
		}
		p4, err := readDSS(serverConn)
		if err != nil {
			serverErr <- err
			return
		}
		if p3.code != cpSECCHK || p4.code != cpACCRDB {
			serverErr <- &testErr{"unexpected authentication request"}
			return
		}
		u, _ := encodeCP500("alice")
		pw, _ := encodeCP500("topsecret")
		if bytes.Contains(p3.body, u) || bytes.Contains(p3.body, pw) || bytes.Contains(p3.body, []byte("topsecret")) {
			serverErr <- &testErr{"credential leaked in SECCHK"}
			return
		}
		if _, err = sendDSS(serverConn, packDDM(cpSECCHKRM, nil), 1, false, false); err != nil {
			serverErr <- err
			return
		}
		if _, err = sendDSS(serverConn, packDDM(0x2201, nil), 2, false, true); err != nil {
			serverErr <- err
			return
		}

		// Query chain.
		q1, err := readDSS(serverConn)
		if err != nil {
			serverErr <- err
			return
		}
		q2, err := readDSS(serverConn)
		if err != nil {
			serverErr <- err
			return
		}
		q3, err := readDSS(serverConn)
		if err != nil {
			serverErr <- err
			return
		}
		if q1.code != cpPRPSQLSTT || q2.code != cpSQLSTT || q3.code != cpOPNQRY {
			serverErr <- &testErr{"unexpected query request"}
			return
		}
		if _, err = sendDSS(serverConn, packDDM(cpOPNQRYRM, packUint(cpQRYINSID, 1, 8)), 1, false, false); err != nil {
			serverErr <- err
			return
		}
		desc := []byte{6, 0x76, 0xd0, drdaNVarchar, 0, 32}
		if _, err = sendDSS(serverConn, packDDM(cpQRYDSC, desc), 2, false, false); err != nil {
			serverErr <- err
			return
		}
		data := []byte{0xff, 0, 0, 0, 5, 'h', 'e', 'l', 'l', 'o'}
		if _, err = sendDSS(serverConn, packDDM(cpQRYDTA, data), 3, false, false); err != nil {
			serverErr <- err
			return
		}
		if _, err = sendDSS(serverConn, packDDM(cpENDQRYRM, nil), 4, false, true); err != nil {
			serverErr <- err
			return
		}
		serverErr <- nil
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.handshake(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := c.query(ctx, "VALUES VARCHAR('hello')")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || len(rows[0]) != 1 || string(rows[0][0].Data) != "hello" {
		t.Fatalf("rows=%+v", rows)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

type testErr struct{ s string }

func (e *testErr) Error() string { return e.s }

func TestDB2TimestampCanonicalization(t *testing.T) {
	raw := []byte("2026-08-30-14.31.36.123456")
	d := drdaFieldDesc{typ: drdaNTimestamp, param: [2]byte{0, byte(len(raw))}}
	buf := append([]byte{0}, raw...)
	cell, err := readDRDAField(&byteCursor{b: buf}, d, binary.LittleEndian)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(cell.Data); got != "2026-08-30 14:31:36.123456" {
		t.Fatalf("timestamp=%q", got)
	}
}

func TestDB2IdentitySpecPreservesModeSeedIncrement(t *testing.T) {
	cases := []struct {
		extra  string
		on     bool
		always bool
		seed   string
		inc    string
	}{
		{"IDENTITY_ALWAYS(100,5)", true, true, "100", "5"},
		{"IDENTITY_BY_DEFAULT(-10,-2)", true, false, "-10", "-2"},
		{"IDENTITY(7,3)", true, false, "7", "3"},
		{"auto_increment", true, false, "1", "1"},
		{"", false, false, "", ""},
	}
	for _, tc := range cases {
		s, err := db2IdentitySpecForColumn(domain.ColumnInfo{Extra: tc.extra})
		if err != nil {
			t.Fatalf("%q: %v", tc.extra, err)
		}
		if s.enabled != tc.on || s.always != tc.always || (tc.on && (s.seed != tc.seed || s.increment != tc.inc)) {
			t.Fatalf("%q => %+v", tc.extra, s)
		}
	}
	for _, bad := range []string{"IDENTITY_ALWAYS(1;DROP,1)", "IDENTITY_BY_DEFAULT(1,0)", "IDENTITY(1.5,1)"} {
		if _, err := db2IdentitySpecForColumn(domain.ColumnInfo{Extra: bad}); err == nil {
			t.Fatalf("unsafe identity %q must fail", bad)
		}
	}
}

func TestDB2AlwaysIdentityUsesByDefaultDuringPropagation(t *testing.T) {
	cols := []domain.ColumnInfo{
		{Name: "id", DataType: "bigint", Extra: "IDENTITY_ALWAYS(100,5)"},
		{Name: "name", DataType: "varchar"},
	}
	sql, err := buildDB2PreparedUpsert("APP", "T", cols, []string{"id"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(sql, "OVERRIDING SYSTEM VALUE") {
		t.Fatalf("Db2 LUW propagation SQL must not use IBM i override syntax: %s", sql)
	}
	if !strings.Contains(sql, `WHEN NOT MATCHED THEN INSERT ("id","name") VALUES`) {
		t.Fatalf("identity insert branch missing: %s", sql)
	}
	matched := strings.Split(sql, " WHEN NOT MATCHED")[0]
	updatePos := strings.Index(matched, " WHEN MATCHED THEN UPDATE SET ")
	if updatePos >= 0 && strings.Contains(matched[updatePos:], `T."id"=S."id"`) {
		t.Fatalf("GENERATED ALWAYS identity must not be updated in matched branch: %s", sql)
	}
	stmts, err := db2FinalizeGeneratedStatements("APP", "T", cols)
	if err != nil || len(stmts) != 1 || stmts[0] != `ALTER TABLE "APP"."T" ALTER COLUMN "id" SET GENERATED ALWAYS` {
		t.Fatalf("finalize statements=%v err=%v", stmts, err)
	}
}

func TestDB2GeneratedExpressionWriterIsFailSafe(t *testing.T) {
	cols := []domain.ColumnInfo{
		{Name: "id", DataType: "bigint"},
		{Name: "computed_total", DataType: "decimal", Extra: "GENERATED_A"},
		{Name: "name", DataType: "varchar"},
	}
	row := []connector.Value{{Raw: []byte("1")}, {Raw: []byte("99.00")}, {Raw: []byte("alice")}}
	wcols, wrow, err := db2WritableRow(cols, row, []string{"id"})
	if err != nil {
		t.Fatal(err)
	}
	if len(wcols) != 2 || len(wrow) != 2 || wcols[0].Name != "id" || wcols[1].Name != "name" {
		t.Fatalf("unexpected writable projection: cols=%+v row=%+v", wcols, wrow)
	}
	if _, _, err := db2WritableRow(cols, row, []string{"computed_total"}); err == nil {
		t.Fatal("generated expression migration key must fail closed")
	}
}

func TestDB2IdentityColumnDefinitionOrder(t *testing.T) {
	ddl, err := db2ColumnDefinition(domain.ColumnInfo{Name: "ID", DataType: "bigint", Nullable: true, Extra: "IDENTITY_ALWAYS(100,5)"})
	if err != nil {
		t.Fatal(err)
	}
	want := `"ID" BIGINT NOT NULL GENERATED BY DEFAULT AS IDENTITY (START WITH 100, INCREMENT BY 5)`
	if ddl != want {
		t.Fatalf("ddl=%q want=%q", ddl, want)
	}
	if _, err := db2ColumnDefinition(domain.ColumnInfo{Name: "X", DataType: "int", Extra: "GENERATED_A"}); err == nil {
		t.Fatal("generated expression auto-create must fail closed")
	}
}

func TestDB2PackedDecimalEncodeExact(t *testing.T) {
	tests := []struct {
		raw       string
		precision int
		scale     int
		want      []byte
	}{
		{"123.45", 5, 2, []byte{0x12, 0x34, 0x5c}},
		{"-0.12", 5, 2, []byte{0x00, 0x01, 0x2d}},
		{"1.23e2", 5, 2, []byte{0x12, 0x30, 0x0c}},
	}
	for _, tc := range tests {
		got, err := encodeDB2PackedDecimal(tc.raw, tc.precision, tc.scale)
		if err != nil {
			t.Fatalf("%s: %v", tc.raw, err)
		}
		if !bytes.Equal(got, tc.want) {
			t.Fatalf("%s: got %x want %x", tc.raw, got, tc.want)
		}
	}
	if _, err := encodeDB2PackedDecimal("1.234", 5, 2); err == nil {
		t.Fatal("rounding-required decimal must fail closed")
	}
	if _, err := encodeDB2PackedDecimal("123456", 5, 0); err == nil {
		t.Fatal("precision overflow must fail closed")
	}
}

func TestDB2PreparedUpsertUsesMarkers(t *testing.T) {
	cols := []domain.ColumnInfo{
		{Name: "ID", DataType: "bigint", ColumnType: "BIGINT"},
		{Name: "AMOUNT", DataType: "decimal", ColumnType: "DECIMAL(20,4)"},
		{Name: "PAYLOAD", DataType: "blob", ColumnType: "BLOB"},
	}
	sql, err := buildDB2PreparedUpsert("APP", "T", cols, []string{"ID"})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`CAST(? AS BIGINT)`, `CAST(? AS DECIMAL(20,4))`, `CAST(? AS BLOB(2G))`} {
		if !strings.Contains(sql, want) {
			t.Fatalf("prepared sql missing %q: %s", want, sql)
		}
	}
	if strings.Contains(sql, "123.45") || strings.Contains(sql, "X'") {
		t.Fatalf("prepared SQL must not inline row values: %s", sql)
	}
}

func TestDB2PreparedBatchStreamsLargeEXTDTA(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	c := &drdaClient{conn: clientConn, database: "SAMPLE            ", endian: binary.LittleEndian}
	payload := bytes.Repeat([]byte{0x5a, 0x00, 0xff, 0x7e}, 30000) // 120 KiB, spans multiple DSS segments.
	cols := []domain.ColumnInfo{
		{Name: "ID", DataType: "bigint", ColumnType: "BIGINT"},
		{Name: "PAYLOAD", DataType: "blob", ColumnType: "BLOB"},
	}
	rows := [][]connector.Value{{{Raw: []byte("7")}, {Raw: payload}}}
	sql, err := buildDB2PreparedUpsert("APP", "T", cols, []string{"ID"})
	if err != nil {
		t.Fatal(err)
	}
	serverErr := make(chan error, 1)
	go func() {
		want := []uint16{cpPRPSQLSTT, cpSQLSTT, cpEXCSQLSTT, cpSQLDTA, cpEXTDTA}
		for i, cp := range want {
			p, err := readDSS(serverConn)
			if err != nil {
				serverErr <- err
				return
			}
			if p.code != cp {
				serverErr <- fmt.Errorf("request %d code=0x%04x want=0x%04x", i, p.code, cp)
				return
			}
			if p.code == cpSQLSTT && bytes.Contains(p.body, payload[:32]) {
				serverErr <- errors.New("LOB bytes leaked into SQL text")
				return
			}
			if p.code == cpSQLDTA {
				params := parseParams(p.body)
				if len(params[cpFDODSC]) != 1 || len(params[cpFDODTA]) != 1 {
					serverErr <- fmt.Errorf("SQLDTA missing FDODSC/FDODTA: %x", p.body)
					return
				}
				if !bytes.Contains(params[cpFDODSC][0], []byte{drdaNLOBBytes, 0x80, 0x09}) {
					serverErr <- fmt.Errorf("SQLDTA descriptor does not advertise out-of-line BLOB: %x", params[cpFDODSC][0])
					return
				}
			}
			if p.code == cpEXTDTA {
				if len(p.body) != len(payload)+1 || p.body[0] != 0 || !bytes.Equal(p.body[1:], payload) {
					serverErr <- fmt.Errorf("EXTDTA reassembly mismatch len=%d", len(p.body))
					return
				}
			}
		}
		if _, err := sendDSS(serverConn, packDDM(cpSQLCARD, nil), 2, false, true); err != nil {
			serverErr <- err
			return
		}
		serverErr <- nil
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	written, err := c.execPreparedBatch(ctx, sql, cols, rows)
	if err != nil {
		t.Fatal(err)
	}
	if written != 1 {
		t.Fatalf("written=%d", written)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestDB2ExtendedDSSMultipleSegmentsRoundTrip(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	body := bytes.Repeat([]byte("abcdef0123456789"), 10000)
	errCh := make(chan error, 1)
	go func() {
		_, err := sendDSSPayload(clientConn, cpEXTDTA, body, 9, false, true)
		errCh <- err
	}()
	p, err := readDSS(serverConn)
	if err != nil {
		t.Fatal(err)
	}
	if p.code != cpEXTDTA || p.corr != 9 || !bytes.Equal(p.body, body) {
		t.Fatalf("extended DSS mismatch code=%x corr=%d len=%d", p.code, p.corr, len(p.body))
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}
