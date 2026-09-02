package sqlserverconnector

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
	"strings"
	"testing"
	"time"
)

func TestPreloginPacketAndVersion(t *testing.T) {
	p := buildPreloginPacket()
	if p[0] != tdsPrelogin || int(binary.BigEndian.Uint16(p[2:4])) != len(p) {
		t.Fatalf("bad packet: %x", p)
	}
	body := []byte{0x00, 0x00, 0x06, 0x00, 0x06, 0xff, 16, 0, 0x10, 0x00, 0, 0}
	v, err := parsePreloginVersion(body)
	if err != nil || v != "16.0.4096" {
		t.Fatalf("v=%q err=%v", v, err)
	}
}

func TestNativeTDSProbeAgainstFakeServer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, _ := ln.Accept()
		if c == nil {
			return
		}
		defer c.Close()
		header := make([]byte, 8)
		if _, err := io.ReadFull(c, header); err != nil {
			return
		}
		packetLen := int(binary.BigEndian.Uint16(header[2:4]))
		if packetLen < len(header) {
			return
		}
		if _, err := io.CopyN(io.Discard, c, int64(packetLen-len(header))); err != nil {
			return
		}
		body := []byte{0x00, 0x00, 0x06, 0x00, 0x06, 0xff, 15, 0, 0x07, 0xd0, 0, 0}
		resp := make([]byte, 8+len(body))
		resp[0] = 0x04
		resp[1] = 1
		binary.BigEndian.PutUint16(resp[2:4], uint16(len(resp)))
		resp[6] = 1
		copy(resp[8:], body)
		_, _ = c.Write(resp)
	}()
	addr := ln.Addr().(*net.TCPAddr)
	c, _ := NewFactory().New(domain.DataSource{Type: domain.DataSourceSQLServer, Host: "127.0.0.1", Port: addr.Port})
	if err := c.TestConnection(context.Background()); err != nil {
		t.Fatal(err)
	}
	v, err := c.GetVersion(context.Background())
	if err != nil || v != "15.0.2000" {
		t.Fatalf("v=%q err=%v", v, err)
	}
}

func writeFakeTDSMessage(t *testing.T, c net.Conn, typ byte, body []byte) {
	t.Helper()
	resp := make([]byte, 8+len(body))
	resp[0] = typ
	resp[1] = tdsEOM
	binary.BigEndian.PutUint16(resp[2:4], uint16(len(resp)))
	resp[6] = 1
	copy(resp[8:], body)
	if _, err := c.Write(resp); err != nil {
		t.Errorf("write fake TDS: %v", err)
	}
}

func fakePreloginBody(versionMajor byte) []byte {
	// option table: VERSION at offset 11 len 6, ENCRYPTION at offset 17 len 1.
	return []byte{
		0x00, 0x00, 0x0b, 0x00, 0x06,
		0x01, 0x00, 0x11, 0x00, 0x01,
		0xff,
		versionMajor, 0x00, 0x03, 0xe8, 0x00, 0x00,
		0x02, // ENCRYPT_NOT_SUP; safe for fake LOGIN7 tests.
	}
}

func fakeLoginAckBody() []byte {
	b := []byte{tokLoginAck, 0x01, 0x00, 0x00, tokDone}
	b = append(b, make([]byte, 12)...)
	return b
}

func fakeNVarCharResult(value string) []byte {
	b := []byte{tokColMetadata, 0x01, 0x00}
	b = append(b, make([]byte, 6)...)
	b = append(b, typeNVarChar)
	// max length 8000 bytes + five-byte collation.
	b = append(b, 0x40, 0x1f, 0, 0, 0, 0, 0)
	b = append(b, 0x01, 'x', 0x00)
	b = append(b, tokRow)
	v := utf16Bytes(value)
	b = append(b, byte(len(v)), byte(len(v)>>8))
	b = append(b, v...)
	b = append(b, tokDone)
	b = append(b, make([]byte, 12)...)
	return b
}

func TestExperimentalNativeTDSSessionAgainstFakeServer(t *testing.T) {
	t.Setenv("QMIGRATION_EXPERIMENTAL_SQLSERVER_NATIVE", "1")
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	serverErr := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer c.Close()
		cl := &tdsClient{conn: c, packetSize: 4096}
		typ, _, err := cl.readMessage(context.Background())
		if err != nil || typ != tdsPrelogin {
			serverErr <- fmt.Errorf("PRELOGIN typ=%x err=%v", typ, err)
			return
		}
		writeFakeTDSMessage(t, c, tdsReply, fakePreloginBody(16))
		typ, login, err := cl.readMessage(context.Background())
		if err != nil || typ != tdsLogin7 || len(login) < 94 {
			serverErr <- fmt.Errorf("LOGIN7 typ=%x len=%d err=%v", typ, len(login), err)
			return
		}
		writeFakeTDSMessage(t, c, tdsReply, fakeLoginAckBody())
		for i, v := range []string{"1", "16.0.1000"} {
			typ, sql, err := cl.readMessage(context.Background())
			if err != nil || typ != tdsSQLBatch || len(sql) == 0 {
				serverErr <- fmt.Errorf("SQLBatch[%d] typ=%x err=%v", i, typ, err)
				return
			}
			writeFakeTDSMessage(t, c, tdsReply, fakeNVarCharResult(v))
		}
		serverErr <- nil
	}()
	addr := ln.Addr().(*net.TCPAddr)
	conn, err := NewFactory().New(domain.DataSource{
		Type: domain.DataSourceSQLServer, Host: "127.0.0.1", Port: addr.Port,
		Username: "sa", Password: "secret", Database: "master",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.TestConnection(context.Background()); err != nil {
		t.Fatal(err)
	}
	v, err := conn.GetVersion(context.Background())
	if err != nil || v != "16.0.1000" {
		t.Fatalf("version=%q err=%v", v, err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestLogin7PasswordIsObfuscated(t *testing.T) {
	packet := buildLogin7("db", "sa", "secret", "master", 4096)
	if len(packet) < 94 {
		t.Fatalf("LOGIN7 too short: %d", len(packet))
	}
	if bytes.Contains(packet, utf16Bytes("secret")) {
		t.Fatal("LOGIN7 contains clear UTF-16 password")
	}
	if !bytes.Contains(packet, obfuscatePassword("secret")) {
		t.Fatal("LOGIN7 does not contain TDS-obfuscated password")
	}
}

func testTDSServerCertificate(t *testing.T) (tls.Certificate, string) {
	t.Helper()
	now := time.Now()
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	caTmpl := &x509.Certificate{SerialNumber: big.NewInt(100), Subject: pkix.Name{CommonName: "QMigration TDS Test CA"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	serverKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	serverTmpl := &x509.Certificate{SerialNumber: big.NewInt(101), Subject: pkix.Name{CommonName: "localhost"}, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	serverDER, err := x509.CreateCertificate(rand.Reader, serverTmpl, caCert, &serverKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(serverKey)})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return cert, string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}))
}

func TestExperimentalNativeTDSTLSRequired(t *testing.T) {
	t.Setenv("QMIGRATION_EXPERIMENTAL_SQLSERVER_NATIVE", "1")
	cert, ca := testTDSServerCertificate(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	serverErr := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(8 * time.Second))
		cl := &tdsClient{conn: c, packetSize: 4096}
		typ, _, err := cl.readMessage(context.Background())
		if err != nil || typ != tdsPrelogin {
			serverErr <- fmt.Errorf("prelogin type=%x err=%v", typ, err)
			return
		}
		body := fakePreloginBody(16)
		body[len(body)-1] = 0x01 // ENCRYPT_ON
		writeFakeTDSMessage(t, c, tdsReply, body)
		framed := &tdsTLSHandshakeConn{raw: c}
		tlsServer := tls.Server(framed, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
		if err := tlsServer.Handshake(); err != nil {
			serverErr <- fmt.Errorf("tls handshake: %w", err)
			return
		}
		framed.switchToRaw()
		secure := &tdsClient{conn: tlsServer, packetSize: 4096}
		typ, login, err := secure.readMessage(context.Background())
		if err != nil || typ != tdsLogin7 || len(login) < 94 {
			serverErr <- fmt.Errorf("login type=%x len=%d err=%v", typ, len(login), err)
			return
		}
		writeFakeTDSMessage(t, tlsServer, tdsReply, fakeLoginAckBody())
		typ, _, err = secure.readMessage(context.Background())
		if err != nil || typ != tdsSQLBatch {
			serverErr <- fmt.Errorf("sql type=%x err=%v", typ, err)
			return
		}
		writeFakeTDSMessage(t, tlsServer, tdsReply, fakeNVarCharResult("1"))
		serverErr <- nil
	}()
	addr := ln.Addr().(*net.TCPAddr)
	raw, err := NewFactory().New(domain.DataSource{Type: domain.DataSourceSQLServer, Host: "127.0.0.1", Port: addr.Port, Username: "sa", Password: "secret", Database: "master", TLSMode: domain.TLSModeRequired, TLSServerName: "localhost", TLSCACert: ca})
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if err := raw.TestConnection(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestSQLServerTLSRequiredNeverDowngrades(t *testing.T) {
	t.Setenv("QMIGRATION_EXPERIMENTAL_SQLSERVER_NATIVE", "1")
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, _ := ln.Accept()
		if c == nil {
			return
		}
		defer c.Close()
		cl := &tdsClient{conn: c, packetSize: 4096}
		_, _, _ = cl.readMessage(context.Background())
		writeFakeTDSMessage(t, c, tdsReply, fakePreloginBody(16))
	}()
	addr := ln.Addr().(*net.TCPAddr)
	raw, _ := NewFactory().New(domain.DataSource{Type: domain.DataSourceSQLServer, Host: "127.0.0.1", Port: addr.Port, Username: "sa", Password: "secret", Database: "master", TLSMode: domain.TLSModeRequired})
	err = raw.TestConnection(context.Background())
	if err == nil || !strings.Contains(err.Error(), "TLS REQUIRED") {
		t.Fatalf("expected required TLS failure, got %v", err)
	}
}

func TestExperimentalNativeTDSPreferredUsesLoginOnlyTLSWhenServerOff(t *testing.T) {
	t.Setenv("QMIGRATION_EXPERIMENTAL_SQLSERVER_NATIVE", "1")
	cert, ca := testTDSServerCertificate(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	serverErr := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(8 * time.Second))
		rawClient := &tdsClient{conn: c, packetSize: 4096}
		typ, _, err := rawClient.readMessage(context.Background())
		if err != nil || typ != tdsPrelogin {
			serverErr <- fmt.Errorf("prelogin typ=%x err=%v", typ, err)
			return
		}
		body := fakePreloginBody(16)
		body[len(body)-1] = 0x00 // ENCRYPT_OFF => LOGIN7 only TLS.
		writeFakeTDSMessage(t, c, tdsReply, body)
		framed := &tdsTLSHandshakeConn{raw: c}
		tlsServer := tls.Server(framed, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
		if err := tlsServer.Handshake(); err != nil {
			serverErr <- err
			return
		}
		framed.switchToRaw()
		secureClient := &tdsClient{conn: tlsServer, packetSize: 4096}
		typ, login, err := secureClient.readMessage(context.Background())
		if err != nil || typ != tdsLogin7 || len(login) < 94 {
			serverErr <- fmt.Errorf("secure login typ=%x len=%d err=%v", typ, len(login), err)
			return
		}
		// Login response and all following traffic are plaintext in login-only mode.
		writeFakeTDSMessage(t, c, tdsReply, fakeLoginAckBody())
		typ, _, err = rawClient.readMessage(context.Background())
		if err != nil || typ != tdsSQLBatch {
			serverErr <- fmt.Errorf("plaintext SQL typ=%x err=%v", typ, err)
			return
		}
		writeFakeTDSMessage(t, c, tdsReply, fakeNVarCharResult("1"))
		serverErr <- nil
	}()
	addr := ln.Addr().(*net.TCPAddr)
	raw, err := NewFactory().New(domain.DataSource{Type: domain.DataSourceSQLServer, Host: "127.0.0.1", Port: addr.Port, Username: "sa", Password: "secret", Database: "master", TLSMode: domain.TLSModePreferred, TLSServerName: "localhost", TLSCACert: ca})
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if err := raw.TestConnection(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func fakeNVarCharRows(rows [][]string) []byte {
	cols := 0
	if len(rows) > 0 {
		cols = len(rows[0])
	}
	b := []byte{tokColMetadata, byte(cols), byte(cols >> 8)}
	for i := 0; i < cols; i++ {
		b = append(b, make([]byte, 6)...)
		b = append(b, typeNVarChar)
		b = append(b, 0x40, 0x1f, 0, 0, 0, 0, 0)
		name := fmt.Sprintf("c%d", i+1)
		b = append(b, byte(len(name)))
		b = append(b, utf16Bytes(name)...)
	}
	for _, row := range rows {
		b = append(b, tokRow)
		for _, value := range row {
			v := utf16Bytes(value)
			b = append(b, byte(len(v)), byte(len(v)>>8))
			b = append(b, v...)
		}
	}
	b = append(b, tokDone)
	b = append(b, make([]byte, 12)...)
	return b
}

func TestSQLServerCDCPositionAndWindowAgainstFakeTDS(t *testing.T) {
	t.Setenv("QMIGRATION_EXPERIMENTAL_SQLSERVER_NATIVE", "1")
	t.Setenv("QMIGRATION_EXPERIMENTAL_SQLSERVER_CDC", "1")
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	serverErr := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer c.Close()
		cl := &tdsClient{conn: c, packetSize: 4096}
		typ, _, err := cl.readMessage(context.Background())
		if err != nil || typ != tdsPrelogin {
			serverErr <- fmt.Errorf("prelogin %x %v", typ, err)
			return
		}
		writeFakeTDSMessage(t, c, tdsReply, fakePreloginBody(16))
		typ, _, err = cl.readMessage(context.Background())
		if err != nil || typ != tdsLogin7 {
			serverErr <- fmt.Errorf("login %x %v", typ, err)
			return
		}
		writeFakeTDSMessage(t, c, tdsReply, fakeLoginAckBody())
		responses := [][][]string{
			{{"1"}},
			{{"0x00000000000000000005"}},
			{{"0x00000000000000000006"}, {"0x00000000000000000007"}},
		}
		for i, rows := range responses {
			typ, _, err = cl.readMessage(context.Background())
			if err != nil || typ != tdsSQLBatch {
				serverErr <- fmt.Errorf("query[%d] %x %v", i, typ, err)
				return
			}
			writeFakeTDSMessage(t, c, tdsReply, fakeNVarCharRows(rows))
		}
		serverErr <- nil
	}()
	addr := ln.Addr().(*net.TCPAddr)
	raw, err := NewFactory().New(domain.DataSource{Type: domain.DataSourceSQLServer, Host: "127.0.0.1", Port: addr.Port, Username: "sa", Password: "secret", Database: "app", TLSMode: domain.TLSModeDisable})
	if err != nil {
		t.Fatal(err)
	}
	c := raw.(*Connector)
	defer c.Close()
	pos, err := c.CurrentCDCPosition(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if pos.PositionType != "SQLSERVER_LSN" || pos.PositionValue != "0x00000000000000000005" {
		t.Fatalf("pos=%+v", pos)
	}
	from, to, empty, err := c.NextCDCWindow(context.Background(), pos.PositionValue, 16)
	if err != nil {
		t.Fatal(err)
	}
	if empty || from != "0x00000000000000000006" || to != "0x00000000000000000007" {
		t.Fatalf("window from=%s to=%s empty=%v", from, to, empty)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestSQLServerReadCDCChangesAgainstFakeTDS(t *testing.T) {
	t.Setenv("QMIGRATION_EXPERIMENTAL_SQLSERVER_NATIVE", "1")
	t.Setenv("QMIGRATION_EXPERIMENTAL_SQLSERVER_CDC", "1")
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	serverErr := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer c.Close()
		cl := &tdsClient{conn: c, packetSize: 4096}
		typ, _, err := cl.readMessage(context.Background())
		if err != nil || typ != tdsPrelogin {
			serverErr <- err
			return
		}
		writeFakeTDSMessage(t, c, tdsReply, fakePreloginBody(16))
		typ, _, err = cl.readMessage(context.Background())
		if err != nil || typ != tdsLogin7 {
			serverErr <- err
			return
		}
		writeFakeTDSMessage(t, c, tdsReply, fakeLoginAckBody())
		typ, sql, err := cl.readMessage(context.Background())
		if err != nil || typ != tdsSQLBatch {
			serverErr <- err
			return
		}
		decoded := decodeUTF16(sql)
		if !strings.Contains(decoded, "fn_cdc_get_all_changes_dbo_orders") {
			serverErr <- fmt.Errorf("unexpected CDC SQL: %s", decoded)
			return
		}
		writeFakeTDSMessage(t, c, tdsReply, fakeNVarCharRows([][]string{{"0x00000000000000000009", "0x00000000000000000001", "2", "1", "2026-08-30T06:00:00.000", "42"}}))
		serverErr <- nil
	}()
	addr := ln.Addr().(*net.TCPAddr)
	raw, _ := NewFactory().New(domain.DataSource{Type: domain.DataSourceSQLServer, Host: "127.0.0.1", Port: addr.Port, Username: "sa", Password: "secret", Database: "app", TLSMode: domain.TLSModeDisable})
	c := raw.(*Connector)
	defer c.Close()
	cap := CDCCapture{Schema: "dbo", Table: "orders", Instance: "dbo_orders", Columns: []domain.ColumnInfo{{Name: "id", DataType: "int"}}}
	changes, err := c.ReadCDCChanges(context.Background(), cap, "0x00000000000000000008", "0x00000000000000000009")
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 || changes[0].Operation != 2 || string(changes[0].Values[0].Raw) != "42" {
		t.Fatalf("changes=%+v", changes)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestLexCompareCompositeBoundsAreExact(t *testing.T) {
	keys := []string{"tenant_id", "order_id"}
	cols := []domain.ColumnInfo{{Name: "tenant_id", DataType: "int"}, {Name: "order_id", DataType: "int"}}
	vals := []connector.Value{{Raw: []byte("10")}, {Raw: []byte("20")}}
	lower := lexCompare(keys, cols, vals, ">=")
	wantLowerParts := []string{
		"([tenant_id]>10)",
		"([tenant_id]=10 AND [order_id]>20)",
		"([tenant_id]=10 AND [order_id]=20)",
	}
	for _, part := range wantLowerParts {
		if !strings.Contains(lower, part) {
			t.Fatalf("lower=%s missing %s", lower, part)
		}
	}
	if strings.Contains(lower, "[tenant_id]>=10") {
		t.Fatalf("composite lower bound is over-broad: %s", lower)
	}
	upper := lexCompare(keys, cols, vals, "<")
	if !strings.Contains(upper, "([tenant_id]<10)") || !strings.Contains(upper, "([tenant_id]=10 AND [order_id]<20)") {
		t.Fatalf("upper=%s", upper)
	}
}

func TestSQLServerExperimentalCapabilitiesIncludePlannerAndFlowControl(t *testing.T) {
	t.Setenv("QMIGRATION_EXPERIMENTAL_SQLSERVER_NATIVE", "1")
	d := NewFactory().Capabilities(domain.DataSourceSQLServer)
	if !d.Has(connector.CapabilityKeysetBoundary) || !d.Has(connector.CapabilityRuntimeLoad) {
		t.Fatalf("capabilities=%v", d.Capabilities)
	}
}

func TestSQLServerPartitionDescriptorRoundTrip(t *testing.T) {
	in := sqlServerPartitionDescriptor{Function: "pf_orders", Column: "tenant_id", Number: 17}
	raw, err := encodeSQLServerPartition(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := decodeSQLServerPartition(raw)
	if err != nil {
		t.Fatal(err)
	}
	if out != in {
		t.Fatalf("out=%+v want=%+v", out, in)
	}
	if _, err := decodeSQLServerPartition(`{"function":"pf","column":"id","number":0}`); err == nil {
		t.Fatal("expected invalid partition number")
	}
}

func TestSQLServerOrderedKeysetBoundaryPlannerAgainstFakeTDS(t *testing.T) {
	t.Setenv("QMIGRATION_EXPERIMENTAL_SQLSERVER_NATIVE", "1")
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	serverErr := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer c.Close()
		cl := &tdsClient{conn: c, packetSize: 4096}
		typ, _, err := cl.readMessage(context.Background())
		if err != nil || typ != tdsPrelogin {
			serverErr <- fmt.Errorf("prelogin typ=%x err=%v", typ, err)
			return
		}
		writeFakeTDSMessage(t, c, tdsReply, fakePreloginBody(16))
		typ, _, err = cl.readMessage(context.Background())
		if err != nil || typ != tdsLogin7 {
			serverErr <- fmt.Errorf("login typ=%x err=%v", typ, err)
			return
		}
		writeFakeTDSMessage(t, c, tdsReply, fakeLoginAckBody())
		typ, sqlRaw, err := cl.readMessage(context.Background())
		if err != nil || typ != tdsSQLBatch {
			serverErr <- fmt.Errorf("query typ=%x err=%v", typ, err)
			return
		}
		sql := decodeUTF16(sqlRaw)
		for _, want := range []string{"NTILE(3)", "[tenant_id]>10", "[tenant_id]=10 AND [order_id]>20", "[tenant_id]=10 AND [order_id]=20", "[tenant_id]<30"} {
			if !strings.Contains(sql, want) {
				serverErr <- fmt.Errorf("boundary SQL missing %q: %s", want, sql)
				return
			}
		}
		if strings.Contains(sql, "[tenant_id]>=10") {
			serverErr <- fmt.Errorf("over-broad lower bound: %s", sql)
			return
		}
		writeFakeTDSMessage(t, c, tdsReply, fakeNVarCharRows([][]string{{"15", "90"}, {"23", "4"}}))
		serverErr <- nil
	}()
	addr := ln.Addr().(*net.TCPAddr)
	raw, err := NewFactory().New(domain.DataSource{Type: domain.DataSourceSQLServer, Host: "127.0.0.1", Port: addr.Port, Username: "sa", Password: "secret", Database: "app", TLSMode: domain.TLSModeDisable})
	if err != nil {
		t.Fatal(err)
	}
	c := raw.(*Connector)
	defer c.Close()
	bounds, err := c.PlanKeysetBoundaries(context.Background(), connector.KeysetBoundaryRequest{
		Schema: "dbo", Table: "orders", Keys: []string{"tenant_id", "order_id"}, Partitions: 3,
		Columns:    []domain.ColumnInfo{{Name: "tenant_id", DataType: "int"}, {Name: "order_id", DataType: "int"}},
		LowerBound: []connector.Value{{Raw: []byte("10")}, {Raw: []byte("20")}},
		UpperBound: []connector.Value{{Raw: []byte("30")}, {Raw: []byte("0")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(bounds) != 2 || string(bounds[0][0].Raw) != "15" || string(bounds[1][1].Raw) != "4" {
		t.Fatalf("bounds=%+v", bounds)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestSQLServerPartitionReadPredicateAgainstFakeTDS(t *testing.T) {
	t.Setenv("QMIGRATION_EXPERIMENTAL_SQLSERVER_NATIVE", "1")
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	serverErr := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer c.Close()
		cl := &tdsClient{conn: c, packetSize: 4096}
		typ, _, err := cl.readMessage(context.Background())
		if err != nil || typ != tdsPrelogin {
			serverErr <- fmt.Errorf("prelogin typ=%x err=%v", typ, err)
			return
		}
		writeFakeTDSMessage(t, c, tdsReply, fakePreloginBody(16))
		typ, _, err = cl.readMessage(context.Background())
		if err != nil || typ != tdsLogin7 {
			serverErr <- fmt.Errorf("login typ=%x err=%v", typ, err)
			return
		}
		writeFakeTDSMessage(t, c, tdsReply, fakeLoginAckBody())
		typ, sqlRaw, err := cl.readMessage(context.Background())
		if err != nil || typ != tdsSQLBatch {
			serverErr <- fmt.Errorf("query typ=%x err=%v", typ, err)
			return
		}
		sql := decodeUTF16(sqlRaw)
		if !strings.Contains(sql, "$PARTITION.[pf_orders]([tenant_id])=3") {
			serverErr <- fmt.Errorf("partition predicate missing: %s", sql)
			return
		}
		writeFakeTDSMessage(t, c, tdsReply, fakeNVarCharRows([][]string{{"101", "hello"}}))
		serverErr <- nil
	}()
	addr := ln.Addr().(*net.TCPAddr)
	raw, _ := NewFactory().New(domain.DataSource{Type: domain.DataSourceSQLServer, Host: "127.0.0.1", Port: addr.Port, Username: "sa", Password: "secret", Database: "app", TLSMode: domain.TLSModeDisable})
	c := raw.(*Connector)
	defer c.Close()
	part, _ := encodeSQLServerPartition(sqlServerPartitionDescriptor{Function: "pf_orders", Column: "tenant_id", Number: 3})
	batch, err := c.ReadBatch(context.Background(), connector.ReadBatchRequest{Schema: "dbo", Table: "orders", PrimaryKey: "id", PrimaryKeys: []string{"id"}, Columns: []domain.ColumnInfo{{Name: "id", DataType: "int"}, {Name: "name", DataType: "nvarchar"}}, StartPK: 1, EndPK: 1000, Limit: 10, Partition: part})
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Rows) != 1 || string(batch.Rows[0][1].Raw) != "hello" {
		t.Fatalf("batch=%+v", batch)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}
