package oracleconnector

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"qmigration/backend/internal/domain"
	"strings"
	"testing"
	"time"
)

func TestBuildConnectPacket(t *testing.T) {
	p := buildConnectPacket("(CONNECT_DATA=(SERVICE_NAME=ORCL))")
	if p[4] != tnsConnect || int(binary.BigEndian.Uint16(p[:2])) != len(p) {
		t.Fatalf("bad TNS header")
	}
	if off := binary.BigEndian.Uint16(p[8+18 : 8+20]); off != 58 {
		t.Fatalf("offset=%d", off)
	}
}
func TestNativeTNSProbeAgainstFakeListener(t *testing.T) {
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
		b := make([]byte, 4096)
		_, _ = c.Read(b)
		resp := make([]byte, 8)
		binary.BigEndian.PutUint16(resp[:2], 8)
		resp[4] = tnsAccept
		_, _ = c.Write(resp)
	}()
	addr := ln.Addr().(*net.TCPAddr)
	c, _ := NewFactory().New(domain.DataSource{Type: domain.DataSourceOracle, Host: "127.0.0.1", Port: addr.Port, Database: "ORCL"})
	if err := c.TestConnection(context.Background()); err != nil {
		t.Fatal(err)
	}
	v, err := c.GetVersion(context.Background())
	if err != nil || v != "oracle-tns" {
		t.Fatalf("v=%q err=%v", v, err)
	}
}

func writeTNSPacket(t *testing.T, c net.Conn, typ byte, body []byte) {
	t.Helper()
	p := make([]byte, 8+len(body))
	binary.BigEndian.PutUint16(p[:2], uint16(len(p)))
	p[4] = typ
	copy(p[8:], body)
	if _, err := c.Write(p); err != nil {
		t.Errorf("write TNS: %v", err)
	}
}

func TestNativeTNSFollowsRedirect(t *testing.T) {
	backend, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	go func() {
		c, _ := backend.Accept()
		if c == nil {
			return
		}
		defer c.Close()
		b := make([]byte, 4096)
		_, _ = c.Read(b)
		body := []byte{0x01, 0x39}
		writeTNSPacket(t, c, tnsAccept, body)
	}()
	front, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer front.Close()
	go func() {
		c, _ := front.Accept()
		if c == nil {
			return
		}
		defer c.Close()
		b := make([]byte, 4096)
		_, _ = c.Read(b)
		p := backend.Addr().(*net.TCPAddr)
		body := []byte(fmt.Sprintf("(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=127.0.0.1)(PORT=%d)))", p.Port))
		writeTNSPacket(t, c, tnsRedirect, body)
	}()
	addr := front.Addr().(*net.TCPAddr)
	raw, _ := NewFactory().New(domain.DataSource{Type: domain.DataSourceOracle, Host: "127.0.0.1", Port: addr.Port, Database: "ORCL"})
	if err := raw.TestConnection(context.Background()); err != nil {
		t.Fatal(err)
	}
	v, err := raw.GetVersion(context.Background())
	if err != nil || v != "oracle-tns-313" {
		t.Fatalf("v=%q err=%v", v, err)
	}
}

func TestParseRedirectAddress(t *testing.T) {
	h, p, err := parseRedirectAddress("(ADDRESS=(PROTOCOL=TCP)(HOST=dbscan.internal)(PORT=1522))")
	if err != nil || h != "dbscan.internal" || p != 1522 {
		t.Fatalf("h=%q p=%d err=%v", h, p, err)
	}
}

func testOracleTLSCertificate(t *testing.T) (tls.Certificate, string) {
	t.Helper()
	now := time.Now()
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	caTmpl := &x509.Certificate{SerialNumber: big.NewInt(201), Subject: pkix.Name{CommonName: "QMigration Oracle Test CA"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
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
	serverTmpl := &x509.Certificate{SerialNumber: big.NewInt(202), Subject: pkix.Name{CommonName: "localhost"}, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
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

func TestNativeOracleTCPSRequired(t *testing.T) {
	cert, ca := testOracleTLSCertificate(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	serverErr := make(chan error, 1)
	go func() {
		raw, err := ln.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer raw.Close()
		tlsConn := tls.Server(raw, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
		if err := tlsConn.Handshake(); err != nil {
			serverErr <- err
			return
		}
		defer tlsConn.Close()
		if typ, _, err := readTNSPacket(tlsConn); err != nil || typ != tnsConnect {
			serverErr <- fmt.Errorf("connect typ=%d err=%v", typ, err)
			return
		}
		writeTNSPacket(t, tlsConn, tnsAccept, []byte{0x01, 0x39})
		serverErr <- nil
	}()
	addr := ln.Addr().(*net.TCPAddr)
	raw, err := NewFactory().New(domain.DataSource{Type: domain.DataSourceOracle, Host: "127.0.0.1", Port: addr.Port, Database: "ORCL", TLSMode: domain.TLSModeRequired, TLSServerName: "localhost", TLSCACert: ca})
	if err != nil {
		t.Fatal(err)
	}
	if err := raw.TestConnection(context.Background()); err != nil {
		t.Fatal(err)
	}
	v, err := raw.GetVersion(context.Background())
	if err != nil || v != "oracle-tns-313-tcps" {
		t.Fatalf("version=%q err=%v", v, err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestNativeOracleTCPSRequiredNeverDowngrades(t *testing.T) {
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
		// Plain TNS response is deliberately not a TLS ServerHello.
		writeTNSPacket(t, c, tnsAccept, nil)
	}()
	addr := ln.Addr().(*net.TCPAddr)
	raw, _ := NewFactory().New(domain.DataSource{Type: domain.DataSourceOracle, Host: "127.0.0.1", Port: addr.Port, Database: "ORCL", TLSMode: domain.TLSModeRequired})
	err = raw.TestConnection(context.Background())
	if err == nil || !strings.Contains(err.Error(), "TCPS handshake") {
		t.Fatalf("expected fail-closed TCPS error, got %v", err)
	}
}

func TestParseOracleTCPSRedirect(t *testing.T) {
	target, err := parseRedirectTarget("(ADDRESS=(PROTOCOL=TCPS)(HOST=dbscan.internal)(PORT=2484))")
	if err != nil || target.Host != "dbscan.internal" || target.Port != 2484 || target.Protocol != "TCPS" {
		t.Fatalf("target=%+v err=%v", target, err)
	}
}

func TestTNSDataSessionRoundTrip(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	serverErr := make(chan error, 1)
	go func() {
		typ, body, err := readTNSPacket(server)
		if err != nil {
			serverErr <- err
			return
		}
		if typ != tnsData || len(body) < 2 || binary.BigEndian.Uint16(body[:2]) != 0x0040 || string(body[2:]) != "client-negotiation" {
			serverErr <- fmt.Errorf("bad client DATA typ=%d body=%x", typ, body)
			return
		}
		response := append([]byte{0x00, 0x00}, []byte("server-negotiation")...)
		serverErr <- sendTNSPacket(server, tnsData, response)
	}()
	s := &tnsDataSession{conn: client}
	if err := s.WriteData(ctx, 0x0040, []byte("client-negotiation")); err != nil {
		t.Fatal(err)
	}
	flags, payload, err := s.ReadData(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if flags != 0 || string(payload) != "server-negotiation" {
		t.Fatalf("flags=%x payload=%q", flags, payload)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestTNSDataSessionRejectsMarker(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	go func() { _ = sendTNSPacket(server, tnsMarker, []byte{1, 2, 3}) }()
	s := &tnsDataSession{conn: client}
	_, _, err := s.ReadData(context.Background())
	if err == nil || !strings.Contains(err.Error(), "MARKER") {
		t.Fatalf("err=%v", err)
	}
}

func TestOpenAcceptedSessionKeepsTransportForTTCLayer(t *testing.T) {
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
		if typ, _, err := readTNSPacket(c); err != nil || typ != tnsConnect {
			serverErr <- fmt.Errorf("connect typ=%d err=%v", typ, err)
			return
		}
		writeTNSPacket(t, c, tnsAccept, []byte{0x01, 0x39})
		typ, body, err := readTNSPacket(c)
		if err != nil {
			serverErr <- err
			return
		}
		if typ != tnsData || len(body) < 2 || string(body[2:]) != "ttc-next-step" {
			serverErr <- fmt.Errorf("post-accept data typ=%d body=%x", typ, body)
			return
		}
		serverErr <- nil
	}()
	addr := ln.Addr().(*net.TCPAddr)
	raw, err := NewFactory().New(domain.DataSource{Type: domain.DataSourceOracle, Host: "127.0.0.1", Port: addr.Port, Database: "ORCL"})
	if err != nil {
		t.Fatal(err)
	}
	c := raw.(*Connector)
	accepted, err := c.openAcceptedSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer accepted.Session.Close()
	if accepted.Protocol != "TCP" || accepted.Host != "127.0.0.1" || accepted.Port != addr.Port {
		t.Fatalf("accepted=%+v", accepted)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := accepted.Session.WriteData(ctx, 0, []byte("ttc-next-step")); err != nil {
		t.Fatal(err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestNativeOracleDeepProbeNegotiatesTTCProtocol(t *testing.T) {
	t.Setenv("QMIGRATION_EXPERIMENTAL_ORACLE_TTC_NEGOTIATION", "1")
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
		if typ, _, err := readTNSPacket(c); err != nil || typ != tnsConnect {
			serverErr <- fmt.Errorf("connect typ=%d err=%v", typ, err)
			return
		}
		writeTNSPacket(t, c, tnsAccept, []byte{0x01, 0x39})
		typ, body, err := readTNSPacket(c)
		if err != nil {
			serverErr <- err
			return
		}
		if typ != tnsData || len(body) < 2 || len(body[2:]) < 4 || body[2] != 1 || body[3] != 6 {
			serverErr <- fmt.Errorf("bad TTC protocol request typ=%d body=%x", typ, body)
			return
		}
		resp := append([]byte{0, 0}, fakeTTCProtocolResponse()...)
		if err := sendTNSPacket(c, tnsData, resp); err != nil {
			serverErr <- err
			return
		}
		typ, body, err = readTNSPacket(c)
		if err != nil {
			serverErr <- err
			return
		}
		if typ != tnsData || len(body) < 3 || body[2] != 2 {
			serverErr <- fmt.Errorf("bad TTC datatype request typ=%d body=%x", typ, body)
			return
		}
		if err := sendTNSPacket(c, tnsData, []byte{0, 0, 2, 0}); err != nil {
			serverErr <- err
			return
		}
		serverErr <- nil
	}()
	addr := ln.Addr().(*net.TCPAddr)
	raw, err := NewFactory().New(domain.DataSource{Type: domain.DataSourceOracle, Host: "127.0.0.1", Port: addr.Port, Database: "ORCL"})
	if err != nil {
		t.Fatal(err)
	}
	if err := raw.TestConnection(context.Background()); err != nil {
		t.Fatal(err)
	}
	v, err := raw.GetVersion(context.Background())
	if err != nil || v != "oracle-ttc-v6-charset-873-ttc12" {
		t.Fatalf("version=%q err=%v", v, err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestNativeOracleExperimentalTTCAuthTranscript(t *testing.T) {
	t.Setenv("QMIGRATION_EXPERIMENTAL_ORACLE_TTC_AUTH", "1")
	const password = "Secret123!"
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
		if typ, _, err := readTNSPacket(c); err != nil || typ != tnsConnect {
			serverErr <- fmt.Errorf("connect typ=%d err=%v", typ, err)
			return
		}
		writeTNSPacket(t, c, tnsAccept, []byte{0x01, 0x39})
		// Protocol negotiation.
		if typ, body, err := readTNSPacket(c); err != nil || typ != tnsData || len(body) < 3 || body[2] != 1 {
			serverErr <- fmt.Errorf("protocol typ=%d body=%x err=%v", typ, body, err)
			return
		}
		if err := sendTNSPacket(c, tnsData, append([]byte{0, 0}, fakeTTCProtocolResponse()...)); err != nil {
			serverErr <- err
			return
		}
		// Datatype negotiation.
		if typ, body, err := readTNSPacket(c); err != nil || typ != tnsData || len(body) < 3 || body[2] != 2 {
			serverErr <- fmt.Errorf("datatype typ=%d body=%x err=%v", typ, body, err)
			return
		}
		if err := sendTNSPacket(c, tnsData, []byte{0, 0, 2, 0}); err != nil {
			serverErr <- err
			return
		}
		// Auth init.
		if typ, body, err := readTNSPacket(c); err != nil || typ != tnsData || len(body) < 4 || body[2] != 3 || body[3] != 118 {
			serverErr <- fmt.Errorf("auth init typ=%d body=%x err=%v", typ, body, err)
			return
		}
		saltHex := "00112233445566778899AABBCCDDEEFF"
		salt, _ := hex.DecodeString(saltHex)
		h := sha1.Sum(append([]byte(password), salt...))
		key := append(append([]byte(nil), h[:]...), 0, 0, 0, 0)
		serverKey := bytes.Repeat([]byte{0x5a}, 48)
		enc, err := aesCBCEncryptHex(key, serverKey, false)
		if err != nil {
			serverErr <- err
			return
		}
		w := &ttcEncoder{}
		w.byte(8)
		w.compactUint(2, 4)
		w.keyVal([]byte("AUTH_SESSKEY"), []byte(enc), 1)
		w.keyVal([]byte("AUTH_VFR_DATA"), []byte(saltHex), 6949)
		if err := sendTNSPacket(c, tnsData, append([]byte{0, 0}, w.Bytes()...)); err != nil {
			serverErr <- err
			return
		}
		// Encrypted auth proof; plaintext password must never appear.
		if typ, body, err := readTNSPacket(c); err != nil || typ != tnsData || len(body) < 4 || body[2] != 3 || body[3] != 0x73 {
			serverErr <- fmt.Errorf("auth response typ=%d body=%x err=%v", typ, body, err)
			return
		} else if bytes.Contains(body, []byte(password)) {
			serverErr <- fmt.Errorf("plaintext password leaked")
			return
		}
		result := &ttcEncoder{}
		result.byte(8)
		result.compactUint(1, 4)
		result.keyVal([]byte("AUTH_SESSION_ID"), []byte("42"), 0)
		result.byte(4)
		result.compactUint(0, 4)
		result.compactUint(0, 2)
		if err := sendTNSPacket(c, tnsData, append([]byte{0, 0}, result.Bytes()...)); err != nil {
			serverErr <- err
			return
		}
		serverErr <- nil
	}()
	addr := ln.Addr().(*net.TCPAddr)
	raw, err := NewFactory().New(domain.DataSource{Type: domain.DataSourceOracle, Host: "127.0.0.1", Port: addr.Port, Database: "ORCL", Username: "scott", Password: password})
	if err != nil {
		t.Fatal(err)
	}
	if err := raw.TestConnection(context.Background()); err != nil {
		t.Fatal(err)
	}
	v, err := raw.GetVersion(context.Background())
	if err != nil || v != "oracle-ttc-v6-charset-873-ttc12-auth" {
		t.Fatalf("version=%q err=%v", v, err)
	}
	if c := raw.(*Connector); c.sessionProperties["AUTH_SESSION_ID"] != "42" {
		t.Fatalf("props=%v", c.sessionProperties)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}
