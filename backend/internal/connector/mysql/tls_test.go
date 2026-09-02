package mysqlconnector

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
	"math/big"
	"net"
	"qmigration/backend/internal/domain"
	"strings"
	"testing"
	"time"
)

func testTLSHandshakePayload() []byte {
	caps := clientLongPassword | clientLongFlag | clientConnectWithDB | clientProtocol41 | clientSSL | clientTransactions | clientSecureConnection | clientMultiResults | clientPluginAuth
	var b bytes.Buffer
	b.WriteByte(0x0a)
	b.WriteString("8.0.99-qmigration-tls-test")
	b.WriteByte(0)
	_ = binary.Write(&b, binary.LittleEndian, uint32(123))
	b.WriteString("12345678")
	b.WriteByte(0)
	_ = binary.Write(&b, binary.LittleEndian, uint16(caps))
	b.WriteByte(45)
	_ = binary.Write(&b, binary.LittleEndian, uint16(2))
	_ = binary.Write(&b, binary.LittleEndian, uint16(caps>>16))
	b.WriteByte(21)
	b.Write(make([]byte, 10))
	b.WriteString("abcdefghijklm")
	b.WriteByte(0)
	b.WriteString("mysql_native_password")
	b.WriteByte(0)
	return b.Bytes()
}

func testServerCertificate(t *testing.T) (tls.Certificate, string) {
	t.Helper()
	now := time.Now()
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "QMigration Test CA"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
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
	serverTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
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
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	return cert, string(caPEM)
}

func runFakeTLSMySQL(t *testing.T) (host string, port int, caPEM string, stop func()) {
	t.Helper()
	cert, ca := testServerCertificate(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	serverErr := make(chan error, 1)
	go func() {
		defer close(done)
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(8 * time.Second))
		fail := func(e error) {
			select {
			case serverErr <- e:
			default:
			}
		}
		if err = writeTestPacket(c, 0, testTLSHandshakePayload()); err != nil {
			fail(err)
			return
		}
		seq, sslReq, err := readTestPacket(c)
		if err != nil {
			fail(err)
			return
		}
		if seq != 1 || len(sslReq) != 32 || binary.LittleEndian.Uint32(sslReq[:4])&clientSSL == 0 {
			fail(fmt.Errorf("invalid MySQL SSLRequest seq=%d len=%d caps=%x", seq, len(sslReq), binary.LittleEndian.Uint32(sslReq[:4])))
			return
		}
		tlsConn := tls.Server(c, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
		if err = tlsConn.Handshake(); err != nil {
			fail(err)
			return
		}
		seq, _, err = readTestPacket(tlsConn)
		if err != nil {
			fail(err)
			return
		}
		if seq != 2 {
			fail(fmt.Errorf("auth packet sequence=%d want=2", seq))
			return
		}
		if err = writeTestPacket(tlsConn, 3, []byte{0, 0, 0, 2, 0, 0, 0}); err != nil {
			fail(err)
			return
		}
		for {
			_, pkt, e := readTestPacket(tlsConn)
			if e != nil {
				return
			}
			if len(pkt) == 0 || pkt[0] != 0x03 {
				fail(fmt.Errorf("unexpected command packet %x", pkt))
				return
			}
			q := string(pkt[1:])
			switch {
			case strings.HasPrefix(q, "SET NAMES"):
				err = writeTestPacket(tlsConn, 1, []byte{0, 0, 0, 2, 0, 0, 0})
			case q == "SELECT 1":
				err = serveOneColumn(tlsConn, "1")
			case q == "SELECT VERSION()":
				err = serveOneColumn(tlsConn, "8.0.99-qmigration-tls-test")
			default:
				err = fmt.Errorf("unexpected query %s", q)
			}
			if err != nil {
				fail(err)
				return
			}
		}
	}()
	addr := ln.Addr().(*net.TCPAddr)
	return "127.0.0.1", addr.Port, ca, func() {
		_ = ln.Close()
		<-done
		select {
		case err := <-serverErr:
			t.Errorf("fake TLS MySQL server: %v", err)
		default:
		}
	}
}

func TestConnectorTLSRequiredWithCustomCA(t *testing.T) {
	host, port, ca, stop := runFakeTLSMySQL(t)
	defer stop()
	cRaw, err := NewFactory().New(domain.DataSource{
		Type: domain.DataSourceMySQL, Host: host, Port: port, Username: "root", Password: "secret", Database: "app",
		TLSMode: domain.TLSModeRequired, TLSServerName: "localhost", TLSCACert: ca,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cRaw.Close()
	c := cRaw.(*Connector)
	if err := c.TestConnection(context.Background()); err != nil {
		t.Fatal(err)
	}
	v, err := c.GetVersion(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if v != "8.0.99-qmigration-tls-test" {
		t.Fatalf("unexpected version %q", v)
	}
}

func TestConnectorTLSRequiredNeverDowngrades(t *testing.T) {
	host, port, stop := runFakeMySQL(t)
	defer stop()
	cRaw, err := NewFactory().New(domain.DataSource{Type: domain.DataSourceMySQL, Host: host, Port: port, Username: "root", Password: "secret", Database: "app", TLSMode: domain.TLSModeRequired})
	if err != nil {
		t.Fatal(err)
	}
	defer cRaw.Close()
	err = cRaw.TestConnection(context.Background())
	if err == nil || !strings.Contains(err.Error(), "TLS REQUIRED") {
		t.Fatalf("expected TLS REQUIRED failure, got %v", err)
	}
}

func TestMySQLTLSConfigRejectsInvalidCA(t *testing.T) {
	_, err := mysqlTLSConfig(domain.DataSource{Host: "db.example", TLSCACert: "not a PEM"})
	if err == nil {
		t.Fatal("expected invalid CA error")
	}
}
