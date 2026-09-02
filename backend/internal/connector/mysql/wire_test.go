package mysqlconnector

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"qmigration/backend/internal/cdc/mysqlbinlog"
	"qmigration/backend/internal/domain"
	"strings"
	"testing"
	"time"
)

func writeTestPacket(c net.Conn, seq byte, payload []byte) error {
	h := []byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16), seq}
	if _, err := c.Write(h); err != nil {
		return err
	}
	_, err := c.Write(payload)
	return err
}
func readTestPacket(c net.Conn) (byte, []byte, error) {
	h := make([]byte, 4)
	if _, err := io.ReadFull(c, h); err != nil {
		return 0, nil, err
	}
	n := int(h[0]) | int(h[1])<<8 | int(h[2])<<16
	b := make([]byte, n)
	_, err := io.ReadFull(c, b)
	return h[3], b, err
}
func testHandshakePayload() []byte {
	caps := clientLongPassword | clientLongFlag | clientConnectWithDB | clientProtocol41 | clientTransactions | clientSecureConnection | clientMultiResults | clientPluginAuth
	var b bytes.Buffer
	b.WriteByte(0x0a)
	b.WriteString("8.0.99-qmigration-test")
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
func lenStr(s string) []byte { return append([]byte{byte(len(s))}, []byte(s)...) }
func columnDef(name string) []byte {
	var b bytes.Buffer
	for _, s := range []string{"def", "", "", "", name, name} {
		b.Write(lenStr(s))
	}
	b.WriteByte(0x0c)
	_ = binary.Write(&b, binary.LittleEndian, uint16(45))
	_ = binary.Write(&b, binary.LittleEndian, uint32(64))
	b.WriteByte(0xfd)
	_ = binary.Write(&b, binary.LittleEndian, uint16(0))
	b.WriteByte(0)
	b.Write([]byte{0, 0})
	return b.Bytes()
}
func serveOneColumn(c net.Conn, value string) error {
	if err := writeTestPacket(c, 1, []byte{1}); err != nil {
		return err
	}
	if err := writeTestPacket(c, 2, columnDef("v")); err != nil {
		return err
	}
	if err := writeTestPacket(c, 3, []byte{0xfe, 0, 0, 2, 0}); err != nil {
		return err
	}
	if err := writeTestPacket(c, 4, lenStr(value)); err != nil {
		return err
	}
	return writeTestPacket(c, 5, []byte{0xfe, 0, 0, 2, 0})
}
func runFakeMySQL(t *testing.T) (host string, port int, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(5 * time.Second))
		if writeTestPacket(c, 0, testHandshakePayload()) != nil {
			return
		}
		if _, _, err = readTestPacket(c); err != nil {
			return
		}
		if writeTestPacket(c, 2, []byte{0, 0, 0, 2, 0, 0, 0}) != nil {
			return
		}
		for {
			_, pkt, err := readTestPacket(c)
			if err != nil {
				return
			}
			if len(pkt) == 0 || pkt[0] != 0x03 {
				return
			}
			q := string(pkt[1:])
			switch {
			case strings.HasPrefix(q, "SET NAMES"):
				err = writeTestPacket(c, 1, []byte{0, 0, 0, 2, 0, 0, 0})
			case q == "SELECT 1":
				err = serveOneColumn(c, "1")
			case q == "SELECT VERSION()":
				err = serveOneColumn(c, "8.0.99-qmigration-test")
			default:
				err = fmt.Errorf("unexpected query %s", q)
			}
			if err != nil {
				return
			}
		}
	}()
	addr := ln.Addr().(*net.TCPAddr)
	return "127.0.0.1", addr.Port, func() { _ = ln.Close(); <-done }
}

func TestConnectorWireAuthenticationAndQuery(t *testing.T) {
	host, port, stop := runFakeMySQL(t)
	defer stop()
	cRaw, err := NewFactory().New(domain.DataSource{Type: domain.DataSourceMySQL, Host: host, Port: port, Username: "root", Password: "secret", Database: "app"})
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
	if v != "8.0.99-qmigration-test" {
		t.Fatalf("unexpected version %q", v)
	}
}

func runFakeBinlogMySQL(t *testing.T, rawEvent []byte) (host string, port int, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(5 * time.Second))
		if writeTestPacket(c, 0, testHandshakePayload()) != nil {
			return
		}
		if _, _, err = readTestPacket(c); err != nil {
			return
		}
		if writeTestPacket(c, 2, []byte{0, 0, 0, 2, 0, 0, 0}) != nil {
			return
		}
		// dialProtocol always normalizes charset first.
		_, pkt, err := readTestPacket(c)
		if err != nil || len(pkt) == 0 || pkt[0] != 0x03 || !strings.HasPrefix(string(pkt[1:]), "SET NAMES") {
			return
		}
		if writeTestPacket(c, 1, []byte{0, 0, 0, 2, 0, 0, 0}) != nil {
			return
		}
		// OpenBinlogStream negotiates checksum policy.
		_, pkt, err = readTestPacket(c)
		if err != nil || len(pkt) == 0 || pkt[0] != 0x03 {
			return
		}
		if writeTestPacket(c, 1, []byte{0, 0, 0, 2, 0, 0, 0}) != nil {
			return
		}
		// Then COM_BINLOG_DUMP.
		_, pkt, err = readTestPacket(c)
		if err != nil || len(pkt) < 11 || pkt[0] != comBinlogDump {
			return
		}
		if binary.LittleEndian.Uint32(pkt[1:5]) != 4 || string(pkt[11:]) != "mysql-bin.000001" {
			return
		}
		_ = writeTestPacket(c, 1, append([]byte{0x00}, rawEvent...))
	}()
	addr := ln.Addr().(*net.TCPAddr)
	return "127.0.0.1", addr.Port, func() { _ = ln.Close(); <-done }
}

func TestOpenBinlogStream(t *testing.T) {
	raw := make([]byte, 19+8+16)
	binary.LittleEndian.PutUint32(raw[9:13], uint32(len(raw)))
	raw[4] = 4 // ROTATE_EVENT
	binary.LittleEndian.PutUint64(raw[19:27], 4)
	copy(raw[27:], []byte("mysql-bin.000002"))
	host, port, stop := runFakeBinlogMySQL(t, raw)
	defer stop()
	cRaw, err := NewFactory().New(domain.DataSource{Type: domain.DataSourceMySQL, Host: host, Port: port, Username: "repl", Password: "secret", Database: "app"})
	if err != nil {
		t.Fatal(err)
	}
	c := cRaw.(*Connector)
	stream, err := c.OpenBinlogStream(context.Background(), "mysql-bin.000001", 4, 12345)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	got, err := stream.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, raw) {
		t.Fatalf("raw event mismatch got=%x want=%x", got, raw)
	}
}

func runFakeGTIDBinlogMySQL(t *testing.T, rawEvent []byte, expectedSIDBlock []byte) (host string, port int, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(5 * time.Second))
		if writeTestPacket(c, 0, testHandshakePayload()) != nil {
			return
		}
		if _, _, err = readTestPacket(c); err != nil {
			return
		}
		if writeTestPacket(c, 2, []byte{0, 0, 0, 2, 0, 0, 0}) != nil {
			return
		}
		_, pkt, err := readTestPacket(c)
		if err != nil || len(pkt) == 0 || pkt[0] != 0x03 || !strings.HasPrefix(string(pkt[1:]), "SET NAMES") {
			return
		}
		if writeTestPacket(c, 1, []byte{0, 0, 0, 2, 0, 0, 0}) != nil {
			return
		}
		_, pkt, err = readTestPacket(c)
		if err != nil || len(pkt) == 0 || pkt[0] != 0x03 {
			return
		}
		if writeTestPacket(c, 1, []byte{0, 0, 0, 2, 0, 0, 0}) != nil {
			return
		}
		_, pkt, err = readTestPacket(c)
		if err != nil || len(pkt) < 23 || pkt[0] != comBinlogDumpGTID {
			return
		}
		if binary.LittleEndian.Uint16(pkt[1:3]) != binlogThroughGTID {
			return
		}
		if binary.LittleEndian.Uint32(pkt[3:7]) != 54321 {
			return
		}
		if binary.LittleEndian.Uint32(pkt[7:11]) != 0 {
			return
		}
		if binary.LittleEndian.Uint64(pkt[11:19]) != 4 {
			return
		}
		ln := int(binary.LittleEndian.Uint32(pkt[19:23]))
		if 23+ln != len(pkt) || !bytes.Equal(pkt[23:], expectedSIDBlock) {
			return
		}
		_ = writeTestPacket(c, 1, append([]byte{0x00}, rawEvent...))
	}()
	addr := ln.Addr().(*net.TCPAddr)
	return "127.0.0.1", addr.Port, func() { _ = ln.Close(); <-done }
}

func TestOpenBinlogGTIDStream(t *testing.T) {
	set, err := mysqlbinlog.ParseGTIDSet("24bc7850-2c16-11e6-a073-0242ac110002:1-8")
	if err != nil {
		t.Fatal(err)
	}
	sidBlock, err := set.EncodeSIDBlock()
	if err != nil {
		t.Fatal(err)
	}
	raw := make([]byte, 19+8+16)
	binary.LittleEndian.PutUint32(raw[9:13], uint32(len(raw)))
	raw[4] = 4
	binary.LittleEndian.PutUint64(raw[19:27], 4)
	copy(raw[27:], []byte("mysql-bin.000002"))
	host, port, stop := runFakeGTIDBinlogMySQL(t, raw, sidBlock)
	defer stop()
	cRaw, err := NewFactory().New(domain.DataSource{Type: domain.DataSourceMySQL, Host: host, Port: port, Username: "repl", Password: "secret", Database: "app"})
	if err != nil {
		t.Fatal(err)
	}
	c := cRaw.(*Connector)
	stream, err := c.OpenBinlogGTIDStream(context.Background(), set.String(), 54321)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	got, err := stream.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, raw) {
		t.Fatalf("raw event mismatch")
	}
}
