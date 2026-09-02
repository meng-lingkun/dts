package oracleconnector

import (
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"
)

func fakeTTCProtocolResponse() []byte {
	num := make([]byte, 11)
	binary.BigEndian.PutUint16(num[9:11], 2000)
	b := []byte{1, 6, 0}
	b = append(b, []byte("Oracle Database 19c")...)
	b = append(b, 0)
	var x [2]byte
	binary.BigEndian.PutUint16(x[:], 873)
	b = append(b, x[:]...)
	b = append(b, 2)
	binary.BigEndian.PutUint16(x[:], 0)
	b = append(b, x[:]...)
	binary.BigEndian.PutUint16(x[:], uint16(len(num)))
	b = append(b, x[:]...)
	b = append(b, num...)
	b = append(b, 3, 1, 2, 3)
	b = append(b, 2, 4, 5)
	return b
}

func TestTTCProtocolCodec(t *testing.T) {
	req := buildTTCProtocolRequest("QMigration")
	if len(req) < 5 || req[0] != 1 || req[1] != 6 || req[len(req)-1] != 0 {
		t.Fatalf("bad request %x", req)
	}
	info, err := parseTTCProtocolResponse(fakeTTCProtocolResponse())
	if err != nil {
		t.Fatal(err)
	}
	if info.ServerVersion != 6 || info.ServerCharset != 873 || info.ServerNCharset != 2000 || info.ServerString != "Oracle Database 19c" {
		t.Fatalf("info=%+v", info)
	}
	if len(info.CompileTimeCaps) != 3 || len(info.RuntimeCaps) != 2 {
		t.Fatalf("caps=%v/%v", info.CompileTimeCaps, info.RuntimeCaps)
	}
}

func TestTTCProtocolNegotiationOverTNSData(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	errc := make(chan error, 1)
	go func() {
		s := &tnsDataSession{conn: server}
		flags, p, err := s.ReadData(ctx)
		if err == nil && (flags != 0 || len(p) < 4 || p[0] != 1 || p[1] != 6) {
			t.Errorf("request=%x flags=%d", p, flags)
		}
		if err == nil {
			err = s.WriteData(ctx, 0, fakeTTCProtocolResponse())
		}
		errc <- err
	}()
	c := &Connector{}
	info, err := c.negotiateTTCProtocol(ctx, &acceptedSession{Session: &tnsDataSession{conn: client}})
	if err != nil {
		t.Fatal(err)
	}
	if info.ServerCharset != 873 {
		t.Fatalf("info=%+v", info)
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
}
