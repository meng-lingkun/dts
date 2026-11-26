package postgresconnector

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

type queryScriptConn struct {
	input  *bytes.Reader
	output bytes.Buffer
	closed bool
}

func (c *queryScriptConn) Read(p []byte) (int, error)     { return c.input.Read(p) }
func (c *queryScriptConn) Write(p []byte) (int, error)    { return c.output.Write(p) }
func (c *queryScriptConn) Close() error                   { c.closed = true; return nil }
func (*queryScriptConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (*queryScriptConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (*queryScriptConn) SetDeadline(time.Time) error      { return nil }
func (*queryScriptConn) SetReadDeadline(time.Time) error  { return nil }
func (*queryScriptConn) SetWriteDeadline(time.Time) error { return nil }

func pgTestFrame(kind byte, payload []byte) []byte {
	frame := make([]byte, 5+len(payload))
	frame[0] = kind
	binary.BigEndian.PutUint32(frame[1:5], uint32(len(payload)+4))
	copy(frame[5:], payload)
	return frame
}

func TestQueryDrainsSQLFailureBeforeReusingConnection(t *testing.T) {
	input := pgTestFrame('E', []byte("SERROR\x00C23505\x00Mduplicate key\x00\x00"))
	input = append(input, pgTestFrame('Z', []byte{'I'})...)
	input = append(input, pgTestFrame('D', []byte{0, 1, 0, 0, 0, 1, '7'})...)
	input = append(input, pgTestFrame('C', []byte("SELECT 1\x00"))...)
	input = append(input, pgTestFrame('Z', []byte{'I'})...)
	conn := &queryScriptConn{input: bytes.NewReader(input)}
	client := &pgClient{conn: conn, r: bufio.NewReader(conn)}
	if _, err := client.query(context.Background(), "INSERT duplicate"); err == nil || !strings.Contains(err.Error(), "duplicate key") {
		t.Fatalf("SQL failure lost: %v", err)
	}
	result, err := client.query(context.Background(), "SELECT 7")
	if err != nil || len(result.rows) != 1 || string(result.rows[0][0]) != "7" {
		t.Fatalf("previous ReadyForQuery contaminated next query: result=%+v err=%v", result, err)
	}
	if conn.closed {
		t.Fatal("ordinary SQL failure must preserve the synchronized connection")
	}
}

func TestQueryTransportFailureDiscardsConnection(t *testing.T) {
	conn := &queryScriptConn{input: bytes.NewReader([]byte{'D', 0, 0})}
	client := &pgClient{conn: conn, r: bufio.NewReader(conn)}
	if _, err := client.query(context.Background(), "SELECT 1"); err == nil {
		t.Fatal("truncated protocol response accepted")
	}
	if !conn.closed {
		t.Fatal("unsynchronized connection can be reused after transport failure")
	}
}

func TestQueryCancellationInterruptsReadWithoutCallerDeadline(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()
	client := &pgClient{conn: clientConn, r: bufio.NewReader(clientConn)}
	requestRead := make(chan struct{})
	go func() {
		var header [5]byte
		if _, err := io.ReadFull(serverConn, header[:]); err != nil {
			return
		}
		_, _ = io.CopyN(io.Discard, serverConn, int64(binary.BigEndian.Uint32(header[1:]))-4)
		close(requestRead)
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan error, 1)
	go func() { _, err := client.query(ctx, "SELECT blocked"); finished <- err }()
	select {
	case <-requestRead:
	case <-time.After(time.Second):
		t.Fatal("query did not reach server")
	}
	cancel()
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation cause lost: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled query is still blocked on network read")
	}
}
