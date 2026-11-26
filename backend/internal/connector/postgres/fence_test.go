package postgresconnector

import (
	"bufio"
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestTargetTransactionCannotReconnectAfterConnectionLoss(t *testing.T) {
	input := pgTestFrame('C', []byte("BEGIN\x00"))
	input = append(input, pgTestFrame('Z', []byte{'T'})...)
	conn := &queryScriptConn{input: bytes.NewReader(input)}
	c := &Connector{client: &pgClient{conn: conn, r: bufio.NewReader(conn)}}
	if err := c.BeginCDCTransaction(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = c.client.close()
	if _, err := c.get(context.Background()); err == nil || !strings.Contains(err.Error(), "transaction connection lost") {
		t.Fatalf("reconnected an open target transaction: %v", err)
	}
	if err := c.CommitCDCTransaction(context.Background()); err == nil {
		t.Fatal("COMMIT on replacement connection accepted")
	}
	if strings.Contains(conn.output.String(), "COMMIT") {
		t.Fatal("COMMIT sent after losing transaction connection")
	}
}

func TestPostgresTargetFenceRejectsStaleEpoch(t *testing.T) {
	input := pgTestFrame('C', []byte("INSERT 0 0\x00"))
	input = append(input, pgTestFrame('Z', []byte{'T'})...)
	conn := &queryScriptConn{input: bytes.NewReader(input)}
	c := &Connector{client: &pgClient{conn: conn, r: bufio.NewReader(conn)}}
	if err := c.FenceTransaction(context.Background(), "cdc/test/forward", 1); err == nil {
		t.Fatal("stale target epoch was accepted")
	}
	if !strings.Contains(conn.output.String(), "WHERE execution_fence.generation<=EXCLUDED.generation") {
		t.Fatal("fence is not conditional")
	}
}
