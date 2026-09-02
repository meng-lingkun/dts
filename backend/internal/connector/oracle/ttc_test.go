package oracleconnector

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestTTCStreamRoundTripOnAcceptedTNSDataSession(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	errc := make(chan error, 1)
	go func() {
		ts, _ := newTTCStream(&tnsDataSession{conn: server})
		m, err := ts.ReadMessage(ctx)
		if err == nil && (m.Code != 3 || string(m.Payload) != "protocol") {
			t.Errorf("message=%+v", m)
		}
		if err == nil {
			err = ts.WriteMessage(ctx, ttcMessage{Code: 9, Payload: []byte("ok")})
		}
		errc <- err
	}()
	ts, err := newTTCStream(&tnsDataSession{conn: client})
	if err != nil {
		t.Fatal(err)
	}
	if err := ts.WriteMessage(ctx, ttcMessage{Code: 3, Payload: []byte("protocol")}); err != nil {
		t.Fatal(err)
	}
	got, err := ts.ReadMessage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.Code != 9 || string(got.Payload) != "ok" {
		t.Fatalf("got=%+v", got)
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
}

func TestTTCStateRequiresOrderedPhases(t *testing.T) {
	s := newTTCState()
	if err := s.Advance(ttcPhaseDataType); err == nil {
		t.Fatal("expected out-of-order transition failure")
	}
	for _, p := range []ttcPhase{ttcPhaseProtocol, ttcPhaseDataType, ttcPhaseAuthenticated, ttcPhaseReady} {
		if err := s.Advance(p); err != nil {
			t.Fatal(err)
		}
	}
	if !s.Ready() {
		t.Fatal("TTC state should be ready")
	}
}

func TestOracleDictionaryTypeNormalizationAndIdentifierQuoting(t *testing.T) {
	p18, s0 := int64(18), int64(0)
	if got := normalizeOracleType("NUMBER", &p18, &s0); got != "BIGINT" {
		t.Fatalf("got=%s", got)
	}
	if got := normalizeOracleType("CLOB", nil, nil); got != "STRING" {
		t.Fatalf("got=%s", got)
	}
	if got := normalizeOracleType("BLOB", nil, nil); got != "BINARY" {
		t.Fatalf("got=%s", got)
	}
	q, err := quoteOracleIdentifier(`A"B`)
	if err != nil || q != `"A""B"` {
		t.Fatalf("q=%q err=%v", q, err)
	}
	if oracleListSchemasSQL == "" || oracleColumnsSQL == "" || oraclePrimaryKeysSQL == "" {
		t.Fatal("dictionary plans missing")
	}
}
