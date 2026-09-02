package oracleconnector

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"testing"
	"time"
)

func fakeTTCRowHeaderPayload(columns int, rows int, bits []byte) []byte {
	w := &ttcEncoder{}
	w.byte(0)
	w.compactUint(uint64(columns), 2)
	w.compactUint(0, 4)
	w.compactUint(uint64(rows), 4)
	w.compactUint(0, 2)
	w.clr(bits)
	w.clr(nil)
	return w.Bytes()
}

func fakeTTCSummaryPayload(ttcVersion byte, ret, cursor, current uint64, msg string) []byte {
	w := &ttcEncoder{}
	w.compactUint(current, 4)
	w.compactUint(ret, 2)
	w.compactUint(0, 2)
	w.compactUint(0, 2)
	w.compactUint(cursor, 2)
	w.compactUint(0, 2)
	w.byte(0)
	w.byte(0)
	if ttcVersion >= 7 {
		w.compactUint(0, 2)
		w.compactUint(0, 2)
	} else {
		w.byte(0)
		w.byte(0)
	}
	w.byte(0)
	w.byte(0)
	w.compactUint(0, 4)
	w.compactUint(0, 2)
	w.byte(0)
	w.compactUint(0, 4)
	w.compactUint(0, 2)
	w.compactUint(0, 4)
	w.byte(0)
	w.byte(0)
	w.compactUint(0, 2)
	w.compactUint(0, 4)
	w.clr(nil)
	if ttcVersion < 7 {
		w.clr(nil)
		w.clr(nil)
		w.clr(nil)
	} else {
		w.compactUint(0, 2)
		w.compactUint(0, 4)
		w.compactUint(0, 2)
		w.compactUint(ret, 4)
		w.compactUint(current, 8)
	}
	if ret != 0 {
		w.clr([]byte(msg))
	}
	return w.Bytes()
}

func TestTTCRowHeaderBitVector(t *testing.T) {
	p := fakeTTCRowHeaderPayload(3, 1, []byte{0b00000101})
	h, err := parseTTCRowHeaderFromDecoder(newTTCDecoder(p), 3)
	if err != nil {
		t.Fatal(err)
	}
	if h.ColumnCount != 3 || h.RowCount != 1 || len(h.Present) != 3 || !h.Present[0] || h.Present[1] || !h.Present[2] {
		t.Fatalf("header=%+v", h)
	}
}

func TestTTCSummaryCursorAndError(t *testing.T) {
	p := fakeTTCSummaryPayload(12, 942, 77, 3, "table or view does not exist")
	r := newTTCDecoder(p)
	s, err := parseTTCSummaryFromDecoder(r, 12, ttcProtocolInfo{})
	if err != nil {
		t.Fatal(err)
	}
	if r.remaining() != 0 || s.CursorID != 77 || s.CurrentRow != 3 || s.RetCode != 942 || s.ErrorMessage == "" {
		t.Fatalf("summary=%+v remaining=%d", s, r.remaining())
	}
	if got := s.err(); got == nil || !bytes.Contains([]byte(got.Error()), []byte("ORA-00942")) {
		t.Fatalf("error=%v", got)
	}
}

func TestTTCFetchAndCursorCloseRequests(t *testing.T) {
	fetch, err := buildTTCFetchRequest(42, 128)
	if err != nil {
		t.Fatal(err)
	}
	if len(fetch) < 5 || fetch[0] != 3 || fetch[1] != 5 || fetch[2] != 0 {
		t.Fatalf("fetch=%x", fetch)
	}
	closeReq, err := buildTTCCursorCloseRequest(42)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(closeReq[:6], []byte{17, 105, 0, 1, 1, 1}) {
		t.Fatalf("close=%x", closeReq)
	}
}

func TestTTCROWIDDecoder(t *testing.T) {
	w := &ttcEncoder{}
	w.byte(1)
	w.compactUint(12345, 4)
	w.compactUint(7, 2)
	w.byte(1)
	w.compactUint(987654, 4)
	w.compactUint(12, 2)
	v, err := parseTTCROWIDFromDecoder(newTTCDecoder(w.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if s, ok := v.(string); !ok || len(s) != 18 {
		t.Fatalf("rowid=%#v", v)
	}
}

func TestTTCLobRequestAndChunkCodec(t *testing.T) {
	locator := []byte{1, 2, 3, 4, 5, 6}
	req, err := buildTTCLobRequest(locator, 12, 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(req) < len(locator)+10 || req[0] != 3 || req[1] != 0x60 || !bytes.Contains(req, locator) {
		t.Fatalf("lob request=%x", req)
	}
	w := &ttcEncoder{}
	w.byte(0xfe)
	w.byte(3)
	w.Write([]byte("abc"))
	w.byte(2)
	w.Write([]byte("de"))
	w.byte(0)
	got, err := parseTTCLobChunkFromDecoder(newTTCDecoder(w.Bytes()), 16)
	if err != nil || string(got) != "abcde" {
		t.Fatalf("chunk=%q err=%v", got, err)
	}
}

func TestTTCQueryPacketCoalescingAndFetchContinuation(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	proto := ttcProtocolInfo{}
	data := ttcDataTypeInfo{TTCVersion: 12}
	describe := fakeDescribePayload(12, func(w *ttcEncoder) {
		encodeFakeDescribeColumn(w, oracleTypeNUMBER, 38, 22, 0, 0, 0, false, "ID", "", 12)
	})
	serverErr := make(chan error, 1)
	go func() {
		s := &tnsDataSession{conn: server}
		flags, req, err := s.ReadData(ctx)
		if err != nil || flags != 0 || len(req) < 4 || req[0] != 3 || req[1] != 0x5e {
			serverErr <- fmt.Errorf("select req=%x flags=%d err=%v", req, flags, err)
			return
		}
		packet := &ttcEncoder{}
		packet.byte(ttcDescribe)
		packet.Write(describe)
		packet.byte(ttcRowHeader)
		packet.Write(fakeTTCRowHeaderPayload(1, 2, nil))
		packet.byte(ttcRowData)
		row1 := &ttcEncoder{}
		row1.clr([]byte{0xc1, 0x02})
		packet.Write(row1.Bytes())
		packet.byte(ttcRowData)
		row2 := &ttcEncoder{}
		row2.clr([]byte{0xc1, 0x03})
		packet.Write(row2.Bytes())
		packet.byte(ttcErrorReturn)
		packet.Write(fakeTTCSummaryPayload(12, 0, 42, 2, ""))
		if err := s.WriteData(ctx, 0, packet.Bytes()); err != nil {
			serverErr <- err
			return
		}

		flags, fetch, err := s.ReadData(ctx)
		if err != nil || flags != 0 || len(fetch) < 5 || fetch[0] != 3 || fetch[1] != 5 {
			serverErr <- fmt.Errorf("fetch=%x flags=%d err=%v", fetch, flags, err)
			return
		}
		packet.Reset()
		packet.byte(ttcRowHeader)
		packet.Write(fakeTTCRowHeaderPayload(1, 1, nil))
		packet.byte(ttcRowData)
		row3 := &ttcEncoder{}
		row3.clr([]byte{0xc1, 0x04})
		packet.Write(row3.Bytes())
		packet.byte(ttcErrorReturn)
		packet.Write(fakeTTCSummaryPayload(12, 1403, 42, 3, ""))
		serverErr <- s.WriteData(ctx, 0, packet.Bytes())
	}()

	c := &Connector{}
	result, err := c.executeTTCSelectBatched(ctx, &acceptedSession{Session: &tnsDataSession{conn: client}}, proto, data, "SELECT ID FROM T", 2, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Columns) != 1 || len(result.Rows) != 3 || fmt.Sprint(result.Rows[0][0]) != "1" || fmt.Sprint(result.Rows[1][0]) != "2" || fmt.Sprint(result.Rows[2][0]) != "3" {
		t.Fatalf("result=%+v", result)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestTTCLobResponseCoalescing(t *testing.T) {
	locator := []byte{9, 8, 7}
	packet := &ttcEncoder{}
	packet.byte(8)
	packet.Write(locator)
	packet.compactUint(5, 8)
	packet.byte(14)
	packet.byte(0xfe)
	packet.byte(2)
	packet.Write([]byte("ab"))
	packet.byte(3)
	packet.Write([]byte("cde"))
	packet.byte(0)
	packet.byte(ttcFunctionStat)
	out := oracleTTCLobResponse{}
	if err := consumeTTCLobPacket(packet.Bytes(), len(locator), 12, ttcProtocolInfo{}, &out, 16); err != nil {
		t.Fatal(err)
	}
	if !out.Done || out.Size != 5 || string(out.Data) != "abcde" {
		t.Fatalf("lob=%+v", out)
	}
}

func TestTTCQueryMessageSplitAcrossTNSDataPackets(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	proto := ttcProtocolInfo{}
	data := ttcDataTypeInfo{TTCVersion: 12}
	describe := fakeDescribePayload(12, func(w *ttcEncoder) {
		encodeFakeDescribeColumn(w, oracleTypeNCHAR, 0, 128, 873, 1, 128, true, "NAME", "", 12)
	})
	packet := &ttcEncoder{}
	packet.byte(ttcDescribe)
	packet.Write(describe)
	packet.byte(ttcRowHeader)
	packet.Write(fakeTTCRowHeaderPayload(1, 1, nil))
	packet.byte(ttcRowData)
	row := &ttcEncoder{}
	row.clr([]byte("split-packet-value"))
	packet.Write(row.Bytes())
	packet.byte(ttcErrorReturn)
	packet.Write(fakeTTCSummaryPayload(12, 1403, 55, 1, ""))
	wire := append([]byte(nil), packet.Bytes()...)
	if len(wire) < 30 {
		t.Fatalf("wire too short: %d", len(wire))
	}

	serverErr := make(chan error, 1)
	go func() {
		s := &tnsDataSession{conn: server}
		_, req, err := s.ReadData(ctx)
		if err != nil || len(req) < 2 || req[0] != ttcFunctionCall {
			serverErr <- fmt.Errorf("select request=%x err=%v", req, err)
			return
		}
		cuts := []int{7, len(wire) / 2, len(wire)}
		start := 0
		for _, end := range cuts {
			body := make([]byte, 2+end-start)
			copy(body[2:], wire[start:end])
			if err := sendTNSPacket(server, tnsData, body); err != nil {
				serverErr <- err
				return
			}
			start = end
		}
		serverErr <- nil
	}()

	c := &Connector{}
	result, err := c.executeTTCSelectBatched(ctx, &acceptedSession{Session: &tnsDataSession{conn: client}}, proto, data, "SELECT NAME FROM T", 8, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || fmt.Sprint(result.Rows[0][0]) != "split-packet-value" {
		t.Fatalf("result=%+v", result)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}
