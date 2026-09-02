package mysqlbinlog

import (
	"encoding/binary"
	"os/exec"
	"testing"
)

func payloadLenEnc(v uint64) []byte {
	if v < 0xfb {
		return []byte{byte(v)}
	}
	if v <= 0xffff {
		return []byte{0xfc, byte(v), byte(v >> 8)}
	}
	panic("test value too large")
}

func payloadField(t, v uint64) []byte {
	enc := payloadLenEnc(v)
	out := append(payloadLenEnc(t), payloadLenEnc(uint64(len(enc)))...)
	return append(out, enc...)
}

func rawNestedEvent(t byte, logPos uint32, body []byte) []byte {
	out := make([]byte, HeaderSize+len(body))
	out[4] = t
	binary.LittleEndian.PutUint32(out[9:13], uint32(len(out)))
	binary.LittleEndian.PutUint32(out[13:17], logPos)
	copy(out[HeaderSize:], body)
	return out
}

func TestParseTransactionPayloadNoneAndSplit(t *testing.T) {
	nested := append(rawNestedEvent(QueryEvent, 10, make([]byte, 13)), rawNestedEvent(XIDEvent, 20, make([]byte, 8))...)
	h := []byte{}
	h = append(h, payloadField(2, TransactionCompressionNone)...)
	h = append(h, payloadField(1, uint64(len(nested)))...)
	h = append(h, 0)
	h = append(h, nested...)
	tp, err := ParseTransactionPayload(&Event{Header: Header{Type: TransactionPayloadEvent}, Payload: h})
	if err != nil {
		t.Fatal(err)
	}
	plain, err := tp.Decompress("")
	if err != nil {
		t.Fatal(err)
	}
	parts, err := SplitTransactionEvents(plain)
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 2 || parts[0][4] != QueryEvent || parts[1][4] != XIDEvent {
		t.Fatalf("unexpected nested events: %d", len(parts))
	}
}

func TestTransactionPayloadZSTD(t *testing.T) {
	zstd, err := exec.LookPath("zstd")
	if err != nil {
		t.Skip("zstd not installed")
	}
	nested := rawNestedEvent(XIDEvent, 20, make([]byte, 8))
	cmd := exec.Command(zstd, "-q", "-c")
	stdin, _ := cmd.StdinPipe()
	go func() { _, _ = stdin.Write(nested); _ = stdin.Close() }()
	compressed, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	h := []byte{}
	h = append(h, payloadField(2, TransactionCompressionZSTD)...)
	h = append(h, payloadField(3, uint64(len(nested)))...)
	h = append(h, payloadField(1, uint64(len(compressed)))...)
	h = append(h, 0)
	h = append(h, compressed...)
	tp, err := ParseTransactionPayload(&Event{Header: Header{Type: TransactionPayloadEvent}, Payload: h})
	if err != nil {
		t.Fatal(err)
	}
	plain, err := tp.Decompress(zstd)
	if err != nil {
		t.Fatal(err)
	}
	if string(plain) != string(nested) {
		t.Fatal("decompressed payload mismatch")
	}
}
