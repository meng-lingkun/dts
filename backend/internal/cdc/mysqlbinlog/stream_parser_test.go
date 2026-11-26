package mysqlbinlog

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"
	"io"
	"testing"

	"dts/backend/internal/domain"
)

func checksummed(raw []byte) []byte {
	raw = append(raw, 0, 0, 0, 0)
	binary.LittleEndian.PutUint32(raw[9:13], uint32(len(raw)))
	binary.LittleEndian.PutUint32(raw[len(raw)-4:], crc32.ChecksumIEEE(raw[:len(raw)-4]))
	return raw
}

func formatEvent(algorithm byte) []byte {
	body := make([]byte, 57+40)
	binary.LittleEndian.PutUint16(body, 4)
	copy(body[2:], "8.0.36-log")
	body[56] = HeaderSize
	return checksummed(rawEvent(FormatDescriptionEvent, 4, append(body, algorithm)))
}

func TestStreamParserFollowsChecksumAcrossFiles(t *testing.T) {
	var p StreamParser
	file := append(make([]byte, 8), []byte("mysql-bin.000002")...)
	parse := func(raw, want []byte) {
		t.Helper()
		ev, err := p.Parse(raw)
		if err != nil || want != nil && !bytes.Equal(ev.Payload, want) {
			t.Fatalf("type=%d event=%+v err=%v", raw[4], ev, err)
		}
	}
	parse(rawEvent(RotateEvent, 0, file), file)
	fde := formatEvent(1)
	fde[17] |= 1 // the on-disk in-use flag is excluded from FDE checksum
	parse(fde, nil)
	parse(checksummed(rawEvent(QueryEvent, 30, []byte("payload"))), []byte("payload"))
	parse(checksummed(rawEvent(RotateEvent, 0, file)), file)
	parse(formatEvent(0), nil)
	parse(rawEvent(QueryEvent, 30, []byte("payload")), []byte("payload"))
}

func TestStreamParserRejectsUnknownFormatAndDamagedEvents(t *testing.T) {
	var p StreamParser
	if _, err := p.Parse(rawEvent(QueryEvent, 30, nil)); err == nil {
		t.Fatal("accepted data before format description")
	}
	badFormat := formatEvent(1)
	badFormat[len(badFormat)-1] ^= 1
	for _, raw := range [][]byte{formatEvent(9), badFormat} {
		if _, err := p.Parse(raw); err == nil || p.haveFormat {
			t.Fatal("accepted invalid format description")
		}
	}
	if _, err := p.Parse(formatEvent(1)); err != nil {
		t.Fatal(err)
	}
	row := checksummed(rawEvent(QueryEvent, 30, nil))
	row[len(row)-1] ^= 1
	if _, err := p.Parse(row); err == nil {
		t.Fatal("accepted damaged event")
	}
}

type packetStream [][]byte

func (s *packetStream) Next(context.Context) ([]byte, error) {
	if len(*s) == 0 {
		return nil, io.EOF
	}
	packet := (*s)[0]
	*s = (*s)[1:]
	return packet, nil
}
func (*packetStream) Close() error { return nil }

func TestNativeReaderChecksummedContainerWithUnselectedRows(t *testing.T) {
	// Only the enclosing TRANSACTION_PAYLOAD_EVENT has a CRC. An unselected
	// table gets no metadata lookup and the transaction still yields a checkpoint.
	begin := append(make([]byte, 14), []byte("BEGIN")...)
	tm := []byte{9, 0, 0, 0, 0, 0, 0, 0, 3, 'a', 'p', 'p', 0, 4, 's', 'k', 'i', 'p', 0, 1, 3, 0, 0}
	rows := []byte{9, 0, 0, 0, 0, 0, 0, 0, 2, 0, 1, 1, 0, 1, 0, 0, 0}
	nested := rawEvent(QueryEvent, 0, begin)
	nested = append(nested, rawEvent(TableMapEvent, 0, tm)...)
	nested = append(nested, rawEvent(WriteRowsEventV2, 0, rows)...)
	nested = append(nested, rawEvent(XIDEvent, 0, make([]byte, 8))...)
	h := append(payloadField(2, TransactionCompressionNone), payloadField(1, uint64(len(nested)))...)
	body := append(append(h, 0), nested...)
	stream := packetStream{formatEvent(1), checksummed(rawEvent(TransactionPayloadEvent, 300, body))}
	r := NewNativeReader(&stream, nil, ReaderState{File: "bin.001", Position: 4}, map[string]bool{"app.selected": true}, "")
	tx, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tx.Events) != 1 || tx.Events[0].Operation != domain.CDCCheckpoint || tx.Checkpoint.PositionValue != "bin.001:300" || r.State().Position != 4 {
		t.Fatalf("unexpected checkpoint or premature acknowledgement: %+v, %+v", tx, r.State())
	}
	if err := r.Acknowledge(context.Background(), tx); err != nil || r.State().Position != 300 {
		t.Fatalf("acknowledge: %v state=%+v", err, r.State())
	}
}
