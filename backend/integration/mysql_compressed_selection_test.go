//go:build integration

package integration

import (
	"context"
	"encoding/base64"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"dts/backend/internal/cdc/mysqlbinlog"
	"dts/backend/internal/connector"
	mysqlconnector "dts/backend/internal/connector/mysql"
	"dts/backend/internal/domain"
)

type countPayloadStream struct {
	connector.RawCDCStream
	payloads int
}

func (s *countPayloadStream) Next(ctx context.Context) ([]byte, error) {
	packet, err := s.RawCDCStream.Next(ctx)
	if len(packet) >= mysqlbinlog.HeaderSize && packet[4] == mysqlbinlog.TransactionPayloadEvent {
		s.payloads++
	}
	return packet, err
}

func TestMySQLCompressedMixedTablesFromBinlogPosition(t *testing.T) {
	requireIsolated(t)
	zstd, err := exec.LookPath("zstd")
	if err != nil {
		t.Fatal("compressed transaction qualification requires zstd")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	raw, err := mysqlconnector.NewFactory().New(mysqlDS())
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	source := raw.(*mysqlconnector.Connector)
	execute := func(sql string) {
		t.Helper()
		if err := source.ExecSQL(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	execute("DROP TABLE IF EXISTS dts_e2e.compressed_chosen, dts_e2e.compressed_ignored")
	execute("CREATE TABLE dts_e2e.compressed_chosen (id BIGINT PRIMARY KEY, value_text TEXT, binary_value VARBINARY(32), choice ENUM('2','1'), flags SET('a','b','c')) ENGINE=InnoDB")
	execute("CREATE TABLE dts_e2e.compressed_ignored LIKE dts_e2e.compressed_chosen")
	execute("SET SESSION binlog_transaction_compression=ON")
	execute("SET @dts_old_row_metadata=@@GLOBAL.binlog_row_metadata")
	execute("SET GLOBAL binlog_row_metadata='FULL'")
	defer func() {
		if err := source.ExecSQL(context.Background(), "SET GLOBAL binlog_row_metadata=@dts_old_row_metadata"); err != nil {
			t.Errorf("restore binlog_row_metadata: %v", err)
		}
	}()
	start, err := source.CurrentBinlogPosition(ctx)
	if err != nil {
		t.Fatal(err)
	}
	pos, err := strconv.ParseUint(strings.TrimPrefix(start.PositionValue, start.Resource+":"), 10, 32)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := source.OpenBinlogStream(ctx, start.Resource, uint32(pos), 61011)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	execute("START TRANSACTION")
	execute("INSERT INTO dts_e2e.compressed_ignored VALUES (1, REPEAT('a',4096),NULL,NULL,NULL)")
	execute("INSERT INTO dts_e2e.compressed_chosen VALUES (2, REPEAT('b',4096),X'00FF805C','2','a,c')")
	execute("COMMIT")
	streamCounter := &countPayloadStream{RawCDCStream: stream}
	reader := mysqlbinlog.NewNativeReader(streamCounter, source, mysqlbinlog.ReaderState{File: start.Resource, Position: uint32(pos)}, map[string]bool{"dts_e2e.compressed_chosen": true}, zstd)
	tx, err := reader.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if streamCounter.payloads != 1 || len(tx.Events) != 1 {
		t.Fatalf("expected exactly one compressed transaction and selected row: payloads=%d rows=%d", streamCounter.payloads, len(tx.Events))
	}
	row := tx.Events[0]
	if row.Operation != domain.CDCInsert || row.SourceTable != "compressed_chosen" || len(row.After) != 5 || row.After[0].Value != "2" {
		t.Fatalf("incorrect row decoded from compressed transaction: table=%s fields=%d", row.SourceTable, len(row.After))
	}
	// MySQL's row protocol encodes TEXT using the same representation as BLOB.
	text, err := base64.StdEncoding.DecodeString(row.After[1].Value)
	if row.After[1].Encoding != "base64" || err != nil || string(text) != strings.Repeat("b", 4096) {
		t.Fatalf("incorrect TEXT image: encoding=%s decoded bytes=%d", row.After[1].Encoding, len(text))
	}
	if row.After[2].Encoding != "base64" || row.After[2].Value != base64.StdEncoding.EncodeToString([]byte{0, 0xff, 0x80, 0x5c}) {
		t.Fatalf("incorrect VARBINARY image: %+v", row.After[2])
	}
	if row.After[3].Value != "2" || row.After[4].Value != "a,c" {
		t.Fatalf("incorrect ENUM/SET labels: %+v", row.After[3:])
	}
	if err := reader.Acknowledge(ctx, tx); err != nil || reader.State().Position <= uint32(pos) {
		t.Fatalf("checkpoint: %v state=%+v", err, reader.State())
	}
}
