//go:build integration

package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"qmigration/backend/internal/cdc/mysqlbinlog"
	"qmigration/backend/internal/cdc/pgoutput"
	"qmigration/backend/internal/connector"
	mysqlconnector "qmigration/backend/internal/connector/mysql"
	postgresconnector "qmigration/backend/internal/connector/postgres"
)

func TestMySQLGTIDReplicationTransport(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	raw, err := mysqlconnector.NewFactory().New(mysqlDS())
	if err != nil {
		t.Fatal(err)
	}
	my := raw.(*mysqlconnector.Connector)
	defer my.Close()
	for _, q := range []string{
		"CREATE DATABASE IF NOT EXISTS `qmigration_e2e` CHARACTER SET utf8mb4",
		"DROP TABLE IF EXISTS `qmigration_e2e`.`cdc_orders`",
		"CREATE TABLE `qmigration_e2e`.`cdc_orders` (`id` bigint NOT NULL,`payload` varchar(64),PRIMARY KEY (`id`))",
	} {
		if err := my.ExecSQL(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	pos, err := my.CurrentCDCPosition(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pos.PositionType != "GTID" || strings.TrimSpace(pos.PositionValue) == "" {
		t.Fatalf("expected GTID start, got %+v", pos)
	}
	source := any(my).(connector.MySQLBinlogSource)
	stream, err := source.OpenBinlogGTIDStream(ctx, pos.PositionValue, 61001)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if err := my.ExecSQL(ctx, "INSERT INTO `qmigration_e2e`.`cdc_orders` VALUES (101,'gtid-e2e')"); err != nil {
		t.Fatal(err)
	}
	parser := mysqlbinlog.Parser{}
	seenGTID, seenRows, seenCommit := false, false, false
	for !(seenGTID && seenRows && seenCommit) {
		rawEvent, err := stream.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		e, err := parser.Parse(rawEvent)
		if err != nil {
			t.Fatal(err)
		}
		switch e.Header.Type {
		case mysqlbinlog.GTIDEvent:
			g, err := mysqlbinlog.ParseGTIDEvent(e)
			if err != nil {
				t.Fatal(err)
			}
			if g.GNO > 0 {
				seenGTID = true
			}
		case mysqlbinlog.WriteRowsEventV1, mysqlbinlog.WriteRowsEventV2:
			seenRows = true
		case mysqlbinlog.XIDEvent:
			seenCommit = true
		}
	}
}

func TestPostgresLogicalReplicationTransport(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	raw, err := postgresconnector.NewFactory().New(pgDS())
	if err != nil {
		t.Fatal(err)
	}
	pg := raw.(*postgresconnector.Connector)
	defer pg.Close()
	for _, q := range []string{
		"CREATE SCHEMA IF NOT EXISTS qmigration_e2e",
		"DROP TABLE IF EXISTS qmigration_e2e.cdc_orders",
		"CREATE TABLE qmigration_e2e.cdc_orders (id bigint PRIMARY KEY,payload varchar(64))",
		"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name='qmigration_e2e_slot'",
	} {
		if err := pg.ExecSQL(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	if err := pg.EnsurePublication(ctx, "qmigration_e2e_pub", []string{"qmigration_e2e.cdc_orders"}); err != nil {
		t.Fatal(err)
	}
	cp, err := pg.CreateCDCCheckpoint(ctx, "qmigration_e2e_slot")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pg.DropCDCCheckpoint(context.Background(), cp.Resource) }()
	source := any(pg).(connector.PostgreSQLLogicalSource)
	stream, err := source.OpenLogicalReplicationStream(ctx, cp.Resource, cp.PositionValue, "qmigration_e2e_pub")
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if err := pg.ExecSQL(ctx, "INSERT INTO qmigration_e2e.cdc_orders VALUES (202,'pg-e2e')"); err != nil {
		t.Fatal(err)
	}
	decoder := pgoutput.NewDecoder()
	for {
		copyData, err := stream.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		tx, err := decoder.Push(copyData)
		if err != nil {
			t.Fatal(err)
		}
		if tx == nil {
			continue
		}
		if len(tx.Events) == 0 || tx.Events[0].Operation != "INSERT" {
			t.Fatalf("unexpected tx %+v", tx)
		}
		lsn := pgoutput.FormatLSN(tx.EndLSN)
		if err := stream.Acknowledge(ctx, lsn); err != nil {
			t.Fatal(err)
		}
		break
	}
}
