//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"dts/backend/internal/cdc/mysqlbinlog"
	"dts/backend/internal/cdc/pgoutput"
	cdcruntime "dts/backend/internal/cdc/runtime"
	"dts/backend/internal/connector"
	my "dts/backend/internal/connector/mysql"
	pg "dts/backend/internal/connector/postgres"
	"dts/backend/internal/domain"
	"dts/backend/internal/migration"
	"dts/backend/internal/repository/memory"
)

func requireIsolated(t *testing.T) {
	t.Helper()
	if os.Getenv("DTS_E2E_ISOLATED") != "1" {
		t.Skip("set DTS_E2E_ISOLATED=1 only for the disposable qualification databases")
	}
}

type nativeTarget interface {
	connector.DataConnector
	connector.TransactionalCDCApplyConnector
	connector.TransactionFencer
	ExecSQL(context.Context, string) error
}

func TestNativeTargetFencing(t *testing.T) {
	requireIsolated(t)
	for _, typ := range []string{"mysql", "postgres"} {
		t.Run(typ, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			var a, b nativeTarget
			schema := "dts_e2e"
			if typ == "mysql" {
				x, _ := my.NewFactory().New(mysqlDS())
				y, _ := my.NewFactory().New(mysqlDS())
				a = x.(nativeTarget)
				b = y.(nativeTarget)
			} else {
				x, _ := pg.NewFactory().New(pgDS())
				y, _ := pg.NewFactory().New(pgDS())
				a = x.(nativeTarget)
				b = y.(nativeTarget)
			}
			defer a.Close()
			defer b.Close()
			if typ == "postgres" {
				if err := a.ExecSQL(ctx, "CREATE SCHEMA IF NOT EXISTS dts_e2e"); err != nil {
					t.Fatal(err)
				}
			}
			if err := a.EnsureExecutionFence(ctx); err != nil {
				t.Fatal(err)
			}
			scope := fmt.Sprintf("qualification/%s/%d", typ, time.Now().UnixNano())
			if err := a.BeginCDCTransaction(ctx); err != nil {
				t.Fatal(err)
			}
			if err := a.FenceTransaction(ctx, scope, 1); err != nil {
				t.Fatal(err)
			}
			// A separate target connection must block on the row held by generation 1.
			if err := b.BeginCDCTransaction(ctx); err != nil {
				t.Fatal(err)
			}
			result := make(chan error, 1)
			go func() { result <- b.FenceTransaction(ctx, scope, 2) }()
			select {
			case err := <-result:
				t.Fatalf("new fence overtook open target transaction: %v", err)
			case <-time.After(100 * time.Millisecond):
			}
			if err := a.RollbackCDCTransaction(ctx); err != nil {
				t.Fatal(err)
			}
			if err := <-result; err != nil {
				t.Fatal(err)
			}
			table := fmt.Sprintf("fence_orders_%d", time.Now().UnixNano())
			// Table creation stays outside the fenced business transaction on MySQL.
			if err := b.RollbackCDCTransaction(ctx); err != nil {
				t.Fatal(err)
			}
			if err := a.ExecSQL(ctx, "CREATE TABLE "+schema+"."+table+" (id bigint PRIMARY KEY,payload varchar(64))"); err != nil {
				t.Fatal(err)
			}
			defer a.ExecSQL(context.Background(), "DROP TABLE "+schema+"."+table)
			if err := b.BeginCDCTransaction(ctx); err != nil {
				t.Fatal(err)
			}
			if err := b.FenceTransaction(ctx, scope, 2); err != nil {
				t.Fatal(err)
			}
			if err := b.ExecSQL(ctx, "INSERT INTO "+schema+"."+table+" VALUES(1,'new')"); err != nil {
				t.Fatal(err)
			}
			if err := b.CommitCDCTransaction(ctx); err != nil {
				t.Fatal(err)
			}
			if err := a.BeginCDCTransaction(ctx); err != nil {
				t.Fatal(err)
			}
			if err := a.FenceTransaction(ctx, scope, 1); err == nil {
				t.Fatal("stale execution accepted after successor committed")
			}
			if err := a.RollbackCDCTransaction(ctx); err != nil {
				t.Fatal(err)
			}
			rows, err := a.ReadBatch(ctx, connector.ReadBatchRequest{Schema: schema, Table: table, Columns: []domain.ColumnInfo{{Name: "id", DataType: "bigint"}, {Name: "payload", DataType: "varchar"}}, PrimaryKey: "id", StartPK: 1, EndPK: 1, Limit: 10})
			if err != nil || len(rows.Rows) != 1 || string(rows.Rows[0][1].Raw) != "new" {
				t.Fatalf("successor data changed: %+v %v", rows, err)
			}
		})
	}
}

func TestNativeMySQLToPostgresMigration(t *testing.T) {
	testNativeFullCDC(t, false)
}

func TestNativePostgresToMySQLMigration(t *testing.T) {
	testNativeFullCDC(t, true)
}

func testNativeFullCDC(t *testing.T, fromPostgres bool) {
	requireIsolated(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	xr, err := my.NewFactory().New(mysqlDS())
	if err != nil {
		t.Fatal(err)
	}
	mysql := xr.(*my.Connector)
	defer mysql.Close()
	yr, err := pg.NewFactory().New(pgDS())
	if err != nil {
		t.Fatal(err)
	}
	postgres := yr.(*pg.Connector)
	defer postgres.Close()
	var source, target nativeTarget = mysql, postgres
	srcDS, dstDS := mysqlDS(), pgDS()
	if fromPostgres {
		source, target = postgres, mysql
		srcDS, dstDS = pgDS(), mysqlDS()
	}
	table := fmt.Sprintf("qualification_orders_%d", time.Now().UnixNano())
	if err := postgres.ExecSQL(ctx, "CREATE SCHEMA IF NOT EXISTS dts_e2e"); err != nil {
		t.Fatal(err)
	}
	for _, x := range []interface {
		ExecSQL(context.Context, string) error
	}{source, target} {
		if err := x.ExecSQL(ctx, "CREATE TABLE dts_e2e."+table+" (id bigint PRIMARY KEY,payload varchar(64))"); err != nil {
			t.Fatal(err)
		}
		defer x.ExecSQL(context.Background(), "DROP TABLE dts_e2e."+table)
	}
	if err := source.ExecSQL(ctx, "INSERT INTO dts_e2e."+table+" VALUES(1,'before'),(2,'delete')"); err != nil {
		t.Fatal(err)
	}
	var reader cdcruntime.Reader
	if fromPostgres {
		pub, slot := "pub_"+table, "slot_"+table
		if err := postgres.EnsurePublication(ctx, pub, []string{"dts_e2e." + table}); err != nil {
			t.Fatal(err)
		}
		defer postgres.ExecSQL(context.Background(), "DROP PUBLICATION "+pub)
		pos, err := postgres.CreateCDCCheckpoint(ctx, slot)
		if err != nil {
			t.Fatal(err)
		}
		defer postgres.DropCDCCheckpoint(context.Background(), slot)
		stream, err := postgres.OpenLogicalReplicationStream(ctx, slot, pos.PositionValue, pub)
		if err != nil {
			t.Fatal(err)
		}
		reader = pgoutput.NewReader(stream)
	} else {
		start, err := mysql.CurrentCDCPosition(ctx)
		if err != nil {
			t.Fatal(err)
		}
		executed, err := mysqlbinlog.ParseGTIDSet(start.PositionValue)
		if err != nil {
			t.Fatal(err)
		}
		stream, err := mysql.OpenBinlogGTIDStream(ctx, start.PositionValue, 61009)
		if err != nil {
			t.Fatal(err)
		}
		reader = mysqlbinlog.NewNativeReader(stream, mysql, mysqlbinlog.ReaderState{Executed: executed}, map[string]bool{"dts_e2e." + table: true}, "")
	}
	defer reader.Close()
	meta, err := source.GetTableMetadata(ctx, "dts_e2e", table)
	if err != nil {
		t.Fatal(err)
	}
	req := connector.ReadBatchRequest{Schema: "dts_e2e", Table: table, PrimaryKey: "id", Columns: meta.Columns, UseKeyset: true, Limit: 3}
	readExpected := func(side string, c nativeTarget, expected [][]connector.Value) *connector.RowBatch {
		t.Helper()
		got, err := c.ReadBatch(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got.Rows, expected) {
			t.Fatalf("%s: got %v, expected %v", side, got.Rows, expected)
		}
		return got
	}
	initial := [][]connector.Value{{{Raw: []byte("1")}, {Raw: []byte("before")}}, {{Raw: []byte("2")}, {Raw: []byte("delete")}}}
	batch := readExpected("source before full load", source, initial)
	if err := target.EnsureExecutionFence(ctx); err != nil {
		t.Fatal(err)
	}
	if err := target.BeginCDCTransaction(ctx); err != nil {
		t.Fatal(err)
	}
	if err := target.FenceTransaction(ctx, "qualification/full/"+table, 1); err != nil {
		t.Fatal(err)
	}
	if written, err := target.WriteBatch(ctx, connector.WriteBatchRequest{Schema: "dts_e2e", Table: table, Columns: meta.Columns, Rows: batch.Rows, PrimaryKey: "id"}); err != nil || written != 2 {
		t.Fatalf("full load wrote %d rows, expected 2: %v", written, err)
	}
	if err := target.CommitCDCTransaction(ctx); err != nil {
		t.Fatal(err)
	}
	readExpected("target after full load and before CDC", target, initial)
	store := memory.New()
	srcDS.ID = "source"
	dstDS.ID = "target"
	_ = store.CreateDataSource(ctx, &srcDS)
	_ = store.CreateDataSource(ctx, &dstDS)
	taskID := "native-" + table
	_ = store.CreateMigration(ctx, &domain.MigrationTask{ID: taskID, SourceID: srcDS.ID, TargetID: dstDS.ID, Mode: domain.ModeIncremental, Status: domain.StatusCDCCatchingUp})
	_ = store.CreateMigrationTable(ctx, &domain.MigrationTable{ID: table, TaskID: taskID, SourceSchema: "dts_e2e", SourceTable: table, TargetSchema: "dts_e2e", TargetTable: table, PrimaryKey: "id", TargetPrimaryKey: "id", Columns: meta.Columns, TargetColumns: meta.Columns})
	reg := connector.NewRegistry()
	reg.Register(domain.DataSourceMySQL, my.NewFactory())
	reg.Register(domain.DataSourcePostgreSQL, pg.NewFactory())
	svc := migration.NewService(store, reg)
	if err := source.ExecSQL(ctx, "START TRANSACTION"); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{"UPDATE dts_e2e." + table + " SET payload='after' WHERE id=1", "DELETE FROM dts_e2e." + table + " WHERE id=2", "INSERT INTO dts_e2e." + table + " VALUES(3,'insert')", "COMMIT"} {
		if err := source.ExecSQL(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	for {
		tx, err := reader.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		result, err := svc.ApplyCDCEvents(ctx, taskID, domain.CDCApplyRequest{Events: tx.Events})
		if err != nil {
			t.Fatal(err)
		}
		if err := reader.Acknowledge(ctx, tx); err != nil {
			t.Fatal(err)
		}
		if result.Applied >= 3 {
			break
		}
	}
	final := [][]connector.Value{{{Raw: []byte("1")}, {Raw: []byte("after")}}, {{Raw: []byte("3")}, {Raw: []byte("insert")}}}
	readExpected("source after CDC", source, final)
	readExpected("target after CDC", target, final)
}

func TestPostgresIdleFreezeMarker(t *testing.T) {
	requireIsolated(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	raw, err := pg.NewFactory().New(pgDS())
	if err != nil {
		t.Fatal(err)
	}
	c := raw.(*pg.Connector)
	defer c.Close()
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	slot, pub := "dts_freeze_"+suffix, "dts_freeze_pub_"+suffix
	table := "freeze_orders_" + suffix
	if err := c.ExecSQL(ctx, "CREATE SCHEMA IF NOT EXISTS dts_e2e; CREATE TABLE dts_e2e."+table+" (id bigint PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	defer c.ExecSQL(context.Background(), "DROP TABLE dts_e2e."+table)
	if err := c.EnsurePublication(ctx, pub, []string{"dts_e2e." + table}); err != nil {
		t.Fatal(err)
	}
	defer c.ExecSQL(context.Background(), "DROP PUBLICATION "+pub)
	cp, err := c.CreateCDCCheckpoint(ctx, slot)
	if err != nil {
		t.Fatal(err)
	}
	defer c.DropCDCCheckpoint(context.Background(), slot)
	stream, err := c.OpenLogicalReplicationStream(ctx, slot, cp.PositionValue, pub)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	reg := connector.NewRegistry()
	reg.Register(domain.DataSourcePostgreSQL, pg.NewFactory())
	repo := memory.New()
	for _, id := range []string{"source", "target"} {
		ds := pgDS()
		ds.ID = id
		if err := repo.CreateDataSource(ctx, &ds); err != nil {
			t.Fatal(err)
		}
	}
	task := &domain.MigrationTask{ID: "freeze", SourceID: "source", TargetID: "target", Mode: domain.ModeIncremental, Status: domain.StatusReadyCutover}
	if err := repo.CreateMigration(ctx, task); err != nil {
		t.Fatal(err)
	}
	if err := repo.CreateMigrationTable(ctx, &domain.MigrationTable{ID: "selected", TaskID: task.ID, SourceSchema: "dts_e2e", SourceTable: table, TargetSchema: "dts_e2e", TargetTable: table}); err != nil {
		t.Fatal(err)
	}
	svc := migration.NewService(repo, reg)
	op, err := svc.ConfirmWriteFreeze(ctx, task.ID, "forward", "admin", domain.WriteFreezeRequest{WritesFrozen: true, HoldTableLocks: true, Evidence: "isolated database maintenance window", IdempotencyKey: "isolated-cutover"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if m, e := repo.GetMigration(ctx, task.ID); e == nil && m.Status != domain.StatusFinished {
			m.Status = domain.StatusFailed
			_ = repo.UpdateMigration(ctx, m)
		}
	}()
	marker := op.Barrier
	reader := pgoutput.NewReader(stream)
	tx, err := reader.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	end, err := pgoutput.ParseLSN(tx.Checkpoint.PositionValue)
	if err != nil {
		t.Fatal(err)
	}
	barrier, err := pgoutput.ParseLSN(marker.PositionValue)
	if err != nil {
		t.Fatal(err)
	}
	if end < barrier || len(tx.Events) != 1 || tx.Events[0].Operation != domain.CDCCheckpoint {
		t.Fatalf("idle stream did not cover marker: %+v marker=%+v", tx, marker)
	}
	if err := c.ExecSQL(ctx, "CREATE TABLE dts_e2e.unrelated_"+suffix+"(id int); DROP TABLE dts_e2e.unrelated_"+suffix); err != nil {
		t.Fatal(err)
	}
	// Finish through a second control-plane instance sharing only metadata.
	peer := migration.NewService(repo, reg)
	if err := peer.RecordCDCProgress(ctx, task.ID, &domain.CDCPosition{PositionType: "LSN", PositionValue: tx.Checkpoint.PositionValue}); err != nil {
		t.Fatal(err)
	}
	if err := peer.Cutover(ctx, task.ID); err != nil {
		t.Fatal(err)
	}
	m, err := repo.GetMigration(ctx, task.ID)
	if err != nil || m.Status != domain.StatusFinished || m.CutoverOperation.TableLockProof == "" || m.CutoverOperation.VerifiedSource.PositionValue == op.SourceHead.PositionValue {
		t.Fatalf("cutover with unrelated WAL failed: status=%v error=%v", m, err)
	}
}
