//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"testing"
	"time"

	"dts/backend/internal/cdc/pgoutput"
	"dts/backend/internal/connector"
	my "dts/backend/internal/connector/mysql"
	pg "dts/backend/internal/connector/postgres"
	"dts/backend/internal/domain"
	"dts/backend/internal/migration"
	"dts/backend/internal/repository"
	metadata "dts/backend/internal/repository/postgres"
	"dts/backend/internal/repository/secure"
	"dts/backend/internal/security"
)

// The single reader, logical slot, durable metadata database and migration
// task live for the entire run. Source commits rotate through 16 row keys.
// One outstanding commit at a time makes each source/target comparison exact.
func TestContinuousPostgresCDC(t *testing.T) {
	requireIsolated(t)
	rawDuration := os.Getenv("DTS_E2E_CONTINUOUS_SECONDS")
	if rawDuration == "" {
		t.Skip("set DTS_E2E_CONTINUOUS_SECONDS and DTS_E2E_CONTINUOUS_EVIDENCE to run one continuous CDC task")
	}
	seconds, err := strconv.Atoi(rawDuration)
	if err != nil || seconds < 5 || seconds > 259200 {
		t.Fatal("duration must be 5–259200 seconds")
	}
	evidence, err := os.OpenFile(os.Getenv("DTS_E2E_CONTINUOUS_EVIDENCE"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	encoder := json.NewEncoder(evidence)
	emit := func(v map[string]any) {
		t.Helper()
		v["utc"] = time.Now().UTC()
		if err := encoder.Encode(v); err != nil {
			t.Fatal(err)
		}
		if err := evidence.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	var started time.Time
	var observed time.Duration
	var transactions int
	var checkpoint string
	var maxRoundtrip time.Duration
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	taskID := "continuous_" + suffix
	t.Cleanup(func() {
		defer evidence.Close()
		elapsed := time.Duration(0)
		if observed > 0 {
			elapsed = observed
		} else if !started.IsZero() {
			elapsed = time.Since(started)
		}
		passed := !t.Failed() && elapsed >= time.Duration(seconds)*time.Second && transactions >= seconds/4
		emit(map[string]any{"kind": "result", "task_id": taskID, "suite_passed": passed, "requested_seconds": seconds, "elapsed_seconds": elapsed.Seconds(), "transactions": transactions, "checkpoint": checkpoint, "max_roundtrip_ms": maxRoundtrip.Milliseconds(), "continuous_24h_passed": passed && seconds >= 86400, "continuous_72h_passed": passed && seconds >= 259200, "production_qualified": false})
		if !passed && !t.Failed() {
			t.Error("insufficient continuous runtime or completed transaction count")
		}
	})
	cleanup := func(name string, fn func(context.Context) error) {
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if err := fn(ctx); err != nil {
				t.Errorf("cleanup %s: %v", name, err)
			}
		})
	}
	check := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	ctx := t.Context()
	srcDS, dstDS := pgDS(), mysqlDS()
	sraw, err := pg.NewFactory().New(srcDS)
	check(err)
	source := sraw.(*pg.Connector)
	t.Cleanup(func() { source.Close() })
	traw, err := my.NewFactory().New(dstDS)
	check(err)
	target := traw.(*my.Connector)
	t.Cleanup(func() { target.Close() })
	table := "continuous_" + suffix
	name := "dts_e2e." + table
	check(source.ExecSQL(ctx, "CREATE SCHEMA IF NOT EXISTS dts_e2e"))
	for _, c := range []interface {
		ExecSQL(context.Context, string) error
	}{source, target} {
		check(c.ExecSQL(ctx, "CREATE TABLE "+name+" (id bigint PRIMARY KEY, payload varchar(64) NOT NULL)"))
		cleanup(name, func(ctx context.Context) error { return c.ExecSQL(ctx, "DROP TABLE "+name) })
	}
	db := "dts_continuous_" + suffix
	check(source.ExecSQL(ctx, "CREATE DATABASE "+db))
	cleanup(db, func(ctx context.Context) error { return source.ExecSQL(ctx, "DROP DATABASE "+db) })
	ds := srcDS
	ds.Database = db
	store, err := metadata.New(ctx, ds, true)
	check(err)
	t.Cleanup(func() { store.Close() })
	cipher, err := security.New("disposable-qualification-" + suffix)
	check(err)
	repo := secure.New(store, cipher)
	srcDS.ID = "source"
	dstDS.ID = "target"
	now := time.Now()
	srcDS.CreatedAt, srcDS.UpdatedAt = now, now
	dstDS.CreatedAt, dstDS.UpdatedAt = now, now
	check(repo.CreateDataSource(ctx, &srcDS))
	check(repo.CreateDataSource(ctx, &dstDS))
	check(repo.CreateMigration(ctx, &domain.MigrationTask{ID: taskID, Name: "Continuous CDC qualification", SourceID: srcDS.ID, TargetID: dstDS.ID, Mode: domain.ModeIncremental, Status: domain.StatusCDCCatchingUp, CreatedAt: time.Now(), UpdatedAt: time.Now()}))
	meta, err := source.GetTableMetadata(ctx, "dts_e2e", table)
	check(err)
	check(repo.CreateMigrationTable(ctx, &domain.MigrationTable{ID: table, TaskID: taskID, SourceSchema: "dts_e2e", SourceTable: table, TargetSchema: "dts_e2e", TargetTable: table, PrimaryKey: "id", TargetPrimaryKey: "id", Columns: meta.Columns, TargetColumns: meta.Columns}))
	reg := connector.NewRegistry()
	reg.Register(domain.DataSourcePostgreSQL, pg.NewFactory())
	reg.Register(domain.DataSourceMySQL, my.NewFactory())
	svc := migration.NewService(repo, reg)
	pub, slot := "pub_"+table, "slot_"+table
	check(source.EnsurePublication(ctx, pub, []string{name}))
	cleanup(pub, func(ctx context.Context) error { return source.ExecSQL(ctx, "DROP PUBLICATION "+pub) })
	start, err := source.CreateCDCCheckpoint(ctx, slot)
	check(err)
	cleanup(slot, func(ctx context.Context) error { return source.DropCDCCheckpoint(ctx, slot) })
	stream, err := source.OpenLogicalReplicationStream(ctx, slot, start.PositionValue, pub)
	check(err)
	reader := pgoutput.NewReader(stream)
	t.Cleanup(func() { reader.Close() })
	read := connector.ReadBatchRequest{Schema: "dts_e2e", Table: table, Columns: meta.Columns, PrimaryKey: "id", UseKeyset: true, Limit: 17}
	previousLSN, err := pgoutput.ParseLSN(start.PositionValue)
	check(err)
	started = time.Now()
	deadline := started.Add(time.Duration(seconds) * time.Second)
	lastSample := started
	emit(map[string]any{"kind": "start", "task_id": taskID, "slot": slot, "publication": pub, "source": "postgres", "target": "mysql", "row_keys": 16, "requested_seconds": seconds, "checkpoint": start.PositionValue})
	next := time.NewTicker(time.Second)
	defer next.Stop()
	lastProgress := started
	for time.Now().Before(deadline) {
		begin := time.Now()
		func() {
			cycle, cancel := context.WithTimeout(ctx, 20*time.Second)
			defer cancel()
			key := transactions%16 + 1
			check(source.ExecSQL(cycle, fmt.Sprintf("INSERT INTO %s (id,payload) VALUES (%d,'revision-%d') ON CONFLICT (id) DO UPDATE SET payload=EXCLUDED.payload", name, key, transactions+1)))
			tx, err := reader.Next(cycle)
			check(err)
			if tx == nil || len(tx.Events) != 1 || tx.Events[0].SourceTable != table {
				t.Fatal("unexpected source transaction")
			}
			result, err := svc.ApplyCDCEvents(cycle, taskID, domain.CDCApplyRequest{Events: tx.Events})
			check(err)
			if result.Applied != 1 || result.Duplicate || result.Staged {
				t.Fatalf("unexpected apply result: %+v", result)
			}
			pos, err := store.LatestCDCPosition(cycle, taskID, "forward")
			check(err)
			if pos == nil || pos.PositionType != "LSN" || pos.PositionValue != tx.Checkpoint.PositionValue {
				t.Fatal("durable task checkpoint differs from source commit")
			}
			lsn, err := pgoutput.ParseLSN(pos.PositionValue)
			check(err)
			if lsn <= previousLSN {
				t.Fatal("CDC checkpoint stopped advancing")
			}
			previousLSN = lsn
			checkpoint = pos.PositionValue
			check(reader.Acknowledge(cycle, tx))
			want, err := source.ReadBatch(cycle, read)
			check(err)
			got, err := target.ReadBatch(cycle, read)
			check(err)
			if len(want.Rows) != min(16, transactions+1) || !reflect.DeepEqual(want.Rows, got.Rows) {
				t.Fatalf("unexpected rows at transaction %d: source=%v target=%v", transactions+1, want.Rows, got.Rows)
			}
			transactions++
		}()
		finished := time.Now()
		if finished.Sub(lastProgress) > 30*time.Second {
			t.Fatal("continuous CDC progress gap exceeded 30 seconds")
		}
		lastProgress = finished
		maxRoundtrip = max(maxRoundtrip, finished.Sub(begin))
		if transactions == 1 || finished.Sub(lastSample) >= time.Minute {
			// Prune only this test's dedicated metadata database.
			_, err := store.PruneMetadata(ctx, repository.MetadataRetentionPolicy{CDCPositionMaxRowsPerStream: 128, TaskLogMaxRowsPerTask: 128, AuditMaxRows: 128, BatchRows: 256, MaxBatches: 2})
			check(err)
			var memory runtime.MemStats
			runtime.ReadMemStats(&memory)
			emit(map[string]any{"kind": "sample", "task_id": taskID, "elapsed_seconds": time.Since(started).Seconds(), "transactions": transactions, "checkpoint": checkpoint, "roundtrip_ms": finished.Sub(begin).Milliseconds(), "max_roundtrip_ms": maxRoundtrip.Milliseconds(), "heap_alloc_bytes": memory.HeapAlloc, "goroutines": runtime.NumGoroutine()})
			lastSample = finished
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-next.C:
		}
	}
	if time.Since(lastProgress) > 30*time.Second {
		t.Fatal("missing progress at end of requested interval")
	}
	observed = time.Since(started)
}
