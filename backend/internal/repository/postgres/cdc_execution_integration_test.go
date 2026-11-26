//go:build integration

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	pg "dts/backend/internal/connector/postgres"
	"dts/backend/internal/domain"
	"dts/backend/internal/repository"
)

// testdata contains the historical embedded bootstrap through the RC40 marker
// from commit 89ea243. Changing only schema_version on a new DB hides regressions.
func testRC40MetadataUpgrade(t *testing.T, ctx context.Context, ds domain.DataSource) {
	t.Helper()
	store, err := NewWithPoolSize(ctx, ds, false, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	schema := fmt.Sprintf("dts_rc40_upgrade_%d", time.Now().UnixNano())
	if err := store.db.ExecSQL(ctx, "CREATE SCHEMA "+schema+"; SET search_path TO "+schema); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := store.db.ExecSQL(cleanup, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Errorf("remove fixture schema: %v", err)
		}
	}()
	historical, err := os.ReadFile("testdata/rc40-schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.db.ExecSQL(ctx, string(historical)); err != nil {
		t.Fatalf("bootstrap historical RC40 schema: %v", err)
	}
	assertValue := func(query, expected string) {
		t.Helper()
		result, err := store.db.QuerySQL(ctx, query)
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Rows) != 1 || sv(result.Rows[0], 0) != expected {
			t.Fatalf("%s: got %v, want %q", query, result.Rows, expected)
		}
	}
	const marker = "SELECT schema_version FROM metadata_schema_state WHERE id=1"
	assertValue(marker, "0.15.0-rc40")
	const fixture = `INSERT INTO datasources (id,name,type,host,port,username,password_ciphertext,created_at,updated_at)
		VALUES ('src','Source','postgresql','localhost',5432,'legacy','fixture-only',now(),now());
		INSERT INTO migration_tasks (id,name,source_datasource_id,target_datasource_id,mode,status,full_engine,
			chunk_rows,batch_rows,parallelism,max_retries,rows_migrated,created_at,updated_at)
		VALUES ('task','Historical migration','src','src','FULL','PAUSED','dts',1000,100,2,3,1703,now(),now());`
	if err := store.db.ExecSQL(ctx, fixture); err != nil {
		t.Fatal(err)
	}
	for pass := 1; pass <= 2; pass++ {
		if err := applyMetadataSchema(ctx, store.db); err != nil {
			t.Fatalf("RC40 bootstrap pass %d: %v", pass, err)
		}
		assertValue(marker, "0.15.0-rc50")
		assertValue("SELECT status || ':' || rows_migrated FROM migration_tasks WHERE id='task'", "PAUSED:1703")
		assertValue("SELECT password_ciphertext FROM datasources WHERE id='src'", "fixture-only")
		assertValue("SELECT to_regclass('dts_user_sessions')::text", "dts_user_sessions")
		assertValue("SELECT to_regclass('dts_personal_tokens')::text", "dts_personal_tokens")
	}
	// Exercise bind errors and then reuse the very same physical session.
	if _, err := store.db.QueryParams(ctx, "SELECT $1::integer", "not an integer"); err == nil {
		t.Fatal("invalid bound integer was accepted")
	}
	const owner = `O'Brien\控制器`
	rows, err := store.db.QueryParams(ctx, "SELECT $1::text, $2::text", owner, "")
	if err != nil || len(rows.Rows) != 1 || sv(rows.Rows[0], 0) != owner || rows.Nulls[0][1] || sv(rows.Rows[0], 1) != "" {
		t.Fatalf("bound text or empty string lost: %+v %v", rows, err)
	}
	acquired, err := store.AcquireControlOperation(ctx, "task", "PRECHECK", owner, time.Minute)
	if err != nil || !acquired {
		t.Fatalf("acquire bound lease: %t %v", acquired, err)
	}
	if err := store.RenewControlOperation(ctx, "task", "PRECHECK", owner, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := store.ReleaseControlOperation(ctx, "task", "PRECHECK", owner); err != nil {
		t.Fatal(err)
	}
}

// Use an explicitly configured test database and a unique disposable schema.
// Ordinary go test never connects to a database. CI supplies a dedicated service.
func TestMetadataCDCExecutionPostgres(t *testing.T) {
	host := os.Getenv("DTS_TEST_POSTGRES_HOST")
	if host == "" {
		t.Skip("DTS_TEST_POSTGRES_HOST is required for real PostgreSQL metadata tests")
	}
	port, err := strconv.Atoi(os.Getenv("DTS_TEST_POSTGRES_PORT"))
	if err != nil || port <= 0 {
		t.Fatal("DTS_TEST_POSTGRES_PORT must be explicitly configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	ds := domain.DataSource{Type: domain.DataSourcePostgreSQL, Host: host, Port: port, Username: os.Getenv("DTS_TEST_POSTGRES_USER"), Password: os.Getenv("DTS_TEST_POSTGRES_PASSWORD"), Database: os.Getenv("DTS_TEST_POSTGRES_DATABASE"), TLSMode: domain.TLSModeDisable}
	t.Run("historical RC40 metadata", func(t *testing.T) {
		testRC40MetadataUpgrade(t, ctx, ds)
	})
	t.Run("pooled transaction stays on one backend", func(t *testing.T) {
		s, err := NewWithPoolSize(ctx, ds, false, 2)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		// A second query must complete on another backend while the first
		// transaction still holds its session. A cancelled waiter must not run.
		err = s.db.InTransaction(ctx, func(tx *pg.SQLTransaction) error {
			a, err := tx.QuerySQL(ctx, "SELECT pg_backend_pid()")
			if err != nil {
				return err
			}
			return s.db.InTransaction(ctx, func(other *pg.SQLTransaction) error {
				b, err := other.QuerySQL(ctx, "SELECT pg_backend_pid()")
				if err != nil {
					return err
				}
				if sv(a.Rows[0], 0) == sv(b.Rows[0], 0) {
					return errors.New("pooled transactions share a session")
				}
				wait, cancelWait := context.WithTimeout(ctx, 30*time.Millisecond)
				defer cancelWait()
				_, err = s.db.QuerySQL(wait, "SELECT 1")
				if !errors.Is(err, context.DeadlineExceeded) {
					return fmt.Errorf("wait for occupied pool: %v", err)
				}
				return nil
			})
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := s.db.QuerySQL(ctx, "SELECT 1"); !errors.Is(err, sql.ErrConnDone) {
			t.Fatalf("query after close: %v", err)
		}
	})
	// The disposable schema search_path below belongs to a single session.
	a, err := NewWithPoolSize(ctx, ds, false, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := NewWithPoolSize(ctx, ds, false, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	schema := fmt.Sprintf("dts_metadata_test_%d", time.Now().UnixNano())
	if err := a.db.ExecSQL(ctx, "CREATE SCHEMA "+schema+"; SET search_path TO "+schema); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if err := a.db.ExecSQL(cleanupCtx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Errorf("cleanup test schema: %v", err)
		}
	}()
	if err := b.db.ExecSQL(ctx, "SET search_path TO "+schema); err != nil {
		t.Fatal(err)
	}
	base, _, found := strings.Cut(schemaSQL, "-- RC50:")
	if !found {
		t.Fatal("missing upgrade boundary")
	}
	if err := a.db.ExecSQL(ctx, base); err != nil {
		t.Fatal(err)
	}
	t.Run("concurrent initial administrator", func(t *testing.T) {
		start, result := make(chan struct{}), make(chan error, 2)
		for i, store := range []*Store{a, b} {
			go func() {
				<-start
				name := fmt.Sprintf("bootstrap-%d", i)
				now := time.Now()
				result <- store.CreateInitialUser(ctx, &domain.User{ID: name, Username: name, PasswordHash: "local-test", Role: "admin", Enabled: true, CreatedAt: now, UpdatedAt: now})
			}()
		}
		close(start)
		first, second := <-result, <-result
		if !(first == nil && errors.Is(second, repository.ErrConflict) || second == nil && errors.Is(first, repository.ErrConflict)) {
			t.Fatalf("bootstrap outcomes: %v, %v", first, second)
		}
		if err := a.db.ExecSQL(ctx, "DELETE FROM users WHERE id IN ('bootstrap-0','bootstrap-1')"); err != nil {
			t.Fatal(err)
		}
	})
	if err := a.db.ExecSQL(ctx, "UPDATE metadata_schema_state SET schema_version='0.15.0-rc51' WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	if err := applyMetadataSchema(ctx, a.db); err == nil {
		t.Fatal("older binary overwrote newer schema")
	}
	if result, err := a.db.QuerySQL(ctx, "SELECT schema_version FROM metadata_schema_state WHERE id=1"); err != nil || sv(result.Rows[0], 0) != "0.15.0-rc51" {
		t.Fatalf("future marker was not preserved: %v", err)
	}
	if err := a.db.ExecSQL(ctx, "UPDATE metadata_schema_state SET schema_version='0.15.0-rc49' WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	if err := a.db.ExecSQL(ctx, "INSERT INTO users(id,username,password_hash,role,enabled,created_at,updated_at) VALUES('legacy','legacy','preserved-hash','viewer',true,now(),now())"); err != nil {
		t.Fatal(err)
	}
	t.Run("packaged RC50 migrations", func(t *testing.T) {
		// Roll back this path so the embedded bootstrap below still upgrades
		// the original pre-RC50 schema independently.
		restoreFixture := errors.New("restore pre-RC50 fixture")
		err := a.db.InTransaction(ctx, func(tx *pg.SQLTransaction) error {
			for pass := 0; pass < 2; pass++ {
				for _, filename := range []string{"083_v015_rc50_atomic_control.sql", "084_v015_rc50_user_sessions.sql", "085_v015_rc50_personal_tokens.sql"} {
					body, err := os.ReadFile("../../../migrations/" + filename)
					if err != nil {
						return err
					}
					if err := tx.ExecSQL(ctx, string(body)); err != nil {
						return fmt.Errorf("%s pass %d: %w", filename, pass+1, err)
					}
				}
			}
			row, err := tx.QuerySQL(ctx, "SELECT schema_version, to_regclass('dts_user_sessions')::text,to_regclass('dts_personal_tokens')::text FROM metadata_schema_state WHERE id=1")
			if err != nil {
				return err
			}
			if len(row.Rows) != 1 || string(row.Rows[0][0]) != "0.15.0-rc50" || string(row.Rows[0][1]) != "dts_user_sessions" || string(row.Rows[0][2]) != "dts_personal_tokens" {
				return fmt.Errorf("packaged upgrade did not create rc50 sessions: %v", row)
			}
			return restoreFixture
		})
		if !errors.Is(err, restoreFixture) {
			t.Fatalf("packaged migration check: %v", err)
		}
	})
	for i := 0; i < 2; i++ {
		if err := applyMetadataSchema(ctx, a.db); err != nil {
			t.Fatalf("idempotent bootstrap pass %d: %v", i, err)
		}
	}
	if err := a.CreateMigration(ctx, &domain.MigrationTask{ID: "task", Name: "metadata integration", SourceID: "src", TargetID: "dst", Mode: domain.ModeIncremental, Status: domain.StatusCDCCatchingUp, CreatedAt: time.Now(), UpdatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	t.Run("numeric range coverage", func(t *testing.T) {
		table := domain.MigrationTable{ID: "numeric-coverage", TaskID: "task", MinPK: math.MinInt64, MaxPK: math.MaxInt64}
		if err := a.CreateMigrationTable(ctx, &table); err != nil {
			t.Fatal(err)
		}
		chunks := []domain.MigrationChunk{
			{ID: "upper-half", ChunkNo: 1, TaskID: "task", TableID: table.ID, SplitType: "PK_RANGE_ADAPTIVE", Start: 0, End: math.MaxInt64, Status: domain.ChunkSuccess},
			{ID: "lower-half", ChunkNo: 2, TaskID: "task", TableID: table.ID, SplitType: "PK_RANGE", Start: math.MinInt64, End: -1, Status: domain.ChunkSuccess},
		}
		if err := a.CreateChunks(ctx, chunks); err != nil {
			t.Fatal(err)
		}
		if ok, err := a.NumericRangeCoverage(ctx, table); err != nil || !ok {
			t.Fatalf("complete BIGINT coverage: %v %v", ok, err)
		}
		chunks[0].Start = 1
		if err := a.UpdateChunk(ctx, &chunks[0]); err != nil {
			t.Fatal(err)
		}
		if ok, err := a.NumericRangeCoverage(ctx, table); err != nil || ok {
			t.Fatalf("missing zero: %v %v", ok, err)
		}
	})
	legacy, err := b.GetUser(ctx, "legacy")
	if err != nil || legacy.PasswordHash != "preserved-hash" || legacy.Revision != 0 {
		t.Fatalf("upgrade lost legacy IAM state: %+v %v", legacy, err)
	}
	t.Run("session revocation across connections", func(t *testing.T) {
		row := domain.UserSession{ID: "session-1", UserID: "legacy", CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)}
		if err := a.RecordSession(ctx, row); err != nil {
			t.Fatal(err)
		}
		if exists, active, err := b.SessionState(ctx, row.UserID, row.ID); err != nil || !exists || !active {
			t.Fatalf("session on replica: %t %t %v", exists, active, err)
		}
		if found, err := b.RevokeSession(ctx, "somebody-else", row.ID); err != nil || found {
			t.Fatalf("revoked another user's session: %t %v", found, err)
		}
		if found, err := b.RevokeSession(ctx, row.UserID, row.ID); err != nil || !found {
			t.Fatalf("revoke: %t %v", found, err)
		}
		if err := a.RecordSession(ctx, row); err != nil {
			t.Fatal(err)
		}
		if exists, active, err := a.SessionState(ctx, row.UserID, row.ID); err != nil || !exists || active {
			t.Fatalf("recording old session undid revocation: %t %t %v", exists, active, err)
		}
	})
	if p, err := b.LatestPendingCDCSpool(ctx, "task", "forward"); err != nil || p != nil {
		t.Fatalf("empty spool: %+v %v", p, err)
	}
	if ok, err := a.AcquireCDCSpoolDrainLease(ctx, "task", "forward", "old", time.Minute); err != nil || !ok {
		t.Fatalf("first acquire: %v %v", ok, err)
	}
	if ok, err := b.AcquireCDCSpoolDrainLease(ctx, "task", "forward", "new", time.Minute); err != nil || ok {
		t.Fatalf("second connection bypassed exclusion: %v %v", ok, err)
	}
	if err := a.db.ExecSQL(ctx, "UPDATE cdc_spool_drain_leases SET lease_until=now()-interval '1 second' WHERE task_id='task' AND direction='forward'"); err != nil {
		t.Fatal(err)
	}
	if err := a.RenewCDCSpoolDrainLease(ctx, "task", "forward", "old", time.Minute); !errors.Is(err, repository.ErrLeaseOwner) {
		t.Fatalf("expired execution resurrected: %v", err)
	}
	if ok, err := b.AcquireCDCSpoolDrainLease(ctx, "task", "forward", "new", time.Minute); err != nil || !ok {
		t.Fatalf("reclaim: %v %v", ok, err)
	}
	epoch, err := b.CDCExecutionEpoch(ctx, "task", "forward", "new")
	if err != nil {
		t.Fatal(err)
	}
	if epoch < 2 {
		t.Fatal("CDC epoch did not advance on takeover")
	}
	staleCtx := repository.WithCDCFence(ctx, repository.CDCFence{TaskID: "task", Direction: "forward", Owner: "old", Epoch: epoch - 1})
	if err := a.CreateCDCPosition(staleCtx, &domain.CDCPosition{ID: "stale", TaskID: "task", Direction: "forward", RecordedAt: time.Now()}); !errors.Is(err, repository.ErrLeaseOwner) {
		t.Fatalf("stale owner checkpoint accepted: %v", err)
	}
	if err := a.CreateCDCSpool(staleCtx, &domain.CDCSpoolRecord{ID: "stale", TaskID: "task", Direction: "forward", Status: domain.CDCSpoolPending, CreatedAt: time.Now()}); !errors.Is(err, repository.ErrLeaseOwner) {
		t.Fatalf("stale owner spool accepted: %v", err)
	}
	if err := a.ReleaseCDCSpoolDrainLease(ctx, "task", "forward", "old"); err != nil {
		t.Fatal(err)
	}
	if err := b.RenewCDCSpoolDrainLease(ctx, "task", "forward", "new", time.Minute); err != nil {
		t.Fatalf("old release removed new owner: %v", err)
	}
	for i, direction := range []string{"", "reverse"} {
		if err := a.CreateCDCPosition(ctx, &domain.CDCPosition{ID: fmt.Sprint(i), TaskID: "task", Direction: direction, PositionType: "BINLOG", PositionValue: "position-" + direction, Resource: "stream", RecordedAt: time.Now().Add(time.Duration(i) * time.Second)}); err != nil {
			t.Fatal(err)
		}
	}
	if p, err := b.LatestCDCPosition(ctx, "task", "forward"); err != nil || p == nil || p.PositionValue != "position-" || p.Resource != "stream" {
		t.Fatalf("legacy forward head obscured by reverse: %+v %v", p, err)
	}
	if p, err := b.LatestCDCPosition(ctx, "task", "reverse"); err != nil || p == nil || p.PositionValue != "position-reverse" {
		t.Fatalf("reverse stream head: %+v %v", p, err)
	}
	if p, err := b.LatestCDCPosition(ctx, "missing", "forward"); err != nil || p != nil {
		t.Fatalf("missing checkpoint: %+v %v", p, err)
	}
	for _, id := range []string{"admin-a", "admin-b"} {
		if err := a.CreateUser(ctx, &domain.User{ID: id, Username: id, Role: "admin", Enabled: true, CreatedAt: time.Now(), UpdatedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	ua, _ := a.GetUser(ctx, "admin-a")
	ub, _ := b.GetUser(ctx, "admin-b")
	ua.Enabled = false
	ub.Enabled = false
	start := make(chan struct{})
	out := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); <-start; out <- a.UpdateUser(ctx, ua) }()
	go func() { defer wg.Done(); <-start; out <- b.UpdateUser(ctx, ub) }()
	close(start)
	wg.Wait()
	close(out)
	okCount, denied := 0, 0
	for err := range out {
		if err == nil {
			okCount++
		} else if errors.Is(err, repository.ErrLastAdmin) {
			denied++
		} else {
			t.Fatal(err)
		}
	}
	if okCount != 1 || denied != 1 {
		t.Fatalf("cross-connection last-admin invariant failed: success=%d denied=%d", okCount, denied)
	}
	ma, _ := a.GetMigration(ctx, "task")
	mb, _ := b.GetMigration(ctx, "task")
	ma.CDCLagMS = 7
	if err := a.UpdateMigration(ctx, ma); err != nil {
		t.Fatal(err)
	}
	mb.Status = domain.StatusFinished
	if err := b.UpdateMigration(ctx, mb); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("cross-connection stale metadata update accepted: %v", err)
	}
	current, _ := a.GetMigration(ctx, "task")
	current.Status = domain.StatusFullMigrating
	current.FullEngine = "dts"
	current.Parallelism = 1
	current.CutoverOperation = &domain.LifecycleOperation{ID: "persisted", Phase: "STOP_READER", Steps: []domain.ControlStep{{Name: "STOP_READER", Status: "RUNNING"}}}
	if err := a.UpdateMigration(ctx, current); err != nil {
		t.Fatal(err)
	}
	loaded, err := b.GetMigration(ctx, "task")
	if err != nil || loaded.CutoverOperation == nil || loaded.CutoverOperation.Steps[0].Status != "RUNNING" {
		t.Fatalf("Saga evidence not durable across connections: %+v %v", loaded, err)
	}
	if err := a.CreateChunks(ctx, []domain.MigrationChunk{{ID: "chunk", TaskID: "task", TableID: "table", ChunkNo: 1, SplitType: "RANGE", PrimaryKey: "id", Status: domain.ChunkPending}}); err != nil {
		t.Fatal(err)
	}
	if err := a.CreateMigrationTable(ctx, &domain.MigrationTable{ID: "table", TaskID: "task", SourceSchema: "app", SourceTable: "orders", TargetSchema: "app", TargetTable: "orders", PrimaryKey: "id", Engine: "dts"}); err != nil {
		t.Fatal(err)
	}
	if err := a.UpsertWorker(ctx, &domain.Worker{ID: "same-worker", Hostname: "qualification", Status: "ONLINE", Capabilities: []string{"dts"}, LastHeartbeat: time.Now()}); err != nil {
		t.Fatal(err)
	}
	old, err := a.ClaimChunk(ctx, "same-worker", time.Minute, []string{"dts"})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.db.ExecSQL(ctx, "UPDATE migration_chunks SET lease_until=now()-interval '1 second' WHERE id='chunk'"); err != nil {
		t.Fatal(err)
	}
	next, err := b.ClaimChunk(ctx, "same-worker", time.Minute, []string{"dts"})
	if err != nil {
		t.Fatal(err)
	}
	old.Status = domain.ChunkSuccess
	if err := a.UpdateOwnedChunk(repository.WithLeaseGeneration(ctx, old.LeaseGeneration), "same-worker", old); !errors.Is(err, repository.ErrLeaseOwner) {
		t.Fatalf("old same-ID worker completion accepted: %v", err)
	}
	if err := a.UpdateChunkProgress(repository.WithLeaseGeneration(ctx, old.LeaseGeneration), old.ID, "same-worker", domain.ChunkProgress{RowsWritten: 999}); !errors.Is(err, repository.ErrLeaseOwner) {
		t.Fatalf("old same-ID worker progress accepted: %v", err)
	}
	next.Status = domain.ChunkSuccess
	if err := b.UpdateOwnedChunk(repository.WithLeaseGeneration(ctx, next.LeaseGeneration), "same-worker", next); err != nil {
		t.Fatal(err)
	}
	t.Run("pending split across connections", func(t *testing.T) {
		checkPostgresPendingSplit(t, ctx, a, b)
	})
	t.Run("failed chunk batch is atomic", func(t *testing.T) {
		first := domain.MigrationChunk{ID: "atomic-batch", TaskID: "task", TableID: "table", ChunkNo: 40, SplitType: "PK_RANGE", PrimaryKey: "id", Status: domain.ChunkPending}
		if err := a.CreateChunks(ctx, []domain.MigrationChunk{first, first}); err == nil {
			t.Fatal("duplicate ID was accepted")
		}
		if err := b.CreateChunks(ctx, []domain.MigrationChunk{first}); err != nil {
			t.Fatalf("first insert survived the failed batch: %v", err)
		}
	})
}

func checkPostgresPendingSplit(t *testing.T, ctx context.Context, a, b *Store) {
	t.Helper()
	c := domain.MigrationChunk{ID: "split-head", TaskID: "task", TableID: "table", ChunkNo: 2, Start: 1, End: 12, Status: domain.ChunkPending, SplitType: "PK_RANGE", PrimaryKey: "id", CustomWhere: "active = 1"}
	if err := a.CreateChunks(ctx, []domain.MigrationChunk{c}); err != nil {
		t.Fatal(err)
	}
	expected, err := b.GetChunk(ctx, c.ID)
	if err != nil {
		t.Fatal(err)
	}
	head, middle, last := *expected, *expected, *expected
	head.End, head.SplitType = 4, "PK_RANGE_ADAPTIVE"
	middle.ID, middle.ChunkNo, middle.Start, middle.End, middle.SplitType = "split-middle", 3, 5, 8, head.SplitType
	last.ID, last.ChunkNo, last.Start, last.SplitType = "split-last", 1, 9, head.SplitType
	pieces := []domain.MigrationChunk{head, middle, last}
	// The final insert collides with a completed chunk. Both the head update
	// and the earlier successful middle insert must roll back.
	if err := a.SplitPendingChunk(ctx, *expected, pieces); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("collision: %v", err)
	}
	unchanged, err := b.GetChunk(ctx, c.ID)
	if err != nil || unchanged.End != c.End || unchanged.LeaseGeneration != expected.LeaseGeneration {
		t.Fatalf("partial head: %+v %v", unchanged, err)
	}
	remaining, err := b.ListPendingTableChunks(ctx, c.TaskID, c.TableID)
	if err != nil || len(remaining) != 1 || remaining[0].ID != head.ID {
		t.Fatalf("partial children: %+v %v", remaining, err)
	}

	pieces[2].ChunkNo = 4
	start, results := make(chan struct{}), make(chan error, 2)
	for _, store := range []*Store{a, b} {
		go func() { <-start; results <- store.SplitPendingChunk(ctx, *expected, pieces) }()
	}
	close(start)
	won, stale := 0, 0
	for i := 0; i < 2; i++ {
		switch err := <-results; {
		case err == nil:
			won++
		case errors.Is(err, repository.ErrConflict):
			stale++
		default:
			t.Fatal(err)
		}
	}
	remaining, err = b.ListPendingTableChunks(ctx, c.TaskID, c.TableID)
	if err != nil || won != 1 || stale != 1 || len(remaining) != 3 {
		t.Fatalf("won=%d stale=%d pending=%+v err=%v", won, stale, remaining, err)
	}
	for i, piece := range remaining {
		if piece.Start != pieces[i].Start || piece.End != pieces[i].End || piece.CustomWhere != c.CustomWhere {
			t.Fatalf("bad partition: %+v", remaining)
		}
	}
	if err := a.UpdateChunk(ctx, expected); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("stale writer could restore parent: %v", err)
	}
	// A plan captured before a real worker claimed the range must not replace
	// its lease or insert children under its running range.
	beforeClaim := remaining[0]
	claimed, err := b.ClaimChunk(ctx, "same-worker", time.Minute, []string{"dts"})
	if err != nil || claimed.ID != beforeClaim.ID {
		t.Fatalf("claim: %+v %v", claimed, err)
	}
	left, right := beforeClaim, beforeClaim
	left.End, right.Start, right.ID, right.ChunkNo = 2, 3, "unexpected-child", 5
	if err := a.SplitPendingChunk(ctx, beforeClaim, []domain.MigrationChunk{left, right}); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("claim overwritten: %v", err)
	}
	running, err := b.GetChunk(ctx, claimed.ID)
	if err != nil || running.Status != domain.ChunkRunning || running.End != beforeClaim.End || running.WorkerID != "same-worker" || running.LeaseGeneration != claimed.LeaseGeneration {
		t.Fatalf("claimed range changed: %+v %v", running, err)
	}
}
