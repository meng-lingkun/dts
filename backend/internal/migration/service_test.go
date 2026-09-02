package migration

import (
	"context"
	"encoding/json"
	"errors"
	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
	"qmigration/backend/internal/engine"
	"qmigration/backend/internal/repository"
	"qmigration/backend/internal/repository/memory"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type rc44ValidationPageRepo struct {
	repository.Repository
	pageCalls int
	maxLimit  int
	listCalls int
}

func (r *rc44ValidationPageRepo) ListTableChunksPage(ctx context.Context, taskID, tableID string, afterChunkNo int, afterID string, limit int) ([]domain.MigrationChunk, error) {
	r.pageCalls++
	if limit > r.maxLimit {
		r.maxLimit = limit
	}
	return repository.ListTableChunksPage(ctx, r.Repository, taskID, tableID, afterChunkNo, afterID, limit)
}

func (r *rc44ValidationPageRepo) ListChunks(ctx context.Context, taskID string) ([]domain.MigrationChunk, error) {
	r.listCalls++
	return r.Repository.ListChunks(ctx, taskID)
}

type rc44EmptyDataConnector struct{}

func (rc44EmptyDataConnector) TestConnection(context.Context) error       { return nil }
func (rc44EmptyDataConnector) GetVersion(context.Context) (string, error) { return "", nil }
func (rc44EmptyDataConnector) ListSchemas(context.Context) ([]domain.SchemaInfo, error) {
	return nil, nil
}
func (rc44EmptyDataConnector) ListTables(context.Context, string) ([]domain.TableInfo, error) {
	return nil, nil
}
func (rc44EmptyDataConnector) GetTableMetadata(context.Context, string, string) (*domain.TableMetadata, error) {
	return nil, nil
}
func (rc44EmptyDataConnector) Close() error { return nil }
func (rc44EmptyDataConnector) ReadBatch(context.Context, connector.ReadBatchRequest) (*connector.RowBatch, error) {
	return &connector.RowBatch{}, nil
}
func (rc44EmptyDataConnector) WriteBatch(context.Context, connector.WriteBatchRequest) (int64, error) {
	return 0, nil
}

type fakeFactory struct{}
type fakeConnector struct{ typ domain.DataSourceType }

func (fakeFactory) New(ds domain.DataSource) (connector.Connector, error) {
	return fakeConnector{typ: ds.Type}, nil
}
func (fakeConnector) TestConnection(context.Context) error       { return nil }
func (fakeConnector) GetVersion(context.Context) (string, error) { return "fake", nil }
func (fakeConnector) ListSchemas(context.Context) ([]domain.SchemaInfo, error) {
	return []domain.SchemaInfo{{Name: "app"}}, nil
}
func (fakeConnector) ListTables(_ context.Context, schema string) ([]domain.TableInfo, error) {
	return []domain.TableInfo{{Schema: schema, Name: "orders", Rows: 250, DataLength: 25000}}, nil
}
func (fakeConnector) GetTableMetadata(_ context.Context, schema, table string) (*domain.TableMetadata, error) {
	if table != "orders" {
		return nil, errors.New("not found")
	}
	return &domain.TableMetadata{Schema: schema, Name: table, Columns: []domain.ColumnInfo{{Name: "id", DataType: "bigint", ColumnType: "bigint", Ordinal: 1, PrimaryKey: true}, {Name: "name", DataType: "varchar", ColumnType: "varchar(64)", Ordinal: 2}}, PrimaryKey: "id", PrimaryKeyType: "bigint", PrimaryKeyNumeric: true, MinPK: 1, MaxPK: 250, EstimatedRows: 250, DataLength: 25000, HasRows: true}, nil
}
func (fakeConnector) Close() error { return nil }
func (fakeConnector) ListSchemaObjects(context.Context, string) ([]domain.SchemaObject, error) {
	return []domain.SchemaObject{
		{Schema: "app", Name: "v_orders", Type: domain.SchemaObjectView, DDL: "CREATE VIEW v_orders AS SELECT 1"},
		{Schema: "app", Name: "trg_orders", Type: domain.SchemaObjectTrigger, RelatedTo: "orders", DDL: "CREATE TRIGGER ..."},
	}, nil
}

func TestServicePlansAndCompletesFullMigration(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	reg := connector.NewRegistry()
	f := fakeFactory{}
	reg.Register(domain.DataSourceMySQL, f)
	reg.Register(domain.DataSourcePolarDBX, f)
	now := time.Now()
	src := domain.DataSource{ID: "src", Type: domain.DataSourceMySQL, Database: "app", CreatedAt: now}
	dst := domain.DataSource{ID: "dst", Type: domain.DataSourcePolarDBX, Database: "app", CreatedAt: now}
	_ = repo.CreateDataSource(ctx, &src)
	_ = repo.CreateDataSource(ctx, &dst)
	_ = repo.UpsertWorker(ctx, &domain.Worker{ID: "w1", Hostname: "w1", Status: "ONLINE", Capabilities: []string{"qmigration"}, LastHeartbeat: now})
	svc := NewService(repo, reg)
	m := domain.MigrationTask{Name: "test", SourceID: "src", TargetID: "dst", Mode: domain.ModeFull, FullEngine: "native", ChunkRows: 100, BatchRows: 10, Parallelism: 2}
	if err := svc.Create(ctx, &m); err != nil {
		t.Fatal(err)
	}
	if err := svc.Start(ctx, m.ID); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		cur, _ := svc.Get(ctx, m.ID)
		if cur.Status == domain.StatusFullMigrating {
			break
		}
		if cur.Status == domain.StatusFailed {
			t.Fatalf("planning failed: %s", cur.LastError)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for FULL_MIGRATING, status=%s", cur.Status)
		}
		time.Sleep(5 * time.Millisecond)
	}
	chunks, err := svc.Chunks(ctx, m.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(chunks))
	}
	for range chunks {
		job, err := svc.ClaimChunk(ctx, "w1")
		if err != nil {
			t.Fatal(err)
		}
		rows := job.Chunk.End - job.Chunk.Start + 1
		if err := svc.CompleteChunk(ctx, "w1", job.Chunk.ID, domain.ChunkResult{RowsRead: rows, RowsWritten: rows, BytesRead: rows * 10, BytesWritten: rows * 10, DurationMS: 100}); err != nil {
			t.Fatal(err)
		}
	}
	cur, _ := svc.Get(ctx, m.ID)
	if cur.Status != domain.StatusFinished {
		t.Fatalf("expected FINISHED, got %s", cur.Status)
	}
	if cur.FinishedChunks != 3 || cur.Progress != 100 {
		t.Fatalf("bad progress: %+v", cur)
	}
	if cur.RowsMigrated != 250 {
		t.Fatalf("expected 250 rows, got %d", cur.RowsMigrated)
	}
}

func (c fakeConnector) CurrentCDCPosition(context.Context) (*domain.CDCPosition, error) {
	if c.typ.IsMySQLFamily() {
		return &domain.CDCPosition{DatabaseType: string(c.typ), PositionType: "BINLOG", PositionValue: "mysql-bin.000001:4", SourceTimestampMS: time.Now().UnixMilli()}, nil
	}
	return &domain.CDCPosition{DatabaseType: string(c.typ), PositionType: "LSN", PositionValue: "0/123", SourceTimestampMS: time.Now().UnixMilli()}, nil
}
func (c fakeConnector) CreateCDCCheckpoint(_ context.Context, resource string) (*domain.CDCPosition, error) {
	p, _ := c.CurrentCDCPosition(context.Background())
	p.Resource = resource
	return p, nil
}
func (fakeConnector) DropCDCCheckpoint(context.Context, string) error { return nil }

func TestServiceIncrementalOnlyCDCBridge(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	reg := connector.NewRegistry()
	f := fakeFactory{}
	reg.Register(domain.DataSourcePostgreSQL, f)
	reg.Register(domain.DataSourcePolarDBPostgreSQL, f)
	now := time.Now()
	src := domain.DataSource{ID: "src-pg", Type: domain.DataSourcePostgreSQL, Database: "appdb", Schema: "public", CreatedAt: now}
	dst := domain.DataSource{ID: "dst-pg", Type: domain.DataSourcePolarDBPostgreSQL, Database: "appdb", Schema: "public", CreatedAt: now}
	_ = repo.CreateDataSource(ctx, &src)
	_ = repo.CreateDataSource(ctx, &dst)
	svc := NewService(repo, reg)
	m := domain.MigrationTask{Name: "cdc-only", SourceID: src.ID, TargetID: dst.ID, Mode: domain.ModeIncremental, CDCEngine: "external"}
	if err := svc.Create(ctx, &m); err != nil {
		t.Fatal(err)
	}
	if err := svc.Start(ctx, m.ID); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		cur, _ := svc.Get(ctx, m.ID)
		if cur.Status == domain.StatusCDCInitializing {
			break
		}
		if cur.Status == domain.StatusFailed {
			t.Fatalf("prepare failed: %s", cur.LastError)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout, status=%s", cur.Status)
		}
		time.Sleep(5 * time.Millisecond)
	}
	positions, err := svc.CDCPositions(ctx, m.ID)
	if err != nil || len(positions) == 0 {
		t.Fatalf("missing CDC start position: %v %+v", err, positions)
	}
	if err := svc.MarkCDCStarted(ctx, m.ID, &domain.CDCPosition{PositionType: "LSN", PositionValue: "0/123", LagMS: 0}); err != nil {
		t.Fatal(err)
	}
	cur, _ := svc.Get(ctx, m.ID)
	if cur.Status != domain.StatusCDCCatchingUp {
		t.Fatalf("expected CDC_CATCHING_UP, got %s", cur.Status)
	}
}

func TestLegacyExternalEngineRequestNormalizesToUnified(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	reg := connector.NewRegistry()
	f := fakeFactory{}
	reg.Register(domain.DataSourceMySQL, f)
	reg.Register(domain.DataSourcePolarDBX, f)
	er := engine.NewRegistry()
	er.Register(engine.NewUnified())
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "src-unified", Type: domain.DataSourceMySQL, Database: "app", CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "dst-unified", Type: domain.DataSourcePolarDBX, Database: "app", CreatedAt: now})
	_ = repo.UpsertWorker(ctx, &domain.Worker{ID: "w-unified", Status: "ONLINE", Capabilities: []string{"qmigration"}, LastHeartbeat: now})
	svc := NewService(repo, reg, er)
	m := domain.MigrationTask{Name: "legacy-input", SourceID: "src-unified", TargetID: "dst-unified", Mode: domain.ModeFull, FullEngine: "datax", Parallelism: 2, ValidationEnabled: false}
	if err := svc.Create(ctx, &m); err != nil {
		t.Fatal(err)
	}
	if m.FullEngine != "qmigration" {
		t.Fatalf("legacy engine input was not normalized: %+v", m)
	}
	if err := svc.Start(ctx, m.ID); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		cur, _ := svc.Get(ctx, m.ID)
		if cur.Status == domain.StatusFullMigrating {
			break
		}
		if cur.Status == domain.StatusFailed {
			t.Fatal(cur.LastError)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout %s", cur.Status)
		}
		time.Sleep(5 * time.Millisecond)
	}
	chunks, _ := svc.Chunks(ctx, m.ID)
	if len(chunks) == 0 || chunks[0].SplitType == "EXTERNAL_ENGINE" {
		t.Fatalf("unexpected chunks %+v", chunks)
	}
	job, err := svc.ClaimChunk(ctx, "w-unified")
	if err != nil {
		t.Fatal(err)
	}
	if job.Engine != "qmigration" {
		t.Fatalf("unexpected unified job %+v", job)
	}
}

func TestRollbackControlPlane(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	reg := connector.NewRegistry()
	f := fakeFactory{}
	reg.Register(domain.DataSourcePostgreSQL, f)
	reg.Register(domain.DataSourcePolarDBPostgreSQL, f)
	now := time.Now()
	src := domain.DataSource{ID: "rs", Type: domain.DataSourcePostgreSQL, Database: "db", Schema: "public", CreatedAt: now}
	dst := domain.DataSource{ID: "rd", Type: domain.DataSourcePolarDBPostgreSQL, Database: "db", Schema: "public", CreatedAt: now}
	_ = repo.CreateDataSource(ctx, &src)
	_ = repo.CreateDataSource(ctx, &dst)
	svc := NewService(repo, reg)
	m := domain.MigrationTask{ID: "rollback-task", Name: "r", SourceID: src.ID, TargetID: dst.ID, Mode: domain.ModeFullAndIncremental, Status: domain.StatusFinished, CreatedAt: now, UpdatedAt: now}
	_ = repo.CreateMigration(ctx, &m)
	if err := svc.PrepareRollback(ctx, m.ID); err != nil {
		t.Fatal(err)
	}
	cur, _ := svc.Get(ctx, m.ID)
	if cur.Status != domain.StatusRollbackPreparing {
		t.Fatal(cur.Status)
	}
	if err := svc.MarkRollbackCDCStarted(ctx, m.ID, &domain.CDCPosition{PositionType: "LSN", PositionValue: "0/456"}); err != nil {
		t.Fatal(err)
	}
	if err := svc.RecordRollbackCDCProgress(ctx, m.ID, &domain.CDCPosition{LagMS: 100}); err != nil {
		t.Fatal(err)
	}
	if err := svc.ReadyForRollback(ctx, m.ID, 1000); err != nil {
		t.Fatal(err)
	}
	if err := svc.Rollback(ctx, m.ID); err != nil {
		t.Fatal(err)
	}
	cur, _ = svc.Get(ctx, m.ID)
	if cur.Status != domain.StatusRolledBack {
		t.Fatalf("expected rolled back got %s", cur.Status)
	}
	ps, _ := svc.CDCPositions(ctx, m.ID)
	found := false
	for _, p := range ps {
		if p.Direction == "reverse" {
			found = true
		}
	}
	if !found {
		t.Fatal("reverse position missing")
	}
}

func TestUnifiedCDCJobLifecycle(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	reg := connector.NewRegistry()
	f := fakeFactory{}
	reg.Register(domain.DataSourceMySQL, f)
	reg.Register(domain.DataSourcePolarDBX, f)
	er := engine.NewRegistry()
	er.Register(engine.NewUnified())
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "cdc-src", Type: domain.DataSourceMySQL, Host: "src", Port: 3306, Username: "u", Password: "p", Database: "app", CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "cdc-dst", Type: domain.DataSourcePolarDBX, Host: "dst", Port: 8527, Username: "u", Password: "p", Database: "app2", CreatedAt: now})
	_ = repo.UpsertWorker(ctx, &domain.Worker{ID: "cdc-worker", Status: "ONLINE", Capabilities: []string{"qmigration", "qmigration:mysql-cdc"}, LastHeartbeat: now})
	svc := NewService(repo, reg, er)
	m := domain.MigrationTask{Name: "unified-cdc", SourceID: "cdc-src", TargetID: "cdc-dst", Mode: domain.ModeFullAndIncremental, ChunkRows: 100, BatchRows: 10, Parallelism: 2}
	if err := svc.Create(ctx, &m); err != nil {
		t.Fatal(err)
	}
	if err := svc.Start(ctx, m.ID); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		cur, _ := svc.Get(ctx, m.ID)
		if cur.Status == domain.StatusCDCInitializing {
			break
		}
		if cur.Status == domain.StatusFailed {
			t.Fatalf("prepare failed: %s", cur.LastError)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for CDC_INITIALIZING: %s", cur.Status)
		}
		time.Sleep(5 * time.Millisecond)
	}
	jobs, err := svc.ListEngineJobs(ctx, m.ID)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("expected one QMigration CDC runtime job: %v %+v", err, jobs)
	}
	claim, err := svc.ClaimEngineJob(ctx, "cdc-worker")
	if err != nil {
		t.Fatal(err)
	}
	if claim.Job.Engine != "qmigration" || len(claim.RuntimeConfig.Command) != 1 || claim.RuntimeConfig.Command[0] != "qmigration-mysql-cdc" {
		t.Fatalf("unexpected claim: %+v", claim)
	}
	if err := svc.StartEngineJob(ctx, "cdc-worker", claim.Job.ID); err != nil {
		t.Fatal(err)
	}
	cur, _ := svc.Get(ctx, m.ID)
	if cur.Status != domain.StatusFullMigrating {
		t.Fatalf("expected full migration after CDC start, got %s", cur.Status)
	}
	ready, status, err := svc.EngineJobCDCReady(ctx, "cdc-worker", claim.Job.ID)
	if err != nil || !ready || status != domain.StatusFullMigrating {
		t.Fatalf("CDC capture must start during full load and stage durably, ready=%v status=%s err=%v", ready, status, err)
	}
	cur.Status = domain.StatusCDCCatchingUp
	cur.UpdatedAt = time.Now()
	if err := repo.UpdateMigration(ctx, cur); err != nil {
		t.Fatal(err)
	}
	ready, status, err = svc.EngineJobCDCReady(ctx, "cdc-worker", claim.Job.ID)
	if err != nil || !ready || status != domain.StatusCDCCatchingUp {
		t.Fatalf("CDC reader should be released, ready=%v status=%s err=%v", ready, status, err)
	}
	if err := svc.requestStopEngineJobs(ctx, m.ID, "forward"); err != nil {
		t.Fatal(err)
	}
	stop, err := svc.EngineJobControl(ctx, "cdc-worker", claim.Job.ID)
	if err != nil || !stop {
		t.Fatalf("expected stop request, stop=%v err=%v", stop, err)
	}
	if err := svc.CompleteEngineJob(ctx, "cdc-worker", claim.Job.ID, domain.EngineJobResult{}); err != nil {
		t.Fatal(err)
	}
	jobs, _ = svc.ListEngineJobs(ctx, m.ID)
	if jobs[0].Status != domain.EngineJobStopped {
		t.Fatalf("expected stopped job, got %s", jobs[0].Status)
	}
}

func TestIncrementalUnifiedJobPlansTablesBeforeRender(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	reg := connector.NewRegistry()
	f := fakeFactory{}
	reg.Register(domain.DataSourceMySQL, f)
	reg.Register(domain.DataSourcePolarDBX, f)
	er := engine.NewRegistry()
	er.Register(engine.NewUnified())
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "is", Type: domain.DataSourceMySQL, Host: "src", Port: 3306, Username: "u", Password: "p", Database: "app", CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "it", Type: domain.DataSourcePolarDBX, Host: "dst", Port: 8527, Username: "u", Password: "p", Database: "app2", CreatedAt: now})
	_ = repo.UpsertWorker(ctx, &domain.Worker{ID: "iw", Capabilities: []string{"qmigration", "qmigration:mysql-cdc"}, LastHeartbeat: now})
	svc := NewService(repo, reg, er)
	m := domain.MigrationTask{Name: "inc", SourceID: "is", TargetID: "it", Mode: domain.ModeIncremental}
	if err := svc.Create(ctx, &m); err != nil {
		t.Fatal(err)
	}
	if err := svc.Start(ctx, m.ID); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		cur, _ := svc.Get(ctx, m.ID)
		if cur.Status == domain.StatusCDCInitializing {
			break
		}
		if cur.Status == domain.StatusFailed {
			t.Fatal(cur.LastError)
		}
		if time.Now().After(deadline) {
			t.Fatal("timeout")
		}
		time.Sleep(5 * time.Millisecond)
	}
	tables, _ := svc.Tables(ctx, m.ID)
	if len(tables) != 1 || len(tables[0].Columns) == 0 {
		t.Fatalf("incremental tables not planned: %+v", tables)
	}
	claim, err := svc.ClaimEngineJob(ctx, "iw")
	if err != nil {
		t.Fatal(err)
	}
	if claim.Job.Engine != "qmigration" || !strings.Contains(claim.RuntimeConfig.Content, "orders") {
		t.Fatalf("unexpected QMigration runtime config: %+v", claim)
	}
}

func TestCompatibilityAssessmentNative(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	reg := connector.NewRegistry()
	f := fakeFactory{}
	reg.Register(domain.DataSourceMySQL, f)
	reg.Register(domain.DataSourcePolarDBX, f)
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "as-src", Type: domain.DataSourceMySQL, Database: "app", CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "as-dst", Type: domain.DataSourcePolarDBX, Database: "app", CreatedAt: now})
	svc := NewService(repo, reg)
	m := domain.MigrationTask{Name: "assessment", SourceID: "as-src", TargetID: "as-dst", Mode: domain.ModeFull, FullEngine: "native", AutoCreateTable: true}
	if err := svc.Create(ctx, &m); err != nil {
		t.Fatal(err)
	}
	a, err := svc.AssessCompatibility(ctx, m.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !a.CanStart || a.Unsupported != 0 {
		t.Fatalf("unexpected assessment: %+v", a)
	}
	if a.Compatible < 2 {
		t.Fatalf("expected compatible findings: %+v", a.Findings)
	}
}

func TestCompatibilityAssessmentExternalRequiresExplicitTables(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	reg := connector.NewRegistry()
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "ora", Type: domain.DataSourceOracle, Host: "oracle", Port: 1521, JDBCURL: "jdbc:oracle:thin:@oracle:1521/XEPDB1", DriverClass: "oracle.jdbc.OracleDriver", CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "px", Type: domain.DataSourcePolarDBX, Host: "px", Port: 8527, Database: "app", CreatedAt: now})
	svc := NewService(repo, reg)
	m := domain.MigrationTask{Name: "ora-px", SourceID: "ora", TargetID: "px", Mode: domain.ModeFull, FullEngine: "seatunnel"}
	if err := svc.Create(ctx, &m); err != nil {
		t.Fatal(err)
	}
	a, err := svc.AssessCompatibility(ctx, m.ID)
	if err != nil {
		t.Fatal(err)
	}
	if a.CanStart || a.Unsupported == 0 {
		t.Fatalf("expected unsupported finding: %+v", a)
	}
}

func TestAssessmentIncludesSchemaObjects(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	reg := connector.NewRegistry()
	f := fakeFactory{}
	reg.Register(domain.DataSourceMySQL, f)
	reg.Register(domain.DataSourcePolarDBX, f)
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "ass-src", Type: domain.DataSourceMySQL, Database: "app", CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "ass-dst", Type: domain.DataSourcePolarDBX, Database: "app", CreatedAt: now})
	svc := NewService(repo, reg)
	m := domain.MigrationTask{Name: "assessment", SourceID: "ass-src", TargetID: "ass-dst", Mode: domain.ModeFull, FullEngine: "native", AutoCreateTable: true}
	if err := svc.Create(ctx, &m); err != nil {
		t.Fatal(err)
	}
	a, err := svc.AssessCompatibility(ctx, m.ID)
	if err != nil {
		t.Fatal(err)
	}
	view, trigger := false, false
	for _, f := range a.Findings {
		if f.Code == "VIEW_REVIEW_REQUIRED" {
			view = true
		}
		if f.Code == "TRIGGER_REVIEW_REQUIRED" {
			trigger = true
		}
	}
	if !view || !trigger {
		t.Fatalf("schema objects missing from assessment: %+v", a.Findings)
	}
}

type cdcApplyState struct {
	writes    int
	writeErr  error
	deletes   int
	truncates int
	lastPK    string
	lastPKs   []string
	lastWrite []string
	lookups   int
	lookupRow []connector.Value
	lookupOK  bool
	lookupErr error
	begins    int
	commits   int
	rollbacks int
}

type cdcApplyFactory struct{ state *cdcApplyState }
type cdcApplyConnector struct{ state *cdcApplyState }

func (f cdcApplyFactory) New(domain.DataSource) (connector.Connector, error) {
	return &cdcApplyConnector{state: f.state}, nil
}
func (c *cdcApplyConnector) TestConnection(context.Context) error       { return nil }
func (c *cdcApplyConnector) GetVersion(context.Context) (string, error) { return "fake", nil }
func (c *cdcApplyConnector) ListSchemas(context.Context) ([]domain.SchemaInfo, error) {
	return []domain.SchemaInfo{{Name: "app"}}, nil
}
func (c *cdcApplyConnector) ListTables(context.Context, string) ([]domain.TableInfo, error) {
	return nil, nil
}
func (c *cdcApplyConnector) GetTableMetadata(context.Context, string, string) (*domain.TableMetadata, error) {
	return nil, errors.New("unused")
}
func (c *cdcApplyConnector) Close() error { return nil }
func (c *cdcApplyConnector) ReadBatch(context.Context, connector.ReadBatchRequest) (*connector.RowBatch, error) {
	return &connector.RowBatch{}, nil
}
func (c *cdcApplyConnector) WriteBatch(_ context.Context, req connector.WriteBatchRequest) (int64, error) {
	if c.state.writeErr != nil {
		return 0, c.state.writeErr
	}
	c.state.writes += len(req.Rows)
	if len(req.Rows) > 0 {
		c.state.lastWrite = c.state.lastWrite[:0]
		for _, value := range req.Rows[len(req.Rows)-1] {
			if value.Null {
				c.state.lastWrite = append(c.state.lastWrite, "<NULL>")
			} else {
				c.state.lastWrite = append(c.state.lastWrite, string(value.Raw))
			}
		}
	}
	return int64(len(req.Rows)), nil
}
func (c *cdcApplyConnector) ReadByKey(context.Context, connector.ReadByKeyRequest) ([]connector.Value, bool, error) {
	c.state.lookups++
	if c.state.lookupErr != nil {
		return nil, false, c.state.lookupErr
	}
	return append([]connector.Value(nil), c.state.lookupRow...), c.state.lookupOK, nil
}
func (c *cdcApplyConnector) DeleteByKey(_ context.Context, req connector.DeleteByKeyRequest) error {
	c.state.deletes++
	if len(req.Values) > 0 {
		c.state.lastPKs = c.state.lastPKs[:0]
		for _, value := range req.Values {
			c.state.lastPKs = append(c.state.lastPKs, string(value.Raw))
		}
		if len(req.Values) == 1 {
			c.state.lastPK = string(req.Values[0].Raw)
		}
	} else {
		c.state.lastPK = string(req.Value.Raw)
	}
	return nil
}
func (c *cdcApplyConnector) TruncateTable(context.Context, string, string) error {
	c.state.truncates++
	return nil
}
func (c *cdcApplyConnector) BeginCDCTransaction(context.Context) error { c.state.begins++; return nil }
func (c *cdcApplyConnector) CommitCDCTransaction(context.Context) error {
	c.state.commits++
	return nil
}
func (c *cdcApplyConnector) RollbackCDCTransaction(context.Context) error {
	c.state.rollbacks++
	return nil
}

func TestApplyCDCEventsUpsertDeleteAndCheckpoint(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	state := &cdcApplyState{}
	reg := connector.NewRegistry()
	f := cdcApplyFactory{state: state}
	reg.Register(domain.DataSourceMySQL, f)
	reg.Register(domain.DataSourcePolarDBX, f)
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "cdc-native-src", Type: domain.DataSourceMySQL, Database: "app", CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "cdc-native-dst", Type: domain.DataSourcePolarDBX, Database: "app2", CreatedAt: now})
	task := domain.MigrationTask{ID: "cdc-native-task", Name: "native-cdc", SourceID: "cdc-native-src", TargetID: "cdc-native-dst", Mode: domain.ModeFullAndIncremental, Status: domain.StatusCDCCatchingUp, CreatedAt: now, UpdatedAt: now}
	if err := repo.CreateMigration(ctx, &task); err != nil {
		t.Fatal(err)
	}
	tbl := domain.MigrationTable{ID: "tbl-cdc", TaskID: task.ID, SourceSchema: "app", SourceTable: "orders", TargetSchema: "app2", TargetTable: "orders_new", PrimaryKey: "id", TargetPrimaryKey: "order_id", Columns: []domain.ColumnInfo{{Name: "id", DataType: "bigint", PrimaryKey: true}, {Name: "name", DataType: "varchar"}}, TargetColumns: []domain.ColumnInfo{{Name: "order_id", DataType: "bigint", PrimaryKey: true}, {Name: "customer_name", DataType: "varchar"}}}
	if err := repo.CreateMigrationTable(ctx, &tbl); err != nil {
		t.Fatal(err)
	}
	svc := NewService(repo, reg)
	res, err := svc.ApplyCDCEvents(ctx, task.ID, domain.CDCApplyRequest{Events: []domain.CDCEvent{
		{ID: "e1", Operation: domain.CDCInsert, SourceSchema: "app", SourceTable: "orders", After: []domain.CDCField{{Column: "id", Value: "1"}, {Column: "name", Value: "alice"}}, PositionType: "BINLOG", PositionValue: "mysql-bin.000001:100"},
		{ID: "e2", Operation: domain.CDCUpdate, SourceSchema: "app", SourceTable: "orders", After: []domain.CDCField{{Column: "id", Value: "1"}, {Column: "name", Value: "bob"}}, PositionType: "BINLOG", PositionValue: "mysql-bin.000001:120"},
		{ID: "e3", Operation: domain.CDCDelete, SourceSchema: "app", SourceTable: "orders", Before: []domain.CDCField{{Column: "id", Value: "1"}}, PositionType: "BINLOG", PositionValue: "mysql-bin.000001:140", SourceTimestampMS: time.Now().Add(-10 * time.Millisecond).UnixMilli()},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Applied != 3 || state.writes != 2 || state.deletes != 1 || state.lastPK != "1" || state.begins != 1 || state.commits != 1 || state.rollbacks != 0 {
		t.Fatalf("unexpected apply state res=%+v state=%+v", res, state)
	}
	positions, err := svc.CDCPositions(ctx, task.ID)
	if err != nil || len(positions) == 0 {
		t.Fatalf("missing position: %v %+v", err, positions)
	}
	if positions[0].PositionValue != "mysql-bin.000001:140" {
		t.Fatalf("wrong checkpoint %+v", positions[0])
	}
}

func TestManagedCDCStagesDuringFullLoadAndDrainsInOrder(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	state := &cdcApplyState{}
	reg := connector.NewRegistry()
	f := cdcApplyFactory{state: state}
	reg.Register(domain.DataSourceMySQL, f)
	reg.Register(domain.DataSourcePolarDBX, f)
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "spool-src", Type: domain.DataSourceMySQL, Database: "app", CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "spool-dst", Type: domain.DataSourcePolarDBX, Database: "app", CreatedAt: now})
	task := domain.MigrationTask{ID: "spool-task", SourceID: "spool-src", TargetID: "spool-dst", Mode: domain.ModeFullAndIncremental, Status: domain.StatusFullMigrating, CreatedAt: now, UpdatedAt: now}
	_ = repo.CreateMigration(ctx, &task)
	_ = repo.CreateMigrationTable(ctx, &domain.MigrationTable{ID: "spool-table", TaskID: task.ID, SourceSchema: "app", SourceTable: "orders", TargetSchema: "app", TargetTable: "orders", PrimaryKey: "id", PrimaryKeys: []string{"id"}, Columns: []domain.ColumnInfo{{Name: "id", DataType: "bigint", PrimaryKey: true}}, TargetColumns: []domain.ColumnInfo{{Name: "id", DataType: "bigint", PrimaryKey: true}}})
	job := domain.EngineJob{ID: "spool-job", TaskID: task.ID, Kind: "CDC", Direction: "forward", Engine: "qmigration", Status: domain.EngineJobRunning, WorkerID: "worker-spool", UpdatedAt: now}
	_ = repo.CreateEngineJob(ctx, &job)
	svc := NewService(repo, reg)
	mk := func(id, pos, value string) domain.CDCApplyRequest {
		return domain.CDCApplyRequest{Events: []domain.CDCEvent{{ID: id, Operation: domain.CDCInsert, SourceSchema: "app", SourceTable: "orders", After: []domain.CDCField{{Column: "id", Value: value}}, PositionType: "BINLOG", PositionValue: pos, SourceTimestampMS: time.Now().UnixMilli()}}}
	}
	for _, req := range []domain.CDCApplyRequest{mk("s1", "mysql-bin.000001:100", "1"), mk("s2", "mysql-bin.000001:120", "2")} {
		res, err := svc.ApplyEngineJobCDCEvents(ctx, "worker-spool", job.ID, req)
		if err != nil {
			t.Fatal(err)
		}
		if !res.Staged || res.SpoolSequence == 0 {
			t.Fatalf("expected durable spool result, got %+v", res)
		}
	}
	if state.writes != 0 {
		t.Fatalf("full-load phase wrote CDC directly: %+v", state)
	}
	stats, err := svc.CDCSpoolStats(ctx, task.ID, "forward")
	if err != nil || stats.PendingTransactions != 2 || stats.PendingEvents != 2 {
		t.Fatalf("spool stats err=%v stats=%+v", err, stats)
	}
	positions, _ := svc.CDCPositions(ctx, task.ID)
	if len(positions) != 0 {
		t.Fatalf("staging must not advance target apply checkpoint: %+v", positions)
	}

	cur, _ := repo.GetMigration(ctx, task.ID)
	cur.Status = domain.StatusCDCCatchingUp
	cur.UpdatedAt = time.Now()
	_ = repo.UpdateMigration(ctx, cur)
	stats, err = svc.DrainCDCSpool(ctx, task.ID, "forward", 100)
	if err != nil {
		t.Fatal(err)
	}
	if stats.PendingTransactions != 0 || state.writes != 2 {
		t.Fatalf("spool did not drain exactly two writes: stats=%+v state=%+v", stats, state)
	}
	positions, _ = svc.CDCPositions(ctx, task.ID)
	if len(positions) == 0 || positions[0].PositionValue != "mysql-bin.000001:120" {
		t.Fatalf("wrong post-drain checkpoint %+v", positions)
	}
	res, err := svc.ApplyEngineJobCDCEvents(ctx, "worker-spool", job.ID, mk("s3", "mysql-bin.000001:140", "3"))
	if err != nil {
		t.Fatal(err)
	}
	if res.Staged || state.writes != 3 {
		t.Fatalf("live CDC should apply directly after spool drains: res=%+v state=%+v", res, state)
	}
}

func TestCDCSpoolCapacityRejectsWithoutDurableAck(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	svc := NewService(repo, connector.NewRegistry())
	t.Setenv("QMIGRATION_CDC_SPOOL_MAX_PENDING_BYTES", "1")
	t.Setenv("QMIGRATION_CDC_SPOOL_MAX_TRANSACTION_BYTES", "1048576")
	events := []domain.CDCEvent{{ID: "quota-1", Operation: domain.CDCInsert, SourceSchema: "app", SourceTable: "orders", After: []domain.CDCField{{Column: "id", Value: "1"}}, PositionType: "GTID", PositionValue: "uuid:1", SourceTimestampMS: time.Now().UnixMilli()}}
	if _, err := svc.stageCDCEvents(ctx, "quota-task", "forward", events); err == nil || !strings.Contains(err.Error(), "source position is not acknowledged") {
		t.Fatalf("expected spool capacity failure without source ACK, got %v", err)
	}
	stats, err := svc.CDCSpoolStats(ctx, "quota-task", "forward")
	if err != nil {
		t.Fatal(err)
	}
	if stats.PendingTransactions != 0 || stats.PendingEvents != 0 || stats.PendingBytes != 0 {
		t.Fatalf("rejected transaction leaked into durable spool: %+v", stats)
	}
}

func TestChaosSpoolPersistedBeforeAckRetryIsIdempotent(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	svc := NewService(repo, connector.NewRegistry())
	events := []domain.CDCEvent{{ID: "chaos-spool-1", Operation: domain.CDCInsert, SourceSchema: "app", SourceTable: "orders", After: []domain.CDCField{{Column: "id", Value: "1"}}, PositionType: "GTID", PositionValue: "uuid:1", Resource: "mysql-bin.000001"}}
	t.Setenv("QMIGRATION_ENABLE_FAULT_INJECTION", "1")
	t.Setenv("QMIGRATION_FAULT_PLAN", "cdc.spool.after_persist_before_ack=1")
	if _, err := svc.stageCDCEvents(ctx, "chaos-spool-task", "forward", events); err == nil || !strings.Contains(err.Error(), "after_persist_before_ack") {
		t.Fatalf("expected injected post-persist failure, got %v", err)
	}
	stats, err := svc.CDCSpoolStats(ctx, "chaos-spool-task", "forward")
	if err != nil || stats.PendingTransactions != 1 {
		t.Fatalf("durable spool was lost after injected pre-ACK failure: err=%v stats=%+v", err, stats)
	}
	t.Setenv("QMIGRATION_ENABLE_FAULT_INJECTION", "")
	res, err := svc.stageCDCEvents(ctx, "chaos-spool-task", "forward", events)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Staged || res.SpoolSequence == 0 {
		t.Fatalf("retry did not reuse durable transaction: %+v", res)
	}
	stats, _ = svc.CDCSpoolStats(ctx, "chaos-spool-task", "forward")
	if stats.PendingTransactions != 1 {
		t.Fatalf("retry duplicated durable spool record: %+v", stats)
	}
}

func TestChaosCheckpointPersistedBeforeSourceAckPreventsDoubleApply(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	state := &cdcApplyState{}
	reg := connector.NewRegistry()
	f := cdcApplyFactory{state: state}
	reg.Register(domain.DataSourceMySQL, f)
	reg.Register(domain.DataSourcePolarDBX, f)
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "chaos-ack-src", Type: domain.DataSourceMySQL, Database: "app", CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "chaos-ack-dst", Type: domain.DataSourcePolarDBX, Database: "app", CreatedAt: now})
	task := domain.MigrationTask{ID: "chaos-ack-task", SourceID: "chaos-ack-src", TargetID: "chaos-ack-dst", Mode: domain.ModeIncremental, Status: domain.StatusCDCCatchingUp, CreatedAt: now, UpdatedAt: now}
	_ = repo.CreateMigration(ctx, &task)
	_ = repo.CreateMigrationTable(ctx, &domain.MigrationTable{ID: "chaos-ack-table", TaskID: task.ID, SourceSchema: "app", SourceTable: "orders", TargetSchema: "app", TargetTable: "orders", PrimaryKey: "id", PrimaryKeys: []string{"id"}, Columns: []domain.ColumnInfo{{Name: "id", DataType: "bigint", PrimaryKey: true}}, TargetColumns: []domain.ColumnInfo{{Name: "id", DataType: "bigint", PrimaryKey: true}}})
	svc := NewService(repo, reg)
	req := domain.CDCApplyRequest{Events: []domain.CDCEvent{{ID: "chaos-ack-e1", Operation: domain.CDCInsert, SourceSchema: "app", SourceTable: "orders", After: []domain.CDCField{{Column: "id", Value: "7"}}, PositionType: "GTID", PositionValue: "uuid:7"}}}
	t.Setenv("QMIGRATION_ENABLE_FAULT_INJECTION", "1")
	t.Setenv("QMIGRATION_FAULT_PLAN", "cdc.apply.after_checkpoint_before_source_ack=1")
	if _, err := svc.ApplyCDCEvents(ctx, task.ID, req); err == nil || !strings.Contains(err.Error(), "after_checkpoint_before_source_ack") {
		t.Fatalf("expected injected post-checkpoint failure, got %v", err)
	}
	if state.writes != 1 || state.commits != 1 {
		t.Fatalf("target transaction did not commit before injected source-ACK failure: %+v", state)
	}
	pos, _ := svc.CDCPositions(ctx, task.ID)
	if len(pos) == 0 || pos[0].PositionValue != "uuid:7" {
		t.Fatalf("checkpoint was not durable before source ACK: %+v", pos)
	}
	t.Setenv("QMIGRATION_ENABLE_FAULT_INJECTION", "")
	res, err := svc.ApplyCDCEvents(ctx, task.ID, req)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Duplicate || state.writes != 1 {
		t.Fatalf("source retry after ACK failure re-applied target rows: res=%+v state=%+v", res, state)
	}
}

func TestChaosSpoolApplyBeforeMarkRecoversWithoutDoubleWrite(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	state := &cdcApplyState{}
	reg := connector.NewRegistry()
	f := cdcApplyFactory{state: state}
	reg.Register(domain.DataSourceMySQL, f)
	reg.Register(domain.DataSourcePolarDBX, f)
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "chaos-drain-src", Type: domain.DataSourceMySQL, Database: "app", CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "chaos-drain-dst", Type: domain.DataSourcePolarDBX, Database: "app", CreatedAt: now})
	task := domain.MigrationTask{ID: "chaos-drain-task", SourceID: "chaos-drain-src", TargetID: "chaos-drain-dst", Mode: domain.ModeFullAndIncremental, Status: domain.StatusCDCCatchingUp, CreatedAt: now, UpdatedAt: now}
	_ = repo.CreateMigration(ctx, &task)
	_ = repo.CreateMigrationTable(ctx, &domain.MigrationTable{ID: "chaos-drain-table", TaskID: task.ID, SourceSchema: "app", SourceTable: "orders", TargetSchema: "app", TargetTable: "orders", PrimaryKey: "id", PrimaryKeys: []string{"id"}, Columns: []domain.ColumnInfo{{Name: "id", DataType: "bigint", PrimaryKey: true}}, TargetColumns: []domain.ColumnInfo{{Name: "id", DataType: "bigint", PrimaryKey: true}}})
	events := []domain.CDCEvent{{ID: "chaos-drain-e1", Operation: domain.CDCInsert, SourceSchema: "app", SourceTable: "orders", After: []domain.CDCField{{Column: "id", Value: "9"}}, PositionType: "GTID", PositionValue: "uuid:9"}}
	svc := NewService(repo, reg)
	if _, err := svc.stageCDCEvents(ctx, task.ID, "forward", events); err != nil {
		t.Fatal(err)
	}
	t.Setenv("QMIGRATION_ENABLE_FAULT_INJECTION", "1")
	t.Setenv("QMIGRATION_FAULT_PLAN", "cdc.spool.after_target_apply_before_mark=1")
	if _, err := svc.DrainCDCSpool(ctx, task.ID, "forward", 10); err == nil || !strings.Contains(err.Error(), "after_target_apply_before_mark") {
		t.Fatalf("expected injected drain failure, got %v", err)
	}
	if state.writes != 1 || state.commits != 1 {
		t.Fatalf("target not committed before fault: %+v", state)
	}
	stats, _ := svc.CDCSpoolStats(ctx, task.ID, "forward")
	if stats.PendingTransactions != 1 {
		t.Fatalf("spool should remain pending until mark: %+v", stats)
	}
	t.Setenv("QMIGRATION_ENABLE_FAULT_INJECTION", "")
	stats, err := svc.DrainCDCSpool(ctx, task.ID, "forward", 10)
	if err != nil {
		t.Fatal(err)
	}
	if stats.PendingTransactions != 0 || state.writes != 1 {
		t.Fatalf("retry did not deduplicate committed transaction: stats=%+v state=%+v", stats, state)
	}
}

func TestCDCSpoolDuplicatePositionIsIdempotent(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	svc := NewService(repo, connector.NewRegistry())
	events := []domain.CDCEvent{{ID: "dup-spool-1", Operation: domain.CDCInsert, SourceSchema: "app", SourceTable: "orders", After: []domain.CDCField{{Column: "id", Value: "1"}}, PositionType: "BINLOG", PositionValue: "mysql-bin.000001:100", Resource: "mysql-bin.000001", SourceTimestampMS: time.Now().UnixMilli()}}
	first, err := svc.stageCDCEvents(ctx, "dup-spool-task", "forward", events)
	if err != nil {
		t.Fatal(err)
	}
	second, err := svc.stageCDCEvents(ctx, "dup-spool-task", "forward", events)
	if err != nil {
		t.Fatal(err)
	}
	if first.SpoolSequence == 0 || second.SpoolSequence != first.SpoolSequence {
		t.Fatalf("duplicate spool transaction did not reuse sequence: first=%+v second=%+v", first, second)
	}
	stats, err := svc.CDCSpoolStats(ctx, "dup-spool-task", "forward")
	if err != nil {
		t.Fatal(err)
	}
	if stats.PendingTransactions != 1 || stats.PendingEvents != 1 {
		t.Fatalf("duplicate source position created multiple durable transactions: %+v", stats)
	}
}

func TestCDCJobResumePrefersNewestPendingSpoolPosition(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	reg := connector.NewRegistry()
	f := fakeFactory{}
	reg.Register(domain.DataSourceMySQL, f)
	reg.Register(domain.DataSourcePolarDBX, f)
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "resume-src", Type: domain.DataSourceMySQL, Database: "app", CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "resume-dst", Type: domain.DataSourcePolarDBX, Database: "app", CreatedAt: now})
	task := domain.MigrationTask{ID: "resume-task", SourceID: "resume-src", TargetID: "resume-dst", Mode: domain.ModeFullAndIncremental, Status: domain.StatusFullMigrating, CDCStartPositionType: "BINLOG", CDCStartPositionValue: "mysql-bin.000001:4", CreatedAt: now, UpdatedAt: now}
	_ = repo.CreateMigration(ctx, &task)
	_ = repo.CreateCDCPosition(ctx, &domain.CDCPosition{ID: "applied-old", TaskID: task.ID, Direction: "forward", PositionType: "BINLOG", PositionValue: "mysql-bin.000001:100", RecordedAt: now})
	_ = repo.CreateCDCSpool(ctx, &domain.CDCSpoolRecord{ID: "pending-new", TaskID: task.ID, Direction: "forward", PositionType: "BINLOG", PositionValue: "mysql-bin.000001:500", EventCount: 1, Status: domain.CDCSpoolPending, CreatedAt: now})
	svc := NewService(repo, reg)
	rendered, _, _, _, err := svc.cdcRenderTask(ctx, &task, "forward")
	if err != nil {
		t.Fatal(err)
	}
	if rendered.CDCStartPositionValue != "mysql-bin.000001:500" {
		t.Fatalf("resume ignored durable spool: %+v", rendered)
	}
}

func TestApplyCDCEventsTransactionalTruncate(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	state := &cdcApplyState{}
	reg := connector.NewRegistry()
	reg.Register(domain.DataSourceGBase8s, cdcApplyFactory{state: state})
	now := time.Now()
	src := domain.DataSource{ID: "g8s-src-truncate", Type: domain.DataSourceGBase8s, Database: "app", Schema: "app", CreatedAt: now}
	dst := domain.DataSource{ID: "g8s-dst-truncate", Type: domain.DataSourceGBase8s, Database: "app", Schema: "app", CreatedAt: now}
	_ = repo.CreateDataSource(ctx, &src)
	_ = repo.CreateDataSource(ctx, &dst)
	task := domain.MigrationTask{ID: "m-truncate", Name: "truncate", SourceID: src.ID, TargetID: dst.ID, Mode: domain.ModeIncremental, Status: domain.StatusCDCCatchingUp, FullEngine: "native", CDCEngine: "native", CDCConflictMode: "SOURCE_WINS", CreatedAt: now, UpdatedAt: now}
	_ = repo.CreateMigration(ctx, &task)
	_ = repo.CreateMigrationTable(ctx, &domain.MigrationTable{ID: "truncate-table", TaskID: task.ID, SourceSchema: "app", SourceTable: "orders", TargetSchema: "app", TargetTable: "orders", PrimaryKey: "id", TargetPrimaryKey: "id", Columns: []domain.ColumnInfo{{Name: "id", DataType: "bigint"}, {Name: "name", DataType: "varchar"}}, TargetColumns: []domain.ColumnInfo{{Name: "id", DataType: "bigint"}, {Name: "name", DataType: "varchar"}}})
	svc := NewService(repo, reg)
	res, err := svc.ApplyCDCEvents(ctx, task.ID, domain.CDCApplyRequest{Events: []domain.CDCEvent{
		{ID: "e1", Operation: domain.CDCInsert, SourceSchema: "app", SourceTable: "orders", After: []domain.CDCField{{Column: "id", Value: "1"}, {Column: "name", Value: "x"}}, PositionType: "GBASE8S_CDC_SEQ", PositionValue: "restart=20;commit=30"},
		{ID: "e2", Operation: domain.CDCTruncate, SourceSchema: "app", SourceTable: "orders", PositionType: "GBASE8S_CDC_SEQ", PositionValue: "restart=30;commit=30"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Applied != 2 || state.writes != 1 || state.truncates != 1 || state.begins != 1 || state.commits != 1 || state.rollbacks != 0 {
		t.Fatalf("result=%+v state=%+v", res, state)
	}
}

func TestApplyCDCEventsRejectsNonFinalTruncate(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	state := &cdcApplyState{}
	reg := connector.NewRegistry()
	reg.Register(domain.DataSourceGBase8s, cdcApplyFactory{state: state})
	now := time.Now()
	src := domain.DataSource{ID: "g8s-src-truncate2", Type: domain.DataSourceGBase8s, Database: "app", Schema: "app", CreatedAt: now}
	dst := domain.DataSource{ID: "g8s-dst-truncate2", Type: domain.DataSourceGBase8s, Database: "app", Schema: "app", CreatedAt: now}
	_ = repo.CreateDataSource(ctx, &src)
	_ = repo.CreateDataSource(ctx, &dst)
	task := domain.MigrationTask{ID: "m-truncate2", SourceID: src.ID, TargetID: dst.ID, Mode: domain.ModeIncremental, Status: domain.StatusCDCCatchingUp, FullEngine: "native", CDCEngine: "native", CreatedAt: now, UpdatedAt: now}
	_ = repo.CreateMigration(ctx, &task)
	_ = repo.CreateMigrationTable(ctx, &domain.MigrationTable{ID: "truncate-table2", TaskID: task.ID, SourceSchema: "app", SourceTable: "orders", TargetSchema: "app", TargetTable: "orders", PrimaryKey: "id", TargetPrimaryKey: "id", Columns: []domain.ColumnInfo{{Name: "id", DataType: "bigint"}}, TargetColumns: []domain.ColumnInfo{{Name: "id", DataType: "bigint"}}})
	svc := NewService(repo, reg)
	_, err := svc.ApplyCDCEvents(ctx, task.ID, domain.CDCApplyRequest{Events: []domain.CDCEvent{
		{Operation: domain.CDCTruncate, SourceSchema: "app", SourceTable: "orders", PositionType: "GBASE8S_CDC_SEQ", PositionValue: "restart=40;commit=40"},
		{Operation: domain.CDCInsert, SourceSchema: "app", SourceTable: "orders", After: []domain.CDCField{{Column: "id", Value: "2"}}, PositionType: "GBASE8S_CDC_SEQ", PositionValue: "restart=40;commit=40"},
	}})
	if err == nil || !strings.Contains(err.Error(), "final event") {
		t.Fatalf("err=%v", err)
	}
	if state.commits != 0 || state.rollbacks != 1 {
		t.Fatalf("state=%+v", state)
	}
}

func TestApplyCDCEventsDeduplicatesConfirmedPosition(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	state := &cdcApplyState{}
	reg := connector.NewRegistry()
	f := cdcApplyFactory{state: state}
	reg.Register(domain.DataSourceMySQL, f)
	reg.Register(domain.DataSourcePolarDBX, f)
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "dup-src", Type: domain.DataSourceMySQL, Database: "app", CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "dup-dst", Type: domain.DataSourcePolarDBX, Database: "app", CreatedAt: now})
	task := domain.MigrationTask{ID: "dup-task", SourceID: "dup-src", TargetID: "dup-dst", Mode: domain.ModeIncremental, Status: domain.StatusCDCCatchingUp, CreatedAt: now, UpdatedAt: now}
	_ = repo.CreateMigration(ctx, &task)
	_ = repo.CreateMigrationTable(ctx, &domain.MigrationTable{ID: "dup-table", TaskID: task.ID, SourceSchema: "app", SourceTable: "orders", TargetSchema: "app", TargetTable: "orders", PrimaryKey: "id", PrimaryKeys: []string{"id"}, Columns: []domain.ColumnInfo{{Name: "id", DataType: "bigint", PrimaryKey: true}}, TargetColumns: []domain.ColumnInfo{{Name: "id", DataType: "bigint", PrimaryKey: true}}})
	svc := NewService(repo, reg)
	req := domain.CDCApplyRequest{Events: []domain.CDCEvent{{ID: "dup-e1", Operation: domain.CDCInsert, SourceSchema: "app", SourceTable: "orders", After: []domain.CDCField{{Column: "id", Value: "1"}}, PositionType: "GTID", PositionValue: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-10"}}}
	if _, err := svc.ApplyCDCEvents(ctx, task.ID, req); err != nil {
		t.Fatal(err)
	}
	res, err := svc.ApplyCDCEvents(ctx, task.ID, req)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Duplicate || res.Applied != 0 {
		t.Fatalf("expected duplicate no-op, got %+v", res)
	}
	if state.writes != 1 {
		t.Fatalf("duplicate event was written again: %+v", state)
	}
}

func TestManagedCDCFailureCreatesDLQAndReplayResolves(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	state := &cdcApplyState{writeErr: errors.New("target type mismatch")}
	reg := connector.NewRegistry()
	f := cdcApplyFactory{state: state}
	reg.Register(domain.DataSourceMySQL, f)
	reg.Register(domain.DataSourcePolarDBX, f)
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "dlq-src", Type: domain.DataSourceMySQL, Database: "app", CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "dlq-dst", Type: domain.DataSourcePolarDBX, Database: "app", CreatedAt: now})
	task := domain.MigrationTask{ID: "dlq-task", SourceID: "dlq-src", TargetID: "dlq-dst", Mode: domain.ModeIncremental, Status: domain.StatusCDCCatchingUp, CreatedAt: now, UpdatedAt: now}
	_ = repo.CreateMigration(ctx, &task)
	_ = repo.CreateMigrationTable(ctx, &domain.MigrationTable{ID: "dlq-table", TaskID: task.ID, SourceSchema: "app", SourceTable: "orders", TargetSchema: "app", TargetTable: "orders", PrimaryKey: "id", PrimaryKeys: []string{"id"}, Columns: []domain.ColumnInfo{{Name: "id", DataType: "bigint", PrimaryKey: true}}, TargetColumns: []domain.ColumnInfo{{Name: "id", DataType: "bigint", PrimaryKey: true}}})
	job := domain.EngineJob{ID: "dlq-job", TaskID: task.ID, Kind: "CDC", Direction: "forward", Engine: "native-mysql-cdc", Status: domain.EngineJobRunning, WorkerID: "worker-1", UpdatedAt: now}
	if err := repo.CreateEngineJob(ctx, &job); err != nil {
		t.Fatal(err)
	}
	svc := NewService(repo, reg)
	req := domain.CDCApplyRequest{Events: []domain.CDCEvent{{ID: "dlq-e1", Operation: domain.CDCInsert, SourceSchema: "app", SourceTable: "orders", After: []domain.CDCField{{Column: "id", Value: "9"}}, PositionType: "BINLOG", PositionValue: "mysql-bin.000009:900"}}}
	if _, err := svc.ApplyEngineJobCDCEvents(ctx, "worker-1", job.ID, req); err == nil {
		t.Fatal("expected apply failure")
	}
	items, err := svc.CDCDeadLetters(ctx, task.ID)
	if err != nil || len(items) != 1 {
		t.Fatalf("missing DLQ: err=%v items=%+v", err, items)
	}
	if items[0].Status != domain.CDCDeadLetterOpen || items[0].RetryCount != 1 {
		t.Fatalf("unexpected DLQ %+v", items[0])
	}
	state.writeErr = nil
	res, err := svc.ReplayCDCDeadLetter(ctx, task.ID, items[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if res.Applied != 1 {
		t.Fatalf("replay result %+v", res)
	}
	resolved, err := repo.GetCDCDeadLetter(ctx, items[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Status != domain.CDCDeadLetterResolved || resolved.ResolvedAt.IsZero() {
		t.Fatalf("DLQ not resolved: %+v", resolved)
	}
}

func TestApplyCDCEventsRequiresPositionBeforeWrite(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	state := &cdcApplyState{}
	reg := connector.NewRegistry()
	f := cdcApplyFactory{state: state}
	reg.Register(domain.DataSourceMySQL, f)
	reg.Register(domain.DataSourcePolarDBX, f)
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "ps", Type: domain.DataSourceMySQL, Database: "app", CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "pt", Type: domain.DataSourcePolarDBX, Database: "app2", CreatedAt: now})
	task := domain.MigrationTask{ID: "p-task", SourceID: "ps", TargetID: "pt", Mode: domain.ModeIncremental, Status: domain.StatusCDCCatchingUp, CreatedAt: now, UpdatedAt: now}
	_ = repo.CreateMigration(ctx, &task)
	_ = repo.CreateMigrationTable(ctx, &domain.MigrationTable{ID: "p-table", TaskID: task.ID, SourceSchema: "app", SourceTable: "orders", TargetSchema: "app2", TargetTable: "orders", PrimaryKey: "id", Columns: []domain.ColumnInfo{{Name: "id", DataType: "bigint", PrimaryKey: true}}, TargetColumns: []domain.ColumnInfo{{Name: "id", DataType: "bigint", PrimaryKey: true}}})
	svc := NewService(repo, reg)
	_, err := svc.ApplyCDCEvents(ctx, task.ID, domain.CDCApplyRequest{Events: []domain.CDCEvent{{Operation: domain.CDCInsert, SourceSchema: "app", SourceTable: "orders", After: []domain.CDCField{{Column: "id", Value: "1"}}}}})
	if err == nil {
		t.Fatal("expected missing position error")
	}
	if state.writes != 0 {
		t.Fatalf("rows were written before position validation: %+v", state)
	}
}

type autoFactory struct {
	numeric   bool
	noPK      bool
	composite bool
}
type autoConnector struct {
	numeric   bool
	noPK      bool
	composite bool
}

func (f autoFactory) New(domain.DataSource) (connector.Connector, error) {
	return autoConnector{numeric: f.numeric, noPK: f.noPK, composite: f.composite}, nil
}
func (c autoConnector) TestConnection(context.Context) error       { return nil }
func (c autoConnector) GetVersion(context.Context) (string, error) { return "fake", nil }
func (c autoConnector) ListSchemas(context.Context) ([]domain.SchemaInfo, error) {
	return []domain.SchemaInfo{{Name: "app"}}, nil
}
func (c autoConnector) ListTables(_ context.Context, schema string) ([]domain.TableInfo, error) {
	return []domain.TableInfo{{Schema: schema, Name: "items", Rows: 10}}, nil
}
func (c autoConnector) GetTableMetadata(_ context.Context, schema, table string) (*domain.TableMetadata, error) {
	cols := []domain.ColumnInfo{{Name: "id", DataType: "varchar", ColumnType: "varchar(32)", PrimaryKey: true, Ordinal: 1}, {Name: "v", DataType: "varchar", ColumnType: "varchar(32)", Ordinal: 2}}
	m := &domain.TableMetadata{Schema: schema, Name: table, Columns: cols, PrimaryKey: "id", PrimaryKeys: []string{"id"}, PrimaryKeyType: "varchar(32)", EstimatedRows: 10, HasRows: true}
	if c.noPK {
		m.PrimaryKey = ""
		m.PrimaryKeys = nil
		m.PrimaryKeyType = ""
		m.Columns[0].PrimaryKey = false
	}
	if c.composite {
		m.Columns = []domain.ColumnInfo{{Name: "tenant_id", DataType: "varchar", ColumnType: "varchar(32)", PrimaryKey: true, Ordinal: 1}, {Name: "id", DataType: "bigint", ColumnType: "bigint", PrimaryKey: true, Ordinal: 2}, {Name: "v", DataType: "varchar", ColumnType: "varchar(32)", Ordinal: 3}}
		m.PrimaryKey = "tenant_id"
		m.PrimaryKeys = []string{"tenant_id", "id"}
		m.PrimaryKeyType = "varchar(32)"
	}
	if c.numeric {
		m.Columns[0].DataType = "bigint"
		m.Columns[0].ColumnType = "bigint"
		m.PrimaryKeyType = "bigint"
		m.PrimaryKeyNumeric = true
		m.MinPK = 1
		m.MaxPK = 10
	}
	return m, nil
}
func (c autoConnector) PlanKeysetBoundaries(_ context.Context, req connector.KeysetBoundaryRequest) ([][]connector.Value, error) {
	if req.Partitions <= 1 {
		return nil, nil
	}
	out := make([][]connector.Value, 0, req.Partitions-1)
	for i := 1; i < req.Partitions; i++ {
		if c.composite {
			out = append(out, []connector.Value{{Raw: []byte{byte('a' + i)}}, {Raw: []byte("10")}})
		} else {
			out = append(out, []connector.Value{{Raw: []byte{byte('a' + i)}}})
		}
	}
	return out, nil
}
func (c autoConnector) Close() error                               { return nil }
func (c autoConnector) EnsureSchema(context.Context, string) error { return nil }
func (c autoConnector) CreateTable(context.Context, string, string, []domain.ColumnInfo, string) error {
	return nil
}
func (c autoConnector) CreateTableWithPrimaryKeys(context.Context, string, string, []domain.ColumnInfo, []string) error {
	return nil
}

func TestAutoEngineRoutesStringPKToNativeKeyset(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	reg := connector.NewRegistry()
	f := autoFactory{numeric: false}
	reg.Register(domain.DataSourceMySQL, f)
	reg.Register(domain.DataSourcePolarDBX, f)
	er := engine.NewRegistry()
	er.Register(engine.NewUnified())
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "auto-s", Type: domain.DataSourceMySQL, Host: "s", Port: 3306, Database: "app", CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "auto-t", Type: domain.DataSourcePolarDBX, Host: "t", Port: 8527, Database: "app", CreatedAt: now})
	_ = repo.UpsertWorker(ctx, &domain.Worker{ID: "auto-w", Capabilities: []string{"qmigration"}, LastHeartbeat: now})
	svc := NewService(repo, reg, er)
	m := domain.MigrationTask{Name: "auto", SourceID: "auto-s", TargetID: "auto-t", Mode: domain.ModeFull, FullEngine: "auto", AutoCreateTable: true, ValidationEnabled: false}
	if err := svc.Create(ctx, &m); err != nil {
		t.Fatal(err)
	}
	if err := svc.Start(ctx, m.ID); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		cur, _ := svc.Get(ctx, m.ID)
		if cur.Status == domain.StatusFullMigrating {
			break
		}
		if cur.Status == domain.StatusFailed {
			t.Fatal(cur.LastError)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout %s", cur.Status)
		}
		time.Sleep(5 * time.Millisecond)
	}
	tables, _ := svc.Tables(ctx, m.ID)
	if len(tables) != 1 || tables[0].Engine != "qmigration" {
		t.Fatalf("unexpected auto route %+v", tables)
	}
	chunks, _ := svc.Chunks(ctx, m.ID)
	if len(chunks) != 1 || chunks[0].SplitType != "PRIMARY_KEY_KEYSET" {
		t.Fatalf("expected generic keyset chunk, got %+v", chunks)
	}
	job, err := svc.ClaimChunk(ctx, "auto-w")
	if err != nil {
		t.Fatal(err)
	}
	if job.Engine != "qmigration" {
		t.Fatalf("unexpected job %+v", job)
	}
}

func TestStringMigrationKeyPlansParallelBoundedKeyset(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	reg := connector.NewRegistry()
	f := autoFactory{numeric: false}
	reg.Register(domain.DataSourceMySQL, f)
	reg.Register(domain.DataSourcePolarDBX, f)
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "bk-s", Type: domain.DataSourceMySQL, Database: "app", CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "bk-t", Type: domain.DataSourcePolarDBX, Database: "app", CreatedAt: now})
	_ = repo.UpsertWorker(ctx, &domain.Worker{ID: "bk-w", Capabilities: []string{"qmigration"}, LastHeartbeat: now})
	svc := NewService(repo, reg)
	m := domain.MigrationTask{Name: "bounded-keyset", SourceID: "bk-s", TargetID: "bk-t", Mode: domain.ModeFull, FullEngine: "native", AutoCreateTable: true, ValidationEnabled: false, ChunkRows: 2, Parallelism: 2}
	if err := svc.Create(ctx, &m); err != nil {
		t.Fatal(err)
	}
	if err := svc.Start(ctx, m.ID); err != nil {
		t.Fatal(err)
	}
	cur := waitMigrationStatus(t, svc, ctx, m.ID, domain.StatusFullMigrating)
	if cur.Status == domain.StatusFailed {
		t.Fatal(cur.LastError)
	}
	chunks, _ := svc.Chunks(ctx, m.ID)
	if len(chunks) != 5 {
		t.Fatalf("expected 5 bounded keyset chunks, got %+v", chunks)
	}
	for i, ch := range chunks {
		if ch.SplitType != "PRIMARY_KEY_KEYSET" {
			t.Fatalf("chunk %d split=%s", i, ch.SplitType)
		}
		if i > 0 && ch.StartCursorJSON != chunks[i-1].EndCursorJSON {
			t.Fatalf("chunk %d lower bound not contiguous", i)
		}
	}
	if chunks[0].StartCursorJSON != "" || chunks[len(chunks)-1].EndCursorJSON != "" {
		t.Fatalf("unexpected edge bounds: first=%+v last=%+v", chunks[0], chunks[len(chunks)-1])
	}
}

func TestNativeCompositePKUsesResumableKeyset(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	reg := connector.NewRegistry()
	f := autoFactory{composite: true}
	reg.Register(domain.DataSourceMySQL, f)
	reg.Register(domain.DataSourcePolarDBX, f)
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "cmp-s", Type: domain.DataSourceMySQL, Database: "app", CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "cmp-t", Type: domain.DataSourcePolarDBX, Database: "app", CreatedAt: now})
	_ = repo.UpsertWorker(ctx, &domain.Worker{ID: "cmp-w", Capabilities: []string{"qmigration"}, LastHeartbeat: now})
	svc := NewService(repo, reg)
	m := domain.MigrationTask{Name: "cmp", SourceID: "cmp-s", TargetID: "cmp-t", Mode: domain.ModeFull, FullEngine: "native", AutoCreateTable: true, ValidationEnabled: true}
	if err := svc.Create(ctx, &m); err != nil {
		t.Fatal(err)
	}
	if err := svc.Start(ctx, m.ID); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		cur, _ := svc.Get(ctx, m.ID)
		if cur.Status == domain.StatusFullMigrating {
			break
		}
		if cur.Status == domain.StatusFailed {
			t.Fatal(cur.LastError)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout %s", cur.Status)
		}
		time.Sleep(5 * time.Millisecond)
	}
	tables, _ := svc.Tables(ctx, m.ID)
	if len(tables) != 1 || len(tables[0].PrimaryKeys) != 2 || tables[0].PrimaryKeys[0] != "tenant_id" || tables[0].PrimaryKeys[1] != "id" {
		t.Fatalf("table=%+v", tables)
	}
	chunks, _ := svc.Chunks(ctx, m.ID)
	if len(chunks) != 1 || chunks[0].SplitType != "PRIMARY_KEY_KEYSET" {
		t.Fatalf("chunks=%+v", chunks)
	}
}

func TestAutoEngineKeepsNumericPKNative(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	reg := connector.NewRegistry()
	f := autoFactory{numeric: true}
	reg.Register(domain.DataSourceMySQL, f)
	reg.Register(domain.DataSourcePolarDBX, f)
	er := engine.NewRegistry()
	er.Register(engine.NewUnified())
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "auto-ns", Type: domain.DataSourceMySQL, Database: "app", CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "auto-nt", Type: domain.DataSourcePolarDBX, Database: "app", CreatedAt: now})
	_ = repo.UpsertWorker(ctx, &domain.Worker{ID: "auto-nw", Capabilities: []string{"qmigration"}, LastHeartbeat: now})
	svc := NewService(repo, reg, er)
	m := domain.MigrationTask{Name: "auto-native", SourceID: "auto-ns", TargetID: "auto-nt", Mode: domain.ModeFull, FullEngine: "auto", AutoCreateTable: true, ValidationEnabled: false, ChunkRows: 5}
	if err := svc.Create(ctx, &m); err != nil {
		t.Fatal(err)
	}
	if err := svc.Start(ctx, m.ID); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		cur, _ := svc.Get(ctx, m.ID)
		if cur.Status == domain.StatusFullMigrating {
			break
		}
		if cur.Status == domain.StatusFailed {
			t.Fatal(cur.LastError)
		}
		if time.Now().After(deadline) {
			t.Fatal("timeout")
		}
		time.Sleep(5 * time.Millisecond)
	}
	tables, _ := svc.Tables(ctx, m.ID)
	if len(tables) != 1 || tables[0].Engine != "qmigration" {
		t.Fatalf("unexpected route %+v", tables)
	}
	chunks, _ := svc.Chunks(ctx, m.ID)
	if len(chunks) != 2 || chunks[0].SplitType != "PRIMARY_KEY_RANGE" {
		t.Fatalf("unexpected chunks %+v", chunks)
	}
}

func TestUnifiedEngineDoesNotFallbackForUnstableTable(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	reg := connector.NewRegistry()
	f := autoFactory{numeric: false, noPK: true}
	reg.Register(domain.DataSourceMySQL, f)
	reg.Register(domain.DataSourcePolarDBX, f)
	er := engine.NewRegistry()
	er.Register(engine.NewUnified())
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "cap-s", Type: domain.DataSourceMySQL, Database: "app", CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "cap-t", Type: domain.DataSourcePolarDBX, Database: "app", CreatedAt: now})
	_ = repo.UpsertWorker(ctx, &domain.Worker{ID: "q-worker", Capabilities: []string{"qmigration"}, LastHeartbeat: now})
	svc := NewService(repo, reg, er)
	m := domain.MigrationTask{Name: "unstable", SourceID: "cap-s", TargetID: "cap-t", Mode: domain.ModeFull, AutoCreateTable: true, ValidationEnabled: false}
	if err := svc.Create(ctx, &m); err != nil {
		t.Fatal(err)
	}
	if err := svc.Start(ctx, m.ID); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		cur, _ := svc.Get(ctx, m.ID)
		if cur.Status == domain.StatusFailed {
			if !strings.Contains(cur.LastError, "stable migration key") && !strings.Contains(cur.LastError, "UNIQUE NOT NULL") {
				t.Fatalf("unexpected failure: %s", cur.LastError)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected failure, status=%s", cur.Status)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestAssessmentFindsMissingIndexAndDeferredForeignKey(t *testing.T) {
	var findings []domain.CompatibilityFinding
	add := func(level domain.CompatibilityLevel, objectType, sourceObject, targetObject, code, message string) {
		findings = append(findings, domain.CompatibilityFinding{Level: level, ObjectType: objectType, SourceObject: sourceObject, TargetObject: targetObject, Code: code, Message: message})
	}
	source := &domain.TableMetadata{
		Indexes: []domain.IndexInfo{
			{Name: "PRIMARY", Columns: []string{"id"}, Unique: true, Primary: true},
			{Name: "uk_email", Columns: []string{"email"}, Unique: true},
			{Name: "idx_tenant_name", Columns: []string{"tenant_id", "name"}},
		},
		ForeignKeys: []domain.ForeignKeyInfo{{Name: "fk_tenant", Columns: []string{"tenant_id"}, ReferencedSchema: "app", ReferencedTable: "tenant", ReferencedColumns: []string{"id"}}},
	}
	target := &domain.TableMetadata{
		Indexes: []domain.IndexInfo{{Name: "idx_name_tenant", Columns: []string{"name", "tenant_id"}}},
	}
	mapping := domain.TableMapping{Columns: []domain.ColumnMapping{{SourceColumn: "email", TargetColumn: "email_addr"}}}
	assessExistingIndexesAndForeignKeys(add, source, target, mapping, "app.users", "public.users")
	codes := map[string]bool{}
	for _, f := range findings {
		codes[f.Code] = true
	}
	if !codes["INDEX_MISSING"] {
		t.Fatalf("expected missing index finding: %+v", findings)
	}
	if !codes["FOREIGN_KEY_MISSING"] {
		t.Fatalf("expected missing foreign key finding: %+v", findings)
	}
}

func TestDeferredIndexAssessmentMapsColumns(t *testing.T) {
	var findings []domain.CompatibilityFinding
	add := func(level domain.CompatibilityLevel, objectType, sourceObject, targetObject, code, message string) {
		findings = append(findings, domain.CompatibilityFinding{Level: level, ObjectType: objectType, SourceObject: sourceObject, TargetObject: targetObject, Code: code, Message: message})
	}
	meta := &domain.TableMetadata{Indexes: []domain.IndexInfo{{Name: "uk_email", Columns: []string{"email"}, Unique: true}}}
	assessDeferredIndexesAndForeignKeys(add, meta, domain.TableMapping{Columns: []domain.ColumnMapping{{SourceColumn: "email", TargetColumn: "email_addr"}}}, "app.users", "public.users")
	if len(findings) != 1 || findings[0].Code != "INDEX_DEFERRED" || !strings.Contains(findings[0].Message, "email_addr") {
		t.Fatalf("unexpected findings: %+v", findings)
	}
}

func TestPauseResumeReturnsToFullMigration(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	reg := connector.NewRegistry()
	svc := NewService(repo, reg)
	now := time.Now()
	task := domain.MigrationTask{ID: "resume-full", Mode: domain.ModeFull, Status: domain.StatusFullMigrating, TotalChunks: 10, FinishedChunks: 4, CreatedAt: now, UpdatedAt: now}
	if err := repo.CreateMigration(ctx, &task); err != nil {
		t.Fatal(err)
	}
	if err := svc.Pause(ctx, task.ID); err != nil {
		t.Fatal(err)
	}
	paused, _ := repo.GetMigration(ctx, task.ID)
	if paused.Status != domain.StatusPaused || paused.PausedFromStatus != domain.StatusFullMigrating {
		t.Fatalf("unexpected paused task: %+v", paused)
	}
	if err := svc.Start(ctx, task.ID); err != nil {
		t.Fatal(err)
	}
	resumed, _ := repo.GetMigration(ctx, task.ID)
	if resumed.Status != domain.StatusFullMigrating || resumed.PausedFromStatus != "" {
		t.Fatalf("unexpected resumed task: %+v", resumed)
	}
}

func TestPauseResumeReturnsToCDCCatchingUp(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	reg := connector.NewRegistry()
	er := engine.NewRegistry()
	er.Register(engine.NewUnified())
	svc := NewService(repo, reg, er)
	now := time.Now()
	task := domain.MigrationTask{ID: "resume-cdc", Mode: domain.ModeIncremental, Status: domain.StatusCDCCatchingUp, CDCEngine: "qmigration", CreatedAt: now, UpdatedAt: now}
	if err := repo.CreateMigration(ctx, &task); err != nil {
		t.Fatal(err)
	}
	if err := svc.Pause(ctx, task.ID); err != nil {
		t.Fatal(err)
	}
	paused, _ := repo.GetMigration(ctx, task.ID)
	if paused.PausedFromStatus != domain.StatusCDCCatchingUp {
		t.Fatalf("paused from=%s", paused.PausedFromStatus)
	}
	if err := svc.Start(ctx, task.ID); err != nil {
		t.Fatal(err)
	}
	resumed, _ := repo.GetMigration(ctx, task.ID)
	if resumed.Status != domain.StatusCDCCatchingUp {
		t.Fatalf("status=%s", resumed.Status)
	}
	jobs, err := repo.ListEngineJobs(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].Status != domain.EngineJobPending {
		t.Fatalf("unexpected jobs: %+v", jobs)
	}
}

func TestAdaptiveChunkSplitsSlowPendingRanges(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	svc := NewService(repo, connector.NewRegistry())
	now := time.Now()
	task := domain.MigrationTask{ID: "adapt-task", Mode: domain.ModeFull, Status: domain.StatusFullMigrating, FullEngine: "native", MaxRetries: 3, CreatedAt: now, UpdatedAt: now}
	if err := repo.CreateMigration(ctx, &task); err != nil {
		t.Fatal(err)
	}
	table := domain.MigrationTable{ID: "adapt-table", TaskID: task.ID, Engine: "native", SourceSchema: "app", SourceTable: "orders", TargetSchema: "app", TargetTable: "orders", PrimaryKey: "id", TotalChunks: 2, Status: "RUNNING"}
	if err := repo.CreateMigrationTable(ctx, &table); err != nil {
		t.Fatal(err)
	}
	chunks := []domain.MigrationChunk{
		{ID: "adapt-done", TaskID: task.ID, TableID: table.ID, ChunkNo: 1, SplitType: "PK_RANGE", PrimaryKey: "id", Start: 1, End: 100, Status: domain.ChunkSuccess},
		{ID: "adapt-pending", TaskID: task.ID, TableID: table.ID, ChunkNo: 2, SplitType: "PK_RANGE", PrimaryKey: "id", Start: 101, End: 300, Status: domain.ChunkPending},
	}
	if err := repo.CreateChunks(ctx, chunks); err != nil {
		t.Fatal(err)
	}
	if err := svc.adaptPendingChunks(ctx, &chunks[0], domain.ChunkResult{DurationMS: 61000, RowsWritten: 100}); err != nil {
		t.Fatal(err)
	}
	got, err := repo.ListChunks(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 chunks after split, got %+v", got)
	}
	var ranges [][2]int64
	for _, ch := range got {
		if ch.ID == "adapt-done" {
			continue
		}
		ranges = append(ranges, [2]int64{ch.Start, ch.End})
	}
	if !((ranges[0] == [2]int64{101, 200} && ranges[1] == [2]int64{201, 300}) || (ranges[1] == [2]int64{101, 200} && ranges[0] == [2]int64{201, 300})) {
		t.Fatalf("unexpected adaptive ranges: %+v", ranges)
	}
}

func TestApplyCDCEventsCompositePrimaryKey(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	state := &cdcApplyState{}
	reg := connector.NewRegistry()
	f := cdcApplyFactory{state: state}
	reg.Register(domain.DataSourcePostgreSQL, f)
	reg.Register(domain.DataSourcePolarDBPostgreSQL, f)
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "cp-src", Type: domain.DataSourcePostgreSQL, Database: "db", Schema: "public", CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "cp-dst", Type: domain.DataSourcePolarDBPostgreSQL, Database: "db2", Schema: "public", CreatedAt: now})
	task := domain.MigrationTask{ID: "cp-task", SourceID: "cp-src", TargetID: "cp-dst", Mode: domain.ModeIncremental, Status: domain.StatusCDCCatchingUp, CreatedAt: now, UpdatedAt: now}
	if err := repo.CreateMigration(ctx, &task); err != nil {
		t.Fatal(err)
	}
	tbl := domain.MigrationTable{
		ID: "cp-table", TaskID: task.ID, SourceSchema: "public", SourceTable: "line_items", TargetSchema: "public", TargetTable: "line_items_v2",
		PrimaryKeys: []string{"order_id", "line_no"}, TargetPrimaryKeys: []string{"order_id", "line_no"},
		Columns:       []domain.ColumnInfo{{Name: "order_id", DataType: "bigint", PrimaryKey: true}, {Name: "line_no", DataType: "integer", PrimaryKey: true}, {Name: "sku", DataType: "text"}},
		TargetColumns: []domain.ColumnInfo{{Name: "order_id", DataType: "bigint", PrimaryKey: true}, {Name: "line_no", DataType: "integer", PrimaryKey: true}, {Name: "sku", DataType: "text"}},
	}
	if err := repo.CreateMigrationTable(ctx, &tbl); err != nil {
		t.Fatal(err)
	}
	svc := NewService(repo, reg)
	res, err := svc.ApplyCDCEvents(ctx, task.ID, domain.CDCApplyRequest{Events: []domain.CDCEvent{
		{ID: "c1", Operation: domain.CDCInsert, SourceSchema: "public", SourceTable: "line_items", After: []domain.CDCField{{Column: "order_id", Value: "42"}, {Column: "line_no", Value: "3"}, {Column: "sku", Value: "ABC"}}, PositionType: "LSN", PositionValue: "0/100"},
		{ID: "c2", Operation: domain.CDCDelete, SourceSchema: "public", SourceTable: "line_items", Before: []domain.CDCField{{Column: "order_id", Value: "42"}, {Column: "line_no", Value: "3"}}, PositionType: "LSN", PositionValue: "0/120"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Applied != 2 || state.writes != 1 || state.deletes != 1 {
		t.Fatalf("unexpected result=%+v state=%+v", res, state)
	}
	if len(state.lastPKs) != 2 || state.lastPKs[0] != "42" || state.lastPKs[1] != "3" {
		t.Fatalf("composite delete keys not preserved: %+v", state.lastPKs)
	}
	if state.begins != 1 || state.commits != 1 || state.rollbacks != 0 {
		t.Fatalf("transaction not committed atomically: %+v", state)
	}
}

func TestApplyCDCEventsLastWriteWinsKeepsNewerTargetAndAdvancesCheckpoint(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	state := &cdcApplyState{lookupOK: true, lookupRow: []connector.Value{{Raw: []byte("20")}}}
	reg := connector.NewRegistry()
	f := cdcApplyFactory{state: state}
	reg.Register(domain.DataSourceMySQL, f)
	reg.Register(domain.DataSourcePolarDBX, f)
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "lww-src", Type: domain.DataSourceMySQL, Database: "app", CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "lww-dst", Type: domain.DataSourcePolarDBX, Database: "app", CreatedAt: now})
	task := domain.MigrationTask{ID: "lww-task", SourceID: "lww-src", TargetID: "lww-dst", Mode: domain.ModeIncremental, Status: domain.StatusCDCCatchingUp, CDCConflictMode: "LAST_WRITE_WINS", CDCConflictColumn: "version", CreatedAt: now, UpdatedAt: now}
	if err := repo.CreateMigration(ctx, &task); err != nil {
		t.Fatal(err)
	}
	cols := []domain.ColumnInfo{{Name: "id", DataType: "bigint", PrimaryKey: true}, {Name: "version", DataType: "bigint"}, {Name: "name", DataType: "varchar"}}
	if err := repo.CreateMigrationTable(ctx, &domain.MigrationTable{ID: "lww-table", TaskID: task.ID, SourceSchema: "app", SourceTable: "orders", TargetSchema: "app", TargetTable: "orders", PrimaryKey: "id", PrimaryKeys: []string{"id"}, TargetPrimaryKey: "id", TargetPrimaryKeys: []string{"id"}, Columns: cols, TargetColumns: cols}); err != nil {
		t.Fatal(err)
	}
	svc := NewService(repo, reg)
	res, err := svc.ApplyCDCEvents(ctx, task.ID, domain.CDCApplyRequest{Events: []domain.CDCEvent{{ID: "lww-e1", Operation: domain.CDCUpdate, SourceSchema: "app", SourceTable: "orders", Before: []domain.CDCField{{Column: "id", Value: "1"}, {Column: "version", Value: "9"}, {Column: "name", Value: "old"}}, After: []domain.CDCField{{Column: "id", Value: "1"}, {Column: "version", Value: "10"}, {Column: "name", Value: "source"}}, PositionType: "GTID", PositionValue: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-20"}}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Applied != 0 || res.SkippedConflicts != 1 || state.writes != 0 || state.deletes != 0 || state.lookups != 1 || state.commits != 1 {
		t.Fatalf("unexpected LWW target-kept result=%+v state=%+v", res, state)
	}
	positions, _ := repo.ListCDCPositions(ctx, task.ID, 10)
	if len(positions) == 0 || positions[0].PositionValue != "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-20" || positions[0].EventsTotal != 1 {
		t.Fatalf("skipped conflict did not advance source checkpoint: %+v", positions)
	}
	conflicts, err := svc.CDCConflicts(ctx, task.ID)
	if err != nil || len(conflicts) != 1 || conflicts[0].Decision != domain.CDCConflictTargetKept || conflicts[0].SourceVersion != "10" || conflicts[0].TargetVersion != "20" {
		t.Fatalf("unexpected conflict audit: err=%v conflicts=%+v", err, conflicts)
	}
}

func TestApplyCDCEventsLastWriteWinsAppliesNewerSource(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	state := &cdcApplyState{lookupOK: true, lookupRow: []connector.Value{{Raw: []byte("10")}}}
	reg := connector.NewRegistry()
	f := cdcApplyFactory{state: state}
	reg.Register(domain.DataSourceMySQL, f)
	reg.Register(domain.DataSourcePolarDBX, f)
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "lww2-src", Type: domain.DataSourceMySQL, Database: "app", CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "lww2-dst", Type: domain.DataSourcePolarDBX, Database: "app", CreatedAt: now})
	task := domain.MigrationTask{ID: "lww2-task", SourceID: "lww2-src", TargetID: "lww2-dst", Mode: domain.ModeIncremental, Status: domain.StatusCDCCatchingUp, CDCConflictMode: "LAST_WRITE_WINS", CDCConflictColumn: "version", CreatedAt: now, UpdatedAt: now}
	_ = repo.CreateMigration(ctx, &task)
	cols := []domain.ColumnInfo{{Name: "id", DataType: "bigint", PrimaryKey: true}, {Name: "version", DataType: "bigint"}, {Name: "name", DataType: "varchar"}}
	_ = repo.CreateMigrationTable(ctx, &domain.MigrationTable{ID: "lww2-table", TaskID: task.ID, SourceSchema: "app", SourceTable: "orders", TargetSchema: "app", TargetTable: "orders", PrimaryKey: "id", PrimaryKeys: []string{"id"}, TargetPrimaryKey: "id", TargetPrimaryKeys: []string{"id"}, Columns: cols, TargetColumns: cols})
	svc := NewService(repo, reg)
	res, err := svc.ApplyCDCEvents(ctx, task.ID, domain.CDCApplyRequest{Events: []domain.CDCEvent{{ID: "lww2-e1", Operation: domain.CDCUpdate, SourceSchema: "app", SourceTable: "orders", Before: []domain.CDCField{{Column: "id", Value: "1"}, {Column: "version", Value: "10"}, {Column: "name", Value: "old"}}, After: []domain.CDCField{{Column: "id", Value: "1"}, {Column: "version", Value: "30"}, {Column: "name", Value: "new"}}, PositionType: "GTID", PositionValue: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-30"}}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Applied != 1 || res.SkippedConflicts != 0 || state.writes != 1 || state.commits != 1 {
		t.Fatalf("unexpected LWW source-applied result=%+v state=%+v", res, state)
	}
	conflicts, _ := svc.CDCConflicts(ctx, task.ID)
	if len(conflicts) != 1 || conflicts[0].Decision != domain.CDCConflictSourceApplied || conflicts[0].SourceVersion != "30" || conflicts[0].TargetVersion != "10" {
		t.Fatalf("unexpected source-applied conflict audit: %+v", conflicts)
	}
}

func TestApplyCDCEventsUpdatePrimaryKeyDeletesOldKey(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	state := &cdcApplyState{}
	reg := connector.NewRegistry()
	f := cdcApplyFactory{state: state}
	reg.Register(domain.DataSourceMySQL, f)
	reg.Register(domain.DataSourcePolarDBX, f)
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "pkmove-src", Type: domain.DataSourceMySQL, Database: "app", CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "pkmove-dst", Type: domain.DataSourcePolarDBX, Database: "app", CreatedAt: now})
	task := domain.MigrationTask{ID: "pkmove-task", SourceID: "pkmove-src", TargetID: "pkmove-dst", Mode: domain.ModeIncremental, Status: domain.StatusCDCCatchingUp, CreatedAt: now, UpdatedAt: now}
	_ = repo.CreateMigration(ctx, &task)
	cols := []domain.ColumnInfo{{Name: "id", DataType: "bigint", PrimaryKey: true}, {Name: "name", DataType: "varchar"}}
	_ = repo.CreateMigrationTable(ctx, &domain.MigrationTable{ID: "pkmove-table", TaskID: task.ID, SourceSchema: "app", SourceTable: "orders", TargetSchema: "app", TargetTable: "orders", PrimaryKey: "id", PrimaryKeys: []string{"id"}, TargetPrimaryKey: "id", TargetPrimaryKeys: []string{"id"}, Columns: cols, TargetColumns: cols})
	svc := NewService(repo, reg)
	res, err := svc.ApplyCDCEvents(ctx, task.ID, domain.CDCApplyRequest{Events: []domain.CDCEvent{{ID: "pkmove-e1", Operation: domain.CDCUpdate, SourceSchema: "app", SourceTable: "orders", Before: []domain.CDCField{{Column: "id", Value: "1"}, {Column: "name", Value: "before"}}, After: []domain.CDCField{{Column: "id", Value: "2"}, {Column: "name", Value: "after"}}, PositionType: "BINLOG", PositionValue: "mysql-bin.000001:500"}}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Applied != 1 || state.deletes != 1 || state.lastPK != "1" || state.writes != 1 || len(state.lastWrite) < 1 || state.lastWrite[0] != "2" || state.commits != 1 {
		t.Fatalf("primary-key move was not atomic: result=%+v state=%+v", res, state)
	}
}

func TestResumeRestoresPausedCDCState(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	reg := connector.NewRegistry()
	f := fakeFactory{}
	reg.Register(domain.DataSourcePostgreSQL, f)
	reg.Register(domain.DataSourcePolarDBPostgreSQL, f)
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "resume-src", Type: domain.DataSourcePostgreSQL, Database: "db", Schema: "public", CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "resume-dst", Type: domain.DataSourcePolarDBPostgreSQL, Database: "db", Schema: "public", CreatedAt: now})
	task := domain.MigrationTask{ID: "resume-cdc", SourceID: "resume-src", TargetID: "resume-dst", Mode: domain.ModeFullAndIncremental, Status: domain.StatusPaused, PausedFromStatus: domain.StatusCDCCatchingUp, CDCEngine: "external", CreatedAt: now, UpdatedAt: now}
	if err := repo.CreateMigration(ctx, &task); err != nil {
		t.Fatal(err)
	}
	svc := NewService(repo, reg)
	if err := svc.Resume(ctx, task.ID); err != nil {
		t.Fatal(err)
	}
	got, _ := svc.Get(ctx, task.ID)
	if got.Status != domain.StatusCDCCatchingUp {
		t.Fatalf("expected CDC_CATCHING_UP, got %s", got.Status)
	}
	if got.PausedFromStatus != "" {
		t.Fatalf("paused state not cleared: %s", got.PausedFromStatus)
	}
}

type jsonFakeFactory struct{}
type jsonFakeConnector struct{ fakeConnector }

func (jsonFakeFactory) New(domain.DataSource) (connector.Connector, error) {
	return jsonFakeConnector{}, nil
}
func (jsonFakeConnector) GetTableMetadata(_ context.Context, schema, table string) (*domain.TableMetadata, error) {
	if table != "orders" {
		return nil, errors.New("not found")
	}
	return &domain.TableMetadata{
		Schema: schema, Name: table,
		Columns: []domain.ColumnInfo{
			{Name: "id", DataType: "bigint", ColumnType: "bigint", Ordinal: 1, PrimaryKey: true},
			{Name: "payload", DataType: "json", ColumnType: "json", Ordinal: 2},
		},
		PrimaryKeys: []string{"id"}, PrimaryKey: "id", PrimaryKeyType: "bigint", PrimaryKeyNumeric: true,
		MinPK: 1, MaxPK: 10, EstimatedRows: 10, DataLength: 1000, HasRows: true,
	}, nil
}

func TestUnifiedCDCEngineFamilyValidationAndRollbackSelection(t *testing.T) {
	// Legacy explicit native protocol names are still validated during rolling upgrade.
	if err := validateCDCEngineSource("native-mysql-cdc", domain.DataSource{Type: domain.DataSourcePostgreSQL}, "forward"); err == nil {
		t.Fatal("expected native MySQL CDC to reject PostgreSQL source")
	}
	if err := validateCDCEngineSource("native-postgres-cdc", domain.DataSource{Type: domain.DataSourceMySQL}, "forward"); err == nil {
		t.Fatal("expected native PostgreSQL CDC to reject MySQL source")
	}
	if err := validateCDCEngineSource("qmigration", domain.DataSource{Type: domain.DataSourcePostgreSQL}, "forward"); err != nil {
		t.Fatalf("unified engine should auto-select PostgreSQL protocol: %v", err)
	}
	svc := &Service{}
	for _, ds := range []domain.DataSource{{Type: domain.DataSourcePostgreSQL}, {Type: domain.DataSourcePolarDBX}} {
		if got := svc.chooseRollbackCDCEngine(&domain.MigrationTask{CDCEngine: "flink-cdc", RollbackCDCEngine: "seatunnel"}, ds); got != "qmigration" {
			t.Fatalf("rollback must normalize to unified engine, got %q", got)
		}
	}
}

func TestAssessmentReportsNativeMySQLJSONAndRollbackEngine(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	reg := connector.NewRegistry()
	f := jsonFakeFactory{}
	reg.Register(domain.DataSourceMySQL, f)
	reg.Register(domain.DataSourcePostgreSQL, f)
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "json-src", Type: domain.DataSourceMySQL, Database: "app", CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "json-dst", Type: domain.DataSourcePostgreSQL, Database: "app", Schema: "public", CreatedAt: now})
	svc := NewService(repo, reg)
	m := domain.MigrationTask{Name: "json-cdc", SourceID: "json-src", TargetID: "json-dst", Mode: domain.ModeFullAndIncremental, FullEngine: "native", CDCEngine: "native-mysql-cdc", AutoCreateTable: true, Tables: []domain.TableMapping{{SourceSchema: "app", SourceTable: "orders", TargetSchema: "public", TargetTable: "orders"}}}
	if err := svc.Create(ctx, &m); err != nil {
		t.Fatal(err)
	}
	a, err := svc.AssessCompatibility(ctx, m.ID)
	if err != nil {
		t.Fatal(err)
	}
	codes := map[string]bool{}
	for _, finding := range a.Findings {
		codes[finding.Code] = true
	}
	for _, code := range []string{"NATIVE_MYSQL_BINARY_JSON", "NATIVE_MYSQL_DDL_FAIL_SAFE", "ROLLBACK_CDC_ENGINE_SELECTION"} {
		if !codes[code] {
			t.Fatalf("expected assessment code %s; findings=%+v", code, a.Findings)
		}
	}
}

type recordingDDLFactory struct{ c *recordingDDLConnector }
type recordingDDLConnector struct{ executed []string }

func (f recordingDDLFactory) New(domain.DataSource) (connector.Connector, error) { return f.c, nil }
func (c *recordingDDLConnector) TestConnection(context.Context) error            { return nil }
func (c *recordingDDLConnector) GetVersion(context.Context) (string, error)      { return "fake", nil }
func (c *recordingDDLConnector) ListSchemas(context.Context) ([]domain.SchemaInfo, error) {
	return nil, nil
}
func (c *recordingDDLConnector) ListTables(context.Context, string) ([]domain.TableInfo, error) {
	return nil, nil
}
func (c *recordingDDLConnector) GetTableMetadata(context.Context, string, string) (*domain.TableMetadata, error) {
	return nil, errors.New("unused")
}
func (c *recordingDDLConnector) Close() error { return nil }
func (c *recordingDDLConnector) ExecDDL(_ context.Context, schema, ddl string) error {
	c.executed = append(c.executed, schema+"|"+ddl)
	return nil
}

func TestApplySameFamilyDDLAndCheckpoint(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	rec := &recordingDDLConnector{}
	reg := connector.NewRegistry()
	f := recordingDDLFactory{c: rec}
	reg.Register(domain.DataSourceMySQL, f)
	reg.Register(domain.DataSourcePolarDBX, f)
	now := time.Now()
	src := domain.DataSource{ID: "src-ddl", Type: domain.DataSourceMySQL, Database: "app", CreatedAt: now}
	dst := domain.DataSource{ID: "dst-ddl", Type: domain.DataSourcePolarDBX, Database: "app", CreatedAt: now}
	_ = repo.CreateDataSource(ctx, &src)
	_ = repo.CreateDataSource(ctx, &dst)
	task := domain.MigrationTask{ID: "m-ddl", Name: "ddl", SourceID: src.ID, TargetID: dst.ID, Mode: domain.ModeIncremental, Status: domain.StatusCDCCatchingUp, CDCEngine: "native-mysql-cdc", CDCDDLMode: "SAME_FAMILY", CreatedAt: now, UpdatedAt: now}
	if err := repo.CreateMigration(ctx, &task); err != nil {
		t.Fatal(err)
	}
	table := domain.MigrationTable{ID: "t-ddl", TaskID: task.ID, SourceSchema: "app", SourceTable: "orders", TargetSchema: "app", TargetTable: "orders", Columns: []domain.ColumnInfo{{Name: "id", PrimaryKey: true}, {Name: "name"}}, TargetColumns: []domain.ColumnInfo{{Name: "id", PrimaryKey: true}, {Name: "name"}}}
	if err := repo.CreateMigrationTable(ctx, &table); err != nil {
		t.Fatal(err)
	}
	svc := NewService(repo, reg)
	res, err := svc.ApplyCDCEvents(ctx, task.ID, domain.CDCApplyRequest{Events: []domain.CDCEvent{{Operation: domain.CDCDDL, SourceSchema: "app", SQL: "ALTER TABLE orders ADD COLUMN note varchar(32)", PositionType: "GTID", PositionValue: "24bc7850-2c16-11e6-a073-0242ac110002:1-9", SourceTimestampMS: time.Now().UnixMilli()}}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Applied != 1 || len(rec.executed) != 1 || !strings.Contains(rec.executed[0], "ALTER TABLE") {
		t.Fatalf("result=%+v executed=%v", res, rec.executed)
	}
	positions, err := repo.ListCDCPositions(ctx, task.ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(positions) == 0 || positions[0].PositionType != "GTID" {
		t.Fatalf("positions=%+v", positions)
	}
}

func TestApplySameFamilyDDLRejectsRenamedMapping(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	rec := &recordingDDLConnector{}
	reg := connector.NewRegistry()
	f := recordingDDLFactory{c: rec}
	reg.Register(domain.DataSourceMySQL, f)
	reg.Register(domain.DataSourcePolarDBX, f)
	now := time.Now()
	src := domain.DataSource{ID: "src-ddl2", Type: domain.DataSourceMySQL, Database: "app", CreatedAt: now}
	dst := domain.DataSource{ID: "dst-ddl2", Type: domain.DataSourcePolarDBX, Database: "app2", CreatedAt: now}
	_ = repo.CreateDataSource(ctx, &src)
	_ = repo.CreateDataSource(ctx, &dst)
	task := domain.MigrationTask{ID: "m-ddl2", Name: "ddl2", SourceID: src.ID, TargetID: dst.ID, Mode: domain.ModeIncremental, Status: domain.StatusCDCCatchingUp, CDCEngine: "native-mysql-cdc", CDCDDLMode: "SAME_FAMILY", CreatedAt: now, UpdatedAt: now}
	_ = repo.CreateMigration(ctx, &task)
	table := domain.MigrationTable{ID: "t-ddl2", TaskID: task.ID, SourceSchema: "app", SourceTable: "orders", TargetSchema: "app2", TargetTable: "orders", Columns: []domain.ColumnInfo{{Name: "id"}}, TargetColumns: []domain.ColumnInfo{{Name: "id"}}}
	_ = repo.CreateMigrationTable(ctx, &table)
	svc := NewService(repo, reg)
	_, err := svc.ApplyCDCEvents(ctx, task.ID, domain.CDCApplyRequest{Events: []domain.CDCEvent{{Operation: domain.CDCDDL, SourceSchema: "app", SQL: "ALTER TABLE orders ADD COLUMN c int", PositionType: "BINLOG", PositionValue: "mysql-bin.000001:100"}}})
	if err == nil || !strings.Contains(err.Error(), "identity schema/table mapping") {
		t.Fatalf("expected identity mapping rejection, got %v", err)
	}
	if len(rec.executed) != 0 {
		t.Fatalf("DDL should not execute: %v", rec.executed)
	}
}

func TestApplyCheckpointOnlyAdvancesWithoutTargetConnector(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	reg := connector.NewRegistry()
	now := time.Now()
	src := domain.DataSource{ID: "src-cp", Type: domain.DataSourceMySQL, Database: "app", CreatedAt: now}
	dst := domain.DataSource{ID: "dst-cp", Type: domain.DataSourcePolarDBX, Database: "app", CreatedAt: now}
	_ = repo.CreateDataSource(ctx, &src)
	_ = repo.CreateDataSource(ctx, &dst)
	task := domain.MigrationTask{ID: "m-cp", Name: "cp", SourceID: src.ID, TargetID: dst.ID, Mode: domain.ModeIncremental, Status: domain.StatusCDCCatchingUp, CDCEngine: "native-mysql-cdc", CreatedAt: now, UpdatedAt: now}
	_ = repo.CreateMigration(ctx, &task)
	svc := NewService(repo, reg)
	res, err := svc.ApplyCDCEvents(ctx, task.ID, domain.CDCApplyRequest{Events: []domain.CDCEvent{{Operation: domain.CDCCheckpoint, PositionType: "GTID", PositionValue: "24bc7850-2c16-11e6-a073-0242ac110002:1-10", SourceTimestampMS: time.Now().UnixMilli()}}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Applied != 0 || res.PositionType != "GTID" {
		t.Fatalf("bad result %+v", res)
	}
	positions, err := repo.ListCDCPositions(ctx, task.ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(positions) == 0 || positions[0].PositionValue != "24bc7850-2c16-11e6-a073-0242ac110002:1-10" {
		t.Fatalf("positions=%+v", positions)
	}
}

func TestCDCRenderTaskResumesFromLatestDurableCheckpoint(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "resume-src", Type: domain.DataSourceMySQL, Database: "app", CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "resume-dst", Type: domain.DataSourcePolarDBX, Database: "app", CreatedAt: now})
	task := domain.MigrationTask{
		ID: "resume-task", Name: "resume", SourceID: "resume-src", TargetID: "resume-dst",
		Mode: domain.ModeFullAndIncremental, Status: domain.StatusCDCCatchingUp,
		CDCEngine: "native-mysql-cdc", CDCStartPositionType: "BINLOG",
		CDCStartPositionValue: "mysql-bin.000001:4", CDCStartResource: "mysql-bin.000001",
		CreatedAt: now, UpdatedAt: now,
	}
	if err := repo.CreateMigration(ctx, &task); err != nil {
		t.Fatal(err)
	}
	cp := domain.CDCPosition{
		ID: "resume-cp", TaskID: task.ID, Direction: "forward", DatabaseType: "mysql",
		PositionType: "GTID", PositionValue: "24bc7850-2c16-11e6-a073-0242ac110002:1-42",
		Resource: "mysql-bin.000009", SourceTimestampMS: now.UnixMilli(), RecordedAt: now.Add(time.Second),
	}
	if err := repo.CreateCDCPosition(ctx, &cp); err != nil {
		t.Fatal(err)
	}
	svc := NewService(repo, connector.NewRegistry())
	rendered, _, _, _, err := svc.cdcRenderTask(ctx, &task, "forward")
	if err != nil {
		t.Fatal(err)
	}
	if rendered.CDCStartPositionType != "GTID" || rendered.CDCStartPositionValue != cp.PositionValue || rendered.CDCStartResource != cp.Resource {
		t.Fatalf("rendered task did not resume from latest checkpoint: %+v", rendered)
	}
}

type schemaObjectFixture struct {
	objects    []domain.SchemaObject
	ddls       []string
	seqValue   string
	seqCalled  bool
	setValue   string
	setCalled  bool
	boundSeq   string
	boundTable string
	boundCol   string
}

type schemaObjectFactory struct {
	fixtures map[string]*schemaObjectFixture
}

func (f schemaObjectFactory) New(ds domain.DataSource) (connector.Connector, error) {
	fixture := f.fixtures[ds.ID]
	if fixture == nil {
		fixture = &schemaObjectFixture{}
	}
	return &schemaObjectConnector{fakeConnector: fakeConnector{}, fixture: fixture}, nil
}

type schemaObjectConnector struct {
	fakeConnector
	fixture *schemaObjectFixture
}

func (c *schemaObjectConnector) ListSchemas(context.Context) ([]domain.SchemaInfo, error) {
	return []domain.SchemaInfo{{Name: "app"}, {Name: "public"}}, nil
}
func (c *schemaObjectConnector) ListSchemaObjects(context.Context, string) ([]domain.SchemaObject, error) {
	return append([]domain.SchemaObject(nil), c.fixture.objects...), nil
}
func (c *schemaObjectConnector) ExecDDL(_ context.Context, _ string, ddl string) error {
	c.fixture.ddls = append(c.fixture.ddls, ddl)
	return nil
}
func (c *schemaObjectConnector) GetSequenceState(context.Context, string, string) (string, bool, error) {
	return c.fixture.seqValue, c.fixture.seqCalled, nil
}
func (c *schemaObjectConnector) SetSequenceState(_ context.Context, _, _ string, value string, called bool) error {
	c.fixture.setValue, c.fixture.setCalled = value, called
	return nil
}
func (c *schemaObjectConnector) BindSequence(_ context.Context, _, sequence, table, column string) error {
	c.fixture.boundSeq, c.fixture.boundTable, c.fixture.boundCol = sequence, table, column
	return nil
}

func TestSchemaObjectPlanAndApplySafeView(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	sourceFixture := &schemaObjectFixture{objects: []domain.SchemaObject{{Schema: "app", Name: "v_orders", Type: domain.SchemaObjectView, DDL: "CREATE OR REPLACE VIEW `app`.`v_orders` AS SELECT 1", DependenciesKnown: true}}}
	targetFixture := &schemaObjectFixture{}
	factory := schemaObjectFactory{fixtures: map[string]*schemaObjectFixture{"obj-src": sourceFixture, "obj-dst": targetFixture}}
	reg := connector.NewRegistry()
	reg.Register(domain.DataSourceMySQL, factory)
	reg.Register(domain.DataSourcePolarDBX, factory)
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "obj-src", Type: domain.DataSourceMySQL, Database: "app", CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "obj-dst", Type: domain.DataSourcePolarDBX, Database: "app", CreatedAt: now})
	svc := NewService(repo, reg)
	m := domain.MigrationTask{Name: "objects", SourceID: "obj-src", TargetID: "obj-dst", Mode: domain.ModeFull, FullEngine: "native"}
	if err := svc.Create(ctx, &m); err != nil {
		t.Fatal(err)
	}
	m.Status = domain.StatusFullFinished
	if err := repo.UpdateMigration(ctx, &m); err != nil {
		t.Fatal(err)
	}
	plan, err := svc.PlanSchemaObjects(ctx, m.ID)
	if err != nil {
		t.Fatal(err)
	}
	if plan.SafeActions != 1 || len(plan.Items) != 1 || plan.Items[0].Action != domain.SchemaObjectApplySafe {
		t.Fatalf("unexpected schema object plan: %+v", plan)
	}
	result, err := svc.ApplySchemaObjects(ctx, m.ID, domain.SchemaObjectApplyRequest{Confirm: true})
	if err != nil {
		t.Fatal(err)
	}
	if result.Applied != 1 || result.Failed != 0 || len(targetFixture.ddls) != 1 {
		t.Fatalf("unexpected apply result=%+v target=%+v", result, targetFixture)
	}
}

func TestSchemaObjectSequenceStateSynchronization(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	sourceFixture := &schemaObjectFixture{objects: []domain.SchemaObject{{Schema: "public", Name: "orders_id_seq", Type: domain.SchemaObjectSequence, BindingKnown: true, DDL: `CREATE SEQUENCE "public"."orders_id_seq" START WITH 1`}}, seqValue: "42", seqCalled: true}
	targetFixture := &schemaObjectFixture{}
	factory := schemaObjectFactory{fixtures: map[string]*schemaObjectFixture{"seq-src": sourceFixture, "seq-dst": targetFixture}}
	reg := connector.NewRegistry()
	reg.Register(domain.DataSourcePostgreSQL, factory)
	reg.Register(domain.DataSourcePolarDBPostgreSQL, factory)
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "seq-src", Type: domain.DataSourcePostgreSQL, Database: "app", Schema: "public", CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "seq-dst", Type: domain.DataSourcePolarDBPostgreSQL, Database: "app", Schema: "public", CreatedAt: now})
	svc := NewService(repo, reg)
	m := domain.MigrationTask{Name: "sequences", SourceID: "seq-src", TargetID: "seq-dst", Mode: domain.ModeFull, FullEngine: "native"}
	if err := svc.Create(ctx, &m); err != nil {
		t.Fatal(err)
	}
	m.Status = domain.StatusFullFinished
	if err := repo.UpdateMigration(ctx, &m); err != nil {
		t.Fatal(err)
	}
	plan, err := svc.PlanSchemaObjects(ctx, m.ID)
	if err != nil {
		t.Fatal(err)
	}
	if plan.SafeActions != 1 || plan.Items[0].Action != domain.SchemaObjectSyncSequence {
		t.Fatalf("unexpected sequence plan: %+v", plan)
	}
	result, err := svc.ApplySchemaObjects(ctx, m.ID, domain.SchemaObjectApplyRequest{Confirm: true, Types: []domain.SchemaObjectType{domain.SchemaObjectSequence}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Applied != 1 || targetFixture.setValue != "42" || !targetFixture.setCalled || len(targetFixture.ddls) != 1 {
		t.Fatalf("sequence was not synchronized: result=%+v target=%+v", result, targetFixture)
	}
}

func TestSchemaObjectApplyRequiresConfirmation(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	factory := schemaObjectFactory{fixtures: map[string]*schemaObjectFixture{"confirm-src": {}, "confirm-dst": {}}}
	reg := connector.NewRegistry()
	reg.Register(domain.DataSourceMySQL, factory)
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "confirm-src", Type: domain.DataSourceMySQL, Database: "app", CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "confirm-dst", Type: domain.DataSourceMySQL, Database: "app", CreatedAt: now})
	svc := NewService(repo, reg)
	m := domain.MigrationTask{Name: "confirm", SourceID: "confirm-src", TargetID: "confirm-dst", Mode: domain.ModeFull}
	if err := svc.Create(ctx, &m); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.ApplySchemaObjects(ctx, m.ID, domain.SchemaObjectApplyRequest{}); err == nil {
		t.Fatal("expected confirm=true requirement")
	}
}

func TestReadyForCutoverRequiresFreshPostgresSequenceSync(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	sourceFixture := &schemaObjectFixture{objects: []domain.SchemaObject{{Schema: "public", Name: "orders_id_seq", Type: domain.SchemaObjectSequence, BindingKnown: true, DDL: `CREATE SEQUENCE "public"."orders_id_seq" START WITH 1`}}, seqValue: "100", seqCalled: true}
	targetFixture := &schemaObjectFixture{}
	factory := schemaObjectFactory{fixtures: map[string]*schemaObjectFixture{"cut-src": sourceFixture, "cut-dst": targetFixture}}
	reg := connector.NewRegistry()
	reg.Register(domain.DataSourcePostgreSQL, factory)
	reg.Register(domain.DataSourcePolarDBPostgreSQL, factory)
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "cut-src", Type: domain.DataSourcePostgreSQL, Database: "app", Schema: "public", CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "cut-dst", Type: domain.DataSourcePolarDBPostgreSQL, Database: "app", Schema: "public", CreatedAt: now})
	svc := NewService(repo, reg)
	m := domain.MigrationTask{Name: "cutover-sequence", SourceID: "cut-src", TargetID: "cut-dst", Mode: domain.ModeFullAndIncremental, FullEngine: "native", CDCEngine: "external", ValidationEnabled: false}
	if err := svc.Create(ctx, &m); err != nil {
		t.Fatal(err)
	}
	m.Status = domain.StatusCDCCatchingUp
	m.CDCLagMS = 0
	if err := repo.UpdateMigration(ctx, &m); err != nil {
		t.Fatal(err)
	}
	if err := repo.CreateCDCPosition(ctx, &domain.CDCPosition{ID: "cut-pos", TaskID: m.ID, Direction: "forward", PositionType: "LSN", PositionValue: "0/100", LagMS: 0, EventsPending: 0, RecordedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := svc.ReadyForCutover(ctx, m.ID, 5000); err == nil || !strings.Contains(err.Error(), "sequences have not been synchronized") {
		t.Fatalf("expected unsynchronized sequence to block cutover, got %v", err)
	}
	if _, err := svc.ApplySchemaObjects(ctx, m.ID, domain.SchemaObjectApplyRequest{Confirm: true, Types: []domain.SchemaObjectType{domain.SchemaObjectSequence}}); err != nil {
		t.Fatal(err)
	}
	updated, err := svc.Get(ctx, m.ID)
	if err != nil {
		t.Fatal(err)
	}
	if updated.SequenceSyncedAt.IsZero() {
		t.Fatal("sequence sync timestamp was not persisted")
	}
	if err := svc.ReadyForCutover(ctx, m.ID, 5000); err != nil {
		t.Fatalf("fresh sequence sync should allow cutover readiness: %v", err)
	}
}

func TestReadyForCutoverRejectsStaleSequenceSync(t *testing.T) {
	t.Setenv("QMIGRATION_SEQUENCE_SYNC_MAX_AGE_SECONDS", "1")
	ctx := context.Background()
	repo := memory.New()
	fixture := schemaObjectFactory{fixtures: map[string]*schemaObjectFixture{
		"stale-src": {objects: []domain.SchemaObject{{Schema: "public", Name: "s", Type: domain.SchemaObjectSequence, BindingKnown: true, DDL: `CREATE SEQUENCE "public"."s"`}}, seqValue: "1"},
		"stale-dst": {},
	}}
	reg := connector.NewRegistry()
	reg.Register(domain.DataSourcePostgreSQL, fixture)
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "stale-src", Type: domain.DataSourcePostgreSQL, Database: "app", Schema: "public", CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "stale-dst", Type: domain.DataSourcePostgreSQL, Database: "app", Schema: "public", CreatedAt: now})
	svc := NewService(repo, reg)
	m := domain.MigrationTask{Name: "stale", SourceID: "stale-src", TargetID: "stale-dst", Mode: domain.ModeFullAndIncremental, CDCEngine: "external", ValidationEnabled: false}
	if err := svc.Create(ctx, &m); err != nil {
		t.Fatal(err)
	}
	m.Status = domain.StatusCDCCatchingUp
	m.SequenceSyncedAt = time.Now().Add(-2 * time.Second)
	if err := repo.UpdateMigration(ctx, &m); err != nil {
		t.Fatal(err)
	}
	_ = repo.CreateCDCPosition(ctx, &domain.CDCPosition{ID: "stale-pos", TaskID: m.ID, Direction: "forward", PositionType: "LSN", PositionValue: "0/200", RecordedAt: now})
	if err := svc.ReadyForCutover(ctx, m.ID, 5000); err == nil || !strings.Contains(err.Error(), "synchronization is stale") {
		t.Fatalf("expected stale sequence sync rejection, got %v", err)
	}
}

func TestSchemaObjectViewDependenciesAreTopologicallyOrdered(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	sourceFixture := &schemaObjectFixture{objects: []domain.SchemaObject{
		{Schema: "app", Name: "z_child", Type: domain.SchemaObjectView, DDL: "CREATE VIEW z_child AS SELECT * FROM a_base", Dependencies: []string{"app.a_base"}, DependenciesKnown: true},
		{Schema: "app", Name: "a_base", Type: domain.SchemaObjectView, DDL: "CREATE VIEW a_base AS SELECT 1", DependenciesKnown: true},
	}}
	targetFixture := &schemaObjectFixture{}
	factory := schemaObjectFactory{fixtures: map[string]*schemaObjectFixture{"dep-src": sourceFixture, "dep-dst": targetFixture}}
	reg := connector.NewRegistry()
	reg.Register(domain.DataSourceMySQL, factory)
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "dep-src", Type: domain.DataSourceMySQL, Database: "app", CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "dep-dst", Type: domain.DataSourceMySQL, Database: "app", CreatedAt: now})
	svc := NewService(repo, reg)
	m := domain.MigrationTask{Name: "view-deps", SourceID: "dep-src", TargetID: "dep-dst", Mode: domain.ModeFull}
	if err := svc.Create(ctx, &m); err != nil {
		t.Fatal(err)
	}
	plan, err := svc.PlanSchemaObjects(ctx, m.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Items) != 2 || plan.Items[0].Source.Name != "a_base" || plan.Items[1].Source.Name != "z_child" {
		t.Fatalf("expected dependency order a_base -> z_child, got %+v", plan.Items)
	}
}

func TestSchemaObjectViewDependencyCycleBecomesManual(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	sourceFixture := &schemaObjectFixture{objects: []domain.SchemaObject{
		{Schema: "app", Name: "v1", Type: domain.SchemaObjectView, DDL: "CREATE VIEW v1 AS SELECT * FROM v2", Dependencies: []string{"app.v2"}, DependenciesKnown: true},
		{Schema: "app", Name: "v2", Type: domain.SchemaObjectView, DDL: "CREATE VIEW v2 AS SELECT * FROM v1", Dependencies: []string{"app.v1"}, DependenciesKnown: true},
	}}
	factory := schemaObjectFactory{fixtures: map[string]*schemaObjectFixture{"cycle-src": sourceFixture, "cycle-dst": {}}}
	reg := connector.NewRegistry()
	reg.Register(domain.DataSourceMySQL, factory)
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "cycle-src", Type: domain.DataSourceMySQL, Database: "app", CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "cycle-dst", Type: domain.DataSourceMySQL, Database: "app", CreatedAt: now})
	svc := NewService(repo, reg)
	m := domain.MigrationTask{Name: "view-cycle", SourceID: "cycle-src", TargetID: "cycle-dst", Mode: domain.ModeFull}
	if err := svc.Create(ctx, &m); err != nil {
		t.Fatal(err)
	}
	plan, err := svc.PlanSchemaObjects(ctx, m.ID)
	if err != nil {
		t.Fatal(err)
	}
	if plan.SafeActions != 0 || plan.Manual != 2 {
		t.Fatalf("dependency cycle should be manual: %+v", plan)
	}
	for _, item := range plan.Items {
		if item.Action != domain.SchemaObjectManual || !strings.Contains(item.Reason, "dependency cycle") {
			t.Fatalf("unexpected cycle item: %+v", item)
		}
	}
}

func TestSchemaObjectViewUnknownDependenciesBecomeManual(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	factory := schemaObjectFactory{fixtures: map[string]*schemaObjectFixture{
		"unknown-src": {objects: []domain.SchemaObject{{Schema: "app", Name: "v_orders", Type: domain.SchemaObjectView, DDL: "CREATE VIEW v_orders AS SELECT 1", DependenciesKnown: false}}},
		"unknown-dst": {},
	}}
	reg := connector.NewRegistry()
	reg.Register(domain.DataSourceMySQL, factory)
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "unknown-src", Type: domain.DataSourceMySQL, Database: "app", CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "unknown-dst", Type: domain.DataSourceMySQL, Database: "app", CreatedAt: now})
	svc := NewService(repo, reg)
	m := domain.MigrationTask{Name: "unknown-view-deps", SourceID: "unknown-src", TargetID: "unknown-dst", Mode: domain.ModeFull}
	if err := svc.Create(ctx, &m); err != nil {
		t.Fatal(err)
	}
	plan, err := svc.PlanSchemaObjects(ctx, m.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Items) != 1 || plan.Items[0].Action != domain.SchemaObjectManual || !strings.Contains(plan.Items[0].Reason, "dependency metadata") {
		t.Fatalf("unknown dependency metadata should force manual view creation: %+v", plan)
	}
}

func TestSchemaObjectSerialSequenceRestoresMappedBinding(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	sourceFixture := &schemaObjectFixture{
		objects: []domain.SchemaObject{{
			Schema: "public", Name: "orders_id_seq", Type: domain.SchemaObjectSequence,
			DDL:       `CREATE SEQUENCE "public"."orders_id_seq" START WITH 1`,
			RelatedTo: "orders.id", Definition: "OWNED", BindingKnown: true,
		}},
		seqValue: "84", seqCalled: true,
	}
	targetFixture := &schemaObjectFixture{}
	factory := schemaObjectFactory{fixtures: map[string]*schemaObjectFixture{"serial-src": sourceFixture, "serial-dst": targetFixture}}
	reg := connector.NewRegistry()
	reg.Register(domain.DataSourcePostgreSQL, factory)
	reg.Register(domain.DataSourcePolarDBPostgreSQL, factory)
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "serial-src", Type: domain.DataSourcePostgreSQL, Database: "app", Schema: "public", CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "serial-dst", Type: domain.DataSourcePolarDBPostgreSQL, Database: "app", Schema: "public", CreatedAt: now})
	svc := NewService(repo, reg)
	m := domain.MigrationTask{
		Name: "serial-binding", SourceID: "serial-src", TargetID: "serial-dst", Mode: domain.ModeFull, FullEngine: "native",
		Tables: []domain.TableMapping{{
			SourceSchema: "public", SourceTable: "orders", TargetSchema: "public", TargetTable: "orders_archive",
			Columns: []domain.ColumnMapping{{SourceColumn: "id", TargetColumn: "order_id"}},
		}},
	}
	if err := svc.Create(ctx, &m); err != nil {
		t.Fatal(err)
	}
	m.Status = domain.StatusFullFinished
	if err := repo.UpdateMigration(ctx, &m); err != nil {
		t.Fatal(err)
	}
	plan, err := svc.PlanSchemaObjects(ctx, m.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Items) != 1 || plan.Items[0].Action != domain.SchemaObjectSyncSequence || plan.Items[0].TargetRelatedTo != "orders_archive.order_id" {
		t.Fatalf("unexpected SERIAL sequence plan: %+v", plan)
	}
	result, err := svc.ApplySchemaObjects(ctx, m.ID, domain.SchemaObjectApplyRequest{Confirm: true, Types: []domain.SchemaObjectType{domain.SchemaObjectSequence}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Applied != 1 || targetFixture.boundSeq != "orders_id_seq" || targetFixture.boundTable != "orders_archive" || targetFixture.boundCol != "order_id" {
		t.Fatalf("SERIAL binding was not restored: result=%+v target=%+v", result, targetFixture)
	}
	if targetFixture.setValue != "84" || !targetFixture.setCalled {
		t.Fatalf("sequence state was not synchronized after binding: %+v", targetFixture)
	}
}

func TestSchemaObjectIdentitySequenceWithoutTargetIsManual(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	factory := schemaObjectFactory{fixtures: map[string]*schemaObjectFixture{
		"identity-src": {objects: []domain.SchemaObject{{
			Schema: "public", Name: "orders_id_seq", Type: domain.SchemaObjectSequence,
			DDL:       `CREATE SEQUENCE "public"."orders_id_seq" START WITH 1`,
			RelatedTo: "orders.id", Definition: "IDENTITY", BindingKnown: true,
		}}},
		"identity-dst": {},
	}}
	reg := connector.NewRegistry()
	reg.Register(domain.DataSourcePostgreSQL, factory)
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "identity-src", Type: domain.DataSourcePostgreSQL, Database: "app", Schema: "public", CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "identity-dst", Type: domain.DataSourcePostgreSQL, Database: "app", Schema: "public", CreatedAt: now})
	svc := NewService(repo, reg)
	m := domain.MigrationTask{Name: "identity-manual", SourceID: "identity-src", TargetID: "identity-dst", Mode: domain.ModeFull}
	if err := svc.Create(ctx, &m); err != nil {
		t.Fatal(err)
	}
	plan, err := svc.PlanSchemaObjects(ctx, m.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Items) != 1 || plan.Items[0].Action != domain.SchemaObjectManual || !strings.Contains(plan.Items[0].Reason, "IDENTITY") {
		t.Fatalf("missing target IDENTITY sequence must remain manual: %+v", plan)
	}
}

func TestSchemaObjectSequenceUnknownBindingMetadataIsManual(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	factory := schemaObjectFactory{fixtures: map[string]*schemaObjectFixture{
		"binding-src": {objects: []domain.SchemaObject{{Schema: "public", Name: "s", Type: domain.SchemaObjectSequence, DDL: `CREATE SEQUENCE "public"."s"`}}},
		"binding-dst": {},
	}}
	reg := connector.NewRegistry()
	reg.Register(domain.DataSourcePostgreSQL, factory)
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "binding-src", Type: domain.DataSourcePostgreSQL, Database: "app", Schema: "public", CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "binding-dst", Type: domain.DataSourcePostgreSQL, Database: "app", Schema: "public", CreatedAt: now})
	svc := NewService(repo, reg)
	m := domain.MigrationTask{Name: "binding-unknown", SourceID: "binding-src", TargetID: "binding-dst", Mode: domain.ModeFull}
	if err := svc.Create(ctx, &m); err != nil {
		t.Fatal(err)
	}
	plan, err := svc.PlanSchemaObjects(ctx, m.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Items) != 1 || plan.Items[0].Action != domain.SchemaObjectManual || !strings.Contains(plan.Items[0].Reason, "metadata is unavailable") {
		t.Fatalf("unknown sequence binding metadata must be manual: %+v", plan)
	}
}

func TestSchemaObjectExistingEquivalentViewIsSkipped(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	source := domain.SchemaObject{Schema: "app", Name: "v_orders", Type: domain.SchemaObjectView, Definition: "SELECT id, name FROM orders", DDL: "CREATE VIEW v_orders AS SELECT id, name FROM orders", DependenciesKnown: true}
	target := source
	factory := schemaObjectFactory{fixtures: map[string]*schemaObjectFixture{
		"view-eq-src": {objects: []domain.SchemaObject{source}},
		"view-eq-dst": {objects: []domain.SchemaObject{target}},
	}}
	reg := connector.NewRegistry()
	reg.Register(domain.DataSourceMySQL, factory)
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "view-eq-src", Type: domain.DataSourceMySQL, Database: "app", CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "view-eq-dst", Type: domain.DataSourceMySQL, Database: "app", CreatedAt: now})
	svc := NewService(repo, reg)
	m := domain.MigrationTask{Name: "view-equivalent", SourceID: "view-eq-src", TargetID: "view-eq-dst", Mode: domain.ModeFull}
	if err := svc.Create(ctx, &m); err != nil {
		t.Fatal(err)
	}
	plan, err := svc.PlanSchemaObjects(ctx, m.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Items) != 1 || plan.Items[0].Action != domain.SchemaObjectSkipExisting || plan.Skipped != 1 {
		t.Fatalf("equivalent target view should be skipped safely: %+v", plan)
	}
}

func TestSchemaObjectExistingDifferentViewIsManual(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	factory := schemaObjectFactory{fixtures: map[string]*schemaObjectFixture{
		"view-diff-src": {objects: []domain.SchemaObject{{Schema: "app", Name: "v_orders", Type: domain.SchemaObjectView, Definition: "SELECT id FROM orders", DDL: "CREATE VIEW v_orders AS SELECT id FROM orders", DependenciesKnown: true}}},
		"view-diff-dst": {objects: []domain.SchemaObject{{Schema: "app", Name: "v_orders", Type: domain.SchemaObjectView, Definition: "SELECT id, name FROM orders", DependenciesKnown: true}}},
	}}
	reg := connector.NewRegistry()
	reg.Register(domain.DataSourceMySQL, factory)
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "view-diff-src", Type: domain.DataSourceMySQL, Database: "app", CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "view-diff-dst", Type: domain.DataSourceMySQL, Database: "app", CreatedAt: now})
	svc := NewService(repo, reg)
	m := domain.MigrationTask{Name: "view-different", SourceID: "view-diff-src", TargetID: "view-diff-dst", Mode: domain.ModeFull}
	if err := svc.Create(ctx, &m); err != nil {
		t.Fatal(err)
	}
	plan, err := svc.PlanSchemaObjects(ctx, m.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Items) != 1 || plan.Items[0].Action != domain.SchemaObjectManual || !strings.Contains(plan.Items[0].Reason, "different") {
		t.Fatalf("different target view must require manual review: %+v", plan)
	}
}

func TestSchemaObjectIdentityRequiresIdentityTarget(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	factory := schemaObjectFactory{fixtures: map[string]*schemaObjectFixture{
		"identity-kind-src": {objects: []domain.SchemaObject{{Schema: "public", Name: "orders_id_seq", Type: domain.SchemaObjectSequence, Definition: "IDENTITY", RelatedTo: "orders.id", BindingKnown: true, DDL: `CREATE SEQUENCE "public"."orders_id_seq"`}}},
		"identity-kind-dst": {objects: []domain.SchemaObject{{Schema: "public", Name: "orders_id_seq", Type: domain.SchemaObjectSequence, Definition: "OWNED", RelatedTo: "orders.id", BindingKnown: true}}},
	}}
	reg := connector.NewRegistry()
	reg.Register(domain.DataSourcePostgreSQL, factory)
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "identity-kind-src", Type: domain.DataSourcePostgreSQL, Database: "app", Schema: "public", CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "identity-kind-dst", Type: domain.DataSourcePostgreSQL, Database: "app", Schema: "public", CreatedAt: now})
	svc := NewService(repo, reg)
	m := domain.MigrationTask{Name: "identity-kind", SourceID: "identity-kind-src", TargetID: "identity-kind-dst", Mode: domain.ModeFull}
	if err := svc.Create(ctx, &m); err != nil {
		t.Fatal(err)
	}
	plan, err := svc.PlanSchemaObjects(ctx, m.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Items) != 1 || plan.Items[0].Action != domain.SchemaObjectManual || !strings.Contains(plan.Items[0].Reason, "not identity-backed") {
		t.Fatalf("identity source must require identity target semantics: %+v", plan)
	}
}

func TestStableMigrationKeyFallsBackToUniqueNotNull(t *testing.T) {
	meta := &domain.TableMetadata{
		Columns: []domain.ColumnInfo{
			{Name: "tenant", Nullable: false},
			{Name: "email", Nullable: false},
			{Name: "optional", Nullable: true},
		},
		Indexes: []domain.IndexInfo{
			{Name: "uk_optional", Columns: []string{"optional"}, Unique: true},
			{Name: "uk_tenant_email", Columns: []string{"tenant", "email"}, Unique: true},
		},
	}
	keys, idx := stableMigrationKey(meta)
	if idx == nil || idx.Name != "uk_tenant_email" || len(keys) != 2 || keys[0] != "tenant" || keys[1] != "email" {
		t.Fatalf("unexpected migration key keys=%v idx=%+v", keys, idx)
	}
}

func TestStableMigrationKeyRejectsNullableAndGeneratedUnique(t *testing.T) {
	meta := &domain.TableMetadata{
		Columns: []domain.ColumnInfo{
			{Name: "nullable_key", Nullable: true},
			{Name: "generated_key", Nullable: false, Extra: "STORED GENERATED"},
		},
		Indexes: []domain.IndexInfo{
			{Name: "uk_nullable", Columns: []string{"nullable_key"}, Unique: true},
			{Name: "uk_generated", Columns: []string{"generated_key"}, Unique: true},
		},
	}
	keys, idx := stableMigrationKey(meta)
	if len(keys) != 0 || idx != nil {
		t.Fatalf("unsafe unique key selected keys=%v idx=%+v", keys, idx)
	}
}

type uniqueKeyState struct {
	mu            sync.Mutex
	targetCreated bool
	targetColumns []domain.ColumnInfo
	targetIndexes []domain.IndexInfo
}

type uniqueKeyFactory struct{ state *uniqueKeyState }
type uniqueKeyConnector struct {
	state  *uniqueKeyState
	source bool
}

func (f uniqueKeyFactory) New(ds domain.DataSource) (connector.Connector, error) {
	return &uniqueKeyConnector{state: f.state, source: ds.Host == "source"}, nil
}
func (c *uniqueKeyConnector) TestConnection(context.Context) error       { return nil }
func (c *uniqueKeyConnector) GetVersion(context.Context) (string, error) { return "fake", nil }
func (c *uniqueKeyConnector) ListSchemas(context.Context) ([]domain.SchemaInfo, error) {
	return []domain.SchemaInfo{{Name: "app"}}, nil
}
func (c *uniqueKeyConnector) ListTables(_ context.Context, schema string) ([]domain.TableInfo, error) {
	return []domain.TableInfo{{Schema: schema, Name: "contacts", Rows: 10}}, nil
}
func (c *uniqueKeyConnector) GetTableMetadata(_ context.Context, schema, table string) (*domain.TableMetadata, error) {
	if c.source {
		return &domain.TableMetadata{
			Schema: schema, Name: table, HasRows: true, EstimatedRows: 10,
			Columns: []domain.ColumnInfo{
				{Name: "tenant", DataType: "varchar", ColumnType: "varchar(32)", Nullable: false, Ordinal: 1},
				{Name: "email", DataType: "varchar", ColumnType: "varchar(128)", Nullable: false, Ordinal: 2},
				{Name: "name", DataType: "varchar", ColumnType: "varchar(64)", Nullable: true, Ordinal: 3},
			},
			Indexes: []domain.IndexInfo{{Name: "uk_tenant_email", Columns: []string{"tenant", "email"}, Unique: true}},
		}, nil
	}
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	if !c.state.targetCreated {
		return &domain.TableMetadata{Schema: schema, Name: table}, nil
	}
	return &domain.TableMetadata{Schema: schema, Name: table, Columns: append([]domain.ColumnInfo(nil), c.state.targetColumns...), Indexes: append([]domain.IndexInfo(nil), c.state.targetIndexes...)}, nil
}
func (c *uniqueKeyConnector) Close() error                               { return nil }
func (c *uniqueKeyConnector) EnsureSchema(context.Context, string) error { return nil }
func (c *uniqueKeyConnector) CreateTable(_ context.Context, _, _ string, cols []domain.ColumnInfo, _ string) error {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	c.state.targetCreated = true
	c.state.targetColumns = append([]domain.ColumnInfo(nil), cols...)
	return nil
}
func (c *uniqueKeyConnector) CreateTableWithPrimaryKeys(ctx context.Context, schema, table string, cols []domain.ColumnInfo, _ []string) error {
	return c.CreateTable(ctx, schema, table, cols, "")
}
func (c *uniqueKeyConnector) CreateIndex(_ context.Context, _, _ string, idx domain.IndexInfo) error {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	c.state.targetIndexes = append(c.state.targetIndexes, idx)
	return nil
}
func (c *uniqueKeyConnector) CreateForeignKey(context.Context, string, string, domain.ForeignKeyInfo) error {
	return nil
}

func waitMigrationStatus(t *testing.T, svc *Service, ctx context.Context, id string, want domain.MigrationStatus) *domain.MigrationTask {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		cur, err := svc.Get(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if cur.Status == want || cur.Status == domain.StatusFailed {
			return cur
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s, current=%s", want, cur.Status)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestNativeUsesUniqueNotNullMigrationKeyAndCreatesTargetUnique(t *testing.T) {
	ctx := context.Background()
	state := &uniqueKeyState{}
	repo := memory.New()
	reg := connector.NewRegistry()
	f := uniqueKeyFactory{state: state}
	reg.Register(domain.DataSourceMySQL, f)
	reg.Register(domain.DataSourcePolarDBX, f)
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "uk-s", Type: domain.DataSourceMySQL, Host: "source", Database: "app", CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "uk-t", Type: domain.DataSourcePolarDBX, Host: "target", Database: "app", CreatedAt: now})
	_ = repo.UpsertWorker(ctx, &domain.Worker{ID: "uk-w", Capabilities: []string{"qmigration"}, LastHeartbeat: now})
	svc := NewService(repo, reg)
	m := domain.MigrationTask{Name: "unique-key", SourceID: "uk-s", TargetID: "uk-t", Mode: domain.ModeFull, FullEngine: "native", AutoCreateTable: true, ValidationEnabled: true}
	if err := svc.Create(ctx, &m); err != nil {
		t.Fatal(err)
	}
	if err := svc.Start(ctx, m.ID); err != nil {
		t.Fatal(err)
	}
	cur := waitMigrationStatus(t, svc, ctx, m.ID, domain.StatusFullMigrating)
	if cur.Status == domain.StatusFailed {
		t.Fatal(cur.LastError)
	}
	tables, _ := svc.Tables(ctx, m.ID)
	if len(tables) != 1 || len(tables[0].PrimaryKeys) != 2 || tables[0].PrimaryKeys[0] != "tenant" || tables[0].PrimaryKeys[1] != "email" {
		t.Fatalf("unexpected migration keys: %+v", tables)
	}
	chunks, _ := svc.Chunks(ctx, m.ID)
	if len(chunks) != 1 || chunks[0].SplitType != "PRIMARY_KEY_KEYSET" {
		t.Fatalf("unexpected chunks: %+v", chunks)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if !matchingUniqueIndex(state.targetIndexes, []string{"tenant", "email"}) {
		t.Fatalf("target UNIQUE migration key not created: %+v", state.targetIndexes)
	}
}

func TestNativeRejectsExistingTargetWithoutUniqueMigrationKey(t *testing.T) {
	ctx := context.Background()
	state := &uniqueKeyState{targetCreated: true, targetColumns: []domain.ColumnInfo{
		{Name: "tenant", DataType: "varchar", ColumnType: "varchar(32)", Nullable: false, Ordinal: 1},
		{Name: "email", DataType: "varchar", ColumnType: "varchar(128)", Nullable: false, Ordinal: 2},
		{Name: "name", DataType: "varchar", ColumnType: "varchar(64)", Nullable: true, Ordinal: 3},
	}}
	repo := memory.New()
	reg := connector.NewRegistry()
	f := uniqueKeyFactory{state: state}
	reg.Register(domain.DataSourceMySQL, f)
	reg.Register(domain.DataSourcePolarDBX, f)
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "uke-s", Type: domain.DataSourceMySQL, Host: "source", Database: "app", CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "uke-t", Type: domain.DataSourcePolarDBX, Host: "target", Database: "app", CreatedAt: now})
	svc := NewService(repo, reg)
	m := domain.MigrationTask{Name: "unique-key-existing", SourceID: "uke-s", TargetID: "uke-t", Mode: domain.ModeFull, FullEngine: "native", AutoCreateTable: false, ValidationEnabled: true}
	if err := svc.Create(ctx, &m); err != nil {
		t.Fatal(err)
	}
	if err := svc.Start(ctx, m.ID); err != nil {
		t.Fatal(err)
	}
	cur := waitMigrationStatus(t, svc, ctx, m.ID, domain.StatusFullMigrating)
	if cur.Status != domain.StatusFailed || !strings.Contains(cur.LastError, "must have a UNIQUE index") {
		t.Fatalf("expected missing UNIQUE failure, got status=%s error=%q", cur.Status, cur.LastError)
	}
}

type adaptiveBoundaryFactory struct{ state *adaptiveBoundaryState }
type adaptiveBoundaryState struct {
	lower [][]connector.Value
	upper [][]connector.Value
}
type adaptiveBoundaryConnector struct{ state *adaptiveBoundaryState }

func (f adaptiveBoundaryFactory) New(domain.DataSource) (connector.Connector, error) {
	return adaptiveBoundaryConnector{state: f.state}, nil
}
func (c adaptiveBoundaryConnector) TestConnection(context.Context) error       { return nil }
func (c adaptiveBoundaryConnector) GetVersion(context.Context) (string, error) { return "fake", nil }
func (c adaptiveBoundaryConnector) ListSchemas(context.Context) ([]domain.SchemaInfo, error) {
	return nil, nil
}
func (c adaptiveBoundaryConnector) ListTables(context.Context, string) ([]domain.TableInfo, error) {
	return nil, nil
}
func (c adaptiveBoundaryConnector) GetTableMetadata(context.Context, string, string) (*domain.TableMetadata, error) {
	return nil, connector.ErrMetadataUnavailable
}
func (c adaptiveBoundaryConnector) Close() error { return nil }
func cloneTestValues(in []connector.Value) []connector.Value {
	out := make([]connector.Value, len(in))
	for i := range in {
		out[i] = connector.Value{Null: in[i].Null, Raw: append([]byte(nil), in[i].Raw...)}
	}
	return out
}
func (c adaptiveBoundaryConnector) PlanKeysetBoundaries(_ context.Context, req connector.KeysetBoundaryRequest) ([][]connector.Value, error) {
	c.state.lower = append(c.state.lower, cloneTestValues(req.LowerBound))
	c.state.upper = append(c.state.upper, cloneTestValues(req.UpperBound))
	if req.Partitions != 2 {
		return nil, nil
	}
	return [][]connector.Value{{{Raw: []byte("t")}}}, nil
}

func TestAdaptiveChunkSplitsPendingBoundedKeyset(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	state := &adaptiveBoundaryState{}
	reg := connector.NewRegistry()
	reg.Register(domain.DataSourceMySQL, adaptiveBoundaryFactory{state: state})
	svc := NewService(repo, reg)
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "adapt-key-src", Type: domain.DataSourceMySQL, Database: "app", CreatedAt: now})
	task := domain.MigrationTask{ID: "adapt-key-task", SourceID: "adapt-key-src", Mode: domain.ModeFull, Status: domain.StatusFullMigrating, FullEngine: "native", CreatedAt: now, UpdatedAt: now}
	if err := repo.CreateMigration(ctx, &task); err != nil {
		t.Fatal(err)
	}
	table := domain.MigrationTable{ID: "adapt-key-table", TaskID: task.ID, Engine: "native", SourceSchema: "app", SourceTable: "customers", TargetSchema: "app", TargetTable: "customers", PrimaryKey: "id", PrimaryKeys: []string{"id"}, Columns: []domain.ColumnInfo{{Name: "id", DataType: "varchar", PrimaryKey: true}}, Status: "RUNNING"}
	if err := repo.CreateMigrationTable(ctx, &table); err != nil {
		t.Fatal(err)
	}
	lower, _ := json.Marshal([]connector.Value{{Raw: []byte("m")}})
	upper, _ := json.Marshal([]connector.Value{{Raw: []byte("z")}})
	chunks := []domain.MigrationChunk{
		{ID: "adapt-key-done", TaskID: task.ID, TableID: table.ID, ChunkNo: 1, SplitType: "PRIMARY_KEY_KEYSET", PrimaryKey: "id", EndCursorJSON: string(lower), Status: domain.ChunkSuccess},
		{ID: "adapt-key-pending", TaskID: task.ID, TableID: table.ID, ChunkNo: 2, SplitType: "PRIMARY_KEY_KEYSET", PrimaryKey: "id", StartCursorJSON: string(lower), EndCursorJSON: string(upper), Status: domain.ChunkPending},
	}
	if err := repo.CreateChunks(ctx, chunks); err != nil {
		t.Fatal(err)
	}
	if err := svc.adaptPendingChunks(ctx, &chunks[0], domain.ChunkResult{DurationMS: 61000, RowsWritten: 100}); err != nil {
		t.Fatal(err)
	}
	got, err := repo.ListChunks(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 chunks after keyset refinement, got %+v", got)
	}
	boundary, _ := json.Marshal([]connector.Value{{Raw: []byte("t")}})
	var left, right *domain.MigrationChunk
	for i := range got {
		if got[i].ID == "adapt-key-pending" {
			left = &got[i]
		}
		if got[i].ID != "adapt-key-done" && got[i].ID != "adapt-key-pending" {
			right = &got[i]
		}
	}
	if left == nil || right == nil || left.StartCursorJSON != string(lower) || left.EndCursorJSON != string(boundary) || right.StartCursorJSON != string(boundary) || right.EndCursorJSON != string(upper) {
		t.Fatalf("unexpected refined bounds left=%+v right=%+v", left, right)
	}
	if len(state.lower) != 1 || string(state.lower[0][0].Raw) != "m" || len(state.upper) != 1 || string(state.upper[0][0].Raw) != "z" {
		t.Fatalf("source boundary request did not preserve parent bounds: lower=%+v upper=%+v", state.lower, state.upper)
	}
}

func TestBackpressureControlUsesDatabaseLatency(t *testing.T) {
	ctl := backpressureControl(domain.ChunkProgress{LastWriteMS: 9000, LastBatchRows: 1000}, &domain.Worker{Status: "ONLINE"})
	if ctl.Level != "CRITICAL" || ctl.PauseMS <= 0 || ctl.MaxBatchRows != 500 {
		t.Fatalf("unexpected critical control: %+v", ctl)
	}
	ctl = backpressureControl(domain.ChunkProgress{LastReadMS: 3500, LastBatchRows: 800}, &domain.Worker{Status: "ONLINE"})
	if ctl.Level != "WARN" || ctl.MaxBatchRows != 600 {
		t.Fatalf("unexpected warn control: %+v", ctl)
	}
	ctl = backpressureControl(domain.ChunkProgress{LastReadMS: 100, LastWriteMS: 120, LastBatchRows: 500}, &domain.Worker{Status: "ONLINE"})
	if ctl.Level != "NORMAL" || ctl.PauseMS != 0 || ctl.MaxBatchRows != 0 {
		t.Fatalf("unexpected normal control: %+v", ctl)
	}
}

func TestRenewChunkPersistsBackpressureTelemetry(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	svc := NewService(repo, connector.NewRegistry())
	now := time.Now()
	task := domain.MigrationTask{ID: "bp-task", Mode: domain.ModeFull, Status: domain.StatusFullMigrating, FullEngine: "native", Parallelism: 1, EffectiveParallelism: 1, TargetThroughputMBps: 64, CreatedAt: now, UpdatedAt: now}
	if err := repo.CreateMigration(ctx, &task); err != nil {
		t.Fatal(err)
	}
	if err := repo.CreateMigrationTable(ctx, &domain.MigrationTable{ID: "bp-table", TaskID: task.ID, Engine: "native", SourceSchema: "app", SourceTable: "t", TargetSchema: "app", TargetTable: "t", PrimaryKey: "id"}); err != nil {
		t.Fatal(err)
	}
	if err := repo.CreateChunks(ctx, []domain.MigrationChunk{{ID: "bp-chunk", TaskID: task.ID, TableID: "bp-table", ChunkNo: 1, SplitType: "PRIMARY_KEY_RANGE", PrimaryKey: "id", Start: 1, End: 10, Status: domain.ChunkPending}}); err != nil {
		t.Fatal(err)
	}
	if err := repo.UpsertWorker(ctx, &domain.Worker{ID: "bp-worker", Hostname: "w", Status: "ONLINE", Capabilities: []string{"qmigration"}, LastHeartbeat: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.ClaimChunk(ctx, "bp-worker", time.Minute, []string{"native"}); err != nil {
		t.Fatal(err)
	}
	ctl, err := svc.RenewChunk(ctx, "bp-worker", "bp-chunk", domain.ChunkProgress{RowsRead: 500, RowsWritten: 500, LastWriteMS: 9000, LastBatchRows: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if ctl.Level != "CRITICAL" {
		t.Fatalf("control=%+v", ctl)
	}
	if ctl.TargetBytesPerSec != 64<<20 {
		t.Fatalf("target bytes/sec=%d want %d", ctl.TargetBytesPerSec, int64(64<<20))
	}
	chunk, err := repo.GetChunk(ctx, "bp-chunk")
	if err != nil {
		t.Fatal(err)
	}
	if chunk.LastWriteMS != 9000 || chunk.LastBatchRows != 1000 || chunk.BackpressureLevel != "CRITICAL" {
		t.Fatalf("telemetry not durable: %+v", chunk)
	}
}

type runtimeLoadState struct{ load domain.DatabaseRuntimeLoad }
type runtimeLoadFactory struct{ state *runtimeLoadState }
type runtimeLoadConnector struct{ state *runtimeLoadState }

func (f runtimeLoadFactory) New(domain.DataSource) (connector.Connector, error) {
	return runtimeLoadConnector{state: f.state}, nil
}
func (c runtimeLoadConnector) TestConnection(context.Context) error       { return nil }
func (c runtimeLoadConnector) GetVersion(context.Context) (string, error) { return "fake", nil }
func (c runtimeLoadConnector) ListSchemas(context.Context) ([]domain.SchemaInfo, error) {
	return nil, nil
}
func (c runtimeLoadConnector) ListTables(context.Context, string) ([]domain.TableInfo, error) {
	return nil, nil
}
func (c runtimeLoadConnector) GetTableMetadata(context.Context, string, string) (*domain.TableMetadata, error) {
	return nil, connector.ErrMetadataUnavailable
}
func (c runtimeLoadConnector) Close() error { return nil }
func (c runtimeLoadConnector) SampleRuntimeLoad(context.Context) (domain.DatabaseRuntimeLoad, error) {
	return c.state.load, nil
}

func TestTaskFlowControlShrinksAndRecoversEffectiveParallelism(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	state := &runtimeLoadState{load: domain.DatabaseRuntimeLoad{Connections: 95, MaxConnections: 100, ConnectionUsagePct: 95}}
	reg := connector.NewRegistry()
	reg.Register(domain.DataSourceMySQL, runtimeLoadFactory{state: state})
	svc := NewService(repo, reg)
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "flow-src", Type: domain.DataSourceMySQL, CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "flow-dst", Type: domain.DataSourceMySQL, CreatedAt: now})
	task := domain.MigrationTask{ID: "flow-task", SourceID: "flow-src", TargetID: "flow-dst", Status: domain.StatusFullMigrating, Parallelism: 8, EffectiveParallelism: 8, FlowControlLevel: "NORMAL", CreatedAt: now, UpdatedAt: now}
	if err := repo.CreateMigration(ctx, &task); err != nil {
		t.Fatal(err)
	}
	level, _ := svc.reconcileTaskFlowControl(ctx, &task, "NORMAL", "")
	if level != "CRITICAL" || task.EffectiveParallelism != 4 {
		t.Fatalf("critical flow control=%s task=%+v", level, task)
	}
	stored, _ := repo.GetMigration(ctx, task.ID)
	if stored.EffectiveParallelism != 4 || stored.FlowControlLevel != "CRITICAL" {
		t.Fatalf("flow state not durable: %+v", stored)
	}
	state.load = domain.DatabaseRuntimeLoad{Connections: 10, MaxConnections: 100, ConnectionUsagePct: 10}
	svc.pressure[task.ID] = taskPressureSample{At: time.Now().Add(-time.Minute), Level: "CRITICAL"}
	level, _ = svc.reconcileTaskFlowControl(ctx, &task, "NORMAL", "")
	if level != "NORMAL" || task.EffectiveParallelism != 5 {
		t.Fatalf("expected gradual recovery to 5, got %s %+v", level, task)
	}
}

func TestAssignTopologyHintsRoundRobin(t *testing.T) {
	chunks := []domain.MigrationChunk{{ID: "c1"}, {ID: "c2"}, {ID: "c3"}}
	topology := []domain.TopologyPlacement{
		{ID: "ob-zone:a", Kind: "OCEANBASE_ZONE", Labels: map[string]string{"ob_zone": "a"}},
		{ID: "ob-zone:b", Kind: "OCEANBASE_ZONE", Labels: map[string]string{"ob_zone": "b"}},
	}
	assignTopologyHints(chunks, topology)
	if chunks[0].PlacementHint["ob_zone"] != "a" || chunks[1].PlacementHint["ob_zone"] != "b" || chunks[2].PlacementHint["ob_zone"] != "a" {
		t.Fatalf("unexpected placement hints: %+v", chunks)
	}
	if chunks[1].TopologyKind != "OCEANBASE_ZONE" || chunks[1].TopologyID != "ob-zone:b" {
		t.Fatalf("topology identity missing: %+v", chunks[1])
	}
}

func TestValidationBarrierRequiresQuietCDCWindowAndDetectsDrift(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	svc := NewService(repo, connector.NewRegistry())
	now := time.Now()
	task := domain.MigrationTask{ID: "barrier-task", Mode: domain.ModeFullAndIncremental, Status: domain.StatusCDCCatchingUp, CDCStartPositionType: "GTID", CDCStartPositionValue: "uuid:1-10", CreatedAt: now, UpdatedAt: now}
	if err := repo.CreateMigration(ctx, &task); err != nil {
		t.Fatal(err)
	}
	fresh := &domain.CDCPosition{ID: "fresh", TaskID: task.ID, Direction: "forward", PositionType: "GTID", PositionValue: "uuid:1-11", RecordedAt: time.Now()}
	if err := repo.CreateCDCPosition(ctx, fresh); err != nil {
		t.Fatal(err)
	}
	t.Setenv("QMIGRATION_VALIDATION_STABLE_WINDOW_SECONDS", "60")
	if err := svc.captureValidationBarrier(ctx, &task); err == nil || !strings.Contains(err.Error(), "not stable yet") {
		t.Fatalf("expected quiet-window rejection, got %v", err)
	}
	fresh.RecordedAt = time.Now().Add(-2 * time.Minute)
	// Memory repository is append-only for CDC positions; add an older-stable checkpoint with a later insertion order.
	stable := &domain.CDCPosition{ID: "stable", TaskID: task.ID, Direction: "forward", PositionType: "GTID", PositionValue: "uuid:1-12", RecordedAt: time.Now().Add(-2 * time.Minute)}
	if err := repo.CreateCDCPosition(ctx, stable); err != nil {
		t.Fatal(err)
	}
	if err := svc.captureValidationBarrier(ctx, &task); err != nil {
		t.Fatal(err)
	}
	if task.ValidationBarrierPositionValue != "uuid:1-12" {
		t.Fatalf("wrong barrier: %+v", task)
	}
	newer := &domain.CDCPosition{ID: "newer", TaskID: task.ID, Direction: "forward", PositionType: "GTID", PositionValue: "uuid:1-13", RecordedAt: time.Now().Add(-2 * time.Minute)}
	if validationBarrierMatches(&task, newer) {
		t.Fatal("barrier drift was not detected")
	}
}

func TestDeleteValidationResultsAllowsBarrierRetry(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	now := time.Now()
	if err := repo.CreateValidationResult(ctx, &domain.ValidationResult{ID: "v1", TaskID: "task", TableID: "t", ChunkID: "c", Status: domain.ValidationSuccess, StartedAt: now, FinishedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := repo.DeleteValidationResults(ctx, "task"); err != nil {
		t.Fatal(err)
	}
	items, err := repo.ListValidationResults(ctx, "task")
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 0 {
		t.Fatalf("validation results not cleared for barrier retry: %+v", items)
	}
}

func TestSameNativeDatabaseFamilyIncludesOracleAndSQLServer(t *testing.T) {
	if !sameNativeDatabaseFamily(domain.DataSourceOracle, domain.DataSourceOracle) {
		t.Fatal("Oracle should be treated as a same native family")
	}
	if !sameNativeDatabaseFamily(domain.DataSourceSQLServer, domain.DataSourceSQLServer) {
		t.Fatal("SQL Server should be treated as a same native family")
	}
	if sameNativeDatabaseFamily(domain.DataSourceOracle, domain.DataSourceSQLServer) {
		t.Fatal("Oracle and SQL Server are not the same native family")
	}
}

func TestApplyGaussDBSameFamilyDDL(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	rec := &recordingDDLConnector{}
	reg := connector.NewRegistry()
	f := recordingDDLFactory{c: rec}
	reg.Register(domain.DataSourceGaussDB, f)
	now := time.Now()
	src := domain.DataSource{ID: "src-gauss-ddl", Type: domain.DataSourceGaussDB, Database: "app", Schema: "public", CreatedAt: now}
	dst := domain.DataSource{ID: "dst-gauss-ddl", Type: domain.DataSourceGaussDB, Database: "app", Schema: "public", CreatedAt: now}
	_ = repo.CreateDataSource(ctx, &src)
	_ = repo.CreateDataSource(ctx, &dst)
	task := domain.MigrationTask{ID: "m-gauss-ddl", Name: "gauss-ddl", SourceID: src.ID, TargetID: dst.ID, Mode: domain.ModeIncremental, Status: domain.StatusCDCCatchingUp, CDCEngine: "native-gaussdb-logical-cdc", CDCDDLMode: "SAME_FAMILY", CreatedAt: now, UpdatedAt: now}
	if err := repo.CreateMigration(ctx, &task); err != nil {
		t.Fatal(err)
	}
	table := domain.MigrationTable{ID: "t-gauss-ddl", TaskID: task.ID, SourceSchema: "public", SourceTable: "orders", TargetSchema: "public", TargetTable: "orders", Columns: []domain.ColumnInfo{{Name: "id", PrimaryKey: true}}, TargetColumns: []domain.ColumnInfo{{Name: "id", PrimaryKey: true}}}
	if err := repo.CreateMigrationTable(ctx, &table); err != nil {
		t.Fatal(err)
	}
	svc := NewService(repo, reg)
	res, err := svc.ApplyCDCEvents(ctx, task.ID, domain.CDCApplyRequest{Events: []domain.CDCEvent{{Operation: domain.CDCDDL, SourceSchema: "public", SQL: "ALTER TABLE public.orders ADD COLUMN note varchar(32)", PositionType: "GAUSSDB_LSN", PositionValue: "0/700"}}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Applied != 1 || len(rec.executed) != 1 {
		t.Fatalf("result=%+v executed=%v", res, rec.executed)
	}
}

func TestValidationFreezesTargetApplyButKeepsCaptureActive(t *testing.T) {
	task := &domain.MigrationTask{Status: domain.StatusValidating}
	if cdcApplyReady(task, "forward") {
		t.Fatal("forward CDC target apply must be frozen during VALIDATING")
	}
	if !cdcCaptureReady(task, "forward") {
		t.Fatal("forward CDC capture must remain active during VALIDATING so transactions can spool")
	}
	for _, status := range []domain.MigrationStatus{domain.StatusCDCCatchingUp, domain.StatusReadyCutover} {
		task.Status = status
		if !cdcApplyReady(task, "forward") {
			t.Fatalf("forward CDC apply should be ready in %s", status)
		}
	}
}

func TestValidationRequireExactWatermarkFlag(t *testing.T) {
	t.Setenv("QMIGRATION_VALIDATION_REQUIRE_EXACT_WATERMARK", "true")
	if !validationRequireExactWatermark() {
		t.Fatal("exact watermark requirement must enable for true")
	}
	t.Setenv("QMIGRATION_VALIDATION_REQUIRE_EXACT_WATERMARK", "0")
	if validationRequireExactWatermark() {
		t.Fatal("exact watermark requirement must disable for 0")
	}
}

func TestTaskFlowControlUsesCDCSpoolBacklogPressure(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	svc := NewService(repo, connector.NewRegistry())
	now := time.Now()
	task := domain.MigrationTask{ID: "spool-flow-task", Mode: domain.ModeFullAndIncremental, Status: domain.StatusFullMigrating, Parallelism: 8, EffectiveParallelism: 8, FlowControlLevel: "NORMAL", CreatedAt: now, UpdatedAt: now}
	if err := repo.CreateMigration(ctx, &task); err != nil {
		t.Fatal(err)
	}
	if err := repo.CreateCDCSpool(ctx, &domain.CDCSpoolRecord{ID: "spool-flow-1", TaskID: task.ID, Direction: "forward", Sequence: 1, PositionValue: "p1", EventCount: 1, PayloadBytes: 500, Status: domain.CDCSpoolPending, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	t.Setenv("QMIGRATION_CDC_SPOOL_MAX_PENDING_BYTES", "1000")
	t.Setenv("QMIGRATION_CDC_SPOOL_BACKLOG_WARN_BYTES", "400")
	t.Setenv("QMIGRATION_CDC_SPOOL_BACKLOG_CRITICAL_BYTES", "800")
	level, reason := svc.reconcileTaskFlowControl(ctx, &task, "NORMAL", "")
	if level != "WARN" || task.EffectiveParallelism != 7 || !strings.Contains(reason, "spool backlog warning") {
		t.Fatalf("unexpected spool flow control level=%s reason=%q task=%+v", level, reason, task)
	}
}

func TestTaskFlowControlPredictsCDCSpoolExhaustion(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	svc := NewService(repo, connector.NewRegistry())
	now := time.Now()
	task := domain.MigrationTask{ID: "spool-predict-task", Mode: domain.ModeFullAndIncremental, Status: domain.StatusFullMigrating, Parallelism: 8, EffectiveParallelism: 8, FlowControlLevel: "NORMAL", CreatedAt: now, UpdatedAt: now}
	if err := repo.CreateMigration(ctx, &task); err != nil {
		t.Fatal(err)
	}
	if err := repo.CreateCDCSpool(ctx, &domain.CDCSpoolRecord{ID: "spool-predict-1", TaskID: task.ID, Direction: "forward", Sequence: 1, PositionValue: "p1", EventCount: 1, PayloadBytes: 300, Status: domain.CDCSpoolPending, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	t.Setenv("QMIGRATION_CDC_SPOOL_MAX_PENDING_BYTES", "1000")
	t.Setenv("QMIGRATION_CDC_SPOOL_BACKLOG_WARN_BYTES", "600")
	t.Setenv("QMIGRATION_CDC_SPOOL_BACKLOG_CRITICAL_BYTES", "800")
	t.Setenv("QMIGRATION_CDC_SPOOL_PREDICT_CRITICAL_SECONDS", "30")
	t.Setenv("QMIGRATION_CDC_SPOOL_PREDICT_WARN_SECONDS", "90")
	svc.pressure[task.ID] = taskPressureSample{At: time.Now().Add(-10 * time.Second), Level: "NORMAL", SpoolPendingBytes: 100}
	level, reason := svc.reconcileTaskFlowControl(ctx, &task, "NORMAL", "")
	if level != "CRITICAL" || task.EffectiveParallelism != 4 || !strings.Contains(reason, "projected critical headroom") {
		t.Fatalf("unexpected predictive spool control level=%s reason=%q task=%+v sample=%+v", level, reason, task, svc.pressure[task.ID])
	}
	if svc.pressure[task.ID].SpoolGrowthBPS <= 0 {
		t.Fatalf("expected positive spool growth sample: %+v", svc.pressure[task.ID])
	}
	stored, err := repo.GetMigration(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.CDCSpoolGrowthBytesSec <= 0 || stored.CDCSpoolCriticalETASeconds <= 0 {
		t.Fatalf("predictive spool telemetry not persisted: %+v", stored)
	}
}

func TestAdaptiveBatchTargetUsesBoundedAIMD(t *testing.T) {
	t.Setenv("QMIGRATION_ADAPTIVE_BATCH_TARGET_MS", "1000")
	t.Setenv("QMIGRATION_ADAPTIVE_BATCH_MIN_ROWS", "50")
	t.Setenv("QMIGRATION_ADAPTIVE_BATCH_MAX_ROWS", "5000")
	if got := adaptiveBatchTarget(domain.ChunkProgress{LastBatchRows: 1000, LastReadMS: 200, LastWriteMS: 300}); got != 1250 {
		t.Fatalf("fast batch target=%d want 1250", got)
	}
	if got := adaptiveBatchTarget(domain.ChunkProgress{LastBatchRows: 1000, LastReadMS: 100, LastWriteMS: 4000}); got != 500 {
		t.Fatalf("slow batch target=%d want bounded 500", got)
	}
	if got := adaptiveBatchTarget(domain.ChunkProgress{LastBatchRows: 1000, LastReadMS: 950, LastWriteMS: 900}); got != 1000 {
		t.Fatalf("near-target batch changed unexpectedly: %d", got)
	}
}

func TestTaskTargetBytesPerWorker(t *testing.T) {
	task := &domain.MigrationTask{TargetThroughputMBps: 100, Parallelism: 8, EffectiveParallelism: 4}
	if got, want := taskTargetBytesPerWorker(task, time.Now()), int64(25<<20); got != want {
		t.Fatalf("target per worker=%d want %d", got, want)
	}
	task.RateLimitWindows = []domain.RateLimitWindow{{Start: "00:00", End: "00:00", TargetThroughputMBps: 40}}
	if got, want := taskTargetBytesPerWorker(task, time.Now()), int64(10<<20); got != want {
		t.Fatalf("window target per worker=%d want %d", got, want)
	}
}

func TestEWMARateStabilizesChunkSpeed(t *testing.T) {
	t.Setenv("QMIGRATION_SPEED_EWMA_ALPHA_PCT", "25")
	if got := ewmaRate(100, 200); got != 125 {
		t.Fatalf("ewma=%d want 125", got)
	}
	if got := ewmaRate(0, 200); got != 200 {
		t.Fatalf("initial ewma=%d want 200", got)
	}
}

func TestRC31TaskGlobalWorkerBudgets(t *testing.T) {
	task := &domain.MigrationTask{Parallelism: 8, EffectiveParallelism: 4, ReadLimitMBps: 200, WriteLimitMBps: 120, TargetThroughputMBps: 80}
	read, write, target := taskWorkerBudgets(task, time.Now())
	if read != 50<<20 || write != 30<<20 || target != 20<<20 {
		t.Fatalf("worker budgets read=%d write=%d target=%d", read, write, target)
	}
	task.EffectiveParallelism = 10
	read, write, target = taskWorkerBudgets(task, time.Now())
	if read != 20<<20 || write != 12<<20 || target != 8<<20 {
		t.Fatalf("rescaled budgets read=%d write=%d target=%d", read, write, target)
	}
}

func TestRC31SLAThroughputController(t *testing.T) {
	now := time.Now()
	task := &domain.MigrationTask{CompletionSLASeconds: 100, SLAStartedAt: now, BytesMigrated: 0}
	tables := []domain.MigrationTable{{DataLength: 1000}}
	reconcileThroughputController(task, tables, now)
	if task.ControllerTargetBytesSec != 11 { // 1000/100 with default 110% headroom
		t.Fatalf("controller target=%d reason=%q", task.ControllerTargetBytesSec, task.ThroughputControllerReason)
	}
	if !strings.Contains(task.ThroughputControllerReason, "SLA") {
		t.Fatalf("reason=%q", task.ThroughputControllerReason)
	}
}

func TestRC31AutoThroughputControllerPressure(t *testing.T) {
	task := &domain.MigrationTask{AutoThroughputEnabled: true, SpeedBytesSec: 1000, FlowControlLevel: "NORMAL"}
	reconcileThroughputController(task, nil, time.Now())
	if task.ControllerTargetBytesSec != 1250 {
		t.Fatalf("normal target=%d", task.ControllerTargetBytesSec)
	}
	task.FlowControlLevel = "CRITICAL"
	reconcileThroughputController(task, nil, time.Now())
	if task.ControllerTargetBytesSec != 700 {
		t.Fatalf("critical target=%d", task.ControllerTargetBytesSec)
	}
}

func TestRC31AdaptiveHotspotFillsIdleWorkers(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	svc := NewService(repo, connector.NewRegistry())
	now := time.Now()
	task := domain.MigrationTask{ID: "hot-task", Mode: domain.ModeFull, Status: domain.StatusFullMigrating, FullEngine: "native", Parallelism: 6, EffectiveParallelism: 6, MaxRetries: 3, SpeedBytesSec: 10 << 20, CreatedAt: now, UpdatedAt: now}
	if err := repo.CreateMigration(ctx, &task); err != nil {
		t.Fatal(err)
	}
	table := domain.MigrationTable{ID: "hot-table", TaskID: task.ID, Engine: "native", SourceSchema: "app", SourceTable: "orders", TargetSchema: "app", TargetTable: "orders", PrimaryKey: "id", TotalChunks: 2, Status: "RUNNING"}
	if err := repo.CreateMigrationTable(ctx, &table); err != nil {
		t.Fatal(err)
	}
	chunks := []domain.MigrationChunk{
		{ID: "hot-done", TaskID: task.ID, TableID: table.ID, ChunkNo: 1, SplitType: "PK_RANGE", PrimaryKey: "id", Start: 1, End: 100, Status: domain.ChunkSuccess},
		{ID: "hot-pending", TaskID: task.ID, TableID: table.ID, ChunkNo: 2, SplitType: "PK_RANGE", PrimaryKey: "id", Start: 101, End: 700, Status: domain.ChunkPending},
	}
	if err := repo.CreateChunks(ctx, chunks); err != nil {
		t.Fatal(err)
	}
	if err := svc.adaptPendingChunks(ctx, &chunks[0], domain.ChunkResult{DurationMS: 61000, BytesWritten: 1 << 20, RowsWritten: 100}); err != nil {
		t.Fatal(err)
	}
	got, err := repo.ListChunks(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	pending := 0
	for _, ch := range got {
		if ch.TableID == table.ID && ch.Status == domain.ChunkPending {
			pending++
		}
	}
	if pending != 6 {
		t.Fatalf("pending=%d chunks=%+v", pending, got)
	}
	stored, _ := repo.GetMigration(ctx, task.ID)
	if stored.AdaptiveHotspotSplits != 5 {
		t.Fatalf("hotspot splits=%d", stored.AdaptiveHotspotSplits)
	}
}

func TestRC32RunningNumericYieldCreatesGapFreeRemainders(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	svc := NewService(repo, connector.NewRegistry())
	now := time.Now()
	task := domain.MigrationTask{ID: "rc32-yield-num", Status: domain.StatusFullMigrating, Mode: domain.ModeFull, FullEngine: "native", Parallelism: 4, EffectiveParallelism: 4, CreatedAt: now, UpdatedAt: now}
	_ = repo.CreateMigration(ctx, &task)
	table := domain.MigrationTable{ID: "rc32-yield-num-table", TaskID: task.ID, Engine: "native", PrimaryKey: "id", Status: "RUNNING"}
	_ = repo.CreateMigrationTable(ctx, &table)
	chunk := domain.MigrationChunk{ID: "rc32-yield-num-chunk", TaskID: task.ID, TableID: table.ID, ChunkNo: 1, SplitType: "PK_RANGE", PrimaryKey: "id", Start: 1, End: 1000, Status: domain.ChunkRunning, WorkerID: "w1", StartedAt: now.Add(-time.Minute)}
	_ = repo.CreateChunks(ctx, []domain.MigrationChunk{chunk})
	if err := svc.CompleteChunk(ctx, "w1", chunk.ID, domain.ChunkResult{RowsWritten: 200, RowsRead: 200, BytesWritten: 20 << 20, BytesRead: 20 << 20, CursorJSON: `{"after_pk":200}`, Yielded: true}); err != nil {
		t.Fatal(err)
	}
	got, _ := repo.ListChunks(ctx, task.ID)
	if len(got) != 5 {
		t.Fatalf("chunks=%+v", got)
	}
	if got[0].Status != domain.ChunkSuccess || got[0].End != 200 {
		t.Fatalf("prefix=%+v", got[0])
	}
	next := int64(201)
	for i := 1; i < len(got); i++ {
		if got[i].Start != next {
			t.Fatalf("gap at %d chunks=%+v", i, got)
		}
		next = got[i].End + 1
	}
	if next != 1001 {
		t.Fatalf("coverage ended %d", next)
	}
	stored, _ := repo.GetMigration(ctx, task.ID)
	if stored.AdaptiveRunningYields != 1 || stored.AdaptiveHotspotSplits != 4 {
		t.Fatalf("telemetry=%+v", stored)
	}
}

func TestRC32HotWorkerDefersClaimToCoolWorker(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	svc := NewService(repo, connector.NewRegistry())
	now := time.Now()
	_ = repo.UpsertWorker(ctx, &domain.Worker{ID: "hot", Status: "ONLINE", CPU: 4, RunningJobs: 2, CPUUsagePct: 80, MemoryUsagePct: 70, Capabilities: []string{"native"}, LastHeartbeat: now})
	_ = repo.UpsertWorker(ctx, &domain.Worker{ID: "cool", Status: "ONLINE", CPU: 8, RunningJobs: 0, CPUUsagePct: 10, MemoryUsagePct: 10, Capabilities: []string{"native"}, LastHeartbeat: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "rc32-load-src", Type: domain.DataSourceMySQL, CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "rc32-load-dst", Type: domain.DataSourceMySQL, CreatedAt: now})
	task := domain.MigrationTask{ID: "rc32-load", SourceID: "rc32-load-src", TargetID: "rc32-load-dst", Status: domain.StatusFullMigrating, Mode: domain.ModeFull, FullEngine: "native", Parallelism: 4, EffectiveParallelism: 4, CreatedAt: now, UpdatedAt: now}
	_ = repo.CreateMigration(ctx, &task)
	table := domain.MigrationTable{ID: "rc32-load-table", TaskID: task.ID, Engine: "native", Status: "RUNNING"}
	_ = repo.CreateMigrationTable(ctx, &table)
	_ = repo.CreateChunks(ctx, []domain.MigrationChunk{{ID: "rc32-load-chunk", TaskID: task.ID, TableID: table.ID, ChunkNo: 1, Status: domain.ChunkPending}})
	if _, err := svc.ClaimChunk(ctx, "hot"); !errors.Is(err, repository.ErrNoChunk) {
		t.Fatalf("hot worker should defer, err=%v", err)
	}
	if _, err := svc.ClaimChunk(ctx, "cool"); err != nil {
		t.Fatalf("cool worker claim: %v", err)
	}
}

func TestRC32PendingCompositeKeysetSplitsMultiway(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	reg := connector.NewRegistry()
	reg.Register(domain.DataSourceMySQL, autoFactory{composite: true})
	svc := NewService(repo, reg)
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "rc32-key-src", Type: domain.DataSourceMySQL, Database: "app", CreatedAt: now})
	task := domain.MigrationTask{ID: "rc32-key", SourceID: "rc32-key-src", Status: domain.StatusFullMigrating, Mode: domain.ModeFull, FullEngine: "native", Parallelism: 4, EffectiveParallelism: 4, SpeedBytesSec: 10 << 20, CreatedAt: now, UpdatedAt: now}
	_ = repo.CreateMigration(ctx, &task)
	table := domain.MigrationTable{ID: "rc32-key-table", TaskID: task.ID, Engine: "native", SourceSchema: "app", SourceTable: "items", PrimaryKey: "tenant_id", PrimaryKeys: []string{"tenant_id", "id"}, Columns: []domain.ColumnInfo{{Name: "tenant_id"}, {Name: "id"}}, Status: "RUNNING"}
	_ = repo.CreateMigrationTable(ctx, &table)
	lo, _ := json.Marshal([]connector.Value{{Raw: []byte("a")}, {Raw: []byte("1")}})
	hi, _ := json.Marshal([]connector.Value{{Raw: []byte("z")}, {Raw: []byte("99")}})
	chunks := []domain.MigrationChunk{{ID: "rc32-key-done", TaskID: task.ID, TableID: table.ID, ChunkNo: 1, SplitType: "PRIMARY_KEY_KEYSET", Status: domain.ChunkSuccess}, {ID: "rc32-key-pending", TaskID: task.ID, TableID: table.ID, ChunkNo: 2, SplitType: "PRIMARY_KEY_KEYSET", PrimaryKey: "tenant_id", StartCursorJSON: string(lo), EndCursorJSON: string(hi), Status: domain.ChunkPending}}
	_ = repo.CreateChunks(ctx, chunks)
	if err := svc.adaptPendingChunks(ctx, &chunks[0], domain.ChunkResult{DurationMS: 61000, BytesWritten: 1 << 20}); err != nil {
		t.Fatal(err)
	}
	got, _ := repo.ListChunks(ctx, task.ID)
	pending := 0
	for _, c := range got {
		if c.Status == domain.ChunkPending {
			pending++
		}
	}
	if pending != 4 {
		t.Fatalf("pending=%d chunks=%+v", pending, got)
	}
}

func TestRC32RunningCompositeKeysetYieldUsesExclusiveCursor(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	reg := connector.NewRegistry()
	reg.Register(domain.DataSourceMySQL, autoFactory{composite: true})
	svc := NewService(repo, reg)
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "rc32-run-key-src", Type: domain.DataSourceMySQL, CreatedAt: now})
	task := domain.MigrationTask{ID: "rc32-run-key", SourceID: "rc32-run-key-src", Status: domain.StatusFullMigrating, Mode: domain.ModeFull, FullEngine: "native", Parallelism: 4, EffectiveParallelism: 4, CreatedAt: now, UpdatedAt: now}
	_ = repo.CreateMigration(ctx, &task)
	table := domain.MigrationTable{ID: "rc32-run-key-table", TaskID: task.ID, Engine: "native", SourceSchema: "app", SourceTable: "items", PrimaryKey: "tenant_id", PrimaryKeys: []string{"tenant_id", "id"}, Columns: []domain.ColumnInfo{{Name: "tenant_id"}, {Name: "id"}}, Status: "RUNNING"}
	_ = repo.CreateMigrationTable(ctx, &table)
	cursor, _ := json.Marshal([]connector.Value{{Raw: []byte("a")}, {Raw: []byte("10")}})
	upper, _ := json.Marshal([]connector.Value{{Raw: []byte("z")}, {Raw: []byte("99")}})
	chunk := domain.MigrationChunk{ID: "rc32-run-key-chunk", TaskID: task.ID, TableID: table.ID, ChunkNo: 1, SplitType: "PRIMARY_KEY_KEYSET", PrimaryKey: "tenant_id", EndCursorJSON: string(upper), CursorJSON: string(cursor), Status: domain.ChunkRunning, WorkerID: "w1", StartedAt: now.Add(-time.Minute)}
	_ = repo.CreateChunks(ctx, []domain.MigrationChunk{chunk})
	if err := svc.CompleteChunk(ctx, "w1", chunk.ID, domain.ChunkResult{RowsWritten: 100, BytesWritten: 20 << 20, CursorJSON: string(cursor), Yielded: true}); err != nil {
		t.Fatal(err)
	}
	got, _ := repo.ListChunks(ctx, task.ID)
	if len(got) != 5 {
		t.Fatalf("chunks=%+v", got)
	}
	if got[0].EndCursorJSON != string(cursor) {
		t.Fatalf("prefix end=%s", got[0].EndCursorJSON)
	}
	if got[1].StartCursorJSON != string(cursor) || got[1].CursorJSON != string(cursor) {
		t.Fatalf("first remainder not exclusive resume: %+v", got[1])
	}
	for i := 1; i < len(got)-1; i++ {
		if got[i].EndCursorJSON != got[i+1].StartCursorJSON {
			t.Fatalf("keyset gap: %+v", got)
		}
	}
}

func TestRC32ControllerLearningIsBoundedAndPersistentState(t *testing.T) {
	now := time.Now()
	task := &domain.MigrationTask{AutoThroughputEnabled: true, SpeedBytesSec: 1000, ControllerTargetBytesSec: 1000, FlowControlLevel: "NORMAL"}
	learnThroughputController(task, nil, now)
	if task.ControllerAutoProbePct != 127 || task.ControllerLearningSamples != 1 {
		t.Fatalf("learned=%+v", task)
	}
	task.FlowControlLevel = "CRITICAL"
	for i := 0; i < 20; i++ {
		learnThroughputController(task, nil, now)
	}
	if task.ControllerAutoProbePct < 80 {
		t.Fatalf("probe underflow=%d", task.ControllerAutoProbePct)
	}
	sla := &domain.MigrationTask{CompletionSLASeconds: 100, SLAStartedAt: now, SpeedBytesSec: 5, ControllerSLAHeadroomPct: 110}
	tables := []domain.MigrationTable{{DataLength: 1000}}
	learnThroughputController(sla, tables, now)
	if sla.ControllerSLAHeadroomPct <= 110 {
		t.Fatalf("sla learning=%+v", sla)
	}
}

func TestRC33TablePerformanceProfilesByTopology(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	svc := NewService(repo, nil)
	now := time.Now()
	task := domain.MigrationTask{ID: "rc33-prof-task", SourceID: "src", TargetID: "dst", Status: domain.StatusFullMigrating, CreatedAt: now, UpdatedAt: now}
	if err := repo.CreateMigration(ctx, &task); err != nil {
		t.Fatal(err)
	}
	table := domain.MigrationTable{ID: "rc33-prof-table", TaskID: task.ID, SourceSchema: "app", SourceTable: "orders", EstimatedRows: 1_000_000, MinPK: 1, MaxPK: 1_000_000, Status: "RUNNING"}
	if err := repo.CreateMigrationTable(ctx, &table); err != nil {
		t.Fatal(err)
	}
	chunk := domain.MigrationChunk{ID: "rc33-prof-chunk", TaskID: task.ID, TableID: table.ID, TopologyID: "dn-1", SplitType: "PK_RANGE", Start: 1, End: 100000}
	if err := svc.recordTablePerformance(ctx, &chunk, domain.ChunkResult{RowsWritten: 100000, BytesWritten: 100 << 20, DurationMS: 10000}); err != nil {
		t.Fatal(err)
	}
	got, err := repo.GetMigrationTable(ctx, table.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ProfileRowsPerSec != 10000 || got.ProfileBytesPerSec != 10<<20 {
		t.Fatalf("profile=%+v", got)
	}
	if got.RecommendedChunkRows != 300000 || got.PerformanceSamples != 1 {
		t.Fatalf("recommend=%d samples=%d", got.RecommendedChunkRows, got.PerformanceSamples)
	}
	p := got.TopologyPerformance["dn-1"]
	if p.RowsPerSec != 10000 || p.RecommendedChunkRows != 300000 || p.Samples != 1 {
		t.Fatalf("topology=%+v", p)
	}
}

func TestRC33HistoricalProfileLookup(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	now := time.Now()
	old := domain.MigrationTask{ID: "rc33-old", SourceID: "src", TargetID: "dst", UpdatedAt: now.Add(-time.Minute), CreatedAt: now.Add(-time.Hour)}
	newer := domain.MigrationTask{ID: "rc33-new", SourceID: "src", TargetID: "dst", UpdatedAt: now, CreatedAt: now}
	_ = repo.CreateMigration(ctx, &old)
	_ = repo.CreateMigration(ctx, &newer)
	_ = repo.CreateMigrationTable(ctx, &domain.MigrationTable{ID: "p1", TaskID: old.ID, SourceSchema: "app", SourceTable: "orders", RecommendedChunkRows: 100000, PerformanceSamples: 10})
	_ = repo.CreateMigrationTable(ctx, &domain.MigrationTable{ID: "p2", TaskID: newer.ID, SourceSchema: "app", SourceTable: "orders", RecommendedChunkRows: 400000, PerformanceSamples: 3})
	p, err := repo.FindMigrationTableProfile(ctx, "src", "dst", "APP", "ORDERS")
	if err != nil {
		t.Fatal(err)
	}
	if p.ID != "p2" || p.RecommendedChunkRows != 400000 {
		t.Fatalf("profile=%+v", p)
	}
}

func TestRC33IdleWorkerMarksRemotePlacementAsWorkSteal(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	svc := NewService(repo, nil)
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "rc33-steal-src", Type: domain.DataSourceMySQL, CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "rc33-steal-dst", Type: domain.DataSourceMySQL, CreatedAt: now})
	task := domain.MigrationTask{ID: "rc33-steal-task", SourceID: "rc33-steal-src", TargetID: "rc33-steal-dst", Status: domain.StatusFullMigrating, Mode: domain.ModeFull, FullEngine: "qmigration", Parallelism: 2, EffectiveParallelism: 2, BatchRows: 100, CreatedAt: now, UpdatedAt: now}
	_ = repo.CreateMigration(ctx, &task)
	table := domain.MigrationTable{ID: "rc33-steal-table", TaskID: task.ID, Engine: "qmigration", SourceSchema: "app", SourceTable: "orders", TargetSchema: "app", TargetTable: "orders", Status: "READY"}
	_ = repo.CreateMigrationTable(ctx, &table)
	_ = repo.CreateChunks(ctx, []domain.MigrationChunk{{ID: "rc33-steal-chunk", TaskID: task.ID, TableID: table.ID, ChunkNo: 1, SplitType: "PK_RANGE", Status: domain.ChunkPending, PlacementHint: map[string]string{"zone": "a"}, TopologyID: "dn-a"}})
	_ = repo.UpsertWorker(ctx, &domain.Worker{ID: "w-b", Hostname: "wb", CPU: 8, Status: "ONLINE", Capabilities: []string{"qmigration"}, Labels: map[string]string{"zone": "b"}, LastHeartbeat: now})
	job, err := svc.ClaimChunk(ctx, "w-b")
	if err != nil {
		t.Fatal(err)
	}
	if !job.WorkSteal || job.WorkStealReason == "" {
		t.Fatalf("job=%+v", job)
	}
}

func TestRC35CircuitAutomaticallyEntersHalfOpenProbe(t *testing.T) {
	t.Setenv("QMIGRATION_TOPOLOGY_HALF_OPEN_AFTER_SECONDS", "1")
	ctx := context.Background()
	repo := memory.New()
	svc := NewService(repo, nil)
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "rc35-src", Type: domain.DataSourceMySQL, CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "rc35-dst", Type: domain.DataSourceMySQL, CreatedAt: now})
	task := domain.MigrationTask{ID: "rc35-half-task", SourceID: "rc35-src", TargetID: "rc35-dst", Status: domain.StatusFullMigrating, Mode: domain.ModeFull, FullEngine: "qmigration", Parallelism: 2, EffectiveParallelism: 2, BatchRows: 100, CreatedAt: now, UpdatedAt: now}
	_ = repo.CreateMigration(ctx, &task)
	table := domain.MigrationTable{ID: "rc35-half-table", TaskID: task.ID, Engine: "qmigration", SourceSchema: "app", SourceTable: "orders", TargetSchema: "app", TargetTable: "orders", Status: "READY", TopologyPerformance: map[string]domain.TableTopologyPerformance{
		"dn-bad": {Health: "CIRCUIT_OPEN", SlowStreak: 5, Samples: 10, HealthChangedAt: now.Add(-2 * time.Second)},
	}}
	_ = repo.CreateMigrationTable(ctx, &table)
	_ = repo.CreateChunks(ctx, []domain.MigrationChunk{{ID: "rc35-half-chunk", TaskID: task.ID, TableID: table.ID, ChunkNo: 1, SplitType: "PK_RANGE", Status: domain.ChunkPending, TopologyID: "dn-bad"}})
	_ = repo.UpsertWorker(ctx, &domain.Worker{ID: "rc35-worker", Hostname: "w", CPU: 8, Status: "ONLINE", Capabilities: []string{"qmigration"}, LastHeartbeat: now})
	job, err := svc.ClaimChunk(ctx, "rc35-worker")
	if err != nil {
		t.Fatal(err)
	}
	if job.Chunk.TopologyID != "dn-bad" {
		t.Fatalf("job=%+v", job)
	}
	got, _ := repo.GetMigrationTable(ctx, table.ID)
	p := got.TopologyPerformance["dn-bad"]
	if p.Health != "HALF_OPEN" || p.LastProbeAt.IsZero() {
		t.Fatalf("profile=%+v", p)
	}
}

func TestRC35HalfOpenProbeSuccessAndFailure(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	svc := NewService(repo, nil)
	now := time.Now()
	task := domain.MigrationTask{ID: "rc35-probe-task", Status: domain.StatusFullMigrating, CreatedAt: now, UpdatedAt: now}
	_ = repo.CreateMigration(ctx, &task)
	table := domain.MigrationTable{ID: "rc35-probe-table", TaskID: task.ID, EstimatedRows: 1000, MinPK: 1, MaxPK: 1000, ProfileBytesPerSec: 1000, TopologyPerformance: map[string]domain.TableTopologyPerformance{
		"dn": {Health: "HALF_OPEN", SlowStreak: 5, Samples: 10, BytesPerSec: 1000, RowsPerSec: 10},
	}}
	_ = repo.CreateMigrationTable(ctx, &table)
	chunk := domain.MigrationChunk{ID: "c", TaskID: task.ID, TableID: table.ID, TopologyID: "dn", Start: 1, End: 100}
	if err := svc.recordTablePerformance(ctx, &chunk, domain.ChunkResult{RowsWritten: 100, BytesWritten: 100000, DurationMS: 1000}); err != nil {
		t.Fatal(err)
	}
	got, _ := repo.GetMigrationTable(ctx, table.ID)
	if p := got.TopologyPerformance["dn"]; p.Health != "DEGRADED" {
		t.Fatalf("successful probe=%+v", p)
	}
	// Force another half-open probe and make it slow enough to reopen the circuit.
	p := got.TopologyPerformance["dn"]
	p.Health = "HALF_OPEN"
	p.P99DurationMS = 100000
	got.TopologyPerformance["dn"] = p
	_ = repo.UpdateMigrationTable(ctx, got)
	if err := svc.recordTablePerformance(ctx, &chunk, domain.ChunkResult{RowsWritten: 1, BytesWritten: 1, DurationMS: 100000}); err != nil {
		t.Fatal(err)
	}
	got, _ = repo.GetMigrationTable(ctx, table.ID)
	if p := got.TopologyPerformance["dn"]; p.Health != "CIRCUIT_OPEN" {
		t.Fatalf("failed probe=%+v", p)
	}
}

func TestRC35P95P99SLARiskPrediction(t *testing.T) {
	now := time.Now()
	task := &domain.MigrationTask{ETASeconds: 1000, CompletionSLASeconds: 1200, SLAStartedAt: now}
	tables := []domain.MigrationTable{{TopologyPerformance: map[string]domain.TableTopologyPerformance{
		"dn": {P95DurationMS: 45000, P99DurationMS: 90000},
	}}}
	updateSLATailRisk(task, tables, now)
	if task.SLAP95ETASeconds <= task.ETASeconds || task.SLAP99ETASeconds <= task.SLAP95ETASeconds {
		t.Fatalf("tail eta=%+v", task)
	}
	if task.SLARiskLevel != "CRITICAL" || task.SLARiskReason == "" {
		t.Fatalf("risk=%+v", task)
	}
}

func TestRC36CircuitOpenRunningChunkDrainsAtDurableBatchBoundary(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	svc := NewService(repo, nil)
	now := time.Now()
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "rc36-src", Type: domain.DataSourceMySQL, CreatedAt: now})
	_ = repo.CreateDataSource(ctx, &domain.DataSource{ID: "rc36-dst", Type: domain.DataSourceMySQL, CreatedAt: now})
	task := domain.MigrationTask{ID: "rc36-drain-task", SourceID: "rc36-src", TargetID: "rc36-dst", Status: domain.StatusFullMigrating, Mode: domain.ModeFull, FullEngine: "qmigration", Parallelism: 1, EffectiveParallelism: 1, BatchRows: 100, CreatedAt: now, UpdatedAt: now}
	_ = repo.CreateMigration(ctx, &task)
	table := domain.MigrationTable{ID: "rc36-drain-table", TaskID: task.ID, Engine: "qmigration", SourceSchema: "app", SourceTable: "orders", TargetSchema: "app", TargetTable: "orders", Status: "READY", TopologyPerformance: map[string]domain.TableTopologyPerformance{
		"dn-a": {Health: "HEALTHY", Samples: 10},
	}}
	_ = repo.CreateMigrationTable(ctx, &table)
	_ = repo.CreateChunks(ctx, []domain.MigrationChunk{{ID: "rc36-drain-chunk", TaskID: task.ID, TableID: table.ID, ChunkNo: 1, SplitType: "PK_RANGE", Start: 1, End: 1000, Status: domain.ChunkPending, TopologyID: "dn-a"}})
	_ = repo.UpsertWorker(ctx, &domain.Worker{ID: "rc36-worker", Hostname: "w", CPU: 8, Status: "ONLINE", Capabilities: []string{"qmigration"}, LastHeartbeat: now})
	job, err := svc.ClaimChunk(ctx, "rc36-worker")
	if err != nil {
		t.Fatal(err)
	}
	if job.Chunk.ID != "rc36-drain-chunk" {
		t.Fatalf("job=%+v", job)
	}

	// Another completed chunk/sample can open the topology while this chunk is
	// still running. RC36 must stop this worker at the next committed checkpoint
	// even though the ordinary hotspot min-bytes/min-seconds thresholds are not met.
	storedTable, _ := repo.GetMigrationTable(ctx, table.ID)
	p := storedTable.TopologyPerformance["dn-a"]
	p.Health = "CIRCUIT_OPEN"
	p.HealthChangedAt = time.Now()
	storedTable.TopologyPerformance["dn-a"] = p
	_ = repo.UpdateMigrationTable(ctx, storedTable)
	control, err := svc.RenewChunk(ctx, "rc36-worker", job.Chunk.ID, domain.ChunkProgress{CursorJSON: `{"after_pk":100}`, RowsRead: 100, RowsWritten: 100, BytesRead: 1024, BytesWritten: 1024, LastBatchRows: 100})
	if err != nil {
		t.Fatal(err)
	}
	if !control.YieldAfterBatch || !strings.Contains(control.YieldReason, "topology circuit drain") {
		t.Fatalf("control=%+v", control)
	}
	if err := svc.CompleteChunk(ctx, "rc36-worker", job.Chunk.ID, domain.ChunkResult{RowsRead: 100, RowsWritten: 100, BytesRead: 1024, BytesWritten: 1024, CursorJSON: `{"after_pk":100}`, Yielded: true, YieldReason: control.YieldReason}); err != nil {
		t.Fatal(err)
	}
	storedTask, _ := repo.GetMigration(ctx, task.ID)
	if storedTask.AdaptiveRunningYields != 1 || storedTask.AdaptiveTopologyDrains != 1 {
		t.Fatalf("task telemetry=%+v", storedTask)
	}
	chunks, _ := repo.ListChunks(ctx, task.ID)
	var pending *domain.MigrationChunk
	for i := range chunks {
		if chunks[i].Status == domain.ChunkPending {
			pending = &chunks[i]
			break
		}
	}
	if pending == nil || pending.TopologyID != "dn-a" || pending.Start != 101 || pending.End != 1000 {
		t.Fatalf("pending remainder=%+v all=%+v", pending, chunks)
	}
	if _, err := svc.ClaimChunk(ctx, "rc36-worker"); !errors.Is(err, repository.ErrNoChunk) {
		t.Fatalf("circuit-bound remainder must stay blocked until recovery: %v", err)
	}
}

func TestRC36CircuitDrainRefusesUnsafeUnsplittableChunk(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	svc := NewService(repo, nil)
	now := time.Now()
	task := &domain.MigrationTask{ID: "rc36-hash", Status: domain.StatusFullMigrating, Parallelism: 4, EffectiveParallelism: 4, CreatedAt: now, UpdatedAt: now}
	_ = repo.CreateMigration(ctx, task)
	table := &domain.MigrationTable{ID: "rc36-hash-table", TaskID: task.ID, TopologyPerformance: map[string]domain.TableTopologyPerformance{"dn-a": {Health: "CIRCUIT_OPEN", HealthChangedAt: now}}}
	_ = repo.CreateMigrationTable(ctx, table)
	chunk := &domain.MigrationChunk{ID: "rc36-hash-chunk", TaskID: task.ID, TableID: table.ID, SplitType: "HASH", TopologyID: "dn-a", Status: domain.ChunkRunning, StartedAt: now.Add(-time.Hour)}
	yield, reason := svc.shouldYieldRunningChunk(ctx, task, chunk, nil, domain.ChunkProgress{CursorJSON: `{"bucket":3}`, BytesWritten: 1 << 30})
	if yield || reason != "" {
		t.Fatalf("unsafe HASH continuation must not be yielded: yield=%v reason=%q", yield, reason)
	}
}

func TestRC37DegradedTopologyThrottlesRunningChunk(t *testing.T) {
	t.Setenv("QMIGRATION_TOPOLOGY_DEGRADED_BATCH_PCT", "40")
	t.Setenv("QMIGRATION_TOPOLOGY_DEGRADED_PAUSE_MS", "333")
	ctx := context.Background()
	repo := memory.New()
	svc := NewService(repo, nil)
	now := time.Now()
	task := domain.MigrationTask{ID: "rc37-throttle-task", Status: domain.StatusFullMigrating, FullEngine: "qmigration", Parallelism: 1, EffectiveParallelism: 1, BatchRows: 1000, ReadLimitMBps: 10, WriteLimitMBps: 20, TargetThroughputMBps: 30, CreatedAt: now, UpdatedAt: now}
	_ = repo.CreateMigration(ctx, &task)
	table := domain.MigrationTable{ID: "rc37-throttle-table", TaskID: task.ID, Engine: "qmigration", TopologyPerformance: map[string]domain.TableTopologyPerformance{"dn-a": {Health: "DEGRADED", Samples: 10}}}
	_ = repo.CreateMigrationTable(ctx, &table)
	chunk := domain.MigrationChunk{ID: "rc37-throttle-chunk", TaskID: task.ID, TableID: table.ID, ChunkNo: 1, SplitType: "PK_RANGE", Start: 1, End: 1000, Status: domain.ChunkRunning, WorkerID: "rc37-worker", TopologyID: "dn-a", StartedAt: now.Add(-time.Minute)}
	_ = repo.CreateChunks(ctx, []domain.MigrationChunk{chunk})
	_ = repo.UpsertWorker(ctx, &domain.Worker{ID: "rc37-worker", Hostname: "w", CPU: 8, Status: "ONLINE", Capabilities: []string{"qmigration"}, LastHeartbeat: now})

	control, err := svc.RenewChunk(ctx, "rc37-worker", chunk.ID, domain.ChunkProgress{CursorJSON: `{"after_pk":100}`, RowsRead: 100, RowsWritten: 100, BytesRead: 1024, BytesWritten: 1024, LastBatchRows: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if control.Level != "WARN" || control.PauseMS != 333 || control.MaxBatchRows != 400 || control.TargetBatchRows > 400 {
		t.Fatalf("degraded topology control=%+v", control)
	}
	if !strings.Contains(control.Reason, "DEGRADED throttle") {
		t.Fatalf("control reason=%q", control.Reason)
	}
	if control.ReadBytesPerSec != 4*(1<<20) || control.WriteBytesPerSec != 8*(1<<20) || control.TargetBytesPerSec != 12*(1<<20) {
		t.Fatalf("degraded byte budgets were not scaled: %+v", control)
	}
	if control.YieldAfterBatch {
		t.Fatalf("single running chunk is already within degraded cap: %+v", control)
	}
}

func TestRC37DegradedTopologyConvergesAlreadyRunningConcurrency(t *testing.T) {
	t.Setenv("QMIGRATION_TOPOLOGY_DEGRADED_MAX_CONCURRENCY", "1")
	ctx := context.Background()
	repo := memory.New()
	svc := NewService(repo, nil)
	now := time.Now()
	task := domain.MigrationTask{ID: "rc37-shed-task", Status: domain.StatusFullMigrating, FullEngine: "qmigration", Parallelism: 2, EffectiveParallelism: 2, BatchRows: 100, CreatedAt: now, UpdatedAt: now}
	_ = repo.CreateMigration(ctx, &task)
	table := domain.MigrationTable{ID: "rc37-shed-table", TaskID: task.ID, Engine: "qmigration", TopologyPerformance: map[string]domain.TableTopologyPerformance{"dn-a": {Health: "DEGRADED", Samples: 10}}}
	_ = repo.CreateMigrationTable(ctx, &table)
	chunks := []domain.MigrationChunk{
		{ID: "rc37-keep", TaskID: task.ID, TableID: table.ID, ChunkNo: 1, SplitType: "PK_RANGE", Start: 1, End: 1000, Status: domain.ChunkRunning, WorkerID: "rc37-w1", TopologyID: "dn-a", StartedAt: now.Add(-2 * time.Minute)},
		{ID: "rc37-yield", TaskID: task.ID, TableID: table.ID, ChunkNo: 2, SplitType: "PK_RANGE", Start: 1001, End: 2000, Status: domain.ChunkRunning, WorkerID: "rc37-w2", TopologyID: "dn-a", StartedAt: now.Add(-time.Minute)},
	}
	_ = repo.CreateChunks(ctx, chunks)
	_ = repo.UpsertWorker(ctx, &domain.Worker{ID: "rc37-w1", Hostname: "w1", CPU: 8, Status: "ONLINE", Capabilities: []string{"qmigration"}, LastHeartbeat: now})
	_ = repo.UpsertWorker(ctx, &domain.Worker{ID: "rc37-w2", Hostname: "w2", CPU: 8, Status: "ONLINE", Capabilities: []string{"qmigration"}, LastHeartbeat: now})

	keep, err := svc.RenewChunk(ctx, "rc37-w1", "rc37-keep", domain.ChunkProgress{CursorJSON: `{"after_pk":100}`, RowsWritten: 100, BytesWritten: 1024, LastBatchRows: 100})
	if err != nil {
		t.Fatal(err)
	}
	if keep.YieldAfterBatch {
		t.Fatalf("oldest deterministic survivor must remain running: %+v", keep)
	}
	yield, err := svc.RenewChunk(ctx, "rc37-w2", "rc37-yield", domain.ChunkProgress{CursorJSON: `{"after_pk":1200}`, RowsRead: 200, RowsWritten: 200, BytesRead: 2048, BytesWritten: 2048, LastBatchRows: 100})
	if err != nil {
		t.Fatal(err)
	}
	if !yield.YieldAfterBatch || !strings.Contains(yield.YieldReason, "topology degraded shed") {
		t.Fatalf("excess degraded chunk must yield: %+v", yield)
	}
	if err := svc.CompleteChunk(ctx, "rc37-w2", "rc37-yield", domain.ChunkResult{RowsRead: 200, RowsWritten: 200, BytesRead: 2048, BytesWritten: 2048, CursorJSON: `{"after_pk":1200}`, Yielded: true, YieldReason: yield.YieldReason}); err != nil {
		t.Fatal(err)
	}
	storedTask, _ := repo.GetMigration(ctx, task.ID)
	if storedTask.AdaptiveRunningYields != 1 || storedTask.AdaptiveTopologyDegradedYields != 1 || storedTask.AdaptiveTopologyDrains != 0 {
		t.Fatalf("task telemetry=%+v", storedTask)
	}
	all, _ := repo.ListChunks(ctx, task.ID)
	running, pending := 0, 0
	for i := range all {
		if all[i].TopologyID != "dn-a" {
			t.Fatalf("yield remainder changed topology ownership: %+v", all[i])
		}
		if all[i].Status == domain.ChunkRunning {
			running++
		}
		if all[i].Status == domain.ChunkPending {
			pending++
		}
	}
	if running != 1 || pending == 0 {
		t.Fatalf("expected convergence to one running degraded chunk plus pending remainder: %+v", all)
	}
	if _, err := svc.ClaimChunk(ctx, "rc37-w2"); !errors.Is(err, repository.ErrNoChunk) {
		t.Fatalf("pending degraded remainder must remain blocked while cap is occupied: %v", err)
	}
}

func TestRC38DegradedRecoveryUsesHysteresisAndRampsConcurrency(t *testing.T) {
	t.Setenv("QMIGRATION_TOPOLOGY_DEGRADED_MAX_CONCURRENCY", "1")
	t.Setenv("QMIGRATION_TOPOLOGY_RECOVERY_MAX_CONCURRENCY", "3")
	t.Setenv("QMIGRATION_TOPOLOGY_RECOVERY_MIN_DEGRADED_SECONDS", "0")
	t.Setenv("QMIGRATION_TOPOLOGY_RECOVERY_STEP_GOOD_SAMPLES", "2")
	t.Setenv("QMIGRATION_TOPOLOGY_RECOVERY_HEALTHY_GOOD_SAMPLES", "6")
	ctx := context.Background()
	repo := memory.New()
	svc := NewService(repo, nil)
	now := time.Now()
	task := domain.MigrationTask{ID: "rc38-recovery-task", Status: domain.StatusFullMigrating, CreatedAt: now, UpdatedAt: now}
	if err := repo.CreateMigration(ctx, &task); err != nil {
		t.Fatal(err)
	}
	table := domain.MigrationTable{ID: "rc38-recovery-table", TaskID: task.ID, EstimatedRows: 10000, MinPK: 1, MaxPK: 10000, ProfileBytesPerSec: 100000, TopologyPerformance: map[string]domain.TableTopologyPerformance{
		"dn-a": {Health: "DEGRADED", SlowStreak: 1, Samples: 10, BytesPerSec: 100000, RowsPerSec: 1000, HealthChangedAt: now.Add(-time.Minute)},
	}}
	if err := repo.CreateMigrationTable(ctx, &table); err != nil {
		t.Fatal(err)
	}
	chunk := domain.MigrationChunk{ID: "rc38-recovery-chunk", TaskID: task.ID, TableID: table.ID, TopologyID: "dn-a", Start: 1, End: 1000}
	good := domain.ChunkResult{RowsWritten: 1000, BytesWritten: 100000, DurationMS: 1000}

	// A single good sample must not flap DEGRADED directly back to HEALTHY.
	if err := svc.recordTablePerformance(ctx, &chunk, good); err != nil {
		t.Fatal(err)
	}
	got, _ := repo.GetMigrationTable(ctx, table.ID)
	p := got.TopologyPerformance["dn-a"]
	if p.Health != "DEGRADED" || p.GoodStreak != 1 || repository.TopologyEffectiveConcurrencyCap(got, "dn-a") != 1 {
		t.Fatalf("first recovery sample profile=%+v effective_cap=%d", p, repository.TopologyEffectiveConcurrencyCap(got, "dn-a"))
	}

	if err := svc.recordTablePerformance(ctx, &chunk, good); err != nil {
		t.Fatal(err)
	}
	got, _ = repo.GetMigrationTable(ctx, table.ID)
	p = got.TopologyPerformance["dn-a"]
	if p.Health != "DEGRADED" || p.GoodStreak != 2 || p.RecoveryConcurrencyCap != 2 {
		t.Fatalf("second recovery sample profile=%+v", p)
	}

	for i := 0; i < 2; i++ {
		if err := svc.recordTablePerformance(ctx, &chunk, good); err != nil {
			t.Fatal(err)
		}
	}
	got, _ = repo.GetMigrationTable(ctx, table.ID)
	p = got.TopologyPerformance["dn-a"]
	if p.Health != "DEGRADED" || p.GoodStreak != 4 || p.RecoveryConcurrencyCap != 3 {
		t.Fatalf("ramped recovery profile=%+v", p)
	}

	for i := 0; i < 2; i++ {
		if err := svc.recordTablePerformance(ctx, &chunk, good); err != nil {
			t.Fatal(err)
		}
	}
	got, _ = repo.GetMigrationTable(ctx, table.ID)
	p = got.TopologyPerformance["dn-a"]
	if p.Health != "HEALTHY" || p.GoodStreak != 0 || p.RecoveryConcurrencyCap != 0 {
		t.Fatalf("healthy recovery profile=%+v", p)
	}
}

func TestRC38BadSampleResetsRecoveryRamp(t *testing.T) {
	t.Setenv("QMIGRATION_TOPOLOGY_DEGRADED_MAX_CONCURRENCY", "1")
	t.Setenv("QMIGRATION_TOPOLOGY_RECOVERY_MAX_CONCURRENCY", "4")
	t.Setenv("QMIGRATION_TOPOLOGY_RECOVERY_MIN_DEGRADED_SECONDS", "0")
	ctx := context.Background()
	repo := memory.New()
	svc := NewService(repo, nil)
	now := time.Now()
	task := domain.MigrationTask{ID: "rc38-reset-task", Status: domain.StatusFullMigrating, CreatedAt: now, UpdatedAt: now}
	_ = repo.CreateMigration(ctx, &task)
	table := domain.MigrationTable{ID: "rc38-reset-table", TaskID: task.ID, ProfileBytesPerSec: 100000, TopologyPerformance: map[string]domain.TableTopologyPerformance{
		"dn-a": {Health: "DEGRADED", SlowStreak: 1, GoodStreak: 4, RecoveryConcurrencyCap: 3, Samples: 10, BytesPerSec: 100000, RowsPerSec: 1000, HealthChangedAt: now.Add(-time.Minute)},
	}}
	_ = repo.CreateMigrationTable(ctx, &table)
	chunk := domain.MigrationChunk{ID: "rc38-reset-chunk", TaskID: task.ID, TableID: table.ID, TopologyID: "dn-a"}
	if err := svc.recordTablePerformance(ctx, &chunk, domain.ChunkResult{RowsWritten: 1, BytesWritten: 1, DurationMS: 100000}); err != nil {
		t.Fatal(err)
	}
	got, _ := repo.GetMigrationTable(ctx, table.ID)
	p := got.TopologyPerformance["dn-a"]
	if p.Health != "DEGRADED" || p.GoodStreak != 0 || p.RecoveryConcurrencyCap != 1 {
		t.Fatalf("bad sample must reset staged recovery: %+v", p)
	}
}

func TestRC38HalfOpenRecoveryIgnoresHistoricalTailAfterGoodProbe(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	svc := NewService(repo, nil)
	now := time.Now()
	task := domain.MigrationTask{ID: "rc38-half-historical-task", Status: domain.StatusFullMigrating, CreatedAt: now, UpdatedAt: now}
	_ = repo.CreateMigration(ctx, &task)
	table := domain.MigrationTable{ID: "rc38-half-historical-table", TaskID: task.ID, ProfileBytesPerSec: 100000, TopologyPerformance: map[string]domain.TableTopologyPerformance{
		"dn-a": {Health: "HALF_OPEN", SlowStreak: 5, Samples: 10, BytesPerSec: 100000, RowsPerSec: 1000, DurationSamplesMS: []int64{100000, 100000, 100000}, P99DurationMS: 100000},
	}}
	_ = repo.CreateMigrationTable(ctx, &table)
	chunk := domain.MigrationChunk{ID: "rc38-half-historical-chunk", TaskID: task.ID, TableID: table.ID, TopologyID: "dn-a"}
	if err := svc.recordTablePerformance(ctx, &chunk, domain.ChunkResult{RowsWritten: 1000, BytesWritten: 100000, DurationMS: 1000}); err != nil {
		t.Fatal(err)
	}
	got, _ := repo.GetMigrationTable(ctx, table.ID)
	p := got.TopologyPerformance["dn-a"]
	if p.Health != "DEGRADED" || p.GoodStreak != 1 {
		t.Fatalf("good HALF_OPEN probe must enter staged recovery despite historical P99: %+v", p)
	}
}

func TestRC39FaultDomainPeerRiskThrottlesHealthyRunningChunk(t *testing.T) {
	t.Setenv("QMIGRATION_TOPOLOGY_FAULT_DOMAIN_PROTECTION", "true")
	t.Setenv("QMIGRATION_TOPOLOGY_FAULT_DOMAIN_CRITICAL_BATCH_PCT", "40")
	t.Setenv("QMIGRATION_TOPOLOGY_FAULT_DOMAIN_CRITICAL_PAUSE_MS", "300")
	ctx := context.Background()
	repo := memory.New()
	svc := NewService(repo, connector.NewRegistry())
	task := domain.MigrationTask{ID: "rc39-throttle-task", Status: domain.StatusFullMigrating, BatchRows: 1000, Parallelism: 4, EffectiveParallelism: 4}
	_ = repo.CreateMigration(ctx, &task)
	table := domain.MigrationTable{
		ID: "rc39-throttle-table", TaskID: task.ID,
		Topology: []domain.TopologyPlacement{
			{ID: "dn-a", Labels: map[string]string{"region": "sg", "zone": "az-1", "rack": "r1"}},
			{ID: "dn-b", Labels: map[string]string{"region": "sg", "zone": "az-1", "rack": "r2"}},
		},
		TopologyPerformance: map[string]domain.TableTopologyPerformance{
			"dn-a": {Health: "HEALTHY"},
			"dn-b": {Health: "CIRCUIT_OPEN"},
		},
	}
	_ = repo.CreateMigrationTable(ctx, &table)
	chunk := &domain.MigrationChunk{ID: "rc39-running", TaskID: task.ID, TableID: table.ID, TopologyID: "dn-a"}
	control := svc.applyTopologyRunningPressure(ctx, &task, chunk, domain.ChunkControl{ReadBytesPerSec: 1000, WriteBytesPerSec: 1000, TargetBytesPerSec: 1000})
	if control.MaxBatchRows != 400 || control.PauseMS != 300 || control.ReadBytesPerSec != 400 || control.Level != "WARN" {
		t.Fatalf("unexpected fault-domain throttle: %+v", control)
	}
	if !strings.Contains(control.Reason, "fault-domain peer risk=3") {
		t.Fatalf("missing fault-domain throttle reason: %q", control.Reason)
	}
}

func TestRC40FaultDomainConvergesAlreadyRunningConcurrencyAndPreservesDomain(t *testing.T) {
	t.Setenv("QMIGRATION_TOPOLOGY_FAULT_DOMAIN_PROTECTION", "true")
	t.Setenv("QMIGRATION_TOPOLOGY_FAULT_DOMAIN_RUNNING_SHED", "true")
	t.Setenv("QMIGRATION_TOPOLOGY_FAULT_DOMAIN_CRITICAL_MAX_CONCURRENCY", "1")
	ctx := context.Background()
	repo := memory.New()
	svc := NewService(repo, nil)
	now := time.Now()
	task := domain.MigrationTask{ID: "rc40-domain-task", Status: domain.StatusFullMigrating, FullEngine: "qmigration", Parallelism: 4, EffectiveParallelism: 4, BatchRows: 100, CreatedAt: now, UpdatedAt: now}
	if err := repo.CreateMigration(ctx, &task); err != nil {
		t.Fatal(err)
	}
	table := domain.MigrationTable{
		ID: "rc40-domain-table", TaskID: task.ID, Engine: "qmigration",
		Topology: []domain.TopologyPlacement{
			{ID: "dn-a", Labels: map[string]string{"region": "sg", "zone": "az-1", "rack": "r1"}},
			{ID: "dn-b", Labels: map[string]string{"region": "sg", "zone": "az-1", "rack": "r2"}},
		},
		TopologyPerformance: map[string]domain.TableTopologyPerformance{
			"dn-a": {Health: "HEALTHY"},
			"dn-b": {Health: "CIRCUIT_OPEN"},
		},
	}
	if err := repo.CreateMigrationTable(ctx, &table); err != nil {
		t.Fatal(err)
	}
	fd := repository.CanonicalFaultDomain(map[string]string{"region": "sg", "zone": "az-1", "rack": "r1"})
	peerFD := repository.CanonicalFaultDomain(map[string]string{"region": "sg", "zone": "az-1", "rack": "r2"})
	chunks := []domain.MigrationChunk{
		{ID: "rc40-keep", TaskID: task.ID, TableID: table.ID, ChunkNo: 1, SplitType: "PK_RANGE", Start: 1, End: 1000, Status: domain.ChunkRunning, WorkerID: "rc40-w1", TopologyID: "dn-a", FaultDomain: fd, StartedAt: now.Add(-2 * time.Minute)},
		{ID: "rc40-yield", TaskID: task.ID, TableID: table.ID, ChunkNo: 2, SplitType: "PK_RANGE", Start: 1001, End: 2000, Status: domain.ChunkRunning, WorkerID: "rc40-w2", TopologyID: "dn-a", FaultDomain: fd, StartedAt: now.Add(-time.Minute)},
		{ID: "rc40-open-peer", TaskID: task.ID, TableID: table.ID, ChunkNo: 3, SplitType: "PK_RANGE", Start: 2001, End: 3000, Status: domain.ChunkPending, TopologyID: "dn-b", FaultDomain: peerFD},
	}
	if err := repo.CreateChunks(ctx, chunks); err != nil {
		t.Fatal(err)
	}
	_ = repo.UpsertWorker(ctx, &domain.Worker{ID: "rc40-w1", Hostname: "w1", CPU: 8, Status: "ONLINE", Capabilities: []string{"qmigration"}, LastHeartbeat: now})
	_ = repo.UpsertWorker(ctx, &domain.Worker{ID: "rc40-w2", Hostname: "w2", CPU: 8, Status: "ONLINE", Capabilities: []string{"qmigration"}, LastHeartbeat: now})

	keep, err := svc.RenewChunk(ctx, "rc40-w1", "rc40-keep", domain.ChunkProgress{CursorJSON: `{"after_pk":100}`, RowsWritten: 100, BytesWritten: 1024, LastBatchRows: 100})
	if err != nil {
		t.Fatal(err)
	}
	if keep.YieldAfterBatch {
		t.Fatalf("oldest healthy domain survivor must remain running: %+v", keep)
	}
	yield, err := svc.RenewChunk(ctx, "rc40-w2", "rc40-yield", domain.ChunkProgress{CursorJSON: `{"after_pk":1200}`, RowsRead: 200, RowsWritten: 200, BytesRead: 2048, BytesWritten: 2048, LastBatchRows: 100})
	if err != nil {
		t.Fatal(err)
	}
	if !yield.YieldAfterBatch || !strings.Contains(yield.YieldReason, "fault-domain shed") || !strings.Contains(yield.YieldReason, "scope=zone") {
		t.Fatalf("excess risky-domain chunk must yield: %+v", yield)
	}
	if err := svc.CompleteChunk(ctx, "rc40-w2", "rc40-yield", domain.ChunkResult{RowsRead: 200, RowsWritten: 200, BytesRead: 2048, BytesWritten: 2048, CursorJSON: `{"after_pk":1200}`, Yielded: true, YieldReason: yield.YieldReason}); err != nil {
		t.Fatal(err)
	}
	storedTask, _ := repo.GetMigration(ctx, task.ID)
	if storedTask.AdaptiveRunningYields != 1 || storedTask.AdaptiveFaultDomainYields != 1 {
		t.Fatalf("task telemetry=%+v", storedTask)
	}
	all, _ := repo.ListChunks(ctx, task.ID)
	running, pending := 0, 0
	for i := range all {
		if all[i].Status == domain.ChunkRunning {
			running++
		}
		if all[i].Status == domain.ChunkPending {
			pending++
			if all[i].TopologyID == "dn-a" && (all[i].FaultDomain["zone"] != fd["zone"] || all[i].FaultDomain["rack"] != fd["rack"]) {
				t.Fatalf("yield remainder lost source ownership/fault domain: %+v", all[i])
			}
		}
	}
	if running != 1 || pending == 0 {
		t.Fatalf("expected one running survivor plus pending remainder: %+v", all)
	}
	if _, err := repo.ClaimChunk(ctx, "rc40-w2", chunkLease, []string{"qmigration"}); !errors.Is(err, repository.ErrNoChunk) {
		t.Fatalf("pending domain remainder must remain blocked while critical cap occupied: %v", err)
	}
}

func TestRC44ComplexValidationStreamsChunkDescriptorsByPage(t *testing.T) {
	ctx := context.Background()
	base := memory.New()
	chunks := make([]domain.MigrationChunk, 0, 1200)
	for i := 0; i < 1200; i++ {
		chunks = append(chunks, domain.MigrationChunk{
			ID:          "rc44-chunk-" + strconv.Itoa(i),
			TaskID:      "rc44-task",
			TableID:     "rc44-table",
			ChunkNo:     i,
			SplitType:   "HASH",
			HashBucket:  i,
			HashBuckets: 1200,
			Status:      domain.ChunkSuccess,
		})
	}
	if err := base.CreateChunks(ctx, chunks); err != nil {
		t.Fatal(err)
	}
	tracked := &rc44ValidationPageRepo{Repository: base}
	tbl := domain.MigrationTable{
		ID:           "rc44-table",
		SourceSchema: "app",
		SourceTable:  "orders",
		Columns:      []domain.ColumnInfo{{Name: "id", DataType: "bigint", PrimaryKey: true}},
	}
	got, err := streamComplexValidationSourceChecksum(ctx, tracked, "rc44-task", rc44EmptyDataConnector{}, tbl, []string{"id"}, 100, 128)
	if err != nil {
		t.Fatal(err)
	}
	if got.Rows != 0 || got.Hash != strings.Repeat("0", 64) {
		t.Fatalf("unexpected empty-source checksum: %+v", got)
	}
	if tracked.listCalls != 0 {
		t.Fatalf("complex validation fell back to full ListChunks %d time(s)", tracked.listCalls)
	}
	if tracked.pageCalls < 10 {
		t.Fatalf("expected 1200 descriptors to require multiple bounded pages, calls=%d", tracked.pageCalls)
	}
	if tracked.maxLimit != 128 {
		t.Fatalf("page bound was not respected: max limit=%d", tracked.maxLimit)
	}
}

func TestRC49RoutingTransparentYieldRelocation(t *testing.T) {
	t.Setenv("QMIGRATION_RUNNING_CHUNK_RELOCATION", "1")
	ctx := context.Background()
	repo := memory.New()
	src := domain.DataSource{ID: "rc49-src", Type: domain.DataSourceTiDB, Database: "app", CreatedAt: time.Now()}
	if err := repo.CreateDataSource(ctx, &src); err != nil {
		t.Fatal(err)
	}
	svc := NewService(repo, nil)
	task := &domain.MigrationTask{ID: "rc49-task", SourceID: src.ID}
	table := &domain.MigrationTable{ID: "rc49-table", TaskID: task.ID, SourceSchema: "app", SourceTable: "orders", Topology: []domain.TopologyPlacement{
		{ID: "store-a", Kind: "TIDB_STORE", Labels: map[string]string{"tidb_store_id": "a", "zone": "z1"}},
		{ID: "store-b", Kind: "TIDB_STORE", Labels: map[string]string{"tidb_store_id": "b", "zone": "z2"}},
	}}
	from := &domain.MigrationChunk{ID: "rc49-old", TaskID: task.ID, TableID: table.ID, SplitType: "PK_RANGE", TopologyID: "store-a", CursorJSON: `{"after_pk":100}`}
	created := []domain.MigrationChunk{{ID: "rc49-new", TaskID: task.ID, TableID: table.ID, SplitType: "PK_RANGE", TopologyID: "store-a"}}
	got, err := svc.relocateYieldRemainders(ctx, task, table, from, created)
	if err != nil {
		t.Fatal(err)
	}
	if got[0].TopologyID != "store-b" || got[0].PlacementHint["tidb_store_id"] != "b" || got[0].FaultDomain["zone"] != "z2" {
		t.Fatalf("remainder was not relocated safely: %+v", got[0])
	}
}
