package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
	"qmigration/backend/internal/engine"
	"qmigration/backend/internal/repository/memory"
)

func TestDebeziumIngressRejectsMissingDurablePosition(t *testing.T) {
	h := newAuthTestServer(t)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/migrations/m1/cdc/debezium", strings.NewReader(`{"payload":{"after":{"id":1},"source":{"db":"app","table":"t"},"op":"c"}}`))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
}

func TestCanalIngressReturnsRetryableWhileFullLoadRuns(t *testing.T) {
	t.Setenv("QMIGRATION_RBAC_TOKENS", "")
	t.Setenv("QMIGRATION_API_TOKEN", "")
	t.Setenv("QMIGRATION_AUTH_REQUIRED", "")
	repo := memory.New()
	now := time.Now().UTC()
	if err := repo.CreateMigration(context.Background(), &domain.MigrationTask{
		ID: "m1", Name: "push", SourceID: "src", TargetID: "dst", Mode: domain.ModeFullAndIncremental,
		Status: domain.StatusFullMigrating, FullEngine: "native", CDCEngine: "canal", ChunkRows: 1000, BatchRows: 100, Parallelism: 1, MaxRetries: 3,
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	h := New(repo, connector.NewRegistry(), engine.NewRegistry()).Handler()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/migrations/m1/cdc/canal", strings.NewReader(`{"data":[{"id":"1"}],"database":"app","es":1000,"id":9,"table":"t","type":"INSERT"}`))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusTooEarly {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["retryable"] != true || body["task_status"] != string(domain.StatusFullMigrating) {
		t.Fatalf("body=%v", body)
	}
}

type externalCDCTestFactory struct{ target *externalCDCTestConnector }

func (f *externalCDCTestFactory) New(domain.DataSource) (connector.Connector, error) {
	return f.target, nil
}

type externalCDCTestConnector struct {
	writes int
	rows   [][]connector.Value
}

func (*externalCDCTestConnector) TestConnection(context.Context) error       { return nil }
func (*externalCDCTestConnector) GetVersion(context.Context) (string, error) { return "fake", nil }
func (*externalCDCTestConnector) ListSchemas(context.Context) ([]domain.SchemaInfo, error) {
	return nil, nil
}
func (*externalCDCTestConnector) ListTables(context.Context, string) ([]domain.TableInfo, error) {
	return nil, nil
}
func (*externalCDCTestConnector) GetTableMetadata(context.Context, string, string) (*domain.TableMetadata, error) {
	return nil, nil
}
func (*externalCDCTestConnector) Close() error { return nil }
func (*externalCDCTestConnector) ReadBatch(context.Context, connector.ReadBatchRequest) (*connector.RowBatch, error) {
	return &connector.RowBatch{}, nil
}
func (c *externalCDCTestConnector) WriteBatch(_ context.Context, r connector.WriteBatchRequest) (int64, error) {
	c.writes++
	c.rows = append(c.rows, r.Rows...)
	return int64(len(r.Rows)), nil
}
func (*externalCDCTestConnector) DeleteByKey(context.Context, connector.DeleteByKeyRequest) error {
	return nil
}

func TestCanalIngressAppliesAndDeduplicatesThroughCDCService(t *testing.T) {
	t.Setenv("QMIGRATION_RBAC_TOKENS", "")
	t.Setenv("QMIGRATION_API_TOKEN", "")
	t.Setenv("QMIGRATION_AUTH_REQUIRED", "")
	repo := memory.New()
	now := time.Now().UTC()
	src := &domain.DataSource{ID: "src", Name: "src", Type: domain.DataSourceMySQL, Host: "src", Port: 3306, CreatedAt: now, UpdatedAt: now}
	dst := &domain.DataSource{ID: "dst", Name: "dst", Type: domain.DataSourceMySQL, Host: "dst", Port: 3306, CreatedAt: now, UpdatedAt: now}
	if err := repo.CreateDataSource(context.Background(), src); err != nil {
		t.Fatal(err)
	}
	if err := repo.CreateDataSource(context.Background(), dst); err != nil {
		t.Fatal(err)
	}
	if err := repo.CreateMigration(context.Background(), &domain.MigrationTask{ID: "m2", Name: "push", SourceID: "src", TargetID: "dst", Mode: domain.ModeIncremental, Status: domain.StatusCDCCatchingUp, FullEngine: "native", CDCEngine: "canal", ChunkRows: 1000, BatchRows: 100, Parallelism: 1, MaxRetries: 3, CDCConflictMode: "SOURCE_WINS", CDCDDLMode: "REJECT", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	cols := []domain.ColumnInfo{{Name: "id", DataType: "bigint", PrimaryKey: true}, {Name: "name", DataType: "varchar(64)"}}
	if err := repo.CreateMigrationTable(context.Background(), &domain.MigrationTable{ID: "tbl", TaskID: "m2", SourceSchema: "app", SourceTable: "users", TargetSchema: "app", TargetTable: "users", PrimaryKey: "id", PrimaryKeys: []string{"id"}, TargetPrimaryKey: "id", TargetPrimaryKeys: []string{"id"}, Columns: cols, TargetColumns: cols, Status: "RUNNING"}); err != nil {
		t.Fatal(err)
	}
	fake := &externalCDCTestConnector{}
	reg := connector.NewRegistry()
	reg.Register(domain.DataSourceMySQL, &externalCDCTestFactory{target: fake})
	h := New(repo, reg, engine.NewRegistry()).Handler()
	body := `{"data":[{"id":"1","name":"alice"}],"database":"app","es":1000,"id":9,"table":"users","type":"INSERT"}`
	for attempt := 0; attempt < 2; attempt++ {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/migrations/m2/cdc/canal", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("attempt=%d status=%d body=%s", attempt, rr.Code, rr.Body.String())
		}
		if attempt == 1 {
			var result domain.CDCApplyResult
			if err := json.Unmarshal(rr.Body.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if !result.Duplicate {
				t.Fatalf("expected duplicate result: %+v", result)
			}
		}
	}
	if fake.writes != 1 || len(fake.rows) != 1 {
		t.Fatalf("writes=%d rows=%d", fake.writes, len(fake.rows))
	}
}

func setupPushCDCTest(t *testing.T, id string, mode domain.MigrationMode, status domain.MigrationStatus, totalChunks int) (*memory.Store, http.Handler, *externalCDCTestConnector) {
	t.Helper()
	t.Setenv("QMIGRATION_RBAC_TOKENS", "")
	t.Setenv("QMIGRATION_API_TOKEN", "")
	t.Setenv("QMIGRATION_AUTH_REQUIRED", "")
	repo := memory.New()
	now := time.Now().UTC()
	for _, ds := range []*domain.DataSource{
		{ID: "src-" + id, Name: "src", Type: domain.DataSourceMySQL, Host: "src", Port: 3306, CreatedAt: now, UpdatedAt: now},
		{ID: "dst-" + id, Name: "dst", Type: domain.DataSourceMySQL, Host: "dst", Port: 3306, CreatedAt: now, UpdatedAt: now},
	} {
		if err := repo.CreateDataSource(context.Background(), ds); err != nil {
			t.Fatal(err)
		}
	}
	if err := repo.CreateMigration(context.Background(), &domain.MigrationTask{
		ID: id, Name: "push", SourceID: "src-" + id, TargetID: "dst-" + id, Mode: mode, Status: status,
		FullEngine: "native", CDCEngine: "canal", ChunkRows: 1000, BatchRows: 100, Parallelism: 1, MaxRetries: 3,
		CDCConflictMode: "SOURCE_WINS", CDCDDLMode: "REJECT", TotalChunks: totalChunks, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	cols := []domain.ColumnInfo{{Name: "id", DataType: "bigint", PrimaryKey: true}, {Name: "name", DataType: "varchar(64)"}}
	if err := repo.CreateMigrationTable(context.Background(), &domain.MigrationTable{
		ID: "tbl-" + id, TaskID: id, SourceSchema: "app", SourceTable: "users", TargetSchema: "app", TargetTable: "users",
		PrimaryKey: "id", PrimaryKeys: []string{"id"}, TargetPrimaryKey: "id", TargetPrimaryKeys: []string{"id"}, Columns: cols, TargetColumns: cols, Status: "RUNNING",
	}); err != nil {
		t.Fatal(err)
	}
	fake := &externalCDCTestConnector{}
	reg := connector.NewRegistry()
	reg.Register(domain.DataSourceMySQL, &externalCDCTestFactory{target: fake})
	return repo, New(repo, reg, engine.NewRegistry()).Handler(), fake
}

func TestCanalIngressInitialIncrementalEventAppliesBeforeCheckpoint(t *testing.T) {
	repo, h, fake := setupPushCDCTest(t, "m3", domain.ModeIncremental, domain.StatusCDCInitializing, 0)
	body := `{"data":[{"id":"1","name":"first"}],"database":"app","es":1000,"id":10,"table":"users","type":"INSERT"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/migrations/m3/cdc/canal", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if fake.writes != 1 {
		t.Fatalf("first pushed record was not applied exactly once: writes=%d", fake.writes)
	}
	positions, err := repo.ListCDCPositions(context.Background(), "m3", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(positions) != 1 || positions[0].PositionValue == "" {
		t.Fatalf("expected one post-apply checkpoint, got %+v", positions)
	}
}

func TestCanalIngressTooEarlyDoesNotCheckpointUnappliedEvent(t *testing.T) {
	repo, h, fake := setupPushCDCTest(t, "m4", domain.ModeFullAndIncremental, domain.StatusCDCInitializing, 1)
	body := `{"data":[{"id":"1","name":"held"}],"database":"app","es":1000,"id":11,"table":"users","type":"INSERT"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/migrations/m4/cdc/canal", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusTooEarly {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	positions, err := repo.ListCDCPositions(context.Background(), "m4", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(positions) != 0 {
		t.Fatalf("unapplied 425 record must not advance checkpoint: %+v", positions)
	}
	if fake.writes != 0 {
		t.Fatalf("too-early record unexpectedly applied: writes=%d", fake.writes)
	}

	// Simulate full-load completion. Retrying the same upstream record must now
	// apply it and only then create its durable checkpoint.
	task, err := repo.GetMigration(context.Background(), "m4")
	if err != nil {
		t.Fatal(err)
	}
	task.Status = domain.StatusCDCCatchingUp
	if err := repo.UpdateMigration(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	req = httptest.NewRequest(http.MethodPost, "/api/v1/migrations/m4/cdc/canal", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("retry status=%d body=%s", rr.Code, rr.Body.String())
	}
	if fake.writes != 1 {
		t.Fatalf("retry did not apply held record exactly once: writes=%d", fake.writes)
	}
	positions, err = repo.ListCDCPositions(context.Background(), "m4", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(positions) != 1 {
		t.Fatalf("expected one checkpoint after successful retry, got %+v", positions)
	}
}
