package migration

import (
	"context"
	"errors"
	"testing"
	"time"

	"dts/backend/internal/connector"
	"dts/backend/internal/domain"
	"dts/backend/internal/repository/memory"
)

type freezeState struct {
	position string
	writers  bool
}
type freezeFactory struct{ state *freezeState }
type freezeConnector struct {
	fakeConnector
	state *freezeState
}

func (f freezeFactory) New(ds domain.DataSource) (connector.Connector, error) {
	return freezeConnector{fakeConnector: fakeConnector{typ: ds.Type}, state: f.state}, nil
}
func (c freezeConnector) CurrentCDCPosition(context.Context) (*domain.CDCPosition, error) {
	if c.typ == domain.DataSourcePostgreSQL {
		return &domain.CDCPosition{PositionType: "LSN", PositionValue: c.state.position}, nil
	}
	return &domain.CDCPosition{PositionType: "BINLOG", PositionValue: c.state.position}, nil
}
func (c freezeConnector) VerifyCDCQuiescence(context.Context) error {
	if c.state.writers {
		return errors.New("in-flight write transaction")
	}
	return nil
}

func lifecycleFixture(t *testing.T) (*Service, *memory.Store, *freezeState) {
	t.Helper()
	ctx := context.Background()
	r := memory.New()
	state := &freezeState{position: "mysql-bin.000001:100"}
	reg := connector.NewRegistry()
	reg.Register(domain.DataSourceMySQL, freezeFactory{state})
	for _, id := range []string{"src", "dst"} {
		if err := r.CreateDataSource(ctx, &domain.DataSource{ID: id, Type: domain.DataSourceMySQL}); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.CreateMigration(ctx, &domain.MigrationTask{ID: "task", SourceID: "src", TargetID: "dst", Mode: domain.ModeIncremental, Status: domain.StatusReadyCutover}); err != nil {
		t.Fatal(err)
	}
	if err := r.CreateCDCPosition(ctx, &domain.CDCPosition{ID: "cp", TaskID: "task", Direction: "forward", PositionType: "BINLOG", PositionValue: "mysql-bin.000001:90"}); err != nil {
		t.Fatal(err)
	}
	return NewService(r, reg), r, state
}
func freezeRequest() domain.WriteFreezeRequest {
	return domain.WriteFreezeRequest{WritesFrozen: true, Evidence: "business maintenance freeze ticket TEST-123", IdempotencyKey: "freeze-test-operation"}
}

func TestManualPostgresFreezeRequiresAppliedObservationAfterMarker(t *testing.T) {
	ctx := context.Background()
	svc, r, state := lifecycleFixture(t)
	source, err := r.GetDataSource(ctx, "src")
	if err != nil {
		t.Fatal(err)
	}
	source.Type = domain.DataSourcePostgreSQL
	if err := r.UpdateDataSource(ctx, source); err != nil {
		t.Fatal(err)
	}
	state.position = "0/200"
	svc.connectors.Register(domain.DataSourcePostgreSQL, freezeFactory{state})
	op, err := svc.ConfirmWriteFreeze(ctx, "task", "forward", "admin", freezeRequest())
	if err != nil {
		t.Fatal(err)
	}
	if op.Barrier.PositionValue != "0/123" || op.SourceHead.PositionValue != "0/200" {
		t.Fatalf("expected a write between marker and physical observation: %+v", op)
	}
	if err := svc.RecordCDCProgress(ctx, "task", &op.Barrier); err != nil {
		t.Fatal(err)
	}
	if err := svc.Cutover(ctx, "task"); !errors.Is(err, ErrWatermarkBehind) {
		t.Fatalf("cutover accepted a checkpoint before the observed write: %v", err)
	}
	if err := svc.RecordCDCProgress(ctx, "task", &op.SourceHead); err != nil {
		t.Fatal(err)
	}
	if err := svc.Cutover(ctx, "task"); err != nil {
		t.Fatal(err)
	}
}

func TestFinalCutoverRequiresFrozenSourceAndAppliedWatermark(t *testing.T) {
	ctx := context.Background()
	svc, r, _ := lifecycleFixture(t)
	if err := svc.Cutover(ctx, "task"); !errors.Is(err, ErrFreezeRequired) {
		t.Fatalf("missing freeze accepted: %v", err)
	}
	op, err := svc.ConfirmWriteFreeze(ctx, "task", "forward", "admin", freezeRequest())
	if err != nil {
		t.Fatal(err)
	}
	again, err := svc.ConfirmWriteFreeze(ctx, "task", "forward", "admin", freezeRequest())
	if err != nil || !again.ConfirmedAt.Equal(op.ConfirmedAt) {
		t.Fatalf("confirmation is not idempotent: %+v %v", again, err)
	}
	if err := svc.Cutover(ctx, "task"); !errors.Is(err, ErrWatermarkBehind) {
		t.Fatalf("target behind barrier accepted: %v", err)
	}
	if err := svc.RecordCDCProgress(ctx, "task", &domain.CDCPosition{PositionType: "BINLOG", PositionValue: "mysql-bin.000001:100"}); err != nil {
		t.Fatal(err)
	}
	if err := svc.Cutover(ctx, "task"); err != nil {
		t.Fatal(err)
	}
	m, _ := r.GetMigration(ctx, "task")
	if m.Status != domain.StatusFinished || m.CutoverOperation.Phase != "COMPLETED" || m.CutoverOperation.Applied.PositionValue != op.Barrier.PositionValue || len(m.CutoverOperation.Steps) != 4 {
		t.Fatalf("missing durable completion proof: %+v", m.CutoverOperation)
	}
	for _, step := range m.CutoverOperation.Steps {
		if step.Status != "DONE" {
			t.Fatalf("unfinished saga step: %+v", step)
		}
	}
}

func TestFreezeRejectsActiveWritersSourceDriftAndExpiry(t *testing.T) {
	for _, scenario := range []string{"writers", "source-drift", "expiry", "endpoint-drift"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			svc, r, state := lifecycleFixture(t)
			if scenario == "writers" {
				state.writers = true
				if _, err := svc.ConfirmWriteFreeze(ctx, "task", "forward", "admin", freezeRequest()); err == nil {
					t.Fatal("active write transaction accepted")
				}
				return
			}
			if _, err := svc.ConfirmWriteFreeze(ctx, "task", "forward", "admin", freezeRequest()); err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "source-drift":
				state.position = "mysql-bin.000001:101"
			case "expiry":
				m, _ := r.GetMigration(ctx, "task")
				m.CutoverOperation.ExpiresAt = time.Now().Add(-time.Second)
				if err := r.UpdateMigration(ctx, m); err != nil {
					t.Fatal(err)
				}
			case "endpoint-drift":
				ds, _ := r.GetDataSource(ctx, "src")
				ds.Host = "other-source"
				if err := r.UpdateDataSource(ctx, ds); err != nil {
					t.Fatal(err)
				}
			}
			if err := svc.Cutover(ctx, "task"); !errors.Is(err, ErrFreezeRequired) {
				t.Fatalf("invalid freeze proof accepted: %v", err)
			}
			m, _ := r.GetMigration(ctx, "task")
			if m.Status != domain.StatusReadyCutover {
				t.Fatal("invalid proof mutated lifecycle")
			}
		})
	}
}

func TestLifecycleRecoveryRetriesSafeStepsAndReviewsDDL(t *testing.T) {
	ctx := context.Background()
	svc, r, _ := lifecycleFixture(t)
	if err := svc.RecordCDCProgress(ctx, "task", &domain.CDCPosition{PositionType: "BINLOG", PositionValue: "mysql-bin.000001:100"}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.ConfirmWriteFreeze(ctx, "task", "forward", "admin", freezeRequest()); err != nil {
		t.Fatal(err)
	}
	m, _ := r.GetMigration(ctx, "task")
	m.CutoverOperation.Phase = "STOP_READER"
	m.CutoverOperation.Steps = []domain.ControlStep{{Name: "STOP_READER", Status: "RUNNING"}}
	if err := r.UpdateMigration(ctx, m); err != nil {
		t.Fatal(err)
	}
	if err := svc.RecoverInterrupted(ctx); err != nil {
		t.Fatal(err)
	}
	m, _ = r.GetMigration(ctx, "task")
	if m.Status != domain.StatusFinished || m.CutoverOperation.Steps[0].Status != "INTERRUPTED" {
		t.Fatalf("safe step did not resume: %+v", m.CutoverOperation)
	}
	m.Status = domain.StatusCutoverRunning
	m.CutoverOperation.Phase = "SYNC_GENERATED_STATE"
	if err := r.UpdateMigration(ctx, m); err != nil {
		t.Fatal(err)
	}
	if err := svc.RecoverInterrupted(ctx); err != nil {
		t.Fatal(err)
	}
	m, _ = r.GetMigration(ctx, "task")
	if m.Status != domain.StatusFailed || m.CutoverOperation.Phase != "NEEDS_REVIEW" {
		t.Fatalf("uncertain DDL was replayed: %+v", m.CutoverOperation)
	}
}

func TestWatermarkProofUsesNumericAndGTIDSemantics(t *testing.T) {
	sid := "24bc7850-2c16-11e6-a073-0242ac110002"
	for _, tc := range []struct {
		kind, a, b string
		covers     bool
		bad        bool
	}{
		{"LSN", "1/0", "0/FFFFFFFF", true, false}, {"LSN", "0/FF", "0/100", false, false}, {"LSN", "invalid", "0/1", false, true},
		{"BINLOG", "bin.000010:4", "bin.000009:999", true, false}, {"BINLOG", "other.000010:4", "bin.000009:999", false, true},
		{"GTID", sid + ":1-100", sid + ":1-90", true, false}, {"GTID", sid + ":1-80:90-100", sid + ":1-100", false, false}, {"EXTERNAL_MANAGED", "100", "99", false, true},
	} {
		ok, err := watermarkCovers(domain.CDCPosition{PositionType: tc.kind, PositionValue: tc.a}, domain.CDCPosition{PositionType: tc.kind, PositionValue: tc.b})
		if ok != tc.covers || (err != nil) != tc.bad {
			t.Errorf("%+v: got %v %v", tc, ok, err)
		}
	}
}
