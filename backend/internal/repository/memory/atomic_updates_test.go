package memory

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"dts/backend/internal/domain"
	"dts/backend/internal/repository"
)

func TestAtomicUserUpdatesCannotRestoreStalePasswordOrRole(t *testing.T) {
	ctx := context.Background()
	s := New()
	u := &domain.User{ID: "u", Username: "user", Role: "viewer", Enabled: true, PasswordHash: "old"}
	if err := s.CreateUser(ctx, u); err != nil {
		t.Fatal(err)
	}
	password, _ := s.GetUser(ctx, "u")
	profile, _ := s.GetUser(ctx, "u")
	password.PasswordHash = "new"
	if err := s.UpdateUser(ctx, password); err != nil {
		t.Fatal(err)
	}
	profile.Role = "operator"
	if err := s.UpdateUser(ctx, profile); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("stale security update accepted: %v", err)
	}
	actual, _ := s.GetUser(ctx, "u")
	if actual.PasswordHash != "new" || actual.Role != "viewer" || actual.Revision != 1 {
		t.Fatalf("security state lost: %+v", actual)
	}
}

func TestConcurrentAdminDisablePreservesOneEnabledAdmin(t *testing.T) {
	ctx := context.Background()
	s := New()
	for _, id := range []string{"a", "b"} {
		if err := s.CreateUser(ctx, &domain.User{ID: id, Username: id, Role: "admin", Enabled: true}); err != nil {
			t.Fatal(err)
		}
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, id := range []string{"a", "b"} {
		u, _ := s.GetUser(ctx, id)
		u.Enabled = false
		wg.Add(1)
		go func() { defer wg.Done(); <-start; results <- s.UpdateUser(ctx, u) }()
	}
	close(start)
	wg.Wait()
	close(results)
	success, rejected := 0, 0
	for err := range results {
		if err == nil {
			success++
		} else if errors.Is(err, repository.ErrLastAdmin) {
			rejected++
		} else {
			t.Fatal(err)
		}
	}
	if success != 1 || rejected != 1 {
		t.Fatalf("success=%d rejected=%d", success, rejected)
	}
}

func TestMigrationCASRejectsStaleTerminalStateOverwrite(t *testing.T) {
	ctx := context.Background()
	s := New()
	m := &domain.MigrationTask{ID: "m", Status: domain.StatusCDCCatchingUp}
	if err := s.CreateMigration(ctx, m); err != nil {
		t.Fatal(err)
	}
	controller, _ := s.GetMigration(ctx, "m")
	finisher, _ := s.GetMigration(ctx, "m")
	finisher.Status = domain.StatusFinished
	if err := s.UpdateMigration(ctx, finisher); err != nil {
		t.Fatal(err)
	}
	controller.CDCLagMS = 50
	if err := s.UpdateMigration(ctx, controller); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("stale snapshot reopened completed task: %v", err)
	}
	actual, _ := s.GetMigration(ctx, "m")
	if actual.Status != domain.StatusFinished {
		t.Fatal(actual.Status)
	}
}

func TestIAMAndLifecycleEvidenceSurvivePersistentRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.json")
	s, err := NewPersistent(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CreateUser(ctx, &domain.User{ID: "u", Username: "user", PasswordHash: "hashed-credential", Role: "admin", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	m := &domain.MigrationTask{ID: "m", CutoverOperation: &domain.LifecycleOperation{ID: "op", Phase: "STOP_READER", Steps: []domain.ControlStep{{Name: "STOP_READER", Status: "RUNNING"}}}}
	if err := s.CreateMigration(ctx, m); err != nil {
		t.Fatal(err)
	}
	copy, _ := s.GetMigration(ctx, "m")
	copy.CutoverOperation.Steps[0].Status = "FORGED"
	reopened, err := NewPersistent(path)
	if err != nil {
		t.Fatal(err)
	}
	u, err := reopened.GetUser(ctx, "u")
	if err != nil || u.PasswordHash != "hashed-credential" {
		t.Fatalf("restart lost password verifier: %+v %v", u, err)
	}
	actual, _ := reopened.GetMigration(ctx, "m")
	if actual.CutoverOperation.Steps[0].Status != "RUNNING" {
		t.Fatalf("durable evidence mutated through shallow snapshot: %+v", actual)
	}
}
