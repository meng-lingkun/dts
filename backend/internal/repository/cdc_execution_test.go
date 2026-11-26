package repository_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"dts/backend/internal/domain"
	"dts/backend/internal/repository"
	"dts/backend/internal/repository/memory"
	"dts/backend/internal/repository/secure"
	"dts/backend/internal/repository/spoolfile"
	"dts/backend/internal/repository/spools3"
	"dts/backend/internal/security"
)

func TestCDCExecutionSurvivesProductionSpoolWrappers(t *testing.T) {
	for _, storage := range []string{"file", "s3"} {
		t.Run(storage, func(t *testing.T) {
			ctx := context.Background()
			base := memory.New()
			var inner repository.Repository
			var err error
			if storage == "file" {
				inner, err = spoolfile.New(base, spoolfile.Config{Root: t.TempDir()})
			} else {
				inner, err = spools3.New(base, spools3.Config{Endpoint: "http://127.0.0.1:1", Bucket: "spool", AccessKey: "test", SecretKey: "test"})
			}
			if err != nil {
				t.Fatal(err)
			}
			cipher, err := security.New("test-master-key")
			if err != nil {
				t.Fatal(err)
			}
			store := secure.New(inner, cipher)
			if ok, err := store.AcquireCDCSpoolDrainLease(ctx, "task", "forward", "execution-a", time.Minute); err != nil || !ok {
				t.Fatalf("acquire: ok=%v err=%v", ok, err)
			}
			if ok, err := base.AcquireCDCSpoolDrainLease(ctx, "task", "forward", "execution-b", time.Minute); err != nil || ok {
				t.Fatalf("lease hidden by wrapper: ok=%v err=%v", ok, err)
			}
			if err := store.RenewCDCSpoolDrainLease(ctx, "task", "forward", "execution-a", time.Minute); err != nil {
				t.Fatal(err)
			}
			if err := store.ReleaseCDCSpoolDrainLease(ctx, "task", "forward", "execution-a"); err != nil {
				t.Fatal(err)
			}
			if ok, err := base.AcquireCDCSpoolDrainLease(ctx, "task", "forward", "execution-b", time.Minute); err != nil || !ok {
				t.Fatalf("release hidden by wrapper: ok=%v err=%v", ok, err)
			}
			if p, err := store.LatestPendingCDCSpool(ctx, "task", "forward"); err != nil || p != nil {
				t.Fatalf("empty spool is not a storage failure: %+v %v", p, err)
			}
			if err := base.CreateCDCPosition(ctx, &domain.CDCPosition{ID: "p", TaskID: "task", Direction: "forward", PositionValue: "confirmed"}); err != nil {
				t.Fatal(err)
			}
			if p, err := store.LatestCDCPosition(ctx, "task", "forward"); err != nil || p == nil || p.PositionValue != "confirmed" {
				t.Fatalf("checkpoint hidden by wrapper: %+v %v", p, err)
			}
		})
	}
}

func TestCDCExecutionLeaseExpiryAndRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "metadata.json")
	store, err := memory.NewPersistent(path)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := store.AcquireCDCSpoolDrainLease(ctx, "task", "forward", "old", -time.Second); err != nil || !ok {
		t.Fatalf("seed expired lease: %v %v", ok, err)
	}
	if err := store.RenewCDCSpoolDrainLease(ctx, "task", "forward", "old", time.Minute); !errors.Is(err, repository.ErrLeaseOwner) {
		t.Fatalf("expired execution resurrected: %v", err)
	}
	if ok, err := store.AcquireCDCSpoolDrainLease(ctx, "task", "forward", "new", time.Minute); err != nil || !ok {
		t.Fatalf("reclaim: %v %v", ok, err)
	}
	if err := store.ReleaseCDCSpoolDrainLease(ctx, "task", "forward", "old"); !errors.Is(err, repository.ErrLeaseOwner) {
		t.Fatalf("old execution released successor: %v", err)
	}
	reopened, err := memory.NewPersistent(path)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := reopened.AcquireCDCSpoolDrainLease(ctx, "task", "forward", "restart", time.Minute); err != nil || ok {
		t.Fatalf("restart forgot live execution: %v %v", ok, err)
	}
	if ok, err := reopened.AcquireCDCSpoolDrainLease(ctx, "task", "reverse", "reverse", time.Minute); err != nil || !ok {
		t.Fatalf("forward lease blocked independent reverse direction: %v %v", ok, err)
	}
}
