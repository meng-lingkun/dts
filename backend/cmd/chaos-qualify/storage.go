package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"
	"time"

	"qmigration/backend/internal/domain"
	"qmigration/backend/internal/faultinject"
	"qmigration/backend/internal/repository/memory"
	"qmigration/backend/internal/repository/spoolfile"
)

func storageENOSPCCheck() check {
	return runCheck("file-spool-enospc", func() (map[string]any, error) {
		root, err := os.MkdirTemp("", "qmigration-chaos-enospc-")
		if err != nil {
			return nil, err
		}
		defer os.RemoveAll(root)
		base := memory.New()
		files, err := spoolfile.New(base, spoolfile.Config{Root: root, WarnUsedPct: 99, CriticalUsedPct: 100, AppliedRetention: time.Hour})
		if err != nil {
			return nil, err
		}
		oldEnable, oldPlan := os.Getenv(faultinject.EnvEnable), os.Getenv(faultinject.EnvPlan)
		defer func() {
			if oldEnable == "" {
				_ = os.Unsetenv(faultinject.EnvEnable)
			} else {
				_ = os.Setenv(faultinject.EnvEnable, oldEnable)
			}
			if oldPlan == "" {
				_ = os.Unsetenv(faultinject.EnvPlan)
			} else {
				_ = os.Setenv(faultinject.EnvPlan, oldPlan)
			}
			faultinject.ResetForTest()
		}()
		_ = os.Setenv(faultinject.EnvEnable, "1")
		_ = os.Setenv(faultinject.EnvPlan, "cdc.spool.file.before_write=1@ENOSPC")
		faultinject.ResetForTest()
		err = files.CreateCDCSpool(context.Background(), &domain.CDCSpoolRecord{ID: "enospc", TaskID: "task", Direction: "forward", EventsCiphertext: "ciphertext", EventCount: 1, Status: domain.CDCSpoolPending, CreatedAt: time.Now()})
		if !errors.Is(err, syscall.ENOSPC) {
			return nil, fmt.Errorf("expected syscall.ENOSPC identity, got %v", err)
		}
		stats, statErr := base.CDCSpoolStats(context.Background(), "task", "forward")
		if statErr != nil {
			return nil, statErr
		}
		if stats.PendingTransactions != 0 {
			return nil, fmt.Errorf("metadata advanced during ENOSPC: pending_transactions=%d", stats.PendingTransactions)
		}
		return map[string]any{"errno": "ENOSPC", "pending_transactions": stats.PendingTransactions}, nil
	})
}
