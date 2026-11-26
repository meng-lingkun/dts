package postgres

import (
	"context"
	pg "dts/backend/internal/connector/postgres"
	"dts/backend/internal/repository"
	"fmt"
)

type admissionSQL interface {
	ExecSQL(context.Context, string) error
	QuerySQL(context.Context, string) (*pg.RawRows, error)
}

// Locks the owning lease rows in the same transaction as the metadata write.
// A concurrent takeover waits for this transaction, then sees its durable work.
func (s *Store) withAdmission(ctx context.Context, taskID, direction string, work func(admissionSQL) error) error {
	f, hasCDC := repository.CDCFenceFrom(ctx)
	m, hasManaged := repository.ManagedFenceFrom(ctx)
	if !hasCDC && !hasManaged {
		return work(s.db)
	}
	if direction == "" {
		direction = "forward"
	}
	if hasCDC && (f.TaskID != taskID || f.Direction != direction || f.Epoch <= 0) {
		return repository.ErrLeaseOwner
	}
	return s.db.InTransaction(ctx, func(tx *pg.SQLTransaction) error {
		if hasCDC {
			q := fmt.Sprintf("SELECT generation FROM cdc_spool_drain_leases WHERE task_id=%s AND direction=%s AND owner=%s AND generation=%d AND lease_until>clock_timestamp() FOR SHARE", qs(taskID), qs(direction), qs(f.Owner), f.Epoch)
			r, err := tx.QuerySQL(ctx, q)
			if err != nil {
				return err
			}
			if len(r.Rows) != 1 {
				return repository.ErrLeaseOwner
			}
		}
		if hasManaged {
			q := fmt.Sprintf("SELECT lease_generation FROM engine_jobs WHERE id=%s AND task_id=%s AND worker_id=%s AND lease_generation=%d AND lease_until>clock_timestamp() AND status IN ('RUNNING','STOP_REQUESTED') FOR SHARE", qs(m.JobID), qs(taskID), qs(m.WorkerID), m.Generation)
			r, err := tx.QuerySQL(ctx, q)
			if err != nil {
				return err
			}
			if len(r.Rows) != 1 {
				return repository.ErrLeaseOwner
			}
		}
		return work(tx)
	})
}
