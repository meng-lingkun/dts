package postgres

import (
	"context"
	"fmt"
	"time"

	"dts/backend/internal/domain"
	"dts/backend/internal/repository"
)

var _ repository.CDCExecutionLeaser = (*Store)(nil)

func (s *Store) CDCExecutionEpoch(ctx context.Context, taskID, direction, owner string) (int64, error) {
	r, err := s.db.QuerySQL(ctx, "SELECT generation FROM cdc_spool_drain_leases WHERE task_id="+qs(taskID)+" AND direction="+qs(direction)+" AND owner="+qs(owner)+" AND lease_until>now()")
	if err != nil {
		return 0, err
	}
	if len(r.Rows) != 1 || iv(r.Rows[0], 0) <= 0 {
		return 0, repository.ErrLeaseOwner
	}
	return iv(r.Rows[0], 0), nil
}

func (s *Store) RenewCDCSpoolDrainLease(ctx context.Context, taskID, direction, owner string, ttl time.Duration) error {
	q := fmt.Sprintf(`UPDATE cdc_spool_drain_leases SET lease_until=now()+make_interval(secs => %d),updated_at=now()
WHERE task_id=%s AND direction=%s AND owner=%s AND lease_until > now() RETURNING owner`, controlLeaseSeconds(ttl), qs(taskID), qs(direction), qs(owner))
	r, err := s.db.QuerySQL(ctx, q)
	if err != nil {
		return err
	}
	if len(r.Rows) == 0 {
		return repository.ErrLeaseOwner
	}
	return nil
}

func (s *Store) LatestCDCPosition(ctx context.Context, taskID, direction string) (*domain.CDCPosition, error) {
	filter := "direction=" + qs(direction)
	if direction == "forward" {
		filter = "(direction='forward' OR direction='' OR direction IS NULL)"
	}
	q := "SELECT id,task_id,COALESCE(NULLIF(direction,''),'forward'),COALESCE(database_type,''),COALESCE(position_type,''),COALESCE(position_value,''),COALESCE(resource,''),source_timestamp_ms,apply_timestamp_ms,lag_ms,events_total,events_pending," + epoch("recorded_at") +
		" FROM cdc_positions WHERE task_id=" + qs(taskID) + " AND " + filter + " ORDER BY recorded_at DESC,id DESC LIMIT 1"
	r, err := s.db.QuerySQL(ctx, q)
	if err != nil {
		return nil, err
	}
	if len(r.Rows) == 0 {
		return nil, nil
	}
	x := r.Rows[0]
	return &domain.CDCPosition{ID: sv(x, 0), TaskID: sv(x, 1), Direction: sv(x, 2), DatabaseType: sv(x, 3), PositionType: sv(x, 4), PositionValue: sv(x, 5), Resource: sv(x, 6), SourceTimestampMS: iv(x, 7), ApplyTimestampMS: iv(x, 8), LagMS: iv(x, 9), EventsTotal: iv(x, 10), EventsPending: iv(x, 11), RecordedAt: tv(x, 12)}, nil
}
