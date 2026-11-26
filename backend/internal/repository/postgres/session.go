package postgres

import (
	"context"
	"errors"
	"time"

	"dts/backend/internal/domain"
)

func (s *Store) RecordSession(ctx context.Context, v domain.UserSession) error {
	// Concurrent logins can prune different expired rows without blocking each
	// other. Never resurrect a previously revoked session during legacy adoption.
	if v.CreatedAt.IsZero() || v.ExpiresAt.IsZero() {
		return errors.New("session creation and expiry are required")
	}
	const q = `WITH expired AS (SELECT id FROM dts_user_sessions WHERE expires_at<=now() ORDER BY expires_at LIMIT 128 FOR UPDATE SKIP LOCKED),
	prune AS (DELETE FROM dts_user_sessions WHERE id IN (SELECT id FROM expired))
	INSERT INTO dts_user_sessions (id,user_id,created_at,expires_at) VALUES ($1,$2,$3,$4) ON CONFLICT(id) DO NOTHING`
	_, err := s.db.QueryParams(ctx, q, v.ID, v.UserID, v.CreatedAt.UTC().Format(time.RFC3339Nano), v.ExpiresAt.UTC().Format(time.RFC3339Nano))
	return err
}
func (s *Store) SessionState(ctx context.Context, userID, id string) (bool, bool, error) {
	r, err := s.db.QueryParams(ctx, "SELECT NOT revoked AND expires_at>now() FROM dts_user_sessions WHERE id=$1 AND user_id=$2", id, userID)
	if err != nil {
		return false, false, err
	}
	if len(r.Rows) == 0 {
		return false, false, nil
	}
	return true, bv(r.Rows[0], 0), nil
}
func (s *Store) ListSessions(ctx context.Context, userID string) ([]domain.UserSession, error) {
	r, err := s.db.QueryParams(ctx, "SELECT id,"+epoch("created_at")+","+epoch("expires_at")+" FROM dts_user_sessions WHERE user_id=$1 AND NOT revoked AND expires_at>now() ORDER BY created_at DESC,id", userID)
	if err != nil {
		return nil, err
	}
	out := make([]domain.UserSession, 0, len(r.Rows))
	for _, row := range r.Rows {
		out = append(out, domain.UserSession{ID: sv(row, 0), UserID: userID, CreatedAt: tv(row, 1), ExpiresAt: tv(row, 2)})
	}
	return out, nil
}
func (s *Store) RevokeSession(ctx context.Context, userID, id string) (bool, error) {
	r, err := s.db.QueryParams(ctx, "UPDATE dts_user_sessions SET revoked=true WHERE user_id=$1 AND id=$2 RETURNING id", userID, id)
	if err != nil {
		return false, err
	}
	return len(r.Rows) > 0, nil
}
