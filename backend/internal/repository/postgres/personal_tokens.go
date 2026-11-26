package postgres

import (
	"context"
	"time"

	pg "dts/backend/internal/connector/postgres"
	"dts/backend/internal/domain"
	"dts/backend/internal/repository"
)

func (s *Store) CreatePersonalToken(ctx context.Context, token domain.PersonalToken, revision int64) error {
	return s.db.InTransaction(ctx, func(tx *pg.SQLTransaction) error {
		owner := token.UserID
		// Serialize issuance with password changes, disablement and other
		// issuances. Subsequent statements see the latest committed quota.
		r, err := tx.QueryParams(ctx, "SELECT revision FROM users WHERE enabled AND id=$1 FOR UPDATE", owner)
		if err != nil {
			return err
		}
		if len(r.Rows) != 1 || iv(r.Rows[0], 0) != revision {
			return repository.ErrConflict
		}
		if _, err = tx.QueryParams(ctx, "DELETE FROM dts_personal_tokens WHERE user_id=$1 AND expires_at<=now()", owner); err != nil {
			return err
		}
		r, err = tx.QueryParams(ctx, "SELECT count(*) FROM dts_personal_tokens WHERE user_id=$1", owner)
		if err != nil {
			return err
		}
		if iv(r.Rows[0], 0) >= repository.MaxPersonalTokens {
			return repository.ErrTokenLimit
		}
		_, err = tx.QueryParams(ctx, "INSERT INTO dts_personal_tokens(id,user_id,name,created_at,expires_at) VALUES ($1,$2,$3,$4,$5)", token.ID, owner, token.Name, token.CreatedAt.Format(time.RFC3339Nano), token.ExpiresAt.Format(time.RFC3339Nano))
		return err
	})
}

func (s *Store) PersonalTokenUser(ctx context.Context, id string) (*domain.User, error) {
	r, err := s.db.QueryParams(ctx, "SELECT id,username,role FROM users WHERE enabled AND id=(SELECT user_id FROM dts_personal_tokens WHERE id=$1 AND expires_at>now())", id)
	if err != nil {
		return nil, err
	}
	if len(r.Rows) == 0 {
		return nil, repository.ErrNotFound
	}
	row := r.Rows[0]
	return &domain.User{ID: sv(row, 0), Username: sv(row, 1), Role: sv(row, 2), Enabled: true}, nil
}

func (s *Store) ListPersonalTokens(ctx context.Context, owner string) ([]domain.PersonalToken, error) {
	r, err := s.db.QueryParams(ctx, "SELECT id,name,"+epoch("created_at")+","+epoch("expires_at")+" FROM dts_personal_tokens WHERE user_id=$1 AND expires_at>now() ORDER BY created_at DESC,id", owner)
	if err != nil {
		return nil, err
	}
	out := make([]domain.PersonalToken, 0, len(r.Rows))
	for _, row := range r.Rows {
		out = append(out, domain.PersonalToken{ID: sv(row, 0), UserID: owner, Name: sv(row, 1), CreatedAt: tv(row, 2), ExpiresAt: tv(row, 3)})
	}
	return out, nil
}

func (s *Store) DeletePersonalToken(ctx context.Context, owner, id string) (bool, error) {
	r, err := s.db.QueryParams(ctx, "DELETE FROM dts_personal_tokens WHERE user_id=$1 AND id=$2 RETURNING id", owner, id)
	if err != nil {
		return false, err
	}
	return len(r.Rows) > 0, nil
}
