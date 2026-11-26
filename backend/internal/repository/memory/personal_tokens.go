package memory

import (
	"context"
	"maps"
	"sort"
	"time"

	"dts/backend/internal/domain"
	"dts/backend/internal/repository"
)

func (s *Store) CreatePersonalToken(_ context.Context, v domain.PersonalToken, revision int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.users[v.UserID]
	if !ok || !u.Enabled || u.Revision != revision {
		return repository.ErrConflict
	}
	now := time.Now()
	next := maps.Clone(s.personalTokens)
	if next == nil {
		next = make(map[string]domain.PersonalToken)
	}
	count := 0
	for id, token := range next {
		if !token.ExpiresAt.After(now) {
			delete(next, id)
		} else if token.UserID == v.UserID {
			count++
		}
	}
	if count >= repository.MaxPersonalTokens {
		return repository.ErrTokenLimit
	}
	if _, exists := next[v.ID]; exists {
		return repository.ErrConflict
	}
	next[v.ID] = v
	old := s.personalTokens
	s.personalTokens = next
	if err := s.persistLocked(); err != nil {
		s.personalTokens = old
		return err
	}
	return nil
}

func (s *Store) PersonalTokenUser(_ context.Context, id string) (*domain.User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	token, ok := s.personalTokens[id]
	u := s.users[token.UserID]
	if !ok || !u.Enabled || !token.ExpiresAt.After(time.Now()) {
		return nil, repository.ErrNotFound
	}
	return &u, nil
}

func (s *Store) ListPersonalTokens(_ context.Context, owner string) ([]domain.PersonalToken, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []domain.PersonalToken{}
	now := time.Now()
	for _, v := range s.personalTokens {
		if v.UserID == owner && v.ExpiresAt.After(now) {
			out = append(out, v)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out, nil
}

func (s *Store) DeletePersonalToken(_ context.Context, owner, id string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, exists := s.personalTokens[id]
	if !exists || v.UserID != owner {
		return false, nil
	}
	delete(s.personalTokens, id)
	if err := s.persistLocked(); err != nil {
		s.personalTokens[id] = v
		return false, err
	}
	return true, nil
}
