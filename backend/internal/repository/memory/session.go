package memory

import (
	"context"
	"sort"
	"time"

	"dts/backend/internal/domain"
)

func (s *Store) RecordSession(_ context.Context, session domain.UserSession) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sessions == nil {
		s.sessions = make(map[string]domain.UserSession)
	}
	now := time.Now()
	for id, item := range s.sessions {
		if !item.ExpiresAt.After(now) {
			delete(s.sessions, id)
		}
	}
	if _, ok := s.sessions[session.ID]; ok {
		return nil
	}
	s.sessions[session.ID] = session
	if err := s.persistLocked(); err != nil {
		delete(s.sessions, session.ID)
		return err
	}
	return nil
}

func (s *Store) SessionState(_ context.Context, userID, id string) (bool, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.sessions[id]
	exists := ok && v.UserID == userID
	return exists, exists && !v.Revoked && v.ExpiresAt.After(time.Now()), nil
}

func (s *Store) ListSessions(_ context.Context, userID string) ([]domain.UserSession, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []domain.UserSession{}
	for _, v := range s.sessions {
		if v.UserID == userID && !v.Revoked && v.ExpiresAt.After(time.Now()) {
			out = append(out, v)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out, nil
}

func (s *Store) RevokeSession(_ context.Context, userID, id string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.sessions[id]
	if !ok || v.UserID != userID {
		return false, nil
	}
	if v.Revoked {
		return true, nil
	}
	v.Revoked = true
	s.sessions[id] = v
	if err := s.persistLocked(); err != nil {
		v.Revoked = false
		s.sessions[id] = v
		return false, err
	}
	return true, nil
}
