package api

import (
	"context"
	"dts/backend/internal/auth"
	"dts/backend/internal/domain"
	"errors"
	"net/http"
	"time"
)

var errSessionUnavailable = errors.New("session registry unavailable")

func publicAuthRoute(path string) bool {
	switch path {
	case "/api/v1/auth/login", "/api/v1/auth/logout", "/api/v1/auth/providers", "/api/v1/auth/oidc/start", "/api/v1/auth/oidc/callback":
		return true
	}
	return false
}

func (s *Server) issueUserSession(ctx context.Context, u *domain.User, role auth.Role, identity *auth.FederatedIdentity) (string, time.Time, error) {
	token, expiry, err := s.sessions.IssueWithIdentity(u.ID, u.Username, role, u.PasswordHash, identity)
	if err != nil {
		return "", time.Time{}, err
	}
	if err = s.repo.RecordSession(ctx, domain.UserSession{ID: auth.SessionID(token), UserID: u.ID, CreatedAt: time.Now().UTC(), ExpiresAt: expiry}); err != nil {
		return "", time.Time{}, errSessionUnavailable
	}
	return token, expiry, nil
}

func (s *Server) sessionOwner(r *http.Request) (string, error) {
	// User administration routes are protected by the same admin-only RBAC
	// rule as password resets. Self-service always derives ownership from
	// authenticated signed claims, never a user parameter.
	if id := r.PathValue("id"); id != "" {
		return id, nil
	}
	claims, err := s.sessions.Verify(requestCredential(r))
	if err != nil {
		return "", errors.New("user session required")
	}
	return claims.Subject, nil
}
func (s *Server) listSessions(w http.ResponseWriter, r *http.Request) {
	owner, err := s.sessionOwner(r)
	if err != nil {
		apiError(w, 403, err)
		return
	}
	records, err := s.repo.ListSessions(r.Context(), owner)
	if err != nil {
		apiError(w, 503, errSessionUnavailable)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	currentID := ""
	token := requestCredential(r)
	if claims, err := s.sessions.Verify(token); err == nil && claims.Subject == owner {
		currentID = auth.SessionID(token)
	}
	writeJSON(w, 200, map[string]any{"sessions": records, "current_id": currentID})
}
func (s *Server) revokeSession(w http.ResponseWriter, r *http.Request) {
	owner, err := s.sessionOwner(r)
	if err != nil {
		apiError(w, 403, err)
		return
	}
	id := r.PathValue("session_id")
	found, err := s.repo.RevokeSession(r.Context(), owner, id)
	if err != nil {
		apiError(w, 503, errSessionUnavailable)
		return
	}
	if !found {
		apiError(w, 404, errors.New("session not found"))
		return
	}
	if current, err := r.Cookie(sessionCookieName); err == nil && auth.SessionID(current.Value) == id {
		expired := sessionCookie(r)
		expired.MaxAge = -1
		http.SetCookie(w, expired)
	}
	s.audit(r, "REVOKE_SESSION", "user", owner, "session="+id)
	w.WriteHeader(http.StatusNoContent)
}
