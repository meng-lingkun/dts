package api

import (
	"errors"
	"net/http"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"dts/backend/internal/auth"
	"dts/backend/internal/domain"
	"dts/backend/internal/repository"
)

func (s *Server) personalTokenOwner(r *http.Request) (*domain.User, error) {
	if !s.authRequired.Load() || s.oidcLogin != nil && s.oidcLogin.only {
		return nil, errors.New("personal API tokens are disabled in this authentication mode")
	}
	claims, err := s.sessions.Verify(requestCredential(r))
	if err != nil {
		return nil, errors.New("sign in with a user session to manage personal API tokens")
	}
	u, err := s.repo.GetUser(r.Context(), claims.Subject)
	if err != nil {
		return nil, errSessionUnavailable
	}
	if !u.Enabled || auth.Role(u.Role) != claims.Role || !s.sessions.CredentialsMatch(claims, u.PasswordHash) {
		return nil, errors.New("user session is stale")
	}
	return u, nil
}

func (s *Server) createPersonalToken(w http.ResponseWriter, r *http.Request) {
	u, err := s.personalTokenOwner(r)
	if err != nil {
		apiError(w, 403, err)
		return
	}
	var req struct {
		Name      string    `json:"name"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	if err := decode(r, &req); err != nil {
		apiError(w, 400, err)
		return
	}
	req.Name = strings.TrimSpace(req.Name)
	now := time.Now().UTC()
	if req.Name == "" || utf8.RuneCountInString(req.Name) > 80 || strings.IndexFunc(req.Name, unicode.IsControl) >= 0 || !req.ExpiresAt.After(now) || req.ExpiresAt.After(now.Add(365*24*time.Hour)) {
		apiError(w, 400, errors.New("name must be 1–80 printable characters and expires_at must be in the next 365 days"))
		return
	}
	raw, id, err := s.sessions.NewPersonalToken()
	if err != nil {
		apiError(w, 500, errors.New("could not generate personal API token"))
		return
	}
	item := domain.PersonalToken{ID: id, UserID: u.ID, Name: req.Name, CreatedAt: now, ExpiresAt: req.ExpiresAt.UTC()}
	if err = s.repo.CreatePersonalToken(r.Context(), item, u.Revision); err != nil {
		if errors.Is(err, repository.ErrTokenLimit) || errors.Is(err, repository.ErrConflict) {
			apiError(w, 409, err)
		} else {
			apiError(w, 503, errSessionUnavailable)
		}
		return
	}
	s.audit(r, "CREATE_PERSONAL_TOKEN", "user", u.ID, "token_id="+id)
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, 201, map[string]any{"token": raw, "personal_token": item})
}
func (s *Server) listPersonalTokens(w http.ResponseWriter, r *http.Request) {
	u, err := s.personalTokenOwner(r)
	if err != nil {
		apiError(w, 403, err)
		return
	}
	items, err := s.repo.ListPersonalTokens(r.Context(), u.ID)
	if err != nil {
		apiError(w, 503, errSessionUnavailable)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, 200, map[string]any{"tokens": items})
}
func (s *Server) revokePersonalToken(w http.ResponseWriter, r *http.Request) {
	u, err := s.personalTokenOwner(r)
	if err != nil {
		apiError(w, 403, err)
		return
	}
	id := r.PathValue("token_id")
	found, err := s.repo.DeletePersonalToken(r.Context(), u.ID, id)
	if err != nil {
		apiError(w, 503, errSessionUnavailable)
		return
	}
	if !found {
		apiError(w, 404, errors.New("personal API token not found"))
		return
	}
	s.audit(r, "REVOKE_PERSONAL_TOKEN", "user", u.ID, "token_id="+id)
	w.WriteHeader(http.StatusNoContent)
}
