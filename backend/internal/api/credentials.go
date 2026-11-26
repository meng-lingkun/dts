package api

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"strings"
	"time"

	"dts/backend/internal/auth"
	"dts/backend/internal/domain"
	"dts/backend/internal/repository"
)

func requestCredential(r *http.Request) string {
	if token := r.Header.Get("X-DTS-API-Token"); token != "" {
		return token
	}
	if value := r.Header.Get("Authorization"); strings.HasPrefix(value, "Bearer ") {
		return strings.TrimSpace(strings.TrimPrefix(value, "Bearer "))
	}
	if r.URL.Path == "/api/v1/ws" {
		for _, proto := range strings.Split(r.Header.Get("Sec-WebSocket-Protocol"), ",") {
			proto = strings.TrimSpace(proto)
			if strings.HasPrefix(proto, "auth.") {
				if decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(proto, "auth.")); err == nil {
					return string(decoded)
				}
			}
		}
	}
	if cookie, err := r.Cookie(sessionCookieName); err == nil {
		return cookie.Value
	}
	return ""
}

func (s *Server) authenticateRequest(r *http.Request) (auth.Role, string, error) {
	ctx := r.Context()
	token := requestCredential(r)
	if token == "" {
		return "", "", errors.New("authentication required")
	}
	if role, ok := s.staticTokens.Authenticate(token); ok {
		if s.oidcLogin != nil && s.oidcLogin.only {
			return "", "", errors.New("OIDC session required")
		}
		return role, "token:" + string(role), nil
	}
	if strings.HasPrefix(token, auth.PersonalTokenPrefix) {
		if s.oidcLogin != nil && s.oidcLogin.only || r.Header.Get("Authorization") != "Bearer "+token || r.Header.Get("X-DTS-API-Token") != "" || r.URL.Path == "/api/v1/ws" || isWorkerInternalRequest(r) {
			return "", "", errors.New("personal API token is not accepted here")
		}
		if strings.HasPrefix(r.URL.Path, "/api/v1/users/") && strings.Contains(r.URL.Path, "/sessions") {
			return "", "", errors.New("user session required")
		}
		u, err := s.repo.PersonalTokenUser(ctx, s.sessions.PersonalTokenID(token))
		if errors.Is(err, repository.ErrNotFound) {
			return "", "", errors.New("invalid or expired credentials")
		}
		if err != nil {
			return "", "", errSessionUnavailable
		}
		return auth.Role(u.Role), u.Username, nil
	}
	claims, err := s.sessions.Verify(token)
	if err != nil {
		return "", "", errors.New("invalid or expired credentials")
	}
	c := s.oidcLogin
	if identity := claims.Federated; identity != nil {
		if c == nil || c.configErr != nil || c.issuer != identity.Issuer || c.bindings[c.issuer][identity.Subject] != claims.Subject || c.assurance != "" && c.assurance != identity.Assurance {
			return "", "", errors.New("OIDC session binding or assurance has changed")
		}
	} else if c != nil && c.only {
		return "", "", errors.New("OIDC session required")
	}
	u, err := s.repo.GetUser(ctx, claims.Subject)
	if err != nil && !errors.Is(err, repository.ErrNotFound) {
		return "", "", errSessionUnavailable
	}
	if err != nil || u == nil || !u.Enabled || auth.Role(u.Role) != claims.Role || u.Username != claims.Username || !s.sessions.CredentialsMatch(claims, u.PasswordHash) {
		return "", "", errors.New("user is disabled or session is stale")
	}
	if err := s.requireActiveSession(ctx, token, claims); err != nil {
		return "", "", err
	}
	return claims.Role, u.Username, nil
}

func (s *Server) requireActiveSession(ctx context.Context, token string, claims auth.SessionClaims) error {
	id := auth.SessionID(token)
	exists, active, err := s.repo.SessionState(ctx, claims.Subject, id)
	if err != nil {
		return errSessionUnavailable
	}
	if !exists && claims.Nonce == "" {
		// Adopt an unexpired pre-registry token the first time it is used. An
		// existing revocation always wins over a concurrent adoption.
		err = s.repo.RecordSession(ctx, domain.UserSession{ID: id, UserID: claims.Subject, CreatedAt: time.Now().UTC(), ExpiresAt: time.Unix(claims.Expires, 0).UTC()})
		if err != nil {
			return errSessionUnavailable
		}
		_, active, err = s.repo.SessionState(ctx, claims.Subject, id)
		if err != nil {
			return errSessionUnavailable
		}
	}
	if !active {
		return errors.New("session has expired or was revoked")
	}
	return nil
}
