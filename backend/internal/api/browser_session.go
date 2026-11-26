package api

import (
	"errors"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"dts/backend/internal/auth"
)

const sessionCookieName = "dts_session"
const browserHeader = "X-DTS-Browser"

// The browser supplies Origin for POST and WebSocket. The web proxy preserves
// Host (including a NodePort). Never derive the public authority from forwarding
// headers provided by the client.
func sameBrowserOrigin(r *http.Request) bool {
	origin, err := url.Parse(r.Header.Get("Origin"))
	if err != nil || origin.User != nil || origin.Path != "" || origin.RawQuery != "" || origin.Fragment != "" || !strings.EqualFold(origin.Host, r.Host) {
		return false
	}
	if r.TLS != nil || strings.HasPrefix(os.Getenv("DTS_CORS_ORIGIN"), "https://") {
		return origin.Scheme == "https"
	}
	return origin.Scheme == "http" || origin.Scheme == "https"
}

func allowBrowserWrite(w http.ResponseWriter, r *http.Request) bool {
	if r.Header.Get(browserHeader) != "1" || !sameBrowserOrigin(r) {
		apiError(w, http.StatusForbidden, errors.New("same-origin browser request required"))
		return false
	}
	return true
}

func sessionCookie(r *http.Request) *http.Cookie {
	return &http.Cookie{Name: sessionCookieName, Path: "/api/v1/", HttpOnly: true,
		Secure:   r.TLS != nil || strings.HasPrefix(r.Header.Get("Origin"), "https://"),
		SameSite: http.SameSiteStrictMode}
}

func (s *Server) browserLogout(w http.ResponseWriter, r *http.Request) {
	c, err := r.Cookie(sessionCookieName)
	token := requestCredential(r)
	if err == nil || r.Header.Get(browserHeader) == "1" || token == "" {
		if !allowBrowserWrite(w, r) {
			return
		}
		if c != nil {
			token = c.Value
		}
	}
	if claims, err := s.sessions.Verify(token); err == nil {
		// A legacy cookie can log out before making its first authenticated
		// request. Register it before revoking so all replicas reject replays.
		if claims.Nonce == "" {
			if err = s.requireActiveSession(r.Context(), token, claims); errors.Is(err, errSessionUnavailable) {
				apiError(w, http.StatusServiceUnavailable, err)
				return
			}
		}
		if _, err = s.repo.RevokeSession(r.Context(), claims.Subject, auth.SessionID(token)); err != nil {
			apiError(w, http.StatusServiceUnavailable, errSessionUnavailable)
			return
		}
	}
	cookie := sessionCookie(r)
	cookie.MaxAge = -1
	w.Header().Set("Cache-Control", "no-store")
	http.SetCookie(w, cookie)
	w.WriteHeader(http.StatusNoContent)
}

// Each API replica limits password derivations in flight as well as attempts
// per minute. Bound memory regardless of the number of distinct user names.
type loginGuard struct {
	mu           sync.Mutex
	reset        time.Time
	peers, users map[string]int
	active       int
}

func (g *loginGuard) admit(now time.Time, peer, user string) int {
	if host, _, err := net.SplitHostPort(peer); err == nil {
		peer = host
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if !now.Before(g.reset) {
		g.reset = now.Add(time.Minute)
		g.peers, g.users = make(map[string]int), make(map[string]int)
	}
	if g.active >= 4 {
		return 1
	}
	if g.peers[peer] >= 120 || g.users[user] >= 8 ||
		(g.peers[peer] == 0 && len(g.peers) >= 4096) || (g.users[user] == 0 && len(g.users) >= 4096) {
		return int(g.reset.Sub(now).Seconds()) + 1
	}
	g.peers[peer]++
	g.users[user]++
	g.active++
	return 0
}

func (g *loginGuard) release() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.active--
}
