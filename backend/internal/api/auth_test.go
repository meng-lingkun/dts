package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"dts/backend/internal/auth"
	"dts/backend/internal/connector"
	"dts/backend/internal/domain"
	"dts/backend/internal/engine"
	"dts/backend/internal/repository"
	"dts/backend/internal/repository/memory"
)

func newAuthTestServer(t *testing.T) http.Handler {
	t.Helper()
	t.Setenv("DTS_RBAC_TOKENS", "")
	t.Setenv("DTS_API_TOKEN", "")
	t.Setenv("DTS_AUTH_REQUIRED", "")
	t.Setenv("DTS_AUTH_SECRET", "test-auth-secret-that-is-long-enough")
	return New(memory.New(), connector.NewRegistry(), engine.NewRegistry()).Handler()
}

func TestMetricsRequireConfiguredScrapeToken(t *testing.T) {
	t.Setenv("DTS_METRICS_TOKEN", "dedicated-scraper")
	h := newAuthTestServer(t)
	for token, status := range map[string]int{"": 401, "invalid": 401, "dedicated-scraper": 200} {
		if rr := requestJSON(t, h, http.MethodGet, "/metrics", nil, token); rr.Code != status {
			t.Fatalf("metrics status=%d want %d", rr.Code, status)
		}
	}
}

func requestJSON(t *testing.T, h http.Handler, method, path string, body any, token string) *httptest.ResponseRecorder {
	t.Helper()
	var b bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&b).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	req := httptest.NewRequest(method, path, &b)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

func loginToken(t *testing.T, h http.Handler, username, password string) string {
	t.Helper()
	rr := requestJSON(t, h, http.MethodPost, "/api/v1/auth/login", map[string]any{"username": username, "password": password}, "")
	if rr.Code != http.StatusOK {
		t.Fatalf("login status=%d body=%s", rr.Code, rr.Body.String())
	}
	var v struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &v); err != nil {
		t.Fatal(err)
	}
	if v.Token == "" {
		t.Fatal("empty token")
	}
	return v.Token
}

func TestFirstUserClosesOpenModeAndCanLogin(t *testing.T) {
	h := newAuthTestServer(t)
	rr := requestJSON(t, h, http.MethodPost, "/api/v1/users", map[string]any{
		"username": "admin", "password": "change-me-123", "role": "admin",
	}, "")
	if rr.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", rr.Code, rr.Body.String())
	}

	rr = requestJSON(t, h, http.MethodGet, "/api/v1/users", nil, "")
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected auth after first user, got %d body=%s", rr.Code, rr.Body.String())
	}

	token := loginToken(t, h, "admin", "change-me-123")
	rr = requestJSON(t, h, http.MethodGet, "/api/v1/users", nil, token)
	if rr.Code != http.StatusOK {
		t.Fatalf("admin list status=%d body=%s", rr.Code, rr.Body.String())
	}
}

func TestFirstAccountClosesOpenModeAcrossReplicas(t *testing.T) {
	t.Setenv("DTS_AUTH_REQUIRED", "")
	t.Setenv("DTS_RBAC_TOKENS", "")
	t.Setenv("DTS_API_TOKEN", "")
	repo := memory.New()
	first := New(repo, connector.NewRegistry(), engine.NewRegistry()).Handler()
	second := New(repo, connector.NewRegistry(), engine.NewRegistry()).Handler()
	if w := requestJSON(t, first, "POST", "/api/v1/users", map[string]string{"username": "viewer", "password": "test-password", "role": "viewer"}, ""); w.Code != 400 {
		t.Fatalf("first viewer: %d", w.Code)
	}
	firstAdmin := map[string]string{"username": "first-admin", "password": "test-password", "role": "admin"}
	if w := requestJSON(t, first, "POST", "/api/v1/users", firstAdmin, ""); w.Code != 201 {
		t.Fatal(w.Code)
	}
	if w := requestJSON(t, second, "GET", "/api/v1/auth/me", nil, ""); w.Code != 401 {
		t.Fatalf("other replica retained open mode: %d", w.Code)
	}
	// A request admitted before the first insert must still lose at commit.
	if err := repo.CreateInitialUser(t.Context(), &domain.User{ID: "another", Username: "another", Enabled: true, Role: "admin"}); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("second bootstrap: %v", err)
	}
	if w := requestJSON(t, second, "GET", "/api/v1/users", nil, loginToken(t, second, "first-admin", "test-password")); w.Code != 200 {
		t.Fatal(w.Code)
	}
}

func TestViewerCannotManageUsers(t *testing.T) {
	h := newAuthTestServer(t)
	rr := requestJSON(t, h, http.MethodPost, "/api/v1/users", map[string]any{
		"username": "admin", "password": "change-me-123", "role": "admin",
	}, "")
	if rr.Code != http.StatusCreated {
		t.Fatalf("bootstrap status=%d body=%s", rr.Code, rr.Body.String())
	}
	adminToken := loginToken(t, h, "admin", "change-me-123")

	rr = requestJSON(t, h, http.MethodPost, "/api/v1/users", map[string]any{
		"username": "viewer", "password": "viewer-pass-123", "role": "viewer",
	}, adminToken)
	if rr.Code != http.StatusCreated {
		t.Fatalf("create viewer=%d body=%s", rr.Code, rr.Body.String())
	}
	viewerToken := loginToken(t, h, "viewer", "viewer-pass-123")
	rr = requestJSON(t, h, http.MethodGet, "/api/v1/users", nil, viewerToken)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("viewer should be forbidden, got %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestCannotDisableLastAdmin(t *testing.T) {
	h := newAuthTestServer(t)
	rr := requestJSON(t, h, http.MethodPost, "/api/v1/users", map[string]any{
		"username": "admin", "password": "change-me-123", "role": "admin",
	}, "")
	if rr.Code != http.StatusCreated {
		t.Fatal(rr.Body.String())
	}
	var created struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &created)
	token := loginToken(t, h, "admin", "change-me-123")
	disabled := false
	rr = requestJSON(t, h, http.MethodPut, "/api/v1/users/"+created.ID, map[string]any{"enabled": &disabled}, token)
	if rr.Code != http.StatusConflict {
		t.Fatalf("expected conflict, got %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestReadinessEndpointIsUnauthenticatedAndChecksRepository(t *testing.T) {
	h := newAuthTestServer(t)
	rr := requestJSON(t, h, http.MethodGet, "/readyz", nil, "")
	if rr.Code != http.StatusOK {
		t.Fatalf("ready status=%d body=%s", rr.Code, rr.Body.String())
	}
}

func TestPasswordResetRevokesExistingSessions(t *testing.T) {
	h := newAuthTestServer(t)
	created := requestJSON(t, h, http.MethodPost, "/api/v1/users", map[string]any{"username": "admin", "password": "old-password-123", "role": "admin"}, "")
	if created.Code != http.StatusCreated {
		t.Fatal(created.Body.String())
	}
	var user struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &user); err != nil {
		t.Fatal(err)
	}
	oldToken := loginToken(t, h, "admin", "old-password-123")
	reset := requestJSON(t, h, http.MethodPost, "/api/v1/users/"+user.ID+"/password", map[string]any{"password": "new-password-456"}, oldToken)
	if reset.Code != http.StatusOK {
		t.Fatal(reset.Body.String())
	}
	for _, path := range []string{"/api/v1/auth/me", "/api/v1/users", "/api/v1/ws"} {
		if rr := requestJSON(t, h, http.MethodGet, path, nil, oldToken); rr.Code != http.StatusUnauthorized {
			t.Fatalf("old session still accepted by %s: %d", path, rr.Code)
		}
	}
	newToken := loginToken(t, h, "admin", "new-password-456")
	if rr := requestJSON(t, h, http.MethodGet, "/api/v1/auth/me", nil, newToken); rr.Code != http.StatusOK {
		t.Fatal(rr.Body.String())
	}
}

type resetDuringLoginRepo struct {
	repository.Repository
	newHash string
}

func (r resetDuringLoginRepo) GetUserByUsername(ctx context.Context, name string) (*domain.User, error) {
	old, err := r.Repository.GetUserByUsername(ctx, name)
	if err != nil {
		return nil, err
	}
	changed := *old
	changed.PasswordHash = r.newHash
	changed.Role = "viewer"
	if err := r.Repository.UpdateUser(ctx, &changed); err != nil {
		return nil, err
	}
	return old, nil
}

func TestLoginTelemetryCannotUndoConcurrentPasswordOrRoleChange(t *testing.T) {
	t.Setenv("DTS_RBAC_TOKENS", "")
	t.Setenv("DTS_API_TOKEN", "")
	t.Setenv("DTS_AUTH_SECRET", "test-auth-secret-that-is-long-enough")
	base := memory.New()
	// The concurrent change demotes admin. Keep a separate enabled admin so
	// the repository-level last-admin invariant does not reject the fixture.
	if err := base.CreateUser(context.Background(), &domain.User{ID: "other-admin", Username: "other-admin", Role: "admin", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	hash, err := auth.HashPassword("old-password-123")
	if err != nil {
		t.Fatal(err)
	}
	if err := base.CreateUser(context.Background(), &domain.User{ID: "admin", Username: "admin", Role: "admin", Enabled: true, PasswordHash: hash}); err != nil {
		t.Fatal(err)
	}
	h := New(resetDuringLoginRepo{Repository: base, newHash: "concurrent-password-reset"}, connector.NewRegistry(), engine.NewRegistry()).Handler()
	token := loginToken(t, h, "admin", "old-password-123")
	u, _ := base.GetUser(context.Background(), "admin")
	if u.PasswordHash != "concurrent-password-reset" || u.Role != "viewer" || u.LastLoginAt.IsZero() {
		t.Fatalf("login overwrote concurrent security change: role=%s last_login=%v", u.Role, u.LastLoginAt)
	}
	if rr := requestJSON(t, h, http.MethodGet, "/api/v1/auth/me", nil, token); rr.Code != http.StatusUnauthorized {
		t.Fatal("session issued against stale credentials remained valid")
	}
}

func TestPasswordResetClosesAnExistingWebSocket(t *testing.T) {
	h := newAuthTestServer(t)
	created := requestJSON(t, h, http.MethodPost, "/api/v1/users", map[string]any{"username": "admin", "password": "old-password-123", "role": "admin"}, "")
	if created.Code != http.StatusCreated {
		t.Fatal(created.Body.String())
	}
	var user struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &user); err != nil {
		t.Fatal(err)
	}
	token := loginToken(t, h, "admin", "old-password-123")
	srv := httptest.NewServer(h)
	defer srv.Close()
	conn, err := net.DialTimeout("tcp", strings.TrimPrefix(srv.URL, "http://"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := fmt.Fprintf(conn, "GET /api/v1/ws HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nAuthorization: Bearer %s\r\n\r\n", token); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	reader := bufio.NewReader(conn)
	response, err := http.ReadResponse(reader, nil)
	if err != nil || response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("websocket handshake failed: response=%v err=%v", response, err)
	}
	reset := requestJSON(t, h, http.MethodPost, "/api/v1/users/"+user.ID+"/password", map[string]any{"password": "new-password-456"}, token)
	if reset.Code != http.StatusOK {
		t.Fatal(reset.Body.String())
	}
	// Drain already queued frames; the next publication cycle must close the
	// connection instead of continuing to publish with the revoked identity.
	frames, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("revoked websocket did not close before deadline: %v", err)
	}
	if !bytes.HasSuffix(frames, []byte{0x88, 2, 0x03, 0xf0}) {
		t.Fatal("revoked stream did not send policy close (1008)")
	}
}

func TestConfiguredAuthenticationWithEmptyRepository(t *testing.T) {
	t.Setenv("DTS_API_TOKEN", "")
	for _, tc := range []struct {
		required, spec string
		want           int
	}{
		{"off", "", 200}, {"true", "", 401}, {"1", "", 401}, {" YES ", "", 401}, {"on", "", 401}, {"typo", "", 401}, {"", "unknown-role:configured", 401},
	} {
		t.Setenv("DTS_AUTH_REQUIRED", tc.required)
		t.Setenv("DTS_RBAC_TOKENS", tc.spec)
		h := New(memory.New(), connector.NewRegistry(), engine.NewRegistry()).Handler()
		if r := requestJSON(t, h, "GET", "/api/v1/auth/me", nil, ""); r.Code != tc.want || r.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("required=%q spec=%q got %d cache=%q", tc.required, tc.spec, r.Code, r.Header().Get("Cache-Control"))
		}
	}
}

type unavailableUsers struct {
	*memory.Store
	offline bool
}

func (s *unavailableUsers) GetUser(ctx context.Context, id string) (*domain.User, error) {
	if s.offline {
		return nil, errors.New("database unavailable")
	}
	return s.Store.GetUser(ctx, id)
}
func (s *unavailableUsers) GetUserByUsername(ctx context.Context, name string) (*domain.User, error) {
	if s.offline {
		return nil, errors.New("database unavailable")
	}
	return s.Store.GetUserByUsername(ctx, name)
}

func TestTemporaryUserStoreFailurePreservesSession(t *testing.T) {
	t.Setenv("DTS_AUTH_REQUIRED", "true")
	t.Setenv("DTS_RBAC_TOKENS", "admin:bootstrap")
	repo := &unavailableUsers{Store: memory.New()}
	h := New(repo, connector.NewRegistry(), engine.NewRegistry()).Handler()
	password := map[string]string{"username": "alice", "password": "secret-password"}
	if w := requestJSON(t, h, "POST", "/api/v1/users", map[string]string{"username": "alice", "password": "secret-password", "role": "viewer"}, "bootstrap"); w.Code != 201 {
		t.Fatal(w.Code)
	}
	token := loginToken(t, h, "alice", "secret-password")
	repo.offline = true
	if w := requestJSON(t, h, "GET", "/api/v1/auth/me", nil, token); w.Code != 503 {
		t.Fatalf("identity: %d", w.Code)
	}
	if w := requestJSON(t, h, "POST", "/api/v1/auth/login", password, ""); w.Code != 503 {
		t.Fatalf("login: %d", w.Code)
	}
	repo.offline = false
	if w := requestJSON(t, h, "GET", "/api/v1/auth/me", nil, token); w.Code != 200 {
		t.Fatalf("recovered identity: %d", w.Code)
	}
	if w := requestJSON(t, h, "POST", "/api/v1/auth/login", map[string]string{"username": "unknown", "password": "secret-password"}, ""); w.Code != 401 {
		t.Fatalf("unknown user: %d", w.Code)
	}
}
