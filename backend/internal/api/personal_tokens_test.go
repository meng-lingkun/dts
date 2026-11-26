package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dts/backend/internal/auth"
	"dts/backend/internal/connector"
	"dts/backend/internal/domain"
	"dts/backend/internal/engine"
	"dts/backend/internal/repository/memory"
)

func TestPersonalTokens(t *testing.T) {
	t.Setenv("DTS_AUTH_REQUIRED", "true")
	t.Setenv("DTS_AUTH_SECRET", "personal-token-test-secret-42")
	t.Setenv("DTS_RBAC_TOKENS", "admin:bootstrap")
	t.Setenv("DTS_WORKER_TOKEN", "")
	file := filepath.Join(t.TempDir(), "state.json")
	store, err := memory.NewPersistent(file)
	if err != nil {
		t.Fatal(err)
	}
	server := func(s *memory.Store) http.Handler {
		return New(s, connector.NewRegistry(), engine.NewRegistry()).Handler()
	}
	a, b := server(store), server(store)
	const path = "/api/v1/auth/personal-tokens"
	const me = "/api/v1/auth/me"
	req := func(h http.Handler, method, url, token string, body any, expected int) *httptest.ResponseRecorder {
		t.Helper()
		w := requestJSON(t, h, method, url, body, token)
		if w.Code != expected {
			t.Fatalf("%s %s returned %d, expected %d", method, url, w.Code, expected)
		}
		return w
	}
	req(a, "POST", "/api/v1/users", "bootstrap", map[string]string{"username": "admin", "password": "Admin-password", "role": "admin"}, 201)
	for _, username := range []string{"alice", "bob"} {
		req(a, "POST", "/api/v1/users", "bootstrap", map[string]string{"username": username, "password": "Secret-password", "role": "viewer"}, 201)
	}
	alice := loginToken(t, a, "alice", "Secret-password")
	bob := loginToken(t, b, "bob", "Secret-password")
	u, err := store.GetUserByUsername(t.Context(), "alice")
	if err != nil {
		t.Fatal(err)
	}
	spec := map[string]any{"name": "daily reports", "expires_at": time.Now().Add(24 * time.Hour)}
	type issued struct {
		Token string
		Item  domain.PersonalToken `json:"personal_token"`
	}
	issue := func(session string) issued {
		t.Helper()
		w := req(a, "POST", path, session, spec, 201)
		var v issued
		if err := json.Unmarshal(w.Body.Bytes(), &v); err != nil || !strings.HasPrefix(v.Token, auth.PersonalTokenPrefix) || len(v.Token) != len(auth.PersonalTokenPrefix)+43 || v.Item.ID == "" || v.Item.UserID != u.ID || w.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("invalid token issuance response")
		}
		return v
	}
	req(a, "POST", path, "bootstrap", spec, 403)
	for _, bad := range []any{map[string]any{"name": "x"}, map[string]any{"expires_at": spec["expires_at"]}, map[string]any{"name": "bad", "expires_at": time.Now().Add(-time.Hour)}, map[string]any{"name": "bad", "expires_at": time.Now().Add(366 * 24 * time.Hour)}} {
		req(a, "POST", path, alice, bad, 400)
	}
	first, second := issue(alice), issue(alice)
	expiredRaw, expiredID, err := auth.NewSessionManager("personal-token-test-secret-42", time.Hour).NewPersonalToken()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreatePersonalToken(t.Context(), domain.PersonalToken{ID: expiredID, UserID: u.ID, Name: "expired fixture", CreatedAt: time.Now().Add(-time.Hour), ExpiresAt: time.Now().Add(-time.Minute)}, u.Revision); err != nil {
		t.Fatal(err)
	}
	req(b, "GET", me, expiredRaw, nil, 401)
	if first.Token == second.Token {
		t.Fatal("token reused")
	}
	list := req(b, "GET", path, alice, nil, 200)
	if !strings.Contains(list.Body.String(), first.Item.ID) || strings.Contains(list.Body.String(), first.Token) {
		t.Fatal("list contains a secret or omits issued token")
	}
	data, err := os.ReadFile(file)
	if err != nil || strings.Contains(string(data), first.Token) {
		t.Fatal("persistent registry exposed raw credential")
	}
	req(b, "DELETE", path+"/"+first.Item.ID, bob, nil, 404)
	req(b, "GET", me, first.Token, nil, 200)
	req(b, "GET", "/api/v1/users", first.Token, nil, 403)
	req(b, "POST", path, first.Token, spec, 403)
	req(b, "GET", me, first.Item.ID, nil, 401)
	for _, kind := range []string{"cookie", "legacy_header", "websocket", "worker"} {
		r := httptest.NewRequest("GET", me, nil)
		switch kind {
		case "cookie":
			r.AddCookie(&http.Cookie{Name: sessionCookieName, Value: first.Token})
		case "legacy_header":
			r.Header.Set("X-DTS-API-Token", first.Token)
		case "websocket":
			r.URL.Path = "/api/v1/ws"
			r.Header.Set("Authorization", "Bearer "+first.Token)
		case "worker":
			r.Method = "POST"
			r.URL.Path = "/api/v1/workers/register"
			r.Header.Set("Authorization", "Bearer "+first.Token)
		}
		w := httptest.NewRecorder()
		b.ServeHTTP(w, r)
		if w.Code != 401 {
			t.Errorf("%s PAT transport accepted: %d", kind, w.Code)
		}
	}
	req(b, "DELETE", path+"/"+first.Item.ID, alice, nil, 204)
	req(a, "GET", me, first.Token, nil, 401)
	reopened, err := memory.NewPersistent(file)
	if err != nil {
		t.Fatal(err)
	}
	restarted := server(reopened)
	req(restarted, "GET", me, first.Token, nil, 401)
	req(restarted, "GET", me, second.Token, nil, 200)
	t.Setenv("DTS_OIDC_ONLY", "true")
	strict := server(store)
	req(strict, "GET", me, second.Token, nil, 401)
	t.Setenv("DTS_OIDC_ONLY", "false")
	req(b, "PUT", "/api/v1/users/"+u.ID, "bootstrap", map[string]string{"role": "dba"}, 200)
	if w := req(a, "GET", me, second.Token, nil, 200); !strings.Contains(w.Body.String(), `"role":"dba"`) {
		t.Fatal("PAT did not use current role")
	}
	req(b, "PUT", "/api/v1/users/"+u.ID, "bootstrap", map[string]bool{"enabled": false}, 200)
	req(a, "GET", me, second.Token, nil, 401)
	req(b, "PUT", "/api/v1/users/"+u.ID, "bootstrap", map[string]bool{"enabled": true}, 200)
	req(a, "GET", me, second.Token, nil, 401)
	alice = loginToken(t, a, "alice", "Secret-password")
	third := issue(alice)
	req(b, "POST", "/api/v1/users/"+u.ID+"/password", "bootstrap", map[string]string{"password": "Updated-password"}, 200)
	req(a, "GET", me, third.Token, nil, 401)
}
