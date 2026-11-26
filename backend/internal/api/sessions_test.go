package api

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"dts/backend/internal/auth"
	"dts/backend/internal/connector"
	"dts/backend/internal/engine"
	"dts/backend/internal/repository/memory"
)

func TestSessionRevocationAcrossReplicasAndRestart(t *testing.T) {
	const secret = "test-session-secret-for-all-replicas"
	t.Setenv("DTS_AUTH_SECRET", secret)
	t.Setenv("DTS_AUTH_REQUIRED", "true")
	t.Setenv("DTS_RBAC_TOKENS", "admin:bootstrap")
	file := filepath.Join(t.TempDir(), "sessions.json")
	repo, err := memory.NewPersistent(file)
	if err != nil {
		t.Fatal(err)
	}
	newAPI := func(repo *memory.Store) http.Handler {
		return New(repo, connector.NewRegistry(), engine.NewRegistry()).Handler()
	}
	a, b := newAPI(repo), newAPI(repo)
	for _, user := range []string{"alice", "other"} {
		if w := requestJSON(t, a, "POST", "/api/v1/users", map[string]string{"username": user, "password": "secret-password", "role": "viewer"}, "bootstrap"); w.Code != 201 {
			t.Fatal(w.Body.String())
		}
	}
	first := loginToken(t, a, "alice", "secret-password")
	second := loginToken(t, b, "alice", "secret-password")
	third := loginToken(t, b, "other", "secret-password")
	if first == second {
		t.Fatal("two logins issued the same token")
	}
	u, err := repo.GetUserByUsername(context.Background(), "alice")
	if err != nil {
		t.Fatal(err)
	}
	check := func(h http.Handler, method, path, token string, code int) *httptest.ResponseRecorder {
		t.Helper()
		w := requestJSON(t, h, method, path, nil, token)
		if w.Code != code {
			t.Fatalf("%s %s wanted %d got %d: %s", method, path, code, w.Code, w.Body.String())
		}
		return w
	}
	const self = "/api/v1/auth/sessions"
	check(a, "DELETE", self+"/"+auth.SessionID(first), third, 404)
	check(a, "GET", "/api/v1/users/"+u.ID+"/sessions", third, 403)
	w := check(b, "GET", self, second, 200)
	if !strings.Contains(w.Body.String(), auth.SessionID(first)) || strings.Contains(w.Body.String(), first) {
		t.Fatal("session list failed")
	}
	check(a, "DELETE", self+"/"+auth.SessionID(first), second, 204)
	check(b, "GET", "/api/v1/auth/me", first, 401)
	check(b, "GET", "/api/v1/auth/me", second, 200)
	restored, err := memory.NewPersistent(file)
	if err != nil {
		t.Fatal(err)
	}
	restarted := newAPI(restored)
	check(restarted, "GET", "/api/v1/auth/me", first, 401)
	check(restarted, "GET", "/api/v1/auth/me", second, 200)
	// Pre-registry sessions are signed with the same format, without jti.
	body, _, _ := strings.Cut(second, ".")
	raw, err := base64.RawURLEncoding.DecodeString(body)
	if err != nil {
		t.Fatal(err)
	}
	var legacy auth.SessionClaims
	if err = json.Unmarshal(raw, &legacy); err != nil {
		t.Fatal(err)
	}
	legacy.Nonce = ""
	raw, err = json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	body = base64.RawURLEncoding.EncodeToString(raw)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(body))
	legacyToken := body + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	check(restarted, "GET", "/api/v1/auth/me", legacyToken, 200)
	check(restarted, "POST", "/api/v1/auth/logout", legacyToken, 204)
	check(restarted, "GET", "/api/v1/auth/me", legacyToken, 401)
	check(restarted, "DELETE", "/api/v1/users/"+u.ID+"/sessions/"+auth.SessionID(second), "bootstrap", 204)
	check(restarted, "GET", "/api/v1/auth/me", second, 401)
}

func TestAPIErrorCorrelation(t *testing.T) {
	h := newAuthTestServer(t)
	for _, tc := range []struct {
		path, method string
		code         int
	}{
		{"/api/v1/absent", "GET", 404}, {"/api/v1/engines", "DELETE", 405}, {"/api/v1/migrations", "POST", 400},
	} {
		r := httptest.NewRequest(tc.method, tc.path, strings.NewReader("{}"))
		r.Header.Set("X-Request-ID", "client.test-123")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		var msg struct {
			Error, Code string
			ID          string `json:"request_id"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &msg); err != nil {
			t.Fatal(err)
		}
		if w.Code != tc.code || msg.Error == "" || msg.Code != statusCode(w.Code) || msg.ID != "client.test-123" || msg.ID != w.Header().Get("X-Request-ID") {
			t.Fatal(w.Code, w.Body.String())
		}
		if w.Code == 405 && w.Header().Get("Allow") == "" {
			t.Fatal("lost Allow header")
		}
	}
}
