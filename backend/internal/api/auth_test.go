package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/engine"
	"qmigration/backend/internal/repository/memory"
)

func newAuthTestServer(t *testing.T) http.Handler {
	t.Helper()
	t.Setenv("QMIGRATION_RBAC_TOKENS", "")
	t.Setenv("QMIGRATION_API_TOKEN", "")
	t.Setenv("QMIGRATION_AUTH_REQUIRED", "")
	t.Setenv("QMIGRATION_AUTH_SECRET", "test-auth-secret-that-is-long-enough")
	return New(memory.New(), connector.NewRegistry(), engine.NewRegistry()).Handler()
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
