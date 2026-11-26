package api

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"dts/backend/internal/connector"
	"dts/backend/internal/domain"
	"dts/backend/internal/engine"
	"dts/backend/internal/repository/memory"
	jose "github.com/go-jose/go-jose/v4"
	"golang.org/x/oauth2"
)

func TestOIDCBrowserFlowAcrossReplicas(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: key}, (&jose.SignerOptions{}).WithHeader("kid", "local-key"))
	if err != nil {
		t.Fatal(err)
	}
	var issuer string
	var mu sync.Mutex
	type grant struct{ nonce, challenge, subject, audience, assurance string }
	grants := map[string]grant{}
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"issuer": issuer, "authorization_endpoint": issuer + "/login", "token_endpoint": issuer + "/token", "jwks_uri": issuer + "/keys", "id_token_signing_alg_values_supported": []string{"RS256"}})
	})
	mux.HandleFunc("/keys", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: &key.PublicKey, KeyID: "local-key", Use: "sig", Algorithm: "RS256"}}})
	})
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		code := r.FormValue("code")
		mu.Lock()
		g, ok := grants[code]
		delete(grants, code)
		mu.Unlock()
		if !ok || oauth2.S256ChallengeFromVerifier(r.FormValue("code_verifier")) != g.challenge {
			http.Error(w, "invalid_grant", 400)
			return
		}
		now := time.Now()
		payload, _ := json.Marshal(map[string]any{"iss": issuer, "aud": g.audience, "sub": g.subject, "nonce": g.nonce, "iat": now.Unix(), "exp": now.Add(time.Minute).Unix(), "acr": g.assurance})
		jws, err := signer.Sign(payload)
		if err != nil {
			t.Error(err)
			w.WriteHeader(500)
			return
		}
		raw, err := jws.CompactSerialize()
		if err != nil {
			t.Error(err)
			w.WriteHeader(500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"id_token": raw, "access_token": "example", "token_type": "bearer"})
	})
	idp := httptest.NewTLSServer(mux)
	defer idp.Close()
	issuer = idp.URL
	t.Setenv("DTS_AUTH_SECRET", "shared-session-key-for-oidc-across-replicas")
	t.Setenv("DTS_OIDC_ISSUER", issuer)
	t.Setenv("DTS_OIDC_CLIENT_ID", "app")
	t.Setenv("DTS_OIDC_ONLY", "true")
	t.Setenv("DTS_OIDC_REQUIRED_ACR", "urn:test:mfa")
	t.Setenv("DTS_RBAC_TOKENS", "admin:automation")
	t.Setenv("DTS_WORKER_TOKEN", "test-node")
	t.Setenv("DTS_OIDC_REDIRECT_URL", "https://dts.test"+oidcCallback)
	t.Setenv("DTS_OIDC_BINDINGS", `{"`+issuer+`":{"emp-123":"user-1"}}`)
	repo := memory.New()
	if err := repo.CreateUser(t.Context(), &domain.User{ID: "user-1", Username: "test", PasswordHash: "test-hash", Role: "viewer", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	makeHandler := func() http.Handler {
		s := New(repo, connector.NewRegistry(), engine.NewRegistry())
		s.oidcLogin.client = idp.Client()
		return s.Handler()
	}
	first, second := makeHandler(), makeHandler()
	for _, tc := range []struct {
		method, path string
		want         int
	}{
		{"POST", "/api/v1/workers/register", 201}, {"GET", "/api/v1/workers", 401}, {"GET", "/api/v1/users", 401}, {"POST", "/api/v1/migrations", 401},
	} {
		req := httptest.NewRequest(tc.method, tc.path, strings.NewReader("{}"))
		req.Header.Set("X-DTS-Worker-Token", "test-node")
		w := httptest.NewRecorder()
		first.ServeHTTP(w, req)
		if w.Code != tc.want {
			t.Fatalf("worker credential %s %s: %d", tc.method, tc.path, w.Code)
		}
	}
	if w := requestJSON(t, first, "POST", "/api/v1/auth/login", map[string]string{"username": "test", "password": "example"}, ""); w.Code != 403 {
		t.Fatal("local login allowed")
	}
	if w := requestJSON(t, first, "GET", "/api/v1/dashboard", nil, "automation"); w.Code != 401 {
		t.Fatal("static token allowed")
	}
	local := New(repo, connector.NewRegistry(), engine.NewRegistry())
	u, err := repo.GetUser(t.Context(), "user-1")
	if err != nil {
		t.Fatal(err)
	}
	localToken, _, err := local.issueUserSession(t.Context(), u, "viewer", nil)
	if err != nil {
		t.Fatal(err)
	}
	if w := requestJSON(t, second, "GET", "/api/v1/auth/me", nil, localToken); w.Code != 401 {
		t.Fatal("previous password session allowed")
	}
	for _, tc := range []struct {
		name, subject, audience string
		badNonce, badState      bool
		want                    int
	}{
		{"success", "emp-123", "app", false, false, 303},
		{"different subject", "unknown", "app", false, false, 403},
		{"wrong audience", "emp-123", "another-app", false, false, 401},
		{"changed nonce", "emp-123", "app", true, false, 401},
		{"incorrect state", "emp-123", "app", false, true, 400},
		{"low assurance", "emp-123", "app", false, false, 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := requestJSON(t, first, "GET", "/api/v1/auth/oidc/start", nil, "")
			if w.Code != 302 {
				t.Fatal(w.Code, w.Body.String())
			}
			loc, err := url.Parse(w.Header().Get("Location"))
			if err != nil {
				t.Fatal(err)
			}
			q := loc.Query()
			if loc.String() == "" || q.Get("code_challenge_method") != "S256" || q.Get("nonce") == "" || q.Get("state") == "" || q.Get("acr_values") != "urn:test:mfa" {
				t.Fatal("incomplete authorization request")
			}
			flow := w.Result().Cookies()[0]
			if !flow.Secure || !flow.HttpOnly || flow.Name != flowCookie {
				t.Fatal("unsafe flow cookie")
			}
			grantCode := "sample"
			nonce, state := q.Get("nonce"), q.Get("state")
			if tc.badNonce {
				nonce = "mismatch"
			}
			if tc.badState {
				state = "mismatch"
			}
			mu.Lock()
			acr := q.Get("acr_values")
			if tc.name == "low assurance" {
				acr = ""
			}
			grants[grantCode] = grant{nonce, q.Get("code_challenge"), tc.subject, tc.audience, acr}
			mu.Unlock()
			callback := func() *httptest.ResponseRecorder {
				r := httptest.NewRequest("GET", oidcCallback+"?code="+grantCode+"&state="+state, nil)
				r.AddCookie(flow)
				w := httptest.NewRecorder()
				second.ServeHTTP(w, r)
				return w
			}
			end := callback()
			if end.Code != tc.want {
				t.Fatal(end.Code, end.Body.String())
			}
			deleted := false
			for _, c := range end.Result().Cookies() {
				if c.Name == flowCookie && c.MaxAge < 0 && c.Secure && c.HttpOnly && c.Path == "/" && c.Domain == "" {
					deleted = true
				}
			}
			if !deleted {
				t.Fatal("callback did not expire the host-only correlation cookie")
			}
			for _, cookie := range end.Result().Cookies() {
				if cookie.Name != sessionCookieName {
					continue
				}
				if tc.want != 303 || !cookie.Secure || !cookie.HttpOnly {
					t.Fatal("invalid session issuance")
				}
				if w := requestJSON(t, first, "GET", "/api/v1/auth/me", nil, cookie.Value); w.Code != 200 {
					t.Fatal(w.Body.String())
				}
				t.Setenv("DTS_OIDC_BINDINGS", `{"`+issuer+`":{"different":"user-1"}}`)
				if w := requestJSON(t, makeHandler(), "GET", "/api/v1/auth/me", nil, cookie.Value); w.Code != 401 {
					t.Fatal("removed OIDC binding still allowed")
				}
				if w := requestJSON(t, first, "POST", "/api/v1/auth/logout", nil, cookie.Value); w.Code != 204 {
					t.Fatal(w.Body.String())
				}
				if w := requestJSON(t, second, "GET", "/api/v1/auth/me", nil, cookie.Value); w.Code != 401 {
					t.Fatal("revoked OIDC session accepted")
				}
				if w := callback(); w.Code != 401 {
					t.Fatal("authorization code replayed")
				}
				return
			}
			if tc.want == 303 {
				t.Fatal("missing local login cookie")
			}
		})
	}
}

func TestOIDCOnlyInvalidConfigurationFailsClosed(t *testing.T) {
	t.Setenv("DTS_OIDC_ONLY", "true")
	t.Setenv("DTS_OIDC_ISSUER", "")
	t.Setenv("DTS_AUTH_REQUIRED", "")
	t.Setenv("DTS_RBAC_TOKENS", "")
	t.Setenv("DTS_API_TOKEN", "")
	h := New(memory.New(), connector.NewRegistry(), engine.NewRegistry()).Handler()
	if w := requestJSON(t, h, "GET", "/readyz", nil, ""); w.Code != 503 {
		t.Fatal("invalid configuration reported ready")
	}
	if w := requestJSON(t, h, "GET", "/api/v1/auth/me", nil, ""); w.Code != 401 {
		t.Fatal("empty registry entered open mode")
	}
}
