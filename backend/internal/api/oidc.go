package api

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"dts/backend/internal/auth"
	"dts/backend/internal/domain"
	"dts/backend/internal/repository"
	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

const oidcCallback = "/api/v1/auth/oidc/callback"
const flowCookie = "__Host-dts_oidc"

type oidcLogin struct {
	only                                                 bool
	assurance                                            string
	issuer, clientID, redirect, clientSecret, authSecret string
	bindings                                             map[string]map[string]string // issuer → subject → immutable local user ID
	configErr                                            error
	client                                               *http.Client
	mu                                                   sync.Mutex
	provider                                             *oidc.Provider
	verifier                                             *oidc.IDTokenVerifier
}

func loadOIDCLogin() *oidcLogin {
	issuer := os.Getenv("DTS_OIDC_ISSUER")
	mode := os.Getenv("DTS_OIDC_ONLY")
	only, modeErr := strconv.ParseBool(mode)
	invalidMode := mode != "" && modeErr != nil
	if issuer == "" && !only && !invalidMode && os.Getenv("DTS_OIDC_CLIENT_ID") == "" && os.Getenv("DTS_OIDC_BINDINGS") == "" && os.Getenv("DTS_OIDC_REDIRECT_URL") == "" {
		return nil
	}
	c := &oidcLogin{issuer: issuer, clientID: os.Getenv("DTS_OIDC_CLIENT_ID"), redirect: os.Getenv("DTS_OIDC_REDIRECT_URL"), clientSecret: os.Getenv("DTS_OIDC_CLIENT_SECRET"), authSecret: os.Getenv("DTS_AUTH_SECRET")}
	c.only = only || invalidMode
	c.assurance = os.Getenv("DTS_OIDC_REQUIRED_ACR")
	c.client = &http.Client{Timeout: 8 * time.Second, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
	redirect, err := url.Parse(c.redirect)
	if invalidMode {
		c.configErr = errors.New("DTS_OIDC_ONLY must be a boolean")
	} else if !validHTTPSEndpoint(c.issuer) || !validHTTPSEndpoint(c.redirect) || err != nil || redirect.Path != oidcCallback || c.clientID == "" || len(c.authSecret) < 32 {
		c.configErr = errors.New("OIDC requires HTTPS issuer and callback URL, client ID and a shared DTS_AUTH_SECRET of at least 32 bytes")
	} else if json.Unmarshal([]byte(os.Getenv("DTS_OIDC_BINDINGS")), &c.bindings) != nil || len(c.bindings[issuer]) == 0 {
		c.configErr = errors.New("OIDC requires explicit issuer and subject bindings to local user IDs")
	}
	return c
}

func validHTTPSEndpoint(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Scheme == "https" && u.Hostname() != "" && u.User == nil && u.Fragment == "" && u.RawQuery == ""
}

func (c *oidcLogin) discover(ctx context.Context) (*oauth2.Config, *oidc.IDTokenVerifier, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.provider == nil {
		p, err := oidc.NewProvider(oidc.ClientContext(ctx, c.client), c.issuer)
		if err != nil {
			return nil, nil, err
		}
		var metadata struct {
			JWKS string `json:"jwks_uri"`
		}
		if err = p.Claims(&metadata); err != nil || !validHTTPSEndpoint(p.Endpoint().AuthURL) || !validHTTPSEndpoint(p.Endpoint().TokenURL) || !validHTTPSEndpoint(metadata.JWKS) {
			return nil, nil, errors.New("invalid OIDC provider endpoints")
		}
		// The cached verifier outlives discovery's request context. The HTTP
		// client independently bounds JWKS refreshes.
		c.verifier = p.VerifierContext(oidc.ClientContext(context.Background(), c.client), &oidc.Config{ClientID: c.clientID})
		c.provider = p
	}
	return &oauth2.Config{ClientID: c.clientID, ClientSecret: c.clientSecret, RedirectURL: c.redirect, Endpoint: c.provider.Endpoint(), Scopes: []string{oidc.ScopeOpenID}}, c.verifier, nil
}

// Keep the PKCE verifier only in a host-only HttpOnly cookie. State and nonce
// are separate domain-bound MACs. Any replica can complete the same flow.
func (c *oidcLogin) proof(label, flow string) string {
	mac := hmac.New(sha256.New, []byte(c.authSecret))
	mac.Write([]byte(strings.Join([]string{"dts-oidc-v1", label, c.issuer, c.clientID, c.redirect, flow}, "\x00")))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func oidcCookie(value string, age int) *http.Cookie {
	return &http.Cookie{Name: flowCookie, Value: value, MaxAge: age, Path: "/", Secure: true, HttpOnly: true, SameSite: http.SameSiteLaxMode}
}

func (s *Server) authProviders(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]any{"oidc": s.oidcLogin != nil && s.oidcLogin.configErr == nil, "password_login": s.oidcLogin == nil || !s.oidcLogin.only})
}
func (s *Server) startOIDC(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	c := s.oidcLogin
	if c == nil || c.configErr != nil {
		apiError(w, 503, errors.New("OIDC unavailable"))
		return
	}
	o, _, err := c.discover(r.Context())
	if err != nil {
		apiError(w, 503, errors.New("identity provider discovery unavailable"))
		return
	}
	verifier := oauth2.GenerateVerifier()
	flow := strconv.FormatInt(time.Now().Unix(), 10) + "." + verifier
	http.SetCookie(w, oidcCookie(flow, 600))
	options := []oauth2.AuthCodeOption{oidc.Nonce(c.proof("nonce", flow)), oauth2.S256ChallengeOption(verifier)}
	if c.assurance != "" {
		options = append(options, oauth2.SetAuthURLParam("acr_values", c.assurance))
	}
	http.Redirect(w, r, o.AuthCodeURL(c.proof("state", flow), options...), http.StatusFound)
}

func (s *Server) finishOIDC(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Referrer-Policy", "no-referrer")
	c := s.oidcLogin
	if c == nil || c.configErr != nil {
		apiError(w, 503, errors.New("OIDC unavailable"))
		return
	}
	flow, err := r.Cookie(flowCookie)
	http.SetCookie(w, oidcCookie("", -1))
	if err != nil || !hmac.Equal([]byte(r.URL.Query().Get("state")), []byte(c.proof("state", flow.Value))) {
		apiError(w, 400, errors.New("invalid OIDC state"))
		return
	}
	ts, verifier, ok := strings.Cut(flow.Value, ".")
	t, err := strconv.ParseInt(ts, 10, 64)
	age := time.Now().Unix() - t
	code := r.URL.Query().Get("code")
	if !ok || err != nil || age < 0 || age > 600 || len(verifier) != 43 || code == "" {
		apiError(w, 400, errors.New("invalid or expired OIDC authorization"))
		return
	}
	o, v, err := c.discover(r.Context())
	if err != nil {
		apiError(w, 503, errors.New("identity provider discovery unavailable"))
		return
	}
	ctx := oidc.ClientContext(r.Context(), c.client)
	tokens, err := o.Exchange(ctx, code, oauth2.VerifierOption(verifier))
	if err != nil {
		apiError(w, 401, errors.New("identity provider rejected authorization code"))
		return
	}
	raw, _ := tokens.Extra("id_token").(string)
	identity, err := v.Verify(ctx, raw)
	if err != nil || identity.Subject == "" || !hmac.Equal([]byte(identity.Nonce), []byte(c.proof("nonce", flow.Value))) {
		apiError(w, 401, errors.New("invalid identity token"))
		return
	}
	var client struct {
		AuthorizedParty string `json:"azp"`
		Assurance       string `json:"acr"`
	}
	if identity.Claims(&client) != nil || (len(identity.Audience) > 1 || client.AuthorizedParty != "") && client.AuthorizedParty != c.clientID {
		apiError(w, 401, errors.New("invalid authorized party"))
		return
	}
	if c.assurance != "" && c.assurance != client.Assurance {
		apiError(w, 403, errors.New("identity provider authentication assurance is insufficient"))
		return
	}
	id := c.bindings[identity.Issuer][identity.Subject]
	if id == "" {
		apiError(w, 403, errors.New("identity has no local account binding"))
		return
	}
	u, err := s.repo.GetUser(ctx, id)
	if err != nil && !errors.Is(err, repository.ErrNotFound) {
		apiError(w, 503, errSessionUnavailable)
		return
	}
	if err != nil || u == nil || !u.Enabled || u.PasswordHash == "" {
		apiError(w, 403, errors.New("local account unavailable"))
		return
	}
	role, ok := validRole(u.Role)
	if !ok {
		apiError(w, 403, errors.New("local account role invalid"))
		return
	}
	token, expiry, err := s.issueUserSession(ctx, u, role, &auth.FederatedIdentity{Issuer: identity.Issuer, Subject: identity.Subject, Assurance: client.Assurance})
	if err != nil {
		apiError(w, 503, errors.New("unable to establish local session"))
		return
	}
	now := time.Now().UTC()
	_ = s.repo.UpdateUserLastLogin(ctx, u.ID, now)
	_ = s.repo.CreateAuditEvent(ctx, &domain.AuditEvent{ID: randomID("aud"), Actor: u.Username, Action: "LOGIN_OIDC", ResourceType: "auth", ResourceID: u.ID, RemoteAddr: r.RemoteAddr, CreatedAt: now})
	session := sessionCookie(r)
	session.Value, session.Expires, session.Secure = token, expiry, true
	http.SetCookie(w, session)
	http.Redirect(w, r, "/", http.StatusSeeOther)
}
