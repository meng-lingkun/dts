package api

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"dts/backend/internal/auth"
)

func TestWebSocketBufferedPingAndConcurrentData(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	_ = client.SetDeadline(time.Now().Add(3 * time.Second))
	writer := &webSocketWriter{conn: server}
	// A masked ping already buffered during HTTP upgrade.
	ping := []byte{0x89, 0x81, 1, 2, 3, 4, 'p' ^ 1}
	done := make(chan struct{})
	go readWebSocketFrames(bufio.NewReader(bytes.NewReader(ping)), writer, done)
	go writer.frame(1, []byte("text"))
	received := map[byte]string{}
	for i := 0; i < 2; i++ {
		var header [2]byte
		if _, err := io.ReadFull(client, header[:]); err != nil {
			t.Fatal(err)
		}
		payload := make([]byte, header[1])
		if _, err := io.ReadFull(client, payload); err != nil {
			t.Fatal(err)
		}
		received[header[0]] = string(payload)
	}
	<-done
	if received[0x81] != "text" || received[0x8a] != "p" {
		t.Fatal(received)
	}
}

func TestBrowserCookieLifecycle(t *testing.T) {
	h := newAuthTestServer(t)
	created := requestJSON(t, h, "POST", "/api/v1/users", map[string]string{"username": "admin", "role": "admin", "password": "test-password"}, "")
	if created.Code != 201 {
		t.Fatal(created.Body.String())
	}
	const site = "https://dts.test:30443"
	call := func(method, path, origin, body string, mark bool, cookie *http.Cookie) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		r.Host = "dts.test:30443"
		if origin != "" {
			r.Header.Set("Origin", origin)
		}
		if mark {
			r.Header.Set(browserHeader, "1")
		}
		if cookie != nil {
			r.AddCookie(cookie)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	login := func(origin string) *httptest.ResponseRecorder {
		return call("POST", "/api/v1/auth/login", origin, `{"username":"admin","password":"test-password"}`, true, nil)
	}
	if w := login("https://elsewhere.test"); w.Code != 403 {
		t.Fatalf("cross-origin login: %d", w.Code)
	}
	w := login(site)
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	var body map[string]json.RawMessage
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || body["token"] != nil {
		t.Fatal("unexpected browser login response")
	}
	cookies := w.Result().Cookies()
	if len(cookies) != 1 {
		t.Fatal("expected one session cookie")
	}
	c := cookies[0]
	if !c.HttpOnly || !c.Secure || c.SameSite != http.SameSiteStrictMode || c.Value == "" {
		t.Fatal("insecure cookie attributes")
	}
	if w := call("GET", "/api/v1/auth/me", "", "", false, c); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	for _, origin := range []string{"", "null", "https://elsewhere.test", "https://dts.test:40000"} {
		if w := call("POST", "/api/v1/migrations", origin, "{}", true, c); w.Code != 403 {
			t.Fatalf("accepted origin %q: %d", origin, w.Code)
		}
		if w := call("GET", "/api/v1/ws", origin, "", false, c); w.Code != 403 {
			t.Fatalf("accepted ws origin %q: %d", origin, w.Code)
		}
	}
	if w := call("POST", "/api/v1/auth/logout", site, "", false, c); w.Code != 403 {
		t.Fatalf("missing browser header: %d", w.Code)
	}
	w = call("POST", "/api/v1/auth/logout", site, "", true, c)
	if w.Code != 204 || len(w.Result().Cookies()) != 1 || w.Result().Cookies()[0].MaxAge >= 0 {
		t.Fatal("logout failed to expire cookie")
	}
	w = login("http://dts.test:30443")
	if w.Code != 200 || w.Result().Cookies()[0].Secure {
		t.Fatal("HTTP NodePort login failed")
	}
	self := w.Result().Cookies()[0]
	rr := call("DELETE", "/api/v1/auth/sessions/"+auth.SessionID(self.Value), "http://dts.test:30443", "", true, self)
	if rr.Code != 204 || len(rr.Result().Cookies()) != 1 || rr.Result().Cookies()[0].MaxAge >= 0 {
		t.Fatalf("self revocation failed to expire cookie: %d", rr.Code)
	}
	if got := call("GET", "/api/v1/auth/me", "", "", false, self); got.Code != 401 {
		t.Fatalf("revoked session got %d", got.Code)
	}
}

func TestLoginLimits(t *testing.T) {
	h := newAuthTestServer(t)
	for _, length := range []int64{-1, 9000} {
		r := httptest.NewRequest("POST", "/api/v1/auth/login", strings.NewReader(`{"username":"`+strings.Repeat("a", 9000)+`"}`))
		r.ContentLength = length
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 413 {
			t.Fatalf("oversize body status %d", w.Code)
		}
	}
	for i := 0; i < 9; i++ {
		w := requestJSON(t, h, "POST", "/api/v1/auth/login", map[string]string{"username": "ADMIN", "password": "wrong"}, "")
		if i < 8 && w.Code != 401 || i == 8 && (w.Code != 429 || w.Header().Get("Retry-After") == "") {
			t.Fatalf("attempt %d status %d", i, w.Code)
		}
	}
	var guard loginGuard
	now := time.Now()
	for i := 0; i < 4; i++ {
		if guard.admit(now, "one", "user") != 0 {
			t.Fatal("unexpected early limit")
		}
	}
	if guard.admit(now, "two", "other") == 0 {
		t.Fatal("parallel verification not limited")
	}
	guard.release()
	if guard.admit(now.Add(time.Minute), "two", "other") != 0 {
		t.Fatal("limit persisted after expiry")
	}
}
