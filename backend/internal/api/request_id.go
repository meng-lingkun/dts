package api

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"regexp"
	"strings"
)

var safeRequestID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,96}$`)

func correlate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if !safeRequestID.MatchString(id) {
			var bytes [16]byte
			_, _ = rand.Read(bytes[:])
			id = hex.EncodeToString(bytes[:])
		}
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r)
	})
}
func statusCode(status int) string {
	if v := http.StatusText(status); v != "" {
		return strings.ToUpper(strings.NewReplacer(" ", "_", "-", "_").Replace(v))
	}
	return "HTTP_ERROR"
}

// Preserve ServeMux redirects and its Allow header on unmatched API requests.
// A normal application response, including WebSocket hijacking, passes through
// the unwrapped ResponseWriter.
func jsonRouteErrors(mux *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/v1/") {
			if h, pattern := mux.Handler(r); pattern == "" {
				h.ServeHTTP(&routeFailure{ResponseWriter: w}, r)
				return
			}
		}
		mux.ServeHTTP(w, r)
	})
}

type routeFailure struct {
	http.ResponseWriter
	handled bool
}

func (w *routeFailure) WriteHeader(code int) {
	if code == http.StatusNotFound || code == http.StatusMethodNotAllowed {
		w.handled = true
		writeJSON(w.ResponseWriter, code, map[string]any{"error": http.StatusText(code)})
		return
	}
	w.ResponseWriter.WriteHeader(code)
}
func (w *routeFailure) Write(body []byte) (int, error) {
	if w.handled {
		return len(body), nil
	}
	return w.ResponseWriter.Write(body)
}
