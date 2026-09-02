package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"plugin"
	"runtime"
	"strings"
	"time"

	"qmigration/backend/internal/cdc/gbase8scdc"
)

func env(k, d string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return d
}
func providerConfig() (string, error) {
	direct := strings.TrimSpace(os.Getenv("QMIGRATION_GBASE8S_CDC_PROVIDER_CONFIG_JSON"))
	path := strings.TrimSpace(os.Getenv("QMIGRATION_GBASE8S_CDC_PROVIDER_CONFIG_FILE"))
	if direct != "" && path != "" {
		return "", errors.New("set only one of QMIGRATION_GBASE8S_CDC_PROVIDER_CONFIG_JSON or QMIGRATION_GBASE8S_CDC_PROVIDER_CONFIG_FILE")
	}
	if path == "" {
		if direct == "" {
			return "{}", nil
		}
		return direct, nil
	}
	st, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if !st.Mode().IsRegular() {
		return "", errors.New("GBase 8s CDC provider config must be a regular file")
	}
	if runtime.GOOS == "windows" {
		return "", errors.New("GBase 8s CDC provider config files are not supported on Windows because Unix owner permissions cannot be verified; use QMIGRATION_GBASE8S_CDC_PROVIDER_CONFIG_JSON")
	}
	if st.Mode().Perm()&0o007 != 0 {
		return "", errors.New("GBase 8s CDC provider config must not be accessible by other users")
	}
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, 1<<20))
	if err != nil {
		return "", err
	}
	if len(b) >= 1<<20 {
		return "", errors.New("GBase 8s CDC provider config file exceeds 1 MiB")
	}
	return string(b), nil
}

func loadProvider() (gbase8scdc.Agent, error) {
	library := strings.TrimSpace(os.Getenv("QMIGRATION_GBASE8S_CDC_PROVIDER_LIBRARY"))
	goPlugin := strings.TrimSpace(os.Getenv("QMIGRATION_GBASE8S_CDC_PROVIDER_PLUGIN"))
	if library != "" && goPlugin != "" {
		return nil, errors.New("configure only one GBase 8s CDC provider: native C ABI library or legacy Go plugin")
	}
	if library != "" {
		config, err := providerConfig()
		if err != nil {
			return nil, err
		}
		p, err := gbase8scdc.OpenNativeProvider(library, os.Getenv("QMIGRATION_GBASE8S_CDC_PROVIDER_SHA256"), config)
		if err != nil {
			return nil, err
		}
		return gbase8scdc.SerializeAgent(p), nil
	}
	if goPlugin == "" {
		return nil, errors.New("set QMIGRATION_GBASE8S_CDC_PROVIDER_LIBRARY (recommended C ABI) or QMIGRATION_GBASE8S_CDC_PROVIDER_PLUGIN (legacy Go plugin)")
	}
	p, err := plugin.Open(goPlugin)
	if err != nil {
		return nil, err
	}
	sym, err := p.Lookup("NewProvider")
	if err != nil {
		return nil, err
	}
	fn, ok := sym.(func() (gbase8scdc.Agent, error))
	if !ok {
		return nil, errors.New("GBase 8s CDC provider plugin NewProvider has incompatible signature")
	}
	provider, err := fn()
	if err != nil {
		return nil, err
	}
	return gbase8scdc.SerializeAgent(gbase8scdc.WithProviderInfo(provider, gbase8scdc.ProviderInfo{Kind: "legacy-go-plugin", ABIVersion: "go-plugin"})), nil
}

func decode(w http.ResponseWriter, r *http.Request, out any) bool {
	b, err := io.ReadAll(io.LimitReader(r.Body, 64<<20))
	if err != nil {
		http.Error(w, err.Error(), 400)
		return false
	}
	if err = json.Unmarshal(b, out); err != nil {
		http.Error(w, err.Error(), 400)
		return false
	}
	return true
}
func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
func listenIsLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	host = strings.TrimSpace(strings.Trim(host, "[]"))
	if strings.EqualFold(host, "localhost") {
		return true
	}
	if host == "" {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func validateAgentExposure(addr, token, cert, key string) error {
	if (cert == "") != (key == "") {
		return errors.New("both TLS cert/key files are required")
	}
	if listenIsLoopback(addr) {
		return nil
	}
	if strings.TrimSpace(token) == "" {
		return errors.New("non-loopback GBase 8s CDC agent requires QMIGRATION_GBASE8S_CDC_AGENT_TOKEN")
	}
	if cert == "" {
		return errors.New("non-loopback GBase 8s CDC agent requires TLS cert/key")
	}
	return nil
}

func bearerMatches(header, token string) bool {
	want := "Bearer " + token
	if len(header) != len(want) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(header), []byte(want)) == 1
}

func newMux(provider gbase8scdc.Agent, token string) http.Handler {
	auth := func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if token != "" && !bearerMatches(r.Header.Get("Authorization"), token) {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			next(w, r)
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/health", auth(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()
		if err := provider.Health(ctx); err != nil {
			http.Error(w, err.Error(), 503)
			return
		}
		info := gbase8scdc.ProviderInfo{}
		if d, ok := provider.(gbase8scdc.ProviderDescriber); ok {
			info = d.ProviderInfo()
		}
		writeJSON(w, gbase8scdc.HealthResponse{Status: "ok", APIVersion: gbase8scdc.AgentAPIVersion, Provider: info})
	}))
	mux.HandleFunc("/v1/status", auth(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method", http.StatusMethodNotAllowed)
			return
		}
		if d, ok := provider.(gbase8scdc.StatusDescriber); ok {
			writeJSON(w, d.AgentStatus())
			return
		}
		http.Error(w, "status unavailable", http.StatusNotImplemented)
	}))
	mux.HandleFunc("/metrics", auth(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method", http.StatusMethodNotAllowed)
			return
		}
		m, ok := provider.(gbase8scdc.MetricsRenderer)
		if !ok {
			http.Error(w, "metrics unavailable", http.StatusNotImplemented)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = io.WriteString(w, m.PrometheusMetrics())
	}))
	mux.HandleFunc("/v1/checkpoint", auth(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method", 405)
			return
		}
		var req gbase8scdc.CheckpointRequest
		if !decode(w, r, &req) {
			return
		}
		out, err := provider.Checkpoint(r.Context(), req)
		if err == nil {
			err = gbase8scdc.ValidateCheckpointResponse(req, out)
		}
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		writeJSON(w, out)
	}))
	mux.HandleFunc("/v1/records", auth(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method", 405)
			return
		}
		var req gbase8scdc.ReadRequest
		if !decode(w, r, &req) {
			return
		}
		out, err := provider.Read(r.Context(), req)
		if err == nil {
			err = gbase8scdc.ValidateReadResponse(req, out)
		}
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		writeJSON(w, out)
	}))
	return mux
}

func main() {
	provider, err := loadProvider()
	if err != nil {
		log.Fatal(err)
	}
	provider = gbase8scdc.ObserveAgent(provider)
	defer func() {
		if c, ok := provider.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	token := env("QMIGRATION_GBASE8S_CDC_AGENT_TOKEN", "")
	addr := env("QMIGRATION_GBASE8S_CDC_AGENT_LISTEN", "127.0.0.1:9188")
	cert, key := env("QMIGRATION_GBASE8S_CDC_AGENT_TLS_CERT_FILE", ""), env("QMIGRATION_GBASE8S_CDC_AGENT_TLS_KEY_FILE", "")
	if err := validateAgentExposure(addr, token, cert, key); err != nil {
		log.Fatal(err)
	}
	srv := &http.Server{Addr: addr, Handler: newMux(provider, token), ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 120 * time.Second, WriteTimeout: 120 * time.Second, IdleTimeout: 120 * time.Second, MaxHeaderBytes: 1 << 20}
	log.Printf("GBase 8s CDC agent listening on %s", addr)
	if cert != "" {
		err = srv.ListenAndServeTLS(cert, key)
	} else {
		err = srv.ListenAndServe()
	}
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(fmt.Errorf("CDC agent: %w", err))
	}
}
