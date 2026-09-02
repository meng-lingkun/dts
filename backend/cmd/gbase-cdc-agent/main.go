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
	"strings"
	"time"

	"qmigration/backend/internal/cdc/gbase8acdc"
)

func env(k, d string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return d
}
func providerConfig() (string, error) {
	direct := strings.TrimSpace(os.Getenv("QMIGRATION_GBASE8A_CDC_PROVIDER_CONFIG_JSON"))
	path := strings.TrimSpace(os.Getenv("QMIGRATION_GBASE8A_CDC_PROVIDER_CONFIG_FILE"))
	if direct != "" && path != "" {
		return "", errors.New("set only one GBase 8a provider config source")
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
	if !st.Mode().IsRegular() || st.Mode().Perm()&0o007 != 0 {
		return "", errors.New("GBase 8a provider config must be a regular file inaccessible to other users")
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
		return "", errors.New("GBase 8a provider config exceeds 1 MiB")
	}
	return string(b), nil
}
func loadProvider() (gbase8acdc.Agent, error) {
	library := strings.TrimSpace(os.Getenv("QMIGRATION_GBASE8A_CDC_PROVIDER_LIBRARY"))
	goPlugin := strings.TrimSpace(os.Getenv("QMIGRATION_GBASE8A_CDC_PROVIDER_PLUGIN"))
	if library != "" && goPlugin != "" {
		return nil, errors.New("configure only one GBase 8a CDC provider")
	}
	if library != "" {
		cfg, err := providerConfig()
		if err != nil {
			return nil, err
		}
		return gbase8acdc.OpenNativeProvider(library, os.Getenv("QMIGRATION_GBASE8A_CDC_PROVIDER_SHA256"), cfg)
	}
	if goPlugin == "" {
		return nil, errors.New("set QMIGRATION_GBASE8A_CDC_PROVIDER_LIBRARY or legacy QMIGRATION_GBASE8A_CDC_PROVIDER_PLUGIN")
	}
	p, err := plugin.Open(goPlugin)
	if err != nil {
		return nil, err
	}
	sym, err := p.Lookup("NewProvider")
	if err != nil {
		return nil, err
	}
	fn, ok := sym.(func() (gbase8acdc.Agent, error))
	if !ok {
		return nil, errors.New("GBase 8a provider plugin NewProvider has incompatible signature")
	}
	return fn()
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
func loopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	host = strings.Trim(strings.TrimSpace(host), "[]")
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
func bearer(header, token string) bool {
	want := "Bearer " + token
	if len(header) != len(want) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(header), []byte(want)) == 1
}
func newMux(p gbase8acdc.Agent, token string) http.Handler {
	auth := func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if token != "" && !bearer(r.Header.Get("Authorization"), token) {
				http.Error(w, "unauthorized", 401)
				return
			}
			next(w, r)
		}
	}
	m := http.NewServeMux()
	m.HandleFunc("/v1/health", auth(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()
		if err := p.Health(ctx); err != nil {
			http.Error(w, err.Error(), 503)
			return
		}
		writeJSON(w, map[string]any{"status": "ok", "api_version": "gbase8a-cdc-agent-v1"})
	}))
	m.HandleFunc("/v1/checkpoint", auth(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method", 405)
			return
		}
		var q gbase8acdc.CheckpointRequest
		if !decode(w, r, &q) {
			return
		}
		out, err := p.Checkpoint(r.Context(), q)
		if err == nil {
			lineage, e := gbase8acdc.NormalizeLineage(out.CaptureLineage)
			err = e
			if err == nil && lineage == "" {
				err = errors.New("empty lineage")
			}
			if err == nil && out.TransactionAtomicity != "COMMITTED_TXN_V1" {
				err = errors.New("provider lacks COMMITTED_TXN_V1 checkpoint proof")
			}
			if err == nil {
				err = gbase8acdc.ValidateSchemaFences(q.Tables, out.SchemaFences)
			}
		}
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		writeJSON(w, out)
	}))
	m.HandleFunc("/v1/read", auth(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method", 405)
			return
		}
		var q gbase8acdc.ReadRequest
		if !decode(w, r, &q) {
			return
		}
		out, err := p.Read(r.Context(), q)
		if err == nil {
			after, lineage, e := gbase8acdc.ParsePosition("seq=" + q.AfterSequence + ";capture=" + q.ExpectedCaptureLineage)
			if e != nil {
				err = e
			} else {
				err = gbase8acdc.ValidateReadResponseForAgent(out, after, lineage, q.Tables)
			}
		}
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		writeJSON(w, out)
	}))
	m.HandleFunc("/v1/ack", auth(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method", 405)
			return
		}
		var q gbase8acdc.AckRequest
		if !decode(w, r, &q) {
			return
		}
		if err := p.Ack(r.Context(), q); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.WriteHeader(204)
	}))
	return m
}
func main() {
	p, err := loadProvider()
	if err != nil {
		log.Fatal(err)
	}
	if c, ok := p.(interface{ Close() error }); ok {
		defer c.Close()
	}
	addr := env("QMIGRATION_GBASE8A_CDC_AGENT_LISTEN", "127.0.0.1:9189")
	token := env("QMIGRATION_GBASE8A_CDC_AGENT_TOKEN", "")
	cert, key := env("QMIGRATION_GBASE8A_CDC_AGENT_TLS_CERT_FILE", ""), env("QMIGRATION_GBASE8A_CDC_AGENT_TLS_KEY_FILE", "")
	if (cert == "") != (key == "") {
		log.Fatal("both TLS cert/key are required")
	}
	if !loopback(addr) && (token == "" || cert == "") {
		log.Fatal("non-loopback GBase 8a CDC agent requires bearer token and TLS")
	}
	srv := &http.Server{Addr: addr, Handler: newMux(p, token), ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 120 * time.Second, WriteTimeout: 120 * time.Second, IdleTimeout: 120 * time.Second, MaxHeaderBytes: 1 << 20}
	log.Printf("GBase 8a CDC agent listening on %s", addr)
	if cert != "" {
		err = srv.ListenAndServeTLS(cert, key)
	} else {
		err = srv.ListenAndServe()
	}
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(fmt.Errorf("GBase 8a CDC agent: %w", err))
	}
}
