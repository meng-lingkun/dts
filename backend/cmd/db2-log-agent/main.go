package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"qmigration/backend/internal/cdc/db2log"
)

func env(k, d string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return d
}

type commandProvider struct{ path string }

func (p commandProvider) run(ctx context.Context, args []string, out any) error {
	cmd := exec.CommandContext(ctx, p.path, args...)
	cmd.Env = os.Environ()
	b, err := cmd.Output()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			return fmt.Errorf("DB2 readlog provider: %w: %s", err, strings.TrimSpace(string(ee.Stderr)))
		}
		return err
	}
	if len(b) == 0 {
		return errors.New("DB2 readlog provider returned empty JSON")
	}
	if err := json.Unmarshal(b, out); err != nil {
		return fmt.Errorf("decode DB2 readlog provider JSON: %w", err)
	}
	return nil
}
func (p commandProvider) Position(ctx context.Context) (*db2log.PositionResponse, error) {
	var v db2log.PositionResponse
	err := p.run(ctx, []string{"position"}, &v)
	return &v, err
}
func (p commandProvider) Read(ctx context.Context, start db2log.LRI, maxRecords, maxBytes int) (*db2log.ReadResponse, error) {
	var v db2log.ReadResponse
	err := p.run(ctx, []string{"read", "--start-lri", start.String(), "--max-records", strconv.Itoa(maxRecords), "--max-bytes", strconv.Itoa(maxBytes)}, &v)
	return &v, err
}
func (p commandProvider) PureScaleStreams(ctx context.Context) (*db2log.PureScaleStreamsResponse, error) {
	var v db2log.PureScaleStreamsResponse
	err := p.run(ctx, []string{"streams"}, &v)
	return &v, err
}
func (p commandProvider) PureScaleRead(ctx context.Context, stream string, start db2log.LRI, maxRecords, maxBytes int) (*db2log.PureScaleReadResponse, error) {
	var v db2log.PureScaleReadResponse
	err := p.run(ctx, []string{"stream-read", "--stream", stream, "--start-lri", start.String(), "--max-records", strconv.Itoa(maxRecords), "--max-bytes", strconv.Itoa(maxBytes)}, &v)
	return &v, err
}

type server struct {
	provider commandProvider
	token    string
}

func (s *server) auth(w http.ResponseWriter, r *http.Request) bool {
	if s.token == "" {
		return true
	}
	got := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	if subtle.ConstantTimeCompare([]byte(got), []byte(s.token)) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return false
	}
	return true
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func (s *server) health(w http.ResponseWriter, r *http.Request) {
	if !s.auth(w, r) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	p, err := s.provider.Position(ctx)
	if err == nil {
		writeJSON(w, 200, map[string]any{"status": "ok", "mode": "single", "recoverable": p.Recoverable, "byte_order": p.ByteOrder})
		return
	}
	ps, psErr := s.provider.PureScaleStreams(ctx)
	if psErr != nil || len(ps.Streams) < 2 {
		http.Error(w, fmt.Sprintf("single position: %v; pureScale streams: %v", err, psErr), http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, 200, map[string]any{"status": "ok", "mode": "purescale", "streams": len(ps.Streams)})
}
func (s *server) position(w http.ResponseWriter, r *http.Request) {
	if !s.auth(w, r) {
		return
	}
	p, err := s.provider.Position(r.Context())
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	writeJSON(w, 200, p)
}
func (s *server) records(w http.ResponseWriter, r *http.Request) {
	if !s.auth(w, r) {
		return
	}
	start, err := db2log.ParseLRI(r.URL.Query().Get("start_lri"))
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	mr, _ := strconv.Atoi(r.URL.Query().Get("max_records"))
	mb, _ := strconv.Atoi(r.URL.Query().Get("max_bytes"))
	if mr <= 0 || mr > 16384 {
		mr = 4096
	}
	if mb <= 0 || mb > 256<<20 {
		mb = 32 << 20
	}
	v, err := s.provider.Read(r.Context(), start, mr, mb)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	writeJSON(w, 200, v)
}
func (s *server) streams(w http.ResponseWriter, r *http.Request) {
	if !s.auth(w, r) {
		return
	}
	v, err := s.provider.PureScaleStreams(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotImplemented)
		return
	}
	writeJSON(w, 200, v)
}
func (s *server) streamRecords(w http.ResponseWriter, r *http.Request) {
	if !s.auth(w, r) {
		return
	}
	prefix := "/v1/streams/"
	suffix := "/records"
	if !strings.HasPrefix(r.URL.Path, prefix) || !strings.HasSuffix(r.URL.Path, suffix) {
		http.NotFound(w, r)
		return
	}
	id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, prefix), suffix)
	id = strings.Trim(id, "/")
	if id == "" || strings.Contains(id, "/") {
		http.Error(w, "invalid stream", 400)
		return
	}
	start, err := db2log.ParseLRI(r.URL.Query().Get("start_lri"))
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	mr, _ := strconv.Atoi(r.URL.Query().Get("max_records"))
	mb, _ := strconv.Atoi(r.URL.Query().Get("max_bytes"))
	if mr <= 0 || mr > 16384 {
		mr = 4096
	}
	if mb <= 0 || mb > 256<<20 {
		mb = 32 << 20
	}
	v, err := s.provider.PureScaleRead(r.Context(), id, start, mr, mb)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	writeJSON(w, 200, v)
}
func (s *server) bootstrap(w http.ResponseWriter, r *http.Request) {
	if !s.auth(w, r) {
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", 405)
		return
	}
	var req db2log.BootstrapRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	if req.EndLRI.IsZero() || len(req.Tables) == 0 {
		http.Error(w, "end_lri and tables are required", 400)
		return
	}
	pos, err := s.provider.Position(r.Context())
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	if pos.InitialLRI.IsZero() {
		http.Error(w, "provider returned empty initial LRI", 500)
		return
	}
	cursor := pos.InitialLRI
	want := map[uint32]bool{}
	for _, t := range req.Tables {
		want[uint32(t.TablespaceID)<<16|uint32(t.TableID)] = true
	}
	found := map[uint32]db2log.RecordEnvelope{}
	scanned := 0
	bytesScanned := 0
	for db2log.CompareLRI(cursor, req.EndLRI) < 0 && len(found) < len(want) {
		rr, err := s.provider.Read(r.Context(), cursor, 8192, 64<<20)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		if len(rr.Records) == 0 {
			break
		}
		for _, e := range rr.Records {
			if db2log.CompareLRI(e.LRI, req.EndLRI) >= 0 {
				break
			}
			p, pe := db2log.ParseDataManager(e, nil, nil)
			if pe != nil {
				http.Error(w, pe.Error(), 500)
				return
			}
			if p != nil && p.Descriptor != nil {
				key := uint32(p.TablespaceID)<<16 | uint32(p.TableID)
				if want[key] {
					found[key] = e
				}
			}
			scanned++
			bytesScanned += len(e.RawBase64)
			if scanned > 2000000 || bytesScanned > 512<<20 {
				http.Error(w, "descriptor bootstrap scan exceeded safety bound", 422)
				return
			}
		}
		next := rr.NextStartLRI
		if next.IsZero() {
			next = rr.CurrentEndLRI
		}
		if next.IsZero() || db2log.CompareLRI(next, cursor) <= 0 {
			break
		}
		cursor = next
	}
	out := db2log.BootstrapResponse{}
	for _, t := range req.Tables {
		key := uint32(t.TablespaceID)<<16 | uint32(t.TableID)
		if e, ok := found[key]; ok {
			out.Records = append(out.Records, e)
		}
	}
	writeJSON(w, 200, out)
}
func main() {
	provider := env("QMIGRATION_DB2_READLOG_PROVIDER", "qmigration-db2-readlog-provider")
	if _, err := exec.LookPath(provider); err != nil {
		log.Fatalf("DB2 readlog provider %q not found: %v", provider, err)
	}
	s := &server{provider: commandProvider{path: provider}, token: strings.TrimSpace(os.Getenv("QMIGRATION_DB2_LOG_TOKEN"))}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/health", s.health)
	mux.HandleFunc("/v1/position", s.position)
	mux.HandleFunc("/v1/records", s.records)
	mux.HandleFunc("/v1/streams", s.streams)
	mux.HandleFunc("/v1/streams/", s.streamRecords)
	mux.HandleFunc("/v1/bootstrap", s.bootstrap)
	srv := &http.Server{Addr: env("QMIGRATION_DB2_LOG_LISTEN", ":8787"), Handler: mux, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 2 * time.Minute, WriteTimeout: 2 * time.Minute, IdleTimeout: 2 * time.Minute}
	cert, key := strings.TrimSpace(os.Getenv("QMIGRATION_DB2_LOG_TLS_CERT_FILE")), strings.TrimSpace(os.Getenv("QMIGRATION_DB2_LOG_TLS_KEY_FILE"))
	log.Printf("QMigration DB2 Log Agent listening on %s", srv.Addr)
	var err error
	if cert != "" || key != "" {
		if cert == "" || key == "" {
			log.Fatal("both DB2 log TLS cert/key files are required")
		}
		err = srv.ListenAndServeTLS(cert, key)
	} else {
		err = srv.ListenAndServe()
	}
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
