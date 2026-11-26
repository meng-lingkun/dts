//go:build integration

package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	pg "dts/backend/internal/connector/postgres"
	"dts/backend/internal/domain"
)

// Process-level acceptance is opt-in. It builds the current binaries and
// launches four direct child processes against a fresh disposable database.
func TestControlProcessFailover(t *testing.T) {
	requireIsolated(t)
	if os.Getenv("DTS_E2E_CONTROL_PROCESSES") != "1" {
		t.Skip("enable DTS_E2E_CONTROL_PROCESSES for process supervision tests")
	}
	dir := t.TempDir()
	build := func(name string) string {
		file := filepath.Join(dir, name)
		if runtime.GOOS == "windows" {
			file += ".exe"
		}
		cmd := exec.CommandContext(t.Context(), "go", "build", "-o", file, "./cmd/"+name)
		cmd.Dir = ".."
		if b, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("build %s: %v %s", name, err, b)
		}
		return file
	}
	server := build("server")
	worker := build("worker")
	ds := pgDS()
	conn, err := pg.NewFactory().New(ds)
	if err != nil {
		t.Fatal(err)
	}
	admin := conn.(*pg.Connector)
	t.Cleanup(func() { admin.Close() })
	secret := func() string {
		var key [32]byte
		if _, err := rand.Read(key[:]); err != nil {
			t.Fatal(err)
		}
		return hex.EncodeToString(key[:])
	}
	db := "dts_process_" + secret()[:16]
	if err := admin.ExecSQL(t.Context(), "CREATE DATABASE "+db); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := admin.ExecSQL(ctx, "DROP DATABASE "+db); err != nil {
			t.Error(err)
		}
	})
	pass, workerKey := secret(), secret()
	base := []string{}
	for _, v := range os.Environ() {
		k, _, _ := strings.Cut(v, "=")
		if !strings.HasPrefix(strings.ToUpper(k), "DTS_") && !strings.HasPrefix(strings.ToUpper(k), "OTEL_") {
			base = append(base, v)
		}
	}
	base = append(base, "DTS_REPOSITORY=postgres", "DTS_METADATA_HOST="+ds.Host, "DTS_METADATA_PORT="+strconv.Itoa(ds.Port), "DTS_METADATA_USER="+ds.Username, "DTS_METADATA_PASSWORD="+ds.Password, "DTS_METADATA_DATABASE="+db,
		"DTS_WORKER_TOKEN="+workerKey, "DTS_AUTH_SECRET="+secret(), "DTS_MASTER_KEY="+secret(), "DTS_AUTH_REQUIRED=true", "DTS_BOOTSTRAP_ADMIN_PASSWORD="+pass, "DTS_CDC_SPOOL_STORAGE=shared-fs", "DTS_CDC_SPOOL_DIR="+filepath.Join(dir, "spool"))
	serverA := freeAddress(t)
	endpointA := "http://" + serverA
	process := func(bin, label string, extra ...string) *childProcess {
		env := append(append([]string{}, base...), extra...)
		return launchChild(t, dir, bin, label, env)
	}
	client := &http.Client{Timeout: 2 * time.Second}
	t.Cleanup(client.CloseIdleConnections)
	do := func(method, url, token string, body, response any) (int, error) {
		var in bytes.Buffer
		if body != nil {
			if err := json.NewEncoder(&in).Encode(body); err != nil {
				return 0, err
			}
		}
		req, err := http.NewRequestWithContext(t.Context(), method, url, &in)
		if err != nil {
			return 0, err
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		res, err := client.Do(req)
		if err != nil {
			return 0, err
		}
		defer res.Body.Close()
		if res.StatusCode < 300 && response != nil {
			err = json.NewDecoder(res.Body).Decode(response)
		} else {
			_, err = io.Copy(io.Discard, res.Body)
		}
		return res.StatusCode, err
	}
	eventually := func(desc string, f func() bool) {
		t.Helper()
		deadline := time.Now().Add(25 * time.Second)
		for time.Now().Before(deadline) {
			if f() {
				return
			}
			time.Sleep(150 * time.Millisecond)
		}
		t.Fatalf("timeout waiting for %s", desc)
	}
	ready := func(server string) {
		eventually("readiness "+server, func() bool { code, err := do("GET", server+"/readyz", "", nil, nil); return err == nil && code == 200 })
	}
	a := process(server, "server-a", "DTS_ADDR="+serverA)
	ready(endpointA)
	serverB := freeAddress(t)
	endpointB := "http://" + serverB
	process(server, "server-b", "DTS_ADDR="+serverB, "DTS_METADATA_AUTO_MIGRATE=false")
	ready(endpointB)
	login := func(url string) string {
		var v struct{ Token string }
		status, err := do("POST", url+"/api/v1/auth/login", "", map[string]string{"username": "admin", "password": pass}, &v)
		if err != nil || status != 200 || v.Token == "" {
			t.Fatalf("login: status=%d err=%v", status, err)
		}
		return v.Token
	}
	old, live := login(endpointA), login(endpointB)
	var mine struct {
		CurrentID string `json:"current_id"`
	}
	code, err := do("GET", endpointB+"/api/v1/auth/sessions", old, nil, &mine)
	if err != nil || code != 200 || mine.CurrentID == "" {
		t.Fatalf("locate remote session: %d %v", code, err)
	}
	code, err = do("DELETE", endpointB+"/api/v1/auth/sessions/"+mine.CurrentID, live, nil, nil)
	if err != nil || code != 204 {
		t.Fatalf("remote revoke: %d %v", code, err)
	}
	verify := func(url, token string, want int) {
		t.Helper()
		got, err := do("GET", url+"/api/v1/auth/me", token, nil, nil)
		if err != nil || got != want {
			t.Fatalf("session at %s: got %d want %d, %v", url, got, want, err)
		}
	}
	verify(endpointA, old, 401)
	verify(endpointB, old, 401)
	var pat struct {
		Token string               `json:"token"`
		Item  domain.PersonalToken `json:"personal_token"`
	}
	code, err = do("POST", endpointA+"/api/v1/auth/personal-tokens", live, map[string]any{"name": "failover", "expires_at": time.Now().Add(time.Hour)}, &pat)
	if err != nil || code != 201 || pat.Token == "" || pat.Item.ID == "" {
		t.Fatalf("issue personal token: %d %v", code, err)
	}
	verify(endpointB, pat.Token, 200)
	process(worker, "worker-a", "DTS_SERVER="+endpointA, "DTS_WORKER_LABELS=slot=a")
	wb := process(worker, "worker-b", "DTS_SERVER="+endpointB, "DTS_WORKER_LABELS=slot=b")
	workers := func(url string) []domain.Worker {
		var items []domain.Worker
		code, err := do("GET", url+"/api/v1/workers", live, nil, &items)
		if code != 200 || err != nil {
			return nil
		}
		return items
	}
	var waID, wbID string
	eventually("two distinct registered workers observed through both servers", func() bool {
		items := workers(endpointA)
		if len(items) != 2 || len(workers(endpointB)) != 2 {
			return false
		}
		for _, w := range items {
			switch w.Labels["slot"] {
			case "a":
				waID = w.ID
			case "b":
				wbID = w.ID
			}
		}
		return waID != "" && wbID != "" && waID != wbID
	})
	t.Log("two servers and two workers registered against one PostgreSQL database")
	a.crash(t)
	verify(endpointB, live, 200)
	code, err = do("DELETE", endpointB+"/api/v1/auth/personal-tokens/"+pat.Item.ID, live, nil, nil)
	if err != nil || code != 204 {
		t.Fatalf("remote personal token revocation: %d %v", code, err)
	}
	verify(endpointB, pat.Token, 401)
	heartbeatAfter := func(url, id string, cutoff time.Time) bool {
		for _, w := range workers(url) {
			if w.ID == id && w.LastHeartbeat.After(cutoff) {
				return true
			}
		}
		return false
	}
	cutoff := time.Now()
	eventually("surviving worker advances heartbeat while server A is stopped", func() bool { return heartbeatAfter(endpointB, wbID, cutoff) })
	process(server, "server-a-restarted", "DTS_ADDR="+serverA, "DTS_METADATA_AUTO_MIGRATE=false")
	ready(endpointA)
	verify(endpointA, old, 401)
	verify(endpointA, live, 200)
	verify(endpointA, pat.Token, 401)
	t.Log("personal token was accepted on another replica and remained revoked after restart")
	cutoff = time.Now()
	eventually("original worker reconnects to restarted server", func() bool { return heartbeatAfter(endpointB, waID, cutoff) })
	wb.crash(t)
	process(worker, "worker-b-restarted", "DTS_SERVER="+endpointB, "DTS_WORKER_LABELS=slot=b")
	eventually("replacement worker registered with a fresh runtime ID", func() bool {
		for _, w := range workers(endpointA) {
			if w.Labels["slot"] == "b" && w.ID != wbID && w.ID != "" {
				return true
			}
		}
		return false
	})
	t.Log("surviving replica served sessions during the outage; revoked session stayed rejected after restart; surviving worker resumed heartbeat and replacement worker registered")
}

func freeAddress(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

type childProcess struct {
	cmd  *exec.Cmd
	done chan struct{}
	err  error
}

func launchChild(t *testing.T, dir, bin, label string, env []string) *childProcess {
	t.Helper()
	log, err := os.Create(filepath.Join(dir, label+".log"))
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin)
	cmd.Dir = dir
	cmd.Env = env
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		log.Close()
		t.Fatal(err)
	}
	p := &childProcess{cmd: cmd, done: make(chan struct{})}
	go func() { p.err = cmd.Wait(); log.Close(); close(p.done) }()
	t.Cleanup(func() {
		p.kill(t)
		if t.Failed() {
			if b, err := os.ReadFile(log.Name()); err == nil {
				if len(b) > 2500 {
					b = b[len(b)-2500:]
				}
				t.Logf("%s: %s", label, b)
			}
		}
	})
	return p
}
func (p *childProcess) kill(t *testing.T) {
	t.Helper()
	select {
	case <-p.done:
		return
	default:
	}
	if err := p.cmd.Process.Kill(); err != nil && err != os.ErrProcessDone {
		t.Error(fmt.Errorf("stop child: %w", err))
	}
	select {
	case <-p.done:
	case <-time.After(10 * time.Second):
		t.Fatal("child did not exit")
	}
}
func (p *childProcess) crash(t *testing.T) {
	t.Helper()
	select {
	case <-p.done:
		t.Fatalf("child exited before fault injection: %v", p.err)
	default:
	}
	p.kill(t)
}
