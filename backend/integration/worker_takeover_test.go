//go:build integration

package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	my "dts/backend/internal/connector/mysql"
	pg "dts/backend/internal/connector/postgres"
	"dts/backend/internal/domain"
)

// This test uses public HTTP endpoints, two real worker processes in sequence,
// and the unmodified production chunk lease. No test code updates metadata.
func TestWorkerTakeoverDuringFullLoad(t *testing.T) {
	requireIsolated(t)
	if os.Getenv("DTS_E2E_WORKER_TAKEOVER") != "1" {
		t.Skip("enable DTS_E2E_WORKER_TAKEOVER for the five-minute production lease test")
	}
	ctx := t.Context()
	dir := t.TempDir()
	build := func(name string) string {
		t.Helper()
		path := filepath.Join(dir, name)
		if runtime.GOOS == "windows" {
			path += ".exe"
		}
		cmd := exec.CommandContext(ctx, "go", "build", "-o", path, "./cmd/"+name)
		cmd.Dir = ".."
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("build %s: %v %s", name, err, output)
		}
		if runtime.GOOS == "windows" {
			// Process.Wait has finished at cleanup, but Windows can briefly
			// retain an executable image handle after a hard process kill.
			t.Cleanup(func() {
				deadline := time.Now().Add(30 * time.Second)
				for {
					err := os.Remove(path)
					if err == nil || os.IsNotExist(err) {
						return
					}
					if !os.IsPermission(err) || !time.Now().Before(deadline) {
						t.Errorf("remove test binary %s: %v", name, err)
						return
					}
					time.Sleep(200 * time.Millisecond)
				}
			})
		}
		return path
	}
	server, worker := build("server"), build("worker")
	pgSource := pgDS()
	raw, err := pg.NewFactory().New(pgSource)
	if err != nil {
		t.Fatal(err)
	}
	admin := raw.(*pg.Connector)
	t.Cleanup(func() { admin.Close() })
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	makeDB := func(prefix string) string {
		t.Helper()
		db := prefix + suffix
		if err := admin.ExecSQL(ctx, "CREATE DATABASE "+db); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if err := admin.ExecSQL(cleanup, "DROP DATABASE "+db); err != nil {
				t.Error(err)
			}
		})
		return db
	}
	metaDB := makeDB("takeover_meta_")
	pgSource.Database = makeDB("takeover_dst_")
	pgSource.Schema = "public"
	raw, err = pg.NewFactory().New(pgSource)
	if err != nil {
		t.Fatal(err)
	}
	target := raw.(*pg.Connector)
	t.Cleanup(func() { target.Close() })
	mySource := mysqlDS()
	raw, err = my.NewFactory().New(mySource)
	if err != nil {
		t.Fatal(err)
	}
	source := raw.(*my.Connector)
	t.Cleanup(func() { source.Close() })
	table := "takeover_" + suffix
	const nrows = 1000
	if err := source.ExecSQL(ctx, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, payload varchar(64) NOT NULL) ENGINE=InnoDB"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := source.ExecSQL(cleanup, "DROP TABLE "+table); err != nil {
			t.Error(err)
		}
	})
	values := make([]string, nrows)
	for i := range values {
		values[i] = fmt.Sprintf("(%d,'value_%04d')", i+1, i+1)
	}
	if err := source.ExecSQL(ctx, "INSERT INTO "+table+" VALUES "+strings.Join(values, ",")); err != nil {
		t.Fatal(err)
	}
	// Count destination row operations independently of control-plane progress.
	// Rows in the committed but unacknowledged batch must replay as updates.
	for _, ddl := range []string{
		"CREATE TABLE " + table + " (id bigint PRIMARY KEY, payload varchar(64) NOT NULL)",
		"CREATE TABLE observed_writes (id bigint NOT NULL, op text NOT NULL)",
		"CREATE FUNCTION observe_write() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO observed_writes VALUES (NEW.id, TG_OP); RETURN NEW; END $$",
		"CREATE TRIGGER observe AFTER INSERT OR UPDATE ON " + table + " FOR EACH ROW EXECUTE FUNCTION observe_write()",
	} {
		if err := target.ExecSQL(ctx, ddl); err != nil {
			t.Fatal(err)
		}
	}

	secret := func() string { return rand.Text() + rand.Text() }
	pass := secret()
	var base []string
	for _, v := range os.Environ() {
		key, _, _ := strings.Cut(strings.ToUpper(v), "=")
		if !strings.HasPrefix(key, "DTS_") && !strings.HasPrefix(key, "OTEL_") {
			base = append(base, v)
		}
	}
	addr := freeAddress(t)
	base = append(base, "DTS_REPOSITORY=postgres", "DTS_METADATA_HOST="+pgSource.Host,
		"DTS_METADATA_PORT="+strconv.Itoa(pgSource.Port), "DTS_METADATA_DATABASE="+metaDB,
		"DTS_METADATA_USER="+pgSource.Username, "DTS_METADATA_PASSWORD="+pgSource.Password,
		"DTS_WORKER_TOKEN="+secret(), "DTS_AUTH_SECRET="+secret(), "DTS_MASTER_KEY="+secret(),
		"DTS_AUTH_REQUIRED=true", "DTS_BOOTSTRAP_ADMIN_PASSWORD="+pass, "DTS_CDC_SPOOL_STORAGE=shared-fs",
		"DTS_CDC_SPOOL_DIR="+filepath.Join(dir, "spool"), "DTS_SERVER=http://"+addr,
		"DTS_WORKER_CONCURRENCY=1", "DTS_CDC_CONCURRENCY=1", "DTS_ADAPTIVE_BATCH=false")
	process := func(bin, label, extra string) *childProcess {
		return launchChild(t, dir, bin, label, append(append([]string{}, base...), extra))
	}
	process(server, "server", "DTS_ADDR="+addr)
	client := &http.Client{Timeout: 10 * time.Second}
	t.Cleanup(client.CloseIdleConnections)
	api := takeoverAPI{t: t, client: client, base: "http://" + addr}
	waitTakeover(t, "server readiness", 30*time.Second, 200*time.Millisecond, func() bool {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, api.base+"/readyz", nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := client.Do(req)
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})
	var login struct{ Token string }
	api.call("POST", "/auth/login", map[string]string{"username": "admin", "password": pass}, &login)
	api.token = login.Token
	register := func(name string, ds domain.DataSource) string {
		t.Helper()
		creds := map[string]any{"name": name, "host": ds.Host, "port": ds.Port, "type": ds.Type,
			"username": ds.Username, "password": ds.Password, "database": ds.Database, "schema": ds.Schema, "tls_mode": "DISABLE"}
		var out domain.DataSource
		api.call("POST", "/datasources", creds, &out)
		return out.ID
	}
	var task domain.MigrationTask
	api.call("POST", "/migrations", domain.MigrationTask{
		Name: "worker take over full load", Mode: domain.ModeFull,
		SourceID: register("source", mySource), TargetID: register("target", pgSource),
		Tables:      []domain.TableMapping{{SourceSchema: mySource.Database, SourceTable: table, TargetSchema: "public", TargetTable: table}},
		Parallelism: 1, ChunkRows: 10000, BatchRows: 50, MaxRetries: 3, RowsLimitPerSec: 50,
	}, &task)
	path := "/migrations/" + task.ID
	first := process(worker, "worker-a", "DTS_WORKER_LABELS=slot=a")
	api.call("POST", path+"/start", nil, nil)
	waitTakeover(t, "prepared migration", 30*time.Second, 150*time.Millisecond, func() bool {
		var state domain.MigrationTask
		api.call("GET", path, nil, &state)
		if state.Status == domain.StatusFailed {
			t.Fatal(state.LastError)
		}
		return state.Status == domain.StatusFullMigrating
	})
	chunk := func() domain.MigrationChunk {
		t.Helper()
		var chunks []domain.MigrationChunk
		api.call("GET", path+"/chunks", nil, &chunks)
		if len(chunks) != 1 || chunks[0].TaskID != task.ID {
			t.Fatalf("expected one chunk belonging to %s, got %+v", task.ID, chunks)
		}
		if chunks[0].Status == domain.ChunkFailed {
			t.Fatal(chunks[0].LastError)
		}
		return chunks[0]
	}
	scalar := func(query string) int64 {
		t.Helper()
		result, err := target.QuerySQL(ctx, query)
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Rows) != 1 || len(result.Rows[0]) != 1 {
			t.Fatal("expected one scalar row")
		}
		value, err := strconv.ParseInt(string(result.Rows[0][0]), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	count := func() int64 { return scalar("SELECT count(*) FROM " + table) }
	waitTakeover(t, "a target commit ahead of the durable checkpoint", 35*time.Second, 35*time.Millisecond, func() bool {
		c := chunk()
		if c.Status != domain.ChunkRunning || c.CursorJSON == "" || c.RowsWritten <= 0 {
			return false
		}
		committed := count()
		return committed > c.RowsWritten && committed < nrows
	})
	first.crash(t)
	killed := time.Now()
	old := chunk()
	committed := count()
	if old.WorkerID == "" || old.LeaseGeneration < 1 || old.RowsWritten < 1 || old.CursorJSON == "" ||
		committed <= old.RowsWritten || committed >= nrows || !old.LeaseUntil.After(killed) {
		t.Fatalf("failed to kill between destination commit and checkpoint: checkpoint=%d committed=%d cursor=%q lease=%s",
			old.RowsWritten, committed, old.CursorJSON, old.LeaseUntil)
	}
	t.Logf("killed worker=%s task=%s chunk=%s generation=%d cursor=%s checkpoint=%d destination=%d lease_left=%s",
		old.WorkerID, task.ID, old.ID, old.LeaseGeneration, old.CursorJSON, old.RowsWritten, committed, time.Until(old.LeaseUntil).Round(time.Second))
	process(worker, "worker-b", "DTS_WORKER_LABELS=slot=b")
	var replacementID string
	waitTakeover(t, "replacement worker registration", 20*time.Second, 200*time.Millisecond, func() bool {
		var workers []domain.Worker
		api.call("GET", "/workers", nil, &workers)
		for _, w := range workers {
			if w.Labels["slot"] == "b" && w.ID != "" && w.ID != old.WorkerID {
				replacementID = w.ID
				return true
			}
		}
		return false
	})
	var replacement domain.MigrationChunk
	waitTakeover(t, "natural lease expiry and fenced claim of original chunk", time.Until(old.LeaseUntil)+30*time.Second, time.Second, func() bool {
		c := chunk()
		if c.ID != old.ID {
			t.Fatalf("chunk replaced: %s -> %s", old.ID, c.ID)
		}
		if c.WorkerID != replacementID {
			return false
		}
		if c.LeaseGeneration != old.LeaseGeneration+1 || c.RetryCount != old.RetryCount+1 ||
			c.RowsWritten < old.RowsWritten || time.Now().Before(old.LeaseUntil) {
			t.Fatalf("invalid reclaimed lease: %+v", c)
		}
		replacement = c
		return true
	})
	t.Logf("replacement worker=%s claimed same task/chunk at generation=%d after %s", replacementID, replacement.LeaseGeneration, time.Since(killed).Round(time.Second))
	waitTakeover(t, "completed task", 60*time.Second, 250*time.Millisecond, func() bool {
		var state domain.MigrationTask
		api.call("GET", path, nil, &state)
		if state.Status == domain.StatusFailed {
			t.Fatal(state.LastError)
		}
		if state.Status != domain.StatusFinished {
			return false
		}
		if state.FinishedChunks != 1 || state.TotalChunks != 1 || state.RowsMigrated != nrows {
			t.Fatalf("terminal task reports %d/%d chunks and %d rows", state.FinishedChunks, state.TotalChunks, state.RowsMigrated)
		}
		return true
	})
	done := chunk()
	// Completing the chunk invalidates the last live lease in metadata. The
	// destination records the live generation that actually committed writes.
	if done.Status != domain.ChunkSuccess || done.ID != old.ID || done.WorkerID != replacementID ||
		!done.LeaseUntil.IsZero() || done.LeaseGeneration != replacement.LeaseGeneration+1 ||
		done.RowsWritten != nrows || done.RowsRead != nrows {
		t.Fatalf("unexpected final chunk: %+v", done)
	}
	if generation := scalar("SELECT generation FROM dts_internal.execution_fence WHERE scope='chunk/" + task.ID + "/" + old.ID + "'"); generation != replacement.LeaseGeneration {
		t.Fatalf("target fence generation=%d, expected %d", generation, replacement.LeaseGeneration)
	}
	actual, err := target.QuerySQL(ctx, "SELECT id,payload FROM "+table+" ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	if len(actual.Rows) != nrows {
		t.Fatalf("destination rows=%d", len(actual.Rows))
	}
	for i, row := range actual.Rows {
		if string(row[0]) != strconv.Itoa(i+1) || string(row[1]) != fmt.Sprintf("value_%04d", i+1) {
			t.Fatalf("incorrect destination row at index %d: %q", i, row)
		}
	}
	// Exactly the ambiguous interval replays. Records before the checkpoint
	// and rows first encountered by the replacement have one INSERT each.
	unexpected := scalar(fmt.Sprintf(`SELECT count(*) FROM (
		SELECT d.id FROM %s d LEFT JOIN observed_writes w ON w.id=d.id GROUP BY d.id
		HAVING count(*) FILTER (WHERE w.op='INSERT') <> 1
		    OR count(*) FILTER (WHERE w.op='UPDATE') <> CASE WHEN d.id > %d AND d.id <= %d THEN 1 ELSE 0 END
	) wrong`, table, old.RowsWritten, committed))
	if unexpected != 0 {
		t.Fatalf("unexpected replay history for %d rows", unexpected)
	}
	t.Logf("FINISHED; all %d destination keys and values verified; only %d committed, unacknowledged rows replayed; target fence generation=%d terminal metadata generation=%d", nrows, committed-old.RowsWritten, replacement.LeaseGeneration, done.LeaseGeneration)
}

type takeoverAPI struct {
	t           *testing.T
	client      *http.Client
	base, token string
}

func (a takeoverAPI) call(method, path string, body, out any) {
	a.t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			a.t.Fatal(err)
		}
	}
	req, err := http.NewRequestWithContext(a.t.Context(), method, a.base+"/api/v1"+path, &buf)
	if err != nil {
		a.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if a.token != "" {
		req.Header.Set("Authorization", "Bearer "+a.token)
	}
	res, err := a.client.Do(req)
	if err != nil {
		a.t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		var message struct{ Error string }
		json.NewDecoder(io.LimitReader(res.Body, 2048)).Decode(&message)
		a.t.Fatalf("%s %s: HTTP %d %s", method, path, res.StatusCode, message.Error)
	}
	if out != nil {
		if err := json.NewDecoder(res.Body).Decode(out); err != nil {
			a.t.Fatal(err)
		}
	} else {
		io.Copy(io.Discard, res.Body)
	}
}

func waitTakeover(t *testing.T, desc string, timeout, interval time.Duration, check func() bool) {
	t.Helper()
	end := time.Now().Add(timeout)
	for time.Now().Before(end) {
		if check() {
			return
		}
		time.Sleep(interval)
	}
	t.Fatalf("timeout waiting for %s", desc)
}
