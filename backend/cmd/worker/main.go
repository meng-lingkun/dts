package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"qmigration/backend/internal/connector"
	damengconnector "qmigration/backend/internal/connector/dameng"
	db2connector "qmigration/backend/internal/connector/db2"
	gbaseconnector "qmigration/backend/internal/connector/gbase"
	gbase8sconnector "qmigration/backend/internal/connector/gbase8s"
	mysqlconnector "qmigration/backend/internal/connector/mysql"
	oracleconnector "qmigration/backend/internal/connector/oracle"
	postgresconnector "qmigration/backend/internal/connector/postgres"
	sqlserverconnector "qmigration/backend/internal/connector/sqlserver"
	"qmigration/backend/internal/domain"
	fullpipeline "qmigration/backend/internal/pipeline"
	"qmigration/backend/internal/transform"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type runtimeWorker struct {
	ID             string            `json:"id"`
	Hostname       string            `json:"hostname"`
	CPU            int               `json:"cpu"`
	MemoryMB       int               `json:"memory_mb"`
	Status         string            `json:"status"`
	RunningJobs    int               `json:"running_jobs"`
	CPUUsagePct    float64           `json:"cpu_usage_pct,omitempty"`
	MemoryUsagePct float64           `json:"memory_usage_pct,omitempty"`
	NetworkRxBps   int64             `json:"network_rx_bps,omitempty"`
	NetworkTxBps   int64             `json:"network_tx_bps,omitempty"`
	Capabilities   []string          `json:"capabilities"`
	Labels         map[string]string `json:"labels,omitempty"`
}

var runningJobs atomic.Int32
var httpClient = &http.Client{Timeout: 70 * time.Second}

type hostCounters struct {
	cpuTotal uint64
	cpuIdle  uint64
	memTotal uint64
	memAvail uint64
	rxBytes  uint64
	txBytes  uint64
	at       time.Time
}

func readHostCounters() hostCounters {
	c := hostCounters{at: time.Now()}
	if b, err := os.ReadFile("/proc/stat"); err == nil {
		for _, line := range strings.Split(string(b), "\n") {
			f := strings.Fields(line)
			if len(f) < 5 || f[0] != "cpu" {
				continue
			}
			for i := 1; i < len(f); i++ {
				v, _ := strconv.ParseUint(f[i], 10, 64)
				c.cpuTotal += v
				if i == 4 || i == 5 {
					c.cpuIdle += v
				}
			}
			break
		}
	}
	if b, err := os.ReadFile("/proc/meminfo"); err == nil {
		for _, line := range strings.Split(string(b), "\n") {
			f := strings.Fields(line)
			if len(f) < 2 {
				continue
			}
			v, _ := strconv.ParseUint(f[1], 10, 64)
			switch strings.TrimSuffix(f[0], ":") {
			case "MemTotal":
				c.memTotal = v * 1024
			case "MemAvailable":
				c.memAvail = v * 1024
			}
		}
	}
	if b, err := os.ReadFile("/proc/net/dev"); err == nil {
		for _, line := range strings.Split(string(b), "\n") {
			if !strings.Contains(line, ":") {
				continue
			}
			parts := strings.SplitN(line, ":", 2)
			iface := strings.TrimSpace(parts[0])
			if iface == "lo" {
				continue
			}
			f := strings.Fields(parts[1])
			if len(f) < 9 {
				continue
			}
			rx, _ := strconv.ParseUint(f[0], 10, 64)
			tx, _ := strconv.ParseUint(f[8], 10, 64)
			c.rxBytes += rx
			c.txBytes += tx
		}
	}
	return c
}

func applyHostUsage(w *runtimeWorker, previous, current hostCounters) {
	if current.memTotal > 0 {
		w.MemoryMB = int(current.memTotal / 1024 / 1024)
		w.MemoryUsagePct = float64(current.memTotal-current.memAvail) * 100 / float64(current.memTotal)
	}
	if previous.cpuTotal > 0 && current.cpuTotal > previous.cpuTotal {
		dTotal := current.cpuTotal - previous.cpuTotal
		dIdle := current.cpuIdle - previous.cpuIdle
		w.CPUUsagePct = float64(dTotal-dIdle) * 100 / float64(dTotal)
	}
	seconds := current.at.Sub(previous.at).Seconds()
	if seconds > 0 && previous.at.Unix() > 0 {
		if current.rxBytes >= previous.rxBytes {
			w.NetworkRxBps = int64(float64(current.rxBytes-previous.rxBytes) / seconds)
		}
		if current.txBytes >= previous.txBytes {
			w.NetworkTxBps = int64(float64(current.txBytes-previous.txBytes) / seconds)
		}
	}
}

func workerLookPath(name string) (string, error) {
	if p, err := exec.LookPath(name); err == nil {
		return p, nil
	}
	dirs := []string{}
	if d := strings.TrimSpace(os.Getenv("QMIGRATION_BIN_DIR")); d != "" {
		dirs = append(dirs, d)
	}
	if exe, err := os.Executable(); err == nil {
		dirs = append(dirs, filepath.Dir(exe))
	}
	for _, d := range dirs {
		p := filepath.Join(d, name)
		if st, err := os.Stat(p); err == nil && !st.IsDir() && st.Mode()&0111 != 0 {
			return p, nil
		}
	}
	return "", fmt.Errorf("executable %q not found in PATH, QMIGRATION_BIN_DIR or Worker binary directory", name)
}

func parseWorkerLabels(raw string) map[string]string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	labels := map[string]string{}
	if strings.HasPrefix(raw, "{") {
		if err := json.Unmarshal([]byte(raw), &labels); err == nil {
			return labels
		}
	}
	for _, item := range strings.Split(raw, ",") {
		parts := strings.SplitN(strings.TrimSpace(item), "=", 2)
		if len(parts) != 2 {
			continue
		}
		key, value := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
		if key != "" && value != "" {
			labels[key] = value
		}
	}
	if len(labels) == 0 {
		return nil
	}
	return labels
}

func detect() []string {
	// A Worker advertises QMigration protocol capabilities, never third-party
	// executables. The control plane therefore schedules one unified engine.
	out := []string{"qmigration", "qmigration:mysql-full", "qmigration:postgres-full"}
	if gaussDBExperimentalEnabled() {
		out = append(out, "qmigration:gaussdb-full-experimental")
	}
	if oracleExperimentalEnabled() {
		out = append(out, "qmigration:oracle-full-experimental")
	}
	if sqlServerExperimentalEnabled() {
		out = append(out, "qmigration:sqlserver-full-experimental")
	}
	if db2ExperimentalEnabled() {
		out = append(out, "qmigration:db2-full-experimental")
	}
	if damengExperimentalEnabled() {
		out = append(out, "qmigration:dameng-full-experimental")
	}
	if gbaseExperimentalEnabled() {
		out = append(out, "qmigration:gbase8a-full-experimental")
		if envOn("QMIGRATION_EXPERIMENTAL_GBASE8A_TRANSACTIONAL_TARGET_CDC") {
			out = append(out, "qmigration:gbase8a-transactional-target-cdc-experimental")
		}
		if envOn("QMIGRATION_EXPERIMENTAL_GBASE8A_SOURCE_CDC") {
			if _, err := workerLookPath("qmigration-gbase-cdc"); err == nil {
				out = append(out, "qmigration:gbase8a-provider-cdc-experimental")
			}
		}
	}
	if gbase8sExperimentalEnabled() {
		out = append(out, "qmigration:gbase8s-full-target-experimental")
		if envOn("QMIGRATION_EXPERIMENTAL_GBASE8S_CDC") {
			out = append(out, "qmigration:gbase8s-csdk-cdc-experimental")
		}
	}
	if _, err := workerLookPath("qmigration-mysql-cdc"); err == nil {
		out = append(out, "qmigration:mysql-cdc", "qmigration:oceanbase-binlog-cdc")
		zstdBin := strings.TrimSpace(os.Getenv("QMIGRATION_ZSTD_BIN"))
		if zstdBin == "" {
			zstdBin = "zstd"
		}
		if _, err := exec.LookPath(zstdBin); err == nil {
			out = append(out, "qmigration:mysql-cdc-zstd")
		}
	}
	if _, err := workerLookPath("qmigration-tidb-cdc"); err == nil {
		out = append(out, "qmigration:tidb-ticdc")
	}
	if _, err := workerLookPath("qmigration-postgres-cdc"); err == nil {
		out = append(out, "qmigration:postgres-cdc")
		if envOn("QMIGRATION_EXPERIMENTAL_KINGBASE_LOGICAL_CDC") {
			out = append(out, "qmigration:kingbase-kboutput-cdc-experimental")
		}
	}
	if envOn("QMIGRATION_EXPERIMENTAL_OPENGAUSS_LOGICAL_CDC") {
		if _, err := workerLookPath("qmigration-opengauss-cdc"); err == nil {
			out = append(out, "qmigration:opengauss-logical-cdc-experimental")
		}
	}
	if gaussDBCDCExperimentalEnabled() {
		if _, err := workerLookPath("qmigration-gaussdb-cdc"); err == nil {
			out = append(out, "qmigration:gaussdb-cdc-experimental")
		}
	}
	if sqlServerCDCExperimentalEnabled() {
		if _, err := workerLookPath("qmigration-sqlserver-cdc"); err == nil {
			out = append(out, "qmigration:sqlserver-cdc-experimental")
		}
	}
	if oracleCDCExperimentalEnabled() {
		if _, err := workerLookPath("qmigration-oracle-cdc"); err == nil {
			out = append(out, "qmigration:oracle-cdc-experimental")
		}
	}
	if db2CDCExperimentalEnabled() {
		if _, err := workerLookPath("qmigration-db2-cdc"); err == nil {
			out = append(out, "qmigration:db2-cdc-experimental")
		}
	}
	return out
}
func gaussDBExperimentalEnabled() bool { return envOn("QMIGRATION_EXPERIMENTAL_GAUSSDB_NATIVE") }
func gaussDBCDCExperimentalEnabled() bool {
	return gaussDBExperimentalEnabled() && envOn("QMIGRATION_EXPERIMENTAL_GAUSSDB_LOGICAL_CDC")
}

func sqlServerCDCExperimentalEnabled() bool {
	return sqlServerExperimentalEnabled() && envOn("QMIGRATION_EXPERIMENTAL_SQLSERVER_CDC")
}

func envOn(name string) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(name)))
	return v == "1" || v == "true" || v == "yes" || v == "on"
}
func oracleExperimentalEnabled() bool { return envOn("QMIGRATION_EXPERIMENTAL_ORACLE_NATIVE") }
func oracleCDCExperimentalEnabled() bool {
	native := strings.ToLower(strings.TrimSpace(os.Getenv("QMIGRATION_EXPERIMENTAL_ORACLE_NATIVE")))
	cdc := strings.ToLower(strings.TrimSpace(os.Getenv("QMIGRATION_EXPERIMENTAL_ORACLE_LOGMINER_CDC")))
	on := func(v string) bool { return v == "1" || v == "true" || v == "yes" || v == "on" }
	return on(native) && on(cdc)
}
func db2ExperimentalEnabled() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("QMIGRATION_EXPERIMENTAL_DB2_NATIVE")))
	return v == "1" || v == "true" || v == "yes" || v == "on"
}
func db2CDCExperimentalEnabled() bool {
	return db2ExperimentalEnabled() && envOn("QMIGRATION_EXPERIMENTAL_DB2_LOG_CDC")
}
func damengExperimentalEnabled() bool {
	return envOn("QMIGRATION_EXPERIMENTAL_DAMENG_NATIVE")
}
func gbaseExperimentalEnabled() bool {
	return envOn("QMIGRATION_EXPERIMENTAL_GBASE8A_NATIVE")
}
func gbase8sExperimentalEnabled() bool {
	return envOn("QMIGRATION_EXPERIMENTAL_GBASE8S_NATIVE")
}

func sqlServerExperimentalEnabled() bool {
	return envOn("QMIGRATION_EXPERIMENTAL_SQLSERVER_NATIVE")
}

func doJSON(method, url string, in, out any) (int, error) {
	var body *bytes.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return 0, err
		}
		body = bytes.NewReader(b)
	} else {
		body = bytes.NewReader(nil)
	}
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	if token := os.Getenv("QMIGRATION_WORKER_TOKEN"); token != "" {
		req.Header.Set("X-QMigration-Worker-Token", token)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return resp.StatusCode, fmt.Errorf("http %s", resp.Status)
	}
	if out != nil && resp.StatusCode != http.StatusNoContent {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return resp.StatusCode, err
		}
	}
	return resp.StatusCode, nil
}
func post(url string, in, out any) error { _, err := doJSON(http.MethodPost, url, in, out); return err }

func credentialToDS(c domain.DataSourceCredential) domain.DataSource {
	return domain.DataSource{Type: c.Type, Host: c.Host, Port: c.Port, Username: c.Username, Password: c.Password, Database: c.Database, Schema: c.Schema, JDBCURL: c.JDBCURL, DriverClass: c.DriverClass, CDCURL: c.CDCURL, TLSMode: c.TLSMode, TLSServerName: c.TLSServerName, TLSCACert: c.TLSCACert, TLSClientCert: c.TLSClientCert, TLSClientKey: c.TLSClientKey}
}
func throttle(bytes int64, mbps int64, started time.Time) {
	if mbps <= 0 {
		return
	}
	throttleBytesPerSec(bytes, mbps*(1<<20), started)
}

func throttleBytesPerSec(bytes, bytesPerSec int64, started time.Time) {
	if bytesPerSec <= 0 || bytes <= 0 {
		return
	}
	target := time.Duration(float64(bytes) / float64(bytesPerSec) * float64(time.Second))
	if remain := target - time.Since(started); remain > 0 {
		time.Sleep(remain)
	}
}
func throttleUnits(units int64, perSecond int64, started time.Time) {
	if units <= 0 || perSecond <= 0 {
		return
	}
	want := time.Duration(float64(units) / float64(perSecond) * float64(time.Second))
	if remain := want - time.Since(started); remain > 0 {
		time.Sleep(remain)
	}
}

type tailBuffer struct {
	mu  sync.Mutex
	max int
	b   []byte
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.b = append(t.b, p...)
	if len(t.b) > t.max {
		t.b = append([]byte(nil), t.b[len(t.b)-t.max:]...)
	}
	return len(p), nil
}
func (t *tailBuffer) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return string(append([]byte(nil), t.b...))
}

func executeManagedEngineJob(parent context.Context, server, workerID string, claim *domain.EngineJobClaim) domain.EngineJobResult {
	cfg := claim.RuntimeConfig
	if len(cfg.Command) == 0 {
		return domain.EngineJobResult{Error: "managed engine configuration has no executable command"}
	}
	dir, err := os.MkdirTemp("", "qmigration-cdc-*")
	if err != nil {
		return domain.EngineJobResult{Error: err.Error()}
	}
	defer os.RemoveAll(dir)
	configPath := filepath.Join(dir, filepath.Base(cfg.Filename))
	if err = os.WriteFile(configPath, []byte(cfg.Content), 0600); err != nil {
		return domain.EngineJobResult{Error: err.Error()}
	}
	args := append([]string(nil), cfg.Command...)
	for i := range args {
		if args[i] == cfg.Filename {
			args[i] = configPath
		}
	}
	binary, err := workerLookPath(args[0])
	if err != nil {
		return domain.EngineJobResult{Error: fmt.Sprintf("engine %s executable %q not found: %v", cfg.Engine, args[0], err)}
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, args[1:]...)
	cmd.Dir = dir
	cmd.Env = os.Environ()
	for key, value := range cfg.Env {
		cmd.Env = append(cmd.Env, key+"="+value)
	}
	cmd.Env = append(cmd.Env,
		"QMIGRATION_SERVER="+server,
		"QMIGRATION_TASK_ID="+claim.Job.TaskID,
		"QMIGRATION_WORKER_ID="+workerID,
		"QMIGRATION_ENGINE_JOB_ID="+claim.Job.ID,
		"QMIGRATION_CDC_DIRECTION="+claim.Job.Direction,
		"QMIGRATION_CDC_ENDPOINT="+fmt.Sprintf("%s/api/v1/workers/%s/engine-jobs/%s/cdc/events", server, workerID, claim.Job.ID),
		"QMIGRATION_CDC_READY_ENDPOINT="+fmt.Sprintf("%s/api/v1/workers/%s/engine-jobs/%s/cdc/ready", server, workerID, claim.Job.ID),
	)
	tail := &tailBuffer{max: 12000}
	cmd.Stdout = tail
	cmd.Stderr = tail
	if err = cmd.Start(); err != nil {
		return domain.EngineJobResult{Error: fmt.Sprintf("start engine %s: %v", cfg.Engine, err)}
	}
	startedURL := fmt.Sprintf("%s/api/v1/workers/%s/engine-jobs/%s/started", server, workerID, claim.Job.ID)
	if err = post(startedURL, nil, nil); err != nil {
		cancel()
		_ = cmd.Wait()
		return domain.EngineJobResult{Error: fmt.Sprintf("report engine start: %v", err), OutputTail: tail.String()}
	}
	waitCh := make(chan error, 1)
	go func() { waitCh <- cmd.Wait() }()
	leaseTicker := time.NewTicker(time.Minute)
	defer leaseTicker.Stop()
	controlTicker := time.NewTicker(5 * time.Second)
	defer controlTicker.Stop()
	stopping := false
	for {
		select {
		case runErr := <-waitCh:
			out := tail.String()
			if stopping {
				return domain.EngineJobResult{OutputTail: out}
			}
			if runErr != nil {
				return domain.EngineJobResult{Error: fmt.Sprintf("engine %s exited: %v", cfg.Engine, runErr), OutputTail: out}
			}
			return domain.EngineJobResult{OutputTail: out}
		case <-leaseTicker.C:
			leaseURL := fmt.Sprintf("%s/api/v1/workers/%s/engine-jobs/%s/lease", server, workerID, claim.Job.ID)
			if err := post(leaseURL, nil, nil); err != nil {
				stopping = true
				cancel()
			}
		case <-controlTicker.C:
			var ctl struct {
				Stop bool `json:"stop"`
			}
			ctlURL := fmt.Sprintf("%s/api/v1/workers/%s/engine-jobs/%s/control", server, workerID, claim.Job.ID)
			if err := post(ctlURL, nil, &ctl); err != nil {
				continue
			}
			if ctl.Stop && !stopping {
				stopping = true
				cancel()
			}
		}
	}
}

type numericChunkCursor struct {
	AfterPK int64 `json:"after_pk"`
}

func numericRangeSplit(splitType string) bool {
	switch strings.ToUpper(strings.TrimSpace(splitType)) {
	case "PRIMARY_KEY_RANGE", "PK_RANGE", "PK_RANGE_ADAPTIVE", "PK_RANGE_REBALANCED":
		return true
	default:
		return false
	}
}

func executeJob(ctx context.Context, server, workerID string, job *domain.ChunkJob) (result domain.ChunkResult) {
	started := time.Now()
	defer func() { result.DurationMS = time.Since(started).Milliseconds() }()
	newConnector := func(c domain.DataSourceCredential) (connector.Connector, error) {
		ds := credentialToDS(c)
		switch {
		case c.Type.IsPostgreSQLWireCompatible():
			if c.Type == domain.DataSourceGaussDB && !gaussDBExperimentalEnabled() {
				return nil, fmt.Errorf("GaussDB native worker requires QMIGRATION_EXPERIMENTAL_GAUSSDB_NATIVE=1")
			}
			return postgresconnector.NewFactory().New(ds)
		case c.Type.IsMySQLFamily():
			return mysqlconnector.NewFactory().New(ds)
		case c.Type == domain.DataSourceSQLServer && sqlServerExperimentalEnabled():
			return sqlserverconnector.NewFactory().New(ds)
		case c.Type == domain.DataSourceOracle && oracleExperimentalEnabled():
			return oracleconnector.NewFactory().New(ds)
		case c.Type == domain.DataSourceDB2 && db2ExperimentalEnabled():
			return db2connector.NewFactory().New(ds)
		case c.Type == domain.DataSourceDameng && damengExperimentalEnabled():
			return damengconnector.NewFactory().New(ds)
		case c.Type == domain.DataSourceGBase && gbaseExperimentalEnabled():
			return gbaseconnector.NewFactory().New(ds)
		case c.Type == domain.DataSourceGBase8s && gbase8sExperimentalEnabled():
			return gbase8sconnector.NewFactory().New(ds)
		default:
			return nil, fmt.Errorf("QMigration native worker connector is not implemented for %s", c.Type)
		}
	}
	srcRaw, err := newConnector(job.Source)
	if err != nil {
		result.Error = err.Error()
		return
	}
	defer srcRaw.Close()
	dstRaw, err := newConnector(job.Target)
	if err != nil {
		result.Error = err.Error()
		return
	}
	defer dstRaw.Close()
	src, ok := srcRaw.(connector.DataConnector)
	if !ok {
		result.Error = "source QMigration connector does not implement data transfer"
		return
	}
	dst, ok := dstRaw.(connector.DataConnector)
	if !ok {
		result.Error = "target QMigration connector does not implement data transfer"
		return
	}

	var readBytesPerSec atomic.Int64
	var writeBytesPerSec atomic.Int64
	initialReadBPS := job.ReadBytesPerSec
	if initialReadBPS <= 0 && job.ReadLimitMBps > 0 {
		initialReadBPS = job.ReadLimitMBps * (1 << 20)
	}
	initialWriteBPS := job.WriteBytesPerSec
	if initialWriteBPS <= 0 && job.WriteLimitMBps > 0 {
		initialWriteBPS = job.WriteLimitMBps * (1 << 20)
	}
	readBytesPerSec.Store(initialReadBPS)
	writeBytesPerSec.Store(initialWriteBPS)

	useKeyset := strings.Contains(job.Chunk.SplitType, "KEYSET")
	var keyCursor, lowerBound, upperBound []connector.Value
	if useKeyset && strings.TrimSpace(job.Chunk.CursorJSON) != "" {
		if err := json.Unmarshal([]byte(job.Chunk.CursorJSON), &keyCursor); err != nil {
			result.Error = fmt.Sprintf("decode keyset cursor: %v", err)
			return
		}
	}
	if useKeyset && strings.TrimSpace(job.Chunk.StartCursorJSON) != "" {
		if err := json.Unmarshal([]byte(job.Chunk.StartCursorJSON), &lowerBound); err != nil {
			result.Error = fmt.Sprintf("decode keyset lower bound: %v", err)
			return
		}
	}
	if useKeyset && strings.TrimSpace(job.Chunk.EndCursorJSON) != "" {
		if err := json.Unmarshal([]byte(job.Chunk.EndCursorJSON), &upperBound); err != nil {
			result.Error = fmt.Sprintf("decode keyset upper bound: %v", err)
			return
		}
	}
	var after int64
	hasAfter := false
	if !useKeyset && numericRangeSplit(job.Chunk.SplitType) && strings.TrimSpace(job.Chunk.CursorJSON) != "" {
		var cur numericChunkCursor
		if err := json.Unmarshal([]byte(job.Chunk.CursorJSON), &cur); err != nil {
			result.Error = fmt.Sprintf("decode numeric chunk cursor: %v", err)
			return
		}
		after, hasAfter = cur.AfterPK, true
	}

	targetCols := job.Table.TargetColumns
	if len(targetCols) == 0 {
		targetCols = job.Table.Columns
	}
	targetPK := job.Table.TargetPrimaryKey
	if targetPK == "" {
		targetPK = job.Table.PrimaryKey
	}
	targetPKs := job.Table.TargetPrimaryKeys
	if len(targetPKs) == 0 && targetPK != "" {
		targetPKs = []string{targetPK}
	}
	valuePlan, err := transform.CompileWithRules(job.Table.Columns, targetCols, job.TransformRules, job.Table.SourceSchema, job.Table.SourceTable)
	if err != nil {
		result.Error = fmt.Sprintf("compile QMigration transform plan: %v", err)
		return
	}

	reader := func(readCtx context.Context, limit int) (*fullpipeline.Batch, error) {
		readStarted := time.Now()
		batch, e := src.ReadBatch(readCtx, connector.ReadBatchRequest{
			Schema: job.Table.SourceSchema, Table: job.Table.SourceTable,
			PrimaryKey: job.Table.PrimaryKey, PrimaryKeys: job.Table.PrimaryKeys,
			Columns: job.Table.Columns, StartPK: job.Chunk.Start, EndPK: job.Chunk.End,
			AfterPK: after, HasAfter: hasAfter, Cursor: keyCursor,
			LowerBound: lowerBound, UpperBound: upperBound, UseKeyset: useKeyset,
			Limit: limit, Partition: job.Chunk.PartitionName,
			HashBucket: job.Chunk.HashBucket, HashBuckets: job.Chunk.HashBuckets,
			CustomWhere: job.Chunk.CustomWhere,
		})
		readMS := time.Since(readStarted).Milliseconds()
		if e != nil {
			return nil, fmt.Errorf("read batch: %w", e)
		}
		if len(batch.Rows) == 0 {
			return &fullpipeline.Batch{}, nil
		}
		throttleBytesPerSec(batch.Bytes, readBytesPerSec.Load(), readStarted)
		cursorJSON := ""
		if useKeyset {
			if len(batch.LastKey) == 0 {
				return nil, fmt.Errorf("keyset batch returned rows without a last-key cursor")
			}
			// Advance only the reader-local cursor. Durable state is updated by the
			// Commit hook after the sink write succeeds.
			keyCursor = append([]connector.Value(nil), batch.LastKey...)
			encoded, e := json.Marshal(keyCursor)
			if e != nil {
				return nil, fmt.Errorf("encode keyset cursor: %w", e)
			}
			cursorJSON = string(encoded)
		} else {
			after = batch.LastPK
			hasAfter = true
			if numericRangeSplit(job.Chunk.SplitType) {
				encoded, e := json.Marshal(numericChunkCursor{AfterPK: after})
				if e != nil {
					return nil, fmt.Errorf("encode numeric chunk cursor: %w", e)
				}
				cursorJSON = string(encoded)
			}
		}
		return &fullpipeline.Batch{Rows: len(batch.Rows), Bytes: batch.Bytes, Cursor: cursorJSON, ReadMS: readMS, Payload: batch}, nil
	}

	transformer := func(_ context.Context, pb *fullpipeline.Batch) (*fullpipeline.Batch, error) {
		batch, ok := pb.Payload.(*connector.RowBatch)
		if !ok || batch == nil {
			return nil, fmt.Errorf("pipeline batch payload is not a connector row batch")
		}
		rows, e := valuePlan.TransformRows(batch.Rows)
		if e != nil {
			return nil, fmt.Errorf("QMigration value transform: %w", e)
		}
		cloned := &connector.RowBatch{Rows: rows, LastPK: batch.LastPK, LastKey: append([]connector.Value(nil), batch.LastKey...), Bytes: batch.Bytes}
		out := *pb
		out.Payload = cloned
		return &out, nil
	}

	writer := func(writeCtx context.Context, pb *fullpipeline.Batch) (int64, int64, int64, error) {
		batch, ok := pb.Payload.(*connector.RowBatch)
		if !ok || batch == nil {
			return 0, 0, 0, fmt.Errorf("pipeline batch payload is not a connector row batch")
		}
		writeStarted := time.Now()
		written, e := dst.WriteBatch(writeCtx, connector.WriteBatchRequest{
			Schema: job.Table.TargetSchema, Table: job.Table.TargetTable,
			Columns: targetCols, Rows: batch.Rows,
			PrimaryKey: targetPK, PrimaryKeys: targetPKs,
		})
		writeMS := time.Since(writeStarted).Milliseconds()
		if e != nil {
			return 0, 0, writeMS, fmt.Errorf("write batch: %w", e)
		}
		throttleBytesPerSec(batch.Bytes, writeBytesPerSec.Load(), writeStarted)
		throttleUnits(int64(len(batch.Rows)), job.RowsPerSecond, writeStarted)
		throttleUnits(2, int64(job.QPS), writeStarted)
		return written, batch.Bytes, writeMS, nil
	}

	lastCommittedCursor := strings.TrimSpace(job.Chunk.CursorJSON)
	yielded := false
	yieldReason := ""
	committer := func(commitCtx context.Context, pb *fullpipeline.Batch, stats fullpipeline.Stats) (fullpipeline.Control, error) {
		progress := domain.ChunkProgress{
			RowsRead: stats.RowsRead, RowsWritten: stats.RowsWritten,
			BytesRead: stats.BytesRead, BytesWritten: stats.BytesWritten,
			CursorJSON: pb.Cursor, LastReadMS: stats.LastReadMS,
			LastWriteMS: stats.LastWriteMS, LastBatchRows: stats.LastBatchRows,
		}
		var control domain.ChunkControl
		if _, e := doJSON(http.MethodPost, server+"/api/v1/workers/"+workerID+"/chunks/"+job.Chunk.ID+"/lease", progress, &control); e != nil {
			return fullpipeline.Control{}, fmt.Errorf("renew lease/checkpoint: %w", e)
		}
		// Lease renewal always carries the current task-global source/target
		// budgets, including zero. Storing zero is important when an operator
		// removes a live limit; otherwise a worker could retain stale pacing.
		readBytesPerSec.Store(control.ReadBytesPerSec)
		writeBytesPerSec.Store(control.WriteBytesPerSec)
		if strings.TrimSpace(pb.Cursor) != "" {
			lastCommittedCursor = pb.Cursor
		}
		if control.YieldAfterBatch {
			yielded = true
			yieldReason = control.YieldReason
		}
		return fullpipeline.Control{Level: control.Level, MaxBatchRows: control.MaxBatchRows, TargetBatchRows: control.TargetBatchRows, TargetBytesPerSec: control.TargetBytesPerSec, Pause: time.Duration(control.PauseMS) * time.Millisecond, StopAfterCommit: control.YieldAfterBatch}, nil
	}

	batchRows := job.BatchRows
	if batchRows <= 0 {
		batchRows = 500
	}
	bufferBatches := 2
	if raw := strings.TrimSpace(os.Getenv("QMIGRATION_PIPELINE_BUFFER_BATCHES")); raw != "" {
		if n, e := strconv.Atoi(raw); e == nil && n >= 1 && n <= 32 {
			bufferBatches = n
		}
	}
	adaptive := !strings.EqualFold(strings.TrimSpace(os.Getenv("QMIGRATION_ADAPTIVE_BATCH")), "false")
	stats, err := (fullpipeline.Runner{Read: reader, Transform: transformer, Write: writer, Commit: committer}).Run(ctx, fullpipeline.Config{
		InitialBatchRows: batchRows, MinBatchRows: 50, MaxBatchRows: 5000,
		BufferBatches: bufferBatches, Adaptive: adaptive, InitialTargetBytesPerSec: job.TargetBytesPerSec,
		InitialStats: fullpipeline.Stats{
			RowsRead: job.Chunk.RowsRead, RowsWritten: job.Chunk.RowsWritten,
			BytesRead: job.Chunk.BytesRead, BytesWritten: job.Chunk.BytesWritten,
		},
	})
	result.RowsRead, result.RowsWritten = stats.RowsRead, stats.RowsWritten
	result.BytesRead, result.BytesWritten = stats.BytesRead, stats.BytesWritten
	result.CursorJSON = lastCommittedCursor
	result.Yielded = yielded && err == nil
	if result.Yielded {
		result.YieldReason = yieldReason
	}
	if err != nil {
		result.Error = err.Error()
	}
	return
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	server := os.Getenv("QMIGRATION_SERVER")
	if server == "" {
		server = "http://127.0.0.1:8080"
	}
	host, _ := os.Hostname()
	previousCounters := readHostCounters()
	w := runtimeWorker{Hostname: host, CPU: runtime.NumCPU(), Capabilities: detect(), Labels: parseWorkerLabels(os.Getenv("QMIGRATION_WORKER_LABELS"))}
	applyHostUsage(&w, hostCounters{}, previousCounters)
	if err := post(server+"/api/v1/workers/register", w, &w); err != nil {
		log.Fatal(err)
	}
	log.Printf("worker registered: %s capabilities=%v", w.ID, w.Capabilities)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				heartbeat := w
				currentCounters := readHostCounters()
				applyHostUsage(&heartbeat, previousCounters, currentCounters)
				previousCounters = currentCounters
				heartbeat.RunningJobs = int(runningJobs.Load())
				if err := post(server+"/api/v1/workers/"+w.ID+"/heartbeat", heartbeat, nil); err != nil {
					log.Printf("heartbeat failed: %v", err)
				}
			}
		}
	}()

	concurrency := 1
	if v := os.Getenv("QMIGRATION_WORKER_CONCURRENCY"); v != "" {
		if n, e := strconv.Atoi(v); e == nil && n > 0 && n <= 128 {
			concurrency = n
		}
	}
	log.Printf("worker concurrency=%d", concurrency)
	cdcConcurrency := 1
	if v := os.Getenv("QMIGRATION_CDC_CONCURRENCY"); v != "" {
		if n, e := strconv.Atoi(v); e == nil && n >= 0 && n <= 32 {
			cdcConcurrency = n
		}
	}
	for slot := 0; slot < cdcConcurrency; slot++ {
		wg.Add(1)
		go func(slot int) {
			defer wg.Done()
			for {
				if ctx.Err() != nil {
					return
				}
				var claim domain.EngineJobClaim
				status, err := doJSON(http.MethodPost, server+"/api/v1/workers/"+w.ID+"/engine-jobs/claim", nil, &claim)
				if err != nil {
					log.Printf("cdc-slot=%d claim failed: %v", slot, err)
					if !sleepContext(ctx, 3*time.Second) {
						return
					}
					continue
				}
				if status == http.StatusNoContent {
					if !sleepContext(ctx, 2*time.Second) {
						return
					}
					continue
				}
				runningJobs.Add(1)
				log.Printf("cdc-slot=%d claimed engine-job=%s engine=%s direction=%s", slot, claim.Job.ID, claim.Job.Engine, claim.Job.Direction)
				result := executeManagedEngineJob(ctx, server, w.ID, &claim)
				runningJobs.Add(-1)
				if ctx.Err() != nil {
					log.Printf("cdc engine-job=%s interrupted by worker shutdown; lease will be reclaimed", claim.Job.ID)
					return
				}
				endpoint := "complete"
				if result.Error != "" {
					endpoint = "fail"
				}
				if err := post(server+"/api/v1/workers/"+w.ID+"/engine-jobs/"+claim.Job.ID+"/"+endpoint, result, nil); err != nil {
					log.Printf("report engine-job %s: %v", endpoint, err)
				}
			}
		}(slot)
	}
	log.Printf("managed CDC concurrency=%d", cdcConcurrency)
	for slot := 0; slot < concurrency; slot++ {
		wg.Add(1)
		go func(slot int) {
			defer wg.Done()
			for {
				if ctx.Err() != nil {
					return
				}
				var job domain.ChunkJob
				status, err := doJSON(http.MethodPost, server+"/api/v1/workers/"+w.ID+"/claim", nil, &job)
				if err != nil {
					log.Printf("slot=%d claim failed: %v", slot, err)
					if !sleepContext(ctx, 3*time.Second) {
						return
					}
					continue
				}
				if status == http.StatusNoContent {
					if !sleepContext(ctx, time.Second) {
						return
					}
					continue
				}
				runningJobs.Add(1)
				log.Printf("slot=%d claimed %s %s.%s PK[%d,%d]", slot, job.Chunk.ID, job.Table.SourceSchema, job.Table.SourceTable, job.Chunk.Start, job.Chunk.End)
				result := executeJob(ctx, server, w.ID, &job)
				runningJobs.Add(-1)
				if ctx.Err() != nil {
					log.Printf("chunk %s interrupted by worker shutdown; durable cursor/lease will allow another worker to resume", job.Chunk.ID)
					return
				}
				if result.Error != "" {
					log.Printf("chunk %s failed: %s", job.Chunk.ID, result.Error)
					if err := post(server+"/api/v1/workers/"+w.ID+"/chunks/"+job.Chunk.ID+"/fail", result, nil); err != nil {
						log.Printf("report failure: %v", err)
					}
					continue
				}
				log.Printf("chunk %s complete rows=%d bytes=%d duration=%s", job.Chunk.ID, result.RowsWritten, result.BytesWritten, time.Duration(result.DurationMS)*time.Millisecond)
				if err := post(server+"/api/v1/workers/"+w.ID+"/chunks/"+job.Chunk.ID+"/complete", result, nil); err != nil {
					log.Printf("report completion: %v", err)
				}
			}
		}(slot)
	}

	<-ctx.Done()
	log.Printf("shutdown signal received; stop claiming new work and cancel active CDC processes")
	grace := 30 * time.Second
	if raw := strings.TrimSpace(os.Getenv("QMIGRATION_WORKER_SHUTDOWN_GRACE_SECONDS")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n >= 1 && n <= 300 {
			grace = time.Duration(n) * time.Second
		}
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		log.Printf("worker shutdown complete")
	case <-time.After(grace):
		log.Printf("worker shutdown grace expired after %s; remaining leases will expire and be reclaimed", grace)
	}
}

func sleepContext(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
