package api

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"qmigration/backend/internal/auth"
	compatcdc "qmigration/backend/internal/cdc/compat"
	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
	"qmigration/backend/internal/engine"
	"qmigration/backend/internal/maintenance"
	"qmigration/backend/internal/migration"
	"qmigration/backend/internal/perfmodel"
	"qmigration/backend/internal/repository"
	schemapkg "qmigration/backend/internal/schema"
	"qmigration/backend/internal/validationreport"
	"qmigration/backend/internal/version"
	workersvc "qmigration/backend/internal/worker"
)

type Server struct {
	repo                            repository.Repository
	connectors                      *connector.Registry
	migrations                      *migration.Service
	workers                         *workersvc.Service
	engines                         *engine.Registry
	staticTokens                    *auth.TokenSet
	sessions                        *auth.SessionManager
	authRequired                    atomic.Bool
	validationReportExports         atomic.Uint64
	validationReportArchiveSuccess  atomic.Uint64
	validationReportArchiveFailures atomic.Uint64
}

func New(repo repository.Repository, c *connector.Registry, e *engine.Registry) *Server {
	spec := os.Getenv("QMIGRATION_RBAC_TOKENS")
	if spec == "" {
		if legacy := os.Getenv("QMIGRATION_API_TOKEN"); legacy != "" {
			spec = "admin:" + legacy
		}
	}
	tokens := auth.ParseTokens(spec)
	ttl := 12 * time.Hour
	if raw := os.Getenv("QMIGRATION_SESSION_TTL_HOURS"); raw != "" {
		if hours, err := strconv.Atoi(raw); err == nil && hours > 0 && hours <= 168 {
			ttl = time.Duration(hours) * time.Hour
		}
	}
	secret := os.Getenv("QMIGRATION_AUTH_SECRET")
	if secret == "" {
		secret = "qmigration-development-auth-secret-change-me"
	}
	users, _ := repo.ListUsers(context.Background())
	required := !tokens.Empty() || len(users) > 0 || strings.EqualFold(os.Getenv("QMIGRATION_AUTH_REQUIRED"), "true")
	s := &Server{
		repo: repo, connectors: c, migrations: migration.NewService(repo, c, e), workers: workersvc.NewService(repo), engines: e,
		staticTokens: tokens, sessions: auth.NewSessionManager(secret, ttl),
	}
	s.authRequired.Store(required)
	return s
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", s.metrics)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{"status": "ok", "name": version.Name, "version": version.Version, "time": time.Now()})
	})
	mux.HandleFunc("GET /readyz", s.readiness)
	mux.HandleFunc("POST /api/v1/auth/login", s.login)
	mux.HandleFunc("GET /api/v1/auth/me", s.me)
	mux.HandleFunc("GET /api/v1/users", s.listUsers)
	mux.HandleFunc("POST /api/v1/users", s.createUser)
	mux.HandleFunc("PUT /api/v1/users/{id}", s.updateUser)
	mux.HandleFunc("POST /api/v1/users/{id}/password", s.changeUserPassword)
	mux.HandleFunc("GET /api/v1/dashboard", s.dashboard)
	mux.HandleFunc("GET /api/v1/ws", s.websocket)
	mux.HandleFunc("GET /api/v1/engines", s.listEngines)
	mux.HandleFunc("GET /api/v1/connectors", s.listConnectors)
	mux.HandleFunc("POST /api/v1/migrations/{id}/engines/{engine}/render", s.renderEngineConfig)
	mux.HandleFunc("GET /api/v1/migrations/{id}/engine-jobs", s.engineJobs)
	mux.HandleFunc("GET /api/v1/datasources", s.listDataSources)
	mux.HandleFunc("POST /api/v1/datasources", s.createDataSource)
	mux.HandleFunc("PUT /api/v1/datasources/{id}", s.updateDataSource)
	mux.HandleFunc("DELETE /api/v1/datasources/{id}", s.deleteDataSource)
	mux.HandleFunc("POST /api/v1/datasources/{id}/test", s.testDataSource)
	mux.HandleFunc("GET /api/v1/datasources/{id}/schemas", s.schemas)
	mux.HandleFunc("GET /api/v1/datasources/{id}/tables", s.tables)
	mux.HandleFunc("GET /api/v1/datasources/{id}/objects", s.schemaObjects)
	mux.HandleFunc("GET /api/v1/datasources/{id}/universal-schema", s.universalSchema)
	mux.HandleFunc("GET /api/v1/migrations", s.listMigrations)
	mux.HandleFunc("POST /api/v1/migrations", s.createMigration)
	mux.HandleFunc("GET /api/v1/migrations/{id}", s.getMigration)
	mux.HandleFunc("GET /api/v1/migrations/{id}/tables", s.migrationTables)
	mux.HandleFunc("GET /api/v1/performance/profiles/export", s.exportPerformanceProfiles)
	mux.HandleFunc("POST /api/v1/performance/profiles/import", s.importPerformanceProfiles)
	mux.HandleFunc("GET /api/v1/migrations/{id}/chunks", s.migrationChunks)
	mux.HandleFunc("GET /api/v1/migrations/{id}/logs", s.migrationLogs)
	mux.HandleFunc("GET /api/v1/migrations/{id}/precheck", s.precheckMigration)
	mux.HandleFunc("GET /api/v1/migrations/{id}/assessment", s.assessMigration)
	mux.HandleFunc("GET /api/v1/migrations/{id}/schema-objects/plan", s.schemaObjectPlan)
	mux.HandleFunc("POST /api/v1/migrations/{id}/schema-objects/apply", s.applySchemaObjects)
	mux.HandleFunc("GET /api/v1/migrations/{id}/validations", s.validationResults)
	mux.HandleFunc("GET /api/v1/migrations/{id}/validation-archive", s.validationArchive)
	mux.HandleFunc("GET /api/v1/validation-report/public-key", s.validationReportPublicKey)
	mux.HandleFunc("POST /api/v1/validation-report/key-transition", s.validationReportKeyTransition)
	mux.HandleFunc("POST /api/v1/validation-report/key-revocation", s.validationReportKeyRevocation)
	mux.HandleFunc("GET /api/v1/migrations/{id}/validation-report", s.validationReport)
	mux.HandleFunc("GET /api/v1/migrations/{id}/validation-report/manifest", s.validationReportManifest)
	mux.HandleFunc("GET /api/v1/migrations/{id}/validation-report/archive", s.validationReportArchiveRecord)
	mux.HandleFunc("POST /api/v1/migrations/{id}/validation-report/archive", s.archiveValidationReport)
	mux.HandleFunc("POST /api/v1/migrations/{id}/validate", s.validateMigration)
	mux.HandleFunc("POST /api/v1/migrations/{id}/repair", s.repairMigration)
	mux.HandleFunc("GET /api/v1/migrations/{id}/cdc", s.cdcPositions)
	mux.HandleFunc("GET /api/v1/migrations/{id}/cdc/spool", s.cdcSpoolStats)
	mux.HandleFunc("POST /api/v1/migrations/{id}/cdc/spool/drain", s.drainCDCSpool)
	mux.HandleFunc("POST /api/v1/migrations/{id}/cdc", s.recordCDCProgress)
	mux.HandleFunc("POST /api/v1/migrations/{id}/cdc/events", s.applyCDCEvents)
	mux.HandleFunc("POST /api/v1/migrations/{id}/cdc/debezium", s.applyDebeziumEvents)
	mux.HandleFunc("POST /api/v1/migrations/{id}/cdc/canal", s.applyCanalEvents)
	mux.HandleFunc("GET /api/v1/migrations/{id}/cdc/dlq", s.cdcDeadLetters)
	mux.HandleFunc("POST /api/v1/migrations/{id}/cdc/dlq/{dlq_id}/replay", s.replayCDCDeadLetter)
	mux.HandleFunc("POST /api/v1/migrations/{id}/cdc/dlq/{dlq_id}/resolve-commit", s.resolveCDCCommitUncertain)
	mux.HandleFunc("GET /api/v1/migrations/{id}/cdc/conflicts", s.cdcConflicts)
	mux.HandleFunc("POST /api/v1/migrations/{id}/cdc/started", s.markCDCStarted)
	mux.HandleFunc("POST /api/v1/migrations/{id}/ready-cutover", s.readyCutover)
	mux.HandleFunc("POST /api/v1/migrations/{id}/cutover", s.cutover)
	mux.HandleFunc("POST /api/v1/migrations/{id}/start", s.startMigration)
	mux.HandleFunc("POST /api/v1/migrations/{id}/pause", s.pauseMigration)
	mux.HandleFunc("POST /api/v1/migrations/{id}/resume", s.resumeMigration)
	mux.HandleFunc("POST /api/v1/migrations/{id}/cancel", s.cancelMigration)
	mux.HandleFunc("POST /api/v1/migrations/{id}/rollback/prepare", s.prepareRollback)
	mux.HandleFunc("POST /api/v1/migrations/{id}/rollback/cdc-started", s.markRollbackCDCStarted)
	mux.HandleFunc("POST /api/v1/migrations/{id}/rollback/cdc", s.recordRollbackCDCProgress)
	mux.HandleFunc("POST /api/v1/migrations/{id}/rollback/ready", s.readyRollback)
	mux.HandleFunc("POST /api/v1/migrations/{id}/rollback", s.rollbackMigration)
	mux.HandleFunc("GET /api/v1/workers", s.listWorkers)
	mux.HandleFunc("POST /api/v1/workers/register", s.registerWorker)
	mux.HandleFunc("POST /api/v1/workers/{id}/heartbeat", s.heartbeat)
	mux.HandleFunc("POST /api/v1/workers/{id}/claim", s.claimChunk)
	mux.HandleFunc("POST /api/v1/workers/{id}/chunks/{chunk_id}/lease", s.renewChunk)
	mux.HandleFunc("POST /api/v1/workers/{id}/chunks/{chunk_id}/complete", s.completeChunk)
	mux.HandleFunc("POST /api/v1/workers/{id}/chunks/{chunk_id}/fail", s.failChunk)
	mux.HandleFunc("POST /api/v1/workers/{id}/engine-jobs/claim", s.claimEngineJob)
	mux.HandleFunc("POST /api/v1/workers/{id}/engine-jobs/{job_id}/started", s.startEngineJob)
	mux.HandleFunc("POST /api/v1/workers/{id}/engine-jobs/{job_id}/lease", s.renewEngineJob)
	mux.HandleFunc("POST /api/v1/workers/{id}/engine-jobs/{job_id}/control", s.engineJobControl)
	mux.HandleFunc("POST /api/v1/workers/{id}/engine-jobs/{job_id}/cdc/events", s.workerEngineJobCDCEvents)
	mux.HandleFunc("POST /api/v1/workers/{id}/engine-jobs/{job_id}/cdc/ready", s.workerEngineJobCDCReady)
	mux.HandleFunc("POST /api/v1/workers/{id}/engine-jobs/{job_id}/complete", s.completeEngineJob)
	mux.HandleFunc("POST /api/v1/workers/{id}/engine-jobs/{job_id}/fail", s.failEngineJob)
	mux.HandleFunc("GET /api/v1/alerts", s.listAlerts)
	mux.HandleFunc("POST /api/v1/alerts/{id}/ack", s.ackAlert)
	mux.HandleFunc("GET /api/v1/audit", s.listAudit)
	return cors(logging(s.apiAuth(workerAuth(mux))))
}
func (s *Server) readiness(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	if _, err := s.repo.ListWorkers(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "not_ready", "name": version.Name, "version": version.Version, "repository": "unavailable", "error": err.Error(), "time": time.Now()})
		return
	}
	type spoolReadyProvider interface {
		CDCSpoolStorageReady(context.Context) error
	}
	if p, ok := s.repo.(spoolReadyProvider); ok {
		if err := p.CDCSpoolStorageReady(ctx); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "not_ready", "name": version.Name, "version": version.Version, "repository": "ok", "cdc_spool": "unavailable", "error": err.Error(), "time": time.Now()})
			return
		}
	}
	schemaVersion := ""
	type schemaVersionProvider interface {
		MetadataSchemaVersion(context.Context) (string, error)
	}
	if p, ok := s.repo.(schemaVersionProvider); ok {
		v, err := p.MetadataSchemaVersion(ctx)
		if err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "not_ready", "name": version.Name, "version": version.Version, "repository": "schema_unavailable", "error": err.Error(), "time": time.Now()})
			return
		}
		schemaVersion = v
		if v != "" && v != version.Version {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "not_ready", "name": version.Name, "version": version.Version, "repository": "schema_version_mismatch", "schema_version": v, "time": time.Now()})
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ready", "name": version.Name, "version": version.Version, "repository": "ok", "cdc_spool": "ok", "schema_version": schemaVersion, "time": time.Now()})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if v != nil {
		_ = json.NewEncoder(w).Encode(v)
	}
}
func decode(r *http.Request, v any) error { return json.NewDecoder(r.Body).Decode(v) }
func randomID(prefix string) string {
	b := make([]byte, 5)
	_, _ = rand.Read(b)
	return prefix + "_" + hex.EncodeToString(b)
}
func apiError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]any{"error": err.Error()})
}

func websocketAccept(key string) string {
	h := sha1.Sum([]byte(strings.TrimSpace(key) + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	return base64.StdEncoding.EncodeToString(h[:])
}

func writeWebSocketText(conn net.Conn, payload []byte) error {
	header := []byte{0x81}
	n := len(payload)
	switch {
	case n <= 125:
		header = append(header, byte(n))
	case n <= 65535:
		header = append(header, 126, byte(n>>8), byte(n))
	default:
		header = append(header, 127)
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(n))
		header = append(header, b[:]...)
	}
	if _, err := conn.Write(header); err != nil {
		return err
	}
	_, err := conn.Write(payload)
	return err
}

func readWebSocketFrames(conn net.Conn, done chan<- struct{}) {
	defer close(done)
	reader := bufio.NewReader(conn)
	for {
		b1, err := reader.ReadByte()
		if err != nil {
			return
		}
		b2, err := reader.ReadByte()
		if err != nil {
			return
		}
		opcode := b1 & 0x0f
		masked := b2&0x80 != 0
		n := uint64(b2 & 0x7f)
		if n == 126 {
			var ext [2]byte
			if _, err := io.ReadFull(reader, ext[:]); err != nil {
				return
			}
			n = uint64(binary.BigEndian.Uint16(ext[:]))
		} else if n == 127 {
			var ext [8]byte
			if _, err := io.ReadFull(reader, ext[:]); err != nil {
				return
			}
			n = binary.BigEndian.Uint64(ext[:])
		}
		if n > 1<<20 {
			return
		}
		var mask [4]byte
		if masked {
			if _, err := io.ReadFull(reader, mask[:]); err != nil {
				return
			}
		}
		data := make([]byte, int(n))
		if _, err := io.ReadFull(reader, data); err != nil {
			return
		}
		if masked {
			for i := range data {
				data[i] ^= mask[i%4]
			}
		}
		if opcode == 0x8 {
			return
		}
		if opcode == 0x9 { // ping -> pong
			header := []byte{0x8a, byte(len(data))}
			_, _ = conn.Write(append(header, data...))
		}
	}
}

func (s *Server) websocket(w http.ResponseWriter, r *http.Request) {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") || !strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade") {
		apiError(w, http.StatusBadRequest, errors.New("websocket upgrade required"))
		return
	}
	key := strings.TrimSpace(r.Header.Get("Sec-WebSocket-Key"))
	if key == "" || r.Header.Get("Sec-WebSocket-Version") != "13" {
		apiError(w, http.StatusBadRequest, errors.New("invalid websocket handshake"))
		return
	}
	h, ok := w.(http.Hijacker)
	if !ok {
		apiError(w, http.StatusInternalServerError, errors.New("websocket hijacking unavailable"))
		return
	}
	conn, rw, err := h.Hijack()
	if err != nil {
		return
	}
	defer conn.Close()
	_, _ = fmt.Fprintf(rw, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\nSec-WebSocket-Protocol: qmigration.v1\r\n\r\n", websocketAccept(key))
	if err := rw.Flush(); err != nil {
		return
	}

	done := make(chan struct{})
	go readWebSocketFrames(conn, done)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	send := func(v any) error {
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		return writeWebSocketText(conn, b)
	}
	selectedTask := strings.TrimSpace(r.URL.Query().Get("task_id"))
	for {
		migrations, _ := s.repo.ListMigrations(r.Context())
		for _, m := range migrations {
			if selectedTask != "" && m.ID != selectedTask {
				continue
			}
			if err := send(map[string]any{"type": "task.progress", "task_id": m.ID, "status": m.Status, "progress": m.Progress, "speed_bytes_sec": m.SpeedBytesSec, "target_throughput_mbps": m.TargetThroughputMBps, "auto_throughput_enabled": m.AutoThroughputEnabled, "completion_sla_seconds": m.CompletionSLASeconds, "controller_target_bytes_sec": m.ControllerTargetBytesSec, "throughput_controller_reason": m.ThroughputControllerReason, "adaptive_hotspot_splits": m.AdaptiveHotspotSplits, "adaptive_running_yields": m.AdaptiveRunningYields, "adaptive_topology_drains": m.AdaptiveTopologyDrains, "adaptive_topology_degraded_yields": m.AdaptiveTopologyDegradedYields, "adaptive_fault_domain_yields": m.AdaptiveFaultDomainYields, "controller_auto_probe_pct": m.ControllerAutoProbePct, "controller_sla_headroom_pct": m.ControllerSLAHeadroomPct, "controller_learning_samples": m.ControllerLearningSamples, "eta_seconds": m.ETASeconds, "sla_p95_eta_seconds": m.SLAP95ETASeconds, "sla_p99_eta_seconds": m.SLAP99ETASeconds, "sla_risk_level": m.SLARiskLevel, "sla_risk_reason": m.SLARiskReason, "rows_migrated": m.RowsMigrated, "bytes_migrated": m.BytesMigrated, "effective_parallelism": m.EffectiveParallelism, "flow_control_level": m.FlowControlLevel, "cdc_spool_growth_bytes_sec": m.CDCSpoolGrowthBytesSec, "cdc_spool_critical_eta_seconds": m.CDCSpoolCriticalETASeconds}); err != nil {
				return
			}
			if err := send(map[string]any{"type": "cdc.metrics", "task_id": m.ID, "lag_ms": m.CDCLagMS, "status": m.Status}); err != nil {
				return
			}
		}
		if selectedTask == "" {
			workers, _ := s.workers.List(r.Context())
			if err := send(map[string]any{"type": "worker.metrics", "workers": workers}); err != nil {
				return
			}
			alerts, _ := s.repo.ListAlerts(r.Context())
			if len(alerts) > 20 {
				alerts = alerts[len(alerts)-20:]
			}
			if err := send(map[string]any{"type": "alert", "alerts": alerts}); err != nil {
				return
			}
		}
		select {
		case <-done:
			return
		case <-r.Context().Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	ms, _ := s.repo.ListMigrations(r.Context())
	ws, _ := s.workers.List(r.Context())
	ds, _ := s.repo.ListDataSources(r.Context())
	dlq, _ := s.repo.ListCDCDeadLetters(r.Context(), "")
	conflicts, _ := s.repo.ListCDCConflicts(r.Context(), "", 1000000)
	running, failed := 0, 0
	var rows, bytes int64
	for _, m := range ms {
		if m.Status != domain.StatusFinished && m.Status != domain.StatusCancelled && m.Status != domain.StatusFailed {
			running++
		}
		if m.Status == domain.StatusFailed {
			failed++
		}
		rows += m.RowsMigrated
		bytes += m.BytesMigrated
	}
	online := 0
	for _, x := range ws {
		if x.Status == "ONLINE" {
			online++
		}
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintf(w, "# TYPE qmigration_datasources gauge\nqmigration_datasources %d\n", len(ds))
	fmt.Fprintf(w, "# TYPE qmigration_migrations gauge\nqmigration_migrations %d\n", len(ms))
	fmt.Fprintf(w, "# TYPE qmigration_migrations_running gauge\nqmigration_migrations_running %d\n", running)
	fmt.Fprintf(w, "# TYPE qmigration_migrations_failed gauge\nqmigration_migrations_failed %d\n", failed)
	fmt.Fprintf(w, "# TYPE qmigration_workers_online gauge\nqmigration_workers_online %d\n", online)
	fmt.Fprintf(w, "# TYPE qmigration_rows_migrated_total counter\nqmigration_rows_migrated_total %d\n", rows)
	fmt.Fprintf(w, "# TYPE qmigration_bytes_migrated_total counter\nqmigration_bytes_migrated_total %d\n", bytes)
	openDLQ, uncertainDLQ, replayRequiredDLQ := 0, 0, 0
	for _, item := range dlq {
		if item.Status == domain.CDCDeadLetterOpen {
			openDLQ++
		}
		if item.Status == domain.CDCDeadLetterCommitUncertain {
			uncertainDLQ++
		}
		if item.Status == domain.CDCDeadLetterReplayRequired {
			replayRequiredDLQ++
		}
	}
	fmt.Fprintf(w, "# TYPE qmigration_cdc_dlq_open gauge\nqmigration_cdc_dlq_open %d\n", openDLQ)
	fmt.Fprintf(w, "# TYPE qmigration_cdc_commit_uncertain gauge\nqmigration_cdc_commit_uncertain %d\n", uncertainDLQ)
	fmt.Fprintf(w, "# TYPE qmigration_cdc_replay_required gauge\nqmigration_cdc_replay_required %d\n", replayRequiredDLQ)
	fmt.Fprintf(w, "# TYPE qmigration_cdc_conflicts_total counter\nqmigration_cdc_conflicts_total %d\n", len(conflicts))
	maintenanceStats := maintenance.Current()
	fmt.Fprintf(w, "# TYPE qmigration_metadata_maintenance_runs_total counter\nqmigration_metadata_maintenance_runs_total %d\n", maintenanceStats.Runs)
	fmt.Fprintf(w, "# TYPE qmigration_metadata_maintenance_failures_total counter\nqmigration_metadata_maintenance_failures_total %d\n", maintenanceStats.Failures)
	fmt.Fprintf(w, "# TYPE qmigration_metadata_maintenance_last_success_timestamp_seconds gauge\nqmigration_metadata_maintenance_last_success_timestamp_seconds %d\n", maintenanceStats.LastSuccessUnix)
	fmt.Fprintf(w, "# TYPE qmigration_metadata_task_logs_pruned_total counter\nqmigration_metadata_task_logs_pruned_total %d\n", maintenanceStats.TaskLogsDeleted)
	fmt.Fprintf(w, "# TYPE qmigration_metadata_audit_events_pruned_total counter\nqmigration_metadata_audit_events_pruned_total %d\n", maintenanceStats.AuditEventsDeleted)
	fmt.Fprintf(w, "# TYPE qmigration_metadata_cdc_positions_pruned_total counter\nqmigration_metadata_cdc_positions_pruned_total %d\n", maintenanceStats.CDCPositionsDeleted)
	fmt.Fprintf(w, "# TYPE qmigration_metadata_validation_results_pruned_total counter\nqmigration_metadata_validation_results_pruned_total %d\n", maintenanceStats.ValidationDeleted)
	fmt.Fprintf(w, "# TYPE qmigration_metadata_maintenance_validation_archives_created_total counter\nqmigration_metadata_maintenance_validation_archives_created_total %d\n", maintenanceStats.ValidationArchivesCreated)
	fmt.Fprintf(w, "# TYPE qmigration_validation_report_exports_total counter\nqmigration_validation_report_exports_total %d\n", s.validationReportExports.Load())
	fmt.Fprintf(w, "# TYPE qmigration_validation_report_archive_success_total counter\nqmigration_validation_report_archive_success_total %d\n", s.validationReportArchiveSuccess.Load())
	fmt.Fprintf(w, "# TYPE qmigration_validation_report_archive_failures_total counter\nqmigration_validation_report_archive_failures_total %d\n", s.validationReportArchiveFailures.Load())
	metadataStorage, _ := repository.ReadMetadataStorageStats(r.Context(), s.repo)
	fmt.Fprintf(w, "# TYPE qmigration_metadata_storage_bytes gauge\nqmigration_metadata_storage_bytes %d\n", metadataStorage.TotalBytes)
	fmt.Fprintf(w, "# TYPE qmigration_metadata_relation_total_bytes gauge\n")
	fmt.Fprintf(w, "# TYPE qmigration_metadata_relation_table_bytes gauge\n")
	fmt.Fprintf(w, "# TYPE qmigration_metadata_relation_index_bytes gauge\n")
	fmt.Fprintf(w, "# TYPE qmigration_metadata_relation_live_rows gauge\n")
	fmt.Fprintf(w, "# TYPE qmigration_metadata_relation_dead_rows gauge\n")
	fmt.Fprintf(w, "# TYPE qmigration_metadata_relation_dead_ratio gauge\n")
	for _, stat := range metadataStorage.Relations {
		fmt.Fprintf(w, "qmigration_metadata_relation_total_bytes{relation=%q} %d\n", stat.Relation, stat.TotalBytes)
		fmt.Fprintf(w, "qmigration_metadata_relation_table_bytes{relation=%q} %d\n", stat.Relation, stat.TableBytes)
		fmt.Fprintf(w, "qmigration_metadata_relation_index_bytes{relation=%q} %d\n", stat.Relation, stat.IndexBytes)
		fmt.Fprintf(w, "qmigration_metadata_relation_live_rows{relation=%q} %d\n", stat.Relation, stat.LiveRows)
		fmt.Fprintf(w, "qmigration_metadata_relation_dead_rows{relation=%q} %d\n", stat.Relation, stat.DeadRows)
		fmt.Fprintf(w, "qmigration_metadata_relation_dead_ratio{relation=%q} %.6f\n", stat.Relation, stat.DeadRatio())
	}
	for _, worker := range ws {
		fmt.Fprintf(w, "qmigration_worker_cpu_usage_percent{worker_id=%q} %.3f\n", worker.ID, worker.CPUUsagePct)
		fmt.Fprintf(w, "qmigration_worker_memory_usage_percent{worker_id=%q} %.3f\n", worker.ID, worker.MemoryUsagePct)
		fmt.Fprintf(w, "qmigration_worker_running_jobs{worker_id=%q} %d\n", worker.ID, worker.RunningJobs)
		fmt.Fprintf(w, "qmigration_worker_scheduler_load_score{worker_id=%q} %.3f\n", worker.ID, worker.SchedulerLoadScore)
	}
	for _, m := range ms {
		chunkSummary, _ := repository.SummarizeChunks(r.Context(), s.repo, m.ID)
		failedChunks, pendingChunks, runningChunks := chunkSummary.Failed, chunkSummary.Pending, chunkSummary.Running
		avgRead, avgWrite := float64(0), float64(0)
		if chunkSummary.LatencySamples > 0 {
			avgRead = float64(chunkSummary.ReadMS) / float64(chunkSummary.LatencySamples)
			avgWrite = float64(chunkSummary.WriteMS) / float64(chunkSummary.LatencySamples)
		}
		mismatches, _ := repository.LatestValidationMismatchCount(r.Context(), s.repo, m.ID)
		fmt.Fprintf(w, "qmigration_task_progress{task_id=%q} %.3f\n", m.ID, m.Progress)
		fmt.Fprintf(w, "qmigration_task_rows_migrated_total{task_id=%q} %d\n", m.ID, m.RowsMigrated)
		fmt.Fprintf(w, "qmigration_task_bytes_migrated_total{task_id=%q} %d\n", m.ID, m.BytesMigrated)
		fmt.Fprintf(w, "qmigration_task_speed_bytes_per_second{task_id=%q} %d\n", m.ID, m.SpeedBytesSec)
		targetBPS := m.TargetThroughputMBps * (1 << 20)
		if targetBPS <= 0 {
			targetBPS = m.ControllerTargetBytesSec
		}
		fmt.Fprintf(w, "qmigration_task_target_throughput_bytes_per_second{task_id=%q} %d\n", m.ID, targetBPS)
		fmt.Fprintf(w, "qmigration_task_controller_target_bytes_per_second{task_id=%q} %d\n", m.ID, m.ControllerTargetBytesSec)
		autoEnabled := 0
		if m.AutoThroughputEnabled {
			autoEnabled = 1
		}
		fmt.Fprintf(w, "qmigration_task_auto_throughput_enabled{task_id=%q} %d\n", m.ID, autoEnabled)
		fmt.Fprintf(w, "qmigration_task_completion_sla_seconds{task_id=%q} %d\n", m.ID, m.CompletionSLASeconds)
		slaRemaining := m.CompletionSLASeconds
		if m.CompletionSLASeconds > 0 && !m.SLAStartedAt.IsZero() {
			slaRemaining -= int64(time.Since(m.SLAStartedAt).Seconds())
			if slaRemaining < 0 {
				slaRemaining = 0
			}
		}
		fmt.Fprintf(w, "qmigration_task_completion_sla_remaining_seconds{task_id=%q} %d\n", m.ID, slaRemaining)
		fmt.Fprintf(w, "qmigration_task_adaptive_hotspot_splits_total{task_id=%q} %d\n", m.ID, m.AdaptiveHotspotSplits)
		fmt.Fprintf(w, "qmigration_task_adaptive_running_yields_total{task_id=%q} %d\n", m.ID, m.AdaptiveRunningYields)
		fmt.Fprintf(w, "qmigration_task_adaptive_topology_drains_total{task_id=%q} %d\n", m.ID, m.AdaptiveTopologyDrains)
		fmt.Fprintf(w, "qmigration_task_adaptive_topology_degraded_yields_total{task_id=%q} %d\n", m.ID, m.AdaptiveTopologyDegradedYields)
		fmt.Fprintf(w, "qmigration_task_adaptive_fault_domain_yields_total{task_id=%q} %d\n", m.ID, m.AdaptiveFaultDomainYields)
		if tables, e := s.repo.ListMigrationTables(r.Context(), m.ID); e == nil {
			for _, table := range tables {
				fmt.Fprintf(w, "qmigration_table_profile_bytes_per_second{task_id=%q,table_id=%q} %d\n", m.ID, table.ID, table.ProfileBytesPerSec)
				fmt.Fprintf(w, "qmigration_table_profile_rows_per_second{task_id=%q,table_id=%q} %d\n", m.ID, table.ID, table.ProfileRowsPerSec)
				fmt.Fprintf(w, "qmigration_table_recommended_chunk_rows{task_id=%q,table_id=%q} %d\n", m.ID, table.ID, table.RecommendedChunkRows)
				fmt.Fprintf(w, "qmigration_table_performance_samples_total{task_id=%q,table_id=%q} %d\n", m.ID, table.ID, table.PerformanceSamples)
				for topologyID, profile := range table.TopologyPerformance {
					fmt.Fprintf(w, "qmigration_table_topology_profile_bytes_per_second{task_id=%q,table_id=%q,topology_id=%q} %d\n", m.ID, table.ID, topologyID, profile.BytesPerSec)
					fmt.Fprintf(w, "qmigration_table_topology_recommended_chunk_rows{task_id=%q,table_id=%q,topology_id=%q} %d\n", m.ID, table.ID, topologyID, profile.RecommendedChunkRows)
					fmt.Fprintf(w, "qmigration_table_topology_chunk_p95_duration_milliseconds{task_id=%q,table_id=%q,topology_id=%q} %d\n", m.ID, table.ID, topologyID, profile.P95DurationMS)
					fmt.Fprintf(w, "qmigration_table_topology_chunk_p99_duration_milliseconds{task_id=%q,table_id=%q,topology_id=%q} %d\n", m.ID, table.ID, topologyID, profile.P99DurationMS)
					health := 0
					switch strings.ToUpper(profile.Health) {
					case "DEGRADED":
						health = 1
					case "CIRCUIT_OPEN":
						health = 2
					case "HALF_OPEN":
						health = 3
					}
					fmt.Fprintf(w, "qmigration_table_topology_health_state{task_id=%q,table_id=%q,topology_id=%q} %d\n", m.ID, table.ID, topologyID, health)
					fmt.Fprintf(w, "qmigration_table_topology_scheduling_weight{task_id=%q,table_id=%q,topology_id=%q} %d\n", m.ID, table.ID, topologyID, perfmodel.TopologySchedulingWeight(profile.Health))
					cap := perfmodel.TopologyConcurrencyCap(profile.Health, repository.TopologyHealthyMaxConcurrency(), repository.TopologyDegradedMaxConcurrency())
					fmt.Fprintf(w, "qmigration_table_topology_concurrency_cap{task_id=%q,table_id=%q,topology_id=%q} %d\n", m.ID, table.ID, topologyID, cap)
					probeEpoch := int64(0)
					if !profile.LastProbeAt.IsZero() {
						probeEpoch = profile.LastProbeAt.Unix()
					}
					fmt.Fprintf(w, "qmigration_table_topology_last_probe_timestamp_seconds{task_id=%q,table_id=%q,topology_id=%q} %d\n", m.ID, table.ID, topologyID, probeEpoch)
					fmt.Fprintf(w, "qmigration_table_topology_good_streak{task_id=%q,table_id=%q,topology_id=%q} %d\n", m.ID, table.ID, topologyID, profile.GoodStreak)
					fmt.Fprintf(w, "qmigration_table_topology_recovery_concurrency_cap{task_id=%q,table_id=%q,topology_id=%q} %d\n", m.ID, table.ID, topologyID, repository.TopologyEffectiveConcurrencyCap(&table, topologyID))
					healthChangedEpoch := int64(0)
					if !profile.HealthChangedAt.IsZero() {
						healthChangedEpoch = profile.HealthChangedAt.Unix()
					}
					fmt.Fprintf(w, "qmigration_table_topology_health_changed_timestamp_seconds{task_id=%q,table_id=%q,topology_id=%q} %d\n", m.ID, table.ID, topologyID, healthChangedEpoch)
					fd := repository.TopologyFaultDomain(&table, topologyID)
					fmt.Fprintf(w, "qmigration_table_topology_fault_domain_info{task_id=%q,table_id=%q,topology_id=%q,region=%q,zone=%q,rack=%q} 1\n", m.ID, table.ID, topologyID, fd["region"], fd["zone"], fd["rack"])
					fmt.Fprintf(w, "qmigration_table_topology_fault_domain_peer_risk{task_id=%q,table_id=%q,topology_id=%q} %d\n", m.ID, table.ID, topologyID, repository.TopologyFaultDomainPeerRisk(&table, topologyID))
				}
			}
		}
		fmt.Fprintf(w, "qmigration_task_controller_auto_probe_percent{task_id=%q} %d\n", m.ID, m.ControllerAutoProbePct)
		fmt.Fprintf(w, "qmigration_task_controller_sla_headroom_percent{task_id=%q} %d\n", m.ID, m.ControllerSLAHeadroomPct)
		fmt.Fprintf(w, "qmigration_task_controller_learning_samples_total{task_id=%q} %d\n", m.ID, m.ControllerLearningSamples)
		utilization := 0.0
		if targetBPS > 0 {
			utilization = float64(m.SpeedBytesSec) / float64(targetBPS)
		}
		fmt.Fprintf(w, "qmigration_task_throughput_target_utilization_ratio{task_id=%q} %.6f\n", m.ID, utilization)
		fmt.Fprintf(w, "qmigration_task_speed_rows_per_second{task_id=%q} %d\n", m.ID, m.SpeedRowsSec)
		fmt.Fprintf(w, "qmigration_task_eta_seconds{task_id=%q} %d\n", m.ID, m.ETASeconds)
		fmt.Fprintf(w, "qmigration_task_sla_p95_eta_seconds{task_id=%q} %d\n", m.ID, m.SLAP95ETASeconds)
		fmt.Fprintf(w, "qmigration_task_sla_p99_eta_seconds{task_id=%q} %d\n", m.ID, m.SLAP99ETASeconds)
		riskState := 0
		switch strings.ToUpper(strings.TrimSpace(m.SLARiskLevel)) {
		case "WARN":
			riskState = 1
		case "CRITICAL":
			riskState = 2
		}
		fmt.Fprintf(w, "qmigration_task_sla_tail_risk_state{task_id=%q} %d\n", m.ID, riskState)
		fmt.Fprintf(w, "qmigration_task_effective_parallelism{task_id=%q} %d\n", m.ID, m.EffectiveParallelism)
		fmt.Fprintf(w, "qmigration_cdc_spool_growth_bytes_per_second{task_id=%q} %d\n", m.ID, m.CDCSpoolGrowthBytesSec)
		fmt.Fprintf(w, "qmigration_cdc_spool_critical_eta_seconds{task_id=%q} %d\n", m.ID, m.CDCSpoolCriticalETASeconds)
		fmt.Fprintf(w, "qmigration_task_chunks_pending{task_id=%q} %d\n", m.ID, pendingChunks)
		fmt.Fprintf(w, "qmigration_task_chunks_running{task_id=%q} %d\n", m.ID, runningChunks)
		fmt.Fprintf(w, "qmigration_task_chunks_failed{task_id=%q} %d\n", m.ID, failedChunks)
		fmt.Fprintf(w, "qmigration_task_read_latency_ms{task_id=%q} %.3f\n", m.ID, avgRead)
		fmt.Fprintf(w, "qmigration_task_write_latency_ms{task_id=%q} %.3f\n", m.ID, avgWrite)
		fmt.Fprintf(w, "qmigration_validation_mismatch_total{task_id=%q} %d\n", m.ID, mismatches)
		fmt.Fprintf(w, "qmigration_cdc_lag_seconds{task_id=%q} %.3f\n", m.ID, float64(m.CDCLagMS)/1000.0)
		forwardSpool, _ := s.repo.CDCSpoolStats(r.Context(), m.ID, "forward")
		reverseSpool, _ := s.repo.CDCSpoolStats(r.Context(), m.ID, "reverse")
		fmt.Fprintf(w, "qmigration_cdc_spool_pending_transactions{task_id=%q,direction=%q} %d\n", m.ID, "forward", forwardSpool.PendingTransactions)
		fmt.Fprintf(w, "qmigration_cdc_spool_pending_events{task_id=%q,direction=%q} %d\n", m.ID, "forward", forwardSpool.PendingEvents)
		fmt.Fprintf(w, "qmigration_cdc_spool_pending_bytes{task_id=%q,direction=%q} %d\n", m.ID, "forward", forwardSpool.PendingBytes)
		fmt.Fprintf(w, "qmigration_cdc_spool_storage_used_pct{task_id=%q,direction=%q,backend=%q,level=%q} %.3f\n", m.ID, "forward", forwardSpool.StorageBackend, forwardSpool.StorageLevel, forwardSpool.StorageUsedPct)
		fmt.Fprintf(w, "qmigration_cdc_spool_storage_free_bytes{task_id=%q,direction=%q,backend=%q} %d\n", m.ID, "forward", forwardSpool.StorageBackend, forwardSpool.StorageFreeBytes)
		fmt.Fprintf(w, "qmigration_cdc_spool_pending_transactions{task_id=%q,direction=%q} %d\n", m.ID, "reverse", reverseSpool.PendingTransactions)
		fmt.Fprintf(w, "qmigration_cdc_spool_pending_events{task_id=%q,direction=%q} %d\n", m.ID, "reverse", reverseSpool.PendingEvents)
		fmt.Fprintf(w, "qmigration_cdc_spool_pending_bytes{task_id=%q,direction=%q} %d\n", m.ID, "reverse", reverseSpool.PendingBytes)
		fmt.Fprintf(w, "qmigration_cdc_spool_storage_used_pct{task_id=%q,direction=%q,backend=%q,level=%q} %.3f\n", m.ID, "reverse", reverseSpool.StorageBackend, reverseSpool.StorageLevel, reverseSpool.StorageUsedPct)
	}
}

func (s *Server) audit(r *http.Request, action, resourceType, resourceID, detail string) {
	actor := r.Header.Get("X-QMigration-User")
	if actor == "" {
		actor = "anonymous"
	}
	_ = s.repo.CreateAuditEvent(r.Context(), &domain.AuditEvent{ID: randomID("aud"), Actor: actor, Action: action, ResourceType: resourceType, ResourceID: resourceID, Detail: detail, RemoteAddr: r.RemoteAddr, CreatedAt: time.Now()})
}

func (s *Server) listConnectors(w http.ResponseWriter, r *http.Request) {
	if s.connectors == nil {
		writeJSON(w, 200, []connector.Descriptor{})
		return
	}
	writeJSON(w, 200, s.connectors.Descriptors())
}

func (s *Server) listEngines(w http.ResponseWriter, r *http.Request) {
	if s.engines == nil {
		writeJSON(w, 200, []domain.EngineInfo{})
		return
	}
	writeJSON(w, 200, s.engines.Infos(r.Context()))
}

func (s *Server) renderEngineConfig(w http.ResponseWriter, r *http.Request) {
	if s.engines == nil {
		apiError(w, 503, errors.New("QMigration unified engine registry is unavailable"))
		return
	}
	a, ok := s.engines.Get(r.PathValue("engine"))
	if !ok {
		apiError(w, 404, errors.New("unknown engine"))
		return
	}
	task, err := s.repo.GetMigration(r.Context(), r.PathValue("id"))
	if err != nil {
		apiError(w, 404, err)
		return
	}
	src, err := s.repo.GetDataSource(r.Context(), task.SourceID)
	if err != nil {
		apiError(w, 400, err)
		return
	}
	dst, err := s.repo.GetDataSource(r.Context(), task.TargetID)
	if err != nil {
		apiError(w, 400, err)
		return
	}
	tables, err := s.repo.ListMigrationTables(r.Context(), task.ID)
	if err != nil {
		apiError(w, 500, err)
		return
	}
	direction := r.URL.Query().Get("direction")
	if direction == "reverse" {
		src, dst = dst, src
		reversed := make([]domain.MigrationTable, 0, len(tables))
		for _, t := range tables {
			r := t
			r.SourceSchema, r.TargetSchema = t.TargetSchema, t.SourceSchema
			r.SourceTable, r.TargetTable = t.TargetTable, t.SourceTable
			r.PrimaryKey, r.TargetPrimaryKey = t.TargetPrimaryKey, t.PrimaryKey
			r.PrimaryKeys, r.TargetPrimaryKeys = append([]string(nil), t.TargetPrimaryKeys...), append([]string(nil), t.PrimaryKeys...)
			r.Columns, r.TargetColumns = t.TargetColumns, t.Columns
			reversed = append(reversed, r)
		}
		tables = reversed
		copyTask := *task
		copyTask.Mode = domain.ModeIncremental
		positions, _ := s.repo.ListCDCPositions(r.Context(), task.ID, 200)
		for _, p := range positions {
			if p.Direction != "reverse" {
				continue
			}
			copyTask.CDCStartTimestampMS = p.SourceTimestampMS
			copyTask.CDCStartPositionType = p.PositionType
			copyTask.CDCStartPositionValue = p.PositionValue
			copyTask.CDCStartResource = p.Resource
			break
		}
		task = &copyTask
	}
	includeSecrets := r.URL.Query().Get("secrets") == "1"
	if includeSecrets {
		role := r.Header.Get("X-QMigration-Role")
		if role != "" && role != "admin" && role != "dba" {
			apiError(w, http.StatusForbidden, errors.New("only admin/dba may render engine configs with credentials"))
			return
		}
	} else {
		src.Password = "******"
		dst.Password = "******"
	}
	cfg, err := a.Render(r.Context(), task, *src, *dst, tables)
	if err != nil {
		apiError(w, 400, err)
		return
	}
	s.audit(r, "RENDER_ENGINE_CONFIG", "migration", task.ID, a.Name()+" direction="+direction)
	writeJSON(w, 200, cfg)
}

func (s *Server) dashboard(w http.ResponseWriter, r *http.Request) {
	ds, _ := s.repo.ListDataSources(r.Context())
	ms, _ := s.repo.ListMigrations(r.Context())
	ws, _ := s.workers.List(r.Context())
	running := 0
	var bytes int64
	for _, m := range ms {
		if m.Status != domain.StatusFinished && m.Status != domain.StatusCancelled && m.Status != domain.StatusFailed {
			running++
		}
		bytes += m.BytesMigrated
	}
	writeJSON(w, 200, map[string]any{"datasources": len(ds), "migrations": len(ms), "running_migrations": running, "workers": len(ws), "bytes_migrated": bytes})
}
func (s *Server) listDataSources(w http.ResponseWriter, r *http.Request) {
	items, err := s.repo.ListDataSources(r.Context())
	if err != nil {
		apiError(w, 500, err)
		return
	}
	writeJSON(w, 200, items)
}

type createDataSourceRequest struct {
	Name          string                `json:"name"`
	Type          domain.DataSourceType `json:"type"`
	Host          string                `json:"host"`
	Port          int                   `json:"port"`
	Username      string                `json:"username"`
	Password      string                `json:"password"`
	Database      string                `json:"database"`
	Schema        string                `json:"schema"`
	JDBCURL       string                `json:"jdbc_url"`
	DriverClass   string                `json:"driver_class"`
	CDCURL        string                `json:"cdc_url"`
	TLSMode       domain.TLSMode        `json:"tls_mode"`
	TLSServerName string                `json:"tls_server_name"`
	TLSCACert     string                `json:"tls_ca_cert"`
	TLSClientCert string                `json:"tls_client_cert"`
	TLSClientKey  string                `json:"tls_client_key"`
}

func normalizeDataSourceTLSMode(t domain.DataSourceType, mode domain.TLSMode) (domain.TLSMode, error) {
	v := domain.TLSMode(strings.ToUpper(strings.TrimSpace(string(mode))))
	if v == "" {
		if t.IsPostgreSQLWireCompatible() || t == domain.DataSourceSQLServer {
			return domain.TLSModePreferred, nil
		}
		return domain.TLSModeDisable, nil
	}
	switch v {
	case domain.TLSModeDisable, domain.TLSModePreferred, domain.TLSModeRequired:
	default:
		return "", fmt.Errorf("unsupported tls_mode %q; expected DISABLE, PREFERRED or REQUIRED", mode)
	}
	if v != domain.TLSModeDisable && !t.IsMySQLFamily() && !t.IsPostgreSQLWireCompatible() && t != domain.DataSourceSQLServer && t != domain.DataSourceOracle {
		return "", fmt.Errorf("tls_mode %s is currently supported only for Native MySQL/PostgreSQL/SQL Server/Oracle datasources", v)
	}
	return v, nil
}

func (s *Server) createDataSource(w http.ResponseWriter, r *http.Request) {
	var in createDataSourceRequest
	if err := decode(r, &in); err != nil {
		apiError(w, 400, err)
		return
	}
	if in.Name == "" || in.Host == "" || in.Username == "" {
		apiError(w, 400, errors.New("name, host and username are required"))
		return
	}
	if in.Type == "" {
		in.Type = domain.DataSourceMySQL
	}
	if in.Type.IsExternalJDBC() && (in.JDBCURL == "" || in.DriverClass == "") {
		apiError(w, 400, errors.New("jdbc_url and driver_class are required for datasource types whose QMigration Native Connector is not implemented yet"))
		return
	}
	if in.Port == 0 {
		switch {
		case in.Type.IsPostgreSQLWireCompatible():
			in.Port = 5432
		case in.Type.IsMySQLFamily():
			in.Port = 3306
		case in.Type == domain.DataSourceOracle:
			in.Port = 1521
		case in.Type == domain.DataSourceSQLServer:
			in.Port = 1433
		case in.Type == domain.DataSourceGBase:
			in.Port = 5258
		case in.Type == domain.DataSourceGBase8s:
			in.Port = 9088
		default:
			apiError(w, 400, errors.New("port is required for datasource types without a native protocol default"))
			return
		}
	}
	tlsMode, err := normalizeDataSourceTLSMode(in.Type, in.TLSMode)
	if err != nil {
		apiError(w, 400, err)
		return
	}
	d := domain.DataSource{ID: randomID("ds"), Name: in.Name, Type: in.Type, Host: in.Host, Port: in.Port, Username: in.Username, Password: in.Password, Database: in.Database, Schema: in.Schema, JDBCURL: in.JDBCURL, DriverClass: in.DriverClass, CDCURL: strings.TrimSpace(in.CDCURL), TLSMode: tlsMode, TLSServerName: strings.TrimSpace(in.TLSServerName), TLSCACert: strings.TrimSpace(in.TLSCACert), TLSClientCert: strings.TrimSpace(in.TLSClientCert), TLSClientKey: strings.TrimSpace(in.TLSClientKey), CreatedAt: time.Now(), UpdatedAt: time.Now()}
	if err := s.repo.CreateDataSource(r.Context(), &d); err != nil {
		apiError(w, 500, err)
		return
	}
	s.audit(r, "CREATE", "datasource", d.ID, d.Name)
	writeJSON(w, 201, d)
}

func (s *Server) updateDataSource(w http.ResponseWriter, r *http.Request) {
	var in createDataSourceRequest
	if err := decode(r, &in); err != nil {
		apiError(w, 400, err)
		return
	}
	old, err := s.repo.GetDataSource(r.Context(), r.PathValue("id"))
	if err != nil {
		apiError(w, 404, err)
		return
	}
	d := *old
	if in.Name != "" {
		d.Name = in.Name
	}
	if in.Type != "" {
		d.Type = in.Type
	}
	if in.Host != "" {
		d.Host = in.Host
	}
	if in.Port > 0 {
		d.Port = in.Port
	}
	if in.Username != "" {
		d.Username = in.Username
	}
	d.Database = in.Database
	d.Schema = in.Schema
	d.JDBCURL = in.JDBCURL
	d.DriverClass = in.DriverClass
	d.CDCURL = strings.TrimSpace(in.CDCURL)
	if in.TLSMode != "" {
		tlsMode, e := normalizeDataSourceTLSMode(d.Type, in.TLSMode)
		if e != nil {
			apiError(w, 400, e)
			return
		}
		d.TLSMode = tlsMode
	} else if d.TLSMode == "" {
		d.TLSMode, _ = normalizeDataSourceTLSMode(d.Type, "")
	}
	d.TLSServerName = strings.TrimSpace(in.TLSServerName)
	d.TLSCACert = strings.TrimSpace(in.TLSCACert)
	d.TLSClientCert = strings.TrimSpace(in.TLSClientCert)
	d.TLSClientKey = strings.TrimSpace(in.TLSClientKey)
	d.Password = in.Password
	d.UpdatedAt = time.Now()
	if err := s.repo.UpdateDataSource(r.Context(), &d); err != nil {
		apiError(w, 500, err)
		return
	}
	d.Password = ""
	s.audit(r, "UPDATE", "datasource", d.ID, d.Name)
	writeJSON(w, 200, d)
}
func (s *Server) deleteDataSource(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.repo.DeleteDataSource(r.Context(), id); err != nil {
		apiError(w, 409, err)
		return
	}
	s.audit(r, "DELETE", "datasource", id, "")
	writeJSON(w, 200, map[string]any{"ok": true})
}

func (s *Server) connectorFor(ctx context.Context, id string) (connector.Connector, error) {
	d, err := s.repo.GetDataSource(ctx, id)
	if err != nil {
		return nil, err
	}
	return s.connectors.New(*d)
}
func (s *Server) testDataSource(w http.ResponseWriter, r *http.Request) {
	ds, err := s.repo.GetDataSource(r.Context(), r.PathValue("id"))
	if err != nil {
		apiError(w, 400, err)
		return
	}
	c, err := s.connectors.New(*ds)
	if err != nil {
		apiError(w, 400, err)
		return
	}
	defer c.Close()
	if err := c.TestConnection(r.Context()); err != nil {
		apiError(w, 400, err)
		return
	}
	v, err := c.GetVersion(r.Context())
	if err != nil {
		apiError(w, 400, err)
		return
	}
	desc, _ := s.connectors.Descriptor(ds.Type)
	writeJSON(w, 200, map[string]any{"ok": true, "version": v, "protocol": desc.Protocol, "native": desc.Native, "capabilities": desc.Capabilities, "note": desc.Note})
}
func (s *Server) schemas(w http.ResponseWriter, r *http.Request) {
	c, err := s.connectorFor(r.Context(), r.PathValue("id"))
	if err != nil {
		apiError(w, 400, err)
		return
	}
	defer c.Close()
	items, err := c.ListSchemas(r.Context())
	if err != nil {
		apiError(w, 400, err)
		return
	}
	writeJSON(w, 200, items)
}
func (s *Server) tables(w http.ResponseWriter, r *http.Request) {
	schema := r.URL.Query().Get("schema")
	if schema == "" {
		apiError(w, 400, errors.New("schema is required"))
		return
	}
	c, err := s.connectorFor(r.Context(), r.PathValue("id"))
	if err != nil {
		apiError(w, 400, err)
		return
	}
	defer c.Close()
	items, err := c.ListTables(r.Context(), schema)
	if err != nil {
		apiError(w, 400, err)
		return
	}
	writeJSON(w, 200, items)
}

func (s *Server) universalSchema(w http.ResponseWriter, r *http.Request) {
	schemaName, table := strings.TrimSpace(r.URL.Query().Get("schema")), strings.TrimSpace(r.URL.Query().Get("table"))
	if schemaName == "" || table == "" {
		apiError(w, 400, errors.New("schema and table are required"))
		return
	}
	ds, err := s.repo.GetDataSource(r.Context(), r.PathValue("id"))
	if err != nil {
		apiError(w, 404, err)
		return
	}
	c, err := s.connectors.New(*ds)
	if err != nil {
		apiError(w, 400, err)
		return
	}
	defer c.Close()
	meta, err := c.GetTableMetadata(r.Context(), schemaName, table)
	if err != nil {
		apiError(w, 400, err)
		return
	}
	writeJSON(w, 200, schemapkg.FromMetadata(ds.Database, meta))
}

func (s *Server) schemaObjects(w http.ResponseWriter, r *http.Request) {
	schema := r.URL.Query().Get("schema")
	if schema == "" {
		apiError(w, 400, errors.New("schema is required"))
		return
	}
	c, err := s.connectorFor(r.Context(), r.PathValue("id"))
	if err != nil {
		apiError(w, 400, err)
		return
	}
	defer c.Close()
	discoverer, ok := c.(connector.SchemaObjectConnector)
	if !ok {
		apiError(w, http.StatusNotImplemented, errors.New("schema object discovery is not available for this datasource; implement the QMigration native connector catalog"))
		return
	}
	items, err := discoverer.ListSchemaObjects(r.Context(), schema)
	if err != nil {
		apiError(w, 400, err)
		return
	}
	writeJSON(w, 200, items)
}

func (s *Server) listMigrations(w http.ResponseWriter, r *http.Request) {
	items, err := s.migrations.List(r.Context())
	if err != nil {
		apiError(w, 500, err)
		return
	}
	writeJSON(w, 200, items)
}
func (s *Server) getMigration(w http.ResponseWriter, r *http.Request) {
	m, err := s.migrations.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		apiError(w, 404, err)
		return
	}
	writeJSON(w, 200, m)
}

type performanceProfileBundle struct {
	Format   string                    `json:"format"`
	Version  string                    `json:"version"`
	Exported time.Time                 `json:"exported_at"`
	Profiles []performanceProfileEntry `json:"profiles"`
}

type performanceProfileEntry struct {
	SourceID             string                                     `json:"source_id"`
	TargetID             string                                     `json:"target_id"`
	SourceSchema         string                                     `json:"source_schema"`
	SourceTable          string                                     `json:"source_table"`
	ProfileBytesPerSec   int64                                      `json:"profile_bytes_per_sec"`
	ProfileRowsPerSec    int64                                      `json:"profile_rows_per_sec"`
	RecommendedChunkRows int64                                      `json:"recommended_chunk_rows"`
	PerformanceSamples   int64                                      `json:"performance_samples"`
	TopologyPerformance  map[string]domain.TableTopologyPerformance `json:"topology_performance,omitempty"`
}

func (s *Server) exportPerformanceProfiles(w http.ResponseWriter, r *http.Request) {
	migrations, err := s.repo.ListMigrations(r.Context())
	if err != nil {
		apiError(w, 500, err)
		return
	}
	b := performanceProfileBundle{Format: "qmigration-performance-profile-v1", Version: version.Version, Exported: time.Now().UTC()}
	for _, m := range migrations {
		tables, e := s.repo.ListMigrationTables(r.Context(), m.ID)
		if e != nil {
			apiError(w, 500, e)
			return
		}
		for _, t := range tables {
			if t.PerformanceSamples <= 0 {
				continue
			}
			b.Profiles = append(b.Profiles, performanceProfileEntry{SourceID: m.SourceID, TargetID: m.TargetID, SourceSchema: t.SourceSchema, SourceTable: t.SourceTable, ProfileBytesPerSec: t.ProfileBytesPerSec, ProfileRowsPerSec: t.ProfileRowsPerSec, RecommendedChunkRows: t.RecommendedChunkRows, PerformanceSamples: t.PerformanceSamples, TopologyPerformance: t.TopologyPerformance})
		}
	}
	writeJSON(w, 200, b)
}

func (s *Server) importPerformanceProfiles(w http.ResponseWriter, r *http.Request) {
	var b performanceProfileBundle
	if err := decode(r, &b); err != nil {
		apiError(w, 400, err)
		return
	}
	if b.Format != "qmigration-performance-profile-v1" {
		apiError(w, 400, fmt.Errorf("unsupported performance profile format %q", b.Format))
		return
	}
	migrations, err := s.repo.ListMigrations(r.Context())
	if err != nil {
		apiError(w, 500, err)
		return
	}
	updated := 0
	for _, p := range b.Profiles {
		for _, m := range migrations {
			if m.SourceID != p.SourceID || m.TargetID != p.TargetID {
				continue
			}
			tables, e := s.repo.ListMigrationTables(r.Context(), m.ID)
			if e != nil {
				apiError(w, 500, e)
				return
			}
			for i := range tables {
				t := &tables[i]
				if t.SourceSchema != p.SourceSchema || t.SourceTable != p.SourceTable {
					continue
				}
				if p.PerformanceSamples < t.PerformanceSamples {
					continue
				} // never regress a newer local profile
				t.ProfileBytesPerSec = p.ProfileBytesPerSec
				t.ProfileRowsPerSec = p.ProfileRowsPerSec
				t.RecommendedChunkRows = p.RecommendedChunkRows
				t.PerformanceSamples = p.PerformanceSamples
				if p.TopologyPerformance != nil {
					t.TopologyPerformance = p.TopologyPerformance
				}
				if e := s.repo.UpdateMigrationTable(r.Context(), t); e != nil {
					apiError(w, 500, e)
					return
				}
				updated++
			}
		}
	}
	writeJSON(w, 200, map[string]any{"updated": updated, "format": b.Format})
}

func (s *Server) migrationTables(w http.ResponseWriter, r *http.Request) {
	items, err := s.migrations.Tables(r.Context(), r.PathValue("id"))
	if err != nil {
		apiError(w, 500, err)
		return
	}
	writeJSON(w, 200, items)
}
func (s *Server) migrationChunks(w http.ResponseWriter, r *http.Request) {
	items, err := s.migrations.Chunks(r.Context(), r.PathValue("id"))
	if err != nil {
		apiError(w, 500, err)
		return
	}
	writeJSON(w, 200, items)
}
func (s *Server) migrationLogs(w http.ResponseWriter, r *http.Request) {
	limit := 500
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if n, e := strconv.Atoi(raw); e == nil && n > 0 {
			limit = n
		}
	}
	items, err := s.migrations.Logs(r.Context(), r.PathValue("id"), limit)
	if err != nil {
		apiError(w, 500, err)
		return
	}
	writeJSON(w, 200, items)
}

func (s *Server) createMigration(w http.ResponseWriter, r *http.Request) {
	var m domain.MigrationTask
	if err := decode(r, &m); err != nil {
		apiError(w, 400, err)
		return
	}
	if m.Name == "" || m.SourceID == "" || m.TargetID == "" {
		apiError(w, 400, errors.New("name, source_datasource_id and target_datasource_id are required"))
		return
	}
	if m.Mode == "" {
		m.Mode = domain.ModeFull
	}
	if _, err := s.repo.GetDataSource(r.Context(), m.SourceID); err != nil {
		apiError(w, 400, fmt.Errorf("source datasource: %w", err))
		return
	}
	if _, err := s.repo.GetDataSource(r.Context(), m.TargetID); err != nil {
		apiError(w, 400, fmt.Errorf("target datasource: %w", err))
		return
	}
	if err := s.migrations.Create(r.Context(), &m); err != nil {
		apiError(w, 500, err)
		return
	}
	s.audit(r, "CREATE", "migration", m.ID, m.Name)
	writeJSON(w, 201, m)
}
func (s *Server) startMigration(w http.ResponseWriter, r *http.Request) {
	if err := s.migrations.Start(r.Context(), r.PathValue("id")); err != nil {
		apiError(w, 400, err)
		return
	}
	s.audit(r, "START", "migration", r.PathValue("id"), "")
	writeJSON(w, 200, map[string]any{"ok": true})
}
func (s *Server) pauseMigration(w http.ResponseWriter, r *http.Request) {
	if err := s.migrations.Pause(r.Context(), r.PathValue("id")); err != nil {
		apiError(w, 400, err)
		return
	}
	s.audit(r, "PAUSE", "migration", r.PathValue("id"), "")
	writeJSON(w, 200, map[string]any{"ok": true})
}
func (s *Server) resumeMigration(w http.ResponseWriter, r *http.Request) {
	if err := s.migrations.Resume(r.Context(), r.PathValue("id")); err != nil {
		apiError(w, 400, err)
		return
	}
	s.audit(r, "RESUME", "migration", r.PathValue("id"), "")
	writeJSON(w, 200, map[string]any{"ok": true})
}
func (s *Server) cancelMigration(w http.ResponseWriter, r *http.Request) {
	if err := s.migrations.Cancel(r.Context(), r.PathValue("id")); err != nil {
		apiError(w, 400, err)
		return
	}
	s.audit(r, "CANCEL", "migration", r.PathValue("id"), "")
	writeJSON(w, 200, map[string]any{"ok": true})
}

func (s *Server) precheckMigration(w http.ResponseWriter, r *http.Request) {
	items, err := s.migrations.Precheck(r.Context(), r.PathValue("id"))
	if err != nil {
		apiError(w, 400, err)
		return
	}
	writeJSON(w, 200, items)
}
func (s *Server) assessMigration(w http.ResponseWriter, r *http.Request) {
	assessment, err := s.migrations.AssessCompatibility(r.Context(), r.PathValue("id"))
	if err != nil {
		apiError(w, 400, err)
		return
	}
	writeJSON(w, 200, assessment)
}

func (s *Server) schemaObjectPlan(w http.ResponseWriter, r *http.Request) {
	plan, err := s.migrations.PlanSchemaObjects(r.Context(), r.PathValue("id"))
	if err != nil {
		apiError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, plan)
}

func (s *Server) applySchemaObjects(w http.ResponseWriter, r *http.Request) {
	var req domain.SchemaObjectApplyRequest
	if err := decode(r, &req); err != nil {
		apiError(w, http.StatusBadRequest, err)
		return
	}
	result, err := s.migrations.ApplySchemaObjects(r.Context(), r.PathValue("id"), req)
	if err != nil {
		apiError(w, http.StatusConflict, err)
		return
	}
	s.audit(r, "APPLY_SCHEMA_OBJECTS", "migration", r.PathValue("id"), fmt.Sprintf("applied=%d skipped=%d failed=%d", result.Applied, result.Skipped, result.Failed))
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) validationResults(w http.ResponseWriter, r *http.Request) {
	items, err := s.migrations.ValidationResults(r.Context(), r.PathValue("id"))
	if err != nil {
		apiError(w, 500, err)
		return
	}
	writeJSON(w, 200, items)
}
func (s *Server) validationArchive(w http.ResponseWriter, r *http.Request) {
	item, err := s.migrations.ValidationArchive(r.Context(), r.PathValue("id"))
	if err != nil {
		apiError(w, http.StatusConflict, err)
		return
	}
	if item == nil {
		apiError(w, http.StatusNotFound, errors.New("validation archive not found"))
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func (s *Server) buildValidationReport(ctx context.Context, taskID string) (validationreport.Bundle, error) {
	task, err := s.migrations.Get(ctx, taskID)
	if err != nil {
		return validationreport.Bundle{}, err
	}
	archive, err := s.migrations.ValidationArchive(ctx, taskID)
	if err != nil {
		return validationreport.Bundle{}, err
	}
	if archive == nil {
		archive, _, err = s.migrations.EnsureValidationArchive(ctx, taskID)
		if err != nil {
			return validationreport.Bundle{}, err
		}
	}
	if archive == nil {
		return validationreport.Bundle{}, errors.New("validation archive not found")
	}
	report, err := validationreport.NewReport(task, archive, version.Name, version.Version)
	if err != nil {
		return validationreport.Bundle{}, err
	}
	signer, err := validationreport.SignerFromEnv()
	if err != nil {
		return validationreport.Bundle{}, err
	}
	bundle, err := validationreport.BuildBundle(report, signer)
	if err != nil {
		return validationreport.Bundle{}, err
	}
	if err := validationreport.ApplyTimestamp(ctx, &bundle); err != nil {
		return validationreport.Bundle{}, err
	}
	return bundle, nil
}

func (s *Server) validationReportPublicKey(w http.ResponseWriter, r *http.Request) {
	signer, err := validationreport.SignerFromEnv()
	if err != nil {
		apiError(w, http.StatusInternalServerError, err)
		return
	}
	doc, err := validationreport.PublicKeyDocumentForSigner(signer)
	if err != nil {
		apiError(w, http.StatusInternalServerError, err)
		return
	}
	if doc == nil {
		apiError(w, http.StatusNotFound, errors.New("validation report Ed25519 signing key is not configured"))
		return
	}
	b, err := validationreport.MarshalPublicKeyDocument(doc)
	if err != nil {
		apiError(w, http.StatusInternalServerError, err)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="qmigration-validation-report-public-key.json"`)
	w.Header().Set("X-QMigration-Public-Key-Fingerprint-SHA256", doc.FingerprintSHA256)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(b)
}

type validationReportKeyTransitionRequest struct {
	NewPublicKey validationreport.PublicKeyDocument `json:"new_public_key"`
	Reason       string                             `json:"reason,omitempty"`
	NotBefore    time.Time                          `json:"not_before,omitempty"`
	OverlapUntil time.Time                          `json:"overlap_until,omitempty"`
}

type validationReportKeyRevocationRequest struct {
	TargetPublicKey validationreport.PublicKeyDocument `json:"target_public_key"`
	Reason          string                             `json:"reason"`
}

func (s *Server) validationReportKeyTransition(w http.ResponseWriter, r *http.Request) {
	var req validationReportKeyTransitionRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		apiError(w, http.StatusBadRequest, err)
		return
	}
	signer, err := validationreport.SignerFromEnv()
	if err != nil {
		apiError(w, http.StatusInternalServerError, err)
		return
	}
	cert, err := validationreport.NewScheduledKeyTransitionCertificate(signer, req.NewPublicKey, time.Now().UTC(), req.NotBefore, req.OverlapUntil, req.Reason)
	if err != nil {
		apiError(w, http.StatusConflict, err)
		return
	}
	b, err := validationreport.MarshalKeyTransitionCertificate(cert)
	if err != nil {
		apiError(w, http.StatusInternalServerError, err)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="qmigration-validation-report-key-transition.json"`)
	w.Header().Set("X-QMigration-Transition-From-Key-ID", cert.From.KeyID)
	w.Header().Set("X-QMigration-Transition-To-Key-ID", cert.To.KeyID)
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write(b)
}

func (s *Server) validationReportKeyRevocation(w http.ResponseWriter, r *http.Request) {
	var req validationReportKeyRevocationRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		apiError(w, http.StatusBadRequest, err)
		return
	}
	signer, err := validationreport.SignerFromEnv()
	if err != nil {
		apiError(w, http.StatusInternalServerError, err)
		return
	}
	cert, err := validationreport.NewKeyRevocationCertificate(signer, req.TargetPublicKey, time.Now().UTC(), req.Reason)
	if err != nil {
		apiError(w, http.StatusConflict, err)
		return
	}
	b, err := validationreport.MarshalKeyRevocationCertificate(cert)
	if err != nil {
		apiError(w, http.StatusInternalServerError, err)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="qmigration-validation-report-key-revocation.json"`)
	w.Header().Set("X-QMigration-Revocation-Issuer-Key-ID", cert.Issuer.KeyID)
	w.Header().Set("X-QMigration-Revocation-Target-Key-ID", cert.Target.KeyID)
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write(b)
}

func (s *Server) validationReport(w http.ResponseWriter, r *http.Request) {
	bundle, err := s.buildValidationReport(r.Context(), r.PathValue("id"))
	if err != nil {
		apiError(w, http.StatusConflict, err)
		return
	}
	format := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("format")))
	if format == "" {
		format = "html"
	}
	item, ok := validationreport.FindArtifact(bundle, format)
	if !ok {
		apiError(w, http.StatusBadRequest, fmt.Errorf("unsupported validation report format %q; use json, html, or pdf", format))
		return
	}
	w.Header().Set("Content-Type", item.ContentType)
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", item.Name))
	w.Header().Set("X-QMigration-Archive-Evidence-Digest", bundle.Report.Validation.EvidenceDigest)
	w.Header().Set("X-QMigration-Content-SHA256", item.SHA256)
	if item.HMACSHA256 != "" {
		w.Header().Set("X-QMigration-HMAC-SHA256", item.HMACSHA256)
		if bundle.Manifest.SignatureKeyID != "" {
			w.Header().Set("X-QMigration-Signature-Key-ID", bundle.Manifest.SignatureKeyID)
		}
	}
	if item.Ed25519Signature != "" {
		w.Header().Set("X-QMigration-Ed25519-Signature", item.Ed25519Signature)
		w.Header().Set("X-QMigration-Ed25519-Key-ID", bundle.Manifest.PublicSignatureKeyID)
		w.Header().Set("X-QMigration-Public-Key-Fingerprint-SHA256", bundle.Manifest.PublicKeyFingerprintSHA256)
	}
	s.validationReportExports.Add(1)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(item.Data)
}

func (s *Server) validationReportManifest(w http.ResponseWriter, r *http.Request) {
	bundle, err := s.buildValidationReport(r.Context(), r.PathValue("id"))
	if err != nil {
		apiError(w, http.StatusConflict, err)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="manifest.json"`)
	w.Header().Set("X-QMigration-Content-SHA256", fmt.Sprintf("%x", sha256.Sum256(bundle.ManifestJSON)))
	if bundle.Manifest.ManifestEd25519Signature != "" {
		w.Header().Set("X-QMigration-Ed25519-Signature", bundle.Manifest.ManifestEd25519Signature)
		w.Header().Set("X-QMigration-Signature-Scope", "canonical-manifest-signature-fields-cleared")
		w.Header().Set("X-QMigration-Ed25519-Key-ID", bundle.Manifest.PublicSignatureKeyID)
		w.Header().Set("X-QMigration-Public-Key-Fingerprint-SHA256", bundle.Manifest.PublicKeyFingerprintSHA256)
	}
	s.validationReportExports.Add(1)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(bundle.ManifestJSON)
}

func (s *Server) validationReportArchiveRecord(w http.ResponseWriter, r *http.Request) {
	archive, err := s.migrations.ValidationArchive(r.Context(), r.PathValue("id"))
	if err != nil {
		apiError(w, http.StatusConflict, err)
		return
	}
	if archive == nil {
		apiError(w, http.StatusNotFound, errors.New("validation archive not found"))
		return
	}
	record, err := s.migrations.ValidationReportArchive(r.Context(), r.PathValue("id"), archive.EvidenceDigest)
	if err != nil {
		apiError(w, http.StatusConflict, err)
		return
	}
	if record == nil {
		apiError(w, http.StatusNotFound, errors.New("validation report external archive is not registered"))
		return
	}
	writeJSON(w, http.StatusOK, record)
}

func (s *Server) archiveValidationReport(w http.ResponseWriter, r *http.Request) {
	bundle, err := s.buildValidationReport(r.Context(), r.PathValue("id"))
	if err != nil {
		apiError(w, http.StatusConflict, err)
		return
	}
	cfg := validationreport.S3ConfigFromEnv()
	if !cfg.Configured() {
		apiError(w, http.StatusConflict, errors.New("validation report S3 archive is not configured"))
		return
	}
	result, err := validationreport.ArchiveBundleS3(r.Context(), cfg, bundle)
	if err != nil {
		s.validationReportArchiveFailures.Add(1)
		apiError(w, http.StatusBadGateway, err)
		return
	}
	if !result.Committed {
		s.validationReportArchiveFailures.Add(1)
		apiError(w, http.StatusBadGateway, errors.New("validation report archive did not commit"))
		return
	}
	record := &domain.ValidationReportArchiveRecord{
		TaskID: r.PathValue("id"), EvidenceDigest: bundle.Report.Validation.EvidenceDigest, URI: result.URI, Bucket: result.Bucket, Prefix: result.Prefix,
		ManifestSHA256: result.ManifestSHA256, PublicSignatureAlgorithm: bundle.Manifest.PublicSignatureAlgorithm, PublicSignatureKeyID: bundle.Manifest.PublicSignatureKeyID,
		PublicKeyEd25519: bundle.Manifest.PublicKeyEd25519, PublicKeyFingerprintSHA256: bundle.Manifest.PublicKeyFingerprintSHA256,
		ObjectLockMode: result.ObjectLockMode, RetainUntil: result.RetainUntil, LegalHold: result.LegalHold, CommittedAt: time.Now().UTC(),
	}
	if _, err := s.migrations.RecordValidationReportArchive(r.Context(), record); err != nil {
		s.validationReportArchiveFailures.Add(1)
		apiError(w, http.StatusConflict, fmt.Errorf("S3 archive committed but metadata registry failed: %w", err))
		return
	}
	persisted, err := s.migrations.ValidationReportArchive(r.Context(), record.TaskID, record.EvidenceDigest)
	if err != nil || persisted == nil {
		s.validationReportArchiveFailures.Add(1)
		if err == nil {
			err = errors.New("validation report registry record missing after commit")
		}
		apiError(w, http.StatusInternalServerError, err)
		return
	}
	s.validationReportArchiveSuccess.Add(1)
	s.audit(r, "ARCHIVE_VALIDATION_REPORT", "migration", r.PathValue("id"), result.URI+" manifest_sha256="+result.ManifestSHA256)
	writeJSON(w, http.StatusCreated, struct {
		validationreport.ArchiveResult
		Registry *domain.ValidationReportArchiveRecord `json:"registry"`
	}{ArchiveResult: result, Registry: persisted})
}
func (s *Server) validateMigration(w http.ResponseWriter, r *http.Request) {
	if err := s.migrations.ValidateNow(r.Context(), r.PathValue("id")); err != nil {
		apiError(w, 400, err)
		return
	}
	s.audit(r, "VALIDATE", "migration", r.PathValue("id"), "")
	writeJSON(w, 202, map[string]any{"ok": true})
}

func (s *Server) repairMigration(w http.ResponseWriter, r *http.Request) {
	n, err := s.migrations.RepairValidation(r.Context(), r.PathValue("id"))
	if err != nil {
		apiError(w, 400, err)
		return
	}
	s.audit(r, "REPAIR", "migration", r.PathValue("id"), fmt.Sprintf("reset_chunks=%d", n))
	writeJSON(w, 202, map[string]any{"ok": true, "reset_chunks": n})
}

func (s *Server) cdcPositions(w http.ResponseWriter, r *http.Request) {
	items, err := s.migrations.CDCPositions(r.Context(), r.PathValue("id"))
	if err != nil {
		apiError(w, 500, err)
		return
	}
	writeJSON(w, 200, items)
}
func (s *Server) cdcSpoolStats(w http.ResponseWriter, r *http.Request) {
	direction := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("direction")))
	if direction == "" {
		direction = "forward"
	}
	if direction != "forward" && direction != "reverse" {
		apiError(w, 400, fmt.Errorf("invalid CDC direction %q", direction))
		return
	}
	stats, err := s.migrations.CDCSpoolStats(r.Context(), r.PathValue("id"), direction)
	if err != nil {
		apiError(w, 500, err)
		return
	}
	writeJSON(w, 200, stats)
}
func (s *Server) drainCDCSpool(w http.ResponseWriter, r *http.Request) {
	direction := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("direction")))
	if direction == "" {
		direction = "forward"
	}
	if direction != "forward" && direction != "reverse" {
		apiError(w, 400, fmt.Errorf("invalid CDC direction %q", direction))
		return
	}
	stats, err := s.migrations.DrainCDCSpool(r.Context(), r.PathValue("id"), direction, 10000)
	if err != nil {
		apiError(w, http.StatusConflict, err)
		return
	}
	s.audit(r, "DRAIN_CDC_SPOOL", "migration", r.PathValue("id"), fmt.Sprintf("direction=%s pending=%d", direction, stats.PendingTransactions))
	writeJSON(w, 200, stats)
}
func (s *Server) markCDCStarted(w http.ResponseWriter, r *http.Request) {
	var p domain.CDCPosition
	_ = decode(r, &p)
	if err := s.migrations.MarkCDCStarted(r.Context(), r.PathValue("id"), &p); err != nil {
		apiError(w, 400, err)
		return
	}
	s.audit(r, "CDC_STARTED", "migration", r.PathValue("id"), p.PositionValue)
	writeJSON(w, 200, map[string]any{"ok": true})
}

func (s *Server) recordCDCProgress(w http.ResponseWriter, r *http.Request) {
	var p domain.CDCPosition
	if err := decode(r, &p); err != nil {
		apiError(w, 400, err)
		return
	}
	if err := s.migrations.RecordCDCProgress(r.Context(), r.PathValue("id"), &p); err != nil {
		apiError(w, 400, err)
		return
	}
	writeJSON(w, 202, map[string]any{"ok": true})
}
func (s *Server) cdcDeadLetters(w http.ResponseWriter, r *http.Request) {
	items, err := s.migrations.CDCDeadLetters(r.Context(), r.PathValue("id"))
	if err != nil {
		apiError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, items)
}

func (s *Server) replayCDCDeadLetter(w http.ResponseWriter, r *http.Request) {
	result, err := s.migrations.ReplayCDCDeadLetter(r.Context(), r.PathValue("id"), r.PathValue("dlq_id"))
	if err != nil {
		apiError(w, http.StatusConflict, err)
		return
	}
	s.audit(r, "REPLAY_CDC_DLQ", "migration", r.PathValue("id"), fmt.Sprintf("dlq=%s duplicate=%t applied=%d", r.PathValue("dlq_id"), result.Duplicate, result.Applied))
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) resolveCDCCommitUncertain(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Decision string `json:"decision"`
	}
	if err := decode(r, &in); err != nil {
		apiError(w, http.StatusBadRequest, err)
		return
	}
	result, err := s.migrations.ResolveCDCCommitUncertain(r.Context(), r.PathValue("id"), r.PathValue("dlq_id"), in.Decision)
	if err != nil {
		apiError(w, http.StatusConflict, err)
		return
	}
	s.audit(r, "RESOLVE_CDC_COMMIT_UNCERTAIN", "migration", r.PathValue("id"), fmt.Sprintf("dlq=%s decision=%s position=%s", r.PathValue("dlq_id"), strings.ToUpper(strings.TrimSpace(in.Decision)), result.PositionValue))
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) cdcConflicts(w http.ResponseWriter, r *http.Request) {
	items, err := s.migrations.CDCConflicts(r.Context(), r.PathValue("id"))
	if err != nil {
		apiError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, items)
}

func (s *Server) applyCDCEvents(w http.ResponseWriter, r *http.Request) {
	var req domain.CDCApplyRequest
	if err := decode(r, &req); err != nil {
		apiError(w, http.StatusBadRequest, err)
		return
	}
	if strings.EqualFold(req.Direction, "reverse") {
		role := r.Header.Get("X-QMigration-Role")
		if role != "" && role != string(auth.RoleAdmin) && role != string(auth.RoleDBA) {
			apiError(w, http.StatusForbidden, errors.New("reverse CDC apply requires admin or dba role"))
			return
		}
	}
	result, err := s.migrations.ApplyCDCEvents(r.Context(), r.PathValue("id"), req)
	if err != nil {
		apiError(w, http.StatusConflict, err)
		return
	}
	s.audit(r, "APPLY_CDC_EVENTS", "migration", r.PathValue("id"), fmt.Sprintf("direction=%s applied=%d position=%s", req.Direction, result.Applied, result.PositionValue))
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) applyDebeziumEvents(w http.ResponseWriter, r *http.Request) {
	s.applyCompatibilityCDC(w, r, "debezium")
}

func (s *Server) applyCanalEvents(w http.ResponseWriter, r *http.Request) {
	s.applyCompatibilityCDC(w, r, "canal")
}

func (s *Server) applyCompatibilityCDC(w http.ResponseWriter, r *http.Request, format string) {
	const maxCompatibilityCDCBody = 16 << 20
	r.Body = http.MaxBytesReader(w, r.Body, maxCompatibilityCDCBody)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		apiError(w, http.StatusRequestEntityTooLarge, fmt.Errorf("read %s CDC payload: %w", format, err))
		return
	}
	var events []domain.CDCEvent
	switch format {
	case "debezium":
		events, err = compatcdc.NormalizeDebezium(raw)
	case "canal":
		events, err = compatcdc.NormalizeCanal(raw)
	default:
		err = fmt.Errorf("unsupported compatibility CDC format %q", format)
	}
	if err != nil {
		apiError(w, http.StatusBadRequest, err)
		return
	}
	if len(events) == 0 {
		apiError(w, http.StatusBadRequest, errors.New("compatibility CDC payload produced no events"))
		return
	}
	direction := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("direction")))
	if direction == "" {
		direction = "forward"
	}
	if direction != "forward" && direction != "reverse" {
		apiError(w, http.StatusBadRequest, fmt.Errorf("invalid CDC direction %q", direction))
		return
	}
	if direction == "reverse" {
		role := r.Header.Get("X-QMigration-Role")
		if role != "" && role != string(auth.RoleAdmin) && role != string(auth.RoleDBA) {
			apiError(w, http.StatusForbidden, errors.New("reverse CDC apply requires admin or dba role"))
			return
		}
	}

	// Push-based engines may be the first component to announce that capture is
	// alive. For incremental-only tasks this immediately opens the apply gate.
	// For full+incremental tasks it starts full load, but the source must keep
	// the record unacknowledged until QMigration reaches CDC_CATCHING_UP.
	task, getErr := s.migrations.Get(r.Context(), r.PathValue("id"))
	if getErr != nil {
		apiError(w, http.StatusNotFound, getErr)
		return
	}
	if direction == "forward" && task.Status == domain.StatusCDCInitializing {
		// The pushed record has not been applied yet, so it MUST NOT become the
		// durable CDC checkpoint. Open only the capture/state gate here; the
		// normal ApplyCDCEvents path persists the record position after target
		// apply succeeds. This also makes HTTP 425 retries lossless.
		if err := s.migrations.MarkCDCStarted(r.Context(), task.ID, nil); err != nil {
			apiError(w, http.StatusConflict, err)
			return
		}
		task, err = s.migrations.Get(r.Context(), task.ID)
		if err != nil {
			apiError(w, http.StatusConflict, err)
			return
		}
	}
	if direction == "reverse" && task.Status == domain.StatusRollbackPreparing {
		// Same apply-before-checkpoint rule as the forward path.
		if err := s.migrations.MarkRollbackCDCStarted(r.Context(), task.ID, nil); err != nil {
			apiError(w, http.StatusConflict, err)
			return
		}
		task, err = s.migrations.Get(r.Context(), task.ID)
		if err != nil {
			apiError(w, http.StatusConflict, err)
			return
		}
	}
	ready := false
	if direction == "forward" {
		ready = task.Status == domain.StatusCDCCatchingUp || task.Status == domain.StatusReadyCutover
	} else {
		ready = task.Status == domain.StatusRollbackSyncing || task.Status == domain.StatusRollbackReady
	}
	if !ready {
		writeJSON(w, http.StatusTooEarly, map[string]any{
			"error":       fmt.Sprintf("%s CDC capture is active but apply is not ready from task status %s", format, task.Status),
			"retryable":   true,
			"task_status": task.Status,
			"rule":        "do not acknowledge the upstream Debezium/Canal record; retry after full load/validation reaches the CDC apply gate",
		})
		return
	}
	result, err := s.migrations.ApplyCDCEvents(r.Context(), task.ID, domain.CDCApplyRequest{Direction: direction, Events: events})
	if err != nil {
		apiError(w, http.StatusConflict, err)
		return
	}
	s.audit(r, "APPLY_"+strings.ToUpper(format)+"_CDC", "migration", task.ID, fmt.Sprintf("direction=%s events=%d applied=%d position=%s", direction, len(events), result.Applied, result.PositionValue))
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) readyCutover(w http.ResponseWriter, r *http.Request) {
	var in struct {
		MaxLagMS int64 `json:"max_lag_ms"`
	}
	_ = decode(r, &in)
	if err := s.migrations.ReadyForCutover(r.Context(), r.PathValue("id"), in.MaxLagMS); err != nil {
		apiError(w, 400, err)
		return
	}
	s.audit(r, "READY_CUTOVER", "migration", r.PathValue("id"), "")
	writeJSON(w, 200, map[string]any{"ok": true})
}
func (s *Server) cutover(w http.ResponseWriter, r *http.Request) {
	if err := s.migrations.Cutover(r.Context(), r.PathValue("id")); err != nil {
		apiError(w, 400, err)
		return
	}
	s.audit(r, "CUTOVER", "migration", r.PathValue("id"), "")
	writeJSON(w, 200, map[string]any{"ok": true})
}

func (s *Server) prepareRollback(w http.ResponseWriter, r *http.Request) {
	if err := s.migrations.PrepareRollback(r.Context(), r.PathValue("id")); err != nil {
		apiError(w, 400, err)
		return
	}
	s.audit(r, "ROLLBACK_PREPARE", "migration", r.PathValue("id"), "")
	writeJSON(w, 202, map[string]any{"ok": true})
}
func (s *Server) markRollbackCDCStarted(w http.ResponseWriter, r *http.Request) {
	var p domain.CDCPosition
	_ = decode(r, &p)
	if err := s.migrations.MarkRollbackCDCStarted(r.Context(), r.PathValue("id"), &p); err != nil {
		apiError(w, 400, err)
		return
	}
	s.audit(r, "ROLLBACK_CDC_STARTED", "migration", r.PathValue("id"), p.PositionValue)
	writeJSON(w, 200, map[string]any{"ok": true})
}
func (s *Server) recordRollbackCDCProgress(w http.ResponseWriter, r *http.Request) {
	var p domain.CDCPosition
	if err := decode(r, &p); err != nil {
		apiError(w, 400, err)
		return
	}
	if err := s.migrations.RecordRollbackCDCProgress(r.Context(), r.PathValue("id"), &p); err != nil {
		apiError(w, 400, err)
		return
	}
	writeJSON(w, 202, map[string]any{"ok": true})
}
func (s *Server) readyRollback(w http.ResponseWriter, r *http.Request) {
	var in struct {
		MaxLagMS int64 `json:"max_lag_ms"`
	}
	_ = decode(r, &in)
	if err := s.migrations.ReadyForRollback(r.Context(), r.PathValue("id"), in.MaxLagMS); err != nil {
		apiError(w, 400, err)
		return
	}
	s.audit(r, "ROLLBACK_READY", "migration", r.PathValue("id"), "")
	writeJSON(w, 200, map[string]any{"ok": true})
}
func (s *Server) rollbackMigration(w http.ResponseWriter, r *http.Request) {
	if err := s.migrations.Rollback(r.Context(), r.PathValue("id")); err != nil {
		apiError(w, 400, err)
		return
	}
	s.audit(r, "ROLLBACK", "migration", r.PathValue("id"), "")
	writeJSON(w, 200, map[string]any{"ok": true})
}

func (s *Server) listAlerts(w http.ResponseWriter, r *http.Request) {
	items, err := s.repo.ListAlerts(r.Context())
	if err != nil {
		apiError(w, 500, err)
		return
	}
	writeJSON(w, 200, items)
}
func (s *Server) ackAlert(w http.ResponseWriter, r *http.Request) {
	if err := s.repo.AcknowledgeAlert(r.Context(), r.PathValue("id")); err != nil {
		apiError(w, 404, err)
		return
	}
	s.audit(r, "ACK", "alert", r.PathValue("id"), "")
	writeJSON(w, 200, map[string]any{"ok": true})
}
func (s *Server) listAudit(w http.ResponseWriter, r *http.Request) {
	items, err := s.repo.ListAuditEvents(r.Context(), 200)
	if err != nil {
		apiError(w, 500, err)
		return
	}
	writeJSON(w, 200, items)
}

func (s *Server) listWorkers(w http.ResponseWriter, r *http.Request) {
	items, err := s.workers.List(r.Context())
	if err != nil {
		apiError(w, 500, err)
		return
	}
	writeJSON(w, 200, items)
}
func (s *Server) registerWorker(w http.ResponseWriter, r *http.Request) {
	var item domain.Worker
	if err := decode(r, &item); err != nil {
		apiError(w, 400, err)
		return
	}
	if err := s.workers.Register(r.Context(), &item); err != nil {
		apiError(w, 500, err)
		return
	}
	writeJSON(w, 201, item)
}
func (s *Server) heartbeat(w http.ResponseWriter, r *http.Request) {
	var item domain.Worker
	if err := decode(r, &item); err != nil {
		apiError(w, 400, err)
		return
	}
	item.ID = r.PathValue("id")
	if err := s.workers.Heartbeat(r.Context(), &item); err != nil {
		apiError(w, 500, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}
func (s *Server) claimChunk(w http.ResponseWriter, r *http.Request) {
	job, err := s.migrations.ClaimChunk(r.Context(), r.PathValue("id"))
	if errors.Is(err, repository.ErrNoChunk) {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err != nil {
		apiError(w, 400, err)
		return
	}
	writeJSON(w, 200, job)
}
func (s *Server) renewChunk(w http.ResponseWriter, r *http.Request) {
	var req domain.ChunkProgress
	if r.ContentLength > 0 {
		if err := decode(r, &req); err != nil {
			apiError(w, 400, err)
			return
		}
	}
	control, err := s.migrations.RenewChunk(r.Context(), r.PathValue("id"), r.PathValue("chunk_id"), req)
	if err != nil {
		apiError(w, 409, err)
		return
	}
	writeJSON(w, 200, control)
}
func (s *Server) completeChunk(w http.ResponseWriter, r *http.Request) {
	var result domain.ChunkResult
	if err := decode(r, &result); err != nil {
		apiError(w, 400, err)
		return
	}
	if err := s.migrations.CompleteChunk(r.Context(), r.PathValue("id"), r.PathValue("chunk_id"), result); err != nil {
		apiError(w, 409, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}
func (s *Server) failChunk(w http.ResponseWriter, r *http.Request) {
	var result domain.ChunkResult
	if err := decode(r, &result); err != nil {
		apiError(w, 400, err)
		return
	}
	if err := s.migrations.FailChunk(r.Context(), r.PathValue("id"), r.PathValue("chunk_id"), result); err != nil {
		apiError(w, 409, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

func (s *Server) engineJobs(w http.ResponseWriter, r *http.Request) {
	items, err := s.migrations.ListEngineJobs(r.Context(), r.PathValue("id"))
	if err != nil {
		apiError(w, 500, err)
		return
	}
	writeJSON(w, 200, items)
}
func (s *Server) claimEngineJob(w http.ResponseWriter, r *http.Request) {
	job, err := s.migrations.ClaimEngineJob(r.Context(), r.PathValue("id"))
	if errors.Is(err, repository.ErrNoChunk) {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err != nil {
		apiError(w, 409, err)
		return
	}
	writeJSON(w, 200, job)
}
func (s *Server) startEngineJob(w http.ResponseWriter, r *http.Request) {
	if err := s.migrations.StartEngineJob(r.Context(), r.PathValue("id"), r.PathValue("job_id")); err != nil {
		apiError(w, 409, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}
func (s *Server) renewEngineJob(w http.ResponseWriter, r *http.Request) {
	if err := s.migrations.RenewEngineJob(r.Context(), r.PathValue("id"), r.PathValue("job_id")); err != nil {
		apiError(w, 409, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}
func (s *Server) workerEngineJobCDCEvents(w http.ResponseWriter, r *http.Request) {
	var req domain.CDCApplyRequest
	if err := decode(r, &req); err != nil {
		apiError(w, http.StatusBadRequest, err)
		return
	}
	result, err := s.migrations.ApplyEngineJobCDCEvents(r.Context(), r.PathValue("id"), r.PathValue("job_id"), req)
	if err != nil {
		apiError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) workerEngineJobCDCReady(w http.ResponseWriter, r *http.Request) {
	ready, status, err := s.migrations.EngineJobCDCReady(r.Context(), r.PathValue("id"), r.PathValue("job_id"))
	if err != nil {
		apiError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ready": ready, "task_status": status})
}

func (s *Server) engineJobControl(w http.ResponseWriter, r *http.Request) {
	stop, err := s.migrations.EngineJobControl(r.Context(), r.PathValue("id"), r.PathValue("job_id"))
	if err != nil {
		apiError(w, 409, err)
		return
	}
	writeJSON(w, 200, map[string]any{"stop": stop})
}
func (s *Server) completeEngineJob(w http.ResponseWriter, r *http.Request) {
	var result domain.EngineJobResult
	_ = decode(r, &result)
	if err := s.migrations.CompleteEngineJob(r.Context(), r.PathValue("id"), r.PathValue("job_id"), result); err != nil {
		apiError(w, 409, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}
func (s *Server) failEngineJob(w http.ResponseWriter, r *http.Request) {
	var result domain.EngineJobResult
	_ = decode(r, &result)
	if err := s.migrations.FailEngineJob(r.Context(), r.PathValue("id"), r.PathValue("job_id"), result); err != nil {
		apiError(w, 409, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type userCreateRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Role     string `json:"role"`
	Enabled  *bool  `json:"enabled,omitempty"`
}

type userUpdateRequest struct {
	Username string `json:"username"`
	Role     string `json:"role"`
	Enabled  *bool  `json:"enabled,omitempty"`
}

type passwordChangeRequest struct {
	Password string `json:"password"`
}

func validRole(v string) (auth.Role, bool) {
	r := auth.Role(strings.ToLower(strings.TrimSpace(v)))
	switch r {
	case auth.RoleAdmin, auth.RoleDBA, auth.RoleOperator, auth.RoleViewer:
		return r, true
	default:
		return "", false
	}
}

func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	var req loginRequest
	if err := decode(r, &req); err != nil {
		apiError(w, http.StatusBadRequest, errors.New("invalid login request"))
		return
	}
	username := strings.ToLower(strings.TrimSpace(req.Username))
	u, err := s.repo.GetUserByUsername(r.Context(), username)
	if err != nil || !u.Enabled || !auth.VerifyPassword(u.PasswordHash, req.Password) {
		// Deliberately keep one generic error so account existence is not leaked.
		apiError(w, http.StatusUnauthorized, errors.New("invalid username or password"))
		return
	}
	role, ok := validRole(u.Role)
	if !ok {
		apiError(w, http.StatusUnauthorized, errors.New("user role is invalid"))
		return
	}
	token, expiry, err := s.sessions.Issue(u.ID, u.Username, role)
	if err != nil {
		apiError(w, http.StatusInternalServerError, err)
		return
	}
	u.LastLoginAt = time.Now().UTC()
	u.UpdatedAt = u.LastLoginAt
	_ = s.repo.UpdateUser(r.Context(), u)
	_ = s.repo.CreateAuditEvent(r.Context(), &domain.AuditEvent{ID: randomID("aud"), Actor: u.Username, Action: "LOGIN", ResourceType: "auth", ResourceID: u.ID, RemoteAddr: r.RemoteAddr, CreatedAt: time.Now().UTC()})
	writeJSON(w, http.StatusOK, map[string]any{
		"token": token, "expires_at": expiry, "user": u,
	})
}

func (s *Server) me(w http.ResponseWriter, r *http.Request) {
	if !s.authRequired.Load() {
		writeJSON(w, http.StatusOK, map[string]any{"authenticated": false, "open_mode": true, "username": "open-mode", "role": auth.RoleAdmin})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"authenticated": true, "open_mode": false, "username": r.Header.Get("X-QMigration-User"), "role": r.Header.Get("X-QMigration-Role")})
}

func (s *Server) listUsers(w http.ResponseWriter, r *http.Request) {
	users, err := s.repo.ListUsers(r.Context())
	if err != nil {
		apiError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, users)
}

func (s *Server) createUser(w http.ResponseWriter, r *http.Request) {
	var req userCreateRequest
	if err := decode(r, &req); err != nil {
		apiError(w, http.StatusBadRequest, err)
		return
	}
	username := strings.ToLower(strings.TrimSpace(req.Username))
	if len(username) < 3 || len(username) > 64 {
		apiError(w, http.StatusBadRequest, errors.New("username must be 3-64 characters"))
		return
	}
	role, ok := validRole(req.Role)
	if !ok {
		apiError(w, http.StatusBadRequest, errors.New("invalid role"))
		return
	}
	hash, err := auth.HashPassword(req.Password)
	if err != nil {
		apiError(w, http.StatusBadRequest, err)
		return
	}
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	now := time.Now().UTC()
	u := &domain.User{ID: randomID("usr"), Username: username, PasswordHash: hash, Role: string(role), Enabled: enabled, CreatedAt: now, UpdatedAt: now}
	if err := s.repo.CreateUser(r.Context(), u); err != nil {
		apiError(w, http.StatusConflict, err)
		return
	}
	// Creating the first account closes development/open mode immediately.
	s.authRequired.Store(true)
	s.audit(r, "CREATE_USER", "user", u.ID, "username="+u.Username+" role="+u.Role)
	writeJSON(w, http.StatusCreated, u)
}

func (s *Server) updateUser(w http.ResponseWriter, r *http.Request) {
	u, err := s.repo.GetUser(r.Context(), r.PathValue("id"))
	if err != nil {
		apiError(w, http.StatusNotFound, err)
		return
	}
	var req userUpdateRequest
	if err := decode(r, &req); err != nil {
		apiError(w, http.StatusBadRequest, err)
		return
	}
	username := strings.ToLower(strings.TrimSpace(req.Username))
	if username != "" {
		if len(username) < 3 || len(username) > 64 {
			apiError(w, http.StatusBadRequest, errors.New("username must be 3-64 characters"))
			return
		}
		u.Username = username
	}
	if req.Role != "" {
		role, ok := validRole(req.Role)
		if !ok {
			apiError(w, http.StatusBadRequest, errors.New("invalid role"))
			return
		}
		u.Role = string(role)
	}
	if req.Enabled != nil {
		u.Enabled = *req.Enabled
	}
	if err := s.ensureEnabledAdminRemains(r.Context(), u); err != nil {
		apiError(w, http.StatusConflict, err)
		return
	}
	u.UpdatedAt = time.Now().UTC()
	if err := s.repo.UpdateUser(r.Context(), u); err != nil {
		apiError(w, http.StatusConflict, err)
		return
	}
	s.audit(r, "UPDATE_USER", "user", u.ID, "username="+u.Username+" role="+u.Role)
	writeJSON(w, http.StatusOK, u)
}

func (s *Server) changeUserPassword(w http.ResponseWriter, r *http.Request) {
	u, err := s.repo.GetUser(r.Context(), r.PathValue("id"))
	if err != nil {
		apiError(w, http.StatusNotFound, err)
		return
	}
	var req passwordChangeRequest
	if err := decode(r, &req); err != nil {
		apiError(w, http.StatusBadRequest, err)
		return
	}
	hash, err := auth.HashPassword(req.Password)
	if err != nil {
		apiError(w, http.StatusBadRequest, err)
		return
	}
	u.PasswordHash = hash
	u.UpdatedAt = time.Now().UTC()
	if err := s.repo.UpdateUser(r.Context(), u); err != nil {
		apiError(w, http.StatusInternalServerError, err)
		return
	}
	s.audit(r, "RESET_USER_PASSWORD", "user", u.ID, "username="+u.Username)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) ensureEnabledAdminRemains(ctx context.Context, changed *domain.User) error {
	if changed.Enabled && auth.Role(changed.Role) == auth.RoleAdmin {
		return nil
	}
	users, err := s.repo.ListUsers(ctx)
	if err != nil {
		return err
	}
	for _, u := range users {
		if u.ID != changed.ID && u.Enabled && auth.Role(u.Role) == auth.RoleAdmin {
			return nil
		}
	}
	return errors.New("at least one enabled admin user is required")
}

func logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start))
	})
}
func cors(next http.Handler) http.Handler {
	origin := os.Getenv("QMIGRATION_CORS_ORIGIN")
	if origin == "" {
		origin = "*"
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-QMigration-User, X-QMigration-API-Token, X-QMigration-Worker-Token")
		w.Header().Set("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) apiAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/api/v1/") || isWorkerInternalRequest(r) || r.URL.Path == "/api/v1/auth/login" {
			next.ServeHTTP(w, r)
			return
		}
		if !s.authRequired.Load() {
			// Development bootstrap mode. Once the first user is created, the
			// server atomically switches to authenticated mode.
			r.Header.Set("X-QMigration-Role", string(auth.RoleAdmin))
			if r.Header.Get("X-QMigration-User") == "" {
				r.Header.Set("X-QMigration-User", "open-mode")
			}
			next.ServeHTTP(w, r)
			return
		}
		got := r.Header.Get("X-QMigration-API-Token")
		if got == "" {
			authorization := r.Header.Get("Authorization")
			if strings.HasPrefix(authorization, "Bearer ") {
				got = strings.TrimSpace(strings.TrimPrefix(authorization, "Bearer "))
			}
		}
		if got == "" && r.URL.Path == "/api/v1/ws" {
			for _, proto := range strings.Split(r.Header.Get("Sec-WebSocket-Protocol"), ",") {
				proto = strings.TrimSpace(proto)
				if !strings.HasPrefix(proto, "auth.") {
					continue
				}
				raw := strings.TrimPrefix(proto, "auth.")
				if decoded, err := base64.RawURLEncoding.DecodeString(raw); err == nil {
					got = string(decoded)
				}
			}
		}
		if got == "" {
			apiError(w, http.StatusUnauthorized, errors.New("authentication required"))
			return
		}

		var role auth.Role
		var actor string
		if staticRole, ok := s.staticTokens.Authenticate(got); ok {
			role, actor = staticRole, "token:"+string(staticRole)
		} else {
			claims, err := s.sessions.Verify(got)
			if err != nil {
				apiError(w, http.StatusUnauthorized, errors.New("invalid or expired credentials"))
				return
			}
			u, err := s.repo.GetUser(r.Context(), claims.Subject)
			if err != nil || !u.Enabled || auth.Role(u.Role) != claims.Role {
				apiError(w, http.StatusUnauthorized, errors.New("user is disabled or session is stale"))
				return
			}
			role, actor = claims.Role, u.Username
		}
		if !auth.Allowed(role, r.Method, r.URL.Path) {
			apiError(w, http.StatusForbidden, fmt.Errorf("role %s is not allowed to perform this operation", role))
			return
		}
		r.Header.Set("X-QMigration-Role", string(role))
		r.Header.Set("X-QMigration-User", actor)
		next.ServeHTTP(w, r)
	})
}

func isWorkerInternalRequest(r *http.Request) bool {
	return r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/api/v1/workers/")
}

func workerAuth(next http.Handler) http.Handler {
	token := os.Getenv("QMIGRATION_WORKER_TOKEN")
	if token == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		internal := isWorkerInternalRequest(r)
		if internal && r.Header.Get("X-QMigration-Worker-Token") != token {
			apiError(w, http.StatusUnauthorized, errors.New("invalid worker token"))
			return
		}
		next.ServeHTTP(w, r)
	})
}
