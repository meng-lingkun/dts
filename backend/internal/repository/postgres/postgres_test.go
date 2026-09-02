package postgres

import (
	"qmigration/backend/internal/repository"
	"strings"
	"testing"
	"time"
)

func TestDatasourceColumnsStayAlignedWithParser(t *testing.T) {
	cols := datasourceCols()
	for _, required := range []string{"tls_client_cert", "tls_client_key_ciphertext", "created_at", "updated_at"} {
		if !strings.Contains(cols, required) {
			t.Fatalf("datasourceCols missing %s: %s", required, cols)
		}
	}
	row := make([][]byte, 19)
	values := []string{"ds1", "source", "mysql", "db", "3306", "user", "cipher", "app", "", "", "", "", "REQUIRED", "db.internal", "ca", "client-cert", "client-key-cipher", "1000", "2000"}
	for i, v := range values {
		row[i] = []byte(v)
	}
	ds := parseDS(row)
	if ds.TLSClientCert != "client-cert" || ds.TLSClientKeyCiphertext != "client-key-cipher" {
		t.Fatalf("mTLS fields misparsed: %+v", ds)
	}
	if !ds.CreatedAt.Equal(time.UnixMilli(1000)) || !ds.UpdatedAt.Equal(time.UnixMilli(2000)) {
		t.Fatalf("timestamps misparsed: created=%v updated=%v", ds.CreatedAt, ds.UpdatedAt)
	}
}
func TestSchemaSQLMarksUnifiedDev8MetadataVersion(t *testing.T) {
	for _, required := range []string{"metadata_schema_state", "cdc_spool", "payload_bytes", "cdc_spool_drain_leases", "validation_barrier_position_value", "0.15.0-unified-dev7", "0.15.0-unified-dev8", "0.15.0-unified-dev9", "ON CONFLICT (id) DO UPDATE"} {
		if !strings.Contains(schemaSQL, required) {
			t.Fatalf("schema.sql missing unified-dev8 marker %q", required)
		}
	}
}

func TestRC36MigrationColumnsStayAlignedWithParser(t *testing.T) {
	cols := migrationCols()
	if !strings.Contains(cols, "COALESCE(adaptive_topology_drains,0)") {
		t.Fatalf("migrationCols missing adaptive_topology_drains: %s", cols)
	}
	row := make([][]byte, 73)
	row[0] = []byte("task-rc36")
	row[72] = []byte("7")
	m := parseMigration(row)
	if m.ID != "task-rc36" || m.AdaptiveTopologyDrains != 7 {
		t.Fatalf("migration parser columns misaligned: %+v", m)
	}
}

func TestRC36SchemaMarksTopologyRunningDrain(t *testing.T) {
	for _, required := range []string{"adaptive_topology_drains", "0.15.0-rc36"} {
		if !strings.Contains(schemaSQL, required) {
			t.Fatalf("schema.sql missing rc36 marker %q", required)
		}
	}
}

func TestRC37MigrationColumnsStayAlignedWithParser(t *testing.T) {
	cols := migrationCols()
	if !strings.Contains(cols, "COALESCE(adaptive_topology_degraded_yields,0)") {
		t.Fatalf("migrationCols missing adaptive_topology_degraded_yields: %s", cols)
	}
	row := make([][]byte, 74)
	row[0] = []byte("task-rc37")
	row[72] = []byte("7")
	row[73] = []byte("9")
	m := parseMigration(row)
	if m.ID != "task-rc37" || m.AdaptiveTopologyDrains != 7 || m.AdaptiveTopologyDegradedYields != 9 {
		t.Fatalf("migration parser columns misaligned: %+v", m)
	}
}

func TestRC37SchemaMarksDegradedTopologyConvergence(t *testing.T) {
	for _, required := range []string{"adaptive_topology_degraded_yields", "0.15.0-rc37"} {
		if !strings.Contains(schemaSQL, required) {
			t.Fatalf("schema.sql missing rc37 marker %q", required)
		}
	}
}

func TestRC38SchemaMarksTopologyRecoveryHysteresis(t *testing.T) {
	for _, required := range []string{"good_streak", "recovery_concurrency_cap", "0.15.0-rc38"} {
		if !strings.Contains(schemaSQL, required) {
			t.Fatalf("schema.sql missing rc38 marker %q", required)
		}
	}
}

func TestRC39ChunkFaultDomainColumnsStayAligned(t *testing.T) {
	cols := chunkCols()
	if !strings.Contains(cols, "fault_domain_json") {
		t.Fatalf("chunkCols missing fault_domain_json: %s", cols)
	}
	row := make([][]byte, 34)
	row[0] = []byte("chunk-rc39")
	row[26] = []byte(`{"ob_zone":"zone-a"}`)
	row[27] = []byte("ob-zone:zone-a")
	row[33] = []byte(`{"region":"sg","zone":"sg/zone-a"}`)
	c := parseChunk(row)
	parseChunkPlacement(row, &c)
	if c.ID != "chunk-rc39" || c.TopologyID != "ob-zone:zone-a" || c.FaultDomain["zone"] != "sg/zone-a" {
		t.Fatalf("chunk parser columns misaligned: %+v", c)
	}
	if !strings.Contains(chunkInsertSQL(c), "fault_domain_json") {
		t.Fatal("chunk INSERT missing fault_domain_json")
	}
}

func TestRC39SchemaMarksFaultDomainCascade(t *testing.T) {
	for _, required := range []string{"fault_domain_json", "0.15.0-rc39"} {
		if !strings.Contains(schemaSQL, required) {
			t.Fatalf("schema.sql missing rc39 marker %q", required)
		}
	}
}

func TestRC40MigrationFaultDomainYieldColumnsStayAligned(t *testing.T) {
	cols := migrationCols()
	if !strings.Contains(cols, "COALESCE(adaptive_fault_domain_yields,0)") {
		t.Fatalf("migrationCols missing adaptive_fault_domain_yields: %s", cols)
	}
	row := make([][]byte, 75)
	row[0] = []byte("task-rc40")
	row[72] = []byte("7")
	row[73] = []byte("9")
	row[74] = []byte("11")
	m := parseMigration(row)
	if m.ID != "task-rc40" || m.AdaptiveTopologyDrains != 7 || m.AdaptiveTopologyDegradedYields != 9 || m.AdaptiveFaultDomainYields != 11 {
		t.Fatalf("migration parser columns misaligned: %+v", m)
	}
}

func TestRC40SchemaMarksFaultDomainRunningConvergence(t *testing.T) {
	for _, required := range []string{"adaptive_fault_domain_yields", "0.15.0-rc40"} {
		if !strings.Contains(schemaSQL, required) {
			t.Fatalf("schema.sql missing rc40 marker %q", required)
		}
	}
}

func TestRC41MetadataPruneSQLIsBoundedAndCDCHeadSafe(t *testing.T) {
	policy := repository.MetadataRetentionPolicy{
		TaskLogMaxAge: 7 * 24 * time.Hour, TaskLogMaxRowsPerTask: 20000,
		AuditMaxAge: 90 * 24 * time.Hour, AuditMaxRows: 100000,
		CDCPositionMaxAge: 7 * 24 * time.Hour, CDCPositionMaxRowsPerStream: 4096,
	}
	now := time.Unix(1700000000, 0).UTC()
	logSQL := taskLogPruneSQL(policy, now, 5000)
	if !strings.Contains(logSQL, "PARTITION BY task_id") || !strings.Contains(logSQL, "LIMIT 5000") {
		t.Fatalf("task log prune is not per-task/bounded: %s", logSQL)
	}
	auditSQL := auditPruneSQL(policy, now, 5000)
	if !strings.Contains(auditSQL, "rn>100000") || !strings.Contains(auditSQL, "LIMIT 5000") {
		t.Fatalf("audit prune is not bounded: %s", auditSQL)
	}
	cdcSQL := cdcPositionPruneSQL(policy, now, 5000)
	for _, required := range []string{"PARTITION BY task_id,COALESCE(direction,'forward')", "rn>4096", "AND rn>1", "LIMIT 5000"} {
		if !strings.Contains(cdcSQL, required) {
			t.Fatalf("CDC prune missing %q: %s", required, cdcSQL)
		}
	}
}

func TestRC41ChunkSummarySQLAggregatesInPostgres(t *testing.T) {
	q := chunkSummarySQL("task'41")
	for _, required := range []string{"GROUP BY table_id", "FILTER (WHERE status='SUCCESS')", "sum(rows_written)", "task_id='task''41'"} {
		if !strings.Contains(q, required) {
			t.Fatalf("summary query missing %q: %s", required, q)
		}
	}
}

func TestRC41SchemaMarksMetadataRetention(t *testing.T) {
	for _, required := range []string{"idx_task_logs_task_time_id", "idx_audit_events_time_id", "idx_cdc_positions_task_direction_time_id", "0.15.0-rc41"} {
		if !strings.Contains(schemaSQL, required) {
			t.Fatalf("schema.sql missing rc41 marker %q", required)
		}
	}
}

func TestRC42SchemaMarksBoundedChunkHotPaths(t *testing.T) {
	for _, required := range []string{
		"idx_chunks_task_chunk_no_desc",
		"idx_chunks_pending_table_hot",
		"idx_chunks_running_topology_hot",
		"idx_chunks_running_fault_rack_hot",
		"idx_chunks_running_fault_zone_hot",
		"idx_chunks_running_fault_region_hot",
		"0.15.0-rc42",
	} {
		if !strings.Contains(schemaSQL, required) {
			t.Fatalf("schema.sql missing rc42 marker/index %q", required)
		}
	}
}

func TestRC42MetadataDeadRatio(t *testing.T) {
	stat := repository.MetadataRelationStats{LiveRows: 90, DeadRows: 10}
	if got := stat.DeadRatio(); got < 0.099 || got > 0.101 {
		t.Fatalf("unexpected dead ratio %.6f", got)
	}
}

func TestRC43SchemaMarksStreamingValidation(t *testing.T) {
	for _, required := range []string{"idx_chunks_validation_table_page", "idx_validation_task_chunk_latest", "idx_validation_task_status_chunk", "0.15.0-rc43"} {
		if !strings.Contains(schemaSQL, required) {
			t.Fatalf("schema.sql missing rc43 marker/index %q", required)
		}
	}
}

func TestRC44ValidationPruneSQLCompactsAttemptsAndBoundsTerminalHistory(t *testing.T) {
	policy := repository.MetadataRetentionPolicy{
		ValidationMaxAttemptsPerChunk: 1,
		ValidationAttemptMaxAge:       24 * time.Hour,
		ValidationTerminalMaxAge:      90 * 24 * time.Hour,
	}
	now := time.Unix(1700000000, 0).UTC()
	q := validationPruneSQL(policy, now, 5000)
	for _, required := range []string{
		"EXISTS(SELECT 1 FROM validation_results newer",
		"newer.task_id=v.task_id AND newer.chunk_id=v.chunk_id",
		"OFFSET 0 LIMIT 1",
		"m.status IN ('FINISHED','FAILED','CANCELLED','ROLLED_BACK')",
		"JOIN migration_tasks m ON m.id=v.task_id",
		"ORDER BY v.finished_at,v.id LIMIT 5000",
	} {
		if !strings.Contains(q, required) {
			t.Fatalf("validation prune missing %q: %s", required, q)
		}
	}
}

func TestRC44SchemaMarksValidationLifecycle(t *testing.T) {
	for _, required := range []string{"idx_validation_finished_id", "0.15.0-rc44"} {
		if !strings.Contains(schemaSQL, required) {
			t.Fatalf("schema.sql missing rc44 marker/index %q", required)
		}
	}
}

func TestRC45ValidationTerminalPruneRequiresImmutableArchive(t *testing.T) {
	policy := repository.MetadataRetentionPolicy{ValidationTerminalMaxAge: 90 * 24 * time.Hour}
	q := validationPruneSQL(policy, time.Unix(1700000000, 0).UTC(), 5000)
	for _, required := range []string{
		"EXISTS(SELECT 1 FROM validation_archives a WHERE a.task_id=v.task_id)",
		"m.status IN ('FINISHED','FAILED','CANCELLED','ROLLED_BACK')",
	} {
		if !strings.Contains(q, required) {
			t.Fatalf("rc45 terminal validation prune missing %q: %s", required, q)
		}
	}
}

func TestRC45SchemaMarksImmutableValidationArchive(t *testing.T) {
	for _, required := range []string{
		"CREATE TABLE IF NOT EXISTS validation_archives",
		"task_id text PRIMARY KEY",
		"evidence_digest text NOT NULL",
		"tables_json jsonb NOT NULL",
		"idx_validation_archives_archived_at",
		"0.15.0-rc45",
	} {
		if !strings.Contains(schemaSQL, required) {
			t.Fatalf("schema.sql missing rc45 archive marker %q", required)
		}
	}
}

func TestRC45ValidationEvidenceQueryIsPagedAndLatestAttemptOnly(t *testing.T) {
	cols := validationEvidenceCols()
	for _, required := range []string{"c.id", "v.source_checksum", "v.finished_at"} {
		if !strings.Contains(cols, required) {
			t.Fatalf("validation evidence columns missing %q: %s", required, cols)
		}
	}
	if !strings.Contains(validationArchiveSelect(), "tables_json") || !strings.Contains(validationArchiveSelect(), "archived_at") {
		t.Fatalf("validation archive select missing immutable payload: %s", validationArchiveSelect())
	}
}

func TestRC46SchemaMarksValidationReportDelivery(t *testing.T) {
	for _, required := range []string{"RC46 validation acceptance report delivery", "0.15.0-rc46"} {
		if !strings.Contains(schemaSQL, required) {
			t.Fatalf("schema.sql missing rc46 validation-report marker %q", required)
		}
	}
}

func TestRC47SchemaMarksPubliclyVerifiableReportRegistry(t *testing.T) {
	for _, required := range []string{
		"CREATE TABLE IF NOT EXISTS validation_report_archives",
		"PRIMARY KEY(task_id,evidence_digest)",
		"manifest_sha256 text NOT NULL",
		"public_key_fingerprint_sha256 text",
		"idx_validation_report_archives_committed_at",
		"0.15.0-rc49",
	} {
		if !strings.Contains(schemaSQL, required) {
			t.Fatalf("schema.sql missing rc47 registry marker %q", required)
		}
	}
	for _, required := range []string{"manifest_sha256", "public_key_ed25519", "object_lock_mode", "committed_at"} {
		if !strings.Contains(validationReportArchiveSelect(), required) {
			t.Fatalf("validation report archive select missing %q: %s", required, validationReportArchiveSelect())
		}
	}
}
