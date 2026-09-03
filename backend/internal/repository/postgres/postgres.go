package postgres

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"qmigration/backend/internal/connector/postgres"
	"qmigration/backend/internal/domain"
	"qmigration/backend/internal/repository"
	"sort"
	"strconv"
	"strings"
	"time"
)

//go:embed schema.sql
var schemaSQL string

type Store struct{ db *postgresconnector.Connector }

var _ repository.ControlOperationLeaser = (*Store)(nil)

func New(ctx context.Context, ds domain.DataSource, autoMigrate bool) (*Store, error) {
	raw, err := postgresconnector.NewFactory().New(ds)
	if err != nil {
		return nil, err
	}
	db, ok := raw.(*postgresconnector.Connector)
	if !ok {
		return nil, errors.New("invalid PostgreSQL connector")
	}
	s := &Store{db: db}
	if err := db.TestConnection(ctx); err != nil {
		db.Close()
		return nil, err
	}
	if autoMigrate {
		if err := applyMetadataSchema(ctx, db); err != nil {
			db.Close()
			return nil, fmt.Errorf("metadata schema: %w", err)
		}
	}
	return s, nil
}

type metadataSchemaExecutor interface {
	ExecSQL(context.Context, string) error
}

// applyMetadataSchema sends the embedded script as one PostgreSQL simple-query
// message. Splitting on semicolons is not SQL-aware: a semicolon in a comment,
// quoted value, or procedure body would turn the remainder into invalid SQL.
func applyMetadataSchema(ctx context.Context, db metadataSchemaExecutor) error {
	return db.ExecSQL(ctx, schemaSQL)
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) MetadataStorageStats(ctx context.Context) (repository.MetadataStorageStats, error) {
	// pg_stat_user_tables is approximate by design and cheap enough for Prometheus
	// scrapes. Size functions use relation OIDs so custom search_path settings do
	// not change which metadata tables are measured.
	q := `SELECT c.relname,pg_total_relation_size(c.oid),pg_relation_size(c.oid),pg_indexes_size(c.oid),COALESCE(st.n_live_tup,0),COALESCE(st.n_dead_tup,0)
FROM pg_class c
JOIN pg_namespace n ON n.oid=c.relnamespace
LEFT JOIN pg_stat_user_tables st ON st.relid=c.oid
WHERE c.relkind='r' AND n.nspname=current_schema() AND c.relname IN (
'migration_chunks','migration_tasks','migration_tables','task_logs','audit_events','cdc_positions','cdc_spool','validation_results','validation_archives','validation_report_archives','cdc_dead_letters','cdc_conflicts')
ORDER BY c.relname`
	r, err := s.db.QuerySQL(ctx, q)
	if err != nil {
		return repository.MetadataStorageStats{}, err
	}
	out := repository.MetadataStorageStats{Relations: make([]repository.MetadataRelationStats, 0, len(r.Rows))}
	for _, row := range r.Rows {
		item := repository.MetadataRelationStats{
			Relation: sv(row, 0), TotalBytes: iv(row, 1), TableBytes: iv(row, 2), IndexBytes: iv(row, 3),
			LiveRows: iv(row, 4), DeadRows: iv(row, 5),
		}
		out.TotalBytes += item.TotalBytes
		out.Relations = append(out.Relations, item)
	}
	return out, nil
}

// MetadataSchemaVersion returns the durable metadata schema marker written by
// the idempotent schema bootstrap. Readiness uses it to reject partially
// upgraded PostgreSQL metadata repositories.
func (s *Store) MetadataSchemaVersion(ctx context.Context) (string, error) {
	r, err := s.db.QuerySQL(ctx, "SELECT schema_version FROM metadata_schema_state WHERE id=1")
	if err != nil {
		return "", err
	}
	if len(r.Rows) == 0 || len(r.Rows[0]) == 0 {
		return "", errors.New("metadata schema version is not initialized")
	}
	return sv(r.Rows[0], 0), nil
}
func qs(v string) string { return "'" + strings.ReplaceAll(v, "'", "''") + "'" }
func qn(v string) string {
	if v == "" {
		return "NULL"
	}
	return qs(v)
}
func qb(v bool) string {
	if v {
		return "TRUE"
	}
	return "FALSE"
}
func qt(v time.Time) string {
	if v.IsZero() {
		return "NULL"
	}
	return qs(v.UTC().Format(time.RFC3339Nano))
}
func ji(v any) string { b, _ := json.Marshal(v); return qs(string(b)) + "::jsonb" }
func sv(row [][]byte, i int) string {
	if i >= len(row) || row[i] == nil {
		return ""
	}
	return string(row[i])
}
func iv(row [][]byte, i int) int64   { v, _ := strconv.ParseInt(sv(row, i), 10, 64); return v }
func fv(row [][]byte, i int) float64 { v, _ := strconv.ParseFloat(sv(row, i), 64); return v }
func bv(row [][]byte, i int) bool    { v := sv(row, i); return v == "t" || v == "true" || v == "1" }
func tv(row [][]byte, i int) time.Time {
	ms := iv(row, i)
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms)
}
func epoch(col string) string { return "COALESCE((extract(epoch from " + col + ")*1000)::bigint,0)" }

func (s *Store) CreateDataSource(ctx context.Context, d *domain.DataSource) error {
	return s.db.ExecSQL(ctx, fmt.Sprintf("INSERT INTO datasources(id,name,type,host,port,username,password_ciphertext,database_name,schema_name,jdbc_url,driver_class,cdc_url,tls_mode,tls_server_name,tls_ca_cert,tls_client_cert,tls_client_key_ciphertext,created_at,updated_at) VALUES(%s,%s,%s,%s,%d,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", qs(d.ID), qs(d.Name), qs(string(d.Type)), qs(d.Host), d.Port, qs(d.Username), qs(d.PasswordCiphertext), qn(d.Database), qn(d.Schema), qn(d.JDBCURL), qn(d.DriverClass), qn(d.CDCURL), qn(string(d.TLSMode)), qn(d.TLSServerName), qn(d.TLSCACert), qn(d.TLSClientCert), qn(d.TLSClientKeyCiphertext), qt(d.CreatedAt), qt(d.UpdatedAt)))
}
func (s *Store) UpdateDataSource(ctx context.Context, d *domain.DataSource) error {
	return s.db.ExecSQL(ctx, fmt.Sprintf("UPDATE datasources SET name=%s,type=%s,host=%s,port=%d,username=%s,password_ciphertext=%s,database_name=%s,schema_name=%s,jdbc_url=%s,driver_class=%s,cdc_url=%s,tls_mode=%s,tls_server_name=%s,tls_ca_cert=%s,tls_client_cert=%s,tls_client_key_ciphertext=%s,updated_at=%s WHERE id=%s", qs(d.Name), qs(string(d.Type)), qs(d.Host), d.Port, qs(d.Username), qs(d.PasswordCiphertext), qn(d.Database), qn(d.Schema), qn(d.JDBCURL), qn(d.DriverClass), qn(d.CDCURL), qn(string(d.TLSMode)), qn(d.TLSServerName), qn(d.TLSCACert), qn(d.TLSClientCert), qn(d.TLSClientKeyCiphertext), qt(d.UpdatedAt), qs(d.ID)))
}
func (s *Store) DeleteDataSource(ctx context.Context, id string) error {
	r, err := s.db.QuerySQL(ctx, "SELECT count(*) FROM migration_tasks WHERE source_datasource_id="+qs(id)+" OR target_datasource_id="+qs(id))
	if err != nil {
		return err
	}
	if len(r.Rows) > 0 && iv(r.Rows[0], 0) > 0 {
		return errors.New("datasource is referenced by migration")
	}
	return s.db.ExecSQL(ctx, "DELETE FROM datasources WHERE id="+qs(id))
}
func parseDS(row [][]byte) domain.DataSource {
	return domain.DataSource{ID: sv(row, 0), Name: sv(row, 1), Type: domain.DataSourceType(sv(row, 2)), Host: sv(row, 3), Port: int(iv(row, 4)), Username: sv(row, 5), PasswordCiphertext: sv(row, 6), Database: sv(row, 7), Schema: sv(row, 8), JDBCURL: sv(row, 9), DriverClass: sv(row, 10), CDCURL: sv(row, 11), TLSMode: domain.TLSMode(sv(row, 12)), TLSServerName: sv(row, 13), TLSCACert: sv(row, 14), TLSClientCert: sv(row, 15), TLSClientKeyCiphertext: sv(row, 16), CreatedAt: tv(row, 17), UpdatedAt: tv(row, 18)}
}
func datasourceCols() string {
	return "id,name,type,host,port,username,password_ciphertext,COALESCE(database_name,''),COALESCE(schema_name,''),COALESCE(jdbc_url,''),COALESCE(driver_class,''),COALESCE(cdc_url,''),COALESCE(tls_mode,''),COALESCE(tls_server_name,''),COALESCE(tls_ca_cert,''),COALESCE(tls_client_cert,''),COALESCE(tls_client_key_ciphertext,'')," + epoch("created_at") + "," + epoch("updated_at")
}

func (s *Store) ListDataSources(ctx context.Context) ([]domain.DataSource, error) {
	q := "SELECT " + datasourceCols() + " FROM datasources ORDER BY created_at"
	r, e := s.db.QuerySQL(ctx, q)
	if e != nil {
		return nil, e
	}
	out := make([]domain.DataSource, 0, len(r.Rows))
	for _, x := range r.Rows {
		out = append(out, parseDS(x))
	}
	return out, nil
}
func (s *Store) GetDataSource(ctx context.Context, id string) (*domain.DataSource, error) {
	r, e := s.db.QuerySQL(ctx, "SELECT "+datasourceCols()+" FROM datasources WHERE id="+qs(id))
	if e != nil {
		return nil, e
	}
	if len(r.Rows) == 0 {
		return nil, errors.New("not found")
	}
	v := parseDS(r.Rows[0])
	return &v, nil
}

func migrationCols() string {
	return "id,name,source_datasource_id,target_datasource_id,mode,status,full_engine,COALESCE(cdc_engine,''),table_mappings::text,chunk_rows,batch_rows,parallelism,max_retries,auto_create_table,validation_enabled,validation_mode,read_limit_mbps,write_limit_mbps,progress,total_chunks,finished_chunks,rows_migrated,bytes_migrated,speed_bytes_sec,cdc_lag_ms,cdc_start_timestamp_ms,COALESCE(cdc_start_position_type,''),COALESCE(cdc_start_position_value,''),COALESCE(cdc_start_resource,''),COALESCE(last_error,'')," + epoch("created_at") + "," + epoch("updated_at") + ",COALESCE(paused_from_status,''),COALESCE(post_load_ddl_mode,'INDEXES'),COALESCE(rollback_cdc_engine,''),COALESCE(cdc_ddl_mode,'REJECT'),COALESCE(cdc_conflict_mode,'SOURCE_WINS'),COALESCE(cdc_conflict_column,'')," + epoch("sequence_synced_at") + ",COALESCE(worker_selector_json,'{}'::jsonb)::text,COALESCE(worker_affinity,'PREFERRED'),COALESCE(NULLIF(effective_parallelism,0),parallelism),COALESCE(flow_control_level,'NORMAL'),COALESCE(flow_control_reason,''),rows_limit_per_sec,qps_limit,COALESCE(rate_limit_timezone,'Local'),COALESCE(rate_limit_windows,'[]'::jsonb)::text,speed_rows_sec,eta_seconds,COALESCE(transform_rules_json,'[]'::jsonb)::text,COALESCE(validation_barrier_position_type,''),COALESCE(validation_barrier_position_value,''),COALESCE(validation_barrier_resource,'')," + epoch("validation_barrier_captured_at") + ",COALESCE(cdc_spool_growth_bytes_sec,0),COALESCE(cdc_spool_critical_eta_seconds,0),COALESCE(target_throughput_mbps,0),COALESCE(auto_throughput_enabled,false),COALESCE(completion_sla_seconds,0)," + epoch("sla_started_at") + ",COALESCE(controller_target_bytes_sec,0),COALESCE(throughput_controller_reason,''),COALESCE(adaptive_hotspot_splits,0),COALESCE(adaptive_running_yields,0),COALESCE(controller_auto_probe_pct,0),COALESCE(controller_sla_headroom_pct,0),COALESCE(controller_learning_samples,0),COALESCE(sla_p95_eta_seconds,0),COALESCE(sla_p99_eta_seconds,0),COALESCE(sla_risk_level,''),COALESCE(sla_risk_reason,''),COALESCE(adaptive_topology_drains,0),COALESCE(adaptive_topology_degraded_yields,0),COALESCE(adaptive_fault_domain_yields,0)"
}
func parseMigration(row [][]byte) domain.MigrationTask {
	m := domain.MigrationTask{ID: sv(row, 0), Name: sv(row, 1), SourceID: sv(row, 2), TargetID: sv(row, 3), Mode: domain.MigrationMode(sv(row, 4)), Status: domain.MigrationStatus(sv(row, 5)), FullEngine: sv(row, 6), CDCEngine: sv(row, 7), ChunkRows: iv(row, 9), BatchRows: int(iv(row, 10)), Parallelism: int(iv(row, 11)), MaxRetries: int(iv(row, 12)), AutoCreateTable: bv(row, 13), ValidationEnabled: bv(row, 14), ValidationMode: sv(row, 15), ReadLimitMBps: iv(row, 16), WriteLimitMBps: iv(row, 17), Progress: fv(row, 18), TotalChunks: int(iv(row, 19)), FinishedChunks: int(iv(row, 20)), RowsMigrated: iv(row, 21), BytesMigrated: iv(row, 22), SpeedBytesSec: iv(row, 23), CDCLagMS: iv(row, 24), CDCStartTimestampMS: iv(row, 25), CDCStartPositionType: sv(row, 26), CDCStartPositionValue: sv(row, 27), CDCStartResource: sv(row, 28), LastError: sv(row, 29), CreatedAt: tv(row, 30), UpdatedAt: tv(row, 31), PausedFromStatus: domain.MigrationStatus(sv(row, 32)), PostLoadDDLMode: sv(row, 33), RollbackCDCEngine: sv(row, 34), CDCDDLMode: sv(row, 35), CDCConflictMode: sv(row, 36), CDCConflictColumn: sv(row, 37), SequenceSyncedAt: tv(row, 38), WorkerAffinity: sv(row, 40), EffectiveParallelism: int(iv(row, 41)), FlowControlLevel: sv(row, 42), FlowControlReason: sv(row, 43), RowsLimitPerSec: iv(row, 44), QPSLimit: int(iv(row, 45)), RateLimitTimezone: sv(row, 46), SpeedRowsSec: iv(row, 48), ETASeconds: iv(row, 49)}
	_ = json.Unmarshal([]byte(sv(row, 8)), &m.Tables)
	_ = json.Unmarshal([]byte(sv(row, 39)), &m.WorkerSelector)
	_ = json.Unmarshal([]byte(sv(row, 47)), &m.RateLimitWindows)
	_ = json.Unmarshal([]byte(sv(row, 50)), &m.TransformRules)
	m.ValidationBarrierPositionType = sv(row, 51)
	m.ValidationBarrierPositionValue = sv(row, 52)
	m.ValidationBarrierResource = sv(row, 53)
	m.ValidationBarrierCapturedAt = tv(row, 54)
	m.CDCSpoolGrowthBytesSec = iv(row, 55)
	m.CDCSpoolCriticalETASeconds = iv(row, 56)
	m.TargetThroughputMBps = iv(row, 57)
	m.AutoThroughputEnabled = bv(row, 58)
	m.CompletionSLASeconds = iv(row, 59)
	m.SLAStartedAt = tv(row, 60)
	m.ControllerTargetBytesSec = iv(row, 61)
	m.ThroughputControllerReason = sv(row, 62)
	m.AdaptiveHotspotSplits = iv(row, 63)
	m.AdaptiveRunningYields = iv(row, 64)
	m.ControllerAutoProbePct = int(iv(row, 65))
	m.ControllerSLAHeadroomPct = int(iv(row, 66))
	m.ControllerLearningSamples = iv(row, 67)
	m.SLAP95ETASeconds = iv(row, 68)
	m.SLAP99ETASeconds = iv(row, 69)
	m.SLARiskLevel = sv(row, 70)
	m.SLARiskReason = sv(row, 71)
	m.AdaptiveTopologyDrains = iv(row, 72)
	m.AdaptiveTopologyDegradedYields = iv(row, 73)
	m.AdaptiveFaultDomainYields = iv(row, 74)
	return m
}
func (s *Store) CreateMigration(ctx context.Context, m *domain.MigrationTask) error {
	q := fmt.Sprintf("INSERT INTO migration_tasks(id,name,source_datasource_id,target_datasource_id,mode,status,full_engine,cdc_engine,table_mappings,chunk_rows,batch_rows,parallelism,max_retries,auto_create_table,validation_enabled,validation_mode,read_limit_mbps,write_limit_mbps,progress,total_chunks,finished_chunks,rows_migrated,bytes_migrated,speed_bytes_sec,cdc_lag_ms,cdc_start_timestamp_ms,cdc_start_position_type,cdc_start_position_value,cdc_start_resource,last_error,created_at,updated_at,paused_from_status,post_load_ddl_mode,rollback_cdc_engine,cdc_ddl_mode,cdc_conflict_mode,cdc_conflict_column,sequence_synced_at,worker_selector_json,worker_affinity,effective_parallelism,flow_control_level,flow_control_reason,rows_limit_per_sec,qps_limit,rate_limit_timezone,rate_limit_windows,speed_rows_sec,eta_seconds,transform_rules_json,validation_barrier_position_type,validation_barrier_position_value,validation_barrier_resource,validation_barrier_captured_at,cdc_spool_growth_bytes_sec,cdc_spool_critical_eta_seconds,target_throughput_mbps,auto_throughput_enabled,completion_sla_seconds,sla_started_at,controller_target_bytes_sec,throughput_controller_reason,adaptive_hotspot_splits) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%d,%d,%d,%d,%s,%s,%s,%d,%d,%f,%d,%d,%d,%d,%d,%d,%d,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%d,%s,%s,%d,%d,%s,%s,%d,%d,%s,%s,%s,%s,%s,%d,%d,%d,%s,%d,%s,%d,%s,%d)", qs(m.ID), qs(m.Name), qs(m.SourceID), qs(m.TargetID), qs(string(m.Mode)), qs(string(m.Status)), qs(m.FullEngine), qn(m.CDCEngine), ji(m.Tables), m.ChunkRows, m.BatchRows, m.Parallelism, m.MaxRetries, qb(m.AutoCreateTable), qb(m.ValidationEnabled), qs(m.ValidationMode), m.ReadLimitMBps, m.WriteLimitMBps, m.Progress, m.TotalChunks, m.FinishedChunks, m.RowsMigrated, m.BytesMigrated, m.SpeedBytesSec, m.CDCLagMS, m.CDCStartTimestampMS, qn(m.CDCStartPositionType), qn(m.CDCStartPositionValue), qn(m.CDCStartResource), qn(m.LastError), qt(m.CreatedAt), qt(m.UpdatedAt), qn(string(m.PausedFromStatus)), qs(m.PostLoadDDLMode), qn(m.RollbackCDCEngine), qs(m.CDCDDLMode), qs(m.CDCConflictMode), qn(m.CDCConflictColumn), qt(m.SequenceSyncedAt), ji(m.WorkerSelector), qs(m.WorkerAffinity), m.EffectiveParallelism, qs(m.FlowControlLevel), qn(m.FlowControlReason), m.RowsLimitPerSec, m.QPSLimit, qs(m.RateLimitTimezone), ji(m.RateLimitWindows), m.SpeedRowsSec, m.ETASeconds, ji(m.TransformRules), qn(m.ValidationBarrierPositionType), qn(m.ValidationBarrierPositionValue), qn(m.ValidationBarrierResource), qt(m.ValidationBarrierCapturedAt), m.CDCSpoolGrowthBytesSec, m.CDCSpoolCriticalETASeconds, m.TargetThroughputMBps, qb(m.AutoThroughputEnabled), m.CompletionSLASeconds, qt(m.SLAStartedAt), m.ControllerTargetBytesSec, qn(m.ThroughputControllerReason), m.AdaptiveHotspotSplits)
	return s.db.ExecSQL(ctx, q)
}

func (s *Store) ListMigrations(ctx context.Context) ([]domain.MigrationTask, error) {
	r, e := s.db.QuerySQL(ctx, "SELECT "+migrationCols()+" FROM migration_tasks ORDER BY created_at DESC")
	if e != nil {
		return nil, e
	}
	out := make([]domain.MigrationTask, 0, len(r.Rows))
	for _, x := range r.Rows {
		out = append(out, parseMigration(x))
	}
	return out, nil
}
func (s *Store) GetMigration(ctx context.Context, id string) (*domain.MigrationTask, error) {
	r, e := s.db.QuerySQL(ctx, "SELECT "+migrationCols()+" FROM migration_tasks WHERE id="+qs(id))
	if e != nil {
		return nil, e
	}
	if len(r.Rows) == 0 {
		return nil, errors.New("not found")
	}
	v := parseMigration(r.Rows[0])
	return &v, nil
}
func (s *Store) UpdateMigration(ctx context.Context, m *domain.MigrationTask) error {
	q := fmt.Sprintf("UPDATE migration_tasks SET name=%s,mode=%s,status=%s,full_engine=%s,cdc_engine=%s,table_mappings=%s,chunk_rows=%d,batch_rows=%d,parallelism=%d,max_retries=%d,auto_create_table=%s,validation_enabled=%s,validation_mode=%s,read_limit_mbps=%d,write_limit_mbps=%d,target_throughput_mbps=%d,auto_throughput_enabled=%s,completion_sla_seconds=%d,sla_started_at=%s,controller_target_bytes_sec=%d,throughput_controller_reason=%s,adaptive_hotspot_splits=%d,adaptive_running_yields=%d,adaptive_topology_drains=%d,adaptive_topology_degraded_yields=%d,adaptive_fault_domain_yields=%d,controller_auto_probe_pct=%d,controller_sla_headroom_pct=%d,controller_learning_samples=%d,sla_p95_eta_seconds=%d,sla_p99_eta_seconds=%d,sla_risk_level=%s,sla_risk_reason=%s,worker_selector_json=%s,worker_affinity=%s,effective_parallelism=%d,flow_control_level=%s,flow_control_reason=%s,cdc_spool_growth_bytes_sec=%d,cdc_spool_critical_eta_seconds=%d,rows_limit_per_sec=%d,qps_limit=%d,rate_limit_timezone=%s,rate_limit_windows=%s,transform_rules_json=%s,speed_rows_sec=%d,eta_seconds=%d,progress=%f,total_chunks=%d,finished_chunks=%d,rows_migrated=%d,bytes_migrated=%d,speed_bytes_sec=%d,cdc_lag_ms=%d,cdc_start_timestamp_ms=%d,cdc_start_position_type=%s,cdc_start_position_value=%s,cdc_start_resource=%s,last_error=%s,paused_from_status=%s,post_load_ddl_mode=%s,rollback_cdc_engine=%s,cdc_ddl_mode=%s,cdc_conflict_mode=%s,cdc_conflict_column=%s,sequence_synced_at=%s,validation_barrier_position_type=%s,validation_barrier_position_value=%s,validation_barrier_resource=%s,validation_barrier_captured_at=%s,updated_at=%s WHERE id=%s", qs(m.Name), qs(string(m.Mode)), qs(string(m.Status)), qs(m.FullEngine), qn(m.CDCEngine), ji(m.Tables), m.ChunkRows, m.BatchRows, m.Parallelism, m.MaxRetries, qb(m.AutoCreateTable), qb(m.ValidationEnabled), qs(m.ValidationMode), m.ReadLimitMBps, m.WriteLimitMBps, m.TargetThroughputMBps, qb(m.AutoThroughputEnabled), m.CompletionSLASeconds, qt(m.SLAStartedAt), m.ControllerTargetBytesSec, qn(m.ThroughputControllerReason), m.AdaptiveHotspotSplits, m.AdaptiveRunningYields, m.AdaptiveTopologyDrains, m.AdaptiveTopologyDegradedYields, m.AdaptiveFaultDomainYields, m.ControllerAutoProbePct, m.ControllerSLAHeadroomPct, m.ControllerLearningSamples, m.SLAP95ETASeconds, m.SLAP99ETASeconds, qn(m.SLARiskLevel), qn(m.SLARiskReason), ji(m.WorkerSelector), qs(m.WorkerAffinity), m.EffectiveParallelism, qs(m.FlowControlLevel), qn(m.FlowControlReason), m.CDCSpoolGrowthBytesSec, m.CDCSpoolCriticalETASeconds, m.RowsLimitPerSec, m.QPSLimit, qs(m.RateLimitTimezone), ji(m.RateLimitWindows), ji(m.TransformRules), m.SpeedRowsSec, m.ETASeconds, m.Progress, m.TotalChunks, m.FinishedChunks, m.RowsMigrated, m.BytesMigrated, m.SpeedBytesSec, m.CDCLagMS, m.CDCStartTimestampMS, qn(m.CDCStartPositionType), qn(m.CDCStartPositionValue), qn(m.CDCStartResource), qn(m.LastError), qn(string(m.PausedFromStatus)), qs(m.PostLoadDDLMode), qn(m.RollbackCDCEngine), qs(m.CDCDDLMode), qs(m.CDCConflictMode), qn(m.CDCConflictColumn), qt(m.SequenceSyncedAt), qn(m.ValidationBarrierPositionType), qn(m.ValidationBarrierPositionValue), qn(m.ValidationBarrierResource), qt(m.ValidationBarrierCapturedAt), qt(m.UpdatedAt), qs(m.ID))
	return s.db.ExecSQL(ctx, q)
}

func controlLeaseSeconds(lease time.Duration) int64 {
	seconds := int64((lease + time.Second - 1) / time.Second)
	if seconds < 1 {
		return 1
	}
	return seconds
}

func (s *Store) AcquireControlOperation(ctx context.Context, taskID, operation, owner string, lease time.Duration) (bool, error) {
	q := fmt.Sprintf(`INSERT INTO control_operation_leases(task_id,operation,owner_id,lease_until,updated_at)
VALUES(%s,%s,%s,now()+make_interval(secs => %d),now())
ON CONFLICT(task_id) DO UPDATE SET operation=EXCLUDED.operation,owner_id=EXCLUDED.owner_id,lease_until=EXCLUDED.lease_until,updated_at=now()
WHERE control_operation_leases.lease_until <= now()
RETURNING task_id`, qs(taskID), qs(operation), qs(owner), controlLeaseSeconds(lease))
	r, err := s.db.QuerySQL(ctx, q)
	if err != nil {
		return false, err
	}
	return len(r.Rows) > 0, nil
}

func (s *Store) RenewControlOperation(ctx context.Context, taskID, operation, owner string, lease time.Duration) error {
	q := fmt.Sprintf(`UPDATE control_operation_leases SET lease_until=now()+make_interval(secs => %d),updated_at=now()
WHERE task_id=%s AND operation=%s AND owner_id=%s RETURNING task_id`, controlLeaseSeconds(lease), qs(taskID), qs(operation), qs(owner))
	r, err := s.db.QuerySQL(ctx, q)
	if err != nil {
		return err
	}
	if len(r.Rows) == 0 {
		return repository.ErrLeaseOwner
	}
	return nil
}

func (s *Store) ReleaseControlOperation(ctx context.Context, taskID, operation, owner string) error {
	q := fmt.Sprintf(`DELETE FROM control_operation_leases WHERE task_id=%s AND operation=%s AND owner_id=%s RETURNING task_id`, qs(taskID), qs(operation), qs(owner))
	r, err := s.db.QuerySQL(ctx, q)
	if err != nil {
		return err
	}
	if len(r.Rows) == 0 {
		return repository.ErrLeaseOwner
	}
	return nil
}

func tableCols() string {
	return "id,task_id,COALESCE(NULLIF(engine,''),'qmigration'),source_schema,source_table,target_schema,target_table,primary_key,COALESCE(target_primary_key,''),primary_key_type,columns_json::text,target_columns_json::text,estimated_rows,data_length,COALESCE(min_pk,0),COALESCE(max_pk,0),total_chunks,finished_chunks,rows_migrated,bytes_migrated,status,indexes_json::text,foreign_keys_json::text,COALESCE(topology_json,'[]'::jsonb)::text,COALESCE(partitions_json,'[]'::jsonb)::text,COALESCE(split_strategy,'AUTO'),COALESCE(custom_where,''),COALESCE(hash_buckets,0),COALESCE(profile_bytes_per_sec,0),COALESCE(profile_rows_per_sec,0),COALESCE(recommended_chunk_rows,0),COALESCE(performance_samples,0),COALESCE(topology_performance_json,'{}'::jsonb)::text"
}
func parseTable(r [][]byte) domain.MigrationTable {
	v := domain.MigrationTable{ID: sv(r, 0), TaskID: sv(r, 1), Engine: sv(r, 2), SourceSchema: sv(r, 3), SourceTable: sv(r, 4), TargetSchema: sv(r, 5), TargetTable: sv(r, 6), PrimaryKey: sv(r, 7), TargetPrimaryKey: sv(r, 8), PrimaryKeyType: sv(r, 9), EstimatedRows: iv(r, 12), DataLength: iv(r, 13), MinPK: iv(r, 14), MaxPK: iv(r, 15), TotalChunks: int(iv(r, 16)), FinishedChunks: int(iv(r, 17)), RowsMigrated: iv(r, 18), BytesMigrated: iv(r, 19), Status: sv(r, 20)}
	_ = json.Unmarshal([]byte(sv(r, 10)), &v.Columns)
	_ = json.Unmarshal([]byte(sv(r, 11)), &v.TargetColumns)
	_ = json.Unmarshal([]byte(sv(r, 21)), &v.Indexes)
	_ = json.Unmarshal([]byte(sv(r, 22)), &v.ForeignKeys)
	_ = json.Unmarshal([]byte(sv(r, 23)), &v.Topology)
	_ = json.Unmarshal([]byte(sv(r, 24)), &v.Partitions)
	v.SplitStrategy = sv(r, 25)
	v.CustomWhere = sv(r, 26)
	v.HashBuckets = int(iv(r, 27))
	v.ProfileBytesPerSec = iv(r, 28)
	v.ProfileRowsPerSec = iv(r, 29)
	v.RecommendedChunkRows = iv(r, 30)
	v.PerformanceSamples = iv(r, 31)
	_ = json.Unmarshal([]byte(sv(r, 32)), &v.TopologyPerformance)
	for _, col := range v.Columns {
		if col.PrimaryKey {
			v.PrimaryKeys = append(v.PrimaryKeys, col.Name)
		}
	}
	for _, col := range v.TargetColumns {
		if col.PrimaryKey {
			v.TargetPrimaryKeys = append(v.TargetPrimaryKeys, col.Name)
		}
	}
	if v.PrimaryKey == "" && len(v.PrimaryKeys) == 1 {
		v.PrimaryKey = v.PrimaryKeys[0]
	}
	if v.TargetPrimaryKey == "" && len(v.TargetPrimaryKeys) == 1 {
		v.TargetPrimaryKey = v.TargetPrimaryKeys[0]
	}
	return v
}
func (s *Store) CreateMigrationTable(ctx context.Context, t *domain.MigrationTable) error {
	if err := s.db.ExecSQL(ctx, fmt.Sprintf("INSERT INTO migration_tables(id,task_id,engine,source_schema,source_table,target_schema,target_table,primary_key,target_primary_key,primary_key_type,columns_json,target_columns_json,estimated_rows,data_length,min_pk,max_pk,total_chunks,finished_chunks,rows_migrated,bytes_migrated,status,indexes_json,foreign_keys_json,topology_json,partitions_json,split_strategy,custom_where,hash_buckets) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%d,%d,%d,%d,%d,%d,%d,%d,%s,%s,%s,%s,%s,%s,%s,%d)", qs(t.ID), qs(t.TaskID), qs(t.Engine), qs(t.SourceSchema), qs(t.SourceTable), qs(t.TargetSchema), qs(t.TargetTable), qs(t.PrimaryKey), qn(t.TargetPrimaryKey), qs(t.PrimaryKeyType), ji(t.Columns), ji(t.TargetColumns), t.EstimatedRows, t.DataLength, t.MinPK, t.MaxPK, t.TotalChunks, t.FinishedChunks, t.RowsMigrated, t.BytesMigrated, qs(t.Status), ji(t.Indexes), ji(t.ForeignKeys), ji(t.Topology), ji(t.Partitions), qs(t.SplitStrategy), qn(t.CustomWhere), t.HashBuckets)); err != nil {
		return err
	}
	return s.UpdateMigrationTable(ctx, t)
}
func (s *Store) ListMigrationTables(ctx context.Context, task string) ([]domain.MigrationTable, error) {
	r, e := s.db.QuerySQL(ctx, "SELECT "+tableCols()+" FROM migration_tables WHERE task_id="+qs(task)+" ORDER BY source_schema,source_table")
	if e != nil {
		return nil, e
	}
	out := []domain.MigrationTable{}
	for _, x := range r.Rows {
		out = append(out, parseTable(x))
	}
	return out, nil
}
func (s *Store) GetMigrationTable(ctx context.Context, id string) (*domain.MigrationTable, error) {
	r, e := s.db.QuerySQL(ctx, "SELECT "+tableCols()+" FROM migration_tables WHERE id="+qs(id))
	if e != nil {
		return nil, e
	}
	if len(r.Rows) == 0 {
		return nil, errors.New("not found")
	}
	v := parseTable(r.Rows[0])
	return &v, nil
}
func (s *Store) UpdateMigrationTable(ctx context.Context, t *domain.MigrationTable) error {
	return s.db.ExecSQL(ctx, fmt.Sprintf("UPDATE migration_tables SET engine=%s,target_primary_key=%s,target_columns_json=%s,indexes_json=%s,foreign_keys_json=%s,topology_json=%s,partitions_json=%s,split_strategy=%s,custom_where=%s,hash_buckets=%d,total_chunks=%d,finished_chunks=%d,rows_migrated=%d,bytes_migrated=%d,profile_bytes_per_sec=%d,profile_rows_per_sec=%d,recommended_chunk_rows=%d,performance_samples=%d,topology_performance_json=%s,status=%s WHERE id=%s", qs(t.Engine), qn(t.TargetPrimaryKey), ji(t.TargetColumns), ji(t.Indexes), ji(t.ForeignKeys), ji(t.Topology), ji(t.Partitions), qs(t.SplitStrategy), qn(t.CustomWhere), t.HashBuckets, t.TotalChunks, t.FinishedChunks, t.RowsMigrated, t.BytesMigrated, t.ProfileBytesPerSec, t.ProfileRowsPerSec, t.RecommendedChunkRows, t.PerformanceSamples, ji(t.TopologyPerformance), qs(t.Status), qs(t.ID)))
}

func (s *Store) FindMigrationTableProfile(ctx context.Context, sourceID, targetID, schema, table string) (*domain.MigrationTable, error) {
	q := "SELECT " + tableCols() + " FROM migration_tables WHERE id=(SELECT t.id FROM migration_tables t JOIN migration_tasks m ON m.id=t.task_id WHERE m.source_datasource_id=" + qs(sourceID) + " AND m.target_datasource_id=" + qs(targetID) + " AND lower(t.source_schema)=lower(" + qs(schema) + ") AND lower(t.source_table)=lower(" + qs(table) + ") AND COALESCE(t.performance_samples,0)>0 ORDER BY m.updated_at DESC,t.performance_samples DESC LIMIT 1)"
	r, e := s.db.QuerySQL(ctx, q)
	if e != nil {
		return nil, e
	}
	if len(r.Rows) == 0 {
		return nil, errors.New("not found")
	}
	v := parseTable(r.Rows[0])
	return &v, nil
}

func chunkCols() string {
	return "id,task_id,table_id,chunk_no,split_type,primary_key,start_value,end_value,COALESCE(start_cursor_json,''),COALESCE(end_cursor_json,''),COALESCE(cursor_json,''),status,COALESCE(worker_id,'')," + epoch("lease_until") + ",rows_read,rows_written,bytes_read,bytes_written,retry_count,COALESCE(last_error,''),last_read_ms,last_write_ms,last_batch_rows,COALESCE(backpressure_level,'')," + epoch("started_at") + "," + epoch("finished_at") + ",COALESCE(placement_hint_json,'{}'::jsonb)::text,COALESCE(topology_id,''),COALESCE(topology_kind,''),COALESCE(partition_name,''),COALESCE(hash_bucket,0),COALESCE(hash_buckets,0),COALESCE(custom_where,''),COALESCE(fault_domain_json,'{}'::jsonb)::text"
}
func parseChunk(r [][]byte) domain.MigrationChunk {
	return domain.MigrationChunk{ID: sv(r, 0), TaskID: sv(r, 1), TableID: sv(r, 2), ChunkNo: int(iv(r, 3)), SplitType: sv(r, 4), PrimaryKey: sv(r, 5), Start: iv(r, 6), End: iv(r, 7), StartCursorJSON: sv(r, 8), EndCursorJSON: sv(r, 9), CursorJSON: sv(r, 10), Status: domain.ChunkStatus(sv(r, 11)), WorkerID: sv(r, 12), LeaseUntil: tv(r, 13), RowsRead: iv(r, 14), RowsWritten: iv(r, 15), BytesRead: iv(r, 16), BytesWritten: iv(r, 17), RetryCount: int(iv(r, 18)), LastError: sv(r, 19), LastReadMS: iv(r, 20), LastWriteMS: iv(r, 21), LastBatchRows: int(iv(r, 22)), BackpressureLevel: sv(r, 23), StartedAt: tv(r, 24), FinishedAt: tv(r, 25), PlacementHint: map[string]string{}, TopologyID: sv(r, 27), TopologyKind: sv(r, 28), PartitionName: sv(r, 29), HashBucket: int(iv(r, 30)), HashBuckets: int(iv(r, 31)), CustomWhere: sv(r, 32), FaultDomain: map[string]string{}}
}
func parseChunkPlacement(r [][]byte, c *domain.MigrationChunk) {
	if c == nil {
		return
	}
	_ = json.Unmarshal([]byte(sv(r, 26)), &c.PlacementHint)
	_ = json.Unmarshal([]byte(sv(r, 33)), &c.FaultDomain)
}

func chunkInsertSQL(c domain.MigrationChunk) string {
	return fmt.Sprintf("INSERT INTO migration_chunks(id,task_id,table_id,chunk_no,split_type,primary_key,start_value,end_value,start_cursor_json,end_cursor_json,cursor_json,status,retry_count,placement_hint_json,topology_id,topology_kind,partition_name,hash_bucket,hash_buckets,custom_where,fault_domain_json) VALUES(%s,%s,%s,%d,%s,%s,%d,%d,%s,%s,%s,%s,%d,%s,%s,%s,%s,%d,%d,%s,%s)", qs(c.ID), qs(c.TaskID), qs(c.TableID), c.ChunkNo, qs(c.SplitType), qs(c.PrimaryKey), c.Start, c.End, qn(c.StartCursorJSON), qn(c.EndCursorJSON), qn(c.CursorJSON), qs(string(c.Status)), c.RetryCount, ji(c.PlacementHint), qn(c.TopologyID), qn(c.TopologyKind), qn(c.PartitionName), c.HashBucket, c.HashBuckets, qn(c.CustomWhere), ji(c.FaultDomain))
}
func (s *Store) CreateChunks(ctx context.Context, chunks []domain.MigrationChunk) error {
	for _, c := range chunks {
		if err := s.db.ExecSQL(ctx, chunkInsertSQL(c)); err != nil {
			return err
		}
	}
	return nil
}
func (s *Store) ListChunks(ctx context.Context, task string) ([]domain.MigrationChunk, error) {
	r, e := s.db.QuerySQL(ctx, "SELECT "+chunkCols()+" FROM migration_chunks WHERE task_id="+qs(task)+" ORDER BY table_id,chunk_no")
	if e != nil {
		return nil, e
	}
	out := []domain.MigrationChunk{}
	for _, x := range r.Rows {
		v := parseChunk(x)
		parseChunkPlacement(x, &v)
		out = append(out, v)
	}
	return out, nil
}

// RC42 hot-path chunk queries are index-backed and intentionally return only
// the small control-plane slice required by lease renewals/adaptive splitting.
func (s *Store) MaxTaskChunkNo(ctx context.Context, taskID string) (int, error) {
	r, err := s.db.QuerySQL(ctx, "SELECT COALESCE(MAX(chunk_no),0) FROM migration_chunks WHERE task_id="+qs(taskID))
	if err != nil {
		return 0, err
	}
	if len(r.Rows) == 0 {
		return 0, nil
	}
	return int(iv(r.Rows[0], 0)), nil
}

func (s *Store) CountTableRunnable(ctx context.Context, taskID, tableID string) (repository.TableRunnableCounts, error) {
	q := "SELECT count(*) FILTER (WHERE status='PENDING'),count(*) FILTER (WHERE status='RUNNING') FROM migration_chunks WHERE task_id=" + qs(taskID) + " AND table_id=" + qs(tableID) + " AND status IN ('PENDING','RUNNING')"
	r, err := s.db.QuerySQL(ctx, q)
	if err != nil {
		return repository.TableRunnableCounts{}, err
	}
	if len(r.Rows) == 0 {
		return repository.TableRunnableCounts{}, nil
	}
	return repository.TableRunnableCounts{Pending: int(iv(r.Rows[0], 0)), Running: int(iv(r.Rows[0], 1))}, nil
}

func (s *Store) queryHotChunks(ctx context.Context, where, order string) ([]domain.MigrationChunk, error) {
	q := "SELECT " + chunkCols() + " FROM migration_chunks WHERE " + where
	if strings.TrimSpace(order) != "" {
		q += " ORDER BY " + order
	}
	r, err := s.db.QuerySQL(ctx, q)
	if err != nil {
		return nil, err
	}
	out := make([]domain.MigrationChunk, 0, len(r.Rows))
	for _, row := range r.Rows {
		v := parseChunk(row)
		parseChunkPlacement(row, &v)
		out = append(out, v)
	}
	return out, nil
}

func (s *Store) ListPendingTableChunks(ctx context.Context, taskID, tableID string) ([]domain.MigrationChunk, error) {
	return s.queryHotChunks(ctx, "task_id="+qs(taskID)+" AND table_id="+qs(tableID)+" AND status='PENDING'", "chunk_no,id")
}

func (s *Store) ListRunningTopologyChunks(ctx context.Context, taskID, topologyID string) ([]domain.MigrationChunk, error) {
	return s.queryHotChunks(ctx, "task_id="+qs(taskID)+" AND status='RUNNING' AND COALESCE(topology_id,'')="+qs(topologyID), "started_at NULLS LAST,chunk_no,id")
}

func (s *Store) ListRunningFaultDomainChunks(ctx context.Context, taskID, scope, value string) ([]domain.MigrationChunk, error) {
	var key string
	switch strings.ToLower(strings.TrimSpace(scope)) {
	case "rack":
		key = "rack"
	case "zone":
		key = "zone"
	case "region":
		key = "region"
	default:
		return nil, fmt.Errorf("unsupported fault-domain scope %q", scope)
	}
	where := "task_id=" + qs(taskID) + " AND status='RUNNING' AND COALESCE(fault_domain_json->>" + qs(key) + ",'')=" + qs(value)
	return s.queryHotChunks(ctx, where, "started_at NULLS LAST,chunk_no,id")
}

func (s *Store) GetChunk(ctx context.Context, id string) (*domain.MigrationChunk, error) {
	r, e := s.db.QuerySQL(ctx, "SELECT "+chunkCols()+" FROM migration_chunks WHERE id="+qs(id))
	if e != nil {
		return nil, e
	}
	if len(r.Rows) == 0 {
		return nil, errors.New("not found")
	}
	v := parseChunk(r.Rows[0])
	parseChunkPlacement(r.Rows[0], &v)
	return &v, nil
}
func (s *Store) UpdateChunk(ctx context.Context, c *domain.MigrationChunk) error {
	q := fmt.Sprintf("UPDATE migration_chunks SET chunk_no=%d,split_type=%s,primary_key=%s,start_value=%d,end_value=%d,start_cursor_json=%s,end_cursor_json=%s,cursor_json=%s,status=%s,worker_id=%s,lease_until=%s,rows_read=%d,rows_written=%d,bytes_read=%d,bytes_written=%d,retry_count=%d,last_error=%s,last_read_ms=%d,last_write_ms=%d,last_batch_rows=%d,backpressure_level=%s,placement_hint_json=%s,topology_id=%s,topology_kind=%s,partition_name=%s,hash_bucket=%d,hash_buckets=%d,custom_where=%s,fault_domain_json=%s,started_at=%s,finished_at=%s WHERE id=%s", c.ChunkNo, qs(c.SplitType), qs(c.PrimaryKey), c.Start, c.End, qn(c.StartCursorJSON), qn(c.EndCursorJSON), qn(c.CursorJSON), qs(string(c.Status)), qn(c.WorkerID), qt(c.LeaseUntil), c.RowsRead, c.RowsWritten, c.BytesRead, c.BytesWritten, c.RetryCount, qn(c.LastError), c.LastReadMS, c.LastWriteMS, c.LastBatchRows, qn(c.BackpressureLevel), ji(c.PlacementHint), qn(c.TopologyID), qn(c.TopologyKind), qn(c.PartitionName), c.HashBucket, c.HashBuckets, qn(c.CustomWhere), ji(c.FaultDomain), qt(c.StartedAt), qt(c.FinishedAt), qs(c.ID))
	return s.db.ExecSQL(ctx, q)
}

func (s *Store) ClaimChunk(ctx context.Context, worker string, lease time.Duration, capabilities []string) (*domain.MigrationChunk, error) {
	_ = s.db.ExecSQL(ctx, "UPDATE migration_chunks SET status='PENDING',worker_id=NULL,lease_until=NULL,retry_count=retry_count+1,last_error='worker lease expired' WHERE status='RUNNING' AND lease_until IS NOT NULL AND lease_until<now()")
	seconds := int(lease.Seconds())
	if seconds < 1 {
		seconds = 300
	}
	if len(capabilities) == 0 {
		return nil, repository.ErrNoChunk
	}
	quoted := make([]string, 0, len(capabilities)+1)
	hasLegacyNative := false
	for _, c := range capabilities {
		quoted = append(quoted, qs(c))
		if c == "native" {
			hasLegacyNative = true
		}
	}
	if hasLegacyNative {
		quoted = append(quoted, qs("qmigration"))
	}
	engineFilter := "COALESCE(NULLIF(t.engine,''),NULLIF(m.full_engine,''),'qmigration') IN (" + strings.Join(quoted, ",") + ")"
	selector := "COALESCE(m.worker_selector_json,'{}'::jsonb)"
	workerLabels := "COALESCE(w.labels,'{}'::jsonb)"
	labelMatch := workerLabels + " @> " + selector
	affinityAllowed := "(UPPER(COALESCE(m.worker_affinity,'PREFERRED'))<>'REQUIRED' OR " + labelMatch + ")"
	affinityRank := "CASE WHEN " + selector + "='{}'::jsonb THEN 1 WHEN " + labelMatch + " THEN 0 ELSE 2 END"
	placement := "COALESCE(c.placement_hint_json,'{}'::jsonb)"
	placementRank := "CASE WHEN " + placement + "='{}'::jsonb THEN 1 WHEN " + workerLabels + " @> " + placement + " THEN 0 ELSE 2 END"
	topologyHealth := "UPPER(COALESCE(t.topology_performance_json->COALESCE(c.topology_id,'')->>'health','HEALTHY'))"
	topologyRunning := "(CASE WHEN COALESCE(c.topology_id,'')='' THEN 0 ELSE (SELECT count(*) FROM migration_chunks hr WHERE hr.task_id=c.task_id AND hr.status='RUNNING' AND hr.topology_id=c.topology_id) END)"
	healthyCap := repository.TopologyHealthyMaxConcurrency()
	degradedCap := repository.TopologyDegradedMaxConcurrency()
	recoveryMaxCap := repository.TopologyRecoveryMaxConcurrency()
	topologyRecoveryCap := "GREATEST(" + strconv.Itoa(degradedCap) + ",LEAST(" + strconv.Itoa(recoveryMaxCap) + ",COALESCE((t.topology_performance_json->COALESCE(c.topology_id,'')->>'recovery_concurrency_cap')::int,0)))"
	topologyAllowed := "(COALESCE(c.topology_id,'')='' OR CASE " + topologyHealth + " WHEN 'CIRCUIT_OPEN' THEN false WHEN 'HALF_OPEN' THEN " + topologyRunning + " < 1 WHEN 'DEGRADED' THEN " + topologyRunning + " < " + topologyRecoveryCap + " ELSE " + func() string {
		if healthyCap <= 0 {
			return "true"
		}
		return topologyRunning + " < " + strconv.Itoa(healthyCap)
	}() + " END)"
	topologyHealthRank := "CASE " + topologyHealth + " WHEN 'HEALTHY' THEN 0 WHEN 'DEGRADED' THEN 1 WHEN 'HALF_OPEN' THEN 2 WHEN 'CIRCUIT_OPEN' THEN 3 ELSE 0 END"
	faultDomainPeerRisk := "0"
	faultDomainRunning := "0"
	faultDomainAllowed := "true"
	if repository.FaultDomainProtectionEnabled() {
		region := "COALESCE(c.fault_domain_json->>'region','')"
		zone := "COALESCE(c.fault_domain_json->>'zone','')"
		rack := "COALESCE(c.fault_domain_json->>'rack','')"
		peerHealth := "UPPER(COALESCE(ft.topology_performance_json->COALESCE(fc.topology_id,'')->>'health','HEALTHY'))"
		peerRank := "CASE " + peerHealth + " WHEN 'DEGRADED' THEN 1 WHEN 'HALF_OPEN' THEN 2 WHEN 'CIRCUIT_OPEN' THEN 3 ELSE 0 END"
		peerBase := " FROM migration_chunks fc JOIN migration_tables ft ON ft.id=fc.table_id WHERE fc.task_id=c.task_id AND COALESCE(fc.topology_id,'')<>COALESCE(c.topology_id,'')"
		rackRisk := "CASE WHEN " + rack + "='' THEN 0 ELSE COALESCE((SELECT max(" + peerRank + ")" + peerBase + " AND COALESCE(fc.fault_domain_json->>'rack','')=" + rack + "),0) END"
		zoneRisk := "CASE WHEN " + zone + "='' THEN 0 ELSE COALESCE((SELECT max(" + peerRank + ")" + peerBase + " AND COALESCE(fc.fault_domain_json->>'zone','')=" + zone + "),0) END"
		regionRawRisk := "CASE WHEN " + region + "='' THEN 0 ELSE COALESCE((SELECT max(" + peerRank + ")" + peerBase + " AND COALESCE(fc.fault_domain_json->>'region','')=" + region + "),0) END"
		regionUnhealthyZones := "CASE WHEN " + region + "='' THEN 0 ELSE (SELECT count(DISTINCT COALESCE(NULLIF(fc.fault_domain_json->>'zone',''),'topology:'||COALESCE(fc.topology_id,'')))" + peerBase + " AND COALESCE(fc.fault_domain_json->>'region','')=" + region + " AND (" + peerRank + ")>0) END"
		regionRisk := "CASE WHEN " + regionUnhealthyZones + ">=" + strconv.Itoa(repository.FaultDomainRegionMinUnhealthyZones()) + " THEN " + regionRawRisk + " ELSE 0 END"
		faultDomainPeerRisk = "GREATEST(" + rackRisk + "," + zoneRisk + "," + regionRisk + ")"
		rackRunning := "(SELECT count(*) FROM migration_chunks fc WHERE fc.task_id=c.task_id AND fc.status='RUNNING' AND " + rack + "<>'' AND COALESCE(fc.fault_domain_json->>'rack','')=" + rack + ")"
		zoneRunning := "(SELECT count(*) FROM migration_chunks fc WHERE fc.task_id=c.task_id AND fc.status='RUNNING' AND " + zone + "<>'' AND COALESCE(fc.fault_domain_json->>'zone','')=" + zone + ")"
		regionRunning := "(SELECT count(*) FROM migration_chunks fc WHERE fc.task_id=c.task_id AND fc.status='RUNNING' AND " + region + "<>'' AND COALESCE(fc.fault_domain_json->>'region','')=" + region + ")"
		faultDomainRunning = "(CASE WHEN " + regionRisk + ">0 THEN " + regionRunning + " WHEN " + zoneRisk + ">0 THEN " + zoneRunning + " WHEN " + rackRisk + ">0 THEN " + rackRunning + " ELSE 0 END)"
		faultDomainAllowed = "(CASE WHEN " + faultDomainPeerRisk + ">=2 THEN " + faultDomainRunning + " < " + strconv.Itoa(repository.FaultDomainCriticalMaxConcurrency()) + " WHEN " + faultDomainPeerRisk + "=1 THEN " + faultDomainRunning + " < " + strconv.Itoa(repository.FaultDomainDegradedMaxConcurrency()) + " ELSE true END)"
	}
	q := `WITH candidate AS (SELECT c.id FROM migration_chunks c JOIN migration_tasks m ON m.id=c.task_id JOIN migration_tables t ON t.id=c.table_id JOIN workers w ON w.id=` + qs(worker) + ` WHERE c.status='PENDING' AND m.status='FULL_MIGRATING' AND ` + engineFilter + ` AND ` + affinityAllowed + ` AND ` + topologyAllowed + ` AND ` + faultDomainAllowed + ` AND (SELECT count(*) FROM migration_chunks r WHERE r.task_id=c.task_id AND r.status='RUNNING') < COALESCE(NULLIF(m.effective_parallelism,0),m.parallelism) ORDER BY ` + affinityRank + `,` + topologyHealthRank + `,` + faultDomainPeerRisk + `,` + faultDomainRunning + `,` + placementRank + `,` + topologyRunning + `,(SELECT count(*) FROM migration_chunks tr WHERE tr.table_id=c.table_id AND tr.status='RUNNING'),c.task_id,c.table_id,c.chunk_no FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE migration_chunks c SET status='RUNNING',worker_id=` + qs(worker) + `,lease_until=now()+interval '` + strconv.Itoa(seconds) + ` seconds',started_at=COALESCE(started_at,now()),last_error=NULL FROM candidate x WHERE c.id=x.id RETURNING ` + chunkCols()
	r, e := s.db.QuerySQL(ctx, q)
	if e != nil {
		return nil, e
	}
	if len(r.Rows) == 0 {
		return nil, repository.ErrNoChunk
	}
	v := parseChunk(r.Rows[0])
	parseChunkPlacement(r.Rows[0], &v)
	return &v, nil
}
func (s *Store) RenewChunkLease(ctx context.Context, id, worker string, lease time.Duration) error {
	sec := int(lease.Seconds())
	q := "UPDATE migration_chunks SET lease_until=now()+interval '" + strconv.Itoa(sec) + " seconds' WHERE id=" + qs(id) + " AND worker_id=" + qs(worker) + " AND status='RUNNING' RETURNING id"
	r, e := s.db.QuerySQL(ctx, q)
	if e != nil {
		return e
	}
	if len(r.Rows) == 0 {
		return repository.ErrLeaseOwner
	}
	return nil
}

func (s *Store) UpdateChunkProgress(ctx context.Context, id, worker string, progress domain.ChunkProgress) error {
	q := fmt.Sprintf("UPDATE migration_chunks SET cursor_json=%s,rows_read=%d,rows_written=%d,bytes_read=%d,bytes_written=%d,last_read_ms=%d,last_write_ms=%d,last_batch_rows=%d,backpressure_level=%s WHERE id=%s AND worker_id=%s AND status='RUNNING' RETURNING id", qn(progress.CursorJSON), progress.RowsRead, progress.RowsWritten, progress.BytesRead, progress.BytesWritten, progress.LastReadMS, progress.LastWriteMS, progress.LastBatchRows, qn(progress.BackpressureLevel), qs(id), qs(worker))
	r, e := s.db.QuerySQL(ctx, q)
	if e != nil {
		return e
	}
	if len(r.Rows) == 0 {
		return repository.ErrLeaseOwner
	}
	return nil
}

func (s *Store) YieldChunk(ctx context.Context, worker string, completed *domain.MigrationChunk, created []domain.MigrationChunk) error {
	if completed == nil {
		return errors.New("yielded chunk is nil")
	}
	update := fmt.Sprintf("UPDATE migration_chunks SET chunk_no=%d,split_type=%s,primary_key=%s,start_value=%d,end_value=%d,start_cursor_json=%s,end_cursor_json=%s,cursor_json=%s,status=%s,worker_id=%s,lease_until=%s,rows_read=%d,rows_written=%d,bytes_read=%d,bytes_written=%d,retry_count=%d,last_error=%s,last_read_ms=%d,last_write_ms=%d,last_batch_rows=%d,backpressure_level=%s,placement_hint_json=%s,topology_id=%s,topology_kind=%s,partition_name=%s,hash_bucket=%d,hash_buckets=%d,custom_where=%s,fault_domain_json=%s,started_at=%s,finished_at=%s WHERE id=%s", completed.ChunkNo, qs(completed.SplitType), qs(completed.PrimaryKey), completed.Start, completed.End, qn(completed.StartCursorJSON), qn(completed.EndCursorJSON), qn(completed.CursorJSON), qs(string(completed.Status)), qn(completed.WorkerID), qt(completed.LeaseUntil), completed.RowsRead, completed.RowsWritten, completed.BytesRead, completed.BytesWritten, completed.RetryCount, qn(completed.LastError), completed.LastReadMS, completed.LastWriteMS, completed.LastBatchRows, qn(completed.BackpressureLevel), ji(completed.PlacementHint), qn(completed.TopologyID), qn(completed.TopologyKind), qn(completed.PartitionName), completed.HashBucket, completed.HashBuckets, qn(completed.CustomWhere), ji(completed.FaultDomain), qt(completed.StartedAt), qt(completed.FinishedAt), qs(completed.ID))
	var b strings.Builder
	b.WriteString("DO $qmig$ BEGIN ")
	b.WriteString("IF NOT EXISTS (SELECT 1 FROM migration_chunks WHERE id=" + qs(completed.ID) + " AND worker_id=" + qs(worker) + " AND status='RUNNING') THEN RAISE EXCEPTION 'chunk lease owner mismatch'; END IF; ")
	b.WriteString(update + "; ")
	for _, ch := range created {
		b.WriteString(chunkInsertSQL(ch) + "; ")
	}
	b.WriteString("END $qmig$;")
	return s.db.ExecSQL(ctx, b.String())
}

func engineJobCols() string {
	return "id,task_id,kind,direction,engine,status,COALESCE(worker_id,'')," + epoch("lease_until") + ",retry_count,COALESCE(last_error,'')," + epoch("started_at") + "," + epoch("updated_at") + "," + epoch("finished_at")
}
func parseEngineJob(r [][]byte) domain.EngineJob {
	return domain.EngineJob{ID: sv(r, 0), TaskID: sv(r, 1), Kind: sv(r, 2), Direction: sv(r, 3), Engine: sv(r, 4), Status: domain.EngineJobStatus(sv(r, 5)), WorkerID: sv(r, 6), LeaseUntil: tv(r, 7), RetryCount: int(iv(r, 8)), LastError: sv(r, 9), StartedAt: tv(r, 10), UpdatedAt: tv(r, 11), FinishedAt: tv(r, 12)}
}
func (s *Store) CreateEngineJob(ctx context.Context, j *domain.EngineJob) error {
	return s.db.ExecSQL(ctx, fmt.Sprintf("INSERT INTO engine_jobs(id,task_id,kind,direction,engine,status,worker_id,lease_until,retry_count,last_error,started_at,updated_at,finished_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%d,%s,%s,%s,%s)", qs(j.ID), qs(j.TaskID), qs(j.Kind), qs(j.Direction), qs(j.Engine), qs(string(j.Status)), qn(j.WorkerID), qt(j.LeaseUntil), j.RetryCount, qn(j.LastError), qt(j.StartedAt), qt(j.UpdatedAt), qt(j.FinishedAt)))
}
func (s *Store) GetEngineJob(ctx context.Context, id string) (*domain.EngineJob, error) {
	r, e := s.db.QuerySQL(ctx, "SELECT "+engineJobCols()+" FROM engine_jobs WHERE id="+qs(id))
	if e != nil {
		return nil, e
	}
	if len(r.Rows) == 0 {
		return nil, errors.New("not found")
	}
	v := parseEngineJob(r.Rows[0])
	return &v, nil
}
func (s *Store) ListEngineJobs(ctx context.Context, taskID string) ([]domain.EngineJob, error) {
	where := ""
	if taskID != "" {
		where = " WHERE task_id=" + qs(taskID)
	}
	r, e := s.db.QuerySQL(ctx, "SELECT "+engineJobCols()+" FROM engine_jobs"+where+" ORDER BY updated_at DESC")
	if e != nil {
		return nil, e
	}
	out := []domain.EngineJob{}
	for _, x := range r.Rows {
		out = append(out, parseEngineJob(x))
	}
	return out, nil
}
func (s *Store) UpdateEngineJob(ctx context.Context, j *domain.EngineJob) error {
	q := fmt.Sprintf("UPDATE engine_jobs SET kind=%s,direction=%s,engine=%s,status=%s,worker_id=%s,lease_until=%s,retry_count=%d,last_error=%s,started_at=%s,updated_at=%s,finished_at=%s WHERE id=%s", qs(j.Kind), qs(j.Direction), qs(j.Engine), qs(string(j.Status)), qn(j.WorkerID), qt(j.LeaseUntil), j.RetryCount, qn(j.LastError), qt(j.StartedAt), qt(j.UpdatedAt), qt(j.FinishedAt), qs(j.ID))
	return s.db.ExecSQL(ctx, q)
}
func (s *Store) ClaimEngineJob(ctx context.Context, worker string, lease time.Duration, capabilities []string) (*domain.EngineJob, error) {
	_ = s.db.ExecSQL(ctx, "UPDATE engine_jobs SET status='PENDING',worker_id=NULL,lease_until=NULL,retry_count=retry_count+1,last_error='worker lease expired',updated_at=now() WHERE status='RUNNING' AND lease_until IS NOT NULL AND lease_until<now()")
	if len(capabilities) == 0 {
		return nil, repository.ErrNoChunk
	}
	quoted := make([]string, 0, len(capabilities)+1)
	legacyUnified := false
	for _, c := range capabilities {
		quoted = append(quoted, qs(c))
		if c == "native" || c == "native-mysql-cdc" || c == "native-postgres-cdc" {
			legacyUnified = true
		}
	}
	if legacyUnified {
		quoted = append(quoted, qs("qmigration"))
	}
	sec := int(lease.Seconds())
	if sec < 1 {
		sec = 300
	}
	q := `WITH candidate AS (SELECT id FROM engine_jobs WHERE status='PENDING' AND engine IN (` + strings.Join(quoted, ",") + `) ORDER BY updated_at,id FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE engine_jobs j SET status='RUNNING',worker_id=` + qs(worker) + `,lease_until=now()+interval '` + strconv.Itoa(sec) + ` seconds',started_at=COALESCE(started_at,now()),updated_at=now(),last_error=NULL FROM candidate x WHERE j.id=x.id RETURNING ` + engineJobCols()
	r, e := s.db.QuerySQL(ctx, q)
	if e != nil {
		return nil, e
	}
	if len(r.Rows) == 0 {
		return nil, repository.ErrNoChunk
	}
	v := parseEngineJob(r.Rows[0])
	return &v, nil
}
func (s *Store) RenewEngineJobLease(ctx context.Context, id, worker string, lease time.Duration) error {
	sec := int(lease.Seconds())
	if sec < 1 {
		sec = 300
	}
	r, e := s.db.QuerySQL(ctx, "UPDATE engine_jobs SET lease_until=now()+interval '"+strconv.Itoa(sec)+" seconds',updated_at=now() WHERE id="+qs(id)+" AND worker_id="+qs(worker)+" AND status='RUNNING' RETURNING id")
	if e != nil {
		return e
	}
	if len(r.Rows) == 0 {
		return repository.ErrLeaseOwner
	}
	return nil
}

func workerCols() string {
	return "id,hostname,cpu,memory_mb,status,running_jobs,capabilities::text," + epoch("last_heartbeat") + ",cpu_usage_pct,memory_usage_pct,network_rx_bps,network_tx_bps,COALESCE(labels,'{}'::jsonb)::text"
}
func parseWorker(r [][]byte) domain.Worker {
	v := domain.Worker{ID: sv(r, 0), Hostname: sv(r, 1), CPU: int(iv(r, 2)), MemoryMB: int(iv(r, 3)), Status: sv(r, 4), RunningJobs: int(iv(r, 5)), LastHeartbeat: tv(r, 7), CPUUsagePct: fv(r, 8), MemoryUsagePct: fv(r, 9), NetworkRxBps: iv(r, 10), NetworkTxBps: iv(r, 11)}
	_ = json.Unmarshal([]byte(sv(r, 6)), &v.Capabilities)
	_ = json.Unmarshal([]byte(sv(r, 12)), &v.Labels)
	return v
}
func (s *Store) UpsertWorker(ctx context.Context, w *domain.Worker) error {
	return s.db.ExecSQL(ctx, fmt.Sprintf("INSERT INTO workers(id,hostname,cpu,memory_mb,status,running_jobs,capabilities,last_heartbeat,cpu_usage_pct,memory_usage_pct,network_rx_bps,network_tx_bps,labels) VALUES(%s,%s,%d,%d,%s,%d,%s,%s,%f,%f,%d,%d,%s) ON CONFLICT(id) DO UPDATE SET hostname=EXCLUDED.hostname,cpu=EXCLUDED.cpu,memory_mb=EXCLUDED.memory_mb,status=EXCLUDED.status,running_jobs=EXCLUDED.running_jobs,capabilities=EXCLUDED.capabilities,last_heartbeat=EXCLUDED.last_heartbeat,cpu_usage_pct=EXCLUDED.cpu_usage_pct,memory_usage_pct=EXCLUDED.memory_usage_pct,network_rx_bps=EXCLUDED.network_rx_bps,network_tx_bps=EXCLUDED.network_tx_bps,labels=EXCLUDED.labels", qs(w.ID), qs(w.Hostname), w.CPU, w.MemoryMB, qs(w.Status), w.RunningJobs, ji(w.Capabilities), qt(w.LastHeartbeat), w.CPUUsagePct, w.MemoryUsagePct, w.NetworkRxBps, w.NetworkTxBps, ji(w.Labels)))
}

func (s *Store) ListWorkers(ctx context.Context) ([]domain.Worker, error) {
	r, e := s.db.QuerySQL(ctx, "SELECT "+workerCols()+" FROM workers ORDER BY hostname")
	if e != nil {
		return nil, e
	}
	out := []domain.Worker{}
	for _, x := range r.Rows {
		out = append(out, parseWorker(x))
	}
	return out, nil
}
func (s *Store) GetWorker(ctx context.Context, id string) (*domain.Worker, error) {
	r, e := s.db.QuerySQL(ctx, "SELECT "+workerCols()+" FROM workers WHERE id="+qs(id))
	if e != nil {
		return nil, e
	}
	if len(r.Rows) == 0 {
		return nil, errors.New("not found")
	}
	v := parseWorker(r.Rows[0])
	return &v, nil
}

func (s *Store) CreateCDCPosition(ctx context.Context, p *domain.CDCPosition) error {
	return s.db.ExecSQL(ctx, fmt.Sprintf("INSERT INTO cdc_positions(id,task_id,direction,database_type,position_type,position_value,resource,source_timestamp_ms,apply_timestamp_ms,lag_ms,events_total,events_pending,recorded_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%d,%d,%d,%d,%d,%s)", qs(p.ID), qs(p.TaskID), qs(p.Direction), qn(p.DatabaseType), qn(p.PositionType), qn(p.PositionValue), qn(p.Resource), p.SourceTimestampMS, p.ApplyTimestampMS, p.LagMS, p.EventsTotal, p.EventsPending, qt(p.RecordedAt)))
}
func (s *Store) ListCDCPositions(ctx context.Context, task string, limit int) ([]domain.CDCPosition, error) {
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	r, e := s.db.QuerySQL(ctx, fmt.Sprintf("SELECT id,task_id,COALESCE(direction,'forward'),COALESCE(database_type,''),COALESCE(position_type,''),COALESCE(position_value,''),COALESCE(resource,''),source_timestamp_ms,apply_timestamp_ms,lag_ms,events_total,events_pending,%s FROM cdc_positions WHERE task_id=%s ORDER BY recorded_at DESC LIMIT %d", epoch("recorded_at"), qs(task), limit))
	if e != nil {
		return nil, e
	}
	out := []domain.CDCPosition{}
	for _, x := range r.Rows {
		out = append(out, domain.CDCPosition{ID: sv(x, 0), TaskID: sv(x, 1), Direction: sv(x, 2), DatabaseType: sv(x, 3), PositionType: sv(x, 4), PositionValue: sv(x, 5), Resource: sv(x, 6), SourceTimestampMS: iv(x, 7), ApplyTimestampMS: iv(x, 8), LagMS: iv(x, 9), EventsTotal: iv(x, 10), EventsPending: iv(x, 11), RecordedAt: tv(x, 12)})
	}
	return out, nil
}

func spoolCols() string {
	return "sequence,id,task_id,direction,COALESCE(position_type,''),COALESCE(position_value,''),COALESCE(resource,''),source_timestamp_ms,event_count,payload_bytes,events_json::text,COALESCE(events_ciphertext,''),status," + epoch("created_at") + "," + epoch("applied_at")
}
func parseSpool(row [][]byte) domain.CDCSpoolRecord {
	v := domain.CDCSpoolRecord{Sequence: iv(row, 0), ID: sv(row, 1), TaskID: sv(row, 2), Direction: sv(row, 3), PositionType: sv(row, 4), PositionValue: sv(row, 5), Resource: sv(row, 6), SourceTimestampMS: iv(row, 7), EventCount: int(iv(row, 8)), PayloadBytes: iv(row, 9), EventsCiphertext: sv(row, 11), Status: domain.CDCSpoolStatus(sv(row, 12)), CreatedAt: tv(row, 13), AppliedAt: tv(row, 14)}
	_ = json.Unmarshal([]byte(sv(row, 10)), &v.Events)
	return v
}
func (s *Store) CreateCDCSpool(ctx context.Context, v *domain.CDCSpoolRecord) error {
	if v.EventCount == 0 {
		v.EventCount = len(v.Events)
	}
	q := fmt.Sprintf("INSERT INTO cdc_spool(id,task_id,direction,position_type,position_value,resource,source_timestamp_ms,event_count,payload_bytes,events_json,events_ciphertext,status,created_at) VALUES(%s,%s,%s,%s,%s,%s,%d,%d,%d,%s,%s,%s,%s) ON CONFLICT(id) DO NOTHING RETURNING sequence", qs(v.ID), qs(v.TaskID), qs(v.Direction), qn(v.PositionType), qn(v.PositionValue), qn(v.Resource), v.SourceTimestampMS, v.EventCount, v.PayloadBytes, ji(v.Events), qn(v.EventsCiphertext), qs(string(v.Status)), qt(v.CreatedAt))
	r, err := s.db.QuerySQL(ctx, q)
	if err != nil {
		return err
	}
	if len(r.Rows) > 0 {
		v.Sequence = iv(r.Rows[0], 0)
		return nil
	}
	r, err = s.db.QuerySQL(ctx, "SELECT sequence FROM cdc_spool WHERE id="+qs(v.ID))
	if err != nil {
		return err
	}
	if len(r.Rows) == 0 {
		return errors.New("cdc spool insert did not create or find record")
	}
	v.Sequence = iv(r.Rows[0], 0)
	return nil
}
func (s *Store) ListCDCSpool(ctx context.Context, taskID, direction string, limit int) ([]domain.CDCSpoolRecord, error) {
	if limit <= 0 || limit > 10000 {
		limit = 200
	}
	r, err := s.db.QuerySQL(ctx, fmt.Sprintf("SELECT %s FROM cdc_spool WHERE task_id=%s AND direction=%s AND status='PENDING' ORDER BY sequence LIMIT %d", spoolCols(), qs(taskID), qs(direction), limit))
	if err != nil {
		return nil, err
	}
	out := make([]domain.CDCSpoolRecord, 0, len(r.Rows))
	for _, row := range r.Rows {
		out = append(out, parseSpool(row))
	}
	return out, nil
}
func (s *Store) LatestPendingCDCSpool(ctx context.Context, taskID, direction string) (*domain.CDCSpoolRecord, error) {
	r, err := s.db.QuerySQL(ctx, fmt.Sprintf("SELECT %s FROM cdc_spool WHERE task_id=%s AND direction=%s AND status='PENDING' ORDER BY sequence DESC LIMIT 1", spoolCols(), qs(taskID), qs(direction)))
	if err != nil {
		return nil, err
	}
	if len(r.Rows) == 0 {
		return nil, errors.New("not found")
	}
	v := parseSpool(r.Rows[0])
	return &v, nil
}
func (s *Store) MarkCDCSpoolApplied(ctx context.Context, id string, at time.Time) error {
	return s.db.ExecSQL(ctx, "UPDATE cdc_spool SET status='APPLIED',applied_at="+qt(at)+" WHERE id="+qs(id))
}
func (s *Store) DeleteAppliedCDCSpool(ctx context.Context, taskID, direction string, keep int) error {
	if keep < 0 {
		keep = 0
	}
	q := fmt.Sprintf("DELETE FROM cdc_spool WHERE sequence IN (SELECT sequence FROM cdc_spool WHERE task_id=%s AND direction=%s AND status='APPLIED' ORDER BY sequence DESC OFFSET %d)", qs(taskID), qs(direction), keep)
	return s.db.ExecSQL(ctx, q)
}
func (s *Store) CDCSpoolStats(ctx context.Context, taskID, direction string) (domain.CDCSpoolStats, error) {
	r, err := s.db.QuerySQL(ctx, fmt.Sprintf("SELECT count(*),COALESCE(sum(event_count),0),COALESCE(sum(payload_bytes),0),COALESCE((array_agg(position_value ORDER BY sequence))[1],''),COALESCE((array_agg(position_value ORDER BY sequence DESC))[1],'') FROM cdc_spool WHERE task_id=%s AND direction=%s AND status='PENDING'", qs(taskID), qs(direction)))
	if err != nil {
		return domain.CDCSpoolStats{}, err
	}
	if len(r.Rows) == 0 {
		return domain.CDCSpoolStats{}, nil
	}
	return domain.CDCSpoolStats{PendingTransactions: iv(r.Rows[0], 0), PendingEvents: iv(r.Rows[0], 1), PendingBytes: iv(r.Rows[0], 2), FirstPosition: sv(r.Rows[0], 3), LastPosition: sv(r.Rows[0], 4)}, nil
}

func (s *Store) AcquireCDCSpoolDrainLease(ctx context.Context, taskID, direction, owner string, ttl time.Duration) (bool, error) {
	seconds := int(ttl.Seconds())
	if seconds < 10 {
		seconds = 300
	}
	q := fmt.Sprintf("INSERT INTO cdc_spool_drain_leases(task_id,direction,owner,lease_until,updated_at) VALUES(%s,%s,%s,now()+interval '%d seconds',now()) ON CONFLICT(task_id,direction) DO UPDATE SET owner=EXCLUDED.owner,lease_until=EXCLUDED.lease_until,updated_at=EXCLUDED.updated_at WHERE cdc_spool_drain_leases.lease_until<now() OR cdc_spool_drain_leases.owner=EXCLUDED.owner RETURNING owner", qs(taskID), qs(direction), qs(owner), seconds)
	r, err := s.db.QuerySQL(ctx, q)
	if err != nil {
		return false, err
	}
	return len(r.Rows) > 0 && sv(r.Rows[0], 0) == owner, nil
}

func (s *Store) ReleaseCDCSpoolDrainLease(ctx context.Context, taskID, direction, owner string) error {
	return s.db.ExecSQL(ctx, "DELETE FROM cdc_spool_drain_leases WHERE task_id="+qs(taskID)+" AND direction="+qs(direction)+" AND owner="+qs(owner))
}

func deadLetterCols() string {
	return "id,task_id,direction,COALESCE(position_type,''),COALESCE(position_value,''),COALESCE(resource,''),events_json::text,COALESCE(events_ciphertext,''),last_error,retry_count,status," + epoch("created_at") + "," + epoch("updated_at") + "," + epoch("resolved_at")
}
func parseDeadLetter(row [][]byte) domain.CDCDeadLetter {
	v := domain.CDCDeadLetter{ID: sv(row, 0), TaskID: sv(row, 1), Direction: sv(row, 2), PositionType: sv(row, 3), PositionValue: sv(row, 4), Resource: sv(row, 5), EventsCiphertext: sv(row, 7), LastError: sv(row, 8), RetryCount: int(iv(row, 9)), Status: domain.CDCDeadLetterStatus(sv(row, 10)), CreatedAt: tv(row, 11), UpdatedAt: tv(row, 12), ResolvedAt: tv(row, 13)}
	_ = json.Unmarshal([]byte(sv(row, 6)), &v.Events)
	return v
}
func (s *Store) CreateCDCDeadLetter(ctx context.Context, v *domain.CDCDeadLetter) error {
	q := fmt.Sprintf("INSERT INTO cdc_dead_letters(id,task_id,direction,position_type,position_value,resource,events_json,events_ciphertext,last_error,retry_count,status,created_at,updated_at,resolved_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%d,%s,%s,%s,%s) ON CONFLICT(id) DO UPDATE SET events_json=EXCLUDED.events_json,events_ciphertext=EXCLUDED.events_ciphertext,last_error=EXCLUDED.last_error,retry_count=EXCLUDED.retry_count,status=EXCLUDED.status,updated_at=EXCLUDED.updated_at,resolved_at=EXCLUDED.resolved_at", qs(v.ID), qs(v.TaskID), qs(v.Direction), qn(v.PositionType), qn(v.PositionValue), qn(v.Resource), ji(v.Events), qn(v.EventsCiphertext), qs(v.LastError), v.RetryCount, qs(string(v.Status)), qt(v.CreatedAt), qt(v.UpdatedAt), qt(v.ResolvedAt))
	return s.db.ExecSQL(ctx, q)
}
func (s *Store) UpdateCDCDeadLetter(ctx context.Context, v *domain.CDCDeadLetter) error {
	q := fmt.Sprintf("UPDATE cdc_dead_letters SET events_json=%s,events_ciphertext=%s,last_error=%s,retry_count=%d,status=%s,updated_at=%s,resolved_at=%s WHERE id=%s", ji(v.Events), qn(v.EventsCiphertext), qs(v.LastError), v.RetryCount, qs(string(v.Status)), qt(v.UpdatedAt), qt(v.ResolvedAt), qs(v.ID))
	return s.db.ExecSQL(ctx, q)
}
func (s *Store) GetCDCDeadLetter(ctx context.Context, id string) (*domain.CDCDeadLetter, error) {
	r, e := s.db.QuerySQL(ctx, "SELECT "+deadLetterCols()+" FROM cdc_dead_letters WHERE id="+qs(id))
	if e != nil {
		return nil, e
	}
	if len(r.Rows) == 0 {
		return nil, errors.New("not found")
	}
	v := parseDeadLetter(r.Rows[0])
	return &v, nil
}
func (s *Store) ListCDCDeadLetters(ctx context.Context, taskID string) ([]domain.CDCDeadLetter, error) {
	where := ""
	if taskID != "" {
		where = " WHERE task_id=" + qs(taskID)
	}
	r, e := s.db.QuerySQL(ctx, "SELECT "+deadLetterCols()+" FROM cdc_dead_letters"+where+" ORDER BY updated_at DESC")
	if e != nil {
		return nil, e
	}
	out := []domain.CDCDeadLetter{}
	for _, row := range r.Rows {
		out = append(out, parseDeadLetter(row))
	}
	return out, nil
}
func (s *Store) DeleteCDCDeadLetter(ctx context.Context, id string) error {
	return s.db.ExecSQL(ctx, "DELETE FROM cdc_dead_letters WHERE id="+qs(id))
}
func (s *Store) CreateCDCConflict(ctx context.Context, v *domain.CDCConflictRecord) error {
	return s.db.ExecSQL(ctx, fmt.Sprintf("INSERT INTO cdc_conflicts(id,task_id,direction,source_schema,source_table,key_fingerprint,policy,decision,source_version,target_version,position_type,position_value,created_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(id) DO NOTHING", qs(v.ID), qs(v.TaskID), qs(v.Direction), qs(v.SourceSchema), qs(v.SourceTable), qs(v.KeyFingerprint), qs(v.Policy), qs(string(v.Decision)), qn(v.SourceVersion), qn(v.TargetVersion), qn(v.PositionType), qn(v.PositionValue), qt(v.CreatedAt)))
}
func (s *Store) ListCDCConflicts(ctx context.Context, taskID string, limit int) ([]domain.CDCConflictRecord, error) {
	if limit <= 0 || limit > 5000 {
		limit = 500
	}
	where := ""
	if taskID != "" {
		where = " WHERE task_id=" + qs(taskID)
	}
	r, err := s.db.QuerySQL(ctx, fmt.Sprintf("SELECT id,task_id,direction,source_schema,source_table,key_fingerprint,policy,decision,COALESCE(source_version,''),COALESCE(target_version,''),COALESCE(position_type,''),COALESCE(position_value,''),%s FROM cdc_conflicts%s ORDER BY created_at DESC LIMIT %d", epoch("created_at"), where, limit))
	if err != nil {
		return nil, err
	}
	out := make([]domain.CDCConflictRecord, 0, len(r.Rows))
	for _, x := range r.Rows {
		out = append(out, domain.CDCConflictRecord{ID: sv(x, 0), TaskID: sv(x, 1), Direction: sv(x, 2), SourceSchema: sv(x, 3), SourceTable: sv(x, 4), KeyFingerprint: sv(x, 5), Policy: sv(x, 6), Decision: domain.CDCConflictDecision(sv(x, 7)), SourceVersion: sv(x, 8), TargetVersion: sv(x, 9), PositionType: sv(x, 10), PositionValue: sv(x, 11), CreatedAt: tv(x, 12)})
	}
	return out, nil
}

func (s *Store) CreateValidationResult(ctx context.Context, v *domain.ValidationResult) error {
	return s.db.ExecSQL(ctx, fmt.Sprintf("INSERT INTO validation_results(id,task_id,table_id,chunk_id,status,source_rows,target_rows,source_checksum,target_checksum,last_error,started_at,finished_at) VALUES(%s,%s,%s,%s,%s,%d,%d,%s,%s,%s,%s,%s)", qs(v.ID), qs(v.TaskID), qs(v.TableID), qs(v.ChunkID), qs(string(v.Status)), v.SourceRows, v.TargetRows, qn(v.SourceChecksum), qn(v.TargetChecksum), qn(v.LastError), qt(v.StartedAt), qt(v.FinishedAt)))
}
func (s *Store) ListValidationResults(ctx context.Context, task string) ([]domain.ValidationResult, error) {
	r, e := s.db.QuerySQL(ctx, "SELECT id,task_id,table_id,chunk_id,status,source_rows,target_rows,COALESCE(source_checksum,''),COALESCE(target_checksum,''),COALESCE(last_error,''),"+epoch("started_at")+","+epoch("finished_at")+" FROM validation_results WHERE task_id="+qs(task)+" ORDER BY started_at")
	if e != nil {
		return nil, e
	}
	out := []domain.ValidationResult{}
	for _, x := range r.Rows {
		out = append(out, domain.ValidationResult{ID: sv(x, 0), TaskID: sv(x, 1), TableID: sv(x, 2), ChunkID: sv(x, 3), Status: domain.ValidationStatus(sv(x, 4)), SourceRows: iv(x, 5), TargetRows: iv(x, 6), SourceChecksum: sv(x, 7), TargetChecksum: sv(x, 8), LastError: sv(x, 9), StartedAt: tv(x, 10), FinishedAt: tv(x, 11)})
	}
	return out, nil
}
func (s *Store) DeleteValidationResults(ctx context.Context, task string) error {
	return s.db.ExecSQL(ctx, "DELETE FROM validation_results WHERE task_id="+qs(task))
}
func (s *Store) CreateAlert(ctx context.Context, a *domain.Alert) error {
	return s.db.ExecSQL(ctx, fmt.Sprintf("INSERT INTO alerts(id,severity,title,message,task_id,acknowledged,created_at) VALUES(%s,%s,%s,%s,%s,%s,%s)", qs(a.ID), qs(a.Severity), qs(a.Title), qs(a.Message), qn(a.TaskID), qb(a.Acknowledged), qt(a.CreatedAt)))
}
func (s *Store) ListAlerts(ctx context.Context) ([]domain.Alert, error) {
	r, e := s.db.QuerySQL(ctx, "SELECT id,severity,title,message,COALESCE(task_id,''),acknowledged,"+epoch("created_at")+" FROM alerts ORDER BY created_at DESC")
	if e != nil {
		return nil, e
	}
	out := []domain.Alert{}
	for _, x := range r.Rows {
		out = append(out, domain.Alert{ID: sv(x, 0), Severity: sv(x, 1), Title: sv(x, 2), Message: sv(x, 3), TaskID: sv(x, 4), Acknowledged: bv(x, 5), CreatedAt: tv(x, 6)})
	}
	return out, nil
}
func (s *Store) AcknowledgeAlert(ctx context.Context, id string) error {
	return s.db.ExecSQL(ctx, "UPDATE alerts SET acknowledged=TRUE WHERE id="+qs(id))
}
func (s *Store) CreateAuditEvent(ctx context.Context, a *domain.AuditEvent) error {
	return s.db.ExecSQL(ctx, fmt.Sprintf("INSERT INTO audit_events(id,actor,action,resource_type,resource_id,detail,remote_addr,created_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)", qs(a.ID), qs(a.Actor), qs(a.Action), qs(a.ResourceType), qn(a.ResourceID), qn(a.Detail), qn(a.RemoteAddr), qt(a.CreatedAt)))
}
func (s *Store) ListAuditEvents(ctx context.Context, limit int) ([]domain.AuditEvent, error) {
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	r, e := s.db.QuerySQL(ctx, fmt.Sprintf("SELECT id,actor,action,resource_type,COALESCE(resource_id,''),COALESCE(detail,''),COALESCE(remote_addr,''),%s FROM audit_events ORDER BY created_at DESC LIMIT %d", epoch("created_at"), limit))
	if e != nil {
		return nil, e
	}
	out := []domain.AuditEvent{}
	for _, x := range r.Rows {
		out = append(out, domain.AuditEvent{ID: sv(x, 0), Actor: sv(x, 1), Action: sv(x, 2), ResourceType: sv(x, 3), ResourceID: sv(x, 4), Detail: sv(x, 5), RemoteAddr: sv(x, 6), CreatedAt: tv(x, 7)})
	}
	return out, nil
}

var _ repository.Repository = (*Store)(nil)
var _ = sort.Strings

func userCols() string {
	return "id,username,password_hash,role,enabled," + epoch("created_at") + "," + epoch("updated_at") + "," + epoch("last_login_at")
}
func parseUser(r [][]byte) domain.User {
	return domain.User{ID: sv(r, 0), Username: sv(r, 1), PasswordHash: sv(r, 2), Role: sv(r, 3), Enabled: bv(r, 4), CreatedAt: tv(r, 5), UpdatedAt: tv(r, 6), LastLoginAt: tv(r, 7)}
}
func (s *Store) CreateUser(ctx context.Context, u *domain.User) error {
	return s.db.ExecSQL(ctx, fmt.Sprintf("INSERT INTO users(id,username,password_hash,role,enabled,created_at,updated_at,last_login_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)", qs(u.ID), qs(u.Username), qs(u.PasswordHash), qs(u.Role), qb(u.Enabled), qt(u.CreatedAt), qt(u.UpdatedAt), qt(u.LastLoginAt)))
}
func (s *Store) UpdateUser(ctx context.Context, u *domain.User) error {
	return s.db.ExecSQL(ctx, fmt.Sprintf("UPDATE users SET username=%s,password_hash=%s,role=%s,enabled=%s,updated_at=%s,last_login_at=%s WHERE id=%s", qs(u.Username), qs(u.PasswordHash), qs(u.Role), qb(u.Enabled), qt(u.UpdatedAt), qt(u.LastLoginAt), qs(u.ID)))
}
func (s *Store) GetUser(ctx context.Context, id string) (*domain.User, error) {
	r, e := s.db.QuerySQL(ctx, "SELECT "+userCols()+" FROM users WHERE id="+qs(id))
	if e != nil {
		return nil, e
	}
	if len(r.Rows) == 0 {
		return nil, errors.New("not found")
	}
	v := parseUser(r.Rows[0])
	return &v, nil
}
func (s *Store) GetUserByUsername(ctx context.Context, name string) (*domain.User, error) {
	r, e := s.db.QuerySQL(ctx, "SELECT "+userCols()+" FROM users WHERE username="+qs(name))
	if e != nil {
		return nil, e
	}
	if len(r.Rows) == 0 {
		return nil, errors.New("not found")
	}
	v := parseUser(r.Rows[0])
	return &v, nil
}
func (s *Store) ListUsers(ctx context.Context) ([]domain.User, error) {
	r, e := s.db.QuerySQL(ctx, "SELECT "+userCols()+" FROM users ORDER BY username")
	if e != nil {
		return nil, e
	}
	out := []domain.User{}
	for _, x := range r.Rows {
		out = append(out, parseUser(x))
	}
	return out, nil
}

func (s *Store) CreateTaskLog(ctx context.Context, v *domain.TaskLog) error {
	return s.db.ExecSQL(ctx, fmt.Sprintf("INSERT INTO task_logs(id,task_id,worker_id,table_id,chunk_id,level,message,created_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)", qs(v.ID), qs(v.TaskID), qn(v.WorkerID), qn(v.TableID), qn(v.ChunkID), qs(v.Level), qs(v.Message), qt(v.CreatedAt)))
}

func (s *Store) ListTaskLogs(ctx context.Context, taskID string, limit int) ([]domain.TaskLog, error) {
	if limit <= 0 || limit > 5000 {
		limit = 500
	}
	where := ""
	if taskID != "" {
		where = " WHERE task_id=" + qs(taskID)
	}
	r, err := s.db.QuerySQL(ctx, fmt.Sprintf("SELECT id,task_id,COALESCE(worker_id,''),COALESCE(table_id,''),COALESCE(chunk_id,''),level,message,%s FROM task_logs%s ORDER BY created_at DESC LIMIT %d", epoch("created_at"), where, limit))
	if err != nil {
		return nil, err
	}
	out := make([]domain.TaskLog, 0, len(r.Rows))
	for _, x := range r.Rows {
		out = append(out, domain.TaskLog{ID: sv(x, 0), TaskID: sv(x, 1), WorkerID: sv(x, 2), TableID: sv(x, 3), ChunkID: sv(x, 4), Level: sv(x, 5), Message: sv(x, 6), CreatedAt: tv(x, 7)})
	}
	return out, nil
}

func pruneCount(ctx context.Context, db interface {
	QuerySQL(context.Context, string) (*postgresconnector.RawRows, error)
}, q string) (int64, error) {
	r, err := db.QuerySQL(ctx, q)
	if err != nil {
		return 0, err
	}
	if len(r.Rows) == 0 || len(r.Rows[0]) == 0 {
		return 0, nil
	}
	return iv(r.Rows[0], 0), nil
}

func taskLogPruneSQL(policy repository.MetadataRetentionPolicy, now time.Time, batch int) string {
	conditions := make([]string, 0, 2)
	if policy.TaskLogMaxRowsPerTask > 0 {
		conditions = append(conditions, fmt.Sprintf("rn>%d", policy.TaskLogMaxRowsPerTask))
	}
	if policy.TaskLogMaxAge > 0 {
		conditions = append(conditions, "created_at<"+qt(now.Add(-policy.TaskLogMaxAge)))
	}
	if len(conditions) == 0 {
		return ""
	}
	return fmt.Sprintf(`WITH ranked AS (
 SELECT id,created_at,row_number() OVER(PARTITION BY task_id ORDER BY created_at DESC,id DESC) AS rn FROM task_logs
), candidates AS (
 SELECT id FROM ranked WHERE (%s) ORDER BY created_at,id LIMIT %d
), deleted AS (
 DELETE FROM task_logs t USING candidates c WHERE t.id=c.id RETURNING 1
) SELECT count(*) FROM deleted`, strings.Join(conditions, " OR "), batch)
}

func auditPruneSQL(policy repository.MetadataRetentionPolicy, now time.Time, batch int) string {
	conditions := make([]string, 0, 2)
	if policy.AuditMaxRows > 0 {
		conditions = append(conditions, fmt.Sprintf("rn>%d", policy.AuditMaxRows))
	}
	if policy.AuditMaxAge > 0 {
		conditions = append(conditions, "created_at<"+qt(now.Add(-policy.AuditMaxAge)))
	}
	if len(conditions) == 0 {
		return ""
	}
	return fmt.Sprintf(`WITH ranked AS (
 SELECT id,created_at,row_number() OVER(ORDER BY created_at DESC,id DESC) AS rn FROM audit_events
), candidates AS (
 SELECT id FROM ranked WHERE (%s) ORDER BY created_at,id LIMIT %d
), deleted AS (
 DELETE FROM audit_events a USING candidates c WHERE a.id=c.id RETURNING 1
) SELECT count(*) FROM deleted`, strings.Join(conditions, " OR "), batch)
}

func cdcPositionPruneSQL(policy repository.MetadataRetentionPolicy, now time.Time, batch int) string {
	conditions := make([]string, 0, 2)
	if policy.CDCPositionMaxRowsPerStream > 0 {
		conditions = append(conditions, fmt.Sprintf("rn>%d", policy.CDCPositionMaxRowsPerStream))
	}
	if policy.CDCPositionMaxAge > 0 {
		// rn>1 is a correctness invariant: the newest task+direction checkpoint
		// survives time-based retention even when a migration is paused for weeks.
		conditions = append(conditions, "(recorded_at<"+qt(now.Add(-policy.CDCPositionMaxAge))+" AND rn>1)")
	}
	if len(conditions) == 0 {
		return ""
	}
	return fmt.Sprintf(`WITH ranked AS (
 SELECT id,recorded_at,row_number() OVER(PARTITION BY task_id,COALESCE(direction,'forward') ORDER BY recorded_at DESC,id DESC) AS rn FROM cdc_positions
), candidates AS (
 SELECT id FROM ranked WHERE (%s) ORDER BY recorded_at,id LIMIT %d
), deleted AS (
 DELETE FROM cdc_positions p USING candidates c WHERE p.id=c.id RETURNING 1
) SELECT count(*) FROM deleted`, strings.Join(conditions, " OR "), batch)
}

func validationPruneSQL(policy repository.MetadataRetentionPolicy, now time.Time, batch int) string {
	conditions := make([]string, 0, 3)
	newerExists := func(offset int) string {
		return fmt.Sprintf(`EXISTS(SELECT 1 FROM validation_results newer
 WHERE newer.task_id=v.task_id AND newer.chunk_id=v.chunk_id
   AND (newer.finished_at>v.finished_at OR (newer.finished_at=v.finished_at AND newer.id>v.id))
 ORDER BY newer.finished_at DESC,newer.id DESC OFFSET %d LIMIT 1)`, offset)
	}
	if policy.ValidationMaxAttemptsPerChunk > 0 {
		// Rank > MaxAttempts is equivalent to at least MaxAttempts newer rows.
		// The correlated probe can use idx_validation_task_chunk_latest and does
		// not require a window over the entire validation_results table.
		conditions = append(conditions, newerExists(policy.ValidationMaxAttemptsPerChunk-1))
	}
	if policy.ValidationAttemptMaxAge > 0 {
		// Age retention applies only when a newer attempt exists. The current
		// task+chunk head is therefore preserved even when it is very old.
		conditions = append(conditions, "(v.finished_at<"+qt(now.Add(-policy.ValidationAttemptMaxAge))+" AND "+newerExists(0)+")")
	}
	if policy.ValidationTerminalMaxAge > 0 {
		// Latest terminal-task detail may be removed only after an immutable
		// validation archive exists. This is a fail-closed audit invariant.
		conditions = append(conditions, "(m.status IN ('FINISHED','FAILED','CANCELLED','ROLLED_BACK') AND m.updated_at<"+qt(now.Add(-policy.ValidationTerminalMaxAge))+" AND EXISTS(SELECT 1 FROM validation_archives a WHERE a.task_id=v.task_id))")
	}
	if len(conditions) == 0 {
		return ""
	}
	return fmt.Sprintf(`WITH candidates AS (
 SELECT v.id,v.finished_at FROM validation_results v JOIN migration_tasks m ON m.id=v.task_id
 WHERE (%s) ORDER BY v.finished_at,v.id LIMIT %d
), deleted AS (
 DELETE FROM validation_results v USING candidates c WHERE v.id=c.id RETURNING 1
) SELECT count(*) FROM deleted`, strings.Join(conditions, " OR "), batch)
}

func (s *Store) archiveExpiredTerminalValidationTasks(ctx context.Context, policy repository.MetadataRetentionPolicy, now time.Time) (int64, error) {
	if policy.ValidationTerminalMaxAge <= 0 {
		return 0, nil
	}
	limit := policy.ValidationArchiveTasksPerRun
	if limit <= 0 {
		limit = 8
	}
	pageSize := policy.ValidationArchivePageSize
	if pageSize <= 0 || pageSize > 5000 {
		pageSize = 512
	}
	q := `SELECT DISTINCT v.task_id FROM validation_results v
JOIN migration_tasks m ON m.id=v.task_id
LEFT JOIN validation_archives a ON a.task_id=v.task_id
WHERE a.task_id IS NULL
  AND m.status IN ('FINISHED','FAILED','CANCELLED','ROLLED_BACK')
  AND m.updated_at<` + qt(now.Add(-policy.ValidationTerminalMaxAge)) + `
ORDER BY v.task_id LIMIT ` + fmt.Sprintf("%d", limit)
	r, err := s.db.QuerySQL(ctx, q)
	if err != nil {
		return 0, err
	}
	var created int64
	for _, row := range r.Rows {
		_, made, archiveErr := repository.EnsureValidationArchive(ctx, s, sv(row, 0), pageSize)
		if archiveErr != nil {
			if errors.Is(archiveErr, repository.ErrNoValidationEvidence) {
				continue
			}
			return created, archiveErr
		}
		if made {
			created++
		}
	}
	return created, nil
}

func (s *Store) PruneMetadata(ctx context.Context, policy repository.MetadataRetentionPolicy) (repository.MetadataPruneResult, error) {
	batch := policy.BatchRows
	if batch <= 0 {
		batch = 5000
	}
	maxBatches := policy.MaxBatches
	if maxBatches <= 0 {
		maxBatches = 4
	}
	now := time.Now().UTC()
	var out repository.MetadataPruneResult
	archives, err := s.archiveExpiredTerminalValidationTasks(ctx, policy, now)
	if err != nil {
		return out, fmt.Errorf("archive terminal validation evidence: %w", err)
	}
	out.ValidationArchivesCreated = archives
	run := func(q string, dst *int64) error {
		if q == "" {
			return nil
		}
		for i := 0; i < maxBatches; i++ {
			n, err := pruneCount(ctx, s.db, q)
			if err != nil {
				return err
			}
			*dst += n
			if n < int64(batch) {
				break
			}
		}
		return nil
	}
	if err := run(taskLogPruneSQL(policy, now, batch), &out.TaskLogsDeleted); err != nil {
		return out, fmt.Errorf("prune task logs: %w", err)
	}
	if err := run(auditPruneSQL(policy, now, batch), &out.AuditEventsDeleted); err != nil {
		return out, fmt.Errorf("prune audit events: %w", err)
	}
	if err := run(cdcPositionPruneSQL(policy, now, batch), &out.CDCPositionsDeleted); err != nil {
		return out, fmt.Errorf("prune CDC positions: %w", err)
	}
	if err := run(validationPruneSQL(policy, now, batch), &out.ValidationDeleted); err != nil {
		return out, fmt.Errorf("prune validation results: %w", err)
	}
	return out, nil
}

func chunkSummarySQL(taskID string) string {
	return `SELECT table_id,count(*),` +
		`count(*) FILTER (WHERE status='SUCCESS'),` +
		`count(*) FILTER (WHERE status='PENDING'),` +
		`count(*) FILTER (WHERE status='RUNNING'),` +
		`count(*) FILTER (WHERE status='FAILED'),` +
		`COALESCE(sum(rows_written) FILTER (WHERE status='SUCCESS'),0),` +
		`COALESCE(sum(bytes_written) FILTER (WHERE status='SUCCESS'),0),` +
		`COALESCE(sum(last_read_ms),0),COALESCE(sum(last_write_ms),0),` +
		`count(*) FILTER (WHERE last_read_ms>0) ` +
		`FROM migration_chunks WHERE task_id=` + qs(taskID) + ` GROUP BY table_id`
}

func (s *Store) SummarizeTaskChunks(ctx context.Context, taskID string) (repository.TaskChunkSummary, error) {
	r, err := s.db.QuerySQL(ctx, chunkSummarySQL(taskID))
	if err != nil {
		return repository.TaskChunkSummary{}, err
	}
	out := repository.TaskChunkSummary{Tables: map[string]repository.ChunkTableSummary{}}
	for _, row := range r.Rows {
		t := repository.ChunkTableSummary{
			Total: int(iv(row, 1)), Success: int(iv(row, 2)), Pending: int(iv(row, 3)), Running: int(iv(row, 4)), Failed: int(iv(row, 5)),
			RowsWritten: iv(row, 6), BytesWritten: iv(row, 7), ReadMS: iv(row, 8), WriteMS: iv(row, 9), LatencySamples: iv(row, 10),
		}
		out.Tables[sv(row, 0)] = t
		out.Total += t.Total
		out.Success += t.Success
		out.Pending += t.Pending
		out.Running += t.Running
		out.Failed += t.Failed
		out.RowsWritten += t.RowsWritten
		out.BytesWritten += t.BytesWritten
		out.ReadMS += t.ReadMS
		out.WriteMS += t.WriteMS
		out.LatencySamples += t.LatencySamples
	}
	return out, nil
}

// RC43 validation hot paths keep validation control-plane memory bounded and
// move latest-result coverage checks into PostgreSQL.
func (s *Store) ListTableChunksPage(ctx context.Context, taskID, tableID string, afterChunkNo int, afterID string, limit int) ([]domain.MigrationChunk, error) {
	if limit <= 0 || limit > 5000 {
		limit = 512
	}
	q := "SELECT " + chunkCols() + " FROM migration_chunks WHERE task_id=" + qs(taskID) + " AND table_id=" + qs(tableID) +
		" AND (chunk_no > " + fmt.Sprintf("%d", afterChunkNo) + " OR (chunk_no = " + fmt.Sprintf("%d", afterChunkNo) + " AND id > " + qs(afterID) + ")) ORDER BY chunk_no,id LIMIT " + fmt.Sprintf("%d", limit)
	r, err := s.db.QuerySQL(ctx, q)
	if err != nil {
		return nil, err
	}
	out := make([]domain.MigrationChunk, 0, len(r.Rows))
	for _, row := range r.Rows {
		v := parseChunk(row)
		parseChunkPlacement(row, &v)
		out = append(out, v)
	}
	return out, nil
}

func (s *Store) HasValidationResults(ctx context.Context, taskID string) (bool, error) {
	r, err := s.db.QuerySQL(ctx, "SELECT EXISTS(SELECT 1 FROM validation_results WHERE task_id="+qs(taskID)+")")
	if err != nil {
		return false, err
	}
	if len(r.Rows) == 0 {
		return false, nil
	}
	return strings.EqualFold(sv(r.Rows[0], 0), "true") || sv(r.Rows[0], 0) == "1" || sv(r.Rows[0], 0) == "t", nil
}

func (s *Store) FirstInvalidSuccessfulChunk(ctx context.Context, taskID string) (string, domain.ValidationStatus, error) {
	q := "SELECT c.id,COALESCE(v.status,'') FROM migration_chunks c LEFT JOIN LATERAL (SELECT status FROM validation_results WHERE task_id=c.task_id AND chunk_id=c.id ORDER BY finished_at DESC,id DESC LIMIT 1) v ON true WHERE c.task_id=" + qs(taskID) + " AND c.status='SUCCESS' AND (v.status IS NULL OR v.status<>'SUCCESS') ORDER BY c.table_id,c.chunk_no,c.id LIMIT 1"
	r, err := s.db.QuerySQL(ctx, q)
	if err != nil {
		return "", "", err
	}
	if len(r.Rows) == 0 {
		return "", "", nil
	}
	return sv(r.Rows[0], 0), domain.ValidationStatus(sv(r.Rows[0], 1)), nil
}

func (s *Store) ListRepairableValidationChunkIDs(ctx context.Context, taskID string, limit int) ([]string, error) {
	if limit <= 0 || limit > 100000 {
		limit = 10000
	}
	q := "SELECT chunk_id FROM (SELECT DISTINCT ON (chunk_id) chunk_id,status FROM validation_results WHERE task_id=" + qs(taskID) + " ORDER BY chunk_id,finished_at DESC,id DESC) v WHERE status IN ('MISMATCH','ERROR') ORDER BY chunk_id LIMIT " + fmt.Sprintf("%d", limit)
	r, err := s.db.QuerySQL(ctx, q)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(r.Rows))
	for _, row := range r.Rows {
		out = append(out, sv(row, 0))
	}
	return out, nil
}

var _ repository.ValidationHotPathProvider = (*Store)(nil)

func validationArchiveSelect() string {
	return "task_id,terminal_status,validation_mode,COALESCE(validation_barrier_position_type,''),COALESCE(validation_barrier_position_value,''),COALESCE(validation_barrier_resource,''),total_tables,total_chunks,covered_chunks,success_chunks,mismatch_chunks,error_chunks,missing_chunks,evidence_digest,tables_json," + epoch("archived_at")
}

func parseValidationArchive(row [][]byte) (*domain.ValidationArchive, error) {
	if len(row) == 0 {
		return nil, nil
	}
	v := &domain.ValidationArchive{
		TaskID: sv(row, 0), TerminalStatus: domain.MigrationStatus(sv(row, 1)), ValidationMode: sv(row, 2),
		ValidationBarrierPositionType: sv(row, 3), ValidationBarrierPosition: sv(row, 4), ValidationBarrierResource: sv(row, 5),
		TotalTables: int(iv(row, 6)), TotalChunks: int(iv(row, 7)), CoveredChunks: int(iv(row, 8)), SuccessChunks: int(iv(row, 9)),
		MismatchChunks: int(iv(row, 10)), ErrorChunks: int(iv(row, 11)), MissingChunks: int(iv(row, 12)), EvidenceDigest: sv(row, 13), ArchivedAt: tv(row, 15),
	}
	if raw := sv(row, 14); raw != "" {
		if err := json.Unmarshal([]byte(raw), &v.Tables); err != nil {
			return nil, fmt.Errorf("parse validation archive tables: %w", err)
		}
	}
	return v, nil
}

func (s *Store) GetValidationArchive(ctx context.Context, taskID string) (*domain.ValidationArchive, error) {
	r, err := s.db.QuerySQL(ctx, "SELECT "+validationArchiveSelect()+" FROM validation_archives WHERE task_id="+qs(taskID))
	if err != nil {
		return nil, err
	}
	if len(r.Rows) == 0 {
		return nil, nil
	}
	return parseValidationArchive(r.Rows[0])
}

func (s *Store) CreateValidationArchive(ctx context.Context, a *domain.ValidationArchive) (bool, error) {
	if a == nil {
		return false, errors.New("nil validation archive")
	}
	q := fmt.Sprintf(`INSERT INTO validation_archives(task_id,terminal_status,validation_mode,validation_barrier_position_type,validation_barrier_position_value,validation_barrier_resource,total_tables,total_chunks,covered_chunks,success_chunks,mismatch_chunks,error_chunks,missing_chunks,evidence_digest,tables_json,archived_at)
VALUES(%s,%s,%s,%s,%s,%s,%d,%d,%d,%d,%d,%d,%d,%s,%s,%s) ON CONFLICT(task_id) DO NOTHING RETURNING task_id`,
		qs(a.TaskID), qs(string(a.TerminalStatus)), qs(a.ValidationMode), qn(a.ValidationBarrierPositionType), qn(a.ValidationBarrierPosition), qn(a.ValidationBarrierResource),
		a.TotalTables, a.TotalChunks, a.CoveredChunks, a.SuccessChunks, a.MismatchChunks, a.ErrorChunks, a.MissingChunks, qs(a.EvidenceDigest), ji(a.Tables), qt(a.ArchivedAt))
	r, err := s.db.QuerySQL(ctx, q)
	if err != nil {
		return false, err
	}
	return len(r.Rows) > 0, nil
}

func validationEvidenceCols() string {
	return "c.id,c.chunk_no,c.split_type,COALESCE(v.id,''),COALESCE(v.status,''),COALESCE(v.source_rows,0),COALESCE(v.target_rows,0),COALESCE(v.source_checksum,''),COALESCE(v.target_checksum,''),COALESCE(v.last_error,''),COALESCE(" + epoch("v.started_at") + ",0),COALESCE(" + epoch("v.finished_at") + ",0)"
}

func (s *Store) ListValidationEvidencePage(ctx context.Context, taskID, tableID string, afterChunkNo int, afterID string, limit int) ([]repository.ValidationEvidenceRow, error) {
	if limit <= 0 || limit > 5000 {
		limit = 512
	}
	q := "SELECT " + validationEvidenceCols() + ` FROM migration_chunks c
LEFT JOIN LATERAL (
 SELECT id,status,source_rows,target_rows,source_checksum,target_checksum,last_error,started_at,finished_at
 FROM validation_results WHERE task_id=c.task_id AND chunk_id=c.id ORDER BY finished_at DESC,id DESC LIMIT 1
) v ON true WHERE c.task_id=` + qs(taskID) + " AND c.table_id=" + qs(tableID) + " AND c.status='SUCCESS'" +
		" AND (c.chunk_no > " + fmt.Sprintf("%d", afterChunkNo) + " OR (c.chunk_no = " + fmt.Sprintf("%d", afterChunkNo) + " AND c.id > " + qs(afterID) + ")) ORDER BY c.chunk_no,c.id LIMIT " + fmt.Sprintf("%d", limit)
	r, err := s.db.QuerySQL(ctx, q)
	if err != nil {
		return nil, err
	}
	out := make([]repository.ValidationEvidenceRow, 0, len(r.Rows))
	for _, row := range r.Rows {
		out = append(out, repository.ValidationEvidenceRow{
			ChunkID: sv(row, 0), ChunkNo: int(iv(row, 1)), SplitType: sv(row, 2), ValidationID: sv(row, 3), Status: domain.ValidationStatus(sv(row, 4)),
			SourceRows: iv(row, 5), TargetRows: iv(row, 6), SourceChecksum: sv(row, 7), TargetChecksum: sv(row, 8), LastError: sv(row, 9), StartedAt: tv(row, 10), FinishedAt: tv(row, 11),
		})
	}
	return out, nil
}

func (s *Store) LatestValidationStatusCounts(ctx context.Context, taskID string) (success, mismatch, validationError, missing int, err error) {
	q := `WITH latest AS (
 SELECT DISTINCT ON (chunk_id) chunk_id,status FROM validation_results WHERE task_id=` + qs(taskID) + ` ORDER BY chunk_id,finished_at DESC,id DESC
), eligible AS (
 SELECT c.id,l.status FROM migration_chunks c LEFT JOIN latest l ON l.chunk_id=c.id WHERE c.task_id=` + qs(taskID) + ` AND c.status='SUCCESS'
)
SELECT count(*) FILTER (WHERE status='SUCCESS'),count(*) FILTER (WHERE status='MISMATCH'),count(*) FILTER (WHERE status='ERROR'),count(*) FILTER (WHERE status IS NULL) FROM eligible`
	r, e := s.db.QuerySQL(ctx, q)
	if e != nil {
		err = e
		return
	}
	if len(r.Rows) > 0 {
		success, mismatch, validationError, missing = int(iv(r.Rows[0], 0)), int(iv(r.Rows[0], 1)), int(iv(r.Rows[0], 2)), int(iv(r.Rows[0], 3))
	}
	return
}

var _ repository.ValidationArchiveProvider = (*Store)(nil)

func validationReportArchiveSelect() string {
	return "task_id,evidence_digest,uri,COALESCE(bucket,''),COALESCE(prefix,''),manifest_sha256,COALESCE(public_signature_algorithm,''),COALESCE(public_signature_key_id,''),COALESCE(public_key_ed25519,''),COALESCE(public_key_fingerprint_sha256,''),COALESCE(object_lock_mode,''),COALESCE(retain_until,''),legal_hold," + epoch("committed_at")
}

func parseValidationReportArchive(row [][]byte) *domain.ValidationReportArchiveRecord {
	if len(row) == 0 {
		return nil
	}
	return &domain.ValidationReportArchiveRecord{
		TaskID: sv(row, 0), EvidenceDigest: sv(row, 1), URI: sv(row, 2), Bucket: sv(row, 3), Prefix: sv(row, 4), ManifestSHA256: sv(row, 5),
		PublicSignatureAlgorithm: sv(row, 6), PublicSignatureKeyID: sv(row, 7), PublicKeyEd25519: sv(row, 8), PublicKeyFingerprintSHA256: sv(row, 9),
		ObjectLockMode: sv(row, 10), RetainUntil: sv(row, 11), LegalHold: bv(row, 12), CommittedAt: tv(row, 13),
	}
}

func (s *Store) GetValidationReportArchive(ctx context.Context, taskID, evidenceDigest string) (*domain.ValidationReportArchiveRecord, error) {
	q := "SELECT " + validationReportArchiveSelect() + " FROM validation_report_archives WHERE task_id=" + qs(taskID) + " AND evidence_digest=" + qs(strings.ToLower(strings.TrimSpace(evidenceDigest)))
	r, err := s.db.QuerySQL(ctx, q)
	if err != nil {
		return nil, err
	}
	if len(r.Rows) == 0 {
		return nil, nil
	}
	return parseValidationReportArchive(r.Rows[0]), nil
}

func (s *Store) CreateValidationReportArchive(ctx context.Context, a *domain.ValidationReportArchiveRecord) (bool, error) {
	if a == nil {
		return false, errors.New("nil validation report archive record")
	}
	digest := strings.ToLower(strings.TrimSpace(a.EvidenceDigest))
	if strings.TrimSpace(a.TaskID) == "" || digest == "" || strings.TrimSpace(a.URI) == "" || strings.TrimSpace(a.ManifestSHA256) == "" {
		return false, errors.New("validation report archive requires task, evidence digest, URI and manifest SHA-256")
	}
	q := fmt.Sprintf(`INSERT INTO validation_report_archives(task_id,evidence_digest,uri,bucket,prefix,manifest_sha256,public_signature_algorithm,public_signature_key_id,public_key_ed25519,public_key_fingerprint_sha256,object_lock_mode,retain_until,legal_hold,committed_at)
VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(task_id,evidence_digest) DO NOTHING RETURNING task_id`,
		qs(a.TaskID), qs(digest), qs(a.URI), qn(a.Bucket), qn(a.Prefix), qs(strings.ToLower(a.ManifestSHA256)), qn(a.PublicSignatureAlgorithm), qn(a.PublicSignatureKeyID), qn(a.PublicKeyEd25519), qn(strings.ToLower(a.PublicKeyFingerprintSHA256)), qn(a.ObjectLockMode), qn(a.RetainUntil), qb(a.LegalHold), qt(a.CommittedAt))
	r, err := s.db.QuerySQL(ctx, q)
	if err != nil {
		return false, err
	}
	if len(r.Rows) > 0 {
		return true, nil
	}
	existing, err := s.GetValidationReportArchive(ctx, a.TaskID, digest)
	if err != nil {
		return false, err
	}
	if existing == nil || !repository.ValidationReportArchiveEqual(existing, a) {
		return false, repository.ErrValidationReportArchiveConflict
	}
	return false, nil
}

var _ repository.ValidationReportArchiveProvider = (*Store)(nil)
