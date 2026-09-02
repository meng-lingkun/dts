CREATE TABLE IF NOT EXISTS datasources(id text PRIMARY KEY,name text NOT NULL,type text NOT NULL,host text NOT NULL,port integer NOT NULL,username text NOT NULL,password_ciphertext text NOT NULL DEFAULT '',database_name text,schema_name text,created_at timestamptz NOT NULL,updated_at timestamptz NOT NULL);
ALTER TABLE datasources ADD COLUMN IF NOT EXISTS schema_name text;
CREATE TABLE IF NOT EXISTS migration_tasks(id text PRIMARY KEY,name text NOT NULL,source_datasource_id text NOT NULL,target_datasource_id text NOT NULL,mode text NOT NULL,status text NOT NULL,full_engine text NOT NULL,cdc_engine text,table_mappings jsonb NOT NULL DEFAULT '[]',chunk_rows bigint NOT NULL,batch_rows integer NOT NULL,parallelism integer NOT NULL,max_retries integer NOT NULL,auto_create_table boolean NOT NULL DEFAULT false,validation_enabled boolean NOT NULL DEFAULT false,validation_mode text NOT NULL DEFAULT 'CHUNK_CHECKSUM',read_limit_mbps bigint NOT NULL DEFAULT 0,write_limit_mbps bigint NOT NULL DEFAULT 0,progress double precision NOT NULL DEFAULT 0,total_chunks integer NOT NULL DEFAULT 0,finished_chunks integer NOT NULL DEFAULT 0,rows_migrated bigint NOT NULL DEFAULT 0,bytes_migrated bigint NOT NULL DEFAULT 0,speed_bytes_sec bigint NOT NULL DEFAULT 0,cdc_lag_ms bigint NOT NULL DEFAULT 0,last_error text,created_at timestamptz NOT NULL,updated_at timestamptz NOT NULL);
CREATE TABLE IF NOT EXISTS migration_tables(id text PRIMARY KEY,task_id text NOT NULL,source_schema text NOT NULL,source_table text NOT NULL,target_schema text NOT NULL,target_table text NOT NULL,primary_key text NOT NULL,target_primary_key text,primary_key_type text NOT NULL,columns_json jsonb NOT NULL,target_columns_json jsonb NOT NULL DEFAULT '[]',estimated_rows bigint NOT NULL DEFAULT 0,data_length bigint NOT NULL DEFAULT 0,min_pk bigint,max_pk bigint,total_chunks integer NOT NULL DEFAULT 0,finished_chunks integer NOT NULL DEFAULT 0,rows_migrated bigint NOT NULL DEFAULT 0,bytes_migrated bigint NOT NULL DEFAULT 0,status text NOT NULL);
CREATE TABLE IF NOT EXISTS migration_chunks(id text PRIMARY KEY,task_id text NOT NULL,table_id text NOT NULL,chunk_no integer NOT NULL,split_type text NOT NULL,primary_key text NOT NULL,start_value bigint NOT NULL,end_value bigint NOT NULL,start_cursor_json text,end_cursor_json text,cursor_json text,status text NOT NULL,worker_id text,lease_until timestamptz,rows_read bigint NOT NULL DEFAULT 0,rows_written bigint NOT NULL DEFAULT 0,bytes_read bigint NOT NULL DEFAULT 0,bytes_written bigint NOT NULL DEFAULT 0,retry_count integer NOT NULL DEFAULT 0,last_error text,started_at timestamptz,finished_at timestamptz,UNIQUE(task_id,table_id,chunk_no));
CREATE INDEX IF NOT EXISTS idx_chunks_claim ON migration_chunks(status,task_id,table_id,chunk_no);
CREATE TABLE IF NOT EXISTS workers(id text PRIMARY KEY,hostname text NOT NULL,cpu integer NOT NULL,memory_mb integer NOT NULL,status text NOT NULL,running_jobs integer NOT NULL,capabilities jsonb NOT NULL,last_heartbeat timestamptz NOT NULL);
CREATE TABLE IF NOT EXISTS cdc_positions(id text PRIMARY KEY,task_id text NOT NULL,database_type text,position_type text,position_value text,source_timestamp_ms bigint NOT NULL DEFAULT 0,apply_timestamp_ms bigint NOT NULL DEFAULT 0,lag_ms bigint NOT NULL DEFAULT 0,events_total bigint NOT NULL DEFAULT 0,events_pending bigint NOT NULL DEFAULT 0,recorded_at timestamptz NOT NULL);
CREATE INDEX IF NOT EXISTS idx_cdc_task_time ON cdc_positions(task_id,recorded_at DESC);
CREATE TABLE IF NOT EXISTS validation_results(id text PRIMARY KEY,task_id text NOT NULL,table_id text NOT NULL,chunk_id text NOT NULL,status text NOT NULL,source_rows bigint NOT NULL,target_rows bigint NOT NULL,source_checksum text,target_checksum text,last_error text,started_at timestamptz NOT NULL,finished_at timestamptz NOT NULL);
CREATE TABLE IF NOT EXISTS alerts(id text PRIMARY KEY,severity text NOT NULL,title text NOT NULL,message text NOT NULL,task_id text,acknowledged boolean NOT NULL DEFAULT false,created_at timestamptz NOT NULL);
CREATE TABLE IF NOT EXISTS audit_events(id text PRIMARY KEY,actor text NOT NULL,action text NOT NULL,resource_type text NOT NULL,resource_id text,detail text,remote_addr text,created_at timestamptz NOT NULL);

ALTER TABLE datasources ADD COLUMN IF NOT EXISTS jdbc_url text;
ALTER TABLE datasources ADD COLUMN IF NOT EXISTS driver_class text;
ALTER TABLE datasources ADD COLUMN IF NOT EXISTS cdc_url text;
ALTER TABLE cdc_positions ADD COLUMN IF NOT EXISTS direction text NOT NULL DEFAULT 'forward';

ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS cdc_start_timestamp_ms bigint NOT NULL DEFAULT 0;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS cdc_start_position_type text;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS cdc_start_position_value text;CREATE TABLE IF NOT EXISTS engine_jobs(
  id text PRIMARY KEY,
  task_id text NOT NULL,
  kind text NOT NULL,
  direction text NOT NULL,
  engine text NOT NULL,
  status text NOT NULL,
  worker_id text,
  lease_until timestamptz,
  retry_count integer NOT NULL DEFAULT 0,
  last_error text,
  started_at timestamptz,
  updated_at timestamptz NOT NULL,
  finished_at timestamptz
);
CREATE INDEX IF NOT EXISTS idx_engine_jobs_claim ON engine_jobs(status,engine,updated_at);
CREATE INDEX IF NOT EXISTS idx_engine_jobs_task ON engine_jobs(task_id,updated_at DESC);
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS cdc_start_resource text;
ALTER TABLE cdc_positions ADD COLUMN IF NOT EXISTS resource text;
CREATE TABLE IF NOT EXISTS users(
  id text PRIMARY KEY,
  username text NOT NULL UNIQUE,
  password_hash text NOT NULL,
  role text NOT NULL,
  enabled boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  last_login_at timestamptz
);

ALTER TABLE migration_tables ADD COLUMN IF NOT EXISTS engine text NOT NULL DEFAULT 'native';

ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS paused_from_status text;

ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS post_load_ddl_mode text NOT NULL DEFAULT 'INDEXES';

ALTER TABLE migration_tables ADD COLUMN IF NOT EXISTS indexes_json jsonb NOT NULL DEFAULT '[]';

ALTER TABLE migration_tables ADD COLUMN IF NOT EXISTS foreign_keys_json jsonb NOT NULL DEFAULT '[]';

ALTER TABLE workers ADD COLUMN IF NOT EXISTS cpu_usage_pct double precision NOT NULL DEFAULT 0;

ALTER TABLE workers ADD COLUMN IF NOT EXISTS memory_usage_pct double precision NOT NULL DEFAULT 0;

ALTER TABLE workers ADD COLUMN IF NOT EXISTS network_rx_bps bigint NOT NULL DEFAULT 0;

ALTER TABLE workers ADD COLUMN IF NOT EXISTS network_tx_bps bigint NOT NULL DEFAULT 0;

ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS rollback_cdc_engine text;

ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS cdc_ddl_mode text NOT NULL DEFAULT 'REJECT';

ALTER TABLE migration_chunks ADD COLUMN IF NOT EXISTS start_cursor_json text;
ALTER TABLE migration_chunks ADD COLUMN IF NOT EXISTS end_cursor_json text;
ALTER TABLE migration_chunks ADD COLUMN IF NOT EXISTS cursor_json text;
CREATE TABLE IF NOT EXISTS cdc_dead_letters(
  id text PRIMARY KEY,
  task_id text NOT NULL,
  direction text NOT NULL,
  position_type text,
  position_value text,
  resource text,
  events_json jsonb NOT NULL DEFAULT '[]',
  events_ciphertext text,
  last_error text NOT NULL,
  retry_count integer NOT NULL DEFAULT 0,
  status text NOT NULL,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  resolved_at timestamptz
);
CREATE INDEX IF NOT EXISTS idx_cdc_dead_letters_task ON cdc_dead_letters(task_id,status,updated_at DESC);

ALTER TABLE cdc_dead_letters ADD COLUMN IF NOT EXISTS events_ciphertext text;
CREATE TABLE IF NOT EXISTS cdc_conflicts(
  id text PRIMARY KEY,
  task_id text NOT NULL,
  direction text NOT NULL,
  source_schema text NOT NULL,
  source_table text NOT NULL,
  key_fingerprint text NOT NULL,
  policy text NOT NULL,
  decision text NOT NULL,
  source_version text,
  target_version text,
  position_type text,
  position_value text,
  created_at timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_cdc_conflicts_task ON cdc_conflicts(task_id,created_at DESC);
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS cdc_conflict_mode text NOT NULL DEFAULT 'SOURCE_WINS';
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS cdc_conflict_column text;

ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS sequence_synced_at timestamptz;

ALTER TABLE datasources ADD COLUMN IF NOT EXISTS tls_mode text;
ALTER TABLE datasources ADD COLUMN IF NOT EXISTS tls_server_name text;
ALTER TABLE datasources ADD COLUMN IF NOT EXISTS tls_ca_cert text;

ALTER TABLE migration_chunks ADD COLUMN IF NOT EXISTS last_read_ms bigint NOT NULL DEFAULT 0;
ALTER TABLE migration_chunks ADD COLUMN IF NOT EXISTS last_write_ms bigint NOT NULL DEFAULT 0;
ALTER TABLE migration_chunks ADD COLUMN IF NOT EXISTS last_batch_rows integer NOT NULL DEFAULT 0;
ALTER TABLE migration_chunks ADD COLUMN IF NOT EXISTS backpressure_level text NOT NULL DEFAULT '';

ALTER TABLE workers ADD COLUMN IF NOT EXISTS labels jsonb NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS worker_selector_json jsonb NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS worker_affinity text NOT NULL DEFAULT 'PREFERRED';

-- V0.14 dev4 topology discovery and task-level adaptive parallelism.
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS effective_parallelism integer NOT NULL DEFAULT 0;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS flow_control_level text NOT NULL DEFAULT 'NORMAL';
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS flow_control_reason text NOT NULL DEFAULT '';
ALTER TABLE migration_tables ADD COLUMN IF NOT EXISTS topology_json jsonb NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE migration_chunks ADD COLUMN IF NOT EXISTS placement_hint_json jsonb NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE migration_chunks ADD COLUMN IF NOT EXISTS topology_id text;
ALTER TABLE migration_chunks ADD COLUMN IF NOT EXISTS topology_kind text;
-- V1.0 feature-completion metadata: mTLS, advanced splitting, scheduled throttling and task logs.
ALTER TABLE datasources ADD COLUMN IF NOT EXISTS tls_client_cert text;
ALTER TABLE datasources ADD COLUMN IF NOT EXISTS tls_client_key_ciphertext text;

ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS rows_limit_per_sec bigint NOT NULL DEFAULT 0;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS qps_limit integer NOT NULL DEFAULT 0;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS rate_limit_timezone text NOT NULL DEFAULT 'Local';
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS rate_limit_windows jsonb NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS speed_rows_sec bigint NOT NULL DEFAULT 0;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS eta_seconds bigint NOT NULL DEFAULT 0;

ALTER TABLE migration_tables ADD COLUMN IF NOT EXISTS partitions_json jsonb NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE migration_tables ADD COLUMN IF NOT EXISTS split_strategy text NOT NULL DEFAULT 'AUTO';
ALTER TABLE migration_tables ADD COLUMN IF NOT EXISTS custom_where text NOT NULL DEFAULT '';
ALTER TABLE migration_tables ADD COLUMN IF NOT EXISTS hash_buckets integer NOT NULL DEFAULT 0;

ALTER TABLE migration_chunks ADD COLUMN IF NOT EXISTS partition_name text;
ALTER TABLE migration_chunks ADD COLUMN IF NOT EXISTS hash_bucket integer NOT NULL DEFAULT 0;
ALTER TABLE migration_chunks ADD COLUMN IF NOT EXISTS hash_buckets integer NOT NULL DEFAULT 0;
ALTER TABLE migration_chunks ADD COLUMN IF NOT EXISTS custom_where text NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS task_logs(
  id text PRIMARY KEY,
  task_id text NOT NULL,
  worker_id text,
  table_id text,
  chunk_id text,
  level text NOT NULL,
  message text NOT NULL,
  created_at timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_task_logs_task_time ON task_logs(task_id,created_at DESC);

-- V1.0.0 RC2 release hardening: durable metadata schema marker.
CREATE TABLE IF NOT EXISTS metadata_schema_state(
  id integer PRIMARY KEY CHECK (id = 1),
  schema_version text NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-unified-dev2', now())
ON CONFLICT (id) DO UPDATE
SET schema_version = EXCLUDED.schema_version, updated_at = EXCLUDED.updated_at;

-- V0.15 unified-dev3: declarative value transform policies.
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS transform_rules_json jsonb NOT NULL DEFAULT '[]'::jsonb;
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-unified-dev5', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;

-- V0.15 unified-dev6 durable encrypted CDC spool.
CREATE TABLE IF NOT EXISTS cdc_spool(
  sequence bigserial PRIMARY KEY,
  id text NOT NULL UNIQUE,
  task_id text NOT NULL,
  direction text NOT NULL,
  position_type text,
  position_value text,
  resource text,
  source_timestamp_ms bigint NOT NULL DEFAULT 0,
  event_count integer NOT NULL DEFAULT 0,
  payload_bytes bigint NOT NULL DEFAULT 0,
  events_json jsonb NOT NULL DEFAULT '[]'::jsonb,
  events_ciphertext text,
  status text NOT NULL DEFAULT 'PENDING',
  created_at timestamptz NOT NULL,
  applied_at timestamptz
);
CREATE INDEX IF NOT EXISTS idx_cdc_spool_pending ON cdc_spool(task_id,direction,status,sequence);
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-unified-dev6', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;

-- V0.15 unified-dev7: external file-backed CDC spool, HA drain lease and validation watermark barrier.
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS validation_barrier_position_type text;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS validation_barrier_position_value text;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS validation_barrier_resource text;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS validation_barrier_captured_at timestamptz;
CREATE TABLE IF NOT EXISTS cdc_spool_drain_leases(
  task_id text NOT NULL,
  direction text NOT NULL,
  owner text NOT NULL,
  lease_until timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  PRIMARY KEY(task_id,direction)
);
CREATE INDEX IF NOT EXISTS idx_cdc_spool_drain_leases_expiry ON cdc_spool_drain_leases(lease_until);
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-unified-dev7', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;


-- V0.15 unified-dev8: S3-compatible encrypted CDC spool backend uses existing opaque payload references.
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-unified-dev8', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;


-- V0.15 unified-dev9: S3 multipart/integrity references and archive-process hardening.
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-unified-dev9', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;

-- V0.15.0-rc29: predictive CDC-spool/full-load flow-control telemetry.
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS cdc_spool_growth_bytes_sec bigint NOT NULL DEFAULT 0;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS cdc_spool_critical_eta_seconds bigint NOT NULL DEFAULT 0;
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc29', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;

-- V0.15.0-rc30: task-global target throughput with worker-local pacing.
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS target_throughput_mbps bigint NOT NULL DEFAULT 0;
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc30', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;

-- RC31 production performance controller and adaptive hotspot telemetry.
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS auto_throughput_enabled boolean NOT NULL DEFAULT false;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS completion_sla_seconds bigint NOT NULL DEFAULT 0;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS sla_started_at timestamptz;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS controller_target_bytes_sec bigint NOT NULL DEFAULT 0;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS throughput_controller_reason text;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS adaptive_hotspot_splits bigint NOT NULL DEFAULT 0;
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc31', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;

-- RC32 running hot-chunk rebalance and persistent controller learning.
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS adaptive_running_yields bigint NOT NULL DEFAULT 0;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS adaptive_topology_drains bigint NOT NULL DEFAULT 0;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS controller_auto_probe_pct integer NOT NULL DEFAULT 0;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS controller_sla_headroom_pct integer NOT NULL DEFAULT 0;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS controller_learning_samples bigint NOT NULL DEFAULT 0;
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc32', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;

ALTER TABLE migration_tables ADD COLUMN IF NOT EXISTS profile_bytes_per_sec BIGINT NOT NULL DEFAULT 0;
ALTER TABLE migration_tables ADD COLUMN IF NOT EXISTS profile_rows_per_sec BIGINT NOT NULL DEFAULT 0;
ALTER TABLE migration_tables ADD COLUMN IF NOT EXISTS recommended_chunk_rows BIGINT NOT NULL DEFAULT 0;
ALTER TABLE migration_tables ADD COLUMN IF NOT EXISTS performance_samples BIGINT NOT NULL DEFAULT 0;
ALTER TABLE migration_tables ADD COLUMN IF NOT EXISTS topology_performance_json JSONB NOT NULL DEFAULT '{}'::jsonb;
UPDATE metadata_schema_state SET schema_version='0.15.0-rc34', updated_at=now() WHERE id=1;


-- RC35 topology circuit scheduling and tail-risk SLA prediction.
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS sla_p95_eta_seconds bigint NOT NULL DEFAULT 0;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS sla_p99_eta_seconds bigint NOT NULL DEFAULT 0;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS sla_risk_level text;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS sla_risk_reason text;
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc36', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;


-- RC37 running DEGRADED topology convergence and cooperative throttle telemetry.
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS adaptive_topology_degraded_yields bigint NOT NULL DEFAULT 0;
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc37', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;

-- RC38 topology recovery hysteresis and staged concurrency are persisted inside
-- topology_performance_json (good_streak / recovery_concurrency_cap).
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc38', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;


-- RC39 fault-domain cascading protection. Canonical region/zone/rack metadata is
-- denormalized onto chunks so hot-path scheduling does not need connector calls.
ALTER TABLE migration_chunks ADD COLUMN IF NOT EXISTS fault_domain_json JSONB NOT NULL DEFAULT '{}'::jsonb;
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc39', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;

-- RC40 fault-domain already-running concurrency convergence. Excess safe-resume
-- chunks cooperatively yield at durable batch boundaries and preserve domain metadata.
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS adaptive_fault_domain_yields bigint NOT NULL DEFAULT 0;
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc40', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;

-- RC41 long-running control-plane metadata retention. Operational history is
-- pruned in small batches; the newest CDC position per task+direction is always retained.
CREATE INDEX IF NOT EXISTS idx_task_logs_task_time_id ON task_logs(task_id,created_at DESC,id DESC);
CREATE INDEX IF NOT EXISTS idx_audit_events_time_id ON audit_events(created_at DESC,id DESC);
CREATE INDEX IF NOT EXISTS idx_cdc_positions_task_direction_time_id ON cdc_positions(task_id,direction,recorded_at DESC,id DESC);
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc41', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;

-- RC42 bounded chunk hot paths and metadata bloat observability.
CREATE INDEX IF NOT EXISTS idx_chunks_task_chunk_no_desc ON migration_chunks(task_id, chunk_no DESC);
CREATE INDEX IF NOT EXISTS idx_chunks_pending_table_hot ON migration_chunks(task_id, table_id, chunk_no, id) WHERE status='PENDING';
CREATE INDEX IF NOT EXISTS idx_chunks_running_topology_hot ON migration_chunks(task_id, topology_id, started_at, chunk_no, id) WHERE status='RUNNING';
CREATE INDEX IF NOT EXISTS idx_chunks_running_fault_rack_hot ON migration_chunks(task_id, (fault_domain_json->>'rack'), started_at, chunk_no, id) WHERE status='RUNNING';
CREATE INDEX IF NOT EXISTS idx_chunks_running_fault_zone_hot ON migration_chunks(task_id, (fault_domain_json->>'zone'), started_at, chunk_no, id) WHERE status='RUNNING';
CREATE INDEX IF NOT EXISTS idx_chunks_running_fault_region_hot ON migration_chunks(task_id, (fault_domain_json->>'region'), started_at, chunk_no, id) WHERE status='RUNNING';
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc42', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;

-- RC43 bounded validation paging and repository-side latest-result coverage.
CREATE INDEX IF NOT EXISTS idx_chunks_validation_table_page ON migration_chunks(task_id, table_id, chunk_no, id);
CREATE INDEX IF NOT EXISTS idx_validation_task_chunk_latest ON validation_results(task_id, chunk_id, finished_at DESC, id DESC) INCLUDE (status);
CREATE INDEX IF NOT EXISTS idx_validation_task_status_chunk ON validation_results(task_id, status, chunk_id, finished_at DESC);
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc43', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;

-- RC44 streaming complex-validation descriptors and validation-result lifecycle.
CREATE INDEX IF NOT EXISTS idx_validation_finished_id ON validation_results(finished_at, id);
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc44', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;

-- RC45 immutable validation audit archive. Per-chunk validation details may be
-- compacted later, but this task/table summary is insert-only and permanent.
CREATE TABLE IF NOT EXISTS validation_archives(
  task_id text PRIMARY KEY,
  terminal_status text NOT NULL,
  validation_mode text NOT NULL DEFAULT '',
  validation_barrier_position_type text,
  validation_barrier_position_value text,
  validation_barrier_resource text,
  total_tables integer NOT NULL,
  total_chunks integer NOT NULL,
  covered_chunks integer NOT NULL,
  success_chunks integer NOT NULL,
  mismatch_chunks integer NOT NULL,
  error_chunks integer NOT NULL,
  missing_chunks integer NOT NULL,
  evidence_digest text NOT NULL,
  tables_json jsonb NOT NULL DEFAULT '[]'::jsonb,
  archived_at timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_validation_archives_archived_at ON validation_archives(archived_at DESC);
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc45', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;


-- RC46 validation acceptance report delivery is metadata-schema compatible.
-- Reports are deterministically derived from immutable validation_archives;
-- external S3/WORM objects remain outside the metadata database.
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc46', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;

-- RC47 permanently registers externally committed validation-report archives.
-- Records are immutable per task+evidence digest and are never janitor-pruned.
CREATE TABLE IF NOT EXISTS validation_report_archives(
  task_id text NOT NULL,
  evidence_digest text NOT NULL,
  uri text NOT NULL,
  bucket text,
  prefix text,
  manifest_sha256 text NOT NULL,
  public_signature_algorithm text,
  public_signature_key_id text,
  public_key_ed25519 text,
  public_key_fingerprint_sha256 text,
  object_lock_mode text,
  retain_until text,
  legal_hold boolean NOT NULL DEFAULT false,
  committed_at timestamptz NOT NULL,
  PRIMARY KEY(task_id,evidence_digest)
);
CREATE INDEX IF NOT EXISTS idx_validation_report_archives_committed_at ON validation_report_archives(committed_at DESC);
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc49', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
