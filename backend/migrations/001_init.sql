-- QMigration metadata schema (PostgreSQL target repository).
-- Runtime V0.2 still defaults to Memory Repository; this schema tracks the
-- persistent model used by the control plane.
CREATE TABLE IF NOT EXISTS datasources (
  id text PRIMARY KEY,
  name text NOT NULL,
  type text NOT NULL,
  host text NOT NULL,
  port integer NOT NULL,
  username text NOT NULL,
  password_ciphertext text NOT NULL,
  database_name text,
  schema_name text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS migration_tasks (
  id text PRIMARY KEY,
  name text NOT NULL,
  source_datasource_id text NOT NULL REFERENCES datasources(id),
  target_datasource_id text NOT NULL REFERENCES datasources(id),
  mode text NOT NULL,
  status text NOT NULL,
  full_engine text NOT NULL,
  cdc_engine text,
  table_mappings jsonb NOT NULL DEFAULT '[]'::jsonb,
  chunk_rows bigint NOT NULL DEFAULT 100000,
  batch_rows integer NOT NULL DEFAULT 500,
  parallelism integer NOT NULL DEFAULT 4,
  max_retries integer NOT NULL DEFAULT 3,
  progress numeric(7,3) NOT NULL DEFAULT 0,
  total_chunks integer NOT NULL DEFAULT 0,
  finished_chunks integer NOT NULL DEFAULT 0,
  rows_migrated bigint NOT NULL DEFAULT 0,
  bytes_migrated bigint NOT NULL DEFAULT 0,
  speed_bytes_sec bigint NOT NULL DEFAULT 0,
  cdc_lag_ms bigint NOT NULL DEFAULT 0,
  last_error text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_migration_tasks_status ON migration_tasks(status);

CREATE TABLE IF NOT EXISTS migration_tables (
  id text PRIMARY KEY,
  task_id text NOT NULL REFERENCES migration_tasks(id),
  source_schema text NOT NULL,
  source_table text NOT NULL,
  target_schema text NOT NULL,
  target_table text NOT NULL,
  primary_key text NOT NULL,
  primary_key_type text NOT NULL,
  columns_json jsonb NOT NULL,
  estimated_rows bigint NOT NULL DEFAULT 0,
  data_length bigint NOT NULL DEFAULT 0,
  min_pk bigint,
  max_pk bigint,
  total_chunks integer NOT NULL DEFAULT 0,
  finished_chunks integer NOT NULL DEFAULT 0,
  rows_migrated bigint NOT NULL DEFAULT 0,
  bytes_migrated bigint NOT NULL DEFAULT 0,
  status text NOT NULL DEFAULT 'READY',
  UNIQUE(task_id, source_schema, source_table)
);

CREATE TABLE IF NOT EXISTS migration_chunks (
  id text PRIMARY KEY,
  task_id text NOT NULL REFERENCES migration_tasks(id),
  table_id text NOT NULL REFERENCES migration_tables(id),
  chunk_no integer NOT NULL,
  split_type text NOT NULL,
  primary_key text NOT NULL,
  start_value bigint NOT NULL,
  end_value bigint NOT NULL,
  status text NOT NULL DEFAULT 'PENDING',
  worker_id text,
  lease_until timestamptz,
  rows_read bigint NOT NULL DEFAULT 0,
  rows_written bigint NOT NULL DEFAULT 0,
  bytes_read bigint NOT NULL DEFAULT 0,
  bytes_written bigint NOT NULL DEFAULT 0,
  retry_count integer NOT NULL DEFAULT 0,
  last_error text,
  started_at timestamptz,
  finished_at timestamptz,
  UNIQUE(task_id, table_id, chunk_no)
);
CREATE INDEX IF NOT EXISTS idx_migration_chunks_claim ON migration_chunks(status, task_id, table_id, chunk_no);
CREATE INDEX IF NOT EXISTS idx_migration_chunks_lease ON migration_chunks(status, lease_until);

CREATE TABLE IF NOT EXISTS workers (
  id text PRIMARY KEY,
  hostname text NOT NULL,
  cpu integer NOT NULL,
  memory_mb integer NOT NULL DEFAULT 0,
  status text NOT NULL,
  running_jobs integer NOT NULL DEFAULT 0,
  capabilities jsonb NOT NULL DEFAULT '[]'::jsonb,
  last_heartbeat timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS cdc_positions (
  id bigserial PRIMARY KEY,
  task_id text NOT NULL REFERENCES migration_tasks(id),
  database_type text NOT NULL,
  position_type text NOT NULL,
  position_value text NOT NULL,
  source_timestamp_ms bigint,
  recorded_at timestamptz NOT NULL DEFAULT now()
);
