ALTER TABLE datasources ADD COLUMN IF NOT EXISTS schema_name text;
-- QMigration V0.3 metadata additions for deployments that choose PostgreSQL
-- as the control-plane repository in a later adapter.
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS auto_create_table boolean NOT NULL DEFAULT false;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS validation_enabled boolean NOT NULL DEFAULT false;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS validation_mode text NOT NULL DEFAULT 'CHUNK_CHECKSUM';
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS read_limit_mbps bigint NOT NULL DEFAULT 0;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS write_limit_mbps bigint NOT NULL DEFAULT 0;
ALTER TABLE migration_tables ADD COLUMN IF NOT EXISTS target_primary_key text;
ALTER TABLE migration_tables ADD COLUMN IF NOT EXISTS target_columns_json jsonb NOT NULL DEFAULT '[]'::jsonb;

CREATE TABLE IF NOT EXISTS validation_results (
  id text PRIMARY KEY,
  task_id text NOT NULL REFERENCES migration_tasks(id),
  table_id text NOT NULL REFERENCES migration_tables(id),
  chunk_id text NOT NULL REFERENCES migration_chunks(id),
  status text NOT NULL,
  source_rows bigint NOT NULL DEFAULT 0,
  target_rows bigint NOT NULL DEFAULT 0,
  source_checksum text,
  target_checksum text,
  last_error text,
  started_at timestamptz NOT NULL,
  finished_at timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_validation_results_task ON validation_results(task_id, status);

CREATE TABLE IF NOT EXISTS alerts (
  id text PRIMARY KEY,
  severity text NOT NULL,
  title text NOT NULL,
  message text NOT NULL,
  task_id text,
  acknowledged boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_events (
  id text PRIMARY KEY,
  actor text NOT NULL,
  action text NOT NULL,
  resource_type text NOT NULL,
  resource_id text,
  detail text,
  remote_addr text,
  created_at timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_audit_events_created_at ON audit_events(created_at DESC);
