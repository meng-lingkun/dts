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
