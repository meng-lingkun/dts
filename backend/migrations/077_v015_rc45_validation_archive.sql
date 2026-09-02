-- QMigration V0.15.0-rc45: immutable terminal-task validation archive.
-- Detailed validation_results may be compacted after retention, but this
-- insert-only task/table evidence summary remains permanently auditable.
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
