ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS cdc_conflict_mode text NOT NULL DEFAULT 'SOURCE_WINS';
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS cdc_conflict_column text;
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
