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
