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
