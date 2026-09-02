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
