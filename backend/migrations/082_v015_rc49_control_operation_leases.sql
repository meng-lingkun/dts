CREATE TABLE IF NOT EXISTS control_operation_leases(
  task_id text PRIMARY KEY REFERENCES migration_tasks(id) ON DELETE CASCADE,
  operation text NOT NULL,
  owner_id text NOT NULL,
  lease_until timestamptz NOT NULL,
  updated_at timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_control_operation_leases_expiry
  ON control_operation_leases(lease_until);

INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc49', now())
ON CONFLICT (id) DO UPDATE
SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
