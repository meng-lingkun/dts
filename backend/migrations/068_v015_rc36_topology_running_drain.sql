-- QMigration V0.15.0-rc36: cooperative drain of already-running Full-load
-- chunks after their topology enters CIRCUIT_OPEN. Workers stop only after a
-- committed batch and durable cursor; pending remainders retain topology
-- binding and remain blocked until the circuit recovers.
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS adaptive_topology_drains bigint NOT NULL DEFAULT 0;
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc36', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
