-- QMigration V0.15.0-rc37: converge already-running Full-load work to the
-- configured DEGRADED topology concurrency cap and expose cooperative-yield telemetry.
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS adaptive_topology_degraded_yields bigint NOT NULL DEFAULT 0;
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc37', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
