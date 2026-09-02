ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS adaptive_running_yields BIGINT NOT NULL DEFAULT 0;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS controller_auto_probe_pct INTEGER NOT NULL DEFAULT 0;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS controller_sla_headroom_pct INTEGER NOT NULL DEFAULT 0;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS controller_learning_samples BIGINT NOT NULL DEFAULT 0;
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc32', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
