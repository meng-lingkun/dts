ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS cdc_spool_growth_bytes_sec bigint NOT NULL DEFAULT 0;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS cdc_spool_critical_eta_seconds bigint NOT NULL DEFAULT 0;
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc29', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
