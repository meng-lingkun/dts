-- Additive columns; all API servers must be upgraded together to enforce CAS.
ALTER TABLE users ADD COLUMN IF NOT EXISTS revision bigint NOT NULL DEFAULT 0;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS revision bigint NOT NULL DEFAULT 0;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS cutover_operation jsonb;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS rollback_operation jsonb;
ALTER TABLE migration_chunks ADD COLUMN IF NOT EXISTS lease_generation bigint NOT NULL DEFAULT 0;
ALTER TABLE engine_jobs ADD COLUMN IF NOT EXISTS lease_generation bigint NOT NULL DEFAULT 0;
ALTER TABLE cdc_spool_drain_leases ADD COLUMN IF NOT EXISTS generation bigint NOT NULL DEFAULT 0;

INSERT INTO metadata_schema_state(id,schema_version,updated_at)
VALUES(1,'0.15.0-rc50',now())
ON CONFLICT(id) DO UPDATE SET schema_version=EXCLUDED.schema_version,updated_at=EXCLUDED.updated_at;
