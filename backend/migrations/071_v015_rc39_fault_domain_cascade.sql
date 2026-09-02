-- QMigration V0.15.0-rc39: topology fault-domain cascading protection.
ALTER TABLE migration_chunks ADD COLUMN IF NOT EXISTS fault_domain_json JSONB NOT NULL DEFAULT '{}'::jsonb;
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc39', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
