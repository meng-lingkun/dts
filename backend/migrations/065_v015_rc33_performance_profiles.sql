ALTER TABLE migration_tables ADD COLUMN IF NOT EXISTS profile_bytes_per_sec BIGINT NOT NULL DEFAULT 0;
ALTER TABLE migration_tables ADD COLUMN IF NOT EXISTS profile_rows_per_sec BIGINT NOT NULL DEFAULT 0;
ALTER TABLE migration_tables ADD COLUMN IF NOT EXISTS recommended_chunk_rows BIGINT NOT NULL DEFAULT 0;
ALTER TABLE migration_tables ADD COLUMN IF NOT EXISTS performance_samples BIGINT NOT NULL DEFAULT 0;
ALTER TABLE migration_tables ADD COLUMN IF NOT EXISTS topology_performance_json JSONB NOT NULL DEFAULT '{}'::jsonb;
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc33', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
