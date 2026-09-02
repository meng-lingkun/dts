-- QMigration V0.15.0-unified-dev4
-- SQL Server Native TDS/TLS + CDC/LSN runtime release marker.
-- No new task columns are required: datasource TLS and CDC position models are
-- already generic. Advancing the schema version keeps /readyz upgrade-safe.
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-unified-dev4', now())
ON CONFLICT (id) DO UPDATE
SET schema_version = EXCLUDED.schema_version,
    updated_at = EXCLUDED.updated_at;
