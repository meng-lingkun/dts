-- QMigration V0.15.0-unified-dev5
-- Native SQL Server planner/CDC hardening + Oracle TCPS/TNS DATA transport release marker.
-- No new durable columns are required by this release; advancing the metadata
-- schema marker keeps /readyz and controlled upgrades version-safe.
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-unified-dev5', now())
ON CONFLICT (id) DO UPDATE
SET schema_version = EXCLUDED.schema_version,
    updated_at = EXCLUDED.updated_at;
