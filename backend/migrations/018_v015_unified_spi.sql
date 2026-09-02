-- QMigration V0.15.0-unified-dev2
-- Unified Connector Capability SPI / Native CDC Runtime release marker.
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-unified-dev2', now())
ON CONFLICT (id) DO UPDATE
SET schema_version = EXCLUDED.schema_version,
    updated_at = EXCLUDED.updated_at;
