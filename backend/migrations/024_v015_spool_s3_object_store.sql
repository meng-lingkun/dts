-- V0.15 unified-dev8 adds S3-compatible CDC spool storage at the repository
-- wrapper layer. No table columns are required because cdc_spool already
-- persists an opaque encrypted payload reference. Advance only the metadata
-- schema marker so /readyz prevents mixed-version control planes.
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-unified-dev8', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
