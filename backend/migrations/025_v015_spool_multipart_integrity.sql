-- V0.15 unified-dev9 adds S3 multipart upload and integrity-tagged opaque
-- spool references at the repository wrapper layer. No table columns are
-- required; advance the schema marker so mixed-version control planes fail
-- readiness instead of silently interpreting storage references differently.
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-unified-dev9', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
