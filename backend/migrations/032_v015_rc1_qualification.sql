-- V0.15.0 RC1 adds the Oracle Native qualification/diagnostic release harness
-- and truthful archive verification metadata. No persistent schema columns are
-- required; this marker records the release-candidate software baseline.
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc1', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
