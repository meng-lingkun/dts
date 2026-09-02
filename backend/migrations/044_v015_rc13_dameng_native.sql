-- V0.15.0 RC13 adds the qualification-gated QMigration Dameng data plane:
-- metadata/full read, schema/full write and transactional target apply through
-- a vendor database/sql provider. No new persistent columns are required.
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc13', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
