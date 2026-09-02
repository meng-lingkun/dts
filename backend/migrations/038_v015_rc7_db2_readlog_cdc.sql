-- V0.15.0 RC7 adds the QMigration DB2 source CDC contract:
-- DB2_LRI durable positions, QMigration DB2 Log Agent and db2ReadLog-backed
-- propagatable log capture. No new persistent columns are required.
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc7', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
