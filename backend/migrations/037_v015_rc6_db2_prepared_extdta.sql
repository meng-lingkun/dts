-- V0.15.0 RC6 replaces DB2 target row-literal DML with native prepared
-- SQLDTA/EXTDTA execution and generic multi-segment DRDA object framing.
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc6', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
