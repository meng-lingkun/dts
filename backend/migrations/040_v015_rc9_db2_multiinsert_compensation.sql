-- V0.15.0 RC9 extends DB2 source CDC with documented multi-insert decoding
-- and row-level compensation/savepoint net-effect reconstruction.
-- No new persistent columns are required.
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc9', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
