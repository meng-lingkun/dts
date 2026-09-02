-- V0.15.0 RC11 extends DB2 source CDC with uniquely-owned out-of-row
-- multi-insert reconstruction and Db2 12.1.4+ DMS 213 VECTOR source values.
-- No new persistent columns are required.
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc11', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
