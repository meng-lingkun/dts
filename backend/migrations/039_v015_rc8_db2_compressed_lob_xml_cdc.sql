-- V0.15.0 RC8 extends DB2 source CDC decoding for documented VALUE COMPRESSION
-- rows, logged out-of-row LOB/varying data and serialized XML CSL records.
-- No new persistent columns are required.
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc8', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
