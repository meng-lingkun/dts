-- V0.15.0 RC15 moves GaussDB source CDC from JSON logical decoding to the
-- documented byte-safe mppdb_decoding binary SQL functions. No new persistent
-- columns are required; durable checkpoints remain GAUSSDB_LSN.
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc15', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
