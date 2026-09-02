-- V0.15.0 RC16 adds qualification-gated GaussDB DDL-only source replay on top
-- of the RC15 byte-safe binary DML path. No persistent columns are required;
-- durable checkpoints remain GAUSSDB_LSN.
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc16', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
