-- V0.15.0 RC14 adds qualification-gated GaussDB PostgreSQL-wire Full/target
-- support and SQL logical-decoding CDC with durable GAUSSDB_LSN checkpoints.
-- No new persistent columns are required.
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc14', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
