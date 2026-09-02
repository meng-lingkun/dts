-- V0.15.0 RC26 adds qualification-gated product-native source CDC for
-- openGauss (mppdb_decoding/OPENGAUSS_LSN) and KingbaseES
-- (sys_* logical slots + kboutput/KINGBASE_LSN). No new persistent columns are
-- required because the existing CDC position model is protocol-neutral.
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc26', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
