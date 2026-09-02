-- V0.15.0 RC10 extends DB2 source CDC with documented update-relocation
-- preceding-INSERT linkage and lrIUDflags=0x8000 decomposed-update pairing.
-- No new persistent columns are required.
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc10', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
