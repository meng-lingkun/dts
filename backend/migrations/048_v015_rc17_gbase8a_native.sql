-- V0.15.0 RC17 adds a qualification-gated GBase 8a MPP Full data plane.
-- No persistent CDC state is introduced because RC17 intentionally does not
-- advertise GBase 8a source CDC or transactional target CDC apply.
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc17', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
