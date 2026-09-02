-- V0.15.0 RC27 adds qualification-gated GBase 8a target CDC apply and
-- deterministic in-process CDC chaos/failpoints. No new persistent columns are
-- required because the capability gate and fault plan are process-local.
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc27', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
