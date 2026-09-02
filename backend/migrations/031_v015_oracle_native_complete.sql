-- V0.15 unified-dev15-complete closes the QMigration-owned Oracle Native
-- software data plane: stream-aware TTC SQL, input/array binds, prepared DML,
-- Full Writer including large BLOB/CLOB, schema/post-load DDL and transactional
-- CDC apply. Real Oracle version/charset E2E qualification remains explicit and
-- is still protected by experimental capability gates. No new columns needed.
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-unified-dev15-complete', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
