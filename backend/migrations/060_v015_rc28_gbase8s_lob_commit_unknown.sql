-- V0.15.0 RC28 adds the GBase 8s event-owned smart BLOB/CLOB CDC
-- provider contract and the durable COMMIT_UNCERTAIN CDC dead-letter state.
-- cdc_dead_letters.status is text, so no physical column change is required.
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc28', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
