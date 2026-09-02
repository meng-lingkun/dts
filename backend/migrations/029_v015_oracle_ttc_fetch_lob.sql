-- V0.15 unified-dev13 hardens the experimental QMigration-owned Oracle TTC
-- SQL path with coalesced-message decoding, OER/Summary cursor state, fetch
-- continuation, ROWID decoding and bounded LOB locator/chunk primitives.
-- Production Oracle metadata/full-read/full-write/CDC capabilities remain
-- disabled until real-Oracle qualification. No persistent columns are needed.
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-unified-dev13', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
