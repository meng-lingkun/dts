-- V0.15 unified-dev11 adds the QMigration-owned Oracle TTC protocol/datatype
-- negotiation and password-authentication wire codecs behind explicit deep-
-- probe gates.  Oracle SQL execution, metadata/full-load and Redo CDC remain
-- capability-gated until qualification against a real Oracle database.
-- No persistent columns are required for this protocol-layer increment.
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-unified-dev11', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
