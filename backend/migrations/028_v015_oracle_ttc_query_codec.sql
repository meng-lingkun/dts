-- V0.15 unified-dev12 adds the QMigration-owned experimental Oracle TTC
-- bind-free SELECT/OALL8 request, describe metadata and scalar row codecs behind
-- an explicit query deep-probe gate. Production metadata/full-read/full-write
-- and Redo CDC capabilities remain disabled until real-Oracle qualification.
-- No persistent columns are required for this protocol-layer increment.
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-unified-dev12', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
