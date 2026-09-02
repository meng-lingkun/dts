-- V0.15.0 RC12 completes the DB2 VECTOR target software path: catalog
-- dimension/coordinate metadata, schema restoration and prepared VECTOR() apply.
-- No new persistent columns are required.
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc12', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
