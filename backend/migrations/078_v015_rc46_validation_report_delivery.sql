-- QMigration V0.15.0-rc46: validation acceptance report delivery.
-- JSON/HTML/PDF artifacts are derived from immutable validation_archives.
-- S3/Object-Lock report objects are external and do not change the archive row.
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc46', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
