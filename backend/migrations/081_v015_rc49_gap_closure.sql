-- QMigration V0.15.0-rc49 software gap-closure marker.
-- This release primarily adds protocol/provider/runtime capabilities and does
-- not persist private HSM/KMS/TSA material in metadata.
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc49', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
