-- QMigration V0.15.0-rc44: streaming complex-validation descriptors and
-- bounded validation-result lifecycle maintenance.
CREATE INDEX IF NOT EXISTS idx_validation_finished_id ON validation_results(finished_at, id);
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc44', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
