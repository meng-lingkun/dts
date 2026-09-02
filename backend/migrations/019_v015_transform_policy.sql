-- QMigration V0.15.0-unified-dev3
-- Durable task-scoped Transform Policy DSL.
ALTER TABLE migration_tasks
  ADD COLUMN IF NOT EXISTS transform_rules_json jsonb NOT NULL DEFAULT '[]'::jsonb;

INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-unified-dev3', now())
ON CONFLICT (id) DO UPDATE
SET schema_version = EXCLUDED.schema_version,
    updated_at = EXCLUDED.updated_at;
