-- QMigration V0.8 schema-object migration state
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS sequence_synced_at timestamptz;
