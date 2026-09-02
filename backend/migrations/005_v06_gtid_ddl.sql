-- QMigration V0.6 native MySQL GTID recovery and safe same-family DDL replay.
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS cdc_ddl_mode text NOT NULL DEFAULT 'REJECT';
