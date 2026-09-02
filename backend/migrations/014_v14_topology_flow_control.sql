ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS effective_parallelism integer NOT NULL DEFAULT 0;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS flow_control_level text NOT NULL DEFAULT 'NORMAL';
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS flow_control_reason text NOT NULL DEFAULT '';

ALTER TABLE migration_tables ADD COLUMN IF NOT EXISTS topology_json jsonb NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE migration_chunks ADD COLUMN IF NOT EXISTS placement_hint_json jsonb NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE migration_chunks ADD COLUMN IF NOT EXISTS topology_id text;
ALTER TABLE migration_chunks ADD COLUMN IF NOT EXISTS topology_kind text;
