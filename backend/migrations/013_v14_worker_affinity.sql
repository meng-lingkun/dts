-- V0.14: topology-aware worker affinity. Labels are intentionally generic so
-- deployments can express zone/rack/region/network-segment or product topology.
ALTER TABLE workers ADD COLUMN IF NOT EXISTS labels jsonb NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS worker_selector_json jsonb NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS worker_affinity text NOT NULL DEFAULT 'PREFERRED';
