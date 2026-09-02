-- QMigration V0.15.0-rc35: topology circuit scheduling and P95/P99 SLA tail-risk telemetry.
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS sla_p95_eta_seconds bigint NOT NULL DEFAULT 0;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS sla_p99_eta_seconds bigint NOT NULL DEFAULT 0;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS sla_risk_level text;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS sla_risk_reason text;
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc35', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
