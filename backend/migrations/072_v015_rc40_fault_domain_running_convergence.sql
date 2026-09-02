-- RC40: converge already-running chunks inside a risky rack/zone/region to the
-- same domain cap used by new-claim admission; expose cooperative-yield telemetry.
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS adaptive_fault_domain_yields bigint NOT NULL DEFAULT 0;
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc40', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
