-- V0.15.0 RC24 restores GBase 8s CDC observability and adds capture-lineage fencing.
INSERT INTO schema_versions(id, version, applied_at)
VALUES (1, '0.15.0-rc24', now())
ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, applied_at = EXCLUDED.applied_at;
