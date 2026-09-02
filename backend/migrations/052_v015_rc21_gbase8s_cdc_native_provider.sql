-- V0.15.0 RC21 replaces the preferred GBase 8s CDC provider integration with
-- a stable native C ABI loaded by the datasource-local CDC agent. No metadata
-- schema change is required; the migration records the software contract level.
INSERT INTO schema_versions(id, version, applied_at)
VALUES (1, '0.15.0-rc21', now())
ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, applied_at = EXCLUDED.applied_at;
