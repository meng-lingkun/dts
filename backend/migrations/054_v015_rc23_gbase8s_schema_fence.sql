-- V0.15.0 RC23 adds GBase 8s CDC schema-fence protocol enforcement.
INSERT INTO schema_versions(id, version, applied_at)
VALUES (1, '0.15.0-rc23', now())
ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, applied_at = EXCLUDED.applied_at;
