-- V0.15.0 RC22 adds documented GBase 8s transactional CDC_REC_TRUNCATE
-- decoding and target transactional TRUNCATE replay. No metadata schema change
-- is required; the migration records the software contract level.
INSERT INTO schema_versions(id, version, applied_at)
VALUES (1, '0.15.0-rc22', now())
ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, applied_at = EXCLUDED.applied_at;
