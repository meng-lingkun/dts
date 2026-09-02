-- V0.15.0 RC18 hardens GBase 8a Full Write by requiring a HASH-distributed
-- target whose distribution columns are contained in the stable migration key.
-- No new CDC persistence is introduced because GBase source CDC remains disabled.
INSERT INTO schema_versions(id, version, applied_at)
VALUES (1, '0.15.0-rc18', now())
ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, applied_at = EXCLUDED.applied_at;
