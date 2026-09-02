-- V0.15.0 RC20 adds qualification-gated GBase 8s syscdcv1/CSDK source CDC.
-- The durable position is GBASE8S_CDC_SEQ with restart=<earliest-open-BEGIN>
-- and commit=<last-applied-COMMIT>; no schema change is required beyond the
-- existing generic CDC position columns.
INSERT INTO schema_versions(id, version, applied_at)
VALUES (1, '0.15.0-rc20', now())
ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, applied_at = EXCLUDED.applied_at;
