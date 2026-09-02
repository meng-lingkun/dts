-- V0.15.0 RC25 adds TiCDC multi-partition/TLS/SASL runtime contracts and exact TSO validation snapshots.
-- No persistent table columns are required; TIDB_TSO partition offsets remain encoded in PositionValue.
INSERT INTO schema_versions(id, version, applied_at)
VALUES (1, '0.15.0-rc25', now())
ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, applied_at = EXCLUDED.applied_at;
