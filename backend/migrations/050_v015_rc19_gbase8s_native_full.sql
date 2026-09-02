-- V0.15.0 RC19 adds a qualification-gated GBase 8s V8.8 Full/target data
-- plane over a vendor Client-SDK ODBC database/sql transport provider.
-- No source CDC checkpoint state is introduced because RC19 intentionally
-- does not advertise a durable GBase 8s source CDC reader/ACK contract.
INSERT INTO schema_versions(id, version, applied_at)
VALUES (1, '0.15.0-rc19', now())
ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, applied_at = EXCLUDED.applied_at;
