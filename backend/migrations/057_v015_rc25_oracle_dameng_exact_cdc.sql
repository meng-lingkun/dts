-- V0.15.0 RC25 extends exact-watermark validation to Oracle SCN and DM_LSN and adds the experimental DM8 DBMS_LOGMNR source CDC software path.
-- No persistent columns are required; DM_LSN and ORACLE_SCN remain encoded in CDC position rows.
INSERT INTO schema_versions(id, version, applied_at)
VALUES (1, '0.15.0-rc25', now())
ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, applied_at = EXCLUDED.applied_at;
