-- Apply after 084, including on installations already marked RC50.
DO $dts_pat_upgrade$
BEGIN
  IF (SELECT schema_version FROM metadata_schema_state WHERE id=1) IS DISTINCT FROM '0.15.0-rc50'
     OR to_regclass('dts_user_sessions') IS NULL THEN
    RAISE EXCEPTION 'personal token migration requires completed RC50 migration 084';
  END IF;
END;
$dts_pat_upgrade$;
CREATE TABLE IF NOT EXISTS dts_personal_tokens(
  id text PRIMARY KEY,
  user_id text NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  name text NOT NULL,
  created_at timestamptz NOT NULL,
  expires_at timestamptz NOT NULL CHECK(expires_at > created_at)
);
CREATE INDEX IF NOT EXISTS idx_dts_personal_tokens_owner ON dts_personal_tokens(user_id, expires_at);
