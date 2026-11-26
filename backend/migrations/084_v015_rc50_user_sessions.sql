-- Apply after 083, including on an existing RC50 installation.
CREATE TABLE IF NOT EXISTS dts_user_sessions(
  id text PRIMARY KEY,
  user_id text NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  created_at timestamptz NOT NULL,
  expires_at timestamptz NOT NULL,
  revoked boolean NOT NULL DEFAULT false
);
CREATE INDEX IF NOT EXISTS idx_dts_user_sessions_user ON dts_user_sessions(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_dts_user_sessions_expiry ON dts_user_sessions(expires_at);

INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc50', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
