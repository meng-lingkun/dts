-- QMigration V0.14 Native datasource TLS policy
ALTER TABLE datasources ADD COLUMN IF NOT EXISTS tls_mode text;
ALTER TABLE datasources ADD COLUMN IF NOT EXISTS tls_server_name text;
ALTER TABLE datasources ADD COLUMN IF NOT EXISTS tls_ca_cert text;
