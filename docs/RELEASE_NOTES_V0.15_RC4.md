# QMigration V0.15.0-rc4 Release Notes

## Release purpose

RC4 closes the OceanBase MySQL source-CDC gap without treating the OceanBase SQL
endpoint as a normal MySQL replication source. QMigration now models the tenant
ODP/Binlog Service subscription path explicitly and reuses its own MySQL Binlog
V4/GTID decoder behind that endpoint.

## OceanBase Binlog Service Native source CDC

- `oceanbase_mysql` now advertises `cdc-position` and `cdc-read` as `EXPERIMENTAL` and `qualification_required`.
- datasource `cdc_url` accepts:
  - `obbinlog://odp-host:port`
  - `obbinlogs://odp-host:port?server_name=...`
  - repeated `fallback=host:port` ODP endpoints (up to seven fallbacks).
- Current CDC position is captured through the Binlog subscription ODP using `SHOW MASTER STATUS` / `SHOW BINARY LOG STATUS`.
- QMigration prefers OceanBase Binlog Service GTID when available and falls back to file/position.
- Unified Engine routes OceanBase CDC through a dedicated `native-oceanbase-binlog` adapter while executing the QMigration-owned MySQL Binlog V4 reader.
- Worker reconnect rotates configured ODP endpoints and always resumes from the last target-applied durable GTID/file-position.
- SQL/full-load and CDC TLS modes are independent: `obbinlog://` is plaintext; `obbinlogs://` requires TLS and reuses encrypted CA/mTLS material from the datasource.
- Precheck validates ODP reachability, `SHOW MASTER STATUS`, visible binlog files and warns when the common Binlog Server management port `2983` may have been supplied by mistake.

## Qualification

Added:

- `qmigration-oceanbase-qualify`
- `deployments/scripts/qualify-oceanbase.sh`
- `docs/OCEANBASE_BINLOG_QUALIFICATION.md`
- `docs/SUPPORT_MATRIX_V0.15_RC4.md`

The qualifier is read-only and does not create/drop OceanBase objects.

## Deliberate boundaries

- RC4 does not auto-deploy or administer OceanBase Binlog Service.
- `cdc_url` must point to a tenant-aware ODP subscription listener; QMigration does not infer it.
- Binlog Service/OceanBase-specific incompatible data types remain a real-instance qualification item.
- OceanBase CDC remains `EXPERIMENTAL` until ODP/Binlog Service failover, GTID retention and version compatibility reports are retained.

## Metadata

- Version: `0.15.0-rc4`
- Migration: `035_v015_rc4_oceanbase_binlog.sql`
