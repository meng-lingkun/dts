# QMigration V0.14 Development Baseline

- Development version: `0.14.0-dev`
- Formal base release: `v0.13.0`
- Formal base commit: `7bbe2fa71baeb38af0c1a3c9fc1832efa4a10786`
- Formal archive SHA-256: `76530fd4919c21fc5a5fad743af4789670a37a9cd93045840aebc7b4d3950250`

## Restored post-V0.13 WIP

The archived post-release WIP patch was restored selectively for backend code. Its README regression was intentionally not applied because it would have reverted V0.13 release documentation to V0.12.

## V0.14 work completed in this development snapshot

- Native datasource TLS policy: `DISABLE / PREFERRED / REQUIRED`.
- MySQL `CLIENT_SSL` negotiation and TLS 1.2+ transport.
- MySQL SQL / Full Load / Binlog CDC share the datasource TLS policy.
- PostgreSQL SSLRequest policy for SQL / Full Load / logical replication.
- Custom CA PEM and TLS server-name certificate verification.
- PostgreSQL metadata persistence and migration `010_v14_datasource_tls.sql`.
- Worker credentials carry TLS settings.
- Vue datasource TLS controls.
- Regression tests for required-no-downgrade, custom CA TLS handshake, invalid CA, and API defaults.

## Validation

- `go test ./...`: PASS
- `go vet ./...`: PASS
- `git diff --check`: PASS
- Vue production build: NOT RUN to completion because the restored archive has no `node_modules` and dependency installation could not complete in the current environment.

## Open-source capability fusion increment

- Re-read the prior DataX / SeaTunnel / Flink CDC / Debezium / Canal comparison and fixed the integration direction in `docs/OPEN_SOURCE_TOOL_DESIGN.md`.
- Added ordered Native Keyset boundary planning for MySQL-family and PostgreSQL-family sources.
- Added parallel `[lower, upper)` chunks for string/composite PK and UNIQUE NOT NULL Migration Keys.
- Added immutable chunk tuple bounds and durable in-chunk cursor recovery; parallel keyset validation is de-duplicated to one full-table checksum per table for heterogeneous collation safety.
- Added metadata migration `011_v14_bounded_keyset.sql` and Vue bound/cursor visibility.
