# QMigration V0.15.0-rc2 Release Notes

## Release purpose

V0.15.0-rc2 turns the connector support boundary into an explicit product
contract and brings the experimental SQL Server Native path to the same
qualification-oriented software-completion model used by Oracle.

## SQL Server Native completion work

- Added schema-object discovery for View / Sequence / Trigger / Function / Procedure plus expression dependencies.
- Added source-to-SQL-Server type mapping rather than copying foreign `column_type` text into target DDL.
- Added SQL Server `IDENTITY(seed,increment)` discovery and target restoration.
- Full Writer / CDC Apply now manages `SET IDENTITY_INSERT ... ON/OFF` when explicit identity values are migrated; cleanup failure closes the connection instead of leaving a poisoned session in the pool.
- Added same-family SQL Server schema-object/DDL policy path; unsafe routines/triggers remain manual rather than blindly converted.
- Added `qmigration-sqlserver-qualify` and `deployments/scripts/qualify-sqlserver.sh` for structured real-instance PASS/FAIL/SKIP qualification.

## Cross-connector hardening

- Added strict connector-neutral numeric-literal validation for remaining SQL-batch writers so malformed/injection-shaped numeric row images fail before SQL transmission while arbitrary decimal precision is preserved.
- Applied validation to MySQL, PostgreSQL and SQL Server writers, keyset boundaries and point/delete key paths.
- Added SQL Server ROWVERSION/TIMESTAMP source normalization so a rowversion is treated as binary state rather than a datetime.

## Truthful source-CDC capability matrix

- MySQL/MariaDB/PolarDB MySQL and PolarDB-X continue to advertise native MySQL-compatible Binlog source CDC with runtime prechecks.
- TiDB no longer advertises MySQL Binlog source CDC from its SQL endpoint; QMigration requires a dedicated TiCDC adapter.
- OceanBase MySQL no longer advertises source CDC from its SQL endpoint; QMigration must model the separately deployed OceanBase Binlog Service endpoint.
- Both products remain native Full Load and CDC targets.

## Connector diagnostics

Connector descriptors now expose:

- `maturity`: `NATIVE`, `NATIVE_FULL_ONLY`, `EXPERIMENTAL`, or `PROBE_ONLY`;
- `qualification_required`: true for experimental Oracle/SQL Server software paths.

This makes `/api/v1/connectors` usable as a machine-readable support matrix instead of requiring database-name assumptions.

## Remaining release gates

- real Oracle qualification remains required;
- real SQL Server qualification remains required;
- TiCDC and OceanBase Binlog Service source adapters remain separate implementation items;
- DB2/DM/GaussDB/GBase remain probe-only until their QMigration Native connectors exist.
