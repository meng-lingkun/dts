# QMigration V0.15.0-rc19 Release Notes

RC19 adds the first qualification-gated QMigration migration data plane for
**GBase 8s V8.8**. This is a separate product family from the existing GBase 8a
MPP connector: RC19 does not reuse the 8a/MySQL-compatible packet path and does
not imply GBase 8a/8c protocol compatibility.

## GBase 8s connector architecture

QMigration owns:

- system-catalog metadata discovery;
- migration-key selection inputs and bounded keyset reads;
- Full Load batching and retry semantics;
- heterogeneous target type conversion;
- target owner/table/index/foreign-key DDL;
- keyed idempotent target DML;
- point lookup/delete and transactional target CDC apply;
- migration prechecks and qualification.

The SQL transport is supplied by the matching GBase Client-SDK ODBC driver,
exposed to Go through a `database/sql` ODBC provider. QMigration does not bundle
or redistribute the vendor CSDK and does not launch JDBC/DataX/SeaTunnel/Flink
as a migration runtime.

The feature is behind:

```bash
QMIGRATION_EXPERIMENTAL_GBASE8S_NATIVE=1
```

and remains `EXPERIMENTAL / QualificationRequired` until retained real-instance
reports exist.

## Credential-safe ODBC provider

RC19 uses the existing datasource `jdbc_url` compatibility field only as a
**non-secret ODBC DSN/connection-property field**. Examples:

```text
odbc:GBASE8S_APP
DSN=GBASE8S_APP
```

`UID=`, `USER=`, `PWD=` and `PASSWORD=` are rejected in that field. QMigration
keeps username/password in the normal encrypted datasource credential path and
injects them only into the in-memory ODBC connection string at runtime.

Linux Server/Worker processes can load a database/sql ODBC provider plugin with:

```bash
QMIGRATION_GBASE8S_DRIVER_PLUGIN=/opt/qmigration/qmigration-gbase8s-driver.so
```

`deployments/scripts/build-gbase8s-driver-plugin.sh` builds that plugin from a
locally supplied Go ODBC wrapper source tree. Runtime nodes still require the
matching GBase Client-SDK/unixODBC libraries. No vendor driver source is included
in the QMigration archive.

## Metadata and Full Read

RC19 discovers GBase 8s catalog information from `systables`, `syscolumnsext`,
`sysconstraints` and `sysindexes`, including:

- visible owners and application tables;
- column type/nullability/ordinal metadata;
- SERIAL-family generator markers;
- composite primary keys;
- unique/ordinary indexes and index column order;
- estimated row counts;
- numeric single-key MIN/MAX statistics.

Full Read supports numeric range and stable single/composite keyset scanning.
Ordered boundary planning uses `NTILE` + `ROW_NUMBER`; an unsupported retained
server build can fail that optional boundary request without changing the core
keyset correctness contract.

RC19 intentionally supports only the safe unquoted identifier subset
`[A-Za-z_][A-Za-z0-9_$#]*`. Quoted/case-sensitive identifiers remain a
qualification item rather than being silently rewritten.

## Target schema and DML

The GBase 8s target path includes conservative mappings for integer, DECIMAL,
FLOAT/SMALLFLOAT, DATE/DATETIME, BOOLEAN, VARCHAR/LVARCHAR, BLOB/CLOB and UUID
families. DECIMAL precision is capped at the connector's RC19 qualified software
boundary of 32; wider source declarations are converted conservatively.

QMigration does not implicitly create a GBase 8s database user/owner. The target
owner must already exist.

Target Full Write requires a stable migration key. Each batch runs in a local
transaction unless it is already inside a QMigration CDC apply transaction. For
each logical row it performs:

1. prepared `UPDATE ... WHERE <migration-key>` of non-key columns;
2. if the ODBC affected-row result is zero/unknown, prepared existence lookup;
3. prepared `INSERT` only when the key is absent.

The existence check is required because ODBC drivers may not provide a reliable
affected-row count. Keyless target writes fail closed.

The same connector implements begin/commit/rollback, keyed delete and point
lookup, so **GBase 8s can be a transactional CDC target for a supported source**.
This does not imply GBase 8s source CDC support.

## Source CDC boundary

GBase 8s exposes CDC/full-row-logging facilities in the `syscdcv1` family, but
RC19 does not have a retained, product-qualified source consumption contract
that proves all of the following together:

- durable restart position;
- complete transaction boundary/order;
- row before/after image semantics for the supported type matrix;
- an explicit source advancement/ACK contract compatible with QMigration's
  apply-before-ACK runtime.

Therefore `CapabilityCDCRead`, `CapabilityCDCPosition` and
`CapabilityCDCCheckpoint` are **not advertised**. A `FULL+CDC` plan with GBase
8s as source fails capability validation instead of falling back to another
runtime.

## Qualification

RC19 adds:

- `qmigration-gbase8s-qualify`;
- `deployments/scripts/qualify-gbase8s.sh`;
- `deployments/scripts/build-gbase8s-driver-plugin.sh`;
- `docs/GBASE8S_NATIVE_QUALIFICATION.md`.

Optional target qualification verifies create -> keyed replay -> BLOB readback ->
transactional delete -> drop on a temporary table.

## Deliberate RC19 boundaries

Not promoted as production support:

- GBase 8s source CDC;
- automatic CSDK TLS/SSL option mapping (non-DISABLE QMigration TLS modes fail
  closed in RC19);
- quoted/case-sensitive identifier semantics;
- source foreign-key catalog reconstruction across every 8s catalog/version
  variant;
- automatic database-user/owner creation;
- production support without retained GBase 8s version/CSDK/unixODBC/charset/
  topology/failover qualification evidence.
