# Dameng / DM Native Connector Qualification

## RC25 scope

QMigration V0.15.0-rc25 retains the qualification-gated Dameng data plane introduced in RC13 and adds an experimental archived-log source CDC path plus exact `DM_LSN` validation snapshots. QMigration owns metadata discovery, Full Load planning/read/write, schema creation, prepared target DML, point lookup and transactional CDC target apply. SQL transport is provided by DM's Go `database/sql` driver; QMigration does not vendor the proprietary DM wire driver in its source archive.

Implemented software paths:

- schema/table/column/PK/index/FK discovery from DM catalog views;
- numeric and composite-key keyset Full Read plus NTILE boundary planning;
- target schema/table/composite PK/index/FK creation;
- prepared keyless INSERT and keyed idempotent MERGE;
- BLOB/binary prepared parameters and numeric-literal validation;
- point lookup/delete and explicit begin/commit/rollback for target CDC apply;
- `qmigration-dameng-qualify` structured real-instance qualification.
- source CDC through `DBMS_LOGMNR` over retained local archive files, with durable numeric `DM_LSN` checkpoints;
- complete before/after row reconstruction through `AS OF SCN` flashback instead of parsing `SQL_REDO` literals;
- long-transaction rewind to the minimum committed `START_SCN` before emitting any transaction;
- same-`COMMIT_SCN` XIDs aggregated into one QMigration target transaction so one durable `DM_LSN` cannot suppress a sibling transaction;
- exact validation snapshots through `AS OF SCN <DM_LSN>`.

Not claimed in RC25:

- automatic procedural object conversion/replay;
- Dameng provider TLS modes. `PREFERRED`/`REQUIRED` fail closed until provider-specific TLS properties are qualified;
- production maturity without retained real-instance reports.

## Official DM Go driver

DM documents a Go driver implementing the standard `database/sql` interface. The driver name is `dm`, the package is imported as `_ "dm"`, and the documented DSN form starts with `dm://user:password@host:port...`. The DM installation distributes the Go driver under its `drivers/go` directory.

QMigration intentionally keeps those vendor sources outside this archive. On Linux, the stock RC13 Server/Worker can load a Go plugin whose `init` registers the DM driver:

```bash
export DM_GO_DRIVER_DIR=/opt/dmdbms/drivers/go/dm
./deployments/scripts/build-dameng-driver-plugin.sh

export QMIGRATION_DAMENG_DRIVER_PLUGIN="$PWD/bin/qmigration-dameng-driver.so"
export QMIGRATION_EXPERIMENTAL_DAMENG_NATIVE=1
```

The plugin must be built with the same Go toolchain as the QMigration binaries. If a custom provider registers another `database/sql` driver name, set `QMIGRATION_DAMENG_SQL_DRIVER` or datasource `driver_class` accordingly.

If no provider is registered, RC25 fails explicitly with `Dameng database/sql driver ... is not registered`; it does not fall back to the old generic/JDBC placeholder.

## Qualification

Read-only qualification:

```bash
export DAMENG_HOST=10.0.0.10
export DAMENG_PORT=5236
export DAMENG_USER=APP
export DAMENG_PASSWORD='...'
export DAMENG_SCHEMA=APP
export DAMENG_TABLE=ORDERS
export QMIGRATION_DAMENG_DRIVER_PLUGIN="$PWD/bin/qmigration-dameng-driver.so"

./deployments/scripts/qualify-dameng.sh
```

Optional destructive target qualification creates a temporary table, performs prepared MERGE/BLOB write, reads it back, deletes the row, and drops the table:

```bash
export DAMENG_QUALIFY_TARGET_WRITE=1
export DAMENG_QUALIFY_OUTPUT=/tmp/qmigration-dameng-qualification.json
./deployments/scripts/qualify-dameng.sh
```

Experimental source CDC / exact-watermark qualification:

```bash
export DAMENG_QUALIFY_CDC=1
export DAMENG_TABLE=ORDERS
./deployments/scripts/qualify-dameng.sh
```

The CDC check requires `ARCH_INI=1`, logical redo (`RLOG_APPEND_LOGIC`), `ENABLE_FLASHBACK=1`, uncompressed logical redo for the RC25 software contract, a deterministic primary key, and continuous local archived-log coverage. QMigration uses archived `DBMS_LOGMNR` records only as the XID/ROWID/commit index and reconstructs row images with flashback `AS OF SCN`; unsupported DDL/BATCH_UPDATE/unknown operations or an archive/UNDO gap fail closed before checkpoint advancement.

The report never includes the password. Keep the JSON report together with the exact DM release, Go driver release, server charset, deployment mode and QMigration build digest.

## Exit criteria before gate removal

Retain successful reports for the supported DM8 maintenance releases and representative charset/database modes, plus:

1. Metadata with composite PK, secondary/unique indexes and foreign keys.
2. Full Load with numeric/composite migration keys, CLOB/BLOB and large tables.
3. Dameng target prepared INSERT/MERGE, rollback/commit and restart/idempotency tests.
4. Network interruption and Worker failover during Full Load and target CDC apply.
5. Provider TLS qualification before exposing non-DISABLE TLS modes.
6. Archived-log CDC restart/long-transaction/archive-gap/flashback-UNDO retention tests, including multiple XIDs sharing one `COMMIT_SCN`.
7. Exact `DM_LSN` validation-snapshot checks while source writes continue and new CDC is staged in the durable spool.
8. Remove the CDC experimental gate only after retained DM8 release/topology reports pass with zero FAIL checks.
