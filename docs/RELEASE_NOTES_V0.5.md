# QMigration V0.5.0 Release Notes

## Release focus

V0.5.0 introduces the first built-in CDC data plane while retaining SeaTunnel, DataX and Flink CDC as pluggable execution engines.

## Major additions

- Native MySQL row-based binlog reader over `COM_BINLOG_DUMP`.
- Native PostgreSQL logical replication reader using `pgoutput`.
- Transactional CDC Apply with checkpoint-after-commit semantics.
- Forward and rollback CDC engines are selected independently.
- Standard MySQL binary JSON object/array/scalar decoding.
- Composite-primary-key CDC UPSERT/DELETE.
- Schema object discovery, index/foreign-key assessment and deferred post-load DDL.
- AUTO per-table full-load routing between Native and external engines.
- Adaptive batch sizing, slow-range splitting and Worker resource-aware scheduling.
- Native CDC executable discovery from PATH, sibling binary directory or `QMIGRATION_BIN_DIR`.
- Backend container now packages all six Go executables.

## Shipped executables

```text
qmigration-server
qmigration-worker
qmigration-cdc-bridge
qmigration-binlog-inspect
qmigration-mysql-cdc
qmigration-postgres-cdc
```

## Native MySQL CDC requirements

- `log_bin=ON`
- `binlog_format=ROW`
- `binlog_row_image=FULL`
- replication privileges
- file:position checkpoint in V0.5

Unsupported MySQL DDL, partial JSON row updates and unknown binary JSON OPAQUE extensions stop the Reader without advancing the durable checkpoint.

## Native PostgreSQL CDC requirements

- `wal_level=logical`
- available replication slot and WAL sender capacity
- replication privilege
- tables included in the managed publication

QMigration ACKs LSN only after the target transaction and QMigration checkpoint succeed.

## Verification performed

```text
git diff --check       PASS
gofmt                  PASS
go test ./...           PASS
go vet ./...            PASS
6 Go binaries build     PASS
Server health smoke     PASS
Worker registration     PASS
Native CDC capability   PASS
REST API smoke          PASS
```

Vue production build was not completed in the release environment because npm registry access timed out. The frontend source remains included.

## Not yet production-complete

- Native MySQL GTID transport/restart.
- MySQL binary JSON OPAQUE temporal/decimal variants and partial JSON diff events.
- Automatic DDL CDC translation/replay.
- PostgreSQL advanced logical streaming/two-phase extensions.
- Oracle LogMiner Native CDC.
- SQL Server Native CDC.
- Native resumable full load for string/composite/no-primary-key tables.
- Active-active conflict detection/resolution.
- Real MySQL/PostgreSQL E2E CDC CI environment.
