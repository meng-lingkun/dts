# QMigration V0.15.0-unified-dev5 Release Notes

## Scope

This development snapshot continues the single **QMigration Unified Engine** line. It does not restore DataX, SeaTunnel, Flink CDC, Debezium or Canal as migration runtimes.

## SQL Server Native planner hardening

- Fixed exact lexicographic `[lower, upper)` predicates for composite keyset chunks.
- Added ordered `NTILE` boundary planning for string/composite/UNIQUE migration keys.
- Added native partition discovery and `$PARTITION` read predicates.
- Added runtime-load sampling so SQL Server participates in the same adaptive task-parallelism/backpressure loop as the other native connectors.

SQL Server Full remains behind `QMIGRATION_EXPERIMENTAL_SQLSERVER_NATIVE=1` until real-server E2E and soak qualification are completed.

## SQL Server CDC durability and retention

- Windows containing no selected-table changes now produce checkpoint-only CDC transactions instead of advancing only an in-memory cursor.
- The source LSN is acknowledged only after QMigration has durably persisted the checkpoint.
- Precheck now reads the SQL Server CDC cleanup retention and can block unsafe configurations below `QMIGRATION_SQLSERVER_CDC_MIN_RETENTION_MINUTES` (QMigration default safety floor: 4320 minutes).
- Existing retained-min-LSN gap checks remain active during CDC consumption.

This is a guard, not a retention pin. Long Full snapshots that can exceed source CDC retention still require a sufficiently large SQL Server retention window. Durable CDC staging/spooling is a later Unified Engine item.

## Oracle Native transport

- Added TCPS with `DISABLE/PREFERRED/REQUIRED` policy.
- Added custom CA, TLS ServerName verification and optional mTLS client certificate/key.
- REQUIRED mode never silently downgrades to plaintext, including listener redirects.
- Added post-ACCEPT TNS DATA framing/session transport foundation for future TTC authentication and SQL protocol work.

Oracle authentication, Data Dictionary access, Full Reader/Writer and Redo/LogMiner CDC are **not** claimed in this snapshot.

## Metadata

- Runtime version: `0.15.0-unified-dev5`
- Metadata schema version: `0.15.0-unified-dev5`
- Added migration: `021_v015_native_planner_tcps_hardening.sql`

## Release gate

The snapshot is acceptable only when:

- `go test ./...` passes;
- `go vet ./...` passes;
- all QMigration binaries build;
- default/experimental capability smoke tests pass;
- dev4 -> dev5 and formal V0.13 -> dev5 patches reproduce the exact source tree and pass tests/vet.
