# QMigration V0.15.0-unified-dev1

This development snapshot is an architecture correction, not a normal RC continuation.

QMigration no longer manages multiple third-party migration runtimes. The data plane is now one QMigration Unified Engine. DataX, SeaTunnel, Flink CDC, Debezium and Canal are treated as reference designs whose useful ideas are reimplemented inside QMigration.

## Breaking architectural changes

- Removed SeaTunnel/DataX/Flink CDC adapters from the active Engine package.
- Removed third-party executable Full Load path from Worker.
- Removed engine selection from Vue.
- All task engine fields normalize to `qmigration`.
- Unsupported database families fail explicitly until a QMigration Native Connector is implemented; no fallback to an external tool.

## New data-plane runtime

```text
Reader -> bounded channel -> Transform -> Writer -> durable Checkpoint
```

The pipeline supports bounded prefetch/backpressure and preserves apply-before-checkpoint semantics.

## Native CDC

MySQL-family and PostgreSQL-family readers remain QMigration-owned processes supervised by Worker for isolation. They feed the same CDCEvent/apply/checkpoint model and are not separate products or selectable engines.

## Compatibility ingestion

The existing Debezium/Canal JSON HTTP endpoints remain only as payload-format compatibility shims. No Debezium or Canal runtime is installed or launched by QMigration.
