# GBase 8a Target CDC Qualification (RC27)

RC27 can use GBase 8a MPP Cluster as a **target** for incremental row apply behind an explicit qualification gate. It does not claim a GBase 8a source CDC feed or atomic replay of a multi-event source transaction.

## Enable

```bash
export QMIGRATION_EXPERIMENTAL_GBASE8A_NATIVE=1
export QMIGRATION_EXPERIMENTAL_GBASE8A_TARGET_CDC=1
```

For a real-instance qualification run:

```bash
export GBASE_PASSWORD='...'
./deployments/scripts/qualify-gbase.sh \
  --host <gcluster-host> --port 5258 --user <user> --database <db> \
  --target-cdc --output gbase8a-target-cdc.json
```

`--target-cdc` creates a temporary HASH-distributed table, validates the actual `SHOW CREATE TABLE` layout, performs an INSERT followed by retry-idempotent MERGE UPDATE, verifies point lookup, performs a keyed DELETE, and verifies disappearance.

## Semantics

- INSERT/UPDATE: validated HASH staging table + GBase `MERGE`.
- DELETE: stable mapped primary/migration key.
- Retry: safe for the same source position/event because MERGE/delete are idempotent and QMigration persists durable source positions.
- LAST_WRITE_WINS: target point lookup is available.
- Transaction atomicity: **not advertised**. QMigration does not expose `cdc-transactional-apply` for GBase 8a. A multi-event source transaction can be transiently visible event-by-event.

## Source CDC boundary

GBase 8a product material describes cluster full/incremental synchronization and binary-file rsync-style tooling, but RC27 has no retained generic row-change API that proves complete historical before/after images for heterogeneous replay. Audit SQL likewise cannot prove row images for expression updates, loads, nondeterministic statements, or concurrent changes. Therefore GBase 8a source CDC remains fail-closed.

Production promotion requires retained real-cluster evidence covering HASH distribution, MERGE retries, delete retries, node/process failure windows, concurrent readers, and version/topology differences.
