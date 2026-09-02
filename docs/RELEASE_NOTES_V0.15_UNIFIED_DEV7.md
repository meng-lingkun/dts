# QMigration V0.15.0-unified-dev7 Release Notes

## Theme

This snapshot moves durable CDC payloads out of Metadata into an independent encrypted file spool and adds a persistent validation watermark barrier.

## Independent encrypted file spool

Default mode is `QMIGRATION_CDC_SPOOL_STORAGE=file`. QMigration gzip-compresses and AES-256-GCM encrypts the transaction first. The ciphertext is atomically written (`0600`, fsync, rename) to `QMIGRATION_CDC_SPOOL_DIR`; only then is the Metadata sequence/index committed and the source position allowed to ACK.

Metadata stores ordering, position, status and a file reference; it no longer needs to carry the large encrypted CDC payload itself.

## Disk backpressure

- WARN defaults to 80% filesystem usage and delays source capture before the next durable stage.
- CRITICAL defaults to 90%; staging fails before Metadata commit and the source position is not acknowledged.
- `/readyz` checks that the spool filesystem is writable and below CRITICAL.
- Prometheus/UI expose used percentage and free bytes.

## Multi-Server HA

PostgreSQL Metadata includes `cdc_spool_drain_leases`. A Server instance must hold the task/direction lease before draining, preventing concurrent drains by multiple Server replicas. The Kubernetes example mounts one RWX PVC into both Server pods.

## Validation Watermark Barrier

Full+CDC validation now requires empty spool, acceptable lag, and a durable CDC checkpoint that has remained unchanged for `QMIGRATION_VALIDATION_STABLE_WINDOW_SECONDS` (default 2 seconds). The position/type/resource/time are persisted on the task.

If the checkpoint changes while validation is scanning, QMigration deletes the results from that generation and moves the task back to `CDC_CATCHING_UP`. This prevents ordinary concurrent-write drift from being reported as a final validation mismatch or success.

This is not yet vendor-specific historical snapshot validation at an exact GTID/LSN; nonstop-write workloads may retry until a stable window is available.

## Metadata migration

`023_v015_spool_file_barrier.sql` adds the validation barrier fields and distributed drain lease table and advances Metadata Schema to `0.15.0-unified-dev7`.
