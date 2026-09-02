# QMigration RC29 Predictive Performance Control

RC29 targets long-running 10–40 TB Full+CDC migrations. The goal is not maximum instantaneous Full throughput; it is the highest sustainable throughput that does not let CDC durability, source pressure, target pressure, or Worker pressure become an outage.

## CDC-spool predictive throttling

While a task is `FULL_MIGRATING` in `FULL_AND_INCREMENTAL` mode, each flow-control sample evaluates:

- current forward CDC spool pending bytes;
- file/shared-fs/S3 storage WARN/CRITICAL level;
- backlog growth bytes/second since the previous sample;
- projected seconds until the configured CRITICAL backlog boundary.

Defaults derive from `QMIGRATION_CDC_SPOOL_MAX_PENDING_BYTES` (64 GiB): WARN at 50%, CRITICAL at 80%. Operators may override:

```bash
QMIGRATION_CDC_SPOOL_BACKLOG_WARN_BYTES
QMIGRATION_CDC_SPOOL_BACKLOG_CRITICAL_BYTES
QMIGRATION_CDC_SPOOL_PREDICT_WARN_SECONDS=900
QMIGRATION_CDC_SPOOL_PREDICT_CRITICAL_SECONDS=300
```

A projected CRITICAL boundary within five minutes is treated as CRITICAL even if current bytes have not yet reached the absolute warning threshold. CRITICAL halves `effective_parallelism`; WARN reduces it gradually. Recovery raises parallelism one slot per sampling cycle.

## Control-plane batch target

The Worker still uses the bounded Reader → Transform → Writer → Checkpoint pipeline, but RC29 makes batch sizing a control-plane feedback decision.

The target is based on the slower of the latest source-read and target-write latency. Default target bottleneck latency is 1200ms:

```bash
QMIGRATION_ADAPTIVE_BATCH_TARGET_MS=1200
QMIGRATION_ADAPTIVE_BATCH_MIN_ROWS=50
QMIGRATION_ADAPTIVE_BATCH_MAX_ROWS=5000
```

To avoid oscillation, one feedback round may grow a batch by at most 25% or shrink it by at most 50%. WARN/CRITICAL backpressure caps the target further.

## Persisted telemetry

Migration tasks expose:

- `cdc_spool_growth_bytes_sec`
- `cdc_spool_critical_eta_seconds`
- `effective_parallelism`
- `flow_control_level`
- `flow_control_reason`

Prometheus exports:

```text
qmigration_cdc_spool_growth_bytes_per_second
qmigration_cdc_spool_critical_eta_seconds
qmigration_task_effective_parallelism
qmigration_cdc_spool_pending_bytes
qmigration_cdc_spool_storage_used_pct
```

The Web monitoring/task detail views show the same growth and projected critical ETA.

## Safety boundary

The predictive controller only slows Full work. It never advances a source CDC ACK, changes CDC transaction ordering, or discards backlog. If the spool reaches its hard capacity/critical storage rule, source capture remains fail-closed/no-ACK as before.
