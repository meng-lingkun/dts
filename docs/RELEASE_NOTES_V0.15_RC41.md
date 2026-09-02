# QMigration V0.15.0-rc41 Release Notes

RC41 starts the long-running stability phase for 10-40 TB migrations. It bounds operational metadata growth and removes full chunk-list materialization from the hottest progress/metrics aggregation paths.

## Metadata retention janitor

- Background maintenance is enabled by default and runs every 10 minutes.
- Task logs default to 7 days and 20,000 rows per task.
- Audit events default to 90 days and 100,000 rows globally.
- CDC position history defaults to 7 days and 4,096 rows per task+direction stream.
- The newest CDC position for each task+direction is never removed by time-based retention or row-count retention.
- PostgreSQL pruning is bounded to small batches (default 5,000 rows x 4 batches per category per run).

## Control-plane aggregation

`refreshProgress()` and `/metrics` now use `ChunkSummaryProvider`. PostgreSQL performs grouped aggregation inside metadata storage and returns one summary row per table instead of returning every chunk to the Server process.

## New controls

- `QMIGRATION_METADATA_MAINTENANCE_ENABLED=true`
- `QMIGRATION_METADATA_MAINTENANCE_INTERVAL_MINUTES=10`
- `QMIGRATION_TASK_LOG_RETENTION_HOURS=168`
- `QMIGRATION_TASK_LOG_MAX_ROWS_PER_TASK=20000`
- `QMIGRATION_AUDIT_RETENTION_HOURS=2160`
- `QMIGRATION_AUDIT_MAX_ROWS=100000`
- `QMIGRATION_CDC_POSITION_RETENTION_HOURS=168`
- `QMIGRATION_CDC_POSITION_MAX_ROWS_PER_STREAM=4096`
- `QMIGRATION_METADATA_PRUNE_BATCH_ROWS=5000`
- `QMIGRATION_METADATA_PRUNE_MAX_BATCHES=4`

## Safety boundary

RC41 does not prune pending CDC spool records, unresolved DLQ records, migration chunks, or validation correctness state.
