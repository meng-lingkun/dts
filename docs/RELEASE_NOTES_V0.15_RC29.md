# QMigration V0.15.0-rc29 Release Notes

RC29 shifts the development focus from connector breadth to long-running production reliability and sustainable Full+CDC throughput.

## Process-level crash qualification

- `QMIGRATION_FAULT_PLAN` accepts `name=N@SIGKILL` in addition to deterministic error injection.
- `qmigration-chaos-qualify` now runs 10 scenarios.
- A real child process is killed after target COMMIT but before durable checkpoint. Restart must preserve the pre-COMMIT ambiguity fence, block replay, and keep target writes at exactly one.
- A real child process is killed after durable CDC spool persistence but before source ACK. Restart/redelivery must retain exactly one spool transaction.

## Predictive Full+CDC flow control

- Full Load pressure now includes CDC spool backlog bytes, storage watermarks, backlog growth rate, and projected time to the critical backlog threshold.
- The controller reduces `effective_parallelism` before the durable spool approaches exhaustion.
- Batch feedback now uses a bounded AIMD-style target rather than relying only on Worker-local 2x/0.5x changes.
- New persisted/API/Web/Prometheus telemetry: `cdc_spool_growth_bytes_sec` and `cdc_spool_critical_eta_seconds`.

## File-spool I/O crash windows

- Added `cdc.spool.file.before_write` and `cdc.spool.file.after_persist_before_metadata` failpoints.
- Atomic-payload/metadata-gap orphan files are quarantined by startup reconciliation and never imply a source ACK.

## Metadata

- Added migration `061_v015_rc29_predictive_flow_process_chaos.sql`.
