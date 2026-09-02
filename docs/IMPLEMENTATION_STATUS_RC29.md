# QMigration Implementation Status

## V0.15.0-rc29

### Real process-death CDC qualification

- [x] `name=N@SIGKILL` opt-in failpoint action
- [x] child-process SIGKILL after target COMMIT / before durable checkpoint
- [x] restart verifies durable `COMMIT_UNCERTAIN` fence and zero automatic duplicate target writes
- [x] operator COMMITTED decision advances checkpoint only; subsequent source redelivery is duplicate-suppressed
- [x] child-process SIGKILL after durable spool persist / before source ACK
- [x] restart/source redelivery retains one durable spool transaction
- [x] `qmigration-chaos-qualify` now 10/10 scenarios
- [ ] external TCP proxy response-drop at a real vendor target COMMIT boundary remains retained qualification work

### Predictive 10–40 TB Full+CDC flow control

- [x] spool pending-byte pressure integrated into task-level flow control
- [x] file/shared-fs/S3 storage WARN/CRITICAL participates in Full throttling
- [x] backlog growth bytes/second sampled across control intervals
- [x] projected seconds to CRITICAL backlog boundary
- [x] WARN/CRITICAL can trigger before absolute byte threshold when projected exhaustion is near
- [x] `effective_parallelism` shrinks under pressure and recovers gradually
- [x] control-plane batch target uses bounded +25%/-50% AIMD-style convergence
- [x] persisted `cdc_spool_growth_bytes_sec` / `cdc_spool_critical_eta_seconds`
- [x] API/WebSocket/Web/Prometheus observability
- [ ] real 10–40 TB soak calibration of threshold defaults remains qualification work

### File-spool I/O failure windows

- [x] pre-write I/O failpoint leaves no pending payload
- [x] after-file-persist/before-metadata failpoint creates no Metadata row and no source ACK
- [x] startup Reconcile quarantines the unreferenced payload under `recovered-orphans`
- [ ] real filesystem ENOSPC / read-only remount multi-process soak remains qualification work
