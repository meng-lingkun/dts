# QMigration V0.15.0-rc24 Release Notes

RC24 closes two GBase 8s source-CDC correctness/operability gaps discovered while validating RC23: the RC22 Agent observability surface had regressed from the RC23 source tree, and an old durable checkpoint could not prove that a restarted/recreated provider was still attached to the same logical capture generation.

## Capture-lineage fence

- Agent API advances to **v3** and the native provider ABI advances to **v3**. Older v1/v2 agents/providers are rejected.
- Checkpoint responses must return a 64-hex `capture_lineage`.
- `GBASE8S_CDC_SEQ` now persists `restart=<seq>;commit=<seq>;capture=<64-hex>`.
- Every read request sends `expected_capture_lineage`; every read response must return the same lineage. A mismatch fails before any row is applied.
- A provider may keep the lineage only when it is genuinely resuming the same logical capture. Recreating/replacing the capture must produce a different lineage.
- RC23-or-older checkpoints without lineage are rejected by the RC24 reader; operators must restart from a newly captured pre-Full checkpoint rather than silently attaching an old watermark to a new capture session.

This fence complements, rather than replaces, the RC23 schema fingerprint fence. Schema identity answers “same row layout”; capture lineage answers “same logical capture generation.”

## RC22 observability restored

RC24 restores the authenticated Agent endpoints that were present in the RC22 observability branch but absent from the RC23 source archive:

- `GET /v1/status`;
- `GET /metrics`;
- provider call/error/record/byte counters;
- busy/current operation and last-operation duration;
- exact checkpoint/read sequence diagnostics in status only;
- latest capture lineage in status only;
- qualifier validation through `qmigration-gbase8s-qualify --cdc`.

Capture lineage and exact sequence values are deliberately not exported as Prometheus labels or numeric gauges.

## Compatibility boundary

RC24 does not widen the smart BLOB/CLOB source-value contract and does not remove the real GBase 8s V8.8/CSDK qualification gate. Transactional TRUNCATE, long-transaction restart/commit watermarks, schema fences, TLS/token boundaries and provider SHA pinning remain in force.
