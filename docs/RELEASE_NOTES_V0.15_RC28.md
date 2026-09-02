# QMigration V0.15.0-rc28 Release Notes

## Scope

RC28 closes two correctness gaps that are more important than adding another database name: exact historical smart BLOB/CLOB transport for the experimental GBase 8s CDC path, and a generic guard for target COMMIT responses whose outcome is unknown.

## GBase 8s smart BLOB/CLOB CDC

- Agent API and native provider ABI advance to **v4**; older experimental providers are rejected until rebuilt.
- New gate: `QMIGRATION_EXPERIMENTAL_GBASE8S_SMART_LOB_CDC=1` in addition to the existing native + CDC gates.
- Selected BLOB/CLOB columns are tagged explicitly in the provider contract.
- Checkpoint/read responses must declare `cdc-event-owned-lob-v1`.
- Every non-NULL smart LOB row image carries column/kind/byte-length/SHA-256/acquisition proof and is verified before target apply.
- INSERT, DELETE, UPDATE-before and UPDATE-after use the same proof rules.
- Current-row SQL lookup is forbidden because it cannot prove the historical CDC image when the consumer is behind.
- The transport remains bounded to complete normalized records; oversize LOBs beyond configured response limits fail closed.
- TEXT/BYTE and opaque/collection/UDT source CDC are not promoted by this contract.

This is a software-complete experimental contract, not a production compatibility claim. A retained real V8.8 CSDK/provider qualification must prove event-owned historical bytes while a later row version exists.

## Target COMMIT outcome uncertainty

A target COMMIT call can fail after the server has committed but before the client receives the response. Blind retry can duplicate triggers or other non-idempotent side effects. RC28 therefore treats every transactional target COMMIT error as an unknown outcome:

1. persist CDC DLQ status `COMMIT_UNCERTAIN`;
2. do not issue automatic rollback after the failed COMMIT response;
3. block automatic replay and later CDC for the same task/direction;
4. require an operator decision.

`COMMITTED` means the operator verified the target transaction committed. QMigration advances the retained source checkpoint without replaying target DML. `NOT_COMMITTED` means the operator verified it did not commit; QMigration changes the retained item to `REPLAY_REQUIRED` and immediately performs one explicit retained-event replay in the same operator action. If that replay fails before COMMIT, the item remains `REPLAY_REQUIRED`, blocks later source flow, and can only be closed by explicit replay. If the replay itself loses a COMMIT response, it returns to `COMMIT_UNCERTAIN`.

New API: `POST /api/v1/migrations/{id}/cdc/dlq/{dlq_id}/resolve-commit`. Web UI exposes “已提交 / 未提交” actions, and Prometheus exports `qmigration_cdc_commit_uncertain` plus `qmigration_cdc_replay_required`.

## Durable pre-COMMIT ambiguity fence

For transactional targets, QMigration now persists the exact retained CDC transaction as a durable ambiguity fence **before sending target COMMIT**. The fence is cleared only after the target COMMIT and durable QMigration source checkpoint both succeed. If the process dies before COMMIT, during COMMIT, or after COMMIT but before checkpoint, restart blocks automatic replay until the operator establishes the target outcome. This closes the process-crash window that a response-error-only guard cannot cover.

Historical durable-spool drain uses the same fence and records the actual failed spool transaction, so a newer live request cannot accidentally own the DLQ ordering gap.

## Chaos qualification

`qmigration-chaos-qualify` now runs eight scenarios, covering persisted-spool/source-ACK ordering, apply-before-spool-mark, checkpoint-before-source-ACK, both COMMIT outcome decisions, historical spool COMMIT uncertainty, COMMIT-success-before-checkpoint crash recovery, and failed NOT_COMMITTED replay retention as `REPLAY_REQUIRED`.

External proxy/network/process-kill soak at the COMMIT boundary remains a separate retained qualification task.
