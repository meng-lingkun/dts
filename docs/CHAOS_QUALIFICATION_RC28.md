# QMigration RC28 CDC Chaos Qualification

Run `bin/qmigration-chaos-qualify` or `deployments/scripts/qualify-chaos.sh`. Fault injection remains disabled by default in normal runtime.

The RC28 self-test covers eight deterministic windows:

1. durable spool persisted before source ACK; retry reuses the same spool record;
2. target apply completed before spool mark; retry is suppressed by durable checkpoint and does not write twice;
3. durable checkpoint completed before source ACK; redelivery is recognized as duplicate;
4. target COMMIT completed but response is lost; `COMMIT_UNCERTAIN` blocks replay, COMMITTED resolution advances checkpoint without another target write, then redelivery is duplicate;
5. target COMMIT did not take effect but the response is still uncertain; NOT_COMMITTED resolution performs one controlled retained-event replay and resolves only on success;
6. a historical durable-spool transaction loses its target COMMIT response; the DLQ is attached to the actual failed spool item, not to a newer request, and COMMITTED resolution drains it without a second target write;
7. the target COMMIT succeeds and the process fails before durable checkpoint; the pre-COMMIT ambiguity fence survives restart, blocks automatic redelivery, and COMMITTED resolution advances only the checkpoint;
8. NOT_COMMITTED recovery itself fails before COMMIT; the DLQ remains `REPLAY_REQUIRED`, later source flow is blocked, and an explicit replay is required to close the ordering gap.

The self-test is deterministic and database-independent. It does not replace retained external process-kill, proxy fault, disk-full, network-partition or multi-hour soak qualification.
