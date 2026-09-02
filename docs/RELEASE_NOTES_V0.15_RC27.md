# QMigration V0.15.0-rc27 Release Notes

## Scope

RC27 closes two production-engineering gaps without overclaiming a GBase 8a source CDC protocol that has not been proven safe for heterogeneous row replication.

## GBase 8a target CDC apply

- New gate: `QMIGRATION_EXPERIMENTAL_GBASE8A_TARGET_CDC=1` (also requires the existing native GBase 8a gate).
- The connector now advertises `cdc-apply` and `point-lookup` under that gate.
- INSERT/UPDATE use the already-hardened HASH layout validation + per-batch staging + `MERGE` replay path.
- DELETE uses mapped stable-key delete.
- LAST_WRITE_WINS can use target point lookup.
- `cdc-transactional-apply` remains deliberately absent because GBase 8a MPP source-transaction atomicity is not qualified/portable.
- GBase 8a source CDC remains fail-closed: vendor cluster binary synchronization and audit SQL are not substituted for an exact row-image feed.

## Deterministic CDC chaos/failpoint framework

RC27 adds opt-in failpoints around the durability ordering that matters most to migration correctness:

- before/after durable spool persistence;
- before target apply;
- after target apply before spool mark;
- after spool mark;
- after target apply before durable checkpoint;
- after durable checkpoint before source ACK.

`qmigration-chaos-qualify` runs three end-to-end synthetic crash windows and verifies durable spool reuse, checkpoint duplicate suppression, and no second target write after a lost source ACK.

Fault injection is disabled by default. A malformed explicitly enabled plan fails closed.

## Precheck safety

Incremental precheck now emits a warning whenever a target advertises CDC apply but not transactional CDC apply, making the intermediate-visibility boundary explicit to operators.

## Qualification boundary

RC27 software tests do not promote GBase 8a target CDC to production maturity. Real GBase 8a version/topology/HASH-MERGE failure-window qualification remains required.
