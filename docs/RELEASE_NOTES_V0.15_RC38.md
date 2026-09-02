# QMigration V0.15.0-rc38 Release Notes

RC38 stabilizes topology recovery after RC36/RC37 introduced running-chunk circuit drain and DEGRADED convergence. The scheduler now recovers conservatively instead of jumping from a single successful sample to unrestricted HEALTHY scheduling.

## Topology recovery hysteresis

- A DEGRADED topology must accumulate consecutive good samples before HEALTHY is restored.
- `QMIGRATION_TOPOLOGY_RECOVERY_MIN_DEGRADED_SECONDS` enforces a minimum dwell before concurrency begins to rise.
- A bad recovery sample resets `good_streak` and returns the effective cap to `QMIGRATION_TOPOLOGY_DEGRADED_MAX_CONCURRENCY`.
- CIRCUIT_OPEN still recovers only through the existing cooldown -> HALF_OPEN probe path.

## Staged concurrency recovery

The topology profile persists `good_streak` and `recovery_concurrency_cap`. After the dwell period, every configured group of good samples earns another concurrent chunk until `QMIGRATION_TOPOLOGY_RECOVERY_MAX_CONCURRENCY` is reached. Only after both the good-sample threshold and maximum recovery cap are reached is the topology marked HEALTHY.

Default controls:

```text
QMIGRATION_TOPOLOGY_RECOVERY_MIN_DEGRADED_SECONDS=30
QMIGRATION_TOPOLOGY_RECOVERY_STEP_GOOD_SAMPLES=2
QMIGRATION_TOPOLOGY_RECOVERY_HEALTHY_GOOD_SAMPLES=8
QMIGRATION_TOPOLOGY_RECOVERY_MAX_CONCURRENCY=4
```

If `QMIGRATION_TOPOLOGY_HEALTHY_MAX_CONCURRENCY` is configured, the recovery maximum is bounded by that healthy cap.

## Cooperative pressure relaxation

RC37 DEGRADED throttling now relaxes progressively as the topology earns a larger recovery cap. Batch percentage and byte budgets move toward 100%, while the DEGRADED pause is reduced. Running-chunk shed logic uses the same effective cap as new ClaimChunk decisions.

## Historical tail handling

Rolling P99 remains part of HEALTHY degradation detection, but HALF_OPEN/DEGRADED recovery uses the current committed chunk sample. This prevents an old tail outlier from blocking a successful probe until the entire latency sample window ages out.

## Observability

Prometheus adds:

- `qmigration_table_topology_good_streak`
- `qmigration_table_topology_recovery_concurrency_cap`
- `qmigration_table_topology_health_changed_timestamp_seconds`

The migration detail UI shows recovery cap and good streak next to DEGRADED topology health.
