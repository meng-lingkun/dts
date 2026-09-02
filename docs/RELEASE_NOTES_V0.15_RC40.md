# QMigration V0.15.0-rc40 Release Notes

RC40 closes the main RC39 fault-domain scheduling gap: a risky rack/zone/region now converges work that was already RUNNING before the peer failure, not only future claims.

## Already-running domain convergence

When correlated peer risk establishes a domain cap, numeric range and bounded-keyset chunks above that cap receive `YieldAfterBatch` only after their current target batch is committed and a durable cursor exists. The remainder stays bound to the same source topology and fault domain.

Default caps remain those introduced by RC39:

- DEGRADED peer evidence: `QMIGRATION_TOPOLOGY_FAULT_DOMAIN_DEGRADED_MAX_CONCURRENCY=2`
- HALF_OPEN/CIRCUIT_OPEN evidence: `QMIGRATION_TOPOLOGY_FAULT_DOMAIN_CRITICAL_MAX_CONCURRENCY=1`

`QMIGRATION_TOPOLOGY_FAULT_DOMAIN_RUNNING_SHED=true` enables the convergence path by default.

## Survivor safety

All renewers calculate the same survivor set. Healthier topology work is retained first; ties use StartedAt, ChunkNo and ID. This avoids an unhealthy old chunk consuming the only domain slot while a healthy peer is shed.

HASH/CUSTOM and other chunks without a provable exact resume boundary are never force-yielded.

## Metadata continuity

RC40 also fixes an RC39 edge: cooperative remainders now copy `fault_domain_json` as well as `topology_id`, so the next claim remains protected by the same domain policy.

## Observability

- task field `adaptive_fault_domain_yields`
- WebSocket task progress field `adaptive_fault_domain_yields`
- Prometheus `qmigration_task_adaptive_fault_domain_yields_total`
- task detail UI item `故障域让出`

## Qualification boundary

Real vendor multi-zone/rack failures and retained 10-40TB long-duration soak remain required before production certification. RC40 still does not implement cross-DN source ownership relocation or asynchronous SQL cancellation.
