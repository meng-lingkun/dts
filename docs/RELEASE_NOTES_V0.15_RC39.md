# QMigration V0.15.0-rc39 Release Notes

RC39 extends topology protection from a single source placement to **correlated failure domains**. A scheduler must not assume two topology IDs are independent when they live in the same region, zone, or rack.

## Fault-domain metadata

Topology labels are normalized to a canonical hierarchy:

- `region`
- `zone` (qualified by region when available)
- `rack` (qualified by region/zone when available)

The canonical map is persisted as `migration_chunks.fault_domain_json`, so ClaimChunk does not depend on a live source metadata query. OceanBase `ob_zone` maps directly to zone. TiDB store label JSON/key-value forms are parsed for common region/zone/rack keys when present.

## Cascading admission protection

For a healthy candidate topology, rack and zone correlation is immediate. Region correlation is deliberately hysteretic: by default at least two distinct zones in the same region must contain unhealthy peer evidence before the entire region receives a risk score. This prevents one bad AZ from throttling every healthy AZ in its region.

- independent / all peers HEALTHY: no additional domain cap;
- same rack/zone peer DEGRADED: default domain cap `2`;
- same rack/zone peer HALF_OPEN or CIRCUIT_OPEN: default domain cap `1`;
- region-wide risk: enabled only after `QMIGRATION_TOPOLOGY_FAULT_DOMAIN_REGION_MIN_UNHEALTHY_ZONES` distinct zones show unhealthy evidence (default `2`).

Candidates are ordered by topology health first, then fault-domain peer risk and current fault-domain running count. This keeps an unhealthy topology from becoming eligible merely because its domain is quiet, while preferring an equally healthy topology in an independent domain.

## Running-work pressure control

Already-running chunks are not killed or rebound to a different source topology. When a healthy topology shares a domain with an unhealthy peer, lease feedback cooperatively lowers pressure at committed-batch boundaries:

- DEGRADED peer: default batch/budget 75%, pause 100 ms;
- HALF_OPEN/CIRCUIT_OPEN peer: default batch/budget 50%, pause 250 ms.

Existing topology DEGRADED throttle and stricter backpressure remain authoritative; the smallest batch/budget and largest pause win.

## Configuration

```text
QMIGRATION_TOPOLOGY_FAULT_DOMAIN_PROTECTION=true
QMIGRATION_TOPOLOGY_FAULT_DOMAIN_DEGRADED_MAX_CONCURRENCY=2
QMIGRATION_TOPOLOGY_FAULT_DOMAIN_CRITICAL_MAX_CONCURRENCY=1
QMIGRATION_TOPOLOGY_FAULT_DOMAIN_REGION_MIN_UNHEALTHY_ZONES=2
QMIGRATION_TOPOLOGY_FAULT_DOMAIN_DEGRADED_BATCH_PCT=75
QMIGRATION_TOPOLOGY_FAULT_DOMAIN_DEGRADED_PAUSE_MS=100
QMIGRATION_TOPOLOGY_FAULT_DOMAIN_CRITICAL_BATCH_PCT=50
QMIGRATION_TOPOLOGY_FAULT_DOMAIN_CRITICAL_PAUSE_MS=250
```

## Observability

Prometheus adds:

- `qmigration_table_topology_fault_domain_info`
- `qmigration_table_topology_fault_domain_peer_risk`

## Safety / scope

- Missing fault-domain labels fail open only for the new domain layer; normal topology health/circuit protection remains unchanged.
- No SQL kill, shard ownership rewrite, or cross-DN source relocation is introduced.
- PolarDB-X logical `GROUP_NAME` is not guessed into a physical zone/rack when the source does not expose that relationship.
- Real multi-zone/rack vendor qualification and retained large-scale soak are still required before production certification.
