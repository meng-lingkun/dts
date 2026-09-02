# QMigration V0.14 Release Notes (Draft)

## Native Datasource TLS

V0.14 adds datasource-level TLS transport policy for Native MySQL-family and PostgreSQL-family connectors.

### Added

- `DISABLE`, `PREFERRED`, and `REQUIRED` TLS modes.
- MySQL `CLIENT_SSL` negotiation before authentication.
- PostgreSQL SSLRequest policy instead of unconditional opportunistic negotiation.
- TLS 1.2 minimum, hostname verification, system roots, and optional custom CA PEM.
- TLS propagation to full-load workers and native CDC/logical-replication sessions.
- PostgreSQL metadata persistence and Vue datasource controls.

### Safety

`REQUIRED` never silently downgrades to plaintext. `PREFERRED` falls back only when the server explicitly reports that TLS is unavailable; a TLS handshake or certificate verification failure remains a hard error.

### Remaining

Client-certificate/mTLS support is not included yet. External JDBC TLS continues to be configured by JDBC URL and the selected external engine.

## Parallel Bounded Keyset Full Load

V0.14 also absorbs the useful split-planning ideas from DataX/SeaTunnel into the Native path. Tables with textual/composite primary keys or a safe `UNIQUE NOT NULL` Migration Key no longer have to run as one keyset stream.

### Added

- Ordered `NTILE` source-boundary discovery for MySQL-family and PostgreSQL-family connectors.
- Gap-free `[lower, upper)` Keyset chunks for string, composite and UNIQUE migration keys.
- Immutable `start_cursor_json` / `end_cursor_json` persisted separately from the durable runtime `cursor_json`.
- Worker resume logic that combines immutable bounds with the last committed `> cursor` position.
- One stable full-table keyset checksum per logical table, de-duplicated across parallel chunks to avoid cross-collation boundary false positives.
- Planning cap of `parallelism * 4` (max 128) to provide queue depth without exploding metadata.

### Compatibility / Fallback

Boundary discovery is an optimization, not a correctness dependency. If the source database/version cannot execute the window-function boundary query, QMigration falls back to one durable keyset stream. Actual migration reads never use OFFSET-based checkpoints.

## Adaptive Chunk Refinement

V0.14 can now refine slow pending work instead of accepting the initial partition plan as immutable forever.

### Added

- Existing integer pending ranges can be bisected after a slow completed chunk exposes skew/long-tail behavior.
- Bounded keyset pending chunks can request an actual source median key inside their current `[lower, upper)` interval and split into two contiguous child ranges.
- Keyset refinement refuses chunks that already have a durable cursor or transferred rows, preserving resume semantics.
- PostgreSQL Repository now persists dynamically modified range/keyset bounds, fixing a restart-safety gap in the earlier adaptive-range implementation.

## Feedback Backpressure

Worker lease renewal is now also a control loop, not only a liveness heartbeat.

### Added

- Per-batch source read latency, target write latency and actual batch-row telemetry.
- Server-side `NORMAL / WARN / CRITICAL` pressure classification.
- Lease response `ChunkControl` with optional pause and maximum next-batch rows.
- Automatic batch shrink under database/worker pressure and gradual recovery when pressure clears.
- Metadata migration `012_v14_backpressure.sql`.

Default thresholds are configurable through `QMIGRATION_BACKPRESSURE_*` environment variables. The current loop intentionally reacts to observed migration-path latency before introducing vendor-specific monitoring dependencies.

## Worker Topology Affinity

V0.14 adds a generic placement layer that can later consume PolarDB-X DN, TiDB Region and OceanBase Partition topology without coupling the scheduler to one database vendor.

### Added

- Worker labels from `QMIGRATION_WORKER_LABELS` (CSV `key=value` or JSON object).
- Task-level `worker_selector` and `worker_affinity` (`PREFERRED` / `REQUIRED`).
- Affinity-aware in-memory and PostgreSQL Claim scheduling.
- Per-table running-chunk balancing as a secondary rank to reduce hotspot concentration.
- Vue task affinity controls and Worker label visibility.
- Metadata migration `013_v14_worker_affinity.sql`.

## Database Topology Advisory Placement / Task Flow Control

V0.14 dev4 connects the generic label scheduler to live database topology while keeping placement fail-safe and advisory.

### Added

- PolarDB-X table topology discovery grouped by physical `GROUP_NAME`.
- TiDB table Region leader discovery aggregated by TiKV Store.
- OceanBase leader Tablet discovery aggregated by Zone.
- Durable table topology inventory and per-Chunk `placement_hint / topology_id / topology_kind`.
- Placement-aware Claim ranking after explicit task affinity.
- MySQL-family runtime pressure sampling from `Threads_connected`, `Threads_running`, and `max_connections`.
- PostgreSQL-family runtime pressure sampling from `pg_stat_activity` and `max_connections`.
- Durable Task `effective_parallelism` and `flow_control_level / reason`.
- Critical pressure halves new-claim concurrency; warning pressure reduces one step; normal periods recover one step at a time.
- Metadata migration `014_v14_topology_flow_control.sql`.

### Safety

Topology placement is deliberately soft. A logical keyset/range does not always map exactly to one PolarDB-X DN, TiDB Region, or OceanBase Tablet. QMigration therefore uses topology only to rank eligible workers and falls back safely when labels do not match. Exact physical partition routing will only be enabled for database/version combinations where the mapping can be proven.

### Remaining

CPU/IO/QPS-aware flow control still requires external metrics integration (Prometheus/DAS/vendor APIs). Exact Partition/Region-aware split routing remains future work.
