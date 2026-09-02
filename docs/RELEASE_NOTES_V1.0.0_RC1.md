# QMigration V1.0.0 RC1 Release Notes

V1.0.0 RC1 consolidates the V0.1–V0.14 development line into the first feature-complete release candidate of the Go + Vue heterogeneous database migration platform.

## Product workflow

`Discover -> Assess -> Schema -> Full Load -> CDC -> Validate -> Catch-up -> Cutover -> Reverse CDC -> Rollback`

The release keeps QMigration as the control plane. DataX, SeaTunnel, Flink CDC, Debezium and Canal remain pluggable execution engines rather than becoming the product state model.

## Full-load planning and recovery

- Integer primary-key range chunks.
- Ordered bounded keyset chunks for string/composite PK and `UNIQUE NOT NULL` migration keys.
- Explicit `HASH`, `PARTITION`, `CUSTOM_SQL`, `PRIMARY_KEY_RANGE`, `UNIQUE_KEY_RANGE` and `AUTO` split strategies.
- Dynamic re-splitting of slow pending range/keyset work.
- Durable per-batch checkpoint and Worker lease recovery.
- Rows/s, QPS, read/write MB/s and time-window throttling.
- Task-level effective parallelism plus per-chunk latency backpressure.
- PolarDB-X Group/Partition, TiDB Store and OceanBase Zone advisory placement.

## Schema and metadata

- Universal Schema Model for portable scalar data types.
- MySQL-family <-> PostgreSQL-family table type conversion during automatic target creation.
- Table/column mapping, index/foreign-key deferred apply.
- View dependency ordering.
- PostgreSQL sequence state, SERIAL ownership and IDENTITY safety.
- Trigger/function/procedure discovery remains conservative: unsafe heterogeneous procedural SQL is never guessed automatically.

## CDC

- Native MySQL GTID/file-position reader and Native PostgreSQL pgoutput reader.
- Atomic apply-before-checkpoint semantics.
- Partial JSON, OPAQUE temporal/decimal, compressed transaction payload support for MySQL Native CDC.
- CDC DLQ, replay, duplicate-position suppression and conflict audit.
- `SOURCE_WINS` and version-column `LAST_WRITE_WINS`.
- SeaTunnel/Flink CDC renderers plus Debezium/Canal runner contracts.

## Security and governance

- AES-256-GCM database credential encryption.
- MySQL/PostgreSQL TLS `DISABLE/PREFERRED/REQUIRED`.
- Optional mTLS client certificate/private key; private key is encrypted at rest.
- Admin/DBA/Operator/Viewer RBAC, login/session token and worker token separation.
- Audit log, high-risk cutover/rollback authorization and explicit schema-object confirmation.

## Validation, cutover and rollback

- Canonical row count/checksum validation.
- Chunk-level mismatch localization and repair workflow.
- CDC lag gates before cutover and rollback.
- Reverse CDC orchestration and rollback state machine.
- PostgreSQL sequence freshness gate before cutover.

## Operations

- Prometheus `/metrics` and alert rules.
- WebSocket task/CDC/Worker live events.
- Task logs, platform alerts and audit events.
- `qmigrationctl` operations CLI.
- Docker and Kubernetes deployment manifests, Worker HPA, control-plane PDB.
- Metadata backup/restore scripts with SHA-256 verification.

## Release-candidate boundary

V1.0 RC1 is feature complete at the QMigration product/control-plane level. Vendor-specific procedural SQL conversion and databases that only have an External JDBC adapter continue to rely on the selected external migration/CDC engine; QMigration intentionally does not pretend those vendor log formats are natively implemented when they are not.
