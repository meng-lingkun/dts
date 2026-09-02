## V0.15.0-rc49

- DONE: multi-public-key offline Trust Store with ACTIVE / RETIRED / REVOKED state.
- DONE: signed Ed25519 key-transition certificates preserve historical validation while establishing a new signing key.
- DONE: signed key-revocation certificates fail closed for revoked report signers.
- DONE: `qmigrationctl verify-report --trust-store` validates full transition path plus artifact/manifest proof.
- DONE: atomic local Trust Store persistence; failed last-active-key revocation cannot corrupt state.
- FAIL-SAFE: a transitioned key cannot validate a report whose signed generated_at predates the transition.
- SECURITY BOUNDARY: a compromised retired private key can backdate a signed report; WORM/external trusted timestamping remains required for compromise-safe historical time.

## V0.15.0-rc47

- DONE: optional Ed25519 public signing of JSON/HTML/PDF artifacts and canonical manifest payload.
- DONE: public-key JSON export with key ID and SHA-256 fingerprint; signing private key never leaves server configuration.
- DONE: fully offline `qmigrationctl verify-report`, including optional externally pinned trust key.
- DONE: immutable external validation-report archive registry survives Memory/PostgreSQL plus Secure/file/S3 decorators.
- FAIL-SAFE: artifact/signature/hash/evidence/READY mismatch and archive registry mutation conflict are rejected.
- QUALIFICATION REQUIRED: real customer key rotation and retained AWS/MinIO Object-Lock/WORM lifecycle tests.

## V0.15.0-rc46

- DONE: immutable Validation Archive can be exported as deterministic JSON/HTML/PDF acceptance artifacts.
- DONE: artifact SHA-256 manifest and optional HMAC-SHA256 signatures/key-id metadata.
- DONE: S3-compatible external archive uses evidence-digest immutable paths and uploads READY.json only after all artifacts/manifest succeed.
- DONE: optional GOVERNANCE/COMPLIANCE Object Lock and legal hold are signed into requests and verified by HEAD after upload; stored bytes are GET/re-hashed before success.
- DONE: Web UI report download and S3/WORM archive actions plus Prometheus counters.
- FAIL-SAFE: configured WORM that cannot be observed/verified is reported as archive failure, never as success.
- REMAINING: asymmetric public-key signing, retained real S3/MinIO/Object-Lock qualification and customer-specific report branding/templates.

## V0.15.0-rc42

- DONE: production repository decorators preserve summary/hot-path/stat optional capabilities.
- DONE: worker renew/scheduler convergence no longer scans whole task Chunk history.
- DONE: adaptive split is table-local and obtains ChunkNo through aggregate MAX.
- DONE: PostgreSQL hot-path partial/expression indexes and metadata bloat Prometheus telemetry.
- INTENTIONAL O(N): validation and explicit Chunk listing remain full correctness/enumeration paths.
- QUALIFICATION REQUIRED: retained multi-day 10-40TB PostgreSQL EXPLAIN/BUFFERS + autovacuum/heap/GC soak.

## V0.15.0-rc41

- DONE: bounded metadata janitor for task logs, audit events and CDC position history.
- DONE: newest task+direction CDC checkpoint is never pruned; paused migrations retain resume safety.
- DONE: PostgreSQL prune work is indexed and batch bounded.
- DONE: progress/metrics chunk aggregation is repository-side and returns O(tables) rows on PostgreSQL.
- DONE: janitor Prometheus telemetry.
- QUALIFICATION REQUIRED: multi-day 10-40TB real-instance metadata/heap/GC soak and autovacuum calibration.

## V0.15.0-rc40

- DONE: already-running risky fault-domain work converges to the same domain cap as new ClaimChunk admission.
- DONE: survivor selection prefers healthier topology work, then oldest deterministic work.
- DONE: yielded numeric/keyset remainders retain source topology and canonical fault-domain metadata.
- DONE: `adaptive_fault_domain_yields` API/WebSocket/Web/Prometheus telemetry.
- FAIL-SAFE: HASH/CUSTOM/unprovable remainder types are not force-yielded.
- NOT CLAIMED: cross-DN source ownership relocation or vendor-native DN failover.
- QUALIFICATION REQUIRED: retained multi-zone/rack real-instance failure tests and 10-40TB soak.

## V0.15.0-rc39

- DONE: canonical region/zone/rack fault domains are derived from topology labels and persisted on chunks.
- DONE: independent healthy domains outrank correlated rack/zone failures; region risk requires unhealthy evidence from multiple zones.
- DONE: correlated-domain admission caps are enforced identically by Memory and PostgreSQL schedulers.
- DONE: already-running healthy work in a risky domain receives cooperative batch/byte/pause throttle.
- DONE: TiDB store labels expose common fault-domain labels when available; OceanBase zones participate natively.
- DONE: Prometheus exposes per-topology fault-domain identity and peer risk.
- FAIL-SAFE: missing/unknown fault-domain metadata disables only domain-level protection; existing topology health gates remain active.
- NOT CLAIMED: PolarDB-X physical region/zone/rack inference from logical group names where the source does not expose those labels.
- QUALIFICATION REQUIRED: retained multi-zone/rack real-instance failure tests and 10-40TB soak.

## V0.15.0-rc38

- DONE: DEGRADED -> HEALTHY recovery requires consecutive good samples plus a configurable minimum degraded dwell.
- DONE: recovery concurrency ramps from degraded cap toward `QMIGRATION_TOPOLOGY_RECOVERY_MAX_CONCURRENCY` before HEALTHY is restored.
- DONE: bad recovery samples reset good-streak and collapse the recovery cap to the conservative degraded value.
- DONE: PostgreSQL ClaimChunk, in-memory scheduling, running-chunk shedding and cooperative throttling share the same effective topology cap.
- DONE: successful HALF_OPEN probes can recover despite historical P99 outliers; current-sample evidence drives recovery while rolling P99 remains a degradation signal.
- DONE: Prometheus/Web expose recovery cap and good-streak state.
- FAIL-SAFE: CIRCUIT_OPEN cannot bypass cooldown by racing in a late good sample; only HALF_OPEN may enter staged recovery.
- Connector maturity unchanged from RC37.

## V0.15.0-rc37

- DONE: already-running DEGRADED topology work converges to `QMIGRATION_TOPOLOGY_DEGRADED_MAX_CONCURRENCY` at durable cursors.
- DONE: deterministic oldest-first survivor set prevents all concurrent workers from yielding simultaneously.
- DONE: surviving DEGRADED work receives cooperative batch/pause/byte-budget throttling.
- DONE: `adaptive_topology_degraded_yields` API/WebSocket/Web/Prometheus telemetry.
- FAIL-SAFE: only numeric range / bounded keyset chunks are force-yielded; unprovable remainder types are not split.
- NOT CLAIMED: cross-topology source ownership relocation or vendor-native source DN failover.
- Connector maturity unchanged from RC36.

## V0.15.0-rc36

- DONE: committed-batch/durable-cursor cooperative drain for running numeric/keyset chunks on `CIRCUIT_OPEN`.
- DONE: exact gap-free remainder reconstruction with original topology binding retained.
- DONE: `adaptive_topology_drains` API/WebSocket/Web/Prometheus telemetry.
- FAIL-SAFE: HASH/CUSTOM chunks are not force-drained because an exact remainder cannot be proven generically.
- NOT CLAIMED: async SQL kill, cross-DN ownership relocation, vendor-native DN failover.
- Connector maturity unchanged from RC35.

## V0.15.0-rc35

- DONE: topology circuit scheduling and automatic HALF_OPEN recovery.
- DONE: P95/P99 tail-adjusted SLA ETA/risk telemetry.
- Connector maturity unchanged.

# QMigration Implementation Status

## V0.15.0-rc40

- DONE: already-running risky fault-domain work converges to the same domain cap as new ClaimChunk admission.
- DONE: survivor selection prefers healthier topology work, then oldest deterministic work.
- DONE: yielded numeric/keyset remainders retain source topology and canonical fault-domain metadata.
- DONE: `adaptive_fault_domain_yields` API/WebSocket/Web/Prometheus telemetry.
- FAIL-SAFE: HASH/CUSTOM/unprovable remainder types are not force-yielded.
- NOT CLAIMED: cross-DN source ownership relocation or vendor-native DN failover.
- QUALIFICATION REQUIRED: retained multi-zone/rack real-instance failure tests and 10-40TB soak.

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

## V0.15.0-rc28

### GBase 8s event-owned smart BLOB/CLOB source CDC

- [x] Agent API v4 and native C provider ABI v4 make the RC28 image contract mandatory
- [x] selected BLOB/CLOB columns are explicitly tagged in the provider table-selection contract
- [x] optional `QMIGRATION_EXPERIMENTAL_GBASE8S_SMART_LOB_CDC=1` gate; ordinary GBase 8s CDC remains separately gated
- [x] checkpoint/read responses for selected smart LOBs must declare `cdc-event-owned-lob-v1`
- [x] every non-NULL BLOB/CLOB image requires column/kind/byte-length/SHA-256/acquisition proof
- [x] BLOB values remain byte-exact base64; CLOB proof hashes the exact transported UTF-8 bytes
- [x] proof kind/column/hash/length/acquisition/NULL semantics are validated before target apply
- [x] current-row SQL SELECT fallback is explicitly rejected because it cannot prove the historical CDC event image
- [x] INSERT/DELETE/UPDATE-before/UPDATE-after all use the same proof validator
- [x] `qmigration-gbase8s-qualify --cdc-smart-lob` verifies the provider declares the event-owned image contract
- [ ] retained real GBase 8s V8.8 CSDK locator/stream qualification must prove historical bytes under CDC lag and later-row updates
- [ ] smart LOB values above the configured provider/read response bounds remain fail-closed; unbounded chunk streaming is future work
- [ ] TEXT/BYTE/opaque/collection/UDT source CDC remains outside this contract

### Target COMMIT outcome uncertainty guard

- [x] any transactional target `CommitCDCTransaction` error is treated as an unknown outcome rather than an ordinary retryable apply failure
- [x] QMigration never issues an automatic rollback after a COMMIT response error because the target may already have committed
- [x] durable CDC DLQ status `COMMIT_UNCERTAIN` blocks later CDC in the same task/direction
- [x] automatic DLQ replay is forbidden while commit outcome is uncertain
- [x] operator `COMMITTED` resolution advances the retained source checkpoint without replaying target DML
- [x] operator `NOT_COMMITTED` resolution reopens the retained item and immediately executes one explicit replay, resolving it only on success
- [x] `POST /api/v1/migrations/{id}/cdc/dlq/{dlq_id}/resolve-commit` and Web UI resolution actions
- [x] Prometheus `qmigration_cdc_commit_uncertain` gauge
- [x] `qmigration-chaos-qualify` covers both COMMITTED and NOT_COMMITTED target-commit-unknown decisions; neither permits duplicate target writes
- [x] durable pre-COMMIT ambiguity fence retains the exact transaction before target COMMIT and is cleared only after durable checkpoint
- [x] process failure after target COMMIT but before checkpoint restarts blocked instead of blindly replaying target DML
- [x] historical durable-spool COMMIT uncertainty is attached to the actual failed spool transaction rather than a newer live request
- [x] failed NOT_COMMITTED controlled replay remains `REPLAY_REQUIRED` and blocks later source flow until explicit replay succeeds
- [x] `qmigration-chaos-qualify` covers eight deterministic durability/recovery windows
- [ ] external proxy/network process-kill qualification at the target COMMIT response boundary remains future soak work

## V0.15.0-rc27

### GBase 8a target CDC apply (non-transactional)

- [x] explicit `QMIGRATION_EXPERIMENTAL_GBASE8A_TARGET_CDC` gate
- [x] advertise target `cdc-apply` + `point-lookup` only under the gate
- [x] INSERT/UPDATE reuse validated HASH staging+MERGE idempotent path
- [x] stable-key DELETE + LAST_WRITE_WINS point lookup
- [x] target precheck warns when CDC apply lacks source-transaction atomicity
- [x] do not advertise `cdc-transactional-apply`
- [x] do not substitute audit SQL/cluster rsync for source CDC
- [ ] retained GBase 8a target CDC failure-window/visibility qualification on real clusters
- [ ] generic exact row-image GBase 8a source CDC API remains unavailable/unqualified

### Deterministic CDC chaos qualification

- [x] opt-in failpoint parser with exact-occurrence triggers and fail-closed malformed plans
- [x] spool-before-persist and spool-after-persist-before-ACK injection points
- [x] before-target-apply and after-target-apply-before-spool-mark injection points
- [x] after-spool-mark injection point
- [x] after-target-before-checkpoint and after-checkpoint-before-source-ACK injection points
- [x] persisted-spool-before-ACK retry reuses one durable spool record
- [x] apply-before-spool-mark retry uses checkpoint duplicate suppression and avoids a second target write
- [x] checkpoint-before-source-ACK retry is detected as duplicate
- [x] `qmigration-chaos-qualify` standalone JSON self-test
- [x] `deployments/scripts/qualify-chaos.sh` wrapper
- [ ] external process kill / network partition / disk-full multi-hour soak remains qualification work

## V0.15.0-rc26

### openGauss / KingbaseES product-native Source CDC

- [x] openGauss dedicated `OPENGAUSS_LSN` checkpoint and `mppdb_decoding` SQL logical reader
- [x] openGauss complete BEGIN/COMMIT/XID transaction assembly, selected-table filter and apply-before-slot-advance ACK
- [x] openGauss PK/replication/slot/sender/SSL prechecks and fail-closed binary/partial transaction handling
- [x] `qmigration-opengauss-cdc` + `qmigration-opengauss-qualify`
- [x] Kingbase dedicated `KINGBASE_LSN` checkpoint using `sys_current_wal_lsn()`
- [x] Kingbase `sys_create_logical_replication_slot(...,'kboutput')`, `sys_drop_replication_slot`, `sys_replication_slots` and `sys_publication` integration
- [x] Kingbase managed stream keeps a distinct `kingbase:` event namespace and rejects a slot whose output plugin is not `kboutput`
- [x] strict Kingbase `kboutput` decoder dialect and `qmigration-kingbase-qualify` qualification entry point
- [x] openGauss/Kingbase CDC remains behind independent experimental gates; Full Load remains available without the gates
- [ ] retained openGauss version/topology/SSL/restart/failover qualification
- [ ] retained Kingbase `kboutput` wire-conformance/version/restart/failover qualification
- [ ] openGauss binary/NUL-safe logical value path and source DDL are not advertised
- [ ] Kingbase source DDL/TRUNCATE and exact-at-LSN validation snapshot are not advertised

## V0.15.0-rc25

### TiDB/TiCDC Kafka production hardening

- [x] multi-partition Kafka metadata/fetch path; no single-partition runtime assumption
- [x] `TIDB_TSO` durable position supports deterministic per-partition next offsets
- [x] TiCDC Resolved TS on every partition is the global transaction ordering fence
- [x] candidate commitTs waits until every partition has `resolvedTS > commitTs`
- [x] future prefetched records keep their earliest unprocessed Kafka offset in the checkpoint
- [x] old non-zero single-partition checkpoints fail closed if topic topology expands
- [x] Kafka TLS 1.2+, custom CA, ServerName and optional mTLS
- [x] Kafka SASL/PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512 native authentication
- [x] unsupported SASL mechanisms and missing credentials fail during endpoint/client precheck
- [x] TiCDC sink URI carries partition/TLS/SASL settings and qualification output redacts passwords
- [x] synthetic tests cover multi-partition ordering/fencing/checkpoints and SASL wire/crypto primitives
- [ ] retained real TiDB/TiCDC/Kafka multi-broker/rebalance/restart/security/GC qualification

### Exact watermark validation foundation

- [x] `VALIDATING` no longer permits target CDC apply; source capture remains active and spools durably
- [x] barrier capture + `VALIDATING` transition serialized by the same forward CDC spool/apply lock
- [x] connector SPI adds `validation-snapshot` exact historical read capability
- [x] TiDB implements an independent `SESSION tidb_snapshot=<TIDB_TSO>` validation connection
- [x] TiDB snapshot session verifies `@@tidb_snapshot` before validation reads
- [x] `QMIGRATION_VALIDATION_REQUIRE_EXACT_WATERMARK=1` fails closed for connectors without exact snapshot support
- [x] Oracle `ORACLE_SCN` and Dameng `DM_LSN` exact historical snapshots
- [ ] exact historical snapshots for remaining GTID/LSN/LRI families


### Oracle/DM exact validation + DM8 archived-log CDC

- [x] Oracle LogMiner capability advertises `validation-snapshot` and validates `ORACLE_SCN` with read-only `AS OF SCN` source reads
- [x] Dameng adds separate `QMIGRATION_EXPERIMENTAL_DAMENG_LOG_CDC` gate
- [x] Dameng `DM_LSN` captures the newest fully archived LogMiner-safe position
- [x] continuous local archive coverage is checked before a DBMS_LOGMNR read window
- [x] COMMIT/XA_COMMIT `START_SCN` rewinds LogMiner for long transactions that began before the durable checkpoint
- [x] committed DM LogMiner records are grouped by XID/COMMIT_SCN and selected table/ROW_ID
- [x] complete DM before/after rows are reconstructed with `AS OF SCN commit-1/commit`; SQL_REDO literals are not parsed into row data
- [x] repeated UPDATEs are coalesced to transaction net effect; INSERT->DELETE emits checkpoint-only progress
- [x] selected DDL/BATCH_UPDATE/UNSUPPORTED, missing ROWID/PK, archive gaps and unavailable flashback history fail before checkpoint advancement
- [x] `qmigration-dameng-cdc` Reader/Unified Engine integration uses apply-before-ACK `DM_LSN` checkpoints
- [x] Dameng implements exact `DM_LSN` validation snapshots and read-only enforcement
- [x] `qmigration-dameng-qualify --cdc` checks LogMiner prerequisites, archived position, PK selection and exact flashback snapshot
- [ ] retained real DM8 version/topology/archive-switch/UNDO/LOB/restart qualification; MPP/DPC-specific semantics are not promoted

- [x] same-`COMMIT_SCN` XIDs are aggregated before target apply so one durable `DM_LSN` cannot suppress another committed source transaction

## V0.15.0-rc24

### GBase 8s capture-lineage fence + observability regression repair

- [x] checkpoint requires a 64-hex provider `capture_lineage`
- [x] durable `GBASE8S_CDC_SEQ` persists `restart`, `commit` and `capture`
- [x] every read sends `expected_capture_lineage` and rejects mismatched provider lineage before row apply
- [x] acknowledgement rejects lineage changes even if restart/commit numbers are monotonic
- [x] RC23/older lineage-less checkpoints fail closed with an explicit re-capture requirement
- [x] Agent API v3 and native C ABI v3 make the new contract non-optional
- [x] RC22 `/v1/status` and `/metrics` observability restored on top of RC23 schema/TRUNCATE code
- [x] status retains latest capture lineage as exact diagnostics; Prometheus does not export lineage/sequence values
- [x] qualifier again validates the status endpoint
- [x] synthetic native-provider and Reader tests cover stable lineage and lineage change failure
- [ ] retained real GBase 8s V8.8 provider restart/resume must prove lineage persistence for the same capture and rotation for capture recreation

## V0.15.0-rc23

### GBase 8s CDC schema-drift fence

- [x] deterministic per-table SHA-256 fingerprint over column order/name/full type/nullability and primary-key membership/order
- [x] current catalog fingerprint captured during selected-table CDC checkpoint
- [x] persisted migration-table fingerprint recreated by the CDC Engine on Worker restart
- [x] checkpoint and every ReadResponse require one matching schema fence per selected table
- [x] missing/duplicate/malformed/mismatched schema fences fail before row apply
- [x] forwarded CDC_REC_TABSCHEMA/TABLE_SCHEMA fingerprint checked against planned schema
- [x] Agent API v2 rejects RC20/21/22 v1 agents
- [x] native C provider ABI v2 rejects stale ABI-v1 providers at load time
- [x] active row-image full-column/order/base64 checks retained as a second independent fence
- [ ] real GBase 8s V8.8 CSDK TABLE_SCHEMA/catalog normalization qualification
- [ ] smart BLOB/CLOB exact historical image contract remains future work

## V0.15.0-rc22

### GBase 8s transactional Source TRUNCATE

- [x] decode documented CDC_REC_TRUNCATE user-data/table identity into a first-class `CDCTruncate` event
- [x] require TRUNCATE to have no row payload and to belong to an open transaction
- [x] enforce GBase 8s source rule: no INSERT/DELETE/UPDATE/DISCARD/second TRUNCATE after TRUNCATE before COMMIT/ROLLBACK
- [x] preserve preceding source DML + TRUNCATE in one QMigration transaction
- [x] add `TruncateTableConnector` target primitive rather than translating TRUNCATE through heterogeneous DDL strings
- [x] GBase 8s target executes TRUNCATE only inside an active CDC transaction and commits immediately after the final event
- [x] target apply rejects TRUNCATE when the target lacks transactional truncate support
- [x] smart BLOB/CLOB/complex source values remain fail-closed because CDC does not directly return their content
- [ ] retained real GBase 8s V8.8 CSDK TRUNCATE commit/rollback/restart qualification
- [ ] smart BLOB/CLOB separate-locator retrieval remains future work

## V0.15.0-rc21

### GBase 8s native C ABI CDC provider + conformance hardening

- [x] preferred Linux native C ABI v1 provider loaded with `dlopen(RTLD_NOW|RTLD_LOCAL)`; legacy Go plugin remains compatibility-only
- [x] native provider absolute-path/regular-file/world-writable checks and optional exact SHA-256 pin
- [x] provider-local JSON configuration file size/permission validation
- [x] synthetic `.so` integration test covers ABI version, Health, Checkpoint, Read, Close and wrong-SHA rejection
- [x] CGO-disabled build keeps an explicit unsupported native-provider fallback instead of silently changing behavior
- [x] datasource-local provider calls serialized to protect one smart-LOB CDC session from concurrent reads
- [x] complete selected-column/order/NULL/base64 conformance validation before CDC apply
- [x] provider response `max_records`/`max_bytes` enforcement and UPDATE_BEFORE memory accounting
- [x] empty committed transactions emit CHECKPOINT events so durable CDC watermarks continue advancing
- [x] `restart <= commit`, monotonic restart ACK and long-transaction restart watermark invariants
- [x] non-loopback agent requires TLS + Bearer token; remote plaintext/userinfo URLs fail closed
- [x] provider kind/ABI/SHA-pin status included in `/v1/health` and retained qualification reports
- [x] native ABI header, compileable example and build helper
- [ ] real GBase 8s V8.8 Client-SDK native provider build and retained syscdcv1/smart-LOB qualification
- [ ] source TRUNCATE and smart BLOB/CLOB/complex values remain fail-closed


## V0.15.0-rc20

### GBase 8s syscdcv1/CSDK source CDC

- [x] separate source CDC from RC19 ODBC SQL transport; use a datasource-local CSDK smart-LOB provider
- [x] bundled `qmigration-gbase8s-cdc-agent` HTTP/TLS/token wrapper + local provider plugin contract
- [x] selection-aware checkpoint captured before Full starts
- [x] deterministic selected-table user-data IDs independent of mapping order
- [x] `GBASE8S_CDC_SEQ` durable `restart=<open BEGIN>;commit=<applied COMMIT>` position
- [x] BEGIN/COMMIT/ROLLBACK, INSERT/DELETE, UPDATE before/after and DISCARD transaction assembly
- [x] apply-before-ACK and duplicate committed transaction suppression after restart
- [x] 100,000 events / 128 MiB / 10,000 open transaction safety gates
- [x] provider next-sequence monotonic continuation contract
- [x] RC20 fail closed on TRUNCATE and smart BLOB/CLOB/complex source columns
- [x] `qmigration-gbase8s-cdc`, provider agent, protocol docs and `--cdc` qualification flow
- [ ] retained real GBase 8s V8.8/CSDK syscdcv1 restart/long-transaction/failover qualification
- [ ] vendor CSDK provider plugin real build in this environment


## V0.15.0-rc19

### GBase 8s V8.8 Native Full + target data plane

- [x] add a distinct `gbase8s` product family; do not reuse GBase 8a packet/EXPRESS semantics
- [x] qualification-gated vendor CSDK/ODBC `database/sql` transport provider
- [x] reject UID/PWD in persisted ODBC DSN; inject encrypted datasource credentials only in memory
- [x] systables/syscolumnsext/sysconstraints/sysindexes Metadata with composite PK/index order
- [x] numeric/composite keyset Full Read and NTILE/ROW_NUMBER boundary planning
- [x] conservative GBase 8s target type conversion and pre-existing owner policy
- [x] target table/PK/index/FK DDL
- [x] stable-key prepared UPDATE -> existence -> INSERT replay path
- [x] exact BLOB/binary database/sql binds
- [x] point lookup/delete + explicit transactional target CDC Apply
- [x] GBase 8s prechecks and `qmigration-gbase8s-qualify` workflow
- [x] QMigration TLS PREFERRED/REQUIRED fail closed until CSDK SSL mapping is retained-qualified
- [x] source CDC remains unadvertised despite syscdcv1/full-row-logging facilities
- [ ] retained GBase 8s V8.8/CSDK/unixODBC/charset/topology qualification
- [ ] supported durable source CDC consume/checkpoint/ACK API qualification

### Remaining highest-priority connector gaps

- [ ] GBase 8s retained real-instance Full/target qualification + source CDC API decision
- [ ] GBase 8a retained Full/HASH-MERGE qualification and distribution-skew guidance
- [ ] GaussDB retained Full/binary CDC/DDL-only qualification + multi-primary
- [ ] Dameng retained qualification + supported source CDC API
- [ ] DB2 retained real-instance qualification / pureScale


## V0.15.0-rc18

### GBase 8a HASH/MERGE correctness hardening

- [x] correct RC17 random-target/MERGE mismatch: auto-created Full targets are HASH distributed
- [x] choose a HASH-eligible distribution column only from the stable migration key
- [x] support composite migration keys by skipping temporal/LOB members that cannot be GBase HASH columns
- [x] fail automatic target creation when no migration-key member is HASH-eligible
- [x] validate actual target layout with `SHOW CREATE TABLE` before staging/MERGE
- [x] reject random and REPLICATED MERGE targets before staging side effects
- [x] reject pre-created HASH targets whose distribution columns are not contained in the MERGE/migration key
- [x] connector-local validated-layout cache is reset on process/connector restart, forcing fresh Worker qualification
- [x] qualification target-write path exercises HASH create + layout validation + replay MERGE + binary round trip
- [x] source CDC / transactional target CDC apply remain unadvertised
- [ ] retained GBase 8a real-instance V9.5.x HASH MERGE/restart qualification
- [ ] workload-specific skew/cardinality qualification for automatically selected migration-key HASH columns

### Remaining highest-priority connector gaps

- [ ] GBase 8a retained real-instance Full/HASH-MERGE failure-window qualification
- [ ] GaussDB retained real-instance Full/binary CDC/DDL-only qualification + multi-primary
- [ ] Dameng retained real-instance qualification + supported source CDC API
- [ ] DB2 retained real-instance qualification / pureScale


## V0.15.0-rc17

### GBase 8a MPP Native Full data plane

- [x] fix product scope to GBase 8a MPP Cluster; GBase 8s/8c are not implied
- [x] replace GBase generic/external-JDBC placeholder with a distinct qualification-gated Connector Factory
- [x] reuse QMigration native MySQL/GBase-compatible packet transport without inheriting MySQL Binlog CDC capability
- [x] information_schema Schema/Table/Column/PK/Index metadata
- [x] numeric/composite stable-key Full Read and ordered NTILE keyset boundaries
- [x] GBase-specific type conversion and `ENGINE=EXPRESS` target creation
- [x] auto-created target deliberately uses random distribution instead of guessing HASH/REPLICATED placement
- [x] keyless GBase target Full Write fails closed
- [x] keyed target Full Write uses per-batch staging table + GBase MERGE instead of MySQL ON DUPLICATE KEY
- [x] AUTO_INCREMENT is not copied onto auto-created target because QMigration writes explicit source values
- [x] GBase source CDC, target transactional CDC apply, FK replay and post-load schema capabilities are not advertised
- [x] `qmigration-gbase-qualify` + `deployments/scripts/qualify-gbase.sh`
- [ ] retained GBase 8a real-instance version/topology/charset/auth/TLS qualification
- [ ] retained long-running Full/restart and staging+MERGE failure-window qualification
- [ ] production HASH/REPLICATED target layout qualification

### Remaining highest-priority connector gaps

- [ ] GBase 8a retained real-instance qualification and workload-aware target layout guidance
- [ ] GaussDB retained real-instance Full/binary CDC/DDL-only qualification + multi-primary
- [ ] Dameng retained real-instance qualification + supported source CDC API
- [ ] DB2 retained real-instance qualification / pureScale


## V0.15.0-rc16

### GaussDB DDL-only replay with binary DML preservation

- [x] enable a separate DDL classification pass through `pg_logical_slot_peek_changes` with `enable-ddl-decoding=true`
- [x] keep DML values on the RC15 `pg_logical_slot_*_binary_changes` path at the exact same commit-LSN boundary
- [x] merge DDL-only and DML-only source transactions back into commit order before target apply
- [x] DDL-only ACK uses text `pg_logical_slot_get_changes`; DML ACK remains byte-safe binary `get_binary_changes`
- [x] reject a decoded transaction containing both DDL and DML
- [x] require explicit worker policy `QMIGRATION_GAUSSDB_DDL_ONLY_TRANSACTIONS=1` because GaussDB itself can omit DML after DDL in hybrid transactions
- [x] require source `enable_logical_replication_ddl=on` when DDL replay is requested
- [x] safe replay subset is limited to selected-table `ALTER TABLE`, `TRUNCATE`, and `CREATE [UNIQUE] INDEX`
- [x] reject multi-statement DDL, CONCURRENTLY, unselected-table DDL and unsupported object families
- [x] GaussDB -> GaussDB identity mapping can use `cdc_ddl_mode=SAME_FAMILY`; heterogeneous DDL remains rejected
- [x] synthetic tests cover DDL-only classification, mixed DDL/DML rejection, safe-subset filtering, binary/text merge ordering and planner policy behavior
- [ ] retained GaussDB real-instance DDL-only qualification on centralized/distributed supported releases
- [ ] multi-primary logical decoding qualification
- [ ] hybrid DDL/DML replay remains impossible to claim while the source decoder itself omits changes

### Remaining highest-priority connector gaps

- [ ] GaussDB retained real-instance Full/binary CDC/DDL-only qualification + multi-primary
- [ ] Dameng retained real-instance qualification + supported source CDC API
- [ ] DB2 retained real-instance qualification / pureScale
- [ ] GBase Native Connector


## V0.15.0-rc15

### GaussDB byte-safe binary logical CDC

- [x] replace RC14 JSON source value path with documented `pg_logical_slot_*_binary_changes` functions
- [x] preserve non-advancing peek -> target transaction/durable checkpoint -> source-slot ACK ordering
- [x] decode documented big-endian B/C/I/U/D frames with strict frame lengths and P/F delimiters
- [x] validate BEGIN/COMMIT transaction boundaries, XID and commit LSN before returning a transaction
- [x] length-delimited tuple decoder distinguishes SQL NULL from non-NULL empty value
- [x] embedded NUL/non-UTF8 values use byte-preserving base64 CDC fields
- [x] PostgreSQL-compatible OID 17 `bytea` `\\x...` values restore exact bytes before CDC apply
- [x] binary tables are no longer rejected solely because JSON logical decoding was unsafe
- [x] `qmigration-gaussdb-qualify --cdc` executes the same temporary binary peek path used by the worker
- [x] `enable-ddl-decoding=false` remains explicit rather than silently mixing DDL and row events
- [x] synthetic protocol tests cover INSERT/UPDATE/DELETE, NULL/empty, NUL/non-UTF8, bytea, malformed frames, XID/LSN mismatch and partial transactions
- [x] full `go test ./...`, `go vet ./...` and GaussDB Connector race coverage
- [ ] retained centralized/distributed GaussDB real-instance binary CDC qualification
- [ ] source DDL transaction-model implementation and retained qualification
- [ ] multi-primary logical decoding qualification
- [ ] remove GaussDB experimental gates only after exit criteria

### Remaining highest-priority connector gaps

- [ ] GaussDB retained real-instance binary CDC + DDL/multi-primary extensions
- [ ] Dameng retained real-instance qualification + supported source CDC API
- [ ] DB2 retained real-instance qualification / pureScale
- [ ] GBase Native Connector


## V0.15.0-rc14

### GaussDB QMigration data plane + SQL logical CDC

- [x] remove GaussDB from generic external-JDBC placeholder routing
- [x] qualification-gated PostgreSQL-wire Metadata / Full Read / Full Write / target transactional CDC Apply
- [x] numeric/composite keyset and existing ordered boundary/partition/runtime integration
- [x] `GAUSSDB_LSN` current position via `pg_current_xlog_location()`
- [x] explicit LSN-ordered `mppdb_decoding` logical slot with `output_order=0`
- [x] `pg_logical_slot_peek_changes` non-advancing complete transaction capture
- [x] JSON INSERT / UPDATE / DELETE decoder with BEGIN/COMMIT/XID validation
- [x] `white-table-list` selected-table decoding and primary-key precheck
- [x] target Apply + durable checkpoint before `pg_logical_slot_get_changes(..., commit_lsn)` source ACK
- [x] worker failover resumes from durable source slot rather than an in-memory cursor
- [x] 100,000 event / 128 MiB decoded-transaction bounds
- [x] binary/NUL-sensitive JSON CDC families fail closed
- [x] GaussDB logical-replication GUC and SYSADMIN/REPLICATION/gs_role_replication prechecks
- [x] `qmigration-gaussdb-qualify` + `qualify-gaussdb.sh`
- [x] synthetic transaction/gate/query tests and Unified Engine render coverage
- [ ] retained centralized/distributed GaussDB real-instance Full + CDC qualification
- [ ] byte-safe binary CDC path
- [ ] source DDL decoding/replay qualification
- [ ] multi-primary logical decoding qualification
- [ ] remove GaussDB experimental gates only after exit criteria

### Remaining highest-priority connector gaps

- [ ] GaussDB retained real-instance qualification + binary/DDL extensions
- [ ] Dameng retained real-instance qualification + supported source CDC API
- [ ] DB2 retained real-instance qualification / pureScale
- [ ] GBase Native Connector


## V0.15.0-rc13

### Dameng / DM8 QMigration data plane

- [x] remove Dameng from generic external-JDBC placeholder routing
- [x] qualification-gated Dameng Connector in Server and Worker
- [x] schema/table/column/PK/index/FK metadata discovery
- [x] exact primary-index identification instead of column-overlap heuristics
- [x] numeric and composite bounded keyset Full Read
- [x] ordered NTILE boundary planning
- [x] target schema/table/composite PK/index/FK creation
- [x] prepared keyless INSERT and keyed idempotent MERGE; row values never rendered into SQL
- [x] numeric fail-closed validation plus BLOB/binary prepared parameters
- [x] point lookup/delete and explicit target CDC begin/commit/rollback
- [x] Linux runtime DM database/sql provider plugin loading without vendoring proprietary driver source
- [x] provider/driver absence produces an explicit qualification/precheck failure
- [x] Dameng TLS PREFERRED/REQUIRED fail closed until provider-specific TLS properties are qualified
- [x] `qmigration-dameng-qualify`, `qualify-dameng.sh` and provider plugin build helper
- [x] synthetic connector tests plus full `go test ./...`, `go vet ./...` and race coverage
- [ ] retained DM8 real-instance Full/target qualification reports
- [ ] provider TLS qualification and secure DSN mapping
- [ ] Dameng source CDC/log API implementation and retained qualification
- [ ] remove `QMIGRATION_EXPERIMENTAL_DAMENG_NATIVE` only after exit criteria

### Remaining highest-priority connector gaps

- [ ] Dameng retained real-instance qualification + source CDC
- [ ] DB2 retained real-instance qualification / pureScale
- [ ] GaussDB Native Connector
- [ ] GBase Native Connector


## V0.15.0-rc12

### DB2 VECTOR target end-to-end software path

- [x] recognize VECTOR catalog types without breaking Db2 11.5 metadata discovery
- [x] preserve `SYSCAT.COLUMNS.LENGTH` as VECTOR dimension and query `COORDINATETYPE` only for VECTOR tables
- [x] normalize metadata to `VECTOR(dimension,FLOAT32|INT8)` and fail closed on missing/invalid shape
- [x] auto-create DB2 VECTOR target columns with exact dimension/coordinate type
- [x] use `VECTOR(CAST(? AS CLOB),dimension,coordinate-type)` around prepared parameter markers
- [x] validate serialized vector syntax, exact coordinate count, INT8 range and finite FLOAT32 values before target send
- [x] small VECTOR values use SQLDTA string parameters; large VECTOR strings reuse CLOB/EXTDTA
- [x] Full Load and transactional CDC Apply reuse the same prepared VECTOR writer
- [x] point lookup/readback continues through `VECTOR_SERIALIZE()` for representation consistency
- [x] `qmigration-db2-qualify --target-vector` and `DB2_QUALIFY_TARGET_VECTOR=1` destructive real-instance workflow
- [x] synthetic tests cover DDL, prepared constructor SQL, malformed metadata/value rejection and large-vector EXTDTA
- [ ] retained Db2 12.1.2+ VECTOR target reports and 12.1.4+ source-CDC VECTOR reports
- [ ] retained DB2 11.5 / 12.1 qualification for remaining row/log cases
- [ ] multi-insert with external data required by more than one row remains fail-closed until a documented row-ownership signal exists
- [ ] retained decomposed-update rollback ordering beyond currently qualified row-level paths
- [ ] pureScale multi-log-stream ordering/failover qualification
- [ ] remove DB2 experimental gates only after retained exit criteria pass

### Remaining highest-priority connector gaps

- [ ] DB2 retained real-instance qualification + remaining advanced row/log cases
- [ ] Dameng Native Connector
- [ ] GaussDB Native Connector
- [ ] GBase Native Connector


## V0.15.0-rc11

### DB2 out-of-row multi-insert + VECTOR source path

- [x] inspect all DMS 167 rows before decode and determine which rows declare external fields
- [x] attach a pending LOB/XML/VECTOR group only when exactly one multi-insert row can own it
- [x] reject multi-insert when two or more rows require external data because documented manager records provide no row ordinal
- [x] reject orphan pending external groups when no multi-insert row declares an external field
- [x] parse Db2 12.1.4+ DMS function 213 VECTOR serialized value records
- [x] require VECTOR records to be scoped by Start Out-of-Row, same table and byte order
- [x] reconstruct VECTOR INSERT/UPDATE values from serialized text; NULL VECTOR does not require function 213
- [x] source Full Read uses `VECTOR_SERIALIZE()` so Full and CDC share one serialized representation
- [x] DB2 target VECTOR auto-create/write fails closed pending native target metadata/encoding support
- [x] transaction byte/event safety bounds include VECTOR buffering and multi-insert expansion
- [x] synthetic tests cover unambiguous/ambiguous out-of-row multi-insert and VECTOR positive/negative cases
- [x] `go test ./...`, `go vet ./...`, plus `go test -race ./internal/cdc/db2log`
- [ ] retained DB2 11.5 / 12.1 qualification for out-of-row multi-insert and DB2 12.1.4+ VECTOR source traces
- [ ] multi-insert with external data required by more than one row remains fail-closed until a documented row-ownership signal exists
- [ ] DB2 VECTOR target dimension/coordinate metadata, schema creation and native parameter encoding
- [ ] retained decomposed-update rollback ordering beyond currently qualified row-level paths
- [ ] pureScale multi-log-stream ordering/failover qualification
- [ ] remove DB2 experimental gates only after retained exit criteria pass

### Remaining highest-priority connector gaps

- [ ] DB2 retained real-instance qualification + remaining advanced row/log cases
- [ ] Dameng Native Connector
- [ ] GaussDB Native Connector
- [ ] GBase Native Connector


## V0.15.0-rc10

### DB2 update-relocation + decomposed-update reconstruction

- [x] decode documented `lrIUDflags` and recognize decomposed-update bit `0x8000`
- [x] pair a flagged DELETE before-image with the following same-table flagged INSERT after-image and emit one logical CDC UPDATE
- [x] reject interrupted, cross-table, nested or commit-incomplete decomposed-update pairs
- [x] detect UPDATE second-row outer type `0x02` as an indirect after-image instead of positional row data
- [x] link indirect UPDATE only to the immediately preceding same-table outer-`0x04` INSERT candidate
- [x] transaction-wide stale-candidate invalidation across selected/unselected DMS row mutations, compensation, out-of-row starts and subtransaction merges
- [x] convert the buffered INSERT event into UPDATE while preserving after-image and replacing event identity/RID with the UPDATE LRI/old RID
- [x] ordinary `DMSUndoUpdate` compensation removes a linked indirect UPDATE through the existing rollback ledger
- [x] missing/stale/cross-table indirect candidates fail closed without advancing `DB2_LRI`
- [x] synthetic tests cover indirect linkage, stale candidate invalidation, rollback, decomposed pairing and interrupted pair safety
- [x] `go test ./...`, `go vet ./...`, plus `go test -race ./internal/cdc/db2log`
- [ ] retained DB2 11.5 / 12.1 qualification for relocation/decomposed updates and rollback ordering
- [ ] arbitrary decomposed-update compensation order reconstruction beyond qualified `DMSUndoUpdate` indirect-link rollback
- [ ] out-of-row multi-insert qualification/reconstruction
- [ ] Db2 12.1.4+ VECTOR type/log end-to-end support
- [ ] pureScale multi-log-stream ordering/failover qualification
- [ ] remove DB2 experimental gates only after retained exit criteria pass

### Remaining highest-priority connector gaps

- [ ] DB2 retained real-instance qualification + remaining advanced row/log cases
- [ ] Dameng Native Connector
- [ ] GaussDB Native Connector
- [ ] GBase Native Connector


## V0.15.0-rc9

### DB2 multi-insert + compensation/SAVEPOINT net effect

- [x] documented 40-byte normal / 56-byte compensation / 64-byte propagatable-compensation header offsets
- [x] DMS 167 multi-insert row-description decoding with count/length/boundary validation
- [x] ordered expansion of one multi-insert record into per-row CDC INSERT events with stable `LRI#index` IDs
- [x] per-transaction `(table, RID, operation)` identity ledger
- [x] DMS 110/111/112/131/166/168 row-level undo matching
- [x] SAVEPOINT-style partial rollback keeps only surviving source mutations
- [x] unmatched compensation RID and unsupported selected-table compensation fail closed
- [x] undo-start-of-out-of-row clears speculative LOB/XML grouping state
- [x] corrected DMS 164/165/166 empty-page row-image offsets to byte 26
- [x] outer row type `0x02` fails closed instead of being mis-decoded as a complete row
- [x] DB2 CDC synthetic tests cover multi-insert, undo multi-insert, insert/delete/update rollback and compensation header boundaries
- [x] `go test ./...`, `go vet ./...`, plus `go test -race ./internal/cdc/db2log`
- [ ] real DB2 11.5 / 12.1 retained qualification for multi-insert and SAVEPOINT rollback
- [ ] update-relocation `0x02` preceding-insert after-image linkage
- [ ] out-of-row multi-insert qualification/reconstruction
- [ ] Db2 12.1.4+ VECTOR type/log end-to-end support
- [ ] pureScale multi-log-stream ordering/failover qualification
- [ ] remove DB2 experimental gates only after retained exit criteria pass

### Remaining highest-priority connector gaps

- [ ] DB2 retained real-instance qualification + remaining advanced row/log cases
- [ ] Dameng Native Connector
- [ ] GaussDB Native Connector
- [ ] GBase Native Connector


## V0.15.0-rc8

### DB2 LUW Native DRDA/DDM + db2ReadLog CDC

- [x] QMigration-owned pure-Go DRDA/DDM transport for Metadata / Full / target apply; no JDBC/Python runtime
- [x] EXCSAT / ACCSEC / SECCHK / ACCRDB session establishment
- [x] SECMEC 9 encrypted credentials and fail-closed TLS rule for plaintext SECMEC 3
- [x] direct TLS / CA / ServerName / optional mTLS
- [x] dynamic PRPSQLSTT / OPNQRY / QRYDSC / QRYDTA / CNTQRY reader
- [x] generic multi-segment QRYDTA/EXTDTA reassembly with 256 MiB safety cap
- [x] integer / exact DECIMAL / float / date / time / timestamp / binary / boolean / LOB decode foundation
- [x] SYSCAT schemas/tables/columns/PK/index/FK metadata
- [x] identity START/INCREMENT and source ALWAYS/BY DEFAULT discovery
- [x] migration-stage BY DEFAULT identity propagation for source GENERATED ALWAYS
- [x] Full completion + committed target CDC `RESTART WITH` identity state synchronization
- [x] Full-only finish / Full+CDC cutover restoration of original GENERATED ALWAYS mode
- [x] numeric/composite bounded keyset and ordered NTILE boundary planning
- [x] heterogeneous DB2 target type compiler and schema/table/PK/index/FK create
- [x] Prepared target INSERT/MERGE through SQLDTA; BLOB/CLOB >32 KiB through EXTDTA
- [x] exact packed DECIMAL target encoding with no-rounding fail-closed checks
- [x] transactional target CDC Apply / delete / point lookup
- [x] schema-object View/Sequence/Trigger/Routine discovery foundation
- [x] `qmigration-db2-qualify` + `qualify-db2.sh`
- [x] DB2 source CDC architecture uses QMigration Log Agent + IBM-supported `db2ReadLog` rather than SQL polling
- [x] `qmigration-db2-log-agent` with HTTP/TLS and optional bearer token
- [x] QMigration-owned native provider source; IBM SDK/libdb2 required only on Agent host
- [x] durable `DB2_LRI` current-position/resume contract using `nextStartLRI`
- [x] `DATA CAPTURE CHANGES`, recoverability and primary-key prechecks
- [x] source-local Initialize Table descriptor bootstrap before captured migration LRI
- [x] ordinary non-value-compressed INSERT / UPDATE / DELETE log-row decoding
- [x] TID transaction aggregation, subtransaction merge, COMMIT / ABORT
- [x] target Apply -> durable checkpoint -> source ACK ordering
- [x] safe checkpoint-only progress when no selected transaction remains open
- [x] 100,000 events / 128 MiB per transaction and 10,000 open-transaction safety bounds
- [x] envelope/raw-header length/type/flags/TID consistency validation
- [x] decode documented VALUE COMPRESSION full-row images, including NULL and COMPRESS SYSTEM DEFAULT attributes
- [x] reconstruct logged out-of-row BLOB/CLOB/DBCLOB and varying values from chunked/consolidated LOB manager records
- [x] reconstruct Db2 11.5.8+ CSL serialized XML INSERT/UPDATE data when DB2_DCC_XML_SERIALIZE=YES
- [x] fail selected tables early when a LOB column is LOGGED=N; verify XML serialization registry state before CDC
- [x] fail closed on multi-insert, unsafe compensation/undo, unsupported LOB operation semantics and unknown selected-table DMS actions
- [ ] decode + qualify value-compressed DB2 row images
- [ ] retained real-DB2 qualification for VALUE COMPRESSION + logged out-of-row LOB + serialized XML across 11.5/12.1
- [ ] lossless compensation/savepoint/undo net-effect reconstruction
- [ ] pureScale multi-log-stream ordering/failover qualification
- [ ] retained DB2 11.5 / 12.1 real-instance Full + CDC qualification reports
- [ ] remove `QMIGRATION_EXPERIMENTAL_DB2_NATIVE` / `QMIGRATION_EXPERIMENTAL_DB2_LOG_CDC` only after exit criteria

### Remaining highest-priority connector gaps

- [ ] DB2 advanced log formats + retained real-instance qualification
- [ ] Dameng Native Connector
- [ ] GaussDB Native Connector
- [ ] GBase Native Connector


## V0.15.0-rc4

### OceanBase Binlog Service Native source CDC

- [x] explicit `cdc_url` separates OceanBase SQL/full-load endpoint from tenant ODP Binlog subscription endpoint
- [x] `obbinlog://` plaintext and `obbinlogs://` TLS endpoint parser; credentials in URL rejected
- [x] primary + seven fallback ODP endpoints with bounded reconnect rotation
- [x] current GTID/BINLOG capture through ODP `SHOW MASTER STATUS` / `SHOW BINARY LOG STATUS`
- [x] `SHOW BINARY LOGS` readiness/precheck and common 2983 management-port warning
- [x] Unified Engine `native-oceanbase-binlog` adapter
- [x] QMigration native MySQL Binlog V4/GTID decoder reuse for OceanBase Binlog Service
- [x] target apply + durable checkpoint before local reader ACK/reconnect state advance
- [x] Worker failover always resumes from last acknowledged GTID/file-position
- [x] `qmigration-oceanbase-qualify` + `qualify-oceanbase.sh` structured qualification workflow
- [x] Web datasource editor enables OceanBase Binlog/ODP `cdc_url`
- [ ] real OceanBase/Binlog Service/ODP version + GTID retention + failover qualification matrix
- [ ] remove EXPERIMENTAL maturity only after retained qualification reports

### Remaining highest-priority connector gaps

- [x] DB2 Native Connector (implemented in rc5; see top section)
- [ ] Dameng Native Connector
- [ ] GaussDB Native Connector
- [ ] GBase Native Connector

## V0.15.0-rc3

### TiDB / TiCDC Native source CDC

- [x] explicit datasource `cdc_url` separates TiDB SQL and TiCDC/Kafka endpoints
- [x] TiCDC OpenAPI v2 health + deterministic changefeed create/reuse/resume/delete
- [x] Kafka Canal-JSON sink with TiDB extension and one-partition ordering requirement
- [x] QMigration-owned pure-Go Kafka metadata/fetch consumer
- [x] Canal-JSON INSERT/UPDATE/DELETE/DDL/WATERMARK decoding
- [x] UPDATE `old` expansion to complete before-image
- [x] binary MySQL families restored from Canal Latin-1 representation to byte-preserving CDC fields
- [x] current TiDB TSO capture before Full Load
- [x] changefeed creation before Full Load readiness gate
- [x] durable `TIDB_TSO` position with `tso=<TSO>;kafka=<nextOffset>`
- [x] consecutive same-`commitTs` rows assembled as one QMigration target transaction
- [x] 100,000 event / 128 MiB transaction fail-closed bounds, including first record
- [x] durable duplicate suppression after restart
- [x] fail closed when non-zero durable Kafka offset exists but deterministic changefeed is missing
- [x] QMigration target apply + durable checkpoint before reader ACK
- [x] `qmigration-tidb-cdc` Worker/Planner integration
- [x] `qmigration-tidb-qualify` + `qualify-tidb.sh` qualification workflow
- [x] MySQL Native Binlog CDC datasource TLS/CA/ServerName/mTLS propagation carried forward
- [ ] real TiDB/TiCDC/Kafka version + failure/restart/GC qualification matrix
- [ ] Kafka TLS/SASL transport
- [ ] multi-partition global transaction merge (not claimed by RC3)

### Remaining highest-priority connector gaps

- [x] OceanBase Binlog Service Native source CDC adapter (implemented in rc4; see top section)
- [ ] DB2 Native Connector
- [ ] Dameng Native Connector
- [ ] GaussDB Native Connector
- [ ] GBase Native Connector

## V0.15.0-rc2

### Product support contract

- [x] Connector Descriptor exposes machine-readable `maturity` and `qualification_required`
- [x] `NATIVE` / `NATIVE_FULL_ONLY` / `EXPERIMENTAL` / `PROBE_ONLY` maturity levels
- [x] TiDB source CDC no longer falsely advertised as MySQL Binlog CDC
- [x] OceanBase MySQL source CDC no longer falsely advertised on the SQL endpoint
- [x] PolarDB-X native source CDC retained on its MySQL-compatible global Binlog path
- [x] PostgreSQL-wire openGauss / Kingbase remain Full Load capable without pgoutput overclaim
- [x] DB2 / DM / GaussDB / GBase remain explicit probe-only and fail-safe for migration planning

### SQL Server Native software completion

- [x] source-to-SQL-Server target type compiler including unsigned, JSON/text, binary, UUID and ROWVERSION handling
- [x] strict numeric-literal validation for Full/CDC/keyset/point/delete paths
- [x] View / Sequence / Trigger / Function / Procedure catalog discovery
- [x] schema-object expression dependency discovery
- [x] IDENTITY seed/increment discovery and target `IDENTITY(seed,increment)` restoration
- [x] Full Writer / CDC Apply `IDENTITY_INSERT` lifecycle with fail-safe session discard on cleanup failure
- [x] same-family SQL Server schema-object/DDL policy path
- [x] `qmigration-sqlserver-qualify` structured PASS / FAIL / SKIP real-instance qualification binary
- [x] qualification covers TDS login, metadata, Full Read, partitions, runtime load, schema objects, optional CDC LSN/retention and optional target transaction/identity/large-value/index test
- [x] `deployments/scripts/qualify-sqlserver.sh` one-command wrapper
- [ ] retain real SQL Server release/encryption/Always On/CDC/LOB soak qualification reports before removing experimental gates

### Cross-connector safety

- [x] common arbitrary-precision numeric SQL literal validator
- [x] MySQL/PostgreSQL/SQL Server remaining SQL-batch writers fail closed on syntax-shaped numeric payloads
- [x] Native MySQL Binlog CDC inherits datasource type + TLS/CA/ServerName/mTLS settings
- [x] numeric keyset bounds and point/delete keys receive the same validation

### Next implementation gaps

- [x] TiDB dedicated TiCDC source adapter (implemented in rc3; see top section)
- [ ] OceanBase dedicated Binlog Service endpoint model + native reader binding
- [ ] DB2 Native Connector
- [ ] Dameng Native Connector
- [ ] GaussDB Native Connector
- [ ] GBase Native Connector

See `docs/SUPPORT_MATRIX_V0.15_RC2.md` and `docs/SQLSERVER_NATIVE_QUALIFICATION.md`.

## V0.15.0-rc1

### Release qualification and diagnostics

- [x] `qmigration-oracle-qualify` direct real-instance qualification binary
- [x] structured PASS / FAIL / SKIP JSON report without credential/private-key leakage
- [x] read-only connection/version/NLS/metadata/full-read/partition/runtime/schema-object qualification
- [x] optional LogMiner prerequisite + current-SCN qualification (`--cdc`)
- [x] explicit destructive target qualification (`--target-write`)
- [x] target qualification covers bind/array bind/prepared DML, exact NUMBER, large CLOB/BLOB, rollback/commit, delete and post-load index
- [x] one-command `deployments/scripts/qualify-oracle.sh` wrapper
- [x] qualifier included in `make backend-build`
- [x] archive manifest distinguishes clean-restore Go rerun, preverified source and not-run modes
- [ ] real Oracle 11g/12c/19c/21c/23ai + NLS/TCPS/RAC qualification reports
- [ ] remove experimental Oracle gates only after qualification exit criteria are met

## V0.15.0-unified-dev15-complete

### Oracle Native complete software data plane

- [x] TTC bind metadata/value codec and exact Oracle NUMBER encoder
- [x] OALL8 parse/execute/bind DML and anonymous PL/SQL
- [x] bounded array bind for Full Writer batches
- [x] prepared cursor re-execute + one-shot ORA-01001 stale-cursor recovery
- [x] target row data removed from literal DML path; native Full Writer is bind-based
- [x] keyed idempotent MERGE + bind-safe delete
- [x] DATE/TIMESTAMP/TIMESTAMP TZ, numeric, boolean, RAW, string, BLOB/CLOB write families
- [x] keyed BLOB/CLOB EMPTY_LOB + DBMS_LOB.WRITEAPPEND streaming
- [x] keyless large BLOB/CLOB temporary-LOB PL/SQL insert path
- [x] UTF-8-safe CLOB chunk boundaries
- [x] TNS fragmentation for TTC requests larger than one DATA packet
- [x] TTC query/fetch/LOB reassembly when one TTC item spans multiple DATA packets
- [x] 256 MiB logical request/response safety limit
- [x] target table/composite-PK/index/FK creation and same-family DDL apply
- [x] explicit target CDC begin/commit/rollback and CDC apply capability
- [x] source Metadata / Full Reader / LOB / keyset / partition data plane from dev14 retained
- [x] LogMiner / SCN CDC reader and shared durable apply-before-ACK runtime retained
- [x] `QMIGRATION_EXPERIMENTAL_ORACLE_TARGET=1` target capability gate
- [x] no DataX/SeaTunnel/Flink CDC/Debezium/Canal/JDBC/OCI migration runtime dependency
- [x] full `go test ./...` and `go vet ./...`
- [ ] real Oracle 11g/12c/19c/21c/23ai + NLS/TCPS/RAC qualification matrix
- [ ] remove experimental Oracle gates only after real-instance qualification

### Capability gates

- `QMIGRATION_EXPERIMENTAL_ORACLE_NATIVE=1`: source Metadata/Full Reader/keyset/partition/runtime/schema-object/point-lookup/precheck.
- `QMIGRATION_EXPERIMENTAL_ORACLE_TARGET=1`: additionally Full Writer/schema/post-load DDL/transactional CDC apply; requires native gate.
- `QMIGRATION_EXPERIMENTAL_ORACLE_LOGMINER_CDC=1`: additionally SCN position + LogMiner CDC read; requires native gate.

### Deliberate policy boundaries

- Oracle schema/user creation and privilege grants are not performed implicitly.
- Oracle cached-sequence runtime state is not auto-rewritten because obtaining/resetting an exact state can alter source/target sequence semantics; schema-object planner keeps unsafe sequence/trigger/routine cases manual.
- Cross-family procedural DDL conversion remains manual/policy-driven.
- See `docs/ORACLE_NATIVE_QUALIFICATION.md` for the real-instance release gate.

## V0.15.0-unified-dev14

### Oracle Native source data plane

- [x] reusable authenticated TNS/TTC connector session
- [x] Data Dictionary schema/table/column/PK/index/FK discovery
- [x] partition and schema-object discovery
- [x] native Full Reader integration with numeric range / composite keyset / HASH / PARTITION / CUSTOM paths
- [x] NTILE keyset boundary planning
- [x] Oracle 11g-compatible ordered outer-ROWNUM batching
- [x] metadata discovery avoids unnecessary `V$PARAMETER` privilege dependency
- [x] numeric qualification literals fail closed on malformed/injection-shaped input
- [x] source-side capability gate (`QMIGRATION_EXPERIMENTAL_ORACLE_NATIVE=1`)
- [x] Oracle target write/schema/DDL capabilities remain hidden pending bind + large-LOB DML qualification
- [ ] real Oracle 11g/12c/19c/21c/23ai E2E qualification
- [ ] un-gated production Full Reader
- [ ] native bind execution / production Full Writer

### Oracle LogMiner / SCN CDC

- [x] built-in `qmigration-oracle-cdc` binary and Worker/Unified Engine wiring
- [x] current SCN capture and bounded SCN polling windows
- [x] selected-table LogMiner predicate
- [x] committed transaction grouping by XID + commit SCN
- [x] Flashback Query full-row reconstruction for experimental before/after images
- [x] checkpoint-only transaction when a window contains no selected changes
- [x] shared Native CDC Runtime apply-before-source-ACK ordering
- [x] `RS_ID` / `SSN` / `CSF` continuation coalescing for long SQL_REDO/SQL_UNDO
- [x] internal DDL filtering and `STATUS=0` user-DDL fail-safe
- [x] Reader regression tests for bounded SCN windows and ACK ordering
- [x] second gate `QMIGRATION_EXPERIMENTAL_ORACLE_LOGMINER_CDC=1`
- [ ] real redo/archive-log retention gap qualification
- [ ] Flashback/UNDO retention and row-movement soak qualification
- [ ] production CDC gate removal

### Experimental controls

- `QMIGRATION_EXPERIMENTAL_ORACLE_NATIVE=1`: enables source Metadata/Full Reader capabilities only.
- `QMIGRATION_EXPERIMENTAL_ORACLE_LOGMINER_CDC=1`: additionally exposes Oracle SCN position + LogMiner CDC reader; requires the native gate.
- Oracle target `full-write` / schema create / DDL apply remain deliberately unadvertised in dev14.

## V0.15.0-unified-dev13

### Oracle Native TTC dataset / cursor continuation

- [x] coalesced TTC message decoding inside a single TNS DATA packet
- [x] dataset row-header decoding including Oracle column-presence bit vectors
- [x] complete SELECT/fetch OER Summary parser for cursor id, current row, return code, flags, error position and ORA message
- [x] native TTC fetch continuation request (`3/5`) with cursor state tracking and bounded row/packet limits
- [x] explicit TTC cursor-close request primitive and exhausted/closed state guards
- [x] physical ROWID wire decoder to canonical 18-character Oracle ROWID
- [x] BLOB/CLOB row decoding preserves inline bytes and opaque server locator separately
- [x] bounded native LOB `0x60` request, RPA, data-chunk and end-of-call response codecs
- [x] Fake-TTC coverage for packet coalescing, multi-batch fetch, ORA errors, ROWID and LOB chunk streams
- [x] `go test ./...` and `go vet ./...`
- [x] production Connector capability remains `protocol-probe` only
- [ ] qualification against real Oracle 11g/12c/19c/21c/23ai instances
- [ ] real Data Dictionary execution through the native session
- [ ] real BLOB/CLOB locator reads across supported Oracle versions/charset modes
- [ ] Native Full Reader / Writer
- [ ] Redo / LogMiner Native CDC Reader

### Experimental controls

- `QMIGRATION_EXPERIMENTAL_ORACLE_TTC_NEGOTIATION=1`: protocol + datatype deep probe.
- `QMIGRATION_EXPERIMENTAL_ORACLE_TTC_AUTH=1`: negotiation + password authentication.
- `QMIGRATION_EXPERIMENTAL_ORACLE_TTC_QUERY=1`: negotiation + authentication + bounded SELECT probe using the dev13 coalesced/fetch-capable dataset runtime.

## V0.15.0-unified-dev12

### Oracle Native TTC experimental query wire path

- [x] bind-free OALL8 SELECT parse+execute request codec on the authenticated TNS/TTC session
- [x] strict describe metadata decoder for datatype / precision / scale / length / charset / nullability / name
- [x] row decoder with precision-safe Oracle NUMBER canonical decimal conversion
- [x] Oracle DATE decoding without inventing source timezone semantics
- [x] RAW / LOB-locator / unsupported scalar values preserve bytes instead of lossy coercion
- [x] bounded query probe limits for SQL size, fetch rows, describe columns, message count and row count
- [x] `QMIGRATION_EXPERIMENTAL_ORACLE_TTC_QUERY=1` implies TTC authentication and validates `SELECT 1 AS QMIGRATION_PROBE FROM DUAL`
- [x] Fake-TTC transcript covers OALL8 request -> describe -> row header -> row -> status
- [x] production Connector capability remains `protocol-probe` only
- [ ] qualification against real Oracle 11g/12c/19c/21c/23ai instances
- [ ] coalesced TTC messages / fetch continuation / cursor close qualification
- [ ] complete TTC summary/warning/error variants and ROWID/LOB streaming semantics
- [ ] real Data Dictionary execution through the native session
- [ ] Native Full Reader / Writer
- [ ] Redo / LogMiner Native CDC Reader

### Experimental controls

- `QMIGRATION_EXPERIMENTAL_ORACLE_TTC_NEGOTIATION=1`: protocol + datatype deep probe.
- `QMIGRATION_EXPERIMENTAL_ORACLE_TTC_AUTH=1`: negotiation + password authentication.
- `QMIGRATION_EXPERIMENTAL_ORACLE_TTC_QUERY=1`: negotiation + authentication + bounded SELECT probe; this is still a qualification switch, not a production metadata/full-read capability switch.

## V0.15.0-unified-dev11

### Oracle Native TTC authentication wire path

- [x] strict TTC compact integer / fixed integer / CLR / key-value codec
- [x] TTC protocol negotiation request/response parsing on the live post-ACCEPT TNS session
- [x] TTC datatype negotiation with client/server capability intersection and migration-relevant scalar/LOB representations
- [x] password authentication challenge parsing (`AUTH_SESSKEY` / `AUTH_VFR_DATA`)
- [x] verifier-family proof generation for 0 / 2361 / 6949 without placing plaintext password in the auth response
- [x] authentication result dictionary + success/error summary parsing
- [x] complete Fake-TTC authenticated transcript covering TNS ACCEPT -> protocol -> datatype -> encrypted auth -> session properties
- [x] experimental deep-probe gates do not leak `metadata`, `full-read`, `full-write` or `cdc-read` into Connector capabilities
- [ ] qualification against a real Oracle instance
- [x] experimental Oracle SELECT request / row metadata codec foundation (dev12; real-Oracle SQL Execute qualification still pending)
- [ ] real Data Dictionary execution through the native session
- [ ] Native Full Reader / Writer
- [ ] Redo / LogMiner Native CDC Reader

### Experimental controls

- `QMIGRATION_EXPERIMENTAL_ORACLE_TTC_NEGOTIATION=1` enables protocol + datatype deep probing after TNS ACCEPT.
- `QMIGRATION_EXPERIMENTAL_ORACLE_TTC_AUTH=1` additionally performs the password-auth wire flow. It remains a qualification tool, not a production Full/CDC capability switch.

## V0.15.0-unified-dev10

### S3 spool hardening

- [x] automatic multipart upload for encrypted objects above configurable threshold
- [x] multipart part size safety floor (5 MiB)
- [x] failed multipart uploads are explicitly aborted and never committed to Metadata
- [x] integrity-tagged `spools3:v2` references verify SHA-256 before secure-repository decryption
- [x] backward-compatible hydration of `spools3:v1` references created by dev8
- [x] fixed environment wiring for private CA / TLS ServerName / S3 mTLS cert/key
- [x] `QMIGRATION_CDC_SPOOL_S3_MULTIPART_THRESHOLD_BYTES` default 8 MiB
- [x] `QMIGRATION_CDC_SPOOL_S3_MULTIPART_PART_BYTES` default 8 MiB

### Oracle Native transport continuation

- [x] Listener CONNECT/REDIRECT/TCPS can return a live post-ACCEPT TNS DATA session instead of probe-and-close only
- [x] same accepted socket can be handed to the future TTC authentication/session layer
- [ ] TTC authentication is still intentionally capability-gated until protocol exchange is implemented and qualified against a real Oracle database

### Archiving

- [x] every development execution must produce source ZIP, previous-version patch, V0.13 cumulative patch, SHA-256 and JSON manifest
- [x] both patches must clean-restore to a byte-equivalent source tree and pass `go test ./...` + `go vet ./...`

## V0.15.0-unified-dev8

### S3-compatible durable CDC spool

- [x] Native AWS Signature V4 using Go standard library only
- [x] S3-compatible PUT/GET/COPY/DELETE/HEAD/ListObjectsV2
- [x] MinIO/Ceph RGW/S3 path-style endpoints
- [x] session-token credentials
- [x] custom CA / TLS ServerName / optional mTLS
- [x] encrypted object payloads; Metadata stores opaque references only
- [x] applied-object retention GC
- [x] startup orphan reconciliation after object-write/Metadata-commit crashes
- [x] signed bucket readiness probe plus periodic write/delete permission check integrated with `/readyz`
- [x] logical pending-capacity WARN/CRITICAL observability for object storage
- [ ] object multipart segment compaction is not implemented; current S3 mode stores one encrypted object per source transaction

### Storage modes

- `QMIGRATION_CDC_SPOOL_STORAGE=file|shared-fs|s3|metadata`
- S3 configuration is documented in `docs/SPOOL_STORAGE.md`

## V0.15.0-unified-dev7

### Independent CDC spool storage / HA

- [x] default file-backed encrypted CDC spool; Metadata keeps transaction index + ciphertext reference
- [x] atomic file persistence (`0600`, fsync, rename) before source ACK
- [x] hashed segment/shard directories
- [x] applied payload leaves the pending filesystem pool; crash reconciliation preserves referenced pending files
- [x] disk WARN/CRITICAL watermarks with delayed capture / fail-closed no-ACK behavior
- [x] `/readyz`, API/UI and Prometheus spool filesystem observability
- [x] PostgreSQL distributed drain lease for multi-Server HA
- [x] Kubernetes RWX shared-spool PVC

### Validation Watermark Barrier

- [x] persistent barrier position/type/resource/capture timestamp
- [x] quiet window via `QMIGRATION_VALIDATION_STABLE_WINDOW_SECONDS` (default 2s)
- [x] validation requires empty spool + lag gate + stable durable CDC checkpoint
- [x] CDC checkpoint drift during validation discards that result generation and returns to catch-up
- [ ] vendor-specific historical snapshot reads exactly at GTID/LSN remain future work for nonstop-write validation

### New controls

- `QMIGRATION_CDC_SPOOL_STORAGE=file|metadata` (default `file`)
- `QMIGRATION_CDC_SPOOL_DIR` default `data/cdc-spool`
- `QMIGRATION_CDC_SPOOL_DISK_WARN_PCT` default 80
- `QMIGRATION_CDC_SPOOL_DISK_CRITICAL_PCT` default 90
- `QMIGRATION_CDC_SPOOL_WARN_BACKPRESSURE_MS` default 250
- `QMIGRATION_CDC_SPOOL_APPLIED_FILE_RETENTION_HOURS` default 24
- `QMIGRATION_CDC_SPOOL_DRAIN_LEASE_SECONDS` default 300
- `QMIGRATION_VALIDATION_STABLE_WINDOW_SECONDS` default 2

## V0.15.0-unified-dev6

### Durable CDC staging / spool

- [x] Native CDC capture starts during `FULL_MIGRATING` rather than waiting for Full completion
- [x] Source transaction ACK only after durable QMigration spool persistence
- [x] gzip compression + AES-256-GCM encrypted spool payloads
- [x] deterministic/idempotent transaction identity by task/direction/source position
- [x] Worker failover resumes from newest durable spool source position
- [x] ordered drain before live CDC can overtake historical backlog
- [x] target checkpoint still advances only after successful target transaction commit
- [x] per-transaction and total-pending spool capacity gates; capacity failure does not ACK source
- [x] pending transaction/event/byte observability through API, Vue and Prometheus
- [x] manual drain API and automatic drain on catch-up
- [x] cutover and rollback require zero pending spool backlog
- [x] applied spool record retention/cleanup

### Full + CDC lifecycle correctness

- [x] Full completion enters CDC catch-up before automatic validation
- [x] automatic validation requires empty durable spool
- [x] configurable `QMIGRATION_VALIDATION_MAX_CDC_LAG_MS` gate (default 5000ms)
- [x] SQL Server CDC retention message updated: with durable staging, retention protects capture outages/backpressure instead of the full snapshot duration
- [ ] transactionally consistent online validation at an exact vendor log watermark remains a future database-specific enhancement; current validation is gated by catch-up but source writes may continue

### Spool operational limits

- `QMIGRATION_CDC_SPOOL_MAX_TRANSACTION_BYTES` default 16 MiB
- `QMIGRATION_CDC_SPOOL_MAX_PENDING_BYTES` default 64 GiB
- `QMIGRATION_CDC_SPOOL_DRAIN_PER_REQUEST` default 1000
- `QMIGRATION_CDC_SPOOL_KEEP_APPLIED` default 1000

## V0.15.0-unified-dev5

### SQL Server Planner / Full Load hardening

- [x] Composite keyset `[lower, upper)` predicates use exact lexicographic semantics
- [x] Ordered `NTILE` source-boundary planner for string/composite/UNIQUE migration keys
- [x] Native SQL Server partition discovery and `$PARTITION` chunk predicates
- [x] SQL Server runtime-load sampling for unified Task backpressure/effective parallelism
- [ ] real SQL Server Full E2E/soak qualification before experimental gate removal

### SQL Server CDC retention / durability

- [x] Checkpoint-only CDC transactions durably advance unrelated-table LSN windows before source ACK
- [x] Cleanup retention precheck from `msdb.dbo.cdc_jobs`
- [x] Configurable `QMIGRATION_SQLSERVER_CDC_MIN_RETENTION_MINUTES` safety floor (default 4320)
- [x] Existing retained-min-LSN gap detection remains active while consuming
- [x] durable CDC staging/spool during long Full snapshots (implemented in dev6; independent file storage added in dev7)

### Oracle Native transport

- [x] Oracle Net/TNS CONNECT / ACCEPT / REFUSE / REDIRECT
- [x] Oracle TCPS with `DISABLE/PREFERRED/REQUIRED`
- [x] custom CA / TLS ServerName / optional mTLS client certificate
- [x] REQUIRED never silently downgrades across redirects
- [x] post-ACCEPT TNS DATA session framing foundation
- [ ] TTC authentication/session negotiation
- [ ] Oracle Data Dictionary / native Full Reader/Writer
- [ ] Redo/LogMiner Native CDC Reader

## V0.15.0-unified-dev4

### SQL Server Native TDS/TLS

- [x] TDS PRELOGIN / LOGIN7 / SQL Batch / token decoder
- [x] TDS 7.x TLS handshake transported in PRELOGIN packets
- [x] Full connection TLS for ENCRYPT_ON / ENCRYPT_REQ
- [x] Login-only TLS semantics for PREFERRED + server ENCRYPT_OFF
- [x] REQUIRED never downgrades
- [x] Custom CA / ServerName / optional mTLS client certificate
- [x] SQL Server datasource TLS defaults to PREFERRED

### SQL Server Native CDC/LSN (experimental)

- [x] `sys.fn_cdc_get_max_lsn()` durable snapshot start position
- [x] `cdc.change_tables` capture-instance discovery
- [x] selected table capture validation before snapshot/CDC start
- [x] bounded LSN windows via `cdc.lsn_time_mapping`
- [x] `cdc.fn_cdc_get_all_changes_<capture_instance>` native reads
- [x] INSERT / DELETE / UPDATE old+new normalization
- [x] transaction grouping by `__$start_lsn`
- [x] binary CDC field encoding
- [x] minimum retained LSN / retention-gap guard
- [x] shared `cdc/runtime.Reader` integration
- [x] target Apply + durable Checkpoint before source cursor ACK
- [x] built-in `qmigration-sqlserver-cdc` worker binary
- [ ] real SQL Server E2E qualification / long-running CDC soak

### Managed CDC / Scheduler

- [x] Worker always injects `QMIGRATION_TASK_ID` into managed CDC processes
- [x] CDC source validation is Capability-SPI driven
- [x] rollback CDC source selection is Capability-SPI driven

### Oracle Native Connector

- [x] Oracle Net/TNS CONNECT frame
- [x] ACCEPT / REFUSE / REDIRECT parser
- [x] bounded listener redirect following (RAC/SCAN style)
- [x] Native `protocol-probe` capability
- [ ] Oracle authentication/session
- [ ] Oracle metadata/full-read/full-write
- [ ] Oracle Redo/LogMiner CDC Reader

### Transform Policy DSL (from dev3)

- [x] TRIM / LOWER / UPPER / EMPTY_TO_NULL / NULL_TO_VALUE / REPLACE_LITERAL
- [x] ZERO_DATE_TO_NULL / ZERO_DATE_TO_VALUE / JSON_COMPACT
- [x] persisted task rules and Worker Claim propagation
- [x] fail-safe zero-date behavior when no rule is declared

## V0.15.0-unified-dev2

### Unified Connector Capability SPI

- [x] Connector Factory 显式声明 `metadata/full-read/full-write/cdc-read/cdc-apply/...` 能力
- [x] Migration Planner 通过 Capability SPI 判断 Full/CDC 是否可执行，不再仅按数据库名称分支
- [x] 新增 `GET /api/v1/connectors` 返回 QMigration Native Connector 能力矩阵
- [x] 未实现数据库只保留连接探测，不再被误判成可迁移 Connector
- [x] openGauss / Kingbase 通过 QMigration PostgreSQL Wire Protocol 接入 Native Full Load
- [x] openGauss / Kingbase 不虚标 pgoutput CDC 能力

### Unified Transform Runtime

- [x] Full Load 使用 Source Column -> Universal Type -> Target Column 编译值转换计划
- [x] Boolean 跨协议规范化
- [x] JSON/JSONB 写入前验证
- [x] Binary/Decimal/UUID 保持无损 Connector-neutral Row Image
- [x] MySQL zero-date -> PostgreSQL 等无法无损表达的值 fail-safe，等待显式 Transform Policy
- [x] Pipeline 继续严格执行 Writer 成功后才提交 Durable Checkpoint

### Unified Native CDC Runtime

- [x] 新增协议无关 `cdc/runtime.Reader` / `Transaction` / `Runner`
- [x] 统一顺序：Decode -> QMigration Apply + Durable Checkpoint -> Source ACK
- [x] PostgreSQL pgoutput 已迁入统一 CDC Runtime
- [x] MySQL Binlog/GTID 已迁入统一 CDC Runtime
- [x] MySQL Transaction Payload / DDL / GTID / metadata refresh 仍由 Native Reader 保留
- [x] Source ACK 失败或 Apply 失败不会推进已确认重连位点

### Native 数据通道

- [x] MySQL-family Full Load
- [x] PostgreSQL / PolarDB PostgreSQL Full Load
- [x] openGauss / Kingbase PostgreSQL-Wire Native Full Load
- [x] MySQL Binlog / GTID CDC
- [x] PostgreSQL pgoutput / LSN CDC
- [ ] openGauss / Kingbase Native CDC Reader（需按产品日志语义单独实现，不复用/冒充 pgoutput）
- [ ] Oracle Native Connector / Redo Reader
- [ ] SQL Server Native Connector / CDC Reader
- [ ] DB2 / DM / GaussDB / GBase Native Connector

## V0.15.0-unified-dev1

- [x] Server 只注册 `qmigration` 单一执行内核
- [x] 删除 SeaTunnel/DataX/Flink CDC Adapter 运行代码
- [x] Full Load 改为 Reader -> bounded channel -> Transformer -> Writer -> Checkpoint
- [x] Worker 不再执行第三方迁移程序
- [x] MySQL/PostgreSQL 原生 Full/CDC 接入统一产品内核

其他 Validation/Cutover/Rollback/RBAC/Audit/Prometheus/Kubernetes 能力沿用此前自研实现，并继续接入统一内核。
