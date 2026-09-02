# V0.15.0-rc49

- Closed remaining software-level connector/runtime gaps: GBase 8a provider CDC + transactional target path + C ABI agent, DB2 pureScale vector merge, GaussDB multi-primary/hybrid DDL+DML, openGauss/Kingbase DDL proof sidecars, Kafka compression/SASL extensions, relocation proof and validation snapshot capability levels.
- Added safe schema-program translation provider, external signer HA/scheduled rotation, RFC3161 timestamping, Unicode PDF renderer interface, and release qualification manifest builder.
- Fixed Worker capability/runtime mismatches for DB2/Oracle and preserved fail-closed production qualification boundaries.
- Added durable control-operation leases and periodic startup recovery for interrupted Precheck/Validation; unsafe partial Prepare replay fails closed with remediation guidance.
- Hardened production startup, Compose and Kubernetes defaults; removed default secrets, enabled mandatory authentication, non-root containers and restrictive pod security contexts.
- Added root version consistency checks, Linux CI, npm lockfile/type checking, route-level frontend code splitting and synchronized image/UI versions.
- Added Unix/Windows File Spool capacity implementations and repaired platform-sensitive GBase 8s and SQL Server protocol tests.

# V0.15.0-rc48

- Added multi-key Ed25519 report Trust Store with ACTIVE/RETIRED/REVOKED lifecycle.
- Added old-key-signed transition certificates for controlled key rotation and new/current-key-signed revocation certificates.
- Added `qmigrationctl trust-init`, `trust-apply-transition`, `trust-apply-revocation`, `trust-show`, and `verify-report --trust-store`.
- Historical reports signed before retirement remain verifiable; reports signed by REVOKED keys fail closed.
- Transitioned keys are rejected for reports that predate their signed transition time.
- Added server APIs to issue key-transition and key-revocation certificates without exporting the current private key.
- Security boundary: compromise-safe historical timing still requires WORM/Object Lock or an external trusted timestamp.

# V0.15.0-rc47

- Added optional Ed25519 public signatures for acceptance artifacts and manifests, with downloadable public-key fingerprint metadata.
- Added offline `qmigrationctl verify-report` with external trusted-key pinning, artifact SHA/signature checks, evidence identity checks and READY commit verification.
- Added immutable `validation_report_archives` metadata registry for S3/WORM URI, Manifest SHA-256, signer identity and observed retention information.
- S3 delivery includes `ED25519SIGNATURES` when public signing is configured; conflicting archive registry rewrites fail closed.

# V0.15.0-rc46

- Added deterministic Validation Acceptance Report export in JSON, HTML and dependency-free PDF formats.
- Added per-artifact SHA-256 manifests plus optional HMAC-SHA256 signing and key identifiers.
- Added S3-compatible immutable report archival with content re-hash verification, idempotent evidence-digest paths, READY commit marker and optional Governance/Compliance Object Lock + legal hold.
- Object Lock/WORM archival fails closed when retention/hold headers cannot be verified after upload.
- Added report download/archive API endpoints, Web UI actions, audit event and Prometheus report counters.

# V0.15.0-rc45

- Added immutable terminal validation archives with task/table evidence summaries and deterministic SHA-256 digests.
- Terminal per-chunk validation detail can be pruned only after archive creation.
- Added validation archive API, wrapper capability passthrough, bounded archive janitor work, and archive-aware Prometheus mismatch metrics.
- Complex table-union archives retain exact rows/checksums once instead of multiplying duplicated chunk coverage.

# V0.15.0-rc44

- Stream complex HASH/partition/custom/keyset validation descriptors through an incremental unordered checksum accumulator instead of retaining a whole-table request slice.
- Compact repeated validation attempts per task+chunk while preserving the latest active result; add configurable terminal-task validation detail retention.
- Use bounded/index-backed PostgreSQL validation pruning and expose validation-result prune telemetry.
- Preserve MetadataMaintenance through Secure/file/S3 repository decorators so long-run retention actually runs in production compositions.

# V0.15.0-rc43

- Added per-table keyset-paged validation, repository-side coverage checks and repair candidate lookup.
- Fixed complex split validation coverage so one exact table scan durably covers every successful physical chunk.

# V0.15.0-rc42

- Fixed production Secure/file/S3 repository decorators to preserve RC41 repository-side Chunk aggregation instead of silently falling back to full task scans.
- Added bounded/index-backed Chunk hot-path providers for topology/fault-domain running convergence, runnable counts, pending-table refinement and max ChunkNo.
- Added PostgreSQL partial/expression hot-path indexes via migration `074_v015_rc42_chunk_hotpath_indexes.sql`.
- Added Prometheus metadata relation size/index/live/dead/dead-ratio telemetry for long-soak bloat and autovacuum qualification.

# V0.15.0-rc41

- Bounded metadata retention with CDC stream-head safety.
- Repository-side chunk aggregation for long-running control-plane stability.
- Prometheus metadata-maintenance telemetry.

# V0.15.0-rc40

- Converge already-running chunks in risky rack/zone/region domains to RC39 domain caps at durable cursor boundaries.
- Deterministic health-first survivor selection prevents unhealthy work from consuming the scarce domain survivor budget.
- Preserve `fault_domain_json` on yielded remainders together with source topology ownership.
- Added `adaptive_fault_domain_yields` API/WebSocket/Web/Prometheus telemetry.
- Added migration `072_v015_rc40_fault_domain_running_convergence.sql` and RC40 convergence/parser coverage.

# V0.15.0-rc39

- Added canonical region/zone/rack fault-domain metadata on Full-load chunks (`fault_domain_json`).
- Added hierarchical rack/zone peer-risk ranking, multi-zone region escalation, and correlated-domain concurrency caps to Memory and PostgreSQL ClaimChunk schedulers.
- Added in-place fault-domain throttle for already-running healthy chunks when a correlated peer degrades or opens its circuit.
- Added TiDB store-label region/zone/rack extraction and OceanBase `ob_zone` canonicalization.
- Added Prometheus fault-domain info/risk metrics and migration `071_v015_rc39_fault_domain_cascade.sql`.
- Added RC39 scheduler, parser, connector and running-throttle qualification tests.

# V0.15.0-rc38

- Added hysteresis-controlled DEGRADED topology recovery; one good sample can no longer flap a placement directly back to HEALTHY.
- Added staged recovery concurrency through persisted `good_streak` and `recovery_concurrency_cap` topology profile fields.
- PostgreSQL and in-memory schedulers use the same effective recovery cap; already-running DEGRADED shedding follows the same dynamic cap.
- DEGRADED batch/pause/byte-budget throttling relaxes progressively as the topology earns recovery concurrency.
- HALF_OPEN/DEGRADED recovery evaluates the current sample instead of stale rolling P99, while HEALTHY degradation still uses tail-risk history plus repeated evidence.
- Added Prometheus/Web topology recovery telemetry and metadata migration `070_v015_rc38_topology_recovery_hysteresis.sql`.
- Added RC38 hysteresis/ramp/reset/historical-tail regression coverage.

# V0.15.0-rc37

- Converge already-running DEGRADED topology chunks to the configured degraded concurrency cap at durable batch boundaries.
- Deterministic survivor selection prevents concurrent renewals from over-draining degraded topology work.
- Cooperative DEGRADED throttle reduces batch size, adds pause, and scales active byte budgets without asynchronous SQL cancellation.
- Added `adaptive_topology_degraded_yields` task/API/WebSocket/Web/Prometheus telemetry.
- Added metadata migration `069_v015_rc37_topology_degraded_convergence.sql`.
- Added RC37 scheduler/control tests plus PostgreSQL migration-column alignment coverage.

# V0.15.0-rc36

- Cooperative drain of already-running numeric/keyset Full-load chunks after their topology enters `CIRCUIT_OPEN`.
- Drain occurs only after committed batch + durable cursor and keeps the remainder bound to the same topology until HALF_OPEN recovery.
- Added `adaptive_topology_drains` persistence, WebSocket/Web UI and Prometheus telemetry.
- Added metadata migration `068_v015_rc36_topology_running_drain.sql`.

# V0.15.0-rc35

- Health-weighted topology scheduling, per-topology concurrency gates, HALF_OPEN recovery probes, and P95/P99 SLA tail-risk prediction.

## 0.15.0-rc29

- Added real child-process SIGKILL qualification for target-COMMIT-before-checkpoint and durable-spool-before-source-ACK crash windows.
- Added `@SIGKILL` failpoint action while preserving default deterministic error injection.
- Added predictive CDC-spool-aware Full Load throttling using backlog bytes, storage watermarks, growth rate and projected critical ETA.
- Added control-plane AIMD batch targets and persisted/API/Prometheus spool growth/ETA telemetry.
- Added file-spool I/O failpoints and orphan reconciliation qualification for the file-persist-before-metadata crash window.
- Added metadata migration `061_v015_rc29_predictive_flow_process_chaos.sql`.

## 0.15.0-rc28

- Added GBase 8s event-owned smart BLOB/CLOB CDC image contract with per-field length/SHA-256/acquisition proofs; current-row SELECT fallback remains forbidden.
- GBase 8s Agent API and native C provider ABI advance to v4; older experimental providers are rejected until rebuilt.
- Added durable `COMMIT_UNCERTAIN` CDC dead-letter protection for target COMMIT response loss; automatic replay is blocked until an operator resolves COMMITTED vs NOT_COMMITTED.
- Added API/UI/Prometheus visibility for uncertain commits and extended `qmigration-chaos-qualify` with a target-commit-unknown scenario.
- Added metadata migration `060_v015_rc28_gbase8s_lob_commit_unknown.sql`.

## 0.15.0-rc27

- Added qualification-gated GBase 8a target CDC apply using existing validated HASH staging+MERGE and keyed delete; source-transaction atomicity remains deliberately unadvertised.
- Added deterministic opt-in CDC failpoints for spool/apply/checkpoint/source-ACK failure windows.
- Added `qmigration-chaos-qualify` and `qualify-chaos.sh` to prove durable retry behavior without external databases.
- Added target CDC atomicity precheck warnings when a target supports idempotent CDC apply but not transactional apply.

## 0.15.0-rc26

- Added qualification-gated openGauss `mppdb_decoding` Source CDC with durable `OPENGAUSS_LSN` and apply-before-slot-advance semantics.
- Added qualification-gated KingbaseES `kboutput` Source CDC with `KINGBASE_LSN`, `sys_*` slot/catalog integration and selected-table publications.
- Added strict Kingbase slot-plugin identity validation so a replaced/non-`kboutput` slot fails before stream startup.
- Added `qmigration-opengauss-cdc`, `qmigration-opengauss-qualify` and `qmigration-kingbase-qualify`.
- Preserved fail-closed boundaries for openGauss binary/DDL and unqualified Kingbase `kboutput` wire variants.

## 0.15.0-rc25

- TiCDC Kafka source CDC now supports multi-partition topics with per-partition durable offsets and Resolved-TS global ordering fences.
- Added Kafka TLS/custom-CA/ServerName/mTLS plus SASL PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512 to the QMigration native consumer.
- Existing TiCDC changefeeds are checked for partition/security compatibility; qualification output redacts SASL passwords.
- Fixed `VALIDATING` CDC routing: target apply freezes atomically at the durable barrier while new source transactions continue into the Durable CDC Spool.
- Added `validation-snapshot` Connector SPI and TiDB exact TSO validation via `SESSION tidb_snapshot`.
- Updated Web TiDB/GBase8s guidance and added migration `056_v015_rc25_tidb_kafka_validation.sql`.
- Added Oracle exact-watermark validation snapshots through native `AS OF SCN` when LogMiner/`ORACLE_SCN` CDC is enabled.
- Added experimental DM8 source CDC through archived `DBMS_LOGMNR` + flashback row reconstruction with durable `DM_LSN` checkpoints and exact `AS OF SCN` validation.
- DM8 CDC now rewinds to committed `START_SCN` for transactions that began before the current checkpoint and aggregates same-`COMMIT_SCN` XIDs into one durable target transaction.
- Added `qmigration-dameng-cdc`, `--cdc` Dameng qualification, and migration `057_v015_rc25_oracle_dameng_exact_cdc.sql`.

## 0.15.0-rc24

- Added a mandatory 64-hex GBase 8s CDC capture-lineage fence and persisted it in `GBASE8S_CDC_SEQ`.
- Read requests now carry `expected_capture_lineage`; provider lineage changes fail before target apply or ACK.
- Agent API/native provider ABI advance to v3; lineage-less RC23/older checkpoints fail closed.
- Restored RC22 authenticated `/v1/status` and `/metrics` observability that had regressed from the RC23 source archive.
- Added metadata migration `055_v015_rc24_gbase8s_capture_lineage.sql`.

## 0.15.0-rc23

- Added mandatory GBase 8s CDC schema fences derived from selected columns/types/nullability/primary-key order.
- Agent API moves to v2 and rejects v1; native C provider ABI moves to v2 so old providers cannot silently run without schema validation.
- Checkpoint and every provider read must return validated schema fingerprints for every selected table; missing/malformed/mismatched fences fail before target apply.
- Forwarded TABLE_SCHEMA records are checked against the planned fingerprint.
- Smart BLOB/CLOB source values remain fail-closed because later key-based SELECT does not inherently preserve the historical LOB image under CDC lag.
- Added metadata migration `054_v015_rc23_gbase8s_schema_fence.sql`.

## 0.15.0-rc22

- GBase 8s source CDC now accepts documented CDC_REC_TRUNCATE records and preserves preceding DML + TRUNCATE in one source transaction.
- Added first-class CDC `TRUNCATE` operation and target `TruncateTableConnector` primitive.
- GBase 8s target TRUNCATE requires an active target CDC transaction; any source record after TRUNCATE other than COMMIT/ROLLBACK fails closed.
- Smart BLOB/CLOB/complex GBase 8s CDC remains intentionally unsupported.

## 0.15.0-rc21

- Added preferred Linux native C ABI v1 provider for GBase 8s syscdcv1/CSDK source CDC, loaded by the QMigration agent with `dlopen`; legacy Go plugin remains compatibility-only.
- Added provider ABI/version checks, optional exact SHA-256 pinning, local config permission/size checks, synthetic shared-library integration tests and a compileable C ABI example.
- Hardened GBase 8s CDC provider conformance: complete selected-column/order checks, NULL/base64 validation, response bounds and UPDATE_BEFORE memory accounting.
- Empty committed GBase 8s source transactions now emit CHECKPOINT events so `GBASE8S_CDC_SEQ` advances without inventing row DML.
- Hardened Agent transport: provider calls serialized; non-loopback listeners require TLS + Bearer token; remote plaintext and URL credentials are rejected.
- Added metadata migration `052_v015_rc21_gbase8s_cdc_native_provider.sql`; real vendor CSDK provider build and real-instance qualification remain release gates.

## 0.15.0-rc20

- Added qualification-gated GBase 8s syscdcv1/CSDK source CDC through a datasource-local smart-LOB provider agent.
- Added selection-aware pre-Full checkpoint capture and durable `GBASE8S_CDC_SEQ` restart/commit watermarks that preserve older open transactions across Worker restarts.
- Added BEGIN/COMMIT/ROLLBACK, DML before/after, DISCARD and duplicate-commit transaction semantics with shared apply-before-ACK runtime ordering.
- Added `qmigration-gbase8s-cdc`, `qmigration-gbase8s-cdc-agent`, provider plugin protocol/build helper and CDC qualification.
- TRUNCATE and smart BLOB/CLOB/complex source columns remain fail-closed pending retained CSDK qualification.

## 0.15.0-rc19

- Added a distinct qualification-gated GBase 8s V8.8 Connector over vendor Client-SDK ODBC; GBase 8a remains a separate MPP connector.
- Added systables/syscolumnsext/sysconstraints/sysindexes Metadata, stable numeric/composite keyset Full Read and ordered NTILE boundaries.
- Added GBase 8s target type conversion, owner/table/PK/index/FK schema, stable-key prepared UPDATE/existence/INSERT replay, exact BLOB binds and transactional target CDC apply.
- Added credential-safe ODBC DSN handling: persisted DSN properties may not contain UID/PWD; encrypted datasource credentials are injected only in memory.
- Added runtime provider-plugin build helper, `qmigration-gbase8s-qualify`, `qualify-gbase8s.sh` and metadata migration `050_v015_rc19_gbase8s_native_full.sql`.
- GBase 8s source CDC and QMigration-managed CSDK TLS remain fail-closed/unadvertised pending supported real-instance qualification.

## 0.15.0-rc18

- Corrected GBase 8a target layout so staging+MERGE is used only with a validated HASH-distributed target.
- Auto-created GBase targets select the first HASH-eligible stable migration-key column and emit `DISTRIBUTED BY(...)`; no random target is silently paired with MERGE.
- Added `SHOW CREATE TABLE` target-layout validation; random/REPLICATED targets and HASH columns outside the migration key fail before staging side effects.
- Added composite-key HASH selection and fail-closed behavior when no migration-key member is eligible for GBase HASH distribution.
- Updated GBase qualification, support matrix and metadata migration `049_v015_rc18_gbase8a_hash_merge.sql`.

## 0.15.0-rc17

- Added qualification-gated GBase 8a MPP native Metadata/Full Read/Full Write; GBase 8s/8c remain out of scope.
- Removed GBase from the external/generic migration placeholder and added a distinct `gbase8a` Connector capability surface over QMigration's native packet transport.
- Added GBase-specific `ENGINE=EXPRESS` target DDL and conservative random-distribution default; workload-specific HASH/REPLICATED targets remain pre-created/qualification-driven.
- Added key-required per-batch staging + `MERGE` target replay instead of inheriting MySQL `ON DUPLICATE KEY UPDATE`; keyless GBase Full Write fails closed.
- GBase source CDC, transactional target CDC apply, FK replay and post-load schema remain unadvertised.
- Added `qmigration-gbase-qualify`, `qualify-gbase.sh`, `GBASE8A_NATIVE_QUALIFICATION.md` and metadata migration `048_v015_rc17_gbase8a_native.sql`.

## 0.15.0-rc15

## V0.15.0-rc16

- GaussDB: added optional DDL-only logical decoding classification while preserving RC15 binary DML values and apply-before-ACK.
- GaussDB: DDL replay requires `cdc_ddl_mode=SAME_FAMILY`, GaussDB -> GaussDB identity mappings and explicit `QMIGRATION_GAUSSDB_DDL_ONLY_TRANSACTIONS=1` source policy acknowledgement.
- GaussDB: safe DDL subset is selected-table ALTER TABLE, TRUNCATE and CREATE [UNIQUE] INDEX; mixed DDL/DML and unsupported DDL fail closed.
- GaussDB: DDL-only transactions ACK with text get_changes; DML remains on binary get_changes.


- Switched GaussDB source CDC from JSON values to the documented `pg_logical_slot_peek_binary_changes` / `get_binary_changes` path.
- Added strict big-endian B/C/I/U/D binary frame and length-delimited tuple decoding.
- Added byte-safe NUL/non-UTF8 transport, SQL NULL-versus-empty preservation and OID 17 `bytea` exact-byte restoration.
- Removed the RC14 table-level binary-family rejection while retaining primary-key and transaction safety checks.
- GaussDB qualification now creates a temporary LSN-based slot and executes the actual binary peek path.
- Source DDL remains explicitly disabled to preserve target transaction atomicity; multi-primary remains unadvertised.
- Added metadata migration `046_v015_rc15_gaussdb_binary_cdc.sql`.

## 0.15.0-rc14

- Added qualification-gated GaussDB PostgreSQL-wire Metadata/Full/target data plane; GaussDB no longer uses the generic external-JDBC placeholder.
- Added `GAUSSDB_LSN` capture and explicit LSN-ordered `mppdb_decoding` logical slots.
- Added transaction-safe SQL logical decoding through `pg_logical_slot_peek_changes`, documented JSON DML decoding and apply-before-`get_changes` source ACK.
- Added primary-key/binary safety checks, logical-replication GUC/permission prechecks and strict transaction bounds.
- Added `qmigration-gaussdb-cdc`, `qmigration-gaussdb-qualify`, `qualify-gaussdb.sh` and metadata migration `045_v015_rc14_gaussdb_native_cdc.sql`.
- GaussDB source DDL, binary/NUL-sensitive JSON CDC and multi-primary logical decoding remain unadvertised pending implementation/qualification.

## 0.15.0-rc13

- Added qualification-gated QMigration Dameng/DM8 metadata, Full Read, schema and target-apply Connector; Dameng no longer uses the generic external-JDBC placeholder.
- Added catalog discovery, numeric/composite keyset reads, NTILE boundary planning and table/PK/index/FK target schema support.
- Added prepared keyless INSERT and keyed MERGE, numeric fail-closed validation, BLOB/binary binding, point lookup/delete and transactional target CDC apply.
- Added Linux runtime loading for a vendor DM `database/sql` provider plugin without vendoring proprietary driver source.
- Dameng provider TLS modes remain fail-closed and Dameng source CDC remains unadvertised pending qualification.
- Added `qmigration-dameng-qualify`, provider build helper and metadata migration `044_v015_rc13_dameng_native.sql`.

## 0.15.0-rc12

- DB2 metadata: preserve VECTOR dimension and `COORDINATETYPE` while keeping Db2 11.5 catalog discovery independent of the newer field.
- DB2 target schema: restore native `VECTOR(dimension,FLOAT32|INT8)` columns.
- DB2 target writer: reconstruct serialized source values with prepared `VECTOR(CAST(? AS CLOB),dimension,coordinate-type)` and reuse EXTDTA for large vectors.
- DB2 safety: validate VECTOR shape, coordinate count, INT8 range and finite FLOAT32 values before target apply.
- DB2 qualifier: add optional destructive VECTOR target create/write/readback qualification.
- Added metadata migration `043_v015_rc12_db2_vector_target.sql`.

## 0.15.0-rc11

- DB2 CDC: reconstruct out-of-row data for DMS 167 multi-insert when exactly one row can own the pending external-value group; ambiguous ownership fails closed.
- DB2 CDC: parse Db2 12.1.4+ DMS 213 VECTOR serialized values and integrate them with Start Out-of-Row grouping.
- DB2 Full Read: use `VECTOR_SERIALIZE()` to keep source Full and CDC VECTOR values on one text contract.
- DB2 target: fail closed for VECTOR auto-create/prepared write until dimension/coordinate metadata and native target encoding are implemented.
- Added regression coverage for unambiguous/ambiguous out-of-row multi-insert, VECTOR INSERT/NULL/malformed/unscoped cases, plus target fail-closed behavior.
- Added metadata migration `042_v015_rc11_db2_multi_outofrow_vector_source.sql`.

## 0.15.0-rc10

- DB2 CDC: link documented indirect UPDATE outer-`0x02` after-images to the immediately preceding same-table outer-`0x04` INSERT and emit one logical UPDATE.
- DB2 CDC: invalidate relocation candidates across selected/unselected row mutations, compensation, out-of-row starts and subtransaction merges.
- DB2 CDC: decode `lrIUDflags=0x8000` decomposed updates and combine the DELETE+INSERT pair into one logical UPDATE.
- DB2 CDC: fail closed on missing/stale relocation candidates and incomplete/interrupted decomposed-update pairs.
- DB2 CDC: retain `DMSUndoUpdate` rollback support after indirect-update linkage.
- Added metadata migration `041_v015_rc10_db2_relocation_decomposed_update.sql`.

## 0.15.0-rc9

- DB2 CDC: decode documented DMS 167 multi-insert and expand each logical row with RID-preserving transaction identity.
- DB2 CDC: parse 56/64-byte compensation headers and reconstruct row-level insert/delete/update/multi-insert SAVEPOINT rollback net effects.
- DB2 CDC: fail closed on unmatched compensation, unsupported compensation DMS functions and incomplete outer `0x02` row images.
- DB2 CDC: correct empty-page row offsets and add multi-insert/compensation/race regression coverage.
- Added metadata migration `040_v015_rc9_db2_multiinsert_compensation.sql`.

## 0.15.0-rc8

- DB2 CDC: decode documented VALUE COMPRESSION full-row images with NULL/system-default attributes.
- DB2 CDC: reconstruct logged out-of-row LOB/varying chunks and serialized XML before DMS row apply.
- DB2 CDC: reject NOT LOGGED LOB columns at selection time and verify `DB2_DCC_XML_SERIALIZE` for selected XML tables.
- Preserve fail-closed boundaries for formats whose full transaction net effect is not yet losslessly qualified.

## 0.15.0-rc7

- Added experimental DB2 LUW source CDC through QMigration DB2 Log Agent + IBM `db2ReadLog`, using durable `DB2_LRI` checkpoints.
- Added source-local Initialize Table descriptor bootstrap, ordinary INSERT/UPDATE/DELETE row decode, transaction/subtransaction assembly and COMMIT/ABORT handling.
- Added strict Apply -> durable checkpoint -> ACK ordering plus transaction/open-transaction safety bounds.
- Added fail-closed detection for value compression, out-of-row LOB, multi-insert, compensation/undo and unknown selected-table log actions.
- Added envelope/raw-header corruption checks and removed the provider's hard dependency on the 11.5.8-only `finalLRI` field.
- Added `qmigration-db2-cdc`, `qmigration-db2-log-agent`, provider build script and CDC qualification workflow.
- Added metadata migration `038_v015_rc7_db2_readlog_cdc.sql`.

## 0.15.0-rc6

- Replaced DB2 row-literal target DML with native DRDA prepared parameter execution.
- Added SQLDTA FDODSC/FDODTA encoding for integer, exact DECIMAL, float, boolean, date/time/timestamp, character and binary families.
- Added out-of-line EXTDTA target streaming for BLOB/CLOB values above 32 KiB and generalized extended DSS continuation to arbitrary multi-segment DDM objects.
- Added 256 MiB per-object request/response safety bounds and exact DECIMAL no-rounding validation.
- Extended `qmigration-db2-qualify --target-write` with multi-megabyte BLOB/CLOB round-trip qualification.
- DB2 source CDC remains deliberately unadvertised.

## 0.15.0-rc5

- Added QMigration-owned DB2 LUW DRDA/DDM transport, encrypted authentication, TLS/mTLS and native Metadata/Full Read.
- Added DB2 composite keyset reads and ordered NTILE keyset boundary planning.
- Added experimental DB2 target schema/table/index/FK plus transactional MERGE/INSERT/delete/point-lookup apply.
- Added DB2 identity START/INCREMENT discovery and lifecycle-safe LUW propagation: source `GENERATED ALWAYS` identities are staged as `BY DEFAULT`, generator state is synchronized after Full Load / committed target CDC, and source `ALWAYS` semantics are restored in the full-only finish or cutover critical section.
- Added `qmigration-db2-qualify`; DB2 source CDC and very-large target LOB parameter streaming remain intentionally unclaimed.
- Corrected the stale backend `internal/version.Version` marker to the current RC5 release.
- Added metadata migration `036_v015_rc5_db2_native_drda.sql`.

## 0.15.0-rc4

- Added OceanBase tenant ODP/Binlog Service native CDC with GTID/file-position resume and multi-ODP failover.
- Added `qmigration-oceanbase-qualify` and metadata migration `035_v015_rc4_oceanbase_binlog.sql`.

## 0.15.0-rc3

- Added TiDB TiCDC OpenAPI + QMigration native Kafka Canal-JSON CDC with TSO/offset durability and transaction assembly.
- Added `qmigration-tidb-qualify` and metadata migration `034_v015_rc3_tidb_ticdc.sql`.

## 0.15.0-rc2

- Completed SQL Server native software qualification surface and support-maturity API.
- Corrected TiDB/OceanBase CDC overclaim and hardened numeric literals across SQL-batch writers.
- Added metadata migration `033_v015_rc2_support_matrix_sqlserver.sql`.

## 0.15.0-rc1

- Added `qmigration-oracle-qualify` and one-command Oracle qualification wrapper.
- Added read-only source, optional LogMiner and explicit destructive target qualification modes with structured JSON evidence.
- Added qualifier to backend release builds.
- Corrected archive manifest Go verification semantics and added `--preverified-go`.
- Added metadata migration `032_v015_rc1_qualification.sql`.

## 0.15.0-unified-dev15-complete

- Completed Oracle TTC input/array binds, exact NUMBER encoding and prepared cursor re-execution.
- Completed Oracle native Full Writer, keyed/keyless large BLOB/CLOB, schema/post-load DDL and transactional CDC apply.
- Added outbound TNS fragmentation and inbound TTC reassembly for large requests/responses.
- Kept Oracle source/target/LogMiner capabilities behind explicit experimental gates pending real-instance qualification.
- Added metadata migration `031_v015_oracle_native_complete.sql`.

## 0.15.0-unified-dev14

- Connected Oracle TTC authentication/query/cursor primitives to an explicitly gated native source Metadata + Full Reader data plane.
- Added Oracle Data Dictionary, partition, keyset-boundary, runtime-load, point-lookup and schema-object discovery paths.
- Added 11g-compatible ordered ROWNUM batching and strict numeric-literal validation.
- Added built-in Oracle DBMS_LOGMNR/SCN CDC reader on the shared apply-before-ACK runtime.
- Added bounded SCN polling, selected-table filtering, XID/commit-SCN grouping, Flashback row reconstruction and checkpoint-only empty windows.
- Added LogMiner RS_ID/SSN/CSF continuation coalescing and user-vs-internal DDL safety filtering.
- Oracle target write/schema/DDL capabilities remain hidden; source and CDC capabilities require explicit experimental gates.
- Added metadata migration `030_v015_oracle_native_source_logminer.sql`.

## 0.15.0-unified-dev13

- Added coalesced Oracle TTC query-message decoding across TNS DATA packets.
- Added full OER Summary/cursor parsing and native TTC fetch continuation.
- Added row-header column bit-vector handling and native physical ROWID decoding.
- Added bounded BLOB/CLOB locator and TTC LOB chunk/request primitives for qualification.
- Added metadata migration `029_v015_oracle_ttc_fetch_lob.sql`; Oracle production capabilities remain protocol-probe only.

## 0.15.0-unified-dev9

- Added S3 multipart upload with abort-on-failure for large encrypted CDC spool objects.
- Added integrity-tagged `spools3:v2` references and SHA-256 verification before hydration/decrypt/apply; dev8 `v1` references remain readable.
- Fixed S3 custom CA / TLS ServerName / mTLS environment wiring.
- Added live Oracle post-ACCEPT TNS session handoff as the safe foundation for TTC authentication.
- Added mandatory version archive script/policy with clean incremental and cumulative restore verification.
- Added metadata migration `025_v015_spool_multipart_integrity.sql`.

## 0.15.0-unified-dev8

- Added QMigration-native S3-compatible encrypted CDC spool using standard-library AWS SigV4.
- Added session-token/path-style/custom-CA/mTLS object-store support.
- Added S3 applied-object GC, startup orphan reconciliation, readiness checks and logical capacity observability.
- Added metadata migration 024 and spool storage operations documentation.

## 0.15.0-unified-dev7

- Added independent encrypted file-backed CDC spool with atomic writes and metadata references.
- Added spool filesystem WARN/CRITICAL watermarks, fail-closed source ACK backpressure, readiness checks and Prometheus/UI storage telemetry.
- Added PostgreSQL distributed spool-drain lease for multi-Server deployments and an RWX shared-spool Kubernetes PVC.
- Added persistent Validation Watermark Barrier + quiet-window gating; validation generations are discarded and retried after CDC checkpoint drift.
- Added metadata migration `023_v015_spool_file_barrier.sql`.

## 0.15.0-unified-dev6

- Added durable CDC staging/spool for native CDC capture during long Full Snapshots.
- Spool payloads are gzip-compressed then AES-256-GCM encrypted by the secure repository; plaintext row values are never persisted in spool ciphertext storage.
- Source ACK now follows durable spool persistence during snapshot phase; target apply checkpoint advances only after target commit during catch-up/drain.
- Managed CDC failover resumes from the newest pending spool source position to avoid re-decoding already staged ranges.
- Added deterministic/idempotent spool transaction identity, per-transaction size limits and total pending-byte backpressure.
- Added ordered spool drain, applied-record retention, manual drain API, Vue backlog visibility and Prometheus spool metrics.
- Added cutover/rollback gates that require zero pending spool transactions.
- Corrected Full+CDC lifecycle to catch up durable CDC backlog before automatic validation.
- Added `QMIGRATION_VALIDATION_MAX_CDC_LAG_MS` validation gate and updated SQL Server CDC retention semantics for durable staging.
- Added metadata migration `022_v015_durable_cdc_spool.sql`.

## 0.15.0-unified-dev5

- Fixed SQL Server composite lexicographic bounded-keyset predicates so `[lower, upper)` ranges are exact for multi-column migration keys.
- Added SQL Server ordered NTILE keyset-boundary planning for string/composite/UNIQUE migration keys.
- Added SQL Server native partition discovery/split predicates and runtime-load sampling for unified planner/backpressure control.
- Added durable checkpoint-only SQL Server LSN transactions for CDC windows with no selected-table changes; source ACK now follows persisted checkpoint.
- Added SQL Server CDC cleanup-retention precheck via `msdb.dbo.cdc_jobs` and configurable `QMIGRATION_SQLSERVER_CDC_MIN_RETENTION_MINUTES` (default QMigration safety floor: 4320 minutes).
- Added Oracle TCPS transport with CA/ServerName/mTLS support and strict TLS redirect/downgrade rules.
- Added post-ACCEPT Oracle TNS DATA session framing as a TTC-session transport foundation; Oracle authentication/SQL/Redo remain gated.
- Added metadata migration `021_v015_native_planner_tcps_hardening.sql`.

## 0.15.0-unified-dev4

- Added SQL Server TDS 7.x TLS transport with PRELOGIN-framed handshake, full encryption and login-only TLS semantics.
- Added SQL Server custom CA/server-name/mTLS support and datasource `PREFERRED` TLS default.
- Added experimental native SQL Server CDC/LSN reader over SQL Server CDC change tables.
- Added selected-table capture-instance validation and CDC retention-gap detection before/while consuming changes.
- Added `qmigration-sqlserver-cdc` built-in binary and unified CDC runtime integration.
- Fixed Worker managed CDC environment to always inject `QMIGRATION_TASK_ID`.
- Changed CDC source validation/rollback engine choice to Connector Capability SPI rather than vendor-name switches.
- Added Oracle TNS listener redirect following for RAC/SCAN-style endpoints.
- Added metadata migration `020_v015_sqlserver_native_cdc.sql`.

## 0.15.0-unified-dev3

- Added persisted Transform Policy DSL inside the QMigration Full Load pipeline.
- Added explicit zero-date conversion policy instead of silent data mutation.
- Added QMigration-native Oracle TNS CONNECT protocol probe.
- Added QMigration-native SQL Server TDS PRELOGIN, LOGIN7, SQL Batch and result-token foundation.
- Added an explicit experimental gate for SQL Server native Full data plane; default capability remains protocol-probe only.
- Added Oracle/SQL Server target type mappings to the Universal Schema Engine.
- Added metadata migration `019_v015_transform_policy.sql`.

## 0.15.0-unified-dev2

- Added Unified Connector Capability SPI and `/api/v1/connectors` diagnostics.
- Planner/precheck now gates execution by native connector capabilities rather than external-engine routing.
- Added compiled value Transform Runtime inside the bounded Full Load pipeline.
- Added shared Native CDC Reader runtime with apply-before-source-ack durability semantics.
- Migrated both PostgreSQL pgoutput and MySQL Binlog/GTID loops onto the shared CDC runtime.
- Added native PostgreSQL-wire Full Load support for openGauss and Kingbase without advertising unsupported pgoutput CDC.
- Added metadata migration `018_v015_unified_spi.sql`.

# Changelog

## V0.15.0-rc43

- Streamed validation chunk traversal per table and moved cutover coverage/repair discovery into indexed repository queries.
- Fixed complex-split validation coverage so one exact table scan proves every successful physical chunk.
- Added migration `075_v015_rc43_validation_streaming.sql`.

## V1.0.0-RC2

- Added built-in Debezium and Canal push CDC normalization/API ingress.
- Added retry-safe HTTP 425 gating before CDC apply is allowed.
- Fixed first-push CDC state gating to preserve strict apply-before-checkpoint semantics.
- Fixed PostgreSQL DataSource mTLS SELECT-column drift and added regression tests.
- Added `/readyz` metadata repository + PostgreSQL schema-version checks.
- Added durable `metadata_schema_state` and migration `016_v1_rc2_release_hardening.sql`.
- Added graceful SIGTERM handling for Server and Worker.
- Added Server/Worker termination grace and PDB hardening for Docker/Kubernetes.
- Added transactional `deployments/scripts/migrate-metadata.sh`.

## V0.14.0-dev4

- Added PolarDB-X Group, TiDB Store, and OceanBase Zone topology discovery.
- Added durable table topology inventory and per-Chunk advisory placement hints.
- Added placement-aware Claim soft ranking without correctness dependence on topology.
- Added MySQL/PostgreSQL database runtime pressure sampling.
- Added Task-level `effective_parallelism` adaptive concurrency and durable flow-control state.
- Added metadata migration `014_v14_topology_flow_control.sql`.


## V0.14 Native Datasource TLS - Unreleased

- Added datasource-level `DISABLE / PREFERRED / REQUIRED` TLS policy for Native MySQL and PostgreSQL families.
- Added MySQL `CLIENT_SSL` negotiation and TLS transport for SQL sessions and Binlog CDC sessions.
- Added PostgreSQL TLS policy handling for SQL and logical-replication sessions.
- Added custom CA PEM and TLS server-name verification.
- Persisted TLS settings in PostgreSQL metadata and propagated them through worker credentials.
- Added Vue datasource TLS configuration and visibility.
- Added metadata migration `010_v14_datasource_tls.sql`.
- Added ordered `NTILE` boundary planning for string/composite/UNIQUE stable migration keys.
- Added parallel `[lower, upper)` bounded keyset chunks with immutable start/end tuple bounds.
- Kept per-batch durable tuple cursor semantics inside every bounded chunk for lease recovery and retries.
- De-duplicated keyset validation to one stable full-table checksum per table, avoiding repeated scans and cross-database collation boundary false positives.
- Added PostgreSQL metadata fields and migration `011_v14_bounded_keyset.sql`.
- Updated Vue chunk detail to display immutable bounds plus the current durable cursor.
- Added adaptive re-splitting for slow pending integer ranges and bounded keyset chunks using actual source median keys.
- Fixed PostgreSQL Repository persistence of dynamically changed Chunk range/keyset boundaries.
- Added migration-path feedback Backpressure using source-read/target-write latency and batch telemetry.
- Added Server `NORMAL / WARN / CRITICAL` ChunkControl responses for batch caps and short pauses.
- Added metadata migration `012_v14_backpressure.sql`.
- Added generic Worker topology labels and task `worker_selector` with `PREFERRED / REQUIRED` affinity.
- Added affinity-aware Claim scheduling plus per-table running-chunk balancing.
- Added metadata migration `013_v14_worker_affinity.sql` and Vue affinity/label visibility.

## V0.13 UNIQUE NOT NULL Native Migration Key

- Added stable Migration Key selection with primary-key priority and UNIQUE NOT NULL fallback.
- Nullable and generated-column unique indexes are rejected as unsafe resume keys.
- Added Native generic keyset full load for single/composite UNIQUE migration keys.
- Added mapped target UNIQUE-key enforcement before native writes begin.
- Auto-created target tables receive the UNIQUE migration constraint before bulk load; existing targets must already provide an equivalent UNIQUE index.
- Updated AUTO routing, validation prerequisites and compatibility assessment for the new stable-key model.

## V0.12 Native MySQL Transaction Payload ZSTD CDC

- Added MySQL `TRANSACTION_PAYLOAD_EVENT` TLV parsing.
- Added ZSTD/NONE transaction-payload decompression with strict declared-size validation.
- Expanded decompressed payloads back into complete nested binlog events and reused the existing TableMap/Rows/XID transaction pipeline.
- Durable file-position recovery uses the outer transaction-payload LogPos; GTID mode continues to use the durable GTID set.
- Added Worker `native-mysql-cdc-zstd` capability and runtime zstd availability checks.
- Added zstd to the backend Docker runtime while keeping QMigration Go binaries CGO-free.

## V0.11 Native MySQL OPAQUE JSON CDC

- Added MySQL Binary JSON `JSONB_OPAQUE` decoding for NEWDECIMAL and packed DATE/TIME/DATETIME/TIMESTAMP values.
- Preserved DECIMAL as a JSON numeric token rather than a quoted string.
- Added negative TIME and microsecond-precision temporal coverage.
- Added OPAQUE values inside Partial JSON Diff Vector replay.
- Unknown OPAQUE subtypes remain fail-safe and do not advance the durable CDC checkpoint.
- Updated Native MySQL CDC compatibility assessment to expose the supported OPAQUE boundary.

## V0.10 Native MySQL Partial JSON CDC

- Added MySQL `PARTIAL_UPDATE_ROWS_EVENT` support.
- Implemented exact `Json_diff_vector` framing: 4-byte little-endian vector length plus ordered serialized diffs.
- Added multi-diff `REPLACE / INSERT / REMOVE` replay against the FULL before-image.
- Added transaction-level Partial JSON CDC tests with durable Binlog checkpoint verification.
- Added `binlog_row_value_options` precheck visibility.
- Added native safety guard for `binlog_transaction_compression=ON` / `TRANSACTION_PAYLOAD_EVENT`.
- Extended `qmigration-binlog-inspect` event naming for Partial Update and Transaction Payload events.


## V0.9 Schema Dependency & Sequence Semantics

- Added MySQL/PostgreSQL View dependency discovery and topological Safe Apply ordering.
- Dependency cycles and unavailable dependency catalogs now fail safe to `MANUAL`.
- Existing target Views are skipped only when discovered definitions are provably equivalent; drift/unknown definitions require manual review.
- Added PostgreSQL sequence ownership/identity discovery.
- Added SERIAL sequence binding restoration with mapped target table/column, `OWNED BY`, and `DEFAULT nextval(...)`.
- Added IDENTITY semantic protection: target must already expose matching identity-backed sequence semantics.
- Sequence ownership metadata discovery failures now disable automatic sequence apply.
- Updated compatibility assessment and Vue Schema Object view with dependency/binding details.

## V0.8 Schema Object Migration / Sequence Cutover Safety

- Added Schema Object Plan/Apply for views, sequences, triggers, functions and procedures.
- Added conservative action classification: `APPLY_SAFE`, `SYNC_SEQUENCE`, `SKIP_EXISTING`, `MANUAL`.
- Added same-family identity View execution; MySQL view rendering strips source DEFINER by rebuilding from `VIEW_DEFINITION`.
- Added PostgreSQL sequence DDL creation plus `last_value/is_called` synchronization.
- Added Admin/DBA-only schema-object DDL execution with explicit confirmation and audit.
- Added `sequence_synced_at` persistence and cutover freshness gate for PostgreSQL-family full+incremental migrations.
- Added metadata migration `009_v08_schema_objects.sql`.
- Added Vue Schema Object Plan/Apply tab.

## V0.7 CDC Recovery / DLQ / Conflict Control

- Added encrypted CDC Dead Letter Queue with Admin/DBA replay workflow.
- Added durable-position duplicate suppression for lost-response CDC retries.
- Added `SOURCE_WINS` and version-column `LAST_WRITE_WINS` conflict policies.
- Added target-row `SELECT ... FOR UPDATE` version comparison inside the active CDC transaction.
- Added conflict audit records using hashed primary-key fingerprints rather than business key plaintext.
- Added CDC conflict query API, Vue conflict view and Prometheus conflict metric.
- Fixed CDC UPDATE primary-key changes by deleting the old mapped key before upserting the after image in the same target transaction.
- Added metadata migrations `007_v07_cdc_dlq.sql` and `008_v07_conflict_policy.sql`.

## V0.6 GTID / DDL / Generic Keyset

- Added native MySQL `COM_BINLOG_DUMP_GTID` transport, GTID Set/SID Block encoding and durable GTID restart recovery.
- Added checkpoint-only CDC events so transactions outside selected tables can advance safely without target writes.
- Added safe CDC DDL policy: default `REJECT`; `SAME_FAMILY` only for identical schema/table/column mappings.
- Added standard MySQL binary JSON decoding while keeping OPAQUE/partial JSON fail-safe.
- Added native string/composite primary-key full load through lexicographic keyset pagination.
- Added durable per-batch JSON tuple cursor plus cumulative rows/bytes checkpointing.
- Added keyset checksum validation and composite-primary-key full-load planning.
- Updated AUTO routing: any stable primary-key table can use Native; no-primary-key tables remain external-engine candidates.
- Added Docker-backed integration suite for MySQL 8.4 GTID and PostgreSQL logical replication transports.
- Added metadata migration `006_v06_keyset_cursor.sql`.

## V0.4 Platform Core - development release

- Added native PostgreSQL/PolarDB PostgreSQL connector and cross-family full-load path.
- Added encrypted credential persistence and PostgreSQL metadata repository.
- Added DataX and SeaTunnel full-load execution adapters.
- Added managed Flink CDC and SeaTunnel CDC process lifecycle.
- Corrected FULL_AND_INCREMENTAL ordering so CDC capture starts before full load.
- Added CDC start position persistence, Lag/pending-event tracking and cutover safety gates.
- Added schema/table/column mappings and automatic target table creation.
- Added chunk checksum validation, cross-database canonicalization and mismatch repair.
- Added reverse CDC rollback orchestration and reverse mapping generation.
- Added external JDBC datasource model for Oracle, SQL Server, DB2 and major domestic databases.
- Added Admin/DBA/Operator/Viewer RBAC, login/user-management baseline, HTTPS/CORS, alerts and audit.
- Added task compatibility assessment API and Vue view.

## 0.15.0-unified-dev1

- Reframed QMigration as one self-developed Unified Engine instead of a platform orchestrating third-party migration runtimes.
- Removed active SeaTunnel, DataX and Flink CDC adapters and third-party Full Load process execution.
- Added built-in staged Full Load runtime: Reader -> bounded channel -> optional Transform -> Writer -> durable Checkpoint.
- Normalized all new task/table/runtime engine metadata to `qmigration`.
- Worker capability discovery now reports only QMigration-owned capabilities.
- Renamed Debezium/Canal handling as compatibility envelope normalization; they are no longer advertised as migration engines.
- Added metadata migration `017_v015_unified_engine.sql`.
