# TiDB / TiCDC Native Qualification

QMigration RC25 treats TiDB source CDC as an explicit TiCDC integration, not as
a MySQL Binlog compatibility shortcut. The software path now supports
multi-partition ordering, Kafka TLS/mTLS and SASL/PLAIN + SCRAM-SHA-256/512, but it remains
`EXPERIMENTAL` until retained real-instance qualification exists.

## Required topology

Record for every environment:

- TiDB release / TiCDC release and PD/TiKV topology;
- TiCDC node count and deployment mode;
- Kafka release, broker count, topic partition count and authentication/encryption mode;
- TiDB SQL TLS mode and certificate policy;
- database/table set and representative transaction/LOB sizes.

## RC25 Kafka endpoint

Example multi-partition plaintext:

```bash
export TIDB_TICDC_URL='ticdc://ticdc:8300?brokers=kafka1:9092,kafka2:9092&kafka_partitions=8'
```

TLS / mTLS uses non-secret filesystem paths in `cdc_url`:

```bash
export TIDB_TICDC_URL='ticdc://ticdc:8300?brokers=kafka1:9093,kafka2:9093&kafka_partitions=8&kafka_tls=true&kafka_ca=/etc/qmigration/kafka-ca.pem&kafka_cert=/etc/qmigration/kafka-client.pem&kafka_key=/etc/qmigration/kafka-client.key'
```

SASL/PLAIN + SCRAM-SHA-256/512 credentials are never accepted in `cdc_url`:

```bash
export QMIGRATION_TIDB_KAFKA_SASL_USERNAME='qmigration'
export QMIGRATION_TIDB_KAFKA_SASL_PASSWORD='***'
export TIDB_TICDC_URL='ticdc://ticdc:8300?brokers=kafka1:9093,kafka2:9093&kafka_partitions=8&kafka_tls=true&kafka_sasl_mechanism=plain'
```

RC25 intentionally rejects unsupported SASL mechanisms instead of silently
falling back to plaintext. TiCDC and the QMigration Worker must both be able to
read the configured CA/client certificate files when those paths are used in a
QMigration-created changefeed.

## Multi-partition correctness contract

TiCDC periodically publishes Resolved TS to every partition. QMigration uses
that signal as a global progress fence:

1. DML/DDL records are buffered independently by Kafka partition.
2. The next candidate commitTs is the minimum queued transaction TSO.
3. The candidate is not emitted until **every** partition has observed a
   Resolved TS strictly greater than that commitTs.
4. All records with that commitTs are assembled before target apply.
5. Durable `TIDB_TSO` positions persist a next offset for every partition, for
   example `tso=456;kafka=0:103,1:98,2:111`.
6. ACK requires monotonic TSO and monotonic per-partition offsets. A previously
   acknowledged partition may not disappear.
7. An old single-partition non-zero checkpoint cannot be resumed after the
   topic becomes multi-partition; QMigration requires a new capture rather than
   guessing offsets for newly discovered partitions.

## Exact TSO validation

During Full+CDC validation RC25 freezes target CDC apply at the durable barrier
while source capture continues into the encrypted Durable CDC Spool. For TiDB,
QMigration opens an independent SQL session and sets `SESSION tidb_snapshot` to
the barrier TSO; all validation SELECTs therefore read the exact historical
source snapshot corresponding to the frozen target apply position.

Set `QMIGRATION_VALIDATION_REQUIRE_EXACT_WATERMARK=1` when a workflow must fail
closed unless the source connector implements an exact historical validation
snapshot. Other databases retain the existing catch-up/stable-window barrier
until their vendor-specific snapshot implementation is added.

## Automated qualifier

Build:

```bash
make backend-build
```

Run read-only checks:

```bash
export TIDB_HOST=127.0.0.1
export TIDB_PORT=4000
export TIDB_DATABASE=test
export TIDB_USER=root
export TIDB_PASSWORD='...'
export TIDB_TICDC_URL='ticdc://127.0.0.1:8300?brokers=127.0.0.1:9092&kafka_partitions=1'
deployments/scripts/qualify-tidb.sh
```

Create/delete an ephemeral changefeed and validate the generated Kafka topic:

```bash
TIDB_QUALIFY_CDC=1 deployments/scripts/qualify-tidb.sh
```

Optional report:

```bash
TIDB_QUALIFY_OUTPUT=/tmp/tidb-ticdc-qualification.json \
  TIDB_QUALIFY_CDC=1 deployments/scripts/qualify-tidb.sh
```

The report contains PASS / FAIL / SKIP. TiDB passwords and Kafka SASL passwords
must not be emitted; sink URIs are redacted before they enter report details.
The lifecycle qualifier deletes its TiCDC changefeed after validation but does
not delete the Kafka topic.

## Required CDC matrix

For every production claim verify at least:

1. start TSO captured before Full Load;
2. deterministic changefeed creation/reuse and restart;
3. Canal-JSON TiDB extension (`commitTs` + WATERMARK/Resolved TS);
4. INSERT / UPDATE / DELETE / DDL decoding;
5. binary/UTF-8/null/decimal/date/time row families;
6. cross-table transactions whose records span multiple partitions;
7. repeated commitTs across partitions and sparse/idle partitions;
8. 100k-event and 128MiB fail-closed transaction bounds;
9. target apply failure does not advance source ACK;
10. process crash after durable target/spool persistence and before local ACK;
11. restart from per-partition durable offsets without gaps;
12. Kafka leader movement, broker restart and consumer reconnect;
13. TLS server validation, custom CA, optional mTLS and SASL/PLAIN + SCRAM-SHA-256/512 failure paths;
14. TiCDC pause/resume/restart and changefeed recreation rejection after durable progress;
15. Full + CDC exact-TSO validation, catch-up, cutover and reverse/rollback workflows;
16. GC/retention behavior for long Full Loads;
17. large-volume soak under QMigration backpressure/spool pressure.

## Exit criteria

Do not promote TiDB source CDC from `EXPERIMENTAL` until supported
TiDB/TiCDC/Kafka releases have retained qualification JSON + E2E logs, no target
transaction is partially acknowledged, multi-partition restart/failover is
proven gap-free, exact TSO validation passes under concurrent writes, and
long-running Full+CDC tests demonstrate TiCDC/GC retention safety.
