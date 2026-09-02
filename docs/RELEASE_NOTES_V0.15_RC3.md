# QMigration V0.15.0-rc3 Release Notes

## Release purpose

V0.15.0-rc3 closes the largest correctness gap left in the MySQL-compatible
family: TiDB source incremental capture is no longer inferred from MySQL
COM_BINLOG_DUMP. QMigration now has a dedicated TiCDC control/data adapter while
keeping the SQL endpoint and CDC endpoint explicitly separated.

RC3 also carries the MySQL Native Binlog TLS/mTLS propagation fix that was found
after the first RC2 archive was cut.

## TiDB Native TiCDC source CDC

- Added datasource `cdc_url` for non-secret CDC control/data endpoint metadata.
- Added TiCDC OpenAPI v2 health/changefeed lifecycle management.
- QMigration creates/reuses a deterministic changefeed per task/direction.
- Changefeeds use Kafka Canal-JSON with the TiDB extension enabled.
- RC3 requires exactly one Kafka partition so global message order is explicit.
- Added a QMigration-owned pure-Go Kafka metadata/fetch consumer. No kcat,
  Debezium, Flink CDC, Canal client or external migration runtime is executed.
- Added Canal-JSON INSERT / UPDATE / DELETE / DDL / WATERMARK decoding.
- UPDATE `old` images are expanded into complete before-images.
- MySQL binary families from Canal-JSON are converted back to byte-preserving
  QMigration CDC fields.
- Durable positions use `TIDB_TSO` and carry both TiDB TSO and Kafka next offset:
  `tso=<commit-ts>;kafka=<next-offset>`.
- Changefeed creation occurs before the Full Load readiness gate so incremental
  changes accumulate while snapshot chunks are copied.
- Consecutive DML messages with the same TiDB `commitTs` are assembled into one
  QMigration target transaction before apply.
- Transaction assembly is bounded at 100,000 events / 128 MiB and fails closed.
- Durable duplicate suppression prevents an already-applied TSO from being
  replayed as DML after restart; only the Kafka receive offset is advanced.
- If a durable non-zero Kafka offset exists but the deterministic changefeed was
  deleted, the reader refuses to recreate a fresh topic behind that checkpoint.
- Source ACK still occurs only after target apply + durable QMigration checkpoint.

## TiDB qualification workflow

Added:

- `qmigration-tidb-qualify`
- `deployments/scripts/qualify-tidb.sh`
- `docs/TIDB_TICDC_QUALIFICATION.md`

Default qualification is read-only. `--cdc` creates an ephemeral qualification
changefeed/topic, verifies `normal` state and a one-partition Kafka topic, then
deletes the TiCDC changefeed. QMigration does not delete the external Kafka topic.

## MySQL CDC TLS completion carried into RC3

The Native MySQL Binlog reader now inherits datasource type, TLS mode,
ServerName, CA and optional mTLS client certificate/key instead of reconnecting
as an unrelated plaintext generic MySQL session.

## Current limitations / gates

- TiDB TiCDC source CDC remains `EXPERIMENTAL` until real TiDB/TiCDC/Kafka version
  and failure/restart qualification reports are retained.
- RC3 Kafka transport is plaintext/no-SASL. TLS/SASL Kafka clusters remain a
  follow-up compatibility item and are not silently downgraded.
- RC3 intentionally uses one partition. Multi-partition transaction merge is not
  claimed.
- TiCDC itself is the TiDB-native upstream capture component; QMigration owns the
  changefeed lifecycle, Kafka protocol consumer, decoding, transaction assembly,
  checkpointing, target apply and restart semantics.
- OceanBase source CDC still requires a dedicated Binlog Service adapter.

## Metadata

- Version: `0.15.0-rc3`
- Migration: `034_v015_rc3_tidb_ticdc.sql`
