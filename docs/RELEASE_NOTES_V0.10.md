# QMigration V0.10.0 Release Notes

## Theme

**Native MySQL Partial JSON CDC correctness.**

V0.10 implements the MySQL 8 `PARTIAL_UPDATE_ROWS_EVENT` data path instead of treating partial JSON as an unsupported binary value. The implementation follows the MySQL `Json_diff_vector` wire format and preserves the existing rule that a source position is acknowledged only after the reconstructed transaction commits on the target.

## Highlights

- Added `PARTIAL_UPDATE_ROWS_EVENT` parsing and transaction assembly.
- Added `binlog_row_value_options` and per-JSON-column partial bitmap decoding.
- Added exact Json Diff Vector framing: 4-byte little-endian vector length followed by multiple serialized diffs.
- Added ordered `REPLACE`, `INSERT`, and `REMOVE` replay against the FULL before-image.
- Added complex JSON path handling for object members, quoted members, and array indexes.
- Added multi-diff tests and a native MySQL CDC transaction-level test that verifies rebuilt JSON and durable Binlog checkpoint attachment.
- Native MySQL precheck now reports `binlog_row_value_options`.
- Added `binlog_transaction_compression` safety guard. Native CDC requires it to be OFF until `TRANSACTION_PAYLOAD_EVENT` ZSTD decoding is implemented.
- `qmigration-binlog-inspect` identifies `PARTIAL_UPDATE_ROWS` and `TRANSACTION_PAYLOAD`.

## Safety boundaries

- `binlog_row_image=FULL` remains mandatory for `native-mysql-cdc`.
- Standard MySQL binary JSON and standard Partial JSON Diff Vector are supported.
- MySQL-specific OPAQUE JSON scalar payloads remain fail-safe.
- Compressed transaction payloads are rejected before checkpoint advancement; use uncompressed native binlog or Flink CDC/SeaTunnel.
- DDL policy remains `REJECT` by default, with `SAME_FAMILY` available only under identity mappings.

## Validation

The release gate requires:

- `git diff --check`
- `go test ./...`
- `go vet ./...`
- all six Go binaries built
- integration-tag suite compile
- isolated Server/Worker smoke
