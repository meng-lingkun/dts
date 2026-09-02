# QMigration V0.12.0 Release Notes

## Native MySQL Transaction Payload ZSTD CDC

V0.12 adds native support for MySQL transaction compression without introducing a Go/CGo ZSTD dependency.

### Added

- MySQL `TRANSACTION_PAYLOAD_EVENT` TLV parser.
- ZSTD (`compression_type=0`) and NONE (`255`) payload handling.
- Strict compressed/uncompressed payload size validation.
- Nested binlog-event splitting and replay through the existing native transaction assembler.
- Outer-event file-position checkpointing and existing durable GTID-set recovery.
- Worker `native-mysql-cdc-zstd` capability and `QMIGRATION_ZSTD_BIN`.
- zstd package in the backend Docker runtime.

### Safety

If zstd is unavailable, the native reader refuses compressed transactions rather than advancing the source checkpoint. Unknown future compression types are also rejected.
