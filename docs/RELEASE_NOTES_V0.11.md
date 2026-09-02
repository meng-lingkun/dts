# QMigration V0.11.0 Release Notes

## Native MySQL OPAQUE JSON CDC

V0.11 extends the zero-dependency Native MySQL binlog decoder with MySQL Binary JSON OPAQUE scalar support.

### Added

- `JSONB_OPAQUE` framing and payload validation.
- MySQL `NEWDECIMAL` OPAQUE decoding to a JSON number.
- MySQL packed `DATE`, `TIME`, `DATETIME`, and `TIMESTAMP` OPAQUE decoding.
- Negative TIME and microsecond precision.
- OPAQUE values embedded in MySQL Partial JSON Diff Vector transactions.
- Compatibility assessment messaging for supported and unsupported OPAQUE subtypes.

### Safety

Unknown OPAQUE subtypes are rejected. The source GTID/binlog checkpoint is not advanced until the complete target transaction has committed and the checkpoint is durable.

`binlog_transaction_compression=ON` remains unsupported by the Native Reader in V0.11; use Flink CDC/SeaTunnel or disable transaction compression.
