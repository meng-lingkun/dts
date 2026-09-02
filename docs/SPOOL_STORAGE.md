# QMigration CDC Spool Storage

QMigration acknowledges a source CDC position only after the transaction is either committed on the target or durably staged in the encrypted QMigration spool. The payload is gzip-compressed and AES-256-GCM encrypted before it reaches any external spool backend.

## Backends

### `file`

Default backend. Suitable for one Server or multiple Servers sharing the same RWX filesystem.

```bash
QMIGRATION_CDC_SPOOL_STORAGE=file
QMIGRATION_CDC_SPOOL_DIR=/var/lib/qmigration/cdc-spool
```

`shared-fs` is an alias for the same implementation and is useful when deployment manifests want to make the HA intent explicit.

### `s3`

Native QMigration S3-compatible backend. It uses AWS Signature V4 implemented in the QMigration Go runtime; no AWS SDK or external migration runtime is required.

```bash
QMIGRATION_CDC_SPOOL_STORAGE=s3
QMIGRATION_CDC_SPOOL_S3_ENDPOINT=https://minio.example.internal
QMIGRATION_CDC_SPOOL_S3_BUCKET=qmigration
QMIGRATION_CDC_SPOOL_S3_PREFIX=prod/cdc-spool
QMIGRATION_CDC_SPOOL_S3_REGION=us-east-1
QMIGRATION_CDC_SPOOL_S3_ACCESS_KEY='...'
QMIGRATION_CDC_SPOOL_S3_SECRET_KEY='...'
QMIGRATION_CDC_SPOOL_S3_PATH_STYLE=true
```

Temporary credentials are supported with:

```bash
QMIGRATION_CDC_SPOOL_S3_SESSION_TOKEN='...'
```

Private S3-compatible endpoints can use a private CA and optional mTLS:

```bash
QMIGRATION_CDC_SPOOL_S3_CA_CERT="$(cat ca.pem)"
QMIGRATION_CDC_SPOOL_S3_TLS_SERVER_NAME=minio.example.internal
QMIGRATION_CDC_SPOOL_S3_TLS_CLIENT_CERT="$(cat client.pem)"
QMIGRATION_CDC_SPOOL_S3_TLS_CLIENT_KEY="$(cat client-key.pem)"
```

Large encrypted transaction objects automatically use S3 Multipart Upload:

```bash
QMIGRATION_CDC_SPOOL_S3_MULTIPART_THRESHOLD_BYTES=8388608
QMIGRATION_CDC_SPOOL_S3_MULTIPART_PART_BYTES=8388608
```

The part size must be at least 5 MiB. If any part or the final completion request fails, QMigration aborts the multipart upload and does not commit the spool record to Metadata, so the source CDC position remains unacknowledged.

Starting in dev9, newly staged S3 references use `spools3:v2` and embed the SHA-256 of the encrypted payload. QMigration verifies this digest after GET and before the secure repository decrypts/applies the transaction. Existing dev8 `spools3:v1` references remain readable for rolling upgrades.

QMigration performs a signed bucket readiness probe plus periodic write/delete permission check before becoming ready. `/readyz` fails if the S3-compatible backend cannot be reached/authenticated.

Pending transactions are stored under a hash-sharded `pending/` prefix. After successful target apply + durable Metadata `APPLIED`, the encrypted object is copied to a date-sharded `applied/` prefix and removed from pending. Applied objects are retention-GCed. Startup reconciliation moves objects that were persisted before a crash but never received a Metadata commit to `applied/recovered-orphans/`.

### `metadata`

Compatibility backend only. Encrypted payloads remain in the Metadata repository. It is not recommended for large or long-running migrations.

## Capacity and backpressure

The existing limits apply to every backend:

```bash
QMIGRATION_CDC_SPOOL_MAX_TRANSACTION_BYTES=16777216
QMIGRATION_CDC_SPOOL_MAX_PENDING_BYTES=68719476736
```

For file/shared-fs, WARN/CRITICAL values measure filesystem utilization. For S3-compatible storage, QMigration maps the configured maximum pending bytes to the same logical utilization fields so API/UI/Prometheus backpressure semantics remain consistent.

## Stale multipart recovery (dev10)

S3 multipart uploads that are interrupted before `CompleteMultipartUpload` can remain allocated in object storage. QMigration now lists multipart uploads under its own `pending/` prefix during reconciliation and aborts only uploads older than:

```bash
QMIGRATION_CDC_SPOOL_S3_MULTIPART_ABORT_AFTER_HOURS=6
```

Fresh uploads are not touched. If an S3-compatible backend omits the multipart `Initiated` timestamp, QMigration fails safe and leaves that upload unchanged rather than guessing its age.
