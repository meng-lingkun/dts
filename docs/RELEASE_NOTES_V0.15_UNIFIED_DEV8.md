# QMigration V0.15.0-unified-dev8 Release Notes

## Native S3-compatible encrypted CDC spool

- Added `QMIGRATION_CDC_SPOOL_STORAGE=s3` without AWS SDK or third-party migration runtimes.
- QMigration implements AWS Signature V4 for S3-compatible PUT/GET/COPY/DELETE/HEAD/ListObjectsV2 operations.
- Supports AWS-style session tokens, path-style endpoints, MinIO/Ceph RGW/S3-compatible services, custom CA, TLS ServerName and optional mTLS.
- CDC payload remains gzip + AES-256-GCM encrypted by QMigration before object upload; object storage never receives plaintext row images.
- Metadata keeps the durable transaction sequence/source position/status and an opaque object reference.
- Target commit + Metadata APPLIED remains the correctness boundary; pending objects are moved to date/hash-sharded applied storage only after that boundary.
- Added applied-object retention GC and crash orphan reconciliation.
- `/readyz` performs a signed bucket readiness probe plus periodic write/delete permission check for S3 mode.
- `file`, `shared-fs`, `s3`, and legacy `metadata` spool modes are supported.

## Metadata

`024_v015_spool_s3_object_store.sql` advances Metadata Schema to `0.15.0-unified-dev8`. No new spool columns are needed because the existing ciphertext/reference field is intentionally opaque.

## Oracle status

Oracle remains capability-gated at Native TNS/TCPS transport and post-ACCEPT DATA session framing. TTC authentication/Data Dictionary/Full/Redo CDC are not claimed in dev8.
