# QMigration V0.15.0-rc42 Release Notes

RC42 continues the long-running 10-40TB control-plane hardening started in RC41.

## Production capability passthrough

RC41 introduced repository-side Chunk summaries, but production wraps the metadata repository with CDC-spool and encryption decorators. Optional repository capabilities were not forwarded by those decorators, so `SummarizeChunks()` could fall back to full `ListChunks()` despite PostgreSQL supporting O(tables) aggregation. RC42 explicitly delegates summary, chunk-hot-path and metadata-stat capabilities through Secure, file-spool and S3-spool stores, with a regression test covering the composed decorator chain.

## Bounded Chunk hot paths

Worker renew/control no longer scans the full task Chunk history for topology or fault-domain convergence. PostgreSQL now provides index-backed queries for relevant RUNNING topology/domain rows, table-local PENDING rows, table runnable counts, and task `MAX(chunk_no)`. Adaptive split therefore scales with one affected table instead of the whole task.

## PostgreSQL indexes

RC42 adds partial RUNNING indexes for topology and canonical rack/zone/region JSON expressions, a PENDING table index, and task ChunkNo index. RUNNING expression indexes remain small because completed history is excluded.

## Metadata bloat observability

Prometheus exposes total metadata bytes and per-relation total/table/index bytes, approximate live/dead rows and dead tuple ratio. These metrics are intended for multi-day soak qualification and autovacuum calibration; they do not automatically VACUUM or rewrite tables.

## Correctness boundary

Explicit Chunk listing and validation remain O(chunks) by design. Validation must prove every successful Chunk has a successful current result and is not converted into an approximate summary.
