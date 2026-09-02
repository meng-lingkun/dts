# QMigration V0.15.0-rc23 Release Notes

RC23 hardens GBase 8s V8.8 source CDC against schema drift. The source CDC API emits a `CDC_REC_TABSCHEMA` record when capture starts, while schema changes are not emitted as ordinary CDC changes and an ALTER is rejected while capture is active. QMigration therefore treats the capture schema as a mandatory fence rather than silently continuing with a stale row layout.

## Schema-fence protocol

Each selected table now carries a deterministic SHA-256 fingerprint over:

- normalized owner/table name;
- selected column order and names;
- full catalog column type declaration;
- nullability;
- primary-key membership and primary-key order.

The datasource checkpoint uses current GBase 8s catalog metadata to create the planned fingerprint. The persisted migration table recreates the same fingerprint when a Worker starts or restarts.

The local CSDK provider must validate the source `CDC_REC_TABSCHEMA` plus current catalog state and return a `schema_fences` entry for every selected table on checkpoint and on every read response. QMigration rejects missing, duplicate, malformed or mismatched fences before any row event is applied. A forwarded `TABLE_SCHEMA` record must also carry the matching fingerprint.

## Protocol compatibility

- Agent API is now `v2`; RC23 clients reject v1 agents.
- Native C provider ABI is now `v2`; RC21/RC22 ABI-v1 providers are rejected at load time and must be rebuilt against the RC23 header/JSON contract.
- Endpoint paths remain `/v1/...`; the version is negotiated in health and the provider ABI.
- Legacy Go providers must also implement the new schema-fence response fields.

## Correctness boundary

RC23 still does not advertise smart BLOB/CLOB source images. The documented fallback requires a later SELECT by primary/unique key and therefore does not by itself guarantee the historical LOB image under CDC lag and concurrent LOB-only updates. QMigration keeps this fail-closed until an exact-image/versioned retrieval contract is qualified.

Real GBase 8s/CSDK provider qualification remains required before production promotion.
