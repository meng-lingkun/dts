# GBase 8s CDC Provider Protocol (RC28)

QMigration keeps GBase 8s `syscdcv1` / Client-SDK CDC calls on the source host. The bundled `qmigration-gbase8s-cdc-agent` owns HTTP/TLS/authentication, request bounds and QMigration transaction semantics; the datasource-local provider owns the vendor CSDK session, capture lineage, schema proof, and (when enabled) event-owned smart-LOB reads. Database credentials remain local to that provider.

## Provider choices

### Preferred: native C ABI v4

```bash
export QMIGRATION_GBASE8S_CDC_PROVIDER_LIBRARY=/opt/qmigration/lib/qm-gbase8s-cdc-provider.so
export QMIGRATION_GBASE8S_CDC_PROVIDER_SHA256=<exact-sha256>
export QMIGRATION_GBASE8S_CDC_PROVIDER_CONFIG_FILE=/etc/qmigration/gbase8s-cdc-provider.json
```

The ABI is declared in `deployments/gbase8s-cdc-provider/qmigration_gbase8s_cdc_provider.h`. Required symbols remain `qm_gbase8s_cdc_abi_version/open/health/checkpoint/read/free/close`, but RC28 requires ABI **4**. ABI v1/v2/v3 providers are intentionally rejected and must be rebuilt because the response contract now includes smart-LOB image attestation.

The `.so` path must be absolute, regular and non-world-writable; exact SHA-256 pinning is recommended. Provider result/error buffers remain provider-owned and are released through `qm_gbase8s_cdc_free` after bounded copying into Go memory.

### Legacy Go plugin

`QMIGRATION_GBASE8S_CDC_PROVIDER_PLUGIN=/path/provider.so` remains compatibility-only. Native and legacy providers cannot be configured simultaneously. Any legacy implementation used with RC28 must implement the same Agent API v4 behavioral contract.

## Runtime HTTP API

RC28 requires Agent API `v4`. Endpoint paths remain `/v1/...` for transport compatibility; the `api_version` returned by health/status is the behavioral fence.

- `GET /v1/health`
- `GET /v1/status`
- `GET /metrics`
- `POST /v1/checkpoint` with `{database,tables[]}`
- `POST /v1/records` with `{database,start_sequence,expected_capture_lineage,tables[],max_records,max_bytes}`

A non-loopback listener requires TLS certificate/key and Bearer token. Remote plaintext and URL userinfo fail closed. Provider Health/Checkpoint/Read calls are serialized because one datasource-local capture/smart-LOB session is not assumed concurrent-safe.

## Selection, schema fence and capture lineage

`TableSelection.id` is QMigration's stable hash of `schema\0table` and MUST be used as `cdc_startcapture` user data. RC23 schema fencing remains mandatory. The selection includes ordered `schema_columns`, primary keys and `schema_fingerprint`; for BLOB/CLOB columns RC28 also derives:

```json
{"name":"payload","column_type":"BLOB","nullable":true,"primary_key":false,"smart_lob":"blob"}
```

The provider MUST independently validate the live `CDC_REC_TABSCHEMA` + catalog metadata and return exactly one matching schema fence for every selected table at checkpoint and on every read. Echoing the request fingerprint without live validation is invalid.

Checkpoint/read responses also keep the RC24 64-hex `capture_lineage`. It may remain stable only when resuming the same logical capture. Recreating/replacing the capture MUST rotate lineage. Every read checks `expected_capture_lineage` before returning records.

QMigration persists:

`GBASE8S_CDC_SEQ = restart=<sequence>;commit=<sequence>;capture=<64-hex>`

`commit` is the latest durably accepted COMMIT; `restart` is the lowest BEGIN of every still-open transaction, or `commit` when no transaction is open. QMigration enforces `restart <= commit`, monotonic ACK and duplicate suppression after restart.

## Normalized records

The provider normalizes records as BEGIN, COMMIT, ROLLBACK, INSERT, DELETE, UPDATE_BEFORE, UPDATE_AFTER, DISCARD, TRUNCATE, TABLE_SCHEMA, TIMEOUT or ERROR. INSERT/DELETE/UPDATE row images must contain the complete selected column list in order. NULL cannot carry a payload. Arbitrary binary data uses `encoding=base64`; invalid/reordered/missing/extra data fails before target apply.

`ReadResponse.next_sequence` is mandatory whenever records are returned and cannot move backwards. QMigration independently enforces requested `max_records` / `max_bytes` and the bounded HTTP response size.

## RC28 smart BLOB/CLOB exact-image contract

Smart BLOB/CLOB source CDC is **off by default** and requires:

```bash
export QMIGRATION_EXPERIMENTAL_GBASE8S_NATIVE=1
export QMIGRATION_EXPERIMENTAL_GBASE8S_CDC=1
export QMIGRATION_EXPERIMENTAL_GBASE8S_SMART_LOB_CDC=1
```

If any selected table contains smart BLOB/CLOB, both checkpoint and read responses MUST declare:

```json
{"smart_lob_image_contract":"cdc-event-owned-lob-v1"}
```

For every non-NULL smart LOB field in INSERT/DELETE/UPDATE_BEFORE/UPDATE_AFTER, the same record MUST include exactly one proof:

```json
{
  "column":"payload",
  "kind":"blob",
  "byte_length":1048576,
  "sha256":"<64-hex-sha256-of-exact-transported-bytes>",
  "acquisition":"cdc-event-owned-lob-v1"
}
```

Rules:

- `kind` must match the selected source type (`blob` or `clob`).
- `byte_length` and SHA-256 must match the exact event bytes QMigration receives.
- BLOB payloads must use base64 transport; CLOB proof hashes the exact transported UTF-8 bytes.
- NULL smart LOBs must not have a proof; non-NULL smart LOBs must have one and only one.
- proofs for a non-LOB column, duplicate proofs, malformed hashes, wrong lengths, or any acquisition value other than `cdc-event-owned-lob-v1` fail closed before target apply.
- UPDATE before/after images are independently proven.

The provider is therefore attesting that it acquired the bytes from the **CDC event-owned locator/stream/version**, not by executing a later SQL query against the current table row. `SELECT ... WHERE primary_key=...` fallback is explicitly forbidden: while the CDC reader is behind, a later source transaction may have replaced the LOB and such a query cannot prove the historical image.

This contract gives QMigration transport-integrity and provider-attestation checks; production promotion still requires retained real GBase 8s V8.8 CSDK qualification demonstrating historical event bytes while newer row versions exist.

### Bounded-value limitation

RC28 still returns complete normalized records from the provider. The Agent/Reader impose bounded response sizes (the current HTTP response cap is 64 MiB and the normal Reader request budget defaults lower). Therefore smart LOB values that cannot fit the configured bounded record/response path remain fail-closed. RC28 does **not** claim unbounded multi-gigabyte smart-LOB chunk streaming.

TEXT/BYTE simple LOBs and opaque/collection/UDT families are not widened by this RC28 contract and remain unsupported for source CDC unless a separate exact-image contract is implemented.

## Qualification

`qmigration-gbase8s-qualify --cdc-smart-lob` implies CDC qualification and requires a selected table containing BLOB/CLOB. It verifies the Agent v4 contract is declared. Retained production evidence should additionally mutate a LOB in a later transaction while the consumer is intentionally behind and prove the earlier event still returns the earlier bytes/hash; a provider that reconstructs from the current row must fail this test.
