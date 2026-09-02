# QMigration V0.15.0-rc21 Release Notes

RC21 productizes the RC20 **GBase 8s V8.8 syscdcv1 source CDC provider
boundary**. The transaction/restart model is unchanged; the major change is that
the datasource-local vendor adapter can now be a stable native C ABI shared
library instead of a Go `buildmode=plugin` module.

## Native C ABI v1

`qmigration-gbase8s-cdc-agent` now prefers:

```bash
QMIGRATION_GBASE8S_CDC_PROVIDER_LIBRARY=/opt/qmigration/lib/qm-gbase8s-cdc-provider.so
```

The library exports the ABI in:

`deployments/gbase8s-cdc-provider/qmigration_gbase8s_cdc_provider.h`

Required symbols are:

- `qm_gbase8s_cdc_abi_version`
- `qm_gbase8s_cdc_open`
- `qm_gbase8s_cdc_health`
- `qm_gbase8s_cdc_checkpoint`
- `qm_gbase8s_cdc_read`
- `qm_gbase8s_cdc_free`
- `qm_gbase8s_cdc_close`

The agent loads the library with `dlopen(RTLD_NOW|RTLD_LOCAL)` and requires ABI
version `1`. This lets an operator build the provider with the local GBase
Client-SDK / ESQL-C toolchain and call `ifx_lo_read()` directly; it no longer
requires the provider itself to use the exact QMigration Go toolchain.

The RC20 Go plugin contract remains as a legacy compatibility path, but both
provider mechanisms may not be configured at the same time.

## Native provider integrity and local configuration

- native library path must be absolute, regular and not world-writable;
- optional `QMIGRATION_GBASE8S_CDC_PROVIDER_SHA256` pins the exact `.so` before
  `dlopen`;
- provider-local JSON can be supplied directly or through
  `QMIGRATION_GBASE8S_CDC_PROVIDER_CONFIG_FILE`;
- a provider config file must not be accessible by `other` users and is capped
  at 1 MiB;
- native JSON responses are capped at 64 MiB before entering Go memory.

A compileable ABI-only C example and build check are included. Release tests
also build a synthetic `.so` at test time and exercise ABI load, SHA pinning,
Health, Checkpoint, Read and Close.

## CDC provider conformance hardening

RC21 validates normalized provider rows before target apply:

- INSERT/DELETE/UPDATE images must contain the complete selected column list;
- column order/names must match the `cdc_startcapture` selection;
- only plain UTF-8 or `encoding=base64` values are accepted;
- invalid base64, NULL values carrying payloads, partial rows and unknown
  encodings fail closed;
- provider batches are independently bounded by requested `max_records` and
  `max_bytes`;
- buffered UPDATE_BEFORE images are included in the transaction memory gate.

Committed transactions that have no surviving DML now emit one CHECKPOINT CDC
event. This advances `GBASE8S_CDC_SEQ` during idle/rolled-back-to-savepoint
workloads instead of leaving the durable lag watermark stale.

`restart=<earliest-open-BEGIN>;commit=<last-applied-COMMIT>` is still validated
with `restart <= commit`, and acknowledged restart watermarks may not move
backwards.

## Agent transport hardening

- datasource-local provider calls are serialized so one CSDK smart-LOB session
  cannot be read concurrently;
- non-loopback agent listeners require both Bearer token and TLS;
- non-loopback client URLs require `gbase8scdcs://` / HTTPS;
- credentials in CDC URL userinfo are rejected;
- Bearer comparison is constant-time;
- HTTP header/read/write/idle bounds are explicit.

The `/v1/health` response now reports provider kind/ABI and whether native
SHA-256 pinning is active. `qmigration-gbase8s-qualify --cdc` records these
fields in the retained qualification report.

## Boundaries unchanged

RC21 still does not claim real-instance production qualification. Source
TRUNCATE, smart BLOB/CLOB and unsupported complex/opaque/collection types remain
fail-closed. The native C ABI is a software/provider integration contract; a
real GBase 8s V8.8 + matching Client-SDK provider must still be built and
retained-qualified before promotion.
