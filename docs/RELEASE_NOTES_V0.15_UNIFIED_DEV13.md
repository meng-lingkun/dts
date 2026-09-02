# QMigration V0.15.0-unified-dev13 Release Notes

## Scope

This snapshot continues the single QMigration Unified Engine and hardens the experimental Oracle Native TTC SQL layer added in dev12. The focus is dataset/cursor correctness: coalesced TTC messages, fetch continuation, complete OER Summary parsing, ROWID wire decoding and bounded BLOB/CLOB locator/chunk primitives. No external migration runtime is introduced and Oracle production metadata/full/CDC capabilities remain disabled.

## TTC dataset and packet handling

- SELECT responses no longer assume one TTC message per TNS DATA packet. QMigration now consumes multiple describe/header/row/RPA/summary/status messages coalesced in one packet while preserving packet-level safety bounds.
- Added dataset row-header decoding including column count, row count, UAC length and Oracle's optional column-presence bit vector.
- Added code-21 column bit-vector updates for subsequent row batches.
- Added query return-parameter-area decoding for SCN fields, session timezone properties and bounded query-id bytes.

## OER Summary and cursor lifecycle

- Added complete QMigration-owned SELECT/fetch OER Summary parsing for the TTC versions currently negotiated by the connector.
- Cursor id, current row, return code, error position, flags, warning state and ORA error text are retained instead of treating the summary as only a terminal return code.
- `ORA-01403` is treated as cursor exhaustion rather than a migration error.
- Added native TTC fetch continuation request generation (`function 3/5`) and cursor-state guards.
- Added the modern cursor-close request primitive and bounded close-on-row-limit behavior.

## ROWID and LOB protocol foundation

- Added physical ROWID wire decoding and canonical 18-character Oracle ROWID encoding.
- BLOB/CLOB row values now preserve inline/prefetch bytes separately from the opaque server locator rather than pretending locator bytes are the LOB payload.
- Added bounded TTC `0x60` LOB read request encoding, RPA size decoding, code-14 chunk stream decoding and end-of-call handling.
- A qualification-only native locator read helper is present, but production Full Reader remains disabled until real Oracle BLOB/CLOB behavior is validated across supported versions and charset modes.

## Qualification coverage

Fake-TTC tests now cover:

- multiple TTC messages in one TNS DATA packet;
- describe + row-header + multiple rows + OER summary;
- cursor-id handoff and a second TTC fetch batch;
- `ORA` error text decoding;
- ROWID decoding;
- BLOB/CLOB request and chunk-stream codecs;
- cursor fetch/close request shapes.

The full backend passes `go test ./...` and `go vet ./...`.

## Capability boundary

`QMIGRATION_EXPERIMENTAL_ORACLE_TTC_QUERY=1` continues to imply negotiation and password authentication, but the production Connector descriptor still exposes only `protocol-probe`. This release does **not** claim real-Oracle Data Dictionary execution, Native Full Reader/Writer, real LOB qualification or Redo/LogMiner CDC.

## Metadata

`029_v015_oracle_ttc_fetch_lob.sql` advances the Metadata Schema marker to `0.15.0-unified-dev13` without adding table columns.

## Next development boundary

The next safe step is native Oracle Data Dictionary execution on this cursor-capable query runtime, starting with bind-safe metadata queries and real-instance qualification. Full Reader capability should remain disabled until real Oracle query/fetch/ROWID/LOB behavior is validated end-to-end.
