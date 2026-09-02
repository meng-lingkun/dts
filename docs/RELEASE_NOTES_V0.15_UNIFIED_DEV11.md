# QMigration V0.15.0-unified-dev11 Release Notes

## Scope

This snapshot continues the single QMigration Unified Engine. It does not add an external migration runtime. The main change is to advance the native Oracle connector from a post-ACCEPT TTC session skeleton to an executable, testable TTC negotiation and password-authentication wire path without falsely enabling Oracle Full/CDC capabilities.

## Oracle Native TTC wire codecs

- Added strict QMigration-owned TTC compact integer, fixed integer, CLR and key/value dictionary codecs.
- Added TTC protocol negotiation on the live TNS/TCPS session and parsing of server protocol version, charset/ncharset, flags and compile/runtime capabilities.
- Added TTC datatype negotiation with client/server capability intersection, timezone handling and the migration-relevant Oracle scalar/LOB representation table.
- Malformed, truncated, over-sized or unterminated negotiation frames fail closed.

## Oracle password authentication path

- Added parsing for `AUTH_SESSKEY` and `AUTH_VFR_DATA` challenge dictionaries.
- Added encrypted password/session-key proof construction for verifier families 0, 2361 and 6949.
- Added authentication response/result decoding including session properties and explicit ORA error reporting.
- Added a complete Fake-TTC transcript covering TNS ACCEPT -> TTC protocol -> datatype -> auth-init -> challenge -> encrypted proof -> success summary.
- Regression tests assert that the datasource plaintext password never appears in the auth response.

## Capability boundary

Oracle deep probing is opt-in only:

```text
QMIGRATION_EXPERIMENTAL_ORACLE_TTC_NEGOTIATION=1
QMIGRATION_EXPERIMENTAL_ORACLE_TTC_AUTH=1
```

Even with those gates enabled, the production Connector descriptor still exposes only `protocol-probe`. This version does **not** claim real-Oracle E2E qualification, native SQL Execute, Data Dictionary execution, Full Reader/Writer or Redo/LogMiner CDC.

## Metadata

`027_v015_oracle_ttc_auth_codec.sql` advances the Metadata Schema marker to `0.15.0-unified-dev11` without adding table columns.

## Archive requirement

The version is accepted only after the mandatory archive flow produces a source ZIP, dev10->dev11 incremental patch, formal V0.13 cumulative patch, SHA-256 file and manifest, and verifies both restored trees byte-for-byte before running `go test ./...` and `go vet ./...`.
