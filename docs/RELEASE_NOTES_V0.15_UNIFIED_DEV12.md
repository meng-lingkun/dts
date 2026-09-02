# QMigration V0.15.0-unified-dev12 Release Notes

## Scope

This snapshot continues the single QMigration Unified Engine and advances the native Oracle connector one layer above dev11 authentication: a bounded, experimental TTC SELECT request/describe/row wire path. It does not add an external migration runtime and it does not enable production Oracle metadata, Full Load or CDC capabilities.

## Experimental Oracle TTC SELECT

- Added a QMigration-owned bind-free OALL8 parse+execute request codec for authenticated TTC sessions.
- Added `QMIGRATION_EXPERIMENTAL_ORACLE_TTC_QUERY=1`. The query gate implies protocol/datatype negotiation and password authentication, then executes `SELECT 1 AS QMIGRATION_PROBE FROM DUAL`.
- Request size, fetch rows, response messages and probe rows are explicitly bounded and fail closed on malformed frames.

## Describe and row codecs

- Added strict column-describe decoding for Oracle datatype, precision, signed scale, maximum length, charset/form, character length, nullability, name and type name.
- Added scalar row decoding for NUMBER, VARCHAR/CHAR/NCHAR/LONG, DATE and RAW families, while unknown/LOB values remain byte-preserving until complete semantics are qualified.
- Oracle NUMBER is converted to a canonical decimal string, avoiding float64 precision loss in the migration boundary.
- Oracle DATE is decoded as a timezone-less timestamp string; QMigration does not invent a timezone the source value does not carry.

## Qualification boundary

A complete Fake-TTC transcript now covers TNS ACCEPT -> protocol -> datatype -> auth -> OALL8 SELECT -> describe -> row header -> row data -> status. Malformed describe frames and representative NUMBER encodings are regression-tested.

The production Connector descriptor still exposes only `protocol-probe`. Real Oracle execution remains unqualified for packet coalescing, fetch continuation/cursor lifecycle, full OER/warning variants, ROWID and LOB streaming. Data Dictionary execution, Native Full Reader/Writer and Redo/LogMiner CDC therefore remain disabled.

## Metadata

`028_v015_oracle_ttc_query_codec.sql` advances the Metadata Schema marker to `0.15.0-unified-dev12` without adding table columns.

## Archive requirement

The snapshot is accepted only after the standard archive flow produces a source ZIP, dev11->dev12 incremental patch, formal V0.13 cumulative patch, SHA-256 file and manifest; restored trees must match byte-for-byte and pass `go test ./...` plus `go vet ./...`.
