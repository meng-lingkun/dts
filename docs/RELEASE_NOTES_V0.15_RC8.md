# QMigration V0.15.0-rc8 Release Notes

RC8 closes the most common remaining Db2 LUW source-CDC row-format gaps after RC7. The source path remains experimental and correctness-first.

## DB2 VALUE COMPRESSION

- Decode documented full-row VALUE COMPRESSION offset-array format.
- Support normal values, NULL attributes and `COMPRESS SYSTEM DEFAULT` for qualified numeric/fixed-character fields.
- Accept only unambiguous complete row layouts; partial/ambiguous compressed images fail closed.
- Table-level classic/adaptive row compression remains delegated to IBM `db2ReadLog` with `DB2READLOG_FILTER_ON`; QMigration does not reverse-engineer a compression dictionary.

## Logged out-of-row data

- Reconstruct chunked logged BLOB/CLOB/DBCLOB and varying values from documented LOB manager records.
- Detect gaps, conflicting overlaps, oversized transactions and non-materializable NOT LOGGED records.
- Support consolidated out-of-row varying payloads (`column_id=65535`).
- `SYSCAT.COLUMNS.LOGGED='N'` LOB columns are rejected during CDC table selection, before the reader advances an LRI.

## Serialized XML

- Decode Db2 11.5.8+ CSL component 15 / operation 114 serialized XML records.
- Concatenate multi-record UTF-8 XML by selected column before the following DMS INSERT/UPDATE row is decoded.
- Selected XML tables require the live `DB2_DCC_XML_SERIALIZE=YES` setting; inability to verify it fails selection.

## Correctness boundaries

RC8 still refuses to advance a selected transaction when it encounters an unqualified multi-insert layout, compensation/savepoint/undo sequence requiring net-effect reconstruction, an unsupported LOB operation that cannot produce a complete image, unknown selected-table DMS action, or unqualified pureScale multi-stream ordering.

## Qualification

The Go/DRDA/log parser paths are covered by synthetic protocol/log tests. Production certification still requires retained Db2 LUW 11.5/12.1 reports using the IBM SDK/libdb2 provider on a real source host. Experimental gates remain enabled.
