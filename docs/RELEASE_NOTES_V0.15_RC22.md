# QMigration V0.15.0-rc22 Release Notes

RC22 closes the next documented GBase 8s V8.8 CDC gap: transactional `TRUNCATE`.
The source CDC/restart architecture from RC20/RC21 is unchanged; RC22 adds a
first-class truncate event and a target-side transactional truncate primitive.

## Source CDC TRUNCATE

The GBase 8s/Informix CDC record format identifies `CDC_REC_TRUNCATE` with a
sequence number, transaction ID and the `user_data` table ID registered by
`cdc_startcapture`; the record has no row payload. RC22 requires exactly that
normalized shape from the local CSDK provider.

Reader rules are correctness-first:

- TRUNCATE must reference an open transaction and selected table;
- row fields on TRUNCATE are rejected;
- an unmatched UPDATE_BEFORE cannot be followed by TRUNCATE;
- only COMMIT or ROLLBACK may follow TRUNCATE in the transaction;
- a second TRUNCATE, DML or DISCARD after TRUNCATE fails closed;
- DML before TRUNCATE remains in the same emitted QMigration transaction.

These rules mirror GBase 8s transaction semantics, where TRUNCATE is logged and
can be rolled back, but after TRUNCATE only COMMIT/ROLLBACK is valid in that
transaction.

## Target apply

A new `TruncateTableConnector` SPI avoids pretending that heterogeneous DDL text
is portable. RC22 GBase 8s target apply implements the primitive as
`TRUNCATE TABLE <owner>.<table>` only while an explicit QMigration CDC target
transaction is active. The service requires TRUNCATE to be the final event in
the batch before target COMMIT. Targets without this primitive fail closed.

This preserves source sequences such as `INSERT -> UPDATE -> TRUNCATE -> COMMIT`
atomically on a GBase 8s target.

## Boundaries unchanged

- smart BLOB/CLOB values remain unsupported: the CDC stream does not directly
  contain the smart-LOB content and a separate locator/read path is required;
- unsupported complex/opaque/collection types remain rejected;
- real GBase 8s V8.8 + matching Client-SDK TRUNCATE commit/rollback/restart
  qualification is still required before production promotion;
- GBase 8a and GBase 8c are not implied by this change.
