# QMigration V0.15.0-rc9 Release Notes

RC9 continues the DB2 LUW source-CDC correctness work. It removes the blanket
fail-closed boundary for documented inline multi-insert and the common row-level
compensation/savepoint rollback path, while keeping unsupported ambiguous formats
fail closed. DB2 source CDC remains experimental until retained real-instance
qualification exists.

## DB2 multi-insert

- Decode DMS function 167 (Insert Multiple Records) using the documented row-count,
  row-length and variable-description structure.
- Preserve each 6-byte RID and expand one physical multi-insert log record into one
  ordered QMigration CDC INSERT event per logical row.
- Validate bounded row counts, row-length sums, description boundaries and row images.
- Decode DMS function 168 rollback descriptions and cancel the matching buffered
  multi-insert rows before target apply.

## Compensation / SAVEPOINT net effect

- Parse the documented 40-byte normal, 56-byte compensation and 64-byte propagatable
  compensation log-manager headers instead of assuming every DMS payload begins at 40.
- Maintain a per-transaction `(table, RID, operation)` identity ledger for buffered
  selected-table mutations.
- Reconstruct row-level rollback for undo insert, undo delete, undo update, empty-page
  variants and undo multi-insert by removing the latest matching buffered mutation.
- SAVEPOINT-style partial rollback therefore commits only the surviving source changes.
- A compensation RID that cannot be matched, or a selected-table compensation function
  outside the qualified set, marks the transaction unsafe and fails closed at commit.

## Row-format correctness hardening

- Correct Insert/Delete-to-empty-page row-image offsets to the documented byte 26.
- Reject outer row type `0x02` as an incomplete/indirect image instead of attempting a
  positional decode. This protects update relocation/decomposition paths until their
  preceding-insert linkage is implemented and qualified.
- Compensation delete/update rows do not need to be decoded when RID identity is enough
  to cancel a previously buffered operation, avoiding unnecessary row-format coupling.

## Verification

Synthetic protocol/log tests cover multi-insert expansion, undo multi-insert, insert/
delete/update compensation, SAVEPOINT-style partial rollback, unmatched-RID failure and
propagatable compensation header validation. The complete Go tree passes `go test ./...`
and `go vet ./...`; the DB2 log package also passes the race detector.

Production certification still requires retained Db2 LUW 11.5/12.1 reports using IBM
`db2ReadLog` on real source hosts.
