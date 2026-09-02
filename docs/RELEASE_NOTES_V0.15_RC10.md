# QMigration V0.15.0-rc10 Release Notes

RC10 continues the correctness-first Db2 LUW source-CDC work from RC9. The
release implements two documented update-relocation forms that previously
failed closed: indirect UPDATE after-images stored in a preceding INSERT, and
`lrIUDflags=0x8000` decomposed delete/insert updates. DB2 source CDC remains
experimental until retained real-instance qualification is complete.

## Indirect UPDATE after-image linkage

- Parse the second UPDATE row header before attempting positional row decoding.
- When the new row outer type has `0x02`, treat the UPDATE as an indirect image.
- Link it only to the immediately preceding selected mutation when that mutation
  is an INSERT from the same table whose complete outer row type has `0x04`.
- Convert the buffered INSERT into one logical UPDATE, preserving the INSERT
  after-image and attaching the UPDATE before-image.
- Re-key the logical event to the UPDATE LRI/RID so ordinary `DMSUndoUpdate`
  compensation can remove it using the existing row-level rollback ledger.
- Invalidate the candidate across another selected mutation, an unselected DMS
  row mutation in the same transaction, compensation, out-of-row group start,
  or subtransaction merge.
- Missing, stale, cross-table or ambiguous candidates fail closed and do not
  advance `DB2_LRI`.

## Decomposed updates

- Decode `lrIUDflags` at the documented IUD header offset.
- Recognize flag `0x8000` as a decomposed update.
- Buffer the flagged DELETE before-image and require the corresponding flagged
  INSERT to complete the pair.
- Emit one logical CDC UPDATE rather than exposing the transient DELETE+INSERT.
- Require same selected table and strict pair completion; a second delete,
  unrelated selected mutation, subtransaction boundary or COMMIT with an open
  pair fails closed.

## Correctness boundaries

RC10 does not claim arbitrary decomposed-update rollback reconstruction. A
compensation sequence encountered while a decomposed pair is incomplete remains
fail closed until retained real-Db2 traces establish its exact supported order.
Out-of-row multi-insert, Db2 VECTOR, non-UTF8 codepage conversion and pureScale
multi-stream ordering also remain outside the production claim.

## Verification

Synthetic tests cover indirect-update linkage, missing/stale candidates,
unselected-table invalidation, ordinary complete INSERT preservation,
`DMSUndoUpdate` rollback after linkage, decomposed delete/insert pairing and
interrupted-pair failure. The DB2 CDC package passes race-enabled tests; the
complete Go tree is release-qualified separately during archive creation.
