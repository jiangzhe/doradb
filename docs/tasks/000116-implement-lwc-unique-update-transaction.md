---
id: 000116
title: Implement LWC Unique Update Transaction
status: implemented
created: 2026-04-11
github_issue: 547
---

# Task: Implement LWC Unique Update Transaction

## Summary

Implement update-by-unique-key for rows that have already been persisted into
LWC blocks. Persisted LWC rows are immutable, so the update path must represent
the old row as a cold-row delete marker and insert the modified row into the
hot RowStore with a new row id.

The foreground transaction flow should:

1. look up the target through the unique index;
2. resolve the row id through the block index;
3. for an LWC row, verify that the persisted row still exists, is visible to
   the writer, and matches the lookup key;
4. acquire write ownership by installing an in-transaction
   `ColumnDeletionBuffer` delete marker;
5. decode the persisted row values, apply the update, and insert a new hot row;
6. record undo and redo for the cold delete plus hot insert;
7. maintain secondary indexes through the existing hot index-update helpers;
8. extend unique-index runtime back-links only where unique MVCC ownership
   transfer requires a visibility bridge to an older owner.

## Context

Doradb stores hot mutable rows in RowStore pages and checkpointed warm rows in
immutable LWC blocks. Existing architecture documents define cold updates as
delete-marker plus hot reinsertion: the old persisted row is hidden through
cold-row deletion metadata, and the updated values are inserted into RowStore
with a new row id.

`TableAccessor::update_unique_mvcc(...)` currently handles RowStore rows but
still stops at `RowLocation::LwcBlock { .. } => todo!("lwc block")`. Cold delete
already demonstrates the required transaction ownership pattern: an
in-transaction `ColumnDeletionBuffer` marker represents the pending delete,
rollback removes the marker through row undo with `page_id = None`, commit
backfills the transaction status to a CTS, and recovery can rebuild newer cold
delete markers from redo.

Unique indexes need additional care. Hot-row unique ownership transfer uses
runtime unique-index back-links so an older snapshot can still find an older
owner when the latest unique mapping points to a newer row. Non-unique indexes
do not use these links; they remain exact `(logical_key, row_id)` entries.

Source Backlogs:
- `docs/backlogs/000011-cold-update-delete-insert-integration-with-column-deletion-buffer.md`

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

## Goals

- Implement `update_unique_mvcc(...)` for `RowLocation::LwcBlock` candidates.
- Keep cold update semantics as delete-marker plus hot RowStore insert.
- Use existing `ColumnDeletionBuffer` delete markers as the LWC row
  write-conflict mechanism; do not introduce a separate lock marker.
- Recheck the decoded persisted row against the lookup unique key before
  modifying state.
- Decode the full persisted row, apply the requested update columns, and insert
  the modified values into RowStore.
- Record cold delete undo with `page_id = None` and `RowUndoKind::Delete` so
  rollback removes the deletion-buffer marker.
- Log cold delete redo with `page_id = INVALID_PAGE_ID` and log the hot insert
  through the existing RowStore insert redo path.
- Reuse existing secondary-index update helpers for row-id and key-change
  handling after the hot replacement row is inserted.
- Preserve non-unique index behavior as exact-entry insert/delete-shadow
  maintenance with no runtime back-links.
- Investigate and implement the unique-only runtime back-link support needed
  when the older unique owner is a cold terminal row.
- Restructure `IndexBranch` enough to make hot and cold unique-link semantics
  explicit instead of forcing cold owners into `RowUndoRef`.
- Fix rollback paths directly exercised by the new cold unique update flow.

## Non-Goals

- Do not add a `Lock` variant or lock-only state to `ColumnDeletionBuffer`.
  The current task uses delete markers for delete/update ownership and write
  conflict detection. A future explicit lock operation can introduce a new
  marker state if needed.
- Do not redesign deletion-buffer GC or persisted marker cleanup.
- Do not change checkpoint publication, `deletion_cutoff_ts`, table-file
  metadata, or recovery replay boundaries.
- Do not mutate persistent `DiskTree` state on the foreground path.
- Do not redesign non-unique index lookup or scan behavior.
- Do not implement unrelated `RowLocation::LwcBlock` TODO branches unless the
  cold unique update path directly requires them for correctness.
- Do not introduce a broad unique-index abstraction rewrite beyond the
  `IndexBranch` and helper changes required for cold terminal owners.

## Unsafe Considerations (If Applicable)

No new `unsafe` code is expected.

The implementation should use existing validated persisted-block access APIs:
`ColumnBlockIndex`, `PersistedLwcBlock`, and LWC decode helpers. If the work
unexpectedly touches unsafe page or LWC layout code, update the unsafe review
scope before continuing.

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Add a persisted LWC row read and validation helper in `table/access.rs`.
   - Accept the resolved `RowLocation::LwcBlock` fields: `block_id`, `row_idx`,
     and `row_shape_fingerprint`.
   - Check `ColumnDeletionBuffer` visibility for the writer before accepting
     the old row.
   - Decode the full row values through `PersistedLwcBlock`.
   - Recheck that the decoded values still match the unique lookup key.
   - Return `NotFound` when the cold row is already deleted for the writer, and
     `WriteConflict` when an uncommitted marker from another transaction owns
     the row.

2. Implement cold update ownership in `update_unique_mvcc(...)`.
   - Call `ColumnDeletionBuffer::put_ref(old_row_id, stmt.trx.status())`.
   - Map `DeletionError::WriteConflict` to `UpdateMvcc::WriteConflict`.
   - Map `DeletionError::AlreadyDeleted` to `UpdateMvcc::NotFound`.
   - Push `OwnedRowUndo::new(table_id, None, old_row_id, RowUndoKind::Delete)`.
   - Add cold delete redo as `RowRedoKind::Delete` with
     `page_id = INVALID_PAGE_ID`.

3. Build and insert the new hot row.
   - Apply the requested update columns to the decoded persisted row values.
   - Track old indexed values in `index_change_cols` using the same convention
     as the RowStore update path.
   - Insert the modified row through `insert_row_internal(...)`.
   - Mark the new page dirty after successful index maintenance.

4. Reuse existing index update helpers for the logical index work.
   - If no indexed column changed, call
     `update_indexes_only_row_id_change(...)`.
   - If indexed columns changed, call
     `update_indexes_may_both_change(...)`.
   - Preserve `DuplicateKey` and `WriteConflict` outcomes.
   - Preserve non-unique exact-entry semantics: insert the new exact entry and
     delete-mark the old exact entry. Do not add non-unique back-links.

5. Restructure unique-index runtime back-links for cold terminal owners.
   - Keep runtime back-links unique-only.
   - Change `IndexBranch` so hot and cold targets are not represented by the
     same mandatory `RowUndoRef` fields.
   - Preserve the existing hot-row branch semantics with a valid commit
     timestamp for the old same-key owner delete/update point and a
     `RowUndoRef` continuation.
   - Add a cold terminal target that can reconstruct the old cold owner from
     `undo_vals` and then stop, because persisted LWC rows do not have row-page
     history chains.
   - Investigate whether the cold terminal target needs only
     `delete_cts: Option<TrxID>` or also the old cold `row_id`. Default toward
     the minimal timestamp-only shape unless implementation evidence shows
     `row_id` is needed for rollback, assertions, or cleanup.
   - Interpret `delete_cts: Some(ts)` as a previously committed delete of the
     old cold owner by another transaction. Visibility can be resolved by
     comparing the reader STS against `ts`.
   - Interpret `delete_cts: None` as the same-row cold-to-hot update case where
     the current transaction's delete marker hides the old cold owner only from
     readers that can see the new hot row.

6. Extend unique-link construction.
   - For same-row cold-to-hot updates, add cold terminal links for unique
     indexes whose latest mapping moves from old cold row id to new hot row id
     when older snapshots may need the cold owner.
   - For key-changing updates that claim a key from a previously deleted cold
     owner, decode that cold owner, validate that it matches the claimed unique
     key, and add a cold terminal link with the committed delete timestamp.
   - For hot old owners, keep the current `RowUndoRef` branch behavior.
   - Do not create links for non-unique indexes.

7. Extend read-time branch resolution.
   - For hot branch targets, preserve existing undo-chain traversal semantics.
   - For cold terminal branch targets, apply `undo_vals` to reconstruct the
     cold row values, verify that the reconstructed key still matches the
     unique lookup key, apply the optional `delete_cts` visibility rule, and
     return the reconstructed row as the terminal visible version when valid.
   - Ensure table scans do not follow unique-index branches, as today.

8. Fix rollback support for directly exercised LWC cases.
   - Existing row undo rollback should remove the cold delete marker because
     the cold delete undo has `page_id = None`.
   - Add LWC handling in index undo rollback for the `UpdateUnique(...,
     deleted = true)` path when the old unique owner is a cold row.
   - Ensure failed statements/transactions do not leak the new unique mapping
     or leave the old cold row delete-marked.

9. Keep recovery compatible with existing boundaries.
   - Recovery should replay the cold delete redo into `ColumnDeletionBuffer`
     when `row_id < pivot_row_id` and `cts >= deletion_cutoff_ts`.
   - Recovery should replay the hot insert redo for the new row through the
     existing RowStore insert path.
   - Do not persist runtime unique back-links; no pre-crash active snapshot
     survives restart.

10. Update focused tests and validation.
    - Add targeted tests in `table/tests.rs` and recovery tests where needed.
    - Run `cargo nextest run -p doradb-storage`.

## Implementation Notes

1. Implemented LWC unique update as cold delete plus hot insert:
   - `update_unique_mvcc(...)` now handles `RowLocation::LwcBlock` by checking
     `ColumnDeletionBuffer` visibility, decoding the persisted row, validating
     the lookup key, installing a delete marker, recording cold delete
     undo/redo, inserting the modified hot row, and maintaining secondary
     indexes through existing index helpers.
   - LWC delete/update paths now mask old index entries through the deferred
     delete-index helpers so rollback and GC see the same logical shape as hot
     row delete/update.
   - LWC delete reads only index-related columns for delete-index maintenance
     instead of decoding the full cold row.

2. Added explicit hot and cold unique-branch targets:
   - `IndexBranchTarget::Hot` preserves the existing row-page undo-chain
     continuation semantics.
   - `IndexBranchTarget::ColdTerminal { delete_cts }` reconstructs a terminal
     cold owner from stored undo values and applies the optional delete-CTS
     visibility rule without needing the old cold `row_id` in the branch
     target.
   - Unique-link creation now handles old LWC owners, including same-row
     cold-to-hot updates and claims over committed deleted cold owners; non-
     unique indexes remain exact-entry insert/delete-shadow maintenance only.

3. Hardened rollback and stale-owner handling for cold unique owners:
   - unique-index undo handles stale/purged cold owners by deleting the masked
     replacement claim instead of treating missing cold rows as unreachable;
   - rollback for `RowLocation::LwcBlock` uses the same purgeability decision
     as the LWC purge path, restoring a masked owner only when snapshots still
     need the deleted marker and removing the replacement claim when it is safe
     to drop;
   - `min_active_sts` calculation is lazy and only evaluated on the rollback
     path when a deleted cold owner actually needs a purgeability decision.

4. Fixed snapshot visibility edge cases found during review:
   - `link_for_unique_index_lwc(...)` treats a committed CDB marker as linkable
     only when `delete_cts <= stmt.trx.sts`; a later committed delete remains
     visible to the writer and rejects the duplicate claim.
   - `ColumnDeletionBuffer::put_ref(...)` now accepts the caller snapshot and
     returns `AlreadyDeleted` only for deletes visible to that snapshot; later
     committed deletes surface as `WriteConflict`.

5. Added focused regression coverage for the implemented behavior:
   - same-key LWC update preserving old snapshots;
   - key-changing LWC update preserving old/new key visibility;
   - duplicate-key rollback for cold marker plus hot insert;
   - claiming committed deleted cold owners with a cold terminal bridge;
   - rollback restore/drop behavior for deleted cold owners;
   - stale unique-index purge for missing/purged cold owners;
   - snapshot-aware CDB marker handling for update/delete paths.
   Validation passed with `cargo nextest run -p doradb-storage` (`529` tests).

## Impacts

- `doradb-storage/src/table/access.rs`
  - `TableAccessor::update_unique_mvcc`
  - `TableAccessor::link_for_unique_index`
  - index update helper call sites used by out-of-place update
  - persisted LWC read/decode helper paths
- `doradb-storage/src/table/deletion_buffer.rs`
  - existing `ColumnDeletionBuffer` APIs are used for cold update ownership
- `doradb-storage/src/trx/undo/row.rs`
  - `IndexBranch`
  - unique branch target representation
  - `RowReadAccess::read_row_mvcc`
  - `RowWriteAccess::link_for_unique_index`
- `doradb-storage/src/trx/undo/index.rs`
  - rollback for unique index updates involving old cold owners
- `doradb-storage/src/trx/redo.rs`
  - existing `RowRedoKind::Delete` and `RowRedoKind::Insert` shapes are reused
- `doradb-storage/src/table/recover.rs`
  - recovery behavior should remain compatible with cold delete plus hot insert
    replay
- `doradb-storage/src/lwc/block.rs`
  - existing persisted row decode APIs are reused
- `doradb-storage/src/table/tests.rs`
  - focused MVCC, rollback, conflict, and recovery coverage

## Test Cases

- Update a persisted LWC row without changing its unique key.
  - New lookup returns the hot replacement row for new snapshots.
  - An older snapshot can still see the old cold row through the unique lookup.
  - The old cold row has an in-transaction delete marker during the update.

- Update a persisted LWC row and change its unique key.
  - Old key no longer returns the latest row to new snapshots.
  - New key returns the hot replacement row.
  - Older snapshots see the correct pre-update cold row where MVCC requires it.

- Change a cold row's unique key to a key owned by another visible row.
  - The update returns `UpdateMvcc::DuplicateKey`.
  - Rollback/statement failure leaves the old cold row and indexes unchanged.

- Change a cold row's unique key to a key whose previous owner is a committed
  deleted cold row.
  - The update succeeds.
  - A cold terminal unique back-link protects older snapshots that still need
    the deleted cold owner.

- Concurrent cold delete/update conflict.
  - A second writer trying to update or delete the same LWC row while an
    uncommitted delete marker exists returns `WriteConflict`.

- Rollback after cold update.
  - The old cold row's deletion-buffer marker is removed.
  - The hot replacement row is rolled back.
  - Unique and non-unique index changes are rolled back.

- Non-unique index maintenance during cold update.
  - Non-unique entries are maintained by exact `(key, row_id)` insert and
    delete-shadow only.
  - No non-unique runtime back-link is created or followed.

- Recovery after committed cold update.
  - Cold delete redo rebuilds the deletion-buffer marker when it is newer than
    `deletion_cutoff_ts`.
  - Hot insert redo rebuilds the replacement RowStore row and latest index
    mapping.
  - Runtime unique back-links are not recovered and are not required after
    restart.

- Validation command:
  - `cargo nextest run -p doradb-storage`

## Open Questions

- Future work may add an explicit lock-only state to `ColumnDeletionBuffer` if
  LWC rows need lock operations independent from delete/update. This task
  intentionally keeps delete markers as the LWC write ownership record.
