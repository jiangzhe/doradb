---
id: 000115
title: Harden LWC Index Purge Cleanup
status: implemented  # proposal | implemented | superseded
created: 2026-04-11
github_issue: 545
---

# Task: Harden LWC Index Purge Cleanup

## Summary

Implement the GC-only secondary-index purge path for rows that have moved from
row pages into persisted LWC blocks. The current purge-facing index-delete
helpers still panic on `RowLocation::LwcBlock { .. }`; this task replaces those
branches with correctness-preserving cleanup logic for both unique and
non-unique indexes.

The main rule is that a cold-row deletion marker is not sufficient by itself to
delete every purge entry for that row. A marker can be created by a later
delete after earlier key-change purge entries already exist. The LWC purge path
must therefore use a fast marker path only when the delete marker is committed
and globally purgeable, and otherwise compare the persisted LWC row's current
index key with the purge key before removing the stale entry.

## Context

`IndexUndoLogs::commit_for_gc()` converts only deferred index deletes into
`IndexPurgeEntry` values after a transaction has committed. `TransactionSystem`
then calls `TableHandle::delete_index(...)` from the purge thread. This
table-level index-delete helper is therefore physical GC cleanup, not the
foreground logical delete/update path.

Source Backlogs:
- `docs/backlogs/000049-purge-crashes-on-checkpointed-rows-in-index-delete-lwc-path.md`

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

The hot row-page cleanup path cannot decide solely from the current row's
delete flag. A purge entry can originate from:

1. a row delete, where the row should eventually disappear from the secondary
   index;
2. a key-changing row update, where the old key should disappear only if no
   visible version of that row still matches the old key.

That is why the existing hot path calls `RowReadAccess::any_version_matches_key`
before physically deleting a delete-marked index entry.

The LWC path differs because successful checkpoint freezes row pages, waits for
insert/update activity to become stable, moves pages to transition, and writes
a single persisted row image under the checkpoint cutoff. After a row has moved
to LWC, there is no row-page undo chain to walk for the persisted image.
Deletes are represented by `ColumnDeletionBuffer` and later persisted delete
payloads, while the LWC row values remain immutable.

The required cleanup cases are:

1. Row delete before checkpoint cutoff:
   - the row is skipped while building LWC;
   - no deletion-buffer marker is retained for the skipped row;
   - purge should reach column block-index lookup, observe `NotFound`, and
     remove the stale delete-marked index entry without reading an LWC block.
2. Row delete at or after checkpoint cutoff:
   - the row is persisted into LWC;
   - a committed deletion-buffer marker is installed;
   - once the marker timestamp is below `min_active_sts`, purge can remove the
     stale delete-marked index entry without column block-index or LWC access.
3. Direct delete of an already persisted row:
   - the delete path installs a deletion-buffer marker;
   - once that marker is globally purgeable, purge should use the same marker
     fast path and avoid block-index/LWC reads.
4. Key-changing update on a row page followed by checkpoint:
   - the purge entry can outlive row-page storage and later target an LWC row;
   - there may be no deletion marker, or there may be a later delete marker
     that is not globally purgeable for the currently active snapshots;
   - purge must resolve the row through `ColumnBlockIndex`, decode only the
     indexed LWC values, and delete the purge entry only when the persisted
     current key differs from the purge key.

This keeps the common delete cases cheap and confines LWC block reads to the
transition key-change cases that truly need persisted-key comparison.

## Goals

- Implement `RowLocation::LwcBlock { .. }` handling in
  `TableAccessor::delete_unique_index(...)`.
- Implement `RowLocation::LwcBlock { .. }` handling in
  `TableAccessor::delete_non_unique_index(...)`.
- Thread the purge `min_active_sts` into the table-level index-delete helper so
  LWC cleanup can distinguish a merely existing deletion marker from a globally
  purgeable deletion marker.
- Preserve the existing unique-index precheck:
  - the index entry exists;
  - it is delete-marked;
  - it still points to the same `row_id`.
- Preserve the existing non-unique precheck:
  - the exact `(key, row_id)` entry exists;
  - it is delete-marked.
- Add a shared LWC cleanup decision helper that:
  - uses a deletion-buffer fast path only when the marker is committed and
    `delete_cts < min_active_sts`;
  - treats uncommitted or not-yet-purgeable delete markers as insufficient;
  - falls back to `ColumnBlockIndex` lookup and persisted-key comparison when
    the marker cannot justify cleanup;
  - removes the purge entry on `RowLocation::NotFound`;
  - removes the purge entry on LWC persisted-key mismatch;
  - keeps the purge entry on LWC persisted-key match.
- Add detailed inline comments around the implementation explaining the
  GC-only origin, hot-row version-chain requirement, deletion-marker timestamp
  rule, and LWC persisted-key fallback.
- Re-enable or replace the ignored transition/delete-marker regression test
  from `table/tests.rs`.
- Cover both unique and non-unique LWC index purge cleanup.

## Non-Goals

- Do not redesign deletion-buffer GC or persisted marker reclamation. Existing
  follow-up: `docs/backlogs/000010-column-deletion-buffer-gc-and-persisted-marker-cleanup-policy.md`.
- Do not implement or redesign cold update/delete-plus-insert semantics for
  already persisted rows. Existing follow-up:
  `docs/backlogs/000011-cold-update-delete-insert-integration-with-column-deletion-buffer.md`.
- Do not change checkpoint publication, `deletion_cutoff_ts`, recovery replay
  boundaries, or table-file metadata.
- Do not implement unrelated `RowLocation::LwcBlock { .. }` TODO branches in
  rollback, recovery-CTS lookup, direct no-transaction DML helpers, or older
  uncommitted lookup/update helpers unless a focused test exposes a direct
  blocker for this purge path.
- Do not add a broad index-purge origin-tagging redesign unless the narrow
  implementation proves that the current `IndexPurgeEntry` shape cannot support
  correct cleanup.
- Do not add batch LWC purge optimization in this task. If single-entry
  persisted-key reads become a measurable bottleneck, capture batching as a
  follow-up.

## Unsafe Considerations (If Applicable)

No new `unsafe` code is expected.

The intended implementation uses existing validated buffer-pool,
`ColumnBlockIndex`, and `PersistedLwcBlock` decode APIs. If implementation
unexpectedly touches unsafe LWC/page layout code, update the unsafe review
scope before continuing.

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Thread purge visibility into table index-delete cleanup.
   - Add `min_active_sts: TrxID` to `TableHandle::delete_index(...)`.
   - Add the same parameter to `TableAccess::delete_index(...)` and the
     `TableAccessor` implementation.
   - Update the `TransactionSystem::purge_trx_list(...)` caller to pass its
     `min_active_sts` argument.

2. Add a private deletion-marker inspection helper in `table/access.rs`.
   - Accept `row_id` and `min_active_sts`.
   - Return whether the row has a committed cold delete marker with
     `delete_cts < min_active_sts`.
   - Treat `DeleteMarker::Committed(ts)` and committed `DeleteMarker::Ref`
     consistently.
   - Return false for missing, uncommitted, or committed-but-not-purgeable
     markers.
   - Do not rely on marker existence alone.

3. Add a private LWC persisted-key comparison helper in `table/access.rs`.
   - Accept `guards`, `key`, and the resolved `RowLocation::LwcBlock` fields:
     `block_id`, `row_idx`, and `row_shape_fingerprint`.
   - Build the read set from `metadata.index_specs[key.index_no].index_cols`.
   - Reuse the existing LWC read/decode path so row-shape fingerprint
     validation and block corruption handling remain unchanged.
   - Compare decoded indexed values with `key.vals`.
   - Return whether the persisted current key differs from the purge key.

4. Update `delete_unique_index(...)`.
   - Keep the existing index lookup and delete-mask/value checks before any
     storage lookup.
   - Check the purgeable deletion-marker fast path before `try_find_row(...)`.
   - If the marker is globally purgeable, call `compare_delete(...,
     ignore_del_mask = false, MIN_SNAPSHOT_TS)` and return its result.
   - If not, continue to the storage-location decision:
     - `NotFound`: delete the stale entry;
     - `RowPage`: keep the current `any_version_matches_key(...)` logic;
     - `LwcBlock`: delete only when persisted-key comparison reports mismatch.

5. Update `delete_non_unique_index(...)`.
   - Keep the exact-entry lookup and delete-mask check before any storage
     lookup.
   - Apply the same marker fast path and storage-location decision as the
     unique helper.
   - Ensure exact `(key, row_id)` semantics remain unchanged.

6. Add inline comments where the LWC helper is used.
   - Explain that this helper is GC-only physical cleanup.
   - Explain why hot rows need version-chain search.
   - Explain why a deletion marker must be both committed and older than
     `min_active_sts`.
   - Explain why LWC key decode is required only when no globally purgeable
     marker proves the whole row is invisible to active snapshots.

7. Re-enable the ignored transition regression.
   - Remove the ignore on
     `test_checkpoint_transition_delete_marker_waits_for_next_cutoff_range`,
     or replace it with equivalent coverage if the test shape needs to change.
   - The test must continue to prove that delete markers moved during row-page
     transition are not selected in the same checkpoint range and are persisted
     by a later checkpoint after cutoff advances.

8. Add targeted purge cleanup tests.
   - Unique row-delete transition with `delete_cts >= cutoff_ts`:
     marker fast path removes the stale delete-marked unique entry without
     panic.
   - Unique row-delete transition with `delete_cts < cutoff_ts`:
     row is skipped from LWC, column lookup returns not found, and cleanup
     removes the stale entry without LWC decode.
   - Unique key-change A/B transition case:
     one old-key purge entry is removed by persisted-key mismatch and the
     current-key entry is retained when it matches the LWC row.
   - Non-unique LWC purge path:
     exact stale `(key, row_id)` entry is removed only when the persisted key
     no longer matches or the row delete is globally purgeable.
   - Later-delete marker guard:
     if a marker exists but `delete_cts >= min_active_sts`, LWC key comparison
     still protects a matching current key from premature removal.

9. Run validation.
   - Run focused nextest tests for the new or changed purge/checkpoint cases.
   - Run `cargo nextest run -p doradb-storage`.
   - If the implementation unexpectedly changes backend-neutral I/O code, also
     run `cargo nextest run -p doradb-storage --no-default-features --features libaio`.

## Implementation Notes

- Implemented the GC-only LWC index-purge decision in
  `doradb-storage/src/table/access.rs`.
  - `TransactionSystem::purge_trx_list(...)` now passes `min_active_sts` into
    table-level index cleanup through `TableHandle::delete_index(...)` and
    `TableAccess::delete_index(...)`.
  - `TableAccessor` now uses a shared purge decision helper for both unique
    and non-unique indexes.
  - The helper uses the cold deletion-buffer fast path only when a marker is
    committed and `delete_cts < min_active_sts`.
  - When no globally purgeable marker proves full-row invisibility, the helper
    resolves storage location: `NotFound` removes the stale entry, `RowPage`
    keeps the existing undo-chain `any_version_matches_key(...)` protection,
    and `LwcBlock` decodes only indexed persisted values and removes the purge
    entry only when the persisted key differs from the purge key.
- Preserved the unique-index precheck that the delete-marked entry still points
  at the target `row_id`.
- Corrected the non-unique purge precheck so the exact `(key, row_id)` entry is
  removed only when it exists and is delete-marked.
- Re-enabled
  `test_checkpoint_transition_delete_marker_waits_for_next_cutoff_range`.
- Added focused table tests for:
  - unique LWC marker fast-path cleanup;
  - unique LWC persisted-key mismatch cleanup and matching-key retention when a
    later marker is not yet purgeable;
  - non-unique LWC persisted-key mismatch cleanup and matching-key retention;
  - stale delete-marked unique cleanup when row lookup returns `NotFound`.
- Review follow-up checked the LWC projection ordering concern. No code change
  was required because `PersistedLwcBlock::decode_persisted_row_values(...)`
  returns values in the requested `read_set` order, and the existing LWC block
  test covers an unsorted `[1, 0]` projection.
- Validation completed:
  - `cargo fmt`
  - focused `cargo nextest run -p doradb-storage` for the new/re-enabled purge
    tests
  - `cargo nextest run -p doradb-storage` (`517` tests passed)
  - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
  - `cargo nextest run -p doradb-storage test_lwc_block_decode_persisted_row_values`
- GitHub issue and PR:
  - Issue: `#545`
  - PR: `#546`

## Impacts

- `doradb-storage/src/trx/purge.rs`
  - Pass `min_active_sts` into table-level index cleanup.
- `doradb-storage/src/catalog/mod.rs`
  - Update `TableHandle::delete_index(...)` signature and dispatch.
- `doradb-storage/src/table/access.rs`
  - Update `TableAccess::delete_index(...)` signature.
  - Implement LWC marker/key-comparison cleanup helpers.
  - Replace `RowLocation::LwcBlock { .. } => todo!("lwc block")` in
    `delete_unique_index(...)`.
  - Replace `RowLocation::LwcBlock { .. } => todo!("lwc block")` in
    `delete_non_unique_index(...)`.
- `doradb-storage/src/table/tests.rs`
  - Re-enable or replace the ignored transition/cutoff regression.
  - Add unique and non-unique LWC purge cleanup coverage.
- `doradb-storage/src/table/deletion_buffer.rs`
  - May receive a narrow helper only if needed to avoid duplicating committed
    marker timestamp inspection in `table/access.rs`.

Affected concepts and interfaces:

- `IndexPurgeEntry`
- `TableHandle::delete_index`
- `TableAccess::delete_index`
- `TableAccessor::delete_unique_index`
- `TableAccessor::delete_non_unique_index`
- `ColumnDeletionBuffer`
- `RowLocation::LwcBlock`
- `RowReadAccess::any_version_matches_key`
- `PersistedLwcBlock` indexed-column decode path

## Test Cases

- Re-enabled transition/cutoff regression passes:
  `test_checkpoint_transition_delete_marker_waits_for_next_cutoff_range`.
- Purge no longer panics when a delete-marked unique index entry points to a
  row that has moved to LWC.
- Purge no longer panics when a delete-marked non-unique exact index entry
  points to a row that has moved to LWC.
- Row delete before checkpoint cutoff:
  - LWC row is absent;
  - deletion buffer has no marker;
  - purge removes the stale index entry after `NotFound`.
- Row delete at or after checkpoint cutoff:
  - LWC row exists;
  - deletion buffer has a committed marker;
  - purge removes the stale index entry through the marker fast path when
    `delete_cts < min_active_sts`.
- Direct delete of an already persisted row:
  - deletion buffer marker fast path removes the stale entry once the delete is
    globally purgeable.
- Key-change A/B transition:
  - stale old-key purge entry is removed when LWC persisted key mismatches;
  - current-key purge entry is retained when LWC persisted key matches.
- Later delete marker guard:
  - a marker with `delete_cts >= min_active_sts` does not justify cleanup;
  - the LWC persisted-key comparison still controls whether the purge entry is
    removed.
- Existing purge tests still pass for row-page undo purge and row-page index
  cleanup.
- Supported validation pass:

```bash
cargo nextest run -p doradb-storage
```

## Open Questions

- Deletion-buffer GC and persisted marker cleanup remain separate work:
  `docs/backlogs/000010-column-deletion-buffer-gc-and-persisted-marker-cleanup-policy.md`.
- Cold update/delete-plus-insert behavior for already persisted rows remains
  separate work:
  `docs/backlogs/000011-cold-update-delete-insert-integration-with-column-deletion-buffer.md`.
- If LWC persisted-key comparison is too expensive in workloads with many
  transition key-change purge entries, a follow-up can evaluate adding purge
  origin metadata to `IndexPurgeEntry` or batching block-index/LWC reads.
