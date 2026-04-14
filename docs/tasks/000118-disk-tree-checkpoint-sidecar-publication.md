---
id: 000118
title: DiskTree Checkpoint Sidecar Publication
status: proposal
created: 2026-04-14
github_issue: 554
---

# Task: DiskTree Checkpoint Sidecar Publication

## Summary

Implement RFC 0014 Phase 2 for user-table secondary indexes: publish secondary
`DiskTree` roots as table checkpoint companion work. Data checkpoint will add
checkpointed row secondary entries to persistent `DiskTree` roots, deletion
checkpoint will remove deleted cold-row entries from those roots, and the
updated secondary roots will be published in the same table-file root as the
corresponding LWC, block-index, delete-metadata, and replay-boundary changes.

The current single-tree runtime secondary index remains the only foreground
access path. Recovery continues using the existing persisted-LWC scan until the
RFC 0014 recovery-source phase.

## Context

RFC 0014 defines a staged migration from the current single mutable secondary
index to a user-table dual-tree model. Phase 1 is already implemented:
table-file metadata stores one `secondary_index_roots` entry per index, and
`index::disk_tree` provides unique and non-unique persisted CoW tree readers
and batch writers.

Phase 2 must connect those primitives to table checkpoint publication without
changing runtime lookup behavior. The checkpoint sidecar work must be sourced
from the same table state that checkpoint already persists:

1. Data checkpoint entries come from committed-visible transition rows under
   the same `cutoff_ts` used to build LWC blocks.
2. Deletion checkpoint entries come from committed cold-row delete markers in
   the existing deletion checkpoint replay range.
3. Both flows publish secondary root changes through the same `MutableTableFile`
   commit as table data/delete metadata.

Data sidecar collection is independent of LWC block packing. A transition row
that is visible to the checkpoint contributes one secondary entry set for
`DiskTree` regardless of which LWC block the row lands in. LWC builder overflow
may split persisted blocks, but it must not force duplicate sidecar collection
or per-builder rollback/recompute; if checkpoint later fails, the unpublished
mutable root is abandoned as a whole.

Unique indexes need an additional same-run overlap rule. A single high-level
transaction can delete an old cold owner for key `K`, insert a new hot owner
for `K`, and later have both the new row and the old cold-row delete selected
by one checkpoint run. In that case the data-checkpoint put for `K` wins and
the deletion sidecar for `K` is ignored. The final unique `DiskTree` must map
`K` to the newly checkpointed row rather than deleting the key.

Issue Labels:
- type:task
- priority:high
- codex

Parent RFC:
- docs/rfcs/0014-dual-tree-secondary-index.md

## Goals

1. Add a checkpoint-owned secondary sidecar accumulator grouped by `index_no`
   and index kind.
2. Collect data-checkpoint sidecar entries once from each committed-visible
   transition row under the same `cutoff_ts` used for LWC generation.
3. Keep data sidecar collection independent of LWC builder block split and
   rollback mechanics.
4. Apply unique data-checkpoint entries as latest-owner `DiskTree` puts.
5. Apply non-unique data-checkpoint entries as exact `(logical_key, row_id)`
   inserts.
6. Extend deletion checkpoint's existing block-grouped path to decode each
   affected persisted LWC block once and reconstruct old secondary keys for
   selected deleted row ids.
7. Apply unique deletion companion work as conditional deletes using
   `expected_old_row_id`.
8. Normalize same-run unique overlap so a data-checkpoint put suppresses a
   deletion-checkpoint conditional delete for the same logical key.
9. Apply non-unique deletion companion work as exact `(logical_key, row_id)`
   deletes without logical-key suppression.
10. Publish updated `secondary_index_roots` through the same table-file root as
    LWC, block-index, delete-metadata, `pivot_row_id`,
    `heap_redo_start_ts`, and `deletion_cutoff_ts` changes.
11. Abort the checkpoint without advancing replay boundaries if companion
    `DiskTree` work needed for selected checkpoint state cannot be resolved,
    decoded, or written.

## Non-Goals

1. Do not change foreground secondary-index lookup, insert, update, delete,
   rollback, uniqueness, or purge behavior.
2. Do not introduce the composite user-table `MemTree`/`DiskTree` runtime.
3. Do not switch recovery from persisted-LWC scan to `DiskTree` roots.
4. Do not add `DiskTree` roots, sidecar publication, or composite behavior for
   catalog tables.
5. Do not scan arbitrary committed `MemTree` entries or add an independent
   index checkpoint watermark.
6. Do not add foreground writes to persistent `DiskTree` pages.
7. Do not add storage compatibility with older table-meta payloads.
8. Do not perform generic B+Tree backend unification or a broad checkpoint
   contributor framework refactor.
9. Do not implement `DiskTree` page GC, bulk-build optimization, or parallel
   deletion-key reconstruction in this task.

## Unsafe Considerations (If Applicable)

No new unsafe code is planned. This task should use the existing safe
`DiskTree`, `MutableTableFile`, `ColumnBlockIndex`, and LWC decode APIs.

If implementation unexpectedly touches unsafe B+Tree or block-layout internals,
keep the unsafe boundary private, document every invariant with `// SAFETY:`,
and run:

```bash
cargo clippy -p doradb-storage --all-targets -- -D warnings
```

## Plan

1. Add checkpoint sidecar batch types.
   - Keep the first implementation local to the table checkpoint domain, either
     in `table/persistence.rs` or a narrow helper module if the file becomes
     too large.
   - Represent data work as owned unique puts and non-unique exact inserts.
   - Represent deletion work as owned unique conditional deletes and non-unique
     exact deletes.
   - Normalize each per-index batch into encoded `DiskTree` order before
     calling Phase 1 writer APIs.

2. Add row-to-sidecar collection helpers.
   - Use `TableMetadata::keys_for_insert` or equivalent metadata-driven key
     extraction from full row values.
   - For data checkpoint, collect from committed-visible transition rows under
     the checkpoint `cutoff_ts`.
   - Collect sidecar entries once per transition row, independent of LWC block
     builder append/split behavior.
   - For unique data work, retain the final row id for each logical key in the
     current checkpoint batch.
   - For non-unique data work, retain exact `(logical_key, row_id)` entries.

3. Integrate data sidecar collection into the new-data checkpoint path.
   - Extend the current transition-page scan in `Table::build_lwc_blocks` or
     split the scan into a helper that produces both LWC payloads and sidecar
     entries.
   - Keep the LWC output contract unchanged for `MutableTableFile::apply_lwc_blocks`.
   - Ensure empty data checkpoint runs do not create secondary root changes.

4. Extend deletion checkpoint grouping.
   - Reuse the existing marker selection range:
     `previous_deletion_cutoff_ts <= cts < cutoff_ts` and `row_id < pivot_row_id`.
   - Reuse `ColumnBlockIndex::locate_block`, `load_entry_deletion_deltas`, and
     `load_entry_row_ids` to resolve selected row ids to persisted LWC entries.
   - For each affected persisted LWC block, load and decode the block once.
   - Map selected row ids to row indexes using the authoritative row-id set for
     the entry.
   - Decode full row values or the required indexed columns, reconstruct
     secondary keys, and append per-index delete work.

5. Preserve deletion metadata correctness.
   - If a selected delete marker cannot be located in the persisted column
     index, keep the existing fail-closed behavior.
   - If the row-id set, LWC row count, row shape, or row decode is invalid,
     return an error and leave the active table root unchanged.
   - Do not advance `deletion_cutoff_ts` until both delete metadata and
     companion sidecar roots are ready to publish.

6. Apply sidecar batches before table-file commit.
   - Open one `UniqueDiskTree` or `NonUniqueDiskTree` view per affected index
     from the mutable root's current `secondary_index_roots[index_no]`.
   - Apply data and deletion work through one writer per index so the final
     root reflects the combined checkpoint run.
   - For unique indexes, suppress deletion conditional deletes for logical keys
     that also have data-checkpoint puts in the same sidecar accumulator.
   - For unique indexes, apply data puts before remaining conditional deletes.
   - For non-unique indexes, apply exact inserts and exact deletes without
     logical-key-level suppression.
   - Store returned roots with `MutableTableFile::set_secondary_index_root`.

7. Keep publication atomic.
   - Continue using one `MutableTableFile` fork and one final
     `MutableTableFile::commit`.
   - On sidecar application failure, roll back the checkpoint transaction and
     drop the mutable table-file fork.
   - Do not publish partial secondary roots or replay-boundary changes.

8. Remove or narrow unused-code scaffolding where feasible.
   - Phase 1 left `index::disk_tree` mostly unused.
   - After integration, remove `#![allow(dead_code)]` if the remaining unused
     items can be deleted or moved under tests without distorting the
     implementation.

9. Validate with formatting, lint, and tests.
   - Run:

```bash
cargo fmt --all
cargo clippy -p doradb-storage --all-targets -- -D warnings
cargo nextest run -p doradb-storage
```

   - Run the alternate backend pass only if implementation changes
     backend-neutral I/O paths beyond using existing table-file and DiskTree
     write APIs:

```bash
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

## Implementation Notes

## Impacts

- `doradb-storage/src/table/persistence.rs`
  - checkpoint orchestration
  - data/deletion sidecar accumulator ownership
  - deletion checkpoint grouping and companion root application
- `doradb-storage/src/table/mod.rs`
  - transition-page and LWC build helpers
  - possible extraction of shared row-to-index-key collection helpers
- `doradb-storage/src/index/disk_tree.rs`
  - unique and non-unique Phase 1 batch writers become production-used
  - possible cleanup of dead-code allowance
- `doradb-storage/src/file/table_file.rs`
  - existing secondary root accessors/setters are used by checkpoint
  - no new table-file format change is expected
- `doradb-storage/src/index/column_block_index.rs`
  - existing locate, row-id load, and delete-delta load APIs are reused by
    deletion companion reconstruction
- `doradb-storage/src/lwc/block.rs`
  - existing persisted LWC decode APIs are reused for deletion companion key
    reconstruction
- `doradb-storage/src/table/tests.rs`
  - checkpoint, deletion checkpoint, unique overlap, and non-unique exact-entry
    coverage
- `docs/rfcs/0014-dual-tree-secondary-index.md`
  - parent RFC phase should be synchronized during `task resolve`

## Test Cases

1. Data checkpoint with a unique secondary index publishes a non-empty unique
   `DiskTree` root, and `UniqueDiskTree::lookup` returns the checkpointed owner
   row ids for rows moved to LWC.
2. Data checkpoint with a non-unique secondary index publishes exact entries,
   and `NonUniqueDiskTree::prefix_scan` returns the checkpointed row ids in
   exact-key order.
3. Data sidecar collection across an LWC builder split does not duplicate
   secondary entries and still publishes all visible transition rows.
4. A checkpoint with no secondary indexes keeps `secondary_index_roots` empty
   and preserves existing data/deletion behavior.
5. A checkpoint with secondary indexes but no eligible data or deletion work
   does not rewrite secondary roots.
6. Deletion checkpoint with one unique cold-row delete removes the durable
   logical-key mapping when the stored owner matches the deleted row id.
7. Unique deletion checkpoint skips the conditional delete when the stored
   owner no longer matches the deleted old row id.
8. Same-run unique overlap publishes the data-checkpoint owner for key `K`
   when one checkpoint contains both a new checkpointed row for `K` and a cold
   delete for the old owner of `K`.
9. Non-unique deletion checkpoint removes only the exact
   `(logical_key, old_row_id)` entry and does not suppress a same-key new row
   with a different row id.
10. Multiple selected deletes in one persisted LWC block reconstruct all
    affected secondary keys from one block-grouped decode path. Use a narrow
    test hook or structural assertion if a decode counter is not practical.
11. Delete bitmap and secondary `DiskTree` roots publish atomically: injected
    companion sidecar failure leaves the previous active root and
    `deletion_cutoff_ts` unchanged.
12. Existing runtime unique and non-unique lookup tests continue to use the
    single-tree runtime path and remain behaviorally unchanged.
13. Recovery remains on the existing persisted-LWC scan path after Phase 2, even
    when checkpointed secondary roots are present.
14. Corrupted LWC or row-id metadata needed for deletion companion key
    reconstruction surfaces an error and does not advance deletion checkpoint
    state.

## Open Questions

1. Should the sidecar accumulator stay in `table/persistence.rs` for locality,
   or move to a small `table/checkpoint_index.rs` module once implementation
   size is clear?
2. Should the "decode each affected LWC block once" requirement be proven with
   a test-only counter hook, or is structural grouping coverage sufficient for
   this phase?
