---
id: 000154
title: Table Root Reachability Block Reclamation
status: proposal
created: 2026-05-22
github_issue: 648
---

# Task: Table Root Reachability Block Reclamation

## Summary

Implement checkpoint-integrated root-reachability block reclamation for user
table files.

The implementation should add a runtime-only table-root effective timestamp that is
assigned after each table-file root publication. Checkpoint readiness for
reclamation must use that post-publish effective timestamp, not the checkpoint
transaction start timestamp currently stored in `ActiveRoot::trx_id`. Once the
effective timestamp is older than `Global_Min_Active_STS`, no active transaction can
still be reading a root that was replaced before that publication. At that
point, an optional checkpoint reclamation step can rebuild the mutable root's
allocation map from only two protected roots:

1. the current active table-file root
2. the mutable root about to be published by the current checkpoint

This keeps reclamation inside the existing table checkpoint flow, avoids a new
foreground command or table-level contention point, and reclaims dropped
secondary-index `DiskTree` pages through the same table-file reachability proof
as ordinary CoW replacement blocks.

## Context

Table files are copy-on-write. A checkpoint writes new blocks, writes a new
meta block, and atomically swaps one A/B super-block slot to point at the new
root. The active root stores the allocation map used by later CoW writers.
Today obsolete user-table blocks are not reclaimed through a forward
reachability mechanism. This task removes the old `gc_block_list` metadata and
uses root reachability as the block-reclamation source of truth.

Issue Labels:
- type:task
- priority:high
- codex

Source Backlogs:
- docs/backlogs/000094-table-file-root-reachability-gc.md

Related Design:
- docs/architecture.md
- docs/transaction-system.md
- docs/index-design.md
- docs/checkpoint-and-recovery.md
- docs/table-file.md
- docs/garbage-collect.md
- docs/tasks/000125-checkpoint-root-liveness-gate.md
- docs/tasks/000150-implement-drop-index-storage-api.md
- docs/rfcs/0018-create-drop-index.md
- docs/process/unit-test.md
- docs/process/coding-guidance.md

Task 000125 added the checkpoint root-liveness gate using
`active_root.trx_id < Global_Min_Active_STS`. That predicate is sufficient for
the original A/B slot-overwrite problem when `trx_id` is the checkpoint root's
publication boundary. Current checkpoint code, however, sets `trx_id` from the
checkpoint transaction start timestamp before doing LWC, deletion, and
secondary-index sidecar work. A foreground transaction can start after that
timestamp but before the root pointer swap, read the old root, and continue
using the retained in-memory `OldRoot`.

For block reclamation, the useful proof is not "the checkpoint transaction
started before the active snapshot horizon." The useful proof is "no active
transaction can still have observed any pre-publication root." Therefore this
task introduces a runtime-only effective timestamp allocated after the root pointer
swap. That value is not persisted. On recovery or table-file load, initialize
the runtime effective timestamp from the selected root's `trx_id`, because no active
transactions or retained pre-crash roots exist.

Task 000150 deliberately did not reclaim dropped secondary-index `DiskTree`
pages inside `DROP INDEX`. A dropped index detaches persistent pages by
publishing a table root whose sparse secondary-root slot is `SUPER_BLOCK_ID`.
This task should reclaim those pages only when ordinary table-file
root-reachability proves they are unreachable.

Rejected alternatives:
- Do not add a `DROP INDEX`-specific page cleanup path.
- Do not add a separate user command for table-file vacuum/reclaim in this task.
- Do not persist a new root effective timestamp in `MetaBlock`.
- Do not implement recovery-time unreachable-block cleanup in this task; track
  it as a follow-up backlog.

## Goals

1. Add a runtime-only effective timestamp to table-file active roots or equivalent
   table-file runtime state.
2. Initialize the effective timestamp from the selected root `trx_id` when a table
   file is loaded during startup/recovery.
3. Ensure every runtime table-root publication installs a post-publish safe
   timestamp after the active-root pointer swap.
4. Change checkpoint readiness/reclamation gating so it uses the root safe
   timestamp rather than the checkpoint transaction start timestamp.
5. Preserve the current A/B root publication and old-root retention semantics.
6. Add a checkpoint-integrated, optional reachability reclamation step before
   publishing a checkpoint root.
7. Compute reachable user-table blocks from the current active root and the
   current mutable checkpoint root once the effective timestamp gate passes.
8. Rebuild the mutable root allocation map from the reachable block set and the
   table file's reserved super block.
9. Cover all user-table persisted structures reachable from table roots:
   - table meta blocks
   - `ColumnBlockIndex` nodes
   - LWC data blocks referenced by column-block entries
   - external column-deletion blob pages
   - active secondary-index `DiskTree` nodes
10. Reclaim dropped secondary-index `DiskTree` pages only through the ordinary
    reachability pass after the dropped root is no longer reachable.
11. Remove `gc_block_list` from runtime roots and from the persisted meta-block
    payload.
12. Treat skipped or unnecessary reclamation as a normal checkpoint outcome, not
    storage poison or checkpoint failure.
13. Add focused regression coverage for safe-timestamp gating, root
    reachability, block reuse, dropped-index roots, column-block nodes, LWC
    blocks, deletion blobs, and restart behavior without recovery-time rebuild.

## Non-Goals

1. Do not add a new SQL/API command for table-file vacuum, compaction, or
   reclamation.
2. Do not reclaim dropped secondary-index pages directly in `DROP INDEX`.
3. Do not change the table-file A/B super-block format.
4. Do not persist the root effective timestamp.
5. Do not implement recovery-time alloc-map rebuild or startup unreachable-block
   cleanup in this task.
6. Do not implement full file truncation or sparse-file hole punching.
7. Do not add a general background reclaimer pipeline.
8. Do not redesign `DiskTree`, `ColumnBlockIndex`, or column-deletion blob page
   formats.
10. Do not change foreground read/write MVCC semantics.
11. Do not broaden this task into DiskTree compaction or ColumnBlockIndex
    compaction policy.
12. Do not change catalog `MultiTableFile` reclamation semantics except for
    narrowly shared helper APIs that remain correct for catalog files.

## Unsafe Considerations (If Applicable)

No unsafe code changes are expected. The intended work should stay in safe
Rust around table-file root metadata, allocation maps, checkpoint orchestration,
and validated persisted-block traversal.

If the implementation touches unsafe pointer or page-cast code, document every
new or changed unsafe block with a concrete `// SAFETY:` invariant and run:

```bash
cargo clippy -p doradb-storage --all-targets -- -D warnings
```

Also apply `docs/process/unsafe-review-checklist.md` before resolving this task.

## Plan

1. Add runtime root effective timestamp support.
   - Extend `doradb-storage/src/file/cow_file.rs` active-root runtime state with
     a non-persisted effective timestamp, or add an equivalent table-file runtime
     sidecar.
   - The value must be readable by checkpoint readiness without taking a long
     lock.
   - New unpublished roots should not accidentally pass the reclamation gate
     before publication installs the post-publish value. Use a blocked value
     such as `MAX_SNAPSHOT_TS` for unpublished roots if the timestamp lives on
     `ActiveRoot`.
   - On root load from disk, initialize effective timestamp to the loaded root's
     `trx_id`.

2. Install effective timestamps after root publication.
   - Add a helper on `TransactionSystem` or table-file publication code that:
     1. runs after `MutableTableFile::commit` swaps the active root pointer;
     2. allocates a timestamp/fence from the global timestamp source;
     3. stores that timestamp into the current active root's runtime safe
        timestamp;
     4. retains the returned `OldRoot` with the same fence when an old root
        exists.
   - Update table-root publish call sites:
     - user-table checkpoint in `doradb-storage/src/table/persistence.rs`
     - create-index and drop-index table-root publication in
       `doradb-storage/src/catalog/index.rs`
     - create-table initial table-file publication in
       `doradb-storage/src/session.rs`
     - test helpers that publish mutable table files directly, where needed
   - Preserve existing old-root release behavior in `trx/purge.rs`.

3. Change checkpoint readiness to use effective timestamp.
   - Update `CheckpointReadiness::for_root` or its caller so delay is based on
     `active_root.effective_ts < trx_sys.calc_min_active_sts_for_gc()`.
   - Keep root `trx_id` in diagnostics as the durable checkpoint/publication id,
     but use `effective_ts` as the reclamation/root-observation boundary.
   - Update tests that currently expect root-liveness behavior from `trx_id`.

4. Add a reachable-block collector.
   - Introduce a small type such as `TableReachableBlocks` or
     `RootReachabilityCollector` under `doradb-storage/src/file/table_file.rs`,
     `doradb-storage/src/table/persistence.rs`, or a narrow new module if it
     keeps file/index traversal boundaries cleaner.
   - The collector should use validated existing read paths, not raw unchecked
     block parsing.
   - It should reject invalid reachable block ids outside the table file's
     allocation-map range.
   - It should deduplicate ids before rebuilding the allocation map.

5. Traverse table roots.
   - For each protected root, mark:
     - `SUPER_BLOCK_ID`
     - the root's `meta_block_id`
     - the root's `column_block_index_root` when not `SUPER_BLOCK_ID`
     - each active secondary `DiskTree` root when not `SUPER_BLOCK_ID`
   - Do not mark inactive secondary slots whose root is `SUPER_BLOCK_ID`.
   - Do not derive reachability from any obsolete-block side list.

6. Traverse `ColumnBlockIndex` reachability.
   - Add a method on `ColumnBlockIndex`, for example
     `collect_reachable_blocks`, that traverses branch and leaf nodes from a
     captured root.
   - Mark every visited ColumnBlockIndex node page.
   - Decode leaf entries and mark each referenced LWC data block.
   - For leaf entries with external deletion payloads, mark every page in the
     referenced column-deletion blob chain.
   - Keep traversal validation consistent with existing `collect_leaf_entries`,
     `load_delete_deltas`, and blob-reader validation behavior.

7. Traverse secondary `DiskTree` reachability.
   - Add a method on `DiskTree` or `DiskTreeRuntime` that traverses a captured
     root and collects every branch/leaf node block id.
   - Reuse existing validated `read_node` traversal logic.
   - Empty roots (`SUPER_BLOCK_ID`) collect no DiskTree nodes.
   - Keep unique and non-unique traversal shape shared through existing
     `DiskTreeSpec` where practical.

8. Rebuild the mutable root allocation map.
   - Add an `AllocMap` construction helper or a `MutableTableFile` method that
     replaces the mutable root's allocation map with a map built from the
     reachable set.
   - Always allocate `SUPER_BLOCK_ID`.
   - Ensure all blocks allocated by current checkpoint writes are included via
     the mutable root traversal.
   - Run this after LWC, deletion checkpoint, and secondary-index sidecar work,
     but before `MutableTableFile::commit` allocates and writes the new meta
     block.
   - Do not serialize an obsolete-block side list. Bump the table and catalog
     meta-block payload versions because older payloads are intentionally not
     compatible.

9. Integrate into table checkpoint.
   - Add a local checkpoint option/policy for the reclamation step. The initial
     policy can always attempt reclamation when the effective timestamp gate is
     ready, or can use a simple threshold based on allocated/reachable delta.
   - Reclamation must happen under the existing checkpoint root-mutation lease
     and before the no-cancel publish section.
   - If the table is not ready by effective timestamp, checkpoint should return the
     existing delayed outcome before mutation, as it does today.
   - If reachability finds no reclaimable blocks, publish proceeds normally.
   - Any IO or data-integrity error during traversal should abort the checkpoint
     before publication and follow existing checkpoint rollback/error handling.

10. Keep recovery behavior unchanged for this task.
    - Loading a table file should initialize effective timestamp from the loaded
      root's `trx_id`.
    - Recovery should not rewrite the allocation map in this task.
    - Add a follow-up backlog for recovery-time alloc-map rebuild and
      unreachable-block cleanup.

11. Update docs and comments.
    - Update `docs/table-file.md` to describe runtime effective timestamp and
      checkpoint-integrated reachability reclamation.
    - Update `docs/checkpoint-and-recovery.md` to clarify that checkpoint root
      readiness uses a post-publish root-observation boundary for reclamation.
    - Update `docs/garbage-collect.md` to keep root reachability as the
      table-file/DiskTree/LWC/deletion-blob reclaim proof.
    - Update comments in `cow_file.rs`, `table_file.rs`, and `trx/purge.rs`
      where the old-root fence and allocation-map rebuild roles are described.

## Implementation Notes


## Impacts

- `doradb-storage/src/file/cow_file.rs`
  - Runtime-only effective timestamp on active roots or an equivalent root sidecar.
  - Root clone/flip/load behavior for the effective timestamp.
- `doradb-storage/src/file/table_file.rs`
  - Mutable allocation-map rebuild API.
  - Table-root reachable-block collection entry points.
  - Table-file publication call-site tests.
- `doradb-storage/src/table/persistence.rs`
  - Checkpoint readiness gate.
  - Checkpoint integration of the reachability reclamation step.
  - Checkpoint tests and helper expectations.
- `doradb-storage/src/trx/purge.rs`
  - Old-root retention helper should share the post-publish fence used as root
    effective timestamp.
- `doradb-storage/src/trx/sys.rs`
  - Helper API for marking table-root publication safe.
- `doradb-storage/src/catalog/index.rs`
  - Create/drop index root publications must install effective timestamps.
- `doradb-storage/src/session.rs`
  - Create-table initial root publication must install effective timestamp.
- `doradb-storage/src/index/column_block_index.rs`
  - Reachability traversal over column-index nodes, LWC block references, and
    external deletion blobs.
- `doradb-storage/src/index/column_deletion_blob.rs`
  - Blob-chain reachability helper or reader support.
- `doradb-storage/src/index/disk_tree.rs`
  - Reachability traversal over DiskTree nodes for unique and non-unique roots.
- `doradb-storage/src/bitmap.rs`
  - Possible helper for building an `AllocMap` from explicit allocated indexes.
- `docs/table-file.md`
- `docs/checkpoint-and-recovery.md`
- `docs/garbage-collect.md`

## Test Cases

1. Safe timestamp blocks premature reclamation.
   - Start a transaction after checkpoint start but before root publication.
   - Let that transaction capture the pre-publication root.
   - Publish the checkpoint.
   - Verify a later checkpoint is delayed by the root effective timestamp until the
     transaction ends, even if `root.trx_id < min_active_sts` would have passed
     under the old start-timestamp rule.

2. Safe timestamp advances after publication.
   - Publish a table root.
   - Verify the active root's effective timestamp is assigned after publication and
     old-root retention uses the same fence.

3. Reclamation preserves active and mutable roots.
   - Create a table with checkpointed data.
   - Run another checkpoint with reclamation enabled.
   - Verify all blocks reachable from active and mutable roots remain allocated.

4. Reclaimed blocks are reused.
   - Create obsolete CoW blocks through a ColumnBlockIndex rewrite or
     secondary DiskTree rewrite.
   - Run checkpoint reclamation after the effective timestamp crosses the GC horizon.
   - Verify later CoW allocation can reuse reclaimed block ids and reads still
     validate.

5. Dropped secondary-index DiskTree pages reclaim through checkpoint.
   - Create a secondary index with a non-empty DiskTree.
   - Drop the index and verify the active root slot is `SUPER_BLOCK_ID`.
   - After the effective timestamp crosses the horizon, run checkpoint reclamation.
   - Verify the dropped DiskTree node blocks become free/reusable only through
     reachability.

6. Active secondary-index roots remain protected.
   - Keep at least one active secondary root while another index is dropped.
   - Run reclamation and verify only the dropped root's unreachable blocks are
     freed.

7. ColumnBlockIndex and LWC reachability.
   - Checkpoint rows into LWC blocks.
   - Run reclamation and verify ColumnBlockIndex nodes and referenced LWC data
     blocks remain allocated and readable.

8. External deletion blob reachability.
   - Build a cold-delete payload large enough to spill into external
     column-deletion blob pages.
   - Run reclamation and verify all blob pages referenced from reachable
     ColumnBlockIndex leaves remain allocated and readable.

9. Meta-block format without `gc_block_list`.
   - Persist and reload a table after reclamation.
   - Verify metadata serialization contains only the allocation map for file
     space state.
   - Verify table and catalog meta-block version mismatch tests cover the
     format bump.

10. Restart without recovery rebuild.
    - Reclaim blocks through checkpoint, publish, restart, and verify the table
      loads from the persisted allocation map and all checkpointed data/index
      reads remain valid.

11. Failure before publish.
    - Inject a traversal/read/write failure before table-root publication.
    - Verify checkpoint aborts before root swap and existing state remains
      readable.

Routine validation:

```bash
cargo nextest run -p doradb-storage
```

If implementation touches IO backend-neutral paths, also run:

```bash
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

Focused coverage should include changed Rust files or directories, for example:

```bash
tools/coverage_focus.rs --path doradb-storage/src/file/table_file.rs
tools/coverage_focus.rs --path doradb-storage/src/table/persistence.rs
tools/coverage_focus.rs --path doradb-storage/src/index
```

## Open Questions

1. Recovery-time unreachable-block cleanup is intentionally deferred. Future
   work should rebuild or validate the allocation map from the selected latest
   valid table root during recovery, when no active transactions or retained
   roots exist.
2. A future task may add reclamation thresholds or scheduling policy if always
   attempting the optional checkpoint step proves too expensive.
3. Full file truncation or sparse-file hole punching remains separate from
   allocation-map reclamation.
