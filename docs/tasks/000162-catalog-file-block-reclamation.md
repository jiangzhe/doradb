---
id: 000162
title: Catalog File Block Reclamation
status: proposal
created: 2026-05-29
github_issue: 668
---

# Task: Catalog File Block Reclamation

## Summary

Implement checkpoint-integrated block reclamation for the catalog multi-table
file, `catalog.mtb`.

Catalog checkpoint currently publishes new CoW roots through
`MutableMultiTableFile::commit`, but it does not rebuild the persisted
allocation map from catalog-root reachability. As a result, repeated catalog
checkpoints can leave obsolete catalog meta blocks, catalog logical-table
`ColumnBlockIndex` nodes, LWC blocks, and external delete-blob blocks marked
allocated after they are no longer reachable.

This task should add a catalog-specific reachability pass that traces only the
to-be-committed mutable catalog root, rebuilds that root's allocation map before
publication, and reuses the root-reachability helpers introduced by task 000154
where they fit. Unlike user-table reclamation, catalog reclamation must not
trace both the current active root and the mutable root: catalog runtime state
is cache-first, catalog checkpoint publication is serialized, and the old
`catalog.mtb` root is not a protected reader root after the new checkpoint root
commits.

As a small naming follow-up from task 000161, rename the active
`next_user_obj_id` code/documentation surface to `next_table_id`. This is a
semantic rename only; it must not change the persisted `catalog.mtb` bytes.

## Context

Issue Labels:
- type:task
- priority:high
- codex

Source Backlogs:
- docs/backlogs/000106-catalog-file-block-reclamation.md

Related Design:
- docs/architecture.md
- docs/transaction-system.md
- docs/index-design.md
- docs/checkpoint-and-recovery.md
- docs/table-file.md
- docs/garbage-collect.md
- docs/tasks/000154-table-root-reachability-block-reclamation.md
- docs/tasks/000161-newtype-storage-ids.md
- docs/process/unit-test.md
- docs/process/coding-guidance.md

Task 000154 implemented user-table root-reachability block reclamation. It
added the shared `MutableCowRoot::rebuild_alloc_map_from_reachable` helper and
reachability collectors for `ColumnBlockIndex`, external column-deletion blob
chains, and secondary `DiskTree` roots. User-table checkpoint uses a runtime
root `effective_ts` gate and traces two protected roots: the current active
table root plus the mutable root that will be published. That two-root proof is
necessary because foreground user-table readers can hold table-file root
snapshots.

Catalog persistence is different. Foreground catalog reads and DDL/DML operate
on in-memory catalog tables. `catalog.mtb` is the durable checkpoint boundary
for catalog logical-table rows plus overlay metadata such as the table-id
allocator watermark and `catalog_replay_start_ts`. Catalog checkpoint already
has a gate and a single mutable writer against the shared `MultiTableFile`.
Recovery/bootstrap and checkpoint are the boundaries that decode catalog-file
roots. Therefore catalog reclamation should use only the new root that is about
to become durable, not the displaced active root.

One implementation detail matters for correctness and for reclaiming old meta
blocks promptly. Today `CowFile::publish_root` allocates the new meta block
inside publish, after caller-side mutation is complete. If a catalog
reachability rebuild runs before that final meta block id is assigned,
`MutableCowRoot::rebuild_alloc_map_from_reachable` will keep the inherited
active meta block by accident. The catalog path must arrange publication so the
final new meta block id is assigned before rebuilding the allocation map that
will be serialized into that same meta block. The old active meta block must
not be overwritten before the super-block swap, but it should be absent from
the newly persisted allocation map once the new root is committed.

Task 000161 removed `ObjID` and made `TableID` the typed user-table identity,
but active names such as `next_user_obj_id` still describe the allocator as an
object-id allocator. In this task, update active code, tests, comments, error
messages, and current concept docs to call that watermark `next_table_id`.
Historical task/RFC text may remain unchanged unless directly edited for this
task.

## Goals

1. Add catalog checkpoint allocation-map rebuild for `catalog.mtb`.
2. Trace only the to-be-committed `MutableMultiTableFile` root for catalog
   reclamation.
3. Do not trace or otherwise protect the current active catalog root during the
   catalog reachability rebuild.
4. Ensure the final new catalog meta block id is assigned before the allocation
   map serialized into that meta block is rebuilt.
5. Reuse existing task 000154 helpers where practical, especially
   `MutableCowRoot::rebuild_alloc_map_from_reachable` and
   `ColumnBlockIndex::collect_reachable_blocks`.
6. Cover all blocks reachable from the committed catalog root:
   - `SUPER_BLOCK_ID`;
   - the final catalog meta block;
   - every non-empty catalog logical-table `ColumnBlockIndex` root;
   - all reachable catalog logical-table `ColumnBlockIndex` nodes;
   - all LWC blocks referenced by reachable catalog logical-table leaves;
   - all external column-deletion blob pages referenced by reachable leaves.
7. Validate every reachable block id against the mutable root allocation map
   before publishing the new root.
8. Rebuild the mutable root allocation map after all catalog CoW table changes
   and checkpoint metadata changes are represented in the mutable root, and
   before the root is serialized and published.
9. Wire the rebuild into both catalog checkpoint paths:
   - metadata-only heartbeat/no-op checkpoint;
   - checkpoint batches with catalog logical-table row changes.
10. Preserve catalog checkpoint replay-boundary semantics,
    `catalog_replay_start_ts` advancement, and `next_table_id` persistence.
11. Rename active `next_user_obj_id` symbols to `next_table_id` across active
    code, tests, comments, diagnostics, and current concept docs.
12. Keep the persisted `catalog.mtb` layout byte-compatible. The allocator
    watermark remains the same serialized `u64`; only names change.
13. Add focused regression coverage for meta-block reclamation, logical-table
    root rewrites, LWC blocks, external delete blobs, restart readability, and
    the allocator rename.

## Non-Goals

1. Do not change user-table checkpoint reclamation behavior.
2. Do not add a catalog two-root retention model.
3. Do not add a catalog-root `effective_ts` gate unless implementation
   research finds a real concurrent catalog-file reader that must be protected;
   if that happens, stop and re-plan rather than silently broadening scope.
4. Do not implement recovery-time allocation-map rebuild for user-table files
   or `catalog.mtb`.
5. Do not implement the broader trigger, threshold, budget, backoff, or
   observability policy for root-reachability reclamation.
6. Do not implement file truncation, sparse-file hole punching, or a background
   vacuum command.
7. Do not redesign `CowFile`, `ColumnBlockIndex`, LWC blocks, or
   column-deletion blob formats beyond the minimal publication hook needed for
   catalog one-root reclamation.
8. Do not change catalog checkpoint scan/filter rules for table/index DDL.
9. Do not change redo formats, catalog logical-table row formats, or
   `catalog.mtb` meta-block binary field order.
10. Do not update historical task/RFC references to `next_user_obj_id` unless a
    directly active design document is already being updated for this task.
11. Do not rename unrelated table-id range concepts unless they are necessary
    to complete the requested `next_table_id` rename safely.

## Unsafe Considerations (If Applicable)

No unsafe code changes are expected.

The intended work should stay in safe Rust around catalog checkpoint
orchestration, CoW root publication, allocation maps, and validated persisted
block traversal. If the implementation touches unsafe page-cast, direct-buffer,
or pointer code, document each new or changed unsafe block with a concrete
`// SAFETY:` invariant and run:

```bash
cargo clippy -p doradb-storage --all-targets -- -D warnings
```

Also apply `docs/process/unsafe-review-checklist.md` before resolving this
task.

## Plan

1. Confirm the catalog one-root safety boundary in code.
   - Check catalog checkpoint, recovery/bootstrap, purge snapshot reads, and
     catalog storage snapshot helpers for any path that decodes blocks from an
     old active `catalog.mtb` root concurrently with checkpoint publication.
   - Keep existing cache-first catalog runtime behavior unchanged.
   - If a real overlapping catalog-file reader is found, pause and revise the
     design rather than adding active-root tracing by default.

2. Add a publication path that can rebuild the serialized allocation map after
   the final meta block id is known.
   - Refactor `CowFile::publish_root` narrowly, or add a catalog-specific
     wrapper, so a mutable root can:
     1. reserve the final new meta block without reusing the current active
        meta block before the super-block swap;
     2. set `new_root.root.meta_block_id` to that final block id;
     3. rebuild the mutable allocation map from catalog reachability;
     4. serialize the meta block with the rebuilt allocation map;
     5. write the super block and swap the active root as today.
   - Preserve existing rollback semantics for unpublished blocks on errors
     before publish.
   - Keep user-table publication behavior unchanged unless a shared helper can
     be extracted without altering the user-table two-root proof.

3. Add `MutableMultiTableFile` allocation-map rebuild support.
   - Add a method equivalent to `MutableTableFile::rebuild_alloc_map_from_reachable`.
   - Expose a narrow immutable root accessor for the mutable catalog root if
     the reachability collector lives outside `multi_table_file.rs`.
   - Keep visibility `pub(crate)` or narrower.

4. Implement catalog mutable-root reachability collection.
   - Prefer keeping the collector in `doradb-storage/src/catalog/storage/checkpoint.rs`
     unless moving a small validation helper to
     `doradb-storage/src/file/multi_table_file.rs` materially reduces
     coupling.
   - Start with a `BTreeSet<BlockID>`.
   - Insert `SUPER_BLOCK_ID`.
   - Insert the mutable root's final `meta_block_id`.
   - For each catalog logical-table root descriptor:
     - accept `root_block_id = None` only when `pivot_row_id == RowID::new(0)`;
     - when `root_block_id` is present, construct `ColumnBlockIndex` with
       `FileKind::CatalogMultiTableFile`, `self.mtb.sparse_file()`,
       `self.disk_pool`, and the descriptor's `pivot_row_id`;
     - call `ColumnBlockIndex::collect_reachable_blocks` to include index
       nodes, LWC blocks, and external delete blobs.
   - Do not collect secondary-index `DiskTree` roots; catalog logical tables in
     `catalog.mtb` do not persist secondary `DiskTree` roots.

5. Validate catalog reachable blocks before rebuilding.
   - Reject any collected block id outside the mutable root allocation-map
     range.
   - Reject any collected block whose allocation bit is not set in the mutable
     root after all CoW writes and final meta-block reservation are complete.
   - Use `DataIntegrityError::InvalidRootInvariant` or the closest existing
     catalog/file invariant error style, with messages that identify
     `catalog.mtb`, `root_ts`, and the offending block id.

6. Integrate rebuild into catalog checkpoint.
   - In the metadata-only path, apply checkpoint metadata, reserve/prepare the
     final meta block, collect reachability from the mutable root only, rebuild
     the allocation map, and publish.
   - In the catalog-op path, finish every `apply_table_ops` CoW change, apply
     checkpoint metadata, reserve/prepare the final meta block, collect
     reachability from the mutable root only, rebuild, and publish.
   - Drop the returned old root as today. It is not part of the catalog
     reclamation protected set.
   - Treat reachability IO/data-integrity errors as checkpoint failures before
     publication; do not publish a partially rebuilt root.

7. Rename the table-id allocator surface.
   - Rename active code fields and methods such as:
     - `MultiTableMetaBlock::next_user_obj_id` to `next_table_id`;
     - `CatalogStorage::next_user_obj_id` to `next_table_id`;
     - `Catalog::next_user_obj_id` to `next_table_id`;
     - `Catalog::curr_next_user_obj_id` to `curr_next_table_id`;
     - `try_update_next_user_obj_id` to `try_update_next_table_id`;
     - checkpoint parameters like `next_user_obj_id` to `next_table_id`;
     - file cleanup parameters like `checkpointed_next_user_obj_id` to
       `checkpointed_next_table_id`.
   - Update tests, helper names, assertions, comments, and error messages.
   - Update active design docs that describe current catalog metadata:
     `docs/architecture.md`, `docs/table-file.md`, and
     `docs/checkpoint-and-recovery.md`.
   - Keep the serialized payload as the same `u64` in the same position.
   - Avoid a meta-block version bump unless another required change makes the
     persisted format intentionally incompatible.

8. Update docs and comments for catalog reclamation.
   - Document that user-table reclamation traces active plus mutable roots,
     while catalog reclamation traces only the to-be-committed catalog root.
   - Clarify that `catalog.mtb` allocation-map rebuild reclaims obsolete
     catalog meta/index/LWC/delete-blob blocks at checkpoint publication time.
   - Keep recovery-time allocation-map rebuild and trigger/budget policy
     references as follow-ups, not part of this task.

9. Validate implementation.
   - Run formatting:
     ```bash
     cargo fmt --all
     ```
   - Run routine validation:
     ```bash
     cargo nextest run -p doradb-storage
     ```
   - Run lint validation:
     ```bash
     cargo clippy -p doradb-storage --all-targets -- -D warnings
     ```
   - If the implementation changes generic CoW publication or backend-neutral
     IO behavior, also run:
     ```bash
     cargo nextest run -p doradb-storage --no-default-features --features libaio
     ```
   - Run focused coverage for changed paths, for example:
     ```bash
     tools/coverage_focus.rs \
       --path doradb-storage/src/file/multi_table_file.rs \
       --path doradb-storage/src/catalog/storage/checkpoint.rs \
       --path doradb-storage/src/file/cow_file.rs
     ```

## Implementation Notes


## Impacts

- `doradb-storage/src/file/cow_file.rs`
  - Possible narrow publish-flow helper that allows final meta-block
    reservation before caller-side allocation-map rebuild and serialization.
- `doradb-storage/src/file/multi_table_file.rs`
  - Mutable catalog root accessor/rebuild helper.
  - `next_user_obj_id` field and related metadata names become
    `next_table_id`.
  - Tests for meta-block allocation-map rebuild and byte-compatible metadata
    serialization.
- `doradb-storage/src/catalog/storage/checkpoint.rs`
  - Catalog mutable-root reachability collector.
  - Checkpoint integration in both metadata-only and catalog-op paths.
  - Checkpoint parameter rename from `next_user_obj_id` to `next_table_id`.
- `doradb-storage/src/catalog/mod.rs`
  - Catalog allocator field and methods rename to `next_table_id`.
  - Current and recovery update helpers rename.
- `doradb-storage/src/catalog/table.rs`
  - `CREATE TABLE` table-id allocation call site rename.
- `doradb-storage/src/file/meta_block.rs`
  - Multi-table meta-block serialization view/data names and diagnostics
    rename while preserving serialized bytes.
- `doradb-storage/src/file/fs.rs`
  - Cleanup parameter and diagnostics rename from checkpointed
    `next_user_obj_id` to checkpointed `next_table_id`.
- `doradb-storage/src/trx/recover.rs`
  - Recovery load/allocator restoration call sites rename.
- Tests in catalog, file, recovery, and table modules that refer to
  `next_user_obj_id`.
- Current concept docs:
  - `docs/architecture.md`
  - `docs/table-file.md`
  - `docs/checkpoint-and-recovery.md`

## Test Cases

1. Catalog metadata-only checkpoint reclaims the displaced meta block.
   - Create/open `catalog.mtb`.
   - Publish a metadata-only checkpoint.
   - Verify the new active root allocation map does not keep the previous
     active meta block allocated solely because it was inherited by the mutable
     root before publish.
   - Verify the new final meta block remains allocated and readable.

2. Repeated metadata-only catalog checkpoints do not monotonically grow the
   allocated meta-block count.
   - Run several heartbeat/no-op catalog checkpoints.
   - Verify old catalog meta blocks are freed/reused across publications and
     snapshots still report the expected `catalog_replay_start_ts` and
     `next_table_id`.

3. Catalog logical-table root rewrite preserves reachable blocks and frees
   obsolete blocks.
   - Create catalog DDL changes that materialize catalog logical-table LWC
     blocks and `ColumnBlockIndex` nodes.
   - Run another catalog checkpoint that rewrites at least one logical-table
     root, such as tail merge or delete-delta rewrite.
   - Verify the new root's index nodes and LWC blocks remain allocated and
     readable, while the displaced root/index/LWC blocks are not retained by
     the persisted allocation map.

4. External delete-blob reachability is preserved.
   - Build catalog logical-table delete deltas large enough to spill into
     external column-deletion blob pages.
   - Run catalog checkpoint reclamation.
   - Verify all blob pages referenced by the committed catalog root remain
     allocated and readable.
   - Verify obsolete blob pages from the displaced root become free.

5. Reclamation validates reachable block allocation bits.
   - Use a focused unit test or corruption-style test to make a reachable
     catalog root descriptor point at an unallocated or out-of-range block.
   - Verify checkpoint fails before super-block publication and the previous
     catalog checkpoint remains readable.

6. Restart after catalog reclamation.
   - Create catalog rows, checkpoint with reclamation, drop the engine, and
     reopen from the same storage root.
   - Verify catalog bootstrap loads rows, `catalog_replay_start_ts`, and
     `next_table_id` from `catalog.mtb`.

7. `next_table_id` rename coverage.
   - Rename tests such as the monotonic allocator restart test to use
     `next_table_id`.
   - Verify create-table allocation, recovery allocator restoration, and
     absent user-table-file cleanup still use the same typed `TableID`
     watermark semantics.
   - Verify serialized multi-table meta-block round trips preserve the same
     logical value under the new name.

8. User-table reclamation remains unchanged.
   - Existing user-table root reachability tests continue to pass.
   - No user-table collector starts using catalog's one-root proof.

Routine validation:

```bash
cargo nextest run -p doradb-storage
```

Focused coverage should include changed Rust files or directories, for example:

```bash
tools/coverage_focus.rs \
  --path doradb-storage/src/file/multi_table_file.rs \
  --path doradb-storage/src/catalog/storage/checkpoint.rs
```

If generic CoW publication or backend-neutral IO paths change, also validate:

```bash
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

## Open Questions

1. Recovery-time allocation-map rebuild remains out of scope. Future work
   should decide whether startup should validate or rebuild `catalog.mtb` and
   user-table allocation maps from selected durable roots when no active
   transactions or retained roots exist. Related user-table work is tracked by
   `docs/backlogs/000108-recovery-table-file-alloc-map-rebuild.md`.
2. Trigger, threshold, budget, backoff, and diagnostics policy for catalog and
   user-table reachability reclamation remains out of scope. This is tracked by
   `docs/backlogs/000109-catalog-user-table-block-reclamation-trigger-budget-policy.md`.
3. If implementation discovers direct catalog-file readers that can overlap
   with catalog checkpoint root publication and require old-root block access,
   the one-root proof must be revisited before code changes proceed.
