---
id: 000120
title: Secondary Index Runtime Access And Recovery
status: implemented
created: 2026-04-16
github_issue: 559
---

# Task: Secondary Index Runtime Access And Recovery

## Summary

Implement RFC 0014 Phase 4 for user-table secondary indexes: wire the Phase 3
`DualTreeUniqueIndex` and `DualTreeNonUniqueIndex` core into user-table
foreground access and recovery.

User-table runtime lookups, uniqueness checks, mutations, rollback, purge
cleanup, and scans must consult the composite MemTree/DiskTree indexes. Recovery
must load checkpointed secondary `DiskTree` roots as the cold layer and rebuild
only post-checkpoint hot/redone rows into MemTree. Catalog tables stay on the
existing in-memory single-tree runtime path.

## Context

RFC 0014 has delivered the prerequisites for this cutover:

1. Phase 1 added table-file secondary `DiskTree` roots and concrete unique and
   non-unique `DiskTree` readers/writers.
2. Phase 2 publishes those roots as table-checkpoint sidecar state.
3. Phase 3 implemented the composite secondary-index core:
   `DualTreeUniqueIndex`, `DualTreeNonUniqueIndex`,
   `DualTreeSecondaryIndex`, and `SecondaryDiskTreeRuntime`.

The current user-table runtime still owns `GenericSecondaryIndex` values backed
by one mutable MemTree per index. `ColumnStorage` already owns reusable
`SecondaryDiskTreeRuntime` values, but foreground access and recovery do not use
the composite indexes yet.

Recovery still scans checkpointed persisted LWC blocks and copies cold
secondary-index entries into the runtime MemTree. Phase 4 removes that old cold
backfill for user tables. The checkpointed `DiskTree` roots are already the
recovered cold secondary-index layer; recovery only needs to rebuild
post-checkpoint hot state into MemTree and replay newer cold-delete markers into
the deletion buffer.

Catalog tables remain intentionally simpler. They continue using
`GenericMemTable<FixedBufferPool, FixedBufferPool>` and
`GenericSecondaryIndex<FixedBufferPool>`. This task does not redesign catalog
index access or create a shared catalog/user secondary-index abstraction.

Issue Labels:
- type:task
- priority:high
- codex

Parent RFC:
- docs/rfcs/0014-dual-tree-secondary-index.md

## Goals

1. Make user-table runtime own and use the Phase 3 dual-tree secondary-index
   core for every user secondary index.
2. Use `DualTreeUniqueIndex<EvictableBufferPool>` and
   `DualTreeNonUniqueIndex<EvictableBufferPool>` directly for user-table index
   behavior.
3. Use the existing `DualTreeSecondaryIndex<EvictableBufferPool>` only as the
   minimal mixed-index array slot when a table needs one collection containing
   both unique and non-unique indexes.
4. Do not add another wrapper enum, facade, or abstraction around
   `DualTreeUniqueIndex` or `DualTreeNonUniqueIndex`.
5. Keep the existing `UniqueIndex` and `NonUniqueIndex` trait signatures
   unchanged.
6. Preserve catalog-table secondary indexes on the current
   `GenericSecondaryIndex<FixedBufferPool>` path.
7. Wire user-table foreground unique lookup, uniqueness enforcement, insert,
   update, delete, rollback, purge-facing cleanup, and non-unique scans through
   the dual-tree indexes.
8. Add recovery-specific MemTree-only index insertion APIs so redo/page
   recovery can rebuild hot MemTree state without probing DiskTree.
9. Remove the user-table recovery path that rebuilds checkpointed cold
   secondary-index entries by scanning persisted LWC data into MemTree.
10. Keep recovery loading checkpointed `DiskTree` roots through
    `ColumnStorage` and `SecondaryDiskTreeRuntime`.
11. Preserve cold-delete recovery: replay cold deletes newer than
    `deletion_cutoff_ts` into `ColumnDeletionBuffer`, and let the composite
    runtime plus deletion visibility suppress stale DiskTree candidates.
12. Keep method semantics from RFC 0014 Phase 3 unchanged:
    - unique MemTree hits remain terminal;
    - unique DiskTree fallback happens only on MemTree miss;
    - non-unique MemTree and DiskTree exact entries are merged;
    - foreground-style mutations affect MemTree only.

## Non-Goals

1. Do not change `UniqueIndex` or `NonUniqueIndex` method signatures.
2. Do not implement the access-context or caller-provided disk-guard redesign
   tracked by `docs/backlogs/000086-secondary-index-dual-tree-access-path.md`.
3. Do not add a new user-table secondary-index wrapper around the Phase 3
   dual-tree types.
4. Do not add a shared catalog/user secondary-index enum or facade.
5. Do not change catalog-table secondary-index storage, construction, or
   checkpoint behavior.
6. Do not remove catalog `GenericSecondaryIndex` usage.
7. Do not perform foreground persistent `DiskTree` writes.
8. Do not rebuild checkpointed cold `DiskTree` entries into user-table MemTree.
9. Do not implement MemTree cleanup or eviction rules; that remains RFC 0014
   Phase 5.
10. Do not implement generic B+Tree backend unification.
11. Do not change table-file metadata or storage format.
12. Do not add storage-version compatibility work.

## Unsafe Considerations

No new unsafe code is planned. The task should reuse the Phase 3 composite
core, existing MemTree helpers, existing DiskTree root runtimes, and current
table/recovery execution paths.

If implementation unexpectedly touches low-level B+Tree node internals or
unsafe buffer/page code, keep the unsafe boundary private, document every
invariant with `// SAFETY:`, and run:

```bash
cargo clippy -p doradb-storage --all-targets -- -D warnings
```

## Plan

1. Make Phase 3 composite types production-facing inside the crate.
   - Remove `#![cfg_attr(not(test), allow(dead_code))]` from
     `doradb-storage/src/index/composite_secondary_index.rs` once production
     call sites use the types.
   - Add only narrow accessors needed by table wiring, such as `unique()`,
     `non_unique()`, `mem()`, or `mem_mut()` equivalents when required.
   - Keep visibility `pub(crate)` or narrower.

2. Add MemTree extraction or construction helpers.
   - Provide a narrow way to consume a `GenericSecondaryIndex<I>` into its
     concrete `GenericUniqueBTreeIndex<I>` or `GenericNonUniqueBTreeIndex<I>`.
   - Use this to build the dual-tree indexes without duplicating MemTree
     construction logic.
   - Validate `index_no` and index kind alignment against table metadata.

3. Change user-table secondary-index ownership.
   - User `Table` should own dual-tree secondary indexes built from MemTree
     backends and `ColumnStorage`'s matching `SecondaryDiskTreeRuntime`.
   - Prefer storing `Box<[DualTreeSecondaryIndex<EvictableBufferPool>]>` only
     if one mixed collection is needed.
   - Do not introduce an additional user index owner type solely to wrap the
     Phase 3 dual-tree types.
   - Preserve catalog `CatalogTable` and catalog `GenericMemTable` index
     ownership unchanged.

4. Keep catalog access simple.
   - Leave `CatalogTable::new` and `MemTableAccessor` on the existing
     `GenericSecondaryIndex<FixedBufferPool>` path.
   - Do not chase catalog index-access cleanup during this task.
   - If implementation exposes concrete catalog follow-up work, record it as a
     backlog during task resolve rather than widening Phase 4.

5. Route user foreground access through dual-tree indexes.
   - Update user-table lookup and scan paths to call `DualTreeUniqueIndex` and
     `DualTreeNonUniqueIndex`.
   - Update user insert/update/delete helpers so uniqueness enforcement,
     compare-exchange ownership transfer, delete-shadow insertion, non-unique
     exact merge/suppression, rollback undo, and purge cleanup all use the
     composite trait implementations.
   - Keep helper functions generic over `impl UniqueIndex` or
     `impl NonUniqueIndex` only where that reduces duplication without hiding
     the user/catalog distinction.

6. Add recovery-specific MemTree-only APIs.
   - Add methods on `DualTreeUniqueIndex` and `DualTreeNonUniqueIndex`, or
     narrow associated helper functions, for recovery-only hot MemTree
     insertion.
   - These APIs must not call DiskTree lookup, prefix scan, or full scan.
   - Unique recovery insertion should preserve the current recovery latest-CTS
     behavior among recovered hot rows.
   - Non-unique recovery insertion should insert exact hot entries into
     MemTree and avoid DiskTree duplicate checks.

7. Cut over user-table recovery.
   - Remove the call that populates user-table secondary indexes from
     checkpointed persisted LWC data.
   - Keep recovery loading table files and `ColumnStorage`, which already opens
     published secondary `DiskTree` roots through `SecondaryDiskTreeRuntime`.
   - Keep hot row-page redo recovery and final row-page index population, but
     route index population through the MemTree-only recovery APIs.
   - Keep cold-delete redo replay into `ColumnDeletionBuffer` for entries with
     `row_id < pivot_row_id` and `cts >= deletion_cutoff_ts`.

8. Remove or narrow obsolete recovery helpers.
   - Delete `populate_index_via_persisted_data` for user tables if no longer
     needed.
   - Keep only hot row-page population helpers.
   - Rename helpers if needed so the remaining recovery code cannot be
     mistaken for cold backfill.

9. Update tests and direct test helpers.
   - Replace direct user-table `table.sec_idx()` test inspections with access
     to the user dual-tree index collection.
   - Keep catalog tests on the existing catalog accessors.
   - Add focused assertions that MemTree is not populated from checkpointed
     cold data after recovery.

10. Validate.
    - Run:

```bash
cargo fmt --all --check
cargo clippy -p doradb-storage --all-targets -- -D warnings
cargo nextest run -p doradb-storage
```

    - Run the alternate backend pass if implementation changes backend-neutral
      I/O behavior beyond using existing DiskTree/table-file read paths:

```bash
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

## Implementation Notes

- User-table secondary indexes now use the Phase 3 dual-tree runtime directly:
  `Table` owns `DualTreeSecondaryIndex<EvictableBufferPool>` values built from
  existing MemTree indexes plus `ColumnStorage` secondary `DiskTree` runtimes.
  Catalog tables stay on the existing `GenericSecondaryIndex<FixedBufferPool>`
  path.
- Foreground user-table lookup, uniqueness enforcement, insert, update, delete,
  scan, rollback, and purge-facing cleanup now route through the dual-tree
  unique/non-unique implementations. Rollback behavior was moved behind the
  crate-level `IndexRollback` trait so user tables and catalog tables can use
  their own index storage without table-specific delegate methods.
- Recovery no longer backfills checkpointed cold user-table secondary-index
  entries into MemTree. Checkpointed secondary `DiskTree` roots remain the cold
  layer, while redo recovery rebuilds only hot row-page state into MemTree
  through recovery-only insertion paths and replays newer cold-delete metadata
  into `ColumnDeletionBuffer`.
- Review fixes removed test-only production accessors, simplified
  `build_dual_tree_secondary_indexes` into one construction loop, renamed the
  generic MemTree conversion helper to `DualTreeSecondaryIndex::new`, added
  descriptive `LogRecovery` field comments, and kept unique rollback stale
  delete-marker cleanup conservative instead of traversing GC-owned undo chains.
- Follow-up work was intentionally deferred to backlog docs:
  `docs/backlogs/000087-refactor-recovery-process-parallel-log-replay.md` for
  broader recovery cleanup/parallel replay and
  `docs/backlogs/000088-remove-recovery-skip-option.md` for removing the
  dangerous production-facing recovery skip option.
- Validation completed:

```bash
cargo fmt --all --check
cargo clippy -p doradb-storage --all-targets -- -D warnings
cargo nextest run -p doradb-storage
tools/coverage_focus.rs --path doradb-storage/src/trx/recover.rs --top-uncovered 20
git diff --check
```

  The focused recovery coverage report was 97.19%, and the full
  `doradb-storage` nextest run passed with 568 tests.

## Impacts

- `doradb-storage/src/index/composite_secondary_index.rs`
  - production use of `DualTreeUniqueIndex`, `DualTreeNonUniqueIndex`,
    `DualTreeSecondaryIndex`, and MemTree-only recovery insertion helpers
- `doradb-storage/src/index/secondary_index.rs`
  - possible consuming helpers for existing MemTree-backed
    `GenericSecondaryIndex` construction output
- `doradb-storage/src/table/mod.rs`
  - user `Table` secondary-index ownership and construction from MemTree plus
    `SecondaryDiskTreeRuntime`
- `doradb-storage/src/table/access.rs`
  - user foreground lookup, scan, insert, update, delete, rollback-facing, and
    purge-facing index call sites
- `doradb-storage/src/table/recover.rs`
  - removal of persisted-LWC cold secondary-index backfill and addition of
    MemTree-only hot recovery insertion
- `doradb-storage/src/trx/recover.rs`
  - recovery orchestration no longer calls user persisted-data index backfill
- `doradb-storage/src/trx/undo/index.rs`
  - user rollback index operations route through dual-tree indexes while
    catalog rollback remains on MemTree indexes
- `doradb-storage/src/trx/purge.rs`
  - user purge/index cleanup tests or helpers that inspect secondary indexes
- `doradb-storage/src/table/tests.rs`
  - runtime, checkpoint, and recovery coverage for dual-tree user indexes
- `doradb-storage/src/trx/recover.rs`
  - restart coverage proving DiskTree cold roots are used directly

## Test Cases

1. User-table unique lookup after a data checkpoint returns a cold row through
   DiskTree without any row-page MemTree data.
2. Restart after checkpointed cold unique row:
   - table has zero recovered RowStore pages for the checkpointed row;
   - secondary lookup finds the row through DiskTree;
   - a MemTree-only inspection proves the cold entry was not backfilled.
3. Restart after checkpointed cold non-unique rows returns DiskTree exact
   entries in deterministic exact-key order.
4. Restart with checkpointed cold rows plus post-checkpoint hot redo rebuilds
   only the hot rows into MemTree and returns both cold and hot candidates.
5. Unique post-checkpoint hot redo for a key that shadows a cold DiskTree owner
   rebuilds MemTree without failing on a DiskTree duplicate.
6. Unique cold update/delete recovery:
   - newer cold-delete redo is replayed into `ColumnDeletionBuffer`;
   - stale DiskTree candidates are rejected by row/deletion visibility and key
     recheck;
   - no recovery path writes DiskTree.
7. Non-unique cold delete recovery suppresses only the exact stale DiskTree
   `(logical_key, row_id)` entry through deletion visibility and/or MemTree
   overlay state, without suppressing same-key different-row candidates.
8. User foreground insert, update, delete, rollback, and purge tests continue
   to pass with composite indexes in production.
9. Catalog table tests continue to pass on the existing
   `GenericSecondaryIndex<FixedBufferPool>` path.
10. Existing Phase 3 composite method-parity tests continue to pass.
11. A regression test proves `populate_index_via_persisted_data` or equivalent
    cold backfill is not called for user-table recovery.
12. Full crate validation passes with `cargo nextest run -p doradb-storage`.

## Open Questions

None blocking for the implemented task.

Deferred follow-up:
- The access-context/caller-provided disk-guard redesign remains tracked by
  `docs/backlogs/000086-secondary-index-dual-tree-access-path.md`.
- Recovery code cleanup and possible log replay parallelization are tracked by
  `docs/backlogs/000087-refactor-recovery-process-parallel-log-replay.md`.
- Removing the production-facing recovery skip option is tracked by
  `docs/backlogs/000088-remove-recovery-skip-option.md`.
