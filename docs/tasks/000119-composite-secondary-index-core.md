---
id: 000119
title: Composite Secondary Index Core
status: implemented
created: 2026-04-15
github_issue: 557
---

# Task: Composite Secondary Index Core

## Summary

Implement RFC 0014 Phase 3 for user-table secondary indexes: a
self-contained composite `MemTree`/`DiskTree` core that groups the existing
single-tree runtime index with the checkpointed secondary `DiskTree` root and
implements the current unique and non-unique secondary-index trait contracts.

This task proves the dual-tree method semantics with real MemTree and DiskTree
fixtures, but it does not wire the composite into table foreground access or
recovery. The existing single-tree runtime path remains the only table runtime
path until RFC 0014 Phase 4.

## Context

RFC 0014 has already delivered the durable prerequisites:

1. Phase 1 added user-table secondary `DiskTree` roots and concrete persisted
   `DiskTree` readers/writers.
2. Phase 2 publishes those roots as table-checkpoint sidecar state.

RFC 0014 Phase 3 focuses on the reusable and testable part of the composite
runtime: method-by-method unique and non-unique semantics over the two tree
layers. Phase 4 remains responsible for table runtime wiring and composite
recovery.

The current table runtime still stores `GenericSecondaryIndex` values backed by
one mutable B+Tree per secondary index. This task wraps that existing
single-tree implementation as MemTree, groups it with the persisted cold
DiskTree context in a `DualTree`-style type, and implements the existing
`UniqueIndex` and `NonUniqueIndex` traits on the grouped types.

Issue Labels:
- type:task
- priority:high
- codex

Parent RFC:
- docs/rfcs/0014-dual-tree-secondary-index.md

## Goals

1. Add a user-table-only dual-tree secondary-index core that groups a MemTree
   backend with one persisted secondary DiskTree root context.
2. Implement a unique dual-tree index that implements the existing
   `UniqueIndex` trait.
3. Implement a non-unique dual-tree index that implements the existing
   `NonUniqueIndex` trait.
4. Add an enum wrapper for one secondary index:
   `DualTreeSecondaryIndex::{Unique, NonUnique}` or an equivalent local name.
5. Keep the cold context table-specific. The composite API must not carry a
   caller-supplied `FileKind`; secondary DiskTree roots are user-table-only and
   internal open helpers should use `FileKind::TableFile`.
6. Keep key encoding single-sourced. The DiskTree context must not store a
   duplicate `BTreeKeyEncoder`; DiskTree readers derive encoders from table
   metadata and MemTree helper scans return already encoded keys from their
   own backing tree.
7. Preserve current unique index semantics over two layers:
   - MemTree live hit is terminal.
   - MemTree delete-shadow hit is terminal.
   - DiskTree is probed only on MemTree miss.
   - DiskTree duplicate candidates are reported without DiskTree mutation.
   - DiskTree owners can be claimed by installing MemTree overlay state.
   - cold-owner delete shadows are installed in MemTree only.
8. Preserve current non-unique index semantics over two layers:
   - exact entries from MemTree and DiskTree are merged.
   - active MemTree exact entries are returned.
   - delete-marked MemTree exact entries suppress only matching DiskTree exact
     `(logical_key, row_id)` entries.
   - results are deduplicated and ordered by exact key order.
   - foreground-style mutations affect MemTree only.
9. Add narrow MemTree helper APIs only where required for correct composite
   semantics:
   - insert a unique delete-shadow for a cold owner when MemTree misses and
     DiskTree matches;
   - insert a non-unique delete-marked exact overlay for a cold exact entry;
   - scan MemTree entries with encoded keys, row ids, and delete state for
     composite merge and suppression.
10. Prove with tests that composite mutations leave DiskTree roots unchanged.

## Non-Goals

1. Do not wire the dual-tree core into `Table`, `GenericMemTable`, table
   foreground DML, table scans, rollback, purge, or recovery.
2. Do not replace the current `GenericSecondaryIndex` table runtime array.
3. Do not change catalog-table secondary indexes.
4. Do not change recovery source selection or remove the persisted-LWC cold
   secondary-index rebuild path.
5. Do not rebuild the current single runtime tree from DiskTree.
6. Do not perform foreground persistent DiskTree writes.
7. Do not change table-file metadata or storage format.
8. Do not add storage-version compatibility work.
9. Do not implement MemTree cleanup or eviction.
10. Do not perform generic B+Tree backend unification.
11. Do not expose non-unique DiskTree delete-mask APIs or value payloads.

## Unsafe Considerations

No new unsafe code is planned. The implementation should reuse existing safe
MemTree, DiskTree, table-file, and readonly-buffer APIs.

If implementation unexpectedly touches low-level B+Tree node internals that
require unsafe changes, keep those changes private, document every invariant
with `// SAFETY:`, and run:

```bash
cargo clippy -p doradb-storage --all-targets -- -D warnings
```

## Plan

1. Add a composite secondary-index module.
   - Create `doradb-storage/src/index/composite_secondary_index.rs`.
   - Export it internally from `doradb-storage/src/index/mod.rs`.
   - Keep all new runtime types `pub(crate)` or narrower.

2. Define the cold-root context.
   - Add a `SecondaryDiskTreeContext` or similarly named struct containing:
     - `root: BlockID`
     - `index_no: usize`
     - `metadata: Arc<TableMetadata>`
     - `file: Arc<SparseFile>`
     - `disk_pool: QuiescentGuard<ReadonlyBufferPool>`
   - Do not store `FileKind`; internal DiskTree open helpers use
     `FileKind::TableFile`.
   - Do not store `BTreeKeyEncoder`; open helpers derive encoding through
     `UniqueDiskTree::new` and `NonUniqueDiskTree::new`, and merge helpers use
     encoded entries returned by each tree layer.

3. Define dual-tree grouping types.
   - Add `DualTreeUniqueIndex<P: BufferPool>` with:
     - `mem: GenericUniqueBTreeIndex<P>`
     - `disk: SecondaryDiskTreeContext`
   - Add `DualTreeNonUniqueIndex<P: BufferPool>` with:
     - `mem: GenericNonUniqueBTreeIndex<P>`
     - `disk: SecondaryDiskTreeContext`
   - Add `DualTreeSecondaryIndex<P: BufferPool>` with unique and non-unique
     variants for future table runtime use.
   - Add constructors that validate `index_no`, metadata index kind, and
     MemTree/DiskTree kind alignment.

4. Add table-only DiskTree open helpers.
   - Add private helper methods on `SecondaryDiskTreeContext` to open
     `UniqueDiskTree` and `NonUniqueDiskTree`.
   - Each helper should allocate or derive a disk pool guard internally from
     the stored readonly pool, then call the existing DiskTree constructor with
     `FileKind::TableFile`.
   - Keep the root snapshot immutable for each method call.

5. Add narrow unique MemTree helper APIs.
   - Add a helper on `GenericUniqueBTreeIndex` to insert a delete-shadow
     `(logical_key -> deleted(row_id))` when the key is currently absent.
   - Add a helper to scan unique MemTree entries as encoded logical keys plus
     `(row_id, deleted)` state.
   - Keep helpers `pub(crate)` and document that they exist for dual-tree
     composition, not for DiskTree mutation.

6. Add narrow non-unique MemTree helper APIs.
   - Add a helper on `GenericNonUniqueBTreeIndex` to insert a delete-marked
     exact `(logical_key, row_id)` overlay when the exact key is currently
     absent.
   - Add a helper to scan exact MemTree entries as encoded exact keys plus
     `(row_id, deleted)` state.
   - Preserve the current one-byte MemTree delete flag as a runtime-only
     concern.

7. Implement `UniqueIndex` for `DualTreeUniqueIndex<P>`.
   - `lookup`:
     - probe `mem.lookup`;
     - return any MemTree live or delete-shadow hit;
     - otherwise open the unique DiskTree and return a cold hit as
       `Some((row_id, false))`.
   - `insert_if_not_exists`:
     - preserve current MemTree duplicate and delete-shadow merge behavior;
     - on MemTree miss, report a DiskTree owner as
       `IndexInsert::DuplicateKey(row_id, false)` without mutation;
     - otherwise insert into MemTree.
   - `compare_exchange`:
     - MemTree `Ok` and `Mismatch` are terminal;
     - on MemTree `NotExists`, claim a matching DiskTree owner by inserting
       `new_row_id` into MemTree;
     - return `Mismatch` for a different DiskTree owner and `NotExists` for
       complete miss.
   - `mask_as_deleted`:
     - mark matching MemTree state deleted when present;
     - when only DiskTree maps the key to `row_id`, install a MemTree
       delete-shadow for that cold owner;
     - never update DiskTree.
   - `compare_delete`:
     - remove or adjust only MemTree overlay state;
     - if only DiskTree has the matching owner, return success as an
       idempotent no-op;
     - never update DiskTree.
   - `scan_values`:
     - merge MemTree and DiskTree entries by encoded logical key;
     - MemTree state is terminal per logical key and suppresses the same-key
       DiskTree candidate;
     - append the resulting row ids in deterministic encoded-key order.

8. Implement `NonUniqueIndex` for `DualTreeNonUniqueIndex<P>`.
   - `lookup`:
     - collect MemTree exact entries for the logical key, including delete
       state;
     - collect DiskTree exact entries for the logical key;
     - return active MemTree row ids;
     - suppress only matching DiskTree exact entries when MemTree has a
       delete-marked exact overlay;
     - deduplicate and order by encoded exact key.
   - `lookup_unique`:
     - check MemTree exact state first;
     - a live or delete-marked MemTree exact hit is terminal;
     - on MemTree miss, return DiskTree exact-key presence as `Some(true)`.
   - `insert_if_not_exists`:
     - preserve current MemTree duplicate and exact delete-mark merge behavior;
     - on MemTree miss, report matching DiskTree exact presence as a duplicate
       without mutation;
     - otherwise insert active exact entry into MemTree.
   - `mask_as_deleted`:
     - mark matching MemTree exact state deleted when present;
     - when only DiskTree contains the exact key, install a delete-marked
       MemTree exact overlay;
     - never update DiskTree.
   - `mask_as_active`:
     - unmask MemTree exact state only;
     - never update DiskTree.
   - `compare_delete`:
     - remove only matching MemTree exact overlay state;
     - if only DiskTree has the exact key, return success as an idempotent
       no-op;
     - never update DiskTree.
   - `scan_values`:
     - merge all MemTree and DiskTree exact entries with the same suppression
       and ordering rules as `lookup`.

9. Keep runtime integration seams explicit.
   - The new dual-tree types may include constructors that accept already-built
     MemTree indexes and cold contexts.
   - Do not alter `build_secondary_indexes`, `GenericMemTable::new`, or
     `Table::sec_idx()` in this task.
   - Leave Phase 4 responsible for changing table-owned secondary-index
     storage and call sites.

10. Add focused tests and validation.
    - Keep tests inline in the changed index modules.
    - Use real `GenericUniqueBTreeIndex` and `GenericNonUniqueBTreeIndex`
      MemTree fixtures.
    - Use real persisted DiskTree roots built through existing batch writers.
    - Run:

```bash
cargo fmt --all --check
cargo clippy -p doradb-storage --all-targets -- -D warnings
cargo nextest run -p doradb-storage
```

Run the alternate backend pass only if implementation changes backend-neutral
I/O paths beyond using existing table-file and DiskTree read helpers:

```bash
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

## Implementation Notes

1. Implemented the self-contained composite secondary-index core:
   - added `DualTreeUniqueIndex`, `DualTreeNonUniqueIndex`, and
     `DualTreeSecondaryIndex` over the existing BTree-backed MemTree indexes.
   - implemented the current `UniqueIndex` and `NonUniqueIndex` trait
     contracts with terminal MemTree semantics, DiskTree fallback, unique cold
     owner claiming, unique delete-shadow overlays, non-unique exact-entry
     merge/suppression, and deterministic encoded-key ordering.
   - added focused MemTree helper APIs for encoded state scans, unique
     delete-shadow insertion, and non-unique delete-marked exact overlays
     without widening the public trait surfaces.

2. Reshaped the cold DiskTree runtime after implementation review:
   - replaced the original root-owning `SecondaryDiskTreeContext` shape with
     `SecondaryDiskTreeRuntime`, which stores the fixed secondary-index shape
     and opens root snapshots against the currently published table root or an
     explicit historical root.
   - split `DiskTreeRuntime<F>` from borrowed `DiskTree<'_, F>` root snapshots,
     keeping `UniqueDiskTree` and `NonUniqueDiskTree` as type aliases.
   - moved reusable secondary DiskTree runtimes into `ColumnStorage`, while
     `TableMeta.secondary_index_roots` remains the slim persisted root vector.
   - checkpoint sidecar application now reuses the stored secondary runtime and
     opens explicit old-root snapshots when rewriting roots.

3. Completed related correctness and ownership cleanup:
   - fixed non-unique encoded-entry collection so malformed exact-key decode
     failures propagate as errors instead of ending scans with partial results.
   - removed `ReadonlyBackingFile`; queued read and write IO now keep plain
     `Arc<SparseFile>` references, and readonly miss-load dedupe clones the file
     only when creating a new miss-load submission.
   - removed CoW file `*_with_owner` plumbing and kept file ownership at the
     async submission boundary.
   - refreshed the unsafe usage baseline for the added BTree-node test helper
     unsafe block.

4. Added coverage for the Phase 3 behavior:
   - unique composite tests cover MemTree hits, DiskTree fallback, duplicate
     detection, cold-owner claiming, delete-shadow behavior, scan merge, and
     unchanged DiskTree roots.
   - non-unique composite tests cover exact lookup/lookup_unique, duplicate
     detection, delete-marked suppression, scan merge, and unchanged DiskTree
     roots.
   - runtime-root tests cover resolving the published secondary root per open.
   - regression coverage verifies malformed non-unique exact keys surface
     `Error::InvalidState`.

5. Validation completed after checklist review:
   - `cargo check -p doradb-storage`
   - `cargo nextest run -p doradb-storage disk_tree` (14 passed, 548 skipped)
   - `cargo nextest run -p doradb-storage composite_secondary_index` (4 passed,
     558 skipped)
   - `cargo nextest run -p doradb-storage readonly` (33 passed, 529 skipped)
   - `cargo fmt --all --check`
   - `git diff --check`
   - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
   - `cargo nextest run -p doradb-storage` (562 passed, 0 skipped)

No unresolved checklist fixes or intentionally deferred backlog items remain
for this task. The Phase 4 runtime/recovery wiring remains tracked by RFC 0014.

## Impacts

- `doradb-storage/src/index/composite_secondary_index.rs`
  - new dual-tree grouping types, `SecondaryDiskTreeRuntime`, trait
    implementations, and composite merge helpers
- `doradb-storage/src/index/mod.rs`
  - internal module export for the composite index core
- `doradb-storage/src/index/unique_index.rs`
  - narrow unique MemTree helper APIs for cold-owner delete-shadow insertion
    and encoded-state scans
- `doradb-storage/src/index/non_unique_index.rs`
  - narrow non-unique MemTree helper APIs for delete-marked exact overlay
    insertion and encoded-state scans; encoded scan callbacks now propagate
    decode failures
- `doradb-storage/src/index/disk_tree.rs`
  - `DiskTreeRuntime<F>` owns fixed shape and `DiskTree<'_, F>` borrows it as a
    root snapshot
- `doradb-storage/src/table/mod.rs`
  - `ColumnStorage` owns reusable secondary DiskTree runtimes for table-scoped
    secondary indexes
- `doradb-storage/src/table/persistence.rs`
  - checkpoint secondary-root rewrites reuse the stored runtimes with explicit
    old-root snapshots
- `doradb-storage/src/buffer/readonly.rs` and `doradb-storage/src/file/*.rs`
  - queued readonly reads and table writes use `Arc<SparseFile>` directly
    instead of `ReadonlyBackingFile`
- `doradb-storage/src/index/btree_scan.rs` and
  `doradb-storage/src/index/btree_node.rs`
  - scan callbacks can propagate errors, and a test helper exposes controlled
    raw-key mutation
- `doradb-storage/src/table/tests.rs`
  - table checkpoint tests use the stored runtime helpers for DiskTree lookups

## Test Cases

1. Unique `lookup`:
   - MemTree live hit returns MemTree row id and does not read DiskTree result.
   - MemTree delete-shadow hit is terminal and does not fall through.
   - MemTree miss plus DiskTree hit returns cold owner as live candidate.
   - complete miss returns `None`.
2. Unique `insert_if_not_exists`:
   - active MemTree duplicate is terminal.
   - matching MemTree delete-shadow can merge when requested.
   - DiskTree duplicate returns `DuplicateKey(row_id, false)` without
     modifying MemTree or DiskTree.
   - complete miss inserts into MemTree.
3. Unique `compare_exchange`:
   - matching MemTree state updates normally.
   - mismatched MemTree state is terminal.
   - matching DiskTree owner is claimed by MemTree overlay.
   - mismatched DiskTree owner returns mismatch.
4. Unique delete behavior:
   - `mask_as_deleted` marks MemTree state when present.
   - `mask_as_deleted` installs a cold-owner MemTree delete-shadow when only
     DiskTree matches.
   - `compare_delete` removes only MemTree overlay state.
   - all delete paths preserve the original DiskTree root.
5. Unique `scan_values`:
   - MemTree entries are returned in deterministic key order.
   - same-key MemTree state suppresses DiskTree state.
   - delete-shadow suppression is covered.
6. Non-unique `lookup`:
   - active MemTree exact entries and DiskTree exact entries are merged.
   - delete-marked MemTree exact entries suppress only matching DiskTree exact
     keys.
   - different row ids with the same logical key remain visible.
   - output is deduplicated and ordered by `(logical_key, row_id)`.
7. Non-unique `lookup_unique`:
   - MemTree active hit returns `Some(true)`.
   - MemTree delete-marked hit returns `Some(false)`.
   - MemTree miss plus DiskTree exact hit returns `Some(true)`.
   - complete miss returns `None`.
8. Non-unique mutation methods:
   - `insert_if_not_exists` reports DiskTree exact duplicate without mutation.
   - `mask_as_deleted` installs a MemTree delete-marked overlay for a cold exact
     key.
   - `mask_as_active` affects only MemTree.
   - `compare_delete` affects only MemTree and treats DiskTree-only matches as
     idempotent no-ops.
9. Non-unique `scan_values`:
   - full exact-entry merge follows lookup suppression and ordering rules.
10. Rollback-shaped behavior:
    - unmask/remask/remove sequences mutate only MemTree.
    - DiskTree scan results and root ids remain unchanged across composite
      mutation tests.

## Open Questions

None blocking for this task.

Future Phase 4 must decide how `DualTreeSecondaryIndex` replaces or coexists
with the current `GenericSecondaryIndex` array inside user-table runtime
storage. This task intentionally leaves that table ownership change out of
scope.
