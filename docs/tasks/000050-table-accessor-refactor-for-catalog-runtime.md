# Task: Table Accessor Refactor for Catalog Runtime

## Summary

Implement RFC-0006 Phase 3 by extracting `TableAccess` logic into a standalone
`TableAccessor<D, I>` and refactoring core table/index structures to generic
forms (`GenericBTree<P>`, `GenericRowBlockIndex<P>`, etc.) with compatibility
aliases that preserve existing public type names. This task is behavior-preserving
and prepares Phase 4 `CatalogTable` without changing `Statement` APIs.

## Context

RFC-0006 Phase 3 requires accessor-first refactor so table operation logic can
be reused by both current `Table` and future dedicated catalog runtime.

Current code has two blockers:
1. `TableAccess` is implemented directly on `Table`, and heavily coupled to
   `Table` internals.
2. Core index/block types are concrete to `FixedBufferPool` and non-generic
   runtime wiring, which limits reuse.

This task adopts Proposal C approved in design review:
1. genericize core index/block types first,
2. implement `TableAccess` on `TableAccessor<D, I>`,
3. keep current names via aliases for compatibility.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Parent RFC:`
`- docs/rfcs/0006-cache-first-unified-catalog-storage-refactor.md`

## Goals

1. Introduce generic type families for key runtime components:
   - `GenericBTree<P: BufferPool>`
   - `GenericUniqueBTreeIndex<P: BufferPool>`
   - `GenericNonUniqueBTreeIndex<P: BufferPool>`
   - `GenericSecondaryIndex<P: BufferPool>`
   - `GenericRowBlockIndex<P: BufferPool>`
   - `GenericBlockIndex<P: BufferPool>`
2. Preserve existing external names through aliases using current concrete pools
   (for example `type BTree = GenericBTree<FixedBufferPool>`).
3. Introduce standalone `TableAccessor<D, I>` where:
   - `D` is data-pool family dependency,
   - `I` is index-pool family dependency.
4. Move `TableAccess` implementation from direct `Table` coupling to
   accessor-driven shared logic.
5. Keep `Statement` API signatures unchanged in this phase.
6. Keep runtime behavior unchanged for DML, MVCC, rollback, purge, and recovery
   call paths.

## Non-Goals

1. Introduce `CatalogTable` runtime type (Phase 4).
2. Remove legacy catalog bootstrap scratch file unlink path (Phase 4).
3. Switch catalog runtime pool wiring to fixed-pool specialization (Phase 5).
4. Change catalog checkpoint/recovery-cutoff semantics (Phases 6-7).
5. Add new engine configuration knobs.
6. Change user-facing `Statement` APIs.

## Unsafe Considerations (If Applicable)

No new `unsafe` scope is expected. This task primarily reshapes generic type
boundaries and call wiring in safe Rust.

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Genericize B-Tree and secondary index stack:
   - replace concrete `FixedBufferPool` fields/constructors with `P: BufferPool`
     in B-Tree and index wrappers.
   - keep current concrete aliases to avoid broad call-site breakage.
2. Genericize row/block index stack:
   - introduce `GenericRowBlockIndex<P>` and `GenericBlockIndex<P>`.
   - keep existing names as aliases to current concrete instantiations.
3. Introduce `TableAccessor<D, I>` as a thin operation wrapper:
   - hold required references to runtime pieces (metadata, pools, block index,
     secondary indexes, deletion buffer, table id).
   - make accessor cheap to construct per operation.
4. Migrate `TableAccess` implementation:
   - move operation internals from `impl TableAccess for Table` to
     `impl<D, I> TableAccess for TableAccessor<D, I>`.
   - keep `impl TableAccess for Table` as forwarding shim by constructing
     `TableAccessor` on demand.
5. Maintain compatibility in dependent modules:
   - `stmt`, `trx/recover`, `trx/purge`, `trx/undo`, catalog storage wrappers.
6. Run formatting and regression tests to prove behavioral parity.

## Implementation Notes

1. Implemented generic type families with compatibility aliases and preserved runtime behavior:
   - `GenericBTree<P>` with alias `BTree = GenericBTree<FixedBufferPool>`.
   - `GenericUniqueBTreeIndex<P>` / `GenericNonUniqueBTreeIndex<P>` with aliases
     `UniqueBTreeIndex` / `NonUniqueBTreeIndex`.
   - `GenericSecondaryIndex<P>` and `GenericIndexKind<P>` with aliases
     `SecondaryIndex` and `IndexKind`.
   - `GenericRowBlockIndex<P>` / `GenericBlockIndex<P>` with aliases
     `RowBlockIndex` / `BlockIndex`.
2. Refactored `TableAccess` to accessor-first design:
   - Added standalone `TableAccessor<'a, D, I>`.
   - Moved `TableAccess` implementation to `impl<D, I> TableAccess for TableAccessor<D, I>`.
   - Kept `impl TableAccess for Table` as a forwarding shim by constructing
     `RuntimeTableAccessor` on each call.
3. Kept `Statement` interface unchanged in this phase.
4. Resolved generic-bound style during implementation:
   - Removed `BufferPool` bounds from generic struct declarations.
   - Kept bounds on impl blocks/methods where needed.
   - Kept minimal `'static` bounds where types store `&'static P`.
5. Added missing rustdoc comments for modified public generic wrappers/aliases
   and accessor constructors to keep API docs clear after refactor.
6. Verification results:
   - `cargo fmt --all`
   - `cargo build -p doradb-storage --no-default-features`
   - `cargo test -p doradb-storage --no-default-features`
   - Full no-default-feature suite passed (`303 passed, 0 failed`).
7. Follow-up backlog:
   - `docs/backlogs/000046-evaluate-explicit-evictable-alias-names-for-generic-index-wrappers.md`

## Impacts

1. `doradb-storage/src/index/btree.rs`
2. `doradb-storage/src/index/unique_index.rs`
3. `doradb-storage/src/index/non_unique_index.rs`
4. `doradb-storage/src/index/secondary_index.rs`
5. `doradb-storage/src/index/row_block_index.rs`
6. `doradb-storage/src/index/block_index.rs`
7. `doradb-storage/src/table/access.rs`
8. `doradb-storage/src/table/mod.rs`
9. `doradb-storage/src/stmt/mod.rs` (compatibility validation only)
10. `doradb-storage/src/trx/recover.rs` (compatibility validation only)
11. `doradb-storage/src/trx/purge.rs` (compatibility validation only)
12. `doradb-storage/src/trx/undo/index.rs` (compatibility validation only)
13. `doradb-storage/src/trx/undo/row.rs` (compatibility validation only)

## Test Cases

1. Build and type-check refactor surface:
   - `cargo build -p doradb-storage --no-default-features`
2. Full regression suite:
   - `cargo test -p doradb-storage --no-default-features`
3. Targeted recovery and catalog bootstrap checks:
   - `test_bootstrap_creates_catalog_mtb_without_catalog_tbl_files`
   - `test_log_recover_ddl`
4. Table access behavior parity checks from existing table tests:
   - insert/update/delete/select MVCC paths,
   - index maintenance and rollback paths.
5. Purge/undo compile-and-behavior checks through existing tests.

## Open Questions

1. Additional disk-read path abstractions remain concrete in this phase.
   Rationale: `ReadonlyBufferPool` is currently the only disk-read buffer pool
   implementation, so genericization here is unnecessary churn for Phase 3.
2. Explicit extra alias names (for example `EvictableRowBlockIndex`) were not
   introduced in this phase; only compatibility aliases that preserve existing
   public names were kept. If multi-runtime readability becomes a pain point in
   later phases, we should evaluate adding those aliases in a follow-up task.
