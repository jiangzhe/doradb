# Task: Readonly Column Buffer Pool Phase 4 Block Index Integration Refactor

## Summary

Implement Phase 4 of RFC-0004 (`docs/rfcs/0004-readonly-column-buffer-pool-program.md`) by integrating readonly buffer pool into column-block-index traversal and refactoring block index into clear components:
1. `RowBlockIndex` for in-memory row-store page mapping.
2. `ColumnBlockIndex` for on-disk column-store mapping and traversal using readonly buffer pool.
3. `BlockIndexRoot` as routing entrypoint (`pivot_row_id` + `column_root_page_id`) guarded by `HybridLatch`.

This task keeps a compatibility `BlockIndex` facade while removing `ActiveRoot` pointer storage from block-index root state.

## Context

Phase 3 (`docs/tasks/000033-readonly-column-buffer-pool-phase-3-per-table-integration.md`) already switched `Table::read_lwc_row` to readonly buffer pool. However, block-index file traversal still uses direct file I/O:
1. `BlockIndex::find_row` and `try_find_row` call `find_in_file` in `doradb-storage/src/index/block_index.rs`.
2. `find_in_file` in `doradb-storage/src/index/column_block_index.rs` reads via `TableFile::read_page`.

Current `BlockIndex` also mixes row-store and column-store responsibilities and stores `AtomicPtr<ActiveRoot>` in `BlockIndexRoot`, making the interface unclear for checkpoint root updates.

Checkpoint path currently updates block-index root with full active-root pointer (`TableAccess::data_checkpoint` -> `BlockIndex::update_file_root`). For this phase we only need to update pivot and column-index root page id.

## Goals

1. Refactor block index into explicit `RowBlockIndex`, `ColumnBlockIndex`, and `BlockIndexRoot` components.
2. Keep `BlockIndex` facade to preserve existing table/trx call sites.
3. Replace direct file I/O in column-block-index lookup with readonly buffer pool reads.
4. Make `BlockIndexRoot` own only:
   - pivot row id.
   - root page id of `ColumnBlockIndex`.
5. Keep `HybridLatch`-based root routing and atomic checkpoint root swap semantics.
6. Change checkpoint root refresh API to update only pivot and column root page id (no `ActiveRoot` pointer storage).

## Non-Goals

1. No redesign of table-file checkpoint/recovery protocol or metadata model.
2. No metrics/admission/eviction policy tuning for readonly buffer pool.
3. No broad API break across table/trx modules; compatibility facade remains.
4. No changes to mutable row-store buffer pool behavior.
5. No new features beyond RFC-0004 Phase 4 integration scope.

## Unsafe Considerations (If Applicable)

This task should avoid introducing new `unsafe` logic and reuse existing readonly pool internals.

Affected areas:
1. Readonly frame/page access already uses unsafe-adjacent code in `doradb-storage/src/buffer/readonly.rs`.
2. Block-index refactor should stay in safe Rust; no raw-pointer ownership in `BlockIndexRoot`.

Requirements:
1. Preserve existing readonly key/frame invariants and latch publication order.
2. Keep or update `// SAFETY:` comments only if existing unsafe-adjacent call paths are touched.
3. Do not reintroduce `ActiveRoot` raw-pointer storage in block-index root state.

Validation scope:
```bash
cargo test -p doradb-storage --no-default-features
cargo test -p doradb-storage
```

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Split block-index modules:
   - add `doradb-storage/src/index/row_block_index.rs`.
   - add `doradb-storage/src/index/block_index_root.rs`.
   - keep `doradb-storage/src/index/block_index.rs` as facade.
2. Move in-memory row index logic from current `BlockIndex` into `RowBlockIndex`:
   - insert-page allocation and free-list cache.
   - row-page lookup in in-memory tree.
   - memory scan cursor support.
   - page committer hooks.
3. Refactor `BlockIndexRoot`:
   - state: `HybridLatch`, `pivot_row_id`, `column_root_page_id`.
   - routing API: guide row id to row side vs column side.
   - checkpoint update API: update pivot and column root page id only.
4. Refactor `ColumnBlockIndex` lookup path:
   - remove direct file-read traversal in lookup.
   - use table-bound readonly buffer pool for all node reads.
   - support thin wrapper usage with guided root page id.
5. Rebuild `BlockIndex` facade composition:
   - own `RowBlockIndex`, `ColumnBlockIndex`, `BlockIndexRoot`.
   - preserve external methods used by table/trx call sites.
6. Update constructors and call sites:
   - `doradb-storage/src/session.rs`.
   - `doradb-storage/src/catalog/storage/mod.rs`.
   - any other `BlockIndex::new` call path.
7. Update checkpoint integration:
   - replace `update_file_root(active_root)` call with explicit root-state update using `(pivot_row_id, column_block_index_root)` in `doradb-storage/src/table/access.rs`.
8. Remove obsolete active-root-pointer-dependent helpers and tests; add replacement tests for new root state model.

## Impacts

Primary files:
1. `doradb-storage/src/index/block_index.rs`
2. `doradb-storage/src/index/row_block_index.rs`
3. `doradb-storage/src/index/block_index_root.rs`
4. `doradb-storage/src/index/column_block_index.rs`
5. `doradb-storage/src/index/mod.rs`
6. `doradb-storage/src/table/access.rs`
7. `doradb-storage/src/session.rs`
8. `doradb-storage/src/catalog/storage/mod.rs`
9. `doradb-storage/src/table/tests.rs`
10. `doradb-storage/src/index/*` tests

Primary structs/functions:
1. `RowBlockIndex`
2. `ColumnBlockIndex`
3. `BlockIndexRoot`
4. `BlockIndex` (facade)
5. `BlockIndex::new`
6. `BlockIndex::find_row`
7. checkpoint root update entry in table checkpoint path
8. column-index lookup helpers previously using direct file I/O

## Test Cases

1. Existing row-store lookup and insert-page behavior remains unchanged through facade APIs.
2. `BlockIndexRoot` routing boundary tests:
   - `row_id < pivot` routes to column side.
   - `row_id >= pivot` routes to row side.
3. Checkpoint root update test:
   - updating pivot and column root page id changes routing and lookup as expected.
4. Column-block-index lookup uses readonly pool path:
   - repeated lookups reuse cache (no functional regression).
5. End-to-end lookup correctness:
   - old and new roots during checkpoint progression remain snapshot-correct for row location resolution.
6. Regression suites:
```bash
cargo test -p doradb-storage --no-default-features
cargo test -p doradb-storage
```

## Open Questions

1. No blocking open questions for this task scope.
2. Optional follow-up: expose readonly cache hit/miss metrics for operational tuning.
