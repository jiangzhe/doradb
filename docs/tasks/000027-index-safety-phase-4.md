# Task: Index Safety Phase 4

## Summary

Implement index-safety cleanup for `docs/rfcs/0003-reduce-unsafe-usage-program.md` by reducing avoidable unsafe usage in index runtime paths while preserving behavior and performance.

This task is a sub-task of RFC 0003:
1. Parent RFC: `docs/rfcs/0003-reduce-unsafe-usage-program.md`

## Context

Phase 1 through Phase 3 already reduced unsafe usage in runtime, IO/file, and trx modules:
1. `docs/tasks/000024-unsafe-usage-baseline-phase-1.md`
2. `docs/tasks/000025-reduce-unsafe-usage-runtime-hotspots-phase-2.md`
3. `docs/tasks/000026-io-file-trx-safety-phase-3.md`

Current index unsafe hotspots in baseline:
1. `doradb-storage/src/index/btree_node.rs`
2. `doradb-storage/src/index/btree.rs`
3. `doradb-storage/src/index/block_index.rs`
4. `doradb-storage/src/index/column_block_index.rs`

Primary avoidable patterns in these files:
1. Optimistic traversal call sites use `page_unchecked()` directly and rely on manual validation sequencing.
2. Packed-layout pointer/slice access is repeated at many call sites instead of centralized behind small internal helpers.
3. Some node construction paths use temporary patterns that should be tightened without introducing stack-heavy page objects.

Performance constraints for this task:
1. Do not introduce page-sized stack allocations in async/hot paths.
2. Keep heap-first initialization for large node/page objects.
3. Do not add extra full-page copy steps in traversal and mutation hot paths.

## Goals

1. Remove avoidable index call-site unsafe usage in optimistic traversal code paths.
2. Encapsulate unavoidable packed-layout unsafe boundaries into smaller, audited helpers with explicit invariants.
3. Refactor node allocation/initialization paths safely while preserving index performance constraints.
4. Preserve B+Tree, block-index, and column-block-index behavior and on-disk format.
5. Validate with tests plus unsafe inventory qualitative reduction.

## Non-Goals

1. B+Tree algorithm redesign or node format redesign.
2. On-disk format changes for index or table file.
3. Changes to transaction/checkpoint/recovery protocol.
4. Full elimination of unsafe in packed node internals.
5. Benchmark gating in this task.

## Plan

1. Add validated optimistic-read helpers in `doradb-storage/src/buffer/guard.rs` for optimistic/facade guards.
2. Replace direct `page_unchecked()` usage in index traversals with validated helper usage in:
   `doradb-storage/src/index/btree.rs` and `doradb-storage/src/index/block_index.rs`.
3. Refactor `doradb-storage/src/index/btree.rs::split_root` to avoid temporary stack-style whole-node construction and keep in-place root update semantics.
4. Tighten packed-layout unsafe boundaries in `doradb-storage/src/index/btree_node.rs`:
   centralize slot/payload raw pointer operations into fewer private helpers, keep `read_unaligned`/`write_unaligned` usage localized, and add `// SAFETY:` contracts.
5. Tighten raw layout access in `doradb-storage/src/index/block_index.rs` and `doradb-storage/src/index/column_block_index.rs` by consolidating repeated `from_raw_parts`/pointer arithmetic boundaries.
6. Encapsulate `BlockIndexRoot` active-root pointer dereference paths in `doradb-storage/src/index/block_index.rs` behind small helper methods with latch/ordering/nullability invariants documented.
7. Preserve heap-first large node allocation strategy:
   keep direct heap initialization for `BTreeNode` and `ColumnBlockNode` paths and avoid stack-first page materialization.
8. Refresh unsafe baseline after refactor.

## Impacts

1. Guard API support for validated optimistic reads:
   `doradb-storage/src/buffer/guard.rs`
2. B+Tree traversal and split internals:
   `doradb-storage/src/index/btree.rs`
3. B+Tree packed-node internals:
   `doradb-storage/src/index/btree_node.rs`
4. Block index traversal/layout helpers:
   `doradb-storage/src/index/block_index.rs`
5. Column block index layout helpers:
   `doradb-storage/src/index/column_block_index.rs`
6. Baseline inventory:
   `docs/unsafe-usage-baseline.md`

## Test Cases

1. Targeted index correctness tests:
   `cargo test -p doradb-storage --no-default-features test_btree_split`
2. Targeted index correctness tests:
   `cargo test -p doradb-storage --no-default-features test_btree_concurrent_split`
3. Targeted index correctness tests:
   `cargo test -p doradb-storage --no-default-features test_block_index_search`
4. Targeted index correctness tests:
   `cargo test -p doradb-storage --no-default-features test_block_index_split`
5. Targeted index correctness tests:
   `cargo test -p doradb-storage --no-default-features test_batch_insert_into_empty_tree_and_find`
6. Full regression:
   `cargo test -p doradb-storage --no-default-features`
7. Feature-on regression:
   `cargo test -p doradb-storage`
8. Unsafe inventory refresh:
   `cargo +nightly -Zscript tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md`
9. Qualitative acceptance:
   updated baseline shows reduction in avoidable index unsafe call sites and improved boundary documentation.

## Open Questions

None.
