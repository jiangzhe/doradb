# Task: Readonly Column Buffer Pool Phase 1

## Summary

Implement Phase 1 of RFC-0004 (`docs/rfcs/0004-readonly-column-buffer-pool-program.md`, issue `#340`) by defining readonly column-buffer-pool interfaces and correctness contracts, without changing runtime read paths.

This phase adopts RFC proposal option 1 (two-level model):
1. `GlobalReadonlyBufferPool` for global frame/cache ownership.
2. Per-table `ReadonlyBufferPool` that implements `BufferPool`.

## Context

Current column-store reads still execute direct table-file I/O in read paths:
1. `Table::read_lwc_row` calls `TableFile::read_page` directly.
2. `index::column_block_index::find_in_file` / `read_node_from_file` call `TableFile::read_page` directly.

Existing pools do not match readonly column-store semantics:
1. `FixedBufferPool` has no eviction.
2. `EvictableBufferPool` evicts with temp-file semantics intended for mutable pages.

RFC-0004 requires readonly caching with:
1. Drop-only eviction (no write-back temporary file).
2. Global cache sharing across tables.
3. Stable mapping from physical on-disk identity to in-memory frame.
4. Snapshot correctness preserved across CoW root swaps and page-id reuse.

## Goals

1. Freeze Phase-1 API and ownership boundaries for readonly buffering.
2. Define global physical cache key as `(table_id, block_id)`.
3. Define page-id reuse invalidation protocol and API ownership.
4. Define per-table readonly wrapper behavior for `BufferPool` methods.
5. Keep all current read behavior unchanged in this phase.

## Non-Goals

1. No integration into `Table::read_lwc_row`.
2. No integration into `column_block_index` traversal reads.
3. No performance tuning, admission policy, or prefetch work.
4. No metrics API for readonly buffer pool in this phase.
5. No changes to recovery/checkpoint semantics.

## Unsafe Considerations (If Applicable)

Phase 1 is interface/contract-focused and should avoid adding new unsafe blocks where possible.

If this phase touches unsafe-adjacent definitions in buffer internals:
1. Add/refresh `// SAFETY:` comments for any changed unsafe boundary.
2. Add debug assertions for key->frame / reverse-map consistency contracts.
3. Scope changes to type definitions and invariants only; defer new direct-I/O unsafe execution paths to later phases.

Inventory/validation command (if unsafe changes occur):
```bash
cargo test -p doradb-storage --no-default-features
```

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Add readonly pool module surface in `doradb-storage/src/buffer/readonly.rs`.
2. Define global key/state contracts:
   - `ReadonlyCacheKey { table_id, block_id }`
   - key->frame mapping and reverse mapping invariants
   - invalidation API contract for physical page-id reuse
3. Define per-table wrapper contract:
   - `ReadonlyBufferPool` contains table context and delegates to global readonly pool
   - implements `BufferPool`
   - `allocate_page` / `allocate_page_at` are unsupported and panic with explicit guidance
4. Extract reusable utility structs/functions currently duplicated or tightly coupled in `FixedBufferPool` and `EvictableBufferPool` into shared common buffer utilities, then reuse them in readonly pool definitions.
5. Export readonly pool types from `doradb-storage/src/buffer/mod.rs`.
6. Add minimal wiring placeholders where required for later integration (types only, no read-path switch).
7. Add focused unit tests for API behavior and invariants.

## Impacts

1. `doradb-storage/src/buffer/mod.rs`
2. New readonly buffer module file `doradb-storage/src/buffer/readonly.rs`
3. Shared common buffer utility module(s) under `doradb-storage/src/buffer/` for extracted structs/functions reused by `FixedBufferPool`, `EvictableBufferPool`, and readonly pool
4. Potentially `doradb-storage/src/engine.rs`, `doradb-storage/src/catalog/mod.rs`, `doradb-storage/src/table/mod.rs` for type-level placeholders only
5. `doradb-storage/src/file/table_file.rs` for invalidation-hook contract boundary (API-level only in this phase)

Impacted traits/structs:
1. `BufferPool` implementor set (new readonly implementor)
2. readonly key/mapping contract structs
3. per-table readonly pool wrapper struct

## Test Cases

1. Readonly per-table wrapper compiles and satisfies `BufferPool` trait.
2. Unsupported allocation APIs panic with expected message.
3. Cache key equality/hash behavior matches `(table_id, block_id)` semantics.
4. Invalidation API contract tests verify map state transitions at interface level.
5. Regression: existing tests around table/index read paths still pass unchanged.
6. Run:
```bash
cargo test -p doradb-storage --no-default-features
```

## Open Questions

1. Final invalidation trigger placement for reused page ids in later phases: allocation-time, commit-time, or both.
