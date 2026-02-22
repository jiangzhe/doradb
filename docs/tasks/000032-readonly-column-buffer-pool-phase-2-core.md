# Task: Readonly Column Buffer Pool Phase 2 Core

## Summary

Implement Phase 2 of RFC-0004 (`docs/rfcs/0004-readonly-column-buffer-pool-program.md`, issue `#340`) by building the global readonly buffer pool core.

This phase delivers:
1. cache-miss load into buffer frames via existing `TableFileSystem` async IO pipeline;
2. inflight read deduplication per `ReadonlyCacheKey`;
3. drop-only eviction with evictor thread;
4. shared eviction policy utilities extracted into `doradb-storage/src/buffer/util.rs`;
5. explicit lifecycle ordering: readonly pool memory must be deallocated only after file-system IO driver thread exits.

## Context

Phase 1 introduced readonly pool interfaces and mapping contracts in `doradb-storage/src/buffer/readonly.rs`, including:
1. global forward map `(table_id, block_id) -> frame_id`;
2. inline reverse key metadata in `BufferFrame`;
3. invalidation interfaces.

Current limitation:
1. `ReadonlyBufferPool::get_page` panics on cache miss because miss-load path is not implemented.

RFC-0004 Phase 2 requires implementing the global cache mechanics while deferring read-path integrations (Phase 3 and Phase 4).

Design decisions fixed in review:
1. reuse existing async IO from `TableFileSystem`/`TableFile` (`AIOClient<FileIO>`) rather than introducing a separate readonly IO driver;
2. share eviction policy machinery with `EvictableBufferPool` by extracting common logic;
3. enforce shutdown ordering between `TableFileSystem` and readonly global buffer pool;
4. panic on miss-load IO failure for this phase (improvements deferred).

## Goals

1. Implement cache miss load in `GlobalReadonlyBufferPool`.
2. Deduplicate concurrent loads of the same `ReadonlyCacheKey`.
3. Implement drop-only eviction with background evictor thread.
4. Share eviction policy structs/functions between evictable and readonly pools via `buffer/util.rs`.
5. Preserve mapping/frame-state invariants (forward map + inline frame key metadata).
6. Keep readonly invalidation APIs compatible with physical page-id reuse protocol.
7. Define and validate lifetime contract: file IO driver shutdown happens before readonly frame/page arena deallocation.

## Non-Goals

1. No integration into `Table::read_lwc_row`.
2. No integration into `index::column_block_index::find_in_file`.
3. No metrics API.
4. No API redesign of `BufferPool` error model.
5. No recovery/checkpoint semantic changes beyond existing invalidation hooks.
6. No retry/error-propagation policy for miss-load failure (panic is accepted in this phase).

## Unsafe Considerations (If Applicable)

Affected unsafe-adjacent paths:
1. Direct read into frame-owned page memory using file async IO (`file` module and `TableFile` helper).
2. Frame/page raw pointers in readonly buffer pool and eviction flow.

Required safeguards:
1. Add `// SAFETY:` comments for new unsafe call sites and pointer lifetime assumptions.
2. Ensure frame memory remains valid until IO completion.
3. Ensure frame state publication ordering prevents readers from observing partially loaded pages.
4. Keep debug assertions for map/frame-key consistency on bind/evict/invalidate transitions.
5. Enforce drop order so no driver-thread IO can touch freed readonly arena memory.

Validation scope:
```bash
cargo test -p doradb-storage --no-default-features
```

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Extend file IO request model in `doradb-storage/src/file/mod.rs` so async read can target readonly frame page memory directly.
2. Add `TableFile` helper in `doradb-storage/src/file/table_file.rs` to submit/wait that read request using existing `AIOClient<FileIO>`.
3. Implement readonly inflight dedup map keyed by `ReadonlyCacheKey` in `doradb-storage/src/buffer/readonly.rs`.
4. Implement miss path in `GlobalReadonlyBufferPool`:
   - lookup key in forward map;
   - if miss, coordinate single loader via inflight map;
   - reserve/select victim frame;
   - async read page into frame memory;
   - publish frame state and mapping.
5. Extract reusable eviction policy structures/functions from `doradb-storage/src/buffer/evict.rs` into `doradb-storage/src/buffer/util.rs`.
6. Implement readonly evictor thread using shared eviction policy with drop-only semantics (no writes).
7. Keep invalidation APIs and mapping invariants aligned with Phase 1 contracts.
8. Add teardown/lifecycle checks or tests ensuring readonly pool is dropped after `TableFileSystem` driver thread has stopped.

## Impacts

Primary files:
1. `doradb-storage/src/buffer/readonly.rs`
2. `doradb-storage/src/buffer/evict.rs`
3. `doradb-storage/src/buffer/util.rs`
4. `doradb-storage/src/file/mod.rs`
5. `doradb-storage/src/file/table_file.rs`
6. `doradb-storage/src/engine.rs` (if lifecycle wiring/update is needed for ordering guarantees)

Key structs/functions:
1. `GlobalReadonlyBufferPool`
2. readonly inflight-load state and dedup structures
3. shared eviction policy helpers extracted from evictable pool
4. `FileIO` request/complete flow for frame-memory reads
5. `TableFile` async read helper for readonly frame target memory

## Test Cases

1. Single-thread cache miss loads page and publishes mapping correctly.
2. Concurrent same-key miss triggers one IO load and all waiters observe loaded page.
3. Concurrent different-key misses work without map/frame corruption.
4. Eviction drops pages without write-back and allows reload on next miss.
5. Invalidation clears forward mapping and inline frame key metadata.
6. Frame-generation/state transitions remain consistent under concurrent get/evict/invalidate operations.
7. Lifecycle ordering test validates no IO on freed readonly frame memory during teardown.
8. Full regression:
```bash
cargo test -p doradb-storage --no-default-features
```

## Open Questions

1. Should miss-load IO failures remain panic in Phase 3+, or move to an explicit recovery/retry strategy via API changes?
2. Should shared eviction utilities be further generalized for future policy tuning (scan resistance, admission control) in a follow-up task?
