# Task: Readonly Buffer Pool Harden Perf Cleanup

## Summary

Implement Phase 5 (final phase) of RFC-0004 (`docs/rfcs/0004-readonly-column-buffer-pool-program.md`, issue `#340`) to harden readonly buffer-pool behavior, add targeted performance coverage, and clean up readonly integration code paths.

This task focuses on:
1. readonly-path hardening and small internal cleanups;
2. extraction of a shared evictor framework for mutable and readonly pools;
3. stress/race and root-swap validation tests;
4. reproducible readonly-path benchmark coverage;
5. a new buffer-pool design document covering fixed/evictable/readonly pools.

## Context

Phases 1-4 are already landed:
1. readonly global/per-table pool exists and serves LWC reads;
2. block index has been refactored into `RowBlockIndex`, `ColumnBlockIndex`, and `BlockIndexRoot`;
3. column block-index traversal uses readonly buffer-pool pages.

Remaining gaps are final-phase quality gaps:
1. readonly miss/load path still has panic-based behavior in some runtime paths;
2. evictor logic remains duplicated across `EvictableBufferPool` and `GlobalReadonlyBufferPool`;
3. current eviction control is mostly static and lacks pressure-aware trigger/stop behavior and dynamic-batch tuning;
4. stress/race validation around checkpoint root-swap + readonly reads is still limited;
5. there is no dedicated readonly benchmark for cold/warm read behavior;
6. there is no centralized design doc for all three buffer-pool implementations.

Decision from design review:
1. error-policy boundaries that are not finalized in this task should use `todo!()`;
2. broad/general IO error policy is deferred to a future task.
3. readonly counters are deferred to a future task.

## Goals

1. Harden readonly buffer-pool runtime behavior and remove avoidable panic-only miss/load handling inside readonly-specific APIs.
2. Extract a shared evictor framework used by both `EvictableBufferPool` and `GlobalReadonlyBufferPool`.
3. Add enhanced eviction controls in shared evictor:
   - dynamic eviction batch size;
   - pressure-delta trigger/stop strategy:
     - trigger when free frames drop below target free, or allocation-failure rate crosses threshold;
     - stop when free frames recover above target plus hysteresis, and failure rate returns below threshold.
4. Add end-to-end stress/race tests for readonly reads under checkpoint/root updates.
5. Add dedicated readonly benchmark coverage (cold and warm phases).
6. Add a new doc under `docs/` describing generic buffer-pool architecture and all three implementations.
7. Keep current architecture and public integration shape from Phase 4.
8. Replace unresolved policy branches with explicit `todo!()` instead of silent fallback.

## Non-Goals

1. No `BufferPool` trait redesign.
2. No broad cross-module IO error handling policy redesign.
3. No checkpoint/recovery protocol redesign.
4. No new cache admission/prefetch feature.
5. No counter/telemetry API in this task; metrics are deferred to a future task.

## Unsafe Considerations (If Applicable)

This task touches unsafe-adjacent code in readonly pool internals:
1. `doradb-storage/src/buffer/readonly.rs` miss-load path that reads into frame-owned memory;
2. `doradb-storage/src/file/mod.rs` and `doradb-storage/src/file/table_file.rs` static-read boundary used by readonly miss loads.
3. shared eviction internals touched by both mutable and readonly pools.

Required safeguards:
1. preserve existing frame ownership and key-mapping invariants during miss/load/evict/invalidate transitions;
2. keep `// SAFETY:` comments accurate for pointer validity, alignment, and lifetime assumptions;
3. keep publication ordering so no reader can observe partially loaded frames;
4. preserve latch/guard safety across shared-evictor extraction and new pressure-delta trigger/stop logic.

Validation scope:
```bash
cargo test -p doradb-storage --no-default-features
cargo test -p doradb-storage
```

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Refactor readonly miss/load flow to return recoverable `Result` in readonly-specific methods:
   - `GlobalReadonlyBufferPool` and `ReadonlyBufferPool` internal/read helpers.
2. Propagate readonly read errors through direct readonly call paths:
   - `Table::read_lwc_row` in `doradb-storage/src/table/mod.rs`;
   - traversal read path in `doradb-storage/src/index/column_block_index.rs`.
3. Apply agreed policy at unresolved boundary:
   - where current behavior silently maps readonly errors to `NotFound`, replace with explicit `todo!()` and defer policy finalization to a future task.
4. Extract shared evictor components from:
   - `doradb-storage/src/buffer/evict.rs`
   - `doradb-storage/src/buffer/readonly.rs`
   into common buffer module(s) (for example `doradb-storage/src/buffer/util.rs` and/or new helper modules).
5. Implement enhanced shared evictor behavior:
   - dynamic batch size based on pressure/resident/free state;
   - pressure-delta trigger/stop:
     - trigger when `free_frames < target_free` or recent allocation-failure rate exceeds threshold;
     - stop when `free_frames >= target_free + hysteresis` and failure rate drops below threshold.
   - expose strategy tuning through pool configuration surfaces for evictable and readonly pools:
     - `target_free`
     - `hysteresis`
     - failure-rate threshold/window
     - dynamic-batch bounds
6. Apply low-risk internal cleanup in readonly pool hot path:
   - reduce redundant map/state checks while preserving invariants.
7. Add stress/race tests:
   - concurrent readonly reads with checkpoint/root updates;
   - cache reuse across root swap for unchanged blocks.
8. Add readonly-path benchmark example (or extend existing benchmark):
   - cold first-pass reads and warm repeated reads;
   - report throughput/latency summary.
9. Add new documentation file `docs/buffer-pool.md` for unified buffer-pool design:
   - common abstractions/invariants;
   - `FixedBufferPool` design and lifecycle;
   - `EvictableBufferPool` design and eviction/writeback flow;
   - `GlobalReadonlyBufferPool` + per-table `ReadonlyBufferPool` design and readonly eviction flow;
   - where implementations share components and where they differ.
10. Update inline comments/doc comments for touched buffer/readonly/block-index paths where clarity improved.

## Impacts

Primary files:
1. `doradb-storage/src/buffer/readonly.rs`
2. `doradb-storage/src/buffer/evict.rs`
3. `doradb-storage/src/buffer/util.rs` and/or new shared-evictor helper module(s) under `doradb-storage/src/buffer/`
4. `doradb-storage/src/table/mod.rs`
5. `doradb-storage/src/index/column_block_index.rs`
6. `doradb-storage/src/index/block_index.rs`
7. `doradb-storage/src/table/tests.rs`
8. `doradb-storage/examples/bench_block_index.rs` or a new readonly benchmark file under `doradb-storage/examples/`
9. new design doc under `docs/`: `docs/buffer-pool.md`

Primary structs/functions:
1. `GlobalReadonlyBufferPool`
2. `ReadonlyBufferPool`
3. shared evictor structs/functions used by readonly and evictable pools
4. `Table::read_lwc_row`
5. `ColumnBlockIndex::find` and related traversal helpers
6. `BlockIndex` column-path error branch (explicit `todo!()` boundary)
7. `EvictableBufferPool` evictor integration points

## Test Cases

1. Readonly miss-load success path returns data correctly without panic/regression.
2. Readonly miss-load failure path returns error on readonly-specific APIs.
3. Shared evictor pressure-delta trigger/stop behavior:
   - eviction starts when `free_frames < target_free` or failure-rate threshold is crossed;
   - eviction stops when `free_frames >= target_free + hysteresis` and failure rate recovers.
4. Shared evictor dynamic batch behavior adjusts under pressure and remains stable.
5. Strategy configuration is applied correctly for both evictable and readonly pools.
6. Concurrent readonly reads with checkpoint/root updates keep lookup correctness.
7. Root-swap scenario preserves cache reuse for unchanged physical blocks.
8. Existing block-index/table read-path regressions remain green.
9. Existing evictable-pool behavior and tests remain green after shared-evictor extraction.
10. Full validation:
```bash
cargo test -p doradb-storage --no-default-features
cargo test -p doradb-storage
```
11. Run readonly benchmark and capture cold/warm delta for acceptance evidence.

## Open Questions

1. General cross-module IO error handling policy is deferred to a future task.
2. Counter/telemetry exposure is deferred to a future task.
