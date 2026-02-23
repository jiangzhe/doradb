# Buffer Pool Design

This document describes the storage buffer-pool architecture in `doradb-storage` and the three concrete implementations:

1. `FixedBufferPool`
2. `EvictableBufferPool`
3. `GlobalReadonlyBufferPool` + per-table `ReadonlyBufferPool`

## Common Model

All pool implementations share these core ideas:

1. **Frame + page split**
   - `BufferFrame` stores latch, generation, frame kind/state, and metadata.
   - `Page` stores page bytes.
2. **Guard-based access**
   - `FacadePageGuard`, `PageSharedGuard`, `PageExclusiveGuard`.
   - Optimistic/shared/exclusive latching follows `HybridLatch` semantics.
3. **Version safety**
   - Frame generation + latch validation prevent stale optimistic readers from observing reused frame content.
4. **Clock-sweep primitives**
   - shared helpers in `buffer/util.rs` implement ordered clock traversal and candidate selection.

## FixedBufferPool

`FixedBufferPool` is a non-evicting metadata/index pool.

Characteristics:

1. In-memory only.
2. No spill/writeback lifecycle.
3. Stable residency once allocated.

Typical usage:

1. metadata pages
2. index structure pages

## EvictableBufferPool

`EvictableBufferPool` is the mutable data pool.

Characteristics:

1. Supports page allocation/deallocation.
2. Supports read/write IO on backing sparse file.
3. Supports dirty-page writeback and drop from memory.

Key components:

1. `InMemPageSet` tracks resident page count and clock candidate set.
2. `InflightIO` deduplicates read/write transitions and synchronizes readers/writers.
3. `BufferPoolEvictor` runs clock-sweep + writeback/drop workflow.

### Eviction Strategy

Eviction behavior is controlled by shared `EvictionArbiter`:

1. `target_free`
2. `hysteresis`
3. `failure_rate_threshold`
4. `failure_window`
5. `min_batch` / `max_batch`

Decision rule:

1. Trigger when `free_frames < target_free` OR allocation-failure rate reaches threshold.
2. Stop when `free_frames >= target_free + hysteresis` AND failure-rate drops below threshold.
3. Dynamic batch size scales with pressure and stays inside configured bounds.

## GlobalReadonlyBufferPool + ReadonlyBufferPool

Readonly caching uses a two-level model:

1. `GlobalReadonlyBufferPool`
   - owns frame/page arena
   - owns global cache map and inflight miss-load dedup
   - owns drop-only eviction thread
2. `ReadonlyBufferPool`
   - per-table wrapper implementing `BufferPool`
   - maps table-local block id to global physical cache key

### Cache Key

Readonly key is physical identity:

`ReadonlyCacheKey { table_id, block_id }`

This preserves cache hits across root swaps when physical blocks are unchanged.

### Miss/Load and Error Flow

1. miss -> reserve free frame -> read table file page into frame memory
2. publish mapping and frame metadata
3. return guards from global frame arena

Readonly-specific accessors return `Result` for recoverable miss/load errors.  
The generic `BufferPool` trait boundary still has deferred error policy and uses explicit `todo!()` for unresolved mapping decisions.

### Readonly Eviction

Readonly eviction is drop-only:

1. no writeback path
2. remove mapping
3. clear frame metadata
4. mark memory `MADV_DONTNEED`
5. return frame to free list

It uses the same shared pressure-delta decision logic and tuning fields as `EvictableBufferPool`.

## Shared vs Different Responsibilities

Shared:

1. frame/page memory model
2. latch/guard semantics
3. clock-sweep primitives
4. pressure-aware eviction policy/tuning

Different:

1. mutable pool has writeback + allocation lifecycle
2. readonly pool has physical-key mapping + drop-only eviction
3. fixed pool has no eviction/IO path
