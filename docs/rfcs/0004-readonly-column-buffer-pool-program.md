---
id: 0004
title: Readonly Column-Store Buffer Pool Program
status: proposal
github_issue: 340
tags: [storage-engine, buffer-pool, io, cow, performance]
created: 2026-02-21
---

# RFC-0004: Readonly Column-Store Buffer Pool Program

Issue tracking:
- RFC issue: [#340](https://github.com/jiangzhe/doradb/issues/340)

## Summary

This RFC proposes a multi-phase program to introduce a readonly buffer pool for table-file column-store reads in `doradb-storage`. The design uses two levels:

1. A global pool that owns memory frames, eviction, direct I/O, and global cache mapping.
2. A per-table pool wrapper that binds table context and directly implements `BufferPool`.

The goal is to replace direct I/O reads on table-file pages in read paths (`LWC` row reads and column-block-index traversal), while preserving CoW snapshot correctness under root swap and page-id reuse.

## Context

Current column-store reads are direct file I/O on each access (`TableFile::read_page`), which causes repeated I/O and misses reuse opportunities. Existing pools in the project are:

- `FixedBufferPool`: fixed in-memory pages without eviction.
- `EvictableBufferPool`: mutable pages with spill/write-back semantics.

Neither directly matches readonly table-file access requirements:

1. No temporary spill file should be used for column-store cache pages.
2. Eviction should be drop-only because disk pages are immutable snapshots (CoW) and can be reloaded.
3. Cache mapping must resolve from on-disk identity to in-memory frame identity.
4. A single global cache should serve all table files.

There is a key tradeoff from table-file CoW behavior:

- For performance, cache should reuse unchanged physical blocks across root swaps.
- For correctness, page-id reuse must not cause stale-cache aliasing.

This RFC adopts physical-key caching and enforces explicit invalidation/reuse invariants.

This change naturally touches multiple subsystems (`buffer`, `table`, `index`, `engine`, `catalog`, recovery wiring), introduces new API surfaces, and requires explicit invariants for unsafe direct-I/O and frame lifecycle; therefore it is RFC scope, not a single task.

## Decision

We will implement readonly buffering as a two-level architecture.

### 1. Two-Level Buffering Model

- `GlobalReadonlyBufferPool`
  - Owns frame/page arena, forward global mapping, inflight read deduplication, and eviction.
  - Performs direct pread into frame memory on cache miss.
  - Applies drop-only eviction (no write-back path).
  - Stores reverse key metadata inline in `BufferFrame` (`table_id`, `block_id`) for frame->key checks during invalidation/reuse.

- Per-table `ReadonlyBufferPool`
  - Binds `(table_id, Arc<TableFile>, &'static GlobalReadonlyBufferPool)`.
  - Implements `BufferPool` for table-local `PageID` access semantics (`PageID == block_id`).
  - Delegates read/load behavior to global pool.

### 2. Cache Key and Snapshot Correctness

Global cache key is physical identity only:

- `ReadonlyCacheKey { table_id, block_id }`

Reason:
- CoW checkpoints typically modify only a small subset of pages.
- Using root-version in cache key would force mass misses on every root swap.
- Physical-key cache preserves hits for unchanged blocks across roots.

`root_version` remains a lookup-layer concept (used while resolving block ids from index/tree roots). It is not part of the buffer-pool API and not part of the global cache key.

### 2.1 Design Rationale: Physical Key vs Triple Key

Two candidate keying strategies were evaluated:

1. Physical key: `(table_id, block_id)`.
2. Strict versioned key: `(table_id, root_version, block_id)`.

Strict versioned keying is simpler for isolation, but in CoW checkpoint workflows it causes a cache miss storm after each root swap, even when only a few pages actually changed. This weakens the main purpose of the readonly pool.

This RFC chooses physical keying to preserve reuse of unchanged blocks across root swaps. The safety tradeoff is handled explicitly through invalidation/reuse invariants:

- cache entry for `(table_id, block_id)` must be invalidated before that page id is reused for new writes;
- page reuse must remain blocked for snapshots that can still observe old content.

This keeps high cache-hit potential without sacrificing snapshot correctness.

### 3. Trait and API Contract

- Per-table `ReadonlyBufferPool` directly implements `BufferPool`.
- `get_page` and `get_child_page` map table-local page id to global physical key `(table_id, block_id)`.
- `allocate_page` and `allocate_page_at` are unsupported for user-facing use and will panic with explicit guidance.
  - Internal frame allocation for miss load is handled by global pool internals only.

### 4. Read Path Integrations

Phase target integrations:

1. `Table::read_lwc_row` path.
2. `index::column_block_index::find_in_file` traversal.

Both paths will use readonly buffer pool rather than direct `TableFile::read_page` per access.

### 5. Lookup/Read Boundary

`RowLocation::LwcPage` remains block-id based. Snapshot semantics are enforced in the lookup stage that resolves the block id from the correct root. After block id is resolved, buffer-pool read uses physical key `(table_id, block_id)`.

## Unsafe Considerations

This RFC introduces/updates unsafe-adjacent code paths in buffer management and direct I/O.

### 1. Unsafe Scope and Boundaries

Unsafe operations are limited to:

- mmap-backed frame/page arena allocation/deallocation.
- Raw pointer access to frame/page memory.
- Direct pread into frame-owned memory pointers.

All unsafe code must stay confined to readonly/global pool internals and utility helpers.

### 2. Required Invariants

The implementation must enforce these invariants:

1. **Frame ownership invariant**:
   - Each frame slot has exactly one logical state at a time (free/loading/resident/evicting).
   - Frame key metadata updates and frame state transitions are atomic with respect to forward cache map updates.

2. **Key mapping invariant**:
   - `disk_to_frame[key] = frame_id` implies `frame_id`'s inline key metadata in `BufferFrame` points to exactly `key`.
   - Key removal and frame reuse happen in one critical transition.

3. **IO lifetime invariant**:
   - Frame memory pointer used by pread remains valid and not reassigned until I/O completion.
   - Inflight dedup map guarantees at most one active loader per key.

4. **Latch/guard invariant**:
   - A frame cannot be exposed to readers before load completion and state publication.
   - Eviction cannot reclaim a frame while a guard holds required protection.

5. **Physical-key reuse and correctness invariant**:
   - Cache lookup is keyed by `(table_id, block_id)` to maximize reuse across root swaps.
   - If a physical page id is reused for a new write, cache entry for that `(table_id, block_id)` must be invalidated before new root visibility.
   - Reuse protocol must remain compatible with snapshot safety: allocator/GC must not reuse a page still visible to active snapshots.

### 3. Unsafe Documentation Requirements

- Every unsafe block/function must include `// SAFETY:` comments with concrete preconditions.
- Key transitions must include debug assertions for mapping/state consistency.
- New unsafe code follows:
  - `docs/unsafe-usage-principles.md`
  - `docs/process/unsafe-review-checklist.md`

### 4. Validation Strategy

Validation includes:

- Unit tests for mapping/state transitions, dedup, eviction/reload, stale-generation behavior.
- Integration tests for:
  - root-swap coexistence and old/new snapshot reads,
  - cache-hit reuse across small CoW root swaps,
  - correctness under page-id reuse with explicit invalidation.
- Existing crate tests in both feature modes:
  - `cargo test -p doradb-storage`
  - `cargo test -p doradb-storage --no-default-features`

## Implementation Phases

- **Phase 1: Interfaces and Correctness Contracts**

Scope:
- Define new readonly pool types and ownership boundaries.
- Define physical cache key structure and page-id reuse invalidation protocol.
- Define API contract for per-table wrapper directly implementing `BufferPool`.
- Define lookup/read boundary contract (snapshot semantics in lookup stage, physical-key read in pool stage).

Goals:
- Freeze interfaces and invariants before implementation.
- Ensure no ambiguous behavior on versioning and unsupported allocation methods.

Non-goals:
- No data-path integration yet.

- **Phase 2: Global Readonly Pool Core**

Scope:
- Implement `GlobalReadonlyBufferPool` core: frame arena, forward key mapping + inline frame key metadata, inflight dedup, direct pread miss load, drop-only eviction.
- Implement explicit invalidation API for reused physical page ids.
- Add focused unit tests for concurrency and state transitions.

Goals:
- Provide stable, tested global cache mechanics with correctness invariants.

Non-goals:
- No table/index integration yet.

- **Phase 3: Per-Table Wrapper and LWC Read Integration**

Scope:
- Implement per-table `ReadonlyBufferPool` wrapper.
- Wire into `Table` as disk-side pool.
- Integrate `Table::read_lwc_row` path.

Goals:
- Replace LWC direct reads with cached reads.
- Keep row-store mutable pool behavior unchanged.

Non-goals:
- No column-block-index traversal integration yet.

- **Phase 4: Column-Block-Index Integration and Lookup Boundary Validation**

Scope:
- Integrate readonly pool into `column_block_index` file traversal.
- Validate block-index lookup stage and readonly read stage boundary: resolved block ids remain snapshot-correct while reads use physical-key cache.
- Ensure old/new roots can coexist in cache.
- Verify unchanged blocks remain cache-hit across root swaps.

Goals:
- Eliminate direct file reads in target column-block-index path.
- Prove root-swap correctness.

Non-goals:
- No broad performance tuning/cleanup yet.

- **Phase 5: Hardening, Performance, and Cleanup**

Scope:
- Stress/race tests, targeted benchmarks, and low-risk utility extraction for shared buffer helpers.
- Final documentation alignment and acceptance validation.

Goals:
- Demonstrate measurable read-path improvement and no correctness regressions.
- Leave implementation maintainable for future extensions.

Non-goals:
- No new features beyond readonly caching program.

## Consequences

### Positive

- Reduces repeated direct I/O for column-store read paths.
- Introduces shared global readonly cache across tables.
- Preserves CoW snapshot correctness via lookup-stage root semantics and page-reuse invalidation.
- Preserves cache locality across root swaps for unchanged physical blocks.
- Keeps mutable and readonly buffering responsibilities separated.

### Negative

- Adds architectural complexity (global + per-table pool layers).
- Requires clear lookup/read boundary and storage-page reuse invalidation hooks.
- Introduces new unsafe-sensitive direct I/O and frame-state machinery requiring strict discipline.

## Open Questions

1. Where is the most robust invalidation hook for page-id reuse: allocation time in `MutableTableFile::allocate_page_id`, commit-time root publication, or both?
2. Should initial eviction policy be pure clock-sweep, or include access-frequency hints from read paths?
3. Do we need runtime metrics APIs for readonly pool hit/miss/eviction to aid tuning and regression detection?

## Future Work

- Optional prefetch interfaces for scan-heavy paths.
- Unified telemetry framework across fixed/evictable/readonly pools.
- Potential follow-up on cache admission policy and scan-resistance tuning.

## References

- `docs/architecture.md`
- `docs/index-design.md`
- `docs/checkpoint-and-recovery.md`
- `docs/table-file.md`
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`
