---
id: 000102
title: Separate readonly pool interface and simplify ownership
status: implemented  # proposal | implemented | superseded
created: 2026-03-31
github_issue: 511
---

# Task: Separate readonly pool interface and simplify ownership

## Summary

Refactor the readonly buffer-pool path so `ReadonlyBufferPool` no longer
implements the mutable `BufferPool` contract, expose a readonly-specific
immutable persisted-block read API around `ReadonlyBlockGuard` while keeping
caller-owned `PoolGuard` provenance explicit, and simplify
`GlobalReadonlyBufferPool` ownership by removing redundant outer `Arc` wrappers
from fields already protected by quiescent pool guards. Keep the current
shared-lock implementation internally in this task, document the narrow raw
read use case, and leave readonly synchronization changes behind the new API
boundary instead of the mutable `BufferPool` trait surface.

## Context

The current readonly path has two design mismatches:

1. `ReadonlyBufferPool` is a per-file persisted-page reader, but it currently
   implements `BufferPool`, including allocation, deallocation, and versioned
   lookup methods that it cannot satisfy semantically.
2. `GlobalReadonlyBufferPool` already has guarded owner/runtime split semantics,
   but its runtime still clones selected sub-objects through outer `Arc`
   wrappers instead of treating guarded pool ownership as the single lifetime
   root.

In practice, readonly consumers already use the concrete readonly type and
persisted-page validators rather than participating in generic mutable
`P: BufferPool` algorithms. The main direct raw-read call path is the CoW root
loader in `doradb-storage/src/file/cow_file.rs`, while other readers such as
LWC, column-block-index, and deletion-blob readers already rely on
readonly-specific validation helpers.

This task was originally designed with
`docs/backlogs/closed/000040-readonly-buffer-pool-shared-lock-elision-for-immutable-pages.md`
in mind. During resolve, that follow-up was closed as `wontfix`: readonly
eviction can reclaim and zero a resident frame, so removing the internal shared
latch would first require a separate pin/lifetime mechanism. This task
therefore keeps the existing shared-latch implementation and narrows only the
public readonly interface.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

## Goals

1. Remove `ReadonlyBufferPool` from the mutable `BufferPool` trait surface.
2. Introduce a readonly-specific immutable persisted-block read API and guard
   while keeping caller-owned `PoolGuard` provenance explicit on readonly call
   paths.
3. Keep persisted-page invalidation and page-kind validation as first-class
   readonly operations.
4. Refactor `ReadonlyRuntime` to hold `ArenaGuard` plus
   `SyncQuiescentGuard<GlobalReadonlyBufferPool>` and dereference pool state
   through that guarded owner.
5. Remove redundant outer `Arc` wrappers from
   `GlobalReadonlyBufferPool.{mappings,inflight_loads,residency}` while
   preserving detached miss-load and eviction lifetimes.
6. Preserve current behavior for miss deduplication, validation-before-publish,
   invalidation, drop-only eviction, and owner-drop ordering.
7. Establish a readonly guard boundary that preserves room for future internal
   synchronization changes without re-exposing mutable `BufferPool` behavior.

## Non-Goals

1. Redesigning `BufferPool` into capability traits for all mutable and readonly
   pools.
2. Changing mutable pool behavior or generic mutable `P: BufferPool`
   algorithms.
3. Removing the final `shutdown_flag: Arc<AtomicBool>` dependency from
   `GlobalReadonlyBufferPool`; shared `Evictor` cleanup is outside this task.
4. Implementing actual shared-lock elision, lock-free immutable-page borrows,
   or a new readonly pin/lifetime mechanism.
5. Changing persisted page formats, page-integrity envelopes, or table-file
   recovery/checkpoint logic beyond the API fallout of the readonly cleanup.
6. Broad refactoring of `buffer/guard.rs` or shared eviction infrastructure
   unrelated to the readonly ownership cleanup.

## Unsafe Considerations (If Applicable)

This task touches unsafe-adjacent buffer lifetime code and must preserve the
existing owner/guard safety contracts even if it does not intentionally add
net-new unsafe operations.

1. Expected affected paths:
   - `doradb-storage/src/buffer/readonly.rs`
   - `doradb-storage/src/buffer/arena.rs`
   - `doradb-storage/src/buffer/guard.rs`
   - `doradb-storage/src/buffer/evictor.rs` if any helper signature fallout is
     required
2. Required invariants:
   - any readonly block guard returned from the new public API must retain the
     pool/frame keepalive for the full borrowed-byte lifetime;
   - removing outer `Arc` wrappers from readonly-pool fields must not weaken
     detached `ReadSubmission`, page-reservation, or evictor-thread lifetime
     guarantees because guarded pool ownership remains the single root;
   - eviction, invalidation, and reload paths must still clear persisted-block
     mappings before frame reuse and preserve generation/provenance checks
     around stale resident references;
   - no new public `unsafe fn` is introduced; if any existing unsafe-adjacent
     helper changes, adjacent `// SAFETY:` comments and invariant assertions
     must be updated to match the new guard-owned lifetime story.
3. Refresh inventory and run:

```bash
tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md
cargo clippy -p doradb-storage --all-targets -- -D warnings
cargo nextest run -p doradb-storage
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Separate readonly API from mutable `BufferPool` in
   `doradb-storage/src/buffer/readonly.rs` and `doradb-storage/src/buffer/mod.rs`.
   - Remove `impl BufferPool for ReadonlyBufferPool`.
   - Keep `GlobalReadonlyBufferPool` as the engine-owned cache component with
     its existing `capacity`, `allocated`, and `pool_guard` component-facing
     API.
   - Remove or demote readonly public methods whose only purpose was to mimic
     mutable pool access, especially public guard-based access helpers.

2. Introduce a readonly persisted-block guard and immutable read API in
   `doradb-storage/src/buffer/readonly.rs`.
   - Add a readonly-specific wrapper type named `ReadonlyBlockGuard`.
   - `ReadonlyBlockGuard` wraps the current internal resident-page guard in
     this task and exposes only immutable accessors:
     - `page(&self) -> &Page`
     - `block_id(&self) -> PageID`
   - Add the new public readonly methods on `ReadonlyBufferPool`:
     - `read_block(&self, guard: &PoolGuard, block_id: PageID) -> Result<ReadonlyBlockGuard>`
     - `read_validated_block(&self, guard: &PoolGuard, block_id: PageID, validator: ReadonlyPageValidator) -> Result<ReadonlyBlockGuard>`
   - Keep `persisted_file_kind()`, `invalidate_block_id()`, and
     `invalidate_block_id_strict()` as readonly-specific metadata/invalidation
     operations, and keep `pool_guard()` available for outer callers that need
     an explicit readonly guard source.
   - Implement the new methods on top of the existing internal
     load/dedup/validation machinery so task scope stays narrow.

3. Migrate readonly consumers to immutable persisted-block semantics.
   - Update `doradb-storage/src/file/cow_file.rs` so
     `CowFile::load_active_root_from_pool()` uses `invalidate_block_id()` plus
     the new immutable block reads while preserving its explicit caller-owned
     guard.
   - Update persisted readers such as:
     - `doradb-storage/src/lwc/page.rs`
     - `doradb-storage/src/index/column_block_index.rs`
     - `doradb-storage/src/index/column_deletion_blob.rs`
     - readonly page tests in `doradb-storage/src/file/table_file.rs`
     to use `read_validated_block()` and the returned immutable guard rather
     than `PageSharedGuard<Page>`.
   - Thread `PoolGuards::disk_guard()` or `ReadonlyBufferPool::pool_guard()`
     from outer call sites instead of reacquiring readonly guards inside the
     read helpers.
   - Remove readonly-only `BufferPool` imports from modules that no longer need
     the trait in scope.

4. Simplify `GlobalReadonlyBufferPool` field ownership in
   `doradb-storage/src/buffer/readonly.rs`.
   - Convert these fields from outer `Arc<...>` wrappers to plain owned fields:
     - `mappings: DashMap<PersistedBlockKey, PageID>`
     - `inflight_loads: DashMap<PersistedBlockKey, Arc<PageIOCompletion>>`
     - `residency: ReadonlyResidency`
   - Keep `shutdown_flag: Arc<AtomicBool>` unchanged in this task because the
     shared `Evictor` still consumes shutdown state as a standalone `Arc`.
   - Update constructor and all helper methods to use direct field access
     through guarded pool ownership.

5. Refactor readonly runtime ownership to guard-based access.
   - Change `ReadonlyRuntime` to hold:
     - `arena: ArenaGuard`
     - `pool: SyncQuiescentGuard<GlobalReadonlyBufferPool>`
   - Remove runtime-owned clones of `mappings` and `residency`; runtime methods
     should dereference `self.pool` for mapping removal, resident-set access,
     and progress notification.
   - Keep `ReadonlyPageReservation` and `ReadSubmission` on guarded pool
     ownership so detached miss loads still complete correctly after wrapper
     drops.

6. Update tests to the new readonly surface.
   - Remove readonly tests that assert `allocate_page` panics because readonly
     no longer implements `BufferPool`.
   - Rewrite raw/validated read tests to use `read_block()` and
     `read_validated_block()`.
   - Keep coverage for:
     - miss deduplication;
     - cancelled loader behavior;
     - detached miss-load completion after pool drop;
     - detached reserve-waiter unblock on owner drop;
     - validation failure not publishing corrupted mappings;
     - drop-only eviction and reload.
   - Add one focused test assertion that the new readonly API returns the new
     immutable guard type rather than exposing latch/`PoolGuard` types to
     consumers.

7. Preserve future lock-elision maneuvering room.
   - The new public readonly API must not expose `PageSharedGuard<Page>`.
   - Caller guard provenance remains explicit via `&PoolGuard` even though
     readonly callers no longer access the mutable `BufferPool` trait surface.
   - Internal implementation may still use current shared-latch mechanics in
     this task.
   - Future readonly synchronization work can change `ReadonlyBlockGuard`
     internals without restoring mutable `BufferPool` behavior to readonly
     consumers.

## Implementation Notes

1. Split readonly access from the mutable pool trait surface:
   - removed `impl BufferPool for ReadonlyBufferPool`;
   - added `ReadonlyBlockGuard` and readonly-specific `read_block(...)` /
     `read_validated_block(...)` entrypoints;
   - kept `ReadonlyBufferPool::pool_guard()` public, and the final shipped API
     requires callers to pass `&PoolGuard` explicitly for readonly reads.
2. Simplified readonly ownership and runtime structure:
   - `GlobalReadonlyBufferPool` now owns `mappings`, `inflight_loads`, and
     `residency` directly without outer `Arc` wrappers;
   - `ReadonlyRuntime` now dereferences readonly state through
     `SyncQuiescentGuard<GlobalReadonlyBufferPool>` plus `ArenaGuard`;
   - detached miss loads, page reservations, and eviction still complete
     through guarded pool ownership.
3. Migrated readonly consumers to the immutable block API:
   - `CowFile` active-root loading now uses the readonly block API and keeps
     root/meta validation at the call site;
   - persisted LWC, column-block-index, deletion-blob, table, catalog, and
     recovery/session paths now read readonly pages through immutable guards and
     explicit disk-pool guard threading.
4. Implementation review adjusted the final public shape:
   - readonly reads keep caller-owned guard provenance instead of reacquiring
     `global.pool_guard()` internally;
   - raw `read_block()` is now documented as a narrow COW root/meta-page helper
     and future expansion is explicitly cautioned;
   - the resident-hit validation-failure invalidation path keeps synchronous
     invalidation, with comments documenting why the current runtime use does
     not make that a production async-executor issue.
5. Related follow-up review outcome:
   - backlog `000040` was closed as `wontfix` during resolve after verifying
     that removing the readonly shared lock would require a new pin/lifetime
     mechanism to keep resident pages safe against eviction and frame reuse.
6. Verification executed:
   - `cargo fmt --all`
   - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
   - `cargo nextest run -p doradb-storage`
   - `cargo nextest run -p doradb-storage --no-default-features --features libaio`
7. Review/traceability outcome:
   - task issue: `#511`
   - implementation PR: `#512`
   - RFC linkage check during resolve reported no parent RFC for this task.

## Impacts

- `doradb-storage/src/buffer/mod.rs`
  - readonly exports and trait-surface cleanup.
- `doradb-storage/src/buffer/readonly.rs`
  - readonly API split, guard wrapper, ownership cleanup, runtime refactor, and
    test updates.
- `doradb-storage/src/file/cow_file.rs`
  - CoW root loader migration from guard-based page fetch to immutable block
    reads.
- `doradb-storage/src/lwc/page.rs`
  - validated persisted LWC reads through the new readonly block guard.
- `doradb-storage/src/index/column_block_index.rs`
  - validated persisted column-index node reads through the new readonly block
    guard.
- `doradb-storage/src/index/column_deletion_blob.rs`
  - validated persisted blob reads through the new readonly block guard.
- `doradb-storage/src/file/table_file.rs`
  - readonly page-access tests updated to the new immutable-read API.
- `docs/backlogs/closed/000040-readonly-buffer-pool-shared-lock-elision-for-immutable-pages.md`
  - related follow-up reviewed during resolve and archived as `wontfix`.

## Test Cases

1. `read_block(&PoolGuard, ...)` reads persisted super/meta/data pages
   correctly after explicit invalidation and still supports the CoW
   active-root loader flow.
2. `read_validated_block(&PoolGuard, ...)` accepts valid LWC pages,
   column-block-index pages, and deletion-blob pages, and rejects corruption
   without publishing a bad mapping.
3. Concurrent readonly cache misses still deduplicate to one inflight load and
   joined waiters receive the same terminal result.
4. Cancelled loaders and detached miss loads still complete correctly after
   wrapper drops, and inflight entries do not leak after completion.
5. Owner drop still unblocks reserve waiters, and drop-only eviction still
   invalidates mappings and supports later reload.
6. Removing outer `Arc` wrappers from readonly-pool fields does not change the
   observable behavior of existing readonly lifetime and invalidation tests.
7. Supported validation:

```bash
cargo nextest run -p doradb-storage
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

## Open Questions

1. If future readonly callers need raw `read_block()` outside the current COW
   super/meta-page flow, should inflight-load joining start distinguishing
   validated and unvalidated reads?
2. If readonly lock elision becomes interesting again later, should it be
   revisited only after a dedicated resident-page pin/lifetime mechanism is
   designed?
3. If later cleanup wants to remove the final `shutdown_flag` field-level
   `Arc`, treat that as shared `Evictor`/worker-runner refactoring rather than
   broadening this readonly task.
