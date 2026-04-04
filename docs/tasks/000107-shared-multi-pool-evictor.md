---
id: 000107
title: Add Shared Multi-Pool Evictor
status: implemented  # proposal | implemented | superseded
created: 2026-04-04
github_issue: 524
---

# Task: Add Shared Multi-Pool Evictor

## Summary

Implement Phase 3 of RFC-0013 by replacing the three production evictor-owner
components with one shared evictor thread for the global readonly pool,
`mem_pool`, and `index_pool`. The shared runner must preserve one
policy/clock-hand per pool, keep readonly drop-only and mutable writeback
behavior unchanged, and avoid making eviction more aggressive than the current
per-pool design. The buffer pools remain high-performance IO caches first; the
shared-thread refactor must not expand effective eviction pressure or cause one
pool to evict only because another pool is under pressure.

## Context

RFC-0013 selected one shared evictor thread across the global readonly pool,
`mem_pool`, and `index_pool` as Phase 3 of the shared storage runtime program.
Phase 1 (`000105`) and Phase 2 (`000106`) are already implemented. The current
post-Phase-2 topology now has one shared storage IO worker under `FileSystem`
and `FileSystemWorkers`, but eviction ownership is still fragmented:

1. `DiskPoolWorkers` starts one readonly evictor thread.
2. `MemPoolWorkers` starts one mutable evictor thread.
3. `IndexPoolWorkers` starts one mutable evictor thread.

This remaining fragmentation is mostly lifecycle and scheduling overhead rather
than algorithm duplication. `buffer/evictor.rs` already provides a generic
`EvictionRuntime`, `PressureDeltaClockPolicy`, and `Evictor<T>` used by both
readonly and mutable pools. What remains pool-local today is wakeup ownership,
shutdown plumbing, and `execute()` semantics.

The task must account for the design goal of buffer pools as high-performance
IO caches. The shared evictor must therefore keep current per-pool pressure
thresholds authoritative. Sharing one thread must not implicitly widen
eviction scope, lower the bar to start eviction, or let pressure from one pool
force another pool into avoidable churn.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Parent RFC:`
`- docs/rfcs/0013-shared-storage-io-service-with-fair-scheduling.md`

## Goals

1. Introduce one `SharedPoolEvictorWorkers` component that owns the production
   shared evictor thread after `FileSystemWorkers` has been built.
2. Remove `DiskPoolWorkers`, `MemPoolWorkers`, and `IndexPoolWorkers` from the
   production engine build path and component registry order.
3. Preserve one `PressureDeltaClockPolicy`, one `ClockHand`, and one
   pool-local `EvictionRuntime::execute()` behavior per pool.
4. Keep current pool-local eviction pressure semantics authoritative:
   a pool should only evict when its own resident count, failure-rate pressure,
   and inflight-eviction state justify it.
5. Add one shared wakeup path so pressure in any pool can wake the single
   shared thread without requiring polling across three private worker events.
6. Preserve explicit reverse-order shutdown and quiescent lifetime safety.
7. Add focused tests proving the shared thread rotates across pressured pools
   without changing readonly vs mutable eviction semantics or making eviction
   more aggressive by default.

## Non-Goals

1. Rewriting eviction policy, `EvictionArbiter`, or clock-sweep candidate
   selection algorithms.
2. Fusing eviction execution into `FileSystemWorkers` or the shared storage IO
   worker loop.
3. Adding new user-facing eviction tuning knobs or fairness configuration.
4. Folding backlog `000071` wrapper cleanup into this task.
5. Broad telemetry, starvation benchmarks, or service-level observability work
   reserved for RFC-0013 Phase 4.
6. Removing isolated test-only helpers that start local evictor threads for
   standalone readonly or evictable pool tests.

## Unsafe Considerations (If Applicable)

This task is unsafe-adjacent because it changes worker ownership and shutdown
around pool/page lifetimes that already rely on unsafe-backed page access,
borrowed IO operations, and `madvise` cleanup.

1. Expected affected modules:
   - `doradb-storage/src/buffer/evictor.rs`
   - `doradb-storage/src/buffer/evict.rs`
   - `doradb-storage/src/buffer/readonly.rs`
2. Required invariants:
   - pool-local runtime state and page guards must remain alive until any
     `execute()` waiter completes;
   - the shared wakeup path must not lose notifications during
     install/start/shutdown transitions;
   - shared component shutdown must signal all pool-local shutdown and waiter
     wakeups before owner drop begins waiting on quiescent guards;
   - the shared runner may serialize pool execution, but it must not alter the
     memory-lifetime guarantees already required by readonly invalidation or
     mutable writeback completion.
3. Validation and inventory refresh if unsafe comments or boundaries change:
   - `tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md`
   - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
   - `cargo nextest run -p doradb-storage`
   - `cargo nextest run -p doradb-storage --no-default-features --features libaio`

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Extend `doradb-storage/src/buffer/evictor.rs` with a shared multi-domain
   runner.
   - Keep the existing single-runtime `Evictor<T>` path for local tests and
     other simple callers.
   - Add a shared runner that owns one policy per pool and rotates domains in
     round-robin order when multiple pools currently report pending pressure.
   - The scheduler should only select pools whose own policy currently decides
     eviction work, so shared execution does not broaden eviction pressure.
2. Add shared-wakeup installation to readonly and mutable pools.
   - Introduce one small helper in readonly and mutable residency state that
     notifies both the existing local test-oriented wakeup event and an
     optional shared worker wakeup handle.
   - Replace direct `evict_ev.notify(1)` production-path calls with that
     helper, while preserving current behavior when no shared wakeup is
     installed.
3. Replace the production worker components with one shared component.
   - Add `SharedPoolEvictorWorkers` plus its owned shutdown state.
   - Build it after `FileSystemWorkers`, once `DiskPool`, `MemPool`, and
     `IndexPool` are all registered and their final runtime dependencies exist.
   - Remove `DiskPoolWorkers`, `MemPoolWorkers`, and `IndexPoolWorkers` from
     the engine build path and registry topology.
4. Keep pool-local eviction semantics local.
   - Reuse the existing readonly runtime adapter for drop-only eviction.
   - Reuse the existing mutable runtime adapter for writeback-capable eviction
     through `FileSystem`.
   - Do not change `EvictionArbiter` thresholds, dynamic batch sizing, or
     pool-local `execute()` behavior in this task.
5. Update component wiring and production-path test builders.
   - `doradb-storage/src/buffer/mod.rs`
   - `doradb-storage/src/component.rs`
   - `doradb-storage/src/engine.rs`
   - `doradb-storage/src/file/fs.rs`
   The new production order becomes:
   1. `DiskPool`
   2. `FileSystem`
   3. `MetaPool`
   4. `IndexPool`
   5. `MemPool`
   6. `FileSystemWorkers`
   7. `SharedPoolEvictorWorkers`
   8. `Catalog`
   9. `TransactionSystem`
   10. `TransactionSystemWorkers`
6. Preserve local standalone test helpers where they remain useful.
   - `StartedGlobalReadonlyBufferPool` and similar isolated pool test owners
     may continue to spawn local evictor threads in test-only code.
   - Registry-backed engine/filesystem test helpers should migrate to the new
     production shared component instead of reassembling the old three-worker
     topology.
7. Encode the cache-performance constraint directly in tests and review.
   - The shared thread must only reconsider which pressured pool runs next.
   - It must not force eviction in a pool that would be idle under its own
     current pressure thresholds.

## Implementation Notes

1. `doradb-storage/src/buffer/evictor.rs` now owns the production
   shared-evictor topology. `SharedPoolEvictorWorkers` starts one shared
   `SharedEvictor` thread after `FileSystemWorkers`, preserves one
   `PressureDeltaClockPolicy` and one `ClockHand` per pool, and rotates
   readonly, mem, and index domains in round-robin order only when the
   selected pool's own policy reports pending work.
2. `doradb-storage/src/buffer/evict.rs` and
   `doradb-storage/src/buffer/readonly.rs` now expose shared-domain builders
   plus one optional shared wake handle in residency state. Production-path
   pressure notifications now wake both the existing local event and the
   shared event when installed, while standalone started-pool helpers keep
   using the local single-pool `Evictor<T>` path in tests.
3. Production registry wiring now registers exactly one
   `shared_pool_evictor_workers` component. `DiskPool` no longer auto-builds
   a readonly worker component, and engine/filesystem test builders now use
   `DiskPool -> FileSystem -> MetaPool -> IndexPool -> MemPool ->
   FileSystemWorkers -> SharedPoolEvictorWorkers`.
4. Validation completed with:
   - `cargo clippy -p doradb-storage --all-targets --locked --offline -- -D warnings`
   - `cargo nextest run -p doradb-storage --locked --offline`
   - `cargo nextest run -p doradb-storage --locked --offline --no-default-features --features libaio`

## Impacts

1. Shared eviction runner and scheduling:
   - `doradb-storage/src/buffer/evictor.rs`
2. Mutable-pool shared wakeup and worker removal:
   - `doradb-storage/src/buffer/evict.rs`
3. Readonly shared wakeup and worker removal:
   - `doradb-storage/src/buffer/readonly.rs`
4. Buffer module exports and component glue:
   - `doradb-storage/src/buffer/mod.rs`
5. Registry order and engine startup:
   - `doradb-storage/src/component.rs`
   - `doradb-storage/src/engine.rs`
6. Production-path shared-storage test builders:
   - `doradb-storage/src/file/fs.rs`

## Test Cases

1. Engine and registry-backed filesystem builds start exactly one shared
   evictor component instead of three production evictor-owner components.
2. Pressure from each of readonly, mem, and index pools wakes the shared
   thread and runs work against the correct pool-local runtime.
3. When multiple pools are pressured, the shared runner rotates pool selection
   instead of draining one pool indefinitely before reconsidering the others.
4. A pool that is not under its own pressure threshold is not selected for
   eviction merely because another pool is pressured.
5. Mutable pools still wait for shared-storage writeback completion before
   post-eviction progress notification and waiter release.
6. Readonly eviction remains drop-only and does not gain writeback or mutable
   cleanup behavior.
7. Shutdown joins the shared evictor thread without quiescent deadlock in both
   engine-backed and registry-backed test paths.
8. Validation commands:
   - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
   - `cargo nextest run -p doradb-storage`
   - `cargo nextest run -p doradb-storage --no-default-features --features libaio`

## Open Questions

1. RFC-0013 Phase 4 should add broader fairness telemetry and observability so
   shared-evictor scheduling can be reviewed against real workloads without
   turning this task into a tuning program.
2. Backlog `000071-collapse-evictor-runtime-wrappers-to-arena-plus-pool-guards`
   remains the intended follow-up for internal runtime-wrapper cleanup after
   the shared topology lands.
