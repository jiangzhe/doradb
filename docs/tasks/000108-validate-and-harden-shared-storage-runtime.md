---
id: 000108
title: Validate And Harden Shared Storage Runtime
status: implemented  # proposal | implemented | superseded
created: 2026-04-05
github_issue: 527
---

# Task: Validate And Harden Shared Storage Runtime

## Summary

Implement Phase 4 of RFC-0013 by validating the shipped shared storage worker
and shared multi-pool evictor under production-path mixed load, adding the
missing runtime-local telemetry needed to review fairness, tightening
compatibility coverage around authoritative shared IO depth, and synchronizing
living docs to the current runtime topology.

## Context

RFC-0013 split the shared storage runtime program into four phases. Phases 1
through 3 are already implemented:

1. `000105` added the scheduler/fairness groundwork in `crate::io`.
2. `000106` consolidated table-file and evictable-pool IO under one concrete
   shared three-lane worker in `doradb-storage/src/file/fs.rs`.
3. `000107` replaced the three production evictor-owner components with one
   `SharedPoolEvictorWorkers` thread in `doradb-storage/src/buffer/evictor.rs`.

That leaves the final hardening phase still open. The current implementation
already has the selected ownership and scheduling topology, but the validation
surface does not yet match the architecture:

1. `doradb-storage/src/file/fs.rs` contains the production shared worker, but
   its tests currently focus on filesystem/config behavior rather than proving
   bounded read progress under background writeback pressure.
2. `doradb-storage/src/buffer/evictor.rs` has scheduler-unit tests using mock
   runtimes, but it does not yet have production-path coverage proving that
   readonly, mem, and index pressure safely share one wake source and one
   thread loop.
3. `FileSystem` currently exposes raw backend submit/wait counters through
   `IOBackendStats`, and pools expose their own access/IO lifecycle counters,
   but there is no service-level telemetry for shared-storage lane selection or
   shared-evictor domain execution.
4. `EvictableBufferPoolConfig.max_io_depth` remains a public compatibility
   field even though `FileSystemConfig.io_depth` is already the authoritative
   shared-worker depth for engine builds.
5. `docs/async-io.md` and related living docs still need explicit sync to the
   shipped shared-runtime ownership and observability shape.

This task keeps the phase-4 boundary narrow. It completes the validation,
telemetry, and documentation work required by RFC-0013 without widening into a
new scheduling framework, a benchmark program, or the separate mutable-pool
shutdown/API refactor deferred from task `000107`.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Parent RFC:`
`- docs/rfcs/0013-shared-storage-io-service-with-fair-scheduling.md`

## Goals

1. Add production-path starvation-focused tests proving that fresh table and
   readonly reads make progress even when the shared background-write lane has
   deferred writeback work.
2. Add production-path shared-evictor wakeup and fairness tests proving that
   readonly, mem, and index pools can safely share one evictor thread while
   keeping pool-local pressure decisions authoritative.
3. Add minimal runtime-local telemetry for the shared storage worker and shared
   evictor so tests and diagnostics can distinguish fairness behavior from raw
   backend saturation.
4. Add explicit compatibility/deprecation coverage proving that
   `FileSystemConfig.io_depth` remains the authoritative shared-worker depth
   while `EvictableBufferPoolConfig.max_io_depth` is compatibility-only.
5. Update living runtime docs to describe the current shared storage worker,
   shared evictor, and telemetry surfaces accurately.
6. Validate both supported backend builds using the repository-supported
   `cargo nextest` workflow.

## Non-Goals

1. Adding new user-facing scheduling knobs, lane weights, fairness tuning, or
   autotuning policy.
2. Changing the selected shared-runtime topology, redo-log ownership, or
   shared-evictor versus shared-worker thread split.
3. Reworking mutable or readonly eviction algorithms beyond the narrow testing
   and telemetry hooks required for validation.
4. Removing `EvictableBufferPoolConfig.max_io_depth` from the public config
   surface in this task. Phase 4 only hardens compatibility coverage and
   documentation around its non-authoritative status.
5. Pulling in the broader `BufferPool::allocate_page() -> Result<_>` shutdown
   refactor deferred in `docs/backlogs/000081-make-bufferpool-allocate-page-fallible-on-shutdown.md`.
6. Building a standalone benchmark or performance-tuning program beyond the
   targeted validation required for this task.

## Unsafe Considerations (If Applicable)

This task is unsafe-adjacent even if it should not expand the existing unsafe
surface area.

1. Expected affected modules:
   - `doradb-storage/src/file/fs.rs`
   - `doradb-storage/src/buffer/evictor.rs`
   - `doradb-storage/src/io/mod.rs` for test-hook-driven validation only
2. Required invariants:
   - telemetry updates must not change lane scheduling, backend submission
     ownership, inflight token validation, or completion ordering in the shared
     storage worker;
   - starvation/fairness tests that use backend test hooks must remain strictly
     test-only and must not introduce always-compiled runtime indirection;
   - shared-evictor stats must observe wakeups and domain execution without
     altering pool-local `EvictionRuntime::execute()` semantics or wakeup
     delivery;
   - any touched unsafe blocks must keep precise `// SAFETY:` comments and must
     not weaken the current borrowed-buffer/page lifetime guarantees.
3. Inventory refresh and validation scope:
   - If implementation changes unsafe code or `// SAFETY:` contracts, run:
     `tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md`
   - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
   - `cargo nextest run -p doradb-storage`
   - `cargo nextest run -p doradb-storage --no-default-features --features libaio`

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Add shared-storage service telemetry in `doradb-storage/src/file/fs.rs`.
   - Introduce a small `StorageServiceStats` snapshot plus internal handle in
     the same snapshot/delta style already used by `IOBackendStats` and
     `BufferPoolStats`.
   - Track only the missing fairness-review signals:
     - ingress request counts per lane (`table_reads`, `pool_reads`,
       `background_writes`);
     - scheduler turn counts per lane.
   - Keep raw backend submit/wait stats in the existing `IOBackendStats`; do
     not duplicate backend counters in the new service-level stats.
   - Persist the configured shared-worker depth on `FileSystem` and expose a
     getter such as `configured_io_depth()` for compatibility assertions.
2. Add starvation-focused production-path tests in `doradb-storage/src/file/fs.rs`.
   - Reuse the existing storage-backend test hook from `doradb-storage/src/io/mod.rs`
     to control completion timing deterministically.
   - Build mixed-load scenarios where background writes occupy headroom and
     leave deferred remainder while queued reads remain pending.
   - Assert that fresh table/readonly reads and fresh pool page-in reads are
     reconsidered before another deferred background-write burst consumes the
     next freed slot.
3. Add shared-evictor telemetry in `doradb-storage/src/buffer/evictor.rs`.
   - Introduce `SharedPoolEvictorStats` plus handle with a narrow field set:
     - wake count;
     - wait count;
     - per-domain run counts for readonly, mem, and index.
   - Store the handle in `SharedPoolEvictorWorkersOwned` and expose it through
     the component access path so engine-backed tests can snapshot it without
     widening pool APIs.
4. Add production-path shared-evictor wake/fairness tests.
   - Extend `doradb-storage/src/buffer/evictor.rs` beyond mock-domain scheduler
     tests by exercising the engine-backed shared thread.
   - Create scenarios where readonly, mem, and index pressure each wake the
     shared thread and increment the expected per-domain counters.
   - Add a concurrent-pressure scenario proving multiple domains make progress
     without one domain monopolizing the loop.
   - Add a no-pressure control proving an idle domain does not accrue run
     counts merely because another domain is active.
5. Add explicit authoritative-depth compatibility coverage.
   - Extend `doradb-storage/src/engine.rs` tests to verify that the configured
     shared worker depth comes from `FileSystemConfig.io_depth`.
   - Verify that changing only `EvictableBufferPoolConfig.max_io_depth` leaves
     the shared-worker depth unchanged.
   - Update `doradb-storage/src/conf/buffer.rs` comments/docstrings so the
     compatibility-only status of `max_io_depth` is explicit.
6. Synchronize living docs.
   - Update `docs/async-io.md` to describe the current split between the
     concrete shared storage worker, the dedicated redo worker, and the current
     stats surfaces.
   - Update `docs/engine-component-lifetime.md` where needed so the runtime
     ownership and shutdown text matches the shipped shared worker and shared
     evictor shape.

## Implementation Notes

Implemented the Phase 4 validation and hardening work in the shipped shared
runtime without changing the selected runtime topology.

1. Added `StorageServiceStats` in `doradb-storage/src/file/fs.rs` to expose
   shared-worker ingress counts and scheduler-turn counts per lane
   (`table_reads`, `pool_reads`, `background_writes`) alongside the existing
   raw backend counters.
2. Added starvation-focused production-path tests in
   `doradb-storage/src/file/fs.rs` proving queued table/readonly reads and
   queued pool page-in reads are reconsidered before deferred background-write
   remainder consumes the next freed slot.
3. Added `SharedPoolEvictorStats` in `doradb-storage/src/buffer/evictor.rs`
   and exposed it through the shared-evictor component access path so engine
   tests can snapshot wake/wait counts plus readonly/mem/index run counts.
4. Added production-path shared-evictor tests proving isolated domain pressure,
   concurrent multi-domain pressure, and idle-domain non-participation for the
   single shared evictor thread.
5. Added authoritative-depth compatibility coverage in
   `doradb-storage/src/engine.rs` and clarified in
   `doradb-storage/src/conf/buffer.rs` that
   `EvictableBufferPoolConfig.max_io_depth` is compatibility-only while
   `FileSystemConfig.io_depth` remains authoritative for shared storage IO.
6. Updated `docs/async-io.md` and `docs/engine-component-lifetime.md` so the
   living docs match the shipped shared storage worker, dedicated redo worker,
   shared evictor ownership, and telemetry surfaces.
7. Validation completed with:
   - `cargo fmt --all`
   - `cargo test -p doradb-storage --lib --no-run`
   - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
   - `cargo nextest run -p doradb-storage`
   - `cargo nextest run -p doradb-storage --no-default-features --features libaio`
8. Post-implementation review hardening consolidated storage-backend test-hook
   installation onto one shared test-only lock in `doradb-storage/src/io/mod.rs`
   so hook-driven tests in shared-storage, readonly, table, and redo-log
   modules cannot race by installing different hooks against the same
   process-global hook slot.

## Impacts

1. Shared storage worker scheduling, stats, and tests:
   - `doradb-storage/src/file/fs.rs`
2. Shared evictor stats, access path, and tests:
   - `doradb-storage/src/buffer/evictor.rs`
3. Engine/runtime access for compatibility and telemetry assertions:
   - `doradb-storage/src/engine.rs`
4. Compatibility-only config documentation:
   - `doradb-storage/src/conf/buffer.rs`
5. Test-only backend-control support used by production-path validation:
   - `doradb-storage/src/io/mod.rs`
6. Living documentation:
   - `docs/async-io.md`
   - `docs/engine-component-lifetime.md`

## Test Cases

1. A deferred background-write remainder cannot consume the next freed worker
   slot before a queued table/readonly read is reconsidered.
2. A deferred background-write remainder cannot consume the next freed worker
   slot before a queued pool page-in read is reconsidered.
3. Shared-storage service stats reflect lane request admission and scheduler
   turns for the mixed-load validation scenarios.
4. Pressure from each of readonly, mem, and index pools wakes the shared
   evictor and increments the expected per-domain run count.
5. When multiple pools are pressured concurrently, the shared evictor makes
   progress across the active domains rather than repeatedly running only one.
6. A pool that is not under local pressure does not accumulate shared-evictor
   run counts merely because another pool is pressured.
7. `FileSystemConfig.io_depth` is reflected by the shared storage worker and
   changing only `EvictableBufferPoolConfig.max_io_depth` does not change that
   depth.
8. Validation commands:
   - `cargo nextest run -p doradb-storage`
   - `cargo nextest run -p doradb-storage --no-default-features --features libaio`

## Open Questions

1. `EvictableBufferPoolConfig.max_io_depth` remains compatibility-only after
   this task. A later cleanup task can remove the field entirely once the
   repository no longer needs the compatibility surface.
2. Mutable-pool shutdown propagation remains intentionally separate in
   `docs/backlogs/000081-make-bufferpool-allocate-page-fallible-on-shutdown.md`
   because the preferred fix requires a broader fallible-allocation API change
   than Phase 4 should absorb.
