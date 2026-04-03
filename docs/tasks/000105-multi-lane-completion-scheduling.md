---
id: 000105
title: Add Multi-Lane Completion Scheduling
status: implemented  # proposal | implemented | superseded
created: 2026-04-03
github_issue: 520
---

# Task: Add Multi-Lane Completion Scheduling

## Summary

Implement Phase 1 of RFC-0013 by extending `doradb-storage/src/io/` with a
generic scheduler boundary that supports multiple logical request lanes,
lane-local deferred remainders, and bounded-burst admission while preserving
the current backend-neutral completion ownership model. Keep existing
single-lane production callers source-compatible in this phase; do not migrate
`TableFileSystem`, evictable pools, or redo-log I/O yet.

## Context

The current completion worker is structurally single-lane. `IOWorker` owns one
ingress receiver and one global `deferred_req`, so request expansion resumes
before fresh intake regardless of request origin. That is acceptable while
table-file, buffer-pool, and redo-log I/O each own their own worker, but it is
not sufficient for RFC-0013, which requires fair shared-worker scheduling for
the later `StorageService` and `StorageRuntime` phases.

RFC-0013 explicitly isolates this work as Phase 1 so the generic I/O core can
gain fair multi-lane scheduling before any storage client migration. The phase
goal is to make fairness a first-class capability of `crate::io`, not a
service-specific fork inside later storage-runtime code. That means this task
must establish the scheduler seam and multi-lane backend construction now while
keeping the existing one-lane API working unchanged for current production
callers.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Parent RFC:`
`- docs/rfcs/0013-shared-storage-io-service-with-fair-scheduling.md`

## Goals

1. Add an internal scheduler boundary in `doradb-storage/src/io/` for
   multi-lane request admission.
2. Replace the single global `deferred_req` model with lane-local deferred
   remainder ownership.
3. Preserve source-compatible single-lane construction and behavior for
   existing production callers.
4. Add a future-facing multi-lane builder path that later phases can consume
   without introducing storage-specific policy into `crate::io`.
5. Prove fairness-sensitive behavior with focused `io`-module tests, including
   shutdown and backpressure behavior across multiple lanes.

## Non-Goals

1. Migrating `doradb-storage/src/file/table_fs.rs`,
   `doradb-storage/src/buffer/evict.rs`, or
   `doradb-storage/src/trx/log.rs` to multi-lane ingress in this task.
2. Implementing `StorageService`, `StorageRuntime`, or any other shared
   storage-runtime ownership in this phase.
3. Merging evictor threads or changing eviction policy.
4. Introducing heterogeneous request types per lane; Phase 1 remains generic
   over a single `T`.
5. Changing `IOStateMachine::prepare_request` or adding new user-facing tuning
   knobs for scheduling.
6. Changing redo durability behavior, table-file semantics, checkpointing, or
   recovery behavior.

## Unsafe Considerations (If Applicable)

This task reshapes an unsafe-sensitive boundary even if it does not need to
expand the unsafe surface area.

1. Expected affected unsafe-bearing paths:
   - `doradb-storage/src/io/iouring_backend.rs`
   - `doradb-storage/src/io/libaio_backend.rs`
   - `doradb-storage/src/io/libaio_abi.rs` if backend factory changes require
     ABI-adjacent plumbing updates
2. Required invariants:
   - scheduler-owned deferred remainders must not bypass global submission
     depth accounting or outlive the ownership rules that currently protect
     in-flight buffers and guarded page references;
   - single-lane compatibility wrappers must remain behaviorally equivalent to
     the existing one-receiver worker path;
   - multi-lane wakeup and shutdown paths must not lose completion or shutdown
     signals while selecting across lane receivers;
   - backend-specific raw-pointer or kernel token handling must remain below
     the generic scheduler boundary with localized `// SAFETY:` contracts.
3. Validation and inventory refresh if unsafe comments or boundaries change:
   - `tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md`
   - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
   - `cargo nextest run -p doradb-storage`
   - `cargo nextest run -p doradb-storage --no-default-features --features libaio`

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Extract a scheduler object inside `doradb-storage/src/io/mod.rs`.
   - Add one lane state per ingress lane.
   - Each lane owns its own `Receiver<AIOMessage<T>>`.
   - Each lane owns its own `deferred_req: Option<T>`.
   - Store scheduler cursor state needed for cyclic lane rotation.
2. Refactor `IOWorker` to depend on the scheduler boundary instead of a single
   ingress receiver plus global deferred remainder.
   - Keep staged batching, inflight-slot accounting, backend submission,
     completion dispatch, and shutdown-drain structure unchanged where
     possible.
   - Move only request intake and fairness-driven admission behind the
     scheduler interface.
3. Lock the fairness algorithm for this phase as round-robin scheduling with
   per-lane operation burst caps.
   - A lane turn may admit at most `min(headroom, burst_limit_ops)` new
     operations before the scheduler rotates.
   - Deferred remainders stay attached to the originating lane and must not
     bypass rotation.
   - Single-lane compatibility uses one lane with effectively unbounded burst
     capacity so existing behavior stays unchanged.
4. Use one bounded flume channel per lane rather than a shared tagged ingress
   channel.
   - Backpressure remains lane-local.
   - A bursty lane cannot fill a shared ingress queue before another lane can
     enqueue latency-sensitive work.
   - Idle blocking for multi-lane workers should wait on any lane; active
     workers should poll lanes non-blockingly in fairness order.
5. Preserve current one-lane caller APIs while adding future-facing multi-lane
   construction.
   - Keep `AIOClient<T>` source-compatible as the sender wrapper.
   - Keep `IOWorkerBuilder<T>` source-compatible as the one-lane wrapper.
   - Keep `StorageBackend::io_worker()` as the compatibility entry point over
     lane count `1`.
   - Add a new multi-lane backend construction path that returns
     `Vec<AIOClient<T>>` plus a multi-lane worker builder/config object.
6. Implement the new multi-lane construction path in both backends.
   - Update `doradb-storage/src/io/iouring_backend.rs`.
   - Update `doradb-storage/src/io/libaio_backend.rs`.
   - Keep lane identity generic and index-based; do not add storage-specific
     lane enums in this phase.
7. Keep the change generic-core only.
   - Do not migrate production state machines to multi-lane clients.
   - Do not change `IOStateMachine::prepare_request`; enforce burst fairness by
     capping `max_new` per scheduler turn.
8. Extend `io`-module tests to cover multi-lane behavior and compatibility.
   - Preserve existing single-lane tests.
   - Add fairness, lane-isolation, wakeup, and shutdown-drain scenarios.

## Implementation Notes

## Impacts

1. Generic I/O core:
   - `doradb-storage/src/io/mod.rs`
   - `AIOClient<T>`
   - `IOWorker<T, B>`
   - `IOWorkerBuilder<T>`
   - new internal scheduler and lane-state types
2. Backend worker construction:
   - `doradb-storage/src/io/iouring_backend.rs`
   - `doradb-storage/src/io/libaio_backend.rs`
   - compile-adjacent spillover into `doradb-storage/src/io/libaio_abi.rs` if
     backend factory plumbing changes require it
3. Compatibility-only validation surfaces:
   - `doradb-storage/src/file/table_fs.rs`
   - `doradb-storage/src/buffer/evict.rs`
   - `doradb-storage/src/trx/log.rs`

## Test Cases

1. Existing single-lane tests continue to pass with the scheduler seam in
   place.
2. A lane with deferred request remainder cannot monopolize admission when
   another lane has ready work.
3. Deferred remainders remain isolated to the originating lane and are drained
   through the same fairness policy as fresh intake.
4. Global worker depth accounting remains correct across staged, deferred, and
   submitted work from multiple lanes.
5. An idle multi-lane worker can wake on any lane and then resume fairness
   rotation correctly.
6. Shutdown drains queued and deferred requests across all lanes exactly once.
7. Validation commands:
   - `cargo nextest run -p doradb-storage`
   - `cargo nextest run -p doradb-storage --no-default-features --features libaio`

## Open Questions

None in this task scope. Later RFC-0013 phases will decide storage-specific
lane mapping, caller migration onto shared clients, storage-runtime ownership,
and any future public tuning surface for scheduler policy.
