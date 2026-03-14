---
id: 000068
title: Worker And Component Handle Migration
status: proposal  # proposal | implemented | superseded
created: 2026-03-14
github_issue: 428
---

# Task: Worker And Component Handle Migration

## Summary

Implement phase 2 of RFC-0008 by migrating long-lived runtime component
handles from leaked `&'static` references to DAG-managed quiescent
dependencies without extracting transaction-system background workers yet.
This task extends the existing `StaticOwner<T>` bridge used in phase 1 in
place so other subsystems can reuse it during this transitional stage, and
converts stored runtime handles such as `RedoLogPageCommitter`,
`ReadonlyBufferPool`, and recovery/startup context structs to use that
bridge instead of bare `&'static` captures.

## Context

RFC-0008 phase 1 adopted a private engine `QuiDAG` for top-level component
teardown, but deliberately kept `EngineInner` fields and most runtime call
paths on leaked `&'static` references. The remaining phase-2 problem is not
just top-level ownership; it is the long-lived runtime state that still stores
or captures those references after engine construction.

Today `PendingTransactionSystem`, `RedoLogPageCommitter`,
`ReadonlyBufferPool`, and recovery/reload helper structs all keep
leaked-static component references alive through implicit shutdown ordering.
That is exactly the lifetime surface phase 2 is meant to make explicit.

Transaction-system worker startup and teardown are a different problem.
IO/GC/purge threads are still tightly coupled to internal channels and
log-partition state owned by `TransactionSystem`, and extracting those
threads now would require a new crate-private runtime-control seam for
shutdown signaling and join orchestration. That is broader than the intended
scope of this phase, so worker extraction is deferred.

This task therefore adopts a narrower split model:

1. long-lived stored component handles move to an internal quiescent bridge
   type over the phase-1 leaked-static owners;
2. transaction-system worker ownership and teardown remain inside
   `TransactionSystem` for now;
3. public engine fields and broader buffer-pool/page-guard APIs remain
   unchanged for later RFC phases.

Issue Labels:
- `type:task`
- `priority:medium`
- `codex`

Parent RFC:
- `docs/rfcs/0008-quiescent-component-migration-program.md`

## Goals

1. Extend the existing `StaticOwner<T>` bridge from phase 1 into a
   crate-visible internal helper so runtime code can store quiescent deps to
   leaked-static owners while still calling legacy `&'static` APIs through an
   explicit boundary.
2. Replace bare leaked-static captures in long-lived runtime handle sites,
   including:
   - `PendingTransactionSystem`
   - `RedoLogPageCommitter`
   - `ReadonlyBufferPool`
   - long-lived recovery and reload helper structs that currently store engine
     component references.
3. Keep `EngineInner` field shape unchanged in this phase.
4. Preserve existing startup/shutdown behavior and verify both default-feature
   and `--no-default-features` test passes.

## Non-Goals

1. Extracting transaction-system IO/GC/purge workers from `TransactionSystem`
   or `LogPartition` in this task.
2. Adding a new general shutdown API solely to support worker extraction.
3. Extracting component-internal service threads from `TableFileSystem`,
   `EvictableBufferPool`, or `GlobalReadonlyBufferPool` into DAG nodes in this
   task.
4. Removing `&'static self` from `BufferPool`, page guards, or broad
   table/index caller APIs.
5. Refactoring buffer-pool frame/page ownership or introducing
   `QuiescentArena`.
6. Removing `StaticLifetime` from top-level components.
7. Implementing graceful shutdown, session draining, or work rejection policy.

## Unsafe Considerations (If Applicable)

This task is expected to reuse and slightly extend unsafe-sensitive lifetime
code around leaked-static teardown and quiescent ownership bridging.

1. Affected modules and why `unsafe` is relevant:
   - `doradb-storage/src/engine.rs` and/or a new internal bridge module for
     DAG-managed leaked-static owners
   - `doradb-storage/src/quiescent.rs` only if the bridge layer is colocated
     there
   Existing `StaticLifetime::drop_static(...)` and quiescent owner invariants
   remain in force. Unsafe risk is concentrated in the bridge between
   leaked-static owners and quiescent dependencies.
2. Required invariants and checks:
   - bridge owner wrappers must still drop each leaked-static pointer exactly
     once;
   - quiescent deps must keep the bridge owner alive for the full lifetime of
     stored runtime handles;
   - migrated handle types must not reconstruct raw leaked-static references
     except through the explicit bridge accessor;
   - all new unsafe blocks or unsafe impls must keep adjacent `// SAFETY:`
     comments.
3. Inventory refresh and validation scope:
   - no `tools/unsafe_inventory.rs` refresh is expected unless implementation
     expands unsafe scope beyond existing engine/quiescent/lifetime
     boundaries;
   - validation scope:
```bash
cargo test -p doradb-storage
cargo test -p doradb-storage --no-default-features
```

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Extend the existing `StaticOwner<T>` bridge introduced in phase 1:
   - add `as_static()` or equivalent explicit access to the wrapped leaked
     reference;
   - add the reusable dep-facing support needed for non-engine modules to keep
     quiescent deps to these owners;
   - keep `StaticOwner<T>` in `engine.rs` for this phase rather than
     relocating it, because this bridge remains transitional and the broader
     static-lifetime machinery is expected to shrink or disappear by the end
     of RFC-0008.
2. Keep transaction-system worker ownership unchanged in this phase:
   - leave IO/GC/purge thread handles in `TransactionSystem` / `LogPartition`;
   - leave `TransactionSystem::drop` responsible for `Commit::Shutdown`,
     `GC::Stop`, `Purge::Stop`, thread joins, and log-file closure.
3. Convert long-lived stored handle sites from raw leaked statics to bridge
   deps:
   - `PendingTransactionSystem`
   - `RedoLogPageCommitter`
   - `ReadonlyBufferPool`
   - recovery/reload structs that retain pools, `TableFileSystem`, or
     readonly-pool owners across async work.
4. Thread the new handle types through internal constructors only as far as
   needed:
   - table/catalog storage construction;
   - catalog reload path;
   - recovery bootstrap path.
   Prefer short-lived borrows in recovery/startup paths when that only
   requires small code changes; use stored bridge deps only where avoiding
   them would require disproportionate churn.
   Do not broaden the change into general `BufferPool` API cleanup.
5. Keep component-internal threads outside scope for now:
   - `TableFileSystem` event loop remains internal to `TableFileSystem`;
   - readonly and evictable buffer-pool IO/evictor threads remain internal to
     those pools.
   The long-term direction can still move all background threads into
   top-level components with customizable teardown, but that belongs to a
   future RFC rather than RFC-0008 phase 2.
6. Add focused regression tests for:
   - page-committer and readonly-wrapper handle lifetime behavior;
   - recovery/reload async handle retention;
   - engine startup/teardown behavior remaining unchanged with
     `TransactionSystem::drop` still driving worker shutdown;
   - full crate test passes with and without default features.

## Implementation Notes


## Impacts

1. `doradb-storage/src/engine.rs`
2. `doradb-storage/src/trx/sys_conf.rs`
3. `doradb-storage/src/index/util.rs`
4. `doradb-storage/src/buffer/readonly.rs`
5. `doradb-storage/src/catalog/storage/mod.rs`
6. `doradb-storage/src/catalog/mod.rs`
7. `doradb-storage/src/table/mod.rs`
8. `doradb-storage/src/trx/recover.rs`
9. `StaticOwner<T>` in `doradb-storage/src/engine.rs`

## Test Cases

1. Verify `RedoLogPageCommitter` no longer stores a raw leaked-static
   transaction-system ref and still works for row-page redo logging.
2. Verify `ReadonlyBufferPool` no longer stores a raw leaked-static global
   readonly-pool ref and still serves table/catalog readonly reads correctly.
3. Verify recovery/reload paths can hold the migrated component handles
   through async work without reintroducing bare leaked-static storage.
4. Verify engine startup/teardown behavior is unchanged with
   `TransactionSystem::drop` still driving worker shutdown.
5. Run:
```bash
cargo test -p doradb-storage
cargo test -p doradb-storage --no-default-features
```

## Decisions And Follow-Ups

1. Keep the shared bridge as an extended `StaticOwner<T>` in `engine.rs` for
   this phase. The current design is transitional, and the long-term plan is
   likely to remove most or all static-lifetime machinery by the end of
   RFC-0008 rather than reshuffle it now.
2. In recovery/startup code, prefer short-lived borrowed access when that can
   be achieved with small local changes. If forcing a borrow-only shape would
   cause disproportionate churn, stored bridge-backed handles remain an
   acceptable fallback for this phase.
3. Component-internal service threads stay internal in this task. The
   long-term direction can still aim to extract all background threads and
   treat them as top-level components with customizable teardown, but that
   should be designed in a future RFC.
4. Transaction-system worker extraction is deferred entirely out of RFC-0008
   phase 2. Revisit it in a future task as part of a new RFC rather than
   expanding this transitional phase.
