---
id: 000073
title: Engine Shutdown Barrier
status: implemented  # proposal | implemented | superseded
created: 2026-03-17
github_issue: 439
---

# Task: Engine Shutdown Barrier

## Summary

Implement phase 1 of RFC-0009 by adding an explicit engine shutdown barrier
that closes new work admission before top-level component teardown begins.

This task keeps the current static component ownership model, but changes
engine lifecycle semantics:

1. `Engine` gets explicit lifecycle state and `shutdown(&self) -> Result<()>`.
2. New sessions and new transactions are rejected once shutdown starts.
3. Worker-backed components move stop/join logic into explicit idempotent
   shutdown hooks instead of relying only on `Drop`.
4. `TransactionSystemShutdown` is renamed to `StorageEngineShutdown`.
5. The old infallible compatibility entry points `new_session()` and
   `begin_trx()` are removed in favor of fallible `try_new_session()` and
   `try_begin_trx()`.

`drop(engine)` remains a fail-fast cleanup path only for the trivial
no-extra-ref case. Coordinated teardown is expected to use explicit shutdown.

## Context

Current engine teardown is still destructor-driven. `Engine::drop` only checks
that no extra `EngineRef` handles remain, while worker-backed components such
as `TransactionSystem`, `EvictableBufferPool`, `GlobalReadonlyBufferPool`, and
`TableFileSystem` stop their threads inside component `Drop`. That is
acceptable while top-level components are still leaked statics, but RFC-0009
selected an explicit shutdown barrier as the prerequisite for later migration
to quiescent-owned components.

The current engine/session API also admits new work unconditionally:

1. `Engine::new_session()` and `EngineRef::new_session()` always create a new
   session.
2. `Session::begin_trx()` always tries to start a transaction unless the
   session is already in one.

That means there is no engine-level admission gate today. The only shutdown
error currently exposed is `TransactionSystemShutdown`, and it appears late in
the log commit path rather than at engine/session admission time.

This phase is intentionally narrow. It adds the minimal explicit shutdown
contract required by RFC-0009:

1. shutdown is explicit, idempotent, and retryable;
2. new sessions and new transactions are rejected after shutdown begins;
3. top-level owner drop does not start until worker stop/join hooks complete.

Full graceful session draining, timeouts, and forced-stop policy remain out of
scope and are now tracked by backlog `000059`.

Issue Labels:
- `type:task`
- `priority:medium`
- `codex`

Parent RFC:
- `docs/rfcs/0009-remove-static-lifetime-from-engine-components.md`

Source Backlogs:
- `docs/backlogs/000042-graceful-storage-engine-shutdown-lifecycle-for-sessions-and-system-components.md`

## Goals

1. Add engine lifecycle state in `EngineInner`:
   - `Running`
   - `ShuttingDown`
   - `Shutdown`
2. Add `Engine::shutdown(&self) -> Result<()>` as the explicit shutdown entry
   point.
3. Keep shutdown idempotent and retryable:
   - first shutdown call closes admission;
   - later shutdown calls either complete teardown or observe that shutdown is
     already complete.
4. Reject new work after shutdown begins:
   - add `Engine::try_new_session()` and `EngineRef::try_new_session()`;
   - add `Session::try_begin_trx()`;
   - remove `new_session()` and `begin_trx()`.
5. Rename `Error::TransactionSystemShutdown` to
   `Error::StorageEngineShutdown`.
6. Add a distinct busy error, e.g. `Error::StorageEngineShutdownBusy(usize)`,
   for the case where explicit shutdown cannot complete because extra engine
   references are still alive.
7. Move worker stop/join logic into explicit idempotent shutdown hooks on:
   - `TransactionSystem`
   - `EvictableBufferPool`
   - `GlobalReadonlyBufferPool`
   - `TableFileSystem`
8. Make `Drop for Engine` use the same internal finalize path when the engine
   is the last strong owner, and panic if extra references still exist.
9. Update all in-crate call sites and tests to use the new fallible session and
   transaction APIs directly.

## Non-Goals

1. Implementing full graceful shutdown with session draining, wait timeouts, or
   forced-stop behavior.
2. Replacing leaked-static engine fields with `QuiescentGuard<T>` or removing
   `StaticHandle` in this phase.
3. Changing `BufferPool` or other runtime interfaces away from `&'static self`.
4. Refactoring worker-backed components into separate DAG worker nodes.
5. Gating every direct subsystem API on engine lifecycle state beyond session
   and transaction admission.
6. Cleaning up the remaining static-lifetime compatibility layer outside the
   shutdown barrier and entry-point changes required by this phase.

## Unsafe Considerations (If Applicable)

This task does not change the engine's raw-pointer or quiescent ownership model
yet, but it does touch unsafe-sensitive runtime modules whose destructors
interact with leaked-static storage and quiescent arenas.

1. Affected modules:
   - `doradb-storage/src/engine.rs`
   - `doradb-storage/src/trx/sys.rs`
   - `doradb-storage/src/buffer/evict.rs`
   - `doradb-storage/src/buffer/readonly.rs`
   - `doradb-storage/src/file/table_fs.rs`
2. Required invariants:
   - explicit shutdown hooks must be idempotent;
   - shutdown hooks must be safe on partially started components;
   - top-level owner drop must not begin before worker stop/join completes;
   - `Drop for Engine` must continue to fail fast when extra engine references
     still exist;
   - this phase must not weaken existing static-lifetime or quiescent
     keepalive invariants.
3. Validation scope:
```bash
cargo test -p doradb-storage
cargo test -p doradb-storage --no-default-features
```

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Add engine lifecycle and shutdown coordination in
   `doradb-storage/src/engine.rs`:
   - introduce private lifecycle state on `EngineInner`;
   - add shutdown serialization so `shutdown()` and `Drop` share one internal
     finalize path;
   - close new-work admission before attempting worker stop/join;
   - if `Arc::strong_count(&engine_inner) > 1`, return
     `Error::StorageEngineShutdownBusy(...)` and keep lifecycle state at
     `ShuttingDown`;
   - once only the root engine remains, execute explicit worker shutdown hooks
     before allowing owner teardown to continue.
2. Update public engine/session entry points:
   - remove `Engine::new_session()` and `EngineRef::new_session()`;
   - add `try_new_session()` on both `Engine` and `EngineRef`;
   - remove `Session::begin_trx()`;
   - add `Session::try_begin_trx() -> Result<Option<ActiveTrx>>`;
   - update session helper methods that currently do `begin_trx().unwrap()` to
     propagate `StorageEngineShutdown`.
3. Rename and extend shutdown errors:
   - rename `TransactionSystemShutdown` to `StorageEngineShutdown`;
   - update late commit-path shutdown rejection in `trx/log.rs` to use the new
     error name;
   - add a separate busy error variant for explicit shutdown retry.
4. Move worker stop/join into explicit component hooks:
   - `TransactionSystem`: split current `Drop` cleanup into a shutdown hook
     that handles started and unstarted IO/GC/purge threads safely;
   - `EvictableBufferPool`: move IO/evictor stop logic behind a shared hook
     safe for started and unstarted states;
   - `GlobalReadonlyBufferPool`: move evictor shutdown behind a shared hook;
   - `TableFileSystem`: make thread handle ownership accessible from `&self`
     so shutdown can run before component destruction.
5. Keep destructor behavior aligned with explicit shutdown:
   - component `Drop` implementations delegate to their explicit shutdown hook;
   - `Engine::drop` runs the same finalize path only when no extra engine refs
     remain, and otherwise panics as a leaked-ref bug.
6. Update in-crate call sites and tests:
   - switch all engine/session callers from removed infallible APIs to the new
     fallible forms;
   - update assertions to expect `StorageEngineShutdown` or the busy shutdown
     error where appropriate.
7. Add focused phase-1 tests for:
   - shutdown idempotence;
   - reject-new-session behavior after shutdown begins;
   - reject-new-transaction behavior for an existing session after shutdown
     begins;
   - busy shutdown with outstanding `EngineRef` / `Session` holders, followed
     by successful retry after those holders are dropped;
   - `drop(engine)` succeeding without explicit shutdown when no extra refs
     exist;
   - `drop(engine)` panicking when extra refs still exist;
   - explicit shutdown hooks being safe on partially started components.

## Implementation Notes

Implemented in `doradb-storage` with the phase-1 shutdown barrier selected by
RFC-0009.

1. `Engine` now owns explicit lifecycle state and an idempotent
   `shutdown(&self) -> Result<()>` path that:
   - closes admission for new work;
   - returns `Error::StorageEngineShutdownBusy(usize)` while extra
     `EngineRef`/`Session` holders are still alive;
   - runs explicit worker shutdown hooks before owner teardown proceeds.
2. Engine and session admission now use only fallible entry points:
   - `Engine::try_new_session()`
   - `EngineRef::try_new_session()`
   - `Session::try_begin_trx()`
   The old `new_session()` and `begin_trx()` compatibility entry points were
   removed.
3. `Error::TransactionSystemShutdown` was renamed to
   `Error::StorageEngineShutdown`, and late shutdown rejection sites were
   updated to the engine-level error naming.
4. Worker-backed components now expose explicit idempotent shutdown hooks:
   - `TransactionSystem`
   - `EvictableBufferPool`
   - `GlobalReadonlyBufferPool`
   - `TableFileSystem`
   Their `Drop` implementations delegate to those hooks instead of being the
   only shutdown path.
5. Focused tests were added for:
   - engine shutdown idempotence and admission rejection;
   - busy shutdown while extra refs remain, followed by successful retry;
   - fail-fast `drop(engine)` behavior when leaked refs still exist;
   - idempotent shutdown on unstarted or partially started worker-backed
     components.
6. Additional regression cleanup during follow-up verification fixed two table
   MVCC tests that incorrectly reused one `Session` for overlapping
   transactions; those tests now use distinct sessions.
7. Validation outcomes:
   - implementation validation completed with:
     - `cargo test -p doradb-storage`
     - `cargo test -p doradb-storage --no-default-features`
   - resolve-time regression verification completed with:
     - `cargo test -p doradb-storage --no-default-features table::tests::test_column_delete_rollback_after_checkpoint -- --exact`
     - `cargo test -p doradb-storage --no-default-features table::tests::test_column_delete_mvcc_visibility -- --exact`

## Impacts

1. `doradb-storage/src/engine.rs`
2. `doradb-storage/src/session.rs`
3. `doradb-storage/src/error.rs`
4. `doradb-storage/src/trx/log.rs`
5. `doradb-storage/src/trx/sys.rs`
6. `doradb-storage/src/buffer/evict.rs`
7. `doradb-storage/src/buffer/readonly.rs`
8. `doradb-storage/src/file/table_fs.rs`
9. in-crate engine/session callers and tests using:
   - `new_session()`
   - `begin_trx()`

## Test Cases

1. `Engine::shutdown()` is idempotent when no extra engine references exist.
2. `Engine::try_new_session()` returns `Err(Error::StorageEngineShutdown)` once
   shutdown begins.
3. `EngineRef::try_new_session()` returns
   `Err(Error::StorageEngineShutdown)` once shutdown begins.
4. `Session::try_begin_trx()` returns
   `Err(Error::StorageEngineShutdown)` once shutdown begins.
5. `Session::try_begin_trx()` still returns `Ok(None)` for the existing
   already-in-transaction case while the engine is running.
6. `Engine::shutdown()` returns `Error::StorageEngineShutdownBusy(...)` while
   extra `EngineRef` or `Session` holders still exist.
7. After those extra holders are dropped, retrying `Engine::shutdown()`
   succeeds.
8. `drop(engine)` without explicit shutdown succeeds when the engine is the
   last strong owner.
9. `drop(engine)` panics when extra engine references still exist.
10. Explicit shutdown hooks on `TransactionSystem`,
    `EvictableBufferPool`, `GlobalReadonlyBufferPool`, and
    `TableFileSystem` are safe to call more than once and safe on partially
    started state.
11. `cargo test -p doradb-storage`
12. `cargo test -p doradb-storage --no-default-features`

## Open Questions

Follow-up work remains outside this task scope:

1. graceful session and transaction draining, timeout policy, and forced
   shutdown behavior are now tracked by
   `docs/backlogs/000059-add-session-drain-and-forced-shutdown-policy-after-engine-shutdown-barrier.md`;
2. same-session overlapping-transaction admission is still incorrectly
   unenforced at runtime and is tracked by
   `docs/backlogs/000058-fix-session-single-active-transaction-enforcement.md`.
