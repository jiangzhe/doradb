---
id: 000165
title: Session Handle Registry Ownership
status: implemented
created: 2026-05-31
github_issue: 677
---

# Task: Session Handle Registry Ownership

## Summary

Implement RFC-0019 Phase 3 by moving strong session state into an
engine-owned session registry and turning public `Session` into a weak,
non-cloneable capability.

After this task, externally held session values must not keep `EngineInner`,
component guards, `SessionState`, or pool guards alive. Session operations
upgrade weak engine reachability internally, acquire engine admission, pin the
registry-owned session state for the operation, and release registry/admission
guards before callbacks, I/O, or `.await` points. This task also adds
`Session::close().await` as the graceful terminal path and defines idle,
active-transaction, and abandoned-session behavior.

Issue Labels:
- type:task
- priority:medium
- codex

Parent RFC:
- docs/rfcs/0019-weak-public-runtime-handles.md

RFC Phase:
- Phase 3: Session Handle Registry Ownership

Related Backlogs:
- docs/backlogs/000066-engine-scoped-weak-runtime-handles.md
- docs/backlogs/000113-transaction-cancellation-safety.md
- docs/backlogs/000115-explicit-session-lock-cache.md
- docs/backlogs/000116-general-session-runtime-pin-ownership.md

## Context

RFC-0019 changes the public runtime model so public handles are weak
capabilities and engine-owned registries hold strong runtime state. Phase 1
implemented the admitted-operation primitive and performance baseline harness.
Phase 2 removed the public cloneable `EngineRef` boundary while keeping
`EngineRef` as crate-private transitional runtime access.

The current `Session` is still a public strong owner:

- `Session` stores `Arc<SessionState>`.
- `SessionState` stores a strong crate-private `EngineRef`.
- `SessionState` owns `PoolGuards`.
- `ActiveTrx` stores `Arc<SessionState>` through `TrxContext`.
- session `Drop` releases session-owned logical locks because session state is
  currently public-handle-owned.

That shape keeps engine runtime state alive while public sessions remain live
and makes `engine.shutdown()` depend on users dropping session handles. Phase 3
must make sessions registry-owned while preserving enough transitional
transaction behavior for Phase 4 to migrate public transaction ownership
separately.

Phase Contract:
- Prerequisites: Phase 1 admission, cleanup-hint semantics, operation-boundary
  guidance, and `weak_handle_baseline` are available. Phase 2 has made
  `EngineRef` crate-private, so session operations can create transitional
  strong runtime pins internally without exposing them publicly.
- Phase-local choices:
  - keep the public type name `Session`, but make it a weak non-cloneable
    handle;
  - use monotonic, engine-local `SessionID` without generation because session
    ids are not reused in one engine runtime;
  - add an engine-owned `SessionRegistry`;
  - model session lifecycle as one enum, including active-transaction state;
  - make `close().await` succeed only for idle sessions and return an
    active-transaction error for active sessions;
  - make dropped-session cleanup best-effort and nonblocking.
- After this phase: public sessions are weak capabilities; idle and abandoned
  session cleanup is registry-owned; active transactions still use transitional
  strong runtime paths until Phase 4.
- Following phase preserved: Phase 4 still owns public transaction handle
  migration, transaction entry states, rollback-equivalent abandoned-transaction
  cleanup, terminal-operation cancellation safety, and shutdown policy for
  active/committing/rolling-back transactions.
- RFC phase-plan edits: none expected unless implementation discovers that
  session close or shutdown behavior must change Phase 4 prerequisites.

## Goals

1. Change public `Session` so it no longer stores `Arc<SessionState>`,
   `EngineRef`, `PoolGuards`, or any equivalent strong runtime owner.
2. Keep public `Session` non-cloneable.
3. Add a crate-private weak engine reachability wrapper for public session
   handles, for example `WeakEngineRef`, backed by `Weak<EngineInner>`.
4. Add `SessionRegistry` under `EngineInner`; it is the strong owner of
   `Arc<SessionState>` entries keyed by `SessionID`.
5. Remove the strong engine backreference from `SessionState`.
6. Replace the current standalone `in_trx` flag with a single session lifecycle
   enum such as:

   ```rust
   enum SessionLifecycle {
       RunningIdle,
       RunningActive { trx_id: TrxID },
       AbandonedIdle,
       AbandonedActive { trx_id: TrxID },
       Closed,
   }
   ```

7. Keep transaction ids in active lifecycle variants when practical, so
   terminal transaction cleanup can verify that it is completing the transaction
   that made the session active.
8. Add internal operation pinning for sessions:
   - upgrade weak engine reachability;
   - acquire engine admission;
   - look up and clone the registry-owned `Arc<SessionState>`;
   - validate lifecycle state;
   - drop admission and registry guards before `.await`, user callbacks,
     statement execution, blocking I/O, or retained registry guards.
9. Add `Session::close(&mut self).await`:
   - idle sessions transition to `Closed`, are removed from the registry, and
     release session-owned locks;
   - active sessions return an active-transaction error and remain active so
     the caller can commit or roll back the transaction, then retry close;
   - successful close is idempotent for the public handle.
10. Make public session `Drop` nonblocking, infallible, and non-panicking.
    Drop may best-effort mark the registry entry abandoned and remove it
    immediately only when idle.
11. Make engine shutdown/drop independent from live weak public session handles:
    shutdown should close admission, drain admissions, clean up registry-owned
    idle/abandoned-idle sessions before component shutdown, and continue to
    report busy for active transitional transaction/runtime pins.
12. Preserve existing user-facing session workflows through the new weak
    boundary:
    - create table/index/drop DDL through `Session`;
    - table lookup through `Session::get_table()` until Phase 5;
    - `begin_trx`, transaction commit/rollback, and statement DML;
    - explicit session table locks.
13. Preserve the RFC-0019 performance rule: weak upgrade, admission, registry
    lookup, and lifecycle validation are paid at public operation boundaries,
    not inside row/index/buffer hot loops.

## Non-Goals

1. Do not migrate public `ActiveTrx` or `Transaction` to weak handles. That is
   Phase 4.
2. Do not implement transaction cancellation safety, commit/rollback operation
   leases, rollback-equivalent transaction-handle abandonment, or detailed
   terminal transaction states. Those remain Phase 4 and backlog
   `000113` work.
3. Do not replace public `Arc<Table>` exposure or choose the final public table
   API shape. That is Phase 5.
4. Do not add a public engine-scoped weak handle.
5. Do not add broad forced shutdown policy or async engine shutdown policy.
   Shutdown may still report busy for active transitional transaction/runtime
   pins.
6. Do not redesign MVCC, redo logging, group commit, rollback, logical lock
   semantics, DDL behavior, checkpoint, recovery, purge, table file roots, or
   buffer-pool internals.
7. Do not add generation to `SessionID` in this phase. The task must document
   and preserve the non-reuse invariant instead.
8. Do not expose public weak-upgrade or strong-pinning APIs.

## Unsafe Considerations (If Applicable)

No new unsafe code is planned.

This task should be implementable in safe Rust by changing ownership,
registries, operation pinning, lifecycle state, tests, and public facade
signatures. If implementation unexpectedly adds or modifies unsafe code, apply
`docs/process/unsafe-review-checklist.md`, update adjacent `// SAFETY:`
comments, and refresh the unsafe inventory with:

```bash
tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md
```

Reference:
- docs/unsafe-usage-principles.md
- docs/process/unsafe-review-checklist.md

## Plan

1. Add crate-private weak engine reachability for session handles.
   - Work primarily in `doradb-storage/src/engine.rs` and
     `doradb-storage/src/session.rs`.
   - Introduce a small `WeakEngineRef` wrapper around `Weak<EngineInner>` or an
     equivalent private type.
   - Keep it crate-private and do not expose an upgrade API to library users.
   - Map failed upgrade to the existing lifecycle error vocabulary, attaching
     operation/session context where useful.

2. Add `SessionRegistry` to `EngineInner`.
   - Store registry entries keyed by monotonic `SessionID`.
   - Acceptable storage is `DashMap<SessionID, Arc<SessionState>>` or a
     `parking_lot::Mutex<HashMap<...>>`; choose the simpler shape that keeps
     lookup/remove critical sections short and does not hold guards across
     `.await`.
   - Add methods for insert, pin/lookup, close idle, abandon, transaction
     terminal completion, shutdown idle cleanup, and test-only snapshot helpers
     if needed.
   - Ensure registry code is the only strong owner of `SessionState` during
     normal session lifetime.

3. Change public `Session` into a weak handle.
   - Shape the public type around:

     ```rust
     pub struct Session {
         id: SessionID,
         engine: WeakEngineRef,
         closed: AtomicBool,
     }
     ```

     An interior flag can be `AtomicBool` or another small interior-mutability
     primitive if implementation needs `&self` methods to reject after close.
   - Do not derive or implement `Clone`.
   - Keep `Session::id()` as a cheap identity accessor.
   - Change `Session::in_trx()` to a checked lifecycle query returning
     `Result<bool>`, or add a checked crate-private helper and update internal
     correctness-sensitive call sites to use that helper. The task should not
     rely on a silent best-effort `false` when the engine is shut down, dropped,
     or the session is stale.
   - Remove or narrow public `Session::pool_guards()`. Internal code should get
     pool guards through an operation pin or a cloned guard bundle from pinned
     session state; public weak session handles must not expose a borrowed
     strong guard bundle.

4. Redesign `SessionState` around session-local state only.
   - Remove `engine_ref: EngineRef`.
   - Replace `in_trx: AtomicBool` with:

     ```rust
     lifecycle: Mutex<SessionLifecycle>
     ```

   - Keep:
     - `id: SessionID`;
     - `pool_guards: PoolGuards`;
     - `lock_manager: QuiescentGuard<LockManager>` or another narrow cleanup
       handle needed to release session-owned locks without an engine
       backreference;
     - `last_cts: AtomicU64`;
     - `active_insert_pages: Mutex<HashMap<TableID, (VersionedPageID, RowID)>>`.
   - Move session lock release into registry/state cleanup. `Drop` for
     `SessionState` may remain as a final safety net if it only performs
     nonblocking lock release through the stored lock-manager guard.

5. Add a session operation pin.
   - Introduce an internal type similar to:

     ```rust
     pub(crate) struct SessionPin {
         engine: EngineRef,
         state: Arc<SessionState>,
     }
     ```

   - The pin is not public and is not a weak-upgrade API.
   - Creating a pin must acquire admission and clone the registry entry under
     short critical sections only.
   - The admission token and registry guard must be dropped before returning a
     pin used by async work.
   - Use the pin to update DDL, `get_table`, `lock_table`, `unlock_table`, and
     internal helpers that currently call `session.engine()` or
     `session.pool_guards()`.

6. Update session begin-transaction behavior.
   - `Session::begin_trx(&mut self)` should use admission and registry state to
     validate `RunningIdle`.
   - It should create the transaction through crate-private transitional
     runtime access and transition lifecycle to
     `RunningActive { trx_id }` before the admitted begin operation is
     complete.
   - Existing `OperationError::ExistingTransaction` should still report
     overlapping transactions.
   - Beginning a transaction on `Closed`, `Abandoned*`, missing, shutdown, or
     dropped-engine session should return the appropriate lifecycle/operation
     error.

7. Add a transitional transaction-session reference.
   - Replace `ActiveTrx`/`TrxContext` ownership of `Arc<SessionState>` with a
     narrower transitional reference, for example:

     ```rust
     pub(crate) struct TrxSessionRef {
         engine: EngineRef,
         session_id: SessionID,
         session: Weak<SessionState>,
         pool_guards: PoolGuards,
     }
     ```

   - The exact fields may vary, but `ActiveTrx` should not strongly own
     `SessionState`.
   - It is acceptable for `ActiveTrx` to keep transitional strong engine and
     component guards until Phase 4; that is the explicit boundary between
     Phase 3 and Phase 4.
   - Transaction terminal paths must use this reference to mark the owning
     session idle after commit/rollback, or remove the registry entry if the
     session was abandoned while active.
   - Preserve existing commit, rollback, no-op discard, fatal rollback, and
     group-commit session completion semantics; only route session ownership
     through the registry-owned state.

8. Implement `Session::close(&mut self).await`.
   - Close must be explicit, idempotent after success, and non-destructive for
     active transactions.
   - If the session is `RunningIdle`, transition to `Closed`, remove it from
     the registry, release session locks, set the local closed flag, and return
     `Ok(())`.
   - If the session is `RunningActive { .. }`, return
     `OperationError::ExistingTransaction` or a similarly narrow operation
     error with context such as `close session with active transaction`.
   - If already closed through the same handle, return `Ok(())`.
   - If the engine is shut down or gone before close can pin state, return the
     lifecycle error; do not run async cleanup from `Drop` as a fallback.

9. Implement public `Session` drop abandonment.
   - `Drop` must be infallible, nonblocking, and non-panicking.
   - If the local closed flag is already set, do nothing.
   - Otherwise, best-effort upgrade the weak engine and ask the registry to
     abandon this `SessionID`.
   - Registry abandonment rules:
     - `RunningIdle` -> remove entry and release locks;
     - `RunningActive { trx_id }` -> `AbandonedActive { trx_id }`;
     - already `Closed` or missing -> no-op.
   - If weak upgrade fails or registry cleanup cannot proceed, do nothing.
     Correctness must not depend on `Drop` running.

10. Update engine shutdown and final drop interaction.
    - In `Engine::finalize_shutdown`, after admission closes and drains but
      before `ComponentRegistry::shutdown_all()`, ask the session registry to
      remove idle and abandoned-idle sessions so their pool/lock guards release
      before component shutdown.
    - If active sessions remain, preserve transitional busy behavior. The
      active public transaction should still keep a strong `EngineRef`, so the
      existing strong-count busy check should catch normal active-transaction
      cases. Add diagnostics/tests for the registry active-session case as
      needed.
    - Do not make live weak public `Session` handles count as shutdown-busy.

11. Update DDL, persistence, recovery tests, and call sites.
    - Replace calls to `session.engine().clone()` with operation pins or
      specific helper methods.
    - Replace `session.pool_guards()` borrowing in async flows with a cloned
      `PoolGuards` from an operation pin when the borrow would cross `.await`.
    - Update `SessionDdlContext` so it validates lifecycle and obtains the
      owner/engine/guards through the new pinning path.
    - Keep transaction and table semantics unchanged outside the necessary
      ownership plumbing.

12. Update public API exports, rustdoc, examples, and tests.
    - Keep `pub use session::Session`.
    - Document `Session` as a weak non-cloneable public session capability.
    - Update `doradb-storage/tests/public_api_smoke.rs` to explicitly
      `close().await` the session before shutdown where appropriate.
    - Update `doradb-storage/examples/weak_handle_baseline.rs` so setup and
      measurement close sessions or intentionally rely on weak-handle cleanup
      in tested cases.

13. Measure performance.
    - Run the Phase 1 baseline before and after the implementation when
      practical:

      ```bash
      cargo run -p doradb-storage --example weak_handle_baseline -- --iterations 10 --scan-rows 10 --out-dir target/weak-handle-baseline-before
      cargo run -p doradb-storage --example weak_handle_baseline -- --iterations 10 --scan-rows 10 --out-dir target/weak-handle-baseline-after
      ```

    - Review at least session begin, statement execution, and table lookup.
    - There is no hard threshold in this task, but any obvious regression or
      repeated registry lookup inside row/index/buffer loops is a blocker.

14. Keep docs synchronized.
    - During `task resolve`, update RFC-0019 Phase 3 with the task path, issue
      number if available, phase status, implementation summary, and any real
      phase-plan change.
    - Do not update Phase 4 or Phase 5 assumptions unless implementation
      changes their prerequisites.

## Implementation Notes

Implemented RFC-0019 Phase 3 session registry ownership in the storage crate.
Public `Session` is now a weak, non-cloneable capability with `SessionID`,
`WeakEngineRef`, and a local closed flag. `EngineInner` owns a `SessionRegistry`
that stores strong `Arc<SessionState>` entries, and `Engine::new_session()`
creates registry-owned session state rather than returning public strong state.

Session lifecycle is now represented by one `SessionLifecycle` enum covering
running idle, running active with `TrxID`, abandoned idle, abandoned active with
`TrxID`, and closed states. `Session::pin()` upgrades weak engine reachability,
acquires admission, looks up registry-owned state, validates lifecycle, and
returns a crate-private `SessionPin`. `Session::close().await`, session `Drop`,
registry abandonment, transaction terminal cleanup, and shutdown idle cleanup
all route through this lifecycle. `Session::in_trx()` returns `Result<bool>` so
closed, stale, and shutdown sessions are reported as errors instead of panics.

Transaction integration uses transitional `TrxSessionRef`: active transactions
keep strong `EngineRef` runtime liveness for Phase 3, but only weakly reference
`SessionState` while retaining cached `PoolGuards`. Commit and rollback terminal
cleanup calls back into `SessionRegistry`; `finish_trx_commit` updates
`last_cts` only when the completing `TrxID` still owns the session. Test helpers
that construct `TrxSessionRef` now keep their backing `Arc<SessionState>` alive
for the transaction scope.

Engine shutdown now closes admission, drains admissions/runtime refs, waits for
transitional runtime pins in `shutdown()`, keeps nonblocking `try_shutdown()`,
and removes idle/abandoned-idle registry sessions before component shutdown.
Weak public sessions no longer keep `EngineInner`, component guards, session
state, or pool guards alive. `EngineLifecycle` uses a `shutdown_lock` to ensure
only one shutdown caller performs final teardown.

Session lock and table/persistence paths were updated to use operation pins and
short-lived guard bundles. `LockManager::acquire_grouped_table_locks` centralizes
metadata/data table lock acquisition. `TablePersistence::checkpoint_readiness`
now returns `Result<CheckpointReadiness>` and propagates `Session::pin` failures
instead of panicking. Test-only session accessors were moved under
`#[cfg(test)]` extension helpers instead of production methods.

Post-implementation review findings were fixed before resolve:
- stale/late transaction commit completion can no longer mutate `last_cts`;
- checkpoint readiness on closed sessions now returns an operation error;
- transaction test helpers no longer create detached weak session references.

Deferred follow-ups created during implementation:
- `docs/backlogs/000115-explicit-session-lock-cache.md` for a session-local
  explicit table lock cache, avoiding broad `LockManager::release_owner` scans;
- `docs/backlogs/000116-general-session-runtime-pin-ownership.md` for a general
  session runtime pin model that keeps engine runtime liveness across long
  async operation boundaries and future transaction refactors.

Validation completed:
- `cargo fmt --check`
- `git diff --check`
- `cargo check -p doradb-storage --all-targets`
- `cargo clippy -p doradb-storage --all-targets -- -D warnings`
- `cargo nextest run -p doradb-storage` (876 tests passed)
- `tools/coverage_focus.rs --path doradb-storage/src/session.rs --path doradb-storage/src/engine.rs --path doradb-storage/src/table/persistence.rs --path doradb-storage/src/trx/mod.rs --path doradb-storage/src/trx/stmt.rs --path doradb-storage/src/lock/mod.rs`
  produced 95.09% deduplicated focused coverage. Per-file coverage was:
  `session.rs` 95.44%, `engine.rs` 96.05%, `table/persistence.rs` 84.27%,
  `trx/mod.rs` 96.89%, `trx/stmt.rs` 98.36%, and `lock/mod.rs` 97.83%.
- `cargo run -p doradb-storage --release --example weak_handle_baseline`
  completed with the example's default parameters and wrote
  `target/weak-handle-baseline/baseline.csv`. Default release averages were:
  session begin 79 ns, statement exec 36 ns, table lookup 49 ns, point lookup
  501 ns, insert 1065 ns, update 696 ns, delete 1101 ns, and table scan
  213657 ns.

Checklist outcome:
- Reliability: pass. Task-required lifecycle, shutdown, close/drop, transaction
  reuse, stale-session, and regression paths are covered; validation and
  focused coverage passed.
- Security: n/a. No unsafe code was added or modified.
- Performance: pass with deferred follow-ups. Registry lookup is paid at public
  operation boundaries; session-lock cache and long-lived session runtime pin
  design are tracked separately.
- Feature completeness: pass. Phase 3 goals are implemented while Phase 4/5
  transaction/table public-handle migrations remain non-goals.
- Documentation: pass. Public/crate-public API comments and core lifecycle
  comments were updated where behavior changed.
- Test-only code: pass. New test helpers remain in `#[cfg(test)]` modules.
- Complexity: pass. Shared registry and lock-manager helpers reduce repeated
  boilerplate; larger existing checkpoint logic remains outside this task's
  refactor scope.

## Impacts

- `doradb-storage/src/engine.rs`
  - Add weak engine reachability support, `SessionRegistry` ownership in
    `EngineInner`, `Engine::new_session` registry insertion, and shutdown idle
    session cleanup.
- `doradb-storage/src/session.rs`
  - Primary implementation location for weak `Session`, `SessionState`,
    `SessionLifecycle`, `SessionPin`, `Session::close`, and abandonment.
- `doradb-storage/src/trx/mod.rs`
  - Replace strong `Arc<SessionState>` transaction attachment with a
    transitional session reference and update session completion paths.
- `doradb-storage/src/trx/sys.rs`
  - Preserve begin/commit/rollback semantics while routing session lifecycle
    completion through registry-owned state.
- `doradb-storage/src/trx/log.rs` and `doradb-storage/src/trx/group.rs`
  - Carry the new transactional session completion reference through
    group-commit paths instead of `Arc<SessionState>`.
- `doradb-storage/src/catalog/table.rs` and `doradb-storage/src/catalog/index.rs`
  - Update DDL context, pool-guard access, and session engine access to use
    operation pins.
- `doradb-storage/src/table/persistence.rs`, `doradb-storage/src/table/gc.rs`,
  and recovery/test helpers
  - Replace direct session engine/guard borrowing where needed.
- `doradb-storage/src/error.rs`
  - Add only narrow operation/lifecycle vocabulary if existing
    `OperationError::ExistingTransaction`, `OperationError::NotSupported`, and
    `LifecycleError::Shutdown` are not precise enough.
- `doradb-storage/examples/weak_handle_baseline.rs`
  - Update to the weak session API and use it for performance comparison.
- `doradb-storage/tests/public_api_smoke.rs`
  - Update public facade coverage for weak sessions and explicit close.

## Test Cases

1. `Engine::new_session()` returns a weak non-cloneable public session handle
   and session ids remain monotonic across closed/dropped sessions.
2. A live idle public `Session` handle no longer makes `engine.shutdown()`
   return `ShutdownBusy`.
3. An idle session `close().await` removes the registry entry, releases
   session-owned logical locks, and makes later operations on that handle fail
   or no-op according to the documented closed-handle contract.
4. Calling `close().await` while a transaction is active returns the selected
   active-transaction error and leaves the transaction able to commit or roll
   back.
5. After an active transaction commits or rolls back, retrying
   `close().await` succeeds.
6. Dropping an idle session handle releases/removes the registry entry through
   best-effort cleanup; shutdown can proceed while the dropped handle is gone.
7. Dropping a session handle before its transaction handle does not invalidate
   the transaction. The transaction can still commit or roll back, and terminal
   cleanup removes the abandoned session entry.
8. Dropping both session and transaction handles without explicit transaction
   terminal cleanup must not perform async rollback from `Drop`; existing
   transaction drop assertions/behavior remain in force until Phase 4.
9. `Session::begin_trx()` still rejects overlapping transactions in the same
   session.
10. The same session can begin a new transaction after commit, rollback,
    readonly commit, no-op discard, and fatal rollback cleanup according to
    existing behavior.
11. Distinct sessions can hold overlapping active transactions.
12. Session operations after engine shutdown begins fail with lifecycle
    shutdown and do not recreate registry entries.
13. Stale/missing/closed session operation paths return deterministic errors
    and do not panic.
14. DDL paths still reject implicit commit due to active transaction and still
    release session-owned locks on close/drop.
15. Public smoke tests cover create table, table lookup, transaction DML, close,
    and shutdown through the weak session facade.
16. Run routine validation:

    ```bash
    cargo fmt
    cargo clippy -p doradb-storage --all-targets -- -D warnings
    cargo nextest run -p doradb-storage
    tools/coverage_focus.rs --path doradb-storage/src/session.rs --path doradb-storage/src/engine.rs
    git diff --check
    ```

17. Focused coverage for changed Rust files should meet the repository's 80%
    focused coverage bar or explain definition-heavy exceptions with consumer
    coverage evidence.

## Open Questions

No Phase 3 design question remains open after selecting the single
`SessionLifecycle` enum and the retryable active-session close policy.

Deferred follow-ups:
- Transaction cancellation safety and the final transaction terminal-state
  machine remain tracked by
  `docs/backlogs/000113-transaction-cancellation-safety.md` and RFC-0019
  Phase 4.
- Session-level explicit lock cache optimization remains tracked by
  `docs/backlogs/000115-explicit-session-lock-cache.md`.
- A general session runtime pin ownership model for long-running async
  operations and future transaction refactors remains tracked by
  `docs/backlogs/000116-general-session-runtime-pin-ownership.md`.
- Public table handle migration and removal of public `Arc<Table>` exposure
  remain RFC-0019 Phase 5 work.
- Final public strong-handle cleanup and documentation sync remain RFC-0019
  Phase 6 work.
