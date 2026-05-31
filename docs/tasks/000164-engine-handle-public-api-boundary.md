---
id: 000164
title: Engine Handle Public API Boundary
status: implemented
created: 2026-05-31
github_issue: 673
---

# Task: Engine Handle Public API Boundary

## Summary

Implement RFC-0019 Phase 2 by removing the public cloneable strong engine
runtime handle from `doradb-storage`'s external API.

The chosen Phase 2 boundary is owner-only: `Engine` remains the public engine
owner and session factory, no public weak engine-scoped handle is added, and
the existing strong `EngineRef` plus `Engine::new_ref()` remain crate-private
transitional plumbing to reduce implementation churn before the later session,
transaction, and table handle phases.

After this task, external users should be able to build an `Engine`, create
sessions through `Engine::new_session`, use session/table/transaction APIs, and
call `engine.shutdown()`, but they should not be able to name, clone, or
obtain `EngineRef` from the public facade.

## Context

RFC-0019 redesigns public runtime handles so user-facing handles do not strongly
own engine runtime state. Phase 1 already factored engine admission into a
crate-private scoped primitive and added `weak_handle_baseline` for operation
boundary measurement. It intentionally left `EngineRef` public and strong so
Phase 2 could choose the concrete engine boundary.

The current public API still exports `EngineRef` from `doradb-storage/src/lib.rs`.
`Engine::new_ref()` returns a cloneable strong runtime handle, `EngineRef`
offers public `new_session()` and `get_table()` helpers, and `Session::engine()`
publicly exposes the strong runtime handle stored in `SessionState`. Internally,
that same `EngineRef` remains useful transitional access for DDL, transaction
commit/rollback, table lookup, lock access, pool guards, and tests.

This task chooses not to add a public weak `EngineHandle`. Current supported
external usage does not need a detached engine-scoped session factory: users can
hold `Engine` when they need to create sessions, then use `Session` for DDL,
table lookup, and transactions. Production background workers also do not need
an engine handle; they are component-owned and retain only narrow component
guards, channels, shutdown flags, and worker handles. Adding a public weak
engine handle now would create a compatibility surface that later phases would
have to preserve without a concrete use case.

Phase 2 must still keep the RFC transition honest. Public `Session`, active
transaction, and table paths may continue to retain strong runtime reachability
until their own phases land. Therefore this task removes only the public strong
engine boundary; it does not make shutdown complete while public sessions,
transactions, or table handles are still alive.

Issue Labels:
- type:task
- priority:medium
- codex

Parent RFC:
- docs/rfcs/0019-weak-public-runtime-handles.md

RFC Phase:
- Phase 2: Engine Handle Public API Boundary

Phase Contract:
- Prerequisites: Phase 1 admission, lifecycle error mapping, cleanup-hint
  semantics, operation-boundary guidance, and `weak_handle_baseline` are
  available.
- Phase-local choices: do not add a public engine-scoped weak capability; keep
  `EngineRef` and `Engine::new_ref()` crate-private to reduce churn; make
  `Engine` the only public engine owner/session factory; expose graceful
  shutdown as synchronous `engine.shutdown()`.
- After this phase: external users can no longer obtain a fresh cloneable
  public `EngineRef` equivalent. Legacy public session, transaction, and table
  paths may still retain strong runtime reachability until their later phases.
- Following phase preserved: Phase 3 can introduce an engine-owned session
  registry and weak public session handle without first unwinding a public
  engine-handle compatibility bridge.

Rejected Alternatives:
- Public weak `EngineHandle`: rejected for this task because no current public
  use case or production background worker needs an engine-scoped detached
  handle. Later phases should introduce weak public handles only where there is
  a concrete resource identity: session, transaction, or table.
- Full `EngineRef` removal: rejected for this task because replacing all
  internal strong runtime access with narrow operation contexts would spill into
  session and transaction ownership redesign.
- Async engine shutdown now: rejected for this task because Phase 2 shutdown
  still performs synchronous owner-side finalization. Revisit an async shutdown
  API or async variant when later weak-handle phases introduce real nonblocking
  cleanup waits. Follow-up: `docs/backlogs/000114-evaluate-async-engine-shutdown-api.md`.

## Goals

1. Remove `EngineRef` from the external `doradb-storage` public facade.
2. Keep the existing `EngineRef` type name and `Engine::new_ref()` helper as
   crate-private transitional internals to reduce churn.
3. Prevent external callers from accessing a strong engine runtime handle
   through `Session::engine()` or any equivalent public method.
4. Keep current public user workflows available through `Engine` and `Session`,
   especially `Engine::new_session`, `Session::get_table`, DDL methods,
   transaction begin/commit/rollback, and statement DML.
5. Keep the public graceful shutdown API synchronous:
   `engine.shutdown()`.
6. Preserve existing lifecycle admission behavior, shutdown-busy behavior, and
   storage-poison behavior except for public API shape.
7. Preserve session, transaction, and table handle ownership semantics as
   transitional strong paths until their RFC-0019 phases.
8. Update the RFC-0019 baseline example and public smoke coverage so they no
   longer require public `EngineRef`.
9. Measure boundary-cost impact for the changed public operation paths using
   the Phase 1 baseline harness.

## Non-Goals

1. Do not add a public weak `EngineHandle` or any public weak engine upgrade
   surface.
2. Do not remove or rename crate-private `EngineRef` unless implementation
   discovers a very small mechanical reason. The requested direction is to keep
   `EngineRef` and `new_ref()` internally.
3. Do not create an engine-owned session registry, weak `SessionHandle`, or
   `session.close().await`; those belong to Phase 3.
4. Do not migrate `ActiveTrx`/`Transaction` ownership or terminal-operation
   state; that belongs to Phase 4.
5. Do not replace public `Arc<Table>` exposure, table DML signatures, or table
   runtime ownership; that belongs to Phase 5.
6. Do not redesign component shutdown ordering, worker shutdown, recovery, DDL,
   MVCC, table GC, checkpoint, table-file roots, or buffer-pool internals.
7. Do not preserve source compatibility for public `EngineRef` callers. Public
   API compatibility is not required for this active development phase.
8. Do not make test-only internals public to compensate for the API narrowing.
9. Do not add async engine shutdown in this task. Async shutdown policy is
   deferred to `docs/backlogs/000114-evaluate-async-engine-shutdown-api.md`.

## Unsafe Considerations (If Applicable)

No new unsafe code is planned.

This task should be a visibility, public API, lifecycle facade, and test update
only. If implementation unexpectedly touches unsafe blocks, apply
`docs/process/unsafe-review-checklist.md`, update adjacent `// SAFETY:`
comments, and refresh the unsafe inventory with:

```bash
tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md
```

Reference:
- docs/unsafe-usage-principles.md
- docs/process/unsafe-review-checklist.md

## Plan

1. Narrow the root public facade.
   - In `doradb-storage/src/lib.rs`, stop re-exporting `EngineRef`.
   - Keep `Engine` publicly re-exported.
   - Keep existing public `Session`, transaction, statement, table, schema,
     row operation, value, config, lock mode, and error exports unchanged unless
     the compiler forces a narrow visibility correction.

2. Make `EngineRef` a crate-private transitional runtime handle.
   - In `doradb-storage/src/engine.rs`, change `pub struct EngineRef` to
     `pub(crate) struct EngineRef`.
   - Keep `#[derive(Clone)]` and the internal strong `Arc<EngineInner>` storage.
   - Keep `Deref<Target = EngineInner>` if it remains useful for crate-internal
     access.
   - Keep `Engine::new_ref()` but make it `pub(crate)`.
   - Prefer changing `EngineRef::new_session()` and `EngineRef::get_table()` to
     `pub(crate)` if all call sites remain crate-internal after the public
     export is removed.
   - Update doc comments so `EngineRef` is described as crate-private
     transitional runtime access, not as a public storage-engine handle.

3. Hide session-to-engine runtime exposure from external users.
   - In `doradb-storage/src/session.rs`, make `Session::engine()` crate-private.
   - Keep `SessionState::engine()` crate-private.
   - Keep `Session::get_table()` public so external users can still obtain the
     current transitional strong `Arc<Table>` until Phase 5.
   - Keep `SessionDdlContext` using internal `EngineRef` as needed.
   - Do not change session ownership, `SessionState` storage, `PoolGuards`, or
     active transaction flags in this task.

4. Keep the public shutdown facade synchronous.
   - Keep public `Engine::shutdown` as `pub fn shutdown(&self) -> Result<()>`.
   - Keep the current synchronous owner-side finalization in a private helper,
     for example `finalize_shutdown(&self) -> Result<()>`, so `Drop` can still
     perform deterministic teardown.
   - Do not redesign component shutdown or worker joins.
   - Do not make the receiver `&mut self` or consuming `self`: `&mut self`
     complicates shared owner/control-plane use without adding correctness to
     the internally synchronized lifecycle gate, and consuming `self` would make
     transitional `ShutdownBusy` retry awkward unless the API returned the
     engine on error.

5. Update the baseline example.
   - Run the existing baseline before code changes when practical:

     ```bash
     cargo run -p doradb-storage --example weak_handle_baseline -- --iterations 10 --scan-rows 10 --out-dir target/weak-handle-baseline-before
     ```

   - Update `doradb-storage/examples/weak_handle_baseline.rs` so it no longer
     calls public `Engine::new_ref()` or `EngineRef::get_table()`.
   - Preserve a `table_lookup` row by measuring `Session::get_table()` through a
     reusable session.
   - Run the updated example after code changes and record the comparison in
     implementation notes during resolve:

     ```bash
     cargo run -p doradb-storage --example weak_handle_baseline -- --iterations 10 --scan-rows 10 --out-dir target/weak-handle-baseline-after
     ```

   - This task does not require a hard performance threshold, but large
     unexpected changes to session begin, table lookup, or statement execution
     should be investigated before resolve.

6. Update tests and internal call sites.
   - Update engine lifecycle tests for the synchronous public shutdown boundary.
   - Keep crate-internal tests that use `Engine::new_ref()` valid, because the
     helper remains crate-private.
   - Add or adjust a test showing owner drop without explicit shutdown still
     finalizes when no legacy strong public handles remain.
   - Preserve a test showing shutdown remains busy when a transitional strong
     internal runtime reference is alive.
   - Preserve or add coverage showing public `Session` operations reject after
     shutdown admission closes.
   - Update public API smoke tests so they only use the intended public facade:
     `EngineConfig`, `Engine`, `Session`, table lookup through `Session`, and
     `engine.shutdown()`.

7. Verify the public boundary mechanically.
   - Search the integration tests and examples for public `EngineRef` usage and
     remove any remaining dependency on it.
   - Confirm no root public re-export exposes `EngineRef`.
   - Rely on `cargo check -p doradb-storage --all-targets` and the integration
     smoke test to catch accidental public signature leaks.

8. Keep docs synchronized.
   - Update this task during implementation only through `task resolve`.
   - During `task resolve`, update RFC-0019 Phase 2 with this task path,
     issue/status/summary, and the resolved phase-local choice: no public
     engine-scoped weak capability.
   - Do not update Phase 3 or later assumptions unless implementation uncovers
     a real prerequisite change.

## Implementation Notes

Implemented RFC-0019 Phase 2 with an owner-only public engine boundary:

- `doradb-storage/src/lib.rs` now re-exports `Engine` without re-exporting
  `EngineRef`.
- `doradb-storage/src/engine.rs` keeps `EngineRef`, `Engine::new_ref()`,
  `EngineRef::new_session()`, and `EngineRef::get_table()` as crate-private
  transitional runtime access.
- `doradb-storage/src/session.rs` hides `Session::engine()` and `SessionState`
  from external callers while preserving public `Session::get_table()`,
  transaction, DDL, and statement DML workflows.
- Public engine shutdown remains synchronous as
  `pub fn shutdown(&self) -> Result<()>`; owner-side finalization is shared
  with `Drop` through `finalize_shutdown()`.
- `doradb-storage/examples/weak_handle_baseline.rs` now measures
  `table_lookup` through a reusable public `Session` instead of public
  `EngineRef`.
- `doradb-storage/tests/public_api_smoke.rs` now exercises the supported public
  facade without importing or naming `EngineRef`.

Lifecycle coverage was updated so shutdown closes public session admission,
`Session::get_table()` reports `LifecycleError::Shutdown` after admission is
closed, shutdown remains busy while transitional strong runtime references are
alive, and owner drop without explicit shutdown succeeds when no extra runtime
references remain.

Validation completed:

```bash
cargo fmt
cargo check -p doradb-storage --all-targets
cargo clippy -p doradb-storage --all-targets -- -D warnings
cargo nextest run -p doradb-storage
tools/coverage_focus.rs --path doradb-storage/src/engine.rs --path doradb-storage/src/session.rs
git diff --check
```

Results:
- `cargo nextest run -p doradb-storage`: 865 tests passed.
- Focused coverage:
  - combined: 1049/1084 lines, 96.77%
  - `doradb-storage/src/engine.rs`: 867/902 lines, 96.12%
  - `doradb-storage/src/session.rs`: 182/182 lines, 100.00%
- No unsafe code was added or modified.

The Phase 1 baseline harness was run before and after the public boundary
change:

```bash
cargo run -p doradb-storage --example weak_handle_baseline -- --iterations 10 --scan-rows 10 --out-dir target/weak-handle-baseline-before
cargo run -p doradb-storage --example weak_handle_baseline -- --iterations 10 --scan-rows 10 --out-dir target/weak-handle-baseline-after
```

Observed after-change sample:
- `session_begin`: 408 ns average
- `statement_exec`: 412 ns average
- `table_lookup`: 399 ns average
- `point_lookup`: 8924 ns average
- `insert`: 23325 ns average
- `update`: 9716 ns average
- `delete`: 11804 ns average
- `table_scan`: 24087 ns average

The before-change `table_lookup` sample was 396 ns average, so the public
boundary change did not show a material table lookup regression in this smoke
run.

Created follow-up backlog:
- `docs/backlogs/000114-evaluate-async-engine-shutdown-api.md` tracks whether
  later weak-handle phases need an async engine shutdown API or explicit async
  variant once there is real nonblocking cleanup work to wait for.

## Impacts

- `doradb-storage/src/lib.rs`
  - Public facade narrowing: remove `EngineRef` from root exports.
- `doradb-storage/src/engine.rs`
  - Primary change location for crate-private `EngineRef`, crate-private
    `Engine::new_ref()`, public `Engine::shutdown`, lifecycle docs, and
    engine lifecycle tests.
- `doradb-storage/src/session.rs`
  - Hide `Session::engine()` from external callers while preserving
    `Session::get_table()` and existing session internals.
- `doradb-storage/src/catalog/table.rs`
  - Expected to keep using crate-private `EngineRef` through
    `SessionDdlContext`; no ownership redesign planned.
- `doradb-storage/src/catalog/index.rs`
  - Expected to keep using crate-private `EngineRef` for index DDL helpers; no
    ownership redesign planned.
- `doradb-storage/src/trx/mod.rs` and `doradb-storage/src/trx/stmt.rs`
  - Expected to keep using crate-private engine reachability for commit,
    rollback, statement table cache, lock manager access, and table-root purge
    requests. Do not migrate transaction ownership.
- `doradb-storage/src/table/gc.rs` and `doradb-storage/src/table/persistence.rs`
  - May require visibility-only updates if they currently use public
    `Session::engine()` from inside the crate.
- `doradb-storage/examples/weak_handle_baseline.rs`
  - Replace public `EngineRef` lookup measurement with session-based lookup and
    update shutdown call shape.
- `doradb-storage/tests/public_api_smoke.rs`
  - Ensure the public facade works without `EngineRef` and uses synchronous
    shutdown.
- `docs/rfcs/0019-weak-public-runtime-handles.md`
  - Resolve-time phase sync target.

## Test Cases

1. Public API smoke test builds an engine, creates a session, creates a table,
   obtains the table through `Session::get_table()`, performs insert/lookup/
   update/scan/delete through statement DML, drops public handles, and calls
   `engine.shutdown()`.
2. External-style code no longer imports or names `doradb_storage::EngineRef`.
   The root facade should export `Engine` but not `EngineRef`.
3. `Engine::shutdown()` remains idempotent and rejects new public sessions
   after admission closes.
4. `Session::begin_trx()` and `Session::get_table()` return lifecycle shutdown
   errors after owner-side shutdown has closed admission.
5. Crate-internal `Engine::new_ref()` still works while running and still
   rejects after shutdown admission closes.
6. Shutdown remains busy while a transitional strong internal `EngineRef` or
   legacy strong public session/transaction path keeps `EngineInner` alive.
7. Engine owner drop without explicit shutdown succeeds when no legacy strong
   runtime handles remain.
8. Engine owner drop behavior with leaked transitional strong runtime refs keeps
   the existing fatal owner-contract behavior until later phases replace those
   strong public handle paths.
9. `weak_handle_baseline` runs after it is updated and still reports at least:
   `session_begin`, `statement_exec`, `table_lookup`, `point_lookup`, `insert`,
   `update`, `delete`, and `table_scan`.
10. Routine validation passes:

```bash
cargo fmt
cargo check -p doradb-storage --all-targets
cargo clippy -p doradb-storage --all-targets -- -D warnings
cargo nextest run -p doradb-storage
```

11. Focused coverage should be checked for changed production paths:

```bash
tools/coverage_focus.rs --path doradb-storage/src/engine.rs --path doradb-storage/src/session.rs
```

## Open Questions

No blocking open questions for Phase 2.

Known follow-up boundaries:
1. Phase 3 owns replacing public `Session` strong runtime reachability with an
   engine-owned session registry and weak public session handle.
2. Phase 4 owns transaction handle migration and cancellation-safe terminal
   operation state.
3. Phase 5 owns removing public `Arc<Table>` exposure and selecting the final
   table access shape.
4. Async engine shutdown policy is deferred to
   `docs/backlogs/000114-evaluate-async-engine-shutdown-api.md`.
