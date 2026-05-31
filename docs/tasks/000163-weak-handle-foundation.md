---
id: 000163
title: Weak Handle Foundation
status: proposal
created: 2026-05-30
github_issue: 671
---

# Task: Weak Handle Foundation

## Summary

Implement RFC-0019 Phase 1 by establishing the minimum shared foundation for
future weak public runtime handles, while preserving the current public API and
avoiding speculative handle infrastructure.

This task should factor and test the engine admission boundary, document the
shared weak-handle contracts that later phases must follow, and add reusable
performance baseline support for current public operation paths. It must not
migrate `EngineRef`, `Session`, `ActiveTrx`/`Transaction`, or `Table` to weak
public handles yet.

## Context

RFC-0019 redesigns the storage runtime API so public handles are weak,
non-cloneable capabilities. The current code still exposes strong runtime
ownership: `EngineRef` is a cloneable `Arc<EngineInner>`, `Session` owns an
`Arc<SessionState>` that stores a strong `EngineRef`, transactions keep strong
session reachability, and table lookup returns `Arc<Table>`.

The current engine already has a useful starting point. `EngineLifecycle` stores
the lifecycle state, an admission `RwLock`, and a finalize lock; public session
and runtime-handle creation enter through `EngineInner::with_running_admission`.
Shutdown closes admission and then requires `Arc::strong_count(inner) == 1`
before component shutdown proceeds. Phase 1 should make the admission boundary
clearer and reusable for later weak-handle phases without changing the current
strong public handles.

`Statement` already provides the right operation boundary for later table weak
handles. `ActiveTrx::exec` creates a borrowed `Statement`, and statement table
methods currently accept borrowed `&Table`. Later phases should be able to
resolve weak table/session/transaction capabilities once at the public operation
or statement-binding boundary, then reuse borrowed strong access in existing
hot paths.

Issue Labels:
- type:task
- priority:medium
- codex

Parent RFC:
- docs/rfcs/0019-weak-public-runtime-handles.md

RFC Phase:
- Phase 1: Weak Handle Foundation And Performance Baselines

Related Backlogs:
- docs/backlogs/000066-engine-scoped-weak-runtime-handles.md
- docs/backlogs/000113-transaction-cancellation-safety.md

## Goals

1. Preserve current public behavior and signatures for `EngineRef`, `Session`,
   `ActiveTrx`/`Transaction`, `Statement`, and `Table`.
2. Factor the existing engine admission path into a small crate-private
   admitted-operation primitive that later phases can reuse.
3. Keep the admitted-operation primitive synchronous and scoped to lifecycle
   validation plus immediate registry lookup or strong pinning. It must not
   encourage holding blocking locks, registry guards, or lifecycle guards across
   user callbacks, statement execution, I/O waits, or `.await` points.
4. Add tests proving admission rejects work after shutdown begins and that the
   refactored admission path preserves existing shutdown-busy behavior for live
   strong runtime references.
5. Define cleanup-hint semantics as a narrow contract for later phases:
   cleanup hints are best-effort, idempotent, non-blocking, non-panicking, and
   never authoritative for correctness.
6. Define the identity policy for later phases without implementing generation
   machinery in this task. Each handle-migration phase must either prove ID
   non-reuse/non-ambiguity for that handle type or add generation in that phase.
7. Define the operation-lease contract for future mutable session/transaction
   cores without adding a production generic lease type unless it is used by
   code in this task. The contract must cover stable entry visibility,
   checked-out/busy/terminal/in-progress vocabulary, and cancellation/drop
   return semantics at the design level.
8. Add reusable performance baseline support for current operation paths that
   later weak-handle phases will affect:
   - session begin;
   - transaction statement execution;
   - table lookup;
   - point lookup;
   - insert;
   - update;
   - delete;
   - table scan.
9. Record that Phase 2 can rely on the admitted-operation primitive and
   baseline harness, but cannot rely on a prebuilt identity/generation framework
   or cleanup worker.

## Non-Goals

1. Do not migrate public `EngineRef` to a weak handle.
2. Do not migrate public `Session`, `ActiveTrx`/`Transaction`, `Statement`, or
   `Table` handle ownership.
3. Do not add an engine-owned session registry, transaction registry, or table
   handle registry.
4. Do not implement a generation counter, generation token, generic identity
   framework, or tombstone registry in Phase 1.
5. Do not add a public weak-upgrade API or any API that exposes internal strong
   pinning to library users.
6. Do not add a cleanup background worker unless a non-speculative current code
   path in this task uses it and tests it.
7. Do not redesign transaction commit, rollback, statement cancellation,
   session close, table DDL, checkpoint, recovery, purge, or buffer-pool
   internals.
8. Do not reintroduce example binaries or widen the public API only to support
   measurement.
9. Do not add a CI performance gate or hard regression threshold in this task.

## Unsafe Considerations (If Applicable)

No new unsafe code is planned.

The intended implementation should stay in safe Rust by refactoring lifecycle
admission, adding tests, documenting contracts, and adding a benchmark/baseline
tool. If implementation unexpectedly touches unsafe-sensitive runtime paths,
apply `docs/process/unsafe-review-checklist.md`, update adjacent `// SAFETY:`
comments, and refresh the unsafe inventory with:

```bash
tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md
```

Reference:
- docs/unsafe-usage-principles.md
- docs/process/unsafe-review-checklist.md

## Plan

1. Refactor engine admission with minimal production code.
   - Work primarily in `doradb-storage/src/engine.rs`.
   - Keep `EngineLifecycle`, `EngineLifecycleState`, and the existing
     admission `RwLock` model unless implementation proves a small rename or
     wrapper is necessary.
   - Replace or wrap `EngineInner::with_running_admission` with a clearer
     crate-private admitted-operation API. Acceptable shapes include a
     synchronous closure helper, a short-lived RAII token, or both if both are
     actually used.
   - The admitted-operation API must validate lifecycle state and runtime
     health before it lets the caller perform immediate work.
   - The token/helper must be documented as unsuitable for holding across
     `.await`, user callbacks, statement execution, blocking I/O, or registry
     guard retention.
   - Preserve current behavior for `Engine::new_session`, `Engine::new_ref`,
     `EngineRef::new_session`, `EngineRef::get_table`, and
     `Session::begin_trx`.

2. Keep weak reachability code out unless it is immediately used.
   - Do not add a public or crate-private weak engine handle wrapper merely
     because later phases will need one.
   - If a tiny crate-private `Weak<EngineInner>` helper is needed by an
     admission test or by the baseline tool, keep it private, non-cloneable
     unless cloning is required by the immediate call site, and expose no
     user-visible upgrade method.
   - Otherwise, record in comments/task notes that Phase 2 owns the concrete
     weak engine reachability representation.

3. Define cleanup-hint and operation-lease contracts without speculative
   production machinery.
   - Prefer documenting these contracts in `docs/rfcs/0019-weak-public-runtime-handles.md`
     only if implementation discovers the RFC text is not specific enough for
     Phase 2. Otherwise the task doc is sufficient until resolve sync.
   - Do not add a cleanup enum, queue, worker, or generic lease type unless a
     current code path in this task consumes it.
   - If a test-only model is useful, keep it under an inline `#[cfg(test)]`
     module and avoid exporting it to production code.
   - The documented lease contract must require stable registry-entry
     visibility while mutable cores are checked out and must forbid removing an
     entire registry entry across `.await` and reinserting it later.

4. Document the identity/generation policy as a later-phase gate.
   - Do not implement generation in Phase 1.
   - Add concise design comments or task acceptance text stating:
     - generation is required only when an ID/slot can be reused, when
       tombstones are needed to distinguish stale from missing, or when stale
       handles could resolve to the wrong live object;
     - a phase that omits generation must document the non-reuse or
       non-ambiguity invariant that makes stale misresolution impossible;
     - `SessionID`, `TrxID`, and `TableID` policy decisions belong to their
       respective handle-migration phases.

5. Add reusable performance baseline support.
   - Provide `doradb-storage/examples/weak_handle_baseline.rs` as a reusable
     Cargo example rather than widening the public API.
   - The example should exercise current supported public APIs only.
   - The example should write output under `target/`, for example
     `target/weak-handle-baseline/`, and should not create tracked runtime
     artifacts.
   - Measurements should be simple, repeatable wall-clock baselines using
     `std::time::Instant`; this task does not need Criterion or a new benchmark
     dependency.
   - Include enough setup to measure the affected operation boundaries:
     session begin, empty statement execution, table lookup, unique point
     lookup, insert, update, delete, and scan.
   - Output should make later comparisons easy, preferably with one row per
     operation including iteration count, elapsed time, and average operation
     cost.
   - If any path is too expensive or flaky for routine local runs, make the
     example accept smaller iteration counts and document the default.

6. Add focused tests.
   - Extend existing inline tests in `doradb-storage/src/engine.rs` where
     possible.
   - Keep test-only helpers inside inline `#[cfg(test)] mod tests`.
   - Cover shutdown rejection, shutdown-busy preservation, idempotent shutdown,
     and the race where admission competes with shutdown.
   - If a test-only operation-lease model is added, cover checked-out,
     returned, abandoned, and terminal state transitions without involving real
     transaction commit semantics.

7. Keep documentation synchronized.
   - Update `docs/rfcs/0019-weak-public-runtime-handles.md` only if the final
     implementation changes the Phase 1 contract, task path, or phase summary.
   - Do not update Phase 2 or later text unless Phase 1 implementation changes
     a later-phase prerequisite.
   - During `task resolve`, run the RFC sync workflow required by the task
     process.

## Implementation Notes


## Impacts

- `doradb-storage/src/engine.rs`
  - Primary production change location for admission factoring and shutdown
    admission tests.
- `doradb-storage/src/session.rs`
  - Existing `Session::begin_trx` should keep behavior while using the
    refactored admission helper if needed.
- `doradb-storage/src/trx/mod.rs`
  - Existing transaction entry points are impacted only as consumers of the
    current admission behavior; do not redesign transaction ownership here.
- `doradb-storage/src/trx/stmt.rs`
  - Statement methods remain the performance-sensitive operation boundary for
    future table-handle phases; this task should not change their public table
    method signatures.
- `doradb-storage/src/catalog/mod.rs`
  - Current `Arc<Table>` lookup remains unchanged. Later table-handle phases own
    replacing public strong table exposure.
- `doradb-storage/src/error.rs`
  - Add or alter lifecycle/error mapping only if an actual current caller or
    test needs it. Otherwise leave error enums unchanged.
- `doradb-storage/examples/weak_handle_baseline.rs`
  - Reusable baseline measurement support for RFC-0019 operation boundaries.
- `docs/rfcs/0019-weak-public-runtime-handles.md`
  - Resolve-time phase sync target.

## Test Cases

1. `Engine::new_session`, `Engine::new_ref`, `EngineRef::new_session`,
   `EngineRef::get_table`, and `Session::begin_trx` still reject after shutdown
   admission closes.
2. Engine shutdown remains idempotent and still returns the existing
   shutdown-busy lifecycle error while live strong public runtime references
   exist under the current API.
3. A shutdown/admission race cannot produce both a successful shutdown and a
   newly admitted owner-created runtime reference.
4. If an admitted-operation RAII token is introduced, tests show that it admits
   work only while the engine is running and releases admission before shutdown
   can complete.
5. If cleanup-hint code is introduced, tests show that duplicate hints are
   idempotent, drop paths do not panic, and correctness does not depend on hint
   delivery.
6. If a test-only operation-lease model is introduced, tests show stable-entry
   visibility while checked out and deterministic return or terminal-state
   publication on drop.
7. The baseline example can run against a temporary engine and report the
   required operation rows without writing tracked artifacts.
8. Routine validation passes:

```bash
cargo fmt
cargo clippy -p doradb-storage --all-targets -- -D warnings
cargo nextest run -p doradb-storage
```

9. Focused coverage should be checked for changed Rust production paths:

```bash
tools/coverage_focus.rs --path doradb-storage/src/engine.rs
```

## Open Questions

1. Phase 2 must choose the concrete public engine boundary: remove `EngineRef`,
   make it crate-private, or replace the public surface with a non-cloneable
   weak engine capability.
2. The session, transaction, and table phases must decide identity/generation
   policy for their own handles. Phase 1 intentionally does not prebuild this.
3. The transaction-handle phase remains blocked on a concrete terminal-state and
   cancellation-safety design before public transaction ownership is migrated.
   This broader work is tracked by
   `docs/backlogs/000113-transaction-cancellation-safety.md`.
