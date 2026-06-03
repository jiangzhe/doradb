---
id: 000166
title: Transaction Terminal Ownership Preparation
status: implemented
created: 2026-06-02
github_issue: 679
---

# Task: Transaction Terminal Ownership Preparation

## Summary

Implement RFC-0019 Phase 4 by preparing transaction terminal ownership for the
future weak transaction handle while preserving the current public
`ActiveTrx`/`Transaction` ownership boundary.

This task makes the internal transaction lifecycle explicit for statement
execution, commit, rollback, and engine shutdown interaction. It must define the
minimum terminal-operation ownership contract needed before Phase 5 can move the
strong transaction core into session-owned stable entries and before Phase 6 can
add weak public transaction handles with abandoned rollback cleanup. It also
fixes the concrete session runtime pin gaps from backlog
`000116` so long-running async session operations cannot outlive engine-owned
runtime components they still need.

Issue Labels:
- type:task
- priority:medium
- codex

Parent RFC:
- docs/rfcs/0019-weak-public-runtime-handles.md

RFC Phase:
- Phase 4: Transaction Terminal Ownership Preparation

Related Backlogs:
- docs/backlogs/000113-transaction-cancellation-safety.md
- docs/backlogs/000116-general-session-runtime-pin-ownership.md

## Context

RFC-0019 now splits the former transaction-handle phase into three narrower
phases. Phase 4 is the preparation phase for explicit transaction terminal
ownership and runtime pin safety. Phase 5 owns session-owned stable transaction
entries and operation leases. Phase 6 owns the weak public `TransactionHandle`,
abandoned transaction handoff, and rollback cleanup executor or worker.

Phase 3 implemented weak public session handles and introduced a transitional
transaction attachment:

- public `Session` is a weak, non-cloneable capability;
- `SessionRegistry` strongly owns `SessionState`;
- active transactions still use `TrxSessionRef`;
- `TrxSessionRef` keeps a strong `EngineRef` runtime pin until transaction
  terminal completion;
- `TrxSessionRef` weakly references `SessionState` and calls back into
  `SessionRegistry` on commit or rollback.

That transitional design intentionally keeps active transactions as shutdown
blockers. Current engine tests already assert that `try_shutdown()` returns
`ShutdownBusy` while an active transaction is alive and that blocking
`shutdown()` waits for explicit transaction completion. Phase 4 must preserve
that behavior. Shutdown is not a transaction terminal owner in this phase.

The risky ownership boundary is commit. Current user commit flow consumes
`ActiveTrx`, prepares it, detaches session completion state around group commit,
and then waits for redo completion before marking the session committed or
rolled back. Once commit has crossed the group-commit handoff boundary,
cancelling or dropping the user future must not make rollback a competing
outcome and must not leave the owning session permanently active.

Backlog `000116` remains relevant because some production table/session
operations currently extract `PoolGuards` from `SessionPin` and release the pin
before awaiting. Those paths are shutdown-sensitive once future transaction
refactors stop relying on a long-lived strong transaction `EngineRef`.

Phase Contract:
- Prerequisites: RFC-0019 Phases 1 through 3 are in place. The Phase 3
  transitional transaction path may still keep a strong runtime pin while this
  phase defines and implements the safer terminal-operation and runtime-pin
  contract for Phase 5 stable transaction entries and Phase 6 weak transaction
  handles.
- Phase-local choices:
  - define the internal logical transaction states for explicit operations;
  - decide which logical terminal states are stored versus represented by Rust
    ownership types or immediate removal;
  - define the commit handoff boundary after which rollback is no longer a
    valid competing outcome;
  - decide how commit session completion is owned if the user commit future is
    cancelled after handoff;
  - decide how to document or enforce the current non-cancellable boundaries
    for statement execution and explicit rollback;
  - decide how `SessionPin` or a successor runtime pin is kept alive across the
    production long-running session paths from backlog `000116`.
- After this phase: explicit transaction terminal operations have concrete
  completion ownership, shutdown behavior, and runtime pin safety. The public
  transaction handle may still be the existing strong owner, and abandoned
  transaction rollback is still not automated.
- Following phases preserved: Phase 5 can move active transaction state into
  stable session-owned entries only after this task defines the
  terminal-operation contract, and Phase 6 can introduce weak public transaction
  handles only after there is no ownership gap at commit/rollback/shutdown
  boundaries.
- RFC phase-plan edits: RFC-0019 has already been updated to split the old
  transaction phase into Phase 4, Phase 5, and Phase 6.

## Goals

1. Preserve the public `ActiveTrx`/`Transaction` API shape for this phase.
2. Define and document the internal logical transaction states for explicit
   transaction operations:

   | Logical State | Meaning | Ownership Rule |
   |---|---|---|
   | `Active` | `ActiveTrx` owns the transaction core, session ref, locks, effects, and runtime pin. | No terminal owner yet; `exec`, `commit`, or `rollback` may be started. |
   | `StatementRunning` | `exec` has borrowed `&mut ActiveTrx` and constructed a statement facade. | Statement execution temporarily owns statement-local mutation, but not the transaction terminal outcome. |
   | `PreparingCommit` | `commit()` has consumed `ActiveTrx` and is preparing commit before irreversible handoff. | Commit owns the terminal attempt, but pre-handoff failure may still route through rollback/discard behavior. |
   | `Committing` | Commit crossed the irreversible public-lifecycle handoff, including queued or actively processed group commit. | Commit owns the terminal outcome; shutdown, drop, cancellation, or cleanup must not convert it to rollback. |
   | `RollbackRunning` | Explicit rollback has consumed `ActiveTrx` and is running undo/lock/session cleanup. | Rollback owns terminal cleanup until completion or fatal discard. |
   | `Committed` | Commit terminal cleanup finished and session/runtime ownership is released. | No further user terminal operation is valid. |
   | `RolledBack` | Rollback terminal cleanup finished and session/runtime ownership is released. | No further user terminal operation is valid. |
   | `Failed`/`Discarded` | Fatal rollback/access/terminal failure poisoned or discarded the transaction. | Prevent reuse and expose diagnostics; shutdown must not treat this as clean active state. |

   These are logical states. The implementation may represent them with
   existing Rust ownership types such as `ActiveTrx`, `PreparedTrx`,
   `PrecommitTrx`, commit-group entries, explicit internal enums, or immediate
   terminal cleanup, as long as the ownership rules and tests are satisfied.
3. Explicitly exclude `Abandoned` from Phase 4 transaction state. Abandonment is
   a Phase 6 state because it needs weak public transaction handles and a cleanup
   owner.
4. Preserve closure-based `ActiveTrx::exec(...)` and the current
   statement-future cancellation boundary: a polled statement execution future
   must be awaited to completion unless the implementation can prove an
   equivalent rollback-safe path.
5. Make commit terminal ownership explicit:
   - before the handoff boundary, failures may roll back or discard according
     to existing transaction rules;
   - after the handoff boundary, rollback is no longer a valid competing
     terminal outcome;
   - session completion, lock release, and runtime release must not depend only
     on the user commit future still being polled after the handoff.
6. Make rollback terminal ownership explicit:
   - explicit rollback owns undo, lock release, GC/session cleanup, and runtime
     release until completion;
   - if rollback cancellation cannot be made safe without the Phase 6 cleanup
     owner, preserve a clear fail-fast or non-cancellable contract rather than
     silently leaking active transaction state.
7. Preserve Phase 4 shutdown behavior:
   - `try_shutdown()` returns `ShutdownBusy` while active, statement-running,
     committing, or rolling-back transactions hold runtime liveness;
   - blocking `shutdown()` waits for those runtime pins to drain;
   - shutdown does not claim active transactions and does not auto-rollback.
8. Fix the production `SessionPin` early-release paths identified by backlog
   `000116`:
   - table freeze;
   - table checkpoint;
   - secondary mem-index cleanup.
9. Add focused regression tests for terminal-operation ownership, commit
   cancellation/handoff behavior, shutdown waiting/busy behavior, rollback
   cleanup, and the fixed long-running session pin paths.
10. Keep the task compatible with RFC-0019 Phase 5 and Phase 6 by documenting
    any remaining prerequisites for stable transaction entries and weak
    transaction handle migration.

## Non-Goals

1. Do not replace public `ActiveTrx` or `Transaction` with a weak
   `TransactionHandle`.
2. Do not move strong active transaction state into session-owned stable
   transaction entries.
3. Do not add abandoned transaction state, abandoned rollback handoff,
   cleanup queue, cleanup worker, async cleanup executor, or public-handle-drop
   rollback behavior.
4. Do not make shutdown a transaction cleanup owner in this phase.
5. Do not redesign MVCC, redo, group commit, rollback algorithms, logical lock
   semantics, table DDL, checkpoint/recovery, table-file roots, or buffer-pool
   internals.
6. Do not redesign full statement async cancellation semantics beyond
   preserving and documenting the existing fail-fast/non-cancellable boundary.
7. Do not expose internal operation leases, weak upgrade APIs, or runtime pins
   as public APIs.
8. Do not migrate public table handles. That remains RFC-0019 Phase 7.

## Unsafe Considerations (If Applicable)

No new unsafe code is planned.

This task should be implementable in safe Rust through ownership state,
terminal handoff, runtime pin lifetime, tests, and documentation changes. If
implementation unexpectedly adds or modifies unsafe code, apply
`docs/process/unsafe-review-checklist.md`, update adjacent `// SAFETY:`
comments, and refresh the unsafe inventory with:

```bash
tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md
```

Reference:
- docs/unsafe-usage-principles.md
- docs/process/unsafe-review-checklist.md

## Plan

1. Audit and document the current transaction ownership transitions.
   - Work primarily in `doradb-storage/src/trx/mod.rs`,
     `doradb-storage/src/trx/sys.rs`, `doradb-storage/src/trx/log.rs`,
     `doradb-storage/src/session.rs`, and `doradb-storage/src/engine.rs`.
   - Map existing ownership types to the logical states in this task.
   - Add concise comments or internal helper names where current transitions are
     not obvious, especially around `ActiveTrx::prepare`,
     `TransactionSystem::commit`, `LogPartition::commit`, and rollback fatal
     discard.
   - Do not add a large enum solely for documentation if Rust ownership types
     already enforce a state boundary.

2. Strengthen the statement-running boundary.
   - Preserve `ActiveTrx::exec` as the closure-scoped statement API.
   - Make it explicit that `StatementRunning` is not a terminal owner.
   - Ensure ordinary statement errors roll back statement-local effects and
     return the original error.
   - Ensure fatal statement rollback failure still discards the transaction and
     prevents later commit/rollback reuse.
   - Keep the existing guard/fail-fast behavior for dropped non-empty statement
     effects; do not attempt async rollback from `Drop`.

3. Define the commit handoff boundary.
   - Treat preparation before group-commit enqueue as `PreparingCommit`.
   - Treat both queued and actively processed group commit as one logical
     `Committing` state unless implementation needs a finer split for tests or
     diagnostics.
   - After entering `Committing`, rollback must no longer be possible through
     shutdown, cancellation, session drop, or future Phase 6 cleanup.
   - Update comments/tests so this boundary is visible to future Phase 5 and
     Phase 6 work.

4. Move post-handoff commit completion ownership out of the cancellable user
   future when needed.
   - Inspect `LogPartition::create_new_group`, `CommitGroup::join`,
     `LogPartition::commit`, and redo worker failure paths.
   - If a session-bound user transaction is enqueued, the transaction system or
     commit-group state must own enough completion data to finish session
     commit/rollback cleanup even if the caller drops the `commit().await`
     future after handoff.
   - The user future may still wait for and return `TrxID`, but it must not be
     the only owner capable of marking the session committed/rolled back after
     the commit handoff.
   - Preserve the existing sessionless system transaction `commit_no_wait`
     contract.

5. Preserve pre-handoff commit failure behavior.
   - If `ensure_runtime_healthy`, prepare, redo serialization, or enqueue fails
     before the commit handoff, route through the existing rollback/discard
     behavior where applicable.
   - Keep fatal storage poison semantics intact.
   - Do not create a new public partially-committed outcome.

6. Make rollback ownership explicit without adding a background cleanup owner.
   - Treat explicit rollback as `RollbackRunning` after it consumes
     `ActiveTrx`.
   - Ensure successful rollback clears effects, releases transaction locks,
     updates GC/session state, and releases runtime liveness.
   - Ensure fatal rollback failure still poisons/discards and prevents later
     transaction reuse.
   - If a rollback future is cancelled after polling and cannot be safely
     completed without Phase 6 cleanup infrastructure, preserve a fail-fast or
     documented non-cancellable boundary. Silent transaction/session leaks are
     not acceptable.

7. Preserve and test shutdown behavior.
   - Keep active transactions as runtime-liveness owners through the
     transitional `TrxSessionRef`.
   - `Engine::try_shutdown()` should remain busy while such runtime pins are
     live.
   - `Engine::shutdown()` should continue to wait for active transaction,
     committing, or rolling-back runtime pins to finish.
   - Shutdown must not claim, commit, roll back, or abandon active transactions
     in Phase 4.
   - Add or update tests in `doradb-storage/src/engine.rs` and transaction tests
     to cover this contract.

8. Fix the backlog `000116` session runtime pin gaps.
   - In `doradb-storage/src/table/persistence.rs`, keep a `SessionPin` or
     equivalent runtime pin alive across `freeze(...).await`.
   - In `TablePersistence::checkpoint`, remove the explicit early `drop(pin)`
     pattern or replace it with an owned runtime pin that lives across the
     long-running checkpoint work that needs engine-owned runtime components.
   - In `doradb-storage/src/table/gc.rs`, keep a session/runtime pin alive
     across the secondary mem-index cleanup loop while it needs transaction
     system and pool/runtime components.
   - If a broader helper such as `SessionRuntimePin` makes these call sites
     clearer, keep it crate-private and scoped to this task's production paths.

9. Add focused tests for the fixed pin paths.
   - Use existing test hooks where available, such as checkpoint hooks, or add
     narrow `#[cfg(test)]` hooks if needed.
   - Prove shutdown cannot complete while freeze/checkpoint/secondary cleanup
     is in the pinned long-running section.
   - Prove `try_shutdown()` reports busy while those pins are live.
   - Avoid test-only production APIs unless they are narrowly justified and kept
     under `#[cfg(test)]`.

10. Update documentation and comments.
    - Add rustdoc or adjacent comments for the internal terminal-state contract
      where implementation ownership is non-obvious.
    - Reference that `Abandoned` and automatic abandoned rollback cleanup remain
      Phase 6 work.
    - Update `docs/transaction-system.md` only if implementation changes the
      concept-level commit/rollback lifecycle enough that the existing document
      becomes misleading.

11. Validate.
    - Run formatting and diff checks:

      ```bash
      cargo fmt --check
      git diff --check
      ```

    - Run the supported storage validation pass:

      ```bash
      cargo nextest run -p doradb-storage
      ```

    - Run focused coverage on changed production Rust files or directories:

      ```bash
      tools/coverage_focus.rs --path doradb-storage/src/trx --path doradb-storage/src/session.rs --path doradb-storage/src/engine.rs --path doradb-storage/src/table/persistence.rs --path doradb-storage/src/table/gc.rs
      ```

    - Target at least 80% focused coverage for the changed scope.

12. Keep RFC/task synchronization in mind for resolve.
    - During `task resolve`, update RFC-0019 Phase 4 with the concrete task
      path, issue number if available, phase status, implementation summary,
      and any remaining Phase 5 or Phase 6 prerequisites.
    - Do not close backlog `000113` unless implementation fully resolves the
      broader cancellation-safety backlog. It is acceptable for this task to
      satisfy only the minimum transaction-handle migration prerequisite.
    - Do not close backlog `000116` unless implementation proves the general
      session runtime pin ownership issue is fully resolved, not just the three
      production early-drop paths.

## Implementation Notes

Implemented RFC-0019 Phase 4 transaction terminal ownership preparation.

- Mapped Phase 4 logical transaction states onto existing Rust ownership
  boundaries instead of introducing a large state enum: `ActiveTrx` represents
  active statement-capable ownership, `PreparedTrx` represents pre-handoff
  commit preparation, and queued `PrecommitTrx` represents irreversible
  committing ownership.
- Moved post-handoff commit completion ownership into queued precommit/group
  commit state. A dropped user commit future after enqueue can stop observing
  the result, but it no longer owns session commit/rollback cleanup and cannot
  convert the transaction back to rollback.
- Preserved rollback as an explicit consumed-`ActiveTrx` terminal path and kept
  abandonment/automatic rollback cleanup out of Phase 4.
- Kept `SessionPin`/runtime liveness alive across the three production
  long-running paths from backlog `000116`: table freeze scan, checkpoint
  publication, and secondary mem-index cleanup scan.
- Added regression coverage for dropped commit futures after commit handoff and
  for shutdown waiting while freeze, checkpoint, and secondary cleanup hold
  operation runtime pins. The shutdown-probe test helper now uses a deterministic
  started signal and RAII hook reset guards so thread-local hooks do not leak on
  panic.
- Updated `docs/transaction-system.md` with the commit handoff contract and
  updated RFC-0019 phase planning to split the future transaction work into
  Phase 5 stable entries/operation leases and Phase 6 weak transaction handles
  with abandoned cleanup.

Validation completed:

- `cargo fmt --check`
- `git diff --check`
- `cargo clippy -p doradb-storage --all-targets -- -D warnings`
- `cargo nextest run -p doradb-storage` - 880 tests passed, 0 skipped.
- `tools/coverage_focus.rs --path doradb-storage/src/trx --path doradb-storage/src/session.rs --path doradb-storage/src/engine.rs --path doradb-storage/src/table/persistence.rs --path doradb-storage/src/table/gc.rs`
  - Deduplicated total: 11795/12571 lines, 93.83%.
  - `doradb-storage/src/trx`: 9126/9677 lines, 94.31%.
  - `doradb-storage/src/session.rs`: 440/461 lines, 95.44%.
  - `doradb-storage/src/engine.rs`: 1115/1165 lines, 95.71%.
  - `doradb-storage/src/table/persistence.rs`: 724/859 lines, 84.28%.
  - `doradb-storage/src/table/gc.rs`: 390/409 lines, 95.35%.

Post-implementation checklist outcome:

- Reliability: pass. Required regression tests, full nextest, clippy, and
  focused coverage passed above the 80% target.
- Security: pass/not applicable. No unsafe code was added or modified.
- Performance: pass. Commit handoff changes retain existing group-commit
  batching, and runtime pin fixes extend existing operation pins without adding
  hot-loop registry lookup or new global synchronization.
- Feature completeness: pass. Phase 4 terminal ownership, shutdown behavior,
  and the three concrete runtime-pin gaps are implemented while weak transaction
  handles and abandoned cleanup remain out of scope.
- Documentation: pass. Transaction lifecycle documentation and adjacent
  ownership comments were updated where behavior would otherwise be ambiguous.
- Test-only code: pass. New hooks and shutdown-probe helpers remain inside
  table tests and exercise production freeze/checkpoint/cleanup paths.
- Complexity: pass. New helper types are narrow, and no broad transaction-state
  enum or public API migration was introduced.

Backlog resolution:

- `docs/backlogs/000113-transaction-cancellation-safety.md` remains open because
  broader statement/rollback async cancellation safety is not fully resolved by
  this Phase 4 preparation task.
- `docs/backlogs/000116-general-session-runtime-pin-ownership.md` remains open
  because this task fixed the three known production early-release paths but did
  not prove every possible future session runtime pin pattern is exhausted.

## Impacts

- `doradb-storage/src/trx/mod.rs`
  - `ActiveTrx`, `ActiveTrxState`, `ActiveTrx::exec`, `ActiveTrx::commit`,
    `ActiveTrx::rollback`, `ActiveTrx::prepare`, `PreparedTrx`, `PrecommitTrx`,
    `Drop` assertions, and discarded/fatal paths.
- `doradb-storage/src/trx/sys.rs`
  - `TransactionSystem::commit`, `TransactionSystem::rollback`,
    runtime-health prechecks, prepare/discard paths, GC/session completion, and
    transaction-system tests.
- `doradb-storage/src/trx/log.rs`
  - `LogPartition::commit`, `commit_no_wait`, `enqueue_commit`,
    `CommitGroup` construction/joining, redo worker success/failure handling,
    completion waiters, and group-commit tests.
- `doradb-storage/src/session.rs`
  - `TrxSessionRef`, `SessionPin`, session lifecycle completion callbacks, and
    runtime pin ownership comments/tests.
- `doradb-storage/src/engine.rs`
  - shutdown waiting/busy tests and any diagnostics around active transaction or
    runtime-pin liveness.
- `doradb-storage/src/table/persistence.rs`
  - `freeze`, `checkpoint_readiness`, and `checkpoint` runtime pin lifetimes.
- `doradb-storage/src/table/gc.rs`
  - `cleanup_secondary_mem_indexes` runtime pin lifetime.
- `docs/transaction-system.md`
  - update only if the implementation materially changes documented
    transaction lifecycle semantics.
- `doradb-storage/tests/public_api_smoke.rs` and examples
  - update only if public-facing behavior or validation setup needs adjustment;
    public transaction type migration is out of scope.

## Test Cases

1. Active transaction shutdown:
   - `try_shutdown()` returns `ShutdownBusy` while an active transaction holds a
     runtime pin.
   - `shutdown()` waits while an active transaction is live and completes after
     explicit commit or rollback.
2. Statement execution:
   - ordinary `exec` errors roll back only statement-local effects and return
     the original error;
   - fatal statement rollback failure discards the transaction and later
     commit/rollback return errors instead of panics;
   - dropping non-empty statement effects remains fail-fast.
3. Commit pre-handoff:
   - failures before the commit handoff use rollback/discard behavior and do
     not publish a committed transaction.
4. Commit handoff/cancellation:
   - after a session-bound transaction enters logical `Committing`, dropping the
     user commit future does not leave the owning session permanently active;
   - after logical `Committing`, no path converts the transaction outcome to
     user rollback;
   - redo/group-commit failure after handoff completes the documented rollback
     or failure session cleanup path.
5. Rollback ownership:
   - successful rollback clears effects, releases transaction locks, updates
     session lifecycle, and lets shutdown complete;
   - fatal rollback failure poisons/discards and prevents transaction reuse;
   - rollback cancellation behavior is covered by either a focused fail-fast
     test or documented as an intentionally non-cancellable boundary.
6. Session dropped before transaction remains covered:
   - dropping a session handle before explicit transaction terminal completion
     does not invalidate the current strong transaction owner;
   - terminal completion still removes or updates the registry state according
     to Phase 3 behavior.
7. Long-running session runtime pins:
   - freeze keeps runtime liveness across the async scan section;
   - checkpoint keeps runtime liveness across the long-running checkpoint
     section;
   - secondary mem-index cleanup keeps runtime liveness across its async cleanup
     loop;
   - shutdown waits or `try_shutdown()` reports busy while each pinned section
     is active.
8. Regression:
   - existing transaction, engine shutdown, session lifecycle, table
     persistence, table GC, and public smoke tests still pass under
     `cargo nextest run -p doradb-storage`.

## Open Questions

- Phase 6 must decide whether abandoned rollback cleanup runs on a dedicated
  cleanup worker, an existing transaction-system worker, or an
  engine/session-registry-owned async executor.
- Phase 6 must choose the weak transaction handle identity shape and whether
  transaction ids are sufficient without generation.
- Phase 6 must define true abandoned transaction states, such as abandoned,
  cleanup-claimed, cleanup-running, rolled-back, and failed-cleanup.
- Broader statement async cancellation safety remains related to backlog
  `000113` unless this task's implementation fully resolves it.
- Broader session runtime pin ownership remains related to backlog `000116`;
  this task fixed the concrete freeze, checkpoint, and secondary cleanup paths.
