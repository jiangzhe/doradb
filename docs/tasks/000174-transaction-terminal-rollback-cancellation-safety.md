---
id: 000174
title: Transaction Terminal Rollback Cancellation Safety
status: implemented
created: 2026-06-12
github_issue: 697
---

# Task: Transaction Terminal Rollback Cancellation Safety

## Summary

Make explicit transaction rollback cancellation-safe after terminal ownership is
claimed. Today `Transaction::rollback(self).await` consumes the public handle and
publishes `RollingBack`, but the async rollback future still owns the
`TrxCompletionClaim`, `TrxInner`, undo buffers, transaction locks, and session
cleanup obligation while awaiting row/index rollback. Dropping that future at an
await point can drop rollback-capable state instead of completing cleanup.

Route terminal rollback through the existing transaction cleanup worker before
rollback awaits storage work. The user future becomes an observer waiting on a
completion cell; dropping the waiter must not cancel rollback cleanup.

## Context

Issue Labels:
- type:task
- priority:medium
- codex

Source Backlogs:
- docs/backlogs/000113-transaction-cancellation-safety.md

Related RFC:
- docs/rfcs/0019-weak-public-runtime-handles.md

RFC-0019 Phases 4 through 6 already implemented the weak transaction handle,
stable session-owned transaction entries, `TrxCheckout`, terminal completion
claims, commit handoff ownership, abandoned transaction cleanup, and shutdown
observation. The remaining cancellation gap is narrower than the original
backlog wording: rollback-equivalent terminal work can still be owned by a
cancellable user future after terminal state has been claimed.

The root invariant is that row undo is transaction-owned memory that row/page
MVCC structures can reference through undo links. Row undo must be rolled back,
committed and handed to GC, or retained after fatal rollback failure; it must not
be silently dropped by async future cancellation. The current `StmtEffects` drop
guard already treats polled statement execution as non-cancellable. This task
keeps that boundary and focuses on explicit terminal rollback.

This task partially addresses backlog `000113`. During `task resolve`, if full
statement execution cancellation remains out of scope, create or link a follow-up
backlog item for the broader statement cancellation model before closing or
replacing the source backlog.

## Goals

1. Make explicit `Transaction::rollback(self).await` cancellation-safe after
   terminal ownership is claimed.
2. Ensure dropping a polled rollback future cannot drop rollback-capable undo,
   leave transaction locks held forever, or leave the owning session permanently
   active.
3. Reuse the existing transaction cleanup worker and completion-cell pattern for
   async rollback work that cannot run from `Drop`.
4. Preserve the public transaction API shape.
5. Preserve existing commit handoff semantics: after a user transaction reaches
   queued precommit, commit or failed-precommit cleanup owns the terminal
   outcome.
6. Preserve abandoned transaction cleanup semantics: abandoned cleanup claims
   only `Abandoned` entries and must not override `RollingBack`, `Committing`,
   `CleanupRunning`, `Terminal`, or `Failed` entries.
7. Cover post-claim, pre-handoff commit failure paths that rollback the claimed
   transaction before it reaches commit handoff.
8. Update lifecycle documentation so the terminal rollback cancellation contract
   is explicit.

## Non-Goals

1. Do not make `Transaction::exec(...)` fully async-cancellation-safe.
2. Do not remove the `StmtEffects` fail-fast guard or attempt async statement
   rollback from `Drop`.
3. Do not redesign MVCC, redo, group commit, rollback algorithms, logical lock
   semantics, checkpoint, recovery, table files, or catalog/table DDL.
4. Do not change public transaction method signatures.
5. Do not make public `Transaction::drop` await, block, panic, or report cleanup
   errors directly to user code.
6. Do not add a second cleanup worker unless implementation proves the existing
   transaction cleanup worker cannot safely own this handoff.
7. Do not resolve the full backlog `000113` statement-cancellation model in this
   task.

## Unsafe Considerations

This task should not add, remove, or materially change unsafe code. The intended
implementation is ordinary Rust ownership, queue handoff, completion signaling,
and lifecycle-state testing.

If implementation unexpectedly changes unsafe blocks or unsafe impls, apply the
unsafe review checklist before resolve:

1. Describe affected modules/paths and why unsafe is required or reducible.
2. Update every affected `// SAFETY:` comment with the concrete invariant.
3. Refresh the unsafe inventory if required by the checklist.
4. Add targeted tests for the changed unsafe boundary.

Reference:
- docs/unsafe-usage-principles.md
- docs/process/unsafe-review-checklist.md

## Plan

1. Add a mandatory terminal rollback cleanup job.
   - Extend `TrxCleanupMessage` with a terminal rollback variant, for example
     `TerminalRollback(TerminalRollbackCleanupJob)`.
   - The job must own the already-claimed `TrxCompletionClaim`, an
     `Arc<Completion<()>>`, and operation context for diagnostics.
   - The job must not re-resolve the transaction through the session registry;
     it already owns the claimed `RollingBack` entry, mutable core, and
     operation-local `TrxAttachment`.
   - Add a small crate-private accessor or helper if the job needs to reach the
     engine or transaction system from `TrxCompletionClaim` without consuming it
     prematurely.

2. Add an enqueue helper on `TransactionSystem`.
   - Add a helper such as `enqueue_terminal_rollback(claim, operation)` that
     creates a completion, sends the terminal rollback job to the cleanup queue,
     and returns the completion to the waiter.
   - Send failure is an invariant violation because the message owns
     rollback-capable state. Follow the failed-precommit pattern: do not drop,
     discard, or synchronously run the returned job from the sender; leak/forget
     the returned message payload before panicking at the broken worker-lifetime
     boundary.
   - The cleanup worker must complete the completion with `Ok(())` on successful
     rollback and with a propagated completion error on rollback failure.

3. Route explicit rollback through the worker-owned path.
   - Keep `Transaction::rollback(self).await` consuming the public handle,
     setting `terminal_started = true`, resolving terminal reachability, and
     claiming `TrxEntryState::RollingBack`.
   - Replace direct inline `rollback_transaction(claim).await` ownership with
     queue handoff plus `Completion::wait_result().await`.
   - If the user future is dropped after the handoff, the worker still owns and
     completes rollback cleanup.
   - Preserve the visible result for callers who await rollback to completion.

4. Audit post-claim commit rollback paths.
   - In `TransactionSystem::commit_transaction`, any path that has already
     consumed a `TrxCompletionClaim` but must roll back before commit handoff
     should route rollback through the same worker-owned helper before awaiting.
   - The current runtime-health failure path is the known case.
   - Keep the irreversible queued-precommit path unchanged: `PrecommitTrx` and
     failed-precommit cleanup already own completion after commit handoff.

5. Preserve abandoned cleanup behavior.
   - `run_trx_cleanup_job` should continue to claim only `Abandoned` entries.
   - Duplicate abandoned cleanup messages must remain harmless when the entry is
     `RollingBack`, `CleanupRunning`, `Committing`, `Terminal`, or `Failed`.
   - Explicit rollback handoff must not require marking the transaction
     abandoned.

6. Preserve fatal rollback retention.
   - Worker-owned terminal rollback must keep the existing failure behavior:
     publish `Failed`, move remaining rollback-owned undo/effects into
     `FatalRollbackRetention`, poison storage with `FatalError::RollbackAccess`,
     finish session rollback where applicable, and wake the waiting completion
     with the rollback failure.
   - Do not drop row undo, index undo, or GC row-page payloads before rollback,
     commit GC handoff, or fatal retention has made their ownership explicit.

7. Add focused test-only synchronization hooks if needed.
   - Prefer inline `#[cfg(test)]` hooks in `trx` modules.
   - The hooks should pause the cleanup worker after terminal rollback is
     enqueued or after rollback begins, so tests can drop the user rollback
     future and observe that cleanup still completes.
   - Keep hooks narrow and avoid production indirection used only by tests.

8. Update documentation.
   - Update `docs/transaction-system.md` to state that explicit rollback hands
     claimed rollback work to transaction-system cleanup ownership before
     awaiting rollback completion.
   - Preserve the current documented statement boundary: a polled
     `Transaction::exec(...)` future must still be awaited to completion, and
     non-empty statement effects fail fast on drop.

## Implementation Notes

Implemented terminal rollback cancellation safety by handing claimed terminal
rollback work to the transaction cleanup worker before rollback awaits storage
work. `TrxCleanupMessage` now includes a mandatory terminal rollback job that
owns the `TrxCompletionClaim`, operation label, and completion cell. Explicit
`Transaction::rollback(self).await` and the post-claim poisoned commit rollback
path enqueue this job and wait only as observers on the completion.

The cleanup worker runs the existing rollback ownership path, preserving fatal
rollback retention, storage poison, transaction lock release, session rollback,
and terminal entry publication. Send failure on terminal rollback enqueue is
treated as a worker-lifetime invariant violation and leaks the returned message
payload before panicking, matching the failed-precommit cleanup safety pattern.

Added focused cancellation tests for dropped explicit rollback waiters, shutdown
while a dropped rollback waiter has worker-owned cleanup in progress, duplicate
abandoned-cleanup attempts against `RollingBack`, and cancellation of the known
post-claim pre-handoff commit rollback path. Existing rollback-access fatal
retention coverage verifies worker-owned terminal rollback still reports
`FatalError::RollbackAccess`, retains rollback-owned undo, poisons storage, and
finishes the session.

Updated `docs/transaction-system.md` to document that explicit rollback becomes
worker-owned after terminal ownership is claimed, while polled
`Transaction::exec(...)` remains non-cancellation-safe and must be driven to
completion.

Resolved source backlog `docs/backlogs/000113-transaction-cancellation-safety.md`
as replaced: this task implements the terminal rollback portion, remaining
statement execution cancellation safety is tracked by
`docs/backlogs/000124-statement-execution-cancellation-safety.md`, and adaptive
cleanup/runtime worker scheduling is tracked by
`docs/backlogs/000123-adaptive-background-worker-runtime.md`.

Validation completed:
- `cargo fmt`
- `cargo clippy -p doradb-storage --all-targets -- -D warnings`
- focused rollback cancellation tests:
  `cargo nextest run -p doradb-storage
  trx::tests::test_dropped_terminal_rollback_waiter_completes_worker_cleanup
  trx::tests::test_terminal_rollback_blocks_shutdown_after_waiter_drop
  trx::tests::test_duplicate_abandoned_cleanup_cannot_claim_terminal_rollback
  trx::tests::test_dropped_commit_waiter_after_pre_handoff_rollback_still_cleans_up`
- rollback fatal retention regression:
  `cargo nextest run -p doradb-storage
  test_mvcc_rollback_poisons_runtime_on_row_page_reload_error`
- related rollback/precommit regression tests:
  `cargo nextest run -p doradb-storage
  trx::tests::test_failed_precommit_cleanup_stops_reverse_after_rollback_failure
  trx::tests::test_transaction_locks_release_on_readonly_commit_rollback_and_ordered_commit
  trx::tests::test_transaction_locks_release_on_precommit_abort`
- `cargo nextest run -p doradb-storage`
- `tools/coverage_focus.rs --path doradb-storage/src/trx` reported 94.72%
  focused line coverage for the transaction directory.

## Impacts

- `doradb-storage/src/trx/sys.rs`
  - `TrxCleanupMessage`
  - transaction cleanup worker dispatch
  - rollback enqueue helper
  - `rollback_transaction`
  - `commit_transaction` post-claim rollback path
  - completion error mapping
- `doradb-storage/src/trx/mod.rs`
  - `Transaction::rollback`
  - `TrxCompletionClaim`
  - `TrxEntryState::RollingBack` documentation/tests
  - fatal rollback retention tests if new coverage is added there
- `doradb-storage/src/trx/log.rs`
  - no intended behavior change; existing commit handoff tests should continue
    to validate that dropped commit waiters do not cancel queued precommit.
- `doradb-storage/src/session.rs`
  - session finish/cleanup observations may need test assertions but should not
    need semantic changes.
- `doradb-storage/src/engine.rs`
  - shutdown tests may need assertions that queued terminal rollback work drains
    before component teardown.
- `docs/transaction-system.md`
  - terminal rollback cancellation contract and remaining statement
    non-cancellation boundary.

## Test Cases

1. Dropped explicit rollback waiter:
   - Start a transaction with rollback-owned row/index effects and a
     transaction-owned lock.
   - Start `rollback()` and poll until terminal rollback is handed to cleanup
     worker ownership.
   - Drop the rollback future.
   - Assert cleanup still releases locks, rolls back effects, marks the session
     out of transaction, and publishes terminal state.

2. Shutdown with dropped rollback waiter:
   - Drop a rollback waiter after terminal rollback handoff.
   - Call blocking `shutdown()`.
   - Assert shutdown waits for cleanup completion and then succeeds.

3. Duplicate cleanup race:
   - Queue or trigger abandoned cleanup while explicit rollback is already
     `RollingBack`.
   - Assert abandoned cleanup does not claim, double rollback, or double-complete
     the session.

4. Fatal rollback failure:
   - Inject or reuse an existing rollback-access failure path.
   - Assert worker-owned terminal rollback poisons storage, records fatal
     retention, completes the waiter with an error when awaited, and does not
     drop rollback-owned undo.

5. Commit pre-handoff rollback path:
   - Force the known post-claim, pre-handoff commit rollback path.
   - Assert cancellation of the waiting commit future after rollback handoff does
     not drop the claimed transaction state and that cleanup reaches a terminal
     or failed state.

6. Regression coverage:
   - Existing dropped-commit-after-handoff test still passes.
   - Existing abandoned transaction cleanup tests still pass.
   - Existing statement non-empty effect guard tests still pass.

Validation commands:
- `cargo nextest run -p doradb-storage`
- `tools/coverage_focus.rs --path doradb-storage/src/trx`

## Open Questions

- Full `Transaction::exec(...)` cancellation safety remains intentionally out of
  scope. It is tracked as
  `docs/backlogs/000124-statement-execution-cancellation-safety.md` because it
  likely requires a broader statement operation ownership model.
- Adaptive processing for long rollback or cleanup work remains intentionally
  out of scope. It is tracked as
  `docs/backlogs/000123-adaptive-background-worker-runtime.md` because several
  large worker-owned rollbacks can monopolize the single cleanup worker even
  though rollback is expected to be rare enough for this task's safety fix.
