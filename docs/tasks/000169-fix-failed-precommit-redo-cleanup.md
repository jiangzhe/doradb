---
id: 000169
title: Fix Failed Precommit Redo Cleanup
status: proposal
created: 2026-06-07
github_issue: 685
---

# Task: Fix Failed Precommit Redo Cleanup

## Summary

Fix the redo group-commit failure path for session-bound transactions after
they have entered precommit but before redo durability succeeds. Failed redo
write or sync must rollback precommit-owned row and index effects before
dropping undo payloads or waking group-commit waiters, so the owning session no
longer observes an active transaction after the failed commit returns and row
version maps cannot retain references to freed row undo entries.

## Context

Issue Labels:
- type:task
- priority:medium
- codex

Source Backlogs:
- docs/backlogs/000117-order-failed-redo-group-commit-cleanup-before-waiter-completion.md
- docs/backlogs/000118-define-failed-precommit-abort-cleanup-and-rowversionmap-teardown-safety.md

Related context:
- docs/tasks/000168-weak-transaction-handle-and-abandoned-cleanup.md
- docs/rfcs/0019-weak-public-runtime-handles.md

Backlog 000117 captures a confirmed ordering bug: `SyncGroup::fail_waiters`
sets the group failed, completes the shared redo waiter, and only then drains
`trx_list` and calls `PrecommitTrx::abort`. For a session-bound commit failure,
the caller can observe the returned fatal commit error before
`TrxAttachment::rollback()` clears the session transaction state.

Backlog 000118 captures the related ownership-safety gap. `PrecommitTrx::abort`
currently takes and drops `PrecommitTrxPayload`, including `row_undo`,
`index_gc`, and `gc_row_pages`, without a rollback-equivalent row/index cleanup
policy. Row-version heads store `RowUndoRef` pointers into transaction-owned
`OwnedRowUndo`; normal committed transactions keep row undo alive until purge
can detach row-version chains. A failed precommit path must not drop row undo
while `RowVersionMap` heads can still point at it.

There is also a second precommit semantic issue: `PreparedTrx::fill_cts`
currently converts `IndexUndoLogs` into `IndexPurgeEntry` as soon as a CTS is
assigned, based on the comment that rollback will no longer happen. Redo write
or sync failure after CTS assignment disproves that assumption. Failed
precommit still needs rollback-capable index undo; conversion to index GC must
be delayed until redo durability succeeds and `PrecommitTrx::commit` is running.

The short verification run:

```bash
cargo nextest run -p doradb-storage table::tests::test_drop_table_commit_poison_preserves_source_error --stress-count 20
```

passed 20/20 iterations in this checkout. The task remains valid because code
inspection confirms the ordering and payload-lifetime defects.

## Goals

1. Ensure failed redo group-commit paths abort queued session-bound precommit
   transactions before completing the shared group waiter.
2. Keep rollback-capable user precommit payload until redo durability succeeds.
   In particular, retain `IndexUndoLogs` in `PrecommitTrx` and convert to
   `IndexPurgeEntry` only on successful commit.
3. Implement failed-precommit abort as rollback-equivalent cleanup for user
   transactions:
   - rollback index undo using the transaction STS;
   - rollback row undo using the transaction STS;
   - clear transaction effects and release row-version heads before row undo is
     dropped;
   - update GC rollback bookkeeping;
   - release transaction-owned logical locks;
   - finish the session as rolled back;
   - release prepare waiters only after row/index cleanup no longer depends on
     payload ownership.
4. Preserve redo poison diagnostics. Failed redo write, submit, fsync, and
   fdatasync should still poison storage with the redo failure source.
5. Preserve the existing successful commit behavior and committed transaction
   purge handoff.
6. Add focused regression coverage for the redo failure ordering and failed
   precommit payload cleanup policy.

## Non-Goals

1. Do not change the redo log format, log record contents, or recovery replay
   format.
2. Do not redesign group commit batching, queueing, or no-wait system
   transaction semantics.
3. Do not redesign MVCC visibility, row undo chain layout, transaction purge,
   checkpoint, or recovery.
4. Do not change public `Session`, `Transaction`, or table APIs.
5. Do not implement a broader terminal transaction abstraction unless a narrow
   helper is required to share the existing rollback-equivalent cleanup safely.
6. Do not add degraded read-only behavior after storage poison.

## Unsafe Considerations (If Applicable)

This task should not add new `unsafe` code.

The task does affect the safety contract around existing raw row undo
references. `RowUndoRef` points into `OwnedRowUndo`, and its `Send`/`Sync`
invariants rely on reachable version-chain references outliving the pointed
undo entries. The implementation must make the failed-precommit path satisfy
the same ownership rule as normal rollback or purge: row-version heads are
rolled back or detached before transaction-owned `RowUndoLogs` are dropped.

If implementation changes any existing unsafe block or `// SAFETY:` contract,
apply `docs/process/unsafe-review-checklist.md` and refresh the unsafe
inventory according to the repository process.

## Plan

1. Rework precommit payload ownership in `doradb-storage/src/trx/mod.rs`.
   - Add the transaction `log_no` to `PrecommitTrxPayload` if failed-precommit
     abort needs it for GC rollback bookkeeping.
   - Store `IndexUndoLogs` in `PrecommitTrxPayload` instead of
     `Vec<IndexPurgeEntry>`.
   - Update `PreparedTrx::fill_cts` to move `index_undo` into
     `PrecommitTrxPayload` without calling `commit_for_gc()`.
   - Update `PrecommitTrx::commit` to call `commit_for_gc()` only after redo
     has been persisted and the transaction is committing successfully.
   - Keep `CommittedTrxPayload` and purge handoff as `index_gc`, because the
     committed path should still hand purge only deferred index cleanup.

2. Add failed-precommit rollback cleanup.
   - Introduce a narrow helper on `TransactionSystem` or `PrecommitTrx` that
     can rollback a precommit payload using `TableCache`, retained
     `PoolGuards`, `sts`, `log_no`, and `gc_no`.
   - Prefer reusing the same cleanup order as `rollback_inner`: index undo
     rollback first, row undo rollback second, rollback GC accounting, lock
     release, and session rollback.
   - Because redo failure is already fatal storage poison, preserve the first
     redo poison source when a cleanup access failure also attempts to poison
     with `FatalError::RollbackAccess`.
   - Keep session completion and lock release deterministic even when cleanup
     fails. If rollback access fails, use the existing fatal rollback policy:
     clear remaining effects as safely as possible, release locks, finish the
     session as rolled back or failed according to existing transaction-system
     conventions, and leave storage poisoned.

3. Fix redo failure waiter ordering in `doradb-storage/src/trx/log.rs`.
   - Change `SyncGroup::fail_waiters` so it drains and aborts `trx_list` before
     `completion.complete(Err(...))`.
   - Ensure all callers that fail pending, queued, written, or inflight sync
     groups use the same ordering for redo submit, write, fsync, and fdatasync
     failure.
   - Avoid holding the group-commit queue mutex across rollback cleanup. Drain
     queued groups into local ownership first, then run abort cleanup.

4. Preserve system transaction behavior.
   - Attachmentless system transactions should remain directly droppable after
     failed ordered completion, with no session rollback path.
   - No-wait system commits must not gain user-session rollback semantics.

5. Document the invariant.
   - Update comments near `PreparedTrx::fill_cts`, `PrecommitTrxPayload`, and
     `PrecommitTrx::abort` to state that CTS assignment is not a durability
     proof and failed precommit must retain rollback-capable payload until
     successful commit.
   - If useful, add a short note to `docs/transaction-system.md` describing the
     failed-precommit cleanup boundary.

6. Update existing tests that assumed index undo is converted at CTS assignment.
   - Any unit tests under `trx::mod` or related modules that inspect
     `PrecommitTrxPayload` should now expect rollback-capable index undo until
     commit success.

## Implementation Notes


## Impacts

- `doradb-storage/src/trx/mod.rs`
  - `PreparedTrxPayload`
  - `PreparedTrx::fill_cts`
  - `PrecommitTrxPayload`
  - `PrecommitTrx::commit`
  - `PrecommitTrx::abort`
  - `CommittedTrxPayload`
- `doradb-storage/src/trx/log.rs`
  - `SyncGroup::fail_waiters`
  - failure handling for redo submit, write completion poison, fsync, and
    fdatasync
- `doradb-storage/src/trx/sys.rs`
  - rollback-equivalent cleanup helper reuse or new failed-precommit cleanup
    helper
  - storage poison preservation when cleanup fails after redo poison
- `doradb-storage/src/trx/undo/index.rs`
  - no semantic change intended, but tests and call sites must reflect delayed
    `commit_for_gc()`
- `doradb-storage/src/trx/undo/row.rs`
  - no structural change intended, but the failed-precommit path must honor the
    `RowUndoRef` lifetime contract
- `doradb-storage/src/table/tests.rs`
  - drop-table redo poison regression coverage
- `docs/transaction-system.md`
  - optional failed-precommit cleanup note if code comments are not sufficient

## Test Cases

1. Redo write failure for a session-bound transaction returns the redo fatal
   error only after the session is no longer in a transaction.
   - Strengthen or extend
     `table::tests::test_drop_table_commit_poison_preserves_source_error`.
   - Run with stress count high enough to exercise waiter ordering.

2. Redo `fsync` and `fdatasync` failure paths have the same waiter ordering as
   redo write failure.
   - Existing `trx::log` sync-failure tests use system transactions; add or
     extend coverage for session-bound/user precommit payload when practical.

3. Failed precommit rollback preserves row undo ownership until row-version
   heads are detached.
   - Use a transaction that creates hot-row undo, inject redo durability
     failure, and assert teardown does not trip dangling row-version state.
   - Prefer a targeted internal test if poisoned runtime prevents public table
     reads after failure.

4. Failed precommit rollback preserves index undo until rollback.
   - Use a transaction with secondary-index insert/update/delete undo, inject
     redo durability failure, and verify rollback cleanup uses `IndexUndoLogs`
     rather than committed index GC.
   - Add a unit test around `PreparedTrx::fill_cts` and `PrecommitTrx::commit`
     if public table-level assertions are blocked by storage poison.

5. Successful commit still converts index undo to index GC and still hands
   committed row undo, index GC, and row-page GC payload to transaction purge.

6. Failure-source preservation:
   - Redo write failure records `FatalError::RedoWrite`.
   - Redo sync failure records `FatalError::RedoSync`.
   - If rollback cleanup also fails, the original redo poison remains visible
     as the canonical storage poison source unless existing poison-state
     semantics explicitly record first and later sources separately.

7. Validation commands:
   - `cargo nextest run -p doradb-storage`
   - targeted stress for
     `table::tests::test_drop_table_commit_poison_preserves_source_error`
   - focused tests for new failed-precommit cleanup cases
   - if backend-neutral IO behavior is touched, also run:
     `cargo nextest run -p doradb-storage --no-default-features --features libaio`

## Open Questions

1. If failed-precommit rollback access fails after redo has already poisoned
   storage, should the implementation only preserve the first redo poison
   source, or should it also retain secondary rollback-cleanup diagnostics in a
   structured way? The task should preserve the current first-poison behavior
   unless implementation finds an existing diagnostics channel for secondary
   fatal cleanup context.

2. Public reads after redo poison may be rejected by runtime admission, so some
   row/index rollback assertions may need to be internal tests that inspect the
   cleanup path directly rather than public session queries after the engine is
   poisoned.
