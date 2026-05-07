---
id: 000143
title: Runtime Lifecycle And Checkpoint Gate
status: implemented
created: 2026-05-06
github_issue: 619
---

# Task: Runtime Lifecycle And Checkpoint Gate

## Summary

Implement RFC-0017 phase 2 by adding a volatile user-table lifecycle gate and a
checkpoint publish/drop gate. Foreground statement paths must reject stale table
handles after their normal logical locks are acquired, and table checkpoint must
publish a table-file root and `DataCheckpoint` redo only while the table remains
`Live`.

The task prepares the runtime safety boundary needed by a later
`Session::drop_table(table_id)` implementation without adding the public drop
API, catalog cascade deletes, catalog checkpoint rule changes, recovery changes,
or physical table-file deletion.

## Context

RFC-0017 defines logical `DROP TABLE` as a multi-phase storage change. Phase 1
has already completed catalog delete helpers and recovery classification for
checkpoint-absent user-table redo. Phase 2 now needs the volatile runtime
boundary that prevents stale `Arc<Table>` handles and background checkpoints
from continuing past the logical drop barrier.

The storage engine already uses logical locks for foreground table access.
`Statement` read paths acquire `TableMetadata(S)`, and write paths acquire
`TableMetadata(S)` plus `TableData(IX)`. Future drop DDL will acquire stronger
locks before removing catalog/runtime identity, but logical locks alone do not
invalidate an `Arc<Table>` that was resolved before catalog removal.

Checkpoint and recovery intentionally do not acquire logical locks. Current
table checkpoint can perform collection/build work and later publish a
table-file root before committing the checkpoint transaction that emits
`DDLRedo::DataCheckpoint`. RFC-0017 therefore requires a runtime gate at the
checkpoint publication boundary: a checkpoint may prepare work while the table is
still live, but it must not publish a root or commit `DataCheckpoint` after drop
has made `Dropping` visible.

The chosen state model is intentionally terminal in normal execution:

```text
Live -> Dropping -> Dropped
```

There is no normal `Dropping -> Live` recovery path. Fallible preparation for a
future drop must happen before the table is marked `Dropping`. Once `Dropping`
is visible, any later failure that prevents a durable drop outcome must be
treated as fatal/poisoned rather than silently reopening the table.

Issue Labels:
- type:task
- priority:medium
- codex

Parent RFC:
- docs/rfcs/0017-drop-table-lifecycle-recovery.md

## Goals

- Add volatile lifecycle state to user-table runtime handles:
  `Live -> Dropping -> Dropped`.
- Add foreground lifecycle checks after normal logical lock acquisition and
  before user-table row, index, deletion-buffer, or table-file access.
- Return a distinct terminal operation error for `Dropping` foreground access
  and `TableNotFound` for `Dropped` foreground access.
- Add a checkpoint publish lease that is acquired immediately before table-root
  publication and held through checkpoint transaction commit.
- Add a drop-side runtime gate that waits for any active checkpoint publish
  lease, marks the table `Dropping`, and prevents future checkpoint publish
  leases.
- Ensure no table checkpoint can publish a table-file root or commit
  `DDLRedo::DataCheckpoint` after `Dropping` is visible.
- Ensure a checkpoint that reaches the publish boundary after `Dropping` returns
  a normal non-published cancellation/failure outcome rather than publishing.
- Treat failures inside the no-cancel publish section as fatal/poisoning when
  root publication could otherwise be ambiguous.
- Add focused tests for foreground stale-handle rejection, checkpoint
  publish/drop exclusion, and the irreversible `Dropping` transition contract.

## Non-Goals

- Do not add public `Session::drop_table`.
- Do not implement catalog cascade deletes for `DROP TABLE`.
- Do not emit `DDLRedo::DropTable`.
- Do not change catalog checkpoint's `DropTable` scan rule in this phase.
- Do not add recovery behavior beyond preserving the phase 1 classifier.
- Do not cancel or drain all in-flight checkpoint preparation work before
  marking a table `Dropping`.
- Do not make background checkpoint acquire logical locks.
- Do not add a durable lifecycle/tombstone table.
- Do not delete, unlink, quarantine, or reclaim table files.
- Do not implement `CREATE INDEX`, `DROP INDEX`, drop by name, or
  `DROP TABLE IF EXISTS`.

## Unsafe Considerations (If Applicable)

This task is not expected to add or modify `unsafe` code. Lifecycle state,
foreground checks, and checkpoint publish gating should stay in safe Rust using
atomics, small mutex-protected state, and event notification. If implementation
unexpectedly touches table-file root unsafe boundaries, document the affected
proof contract and apply `docs/process/unsafe-review-checklist.md` before task
resolution.

## Plan

1. Add a lifecycle module for user-table runtime state.
   - Introduce a crate-private `TableLifecycle` owned by `Table`.
   - Represent states as `Live`, `Dropping`, and `Dropped`.
   - Provide foreground check helpers that attach table id and operation context
     to errors.
   - Provide crate-private drop-side methods for the future DDL phase:
     begin the irreversible drop gate, mark dropped, and inspect state in tests.
   - Do not provide a normal API that restores `Dropping` to `Live`.

2. Add operation errors and checkpoint cancellation vocabulary.
   - Add `OperationError::TableDropping` for foreground access that observes
     `Dropping`.
   - Keep `OperationError::TableNotFound` for `Dropped` foreground access.
   - Extend checkpoint outcome vocabulary with a normal non-published result,
     such as `CheckpointOutcome::Cancelled { reason:
     CheckpointCancelReason::TableDropping }`, or an equivalent explicit
     outcome name if implementation finds a clearer local term.
   - Do not overload `CheckpointOutcome::Delayed`, which is currently tied to
     GC-horizon/root-liveness scheduling.

3. Integrate foreground lifecycle checks in `Statement`.
   - After `acquire_table_read_lock(table_id)` succeeds, check that the user
     table remains `Live` before table scan, unique lookup, or index scan.
   - After `acquire_table_write_locks(table_id)` succeeds, check that the user
     table remains `Live` before insert, update, or delete can mutate row/index
     state or add redo.
   - Keep catalog-table helpers on the existing catalog path; lifecycle applies
     to user-table `Table`, not `CatalogTable`.
   - Leave recovery, no-transaction catalog replay, and explicit test/internal
     helpers outside the foreground lifecycle requirement unless they call the
     foreground `Statement` APIs.

4. Add checkpoint publish/drop gating.
   - Allow checkpoint pre-publication work to run while state is `Live`.
   - Immediately before table-root publication, acquire a
     `CheckpointPublishLease`.
   - The publish lease acquisition must fail if the lifecycle state is already
     `Dropping` or `Dropped`.
   - The drop-side gate must wait for any active publish lease, then mark
     `Live -> Dropping`; once `Dropping` is visible, future publish lease
     acquisition fails.
   - The publish lease must be held through:
     - `MutableTableFile::commit(...)`;
     - runtime block-index root update;
     - old-root retention in the checkpoint transaction;
     - checkpoint transaction commit that makes `DDLRedo::DataCheckpoint`
       durable.

5. Handle publish-section failures explicitly.
   - Existing IO publication failure already poisons storage with
     `FatalError::CheckpointWrite`; preserve that behavior.
   - Audit failures after successful root publication, especially block-index
     root update, old-root retention, and checkpoint transaction commit.
   - If a failure leaves the relationship between the published root and
     `DataCheckpoint` redo ambiguous, poison storage before returning. Reuse
     `FatalError::CheckpointWrite` unless a more precise existing fatal reason
     fits.
   - Do not let a future drop proceed normally after an ambiguous checkpoint
     publish/commit failure.

6. Keep checkpoint preparation cancellation out of scope.
   - Do not add a broad cancellable checkpoint lease over frozen-page
     collection, stabilization waits, LWC build, deletion checkpoint, or
     secondary sidecar work.
   - If a checkpoint prepared work before drop and then loses the publish/drop
     gate, it must exit without publishing a new table-file root or committing
     `DataCheckpoint`.
   - If implementation discovers irreversible runtime preparation side effects
     that cannot be safely abandoned before publication, stop and narrow the
     design around the smallest rollback/fatal handling needed for those side
     effects instead of adding broad cancellation by default.

7. Add tests near the changed runtime paths.
   - Prefer inline unit tests in `doradb-storage/src/table/tests.rs` and narrow
     `#[cfg(test)]` helpers exposed from `table::lifecycle` only when required
     for timing or fault injection.
   - Use production `Statement` APIs for foreground lifecycle tests.
   - Use test-only publish/drop synchronization hooks only when a real
     concurrent checkpoint/drop ordering cannot be expressed deterministically.
   - Run:

   ```bash
   cargo nextest run -p doradb-storage
   ```

## Implementation Notes

- Implemented a crate-private `TableLifecycle` owned by `Table` with terminal
  runtime states `Live -> Dropping -> Dropped`, foreground access validation,
  and drop-side lifecycle methods for the later public DDL phase.
- Added a single atomic checkpoint publish gate with states `Open`,
  `Publishing`, `ClosingPublishing`, and `Closed`. Checkpoint publication now
  acquires a `CheckpointPublishLease` immediately before table-root publication
  and holds it through runtime root update, old-root retention, and checkpoint
  transaction commit. Drop closes the gate, waits for the single active
  publisher when present, then marks the table `Dropping`.
- Added `OperationError::TableDropping`,
  `CheckpointCancelReason::{TableDropping, TableDropped}`, and
  `CheckpointOutcome::Cancelled { reason }` so foreground stale-handle access
  and checkpoint publish cancellation are explicit and distinct from
  GC-horizon delays.
- Integrated foreground lifecycle checks into `Statement` read/write user-table
  paths after logical lock acquisition and before table row/index access.
- Audited checkpoint publication failure handling. Root publication IO errors
  still poison storage with `FatalError::CheckpointWrite`; ambiguous failures
  after root publication, including old-root retention or checkpoint commit
  failure, now poison storage before returning.
- Added lifecycle and checkpoint gate tests covering foreground `Dropping` and
  `Dropped` rejection, publish/drop exclusion, checkpoint cancellation after
  drop, root publication write failure poisoning, and post-publication failure
  poisoning. The drop/publish tests exercise production futures and do not add
  test-only helper APIs to production impls.
- Verification completed:
  - `cargo fmt --check`: passed.
  - `cargo clippy -p doradb-storage --all-targets -- -D warnings`: passed.
  - `git diff --check`: passed.
  - `cargo nextest run -p doradb-storage lifecycle`: 5 passed.
  - `cargo nextest run -p doradb-storage table_drop_gate_waits`: 1 passed.
  - `cargo nextest run -p doradb-storage`: 701 passed.
  - `tools/coverage_focus.rs --path doradb-storage/src/table/lifecycle.rs --path doradb-storage/src/table/persistence.rs --path doradb-storage/src/trx/stmt.rs --path doradb-storage/src/error.rs --path doradb-storage/src/trx/recover.rs --path doradb-storage/src/table/mod.rs`:
    89.01% combined focused coverage. Changed runtime paths met the focused
    coverage target except `error.rs`, which is a central definition-heavy file
    and was covered through the lifecycle, persistence, statement, table, and
    recovery consumer paths.
- Task checklist found no remaining required fixes and no deferred actionable
  backlog items. PR #620 was opened with `Closes #619`.

## Impacts

- `doradb-storage/src/table/lifecycle.rs`
  - New lifecycle state machine, publish lease, drop gate, and focused unit
    tests.
- `doradb-storage/src/table/mod.rs`
  - Add lifecycle field to `Table`.
  - Expose crate-private lifecycle methods used by `Statement`, checkpoint, and
    future drop DDL.
- `doradb-storage/src/table/persistence.rs`
  - Add checkpoint publish lease acquisition immediately before publication.
  - Add non-published checkpoint outcome when publish is blocked by
    `Dropping`/`Dropped`.
  - Audit and poison ambiguous publish-section failures.
- `doradb-storage/src/trx/stmt.rs`
  - Add foreground user-table lifecycle checks after logical lock acquisition.
- `doradb-storage/src/error.rs`
  - Add `OperationError::TableDropping`.
- `doradb-storage/src/table/tests.rs`
  - Add integration-style lifecycle and checkpoint gate tests using real table
    and statement paths.
- `docs/rfcs/0017-drop-table-lifecycle-recovery.md`
  - Synced the phase 2 block with the implemented task outcome.

## Test Cases

- A foreground table scan through a stale `Arc<Table>` returns
  `OperationError::TableDropping` after the table is marked `Dropping`.
- A foreground insert/update/delete through a stale `Arc<Table>` returns
  `OperationError::TableDropping` after acquiring normal write locks and emits
  no row redo.
- A foreground read/write through a `Dropped` table returns
  `OperationError::TableNotFound`.
- Direct test/internal accessors can still be used only where explicitly
  intended; normal `Statement` APIs enforce lifecycle.
- A checkpoint that reaches the publish boundary while the table is `Live`
  obtains a publish lease and can publish normally.
- A drop-side gate waits for an already-held checkpoint publish lease before
  marking the table `Dropping`.
- A checkpoint that reaches the publish boundary after `Dropping` is visible
  returns a non-published cancellation/failure outcome and does not publish a
  root or commit `DataCheckpoint`.
- After `Dropping` is visible, lifecycle does not provide a normal path back to
  `Live`.
- A root publication IO failure still poisons storage with
  `FatalError::CheckpointWrite`.
- Injected failure after root publication but before checkpoint commit is
  handled as fatal/poisoned if the checkpoint outcome is ambiguous.
- Existing recovery tests for checkpoint-covered unknown-table redo from phase 1
  remain unchanged.

## Open Questions

- Resolved during implementation. The non-published checkpoint outcome is
  `CheckpointOutcome::Cancelled { reason }`, and ambiguous post-publication
  checkpoint failures reuse `FatalError::CheckpointWrite`.
- No deferred follow-up backlog items were identified during task checklist.
