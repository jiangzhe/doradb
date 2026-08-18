---
id: 000272
title: Route Row Undo Rollback Through Page Transition
status: implemented  # proposal | implemented | superseded
created: 2026-08-18
github_issue: 983
---

# Task: Route Row Undo Rollback Through Page Transition

## Summary

User-table row-undo rollback now treats a checkpoint-transitioning page, and
an exact-generation page miss while the row remains above the hot/cold pivot,
as unresolved routing rather than successful rollback. The current boxed undo
remains owned while rollback releases page and row guards, waits for
authoritative cold-route publication or engine poison, and retries from the
current pivot.

The implementation separates exact hot-page rollback classification from
semantic retry policy. Foreground mutation and rollback share one table-level,
lost-wakeup-safe route-or-poison helper. Stable hot, cold-origin, and
already-cold rollback retain their prior fast-path poison-observation costs.

## Context

Issue Labels:

- type:task
- priority:medium
- codex

Source Backlogs:

- `docs/backlogs/closed/000185-row-undo-rollback-through-page-transition.md`

Related implemented tasks:

- `docs/tasks/000219-optimize-frozen-page-checkpoint-transition-planning.md`
  excluded rollback mutation on transition pages.
- `docs/tasks/000264-engine-poison-foreground-waiters.md` established the
  route-epoch and poison-listener protocol reused here.
- `docs/tasks/000271-index-mutation-unique-driver-key-changes.md` made the
  rollback gap observable through delayed hot-to-cold mutation ownership.
- `docs/tasks/000174-transaction-terminal-rollback-cancellation-safety.md`
  established mandatory ownership for terminal rollback.

Checkpoint transition installs `RowPageState::Transition` and
`DeleteMarker` overlays before LWC construction and table-root publication.
Only the later `BlockIndexRoot::update_column_root` advances the pivot and
notifies route waiters. During that interval, the hot page is sealed but the
cold route is not yet authoritative.

Previously, row undo could mutate the sealed page or treat an absent recorded
page generation as completion while the pivot still classified the row as
hot. Either outcome could pop and free an undo before it was unlinked from the
authoritative row representation.

The existing ownership model already kept the newest `OwnedRowUndo`
vector-owned across awaits and transferred cancelled or mandatory cleanup to
statement, terminal, abandoned, failed-precommit, or fatal-retention owners.
This task preserved that model and extended its retry protocol through page
transition.

No RFC was required because the change affects no public API, durable format,
data model, recovery protocol, or rollout sequence.

## Goals

1. Never mutate a user row page while it is in `Transition`.
2. Treat an exact-generation page miss as unresolved while the current pivot
   still classifies the row as hot.
3. Keep the current boxed row undo owned until hot unlink or cold-marker
   removal succeeds.
4. Release page, state, and row guards before waiting for route progress or
   poison.
5. Retry from the authoritative pivot after a normal wake.
6. Preserve the first canonical Fatal report and unresolved ownership when
   poison prevents route publication.
7. Apply the same rollback behavior to statement, explicit terminal,
   abandoned, and failed-precommit cleanup.
8. Share the route-or-poison protocol with foreground row mutation.
9. Preserve stable hot and cold rollback fast-path costs.
10. Preserve catalog rollback and index-before-row rollback ordering.

## Non-Goals

1. Checkpoint planning, marker selection, root publication, page retirement,
   and durable block-index state were not changed.
2. No generalized route-token abstraction or unrelated lookup API migration
   was introduced.
3. Public transaction, statement, table, and checkpoint APIs were unchanged.
4. Row, undo, redo, LWC, table-file, catalog, and recovery formats were
   unchanged.
5. Rollback remains sequential and index undo still precedes row undo.
6. Statement error suppression and eager failure rollback remain outside this
   task; `docs/backlogs/000186-statement-failure-rollback-before-error-return.md`
   owns that work.
7. Clean shutdown does not cancel transition waits or mandatory cleanup.
8. Catalog page lifecycle and missing-page rollback behavior remain unchanged.
9. Pivot values are not cached across undo entries.
10. No rollback benchmark or performance threshold was added.

## Plan

### Hot-page attempt boundary

`MemTable::try_rollback_hot_row_undo` performs one exact hot-page attempt and
returns `RowUndoRollbackAttempt::{Applied, PageMissing, Transition}`.
`RowUndoRollbackAttempt` is colocated with row-undo ownership in
`trx/undo/row.rs`.

The attempt loads the recorded `VersionedPageID`, retains the page-state read
guard before row mutation, and refuses mutable access when the state is
`Transition`. Active and Frozen pages synchronously unlink the exact owned
undo. The attempt does not inspect poison, read the pivot, remove cold markers,
or decide whether a page miss is successful.

Catalog rollback accepts `Applied` and its historical missing-page no-op.
`Transition` remains an impossible catalog lifecycle state and is asserted as
an invariant violation.

### Authoritative rollback retry

`RowUndoLogs::rollback` owns user-table routing and accepts a borrowed
`RowUndoRollbackContext` containing only pool guards and the engine poisoner.
For each newest entry:

1. A cold-origin undo removes its transaction-owned deletion marker.
2. An entry already below the current pivot removes that marker.
3. An entry still classified hot performs the exact-page attempt.
4. `Applied` completes synchronously.
5. `Transition` or `PageMissing` releases all page-local ownership and
   waits for route publication or poison.
6. A normal wake retries from the pivot; a failure leaves the current and older
   entries owned.
7. The vector entry is popped only after hot unlink or cold-marker removal.

The existing reverse order, polling budget, and cooperative yield remain
unchanged.

### Shared transition wait

`Table::wait_transition_route_or_poison` is the single protocol used by
foreground update/delete, deferred index mutation, and row rollback.

The helper checks sticky health and the pivot, samples the route epoch,
registers a poison listener, then rechecks health and the pivot before racing
the route epoch against poison. The pivot is authoritative; the epoch is only
a wake hint.

The health check immediately before the successful pivot recheck closes
poison-before-registration and simultaneous route/poison windows. It is the
acceptance linearization point for the synchronous retry. A separate health
check after the wait race was intentionally removed during review because the
loop head performs the same check before any subsequent success path.

Clean shutdown does not cancel this wait. Foreground statements retain their
operation owner; terminal, abandoned, and failed-precommit cleanup retain their
mandatory owner.

### Error and retention policy

Row rollback now propagates Runtime-or-Fatal results through statement,
terminal, and precommit boundaries.

- A Runtime rollback access failure transfers residual ownership, stacks
  `FatalError::RollbackAccess`, and publishes poison.
- An already-Fatal route result preserves the first stored poison reason and
  transfers the same residual ownership without republishing a replacement
  reason.
- Successful rollback clears effects, bindings, purge bookkeeping, locks, and
  status/session state in the established order.

The rollback context remains borrowed and no engine handle, status, guard root,
or heap allocation is cloned per undo.

## Implementation Notes

The shipped implementation closes both unsafe completion paths: a transition
page is never mutated by rollback, and a stale exact page generation no longer
causes an undo to be popped while the pivot remains hot.

`RowUndoRollbackAttempt` was moved from the table module to
`trx/undo/row.rs` during review because it describes row-undo progress rather
than table lifecycle. The callback-based `MemTable::rollback_row_undo` was
replaced by the exact-page attempt, while `RowUndoLogs` became the sole owner
of pivot classification, cold marker removal, route waiting, and retry.

The former accessor-local transition waiter moved to `Table` and all existing
foreground callers were routed through it. Deferred index mutation now
documents why its provisional undo remains owned while the page is released
and the row location is re-resolved after wake.

Review also distinguished two apparently similar health checks. The
post-`select!` check was redundant because control returns directly to the
loop-head health check. The health check after listener registration is
required because the following pivot recheck can return success without
polling the poison listener; it preserves canonical poison precedence.

Final validation also exposed a legacy marker test that manually ended the
checkpoint workflow without publishing either a cold route or poison. The
correct transition-aware rollback then waited indefinitely. The test now uses
the real checkpoint phase gate, inspects the uncommitted Lock marker while
publication is paused, releases authoritative route publication, and only then
triggers statement rollback.

Typed errors were widened only along row-rollback seams. Runtime failures still
become `RollbackAccess` at their existing policy owners, while Fatal route
failures retain their original checkpoint reason. Fatal cleanup retains the
exact unresolved row undo and marker in `FailedRetained`.

No planned public, persistent, recovery, checkpoint-ordering, or catalog
behavior changed. No follow-up work was deferred from this task.

Final verification completed:

- deterministic hot classification, statement, move, cancellation,
  failed-precommit, poison-retention, page-miss, lost-wakeup, and fast-path
  tests;
- 100-iteration focused stress runs for transition/cancellation,
  route-registration precedence, and page-miss routing;
- full workspace validation with 1,733 passing tests;
- alternate `libaio` validation with 1,664 passing tests;
- strict default and `libaio` Clippy;
- branch-diff style audit across nine Rust files.

## Impacts

- Row undo now owns transition-aware hot/cold retry policy and a narrow borrowed
  rollback context.
- MemTable exposes exact-page rollback classification without routing policy.
- Foreground and rollback paths share the same table-level route-or-poison
  protocol.
- Statement, terminal, abandoned, and failed-precommit cleanup preserve typed
  Fatal propagation and existing ownership transfer.
- Transaction and shutdown documentation now records transition-aware rollback
  ownership and wait semantics.
- Test-only hooks expose listener-registration boundaries and abandoned cleanup
  claims without sleeps or elapsed-time progress.
- Public APIs, storage formats, recovery, compatibility, and checkpoint
  publication order are unchanged.
- Stable hot, cold-origin, and already-below-pivot rollback add no poison
  health checks or listener registrations.

## Test Cases

1. Active and Frozen hot pages unlink the exact undo; Transition preserves the
   row image, delete bit, undo head, dirty state, and mutation version; a stale
   generation reports `PageMissing`.
2. Ordinary statement rollback waits through transition with its exact undo
   and marker retained, preserves MVCC visibility, then restores row/index state
   and the original statement error after publication.
3. Frozen-page move rollback removes the active replacement first, waits on the
   old transition row, then restores the exact original row and index mappings.
4. Dropping a statement rollback future folds residual undo into transaction
   effects; abandoned mandatory cleanup resumes it without double rollback.
5. Dropping an explicit rollback observer does not cancel the mandatory
   terminal job, which reaches Terminal and releases locks and markers.
6. Failed-precommit rollback keeps prepare and redo observers blocked until
   undo, marker, purge, lock, and session cleanup complete.
7. Checkpoint failure wakes rollback with canonical
   `FatalError::CheckpointWrite`, retains the exact undo and marker, enters
   `FailedRetained`, and rejects session reuse.
8. A stale versioned page waits while above the pivot, completes only after a
   cold route is published, and remains owned when poison wins.
9. Deterministic registration-boundary tests cover route publication before and
   after listener registration, poison before the health recheck, and
   simultaneous route/poison precedence.
10. Stable hot, cold-origin, and already-cold paths perform zero additional
    poison health checks or listener registrations.
11. Existing row/index rollback, cancellation, retention, and foreground
    transition regressions remain green on both supported I/O backends.

## Open Questions

None.
