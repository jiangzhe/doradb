---
id: 000272
title: Route Row Undo Rollback Through Page Transition
status: proposal  # proposal | implemented | superseded
created: 2026-08-18
github_issue: 983
---

# Task: Route Row Undo Rollback Through Page Transition

## Summary

Make user-table row-undo rollback safe while an undo's original hot page is in
`RowPageState::Transition`. Rollback must not mutate the transition page or
treat a temporarily missing page generation as successful. It must keep the
current boxed undo owned, release page and row guards, wait for authoritative
cold-route publication or engine poison, and retry from the current pivot.

Separate one-page rollback classification from semantic retry policy. A narrow
rollback context will carry only pool guards and engine-poison authority through
statement, explicit or abandoned terminal, and failed-precommit rollback. The
existing foreground route-epoch/poison protocol will become a shared table
helper. Stable hot and already-cold rollback will retain their current pivot,
locking, allocation, and poison-observation costs.

## Context

Issue Labels:

- type:task
- priority:medium
- codex

Source Backlogs:

- `docs/backlogs/000185-row-undo-rollback-through-page-transition.md`

Related implemented tasks:

- `docs/tasks/000219-optimize-frozen-page-checkpoint-transition-planning.md`
  deliberately excluded rollback on `Transition` pages.
- `docs/tasks/000264-engine-poison-foreground-waiters.md` defines the canonical
  poison-aware wait protocol and production wait review contract.
- `docs/tasks/000271-index-mutation-unique-driver-key-changes.md` retains
  provisional hot row locks across delayed application and can carry that
  ownership through checkpoint publication.
- `docs/tasks/000174-transaction-terminal-rollback-cancellation-safety.md`
  transfers explicit terminal rollback to mandatory cleanup before its first
  storage await.

Checkpoint transition has two distinct publication boundaries. Under the
page-state write lock, `Table::apply_page_transition` changes a frozen page to
`Transition` and installs `DeleteMarker` overlays derived from its prepared
plan. Only after LWC construction and table-file root publication does
`BlockIndexRoot::update_column_root` atomically advance the pivot/root and
notify the route epoch. The page and marker therefore exist in an intentional
interval where the original hot page must not be modified but the pivot does
not yet authorize cold routing.

`MemTable::rollback_row_undo` currently checks the pivot once, returns success
when `get_row_page_versioned_shared` returns no matching generation, and calls
`write_row_by_id` without rejecting `Transition`. A rollback can consequently
alter a page after the checkpoint plan and marker were fixed, or pop and free
an undo that was never unlinked from either authoritative representation.

The surrounding ownership model is already cancellation-safe. `RowUndoLogs`
keeps its current `OwnedRowUndo` vector-owned across each await and pops it only
after the page operation returns. Dropping an active public statement folds
residual statement undo into transaction effects and requests abandoned
cleanup. Explicit terminal, abandoned, and failed-precommit rollback are
mandatory-runtime jobs, and supervised panic paths retain unsafe ownership.
This task preserves that model while making the new transition wait use it
correctly.

The strict RFC gate does not require an RFC. This task changes no public API,
durable format, data model, recovery protocol, or rollout sequence. It repairs
one converged row-undo path using an existing production wait family and is
narrowly testable.

## Goals

1. Never mutate a user row page while its state is `Transition`.
2. Never treat a missing original page generation as completed rollback while
   the authoritative pivot still classifies the row as hot.
3. Keep the current `OwnedRowUndo` vector-owned until hot unlink or cold marker
   removal succeeds.
4. After transition or page-miss observation, release page, state, and row
   guards before waiting for route progress or poison.
5. Re-resolve the same undo from the current pivot after successful route
   publication and remove the transaction-owned cold marker before popping it.
6. Preserve the first canonical Fatal report when poison makes cold-route
   publication impossible, without popping the undo or mutating unresolved
   state.
7. Carry the same narrow rollback context through ordinary statement,
   explicit or abandoned terminal, and failed-precommit rollback.
8. Reuse one lost-wakeup-safe route-or-poison implementation for foreground
   mutation and rollback.
9. Keep stable hot, cold-origin, and already-below-pivot rollback free of poison
   health checks, listener registration, route-epoch reads, and new allocation.
10. Preserve catalog-table rollback behavior and existing index-before-row
    rollback ordering.
11. Add deterministic physical-state, ownership, cancellation, poison, route
    race, and fast-path regression coverage.

## Non-Goals

1. Do not change checkpoint planning, marker selection, table-root publication,
   row-page retirement, or block-index durable state.
2. Do not introduce a generalized hot/cold route-token framework or migrate
   unrelated row lookup and mutation APIs.
3. Do not change public transaction, statement, table, or checkpoint APIs.
4. Do not change row, undo, redo, LWC, table-file, catalog, or recovery formats.
5. Do not reorder index and row rollback or parallelize rollback within a
   transaction.
6. Do not redesign statement error suppression or eager failure rollback;
   backlog `000186` owns that work.
7. Do not make clean shutdown cancel route waits or mandatory cleanup.
8. Do not change catalog page lifecycle or add transition routing to catalog
   rollback.
9. Do not cache or batch pivot snapshots across undo entries in this task.
10. Do not add a rollback benchmark or performance threshold without workload
    evidence; deterministic fast-path observation tests are sufficient here.

## Plan

### 1. Separate hot-page attempt classification from semantic policy

Replace the callback-based `MemTable::rollback_row_undo` with a one-page
attempt, named along the lines of `try_rollback_hot_row_undo`, and a private
crate-visible result:

```rust
enum RowUndoRollbackAttempt {
    Applied,
    PageMissing,
    Transition,
}
```

The caller resolves cold routing; the MemTable attempt owns only page access:

1. Require or extract the undo's `VersionedPageID` and try to load that exact
   generation.
2. Return `PageMissing` when no matching generation is present. Do not report
   rollback success and do not alter the undo entry.
3. Acquire the page-state read guard before the row write latch.
4. If the retained state is `Transition`, return `Transition` without acquiring
   mutable row access or changing the row image, delete bit, undo head, dirty
   flag, or frozen mutation version.
5. For `Active` or `Frozen`, pass the already-held state guard into
   `write_row_with_state_guard`, call `rollback_first_undo`, and return
   `Applied` only after synchronous unlink completes.

The attempt must not inspect engine poison, register route listeners, remove
deletion-buffer markers, or decide whether a missing page is a valid cold
route. Catalog rollback maps `Applied` to completion, retains its existing
missing-page no-op, and treats a catalog `Transition` result as an internal
lifecycle invariant violation.

### 2. Make `RowUndoLogs` the authoritative retry owner

Add an immutable borrowed context in `trx/undo/row.rs`:

```rust
struct RowUndoRollbackContext<'a> {
    pool_guards: &'a PoolGuards,
    poisoner: &'a EnginePoisoner,
}
```

Change `RowUndoLogs::rollback` to accept this context beside `TableCache` and
return `RuntimeOrFatalResult<()>`. For each newest entry, keep using
`last_mut()` across every await and follow this user-table state machine:

1. If `page_id` is `None`, the undo originated cold. Remove its deletion-buffer
   marker and complete the entry without reading the pivot.
2. Otherwise read the current pivot once. If `row_id < pivot`, remove the cold
   marker and complete the entry.
3. If the row remains hot, run the exact-page attempt.
4. On `Applied`, complete the entry.
5. On `Transition` or `PageMissing`, release every page, state, and row owner,
   then invoke the shared route-or-poison helper. A missing generation is an
   unresolved checkpoint route while the pivot remains hot, not successful
   rollback.
6. After normal wake, loop from the pivot instead of assuming what the
   notification meant. The expected next classification is cold, but the
   predicate remains authoritative.
7. Pop the entry only after hot unlink or cold marker removal returns
   successfully. On Runtime or Fatal error, leave the current entry and every
   older entry in the vector.

Retain the existing reverse order, polling budget, and cooperative yield. Do
not hold a `TableCache` mutable lookup borrow in a way that prevents the same
entry from retrying after the await; the cached `Arc<Table>` remains the pinned
runtime authority for the loop.

### 3. Share the row-page transition wait protocol

Move the existing `UserTableAccessor::wait_transition_route_or_poison` logic to
a crate-private `Table` method in the page-transition/table-access boundary,
with inputs equivalent to `(&EnginePoisoner, RowID)`. Foreground update,
delete, and deferred index-mutation resumption call the same implementation as
rollback.

The helper begins only after a caller observed `Transition` or `PageMissing`:

1. Check sticky health and whether the current pivot already classifies the row
   as cold.
2. Capture the route epoch and register the poison listener.
3. Recheck the pivot and sticky health to close publication-before-registration
   and poison-before-registration windows.
4. Race `wait_route_since(observed_epoch)` against the poison listener.
5. Recheck sticky health after either wake and loop until the pivot is cold.

The semantic wait contract is:

- Progress producer: the admitted checkpoint publishes the table root and then
  calls `BlockIndexRoot::update_column_root`.
- Authoritative result: `row_id < pivot_row_id`; the route epoch is only a wake
  hint.
- Poison: checkpoint failure after transition may strand route publication, so
  already-published or racing poison returns the first Fatal report.
- Shutdown: clean shutdown does not cancel the wait; the active statement or
  mandatory cleanup owner participates in normal drain.
- Cancellation owner: `RowUndoLogs` retains the boxed current entry, while the
  enclosing `StmtState`, terminal claim, abandoned job, or precommit payload
  owns transfer or fatal retention.
- Acceptance linearization: the final successful health check authorizes the
  immediate synchronous retry; there is no await before consuming that
  authority.

This is another consumer of the existing row-page transition route family, not
a new production wait category. Update `docs/shutdown-and-poison.md` and
`docs/transaction-system.md` with the rollback ownership details.

### 4. Preserve the ordinary rollback fast path

The implementation must preserve these costs:

- A cold-origin undo (`page_id == None`) performs no pivot, route-epoch, poison,
  or listener operation.
- An undo with a page id performs the same one pivot snapshot already present
  in `MemTable::rollback_row_undo` before exact-page access.
- A stable hot page acquires the same page-state guard and row latch as today.
  The only added common-path operation is the predictable
  `state == Transition` comparison under the already-held guard.
- `ensure_healthy`, poison-listener registration, and route-epoch reads occur
  only in the `Transition | PageMissing` slow branch.
- The rollback context is borrowed, the attempt result is an inline enum, and
  no engine handle, status, guard root, or heap allocation is cloned or created
  per undo.

Do not introduce pivot caching across entries. A correctness task should not
weaken current-route observation or add invalidation machinery without measured
need. Use existing test-only poison observation counters to enforce zero health
checks and listener registrations on ordinary hot and cold paths.

### 5. Preserve typed errors and fatal retention

Widen only row-rollback seams that can now observe existing poison:

- `RowUndoLogs::rollback` and `StmtEffects::rollback_row` return
  `RuntimeOrFatalResult`.
- `PrecommitTrxPayload::rollback` returns `RuntimeOrFatalResult`, converting its
  still-Runtime index rollback arm without losing the subsequent Fatal row arm.
- Terminal rollback constructs `RowUndoRollbackContext` from its
  `TrxAttachment` pool guards and `TransactionSystem` poisoner.

At statement, terminal/abandoned, and failed-precommit policy owners:

- A Runtime page-access failure first transfers all residual rollback
  ownership into the existing fatal-retention owner, then stacks
  `FatalError::RollbackAccess`, publishes poison, and returns that report.
- An already-Fatal route result means poison is already published. Retain the
  same residual ownership and return the report with caller-owned rollback
  diagnostics, without converting it to Runtime, replacing its current Fatal
  reason, or publishing a second rollback reason.
- The current undo must remain in its vector before either arm transfers or
  retains the enclosing payload.

Keep index rollback before row rollback. Do not add a thin shared
poison-publication wrapper: statement, terminal, and failed-precommit owners
have different state/retention transitions and should keep their policy local.

### 6. Preserve cancellation and mandatory ownership

The route wait adds no new cancellation mechanism:

- If a public `Transaction::exec` future is dropped while statement rollback
  waits, dropping the listener is neutral. `StmtState::Drop` settles deferred
  effects, folds the still-owned current and older row undo into transaction
  effects, publishes `CleanupReady`, and requests abandoned mandatory cleanup.
  That job constructs a fresh rollback context and resumes the same
  authoritative route loop.
- Explicit rollback continues to submit `TerminalRollbackCleanupJob` before
  any storage await. Dropping its public completion waiter cannot cancel the
  transition wait or release undo.
- Abandoned and failed-precommit paths remain mandatory internal tasks and are
  not preempted by clean shutdown or unrelated future cancellation.
- Supervised panic handling continues to move the current job or claim into
  fatal retention before its owner is dropped.

No path may await route progress after unlinking the current undo, and no path
may pop the vector entry before the final hot or cold action succeeds.

## Implementation Notes

## Impacts

- `doradb-storage/src/table/mem_table.rs`
  - replace callback-based row rollback with one-page attempt classification
  - reject `Transition` under the page-state guard
- `doradb-storage/src/trx/undo/row.rs`
  - add `RowUndoRollbackContext`
  - own pivot classification, cold marker removal, wait/retry, and typed result
- `doradb-storage/src/table/page_transition.rs`
  - host the shared table-level route-or-poison predicate protocol
- `doradb-storage/src/table/access.rs`
  - route existing foreground transition callers through the shared helper
- `doradb-storage/src/trx/stmt.rs`
  - propagate Runtime/Fatal row rollback and preserve statement cancellation
    and retention behavior
- `doradb-storage/src/trx/sys.rs`
  - construct terminal/abandoned rollback context and retain existing Fatal
    sources
- `doradb-storage/src/trx/mod.rs`
  - carry the context and typed result through failed-precommit rollback
- `doradb-storage/src/table/rollback.rs`, transaction test modules, and existing
  checkpoint test support
  - add deterministic state, lifecycle, poison, and performance coverage
- `docs/transaction-system.md` and `docs/shutdown-and-poison.md`
  - document transition-aware rollback and its existing wait-family contract

Public APIs, persistent formats, recovery, checkpoint publication order, and
catalog runtime behavior are unchanged.

## Test Cases

1. Hot-attempt classification:
   - Verify `Active` and `Frozen` pages apply and unlink the exact owned undo.
   - Verify `Transition` returns without changing the row image, delete bit,
     undo head, dirty state, frozen mutation version, or owned entry.
   - Verify a stale `VersionedPageID` returns `PageMissing`, not success.

2. Ordinary statement rollback through transition:
   - Delete a row on a frozen page inside a statement, retaining statement row
     and index undo.
   - Start checkpoint and use the existing asynchronous checkpoint phase gate
     after transition state and marker installation but before pivot
     publication.
   - Return an injected ordinary statement error and prove rollback remains
     pending.
   - While pending, assert the physical delete bit remains set, the undo head
     still names the exact owned entry, the marker remains the same
     `DeleteMarker::Ref`, the pivot remains hot, and MVCC reads reconstruct the
     correct pre-delete image.
   - Release checkpoint; assert pivot publication wakes rollback, the marker is
     removed, the original statement error is returned, the original row is
     visible, row ownership is gone, and the transaction remains reusable.
     Transaction-lifetime logical locks may remain until terminal processing,
     as required by the existing lock contract.

3. Reverse-order move-update rollback:
   - Produce the normal frozen-page move shape: a `Delete` undo for the old
     frozen row and an `Insert` undo for its active replacement.
   - Transition the old page and start terminal rollback.
   - Prove rollback removes the replacement first, then waits without mutating
     the old page, and after publication restores exactly the original row and
     every index mapping with no replacement or marker remaining.

4. Statement-future cancellation during route wait:
   - Reach the pending ordinary statement rollback state and drop the
     `Transaction::exec` future.
   - Assert residual undo is folded into transaction effects, the operation
     becomes `CleanupReady`, and abandoned mandatory cleanup claims it.
   - Publish the route and assert cleanup reaches the pre-statement row/index
     state without double rollback, dangling undo ownership, or fatal
     retention.

5. Explicit terminal rollback observer cancellation:
   - Start `Transaction::rollback`, wait until its mandatory terminal job is
     blocked on the transition route, and drop the public rollback waiter.
   - Publish the route and assert the mandatory job still reaches `Terminal`,
     removes row/index undo and the marker, releases every transaction logical
     lock, and leaves the session outside a transaction.

6. Failed-precommit rollback ordering:
   - Use the internal precommit harness with transaction-owned delete state
     captured into a transition marker.
   - Start failed-precommit rollback while publication is paused and prove its
     prepare waiters and redo completion observer remain blocked.
   - Publish the route and assert row/index undo, marker ownership, purge
     bookkeeping, locks, and session rollback complete before prepare waiters
     and redo observers are released.

7. Checkpoint poison while rollback waits:
   - Pause after transition, start terminal rollback, force the existing LWC
     build failure, and release the checkpoint.
   - Assert poison wakes rollback with the canonical
     `FatalError::CheckpointWrite`, not a replacement `RollbackAccess` reason.
   - Assert the undo is not popped, the transition page and marker remain
     unchanged, the operation becomes `FailedRetained`, fatal retention names
     the exact table/row owner, and session reuse is rejected.

8. Versioned-page-miss routing race:
   - Construct a user undo with a deliberately stale page generation while its
     row remains above the pivot and has the matching transition/cold marker.
   - Poll rollback and prove it remains pending rather than popping the undo.
   - Publish a pivot that classifies the row as cold; assert the epoch wakes
     rollback, the marker is removed, and only then is the entry popped.
   - In a companion case publish poison instead of a route and assert the entry
     remains owned for fatal retention.

9. Lost-wakeup and result-precedence boundaries:
   - Route publication before route-wait registration is found by the pivot
     recheck.
   - Poison after poison-listener registration but before the health recheck is
     returned as Fatal.
   - Route and poison becoming ready together still return already-published
     poison after the final health check.
   - Normal publication after registration wakes and authorizes exactly one
     authoritative retry.
   - Use channels, existing epoch primitives, and a minimal `#[cfg(test)]`
     semantic phase hook only where required; do not use sleeps or elapsed time
     as progress.

10. Fast-path performance contract:
    - Snapshot `EnginePoisoner::test_observation_counts` immediately around
      stable hot, cold-origin, and already-below-pivot rollback.
    - Assert zero added health checks and zero poison-listener registrations.
    - Assert transition and missing-page cases are the only cases entering the
      route/poison slow path.

11. Regression and validation:
    - Retain existing insert, delete, update, lock, secondary-index, statement
      cancellation, terminal cancellation, failed-precommit retention, and
      foreground transition tests.
    - Stress the focused transition/cancellation and lost-wakeup tests with
      `rtk cargo nextest run -p doradb-storage --stress-count 100 <filters>`.
    - Run `rtk cargo nextest run --workspace`.
    - Run
      `rtk cargo nextest run -p doradb-storage --no-default-features --features libaio`.
    - Run formatting, strict workspace Clippy, and the branch-diff style audit
      required by repository process.

Acceptance requires both the successful-publication and poison paths to prove
the current undo is never popped before its authoritative representation is
resolved. Passing tests without those intermediate physical and ownership
assertions is insufficient.

## Open Questions

None.
