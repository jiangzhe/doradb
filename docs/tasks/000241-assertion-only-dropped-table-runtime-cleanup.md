---
id: 000241
title: Make Dropped-Table Runtime Cleanup Assertion-Only
status: implemented  # proposal | implemented | superseded
created: 2026-07-26
github_issue: 898
---

# Task: Make Dropped-Table Runtime Cleanup Assertion-Only

## Summary

Make dropped-table runtime destruction a one-way, assertion-only transition.
Every production strong holder of `Table`, `TableRuntimeLayout`, or
`SecondaryIndex` must release through one explicit authority before
dropped-table GC becomes eligible: logical metadata locks, the strict active-STS
horizon, purge phase completion, a borrowed catalog-map guard, or offline
recovery.

Unify finite session maintenance access behind scoped
`TableMetadata(S) -> TableData(IS)` admission. Redesign
`wait_for_checkpoint_retry` so it holds those locks and the table runtime only
while performing one bounded predicate observation; it must detach owned
notification listeners and release all table, layout, checkpoint-attempt, page,
and lock state before sleeping. Replace internal catalog readers that clone a
table merely to copy replay/root facts with borrowed or copied non-runtime
projections.

Once all production holders are covered, remove dropped-runtime
probe/restore/retry behavior. The strict-horizon purge path must detach the
catalog-owned runtime once, require `Arc::try_unwrap(Table)` to succeed, destroy
the table by value, and continue to the existing checkpoint-gated file cleanup.
The existing nested layout and secondary-index uniqueness assertions remain the
final proof for resources owned below `Table`.

## Context

Issue Labels:

- type:task
- priority:medium
- codex

Source Backlogs:

- `docs/backlogs/closed/000166-replace-arc-probed-dropped-table-runtime-purge.md`

Related Backlogs:

- `docs/backlogs/closed/000098-dropped-table-purge-retry-stall.md`

Related Designs And Tasks:

- `docs/rfcs/0017-drop-table-lifecycle-recovery.md`
- `docs/rfcs/0024-versioned-metadata-immediate-retirement.md`
- `docs/tasks/000145-gc-managed-dropped-table-destroy.md`
- `docs/tasks/000170-table-handle-catalog-ownership.md`
- `docs/tasks/000240-operational-reclamation-recovery-validation.md`

`Catalog` currently retains a logically dropped table as
`DroppedTableOperationalState::Runtime`. Once `drop_cts < min_active_sts`,
purge changes that entry to `Floor`, calls `Arc::try_unwrap`, and either destroys
the uniquely owned table or restores the runtime when another strong handle
exists. Restoration has no matching final-handle notification. It therefore
depends on a later unrelated purge wake, as recorded by backlog 000098.

`Arc::try_unwrap` is race-free as a uniqueness check, but the current restore
branch makes it both a readiness probe and an incomplete scheduling protocol.
If admission closes before all existing strong holders drain, uniqueness is
instead an invariant: `try_unwrap` remains useful as the final assertion, but a
failed assertion is a holder-discipline bug rather than retryable runtime
state.

Most production holders already have a release authority:

| Holder | Current or required release authority |
| --- | --- |
| `TransactionTableBinding` table/layout pair | Transaction-lifetime `TableMetadata(S)`; DROP takes metadata X |
| Lazy index stream table/layout and owned index cursors | Transaction binding; cursor/index state must be destroyed before transaction locks |
| Freeze and checkpoint operations | Scoped metadata S/data IS; checkpoint publication also uses `TableDropDrain` |
| Transaction and statement rollback `TableCache` | Transaction locks remain held until rollback clears effects, bindings, and cache |
| Purge bucket/index cache | Purge coordinator joins bucket work before dropped-table GC |
| Retired-row-page purge pin | Purge coordinator completes retirement before dropped-table GC |
| MemIndex cleanup | Currently active STS only; this task also gives the finite operation scoped logical locks |
| `total_row_pages` | Currently none; this task gives it scoped logical locks |
| Checkpoint retry recheck | This task gives each bounded recheck scoped logical locks |
| Checkpoint retry sleep | This task permits detached listeners only, with no runtime pin |
| Catalog checkpoint index-DDL root proof | This task replaces its strong clone with borrowed inspection |
| Redo-retention replay-floor snapshot | This task returns copied floors without escaping a strong clone |
| Recovery | Offline before normal runtime admission |
| Session cache | `Weak<Table>` only |

No production holder may be justified merely as “short-lived.” A synchronous
reader can be descheduled while holding an Arc, so it must either avoid the
strong clone or participate in an authority that dropped-runtime detachment
must cross.

The detached checkpoint-wait design can reuse existing notification types.
`event_listener::EventListener` owns the event's internal listener state rather
than the enclosing `Table`. Transaction terminal listeners similarly own
`Arc<SharedTrxStatus>`, and GC-horizon, poison, and shutdown listeners are
engine-owned. Registering listeners and then rechecking the predicate before
releasing scoped access prevents a lost notification; notification after the
recheck remains recorded in the detached listener.

External implementation comparison supports this separation:

- PostgreSQL makes `DROP TABLE` acquire `ACCESS EXCLUSIVE`, drains conflicting
  relation users, checks backend-local open use, and schedules physical storage
  unlink at commit:
  `https://www.postgresql.org/docs/current/explicit-locking.html`,
  `https://github.com/postgres/postgres/blob/master/src/backend/catalog/heap.c`,
  and
  `https://github.com/postgres/postgres/blob/master/src/backend/catalog/storage.c`.
- MySQL retains metadata locks until statement or transaction completion so
  DROP waits for users; InnoDB separately checks internal references and has a
  defensive background-drop path:
  `https://dev.mysql.com/doc/refman/8.4/en/metadata-locking.html`,
  `https://github.com/mysql/mysql-server/blob/trunk/sql/sql_table.cc`, and
  `https://github.com/mysql/mysql-server/blob/trunk/storage/innobase/row/row0mysql.cc`.

The approved direction uses Doradb's existing logical lock manager for finite
foreground holders, detaches long-lived waits, and makes the storage-layer
uniqueness check an assertion after complete production-path coverage.

The RFC complexity gate passes for this narrowed task. It changes only volatile
runtime access, notification waiting, internal projections, purge assertions,
tests, and live documentation. It does not change a durable format, public API,
recovery policy, worker topology, table ownership split, or RFC phase contract.

## Goals

1. Inventory every production construction or clone of `Arc<Table>`,
   `Arc<TableRuntimeLayout>`, and `Arc<SecondaryIndex>` and assign exactly one
   release authority to each.
2. Add one private scoped access abstraction for finite session maintenance
   operations.
3. Acquire scoped table access in the established order
   `TableMetadata(S) -> TableData(IS) -> current live runtime resolution`.
4. Guarantee that scoped access drops its table Arc before releasing fresh
   metadata/data lock guards.
5. Migrate freeze, checkpoint, row-page counting, MemIndex cleanup, and
   checkpoint-retry rechecks to the common scoped access abstraction without
   changing each method's current idle-session policy.
6. Split checkpoint retry into bounded predicate observation and detached
   notification waiting.
7. Ensure no indefinite checkpoint-retry await retains a table/layout Arc,
   `CheckpointAttempt`, frozen-page guard, or logical table lock.
8. Preserve the existing public `CheckpointDelayReason`,
   `wait_for_checkpoint_retry`, and `checkpoint_table_with_wait` behavior:
   completion means the original predicate is satisfied or obsolete, while a
   later checkpoint attempt may observe another delay.
9. Remove strong table clones from catalog checkpoint root proof and redo-floor
   snapshot paths when only copied or borrowed facts are required.
10. Preserve transaction binding, rollback, DDL, purge phase, and recovery
    authorities rather than adding a second lease to those paths.
11. Replace top-level dropped-runtime probe/restore with one
    `Arc::try_unwrap` invariant assertion and consuming destroy.
12. Remove all dropped-runtime restoration APIs, state transitions, logging,
    and stale-handle worklists.
13. Preserve the strict eligibility boundary `drop_cts < min_active_sts`.
14. Preserve fatal poison behavior for fallible consuming destruction after
    uniqueness has been established.
15. Preserve catalog-checkpoint gating and recovery behavior for physical
    table-file deletion.
16. Prove all holder classes and the detached-wait race protocol with
    deterministic tests using production synchronization.
17. Update live ownership/GC documentation and resolve or narrow the source
    backlogs according to the implemented result.

## Non-Goals

1. Do not split `Table` into an Arc-backed shell and `OwnedTableRuntime`.
2. Do not introduce a general `TableRuntimeLease` or a table lifecycle use
   counter.
3. Do not add a new reclaimer, worker, retry timer, backoff loop, or global
   holder registry.
4. Do not hold metadata S or data IS across the indefinite sleep inside
   `wait_for_checkpoint_retry`.
5. Do not change transaction first-touch binding, logical lock modes, DDL lock
   ordering, stable index numbers, or transaction-visible schema behavior.
6. Do not change `CheckpointDelayReason`, `CheckpointOutcome`, or other public
   API signatures.
7. Do not change table-file, catalog-file, root, redo, undo, checkpoint, or
   recovery formats.
8. Do not change the active-STS horizon or catalog checkpoint boundary.
9. Do not make logical DROP synchronously destroy runtime memory or delete the
   table file.
10. Do not change checkpoint-gated or retryable table-file deletion policy;
    backlog 000098 remains responsible for any file-delete-only retry issue.
11. Do not add unsafe code.
12. Do not expand or amend RFC 0017 or RFC 0024.
13. Do not add brittle source-text tests for Arc call sites; the inventory is a
    review contract and tests must prove behavior at the authority boundaries.

## Plan

### 1. Fix the production holder and release matrix

Audit all production call sites of:

- `Catalog::get_table` and `get_table_now`;
- `Catalog::resolve_user_table_current`;
- `Catalog::current_live_user_table`;
- `Catalog::pin_user_table_for_purge`;
- `Table::layout_snapshot`;
- `TableRuntimeLayout::secondary_indexes`;
- owned secondary-index scan handles and streams.

Classify each result under the approved matrix. Test-only helpers may retain raw
Arcs only within an explicitly bounded test scope and must drop them before a
healthy dropped-runtime purge assertion.

Keep transaction bindings unchanged: first-touch statement metadata S is handed
to transaction metadata S before `TransactionTableBinding` is installed.
Confirm commit, rollback, failed-precommit cleanup, and abandoned-transaction
cleanup clear bindings and operation caches before releasing transaction locks.

Confirm `IndexScanMvccStreamState` and its owned candidate cursor release
secondary-index, table, and layout state before the final transaction/statement
lock owner. Adjust field ownership or explicit teardown order if the audit
finds an ambiguous destructor ordering.

Keep purge work ordering unchanged but document it as an assertion prerequisite:
bucket tasks and `TableCache` layouts are joined, then retired row pages are
processed, then retained roots/history are processed, and only then does
dropped-table GC run.

### 2. Introduce scoped finite session runtime access

Add a private `ScopedTableRuntimeAccess<'lock>` in
`doradb-storage/src/session.rs`. It should contain:

- an `Option<Arc<Table>>` or equivalent private table owner;
- the optional fresh `TableMetadata(S)` guard;
- the optional fresh `TableData(IS)` guard.

Acquisition must:

1. preserve the caller's existing idle-session check policy;
2. acquire grouped metadata S and data IS with the session owner/group;
3. resolve current catalog state only after both grants;
4. validate lifecycle `Live` for ordinary finite operations;
5. update only the existing weak session cache;
6. return borrowed table access without exposing a new clone operation.

Implement explicit destruction ordering: take and drop the table Arc before
fresh lock guards are destroyed. Do not rely only on incidental local-variable
or struct-field order for this invariant. Existing owner grants may yield no
fresh guard and remain governed by their explicit owner lifetime.

Migrate:

- `Session::freeze_table`;
- `Session::checkpoint_table`;
- `Session::total_row_pages`;
- `Session::cleanup_secondary_mem_indexes`.

The finite operation keeps scoped access through its last table/layout/index
use. Ordinary IX DML remains compatible with data IS. Same-table DROP waits
behind metadata S and data IS until the operation has released its strong
runtime state.

Add a retry-recheck acquisition variant that treats catalog absence or terminal
state as an obsolete delay rather than returning a foreground
`TableNotFound`/`TableDropping` error.

### 3. Detach checkpoint retry notification waits

In `doradb-storage/src/table/persistence.rs`, split the current
`Table::wait_for_checkpoint_retry` loops into one bounded observation and one
runtime-free wait. Introduce private shapes equivalent to:

```rust
enum CheckpointRetryObservation {
    Ready,
    Wait(DetachedCheckpointRetryWait),
}

struct DetachedCheckpointRetryWait {
    listeners: Vec<EventListener>,
}
```

`DetachedCheckpointRetryWait` must not contain or borrow:

- `Table`, `TableRuntimeLayout`, or `SecondaryIndex`;
- `TableFile` or a table-owned storage runtime;
- `CheckpointAttempt` or `FrozenPageBatch`;
- buffer/page guards;
- logical lock guards.

Move the outer loop to `Session::wait_for_checkpoint_retry`:

1. acquire `ScopedTableRuntimeAccess` for a recheck;
2. return success if the table is absent/terminal or the original reason is
   obsolete;
3. request required purge observation;
4. register lifecycle, horizon, blocker-terminal, poison, and shutdown
   listeners needed by the current predicate;
5. recheck the predicate while scoped access and any checkpoint/page proof are
   still held;
6. if ready, return and discard listeners;
7. otherwise return a detached waiter;
8. drop scoped access and all bounded proof state;
9. await the detached waiter;
10. repeat from current catalog state.

For `ActiveRoot`, recheck the root effective timestamp and published GC horizon.

For `FrozenPageCutoff`, reacquire a `CheckpointAttempt`, locate the frozen batch
and page, load/reanalyse the page when validation requires it, and preserve
updated validation/blocker state by returning the attempt to the workflow
before detaching. A blocked observation detaches transaction-terminal
listeners. A stable cutoff observation detaches the GC-horizon listener. Batch
or page disappearance makes the original reason obsolete.

`TableLifecycle::start_drop` already notifies lifecycle listeners when it
publishes `Dropping`. Because the waiter releases metadata S before sleeping,
DROP can acquire X, publish the terminal transition, finish logical removal,
and become purge-eligible without waiting for the waiter future to run again.
After wake, the waiter waits behind any active DROP X and then observes terminal
or absent current state.

Add deterministic hooks only at semantic boundaries needed to prove:

- listener registered before final predicate recheck;
- scoped runtime access released before detached await;
- waiter deliberately remains unscheduled while DROP and purge progress.

Do not add sleep-based synchronization.

### 4. Remove strong clones from narrow internal observations

In `doradb-storage/src/catalog/history.rs` and
`doradb-storage/src/catalog/mod.rs`, change live replay-floor observation to
return only `TableRedoReplayFloor`. Borrow the current live table under the
catalog entry guard, copy `redo_replay_floor_snapshot`, and discard the borrow
before advancing the map scan. Update standalone and combined redo-retention
planning callers without changing their output.

In `doradb-storage/src/catalog/checkpoint.rs`, classify index-DDL root proof
while borrowing the current live table through the catalog entry. Do not call
`get_table_now` merely to own an Arc. Keep the entry guard until
`classify_index_ddl_root` has copied/classified the required facts, and do not
return the borrowed table or another strong runtime handle.

Review other synchronous catalog scans for the same pattern. Return copied
metadata/floor/root facts when needed; do not introduce a generic closure API
that casually permits callers to clone and escape the Table Arc.

Catalog publication and dropped-runtime detachment acquire the same map entry
mutably, so they naturally wait for these bounded borrowed observations.

### 5. Make dropped-runtime GC assertion-only

In `doradb-storage/src/trx/purge.rs`:

1. take strict-horizon candidates from the catalog;
2. pass each table through a small private uniqueness helper that includes
   table id and observed strong count in its invariant panic;
3. call `Table::destroy_dropped_runtime` on the uniquely owned value;
4. enqueue the existing checkpoint-gated `DroppedTableFileCleanup`;
5. process file deletes under the existing policy.

Remove the `stale_handles` vector, strong-count restore log, and restore loop.

Remove:

- `Catalog::restore_dropped_runtime`;
- `UserTableEntry::restore_dropped_runtime`;
- `DroppedTableOperationalState` Runtime restoration transitions;
- tests whose expected success behavior is retrying a deliberately retained
  raw Table Arc.

Preserve `take_dropped_runtime`'s one-way `Runtime -> Floor` transition. A
uniqueness failure is an invariant panic, consistent with
`Table::destroy_dropped_runtime` assertions for retired indexes, the current
layout, and active secondary indexes.

Do not change fallible destroy handling. After uniqueness succeeds,
`destroy_dropped_runtime` errors remain fatal and the purge caller converts
them to storage poison.

### 6. Synchronize live documentation and backlog outcomes

Update `docs/garbage-collect.md` and any directly affected live lifetime or
transaction documentation to state:

- logical DROP drains finite foreground runtime holders through metadata/data
  locks;
- checkpoint retry sleeps own notification state but no table runtime;
- strict-horizon dropped GC asserts unique ownership and never restores a
  runtime;
- file deletion remains independently catalog-checkpoint-gated and retryable.

During `$task-resolve`, if all acceptance criteria pass:

- close backlog 000166 as replaced by the lock-drained assertion-only design;
- narrow backlog 000098 to checkpoint-gated/retryable file deletion, removing
  stale-runtime Arc retry from its summary, scope, and acceptance criteria.

If implementation finds a real production holder that cannot be attached to
an existing lock, catalog borrow, purge phase, strict horizon, or offline
boundary without materially widening this task, stop and report that exact
holder. Do not silently retain both assertion-only and restore protocols.
After review, either solve the holder in this task or record a focused follow-up
and leave backlog 000166 open.

No RFC phase-plan edit is required.

### 7. Validate with authoritative runners

Run focused tests while implementing, then:

```bash
rtk cargo nextest run --workspace
rtk cargo nextest run -p doradb-storage --no-default-features --features libaio
```

Follow `docs/process/unit-test.md`. Do not use retry-based test masking or
wall-clock sleeps.

## Implementation Notes

- Added private scoped session runtime access that acquires grouped metadata
  S/data IS admission, resolves the live runtime only after admission, and
  explicitly releases the table Arc before fresh logical-lock guards. Freeze,
  checkpoint, row-page counting, MemIndex cleanup, and checkpoint retry
  rechecks now share this boundary.
- Split checkpoint retry into bounded active-root/frozen-page observations and
  listener-only detached waits. The waits retain no table/layout/index runtime,
  checkpoint attempt, page guard, or logical table lock, so DROP and
  strict-horizon destruction can finish while a retry future remains parked.
- Replaced replay-floor and index-DDL root-proof table clones with catalog-entry
  borrows and copied facts. The holder audit also found and fixed rollback-local
  `TableCache` lifetime: rollback now releases cached table/layout/index owners
  before bindings and transaction locks.
- Removed dropped-runtime restoration and stale-handle retry state. Eligible
  runtimes transition one-way from Runtime to Floor, pass a table-id/count
  uniqueness assertion, and are destroyed by value before the existing
  checkpoint-gated file-cleanup policy.
- Review removed initially planned explicit full-purge observations from
  active-root and stable frozen-page retry waits. Both delays have causal
  transaction completion wakes: an active-root delay requires an active
  snapshot, while a stable cutoff comes from an ordered committed-image
  handoff. Listener registration plus the final predicate recheck closes the
  notification race without scheduling redundant full purge work. The generic
  maintenance-boundary wait retains its explicit observation because it accepts
  arbitrary timestamps without that causal guarantee.
- Added deterministic DROP-drain, detached-wait, notification-race,
  replay-floor ownership, and uniqueness-assertion coverage. The final
  validation passed the branch-diff style audit for 10 Rust files,
  `rtk cargo nextest run --workspace` with 1,546 tests, and the alternate
  `libaio` suite with 1,471 tests. Active-root and both stable-cutoff waits also
  passed focused 100-iteration stress runs without retries or sleeps.
- Resolution archived source backlog 000166 as implemented. It first narrowed
  backlog 000098 to retryable physical file deletion, then archived it as
  `wontfix`: normal transaction/checkpoint purge wakes and checkpoint-absence
  startup cleanup are the accepted progress mechanisms, without a dedicated
  idle retry scheduler. No new deferred work or parent-RFC phase
  synchronization was required.

## Impacts

Primary modules and interfaces:

- `doradb-storage/src/session.rs`
  - `Session::freeze_table`
  - `Session::checkpoint_table`
  - `Session::wait_for_checkpoint_retry`
  - `Session::checkpoint_table_with_wait`
  - `Session::total_row_pages`
  - `Session::cleanup_secondary_mem_indexes`
  - `SessionPin::resolve_user_table`
  - new private `ScopedTableRuntimeAccess`
- `doradb-storage/src/table/persistence.rs`
  - `Table::wait_for_checkpoint_retry`
  - active-root and frozen-page retry predicates
  - new bounded observation and detached waiter shapes
- `doradb-storage/src/catalog/history.rs`
  - `UserTableEntry::live_replay_floor`
  - removal of dropped-runtime restoration
- `doradb-storage/src/catalog/mod.rs`
  - replay-floor snapshots
  - removal of `Catalog::restore_dropped_runtime`
  - review of raw current-runtime accessors
- `doradb-storage/src/catalog/checkpoint.rs`
  - `catalog_checkpoint_index_ddl_action`
- `doradb-storage/src/trx/retention.rs`
  - live/dropped replay-floor planning inputs
- `doradb-storage/src/trx/purge.rs`
  - `process_dropped_table_gc`
  - dropped-runtime purge test events/helpers
- `doradb-storage/src/trx/admission.rs`
  - holder-order audit only unless a concrete ordering fix is needed
- `doradb-storage/src/trx/stream_stmt.rs`
  - owned stream holder-order audit and tests
- `doradb-storage/src/table/mod.rs`
  - existing consuming destroy and nested Arc assertions
- `docs/garbage-collect.md`
- directly affected live lifetime/transaction documentation
- backlog 000166 and backlog 000098 during task resolution

Behavioral impact:

- Finite maintenance operations become explicit DROP blockers while they own
  scoped runtime access.
- A checkpoint retry future parked on a long-lived predicate no longer blocks
  DROP or runtime destruction.
- A top-level stale strong Table holder is an invariant failure instead of
  retryable purge state.
- Public checkpoint, DDL, and maintenance API signatures remain unchanged.
- Durable recovery and physical table-file deletion behavior remain unchanged.

Performance impact:

- Finite maintenance paths add or reuse one grouped logical-lock acquisition
  per operation, never per row/page/index entry.
- Checkpoint retry may repeat bounded table lookup, lock admission, and
  frozen-page analysis after notifications; it avoids holding runtime state
  during potentially long sleeps.
- Replay-floor and root-proof scans avoid unnecessary Arc reference-count
  traffic.

Unsafe considerations:

- No unsafe code is planned.

## Test Cases

1. Pause `total_row_pages` after scoped runtime acquisition. Verify same-table
   DROP waits, the table Arc drops before the fresh locks release, and DROP then
   completes.
2. Pause `cleanup_secondary_mem_indexes` after admission/transaction start.
   Verify DROP waits for the finite maintenance operation and later destroys
   the runtime on its first eligible purge cycle.
3. Migrate freeze and checkpoint through scoped access and retain existing
   tests proving DROP waits for reversible checkpoint work and an admitted
   checkpoint publisher.
4. Create an `ActiveRoot` checkpoint delay, detach its listeners, and park the
   waiter future. Verify DROP commits and eligible runtime destruction finishes
   while the waiter remains unscheduled.
5. Resume the detached active-root waiter after DROP. Verify it rechecks current
   state and returns `Ok(())`.
6. Create a `FrozenPageCutoff` delay blocked by transaction status, detach its
   terminal/lifecycle listeners, and park the waiter. Verify DROP and runtime
   destruction do not wait for the waiter future.
7. Repeat frozen-page detachment for a stable required GC cutoff and verify its
   detached horizon listener owns no table/page/checkpoint state.
8. Notify between listener registration and the final predicate recheck.
   Verify the wait cannot miss the notification or hang.
9. Publish DROP after the final predicate recheck but before the waiter is
   polled. Verify the listener carries the notification and the next recheck
   returns.
10. Remove or replace the frozen batch/page between notifications. Verify the
    original delay is treated as obsolete.
11. Hold a transaction table/layout binding across a queued DROP, then commit.
    Verify DROP waits for metadata S and the first eligible runtime purge
    assertion succeeds.
12. Repeat transaction binding drainage through explicit rollback,
    failed-precommit rollback cleanup, and abandoned transaction cleanup.
13. Hold a public lazy secondary-index stream. Verify its owned index cursors,
    table, and layout release before transaction/statement locks and DROP then
    proceeds.
14. Run purge with bucket-local `TableCache` layout/index work and retired row
    pages for the same table. Verify all such work completes before
    `DroppedTableStarted` and top-level uniqueness succeeds.
15. Pause catalog checkpoint index-DDL proof under its borrowed catalog entry.
    Verify no extra Table strong owner exists and the catalog transition waits
    only for the bounded borrow.
16. Snapshot redo replay floors while DROP races. Verify the returned plan
    contains copied floors and does not retain a Table Arc.
17. Verify weak session cache entries do not change Table strong ownership and
    cannot resurrect a terminal table.
18. Verify recovery constructs/destroys runtimes offline and leaves no
    recovery-only strong holder after online admission starts.
19. Verify the strict equality boundary retains Runtime when
    `min_active_sts == drop_cts`, and the first strictly newer horizon performs
    one-way Runtime-to-Floor destruction.
20. Verify the successful dropped-runtime path contains no restore/requeue
    event and requires no later manual purge wake.
21. Unit-test the private uniqueness helper with an intentionally cloned Table
    Arc and assert that it reports the table id/strong count through the
    invariant panic without routing through asynchronous global purge state.
22. Preserve nested uniqueness coverage for retired indexes, pinned layouts,
    active secondary indexes, DROP INDEX, and streaming index cursors.
23. Verify runtime destruction failure after successful uniqueness still
    poisons storage under the existing fatal error policy.
24. Verify table-file deletion remains blocked until catalog checkpoint makes
    absence durable, then succeeds under existing purge/checkpoint wakes.
25. Run the full default and libaio workspace validation commands without test
    retries or sleep-based synchronization.

## Open Questions

None at design approval.

Implementation must stop and surface any newly discovered production strong
holder that cannot fit the approved authority matrix. Such a finding is not
permission to reintroduce restore/retry silently or expand into the full
owned-runtime design.
