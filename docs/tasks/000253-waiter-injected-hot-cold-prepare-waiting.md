---
id: 000253
title: Add Waiter-Injected Prepare Waiting for Hot and Cold Rows
status: proposal  # proposal | implemented | superseded
created: 2026-08-03
github_issue: 934
---

# Task: Add Waiter-Injected Prepare Waiting for Hot and Cold Rows

## Summary

Replace eager prepare-notifier installation in `SharedTrxStatus` with a
waiter-injected notifier. An ordered transaction will publish prepare state
without locking or creating an event. The first actual waiter will install the
event under the existing per-transaction mutex, and prepare completion will
take and drop that event after publishing completion.

Use the same shared-status mechanism for existing hot-row waiting and new
prepare-aware cold-row ownership waiting. Foreground cold update, delete, and
full-table mutation paths will await a preparing owner only after releasing
short-lived ownership and storage guards, then retry from authoritative row
location and marker state. Ordinary active owners remain immediate write
conflicts, and maintenance callers remain non-waiting.

## Context

`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Source Backlogs:`
`- docs/backlogs/000168-add-cold-row-prepare-waiting.md`

`Related Tasks:`
`- docs/tasks/000242-enforce-terminal-transaction-lock-release-ordering.md`

Hot rows and cold rows already retain the same `Arc<SharedTrxStatus>` as their
write-owner identity. Hot `RowUndoHead` handling requests
`prepare_listener()`, releases row write access when registration succeeds,
waits, and retries. A
`ColumnDeletionBuffer` `DeleteMarker::Ref` distinguishes committed, same-owner,
and foreign-active status, but `put_ref()` currently maps every foreign active
owner directly to `DeletionError::WriteConflict`.

`SharedTrxStatus::mark_preparing()` currently acquires
`prepare_ev: Mutex<Option<EventNotifyOnDrop>>`, eagerly inserts a notifier, and
then publishes `preparing = true`. Every completion path acquires the same
mutex, publishes `preparing = false`, takes the notifier, and drops it to wake
listeners. This gives correct notification ordering, but every ordered
transaction pays prepare-side locking plus event notification work even when
no row waiter exists. The mutex is per transaction rather than global, but the
unconditional synchronization and lazy event-internal initialization still
add commit-path work.

The selected design keeps the simple mutex protocol and moves event insertion
to the first waiter. It relies on this invariant:

1. A waiter may install `prepare_ev` only after observing `preparing = true`
   while holding the mutex.
2. Every transition from preparing to finished holds the same mutex, stores
   `preparing = false`, and takes the event before releasing the mutex.
3. Therefore, a waiter that observes an existing event while holding the mutex
   does not need another `preparing` load: the finisher still owes that event a
   notification. Only the first waiter, which sees an empty slot, must recheck
   `preparing` under the mutex before insertion.

Task 000242 established terminal cleanup ordering. Successful
failed-precommit rollback removes transaction-owned cold markers before
releasing prepare waiters. Fatal rollback publishes engine poison and releases
prepare waiters while retaining unsafe undo state. This task must preserve
those boundaries; it does not authorize earlier logical-lock release.

Full-table mutation currently holds transaction-lifetime `TableData(X)`, so a
foreign foreground owner normally finishes before full-table scanning begins.
Prepare-aware handling is still required in its cold-row observer and
definitive claim paths so the row-concurrency contract remains complete and
does not become an obstacle to separately proven future lock-release work.

## Goals

1. Remove prepare-side mutex acquisition and eager prepare-event installation
   from ordered transactions with no waiters.
2. Make the first waiter install one shared `EventNotifyOnDrop`; let later
   waiters reuse it without a redundant under-lock state recheck.
3. Preserve lossless wakeup for successful commit, successful
   failed-precommit rollback, fatal cleanup, registration races, multiple
   waiters, and cancelled waiters.
4. Reuse the lazy `SharedTrxStatus` listener for both hot undo ownership and
   cold `ColumnDeletionBuffer` ownership.
5. Distinguish a preparing cold owner from a non-preparing active owner without
   changing committed-marker snapshot semantics or same-owner idempotence.
6. Await only in foreground async table access after short-lived CDB,
   row-location, index, LWC block, and page guards have been released.
7. Restart point update/delete from authoritative index lookup, row location,
   and CDB state after a wake rather than assuming commit or rollback.
8. Preserve full-table callback-at-most-once behavior while supporting waits
   both before callback selection and after a mutation has been staged.
9. Preserve recovery, purge, checkpoint, and page-transition no-wait behavior.
10. Provide deterministic race and integration tests plus structural and
    measured evidence for the uncontended prepare fast path.

## Non-Goals

1. Do not release transaction logical locks before redo durability or otherwise
   change task 000242 terminal ordering.
2. Do not optimize `terminal_ev`, terminal-operation observers, group commit,
   redo synchronization, or log-thread scheduling.
3. Do not introduce an atomic prepare-state machine, `OnceLock<Event>`, unsafe
   pointer state, or a general lock-free notification primitive.
4. Do not unify hot and cold storage ownership behind a new general
   `RowWriteClaim` trait or rewrite hot-row retry control flow.
5. Do not make recovery, purge, checkpoint, page transition, or replay wait on
   foreground transaction ownership.
6. Do not change CDB persistence, delete bitmaps, redo records, undo formats,
   recovery reconstruction, transaction timestamps, or snapshot rules.
7. Do not change public APIs or introduce a new public conflict/error kind.
8. Do not release transaction-lifetime table admission,
   `TransactionTableBinding`, `TrxAttachment`, or baseline pool guards while a
   foreground statement waits. The release requirement applies to
   operation-local executable handles and short-lived data guards.
9. Do not add fairness, queueing, timeout, deadlock detection, or starvation
   guarantees for a sequence of changing row owners.
10. Do not add a new benchmark command or make a broad transaction-throughput
    optimization program part of this task.

## Plan

### Lazy shared prepare notifier

Keep `SharedTrxStatus` represented by `preparing: AtomicBool` and
`prepare_ev: Mutex<Option<EventNotifyOnDrop>>`.

Change `mark_preparing()` to validate the one-shot lifecycle and publish
`preparing = true` without acquiring `prepare_ev` or installing an event. Keep
the existing ordering strength unless a weaker ordering is separately proven
against timestamp publication and every observer.

Change `prepare_listener()` to:

1. return `NotPreparing` on the fast-path load when the transaction is not
   preparing;
2. acquire `prepare_ev`;
3. if an event already exists, return `Registered(listener)` without reloading
   `preparing`;
4. if the slot is empty, recheck `preparing` while holding the mutex;
5. return `Completed` if completion won the race; otherwise insert
   `EventNotifyOnDrop`, derive its listener while still holding the mutex, and
   return `Registered(listener)`.

Keep `finish_preparing()` serialized by `prepare_ev`: store
`preparing = false`, take the optional event, release the mutex, and only then
drop the event. All successful commit, successful rollback, fatal-retention,
and test-only completion routes must use this function rather than directly
clearing the atomic. This rule makes an existing event under the mutex proof
that notification is still owed.

An abandoned listener may disappear, but its shared event remains in the slot
until prepare completion. Multiple listeners use the same event and wake
together. A late call returning `NotPreparing` never awaits; its caller
re-observes the owner state. `Completed` distinguishes the registration race
from an ordinary active owner without a redundant caller-side prepare load.

### Prepare-aware CDB claiming

Add a crate-private foreground result such as:

```rust
enum DeletionClaim {
    Acquired,
    Preparing(Option<EventListener>),
}
```

Add a foreground `ColumnDeletionBuffer::claim_ref()` operation while retaining
the existing `put_ref()` no-wait contract for maintenance and test callers.
Share marker classification internally without allowing the no-wait operation
to create an unused listener.

Under the CDB entry guard, the foreground operation must produce:

- vacant entry: insert the caller's `DeleteMarker::Ref` and return `Acquired`;
- the same status `Arc`: return `Acquired`;
- committed `Ref` or `Committed`: preserve the existing
  `AlreadyDeleted`/`WriteConflict` snapshot decision;
- foreign, active, non-preparing status: return `WriteConflict`;
- foreign preparing status: map `Registered(listener)` to
  `Preparing(Some(listener))`;
- completion during listener registration: reclassify a committed timestamp or
  return `Preparing(None)` for an immediate retry.

`Preparing(None)` means completion won listener registration and requires an
immediate full retry, not a write conflict. Revalidate the timestamp when
prepare state changes during classification so a newly committed status is
handled using committed-marker rules instead of a stale active observation.
The CDB entry guard must be destroyed when `claim_ref()` returns; only the
owned listener may cross into async control flow.

The lock order is CDB entry guard followed by the owner's prepare mutex.
Failed-precommit rollback already removes the CDB marker before
`finish_preparing()` acquires the prepare mutex, and successful completion does
not acquire a CDB entry while holding that mutex. Preserve this ordering and
add a deterministic regression proof so the lazy registration cannot create a
CDB/prepare-mutex cycle.

### Foreground wait boundary

Introduce one table-access helper or a small attempt result that centralizes
the prepare wait boundary:

- `None` skips the await because completion won registration;
- `Some(listener)` is awaited;
- after either outcome, check `rt.engine().poisoner.ensure_healthy()` before
  retrying or accessing table state again;
- return only after every operation-local CDB entry, index handle, row
  location, persisted LWC block, and page guard from the failed attempt has
  been dropped.

Do not carry borrowed table data through the wait merely to avoid a second
lookup. Baseline transaction attachment, table binding, metadata/layout
binding, and pool-role guards remain transaction-owned and are not evidence of
an await-holding-guard defect.

### Point cold update and delete

Extend cold update visibility observation (`ColdRowUpdateRead` and
`read_lwc_row_for_update()`) with a preparing-owner result. This is necessary
because update currently returns a conflict during its validation read before
it reaches the definitive CDB claim.

Restructure point unique update and delete attempts into scopes that yield
success, ordinary conflict/not-found, immediate retry, or an owned listener.
Before awaiting, leave the scope that owns the root snapshot's executable
index handle, index binding, page/block guard, decoded old row, and provisional
key set.

After wake, continue the existing outer operation loop. Reacquire the root
snapshot and index handle, look up the requested key again, resolve the current
row location, reread or revalidate the cold row, and repeat the definitive CDB
claim. Expected outcomes include:

- owner committed a delete: resolve not-found through normal marker/index
  state;
- owner committed an update: follow the current index mapping and row
  location rather than the old cold row;
- owner rolled back successfully: observe the removed marker and claim the
  restored cold row;
- owner failed fatally: return the normal engine-poison error;
- a new ordinary active owner won: return `WriteConflict`;
- this transaction already owns the marker: retain idempotent behavior.

Do not append row undo, redo, index undo, index masks, or replacement hot rows
until the definitive CDB claim succeeds.

### Full-table cold mutation

Make `read_latest_cold_row()` distinguish a preparing foreign status from an
ordinary active-owner conflict. A wait encountered before invoking the
full-table callback must release the persisted LWC block and its executable
column-index/storage guards, await, reload authoritative state for that
original row, and invoke the callback at most once if the row remains eligible.

Use an explicit per-entry cursor and owned pending-mutation state so rows whose
callbacks already ran are never sent through the callback again after a wait.
Previously staged mutations may be applied after releasing the persisted block
before waiting on the current pre-callback row; statement rollback remains
responsible for effects if a later operation fails.

`delete_known_cold_row()` and `update_known_cold_row()` run after the persisted
block is dropped. If their definitive claim encounters a preparing owner, keep
the already-owned staged callback output, await without rerunning the callback,
and revalidate the frozen root/row marker before retrying that same staged
action. A rollback may make the original row claimable. A committed or
otherwise changed row after callback selection remains a statement
`WriteConflict`, matching the existing "row changed after visibility"
contract.

Keep outcome counting and replacement-row scan boundaries consistent with
existing full-table semantics: a replacement hot row is not scanned as a new
original row, and waiting must not duplicate delete/update counts.

### Lifecycle, maintenance, and documentation

Preserve the ordering established by task 000242:

- successful commit publishes its CTS before prepare waiters retry;
- successful failed-precommit rollback removes CDB ownership before wake;
- fatal rollback publishes poison and releases waiters without exposing
  retained undo as safely reusable state.

Keep `ColumnDeletionBuffer::put_ref()` non-waiting for page transition, purge
tests, recovery setup, and other maintenance consumers. A maintenance
collision with a preparing owner remains its existing immediate conflict or
invariant failure; it must not inject and discard a listener.

Update `docs/transaction-system.md` and `docs/deletion-checkpoint.md` to
describe waiter-injected shared prepare notification, hot/cold foreground
waiting, authoritative retry, and the no-wait maintenance boundary. Do not
describe this work as sufficient authority for pre-durability logical-lock
release.

### Performance evidence

Add a structural unit assertion that an ordered status can enter and finish
prepare with `prepare_ev` remaining empty when no listener registers. Assert
that the first waiter creates the slot and later waiters reuse it.

Record a small paired release comparison using the existing `doradb-bench`
sequential insert workload with batch size one, fixed threads/sessions and
payload, and `log-sync=none`. This workload performs one ordered commit per
row and avoids durability-sync noise. Use equivalent fresh roots and report
the command/configuration plus median and range; do not add a benchmark
command or claim a broad throughput guarantee from this single workload. A
repeatable regression requires investigation even though the structural
fast-path proof remains authoritative.

## Risks and Mitigations

1. **Lost wakeup during first registration.** Recheck `preparing` only when the
   mutex-protected slot is empty; completion uses the same mutex to clear state
   and take the event.
2. **Incorrect inference from an existing event.** Require every false
   transition to take the event under the mutex, and cover all completion
   routes in tests.
3. **CDB/prepare lock inversion.** Preserve rollback's marker-removal-before-wake
   order and prove the competing registration/rollback interleaving with
   barriers rather than timing.
4. **Awaiting with storage guards.** Return owned attempt outcomes from lexical
   scopes, then await outside those scopes; add a test hook that can acquire
   the same CDB/storage resources while the foreground writer is suspended.
5. **Duplicate full-table callback effects.** Track original-row cursor and
   owned staged output independently from retry state; never reconstruct a
   staged action by calling user code again.
6. **Retrying stale cold identity.** Point paths restart index and row-location
   resolution; full-table paths revalidate their frozen original-row
   descriptor and marker before consuming staged output.
7. **Fatal cleanup mistaken for rollback.** Check engine poison after wake
   before treating marker state as reusable.
8. **Noisy performance inference.** Pair equivalent release runs and retain
   structural no-event evidence as the primary acceptance condition.

## Implementation Notes

## Impacts

- `doradb-storage/src/trx/mod.rs`
  - `SharedTrxStatus::{mark_preparing, prepare_listener, finish_preparing}`
  - prepare listener tests and narrow test-controlled status helpers
- `doradb-storage/src/trx/undo/row.rs`
  - shared-status observation used by hot `RowUndoHead` and cold rollback
    marker-removal ordering
- `doradb-storage/src/table/hot.rs`
  - regression coverage for existing hot prepare wait/retry behavior
- `doradb-storage/src/table/deletion_buffer.rs`
  - foreground prepare-aware claim result and no-wait `put_ref()` boundary
- `doradb-storage/src/table/access.rs`
  - cold update visibility, point update/delete retry scopes, full-table cold
    cursor/staging, poison check, and guard-free await boundary
- `doradb-storage/src/table/page_transition.rs`
  - confirmation that transition marker installation remains non-waiting
- `docs/transaction-system.md`
  - shared prepare notification and foreground retry contract
- `docs/deletion-checkpoint.md`
  - CDB ownership waiting and maintenance boundary

No public type, public method, persistent record, recovery format, or error
taxonomy changes.

## Test Cases

### Shared status

1. Enter and finish prepare without listeners; assert `prepare_ev` stays empty.
2. Use a test-only barrier between a waiter's fast load and mutex acquisition,
   let completion finish, and then release the waiter; assert the empty-slot
   recheck returns `None` and no waiter blocks.
3. Let the first waiter win the mutex, install its listener, then complete;
   assert it wakes and sees the published commit result.
4. Register later waiters against the existing event; assert all wake and the
   existing-slot branch needs no state recheck for correctness.
5. Drop one or all listeners before completion; assert no leak, panic, or
   retained notifier after finish.
6. Repeat successful failed-precommit rollback and fatal prepare-release paths;
   assert late registration returns `None`.

### CDB classification

7. Preserve vacant acquisition, same-status idempotence, committed-ref
   compaction semantics, and snapshot-relative committed outcomes.
8. A foreign non-preparing active owner returns immediate `WriteConflict` and
   does not create `prepare_ev`.
9. A foreign preparing owner returns one listener; multiple claimers share the
   same owner event.
10. Completion racing first CDB registration yields either a valid listener or
    immediate retry, never an unnotified listener.
11. Successful rollback cannot deadlock against a claimant holding the CDB
    entry while registering; marker removal precedes the wake.
12. No-wait `put_ref()` callers retain their previous conflict behavior and do
    not install prepare events.

### Foreground behavior

13. Checkpoint a row cold, prepare an owning delete, and issue a competing
    point delete. It waits; commit wakes it into the normal not-found result.
14. Repeat with owner rollback. The waiter wakes, re-resolves the row, acquires
    the marker, and can commit its delete.
15. Prepare a cold-row update that retains or changes a unique key. A competing
    update wakes after commit and follows the current index mapping rather than
    mutating the stale cold row.
16. Cover the prepare wait encountered during update visibility read as well
    as during the definitive claim race.
17. Force fatal failed-precommit cleanup with existing deterministic rollback
    hooks. The waiter wakes and returns the engine-poison error without
    claiming retained state.
18. Cancel a waiting cold operation. The owner can still commit or roll back,
    subsequent waiters make progress, and no marker/listener ownership leaks.
19. At the suspended wait boundary, synchronously prove another test actor can
    acquire the same CDB entry and relevant operation-local storage/index
    resources; do not use elapsed time as the readiness predicate.

### Full-table behavior

20. A preparing owner observed before callback execution wakes into commit
    exclusion or rollback eligibility, and the callback runs zero or one time
    for that original row.
21. A preparing owner encountered after callback staging does not cause a
    second callback invocation. Rollback permits the staged action; commit or
    changed identity returns the established statement conflict.
22. Waiting does not rescan replacement hot rows, duplicate mutation counts,
    or alter the frozen original-row boundary.
23. Ordinary active conflict and same-transaction ownership retain their
    existing full-table outcomes.

### Validation

24. Run focused affected tests repeatedly with
    `rtk cargo nextest run -p doradb-storage --stress-count 100 <filter>`;
    synchronization predicates and barriers, not sleeps or retries, must drive
    the race.
25. Run `rtk cargo fmt`, `tools/style_audit.rs`,
    `rtk cargo clippy --workspace --all-targets -- -D warnings`, and
    `rtk cargo nextest run --workspace`.
26. Run alternate-backend strict clippy and tests:
    `rtk cargo clippy -p doradb-storage --no-default-features --features libaio --all-targets -- -D warnings`
    and
    `rtk cargo nextest run -p doradb-storage --no-default-features --features libaio`.
27. Record the configured paired release `insert-seq` batch-size-one result and
    verify the candidate shows no repeatable regression.

## Open Questions

None for implementation.

If measured evidence later shows the remaining uncontended
`finish_preparing()` mutex acquisition is material, evaluate a separate
atomic-state/lazy-event design with an explicit registration/completion
linearization proof. Optimizing `terminal_ev` would likewise be separate work;
neither follow-up is required or authorized by this task.
