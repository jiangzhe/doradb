---
id: 000253
title: Add Waiter-Injected Prepare Waiting for Hot and Cold Rows
status: implemented  # proposal | implemented | superseded
created: 2026-08-03
github_issue: 934
---

# Task: Add Waiter-Injected Prepare Waiting for Hot and Cold Rows

## Summary

Transaction prepare notification now installs an event only when the first
actual waiter arrives. Ordered transactions with no waiters publish prepare
state without taking the prepare-event mutex or allocating an event, while
commit, rollback, registration races, multiple waiters, and cancelled waiters
retain lossless completion notification.

The same shared-status protocol now supports foreground cold-row ownership.
Point update/delete and full-table mutation wait for a foreign preparing
`ColumnDeletionBuffer` owner only after releasing operation-local guards, check
engine health after prepare completion, and retry authoritative row and marker
state. Ordinary active owners remain immediate write conflicts, and
maintenance callers remain non-waiting.

## Context

`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Source Backlogs:`
`- docs/backlogs/closed/000168-add-cold-row-prepare-waiting.md`

`Related Tasks:`
`- docs/tasks/000242-enforce-terminal-transaction-lock-release-ordering.md`

`Follow-up Backlogs:`
`- docs/backlogs/000177-propagate-engine-poison-through-hot-row-prepare-waiting.md`

Hot and cold write ownership both retain `Arc<SharedTrxStatus>`, but only hot
undo ownership previously observed prepare state and waited for completion.
Cold deletion markers mapped every foreign active owner directly to
`WriteConflict`, even when the owner had already entered ordered commit.

Task 000242 established the lifecycle boundary this work preserves:
successful rollback removes owned cold markers before releasing prepare
waiters, while fatal rollback publishes engine poison before releasing waiters
and retains unsafe undo state. Prepare-aware row waiting does not authorize
earlier logical-lock release or weaken terminal cleanup ordering.

The durable synchronization invariant is:

1. A waiter installs a prepare event only while holding the per-transaction
   prepare mutex and after confirming prepare is still active.
2. Prepare completion holds the same mutex, publishes the finished state,
   takes the event, and drops it after releasing the mutex.
3. An event already present under the mutex proves completion still owes its
   notification, so later waiters do not reread prepare state.

## Goals

1. Remove prepare-side mutex and event work from ordered transactions with no
   waiters.
2. Let the first waiter install one shared event and later waiters reuse it.
3. Preserve lossless wakeup across commit, rollback, fatal cleanup,
   registration races, multiple waiters, and cancelled listeners.
4. Add prepare-aware foreground cold-row ownership without changing committed
   marker visibility or same-owner idempotence.
5. Release operation-local CDB, index, row-location, LWC-block, and page guards
   before awaiting.
6. Retry point mutations from authoritative index, location, and marker state.
7. Preserve callback-at-most-once and original-row scan boundaries for
   full-table mutation.
8. Keep recovery, purge, checkpoint, transition, and other maintenance paths
   non-waiting.

## Non-Goals

1. No change to terminal logical-lock release ordering, redo durability, or
   failed-precommit ownership cleanup.
2. No general lock-free notification primitive or atomic prepare state
   machine.
3. No general hot/cold row-write claim abstraction or hot-row retry rewrite.
4. No waiting in recovery, purge, checkpoint, transition, or replay.
5. No persistent format, redo/undo layout, timestamp, snapshot, or public error
   taxonomy change.
6. No release of transaction-lifetime admission, attachment, table binding, or
   baseline pool guards while a statement waits.
7. No fairness, timeout, deadlock detection, or starvation guarantee across a
   changing sequence of owners.
8. No new benchmark command or broad transaction-throughput claim.

## Plan

### Shared prepare protocol

`SharedTrxStatus` retains an atomic prepare flag and a mutex-protected optional
event. `mark_preparing()` validates the one-shot lifecycle and publishes the
flag without locking or creating an event.

`prepare_listener()` returns one of three explicit outcomes:

- `NotPreparing` when its fast load sees no active prepare;
- `Registered(listener)` when it reuses an existing event or installs the
  first event after an under-lock state recheck;
- `Completed` when completion wins first-listener registration.

`finish_preparing()` is the only completion transition. Under the prepare
mutex, it publishes `preparing = false` and takes the event; it drops the event
after unlocking so all registered listeners wake. Successful precommit,
successful failed-precommit rollback, fatal retention, and test completion all
use this transition.

`TrxContext::status()` and `TrxRuntime::status()` return `&Arc<SharedTrxStatus>`.
Callers clone explicitly only when ownership crosses into a marker or other
stored state; pointer comparisons and read-only consumers avoid internal Arc
clones.

### Cold ownership classification

`ColumnDeletionBuffer::claim_ref()` is the foreground ownership operation.
Vacant and same-owner entries acquire successfully. Committed markers retain
their snapshot-relative `AlreadyDeleted` or `WriteConflict` classification.
Foreign ordinary active owners conflict immediately.

A foreign preparing owner maps `Registered` to
`DeletionClaim::Preparing(Some(listener))`. If completion wins registration,
the claim reclassifies the timestamp: a newly committed owner follows committed
marker rules, while rollback returns `Preparing(None)` for an immediate retry.

`put_ref()` remains the no-wait maintenance operation and shares marker
classification without creating a discarded listener. The lock order remains
CDB entry then owner prepare mutex; rollback removes the marker before taking
the prepare mutex to finish.

### Foreground retry boundaries

`UserTableAccessor::wait_prepare_retry()` awaits an optional listener and then
checks `EnginePoisoner`. The health check runs after both a real wake and the
completion-won-registration path, before retained unsafe state can be touched.

Point update and delete use an owned attempt result. All executable index
handles, row locations, decoded cold data, CDB entries, and page/block guards
leave scope before waiting. A retry reacquires the root snapshot and index
handle, repeats lookup and location resolution, rereads cold visibility, and
performs the definitive CDB claim before adding effects.

Full-table mutation separates cold and hot scans and carries scan boundaries,
reusable values, and outcomes in `TableMutationState`. Cold rows use an
explicit cursor and owned staged mutations. A prepare interruption is
represented locally as `Option<Option<EventListener>>`:

- outer `None`: the block scan completed without interruption;
- `Some(None)`: prepare completion won listener registration;
- `Some(Some(listener))`: await the registered listener.

Callbacks already executed are never repeated. Staged actions are applied
after the persisted block is released, and a staged delete or update retries
only its definitive ownership claim. `claim_known_cold_row()` centralizes that
claim loop with `OperationOrFatalResult`, preserving typed conflicts and poison
until the caller adds delete/update context and discloses at its public result
boundary.

### Lifecycle and documentation

Successful commit publishes its CTS before waiters retry. Successful rollback
removes CDB ownership before wake. Fatal rollback publishes poison, wakes
waiters, and retains unsafe state. Documentation records both the shared
prepare protocol and the boundary between waiting foreground operations and
non-waiting maintenance consumers.

## Implementation Notes

Implemented waiter-injected shared prepare notification and prepare-aware
foreground cold-row retry across point and full-table mutations. The source
backlog contract is complete, and no public API or persistent format changed.

Review produced several material refinements:

- `PrepareListenerResult` replaced separate prepare reads with one
  authoritative registration outcome, eliminating duplicate `preparing()`
  observations in hot undo, CDB, deletion-buffer cleanup, and table access.
- Transaction status accessors now borrow their status Arc; only ownership
  consumers clone it.
- Full-table mutation was split into cold/hot helpers, then adopted the nested
  option state above instead of inferring prepare interruption from its cursor.
- The common known-cold delete/update claim loop was extracted with the
  existing typed Operation-or-Fatal carrier.
- Cold paths check poison after registered wake and completion races. Extending
  the same fatal propagation through the shared hot mutator requires a broader
  error-contract change and is deferred to backlog 000177.

`docs/transaction-system.md` and `docs/deletion-checkpoint.md` now describe lazy
listener injection, authoritative cold retry, poison precedence, and the
foreground/maintenance boundary. The public-error audit was refreshed for the
new helper boundaries.

Final verification completed:

- resolve-time style audit passed all 6 branch-diff Rust files;
- default strict clippy passed and 1,644 workspace tests passed;
- alternate `libaio` strict clippy passed and 1,548 storage tests passed;
- the focused prepare/CDB/point/full-table concurrency suite passed 100 stress
  iterations;
- formatting and diff whitespace checks passed.

Five alternating optimized `insert-seq` samples per side used fresh roots,
100,000 rows, one thread/session, 128-byte values, batch size one, no index,
seed zero, and `log-sync=none`. `origin/main` had a median of 41,425.588 ns/op
(range 36,301.292-42,853.205); the candidate median was 40,149.986 ns/op
(range 36,360.603-41,937.376). The candidate median was 3.1% lower and the
ranges overlapped, so no repeatable regression was observed. The structural
no-listener test remains the primary fast-path proof.

## Impacts

- Transaction shared status now performs lazy prepare-event installation and
  exposes explicit registration outcomes.
- Hot undo consumers use the shared outcome protocol; their existing
  successful commit/rollback waiting behavior remains intact.
- Column deletion ownership has separate foreground wait-aware and maintenance
  no-wait APIs.
- User-table point and full-table cold mutations now wait and authoritatively
  retry preparing owners.
- Transaction, deletion/checkpoint, and public-error audit documentation match
  the implemented boundaries.
- There are no public API, schema, on-disk, redo/undo, recovery, snapshot, or
  operational configuration changes.

## Test Cases

1. Prepare with no listeners leaves the event slot empty.
2. First and later waiters share one event and wake after commit.
3. Completion winning first registration returns `Completed` without a lost
   wakeup.
4. Cancelled listeners do not leak or prevent completion.
5. Failed-precommit rollback wakes listeners after marker removal.
6. Foreground CDB claims wait for preparing owners; ordinary active owners
   still conflict.
7. No-wait CDB claims do not inject prepare events.
8. Deterministic CDB registration versus rollback proves the lock order does
   not deadlock.
9. Cold point update retries successfully after owner rollback.
10. Cold point delete wakes into not-found after owner commit.
11. Full-table waiting before callback reloads the row and invokes the callback
    at most once.
12. Full-table waiting after staging preserves the owned callback result and
    does not duplicate counts or scan replacement rows.
13. Fatal completion is checked before cold retry touches retained state.
14. Default and `libaio` suites preserve existing transaction, table,
    checkpoint, purge, recovery, and benchmark behavior.

## Open Questions

Hot-row prepare retry still masks fatal engine poison as an ordinary write
conflict because the shared hot mutator carries Operation-only errors across
user-table, catalog, and standalone `MemTable` callers. The typed propagation
design and both registration outcomes are tracked in
`docs/backlogs/000177-propagate-engine-poison-through-hot-row-prepare-waiting.md`.
