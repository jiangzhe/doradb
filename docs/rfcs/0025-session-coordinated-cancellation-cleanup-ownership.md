---
id: 0025
title: Session-Coordinated Cancellation and Cleanup Ownership
status: accepted
tags: [storage-engine, session, cancellation, cleanup, lifecycle]
created: 2026-07-28
github_issue: 908
---

# RFC-0025: Session-Coordinated Cancellation and Cleanup Ownership

## Summary

Adopt an engine-owned session-operation coordinator that gives every enclosing
effectful session operation one stable lifecycle entry and one terminal owner.
Cancellation semantics depend on operation kind. A cancelled public statement
synchronously folds its remaining row and index undo into the checked-out
transaction, discards redo, and releases statement locks; the existing cleanup
worker therefore claims only ordinary transaction rollback, not a separate
statement rollback phase. DDL and mutating maintenance instead run as one
`Pin<Box<dyn Future + Send + 'static>>`: the caller's executor polls the whole
operation on the normal path, and dropping the public observer transfers that
same pinned future to the existing cleanup worker rather than cancelling it.
The worker uses one engine-owned `async-executor` instance to drive transferred
operations and transaction cleanup jobs cooperatively on the existing physical
thread. Standalone progress waits remain ordinarily drop-cancellable. Session
admission remains closed until transactions, mandatory operations, operation
locks, and optional session locks have reached an ordered idle, closed, or
failed-retained outcome. Coordinator and DDL/maintenance lock identity use one
session-local `(SessionID, OperationID)` key; operation purpose remains stable
metadata rather than a second identity type.

## Context

RFC-0019 moved sessions and transactions behind engine-owned stable registries,
made public handles weak capabilities, and gave abandoned transactions a
state-authorized cleanup claim. That work made transaction lifecycle visible to
shutdown, but `SessionState` still models only idle and active-transaction
states. A DDL or maintenance future can acquire exact operation-scope locks,
start a private transaction, and retain phase progress while the enclosing
session operation itself has no stable entry. [D4] [C1] [C2]

This gap creates multiple cancellation owners. Dropping a nested DDL future can
queue transaction rollback while DDL guards synchronously release operation
locks. Dropping a polled `Transaction::exec` future can synchronously drop
statement-local effects, which the current `StmtEffects` guard turns into a
panic instead of an ownership handoff. Maintenance has additional progress
objects whose behavior changes after table lifecycle, catalog commit, root
publication, or system-transaction gates. [D2] [D3] [C3] [C5] [C6] [C7] [C8]

Backlog 000169 and task 000243 established exact DDL and maintenance operation
lock identities, but deliberately deferred cancellation coordination. Their
engine-global typed ids and per-`ScopedTableRuntimeAccess` maintenance boundary
were suitable before a session operation entry existed; this RFC replaces
those identity choices with the entry's session-local identity while preserving
exact-owner isolation. Backlog 000171 requires one serialized lock-family
mutation owner and cannot safely remove the current manager's
concurrent-release defenses until the outer session lifecycle proves that
pending acquisition, nested transaction cleanup, and operation-scope cleanup
cannot overlap. [D3] [D13] [B1] [B3] [B6] [U3]

The selected direction keeps Doradb's foreground memory-first execution model.
It extends the stable-entry and claim pattern already used by `TrxEntry`, boxes
only cold-path DDL and mutating-maintenance operations, and adds cooperative
concurrency inside the existing cleanup worker. It does not introduce a session
actor, a second physical worker, or a physical lock-manager redesign. [D1]
[D4] [D15] [U1] [U2] [U7]

### Goals

1. Make `SessionState` authoritative for the complete lifecycle of one
   enclosing session operation, including public transactions, nested DDL
   transactions, statements, maintenance, close, abandonment, and shutdown.
   [B1] [U1]
2. Give blind future drop explicit operation-kind semantics: synchronously
   settle and terminally cancel a checked-out public statement/transaction, but
   detach the observer and transfer the exact whole-operation future for DDL
   and mutating maintenance. Neither path performs async destructor work or
   loses owned state. [D4] [B1] [B2] [U6] [U7]
3. Allow exactly one non-cloneable owner to poll, roll back, complete, or
   fatally retain an operation at a time. A foreground-to-worker transfer moves
   the same pinned future and cannot reconstruct it from phase diagnostics or
   identifiers. [C2] [C4] [B1] [U7]
4. Preserve proof-bound release order from pending acquisition through
   statement and transaction cleanup, operation-scope release, optional
   `SessionExplicit` release, and final session publication. [D2] [D3] [B1]
5. Generalize the existing transaction cleanup queue and physical worker with
   one lightweight cooperative executor, so independently pending mandatory
   operations and transaction cleanup jobs can make progress without scheduling
   successful foreground DDL/maintenance unnecessarily or changing normal
   row/index hot loops. [D4] [D15] [C4] [B1] [B4] [U7]
6. Give explicit close, public handle drop, worker failure, and synchronous
   engine shutdown deterministic behavior for every operation state. [D4] [D6]
7. Use one session-local `OperationID` domain for coordinator identity and
   DDL/maintenance lock ownership, without adding or retaining an engine-global
   operation-id allocator. [D13] [C1] [C13] [C15] [U3]
8. Preserve successful transaction and statement path cost: synchronize only at
   an ownership boundary that requires shared visibility, and add no second
   lookup, allocation, lock, atomic operation, notification, or queue hop around
   existing transaction checkout/check-in. [D14] [C1] [C2] [C16] [U5]

### Non-goals

- Do not redesign physical lock resources, waiter nodes, grant tokens,
  duplicate-waiter policy, or `LockScopeState`; backlog 000171 owns that work.
  [D3] [B3]
- Do not add parallel execution within one session or make public `Session`
  shareable. [D3] [D4]
- Do not add a second physical cleanup worker, adaptive worker pool, global
  executor, or general background runtime. One worker-local
  `async_executor::Executor` for mandatory cleanup and transferred operations is
  explicitly in scope. [D15] [B1] [B4] [U7]
- Do not change MVCC, redo, group-commit, checkpoint, or recovery semantics
  except where ownership must move before an existing terminal or irreversible
  boundary. [D1] [D2] [D5] [D9]
- Do not replace `TrxID` with `OperationID`. `TrxID` remains the engine-wide
  transaction timestamp/identity used by MVCC, transaction locks, statement
  locks, commit, and recovery. [D2] [C2] [C4] [C15]
- Do not introduce a public operation-cancellation or finish-cancellation API.
  Explicit client-side cancellation is deferred; blind future drop must not be
  treated as a cancellation request for DDL or mutating maintenance. [B1] [U2]
  [U7]
- Do not change `Engine::shutdown` into an async API. [D4] [B5]

`Issue Labels:`
`- type:epic`
`- priority:medium`
`- codex`

## Design Inputs

### Documents

- [D1] `docs/architecture.md` - engine ownership, transaction boundaries, and
  foreground storage architecture.
- [D2] `docs/transaction-system.md` - statement effects, stable transaction
  entries, terminal claims, commit ownership, rollback ordering, and session
  completion.
- [D3] `docs/lock-system.md` - exact session/transaction/statement/DDL/
  maintenance scopes, serialized family ownership, and the nested DDL
  cancellation prerequisite.
- [D4] `docs/rfcs/0019-weak-public-runtime-handles.md` - registry authority,
  weak public handles, operation leases, idempotent cleanup hints, non-blocking
  drop, and synchronous shutdown contracts.
- [D5] `docs/table-file.md` - staged table-file construction, durable root
  publication, readiness, and retained-root lifecycles.
- [D6] `docs/engine-component-lifetime.md` - operation/runtime pins, shutdown
  admission, worker ordering, and component teardown.
- [D7] `docs/process/unit-test.md` - authoritative nextest workflow and
  deterministic concurrency-test requirements.
- [D8] `docs/process/issue-tracking.md` - RFC-scale change and phased
  task/issue planning requirements.
- [D9] `docs/checkpoint-and-recovery.md` - checkpoint and recovery ownership
  boundaries.
- [D10] `docs/index-design.md` - hot/cold index ownership and checkpoint/GC
  context.
- [D11] `docs/tasks/000174-transaction-terminal-rollback-cancellation-safety.md`
  - existing worker-owned rollback handoff.
- [D12] `docs/tasks/000242-enforce-terminal-transaction-lock-release-ordering.md`
  - existing `ReleasedTransactionLocks` proof contract.
- [D13] `docs/tasks/000243-separate-session-operation-lock-scopes.md` - exact
  DDL and maintenance operation owner implementation, including the
  engine-global typed-id allocator and per-scoped-access maintenance boundary
  that this RFC revises.
- [D14] `docs/process/coding-guidance.md` - reliability-first design,
  performance as the next engineering priority, minimized overhead, and
  blocking mutexes restricted to very small and fast transitions.
- [D15] `async-executor` 1.13.3 and `async-task` 4.7.1 API documentation -
  `Executor::spawn`/`run`/`is_empty`, `Send` task requirements, cooperative task
  polling, and the distinction between cancelling a dropped `Task` handle and
  explicitly detaching it.
- [D16] `docs/tasks/000244-add-rfc-0025-benchmark-workloads.md` - program
  prerequisite benchmark workloads for statement/transaction lifecycle,
  bounded materialized and caller-driven index scans, and successful
  table/index DDL.

### Code References

- [C1] `doradb-storage/src/session.rs` - public session admission, DDL and
  maintenance scope guards, transaction-only `SessionLifecycle`, close,
  abandonment, and shutdown collection.
- [C2] `doradb-storage/src/trx/mod.rs` - `Transaction::exec`, `TrxEntry`,
  `TrxCheckout`, `TrxCompletionClaim`, transaction-handle drop, and
  `ReleasedTransactionLocks`.
- [C3] `doradb-storage/src/trx/stmt.rs` - statement-local row undo, index undo,
  redo, statement locks, rollback order, fatal retention, and the non-empty
  `StmtEffects` drop assertion.
- [C4] `doradb-storage/src/trx/sys.rs` - the single cleanup queue/worker,
  terminal and abandoned rollback, failed-precommit retention, queue shutdown,
  and rollback completion ordering.
- [C5] `doradb-storage/src/lock/mod.rs` - fresh lock guards, exact-owner release,
  and synchronous `WaiterGuard` cancellation.
- [C6] `doradb-storage/src/catalog/table.rs` - create-table progress, nested
  catalog transaction compensation, table installation, and irreversible
  drop-table lifecycle.
- [C7] `doradb-storage/src/catalog/index.rs` - create/drop index phase progress,
  staged runtime/layout cleanup, catalog commit, root publication, and install.
- [C8] `doradb-storage/src/table/persistence.rs` - checkpoint attempts,
  publication admission, irreversible table-root and silent-watermark work,
  system transactions, and drop-time poison behavior.
- [C9] `doradb-storage/src/table/checkpoint_workflow.rs` - reversible
  freeze/checkpoint attempts and publication/transition state.
- [C10] `doradb-storage/src/table/gc.rs` - maintenance-scoped table access and
  private transaction cleanup.
- [C11] `doradb-storage/src/catalog/storage/mod.rs` - prepared catalog
  checkpoint root publication.
- [C12] `doradb-storage/src/trx/retention.rs` - catalog checkpoint and redo
  retention marker/root publication and post-publication cleanup.
- [C13] `doradb-storage/src/engine.rs` - admission closure, runtime drain,
  engine-global DDL/maintenance id allocation, transaction-only shutdown
  cleanup scanning, and worker teardown.
- [C14] `doradb-storage/src/error.rs` - lifecycle error vocabulary including
  `ExistingTransaction`, `SessionUnavailable`, and `TransactionDiscarded`,
  which this RFC extends with `ExistingOperation`.
- [C15] `doradb-storage/src/id.rs` - distinct `SessionID`, `TrxID`,
  `DdlOperationID`, and `MaintenanceOperationID` definitions.
- [C16] `doradb-bench/src/runner.rs` - transaction batching, statement-per-key
  insert and lookup paths, table/index scans, isolated statement/transaction
  lifecycle, caller-driven index streams, successful DDL cycles,
  thread/session scaling, and `LogSync::None` support for isolating coordinator
  overhead.
- [C17] `doradb-storage/src/trx/undo/{index,row}.rs` and
  `doradb-storage/src/table/rollback.rs` - cancellation-safe normal rollback
  loops that keep the last undo entry buffer-owned across every await and remove
  it only after successful rollback.
- [C18] workspace/storage `Cargo.toml`, `Cargo.lock`, and
  `doradb-storage/src/runtime.rs` - the production `futures` blocking executor,
  test-only `smol` dependency, and already locked `async-executor` 1.13.3
  transitive version that becomes a direct production dependency.
- [C19] async test hooks in `doradb-storage/src/session.rs`,
  `doradb-storage/src/table/persistence.rs`, and
  `doradb-storage/src/table/gc.rs` - current thread-local and non-`Send` hook
  futures that must become worker-migration-safe for affected operations.

### Conversation References

- [U1] User requested session-coordinated cancellation and cleanup ownership,
  based on backlog 000170, with cancellation and cleanup unified by defining and
  maintaining session state.
- [U2] On 2026-07-28, the user approved the original-requirement-fit proposal:
  stable session operation entries, foreground leases, the existing cleanup
  worker, whole-transaction cancellation after a polled statement is dropped,
  close waiting only after terminal cleanup ownership, and explicit continuation
  authority before irreversible DDL or maintenance gates. The DDL/maintenance
  continuation representation is later superseded by [U7].
- [U3] On 2026-07-28, the user requested one session-local monotonic
  `OperationID` sequence for DDL, maintenance, and transactions, with identity
  represented as `(SessionID, OperationID)` and DDL/maintenance lock scopes
  collapsed to `Operation(OperationID)` because `LockOwner` already carries
  `LockFamily(SessionID)`.
- [U4] On 2026-07-28, the user clarified that an internal DDL or maintenance
  transaction inherits the enclosing `(SessionID, OperationID)` rather than
  allocating another operation id. The nested-identity decision remains
  normative; [U7] later supersedes extracting DDL/maintenance phase state into
  an outer checked-in payload.
- [U5] On 2026-07-28, the user required performance to remain a normative design
  constraint: the successful path must not pay unnecessary overhead and shared
  synchronization occurs only where ownership transfer requires it.
- [U6] On 2026-07-28, the user selected synchronous statement-cancellation
  settlement: move remaining statement row/index undo into transaction undo,
  discard statement redo, release statement locks under first-touch
  transaction-lock coverage, and let ordinary transaction rollback finish the
  work. Normal statement rollback keeps each last undo entry buffer-owned
  across awaits and pops it only after success, so cancellation needs neither a
  statement rollback phase enum nor a worker-owned statement payload.
- [U7] On 2026-07-29, the user selected whole-operation pinned futures for DDL
  and mutating maintenance. The caller drives the exact
  `Pin<Box<dyn Future + Send + 'static>>` until completion or observer drop;
  Drop transfers it to the existing cleanup worker, where a lightweight
  `async-executor` drives multiple mandatory tasks cooperatively. Blind future
  drop is not cancellation, explicit client-side cancellation is deferred, the
  task itself owns `SessionPin`/`EngineRef`, and standalone observer waits remain
  ordinarily drop-cancellable.

### Source Backlogs

- [B1] `docs/backlogs/000170-session-coordinated-cancellation-cleanup.md`
- [B2] `docs/backlogs/000124-statement-execution-cancellation-safety.md`
- [B3] `docs/backlogs/000171-exact-family-lock-system-redesign.md`
- [B4] `docs/backlogs/000123-adaptive-background-worker-runtime.md`
- [B5] `docs/backlogs/000114-evaluate-async-engine-shutdown-api.md`
- [B6] `docs/backlogs/closed/000169-separate-session-operation-lock-scopes.md`

## Decision

### Session State Is The Enclosing Authority

`SessionState` will store the authoritative disposition and at most one active
`SessionOperationEntry`. The lifecycle mutex stores only small state and an
entry reference; it does not store a checked-out mutable operation payload and
is never held across `.await`. The entry follows the existing `TrxEntry`
pattern: a short mutex protects payload ownership transfer, a registry-visible
state identifies the current owner, and an event/epoch wakes close and shutdown
waiters. [D4] [C1] [C2] [U1]

The conceptual outer state is:

```text
SessionDisposition =
    Open
  | CloseRequested
  | Abandoned

SessionOperationSlot =
    Idle
  | Active(Arc<SessionOperationEntry>)
  | Closed
  | ClosedFailedRetained(RetentionID)
```

Disposition is orthogonal to active work. Dropping a public session changes
`Open` to `Abandoned` without stealing a still-live detached transaction.
Explicit close may change it to `CloseRequested` after transaction cleanup, a
background whole-operation task, or a terminal subsystem already owns the
active operation. The final operation owner decides whether completion
publishes `Idle`, retains `SessionExplicit` locks, or publishes a closed state
and releases them. [D4] [C1] [U2] [U7]

The stable entry never stores a DDL/maintenance future. That exact pinned task
is owned by exactly one of the foreground wrapper, the mandatory handoff queue,
or the cleanup executor. It may own `SessionPin` and `EngineRef` while active,
so shutdown observes it as a runtime pin and keeps the worker alive until the
task terminates. Transaction cleanup payloads and failed-retention identifiers
that are checked into an engine-reachable entry still must not retain a strong
`EngineRef`. This separates an intentionally active runtime pin from a durable
registry-to-engine ownership cycle and preserves RFC-0019's weak-public-handle
contract. [D4] [C2] [C13] [U7]

### Unified Session-Local Operation Identity

`OperationID` is a crate-private `u64` newtype whose value is meaningful only
within one `SessionID`. The canonical coordinator key is:

```text
SessionOperationKey = (SessionID, OperationID)
```

`SessionState` owns a plain next-operation counter initialized to one. Reserving
an enclosing operation increments it while the lifecycle mutex is already held.
Failed reservations may consume values. Values never deliberately repeat, zero
is not allocated, and exhaustion must fail closed rather than wrap. The pair is
a volatile engine-lifetime identity and is not serialized. `SessionID` is
already engine-lifetime unique, so equal raw `OperationID` values in different
sessions do not alias. [D4] [D13] [C1] [C13] [C15] [U3] [U4]

Every reserved outer operation gets one `OperationID`. Every public or private
transaction is either itself that outer operation or is nested inside it. A
public transaction uses its enclosing operation id. A private DDL or maintenance
transaction inherits the already-reserved outer key as coordination context and
allocates only its engine-wide `TrxID`; it does not increment the operation
counter. Statements remain transaction-local children identified by
`(TrxID, StmtNo)` and do not consume `OperationID`. Truly sessionless system
transactions have no `SessionID` and therefore remain outside this key domain;
when such work is spawned by a session operation, its ownership and terminal
proof remain in the outer whole-operation future. [D2] [C1] [C2] [C4] [U3]
[U4] [U7]

Identity and purpose are separate. `SessionOperationEntry` stores its
`OperationID` and stable `SessionOperationKind`; nested transaction records
store their `TrxID` and refer to the outer key. Transaction cleanup hints carry
the outer `(SessionID, OperationID)` plus a reason, and the registry verifies
that key before constructing a claim, so stale hints are neutral. A mandatory
DDL/maintenance message instead carries the exact pinned task plus its key; the
worker verifies `BackgroundQueued` before starting it. A nested transaction
never independently publishes session state or replaces the outer key. The
kind—not the numeric id—selects DDL, maintenance, transaction, or explicit-lock
policy and diagnostics. [C1] [C4] [U3] [U4] [U7]

The identities have these non-interchangeable roles:

| Identity | Domain and role | Lock representation |
| --- | --- | --- |
| `SessionID` | Engine-local session registry identity and lock family | `LockFamily(SessionID)` |
| `OperationID` | Session-local coordinator identity for one enclosing operation | `Operation(OperationID)` for DDL/maintenance only |
| `TrxID` | Engine-wide MVCC timestamp and transaction identity | `Transaction(TrxID)` and `Statement(TrxID, StmtNo)` |
| `StmtNo` | Transaction-local statement sequence | Child of `TrxID` |

A public transaction consequently has both `(SessionID, OperationID)` for
session coordination and `TrxID` for MVCC/transaction semantics. Neither can
replace the other: a session-local id is not a globally ordered timestamp, and
allocating `TrxID` for non-transaction DDL or maintenance would incorrectly
couple generic lifecycle work to the transaction timestamp domain. There is no
numeric conversion or ordering relationship between the two. [D2] [C2] [C4]
[C15] [U3]

`DdlOperationID`, `MaintenanceOperationID`, and
`EngineInner::next_lock_operation_id` are removed. `LockOwner` becomes:

```text
LockOwner {
    family: LockFamily(SessionID),
    scope: LockScope,
}

LockScope =
    SessionExplicit
  | Transaction(TrxID)
  | Statement(TrxID, StmtNo)
  | Operation(OperationID)
```

`Operation(OperationID)` replaces only the current `Ddl` and `Maintenance`
variants. The typed DDL or maintenance operation authority constructs the owner and
retains purpose metadata; callers must not manufacture an operation owner from
a bare id. DDL's explicit-session-lock rejection remains a DDL acquisition
policy selected by the typed DDL path, not a property inferred from
`LockScope`. A future `LockScopeState` must likewise validate purpose through
the coordinator capability or stored operation metadata when purpose affects
policy. This preserves task 000243's exact-owner isolation while superseding
its duplicate id types and purpose-bearing scope variants. [D3] [D13] [C1]
[C5] [U3]

### Normative Operation Entry States

The implementation may split labels and payload storage as `TrxEntry` does, but
it must preserve these logical states and operation-kind restrictions: [D4]
[C2] [C4] [B1] [U7]

```text
ForegroundAvailable
    a reusable public-transaction payload is checked in and may be leased

ForegroundRunning
    one foreground authority owns a checked-out transaction payload or polls
    the whole DDL/maintenance future

CleanupReady
    a complete terminal transaction payload is checked in and may be claimed
    for rollback or proven discard

CleanupRunning
    one SessionOperationCleanupClaim owns that transaction payload

BackgroundQueued
    the exact pinned DDL/maintenance future has left the foreground wrapper
    and is owned by the mandatory worker queue

BackgroundRunning
    the cleanup executor owns and polls that exact pinned future

CompletionOwned
    an existing terminal subsystem, such as group commit, owns completion;
    observer drop and cleanup cannot override it

Terminal
    all required proofs were consumed and the outer session may finalize

FailedRetained
    terminal cleanup, operation execution, or executor supervision failed;
    the engine is poisoned and the safe residual owner is recorded
```

`ForegroundAvailable`, `CleanupReady`, and `CleanupRunning` apply to reusable
or terminal transaction payloads. A DDL or mutating-maintenance task instead
moves directly from `ForegroundRunning` to `BackgroundQueued` by taking one
non-cloneable `Pin<Box<...>>`, and from `BackgroundQueued` to
`BackgroundRunning` when the worker receives it. The task is never checked into
the stable entry and no phase payload is reconstructed there. The transition
and `Option::take` happen before queue publication, so the foreground and worker
cannot poll concurrently. Duplicate identity hints cannot manufacture another
task or claim. [C2] [C4] [U5] [U7]

### Operation Admission And Hierarchy

Every public session operation that may mutate session/runtime state, acquire
effect-owning logical locks, checkout a transaction, or begin an externally
visible workflow must reserve or resolve the active entry before beginning that
work. Reservation happens after engine admission and registry resolution but
before table pins, effect-owning logical locks, nested transactions, files, or
progress objects are acquired. Synchronous read-only observations and
standalone progress waits may keep current idle validation without allocating
an entry only when every pending waiter/guard is independently
cancellation-safe and dropping the future cannot strand an effect. [D4] [C1]
[C5] [C13] [U7]

The hierarchy is:

```text
SessionOperationEntry(key=(session_id, outer_operation_id), kind)
  ├─ PublicTransaction(trx_id; uses outer_operation_id)
  │    ├─ transaction payload in the outer stable entry
  │    └─ at most one active StatementOperation
  ├─ DdlOperation(uses outer_operation_id)
  │    └─ optional PrivateTransaction(trx_id; inherits outer_operation_id)
  ├─ MaintenanceOperation(uses outer_operation_id)
  │    └─ optional PrivateTransaction(trx_id; inherits outer_operation_id)
  └─ SessionExplicitLockOperation
```

`begin_trx` reserves a `PublicTransaction` operation that remains active across
all statement calls until commit, rollback, cancellation cleanup, or fatal
retention. Its coordinator identity is the outer `(SessionID, OperationID)`;
its transaction payload separately retains `TrxID`. The outer entry generalizes
the current `TrxEntry`; it must not wrap another independently synchronized
`TrxEntry` on the public statement path. A statement is a child checkout, not a
second session operation. Private DDL and maintenance transactions inherit the
outer key, live inside the whole-operation future, and must not replace the
outer lifecycle with a transaction-only session state. [C1] [C2] [C6] [C7]
[C10] [U3] [U4] [U5] [U7]

An internal capability derived from the outer operation authority is required
to begin a private transaction and attach its `TrxID` to the same operation.
Its terminal callback returns proof to the enclosing whole-operation future; it
cannot publish the session idle by itself. Public admission stays closed until
that future releases its exact `Operation(OperationID)` lock scope and
publishes terminal completion. [D2] [D3] [C1] [U3] [U4] [U7]

Standalone `wait_for_gc_horizon_after`, `wait_for_purge_completion_after`, and
`wait_for_checkpoint_retry` calls are observer waits, not mandatory maintenance
operations. They release table/runtime claims before an indefinite await and
remain ordinarily drop-cancellable. When the same wait logic is nested inside
an effectful workflow such as `checkpoint_table_with_wait`, it is part of that
outer must-complete maintenance future and inherits its one `OperationID`.
[C1] [C8] [U7]

### Foreground Ownership And Operation-Kind Drop Semantics

Normal work remains on the caller's executor. No registry or entry mutex remains
locked while user callbacks, lock waits, I/O, rollback, DDL, or maintenance work
executes. A non-cloneable authority records the one foreground owner. Its Drop
behavior is selected when the operation is reserved and cannot be inferred from
the most recent internal phase. [D1] [D4] [C2] [U7]

Public transactions retain stable checked-in payload ownership between calls.
Their statement carrier, rather than a borrowed `Statement` facade, is the sole
final owner of `TrxCheckout`, statement undo/redo, and statement-lock state.
Pending acquisition guards settle synchronously before a transaction payload
becomes `CleanupReady`. Normal completion disarms each carrier only after it has
returned the payload or published a terminal owner. [C2] [C3] [C5] [U4] [U6]

DDL and mutating maintenance use a different carrier. After synchronous session
admission and operation reservation, all operation work is captured by an owned
inner future:

```text
OwnedOperation<T> =
    Pin<Box<dyn Future<Output = Result<T>> + Send + 'static>>
```

The outer public future may still borrow `&mut Session`; only the inner
operation and its result `T` must be owned, `Send`, and `'static`. On background
handoff, a cold adapter consumes or records the typed result and erases the
mandatory task to `Future<Output = ()> + Send + 'static`. The pinned generator may
self-borrow its owned `SessionPin`, guards, private transaction, staged runtime,
files, and workflow state across awaits. Those values do not need to be
extracted into a second resumable phase enum merely to survive observer Drop.
[D15] [C1] [C6] [C7] [C8] [U7]

The normative start and Drop boundaries are:

```text
public future never polled
    -> Drop starts nothing and reserves no operation

first poll
    -> reserve entry
    -> construct and immediately poll the owned operation

owned operation Ready
    -> consume terminal proofs
    -> publish Terminal
    -> return the typed result

public observer dropped after reservation while Pending
    -> take the exact Pin<Box<...>> once
    -> publish BackgroundQueued
    -> send one mandatory task
    -> return without polling, cancelling, or destroying the operation
```

The worker transitions `BackgroundQueued` to `BackgroundRunning` and polls the
same heap allocation. Transfer occurs only after a foreground `poll` has
returned, so there is never concurrent polling. Spawning schedules an immediate
worker poll, which installs the worker waker in well-formed nested futures; a
late wake through the old foreground waker may cause an extra schedule but
cannot create a second owner. [D15] [C4] [U7]

Dropping an armed transaction carrier and dropping an armed whole-operation
carrier therefore have intentionally different meanings. The transaction path
synchronously settles local state and requests terminal rollback. The
DDL/maintenance path moves the future and requests no cancellation at all.
Neither destructor performs async cleanup. [D4] [C2] [C4] [U6] [U7]

### Statement Cancellation Terminates The Public Transaction

Once a `Transaction::exec` future has successfully checked out the transaction
and constructed its statement operation, dropping that future requests terminal
cancellation of the whole public transaction. The transaction handle becomes
discarded and cannot execute another statement or select commit afterward.
Cancellation before the future reserves/checks out the statement has no effect
on the still-active transaction. [B2] [U2]

An armed cancellation guard covers the checked-out transaction core,
statement effects, and statement lock state. After any pending acquisition has
synchronously cancelled or yielded its grant, dropping the guard appends the
remaining statement row and index undo buffers to the corresponding transaction
buffers in their original order, discards statement redo, and releases every
statement-owned lock. It then checks the complete transaction payload into the
stable entry as terminally cancelled. These are synchronous ownership and lock
operations; `Drop` neither applies undo nor awaits cleanup. [D2] [C2] [C3]
[C5] [B2] [U6]

Releasing statement locks before the worker applies row or index undo is safe
under the first-touch policy's effect-coverage invariant. A first table touch
uses statement-owned `TableMetadata(S)` only while resolving admission; a
successful binding acquires and caches transaction-owned `TableMetadata(S)`
before the statement grant is released. Point writes acquire
transaction-owned `TableData(IX)`, and full-table mutation acquires
transaction-owned `TableData(X)`, before installing row undo, deletion-buffer
ownership, or index undo. Those transaction locks remain held through terminal
rollback. A failed or cancelled first touch can transiently hold only the
statement metadata lock, so the literal claim that every statement lock always
already has a matching transaction lock is too strong; that pre-binding case
has created no statement effect and may simply release the statement lock.
[D2] [D3] [C2] [C3] [C5] [U6]

An ordinary callback error continues to roll back only the current statement,
with index undo before row undo and redo discarded after both succeed. Each
normal rollback loop borrows its last vector entry across every awaited access
(`last()` for index undo and `last_mut()` for row undo) and calls `pop()` only
after that entry returns success. Cancellation or failure therefore leaves the
current entry, plus every older unprocessed entry, owned by the buffer; entries
already popped are complete. The rollback primitive contract is equally
important: all awaited or fallible access precedes the synchronous mutation
that reverses the effect, and the primitive does not suspend or fail after that
mutation before returning success. The remaining buffer itself records
progress, so cancellation needs no separate statement rollback state or phase
enum. Its armed guard folds any residual undo into transaction undo before
handoff. [C3] [C17] [U6]

This whole-transaction policy avoids a background statement cleanup interval in
which the public transaction appears reusable but its core and lock-family
authority remain worker-owned. Later calls through the weak transaction handle
resolve to `TransactionDiscarded`; they never race the worker for the returned
core. The cleanup worker receives only the resulting `TransactionRollback` or
proven `DiscardOnly` obligation; it never owns a statement-effects payload and
never selects a statement-then-transaction phase. [C2] [C4] [C14] [U2] [U6]

Catalog statements remain children of the enclosing DDL operation. Observer
Drop does not separately cancel or destroy a currently awaited catalog
statement because the exact outer generator, including that child future, moves
to the worker. An ordinary catalog statement error still performs its normal
statement-local rollback; any private-transaction rollback or commit remains
inside the whole DDL control flow. [C2] [C6] [C7] [U6] [U7]

### One Existing Queue And Concurrent Cleanup Worker

The transaction-system cleanup channel and thread become the physical
session-operation mandatory-work channel and worker. Existing abandoned
transaction, terminal rollback, and failed-precommit messages remain supported.
One worker-local `async_executor::Executor` is added as a direct production
dependency and is driven by the existing physical thread; no second consumer,
thread, global runtime, or successful-path worker hop is introduced. [D15]
[C4] [C18] [B1] [B4] [U7]

The message dispatcher is the control future passed to `Executor::run`. It does
not await one job to completion before receiving another. Instead, every
existing transaction cleanup message and every transferred
DDL/maintenance operation is converted to a supervised
`Future<Output = ()> + Send + 'static`, spawned, and explicitly detached. The
executor then polls runnable tasks cooperatively. This permits a background DDL
task awaiting private transaction completion to coexist with the
failed-precommit cleanup job that may be required to complete that wait; the
current sequential await loop would deadlock that dependency. [D15] [C4] [U7]

Dropping the `async_task::Task` returned by `spawn` cancels its future, so raw
task handles are never exposed and are never used as the operation-cancellation
mechanism. The worker calls `detach()` only after wrapping the task with
completion, error, panic, and entry-finalization supervision. The stable
operation entry, not the detached task handle, remains the authoritative
diagnostic and shutdown record. [D15] [C4] [U7]

Transaction cleanup hints remain identity-and-reason messages. The worker
resolves the stable entry and constructs a claim only if the transaction payload
is claimable, so stale or duplicate hints are neutral. A whole-operation
handoff is different: the mandatory message owns the exact non-cloneable pinned
future and is correctness-critical. Its `EngineRef` runtime pin guarantees that
normal shutdown cannot stop the receiver first. A mandatory send failure is
therefore an internal worker-lifetime violation; it must poison the engine,
publish failed retention, and preserve rather than drop the task. [D4] [C2]
[C4] [C13] [U7]

`Stop` is a quiescence barrier, not permission to cancel executor tasks. After
the producers that can create mandatory work are closed, the dispatcher drains
messages already queued behind the marker, spawns them, and continues driving
the executor until it has no unfinished tasks. Only then may the control future
return and the executor be dropped. [D6] [D15] [C4] [C13]

Concurrency is cooperative. A task that returns `Pending` cannot block another
runnable cleanup task, but synchronous blocking or an unbounded CPU loop inside
one `poll` still blocks the physical worker. DDL and maintenance code used by
this path must remain executor-neutral, avoid blocking calls, and add explicit
cooperative yield points where a bounded poll cannot otherwise be guaranteed.
Adaptive capacity and additional physical runners remain backlog 000123. [D14]
[B4] [U7]

### Transaction Cleanup Classification

Only checked-in transaction payloads require claim-time classification in the
initial design: [B1] [U2] [U6] [U7]

| Obligation | Examples | Required action |
| --- | --- | --- |
| `DiscardOnly` | synchronously settled transaction proven to own no undo, redo, binding, or lock effect | close transaction and operation proofs without applying undo |
| `TransactionRollback` | cancelled public statement after synchronous undo folding or abandoned public transaction | use the existing worker-owned rollback path and consume `ReleasedTransactionLocks` |
| `CompletionOwned` | public commit accepted by prepare/group commit | never roll back; the completion owner reports transaction-lock and operation completion |
| `FailedRetained` | rollback failure or cleanup-claim abandonment | poison, retain the complete residual owner, and keep the session unavailable |

`DiscardOnly` is based on proven absence of effects and external publication,
not transaction kind or caller intent. Public transaction commit retains its
existing specialized handoff: once prepare/group commit owns completion,
`CompletionOwned` prevents cancellation cleanup from converting it to rollback,
even if the public waiter disappears. [D2] [D11] [C2] [C4]

Private DDL/maintenance transactions are not independently classified because
of observer Drop. They remain owned by the whole-operation future, whose normal
control flow commits, rolls back, or compensates them. Likewise, sessionless
system work remains inside that future until an existing terminal subsystem
accepts it. [C6] [C7] [C8] [C10] [U7]

### Whole-Operation Continuation Across Irreversible Gates

Once first poll has reserved a DDL or mutating-maintenance operation, the whole
future is must-complete. It may cross an irreversible gate in the foreground
because caller Drop cannot destroy any nested subfuture or generator local: the
same pinned allocation moves to the cleanup worker and resumes from its current
await. No `ForegroundContinuation`, checked-in phase payload, or bespoke
resumable state machine is required solely for cancellation safety. [D15] [C6]
[C7] [C8] [C11] [C12] [U7]

Irreversible gates remain explicit error-policy boundaries. Before a gate, a
normal operational error may use existing guards and private-transaction
rollback to compensate and return an error. After a gate, control flow must
finish the required publication/installation/retirement sequence or poison the
engine; it must not report success or attempt conceptual rollback while leaving
partially published state. Known failures are represented as `Result`, not
panic, so the operation supervisor can record a terminal outcome. [D2] [D5]
[C6] [C7] [C8]

The initial operation policy is:

| Operation | Ordinary error before the gate | Required behavior after the gate |
| --- | --- | --- |
| Public statement/transaction | statement cancellation uses whole-transaction rollback | accepted commit remains `CompletionOwned` |
| Create table | roll back catalog staging and destroy/delete provisional file/runtime state | finish catalog-owned publication and runtime installation, or poison |
| Create index | roll back build state, staged runtime/layout, and private catalog transaction | finish catalog commit, root/layout publication, and installation, or poison |
| Drop index | restore staged catalog/layout removal | finish catalog commit, root publication, and runtime retirement, or poison |
| Drop table | release validation/acquisition state before `start_drop_lifecycle` | finish lifecycle drain, catalog cascade/commit, and dropped-runtime retention, or poison |
| Table checkpoint | restore the admitted freeze/checkpoint attempt | finish root or silent-watermark publication, system commit, and workflow completion, or poison |
| Catalog checkpoint | discard scan and prepared unpublished file state | finish durable root publication and in-memory progress/cache completion, or poison |
| Redo retention/truncation | discard an unpublished retention plan | finish marker/root publication and progress update; file unlink remains retryable |
| Table GC/index cleanup | roll back its private transaction on error | no irreversible continuation gate in the initial workflow |

Each phase task must identify the concrete existing source states on both sides
of every gate and test error behavior there. The gate may move earlier to make
the post-gate obligation unambiguous, but never later than the first
irreversible mutation. This classification is also the foundation for a later
explicit cancellation protocol: a cooperative request may be accepted at a
proven pre-gate checkpoint, while a post-gate request must report too-late or
deferred completion and let continuation win. Blind future Drop never consults
that policy. [C6] [C7] [C8] [C11] [C12] [U7]

### Lock Ownership And Terminal Proofs

Operation-scope lock ownership is captured by the whole DDL/maintenance future.
A fresh local guard may release an untransferred grant on ordinary error. Once
the task records the exact
`LockOwner(LockFamily(SessionID), Operation(OperationID))`, only its terminal
control flow may close that scope. Observer Drop moves the pinned generator and
therefore does not run nested lock-guard destructors or release the scope. The
stable entry retains `SessionOperationKind`, so unifying the scalar owner does
not erase whether DDL or maintenance policy applies. The initial implementation
may use current exact-owner release and its existing resource scan; targeted
`LockScopeState` cleanup remains backlog 000171. [D3] [D13] [C5] [B3] [U3]
[U7]

A DDL operation uses its outer `OperationID` from construction through all
target-table claims, ordinary-error compensation, required post-gate work, and
terminal scope close. A maintenance workflow likewise uses one outer
`OperationID` for the whole coordinated call, including internal wait/retry
cycles. `ScopedTableRuntimeAccess` therefore becomes a carrier for one
acquisition's table pin and fresh claim guards; it no longer allocates or
defines an operation identity. Before an indefinite internal wait, the workflow
releases the current table pin and resource claims, settles any pending waiter
or unobserved grant, and keeps the operation scope open. A retry may then
reacquire claims under the same exact owner. Only whole-operation terminal
control flow consumes the scope. [D13] [C1] [C5] [U3] [U7]

This distinction is normative for backlog 000171: releasing the current claims
of an open operation is not the same transition as closing its
`LockScopeState`. Each acquisition incarnation must be settled before the next
one begins, so a stale guard or waiter cannot later release a reacquired claim.
The exact representation may use non-cloneable claim tokens or an acquisition
epoch in the later lock-system phase; it must not allocate a new semantic
operation id merely to distinguish retries. This deliberately supersedes task
000243's one-`MaintenanceOperationID`-per-`ScopedTableRuntimeAccess` boundary
while preserving its exact-owner and release-order guarantees. [D3] [D13] [B3]
[U3]

The required terminal order is:

```text
pending acquisition cancelled or observed
  -> if a cancelled statement exists:
       residual row/index undo folded into transaction and redo discarded
  -> statement locks released while transaction locks remain
  -> transaction effects, including folded undo, rolled back or commit completed
  -> transaction table bindings released
  -> transaction locks released
  -> ReleasedTransactionLocks consumed
  -> DDL or maintenance future completed all compensation or required
     post-gate work
  -> exact Operation(OperationID) scope closed
  -> operation terminal proof published
  -> if Open: retain SessionExplicit and publish Idle
     if CloseRequested/Abandoned: release SessionExplicit and publish Closed
```

Ordinary callback-error statement rollback is a foreground, non-terminal path
and does not enter this sequence if it completes. If that future is cancelled,
its remaining vector-owned undo enters the synchronous folding step above.
[C3] [C17] [U6]

The implementation will retain `ReleasedTransactionLocks` keyed by `TrxID` and
add non-cloneable operation-level proof types as necessary. An operation proof
for a transaction-bearing payload can be constructed only by consuming the
nested transaction proof; a no-transaction payload must carry an explicit
no-transaction obligation instead. Session finalization consumes the operation
proof for the outer `(SessionID, OperationID)` and cannot be called directly by
a nested transaction. Exact type names and whether no-transaction and
transaction cases use an enum or separate proof types are phase-local
representation choices. [D2] [D3] [D12] [C2] [U3] [U4]

`SessionExplicit` locks remain held when a live session returns to `Idle`. They
release only when an idle session closes, or after the active operation reaches
its ordered terminal point under `CloseRequested` or `Abandoned`. Session drop
must never release them merely because the outer operation future is between
foreground, mandatory-queue, and cleanup-executor ownership. [C1] [B1] [U7]

### Public Session Drop And Explicit Close

Public `Session` drop records `Abandoned` and returns immediately. If the
session is idle, it may synchronously release `SessionExplicit` and remove the
entry as today. If a public detached transaction is still foreground-owned,
session abandonment does not cancel or steal it; the transaction may still
explicitly commit or roll back. Its eventual terminal outcome closes the
abandoned session. [D4] [C1] [U2]

If transaction cancellation has already published `CleanupReady` or
`CleanupRunning`, session drop only changes disposition and sends an idempotent
hint. If a DDL/maintenance observer has detached, the entry is already
`BackgroundQueued` or `BackgroundRunning`; session drop changes disposition but
does not cancel, steal, or duplicate the task. The current owner completes and
closes the session. [B1] [U2] [U7]

`Session::close().await` preserves the current rejection of a still-live
detached transaction and does not implicitly cancel it. A background
DDL/maintenance task has no remaining caller capable of completing it, so close
records `CloseRequested`, obtains a completion listener without holding the
lifecycle mutex, and waits for `BackgroundQueued`, `BackgroundRunning`, or
`CompletionOwned` to publish closed or failed-retained terminal state. Closing
an idle session remains idempotent. [D4] [C1] [U2] [U7]

A new operation attempted while a detached DDL/maintenance task owns the
session returns an `ExistingOperation` lifecycle error rather than waiting or
starting parallel work. Existing errors remain: `ExistingTransaction` for a
live detached transaction, `SessionUnavailable` for a closed/abandoned session,
and `TransactionDiscarded` for a transaction invalidated by cancellation.
Operation id, kind, state, and disposition are attached for diagnostics. [C14]
[U7]

### Shutdown Drain

`Engine::try_shutdown` and blocking `Engine::shutdown` remain synchronous.
Closing engine admission precedes operation scanning. The session registry gains
active-operation counts, state-change epochs, and collection of every
claimable session operation, not only abandoned transactions. [D4] [D6] [C13]
[B5]

`try_shutdown` queues claimable work and reports `ShutdownBusy` while a
foreground authority, commit owner, transaction cleanup claim, background
whole-operation task, retained terminal payload, or runtime pin remains.
Blocking shutdown waits for live foreground capabilities to finish or be
dropped; it never commits or cancels a still-live public transaction on the
user's behalf. Dropping a DDL/maintenance observer during that wait transfers
the task, whose `EngineRef` keeps runtime drain and the cleanup worker alive
until terminal completion. Shutdown repeatedly scans stable entries and waits
on operation/runtime epochs rather than polling under a registry guard. [D4]
[C13] [U7]

The cleanup worker receives `Stop` only after the log thread and every producer
that can enqueue mandatory work are quiescent. It drains messages already queued
behind the marker, spawns them, and drives all unfinished executor tasks before
transaction-system and lower components shut down. The executor must never be
dropped as a cancellation shortcut. Failed-retained payloads are released only
by an owner-side retention teardown ordered before their required table, pool,
log, file, and lock components disappear. [D6] [D15] [C4] [C13] [U7]

This RFC does not add forced shutdown. Dropping `Engine` without graceful
shutdown retains RFC-0019's non-graceful policy; the implementation must not
make weak public handles into strong lifetime owners. [D4] [B5]

### Failure Retention And Executor Failure

A transaction cleanup claim and whole-operation supervisor each have a
non-panicking failure fallback. Known operational failures use `Result` and
must finish their ordinary-error compensation or post-gate failure policy
before publishing the entry outcome. A background result with no observer is
still recorded and logged with operation identity, kind, phase, and disposition.
[C3] [C4] [C6] [C8] [B1] [U7]

The foreground driver guards each direct inner poll so an unwind marks the
future non-transferable before the driver's Drop path can mistake it for
observer detachment. Every background mandatory task is likewise wrapped so a
panic cannot silently disappear through a detached `async_task::Task` or
terminate the shared dispatcher. A panic while polling an operation makes that
future non-resumable: the supervisor poisons the engine, publishes
`FailedRetained`, wakes lifecycle waiters, and never polls or enqueues the
panicked future again. Cancellation-sensitive resources held inside the
generator require panic-safe guards whose unwind path either restores a proven
safe state or transfers a component-owned retention token before destruction.
The entry stores that token/diagnostic, not the generator or its `EngineRef`.
[D15] [C3] [C4] [C6] [C8] [U7]

Failed retention is terminal for admission but remains visible to shutdown. It
is represented as a closed failed session only after the residual payload and
any intentionally retained locks have a safe owner whose teardown is ordered.
The coordinator never publishes ordinary `Idle` after a fatal cleanup error.
[D6] [C1] [C4]

The implementation must avoid a registry-to-engine strong cycle in retention.
Stable entries retain component-owned payload or retention identifiers; a
running cleanup claim or whole-operation future may hold `EngineRef`, but
checked-in or failed-retained state may not. A mandatory queue send failure is
an invariant failure handled by the same poison-and-retain policy; Drop must not
run the task synchronously, panic, or destroy it. [D4] [C2] [C4] [U7]

### API And Performance Contract

Normal public method signatures remain unchanged. `Transaction::exec` changes
only its cancellation semantics: dropping a polled, checked-out statement
becomes safe synchronous settlement followed by terminal transaction
cancellation instead of a `StmtEffects` panic. DDL and mutating-maintenance
method documentation explicitly changes their future contract:

- dropping before first poll starts no operation;
- after first-poll reservation, dropping the public future detaches its observer
  and the operation continues in the background;
- the session remains busy until terminal completion, and the dropped typed
  result is not recoverable through the initial API;
- standalone progress waits remain ordinary cancellation-safe futures. [D1]
  [D4] [C1] [C2] [C3] [U2] [U6] [U7]

The public compiler-generated future may continue to borrow `&mut Session` and
need not itself expose a `Send + 'static` bound. Its inner DDL/maintenance
operation is boxed as `Send + 'static` before its first await. All owned inputs,
runtime guards, private transactions, I/O waiters, and affected test hooks must
therefore support worker migration. User callbacks continue to borrow
`Statement` and are never moved to the cleanup executor. [D15] [C1] [C19]
[U7]

Performance is a normative correctness-adjacent contract, not best-effort
follow-up work. Shared coordination is necessary when reserving the one active
outer operation, transferring a whole future or transaction cleanup payload,
recording foreground/background ownership, and finalizing session admission. It
is not necessary for local phase changes, each successful statement, each
stream item, or each storage object visited by an already-authorized owner.
[D14] [U5] [U7]

The required successful-path cost deltas are:

| Path | Hard delta budget |
| --- | --- |
| Outer operation reservation | Fuse idle validation, checked session-local `OperationID` increment, and entry installation into one lifecycle-mutex critical section. Add no engine-global operation-id atomic and no second pin/reservation lifecycle lock. |
| Public transaction begin/end | Generalize the existing `TrxEntry` allocation and synchronization; do not allocate or synchronize an outer wrapper around a separately active `TrxEntry`. |
| Successful `Transaction::exec` checkout/check-in | Add no registry lookup, heap allocation, `Arc` refcount operation, mutex acquisition, atomic operation, notifier wake, or queue send beyond the current transaction checkout/check-in. An owned stack carrier may add only local tag/armed checks and constant-time moves. |
| `StreamStmt` and returned streams | Preserve one checkout for the stream lifetime and add no coordinator access per `next`, candidate, or returned row. |
| Row/index/page/MVCC loops | Add no coordinator lookup, phase publication, allocation, atomic traffic, locking, or cancellation polling. |
| Logical-lock acquisition | Pass the already-constructed `LockOwner` directly. Do not resolve `SessionOperationKind` or lock the session coordinator per resource probe; typed DDL/maintenance call paths retain purpose locally. |
| DDL/mutating maintenance | Pay one outer reservation, one boxed/vtable-dispatched inner future, and one terminal transition. Do not allocate an id or take the lifecycle mutex per private transaction, retry, or `ScopedTableRuntimeAccess`. Successful execution stays entirely on the caller executor and performs no cleanup-queue send or worker spawn. |
| Cancellation/failure | Synchronous statement undo-buffer folding, redo destruction, and statement-lock release precede transaction cleanup queueing. DDL/maintenance observer Drop pays one `Option::take`, ownership-state transition, mandatory queue send, and cold-path worker spawn; it performs no phase extraction or rollback. |

The operation counter is a plain checked `u64` mutated under the lifecycle mutex
already required for admission. The current engine-global DDL/maintenance
operation-id atomic is removed; existing engine-global `SessionID` allocation
and ordered `TrxID` timestamp source are unchanged. A nested transaction
inherits the outer operation key and therefore adds no coordinator allocation
or synchronization. [D4] [D13] [C1] [C2] [C13] [U3] [U4]

Payload representation must not make the transaction hot path move or clear the
size of a DDL/maintenance future. The checked-in transaction variant reuses the
compact transaction core; cold DDL/maintenance state remains inside its pinned
box and ownership transfer uses `Option::take` rather than phase extraction or
deep copies. Normal transaction checkout/check-in does not advance a session
change epoch or notify lifecycle waiters. Diagnostics and formatted attachments
remain lazy on success. [C1] [C2] [C3] [U5] [U7]

Synchronous statement settlement is not required to be constant-time: moving
residual undo entries, destroying redo, and releasing statement grants may
scale with statement-local state. It must remain non-awaiting and must not add a
separate worker-owned statement phase. A phase may choose an ownership layout
that makes whole-buffer transfer constant-time, provided it preserves undo
order and the successful-path budget. [C3] [U5] [U6]

The cooperative executor removes head-of-line blocking when a cleanup task is
pending on a wakeable async dependency. It cannot preempt synchronous blocking
or long CPU work inside one poll. Phase tasks must establish bounded poll work
or explicit yields; fairness policy, adaptive capacity, and additional physical
runners remain backlog 000123. Operation kind, foreground/background state,
phase, and handoff reason make actual worker use observable. [D14] [B4] [U5]
[U7]

### Validation Strategy

Tests must use explicit hooks, listeners, barriers, and state predicates. They
must not use sleeps or elapsed time to make a race true. `cargo-nextest` remains
the authoritative runner; this RFC does not change `.config/nextest.toml` or
runner timeout policy. [D7]

The implementation phases must add deterministic coverage for:

1. dropping an unpolled DDL/maintenance public future without reserving an
   operation, allocating an id, or starting an effect;
2. independent session-local sequences, uniqueness of
   `(SessionID, OperationID)`, monotonic outer-operation allocation, nested
   transaction inheritance without another allocation, failed-reservation gaps,
   and fail-closed exhaustion without wrap or reuse;
3. public transactions retaining distinct coordinator `OperationID` and MVCC
   `TrxID` roles, with statement locks still derived from `(TrxID, StmtNo)`;
4. DDL and maintenance constructing the same `Operation(OperationID)` lock
   representation while typed operation kind still selects DDL-only policy;
5. one maintenance operation retaining its `OperationID` across
   release/wait/reacquire, with a settled old waiter or guard unable to affect
   the reacquired claims, while standalone progress waits allocate no operation
   and disappear safely on Drop;
6. transaction cancellation while a logical-lock acquisition is queued and
   while an unobserved grant is promoted, plus DDL/maintenance observer Drop at
   the same points transferring the still-pending future and replacing the
   foreground waker on its first worker poll;
7. statement cancellation before mutation, after row undo, after index undo,
   after redo, after merge, while transaction state is checked out, and while
   ordinary index or row rollback holds its last entry across an await; assert
   exact residual-buffer ownership, redo discard, synchronous folding, and the
   absence of a worker-owned statement phase;
8. releasing statement metadata locks before transaction rollback while
   transaction metadata/data locks continue to exclude conflicting work, plus
   cancellation of a pre-binding statement-only metadata grant with no effects;
9. cancellation racing transaction handle drop, session drop, explicit
   rollback, commit handoff, duplicate queue hints, and shutdown scanning;
10. observer Drop before, at, and after every table/index DDL irreversible gate,
    proving that the same pinned task reaches exactly one terminal outcome and
    that ordinary errors still select the correct pre/post-gate policy;
11. the equivalent transfer and error-policy coverage for table/catalog
    checkpoint, retention marker, system-transaction, and mutating-maintenance
    gates, including `checkpoint_table_with_wait` continuing through its
    internal wait;
12. transaction locks closing before operation scope, and `SessionExplicit`
   retention for open idle versus release for abandoned/closed;
13. a transferred DDL/maintenance task awaiting transaction completion while a
    failed-precommit rollback job on the same worker runs concurrently and wakes
    it, with no sequential-worker deadlock;
14. `Stop` draining queued messages, spawning them, and waiting for every
    unfinished executor task before teardown, without dropping a live task
    handle or executor;
15. no session admission while an operation is foreground-running,
    cleanup-ready/running, background-queued/running, completion-owned, or
    failed-retained; explicit close waits for detached DDL/maintenance whereas a
    new operation receives `ExistingOperation`;
16. mandatory queue closure, injected operational error, cleanup claim drop,
    foreground and worker task panic, fatal retention, and shutdown teardown,
    proving that no panic is silently swallowed by task detachment;
17. stale `(SessionID, OperationID)` transaction hints and duplicate jobs
    producing no second claim, while a whole-operation task is physically
    non-cloneable and transferred exactly once;
18. successful statement, stream, DDL, and mutating-maintenance execution
    producing no cleanup-queue notification or worker task;
19. every affected production future and deterministic test hook satisfying
    `Send + 'static`, with a transfer test that first polls on one thread and
    completes on the cleanup thread.

Focused state-machine tests must assert exact intermediate states and payload
location. End-to-end tests must run under the standard workspace pass; phases
touching file or backend-neutral I/O also run the alternate `libaio` pass.
Selected cancellation races should use focused nextest stress runs after their
deterministic prerequisites are established. [D7]

Each phase must include structural and measured performance evidence. Test-only
or benchmark-only counters assert the hard budgets without adding production
traffic: in particular, no second entry allocation/lock for a public
transaction, no success cleanup-queue notification/job absent a specifically
proven mandatory handoff, and no per-row or per-lock coordinator access.
Measurement uses paired before/after optimized builds on the same host,
configuration, and warmed data, with repeated runs reporting median and
dispersion. A repeatable regression outside measured baseline noise blocks the
phase unless the RFC is explicitly amended; storage or I/O noise is not an
automatic waiver. [D14] [U5]

The minimum measurement matrix is:

1. a no-op `Transaction::exec` loop inside one transaction to isolate statement
   checkout/check-in;
2. repeated no-effect transaction begin/commit to isolate outer reservation and
   terminal publication;
3. one long-lived `StreamStmt` scan to verify no per-item coordination;
4. existing sequential/random lookup and insert workloads with `LogSync::None`,
   both batch size one and a large batch size;
5. existing table/index scan workloads to detect per-row or per-candidate work;
6. single-session and multi-thread/multi-session runs to expose shared atomic or
   cache-line contention;
7. successful DDL/checkpoint latency including the pinned-box cost, plus
   detached-operation and concurrent transaction-cleanup latency, demonstrating
   no ordinary worker hop and no sequential cleanup deadlock.

The final phase runs the workspace validation, alternate I/O backend validation,
and focused coverage for the changed session, transaction, cleanup-worker, DDL,
maintenance, and benchmark modules. [D4] [D7] [C16]

## Alternatives Considered

### Alternative A: Per-Session Actor Owns All Execution

- Summary: Submit every mutating session operation to an engine-owned actor;
  public futures observe results and cancellation only detaches the observer.
- Analysis: One actor naturally owns execution and cleanup from start to finish,
  eliminating foreground-to-worker handoff. However, the current
  closure-borrowed `Transaction::exec` API cannot outlive its caller without
  requiring owned `Send + 'static` commands or a new execution API. Normal
  foreground work would also pay executor and channel scheduling costs.
- Why Not Chosen: It changes the public programming model, broadens the worker
  runtime, and conflicts with preserving memory-first foreground execution.
  Whole-operation boxing is limited to DDL/mutating maintenance and transfers
  only after observer Drop; statements and successful DDL remain foreground.
- References: [D1], [D4], [C2], [B1], [B4], [U2], [U7]

### Alternative B: Engine-Wide Hierarchical Operation Supervisor

- Summary: Introduce a generic engine operation registry for session operations,
  system transactions, checkpoints, and all component background work, with
  abstract discard/rollback/continue/retain obligations.
- Analysis: This could become a long-term home for operation observability,
  background scheduling, and future parallel session work. It broadens the
  ownership model beyond the one-session serialization prerequisite and would
  require a generic cross-subsystem payload abstraction before those consumers
  are designed.
- Why Not Chosen: It couples backlog 000170 to the adaptive worker/runtime
  problem and delays the exact-family lock prerequisite. The selected
  `SessionOperationEntry` can later become one consumer of such a supervisor
  without designing it now.
- References: [D3], [D6], [B1], [B3], [B4], [U1]

### Alternative C: Coordination Flags Or A Cleanup Counter Only

- Summary: Add `Cancelling`/`Cleaning` session flags or an outstanding-cleanup
  counter while leaving statement, transaction, DDL, and maintenance payloads
  in their current owners.
- Analysis: A barrier could prevent early idle publication, but it would not
  install the armed guard that synchronously settles `StmtEffects` into a
  transaction before the future disappears, prevent nested transaction cleanup
  from overlapping DDL guard release, classify irreversible phases, or produce
  one successful cleanup claim.
- Why Not Chosen: It coordinates observation rather than ownership and therefore
  fails the core acceptance condition.
- References: [D3], [C1], [C2], [C3], [B1], [B2]

### Alternative D: Separate Cleanup Worker Per Operation Kind

- Summary: Keep current local ownership and route statement, transaction, DDL,
  and maintenance cancellation to separate workers.
- Analysis: Separate queues can reduce head-of-line blocking but create multiple
  executors that can mutate one session lock family and require a new
  cross-worker ordering protocol.
- Why Not Chosen: Backlog 000170 explicitly requires one existing physical
  cleanup worker, and backlog 000171 requires the outer lifecycle—not internal
  lock-manager repair—to prove one mutation owner.
- References: [D3], [C4], [B1], [B3], [B4]

### Alternative E: Roll Back Only The Cancelled Statement And Reuse The Transaction

- Summary: Worker-own statement rollback but return the transaction core to
  `Active` afterward when the public transaction handle still exists.
- Analysis: This preserves more transaction work, but creates an asynchronous
  interval where the caller owns the handle while the worker owns its core and
  family authority. It also needs a public wait/retry contract to distinguish
  temporary statement cleanup from terminal abandonment and to handle an
  immediate transaction-handle drop.
- Why Not Chosen: Whole-transaction cancellation gives one terminal outcome,
  makes the weak handle deterministically discarded, requires no background
  statement payload, and matches the required ordering through
  transaction-lock release.
- References: [D2], [C2], [C3], [B1], [B2], [U2], [U6]

### Alternative F: Preserve Fail-Fast Statement Cancellation

- Summary: Keep the `StmtEffects` drop assertion and document that a polled
  statement future must complete, optionally poisoning or aborting if violated.
- Analysis: The assertion catches misuse but is not cleanup ownership. If panic
  is caught, undo memory reachable from MVCC state may be lost while the engine
  continues.
- Why Not Chosen: It does not satisfy cancellation safety or the requirement
  that public `Drop` be non-panicking and non-blocking.
- References: [D4], [C2], [C3], [B2]

### Alternative G: Worker-Owned Statement Payload And Two-Stage Rollback

- Summary: Check statement effects and locks into the stable entry on
  cancellation, track statement rollback progress explicitly, and have the
  worker roll back the statement before rolling back the transaction.
- Analysis: This preserves every local distinction until the worker runs, but
  row and index undo already have transaction-level owners and identical
  terminal rollback semantics. Redo cannot be committed after terminal
  cancellation, and first-touch transaction locks cover every created
  statement effect after statement locks are released. Borrow-last,
  pop-after-success rollback loops also leave residual undo self-describing
  under cancellation.
- Why Not Chosen: Synchronous folding produces one complete transaction
  rollback payload without applying undo in `Drop`. A separate statement
  payload, claim obligation, and phase state would duplicate ownership and
  recovery logic without adding a safety guarantee.
- References: [D2], [C2], [C3], [C17], [B2], [U6]

### Alternative H: Phase-Specific Resumable DDL/Maintenance Payloads

- Summary: Extract files, guards, private transactions, and phase enums into
  checked-in payloads; upgrade to explicit continuation authority at each
  irreversible gate and reconstruct worker continuation after foreground
  cancellation.
- Analysis: This can make every phase and retention object explicit, but each
  async workflow needs a bespoke resumable state machine and a proof that every
  nested awaited future returns complete state before Drop. Rust already
  compiles the whole async operation into a pinned state machine, and moving its
  box pointer preserves self-references without moving the generator.
- Why Not Chosen: Treating caller Drop as observer detachment lets the exact
  compiled state machine continue on the worker, removing duplicated phase
  representation and the risky gap where a nested future must reconstruct
  outer state. Irreversible gates remain explicit for error policy.
- References: [D15], [C6], [C7], [C8], [U2], [U7]

### Alternative I: Blindly Cancel DDL/Maintenance Futures

- Summary: Treat public future Drop, `async_task::Task` Drop, or
  `Task::cancel()` as an operation cancellation request and rely on RAII guards
  or poison after an irreversible gate.
- Analysis: Destroying an arbitrary async generator can occur at any await,
  including after irreversible publication. `async_task` cancellation closes
  and drops the future; it does not provide a database-safe cancellation
  checkpoint or distinguish “caller stopped waiting” from an explicit client
  request.
- Why Not Chosen: One client observer must not be able to poison the engine or
  abandon partially published work. Explicit cancellation requires a separate
  cooperative protocol with safe pre-gate checkpoints and a post-gate
  too-late/deferred result.
- References: [D4], [D15], [C6], [C8], [U7]

### Alternative J: Hand-Rolled `FuturesUnordered` Worker Task Set

- Summary: Keep the existing `futures` dependency and make the cleanup
  dispatcher select between the channel and a `FuturesUnordered` collection of
  mandatory jobs.
- Analysis: This can provide the required cooperative concurrency without a new
  production crate. It also makes the dispatcher own task-set polling, nested
  scheduling, panic supervision, active-task accounting, and stop/drain
  bookkeeping directly.
- Why Not Chosen: A worker-local `async-executor` provides a focused spawn/wake
  abstraction and unfinished-task visibility while remaining independent of a
  global runtime; version 1.13.3 is already present in the lockfile. The RFC
  still requires Doradb-owned supervision and shutdown barriers around it.
- References: [D15], [C4], [C18], [U7]

## Unsafe Considerations (If Applicable)

This RFC does not require new unsafe code or change an existing unsafe
invariant. Ownership transfer, state authorization, retention, and ordering are
implemented with ordinary Rust ownership, atomics, mutexes, events, and
non-cloneable proof types. Moving a `Pin<Box<...>>` transfers only its owning
pointer; the pinned allocation remains stationary, and the `Send + 'static`
trait boundary supplies cross-thread validity. Any phase that discovers an
unavoidable unsafe change must treat it as a separately reviewed scope
expansion rather than assuming approval from this RFC. [D15] [U7]

## Implementation Phases

Implemented program prerequisite (2026-07-29): task
`docs/tasks/000244-add-rfc-0025-benchmark-workloads.md` supplies the
successful-path workload shapes before Phase 1 begins. `stmt-noop` and
`trx-noop` provide Phase 1/2 lifecycle evidence, while bounded `index-scan` and
`index-stream` workloads provide Phase 2's materialized and long-lived stream
evidence over unique or non-unique indexes. `table-ddl` provides Phase 4
evidence, and `index-ddl` provides Phase 5 evidence. Existing insert, lookup,
and table-scan workloads remain the row/index/page-loop baselines. The
workloads use one movable session executor, preserve the artifact and manifest
contracts, and record the resolved logical-key range in benchmark output. This
prerequisite does not implement or resolve a numbered phase. Long-history
table-DDL validation found a separate physical-delete layout issue tracked by
`docs/backlogs/000173-fix-btree-physical-deletion-layout-and-amortize-reclamation.md`;
it does not change the successful-path baseline availability. [D16] [C16]

- **Phase 1: Session Operation Coordinator Foundation**
  - Scope: Add crate-private `OperationID`, a plain session-local monotonic
    allocator in `SessionState`, `(SessionID, OperationID)` operation keys,
    stable `SessionOperationKind`, orthogonal session disposition and operation
    slot state, stable operation entries, transaction foreground
    checkout/check-in leases and cleanup claims, whole-operation
    foreground/background ownership labels, lifecycle events/epochs, queue
    identity messages, `ExistingOperation`, and registry/shutdown inspection.
    Stable entries track but never store a whole-operation future. Replace
    `DdlOperationID`, `MaintenanceOperationID`,
    `EngineInner::next_lock_operation_id`, `LockScope::Ddl`, and
    `LockScope::Maintenance` with `LockScope::Operation(OperationID)`. Adapt the
    existing public transaction lifecycle through the new outer entry without
    changing statement cancellation yet. [D4] [D13] [C1] [C2] [C4] [C5]
    [C13] [C14] [C15] [U3] [U5] [U7]
  - Goals: Prove one active session operation, no registry lock across await, no
    registry-owned whole-operation task or engine strong cycle, neutral
    stale/duplicate transaction hints, independent session-local allocation,
    distinct coordinator/`TrxID` roles, typed DDL/maintenance purpose
    validation, and correct
    idle/open/close-requested/abandoned transitions. Preserve existing terminal
    rollback and failed-precommit messages while generalizing the physical
    worker interface. Prove that public transaction admission reuses rather than
    wraps current `TrxEntry` allocation and synchronization. [B1] [U3] [U5]
  - Prerequisites: RFC-0019 stable session/transaction entries are implemented.
    Task 000243's exact-owner isolation and release-order behavior are the
    migration baseline; its typed ids, global allocator, purpose-bearing scope
    variants, and per-access maintenance identity are intentionally superseded.
    Task 000244's `stmt-noop` and `trx-noop` baselines are
    available for paired successful-path evidence. [D4] [D13] [D16] [U3]
  - Phase-local Choices: Choose the concrete entry-state enum or
    atomic-plus-mutex layout, event epoch representation, diagnostic labels,
    exhaustion error representation, and temporary adapter between legacy
    transaction messages and session-operation messages. The operation counter
    remains a plain session-local value under the lifecycle mutex, and operation
    purpose must not be recovered from the numeric id. Cold payload layout must
    not enlarge transaction checkout to a future or largest operation variant.
  - Non-goals: Do not add statement cancellation settlement, the cooperative
    executor, or DDL/maintenance task transfer in this phase. Do not change the
    physical lock manager or add a physical worker.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`
  - Related Backlogs:
    - `docs/backlogs/000170-session-coordinated-cancellation-cleanup.md`

- **Phase 2: Statement And Public Transaction Cancellation Ownership**
  - Scope: Arm `Transaction::exec` after checkout with a cancellation guard that
    synchronously settles pending acquisition, appends residual statement
    row/index undo to transaction undo, discards statement redo, releases
    statement locks, and checks the transaction into the outer session entry as
    terminally cancelled. Refactor normal row/index rollback to borrow the last
    entry across awaits and pop only after success. Integrate transaction-handle
    abandonment, explicit rollback, and commit handoff with the same entry, and
    route cancellation only to whole-transaction worker cleanup. Private
    transaction carriers inherit the enclosing operation key without allocating
    another `OperationID`; `TrxID` remains the MVCC identity and source of
    transaction/statement lock proofs. [D2] [C2] [C3] [C4] [C17] [B2] [U3]
    [U4] [U6]
  - Goals: Make non-empty statement future drop non-panicking; settle pending
    acquisitions before handoff; prove first-touch transaction-lock coverage
    before statement-effect creation; preserve the current undo entry on normal
    rollback cancellation or failure; prove synchronous undo folding, redo
    discard, and statement-lock release before transaction rollback; preserve
    commit-owned completion; return `TransactionDiscarded` after cancellation;
    and consume `ReleasedTransactionLocks` before outer operation completion.
    Prove that the worker receives no statement payload or statement rollback
    phase. Preserve the hard successful `Transaction::exec` and `StreamStmt`
    cost budgets.
  - Measurement Evidence: Compare `stmt-noop` and `trx-noop`
    against their Phase 1 baselines, and use `index-stream` with fixed loaded
    data and `--range` to enforce the no-per-item stream budget across unique
    and non-unique index modes. [D16] [C16] [U5]
  - Prerequisites: Phase 1 entry/lease/claim transitions are available, and task
    000174 worker-owned terminal rollback plus task 000242 transaction-lock
    release proof remain intact. [D11] [D12]
  - Phase-local Choices: Choose the cancellation-guard layout, exact terminal
    transition, and efficient whole-buffer transfer representation. The guard
    must compose with, not duplicate, the current checkout ownership transfer;
    borrowed `Statement` facades cannot be unguarded final payload owners. The
    residual undo buffers encode rollback progress, so the stable entry must not
    add a statement payload or phase enum. The task may optimize the proven
    no-effect path, but may not return a cancelled transaction to reusable
    `Active`.
  - Non-goals: Do not migrate DDL or maintenance progress and do not redesign
    statement APIs, MVCC undo formats, or group commit. Do not add worker-side
    statement rollback.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`
  - Related Backlogs:
    - `docs/backlogs/000124-statement-execution-cancellation-safety.md`

- **Phase 3: Mandatory Operation Driver And Concurrent Cleanup Executor**
  - Scope: Add `async-executor` as a direct storage dependency. Introduce the
    generic foreground driver around
    `Pin<Box<dyn Future<Output = Result<T>> + Send + 'static>>`, the
    type-erased `Future<Output = ()> + Send + 'static` mandatory message,
    exactly-once `Option::take` handoff, background result/failure supervision,
    and `ForegroundRunning -> BackgroundQueued -> BackgroundRunning` entry
    transitions. Replace the worker's sequential message await with one
    worker-local `async_executor::Executor` that spawns and explicitly detaches
    supervised transaction cleanup and operation tasks. Make `Stop` drain the
    queue and executor to quiescence. [D15] [C4] [C13] [C18] [U7]
  - Goals: Prove that unpolled Drop starts nothing, successful mock operations
    remain foreground-only, a pending task moves between threads without moving
    its allocation, foreground and worker never poll concurrently, detached task
    handles cannot cancel mandatory work, independent pending jobs make
    progress, panic cannot disappear silently, and worker shutdown drops only
    an empty executor.
  - Prerequisites: Phase 1 supplies stable foreground/background ownership
    states and terminal publication. Existing cleanup worker lifetime and
    log-before-cleanup shutdown ordering remain the baseline. [C4] [C13]
  - Phase-local Choices: Choose the concrete public-wrapper/internal-driver type
    names, typed background result sink, active-task quiescence notification,
    and mandatory-send failed-retention representation. Use
    `async_executor::Executor`, not a global runtime or `Task::cancel`; every
    spawn must be supervised before `detach()`.
  - Non-goals: Do not migrate production DDL or maintenance operations in this
    phase, add another physical runner, or design explicit client cancellation.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

- **Phase 4: Whole-Operation Table DDL**
  - Scope: Reserve outer DDL entries for create/drop table and wrap each complete
    workflow, including its owned `SessionPin`, exact
    `Operation(OperationID)` lock authority, provisional file/runtime state,
    drop lifecycle, and nested private catalog transaction, in the Phase 3
    pinned operation driver. Observer Drop at any await transfers the exact
    future; ordinary errors retain explicit pre/post-gate policy. [C1] [C5]
    [C6] [U3] [U4] [U5] [U7]
  - Goals: Preserve compensation for every pre-gate create-table error; require
    completion or poison after catalog commit or `start_drop_lifecycle`;
    prevent private transaction completion from overlapping DDL scope release;
    keep successful DDL on the foreground executor; and make public observer
    Drop non-cancelling before and after every gate.
  - Measurement Evidence: Use `table-ddl` on equivalently fresh prepared roots
    to compare successful create/drop latency without catalog-history skew.
    [D16] [C16] [U5]
  - Prerequisites: Phase 2 supplies nested statement/transaction cancellation
    and terminal proof composition; Phase 3 supplies mandatory future transfer.
    Existing create/drop lifecycle gates and exact DDL owner isolation remain
    behaviorally stable under Phase 1's unified scope. [D3] [D13]
  - Phase-local Choices: Refactor public wrappers versus owned inner async
    helpers, place deterministic hooks immediately before/after catalog commit,
    file/root publication, lifecycle gate, drain, and runtime installation, and
    add yields only where one poll otherwise has unbounded work. A gate may move
    earlier but not later than the first irreversible mutation. Do not introduce
    a duplicate resumable phase payload.
  - Non-goals: Do not migrate index DDL, redesign table-file format, or change
    physical lock-manager representation.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

- **Phase 5: Whole-Operation Index DDL**
  - Scope: Apply the Phase 4 whole-operation contract to create/drop index,
    including build progress, staged hot/cold runtime, layout/root state,
    private catalog transaction, unified operation-scope DDL locks, post-commit
    publication, installation, and retirement. [D10] [C7] [U3] [U7]
  - Goals: Preserve ordinary-error compensation for every pre-commit phase,
    keep successful work in the foreground, make observer Drop transfer rather
    than cancel at every build/publication await, prevent post-commit rollback,
    and poison safely if required root publication or installation cannot
    complete.
  - Measurement Evidence: Use `index-ddl` on equivalently fresh
    `index = "none"` roots, with matched empty or preloaded tables, to compare
    successful create/drop latency. [D16] [C16] [U5]
  - Prerequisites: Phase 2 transaction proof composition, Phase 3 task transfer,
    and Phase 4's owned DDL wrapper/lock-scope pattern are available.
  - Phase-local Choices: Select shared or separate create/drop index inner
    helpers, precise ordinary-error cleanup order for staged runtime versus
    layout, deterministic fault/transfer hooks for build, catalog commit, root
    publication, install, and retirement, and bounded-poll/yield points for
    index build work.
  - Non-goals: Do not redesign index algorithms, MVCC build visibility, table
    layout format, or lock-family internals.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

- **Phase 6: Whole-Operation Maintenance And System Transactions**
  - Scope: Wrap effectful table checkpoint/freeze workflows, catalog checkpoint,
    redo retention/truncation, and table/index cleanup as whole pinned
    operations. Retain one outer `OperationID` across each workflow's internal
    release/wait/reacquire cycles; `ScopedTableRuntimeAccess` carries claims but
    does not allocate identity. Keep private/sessionless transactions and
    publication state inside the future. Leave standalone
    `wait_for_checkpoint_retry`, `wait_for_gc_horizon_after`, and
    `wait_for_purge_completion_after` as ordinary drop-cancellable observers;
    an internal retry wait inside `checkpoint_table_with_wait` inherits the
    outer must-complete task. [D13] [C1] [C8] [C9] [C10] [C11] [C12] [U3]
    [U5] [U7]
  - Goals: Preserve restoration of reversible attempts on ordinary error,
    rollback session-bound private transactions, continue ordered/published
    system work, settle each acquisition before retry, close the operation scope
    last, poison/retain failed irreversible phases, and prove that standalone
    observer Drop leaves no entry or task.
  - Prerequisites: Phase 2 supplies transaction cancellation/proofs; Phase 3
    supplies task transfer; and Phases 4-5 establish the exact operation-scope
    whole-future pattern at publication gates.
  - Phase-local Choices: Map concrete checkpoint/retention error gates, refactor
    public wrappers versus owned inner helpers, convert affected thread-local
    non-`Send` test hooks to executor-neutral shared hooks, and place
    deterministic hooks around publication admission, silent watermark,
    table/catalog root, retention marker, system commit, retry wake, and unlink
    completion. Do not allocate a new semantic `OperationID` for a retry or add
    a physical lock-manager claim token in this phase.
  - Non-goals: Do not change checkpoint or recovery formats, retention policy,
    GC algorithms, or turn standalone observer waits into mandatory background
    work.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

- **Phase 7: Lifecycle Drain, Failure Retention, And Readiness Gate**
  - Scope: Remove transitional lifecycle/queue paths; complete explicit close,
    session abandonment, worker failure, fatal retention, `try_shutdown`,
    blocking shutdown, executor-aware queue stop/drain, supervised panic and
    mandatory-send failure, operation diagnostics, API/documentation
    synchronization, cross-operation tests, performance validation, and
    readiness evidence for backlog 000171. [D3] [D4] [D6] [D15] [C13] [U3]
    [U5] [U7]
  - Goals: Demonstrate exactly one poll owner under all
    drop/close/terminal/shutdown races, no lost payload after claim or handoff
    failure, ordered `SessionExplicit` behavior, deterministic queue-and-executor
    drain, no new transaction hot-loop cost, bounded cooperative polling, and a
    documented proof that exact-family serialization can safely assume one
    outer mutation owner. Run the normative structural budgets and paired
    benchmark matrix; no repeatable regression may be waived without an RFC
    amendment.
  - Prerequisites: Phases 1-6 cover every production payload/task variant; no
    legacy effectful operation path may bypass the coordinator before the final
    readiness claim.
  - Phase-local Choices: Finalize state/phase observability, shutdown busy
    attachments, component-owned retention teardown placement, cooperative
    fairness diagnostics, and focused benchmark/test selection. Public shutdown
    remains synchronous and no new physical worker is allowed.
  - Non-goals: Do not implement backlog 000171, adaptive worker capacity, forced
    shutdown, explicit client cancellation, or an async shutdown API.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`
  - Related Backlogs:
    - `docs/backlogs/000171-exact-family-lock-system-redesign.md`

## Consequences

### Positive

- Session state becomes the single admission and terminal ownership authority
  for transactions, DDL, maintenance, close, abandonment, and shutdown.
- Non-empty statement undo cannot disappear with a cancelled caller future:
  residual entries synchronously become transaction-owned, while redo that
  cannot commit is discarded.
- Normal statement rollback is cancellation-safe without a separate phase
  machine because the current undo entry remains buffer-owned until success.
- The cleanup worker handles one transaction rollback payload rather than
  nested statement and transaction rollback payloads.
- A whole pinned DDL/maintenance future is already a resumable state machine;
  caller Drop moves it instead of duplicating files, guards, transactions, and
  phase progress into a hand-written continuation payload.
- One client stopping observation cannot poison the engine or destroy
  irreversible DDL/maintenance progress.
- The operation naturally carries `SessionPin` and `EngineRef` between
  executors, while the stable entry remains free of a strong engine cycle.
- Cooperative worker concurrency lets an operation and the transaction cleanup
  jobs it depends on make progress on one physical thread.
- Existing transaction stable-entry, rollback worker, release proofs, public
  APIs, and foreground hot paths are preserved and generalized.
- One `(SessionID, OperationID)` identity serves registry lookup, cleanup hints,
  diagnostics, and DDL/maintenance lock ownership; duplicate typed ids and the
  engine-global lock-operation counter disappear.
- Backlog 000171 receives the lifecycle proof needed to enforce serialized
  exact-family lock mutation without internal concurrent-close repair.
- Stable operation identities and states improve cleanup, shutdown, and fatal
  retention diagnostics.

### Negative

- Session and operation state machines become larger, and payload checkout/
  check-in requires careful transition and destructor auditing.
- Statement cancellation intentionally discards the entire public transaction,
  even when statement-only rollback might have preserved earlier work.
- Cancellation may synchronously move residual undo entries, destroy redo, and
  release statement locks in the dropping thread; this cold-path cost can scale
  with statement-local state.
- Every DDL and mutating-maintenance call pays one pinned-box allocation and
  dynamic future dispatch, even when it completes in the foreground.
- Inner DDL/maintenance futures and everything retained across their awaits must
  satisfy `Send + 'static`; affected thread-local/non-`Send` test hooks require
  refactoring.
- Dropping a started DDL/maintenance future no longer stops work. Its typed
  result is lost, the session remains busy until terminal completion, and a
  later `close()` may wait for background work.
- `async-executor` becomes a direct production dependency, and mandatory task
  handles require disciplined supervision plus explicit `detach()`.
- Cooperative concurrency is not preemption: one blocking call or unbounded CPU
  poll can still stall every cleanup task on the physical worker.
- A transferred task intentionally keeps `EngineRef` alive, so graceful
  shutdown must drive it to terminal before component teardown.
- A public transaction carries its own coordinator `OperationID` and `TrxID`;
  a private transaction inherits its enclosing operation key and carries its own
  `TrxID`. This intentional semantic separation adds one outer identity field
  but no child operation allocation.
- `LockOwner` no longer encodes DDL versus maintenance purpose, so typed
  operation capabilities, stable kind metadata, policy checks, and diagnostics
  must preserve and validate that distinction outside `LockScope`.
- DDL and maintenance add one stable outer-entry reservation and necessary
  ownership-boundary transitions. Public transaction entry synchronization is
  generalized rather than layered, the previous engine-global
  DDL/maintenance id atomic is removed, and successful statement plus
  row/index/buffer hot loops have a zero-added-shared-coordination budget.
- Fatal retention and shutdown now account for more payload variants and require
  explicit component-order teardown.

## Open Questions

No blocking architectural questions remain for the approved draft direction.
Entry-state packing, helper/proof type names, event representation, exhaustion
error vocabulary, operation-driver naming, executor quiescence notification,
panic-retention representation, and per-workflow test hook placement are
intentionally phase-local choices constrained by the normative identity,
purpose, ownership, gate, and ordering contracts above.

## Future Work

- Complete the exact-family lock-system redesign in backlog 000171 after Phase 7
  publishes coordinator readiness, including explicit open-scope versus
  released-claim transitions for maintenance retry and purpose validation
  independent of `LockScope`.
- Evaluate cleanup fairness, cooperative chunking, or adaptive/shared worker
  capacity through backlog 000123.
- Evaluate an async engine shutdown variant through backlog 000114 only if
  synchronous owner waiting proves unsuitable.
- Design an explicit client-side cancellation API through backlog 000172 after
  internal semantics and error reporting are stable. It should use an operation
  id/token plus cooperative safe points: accept and compensate only before a
  proven irreversible gate, and return too-late/deferred while required
  post-gate continuation wins. It must not call `Task::cancel`.
- Revisit parallel work within one session only with an explicit family
  coordinator and operation submission model.

## References

- `docs/backlogs/000170-session-coordinated-cancellation-cleanup.md`
- `docs/backlogs/000124-statement-execution-cancellation-safety.md`
- `docs/backlogs/000171-exact-family-lock-system-redesign.md`
- `docs/backlogs/000123-adaptive-background-worker-runtime.md`
- `docs/backlogs/000114-evaluate-async-engine-shutdown-api.md`
- `docs/backlogs/000173-fix-btree-physical-deletion-layout-and-amortize-reclamation.md`
- `docs/backlogs/closed/000169-separate-session-operation-lock-scopes.md`
- `docs/process/coding-guidance.md`
- `docs/rfcs/0019-weak-public-runtime-handles.md`
- `docs/rfcs/0016-logical-lock-manager.md`
- `docs/tasks/000174-transaction-terminal-rollback-cancellation-safety.md`
- `docs/tasks/000242-enforce-terminal-transaction-lock-release-ordering.md`
- `docs/tasks/000243-separate-session-operation-lock-scopes.md`
- `docs/tasks/000244-add-rfc-0025-benchmark-workloads.md`
- `https://docs.rs/async-executor/1.13.3/async_executor/struct.Executor.html`
- `https://docs.rs/async-task/4.7.1/async_task/struct.Task.html`
