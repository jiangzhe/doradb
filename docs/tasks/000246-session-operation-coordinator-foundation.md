---
id: 000246
title: Session Operation Coordinator Foundation
status: implemented  # proposal | implemented | superseded
created: 2026-07-29
github_issue: 914
---

# Task: Session Operation Coordinator Foundation

## Summary

Implement RFC-0025 Phase 1 by replacing the transaction-only session lifecycle
with one engine-owned session-operation coordinator. Every effectful public
session operation reserves one stable `(SessionID, OperationID)` entry with a
stable purpose, while `SessionState` separately records session disposition and
whether its single operation slot is idle, active, or closed.

Directly generalize the current `TrxEntry` allocation into
`SessionOperationEntry`; do not wrap it in a second outer entry. Public
transaction statements continue to checkout and check in the same compact
`TrxInner` through one entry mutex. DDL and maintenance entries track their
foreground ownership and optional private transaction without storing the
whole operation future. This phase defines the later background ownership
labels but does not add whole-future transfer or the cooperative executor.

Move DDL, maintenance, public transaction, and explicit-lock operations onto
one plain session-local `u64` allocation sequence. Replace
`DdlOperationID`, `MaintenanceOperationID`, the engine-global operation-id
atomic, and the two purpose-bearing lock scopes with crate-private
`OperationID` and `LockScope::Operation(OperationID)`. Keep operation purpose
in `SessionOperationKind` and in typed DDL/maintenance authorities rather than
recovering it from the numeric id or lock scope.

Generalize transaction cleanup queue identities, registry inspection, close,
abandonment, and shutdown around stable operation keys. Replace unconditional
normal transaction lifecycle notifications with a lazy session-local
predicate-wait protocol: close or blocking shutdown installs or reuses an event
only on the first session it must wait for, and subsequent wait-relevant
transitions wake that session after releasing state locks. Normal transaction
completion must perform no notifier atomic update, event allocation, or wake
while the event is absent.

Use task 000244's `stmt-noop` and `trx-noop` workloads for paired optimized
before/after measurements. The statement workload guards the existing
checkout/check-in budget; the transaction workload measures reservation,
entry allocation, terminal publication, and removal of the current
per-transaction notification cost.

## Context

RFC-0025 makes `SessionState` the enclosing authority for one complete
effectful session operation. Its first phase supplies the identities, stable
entry, state vocabulary, checkouts, claims, wake protocol, and worker message
keys required by all later cancellation and whole-operation-transfer phases.
The program-level architecture is already accepted, so the strict RFC
complexity gate is satisfied by implementing this bounded phase rather than
opening another RFC.

Parent RFC:

- `docs/rfcs/0025-session-coordinated-cancellation-cleanup-ownership.md`

RFC Relationship:

- Phase 1: Session Operation Coordinator Foundation.

Source Backlogs:

- `docs/backlogs/closed/000170-session-coordinated-cancellation-cleanup.md`

Prerequisites:

- `docs/rfcs/0019-weak-public-runtime-handles.md`
- `docs/tasks/000174-transaction-terminal-rollback-cancellation-safety.md`
- `docs/tasks/000242-enforce-terminal-transaction-lock-release-ordering.md`
- `docs/tasks/000243-separate-session-operation-lock-scopes.md`
- `docs/tasks/000244-add-rfc-0025-benchmark-workloads.md`

Related Follow-ups:

- `docs/backlogs/000124-statement-execution-cancellation-safety.md`
- `docs/backlogs/000171-exact-family-lock-system-redesign.md`
- `docs/backlogs/000123-adaptive-background-worker-runtime.md`

Issue Labels:

- type:task
- priority:high
- codex

The current implementation has four coupled limitations:

1. `SessionLifecycle` combines disposition and transaction presence in
   `RunningIdle`, `RunningActive`, `AbandonedIdle`, and `AbandonedActive`.
   It cannot represent an active DDL, maintenance, explicit-lock call, close
   request, background owner, or failed-retained operation independently.
2. `TrxEntry` is stable and cleanup-claimable, but its identity is only
   `(SessionID, TrxID)`. DDL and maintenance instead allocate unrelated
   engine-global ids, so there is no enclosing identity shared by the workflow,
   its private transaction, registry inspection, queue hints, and operation
   locks.
3. `SessionDdlContext` allocates one `DdlOperationID`, while every
   `ScopedTableRuntimeAccess` allocates a new `MaintenanceOperationID`.
   Task 000243 intentionally chose those boundaries before an outer session
   operation existed; RFC-0025 supersedes them with one operation id for the
   whole coordinated call, including private transactions and retry
   reacquisition.
4. `SessionRegistry::trx_changes` is observed in production only by blocking
   `Engine::shutdown`, but commit and rollback always call
   `ChangeNotifier::notify`. Each no-effect transaction therefore performs an
   `AtomicU64::fetch_add`, an event notification/fence, and first-use event
   initialization even when no close or shutdown waiter exists. Normal
   statement checkout/check-in does not notify today and must remain silent.

The public `Session` API uses `&mut self` for DDL, maintenance, and explicit
lock mutation. Safe foreground calls therefore cannot ordinarily overlap.
`LifecycleError::ExistingOperation` is nevertheless required as coordinator
vocabulary: later phases transfer a dropped DDL/maintenance observer's exact
future to background ownership, at which point the `&mut Session` borrow has
ended while the registry entry is still active. It also gives common admission
and diagnostics the correct non-transaction classification. A live public
transaction continues to produce `ExistingTransaction`; closed, abandoned, or
close-requested sessions produce `SessionUnavailable`.

The chosen entry layout is a direct generalization rather than a wrapper. A
second `Arc`, allocation, mutex, or registry lookup around `TrxEntry` would
violate RFC-0025's successful-path contract and make every statement pay for
cold DDL/maintenance state. Conversely, retaining the current atomic state
beside a separately locked payload would preserve split ownership truth and
unnecessary atomic traffic. One compact mutex containing state and payload
ownership gives registry inspection a coherent snapshot while preserving one
checkout/check-in mutex acquisition.

## Goals

1. Add crate-private `OperationID`, meaningful only with its owning
   `SessionID`, and a canonical `SessionOperationKey`.
2. Allocate operation ids from a plain `u64` stored in `SessionState`, starting
   at one and mutated only while the lifecycle mutex is held.
3. Assert before operation-id overflow and never wrap, recycle, serialize, or
   report exhaustion as a `LifecycleError`.
4. Reserve exactly one outer operation for every public transaction, DDL,
   mutating maintenance, and explicit session-lock mutation.
5. Keep standalone read-only observations and standalone progress waits outside
   the operation-id domain where their guards and pending waits are already
   independently drop-safe.
6. Store stable operation purpose in `SessionOperationKind`; never infer DDL
   versus maintenance policy from `OperationID` or `LockScope`.
7. Make session disposition (`Open`, `CloseRequested`, `Abandoned`) orthogonal
   to the operation slot (`Idle`, `Active`, `Closed`).
8. Replace `TrxEntry` with one stable `SessionOperationEntry` rather than
   adding an outer wrapper.
9. Represent the RFC's foreground, cleanup, background, completion-owned,
   terminal, and failed-retained ownership labels with explicit invariants.
10. Keep a whole DDL/maintenance future out of the stable entry and out of the
    engine-owned registry graph.
11. Preserve one compact transaction checkout/check-in synchronization
    boundary and add no statement-path registry lookup, allocation, `Arc`
    operation, mutex, atomic operation, event wake, queue send, or lifecycle
    lock beyond the current path.
12. Give public transactions both an operation key and their independent
    engine-wide `TrxID`; do not change MVCC, status, lock, statement, commit, or
    recovery uses of `TrxID`.
13. Let private DDL/maintenance transactions inherit the enclosing operation
    key without allocating another `OperationID` or replacing the outer
    operation slot.
14. Ensure a private transaction terminal callback returns control to its
    enclosing operation and cannot independently publish the session idle.
15. Generalize transaction foreground checkouts, terminal/cleanup claims,
    attachments, stale resolution, and cleanup hints around
    `(SessionOperationKey, TrxID)`.
16. Preserve worker-owned terminal rollback, failed-precommit rollback, and
    `ReleasedTransactionLocks` ordering.
17. Make stale or duplicate identity hints neutral and make payload/terminal
    claims physically single-owner.
18. Replace DDL and maintenance operation ids and lock scopes with
    `LockScope::Operation(OperationID)` while preserving exact-owner isolation
    and DDL-specific policy.
19. Reuse one maintenance operation owner across every scoped runtime access,
    bounded recheck, and internal retry in the enclosing workflow.
20. Make registry inspection and engine shutdown recognize every active
    operation kind, not only transactions, while stopping at the first blocker.
21. Let explicit close wait only after cleanup, completion, or later background
    ownership has become authoritative; continue rejecting a still-live public
    transaction.
22. Wake close/shutdown through an observation-armed predicate protocol with no
    lost-wake interval and no ordinary transaction notification.
23. Preserve weak public handles and prevent a registry-to-engine strong
    reference cycle.
24. Produce paired structural and measured evidence that successful statement
    and transaction paths remain within RFC-0025's performance budget.

## Non-Goals

1. Do not implement statement-future cancellation settlement, residual
   statement undo folding, `DiscardOnly` classification, or the Phase 2
   transaction-cancellation guard.
2. Do not add the worker-local `async-executor`, cooperative task dispatch,
   concurrent cleanup jobs, task supervision, or Phase 3 stop/drain changes.
3. Do not box, transfer, resume, or background-poll a DDL or maintenance
   future. Phase 1 entries define those labels but do not exercise a
   whole-operation handoff.
4. Do not migrate table DDL, index DDL, checkpoint, retention, or GC
   irreversible-gate policy beyond the identity and foreground coordinator
   plumbing required by this phase.
5. Do not treat blind DDL/maintenance future drop as explicit cancellation and
   do not add a public cancellation API.
6. Do not redesign physical lock resources, modes, waiter nodes, grant tokens,
   duplicate-waiter behavior, queue ordering, conversion, or
   `LockScopeState`.
7. Do not implement backlog 000171's serialized family owner, claim map,
   resource incarnation, or exact-scope close proof.
8. Do not change transaction MVCC timestamps, `TrxID` allocation, statement
   numbering, undo/redo semantics, group commit, purge, checkpoint formats, or
   recovery formats.
9. Do not make `Session` shared, `Sync`, concurrently executable, or actor
   driven.
10. Do not add a second transaction entry or independent synchronization layer
    for a public transaction.
11. Do not place a DDL/maintenance future, `EngineRef`, `SessionPin`, or other
    strong engine owner in `SessionOperationEntry`.
12. Do not allocate an `OperationID` per statement, private transaction,
    maintenance retry, `ScopedTableRuntimeAccess`, table, lock acquisition, or
    physical resource.
13. Do not expose operation ids, operation state, or coordinator controls as a
    public API, SQL-visible value, persisted value, or production metric.
14. Do not add `LifecycleError::OperationIdExhausted`; `u64` exhaustion is an
    internal invariant failure guarded by an assertion.
15. Do not change generic `ChangeNotifier` semantics for block-index and other
    unrelated users. Gate session-operation notifications at the coordinator
    boundary.
16. Do not add a benchmark suite runner, repetition framework, CI wall-clock
    threshold, production counter, or benchmark-only storage API.
17. Do not rewrite implemented prerequisite task documents or close related
    backlogs during implementation; `$task-resolve` owns synchronization.

## Plan

### 1. Define session-local operation identity

In `doradb-storage/src/id.rs`, replace `DdlOperationID` and
`MaintenanceOperationID` with crate-private `OperationID`. Keep the usual
copy/equality/hash/order/display behavior required by keys, lock owners, tests,
and diagnostics, but do not add serialization or public construction.

Define a crate-private key equivalent to:

```rust
struct SessionOperationKey {
    session_id: SessionID,
    operation_id: OperationID,
}
```

Keep the pair intact in registry, transaction attachment, and queue interfaces.
Do not convert it to `TrxID`, assume global `OperationID` uniqueness, or use the
raw scalar as a registry key.

Add `next_operation_id: u64` to the lifecycle data in `SessionState`, initialized
to one. Reservation must:

1. lock the lifecycle once;
2. validate open/idle admission;
3. assert that incrementing the counter cannot overflow;
4. construct the key and stable entry;
5. advance the plain counter; and
6. install the entry before releasing the lifecycle lock.

Use a normal assertion or `checked_add(...).expect(...)`, not a debug-only
assertion, wrapping arithmetic, allocator object, engine atomic, recoverable
error, or poison path. Failed work after successful reservation may consume an
id; ids are never deliberately reused.

### 2. Replace the combined lifecycle and transaction entry

Refactor `SessionState` around data equivalent to:

```rust
struct SessionLifecycle {
    disposition: SessionDisposition,
    slot: SessionOperationSlot,
    next_operation_id: u64,
    change_ev: Option<Arc<EventNotifyOnDrop>>,
}

enum SessionDisposition {
    Open,
    CloseRequested,
    Abandoned,
}

enum SessionOperationSlot {
    Idle,
    Active(Arc<SessionOperationEntry>),
    Closed,
}

struct SessionOperationEntry {
    key: SessionOperationKey,
    kind: SessionOperationKind,
    inner: Mutex<SessionOperationEntryInner>,
}

struct SessionOperationEntryInner {
    state: SessionOperationState,
    trx_id: Option<TrxID>,
    trx_inner: Option<TrxInner>,
    cleanup_requested: bool,
}
```

The exact compact representation may use an internal enum to make impossible
field combinations unrepresentable, but it must retain one entry mutex and the
same ownership information. `key` and `kind` are immutable. The entry must
contain neither an independently synchronized public `TrxEntry` nor any whole
operation future or strong engine reference. `cleanup_requested` records only
whether cleanup is required; its source does not alter the rollback policy.

Define stable kinds for:

- `PublicTransaction`;
- `Ddl`;
- `Maintenance`; and
- `SessionExplicitLock`.

Preserve these logical state labels and payload rules:

| State | Public transaction payload | DDL/maintenance meaning |
| --- | --- | --- |
| `ForegroundAvailable` | checked-in `TrxInner` may be checked out | invalid as an outer owner label |
| `ForegroundRunning` | one checkout owns `TrxInner` | caller owns and polls the whole operation; optional private transaction remains a child |
| `CleanupReady` | complete checked-in transaction may be claimed | reserved for later whole-operation cleanup foundations; never stores its future |
| `CleanupRunning` | one cleanup claim owns `TrxInner` | cleanup authority is unique |
| `BackgroundQueued` | invalid | later mandatory queue owns the exact future |
| `BackgroundRunning` | invalid | later executor owns and polls the exact future |
| `CompletionOwned` | prepare/group commit or another terminal subsystem owns completion | enclosing terminal subsystem owns the required completion |
| `Terminal` | transaction and operation obligations are complete | outer operation obligations are complete |
| `FailedRetained` | safe residual owner is retained and session remains unavailable | failed operation remains diagnosable and blocks unsafe reuse |

For `Ddl` and `Maintenance`, the outer state remains
`ForegroundRunning` while an optional child transaction moves among absent,
checked-in, checked-out, and terminal-owned payload positions inside the same
entry mutex. For `PublicTransaction`, the outer state itself exposes those
checkout and cleanup positions. The representation must distinguish these cases
without changing the stable outer kind or adding another synchronized entry.

Track a handle-drop request orthogonally while a transaction core is checked
out. Returning that checkout must publish `CleanupReady`, not
`ForegroundAvailable`. Assert state/payload invariants at every transition.
Keep inspection snapshots coherent under the entry mutex; do not restore a
separate atomic state solely for lock-free diagnostics.

Establish one lock order:

```text
SessionState.lifecycle -> SessionOperationEntry.inner
```

Code that starts from an entry checkout or claim must release the entry mutex
before reacquiring session lifecycle. Hold neither mutex across `.await`,
registry shard retention, queue send, event notification, lock-manager
acquisition, I/O, rollback, or user callbacks.

### 3. Centralize operation reservation, admission, and finalization

Replace idle-only `SessionPin` admission for effectful calls with a
non-cloneable operation authority that owns:

- the `EngineRef` runtime pin;
- the `Arc<SessionState>`;
- the `Arc<SessionOperationEntry>`;
- the exact key and stable kind; and
- its armed foreground/finalization state.

The authority may retain the `SessionOperationPin` name if that best fits local
code, but it must not be stored in the registry entry. Reservation happens
after engine admission and registry resolution and before table/runtime pins,
logical locks, private transactions, files, or progress objects.

Map public entry points as follows:

- `Session::begin_trx` reserves `PublicTransaction`;
- create/drop table and create/drop index reserve `Ddl`;
- mutating catalog/table checkpoint, retention, freeze, and MemIndex cleanup
  workflows, plus finite maintenance-scoped observations such as
  `total_row_pages`, reserve `Maintenance`;
- `lock_table` and `unlock_table` reserve `SessionExplicitLock`;
- `list_table_ids` and independently drop-safe standalone GC/purge/checkpoint
  progress waits retain query/observer admission without an operation id.

One public workflow receives one reservation even when it calls internal
helpers or retries. Internal helpers consume or borrow the already-authorized
operation capability; they must not call the outer reservation path again.

Common admission returns:

- `ExistingTransaction` when the active entry kind is
  `PublicTransaction`;
- `ExistingOperation` with key, kind, state, and disposition attachments for
  another active kind; and
- `SessionUnavailable` for `CloseRequested`, `Abandoned`, or `Closed`.

`ExistingOperation` is intentionally uncommon until later transfer phases
because `&mut Session` serializes current foreground DDL/maintenance calls.
Keep the variant and tests now so later `BackgroundQueued` and
`BackgroundRunning` states do not need an error-vocabulary redesign.

Finalization must verify that the same key still occupies the active slot.
Terminal completion publishes `Idle` only for `Open`; it publishes `Closed`
and releases `SessionExplicit` locks for `CloseRequested` or `Abandoned`.
Stale terminal calls do nothing and cannot finalize a replacement operation.
`FailedRetained` never makes the session reusable.

### 4. Generalize public and private transaction ownership in place

In `doradb-storage/src/trx/mod.rs`, replace `TrxEntry`, `TrxEntryState`,
`TrxCheckout`, and `TrxCompletionClaim` with `SessionOperationEntry`,
`SessionOperationCheckout`, and `SessionOperationCompletionClaim`, which act
directly on `SessionOperationEntryInner`.

For a public transaction:

1. reserve the outer entry and allocate `TrxID` through the unchanged
   transaction timestamp source;
2. install that `TrxID` and `TrxInner` in the same operation entry;
3. put `SessionOperationKey` in the weak public `Transaction` handle;
4. resolve the exact operation key through the session registry;
5. validate `TrxID` and checkout/check in through the entry's single mutex; and
6. let terminal commit, rollback, or cleanup consume the same entry and
   finalize the session only after `ReleasedTransactionLocks`.

Do not allocate an outer entry around a retained `TrxEntry`. The public
statement path may add only local kind/armed checks and constant-time field
moves. It must not touch `SessionState.lifecycle`, the registry notifier, or
the cleanup queue during a normal checkout/check-in.

Add an internal private-transaction capability derived from an active DDL or
maintenance operation authority. It must:

- verify the stable operation kind;
- inherit the existing operation key;
- allocate only a `TrxID`;
- install/checkout its compact transaction payload through the active entry
  without replacing `SessionOperationSlot::Active`;
- remain controlled by the enclosing foreground operation;
- clear or hand back its transaction obligation at terminal completion; and
- leave the outer entry `ForegroundRunning` rather than publishing session
  idle.

The current catalog and table-maintenance transaction APIs may share the
public transaction execution machinery internally, but terminal routing must
distinguish public versus private ownership from the stable operation kind.
Do not allocate a second `OperationID`, perform a second session reservation,
or create independently synchronized outer/inner entries.

Until the later whole-future-transfer phases, dropping a foreground
DDL/maintenance authority retains the current destructor and compensation
behavior. If its private transaction has already transferred rollback or
completion ownership, the outer entry must remain cleanup/completion-owned
until that exact transaction terminal callback settles it; it must not publish
idle merely because the foreground wrapper disappeared. This foundation does
not claim cancellation safety for the remaining DDL/maintenance generator
state.

Update `Transaction`, `StartedTransaction`, transaction resolution, terminal
claims, and `TrxAttachment` so the operation key is authoritative while
`TrxID` remains authoritative for MVCC and transaction/statement locks.
Retain the current weak public engine handle. An attachment may hold
`EngineRef` and `Arc<SessionState>` only while a foreground/terminal/cleanup
owner is active; checking payload back into the stable entry must not check in
that runtime attachment.

Preserve existing commit and rollback boundaries:

```text
transaction effects complete
  -> bindings released
  -> transaction locks released
  -> ReleasedTransactionLocks consumed
  -> private transaction returns to outer operation, or
     public transaction finalizes outer operation
```

Keep terminal rollback and failed-precommit payload ownership unchanged except
for carrying and validating the enclosing operation key.

### 5. Unify DDL/maintenance lock identity

In `doradb-storage/src/engine.rs`, remove
`EngineInner::next_lock_operation_id`, `next_ddl_operation_id`, and
`next_maintenance_operation_id` plus their tests and initialization.

In `doradb-storage/src/lock/mod.rs`, replace:

```text
LockScope::Ddl(DdlOperationID)
LockScope::Maintenance(MaintenanceOperationID)
```

with:

```text
LockScope::Operation(OperationID)
```

Provide a narrow exact-owner constructor from `SessionOperationKey`.
`LockFamily(SessionID)` continues to distinguish equal raw operation ids from
different sessions. Display and debug output must include both session and
operation ids.

Do not recover operation purpose from the unified scope. `SessionDdlContext`
must be constructed from a typed `Ddl` operation authority, and
DDL-specific explicit-session-lock rejection remains selected by that typed
path. Maintenance helpers must be constructed from a typed `Maintenance`
authority. Bare numeric ids must not select DDL or maintenance policy.

Create one operation owner at outer reservation and pass it through all
logical-lock helpers. `ScopedTableRuntimeAccess` becomes only one acquisition
incarnation's table pin and fresh-lock guards; remove id allocation from
`acquire` and `acquire_for_retry`. A retry drops the prior pin and guards,
settles that acquisition, and reacquires with the same outer owner. Do not
reinterpret releasing current grants as closing the semantic operation scope.

Preserve task 000243's:

- exact-owner isolation;
- same-family directional coverage;
- DDL rejection under a same-session explicit table lock;
- maintenance coexistence under covering explicit locks;
- fresh-guard release ordering;
- pending-wait cancellation; and
- exact `SessionExplicit`, `Transaction`, and `Statement` identities.

Update DDL and maintenance call paths in `session.rs`, `catalog/table.rs`,
`catalog/index.rs`, `table/persistence.rs`, `table/gc.rs`, transaction
retention, and related helpers to accept the outer authority/key rather than
allocating local operation ids.

### 6. Generalize cleanup messages, registry inspection, and waits

Rename transaction-only registry/message concepts to session-operation
terminology where they now cover every kind. The existing physical cleanup
channel and sequential worker remain in place for this phase.

An abandoned transaction cleanup hint must carry:

```text
EngineRef
SessionOperationKey
TrxID
```

The worker resolves the exact active operation key without a registry-level
transaction-id check, verifies the transaction id and claimable state under the
entry mutex, and constructs one cleanup claim. This distinction is required
because one DDL or maintenance entry may host sequential private transactions
under the same operation key.
Missing sessions, replaced operation ids, already-claimed payloads, duplicate
hints, and terminal entries are neutral stale work. Preserve the existing
mandatory `TerminalRollback`, `FailedPrecommit`, and `Stop` messages while
generalizing their attachments and the physical worker interface; do not add a
whole-operation task variant or executor yet. Shutdown inspection queues the
same reason-free cleanup hint only for transactions that are already claimable;
it does not abandon or steal a live foreground transaction.

Replace separate transaction-only shutdown scans with one lazy first-blocker
inspection:

- traverse the DashMap directly and stop at the first active session operation;
- allow the short `DashMap shard read -> lifecycle -> entry` probe because every
  registry mutation releases lifecycle and entry guards first;
- drop the iterator and every state guard before cleanup queueing, waiting,
  notification, or registry removal;
- return only that blocker's currently claimable transaction cleanup hint;
- treat foreground, cleanup, background, completion-owned, and
  failed-retained states as blockers; and
- perform a complete traversal only when proving that no blocker remains.

`Engine::try_shutdown` uses a non-observing first-blocker probe, queues at most
one claimable cleanup hint, and reports `ShutdownBusy` without installing an
event. Blocking `Engine::shutdown` closes admission, drains runtime pins, and
repeatedly installs a listener for the first current blocker, queues at most its
cleanup hint, waits, and rescans.

Replace unconditional `notify_trx_changed` calls and registry-level transition
results with session-local observation:

1. `change_ev` starts `None` under the lifecycle mutex.
2. Explicit close or blocking shutdown installs or reuses `Arc<Event>` only
   when it must wait and creates the listener before releasing the predicate.
3. A lifecycle transition clones the event, when present, while already holding
   that mutex.
4. An entry-only cold transition releases the entry mutex, verifies the same
   active key through the retained `Arc<SessionState>`, and clones its event.
5. `Event::notify(usize::MAX)` happens after releasing lifecycle, entry, and
   explicit-lock state.
6. The event may remain installed because production observation occurs only
   after close request or engine admission closure, when no later ordinary
   operation can be admitted.
7. State methods perform local lock release and notification and return only a
   named `remove_from_registry: bool`; the registry performs idempotent map
   removal.

This arm-before-scan handshake is the lost-wake proof:

```text
transition wins lifecycle mutex first
    -> observer's later scan sees the changed predicate

observer arms and scans first
    -> later wait-relevant transition sees the flag and notifies
```

Normal transaction commit/rollback while open and unobserved must not initialize
the event or call `Event::notify`. Normal statement checkout/check-in remains
completely silent. Test-only waits must install a listener through the exact
session predicate rather than exposing a registry-wide change channel.

### 7. Implement close, abandonment, and diagnostics on the new states

Preserve nonblocking `Session::drop`:

- idle drop releases `SessionExplicit`, closes, and removes the registry entry;
- active drop changes `Open` to `Abandoned` without stealing a live
  transaction or other foreground owner;
- eventual exact-key terminal completion closes and removes the abandoned
  session; and
- repeated abandonment is idempotent.

`Session::close().await` remains idempotent and follows ownership:

- close idle immediately;
- reject `ForegroundAvailable` or caller-owned transaction work with
  `ExistingTransaction`;
- when cleanup or completion ownership is already authoritative, set
  `CloseRequested`, arm observation, and wait without holding lifecycle or
  entry locks;
- retain `ExistingOperation` for a foreground non-transaction operation; and
- define the same wait behavior for later background/completion states without
  implementing background transfer in this phase.

New operation admission never waits behind an active operation. Attach
session id, operation id, kind, state, disposition, and transaction id when
present, formatting attachments only on error/diagnostic paths.

Keep failed-retained entries registry-visible and unavailable. Preserve the
existing engine poison and fatal rollback retention owners; this phase supplies
the stable label and shutdown visibility but does not redesign retention
storage.

### 8. Update living documentation and RFC phase tracking

Update:

- `docs/transaction-system.md` for operation keys, the generalized stable
  entry, public/private transaction roles, checkouts, claims, and terminal
  routing;
- `docs/lock-system.md` for unified `Operation(OperationID)` scope and typed
  purpose authority;
- `docs/engine-component-lifetime.md` for operation registry inspection,
  observation-armed shutdown waits, and the no-strong-cycle rule; and
- any session lifecycle descriptions that still claim only transactions can
  occupy a session.

Treat tasks 000243 and 000244 as immutable historical records. Explain in
living documents that RFC-0025 intentionally supersedes task 000243's typed
global ids and per-access maintenance allocation while preserving its exact
owner behavior.

During `$task-resolve`, synchronize RFC-0025 Phase 1:

- fill `Task Doc` with this document;
- fill `Task Issue` if an issue exists;
- set Phase 1 status and implementation summary from verified outcomes;
- record the concrete entry layout, overflow assertion, notification
  representation, labels, and queue adapter as resolved phase-local choices;
- leave Phases 2 through 7 pending and unchanged in scope; and
- keep related backlogs open unless separately resolved.

### 9. Collect structural and measured performance evidence

Before implementation changes, build and measure `origin/main`; after the
implementation is complete, measure the candidate with the same optimized
toolchain, host, prepared-root shape, environment, and worker configuration.
Do not compare a debug build with an optimized build or reuse a root whose
history differs between the two binaries.

Use at least one unrecorded warm-up and seven measured runs for each cell,
alternating baseline and candidate order where practical. Report every sample,
the median, and dispersion such as interquartile range.

Minimum Phase 1 matrix:

| Workload | Count | Single-session | Contended |
| --- | ---: | --- | --- |
| `stmt-noop` | `1_000_000` | `--threads 1 --sessions 1` | `--threads 4 --sessions 16` |
| `trx-noop` | `100_000` | `--threads 1 --sessions 1` | `--threads 4 --sessions 16` |

Prepare equivalent empty roots and run both workloads with
`--log-sync none`. `stmt-noop` isolates repeated checkout/check-in while
amortizing one reservation and terminal transition per nonempty session.
`trx-noop` isolates reservation, entry allocation, begin/commit, terminal
publication, and notification behavior.

Record throughput and elapsed/average latency in `Implementation Notes` during
`$task-resolve`. A repeatable regression outside measured baseline dispersion
blocks resolution until explained and corrected or RFC-0025 is explicitly
amended. No fixed percentage improvement is required, but `trx-noop` should
also show that removing unconditional notifications did not merely move shared
traffic elsewhere.

Add test-only structural observations, without production counters, proving:

- one stable entry allocation per public transaction and no outer wrapper;
- no success cleanup-queue message;
- no operation change wake for ordinary statement checkout/check-in;
- no operation change wake for unobserved open-session transaction terminal;
- one inherited operation id across private transactions and maintenance
  retries; and
- no coordinator access inside row, index, page, stream-item, or lock-probe
  loops.

### 10. Validate with repository-authoritative tooling

Run:

- `tools/style_audit.rs --diff-base origin/main`;
- `rtk cargo fmt --all -- --check`;
- focused nextest filters for session, transaction, cleanup worker, engine
  shutdown, logical locks, table/index DDL, and maintenance;
- `rtk cargo nextest run --workspace`;
- `rtk cargo nextest run -p doradb-storage --no-default-features --features libaio`;
- `rtk cargo clippy --workspace --all-targets -- -D warnings`; and
- the optimized paired benchmark matrix above.

Use `.config/nextest.toml` as the timeout and hang-detection authority. Add no
ad hoc test timeout framework.

## Implementation Notes

- Replaced the transaction-only session lifecycle with one stable
  `SessionOperationEntry` per effectful public operation. `SessionLifecycle`
  now keeps disposition, one idle/active/closed slot, a plain session-local
  operation-id counter, and an optional change event under one short mutex.
  `OperationID` is crate-private and meaningful only in
  `SessionOperationKey(SessionID, OperationID)`; exhaustion asserts instead of
  wrapping or adding a public error.
- Generalized transaction ownership directly in the stable entry without an
  outer wrapper. Public transactions and inherited DDL/maintenance private
  transactions use `SessionOperationCheckout` and
  `SessionOperationCompletionClaim`; the registry resolves only the stable
  operation key, while the entry atomically validates the current `TrxID` and
  ownership state. Stale and duplicate cleanup hints remain neutral even when
  one outer operation hosts sequential private transactions.
- Unified DDL and maintenance lock identity as
  `LockScope::Operation(OperationID)`. Typed operation capabilities preserve
  DDL-specific policy, equal raw ids remain isolated by `LockFamily(SessionID)`,
  and maintenance retries reuse the enclosing operation owner.
- Replaced the registry-wide notifier with an on-demand session-local
  `EventNotifyOnDrop`. Explicit close and blocking shutdown arm or reuse the
  first blocking session's listener before releasing the predicate;
  transitions notify only when that session is observed. Ordinary statement
  checkout/checkin and unobserved commit/rollback perform no event allocation,
  atomic update, or wake. Shutdown scans lazily and stops at the first blocker.
- Preserved worker-owned terminal rollback, abandoned cleanup, failed
  precommit rollback, lock-release proofs, and fatal retention. Review
  strengthened failed-precommit cleanup with an explicit
  `FailedPrecommitRollbackOutcome`, retained all older payloads after rollback
  access failure, preserved the initiating poison error, and published
  `FailedRetained` before fatal rollback can detach the session entry.
- Review-driven refinements renamed the operation authority and transaction
  boundaries for intent (`SessionOperationCheckout`, `begin_private_trx`, and
  `begin_public_trx`), moved terminal finalization and listener construction to
  `SessionLifecycle`, removed unused registry/session transaction inspection
  APIs, simplified registry finalization/removal results, and documented the
  deliberate exhaustive abandonment match and retry races.
- Updated transaction, lock, engine-lifetime, public-error, unsafe-baseline,
  and RFC documentation to match the implemented identity, ownership, wake,
  and shutdown contracts. Investigation during stress validation found an
  orthogonal metadata-publication race and panic-unsafe component shutdown;
  both remain deferred together in
  `docs/backlogs/000174-atomic-index-metadata-publication-and-panic-safe-shutdown.md`.
- Optimized paired measurements used `origin/main@e60046e` as baseline and
  `session-op-coord@ada83c1` plus the reviewed working-tree fixes as candidate.
  Every cell used a fresh empty `index = "none"` root, `--log-sync none`, one
  unrecorded warm-up, seven recorded runs, and alternating baseline/candidate
  order on the same host.

  | Cell | Revision | Recorded elapsed samples (ms) |
  | --- | --- | --- |
  | `stmt-noop`, 1 thread / 1 session, 1,000,000 ops | baseline | 97.085, 86.600, 85.432, 85.771, 85.699, 86.741, 86.367 |
  | `stmt-noop`, 1 thread / 1 session, 1,000,000 ops | candidate | 84.076, 83.375, 84.317, 86.895, 84.962, 84.906, 84.895 |
  | `stmt-noop`, 4 threads / 16 sessions, 1,000,000 ops | baseline | 122.012, 123.688, 122.011, 120.229, 120.287, 123.754, 121.691 |
  | `stmt-noop`, 4 threads / 16 sessions, 1,000,000 ops | candidate | 97.659, 120.461, 99.063, 98.827, 96.892, 110.163, 98.481 |
  | `trx-noop`, 1 thread / 1 session, 100,000 ops | baseline | 36.081, 38.700, 40.199, 29.981, 38.819, 35.108, 38.833 |
  | `trx-noop`, 1 thread / 1 session, 100,000 ops | candidate | 35.713, 35.182, 35.890, 36.994, 35.367, 37.328, 37.817 |
  | `trx-noop`, 4 threads / 16 sessions, 100,000 ops | baseline | 44.451, 47.478, 47.069, 46.143, 44.510, 42.016, 45.211 |
  | `trx-noop`, 4 threads / 16 sessions, 100,000 ops | candidate | 38.310, 39.310, 39.939, 38.658, 37.225, 40.223, 36.375 |

  | Cell | Revision | Median elapsed / IQR (ms) | Median throughput (ops/s) | Median latency / IQR (ns/op) | Candidate delta |
  | --- | --- | ---: | ---: | ---: | ---: |
  | `stmt-noop`, 1 / 1 | baseline | 86.367 / 1.042 | 11,578,449 | 86.367 / 1.042 | - |
  | `stmt-noop`, 1 / 1 | candidate | 84.895 / 0.887 | 11,779,196 | 84.895 / 0.886 | +1.73% throughput, -1.70% latency |
  | `stmt-noop`, 4 / 16 | baseline | 122.011 / 3.401 | 8,195,988 | 122.011 / 3.401 | - |
  | `stmt-noop`, 4 / 16 | candidate | 98.827 / 12.504 | 10,118,668 | 98.827 / 12.504 | +23.46% throughput, -19.00% latency |
  | `trx-noop`, 1 / 1 | baseline | 38.700 / 3.724 | 2,583,966 | 387.002 / 37.244 | - |
  | `trx-noop`, 1 / 1 | candidate | 35.890 / 1.961 | 2,786,325 | 358.896 / 19.614 | +7.83% throughput, -7.26% latency |
  | `trx-noop`, 4 / 16 | baseline | 45.211 / 2.618 | 2,211,868 | 452.107 / 26.180 | - |
  | `trx-noop`, 4 / 16 | candidate | 38.658 / 2.713 | 2,586,757 | 386.584 / 27.133 | +16.95% throughput, -14.49% latency |

  No cell shows a repeatable regression outside baseline dispersion. The
  statement result preserves the checkout/checkin budget, and the transaction
  result confirms that lazy notification did not move equivalent shared
  traffic elsewhere.
- Validation passed:
  - `tools/style_audit.rs --diff-base origin/main` (18 Rust files, including
    formatting and strict workspace Clippy);
  - focused session-operation, terminal rollback, shutdown-wait, stale
    transaction identity, failed-precommit retention, and fatal rollback
    publication regressions;
  - 100/100 stress iterations of failed-precommit rollback-access retention;
  - `rtk cargo nextest run --workspace` (1,594 tests);
  - `rtk cargo nextest run -p doradb-storage --no-default-features --features libaio`
    (1,501 tests); and
  - `rtk git diff --check`.

## Impacts

- `doradb-storage/src/id.rs`
  - unified crate-private operation identity.
- `doradb-storage/src/error.rs`
  - `ExistingOperation` lifecycle vocabulary and diagnostic mapping.
- `doradb-storage/src/session.rs`
  - orthogonal lifecycle data, reservation/finalization authority, stable
    operation registry, explicit close, abandonment, DDL/maintenance contexts,
    scoped runtime access, and observation-armed change notification.
- `doradb-storage/src/trx/mod.rs`
  - generalized entry states, transaction checkouts/claims, operation-keyed
    handles and attachments, private transaction inheritance, and terminal
    routing.
- `doradb-storage/src/trx/sys.rs`
  - generalized cleanup message identity and sequential worker dispatch.
- `doradb-storage/src/engine.rs`
  - removal of the global lock-operation allocator and operation-aware
    shutdown inspection/waiting.
- `doradb-storage/src/lock/mod.rs`
  - unified `Operation(OperationID)` scope, exact owner construction,
    diagnostics, and preserved typed DDL policy.
- `doradb-storage/src/catalog/table.rs` and
  `doradb-storage/src/catalog/index.rs`
  - outer DDL authority and inherited private transaction identity.
- `doradb-storage/src/table/persistence.rs`,
  `doradb-storage/src/table/gc.rs`, catalog checkpoint, and transaction
  retention helpers
  - one outer maintenance authority across scoped accesses and retries.
- Transaction, lock, engine-lifetime, and session lifecycle documentation
  - living coordinator contracts.
- `docs/rfcs/0025-session-coordinated-cancellation-cleanup-ownership.md`
  - Phase 1 synchronization during `$task-resolve`.

Public method signatures, durable formats, recovery compatibility, workspace
dependencies, and physical worker count do not change. `Transaction` remains a
weak public facade and `Session` remains `Send` but not `Sync`.

Primary risks are:

- a lifecycle/entry lock inversion during terminal or cleanup publication;
- a stale key finalizing a replacement operation;
- private transaction completion publishing outer idle too early;
- an arm/scan race losing the only shutdown or close wake;
- unified lock scope accidentally erasing DDL-specific policy;
- maintenance retry allocating a new semantic id or releasing another
  acquisition incarnation; and
- entry layout or notification traffic regressing successful transaction
  performance.

The transition invariants, deterministic race tests, exact-key checks, and
paired no-op benchmarks are acceptance requirements for those risks.

## Test Cases

1. A new session starts `Open`, `Idle`, with `next_operation_id == 1` and
   change observation disarmed.
2. One session allocates strictly increasing operation ids across public
   transaction, DDL, maintenance, and explicit-lock kinds.
3. Two sessions independently allocate raw operation id one without key or
   lock-owner aliasing.
4. A private DDL/maintenance transaction inherits the outer key and allocates
   a new `TrxID` without changing `next_operation_id`.
5. Maintenance retry and repeated `ScopedTableRuntimeAccess` acquisition reuse
   the same operation owner.
6. Setting the internal next id to the exhaustion boundary triggers the hard
   overflow assertion, never wraps to zero, and exposes no lifecycle error.
7. Idle validation, id increment, entry construction, and installation occur
   under one lifecycle critical section with exactly one entry allocation.
8. A second public transaction returns `ExistingTransaction` with exact key,
   state, disposition, and transaction diagnostics.
9. A second admission against a synthetic foreground/background DDL or
   maintenance entry returns `ExistingOperation`; closed, abandoned, and
   close-requested sessions return `SessionUnavailable`.
10. Entry tests cover every legal state/payload combination and reject
    kind-invalid transitions, double checkout, double checkin, double claim,
    terminal reuse, and payload loss.
11. Public transaction checkout removes exactly one `TrxInner`; ordinary drop
    restores it and preserves the same operation key and `TrxID`.
12. Handle drop while checked out records cleanup intent; checkout return
    publishes cleanup-ready exactly once and queues at most one useful hint.
13. Public commit, explicit rollback, terminal rollback cancellation, abandoned
    cleanup, failed precommit, and fatal retention preserve existing
    transaction-lock release proof ordering.
14. Private transaction commit/rollback clears its child obligation but leaves
    the DDL/maintenance outer entry active until the outer authority finishes.
15. A replaced `(SessionID, OperationID)` does not resolve. When one operation
    entry hosts sequential private transactions, a stale `TrxID` may resolve
    that outer key but cannot abandon, claim, finish, or remove the replacement
    transaction; duplicate hints are neutral.
16. The stable entry and checked-in transaction payload retain no `EngineRef`;
    dropping all intentional runtime pins leaves no registry-to-engine strong
    cycle.
17. DDL and maintenance owners render the same unified lock-scope shape while
    their stable kinds and typed contexts remain distinct.
18. DDL under a same-session explicit target-table lock remains rejected, and
    maintenance under a covering explicit lock retains its separate exact
    operation claim.
19. Equal raw operation ids in different session families never share,
    convert, release, or cancel each other's grants or waiters.
20. Create/drop table and create/drop index retain one operation owner through
    all target locks and their private catalog transaction.
21. Checkpoint, retention, freeze, and MemIndex cleanup retain one maintenance
    operation owner through internal waits/retries; standalone observer waits
    allocate no operation id.
22. Normal `Transaction::exec`, `StreamStmt::next`, row/index/page loops, and
    logical-lock probes perform no additional coordinator access or
    notification.
23. An unobserved open-session commit or rollback does not initialize or notify
    the lifecycle event.
24. Shutdown listener installation racing commit, rollback, handle drop, checked-out
    abandoned return, cleanup claim, and terminal completion either observes
    the new predicate or receives a wake.
25. Explicit close racing cleanup-ready, cleanup-running, completion-owned,
    terminal, and failed-retained publication has no lost wake and holds no
    lifecycle or entry mutex while blocked.
26. Explicit close rejects a still-live public transaction, closes idle
    idempotently, and removes a terminal close-requested session only after
    exact operation completion.
27. Session drop is nonblocking, preserves a live transaction owner's ability
    to commit or roll back, and releases session-explicit locks only at idle or
    exact terminal close.
28. Shutdown treats foreground DDL/maintenance and synthetic later background
    states as active, arms only the first current blocker, queues at most that
    blocker's claimable transaction cleanup, and never waits under a registry
    guard.
29. Cleanup `Stop`, terminal rollback, and failed-precommit behavior remain
    sequential and unchanged except for exact operation-key validation.
30. Existing session, transaction, statement, stream, DDL, maintenance, lock,
    shutdown, poison, and recovery tests remain behaviorally green.
31. Paired optimized `stmt-noop` results show no repeatable checkout/check-in
    regression outside baseline dispersion in both matrix configurations.
32. Paired optimized `trx-noop` results show no repeatable reservation/terminal
    regression outside baseline dispersion and confirm normal completion no
    longer performs unconditional lifecycle notification.
33. Living documentation and the RFC Phase 1 resolution summary agree with the
    implemented identities, states, wake protocol, benchmark evidence, and
    following-phase constraints.

## Open Questions

No blocking questions remain for this task.

Phase 2 owns statement cancellation settlement and transaction cancellation
classification. Phase 3 owns the cooperative executor and concurrent mandatory
task dispatch. Phases 4 through 6 own production whole-future transfer and
operation-specific irreversible-gate migration. Phase 7 owns final failure,
shutdown, diagnostics, and readiness consolidation. Backlog 000171 remains
responsible for physical lock-family ownership and `LockScopeState`.

The metadata-history/runtime-layout publication race and panic-unsafe partial
component shutdown found during task stress validation are intentionally
deferred to
`docs/backlogs/000174-atomic-index-metadata-publication-and-panic-safe-shutdown.md`.
