---
id: 000282
title: Shared snapshot preparation
status: proposal
created: 2026-08-24
github_issue: 1013
---

# Task: Shared snapshot preparation

## Summary

Implement RFC-0030 Phase 2 as a focused, crate-private shared-snapshot
preparation workflow. One consuming `ReadSnapshotBuilder` registers a snapshot
timestamp, acquires metadata-S for the complete query-level user-table set,
binds snapshot-visible metadata to current table runtimes and layouts, captures
owned scan roots, and atomically publishes one cloneable `ReadSnapshot` backed
by the stable session registry entry.

The registry remains the canonical resource owner. Weak public facades retain
no table, root, STS-registration, logical-lock, stable-entry, or strong runtime
owner. Counted internal checkouts lend the immutable frozen core and expose a
table/root view borrowed from the exact checkout. Explicit close, final-facade
drop, session close or abandonment, and engine shutdown seal the snapshot and
run one ordered terminal cleanup after accepted checkouts return.

Deterministic table-scan planning is deliberately removed from this task. The
implementation must split the parent RFC so a new Phase 3 owns
`TableScanOptions`, worklist capture, coverage validation, checked weighting,
partitioning, value-only plans, and plan-publication races. The existing row
stream and benchmark phases then become Phases 4 and 5.

## Context

Issue Labels:

- type:task
- priority:high
- codex

Parent RFC:

- `docs/rfcs/0030-shared-read-snapshots-parallel-table-scan.md`

RFC Phase:

- Phase 2: Shared snapshot preparation, after the phase split specified by
  this task.

RFC-0030 Phase 1 is implemented by
`docs/tasks/000281-transaction-neutral-scan-read-view-owned-root-binding.md`.
It supplies the ownerless `MvccReadView`, transaction-neutral
`MvccVisibility`, scan-only `TableScanRuntime`, `OwnedTableScanRoot`,
`CheckedOutTableScanRoot<'_>`, `TableScanRootView`, and captured-worklist seam.
The existing transaction table stream is their production consumer.

The session registry currently stores one `Arc<SessionOperationEntry>` in its
active slot. That entry is deliberately transaction-centric: it contains an
optional `TrxID`, checked-in `TrxInner`, private-transaction positions,
transaction cleanup intent, and returned family authority. DDL, maintenance,
and explicit-lock operations use the same stable entry for outer ownership but
do not install another durable payload. A read snapshot needs different
building, shared-checkout, draining, and terminal states, so this task adds a
typed snapshot entry beside the established entry rather than combining both
payload state spaces inside one mutex.

The storage engine already has the required lower-level ownership mechanisms:

- `SessionState` owns the pointer-stable operation slot, session disposition,
  one boxed `FamilyLockAuthority`, and exact-key terminal publication.
- `FamilyLockState` and `LockScopeState` serialize all lock mutation within one
  session family and provide token-exact pending-acquisition cancellation and
  exact-scope cleanup.
- `PrivateSnapshot` demonstrates RAII registration in the active GC horizon,
  but is maintenance-only and is not a session-registry payload.
- transaction table admission acquires metadata-S before resolving
  snapshot-visible metadata and authoritative current runtime/layout state.
- the Phase 1 owned scan root has no direct `TableScanRootView`
  implementation; only its checkout-borrowed view exposes root fields.

The query or compute engine is expected to optimize a complete physical plan
before beginning the storage snapshot. It can therefore enumerate all user
tables once and pass the complete set to the consuming builder. Acquisition
preserves first-occurrence caller order after deduplication, but that sequence
is an implementation detail rather than a new lock-order or deadlock
contract. Late or dynamic table registration is intentionally unsupported.

This task originates directly from RFC-0030 and the approved phase-split
decision. It has no source backlog.

## Goals

1. Add a distinct `ReadSnapshot` session-operation kind and one typed,
   pointer-stable registry entry without changing the existing transaction
   entry's payload state machine.
2. Implement a one-shot `ReadSnapshotBuilder: Send + !Sync + !Clone` whose
   successful begin call owns one registered STS before any table root can be
   captured.
3. Acquire metadata-S for one nonempty, deduplicated set of user tables through
   the existing serialized family authority and operation lock scope.
4. Bind each STS-visible table definition to a compatible current `Table` and
   `TableRuntimeLayout`, capture one `OwnedTableScanRoot`, and freeze the
   complete set atomically or publish nothing.
5. Implement a weak `ReadSnapshot: Clone + Send + Sync` and a resource-free
   shared facade-liveness group that Phase 3 plans can later join.
6. Implement counted `ReadSnapshotCheckout` values that lend the exact frozen
   core, provide checkout-borrowed table/layout/root access, and can coexist
   across threads without mutating family lock state.
7. Linearize build checkout, `Ready` publication, shared checkout, close,
   session abandonment, and shutdown against the exact session disposition and
   operation key without holding a registry, lifecycle, or entry lock across
   an await.
8. Make a pending metadata-lock wait observe exact snapshot abort caused by
   close, session abandonment, or shutdown and synchronously cancel its queued
   or provisional claim when that abort wins.
9. Make explicit close consuming, group-wide, idempotent, and cancellation safe
   after its close request; make final-facade drop request the same drain
   without waiting.
10. Enforce checkout-local pins before registry roots, roots before active-STS
    deregistration, snapshot metadata locks before family-authority return, and
    operation-scope cleanup before session terminal publication.
11. Exercise a complete, real
    `begin_read_snapshot -> acquire_tables -> shared checkout -> close`
    workflow with deterministic lifecycle and reclamation tests.
12. Update RFC-0030 to split deterministic planning into a new Phase 3 and
    renumber the row-stream and benchmark phases without changing their
    semantic contracts.

## Non-Goals

1. No late `acquire_table`, dynamic table registration, mutable frozen table
   set, or concurrent lock mutation within one session family.
2. No `TableScanOptions`, `TableScanPlan`, planning checkout/publication,
   projection validation, or dormant plan facade.
3. No cold/hot worklist capture, `TableScanUnit`, physical coverage
   validation, unit weighting, prefix sums, partition count, or cut-point
   algorithm.
4. No partition `open`, page or block loading, MVCC row filtering, projection
   into `Vec<Val>`, or row-oriented stream.
5. No snapshot-wide first-execution-error signal, peer abort, execution
   checkout, or failed-drain state; those require real Phase 4 streams.
6. No Arrow, vectorized decoding, DataFusion, predicate pushdown, callback,
   aggregation, join, or engine-owned query scheduler.
7. No public crate-root export or externally supported incomplete scan API.
8. No transaction ID, shared transaction status, undo, redo, effects,
   commit/rollback, or read-your-own-write identity for `ReadSnapshot`.
9. No transaction, recovery, checkpoint, GC, table-file, LWC, block-index, or
   persisted-format change.
10. No snapshot-specific metadata-lock order, timeout, deadlock victim,
    lock-manager cancellation protocol, or general user-cancellation token.
11. No replacement or lifetime erasure of `TableRootSnapshot<'_>` and no
    independently usable accessor on `OwnedTableScanRoot`.
12. No test-runner or `.config/nextest.toml` change.

## Rejected Alternatives

1. **Keep deterministic planning in Phase 2.** The combined scope would add
   worklist capture, scan-unit validation, checked weighting, partitioning,
   plan ownership, and another publication race to an already substantial
   registry, lock, reclamation, close, abandonment, and shutdown lifecycle.
   Splitting planning gives each task one coherent correctness boundary and
   leaves the snapshot task reviewable.
2. **Allow late or dynamic table registration.** The optimized query-level
   physical plan can enumerate the complete table set before snapshot begin.
   Late acquisition would either mutate a core already shared with workers or
   introduce another serialized family-lock coordinator and new
   planning-versus-table-set races. Version 1 freezes one all-or-nothing set.
3. **Add snapshot fields and states directly to `SessionOperationEntryInner`.**
   That would place transaction identity, `TrxInner`, private-transaction
   positions, snapshot build payload, immutable read core, and shared-checkout
   counts in one mutex with many structurally invalid combinations. A typed
   active-operation wrapper preserves the mature transaction state machine and
   gives snapshots a closed state space.

## Plan

### RFC phase contract

Update `docs/rfcs/0030-shared-read-snapshots-parallel-table-scan.md` as part of
this task before implementation is considered complete:

1. Add a conversation/design input recording that the complete optimized
   physical plan supplies the one-shot table set and that planning was split
   to keep snapshot lifecycle work focused. State that this decision
   supersedes the earlier implementation-phase choice to merge freeze and
   planning.
2. Rewrite Phase 2 as **Shared snapshot preparation** with the scope, goals,
   non-goals, choices, and verification in this document. Set its task doc to
   this path; issue and status synchronization remain owned by the issue and
   task-resolution workflows.
3. Insert **Phase 3: Deterministic table-scan planning**. Its prerequisite is
   Phase 2's complete shared snapshot, counted checkout, immutable table
   bindings, and checkout-borrowed root access. Move into it:
   `TableScanOptions`, planning checkout/return, projection validation,
   worklist capture, cold/hot scan units, coverage validation, checked cold-row
   and hot-reserved-span weights, deterministic contiguous partitions,
   empty/reduced partition behavior, immutable value-only plans, facade-group
   participation, and final plan-publication ordering.
4. Give new Phase 3 non-goals covering partition `open`, page loading, row
   output, MVCC execution, streams, failure propagation, and API export. Its
   verification must cover deterministic complete unit coverage, weights,
   target counts, empty input, value-only ownership, concurrent/cancelled
   planners, and publication races.
5. Renumber the current parallel row-oriented scan phase to Phase 4 and change
   its prerequisite to Phase 3 immutable plan descriptors. Preserve its real
   stream, spawnability, MVCC, failure, and public-export contracts.
6. Renumber the benchmark phase to Phase 5 and change all prose references to
   its performance responsibility from Phase 4 to Phase 5.
7. Change prose that keeps the incomplete API private until Phase 3 opens real
   streams so that it now names Phase 4.
8. Preserve Phase 1 and all overall RFC invariants, alternatives, non-goals,
   and later execution/benchmark semantics not directly affected by the split.

After the split, the phase sequence is:

```text
Phase 1  transaction-neutral read/root primitives       done
Phase 2  shared snapshot preparation                    this task
Phase 3  deterministic table-scan planning              future task
Phase 4  parallel row-oriented table scan               future task
Phase 5  benchmark and performance proof                future task
```

### Crate-private object model

Add a new `doradb-storage/src/trx/read_snapshot.rs` module. Keep every new
facade and method crate-private until Phase 4 lands a real public row stream.
The Phase 2 interface is:

```rust,ignore
pub(crate) struct ReadSnapshotBuilder { /* weak, armed single owner */ }
pub(crate) struct ReadSnapshot { /* shared weak facade group */ }
pub(crate) struct ReadSnapshotCheckout { /* counted internal owner */ }

impl Session {
    pub(crate) fn begin_read_snapshot(
        &mut self,
    ) -> Result<ReadSnapshotBuilder>;
}

impl ReadSnapshotBuilder {
    pub(crate) fn sts(&self) -> TrxID;

    pub(crate) async fn acquire_tables<I>(
        self,
        table_ids: I,
    ) -> QuadResult<ReadSnapshot>
    where
        I: IntoIterator<Item = TableID>;
}

impl ReadSnapshot {
    pub(crate) fn sts(&self) -> TrxID;
    pub(crate) fn checkout(&self) -> LifecycleOrFatalResult<ReadSnapshotCheckout>;
    pub(crate) async fn close(self) -> Result<()>;
}

impl ReadSnapshotCheckout {
    pub(crate) fn table(
        &self,
        table_id: TableID,
    ) -> OperationResult<CheckedOutSnapshotTable<'_>>;
}
```

`ReadSnapshotBuilder` contains weak session reachability, operation key, copied
STS, and local drop-suppression state. Use `Cell` or an equivalent marker to
keep it `Send + !Sync`; do not implement `Clone`.

`ReadSnapshot` contains one `Arc<ReadSnapshotFacadeGroup>`. The group contains
only weak session reachability, exact operation key, copied STS, and atomic
close-request state. Its final `Drop` performs best-effort exact-key terminal
upgrade and requests close without waiting. It must not contain or reach a
strong `SessionState`, `EngineCore`, operation entry, table/layout/root,
active-STS registration, lock scope, or family authority.

Phase 3 may clone the same facade-group `Arc` into plans. Phase 2 must not make
the group snapshot-specific in a way that prevents that extension.

### Typed active session operation

Change `SessionOperationSlot::Active` to store a typed internal wrapper:

```rust,ignore
enum ActiveSessionOperation {
    Operation(Arc<SessionOperationEntry>),
    ReadSnapshot(Arc<ReadSnapshotEntry>),
}
```

Add `SessionOperationKind::ReadSnapshot` for stable diagnostics and existing
operation admission errors. The wrapper delegates exact key, kind, diagnostic
snapshot, standard transaction cleanup candidate, snapshot cleanup candidate,
and terminal notification behavior. Keep the existing
`SessionOperationEntryInner`, `SessionOperationState`, transaction checkouts,
private-transaction transitions, and terminal proofs unchanged.

Use a separate diagnostic phase enum for snapshot states rather than adding
unreachable snapshot variants to transaction-state matches. Preserve the
global lock order `SessionState.lifecycle -> operation entry`; no snapshot
method may acquire the lifecycle lock while already holding the snapshot entry
mutex.

### Snapshot state and payload ownership

The pointer-stable `ReadSnapshotEntry` contains immutable key/STS identity, one
small mutex, and a lazily allocated abort/change event. Its state distinguishes
at least:

```rust,ignore
enum ReadSnapshotEntryState {
    BuildingAvailable(ReadSnapshotBuildCore),
    BuildingCheckedOut {
        abort: Option<ReadSnapshotDrainReason>,
    },
    Ready {
        payload: ReadSnapshotReadyPayload,
        active_checkouts: usize,
    },
    Draining {
        payload: ReadSnapshotReadyPayload,
        active_checkouts: usize,
        reason: ReadSnapshotDrainReason,
    },
    CompletingAvailable(ReadSnapshotTerminalPayload),
    CompletingCheckedOut,
    Terminal,
}
```

`BuildingCheckedOut` leaves the core in the exclusive RAII build checkout.
Close, abandonment, or shutdown records sticky abort and wakes the exact build
wait. A checked-in build or ready payload with no active checkout moves to
`CompletingAvailable`; this keeps terminal ownership registry-authoritative
until one exact resolver claims it. Only one claimant may perform cleanup.

`Ready` remains ready when `active_checkouts` returns to zero. Only explicit
close, final-facade close, session close/abandonment, or shutdown changes it to
`Draining`. Poison rejects new healthy checkout but does not independently
drain a checked-in ready snapshot.

Use checked active-count increment and assertion-bearing underflow protection.
Do not add Phase 4 execution failure state or a dormant
`SnapshotExecutionControl`.

### Active STS and build core

Factor the active-GC registration currently embedded in `PrivateSnapshot` into
one crate-private `ActiveSnapshotRegistration` that owns the transaction-system
guard, GC bucket, and STS and deregisters on `Drop`. Preserve
`PrivateSnapshot` behavior by wrapping or composing this owner.

`Session::begin_read_snapshot` performs ordinary lifecycle admission, exact
session validation, and health validation, then:

1. reserves the typed snapshot operation and takes the one boxed family
   authority from the idle session;
2. creates `LockScopeState` with `LockOwner::operation(operation_key)`;
3. registers one active STS;
4. creates `MvccReadView::ownerless(sts)`;
5. checks an empty `ReadSnapshotBuildCore` into `BuildingAvailable`; and
6. releases short-lived operation-start admission only after the stable entry
   owns the complete core.

The build core owns:

```rust,ignore
struct ReadSnapshotBuildCore {
    bindings: FastHashMap<TableID, SnapshotTableBinding>,
    read_view: MvccReadView,
    active_sts: ActiveSnapshotRegistration,
    locks: ReadSnapshotLockOwner,
}

struct ReadSnapshotLockOwner {
    authority: Box<FamilyLockAuthority>,
    metadata_scope: LockScopeState,
}
```

Builder drop or an error before exclusive checkout claims the checked-in build
core through exact-key terminal resolution. An unpolled consuming future owns
no runtime checkout; dropping it drops the builder and requests the same
cleanup.

### Complete table-set acquisition

On first poll, `acquire_tables` validates and collects its iterator into a
`Vec<TableID>` while a `FastHashSet` preserves only each ID's first occurrence.
Reject an empty deduplicated set with
`OperationError::InvalidReadSnapshotInput`; reject catalog IDs consistently as
`OperationError::TableNotFound`. Do not sort the IDs or define a public lock
order.

Normal healthy admission resolves the exact active snapshot and atomically
moves its core from `BuildingAvailable` into one
`ReadSnapshotBuildCheckout`. The checkout owns a strong operation-local
`SessionRuntime` attachment and leaves the stable entry visibly
`BuildingCheckedOut`. No lifecycle or entry lock remains held during metadata
waits or catalog/root work.

For each table ID:

1. register the entry abort listener before checking sticky abort;
2. acquire `TableMetadata(table_id)` in `Shared` mode through the build core's
   family authority and operation scope;
3. race a genuinely blocked acquisition against exact snapshot abort while
   preserving the lock layer's existing poison-aware behavior;
4. if abort wins, drop the acquisition future so its existing
   `PendingClaimGuard` synchronously removes queued, provisional, or partially
   published state;
5. if acquisition wins, recheck sticky abort before resolving metadata;
6. resolve STS-visible live metadata and authoritative current table/layout
   under the accepted metadata claim;
7. capture one registration-gated `OwnedTableScanRoot`; and
8. install one immutable binding, asserting that deduplication prevents
   replacement.

Extract the common metadata/current-runtime resolution from
`trx/admission.rs` into a narrow crate-private read-binding helper. Preserve
all existing transaction hit/miss, write-version, index, cache, and error
behavior. The snapshot binding retains snapshot-visible metadata, current
effective metadata identity as needed for diagnostics, `Arc<Table>`,
`Arc<TableRuntimeLayout>`, and the owned root. It acquires no
`TableData(IntentShared)` claim.

The operation scope begins empty and input IDs are deduplicated, so all
accepted metadata claims belong to this build. Any error or cancellation
closes the whole scope after the currently pending acquisition future has
dropped; no dynamically sized `FreshClaimsGuard` is required.

### Registration-gated root capture

Add a table/storage method that accepts `&ActiveSnapshotRegistration` and
copies an `OwnedTableScanRoot` from one active-root observation. Tighten the
owned-root constructor visibility so production snapshot capture goes through
that method instead of `active_root_unchecked`.

Add the minimum crate-private constructor needed for
`ReadSnapshotCheckout::table` to return `CheckedOutTableScanRoot<'_>`. The
returned lifetime is inferred from the binding borrowed through the checkout;
`OwnedTableScanRoot` itself still has no field getter and does not implement
`TableScanRootView`.

### Atomic freeze and publication

After the last binding succeeds, move the build core into:

```rust,ignore
struct FrozenReadSnapshotCore {
    bindings: FastHashMap<TableID, SnapshotTableBinding>,
    read_view: MvccReadView,
    // Declared after bindings so roots and table/layout owners drop first.
    active_sts: ActiveSnapshotRegistration,
}

struct ReadSnapshotReadyPayload {
    read_core: Arc<FrozenReadSnapshotCore>,
    locks: ReadSnapshotLockOwner,
}
```

Before `Ready` publication, recheck engine health and shutdown state without an
intervening await. Under the session lifecycle lock and then entry mutex,
require the exact active key, session disposition `Open`,
`BuildingCheckedOut`, and no sticky abort. The transition either checks the
complete payload into `Ready { active_checkouts: 0 }` and returns a weak
snapshot facade, or checks it into terminal cleanup and publishes no facade.

If `Ready` wins immediately before abandonment or shutdown, returning a facade
that is already draining is permitted. If abandonment or shutdown wins first,
no usable facade may be returned.

### Shared checkout and borrowed table view

`ReadSnapshot::checkout` uses ordinary lifecycle admission and healthy-runtime
validation. In one `lifecycle -> entry` transition, require session `Open`, the
exact snapshot `Ready`, and an open facade group; checked-increment
`active_checkouts` and clone only the frozen `read_core` into a
`ReadSnapshotCheckout`. The checkout owns an operation-local runtime
attachment but never receives the metadata scope or family authority.

`ReadSnapshotCheckout::table` looks up the immutable binding and returns
`OperationError::TableNotAcquired` when absent. `CheckedOutSnapshotTable<'_>`
borrows table/layout owners and constructs the usable checked-out scan-root
view from the same binding. No root view, borrowed table/layout reference, or
read-core pin may outlive the checkout.

On normal or cancellation drop, the checkout explicitly destroys all local
views and its `Arc<FrozenReadSnapshotCore>` before decrementing the active
count. Returning to `Ready` with count zero keeps the registry payload alive.
Returning the final checkout while `Draining` changes the entry to
`CompletingAvailable` and triggers or exposes the sole terminal claim.

### Close and facade liveness

`ReadSnapshot::close(self)` first atomically marks the shared facade group
closed, then performs exact-key terminal upgrade without requiring new healthy
foreground admission. It requests close synchronously before its first wait,
so dropping the future after that boundary cannot reopen the snapshot. It then
loops on the session operation-change event until the exact key is terminal or
absent. An absent exact key is successful idempotence; a replacement operation
must never be mutated.

One clone's close invalidates every clone and, in Phase 3, every plan sharing
the group. Concurrent close callers request the same drain and wait on the same
terminal edge. Close waits only for accepted internal checkouts, not dormant
facades. Final facade-group drop performs the same request best-effort without
waiting.

### Session abandonment, close, and shutdown

Extend `SessionState::request_close` so an active read snapshot is helpful
rather than rejected as an ordinary voluntary foreground operation. It marks
the session `CloseRequested`, seals or aborts the exact snapshot, waits for
checked-out work, runs terminal cleanup, and then closes the session. Preserve
the current ordinary healthy-admission requirement of public
`Session::close`.

Extend `SessionState::abandon` so dropping the public `Session` changes the
disposition to `Abandoned` and requests snapshot abort/drain without blocking.
A checked-in build or ready payload with zero checkouts becomes immediately
claimable. A build checkout receives sticky abort and a wake but remains a
legitimate shutdown blocker until its caller polls or drops the future. A
shared checkout accepted before abandonment may finish local safe work, but no
later checkout is admitted.

Generalize `SessionCleanupRequest` to distinguish exact transaction cleanup
from exact read-snapshot cleanup. Shutdown registry inspection must request
abort/drain even when the session disposition was still `Open` before engine
shutdown. Checked-in snapshot terminal cleanup is synchronous and requires no
mandatory-runtime job; transaction rollback cleanup retains its existing
mandatory path.

`try_shutdown` may perform or queue one snapshot cleanup and still report the
blocker sampled by that call; a later call can observe completion. Blocking
shutdown installs the exact lifecycle listener before cleanup, performs no
cleanup while holding registry/lifecycle/entry guards, waits, and rescans.
Forgotten weak facades never block shutdown. A checked-out build or shared
checkout remains visible until return; shutdown never force-drops it.

### Terminal cleanup

Use one non-cloneable `ReadSnapshotTerminalClaim` to atomically move a
`ReadSnapshotTerminalPayload` from `CompletingAvailable` to
`CompletingCheckedOut`. It retains the exact entry and a strong terminal
runtime attachment while performing this synchronous sequence:

```text
drop checkout-local root/table/layout/read-core pins
-> take registry-owned build or ready payload
-> drop table/layout bindings and OwnedTableScanRoot values
-> drop MvccReadView and deregister ActiveSnapshotRegistration
-> close the snapshot operation's metadata LockScopeState
-> recover and assert-idle the exact FamilyLockAuthority
-> publish the exact snapshot entry Terminal
-> finalize the session slot as Idle or Closed for its disposition
-> close session-explicit claims when the session is closing/abandoned
-> notify close and shutdown observers
```

Do not hold a registry guard, session lifecycle lock, or entry mutex while
dropping bindings/registration or closing logical locks. Use field order and
explicit `Option::take` boundaries so checkout-local pins drop before active
count return and frozen bindings drop before STS deregistration. Snapshot
terminal cleanup has no recoverable or snapshot-originated fatal failure: phase,
count, identity, and ownership mismatches are internal invariants and use
release assertions. An unexpectedly dropped terminal claim must preserve its
complete unconsumed payload before surfacing that invariant; it must not convert
the programming error into engine poison.

### Errors, documentation, and diagnostics

Add fieldless classifications with request-specific facts in attachments:

- `LifecycleError::ReadSnapshotUnavailable` for stale, closed, draining,
  abandoned, or otherwise non-admissible exact snapshot identity;
- `OperationError::InvalidReadSnapshotInput` for an empty deduplicated table
  set; and
- `OperationError::TableNotAcquired` for checkout lookup outside the frozen
  table set.

Retain `OperationError::TableNotFound` for catalog IDs and missing/tombstoned
tables and existing schema-change diagnostics for incompatible current state.
Do not add planning, partition-index, or peer-abort variants in this phase.

Update:

- `docs/transaction-system.md` with registry-owned snapshot identity, table
  binding, shared checkout, weak facade, and cleanup ordering;
- `docs/lock-system.md` with the snapshot operation scope and the fact that
  exact snapshot abort wraps, but does not change, token-exact lock-manager
  cancellation; and
- `docs/shutdown-and-poison.md` with the new snapshot metadata-acquisition wait
  family, its progress producer, primary wake, poison/shutdown behavior,
  acceptance edge, and cleanup owner.

Do not document or re-export an incomplete public snapshot API in
`docs/public-api.md` or `lib.rs`; Phase 4 owns public rollout with real streams.

## Implementation Notes

## Impacts

- `doradb-storage/src/trx/read_snapshot.rs` becomes the private owner of the
  builder, snapshot facade, facade group, entry state, build/shared checkouts,
  frozen core, bindings, and terminal claim.
- `doradb-storage/src/session.rs` gains typed active-operation dispatch and
  snapshot-specific begin, checkout, close, abandonment, terminal, and
  shutdown coordination while retaining its single-slot model.
- `doradb-storage/src/engine.rs` dispatches transaction or snapshot cleanup
  hints during try/blocking shutdown.
- `doradb-storage/src/trx/mod.rs` adds the snapshot operation kind and module
  exports but keeps transaction entry payload states and transaction checkout
  behavior unchanged.
- `doradb-storage/src/trx/read_snapshot.rs` owns the reusable active-STS RAII
  owner and preserves maintenance `PrivateSnapshot` semantics beside the
  shared-snapshot lifecycle that consumes the same registration.
- `doradb-storage/src/trx/admission.rs` factors read-binding resolution without
  changing transaction lock retention, cached binding, index validation, or
  write compatibility.
- `doradb-storage/src/table/scan_root.rs`, `table/storage.rs`, and `table/mod.rs`
  add registration-gated owned-root capture and exact-checkout view
  construction without weakening `TableRootSnapshot<'_>`.
- `doradb-storage/src/error.rs` adds the snapshot lifecycle and input
  classifications required by this phase.
- Ready snapshots retain one active GC timestamp, all bound table/layout/root
  owners, and metadata-S claims until explicit or implicit drain. Long-lived
  builders and snapshots can therefore delay root, undo, dropped-table, and
  metadata-X progress by design.
- Session and engine shutdown gain a synchronous, registry-authoritative
  snapshot cleanup kind; dormant weak facades remain non-blocking.
- No public API, dependency, persisted format, recovery protocol, benchmark,
  or operational configuration changes in this phase.

## Test Cases

1. Compile-check `ReadSnapshotBuilder: Send + !Sync + !Clone` and
   `ReadSnapshot: Clone + Send + Sync`. White-box assert that builder, snapshot,
   and facade group contain no strong session/runtime, stable-entry,
   table/layout/root, STS-registration, scope, or family-authority owner.
2. Begin a snapshot and assert one stable `ReadSnapshot` operation key remains
   registry-visible from `BuildingAvailable` through terminal publication.
   Verify STS registration occurs before any root-capture hook.
3. Drop an unpolled builder, an unpolled consuming acquisition future, and a
   checked-in builder after ordinary validation failure. Prove immediate exact
   cleanup and no leaked active STS or lock claim.
4. Reject an empty iterator and catalog IDs. Deduplicate repeated IDs by first
   occurrence and prove that each accepted user table has exactly one metadata-S
   operation claim and no table-data claim.
5. Acquire at least two tables under one snapshot. Verify each binding's
   STS-visible metadata, current table/layout identity, captured root
   timestamps, pivot, and column root. Commit catalog or data changes after the
   snapshot STS and prove every binding retains the same reader STS.
6. Fail after one or more metadata grants because a later table is missing,
   dropped, incompatible, poisoned, or cancelled. Prove the accepted prefix,
   bindings, roots, STS, operation scope, and family authority unwind exactly
   once and no partial snapshot is published.
7. Hold metadata-X from another family, poll snapshot metadata-S until its
   exact waiter is installed, then request close, drop the public session, and
   start shutdown in separate deterministic cases. Prove listener registration
   precedes the abort recheck, the pending guard cancels queued/provisional
   state, and no blocker release is needed to make the snapshot future runnable.
8. Arrange abandonment immediately before build checkout, while the checkout
   owns the core, after an accepted lock prefix, and immediately before
   `Ready`. Prove sticky abort, prompt prefix unwind, and no facade publication
   when abandonment wins.
9. Race final `Ready` publication and abandonment in both directions. An
   abandonment-first result publishes no snapshot; a `Ready`-first result may
   return an immediately draining facade but cannot enter storage afterward.
10. Cancel the build future at every await boundary and retain an accepted but
    unpolled build future after its abort wake. Prove RAII cleanup when dropped
    and correct shutdown blocker reporting while deliberately retained.
11. Open multiple shared checkouts from snapshot clones on different threads.
    Verify exact active counts, immutable core identity, ownerless read view,
    and table/root views borrowed from each checkout. No checkout receives or
    mutates family lock authority.
12. Request an acquired table and an absent table through checkout. Return the
    exact binding for the first and typed `TableNotAcquired` for the second.
    Preserve the negative `OwnedTableScanRoot: !TableScanRootView` assertion
    and prove checked-out views cannot escape their checkout borrow.
13. Return all current checkouts while the entry remains `Ready`; prove count
    zero retains the active STS, metadata locks, bindings, roots, and ability to
    admit a later checkout.
14. Poll explicit close while several checkouts remain. Prove the first close
    request seals new checkout, accepted checkouts remain safe, the last return
    creates the sole terminal cleanup opportunity, and every close waiter sees
    completion.
15. Call close concurrently through multiple clones and cancel one close future
    after its request. Prove group-wide idempotence, no reopening, no
    replacement-key mutation, and eventual completion independent of dormant
    clones.
16. Drop snapshot clones in every order. Prove non-final drops are neutral,
    final facade-group drop requests close without waiting, and forgotten
    facades do not prevent session close, abandonment, or shutdown-driven
    drain.
17. Verify public `Session::close` drains an active snapshot and then closes the
    session. Verify public session drop is nonblocking, marks abandonment, and
    either cleans a checked-in payload immediately or waits for accepted
    checkout/build ownership to return.
18. Verify poison rejects new build/shared healthy admission, a build wait
    propagates Fatal after cancelling its pending claim, poison alone does not
    drain an already-ready checked-in snapshot, and terminal snapshot close
    remains available through established cleanup authority.
19. Verify `try_shutdown` requests snapshot abort/drain and reports Busy for a
    sampled live build/shared checkout. Verify a checked-in builder or ready
    zero-checkout snapshot is synchronously cleaned despite retained weak
    facades, a later try succeeds, and blocking shutdown arms its listener
    before cleanup and rescan.
20. Hold metadata-X DDL from another session while the snapshot is ready. Prove
    it waits, then progresses only after checkout views and core pins drop,
    registry bindings/roots drop, STS deregisters, and the metadata operation
    scope closes.
21. Install deterministic drop hooks around checkout-local pins, bindings,
    owned roots, active registration, operation scope, family authority, and
    session terminal publication. Assert the complete required order for
    builder error, explicit close, final-facade close, abandonment, and
    shutdown cleanup.
22. Preserve existing `PrivateSnapshot`, `TrxReadProof`, transaction table
    admission, `SessionOperationEntry`, transaction stream, DDL, maintenance,
    logical-lock waiter, session close, and engine shutdown test behavior.
23. Use production predicates, events, channels, barriers, and test hooks for
    every race. Do not use scheduler sleeps or elapsed time to establish a
    state transition. Stress focused build/close/abandonment/shutdown races
    without nextest retries.
24. Run focused coverage for changed snapshot, session, transaction-admission,
    lock integration, and scan-root modules. Meet the 80% focused review bar or
    explain definition-only lines through covered production consumers.
25. Run the authoritative workspace pass with
    `rtk cargo nextest run --workspace` and the alternate backend pass with
    `rtk cargo nextest run -p doradb-storage --no-default-features --features libaio`.
    Preserve `.config/nextest.toml` timeout and hang-detection policy.

## Open Questions

None. The table set is one-shot and immutable; deterministic planning is the
new RFC Phase 3; row execution and public export remain Phase 4; benchmark
proof remains Phase 5. During `$task-resolve`, synchronize the implemented
Phase 2 outcome and the final phase split back to RFC-0030.
