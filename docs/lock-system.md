# Lock System

## Status and Purpose

This document is a pre-RFC design study for Doradb's logical lock system.

It has three purposes:

1. Describe the behavior and constraints of the implemented lock manager.
2. Present the current working direction for redesigning its ownership,
   resource, waiter, and cleanup structures.
3. Preserve the limitations and unresolved questions that must be answered
   before the redesign becomes an RFC.

This document is not yet an implementation contract. The implemented baseline
is the code in `doradb-storage/src/lock/` and its lifecycle call sites.
[RFC-0016](./rfcs/0016-logical-lock-manager.md) records the original accepted
design, but subsequent work has changed parts of it; notably, the implemented
manager no longer has `CatalogNamespace`. Where this document says **current**,
it describes the implementation. Where it says **working design**, it describes
the pre-RFC proposal.

Deadlock handling is intentionally excluded. It is tracked independently in
[backlog 000167](./backlogs/000167-logical-lock-deadlock-handling.md) and should
not expand the scope of this design.

## Executive Summary

Doradb uses MVCC for row visibility and row-level write ownership, but MVCC
alone does not coordinate every operation that changes or depends on table-wide
state. The logical lock manager supplies that coordination at two table-level
resources:

- `TableMetadata(table_id)` protects table existence, schema, and runtime
  layout assumptions.
- `TableData(table_id)` coordinates table-wide operations with row writers and
  maintenance.

The current manager represents session-explicit, transaction, statement, DDL,
and maintenance owners as canonical exact owners with one session family and
one scope. Each exact owner still produces a separate physical grant. Each
resource stores grants in a `Vec` and waiters in a `VecDeque`. This is
functionally small and cancellation-aware, but many operations scan all grants
or rebuild the whole queue.

The working design separates:

1. **Physical conflict participation**: one holder per session family and
   resource.
2. **Exact logical ownership**: one claim per session, transaction, statement,
   DDL operation, or maintenance operation.
3. **Serialized family execution**: one lock acquisition, release, conversion,
   or cleanup operation at a time for all scopes in one session family.
4. **Pending acquisition identity**: one call-local cancellation guard for the
   family's optional FIFO waiter.

The two primary indexes become:

```text
owner-lifecycle side                     manager/resource side
--------------------                     ---------------------
LockScopeState                           ResourceState
└── resource -> ScopeClaim               └── family -> FamilyResourceState
                                              ├── optional physical holder
                                              ├── exact claims
                                              └── optional queued waiter
```

The scope index provides targeted cleanup. The resource index provides conflict
arbitration, FIFO queueing, physical family aggregation, and diagnostics.
Different families remain concurrent. Within one family, the session execution
owner retains exclusive lock-mutation authority across `.await`.

The intended result is:

- average `O(1)` compatibility and exact-owner lookup;
- `O(1)` enqueue and queue unlink;
- `O(K)` work to promote `K` actual waiters;
- cleanup proportional to one scope's indexed resources rather than the whole
  lock table; and
- preservation of coverage, conversion, FIFO, cancellation, and same-session
  claim semantics under the serialized family contract.

## Why Doradb Needs Logical Locks

### MVCC and logical locks solve different problems

MVCC answers:

- which row version a transaction may read;
- whether a hot-row undo head is visible;
- whether a cold-row deletion marker is visible;
- which row or index write owner conflicts with another writer; and
- how rollback and recovery reconstruct committed state.

Logical locks answer:

- whether table metadata may change while a transaction depends on it;
- whether a table-wide mutation may run with row writers;
- whether freeze or checkpoint may move page state while a full-table mutation
  is active;
- whether DDL may publish a new schema or table lifecycle state; and
- how an explicit table lock constrains work from other sessions and from the
  same session.

Logical locks do not replace row undo ownership or the column deletion buffer.
A transaction may hold `TableData(IX)` and still receive a row-level
`WriteConflict`.

### Current operation mapping

The current implementation uses the following table-level mapping:

| Operation | Metadata lock | Data lock | Logical lifetime |
|---|---|---|---|
| First table touch | `S` | none | statement `S`, then transaction `S` binding |
| Repeated bound read | cached transaction `S` | none | transaction |
| Insert/update/delete | transaction `S` | transaction `IX` | transaction |
| Full-table MVCC mutation | transaction `S` | transaction `X` | transaction |
| Explicit shared table lock | session `S` | session `S` | explicit session |
| Explicit exclusive table lock | session `S` | session `X` | explicit session |
| Freeze/checkpoint | prepared owned `S` | prepared owned `IS` | prepared maintenance operation, then mandatory owner |
| CREATE TABLE on a new id | target `X`; catalog slots 0-3 `S` | catalog slots 0-3 `IX` | prepared DDL operation, then mandatory owner |
| DROP TABLE | target `X`; catalog slots 0-4 `S` | target `X`; catalog slots 0-4 `IX` | prepared DDL operation, then mandatory owner |
| CREATE INDEX | target `X`; catalog slots 0,2,3 `S` | target `X`; catalog slots 0,2,3 `IX` | prepared DDL operation, then mandatory owner |
| DROP INDEX | target `X`; catalog slots 2,3 `S` | target `X`; catalog slots 2,3 `IX` | prepared DDL operation, then mandatory owner |

On first touch, statement metadata protection is handed to the transaction
without a gap:

```text
statement acquires Metadata(S)
    -> resolve and validate the binding
    -> transaction acquires Metadata(S)
    -> release statement Metadata(S)
```

Successfully bound reads therefore retain metadata protection until transaction
commit or rollback. Repeated operations use the transaction binding and exact
transaction scope.

Table and index DDL acquire their complete fixed lock sequences while the
public session future is still cancellable. Winning mandatory capacity
synchronously transfers the same boxed family authority and operation
`curr_scope` to accepted execution; there is no release/reacquire window.
Catalog statements
receive a typed prepared-write authority that proves metadata S plus data IX
for each catalog table and bypasses ordinary transaction lock acquisition.
Effectful maintenance likewise prepares an owned lock scope before mandatory
admission and transfers the exact authority without a release/reacquire
window. The read-only `total_row_pages` observation remains caller-owned and
uses an operation `curr_scope` that closes before its foreground pin returns.

Recovery, purge, and no-transaction replay do not acquire logical locks. They
run at lifecycle boundaries where foreground lock owners do not exist. Logical
lock state is volatile and is never reconstructed from redo.

## Resources, Modes, Compatibility, and Coverage

### Resources

The implemented manager has two resource kinds:

```rust
enum LockResource {
    TableMetadata(TableID),
    TableData(TableID),
}
```

The declared acquisition order for built-in multi-resource operations is:

```text
TableMetadata(table_id ascending)
    -> TableData(table_id ascending)
    -> row undo / column deletion-buffer ownership
```

`LockResource`'s derived ordering encodes the metadata/data portion. The lock
manager does not reorder separate caller acquisitions. Policy for arbitrary
cross-call sequences is outside this document and belongs to
[backlog 000167](./backlogs/000167-logical-lock-deadlock-handling.md).

The redesign does not add row resources.

### Modes

`TableMetadata` accepts:

- shared (`S`);
- exclusive (`X`).

`TableData` accepts:

- intention shared (`IS`);
- intention exclusive (`IX`);
- shared (`S`);
- exclusive (`X`).

The public explicit-lock API exposes only `S` and `X`. Intention modes are
internal coordination modes.

### Compatibility

Compatibility is symmetric and compares different physical families.

`TableMetadata`:

| Existing \ Requested | `S` | `X` |
|---|---:|---:|
| `S` | yes | no |
| `X` | no | no |

`TableData`:

| Existing \ Requested | `IS` | `IX` | `S` | `X` |
|---|---:|---:|---:|---:|
| `IS` | yes | yes | yes | no |
| `IX` | yes | yes | no | no |
| `S` | yes | no | yes | no |
| `X` | no | no | no | no |

### Coverage

Coverage is directional. It answers whether an existing mode is strong enough
to satisfy a request from the same exact owner or to permit a new exact claim
inside the same family.

`TableMetadata`:

| Existing \ Requested | `S` | `X` |
|---|---:|---:|
| `S` | yes | no |
| `X` | yes | yes |

`TableData`:

| Existing \ Requested | `IS` | `IX` | `S` | `X` |
|---|---:|---:|---:|---:|
| `IS` | yes | no | no | no |
| `IX` | yes | yes | no | no |
| `S` | yes | no | yes | no |
| `X` | yes | yes | yes | yes |

`S` and `IX` are deliberately incomparable. The system does not synthesize a
`SIX` mode or treat `S + IX` as `X`.

### Different owners in one family

Same-session owners are not generally conflict-free. For a request `R` from
exact owner `O`, every held claim from a different owner in the same family must
cover `R`. Otherwise the request returns `LockFamilyConflict`.

For table data, this produces:

| Existing other owner | New `IS` | New `IX` | New `S` | New `X` |
|---|---:|---:|---:|---:|
| `IS` | allow | conflict | conflict | conflict |
| `IX` | allow | allow | conflict | conflict |
| `S` | allow | conflict | allow | conflict |
| `X` | allow | allow | allow | allow |

Examples:

```text
session X -> transaction IX     allowed
transaction IX -> session X     manager conflict if evaluated
session S -> transaction IX     conflict
transaction IX -> statement S   conflict
```

The rule is evaluated against every granted and waiting exact claim, not only
the family's strongest physical mode. The current manager still supports
duplicate waiters and does not serialize all family mutation. The later working
design would establish exclusive family lock-mutation authority before
removing those defenses. The transaction-to-Session example is not a reachable
public request: active transaction lifecycle admission returns
`ExistingTransaction` before the Session can call the manager. It remains
useful as the manager-level directional rule for internal modeling and
invariant tests.

## Implemented Baseline

### Resource representation

The current resource state is:

```rust
struct ResourceState {
    granted: Vec<GrantedLock>,
    waiters: VecDeque<Arc<Waiter>>,
}

struct GrantedLock {
    owner: LockOwner,
    mode: LockMode,
}
```

`LockOwner` contains a canonical `LockFamily(SessionID)` and an exact
`LockScope`: `SessionExplicit`, `Transaction`, `Statement`, or
`Operation(OperationID)`. `OperationID` is meaningful only within the owning
session family; the exact owner therefore preserves both session and operation
ids. Operation purpose is not encoded in the numeric id or scope. It remains in
the stable `SessionOperationKind` and typed DDL/maintenance authority. Owners
from one family may produce multiple granted entries for the same resource even
though they are one external conflict participant. The resource side does not
yet physically aggregate those entries.

### Linear family authority and owner-side indexes

Each engine-local session allocates one boxed `FamilyLockAuthority`. The same
box moves through the idle session, foreground operation, transaction,
prepared/precommit state, terminal proof, and accepted DDL or maintenance
carrier. It is never cloned or reconstructed from an owner id. The authority
contains the family/resource index and the session-explicit
`LockScopeState`; transaction, statement, DDL, and maintenance carriers own
their exact `curr_scope`.

Every accepted logical claim is authoritative in both directions:

```text
family.resources[resource].typed_scope_slot = (claim_no, mode)
curr_scope.claims[resource]                 = (claim_no, mode)
```

The common single claim is inline. A second scope expands the resource once
into fixed session-explicit, operation, transaction, and statement slots; the
box remains expanded until the entire family/resource entry disappears.
`ClaimNo` is a session-local `u64` identifier allocated with checked
arithmetic. Failed, rejected, and cancelled fresh attempts burn their reserved
number; conversion retains it; release followed by reacquisition receives a
new number.

Repeated covered acquisition by the same exact scope is fully local. A fresh
claim covered by another scope in the family still creates its own exact
manager grant in Phase 1. Thus the manager representation below remains an
exact mirror while owner-side state supplies bounded family lookup and
targeted cleanup.

Transactions, DDL, maintenance, and explicit-lock mutations reserve ids from
one plain session-local sequence. One public DDL call retains one
`Operation` owner through its typed `SessionDdlContext`; one effectful public
maintenance call retains one `Operation` owner through its prepared and
accepted owned scope. A delayed checkpoint completes that operation before its
observer-only wait and allocates a fresh operation owner for the next attempt.
The caller-owned `total_row_pages` observation uses a lifetime-bound
`SessionTable` minted by `SessionOperationPin::read_table`, so its strong table
runtime owner cannot outlive the maintenance operation that retains metadata-S
and data-IS claims.

Maintenance records its own exact claims even when a covering
`SessionExplicit` claim admits it. Releasing maintenance therefore cannot
consume the explicit claim. DDL has an additional purpose preflight: a held
same-family `SessionExplicit` claim on the target table returns
`LockFamilyConflict`, even if directional coverage would otherwise admit the
DDL request. The preflight reads the authoritative family slot rather than
scanning manager grants.

### Session and transaction admission

The public session lock, DDL, and mutating maintenance APIs already use
`&mut Session`. `Session` is `Send` and `!Sync`. These type-level constraints
serialize calls through one public session handle, but `begin_trx()` returns a
detached `Transaction`, so the mutable borrow ends while that transaction
remains active.

The session registry closes that coexistence gap with one operation
coordinator. Session disposition (`Open`, `CloseRequested`, or `Abandoned`) is
orthogonal to its single slot (`Idle`, `Active`, or `Closed`). Effectful public
operations require an open idle slot. An active public transaction returns
`LifecycleError::ExistingTransaction`; another active kind returns
`ExistingOperation`, with exact key, kind, state, disposition, and optional
transaction id in diagnostics. Independently drop-safe read-only observations
and standalone progress waits use observer admission and allocate no operation
id. `Session::id()` remains a local observation, and `Drop` retains its
nonblocking abandonment behavior.

The implemented authority transition is:

```text
Open + Idle
    -> take the one boxed family authority
    -> reserve one (SessionID, OperationID) entry
Open + Active(PublicTransaction)
    -> commit, rollback, or abandoned cleanup closes transaction curr_scope
    -> ReleasedTransactionLocks returns the same box
Open + Idle
```

An explicit session claim acquired before `begin_trx()` may remain held while
the transaction owns its own claims. The Session cannot start another
effectful operation until transaction completion restores `Idle`, but
standalone observer operations remain available.

DDL is an internal nesting exception, not a second public execution owner. A
DDL call first reserves a typed DDL operation while idle and retains its
`&mut Session` borrow while the same entry hosts a private catalog
transaction. The private transaction inherits the operation key and allocates
only a `TrxID`; it temporarily takes the outer carrier's family box while the
operation `curr_scope` remains owned and immutable. Terminal completion parks
the returned box in the stable entry, and the still-active outer operation
reclaims that exact allocation before acquiring again or closing.

### Wait and cancellation behavior

The current waiter is an `Arc` containing:

- exact owner and owner group;
- target mode;
- `Waiting`, `Granted`, or `Released` outcome;
- an `Event`;
- active cancellation-guard count; and
- a grant-observed flag.

Duplicate acquisitions by the same exact owner share the waiter. The last
cancellation guard:

- removes a queued waiter; or
- removes a promoted but unobserved grant.

Promotion installs the grant before notifying the async task. This closes the
wakeup interval in which an incompatible request could otherwise be admitted.

### Current complexity

Let:

- `G` be granted entries on one resource;
- `W` be queued waiters on one resource;
- `K` be waiters promoted by one transition;
- `H_scope` be resources indexed by one exact scope.

Approximate current costs are:

| Operation | Current cost |
|---|---:|
| Find same owner grant | `O(G)` |
| Find same owner waiter | `O(W)` |
| Validate same-session owners | `O(G + W)` |
| Fresh acquisition | `O(G + W)` |
| Immediate conversion | `O(G + W)` |
| Release one owner/resource | `O(G + W)` plus promotion work |
| Cancel one waiter | `O(W)` plus promotion work |
| Promote `K` waiters | up to `O(K * (G + K))` |
| Exact-scope cleanup | sum of release costs over `H_scope` |
| Session-explicit cleanup | sum of release costs over its `H_scope` |

These costs are not automatically problematic for small queues. The design
concern is that they grow with all exact owners on a hot resource and perform
repeated scans under the resource shard lock.

Normal statement, transaction, operation, and session close never call
`LockManager::release_owner()` or scan unrelated manager resources.
`release_owner()` remains only as a manager-level migration and diagnostic
defense.

### Behavioral constraints worth preserving

The redesign must preserve:

1. FIFO-compatible prefix granting.
2. A fresh compatible request waits behind an older incompatible waiter.
3. A covering granted same-family claim may permit queue bypass.
4. Blocking conversion is unsupported.
5. Cancellation after promotion but before observation cannot leak a grant.
6. DDL rejects explicit same-session table locks in the current behavior.
7. Cleanup remains proportional to one exact scope's indexed resources.

The redesign intentionally narrows one current manager behavior. Concurrent
lock mutations in one session family are unsupported, so duplicate pending
acquisitions no longer share a waiter. Public session `lock_table()` and
`unlock_table()` already require `&mut self`, and public admission already
rejects every effectful Session operation while its detached transaction is
active. A future parallel executor must route family lock mutations through one
serial coordinator.

### Proof-bound terminal cleanup ordering

Implemented [task 000242](./tasks/000242-enforce-terminal-transaction-lock-release-ordering.md)
made transaction-lock release a structural prerequisite of terminal session
completion.

Every terminal user-transaction path closes its transaction `curr_scope`
before finishing the session transaction lifecycle. Transaction code mints one
non-cloneable, transaction-id-bound `ReleasedTransactionLocks` proof only after
that scope is empty. The proof owns the returned `Box<FamilyLockAuthority>`,
so terminal publication cannot lose, duplicate, or reconstruct family
authority. Prepared and precommit paths reach the engine lock manager through
their retained terminal attachment, avoiding a second retained component
guard.

`TrxAttachment::commit()` and `TrxAttachment::rollback()` consume a matching
proof before they can make a running session idle or close an abandoned
session. Raw session-registry finish operations are private to the session
module, so production terminal cleanup cannot bypass this boundary.

The implemented order is:

```text
ordered commit:
    publish the committed status
    -> release transaction-owned logical locks
    -> finish the session transaction lifecycle

rollback or no-op discard:
    finish rollback effects and required purge bookkeeping
    -> release transaction-owned logical locks
    -> finish the session rollback-style lifecycle

abandoned session:
    release transaction-owned logical locks
    -> close the session
    -> release explicit session-owned logical locks
```

Public statement-future cancellation composes with the same terminal proof
boundary:

```text
drop callback and pending acquisition
    -> fold residual statement undo into transaction undo and discard statement redo
    -> release statement-owned logical locks
    -> check the complete transaction core in as CleanupReady
    -> worker rolls back transaction effects
    -> release transaction table bindings
    -> release transaction-owned logical locks
    -> consume ReleasedTransactionLocks at session rollback completion
```

The callback future is destroyed before its `StmtState`, so a queued waiter is
removed, or a promoted-but-unobserved grant is released, before the core becomes
cleanup-claimable. Statement cancellation does not release transaction-owned
metadata/data locks or table bindings inline; those remain attached to
`TrxInner` until whole-transaction rollback reaches the ordering above.

The proof covers the implemented closed transaction scope and owns the returned
family root. Later physical-manager phases must preserve this terminal
ordering.

## Working Design Overview

### Canonical owner identity

The separate owner and owner-group concepts become one exact identity:

```rust
struct LockOwner {
    family: LockFamily,
    scope: LockScope,
}

struct LockFamily(SessionID);

enum LockScope {
    SessionExplicit,
    Transaction(TrxID),
    Statement(TrxID, StmtNo),
    Operation(OperationID),
}
```

`LockOwner` is used for claims, waiters, cleanup, tokens, diagnostics, and
purpose-specific policy. `LockFamily` is used for physical conflict
aggregation and same-session policy. Purpose-specific policy is selected by
typed session-operation authority, never recovered from `OperationID`.

Constructors must enforce that transaction and statement ids belong to the
declared session family. Operation ids are monotonic only within one session;
equal raw ids in different families remain distinct exact owners.

### Serialized family ownership

One logical execution owner has lock-mutation authority for a `LockFamily`.
That authority covers every session, transaction, statement, DDL, and
maintenance scope in the family. At most one acquisition, release, conversion,
or scope cleanup may be active in the family, including while an acquisition
awaits an external blocker.

This is not OS-thread affinity. A lock-mutating operation holding mutable
session access, transaction checkout, statement borrow, or operation guard may
move between executor threads while retaining exclusive authority. Different
families continue to execute concurrently.

The outer lifecycle owns and transfers this authority. Session teardown,
transaction completion, statement drop, and operation-guard cleanup must first
obtain it, which proves that an earlier acquisition future completed or was
cancelled. `LockScopeState` does not add an internal mutex, reference count, or
close-drain lease to repair a violation of that ownership contract.

The implemented explicit session-lock API already reflects the contract:

```rust
impl Session {
    pub async fn lock_table(
        &mut self,
        table_id: TableID,
        mode: TableLockMode,
    ) -> Result<()>;

    pub fn unlock_table(&mut self, table_id: TableID) -> Result<()>;
}
```

Transaction lock mutation also uses `&mut self`; DDL and mutating maintenance
operations use exclusive Session access. These receivers serialize calls
through one handle, but do not by themselves exclude the detached
`Transaction` returned by `begin_trx()`. Registry admission supplies that
missing proof: every effectful public Session operation requires an open idle
coordinator slot and fails with `LifecycleError::ExistingTransaction` while a
public transaction is active, checked out, terminal-owned, abandoned, or being
cleaned up. Read-only diagnostics and standalone progress waits are observer
operations and do not enter the operation-id domain.

The lifecycle transfers public family authority as follows:

```text
Open + Idle: Session may admit one public operation
    -> reserve operation and begin transaction
Open + Active(PublicTransaction): transaction leases/terminal/cleanup carriers own it
    -> release transaction claims
    -> finish exact operation
Open + Idle: effectful Session admission resumes
```

An already-admitted typed DDL operation may create a private catalog
transaction in its stable entry while the outer `&mut Session` call remains borrowed. The normal
path is sequential, but cancellation of the whole DDL future can queue
transaction cleanup while DDL scope guards unwind. The later exact-family
redesign must define a cleanup handoff that serializes those two paths before
it relies on the single-family mutation invariant; idle-only effectful public
admission does not solve that internal cancellation boundary.

The public `Session` handle remains movable between threads but is not
shareable: its local closed flag uses `Cell<bool>`, making the type `Send` and
`!Sync`. Consequently, an async lock-free read borrowing `&Session` is not a
`Send` future. This is separate from the lock-serialization proof. Mutable
access serializes one handle, registry admission excludes its detached
transaction, and the future DDL cleanup handoff must cover internal nesting. If
Doradb later adds parallel execution within one session, workers must submit
lock mutations through one family coordinator.

### Dual indexing

The design maintains both directions:

```text
session/transaction/statement/operation runtime
└── LockScopeState(owner)
    └── resource -> ScopeClaim

LockManager
└── resource -> ResourceState
    └── family -> exact claims and optional waiter

active acquisition call
└── PendingAcquireGuard -> waiter or fresh claim
```

Neither index replaces the other:

- scope state makes granted-claim lookup and cleanup targeted;
- resource state makes conflict checks and FIFO transitions atomic.

The lifecycle object for an exact owner owns its `LockScopeState` exclusively.
The manager does not retain another strong reference to the scope. Pending
state belongs to the active acquisition call until it either transfers a
granted claim into the scope or cancels.

### One physical holder, multiple exact claims

For one `(resource, family)`:

```text
FamilyResourceState
├── zero or one physical holder
├── zero or more provisional/held exact-owner claims
└── zero or one queued exact-owner waiter
```

Only the physical holder is collapsed. Exact claims retain their lifetime and
purpose. A waiter retains its FIFO position and excludes another lock mutation
from the same family until it completes or is cancelled.

## Core Data Structures

The following structures describe the working direction. Names and integer
widths remain open to implementation refinement.

### Ownership and access path

There is one `LockScopeState` for each exact `LockOwner`. It is the
authoritative owner-side index of accepted claims, not a best-effort cache.
Cleanup relies on it to find every held resource without scanning unrelated
resources.

The expected lifecycle owners are:

| Lock scope | Owner-side location | Close boundary |
|---|---|---|
| `SessionExplicit` | session runtime state | session teardown; explicit unlock removes selected claims |
| `Transaction` | transaction state and its completion carrier | after commit publication or rollback effects, before the session becomes idle |
| `Statement` | statement or streaming-statement state | statement completion or drop |
| `Ddl` | one DDL operation guard | DDL success or failure |
| `Maintenance` | one maintenance operation guard | maintenance success or failure |

`LockScopeState` is the implemented exact-scope cleanup index. It gives session
explicit locks, DDL, and maintenance the same targeted cleanup mechanism used
by transactions and statements.

An acquisition borrows the family execution authority and target scope
exclusively across `.await`. A pending request is therefore not inserted into
the scope map. Its call-local guard owns the waiter token and completion. When
promotion is observed, the guard first adopts the provisional claim and then
transfers the resulting claim token into the scope map. Dropping the guard
before transfer cancels a queued waiter or releases a promoted but unobserved
claim.

Scope cleanup begins only after the active family operation has completed or
been cancelled. It consumes the uniquely owned scope and releases its accepted
claims. There is no concurrent closer or pending scope entry to reconcile.

The normal access path is:

```text
exclusive family operation
    -> inspect the exact scope's claim map
    -> perform one synchronous LockManager resource transition
    -> return a fresh claim, or create a call-local waiter guard
    -> await WaitCompletion without manager mutexes
    -> adopt the provisional claim
    -> transfer the accepted claim into LockScopeState

scope close
    -> consume LockScopeState after no family operation remains
    -> release every indexed claim
    -> return the lifecycle-specific completion proof when required
```

### Scope-owned claims

```rust
struct LockScopeState {
    owner: LockOwner,
    claims: FastHashMap<LockResource, ScopeClaim>,
}

struct ScopeClaim {
    claim_token: ClaimToken,
    mode: LockMode,
}
```

The map contains only accepted claims. Releasing or explicitly unlocking one
resource removes its entry. Closing a scope consumes the map; lifecycle owners
that need idempotent cleanup store the scope in an `Option` and take it once.
The transaction close path may evolve this consumption into the closed-scope
proof that replaces `ReleasedTransactionLocks`.

### Resource and family state

```rust
struct ResourceState {
    incarnation: ResourceIncarnation,

    // Counts physical family holders, not exact claims.
    granted_counts: [u32; MODE_COUNT],
    grant_mask: ModeMask,

    families: FastHashMap<LockFamily, FamilyResourceState>,
    wait_queue: WaitQueue,
}

struct FamilyResourceState {
    holder: Option<FamilyHolder>,

    claims: FastHashMap<LockOwner, ClaimRecord>,
    claim_counts: [u32; MODE_COUNT],
    claim_mask: ModeMask,

    queued_waiter: Option<FamilyWaiter>,
}

struct FamilyHolder {
    physical_mode: LockMode,
}

struct FamilyWaiter {
    owner: LockOwner,
    target_mode: LockMode,
    waiter_token: WaiterToken,
}

struct ClaimRecord {
    mode: LockMode,
    claim_id: ClaimID,
    phase: ClaimPhase,
}

enum ClaimPhase {
    Provisional {
        waiter_id: WaiterID,
    },
    Held,
}
```

The per-mode claim counts avoid scanning all exact claims during directional
admission or holder recomputation. The exact claim map remains necessary for
lookup, token validation, DDL policy, cleanup, and diagnostics. The optional
waiter provides direct token validation and diagnostics without supporting
multiple in-flight operations in one family.

`FamilyResourceState` exists while it has an optional waiter or any provisional
or held claim. It is removed only when all three are absent:

```text
holder == None
claims.is_empty()
queued_waiter == None
```

### Persistent waiter state

```rust
struct WaitQueue {
    head: Option<WaiterID>,
    tail: Option<WaiterID>,
    nodes: GenerationalSlab<WaitNode>,
}

struct WaitNode {
    owner: LockOwner,
    target_mode: LockMode,
    phase: WaitNodePhase,
    completion: Arc<WaitCompletion>,
}

enum WaitNodePhase {
    Queued {
        prev: Option<WaiterID>,
        next: Option<WaiterID>,
    },
    Provisional {
        claim_id: ClaimID,
    },
}

struct WaitCompletion {
    outcome: Mutex<WaitOutcome>,
    event: Event,
}

enum WaitOutcome {
    Waiting,
    Promoted,
}
```

Promotion detaches a node from the FIFO links but does not reclaim its slab
slot. The node remains addressable until the provisional claim is observed or
cancelled.

The completion has independent `Arc` lifetime, so the acquisition guard can
listen without borrowing the queue node or retaining resource synchronization.

### Resource-qualified tokens

```rust
struct WaiterToken {
    resource: LockResource,
    resource_incarnation: ResourceIncarnation,
    waiter_id: WaiterID,
    owner: LockOwner,
}

struct ClaimToken {
    resource: LockResource,
    owner: LockOwner,
    claim_id: ClaimID,
}
```

`WaiterID` contains slab slot and slot generation. `ResourceIncarnation` is
allocated from an engine-lifetime monotonic source whenever an empty resource
entry is recreated. It prevents an old waiter token from matching the same slab
slot in a new resource-state instance.

`ClaimID` is allocated from an engine-lifetime monotonic source. It must not
reset when a family or resource entry is removed. A stale claim token therefore
cannot release a later claim by the same exact owner on the same resource.

All identity and generation counters, including resource incarnations, claim
ids, and slab-slot generations, must use checked arithmetic and define an
explicit fatal invariant response on exhaustion.

### Call-local pending acquisition

One call-local guard owns a waiting acquisition:

```rust
struct PendingAcquireGuard<'manager> {
    manager: &'manager LockManager,
    state: Option<PendingAcquireState>,
}

enum PendingAcquireState {
    Waiting {
        waiter_token: WaiterToken,
        completion: Arc<WaitCompletion>,
    },
    Granted {
        claim_token: ClaimToken,
        mode: LockMode,
    },
}
```

Observation changes the guard from `Waiting` to `Granted` before the waiter node
is reclaimed. The guard is disarmed only after the `ScopeClaim` is inserted.
Dropping it in `Waiting` cancels the queued or provisional waiter; dropping it
in `Granted` releases the fresh claim. Immediate fresh grants use the same
claim-before-transfer discipline.

## Core Operations

### 1. Begin an exclusive family acquisition

The caller holds exclusive family execution authority and mutable access to the
target `LockScopeState` for the entire operation. It first checks the scope's
claim map.

When an existing claim has mode `H`:

1. If `H` covers the requested mode, return the existing claim locally.
2. If the requested mode covers `H`, attempt immediate conversion.
3. If neither covers the other, return `LockConversionNotSupported`.

The common covered path does not enter shared resource state. If the scope has
no claim, acquisition enters one synchronous manager transition. Finding an
existing queued waiter for the same family is an invariant violation because
the waiting call still owns the family execution authority.

### 2. Fresh immediate acquisition

For a fresh request:

1. Lock the resource state.
2. Check purpose-specific policy such as DDL versus `SessionExplicit`.
3. Assert that the family has no queued waiter or provisional claim.
4. Validate every different-owner same-family held claim using the family
   claim counts/mask.
5. Check external compatibility using physical-holder counts/masks.
6. Apply FIFO policy.
7. If immediately grantable:
   - insert a held exact claim;
   - create or update the physical family holder;
   - update counts and masks;
   - return a guarded `ClaimToken`.
8. Release resource synchronization.
9. Transfer the guarded token and mode into the scope's claim map.

A new different-owner claim that is covered by an existing family holder still
enters shared resource state because its exact lifetime must be registered.
It does not change external holder counts.

### 3. Enqueue a waiter

If a fresh request cannot be granted:

1. Allocate a slab node.
2. Link it at the FIFO tail.
3. Install the family's single `queued_waiter`.
4. Return `WaiterToken` plus `Arc<WaitCompletion>`.
5. Construct a `PendingAcquireGuard`.
6. Release all synchronous locks.
7. Await the completion event while retaining exclusive family authority.

Enqueue is `O(1)` average.

### 4. Wait for completion

The async loop registers a listener before inspecting the outcome:

```text
listen
    -> Waiting: await event
    -> Promoted: observe promotion through the pending guard
```

Registering first prevents a lost wakeup.

### 5. Promote the FIFO prefix

Every transition that may reduce blocking runs one central grant loop while
holding the resource-state lock:

```text
while queue head is promotable:
    detach head links
    clear the family's queued_waiter
    install provisional exact claim
    recompute/create family holder
    update physical aggregates when needed
    mark waiter node Provisional
    publish Promoted outcome
    queue completion for notification
```

Notifications occur after the resource lock is released.

Promotion eligibility checks:

1. compatibility with external physical family holders;
2. coverage by all current same-family held claims.

The loop promotes the maximal compatible FIFO prefix.

### 6. Observe a promoted waiter

The waiting family operation observes `Promoted`:

```text
manager validates resource incarnation, node phase, owner, and provisional claim
    -> provisional claim becomes held
    -> pending guard changes Waiting -> Granted
    -> reclaim waiter slab node
    -> insert ScopeClaim into the exact scope
    -> disarm the guard
```

Observation does not change compatibility and does not rerun queue granting.
There is no `.await` between adopting the claim and transferring it into the
scope. If unwinding occurs in that interval, the still-active guard releases
the fresh claim.

### 7. Cancel a pending acquisition

Dropping an active `PendingAcquireGuard`:

1. In `Waiting`, cancel the queued or provisional waiter, reclaim its node, and
   rerun FIFO-prefix granting.
2. In `Granted`, release the fresh claim and rerun granting through the normal
   claim-release transition.

The scope map is unchanged because pending and unaccepted fresh state remains
call-local. Guard drop finishes synchronously before family execution authority
can move to another operation or scope cleanup.

### 8. Release an exact claim

Releasing a claim:

1. Remove and take the `ScopeClaim` from the uniquely borrowed scope.
2. Validate resource, exact owner, and global `ClaimID` in manager state.
3. Remove the exact claim.
4. Update family claim counts/mask.
5. Recompute the strongest remaining family claim using the fixed mode set.
6. Retain, downgrade, or remove the physical family holder.
7. Update resource holder counts/mask if the physical mode changed.
8. Rerun FIFO-prefix granting even if the physical mode did not change.

The last step is required because removing a weaker same-family claim may remove
a directional constraint that prevented the queue head from being promoted.

An explicit session unlock removes and releases the selected metadata and data
claims; it does not consume the reusable `SessionExplicit` scope.

### 9. Consume a scope

Scope cleanup consumes the unique `LockScopeState`. The outer lifecycle must
first complete or cancel the active family operation, so the scope contains
only accepted claims and has no waiter to cancel.

```text
take the scope once from its lifecycle owner
    -> drain its claim map
    -> release each exact claim
    -> produce any lifecycle-specific completion proof
```

No global resource scan is required.

Scope-level close is intentionally neither shared nor independently
idempotent. A lifecycle object that has multiple sequential cleanup paths
stores `Option<LockScopeState>` and takes it once. Parallel close callers are
outside the serialized family contract.

Lifecycle ordering is:

```text
statement scope cleanup
    before statement object/effects disappear

transaction scope cleanup
    after commit decision or rollback undo is complete
    before session is marked idle or abandoned-session cleanup runs

session explicit scope cleanup
    after all transaction scopes in the family are closed
    before final session-state destruction
```

For successful commit, the intended order is:

```text
publish commit status
    -> close transaction lock scope
    -> finish session transaction lifecycle
    -> release abandoned-session explicit claims if required
```

The current proof-gated attachment boundary enforces this rule for ordered
commit, no-op discard, rollback, failed precommit, fatal cleanup, and abandoned
session cleanup. A future transaction-scope implementation may consume
`LockScopeState` to mint the same proof; it must not weaken that boundary.

### 10. Immediate exact-owner conversion

Given an exact-owner held mode `H` and requested mode `R`:

1. If `H` covers `R`, return existing.
2. If `R` covers `H`:
   - every other same-family claim must cover `R`;
   - the resource queue must be empty;
   - `R` must be immediately compatible with external family holders after
     excluding this family's current holder;
   - otherwise return `LockUpgradeWouldBlock`;
   - if valid, update the manager claim, physical holder, and scope mode.
3. If neither covers the other, return `LockConversionNotSupported`.

The claim keeps its global `ClaimID` across a successful in-place conversion.
Release of that claim releases its current mode.

Different exact owners cannot strengthen an earlier owner's claim. They can
only add a claim covered by every existing same-family claim.

### 11. Queue bypass

A fresh compatible request normally waits behind a non-empty queue.

It may bypass only when:

- an existing held same-family claim covers it;
- every other same-family claim passes directional admission;
  and
- external family holders remain compatible.

No same-family waiter can coexist with the request.

### 12. DDL and explicit session locks

The working design preserves the current rule:

> DDL rejects a target resource when its family has a held `SessionExplicit`
> owner on that table.

This is checked inside the same resource-state critical section that admits
the DDL claim or waiter:

```text
explicit_owner = LockOwner {
    family: ddl_owner.family,
    scope: SessionExplicit,
}

reject when claims contains explicit_owner
```

The check is exact-owner `O(1)` average and does not inspect only the physical
holder mode. A queued or provisional session-explicit request cannot coexist
with DDL because both require the family's exclusive execution authority.

Metadata and data resources are acquired in normal order. If later acquisition
or table validation fails, only exact claims newly created by that DDL scope are
released.

## Same-Family Physical Mode

The physical family mode is the strongest actual exact claim under `covers()`.
It is not a lattice join.

Directional admission guarantees that the admitted claim set has one maximum.
Removing claims preserves comparability among the remaining claims. The
fixed-size claim counts permit recomputation in `O(MODE_COUNT)`.

Examples:

```text
claims: X, IX, IS
physical mode: X

release X
remaining claims: IX, IS
physical mode: IX
```

If `S` and `IX` would coexist, directional admission rejects the later request.
The manager never manufactures `X` to represent them.

## Proposed Complexity

Let:

- `M` be the fixed mode count, currently four;
- `K` be the number of waiters actually promoted by a transition;
- `H_scope` be the number of accepted claims held by one scope; and
- `C_family` be the number of exact claims in one family/resource state.

Hash-map costs below are average costs. `M` is constant.

| Operation | Working-design cost |
|---|---:|
| Repeated covered exact-owner acquisition | `O(1)` local |
| Covered different-owner family claim | `O(M)` shared average |
| Fresh immediate physical acquisition | `O(M)` shared average |
| Exact-owner lookup | `O(1)` average |
| Same-family directional validation | `O(M)` |
| Immediate conversion | `O(M)` |
| Enqueue waiter | `O(1)` average |
| Unlink queued waiter by token | `O(1)` |
| Observe provisional grant | `O(1)` average |
| Release exact claim | `O(M + K * M)` |
| Cancel one pending acquisition | `O(M + K * M)` |
| Promote `K` waiters | `O(K * M)`, effectively `O(K)` |
| Consume one scope | `O(H_scope + total promoted work)` |
| Resource debug snapshot | `O(C_family + 1)` per family |

The design removes scans over all physical holders for compatibility and
removes queue rebuilding for exact cancellation. Actual promotions remain real
work and cannot be made sublinear in the number of waiters promoted.

### Complexity caveats

Big-O improvement does not guarantee lower latency:

- one hot resource still serializes transitions through its shard lock;
- more exact claim records increase memory traffic;
- hash maps and slab nodes allocate unless optimized;
- FIFO may intentionally leave compatible work waiting behind an older
  incompatible request;
- scope cleanup is proportional to currently accepted claims; and
- notification scheduling can dominate small manager transitions.

Benchmarks must validate representation choices.

## Concurrency and Synchronization Rules

The owner side uses exclusive logical ownership rather than mutexes:

```text
exclusive family execution authority
    -> mutable exact-scope claim map
    -> one synchronous resource-state transition
```

Rules:

1. At most one lock-mutating operation may be active in a family, including
   while awaiting a waiter completion.
2. Scope state is uniquely borrowed or transferred; it has no internal mutex.
3. Scope cleanup begins only after the active family operation completes or its
   call-local guard cancels synchronously.
4. Never hold a resource-state mutex across `.await`.
5. Resource transitions never acquire or mutate owner-side scope state.
6. Queue promotion changes manager state and notifies through
   `WaitCompletion`; the exclusive async caller later updates its scope.
7. Notifications occur after releasing resource synchronization.
8. Resource state may be removed when empty, so tokens must validate resource
   incarnation.
9. Counter/mask updates and their maps are one atomic resource-state
   transition.

The implementation should add debug assertions for:

- counts matching map contents;
- masks matching nonzero counts;
- exactly one physical holder count per family with claims;
- holder absence when claims are empty;
- physical mode covering every exact claim;
- queue link and singular `queued_waiter` agreement;
- at most one queued waiter for a family;
- provisional node and provisional claim agreement;
- unique global claim ids among live claims; and
- empty family state before removal.

## Fresh Versus Existing Claims

Current multi-resource helpers distinguish a fresh exact grant from an existing
one so failure rollback does not release an older claim.

The redesign must preserve that distinction at the exact-claim layer:

```text
Fresh    = this operation created the exact owner/resource claim
Existing = the exact owner already had a covering claim
```

It must not confuse this with physical family-holder creation. A fresh exact
statement claim may reuse an existing physical transaction holder.

Statement-to-transaction handoff installs the destination transaction claim
before releasing the source statement claim.

DDL and maintenance use unique operation scopes, so closing a failed operation
releases only its own claims. Transaction and statement mutations are already
serialized by their runtime ownership. The redesign extends that serialization
to session explicit and cross-scope family mutations. While a multi-resource
helper is active, no later family operation can depend on one of its fresh
claims, so failure rollback may release exactly the claims created by that
helper without acquisition refcounts.

## Diagnostics

Diagnostics should expose both indexes without changing their ownership.

### Physical resource view

The lock manager can report, for each resource:

- resource incarnation;
- physical family holders and modes;
- aggregate holder counts/mask;
- FIFO queue order;
- waiter phase and target mode.

### Exact logical resource view

The lock manager can also report, for each family/resource:

- exact owner scope and purpose;
- claim mode;
- provisional or held phase;
- claim id;
- queued waiter token and mode.

### Owner scope view

A live `LockScopeState` can report:

- exact owner;
- accepted resource claims;
- claim modes and tokens.

The baseline does not require `LockManager` to strongly own or globally
enumerate scope states. If future system-wide inspection requires enumeration,
the observability design may add a weak scope registry or aggregate snapshots
through the lifecycle owners.

Debug snapshots should clearly separate physical holders from logical claims.
Counting only the physical family holder is insufficient to prove that a
transaction or statement owns the required claim.

## Limitations and Explicit Non-Goals

### Row ownership remains separate

The manager does not provide row locks, record locks, gap locks, or next-key
locks. Hot rows continue to use undo ownership; cold rows continue to use the
column deletion buffer.

### No persisted lock state

All holders, claims, waiters, tokens, and scope indexes are volatile. Recovery
runs before foreground owners exist and reconstructs no lock state.

### Immediate-only conversion

Conversions never wait. A stronger conversion succeeds immediately or returns
`LockUpgradeWouldBlock`. `S` and `IX` remain incomparable.

### No early transaction unlock

Transaction claims remain until commit, rollback, or fatal cleanup. Early
unlock would require proof that no uncommitted row, index, metadata-binding, or
rollback obligation still depends on the claim.

### FIFO head-of-line blocking

FIFO-compatible prefix granting prevents starvation of an older incompatible
request, but it can reduce throughput by delaying compatible requests.

### DDL under explicit session locks remains rejected

The redesign can distinguish DDL and explicit claims, so the original cleanup
ambiguity disappears. The rejection remains intentionally for behavioral
compatibility until a separate semantic decision changes it.

### Single-process, session-family model

`LockFamily(SessionID)` assumes one engine process and one logical execution
family per session. Distributed ownership, cross-process lock recovery, and
independent parallel lock mutation within one family are outside this design.

### Serialized family lock mutation

Session, transaction, statement, DDL, and maintenance work in one family may
hold claims at the same time, but their lock-manager transitions are
serialized. Session explicit lock and unlock already require mutable access,
and effectful public Session admission is idle-only while a detached transaction exists.
Parallel workers may use protection acquired by their coordinator, but must
send new acquisitions, conversions, releases, and cleanup through that single
family execution owner. Internal DDL cancellation still requires the explicit
cleanup handoff described below before the redesign may depend on this
invariant.

### No lock escalation or weak-lock fast path

The first redesign uses the shared manager for every new exact claim. It does
not add lock escalation, per-session metadata fast slots, or a PostgreSQL-style
weak-lock migration barrier.

### Deadlock handling is external

No deadlock policy is designed here. See
[backlog 000167](./backlogs/000167-logical-lock-deadlock-handling.md).

## Unresolved Questions

These questions remain in scope for pre-RFC design work.

### 1. Token allocation strategy

The working design uses:

- per-resource generational slab slots;
- a resource-incarnation id; and
- global claim ids.

An alternative uses global waiter ids in addition to slab slots. The RFC should
choose the smallest representation that still proves stale-token safety across
resource and family removal/recreation.

### 2. Family aggregate representation

Per-mode counts and masks make directional checks constant in `MODE_COUNT`.
The exact maps still carry overhead for the common family with only a
transaction claim.

Candidates include:

- hash maps plus counts from the first implementation;
- inline one/two-claim storage that spills to a map;
- `SmallVec` exact claims plus mode counts; or
- specialized fields for session, transaction, and current statement plus an
  operation map.

Benchmark and complexity evidence should decide this, not expected owner count
alone. The family waiter remains a single inline `Option` under every candidate.

### 3. Resource sharding

The current `FastDashMap` supplies resource-level shard synchronization. The
RFC should decide whether to retain it or introduce explicit partitions with:

- stable partition hashing;
- one mutex per partition;
- per-partition resource maps; and
- clearer aggregate statistics.

Partition count, hash quality, and hot-resource contention need measurement.

### 4. Additional purpose-specific family policy

DDL versus `SessionExplicit` is an implemented policy exception. Maintenance
uses ordinary directional coverage and retains a distinct exact claim. Future
internal operation scopes may need additional explicit rules.

The RFC should specify:

- which other pairs of already-held scope purposes require policy beyond
  directional coverage; and
- how purpose checks become atomic with family/resource admission.

Family execution serialization is not a purpose policy. It determines when
requests run; these checks determine whether a new exact claim may coexist with
claims retained by earlier scopes.

### 5. Downgrade API

Internal release can downgrade a family holder when its strongest claim
disappears. There is no explicit public or transaction downgrade operation.

Future need should determine whether to add:

- exact-owner mode downgrade;
- transaction `X -> IX`;
- session `X -> S`; or
- no explicit downgrade at all.

A downgrade must rerun FIFO granting and update the exact claim atomically.

### 6. Observability boundary

Internal debug snapshots are required. It remains open whether the first RFC
also adds:

- global enumeration of live scope states and, if needed, a weak registry;
- per-mode holder/waiter counters;
- queue length and wait-duration metrics;
- cancellation and stale-token counters;
- shard-lock contention measurements;
- structured wait tracing; or
- user-visible lock inspection.

### 7. Migration and compatibility

The implementation cannot switch physical ownership, tokens, and cleanup in one
unreviewable patch. The RFC must specify intermediate states that preserve
behavior and testability.

Migration must:

- preserve the implemented canonical `LockFamily` plus exact `LockScope`
  identity and `LockFamilyConflict` error;
- preserve the implemented `&mut self` session lock mutation APIs and
  `Session: Send + !Sync` boundary;
- preserve idle-only effectful public Session admission while a transaction is active or
  undergoing terminal/abandoned cleanup;
- serialize lock mutation across all scopes in one family before removing
  duplicate-waiter support from the manager;
- retain cancellation safety while pending state moves from shared waiters to
  one call-local guard.

### 8. Nested DDL transaction cancellation

DDL reserves one typed operation authority and then starts a private catalog
transaction inside the same stable entry and `&mut Session` call. Normal
execution serializes DDL-scope and
transaction-scope mutations, but dropping the outer future can abandon the
transaction and queue asynchronous rollback while DDL guards synchronously
release their claims.

Before the exact-family manager assumes one mutation owner per family, the RFC
must define an ownership handoff that guarantees:

- statement and pending-acquisition cancellation finishes first;
- DDL-scope cleanup and nested transaction cleanup cannot overlap;
- transaction claims still close before the Session becomes idle; and
- the Session remains unavailable until all transferred cleanup completes.

This handoff is a separate design stage. The implemented idle-only public
admission is necessary, but does not by itself serialize these internal cleanup
paths.

## Suggested Implementation Stages

These stages are a planning aid, not yet an accepted RFC plan.

### Stage A: canonical identity (implemented)

- Introduce `LockOwner { family, scope }`.
- Use the session-local operation id shared by DDL and maintenance authorities.
- Preserve idle-only effectful public Session admission and the existing mutable
  explicit-lock APIs.
- Preserve the current vector/deque resource representation, guard-owned
  operation cleanup, duplicate waiters, and exact-owner release.

### Stage B: exclusive scope ownership

- Define the DDL operation-guard/nested-transaction cleanup handoff before
  declaring family cleanup serialized.
- Serialize lock mutation across every scope in one family.
- Add uniquely owned `LockScopeState` claim maps.
- Route statement, transaction, session, DDL, and maintenance cleanup through
  scope indexes.
- Preserve proof-bound transaction-lock cleanup before session completion.

### Stage C: tokenized waiter and claim lifecycle

- Add resource-incarnation and global claim ids.
- Add generational waiter nodes.
- Keep nodes alive through provisional state.
- Add independent `WaitCompletion`.
- Add the call-local pending acquisition guard.
- Remove current duplicate-waiter behavior after all family mutations are
  serialized.

### Stage D: exact-claim family registry

- Store exact claims and one optional queued waiter per family.
- Collapse physical grants to one holder per family/resource.
- Add per-family claim counts and masks.
- Make provisional claims fully granted for arbitration.
- Preserve directional same-family admission and queue bypass.
- Make DDL purpose checks atomic with resource admission.

### Stage E: aggregate compatibility and intrusive FIFO

- Add physical holder counts and masks.
- Replace granted-vector scans.
- Replace queue rebuilding with intrusive token unlink.
- Centralize all blocker-removal transitions through the FIFO-prefix grant loop.
- Add invariant-rich physical and exact debug snapshots.

### Stage F: benchmark-led refinement

- Measure hot shared metadata resources, cancellation, scope cleanup, and queue
  promotion.
- Decide exact claim inline versus map representation.
- Decide resource partitioning.
- Consider a weak metadata-lock fast path only if the shared design remains a
  measured bottleneck.

## Validation Strategy

### Semantic tests

At minimum:

1. Compatibility and coverage matrices for both resources.
2. Directional same-family matrices.
3. Session `X` covering transaction `IX`.
4. An active transaction rejects every effectful public Session operation
   with `LifecycleError::ExistingTransaction` before lock-manager mutation.
5. Session `S` rejects a later transaction `IX`; the reverse public Session
   request is rejected at lifecycle admission before reaching the manager.
6. Exact covered reacquisition returns existing.
7. Immediate conversion succeeds only with empty queue and external
   compatibility.
8. DDL rejects held `SessionExplicit` claims; family serialization prevents a
   queued or provisional session-explicit request from coexisting with DDL.
9. Fresh-versus-existing rollback retains older claims.
10. Statement-to-transaction handoff has no protection gap.
11. Session explicit lock and unlock require mutable access.
12. Lock-free session observations accept immutable observer admission without
    allocating an operation id, while lock-bearing operations require mutable
    access and an idle coordinator slot.
13. `Session` remains `Send` and is not `Sync`.
14. Session, transaction, statement, DDL, and maintenance lock mutations in one
    family never overlap after the DDL cleanup handoff is implemented.

### Queue and cancellation tests

1. One family may retain at most one queued waiter.
2. Dropping a waiting acquisition guard unlinks a queued waiter.
3. Dropping a promoted acquisition guard removes its provisional claim.
4. Observation changes the guard to a fresh held claim before reclaiming the
   waiter node.
5. Transferring the claim into its scope disarms the guard.
6. Unwinding before transfer releases the fresh claim.
7. Cancelling the head reconsiders the next waiter.
8. Physical downgrade promotes newly compatible waiters.
9. Removing a same-family claim with unchanged physical mode reconsiders the
   queue.
10. A covering same-family claim permits bypass when all other admission checks
    pass.

### Token and lifecycle race tests

1. Stale waiter slot generation cannot affect a reused slot.
2. Stale waiter token cannot affect a recreated resource incarnation.
3. Stale claim id cannot release a later claim.
4. Acquisition cancellation finishes before family authority moves to another
   operation.
5. Scope consumption requires no active pending acquisition.
6. Sequential lifecycle cleanup takes and consumes the scope at most once.
7. Explicit unlock followed by relock uses a new claim id.
8. Transaction claims are absent before the session becomes idle.
9. Abandoned session explicit cleanup runs after transaction scope cleanup.
10. Cancelling DDL cannot overlap DDL-scope cleanup with nested transaction
    cleanup, and public Session admission stays closed through both.

### Invariant and model tests

Randomized transition tests should compare the optimized structure with a
simple reference model that stores exact claims and FIFO waiters in vectors.
After every operation, verify:

- physical mode;
- physical and claim masks/counts;
- exact claims;
- queue order;
- singular family-waiter agreement;
- provisional state;
- notifications;
- scope claim maps; and
- token validity.

### Benchmarks

Useful benchmark shapes include:

1. Many families acquiring `TableMetadata(S)` on one table.
2. Repeated exact-owner cache hits.
3. Transaction plus statement claims in one family.
4. A queued `X` behind shared holders while new readers arrive.
5. Cancellation at queue head, middle, and tail.
6. Promotion of a long compatible FIFO prefix.
7. Session explicit lock/unlock churn across many tables.
8. Scope cleanup with many active claims.

Measure:

- operations per second;
- resource-shard hold and wait time;
- allocations per fresh claim and waiter;
- queue promotion latency;
- cancellation latency;
- scope cleanup latency;
- memory per resource/family/claim/waiter/scope entry; and
- debug-assertion overhead in test builds.

## Normative Invariants Proposed for the RFC

The eventual RFC should accept, reject, or refine each invariant explicitly:

1. One exact owner identifies each session, transaction, statement, DDL
   operation, or maintenance operation.
2. At most one lock acquisition, release, conversion, or cleanup operation is
   active in a family.
3. Every exact owner has one uniquely owned scope index containing only
   accepted claims.
4. Pending waiter or fresh-claim state remains call-local until transferred
   into the exact scope.
5. A family has at most one queued waiter.
6. Scope cleanup consumes the scope only after its active family operation has
   completed or cancelled.
7. One physical holder exists per `(resource, family)`.
8. Zero or more exact claims contribute to one physical holder.
9. A waiter node survives promotion until observation or cancellation.
10. Provisional claims are fully granted for arbitration.
11. Tokens remain unambiguous across resource and family recreation.
12. Stale resource, waiter, and claim tokens have no effect.
13. Different-owner same-family admission is directional and validates every
    held claim.
14. Incomparable modes are rejected and never joined.
15. Only immediate exact-owner conversion may strengthen a family holder.
16. Every blocker or queue-barrier removal reruns FIFO-prefix granting.
17. Notifications occur after resource synchronization is released.
18. DDL preserves explicit same-session rejection unless separately changed.
19. Scope cleanup never scans unrelated resources.
20. Transaction scope cleanup precedes session transaction completion.
21. Destination claims are installed before source claims are released during
    lifetime handoff.

## References

- [Storage Architecture](./architecture.md)
- [Transaction System](./transaction-system.md)
- [RFC-0016: Logical Lock Manager](./rfcs/0016-logical-lock-manager.md)
- [Deadlock handling backlog](./backlogs/000167-logical-lock-deadlock-handling.md)
- `doradb-storage/src/lock/mod.rs`
- `doradb-storage/src/lock/claim.rs`
- `doradb-storage/src/lock/state.rs`
- `doradb-storage/src/session.rs`
- `doradb-storage/src/trx/admission.rs`
- `doradb-storage/src/trx/mod.rs`
- `doradb-storage/src/trx/stmt.rs`
- `doradb-storage/src/trx/stream_stmt.rs`
