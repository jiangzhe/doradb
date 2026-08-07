# Lock System

## Status and Purpose

This document describes Doradb's implemented logical lock system after the
RFC-0027 physical-family aggregation cutover and removal of statement-scope
logical locks.

It has three purposes:

1. Describe the behavior and constraints of the implemented lock manager.
2. Define the split between owner-local exact authority and shared physical
   arbitration.
3. Preserve the limitations and follow-up work assigned outside RFC-0027.

The implemented baseline is the code in `doradb-storage/src/lock/` and its
lifecycle call sites.
[RFC-0016](./rfcs/0016-logical-lock-manager.md) records the original accepted
design, but subsequent work has changed parts of it; notably, the implemented
manager no longer has `CatalogNamespace` and no longer mirrors accepted exact
claims.

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

The implementation separates:

1. **Physical conflict participation**: one holder per session family and
   resource.
2. **Exact logical ownership**: one claim per session-explicit scope,
   transaction, or DDL/maintenance operation.
3. **Serialized family execution**: one lock acquisition, release, conversion,
   or cleanup operation at a time for all scopes in one session family.
4. **Pending acquisition identity**: one call-local cancellation guard for the
   family's optional FIFO waiter.

The two primary indexes become:

```text
owner-lifecycle side                     manager/resource side
--------------------                     ---------------------
FamilyLockState                          ResourceState
├── resource -> fixed exact slots        ├── fixed holder counts/mask
└── each scope -> resource claim          ├── family -> physical state
                                          └── generational intrusive FIFO
```

Accepted exact owners and `ClaimNo`s exist only in the owner-side indexes.
Manager `Held` entries contain a family and physical mode; exact pending
identity exists in a waiter only until provisional adoption. The scope index
provides targeted cleanup. The resource index provides conflict arbitration,
FIFO queueing, physical family aggregation, and physical diagnostics.
Different families remain concurrent. Within one family, the session execution
owner retains exclusive lock-mutation authority across `.await`.

The resulting costs are:

- fixed three-slot compatibility and bounded exact-family lookup;
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
| First table touch | transaction `S` | none | transaction |
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

On first touch, metadata protection belongs directly to the transaction:

```text
transaction acquires Metadata(S)
    -> resolve and validate the binding
    -> install the binding and update the weak session cache
```

Every accepted first-touch claim remains until transaction commit, rollback, or
fatal cleanup. Resolution or validation failure installs no binding but retains
the claim, so a retry reuses the same exact transaction claim. Successfully
bound reads use the transaction binding without another metadata request.

Table and index DDL acquire their complete fixed lock sequences while the
public session future is still cancellable. Winning mandatory capacity
synchronously transfers the same boxed family authority and operation
`curr_scope` to accepted execution; there is no release/reacquire window.
Nested catalog transactions acquire their ordinary exact metadata-S and
data-IX claims. The enclosing DDL operation already holds
covering physical modes, so these claims publish owner-locally without another
manager transition.
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
operation IX -> private transaction S   conflict
```

The rule is evaluated against every accepted owner-local exact claim, not only
the family's strongest physical mode. Family lock mutation is serialized and
one family has at most one pending acquisition. The transaction-to-Session
example is not a reachable public request: active transaction lifecycle admission returns
`ExistingTransaction` before the Session can call the manager. It remains
useful as the manager-level directional rule for internal modeling and
invariant tests.

## Implemented Baseline

### Resource representation

The current resource state is:

```rust
struct ResourceState {
    granted_counts: [u32; 4],
    grant_mask: ModeMask,
    families: FastHashMap<LockFamily, PhysicalFamilyState>,
    wait_queue: WaitQueue,
}

enum PhysicalFamilyState {
    Held { mode: LockMode },
    Queued { node_id: WaitNodeID },
    Provisional { mode: LockMode, node_id: WaitNodeID },
}

struct WaitQueue {
    head: Option<WaitNodeID>,
    tail: Option<WaitNodeID>,
    nodes: WaitNodeSlab,
}
```

`Held` and `Provisional` each contribute exactly one family to the fixed count
for their mode. `Queued` contributes no holder. Compatibility excludes the
requesting family and inspects the four counts and compact mask.

`LockOwner` contains a canonical `LockFamily(SessionID)` and exact
`LockScope`: `SessionExplicit`, `Transaction`, or `Operation(OperationID)`.
Accepted manager state stores neither this exact owner nor its `ClaimNo`.
Pending nodes retain both until observation or
cancellation because they are required for token-exact validation.

### Linear family authority and owner-side indexes

Each engine-local session allocates one boxed `FamilyLockAuthority`. The same
box moves through the idle session, foreground operation, transaction,
prepared/precommit state, terminal proof, and accepted DDL or maintenance
carrier. It is never cloned or reconstructed from an owner id. The authority
contains the family/resource index and the session-explicit
`LockScopeState`; transaction, DDL, and maintenance carriers own their exact
`curr_scope`.

Every accepted logical claim is authoritative in both directions:

```text
family.resources[resource].typed_scope_slot = (claim_no, mode)
curr_scope.claims[resource]                 = (claim_no, mode)
```

Every family/resource entry embeds fixed session-explicit, operation, and
transaction slots. It does not allocate or expand when another scope is
inserted.
`ClaimNo` is a session-local `u64` identifier allocated with checked
arithmetic. Failed, rejected, and cancelled fresh attempts burn their reserved
number; conversion retains it; release followed by reacquisition receives a
new number.

Repeated covered acquisition by the same exact scope is fully local. A fresh
claim covered by another scope publishes into both owner-side indexes without
manager access. Mode-preserving conversion and release likewise inspect at
most the three fixed slots and remain owner-local.

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
operation `curr_scope` remains owned and immutable. It owns one checked-out
core and strong runtime attachment for its complete lifetime, so catalog
statement boundaries do not move the family authority or core through the
entry. Terminal completion returns the family box through the stable entry,
and the still-active outer operation reclaims that exact allocation before
acquiring again or closing.

### Wait and cancellation behavior

A fresh attempt reserves one move-only `PendingClaimToken` before manager
entry. Immediate compatibility installs one physical family without allocating
a completion or waiter slot. A blocked attempt allocates one `Completion<()>`
and one reusable `WaitNode` containing the same exact owner, `ClaimNo`, and
target mode.

Queued nodes carry intrusive `prev` and `next` links. Promotion detaches the
maximal compatible FIFO prefix, installs counted physical `Provisional`
families, marks the nodes `Provisional`, drops manager synchronization, and
then publishes their one-shot notifications. Provisional families fully
participate in compatibility.

One transition accumulates notifications in
`DeferredNotifications::{None, One, Many}`. The value is published only after
resource synchronization is released; its Drop fallback prevents a committed
promotion from silently losing wakeups during an early return or unwind. Each
wait node still owns exactly one completion and observer.

The unique observer uses `wait_take_result()`, stages its exact family and scope
records under the armed guard, and then validates the token, node generation,
fields, node phase, and provisional family in one manager transition.
Observation changes `Provisional` to `Held` and reclaims the node. There is no
`.await` during this staged transfer.

Dropping `PendingClaimGuard` synchronously removes only its exact queued node,
provisional physical family and node, immediate physical family, or matching
partial local publication. Family mutation remains exclusively borrowed for
the lifetime of the acquisition future, so no separate lifecycle path can
release its pending state while the future remains live. Duplicate pending
observer sharing is not supported.

After first poll, the caller must eventually continue polling the acquisition
future or drop it. Retaining it indefinitely without polling intentionally
retains its queued request or provisional physical reservation and may block
other acquisitions. No timeout, lease, watchdog, or background reclamation is
provided.

### Current complexity

Let `M = 4` be the fixed mode count, `K` the number of waiters promoted by one
transition, and `H_scope` the claims indexed by one exact scope.

Implemented costs are:

| Operation | Current cost |
|---|---:|
| Repeated exact coverage | `O(1)` owner-local |
| Covered cross-scope publication | `O(M)` owner-local |
| Fresh physical acquisition | `O(M)` shared average |
| Immediate conversion | `O(M)`; shared only if physical mode changes |
| Mode-preserving release | `O(M)` owner-local |
| Last-family physical removal | `O(M + K * M)` |
| Cancel one queued waiter by token | `O(1)` plus promotion work |
| Promote `K` waiters | `O(K * M)` |
| Exact-scope cleanup | `O(H_scope + physical changes + promotion work)` |

Production cleanup uses the exact scope index and has no accepted-owner manager
scan or `release_owner()` fallback.

### Behavioral constraints worth preserving

The implementation preserves:

1. FIFO-compatible prefix granting.
2. A fresh compatible request waits behind an older incompatible waiter.
3. A covering held same-family claim publishes locally without queue access.
4. Blocking conversion is unsupported.
5. Cancellation after promotion but before observation cannot leak a grant.
6. DDL rejects explicit same-session table locks in the current behavior.
7. Cleanup remains proportional to one exact scope's indexed resources.

The design intentionally narrows one former manager behavior. Concurrent
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
    -> check the complete transaction core in as CleanupReady
    -> worker rolls back transaction effects
    -> release transaction table bindings
    -> release transaction-owned logical locks
    -> consume ReleasedTransactionLocks at session rollback completion
```

The callback future is destroyed before its `StmtState`, so a queued waiter or
promoted-but-unobserved request is cancelled by its call-local pending guard
before the core becomes cleanup-claimable. An accepted transaction claim is not
released inline; it remains attached to `TrxInner` until whole-transaction
rollback reaches the ordering above.

The proof covers the implemented closed transaction scope and owns the returned
family root. Physical aggregation preserves this terminal ordering.

## Design Overview

### Canonical owner identity

Canonical exact identity is:

```rust
struct LockOwner {
    family: LockFamily,
    scope: LockScope,
}

struct LockFamily(SessionID);

enum LockScope {
    SessionExplicit,
    Transaction(TrxID),
    Operation(OperationID),
}
```

`LockOwner` is used for claims, waiters, cleanup, tokens, diagnostics, and
purpose-specific policy. `LockFamily` is used for physical conflict
aggregation and same-session policy. Purpose-specific policy is selected by
typed session-operation authority, never recovered from `OperationID`.

Constructors must enforce that transaction ids belong to the declared session
family. Operation ids are monotonic only within one session; equal raw ids in
different families remain distinct exact owners.

### Serialized family ownership

One logical execution owner has lock-mutation authority for a `LockFamily`.
That authority covers every session-explicit, transaction, DDL, and maintenance
scope in the family. At most one acquisition, release, conversion, or scope
cleanup may be active in the family, including while an acquisition awaits an
external blocker.

This is not OS-thread affinity. A lock-mutating operation holding mutable
session access, transaction checkout, or operation guard may
move between executor threads while retaining exclusive authority. Different
families continue to execute concurrently.

The outer lifecycle owns and transfers this authority. Session teardown,
transaction completion and operation-guard cleanup must first
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
transaction in its stable entry while the outer `&mut Session` call remains
borrowed. Mandatory execution owns that transaction through normal terminal
completion; there is no caller-abandonment cleanup boundary between its
statements. On a supervised panic, the private checkout is synchronously
parked before the operation and its family authority are retained as failed.

The public `Session` handle remains movable between threads but is not
shareable: its local closed flag uses `Cell<bool>`, making the type `Send` and
`!Sync`. Consequently, an async lock-free read borrowing `&Session` is not a
`Send` future. This is separate from the lock-serialization proof. Mutable
access serializes one handle, registry admission excludes its detached
transaction, and the DDL cleanup handoff covers internal nesting. If
Doradb later adds parallel execution within one session, workers must submit
lock mutations through one family coordinator.

### Dual indexing

The design maintains both directions:

```text
session/transaction/operation runtime
└── LockScopeState(owner)
    └── resource -> ScopeClaim

LockManager
└── resource -> ResourceState
    └── family -> held, queued, or provisional physical state

active acquisition call
└── PendingClaimGuard -> pending token and optional waiter
```

Neither index replaces the other:

- owner-side family and scope state make exact lookup and cleanup targeted;
- resource state makes conflict checks and FIFO transitions atomic.

The lifecycle object for an exact owner owns its `LockScopeState` exclusively.
The manager does not retain another strong reference to the scope. Pending
state belongs to the active acquisition call until it either transfers a
granted claim into the scope or cancels.

### One physical holder, multiple exact claims

For one `(resource, family)`:

```text
manager PhysicalFamilyState = Held | Queued | Provisional
owner LocalFamilyResourceState
└── three fixed exact-scope-class slots
```

The manager state is exactly one physical entry. Exact claims retain their
lifetime, mode, purpose, and `ClaimNo` only in the owner-side slots and scope
indexes. A pending node retains its FIFO position and exact token identity until
it completes or is cancelled.

## Core Data Structures

The following structures describe the implemented ownership split.

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
| `Operation` (DDL) | one DDL operation guard | DDL success or failure |
| `Operation` (maintenance) | one maintenance operation guard | maintenance success or failure |

`LockScopeState` is the implemented exact-scope cleanup index. It gives session
explicit locks, DDL, and maintenance the same targeted cleanup mechanism used
by transactions.

An acquisition borrows the family execution authority and target scope
exclusively across `.await`. A pending request is therefore not inserted into
the scope map. Its call-local guard owns the pending claim token and, while
blocked, its waiter-node id and completion. When promotion is observed, the
guard stages both owner-side records, atomically adopts the provisional
physical family, and reclaims the node. The armed guard owns rollback until it
consumes the pending token into its accepted token.

Scope cleanup begins only after the active family operation has completed or
been cancelled. It consumes the uniquely owned scope and releases its accepted
claims. There is no concurrent closer or pending scope entry to reconcile.

The normal access path is:

```text
exclusive family operation
    -> inspect the exact scope's claim map
    -> publish locally when the family physical mode is unchanged
    -> otherwise perform one synchronous LockManager resource transition
    -> return an immediate physical claim, or create a call-local waiter guard
    -> await Completion<()> without manager synchronization
    -> stage both owner-side records under token-exact rollback
    -> observe the provisional family and reclaim the waiter node
    -> consume PendingClaimToken into ClaimToken

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
    claim_no: ClaimNo,
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
    granted_counts: [u32; MODE_COUNT],
    grant_mask: ModeMask,
    families: FastHashMap<LockFamily, PhysicalFamilyState>,
    wait_queue: WaitQueue,
}

enum PhysicalFamilyState {
    Held { mode: LockMode },
    Queued { node_id: WaitNodeID },
    Provisional { mode: LockMode, node_id: WaitNodeID },
}
```

Owner-side family state embeds:

```rust
struct LocalFamilyResourceState {
    claims: FamilyClaimSlots, // session, operation, transaction
    claim_mask: ModeMask,
    covering_mode: LockMode,
}
```

The manager family state is exclusive. `Held` and `Provisional` are counted
physical holders; `Queued` is linked but uncounted. Exact lookup, directional
family validation, DDL policy, cleanup, and accepted-claim diagnostics all use
the fixed owner-side slots.

`ResourceState` is removed only when its family map and linked queue are empty,
all counts and mask bits are zero, and the waiter slab has no live queued or
provisional nodes.

### Persistent waiter state

```rust
struct WaitQueue {
    head: Option<WaitNodeID>,
    tail: Option<WaitNodeID>,
    nodes: WaitNodeSlab,
}

struct WaitNode {
    owner: LockOwner,
    claim_no: ClaimNo,
    target_mode: LockMode,
    phase: WaitNodePhase,
    completion: Arc<Completion<()>>,
}

enum WaitNodePhase {
    Queued {
        prev: Option<WaitNodeID>,
        next: Option<WaitNodeID>,
    },
    Provisional,
}
```

Promotion detaches a node from the FIFO links but does not reclaim its slab
slot. The node remains addressable until the provisional claim is observed or
cancelled.

The existing success-only `Completion<()>` has independent `Arc` lifetime, so
the acquisition guard can listen without borrowing the queue node or retaining
resource synchronization. It reports only that manager state changed; the
observer takes the result once and validates the authoritative node phase.

### Resource-qualified tokens

```rust
struct PendingClaimToken {
    resource: LockResource,
    owner: LockOwner,
    claim_no: ClaimNo,
}

struct ClaimToken {
    resource: LockResource,
    owner: LockOwner,
    claim_no: ClaimNo,
}
```

`WaitNodeID` contains a resource-local slab slot and slot generation. An
occupied queued or provisional node pins its `ResourceState`, so no
waiter id can cross resource destruction and recreation; no resource
incarnation counter is needed. `ClaimNo` is allocated from the session-family
authority with checked arithmetic before policy validation or manager entry.
A stale accepted token therefore cannot release a later reacquisition by the
same exact owner on the same resource. Slab generations also advance with
checked arithmetic before reclamation and never wrap.

### Call-local pending acquisition

One call-local guard owns a waiting acquisition:

```rust
struct PendingClaimGuard<'a> {
    manager: &'a LockManager,
    family: &'a mut FamilyLockState,
    curr_scope: &'a mut LockScopeState,
    token: Option<PendingClaimToken>,
    requested_mode: LockMode,
    state: PendingGuardState,
    transfer_started: bool,
}

enum PendingGuardState {
    NotStarted,
    LocalCovered,
    Waiting {
        node_id: WaitNodeID,
        completion: Arc<Completion<()>>,
    },
    FreshGranted,
    Disarmed,
}
```

After notification the guard stages both local records, observes and reclaims
the provisional node, and changes from `Waiting` to `FreshGranted`. It disarms
only after the local records own the claim and the pending token is consumed.
Dropping in `LocalCovered` rolls back only matching local records. Dropping in
`Waiting` cancels a queued or provisional waiter and matching partial
publication. Dropping in `FreshGranted` rolls back local publication and
releases the physical family. Immediate fresh grants use the same transfer
discipline without waiter or completion allocation.

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
no claim, the family slots first apply directional coverage. A covering family
mode publishes the new exact claim locally. Only the family's first physical
claim enters the manager. Finding an existing queued waiter for the same family
is an invariant violation because the waiting call still owns the family
execution authority.

### 2. Fresh immediate acquisition

For a fresh request:

1. Reserve a family-local `ClaimNo`.
2. Apply purpose-specific and directional family policy owner-locally.
3. If an existing family mode covers the request, publish both exact indexes
   locally and return.
4. Otherwise lock the manager resource, validate the family miss, fixed-count
   compatibility, and FIFO policy.
5. If immediately grantable, install one counted `Held` family.
6. Release manager synchronization.
7. Publish both owner-side records under the still-armed guard.

### 3. Enqueue a waiter

If a fresh request cannot be granted:

1. Allocate a slab node.
2. Link it at the FIFO tail.
3. Install the family's single `Queued` physical state.
4. Return `WaitNodeID` plus `Arc<Completion<()>>`.
5. Construct a `PendingClaimGuard`.
6. Release all synchronous locks.
7. Await the completion event while retaining exclusive family authority.

Enqueue is `O(1)` average.

### 4. Wait for completion

The observer uses the completion's listener-before-check take:

```text
wait_take_result
    -> stage token-matching exact local records
    -> reacquire the resource once
    -> Provisional: change family to Held and reclaim the node
```

Registering first prevents a lost wakeup.

### 5. Promote the FIFO prefix

Every transition that may reduce blocking runs one central grant loop while
holding the resource-state lock:

```text
while queue head is promotable:
    detach head links
    change family Queued -> Provisional
    increment the requested physical mode count and mask
    mark waiter node Provisional
    queue completion for notification
```

Notifications occur after the resource lock is released.

Promotion eligibility checks compatibility with the fixed counts of external
physical families. Family serialization and the exclusive `Queued` state mean
no accepted same-family mutation coexists with the waiter.

The loop promotes the maximal compatible FIFO prefix.

### 6. Observe a promoted waiter

The waiting family operation takes its completion and observes manager state:

```text
pending guard inserts matching family and scope records
    -> manager validates resource, family, slot generation, pending owner,
       ClaimNo, requested mode, node phase, and provisional family
    -> change Provisional -> Held
    -> reclaim waiter slab node
    -> pending guard changes Waiting -> FreshGranted
    -> consume PendingClaimToken into ClaimToken
    -> disarm the guard
```

Observation does not change compatibility and does not rerun queue granting.
There is no `.await` between adopting the claim and transferring it into the
scope. If unwinding occurs in that interval, the still-active guard releases
the fresh claim.

### 7. Cancel a pending acquisition

Dropping an active `PendingClaimGuard`:

1. In `Waiting`, cancel the queued or provisional waiter, reclaim its node, and
   rerun FIFO-prefix granting.
2. In `Granted`, release the fresh claim and rerun granting through the normal
   claim-release transition.

Any staged token-matching local records are rolled back. Guard drop finishes
synchronously before family execution authority can move to another operation
or scope cleanup.

### 8. Release an exact claim

Releasing a claim:

1. Validate its exact scope and family slots and token-matching `ClaimNo`.
2. Compute the remaining owner-side mask and actual covering mode.
3. If the physical mode is unchanged, remove both exact entries locally.
4. If no exact claim remains, remove the physical family, update counts and
   mask, and promote the maximal compatible FIFO prefix.
5. If a different live mode remains, assert the lifecycle-order violation
   before manager or owner-side mutation.
6. Commit the staged local removal.

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
   - compute the candidate family covering mode from the fixed slots;
   - when physical mode is unchanged, update both exact indexes locally;
   - otherwise the resource queue must be empty and `R` must be immediately
     compatible with external family holders after excluding this family;
   - on failure return `LockUpgradeWouldBlock`; on success update the physical
     family and both exact indexes.
3. If neither covers the other, return `LockConversionNotSupported`.

The claim keeps its session-local `ClaimNo` across a successful in-place conversion.
Release of that claim releases its current mode.

Different exact owners cannot strengthen an earlier owner's claim. They can
only add a claim covered by every existing same-family claim.

### 11. Queue bypass

A fresh first-family request waits behind a non-empty queue. A claim covered by
the family's accepted physical mode is owner-local and does not inspect the
queue. No same-family waiter can coexist with that request because family lock
mutation is serialized.

### 12. DDL and explicit session locks

The implementation preserves the current rule:

> DDL rejects a target resource when its family has a held `SessionExplicit`
> owner on that table.

This is checked in the family-local `SessionExplicit` slots before manager
entry:

```text
explicit_owner = LockOwner {
    family: ddl_owner.family,
    scope: SessionExplicit,
}

reject when metadata or data slot contains SessionExplicit
```

The check is bounded and does not depend on the physical holder mode. A queued
or provisional session-explicit request cannot coexist with DDL because both
require the family's exclusive execution authority.

Metadata and data resources are acquired in normal order. If later acquisition
or table validation fails, only exact claims newly created by that DDL scope are
released.

## Same-Family Physical Mode

The physical family mode is the strongest actual exact claim under `covers()`.
It is not a lattice join. Production ownership has two nesting chains:

```text
SessionExplicit -> PublicTransaction
SessionExplicit -> Operation -> PrivateTransaction
```

Public transactions and operations are alternatives in the session operation
slot. Directional admission requires every live outer claim to cover a child
request. Explicit unlock is idle-only, private transactions return the family
authority before their operation closes, and transaction cleanup precedes
session-explicit cleanup. A production release can therefore preserve the
physical mode or remove the last family claim; it cannot select a different
live mode.

If `S` and `IX` would coexist, directional admission rejects the later request.
The manager never manufactures `X` to represent them.

## Implemented Complexity

Let:

- `M` be the fixed mode count, currently four;
- `K` be the number of waiters actually promoted by a transition;
- `H_scope` be the number of accepted claims held by one scope; and

Hash-map costs below are average costs. `M` is constant.

| Operation | Implemented cost |
|---|---:|
| Repeated covered exact-owner acquisition | `O(1)` local |
| Covered different-owner family claim | `O(M)` owner-local |
| Fresh immediate physical acquisition | `O(M)` shared average |
| Exact-owner lookup | `O(1)` average |
| Same-family directional validation | `O(M)` |
| Immediate conversion | `O(M)` |
| Enqueue waiter | `O(1)` average |
| Unlink queued waiter by token | `O(1)` |
| Observe provisional grant | `O(1)` average |
| Mode-preserving exact release | `O(M)` owner-local |
| Last-family exact release | `O(M + K * M)` |
| Cancel one pending acquisition | `O(M + K * M)` |
| Promote `K` waiters | `O(K * M)`, effectively `O(K)` |
| Consume one scope | `O(H_scope + total promoted work)` |
| Manager resource debug snapshot | `O(physical families + waiters)` |

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
   `Completion<()>`; the exclusive async caller stages local state and then
   observes the provisional node once.
7. Notifications occur after releasing resource synchronization.
8. Resource state may be removed only after every grant and occupied waiter
   node is gone; this prevents a waiter id from crossing resource recreation.
9. Counter/mask updates and their maps are one atomic resource-state
   transition.

The implementation maintains debug assertions for:

- counts matching map contents;
- masks matching nonzero counts;
- exactly one physical holder count per held or provisional family;
- owner-local physical mode covering every accepted exact claim;
- queue-link and `Queued` family-state agreement;
- at most one queued waiter for a family;
- provisional node and provisional claim agreement;
- unique session-family claim numbers among live claims; and
- empty family state before removal.

## Fresh Versus Existing Claims

Multi-resource helpers distinguish a fresh exact grant from an existing
one so failure rollback does not release an older claim.

That distinction is preserved at the exact-claim layer:

```text
Fresh    = this operation created the exact owner/resource claim
Existing = the exact owner already had a covering claim
```

It must not confuse this with physical family-holder creation. A fresh exact
private-transaction claim may reuse an existing physical operation holder.

DDL and maintenance use unique operation scopes, so closing a failed operation
releases only its own claims. Transaction mutations are already serialized by
their runtime ownership. The redesign extends that serialization
to session explicit and cross-scope family mutations. While a multi-resource
helper is active, no later family operation can depend on one of its fresh
claims, so failure rollback may release exactly the claims created by that
helper without acquisition refcounts.

## Diagnostics

Diagnostics expose both indexes without changing their ownership.

### Physical resource view

The lock manager reports, for each resource:

- held, queued, and provisional physical family state;
- physical mode and fixed counts/mask;
- FIFO queue order;
- exact owner and `ClaimNo` only for pending waiter state;
- waiter slot/generation, phase, and target mode; and
- slab slot length, retained capacity, live count, free-list order, and
  generations.

### Exact logical resource view

The family authority can report, for each family/resource:

- fixed-slot occupancy;
- exact owner scope, mode, and accepted `ClaimNo`;
- exact mode mask and actual covering mode; and
- accepted resources per exact scope.

### Owner scope view

A live `LockScopeState` can report:

- exact owner;
- accepted resource claims;
- claim modes and tokens.

`LockManager` does not strongly own or globally enumerate accepted scope
states. If future system-wide inspection requires enumeration,
the observability design may add a weak scope registry or aggregate snapshots
through the lifecycle owners.

Debug snapshots should clearly separate physical holders from logical claims.
Counting only the physical family holder is insufficient to prove that a
transaction or operation owns the required claim.

`Session::logical_lock_stats()` returns the public cumulative
`LogicalLockStats` snapshot. It separates owner-local covered
hits/publications/conversions/releases from manager transitions, fixed mode
slots examined, enqueue/link/cancellation/promotion work, scope-close work,
completion and slab allocation classes, and current/peak physical resource,
family, and waiter cardinalities. Shared-path counters use relaxed manager
atomics. `scope_close_physical_changes` counts claims whose indexed scope close
removed the family's physical entry because no exact claim remained.
Owner-local counters remain plain family data and are aggregated once when
final session authority closes.

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

The implementation distinguishes DDL and explicit claims, so the original
cleanup ambiguity is absent. The rejection remains intentionally for behavioral
compatibility until a separate semantic decision changes it.

### Single-process, session-family model

`LockFamily(SessionID)` assumes one engine process and one logical execution
family per session. Distributed ownership, cross-process lock recovery, and
independent parallel lock mutation within one family are outside this design.

### Serialized family lock mutation

Session, transaction, DDL, and maintenance work in one family may
hold claims at the same time, but their lock-manager transitions are
serialized. Session explicit lock and unlock already require mutable access,
and effectful public Session admission is idle-only while a detached transaction exists.
Parallel workers may use protection acquired by their coordinator, but must
send new acquisitions, conversions, releases, and cleanup through that single
family execution owner. Nested DDL cleanup uses the same move-only family
authority handoff.

### No lock escalation

Covered exact claims use owner-local fixed slots. The design does not add lock
escalation or a separate weak-lock migration barrier.

### Deadlock handling is external

No deadlock policy is designed here. See
[backlog 000167](./backlogs/000167-logical-lock-deadlock-handling.md).

## Resolved and Remaining Design Choices

### Token allocation strategy

RFC-0027 uses resource-local generational waiter slots and session-family
`ClaimNo` allocation. Every occupied waiter node pins the resource state, so
no resource-incarnation or global waiter/claim counter is required.

### Family aggregate representation

The selected representation uses manager-side per-mode physical counts and a
mask plus owner-side fixed session, operation, and transaction slots. Accepted
exact claims are not stored in manager state. One family's
pending state is represented by its `Queued` or `Provisional` entry and one
generational waiter node.

### 3. Resource sharding

The current `FastDashMap` supplies resource-level shard synchronization.
Explicit repartitioning remains possible future work and would require:

- stable partition hashing;
- one mutex per partition;
- per-partition resource maps; and
- clearer aggregate statistics.

Partition count, hash quality, and hot-resource contention evidence.

### 4. Additional purpose-specific family policy

DDL versus `SessionExplicit` is an implemented policy exception. Maintenance
uses ordinary directional coverage and retains a distinct exact claim. Future
internal operation scopes may need additional explicit rules.

Any future addition must specify:

- which other pairs of already-held scope purposes require policy beyond
  directional coverage; and
- how purpose checks become atomic with family/resource admission.

Family execution serialization is not a purpose policy. It determines when
requests run; these checks determine whether a new exact claim may coexist with
claims retained by earlier scopes.

### 5. Removal-only release

Release either preserves the current physical mode or removes the family's last
physical claim. A candidate different live mode is an invariant violation
asserted before manager or owner-side mutation. There is no public or internal
release-time downgrade API; same-scope immediate strengthening remains
supported.

### 6. Observability boundary

Internal physical and exact debug snapshots and public cumulative
`LogicalLockStats` are implemented. Future observability may add:

- global enumeration of live scope states and, if needed, a weak registry;
- per-mode holder/waiter counters;
- queue length and wait-duration metrics;
- cancellation and stale-token counters;
- shard-lock contention measurements;
- structured wait tracing; or
- user-visible lock inspection.

### 7. Migration and compatibility

The completed phased migration:

- preserve the implemented canonical `LockFamily` plus exact `LockScope`
  identity and `LockFamilyConflict` error;
- preserve the implemented `&mut self` session lock mutation APIs and
  `Session: Send + !Sync` boundary;
- preserve idle-only effectful public Session admission while a transaction is active or
  undergoing terminal/abandoned cleanup;
- serialized lock mutation across all scopes before removing duplicate-waiter
  support; and
- retained cancellation safety through the call-local pending guard.

### 8. Nested DDL transaction cancellation

DDL reserves one typed operation authority and then starts a private catalog
transaction inside the same stable entry and `&mut Session` call. Normal
execution serializes DDL-scope and
transaction-scope mutations, but dropping the outer future can abandon the
transaction and queue asynchronous rollback while DDL guards synchronously
release their claims.

The implemented ownership transfer guarantees:

- pending-acquisition cancellation finishes first;
- DDL-scope cleanup and nested transaction cleanup cannot overlap;
- transaction claims still close before the Session becomes idle; and
- the Session remains unavailable until all transferred cleanup completes.

The transferred boxed family authority and terminal cleanup proof serialize
these internal cleanup paths.

## Implemented RFC-0027 Stages

These stages record the completed progression.

### Stage A: canonical identity (implemented)

- Introduce `LockOwner { family, scope }`.
- Use the session-local operation id shared by DDL and maintenance authorities.
- Preserve idle-only effectful public Session admission and the existing mutable
  explicit-lock APIs.
- Establish the original vector/deque resource representation and exact-owner
  release used before the waiter cutover.

### Stage B: exclusive scope ownership (implemented)

- Define the DDL operation-guard/nested-transaction cleanup handoff before
  declaring family cleanup serialized.
- Serialize lock mutation across every scope in one family.
- Add uniquely owned `LockScopeState` claim maps.
- Route transaction, session, DDL, and maintenance cleanup through scope
  indexes.
- Preserve proof-bound transaction-lock cleanup before session completion.

### Stage C: tokenized waiter and claim lifecycle (implemented)

- Carry session-local `ClaimNo` through move-only pending and accepted tokens.
- Add resource-local generational waiter nodes and intrusive FIFO links.
- Keep promoted nodes alive through provisional state until the unique
  observer consumes them or its pending guard drops.
- Reuse `Completion<()>` as a success-only one-shot notification.
- Add the call-local `PendingClaimGuard` across manager and owner-side transfer.
- Remove duplicate-waiter sharing after all family mutations are serialized.

### Stage D: physical family aggregation (implemented)

- Store exact claims only in fixed owner-side slots.
- Collapse physical grants to one holder per family/resource.
- Add fixed physical counts/mask and owner-local claim masks.
- Make provisional claims fully granted for arbitration.
- Preserve directional same-family admission and queue bypass.
- Make DDL purpose checks atomic with resource admission.

### Stage E: aggregate compatibility and intrusive FIFO (implemented)

- Add physical holder counts and masks.
- Replace granted-vector scans.
- Replace queue rebuilding with intrusive token unlink.
- Centralize all blocker-removal transitions through the FIFO-prefix grant loop.
- Add invariant-rich physical and exact debug snapshots.

### Stage F: benchmark-led refinement (implemented)

- Measure hot shared metadata resources, cancellation, scope cleanup, and queue
  promotion.
- Select fixed exact slots and retain the existing resource map.
- Expose structural statistics for paired optimized-build measurement.

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
10. First touch acquires transaction metadata S before metadata resolution and
    retains an accepted claim after ordinary admission failure.
11. Session explicit lock and unlock require mutable access.
12. Lock-free session observations accept immutable observer admission without
    allocating an operation id, while lock-bearing operations require mutable
    access and an idle coordinator slot.
13. `Session` remains `Send` and is not `Sync`.
14. Session, transaction, DDL, and maintenance lock mutations in one
    family never overlap.

### Queue and cancellation tests

1. One family may retain at most one queued waiter.
2. Dropping a waiting acquisition guard unlinks a queued waiter.
3. Dropping a promoted acquisition guard removes its provisional physical
   family and matching staged local state.
4. Observation validates staged local state, changes the physical family to
   held, and reclaims the waiter node.
5. Accepting both exact indexes disarms the guard.
6. Unwinding before disarm releases the physical family and local claim.
7. Cancelling the head reconsiders the next waiter.
8. Last-family physical removal promotes newly compatible waiters.
9. Removing a same-family claim with unchanged physical mode remains local.
10. A covering same-family claim publishes locally.

### Token and lifecycle race tests

1. Stale waiter slot generation cannot affect a reused slot.
2. A resource cannot be recreated until every old waiter node is consumed.
3. A stale accepted claim number cannot release a later claim.
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

Randomized transition tests compare the optimized structure with a
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
3. Direct transaction-owned first-touch admission and terminal release.
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

## Normative Invariants

1. One exact owner identifies each session-explicit scope, transaction, DDL
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
10. Provisional physical families are fully granted for arbitration.
11. Tokens remain unambiguous across resource and family recreation.
12. Stale resource, waiter, and claim tokens have no effect.
13. Different-owner same-family admission is directional and validates every
    held claim.
14. Incomparable modes are rejected and never joined.
15. Only immediate exact-owner conversion may strengthen a family holder.
16. Release preserves the live physical family mode or removes the last claim;
    a different candidate mode asserts before mutation.
17. Every blocker or queue-barrier removal reruns FIFO-prefix granting.
18. Notifications occur after resource synchronization is released.
19. DDL preserves explicit same-session rejection unless separately changed.
20. Scope cleanup never scans unrelated resources.
21. Transaction scope cleanup precedes session transaction completion.

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
