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

The current manager represents session-, transaction-, and statement-owned
locks as separate physical grants. Each resource stores grants in a `Vec` and
waiters in a `VecDeque`. This is functionally small and cancellation-aware, but
many operations scan all grants or rebuild the whole queue.

The working design separates:

1. **Physical conflict participation**: one holder per session family and
   resource.
2. **Exact logical ownership**: one claim per session, transaction, statement,
   DDL operation, or maintenance operation.
3. **Pending acquisition identity**: one FIFO waiter per exact owner, with
   duplicate subscribers allowed only for that exact owner.

The two primary indexes become:

```text
scope/owner side                         resource side
----------------                         -------------
LockScopeState                           ResourceState
└── resource -> OwnerResourceCell        └── family -> FamilyResourceState
                                              ├── optional physical holder
                                              ├── exact claims
                                              └── exact queued waiters
```

The scope index provides targeted cleanup. The resource index provides conflict
arbitration, FIFO queueing, physical family aggregation, and diagnostics.

The intended result is:

- average `O(1)` compatibility and exact-owner lookup;
- `O(1)` enqueue and queue unlink;
- `O(K)` work to promote `K` actual waiters;
- cleanup proportional to one scope's indexed resources rather than the whole
  lock table; and
- preservation of the current coverage, conversion, FIFO, cancellation, and
  same-session semantics.

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
| Freeze/checkpoint | scoped `S` | scoped `IS` | maintenance operation |
| CREATE TABLE on a new id | scoped `X` | none | DDL operation |
| DROP TABLE | scoped `X` | scoped `X` | DDL operation |
| CREATE/DROP INDEX | scoped `X` | scoped `X` | DDL operation |

On first touch, statement metadata protection is handed to the transaction
without a gap:

```text
statement acquires Metadata(S)
    -> resolve and validate the binding
    -> transaction acquires Metadata(S)
    -> release statement Metadata(S)
```

Successfully bound reads therefore retain metadata protection until transaction
commit or rollback. Repeated operations use the transaction binding and lock
cache.

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
exact owner `O`, every provisional or held claim and every queued waiter from a
different owner in the same family must cover `R`. Otherwise the request
returns the current `LockOwnerGroupConflict` error.

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
transaction IX -> session X     conflict
session S -> transaction IX     conflict
transaction IX -> statement S   conflict
```

The rule is evaluated against every exact claim and waiter, not only the
family's strongest physical mode.

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
    owner_group: Option<LockOwnerGroup>,
    mode: LockMode,
}
```

Session, transaction, and statement owners from one session may therefore
produce three granted entries for the same resource even though they are one
external conflict participant.

### Owner-local cache

Transactions and statements use:

```rust
struct OwnerLockState {
    owner: LockOwner,
    owner_group: Option<LockOwnerGroup>,
    held: FastHashMap<LockResource, LockMode>,
}
```

This cache is already useful:

- repeated covered requests are local;
- release iterates resources known to that owner;
- transaction locks can move through prepare/precommit ownership; and
- statement drop releases statement-owned resources deterministically.

Session explicit locks do not yet have the equivalent cache and still use a
global `release_owner()` scan during session cleanup.

DDL and maintenance also have no distinct exact-owner identity today. They
reuse `LockOwner::Session` and rely on fresh-versus-existing guards so scoped
cleanup does not release a pre-existing explicit session grant. DDL performs a
separate preflight rejection because the shared identity cannot otherwise
distinguish its temporary claim from an explicit session claim. This purpose
overloading motivates the working design's DDL and maintenance scope ids.

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
- `H_owner` be resources cached by one transaction or statement owner; and
- `R` be resource entries in the whole manager.

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
| Transaction/statement cleanup | sum of release costs over `H_owner` |
| Session cleanup by global scan | `O(R log R)` plus per-resource release work |

These costs are not automatically problematic for small queues. The design
concern is that they grow with all exact owners on a hot resource and perform
repeated scans under the resource shard lock.

The `O(R log R)` session-cleanup term comes from snapshotting and sorting all
resource keys before scanning them. Transaction and statement cleanup already
use their owner-local caches and do not pay this global scan.

### Behavioral constraints worth preserving

The redesign must preserve:

1. FIFO-compatible prefix granting.
2. A fresh compatible request waits behind an older incompatible waiter.
3. A covering granted same-family claim may permit queue bypass.
4. A covering queued same-family waiter validates a request but does not permit
   bypass.
5. Exact-owner duplicate waiters share without changing the queued mode.
6. Stronger waiting requests do not rewrite an earlier waiter.
7. Blocking conversion is unsupported.
8. Cancellation after promotion but before observation cannot leak a grant.
9. DDL rejects explicit same-session table locks in the current behavior.
10. Cleanup wakes acquisitions with `LockWaiterReleased`.

### Proof-bound terminal cleanup ordering

Every terminal user-transaction path drains its owner-local `OwnerLockState`
before finishing the session transaction lifecycle. Transaction code mints one
non-cloneable, transaction-id-bound `ReleasedTransactionLocks` proof only after
the local state is empty. Prepared and precommit paths also consume and drop
their retained lock-manager guard before minting the proof.

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

The proof covers the current owner-local cache contract, not the future scope
representation. A redesign may evolve it into a closed-scope proof, but must
preserve this terminal ordering.

## Working Design Overview

### Canonical owner identity

The separate owner and owner-group concepts become one exact identity:

```rust
struct LockOwner {
    family: LockFamilyId,
    scope: LockScopeId,
}

struct LockFamilyId(SessionID);

enum LockScopeId {
    SessionExplicit,
    Transaction(TrxID),
    Statement {
        trx_id: TrxID,
        stmt_no: StmtNo,
    },
    Ddl(DdlOperationID),
    Maintenance(MaintenanceOperationID),
}
```

`LockOwner` is used for claims, waiters, cleanup, tokens, diagnostics, and
purpose-specific policy. `LockFamilyId` is used for physical conflict
aggregation and same-session policy.

Constructors must enforce that transaction and statement ids belong to the
declared session family. DDL and maintenance operation ids must be unique for
the engine lifetime.

### Dual indexing

The design maintains both directions:

```text
LockScopeState
    owner -> resources acquired or being acquired

ResourceState
    resource -> families -> exact claims and waiters
```

Neither index replaces the other:

- scope state makes cleanup targeted and closes acquisition admission;
- resource state makes conflict checks and FIFO transitions atomic.

### One physical holder, multiple exact claims

For one `(resource, family)`:

```text
FamilyResourceState
├── zero or one physical holder
├── zero or more provisional/held exact-owner claims
└── zero or more queued exact-owner waiters
```

Only the physical holder is collapsed. Exact claims retain their lifetime and
purpose. Exact waiters retain their FIFO position.

## Core Data Structures

The following structures describe the working direction. Names and integer
widths remain open to implementation refinement.

### Scope state and owner-resource cell

```rust
struct LockScopeState {
    owner: LockOwner,

    // Makes close idempotent and ensures only one caller drains the scope.
    close_gate: Mutex<()>,

    inner: Mutex<LockScopeInner>,
}

struct LockScopeInner {
    lifecycle: ScopeLifecycle,
    resources: FastHashMap<LockResource, Arc<OwnerResourceCell>>,
}

enum ScopeLifecycle {
    Open,
    Closing,
    Closed,
}

struct OwnerResourceCell {
    resource: LockResource,
    state: Mutex<OwnerAcquireState>,
}

enum OwnerAcquireState {
    Vacant {
        generation: u64,
    },
    Pending {
        waiter_token: WaiterToken,
        completion: Arc<WaitCompletion>,
        target_mode: LockMode,
        generation: u64,
        subscribers: FastHashSet<SubscriberID>,
    },
    Granted {
        claim_token: ClaimToken,
        mode: LockMode,
        generation: u64,
    },
    Closed {
        generation: u64,
    },
}
```

A cell is inserted before the manager transition begins. No scope or cell mutex
is retained across `.await`.

The simplest correct first implementation retains `Vacant` cells in the scope
map until the scope closes. This prevents an acquisition holding an older
`Arc<OwnerResourceCell>` from creating a claim after the cell was removed from
the reverse index. The memory and cleanup cost of retained vacant cells is an
explicit open question.

State transitions include:

```text
Vacant  -> Pending
Vacant  -> Granted
Pending -> Granted
Pending -> Vacant   // last subscription cancels
Granted -> Vacant   // explicit resource release
any     -> Closed   // scope cleanup
```

Every transition back to `Vacant`, and every transition to `Closed`, advances
the local generation.

### Resource and family state

```rust
struct ResourceState {
    incarnation: ResourceIncarnation,

    // Counts physical family holders, not exact claims.
    granted_counts: [u32; MODE_COUNT],
    grant_mask: ModeMask,

    families: FastHashMap<LockFamilyId, FamilyResourceState>,
    wait_queue: WaitQueue,
}

struct FamilyResourceState {
    holder: Option<FamilyHolder>,

    claims: FastHashMap<LockOwner, ClaimRecord>,
    claim_counts: [u32; MODE_COUNT],
    claim_mask: ModeMask,

    queued_waiters: FastHashMap<LockOwner, WaiterToken>,
    queued_waiter_counts: [u32; MODE_COUNT],
    queued_waiter_mask: ModeMask,
}

struct FamilyHolder {
    physical_mode: LockMode,
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

The per-mode family counts avoid scanning all exact claims and waiters during
directional admission or holder recomputation. The maps remain necessary for
exact lookup, token validation, DDL policy, cleanup, and diagnostics.

`FamilyResourceState` exists while it has any queued waiter or any provisional
or held claim. It is removed only when all three are absent:

```text
holder == None
claims.is_empty()
queued_waiters.is_empty()
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
    Released,
}
```

Promotion detaches a node from the FIFO links but does not reclaim its slab
slot. The node remains addressable until the provisional claim is observed or
cancelled.

The completion has independent `Arc` lifetime, so subscribers can observe a
terminal outcome after the manager reclaims the node.

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
ids, slab-slot generations, and local cell generations, must use checked
arithmetic and define an explicit fatal invariant response on exhaustion.

### Per-call acquisition subscription

Each caller waiting on a shared exact-owner request retains its own
subscription:

```rust
struct AcquireSubscription {
    cell: Arc<OwnerResourceCell>,
    waiter_token: WaiterToken,
    completion: Arc<WaitCompletion>,
    cell_generation: u64,
    subscriber_id: SubscriberID,
    active: bool,
}
```

`subscriber_id` is unique among subscriptions in one pending cell generation.
The cell generation makes reuse in a later pending generation safe. Dropping an
active subscription removes only that subscriber; it reaches manager
cancellation only when it removed the last subscriber.

## Core Operations

### 1. Admit an owner resource cell

Acquisition first takes the scope mutex:

```text
scope must be Open
find or insert resource cell
clone cell Arc
release scope mutex
```

Publishing `Closing` prevents new cell insertion. An acquisition that already
obtained a cell serializes with cleanup through the cell mutex.

### 2. Reentrant exact-owner acquisition

When the cell is `Granted(H)`:

1. If `H` covers the requested mode, return the existing claim locally.
2. If the requested mode covers `H`, attempt immediate conversion.
3. If neither covers the other, return `LockConversionNotSupported`.

The common covered path does not enter shared resource state.

### 3. Fresh immediate acquisition

For a `Vacant` cell:

1. Lock the resource state.
2. Check purpose-specific policy such as DDL versus `SessionExplicit`.
3. Validate every different-owner same-family claim and waiter using the family
   mode counts/masks.
4. Check external compatibility using physical-holder counts/masks.
5. Apply FIFO policy.
6. If immediately grantable:
   - insert a held exact claim;
   - create or update the physical family holder;
   - update counts and masks;
   - return a `ClaimToken`;
   - set the cell to `Granted`.
7. Release resource and cell synchronization before returning.

A new different-owner claim that is covered by an existing family holder still
enters shared resource state because its exact lifetime must be registered.
It does not change external holder counts.

### 4. Enqueue a waiter

If a fresh request cannot be granted:

1. Allocate a slab node.
2. Link it at the FIFO tail.
3. Add it to `FamilyResourceState::queued_waiters`.
4. Update family waiter counts/mask.
5. Return `WaiterToken` plus `Arc<WaitCompletion>`.
6. Set the cell to `Pending` with its first subscriber.
7. Release all synchronous locks.
8. Await the completion event.

Enqueue is `O(1)` average.

### 5. Duplicate pending acquisition

For an exact owner already pending mode `P` and a new request `R`:

| Relationship | Result |
|---|---|
| `P` covers `R` | add a subscriber to the existing waiter |
| `R` strictly covers `P` | `LockUpgradeWouldBlock` |
| modes are incomparable | `LockConversionNotSupported` |

The queued target mode never changes. Distinct owners in the same family never
share a queue node.

### 6. Wait for completion

The async loop registers a listener before inspecting the outcome:

```text
listen
    -> Waiting: await event
    -> Promoted: observe promotion through the owner cell
    -> Released: return LockWaiterReleased
```

Registering first prevents a lost wakeup.

### 7. Promote the FIFO prefix

Every transition that may reduce blocking runs one central grant loop while
holding the resource-state lock:

```text
while queue head is promotable:
    detach head links
    remove exact owner from queued_waiters
    decrement waiter aggregates
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
2. coverage by all current same-family provisional/held claims; and
3. no queued waiter behind the head as a granted blocker.

The loop promotes the maximal compatible FIFO prefix.

### 8. Observe a promoted waiter

The first subscriber observing `Promoted`:

```text
lock cell
validate Pending + local generation + waiter token
manager validates resource incarnation, node phase, owner, and provisional claim
provisional claim -> held claim
reclaim waiter slab node
return ClaimToken
cell Pending -> Granted
unlock cell
```

Observation does not change compatibility and does not rerun queue granting.
Later subscribers find the cell already `Granted` and return success without
calling the manager again.

### 9. Cancel a subscription

Dropping one pending subscription:

1. Lock the cell.
2. Validate local generation, waiter token, and subscriber id.
3. Remove that subscriber.
4. If subscribers remain, do nothing to manager state.
5. If it was the last subscriber:
   - cancel the queued or provisional waiter;
   - publish `Released`;
   - reclaim the waiter node;
   - rerun FIFO-prefix granting;
   - set the local cell to a new-generation `Vacant`.

If another subscriber already observed the grant, the cell is `Granted`.
Dropping an older subscription cannot cancel or release that claim.

Scope cleanup uses the same manager transitions but finishes with `Closed`
instead of `Vacant`.

### 10. Release an exact claim

Releasing a claim:

1. Validate resource, exact owner, and global `ClaimID`.
2. Remove the exact claim.
3. Update family claim counts/mask.
4. Recompute the strongest remaining family claim using the fixed mode set.
5. Retain, downgrade, or remove the physical family holder.
6. Update resource holder counts/mask if the physical mode changed.
7. Rerun FIFO-prefix granting even if the physical mode did not change.

The last step is required because removing a weaker same-family claim may remove
a directional constraint that prevented the queue head from being promoted.

An explicit session unlock changes its cells from `Granted` to new-generation
`Vacant`; it does not close the reusable `SessionExplicit` scope.

### 11. Close a scope

Scope close is synchronous and serialized by `close_gate`:

```text
lock close gate
lock scope
    Closed  -> return success
    Closing -> internal invariant unless another closer owns the gate
    Open    -> Closing; take all resource cells
unlock scope

for each cell:
    Vacant  -> Closed
    Pending -> cancel waiter -> Closed
    Granted -> release claim -> Closed

lock scope
Closing -> Closed
unlock scope
unlock close gate
```

No global resource scan is required.

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
session cleanup.

### 12. Immediate exact-owner conversion

Given an exact-owner held mode `H` and requested mode `R`:

1. If `H` covers `R`, return existing.
2. If `R` covers `H`:
   - every other same-family claim and waiter must cover `R`;
   - the resource queue must be empty;
   - `R` must be immediately compatible with external family holders after
     excluding this family's current holder;
   - otherwise return `LockUpgradeWouldBlock`;
   - if valid, update the claim mode and physical holder atomically.
3. If neither covers the other, return `LockConversionNotSupported`.

The claim keeps its global `ClaimID` across a successful in-place conversion.
Release of that claim releases its current mode.

Different exact owners cannot strengthen an earlier owner's claim. They can
only add a claim covered by every existing same-family claim and waiter.

### 13. Queue bypass

A fresh compatible request normally waits behind a non-empty queue.

It may bypass only when:

- an existing provisional or held same-family claim covers it;
- every other same-family claim and queued waiter passes directional admission;
  and
- external family holders remain compatible.

A covering same-family queued waiter does not permit bypass. The new exact
owner receives its own tail position.

### 14. DDL and explicit session locks

The working design preserves the current rule:

> DDL rejects a target resource when its family has a provisional/held or
> queued `SessionExplicit` owner on that table.

This is checked inside the same resource-state critical section that admits the
DDL claim or waiter:

```text
explicit_owner = LockOwner {
    family: ddl_owner.family,
    scope: SessionExplicit,
}

reject when claims contains explicit_owner
reject when queued_waiters contains explicit_owner
```

The check is exact-owner `O(1)` average and does not inspect only the physical
holder mode.

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
- `T_scope` be the number of cells retained by one scope;
- `C_family` and `W_family` be exact claims and waiters retained for diagnostics
  and exact lookup.

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
| Cancel final subscriber | `O(M + K * M)` |
| Promote `K` waiters | `O(K * M)`, effectively `O(K)` |
| Close one scope | `O(T_scope + total promoted work)` |
| Resource debug snapshot | `O(C_family + W_family)` per family |

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
- scope cleanup is proportional to retained cells, including `Vacant` cells in
  the simplest design; and
- notification scheduling can dominate small manager transitions.

Benchmarks must validate representation choices.

## Concurrency and Synchronization Rules

The working lock order is:

```text
scope map mutex
    released before
owner-resource cell mutex
    -> resource shard lock
```

Rules:

1. Never hold a mutex across `.await`.
2. Resource transitions never acquire an owner cell.
3. Queue promotion changes manager state and notifies through
   `WaitCompletion`; the async observer later updates its cell.
4. A caller holding a cell may enter one synchronous manager transition.
5. Notifications occur after releasing resource synchronization.
6. Scope close takes the scope map only to close admission and take cells, then
   drains cells without the scope mutex.
7. Resource state may be removed when empty, so tokens must validate resource
   incarnation.
8. Counter/mask updates and their maps are one atomic resource-state
   transition.

The implementation should add debug assertions for:

- counts matching map contents;
- masks matching nonzero counts;
- exactly one physical holder count per family with claims;
- holder absence when claims are empty;
- physical mode covering every exact claim;
- queue link and `queued_waiters` bijection;
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
serialized by their runtime ownership. Concurrent `SessionExplicit`
acquisitions need a final policy for fresh-claim rollback; this is listed as an
open question below.

## Diagnostics

The manager should retain two inspection views.

### Physical resource view

For each resource:

- resource incarnation;
- physical family holders and modes;
- aggregate holder counts/mask;
- FIFO queue order;
- waiter phase and target mode.

### Exact logical view

For each family/resource:

- exact owner scope and purpose;
- claim mode;
- provisional or held phase;
- claim id;
- queued waiter token and mode.

For each scope:

- lifecycle;
- indexed resources;
- cell state and local generation;
- subscriber count for pending cells.

Debug snapshots should clearly separate physical holders from logical claims.
Counting only the physical family holder is insufficient to prove that a
transaction or statement owns the required claim.

## Limitations and Explicit Non-Goals

### Row ownership remains separate

The manager does not provide row locks, record locks, gap locks, or next-key
locks. Hot rows continue to use undo ownership; cold rows continue to use the
column deletion buffer.

### No persisted lock state

All holders, claims, waiters, tokens, and scope cells are volatile. Recovery
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

`LockFamilyId(SessionID)` assumes one engine process and one logical execution
family per session. Distributed ownership, cross-process lock recovery, and
cooperating parallel-worker families are outside this design.

### No lock escalation or weak-lock fast path

The first redesign uses the shared manager for every new exact claim. It does
not add lock escalation, per-session metadata fast slots, or a PostgreSQL-style
weak-lock migration barrier.

### Deadlock handling is external

No deadlock policy is designed here. See
[backlog 000167](./backlogs/000167-logical-lock-deadlock-handling.md).

## Unresolved Questions

These questions remain in scope for pre-RFC design work.

### 1. Vacant cell retention

The simplest safe model retains every touched resource cell until scope close.
For a long-lived session that repeatedly locks and unlocks many tables, cleanup
and memory become `O(resources ever touched)`, not `O(current claims)`.

Alternatives:

- retain vacant cells and accept the bound;
- compact only session scopes after a threshold;
- add a `Detached` state and remove a cell under the scope mutex while stale
  `Arc` users are forced to retry; or
- store cells in a generational owner-local slab.

The RFC must choose one baseline and state its complexity.

### 2. Concurrent same-scope acquisition rollback

Duplicate pending acquisitions by the same exact owner share one waiter and one
eventual claim. If one multi-resource operation fails while another concurrent
operation depends on that claim, a per-call fresh guard cannot blindly release
it.

The main case is `SessionExplicit`, whose public acquisition API uses `&self`.
Possible policies:

- serialize session explicit lock mutations per table;
- add logical acquisition intents/refcounts above the one exact claim;
- define table validation and paired metadata/data acquisition as one
  shareable operation state; or
- narrow the public concurrency contract.

Transaction, statement, DDL, and maintenance scopes already have stronger
serialization or unique operation identity.

### 3. Scope close and operation lifetime

The outer session pin, transaction checkout, statement borrow, or operation
scope should prevent cleanup from releasing a successfully returned claim while
protected work is still using it.

The RFC must audit every close caller and decide whether outer lifecycle
ownership is sufficient. If it is not, `LockScopeState` needs an in-flight
operation lease or a close-drain counter in addition to its resource cells.

The success linearization point should be the transition to `Granted` while the
scope is open. Pending acquisitions cancelled by close return
`LockWaiterReleased` or a new closing-specific error.

### 4. Token allocation strategy

The working design uses:

- per-resource generational slab slots;
- a resource-incarnation id; and
- global claim ids.

An alternative uses global waiter ids in addition to slab slots. The RFC should
choose the smallest representation that still proves stale-token safety across
resource and family removal/recreation.

### 5. Family aggregate representation

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
alone.

### 6. Resource sharding

The current `FastDashMap` supplies resource-level shard synchronization. The
RFC should decide whether to retain it or introduce explicit partitions with:

- stable partition hashing;
- one mutex per partition;
- per-partition resource maps; and
- clearer aggregate statistics.

Partition count, hash quality, and hot-resource contention need measurement.

### 7. Purpose-specific family policy

DDL versus `SessionExplicit` is one policy exception. Maintenance and future
internal operation scopes may need equally explicit rules.

The RFC should specify:

- whether a session explicit request may begin after DDL has already been
  admitted;
- whether maintenance claims may share an explicit session claim solely by
  directional coverage;
- whether operation scopes are serialized by the session API or enforced by
  manager policy; and
- whether purpose conflicts use the existing `LockOwnerGroupConflict` error or
  a clearer family/purpose error.

### 8. Downgrade API

Internal release can downgrade a family holder when its strongest claim
disappears. There is no explicit public or transaction downgrade operation.

Future need should determine whether to add:

- exact-owner mode downgrade;
- transaction `X -> IX`;
- session `X -> S`; or
- no explicit downgrade at all.

A downgrade must rerun FIFO granting and update the exact claim atomically.

### 9. Observability boundary

Internal debug snapshots are required. It remains open whether the first RFC
also adds:

- per-mode holder/waiter counters;
- queue length and wait-duration metrics;
- cancellation and stale-token counters;
- shard-lock contention measurements;
- structured wait tracing; or
- user-visible lock inspection.

### 10. Migration and compatibility

The implementation cannot switch physical ownership, tokens, and cleanup in one
unreviewable patch. The RFC must specify intermediate states that preserve
behavior and testability.

It must also decide whether the public/internal error name
`LockOwnerGroupConflict` remains during migration after `LockOwnerGroup` itself
is removed.

## Suggested Implementation Stages

These stages are a planning aid, not yet an accepted RFC plan.

### Stage A: identity and scope lifecycle

- Introduce `LockOwner { family, scope }`.
- Add DDL and maintenance operation ids.
- Add `LockScopeState` and reusable cells.
- Route statement, transaction, session, DDL, and maintenance cleanup through
  scope indexes.
- Preserve proof-bound transaction-lock cleanup before session completion.
- Preserve the current vector/deque resource representation temporarily.

### Stage B: tokenized waiter and claim lifecycle

- Add resource-incarnation and global claim ids.
- Add generational waiter nodes.
- Keep nodes alive through provisional state.
- Add independent `WaitCompletion`.
- Implement local generation and stale completion rejection.
- Preserve duplicate exact-owner subscriber behavior.

### Stage C: shared exact-claim family registry

- Store exact claims and queued waiters per family.
- Collapse physical grants to one holder per family/resource.
- Add per-family mode counts and masks.
- Make provisional claims fully granted for arbitration.
- Preserve directional same-family admission and queue bypass.
- Make DDL purpose checks atomic with resource admission.

### Stage D: aggregate compatibility and intrusive FIFO

- Add physical holder counts and masks.
- Replace granted-vector scans.
- Replace queue rebuilding with intrusive token unlink.
- Centralize all blocker-removal transitions through the FIFO-prefix grant loop.
- Add invariant-rich physical and exact debug snapshots.

### Stage E: benchmark-led refinement

- Measure hot shared metadata resources, cancellation, scope cleanup, and queue
  promotion.
- Decide vacant-cell compaction.
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
4. Transaction `IX` rejecting later session `X`.
5. `S` and `IX` rejected in both arrival orders.
6. Exact covered reacquisition returns existing.
7. Immediate conversion succeeds only with empty queue and external
   compatibility.
8. DDL rejects held, provisional, and queued `SessionExplicit` claims.
9. Fresh-versus-existing rollback retains older claims.
10. Statement-to-transaction handoff has no protection gap.

### Queue and cancellation tests

1. Duplicate acquisitions share only one exact-owner waiter.
2. Cancelling one subscriber retains the waiter.
3. Last cancellation removes a queued waiter and returns the cell to `Vacant`.
4. Last cancellation removes a provisional claim and returns the cell to
   `Vacant`.
5. One observer adopts the claim; cancellation of another subscriber does not
   release it.
6. Cancelling the head reconsiders the next waiter.
7. Physical downgrade promotes newly compatible waiters.
8. Removing a same-family claim with unchanged physical mode reconsiders the
   queue.
9. Distinct same-family waiters separated by an external waiter retain FIFO
   order.
10. A covering waiter does not permit bypass; a covering claim does.

### Token and lifecycle race tests

1. Stale waiter slot generation cannot affect a reused slot.
2. Stale waiter token cannot affect a recreated resource incarnation.
3. Stale claim id cannot release a later claim.
4. Scope close before manager begin prevents enqueue.
5. Scope close after manager begin cancels or releases the cell.
6. Repeated scope close is idempotent.
7. Explicit unlock followed by relock uses a new local generation and claim id.
8. Transaction claims are absent before the session becomes idle.
9. Abandoned session explicit cleanup runs after transaction scope cleanup.

### Invariant and model tests

Randomized transition tests should compare the optimized structure with a
simple reference model that stores exact claims and FIFO waiters in vectors.
After every operation, verify:

- physical mode;
- grant and waiter masks/counts;
- exact claims;
- queue order;
- provisional state;
- notifications;
- scope cells; and
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
8. Scope cleanup with many active and vacant cells.

Measure:

- operations per second;
- resource-shard hold and wait time;
- allocations per fresh claim and waiter;
- queue promotion latency;
- cancellation latency;
- scope cleanup latency;
- memory per resource/family/claim/waiter/cell; and
- debug-assertion overhead in test builds.

## Normative Invariants Proposed for the RFC

The eventual RFC should accept, reject, or refine each invariant explicitly:

1. One exact owner identifies each session, transaction, statement, DDL
   operation, or maintenance operation.
2. Every exact owner has one closeable scope-owned reverse index.
3. A resource cell is indexed before manager acquisition begins.
4. No new cell is admitted after scope `Closing`.
5. One physical holder exists per `(resource, family)`.
6. Zero or more exact claims contribute to one physical holder.
7. Zero or more exact queued waiters may exist per family.
8. Duplicate acquisitions share only for the same exact owner.
9. A waiter node survives promotion until observation or cancellation.
10. Provisional claims are fully granted for arbitration.
11. Tokens remain unambiguous across resource and family recreation.
12. Stale tokens and stale local generations have no effect.
13. Different-owner same-family admission is directional and validates every
    claim and waiter.
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
- `doradb-storage/src/lock/state.rs`
- `doradb-storage/src/session.rs`
- `doradb-storage/src/trx/admission.rs`
- `doradb-storage/src/trx/mod.rs`
- `doradb-storage/src/trx/stmt.rs`
- `doradb-storage/src/trx/stream_stmt.rs`
