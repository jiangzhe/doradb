---
id: 0027
title: Session-Family Logical Lock System Redesign
status: proposal
tags: [storage-engine, logical-locking, session, transaction, concurrency, performance]
created: 2026-08-06
github_issue: 947
---

# RFC-0027: Session-Family Logical Lock System Redesign

## Summary

Doradb will replace exact-owner physical lock grants with one physical holder
per `(LockResource, LockFamily)`, where a family is one engine-local session.
The session family will authoritatively own its exact
`SessionExplicit`/`Operation`/`Transaction`/`Statement` claims and per-resource
aggregate state. Shared resource state will contain only physical family
holders, external compatibility aggregates, and pending acquisition state.
Covered exact-owner reacquisition, covered cross-scope claims, and releases
that do not change the physical family mode will therefore remain entirely
family-local. [D7] [C1] [C2] [U1] [U2] [U6] [B1]

All family mutation and cleanup remains linear, including across lock waits.
Each fresh logical claim attempt receives a checked, session-local
`FamilyClaimNo`; stale accepted-claim identity is an asserted invariant
violation rather than a recoverable concurrency case. A blocked attempt keeps
that claim number while a separate `(slot, generation)` pair addresses its
transient waiter node. Waiting uses a resource-local minimal generational
slab, provisional physical grants, call-local pending guards, and the existing
`crate::completion::Completion<()>`. Scope cleanup is proportional to the
scope's accepted claims, and shared-manager work is proportional only to
physical-mode changes and waiters actually promoted. [D9] [D10] [C3] [C4]
[C7] [U3] [U4] [U5] [U6] [U7] [U8]

The redesign is delivered in three compact phases: establish family authority
and owner-side indexes; replace waiter and cancellation identity; then cut over
to physical-only family aggregation and remove legacy scans and purpose
workarounds. Completed scope-identity and cancellation-coordination work are
prerequisites, not a separate Phase 0. [D11] [D12] [B4] [B5] [U3]

## Context

RFC-0016 established logical metadata and table-data locks with exact
session, operation, transaction, and statement owners. The current manager
stores every exact grant in a `Vec` and every waiter in a `VecDeque`. Admission,
conversion, exact release, cancellation, and promotion repeatedly scan or
rebuild those collections. `release_owner()` additionally collects and sorts
every live resource before searching for one owner. [D8] [C1]

`OwnerLockState` already avoids manager entry for covered transaction,
statement, DDL, and maintenance acquisitions and provides targeted cleanup,
but it is a best-effort cache rather than the authoritative family model.
Explicit session locks do not have equivalent scope state, and the manager
still represents multiple exact scopes from one session as multiple physical
holders. Nested DDL transactions require `PreparedCatalogWriteAuthority` to
bypass reacquisition of catalog locks already owned by their operation scope.
[C2] [C3] [C4] [C5] [C6] [C8] [B2]

Backlogs 000169 and 000170 established exact operation-scope identity and
session-coordinated cancellation ownership. RFC-0025 and RFC-0026 now ensure
that public transactions, statements, accepted DDL, maintenance, and cleanup
have explicit lifecycle owners. Those prerequisites make it possible to
serialize every lock mutation for one family without adding owner-side
mutexes, reference-counted close protocols, or duplicate cleanup workers.
[D9] [D10] [D12] [B4] [B5]

The working design in `docs/lock-system.md` proposed both owner-side scope
maps and resource-side exact-claim maps, an engine-global claim id, and a new
wait-completion helper. Discussion refined that direction. Linear family
ownership means exact claims need not be duplicated in shared state, a
session-local sequence is sufficient for accepted-claim assertions, and the
existing completion cell already provides the required independent one-shot
notification. [D7] [C7] [U2] [U4] [U5] [U6]

`Issue Labels:`
`- type:epic`
`- priority:high`
`- codex`

### Goals

- Represent one physical holder per resource/session family while retaining
  exact claims for every lifecycle scope.
- Make the authoritative owner-side state session-family scoped and preserve
  one linear mutation authority across session, operation, transaction, and
  statement execution.
- Keep repeated covered acquisitions, covered nested claims, and non-physical
  releases out of shared resource state.
- Preserve directional same-family coverage, FIFO-prefix granting,
  immediate-only conversion, explicit DDL policy, statement handoff, and
  transaction-before-session cleanup ordering.
- Make cancellation and promotion token-exact, ABA-safe, and free of
  provisional-grant leaks.
- Replace global cleanup scans with work proportional to the exact scope being
  closed.
- Make algorithmic bounds, shared-state transitions, allocation classes, and
  contention benchmarks first-class acceptance criteria.

### Non-goals

- General multi-resource deadlock detection, timeout policy, or victim
  selection; those remain backlog 000167.
- Parallel lock mutation, distributed family ownership, or multiple active
  execution lineages within one session.
- Blocking conversion, `SIX`, lock escalation, weak-lock fast paths, leases,
  or automatic lock-plan reordering.
- Row-lock redesign, MVCC changes, storage-format changes, recovery of logical
  lock state, or changes to transaction isolation.
- A per-family actor, public scheduler, or new background-runtime ownership
  model.
- A new completion primitive unless implementation evidence proves
  `crate::completion::Completion` cannot satisfy a later requirement.

## Design Inputs

### Documents

- [D1] `docs/architecture.md` - engine, session, transaction, catalog, and
  storage subsystem boundaries.
- [D2] `docs/transaction-system.md` - transaction ownership, commit,
  rollback, cleanup, and visibility ordering.
- [D3] `docs/index-design.md` - index DDL, metadata protection, and
  publication boundaries.
- [D4] `docs/checkpoint-and-recovery.md` - maintenance ownership, recovery
  boundaries, and volatile logical-lock state.
- [D5] `docs/table-file.md` - table mutation and maintenance resource
  lifetimes.
- [D6] `docs/engine-component-lifetime.md` - session drain, component access,
  and shutdown ordering.
- [D7] `docs/lock-system.md` - implemented behavior, working exact-family
  design, unresolved identity questions, complexity analysis, and migration
  constraints.
- [D8] `docs/rfcs/0016-logical-lock-manager.md` - existing resource, mode,
  compatibility, fairness, and conversion contract.
- [D9] `docs/rfcs/0025-session-coordinated-cancellation-cleanup-ownership.md`
  - stable session-operation and cancellation ownership prerequisites.
- [D10] `docs/rfcs/0026-engine-owned-mandatory-background-runtime.md` -
  accepted DDL/maintenance execution and runtime ownership.
- [D11] `docs/tasks/000242-enforce-terminal-transaction-lock-release-ordering.md`
  - transaction-lock release proof before session completion.
- [D12] `docs/tasks/000246-session-operation-coordinator-foundation.md` and
  `docs/tasks/000247-statement-public-transaction-cancellation-ownership.md`
  - implemented exact operation identity and cancellation sequencing.
- [D13] `docs/tasks/000249-runtime-owned-table-ddl.md` - prepared DDL lock
  ownership and private catalog transaction nesting.
- [D14] `docs/tasks/000257-doradb-bench-lock-table-workload.md` - current
  lock-table benchmark coverage.
- [D15] `docs/process/unit-test.md` - authoritative nextest workflow and
  deterministic concurrency-test requirements.
- [D16] `docs/process/coding-guidance.md` - ownership, performance,
  invariants, and maintainability guidance.
- [D17] `docs/process/issue-tracking.md` - RFC phase and downstream task
  tracking requirements.

### Code References

- [C1] `doradb-storage/src/lock/mod.rs` - canonical owner identity, vector
  grants, deque waiters, family coverage, cancellation guards, promotion,
  conversion, diagnostics, and global owner cleanup.
- [C2] `doradb-storage/src/lock/state.rs` - current per-owner held-resource
  cache and targeted release.
- [C3] `doradb-storage/src/session.rs` - session explicit locks, one active
  operation slot, public transaction exclusion, operation transfer, and
  teardown.
- [C4] `doradb-storage/src/trx/mod.rs` - transaction lock state, terminal
  cleanup proofs, private transactions, and session completion.
- [C5] `doradb-storage/src/trx/stmt.rs` - statement ownership, cancellation,
  and statement-to-transaction lock handoff.
- [C6] `doradb-storage/src/trx/admission.rs` - admission rollback and
  fresh-versus-existing grant handling.
- [C7] `doradb-storage/src/completion.rs` and
  `doradb-storage/src/runtime/mandatory.rs` - reusable one-shot completion and
  exclusive observer patterns.
- [C8] `doradb-storage/src/catalog/table.rs`,
  `doradb-storage/src/catalog/index.rs`, and maintenance call sites - prepared
  operation scopes and nested catalog transactions.
- [C9] `doradb-bench/src` - storage benchmark framework and lock-table
  workload extension surface.
- [C10] `doradb-storage/src/map.rs` - project fast hash-map types and their
  average-cost assumptions.

### Conversation References

- [U1] The user requires a high-performance session-level lock state without
  sacrificing correctness.
- [U2] The user selected one physical holder per resource/session family with
  multiple exact claims and linear session-to-transaction-to-statement
  ownership.
- [U3] The user requested a compact phased plan with no separate Phase 0.
- [U4] The user selected an asserted stale-claim invariant and a cheap
  session-local sequence named `FamilyClaimNo`, rejecting a global monotonic
  claim id.
- [U5] The user requires reuse of `crate::completion::Completion` unless it
  demonstrably cannot satisfy waiter requirements.
- [U6] The user made lock-operation complexity and further exploitation of
  linear family topology high-priority design criteria.
- [U7] The user confirmed that `FamilyClaimNo` identifies the logical claim
  while slab generation belongs to transient waiter identity, and accepted
  removal of a separate resource incarnation when waiter lifetime pins the
  resource state.
- [U8] The user prefers a minimal wait-queue-specific slab implementation over
  a general-purpose slab dependency.

### Source Backlogs

- [B1] `docs/backlogs/000171-exact-family-lock-system-redesign.md` - source
  backlog and RFC acceptance criteria.
- [B2] `docs/backlogs/000115-explicit-session-lock-cache.md` - session lock
  cache requirement absorbed by the family state.
- [B3] `docs/backlogs/000167-logical-lock-deadlock-handling.md` - related but
  explicitly deferred deadlock policy.
- [B4] `docs/backlogs/closed/000169-separate-session-operation-lock-scopes.md`
  - completed exact operation-scope prerequisite.
- [B5] `docs/backlogs/closed/000170-session-coordinated-cancellation-cleanup.md`
  - completed cancellation ownership prerequisite.

## Decision

### 1. One linear family authority owns every exact scope

`LockFamily(SessionID)` remains the canonical family identity.
`LockOwner { family, scope }` remains the canonical exact identity, with
`SessionExplicit`, `Operation`, `Transaction`, and `Statement` scopes. A
physical holder is unique per `(LockResource, LockFamily)`, not per resource:
different compatible session families may still hold the resource
concurrently. [D7] [D8] [C1] [U2]

The lifetime topology is:

```text
LockFamily(SessionID)
├── SessionExplicit scope, which may outlive individual executions
└── one active execution lineage
    ├── PublicTransaction -> Statement
    └── DDL/Maintenance Operation -> PrivateTransaction -> Statement
```

The claims are exact lifetime peers under one family authority rather than a
single inheritance chain. A destination claim must be installed before a
shorter-lived source claim is released, as in statement-to-transaction
metadata handoff. [D2] [D7] [C4] [C5]

At most one acquire, release, conversion, explicit unlock, or scope close may
be active for a family, including while an acquisition awaits another family.
The authority may move between executor threads and lifecycle carriers, but it
is never copied. Session teardown and operation cancellation must first recover
that authority. Owner-side lock state adds no mutex, atomic lease count,
parallel closer, or repair protocol. [D9] [D10] [C3] [C4] [U2]

### 2. Owner-side family and scope indexes are authoritative

The owner side contains both a family/resource aggregate and a per-exact-scope
cleanup index:

```rust
struct FamilyLockState {
    family: LockFamily,
    next_claim_no: u64,
    resources: FastHashMap<LockResource, LocalFamilyResourceState>,
}

struct LockScopeState {
    owner: LockOwner,
    claims: FastHashMap<LockResource, ScopeClaim>,
}

struct ScopeClaim {
    claim_no: FamilyClaimNo,
    mode: LockMode,
}

struct LocalFamilyResourceState {
    claims: FamilyClaims,
    claim_mask: ModeMask,
    physical_mode: LockMode,
}
```

`LockScopeState` contains only accepted claims and is consumed or selectively
mutated by its lifecycle owner. `FamilyLockState` travels with the unique
family execution authority and aggregates all live scope claims. The
`SessionExplicit` scope remains logically distinct even when stored beside the
family root. Transaction, statement, and operation carriers borrow or carry
the same root authority rather than constructing independent family
coordinators. [D7] [D9] [C2] [C3] [C4] [U1] [U2]

The live scope topology bounds one family/resource to at most one claim of
each of four scope classes. `FamilyClaims` therefore must not allocate a
nested hash map. It stores the common single claim inline and expands at most
once to fixed typed slots for `SessionExplicit`, `Operation`, `Transaction`,
and `Statement`. Once expanded, it retains those slots until the local
family/resource entry disappears so repeated statement handoffs do not churn
allocations. Exact ids in occupied operation, transaction, and statement slots
are still validated. [D7] [C10] [U2] [U6]

The implementation may tune the byte layout after size and benchmark
measurement, but it must preserve bounded lookup, no hashing among a family's
four scope classes, and no heap allocation for the common single-claim case.
[D14] [D16] [U6]

### 3. `FamilyClaimNo` is local, checked, and invariant-enforced

Every fresh logical claim attempt reserves an opaque `FamilyClaimNo`. Its full
logical identity is `(LockFamily, FamilyClaimNo)`; no comparison or uniqueness
is required across families. The sequence is a plain session-local integer
advanced only under exclusive family authority. Zero may be reserved for
niche optimization. Arithmetic is checked, and exhaustion is an internal
fatal invariant. [U2] [U4] [U6] [U7]

The number is reserved after an exact-scope cache miss and before a shared
manager transition or waiter enqueue. Rejected and cancelled acquisitions may
burn numbers. Immediate success transfers the number directly into the
accepted claim. A blocked attempt stores the same number in its waiter node so
another family can provisionally promote it without accessing the waiting
session's family state. Cancellation consumes the attempt and burns the
number. Successful in-place conversion retains the number; unlock followed by
reacquire receives a new one. The sequence may reset only after session
teardown proves that no accepted claim, provisional grant, waiter, or
call-local guard survives. [D6] [D7] [D9] [C3] [U4] [U7]

The call lifecycle uses two move-only logical tokens:

```rust
struct PendingClaimToken {
    resource: LockResource,
    owner: LockOwner,
    claim_no: FamilyClaimNo,
}

struct ClaimToken {
    resource: LockResource,
    owner: LockOwner,
    claim_no: FamilyClaimNo,
}
```

`PendingClaimToken` exists from reservation through immediate or provisional
physical-grant transfer. Acceptance consumes it and produces a `ClaimToken`
and `ScopeClaim` with the same number. A waiter is therefore a transient phase
of creating a claim, not a second logical ownership object. Its
`WaitNodeID { slot, generation }` is only a resource-local storage coordinate
held by the pending guard; it is not part of claim identity. The waiter node
contains the pending token fields so observation and cancellation validate
both the storage coordinate and logical attempt before mutation. [D7] [C1]
[U4] [U7]

Accepted-claim release first locates the exact local record and asserts
equality with the `ClaimToken` before changing either owner-side or shared
state. A mismatch is not a recoverable stale result: correct linear ownership
makes it impossible. For example, claim 41 may be unlocked and the same owner
may reacquire the resource as claim 42; accidentally replaying the old claim
41 token must assert before it can remove claim 42. Tests inject that sequence,
assert the invariant failure, and prove that the newer claim was not mutated.
[D7] [C2] [C6] [U4]

### 4. Shared resource state stores physical families, not exact claims

The shared manager is reduced to external compatibility, physical family
state, and the FIFO queue:

```rust
struct ResourceState {
    granted_counts: [u32; MODE_COUNT],
    grant_mask: ModeMask,
    families: FastHashMap<LockFamily, PhysicalFamilyState>,
    wait_queue: WaitQueue,
}

enum PhysicalFamilyState {
    Held {
        mode: LockMode,
    },
    Queued {
        node_id: WaitNodeID,
    },
    Provisional {
        mode: LockMode,
        node_id: WaitNodeID,
    },
}
```

The exact byte representation may combine the queue node and family entry, but
the semantic states are exclusive. A queued family has no accepted claim or
physical holder on that resource. A provisional state has installed the
physical grant but has not yet transferred the exact claim into owner-side
scope state. A held state represents all accepted exact claims in the family.
[D7] [C1] [U2] [U6] [U7]

`ResourceState::is_empty()` is true only when there are no physical families,
no granted counts, no linked queue nodes, and no occupied slab nodes in any
phase. In particular, detached `Provisional` and `Released` nodes keep the
resource entry alive until the unique pending observer or guard consumes
them. Consequently, every live `WaitNodeID` pins the `ResourceState` and its
slab. The whole slab can be destroyed and a new resource entry created only
after no waiter-node id can survive, so a separate `ResourceIncarnation` is
unnecessary. Slot generation handles reuse inside one live slab; the
resource-removal invariant handles reuse of the entire resource entry. [D7]
[C1] [U7]

Resource state does not duplicate exact-owner claim maps, claim counts, DDL
purpose records, or accepted `FamilyClaimNo`s. The manager needs only the
physical mode to determine compatibility with other families. Resource holder
counts and masks count families, never exact claims. [D8] [C1] [U2] [U6]

This reduction is correct because:

1. a fresh different-owner claim is admissible only if every existing
   same-family claim covers it;
2. the current physical maximum therefore already covers the request;
3. compatibility with other families was established for that maximum;
4. a covered same-family claim may bypass the external FIFO queue;
5. same-owner strengthening is immediate-only and is never queued; and
6. no family claim can change while that family owns a wait.

Therefore a family with accepted claims cannot also have a waiter under this
RFC's conversion and coverage rules. If blocking conversion or parallel
family mutation is introduced later, this proof and representation must be
revisited. [D7] [D8] [B3] [U2]

### 5. Covered claim changes are owner-local

An acquisition borrows the family root and target scope exclusively across the
entire operation. It first checks the exact scope:

- If the existing exact claim covers the request, return locally.
- If the requested mode covers the existing mode, attempt immediate
  conversion.
- If the modes are incomparable, return `LockConversionNotSupported`.

For a new exact claim, the family aggregate validates directional coverage and
derives the physical mode before and after insertion. If the physical mode is
unchanged, the guarded claim is inserted into the family and scope indexes
without touching shared resource state. This is the normal nested
operation/transaction/statement path. [D7] [C2] [C5] [U1] [U6]

Likewise, release first validates `FamilyClaimNo` and computes the remaining
family maximum. If the physical mode is unchanged, it removes the claim
entirely locally. It does not rerun the resource grant loop: other families
observe only the unchanged physical holder, and the same family cannot have a
concurrent waiter. [D7] [U2] [U4] [U6]

The family aggregate uses a compact mode mask and precomputed coverage rules.
Because at most four exact slots exist, insertion validation and recomputation
may scan the fixed slots or use a small lookup table; they must not maintain a
resource-side exact-claim hash or loop over unrelated families. The physical
mode is the strongest actual exact claim under `covers()`, never a synthetic
lattice join. `S` and `IX` remain incomparable, and the manager never
manufactures `SIX` or `X` to combine them. [D7] [D8] [C1] [U6]

### 6. Physical changes use one guarded shared transition

When a new claim, conversion, or release changes the physical family mode, the
manager performs one synchronous resource transition:

1. Validate the expected old family state.
2. Check purpose-independent external compatibility using fixed mode
   counts/masks.
3. Apply FIFO and immediate-conversion policy.
4. Insert, replace, downgrade, or remove the physical family holder.
5. Update resource holder counts/masks.
6. Run the maximal FIFO-prefix grant loop when blocking may have decreased.
7. Release resource synchronization before notification.

An immediate fresh physical grant is owned by a call-local guard until its
exact claim is inserted into both owner-side indexes. This transfer does not
require a second shared transition. If unwinding occurs first, the guard
releases the physical grant synchronously. A conversion or release updates
owner-side and shared representations under an equivalent synchronous rollback
discipline so no half-transition can escape family authority. [D7] [C1] [C2]
[C6] [U6]

Physical strengthening remains immediate-only. A nonempty queue or conflict
with another family returns `LockUpgradeWouldBlock`; it does not enqueue a
conversion. Physical downgrades and removals run the grant loop. Local-only
claim removal does not. [D8] [C1]

### 7. Pending claims use a minimal generational waiter slab

Only a first physical acquisition for a family/resource may wait. The queue
uses a wait-node-specific generational slab and intrusive index links:

```rust
#[derive(Clone, Copy, Eq, PartialEq)]
struct WaitNodeID {
    slot: u32,
    generation: u64,
}

struct WaitQueue {
    head: Option<WaitNodeID>,
    tail: Option<WaitNodeID>,
    nodes: WaitNodeSlab,
}

struct WaitNodeSlab {
    slots: Vec<WaitNodeSlot>,
    free_head: Option<u32>,
    live_count: usize,
}

struct WaitNodeSlot {
    generation: u64,
    entry: WaitNodeSlotEntry,
}

enum WaitNodeSlotEntry {
    Occupied(WaitNode),
    Vacant { next_free: Option<u32> },
}

struct WaitNode {
    family: LockFamily,
    owner: LockOwner,
    target_mode: LockMode,
    claim_no: FamilyClaimNo,
    phase: WaitNodePhase,
    completion: Arc<Completion<()>>,
}

enum WaitNodePhase {
    Queued {
        prev: Option<WaitNodeID>,
        next: Option<WaitNodeID>,
    },
    Provisional,
    Released,
}
```

`WaitNodeSlab` is a small safe internal container, not a general collection and
not a direct dependency on the `slab` crate. Insertion pops the intrusive free
list or appends one vector slot. Lookup validates both slot bounds and
generation. Removal increments the checked generation, pushes the slot onto
the free list, and decrements `live_count`; generations never wrap. The vector
retains capacity while its resource state lives. These operations and
head/middle/tail queue unlink are `O(1)`, with heap growth only when a blocked
acquisition finds no reusable slot. [D16] [C1] [U6] [U8]

`WaitNodeID` is the transient identity of one occupied storage slot, not the
identity of a claim. The call-local pending guard owns the
`PendingClaimToken` and, while blocked, its `WaitNodeID`. The node repeats the
token's family, owner, and `FamilyClaimNo`. Every manager-side observation
first validates slot generation and then asserts those logical fields before
mutation. A missing slot, generation mismatch, or logical mismatch is an
internal invariant violation; a matching `Released` phase is the ordinary
cancelled outcome. [D7] [C1] [U4] [U7]

The waiter node and call-local guard share
`Arc<crate::completion::Completion<()>>`. The completion is only an independent
one-shot notification that authoritative manager state changed. Promotion or
release mutates that state, drops resource synchronization, and calls
`complete(Ok(()))`. The unique acquisition observer uses
`wait_take_result()` and then validates its pending token and node id:

- matching `Provisional` adopts the physical grant and accepts the claim;
- matching `Released` reclaims the node and returns `LockWaiterReleased`; and
- `Queued` after completion, a missing node, or any identity mismatch is an
  invariant violation before mutation.

No new `WaitCompletion`, outcome mutex, event wrapper, or completion error
bridge is introduced. The existing cell already registers its listener before
checking completion and supports notification lifetime independent of the
manager lock and waiter-node borrow. [C7] [U5]

Promotion installs `PhysicalFamilyState::Provisional` and marks the node
`Provisional` before notification. The observer validates the node, changes the
pending guard to provisional physical-grant ownership, installs the exact
local `ScopeClaim` with the same `FamilyClaimNo`, and then commits the transfer
by changing the physical family state to `Held`, removing the waiter node, and
converting the pending token into its accepted `ClaimToken`. If local transfer
unwinds before that commit, the armed guard removes any inserted local record
and uses the still-addressable provisional node to release the physical grant.
No new claim identity is allocated during this transfer. [D7] [C1] [U7]

Cancellation by a separate lifecycle control path unlinks a queued node or
removes its promoted physical grant, marks the node `Released`, and notifies
after dropping resource synchronization. The occupied released node pins the
resource until the unique observer consumes it. If the caller future itself is
dropped, its pending guard is the final observer and may cancel and reclaim the
node in the same resource transition. Dropping the guard also synchronously:

- releases an immediate fresh physical grant not yet transferred; or
- releases a fresh accepted claim during the narrow guarded local-transfer
  interval.

Every removal reruns the FIFO grant loop only when queue or physical state
changed. Family authority cannot move to cleanup or another operation until
guard Drop finishes. Resource removal is forbidden until `live_count == 0`,
which proves that no `WaitNodeID` can cross whole-slab destruction and makes a
resource incarnation counter unnecessary. [D9] [C1] [C3] [C4] [U7]

### 8. FIFO, policy, handoff, and exact lifetime behavior remain explicit

The resource grant loop promotes the maximal compatible FIFO prefix. Each
candidate is checked against physical-holder masks, not exact claims. A fresh
family does not bypass an older incompatible waiter. A covered claim inside an
already-held family is local and retains the existing RFC-0016 same-family
bypass behavior because it does not strengthen the external holder. [D7] [D8]
[C1]

DDL checks the family-local `SessionExplicit` slot for both metadata and data
resources before creating the DDL claim. A matching explicit claim rejects the
DDL atomically under family authority. Maintenance always records its own
exact operation claim even when a stronger explicit claim covers it. Releasing
maintenance therefore cannot consume an explicit claim. [D7] [C3] [C8]

Nested catalog transactions record their own exact claims locally under the
covering DDL operation claims. After physical-family aggregation proves this
path, `PreparedCatalogWriteAuthority` and lock-bypass branches are removed;
the ordinary acquisition path becomes both correct and local. Failure rollback
releases only exact claims whose acquisition reported `Fresh`. [D13] [C6]
[C8] [B1]

Statement-to-transaction metadata handoff inserts the destination transaction
claim before releasing the statement claim. Both updates may be local under a
covering physical mode, but their order remains a correctness contract.
Statement cancellation destroys its pending acquisition before making
transaction cleanup claimable. [D2] [D11] [D12] [C4] [C5]

### 9. Cleanup follows exact scope ownership

Scope close takes or consumes one uniquely owned `LockScopeState`, drains its
accepted claims, and applies each removal to `FamilyLockState`. Only removals
that change the physical family mode enter the manager. There is no scan over
unrelated manager resources and no independently idempotent or concurrent
scope close. Lifecycle owners that need sequential idempotence store the scope
in an `Option` and take it once. [D7] [C2] [U6] [B1]

The required close order remains:

```text
statement pending/claims
    -> transaction claims
    -> operation claims
    -> session completion or next operation
    -> SessionExplicit claims at explicit unlock or final teardown
```

Transaction completion must consume a proof that its scope is closed before
the session becomes idle or closed. Accepted DDL and maintenance retain their
operation scope through mandatory execution and nested transaction cleanup.
Engine shutdown first drains session and operation owners; the lock manager
remains a passive component with no independent cleanup worker. [D6] [D9]
[D10] [D11] [C3] [C4]

`release_owner()` and duplicate-waiter/concurrent-release repair remain during
migration. They are removed from production paths only after every scope uses
authoritative family state and deterministic tests prove one-family authority.
An optional diagnostic full scan may remain test-only; it is not a lifecycle
operation. [D7] [C1] [B1]

### 10. Complexity and constant factors are acceptance criteria

Let:

- `M` be the fixed mode count, currently four;
- `K` be waiters actually promoted by one transition;
- `H_scope` be accepted claims in one closing scope;
- `B` be those releases that change physical mode, where
  `B <= H_scope`; and
- `P` be total waiters promoted while closing the scope.

Hash-map costs are average costs. Fixed-scope operations inspect at most four
local slots.

| Operation | Target cost | Shared-manager work |
|---|---:|---:|
| Repeated covered exact-owner acquisition | `O(1)` local | none |
| Covered new exact claim in another scope | `O(1)` bounded local | none |
| Release with unchanged physical mode | `O(1)` bounded local | none |
| DDL versus explicit-session policy | `O(1)` local | none |
| First immediate physical acquisition | `O(M)` average | one resource transition |
| Physical-mode-changing conversion | `O(M)` average | one immediate transition |
| Enqueue first family acquisition | `O(M)` average | one transition plus blocked-only allocation |
| Observe provisional grant | `O(1)` average | one token-validation transition |
| Unlink queued waiter | `O(1)` plus promotion | one transition |
| Release/downgrade physical holder | `O(M + K * M)` | one transition |
| Promote `K` waiters | `O((K + 1) * M)` | one resource critical section |
| Close one scope | `O(H_scope + B * M + P * M)` | only `B` physical changes |

Actual promotion and scope drainage are real work and cannot be sublinear in
`K` or `H_scope`. Normal transitions must not scan physical families, exact
claims from unrelated families, the waiter queue, or all manager resources.
[D7] [C1] [U1] [U6]

Structural performance gates are:

- a repeated covered request performs no shared lookup, allocation, atomic
  operation, event access, or completion access;
- a covered cross-scope claim performs no shared-manager transition or global
  atomic operation;
- physical compatibility visits only the fixed mode representation;
- cancellation unlinks one generational node without rebuilding the queue;
- scope close visits exactly its indexed claims and physical changes;
- notifications occur after resource synchronization is released;
- completion allocation occurs only for a blocked acquisition;
- waiter slots are reused from the resource-local free list before vector
  growth; and
- no engine-global claim counter exists.

The lock-table benchmark added by task 000257 is extended with repeated
session-explicit hits, nested covered claims, first-family shared/exclusive
acquisition, immediate conversion, conflict/enqueue, head/middle/tail
cancellation, release with zero and many promotions, statement handoff, and
scope close at varying cardinalities. Measurements record throughput, latency
distribution, allocations, resource transitions, examined mode slots, queue
links, and promoted waiters. The RFC requires those measurements and their
before/after evidence but imposes no hard numeric regression budget; phase
tasks record and explain the observed tradeoffs. [D14] [D15] [C9] [U1] [U6]

### 11. Diagnostics expose physical and exact views separately

Manager diagnostics report physical family mode, queue order, waiter-node
slot/generation, phase, and slab live/free counts. Family diagnostics report
exact owner, scope, `FamilyClaimNo`, mode, and accepted resource set. Combined
debug tests may join both snapshots by family/resource, but production
resource state does not retain exact claims solely for diagnostics. [D7] [C1]
[C3] [U7]

Debug assertions and reference-model tests cover:

- scope and family-index agreement;
- at most one occupied claim per scope class;
- local physical mode covering every exact claim;
- manager holder mode agreeing with the family aggregate at API boundaries;
- physical holder counts and masks;
- queued/provisional/held physical-family-state exclusivity;
- queued/provisional/released node-phase transitions;
- queue-link, free-list, `live_count`, and slot-generation consistency;
- pending-token fields matching their occupied waiter nodes;
- resource removal forbidden while any slab node remains occupied;
- `FamilyClaimNo` equality before accepted-claim mutation; and
- empty local and shared family state before removal.

### 12. Validation preserves current test and backend workflows

Concurrency tests use deterministic events, barriers, and explicit state hooks
rather than timing sleeps. Required races include release versus promotion,
queued cancellation, provisional cancellation, attempted resource removal
while a queued/provisional/released node remains, removal and recreation after
node consumption, waiter-slot reuse, caller-future Drop, nested DDL
cancellation, transaction completion, session teardown, and shutdown drain.
A simple scan-based reference model validates randomized sequential
acquisition/release traces against the optimized family model. [D15] [C1]
[C7] [U7]

Each implementation phase runs:

```text
cargo build --workspace
cargo nextest run --workspace
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

Focused lock and lifecycle tests run before the workspace passes. The existing
`.config/nextest.toml` remains authoritative for timeout and hang behavior;
this RFC does not introduce a new runner or timeout mechanism. [D15]

## Alternatives Considered

### Alternative A: Mirror every exact claim in shared resource state

- Summary: Keep authoritative scope maps while also storing
  `FastHashMap<LockOwner, ClaimRecord>` and per-mode exact-claim counts inside
  every resource/family entry.
- Analysis: This makes the manager independently self-describing and permits
  token validation without owner-side state. It was the working direction in
  `docs/lock-system.md`.
- Why Not Chosen: Linear family authority makes the second authoritative copy
  unnecessary. It forces every covered nested claim and non-maximal release
  through shared synchronization, adds a nested map/allocation, and requires
  cross-index repair logic. Other families depend only on the physical mode.
- References: [D7] [C1] [U2] [U6]

### Alternative B: Per-family actor or command coordinator

- Summary: Route every family acquisition, release, conversion, and close
  through a long-lived actor or command queue.
- Analysis: This could later support parallel callers, batching, explicit
  cancellation messages, or distributed ownership.
- Why Not Chosen: Current session execution is deliberately linear. An actor
  adds allocation, scheduling, completion, shutdown, and failure states to
  covered local operations without a current concurrency requirement.
- References: [D9] [D10] [C3] [U1] [U2]

### Alternative C: Add only an explicit-session lock cache

- Summary: Give `SessionExplicit` an `OwnerLockState` equivalent and otherwise
  retain exact physical grants, vector scans, deque cancellation, and global
  owner cleanup.
- Analysis: This directly resolves backlog 000115 and accelerates repeated
  explicit lock calls with limited implementation risk.
- Why Not Chosen: It does not aggregate physical session ownership, remove
  exact-grant scans, solve token ABA, simplify nested DDL, or provide
  scope-proportional cleanup. It is a partial migration step, not the required
  lock-system redesign.
- References: [C1] [C2] [C3] [B1] [B2] [U1]

### Alternative D: Engine-global claim ids and a dedicated wait completion

- Summary: Allocate every claim from one engine-global monotonic source and
  introduce a lock-specific mutex/event completion carrying waiter outcome.
- Analysis: Global identity is simple to log, and a dedicated completion can
  encode exactly the desired waiter states.
- Why Not Chosen: Claim uniqueness is required only within a session family,
  and correct linear code treats stale accepted identity as an invariant.
  A global counter adds hot cache-line synchronization. Existing
  `Completion<()>` already provides one-shot, lost-wakeup-safe independent
  notification; authoritative waiter state determines the outcome.
- References: [D7] [C7] [U4] [U5] [U6]

### Alternative E: Retain exact-owner physical grants

- Summary: Optimize the existing grant and waiter containers while retaining
  one physical manager entry per exact owner.
- Analysis: Indexed maps and slab queues can improve lookup and cancellation
  without changing physical semantics.
- Why Not Chosen: Other sessions would continue to inspect multiple physical
  entries for one session family, covered nested claims would still enter
  shared state, and physical release would remain coupled to exact lifetime
  representation.
- References: [D8] [C1] [B1] [U2]

### Alternative F: General-purpose slab dependency and resource incarnations

- Summary: Add the `slab` crate as a direct dependency, wrap its reusable
  integer keys with slot generations, and add a manager- or shard-generated
  resource incarnation to every pending waiter token.
- Analysis: A general slab avoids implementing its allocator, and a resource
  incarnation can distinguish identical slot/generation pairs across
  destruction and recreation of a whole resource entry.
- Why Not Chosen: The required allocator is only a vector, intrusive free
  list, generation check, and live count; the general slab key would still
  need a generation layer. More importantly, retaining every occupied
  queued/provisional/released node until its unique observer consumes it
  proves that no node id survives destruction of the resource slab. A second
  incarnation source would protect a lifecycle that the design already
  forbids while adding state and validation to every pending operation.
- References: [D16] [C1] [U7] [U8]

## Unsafe Considerations

No new unsafe code is required. The minimal waiter slab, free list, and
intrusive queue use safe vector indices and checked generations, not raw
pointers. Owner-side linearity is expressed through owned values and exclusive
borrows rather than lifetime extension. If a phase proposes unsafe storage
solely for layout or speed, it requires separate explicit review and evidence
that the safe bounded-slot representation is insufficient; this RFC does not
authorize it. [D16] [U8]

## Implementation Phases

- **Phase 1: Linear Family Authority And Owner-Side Indexes**
  - Scope: Generalize `OwnerLockState` into authoritative
    `FamilyLockState`/`LockScopeState`, add the `SessionExplicit` scope,
    family/resource aggregation, bounded exact-scope slots,
    `FamilyClaimNo`, and scope-targeted close across public transactions,
    statements, DDL, maintenance, and teardown. Continue mirroring each exact
    grant into the current manager during this phase.
  - Goals: Prove one mutation/cleanup authority across await and lifecycle
    transfer; make every scope authoritative; assert claim identity; remove
    normal global owner cleanup; establish local covered-path and close-cost
    instrumentation.
  - Non-goals: Physical family aggregation, waiter-container replacement,
    removal of duplicate-waiter defenses, or catalog authority removal.
  - Prerequisites: Backlogs 000169 and 000170 and their implementation tasks
    are complete.
  - Phase-local Choices: Select the compact one-inline/fixed-slot byte layout
    using type-size and workload evidence without introducing per-family claim
    hashing.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`
  - Related Backlogs:
    - `docs/backlogs/000115-explicit-session-lock-cache.md`
    - `docs/backlogs/000171-exact-family-lock-system-redesign.md`

- **Phase 2: Tokenized Waiter And Provisional-Grant Lifecycle**
  - Scope: Replace `Arc<Waiter>`/`VecDeque` cancellation identity with
    resource-local `WaitNodeID`s, the minimal generational `WaitNodeSlab`,
    `PendingClaimToken` to `ClaimToken` transfer, `Completion<()>`,
    provisional/released node phases, and one call-local pending guard covering
    queued, promoted, immediate-fresh, and transfer states. Pin resource state
    until every occupied waiter node is consumed.
  - Goals: Provide `O(1)` waiter unlink, no lost wakeup, exact
    promotion/cancellation ownership, no provisional leak, no waiter id across
    whole-slab destruction, and deterministic ABA/race tests while retaining
    current exact physical grants.
  - Non-goals: Physical-only family entries, blocking conversion, deadlock
    policy, or removal of migration defenses.
  - Prerequisites: Phase 1 authority and scope-close tests pass for every
    lifecycle carrier.
  - Phase-local Choices: Tune retained slot capacity and compact slot layout
    using measurements; retain the safe vector/free-list design, checked
    generation, and resource-pinning invariant without an external slab or
    resource-incarnation source.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

- **Phase 3: Physical Family Aggregation And Performance Cutover**
  - Scope: Replace exact physical grants with the physical-only family state,
    fixed holder counts/masks, and family-local covered claim changes; make
    DDL policy local; remove resource-side exact claim mirrors,
    `PreparedCatalogWriteAuthority`, duplicate-waiter repair, production
    `release_owner()`, and obsolete global scans after proof gates pass.
    Complete diagnostics, reference-model validation, and expanded
    lock-table benchmarks.
  - Goals: Deliver the stated complexity bounds, preserve RFC-0016 behavior,
    prove nested DDL/maintenance and shutdown cleanup, and document allocation,
    contention, throughput, and latency changes for every operation class.
  - Non-goals: Deadlock handling, blocking conversion, escalation, weak locks,
    family actors, or parallel same-session mutation.
  - Prerequisites: Phase 2 pending-token, cancellation, provisional/released
    node, resource-retention, and post-consumption recreation tests pass under
    both storage I/O feature sets.
  - Phase-local Choices: Tune masks, slot packing, notification batches, and
    retained capacities without weakening structural no-scan/no-global-atomic
    gates.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`
  - Related Backlogs:
    - `docs/backlogs/000171-exact-family-lock-system-redesign.md`

## Consequences

### Positive

- Covered exact and nested claims avoid shared resource synchronization.
- One physical entry per session family reduces conflict-state size and
  compatibility work on hot resources.
- Scope cleanup touches owned claims rather than every manager resource.
- `FamilyClaimNo` provides cheap, deterministic invariant checking without a
  global cache-line bottleneck.
- Generational cancellation removes queue rebuilding and makes ABA behavior
  explicit.
- Resource-state pinning removes a separate incarnation generator and its
  token field.
- The minimal waiter slab reuses blocked-operation storage without a new
  dependency or unsafe pointer structure.
- Existing completion, lifecycle authority, and mandatory-runtime ownership
  are reused instead of adding coordinators or synchronization domains.
- Exact claims retain independent lifetime and purpose even though their
  physical holder is aggregated.

### Negative

- `FamilyLockState` duplicates exact records between its family aggregate and
  per-scope cleanup indexes, so every accepted claim must update two local
  representations consistently.
- The family root must travel through transaction, statement, DDL,
  maintenance, cancellation, and cleanup carriers, increasing lifecycle API
  discipline.
- A second exact scope may allocate the fixed multi-claim representation, and
  owner-side resource maps still allocate as they grow.
- The project owns a small custom waiter-slot allocator and must test its free
  list, generation, live-count, and resource-retention invariants.
- Hot resources remain serialized by one resource-state critical section for
  physical changes and queue transitions.
- FIFO can intentionally delay compatible new families behind an older
  incompatible waiter.
- Diagnostics must join owner-side exact and manager-side physical views
  rather than reading one self-contained resource record.
- The three-phase migration temporarily maintains both new authoritative
  owner state and legacy manager defenses.

## Open Questions

No design-blocking questions remain in this draft. The RFC intentionally
imposes no hard numeric benchmark budget; each phase must still record and
explain before/after evidence for its affected operation classes.

## Future Work

- `docs/backlogs/000167-logical-lock-deadlock-handling.md` - multi-resource
  deadlock policy and diagnostics.
- Blocking conversion or `SIX`, if justified by future SQL semantics.
- Parallel mutation within one session family, which would invalidate the
  local-only claim proof and require a new ownership design.
- Lock escalation, weak-lock elision, distributed ownership, or family actors.
- Additional checkpoint and mixed-runtime workloads unrelated to the lock
  operation classes introduced here.

## References

- `docs/lock-system.md`
- `docs/rfcs/0016-logical-lock-manager.md`
- `docs/rfcs/0025-session-coordinated-cancellation-cleanup-ownership.md`
- `docs/rfcs/0026-engine-owned-mandatory-background-runtime.md`
- `docs/tasks/000242-enforce-terminal-transaction-lock-release-ordering.md`
- `docs/tasks/000246-session-operation-coordinator-foundation.md`
- `docs/tasks/000247-statement-public-transaction-cancellation-ownership.md`
- `docs/tasks/000249-runtime-owned-table-ddl.md`
- `docs/tasks/000257-doradb-bench-lock-table-workload.md`
- `docs/backlogs/000115-explicit-session-lock-cache.md`
- `docs/backlogs/000167-logical-lock-deadlock-handling.md`
- `docs/backlogs/000171-exact-family-lock-system-redesign.md`
- `docs/backlogs/closed/000169-separate-session-operation-lock-scopes.md`
- `docs/backlogs/closed/000170-session-coordinated-cancellation-cleanup.md`
- `docs/architecture.md`
- `docs/transaction-system.md`
- `docs/index-design.md`
- `docs/checkpoint-and-recovery.md`
- `docs/table-file.md`
- `docs/engine-component-lifetime.md`
- `docs/process/coding-guidance.md`
- `docs/process/issue-tracking.md`
- `docs/process/unit-test.md`
