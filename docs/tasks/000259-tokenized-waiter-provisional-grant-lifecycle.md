---
id: 000259
title: Tokenized Waiter and Provisional-Grant Lifecycle
status: proposal  # proposal | implemented | superseded
created: 2026-08-06
github_issue: 950
---

# Task: Tokenized Waiter and Provisional-Grant Lifecycle

## Summary

Implement RFC-0027 Phase 2 by replacing `Arc<Waiter>` pointer identity and the
`VecDeque` wait queue with a resource-local generational waiter slab, intrusive
FIFO links, move-only pending claim identity, and one cancellation-safe
`PendingClaimGuard`.

Each fresh exact claim retains the `ClaimNo` reserved by the Phase 1 family
authority. A blocked acquisition stores that identity in a resource-local
`WaitNode`; promotion installs an exact provisional manager grant before
notifying the unique observer. Observation validates the token and node in one
manager transition, reclaims the node, and transfers the exact fresh grant to
the call-local guard. The guard then publishes both owner-side records without
re-entering the manager. Dropping the guard at any earlier point removes only
its queued node, provisional grant, adopted fresh grant, or partially published
local records.

Phase 2 deliberately retains exact physical `GrantedLock` entries, current
compatibility scans, and migration-only manager cleanup defenses. Normal
statement, transaction, operation, and session cleanup continues to iterate
the authoritative `LockScopeState` resource index; it never scans the lock
manager. Physical family aggregation and removal of exact mirrors and global
fallback scans remain RFC-0027 Phase 3.

## Context

Issue Labels:

- type:task
- priority:high
- codex

Parent RFC:

- `docs/rfcs/0027-session-family-logical-lock-system-redesign.md`

RFC Relationship:

- Phase 2: Tokenized Waiter And Provisional-Grant Lifecycle.

Phase 1 is complete in
`docs/tasks/000258-linear-lock-family-authority-owner-side-indexes.md`.
It established one move-only `FamilyLockAuthority` per session, authoritative
family/resource and exact-scope/resource indexes, checked session-local
`ClaimNo`, and targeted close for session-explicit, operation, transaction,
and statement scopes. A focused planning-time validation ran:

```text
rtk cargo nextest run -p doradb-storage 'lock::'
```

All 66 selected tests passed.

The remaining manager path in `doradb-storage/src/lock/mod.rs` stores exact
grants in `Vec<GrantedLock>` and waiters in `VecDeque<Arc<Waiter>>`.
`Waiter` combines owner and mode with a mutex-protected outcome, an `Event`,
active-guard atomics, and a grant-observed atomic. Cancellation identifies a
waiter by `Arc` pointer and rebuilds the queue. Duplicate acquisitions may
share one waiter, while promotion installs an exact grant before notification.

`FamilyLockState::acquire()` in `doradb-storage/src/lock/state.rs` already
reserves a fresh `ClaimNo` before policy failure, waiting, or cancellation, but
the manager does not receive that identity. After an async manager acquisition
returns, Phase 1 synchronously inserts the claim into both authoritative local
indexes. Accepted release first validates the local token, then removes the
manager mirror by owner alone.

The existing `crate::completion::Completion<()>` supplies the required
one-shot, listener-before-check notification and exclusive
`wait_take_result()` observer. This task reuses it without adding a
lock-specific completion cell or error bridge.

The following RFC phase constrains this task:

- Phase 3 requires Phase 2 tests for pending tokens, queued/provisional/released
  cancellation, resource retention, node consumption, and resource recreation
  to pass under both storage I/O feature sets.
- Phase 2 must leave exact manager grants, compatibility and family-coverage
  scans, duplicate-state migration checks, and manager-level fallback cleanup
  available for the Phase 3 cutover.
- Phase 3 remains responsible for physical-only family entries, fixed holder
  counts and masks, family-local covered claims, final diagnostic and benchmark
  expansion, and removal of production `release_owner()`.

Related work includes open backlog
`docs/backlogs/000171-exact-family-lock-system-redesign.md`, completed
operation/cancellation prerequisite tasks 000242, 000243, 000246, 000247, and
000249, and the lock-table benchmark introduced by task 000257. This task is
sourced from RFC-0027 Phase 2 rather than directly from a backlog.

## Goals

1. Add `doradb-storage/src/lock/wait.rs` as the private home for waiter slab,
   intrusive queue, node phases, notification collection, and
   `PendingClaimGuard`.
2. Replace pointer-based waiter identity with
   `WaitNodeID { slot: usize, generation: u64 }`.
3. Make normal queued cancellation unlink a validated head, middle, or tail
   node in `O(1)` without rebuilding or scanning the queue.
4. Preserve one reserved `ClaimNo` from fresh-attempt creation through
   immediate grant, waiting, provisional promotion, observation, local
   publication, and accepted `ClaimToken`.
5. Reuse `Completion<()>` only for blocked acquisitions and preserve
   listener-before-check lost-wakeup safety.
6. Make queued, provisional, released, adopted-fresh, and partial-transfer
   cleanup exact, synchronous, and leak-free under one call-local guard.
7. Pin a `ResourceState` while any occupied waiter node or exact grant remains,
   without adding a resource-incarnation source.
8. Preserve FIFO-prefix granting, directional family coverage, same-family
   queue bypass, compatibility, immediate-only conversion, exact release, and
   `LockWaiterReleased` behavior.
9. Keep authoritative scope cleanup proportional to the exact scope's indexed
   accepted resources and keep global manager scans out of every production
   lifecycle path.
10. Add deterministic structure, ABA, cancellation, promotion, transfer,
    lifecycle, and resource-recreation tests suitable for both storage
    backends.
11. Record final waiter layout and allocation behavior so Phase 3 starts from
    measured sizes and capacities rather than assumptions.

## Non-Goals

1. Do not aggregate exact manager grants into one physical family holder.
2. Do not add physical holder counts/masks, family-local manager entries, or
   the final Phase 3 compatibility representation.
3. Do not remove exact manager mirrors, `PreparedCatalogWriteAuthority`,
   duplicate-state migration checks, raw manager test helpers, or the
   migration/diagnostic `release_owner()` fallback.
4. Do not route normal lifecycle cleanup through `release_owner()` or any
   manager-wide scan.
5. Do not add blocking conversion, `SIX`, downgrade APIs, lock escalation,
   timeouts, deadlock detection, or victim policy.
6. Do not introduce parallel mutation within one session family, a family
   mutex, actor, lease, or cleanup coordinator.
7. Do not add a global waiter or claim counter, resource incarnation, external
   slab dependency, or unsafe waiter storage.
8. Do not add a new completion primitive, outcome mutex, event wrapper, or
   completion error transport.
9. Do not change public storage APIs, transaction isolation, MVCC, recovery,
   persisted formats, schemas, or storage I/O behavior.
10. Do not expand `doradb-bench` with the final conflict, cancellation, and
    promotion workloads; RFC-0027 Phase 3 owns that benchmark cutover.

## Plan

### 1. Establish the wait module and token boundary

Add `mod wait;` to `doradb-storage/src/lock/mod.rs` and create
`doradb-storage/src/lock/wait.rs`. Keep all new production visibility private
or `pub(super)`; no public storage surface changes.

Keep logical claim identity in `lock/claim.rs`:

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

Remove `Copy` and `Clone` from `ClaimToken`, and do not implement them for
`PendingClaimToken`. Add one consuming conversion from a successfully adopted
pending token to its accepted token. `ScopeClaim` continues to store only
`ClaimNo` and mode because the exact owner and resource already come from its
scope and map key.

A fresh scope miss reserves `ClaimNo` before family policy validation or
manager entry. Rejection or cancellation burns that number. Successful
conversion of an existing claim retains its current number, while unlock
followed by reacquire allocates a new one.

### 2. Implement the minimal generational slab

Define the waiter-specific storage in `lock/wait.rs`:

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct WaitNodeID {
    slot: usize,
    generation: u64,
}

struct WaitNodeSlab {
    slots: Vec<WaitNodeSlot>,
    free_head: usize,
    live_count: usize,
}

struct WaitNodeSlot {
    generation: u64,
    entry: WaitNodeSlotEntry,
}

enum WaitNodeSlotEntry {
    Occupied(WaitNode),
    Vacant { next_free: usize },
}
```

Use `slots.len()` as the direct free-list end sentinel:

- `free_head < slots.len()` identifies the next vacant slot;
- `free_head == slots.len()` means the free list is empty;
- every vacant `next_free` is at most `slots.len()`; and
- `next_free == slots.len()` terminates the list.

No `Option`, `NonZeroUsize`, offset encoding, or integer-width conversion is
used for the free list. The sentinel stays valid because insertion appends only
when the free list is empty. Reusing the final vacant slot replaces that entry
with `Occupied` before a later append changes `slots.len()`, so no surviving
vacant entry retains the old end sentinel.

New vector slots start at generation zero. Reclamation validates the complete
`WaitNodeID`, increments generation with checked arithmetic before mutation,
stores `Vacant { next_free: old_free_head }`, points `free_head` at the
reclaimed slot, and decrements `live_count`. Generation exhaustion is an
internal invariant failure and must leave the slot occupied rather than permit
wraparound.

Start with `Vec::new()`. Do not eagerly reserve or shrink waiter capacity.
Reuse vacant slots before growth, and retain actual capacity for the life of
the containing resource state. Expose narrow test-only snapshots for slot
length, capacity, live count, free-list order, and generations.

### 3. Replace `VecDeque` with an intrusive FIFO

Define:

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
    Released,
}
```

The resource is the parent map key and the family is derived from `owner`, so
the node does not duplicate those derivable fields. Every manager-side node
operation validates resource context, slot bounds, generation, owner,
`ClaimNo`, mode, and expected phase before mutation.

Provide narrow queue operations for:

- append at the tail;
- inspect the linked head;
- detach a queued node by exact ID;
- transition a detached node to `Provisional` or `Released`;
- consume a provisional or released node back into the slab;
- iterate only the linked FIFO for compatibility, migration checks, and test
  diagnostics; and
- validate head/tail, forward/back links, free-list reachability, generations,
  and `live_count` in tests.

Normal cancellation obtains `prev` and `next` from the validated node and
touches only those neighbors plus head/tail, making head, middle, and tail
unlink `O(1)`. FIFO promotion may still visit each actually promoted node, and
Phase 2 compatibility/family checks may still scan exact grants or the linked
queue until Phase 3.

### 4. Make exact manager grants token-aware during migration

Retain the exact grant vector but extend its temporary representation:

```rust
struct GrantedLock {
    owner: LockOwner,
    claim_no: ClaimNo,
    mode: LockMode,
    provisional_node: Option<WaitNodeID>,
}
```

An immediate fresh grant has no waiter node and is owned by the armed pending
guard until local acceptance. A promoted waiter installs an exact grant with
`provisional_node = Some(node_id)` and marks the node `Provisional` before
notification. Provisional grants participate fully in compatibility and family
coverage, but diagnostics distinguish them from accepted held claims.

Observation validates the node and matching provisional exact grant under one
manager resource guard, clears `provisional_node`, reclaims the waiter node,
and transfers the exact fresh grant to `PendingClaimGuard`. The exact grant
then pins the resource while owner-side publication completes. This
claim-number field and provisional marker are migration representation and are
removed with exact physical grants in Phase 3.

Split manager operations by semantic identity:

- an existing exact conversion validates a reconstructed accepted
  `ClaimToken` and remains immediate-only;
- a fresh acquisition accepts `PendingClaimToken`;
- accepted release consumes or borrows the exact `ClaimToken` and validates
  `ClaimNo` before manager mutation; and
- pending cancellation validates `PendingClaimToken` and any `WaitNodeID`.

Organize immediate conversion as one synchronous manager transition rather
than routing it through the async fresh/wait path. Preserve the current
operation error and attachment behavior.

### 5. Implement the call-local pending guard

Place `PendingClaimGuard` in `lock/wait.rs`. It exclusively borrows the manager,
family state, and target scope for the complete fresh acquisition:

```rust
enum PendingGuardState {
    NotStarted,
    Waiting {
        node_id: WaitNodeID,
        completion: Arc<Completion<()>>,
    },
    FreshGranted,
    Disarmed,
}
```

The guard also owns `Option<PendingClaimToken>`, requested mode, and a
`transfer_started` flag.

The fresh lifecycle is:

1. Construct the guard after claim-number reservation and family policy
   validation.
2. Enter one synchronous manager transition.
3. On immediate compatibility, install the exact grant without allocating a
   waiter node or completion and enter `FreshGranted`.
4. Otherwise allocate one `Completion<()>`, append one queued node, and enter
   `Waiting`.
5. Await the completion with `wait_take_result()`. The completion only reports
   that authoritative manager state changed; it never carries waiter outcome.
6. Re-enter the manager once to observe the notified node:
   - a matching `Provisional` node and grant are adopted atomically by clearing
     the grant marker, reclaiming the node, and entering `FreshGranted`;
   - a matching `Released` node is reclaimed and returns
     `LockWaiterReleased`; and
   - `Queued` after completion, a missing or stale slot, or identity mismatch
     is an invariant failure before mutation.
7. In `FreshGranted`, arm local-transfer rollback, insert the family/resource
   record, then insert the scope/resource record.
8. Consume `PendingClaimToken` into `ClaimToken`, record accepted statistics,
   clear transfer rollback, and disarm the guard.

There is no `.await` after promotion notification is observed. There is also
no manager reacquisition after the node is adopted: the exact fresh grant and
guard together own rollback across local publication.

Before the first owner-side insertion, set `transfer_started`. Targeted
rollback helpers must inspect and remove zero, one, or both local records only
when resource, exact owner, and `ClaimNo` match. This supports unwind between
the two insertions without consuming a pre-existing or later reacquired claim.
Only successful acceptance increments fresh-accepted statistics.

`Drop` is synchronous:

- `NotStarted`: no manager state exists; burn the pending identity.
- `Waiting` with `Queued`: unlink and consume the node, then rerun FIFO
  promotion.
- `Waiting` with `Provisional`: remove the provisional exact grant and consume
  the node, then rerun promotion.
- `Waiting` with `Released`: consume the released node; the cancelling
  transition already updated compatibility.
- `FreshGranted`: remove any matching partial local publication, release the
  exact fresh grant by pending token, and rerun promotion.
- `Disarmed`: do nothing.

Caller-future Drop is the final observer and therefore consumes its node
directly. A separate migration/test cleanup path instead changes a queued or
provisional node to `Released`, retains the occupied node, and notifies so the
unique original guard can consume it.

Once observation transfers a provisional grant into `FreshGranted`, exclusive
family authority makes that guard the only legitimate cleanup owner. A
concurrent manager-only `release_owner()` against the adopted grant would
violate the Phase 1 family-authority contract and is not a supported race.

### 6. Centralize promotion and notification ordering

Every manager transition that may reduce blocking runs one FIFO-prefix grant
loop while holding the resource-state guard:

```text
while the linked head is grantable:
    detach head
    install exact provisional grant with the same ClaimNo
    mark node Provisional
    collect Arc<Completion<()>>
```

Promotion eligibility preserves:

- compatibility against every exact physical grant from other owners;
- directional coverage against same-family exact grants;
- the current covered same-family bypass rule;
- maximal compatible FIFO-prefix behavior; and
- no blocking conversion.

Release the DashMap resource guard before calling `complete(Ok(()))` on any
collected completion. No error bridge is constructed. An error obtained from
this success-only completion channel is an internal invariant, not a
recoverable lock error.

Resource emptiness becomes:

```text
granted.is_empty()
wait_queue has no linked nodes
wait_queue.live_count() == 0
```

A detached provisional or released node prevents removal until consumption.
After a transition observes empty state, use the existing conditional-removal
pattern so emptiness is rechecked under DashMap synchronization before the map
entry is erased.

### 7. Preserve indexed lifecycle cleanup

Pending state remains call-local and is not inserted into `LockScopeState`.
The active acquisition future must complete or drop its `PendingClaimGuard`
before family authority returns to a lifecycle carrier.

Accepted cleanup remains:

```text
close exact scope
    -> iterate exactly LockScopeState.claims
    -> construct token for each indexed resource
    -> release exact manager grant by ClaimToken
    -> remove matching family/resource record
    -> remove matching scope/resource record
```

Session-explicit unlock performs the same targeted operation for its selected
metadata and data resources. Statement, transaction, operation, and final
session close retain their established order and proof boundaries.
`FreshClaimsGuard` remains the reverse-order rollback owner for claims that
have already completed this acceptance sequence during a fallible
multi-resource operation.

Do not introduce `release_owner()` into any lifecycle path. It lacks an
owner-side scope index and therefore scans only because manager-level tests and
migration defenses can construct raw grants or waiters with no corresponding
`LockScopeState`. Adapt that fallback minimally to the tokenized queue, retain
its existing private/dead-code boundary, and test it only with deliberately raw
manager state. Do not optimize or redesign its global scan in this phase.

Remove duplicate waiter sharing because `Completion::wait_take_result()` has
one observer. Retain migration assertions/repair checks that detect an
impossible second pending attempt or exact grant for the same owner. Phase 3
removes those defenses after the physical family representation makes
singularity structural.

### 8. Enforce the synchronization contract

Owner-side synchronization is exclusive ownership, not a mutex:

- `FamilyLockState` and the target `LockScopeState` are mutably borrowed across
  the complete async acquisition;
- no second family acquire, release, conversion, or close can overlap; and
- guard Drop finishes before those mutable borrows and family authority can
  move to cleanup or another operation.

Shared manager synchronization remains the mutable DashMap entry guard. It is
a shard-level write lock, so all critical sections must be short. The exact
grant vector, queue links, slab entries, free list, node phases, and resource
emptiness are accessed only under that guard. `WaitQueue` and
`WaitNodeSlab` add no mutexes or atomics.

No path may:

- `.await` while holding a DashMap guard;
- call `Completion::complete()` while holding a DashMap guard;
- mutate owner-side maps while holding a DashMap guard;
- hold mutable manager guards for two resources simultaneously; or
- re-enter the resources map from a manager iterator or held entry reference.

Promotion observation uses one manager guard to validate the provisional
state, convert it to guard-owned fresh state, and reclaim the node. Owner-side
publication begins only after that manager guard is released and requires no
manager commit transition.

`Completion<()>` uses its own short state mutex and event. Producers publish
only after manager synchronization is released; observers await without a
manager guard and reacquire the manager only to validate authoritative state.

Promotion and external queued/provisional cancellation serialize on the same
resource guard:

- cancellation first changes `Queued` to `Released` and may promote the next
  waiter; or
- promotion first installs the provisional grant and node state, after which
  cancellation removes that grant and changes the node to `Released`.

Normal indexed close acquires at most one resource guard for each exact token.
The migration-only `release_owner()` fallback may snapshot manager resources
and visit them one at a time, but it is not an authority or performance path
and must not publish notifications while a manager guard is held.

### 9. Update diagnostics, measurements, and documentation

Extend test-only lock diagnostics to report:

- held versus provisional exact grants;
- linked waiter FIFO order;
- waiter slot and generation;
- queued, provisional, and released node phases;
- slab slot length and retained capacity;
- live and free counts; and
- free-list and queue-link consistency.

Add focused layout tests for `WaitNodeID`, `WaitNode`, `WaitNodeSlot`, and
`WaitNodeSlotEntry`. Record the final sizes and observed blocked-path allocation
behavior in Implementation Notes during task resolution. The fixed policy is
zero eager capacity, reuse before growth, and no shrinking; a layout-only field
reordering is allowed when measurement reduces size without weakening the
semantic representation.

Update `docs/lock-system.md` from the `Arc<Waiter>` baseline to the implemented
Phase 2 token and guard lifecycle. Clarify the RFC-0027 Section 7 phase-local
implementation refinement:

- a waiter node survives through provisional notification and observation;
- observation consumes the node and transfers the retained exact fresh grant
  to `PendingClaimGuard`;
- the exact grant, rather than the node, pins manager resource state through
  local publication; and
- guard-owned token-exact rollback eliminates a second manager transition.

During `$task-resolve`, update RFC-0027 Phase 2 with this task path, issue,
status, implementation summary, final layout choice, and validation results.
Keep Phase 3 pending and preserve its prerequisite for token, cancellation,
node-phase, retention, and recreation tests under both feature sets.

## Implementation Notes

- The implemented resource-local waiter representation uses the specified
  direct-index free-list sentinel (`slots.len()`), checked generation advance,
  intrusive FIFO links, and `Queued`, `Provisional`, and `Released` node
  phases. Queue cancellation reads the validated node's two links and does not
  rebuild or scan the FIFO.
- On the 64-bit Linux validation target, the final measured layouts are:
  `WaitNodeID` 16 bytes, `WaitNode` 104 bytes, `WaitNodeSlot` 112 bytes, and
  `WaitNodeSlotEntry` 104 bytes. Field reordering did not produce a smaller
  safe representation because `WaitNodePhase` carries two optional
  generational ids.
- Immediate fresh acquisition allocates no `Completion` and leaves waiter slot
  length and capacity at zero. The first blocked acquisition allocates one
  `Arc<Completion<()>>` and grows the resource-local slab from zero; reclaimed
  slots are reused before growth, capacity is retained for the resource
  state's lifetime, and no waiter path shrinks it.
- Promotion installs the token-exact provisional grant before notification.
  Observation validates the node and grant, clears the provisional marker,
  reclaims the node, and leaves the exact grant owned by
  `PendingClaimGuard`. That exact grant pins the resource and supplies rollback
  while the family and scope indexes are published, so successful publication
  performs no second manager transition.
- Manager diagnostics now distinguish held, provisional, queued, and released
  state and expose claim number, waiter slot/generation, queue order, slot
  length, retained capacity, live count, free-list order, and slot
  generations.
- Implementation validation passed with 77 focused `lock::` tests, 1,694
  workspace tests on the default backend, and 1,587 `doradb-storage` tests on
  `libaio`. Workspace and alternate-backend strict Clippy, workspace build,
  formatting, tracked branch-diff style audit, and the explicit new-file style
  audit all passed. Focused line coverage measured 95.06% for `lock/wait.rs`,
  97.93% for `lock/mod.rs`, and 96.80% for `lock/state.rs`.

## Impacts

- `doradb-storage/src/lock/wait.rs`: new private waiter slab, intrusive FIFO,
  node phases, notification helpers, and pending guard.
- `doradb-storage/src/lock/claim.rs`: move-only pending and accepted claim
  tokens while retaining compact scope claims.
- `doradb-storage/src/lock/mod.rs`: token-aware exact grants, tokenized fresh
  acquisition, synchronous conversion, promotion, cancellation, release,
  diagnostics, and migration-helper adaptation.
- `doradb-storage/src/lock/state.rs`: fresh pending lifecycle, token-exact
  conversion/release, partial local-transfer rollback, and statistics.
- `doradb-storage/src/completion.rs`: reused without a new production
  abstraction; focused tests may be extended only if a waiter-specific
  lost-wakeup case exposes missing generic coverage.
- Session, transaction, statement, admission, engine, catalog, and table tests:
  update raw manager helpers and debug expectations to construct explicit
  pending identity and recognize provisional/released diagnostics. Their
  production ownership and cleanup APIs should not change.
- `docs/lock-system.md`: record the implemented Phase 2 baseline and
  synchronization rules.
- RFC-0027: resolve-time Phase 2 link/status/summary and node-consumption
  clarification.
- Performance: normal head/middle/tail waiter cancellation becomes `O(1)`;
  immediate fresh acquisition allocates no waiter state; exact grant and
  compatibility scans remain until Phase 3.
- Memory: each blocked attempt owns one completion and one reusable slab slot.
  Released nodes intentionally retain a slot until their unique observer
  consumes them.
- Compatibility: public APIs, lock modes, FIFO policy, errors, transaction and
  session ordering, storage formats, and recovery behavior remain unchanged.
- Safety and dependencies: no unsafe code or new dependency is required.

## Test Cases

### Slab and queue structure

1. An empty slab has `free_head == slots.len() == 0`, zero live nodes, and no
   allocation.
2. First insertion appends slot zero with generation zero and advances
   `live_count`.
3. Reclaiming the only node stores `next_free == slots.len()` and makes that
   slot the free head.
4. Multiple reclaims form the exact direct-index free-list order without
   `Option`, offset encoding, or a stale end sentinel.
5. Reusing the final free slot restores `free_head == slots.len()`; the next
   insertion appends safely after the vector length changes.
6. Reuse increments generation and makes the old `WaitNodeID` fail validation
   before mutation.
7. Generation overflow panics before reclaiming or exposing a reusable slot.
8. Slot bounds, generation, occupancy, and phase mismatches produce invariant
   failures without changing queue or free-list state.
9. Head, middle, tail, singleton, and final-node unlink update exactly the
   affected links and preserve FIFO order.
10. Randomized sequential insert/unlink/consume traces match a simple vector
    reference model for order, live nodes, generations, and free slots.
11. Capacity starts at zero, grows only on a blocked allocation with no free
    slot, is reused before further growth, and is retained while the resource
    remains.

### Acquisition, promotion, and transfer

12. Immediate fresh acquisition allocates neither `Completion` nor waiter slot
    and transfers the original `ClaimNo`.
13. A blocked attempt stores the same owner, mode, and `ClaimNo` in its node.
14. Promotion detaches the maximal compatible FIFO prefix, installs exact
    provisional grants before notification, and treats those grants as
    compatibility blockers.
15. Notifications published before the observer starts waiting are still
    consumed without a lost wakeup.
16. Observation validates and consumes the provisional node, clears its grant
    marker, and enters guard-owned `FreshGranted` in one manager transition.
17. Owner-side publication after observation performs no second manager
    lookup or transition.
18. Successful acceptance places matching `ClaimNo` and mode in both local
    indexes and disarms manager rollback.
19. Existing exact covered acquisition remains entirely local, while
    immediate conversion validates the accepted token and retains `ClaimNo`.
20. Incomparable and would-block conversions preserve current operation errors
    and leave all representations unchanged.

### Cancellation and ABA behavior

21. Dropping a queued future unlinks and consumes its exact node in `O(1)` and
    reconsiders the new FIFO head.
22. Dropping after manager promotion but before completion observation removes
    the provisional grant and node without a leak.
23. Dropping after observation releases the adopted exact fresh grant even
    though its waiter node has already been reclaimed.
24. Injected unwind after only the family record or after both local records
    removes only entries with the pending `ClaimNo`, releases the fresh grant,
    and preserves older claims.
25. External migration/test cleanup of a queued node marks it `Released`,
    retains its occupied slot and resource state, and wakes the unique observer
    with `LockWaiterReleased`.
26. External cleanup of a provisional node removes its grant, marks the node
    `Released`, and retains the slot until observation or future Drop consumes
    it.
27. Promotion-versus-release tests deterministically exercise both serialized
    winner orders without timing sleeps.
28. A released node cannot be reused before consumption; after consumption,
    the slot may be reused only with its incremented generation.
29. Resource removal is rejected while a queued, provisional, or released node
    is occupied and while an adopted exact fresh grant exists.
30. After the final node or exact grant is consumed, the resource may be
    removed and recreated; no legitimate old waiter ID survives that boundary.
31. Duplicate pending observer sharing is removed, while impossible
    duplicate-owner manager state is detected by retained migration checks.

### Cleanup and lifecycle integration

32. Accepted `release`, selective session unlock, and `close_scope` validate
    `ClaimToken` and iterate only the exact scope's indexed resources.
33. A stale accepted token cannot release a later reacquisition with the same
    owner/resource and a different `ClaimNo`.
34. `FreshClaimsGuard` rolls back accepted fresh claims in reverse order and
    preserves pre-existing or converted claims.
35. Deliberately raw manager test state remains removable through the
    migration-only `release_owner()` fallback, including queued and
    provisional nodes; normal lifecycle tests never invoke it for indexed
    claims.
36. Statement-future Drop cancels both queued and promoted-but-unobserved lock
    acquisition before statement scope cleanup.
37. Transaction commit, rollback, no-op discard, abandonment, and failed
    precommit retain transaction-lock close before session completion.
38. DDL and maintenance cancellation retain operation authority through
    pending guard Drop, nested transaction cleanup, and operation-scope close.
39. Session teardown closes transaction/operation scopes before indexed
    session-explicit claims, and engine shutdown leaves no occupied waiter
    nodes or exact grants.
40. Existing compatibility matrices, directional family coverage, queue
    bypass, FIFO fairness, fresh-versus-existing semantics, admission handoff,
    DDL explicit-session rejection, and manager diagnostic tests continue to
    pass.

### Validation

41. Run focused lock and affected lifecycle tests before full validation,
    using deterministic hooks, barriers, channels, or semantic predicates
    rather than sleeps.
42. Run:

```text
rtk cargo build --workspace
rtk cargo nextest run --workspace
rtk cargo nextest run -p doradb-storage --no-default-features --features libaio
rtk cargo clippy --workspace --all-targets -- -D warnings
rtk cargo clippy -p doradb-storage --no-default-features --features libaio --all-targets -- -D warnings
rtk cargo fmt --check
```

43. Run `tools/style_audit.rs` against the branch diff and preserve the
    repository's standard nextest timeout and hang-detection configuration.

## Open Questions

No design-blocking questions remain.

- RFC-0027 Phase 3 owns physical family aggregation, removal of exact manager
  mirrors and manager-wide migration scans, final diagnostics, reference-model
  validation, and expanded lock-table benchmarks.
- `docs/backlogs/000167-logical-lock-deadlock-handling.md` continues to own
  deadlock detection and policy.
