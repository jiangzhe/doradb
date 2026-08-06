---
id: 000259
title: Tokenized Waiter and Provisional-Grant Lifecycle
status: implemented  # proposal | implemented | superseded
created: 2026-08-06
github_issue: 950
---

# Task: Tokenized Waiter and Provisional-Grant Lifecycle

## Summary

Implemented RFC-0027 Phase 2 by replacing pointer-identified lock waiters and
the `VecDeque` wait queue with resource-local generational waiter nodes,
intrusive FIFO links, move-only pending claim identity, and one
cancellation-safe `PendingClaimGuard`.

Each fresh exact claim now carries its reserved `ClaimNo` through immediate
grant, queued wait, provisional promotion, observation, owner-side
publication, and accepted `ClaimToken`. Promotion installs the exact
provisional manager grant before waking the waiter. Observation validates and
consumes the node, while the pending guard owns exact rollback until both
authoritative owner-side indexes are published.

Phase 2 retains exact physical `GrantedLock` entries, current compatibility
scans, and migration-only manager cleanup. Physical family aggregation and
removal of exact manager mirrors remain RFC-0027 Phase 3.

## Context

Issue Labels:

- type:task
- priority:high
- codex

Parent RFC:

- `docs/rfcs/0027-session-family-logical-lock-system-redesign.md`

RFC Relationship:

- Phase 2: Tokenized Waiter And Provisional-Grant Lifecycle.

Task 000258 established one move-only family authority, authoritative
family/resource and exact-scope/resource indexes, checked session-local claim
numbers, and targeted lifecycle cleanup. The remaining manager path still used
`Arc<Waiter>` identity, duplicate waiter sharing, and queue rebuilding during
cancellation.

This task replaced those transient mechanisms without changing the Phase 1
owner-side authority model. It reused the existing success-only
`Completion<()>`, kept pending state call-local, and preserved indexed cleanup
for statements, transactions, operations, and session-explicit locks.

The implementation exposed a separate liveness gap: a logical-lock waiter does
not observe engine poison and may remain active until its future is cancelled
or its blocker releases. That follow-up is recorded in
`docs/backlogs/000179-cancel-pending-logical-lock-acquisition-on-engine-poison.md`.

Related work remains in open backlog
`docs/backlogs/000171-exact-family-lock-system-redesign.md`; deadlock policy
remains in backlog 000167.

## Goals

1. Replace waiter pointer identity with ABA-safe resource-local
   `(slot, generation)` identity.
2. Make normal queued cancellation unlink head, middle, or tail nodes in
   `O(1)`.
3. Preserve one reserved `ClaimNo` through the complete fresh-claim lifecycle.
4. Install provisional grants before notification and clean every cancellation
   point synchronously and exactly.
5. Keep resource state alive until every queued, provisional, or released node
   and every exact grant is consumed.
6. Preserve FIFO-prefix promotion, compatibility, family coverage,
   immediate-only conversion, and `LockWaiterReleased`.
7. Keep normal lifecycle cleanup proportional to authoritative scope indexes.
8. Provide deterministic structure, ABA, cancellation, transfer, retention,
   and recreation coverage under both storage backends.

## Non-Goals

1. Do not aggregate exact manager grants into physical family holders.
2. Do not remove exact manager mirrors, migration checks, or the test/migration
   `release_owner()` fallback.
3. Do not add blocking conversion, downgrade, escalation, timeout, deadlock, or
   victim policy.
4. Do not add parallel same-family mutation, a family mutex, actor, lease, or
   cleanup coordinator.
5. Do not add an external slab dependency, resource incarnation, unsafe
   storage, or a new completion primitive.
6. Do not make lock waiting poison-aware in this phase.
7. Do not change public storage APIs, isolation, MVCC, recovery, schemas,
   persisted formats, or storage I/O behavior.
8. Do not perform the Phase 3 benchmark and physical-representation cutover.

## Plan

### Waiter storage and identity

Each resource owns a safe `WaitNodeSlab` and intrusive `WaitQueue`.
`WaitNodeID` combines a vector slot with a checked generation. Vacant slots
form a direct-index free list whose end sentinel is `slots.len()`.

The slab starts with zero capacity, reuses reclaimed slots before growth,
retains capacity for the resource lifetime, and never shrinks. Reclamation
increments the generation before exposing a slot for reuse. Generation
exhaustion and stale, vacant, or mismatched identities fail before mutation.

Queued nodes store exact owner, claim number, target mode, completion, and
phase. `Queued` carries previous and next links; `Provisional` and `Released`
remain occupied until the unique waiter observes or drops them. Resource
emptiness requires no exact grants, no linked nodes, and zero occupied slab
nodes.

### Pending claim lifecycle

`PendingClaimToken` and `ClaimToken` are move-only. A fresh miss reserves its
claim number before manager entry; rejection and cancellation burn it.
Successful conversion preserves an accepted claim number, while unlock and
reacquire allocates a new one.

`PendingClaimGuard` exclusively borrows the manager, family state, and target
scope across the acquisition:

- Immediate compatibility installs an exact fresh grant without allocating a
  completion or waiter node.
- A blocked request allocates one completion and one queued node.
- Promotion detaches a compatible FIFO prefix, installs matching provisional
  grants, changes the nodes to `Provisional`, and collects notifications.
- Observation validates the node and provisional grant in one manager
  transition, clears the provisional marker, and reclaims the node.
- The adopted exact grant remains guard-owned while the family and scope
  indexes are published, after which the pending token is consumed into its
  accepted token.

Guard drop consumes its owned pending token. Depending on state, it unlinks a
queued node, removes a promoted-but-unobserved grant, consumes a released node,
or rolls back token-matching local publication and the adopted fresh grant.
Every compatibility-reducing cleanup reruns FIFO promotion.

### Manager synchronization and notification

The exact grant vector temporarily records `ClaimNo` and an optional
provisional node. Provisional grants participate fully in compatibility and
family coverage, while diagnostics distinguish them from accepted held
claims.

All grant, queue, slab, phase, and resource-emptiness mutation occurs under one
resource guard. No path awaits, completes a notification, mutates owner-side
indexes, holds two resource guards, or re-enters the resource map while that
guard is held. Collected completions are published only after manager
synchronization is released.

Promotion and external migration cleanup serialize on the same resource
guard. External cleanup leaves a `Released` node for its unique observer and
returns `LockWaiterReleased`; normal future cancellation consumes its own
node directly.

### Lifecycle and migration boundaries

Pending state never enters `LockScopeState`. The acquisition future must
complete or drop its guard before family authority can move to another
lifecycle carrier.

Accepted release and scope close validate exact tokens and visit only the
scope's indexed resources. `FreshClaimsGuard` continues to roll back already
accepted fresh claims in reverse order for fallible multi-resource
operations.

The raw manager helpers, duplicate-state checks, exact grant scans, and
`release_owner()` remain test/migration boundaries. Production statement,
transaction, operation, session, DDL, and maintenance cleanup does not use a
manager-wide scan.

### Diagnostics and documentation

Test diagnostics report held, provisional, queued, and released entries with
claim number, FIFO order, waiter slot/generation, allocated slots, retained
capacity, live count, free-list order, and slot generations.

Test-only waiter snapshots and construction helpers live in the test module as
free helper functions rather than widening production types with inherent
methods. `docs/lock-system.md` records the implemented token, guard,
notification, and resource-retention lifecycle.

## Implementation Notes

Implemented RFC-0027 Phase 2 with token-exact pending ownership, constant-time queued cancellation, provisional-grant rollback, and resource pinning while retaining exact manager grants for Phase 3.

- The final slab uses the direct `slots.len()` sentinel, checked generation
  advance, intrusive links, and `Queued`, `Provisional`, and `Released` phases.
- On validated 64-bit targets, layouts are: `WaitNodeID` 16 bytes,
  `WaitNode` 104 bytes, `WaitNodeSlot` 112 bytes, and `WaitNodeSlotEntry`
  104 bytes. The recorded assertions are gated to 64-bit pointer width.
- Immediate fresh acquisition leaves waiter length and capacity at zero. The
  blocked path allocates one completion and one reusable slab slot.
- Observation transfers the exact fresh grant to `PendingClaimGuard`; owner
  publication needs no second manager transition.
- Review changed guard cleanup to take and consume the owned
  `PendingClaimToken`, making the move-only cleanup authority explicit.
- Review moved waiter test snapshots and test-only construction/inspection
  logic into the test module and converted unnecessary inherent methods into
  helper functions.
- Poison investigation confirmed that queued lock waits currently use only
  success completion. Poison-aware cancellation and original-fatal
  propagation were intentionally deferred to backlog 000179 rather than
  weakening the Phase 2 completion contract.
- CI investigation found a test-only `Arc<Table>` in
  `test_drop_waits_for_active_freeze` retained longer than necessary. The test
  now releases that assertion owner before unblocking freeze so it cannot
  overlap dropped-runtime purge; the production uniqueness invariant remains
  unchanged.
- Final implementation verification passed 77 focused lock tests, 1,694
  default-feature workspace tests, and 1,587 alternate-backend storage tests.
  Workspace build, strict workspace and `libaio` Clippy, formatting, and
  focused coverage also passed.
- Resolve-time verification passed the six-file branch-diff style gate, the
  full 1,586-test default `doradb-storage` suite, and 100 consecutive
  `test_drop_waits_for_active_freeze` runs under each backend.
- Focused line coverage measured 95.06% for `lock/wait.rs`, 97.93% for
  `lock/mod.rs`, and 96.80% for `lock/state.rs`.

## Impacts

- Lock manager waiter identity is now generational and cancellation unlink is
  `O(1)` instead of queue rebuilding.
- Fresh claim identity is preserved across manager and owner-side state;
  partial publication rollback is token-exact.
- Blocked attempts use one completion and one reusable slab slot; immediate
  attempts allocate no waiter storage.
- Diagnostics expose waiter phases, identity, queue position, and slab
  retention for deterministic verification.
- Exact grant vectors, compatibility scans, and migration cleanup remain until
  Phase 3.
- Session, transaction, statement, DDL, maintenance, and shutdown ownership
  contracts are unchanged.
- Public APIs, error classifications, persistent formats, recovery, MVCC,
  dependencies, and storage-backend behavior are unchanged.

## Test Cases

Completed coverage verifies:

1. Empty, append, reclaim, reuse, retained-capacity, direct-free-list, and
   generation-exhaustion slab behavior.
2. Stale generation, bounds, occupancy, phase, and link mismatches fail before
   mutation.
3. Head, middle, tail, singleton, and randomized intrusive-queue traces match
   a vector/free-list reference model.
4. Immediate fresh claims allocate no waiter state and retain their reserved
   claim number.
5. Blocked claims preserve identity through queueing, FIFO-prefix promotion,
   provisional grant, observation, and acceptance.
6. Listener-before-check completion prevents lost wakeups when notification
   precedes observation.
7. Queued, provisional, released, adopted-fresh, and partially published
   cancellation leaves no grant, node, or owner-index leak.
8. Released nodes prevent premature slot/resource reuse; consumed nodes reuse
   only with a new generation.
9. Adopted exact grants pin the resource until pending-guard acceptance or
   drop.
10. Stale accepted tokens cannot release a later reacquisition.
11. Existing compatibility, family coverage, same-family bypass,
    immediate-only conversion, FIFO fairness, and explicit-session DDL policy
    remain unchanged.
12. Scope close, selective unlock, statement cancellation, transaction
    terminal paths, DDL/maintenance cancellation, session teardown, and
    migration-only raw cleanup preserve exact ownership.
13. Debug snapshots report stable queue order and all manager phases without
    timing sleeps.
14. Default and `libaio` storage backends, workspace Clippy, formatting, style
    rules, and focused coverage satisfy the repository gates.

## Open Questions

No blocking Phase 2 questions remain.

- `docs/backlogs/000179-cancel-pending-logical-lock-acquisition-on-engine-poison.md`
  owns poison-aware pending acquisition cancellation and original-fatal
  propagation.
- RFC-0027 Phase 3 owns physical family aggregation, removal of exact manager
  mirrors and manager-wide migration scans, final diagnostics,
  reference-model validation, and expanded lock-table benchmarks.
- `docs/backlogs/000167-logical-lock-deadlock-handling.md` continues to own
  deadlock detection and victim policy.
