---
id: 0027
title: Session-Family Logical Lock System Redesign
status: implemented
tags: [storage-engine, logical-locking, session, transaction, concurrency, performance]
created: 2026-08-06
github_issue: 947
---

# RFC-0027: Session-Family Logical Lock System Redesign

## Summary

Doradb replaced exact-owner physical lock grants with one physical holder per
`(LockResource, LockFamily)`. One engine-local session family now owns all
accepted `SessionExplicit`, `Operation`, `Transaction`, and `Statement` claims
in owner-side indexes, while shared resource state retains only physical family
holders, fixed compatibility aggregates, and pending acquisitions. Covered
reacquisition, covered cross-scope publication, and mode-preserving release are
therefore owner-local. [D7] [D8] [C1] [C2] [U1] [U2]

The three implementation phases also delivered checked session-local
`ClaimNo` identity, a resource-local generational waiter slab, token-exact
queued and provisional cancellation, scope-proportional cleanup, split
physical/exact diagnostics, cumulative logical-lock statistics, and an
expanded lock benchmark workload. The implemented boundary preserves
immediate-only conversion, directional family coverage, FIFO-prefix promotion,
and linear mutation authority. Deadlock policy, poison-aware wait
cancellation, statement-scope removal, waitable upgrades, and a reproducible
pre-cutover benchmark comparison remain deferred. [D18] [D19] [D20] [B3]
[B6] [B7] [B8] [B9] [B10]

## Context

RFC-0016 represented every exact session, operation, transaction, or statement
owner as a separate physical grant. Admission, conversion, release,
cancellation, promotion, and `release_owner()` consequently scanned or rebuilt
shared collections, even when several claims belonged to one serialized
session family. The original `OwnerLockState` accelerated some covered
transaction and statement paths but was only a cache, did not cover explicit
session scope, and could not eliminate shared exact-owner duplication. [D7]
[D8] [C1] [C2]

RFC-0025, RFC-0026, and tasks 000242, 000246, 000247, and 000249 established
the lifecycle prerequisites: exact operation identity, one session operation
coordinator, cancellation ownership, mandatory runtime ownership, and proof
that transaction claims close before session completion. Those contracts made
one move-only family authority possible across foreground work, detached
transactions, prepared DDL or maintenance, cancellation, and teardown. [D6]
[D9] [D10] [D11] [D12] [D13] [B4] [B5]

Implementation refined the proposal in two material ways. Fixed typed claim
slots replaced the proposed inline-then-expanded representation after size and
path review, and the migration-only released-waiter phase disappeared at the
physical-family cutover because the acquisition future and its guard are the
unique pending cleanup owner. [D18] [D19] [D20] [C2] [C3]

`Issue Labels:`
`- type:epic`
`- priority:high`
`- codex`

### Goals

- Represent one shared physical holder per resource/session family while
  retaining exact claims for every supported lifecycle scope.
- Keep repeated covered acquisition, covered nested publication, and
  unchanged-mode release out of shared resource state.
- Preserve one linear mutation and cleanup authority across waits and
  lifecycle transfers.
- Make pending cancellation and promotion exact, ABA-safe, and free of leaked
  provisional grants.
- Replace global owner cleanup scans with exact-scope indexing.
- Preserve compatibility, FIFO fairness, DDL policy, handoff, and terminal
  cleanup ordering while making algorithmic costs observable.

### Non-goals

- Deadlock detection, timeout or victim policy, blocking conversion, `SIX`,
  escalation, weak-lock fast paths, or lock-plan reordering.
- Parallel mutation within one family, multiple active execution lineages,
  distributed family ownership, or a per-family actor.
- Row-lock, MVCC, storage-format, recovery, or transaction-isolation changes.
- A new completion primitive or unsafe waiter storage.

## Design Inputs

### Documents

- [D1] `docs/architecture.md` - engine, session, transaction, catalog, and
  storage subsystem boundaries.
- [D2] `docs/transaction-system.md` - transaction, statement, rollback, and
  cleanup ordering.
- [D6] `docs/engine-component-lifetime.md` - session drain and shutdown
  ownership.
- [D7] `docs/lock-system.md` - implemented ownership, arbitration, lifecycle,
  complexity, and invariant record.
- [D8] `docs/rfcs/0016-logical-lock-manager.md` - original compatibility,
  coverage, conversion, and FIFO contract.
- [D9] `docs/rfcs/0025-session-coordinated-cancellation-cleanup-ownership.md`
  and [D10]
  `docs/rfcs/0026-engine-owned-mandatory-background-runtime.md` - session and
  accepted-execution ownership prerequisites.
- [D11] `docs/tasks/000242-enforce-terminal-transaction-lock-release-ordering.md`
  and [D12] tasks 000246/000247 - terminal proof, operation identity, and
  cancellation sequencing.
- [D13] `docs/tasks/000249-runtime-owned-table-ddl.md` and [D14]
  `docs/tasks/000257-doradb-bench-lock-table-workload.md` - prepared DDL
  ownership and benchmark baseline.
- [D15] `docs/process/unit-test.md` and [D16]
  `docs/process/coding-guidance.md` - deterministic test, ownership,
  performance, and maintainability requirements.
- [D18] `docs/tasks/000258-linear-lock-family-authority-owner-side-indexes.md`,
  [D19] `docs/tasks/000259-tokenized-waiter-provisional-grant-lifecycle.md`,
  and [D20]
  `docs/tasks/000260-physical-lock-family-aggregation-performance-cutover.md`
  - implemented phase outcomes, review findings, and verification evidence.

### Code References

- [C1] `doradb-storage/src/lock/mod.rs` - shared physical families,
  compatibility aggregates, conversion, FIFO promotion, and diagnostics.
- [C2] `doradb-storage/src/lock/state.rs` and
  `doradb-storage/src/lock/claim.rs` - authoritative family/scope indexes,
  typed claim slots, and claim identity.
- [C3] `doradb-storage/src/lock/wait.rs` and
  `doradb-storage/src/completion.rs` - generational waiter storage,
  notification, observation, and rollback guard.
- [C4] `doradb-storage/src/session.rs`, `doradb-storage/src/trx/mod.rs`, and
  `doradb-storage/src/trx/stmt.rs` - authority transfer, handoff,
  cancellation, and cleanup.
- [C8] Catalog and maintenance call sites - ordinary nested acquisition and
  prepared operation ownership.
- [C9] `doradb-storage/src/stats.rs` and `doradb-bench/src` - public
  logical-lock counters and scenario benchmark instrumentation.

### Conversation References

- [U1] The redesign must improve session-level lock performance without
  sacrificing correctness.
- [U2] One physical holder represents a family, while exact logical claims
  remain distinct under linear session-to-operation/transaction ownership.
- [U3] The delivery plan must remain compact and must not add a separate
  prerequisite Phase 0.
- [U4] `ClaimNo` is a checked session-local logical identity; stale accepted
  identity is an asserted invariant, not a recoverable race.
- [U5] Pending notification reuses `crate::completion::Completion`.
- [U6] Complexity, constant factors, and exploitation of linear family
  topology are acceptance criteria.
- [U7] Waiter `(slot, generation)` identity is transient and separate from
  `ClaimNo`; a live node pins its resource state.
- [U8] Waiter storage uses a minimal safe internal slab rather than a
  general-purpose dependency.

### Source Backlogs

- [B1] `docs/backlogs/closed/000171-exact-family-lock-system-redesign.md` -
  source acceptance criteria completed by the three phases.
- [B2] `docs/backlogs/closed/000115-explicit-session-lock-cache.md` - explicit
  session caching absorbed by authoritative family state.
- [B3] `docs/backlogs/000167-logical-lock-deadlock-handling.md` - deferred
  deadlock policy.
- [B4] `docs/backlogs/closed/000169-separate-session-operation-lock-scopes.md`
  and [B5]
  `docs/backlogs/closed/000170-session-coordinated-cancellation-cleanup.md` -
  completed prerequisites.
- [B6] `docs/backlogs/000178-common-multi-domain-error-carrier.md` and [B7]
  `docs/backlogs/000179-cancel-pending-logical-lock-acquisition-on-engine-poison.md`
  - deferred error and poison propagation.
- [B8] `docs/backlogs/000180-remove-statement-scope-logical-locks.md`, [B9]
  `docs/backlogs/000181-waitable-comparable-same-scope-lock-upgrades.md`, and
  [B10]
  `docs/backlogs/000182-capture-lock-family-cutover-benchmark-comparison.md` -
  post-cutover follow-ups.

## Decision

### Final authority and owner-side representation

`LockFamily(SessionID)` is the physical identity. `LockOwner { family, scope }`
is the exact identity, where the implemented scope classes are
`SessionExplicit`, `Operation`, `Transaction`, and `Statement`. Every session
allocates one boxed family authority. The same allocation moves through idle
session state, public transactions, prepared and accepted operations, private
transactions, cancellation, terminal proof, and teardown; it is never cloned
or reconstructed from an id. At most one lock mutation or cleanup may be
active for a family, including across `.await`. [D6] [D9] [D10] [C4] [U2]

Accepted claims are authoritative in two owner-side directions:

```text
FamilyLockState.resources[resource].typed_scope_slot = (ClaimNo, mode)
LockScopeState.claims[resource]                       = (ClaimNo, mode)
```

The family/resource entry embeds fixed typed slots. There is no per-family
claim hash, heap expansion for a second scope, or shared exact-claim mirror.
Each fresh logical attempt reserves a checked session-local `ClaimNo`; rejected
and cancelled attempts may burn numbers, conversion retains the number, and
release followed by reacquisition receives a new one. Accepted release asserts
the exact number before either index can change. [D18] [D20] [C2] [U4] [U6]

### Shared physical arbitration

Each resource stores fixed mode counts, a compact presence mask, one
`PhysicalFamilyState` per participating family, and one intrusive FIFO:

```text
Held(mode)                accepted family aggregate
Queued(node_id)           uncounted first physical request
Provisional(mode,node_id) counted promotion awaiting unique observation
```

`Held` and `Provisional` contribute one physical mode each; `Queued`
contributes no holder. Compatibility with other families reads only the fixed
mode aggregates. Accepted exact owners and accepted `ClaimNo`s never appear in
shared state. A resource is removable only after its families, counts, linked
queue, and occupied slab nodes are all empty, so no waiter id can survive
whole-resource recreation. [D7] [D20] [C1] [U2] [U7]

### Acquisition, conversion, release, and policy

An exact covered reacquisition returns locally. A fresh cross-scope claim is
admissible only when every other live same-family claim covers its requested
mode. If the family covering mode remains unchanged, publication or release
updates only the two owner indexes. Otherwise one guarded manager transition
installs, strengthens, replaces, or removes the physical family and promotes
the maximal compatible FIFO prefix when blocking decreases. [D7] [C1] [C2]
[U1] [U6]

Coverage remains directional. `S` and `IX` are incomparable, and the manager
does not synthesize `SIX` or over-lock with `X`. Same-scope comparable
strengthening retains its `ClaimNo` and succeeds only when immediate
compatibility and queue state permit; blocked conversion returns
`LockUpgradeWouldBlock`. A compatible new family cannot bypass an older
incompatible waiter, while a covered claim in an already-held family remains
local. [D8] [B3] [B9]

DDL preflights the family-local `SessionExplicit` slot and rejects a target
table already explicitly locked by the same session. Maintenance records its
own exact operation claims even when explicit locks cover them. Nested catalog
transactions use ordinary covered acquisition; the physical cutover removed
`PreparedCatalogWriteAuthority` and its bypass branches. Statement metadata
handoff publishes the destination transaction claim before releasing the
statement claim. [D2] [D13] [D20] [C4] [C8]

### Pending wait, promotion, and cancellation

A blocked first-family attempt stores its owner, target mode, `ClaimNo`, and
one `Completion<()>` in a resource-local node addressed by
`WaitNodeID { slot, generation }`. The safe slab uses a vector, an intrusive
free list, checked generation advance, and a live count. Queue append and exact
unlink are constant time, and freed slots are reused before vector growth.
[D16] [D19] [C3] [U5] [U8]

Promotion first counts the compatible FIFO prefix as `Provisional`, then drops
resource synchronization and publishes deferred notifications. The unique
observer validates node generation, pending logical fields, node phase, and
physical family state before changing the family to `Held` and reclaiming the
node. `PendingClaimGuard` owns rollback until the manager and both owner-side
indexes agree; dropping it synchronously removes its exact queued,
provisional, immediate, or partially published state. [D19] [D20] [C1] [C3]
[U7]

The final system has no released-waiter phase, resource incarnation, duplicate
wait observer, new completion type, or background waiter cleanup. A caller
that has polled an acquisition must continue polling or drop it. Engine poison
does not yet cancel a success-only lock wait; backlog 000179 owns that
cross-domain result and cancellation change. [D20] [B7]

### Cleanup and lifecycle ordering

Closing a scope drains exactly its indexed claims. Only releases that change
the physical family mode enter the manager; production has no
`release_owner()` scan or raw exact-owner repair path. A non-cloneable terminal
proof carries the returned family authority and proves that transaction claims
closed before session completion. [D11] [D18] [D20] [C2] [C4]

The durable close order is:

```text
pending statement work and statement claims
    -> transaction claims
    -> operation claims
    -> session completion or next operation
    -> explicit session claims at unlock or final teardown
```

Accepted DDL and maintenance retain operation scope until nested transaction
cleanup completes. Engine shutdown drains session and operation owners; the
lock manager remains passive. Logical lock state is volatile and is not
recovered from redo. [D2] [D6] [D10] [C4] [C8]

### Complexity, diagnostics, and compatibility

With four fixed modes, `K` promoted waiters, and `H_scope` claims in one
closing scope, the implemented bounds are:

| Operation | Implemented cost |
|---|---:|
| Repeated exact coverage | `O(1)` owner-local |
| Covered cross-scope publication | `O(4)` owner-local |
| Fresh acquisition or immediate conversion | `O(4)` shared average |
| Mode-preserving release | `O(4)` owner-local |
| Queued cancellation | `O(1)` plus actual promotion work |
| Promote `K` waiters | `O(K * 4)` |
| Scope cleanup | `O(H_scope + physical changes + promotion work)` |

Diagnostics intentionally split the manager's physical family/waiter view
from the owner-side exact claim view. Tests join them by resource and family.
Public cumulative `LogicalLockStats` and the benchmark scenario counters make
local, shared, queue, promotion, and cleanup work observable. [D20] [C1] [C2]
[C9] [U6]

Public lock modes, error classifications, compatibility, FIFO fairness,
transaction isolation, persistent formats, and both storage backends remain
compatible. The internal obsolete `LockWaiterReleased` error disappeared; the
new cumulative statistics are additive observability. [D8] [D20]

## Alternatives Considered

### Mirror exact claims in shared resource state

- Summary: Keep owner indexes but also store every exact claim in each shared
  family entry.
- Why Not Chosen: Linear family authority makes the second authoritative copy
  unnecessary and would force covered publication and non-maximal release
  through shared synchronization. Other families need only the physical mode.
- References: [D7] [C1] [C2] [U2] [U6]

### Add only an explicit-session cache

- Summary: Extend the old owner cache to `SessionExplicit` while retaining
  exact physical grants, vector/deque scans, and global cleanup.
- Why Not Chosen: This would complete backlog 000115 but would not aggregate a
  family, fix waiter ABA, remove queue rebuilding, or make cleanup
  scope-proportional.
- References: [C1] [C2] [B1] [B2] [U1]

### Introduce a family actor

- Summary: Serialize all family lock operations through a command queue.
- Why Not Chosen: Current lifecycle ownership is already linear. An actor
  would add scheduling, allocation, completion, and shutdown states to every
  covered local operation without enabling a current requirement.
- References: [D9] [D10] [C4] [U2]

### Use global claim ids and a lock-specific completion

- Summary: Allocate claim ids from an engine-global atomic and introduce a
  new result-carrying waiter completion.
- Why Not Chosen: Claim uniqueness is needed only within one serialized
  family, and `Completion<()>` already provides lost-wakeup-safe one-shot
  notification. Global identity would add hot shared synchronization.
- References: [C3] [U4] [U5] [U6]

### Use a general slab plus resource incarnation

- Summary: Add a generic slab dependency and an incarnation to distinguish
  waiter ids across resource destruction.
- Why Not Chosen: The implemented safe vector/free-list container is smaller,
  and resource removal is forbidden while any live node can retain an id.
  Slot generation handles reuse within the live resource.
- References: [D16] [C3] [U7] [U8]

## Unsafe Considerations

No new unsafe code was introduced. Fixed claim slots, waiter slab indices,
free-list links, generations, and family authority use safe Rust. Layout
assertions are gated to validated 64-bit targets. Any future unsafe
representation optimization requires separate evidence and review; this RFC
does not authorize it. [D16] [D18] [D19]

## Implementation Phases

- **Phase 1: Linear Family Authority And Owner-Side Indexes**
  - Scope: Establish one move-only family authority, authoritative family and
    scope indexes, fixed exact-scope slots, checked `ClaimNo`, and targeted
    lifecycle cleanup while retaining exact manager mirrors.
  - Outcome: Fixed slots replaced the proposed expanding representation;
    review simplified identity and publication invariants, and all lifecycle
    carriers transferred the same boxed authority.
  - Task Doc: `docs/tasks/000258-linear-lock-family-authority-owner-side-indexes.md`
  - Task Issue: `#948`
  - Phase Status: `done`
  - Implementation Summary: Implemented authoritative owner-side family and scope indexes, checked exact claim identity, linear lifecycle transfer, and scope-proportional cleanup while retaining migration mirrors. [Task Resolve Sync: docs/tasks/000258-linear-lock-family-authority-owner-side-indexes.md @ 2026-08-06]
  - Related Backlogs:
    - `docs/backlogs/closed/000115-explicit-session-lock-cache.md`
    - `docs/backlogs/closed/000171-exact-family-lock-system-redesign.md`

- **Phase 2: Tokenized Waiter And Provisional-Grant Lifecycle**
  - Scope: Replace pointer/deque waiter identity with pending claim tokens,
    a resource-local generational slab, intrusive FIFO links, provisional
    promotion, existing completion notification, and one rollback guard.
  - Outcome: Queue cancellation became exact and constant-time; promotion and
    cancellation cannot leak a provisional or partially published claim.
  - Task Doc: `docs/tasks/000259-tokenized-waiter-provisional-grant-lifecycle.md`
  - Task Issue: `#950`
  - Phase Status: `done`
  - Implementation Summary: Implemented token-exact pending ownership, generational waiter identity, constant-time queued cancellation, provisional-grant rollback, and resource pinning while retaining exact manager grants for Phase 3. [Task Resolve Sync: docs/tasks/000259-tokenized-waiter-provisional-grant-lifecycle.md @ 2026-08-06]

- **Phase 3: Physical Family Aggregation And Performance Cutover**
  - Scope: Cut shared state over to one physical family entry, fixed
    compatibility aggregates, local covered changes, final lifecycle
    integration, split diagnostics, statistics, and expanded benchmarks.
  - Outcome: Removed exact manager mirrors, production owner scans,
    migration repair paths, prepared catalog bypass, and the released-waiter
    phase; preserved ordinary nested acquisition and deterministic cleanup.
  - Task Doc: `docs/tasks/000260-physical-lock-family-aggregation-performance-cutover.md`
  - Task Issue: `#953`
  - Phase Status: `done`
  - Implementation Summary: Implemented one shared physical entry per lock family, owner-local exact authority, bounded compatibility work, ordinary nested catalog acquisition, deterministic structural observability, and the final migration cleanup. [Task Resolve Sync: docs/tasks/000260-physical-lock-family-aggregation-performance-cutover.md @ 2026-08-07]
  - Related Backlogs:
    - `docs/backlogs/closed/000171-exact-family-lock-system-redesign.md`

## Validation and Results

All three tasks recorded review and behavior verification. Phase 1 passed
1,683 default-feature workspace tests, 1,576 `libaio` storage tests, strict
Clippy, formatting, diff checks, and a 14-file style audit. Phase 2 passed 77
focused lock tests, 1,694 workspace tests, 1,587 alternate-backend tests,
strict lint/format/style gates, focused coverage, and resolve-time stress runs;
focused line coverage was 95.06% for `lock/wait.rs`, 97.93% for `lock/mod.rs`,
and 96.80% for `lock/state.rs`. [D18] [D19]

Phase 3 passed its 26-file style audit, 1,688 workspace tests, 1,579
alternate-`libaio` tests, release benchmark build and CLI execution, and 13
focused scan/waiter regressions. Deterministic coverage exercises counts and
masks, exact/physical agreement, covered paths, conversion, release, scope
close, every waiter position and guard-drop state, generation reuse, nested
DDL and maintenance, transaction completion, teardown, shutdown, statistics,
and benchmark validation. [D20]

The final candidate ran all ten benchmark scenarios across 26 valid
configurations with zero failures and matching structural counters.
Candidate-only samples demonstrated the implemented paths, but they were
single optimized-debug-assertion measurements without equivalent pre-cutover
instrumentation. They are not treated as authoritative comparative evidence;
backlog 000182 retains reproducible paired release-profile trials. [D20] [B10]

## Consequences

### Positive

- Covered exact and nested claims avoid shared synchronization and allocation.
- One shared entry per family bounds compatibility work by the fixed mode set.
- Scope cleanup touches owned claims instead of scanning the lock table.
- Generational cancellation avoids queue rebuilding and makes ABA explicit.
- Provisional state participates in compatibility before notification, while
  the pending guard prevents leaked physical or logical state.
- Existing lifecycle authority and completion infrastructure are reused.

### Negative

- Every accepted claim must update two owner-side indexes consistently.
- The boxed family authority must move correctly through every lifecycle
  carrier.
- The project owns a small custom slab/free-list implementation and its
  invariants.
- Hot resources remain serialized for physical changes and queue transitions.
- FIFO intentionally permits head-of-line blocking.
- Diagnostics must join physical and exact views.
- Success-only lock waits do not yet observe engine poison.

## Open Questions

No design-blocking question remains for the implemented program. The missing
paired pre-cutover performance comparison is explicitly accepted as deferred
verification rather than an unresolved implementation claim. [B10]

## Future Work

- `docs/backlogs/000167-logical-lock-deadlock-handling.md` - bounded deadlock,
  timeout or prevention policy and diagnostics. [B3]
- `docs/backlogs/000178-common-multi-domain-error-carrier.md` - preserve
  operation and fatal reports across lifecycle domains. [B6]
- `docs/backlogs/000179-cancel-pending-logical-lock-acquisition-on-engine-poison.md`
  - cancel queued or provisional preparation on poison and propagate the
  original fatal report. [B7]
- `docs/backlogs/000180-remove-statement-scope-logical-locks.md` - remove the
  statement scope after the production audit found no durable
  statement-only lifetime. [B8]
- `docs/backlogs/000181-waitable-comparable-same-scope-lock-upgrades.md` -
  add waitable strengthening only after deadlock policy can represent a
  family that both holds and waits. [B9]
- `docs/backlogs/000182-capture-lock-family-cutover-benchmark-comparison.md` -
  reproduce equivalent pre-cutover and final release builds and retain
  repeated paired evidence. [B10]
- Any future parallel same-family execution must replace the linear authority
  proof with an explicit serialization design. [U2]

## References

- `docs/lock-system.md` and `docs/rfcs/0016-logical-lock-manager.md`
- RFC prerequisites: RFC-0025 and RFC-0026 under `docs/rfcs/`
- Phase records: tasks 000258, 000259, and 000260 under `docs/tasks/`
- `docs/backlogs/closed/000171-exact-family-lock-system-redesign.md`
- Open follow-ups: backlogs 000167, 000178-000182 under `docs/backlogs/`
