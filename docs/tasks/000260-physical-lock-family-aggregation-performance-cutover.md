---
id: 000260
title: Physical Lock Family Aggregation and Performance Cutover
status: implemented  # proposal | implemented | superseded
created: 2026-08-06
github_issue: 953
---

# Task: Physical Lock Family Aggregation and Performance Cutover

## Summary

Implemented RFC-0027 Phase 3 by replacing accepted exact-owner grants in the
shared lock manager with one physical entry per
`(LockResource, LockFamily)`. Exact scope, mode, and `ClaimNo` authority now
exists only in the session family's fixed owner-side indexes.

Covered exact acquisitions, covered cross-scope publications, unchanged-mode
conversions, and unchanged-mode releases remain owner-local. Shared resource
state contains fixed mode counts and a mask, physical family state, and
transient FIFO waiter state. Physical changes use one guarded resource
transition and publish any resulting notifications after synchronization is
released.

The cutover also removed exact-manager scans and repair APIs, the
`PreparedCatalogWriteAuthority` bypass, and the migration-only released-waiter
state. Public logical-lock statistics and expanded deterministic lock
benchmarks expose the resulting structural work.

## Context

Issue Labels:

- type:task
- priority:high
- codex

Parent RFC:

- `docs/rfcs/0027-session-family-logical-lock-system-redesign.md`

RFC Relationship:

- Phase 3: Physical Family Aggregation And Performance Cutover.
- Task 000258 established linear family authority and authoritative owner-side
  indexes.
- Task 000259 established tokenized waiter identity, provisional promotion,
  and cancellation-safe pending guards.

Source Backlogs:

- `docs/backlogs/closed/000171-exact-family-lock-system-redesign.md`

Related Backlogs:

- `docs/backlogs/000179-cancel-pending-logical-lock-acquisition-on-engine-poison.md`
- `docs/backlogs/000180-remove-statement-scope-logical-locks.md`
- `docs/backlogs/000181-waitable-comparable-same-scope-lock-upgrades.md`
- `docs/backlogs/000182-capture-lock-family-cutover-benchmark-comparison.md`

The transitional manager stored every accepted exact owner and claim number
in shared resource vectors even though one family already had authoritative
exact indexes. Compatibility and cleanup consequently retained exact scans,
duplicate repair, and manager-global owner operations.

The final representation relies on the existing linearity proof: one session
family has one mutation authority, and a blocked acquisition owns that
authority until it completes or is dropped. No lifecycle path can therefore
release a still-owned waiter independently.

## Goals

1. Store one accepted physical holder per resource and session family.
2. Keep all accepted exact claim authority in bounded owner-side indexes.
3. Make covered and unchanged-mode operations avoid shared manager work.
4. Replace grant-vector compatibility scans with fixed mode counts and masks.
5. Preserve compatibility, FIFO-prefix granting, directional family coverage,
   and immediate-only conversion.
6. Preserve token-exact queued, provisional, publication, and cancellation
   cleanup.
7. Make scope close proportional to the scope's indexed claims and resulting
   physical changes.
8. Remove exact-manager mirrors, scans, migration repair, and prepared catalog
   lock bypasses.
9. Expose split physical/exact diagnostics and stable logical-lock statistics.
10. Add deterministic benchmark scenarios for local, physical, queue,
    handoff, and scope-close paths.

## Non-Goals

1. No deadlock detection, victim selection, timeout, lease, or escalation.
2. No waitable conversion, cross-scope strengthening, or `SIX` mode.
3. No parallel mutation within one session family or additional family
   synchronization.
4. No forced reclamation of an owned acquisition future that is neither
   polled nor dropped.
5. No poison-aware lock waiting or change to fatal-error propagation.
6. No resource-map repartitioning, lock-free replacement, unsafe packing, or
   external waiter-slab dependency.
7. No public SQL, isolation, MVCC, recovery, schema, or persisted-format
   change.
8. No hard numeric performance threshold; structural correctness remains the
   cutover gate.

## Plan

### Physical and exact authority

`ResourceState` stores a family map, four checked physical holder counts, a
compact presence mask, and one intrusive waiter queue. A physical family is
exactly one of:

- held in its accepted covering mode;
- queued without contributing a physical holder; or
- provisional and counted while awaiting its unique observer.

Accepted exact claims do not appear in shared state. `FamilyLockState` retains
the resource-oriented fixed scope slots, while each `LockScopeState` retains
the inverse cleanup index. Both owner-side entries carry the same claim number
and exact mode.

`ModeMask` remains a small purpose-built `u8` wrapper. `LockMode` remains an
enum because it represents one semantic mode rather than a combinable set, so
the `bitflags` crate is not used. `LockManager` owns its `FastDashMap`
directly; the resource field does not require an outer `Arc`.

### Acquisition, conversion, and release

Repeated exact coverage returns locally. A fresh exact claim covered by the
family's current physical mode publishes into both owner-side indexes without
manager access.

The first physical claim enters the manager once. It either installs a held
family immediately or appends one generational waiter node. Immediate grants
allocate no completion or waiter storage.

Comparable same-scope strengthening remains immediate-only. It preserves
`ClaimNo`, succeeds only with an empty queue and external compatibility, and
otherwise returns `LockUpgradeWouldBlock`. Incomparable modes remain
`LockConversionNotSupported`; sibling-scope policy remains
`LockFamilyConflict`.

Release validates exact identity and recomputes the bounded family aggregate.
An unchanged physical mode is removed locally. A physical mode change or final
family removal performs one manager transition and reruns maximal FIFO-prefix
promotion. Scope close visits only the exact scope's resource index.

### Pending waiter lifecycle

`PendingClaimGuard` is the unique synchronous cleanup owner from claim-number
reservation through acceptance. It handles queued cancellation, provisional
rollback, immediate fresh-grant rollback, partial owner-side publication, and
final disarm.

Promotion installs counted provisional state before notification. The waiter
stages both owner-side indexes, observes and validates the matching node once,
converts the physical family to held, reclaims the node, consumes the pending
token into its accepted identity, and disarms without an intervening await.

Queue unlink is `O(1)` for head, middle, and tail nodes. Slot generation
prevents ABA within a live slab, and queued or provisional nodes pin the
resource until consumed. The removed migration-only `Released` phase is not
recreated because no independent lifecycle release path remains.

Deferred notifications use zero, one, or many completion storage. Promotion
finishes under the resource guard; notification occurs afterward, with a Drop
fallback preventing committed wakeups from being lost during unwind.

### Lifecycle and catalog integration

DDL-versus-session-explicit policy is checked from family-local exact slots.
Accepted DDL retains its operation claims while nested private catalog
transactions acquire ordinary exact metadata and data claims under the same
family.

`PreparedCatalogWriteAuthority`, its runtime fields, and catalog bypass
parameters were removed. Failure rollback still releases only claims reported
fresh by the ordinary acquisition path.

Statement-to-transaction handoff publishes transaction metadata protection
before releasing the statement claim. Transaction, operation, and
session-explicit scopes retain inside-out cleanup and continuous physical
coverage.

### Diagnostics and benchmarks

Public `LogicalLockStats` separates owner-local hits and publications from
resource transitions, upgrades, queue mutation, cancellation position,
promotion, scope-close work, completion allocation, slab growth/reuse, and
current/peak physical cardinalities.

Test diagnostics expose physical family and pending waiter state separately
from owner-side exact claims. Tests join these views by resource and family
without adding accepted exact mirrors back to the manager.

The `lock-table` workload now supports basic, nested-covered, convert, enqueue,
cancel-head, cancel-middle, cancel-tail, promote, handoff, and scope-close
scenarios with mode and width controls. Contended scenarios use explicit
counter observation and cancellation permits rather than timing sleeps.

## Implementation Notes

Implemented RFC-0027 Phase 3 with one shared physical entry per lock family, owner-local exact authority, bounded compatibility work, and deterministic structural observability.

- Shared accepted state now contains physical families only. Exact manager
  grants, owner scans, raw ownership helpers, duplicate repair, and
  `LockWaiterReleased` were removed.
- `PendingClaimGuard::accept()` consumes its pending token as the commit point.
  Guard Drop remains armed until manager and both owner-side indexes agree.
- Review expanded comments around family aggregation, lock acquisition, and
  every `LockTableScenario`; legacy test-only manager compatibility methods
  were removed when no production caller remained.
- The statement scope remains temporarily because this task preserved the
  existing admission handoff contract. The production audit and preferred
  transaction-owned replacement are recorded in backlog 000180.
- Waitable comparable same-scope upgrades remain deferred behind deadlock
  handling in backlog 000181. `SIX` remains a separate future decision.
- Poison-aware pending acquisition remains backlog 000179.
- A release-build review found `TrxAttachment::trx_id()` incorrectly gated by
  `debug_assertions`; removing that gate restored the standard release build.
- A review request to restore the old
  `table_scan_mvcc` released-waiter test was rejected as stale: the event,
  released phase, and external waiter-release API no longer exist. Current
  scan, cancellation, and provisional-cleanup tests passed.
- Candidate benchmarking exercised all ten scenarios across 26 valid
  configurations with zero failures and matching structural counters.
  Retained exact hits measured 74.9-98.9 ns/op, paired basic paths
  441.5-565.4 ns/op, width-eight covered nesting 6.92-7.21 us/op,
  conversion 541 ns/op, width-eight scope close 4.28-4.40 us/op, handoff
  905 ns/op, and width-eight queue lifecycles 230-326 us/op.
- Those timings were single candidate samples from an optimized build with
  debug assertions. Equivalent pre-cutover instrumentation was not preserved,
  so the planned repeated baseline/candidate median, IQR, and range comparison
  was not completed. Backlog 000182 retains that verification work.
- Resolve-time style audit passed for 26 branch-diff Rust files. Validation
  passed 1,688 workspace tests, 1,579 alternate-`libaio` storage tests,
  benchmark CLI execution, the standard release benchmark build, and 13
  focused scan/waiter regression tests.

## Impacts

- Lock compatibility scales with physical session families instead of exact
  scope claims.
- Covered nested claims and unchanged-mode releases avoid shared map entry,
  allocation, completion, event, and global atomic work.
- Queue cancellation no longer rebuilds the FIFO; provisional holders remain
  visible to compatibility before notification.
- Nested catalog writes use ordinary lock acquisition without a prepared
  bypass.
- Public users gain cumulative `LogicalLockStats`; benchmark output can include
  those counters.
- Public lock APIs and existing error classification remain otherwise
  compatible, except the internal obsolete `LockWaiterReleased` variant was
  removed.
- No persisted data, recovery, storage backend, MVCC, or isolation behavior
  changed.

## Test Cases

Completed deterministic coverage verifies:

1. Fixed counts and masks match held and provisional physical families.
2. Multiple exact scopes retain distinct claim numbers behind one physical
   family.
3. Exact coverage and covered publication perform no manager transition.
4. Immediate conversion preserves claim identity and rejects blocked or
   incomparable strengthening without mutation.
5. Mode-preserving release stays local; physical replacement/removal preserves
   FIFO promotion.
6. Indexed scope close visits exactly its accepted claims.
7. Head, middle, and tail cancellation preserve intrusive queue links.
8. Compatible FIFO prefixes become provisional before notification and are
   observed exactly once.
9. Dropping queued, provisional, immediate, or partially published guards
   leaves no family, claim, count, mask, or waiter leak.
10. Slab reuse advances generation and stale identities fail before mutation.
11. Nested DDL, maintenance, catalog writes, statement handoff, transaction
    completion, abandonment, teardown, and shutdown preserve release ordering.
12. Split diagnostics prove physical/exact agreement without shared exact
    mirrors.
13. Logical-lock statistics match known local, physical, queue, promotion, and
    scope-close work.
14. Benchmark option validation rejects invalid scenario/mode/width/control
    combinations.
15. Default and `libaio` suites, strict style/lint gates, release build, and
    benchmark CLI pass.

## Open Questions

No blocking Phase 3 design question remains.

- `docs/backlogs/000179-cancel-pending-logical-lock-acquisition-on-engine-poison.md`
- `docs/backlogs/000180-remove-statement-scope-logical-locks.md`
- `docs/backlogs/000181-waitable-comparable-same-scope-lock-upgrades.md`
- `docs/backlogs/000182-capture-lock-family-cutover-benchmark-comparison.md`
