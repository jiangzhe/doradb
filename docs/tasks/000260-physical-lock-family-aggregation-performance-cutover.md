---
id: 000260
title: Physical Lock Family Aggregation and Performance Cutover
status: proposal
created: 2026-08-06
github_issue: 953
---

# Task: Physical Lock Family Aggregation and Performance Cutover

## Summary

Implement RFC-0027 Phase 3 by making one physical lock-family entry, rather
than every exact logical claim, the shared lock manager's unit of ownership.
Exact owner, scope, mode, and `ClaimNo` authority remains exclusively in the
session family's fixed owner-side indexes. Shared `ResourceState` retains only
physical family mode, fixed holder counts and mask, transient waiter identity,
and FIFO state.

Covered acquisitions, releases, and conversions whose family physical mode is
unchanged become bounded owner-local operations. Physical changes use one
guarded resource transition. Blocked first-family acquisitions retain the
Phase 2 generational waiter and provisional-grant protocol, extended with
token-exact rollback through every queued, promoted, locally staged, and
accepted-but-still-armed state.

After the representation proves equivalent behavior, remove exact manager
mirrors, exact-owner scans and repair paths, and the
`PreparedCatalogWriteAuthority` bypass. Complete the cutover with split
physical/exact diagnostics, stable logical-lock statistics, expanded
lock-table benchmark scenarios, and paired before/after performance evidence.

## Context

Issue Labels:
- type:task
- priority:high
- codex

Parent RFC:
- `docs/rfcs/0027-session-family-logical-lock-system-redesign.md`

RFC Relationship:
- Phase: 3, Physical Family Aggregation And Performance Cutover.
- This is the final implementation phase of RFC-0027.
- Phase 1 task:
  `docs/tasks/000258-linear-lock-family-authority-owner-side-indexes.md`.
- Phase 2 task:
  `docs/tasks/000259-tokenized-waiter-provisional-grant-lifecycle.md`.
- Prerequisite verification at design time passed the focused lock tests under
  both storage I/O configurations: 250 default-backend tests and 249
  `libaio`-backend tests.
- `$task-resolve` must synchronize the Phase 3 outcome into RFC-0027. RFC
  program resolution remains a separate `$rfc-resolve` action after all final
  gates pass.

Source Backlogs:
- `docs/backlogs/000171-exact-family-lock-system-redesign.md`

Related Backlogs:
- `docs/backlogs/000179-cancel-pending-logical-lock-acquisition-on-engine-poison.md`

Phase 1 established one move-only `FamilyLockAuthority`, authoritative
family/resource and exact-scope/resource indexes, family-local `ClaimNo`
allocation, exact scope cleanup, and the current family-coverage policy. Phase
2 replaced pointer waiter identity and queue rebuilding with a resource-local
generational slab, intrusive O(1) unlink, provisional promotion, and one
cancellation-safe `PendingClaimGuard`. Its migration-only `Released` phase is
removed in this final cutover because linear family authority leaves no
production path that can independently release a still-owned waiter.

The manager is still transitional. `ResourceState::granted` stores a
`Vec<GrantedLock>` containing every accepted exact owner and `ClaimNo`.
Acquisition, conversion, release, diagnostics, `owner_holds()`, and
`release_owner()` still find or remove exact entries. Every fresh covered
cross-scope claim therefore creates a shared exact mirror even though other
families observe only one unchanged physical mode.

The transitional representation also requires
`PreparedCatalogWriteAuthority` to bypass ordinary nested catalog statement
locking during accepted DDL. Once same-family covered claims are owner-local,
the nested catalog transaction can take its exact metadata and data claims
through the ordinary path without changing physical state.

The existing `doradb-bench lock-table` workload measures shared explicit
session or transaction locking with retained or paired release. It does not
isolate nested covered claims, conversion, enqueue, cancellation position,
promotion, handoff, or scope-close cardinality, and current public statistics
do not expose the structural work required to prove RFC-0027's complexity
bounds.

### Design Inputs

Documents:

- `docs/architecture.md`
- `docs/transaction-system.md`
- `docs/lock-system.md`
- `docs/benchmark-tool.md`
- `docs/rfcs/0016-multi-granularity-table-locking.md`
- `docs/rfcs/0027-session-family-logical-lock-system-redesign.md`
- `docs/tasks/000257-doradb-bench-lock-table-workload.md`
- `docs/tasks/000258-linear-lock-family-authority-owner-side-indexes.md`
- `docs/tasks/000259-tokenized-waiter-provisional-grant-lifecycle.md`

Primary code:

- `doradb-storage/src/lock/mod.rs`
- `doradb-storage/src/lock/claim.rs`
- `doradb-storage/src/lock/state.rs`
- `doradb-storage/src/lock/wait.rs`
- `doradb-storage/src/session.rs`
- `doradb-storage/src/trx/mod.rs`
- `doradb-storage/src/trx/stmt.rs`
- `doradb-storage/src/catalog/table.rs`
- `doradb-storage/src/catalog/index.rs`
- `doradb-bench/src/workload/lock.rs`

Accepted design decisions:

1. Retain the outer `FastDashMap<LockResource, ResourceState>` and cut over its
   value representation in place. Resource repartitioning is not justified by
   the current evidence.
2. Use a `u8` mode mask and `[u32; 4]` physical family counts. Do not introduce
   unsafe packing or an external waiter/container dependency.
3. Keep exact owner and `ClaimNo` data out of accepted shared holder state.
   Exact pending identity remains in `WaitNode` until provisional adoption.
4. Preserve current directional family coverage and immediate-only conversion
   semantics. Do not synthesize `SIX` or an artificial `X` join for
   incomparable `S` and `IX`.
5. Preserve one manager observation transition after a blocked wake. Stage
   owner-local publication under an armed rollback guard, validate and commit
   `Provisional -> Held` once, and perform no `await` inside that transfer.
6. Treat notification as a state-change wakeup only. Manager state and the
   move-only pending guard remain authoritative for adoption and cancellation.
7. Use a zero/one/many deferred-notification value because one manager
   transition can promote a maximal compatible FIFO prefix containing multiple
   waiters, while each wait node still has exactly one completion and one
   logical observer.
8. A caller that has polled a lock-acquisition future must eventually poll it
   again or drop it. Retaining it indefinitely without polling intentionally
   retains its queued request or provisional physical reservation. This task
   adds no timeout, lease, watchdog, or forced reclamation for a still-owned
   future.
9. Active engine-poison observation and original-fatal propagation remain
   backlog 000179. If a caller races acquisition against poison, choosing the
   poison branch must drop the acquisition future so `PendingClaimGuard`
   performs synchronous cleanup.

## Goals

1. Store exactly one accepted physical holder per
   `(LockResource, LockFamily)` and remove accepted exact-owner mirrors from
   shared resource state.
2. Make repeated exact-owner coverage, covered cross-scope insertion, unchanged
   physical-mode release, local DDL policy, and unchanged physical-mode
   conversion avoid shared lookup, allocation, event access, completion
   access, and global atomic work.
3. Replace grant-vector compatibility scans with fixed mode counts and a mode
   mask while preserving the RFC-0016 compatibility matrix and FIFO policy.
4. Preserve token-exact, ABA-safe acquisition and cancellation across
   immediate grant, queueing, provisional promotion, notification, local
   staging, observation, acceptance, caller-future Drop, and lifecycle release.
5. Keep scope close proportional to that scope's indexed claims and the
   physical changes and waiter promotions it actually causes.
6. Remove production exact-owner scans, duplicate-waiter repair, and global
   lifecycle cleanup fallbacks after deterministic authority proofs pass.
7. Route nested accepted-DDL catalog writes through ordinary exact transaction
   and statement claims, then remove `PreparedCatalogWriteAuthority` and its
   bypass branches.
8. Preserve DDL-versus-explicit-session policy, maintenance claims,
   statement-to-transaction handoff, transaction completion, abandonment, and
   shutdown release ordering.
9. Expose separate physical-manager and exact-owner diagnostics plus cumulative
   logical-lock statistics suitable for behavioral assertions and benchmark
   evidence.
10. Record paired baseline/candidate allocation, transition, contention,
    throughput, and latency-distribution evidence for every required lock
    operation class.
11. Update durable lock and benchmark documentation, including the explicit
    caller-progress contract for pending acquisition futures.

## Non-Goals

1. No blocking conversion, conversion queue, deadlock detection, victim
   policy, timeout, lease, lock escalation, or `SIX` mode.
2. No parallel mutation inside one session family, family mutex, actor,
   cleanup coordinator, or weak-lock fast path.
3. No forced reclamation of a pending future that remains owned but is never
   polled or dropped.
4. No poison-aware lock wait or change to fatal-error propagation; that remains
   backlog 000179.
5. No resource-map sharding or partition redesign, new lock-free map, external
   slab dependency, unsafe waiter storage, or unsafe holder packing.
6. No engine-global claim sequence, per-covered-operation global atomic
   counter, or exact allocator-hook accounting.
7. No public SQL semantics, isolation, MVCC, recovery, persisted format,
   catalog format, or storage I/O behavior change.
8. No hard numeric performance-regression threshold. Structural gates are
   mandatory; measured tradeoffs must be recorded and explained.
9. No GitHub issue creation, commit, push, or RFC status mutation as part of
   task creation.

## Plan

### 1. Replace exact manager grants with physical family state

Refactor shared state toward the following semantic representation:

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

`MODE_COUNT` remains four. `ModeMask` uses a `u8`; physical counts use checked
`u32` increments/decrements. A bit is set exactly when the corresponding count
is nonzero. `Held` and `Provisional` contribute one physical family to the
count for their mode; `Queued` contributes no holder. Compatibility excludes
the requesting family and inspects only the fixed counts/mask.

The semantic family states are exclusive:

- `Queued` has a linked node and no physical holder.
- `Provisional` has a detached provisional node and an installed physical
  holder that the pending guard has not yet transferred to exact local state.
- `Held` represents all accepted exact claims in the family and retains no
  waiter-node identity.

`ResourceState::is_empty()` requires an empty family map, zero holder counts
and mask, no linked queue node, and `wait_queue.live_count() == 0`. A detached
`Provisional` node therefore pins the resource and slab until its unique
observer or dropping guard consumes it.

Remove accepted owner, exact scope, accepted `ClaimNo`, DDL purpose, and exact
claim count from shared holder state. Keep owner, pending `ClaimNo`, requested
mode, completion, and phase in `WaitNode`, where they are required for
token-exact pending observation and cancellation.

### 2. Make fixed owner-side state the exact authority

Retain `FamilyLockState.resources` and `LockScopeState.claims` as the two
authoritative accepted indexes. Replace variable exact family storage with
fixed scope-class slots for the at-most-one live session-explicit, operation,
transaction, and statement claims allowed by linear family ownership.

Each live `LocalFamilyResourceState` retains:

- the occupied fixed exact slots containing `ClaimNo` and `LockMode`;
- a compact exact-claim mode mask; and
- the strongest actual occupied covering mode.

The covering mode must be one of the exact held modes. Do not construct a
synthetic lattice join. In particular, `Shared` and `IntentExclusive` remain
incomparable for table-data resources.

Introduce a small mutation-plan or equivalent staged calculation that:

1. validates exact scope and `ClaimNo`;
2. applies the existing directional family-coverage rule;
3. computes old and candidate local masks and covering modes;
4. identifies whether the physical mode is unchanged, strengthened,
   downgraded, inserted, or removed; and
5. carries enough token-exact information to commit or roll back both local
   indexes without rescanning unrelated families or manager resources.

At most four fixed slots may be inspected. A covered path must not enter
`LockManager`.

### 3. Split acquisition into local and first-physical paths

For an existing exact scope claim:

- If its held mode covers the request, return `LockGrant::Existing` entirely
  locally without changing the stored mode.
- If the requested mode covers the held mode, use the conversion path in
  section 5.
- If neither covers the other, return `LockConversionNotSupported`.

For a fresh exact scope claim:

1. Reserve the next family-local `ClaimNo` before any policy failure, manager
   transition, enqueue, or cancellation. Failed attempts burn the number.
2. Validate that every other exact claim in the family covers the requested
   mode.
3. If the family already has a physical covering mode, publish the new exact
   claim into both local indexes under rollback and return without consulting
   the manager or external FIFO queue.
4. Otherwise enter the manager once. Grant immediately only when externally
   compatible and FIFO permits; otherwise allocate one completion and one
   generational waiter node and install `PhysicalFamilyState::Queued`.

Immediate physical acquisition allocates no waiter or completion. Its
call-local pending guard owns the newly installed physical state until exact
publication succeeds. If publication unwinds, guard Drop removes the physical
family, updates counts/mask, and runs promotion before family authority can be
returned.

### 4. Make pending ownership and rollback explicit

Extend `PendingClaimGuard` state so exactly one synchronous cleanup owner exists
until acceptance is complete. Exact type and variant names may follow the
implementation, but the following semantic phases and cleanup behavior are
required:

| Guard/manager phase | Required guard cleanup |
|---|---|
| Reserved but not enqueued | Burn the pending identity; no shared mutation |
| `Queued(node_id)` | Validate token and generation, unlink/reclaim in O(1), and rerun promotion |
| `Provisional(node_id)` | Remove the physical family, decrement counts/mask, reclaim the node, and rerun promotion |
| Immediate physical grant still guard-owned | Remove any staged local record and release the physical family |
| Provisional plus partial/full local staging | Remove only token-matching family/scope records, then remove the provisional physical family |
| `Held` plus accepted local state but guard still armed | Release the newly accepted claim through ordinary family-mode recomputation |
| Disarmed | Perform no cleanup; exact scope ownership has accepted the claim |

All manager-side cleanup first validates resource, family, node generation,
pending owner, `ClaimNo`, and mode as applicable. Stale or mismatched identity
is an invariant failure before mutation. Cleanup that reduces blocking runs the
maximal FIFO-prefix grant loop.

Future Drop is the final observer and reclaims its queued or provisional node
in the same transition. The acquisition future exclusively borrows family
mutation authority, so lifecycle cleanup cannot independently release the
pending request while its observer remains live.

Document the caller-progress contract in `docs/lock-system.md` and adjacent
pending-acquisition rustdoc:

> After first poll, the caller must eventually continue polling the
> acquisition future or drop it. Retaining the future indefinitely without
> polling intentionally retains its queued request or provisional physical
> reservation and may block other acquisitions. No timeout, lease, watchdog,
> or background reclamation is provided.

### 5. Preserve FIFO promotion and defer zero/one/many notifications

Under one resource guard, promotion repeatedly examines the queue head and
installs the maximal compatible FIFO prefix. For each promoted node:

1. validate queue links and pending identity;
2. detach `Queued -> Provisional`;
3. change the matching family entry `Queued -> Provisional`;
4. increment its physical mode count and mask; and
5. retain its completion for post-lock publication.

Stop at the first incompatible head; never skip it. Provisional families fully
participate in compatibility before any waiter is notified.

Replace unconditional notification-vector allocation with a small deferred
value:

```rust
enum DeferredNotifications {
    None,
    One(Arc<Completion<()>>),
    Many(Vec<Arc<Completion<()>>>),
}
```

This is not a synchronization guard and does not combine observers. Each wait
node still owns one completion for one logical waiter. The deferred value
collects notifications produced by one release, downgrade, cancellation, or
promotion transition, which may promote multiple compatible waiters.

The manager transition must finish and release resource synchronization before
publishing any completion. Make the completed transition's deferred value
must-publish, with a Drop fallback or equivalent structure so an early return
or unwind after a committed promotion cannot silently discard its wakeups.
Notification remains an independent state-change signal; authoritative
manager state must contain the matching `Provisional` family and node.

After a notification, the waiter:

1. keeps the pending token and node ID armed;
2. stages exact family and scope publication under token-exact rollback;
3. performs no `await` between staging and commit;
4. re-enters the manager once and validates resource, family, node generation,
   owner, `ClaimNo`, requested mode, node phase, and matching provisional
   family state;
5. changes the matching `Provisional` family state to `Held`, reclaims the
   node, converts the pending token to its accepted token, and disarms only
   after both exact indexes own the claim.

A client may leave the notified future unpolled. In that case
`Provisional` remains a valid counted physical holder and exact local state is
not published. Later polling adopts it; later Drop reverses it. The manager
must never infer abandonment merely from delayed observation.

### 6. Preserve conversion semantics while aggregating physical changes

Keep the current directional conversion contract:

- Held covers requested: local existing hit; retain the stronger held mode.
- Requested covers held: attempt an immediate conversion.
- Neither covers the other: return `LockConversionNotSupported`.

Before staging a conversion, require every other exact scope in the family to
cover the requested mode. This preserves current `LockFamilyConflict`
behavior; Phase 3 must not broaden family semantics merely because a stronger
synthetic aggregate could cover weaker siblings.

Successful conversion retains `ClaimNo`. If the candidate exact update leaves
the family physical mode unchanged, commit both owner-side indexes locally.
For example, upgrading one `IS` claim to `IX` is local when another exact
family claim already holds `IX`.

If the physical mode strengthens, perform one manager transition. Validate the
expected old family mode, require an empty FIFO queue, and require external
compatibility with the stronger mode. Failure returns
`LockUpgradeWouldBlock` and restores the exact old mode. Conversion never
enqueues or waits.

Supported candidate upgrades remain:

- table metadata: `S -> X`;
- table data: `IS -> IX`, `IS -> S`, `IS -> X`, `IX -> X`, and `S -> X`.

`IX -> S` and `S -> IX` remain unsupported without `SIX`.

### 7. Make release, handoff, and scope close proportional

Release first validates the exact token and computes the remaining fixed-slot
mask and covering mode under family authority.

- If the physical mode is unchanged, remove the exact family and scope entries
  locally and do not run the shared grant loop.
- If the physical mode downgrades or disappears, stage the owner-side removal,
  perform one checked manager replacement/removal, update counts/mask, run the
  maximal compatible FIFO-prefix promotion, then commit or roll back the local
  plan.

Statement-to-transaction metadata handoff must insert the destination
transaction claim before releasing the statement claim. Both changes may be
local, but their order remains a correctness requirement.

Scope close visits only `LockScopeState.claims`. For `H_scope` claims, `B`
physical changes, and `P` resulting promotions, the target work remains
`O(H_scope + B * MODE_COUNT + P * MODE_COUNT)` average. It must not scan the
manager's resource map, unrelated physical families, or unrelated exact
scopes.

### 8. Remove catalog prepared-lock bypasses

Make DDL-versus-explicit-session policy consult the family-local
`SessionExplicit` slots for both metadata and data resources before creating a
DDL operation claim. Keep policy separate from purpose-independent shared
compatibility.

Accepted DDL retains its operation claims. Nested private catalog transactions
and statements then acquire ordinary exact claims:

- transaction metadata `Shared`;
- transaction data `IntentExclusive`; and
- the existing exact statement claims needed by catalog row/index work.

Those claims are normally covered by the DDL operation's physical mode and
therefore publish locally without manager access.

After behavioral tests prove this path:

1. remove `PreparedCatalogWriteAuthority`;
2. remove prepared-catalog fields and constructors from `TrxRuntime` and
   statement facades;
3. turn `stage_prepared_catalog_statement` call sites into the ordinary private
   catalog statement wrapper, renaming the helper to describe behavior rather
   than bypass authority;
4. remove catalog table/index authority parameters and session authority
   constructors; and
5. require catalog insert/delete paths to use ordinary transaction metadata
   and data claims.

Maintenance continues to record its own exact operation claims even when a
stronger session-explicit claim covers them. Releasing maintenance must never
consume the explicit claim.

### 9. Remove exact manager APIs and migration repairs

After physical and lifecycle proof gates pass, delete:

- `GrantedLock` and `ResourceState::granted`;
- exact manager grant lookup and accepted exact-claim conversion/release APIs;
- production `LockManager::owner_holds()`;
- production `LockManager::release_owner()` and resource-global owner scans;
- raw manager-only test acquisition, release, query, and repair APIs;
- the migration-only released-waiter phase and error;
- duplicate exact waiter/grant repair retained for migration; and
- assertions requiring one exact manager mirror per accepted logical claim.

Replace `TrxRuntime` write-authority debug proof with a borrow of the
authoritative `TransactionLockState` or an equally direct owner-local proof.
Engine and lock tests that currently call `owner_holds()` must use split
physical/exact diagnostics or the exact transaction/family authority.

An optional test-only full diagnostic scan may remain, but it cannot be a
production lifecycle operation or a dependency of ordinary cleanup.

### 10. Split diagnostics and expose stable logical-lock statistics

Manager diagnostics report only:

- resource and physical family;
- held, queued, or provisional state and physical mode;
- fixed counts and mask;
- FIFO order;
- waiter slot/generation, pending identity, and node phase; and
- slab slot, free-list, capacity, and live counts.

Family diagnostics report:

- exact owner and scope;
- resource;
- accepted `ClaimNo` and exact mode;
- fixed-slot occupancy, mask, and covering mode; and
- accepted resource sets per scope.

Tests may join the snapshots by `(resource, family)` to prove agreement, but
shared production state must not retain exact claims merely for diagnostics.

Add a public cumulative `LogicalLockStats` snapshot and a poison-tolerant
`Session` diagnostic accessor consistent with existing transaction, storage
I/O, buffer-pool, and mandatory-runtime statistics. Include stable semantic
counters for:

- owner-local exact covered hits;
- owner-local covered cross-scope publications;
- owner-local mode-preserving conversions and releases;
- resource transitions and fixed mode slots examined;
- immediate physical acquisitions and upgrades;
- enqueue, O(1) queue-link mutation, cancellation by position, provisional
  observation, and promoted waiters;
- scope-close claims visited and physical changes;
- completion allocations, waiter-slab growth/reuse, and relevant representation
  growth classes; and
- current/peak physical resources, families, linked waiters, and live waiter
  nodes where safely observable.

Shared manager counters may use relaxed atomics only after a path has already
entered shared manager state. Owner-local paths update plain family counters
and aggregate them once when final session family authority closes, avoiding a
global atomic on covered operations. Allocation counters describe explicit
lock-system allocation classes, not exact process allocator calls.

### 11. Expand `doradb-bench lock-table`

Extend the workload with explicit scenario and physical-mode controls while
retaining the existing basic scope/unlock/random behavior:

```text
--scenario basic
           |nested-covered
           |convert
           |enqueue
           |cancel-head
           |cancel-middle
           |cancel-tail
           |promote
           |handoff
           |scope-close
--mode shared|exclusive
--width N
```

`basic` remains the default and maps existing `--scope`, `--unlock`, `--rand`,
and `--seed` semantics. Validate and reject irrelevant combinations for
specialized scenarios. `width` controls the relevant resource, exact-scope,
waiter, promotion-prefix, or scope-close cardinality rather than silently
changing meanings between runs.

Contended scenarios must use deterministic barriers/events and explicit
permits:

- install a known blocker;
- admit waiters one at a time;
- confirm enqueue through monotonic stats or test/benchmark hooks;
- cancel or release the intended participant;
- verify the exact head/middle/tail or promotion order; and
- avoid timing sleeps as an ordering mechanism.

One operation count represents one completed scenario lifecycle. Do not add a
per-operation wall-clock timer to the hot loop. Continue reporting aggregate
throughput and average latency; obtain latency distribution from repeated
paired runs using median, interquartile range, and full range.

Add logical-lock statistics to `--include-stats` output and document exact
scenario semantics in `docs/benchmark-tool.md`.

### 12. Capture baseline before deleting the transitional representation

Add stable scenario coverage and temporary baseline instrumentation before the
physical representation cutover. Build and run an `origin/main` or preserved
pre-cutover baseline binary and the final candidate with identical release
profile, prepared roots, operation counts, threads, sessions, modes, widths,
seeds, storage backend, and host conditions.

The required matrix covers:

- repeated retained exact hits;
- nested covered exact claims;
- first-family shared and exclusive acquisition;
- immediate conversion;
- conflict/enqueue;
- head, middle, and tail cancellation;
- release with zero, one, and many promotions;
- statement-to-transaction handoff; and
- scope close at multiple cardinalities.

For each operation class, record:

- throughput and average operation latency;
- repeated-run median, interquartile range, and range;
- resource transitions and mode slots examined;
- queue link changes and promoted waiters;
- completion allocation and waiter-slab growth/reuse;
- physical resource/family and exact claim cardinalities; and
- a concise explanation of regressions, improvements, or measurement noise.

No hard numeric budget is imposed, but unexplained regression or a structural
gate violation blocks cutover.

### 13. Update durable documentation and resolve traceability

Update `docs/lock-system.md` so it no longer describes accepted resource-side
exact grants. Document physical family aggregation, fixed compatibility state,
owner-local exact authority, immediate-only conversion, pending guard cleanup,
deferred zero/one/many notifications, the caller-progress contract, and
physical/exact diagnostics.

Update `docs/transaction-system.md` for ordinary nested catalog lock
acquisition after removal of prepared authority. Update
`docs/benchmark-tool.md` for new scenarios, controls, stats, and paired
measurement procedure.

During `$task-resolve`:

1. record the actual representation, code changes, tests, review findings, and
   benchmark evidence in `Implementation Notes`;
2. synchronize the Phase 3 task path, issue, status, and implementation summary
   into RFC-0027;
3. close source backlog 000171 as implemented if no unresolved requirement
   remains;
4. leave related backlog 000179 open unless independently implemented; and
5. run the RFC completion-readiness checks before a later `$rfc-resolve`.

## Acceptance Criteria

1. Accepted shared holder state contains one physical entry per
   `(resource, family)` and no accepted exact owner or `ClaimNo`.
2. Fixed counts/mask exactly match held and provisional physical families in
   deterministic and randomized tests.
3. Repeated exact coverage, covered cross-scope insertion, unchanged-mode
   conversion/release, and local DDL policy perform no manager transition,
   allocation, event/completion access, or global atomic operation.
4. First physical acquisition, physical conversion, downgrade, removal,
   enqueue, cancellation, provisional observation, and promotion meet the
   RFC-0027 target complexity bounds.
5. FIFO-prefix behavior and current compatibility, family coverage, conversion,
   `ClaimNo`, and DDL policy semantics are unchanged.
6. Dropping a future in every queued, provisional, immediate,
   partially published, and accepted-but-armed phase leaves no leaked exact
   claim, physical family, count/mask bit, or waiter node and promotes later
   waiters correctly.
7. A retained but unpolled queued or provisional future remains a valid
   blocker until repolled or dropped, as explicitly documented.
8. Immediate acquisitions allocate no completion or waiter slot. Blocked
   acquisitions reuse waiter slots before vector growth.
9. `PreparedCatalogWriteAuthority`, exact manager mirrors, manager-only test
   ownership APIs, `owner_holds()`, `release_owner()`, released-waiter state,
   duplicate repair, and obsolete global scans are absent.
10. Nested table/index DDL, maintenance, catalog writes, statement handoff,
    transaction completion, abandonment, and shutdown preserve exact release
    ordering and leave all family state empty.
11. Split diagnostics and `LogicalLockStats` prove physical/exact agreement
    without reintroducing exact shared mirrors or local-path global atomics.
12. Expanded benchmark scenarios are deterministic, documented, and accompanied
    by paired baseline/candidate evidence for every required operation class.
13. Focused, workspace, strict lint, documentation, benchmark CLI, and
    alternate-`libaio` validations pass.

## Implementation Notes

## Impacts

Primary storage implementation:

- `doradb-storage/src/lock/mod.rs`
  - `LockManager`, `ResourceState`, compatibility, conversion, release,
    promotion, diagnostics, and obsolete exact-manager APIs.
- `doradb-storage/src/lock/claim.rs`
  - fixed exact scope slots, masks, covering-mode recomputation, and staged
    local mutation.
- `doradb-storage/src/lock/state.rs`
  - family/scope authority, acquisition, conversion, release, close,
    handoff, rollback, and local statistics.
- `doradb-storage/src/lock/wait.rs`
  - pending guard phases, provisional adoption, cancellation, waiter
    retention, and `DeferredNotifications`.
- `doradb-storage/src/session.rs`
  - DDL/maintenance authority, logical-lock statistics accessor, and lifecycle
    proofs.
- `doradb-storage/src/trx/mod.rs`
  - transaction lock proof, ordinary catalog statement staging, and removal of
    `PreparedCatalogWriteAuthority`.
- `doradb-storage/src/trx/stmt.rs`
  - ordinary catalog statement facade and statement/transaction claim
    handoff.
- `doradb-storage/src/catalog/table.rs`
- `doradb-storage/src/catalog/index.rs`
  - removal of prepared authority parameters and ordinary nested catalog lock
    acquisition.
- `doradb-storage/src/engine.rs`
  - diagnostics/tests that currently depend on exact manager `owner_holds()`.
- `doradb-storage/src/lib.rs`
  - public `LogicalLockStats` export if kept with the crate's public statistics
    types.

Benchmark implementation:

- `doradb-bench/src/cli.rs`
- `doradb-bench/src/workload/lock.rs`
- `doradb-bench/src/output.rs`
- related benchmark configuration, manifest, and output tests.

Documentation:

- `docs/lock-system.md`
- `docs/transaction-system.md`
- `docs/benchmark-tool.md`
- this task document during `$task-resolve`
- RFC-0027 phase metadata during `$task-resolve`

No unsafe code, persisted data, recovery format, or storage backend behavior is
expected to change.

## Test Cases

### Representation and local authority

1. Empty resource state has zero counts/mask, no families, no linked nodes, and
   zero live waiter nodes.
2. Held and provisional insertion/removal update exactly one physical family
   count and mask bit; queued families do not affect counts.
3. Multiple exact scopes in one family produce one physical holder and retain
   distinct `ClaimNo`s in both owner-side indexes.
4. Repeated exact coverage and covered cross-scope insertion increment only
   plain local statistics and perform no manager/event/allocation work.
5. `S` and `IX` remain incomparable; no synthetic `SIX` or `X` aggregate is
   created.
6. Randomized sequential reference-model traces compare acquisition,
   conversion, release, unlock/reacquire, handoff, and scope close against the
   fixed optimized state.

### Conversion, release, and close

7. Every supported immediate upgrade succeeds only with an empty queue and
   external compatibility, retains `ClaimNo`, and updates the physical family
   only when its maximum changes.
8. Incomparable conversion returns `LockConversionNotSupported`; a blocked
   strengthening returns `LockUpgradeWouldBlock`; both retain exact and
   physical old modes.
9. Same-family sibling coverage rejection preserves current
   `LockFamilyConflict` behavior.
10. Mode-preserving release is local; physical downgrade/removal updates
    counts/mask and promotes the maximal compatible FIFO prefix.
11. Scope close visits exactly its indexed resources and never invokes a
    manager-global owner/resource scan.

### Wait, notification, cancellation, and resource lifetime

12. Immediate grant allocates no completion or waiter node.
13. Promotion installs matching provisional family and node phases before
    notification and counts the physical holder for compatibility.
14. One release can produce zero, one, or multiple deferred notifications;
    each promoted wait node receives exactly one completion after the resource
    guard is released.
15. Head, middle, and tail queued cancellation unlink in O(1), preserve links,
    and promote the correct later prefix.
16. Future Drop racing promotion cleans either queued or provisional state
    according to the resource-guard linearization order.
17. Future Drop after notification but before observation removes the
    provisional family, counts/mask, and waiter node.
18. No separate lifecycle or test-only manager path can release a pending
    request while its acquisition future owns family mutation authority.
19. Injected unwind after the first local index, after both local indexes, and
    after physical commit but before disarm performs token-exact rollback and
    preserves any preexisting claim.
20. A promoted future deliberately retained without polling stays
    provisional, continues blocking incompatible requests, then either adopts
    on repoll or cleans synchronously on Drop.
21. Resource removal is rejected while any queued or provisional node is
    live. Consumption permits removal/recreation, and stale
    slot/generation identities fail before mutation.
22. Waiter slab reuse precedes growth and generation advance prevents ABA.

### DDL, catalog, transaction, and lifecycle behavior

23. Explicit session metadata or data claims reject table DDL through the
    family-local policy without a manager exact lookup.
24. Nested CREATE/DROP TABLE and CREATE/DROP INDEX catalog transactions acquire
    ordinary exact metadata/data claims locally under DDL operation coverage.
25. Failure rollback releases only fresh nested claims and never consumes
    preexisting explicit or operation claims.
26. Maintenance records and releases its exact operation claims independently
    of covering session-explicit claims.
27. Statement-to-transaction handoff publishes the transaction claim before
    releasing the statement claim and preserves continuous physical coverage.
28. Commit, rollback, statement cancellation, public transaction abandonment,
    mandatory DDL/maintenance failure, session close, and engine shutdown leave
    no exact, held, queued, or provisional state.
29. Runtime write-authority assertions use the authoritative transaction state
    after `owner_holds()` removal.

### Diagnostics, statistics, benchmark, and validation

30. Physical and exact snapshots join to one family/resource agreement while
    neither view contains data owned exclusively by the other.
31. Logical-lock statistic deltas match deterministic scenario work, remain
    visible through poison-tolerant diagnostics where intended, and do not add
    shared atomics to local-only paths.
32. Benchmark CLI defaults preserve current `lock-table` behavior and reject
    invalid scenario/mode/width/control combinations.
33. Deterministic enqueue, cancellation, promotion, handoff, and scope-close
    scenarios report the expected operation count and lock-stat deltas without
    timing sleeps.
34. Paired baseline/candidate release runs record the required throughput,
    latency distribution, allocation-class, transition, queue, and cardinality
    evidence.

Validation commands:

```bash
rtk cargo fmt --all -- --check
rtk cargo clippy --workspace --all-targets -- -D warnings
rtk cargo nextest run -p doradb-storage lock
rtk cargo nextest run --workspace
rtk cargo nextest run -p doradb-storage --no-default-features --features libaio
rtk cargo run -p doradb-bench -- --help
```

Run the documented release-profile benchmark matrix separately for the
preserved baseline and final candidate.

## Open Questions

No design-blocking questions remain.

The exact safe byte packing of fixed slots, retained waiter-slab capacity, and
zero/one/many deferred-notification representation may be tuned from layout
and benchmark evidence, provided the semantic states, cancellation guarantees,
and structural performance gates above remain unchanged.

Active engine-poison cancellation remains
`docs/backlogs/000179-cancel-pending-logical-lock-acquisition-on-engine-poison.md`.
This task guarantees cleanup when the acquisition future is dropped; it does
not make the lock wait independently observe poison.
