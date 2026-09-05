---
id: 000296
title: Preallocate Catalog Lock Manager Slots
status: implemented
created: 2026-09-05
github_issue: 1046
---

# Task: Preallocate Catalog Lock Manager Slots

## Summary

The six built-in catalog tables now use six permanent lock-manager slots,
each containing independently synchronized and individually cache-padded
metadata and data states. These twelve resources bypass the outer resource
map; all other identities continue through the dynamic FastDashMap.

Physical-family arbitration, exact ownership, FIFO, cancellation, and
deferred notification retain their existing contracts. Idle catalog states
reuse family-map and waiter-slab capacity through 1024 entries or slots,
reclaiming oversized containers independently after complete drain.

## Context

Source Backlogs:
- docs/backlogs/closed/000190-preallocate-catalog-lock-manager-slots.md

Issue Labels:
- type:task
- priority:medium
- codex

This standalone follow-up implements work deferred from
docs/tasks/000294-managed-table-bindings-and-versioned-resolution.md.
RFC 0031 records the original deferral but is not this task's parent;
its completed phases remain unchanged. GitHub issue #1046 tracks this task.

The baseline, 6ef453dbf0f06a94c80630707b783a4b475be7f8, stored every resource
in a DashMap and removed drained entries. Frequent binding probes and final
resolution therefore paid outer-map lookup and insertion/removal costs even
for fixed catalog identities. Distributed user targets still share the
catalog binding resources, and full resolution also reads descriptor rows.

The catalog ID range includes unknown reserved identities. Fixed-array
routing must recognize the exact built-in set rather than relying on
TableID::is_catalog or the existing broad catalog_table_slot helper.

## Goals

- Route exactly six built-in table IDs to twelve independently synchronized,
  individually padded metadata/data resources.
- Share one canonical catalog layout and preserve dynamic fallback for every
  other identity, including unknown reserved catalog IDs.
- Use one resource-access boundary for every physical lifecycle transition
  while preserving arbitration and owner-side interfaces.
- Reuse bounded idle capacity without reclaiming queued or provisional state.
- Preserve active-resource accounting, generational waiter identity, and
  notification after synchronization releases.
- Exercise public narrow/full binding resolution in the benchmark framework
  and compare both stores with user-table lock controls.

## Non-Goals

- Changes to lock modes, family aggregation, FIFO, conversion policy,
  acquisition order, deadlock handling, or resource admission.
- Resource-owned handles, new dynamic eviction, global sharding changes,
  or unrelated statistics padding.
- Managed-definition or binding-key caching; backlog 000192 owns the
  current-state managed-definition cache.
- Changes to public storage APIs, numeric catalog IDs, persistence formats,
  redo, checkpoint/recovery, or component initialization.
- Preallocating family/waiter populations, retaining unbounded idle capacity,
  or adding a production backend selector for comparison.

## Rejected Alternatives

Resource-owned synchronization handles could also avoid user-resource
lookups, but would introduce creation, retirement, and lifetime contracts
across subsystems. The fixed built-in set did not require that redesign.

Managed-definition caching can avoid descriptor reads but requires coherent
DDL publication and recovery hydration. It remains an independent follow-up
in docs/backlogs/000192-cache-managed-table-definitions-in-current-catalog-state.md.

## Plan

### Canonical layout and routing

The pure catalog-storage layout owns the six existing IDs, their durable
order, and the count used by bootstrap and catalog-root descriptors. Existing
numeric values, broad arithmetic helpers, and re-export paths are preserved.

The separate builtin_catalog_table_slot helper performs checked subtraction,
bounds the 64-bit offset before narrowing, and verifies the selected identity.
Unknown reserved and user IDs return no fixed slot and use the dynamic store.
The layout depends only on IDs, so lock-manager construction needs no catalog
runtime or additional component initialization.

Each catalog table slot contains separate CachePadded wrappers around its
metadata and data Mutex<ResourceState>. Padding follows the library's
target-specific layout. The fallback map is named user and also stores
unknown reserved catalog resources.

### Resource transitions and memory lifetime

The synchronous with_resource boundary selects storage, acquires its guard,
runs one transition, and finishes accounting and cleanup. ResourceAccess
distinguishes create-if-absent acquisition from transitions requiring existing
active state. Missing required state remains an invariant failure.

Acquisition, observation, queued/provisional cancellation, fresh-grant
rollback, conversion, release, and FIFO promotion all use this boundary.
Arbitration remains shared; owner-local covered claims still bypass it.
No guard escapes the boundary or survives an await.

Complete drain requires no family entries, holder counts, grant-mask bits,
linked waiters, or live waiter nodes. A provisional grant remains active
after leaving the linked FIFO and pins its state until exact cleanup.

Fixed-resource activation and drain accounting finish under the cell mutex.
On drain, each container's reported capacity is compared independently with
the private limit of 1024. The map reports entry capacity; the waiter vector
reports slot capacity, including reserved but uninitialized space. Capacity
at or below the limit is retained. An oversized container is replaced under
the mutex and freed after unlocking. Growth rounding makes this a capacity
limit rather than a participant count or byte limit.

Retained slabs preserve free lists and generations across drains. Complete
drain permits an oversized slab to restart because the unique pending-token
contract leaves no live observer. Owner, claim, generation, mode, and phase
checks remain intact. Retained allocation does not make a resource active.

Dynamic entries preserve vacant-entry accounting and guarded remove_if
cleanup after the mutable guard releases. A racing acquisition can refill
an entry and prevent its removal.

### Notification and diagnostics

Promotion collects deferred notifications under resource synchronization.
Publication and the Drop/unwind fallback occur after the resource guard
releases, allowing a wake to re-enter the same resource safely.

Current and peak physical-resource statistics count active states across
both stores, not permanently allocated cells. Slab growth/reuse counters
continue to distinguish initialized-slot growth from vacant-slot reuse.
Statistics retain their relaxed snapshot contract.

Test diagnostics inspect one resource at a time, combine both stores in
canonical resource order, and omit idle fixed cells. No public diagnostic
API or storage interface was added.

### Binding benchmark

The typed managed-bindings-prepare fixture creates deterministic managed
tables with fixed-width binding keys and descriptors through public APIs.
The replay-safe resolve-table-binding workload calls public resolution with
include_full_schema selecting narrow identity/version or full definition.

Strict plan/runtime fixture validation rejects missing or duplicate
preparation, invalid counts/topology, and preparation in a measured role.
Each call contributes one latency sample; every result is validated against
the prepared identity, version, and expected definition. Validation and result
destruction are outside call latency but remain inside run duration.

The workload reuses session orchestration, cancellation, checked counters,
and measurement infrastructure, and verifies logical-lock drain after each
run. docs/benchmark-tool.md and the resolution template document usage.

## Implementation Notes

Implemented the hybrid catalog lock manager with exact built-in routing,
twelve independently padded resources, shared lifecycle access, and bounded
per-container retention. Public storage behavior and persistence formats
remain unchanged.

Review replaced unconditional empty-state reset with bounded reuse. This
preserves ordinary working allocations while still releasing burst capacity;
the source backlog's original reset direction is superseded by that accepted
policy. Provisional grants continue to prevent premature reclamation.

Final branch review verified lifecycle routing, release-before-notify ordering,
dynamic cleanup races, active-only diagnostics, and capacity/token invariants.
The implementation commit 09cbf4f matches the source snapshots used for the
completed test and benchmark validation.

Workspace validation passed all 1,928 tests, including 91 focused lock tests.
The mandatory resolution style gate passed for all 19 changed Rust files,
including formatting and strict workspace clippy. Focused coverage measured
99.02% in the lock manager and 92.35% in the waiter implementation. Earlier
captures for the unchanged layout and binding workload measured 97.22% and
85.40%, respectively. Alternate libaio validation was unnecessary because
no I/O paths changed.

The release comparison used identical benchmark sources on the preserved
DashMap baseline and final hybrid, with matching fixtures, engine settings,
and operation counts. Narrow/full resolution and user-lock controls covered
one and 64 targets at 1/1, 4/4, 8/8, and 4/16 workers/sessions. Fresh reversed-order
runs checked slower cases. Result, sample, counter, and drain checks passed.

Benchmark results, profiles, commands, and source/environment evidence remain
only under ignored target/task-000296/. The final local report is
target/task-000296/bounded-retention/report.md; the earlier report directory
describes unconditional reset. Measurements are not persisted in this task.

Review accepts expected hot-catalog contention during frequent uncached
resolution within this task's scope. Higher-layer callers may reuse versioned
definitions according to their cache-validity rules. Backlog 000192 retains
the cache work and requires comparison of both lock stores after it lands,
including the dynamic user-resource controls.

Source backlog 000190 is closed as implemented. Parent lookup confirms that
this standalone task requires no RFC phase synchronization.

## Impacts

- Catalog storage and file metadata share one authoritative built-in layout;
  existing IDs, bootstrap order, and six durable roots are preserved.
- The lock manager adds permanent padded cells and bounded idle allocations.
  Dynamic resources retain their existing eviction and arbitration contracts.
- The benchmark crate gains managed-binding fixtures, public resolution
  workloads, a semantic latency unit, and a runnable template.
- docs/lock-system.md records routing, drain, retention, accounting, and
  notification contracts; docs/benchmark-tool.md records workload usage.

## Test Cases

- Exact routing, all twelve resources, user/unknown reserved fallback, extreme
  IDs, six durable roots, independent cells, and sorted active-only snapshots.
- Compatibility, family coverage/rejection, conversion, FIFO admission and
  promotion, immediate rollback, partial publication, and exact-node cleanup
  exercised against both fixed and dynamic resources.
- Queued head/middle/tail cancellation, provisional observation/cancellation,
  release/cancel/reacquire races, and zero gauges after drain.
- Neither/either/both containers exceeding the retention limit, capacity
  exactly 1024, map growth rounding, reserved vector space versus initialized
  slots, generation/free-list reuse, and acquisition after reclamation.
- Reentrant notification on explicit publication, Drop, and unwind, proving
  synchronization releases before committed promotions are published.
- Existing managed-binding cancellation, DROP/rebind, DDL, recovery, and
  family regressions, plus benchmark fixture/role validation, repetition,
  exact samples/counters, malformed results, and cancellation/drain behavior.

## Open Questions

No unresolved correctness issue blocks this task. Current-state
managed-definition caching and the required benchmark revisit remain in
docs/backlogs/000192-cache-managed-table-definitions-in-current-catalog-state.md.
Binding-key lookup remains catalog-backed after descriptor caching; future
resource-handle or sharding redesign requires separate evidence and planning.
