# Backlog: Preallocate Lock Manager Slots For Built-In Catalog Resources

## Summary

Replace dynamic DashMap resource lookup for the six fixed built-in catalog tables with twelve preallocated, independently synchronized metadata and data resource slots, while retaining dynamic storage for user-table resources.

## Reference

Identified during docs/tasks/000294-managed-table-bindings-and-versioned-resolution.md. Binding probes and final resolution repeatedly acquire catalog metadata and data resources through doradb-storage/src/lock/mod.rs, where every physical transition currently performs a sharded DashMap lookup and empty resources are inserted and removed.

## Deferred From (Optional)

docs/tasks/000294-managed-table-bindings-and-versioned-resolution.md; docs/rfcs/0031-compact-numeric-catalog-table-definitions.md Phase 7

## Deferral Context (Optional)

- Defer Reason: This is a lock-manager representation and performance optimization rather than a correctness requirement for managed bindings. Including it in Phase 7 would broaden task 000294 across the shared lock core without benchmark evidence for the expected gain.
- Findings: The persisted catalog layout has six dense, stable table IDs, but each table contributes separate TableMetadata and TableData resources, requiring twelve slots rather than six. Fixed slots remove hashing, shard collisions, and resource insert/remove churn, but same-resource transitions must still serialize and each ResourceState still needs a dynamic family map and waiter queue. TableID::is_catalog covers the entire upper half of the ID space, and catalog_table_slot currently does not enforce the six-table upper bound, so fixed routing must recognize the exact built-in set. Permanent slots must reset their empty inner state to avoid retaining peak waiter allocations.
- Direction Hint: Prefer one independently locked cell per catalog lock resource plus the existing FastDashMap for dynamic resources; do not use one mutex for both resources of a catalog table. Reuse the catalog/storage table ID constants through one ordered layout, address the lock-core layering dependency explicitly, and route every manager lifecycle path consistently. Preserve publishing waiter notifications after releasing the resource lock and preserve active-resource rather than allocated-slot statistics. Benchmark managed binding resolution because the existing lock-table workload targets user tables and cannot validate this optimization.

## Scope Hint

Design and implement a hybrid LockManager resource store: route exactly the six existing catalog table IDs to twelve independently synchronized fixed ResourceState slots and route all other resources to the existing FastDashMap. Cover acquisition, observation, cancellation, conversion, release, FIFO promotion, deferred notification, statistics, and debug snapshots. Reset an empty fixed ResourceState so family-map and waiter-slab burst capacity is reclaimed. Keep catalog table IDs defined in catalog storage and avoid duplicating numeric IDs or an unchecked catalog-range assumption.

## Acceptance Hint

All twelve built-in catalog metadata and data resources use fixed slots without changing lock compatibility, family aggregation, FIFO, cancellation, or release behavior; unknown IDs in the reserved catalog range cannot index out of bounds; empty-slot memory, physical-resource statistics, and diagnostics preserve current semantics; existing lock tests pass; and repeated narrow and full managed-binding resolution benchmarks compare the hybrid implementation with the DashMap baseline and report throughput and latency under concurrency.

## Notes (Optional)

Do not assume preallocation removes contention among sessions touching the same catalog resource. Use benchmark evidence to distinguish reduced DashMap overhead and cross-resource shard contention from unavoidable per-resource serialization.

