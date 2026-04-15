# Backlog: Optimize secondary index access path for dual-tree indexes

## Summary

Revisit the secondary-index access path now that user-table indexes are dual-tree; the current UniqueIndex and NonUniqueIndex interfaces were shaped around a single mutable tree and may force unnecessary DiskTree opens, scans, merges, or guard construction. In particular, evaluate whether callers should provide disk pool guards for operations that can touch DiskTree instead of having dual-tree indexes construct those guards internally.

## Reference

Current task discussion; docs/tasks/000119-composite-secondary-index-core.md; docs/rfcs/0014-dual-tree-secondary-index.md; docs/secondary-index.md; doradb-storage/src/index/unique_index.rs; doradb-storage/src/index/non_unique_index.rs; doradb-storage/src/index/composite_secondary_index.rs; table/trx call sites that already pass PoolGuards for other storage access.

## Deferred From (Optional)

docs/tasks/000119-composite-secondary-index-core.md; docs/rfcs/0014-dual-tree-secondary-index.md

## Deferral Context (Optional)

- Defer Reason: Task 000119 is focused on making the composite secondary-index core correct under the existing trait surface; broader access-path optimization may require interface or call-site changes and should be planned separately.
- Findings: The current UniqueIndex and NonUniqueIndex traits expose single-tree-style operations. DualTreeUniqueIndex and DualTreeNonUniqueIndex must internally decide when to consult MemTree, DiskTree, or both, which can lead to extra DiskTree opens/lookups and makes cleanup-only behavior easy to mis-specify. DualTree methods currently receive an index pool guard through the existing traits, but DiskTree access also needs a disk pool guard; constructing that guard inside SecondaryDiskTreeRuntime is opposite to the existing client-passes-guards pattern used by table and transaction code. Some decisions may be better expressed as dual-tree-specific APIs or by changing internal behavior behind the existing table-facing surface.
- Direction Hint: Start from table/trx call sites and classify operations by intent: read lookup, duplicate check, foreground logical delete, compare-exchange claim, rollback cleanup, purge cleanup, and scan. Prefer an access contract that makes cold-layer reads explicit only where they are semantically required. Evaluate whether to change trait signatures to carry caller-owned PoolGuards or explicit disk PoolGuard references, or preserve the public traits while adding internal dual-tree fast paths if that limits churn.

## Scope Hint

Evaluate whether to change the index traits, add dual-tree-specific internal methods, batch or cache DiskTree opens, pass caller-owned disk pool guards, and avoid cold-layer reads for cleanup-only paths while preserving table/trx call-site semantics. Include UniqueIndex and NonUniqueIndex signatures, DualTreeUniqueIndex and DualTreeNonUniqueIndex internals, SecondaryDiskTreeRuntime open paths, and table/trx call sites.

## Acceptance Hint

A future task identifies and implements a cleaner dual-tree access contract or internal fast path, removes unnecessary cold-layer work from common lookup/delete/update paths, and includes benchmarks or targeted counters/tests showing fewer DiskTree opens/scans for representative secondary-index operations. Operations that may consult DiskTree should receive or derive the needed caller-provided disk pool guard from operation context, with internal ad-hoc disk_pool_guard creation removed or limited to clearly justified helper boundaries.

## Notes (Optional)


## Close Reason (Added When Closed)

When a backlog item is moved to `docs/backlogs/closed/`, append:

```md
## Close Reason

- Type: <implemented|stale|replaced|duplicate|wontfix|already-implemented|other>
- Detail: <reason detail>
- Closed By: <backlog close>
- Reference: <task/issue/pr reference>
- Closed At: <YYYY-MM-DD>
```
