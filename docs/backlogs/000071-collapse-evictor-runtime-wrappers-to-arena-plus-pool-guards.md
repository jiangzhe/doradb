# Backlog: Collapse evictor runtime wrappers to arena-plus-pool guards

## Summary

Track the private refactor to reduce EvictableRuntime and ReadonlyRuntime to the minimal two-field shape: an ArenaGuard for sweep/lock operations and a SyncQuiescentGuard to the owning pool, removing duplicated cached handle fields that can be reached from the pool directly.

## Reference

1. User analysis in task 000092 branch: EvictableRuntime and ReadonlyRuntime currently cache arena plus selected pool sub-handles even though the evictor thread can retain one arena guard and one pool keepalive. 2. Current implementation sites: doradb-storage/src/buffer/evict.rs and doradb-storage/src/buffer/readonly.rs. 3. Related but broader follow-up: docs/backlogs/000063-remove-internal-arc-state-from-readonly-and-evictable-pools.md.

## Scope Hint

Refactor the private evictor runtime structs for evictable and readonly pools to store only ArenaGuard plus SyncQuiescentGuard<Pool>; derive in_mem/residency, inflight IO state, stats, and IO client access from the retained pool handle at use sites; preserve the existing generic EvictionRuntime boundary and worker startup/shutdown contracts.

## Acceptance Hint

Both evictor runtime wrappers are reduced to the two-field design without behavior changes, worker lifetime safety still relies on quiescent ownership plus arena keepalive, and existing eviction/readonly tests continue to pass with no API changes outside private buffer internals.

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
