# Backlog: Replace Arc-Probed Dropped-Table Runtime Purge

## Summary

Dropped-table GC currently treats successful Arc uniqueness acquisition as runtime-destruction readiness: purge detaches Arc<Table>, calls Arc::try_unwrap, restores the runtime on failure, and waits for a later wake. Replace that probe-and-restore protocol with an explicit executable-runtime lease gate and a uniquely owned dropped-runtime cleanup job so logical handles, active use, reclamation scheduling, and resource ownership are separate.

## Reference

docs/tasks/000240-operational-reclamation-recovery-validation.md; docs/rfcs/0024-versioned-metadata-immediate-retirement.md Phase 4; docs/rfcs/0017-drop-table-lifecycle-recovery.md Phase 4; docs/backlogs/000098-dropped-table-purge-retry-stall.md; doradb-storage/src/trx/purge.rs process_dropped_table_gc; doradb-storage/src/catalog/history.rs DroppedTableOperationalState; doradb-storage/src/table/mod.rs Table and destroy_dropped_runtime

## Deferred From (Optional)

docs/tasks/000240-operational-reclamation-recovery-validation.md; docs/rfcs/0024-versioned-metadata-immediate-retirement.md Phase 4

## Deferral Context (Optional)

- Defer Reason: Task 000240 is approved as validation of RFC 0024 existing Arc-based cleanup contract. Replacing that contract changes table-handle and runtime ownership, executable proof lifetimes, purge handoff, and recovery cleanup coordination, so it requires a separate RFC complexity evaluation rather than expanding Phase 4 implicitly.
- Findings: Arc::try_unwrap is race-free as a final uniqueness assertion but provides no readiness notification or eventual-progress guarantee. Current dropped-table GC speculatively changes Runtime to Floor, probes uniqueness, restores Runtime when stale handles exist, and then depends on a later purge wake. Backlog 000098 records the resulting stall risk but does not select a replacement ownership model. Public user operations use table ids and session caches are weak, yet crate-private Arc<Table> and Arc<TableRuntimeLayout> values still escape individual call frames. Table destruction is consuming, asynchronous, and fallible, while dropped-file deletion must remain independently gated by catalog checkpoint durability.
- Direction Hint: Prefer separating an Arc-backed lifecycle and identity shell from OwnedTableRuntime resources such as MemTable, ColumnStorage, current layout, and retired indexes. Enforce a TableRuntimeLease for every executable access, including layout and index proofs. Under metadata X, close lease acquisition, wait only for already-active leases, and move the owned bundle once into a horizon-gated purge job; stale shells remain terminal and resource-free. Keep Arc::try_unwrap only as a temporary invariant for nested Arcs during migration, not as scheduling. Avoid strong-count polling, unconditional self-wakes, retry backoff as the primary protocol, or fallible asynchronous destruction inside Drop. Future planning must apply the RFC complexity gate and decide whether RFC 0017 or RFC 0024 needs an amendment.

## Scope Hint

Design and implement a lightweight table lifecycle handle plus an owned runtime resource bundle. Require every executable table, layout, and index access to hold a drainable runtime lease. DROP metadata X closes lease admission, drains active operations without waiting for stale handle shells, and moves the owned runtime bundle once into purge. Purge retains the bundle until the strict transaction horizon, destroys it by value, then exposes the existing checkpoint-gated file floor. Preserve logical DROP latency, poison semantics, recovery behavior, and catalog checkpoint deletion safety. Evaluate whether retired-index cleanup can reuse the abstraction, but keep the first implementation focused on dropped-table runtime unless the RFC complexity gate requires a broader program.

## Acceptance Hint

Logical DROP TABLE never waits for stale non-executable handles, no executable lease can begin after the terminal gate, and stale handles cannot retain or access the detached runtime bundle. Eligible runtime destruction is scheduled by an authoritative state transition without Arc probe-and-restore, manual purge wakes, unrelated activity, or a busy retry loop. Destruction failures preserve fatal poison, the table file remains until catalog checkpoint safety, and restart reconstructs only the existing dropped floor. Deterministic tests cover active-lease drain, stale handles, automatic cleanup progress, shutdown ordering, destroy failure, checkpoint-gated file deletion, and recovery.

## Notes (Optional)

This item is intentionally separate from backlog 000098. Backlog 000098 tracks non-busy retry progress within the current Arc-owned design; this item proposes replacing Arc uniqueness as the readiness and ownership protocol. Future planning may supersede or narrow 000098 after the replacement lands.

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

## Close Reason

- Type: implemented
- Detail: Implemented by task 000241 through lock-drained holder discipline and one-way assertion-only dropped-runtime destruction.
- Closed By: backlog close
- Reference: docs/tasks/000241-assertion-only-dropped-table-runtime-cleanup.md
- Closed At: 2026-07-26
