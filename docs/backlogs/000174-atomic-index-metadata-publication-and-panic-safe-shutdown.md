# Backlog: Make index metadata publication atomic and component shutdown panic-safe

## Summary

Index create/drop installs a new table runtime layout before publishing the
pointer-identical catalog-history metadata. Concurrent metadata-history purge
can observe the transient old-history/new-layout combination and panic.
Separately, a worker panic during component shutdown can leave the registry
marked as shut down before later worker hooks run, causing owner drop to wait
forever on quiescent guards retained by those workers.

## Reference

Discovered while validating
`docs/tasks/000246-session-operation-coordinator-foundation.md` against
`docs/rfcs/0025-session-coordinated-cancellation-cleanup-ownership.md`.

The workspace test
`trx::admission::tests::stale_write_first_rejects_both_index_kinds_before_locks_or_binding`
timed out after `Purge-Dispatcher` panicked in
`catalog/history.rs::assert_current_layout_metadata`.

Relevant paths:

- `doradb-storage/src/catalog/index.rs`: create/drop index install the runtime
  layout and then publish catalog history in separate critical sections.
- `doradb-storage/src/catalog/mod.rs`: metadata-history purge independently
  acquires each user-table entry.
- `doradb-storage/src/catalog/history.rs`: purge validation requires the
  current history metadata and installed runtime layout to be pointer-identical.
- `doradb-storage/src/component.rs` and `doradb-storage/src/trx/sys.rs`: a
  purge-worker join panic aborts reverse shutdown after the registry-wide
  shutdown flag is set.

## Deferred From (Optional)

`docs/tasks/000246-session-operation-coordinator-foundation.md`;
`docs/rfcs/0025-session-coordinated-cancellation-cleanup-ownership.md`

## Deferral Context (Optional)

- Defer Reason:
  The defects were exposed while validating session-operation lifecycle work,
  but their synchronization and component-teardown changes are orthogonal to
  that task's cleanup-reason simplification and should be designed and reviewed
  as a focused follow-up.
- Findings:
  - A natural Nextest stress run completed 10,000/10,000 iterations, showing that
    the production interleaving is extremely narrow.
  - A temporary diagnostic purge wake immediately after runtime-layout
    installation reproduced the original assertion and 10-second timeout
    exactly; the diagnostic change was then removed.
  - The same split publication sequence exists in both create index and drop
    index.
  - Debugger thread stacks showed the timed-out test unwinding through
    `ComponentRegistry` drop while blocked in `QuiescentBox<MemPool>::drop`; the
    shared evictor and I/O worker threads were still alive.
  - `ComponentRegistry::shutdown_all` sets its registry-wide idempotence flag
    before iterating. A purge-worker join panic aborts that iteration, and the
    retry during unwind returns early because the flag is already set.
- Direction Hint:
  Prefer one atomic publication boundary that prevents purge from entering
  between runtime-layout installation and history publication. A combined
  catalog operation holding the user-table entry guard while installing and
  publishing is a strong candidate if it preserves the established
  catalog-entry-to-table-layout lock order; coordinating purge with the metadata
  change gate is another option. Avoid treating removal of pointer-identity
  validation as the fix.
  
  For teardown, preserve reverse registration order and idempotence while making
  partial progress recoverable. Consider catching each component shutdown panic,
  continuing the remaining hooks, and resuming the first panic afterward, or
  tracking per-component/cursor progress instead of one preemptive global flag.

## Scope Hint

- Make create-index and drop-index runtime-layout/catalog-history publication
  atomic relative to metadata-history purge, with a documented lock order.
- Add deterministic regression coverage that forces purge at the publication
  boundary for both index-DDL directions.
- Make reverse component shutdown panic-safe so all remaining worker shutdown
  hooks run before the first panic is propagated.
- Add teardown coverage proving a worker panic cannot strand buffer-pool
  quiescent guards or turn into a test/process hang.
- Exclude broader DDL redesign and adaptive worker scheduling.

## Acceptance Hint

1. Metadata-history purge cannot observe old catalog metadata paired with a new
   installed runtime layout during create index or drop index.
2. Pointer-identity validation remains meaningful; the fix does not merely
   remove or weaken the invariant assertion.
3. Deterministic concurrency tests cover purge overlap with both index-DDL
   publication paths.
4. If one component shutdown hook panics, later hooks still execute in reverse
   registration order and the original panic remains visible afterward.
5. A worker panic during engine shutdown terminates without hanging on retained
   quiescent guards.
6. Workspace tests, alternate `libaio` tests, formatting, and Clippy pass.

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
