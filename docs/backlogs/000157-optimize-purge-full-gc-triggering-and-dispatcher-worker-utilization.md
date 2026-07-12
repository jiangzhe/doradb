# Backlog: Optimize purge full-GC triggering and dispatcher worker utilization

## Summary

Optimize purge scheduling so bucket-local progress does not launch a full cross-bucket GC round when the global horizon is unchanged, while still promptly observing eligible system GC payloads. Also make `PurgeDispatcher` execute a share of bucket tasks so `purge_threads` describes the total threads doing purge work instead of executor threads plus a mostly waiting coordinator.

## Reference

Deferred from `docs/tasks/000220-move-table-checkpoint-completion-to-no-wait-system-transactions.md` after reviewing system checkpoint payload reclamation in `doradb-storage/src/trx/purge.rs`.

Related worker-ownership context is preserved in `docs/backlogs/closed/000125-redesign-redo-gc-purge-worker-ownership.md`, which was implemented through the unified transaction background-worker work. This follow-up does not reopen the ownership redesign; it refines purge-cycle eligibility and utilization within the resulting purge-owned committed-payload pipeline.

## Deferred From (Optional)

`docs/tasks/000220-move-table-checkpoint-completion-to-no-wait-system-transactions.md`

## Deferral Context (Optional)

- Defer Reason: Task 000220 is focused on correctness of no-wait checkpoint system transactions, durable publication, failure handling, and safe purge ownership. Eligibility-aware wake suppression and changing the effective purge worker topology are performance and scheduling refinements with their own fairness, shutdown, and validation tradeoffs. Keeping them separate avoids expanding the current task while preserving the analysis needed for a future implementation-ready task.
- Findings:
  - `GCBucket::record_committed_for_purge()` currently returns one boolean that conflates bucket-local active-STS progress with the arrival of a system GC payload. Coalescing turns either cause into `PurgeWork::full_gc`.
  - The previous `curr_sts > min_sts` gate suppressed bucket-local progress when another bucket still held the global minimum. It was removed because system payloads have no active STS and can require observation even when the global horizon is unchanged.
  - Unconditional `work.full_gc` is expensive. There are 64 GC buckets. In dispatcher mode an empty round sends 64 tasks, waits for 64 completions, locks every committed queue, creates per-task purge state, and may also run table-root and dropped-table housekeeping. The default configuration uses two executor threads.
  - A boolean `force_gc` for every system handoff is still too broad. If an old reader holds `curr_sts` below the system CTS, no newly queued system payload is eligible and the forced round is empty.
  - Carrying the minimum CTS among newly recorded system payloads permits the coordinator to force GC only when `min_system_cts < curr_sts`, matching the strict eligibility check in `GCBucket::get_purge_list()`.
  - User payloads do not need the same force threshold. Their active STS remains registered until the committed handoff is recorded, so the global horizon cannot pass their later CTS before that handoff.
  - If a system payload is not yet eligible, the transaction that blocks the horizon will later generate normal commit or rollback progress. A persistent force flag is therefore unnecessary.
  - An eligible system payload must still trigger a full cross-bucket transaction-GC round. Undo or index state in another bucket may reference a checkpoint-retired row page, so every eligible bucket must complete before any collected page is deallocated.
  - `PurgeDispatcher` currently coordinates, collects pages, performs final deallocation and housekeeping, but mostly waits while executor threads process bucket tasks. With `purge_threads = N`, dispatcher mode currently has N executor threads plus the dispatcher thread; the configured value does not describe the total threads capable of purge work.
  - The dispatcher can own one worker slot. For `N > 1`, spawn `N - 1` executor threads, dispatch their remote bucket tasks first, process the dispatcher's assigned bucket share locally while they run, then wait for remote completions before deduplication and page deallocation.
- Direction Hint:
  Prefer a structured committed-purge progress result containing whether the global horizon should be re-evaluated and the minimum newly recorded system CTS. Aggregate boolean progress with OR and system CTS with minimum during non-lossy committed-message coalescing.
  
  Derive one shared purge-cycle plan from the coalesced work, the last successfully completed GC horizon, and the newly calculated current horizon. Run full transaction GC when the global horizon advanced or when `min_system_cts < curr_sts`. A forced system round at an unchanged horizon should run the required all-bucket transaction GC without implicitly repeating horizon-dependent table-root or dropped-table work; explicit housekeeping requests remain independent. Advance `global_visible_sts` only after successful work for a genuinely newer horizon.
  
  For dispatcher mode, treat the dispatcher as one of the configured purge workers and spawn only `purge_threads - 1` executor threads. Extract shared per-bucket execution so dispatcher-local and executor tasks use identical undo/index purge logic. Start remote work before local work, retain the completion barrier across every assigned bucket, and merge local and remote retired-page collections only after successful completion. Preserve current poison boundaries, non-lossy committed-payload recording, stop ordering, and executor shutdown behavior. Avoid checkpoint-specific GC queues or immediate per-bucket page deallocation because they weaken the cross-bucket reference invariant.

## Scope Hint

Design and implement eligibility-aware purge-cycle scheduling and dispatcher participation in bucket work. Cover committed-handoff progress reporting, purge-work coalescing, shared single/multi-thread cycle planning, configured thread-count semantics, dispatcher/executor task distribution, completion barriers, page collection and deduplication, fatal-error propagation, and worker shutdown. Preserve existing payload eligibility and the all-bucket undo/index barrier before retired row-page deallocation.

## Acceptance Hint

An unchanged global horizon no longer launches an empty full transaction-GC round merely because one bucket observed local progress. A newly handed-off system payload forces a round only when its CTS is already below the current purge horizon; otherwise normal later horizon progress makes it eligible. Single- and multi-thread purge modes share the same scheduling decision and update `global_visible_sts` only after successful horizon-advancing work. For `purge_threads > 1`, one dispatcher-worker plus `purge_threads - 1` executors perform bucket work, so the configured count matches the total purge-working threads. Remote tasks are started before the dispatcher processes its local share, all required bucket work completes before retired pages are deallocated, and fatal/shutdown behavior remains non-lossy. Tests cover trigger decisions, coalescing, delayed system payloads, reader-delayed checkpoint-page reclamation, dispatcher participation, task distribution, and cross-bucket ordering.

## Notes (Optional)

Future planning should include focused scheduling tests for unchanged global horizons, strict system-CTS eligibility, coalesced minimum CTS, and terminal stop behavior. Add an end-to-end checkpoint test with a long-running reader that retains retired pages until its STS is released. For dispatcher utilization, verify configured thread counts for `purge_threads = 1`, the default value, and larger values; verify that the dispatcher receives a stable share of buckets and that no page deallocation or visible-horizon publication occurs before all local and remote tasks complete. Performance validation should count avoided empty rounds/tasks and compare purge throughput before and after dispatcher participation.

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
