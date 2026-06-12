---
id: 000176
title: Unify Transaction Background Workers
status: proposal
created: 2026-06-12
github_issue: 702
---

# Task: Unify Transaction Background Workers

## Summary

Refactor transaction-system background worker ownership so redo IO, purge
coordination, purge execution, and transaction cleanup are owned by one
transaction-background worker component with explicit staged shutdown.

The dedicated GC analyzer thread should be removed. Successful redo ordered
completion should hand committed transaction payloads to the purge coordinator,
which performs GC analysis before scheduling purge work. In single-thread purge
mode, the single purge thread owns both GC analysis and purge execution. In
dispatch/worker purge mode, the dispatcher owns GC analysis and dispatches only
the actual purge tasks to executors.

## Context

Issue Labels:
- type:task
- priority:medium
- codex

Source Backlogs:
- docs/backlogs/000125-redesign-redo-gc-purge-worker-ownership.md

Related context:
- docs/tasks/000175-remove-multi-stream-redo.md
- docs/tasks/000169-fix-failed-precommit-redo-cleanup.md
- docs/architecture.md
- docs/transaction-system.md
- docs/checkpoint-and-recovery.md
- docs/garbage-collect.md
- docs/engine-component-lifetime.md
- docs/process/unit-test.md

Backlog 000125 was created after task 000175 exposed a shutdown race in the
transaction background worker topology. Today redo IO emits `GC::Commit`
messages to a separate GC analyzer thread, while worker shutdown sends
`GC::Stop` before joining redo IO. That allows a stop marker to overtake a final
commit handoff: redo IO can still complete a group and send `GC::Commit` after
`GC::Stop` has already caused the GC thread to exit.

The current split also makes lifecycle ownership wider than the logical work
requires. GC analysis only updates transaction GC buckets and wakes purge when
the minimum active STS may have advanced. Purge already owns the correctness
barrier for advancing `global_visible_sts`: it collects purgeable committed
transactions, runs row/index/page cleanup, and updates the global visible
timestamp only after required purge work succeeds.

Transaction cleanup belongs under the same transaction-background owner but has
a distinct drain barrier. Redo failure paths enqueue `FailedPrecommit` cleanup,
explicit rollback and abandonment enqueue terminal cleanup, and cleanup rollback
updates GC bucket state through `gc_analyze_rollback`. Cleanup therefore must
stay alive until redo IO can no longer enqueue failed-precommit work, and purge
shutdown must wait until cleanup can no longer update GC buckets.

Rejected alternatives considered:

1. Keep the GC thread and only group redo IO plus GC shutdown. This is a valid
   tactical fix if implementation risk must be minimized, but it preserves the
   unnecessary GC/purge split and leaves extra lifecycle surface.
2. Make redo IO perform GC analysis directly. This removes the GC thread but
   puts GC bucket locking on the ordered commit IO path and keeps purge from
   owning the analysis that exists only to feed purge progression.
3. Introduce a general adaptive background runtime for all storage components.
   That is likely useful long term, but it spans multiple component families and
   should be planned as an RFC, not hidden inside this transaction-worker task.

## Goals

1. Remove the dedicated GC analyzer thread.
2. Move committed-transaction GC analysis into purge coordination.
3. Preserve non-lossy commit handoff semantics: every committed transaction
   payload accepted from redo IO is analyzed before transaction-worker shutdown
   completes.
4. Prevent any shutdown or stop marker from overtaking earlier committed payload
   handoffs.
5. Keep `global_visible_sts` advancement behind successful purge work.
6. In `purge_threads == 1`, make the single purge thread manage GC analysis and
   purge execution.
7. In `purge_threads > 1`, make the purge dispatcher manage GC analysis and
   dispatch actual purge work to executors.
8. Group redo IO, purge coordination, purge executors, and cleanup under one
   transaction-background worker owner.
9. Preserve a staged shutdown order:
   - close group-commit admission and wake redo IO;
   - join redo IO after final commit and failed-precommit handoffs drain;
   - stop and join cleanup after redo can no longer enqueue cleanup work;
   - stop and join purge after redo and cleanup can no longer affect GC state;
   - join purge executors and close the active redo log file.
10. Preserve failed-precommit cleanup guarantees from task 000169: rollback-
    capable payloads remain owned until cleanup finishes or fatal retention takes
    ownership, cleanup receiver closure before redo shutdown remains an
    invariant violation, and waiters are not completed before mandatory cleanup
    handoff is satisfied.

## Non-Goals

1. Do not change redo file format, redo record encoding, recovery replay rules,
   timestamp seeding, or storage-layout compatibility.
2. Do not redesign MVCC visibility, row undo layout, index undo layout,
   checkpoint algorithms, table-file roots, or public transaction/session APIs.
3. Do not fold cleanup rollback execution into purge workers.
4. Do not introduce a general storage-wide background runtime.
5. Do not make purge advancement best-effort. Failed purge task handoff or failed
   purge execution remains an invariant/fatal boundary, and
   `global_visible_sts` must not advance for buckets whose required purge work
   did not run.
6. Do not replace staged shutdown with one flat broadcast stop consumed
   independently by all workers.

## Plan

1. Define the transaction background worker owner.
   - Keep the registry component name `TransactionSystemWorkers` unless a narrow
     rename can be done without component-order churn.
   - Replace the separate top-level `io_thread`, `gc_thread`, `purge_threads`,
     and `cleanup_thread` fields with a grouped owner, or with nested structs
     whose public shutdown entry point enforces the same staged order.
   - Include redo IO, purge coordinator, purge executors, cleanup, shutdown
     state, and the senders needed for internal staged shutdown.
   - Keep shutdown idempotent.

2. Replace the GC channel with a purge-owned coordinator channel.
   - Remove `GC::Stop` and the dedicated GC receiver from startup state.
   - Replace `GC::Commit(FastHashMap<usize, Vec<CommittedTrx>>)` with a
     purge-coordinator message such as
     `PurgeMessage::Committed(FastHashMap<usize, Vec<CommittedTrx>>)`.
   - Keep lightweight purge wakeups for table-root retention and dropped-table
     cleanup, but distinguish them from non-lossy committed-payload handoffs.
   - Coalescing may collapse lossy wakeups. It must never drop or skip committed
     payload batches that were handed off by redo IO.

3. Move GC analysis into purge coordination.
   - Move or re-home `RedoLog::gc_analyze` as a transaction-system or purge
     helper that updates GC buckets and reports whether the min-active horizon may
     have advanced.
   - When a committed-payload message is received, analyze it before any purge
     shutdown decision can exit the coordinator loop.
   - If analysis reports that the horizon may have advanced, upgrade the current
     work to a full purge cycle.
   - Preserve `calc_min_active_sts_for_gc` as the authoritative global horizon
     calculation over GC bucket minima and the current timestamp upper bound.

4. Update redo ordered completion handoff.
   - In `FileProcessor::sync_io`, after successful ordered completion converts
     `PrecommitTrx` values to `CommittedTrx`, send the committed payload map to
     the purge coordinator.
   - Treat send failure as an invariant violation unless the new shutdown protocol
     makes it impossible for redo IO to still produce committed handoffs.
   - Complete the commit group waiter only after the committed-payload handoff has
     been accepted by the purge coordinator.
   - Keep redo write, submit, sync, and sequential durable-prefix failure handling
     behavior from task 000169 unchanged.

5. Preserve single-thread purge behavior with integrated analysis.
   - In `purge_threads == 1`, the purge thread should receive coordinator
     messages, analyze committed payloads, collect purgeable committed
     transactions, run row/index purge, deallocate GC row pages, process retained
     roots and dropped tables, and advance `global_visible_sts` only after the
     full purge cycle succeeds.
   - Shutdown should drain committed-payload messages already accepted before
     exit. It may skip lossy housekeeping wakeups that are irrelevant once owner
     teardown is proceeding.

6. Preserve dispatch/worker purge behavior with dispatcher-owned analysis.
   - In `purge_threads > 1`, the dispatcher should receive coordinator messages
     and analyze committed payloads before dispatching purge tasks to executors.
   - Executors should continue to execute bucket purge tasks and report completion
     or failure to the dispatcher.
   - The dispatcher must wait for every expected executor result before
     deallocating accumulated GC row pages and advancing `global_visible_sts`.
   - Executor sender failure remains an invariant violation while the dispatcher
     owns the executor senders.

7. Implement staged shutdown in the grouped owner.
   - Close group-commit admission with `FailedPrecommitReason::Shutdown`, enqueue
     or signal redo shutdown, and wake the redo IO thread.
   - Join redo IO first. After this point no new successful committed-payload
     handoffs or failed-precommit cleanup jobs can be produced by redo.
   - Send cleanup `Stop` and join cleanup. Cleanup must drain messages already
     queued behind the stop marker before exiting.
   - After cleanup joins, send purge shutdown/drain and join the purge coordinator
     plus purge executors. This ensures cleanup-side `gc_analyze_rollback` cannot
     race with purge teardown.
   - Close the active redo log file after redo, cleanup, and purge workers have
     joined.

8. Remove dead GC-thread code and update docs/comments.
   - Remove `start_gc_thread`, `RedoLog::gc_loop`, `GC::Stop`, and stale comments
     that describe a separate GC analyzer worker.
   - Update nearby comments in `trx/sys.rs`, `trx/log.rs`, and `trx/purge.rs` to
     describe the new handoff and staged shutdown invariants.
   - Update `docs/transaction-system.md` or `docs/engine-component-lifetime.md`
     only if implementation makes current worker lifecycle descriptions
     inaccurate.

## Implementation Notes

## Impacts

- `doradb-storage/src/conf/trx.rs`
  - `PendingTransactionSystemStartup`
  - transaction worker startup
  - purge/cleanup channel construction
- `doradb-storage/src/trx/sys.rs`
  - `TransactionSystemWorkersOwned`
  - `TransactionSystemWorkers::shutdown`
  - `start_gc_thread`
  - `start_io_thread`
  - `start_cleanup_thread`
  - failed-precommit cleanup lifetime comments
  - terminal and abandoned cleanup sender lifetime
- `doradb-storage/src/trx/log.rs`
  - `RedoLog`
  - `gc_chan`
  - `gc_loop`
  - `FileProcessor::sync_io`
  - redo ordered completion and waiter completion ordering
- `doradb-storage/src/trx/purge.rs`
  - `GC`
  - `Purge`
  - purge coordinator message types
  - purge work coalescing
  - `PurgeSingleThreaded`
  - `PurgeDispatcher`
  - `PurgeExecutor`
  - `GCBucket` analysis helpers
- `doradb-storage/src/trx/mod.rs`
  - tests and helpers that directly call GC-bucket commit/rollback analysis
  - failed-precommit cleanup expectations
- `doradb-storage/src/session.rs`
  - abandoned cleanup request expectations, if comments need refresh
- `doradb-storage/src/engine.rs`
  - shutdown cleanup queueing expectations, if comments need refresh
- `docs/transaction-system.md`
- `docs/engine-component-lifetime.md`

## Test Cases

1. Final commit handoff during shutdown.
   - Arrange a commit group that reaches ordered completion while transaction
     worker shutdown has started.
   - Assert the committed payload is analyzed before shutdown completes.
   - Assert the transaction STS is removed from the relevant GC bucket active list
     and can become purgeable under the normal horizon rule.

2. No stop-marker overtake.
   - Unit-test purge coordinator message handling with queued committed payloads,
     lightweight wakeups, and shutdown.
   - Assert shutdown does not discard committed batches that were accepted before
     the shutdown/drain decision.

3. Single-thread purge owns analysis and purge.
   - Configure `TrxSysConfig::purge_threads(1)`.
   - Commit transactions with row undo, index GC, or GC row-page payloads.
   - Assert GC analysis and purge run through the single purge thread and
     `global_visible_sts` advances only after purge succeeds.

4. Dispatcher owns analysis in multi-thread purge.
   - Configure `TrxSysConfig::purge_threads(2)` or more.
   - Commit transactions that populate multiple GC buckets.
   - Assert the dispatcher analyzes commit batches, sends bucket tasks to
     executors, waits for all task results, and only then advances
     `global_visible_sts`.

5. Cleanup remains alive until redo stops.
   - Exercise or fault-inject a redo failed-precommit path that enqueues cleanup.
   - Assert cleanup receiver remains open until redo IO has joined and queued
     failed-precommit jobs drain.
   - Assert closing cleanup too early remains an invariant violation rather than
     dropping rollback-capable payloads.

6. Cleanup rollback can affect purge horizon before purge shutdown.
   - Queue terminal or abandoned rollback cleanup that calls
     `gc_analyze_rollback`.
   - Start transaction worker shutdown.
   - Assert purge shutdown waits until cleanup is joined, so cleanup-side GC bucket
     updates cannot race with purge teardown.

7. Failed purge handoff/execution does not advance global visible STS.
   - Preserve or add coverage around dispatcher/executor failure paths.
   - Assert `global_visible_sts` is unchanged when required purge work did not
     complete.

8. Idempotent grouped worker shutdown.
   - Call transaction worker shutdown twice.
   - Assert no duplicate stop handling, double join, or log-file double close
     occurs.

9. Validation commands:
   - `cargo fmt`
   - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
   - `cargo nextest run -p doradb-storage`
   - Because this task touches redo IO worker lifecycle and backend-neutral IO
     shutdown, also run:
     `cargo nextest run -p doradb-storage --no-default-features --features libaio`

## Open Questions

No open design questions are blocking this task. Exact internal type names are
left to the implementation, but the commit-handoff, purge-analysis ownership,
cleanup lifetime, and staged shutdown semantics above are acceptance
requirements.
