---
id: 000223
title: Optimize Purge Cycle Triggering and Worker Utilization
status: proposal
created: 2026-07-13
github_issue: 846
---

# Task: Optimize Purge Cycle Triggering and Worker Utilization

## Summary

Make purge scheduling distinguish a GC-bucket minimum transition that actually
crosses the current global active-snapshot horizon from progress in a later,
non-blocking bucket. Record the original and resulting bucket minimum for both
commit and rollback, aggregate the minimum original STS across coalesced
progress, and run full transaction GC only when that original STS is strictly
below a freshly calculated current horizon. Independently carry the minimum CTS
of newly recorded system payloads and force transaction GC only when that CTS is
strictly below the current horizon.

Use one dispatcher purge loop for every positive `purge_threads` value. A
genuinely newer horizon cycle continues to include all-bucket transaction GC,
retained-root processing, dropped-table processing, and completed-horizon
publication. An eligible system payload at an unchanged horizon runs the
required all-bucket transaction GC without implicitly repeating
horizon-dependent housekeeping or republishing completed progress.

For `purge_threads = N`, start one dispatcher-worker and `N - 1` executors so
the configuration describes the total number of threads capable of bucket
purge work. With `N = 1`, use a zero-executor fast path without task or
completion channels. Otherwise enqueue all remote tasks before processing the
dispatcher's local share, wait for every required bucket to succeed, and only
then merge retirement batches and deallocate checkpoint-retired row pages.

Replace the fixed 64-bucket layout with startup configuration `gc_buckets`.
Default it to 32, accept only powers of two from 1 through 256, and keep it
independent from `purge_threads`. User routing remains round-robin, while system
retirement routing is computed from the table id and current runtime bucket
count at ordered handoff rather than stored in the payload.

## Context

Issue Labels:
- type:task
- priority:medium
- codex

Source Backlogs:
- docs/backlogs/000157-optimize-purge-full-gc-triggering-and-dispatcher-worker-utilization.md

Related implemented design:
- `docs/tasks/000220-move-table-checkpoint-completion-to-no-wait-system-transactions.md`
- `docs/tasks/000176-unify-transaction-background-workers.md`
- `docs/backlogs/closed/000125-redesign-redo-gc-purge-worker-ownership.md`
- `docs/transaction-system.md`
- `docs/checkpoint-and-recovery.md`
- `docs/garbage-collect.md`
- `docs/process/unit-test.md`

Task 000176 moved committed-payload GC analysis into purge coordination and
established the current staged shutdown contract. Redo completion hands
committed payload maps non-lossily to purge; cleanup may continue to record
rollback progress until redo and cleanup have joined; purge receives its
terminal marker only afterward. This task refines eligibility and execution
inside that ownership model. It does not reopen transaction-background worker
ownership.

Task 000220 added checkpoint system transactions to the normal committed-purge
pipeline. A system payload has no active STS to remove. Its ordered CTS is its
reclamation fence, and it carries one table-affine `RetiredRowPageBatch`. Purge
must finish eligible row-undo and index work in every GC bucket before it
validates, unlinks, and deallocates any page from such a batch: undo owned by
another bucket can still reference a checkpoint-retired page. The existing
all-bucket barrier and coordinator-owned physical deallocation are therefore
correctness requirements, not optional scheduling policy.

The current trigger loses important information. A GC bucket returns one
boolean when its cached minimum active STS advances, and it also returns `true`
for every system payload. `TransactionSystem::record_committed_for_purge()` ORs
those booleans across buckets. Coalescing upgrades a true result to
`PurgeWork::full()`, which schedules transaction GC, retained-root cleanup, and
dropped-table cleanup together. This has two avoidable consequences:

1. If a long-lived transaction in bucket A is the global blocker, completion of
   any later transaction in another bucket advances that later bucket's local
   minimum and launches a full round even though the global active horizon is
   unchanged. In dispatcher mode the empty round sends 64 tasks and waits for
   64 completions.
2. Every new system payload launches a full round even when its CTS is equal to
   or newer than the current horizon and `GCBucket::get_purge_list()` therefore
   cannot select it.

An observed new bucket minimum is not by itself an authoritative purge horizon.
For example, removing the last transaction can observe
`MAX_SNAPSHOT_TS`, after which a new transaction may enter the same bucket
before purge consumes the progress message. The message must carry causal
information about the transition, while purge must still calculate the current
global horizon from the authoritative bucket atomics and timestamp upper bound.

The missing causal value is the bucket minimum before removal. Let a bucket
transition from `original_sts` to either a finite `new_sts` or no active STS.
After purge records/coalesces all available work and calculates the fresh global
horizon:

- `original_sts < current_horizon` proves that this transition crossed the old
  bucket blocker;
- `original_sts > current_horizon` proves that an older transaction elsewhere
  still blocks global progress;
- the additional check `current_horizon > completed_horizon` prevents a stale
  progress message from repeating a horizon cycle that already succeeded.

Start timestamps are globally ordered, so a transaction entering after the
recorded transition cannot invalidate the strict crossing test: the fresh
authoritative scan observes that transaction, and its STS is newer than the
transition's original STS. Commit and rollback use the same transition rule.
Committed payloads are always queued before scheduling is decided; skipping a
round for a later non-blocking bucket loses no payload. When the true oldest
transaction later commits or rolls back, its transition crosses the fresh
horizon and launches the all-bucket round that drains every newly eligible
queue prefix.

The current multi-thread topology also underuses its coordinator. With
`purge_threads = N > 1`, startup creates N executor threads plus a dispatcher.
The dispatcher sends one task for each of the 64 buckets, collects retirement
batches in a shared mutex-protected vector, waits for all results, and performs
page deallocation and housekeeping. It does not execute bucket work. The
default value of two therefore creates three purge threads while only two run
row-undo/index purge. This task makes the dispatcher one of the configured
workers without changing its ownership of the final barrier and housekeeping.

Resolved alternatives:

- Do not trust a reported post-removal bucket minimum as the global horizon.
  Transaction start can change an empty bucket from `MAX_SNAPSHOT_TS` to a new
  STS before purge consumes the message. Keep the authoritative global scan.
- Do not use only a boolean bucket-advanced result. It cannot distinguish
  progress in the actual global blocker from progress behind a long-lived
  transaction in another bucket.
- Do not model the entire committed result as one enum. Active-STS transition
  and system-payload arrival are independent and can occur together in one
  committed batch. Use an enum for the mutually exclusive bucket-minimum
  transition and a struct for the aggregate product.
- Do not introduce a dynamic shared cursor, work stealing, or a general
  storage-wide maintenance runtime. Static worker slots solve the observed
  utilization problem with simpler failure, fairness, and shutdown reasoning.
- Do not retain N executors and count the dispatcher as an extra worker. The
  public configuration should describe the total bucket-working concurrency.
- Do not add checkpoint-specific GC queues, select only the system payload's
  bucket, or deallocate pages per bucket. Those approaches violate the
  cross-bucket undo/index-before-page-deallocation invariant.
- Do not add public purge-round/task counters. Use pure planner assertions and
  narrow test-only scheduling observation for deterministic validation without
  expanding the public statistics API.

## Goals

1. Represent a changed GC-bucket active minimum with its original STS and its
   finite new STS or no-active outcome.
2. Apply the same active-STS transition semantics to committed user payloads and
   rollback cleanup.
3. Keep committed payload recording non-lossy regardless of whether the current
   message schedules a purge round.
4. Avoid full transaction GC when later transactions finish behind a long-lived
   global blocker in another bucket.
5. Aggregate the minimum original STS across coalesced active progress and the
   minimum CTS across newly recorded system payloads.
6. Calculate the current global horizon from authoritative GC-bucket minima and
   the timestamp upper bound before making a scheduling decision.
7. Run all-bucket transaction GC when active progress crosses a genuinely newer
   horizon, an explicit full observation sees a newer horizon, or a newly
   recorded system payload is strictly eligible.
8. Keep retained-root and dropped-table housekeeping independent from a forced
   system round at an unchanged horizon.
9. Publish `global_visible_sts` only after every operation in a genuinely newer
   horizon cycle succeeds.
10. Use one dispatcher cycle planner and completed-horizon implementation for
    every positive purge-thread count.
11. Define `purge_threads` as the total number of bucket-working purge threads,
    including the dispatcher-worker.
12. Give the dispatcher a deterministic worker slot and stable bucket share,
    enqueue remote work first, and preserve the all-bucket completion barrier.
13. Preserve fatal poison boundaries, non-lossy committed ownership, stop
    ordering, executor channel closure, and worker joining behavior.
14. Reject `purge_threads = 0` before transaction-system worker startup.
15. Make the GC bucket count startup-configurable, default it to 32, and reject
    values outside the power-of-two range 1 through 256.

## Non-Goals

1. Do not change user transaction round-robin sharding or table-affine
   system-payload sharding beyond applying them to the configured bucket count.
2. Do not change the strict `committed.cts < current_horizon` eligibility rule,
   committed FIFO order, or same-table retirement-batch order.
3. Do not introduce a global active-transaction table, foreground notification
   on transaction start, or a cached dispatcher shadow that replaces the
   authoritative horizon scan.
4. Do not change row undo, index GC, deletion-buffer promotion, retained-root
   eligibility, dropped-table eligibility, or physical page deallocation
   algorithms.
5. Do not add a checkpoint-specific purge queue or permit page deallocation
   before every required GC bucket succeeds.
6. Do not redesign redo completion, transaction cleanup ownership, transaction
   background-worker grouping, or staged shutdown order from task 000176.
7. Do not introduce dynamic work stealing, adaptive thread counts, priorities,
   backpressure, or a general storage-wide background runtime.
8. Do not change the default `purge_threads = 2` or cap positive thread counts
   at the number of GC buckets. Executors beyond the bucket count may be idle.
9. Do not add public transaction-system statistics, wall-clock performance
   thresholds, or a new purge benchmark framework.
10. Do not change public maintenance wait predicates, transaction APIs,
    persistent formats, redo encoding, checkpoint publication, or recovery.
11. Do not support changing `gc_buckets` within one running engine instance or
    persist bucket identity across restarts.

## Plan

### 1. Model active-bucket minimum transitions

Add a private transition enum in `doradb-storage/src/trx/purge.rs` with the
following semantic shape:

```rust
enum ActiveStsProgress {
    NoActive {
        original_sts: TrxID,
    },
    Advance {
        original_sts: TrxID,
        new_sts: TrxID,
    },
}
```

Use `Option<ActiveStsProgress>` for bucket removal results. `None` means the
removed transaction was not the bucket minimum, or a committed batch otherwise
left the cached minimum unchanged. Enforce these invariants with debug
assertions and focused tests:

- `original_sts` is the bucket's cached minimum before removal;
- `Advance` requires `original_sts < new_sts < MAX_SNAPSHOT_TS`;
- `NoActive` means the resulting minimum is `MAX_SNAPSHOT_TS`;
- the bucket atomic is updated to the represented result before the transition
  is returned or sent.

Refactor the current boolean `update_min_active_sts()` result so it constructs
this transition after the active-list lock determines the final minimum. For a
committed batch in one bucket, capture the original minimum before removing any
user STSs and represent the one transition from that original value to the
final value after the whole batch. System payloads do not alter active STS.

### 2. Return orthogonal committed progress and send rollback progress

Let per-bucket committed recording return both:

- `active_sts: Option<ActiveStsProgress>`;
- `min_system_cts: Option<TrxID>`.

The minimum system CTS covers only newly recorded system payloads in that
batch. Every committed payload is appended to its existing committed FIFO
before the result is returned.

At `TransactionSystem::record_committed_for_purge()`, aggregate all bucket
results into a `CommittedPurgeProgress`-style struct containing:

- `min_original_sts: Option<TrxID>`, the minimum `original_sts` from every
  changed active bucket;
- `min_system_cts: Option<TrxID>`, the minimum newly recorded system CTS.

Minimum aggregation is sufficient because the cycle decision asks whether any
coalesced transition has `original_sts < current_horizon`. Do not discard either
dimension when a committed map contains user and system payloads together.

Make `TransactionSystem::record_rollback_for_purge()` use the same bucket
transition result. When it is `Some`, send a new targeted, payload-free purge
message carrying the transition instead of upgrading rollback progress to
`Purge::FullObservation`. The bucket state is authoritative; the message is a
coalescible scheduling observation and may be ignored only after normal purge
shutdown has closed its receiver. `None` sends no wake because the bucket
minimum did not change.

### 3. Preserve causal progress during coalescing

Replace `PurgeWork::full_gc` with decision inputs rather than a preselected
cycle:

- minimum original active STS from commit and rollback progress;
- minimum new system CTS;
- an explicit full-observation flag;
- explicit table-root-retention work;
- explicit dropped-table work;
- terminal stop.

Absorb messages as follows:

- `Purge::Committed` records every payload map and merges both aggregate
  minima;
- targeted active-STS progress merges its transition's `original_sts`;
- `Purge::FullObservation` sets the explicit observation flag and explicitly
  requests both housekeeping classes;
- targeted root/drop messages set only their own flags;
- `Purge::Stop` remains terminal after all preceding committed messages have
  been recorded, and collected lossy work is intentionally skipped during
  shutdown as today.

Use strict minimum aggregation for original STS and system CTS. Do not coalesce
away committed payload recording, and do not infer system eligibility from an
active-STS transition.

### 4. Derive one shared purge-cycle plan

Add a pure private planner used by both purge-loop implementations. Its inputs
are the coalesced work, the last successfully completed GC horizon, and the
fresh current horizon from `calc_min_active_sts_for_gc()`.

Skip the global calculation only when recording produced no active transition
or system CTS and no explicit observation/housekeeping work exists. Otherwise
calculate and publish the current GC horizon as today; this early publication
continues to serve checkpoint cutoff and active-root readiness and does not
claim physical cleanup completion.

Define the plan with these predicates:

```text
horizon_newer = current_horizon > completed_horizon

active_crossed =
    horizon_newer
    and min_original_sts exists
    and min_original_sts < current_horizon

explicit_observation_advanced =
    horizon_newer
    and full_observation was requested

system_eligible =
    min_system_cts exists
    and min_system_cts < current_horizon

horizon_cycle =
    active_crossed
    or explicit_observation_advanced
    or (horizon_newer and system_eligible)

transaction_gc = horizon_cycle or system_eligible
table_root_retention = explicit_table_root_retention or horizon_cycle
dropped_table = explicit_dropped_table or horizon_cycle
advance_completed_horizon = horizon_cycle
```

This distinction is intentional:

- Later transactions finishing behind an older blocker have
  `min_original_sts > current_horizon` and do not launch a transaction-GC
  round, even if the coordinator's completed horizon is older than the current
  active STS because transactions started since the last cycle.
- When the real blocker finishes, its original STS is strictly below the fresh
  current horizon and one all-bucket horizon cycle runs.
- A system payload eligible at an unchanged horizon runs transaction GC only.
- An eligible system payload observed together with a genuinely newer horizon
  runs the complete horizon cycle and may advance completed progress.
- An explicit full observation is the fallback that scans all authoritative
  queues when a waiter or caller requests progress; it runs a horizon cycle only
  if the observed horizon is newer.
- Targeted root/drop work does not implicitly run transaction GC or advance
  completed progress merely because timestamps have been allocated since the
  previous horizon cycle.

Initialize the loop-local completed horizon from
`TransactionSystem::global_visible_sts()`. Update it, and call
`update_global_visible_sts()`, only after every transaction, retirement-page,
retained-root, and dropped-table operation selected by a `horizon_cycle`
succeeds. A system-only unchanged-horizon round must not update it. Use this
planner and publication order in the sole `PurgeDispatcher` loop.

### 5. Make the GC bucket count configurable

Add public `TrxSysConfig::gc_buckets` configuration and a builder of the same
name. Export `DEFAULT_GC_BUCKETS = 32` through `doradb_storage::conf`. Missing
serialized fields default to 32 for configuration compatibility. Central
transaction-system normalization accepts only powers of two from 1 through 256
and reports `ConfigError::InvalidGcBuckets` with the rejected value and legal
range for every other value.

Allocate the runtime bucket slice from this setting and use its actual length
for user round-robin routing. Remove the redundant bucket number from
`SysTrxPayload`; at ordered committed-payload handoff, map system retirement
payloads with `table_id % gc_buckets`. The count is immutable for one engine
lifetime, does not enter redo/checkpoint formats, and may change across restart
without migration. Keep `purge_threads` independent and allow it to exceed the
bucket count.

### 6. Make configured concurrency exact

For every `purge_threads = N`, make the dispatcher worker slot zero and spawn
exactly `N - 1` executor threads. Give executor thread names stable slot numbers
`1..N`. Store the total worker count in the dispatcher or derive it as
`executor_senders.len() + 1`. For `N = 1`, run the same dispatcher with an empty
executor-sender list.

Partition every full transaction-GC round with:

```text
worker_slot(gc_no) = gc_no % purge_threads
```

Slot zero is dispatcher-local. Slot `k > 0` maps to executor sender `k - 1`.
The assignment is stable across rounds, covers every GC bucket exactly once,
and differs in bucket count by at most one among worker slots while
`purge_threads <= gc_buckets`. Remove the rotating `dispatch_no`. Positive
thread counts above `gc_buckets` remain valid; extra slots simply receive no
buckets.

### 7. Share per-bucket execution and return task-owned results

Extract one async per-bucket helper that:

1. drains the eligible committed prefix from the selected bucket using the
   round's current horizon;
2. calls the existing `purge_trx_list()` access/fatal boundary;
3. returns that bucket's ordered `Vec<RetiredRowPageBatch>`.

Use this helper from dispatcher-local execution and `PurgeExecutor` so both
paths have identical row-undo, index-GC, statistics, and fatal behavior. The
zero-executor path must skip completion-channel creation, remote sends and
waits, and per-bucket modulo selection; it directly iterates every bucket as
dispatcher-local work.

Remove the shared `Arc<Mutex<Vec<RetiredRowPageBatch>>>` from `PurgeTask`.
Return a task result containing the GC bucket number and either its retirement
batches or its existing purge error. The dispatcher must:

1. enqueue every remote bucket task in deterministic GC-bucket order;
2. only then execute its local buckets in deterministic order;
3. retain local results without processing any retired page;
4. receive every expected remote result;
5. return immediately without page processing or horizon publication on any
   local or remote error;
6. after all results succeed, order results by GC bucket and flatten the
   per-bucket batch vectors;
7. call the existing coordinator-owned `process_retired_row_pages()` once.

Same-table FIFO remains intact because all retirement batches for one table use
one table-affine GC bucket and that bucket drains its committed FIFO in order.
No retired page is unlinked or deallocated until every required bucket has
successfully finished undo/index cleanup.

Preserve current invariant handling for executor task-channel send failure,
completion-channel closure, fatal poison, and normal stop. On normal stop the
dispatcher closes executor senders after committed recording is no longer
possible. On fatal return, dropping the dispatcher closes the same senders;
executors finish already queued tasks, observe channel closure, and remain
joinable by transaction-system shutdown. A failed result never permits page
processing or completed-horizon advancement.

### 8. Validate and document `purge_threads`

Define `purge_threads >= 1` as a configuration invariant. Add a dedicated
`ConfigError::InvalidPurgeThreads` and validate the value during central
transaction-system configuration normalization, before recovery/startup creates
purge workers. Attach the rejected value and the minimum valid value to the
error report.

Update `TrxSysConfig::purge_threads()` and field documentation to state that the
value is the total number of threads that execute purge bucket work, including
the dispatcher-worker. Keep the default value unchanged.

### 9. Synchronize design documentation

Update:

- `docs/transaction-system.md` for original-STS crossing, strict system-CTS
  eligibility, common cycle planning, and configured worker-count semantics;
- `docs/garbage-collect.md` for deferred non-blocking bucket progress, the
  all-bucket barrier, and unchanged-horizon system rounds;
- `docs/checkpoint-and-recovery.md` for delayed system retirement payloads and
  the distinction between observed GC horizon and completed horizon;
- nearby configuration, purge, and shutdown comments whose worker or trigger
  descriptions become inaccurate.

Do not expand public transaction-system statistics. Test-only observation may
count planned rounds/tasks and record worker-slot execution, but it must remain
narrow and must not complicate production data structures solely for tests.

## Implementation Notes

## Impacts

- `doradb-storage/src/trx/purge.rs`
  - add `ActiveStsProgress` and committed-progress aggregation
  - change commit/rollback bucket recording results
  - add targeted active-progress coordinator messages
  - replace preselected `full_gc` coalescing with decision inputs
  - add the shared pure purge-cycle planner
  - use one dispatcher loop for every positive purge-thread count
  - change executor count, static worker-slot partitioning, task results, and
    dispatcher-local bucket execution
  - extend focused unit, concurrency, failure, and integration tests
- `doradb-storage/src/trx/sys.rs`
  - preserve authoritative global horizon calculation
  - update transaction-background worker comments or test access where needed
  - keep staged shutdown and worker joining unchanged
- `doradb-storage/src/conf/trx.rs`
  - validate nonzero purge threads
  - document total bucket-working thread semantics
  - add configuration tests
- `doradb-storage/src/error.rs`
  - add the fieldless invalid-purge-thread configuration domain error
- `doradb-storage/src/table/persistence.rs` or colocated purge integration tests
  - add long-reader checkpoint-retirement eligibility coverage using existing
    production waits and narrow test hooks
- `docs/transaction-system.md`
  - document active transition and worker semantics
- `docs/garbage-collect.md`
  - document cycle eligibility and the preserved page barrier
- `docs/checkpoint-and-recovery.md`
  - document deferred/eligible system retirement behavior

No public statistics, public maintenance APIs, redo formats, persistent table
formats, or recovery interfaces change.

## Test Cases

1. Active-list removal that does not remove the bucket minimum returns no
   `ActiveStsProgress` and sends no rollback wake.
2. Removing the bucket minimum while later transactions remain returns
   `Advance { original_sts, new_sts }`, updates the bucket atomic to `new_sts`,
   and enforces `original_sts < new_sts`.
3. Removing the last active transaction returns
   `NoActive { original_sts }` and updates the bucket atomic to
   `MAX_SNAPSHOT_TS`.
4. A committed batch reports the one transition from its pre-batch bucket
   minimum to its final post-batch minimum, even when it removes several user
   STSs in mixed order.
5. A committed system-only batch reports no active transition and the minimum
   system CTS while enqueuing every payload.
6. A mixed committed batch preserves both active progress and minimum system
   CTS; transaction-system aggregation takes the minimum original STS and
   minimum system CTS across all affected buckets.
7. Coalescing commit and rollback progress preserves the minimum original STS;
   coalescing several system handoffs preserves the minimum new system CTS.
8. Committed payloads preceding `Purge::Stop` are recorded even though terminal
   stop discards the collected lossy scheduling work.
9. A long-lived transaction in bucket A remains the fresh global horizon while
   later transactions in other buckets commit or roll back. Their original
   bucket minima are greater than the current horizon, no transaction-GC round
   or all-configured-bucket dispatch occurs, and every committed payload
   remains queued.
10. When that bucket-A transaction later commits, its original STS is below the
    fresh current horizon, one horizon cycle runs, and all newly eligible bucket
    prefixes are purged.
11. The same long-lived bucket-A scenario triggers the same final cycle when A
    rolls back instead of committing.
12. A stale `NoActive` observation followed by a new transaction entering the
    same bucket uses the freshly scanned finite bucket minimum and never treats
    the reported no-active outcome as the authoritative horizon.
13. Active progress whose original STS is below the current horizon but whose
    current horizon is already at the completed horizon does not repeat a
    successfully completed cycle.
14. An explicit full observation at an unchanged horizon performs explicitly
    requested housekeeping but does not dispatch transaction GC or republish
    completed progress.
15. An explicit full observation at a newer horizon performs the complete
    horizon cycle and publishes completed progress only after success.
16. A new system CTS equal to the current horizon is not eligible and launches
    no transaction-GC round.
17. A new system CTS strictly below an unchanged current horizon launches
    all-bucket transaction GC without implicit retained-root/dropped-table work
    and without completed-horizon publication.
18. An eligible system CTS observed at a genuinely newer horizon performs the
    complete horizon cycle and may publish the newer completed horizon.
19. Previously deferred system payloads become eligible when a later explicit
    observation or active-blocker transition sees a newer horizon; no persistent
    force flag is required.
20. For `purge_threads = 1`, startup creates one dispatcher and zero executors;
    it executes every bucket locally exactly once without remote task or
    completion channels and preserves undo/index-before-page-deallocation
    ordering.
21. For the default `purge_threads = 2`, startup creates one dispatcher-worker
    and one executor; even buckets belong to dispatcher slot zero, odd buckets
    belong to executor slot one, and all configured buckets execute exactly
    once.
22. For `purge_threads = 3` and representative larger values, static modulo
    assignment covers every bucket exactly once, has no duplicate ownership,
    and differs by at most one bucket among nonempty slots.
23. For `purge_threads > gc_buckets`, every bucket still executes exactly once
    and extra executors remain idle without division, indexing, or shutdown
    failure.
24. Narrow test-only scheduling observation proves every remote task is enqueued
    before the first dispatcher-local bucket begins.
25. Hold one remote bucket before completion, let all dispatcher-local buckets
    finish, and assert that no retirement page is processed and no completed
    horizon is published until the remote bucket succeeds. Use channels/events,
    not sleeps.
26. Dispatcher-local and executor execution return retirement batches through
    task-owned results; deterministic merge retains FIFO order within every GC
    bucket and processes pages only after the full success barrier.
27. Controlled dispatcher-local purge failure and controlled remote purge
    failure both leave retirement pages unprocessed, leave completed progress
    unchanged, poison through the existing fatal boundary, and let executor
    channels close/join normally.
28. An end-to-end table checkpoint with a long-running reader observes the
    ordered system handoff, retains the checkpoint-retired hot pages while the
    system CTS is ineligible, then reclaims them only after releasing the reader
    produces eligible horizon progress. Cover the default two-worker mode and
    the zero-executor dispatcher path without elapsed-time progress assumptions.
29. Existing transaction purge statistics still count exactly the processed
    transactions, row undo entries, and index entries after dispatcher-local
    execution is introduced.
30. Test-only planner/task counts show zero bucket tasks for the non-global
    bucket-A scenario and `gc_buckets` exactly-once bucket executions for a
    required full round. Do not assert wall-clock timing.
31. `purge_threads = 0` fails configuration normalization with
    `ConfigError::InvalidPurgeThreads`; values 1, the default, and larger
    positive values remain accepted. `gc_buckets` defaults to 32, accepts
    1/2/4/8/16/32/64/128/256, rejects every other value with
    `ConfigError::InvalidGcBuckets`, and defaults a missing serialized field.
32. Existing shutdown regression coverage proves final committed handoffs are
    recorded before stop, cleanup progress cannot race purge teardown, queued
    executor tasks drain or fail safely, and every configured purge thread
    joins without deadlock.
33. Concurrent tests establish prerequisite predicates in production order and
    use hooks, events, channels, or production wait APIs rather than sleeps.
34. Run `cargo fmt --all --check`.
35. Run `cargo clippy --workspace --all-targets -- -D warnings`.
36. Run `cargo nextest run --workspace`.
37. Run `cargo nextest run -p doradb-storage --no-default-features --features
    libaio` because the changed barrier coordinates backend-neutral row-page
    access/deallocation paths.
38. Stress the focused dispatcher barrier and long-reader eligibility tests with
    `cargo nextest run -p doradb-storage --stress-count 100 <test-filter>`; a
    passing stress run supplements but does not replace predicate reasoning.
39. Run focused coverage for `doradb-storage/src/trx/purge.rs`, changed
    transaction configuration/error paths, and the checkpoint-retirement
    regression. Meet the 80% focused review bar or explain definition-heavy
    exceptions through covered consumers.
40. Run `git diff --check` and `tools/style_audit.rs --diff-base origin/main`
    before review/resolve.

## Open Questions

No blocking design questions remain for the approved scope.
