---
id: 000309
title: Pipelined Recovery with Parallel Page Replay
status: proposal
created: 2026-09-17
github_issue: 1076
---

# Task: Pipelined Recovery with Parallel Page Replay

## Summary

Pipeline the existing single-threaded redo decoder and recovery coordinator
with bounded parallel hot-row replay on the engine thread pool. The dispatcher
groups eligible operations by table and row page, permits at most one submitted
batch and one pending batch per page, and preserves the consumed redo order
within each page lifetime.

Keep page creation, catalog replay, cold deletes, and DDL on the dispatcher.
Table/index DDL flushes and completes all earlier eligible DML for its table.
Retire scheduling entries whenever they become idle, retaining the existing
insertion-history bitmap separately so later redo can reactivate the page.
Complete all replay before existing validation, serial hot-index reconstruction,
and redo repair/startup.

## Context

Source Backlogs:

- docs/backlogs/000087-refactor-recovery-process-parallel-log-replay.md

Issue Labels:

- type:perf
- priority:medium
- codex

There is no parent RFC. The approved scope is one recovery scheduling task,
using the already-running finite-job pool without a persistent-format migration,
runtime lifecycle redesign, or phased index-construction program.

Current implementation:

- [Recovery contracts](../recovery.md) define checkpoint bootstrap, catalog and
  table replay floors, cold-delete classification, metadata validation, and the
  final index-rebuild boundary.
- [RecoveryCoordinator](../../doradb-storage/src/recovery/mod.rs) awaits each
  transaction's replay before consuming the next record. It owns the timeline,
  DDL handling, and a nested table/page registry of insertion-history bitmaps.
- [RedoLogStream](../../doradb-storage/src/recovery/stream.rs) already has a
  bounded direct-I/O read-ahead worker. The consumer assembles and decodes a
  complete validated group before yielding its transaction records.
- [Hot replay](../../doradb-storage/src/table/recover.rs) acquires an exclusive
  page for each row operation. The mutation helpers in
  [table/mod.rs](../../doradb-storage/src/table/mod.rs) touch that page and its
  insertion-history bitmap without maintaining secondary indexes inline.
- [Row-page allocation](../../doradb-storage/src/index/row_page_index.rs)
  reconstructs RowID ranges through ordered append. The dispatcher must keep
  eligible CreateRowPage operations in stream order.
- [ThreadPool](../../doradb-storage/src/runtime/thread_pool.rs) supports finite
  asynchronous jobs, starts before recovery, supervises panics, and drains with
  storage/eviction available. Callers provide ordering, fan-out, and memory bounds.
  [Completion](../../doradb-storage/src/completion.rs) already supports exclusive
  movement of a non-Clone job result.
- [Task 000305](000305-remove-recovery-maps-from-buffer-frames.md) established
  recovery-owned insertion history. A deleted slot cannot reveal whether it was
  never inserted or inserted and subsequently deleted; idle queue retirement
  must preserve this history.
- [Task 000306](000306-recovery-benchmark-and-startup-metrics.md) supplies the
  clean-reopen benchmark and immutable recovery report for correctness and
  performance comparison.

The ordering argument is specific to startup hot-row replay: there are no
surviving transactions or admitted foreground readers, hot mutations affect one
physical page, and secondary indexes consume final row images after a global
drain. A transaction spanning pages or replacing a row across pages can therefore
be split across jobs. Preserve each page's projection of the current replay
sequence, including its page-local space allocation and slot-history checks.
Transaction/group framing and validation remain unchanged.

Related work stays separate:

- [Backlog 000110](../backlogs/000110-unify-hot-row-mem-scan-index-build-recovery.md):
  parallel hot-index construction for recovery and CREATE INDEX.
- [Backlog 000130](../backlogs/000130-large-redo-transaction-streaming-replay.md):
  bounded-memory decoding of very large transaction/group payloads.

## Goals

- Overlap decoding/dispatch with hot-row replay on independent pages, including
  pages of the same table.
- Preserve page-local operation order and all current eligibility, integrity,
  timestamp, catalog, and page-lifetime contracts.
- Bound outstanding submissions, active scheduling entries, and operation counts
  per batch, with progress even when no batch reaches its target size.
- Retire idle scheduling entries and safely reactivate pages with their original
  insertion history.
- Drain the affected table before eligible table/index DDL and drain all replay
  before post-replay validation and index reconstruction.
- Use existing pool completion, error, poison, and bootstrap rollback contracts.
- Demonstrate correctness under controlled scheduling and measure release
  recovery performance against the sequential baseline.

## Non-Goals

- Parallel decoding, catalog replay, cold-delete replay, or index construction.
- Parsing ahead past a blocked DDL, a dependency graph, or transaction-level
  worker scheduling/atomic publication during replay.
- Changes to redo encoding, checkpoint publication, replay floors, retention,
  recovery-visible timestamps, normal MVCC, or buffer-frame metadata.
- Removing or spilling insertion-history bitmaps, or bounding total recovery
  memory independently of decoded groups and the recovered database.
- A new worker pool, general pool queue limit, or a new
  benchmark workload/fixture format.

## Rejected Alternatives

- Fixed worker-owned page partitions add false dependencies between unrelated
  pages sharing a partition and handle skew less directly than page jobs.
- A dependency graph that continues parsing past blocked DDL requires deferred
  metadata resolution and additional lifetime/failure machinery. It belongs in
  an RFC if later measurements justify that broader scope.
- Keeping scheduling structures for every created page makes new scheduling
  overhead grow with all recovered pages. Separate page history from the bounded
  active working set instead; page exhaustion need not be known.

## Plan

### 1. Separate retained history from active scheduling

Add a private recovery dispatcher module, `recovery/dispatch.rs`. Evolve the
current `recovered_tables` registry into page history and a bounded active map.
Conceptual structures, with visibility kept as narrow as their consumers allow:

```rust
type PageKey = (TableID, PageID);

struct ReplayOp {
    cts: TrxID,
    row: RowRedo,
}

struct ActivePage {
    // None while the outstanding job or its completion result owns the bitmap.
    state: Option<RowReplayState>,
    pending: Vec<ReplayOp>,
    ready_queued: bool,
}

struct TableWork {
    submitted_batches: usize,
    pending_ops: usize,
}

struct ReplayDispatcher {
    page_history: FastHashMap<TableID, FastHashMap<PageID, RowReplayState>>,
    active_pages: FastHashMap<PageKey, ActivePage>,
    active_tables: FastHashMap<TableID, TableWork>,
    ready: VecDeque<PageKey>,
    // Tagged waiters over existing Completion cells, yielding in completion order.
    in_flight: FuturesUnordered<BatchCompletion>,
    limits: ReplayLimits,
}
```

The exact future alias can use a boxed Send future. Each waiter retains its page
key outside the job result so rejection/panic still identifies
the submission. A successful batch returns its `RowReplayState` and operation
counts; no Table handle or page guard escapes in a result.

History, an active entry, or its outstanding job/result owns each bitmap exclusively.
Activating a page moves its bitmap out of page history. Pending operations never
mutate the bitmap. Submission moves it into the job; completion returns it.

When completion leaves no pending operations, immediately remove the active
entry and return its bitmap to page history. A later eligible record can activate
the page again. Before collection of an already-finished job's completion, the
entry remains active, so incoming rows join its existing pending batch.

A page with both submitted and pending work occupies one active entry. Keep at
most one ready-queue key per eligible page; there must be no accumulated stale
keys or idle table scheduling counters. Remove a table's scheduling counter when
it has no outstanding work. The active map's retained allocation can follow its
bounded high-water mark; repeated shrinking on every completion is unnecessary.

Retained page history still scales with recovered live pages. It includes empty
created pages for final reconstruction and preserves duplicate-insert validation.
This task bounds the added scheduler state, not that existing recovery obligation.

### 2. Define count limits

Use an internal `ReplayLimits` populated from validated `EngineConfig.recovery`.
`RecoveryConfig` owns startup I/O depth and the DML validation opt-out extracted
from `TrxSysConfig`, together with the three replay limits below. Defaults and
automatic-sizing factors are named `DEFAULT_RECOVERY_*` constants. Tests supply
smaller positive limits through the same public configuration builders.

Optional submission and active-page caps default to `None`. Engine validation
resolves them with saturating arithmetic before storage-root creation: automatic
submissions are twice the pool worker count, and automatic pages are four times
the effective submission limit, including an explicit override. All explicit
limits and I/O depth must be positive.
Benchmark plans expose these settings through `[engine.recovery]`, and resolved
results record concrete limits.

| Limit | Initial production value |
| --- | --- |
| Outstanding batch submissions | `2 * thread_pool.worker_threads()` |
| Active page entries | `4 * task_limit` |
| Operations in one batch | 256 |

An outstanding submission includes pool-queued, executing, I/O-waiting, and
completed-but-uncollected jobs. Release its recovery admission credit only when
the dispatcher consumes completion. The pool's own admission lock remains the
acceptance linearization point; the recovery limit is enforced before calling
`submit_async` and is not a new pool semaphore.

Each pending batch is a `Vec<ReplayOp>` that reserves `max_batch_ops` slots when
its first operation is admitted. Enforce the operation limit using vector length.
Moving it to a worker transfers the allocation without cloning payloads. A page
has at most one submitted batch and one pending batch. Free active-page capacity
only when all work on that page is finished.

Admission does not measure bytes or isolate operations by payload size. Retained
operation counts and scheduler objects are bounded by the three limits, while
memory usage depends on payload sizes and has no explicit byte budget.

The existing whole-group decoder, the currently consumed transaction/operation,
recovered row pages, and retained history are separate costs. On admission
pressure, keep the unadmitted operation in the already-decoded input and stop
advancing that cursor; do not add a separate lookahead queue.

### 3. Dispatch eligible rows in canonical order

Keep decoding, timeline updates, filtering, and catalog access on the coordinator.
Consume complete transaction records in the current stream order and move
eligible hot row payloads into `ReplayOp` without cloning their values.

Preserve the current classification order: coarse floor, known/unknown table
rules, per-table floors, pivot, and hot/cold operation checks precede sidecar
lookup/admission. Skipped records require no registered hot page. Eligible hot
deletes still require a PageID; keyed catalog operations remain invalid for user
tables. Keep cold deletes synchronous on the dispatcher and preserve their
cutoff, same-CTS idempotence, conflicting-marker, and legacy-PageID behavior.

For each eligible hot operation:

1. Reap available completions and resolve any admission pressure.
2. Use its active page, or reserve capacity and move its registered bitmap from
   page history into a new active entry. Missing history is an integrity failure.
3. Append its CTS and payload in consumed order, within the page/global bounds.
4. If it has no outstanding job, queue its key exactly once as ready.
5. Pump ready work at batch limits and after each consumed transaction. Available
   slots may take partial batches; transaction end is a dispatch opportunity,
   not an execution barrier.

The ready queue is FIFO across eligible pages, including partial batches. Batch
fullness triggers a dispatch attempt; it does not give later full batches
priority over an older ready partial batch. A pending successor becomes eligible
only after its predecessor's completion is collected and joins the ready tail.

Whenever a ready batch is submitted, move its operations and bitmap into the
job, clear the ready membership flag, and leave one empty pending slot for new
rows. There is never a second outstanding job for the same page and never a
third buffered batch. Jobs neither submit successors nor wait on replay jobs.

### 4. Apply a batch through one page acquisition

Add a narrow `Table::recover_row_batch` entry in `table/recover.rs` taking pool
guards, the borrowed job-owned bitmap, a slice of replay operations, and the
existing validation policy. Capture the table layout once and acquire the exact
row page exclusively once per batch.

Apply operations sequentially through the existing `recover_row_*_to_page`
helpers. Validate each insert/update before its mutation, preserve RowID-range,
inserted-slot, deleted-state, and space checks, and mark successful mutations
dirty even if a later operation fails. Keep per-operation CTS and row/page/table
diagnostic context. The bounded mutation loop has no scheduler wait while holding
the page latch. Page acquisition may await existing residency, I/O, and latch
progress, so use `ThreadPool::submit_async`.

Each job captures only owned row data, its bitmap, a Table handle, and the pool
guards needed for finite execution. Retain no borrowed coordinator state, catalog
map guard, or engine owner shell. The pool drops the job future and captures
before publishing completion; successful results contain only bitmap/count data.

Refactor existing single-row async wrappers where appropriate to keep one
production implementation of validation/mutation logic. Do not retain unused
production APIs solely for tests. Permanent version maps and page-creation CTS
remain unchanged; no per-row recovery timestamps or frame sidecars are added.

### 5. Collect completion and make progress before waiting

Reuse `ThreadPool::submit_async` and `Completion::wait_take_result`. Conceptually:

```rust
let completion = pool.submit_async(async move {
    replay_page_batch(table, guards, state, batch).await
});
in_flight.push(async move {
    (page_key, completion.wait_take_result().await)
});
```

The pool wrapper stores a terminal result and notifies the Completion Event once
per batch. `wait_take_result` registers before rechecking the sticky result, so
completion before registration is safe. Poll the `FuturesUnordered` after adding
waiters and at progress points; collect completed jobs without waiting for an
earlier unrelated submission. A wake does not preempt synchronous decoding.

For each success, restore the bitmap, merge local work counts, release the task
slot, and either enqueue its pending successor or retire its active entry.
The outer pool completion can carry rejection/panic Fatal; the job result carries
ordinary typed replay failure or success. Preserve the original typed reports
through the existing completion bridge APIs.

Before waiting, reap all immediately available completions, dispatch runnable
work within admission, and recheck the blocked predicate. Under pressure or
drain, submit partial batches regardless of size. A state with pending work,
available admission, and no producer capable of progress is a scheduler bug.

| Condition | Allowed progress and wait predicate |
| --- | --- |
| Submission limit reached | Continue consuming rows while other bounds permit; collect completions to regain slots. |
| Next operation would overflow its page's pending batch | Stop at that operation; submit/drain its predecessor and pending work before admitting it. |
| New active page would exceed the page limit | Stop at that operation; flush/drain until an entry becomes idle and retires. Existing active pages alone do not trigger this limit. |
| Eligible table/index DDL | Stop consuming later redo; flush/drain the target table, then execute the DDL. |
| EOF | Flush all partial batches and drain all outstanding work. |
| Redo input unavailable | Await input while continuing completion collection and eligible dispatch. |

While awaiting input, retain the same pinned `stream.try_next()` future across
worker-completion events. `read_next_group` temporarily moves segment state into
locals across awaits; cancelling and restarting that read after each completion
would lose parser state. Drop an unfinished read only when abandoning the whole
recovery attempt.

Inline page allocation, catalog replay, and DDL bodies may still await their own
I/O/latches and delay new dispatch until they return. Already-submitted jobs
continue. This bounded design does not promise a maximum wall-clock replay
latency: an early pending row may finish late behind a slow predecessor. It must
not wait for more rows or EOF merely because its batch is partial, or starve
behind later eligible full batches.

### 6. Keep creation and DDL lifetimes explicit

For eligible `CreateRowPage`, allocate and initialize the page on the dispatcher
in redo order. Explicit page allocation rejects an already allocated PageID,
including pages whose history has moved into an active entry or job; no separate
history/active-page lookup is needed. Restore creation CTS, then insert its bitmap
into page history. This is the page's first replay obligation; later row redo
activates its scheduling entry. Do not drain unrelated existing pages of the same
table for creation. Preserve allocation cleanup and current replay eligibility.

For eligible CreateTable, DropTable, CreateIndex, and DropIndex, flush and replay
all earlier eligible DML for that table before executing the existing DDL and
associated catalog modifications. The barrier predicate is:

```text
pending_ops(table) == 0 && submitted_batches(table) == 0
```

Drain includes underfilled pending batches, pool-queued jobs, and executing or
I/O-waiting jobs. DROP follows the same replay-before-DDL rule. A new table with
no outstanding work passes immediately. Apply existing eligibility filters
before imposing barriers on skipped DDL.

During the barrier, keep collecting completions and pumping the normal FIFO
ready queue, including already-parsed work for other tables. Return when the
target is drained; unrelated completion is not an additional barrier condition,
although shared admission/worker capacity can affect progress. Do not consume
redo after the DDL until its body finishes.

On DROP, all worker-held table handles must be released before the existing
`Arc::try_unwrap` and runtime destruction. Remove the table's page history and
remaining bookkeeping before subsequent allocation can reuse a PageID. No late
completion can survive this fence. Page creation in another table then starts
with a fresh bitmap.

DataCheckpoint remains the current filtered existence/no-op handling.
TableReplaySilentWatermark and other catalog modifications remain serial with
their existing root-proof/catalog semantics. Replayed silent-watermark rows must
not alter the current restart's effective bounds. These system records do not
acquire a new blanket hot-row drain solely because they use `DDLRedo`.

### 7. Settle success, errors, and cancellation

On EOF, force all pending work through the pool and collect every result before
catalog-parent/descriptor validation, loaded-table reconciliation, absent-file
cleanup, or index rebuild. After successful drain, all surviving page bitmaps
reside in page history. Consume that history through the existing serial index
reconstruction, including empty created pages. Redo repair/finalization and
foreground admission remain after successful replay and reconstruction.

Put every fallible replay-loop exit, including stream, classification, allocation,
and DDL errors, through one settlement boundary:

- Stop consumption and new submissions after observing failure.
- Discard unsubmitted pending operations and ready keys.
- Collect/drain every accepted job and release its resources before returning.
- Return the first observed ordinary error unless draining observes Fatal, which
  takes precedence and retains the engine's original poison reason. Attach
  relevant additional failures without replacing native error domains.
- Do not publish successful recovery metrics or perform post-replay repair.

Error observation order across independent pages may differ from sequential
replay. Within each page, the first invalid operation remains ordered. There is
no attempt to reconstruct a globally earliest error through additional replay.

Bootstrap cancellation/unwind drops unsubmitted state and observers. Accepted
jobs own their captures independently; the existing RegistryBuilder rollback
closes/drains the pool while storage/eviction are live and before resource-owner
teardown. Verify this path rather than relying on an observer Drop to cancel jobs
or adding blocking waits inside worker jobs.

Document scheduler waits at their semantic owner using
[the wait review contract](../shutdown-and-poison.md#review-contract-for-new-waits):
jobs and their live I/O/eviction dependencies produce progress; terminal
Completion results plus counters are authoritative; poison stops new submissions
but never substitutes for draining accepted work; bootstrap teardown drains;
and the coordinator owns combined cleanup while each accepted job owns its
captures. Pool acceptance is the linearization point. Classify the derived
backpressure/table/global drains under the existing pool-job completion family,
updating the document with these recovery predicates.

### 8. Wire resources, reporting, and performance validation

Pass a `QuiescentGuard<ThreadPool>` from the existing component registry through
TransactionSystem bootstrap into RecoveryResources/dispatch construction. Worker
startup order already satisfies this dependency; do not add a pool or reorder
components.

Keep timeline/max-CTS updates on the dispatcher, including skipped records and
skipped sealed-segment ranges. Keep seen/skipped classification accounting there;
merge successful hot insert/update/delete counts once per collected batch without
per-row shared atomics. Preserve immutable report accounting and saturation
behavior. Measure redo replay through final drain. Document that
`apply_and_dispatch_elapsed` remains consumer elapsed time excluding refill,
rather than summed worker CPU time.

Compare the existing release clean-reopen workload at baseline and with one,
two, and four pool workers, using fresh equivalent prepared roots per sample.
Use the unindexed fixture to isolate replay and indexed/checkpoint fixtures to
verify integrated results. Record bootstrap, replay, decode/refill, index rebuild,
configuration, allocator/version and tuning, and content verification. Use the
same allocator for baseline and candidate; compare both versions under each
allocator when investigating allocation costs. No fixed CI speedup threshold;
investigate material regressions and report cases limited by parsing, allocation, I/O, or
single-page skew. New benchmark fixture types remain outside this task.

## Implementation Notes

Implemented the page dispatcher in `recovery/dispatch.rs` and connected the
existing thread pool through transaction-system bootstrap. The scheduler owns
bounded active page/table bookkeeping, FIFO ready work, exclusive completion
observers, and retained page insertion history. Each batch allocates its operation
vector once with the configured operation capacity. Submission slots remain held
through completion collection. Admission rechecks capacity after both submission
and collection, so a submitted successor immediately frees its pending slot.

Hot replay now uses `Table::recover_row_batch`: one layout capture, one exclusive
page acquisition, ordered validation/mutation, and dirty marking for successful
mutations even when a later operation fails. Private synchronous
`recover_row_batch_to_page` and `recover_row_op_to_page` methods keep the batch
and per-operation error boundaries explicit. `RowReplayCounts` carries named
insert/update/delete counts through mutation, completion, and report aggregation;
report merges retain saturating arithmetic. Catalog replay, creation, and cold
deletes retain their serial classification paths. Eligible table/index DDL drains
its table. The common replay settlement boundary discards unsubmitted work,
drains accepted jobs, and merges native failures with Fatal precedence. EOF
finishes all replay before validation and serial index reconstruction. Input
waiting preserves one pinned read future across completion events. Stream
consumption, owned page jobs, tagged completion waiting, and batch admission
predicates use named functions or methods. Multi-step test setup, input
observation, and cancellation synchronization also use named helpers; small
callbacks and standard test executor wrappers remain inline.

Added deterministic coverage for page order and independent progress, task/page/
operation-count pressure, FIFO partial batches, mixed payload sizes,
completion-held submission slots, retirement/reactivation, retained
history after deletion, creation while older pages replay, active duplicate
registration, DROP and PageID reuse with an unrelated table blocked, cross-page
replacement and final unique/non-unique indexes, retained input futures, dirty
marking on failure, panic/rejection precedence, and actual bootstrap cancellation
with an accepted replay job. Test gates observe page ownership, job events, and
completion collection; they do not use timing sleeps. Shared fixtures and gates
cover the scheduler tests.

Initial implementation validation, before configuration extraction and byte-accounting removal:

- `rtk cargo fmt --check` and `rtk git diff --check`.
- `rtk cargo clippy --workspace --all-targets -- -D warnings`.
- `rtk cargo nextest run --workspace`: 2,086 passed.
- `rtk cargo nextest run -p doradb-storage --no-default-features --features libaio`:
  1,941 passed.
- `tools/style_audit.rs --diff-base origin/main`: passed, eight Rust files.
- Focused ordering, admission, DDL, replacement, and cancellation tests: 100/100
  stress iterations passed without retries.

The following release measurements preceded removal of replay byte accounting.

Release comparison (2026-09-17): baseline commit
`381921109c997c4ef108aafbab2080b3f86ea9c9`, saved before implementation;
`cargo build --release -p doradb-bench`, rustc 1.97.1, aarch64 Linux, default
io_uring backend, fresh `/tmp` tmpfs roots. The final samples below ran serially
without concurrent builds/tests. Each existing fixture prepared 1,000,000 rows,
128-byte values, four insertion threads, sixteen sessions, and 100-row transaction
batches. Indexed uses the existing unique-index template; checkpoint uses its
500,000-row freeze/checkpoint phase. Engine settings match `engine-defaults.toml`
(1 GiB data pool, 512 MiB index pool, 1 GiB readonly pool, fsync), with the stated
worker count; recovery read depth is 32 and redo block size is 4 KiB. Baseline
uses four configured pool workers but its hot replay remains sequential.

All twelve final runs passed full table fingerprints, row counts, and applicable
index verification: 1,000,000 rows each. The checkpoint runs replayed 499,584 hot
inserts. Times are milliseconds; decode is included in refill, and refill is
included in replay, so these columns must not be summed.

| Fixture | Implementation | Workers | Bootstrap | Replay | Decode | Refill | Index rebuild |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| unindexed | baseline | 4 | 334.05 | 332.51 | 86.52 | 155.08 | 0.43 |
| unindexed | pipeline | 1 | 416.53 | 415.11 | 178.40 | 263.69 | 0.37 |
| unindexed | pipeline | 2 | 448.83 | 447.38 | 225.68 | 312.16 | 0.38 |
| unindexed | pipeline | 4 | 456.90 | 455.45 | 232.55 | 320.19 | 0.36 |
| indexed | baseline | 4 | 641.75 | 322.82 | 83.35 | 146.80 | 317.74 |
| indexed | pipeline | 1 | 737.67 | 410.79 | 174.61 | 258.28 | 325.82 |
| indexed | pipeline | 2 | 779.71 | 451.55 | 230.85 | 315.13 | 327.01 |
| indexed | pipeline | 4 | 797.41 | 470.15 | 220.53 | 306.31 | 325.82 |
| checkpoint | baseline | 4 | 247.96 | 246.61 | 80.99 | 146.21 | 0.20 |
| checkpoint | pipeline | 1 | 293.21 | 292.03 | 132.39 | 206.08 | 0.17 |
| checkpoint | pipeline | 2 | 316.39 | 315.05 | 160.41 | 234.28 | 0.18 |
| checkpoint | pipeline | 4 | 320.46 | 319.08 | 161.22 | 239.70 | 0.21 |

These glibc samples show a regression: unindexed replay is 25–37%
slower, indexed replay is 27–46% slower, and checkpoint replay is 18–29% slower
than the sequential baseline. They are individual local samples, not confidence
intervals or a CI performance threshold. Preliminary independent samples showed
the same direction. The dominant increase is in serial decode/refill, despite
lower consumer apply/dispatch time in the unindexed multiworker cases (about
135 ms versus 177 ms baseline). Serial indexed reconstruction stays near
318–327 ms.

A separate `perf record -e cpu-clock -g --call-graph dwarf` run found substantial
coordinator allocator allocation/consolidation work and libc lock/futex stacks;
page mutation had much less sampled work. This is consistent with allocation
costs from decoding values on the coordinator and freeing them on workers, rather
than a useful parallelism gain on these inexpensive inserts. A controlled
`GLIBC_TUNABLES=glibc.malloc.tcache_count=0` comparison did not resolve the gap
(about 346 ms baseline versus 498 ms pipeline), so disabling tcache is not a fix.
The implementation retains the system allocator. The follow-up measurements
below test allocator and payload ownership independently of the production code.

Local plans, final result TOMLs, logs, and profiling evidence are under
`target/task-000309/` (ignored build artifacts); each `final-*.toml` is copied
from that run's verified benchmark output. Reproduction uses the existing
`doradb-bench/templates/recovery{,-indexed,-checkpoint}.toml` templates with an
`[engine.thread_pool] worker_threads = 1|2|4` overlay and a new root for every run.

### Allocator impact (2026-09-18)

**Allocator choice and the threads that allocate/free payloads are part of the
performance result.** A later 10-million-row unindexed comparison, after
configuration extraction and byte-accounting removal, confirmed that allocator
contention dominated the glibc regression. The coordinator decodes owned
`Vec<Val>` and heap-backed `MemVar` payloads; `replay_page_batch` drops those
coordinator-allocated rows on pool workers while the coordinator decodes more
groups.

Samply 0.13.1 captures at 1000 Hz, attached after preparation and old-engine
shutdown, attributed 28.2% of coordinator recovery CPU samples to glibc
`__lll_lock_wake_private` / `__lll_lock_wait_private` through malloc/free.
Worker stacks showed `replay_page_batch` dropping `ReplayOp` / `RowRedoKind`
through `__libc_free`. An isolated disposal-only prototype returned completed
batches in `BatchOutput` for collection and disposal on the coordinator. It
reduced sampled coordinator allocator-lock CPU from 1.112 s to 0.030 s;
three unprofiled confirmation runs per version reduced median replay from 4.174 s to
2.884 s. This prototype was diagnostic and was not applied to the working tree.
Coordinator CPU attribution excludes the post-recovery verification scan; sampled
CPU time is distinct from elapsed lock-wait time.

Then both original binaries were rerun with glibc and with Ubuntu's
`libjemalloc2` 5.3.0 (`5.3.0-2build1`) through `LD_PRELOAD`, using default
jemalloc settings (`MALLOC_CONF` unset). Loader bindings and process mappings
verified allocation calls used jemalloc. No rebuild or ownership change was
made for this comparison. Baseline remains commit
`381921109c997c4ef108aafbab2080b3f86ea9c9`; pipeline is the saved working-tree
implementation. Both release binaries use rustc 1.98.0 and the same Cargo.lock
on aarch64 Ubuntu 24.04, glibc 2.39, and the default io_uring backend.

Twelve serial, interleaved runs cover three fresh roots per version/allocator.
Each prepares 10,000,000 rows with no index, 128-byte values, four insertion
threads, sixteen sessions, and 100-row transactions. Both versions configure
two pool workers; pipeline limits are four in-flight batches, sixteen active
pages, and 256 operations per batch. Buffer sizes and fsync match the earlier
experiment. Roots use `/tmp` tmpfs with a clean reopen and no OS cache dropping.
Preparation, content scans, and shutdown are outside the recovery timer; no
profiler or build overlaps these timings. All runs verified the same full
content fingerprint and row count, with zero index entries inserted.

Medians in seconds (decode is nested in replay):

| Implementation | Allocator | Bootstrap | Replay | Decode |
| --- | --- | ---: | ---: | ---: |
| baseline | glibc | 4.954 | 3.359 | 0.835 |
| baseline | jemalloc | 4.607 | 2.975 | 0.601 |
| pipeline | glibc | 5.744 | 4.093 | 2.173 |
| pipeline | jemalloc | 3.566 | 2.014 | 0.691 |

jemalloc reduced pipeline bootstrap by 37.9% and replay by 50.8%. With jemalloc
on both versions, pipeline bootstrap was 22.6% faster and replay 32.3% faster
than baseline, reversing the glibc result. Full-invocation peak RSS remained
about 1.3–1.4 GiB, and no swap-out occurred. These local three-sample medians
establish allocator sensitivity for this workload, not a general speedup
guarantee. Future comparisons must report allocator/version and tuning alongside
worker counts and phase timings, and investigate allocation/free ownership when
parallel consumers increase serial decode time.

Reproduce from `doradb-bench/templates/recovery.toml` with 10,000,000 inserted
rows and `[engine.thread_pool] worker_threads = 2`, retaining a valid
`engine_defaults` path. The
[benchmark guide](../benchmark-tool.md#allocator-impact) documents installation,
preloading, and comparison controls. Local plans, canonical results, manifests,
profiles, and the diagnostic patch are retained under
`target/recovery-10m-20260918/{jemalloc,samply}/` (ignored artifacts).
Temporary datasets and redundant build caches were removed after validation.

## Impacts

- `doradb-storage/src/recovery/dispatch.rs`: new bounded scheduler, owned batch
  completion, active-page retirement, and deterministic scheduling tests.
- `doradb-storage/src/recovery/mod.rs`: dispatch eligible hot rows, preserve
  serial domains, register creation, impose table/global barriers, and settle
  failures before post-replay work.
- `doradb-storage/src/recovery/resources.rs` and `doradb-storage/src/trx/sys.rs`:
  carry the existing thread-pool dependency into startup recovery.
- `doradb-storage/src/recovery/row_state.rs`: document exclusive transfer between
  page history, active entry, and job without changing bitmap/frame semantics.
- `doradb-storage/src/table/recover.rs` and applicable helpers/tests in
  `doradb-storage/src/table/mod.rs`: one page acquisition per replay batch with
  existing validation, dirty marking, and typed failures.
- `doradb-storage/src/recovery/stream.rs` and `doradb-storage/src/engine.rs`:
  narrow test hooks/regressions for read-future retention and cancelled bootstrap
  where necessary; retain stream framing and engine lifecycle behavior.
- `docs/recovery.md`, `docs/shutdown-and-poison.md`, and recovery timing comments
  in `doradb-storage/src/stats.rs`: document ordering, waits, memory scope, and
  elapsed-time meaning.
- Main risks: single-page backpressure can stop the parser before unrelated
  later work; small batches add task overhead; large decoded groups and idle
  history still consume memory; slow inline operations delay completion
  collection; missing drain/handle release could break table destruction.
  The bounded state machine and targeted tests below are required mitigations.

## Test Cases

Use existing inline recovery fixtures and narrow `#[cfg(test)]` hooks/channels.
Synchronize on semantic events; never depend on parsing being faster than replay
or use sleeps to arrange progress. Reuse setup/assertion helpers and keep nextest's
configured watchdog behavior authoritative.

1. **Page order and concurrency:** split insert/update/delete histories across
   batches and transactions, include sparse/out-of-order RowIDs and variable-size
   updates, and gate one page while an independent page of the same table
   completes. Verify no page has overlapping jobs.
2. **Cross-page transactions:** recover transactions touching multiple pages and
   tables, including row replacements, and verify final rows and unique/non-unique
   indexes after the unchanged global reconstruction barrier.
3. **Task admission:** with one worker and task limit one, hold the first batch,
   fill pending capacity, and prove a second submission and further consumption
   stop at the appropriate predicate. Release it and verify resumed order.
   Separately use a task limit above worker count to prove all outstanding
   submissions, including queued jobs, count against admission.
4. **Active-page pressure:** use many pages with fewer rows than the batch target,
   a small page cap, and one worker. Assert the cap, force partial-batch progress
   without EOF, and verify a waiting new page is admitted only after retirement.
5. **Batch pressure:** cover operation-count boundaries and mixed variable-length
   payloads. Payload size must not force global draining; independent pages can
   progress while another page is blocked. Ensure full submission capacity alone
   permits bounded parsing ahead and that no new unbounded lookahead queue appears.
6. **Retirement/reactivation:** complete a page, remove its active entry, then
   replay later valid updates/deletes and fresh-slot inserts. Reject duplicate
   inserts, including after deletion, across retirement. Preserve empty created
   pages for reconstruction. Ready keys/table counters must not accumulate.
7. **Partial-batch fairness:** place an early partial batch ahead of a sustained
   stream of later full batches and verify FIFO dispatch once eligible. Delay
   its same-page predecessor separately to show the permitted late-completion
   case without violating order.
8. **DDL barriers and reuse:** hold submitted and partial pending work for table T
   when table/index DDL arrives; prove both replay before DDL. Keep table U's job
   blocked and prove it need not complete for T's DDL. Cover DROP releasing table
   handles and history before another table reuses the PageID.
9. **Creation and boundaries:** allocate a new page while older pages replay;
   preserve allocation/RowID order and reject duplicate registration in both
   idle and active states. Retain unknown-table, heap/pivot, deletion-cutoff,
   silent-watermark, root-proof, skipped-CTS, and cold-delete regressions.
10. **Input/completion races:** cover completion before waiter registration,
    out-of-submission-order results, completion while awaiting a multi-block redo
    group, and retained reader state across those wakes. No lost wake, duplicated
    result consumption, or read cancellation on ordinary worker completion.
11. **Failure ownership:** malformed payload/slot history after earlier successful
    mutations preserves dirty marking and fails recovery; parser/DDL failures
    with jobs outstanding drain them; worker panic/rejection preserves Fatal;
    ordinary error followed by Fatal during drain returns Fatal. No partial
    recovery reaches validation/index rebuild or redo finalization.
12. **EOF and cancellation:** flush all underfilled batches, consume final history
    once, preserve exact work/report accounting, and verify bootstrap cancellation
    drains accepted jobs before pool/storage teardown without leaked handles.
13. **Integration/performance:** run existing restart, checkpoint, catalog,
    index-lifecycle, mixed hot/cold, and eviction regressions, plus the release
    benchmark comparisons described above with complete content verification.

Required implementation validation:

- `rtk cargo fmt --check`
- `rtk cargo clippy --workspace --all-targets -- -D warnings`
- `rtk cargo nextest run --workspace`
- `rtk cargo nextest run -p doradb-storage --no-default-features --features libaio`
- `tools/style_audit.rs --diff-base origin/main`
- Focused stress runs without retries for new ordering/backpressure races when
  needed; use `.config/nextest.toml` for timeout/hang behavior.

## Open Questions

None blocking. Initial limits are performance defaults whose tuning must preserve
the stated bounds, FIFO eligibility, and progress rules. Parallel index building,
large-transaction streaming, and bounding/spilling retained replay history remain
separate work; this task makes no total-memory or maximum-latency guarantee.
