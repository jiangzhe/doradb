---
id: 000309
title: Pipelined Recovery with Parallel Page Replay
status: implemented
created: 2026-09-17
github_issue: 1076
---

# Task: Pipelined Recovery with Parallel Page Replay

## Summary

Implemented bounded parallel hot-row replay on the existing engine thread pool,
overlapping page application with the serial redo decoder and coordinator.
Operations retain consumed order within each page lifetime. Table/index DDL drains
its table; EOF drains all jobs before validation, index reconstruction, and redo
repair. Recovery settings now live in `EngineConfig.recovery`.

## Context

Source Backlogs:

- docs/backlogs/closed/000087-refactor-recovery-process-parallel-log-replay.md

Issue Labels:

- type:perf
- priority:medium
- codex

There is no parent RFC. This task builds on recovery-owned insertion history from
[task 000305](000305-remove-recovery-maps-from-buffer-frames.md), recovery reporting
and benchmarks from [task 000306](000306-recovery-benchmark-and-startup-metrics.md),
and finite asynchronous pool jobs from
[task 000308](000308-support-finite-sync-and-async-thread-pool-jobs.md).

Previously the coordinator awaited each row mutation before consuming more redo.
Startup has no admitted foreground readers or surviving transactions, hot row
mutations affect one physical page, and indexes consume final row images after
replay. These constraints permit independent pages of one transaction or table
to execute concurrently without adding per-row recovery timestamps. Transaction
framing, checkpoint replay floors, and the global recovered-CTS watermark remain
unchanged. Durable contracts are in [Recovery](../recovery.md) and
[Shutdown and Engine Poison](../shutdown-and-poison.md).

## Goals

- Overlap decoding/dispatch with independent hot-page application while preserving
  page order, payload validation, insertion history, and page lifetimes.
- Bound outstanding submissions, active scheduling entries, and operations per
  batch; make progress with partial batches and retire idle scheduler entries.
- Fence eligible table/index DDL and final reconstruction with the appropriate
  drains, using existing completion, poison, and bootstrap rollback contracts.
- Verify correctness under controlled scheduling and report measured performance,
  including regressions and allocator sensitivity.

## Non-Goals

- Parallel decoding, catalog replay, cold deletes, or index construction; parsing
  ahead past blocked DDL; transaction-level replay publication.
- Changes to persistent formats, checkpoint publication, redo retention, replay
  eligibility, normal MVCC, or buffer-frame metadata.
- A new pool or general pool queue limit; total recovery memory or latency bounds.
  Decoded groups, recovered pages, and insertion history remain separate costs.
- A production allocator replacement or payload-recycling implementation; that
  investigation is deferred to backlog 000202.

## Rejected Alternatives

- Fixed worker-owned page partitions introduce false dependencies between
  unrelated pages and make skew harder to handle than independent page jobs.
- Parsing past blocked DDL with a dependency graph adds deferred metadata and
  lifetime machinery outside this bounded scheduling change.
- Retaining scheduling entries for every created page makes new scheduler overhead
  scale with all recovered pages; insertion history remains separate instead.

## Plan

### Ownership and bounded scheduling

`ReplayDispatcher` owns retained table/page history, active pages keyed by named
`PageKey` fields, table work counters, a FIFO ready queue, and tagged completion
futures. Each bitmap belongs exclusively to history, an active entry, or its
submitted job/result. Pending operations never mutate it.

Each page has at most one outstanding job and one pending operation vector.
Submission moves owned payloads and the bitmap without cloning. Collection either
queues the pending successor or retires the active entry and restores history.
Empty created pages and previously deleted slots retain their insertion history
until DROP or final reconstruction. Idle table counters and ready keys retire.

`RecoveryConfig` holds startup I/O, validation, and replay sizing. Named
`DEFAULT_RECOVERY_*` constants define defaults:

| Setting | Default |
| --- | --- |
| `io_depth` | 32 |
| `disable_dml_validation` | `false` |
| `max_in_flight_batches` | `None`: twice the pool worker count |
| `max_active_pages` | `None`: four times the effective submission limit |
| `max_batch_ops` | 256 |

Engine validation rejects zero counts and resolves automatic limits into `Some`
values with saturating arithmetic before filesystem changes. An explicit batch
limit also controls automatic page sizing. Reset a validated optional limit to
`None` to recompute it after changing worker sizing. Benchmark overlays use
`[engine.recovery]`; canonical results record concrete limits.

Submission credits cover queued, executing, waiting, and completed-but-uncollected
jobs. Each batch reserves its operation capacity on first admission. Counts bound
scheduler state and retained operations; payload sizes are not measured for
admission, and there is no explicit byte budget.

### Replay, progress, and barriers

The coordinator retains decoding, timeline updates, eligibility filtering, catalog
operations, page creation, cold deletion, and DDL. Eligible hot payloads move to
page batches in consumed order. Each worker captures the layout and acquires the
page exclusively once, applies ordered mutations, and marks successful changes
dirty even if a later operation fails. No scheduler wait occurs while latched.

`reap` collects immediately available completions; `pump` submits FIFO eligible
batches. Progress flushes partial batches at transaction boundaries and whenever
admission, drains, or input waits require it. Pressure stops at the unadmitted row
and rechecks capacity after submission/collection before waiting. A missing
registered hot-page history is an integrity error, not a retry condition.

The pool publishes a terminal completion after dropping job captures. Waiters
retain page identity, register before rechecking the sticky result, and collect
in completion order. Input waiting preserves one pinned read future across worker
completion events, because cancelling and restarting a group read can lose parser
state. Wakes do not preempt synchronous decoding or inline coordinator operations.

Eligible CreateTable, DropTable, CreateIndex, and DropIndex call `drain(table_id)`
before mutation. Both pending operations and submitted batches must reach zero;
other tables can progress without becoming part of the barrier predicate.
CreateRowPage stays in redo allocation order without draining older pages.
Explicit allocation rejects duplicate PageIDs before replay state initialization.
DROP releases worker-held table handles and removes history before PageID reuse.
DataCheckpoint and silent-watermark records retain their existing filtered paths.

`drain_all` completes EOF work before validation, serial hot-index reconstruction,
and redo finalization. Every replay-loop error discards unsubmitted work and
settles accepted jobs; Fatal during settlement takes precedence while preserving
original poison context. Cancellation drops observers and pending state, leaving
accepted captures pool-owned. Registry rollback drains the pool while storage and
eviction remain live. These waits use the existing pool-job completion family.

## Implementation Notes

Implemented bounded page replay with completion-owned admission credits, table
DDL barriers, retained insertion history, and a common failure-settlement boundary.
The existing thread pool is passed through transaction-system bootstrap without
changing component startup/teardown order. Successful batch counts merge into the
immutable recovery report once; replay timing includes the final drain.

### Review outcomes and deviations

- Extracted startup I/O depth and DML validation settings from `TrxSysConfig` into
  public `RecoveryConfig`, together with the three configurable replay limits.
  Benchmark overlays and normalized results follow the same configuration split.
- Removed proposed byte limits: shallow operation sizes do not account for nested
  payload allocations. Limits now bound counts without implying a memory budget.
- Kept validation at its owning execution path: duplicate creation is rejected by
  explicit page allocation, and page grouping follows the redo identity. Row-range,
  slot-history, payload, and mutation checks remain intact.
- Verified checkpoint filtering precedes hot-page lookup. Inserts/updates below
  the published pivot are skipped even if their CTS exceeds the heap floor; cold
  deletes ignore obsolete optional hot PageIDs. Eligible hot redo requires history.
- The production path still frees coordinator-allocated payloads on workers and
  uses the system allocator. Its measured allocator regression is explicitly
  deferred to [backlog 000202](../backlogs/000202-recovery-payload-allocation-bottleneck-bulk-recycling.md).

### Verification

The final implementation passed 2,092 workspace tests, formatting, and strict
workspace/all-targets Clippy. Resolution reran the branch style gate against
`origin/main` (22 Rust files) and the alternate libaio suite (1,944 tests), all
passing. Fourteen focused admission/checkpoint/page-identity tests also passed
during review. Earlier ordering, admission, DDL, replacement, and cancellation
coverage passed 100/100 stress iterations without retries, before configuration
extraction and byte-limit removal. Tests synchronize on ownership, job events,
and completion collection rather than timing sleeps.

### Initial performance findings

The 2026-09-17 one-million-row release comparison used the existing unindexed,
unique-index, and checkpoint fixtures on aarch64 Linux, rustc 1.97.1, glibc, and
io_uring. Against sequential commit `381921109c997c4ef108aafbab2080b3f86ea9c9`,
pipeline replay with one/two/four workers was 25–37% slower without indexes,
27–46% slower with an index, and 18–29% slower after checkpointing. Decode/refill
grew while serial index reconstruction remained near 318–327 ms. All twelve
samples passed complete row/index verification; these individual samples preceded
byte-accounting removal and are not a speedup guarantee or CI threshold.

Each fixture used 128-byte values, four preparation threads, sixteen sessions,
100-row transactions, fsync, and fresh tmpfs roots. The checkpoint fixture froze
500,000 rows and replayed 499,584 hot inserts. Separate profiling found allocator
allocation/consolidation and lock stacks; disabling glibc tcache did not resolve
the gap. Local plans/results/profiles remain under `target/task-000309/`.

### Allocator impact (2026-09-18)

**Allocator identity and allocation/free ownership are part of a performance
result.** A later ten-million-row unindexed comparison, after configuration
extraction and byte-accounting removal, found substantial contention when the
coordinator decoded `Vec<Val>` and heap-backed `MemVar` payloads and workers freed
them while more decoding proceeded.

Samply 0.13.1 at 1 kHz attributed 28.2% of coordinator recovery CPU samples to
glibc `__lll_lock_wake_private` / `__lll_lock_wait_private` through malloc/free.
An isolated disposal-only prototype returned completed batches to the coordinator:
sampled coordinator allocator-lock CPU fell from 1.112 s to 0.030 s, and separate
three-run unprofiled medians reduced replay from 4.174 s to 2.884 s. That diagnostic
prototype was not applied. Samples exclude post-recovery verification and measure
CPU attribution, not elapsed lock-wait time.

A separate twelve-run matrix compared both original binaries under glibc 2.39 and
Ubuntu jemalloc 5.3.0 (`5.3.0-2build1`) via per-process `LD_PRELOAD`, with
`MALLOC_CONF` unset and loader bindings/process mappings verified. Medians over
three fresh roots per version/allocator are seconds; decode is nested in replay:

| Implementation | Allocator | Bootstrap | Replay | Decode |
| --- | --- | ---: | ---: | ---: |
| baseline | glibc | 4.954 | 3.359 | 0.835 |
| baseline | jemalloc | 4.607 | 2.975 | 0.601 |
| pipeline | glibc | 5.744 | 4.093 | 2.173 |
| pipeline | jemalloc | 3.566 | 2.014 | 0.691 |

Jemalloc reduced pipeline bootstrap by 37.9% and replay by 50.8%. With jemalloc on
both versions, pipeline bootstrap was 22.6% faster and replay 32.3% faster than
baseline. Full-invocation peak RSS was about 1.3–1.4 GiB, with no swap-out. These
results establish sensitivity, not a universal allocator ranking. The disposal
prototype and allocator matrix are separate experiments, not a direct comparison.

Both matrix binaries used rustc 1.98.0, the same Cargo.lock, aarch64 Ubuntu 24.04,
and io_uring. The pipeline binary is a saved working-tree snapshot predating later
source refactors; baseline is the commit identified above. Runs used 10,000,000
rows, no index, 128-byte values, two pool workers, and pipeline limits of four
in-flight batches, sixteen active pages, and 256 operations per batch. Preparation
used four threads, sixteen sessions, and 100-row transactions; pools were 1 GiB
data, 512 MiB index, and 1 GiB readonly, with fsync and recovery I/O depth 32.
Fresh tmpfs roots used clean reopen and warm caches; preparation, verification,
and shutdown were outside recovery timing, with no overlapping builds/profilers.
Every run verified matching full content, row counts, and zero index entries.

The exact jemalloc mechanism was not isolated: no jemalloc Samply capture was
collected. Batched freeing versus arenas/thread-local caching remains a hypothesis
for backlog 000202, which compares other allocators and prefers recovery-owned
batches with bulk recycling without requiring global allocator replacement.

[Benchmark guidance](../benchmark-tool.md#allocator-impact) records comparison
controls and preload verification. Reproduce from the tracked recovery benchmark
template with the settings above. Local manifests, binaries, profiles, reports,
and the diagnostic patch remain under `target/recovery-10m-20260918/`; ignored
artifacts may be unavailable elsewhere. Temporary datasets and redundant build
caches were removed. Production allocator policy remains unchanged.

## Impacts

Recovery now depends on finite pool jobs while preserving startup ordering,
persistent formats, checkpoint semantics, serial cold/catalog work, and final
index reconstruction. The public configuration split removes the old transaction
recovery fields/builders; benchmark plans reject the old transaction keys and
require a concrete `recovery` table in normalized results. Removed byte-limit keys
are also rejected. Recovery documentation explains count bounds and elapsed-time
metrics; benchmark guidance records allocator effects. Single-page skew, large
payloads/groups, retained history, and allocator contention remain limitations.

## Test Cases

- Page-local insert/update/delete order, independent-page progress, cross-page
  replacement, and correct final unique/non-unique indexes.
- Task/page/operation pressure, FIFO partial batches, immediate pending-capacity
  reuse, completion-held credits, retirement/reactivation, and retained history.
- Table DDL fences, concurrent unrelated work, duplicate creation in idle/active
  states, DROP handle release, and PageID reuse with fresh history.
- Checkpoint pivot/floor filtering, cold-delete idempotence and obsolete PageIDs,
  unknown-table rules, silent watermarks, and global recovered CTS.
- Completion-before-registration, retained input futures, dirty marking after
  partial failure, ordinary/Fatal settlement, panic/rejection, and real bootstrap
  cancellation with an accepted job.
- Recovery configuration defaults, overrides/automatic sizing, zero-limit rejection
  before filesystem changes, strict benchmark overlays, and resolved round trips.
- Existing restart, catalog, checkpoint, index lifecycle, mixed hot/cold, eviction,
  report-accounting, and full-content benchmark verification on the stated suites.

## Open Questions

No blocking implementation questions remain. Allocator and reclamation analysis
is tracked by [backlog 000202](../backlogs/000202-recovery-payload-allocation-bottleneck-bulk-recycling.md).
Parallel hot-index construction remains
[backlog 000110](../backlogs/000110-unify-hot-row-mem-scan-index-build-recovery.md),
and large-transaction/group streaming remains
[backlog 000130](../backlogs/000130-large-redo-transaction-streaming-replay.md).
