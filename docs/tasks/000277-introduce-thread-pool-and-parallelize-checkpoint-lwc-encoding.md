---
id: 000277
title: Introduce ThreadPool and Parallelize Checkpoint LWC Encoding
status: implemented
created: 2026-08-21
github_issue: 1000
---

# Task: Introduce ThreadPool and Parallelize Checkpoint LWC Encoding

## Summary

The engine now owns a fixed-size, crate-private `ThreadPool` for short,
finite, synchronous CPU computations. It accepts jobs through an unbounded
`flume` channel, returns the existing `Arc<Completion<T>>`, and lets accepted
jobs finish even when their observer is dropped. Task panic and unexpected
ingress loss publish typed Fatal errors and poison the engine.

User-table checkpoint is the first consumer. Once the mandatory runner has
copied a complete LWC block into an owned `LwcBuilder`, serialization,
compression, and checksum generation run on the CPU pool. A checkpoint-local
FIFO bounds pending encodes to the worker count, consumes results in logical
block order, and drains all accepted jobs before checkpoint termination.

`MandatoryRuntime` is now permanently single-runner. Its caller concurrency
limit and internal cleanup admission remain independent, so accepted async
obligations still make cooperative progress while true CPU parallelism is
isolated in `ThreadPool`.

## Context

Before this task, two configurable mandatory-runtime threads drove accepted
DDL, maintenance, and transaction-cleanup futures. Long synchronous regions
could occupy those async runners, and increasing the runner count neither
isolated CPU consumption nor exposed algorithm-level parallelism.

The motivating checkpoint workload inserted 2,100,000 deterministic 128-byte
rows, froze a 2,000,320-row prefix across 4,465 row pages, and checkpointed it
without retry waits. The pre-change median was approximately 604.6 ms across
five fresh roots. Profiling concentrated CPU time in `Table::build_lwc_blocks`,
`LwcBuilder::build`, and whole-block checksum hashing.

Page residency, visibility analysis, borrowed vector views, and sidecar
collection cannot move to synchronous workers because they may await IO or
retain engine guards. Final encoding can move safely after the builder owns
all input and no page guard, view, latch, logical lock, or IO request owner
crosses the submission boundary.

Issue Labels:

- type:task
- priority:medium
- codex

This task has no parent RFC and no source backlog. Related follow-ups that were
not source inputs remain:

- `docs/backlogs/000104-stream-parallel-create-index-cold-build.md`
- `docs/backlogs/000137-runtime-agnostic-blocking-work-abstraction.md`
- `docs/backlogs/000187-pipeline-checkpoint-lwc-encoding-and-data-writes.md`

## Goals

- Provide a configurable fixed CPU pool with two workers by default and reject
  a zero worker count before storage-root mutation.
- Keep submission synchronous and infallible at the API boundary while
  returning a typed completion for success or Fatal failure.
- Preserve accepted work after observer detachment and keep workers alive
  after task-body panic.
- Make partial startup and reverse shutdown join every started worker without
  cancelling accepted jobs.
- Parallelize only owned LWC encoding and checksum work while preserving
  checkpoint ordering, sidecars, publication, and durable bytes.
- Bound pending checkpoint encodes to the configured worker count and drain
  every accepted completion on success and error paths.
- Fix `MandatoryRuntime` to one runner while retaining caller capacity,
  internal cleanup, statistics, and shutdown ownership.
- Add strict benchmark configuration and normalized output for ThreadPool
  sizing and remove mandatory worker-count configuration.
- Demonstrate multi-worker CPU scaling with the 2-million-row checkpoint
  workload and a post-change process profile.

## Non-Goals

- Public or caller-provided task spawning.
- Generic task groups, semaphores, bounded queues, cancellation, deadlines,
  preemption, work stealing, affinity, NUMA policy, or runtime resizing.
- IO, async futures, channel waits, sleeps, locks, or latches inside a pool
  task; the worker receive loop is the only blocking executor operation.
- ThreadPool statistics or additional engine shutdown blockers.
- Moving CREATE INDEX, catalog or deletion checkpoint, recovery, purge,
  rollback, or MemIndex cleanup onto the pool.
- Parallel page access, visibility analysis, sidecar collection, storage
  writes, index construction, or root publication.
- LWC format, compression, checksum, table-root, redo, MVCC, recovery, or
  public checkpoint behavior changes.
- A CI performance threshold or edits to historical RFC-0026.

## Rejected Alternatives

### Reuse MandatoryRuntime for CPU parallelism

More mandatory runners would keep synchronous encoding on async scheduling
threads and couple accepted-operation capacity to CPU parallelism. The shipped
design separates one cooperative orchestration runner from a dedicated CPU
executor.

### Add a checkpoint-specific worker pool

A private checkpoint pool would duplicate configuration, startup rollback,
poison, completion, and shutdown behavior. The engine-level executor remains
small and generic, while checkpoint alone owns concurrency limiting and output
ordering.

## Plan

### Configuration and runtime boundary

`ThreadPoolConfig` is part of `EngineConfig`, defaults to two workers, and is
validated before filesystem mutation. It is publicly constructible for engine
configuration, but the pool and submission API remain crate-private.

`MandatoryRuntimeConfig` retains only `concurrency_limit`, defaulting to four.
`MandatoryRuntimeWorkers` always starts one `Mandatory-Runtime-1` runner.
Several accepted futures may remain live and make progress when they await or
yield, while synchronous CPU work belongs to ThreadPool.

### ThreadPool execution and failure model

`ThreadPool` owns the configured worker count, a direct unbounded sender, and a
guard to `EnginePoisoner`. `ThreadPoolWorkers` separately owns startup state
and join handles. Workers are named `ThreadPoolWorker-1` through
`ThreadPoolWorker-N`.

Submission checks the poisoner's atomic healthy path, then sends directly
without an external mutex. A racing poison may therefore admit bounded extra
finite work. Once poison is observed, submission reuses the cached shared
Fatal; disconnected ingress publishes `ThreadPoolUnavailable` and returns an
already-failed completion.

Each type-erased job owns its `FnOnce() -> T` and producer completion. The task
body alone is caught with `catch_unwind`. Panic publishes
`ThreadPoolTaskPanic`, poisons before waking observers, releases job input once,
and returns the worker to its receive loop.

Partial startup sends one FIFO stop for every worker already created and joins
all of them while preserving the spawn report. Reverse component shutdown
drains mandatory work first, sends one FIFO stop per CPU worker, and attempts
every join before propagating the first join panic.

### Checkpoint encoding flow

`LwcBuilder` owns `Arc<TableColumnLayout>` and consumes itself during final
encoding, making the submitted input `Send + 'static`. Page loading,
visibility filtering, row copying, block-boundary selection, and
secondary-index sidecar mutation remain on the mandatory runner.

`CheckpointLwcEncodeQueue` stores ordered shape/completion pairs and permits at
most one pending encode per configured worker. At capacity it consumes the
oldest result before submitting more work. Successful buffers are paired with
their retained shapes in FIFO order, independent of worker completion order.

Producer and drain results are resolved together. Any page, builder,
submission-completion, or encode failure stops new production but awaits every
already accepted task. Cleanup errors are merged without replacing an earlier
Fatal. Queue waits occur only after borrowed page views and guards leave scope.

### Benchmark and compatibility surface

Strict benchmark TOML accepts `[engine.thread_pool].worker_threads` and reports
the normalized value. Mandatory runtime output retains only
`concurrency_limit`; legacy explicit mandatory `worker_threads` is rejected as
an unknown field rather than silently ignored.

The component registry and active architecture documents record ThreadPool
core/worker ownership, direct poison behavior, accepted-work drain, and reverse
teardown order. `Engine::bootstrap` delegates construction to the module-level
`bootstrap_engine` helper so lifecycle logging remains separate from the
component build program.

## Implementation Notes

Implemented the engine-owned CPU pool, fixed single-runner mandatory runtime,
and ordered checkpoint encoding queue without changing persisted formats or
checkpoint publication semantics.

The final submission design intentionally differs from an early serialized
proposal: `ThreadPool` holds a direct sender and does not use a weak sender or
submission mutex. `EnginePoisoner` supplies the atomic fast check and cached
shared Fatal. This permits small racing over-commit after poison in exchange
for an uncontended healthy submission path.

The join-panic capture helper was retained as defensive ownership protection.
Although production task bodies are caught, worker output destruction, panic
payload destruction, thread-wrapper code, and test observers can still unwind
outside that boundary. Startup and shutdown therefore attempt every join
before suppressing or propagating the first payload.

The initial persistent Btrfs comparison across five fresh roots produced:

| Workers | Median | Min-Max | Sample CV |
| ---: | ---: | ---: | ---: |
| 1 | 579.3 ms | 474.5-943.8 ms | 30.91% |
| 2 | 573.5 ms | 358.2-708.4 ms | 23.45% |
| 4 | 599.4 ms | 293.6-831.0 ms | 41.93% |

Two workers improved the median by 0.99%, satisfying the planned comparison,
but storage variance masked CPU scaling. Eight alternating one/four-worker
pairs with root deletion and `sync` made four workers faster in seven pairs:
the medians were 444.7 ms and 272.4 ms respectively, a 38.8% reduction.

The tmpfs control isolated CPU/memory behavior. Median checkpoint time fell
from 410.9 ms with one worker to 257.4 ms with two and 208.4 ms with four,
improvements of 37.4% and 49.3%. Sample CV fell to 1.45% at four workers.

Linux `perf` data and SVG flamegraphs were used instead of the planned Samply
artifact. The main encode/scan cluster ended near 384.7, 258.8, and 181.2 ms
for one, two, and four workers. In the four-worker profile, pool workers
accounted for 49.35% of CPU samples and participated evenly; BLAKE3 hashing was
the largest named self-time hotspot at 25.54%. Temporary profile artifacts
were not added to the repository, so the durable measurements are recorded
here.

The apparent four-worker IO regression was not stable. Controlled four-worker
runs with reclaimed roots had a 262.1 ms median, 234.9-291.6 ms range, and
6.47% CV. Btrfs full-transaction commit counters did not advance during those
checkpoints. Retaining ten roots without intervening reclaim increased the
average from 317.8 ms for the first five to 595.5 ms for the last five. The
remaining evidence points to cumulative Btrfs or sparse virtual-disk backing
allocation and flush behavior, not ThreadPool imbalance.

Review identified a valuable next step: overlap ordered encode completion with
a bounded data-write window. It also preserved a pre-existing final-block edge
where extending a dense block shape after encoding can change the index
fingerprint already embedded in the LWC buffer. Both are captured with design
and acceptance context in backlog 000187.

Final verification completed with:

- mandatory branch-diff style audit: 17 Rust files passed, including workspace
  formatting and clippy with warnings denied;
- `cargo nextest run`: 1,763 tests passed across four binaries; and
- alternate libaio storage suite: 1,680 tests passed.

## Impacts

- Public engine configuration gains `ThreadPoolConfig`; mandatory runtime
  worker-count builders and validation are removed.
- Existing Rust or strict benchmark TOML that explicitly configures mandatory
  `worker_threads` must migrate to ThreadPool sizing or remove the field.
- Engine bootstrap creates two CPU workers by default and removes one of the
  former two mandatory runners, a net increase of one OS thread.
- ThreadPool task panic and unavailable ingress add two Fatal classifications;
  zero ThreadPool size adds one configuration classification.
- Checkpoint retains up to one builder/completion per worker plus its ordered
  encoded output, without retaining extra row-page guards during waits.
- Catalog LWC construction remains synchronous; its builder changes are
  ownership-only.
- Durable files, LWC bytes, recovery inputs, redo ordering, MVCC, transaction
  semantics, and public spawning APIs do not change.
- No dependency or unsafe-code baseline changes were introduced.

## Test Cases

- Validate ThreadPool and mandatory-runtime defaults, zero rejection, strict
  benchmark merging, normalization, and legacy-field rejection.
- Prove named workers execute CPU tasks in parallel and move non-Clone output
  exactly once through exclusive completion consumption.
- Prove observer detachment does not cancel accepted success or panic work and
  owned task input is released exactly once.
- Verify task panic publishes cached `ThreadPoolTaskPanic`, poisons before
  completion, and leaves the worker able to process queued work.
- Verify observed poison skips execution and unavailable ingress publishes an
  already-completed `ThreadPoolUnavailable` handle.
- Inject every worker spawn failure and verify earlier workers stop and join
  before bootstrap returns; verify shutdown attempts every join before panic
  propagation.
- Prove the fixed mandatory runner supports cooperative caller overlap and
  internal cleanup progress under saturated caller admission.
- Verify the checkpoint queue bound, FIFO output under out-of-order completion,
  and complete draining after producer, encode, or Fatal failure.
- Prove checkpoint capacity waits retain no page view or guard and preserve
  sidecar callbacks exactly once across block splits.
- Compare one, two, and four workers for identical LWC bytes, checksums, row
  shapes, pivot, replay floor, and column-index routes.
- Preserve heartbeat, empty-page, delay, retry, cancellation, deletion,
  recovery, shutdown, poison, DDL, and transaction-cleanup behavior.
- Run standard and libaio nextest suites plus the mandatory style gate.

## Open Questions

- Backlog
  `docs/backlogs/000187-pipeline-checkpoint-lwc-encoding-and-data-writes.md`
  covers encode/write pipelining, bounded write depth, phase timing, and the
  final dense-block fingerprint edge.
- Backlog `docs/backlogs/000104-stream-parallel-create-index-cold-build.md` may
  adopt ThreadPool for separately designed CREATE INDEX work.
- Backlog
  `docs/backlogs/000137-runtime-agnostic-blocking-work-abstraction.md` remains
  the scope for blocking work and future runtime-agnostic execution.
