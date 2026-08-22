---
id: 000277
title: Introduce ThreadPool and Parallelize Checkpoint LWC Encoding
status: proposal
created: 2026-08-21
github_issue: 1000
---

# Task: Introduce ThreadPool and Parallelize Checkpoint LWC Encoding

## Summary

Add a crate-private, engine-owned `ThreadPool` for short, finite,
synchronous CPU-bound computations. The pool uses `flume` only for its
unbounded job ingress and returns the existing `Arc<Completion<T>>` as each
task handle. Accepted tasks are not cancelled when their completion observer
is dropped. A task panic or unexpected loss of pool ingress poisons the engine
and publishes a typed Fatal failure through the same completion.

Use the pool first to parallelize user-table checkpoint LWC serialization,
compression, and checksum generation. Checkpoint owns a private FIFO of
submitted encodes, limits its in-flight work to the configured pool size, and
consumes completions in logical block order. Page access, visibility analysis,
secondary-index sidecar collection, table-file IO, and root publication remain
on the asynchronous mandatory runtime.

As part of the scheduling boundary, make `MandatoryRuntime` permanently
single-runner. Remove its `worker_threads` configuration while retaining its
bounded caller `concurrency_limit`. True CPU parallelism belongs to
`ThreadPool`; the mandatory runner remains responsible for cooperative async
orchestration and accepted-work ownership.

## Context

The engine currently runs accepted DDL, maintenance, and transaction cleanup
on `MandatoryRuntime`, an `async_executor::Executor` driven by two configurable
OS threads by default. One accepted task can execute a long synchronous region
between await or yield points. Increasing mandatory runner count permits more
such regions to overlap, but does not isolate CPU consumption from the async
scheduler or provide algorithm-level parallelism.

The 2026-08-21 checkpoint analysis used the public `doradb-bench` maintenance
workload with 2,100,000 inserted 128-byte rows and a 2,000,000-row freeze
budget. The actual frozen prefix contained 2,000,320 rows across 4,465 row
pages. Five fresh-root release runs completed in 505.1-922.2 ms, with a
604.6 ms median, approximately 3.31 million rows/second at the median, and no
checkpoint retry waits.

The accompanying process profile found the checkpoint CPU path concentrated
in `Table::build_lwc_blocks`, `LwcBuilder::build`, and block checksum hashing.
Inclusive profile shares were approximately 57.1%, 49.2%, and 33.8%
respectively. The profile also showed the checkpoint computation concentrated
on a mandatory-runtime runner. Percentages include overlapping frames and
normal process teardown, but the call stacks identify owned LWC encoding and
checksumming as the dominant safe parallelization boundary.

`Table::build_lwc_blocks` cannot move wholesale to synchronous workers. It
awaits row-page residency and acquires page guards before constructing borrowed
vector views. Those operations may perform IO or wait on engine-managed state.
Once `LwcBuilder` has copied a block's visible values into owned buffers,
however, its final encode and checksum step requires no IO, page guard, logical
lock, or wait. This task uses that narrow ownership boundary.

The active engine lifetime design already identifies a separate CPU pool as
future work requiring workload evidence. The pool must fit the existing
component registry, reverse shutdown, engine poison, and accepted-service
completion contracts rather than reintroducing the retired thread-pool async
IO fallback.

Issue Labels:

- type:task
- priority:medium
- codex

Related Backlogs, not source inputs:

- `docs/backlogs/000104-stream-parallel-create-index-cold-build.md`
- `docs/backlogs/000137-runtime-agnostic-blocking-work-abstraction.md`

The first backlog may use `ThreadPool` in a separately designed task. The
second concerns blocking filesystem operations and remains deliberately
separate because this task's jobs prohibit IO and waiting.

Relevant repository references:

- `docs/architecture.md`
- `docs/checkpoint.md`
- `docs/data-checkpoint.md`
- `docs/engine-component-lifetime.md`
- `docs/shutdown-and-poison.md`
- `docs/process/coding-guidance.md`
- `docs/process/unit-test.md`
- `docs/benchmark-tool.md`
- `doradb-storage/src/component.rs`
- `doradb-storage/src/engine.rs`
- `doradb-storage/src/runtime/mandatory.rs`
- `doradb-storage/src/completion.rs`
- `doradb-storage/src/table/persistence.rs`
- `doradb-storage/src/lwc/mod.rs`
- `doradb-bench/src/engine_config.rs`

Approved design constraints are:

- configure only the CPU pool's worker count;
- run only finite synchronous computation in pool tasks;
- do not support task cancellation;
- do not add a generic semaphore or generic bounded task queue;
- use `flume` for async-to-worker job ingress and the existing `Completion`
  for result observation;
- poison the engine on task panic;
- keep concurrency limiting and output ordering private to checkpoint; and
- remove mandatory-runtime worker-count configuration and fix it to one
  runner without editing historical RFC or completed task documents.

## Goals

- Add a crate-private `ThreadPool` component with a nonzero, immutable,
  configurable fixed worker count and a default of two workers.
- Use an unbounded `flume` channel for synchronous, nonblocking job acceptance.
- Return `Arc<Completion<T>>` directly from infallible task submission.
- Continue accepted task execution when the completion observer is dropped.
- Convert task-body unwind to `FatalError::ThreadPoolTaskPanic`, poison the
  engine before publishing completion, and keep the worker alive.
- Convert unexpected closed or unavailable pool ingress to
  `FatalError::ThreadPoolUnavailable`, poison the engine, and return an already
  failed completion rather than panicking or adding a submit result.
- Encode partial-startup rollback and reverse shutdown so every started worker
  is signalled and joined without cancelling accepted jobs.
- Make `LwcBuilder` movable into a `'static` CPU task without moving page
  guards, borrowed vector views, locks, or IO capabilities.
- Add a checkpoint-private FIFO that bounds in-flight LWC encode jobs to the
  pool worker count, consumes results in submission order, and drains all
  accepted work before the checkpoint attempt reaches terminal state.
- Preserve LWC bytes, block shapes, RowID ordering, secondary-index companion
  state, checkpoint outcomes, publication semantics, and persisted formats.
- Remove `MandatoryRuntimeConfig::worker_threads` and drive
  `MandatoryRuntime` with exactly one fixed runner while preserving caller and
  internal admission semantics.
- Extend `doradb-bench` configuration and normalized output for
  `ThreadPoolConfig`, and remove mandatory worker count from that strict
  schema.
- Re-run the 2-million-row checkpoint comparison and produce a post-change
  flamegraph demonstrating multi-worker LWC encoding.

## Non-Goals

- Public or caller-supplied task spawning.
- A generic semaphore, generic task group, generic bounded task queue, or
  reusable checkpoint queue abstraction.
- Task cancellation, forced shutdown, deadlines, preemption, or resumable
  tasks.
- Blocking filesystem work, storage IO, async futures, lock acquisition,
  latch acquisition, channel waits, sleeps, or other blocking work inside a
  production pool task.
- Priority lanes, work stealing, affinity, NUMA policy, adaptive sizing,
  runtime resizing, or fairness guarantees.
- Thread-pool statistics or new `Session` statistics APIs.
- Separate ThreadPool task accounting in engine shutdown blockers or
  `try_shutdown` diagnostics.
- CREATE INDEX, catalog checkpoint, deletion checkpoint, recovery, rollback,
  purge, MemIndex cleanup, or other maintenance migration.
- Parallel page loading, visibility analysis, secondary-index sidecar
  collection, table-file allocation, storage writes, or root publication.
- LWC format, compression algorithm, checksum algorithm, table-root, redo,
  MVCC, recovery, or durable metadata changes.
- A hard performance threshold in CI.
- Editing RFC-0026 or any other historical RFC or completed task document.
- Closing either related backlog as part of this task.

## Rejected Alternatives

### Reuse MandatoryRuntime for CPU parallelism

Adding mandatory runners or spawning child futures on the existing executor
would leave synchronous encoding on async scheduling threads. It would neither
isolate CPU resources nor establish a contract that excludes IO and waiting.
It also couples accepted-operation concurrency to computation parallelism.
The approved design instead fixes mandatory orchestration to one runner and
puts true CPU parallelism behind a separate component.

### Add a checkpoint-specific worker pool

A checkpoint-only worker pool could encode LWC blocks with fewer engine-level
interfaces, but it would duplicate configuration, startup rollback, poison,
completion, and shutdown behavior when another proven CPU-heavy consumer is
added. The approved task introduces one small generic CPU executor while
keeping only the concurrency and ordering policy checkpoint-specific.

## Plan

### Configuration and public builder surface

1. Add `ThreadPoolConfig` in `doradb-storage/src/conf/engine.rs` with:
   - public `worker_threads: usize`;
   - `Default` value `2`;
   - a consuming `worker_threads(...)` builder;
   - validation that rejects zero with
     `ConfigError::InvalidThreadPoolWorkerThreads`.
2. Add `EngineConfig::thread_pool` and a consuming `thread_pool(...)` builder.
   Validate the pool config before any filesystem mutation during engine
   bootstrap.
3. Export `ThreadPoolConfig` through `conf/mod.rs` and the crate root beside
   `MandatoryRuntimeConfig`.
4. Remove `worker_threads` and its builder from `MandatoryRuntimeConfig`.
   Retain only `concurrency_limit`, defaulting to four, and validate only that
   value.
5. Remove `ConfigError::InvalidMandatoryWorkerThreads`. Unknown benchmark TOML
   containing the removed mandatory leaf must fail strict deserialization.

### ThreadPool core and workers

6. Add `doradb-storage/src/runtime/thread_pool.rs` and register it from
   `runtime/mod.rs`.
7. Implement a `ThreadPool` core component that owns:
   - its configured worker count;
   - the direct `flume::Sender<ThreadPoolMessage>`; and
   - a direct `QuiescentGuard<EnginePoisoner>` for the atomic health fast path,
     cached poison retrieval, and enqueue-failure poison.
8. Use `flume::unbounded` for the job channel. Submission reads the poisoner's
   atomic flag and locks only when poison is already published, then performs
   the nonblocking send directly without an external submission mutex. Poison
   may race the fast check and admit bounded extra finite work.
9. Type-erase jobs behind a private `ThreadPoolJob` interface. A generic job
   owns its `FnOnce() -> T` and one producer-side `Arc<Completion<T>>`. Its
   execute method must move the completion outside the unwind-caught task-body
   closure before invoking the `FnOnce`, so a task panic cannot destroy the
   only producer handle before Fatal completion is published.
10. Implement the crate-private submission surface as:

    ```rust
    fn submit<T, F>(&self, task: F) -> Arc<Completion<T>>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static;
    ```

    A successful synchronous flume send is task acceptance. Normal execution
    publishes `Ok(output)` to the completion. The task owns the producer Arc,
    so dropping the returned Arc only detaches observation and never cancels
    execution.
11. If the fast check observes poison, complete the returned handle with the
    poisoner's cached shared Fatal without sending. If `send` reports
    disconnection, create and publish `FatalError::ThreadPoolUnavailable`,
    poison through the core's poisoner, complete the returned handle with the
    same shared Fatal bridge, and return it. `submit` itself remains infallible.
12. Add `ThreadPoolWorkers` as the separate join-handle owner. `ThreadPool`
    supplies a pending startup provision containing the receiver, poisoner,
    core guard, and fixed worker count.
13. Start `Thread-Pool-1` through `Thread-Pool-N` with the repository's named
    thread helper. Each worker blocks only in the executor's `recv` loop; this
    idle receive is not a user task.
14. Wrap each task body, not the complete job owner, in
    `catch_unwind(AssertUnwindSafe(...))`. On unwind:
    - format only pool-owned facts: worker name and panic-payload description;
    - create `FatalError::ThreadPoolTaskPanic`;
    - poison the engine before waking the completion;
    - publish the shared Fatal bridge through `Completion`; and
    - continue the worker receive loop.
15. Keep task-specific identifiers out of `ThreadPool`. The caller that pairs
    a completion with its logical input owns and attaches table, RowID, or
    operation context when it consumes the completion.
16. Make pending worker startup failure-atomic. If any named spawn fails,
    enqueue one private FIFO stop message per worker already started, join all
    of them, preserve the spawn failure as the primary report, and let registry
    rollback release the passive core.
17. Register components in this relative order:

    ```text
    StorageRootLease
    -> EnginePoisoner
    -> ThreadPool
    -> ThreadPoolWorkers
    -> MandatoryRuntime
    -> existing storage components and worker owners
    ```

    Reverse teardown therefore drains and stops mandatory execution before
    ThreadPool stop signalling. `ThreadPoolWorkers::shutdown` enqueues one
    private FIFO stop message per worker, lets receivers finish every earlier
    accepted finite job, joins all workers, and propagates the first join panic
    only after every join was attempted. `ThreadPool::shutdown` remains
    passive.
18. Add the pool guard to `EngineCore`. Do not expose the pool through public
    `Engine`, `Session`, or transaction APIs.

### Fatal completion behavior

19. Add `FatalError::ThreadPoolTaskPanic` and
    `FatalError::ThreadPoolUnavailable`.
20. Reuse `SharedFatalError::into_completion_bridge` and the existing
    `Completion<T>` failure channel. Do not add a new task-handle wrapper,
    result channel, error bridge, or panic-resume path.
21. Update `completion.rs` module documentation only as needed to include CPU
    task completion among its supported engine flows. The completion state
    machine and exclusive-take behavior remain unchanged.
22. Use the poisoner's existing atomic flag as the submission fast check and
    its mutex-protected shared report as the cached failure. Once poison is
    observed, reject new pool submission with that cached Fatal. Submission
    racing poison may over-commit bounded extra finite work; already submitted
    computations drain normally.

### Fixed single-runner MandatoryRuntime

23. Remove configured runner count from `MandatoryRuntime::new`, pending worker
    startup, component shelf provisions, and worker construction.
24. Start exactly one runner named `Mandatory-Runtime-1`. Retain the existing
    executor, caller admission, separate internal admission, supervision,
    statistics, stop event, and completion-observer behavior.
25. Keep `concurrency_limit` independent from the single runner. Several
    accepted tasks may remain live and make cooperative progress when they
    await IO, a completion, or an explicit yield.
26. Remove production and test branches that expect
    `Mandatory-Runtime-2`. Tests that prove async overlap must use two futures
    yielding on the one executor runner; true simultaneous CPU execution is
    tested on `ThreadPool` instead.

### Owned LWC encoding input

27. Change `LwcBuilder` from borrowing `&TableColumnLayout` to owning
    `Arc<TableColumnLayout>`. Update its constructor and all catalog, table,
    LWC, and test call sites to clone the metadata-owned Arc.
28. Make final LWC encoding consume the builder, or otherwise expose an
    explicitly consuming method, so the complete owned scan buffer and column
    statistics move into one `'static` task and cannot be reused after
    submission.
29. Add a compile-time test assertion that the submitted builder/input and its
    result are `Send + 'static`.
30. Keep catalog LWC construction synchronous in this task. Its constructor
    adjustments are ownership-only.

### Checkpoint-private encode queue

31. Add private `CheckpointLwcEncodeQueue` and `PendingLwcEncode` types in
    `table/persistence.rs`. They are not exported from the table or runtime
    modules.
32. Construct one queue per checkpoint LWC build with:
    - a borrowed or cloned engine `ThreadPool` guard;
    - `max_in_flight = thread_pool.worker_threads()`;
    - the caller-owned `table_id`;
    - a FIFO `VecDeque` of shape/completion pairs; and
    - an ordered `Vec<LwcBlockPersist>` output.
33. Store `ColumnBlockEntryShape` beside
    `Arc<Completion<InternalResult<DirectBuf>>>`. The worker owns only the
    `LwcBuilder` and row-shape fingerprint; it does not receive table or RowID
    diagnostic metadata.
34. Before submitting while the queue length equals its limit, await and
    exclusively take the oldest completion. Convert a completion failure to
    `RuntimeOrFatalError` without replacing Fatal. Convert an inner
    `InternalResult` error to `RuntimeError::CheckpointExecution`. At this
    caller boundary, attach table ID and shape start/end RowIDs.
35. Pair each successful buffer with its retained shape and append it to output
    in FIFO order. Worker completion order must not affect persistent block
    ordering.
36. On the final builder, drain every remaining completion in the same FIFO
    order and return the ordered vector.
37. Structure production and drain results so every ordinary page-access,
    builder, submission-completion, or encode error stops new submission but
    still awaits all previously submitted completions. Merge later drain
    failures without replacing an earlier Fatal or losing the primary source.
    Do not rely on async Drop.
38. A pool task panic poisons immediately in the worker. When checkpoint
    consumes that Fatal completion it stops producing, drains the remaining
    accepted pure computations, and propagates Fatal through
    `TableCheckpointer::resolve`. The existing irreversible-checkpoint policy
    must preserve, not replace, an already-Fatal thread-pool reason.

### Checkpoint build-loop integration

39. Change `Table::build_lwc_blocks` to return
    `RuntimeOrFatalResult<Vec<LwcBlockPersist>>` and provide access to the
    engine ThreadPool through the accepted session runtime.
40. Keep these operations on the mandatory runner:
    - `must_get_row_page_shared(...).await`;
    - page/vector-view validation;
    - deletion-bitmap interpretation;
    - `LwcBuilder::append_view` and block-boundary selection;
    - visible-row callbacks and secondary-index sidecar mutation; and
    - all later mutable-file, IO, index, and publication work.
41. When an appended view overflows the current builder:
    - retain the completed builder and its shape;
    - leave the attempted view rolled back;
    - exit the lexical scope that owns the borrowed view and page guard;
    - make room in and submit to `CheckpointLwcEncodeQueue`; and
    - reacquire the same page and rebuild its view before appending it to the
      fresh builder.
42. Invoke the visible-row sidecar callback exactly once for each accepted row,
    even when the page view must be rebuilt after an LWC split.
43. Never await queue capacity or completion while retaining a page guard,
    vector view, latch, logical lock newly acquired by the build loop, or an IO
    request owner.
44. Preserve the existing final-block end-RowID adjustment, block-index input,
    write parallelism, sidecar ordering, checkpoint workflow state, and fatal
    boundary.

### Shutdown ownership

45. Add no ThreadPool active counter, permit, engine-shutdown blocker, or
    statistics surface. The production ownership rule is that every submitted
    CPU task is nested under an already shutdown-accounted foreground or
    mandatory operation, and that operation explicitly drains its local task
    handles before terminal publication.
46. Make checkpoint satisfy that rule on every normal and typed-error path.
    Dropping an arbitrary low-level completion remains valid and does not
    cancel the task, but no production checkpoint path detaches its submitted
    queue.
47. Retain defensive worker-owner drain on component teardown for a terminal
    unrelated unwind. After mandatory drain, enqueue one private FIFO stop per
    worker without consulting poison. The finite-task contract bounds the
    join; shutdown does not cancel or abandon an accepted job.
48. Document the completion wait under the existing accepted-service wait
    family: worker execution is the progress producer, `Completion` is the
    authoritative result, unrelated poison does not abandon an accepted task,
    shutdown drains the outer mandatory owner, and the job plus checkpoint
    queue own cleanup. The successful flume send is acceptance.

### Benchmark schema, documentation, and evidence

49. Add strict `[engine.thread_pool] worker_threads = ...` overlay parsing,
    fieldwise merge, storage-config application, and normalized result output
    in `doradb-bench/src/engine_config.rs`.
50. Remove `worker_threads` from `MandatoryRuntimeOverlay` and
    `ResolvedMandatoryRuntimeConfig`; retain only `concurrency_limit`. Because
    benchmark TOML uses `deny_unknown_fields`, legacy explicit mandatory worker
    counts are rejected rather than silently ignored.
51. Update benchmark parser, merge, normalization, result round-trip, and
    unknown-field tests for both schema changes.
52. Update active documentation and component-order comments, including:
    - `docs/architecture.md`;
    - `docs/data-checkpoint.md` and checkpoint documentation as needed;
    - `docs/engine-component-lifetime.md`;
    - `docs/shutdown-and-poison.md` production wait classification;
    - `docs/benchmark-tool.md`; and
    - corresponding Rust API/module documentation.
53. Do not edit `docs/rfcs/0026-engine-owned-mandatory-background-runtime.md`
    or completed task documents that historically record the two-runner
    implementation and validation.
54. Run release checkpoint measurements using the established 2-million-row
    shape:
    - create one index-free table;
    - insert 2,100,000 sequential rows with 128-byte values, four benchmark
      threads, sixteen sessions, and batch size 100;
    - freeze with `max_rows = 2,000,000` and verify the actual frozen row and
      page counts;
    - run one measured checkpoint per fresh root; and
    - repeat five fresh roots for ThreadPool sizes 1, 2, and 4.
55. Keep all other engine and workload settings identical. Report raw timings,
    median, min/max, coefficient of variation, rows/second, attempt count, and
    retry-wait count for each pool size. At least one multi-worker setting must
    show a lower median than the one-worker setting before the performance goal
    is considered demonstrated; this is implementation evidence, not a CI
    threshold.
56. Capture a post-change Samply profile and interactive flamegraph for the
    2-million-row, multi-worker run. Confirm LWC encode/checksum stacks appear
    on multiple `Thread-Pool-*` workers and are no longer executed on
    `Mandatory-Runtime-1`.

## Implementation Notes

## Impacts

- `doradb-storage/src/conf/engine.rs`, `conf/mod.rs`, and `lib.rs` gain
  `ThreadPoolConfig`; mandatory runtime loses its worker-count builder and
  validation surface.
- `doradb-storage/src/runtime/thread_pool.rs` becomes the CPU executor,
  completion publisher, panic supervisor, and worker owner implementation.
- `doradb-storage/src/runtime/mandatory.rs` becomes fixed to one OS runner but
  retains multiple accepted async tasks, admission accounting, and cooperative
  scheduling.
- `doradb-storage/src/component.rs` and `engine.rs` gain two registered
  component entries and an `EngineCore` ThreadPool capability. Reverse teardown
  changes accordingly.
- `doradb-storage/src/error.rs` gains one configuration error and two Fatal
  classifications while removing the mandatory-worker configuration error.
- `doradb-storage/src/completion.rs` is reused unchanged apart from possible
  documentation synchronization.
- `doradb-storage/src/lwc/mod.rs` changes builder layout ownership from a borrow
  to an Arc and makes final encoding transferable.
- `doradb-storage/src/table/persistence.rs` gains checkpoint-local task
  orchestration and a Fatal-capable LWC build result.
- `doradb-bench/src/engine_config.rs`, benchmark docs, and result schema gain
  CPU-pool sizing and lose mandatory runner sizing.
- Existing explicit Rust or benchmark configuration of mandatory
  `worker_threads` is a deliberate breaking change. Backward compatibility is
  not provided.
- Engine bootstrap creates two additional CPU workers by default and one fewer
  mandatory runner, for a net default increase of one OS thread.
- Checkpoint may hold up to one owned builder/completion per configured pool
  worker in addition to its existing ordered encoded-block output. It does not
  retain additional row-page guards while waiting.
- Durable formats, recovery inputs, redo ordering, public checkpoint outcomes,
  MVCC, transaction semantics, and public spawning APIs do not change.
- No new dependency, unsafe block, raw-pointer lifetime, or unsafe baseline
  change is expected; `flume` and `Completion` are already production
  dependencies.

## Test Cases

1. `ThreadPoolConfig::default()` resolves to two workers; zero is rejected as
   `InvalidThreadPoolWorkerThreads` before storage-root mutation.
2. `MandatoryRuntimeConfig` exposes only `concurrency_limit`, defaults it to
   four, rejects zero, and always starts exactly `Mandatory-Runtime-1`.
3. Strict benchmark config merges and round-trips ThreadPool worker count and
   mandatory concurrency, while removed mandatory `worker_threads` and unknown
   ThreadPool fields are rejected.
4. Successful submission runs on a named ThreadPool worker and moves a
   non-Clone result exactly once through `Completion::wait_take_result`.
5. Dropping the returned completion immediately after accepted submission does
   not cancel the task; deterministic test control proves its body reaches
   completion exactly once.
6. A task panic poisons with `ThreadPoolTaskPanic`, completes an attached
   observer with the same Fatal classification, poisons when the observer is
   detached, releases owned input exactly once, and does not terminate the
   worker. An already queued normal task still completes.
7. Observed poison returns the cached Fatal without running the task. A
   disconnected worker ingress returns an already completed Fatal handle,
   publishes `ThreadPoolUnavailable`, and never panics or returns a Runtime
   submission error.
8. Inject failure at every `Thread-Pool-N` spawn point. Every earlier worker is
   stopped and joined before bootstrap returns the named `BackgroundSpawn`
   report, and no later component starts.
9. Explicit and failed-bootstrap shutdown enqueue private FIFO worker stops
   after mandatory workers, join every ThreadPool worker even when one join
   reports a panic, and preserve the first payload only after all joins are
   attempted.
10. The fixed mandatory runner polls multiple accepted tasks cooperatively when
    one awaits `Completion`, preserves internal cleanup progress under caller
    saturation, and retains admission/statistics/observer behavior.
11. The checkpoint queue never has more pending handles than the configured
    pool worker count. Deterministic test-only gates prove the call-site bound
    without sleeps.
12. Jobs completing out of order still produce `LwcBlockPersist` entries in
    original RowID range order.
13. Inner encode error, Fatal task completion, and producer-side page-access
    error each stop new submission, drain every previously accepted task, and
    retain the correct primary/secondary error relationship.
14. A checkpoint split that must wait for the oldest task drops its current
    page view and guard first. A synchronized competing page user proves no
    guard is retained across the completion wait.
15. Pool sizes 1, 2, and 4 produce byte-identical valid LWC blocks, checksums,
    shapes, pivot, heap replay floor, and column block-index routes from the
    same prepared inputs.
16. Index-free and secondary-index checkpoints preserve visible-row selection,
    call the sidecar exactly once per accepted row across splits, and publish
    matching DiskTree companion state.
17. Injected LWC task panic after irreversible page transition returns Fatal,
    keeps `ThreadPoolTaskPanic` as the typed reason, poisons the engine, drains
    other submitted encodes, and preserves workflow/publication ownership.
18. Heartbeat and empty-page checkpoints submit no CPU task and retain their
    existing metadata-only or silent-watermark behavior.
19. Existing freeze/checkpoint delay, retry, cancellation, deletion,
    publication, recovery, DDL, transaction-cleanup, shutdown, and poison tests
    remain passing with the fixed mandatory runner.
20. Run `rtk cargo fmt --all -- --check`.
21. Run `rtk cargo clippy --workspace --all-targets -- -D warnings`.
22. Run `rtk cargo build --workspace`.
23. Run `rtk cargo nextest run --workspace`.
24. Run
    `rtk cargo nextest run -p doradb-storage --no-default-features --features libaio`.
25. Run the mandatory branch-diff style audit required by the resolution
    workflow.
26. Run five fresh-root 2-million-row checkpoint measurements for pool sizes
    1, 2, and 4, record the complete comparison, and verify at least one
    multi-worker median improves on one worker.
27. Validate the Samply profile and generated SVG flamegraph and retain their
    inspection commands and artifact paths in implementation notes at task
    resolution.

## Open Questions

None. Generic task limiting, CREATE INDEX adoption, blocking work, additional
consumers, pool observability, and scheduler policy require separately scoped
evidence and design.
