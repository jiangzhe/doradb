---
id: 0013
title: Introduce Shared Storage Runtime With Shared IO And Multi-Pool Eviction
status: proposal
tags: [io, scheduling, eviction, table-file, buffer-pool]
created: 2026-04-03
---

# RFC-0013: Introduce Shared Storage Runtime With Shared IO And Multi-Pool Eviction

## Summary

This RFC introduces one engine-owned shared storage runtime that replaces the
current per-subsystem storage worker topology with two shared threads: one
fair-scheduled storage-I/O worker for table-file I/O, readonly miss loads,
`mem_pool`, and `index_pool`; and one shared evictor thread for `mem_pool`,
`index_pool`, and the global readonly pool. The selected direction keeps the
backend-neutral completion core from RFC-0010, keeps redo-log I/O dedicated,
and keeps pool-local eviction policy/state local even though evictor execution
is shared. It also centralizes storage-I/O capacity under
`TableFileSystemConfig.io_depth`, deprecates
`EvictableBufferPoolConfig.max_io_depth`, and reduces the default engine
background-thread count from `12` to `8`. [D2], [D6], [D9], [C1], [C3], [C5],
[C6], [C8], [C9], [C10], [U1], [U2], [U5], [U6], [U7], [U8]

## Context

The storage engine already uses one backend-neutral completion model for direct
I/O, but it still starts three separate storage-facing I/O workers.
`TableFileSystem` owns its own backend, worker, and stats handle, while
`mem_pool` and `index_pool` each create their own backend context and worker
thread. All three paths already submit fd-based `Operation`s into the same
completion-core shape, so the current topology is more fragmented than the
generic I/O layer itself. [D1], [D2], [D9], [C1], [C2], [C3], [C5]

The read path also crosses the buffer/file boundary already. Foreground reads
always go through a buffer pool, and readonly cache miss loads already route
through `TableFsRequest::Read` on the table-file worker. A worker merge is
therefore not just a background-writeback change; it directly affects
latency-sensitive readonly and table reads as well. [D1], [D3], [C2], [C3],
[C4], [C8], [U5]

Current worker mechanics are not safe to reuse unchanged for that mixed load.
`IOWorker::fetch_reqs()` drains one global worker-owned `deferred_req` before
accepting fresh channel traffic, and `EvictablePoolStateMachine` expands
`PoolRequest::BatchWrite` into as many writes as current headroom allows while
returning the remainder. Under one shared worker, a bursty eviction batch could
therefore keep reclaiming headroom and starve unrelated reads. [D2], [C1],
[C5], [U2], [U3]

Thread count is also fragmented on the eviction side. `mem_pool`,
`index_pool`, and the global readonly pool each start a dedicated evictor
thread. At the same time, the repository already has one generic
`EvictionRuntime` abstraction and one generic `Evictor<T>` runner shared by
mutable and readonly pools. That means the existing design already separates
pool-local eviction logic from the thread that runs it; what remains
per-thread today is mainly wakeup, loop ownership, and one `ClockHand` plus
policy instance per pool. [D1], [D6], [C5], [C7], [C8], [U5], [U6], [U7]

Lifecycle ownership is another blocker for a drop-in merge. Today
`DiskPoolWorkers`, `TableFileSystemWorkers`, `IndexPoolWorkers`, and
`MemPoolWorkers` each treat their worker threads as private owner state and
shut them down independently. The engine lifecycle document and component
registry require one clear owner for reverse-order shutdown and final drop, so
any shared worker topology needs one explicit owner as well. [D6], [D7], [C3],
[C5], [C6], [C8]

This RFC is needed now because the selected direction is no longer just "merge
the three I/O workers." The user chose a broader storage-runtime boundary:
shared I/O, shared eviction execution, file-system-level storage-I/O capacity,
and no new user-facing scheduling knobs in the first implementation. That is
architectural scope, not task scope. [D7], [U4], [U5], [U6], [U7], [U8]

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:epic`
`- priority:medium`
`- codex`

## Design Inputs

### Documents

- [D1] `docs/architecture.md` - storage subsystem boundaries and the split
  between table files, readonly caching, mutable buffer pools, and secondary
  indexes.
- [D2] `docs/async-io.md` - current backend-neutral completion-core model and
  the storage-engine integration points that already depend on it.
- [D3] `docs/table-file.md` - table-file persistence and CoW-root publication
  model that the shared storage runtime must continue to serve.
- [D4] `docs/index-design.md` - index checkpoint and DiskTree persistence model
  that drives `index_pool` writeback pressure.
- [D5] `docs/checkpoint-and-recovery.md` - No-Steal / No-Force persistence
  model and the checkpoint workloads that generate table-file and index I/O.
- [D6] `docs/engine-component-lifetime.md` - owner/runtime split, reverse-order
  shutdown, and worker teardown rules for engine-owned components.
- [D7] `docs/process/issue-tracking.md` - RFCs are the required planning format
  for large architectural changes before implementation.
- [D8] `docs/process/unit-test.md` - backend-neutral I/O changes must validate
  on the default `io_uring` path and the supported `libaio` alternate backend.
- [D9] `docs/rfcs/0010-retire-thread-pool-async-io-and-introduce-backend-neutral-completion-core.md`
  - completion-core direction and the requirement to keep backend differences
  below the generic I/O layer.
- [D10] `docs/unsafe-usage-principles.md` - direct-I/O and stable-memory
  changes must keep explicit safety boundaries and invariant checks.
- [D11] `docs/process/unsafe-review-checklist.md` - validation and inventory
  expectations if implementation introduces or modifies unsafe code.

### Code References

- [C1] `doradb-storage/src/io/mod.rs` - current `IOWorker`, single-channel
  request intake, single `deferred_req`, `AIOClient<T>`, and backend-neutral
  completion ownership.
- [C2] `doradb-storage/src/file/mod.rs` - `TableFsRequest`,
  `TableFsStateMachine`, and the current table-file request/submit/complete
  contract.
- [C3] `doradb-storage/src/file/table_fs.rs` - `TableFileSystem` currently owns
  its own backend context, worker thread, shutdown path, and stats handle.
- [C4] `doradb-storage/src/file/cow_file.rs` - readonly cache miss loads route
  through `TableFsRequest::Read`, so table-file worker latency directly affects
  readonly reads.
- [C5] `doradb-storage/src/buffer/evict.rs` - evictable pools own separate I/O
  and evictor threads, maintain pool-local inflight/writeback state, and use
  `max_io_depth` today.
- [C6] `doradb-storage/src/component.rs` and `doradb-storage/src/engine.rs` -
  current component registration order and reverse-order shutdown/drop.
- [C7] `doradb-storage/src/buffer/evictor.rs` - generic `EvictionRuntime`,
  `PressureDeltaClockPolicy`, and `Evictor<T>` loop already shared across pool
  types.
- [C8] `doradb-storage/src/buffer/readonly.rs` - global readonly pool has its
  own dedicated evictor thread, no dedicated I/O worker, and different
  drop-only eviction behavior.
- [C9] `doradb-storage/src/conf/table_fs.rs` - current table-file config owns
  `io_depth` and readonly-buffer sizing.
- [C10] `doradb-storage/src/conf/buffer.rs` - evictable-pool config currently
  exposes `max_io_depth` and pool-local eviction-arbiter tuning.
- [C11] `doradb-storage/src/conf/engine.rs` - engine config and storage-path
  resolution already treat table files and swap files as one storage layout.

### Conversation References

- [U1] User requested merging the `table-file`, `mem-pool`, and `index-pool`
  I/O workers.
- [U2] User explicitly requested fairness controls for the merged worker rather
  than a raw thread reduction only.
- [U3] Prior design discussion established that the merge is feasible in
  principle but not a drop-in change because current shutdown ownership, stats,
  and `deferred_req` behavior would create starvation and lifecycle ambiguity.
- [U4] User selected the long-term shared-service direction and asked to switch
  from task planning to RFC workflow.
- [U5] User asked whether evictor-count reduction should also be included and
  argued that the read path naturally groups buffer pools and storage I/O.
- [U6] User preferred keeping eviction logic mostly intact and grouping
  execution into one thread loop rather than redesigning eviction itself.
- [U7] User selected one shared evictor thread across `mem_pool`,
  `index_pool`, and the global readonly pool.
- [U8] User decided that shared storage-I/O depth should be owned by
  `TableFileSystemConfig`, and that pool `max_io_depth` should be deprecated
  and removed from the target state.

## Decision

We will introduce one engine-owned `StorageRuntime` that replaces the
current four storage worker-owner components with two shared threads:
`StorageService` and `SharedPoolEvictor`. The runtime serves
`TableFileSystem`, readonly miss loads, `mem_pool`, and `index_pool`, while
redo-log I/O remains dedicated. Storage-I/O capacity becomes file-system-level
configuration owned by `TableFileSystemConfig`, and eviction execution becomes
shared while eviction policy/state remain pool-local. [D2], [D6], [D9], [C1],
[C3], [C5], [C6], [C7], [C8], [C9], [C10], [C11], [U1], [U2], [U4], [U5],
[U6], [U7], [U8]

### 1. One shared runtime will own both shared storage threads

The new runtime will be registered after `DiskPool`, `TableFileSystem`,
`IndexPool`, and `MemPool`, and before `Catalog`. It replaces
`DiskPoolWorkers`, `TableFileSystemWorkers`, `IndexPoolWorkers`, and
`MemPoolWorkers` as the owner responsible for worker startup, shutdown
signaling, join, and runtime-wide telemetry. [D6], [C3], [C5], [C6], [C8],
[U5], [U7]

Build sequencing will mirror the current owner-then-worker split rather than
forcing table/pool owners to depend on a later-constructed runtime directly.
`DiskPool`, `TableFileSystem`, `IndexPool`, and `MemPool` will still register
their owners first and place build-only provisions on the shelf; the shared
runtime consumes those provisions later and starts the two shared threads once
all required owners exist. That keeps current ownership topology workable under
the existing component registry model. [D6], [C3], [C5], [C6], [C8]

### 2. Storage I/O becomes one file-system-level service

`StorageService` owns one backend-neutral completion worker and serves
typed clients for:

- table-file reads and writes;
- readonly cache miss loads that already route through `TableFsRequest::Read`;
- `mem_pool` page-in reads and swap-file writeback; and
- `index_pool` page-in reads and swap-file writeback. [D1], [D2], [D3], [C2],
  [C3], [C4], [C5], [U1], [U5]

Redo-log I/O is explicitly excluded. It has its own durability and fatal-error
policy and should not share a queueing domain with foreground table/readonly
reads and background buffer-pool writeback. [D2], [D5], [C1], [U4]

`TableFileSystemConfig.io_depth` becomes the single authoritative shared
storage-I/O depth. Pool swap files are treated as part of the same storage
runtime and no longer own separate I/O-capacity settings. `EvictableBufferPool`
will stop treating `max_io_depth` as authoritative during migration, and the
RFC target state removes that field from the public config surface after the
shared runtime lands. [C5], [C9], [C10], [C11], [U8]

The first implementation does not add new user-facing fairness knobs. Lane
weights, burst budgets, and shared-evictor scan order remain internal policy
with tests and counters, not external configuration. [D8], [C9], [C10], [U8]

### 3. Fairness lives inside the generic completion path

The repository should not add a one-off mixed enum worker beside `crate::io`,
nor should it keep current single-channel `IOWorker` semantics and only merge
request types. Instead, `crate::io` will gain a scheduler boundary that
supports multiple logical request lanes and lane-local deferred remainders while
preserving the existing backend-neutral completion ownership model. Existing
single-lane worker construction remains available for redo-log I/O and other
simple clients. [D2], [D9], [C1], [C2], [C5], [U2], [U3], [U4]

The shared worker will expose three lane classes in v1:

- `table_reads`: table-file reads and readonly miss loads;
- `pool_reads`: `mem_pool` and `index_pool` page-in reads; and
- `background_writes`: table-file writes, `mem_pool` writeback, and
  `index_pool` writeback. [C2], [C4], [C5], [U2], [U5]

The scheduler contract is:

1. fresh read traffic must be reconsidered before another background-write
   burst can consume newly freed headroom;
2. deferred request remainders are lane-local, not global;
3. multi-page writeback is split into bounded bursts before the scheduler
   returns to lane selection; and
4. shutdown still drains staged, deferred, and in-flight work exactly once
   before owner drop begins. [D2], [D5], [D6], [C1], [C2], [C4], [C5], [U2],
   [U3], [U5]

### 4. One shared thread will execute three pool-local eviction policies

`SharedPoolEvictor` is a sibling worker thread, not part of the I/O worker
loop. It drives three pool-local eviction runtimes:

- global readonly pool;
- `mem_pool`; and
- `index_pool`. [C5], [C7], [C8], [U6], [U7]

This is intentionally an execution merge, not an eviction-logic merge. Each
pool keeps its own `EvictionArbiterBuilder`, `PressureDeltaClockPolicy`,
`ClockHand`, residency accounting, allocation-failure tracking, and
`EvictionRuntime` behavior. The shared thread only centralizes wakeup and loop
ownership. [C5], [C7], [C8], [C10], [U6], [U7]

The shared evictor will therefore:

1. keep one policy instance and one clock hand per pool;
2. use one shared wakeup source so pressure in any pool can wake the single
   thread;
3. round-robin pools that currently report pending pressure rather than fully
   draining one pool before considering others; and
4. preserve existing pool-local `execute()` behavior, including readonly
   drop-only eviction and mutable-pool dirty-page writeback via the shared I/O
   service. [C5], [C7], [C8], [U6], [U7]

### 5. Eviction stays separate from the I/O worker thread

The shared evictor must not be fused with the shared I/O worker into one single
thread execution loop. Mutable-pool eviction can block waiting for writeback
completion, while the I/O worker must keep submitting and completing direct I/O
promptly. Coupling them would make one thread responsible for both kernel-I/O
progress and eviction wait phases, expanding the stall domain instead of
reducing it. [D2], [C1], [C5], [C7], [C8], [U6], [U7]

### 6. Subsystem request and page-lifetime semantics remain local

This RFC does not redefine table-file or buffer-pool semantics. `TableFsStateMachine`
still owns table-file submission and completion behavior. Evictable pools still
own page-in dedupe, in-flight page bookkeeping, and swap-file completion rules.
Readonly still owns its own residency map, inflight-load publication, and
drop-only eviction behavior. The shared runtime only centralizes worker
ownership, scheduling, and wakeup. [D2], [D3], [D4], [C2], [C5], [C7], [C8],
[U5], [U6]

## Alternatives Considered

### Alternative A: Shared Storage I/O Only, Keep Per-Pool Evictor Threads

- Summary: Merge the three storage-I/O workers but leave the readonly, mem, and
  index evictor threads unchanged.
- Analysis: This captures the most obvious I/O-layer consolidation and is lower
  risk than changing both I/O and eviction topology at once. It still leaves
  three mostly lightweight dedicated evictor threads in place, even though the
  repository already has a generic `Evictor<T>` abstraction and the user
  explicitly wants thread-count reduction on the eviction side as well. [C5],
  [C7], [C8], [U5], [U6], [U7]
- Why Not Chosen: The selected runtime boundary now explicitly includes shared
  eviction execution, and leaving evictors separate would preserve avoidable
  thread overhead after the harder part of shared ownership has already landed.
- References: [C5], [C7], [C8], [U5], [U6], [U7]

### Alternative B: Shared Evictor Only For `mem_pool` And `index_pool`

- Summary: Share one evictor thread for the two mutable pools and keep the
  global readonly pool on its own evictor thread.
- Analysis: This minimizes behavioral differences because readonly eviction is
  drop-only and uses a different residency model. However, `EvictionRuntime`
  and `Evictor<T>` are already generic across mutable and readonly pools, so
  keeping readonly separate would preserve one dedicated thread mainly because
  of historical ownership rather than because of a strong abstraction barrier.
  [C7], [C8], [U6], [U7]
- Why Not Chosen: The user explicitly chose one shared evictor thread across
  all three pools, and the existing abstraction is already strong enough to
  support that direction while keeping pool-local behavior intact.
- References: [C7], [C8], [U6], [U7]

### Alternative C: One Mixed FIFO Shared I/O Worker Using Current `IOWorker` Semantics

- Summary: Replace the three storage request types with one shared enum and run
  them through the existing single-channel `IOWorker` with its current global
  `deferred_req`.
- Analysis: This is the most direct implementation of "merge the workers," and
  it would reduce thread count quickly. But it preserves the starvation
  mechanism that motivated the fairness requirement: a bursty `BatchWrite`
  remainder can keep occupying the one worker-owned deferred slot and prevent
  fresh reads from being admitted promptly. [D2], [C1], [C5], [U2], [U3]
- Why Not Chosen: It meets the topology goal while failing the fairness goal,
  which is the main correctness requirement of the shared-I/O design.
- References: [D2], [C1], [C5], [U2], [U3]

### Alternative D: Fuse Shared I/O And Shared Eviction Into One Thread

- Summary: Use one thread execution loop for both direct-I/O progress and all
  pool eviction work.
- Analysis: This minimizes thread count further and matches the intuition that
  evictor work is lightweight. In practice, mutable-pool eviction can block on
  dirty-page writeback completion, while the I/O worker must stay responsive for
  reads, submissions, and completions. One fused loop would conflate those wait
  modes and increase the chance that eviction waiting delays I/O progress or
  vice versa. [D2], [C1], [C5], [C7], [C8], [U6]
- Why Not Chosen: One shared evictor thread is reasonable; collapsing it into
  the I/O worker is the wrong coupling boundary.
- References: [D2], [C1], [C5], [C7], [C8], [U6]

### Alternative E: Merge Redo Into The Same Shared Runtime

- Summary: Use one storage-wide runtime for table-file, buffer-pool, readonly,
  and redo-log I/O.
- Analysis: This maximizes thread reduction and yields one uniform I/O
  ownership story. It also couples unlike traffic classes: redo is part of
  commit durability and fatal-engine error handling, while the selected shared
  runtime is about foreground reads plus background persistence. [D2], [D5],
  [C1], [U4]
- Why Not Chosen: The added coupling is larger than the benefit. Keeping redo
  dedicated preserves a simpler failure domain and respects its different
  liveness policy.
- References: [D2], [D5], [C1], [U4]

## Unsafe Considerations (If Applicable)

This RFC changes scheduling and ownership around direct-I/O operations that can
carry owned aligned buffers or borrowed page pointers, and it changes worker
ownership around arena-backed page guards and eviction waiting. Implementation
must therefore treat this as unsafe-adjacent even if the refactor adds little
net-new unsafe code. [D10], [D11], [C1], [C5], [C7], [C8]

### 1. Unsafe scope and boundaries

Expected unsafe-sensitive areas are:

1. completion-core slot ownership and token-to-submission mapping in
   `crate::io`; [C1]
2. borrowed page-pointer lifetimes for mutable-pool reads and writes; [C5]
3. direct-I/O buffer ownership transfer for table-file reads and writes; [C1],
   [C2], [C4]
4. shared-evictor interaction with arena-backed page guards, residency release,
   and post-writeback cleanup; [C5], [C7], [C8]
5. shutdown ordering between client owners, scheduler-owned deferred
   remainders, evictor waiters, and backend in-flight operations. [D6], [C1],
   [C3], [C5], [C6], [C8]

### 2. Retained versus changed safety patterns

The implementation must retain the existing completion-core invariants from
RFC-0010:

1. submitted memory stays valid until completion is observed exactly once; and
2. each backend token maps back to exactly one in-flight slot. [D2], [D9],
   [C1]

What changes is the ownership model above those invariants. Lane-local deferred
remainders in the scheduler must be owned as strongly as the current single
worker-owned `deferred_req`, and the shared-evictor loop must not release
arena-backed memory or residency state before pool-local `execute()` behavior
has completed. [C1], [C5], [C7], [C8], [D10]

### 3. Required safety comments and invariant enforcement

Any new or modified unsafe blocks must keep adjacent `// SAFETY:` comments with
concrete lifetime, ownership, and alignment assumptions. Implementation should
prefer small internal helpers plus explicit assertions for lane state,
shared-evictor drain invariants, and shutdown ordering. If a public API becomes
`unsafe`, it must carry a `/// # Safety` contract. [D10], [D11]

### 4. Validation and inventory strategy

If implementation introduces or changes unsafe code in the affected modules, it
must refresh the unsafe inventory and run the repo's required checks:

- `tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md`
- `cargo clippy -p doradb-storage --all-targets -- -D warnings`
- `cargo nextest run -p doradb-storage`

Backend-touching phases must also run the supported alternate backend pass:

- `cargo nextest run -p doradb-storage --no-default-features --features libaio`

[D8], [D10], [D11]

## Implementation Phases

- **Phase 1: Multi-Lane Completion Scheduling**
  - Scope: Extend `crate::io` with a scheduler boundary that supports multiple
    logical request lanes, lane-local deferred remainders, and bounded-burst
    admission while preserving the current backend-neutral submission and
    completion ownership model.
  - Goals: Make fair shared-worker scheduling a first-class generic capability
    instead of a service-specific fork; preserve single-lane compatibility for
    redo and simple clients.
  - Non-goals: No client migration yet; no redo-path changes; no shared-evictor
    work yet; no new user-facing scheduling knobs.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

- **Phase 2: Shared Storage IO Runtime And Config Centralization**
  - Scope: Introduce `StorageRuntime` and `StorageService`,
    migrate `TableFileSystem`, `mem_pool`, and `index_pool` to typed shared-I/O
    clients, replace their dedicated I/O-worker ownership, and move
    authoritative shared-I/O depth to `TableFileSystemConfig`.
  - Goals: Replace three private storage-I/O worker owners with one shared
    owner; preserve readonly miss-load correctness; deprecate
    `EvictableBufferPoolConfig.max_io_depth` immediately and remove it from the
    RFC target state.
  - Non-goals: No evictor-thread merge yet; no redo migration; no public tuning
    API for fairness policy.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

- **Phase 3: Shared Multi-Pool Evictor**
  - Scope: Introduce `SharedPoolEvictor`, migrate global readonly, `mem_pool`,
    and `index_pool` eviction execution onto one shared thread, add the shared
    wakeup mechanism, and remove the three dedicated evictor-owner components.
  - Goals: Reduce evictor threads from three to one while preserving one
    policy/clock-hand per pool and keeping pool-local `execute()` behavior
    unchanged.
  - Non-goals: No merger of pool eviction algorithms; no fusion with the shared
    I/O worker; no change to readonly drop-only eviction semantics.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

- **Phase 4: Validation, Stats, And Documentation Hardening**
  - Scope: Add starvation-focused shared-I/O tests, multi-pool wakeup and
    fairness tests for the shared evictor, service-level telemetry, config
    deprecation tests, and updated runtime-topology documentation.
  - Goals: Prove bounded read progress under writeback pressure, prove that all
    three pools can share one evictor thread safely, and validate both
    `io_uring` and `libaio` backend behavior.
  - Non-goals: No autotuner; no external scheduling knobs; no broader engine
    scheduling framework beyond this storage runtime.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

## Consequences

### Positive

- Reduces the default engine background-thread count from `12` to `8` by
  replacing three storage-I/O workers with one and three storage evictor
  threads with one, while leaving redo and other transaction-system workers
  unchanged. [C3], [C5], [C6], [C8], [U1], [U5], [U7]
- Makes worker ownership explicit at the engine-component level, which aligns
  shutdown behavior with the existing reverse-order owner/drop model. [D6],
  [C3], [C5], [C6], [C8]
- Prevents the obvious starvation mode of a merged storage-I/O worker by
  replacing the global deferred-remainder model with lane-local fairness. [D2],
  [C1], [C5], [U2], [U3]
- Reuses the existing generic eviction abstraction instead of forcing a second
  algorithm rewrite just to reduce evictor thread count. [C7], [C8], [U6],
  [U7]
- Unifies storage-I/O capacity under one file-system-level configuration
  boundary. [C9], [C10], [C11], [U8]

### Negative

- Adds scheduler complexity to the generic I/O path, which increases the review
  and validation surface of `crate::io`. [D2], [D9], [C1]
- One shared evictor thread serializes eviction execution across three pools,
  so a long mutable-pool writeback wait can delay eviction progress for another
  pool even though policies remain separate. [C5], [C7], [C8], [U7]
- Changes the current observability shape because backend stats become
  runtime-owned instead of trivially per-worker, and evictor activity becomes
  one shared loop rather than one thread per pool. [C3], [C5], [C8]
- Requires config migration because `EvictableBufferPoolConfig.max_io_depth`
  becomes deprecated and eventually removed. [C9], [C10], [U8]

## Open Questions

1. What initial bounded-burst sizes best balance read latency against write
   throughput for the `background_writes` lane across both `io_uring` and
   `libaio`?
2. What minimum per-runtime and per-pool counters are required to replace the
   current per-worker debugging value without exposing new user-facing knobs?

## Future Work

1. Reevaluate whether other storage clients should adopt the same shared
   scheduler boundary after the initial storage runtime proves stable.
2. Consider adaptive scheduling or workload-sensitive lane tuning only after
   the fixed-policy runtime is validated.
3. Remove the deprecated pool-local I/O depth field once the migration phase is
   complete and downstream config users are updated.

## References

- `docs/rfcs/0010-retire-thread-pool-async-io-and-introduce-backend-neutral-completion-core.md`
- `docs/async-io.md`
- `docs/engine-component-lifetime.md`
