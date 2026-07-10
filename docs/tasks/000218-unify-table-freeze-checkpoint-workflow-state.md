---
id: 000218
title: Unify table freeze and checkpoint workflow state
status: implemented
created: 2026-07-10
github_issue: 834
---

# Task: Unify table freeze and checkpoint workflow state

## Summary

Add one table-owned volatile state machine for the complete user-table
freeze-to-checkpoint workflow. The table, rather than a session caller, will own
the original freeze fence, selected page metadata, and cached cutoff validation
state. Repeated and concurrent maintenance calls will have explicit outcomes,
and page states will be consequences of admitted workflow transitions instead
of signals that scans silently skip.

Keep this workflow adjacent to `TableLifecycle`. `TableLifecycle` remains
authoritative for `Live -> Dropping -> Dropped`, metadata/root exclusion, and
the checkpoint publish/drop gate. The new workflow owns the canonical frozen
batch and its semantic phase. Integrate the two at short, synchronous admission
boundaries so no mutex is held across `.await` and every potentially long
conflict wait remains asynchronous.

Remove caller-owned `FrozenPageBatch`, compatibility batch reconstruction, and
the standalone checkpoint-readiness API. Keep separate public freeze and
checkpoint operations so callers can control the freeze row budget, but make
`freeze_table` return only a read-only batch summary and make
`checkpoint_table` consume or retain the table-owned canonical batch.

## Context

Issue Labels:
- type:task
- priority:high
- codex

Source Backlogs:
- docs/backlogs/000152-unify-table-freeze-checkpoint-workflow-state.md

Related implemented design and follow-up:

- `docs/tasks/000217-redesign-checkpoint-frozen-page-cutoff-validation.md`
  established the original post-freeze fence, actual undo-status cutoff
  validation, cached page validation, retryable frozen-page delay, and fatal
  fail-closed behavior after page transition.
- `docs/rfcs/0017-drop-table-lifecycle-recovery.md` established the terminal
  table lifecycle and no-cancel checkpoint publish lease used by drop.
- `docs/tasks/000147-runtime-layout-and-checkpoint-gate.md` added the reversible
  metadata-change/root-mutation exclusion used by checkpoint and index DDL.
- `docs/backlogs/000151-optimize-frozen-page-checkpoint-transition-planning.md`
  separately tracks page-local transition preparation, reduced batch-wide
  page-state locking, and measured checkpoint/foreground-write optimization.

The current workflow is implicit:

- `Table::freeze` accepts both `ACTIVE` and already-`FROZEN` pages, silently
  stops on `TRANSITION`, allocates a new fence for every call, and returns a
  caller-owned batch.
- `checkpoint_frozen_pages` relies on the caller retaining that batch and its
  validation cache across retryable delays.
- compatibility `checkpoint_table` reconstructs a batch from the current frozen
  prefix and allocates a later fence, which is not equivalent to the original
  freeze boundary.
- `table_checkpoint_readiness` observes only active-root age. It reserves no
  workflow phase, owns no batch, and does not prevent the observed state from
  changing before checkpoint execution.
- checkpoint currently moves pages to `TRANSITION` before acquiring the publish
  lease. Drop can therefore close publication after transition has become
  irreversible, turning an otherwise valid public race into fatal poison.
- silent-watermark checkpoint commits a table-scoped catalog marker without the
  publish lease, so drop does not order that irreversible publication through
  the existing checkpoint/drop gate.

The durable checkpoint and recovery model does not change. One table-file root
still atomically publishes LWC data, block-index state, cold-delete state,
secondary `DiskTree` roots, allocation reachability, and replay bounds. An idle
checkpoint may still publish only a catalog silent watermark. Recovery still
initializes loaded user tables from the durable root and redo; the new workflow
is volatile and initializes as idle.

## Goals

1. Give each live user table exactly one authoritative freeze/checkpoint
   workflow and at most one canonical frozen batch and fence.
2. Make repeated freeze return the original batch information without extending
   the prefix, replacing the fence, or resetting cached validation.
3. Define deterministic, structured outcomes for concurrent freeze, checkpoint,
   metadata-change, and drop races.
4. Preserve task-000217 undo-status cutoff validation and retain its per-page
   cache across delayed checkpoint attempts.
5. Make `ACTIVE -> FROZEN -> TRANSITION` page states consequences of admitted
   workflow transitions and remove silent termination on page state.
6. Freeze selected pages with short, independent page-state critical sections
   rather than holding all selected page-state write locks simultaneously.
7. Acquire the lifecycle publish lease after reversible batch validation but
   before the first page enters `TRANSITION`.
8. Order silent-watermark and root-only/deletion-only publication through the
   same publish/drop gate even when no row page transitions.
9. Make ordinary errors and future cancellation restore `Idle` or the canonical
   `Frozen` batch while the attempt remains reversible.
10. Close workflow state during drop without waiting for a delayed or reversible
    batch, prevent attempt guards from resurrecting state after closure, and
    asynchronously drain a checkpoint that already won publish admission.
11. Require a closed workflow before a runtime is marked dropped or destroyed,
    including recovery and failed-create offline cleanup paths.
12. Add no workflow synchronization, waiting, or bookkeeping to foreground
    transaction read/write paths.
13. Preserve existing checkpoint/recovery, metadata publication, cold-delete,
    secondary-index, and root-reachability semantics.

## Non-Goals

1. Do not implement an automatic tuple-mover actor, background scheduling
   policy, event-driven cutoff notification index, or maintenance request queue.
2. Do not implement backlog 000151's prepared transition pages, mutation epochs,
   commit-maintained page watermarks, page-local irreversible transition, or
   benchmark-driven foreground-write optimizations.
3. Do not convert `RowVersionMap` page-state locks to async locks. That change
   would reach every hot row-write path and must be evaluated together with the
   shorter-lock transition design in backlog 000151.
4. Do not change LWC format, table-file format, redo format, recovery boundaries,
   or root publication atomicity.
5. Do not change which lock/delete overlays are representable or how task 000217
   validates insert/update image CTS values.
6. Do not merge canonical batch ownership into the atomically packed
   `TableLifecycle`; the two state machines have separate responsibilities.
7. Do not preserve compatibility for public caller-owned batch, explicit-batch
   checkpoint, or standalone readiness APIs.
8. Do not make the irreversible portion of a checkpoint independently owned by
   a background executor. Dropping a checkpoint future after transition remains
   fatal because page routing cannot be rolled back safely.
9. Do not restore a table from `Dropping` to `Live`. Abandoning `DROP TABLE`
   after its terminal lifecycle gate remains a fatal storage outcome.

## Plan

### 1. Add the table-owned checkpoint workflow

Create `doradb-storage/src/table/checkpoint_workflow.rs` and add one
`TableCheckpointWorkflow` field to `Table`. Use a short critical-section
`parking_lot::Mutex` around state equivalent to:

```rust
enum TableCheckpointWorkflowState {
    Idle,
    Freezing,
    Frozen(FrozenPageBatch),
    Checkpointing { source: CheckpointSource },
    Publishing,
    Transition,
    Closed,
}

enum CheckpointSource {
    Idle,
    Frozen,
}
```

`Publishing` represents a root-only, deletion-only, or silent-watermark
checkpoint that owns publish admission but has no row-page transition. It may
still abort safely before the actual root swap or commit handoff. `Transition`
represents an irreversible checkpoint whose canonical prefix is moving to cold
storage.

The workflow mutex must never be held across `.await`. It may be held while
performing a lifecycle atomic admission operation and recording the matching
workflow phase so drop and checkpoint have one unambiguous linearization order.

Add RAII attempt guards:

- a freeze guard restores `Freezing -> Idle` if an attempt is cancelled or
  returns before any page-state publication and the workflow is still live;
- a checkpoint guard temporarily owns `Option<FrozenPageBatch>` while workflow
  state says `Checkpointing`;
- a reversible checkpoint guard restores the updated batch to `Frozen` or
  restores `Idle` on ordinary error, delay, or future cancellation;
- a guard observing `Closed` never restores state;
- a `Publishing` guard may finish back at `Idle` only after an explicitly safe
  pre-publication rollback; it must keep the publish lease until that rollback
  is complete and never restore a frozen batch;
- after `Transition`, or after a root swap/commit handoff becomes irreversible,
  a guard never restores reversible state. Unexpected failure remains protected
  by the existing checkpoint fatal guard.

Keep `FrozenPageBatch` private to the table workflow. It continues to contain
the original `table_id`, `frozen_ts`, approximate row count, selected page
metadata, the cached successor page creation timestamp when one is observed,
and the task-000217 validation cache.

Define live page-layout invariants:

- `Idle`: every hot page at or above the current pivot is `ACTIVE`.
- `Freezing`: before finalization every selected page is `ACTIVE`; during the
  synchronous page-by-page publication there may be a monotonically growing
  `FROZEN` prefix followed by an `ACTIVE` suffix.
- `Frozen` and `Checkpointing(Frozen)`: the canonical prefix is exactly
  `FROZEN`; the remaining hot suffix is `ACTIVE`.
- `Checkpointing(Idle)` and `Publishing`: no frozen prefix is required;
  `Publishing` additionally means the attempt owns the publish lease.
- `Transition`: the canonical prefix is `TRANSITION`, the suffix is `ACTIVE`,
  and the attempt owns root-mutation and publish admission.
- `Closed`: no foreground page-layout guarantee is required; an already-admitted
  publisher may finish from owned attempt state, but no new or restored workflow
  state is permitted.

### 2. Replace the public freeze/checkpoint API surface

Change session freeze to require mutable session ownership:

```rust
pub async fn freeze_table(
    &mut self,
    table_id: TableID,
    max_rows: usize,
) -> Result<FreezeOutcome>;
```

Keep checkpoint as:

```rust
pub async fn checkpoint_table(
    &mut self,
    table_id: TableID,
) -> Result<CheckpointOutcome>;
```

Add public read-only result types with documented accessors:

```rust
pub enum FreezeOutcome {
    Frozen { batch: FrozenPageBatchInfo },
    AlreadyFrozen { batch: FrozenPageBatchInfo },
    Cancelled { reason: CheckpointCancelReason },
}

pub struct FrozenPageBatchInfo {
    table_id: TableID,
    frozen_ts: TrxID,
    approximate_rows: usize,
    page_count: usize,
    stable_page_count: usize,
}
```

Extend cancellation vocabulary with `FreezeInProgress`. Use:

- `FreezeInProgress` when another freeze owns `Freezing`;
- `CheckpointInProgress` when checkpoint owns `Checkpointing`, `Publishing`, or
  `Transition`;
- `TableMetadataChanging` for metadata/root exclusion;
- `TableDropping` and `TableDropped` for terminal lifecycle races.

Operation behavior by workflow phase is:

| State | `freeze_table` | `checkpoint_table` |
| --- | --- | --- |
| `Idle` | claim `Freezing` | claim `Checkpointing(Idle)` |
| `Freezing` | cancel `FreezeInProgress` | cancel `FreezeInProgress` |
| `Frozen` | return `AlreadyFrozen` | claim canonical batch |
| `Checkpointing` | cancel `CheckpointInProgress` | cancel `CheckpointInProgress` |
| `Publishing` / `Transition` | cancel `CheckpointInProgress` | cancel `CheckpointInProgress` |
| `Closed` | terminal lifecycle cancellation | terminal lifecycle cancellation |

Missing tables continue returning the existing table-not-found operation error.
The cancellation results cover races after an existing runtime handle has been
resolved.

Remove:

- public `FrozenPageBatch` and its crate re-export;
- `Session::checkpoint_frozen_pages`;
- `Table::checkpoint_existing_frozen` and compatibility batch reconstruction;
- `Session::table_checkpoint_readiness` and public `CheckpointReadiness`.

Retain a private active-root readiness helper that returns a normal
`CheckpointOutcome::Delayed` reason from the definitive checkpoint attempt.

### 3. Implement cancellation-safe page-by-page freeze

Freeze must perform this sequence:

1. Briefly lock workflow state, observe lifecycle terminal state, and linearize
   `Idle -> Freezing`. A repeated call in `Frozen` returns the original
   `FrozenPageBatchInfo` without acquiring a new fence.
2. Acquire the lifecycle checkpoint root-mutation lease. If metadata change,
   drop, or another root mutation wins, return structured cancellation and let
   the freeze guard restore `Idle` unless drop already closed the workflow.
3. Asynchronously scan and load the selected contiguous hot-page prefix using
   the existing page-granular row-budget behavior. Do not change page state
   during this fallible or cancellable phase.
4. Run a synchronous preflight over the loaded pages and confirm each selected
   page id/range still matches and each page state is `ACTIVE`. Workflow
   admission excludes another maintenance state change, while foreground row
   writers do not change page state.
5. For each selected page independently, acquire its page-state write lock,
   assert `ACTIVE`, publish `FROZEN`, and immediately release the lock. Do not
   retain one page's state lock while acquiring or mutating the next page.
6. Immediately after the last page publishes `FROZEN`, allocate the original
   snapshot fence, construct the canonical batch, and synchronously install
   workflow `Frozen` if the workflow remains open.
7. If drop closed the workflow during the synchronous publication loop, do not
   install or restore a batch. Return terminal cancellation; frozen pages may be
   left in the closed runtime because foreground access is terminal and runtime
   destruction waits for exclusive `Arc<Table>` ownership.
8. Release the root-mutation lease.

There must be no fallible operation or `.await` after the first page publishes
`FROZEN`. Ordinary future cancellation can only occur before that synchronous
section. An invariant panic after partial page-state publication is not a
reversible error; structure the helper and guard state so it poisons rather than
restoring a live `Idle` workflow with frozen pages.

The fence proof does not require overlapping page locks. Let `F` be allocated
after the last `FROZEN` publication:

- any transaction with `STS >= F` starts after all selected state publications
  and must observe `FROZEN` before an insert or in-place update;
- a transaction with `STS < F` may have modified a not-yet-frozen page, but
  task-000217 validation treats unresolved pre-fence status as blocking and
  requires every committed image-producing CTS to be below the checkpoint
  cutoff;
- lock/delete changes remain representable overlays.

Remove the current silent stop on `RowPageState::Transition` and idempotent
acceptance of already-frozen pages from the admitted freeze path. Once workflow
admission excludes overlap, unexpected `FROZEN` or `TRANSITION` pages are
internal invariant failures.

### 4. Consume the canonical batch in checkpoint attempts

Checkpoint must briefly claim workflow state before asynchronous work:

- move `Frozen(batch) -> Checkpointing(Frozen)` and move the batch into the
  checkpoint attempt guard; or
- move `Idle -> Checkpointing(Idle)` for heartbeat, cold-delete-only, silent
  watermark, or allocation-reachability work.

Then:

1. Verify the session is idle and acquire the lifecycle root-mutation lease.
2. Perform the definitive active-root effective-timestamp readiness check once,
   under that lease and before `MutableTableFile::fork()`. A delay restores the
   batch or `Idle`; do not keep a separate scheduling preflight API.
3. Capture one runtime layout, fork the mutable root, select the purge-published
   cutoff, and open the checkpoint transaction.
4. For a frozen source, validate the exact canonical page list and cached
   task-000217 cutoff state. Any root or frozen-page delay occurs before publish
   admission or page transition and restores the updated canonical batch.
5. Preserve existing atomic preparation of LWC, block-index, cold-delete,
   secondary `DiskTree`, and allocation-reachability state.

Ordinary errors before irreversible publication must roll back the checkpoint
transaction and let the attempt guard restore `Frozen` or `Idle`. Future
cancellation before irreversible publication has the same workflow result.

### 5. Linearize publish admission with workflow phase

For a frozen-page checkpoint, keep the current whole-batch validation and
transition correctness boundary in this task. After the entire batch validates
but while the page-state write locks are still held:

1. Briefly lock the workflow mutex and verify this attempt still owns
   `Checkpointing(Frozen)`.
2. While holding only that short workflow mutex plus the already-required page
   state guards, call the nonblocking lifecycle
   `try_begin_checkpoint_publish()` atomic operation.
3. If drop won lifecycle admission first, leave workflow reversible, release
   page locks, roll back, and return `Cancelled(TableDropping)`. Drop closure
   prevents the attempt guard from restoring the batch.
4. If checkpoint wins, record workflow `Transition` before releasing the
   workflow mutex, arm the fatal transition publication guard, change every
   selected page to `TRANSITION`, and capture the representable lock/delete
   overlays.
5. Release all page-state guards before LWC construction or any `.await`.

Holding the workflow mutex across the publish compare-exchange and matching
phase update closes the cross-state race without a blocking wait:

- if drop changes lifecycle to `Dropping` first, publish admission fails;
- if checkpoint changes lifecycle to `Publishing` first, workflow records the
  irreversible owner before drop can close workflow state;
- drop may then close `Transition -> Closed`, but the already-admitted attempt
  continues from owned batch/transaction state and the publish lease.

For an idle checkpoint with no page transition:

- keep reversible CoW construction outside the publish lease;
- immediately before a table-root publication, briefly lock workflow state,
  acquire publish admission, and record `Publishing` in the same short critical
  section;
- immediately before silent-watermark catalog DML, acquire the same admission
  and record `Publishing`, then retain the lease through transaction commit.

Hold the publish lease through table-root publication when present, runtime
block-index pivot/root installation, old-root retention, and checkpoint
transaction commit. If a no-transition attempt fails before root publication or
commit becomes irreversible, complete its rollback while still holding the
lease and finish `Publishing -> Idle` unless drop already closed the workflow.
On healthy completion, likewise change `Transition` or `Publishing` to `Idle`
unless drop has already changed the workflow to `Closed`, then release the
publish and root-mutation leases.

After the first page enters `TRANSITION`, or after irreversible root/silent
publication begins, cancellation or unexpected failure must not restore the
batch. Preserve the current fatal poison and route-waiter wake behavior.

### 6. Integrate asynchronous drop drain and terminal closure

Split lifecycle drop admission from its potentially long publisher drain. The
exact private API shape may vary, but it must support this ordering:

```rust
let drain = table.lifecycle.start_drop(table_id)?;
table.checkpoint_workflow.close();
drain.wait().await;
```

`start_drop` synchronously performs the current terminal lifecycle transition,
closes new publish admission, and returns whether an active publisher must
drain. It must not wait while holding a mutex. Workflow closure then:

- discards `Frozen` immediately;
- converts `Freezing` and reversible `Checkpointing` to `Closed`, so their
  guards cannot resurrect state;
- converts `Publishing` or `Transition` to `Closed` while allowing the attempt
  that already owns the publish lease to finish;
- rejects all later maintenance admission.

If a publisher won, `drain.wait()` uses the existing event-listener mechanism
and is fully asynchronous. Drop must not wait for `Frozen`, `Freezing`, a
pre-publish checkpoint, root readiness, or checkpoint backoff. It waits only for
an attempt that already crossed publish admission.

Add an armed catalog drop-progress guard after the lifecycle terminal gate is
crossed. Disarm it only after drop commit and runtime retention complete. If the
`DROP TABLE` future is abandoned while asynchronously waiting for a publisher or
during later post-gate work, poison storage instead of reopening `Live` or
leaving a seemingly healthy terminal runtime.

Require:

- `mark_dropped_lifecycle()` to assert workflow `Closed` and a fully drained
  lifecycle publish gate;
- `destroy_dropped_runtime()` to consume and assert a closed workflow;
- recovery drop replay and failed create-table cleanup to perform an explicit
  offline `Idle -> Closed` before consuming a runtime;
- recovery/bootstrap construction of a live table to initialize workflow
  `Idle` with no persisted workflow payload.

### 7. Preserve the foreground performance boundary

Do not read or mutate `TableCheckpointWorkflow` from foreground statement,
transaction, row, index, or scan code. The new workflow adds no per-row or
per-transaction mutex, atomic, status publication, or retained reference.

Foreground behavior remains:

| Workflow/page period | Reads | Inserts | Updates/deletes on selected pages |
| --- | --- | --- | --- |
| async freeze loading | normal | normal | normal |
| page-by-page freeze publication | normal | one page may briefly wait, then use the active suffix | one page may briefly wait; frozen update becomes a move and frozen delete remains allowed |
| `Frozen` / reversible checkpoint | normal | cached frozen page is rejected and insertion uses an active/new suffix page | update is a move; delete remains allowed |
| batch validation boundary | page-state locks do not block reads | may wait on selected state locks and retry elsewhere | may wait on selected state locks |
| `TRANSITION` | normal MVCC routing | active suffix; no cold-route wait | asynchronously wait for route publication or poison |
| post-pivot publication | normal snapshot/root routing | normal | waiter wakes and retries through cold routing |

The earlier publish-lease acquisition adds one checkpoint-frequency atomic
compare-exchange and one short workflow phase update immediately before page
transition. Foreground DML does not acquire or wait on the publish lease. The
lease only makes drop wait for the complete irreversible transition period,
which is required once page routing cannot be rolled back.

Potentially long conflict waits must be asynchronous:

- drop waiting for a publisher;
- metadata change waiting for root mutation;
- logical lock-manager waits;
- foreground transition-route waits;
- checkpoint I/O, fsync, and commit completion.

Concurrent maintenance calls and metadata/root conflicts return structured
cancellation rather than queueing. Root/GC-horizon pressure returns a structured
delay rather than waiting inside checkpoint.

Short `parking_lot` workflow and page-state locks remain synchronous and must
not be held across `.await`. One existing limitation is deliberately preserved:
final task-000217 batch validation currently holds all selected page-state write
locks while walking undo chains and capturing transition state, so foreground
writers can synchronously pause during that section. Converting the hot-path
page-state lock to an async lock would affect every row write, while the preferred
solution is backlog 000151's page-local prepared transition plan. Document this
known limitation and ensure this task neither lengthens that section nor adds a
new long blocking section.

### 8. Migrate callers, tests, and living documentation

Migrate all current test and internal call sites to:

- use mutable sessions for freeze;
- inspect `FreezeOutcome` and `FrozenPageBatchInfo` instead of owning a batch;
- retry `checkpoint_table` directly on `CheckpointOutcome::Delayed`;
- use private root-readiness helpers only in focused internal tests where a
  non-publishing observation is specifically required.

Update living documentation to describe table-owned canonical workflow state,
page-by-page freeze publication, internal root readiness, publish admission
before transition/silent commit, asynchronous drop drain, and volatile recovery
initialization:

- `docs/checkpoint-and-recovery.md`
- `docs/transaction-system.md`
- `docs/table-file.md`

Keep backlog 000151 open and explicitly separate from this correctness/ownership
task.

## Implementation Notes

- Implemented one volatile `TableCheckpointWorkflow` per user table. It owns the
  canonical frozen-page batch, original fence, validation cache, and reversible
  versus irreversible maintenance phase, while `TableLifecycle` remains the
  authority for terminal state, metadata/root exclusion, and publish/drop
  admission. Freeze publishes page state one page at a time, checkpoint attempt
  guards restore reversible state, and drop closes the workflow before runtime
  destruction while asynchronously draining only a publisher that already won
  admission.
- Replaced caller-owned freeze/checkpoint state with `FreezeOutcome` and
  `FrozenPageBatchInfo`; `Session::freeze_table` now requires mutable session
  ownership and `checkpoint_table` consumes the table-owned batch or runs the
  idle checkpoint path. The explicit-batch checkpoint and standalone readiness
  APIs were removed. Review hardening marked `FreezeOutcome` as `must_use` and
  made test setup assert `Frozen`, with explicit `AlreadyFrozen` and
  cancellation assertions where those outcomes are intentional.
- Moved publish admission before page transition and retained it through root or
  silent-watermark publication, route installation, and transaction commit.
  Recovery initializes live workflow state as idle, and recovery/drop cleanup
  explicitly closes offline runtimes before destruction. Living checkpoint,
  transaction, and table-file documentation was synchronized with these
  ownership and ordering contracts.
- Dropped-runtime destruction now filters historical `RowPageIndex` entries
  below the published pivot because purge may already have reclaimed those row
  pages. This is a safety bridge rather than the planned long-term cleanup;
  actual left-prefix index deletion before row-page deallocation is deferred to
  backlog 000155.
- Verification completed with branch-diff style audit over 22 Rust files,
  `cargo fmt --all -- --check`, strict workspace clippy, 1323 workspace nextest
  tests, and 1247 `libaio` nextest tests. Focused coverage across the nine
  workflow, persistence, lifecycle, catalog, recovery, table, hot-row, and
  session files was 93.88%, above the repository's 80% review bar.

## Impacts

- `doradb-storage/src/table/checkpoint_workflow.rs`
  - new workflow state, canonical batch ownership, public batch summary, and
    reversible attempt guards.
- `doradb-storage/src/table/mod.rs`
  - own and initialize the workflow;
  - keep frozen page metadata and validation helpers table-private;
  - enforce live page-layout and close-before-destroy invariants.
- `doradb-storage/src/table/persistence.rs`
  - page-by-page freeze publication and fence installation;
  - remove compatibility batch reconstruction and public readiness;
  - consume/restore the table-owned batch;
  - move publish admission before transition and silent-watermark DML.
- `doradb-storage/src/table/lifecycle.rs`
  - split synchronous drop admission from asynchronous publish drain;
  - preserve metadata/root exclusion and atomic publish gate semantics;
  - extend cancellation vocabulary and phase-integration tests.
- `doradb-storage/src/session.rs`
  - make `freeze_table` take `&mut self` and return `FreezeOutcome`;
  - remove explicit-batch checkpoint and standalone readiness methods.
- `doradb-storage/src/lib.rs`
  - export `FreezeOutcome` and `FrozenPageBatchInfo`;
  - stop exporting caller-owned batch and readiness types.
- `doradb-storage/src/catalog/table.rs`
  - integrate workflow closure into drop and staged-runtime cleanup;
  - add terminal drop-future abandonment poison protection.
- `doradb-storage/src/recovery/mod.rs` and `doradb-storage/src/table/recover.rs`
  - initialize recovered live workflows as idle and close offline runtimes
    before consuming destroy.
- `doradb-storage/src/table/access.rs`, `doradb-storage/src/table/gc.rs`,
  `doradb-storage/src/table/rollback.rs`, catalog/index tests, session tests, and
  recovery tests
  - migrate freeze/checkpoint call sites without adding foreground workflow
    dependencies.
- `docs/checkpoint-and-recovery.md`, `docs/transaction-system.md`, and
  `docs/table-file.md`
  - synchronize living workflow, performance, and recovery contracts.

## Test Cases

1. New and recovered live tables initialize workflow `Idle`; no workflow data is
   persisted.
2. A first freeze returns `FreezeOutcome::Frozen` with the original table id,
   fence, approximate rows, page count, and zero initial stable pages.
3. Repeated freeze in `Frozen` returns `AlreadyFrozen` with identical fence and
   page set information, ignores a different row budget, and does not reset
   cached validation.
4. Same-session overlapping freeze/checkpoint is prevented by mutable `Session`
   borrowing; different-session overlap returns the defined structured reason.
5. Concurrent freeze while `Freezing` returns `FreezeInProgress`.
6. Freeze or checkpoint while `Checkpointing`, `Publishing`, or `Transition`
   returns `CheckpointInProgress`.
7. Cancelling freeze during asynchronous page loading restores `Idle` and leaves
   every selected page `ACTIVE`.
8. Page-by-page freeze blocks a writer only on the page whose state is being
   changed; writers can progress on later active pages.
9. A writer that modifies a later selected page during the page-by-page freeze
   has `STS < frozen_ts` and is correctly delayed or validated by task-000217
   cutoff logic.
10. A post-fence insert/update cannot modify the frozen prefix and uses the
    active suffix/move-update path.
11. Encountering `FROZEN` or `TRANSITION` during an admitted idle freeze is an
    invariant failure rather than silent scan termination.
12. Active-root delay under the root-mutation lease restores the exact canonical
    batch or restores `Idle` for an idle checkpoint.
13. Frozen-page cutoff delay restores the batch with its updated stable-page
    validation cache; retry reuses that cache.
14. Cancelling a checkpoint before publish admission restores `Frozen` or
    `Idle` and leaves every selected page out of `TRANSITION`.
15. If drop wins lifecycle admission while checkpoint is validated, publish
    admission returns `TableDropping`, no page transitions, and workflow closure
    prevents batch resurrection.
16. If checkpoint wins publish admission, drop closes workflow and waits
    asynchronously until root/route publication and checkpoint commit finish.
17. The checkpoint transaction commit timestamp precedes the drop transaction
    commit timestamp when drop waits for that publisher.
18. Drop discards a delayed `Frozen` batch without waiting and later runtime
    destruction observes workflow `Closed`.
19. Drop closes an active `Freezing` or reversible `Checkpointing` attempt; its
    future/guard cannot restore state afterward.
20. Dropping a `DROP TABLE` future after the terminal gate poisons storage and
    never restores lifecycle or workflow state.
21. Silent-watermark checkpoint holds publish admission through catalog DML and
    commit; drop either cancels it before admission or asynchronously waits for
    its earlier commit.
22. Deletion-only/root-only checkpoint uses `Publishing` admission without page
    transition and has the same drop ordering.
23. Metadata change may proceed while a batch is merely `Frozen`; freeze or
    checkpoint attempts cancel while metadata change is active.
24. Metadata change waiting for active root mutation, drop waiting for publish,
    and foreground waiting for transition route use explicit event-driven async
    coordination without sleeps or blocking mutex waits.
25. Reads continue while selected pages are `FROZEN` and `TRANSITION`.
26. Inserts continue on the active suffix during `FROZEN` and `TRANSITION`.
27. Updates/deletes reaching `TRANSITION` sleep asynchronously and wake after
    pivot/column-root installation; fatal checkpoint failure wakes them through
    storage poison.
28. A successful checkpoint returns workflow to `Idle` before releasing publish
    admission unless drop already closed it; the live hot suffix is `ACTIVE`.
29. A post-transition build, root publication, route installation, or commit
    failure preserves fatal fail-closed behavior and never restores a batch.
30. Recovery drop replay and failed create-table cleanup explicitly close an
    idle workflow before consuming runtime destroy.
31. Existing task-000217 stale-cutoff, cold-delete, heartbeat, secondary-index,
    metadata DDL, root-reachability, redo recovery, and drop lifecycle tests
    continue to pass after API migration.
32. Concurrent tests use hooks, channels, events, or predicate signaling rather
    than sleeps.
33. Run `cargo fmt --all --check`.
34. Run `cargo clippy --workspace --all-targets -- -D warnings`.
35. Run `cargo nextest run --workspace`.
36. Run `cargo nextest run -p doradb-storage --no-default-features --features libaio`
    because checkpoint I/O sequencing and publication admission are changed.
37. Run focused coverage for the changed table workflow, persistence, lifecycle,
    catalog drop, and recovery paths; meet the repository's 80% focused coverage
    review bar or explain definition-heavy exceptions with covered consumers.

## Open Questions

No blocking design questions remain. The following improvements stay explicitly
outside this task:

- `docs/backlogs/000151-optimize-frozen-page-checkpoint-transition-planning.md`
  tracks the existing batch-wide checkpoint validation lock duration, fused
  transition analysis, page-local irreversible transition, and foreground
  delete/update tail-latency measurement.
- `docs/backlogs/000153-remove-obsolete-global-catalog-namespace-lock.md`
  tracks removal of the coarse catalog namespace lock so a checkpoint drain for
  one table does not delay unrelated-table DDL.
- `docs/backlogs/000154-move-table-checkpoint-completion-to-no-wait-system-transactions.md`
  tracks a checkpoint-specific no-wait system transaction while preserving
  redo, silent-watermark, DROP ordering, and purge guarantees.
- `docs/backlogs/000155-add-block-index-range-deletion-for-purged-row-pages.md`
  tracks left-prefix `BlockIndex` deletion, balancing/root collapse, and unlink
  before purge deallocates checkpointed row pages.
