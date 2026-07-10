# Backlog: Unify table freeze and checkpoint workflow state

## Summary

Design a table-owned state machine for the complete user-table freeze-to-checkpoint workflow after task 000217 lands its cutoff correctness fix. Replace implicit coordination through caller-owned `FrozenPageBatch` values and page-state scans with an explicit per-table phase contract that owns the canonical batch, defines repeated and concurrent calls, coordinates with `TableLifecycle`, and makes `ACTIVE -> FROZEN -> TRANSITION` page states consequences of valid workflow transitions rather than signals that scans silently skip.

## Reference

Follow-up design discussion during `docs/tasks/000217-redesign-checkpoint-frozen-page-cutoff-validation.md`, prompted by `Table::freeze` silently stopping when it encounters `RowPageState::Transition`. Relevant implementation is concentrated in `doradb-storage/src/table/persistence.rs`, `doradb-storage/src/table/mod.rs`, `doradb-storage/src/table/lifecycle.rs`, `doradb-storage/src/session.rs`, and `doradb-storage/src/trx/ver_map.rs`. This is related to but distinct from `docs/backlogs/000151-optimize-frozen-page-checkpoint-transition-planning.md`: backlog 000151 optimizes transition analysis and locking after correctness, while this item defines workflow ownership, API ordering, concurrency, and drop-table coordination.

## Deferred From (Optional)

`docs/tasks/000217-redesign-checkpoint-frozen-page-cutoff-validation.md`; follow-up to the correctness issue recorded by `docs/backlogs/000149-redesign-checkpoint-frozen-page-cutoff-validation.md`; complementary to but not part of `docs/backlogs/000151-optimize-frozen-page-checkpoint-transition-planning.md`

## Deferral Context (Optional)

- Defer Reason: Task 000217 must stay focused on the correctness boundary: validate actual undo-status CTS values, return retryable stale-cutoff outcomes, and prevent invalid LWC views. A complete freeze/checkpoint workflow redesign changes public APIs, batch ownership, table lifecycle coordination, cancellation behavior, drop-table ordering, and many checkpoint/recovery tests. Mixing that architectural work into the current fix would substantially enlarge its proof surface and delay the correctness repair. Land and validate task 000217 first, then use this backlog as input to a dedicated RFC or task design.
- Findings:
  The current page-state behavior exposes a broader coordination gap rather than a local `match` issue. `Table::freeze` accepts both `ACTIVE` and already-`FROZEN` pages, allocates a new batch fence, and silently stops on `TRANSITION`. Because freeze and checkpoint are separate public calls and `FrozenPageBatch` is caller-owned, multiple sessions can create overlapping batches, a later freeze can replace the conceptual fence, and a caller can lose the only batch carrying cached validation. The compatibility `checkpoint_table` path reconstructs a batch from current pages and allocates a later `frozen_ts`; that later fence is not equivalent to the original freeze boundary and can misclassify transactions that began between the two fences.

  `table_checkpoint_readiness` only probes the active-root GC-horizon condition. It does not own or validate a batch, does not reserve checkpoint admission, and cannot prevent a race between a ready observation and another freeze/checkpoint operation. The checkpoint call already returns structured root and frozen-page delay reasons, so a separate readiness API duplicates policy without establishing ordering.

  `TRANSITION` is reachable through valid concurrent public calls today, so replacing the silent stop with an assertion alone would turn supported concurrency into a panic. It becomes a valid invariant only after higher-level admission excludes concurrent freeze/checkpoint execution. The desired invariant is a page layout derived from workflow state: live `Idle` pages are `ACTIVE`; `Frozen` owns one frozen prefix followed by an active suffix; irreversible `Transition` owns exactly the canonical prefix.

  `TableLifecycle` and the proposed workflow solve different problems. Lifecycle is authoritative for `Live -> Dropping -> Dropped`, metadata/root exclusion, and the no-cancel publish gate. The workflow should own the volatile batch and semantic phase. Table-level operations must coordinate both rather than let either state machine override the other. Workflow mutexes must not be held across `.await`; RAII attempt guards should restore `Idle` or `Frozen` on ordinary errors and future cancellation.

  Drop behavior defines the critical cross-state contract. A pending `Frozen` batch is volatile and may be discarded once drop rejects foreground access; drop must not wait indefinitely for a delayed batch. A reversible `Freezing` or `Checkpointing` attempt must be prevented from restoring a batch after the workflow closes. Conversely, once any page enters `TRANSITION`, rollback is unsafe. The checkpoint must therefore acquire the existing publish lease before the synchronous validation-to-transition boundary. `begin_drop` then waits for the publish lease, allowing the checkpoint to complete, while pre-transition work cancels normally. Physical destruction already waits for exclusive `Arc<Table>` ownership, so reversible attempts can unwind after logical drop without use-after-free.

  Silent-watermark and deletion-only checkpoints also need a defined workflow path from `Idle`. Their irreversible catalog or root publication should use the same publish admission even when no row page transitions. Metadata changes should remain allowed while a batch is merely frozen and delayed; each concrete freeze/checkpoint attempt still uses the root-mutation lease to obtain a consistent layout.
- Direction Hint:
  Start future work as an RFC or a dedicated architectural task after 000217 is resolved. Prefer a table-owned canonical batch and a single checkpoint execution API. A candidate public surface is `freeze_table -> FreezeOutcome`, where outcomes distinguish newly frozen, already frozen, and cancelled work and carry a read-only `FrozenPageBatchInfo`; `checkpoint_table` consumes the table-owned batch or performs the existing heartbeat/cold-delete path from `Idle`. Remove the explicit-batch checkpoint and standalone readiness APIs unless research identifies a concrete caller that requires them.

  Implement a short-lock workflow state machine beside `TableLifecycle`, including terminal `Closed`. Claim the workflow phase before asynchronous work, acquire lifecycle root-mutation admission for each freeze/checkpoint attempt, and restore state with RAII guards. Freeze should load the selected prefix before changing page state, then acquire all selected state locks, assert `ACTIVE`, publish `FROZEN`, allocate the original fence, and install the canonical batch without later fallible work. Checkpoint should move the canonical batch into an attempt guard, perform root readiness once under the root lease, retain it on delay, and acquire the publish lease before entering `TRANSITION`. Every irreversible publication, including a silent watermark commit, needs a no-cancel boundary.

  Extend `Table::begin_drop_lifecycle` to close the workflow after lifecycle admission has stopped new publication and drained any held publish lease. Closing discards `Frozen`, prevents reversible attempt guards from restoring state, and permits `Transition` only as a transient state protected by the publish lease. Assert `Closed` before marking the runtime fully dropped or destroying it. Keep the workflow design separate from backlog 000151's performance work; establish ownership and correctness first, then optimize transition analysis and locking against the stable contract.

## Scope Hint

Research and specify a per-table checkpoint workflow with states equivalent to `Idle`, `Freezing`, `Frozen(canonical_batch)`, reversible `Checkpointing`, irreversible `Transition`, and terminal `Closed`. Make the table own the original freeze fence, page metadata, and validation cache. Define public freeze/checkpoint outcomes, repeated-freeze behavior, concurrent admission, cancellation safety, and whether the standalone readiness and explicit-batch checkpoint APIs should be removed. Coordinate workflow admission with the existing lifecycle root-mutation and publish leases. Remove silent state-based scan termination and enforce page-layout invariants only after concurrency admission makes them true. Include the interaction with metadata change, logical drop, physical runtime retention, silent watermark checkpoints, cold-delete checkpoints, and recovery initialization.

## Acceptance Hint

One authoritative frozen batch and fence exist per live table. Repeated freeze does not extend or replace the batch and returns an explicit already-frozen result. A delayed checkpoint retains the canonical batch and cached validation; successful publication clears it. Concurrent freeze/checkpoint calls receive structured cancellation and cannot manufacture overlapping or stale batches. No valid public race is handled by silently stopping at `TRANSITION`. Selected batch pages are asserted `FROZEN`, the remaining hot suffix is asserted `ACTIVE`, and `TRANSITION` is reachable only under the irreversible checkpoint phase. Checkpoint future cancellation restores reversible workflow state, while post-transition failure remains fatal. Drop discards pending frozen work without waiting, cancels reversible attempts without batch resurrection, and waits for an irreversible checkpoint through the existing publish lease. Runtime destruction observes a closed workflow. Existing task-000217 cutoff correctness, recovery, heartbeat, cold-delete, secondary-index, and metadata publication semantics remain unchanged. Tests cover the full phase matrix and drop races.

## Notes (Optional)

Candidate public information shape:

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

Candidate workflow transitions:

```text
Idle --freeze claim--> Freezing --publish batch--> Frozen
Frozen --checkpoint claim--> Checkpointing
Idle --heartbeat/delete checkpoint claim--> Checkpointing
Checkpointing --root/page delay--> Frozen or Idle
Checkpointing --publish lease + page transition--> Transition
Transition --publish + commit--> Idle
Idle|Freezing|Frozen|Checkpointing|Transition --drop close--> Closed
```

Cross-state rules to preserve:

1. `Transition` implies a live checkpoint attempt owns root-mutation and publish admission.
2. Drop may discard `Frozen` immediately but must wait for `Transition` by waiting for the publish lease.
3. A reversible guard observing `Closed` never restores `Idle` or `Frozen`.
4. Any failure after the first page transition poisons storage, matching the current transition publication guard.
5. A new freeze budget never extends an existing batch; extension would require per-page fences or another proof because replacing the original fence with a later timestamp is unsafe.
6. Recovery creates live workflow state as `Idle`; the state is volatile and is not a new durable format.

Future planning should explicitly review cancellation of async freeze/checkpoint futures, lock ordering between workflow and lifecycle state, the exact publish-lease boundary for silent watermark work, and migration of current tests that use readiness polling or caller-owned batches.

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

## Close Reason

- Type: implemented
- Detail: Implemented by the table-owned freeze and checkpoint workflow.
- Closed By: backlog close
- Reference: docs/tasks/000218-unify-table-freeze-checkpoint-workflow-state.md
- Closed At: 2026-07-10
