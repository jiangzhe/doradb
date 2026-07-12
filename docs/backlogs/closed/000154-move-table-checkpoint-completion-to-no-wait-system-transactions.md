# Backlog: Move table checkpoint completion to no-wait system transactions

## Summary

Replace the normal session transaction used to finalize a user-table checkpoint with a checkpoint-specific system transaction that joins ordered group commit without waiting for redo persistence. Preserve durable table-root publication, catalog-backed silent-watermark proof, DROP ordering, and safe row-page reclamation while removing the foreground durability wait from checkpoint completion.

## Reference

Discovered while implementing docs/tasks/000218-unify-table-freeze-checkpoint-workflow-state.md and reviewing doradb-storage/src/table/persistence.rs, doradb-storage/src/trx/sys_trx.rs, doradb-storage/src/catalog/checkpoint.rs, and docs/tasks/000199-catalog-silent-table-replay-watermarks.md. The current checkpoint opens a normal transaction primarily to obtain a timestamp, carry redo and retired row pages, perform silent-watermark catalog DML, and wait for durability.

## Deferred From (Optional)

docs/tasks/000218-unify-table-freeze-checkpoint-workflow-state.md

## Deferral Context (Optional)

- Defer Reason: Task 000218 is scoped to volatile freeze/checkpoint workflow ownership and explicitly preserves the durable checkpoint, redo, and recovery model. This follow-up changes transaction payloads, group-commit acknowledgement semantics, catalog mirror mutation, purge handoff, and public checkpoint outcome documentation, so folding it into 000218 would obscure and materially expand that task.
- Findings: The normal transaction is not required to define checkpoint consistency: the canonical frozen batch, cutoff validation, purge-published horizon, and lifecycle publish lease provide that boundary. It currently supplies four replaceable services: an early timestamp, redo construction, retired-row-page GC handoff, and transactional silent-watermark upsert with a persistence waiter. Existing SysTrx supports only redo and has neither catalog-DML helpers nor GC payload. The live catalog.table_replay_silent_watermarks row is not truncation proof; the checkpointed catalog root rebuilt from persisted redo is authoritative. Therefore a no-wait system path is safe only if it emits the same logical catalog DML, not merely the DDL marker. Because a system CTS is assigned only at enqueue but root building needs an earlier timestamp, the design needs a separate non-active checkpoint fence. DROP safety follows by holding the per-table publish lease through system enqueue: DROP queues later, so its durable normal commit implies durability of the earlier system redo. System-owned GC pages need a purge payload that does not remove an active STS and uses the system CTS as the reclamation fence.
- Direction Hint: Prefer a narrow CheckpointSysTrx builder over routing checkpoint work through a detached normal session transaction. Reuse the existing redo format and encode the silent-watermark Insert or UpdateByPrimaryKey catalog operation alongside TableReplaySilentWatermark, avoiding a new DDL payload format unless planning finds a concrete simplification. Apply the live watermark through a dedicated monotonic no-transaction upsert under the per-table publish lease and treat it as an inspection mirror only. Extend prepared, precommit, and committed payloads with an explicit system-GC variant rather than pretending the system transaction owns a user STS. Keep the root/build timestamp distinct from the ordered redo CTS, and revisit the Published outcome wording because silent redo is merely enqueued at return. Ensure an explicit catalog checkpoint can establish or wait for a durable redo upper bound so a just-enqueued silent watermark is not missed nondeterministically.

## Scope Hint

Design and implement a checkpoint-specific system transaction on the no-wait group-commit path. Separate the checkpoint operation timestamp used by table roots and DiskTree blocks from the system redo CTS assigned at enqueue; emit DataCheckpoint redo or TableReplaySilentWatermark plus the equivalent logical catalog upsert redo; maintain the live catalog watermark row as a no-transaction inspection mirror; retain the table publish lease through mirror publication and system enqueue; hand retired row pages to purge only after ordered redo completion; and define accurate CheckpointOutcome acknowledgement semantics, including deterministic behavior for an immediately following catalog checkpoint.

## Acceptance Hint

Table checkpoint no longer waits for redo persistence. Root checkpoints return only after the table root is durable and routing is installed; silent checkpoints return only after the watermark mirror and system redo are accepted. A later DROP transaction is ordered after the checkpoint system transaction and its durability wait proves the earlier redo prefix. Retired row pages are not reclaimed until the system redo completes and the global active-snapshot horizon crosses its CTS. Catalog checkpoint and recovery fold the silent-watermark logical DML from persisted redo, while unpersisted or mirror-only state never authorizes truncation. Synchronous enqueue failure or asynchronous redo failure after irreversible publication poisons the engine. Add root, silent, DROP race, immediate catalog-checkpoint, restart, truncation, GC, and injected redo-failure tests.

## Notes (Optional)


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
- Detail: Implemented via docs/tasks/000220-move-table-checkpoint-completion-to-no-wait-system-transactions.md
- Closed By: backlog close
- Reference: docs/tasks/000220-move-table-checkpoint-completion-to-no-wait-system-transactions.md
- Closed At: 2026-07-12
