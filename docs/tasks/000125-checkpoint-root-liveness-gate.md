---
id: 000125
title: Checkpoint Root Liveness Gate
status: proposal
created: 2026-04-19
github_issue: 572
---

# Task: Checkpoint Root Liveness Gate

## Summary

Make user-table checkpoint publication explicitly gated by table-file root
liveness. A checkpoint may overwrite the inactive A/B super-block slot only when
the currently active root's checkpoint timestamp has crossed the
`Global_Min_Active_STS` horizon. The check must be caller-visible so background
checkpoint scheduling can distinguish "run now" from "delay and retry later".

This task should add checkpoint readiness and outcome types, return a normal
delayed outcome when the root horizon has not advanced far enough, and preserve a
final execution-time guard before checkpoint state is mutated or a root is
published. It should also document that `gc_block_list` is not the future block
reclamation mechanism; future table-file/DiskTree block reclamation should be
based on root reachability, including recovery-time reclamation when no active
transactions exist.

## Context

Table files publish persistent user-table state through a copy-on-write root
swap. Each checkpoint writes new blocks and a new metadata root, then atomically
switches one of two super-block root slots. The previous active root remains
owned by an `OldRoot` guard and is currently retained through the checkpoint
transaction payload until purge advances past the checkpoint transaction's CTS.

That old-root retention fixes in-memory pointer lifetime, but it does not give a
caller-visible policy for whether the next checkpoint is allowed to overwrite
the inactive A/B slot. If checkpoints continue to publish while long-running
readers keep the GC horizon pinned, runtime may need to reason about more than
the two durable super-block roots. That makes future root-reachability GC harder
than the table-file A/B design needs to be.

The intended rule is intentionally conservative:

```text
active_root.trx_id < trx_sys.calc_min_active_sts_for_gc()
```

Here `active_root.trx_id` is the checkpoint CTS of the root currently active in
the table file. It is also the transaction horizon that protects the inactive
slot's previous root: once that CTS is older than the oldest active snapshot, no
active transaction should still require the root that would be displaced by the
next A/B swap. The current active root may still be captured by transactions that
started after that CTS; after publication it is protected by the new checkpoint
transaction's old-root retention.

Checkpoint already depends on the same horizon for data correctness. Frozen row
pages can be moved into transition and encoded into LWC blocks only after the
selected rows are committed-visible to all active transactions. When a
long-running reader pins `min_active_sts`, checkpoint work should be delayed
rather than silently publishing another metadata root.

Issue Labels:
- type:task
- priority:high
- codex

Source Backlogs:
- docs/backlogs/000090-protect-disk-tree-root-lifetime.md

Related Design:
- docs/architecture.md
- docs/transaction-system.md
- docs/index-design.md
- docs/checkpoint-and-recovery.md
- docs/table-file.md
- docs/garbage-collect.md
- docs/tasks/000124-checkpoint-old-root-retention.md
- docs/rfcs/0014-dual-tree-secondary-index.md
- docs/process/unit-test.md

## Goals

1. Add a caller-visible user-table checkpoint readiness API that reports whether
   checkpoint can run immediately or should be delayed.
2. Make root liveness part of that readiness decision using the strict predicate
   `active_root.trx_id < min_active_sts`.
3. Change user-table checkpoint execution to return a caller-observable outcome
   such as `Published` or `Delayed`, instead of hiding root-liveness failure as a
   silent no-op.
4. Include root and horizon details in the delayed reason so callers can log,
   schedule retry/backoff, or expose checkpoint pressure diagnostics.
5. Keep an execution-time liveness guard tied to the root snapshot that will be
   published from, so a stale preflight decision cannot race with another root
   publication.
6. Ensure checkpoint returns `Delayed` before moving frozen pages to transition,
   applying deletion checkpoint state, publishing DiskTree roots, or swapping the
   table-file root.
7. Preserve existing `OldRoot` retention for successfully published checkpoints;
   the root that is current before the publish still becomes the new retained old
   root until the publishing checkpoint crosses the purge horizon.
8. Treat root-liveness delay as normal scheduling pressure, not storage poison,
   checkpoint write failure, or transaction rollback failure.
9. Mark `gc_block_list` as legacy/retired for future user-table block reclaim
   design, and avoid adding new reclamation behavior that depends on it.
10. Document future root-reachability GC as the unified reclaim mechanism for
    table-file metadata, ColumnBlockIndex nodes, LWC replacement blocks, and
    DiskTree blocks.
11. Document that recovery-time root-reachability GC can reclaim blocks
    unreachable from the selected latest valid root because restart has no active
    transactions.
12. Add regression coverage for ready, delayed, and delayed-then-ready checkpoint
    flows.

## Non-Goals

1. Do not implement full table-file or DiskTree root-reachability GC in this
   task.
2. Do not implement recovery-time block reclamation in this task.
3. Do not remove or change the persisted table-file metadata format solely to
   delete `gc_block_list`.
4. Do not implement page reuse, compaction, allocator rebuild, or free-list
   draining for user-table files.
5. Do not change catalog `MultiTableFile` checkpoint semantics unless a small
   shared helper is required by the user-table implementation.
6. Do not redesign foreground row, index, or DiskTree read paths.
7. Do not remove the existing old-root transaction-payload retention introduced
   by task 000124.
8. Do not turn root-liveness delay into an error that poisons storage or forces
   checkpoint callers to parse generic `Error` variants.

## Unsafe Considerations

No unsafe code changes are expected. The intended implementation changes safe
Rust checkpoint APIs, result enums, scheduling decisions, and documentation.

If implementation unexpectedly touches unsafe pointer, buffer, file, or B+Tree
node code, keep the unsafe boundary private, document each invariant with a
`// SAFETY:` comment, refresh the unsafe inventory if needed, and apply
`docs/process/unsafe-review-checklist.md` before resolving this task.

## Plan

1. Add checkpoint readiness and outcome types.
   - Define a caller-facing readiness type in or near
     `doradb-storage/src/table/persistence.rs`.
   - Include enough data for scheduling and diagnostics, for example
     `root_cts`, `min_active_sts`, and a delay reason.
   - Define a checkpoint execution outcome, for example:

     ```rust
     pub enum CheckpointOutcome {
         Published { checkpoint_ts: TrxID },
         Delayed { reason: CheckpointDelayReason },
     }
     ```

   - Keep exact names local-style, but preserve the semantic split between
     storage errors and normal checkpoint delay.

2. Add a cheap readiness/preflight API.
   - Add a method such as `checkpoint_readiness(...)` to `TablePersistence` or
     a nearby user-table checkpoint API.
   - Compute `min_active_sts` with
     `TransactionSystem::calc_min_active_sts_for_gc()`.
   - Bind one local `active_root` reference before reading root fields.
   - Return delayed when `active_root.trx_id >= min_active_sts`.
   - Do not require a mutable `Session` for the preflight check unless local
     API style makes that unavoidable.

3. Change checkpoint execution to report outcomes.
   - Change `TablePersistence::checkpoint` from `Result<()>` to
     `Result<CheckpointOutcome>` or an equivalent caller-visible result.
   - At the start of checkpoint execution, run the same readiness check and
     return `CheckpointOutcome::Delayed` before beginning checkpoint mutation
     work when the root is not live-safe.
   - Keep existing storage, IO, session-state, and invalid-state failures as
     `Err(...)`.

4. Tie the execution guard to the root that will be published from.
   - Recheck root liveness after frozen-page stabilization refreshes
     `min_active_sts` and before any irreversible checkpoint preparation step.
   - Move `MutableTableFile::fork` or a new checked fork helper early enough that
     the mutable root snapshot being checked is the one used for publication.
   - If that checked root is not live-safe, release the mutable claim and return
     `Delayed` before calling `set_frozen_pages_to_transition`.
   - This avoids a time-of-check/time-of-use gap between caller preflight and the
     root actually displaced by `MutableTableFile::commit`.

5. Preserve current successful publication semantics.
   - Keep checkpoint transaction creation, LWC block building, deletion
     checkpoint application, secondary DiskTree sidecar publication, table-file
     root publish, block-index root update, and old-root retention behavior for
     ready checkpoints.
   - The checkpoint that publishes a new root must still retain the previous
     active root through `ActiveTrx::retain_old_table_root`.
   - Do not publish a metadata heartbeat root when the readiness result is
     delayed.

6. Update call sites.
   - Update direct callers of `TablePersistence::checkpoint` to handle
     `CheckpointOutcome`.
   - Tests and recovery scaffolding that require a successful checkpoint should
     assert `Published`.
   - Future background checkpoint scheduling should be able to inspect
     `Delayed` without treating it as failure.

7. Retire `gc_block_list` from the forward design.
   - Update docs and comments that currently imply `gc_block_list` is the future
     user-table page-reclamation mechanism.
   - Keep parsing and serialization compatibility unless a separate migration is
     explicitly scoped.
   - Avoid wiring new block reclamation to `record_gc_block`.
   - Where existing code records obsolete blocks, keep behavior compatible but
     document that root-reachability GC will replace this list for user-table
     block reclaim.

8. Update concept docs.
   - Update `docs/table-file.md` to describe checkpoint readiness, the A/B slot
     overwrite rule, and `GC_Wait` to `Free` as future root-reachability GC.
   - Update `docs/checkpoint-and-recovery.md` to state checkpoint delay is a
     normal outcome when long-running readers pin the root horizon.
   - Update `docs/garbage-collect.md` to clarify that DiskTree and table-file
     block reclaim is rooted at reachable checkpoint roots, with recovery allowed
     to reclaim unreachable blocks because there are no active transactions.

## Implementation Notes

Keep this section blank in design phase. Fill this section during `task resolve`
after implementation, tests, review, and verification are completed.

## Impacts

- `doradb-storage/src/table/persistence.rs`
  - `TablePersistence`
  - checkpoint readiness/outcome types
  - `Table::checkpoint`
  - frozen-page transition ordering
  - deletion and secondary sidecar checkpoint sequencing
- `doradb-storage/src/file/table_file.rs`
  - `MutableTableFile::fork`
  - `MutableTableFile::commit`
  - possible checked fork or root-liveness helper integration
- `doradb-storage/src/file/cow_file.rs`
  - `ActiveRoot::trx_id`
  - active-root caller contract comments
  - `gc_block_list` comments if they continue to imply future reclaim
- `doradb-storage/src/trx/purge.rs`
  - `TransactionSystem::calc_min_active_sts_for_gc`
  - existing purge horizon behavior used by the root gate
- `doradb-storage/src/trx/mod.rs`
  - existing old-root retention must remain compatible with delayed outcomes
- `doradb-storage/src/index/column_block_index.rs`
  - existing `record_obsolete_node` use should not become the future reclaim
    contract
- `doradb-storage/src/catalog/storage/checkpoint.rs`
  - existing `record_gc_block` use should be audited for documentation impact
- `doradb-storage/src/index/disk_tree.rs`
  - tests and comments that assert DiskTree rewrites do not populate
    `gc_block_list` should remain consistent with root-reachability direction
- `docs/table-file.md`
- `docs/checkpoint-and-recovery.md`
- `docs/garbage-collect.md`
- `docs/process/unit-test.md`

## Test Cases

1. `checkpoint_readiness` returns ready when the active root CTS is below
   `min_active_sts`.
2. `checkpoint_readiness` returns delayed with `root_cts` and `min_active_sts`
   when the active root CTS is equal to or newer than `min_active_sts`.
3. `Table::checkpoint` returns `CheckpointOutcome::Delayed` without publishing a
   new root while a long-running transaction pins the horizon.
4. A delayed checkpoint leaves `active_root.trx_id`, `meta_block_id`,
   `pivot_row_id`, `heap_redo_start_ts`, `deletion_cutoff_ts`,
   `column_block_index_root`, and `secondary_index_roots` unchanged.
5. A delayed checkpoint does not move frozen row pages into transition state.
6. After the long-running reader commits and purge/min-active advances, the same
   table can checkpoint successfully and returns `Published`.
7. A successful checkpoint still retains the previous active root through the
   checkpoint transaction payload and releases it after purge reaches the
   publishing checkpoint CTS.
8. A second checkpoint cannot overwrite the inactive A/B slot until the previous
   checkpoint root CTS crosses the min-active horizon.
9. Recovery tests that need durable user-table checkpoint state assert
   `Published` and continue to load the latest valid table root.
10. Secondary DiskTree checkpoint tests cover delayed-then-published behavior so
    root updates are not partially visible after a delay.
11. Existing tests around old-root retention, rollback, precommit abort, and
    purge release continue to pass.
12. Run the supported validation pass:

    ```bash
    cargo nextest run -p doradb-storage
    ```

13. If implementation changes storage backend or backend-neutral IO paths, also
    run:

    ```bash
    cargo nextest run -p doradb-storage --no-default-features --features libaio
    ```

## Open Questions

1. Full table-file and DiskTree root-reachability GC remains follow-up work. If
   it spans allocator rebuild, recovery-time scanning, page reuse, compaction,
   and persistent format cleanup, it should be designed as an RFC rather than a
   single task.
2. Physical removal of `gc_block_list` from metadata is intentionally deferred.
   A later compatibility or format-cleanup task should decide whether to remove
   the field, keep it reserved, or replace it with root-reachability GC metadata.
3. Catalog `MultiTableFile` uses the same CoW helper but has different runtime
   visibility rules. This task should not change catalog checkpoint behavior
   unless implementation finds a small shared helper is unavoidable.
