---
id: 000217
title: Redesign checkpoint frozen page cutoff validation
status: implemented
created: 2026-07-10
github_issue: 832
---

# Task: Redesign checkpoint frozen page cutoff validation

## Summary

Redesign user-table frozen-page checkpoint validation so checkpoint readiness is
proved from actual row undo status timestamps instead of page-level
`max_ins_sts`. The current shortcut can classify a frozen page as stable after
the writer transaction becomes inactive even when the writer's committed CTS is
at or after the selected checkpoint cutoff. That mismatch lets checkpoint reach
`vector_view_in_transition`, where future or uncommitted insert/update versions
panic because they cannot be represented in the LWC image.

The implementation should introduce a frozen-page batch and checkpoint API,
cache per-page validation results across retries, and return a normal delayed
stale-cutoff outcome when frozen pages are not yet safe for the selected
cutoff.

## Context

Issue Labels:
- type:task
- priority:medium
- codex

Source Backlogs:
- docs/backlogs/000149-redesign-checkpoint-frozen-page-cutoff-validation.md

Doradb checkpoint moves hot RowStore pages through `ACTIVE -> FROZEN ->
TRANSITION`, builds LWC blocks from committed visible rows, persists matching
block-index and secondary-index `DiskTree` state, and publishes one table-file
root. The checkpoint/recovery contract requires checkpointed persistent state to
contain only committed row images. Future deletes are representable as
in-memory `ColumnDeletionBuffer` markers, but future insert/update row images
are not representable by the current checkpoint format.

The current user-table path freezes pages independently from checkpoint. Public
`Session::freeze_table` returns only an approximate row count, while
`checkpoint_table` later rediscovers frozen pages and waits for
`RowVersionMap::max_ins_sts() < published_gc_horizon()`. That predicate proves
the writer STS is older than the purge-published active horizon; it does not
prove the writer's commit CTS is older than the checkpoint cutoff. A transaction
can start before an active reader, update a frozen page, commit after that
reader's STS, and become inactive. The current wait can then pass even though
the update CTS is still future relative to the checkpoint cutoff.

The task should refactor the freeze/checkpoint API around explicit frozen page
batches. Compatibility with the current no-batch `freeze_table` and
`checkpoint_table` signatures is not required. Avoid making this task a broader
event-driven checkpoint scheduler.

## Goals

- Replace `max_ins_sts`-based frozen-page stability with undo-chain validation
  that reads actual `UndoStatus` timestamps.
- Remove `RowVersionMap::max_ins_sts` and insert/update tracking in row-version
  write guards unless a later review finds a non-checkpoint consumer that still
  needs it.
- Add a `FrozenPageBatch` type containing table id, frozen page metadata,
  approximate rows, `frozen_ts`, and cached per-page validation state.
- Add a checkpoint API that accepts a frozen-page batch and updates its
  validation state across retry attempts.
- Return a normal delayed stale-cutoff outcome when validation finds
  uncommitted or future insert/update row image state for the current cutoff.
- Keep existing checkpoint publication atomic across LWC data, block-index
  state, cold-delete metadata, and secondary-index `DiskTree` sidecar updates.
- Replace the defensive `vector_view_in_transition` panic with checked
  validation/build behavior so stale cutoffs do not unwind storage tests or
  runtime maintenance.

## Non-Goals

- Do not implement a full event-driven checkpoint scheduler or transaction
  status notification index in this task.
- Do not change checkpoint recovery format, LWC block format, or table-file root
  publication semantics.
- Do not make delete CTS participate in the page data-stability cutoff proof.
  Deletes remain an overlay state because deletes are allowed on frozen pages.
- Do not change cold-row deletion checkpoint selection outside what is required
  to consume transition-captured delete markers safely.

## Plan

1. Introduce frozen page batch types.
   - Add a `FrozenPageBatch` type for user-table checkpoint
     scheduling. It should carry `table_id`, `frozen_ts`, approximate rows,
     page ids, row-id ranges, and validation cache.
   - Add a per-page validation state such as `Unchecked`, `Blocked`, and
     `Stable { required_cutoff_ts }`. The exact shape can be adjusted during
     implementation, but already-stable pages must not be rescanned on later
     attempts.

2. Refactor freeze APIs around page batches.
   - Add a table/session freeze method that freezes a contiguous hot-page prefix
     up to the requested row budget and returns `FrozenPageBatch`.
   - Allocate `frozen_ts` after all selected pages have been marked frozen.
     This timestamp is a fence: transactions with STS at or after `frozen_ts`
     should observe the frozen state before attempting insert/update on those
     pages.
   - Replace or refactor the current no-batch public entry points as needed.
     Compatibility with the old signatures is not a requirement.

3. Replace frozen-page stability waiting with cached undo-chain validation.
   - Remove `Table::wait_for_frozen_pages_stable` or change it into a validator
     that updates the batch cache and returns a structured readiness result.
   - Remove the correctness dependency on `max_ins_sts`. Use
     `published_gc_horizon` as the checkpoint `cutoff_ts`; actual row-image
     safety must still come from undo status CTS.
   - Open the checkpoint transaction before final validation when needed, but
     do not derive the cutoff from checkpoint STS. The checkpoint cutoff is the
     purge-published GC horizon.

4. Validate each unchecked row page by walking row undo chains.
   - For each row, walk from the current undo head toward older versions until
     the row image source that LWC would encode is proven safe.
   - `Lock`: not an image-producing operation. If uncommitted and its writer
     STS is before `frozen_ts`, the page is blocked because that writer may not
     have observed the frozen flag. Continue walking to older entries.
   - `Delete`: not an image-producing operation for cutoff proof. If
     uncommitted and its writer STS is before `frozen_ts`, the page is blocked.
     Otherwise remember any transition delete-buffer marker that will be needed
     and continue walking to older entries. Committed delete CTS must not become
     the page's required cutoff because deletes are allowed after freeze.
   - `Insert` / `Update`: image-producing operations. If committed, the page's
     required cutoff must be at least that CTS plus the exclusive-cutoff rule.
     The page is stable for an attempt only when every required image-producing
     CTS is below `cutoff_ts`. If uncommitted, the page is blocked or reported
     stale for this attempt because LWC cannot encode an uncommitted row image.
   - Cache the maximum committed image-producing CTS required by each page once
     the page has no blockers. On retry, compare cached requirement to the new
     cutoff instead of rescanning the page.

5. Model stale cutoff as a normal checkpoint outcome.
   - Extend checkpoint delay diagnostics with a structured stale-cutoff reason,
     or add a reason kind while preserving existing active-root delay fields.
   - Include enough diagnostic data for tests and schedulers: table id, page id
     or stable-prefix count, selected cutoff, required cutoff when known, and
     whether validation is blocked by an unresolved status.
   - Ensure existing root-readiness delay remains distinguishable from frozen
     page cutoff delay.

6. Transition only after the whole batch is stable for the selected cutoff.
   - Load validated frozen pages, set them to `TRANSITION`, and then capture
     representable overlay state into `ColumnDeletionBuffer`.
   - Transition capture must trace leading `Lock` and `Delete` entries until
     the first image-producing operation, matching the validator's chain walk.
     Do not rely only on the undo head.
   - Move only overlay state to the deletion buffer:
     - committed `Delete` with `CTS >= cutoff_ts` becomes
       `DeleteMarker::Committed(cts)`;
     - uncommitted post-fence `Lock` or `Delete` becomes `DeleteMarker::Ref`;
     - committed `Delete` with `CTS < cutoff_ts` needs no marker for that
       delete, but validation must already have proven the previous row image
       is safe.
   - Insert/update undo below the cutoff is folded into the LWC row image and
     should not be copied to the deletion buffer.

7. Make LWC build consume a validated transition view.
   - Change `RowPage::vector_view_in_transition` to return `Result` or a
     prevalidated view type instead of panicking on future insert/update.
   - Ensure `build_lwc_blocks` and secondary-index sidecar collection use the
     same validated cutoff semantics.
   - If a stale future insert/update is detected despite prior validation, fail
     closed with the stale-cutoff outcome before any publication-sensitive state
     is committed.

8. Preserve existing checkpoint publication flow.
   - Continue publishing LWC blocks, block-index state, cold-delete checkpoint
     state, secondary-index sidecar updates, allocation-map rebuilds, and the
     final table-file root through the existing single publication boundary.
   - The public or internal checkpoint entry point should consume a
     `FrozenPageBatch`. Add no-batch helpers only if they simplify internal
     tests or scheduler call sites.

## Implementation Notes

- Implemented public retryable `FrozenPageBatch` ownership across
  `Session::freeze_table` and `Session::checkpoint_frozen_pages`. The batch
  records the post-freeze fence, selected page metadata, approximate rows, and
  cached per-page validation state. The compatibility `checkpoint_table` entry
  point remains and rebuilds a batch from the current frozen prefix.
- Replaced page-level `max_ins_sts` readiness with undo-chain validation against
  the purge-published exclusive cutoff. Stable pages cache their maximum
  required image cutoff across retries; unresolved or future image state returns
  structured `CheckpointDelayReason::FrozenPageCutoff` diagnostics before any
  selected page enters `TRANSITION`.
- Removed the obsolete row-version modification/max-writer timestamp bookkeeping
  and simplified row write and rollback plumbing. Transition capture now traces
  lock/delete chains, while LWC and secondary-index sidecar construction consume
  the same cutoff-visible row view. Table-root, LWC, deletion metadata, and
  secondary `DiskTree` publication remain atomic.
- Changed `RowPage::vector_view_in_transition` from a defensive panic to a
  checked error. An unexpected stale image after the irreversible transition
  boundary fails closed through the existing checkpoint publication guard and
  poisons storage instead of leaving foreground route waiters blocked.
- Added regression coverage for unresolved pre-fence inserts, committed future
  updates, cached retry after cutoff advancement, future updates hidden behind
  deletes, and checked transition-view failures. Resolve validation passed
  `tools/style_audit.rs --diff-base origin/main` for 16 Rust files,
  `cargo nextest run --workspace` with 1,299 tests, and
  `cargo nextest run -p doradb-storage --no-default-features --features libaio`
  with 1,223 tests; all tests passed with no skips. PR #833 CI and automated
  reviews passed, and both inline review threads were resolved.
- The implementation deliberately retains compatibility batch reconstruction.
  Its later fence can conservatively delay a valid post-freeze lock/delete
  overlay. Review accepted this limitation for follow-up in
  `docs/backlogs/000152-unify-table-freeze-checkpoint-workflow-state.md` rather
  than expanding this correctness task. Repeated undo analysis and batch-wide
  page-state lock duration are separately deferred to
  `docs/backlogs/000151-optimize-frozen-page-checkpoint-transition-planning.md`.

## Impacts

- `doradb-storage/src/session.rs`
  - refactor session freeze/checkpoint methods around `FrozenPageBatch`.
- `doradb-storage/src/lib.rs`
  - re-export any new public checkpoint batch/outcome types that belong to the
    crate API.
- `doradb-storage/src/table/persistence.rs`
  - refactor checkpoint setup, cutoff selection, batch validation, delayed
    outcome handling, and transition/LWC build sequencing.
- `doradb-storage/src/table/mod.rs`
  - define frozen page/batch internals if they remain table-local;
  - replace `wait_for_frozen_pages_stable`;
  - update transition delete-buffer capture.
- `doradb-storage/src/row/vector_scan.rs`
  - replace transition panic with checked result or validated-view contract.
- `doradb-storage/src/trx/ver_map.rs`
  - remove `max_ins_sts` state and accessors if no other consumer remains.
- `doradb-storage/src/table/hot.rs` and row write paths
  - remove insert/update max tracking calls tied only to checkpoint readiness.
- `doradb-storage/src/table/gc.rs`
  - preserve secondary MemIndex cleanup assumptions that rely on checkpointed
    column roots and deletion-buffer proof.
- `docs/checkpoint-and-recovery.md` and/or `docs/transaction-system.md`
  - update the frozen-page stability description if implementation changes the
    documented checkpoint readiness model.

## Test Cases

- Stale cutoff update:
  - create a reader that pins the GC horizon;
  - commit an insert/update on a frozen page with CTS at or after the pinned
    cutoff;
  - checkpoint returns delayed stale-cutoff and does not panic or transition the
    page.
- Retry after cutoff advances:
  - reuse the same batch after the reader exits;
  - checkpoint publishes successfully without rescanning pages whose cached
    validation requirement is already satisfied.
- Pre-fence unresolved writer:
  - start a transaction before `frozen_ts`, install a lock/delete or
    insert/update on a soon-frozen page, and keep it unresolved;
  - validation reports blocked/delayed instead of publishing.
- Post-fence lock/delete overlay:
  - install a lock/delete after `frozen_ts`;
  - validation traces behind it to prove the older image-producing operation;
  - transition captures the marker into `ColumnDeletionBuffer`.
- Committed delete after cutoff:
  - commit a delete on a frozen page with CTS at or after cutoff;
  - validation ignores delete CTS for image cutoff proof but traces to the
    previous image-producing version;
  - transition captures a committed delete marker and LWC keeps the row image
    available for older snapshots.
- Committed delete before cutoff:
  - validation traces behind the delete;
  - no delete-buffer marker is needed for that delete;
  - LWC view excludes the row at the checkpoint cutoff.
- Future insert/update hidden behind lock/delete:
  - construct or exercise a chain where a lock/delete sits above a future
    insert/update;
  - validation detects the future image-producing operation and delays.
- `vector_view_in_transition` no longer panics on uncommitted/future
  insert/update; callers receive a checked error/outcome.
- Existing checkpoint recovery, cold delete checkpoint, secondary-index sidecar,
  and secondary MemIndex cleanup tests continue to pass.
- Run `cargo nextest run --workspace`.
- If storage backend-neutral I/O paths are touched materially, also run
  `cargo nextest run -p doradb-storage --no-default-features --features libaio`.

## Open Questions

- `docs/backlogs/000151-optimize-frozen-page-checkpoint-transition-planning.md`
  tracks fused transition analysis, shorter page-state lock duration, and the
  measured decision on commit-maintained page watermarks.
- `docs/backlogs/000152-unify-table-freeze-checkpoint-workflow-state.md` tracks a
  table-owned freeze/checkpoint state machine, canonical batch ownership,
  concurrent-call and drop coordination, and removal of the compatibility
  rebuilt-fence ambiguity.
