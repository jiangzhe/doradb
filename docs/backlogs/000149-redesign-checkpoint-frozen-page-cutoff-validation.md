# Backlog: Redesign checkpoint frozen-page cutoff validation

## Summary

Redesign user-table freeze/checkpoint cutoff validation. The current checkpoint path treats frozen pages as stable using page-level `max_ins_sts` and the purge-published GC horizon, but LWC build validates actual row undo status timestamps. That mismatch can allow a frozen page to pass readiness and then panic when `vector_view_in_transition` observes an insert/update whose committed CTS is at or after the selected checkpoint cutoff.

The future design should replace this unsafe shortcut with an undo-chain-aware validation model and make the freeze/checkpoint API easier to use for batch-oriented, incremental, or event-driven checkpoint scheduling.

## Reference

Investigation during the UserTableAccessor / secondary-index guarded accessor refactor found an intermittent panic in `table::gc::tests::test_secondary_mem_index_cleanup_propagates_cold_delete_overlay_proof_error`:

- Root panic: `doradb-storage/src/row/vector_scan.rs:435: Uncommitted/Future Insert/Update found in Checkpoint`.
- Shutdown panic `storage engine shutdown is busy: 2` was a cascade during unwind.
- `wait_for_frozen_pages_stable` in `doradb-storage/src/table/mod.rs` checks `row_ver.max_ins_sts() < trx_sys.published_gc_horizon()`.
- `max_ins_sts` records writer STS from `RowVersionWriteGuard::drop` in `doradb-storage/src/trx/ver_map.rs`.
- `vector_view_in_transition` in `doradb-storage/src/row/vector_scan.rs` checks undo status timestamps, which become committed CTS after `SharedTrxStatus::commit_prepared`.
- `capture_delete_markers_for_transition` handles future/uncommitted `Delete` by moving markers into `ColumnDeletionBuffer`, but `Insert`/`Update` at or after cutoff are not representable by the current checkpoint format and currently panic.

Key design observation: `max_ins_sts < min_active_sts` proves the transaction with that writer STS is no longer active; it does not prove the writer's committed CTS is below the checkpoint cutoff. Future delete is representable via deletion buffer; future insert/update is a stale-cutoff condition and should stop or retry checkpoint, not panic.

## Deferred From (Optional)


## Deferral Context (Optional)


## Scope Hint

Design and implement a safe checkpoint cutoff protocol for frozen row pages. Include freeze/checkpoint API changes that return or consume frozen page batches and freeze/fence metadata, stale-cutoff reporting, and an incremental/event-driven validation path that avoids repeatedly scanning every undo chain when pages or relevant transaction states have not changed.

The work should cover the full checkpoint process: freeze, frozen-page collection, cutoff selection, transition publication, deletion-buffer capture, LWC build, secondary checkpoint sidecar population, checkpoint outcome reporting, recovery/replay implications, and maintenance API ergonomics.

## Acceptance Hint

Checkpoint no longer relies on `max_ins_sts` plus `published_gc_horizon` as the correctness predicate for frozen-page conversion. Future/uncommitted insert/update versions at the selected cutoff are reported as a normal stale-cutoff outcome or trigger a safe retry path; they never panic or produce an unsafe LWC block. Future/uncommitted deletes continue to be represented through the deletion buffer. Public or internal freeze/checkpoint APIs support batch-oriented use without forcing callers to understand unsafe internals, and tests cover stale-cutoff insert, update, delete, lock-chain, retry, and recovery scenarios.

## Notes (Optional)

Potential direction from the investigation:

- Add `CheckpointDelayReasonKind::StaleCutoff` or equivalent structured outcome.
- Change freeze to return a `FreezeTableBatch` containing table id, freeze timestamp/fence, approximate rows, and frozen page metadata; add a checkpoint API that can consume such a batch while still validating it against current table state.
- Validate row version chains from head to tail against a selected cutoff. Insert/update committed before cutoff are safe. Insert/update uncommitted or committed at/after cutoff mean stale cutoff. Lock/delete uncommitted, and delete committed at/after cutoff, should continue through the chain to find the visible committed version while preserving deletion-buffer behavior.
- Avoid making validation a full O(all frozen rows) scan on every attempt where possible. Consider per-page validation state, transaction-status notifications, version-map modification counters, or event-driven recheck scheduling.
- `vector_view_in_transition` should return an error or validated-result type instead of panicking defensively.

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
