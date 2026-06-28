---
id: 000198
title: Retain Dropped Table Floors In Catalog Map
status: proposal
created: 2026-06-28
github_issue: 780
---

# Task: Retain Dropped Table Floors In Catalog Map

## Summary

Close the redo truncation planning race where foreground `DROP TABLE` removes a
table runtime from `Catalog.user_tables` before the dropped-table queue records
the retained replay floor. Make the catalog user-table map the authoritative
in-memory source for live and dropped table redo floors until catalog
checkpoint proves the dropped table absence durable.

Foreground drop should mark the table as dropped and retain it in the catalog
map. Purge may destroy the heavy runtime after the active-snapshot horizon, but
the map must keep a lightweight dropped-table floor until
`catalog_replay_start_ts > drop_cts`. Public enumeration and foreground access
must filter or reject dropped entries.

## Context

Issue Labels:
- type:task
- priority:medium
- codex

Source Backlogs:
- `docs/backlogs/000135-defer-dropped-table-runtime-removal-until-catalog-absence-is-durable.md`

Related design documents:
- `docs/checkpoint-and-recovery.md`
- `docs/table-file.md`
- `docs/architecture.md`
- `docs/transaction-system.md`
- `docs/rfcs/0017-drop-table-lifecycle-recovery.md`
- `docs/rfcs/0022-catalog-backed-redo-log-truncation.md`

Related completed tasks:
- `docs/tasks/000196-global-truncation-floor-planning.md`
- `docs/tasks/000197-session-redo-log-truncation-api.md`

Current foreground drop behavior copies the table replay floor, removes the
runtime from `Catalog.user_tables`, and then calls
`TransactionSystem::enqueue_dropped_table`. A concurrent
`TransactionSystem::plan_redo_truncation` can snapshot live floors after the
map removal and pending dropped-table floors before enqueue, so the dropped
table is temporarily absent from both sources.

The existing queue-based retained-floor model was sufficient for serial
planning, but it is the wrong ownership boundary for concurrent planning. The
catalog map should continue to represent every table id whose runtime or replay
floor can still constrain redo retention. Public catalog APIs should expose
only live user tables, while internal retention planning can include both live
and dropped retained entries from one map snapshot path.

This task is a follow-up to RFC 0022 Phase 3 and Phase 4, not a new RFC phase.
It preserves RFC 0017's recovery rule: checkpointed catalog absence plus
`catalog_replay_start_ts > drop_cts` is the durable negative knowledge that
allows old table redo and leftover table files to be ignored or removed.

## Goals

- Replace the plain `TableID -> Arc<Table>` catalog user-table map value with an
  explicit in-memory entry model that distinguishes live tables, dropped
  runtime-retained tables, and dropped replay-floor-only entries.
- Make foreground `DROP TABLE` transition the catalog map entry from live to
  dropped-retained after the durable drop commit instead of removing the entry.
- Keep dropped entries invisible to public table-id listing and unavailable to
  foreground user operations.
- Make redo truncation planning consult only the catalog map for live and
  dropped table replay floors.
- Let purge destroy heavy dropped-table runtime resources after
  `drop_cts < min_active_sts`, then leave a lightweight dropped-floor entry in
  the catalog map.
- Remove the lightweight dropped-floor entry and delete or retry table-file
  deletion only after catalog checkpoint publishes
  `catalog_replay_start_ts > drop_cts`.
- Keep recovery replay safe by destroying replayed dropped runtimes immediately
  while seeding the same lightweight dropped-floor catalog entry when catalog
  absence is not yet durable.
- Add focused tests proving truncation planning cannot miss a dropped table
  during the foreground drop handoff.

## Non-Goals

- Do not add a durable dropped-table tombstone or object lifecycle catalog
  table.
- Do not change table-id allocation, table-id non-reuse, or add generation-aware
  reclamation.
- Do not change redo file format, redo payload format, `catalog.mtb` metadata
  encoding, or table-file format.
- Do not add a public redo truncation dry-run API.
- Do not add background redo truncation, automatic table checkpointing, or a
  combined checkpoint-and-truncate command.
- Do not make public APIs expose dropped retained entries as live tables.
- Do not change the recovery rule that replayed drop can remove and destroy
  runtime immediately because startup has no concurrent foreground planner.

## Plan

1. Add a catalog user-table entry model.
   - Introduce an internal enum or equivalent private type in
     `doradb-storage/src/catalog/mod.rs` for the `user_tables` map value.
   - The entry model should represent:

```rust
Live {
    table: Arc<Table>,
}

DroppedRuntime {
    table: Arc<Table>,
    drop_cts: TrxID,
    replay_floor: TableRedoReplayFloor,
}

DroppedFloor {
    drop_cts: TrxID,
    replay_floor: TableRedoReplayFloor,
}
```

   - `DroppedRuntime` means the logical table is already dropped and hidden from
     public/foreground access, but the map still owns the heavy runtime so purge
     can destroy it after the active-snapshot horizon.
   - `DroppedFloor` means the heavy runtime has been destroyed or was destroyed
     during recovery replay, but redo truncation still needs the retained floor
     until catalog absence is durable.
   - Keep the entry type private unless tests require a narrow
     `#[cfg(test)]` helper.

2. Update live lookup and public enumeration.
   - `Catalog::get_table`, `Catalog::get_table_now`, and
     `Catalog::validate_user_table_live` must return a table only for live map
     entries.
   - `Catalog::list_user_table_ids_now` must include only live user-table
     entries.
   - `Session::list_table_ids` should keep its public shape but inherit the live
     filtering.
   - Existing cached stale `Arc<Table>` handles should continue to fail through
     `TableLifecycleState::Dropping` or `Dropped` checks.
   - `TableCache` and rollback/purge helpers that require live user tables
     should keep using live lookup unless a specific cleanup path needs a new
     dropped-entry helper.

3. Add catalog entry transition helpers.
   - Add a helper that validates the current entry is the expected live
     `Arc<Table>` and transitions it to `DroppedRuntime`.
   - The helper should store the drop CTS and the copied
     `TableRedoReplayFloor`.
   - A failed pointer or state validation after the drop lifecycle gate should
     be treated as the same fatal drop-after-gate poison boundary as today's
     failed runtime removal.
   - Add a helper to transition `DroppedRuntime` to `DroppedFloor` after
     runtime destroy succeeds.
   - Add a helper to remove `DroppedFloor` entries after catalog absence is
     durable.

4. Change foreground `DROP TABLE` handoff.
   - In `doradb-storage/src/catalog/table.rs`, keep the current lock order,
     lifecycle gate, catalog cascade delete, and durable commit shape.
   - After commit, snapshot the table replay floor and mark the table lifecycle
     dropped.
   - Replace `finish_drop_table_runtime_removal` with a retained-map transition
     that changes the catalog entry to `DroppedRuntime` instead of removing it.
   - Do not enqueue the dropped table for redo-floor retention.
   - Wake dropped-table purge only to attempt runtime destruction.
   - Keep waiter failure semantics: waiters should see `TableDropping` before
     durable drop completion and `TableNotFound` after the final dropped state
     is visible.

5. Move dropped-table runtime cleanup to catalog-map-owned entries.
   - In `doradb-storage/src/trx/purge.rs`, remove
     `DroppedTableQueue::runtime` as the owner of retained dropped runtimes, or
     reduce the queue to a wake/retry mechanism that does not own replay-floor
     state.
   - Add a purge path that asks the catalog map for dropped runtime entries
     where `drop_cts < min_active_sts`.
   - For each eligible entry, detach or otherwise take ownership of the
     `Arc<Table>` without losing the map's replay-floor entry.
   - If `Arc::try_unwrap` fails because stale handles remain, restore or retain
     the `DroppedRuntime` entry for a later purge wake.
   - If runtime destroy succeeds, transition the map entry to `DroppedFloor`.
   - Runtime destroy errors remain fatal storage errors, matching current
     purge behavior.

6. Move checkpoint-safe file deletion/final map removal to catalog-map-owned
   entries.
   - Keep catalog checkpoint as the important cleanup trigger:
     `Catalog::checkpoint_now` should continue waking dropped-table purge after
     publish.
   - File deletion should inspect dropped entries whose
     `catalog_replay_start_ts > drop_cts`.
   - It is acceptable to delete the file from either `DroppedRuntime` or
     `DroppedFloor` only when runtime ownership is already gone or can be safely
     destroyed first. Prefer keeping final entry removal ordered after runtime
     destroy so the map does not lose a pending runtime cleanup obligation.
   - Missing table files remain a successful no-op. Other unlink errors should
     retain the dropped-floor entry for retry.

7. Change redo truncation planning to read one authoritative source.
   - In `doradb-storage/src/trx/retention.rs`, replace the separate
     `catalog.snapshot_live_table_redo_floors()` plus
     `dropped_tables.snapshot_pending_redo_floors()` model with one catalog
     helper that snapshots both live and pending dropped floors from
     `Catalog.user_tables`.
   - Live entries should produce `LiveTableRedoReplayFloor`.
   - Dropped entries should produce `PendingDroppedTableRedoFloor` only while
     `catalog_replay_start_ts <= drop_cts`.
   - The planner should no longer consult `DroppedTableQueue` for redo floors.

8. Update recovery replay.
   - In `doradb-storage/src/recovery/mod.rs`, replayed `DropTable` can continue
     removing the loaded runtime and destroying it immediately, because recovery
     has no concurrent foreground truncation planner.
   - After destroy, seed a lightweight `DroppedFloor` catalog entry with the
     drop CTS and copied replay floor if catalog absence is not yet durable.
   - Ensure post-replay absent-file cleanup continues preserving files for
     dropped entries whose file deletion is still catalog-checkpoint gated.
   - Once recovery returns its initial dropped-table file-delete work to
     `TransactionSystem`, avoid duplicating ownership between that startup list
     and the catalog map. Prefer normalizing to the catalog-map-owned cleanup
     model before opening foreground admission.

9. Update comments and docs that currently state foreground drop removes the
   runtime from the catalog cache immediately.
   - Update comments in touched code paths.
   - If implementation changes user-visible maintenance semantics beyond this
     task document, update the relevant small section in
     `docs/checkpoint-and-recovery.md` or `docs/table-file.md`.

## Implementation Notes

## Impacts

- `doradb-storage/src/catalog/mod.rs`
  - `Catalog.user_tables`
  - `Catalog::get_table`
  - `Catalog::get_table_now`
  - `Catalog::list_user_table_ids_now`
  - `Catalog::snapshot_live_table_redo_floors`
  - `Catalog::remove_user_table`
  - `TableCache`
- `doradb-storage/src/catalog/table.rs`
  - `drop_table_for_session`
  - `finish_drop_table_runtime_removal`
  - drop-table tests around runtime visibility and GC cleanup
- `doradb-storage/src/trx/retention.rs`
  - `TransactionSystem::plan_redo_truncation`
  - `PendingDroppedTableRedoFloor`
  - blocker reporting for pending dropped table floors
- `doradb-storage/src/trx/purge.rs`
  - `DroppedTableGcItem`
  - `DroppedTableFileDeleteItem`
  - `DroppedTableQueue`
  - `TransactionSystem::enqueue_dropped_table`
  - `process_dropped_table_gc`
  - `process_dropped_table_file_deletes`
- `doradb-storage/src/recovery/mod.rs`
  - `Recovery::replay_drop_table_ddl`
  - startup dropped-table file-delete seeding
  - post-replay absent user-table file cleanup
- `doradb-storage/src/catalog/checkpoint.rs`
  - post-publish dropped-table purge wake
- `doradb-storage/src/session.rs`
  - `Session::list_table_ids`
  - redo truncation outcome tests that observe public table listing

## Test Cases

- Foreground `DROP TABLE` leaves no window where truncation planning can miss the
  dropped table floor.
  - Use explicit synchronization or a narrow test hook around the post-commit
    map transition; do not use sleeps.
  - Assert `plan_redo_truncation` sees either a live table floor or a pending
    dropped-table floor at every forced observation point.
- `Session::list_table_ids` excludes retained dropped entries immediately after
  successful drop, while live tables remain sorted.
- Foreground reads, writes, table checkpoints, and explicit table locks reject a
  retained dropped entry with the existing terminal `TableDropping` or
  `TableNotFound` behavior.
- Purge destroys a `DroppedRuntime` entry only after `drop_cts < min_active_sts`
  and leaves a `DroppedFloor` entry when catalog absence is not durable.
- Stale `Arc<Table>` handles keep a dropped runtime in `DroppedRuntime` and
  delay runtime destroy without losing the replay floor.
- Redo truncation planning reports a `PendingDroppedTableFloor` blocker from the
  catalog map while `catalog_replay_start_ts <= drop_cts`.
- Catalog checkpoint publish followed by dropped-table purge removes the final
  dropped-floor entry and deletes the table file when
  `catalog_replay_start_ts > drop_cts`.
- File deletion `NotFound` is treated as already deleted; other unlink failures
  retain the entry for retry.
- Recovery replay before catalog checkpoint destroys the runtime immediately,
  seeds a dropped-floor entry, keeps public table listing empty, and makes redo
  truncation planning see the dropped floor.
- Recovery after catalog checkpointed absence does not seed unnecessary dropped
  entries and can clean absent leftover files as before.
- Standard validation:

```bash
cargo nextest run -p doradb-storage
```

- If the implementation materially changes backend-neutral table-file cleanup
  helpers, also run:

```bash
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

## Open Questions

None. The chosen design intentionally keeps this as an in-memory ownership
change and leaves durable object lifecycle metadata, table-id reuse, and
background retention scheduling out of scope.
