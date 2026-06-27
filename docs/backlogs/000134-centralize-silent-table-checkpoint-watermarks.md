# Backlog: Centralize silent table checkpoint watermarks

## Summary

Reduce redo-truncation write amplification for many static user tables by centralizing heartbeat-only checkpoint watermarks in catalog storage. Today a table checkpoint can publish a new table-file root even when no table data shape, persisted index root, delete payload, or allocation reachability changed, solely to advance replay floors for future redo truncation. The proposed optimization stores per-table replay watermark overrides in a catalog table or catalog.tables extension, batches silent-table updates through catalog redo/checkpoint, and computes effective table replay bounds from both the table root and the catalog overlay.

## Reference

Discovered during task 000196 / RFC 0022 redo-log truncation planning after implementing global floor calculation from live table roots and pending dropped-table floors.

Relevant context:
- docs/tasks/000196-global-truncation-floor-planning.md
- docs/rfcs/0022-catalog-backed-redo-log-truncation.md
- doradb-storage/src/table/persistence.rs: table checkpoint intentionally publishes heartbeat roots when only replay floor metadata changes.
- doradb-storage/src/trx/retention.rs and doradb-storage/src/catalog/mod.rs: truncation planning currently snapshots per-table floors from resident live table roots.
- doradb-storage/src/recovery/mod.rs: recovery currently seeds table replay bounds from the loaded table root.

User design direction: gather heartbeat checkpoints into a central catalog-backed place to reduce repeated A/B table-root writes for many static tables. The catalog watermark is expected to be logged and checkpointed, so recovery and truncation can treat it as durable evidence once checkpointed.

## Deferred From (Optional)

docs/tasks/000196-global-truncation-floor-planning.md; docs/rfcs/0022-catalog-backed-redo-log-truncation.md phase 3 / future phase 4 follow-up

## Deferral Context (Optional)

- Defer Reason: Task 000196 is focused on recovery-safe redo truncation floor planning and retained redo candidate/blocker calculation. Moving heartbeat-only table checkpoint floors into catalog storage changes checkpoint proof production, catalog schema/checkpointing, recovery bootstrap, and dropped-table floor capture, so it should be planned as a separate performance optimization task rather than folded into the current truncation planner implementation.
- Findings:
  The idea is feasible with no fundamental correctness blocker if the catalog watermark is a first-class checkpointed replay-bound overlay. The safe effective-bound rule is fieldwise max: effective_heap_floor = max(table_root.heap_redo_start_ts, catalog.heap_redo_start_ts), effective_delete_floor = max(table_root.deletion_cutoff_ts, catalog.deletion_cutoff_ts), and table_replay_floor = min(effective_heap_floor, effective_delete_floor). The table checkpoint process is the right place to decide whether a checkpoint is silent because it already proves whether frozen pages, LWC blocks, delete deltas, secondary roots, allocation reachability, or only metadata floors changed.
  
  Important constraints learned during review: truncation must only use checkpoint-durable catalog watermarks; recovery must seed table bounds from root plus catalog overlay before replay planning; catalog checkpoint must explicitly fold the maintenance record because ordinary DML-only catalog redo is currently not materialized as catalog state; and the overlay should remain separate from generic table-root snapshots because catalog floors may advance beyond root_ts. Dropped-table cleanup should capture the effective root+catalog floor before runtime removal.
- Direction Hint: Prefer introducing a separate catalog watermark table unless extending catalog.tables is clearly simpler and preserves index-DDL table-row replacement semantics. Add a narrow maintenance redo/DDL marker or equivalent scan rule so catalog checkpoint folds watermark updates. Keep volatile buffering optional: losing buffered state must only reduce truncation progress, never correctness. Future design should distinguish real table-root checkpoint publication from catalog-only heartbeat floor publication and keep existing table-root invariants intact.

## Scope Hint

Design and implement catalog-backed per-table replay watermark overrides for heartbeat-only table checkpoints. The future task should cover catalog schema/storage choice, table checkpoint branching for silent proofs, recovery/truncation effective-bound calculation, dropped-table floor capture, batching/collapsing repeated updates per table, and crash safety around catalog checkpoint durability. Physical redo truncation API work can consume the effective bounds but does not have to be part of this item unless planning chooses to combine them.

## Acceptance Hint

A heartbeat-only table checkpoint can advance heap/delete replay floors without publishing a user table-file root. The advanced floors become usable for recovery and redo truncation only after the catalog watermark is durable through catalog checkpoint. Effective replay bounds use fieldwise max(root floor, catalog floor), and lost/uncheckpointed buffered watermark state falls back safely to older root floors. Tests cover restart recovery, truncation planning, dropped-table capture, mixed active/static tables, repeated collapsed updates, and cases where actual table data/index/delete checkpoint work still publishes a table root.

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
