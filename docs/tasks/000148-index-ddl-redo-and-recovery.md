---
id: 000148
title: Index DDL Redo And Recovery
status: implemented
created: 2026-05-14
github_issue: 634
---

# Task: Index DDL Redo And Recovery

## Summary

Implement RFC 0018 Phase 3 by making index DDL redo and recovery obey the
root-publish durability rule.

`CREATE INDEX` and `DROP INDEX` redo must use table-local identity
`(table_id, index_no)`. During recovery, an index DDL redo record is only a
durable catalog metadata change when the active table-file root proves the
matching allocation or final index state. If a crash happens after index DDL
redo commit but before the matching table root is published, recovery skips the
provisional catalog DML and continues from the old table root.

This task does not expose `CREATE INDEX` or `DROP INDEX` APIs. It prepares the
redo, recovery, and catalog checkpoint rules that later RFC 0018 phases must
use.

## Context

RFC 0018 defines index DDL durability as a two-part fact: catalog redo records
the DDL intent and catalog row changes, while the user-table CoW root publish is
the durable metadata commit point. A `CreateIndex` or `DropIndex` redo record
without a matching table root is provisional and must be ignored after restart.

Phase 1 already made index metadata stable and sparse: user-table indexes are
addressed by stable table-local `index_no`, each table persists a monotonic
`next_index_no`, and table-file secondary-root vectors have one slot per
historical index number. Phase 2 already added runtime layout snapshots plus
table/catalog metadata-change gates so future index DDL can exclude checkpoint
while a root mutation is provisional.

The current code still has Phase 3 gaps:

- `DDLRedo::CreateIndex` and `DDLRedo::DropIndex` carry `IndexID` payloads
  rather than `(table_id, index_no)`.
- Recovery still falls through to `todo!()` for index DDL.
- Recovery reload currently rejects catalog/table-file metadata mismatch before
  redo replay, but RFC 0018 requires a temporary pending-reconciliation state
  when the table root is newer than checkpointed catalog metadata.
- Catalog checkpoint scans redo logs directly and currently understands table
  DDL, but not provisional index DDL.
- Published table roots are durable timestamp carriers. Recovery must seed the
  transaction timestamp watermark from table root `trx_id` even when a matching
  `DataCheckpoint` redo marker is absent or already truncated.

Issue Labels:
- type:task
- priority:high
- codex

Parent RFC:
- docs/rfcs/0018-create-drop-index.md

Related implemented tasks:
- docs/tasks/000146-stable-index-metadata.md
- docs/tasks/000147-runtime-layout-and-checkpoint-gate.md

## Goals

1. Replace `DDLRedo::CreateIndex(IndexID)` and
   `DDLRedo::DropIndex(IndexID)` with payloads containing
   `{ table_id: TableID, index_no: u16 }`.
2. Add recovery helpers that classify index DDL redo by inspecting the active
   table-file root and the DDL commit timestamp.
3. Treat the active table-file root as the durable source of user-table
   metadata during recovery bootstrap when checkpointed catalog metadata is
   older but index-DDL-reconcilable.
4. Replay catalog DML for root-proven index DDL and skip the entire catalog DML
   batch for unproved provisional index DDL.
5. Support create-then-drop-before-catalog-checkpoint replay by preserving the
   durable allocation history from root-proven `CreateIndex` redo before a later
   root-proven `DropIndex` removes the active catalog index rows.
6. Validate after redo replay that every loaded user table's catalog metadata
   matches its active table-file metadata.
7. Update catalog checkpoint scanning so unproved provisional index DDL catalog
   rows cannot become checkpointed catalog state.
8. Keep `DDLRedo::DataCheckpoint` recovery behavior as a no-op for state
   application, while ensuring published table roots remain sufficient proof of
   checkpoint success and timestamp advancement.
9. Add focused tests for redo serialization, root-proof classification,
   provisional skip, durable replay, catalog checkpoint scanning, and repeatable
   recovery after an interrupted recovery attempt.

## Non-Goals

1. Do not implement public `CREATE INDEX` or `DROP INDEX` session APIs.
2. Do not build a new persistent `DiskTree` root for a created index.
3. Do not remove or retire runtime index layouts for dropped indexes beyond the
   Phase 2 install/cleanup surface.
4. Do not rebuild missing indexes during normal recovery.
5. Do not add a new persisted DDL marker, generation, or epoch to table-file
   roots.
6. Do not make table metadata multi-versioned or add online DDL.
7. Do not change the table checkpoint publication protocol except for recovery
   validation around root timestamp carriers.
8. Do not implement physical reclamation of orphaned unpublished index pages or
   dropped index `DiskTree` pages.
9. Do not support compatibility with older redo payload encodings. The storage
   crate is in active breaking development.

## Unsafe Considerations (If Applicable)

No new unsafe code is planned. The implementation should stay in safe redo
serde, catalog reload/checkpoint, table-file metadata inspection, and recovery
control-flow code.

If implementation unexpectedly touches unsafe code, document each unsafe block
with a concrete `// SAFETY:` invariant and run:

```bash
cargo clippy -p doradb-storage --all-targets -- -D warnings
```

## Plan

1. Change the index DDL redo payload shape.
   - Update `doradb-storage/src/trx/redo.rs`:

```rust
CreateIndex { table_id: TableID, index_no: u16 }
DropIndex { table_id: TableID, index_no: u16 }
```

   - Update `DDLRedo::ser_len`, `Ser`, and `Deser`.
   - Keep the existing redo op codes `131` and `132`.
   - Remove the `IndexID` dependency from redo if it becomes unused.
   - Update redo unit tests to round-trip the new payloads and verify both
     table id and index number.

2. Define root-proof classification for index DDL.
   - Add a small recovery-local enum, for example:

```rust
enum IndexDdlRootProof {
    Provisional,
    DurableFinalCreate,
    DurableAllocationOnly,
    DurableFinalDrop,
}
```

   - `CreateIndex` is final-create proven when:
     - the table exists in recovery runtime;
     - `active_root.trx_id >= ddl_cts`;
     - `active_root.metadata.next_index_no() > index_no`;
     - `active_root.metadata.index_spec(index_no)` is active;
     - `active_root.secondary_index_roots` contains the slot.
   - The secondary root for an active created index may be `SUPER_BLOCK_ID`.
     That is a valid empty cold `DiskTree` root and must not make proof fail.
   - `CreateIndex` is durable-allocation-only proven when:
     - the table exists in recovery runtime;
     - `active_root.trx_id >= ddl_cts`;
     - `active_root.metadata.next_index_no() > index_no`;
     - the final active root no longer has an active spec for `index_no`.
   - `DropIndex` is final-drop proven when:
     - the table exists in recovery runtime;
     - `active_root.trx_id >= ddl_cts`;
     - `active_root.metadata.next_index_no() > index_no`;
     - `active_root.metadata.index_spec(index_no)` is inactive;
     - `active_root.secondary_index_roots[index_no] == SUPER_BLOCK_ID`.
   - Any missing table, root timestamp older than the DDL CTS, missing slot, or
     incompatible metadata shape is provisional or a data-integrity error
     according to the existing user-table replay classification rules.

3. Add recovery bootstrap support for pending index-DDL reconciliation.
   - Refactor `Catalog::reload_create_table` so strict runtime reload can keep
     rejecting catalog/table-file metadata mismatch, while recovery bootstrap
     can allow mismatch that is limited to RFC 0018 index DDL.
   - A reconcilable mismatch must satisfy:
     - columns are identical;
     - `file.next_index_no >= catalog.next_index_no`;
     - any index active in both catalog and file has identical columns and
       attributes;
     - differences are only active/inactive index slots or the monotonic
       `next_index_no`.
   - Use table-file metadata to construct the recovered user-table runtime so
     foreground access after successful recovery observes the published root
     metadata.
   - Record pending reconciliation for tables loaded from table-file metadata
     that does not yet match checkpointed catalog rows.

4. Replay index DDL in `LogRecovery::replay_ddl`.
   - For root-proven `CreateIndex`, replay its catalog DML with
     `replay_catalog_modifications`.
   - For durable-allocation-only `CreateIndex`, also replay its catalog DML.
     This preserves the `catalog.tables.next_index_no` refresh and creates
     transient active catalog index rows that a later root-proven `DropIndex`
     can remove before final validation.
   - For root-proven `DropIndex`, replay its catalog DML. The normal drop DML
     deletes rows from `catalog.indexes` and `catalog.index_columns`.
   - For provisional `CreateIndex` or `DropIndex`, skip the whole transaction's
     catalog DML and continue replay. Do not publish roots, rebuild indexes, or
     wait for checkpoint.
   - If a root-proven DDL's catalog DML is malformed or final validation cannot
     reconcile catalog rows with the active table root, fail recovery with a
     data-integrity error rather than guessing.

5. Make the catalog DML contract explicit for later CREATE/DROP INDEX tasks.
   - Future `CREATE INDEX` must commit catalog DML equivalent to:

```text
catalog.tables:
  DeleteByUniqueKey(table_id)
  Insert(table_id, next_index_no + 1)

catalog.indexes:
  Insert(table_id, index_no, index_attributes)

catalog.index_columns:
  Insert(table_id, index_no, index_column_no, column_no, index_order)...
```

   - The matching table root must carry the same advanced `next_index_no`, the
     active spec at `index_no`, and the matching secondary-root slot vector.
   - Future `DROP INDEX` must delete the active index rows from
     `catalog.indexes` and `catalog.index_columns`. It may redundantly refresh
     `catalog.tables`, but Phase 3 recovery must not depend on that refresh
     because durable allocation state comes from the earlier root-proven create
     DML.

6. Validate final catalog/table-file metadata consistency.
   - After log replay and before `recover_indexes_and_refresh_pages`, rebuild
     each loaded table's metadata from current in-memory catalog rows and
     compare it with the table-file active root metadata.
   - Require exact equality after redo replay for all loaded user tables, not
     only tables that started with pending reconciliation.
   - Include table id, root transaction id, catalog replay boundary, and a
     concise mismatch reason in any data-integrity error.

7. Update catalog checkpoint scanning.
   - Teach `Catalog::scan_checkpoint_batch_with_config` to recognize
     `CreateIndex` and `DropIndex`.
   - For root-proven index DDL, include that transaction's catalog DML in the
     checkpoint batch and count it as catalog DDL.
   - For unproved provisional index DDL, skip that transaction's catalog DML
     while allowing the scan to continue. Advancing `catalog_replay_start_ts`
     past skipped provisional index DDL is correct because table-root absence
     makes the catalog absence durable.
   - If root proof cannot be evaluated due corruption or an impossible
     checkpointed-catalog/table-root state, stop with a data-integrity error.
   - Preserve existing drop-table scan behavior and stop reasons.

8. Preserve `DataCheckpoint` recovery semantics.
   - Keep `DDLRedo::DataCheckpoint` as a recovery no-op for state application:
     recovery uses the active table-file root's `pivot_row_id`,
     `heap_redo_start_ts`, `deletion_cutoff_ts`, secondary roots, and metadata.
   - When tracking a loaded table, include `active_root.trx_id` in
     `max_recovered_cts`. A published checkpoint root whose redo marker is
     absent or truncated must still advance the timestamp generator watermark.
   - Existing `DataCheckpoint` redo replay may still validate table existence
     and replay boundaries when the marker is present, but it must not be
     required for checkpoint success.

9. Keep recovery repeatable after a crash during recovery.
   - Do not publish catalog checkpoints, table roots, or physical cleanup as
     part of index DDL replay.
   - Recovery may mutate only in-memory catalog/runtime state before normal
     foreground admission.
   - If the engine crashes during recovery, the next recovery must see the same
     durable table roots and redo logs and reach the same classification.

10. Update docs where code behavior changes are externally visible.
   - If implementation refines root-proof terminology or catalog checkpoint
     scan behavior beyond this task text, update `docs/rfcs/0018-create-drop-index.md`
     during implementation or resolve synchronization.
   - Keep `Implementation Notes` blank until `task resolve`.

## Implementation Notes

Resolved on 2026-05-16.

- Replaced index DDL redo payloads with `{ table_id, index_no }` while
  preserving redo op codes, and updated redo serialization/deserialization
  tests.
- Added table-root proof classification for index DDL and used it in both
  recovery replay and catalog checkpoint scanning. Root-proven
  `CreateIndex`/`DropIndex` catalog DML is replayed or checkpointed; unproved
  provisional index DDL advances past the transaction without materializing its
  catalog rows.
- Added recovery reload modes so normal runtime reload stays strict while
  recovery bootstrap can temporarily load from table-file metadata when the
  mismatch is limited to index-DDL reconciliation. Recovery now validates exact
  catalog/table-file metadata agreement before foreground admission.
- Preserved durable allocation history for create-then-drop replay, kept
  `DataCheckpoint` as a recovery state-application no-op, and advanced the
  recovered timestamp watermark from published table-root `trx_id` values.
- Added integrity hardening found during review: orphan
  `IndexColumnObject` rows now fail metadata rebuild, reconciliation compares
  full column metadata, impossible catalog/file `next_index_no` ordering returns
  an explicit data-integrity error, and table metadata rejects inconsistent
  nullable column metadata.
- Stabilized secondary mem-index cleanup tests that require the checkpoint root
  to cross the GC horizon before cold-row delete-overlay proof is valid.

Validation and checklist outcome:

- `tools/task.rs resolve-task-next-id --task docs/tasks/000148-index-ddl-redo-and-recovery.md`
  confirmed local `docs/tasks/next-id` was already `000149`.
- `cargo fmt -p doradb-storage -- --check` passed.
- `git diff --check` passed.
- `cargo clippy -p doradb-storage --all-targets -- -D warnings` passed.
- `cargo nextest run -p doradb-storage` passed: 770 tests.
- `tools/coverage_focus.rs --path doradb-storage/src/catalog --path doradb-storage/src/trx --path doradb-storage/src/file --path doradb-storage/src/table --verbose`
  passed with 91.86% deduplicated focused coverage; requested targets were
  catalog 92.39%, trx 94.03%, file 92.25%, and table 89.98%.
- `cargo nextest run -p doradb-storage --no-default-features --features libaio`
  passed: 768 tests.
- No new unsafe code was introduced; the unsafe baseline date was refreshed
  during implementation review.
- No unresolved checklist issues remain and no follow-up backlog item was
  needed for this task.

## Impacts

- `doradb-storage/src/trx/redo.rs`
  - `DDLRedo`, serde/deser, redo tests, and any construction call sites.
- `doradb-storage/src/trx/recover.rs`
  - `LogRecovery::bootstrap_checkpointed_user_tables`,
    `track_loaded_table`, `replay_ddl`, catalog replay filtering, pending
    reconciliation tracking, and final validation.
- `doradb-storage/src/catalog/mod.rs`
  - user-table reload policy, metadata construction from catalog rows, strict
    versus recovery-tolerant metadata comparison.
- `doradb-storage/src/catalog/checkpoint.rs`
  - catalog checkpoint scan classification for index DDL and skipped
    provisional catalog ops.
- `doradb-storage/src/catalog/storage/indexes.rs`,
  `doradb-storage/src/catalog/storage/tables.rs`, and
  `doradb-storage/src/catalog/storage/checkpoint.rs`
  - helper accessors or idempotency adjustments needed by recovery and
    checkpoint tests.
- `doradb-storage/src/file/table_file.rs` and
  `doradb-storage/src/file/meta_block.rs`
  - root metadata inspection helpers only if existing accessors are not enough.
- `docs/rfcs/0018-create-drop-index.md`
  - resolve-time synchronization for the completed Phase 3 behavior.

## Test Cases

1. Redo serialization round-trips `CreateIndex { table_id, index_no }` and
   `DropIndex { table_id, index_no }`.
2. Recovery skips `CreateIndex` catalog DML when redo exists but the active
   table root does not prove the index allocation.
3. Recovery skips `DropIndex` catalog DML when redo exists but the active table
   root still has the index active.
4. Recovery replays root-proven `CreateIndex` catalog DML after a crash between
   table-root publish and catalog checkpoint.
5. Recovery replays root-proven `DropIndex` catalog DML after a crash between
   table-root publish and catalog checkpoint.
6. Recovery handles create-then-drop before catalog checkpoint by replaying the
   create allocation DML and then the drop DML, ending with catalog metadata
   equal to the final table root.
7. Recovery fails with a data-integrity error when catalog metadata and
   table-file metadata still mismatch after redo replay.
8. Catalog checkpoint scanning includes catalog DML for root-proven index DDL.
9. Catalog checkpoint scanning skips catalog DML for unproved provisional index
   DDL and can continue to later unrelated catalog work.
10. A published table checkpoint root advances recovered timestamp allocation
    even if the matching `DataCheckpoint` redo marker is absent from the replay
    range.
11. Recovery is repeatable after an injected failure or crash-equivalent abort
    during in-memory replay: a second engine start can recover from the same
    durable inputs.
12. Existing table DDL, table checkpoint, catalog checkpoint, hot-row redo,
    cold-delete redo, and secondary-index recovery tests continue to pass.

Routine validation:

```bash
cargo fmt
cargo clippy -p doradb-storage --all-targets -- -D warnings
cargo nextest run -p doradb-storage
```

Because this task touches file/checkpoint/recovery behavior, also run the
alternate backend validation when the local environment has `libaio` packages:

```bash
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

## Open Questions

- Phase 5 may decide whether `DROP INDEX` redundantly refreshes
  `catalog.tables` with the unchanged `next_index_no` as a defensive catalog
  DML shape. Phase 3 recovery must not require that refresh because
  root-proven `CREATE INDEX` replay is the required durable allocation source.
- Future work may introduce an explicit persisted DDL epoch or root operation
  marker if richer online DDL or metadata versioning makes table-root state
  proof ambiguous. This task intentionally avoids that extra persisted state.
