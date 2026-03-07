---
id: 0006
title: Cache-First Unified Catalog Storage Refactor
github_issue: 382
status: proposal
tags: [storage-engine, catalog, checkpoint, recovery, refactor]
created: 2026-03-03
---

# RFC-0006: Cache-First Unified Catalog Storage Refactor

## Summary

This RFC proposes a cache-first catalog runtime with unified on-disk persistence in a single `catalog.mtb` file. It refactors catalog logical schemas away from global `column_id`/`index_id`, introduces explicit catalog-vs-user object-id boundaries, and adopts a replay-based catalog checkpoint: checkpointed catalog state is produced by replaying catalog redo logs up to a global recovery watermark `W`. Recovery then replays only logs with `cts > W`. Physical log truncation is intentionally deferred; only logical replay cutoff is required in this RFC. The expected outcomes are simpler catalog persistence topology, lower catalog file overhead, and a stronger correctness model for table lifecycle and future schema-evolution DDL.

## Context

Current catalog persistence and recovery have several structural mismatches with the target checkpoint model:

1. Catalog bootstrap currently creates per-catalog-table files (`0.tbl`..`3.tbl`) and relies on redo replay to rebuild logical state, rather than publishing one catalog-wide persistence boundary ([C2], [C7], [C8]).
2. Catalog schemas and object structs still encode global synthetic ids (`column_id`, `index_id`), while many table-scoped lookups are implemented as table scans with filters in catalog tables ([C3], [C4]).
3. Object-id allocation is single-sequence and does not enforce an explicit catalog/user id partition ([C1], [C5]).
4. Table-file naming is generic `<table_id>.tbl`; catalog has no dedicated file identity or persistence contract ([C6]).
5. The storage engine already has an explicit checkpoint and watermark model for table persistence; catalog should adopt similar clarity without changing cache-first runtime behavior ([D4], [D5], [C9], [C10]).
6. Fully decoupling catalog checkpoint from data checkpoint is unsafe for future schema-changing DDL (add/drop column, type change) unless catalog snapshot and replay cutoff are tied to a watermark that guarantees replay-order consistency across catalog and user logs ([D2], [D4], [C8], [C9], [U5]).

This decision is needed now because catalog model simplification is already underway and this is the right point to converge runtime semantics, file layout, and recovery boundary before more catalog features depend on current assumptions ([D1], [D2], [C1], [C2]).

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:epic`
`- priority:medium`
`- codex`

## Design Inputs

### Documents

- [D1] `docs/architecture.md` - cache-first + checkpoint architecture principles.
- [D2] `docs/transaction-system.md` - no-steal/no-force and redo recovery constraints.
- [D3] `docs/index-design.md` - index/checkpoint interplay and row-id mapping assumptions.
- [D4] `docs/checkpoint-and-recovery.md` - watermark-oriented recovery/truncation model.
- [D5] `docs/table-file.md` - CoW root publish pattern and metadata anchoring.
- [D6] `docs/process/issue-tracking.md` - planning and phased execution requirements.

### Code References

- [C1] `doradb-storage/src/catalog/mod.rs` - `Catalog` allocator semantics and reload path.
- [C2] `doradb-storage/src/catalog/storage/mod.rs` - catalog bootstrap and per-catalog-table file creation.
- [C3] `doradb-storage/src/catalog/storage/columns.rs` - `column_id`-centric schema and table-scan lookup path.
- [C4] `doradb-storage/src/catalog/storage/indexes.rs` - `index_id`-centric schema and index-column linking.
- [C5] `doradb-storage/src/session.rs` - DDL object allocation and catalog DML writes.
- [C6] `doradb-storage/src/file/table_fs.rs` - `<table_id>.tbl` naming path and open/create APIs.
- [C7] `doradb-storage/src/trx/sys_conf.rs` - catalog bootstrap + recovery wiring.
- [C8] `doradb-storage/src/trx/recover.rs` - catalog replay and `reload_create_table` behavior.
- [C9] `doradb-storage/src/table/persistence.rs` - table checkpoint orchestration pattern.
- [C10] `doradb-storage/src/trx/redo.rs` - DDL checkpoint marker model and serialization surface.

### Conversation References

- [U1] User selected Proposal A as the direction for this RFC.
- [U2] User requested code to be treated as the strongest reference when draft and code differ.
- [U3] User requested catalog checkpoint interval support plus an ad-hoc checkpoint API.
- [U4] User requested removing dependency on temporary partial planning document in RFC references.
- [U5] User proposed replay-based catalog checkpoint aligned to global recovery watermark with deferred physical log truncation.
- [U6] User requested explicit follow-up to remove transitional catalog `*.tbl` bootstrap scratch files and keep catalog runtime in-memory.
- [U7] User requested a dedicated `CatalogTable` type and accessor-first refactor direction (defer `Statement` API genericization).

### Source Backlogs (Optional)

- [B1] `docs/backlogs/000002-non-unique-index-for-catalog-tables.md` - related deferred catalog index work.
- [B2] `docs/backlogs/000043-catalog-pure-in-memory-runtime-no-legacy-tbl-files.md` - remove transitional catalog runtime dependency on legacy table files.

## Decision

Adopt a cache-first unified catalog storage design with one persistent catalog file and catalog-specific checkpoint/recovery watermark integration.

### 1. Scope and compatibility

1. This refactor is treated as new-cluster-only in RFC scope. Legacy on-disk catalog layouts are not migrated here ([C2], [C6], [D6], [U2]).
2. No backward-compatibility shim for previous per-catalog-table files is included in this RFC phases ([C2], [C7], [U2]).

### 2. Runtime semantics remain cache-first

1. `CatalogCache` remains the runtime source of truth for catalog reads/writes in foreground paths ([C1], [C5], [U2]).
2. Catalog on-disk state is durability/recovery state, not a direct read path target for normal metadata lookup ([D1], [D2], [C8], [U1]).

### 3. Unified catalog persistence file

1. Persist all catalog logical tables into one fixed catalog file: `catalog.mtb` ([U1], [U2], [C6]).
2. Add catalog-specific file open/create paths in table-file subsystem so catalog persistence does not rely on numeric table-id naming ([C6], [D5], [U2]).
3. Keep user table file naming deterministic and machine-friendly by switching to fixed-width 16-hex format for user table files ([C6], [U1]).

### 4. Explicit object-id boundary

1. Reserve a low id range for catalog/system ids and a high range for user-created objects, with catalog bootstrap ids (`0..3`) remaining in reserved range ([C2], [C5], [U2]).
2. Add a dedicated allocator entry point for user object ids (`next_user_obj_id`) and update DDL paths to use it ([C1], [C5], [U1]).
3. Persist allocator watermark (`next_user_obj_id`) in catalog overlay metadata and restore it at startup to guarantee monotonic non-reuse across restart ([C1], [C5], [C8], [U2]).

### 5. Catalog schema refactor to table-scoped keys

1. `columns`: remove `column_id`; primary key becomes `(table_id, column_no)` ([C3], [U1], [U2]).
2. `indexes`: remove `index_id`; primary key becomes `(table_id, index_no)` where `index_no` is stable ordinal in table metadata ([C4], [C5], [U1]).
3. `index_columns`: remove global id references and use `(table_id, index_no, index_column_no, column_no, index_order)` with PK `(table_id, index_no, index_column_no)` ([C4], [U1], [U2]).
4. Reload/recovery table reconstruction (`reload_create_table`) must derive metadata via table-scoped keys, not global id joins ([C1], [C8], [U2]).

### 6. Catalog checkpoint model and control

1. Introduce a catalog checkpoint worker that builds checkpointed catalog state from redo-log replay, not from ad-hoc scan of current catalog tables:
   1. compute global watermark `W`;
   2. scan redo logs in `(last_catalog_checkpoint_cts, W]`;
   3. replay only catalog mutations from that range into a checkpoint image;
   4. publish one new catalog overlay root atomically in `catalog.mtb` ([D4], [D5], [C8], [U5]).
2. Checkpoint trigger model includes:
   1. configurable periodic interval with a default value;
   2. dirty-event wakeup;
   3. ad-hoc explicit API `Catalog::checkpoint_now()` for tests/operations ([U3], [C1], [C9]).
3. Catalog checkpoint publish should be one persistence boundary for all catalog logical tables, analogous to single-table root swap semantics but multi-table in scope ([D5], [C9], [U1]).

### 7. Recovery integration

1. Persist `catalog_checkpoint_cts` in catalog overlay root and load it during startup ([D4], [C8], [U1]).
2. Recovery replay start is bounded by watermark `W`:
   1. load catalog snapshot at `catalog_checkpoint_cts`;
   2. replay only logs with `cts > W`;
   3. rely on redo ordering so catalog DDL and user-data logs after `W` remain consistent ([D2], [D4], [C8], [U5]).
3. Recovery must fail fast on post-cutoff inconsistencies (for example, user DML at `cts > W` referencing unknown table id) rather than silently skipping ([C8], [U2]).

### 8. Watermark semantics and truncation staging

1. In this RFC stage, `W` is derived conservatively from currently persisted user-table recovery watermarks; at minimum this includes `heap_redo_start_ts` from all user tables ([C9], [C7], [U2]).
2. As additional per-table persisted recovery watermarks become available (for example deletion/index), `W` is extended to the minimum across all required components without changing replay contract ([D4], [U5]).
3. Physical log truncation is deferred:
   1. this RFC requires logical replay cutoff using `W`;
   2. later truncation implementation can reuse the same watermark definition ([D4], [U5]).
4. Safety of replay-based cutoff depends on non-reused user table ids; this RFC therefore requires persisted monotonic `next_user_obj_id` in catalog checkpoint metadata ([C1], [C5], [U2]).

## Alternatives Considered

### Alternative A: Watermark-aligned replay-based catalog checkpoint (chosen)

- Summary: Unified `catalog.mtb`, schema/key refactor, id boundary, and catalog checkpoint derived from replaying catalog redo prefix up to global watermark `W`.
- Analysis: Ensures catalog snapshot and user-data replay cutoff are synchronized by the same watermark contract; robust for table lifecycle and future schema DDL ordering.
- Why Not Chosen: N/A (chosen).
- References: [D2], [D4], [D5], [C1], [C2], [C8], [U1], [U2], [U3], [U5]

### Alternative B: DDL fence/barrier between catalog and data checkpoints

- Summary: Keep independent checkpoints but add explicit DDL dependency fences that block catalog publish past unresolved table-watermark boundaries.
- Analysis: Correctness can be preserved, but coordination complexity and DDL stall risks are high.
- Why Not Chosen: More operationally complex than replay-prefix model and still requires intricate cross-component scheduling.
- References: [D4], [C9], [C8], [U5]

### Alternative C: Schema-versioned redo and schema history

- Summary: Add schema epoch/version to row redo and persist schema history for replay-by-version.
- Analysis: Most flexible long-term model; can fully decouple catalog and data checkpoints.
- Why Not Chosen: Largest redesign scope and not required for current RFC milestones.
- References: [D2], [C10], [C8], [U5]

## Unsafe Considerations (If Applicable)

No new `unsafe` scope is expected from this RFC. Existing `unsafe` usage in file/buffer internals is outside this decision surface.

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Implementation Phases

- **Phase 1: Catalog/File Foundation**
  - Scope: add catalog-dedicated file path handling, user file 16-hex naming, id-range constants, `next_user_obj_id`, and persisted allocator watermark in catalog overlay metadata.
  - Goals: establish deterministic naming and object-id boundary without changing query behavior.
  - Non-goals: no schema/key refactor yet.
  - Task Doc: `docs/tasks/000048-catalog-file-id-foundation.md`
  - Task Issue: `#383`
  - Phase Status: done
  - Implementation Summary: Implemented Phase 1 foundations: unified `catalog.mtb` persistence, explicit catalog/user object-id boundary with user-id allocator, and deterministic 16-hex user table file naming. [Task Resolve Sync: docs/tasks/000048-catalog-file-id-foundation.md @ 2026-03-05]

- **Phase 2: Catalog Logical Schema Refactor**
  - Scope: migrate `columns`/`indexes`/`index_columns` schemas and object structs to table-scoped composite keys; update DDL write paths and reload logic.
  - Goals: remove global `column_id`/`index_id` dependencies.
  - Non-goals: no unified overlay/checkpoint publish yet.
  - Task Doc: `docs/tasks/000049-catalog-composite-key-refactor.md`
  - Task Issue: `#385`
  - Phase Status: done
  - Implementation Summary: Implemented Phase 2 catalog schema refactor to table-scoped composite keys, updated DDL/reload paths, and validated catalog delete/list behavior with added regression tests. [Task Resolve Sync: docs/tasks/000049-catalog-composite-key-refactor.md @ 2026-03-06]
  - Related Backlogs:
    - `docs/backlogs/000045-composite-index-prefix-scan-support-for-catalog-table-id-lookups.md`

- **Phase 3: Accessor-First TableAccess Refactor**
  - Scope: extract standalone thin `TableAccessor` and migrate `TableAccess` implementation internals to accessor-driven shared logic.
  - Goals: make table-operation implementation reusable by both user-table runtime and future dedicated catalog-table runtime.
  - Non-goals: no `Statement` API signature changes in this phase; no catalog checkpoint/recovery cutoff changes.
  - Task Doc: `docs/tasks/000050-table-accessor-refactor-for-catalog-runtime.md`
  - Task Issue: `#387`
  - Phase Status: done
  - Implementation Summary: 1. Implemented generic type families with compatibility aliases and preserved runtime behavior: [Task Resolve Sync: docs/tasks/000050-table-accessor-refactor-for-catalog-runtime.md @ 2026-03-07]

- **Phase 4: Dedicated CatalogTable Runtime (No Legacy Catalog *.tbl Bootstrap)**
  - Scope: introduce `CatalogTable` runtime type, switch catalog storage wrappers to it, and remove transient legacy catalog `0.tbl..3.tbl` bootstrap file creation/unlink path.
  - Goals: catalog runtime no longer depends on `TableFile` bootstrap scratch files.
  - Non-goals: no fixed-pool specialization yet; no catalog checkpoint algorithm changes.
  - Task Doc: `docs/tasks/000051-catalog-table-runtime-no-legacy-bootstrap-files.md`
  - Task Issue: `#389`
  - Phase Status: done
  - Implementation Summary: 1. Introduced dedicated catalog runtime `CatalogTable` in [Task Resolve Sync: docs/tasks/000051-catalog-table-runtime-no-legacy-bootstrap-files.md @ 2026-03-07]
  - Related Backlogs:
    - `docs/backlogs/000043-catalog-pure-in-memory-runtime-no-legacy-tbl-files.md`

- **Phase 5: Catalog FixedBufferPool Specialization**
  - Scope: adapt reusable table-access/runtime components to support catalog data/index runtime on fixed pools; reuse existing engine `meta_pool`/`index_pool`.
  - Goals: remove catalog runtime dependence on evictable data-pool semantics.
  - Non-goals: no new engine config knobs for dedicated catalog pool sizing in this phase.
  - Task Doc: `docs/tasks/000052-catalog-fixed-buffer-pool-specialization.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

- **Phase 6: Unified Catalog Overlay and Checkpoint Worker**
  - Scope: implement catalog overlay root format over `catalog.mtb`, periodic + dirty-event scheduling, `Catalog::checkpoint_now()`, and replay-based catalog checkpoint build for redo prefix `(last_catalog_checkpoint_cts, W]`.
  - Goals: atomic multi-table catalog checkpoint publish with cache-first runtime preserved and watermark-aligned replay correctness.
  - Non-goals: no user-table checkpoint algorithm changes.
  - Task Doc: `docs/tasks/000053-catalog-overlay-checkpoint-worker.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

- **Phase 7: Recovery Cutoff and Consistency Checks**
  - Scope: persist/load `catalog_checkpoint_cts`, compute/load watermark `W`, replay only logs with `cts > W`, and add fail-fast checks for post-cutoff catalog/data inconsistencies.
  - Goals: deterministic startup replay boundary with strong consistency guarantees.
  - Non-goals: no physical log truncation implementation.
  - Task Doc: `docs/tasks/000054-catalog-recovery-cutoff-consistency.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

- **Phase 8: Validation and Documentation Sync**
  - Scope: tests for id ranges, file naming, schema behavior, checkpoint atomicity, replay cutoff correctness, and lifecycle ordering (`create/insert/drop`, DDL around cutoff); doc updates.
  - Goals: verify behavioral contract end-to-end.
  - Non-goals: no legacy migration tooling; no physical truncation.
  - Task Doc: `docs/tasks/000055-catalog-storage-refactor-validation.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

## Consequences

### Positive

- Clear separation of cache-first runtime metadata and persisted catalog checkpoint state.
- Single-file catalog persistence reduces file management overhead and clarifies atomic publish boundary.
- Composite table-scoped keys simplify catalog schema model and eliminate unnecessary global ids.
- Recovery gets an explicit global-cutoff replay contract instead of replaying all redo since startup.

### Negative

- Larger refactor surface across catalog, file naming, DDL, and recovery.
- New-cluster-only scope drops compatibility with existing catalog file layout in this RFC.
- Adds catalog-specific overlay/checkpoint machinery that must be maintained.
- Without physical truncation in this stage, disk log usage improvement is deferred.

## Open Questions

None currently.

## Future Work

1. Legacy catalog migration tooling if backward compatibility becomes required.
2. Additional catalog secondary indexes for high-cardinality metadata lookups.
3. Operational metrics/endpoints for catalog checkpoint observability (including trigger source counters).

## References

- `docs/architecture.md`
- `docs/transaction-system.md`
- `docs/index-design.md`
- `docs/checkpoint-and-recovery.md`
- `docs/table-file.md`
- `docs/process/issue-tracking.md`
- `doradb-storage/src/catalog/mod.rs`
- `doradb-storage/src/catalog/storage/mod.rs`
- `doradb-storage/src/catalog/storage/columns.rs`
- `doradb-storage/src/catalog/storage/indexes.rs`
- `doradb-storage/src/session.rs`
- `doradb-storage/src/file/table_fs.rs`
- `doradb-storage/src/trx/sys_conf.rs`
- `doradb-storage/src/trx/recover.rs`
- `doradb-storage/src/table/persistence.rs`
- `doradb-storage/src/trx/redo.rs`
- `docs/backlogs/000002-non-unique-index-for-catalog-tables.md`
- `docs/backlogs/000043-catalog-pure-in-memory-runtime-no-legacy-tbl-files.md`
