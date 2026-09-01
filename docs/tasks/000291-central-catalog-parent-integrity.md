---
id: 000291
title: Central Catalog Parent Integrity
status: implemented
created: 2026-08-31
github_issue: 1035
---

# Task: Central Catalog Parent Integrity

## Summary

Implemented RFC-0031 Phase 4 by making `catalog.tables` the checked parent
of all five satellite catalog tables. One shared logical validator now checks
the complete live catalog after recovery replay and the complete projected
catalog root set before checkpoint publication.

DROP TABLE now locks all six catalog tables, discovers and verifies its
cascade through bounded catalog indexes, returns typed integrity failures for
malformed catalog state, and proves that no central or satellite row survives
in its staged read-your-writes view. The catalog-only primitive
`index_lookup_current_locked` provides that raw current view while binding
the read to the private transaction and its retained DDL lock authority.

## Context

Parent RFC:

- `docs/rfcs/0031-compact-numeric-catalog-table-definitions.md`, Phase 4

Prerequisite task:

- `docs/tasks/000290-atomic-numeric-format-cutover-and-replay-safe-allocation.md`

Issue Labels:

- type:task
- priority:high
- codex

Phase 3 installed the final six catalog roots and five satellite schemas:
`catalog.columns`, `catalog.indexes`, `catalog.table_descriptors`,
`catalog.table_replay_silent_watermarks`, and
`catalog.table_bindings`. Every satellite stores a parent `table_id`, but
the previous table-driven reconstruction path could not discover an
independently orphaned satellite.

The invariant must hold at two durable boundaries. Recovery has a quiescent
in-memory catalog containing checkpoint bootstrap plus replayed redo.
Checkpoint preparation has a not-yet-published array of projected catalog
roots. Online DROP needs the same logical rule for one target, but must use
bounded indexed reads rather than a complete catalog scan.

## Goals

1. Validate every live or projected satellite row against the complete
   `catalog.tables.table_id` set.
2. Fail recovery before metadata reconciliation, index rebuilding, or
   foreground admission when a checkpointed or replay-visible orphan exists.
3. Fail checkpoint preparation before root, replay cursor, watermark-cache,
   allocation-map, or provisional-reservation publication.
4. Make DROP discovery and final validation index-driven and read-your-writes.
5. Replace reachable DROP catalog assertions with typed
   `DataIntegrityError::InvalidRootInvariant` reports.
6. Retain the checkpoint-durable root-plus-silent-watermark replay floor after
   a successful DROP.
7. Supply a checked central-parent helper for the later binding API.
8. Keep projected validation linear in catalog rows, with owned memory
   proportional only to the central parent count.

## Non-Goals

1. Populate or expose descriptor and binding product APIs.
2. Implement Phase 5 dropped-index slot reuse.
3. Add a general foreign-key or constraint registry.
4. Change catalog schemas, redo encodings, persistent formats, or public APIs.
5. Interpret descriptor payloads, binding keys, or higher-layer metadata.
6. Replace full-image catalog checkpointing with an incremental model.

## Rejected Alternatives

### Infer Integrity From Table Reconstruction

Starting from known central rows cannot discover an independently orphaned
satellite. Recovery and checkpoint therefore scan every satellite directly.

### Use Whole-Table Scans For DROP

DROP already owns one exact `table_id` and retains target-specific exclusion.
Bounded primary-key ranges and the binding reverse index keep one table's DDL
cost independent of total catalog size.

### Introduce A General Constraint Engine

Phase 4 has one fixed parent relationship. A static satellite inventory keeps
the persisted and runtime contract explicit without introducing generic
constraint machinery.

## Plan

### Shared Parent Invariant

`catalog/storage/integrity.rs` owns the fixed five-entry satellite inventory,
including diagnostic name and parent-column ordinal. The validator first
builds one `FastHashSet<TableID>` from live central rows, then visits every
satellite row once and requires membership.

Live recovery validation uses `table_scan_uncommitted` over raw latest
in-memory rows after bootstrap and redo are complete. It filters deleted rows
and intentionally does not trust catalog indexes while diagnosing complete
catalog corruption.

Projected validation checks every root descriptor against its canonical slot,
walks compact-root column-index entries, rejects delete deltas and row-count
disagreement, and decodes only the selected parent column from each LWC row.
Opaque descriptor, binding, and index payloads are neither interpreted nor
retained.

Both views return `InvalidRootInvariant` with the view kind, satellite name,
satellite table ID, and orphan parent ID.

### Locked Current Index Reads

`CatalogTable::index_lookup_current_locked` accepts a
`PrivateTransaction`, catalog index slot, and an `IndexLookupCriteria` defined
with the in-memory table implementation. Its variants select one of:

- unique exact key;
- unique inclusive logical-key range; or
- non-unique exact equality.

The lookup requires metadata-S and data-IX, or stronger, for the selected
catalog table. Doradb DDL keeps those claims in the accepted operation scope
while its nested private transaction owns the same family authority, so the
proof checks that exact retained operation scope. Focused internal tests may
also supply equivalent transaction-scope claims.

The implementation scans only matching MemIndex candidates, resolves each
candidate to its raw current row, filters deleted rows, rejects catalog cold
locations, and rechecks the current logical key before invoking an early-stop
visitor. The lower-level `MemTable::catalog_index_lookup_current` keeps this
catalog-only scope explicit and drains every criterion stream through one
statically dispatched candidate visitor. It performs no MVCC snapshot
selection and is not available for unlocked catalog reads.

Existing unlocked exact and whole-table helpers retain their prior visibility
contracts. DROP-specific column and index enumeration uses new bounded
transaction-bound helpers.

### Durable Boundary Integration

Recovery runs live parent validation after redo replay and index-DDL root
classification, before user-table metadata reconciliation, absent-file
cleanup, and recovered hot-index rebuilding. Catalog failures are recontexted
as `RuntimeError::Recovery` without losing their integrity source.

Checkpoint preparation validates all six projected roots after folding every
included catalog operation and before applying checkpoint metadata, preparing
allocation-map publication, or loading the projected watermark cache. This
also applies to metadata-only publications; stale and no-progress `Noop`
paths still publish nothing and perform no scan.

Dropping a failed mutable checkpoint fork leaves active roots,
`catalog_replay_start_ts`, the durable watermark cache, and provisional index
reservations unchanged.

### DROP Final State And Replay Floor

DROP acquires all six catalog table metadata-S/data-IX pairs in canonical slot
order. It deletes indexes, columns, and the replay-silent watermark before the
central table row. Count mismatches and a missing central row return typed
integrity failures.

Before installing DROP redo, the staged final view must contain none of:

- the exact central row;
- any column or index in the target's bounded primary-key ranges;
- the exact descriptor or replay-silent-watermark row; or
- any binding found through the non-unique reverse `table_id` index.

Descriptor and binding tables remain empty in the Phase 4 product surface.
Injected survivors abort DROP; Phase 6 and Phase 7 must add their indexed
deletes before this proof rather than weakening it.

After lifecycle closure and checkpoint drain, but before catalog deletion,
DROP captures the fieldwise maximum of the stable table-root replay floor and
checkpoint-durable silent-watermark overlay. The captured value is retained in
dropped-table operational state after commit, independent of watermark-row
deletion.

An integrity failure after lifecycle closure rolls back the private
transaction best-effort, preserves the typed source, poisons the engine, and
never commits partial catalog redo.

### Checked Parent Resolution

`CatalogStorage::require_catalog_parent` accepts a private transaction,
known satellite table ID, and parent table ID. It validates the satellite
inventory entry and performs an exact locked-current lookup in
`catalog.tables`. Missing parents and unknown satellite IDs are integrity
errors rather than `TableNotFound`.

Phase 7 binding resolution must acquire the required catalog locks, find the
binding row, and call this helper in the same transaction view.

## Implementation Notes

Implemented central parent integrity across recovery, checkpoint, and DROP boundaries.

The delivered work includes the complete live/projected parent validator,
locked-current index lookup, six-table DROP proof, pre-deletion replay-floor
capture, recovery/checkpoint gates, and durable boundary documentation.

Review found that DDL locks are retained by the accepted operation scope rather
than copied into the nested transaction's exact scope. The final authority
check therefore binds the private transaction to its stable operation key and
verifies the matching claims in the shared family authority. This preserves
the selected `index_lookup_current_locked` semantics without weakening it to
an arbitrary family-wide or unlocked read.

The required metadata-only checkpoint fast path still skips allocation-map
reconstruction, but now reads unchanged catalog roots for parent validation as
required. Its test was updated to verify that roots and allocation state remain
unchanged while validation reads occur.

Follow-up review moved the lookup criteria beside the in-memory table as
`IndexLookupCriteria` and unified the exact, range, and non-unique candidate
draining without boxing or dynamic dispatch.

No persistent format, schema, redo encoding, public API, or alternate I/O
backend path changed.

Final verification:

- `tools/style_audit.rs --diff-base origin/main`: passed for 16 Rust files.
- `rtk cargo clippy --workspace --all-targets -- -D warnings`: passed.
- `rtk cargo nextest run --workspace`: 1,857 tests passed.

## Impacts

- Catalog storage now owns one cross-table parent-integrity boundary for live
  recovery state and projected checkpoint roots.
- Catalog MemIndex access has a crate-private, lock-authority-bound raw current
  visitor for exact, range, and non-unique equality reads.
- DROP holds two additional catalog table lock pairs and performs bounded
  indexed cascade discovery and final absence validation.
- Recovery and checkpoint preparation fail closed on catalog parent
  corruption with typed diagnostics.
- Dropped-table redo retention now owns a replay floor captured before
  watermark deletion.
- Recovery and checkpoint documentation records the new ordering and
  unchanged-publication guarantees.
- Public API and durable compatibility are unchanged.

## Test Cases

1. Raw live validation rejects an independently injected orphan in each of the
   five satellite tables and accepts a complete multi-satellite view.
2. Projected checkpoint validation rejects an orphan before publication and
   preserves the active meta block, roots, replay cursor, and durable
   watermark-cache identity.
3. Locked-current lookup rejects missing lock authority; exact parent lookup,
   bounded composite ranges, populated non-unique equality, early stop, and
   staged deletion filtering execute through catalog and DROP tests.
4. Column deletion by table ID is bounded, counted, idempotent, and leaves
   other table IDs untouched.
5. DROP with a missing central row reports typed integrity, rolls back staged
   child deletion, poisons after lifecycle closure, and emits no partial DROP.
6. Existing DROP lifecycle, replay retention, crash/restart, checkpoint,
   recovery, and DDL lock-ownership suites pass with the six-table lock set.
7. Metadata-only and changed-root checkpoints preserve their existing
   publication/reclamation behavior while enforcing projected parent checks.

## Open Questions

None. Phase 6 must delete descriptors before the existing DROP proof. Phase 7
must delete bindings through their reverse index and use the checked parent
helper in the same locked current view.
