---
id: 000053
title: Catalog Checkpoint Now with MultiTable Roots
status: proposal  # proposal | implemented | superseded
created: 2026-03-08
github_issue: 393
---

# Task: Catalog Checkpoint Now with MultiTable Roots

## Summary

Implement RFC-0006 Phase 6 as a scoped `checkpoint_now` path only:
1. persist catalog checkpoint data directly in `catalog.mtb` by reusing `MultiTableMetaPage.table_roots`;
2. materialize each catalog table checkpoint with reusable `ColumnBlockIndex` and `LwcPage` structures;
3. replay catalog redo in `(last_catalog_checkpoint_cts, W]` and publish one CoW root;
4. defer background checkpoint worker and redo-maintenance worker to follow-up work.

## Context

RFC-0006 Phase 6 requires unified catalog checkpoint persistence. Current code already has:
1. `catalog.mtb` with `MultiTableMetaPage` metadata and `table_roots` slots;
2. reusable on-disk structures for table checkpoint (`ColumnBlockIndex`, `LwcPage`);
3. no active catalog checkpoint implementation yet.

Design decisions confirmed in review:
1. do not add a separate overlay-root pointer; reuse `MultiTableMetaPage.table_roots` as catalog table checkpoint roots;
2. `last_catalog_checkpoint_cts` must be the same semantic value as `catalog.mtb` active-root `trx_id`;
3. catalog secondary indexes are not checkpointed in this phase and are rebuilt from checkpoint rows + replay logs.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Parent RFC:`
`- docs/rfcs/0006-cache-first-unified-catalog-storage-refactor.md`

## Goals

1. Add `Catalog::checkpoint_now()` as an explicit ad-hoc checkpoint API.
2. Persist catalog table checkpoint state in `catalog.mtb` via `MultiTableMetaPage.table_roots` without introducing new parallel root indirection.
3. Reuse existing CoW table persistence structures as much as possible:
   - `ColumnBlockIndex` for mapping `(start_row_id -> lwc_block_page_id)`,
   - `LwcPage` for checkpointed catalog row payload.
4. Define `last_catalog_checkpoint_cts` as active-root `trx_id` in `catalog.mtb` and keep it monotonic.
5. Keep current catalog runtime behavior cache-first and in-memory for foreground metadata operations.

## Non-Goals

1. Background catalog checkpoint worker (periodic timer, dirty-event wakeup).
2. Redo log maintenance worker (global truncation/maintenance orchestration).
3. Recovery replay cutoff changes (`cts > W`) and fail-fast consistency policy (RFC-0006 Phase 7).
4. Physical redo-log truncation implementation.
5. Persisting catalog secondary-index pages in checkpoint output.

## Unsafe Considerations (If Applicable)

No new `unsafe` scope is expected. This task focuses on checkpoint orchestration and CoW integration across existing safe abstractions.

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Add `checkpoint_now` API surface:
   - add `Catalog::checkpoint_now()` entrypoint;
   - route to catalog storage checkpoint implementation.
2. Implement checkpoint replay boundary:
   - read `last_catalog_checkpoint_cts` from `catalog.mtb` active root `trx_id`;
   - compute `W` from currently available persisted user-table watermarks (at minimum user table `heap_redo_start_ts`), bounded by globally persisted redo state;
   - replay catalog redo records in `(last_catalog_checkpoint_cts, W]`.
3. Build catalog table checkpoint pages with reusable structures:
   - append replayed rows into LWC page batches per catalog table;
   - update per-table column block-index roots in CoW style;
   - keep append-focused fast path for current `CreateTable`-only catalog mutations.
4. Reuse/extend CoW write interfaces:
   - generalize `ColumnBlockIndex` update path so it can write through `MultiTableFile` mutable state (currently coupled to `MutableTableFile`);
   - add required mutable write helpers on `MultiTableFile` side for page allocation, page write, and obsolete-page tracking.
5. Publish checkpoint atomically:
   - update `table_roots` and `next_user_obj_id` in `MultiTableMetaPage`;
   - commit new `catalog.mtb` root with `trx_id = W`;
   - enforce invariant that active-root `trx_id` equals persisted `last_catalog_checkpoint_cts`.
6. Keep catalog index handling replay-based:
   - do not persist catalog secondary-index structures in checkpoint;
   - ensure rebuild path remains from checkpoint rows and replay logs.

## Implementation Notes


## Impacts

1. `doradb-storage/src/catalog/mod.rs`
2. `doradb-storage/src/catalog/storage/mod.rs`
3. `doradb-storage/src/file/multi_table_file.rs`
4. `doradb-storage/src/file/meta_page.rs`
5. `doradb-storage/src/index/column_block_index.rs`
6. `doradb-storage/src/trx/log.rs`
7. `doradb-storage/src/trx/log_replay.rs`
8. New catalog checkpoint support module(s) under `doradb-storage/src/catalog/` if needed for replay/apply orchestration.

## Test Cases

1. `checkpoint_now` publishes a new `catalog.mtb` active root when replay range is non-empty.
2. Published active-root `trx_id` is monotonic and equals checkpoint boundary `last_catalog_checkpoint_cts`.
3. `table_roots` entries are updated for catalog tables with appended checkpoint data and can be reloaded after restart.
4. Replay range is incremental:
   - first checkpoint replays from initial root to `W1`,
   - second checkpoint replays only `(W1, W2]`.
5. Restart + recovery correctness:
   - load checkpointed catalog rows from `catalog.mtb`,
   - replay newer catalog logs,
   - rebuild catalog indexes and preserve metadata lookup correctness.
6. No-op checkpoint behavior:
   - when no eligible catalog redo exists in `(last_catalog_checkpoint_cts, W]`, checkpoint returns without root regression.

## Open Questions

1. Should this phase include a small follow-up helper to expose checkpoint diagnostics (`last_catalog_checkpoint_cts`, replayed-row counts), or keep all observability for the deferred maintenance-worker task?
2. If future catalog mutations introduce heavy delete workloads before maintenance worker exists, do we need a temporary policy for deletion-field growth thresholds in catalog checkpoint pages?
3. Follow-up task is required for combined redo-maintenance worker design:
   - periodic scheduling,
   - dirty-event coalescing,
   - checkpoint + truncation coordination.
