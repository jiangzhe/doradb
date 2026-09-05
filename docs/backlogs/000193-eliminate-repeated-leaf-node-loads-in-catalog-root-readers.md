# Backlog: Eliminate repeated leaf-node loads in catalog root readers

## Summary

Reuse validated index-leaf row metadata during catalog root enumeration and
decoding so each LWC entry does not cause another load of the same leaf node.
Preserve complete-image checkpoints and all projected integrity checks while
reducing cache-independent logical reads and repeated leaf decoding.

## Reference

Discovered while implementing and resolving
`docs/tasks/000295-catalog-checkpoint-scale-proof.md` for
`docs/rfcs/0031-compact-numeric-catalog-table-definitions.md` Phase 8.
Relevant paths are `CatalogStorage::load_rows_from_root`,
`visit_projected_catalog_column`, `ColumnBlockIndex::collect_leaf_entries`,
and `load_delete_deltas_and_row_ids`.

## Deferred From (Optional)

docs/tasks/000295-catalog-checkpoint-scale-proof.md; docs/rfcs/0031-compact-numeric-catalog-table-definitions.md Phase 8

## Deferral Context (Optional)

- Defer Reason: All target-envelope cases pass; the approved validation fusion left per-entry leaf rereads outside its bounded scope. Changing the traversal/decoding interface warrants a focused follow-up.
- Findings:
  Fusing projected parent and descriptor validation reduced stress managed-CREATE
  logical reads from 559,611,904 to 376,963,072 bytes while physical reads stayed
  at 1,439 and writes stayed at 94,404,608 bytes. Each remaining full root scan
  enumerates index leaves, then loads a leaf again per LWC entry to obtain row IDs
  and deletion state. The stress descriptor image has 1,389 LWC entries sharing
  one index page: each full scan currently costs 2,779 logical 64-KiB accesses
  (one index traversal, 1,389 leaf reloads, and 1,389 LWC reads).
  The separate debug-only deletion read has already been removed; the combined
  reader now rejects catalog delete deltas in every build. This follow-up concerns
  the remaining enumeration-to-decoding rereads, not that resolved duplication.
- Direction Hint:
  Investigate carrying validated row identities and deletion state from the index
  walk into each LWC decode, or processing entries while their validated leaf is
  available. Reuse existing scan-entry abstractions where appropriate. Preserve
  sparse row IDs, typed corruption errors, and pre-publication CoW atomicity;
  do not suppress logical accounting to make the metric smaller.
  Measure warm/cold logical reads, physical reads, CPU, and memory on the same
  small/target/stress workloads. Backlog 000192 covers current-state managed
  definition caching and cannot substitute for projected durable-root validation.

## Scope Hint

Reuse validated leaf row metadata across catalog enumeration and decoding without changing durable formats or full-root rewrite semantics.

## Acceptance Hint

Eliminate per-entry reloads of already traversed catalog index leaves; retain sparse-row and deletion rejection correctness, cache-independent accounting, publication safety, and both I/O backend tests; rerun all nine release benchmark cases.

## Notes (Optional)


