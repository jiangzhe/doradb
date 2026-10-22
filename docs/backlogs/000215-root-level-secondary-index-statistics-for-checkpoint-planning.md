# Backlog: Root-level secondary index statistics for checkpoint planning

## Summary

Record statistics for each published secondary DiskTree root and reuse them when planning subsequent checkpoints. Reduce repeated estimation of the existing tree and improve reconstruction-versus-partial-rewrite decisions while retaining one strategy choice per index.

## Reference

- docs/tasks/000332-parallel-disk-tree-checkpoint-reconciliation.md
- docs/rfcs/0033-parallel-disk-tree-construction-and-checkpoint-application.md, Phase 4
- doradb-storage/src/index/build/checkpoint.rs: CheckpointPlanner::choose() and sampled_leaf()
- doradb-storage/src/index/build/checkpoint_builder.rs: Assembler and Writer
- doradb-storage/src/index/disk_tree.rs: collect_reachable_blocks()
- doradb-storage/src/table/persistence.rs: rebuild_reachable_alloc_map()
- doradb-storage/src/catalog/table.rs: SecondaryIndexRoot
- User direction on 2026-10-19: use 16/64 sampling now; record and reuse root-level statistics later; defer per-subtree strategy selection.

## Deferred From (Optional)

Task 000332 / RFC 0033 Phase 4: docs/tasks/000332-parallel-disk-tree-checkpoint-reconciliation.md; docs/rfcs/0033-parallel-disk-tree-construction-and-checkpoint-application.md.

## Deferral Context (Optional)

- Defer Reason: The immediate refinement only changes selector decision rounds to 16 and 64 samples. Root-level statistics need a separate design for storage, maintenance, recovery and publication consistency. The user explicitly deferred this work and excluded per-subtree strategy selection from the current direction.
- Findings: The selector currently estimates leaf count, entry count, survivors, growth and touched leaves using deterministic base-path and merged-mutation samples. Fixed positions can correlate with workload patterns, and repeated positions can reach the same leaf; a completed probe is not a statistical confidence guarantee. SecondaryIndexRoot currently stores only empty/present state and a block reference, while BTreeHeader records page-local counts and occupied space, not subtree totals. The existing reachability traversal reads branch nodes and counts leaf pointers without loading all leaves; it returns total nodes and height after reconciliation. Extending it can obtain leaf/internal-node counts, but exact entry cardinality or key-byte volume cannot be obtained from unread leaves without additional maintained information. Root geometry alone does not establish effective deletions or insertions: owner checks, duplicate mutations and replacements still matter.
- Direction Hint: Prefer statistics attached to each published root and reusable by later checkpoints. Evaluate entry cardinality, leaf/internal-node counts and useful occupancy or encoded-byte measures; define which values are exact and which are estimates. Investigate collecting statistics during construction/reconciliation and reusing existing reachability work rather than introducing a recurring full leaf scan. Define storage and recovery behavior, including unavailable statistics, root reuse, empty roots, rollback, index-generation changes and block-ID reuse. Publish statistics consistently with their owning root; profiling aggregates must not become correctness authority. Keep the current per-index strategy choice. Per-subtree statistics and per-subtree strategy selection are outside this backlog scope.

## Scope Hint

Root-level secondary DiskTree statistics: representation, collection/maintenance, association with the captured root, availability after restart, and use by CheckpointPlanner in checkpoint.rs. Preserve bounded planning and existing memory/I/O admission. Do not add per-subtree strategy selection, cross-index scheduling, or a mandatory full-tree data scan.

## Acceptance Hint

Verify recorded exact counts against independent tree traversal for initial creation, inserts, owner replacements, matching and mismatched deletes, partial subtree reuse, complete removal, no-effect checkpoints and restart. Prove failed or aborted publication cannot expose statistics for a different root or index incarnation. Define a safe fallback when statistics are unavailable. Use matched sparse, broad-delete, skewed, wide-key and growth cases to report selector work, strategy decisions, total checkpoint CPU/latency and statistics-maintenance overhead; validate identical persisted contents under both execution strategies.

## Notes (Optional)

This item follows the 16/64 sampling refinement. Sampling can remain for mutation effects and output packing that root totals do not determine. Related backlogs 000035, 000084, 000107, 000109, 000144, 000158 and 000212 cover other structures, scheduling, reclamation or CREATE; none records reusable secondary-root statistics.

