# Backlog: Deletion Checkpoint RowID-to-Block Resolution Parallelization

## Summary

Parallelize row-id to target-block resolution for disjoint ranges during deletion checkpoint processing.

## Reference

1. Source task document: `docs/tasks/000039-unify-new-data-deletion-checkpoint-table-persistence.md`.
2. Open question: row-id-to-block resolution parallelization.

## Scope Hint

- Define safe parallel range partitioning.
- Avoid root/view inconsistency during lookup.
- Measure gains on large deletion batches.

## Acceptance Hint

Resolution stage scales with parallel workers while preserving mapping correctness.

