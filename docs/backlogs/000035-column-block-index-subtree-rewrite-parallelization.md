# Backlog: Column Block Index Subtree Rewrite Parallelization

## Summary

Explore and implement parallel subtree rewrite for block-index patching, including required staged mutable-root mutation and allocator partition design.

## Reference

1. Source task document: `docs/tasks/000039-unify-new-data-deletion-checkpoint-table-persistence.md`.
2. Open question: parallel subtree rewrite feasibility.

## Scope Hint

- Design mutation/allocator isolation strategy.
- Preserve CoW snapshot guarantees and deterministic root publication.
- Add stress tests for patch correctness under parallel rewrite.

## Acceptance Hint

Parallel subtree rewrite design is validated and implemented with no CoW correctness regressions.

