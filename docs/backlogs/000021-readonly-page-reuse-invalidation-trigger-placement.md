# Backlog: Readonly Page-Reuse Invalidation Trigger Placement

## Summary

Finalize invalidation trigger placement for reused page IDs in readonly cache workflows (allocation-time, commit-time, or both).

## Reference

1. Source task document: `docs/tasks/000031-readonly-column-buffer-pool-phase-1.md`.
2. Open question: invalidation timing policy.

## Scope Hint

- Compare correctness and complexity tradeoffs for trigger timing options.
- Validate behavior under checkpoint/root-swap and page reuse races.
- Align with existing invalidation APIs and lifecycle ordering.

## Acceptance Hint

Trigger placement is chosen and validated with race/regression tests.

