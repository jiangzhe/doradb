# Backlog: Readonly Cache Hit/Miss Metrics Exposure

## Summary

Expose readonly cache hit/miss metrics for operational tuning and observability.

## Reference

1. Source task document: `docs/tasks/000034-readonly-column-buffer-pool-phase-4-block-index-integration-refactor.md`.
2. Deferred follow-up: optional readonly cache metrics.

## Scope Hint

- Define metric names, scope, and collection overhead constraints.
- Provide access path (internal API, logs, or export surface).
- Add tests for counter correctness under concurrent reads/evictions.

## Acceptance Hint

Readonly hit/miss metrics are available with validated correctness and documented usage.

