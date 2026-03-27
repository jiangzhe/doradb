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

## Close Reason

- Type: implemented
- Detail: Implemented via docs/tasks/000092-benchmark-and-improve-iouring-single-miss-latency.md, which exposed readonly cache hit/miss metrics through shared BufferPoolStats, readonly global_stats(), tests, and benchmark reporting.
- Closed By: backlog close
- Reference: docs/tasks/000092-benchmark-and-improve-iouring-single-miss-latency.md
- Closed At: 2026-03-27
