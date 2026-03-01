# Backlog: Readonly Miss-Load IO Failure Policy

## Summary

Define stable runtime policy for readonly cache miss-load IO failures (panic vs explicit retry/recovery/error propagation).

## Reference

1. Source task document: `docs/tasks/000032-readonly-column-buffer-pool-phase-2-core.md`.
2. Open question: panic behavior in later phases.

## Scope Hint

- Evaluate operational impact of panic behavior.
- Define API/error-surface expectations for callers.
- Implement and test selected policy.

## Acceptance Hint

Readonly miss-load failure behavior is explicit, implemented, and covered by tests.
