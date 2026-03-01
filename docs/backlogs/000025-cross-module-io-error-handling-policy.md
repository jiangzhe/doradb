# Backlog: Cross-Module IO Error Handling Policy

## Summary

Define unified IO error handling policy across storage modules to replace ad-hoc or deferred behavior boundaries.

## Reference

1. Source task document: `docs/tasks/000035-readonly-buffer-pool-harden-perf-cleanup.md`.
2. Open question: general cross-module IO error policy.

## Scope Hint

- Classify recoverable vs fatal IO errors by subsystem.
- Define propagation, retry, and logging expectations.
- Update affected modules incrementally with tests.

## Acceptance Hint

A documented cross-module policy exists and core paths align with it in code/tests.

