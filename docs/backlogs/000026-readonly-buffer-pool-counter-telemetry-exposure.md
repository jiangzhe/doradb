# Backlog: Readonly Buffer Pool Counter/Telemetry Exposure

## Summary

Add broader readonly buffer-pool telemetry exposure (beyond basic hit/miss) to support tuning and diagnosis.

## Reference

1. Source task document: `docs/tasks/000035-readonly-buffer-pool-harden-perf-cleanup.md`.
2. Open question: deferred counter/telemetry exposure.

## Scope Hint

- Define core counters (loads, evictions, inflight dedup behavior, failures).
- Decide export/report interfaces.
- Validate counter behavior under concurrency and teardown.

## Acceptance Hint

Readonly telemetry API/reporting is implemented with stable semantics and tests.
