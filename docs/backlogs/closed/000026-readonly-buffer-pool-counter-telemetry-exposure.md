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

## Close Reason

- Type: implemented
- Detail: Implemented by tasks 000092 and 000193. The storage crate now exposes public BufferPoolStats through Session::buffer_pool_stats and root/stats re-exports; the readonly disk pool snapshot includes capacity, allocation, hit/miss, miss join, read lifecycle/error, and write lifecycle/error counters. Focused verification passed for public stats snapshots and readonly cold/warm/shared/dedup behavior. Shared evictor wake/run counters remain intentionally out of scope for this backlog closure.
- Closed By: backlog close
- Reference: docs/tasks/000092-benchmark-and-improve-iouring-single-miss-latency.md; docs/tasks/000193-session-maintenance-interface.md
- Closed At: 2026-06-29
