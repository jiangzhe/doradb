# Backlog: Evaluate adaptive optimistic RowPageIndex prefix pruning

## Summary

Benchmark whether exclusive-root checkpoint-prefix validation creates a material synchronization bottleneck, and implement adaptive optimistic planning only when workload evidence justifies its complexity.

## Reference

Task 000222 implementation review found that every prune must lock the fixed root for mutation, while optimistic planning can only reduce the duration of that lock. The current implementation validates and applies under one exclusive root guard.

## Deferred From (Optional)

docs/tasks/000222-prune-purged-row-page-prefixes-from-block-index.md

## Deferral Context (Optional)

- Defer Reason: The current exclusive-root implementation is simpler, passes validation, and has no demonstrated production synchronization bottleneck. Adding optimistic planning now would substantially enlarge concurrency reasoning and test scope without measured benefit.
- Findings: Root locking is unavoidable during mutation. Exclusive validation performs one scan with guaranteed progress but holds the root for validation and apply. Optimistic planning shortens the normal root-exclusive window but adds version checks, plan state, concurrency hooks, and up to a second full scan on fallback. Root-leaf validation is bounded to one 64 KiB node and is likely cheaper exclusively; multi-level prefixes are the plausible benefit case because ordinary right-edge appends often do not modify the planned left path.
- Direction Hint: Start with instrumentation and workload benchmarks rather than implementation. Revisit adaptive planning only when root-exclusive hold time or foreground tail latency is material. Avoid unbounded optimistic retries; use at most one optimistic attempt and fall back immediately to exclusive-root validation. Preserve specific invariant errors, top-down path locking, exact prefix proof, and unlink-before-deallocation semantics.

## Scope Hint

Measure retirement batch size, tree height, root-exclusive wait and hold time, foreground lookup/cursor/append latency, and optimistic invalidation rate across root-leaf and multi-level indexes. If results show a material bottleneck, prefer direct exclusive validation for root-leaf trees and one optimistic plan attempt for branch roots with immediate exclusive-root fallback on invalidation.

## Acceptance Hint

Publish reproducible benchmark results for idle and concurrent lookup, cursor, and append workloads. Either retain exclusive-root validation with evidence that contention is immaterial, or implement and test the adaptive strategy with deterministic invalidation, fallback, lookup, cursor, append, and structural correctness coverage.

## Notes (Optional)


## Close Reason (Added When Closed)

When a backlog item is moved to `docs/backlogs/closed/`, append:

```md
## Close Reason

- Type: <implemented|stale|replaced|duplicate|wontfix|already-implemented|other>
- Detail: <reason detail>
- Closed By: <backlog close>
- Reference: <task/issue/pr reference>
- Closed At: <YYYY-MM-DD>
```
