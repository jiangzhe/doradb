# Backlog: Capture Lock-Family Cutover Benchmark Comparison

## Summary

Capture reproducible repeated pre-cutover and final-candidate lock benchmark evidence with equivalent scenario instrumentation.

## Reference

Deferred from docs/tasks/000260-physical-lock-family-aggregation-performance-cutover.md and docs/rfcs/0027-session-family-logical-lock-system-redesign.md Phase 3; candidate-only measurements were recorded during task resolution.

## Deferred From (Optional)

docs/tasks/000260-physical-lock-family-aggregation-performance-cutover.md; docs/rfcs/0027-session-family-logical-lock-system-redesign.md Phase 3

## Deferral Context (Optional)

- Defer Reason: The expanded scenario instrumentation and physical aggregation cutover landed in one commit, so no preserved pre-cutover binary can execute an equivalent scenario matrix.
- Findings: The final candidate completed all ten scenarios across 26 valid configurations with zero failures and matching structural counters, but the measurements were single samples from an optimized debug-assertion build and lack an equivalent baseline.
- Direction Hint: Create a reproducible pre-cutover benchmark branch by applying only compatible scenario instrumentation to the Phase 2 representation, then build both revisions with the same standard release profile and automate repeated paired trials.

## Scope Hint

Build equivalent pre-cutover and final release binaries, prepare identical roots, run every lock-table operation class with matching modes, widths, sessions, threads, seeds, and host conditions, and retain machine-readable results.

## Acceptance Hint

Repeated paired runs report throughput and average latency median, interquartile range, and full range together with structural lock counters; differences are explained and the procedure is reproducible from committed documentation or scripts.

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
