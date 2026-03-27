# Backlog: Add batch-IO backend efficiency benchmark baseline

## Summary

Track a follow-up benchmark that exercises multiple outstanding IO operations so the repository can demonstrate backend efficiency under batched work and preserve a reproducible baseline for future io_uring and libaio optimization.

## Reference

1. User follow-up on 2026-03-27 after task 000092 benchmark rerun: io_uring is expected to show its strengths on batched IO operations rather than the serialized single-miss scenario. 2. Task 000092 intentionally scoped batch IO evaluation out of the single-miss latency investigation. 3. Current examples cover serialized readonly single-miss latency but do not provide a dedicated batched backend-comparison baseline.

## Scope Hint

Add one dedicated batched-IO benchmark scenario, likely under doradb-storage/examples/, that runs the same block-backed workload under default io_uring and explicit libaio with configurable outstanding depth or batch size, reports backend stats plus throughput/latency summaries, and uses a workload shape that keeps multiple operations in flight so backend API efficiency is visible rather than hidden by serialized miss cost.

## Acceptance Hint

The repository includes a reproducible batched-IO benchmark that can be run against both backends on the same block-backed filesystem, the output makes backend efficiency comparable with a stable baseline for future optimization work, and the scenario is documented clearly enough that later changes can use it as a before/after performance reference without conflating it with the serialized single-miss benchmark.

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
