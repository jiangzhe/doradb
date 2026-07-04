# Backlog: Add doradb-bench read workloads

## Summary

Add doradb-bench read workload support beyond the initial load workloads, including sequential reads, random point lookups, missing-key lookups, index scans, and future batched read equivalents.

## Reference

docs/tasks/000211-create-doradb-bench-load-benchmark-crate.md; docs/benchmark-tool.md; docs/backlogs/000074-expand-runtime-lookup-benchmark-coverage.md

## Deferred From (Optional)

docs/tasks/000211-create-doradb-bench-load-benchmark-crate.md

## Deferral Context (Optional)

- Defer Reason: Task 000211 intentionally establishes the benchmark crate and only the first load workloads so lifecycle, manifest, output, and worker semantics can settle before read semantics are added.
- Findings: The implemented crate now has prepare, run, cleanup, fixed manifests, result artifacts, deterministic key ranges, and public-session execution. The warmup phase was removed because doradb-bench targets an embedded storage engine. Read workloads need separate decisions for hot versus cold state, lookup/index choice, missing-key ranges, and whether batched reads should share controls with write batches.
- Direction Hint: Prefer reusing the manifest key-range and result-output machinery from doradb-bench. Keep read execution on public Session and Statement APIs, distinguish hot-cache from cold/persisted measurements explicitly without assuming a warmup command, and link or coordinate with backlog 000074 for cold persisted lookup expansion.

## Scope Hint

Design and implement read workload CLI controls, key selection, lookup or scan execution through public storage APIs, result reporting, and tests without depending on storage internals.

## Acceptance Hint

doradb-bench can run documented readseq/readrandom/readmissing or equivalent point and index read workloads against a prepared benchmark table, report the same result artifacts, and pass focused CLI, generation, smoke, and storage regression tests.

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
