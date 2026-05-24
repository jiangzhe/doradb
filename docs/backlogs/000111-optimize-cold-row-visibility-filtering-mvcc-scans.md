# Backlog: Optimize cold-row visibility filtering for MVCC scans

## Summary

Improve full-table MVCC cold-row scan performance by precomputing eligible deletion markers once per scan instead of checking deletion-buffer visibility row by row.

## Reference

Discovered while reviewing docs/tasks/000156-full-table-scan-mvcc.md cold row visibility implementation. Current cold scan validates each persisted row against ColumnDeletionBuffer state and persisted deletion sets during LWC block iteration.

## Deferred From (Optional)

docs/tasks/000156-full-table-scan-mvcc.md

## Deferral Context (Optional)

- Defer Reason: Current task is focused on correctness and create-index scan choice. Cold-row visibility optimization changes a hot scan path and needs separate performance design and validation, so it is postponed.
- Findings: The proposed improvement is to scan and filter ColumnDeletionBuffer once for committed row ids visible before scan STS, sort row ids, then compare against sorted LWC row ids or pivots during block scans. Persisted deletion set checks may be improved with the same batched or pivoted treatment.
- Direction Hint: Prefer scan-local precomputation and merge or pivot comparisons over per-row lookups. Consider range-limiting by LWC block pivots, bounded allocation for large deletion buffers, and whether parallel deletion-buffer filtering or LWC-block scanning is beneficial. Keep this separate from CREATE INDEX current-state semantics.

## Scope Hint

Design and implement a scan-local prefilter: traverse the column deletion buffer once, collect committed row ids whose CTS is below scan STS, sort them, then use pivot or merge-style comparison while scanning each LWC block. Revisit persisted deletion set handling with the same goal of reducing repeated work. Preserve MVCC correctness and existing cold/hot snapshot semantics.

## Acceptance Hint

MVCC table scan tests still pass for cold rows, deletes, updates, long-running snapshots, and cold/hot boundary cases. New performance-oriented tests or benchmarks show reduced repeated deletion visibility work. Implementation documents memory and CPU tradeoffs for large deletion buffers and LWC block ranges.

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
