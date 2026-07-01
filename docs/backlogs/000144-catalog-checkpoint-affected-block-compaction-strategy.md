# Backlog: Catalog checkpoint affected-block compaction strategy

## Summary

Design an optional affected-block catalog checkpoint compaction strategy for future catalog tables where full-root rewrite becomes too expensive.

## Reference

Deferred while resolving docs/tasks/000207-catalog-checkpoint-pk-folding-and-compaction.md after implementing compact full-root rewrite for changed catalog tables.

## Deferred From (Optional)

docs/tasks/000207-catalog-checkpoint-pk-folding-and-compaction.md; docs/backlogs/000142-catalog-checkpoint-pk-aware-folding-and-compaction-design.md

## Deferral Context (Optional)

- Defer Reason: Task 000207 chose correctness-first compact full-root rewrite and intentionally left affected-block CoW compaction out of scope.
- Findings: Catalog tables are currently small enough for full-root rewrite, and PK-folded roots now remove delete-delta accumulation. Affected-block rewriting would add API and strategy complexity without being required for this task's correctness goal.
- Direction Hint: Prefer keeping full-root rewrite as the default. Revisit affected-block compaction only with size, churn, or checkpoint-budget evidence, and make it a strategy layered on top of the same PK fold semantics rather than a separate correctness model.

## Scope Hint

Define block-local rewrite APIs, strategy selection inputs, and integration with catalog checkpoint root publication while preserving PK fold correctness and catalog recovery semantics.

## Acceptance Hint

A future task or RFC decides whether affected-block compaction is needed, documents strategy selection against full-root rewrite, and covers unchanged rows, updated rows, deletes, block reachability, recovery, and allocation-map safety.

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
