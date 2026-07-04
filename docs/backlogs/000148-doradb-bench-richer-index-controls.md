# Backlog: Add doradb-bench richer index controls

## Summary

Extend doradb-bench index-shape controls beyond the initial none and unique modes to cover non-unique indexes, multiple indexes, composite keys, and richer key-distribution options.

## Reference

docs/tasks/000211-create-doradb-bench-load-benchmark-crate.md; docs/benchmark-tool.md; docs/index-design.md

## Deferred From (Optional)

docs/tasks/000211-create-doradb-bench-load-benchmark-crate.md

## Deferral Context (Optional)

- Defer Reason: Task 000211 intentionally limits index support to none and one unique secondary index so the first load benchmark can ship with a small, well-defined schema surface.
- Findings: The implemented manifest records index mode and prepare creates either no secondary indexes or one unique secondary index on logical_key. DoraDB allows duplicate logical keys without a unique index, so future index modes need explicit duplicate and key-distribution semantics.
- Direction Hint: Keep manifest schema evolution explicit and backward-compatible where practical. Add index modes incrementally, prefer clear names over overloading the existing unique flag, and document how each mode affects fillrandom key generation and duplicate allowance.

## Scope Hint

Design CLI and manifest compatibility for additional index modes, create-table schema generation, deterministic key generation, duplicate-key expectations, and tests for each index shape.

## Acceptance Hint

doradb-bench supports documented non-unique or multiple-index configurations without breaking existing manifests, preserves deterministic generation where promised, and validates schema creation and load behavior through public storage APIs.

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
