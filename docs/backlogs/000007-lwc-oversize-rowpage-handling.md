# Backlog: LWC Oversize RowPage Handling

## Summary

Define and implement deterministic behavior when a single source row-page payload cannot fit into one LWC page due to large varbyte content or poor compression.

## Reference

1. Source task document: `docs/tasks/000004-gather-row-pages-to-lwc.md`.
2. Open question: handling unlikely but possible oversized single-row-page cases.
3. Task 000323 implements row-count splitting while preserving this value-byte limit follow-up.

## Scope Hint

- Define fallback strategy (split, spill, reject, or alternate encoding).
- Ensure checkpoint flow handles the fallback without data loss.
- Add targeted tests for oversized inputs.

## Acceptance Hint

Oversized source cases are handled explicitly and tested; checkpoint conversion no longer relies on undocumented assumptions.


## Deferred From (Optional)

docs/tasks/000323-inline-adaptive-deletion-encoding-and-deletion-blob-retirement.md

## Deferral Context (Optional)

- Defer Reason: Task 000323 limits physical rows and splits prepared source ranges;
  general value-byte oversize handling remains an explicit non-goal.
- Findings: A count-bounded selection is retried unchanged after flushing a
  nonempty builder. If it still cannot fit an empty builder, checkpoint retains
  its typed unsupported-value failure and existing post-transition fatal policy.
- Direction Hint: Plan deterministic value-aware partitioning or admission
  before irreversible transition. Preserve prepared bitmaps, exact sidecars,
  final coverage/bindings, and the fixed inline-metadata capacity invariant.
  Coordinate unsupported RowID spans with backlog 000208.
