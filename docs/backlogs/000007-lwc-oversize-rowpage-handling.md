# Backlog: LWC Oversize RowPage Handling

## Summary

Define and implement deterministic behavior when a multirow source selection
cannot fit into one LWC page despite every row being individually admissible.
Task 000338 implements mandatory single-row admission; aggregate value-byte
overflow remains open here.

## Reference

1. Source task document: `docs/tasks/000004-gather-row-pages-to-lwc.md`.
2. Open question: handling unlikely but possible oversized single-row-page cases.
3. Task 000323 implements row-count splitting while preserving this value-byte limit follow-up.
4. Task 000338 implements single-row RowPage/LWC admission:
   `docs/tasks/000338-limit-single-rows-to-rowpage-and-lwc-block-capacity.md`.

## Scope Hint

- Define value-byte partitioning or pre-transition admission for oversized
  multirow selections while retaining task 000338's single-row limits.
- Ensure checkpoint flow handles the fallback without data loss.
- Add targeted tests for oversized inputs.

## Acceptance Hint

Selections of individually admissible rows that exceed one LWC block are
partitioned or rejected before irreversible transition, with preserved values,
RowIDs, visibility, and coverage bindings. Checkpoint and recovery tests cover
poor compression and large aggregate variable content.

## Deferred From (Optional)

- docs/tasks/000323-inline-adaptive-deletion-encoding-and-deletion-blob-retirement.md
- docs/tasks/000338-limit-single-rows-to-rowpage-and-lwc-block-capacity.md

## Deferral Context (Optional)

- Defer Reason: Task 000323 limits physical rows and splits prepared source ranges;
  task 000338 adds mandatory complete-row admission. General multirow value-byte
  splitting remains outside both tasks' approved scope, so this backlog stays open.
- Findings: A count-bounded selection is retried unchanged after flushing a
  nonempty builder. If it still cannot fit an empty builder, checkpoint retains
  its typed unsupported-value failure and existing post-transition fatal policy.
  Singleton representability does not bound the encoded size of several rows;
  task 000338's boundary persistence tests intentionally use selections that fit.
- Direction Hint: Plan deterministic value-aware partitioning or admission
  before irreversible transition. Preserve prepared bitmaps, exact sidecars,
  final coverage/bindings, and the fixed inline-metadata capacity invariant.
  Coordinate unsupported RowID spans with backlog 000208.
