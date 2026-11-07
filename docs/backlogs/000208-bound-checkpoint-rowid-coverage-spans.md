# Backlog: Bound checkpoint RowID coverage spans before irreversible transition

## Summary

Define admission and splitting for cold block coverage wider than a nonzero u32 span, including sparse holes and final invisible tails, before checkpoint crosses its irreversible transition.

## Reference

docs/tasks/000323-inline-adaptive-deletion-encoding-and-deletion-blob-retirement.md; docs/tasks/000339-stream-partial-rowpages-into-lwc-blocks.md; docs/data-checkpoint.md; doradb-storage/src/table/persistence.rs; doradb-storage/src/index/identity_set.rs

## Deferred From (Optional)

docs/tasks/000323-inline-adaptive-deletion-encoding-and-deletion-blob-retirement.md
docs/tasks/000339-stream-partial-rowpages-into-lwc-blocks.md

## Deferral Context (Optional)

- Defer Reason: Tasks 000323 and 000339 explicitly exclude general unsupported RowID span handling and preserve the existing fatal policy after transition.
- Findings: The 15360 physical-row cap bounds both inline bodies but does not bound coverage width. Identity planning still returns ColumnBlockEntryCapacityExceeded for zero or greater-than-u32 coverage, including final tails after the builder has been accepted.
- Findings from task 000339: Exact byte-prefix splitting fixes aggregate value overflow,
  but physical-row and byte caps still do not bound RowID coverage. Full builders
  retain invisible tails until the final pivot fixes their binding.
- Direction Hint: Plan coverage-aware partitioning and early admission using prepared
  RowIDs; finalize each binding only after its coverage is fixed. Build on task
  000339's completed byte splitting without weakening post-transition ownership.

## Scope Hint

User and catalog coverage finalization, prepared source selection, and pre-transition handling of unsupported coverage spans. Keep the fixed physical row cap and inline ordinal deletion contract.

## Acceptance Hint

Wide sparse RowID gaps and trailing invisible coverage either repartition into supported entries with correct bindings or reject before irreversible transition; ordinary checkpoint and recovery remain exact.

## Notes (Optional)


