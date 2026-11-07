# Backlog: Row-to-LWC Streaming Serialization Memory Optimization

## Summary

Reduce remaining row-to-LWC staging memory with streaming or chunked construction,
building on task 000339's exact prefix admission and direct final-buffer writing.

## Reference

1. Source task document: `docs/tasks/000001-row-to-lwc-conversion.md`.
2. Implemented foundation: `docs/tasks/000339-stream-partial-rowpages-into-lwc-blocks.md`.

## Scope Hint

- Profile retained column staging, concurrent encode inputs, and shared-storage buffers.
- Evaluate chunked staging beyond the existing direct destination-buffer writer.
- Preserve output format and correctness guarantees.

## Acceptance Hint

Conversion memory peak is reduced relative to task 000339 with equivalent output
correctness and large-row-page regression coverage. Report component and whole-engine
peaks separately, including concurrent owners and allocator retention.

## Deferred From (Optional)

docs/tasks/000339-stream-partial-rowpages-into-lwc-blocks.md

## Deferral Context (Optional)

- Defer Reason: Task 000339 removes intermediate encoded buffers but retains owned
  accepted columns for worker handoff; broader chunked staging remains separate work.
- Findings: Recorded component peaks fell across all eight fixtures, while the
  million-row checkpoint's whole-process peak RSS rose about 4%. Source values,
  concurrent encode inputs, and shared-storage output remain distinct owners.
- Direction Hint: Measure the final implementation before designing further changes.
  Preserve frozen encoding plans, guard-free waits, bounded worker occupancy,
  rejection immutability, and exact identity/sidecar membership.

