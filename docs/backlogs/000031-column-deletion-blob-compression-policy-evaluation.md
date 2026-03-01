# Backlog: Column Deletion Blob Compression Policy Evaluation

## Summary

Evaluate optional compression policy for offloaded deletion blob payloads after baseline correctness and performance are stable.

## Reference

1. Source task document: `docs/tasks/000038-column-block-index-offloaded-deletion-bitmap.md`.
2. Open question: optional blob compression policy.

## Scope Hint

- Measure compression ratio vs CPU/latency costs on representative bitmaps.
- Define when compression should be applied.
- Preserve format compatibility and decode simplicity.

## Acceptance Hint

Compression policy decision is documented with benchmark evidence and implementation plan if adopted.
