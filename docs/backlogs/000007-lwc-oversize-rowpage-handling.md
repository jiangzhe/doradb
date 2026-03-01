# Backlog: LWC Oversize RowPage Handling

## Summary

Define and implement deterministic behavior when a single source row-page payload cannot fit into one LWC page due to large varbyte content or poor compression.

## Reference

1. Source task document: `docs/tasks/000004-gather-row-pages-to-lwc.md`.
2. Open question: handling unlikely but possible oversized single-row-page cases.

## Scope Hint

- Define fallback strategy (split, spill, reject, or alternate encoding).
- Ensure checkpoint flow handles the fallback without data loss.
- Add targeted tests for oversized inputs.

## Acceptance Hint

Oversized source cases are handled explicitly and tested; checkpoint conversion no longer relies on undocumented assumptions.

