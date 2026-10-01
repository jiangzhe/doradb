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

## Close Reason

- Type: replaced
- Detail: Replaced by backlog 000206 at the user's request. The original optional deletion-blob compression investigation is superseded by a separate design for inline adaptive ordinal deletion encoding, joint identity/deletion capacity guarantees, LWC splitting, and deletion-blob retirement. Original backlog content is preserved.
- Closed By: backlog close
- Reference: [Backlog 000206](../000206-inline-adaptive-deletion-encoding-and-deletion-blob-retirement.md); user decision on 2026-09-30.
- Closed At: 2026-09-30
