# Backlog: Support unique-driver key changes in index-driven MVCC mutation

## Summary

Extend index-driven MVCC mutation so unique driver updates may change encoded keys without shadowing unread candidates or reprocessing replacement rows.

## Reference

Task 000265 established the weak monotonic dual-tree mutation traversal and currently rejects unique-driver key changes.

## Deferred From (Optional)

docs/tasks/000265-index-driven-mvcc-mutation-api.md

## Deferral Context (Optional)

- Defer Reason: The current bounded weak-monotonic scanner cannot prevent a newly written MemIndex key from shadowing an unread DiskTree owner, so a safe solution requires separate design work.
- Findings: Unique MemIndex and DiskTree entries merge by logical key, without RowID in the exact identity. Same-statement tags skip replacement rows only after candidate emission and cannot recover a shadowed unread candidate.
- Direction Hint: Evaluate bounded materialization with spill, a statement work file, or equivalent candidate stabilization. Preserve callback non-retry, bounded resources, and current-read ownership semantics; do not simply permit key changes in the existing scanner.

## Scope Hint

Choose and implement candidate stabilization, such as bounded materialization with spill or statement work storage, while preserving bounded resources and rollback.

## Acceptance Hint

Unique-driver key-changing updates work across mixed MemIndex and DiskTree ranges, cannot shadow unread candidates, invoke each callback at most once, and pass concurrency and rollback coverage.

## Notes (Optional)


