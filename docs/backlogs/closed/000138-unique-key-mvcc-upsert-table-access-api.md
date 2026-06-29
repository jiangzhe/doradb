# Backlog: Add unique-key MVCC upsert table access API

## Summary

Add a storage/table access API for unique-key MVCC upsert so callers do not manually spell the common lookup/delete/insert sequence for one logical row replacement.

## Reference

During task 000199, catalog silent table replay watermark upsert needs to find the existing watermark by primary key, optionally delete it, and insert the merged row. The same pattern is common enough at the storage layer that it should be represented by a table access API such as `upsert_unique_mvcc` instead of bespoke call-site sequencing.

## Deferred From (Optional)

docs/tasks/000199-catalog-silent-table-replay-watermarks.md

## Deferral Context (Optional)

- Defer Reason: Task 000199 is focused on catalog-backed silent replay watermarks. Adding a generic upsert access API changes broader table access contracts and deserves its own design/test pass instead of expanding the current checkpoint/recovery task.
- Findings: The silent watermark accessor currently needs at most three logical operations: find by unique key, delete existing row if the merged watermark advances, and insert the fieldwise-max row. Catalog replay currently expects updates to be represented as delete-by-unique-key plus insert rather than `RowRedoKind::Update`, so a future upsert API must make its redo shape explicit.
- Direction Hint: Prefer a generic storage-layer API over a watermark-specific helper. Start with unique-key MVCC upsert semantics, decide whether it is only an ergonomic wrapper around existing redo-compatible operations or a new lower-level operation, and keep catalog redo replay compatibility as a first-class requirement.

## Scope Hint

Design and implement a unique-key MVCC upsert API in the table access/statement layer. Apply it first to `catalog.table_replay_silent_watermarks` if its merge-and-replace semantics fit, and document any cases where callers should still use explicit find/delete/insert steps.

## Acceptance Hint

A future task should provide an `upsert_unique_mvcc`-style interface with clear semantics for absent and existing unique-key rows, preserve MVCC/redo/rollback invariants, and include tests for insert, replacement, no-op/covered update if supported, duplicate-key behavior, rollback, and catalog redo replay compatibility.

## Notes (Optional)

The name `upsert_unique_mvcc` is a direction hint, not a fixed API name. Future planning should inspect existing statement/table access naming before finalizing the interface.

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

## Close Reason

- Type: implemented
- Detail: Storage-layer unique-key MVCC upsert API implemented via docs/tasks/000202-unique-key-mvcc-upsert-table-access-api.md; catalog silent watermark adoption deferred to docs/backlogs/000139-catalog-silent-watermark-upsert-adoption.md.
- Closed By: backlog close
- Reference: User decision
- Closed At: 2026-06-29
