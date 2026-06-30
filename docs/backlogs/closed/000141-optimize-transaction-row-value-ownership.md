# Backlog: Optimize transaction row-value ownership and borrowed update views

## Summary

Design and implement a transaction hot-path value ownership model that keeps owned rows where they feed redo directly, but uses borrowed row/update views for lookup, comparison, retry loops, and sparse/full-row update execution so transaction processing avoids cloning unchanged or non-terminal values.

## Reference

Deferred from task 000202 after investigating unnecessary clones in upsert_unique_mvcc. The current upsert update attempt builds full_row_update_cols from the input row and update_unique_mvcc clones the update vector before HotRowUpdater::update_inplace. Further review showed the issue is broader than one method: UpdateCol currently serves both as caller input and owned durable redo/index-branch payload, while insert benefits from owned Vec<Val> because the same values can be moved into RowRedoKind::Insert.

## Deferred From (Optional)

docs/tasks/000202-unique-key-mvcc-upsert-table-access-api.md

## Deferral Context (Optional)

- Defer Reason: Task 000202 is scoped to unique-key MVCC upsert correctness and narrow hot-row helper extraction. The value ownership cleanup affects public transaction APIs, internal row update shapes, redo/undo payload construction, and hot-path performance across storage, so it should be planned separately.
- Findings: Owned Val passing is beneficial for insert paths because insert_row_to_page writes page data by reference and then moves the same Vec<Val> into RowRedoKind::Insert. Borrowed values are beneficial for lookup, comparison, retry loops, no-op updates, and full-row upsert update attempts. UpdateCol currently mixes input-shape and durable-payload responsibilities, causing full-row upsert to clone all values into Vec<UpdateCol> and retry loops to clone update vectors even though only changed columns need owned undo/redo/index payloads.
- Direction Hint: Prefer adding an internal RowUpdateSource or similar borrowed abstraction with sparse UpdateCol, struct-of-arrays, and full-row variants. Keep UpdateCol as the owned durable redo/index-branch payload unless a later serialization design changes it deliberately. Public APIs may offer both owned and borrowed forms, but owned insert/upsert should remain the fast path when callers naturally own rows.

## Scope Hint

Evaluate transaction-facing row value APIs and internal table update helpers across Statement, UserTableAccessor, MemTable, HotRowUpdater, row-page update preparation, redo, row undo, and index undo. Keep owned insert/upsert inputs where they avoid redo copies, add borrowed update/key/full-row views where they avoid lookup and retry-loop clones, and avoid changing durable redo/undo serialization unless a separate design proves it worthwhile.

## Acceptance Hint

A future task introduces a clear borrowed row/update-view abstraction, routes hot update/upsert lookup and retry paths through borrowed values, removes avoidable full-row UpdateCol construction and update.clone calls, preserves owned redo/undo/index payload semantics, adds no-op and changed-column upsert/update tests, and validates with cargo fmt, clippy, nextest, and style audit.

## Notes (Optional)

- Investigation context:
  - Owned row input is not inherently bad. For insert, owned `Vec<Val>` is the efficient shape because the row page can be written from borrowed values and the same vector can then move into `RowRedoKind::Insert` without cloning.
  - Borrowed values are most valuable before terminal effects are known: unique lookup, key validation, row comparison, retry loops, no-op updates, and full-row upsert attempts that may either update or fall back to insert.
  - The full-row upsert update path currently pays avoidable costs because it converts `&[Val]` into `Vec<UpdateCol>` via `full_row_update_cols`, then clones that vector before `HotRowUpdater::update_inplace`. This happens even when no column changes.
  - The hot in-place update effect only needs owned values for changed columns: old before-images for `RowUndoKind::Update`, new values for `RowRedoKind::Update`, and indexed old/new values for index maintenance.
  - `UpdateCol` currently carries both caller input semantics and durable payload semantics. It is exported as an update API type, serialized in redo updates, and reused in runtime unique-index branch undo values. Treating it only as an input shape would accidentally broaden the change into recovery and undo format work.
  - A struct-of-arrays shape can help sparse update input because indexes can be read without cloning values, but it should be introduced as a borrowed update source/view first. Durable redo/undo payloads can remain array-of-structs until a separate serialization/storage-layout task justifies changing them.
  - Delete and lookup should continue to borrow keys for search. Owned `SelectKey` is still needed when the key is stored in redo, index undo, index purge state, or runtime unique-index branches.
- Suggested future shape:
  - Add an internal borrowed `RowUpdateSource`/`RowUpdateView` with variants for sparse `&[UpdateCol]`, sparse struct-of-arrays `(&[usize], &[Val])`, and full-row `&[Val]`.
  - Let `HotRowUpdater` and cold-row replacement helpers iterate `(column_idx, &Val)` from this view.
  - Keep public owned insert/upsert APIs, and optionally add borrowed update/upsert variants once the internal view has settled.
  - Allocate owned `Val`s only when building terminal row/index effects or owned replacement rows.

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
- Detail: Implemented by task 000205.
- Closed By: backlog close
- Reference: docs/tasks/000205-optimize-transaction-row-value-ownership.md
- Closed At: 2026-06-30
