---
id: 000205
title: Optimize Transaction Row Value Ownership
status: proposal  # proposal | implemented | superseded
created: 2026-06-30
github_issue: 796
---

# Task: Optimize Transaction Row Value Ownership

## Summary

Optimize transaction hot-path row value ownership so update and upsert paths
borrow values while searching, comparing, and retrying, but keep owned values
available for terminal redo, undo, index, replacement-row, and insert effects.

The task should introduce internal update input/view types, remove the full-row
upsert conversion to `Vec<UpdateCol>`, remove retry-loop `update.clone()`
patterns, and clean up other avoidable clone hot spots found in the same value
ownership pass. It must preserve existing public write APIs and durable
redo/undo serialization shapes.

## Context

`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Source Backlogs:`
`- docs/backlogs/000141-optimize-transaction-row-value-ownership.md`

Task 000202 added unique-key MVCC upsert by deriving a lookup key from an
owned full row, attempting update, and falling back to insert. Its
implementation intentionally deferred broader row-value ownership cleanup to
backlog 000141.

The current ownership model has mixed outcomes:

- Insert paths are already close to optimal for owned caller rows. A hot insert
  writes the page from `&[Val]` and then moves the same owned `Vec<Val>` into
  `RowRedoKind::Insert`.
- Update paths receive owned `Vec<UpdateCol>` even while lookup, retry, and
  comparison only need borrowed values.
- Full-row upsert converts `&[Val]` into `Vec<UpdateCol>` via
  `full_row_update_cols`, even when the upsert becomes an insert or only a few
  columns changed.
- Both `MemTable::update_unique_mvcc` and
  `UserTableAccessor::update_unique_mvcc` clone update vectors in retry loops.
- `HotRowUpdater::update_inplace` already limits durable owned effects to
  changed columns, but its API forces owned values to be prepared too early.
- `UpdateCol` is both a public update input type and an owned durable/runtime
  payload used in redo, undo deltas, and unique-index branches. This task must
  not reinterpret it as only a borrowed input shape.

Storage docs emphasize that hot rows are updated in place when possible, moved
to a new hot RowID when they cannot fit, and cold LWC rows are immutable so
updates claim a cold delete marker and insert a hot replacement. Checkpoint and
recovery rely on owned redo payloads and table-centric replay boundaries, so
the optimization must stay inside runtime ownership and not alter durable
formats.

## Goals

- Add internal ownership/view types for transaction row updates:
  - `RowUpdateInput` owns update values across retry and terminal paths.
  - `RowUpdateView<'a>` borrows from `RowUpdateInput` for validation, lookup,
    comparison, and pre-terminal decisions.
- Keep public transaction and statement APIs source-compatible in this task.
- Keep owned `Vec<Val>` insert and upsert inputs where they naturally feed
  insert redo without cloning.
- Route sparse public updates through owned `RowUpdateInput::Sparse` and
  borrow from it for retry-loop attempts.
- Route full-row upsert through owned `RowUpdateInput::FullRow` and borrow from
  it while attempting update; move the row into `insert_mvcc` if the key is not
  found.
- Remove avoidable full-row `UpdateCol` construction from upsert.
- Remove update-vector clones from hot/cold update retry loops.
- Move or clone new values only when building terminal owned effects:
  `RowRedoKind::Update`, replacement hot rows, runtime unique-index branch
  payloads, and index/row undo payloads.
- Include a focused clone-audit pass over adjacent transaction row update
  helpers and fix small, directly related clone hot spots.
- Preserve MVCC correctness, rollback behavior, redo recovery behavior, and
  current write-conflict/duplicate-key semantics.

## Non-Goals

- Do not add public borrowed update APIs, `Cow` APIs, or generic public update
  input traits in this task.
- Do not include a struct-of-array update variant unless a current in-repo
  caller naturally provides separate index/value slices during implementation.
  Current design should stay with sparse and full-row shapes.
- Do not change redo, undo, or table-file serialization formats.
- Do not change `UpdateCol` as the owned durable/runtime update payload.
- Do not redesign unique-key upsert semantics from task 000202.
- Do not attempt broad `MemTable` and `UserTableAccessor` deduplication beyond
  clone-removal helpers needed for this task.
- Do not eliminate no-op row locks or change transaction conflict semantics for
  no-op updates. Avoiding clone/redo/replacement work after the lock is in
  scope; changing lock acquisition is not.
- Do not add benchmarks as an acceptance gate. Focused tests and standard
  validation are sufficient for this task; a benchmark follow-up can be opened
  if implementation reveals a useful stable measurement target.

## Plan

1. Add internal update ownership and borrowed-view types.
   - Prefer placing them next to existing row operation types in
     `doradb-storage/src/row/ops.rs`, unless the implementation finds a better
     table-local home with less visibility.
   - Shape:

```rust
pub(crate) enum RowUpdateInput {
    Sparse(Vec<UpdateCol>),
    FullRow(Vec<Val>),
}

pub(crate) enum RowUpdateView<'a> {
    Sparse(&'a [UpdateCol]),
    FullRow(&'a [Val]),
}

pub(crate) struct RowUpdateItem<'a> {
    pub(crate) idx: usize,
    pub(crate) val: &'a Val,
}
```

   - Add small methods only as needed:
     - `RowUpdateInput::as_view(&self) -> RowUpdateView<'_>`
     - `RowUpdateView::iter()` yielding `RowUpdateItem<'_>`
     - validation/debug-validation helpers for index ordering, bounds, and
       type checks
     - consuming helpers for terminal paths when moving owned values is useful
   - Keep the enum internal. Do not expose it as a public API type.

2. Define exact validation and iteration behavior.
   - `Sparse(Vec<UpdateCol>)`:
     - column indexes must be strictly ascending;
     - each index must be within the table column count;
     - each value must match the target column type.
   - `FullRow(Vec<Val>)`:
     - length must equal the table column count;
     - iteration yields column indexes `0..cols.len()`;
     - every value must match the target column type.
   - Use explicit `Result` validation where existing public or recovery-facing
     paths already return validation errors. Use `debug_assert!` for internal
     invariants that are already asserted today.

3. Update row-page update preparation to accept borrowed views.
   - Replace `RowWriteAccess::update_row(..., cols: &[UpdateCol], ...)` with a
     form that accepts `RowUpdateView<'_>` or a small trait/helper derived from
     it.
   - Replace `RowRead::var_len_for_update(&[UpdateCol])` and
     `RowPage::var_len_for_update(&[UpdateCol])` call paths used by MVCC update
     with borrowed-view iteration.
   - Preserve existing reserved no-transaction row-page APIs unless changing
     them is mechanically simpler and remains well covered.

4. Rework `HotRowUpdater::update_inplace`.
   - Change the method to borrow update values during the lock/compare phase.
   - Hold the owned `RowUpdateInput` outside retry loops and pass only
     `input.as_view()` to each attempt.
   - After the row is locked and the operation is committed to an in-place
     update, build changed-column terminal payloads:
     - `UndoCol` from old values and var offsets;
     - `UpdateCol` redo payload from changed new values;
     - `index_change_cols` from indexed old values.
   - For owned sparse input, move changed `Val`s into redo where practical.
     When borrowing is simpler than splitting the input, clone only changed
     values, never unchanged values.
   - For full-row input, clone or move only changed values into redo. If the
     row update is not found and the operation falls back to insert, preserve
     the original `Vec<Val>` for `insert_mvcc`.
   - Keep no-op update behavior compatible with today: row lock may still be
     acquired, but no redo, replacement row, or index update should be emitted
     when no values changed.

5. Rework hot move-update preparation.
   - Change `prepare_move_update` so it does not require a prebuilt
     `Vec<UpdateCol>` for full-row upsert.
   - Build the owned replacement row from the old row image plus changed
     values only.
   - Avoid cloning the entire old row solely to compute `undo_vals`.
     Retain changed old values in a delta structure while comparing, and build
     runtime branch undo values from that changed-column delta.
   - Keep owned `SelectKey` values for index undo and branch keys because those
     are retained beyond the borrowed view lifetime.
   - Consider converting `IndexBranch.undo_vals` from `Vec<UpdateCol>` to
     `Arc<[UpdateCol]>` if multiple unique branches currently clone the same
     changed-column delta. This is allowed only if it stays internal and does
     not affect redo serialization.

6. Rework `MemTable::update_unique_mvcc`.
   - Change the internal update helper to accept `RowUpdateInput` rather than
     `Vec<UpdateCol>`.
   - Wrap the existing public/test sparse update vector with
     `RowUpdateInput::Sparse(update)`.
   - Keep the owned input in the outer retry loop and pass borrowed views to
     row-page attempts.
   - Remove `update.clone()` in the retry loop.
   - Keep catalog-style LWC error behavior unchanged.

7. Rework `UserTableAccessor::update_unique_mvcc`.
   - Change the internal update helper to accept `RowUpdateInput`.
   - Keep public `Statement::table_update_unique_mvcc` accepting
     `Vec<UpdateCol>` and wrap it internally.
   - For hot rows, use the same borrowed-view hot updater path as `MemTable`.
   - For cold rows:
     - decode the old cold row as today;
     - claim the deletion-buffer marker as today;
     - build old index keys from the decoded old row as today;
     - apply the borrowed view to build the owned replacement hot row;
     - clone/move only changed new values where practical;
     - preserve cold delete redo, index masking, hot insert redo, and
       unique-branch visibility behavior.
   - Remove `update.clone()` before `build_cold_update_row`.

8. Rework full-row upsert in `MemTable` and `UserTableAccessor`.
   - Derive the unique key from `&cols` as today.
   - Build `RowUpdateInput::FullRow(cols)`.
   - Attempt update using borrowed `FullRow` view.
   - If update returns `NotFound`, recover the owned full row from
     `RowUpdateInput` and move it into `insert_mvcc`.
   - Delete `full_row_update_cols` if no remaining caller needs it.

9. Run a focused clone-audit pass over adjacent update helpers.
   - Inspect clone sites in `doradb-storage/src/table/hot.rs`,
     `doradb-storage/src/table/access.rs`,
     `doradb-storage/src/table/mem_table.rs`,
     `doradb-storage/src/table/mod.rs`,
     `doradb-storage/src/trx/row.rs`, and row operation helpers.
   - Fix clone sites that are directly part of update/upsert ownership and can
     be removed without broad refactoring.
   - Leave unrelated `Arc::clone`, test data clones, recovery/test fixture
     clones, and retained-index-key clones outside this task.

10. Keep implementation small and correctness-first.
    - Preserve statement rollback ordering: index effects roll back before row
      effects.
    - Preserve redo emission rules: insert redo owns full rows; update redo
      owns changed columns; cold update emits delete plus hot insert redo.
    - Preserve existing write-conflict, duplicate-key, and not-found behavior.

## Implementation Notes

## Impacts

- `doradb-storage/src/row/ops.rs`
  - Add internal update input/view types and iteration/validation helpers.
  - Keep `UpdateCol` as the owned durable/runtime payload.
- `doradb-storage/src/row/mod.rs`
  - Adjust row-page variable-length update estimation helpers used by MVCC
    update paths to consume borrowed update views.
- `doradb-storage/src/trx/row.rs`
  - Adjust row write preparation to accept borrowed update views.
- `doradb-storage/src/table/hot.rs`
  - Rework `HotRowUpdater::update_inplace` and `prepare_move_update` around
    `RowUpdateInput`/`RowUpdateView`.
  - Remove unnecessary update-vector and full-row clone patterns.
- `doradb-storage/src/table/mem_table.rs`
  - Wrap sparse update inputs, route full-row upsert through full-row input,
    and remove retry-loop `update.clone()`.
- `doradb-storage/src/table/access.rs`
  - Apply the same ownership model to user-table hot and cold update paths.
  - Preserve cold delete marker, replacement insert, and unique-branch
    visibility semantics.
- `doradb-storage/src/table/mod.rs`
  - Remove or replace `full_row_update_cols` if no longer needed.
- `doradb-storage/src/trx/undo/row.rs`
  - Optional: change runtime `IndexBranch.undo_vals` to shared internal storage
    only if it removes repeated clone of the same changed-column delta without
    affecting public or durable formats.

## Test Cases

- Sparse hot update changes one non-indexed column and preserves existing MVCC
  read-your-own-write and committed-read behavior.
- Sparse hot update changes indexed columns and updates unique and non-unique
  MemIndex entries correctly.
- Full-row upsert inserts by moving the owned row into existing insert redo
  path when the unique key is absent.
- Full-row upsert updates an existing hot row without constructing a full
  `Vec<UpdateCol>`.
- Full-row no-op upsert/update keeps row contents and indexes unchanged and
  emits no update redo for unchanged columns.
- Hot move update still works when a large value cannot fit in the original
  row page, including RowID movement and index remapping.
- Cold row update still claims the deletion marker, masks old index entries,
  inserts the hot replacement, and preserves old-snapshot visibility.
- Duplicate-key and write-conflict cases for hot and cold update/upsert keep
  existing errors.
- Rollback restores row, cold delete marker, and index state for sparse update,
  hot move update, full-row upsert update, and cold update.
- Existing redo recovery tests covering insert, update, delete, and
  update-by-unique-key continue to pass.

Validation command:

```bash
cargo nextest run -p doradb-storage
```

Run `cargo clippy -p doradb-storage --all-targets -- -D warnings` and
`cargo fmt` before review. If implementation changes storage backend or
backend-neutral I/O code unexpectedly, also run:

```bash
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

## Open Questions

- Should `IndexBranch.undo_vals` become shared internal storage such as
  `Arc<[UpdateCol]>` to avoid cloning the same changed-column delta across
  multiple unique branches? This should be decided during implementation based
  on measured simplicity and blast radius; it must not change redo format.
- Should a future public API accept borrowed sparse updates or `Cow` after the
  internal ownership model settles? This is intentionally deferred from this
  task.
