---
id: 000233
title: Unify full-table MVCC mutation API
status: proposal
created: 2026-07-22
github_issue: 876
---

# Task: Unify full-table MVCC mutation API

## Summary

Replace the update-only `Statement::table_update_mvcc` API with one sequential
`Statement::table_mutate_mvcc` API whose callback can skip, delete, or sparsely
update each latest modifiable row. Expose a public `RowMutation` decision enum
and return a `TableMutationOutcome` with independent delete and update counts.

Retain the existing transaction-duration exclusive table-data lock, original
hot-page worklist, lazy storage-independent row access, statement rollback,
immediate index ownership, and recovery protocols. Extend the shared traversal
with known-row deletion for cold and hot storage without adding a separate
full-table delete facade or preserving an update-only compatibility wrapper.

## Context

`Statement::table_update_mvcc` currently supplies the core mechanism needed for
callback-driven full-table mutation. It acquires transaction-lifetime
`TableMetadata(S)` and `TableData(X)`, captures one coherent cold root and
original hot-page worklist, exposes latest modifiable values through `LazyRow`,
and excludes replacement rows inserted by the operation from the same scan.
Its callback can only return `None` to skip a row or `Some(Vec<UpdateCol>)` to
update it.

The storage engine already has transactional delete mechanics, but the public
path is `Statement::table_delete_unique_mvcc` and is coupled to a unique-key
lookup. Cold deletion installs a deletion-buffer marker, row undo, row redo,
and secondary-index masks. Hot deletion locks the known row through its undo
head, sets the row-page delete bit, rewrites the undo kind, emits row redo, and
masks every secondary-index entry. Those physical protocols should be reused
by the full-table mutation engine without performing a per-row unique lookup.

The public API is intentionally still under active development. This task
makes a deliberate breaking cleanup: one public mutation enum should expose the
same decision model used by the internal traversal, and callers do not require
`table_update_mvcc` compatibility. The selected outcome contract reports
delete and update decisions independently rather than collapsing them into one
ambiguous integer.

Relevant design and process references:

- `docs/architecture.md`
- `docs/transaction-system.md`
- `docs/index-design.md`
- `docs/checkpoint-and-recovery.md`
- `docs/table-file.md`
- `docs/process/coding-guidance.md`
- `docs/process/unit-test.md`
- `docs/tasks/000224-support-full-table-mvcc-update.md`

Issue Labels:

- type:task
- priority:medium
- codex

## Goals

1. Replace `Statement::table_update_mvcc` with one public
   `Statement::table_mutate_mvcc` operation that accepts skip, delete, and
   sparse-update decisions for callback-visible rows.
2. Add a public exhaustive `RowMutation` enum with `Skip`, `Delete`, and
   `Update(Vec<UpdateCol>)` variants.
3. Return a public `TableMutationOutcome` containing independent
   `delete_count` and `update_count` fields.
4. Preserve the existing latest-modification-read contract for both cold and
   hot rows through the storage-independent `LazyRow` accessor.
5. Invoke the callback at most once for each original eligible row and exclude
   any replacement row inserted by an update action from the current scan.
6. Delete selected cold and hot rows through the existing MVCC row, index,
   undo, redo, rollback, purge, checkpoint, and recovery protocols.
7. Preserve transaction-duration `TableMetadata(S)` and `TableData(X)`
   admission and its exclusion from freeze/checkpoint page transition.
8. Roll back every earlier update and delete action when any later callback,
   validation, row mutation, index mutation, or storage operation fails.
9. Retain the existing direct known-row mutation and lazy decoding performance
   properties without introducing per-row unique-key lookups.

## Non-Goals

- Keeping `Statement::table_update_mvcc` as a compatibility or deprecated
  wrapper.
- Adding a separate `Statement::table_delete_mvcc` API.
- Changing point insert, update, upsert, or delete APIs.
- Adding insert, full-row replace, early-termination, or custom variants to
  `RowMutation`.
- Marking `RowMutation` as `#[non_exhaustive]` in anticipation of unspecified
  future actions.
- Async callbacks, vectorized callbacks, parallel mutation, or a general DML
  execution pipeline.
- Running the operation under `TableData(IX)` or allowing concurrent table
  writers and checkpoint transition during the scan.
- Final-state or batch uniqueness. Unique keys remain claimed immediately in
  physical scan order.
- Defining a stable user-visible callback order beyond the documented
  original-row, at-most-once contract.
- A public or general mutation-by-`RowID` primitive.
- New undo, redo, table-file, LWC, deletion-buffer, index, checkpoint, purge, or
  recovery formats.
- Undoing external side effects performed by callback code.

## Plan

### 1. Public mutation and outcome types

Add the following public types beside the existing public row-operation types
in `doradb-storage/src/row/ops.rs` and re-export them from
`doradb-storage/src/lib.rs`:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowMutation {
    Skip,
    Delete,
    Update(Vec<UpdateCol>),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TableMutationOutcome {
    pub delete_count: usize,
    pub update_count: usize,
}
```

Add descriptive public documentation to both types, every field, and every
variant. Keep `RowMutation` exhaustive. Do not add a total or skip counter;
callers can sum the two action counts, while rows omitted by current-read
eligibility are not callback `Skip` decisions.

Define the action-count contract precisely:

- `Skip` increments neither field and leaves the row unchanged;
- `Delete` increments `delete_count` once;
- `Update(update)` increments `update_count` once;
- `Update(Vec::new())` still increments `update_count` but creates no physical
  row, index, undo, or redo work; and
- a failed operation returns no outcome and rolls back all statement effects.

### 2. Replace the public Statement facade

Remove `Statement::table_update_mvcc` and add:

```rust
pub async fn table_mutate_mvcc<F>(
    &mut self,
    table_id: TableID,
    mutate_row: F,
) -> Result<TableMutationOutcome>
where
    F: for<'row> FnMut(&mut LazyRow<'row>) -> Result<RowMutation>;
```

Use `table_mutate_mvcc` as the public operation name in documentation and error
attachments. Preserve the existing admission order:

1. resolve and pin the live user table;
2. acquire transaction-lifetime `TableMetadata(S)`;
3. revalidate foreground table liveness;
4. acquire transaction-lifetime `TableData(X)`;
5. revalidate liveness again;
6. capture the table layout and enter the accessor traversal.

Retain the transaction-owned `X` grant after statement success or rollback
until transaction commit or rollback. An incompatible `IX`-to-`X` conversion
must return `LockUpgradeWouldBlock` before the callback is invoked. Ordinary
metadata-only MVCC readers remain admitted while the `X` grant is held.

Pass the existing `disable_dml_validation` policy into the accessor so only
`RowMutation::Update` payloads use sparse-update validation. Public
`LazyRow::val` bounds and integrity checks remain unconditional.

### 3. Generalize the original-row traversal

Rename and generalize the current internal full-table update entry point in
`doradb-storage/src/table/access.rs` to consume `RowMutation` and return
`TableMutationOutcome`.

Preserve these scan properties:

- acquire one table-root snapshot after `TableData(X)` is granted;
- capture the original hot row-page descriptors and exclusive block-index
  upper bound before any callback;
- scan the persisted region from the captured root before the captured hot
  page descriptors;
- retain no block-index leaf latch across callbacks or mutation;
- use current-read ownership checks before exposing either cold or hot rows;
- invoke the callback once for each original latest modifiable row unless an
  earlier error aborts the scan;
- keep replacement insertion tracking for update actions so newly inserted
  hot rows are excluded from future original-page scans; and
- reuse the `LazyRow` allocation and initialized-value clearing strategy.

Do not promise the physical cold/hot scan order as a stable caller-facing API.
Nevertheless, apply selected mutations in their current physical row order so
immediate uniqueness and deterministic statement behavior remain aligned with
the existing update implementation.

### 4. Dispatch cold-row actions in one ordered batch

Replace the update-only cold pending item with one private ordered enum, for
example:

```rust
enum PendingColdMutation {
    Delete {
        row_id: RowID,
        index_keys: Vec<SelectKey>,
    },
    Update {
        row_id: RowID,
        old_row: Vec<Val>,
        update: Vec<UpdateCol>,
    },
}
```

For each eligible cold row:

1. construct `LazyRow` over the already loaded and validated LWC block;
2. call the public callback;
3. reuse the value buffer immediately for `Skip`;
4. validate an `Update` payload when validation is enabled;
5. count and discard an empty `Update` without staging physical work;
6. materialize the complete old row only for a non-empty cold update; and
7. for `Delete`, materialize only values required by active secondary indexes,
   reusing callback-loaded `LazyRow` values and decoding other indexed columns
   lazily.

Keep update and delete actions in one pending vector in callback row order.
Release the readonly LWC block guard before performing asynchronous row and
index mutations, then apply the pending actions sequentially.

Add a known-cold-row delete helper that uses the existing effect ordering:

1. claim the cold deletion-buffer marker;
2. push `OwnedRowUndo` with `RowUndoKind::Delete`;
3. insert physical `RowRedoKind::Delete` with `INVALID_PAGE_ID`;
4. mask every old secondary-index key and record index undo.

Map a marker ownership conflict to `WriteConflict`. A cold row that changes
between successful latest-read eligibility and known deletion is also a
statement failure rather than a second callback or silent skip. Allow a narrow
shared effect-installation helper with point deletion when it reduces
duplication without weakening the point path's key recheck and `NotFound`
semantics.

### 5. Add known hot-row delete dispatch

For each eligible hot row, invoke the callback while one latest-image read
guard is retained, then drop all `LazyRow` and read borrows before mutation.

- `Skip` clears and reuses the lazy buffer.
- `Update` uses the existing direct `update_known_hot_row` path and reports any
  replacement insertion to the scan-boundary tracker.
- `Delete` uses a new known-row hot delete path, then masks all current
  secondary-index keys through `defer_delete_indexes`.

Refactor `HotRowMutator::delete` around a private common core, parallel to the
existing keyed and known-row update entry points:

- keyed point delete supplies the unique lookup key and optional
  `DeleteByPrimaryKey` redo behavior;
- full-table known delete supplies no lookup key and always emits physical
  `RowRedoKind::Delete`;
- both paths install the same undo-head lock, set the delete bit, rewrite the
  row undo kind, mark the page dirty through the existing row access boundary,
  and retain the page guard for index masking.

Under the full-table `X` grant, observing `RowPageState::Transition` after scan
admission is an internal invariant violation because checkpoint transition
holds conflicting `TableData(IS)`. A known row that becomes absent after its
callback is a statement failure; do not invoke the callback again.

### 6. Preserve indexes, effects, uniqueness, and recovery

Keep action execution sequential and enforce unique constraints immediately.
An update that claims a key still owned by a row which would be deleted later
must fail with the existing duplicate-key or conflict error and roll back the
whole statement. Do not plan or validate the callback's proposed final table
state.

Maintain statement effect ordering so every visible row change has rollback
ownership before a later fallible index operation. Statement failure continues
to unwind index effects before row/deletion effects and discard statement redo.
Successful effects merge into the transaction as they do today.

Reuse existing redo kinds:

- hot in-place update uses `Update`;
- hot move update uses delete plus insert;
- cold update uses cold delete plus hot insert; and
- cold or hot delete uses `Delete`.

No new recovery branch, persisted action enum, or table-file metadata is
allowed. Restart reconstructs committed hot deletes from row redo and committed
cold deletes from deletion redo relative to the existing replay boundaries.

### 7. Documentation, call sites, and performance

Broaden `LazyRow` documentation from a full-table update callback to a
full-table mutation callback. Update `docs/transaction-system.md` and
`docs/checkpoint-and-recovery.md` to describe `TableData(X)` as protecting
sequential full-table mutation, including both update and delete actions.

Migrate every repository call site and test from `table_update_mvcc` to
`table_mutate_mvcc` with `RowMutation::Skip` or `RowMutation::Update`.
Historical implemented task documents remain unchanged.

Retain the existing straightforward performance properties:

- one validated load per cold LWC block;
- lazy column decoding and reusable value buffers;
- index-column-only materialization for cold delete;
- one original hot-page descriptor snapshot;
- sparse insertion-boundary tracking only for update replacements;
- direct known hot-row mutation without unique lookup; and
- changed-index-only maintenance for updates.

Do not add a benchmark threshold or speculative general mutation framework.

## Implementation Notes

## Impacts

- `doradb-storage/src/row/ops.rs`
  - public `RowMutation` and `TableMutationOutcome` definitions and docs.
- `doradb-storage/src/lib.rs`
  - crate-root exports for both public types.
- `doradb-storage/src/trx/stmt.rs`
  - breaking replacement of `table_update_mvcc` with
    `table_mutate_mvcc`, including lock admission, operation context, callback,
    and outcome contract.
- `doradb-storage/src/table/access.rs`
  - generalized cold/hot original-row traversal, outcome accounting, ordered
    cold action staging, known cold delete, lazy index-key materialization, and
    mixed action dispatch.
- `doradb-storage/src/table/hot.rs`
  - common keyed/known hot delete core and physical delete redo selection.
- Inline tests in `doradb-storage/src/table/access.rs` and supporting table,
  transaction, checkpoint, rollback, recovery, or index test modules where
  their existing helpers provide the clearest production-path verification.
- `docs/transaction-system.md`
  - sequential full-table mutation callback and lock-duration semantics.
- `docs/checkpoint-and-recovery.md`
  - freeze/checkpoint exclusion against full-table mutation `TableData(X)`.

No persistent data format, redo serialization, recovery boundary, or public
point-DML result type is expected to change.

## Test Cases

Use inline production-path tests and deterministic synchronization for
concurrency cases. Do not use sleeps as correctness synchronization.

1. **Public actions and outcome**
   - mutate an empty table and verify `TableMutationOutcome::default()`;
   - return `Skip` for every row and verify callbacks occur while both counts
     remain zero;
   - mix `Skip`, `Delete`, and `Update` on hot rows and verify independent exact
     counts and final values;
   - return `Update(Vec::new())` and verify `update_count` increments without
     row, index, undo, or redo effects;
   - access columns out of order and repeatedly through `LazyRow`.
2. **Validation and callback failure**
   - return invalid, unordered, duplicate, out-of-range, and type-mismatched
     update columns after earlier update/delete actions and verify full statement
     rollback;
   - request an invalid `LazyRow` column after earlier mixed actions and verify
     `InvalidDmlInput` plus rollback;
   - return an explicit callback error after earlier cold and hot mutations and
     verify the original error is preserved;
   - verify the existing DML validation opt-out affects update payload validation
     but not public lazy-column bounds.
3. **Latest-row semantics**
   - from an older snapshot, observe and mutate the latest committed hot value;
   - observe the latest committed cold replacement and skip the consumed old
     cold image;
   - allow this transaction's own active row state and reject incompatible row
     ownership according to existing current-read rules;
   - verify a callback is never invoked for persisted, committed-deleted, or
     already own-consumed cold images.
4. **Mixed storage and scan boundaries**
   - mix update and delete across cold and hot regions in one invocation;
   - verify cold and hot update replacements are not revisited or counted twice;
   - force replacements into a future original page and verify the sparse
     first-insert boundary excludes them;
   - delete rows before and after replacement-producing updates and verify every
     original eligible row receives at most one callback.
5. **Physical delete and index behavior**
   - delete active and frozen hot rows and persisted cold rows;
   - cover tables with no secondary index, unique indexes, and non-unique
     indexes;
   - verify deleted keys are immediately hidden, rollback restores them, and
     later GC/checkpoint uses existing cleanup paths;
   - combine deletes with hot in-place updates, hot move updates, and cold
     delete-plus-insert updates.
6. **Uniqueness and statement rollback**
   - trigger an early, middle, and late duplicate after mixed successful actions
     and verify every row plus every index returns to pre-statement state;
   - verify an update cannot claim a unique key whose current owner is scheduled
     for deletion later in the physical scan;
   - verify a delete followed by an update may claim the released key when that
     is the actual action order;
   - explicitly roll back a transaction after successful mixed mutation and
     verify rows, cold deletion markers, and indexes.
7. **Locking and lifecycle**
   - verify the operation's `TableData(X)` blocks freeze/checkpoint until
     transaction completion, including after statement rollback;
   - verify ordinary metadata-only table/index MVCC readers remain admitted;
   - verify failed `IX`-to-`X` conversion invokes zero callbacks and performs no
     mutation;
   - verify table drop, poison, and shutdown propagate through existing
     foreground admission rules.
8. **Commit and recovery**
   - commit mixed cold delete, hot delete, in-place update, and move update,
     restart storage, and verify rows and every index through public reads;
   - verify checkpointed cold deletions and post-checkpoint deletion redo retain
     their existing replay-boundary behavior;
   - retain all existing full-table update regression coverage after migrating
     it to `table_mutate_mvcc`.

Run focused storage tests during development, then validate with:

```bash
cargo build --workspace
cargo nextest run --workspace
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

## Open Questions

There are no unresolved implementation choices for this task.

Any future mutation action, row-concurrent full-table execution model, async
callback, early termination contract, or final-state uniqueness planner
requires separate evidence and task/RFC design. Do not widen this task
speculatively.
