---
id: 000234
title: Variant Page Identity for Row Redo
status: implemented
created: 2026-07-23
github_issue: 878
---

# Task: Variant Page Identity for Row Redo

## Summary

Move row-page identity from the outer `RowRedo` record into the
`RowRedoKind` variants that can produce and consume it. Require a `PageID` for
physical insert and update redo, use `Option<PageID>` for physical delete redo,
and keep primary-key catalog redo page-independent.

Cold-row deletes must encode the absence of a row page as `Delete(None)`
instead of storing `INVALID_PAGE_ID`. This is an intentional redo payload
format change: advance the redo file format version and do not retain a
compatibility decoder for existing redo files.

## Context

`RowRedo` currently stores `page_id`, `row_id`, and `kind` for every operation.
That shape does not match the recovery requirements of its variants:

- user-table insert and update replay require a concrete row-page id;
- hot user-table delete replay requires a page id, but cold delete replay only
  rebuilds a deletion-buffer marker and has no row page;
- `DeleteByPrimaryKey` and `UpdateByPrimaryKey` are logical catalog operations
  whose recovery and checkpoint consumers locate rows by key and do not use a
  page id.

Cold delete producers in `table/access.rs` currently fill the mandatory field
with `INVALID_PAGE_ID`. Recovery happens to ignore that sentinel when the
replayed `row_id` is below the table's current pivot, but the record itself
cannot express that page identity is unavailable.

The delete option cannot be interpreted as the recovery destination. A delete
logged while its row was hot can be replayed after a later checkpoint has made
that row cold. Recovery must therefore choose the hot or cold path from the
current recovered pivot: the cold branch accepts both `Some(page_id)` and
`None`, while the hot branch requires `Some(page_id)`.

The current redo file format version is 4. Backward compatibility is a low
priority for the project, and this task deliberately rejects older redo files
instead of adding migration or dual decoding.

Relevant design and process references:

- `docs/architecture.md`
- `docs/checkpoint-and-recovery.md`
- `docs/redo-log.md`
- `docs/process/coding-guidance.md`
- `docs/process/unit-test.md`

Issue Labels:

- type:task
- priority:medium
- codex

## Goals

1. Remove the outer `page_id` field from `RowRedo` while retaining `row_id` and
   `kind`.
2. Make page availability part of the operation type: mandatory for physical
   insert/update, optional for physical delete, and absent from keyed catalog
   variants.
3. Emit `Delete(None)` for cold user-table deletes and eliminate redo-specific
   use of `INVALID_PAGE_ID`.
4. Preserve recovery correctness when a delete logged with a page id is cold
   according to the recovered table pivot.
5. Reject a page-less delete that must replay against a hot row as malformed
   redo.
6. Preserve existing `TableDML` folding semantics while moving page identity
   into variants.
7. Update redo serialization, minimum-size hints, file format version,
   documentation, producers, consumers, fixtures, and tests together.

## Non-Goals

- Splitting user-table physical redo and catalog logical redo into separate
  enum hierarchies or DML containers.
- Removing the repeated `row_id` from `RowRedo` and its containing
  `BTreeMap<RowID, RowRedo>`.
- Giving catalog insert its own page-free variant; it continues to share the
  physical insert shape even though catalog replay ignores the page id.
- Changing redo operation codes 1 through 5.
- Changing checkpoint boundaries, replay cutoffs, transaction semantics, or
  MVCC behavior.
- Removing `INVALID_PAGE_ID` from unrelated uses such as the B-tree value
  representation.
- Reading, migrating, or otherwise supporting redo file format version 4.
- Redesigning the generic `Option<T>` serialization format.

## Plan

1. Change the row redo model in `doradb-storage/src/log/redo.rs` to the
   following decision-complete shape:

   ```rust
   pub(crate) struct RowRedo {
       pub(crate) row_id: RowID,
       pub(crate) kind: RowRedoKind,
   }

   pub(crate) enum RowRedoKind {
       Insert(PageID, Vec<Val>),
       Delete(Option<PageID>),
       Update(PageID, Vec<UpdateCol>),
       DeleteByPrimaryKey(SelectKey),
       UpdateByPrimaryKey(SelectKey, Vec<UpdateCol>),
   }
   ```

   Keep tuple variants to limit taxonomy and call-site churn. Do not add a
   general page-id accessor because not every operation has physical page
   identity.

2. Change `RowRedo` serialization from `page_id + row_id + kind` to
   `row_id + kind`. Encode variant payloads in this order:

   - `Insert`: code, `PageID`, `Vec<Val>`;
   - `Delete`: code, generic `Option<PageID>` using its one-byte presence tag;
   - `Update`: code, `PageID`, `Vec<UpdateCol>`;
   - `DeleteByPrimaryKey`: code, `SelectKey`;
   - `UpdateByPrimaryKey`: code, `SelectKey`, `Vec<UpdateCol>`.

   Keep the existing operation code values. Update deserialization and make
   `MIN_BYTES_HINT` account for the shortest valid kind, `Delete(None)`, which
   contains the operation code and option tag after `row_id`.

3. Adapt `TableDML::insert` folding without changing its logical rules:

   - folding a physical or keyed update into an insert changes the insert
     values and retains the insert variant's original page id;
   - repeated physical updates merge columns and retain the original update
     page id;
   - update followed by physical delete replaces the update with the incoming
     `Delete(Option<PageID>)`;
   - insert followed by physical or keyed delete removes the row entry;
   - keyed update followed by keyed delete retains the previously logged key
     and remains page-free.

4. Update row redo producers in `table/hot.rs`, `table/access.rs`,
   `trx/sys_trx.rs`, and their callers:

   - hot insert emits `Insert(page_id, values)`;
   - hot in-place update emits `Update(page_id, columns)`;
   - unkeyed hot delete, including the old side of a hot move update, emits
     `Delete(Some(page_id))`;
   - unkeyed cold delete emits `Delete(None)`;
   - keyed catalog delete/update emits the existing keyed variants with no
     page id;
   - catalog insert emits `Insert(page_id, values)`.

   Remove the redo-specific `INVALID_PAGE_ID` import and update comments in
   `table/access.rs`. Leave the constant and unrelated users intact.

5. Update recovery dispatch in `recovery/mod.rs`:

   - destructure the mandatory page id from `Insert` and `Update` before
     calling physical row recovery;
   - pass the delete option to `Table::recover_row_delete`;
   - update keyed-user-redo diagnostics so they no longer print a nonexistent
     outer page id;
   - update catalog replay matching to ignore the page carried by `Insert` and
     continue consuming keyed operations logically.

6. Change `Table::recover_row_delete` in `table/recover.rs` to accept
   `Option<PageID>`. First inspect the current active-root pivot:

   - for `row_id < pivot_row_id`, retain the existing deletion-cutoff and
     deletion-buffer behavior and ignore the optional page id;
   - otherwise, require `Some(page_id)` before opening the row page;
   - return `DataIntegrityError::InvalidPayload`, converted and annotated at
     the existing table-access recovery boundary, when a currently hot row has
     no page id.

   This ordering intentionally accepts `Some(page_id)` on the cold path so a
   formerly hot delete remains replayable after checkpoint movement.

7. Update catalog checkpoint and storage consumers to match
   `Insert(_, values)`, while leaving keyed delete/update payloads unchanged.
   Mechanically update all affected logging, transaction, recovery-stream,
   catalog, and table test fixtures to construct the new variants.

8. Advance `REDO_FILE_FORMAT_VERSION` in `log/format.rs` from 4 to 5. Rely on
   the existing super-block version validation to reject version-4 files; add
   no fallback decoder.

9. Update `docs/redo-log.md` to describe format version 5 and the new `RowRedo`
   and `RowRedoKind` layout. Replace its stale format-version references as
   part of the synchronization.

10. Run formatting, linting, and the authoritative workspace test suite:

    ```bash
    rtk cargo fmt --all --check
    rtk cargo clippy --workspace --all-targets -- -D warnings
    rtk cargo nextest run --workspace
    ```

    The alternate `libaio` pass is not required because this task does not
    change storage-backend or backend-neutral I/O behavior.

## Implementation Notes

- Implemented the planned `RowRedo` and `RowRedoKind` shapes, including
  variant-owned page identity, the version-5 wire layout, minimum-size hints,
  explicit truncated-payload rejection, and updated `TableDML` folding.
- Updated all redo producers, recovery and catalog consumers, transaction and
  recovery fixtures, and `docs/redo-log.md`. Cold user-table deletes now emit
  `Delete(None)`; hot physical operations carry their concrete page id; keyed
  catalog operations remain page-independent.
- Recovery now selects delete replay from the recovered table pivot. Cold
  replay accepts either optional-page form, while hot replay rejects
  `Delete(None)` with `DataIntegrityError::InvalidPayload` at the table-access
  recovery boundary.
- Review tightened the producer invariant beyond the original call shape:
  `install_cold_delete_effects` constructs `Delete(None)` internally, and
  `log_by_key` was removed from the public user-table unique-delete API and
  accessor. Catalog `MemTable` paths retain keyed redo support.
- Added coverage for every variant and changed encoding, folding with distinct
  page ids, hot and cold delete recovery, format-version rejection, and hot
  versus cold producer behavior through both full-table mutation and unique
  delete paths.
- Verification passed with `rtk cargo build --workspace`,
  `rtk cargo nextest run --workspace` (1,505 tests), and
  `tools/style_audit.rs --diff-base origin/main` (22 branch-diff Rust files).
  The alternate `libaio` pass was intentionally omitted because no storage-I/O
  behavior changed.

## Impacts

- `doradb-storage/src/log/redo.rs`
  - `RowRedo`, `RowRedoKind`, codec implementations, minimum-byte hints, and
    `TableDML` merge logic.
- `doradb-storage/src/table/hot.rs`
  - hot insert, update, delete, and move-update redo producers.
- `doradb-storage/src/table/access.rs`
  - cold delete producers, `install_cold_delete_effects`, and removal of the
    redo sentinel.
- `doradb-storage/src/recovery/mod.rs`
  - catalog/user replay dispatch and invalid keyed-redo diagnostics.
- `doradb-storage/src/table/recover.rs`
  - optional delete page identity and hot/cold validation.
- `doradb-storage/src/trx/sys_trx.rs`
  - catalog system-transaction redo construction.
- `doradb-storage/src/catalog/checkpoint.rs` and
  `doradb-storage/src/catalog/storage/`
  - catalog redo matching and fixtures.
- `doradb-storage/src/log/format.rs`
  - redo file format version.
- Logging, transaction, recovery-stream, and catalog test modules containing
  `RowRedo` fixtures or `RowRedoKind` pattern matches.
- `docs/redo-log.md`
  - authoritative serialized layout and version description.

## Test Cases

1. Round-trip every row redo variant and verify its page identity and payload,
   including both `Delete(Some(page_id))` and `Delete(None)`.
2. Verify `ser_len`, nonzero start offsets, and truncated-input rejection for
   the changed insert, update, and delete encodings.
3. Round-trip `TableDML` containing physical and keyed variants under the new
   `RowRedo` layout.
4. Verify insert-plus-update folding retains the insert page while applying
   updated values, including keyed catalog update folded into catalog insert.
5. Verify repeated physical updates retain the original update page and merge
   columns.
6. Verify physical update followed by delete adopts the incoming `Some` or
   `None` page value, and insert followed by delete still removes the entry.
7. Verify keyed update followed by keyed delete remains page-free and retains
   the established primary key.
8. Verify hot delete producers emit `Delete(Some(page_id))` and cold delete
   producers emit `Delete(None)` without `INVALID_PAGE_ID`.
9. Verify hot delete recovery succeeds with `Some(page_id)` and reports
   `DataIntegrityError::InvalidPayload` for `None`.
10. Verify cold delete recovery succeeds with `None` and also with
    `Some(page_id)`, preserving the logged-hot/replayed-cold case.
11. Retain coverage for deletion cutoffs, idempotent same-CTS cold delete
    replay, and conflicting duplicate cold deletes.
12. Verify catalog recovery and catalog checkpoint accept the changed insert
    shape and continue handling keyed variants without page identity.
13. Verify redo super-block serialization advertises format version 5 and the
    existing validator rejects a mismatched older version.
14. Pass workspace formatting, Clippy with warnings denied, and
    `cargo-nextest` validation.

## Open Questions

None. The row redo shape, delete recovery interpretation, wire encoding,
format-version policy, and compatibility boundary are resolved by this task.
