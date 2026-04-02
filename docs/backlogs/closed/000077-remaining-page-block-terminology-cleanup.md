# Backlog: Track remaining page-block terminology cleanup in LWC and row index names

## Summary

Task 000104 completed the PageID/BlockID split and the high-signal file/readonly/catalog renames, but it intentionally left `LwcPage`/`PersistedLwcPage` and `row_block_index.rs`/`GenericRowBlockIndex` naming in place. Capture that follow-up explicitly so later planning can decide whether those names should be renamed for consistency or preserved as established subsystem/format proper nouns.

## Reference

See `docs/tasks/000104-separate-buffer-pages-and-persisted-blocks.md`, `doradb-storage/src/lwc/page.rs`, and `doradb-storage/src/index/row_block_index.rs`.

## Deferred From (Optional)

docs/tasks/000104-separate-buffer-pages-and-persisted-blocks.md

## Deferral Context (Optional)

- Defer Reason: The implemented branch already touched buffer, readonly, file, catalog, error, and documentation surfaces broadly. Extending the task to rename `LwcPage` and row-index proper names would have expanded scope into active LWC-format vocabulary and broader runtime lookup/index call paths beyond the high-value PageID/BlockID split.
- Findings: `LwcPage` and `row_block_index.rs` remain live names in both code and planning docs. Task 000104's original design mentioned those renames, but the implemented branch resolved the type split and most persisted block terminology without changing these names. That means resolve needs to preserve the deferred decision explicitly instead of implying the wider rename already happened.
- Direction Hint: Before renaming, decide whether these names are still true terminology bugs or have become format/subsystem proper nouns. Avoid folding that decision into unrelated refactors; scope it as a dedicated follow-up with explicit interaction notes for active LWC and block-index planning docs.

## Scope Hint

Plan a narrow terminology follow-up for the remaining page-vs-block naming holdouts in persisted LWC type names and the runtime row-page index surface, without changing behavior or on-disk layout.

## Acceptance Hint

Future planning either produces a concrete task/RFC for the remaining renames with explicit scope and non-goals, or records a deliberate decision that `LwcPage` and/or `RowBlockIndex` stay as stable proper nouns.

## Notes (Optional)


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
- Detail: Implemented via docs/tasks/000104-separate-buffer-pages-and-persisted-blocks.md
- Closed By: backlog close
- Reference: User decision
- Closed At: 2026-04-02
