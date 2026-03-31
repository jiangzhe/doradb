# Backlog: Compact Column Block Leaf Search Prefix

## Summary

Compact the `ColumnBlockLeafPrefix` search footprint so binary search walks less cold metadata. Move fields that are only needed after the target leaf entry is selected into the entry section format, while preserving the authoritative row-shape and delete semantics introduced by task 000100.

## Reference

- docs/tasks/000100-authoritative-row-shape-api-cutover.md
- docs/rfcs/0012-remove-row-id-from-lwc-page.md phase 1
- doradb-storage/src/index/column_block_index.rs

## Deferred From (Optional)

docs/tasks/000100-authoritative-row-shape-api-cutover.md; docs/rfcs/0012-remove-row-id-from-lwc-page.md phase 1

## Deferral Context (Optional)

- Defer Reason: Task 000100 intentionally implemented the phase-1 ownership cutover without broadening into a second persisted leaf-layout optimization. Repacking `ColumnBlockLeafPrefix` changes the binary-search footprint and persisted entry layout, which is a real format/performance follow-up rather than required correctness work for the authoritative row-shape API cutover.
- Findings:
  - `search_start_row_id()` currently binary-searches leaf prefixes by `start_row_id`, but each probe still walks the full 64-byte `ColumnBlockLeafPrefix`.
  - The phase-1 fingerprint added 16 bytes to the prefix even though `row_shape_fingerprint` is only needed after the target entry has already been selected.
  - Current hit-path decoding already goes through the validated entry view, so moving fingerprint, delete metadata, and other cold fields into the section payload should not change the search algorithm.
  - A staged compaction path looks viable: move `row_shape_fingerprint` out first to recover the old 48-byte prefix, then evaluate moving delete metadata and collapsing section offsets into a smaller search-prefix-plus-entry-offset design.
- Direction Hint: Future planning should treat this as a layout/performance task with explicit persisted-format tradeoffs. Prefer a design where the search prefix contains only binary-search-critical data such as `start_row_id` and an entry locator, and keep row-shape fingerprint plus delete metadata in a self-describing entry payload. Validate gains against the current leaf fanout and avoid entangling this work with RFC-0012 phase 2 unless a shared format decision is actually required.

## Scope Hint

Column block-index leaf layout only: shrink the binary-search prefix to fields needed for row-block selection, move post-hit metadata into the entry section payload, and keep validation/build plumbing aligned with the new layout. Keep phase-2 LWC format work and unrelated delete-domain redesign out of scope unless the new entry-section layout requires narrowly scoped follow-up updates.

## Acceptance Hint

A follow-up task can reduce per-entry prefix bytes without changing lookup semantics: leaf search still resolves the correct block by `start_row_id`, non-search metadata is loaded from the entry payload after the hit, and tests/validators cover dense and sparse row-shape metadata plus any moved delete metadata. If the persisted layout changes, the task documents the new leaf-prefix contract explicitly.

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
