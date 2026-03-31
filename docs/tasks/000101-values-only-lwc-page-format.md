---
id: 000101
title: Values-Only LWC Page Format
status: implemented  # proposal | implemented | superseded
created: 2026-03-31
github_issue: 508
---

# Task: Values-Only LWC Page Format

## Summary

Implement RFC-0012 phase 2 by removing persisted row IDs from `LwcPage`,
turning the page into a values-only payload bound to `ColumnBlockIndex` by
`row_shape_fingerprint`, and replacing runtime `row_id_at(row_idx)` validation
with fingerprint comparison. Keep `LWC_PAGE_SPEC` unchanged; compatibility is
out of scope, and this task only defines the new values-only payload/header
contract used by production readers and writers.

## Context

Phase 1 already made `ColumnBlockIndex` the authoritative source of cold-row
identity for recovery, catalog checkpoint, and runtime lookup metadata. The
remaining ownership split is localized:

1. `doradb-storage/src/lwc/page.rs` still stores `first_row_id`,
   `last_row_id`, and a row-id section in the page body.
2. `doradb-storage/src/lwc/mod.rs` still serializes row IDs into the persisted
   page and includes them in page-size estimation.
3. `doradb-storage/src/table/access.rs` still validates persisted reads with
   `page.row_id_at(row_idx) == row_id` even though the resolved lookup result
   already carries `row_shape_fingerprint`.

Recovery bootstrap and catalog checkpoint already source row IDs from
`ColumnBlockIndex::load_entry_row_ids()` and only need LWC pages for ordinal
value decode. This task finishes the phase-2 cutover by removing the last
page-owned row-id contract from production code.

This task intentionally follows the approved phase-2 direction even though the
RFC draft text currently says to bump the LWC page-integrity version. The
approved scope keeps the shared page-integrity version unchanged and does not
add compatibility handling or explicit legacy-layout rejection.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`
`Parent RFC:`
`- docs/rfcs/0012-remove-row-id-from-lwc-page.md`

## Goals

1. Remove persisted row-id metadata from the LWC page header and body.
2. Store `row_shape_fingerprint` in the LWC page header and use it as the only
   runtime page-binding proof.
3. Keep `ColumnBlockIndex` as the sole authoritative source of persisted row
   identity while LWC pages remain ordinal-only value containers.
4. Keep `LWC_PAGE_SPEC.version` unchanged without adding compatibility work for
   legacy row-id pages.
5. Remove dead persisted row-id page APIs and update tests/fixtures to the new
   values-only contract.

## Non-Goals

1. Supporting old/new mixed-format roots, any online/offline upgrade path, or
   any explicit legacy-layout discrimination path.
2. Changing the canonical fingerprint definition or block-index row-set
   ownership introduced in phase 1.
3. Redesigning recovery, catalog checkpoint, or delete-checkpoint ownership
   beyond the compile-time fallout from removing page-owned row IDs.
4. Broader `lwc` module refactors that are unrelated to persisted format
   removal.
5. Keeping deprecated row-id page helpers around for possible future use.

## Unsafe Considerations (If Applicable)

This task changes the fixed-size byte layout cast by `LwcPage` and therefore
touches existing unsafe-adjacent persisted-layout code.

1. Expected affected paths:
   - `doradb-storage/src/lwc/page.rs`
   - `doradb-storage/src/lwc/mod.rs`
   - `doradb-storage/src/table/access.rs`
   - `doradb-storage/src/buffer/readonly.rs`
2. Required invariants:
   - typed access to persisted LWC bytes remains behind the existing validated
     page helpers and `Pod`/`Zeroable` assumptions remain true after the header
     layout change;
   - the new fixed-size header stores fields explicitly as little-endian byte
     arrays, not through incidental struct layout;
   - any retained `flags`/reserved header bytes are written deterministically
     for the new layout, but the task does not add compatibility-discriminator
     checks for legacy row-id pages;
   - the first column payload starts immediately after the offset array, and
     offset validation must reject malformed ranges that would otherwise be
     misread as column bytes;
   - fingerprint mismatch in runtime lookup remains a hard persisted-corruption
     error.
3. Refresh inventory and run:

```bash
tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md
cargo clippy -p doradb-storage --all-targets -- -D warnings
cargo nextest run -p doradb-storage
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Redefine the persisted LWC header/body contract in
   `doradb-storage/src/lwc/page.rs`.
   - Replace the old header fields with a fixed 32-byte values-only header:
     `row_shape_fingerprint: u128`, `row_count: u16`, `col_count: u16`,
     `flags: u16`, and `reserved: [u8; 10]`.
   - Keep `flags`/`reserved` as deterministic values for the new layout only;
     do not add layout-discriminator logic for compatibility handling.
   - Derive column 0 start as `col_count * size_of::<u16>()` inside the body.
     The page body becomes only `col_offsets` plus column payloads.
   - Remove production row-id parsing/search helpers from `LwcPage` and
     `PersistedLwcPage`, including `RowIDSet`, `row_id_exists`, `row_idx`,
     `find_persisted_row_idx`, `row_id_at`, `decode_row_ids`, and the related
     persisted wrappers.
   - Keep the ordinal decode APIs and add header accessors needed by runtime
     fingerprint validation.

2. Change `LwcBuilder` to write the values-only page layout.
   - Keep collecting `row_ids` while scanning row pages so callers can still
     build `ColumnBlockEntryShape`.
   - Add a `ColumnBlockEntryShape::row_shape_fingerprint()` accessor and change
     `LwcBuilder::build` to accept the already-computed authoritative
     fingerprint from the shape instead of recomputing it from incomplete page
     context.
   - Remove row-id serialization and row-id size accounting from
     `estimate_size()`, and write only the offset array plus column payloads
     into the page body.

3. Switch runtime persisted-row validation in
   `doradb-storage/src/table/access.rs`.
   - Make `read_lwc_row` compare the expected `row_shape_fingerprint` from
     `RowLocation::LwcPage` with the fingerprint stored in the page header
     before any value decode.
   - Remove the `row_id_at(row_idx)` cross-check and any page-read dependency
     on the logical row ID after delete-buffer visibility has already been
     resolved.

4. Keep infrastructure consumers strictly ordinal-only.
   - Preserve the phase-1 recovery and catalog-checkpoint model: row IDs come
     from `ColumnBlockIndex::load_entry_row_ids()`, while values come from the
     LWC page by ordinal.
   - Update any compile-time fallout in recovery/checkpoint helpers so they do
     not depend on removed LWC row-id methods.

5. Update corruption tests and persisted fixtures.
   - Replace row-id-order corruption helpers with values-only corruption cases,
     such as fingerprint mismatch, malformed offsets, or truncated payload
     ranges.
   - Update readonly-cache fixture builders and LWC unit tests to emit the new
     header shape and no row-id payload.
   - Do not add compatibility-specific tests for legacy row-id-layout pages.

6. Keep RFC/task synchronization explicit.
   - Do not add compatibility support just because the envelope version stays
     the same.
   - During `task resolve`, sync RFC-0012 phase-2 wording so it reflects the
     approved no-version-bump values-only cutover.

## Implementation Notes

1. Implemented the phase-2 values-only cutover in `doradb-storage` so
   `LwcPage` now stores `row_shape_fingerprint` in the persisted header, the
   body contains only column offsets plus column payloads, and production
   persisted row-id helpers were removed from `LwcPage` and
   `PersistedLwcPage`.
2. Updated `LwcBuilder` and the table/checkpoint LWC build paths to write the
   new values-only layout from the authoritative
   `ColumnBlockEntryShape::row_shape_fingerprint()`, while continuing to use
   the collected row-id set only for block-index entry-shape construction.
3. Switched runtime persisted reads in `doradb-storage/src/table/access.rs`
   from `row_id_at(row_idx)` validation to fingerprint comparison against the
   resolved `RowLocation::LwcPage` metadata, keeping fingerprint mismatch as
   persisted `InvalidPayload` corruption.
4. Kept recovery bootstrap and catalog checkpoint strictly ordinal-only on the
   LWC side: row IDs continue to come from
   `ColumnBlockIndex::load_entry_row_ids()`, while page decode uses only row
   ordinals and values.
5. Updated unit and corruption coverage for the new contract, including:
   - values-only page/header parsing in `doradb-storage/src/lwc/page.rs`;
   - builder output coverage in `doradb-storage/src/lwc/mod.rs`;
   - readonly-cache invalid-offset rejection in
     `doradb-storage/src/buffer/readonly.rs`;
   - persisted point-read fingerprint-mismatch corruption in
     `doradb-storage/src/table/tests.rs`.
6. Validation completed successfully:
   - `cargo fmt --all`
   - `tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md`
   - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
   - `cargo nextest run -p doradb-storage`
   - `cargo nextest run -p doradb-storage --no-default-features --features libaio`
7. Review handoff is tracked in GitHub issue `#508` and PR `#509`.
8. Resolve-time sync completed:
   - `tools/task.rs resolve-task-next-id --task docs/tasks/000101-values-only-lwc-page-format.md`
   - `tools/task.rs resolve-task-rfc --task docs/tasks/000101-values-only-lwc-page-format.md`

## Impacts

- `doradb-storage/src/lwc/page.rs`
- `doradb-storage/src/lwc/mod.rs`
- `doradb-storage/src/table/access.rs`
- `doradb-storage/src/table/tests.rs`
- `doradb-storage/src/buffer/readonly.rs`
- `docs/rfcs/0012-remove-row-id-from-lwc-page.md` during `task resolve`

## Test Cases

1. `LwcBuilder` writes a values-only page whose header carries the expected
   fingerprint and whose body contains no row-id payload.
2. Persisted row decode still succeeds when row identity comes from
   `ColumnBlockIndex` and values come from the page by ordinal.
3. Runtime point reads succeed when page/header fingerprint matches the
   resolved lookup result.
4. Runtime point reads surface `PersistedPageCorrupted { InvalidPayload }` when
   the stored fingerprint or values-only payload structure is corrupted.
5. Readonly-cache validation tests accept the new layout and reject malformed
   payload/header combinations.
6. Supported validation:

```bash
cargo nextest run -p doradb-storage
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

## Open Questions

1. No open questions remain for this implemented task. If the project later
   needs backward-compatibility, mixed-format support, or upgrade behavior for
   legacy row-id LWC pages, treat that as separate RFC-level planning rather
   than a follow-up on this completed phase-2 cutover.
