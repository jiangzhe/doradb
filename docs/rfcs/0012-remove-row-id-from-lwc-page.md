---
id: 0012
title: Remove Row ID From LWC Page
status: proposal
tags: [storage-engine, lwc, file-format, checkpoint, recovery]
created: 2026-03-31
github_issue: 504
---

# RFC-0012: Remove Row ID From LWC Page

## Summary

This RFC removes persisted row-id metadata from `LwcPage` and makes
`ColumnBlockIndex` the single authoritative owner of cold-row identity. The
change is delivered as a phased cutover: first expand the block-index
row-shape API and migrate runtime, recovery, and checkpoint consumers off
page-owned row ids; then introduce a values-only LWC page format whose header
stores only a `row_shape_fingerprint` binding to the authoritative block-index
entry. This RFC assumes a new-format-only cutover and does not define backward
compatibility, mixed-format roots, or online upgrade behavior ([D1], [D4],
[D5], [D6], [D8], [C1], [C3], [C4], [C5], [C6], [U1], [U2], [U4], [U5]).

## Context

RFC-0011 finished the column-block-index redesign and made the block index the
preferred runtime `RowID -> ordinal` path, but it intentionally left LWC pages
as owners of persisted row-id metadata for recovery, page decoding, and other
non-runtime consumers unless those consumers were redesigned separately ([D8],
[C4], [C5], [C6]). The current code therefore still duplicates cold-row
identity in two places:

1. `ColumnBlockIndex` stores the authoritative logical row set for each block
   and already resolves runtime `row_idx` through `locate_and_resolve_row()`
   ([C3], [C8]).
2. `LwcPage` still serializes a row-id section, and recovery/checkpoint code
   still decodes row ids from the page itself ([C1], [C2], [C5], [C6], [C7]).

That duplication now has clear costs:

1. It keeps LWC page payloads larger than necessary even though row ids are
   already required for block-index entry construction and are already
   compressed on the index side ([C2], [C7]).
2. It preserves an ownership split that no longer matches the desired
   post-RFC architecture where the block index is the sole persisted row-shape
   authority for cold data ([D1], [D3], [U1], [U2]).
3. It keeps runtime point reads dependent on a page-local `row_id_at(row_idx)`
   cross-check even after lookup routing already resolved the row through the
   block index ([C4], [C8]).

The user requirement is to truly remove row ids from `LwcPage`, not merely to
stop using them on the hot path. The replacement consistency mechanism must be
strictly better than coarse summary fields such as `first_row_id` or
`last_row_id`, and the design should avoid reintroducing comparable runtime
costs if possible ([U1], [U3], [U4]).

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:epic`
`- priority:medium`
`- codex`

## Design Inputs

### Documents

- [D1] `docs/architecture.md` - stable `RowID`, block-index role, and point
  lookup flow.
- [D2] `docs/transaction-system.md` - no-steal/no-force model and the role of
  immutable cold data during reads, updates, and recovery.
- [D3] `docs/index-design.md` - block-index ownership of `RowID -> location`
  and the role of `ColumnBlockIndex`.
- [D4] `docs/checkpoint-and-recovery.md` - recovery/bootstrap and checkpoint
  flows that currently depend on persisted cold-row identity.
- [D5] `docs/table-file.md` - persisted LWC block role, random-access
  expectations, and file-format implications.
- [D6] `docs/process/issue-tracking.md` - RFC-first workflow and phase/task
  tracking expectations.
- [D7] `docs/process/unit-test.md` - supported validation runner and
  alternate-backend requirements.
- [D8] `docs/rfcs/0011-redesign-column-block-index-program.md` - current
  post-RFC ownership boundary that this RFC intentionally supersedes.
- [D9] `docs/unsafe-usage-principles.md` - low-level packed-layout and typed
  byte-view expectations.
- [D10] `docs/process/unsafe-review-checklist.md` - validation expectations
  for layout changes adjacent to existing unsafe code.

### Code References

- [C1] `doradb-storage/src/lwc/page.rs` - current LWC layout, row-id section,
  `row_id_at`, `find_row_idx`, and persisted page validation.
- [C2] `doradb-storage/src/lwc/mod.rs` - `LwcBuilder` row-id collection,
  serialization, and row-id size estimation.
- [C3] `doradb-storage/src/index/column_block_index.rs` - logical row-set
  ownership, runtime resolution, and current private row-set decoding.
- [C4] `doradb-storage/src/table/access.rs` - current persisted point-read path
  and `row_id_at(row_idx)` consistency check.
- [C5] `doradb-storage/src/table/recover.rs` - recovery bootstrap that rebuilds
  indexes from persisted LWC row ids.
- [C6] `doradb-storage/src/catalog/storage/checkpoint.rs` - catalog checkpoint
  and tail-merge logic that decodes row ids from persisted LWC pages.
- [C7] `doradb-storage/src/table/mod.rs` - row-page to LWC build flow and
  block-entry shape construction.
- [C8] `doradb-storage/src/index/block_index.rs` - current unified runtime
  lookup path using `locate_and_resolve_row()`.
- [C9] `doradb-storage/src/file/page_integrity.rs` - LWC page-integrity
  envelope versioning and validation model.

### Conversation References

- [U1] User requirement: truly remove row ids from `LwcPage`.
- [U2] User rationale: the block index is already the pre-step to LWC access,
  so row-id ownership should live there.
- [U3] User constraint: the replacement consistency mechanism must be stronger
  than coarse row-id summary fields and should avoid avoidable runtime cost.
- [U4] User approved the phased cutover direction and selected
  `row_shape_fingerprint` instead of summary fields as the replacement binding.
- [U5] User constraint: do not consider compatibility in this RFC.
- [U6] User decision: `first_col_offset` is not needed once the row-id section
  is removed and should not remain in the v2 LWC header.
- [U7] User decision: pad the v2 `LwcPage` header to 32 bytes.

## Decision

Adopt a two-phase cutover that moves all persisted cold-row identity ownership
to `ColumnBlockIndex`, replaces page-owned row-id validation with
`row_shape_fingerprint` binding, and then removes the LWC row-id payload
entirely ([D1], [D4], [D5], [D8], [C1], [C3], [C4], [C5], [C6], [U1], [U4]).

### 1. Ownership boundary

1. `ColumnBlockIndex` becomes the only persisted owner of cold-row identity and
   block-internal row shape. `LwcPage` becomes a values-only page format with
   no row-id payload and no page API that enumerates or resolves `RowID`
   directly ([D1], [D3], [D8], [C1], [C3], [U1], [U2]).
2. This RFC intentionally supersedes RFC-0011 section 6.4 for new-format
   LWC pages: LWC no longer owns persisted row-id metadata for recovery or
   page decoding once the consumer migration is complete ([D8], [C1], [C5],
   [C6], [U1]).
3. This RFC is new-format-only. It does not define support for pre-change LWC
   pages, mixed old/new LWC page versions in one active table root, online
   upgrade rules, or fallback readers. Once phase 2 lands, published roots are
   expected to reference only the new page format ([D5], [D6], [D8], [C9],
   [U5]).

### 2. Authoritative row-shape API

1. `ColumnLeafEntry` gains a persisted `row_shape_fingerprint` field in its
   validated metadata surface, and the corresponding leaf prefix stores the
   same fingerprint on disk ([C3], [U4]).
2. `ResolvedColumnRow` gains `row_shape_fingerprint` so the runtime path can
   carry the expected page binding without a second lookup ([C3], [C8]).
3. The shared facade result for persisted rows also carries the fingerprint:

   ```rust
   RowLocation::LwcPage {
       page_id: PageID,
       row_idx: usize,
       row_shape_fingerprint: u128,
   }
   ```

   This replaces the current page-only persisted location contract for runtime
   consumers that need entry/page binding ([C4], [C8]).
4. `ColumnBlockIndex` exposes a public authoritative row-set API for
   infrastructure callers:

   ```rust
   async fn load_entry_row_ids(&self, entry: &ColumnLeafEntry) -> Result<Vec<RowID>>
   ```

   Recovery and checkpoint code must use this API instead of decoding row ids
   from `LwcPage` ([D4], [C3], [C5], [C6]).

### 3. Fingerprint definition

1. `row_shape_fingerprint` is a 128-bit truncated BLAKE3 digest stored as
   little-endian `u128` in both the block-index leaf prefix and the LWC page
   header ([C3], [C9], [U4]).
2. The digest input is the canonical logical row-set encoding of the block,
   independent of LWC page bytes, independent of delete state, and independent
   of the physical row-map codec used by the block index:
   - fingerprint format version byte `1`
   - logical row-set kind byte:
     - `Dense`
     - `PresentDeltaList`
   - `start_row_id`
   - `row_count`
   - ordered present-delta list as canonical `u32` values when the row set is
     not dense ([C3], [U4]).
3. Dense blocks therefore fingerprint the implied dense row range without any
   per-row payload. Sparse blocks fingerprint the full ordered present-delta
   list. Persisted deletes are excluded because the fingerprint binds the
   logical physical-row membership of the block, not MVCC visibility state
   ([C3], [C4], [D2]).
4. Future row-map encodings such as hole-list, bitmap, run-based encodings, or
   external/offloaded blobs can be added without changing this fingerprint
   contract as long as they decode to the same canonical logical row set
   before hashing. Adding a new physical codec does not require a fingerprint
   change. Only a change to the canonical logical encoding itself requires a
   new fingerprint format version ([C3], [U3], [U4]).

### 4. Replacement consistency mechanism

1. The current runtime check in `table/access.rs` verifies
   `page.row_id_at(row_idx) == requested_row_id`. That exact proof depends on a
   duplicated per-ordinal row-id vector in the page and is incompatible with
   the goal of truly removing row ids from `LwcPage` ([C1], [C4], [U1]).
2. The new runtime binding check is:
   - lookup resolves `page_id`, `row_idx`, and `row_shape_fingerprint` from the
     block index;
   - persisted page load validates the LWC page envelope and column payload
     structure;
   - before value decode, runtime compares the expected fingerprint from the
     lookup result with the fingerprint stored in the page header;
   - mismatch is surfaced as persisted LWC corruption (`InvalidPayload`) ([C1],
     [C4], [C8], [C9], [U3], [U4]).
3. This mechanism is intentionally stronger than coarse summary fields such as
   `first_row_id` or `last_row_id`, because it binds the entire canonical row
   shape of the block in O(1) time rather than only a range summary ([U3],
   [U4]).
4. The RFC does not add a per-ordinal verification vector to the page, because
   that would reintroduce row-id-derived metadata proportional to row count and
   would materially reduce the benefit of removing the row-id payload. Within
   the design space that truly removes page-owned row ids, the fingerprint
   check is the chosen integrity/cost tradeoff ([C2], [U1], [U3], [U4]).
5. Correctness of `row_idx` resolution becomes an index-side invariant:
   - structural validation of leaf row sections remains mandatory;
   - `resolve_row` tests must continue to cover dense and sparse cases;
   - runtime no longer asks the page to independently prove row identity per
     lookup ([C3], [C4], [D7], [U4]).

### 5. LWC page format v2

1. The LWC page-integrity envelope version is bumped for the new page format so
   readers can reject old payloads cleanly rather than attempting to interpret
   them as values-only pages ([C9], [D5]).
2. The v2 LWC page header is a fixed 32-byte structure. It replaces
   `first_row_id`, `last_row_id`, and `first_col_offset` with values-page-local
   metadata only:
   - `row_shape_fingerprint: u128`
   - `row_count: u16`
   - `col_count: u16`
   - `flags: u16`
   - `reserved: [u8; 10]`
   The reserved bytes are written as zero and validated as zero in v2
   readers ([C1], [U4], [U6], [U7]).
3. The v2 LWC page body no longer contains a row-id section. Its layout is:
   - column offsets
   - column payloads
   - padding
   The start of column 0 is derived as `col_count * sizeof(u16)`, so
   `first_col_offset` is not retained in the v2 header ([C1], [U6]).
   There is no page API equivalent to `decode_row_ids`, `find_row_idx`, or
   `row_id_at` in the v2 contract ([C1], [C2], [U1]).
4. `LwcBuilder` still collects row ids while building a block so it can produce
   `ColumnBlockEntryShape` and the canonical row-shape fingerprint, but it no
   longer serializes those row ids into the page payload ([C2], [C7], [U1]).

### 6. Consumer behavior

1. Runtime point reads continue to use `locate_and_resolve_row()` to get
   `page_id` and `row_idx`, but replace `row_id_at(row_idx)` validation with
   the fingerprint match and decode values directly by ordinal ([C4], [C8],
   [U4]).
2. Recovery bootstrap enumerates persisted row ids from
   `ColumnBlockIndex::load_entry_row_ids()` and zips them with value decode by
   ordinal from the LWC page ([C5], [C3]).
3. Catalog checkpoint and tail-merge paths make the same shift: row identity
   comes from the block index, value bytes come from `LwcPage` by ordinal
   ([C6], [C3]).
4. Delete checkpoint remains row-id-based at its API boundary and continues to
   resolve absolute row ids against authoritative block-index row sets when
   rewriting persisted delete state ([D4], [D8], [C3]).

## Alternatives Considered

### Alternative A: Keep Row IDs In LWC And Only Clean Up Runtime Lookup

- Summary: Preserve the current LWC row-id section and continue using
  `row_id_at()` while treating the block index as the preferred runtime lookup
  path.
- Analysis: This is the lowest-risk follow-up and largely matches the current
  post-RFC-0011 state, but it does not satisfy the user requirement to truly
  remove row ids from `LwcPage`, and it preserves duplicate cold-row identity
  ownership across page and index ([D8], [C1], [C3], [C4], [U1]).
- Why Not Chosen: It is a runtime cleanup, not a true ownership redesign.
- References: [D8], [C1], [C3], [C4], [U1]

### Alternative B: Replace Row IDs With Coarse Summary Fields Only

- Summary: Remove the row-id vector but keep only page-level summaries such as
  `first_row_id`, `last_row_id`, and `row_count`.
- Analysis: This reduces payload size, but it does not bind sparse interior row
  shape and therefore cannot detect many index/page mismatches that the user
  explicitly wanted to avoid ([C1], [C3], [U3]).
- Why Not Chosen: It is weaker than the required replacement mechanism.
- References: [C1], [C3], [U3], [U4]

### Alternative C: Add A Per-Ordinal Verification Vector

- Summary: Remove full row ids from the page but store a per-ordinal hash/tag
  vector so runtime can still verify `row_idx -> row_id` on every lookup.
- Analysis: This most closely mimics the strength of the current
  `row_id_at()`-based proof, but it reintroduces row-id-derived metadata
  proportional to row count and materially reduces the density and ownership
  gains of the redesign ([C1], [C2], [U1], [U3]).
- Why Not Chosen: It conflicts with the goal of truly removing page-owned
  row-id metadata and eliminating as much runtime overhead as possible.
- References: [C1], [C2], [U1], [U3]

## Unsafe Considerations (If Applicable)

This RFC changes packed persisted layouts and validated typed-page access in
code that already relies on `Pod`/`Zeroable` casts and byte-slice decoding. It
therefore requires explicit unsafe-boundary discipline ([D9], [D10], [C1],
[C3], [C9]).

1. Expected unsafe-adjacent touch points:
   - `doradb-storage/src/lwc/page.rs`
   - `doradb-storage/src/lwc/mod.rs`
   - `doradb-storage/src/index/column_block_index.rs`
   - `doradb-storage/src/file/page_integrity.rs`
2. Required invariants:
   - all typed access to persisted LWC and block-index bytes remains behind the
     existing validated page helpers;
   - new header fields use explicit little-endian encode/decode helpers rather
     than relying on implicit layout assumptions;
   - v2 LWC payload parsing must reject any attempt to interpret an old row-id
     section as column bytes;
   - fingerprint mismatch must be surfaced as persisted corruption, not treated
     as a soft miss.
3. If implementation changes unsafe code or low-level page-casting logic, run:

```bash
tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md
cargo clippy -p doradb-storage --all-targets -- -D warnings
cargo nextest run -p doradb-storage
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Implementation Phases

- **Phase 1: Authoritative Row-Shape API Cutover**
  - Scope: Extend `ColumnBlockIndex` so it is the authoritative source of cold
    row ids and row-shape fingerprints for all consumers, while retaining the
    current LWC page format temporarily.
  - Goals:
    - add persisted `row_shape_fingerprint` to column-block-index leaf entries;
    - expose `load_entry_row_ids(entry)` for recovery and checkpoint code;
    - carry `row_shape_fingerprint` through resolved runtime lookup results;
    - migrate non-runtime consumers away from `LwcPage::decode_row_ids()`.
  - Non-goals:
    - removing row ids from the LWC page payload;
    - changing the LWC page-integrity version;
    - removing `row_id_at()` runtime validation yet.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

- **Phase 2: Values-Only LWC Page Format**
  - Scope: Introduce LWC page v2 with no row-id section and switch runtime
    validation to fingerprint binding.
  - Goals:
    - replace the LWC header row-id fields with `row_shape_fingerprint`;
    - remove row-id serialization from `LwcBuilder`;
    - remove page APIs that expose persisted row ids;
    - replace `row_id_at()` runtime checks with fingerprint mismatch checks.
  - Non-goals:
    - defining backward compatibility or mixed-format support;
    - per-ordinal verification vectors;
    - broader analytical column-page redesign.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

## Consequences

### Positive

- Removes duplicated persisted cold-row identity and makes block-index
  ownership explicit ([D1], [D3], [D8], [U1]).
- Improves LWC page density by removing row-id payload bytes from every
  persisted block ([C2], [U1]).
- Lowers runtime point-read overhead relative to the current `row_id_at()`
  check while keeping an O(1) entry/page binding check ([C4], [U3], [U4]).
- Simplifies the conceptual split: block index owns identity, LWC owns values
  ([D1], [D5], [U2]).

### Negative

- Introduces another persisted-format program after RFC-0011 and deliberately
  chooses a new-format-only cutover instead of spending scope on compatibility
  rules ([D5], [D6], [D8], [U5]).
- Removes the current independent per-lookup `row_id_at()` proof and relies on
  block-index correctness plus whole-block fingerprint binding instead ([C4],
  [U3], [U4]).
- Broadens implementation scope beyond runtime reads into recovery and catalog
  checkpoint flows ([C5], [C6], [D4]).

## Open Questions

1. No blocking design questions remain for the draft direction. Phase planning
   should focus on work packaging, not on re-opening the ownership model or the
   chosen consistency mechanism.

## Future Work

1. Analytical scan paths may later choose to consume block-index row-shape
   metadata more directly, but this RFC does not redesign analytical page scan
   behavior.
2. If future evidence shows that whole-block fingerprint binding is not
   sufficient, a separate follow-up can evaluate stronger per-ordinal
   verification metadata. That is intentionally not part of this RFC.

## References

- `docs/rfcs/0011-redesign-column-block-index-program.md`
- `docs/tasks/000097-runtime-lookup-alignment.md`
- `docs/tasks/000099-merge-block-index-lookup-path-and-result.md`
