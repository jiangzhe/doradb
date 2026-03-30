---
id: 000098
title: Later-Domain Evolution and Auxiliary Payload Expansion
status: implemented  # proposal | implemented | superseded
created: 2026-03-30
github_issue: 499
---

# Task: Later-Domain Evolution and Auxiliary Payload Expansion

## Summary

Implement RFC-0011 phase 4 as a narrow persisted-domain evolution task:
add validated support for ordinal-domain delete payloads on authoritative
full-entry rewrite paths, preserve existing persisted delete domains during
delete-only rewrites, generalize auxiliary-payload handling only as far as
that evolution requires, and remove the superseded payload-era compatibility
layer introduced during phases 1 through 3. Runtime hint fields, row-id
spill support, blob reclamation policy, and broad `RowLocation::LwcPage(..)`
cleanup remain out of scope.

## Context

RFC-0011 leaves phase 4 intentionally evidence-gated. After phases 1 through
3, it does not require shipping every deferred idea; it permits only the
later work that still has concrete justification. The phase-4 scope that
remains justified in the current codebase is persisted delete-domain
evolution plus cleanup of the narrow compatibility layer that earlier phases
kept alive while the v2 leaf contract rolled out.

Phase 1 landed the v2 leaf format, logical-entry rebuild model, and shared
auxiliary-blob framing. Phase 2 made v2 entry metadata authoritative for
checkpoint and recovery infrastructure paths. Phase 3 introduced runtime
`resolve_row` and moved persisted point reads off the compatibility payload
path, while explicitly deferring later negative-lookup and delete-hint work
until broader benchmark coverage exists.

The remaining phase-4 gaps are concrete:

1. The current leaf implementation still hardcodes persisted delete-domain
   handling to `RowIdDelta`. Validation rejects any other domain, and encode
   paths always emit row-id-delta delete payloads.
2. Delete-only checkpoint rewrites still rebuild delete sections purely from
   row-id deltas. If a later authoritative rewrite were to emit an
   ordinal-domain entry, the next delete-only checkpoint would currently
   collapse it back to row-id space.
3. The current compatibility layer around `ColumnPagePayload` is no longer
   required on the main read and infrastructure paths, but several payload-era
   aliases and reconstruction helpers remain in the column-block-index code.
4. Catalog tail merge already has authoritative block row ordering through
   `merged_row_ids` and already rewrites full entries through
   `batch_replace_entries`, which makes it a legitimate first consumer for
   ordinal-domain persisted deletes under the RFC rules.
5. Broader runtime hint fields still lack the required benchmark evidence.
   The current phase-3 benchmark only covers hot-cache block-index lookup, and
   backlog `000074` explicitly says cold-cache and end-to-end persisted
   row-read measurement must land first.
6. Several older runtime callers still branch on `RowLocation::LwcPage(..)`
   and remain unfinished or intentionally deferred. Pulling those branches
   into this task would broaden phase 4 well past the scoped persisted-domain
   and compatibility cleanup slice approved here.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
Issue Labels:
- type:task
- priority:medium
- codex

Parent RFC:
- docs/rfcs/0011-redesign-column-block-index-program.md

## Goals

1. Add validated persisted delete-domain support for both `RowIdDelta` and
   `Ordinal` in the v2 column-block-index leaf contract.
2. Allow authoritative full-entry rewrite paths that already own exact block
   row ordering to persist ordinal-domain delete payloads.
3. Keep delete-only checkpoint and similar row-id-based callers stable at
   their API boundary, but make the rewrite path preserve an entry's existing
   persisted domain instead of always rewriting back to row-id space.
4. Generalize auxiliary-payload handling only enough to support the persisted
   delete-domain evolution cleanly, without reopening the phase-1 framing
   contract.
5. Remove the superseded payload-era compatibility surface that phases 1
   through 3 intentionally kept temporarily:
   - `ColumnBlockIndex::find`
   - `ColumnBlockIndex::find_entry`
   - `load_payload_deletion_deltas`
   - `ColumnPagePayloadPatch`
   - `batch_replace_payloads`
   - compatibility `ColumnPagePayload` reconstruction inside leaf decoding
6. Keep production callers on authoritative entry metadata, direct `block_id`
   access, and typed delete decoding after the cleanup.

## Non-Goals

1. Removing `RowLocation::LwcPage(..)` from the broader runtime API or
   finishing the cold-row update/delete, recovery-CTS, or uncommitted-read
   branches that still depend on it.
2. Adding delete hints, negative-lookup hints, or other runtime hint fields.
3. Consuming backlog `000074` benchmark expansion in this task.
4. Mandating row-id external spill support for row sections.
5. Changing auxiliary-blob reclamation reachability, trigger, or SLA policy
   tracked by backlogs `000029` and `000030`.
6. Changing the page-integrity envelope, tree-version boundary, or other
   phase-1 on-disk framing contracts.
7. Reassigning all persisted row-shape ownership away from LWC pages for
   non-runtime consumers.

## Unsafe Considerations (If Applicable)

This task does not introduce a new page kind or a new unsafe category, but it
does touch low-level persisted page parsing and typed layout access in the
column index path.

1. Expected affected paths:
   - `doradb-storage/src/index/column_block_index.rs`
   - `doradb-storage/src/index/column_deletion_blob.rs`
   - `doradb-storage/src/index/column_payload.rs` if payload-era types are
     retired or moved during compatibility cleanup
2. Required invariants:
   - leaf validation must continue to reject invalid section/domain/codec
     combinations and any malformed bounds or overlap conditions before typed
     access;
   - ordinal-domain decode must only be accepted when counts, offsets, and
     row-shape membership remain consistent with the validated leaf entry;
   - delete-only rewrites that preserve ordinal-domain entries must derive
     ordinals from the authoritative row mapping of the same block version,
     not from unvalidated or stale caller assumptions;
   - any retained unsafe or pod casts must remain behind validated helpers
     with concrete `// SAFETY:` comments tied to byte layout, bounds, and
     lifetime invariants.
3. If implementation changes unsafe code or low-level typed layout access,
   refresh inventory and run:

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

1. Introduce explicit persisted delete-domain handling in
   `doradb-storage/src/index/column_block_index.rs`.
   - Replace the current single-domain assumption with typed support for both
     `RowIdDelta` and `Ordinal`.
   - Update leaf validation so both legal domains decode, while malformed
     domain/codec/header combinations still fail as persisted corruption.
   - Keep count and membership validation aligned with the RFC's semantic
     checks for v2 leaves.

2. Extend authoritative full-entry build inputs with explicit domain choice.
   - Add a narrow domain-selection path to `ColumnBlockEntryShape` /
     `ColumnBlockEntryInput` so callers that own exact `row_ids` can request
     ordinal-domain persistence.
   - Keep the default append/new-block path conservative so ordinary writers
     continue to emit row-id-domain deletes unless a caller explicitly opts
     into ordinal persistence on an authoritative rewrite path.

3. Make typed delete rewrites domain-preserving.
   - Keep `ColumnDeleteDeltaPatch` row-id-based at the caller boundary.
   - Inside `batch_replace_delete_deltas`, load the current entry's row shape
     and existing persisted delete domain, merge pending deletes in row-id
     space, and re-encode back into the same domain for that entry.
   - This prevents delete-only checkpoint from erasing ordinal-domain entries
     produced by authoritative full-entry rewrites.

4. Use ordinal-domain persistence on the first justified authoritative caller.
   - In `doradb-storage/src/catalog/storage/checkpoint.rs`, make catalog tail
     merge use the full-entry rewrite path with ordinal-domain delete
     persistence, because that code already rebuilds the exact merged row
     ordering for the rewritten block.
   - Keep user-table deletion checkpoint in
     `doradb-storage/src/table/persistence.rs` row-id-based externally while
     relying on the domain-preserving rewrite behavior above.

5. Generalize auxiliary-payload handling only as far as phase 4 needs.
   - Keep the existing 8-byte auxiliary-blob framing contract and page kind
     unchanged.
   - Update helper naming and validation logic so the payload path is not
     semantically hardwired to "row-id delete deltas only" when the same blob
     framing is used for persisted delete payloads in either legal domain.
   - Do not add row-id external spill codecs or new runtime-hint payloads in
     this task.

6. Remove superseded payload-era compatibility APIs and callers.
   - Remove:
     - `ColumnBlockIndex::find`
     - `ColumnBlockIndex::find_entry`
     - `load_payload_deletion_deltas`
     - `ColumnPagePayloadPatch`
     - `batch_replace_payloads`
   - Stop reconstructing `ColumnPagePayload` inside `build_leaf_entry`.
   - Migrate remaining callers and tests to use `locate_block`,
     `entry.block_id()`, and authoritative delete decoding.
   - If `ColumnPagePayload` becomes dead after the migration, delete the
     payload-era type and move any still-needed shared `BlobRef` ownership
     into the surviving index/auxiliary-payload layer.

7. Keep compatibility cleanup explicitly bounded.
   - Do not remove `RowLocation::LwcPage(..)` or broaden this task into the
     older runtime TODO branches in `table/access.rs`, `table/mod.rs`, or
     `trx/undo/index.rs`.
   - Document those branches as separate follow-up work if broader runtime
     cleanup is still desired after this task.

## Implementation Notes

1. Implemented the phase-4 persisted delete-domain evolution in
   `doradb-storage/src/index/column_block_index.rs` and
   `doradb-storage/src/index/column_deletion_blob.rs`:
   - leaf validation, encode, and decode now support both persisted delete
     domains, `RowIdDelta` and `Ordinal`;
   - authoritative full-entry rewrites can persist ordinal-domain delete
     payloads;
   - delete-only rewrites remain row-id-based at the caller boundary but now
     preserve each target entry's existing persisted delete domain when
     rebuilding the leaf entry.
2. Made catalog tail merge in
   `doradb-storage/src/catalog/storage/checkpoint.rs` the first justified
   authoritative ordinal-domain rewrite consumer:
   - catalog tail merge now opts into ordinal-domain delete persistence on its
     full-entry rewrite path because it already owns the merged block row
     ordering;
   - user-table deletion checkpoint in `doradb-storage/src/table/persistence.rs`
     stays row-id-based externally while relying on the domain-preserving
     rewrite behavior above.
3. Removed the payload-era compatibility layer and migrated remaining callers:
   - removed `ColumnBlockIndex::find`, `ColumnBlockIndex::find_entry`,
     `load_payload_deletion_deltas`, `ColumnPagePayloadPatch`, and
     `batch_replace_payloads`;
   - deleted `doradb-storage/src/index/column_payload.rs`;
   - moved remaining callers and tests onto `locate_block`, direct
     `entry.block_id()`, and authoritative typed delete decoding.
4. Performed follow-up cleanup after implementation and review:
   - removed the unused `batch_update_offloaded_bitmaps` compatibility path
     because the current repo has no production caller for it;
   - trimmed `ColumnLeafEntry` to the surviving live metadata surface;
   - removed the `read_delete_payload_bytes` wrapper and consolidated shared
     delete-set decoding for inline and external delete payloads.
5. Refreshed supporting measurement and low-level safety tracking:
   - updated `doradb-storage/examples/bench_column_runtime_lookup.rs` to
     compare `locate_block` with `locate_and_resolve_row`;
   - refreshed `docs/unsafe-usage-baseline.md` after the low-level leaf/decode
     changes.
6. Verification executed for the implemented state:
   - `tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md`
   - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
   - `cargo nextest run -p doradb-storage`
   - `cargo nextest run -p doradb-storage --no-default-features --features libaio`

## Impacts

- `doradb-storage/src/index/column_block_index.rs`
  - persisted delete-domain validation, encode/decode, domain-preserving
    delete rewrites, and removal of payload-era compatibility APIs.
- `doradb-storage/src/index/column_checkpoint.rs`
  - authoritative delete loader remains; payload-named compatibility alias is
    removed.
- `doradb-storage/src/catalog/storage/checkpoint.rs`
  - catalog tail merge becomes the first authoritative ordinal-domain rewrite
    consumer.
- `doradb-storage/src/table/persistence.rs`
  - delete-only checkpoint stays row-id-based externally while using the new
    domain-preserving rewrite behavior.
- `doradb-storage/src/index/block_index.rs`
  - remaining compatibility route switches from payload-style lookup to
    authoritative entry/block access without changing `RowLocation`.
- `doradb-storage/src/index/column_deletion_blob.rs`
  - auxiliary-payload helper and validation cleanup for persisted delete
    payloads across legal domains.
- `doradb-storage/src/index/column_payload.rs`
  - retired or reduced to any minimal surviving shared type if payload-era
    compatibility cleanup leaves nothing else in use.
- `doradb-storage/src/file/table_file.rs`
  - tests and any helper lookups move off `ColumnBlockIndex::find`.
- `doradb-storage/src/trx/recover.rs`
  - tests and corruption-path helpers move off `find_entry` aliases.
- `doradb-storage/src/index/mod.rs`
  - re-exports updated after compatibility-layer cleanup.

## Test Cases

1. Leaf validation accepts legal `RowIdDelta` and `Ordinal` delete-domain
   entries and rejects malformed domain/codec/header combinations.
2. Authoritative full-entry rewrite can persist ordinal-domain deletes and
   still decode back to the correct logical deleted-row set.
3. Delete-only checkpoint against an ordinal-domain entry preserves the
   ordinal domain while merging new row-id deletes correctly.
4. Row-id-domain entries still round-trip through typed delete rewrites
   without regression.
5. Catalog tail merge rewrites the last block using authoritative merged row
   ordering and persists the selected later-domain form deterministically.
6. Auxiliary-blob validation still rejects framing mismatches and wrong
   blob-kind/codec/version combinations for persisted delete payloads.
7. Production callers and tests compile and pass after removal of payload-era
   compatibility APIs, using `locate_block`, direct `block_id`, and
   authoritative delete decoding instead.
8. Existing runtime callers that still branch on `RowLocation::LwcPage(..)`
   remain behaviorally unchanged by this task.
9. Routine validation for the implemented change uses:

```bash
cargo nextest run -p doradb-storage
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

## Open Questions

1. Runtime hint fields remain blocked on broader benchmark coverage from
   `docs/backlogs/000074-expand-runtime-lookup-benchmark-coverage.md`.
2. Row-id external spill remains a separate follow-up if builder correctness
   or measured wins later justify it.
3. Broader cleanup of `RowLocation::LwcPage(..)` and its unfinished runtime
   callers should be planned separately if phase-4 persisted-domain cleanup
   proves stable.
4. Finalizing the inline delete-field footprint and removing the remaining
   stale delete-surface sizing/constants is deferred to
   `docs/backlogs/000075-refine-column-block-index-inline-delete-field-and-delete-surface-cleanup.md`.
