---
id: 0011
title: Redesign Column Block Index Program
status: proposal
tags: [storage-engine, index, checkpoint, recovery, file-format]
created: 2026-03-28
github_issue: 494
---

# RFC-0011: Redesign Column Block Index Program

## Summary

This RFC defines a phased redesign program for `ColumnBlockIndex` that replaces the current fixed-width persisted leaf format with a variable-entry leaf layout, preserves stable `RowID` and 64-bit persisted page identity, keeps phase 1 externally row-id-based, and creates a path for later runtime lookup improvements without requiring another leaf-format redesign. The program freezes the important contracts now: leaf-only phase-1 scope, no-slot front-packed leaf layout, non-overlapping coverage semantics, root-page-driven tree-format dispatch, exact section and auxiliary-header formats, split lookup APIs, conservative delete-domain transition, and structural plus semantic validation expectations. Compatibility with pre-redesign persisted trees is intentionally out of scope for this RFC ([D1], [D2], [D3], [D4], [D5], [C1], [C4], [C5], [C10], [C11], [U1], [U2], [U3], [U5], [U6]).

## Context

The current `ColumnBlockIndex` was introduced by RFC-0002 as a persistent CoW B+Tree mapping `start_row_id` to one persisted LWC page plus fixed-width delete metadata. That design solved the original durability and lookup problem, but its leaf format now constrains further evolution:

1. The current leaf pays a fixed 128-byte payload per entry even when the block is dense and has no persisted deletes, which depresses leaf fanout in the common dense case ([C1], [C2], [D3]).
2. The current CoW mutation model is payload-patch oriented. It is well-suited to fixed-width inline payload replacement, but it does not generalize cleanly once row-id metadata and delete metadata become variable-sized per entry ([C1], [C2], [C5]).
3. Runtime point reads still consult the in-memory deletion buffer first and then rely on persisted LWC page-local row-id resolution to find a row position, so the current block index is not yet the preferred runtime `RowID -> ordinal` authority ([C4], [C6], [D1], [D2]).
4. Deletion checkpoint and recovery logic currently operate in row-id-relative space, merging absolute row ids into block-relative row-id deltas keyed by block `start_row_id` ([C5], [C8], [C9], [D2], [D4]).
5. Proposal review converged on a stronger target shape: redesign the persisted leaf format first, preserve current external semantics in phase 1, and only shift runtime lookup and ordinal delete semantics later if that path is still justified after the leaf redesign lands ([U1], [U3], [U4], [U5], [U6]).

This decision is needed now because the current leaf format is already the limiting factor for dense-block economics and for any future inline row-id translation strategy. The redesign should be planned as a program before implementation starts so phase boundaries, validation behavior, and later semantic transitions are explicit rather than discovered piecemeal in task docs ([D6], [D7], [C1], [C5], [C10], [U1], [U5]).

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:epic`
`- priority:medium`
`- codex`

## Design Inputs

### Documents

- [D1] `docs/architecture.md` - stable `RowID`, block-index role, point-read flow, and storage-tier boundaries.
- [D2] `docs/transaction-system.md` - no-steal/no-force model, runtime deletion-buffer role, and redo-only recovery expectations.
- [D3] `docs/index-design.md` - current conceptual role of `ColumnBlockIndex` and persisted block lookup.
- [D4] `docs/checkpoint-and-recovery.md` - deletion-checkpoint watermark rules and persisted-state recovery constraints.
- [D5] `docs/table-file.md` - 64 KiB page model, CoW publication, and meta/root behavior.
- [D6] `docs/process/issue-tracking.md` - RFC-first workflow and phase/task tracking expectations.
- [D7] `docs/process/unit-test.md` - supported validation runner and timeout policy.
- [D8] `docs/unsafe-usage-principles.md` - expectations for packed/on-page layout changes and any unsafe boundary updates.
- [D9] `docs/process/unsafe-review-checklist.md` - required validation and inventory expectations if low-level page access changes touch unsafe code.
- [D10] `docs/rfcs/0002-column-block-index.md` - current implemented `ColumnBlockIndex` design being evolved by this program.
- [D11] `docs/rfcs/0007-disk-page-integrity-for-cow-storage-files.md` - persisted page integrity envelope already in force for CoW pages.

### Code References

- [C1] `doradb-storage/src/index/column_block_index.rs` - current fixed-width leaf layout, split arrays, and payload-patch-oriented CoW updates.
- [C2] `doradb-storage/src/index/column_payload.rs` - current fixed-width `ColumnPagePayload` and row-id-relative delete storage.
- [C3] `doradb-storage/src/index/column_deletion_blob.rs` - current deletion-specific external blob format and writer/reader behavior.
- [C4] `doradb-storage/src/table/access.rs` - current point-read path checks deletion buffer first and then resolves row position via LWC page.
- [C5] `doradb-storage/src/table/persistence.rs` - current deletion checkpoint groups absolute row ids into block-relative row-id deltas and rewrites payloads.
- [C6] `doradb-storage/src/lwc/page.rs` - current persisted LWC row-id decoding and row-position lookup behavior.
- [C7] `doradb-storage/src/table/mod.rs` - current LWC build flow and persisted block boundary behavior during data checkpoint.
- [C8] `doradb-storage/src/table/recover.rs` - recovery-time use of persisted LWC rows and deletion state.
- [C9] `doradb-storage/src/catalog/storage/checkpoint.rs` - catalog checkpoint/rebuild use of the same persisted structures and contracts.
- [C10] `doradb-storage/src/file/meta_page.rs` - current table root metadata shape with no explicit column-block-index format contract beyond root page id.
- [C11] `doradb-storage/src/file/page_integrity.rs` - shared page-kind/version/checksum envelope used by persisted CoW page kinds.

### Conversation References

- [U1] User requirement: treat the redesign as the ultimate target state for `ColumnBlockIndex`; broad code and API changes are acceptable if the end-state is strong.
- [U2] User requirement: compatibility with older APIs and persisted layouts is not a concern for this redesign.
- [U3] User preference: remove a generic slot array and use a front-packed fixed-prefix layout with row-id and deletion sections packed from the page tail.
- [U4] User request: close remaining design questions before moving into RFC writing.
- [U5] User requirement: use a program RFC with clear phase boundaries, but do not hardcode the overall program to exactly three phases; each phase must be independently testable.
- [U6] User approved the round-1 recommendation to use a program-level RFC that freezes phase-1 leaf contracts now and leaves later phases flexible.

### Source Backlogs (Optional)

- [B1] `docs/backlogs/000029-column-deletion-blob-reachability-sweep-strategy.md`
- [B2] `docs/backlogs/000030-column-deletion-blob-reclamation-trigger-and-sla.md`

## Decision

Adopt a phased `ColumnBlockIndex` redesign program that freezes the persisted leaf-format and lookup contracts required now, preserves current external row-id semantics in phase 1, and leaves later runtime lookup and ordinal-delete evolution task-decomposable behind explicit phase boundaries ([D1], [D2], [D3], [D4], [D5], [D10], [D11], [C1], [C4], [C5], [C10], [U1], [U2], [U3], [U5], [U6]).

### 1. Program scope and relationship to RFC-0002

1. This RFC is a redesign program for `ColumnBlockIndex`, not a full persisted-storage unification. It keeps the existing CoW B+Tree role from RFC-0002 while superseding RFC-0002 decisions related to leaf payload layout, lookup contract evolution, and persisted delete-format evolution ([D10], [C1], [U1], [U6]).
2. The redesign preserves these core invariants:
   - `RowID` remains stable across storage tiers.
   - persisted page/block identity remains 64-bit.
   - the in-memory deletion buffer remains the first MVCC authority on the runtime point-read path.
   - ordinal-based persisted deletes are not required in phase 1 ([D1], [D2], [C4], [C5], [U1]).
3. Compatibility with pre-redesign persisted trees, mixed-format migration rules, and backward-reading older roots are intentionally out of scope. This RFC assumes a new-format-only implementation path for the redesigned tree format rather than specifying long-lived compatibility behavior ([D6], [D11], [C10], [U2]).
4. The program is phased, but phase count is not fixed. Task planning may split or combine work further as long as each phase boundary remains behaviorally clear and independently testable ([D6], [D7], [U5]).

### 2. Tree scope and cutover model

1. Phase 1 redesigns leaf pages only. Branch pages remain fixed-width `start_row_id -> child_page_id` mappings unless a later task explicitly proves that branch redesign is required ([C1], [D3], [U3]).
2. Parent separators continue to be keyed by the lower bound of the right child’s first leaf entry ([C1], [D10]).
3. The redesigned tree format publishes a new column-block-index page version under the existing page-integrity model. Tree-format dispatch is driven by the validated page kind and version of the root page itself, not by a separate format field in table meta/root metadata. Once one root page is selected, all pages reachable from that root must obey that same tree-format version; mixed-version trees are not part of this RFC’s contract ([D11], [C10], [C11], [U2]).

### 3. Search semantics and the meaning of `start_row_id`

1. `start_row_id` is the coverage lower bound used for tree search. It is not required to equal the first present row id inside a non-normalized block ([C1], [C6], [C7], [U3]).
2. Each leaf entry therefore describes a candidate coverage interval:
   - `coverage_lower = start_row_id`
   - `coverage_upper = start_row_id + row_id_span - 1`
3. After tree descent, lookup must perform an entry-local membership test:
   - reject if the target row id is outside the coverage interval,
   - reject if `row_id - start_row_id < first_present_delta`,
   - consult the row-id section codec only after those prefix checks pass ([C4], [C6], [U3]).
4. “No leading or trailing holes” is not a system invariant. It is only an encoder normalization preference when the writer can reduce holes without changing row identity or external semantics ([C7], [U1], [U3]).
5. Adjacent leaf entries must be strictly ordered by `coverage_lower`, and their coverage intervals must not overlap. Gaps are legal. Overlap is not. In normalized form this reduces to monotonic non-overlapping contiguous or gapped ranges; in non-normalized form it still forbids ambiguous leaf-boundary membership ([C1], [C6], [C7], [U6]).

### 4. Phase-1 leaf page contract

1. Inside the persisted column-block-index page payload protected by RFC-0007’s page-integrity envelope, redesigned leaf pages use this logical layout:
   - node header,
   - ordered fixed-prefix array,
   - free space,
   - shared reverse-growing payload arena ([D11], [C1], [C11], [U3]).
2. The redesign does not use a generic slot array. Search-critical metadata stays in the front-packed fixed-prefix array; variable row-id and delete sections are addressed directly from the prefix ([U3]).
3. The fixed prefix must carry enough metadata for constant-time section lookup after binary search. Phase-1 stable fields are:
   - `start_row_id`
   - `block_page_id`
   - `row_id_span`
   - `first_present_delta`
   - `row_count`
   - `del_count`
   - row-id section offset and length
   - delete section offset and length
   - row-id codec kind
   - delete codec kind
   - delete domain
   - entry/prefix flags and version metadata ([C1], [C2], [U3], [U6]).
4. `first_present_delta` is widened to a 32-bit field so non-normalized coverage is legal without forcing an artificial `u16` bound on the distance from coverage lower bound to first present row ([C6], [C7], [U6]).
5. Offset rules are part of the stable contract:
   - offsets are measured from the first byte of the column-block-index node payload area immediately after the node header,
   - offsets address the section header, not just codec bytes,
   - `off == 0 && len == 0` means the section is absent,
   - `len == 0 && off != 0` is invalid,
   - `off == 0 && len != 0` is invalid,
   - row-id and delete sections may not overlap each other, the prefix array, or the integrity trailer ([D11], [C11], [U3]).
6. Optional delete synopsis fields are not part of the stable phase-1 prefix. They may be introduced in a later phase only if measured lookup benefit justifies their per-entry cost ([C4], [U6]).
7. All persisted integer fields in redesigned leaves, section headers, and auxiliary headers are little-endian. Packed-layout access must go through validated encode/decode helpers rather than unchecked direct field reads from unvalidated page bytes ([D8], [D11], [C11], [U6]).

### 5. Section headers, codecs, and external payload direction

1. Every variable row-id or delete section begins with the same fixed 4-byte binary header:

   ```text
   byte 0: kind
   byte 1: version
   byte 2: flags
   byte 3: aux
   ```

   This header has no padding and is part of the stable phase-1 on-disk contract. Prefix kind fields select the top-level decoder family; the section header provides local evolution space within that family ([C1], [C2], [U6]).
2. The row-id codec family remains speed-biased and includes dense, hole-list, hole-run, bitmap-style, and external forms. Exact thresholds remain provisional tuning parameters rather than RFC-stable constants ([C6], [U1], [U6]).
3. The delete section is domain-tagged as either `RowIdDelta` or `Ordinal`, but phase-1 writers only emit `RowIdDelta` payloads ([C2], [C5], [U6]).
4. External payload handling is generalized at the RFC level into a column-block auxiliary blob facility rather than remaining permanently deletion-specific. Phase 1 lands a minimal common auxiliary-blob header with the following stable binary contract:

   ```text
   byte 0: blob_kind
   byte 1: codec_kind
   byte 2: codec_version
   byte 3: flags
   bytes 4..7: payload_len_le_u32
   ```

   The RFC-0007 page-integrity envelope remains the integrity mechanism for blob pages themselves. The common auxiliary header is required in phase 1 even if phase-1 builders emit only delete payload blobs at first ([C3], [D11], [B1], [B2], [U6]).
5. Delete payload spills remain preferred over row-id payload spills. Row-id spill support may still arrive in a later phase if phase-1 builders do not need it, but it must reuse the phase-1 generic auxiliary-blob header rather than reopening the on-disk blob contract later ([C3], [U5], [U6]).

### 6. Lookup APIs and ownership boundaries

1. The lookup surface is split into two APIs rather than one partially populated result type:
   - `locate_block(row_id)` for checkpoint, recovery, rebuild, and infrastructure paths,
   - `resolve_row(row_id, entry_ref)` for later runtime-oriented row resolution ([C4], [C5], [C8], [C9], [U6]).
2. Phase-1 `locate_block(row_id)` returns block-location and entry-structure metadata but does not promise ordinal resolution. At minimum it must expose enough metadata for:
   - block page identification,
   - coverage tests,
   - row-id and delete section decoding,
   - deletion checkpoint and recovery merge logic in row-id space ([C5], [C8], [C9], [U6]).
3. `resolve_row` is introduced only in a later phase. Its first stable version returns membership and ordinal-oriented row resolution without delete hints. Any later negative-lookup or delete-hint fields remain separate, optional additions that require benchmark justification after runtime alignment exists ([C4], [C6], [U6]).
4. The block index may become the preferred runtime lookup authority for `RowID -> ordinal` later, but the LWC page remains an owner of persisted row-id metadata for recovery, page decoding, and other non-runtime consumers unless those consumers are redesigned separately ([C4], [C6], [C8], [C9], [U6]).

### 7. Delete-domain transition rules

1. Ordinary delete-only checkpoint rewrites do not opportunistically switch persisted delete domain from `RowIdDelta` to `Ordinal` ([C5], [U6]).
2. The stable phase rules are:
   - Phase 1: writers emit only `RowIdDelta`,
   - intermediate readers may decode both domains if needed later,
   - `RowIdDelta -> Ordinal` conversion is legal only on a block rewrite path that also has authoritative row-id mapping for that exact block version ([C5], [C6], [C8], [U6]).
3. Pending deletes always arrive as absolute row ids. If persisted state is ordinal-domain in a later phase, incoming row ids must be resolved against the authoritative row-id mapping of the same block version before merge. If that mapping is unavailable on the write path, the rewrite must not convert domains ([C5], [C6], [U6]).

### 8. Mutation model and validation behavior

1. The stable mutation model for redesigned leaves is logical-entry rebuild, not payload-byte patching:
   - decode one old leaf into logical entry descriptors,
   - apply entry-level mutations,
   - rebuild one or more new leaf pages with a page builder,
   - split by encoded byte size if needed,
   - repair parent separators as part of CoW publication ([C1], [C5], [U6]).
2. Structural and semantic validation are part of the format contract, not just builder discipline. At minimum validation must check:
   - prefix ordering by `start_row_id`,
   - adjacent leaf coverage intervals satisfy the non-overlap rule,
   - branch separator consistency with child lower bounds,
   - in-bounds and non-overlapping section ranges,
   - valid absent-section encoding,
   - compatibility between codec kinds, flags, domains, and lengths,
   - `row_count <= row_id_span`,
   - `del_count <= row_count`,
   - decoded row-id membership count equals `row_count`,
   - decoded delete membership count equals `del_count`,
   - `Dense` row-id sections imply `first_present_delta == 0` and `row_count == row_id_span`,
   - matching blob kind/version for external payload references against both the prefix and section header,
   - and leaf/LWC consistency where the leaf metadata is intended to describe stable block-internal row shape ([D11], [C3], [C6], [C8], [C9], [C11], [U6]).
3. Validation failures are corruption or format failures, not soft mismatches. They must fail the consuming checkpoint/recovery/read path consistently with the page-integrity model already established by RFC-0007 ([D11], [C8], [C9], [C11], [U6]).

### 9. Testing and delivery expectations

1. Each implementation phase must be independently testable and suitable for one or more task docs under this RFC. Later task planning may split phases further, but phase boundaries must preserve clear acceptance criteria ([D6], [D7], [U5]).
2. Supported validation for implementation work under this RFC uses `cargo nextest run -p doradb-storage`, and I/O-path changes must also validate the supported alternate `libaio` backend when relevant ([D7]).
3. If low-level page-layout work introduces or changes `unsafe` access patterns, implementation tasks must follow the repository unsafe policy, include `// SAFETY:` comments, refresh unsafe inventory, and run the required lint/test checks ([D8], [D9]).

## Alternatives Considered

### Alternative A: Canonicalize All Persisted Row-Shape Ownership in the Block Index

- Summary: Make the block index the single canonical owner of row-id translation and persisted delete interpretation, and reduce the LWC page to value storage plus generic decoding.
- Analysis: This is architecturally cleaner in the long term, but it broadens the scope far beyond the current redesign. It would couple this RFC to recovery, rebuild, and page-decoding changes that are not necessary to land the leaf-format win and would delay the initial improvement materially ([D1], [D2], [C4], [C6], [C8], [C9]).
- Why Not Chosen: The program should first redesign the leaf format and CoW mutation model while preserving phase-1 external semantics. Full ownership consolidation can be revisited later if phase-2 and phase-3 results justify it ([U1], [U5], [U6]).
- References: [D1], [D2], [C4], [C6], [U1], [U5]

### Alternative B: Narrow RFC to Phase-1 Leaf Redesign Only

- Summary: Limit the RFC to the new leaf layout and immediate CoW builder work, and leave later lookup and delete-domain evolution to separate planning docs.
- Analysis: This would reduce RFC size, but it would also leave the surrounding lookup and delete-transition contracts underdefined. The current proposal work has already identified those boundaries as the main sources of ambiguity; deferring them would push critical decisions into later task docs and make the program easier to drift ([C4], [C5], [C10], [U4], [U5]).
- Why Not Chosen: The user explicitly asked for a program-level RFC with flexible but clear phase boundaries. The selected direction is to freeze the important cross-phase contracts now while keeping the exact number of implementation tasks flexible ([U5], [U6]).
- References: [D6], [C4], [C5], [U5], [U6]

### Alternative C: Freeze the Entire End-State Binary and Runtime Contract Now

- Summary: Treat the latest proposal as the complete final binary/API contract and formalize all later-phase details immediately, including eventual runtime hints and ordinal-oriented behavior.
- Analysis: This is the most direct translation of the proposal into RFC text, but it overfreezes parts of the design that are not yet validated by phase-1 implementation. In particular, later hint fields and some runtime-oriented return shapes should stay benchmark-gated rather than becoming RFC-stable without experience from the new leaf format ([C1], [C4], [C6], [U6]).
- Why Not Chosen: The RFC should lock the durable structural contracts now and leave clearly identified later-phase details flexible enough for task-level validation and measurement ([U5], [U6]).
- References: [C1], [C4], [C6], [U5], [U6]

## Unsafe Considerations

This RFC touches persisted on-page layout and low-level page parsing, so implementation is likely to modify code in areas that currently rely on byte reinterpretation, packed layout handling, or other low-level helpers ([C1], [C2], [C3], [D8], [D9]).

The intended unsafe scope is limited:

1. No new unsafe category is justified beyond packed/binary on-page layout access and existing low-level storage helpers.
2. Safe decoding should be preferred where practical; any retained or new unsafe must be narrowly scoped behind layout helpers.
3. Modified unsafe blocks and impls must carry concrete `// SAFETY:` comments, and invariants such as offset bounds, overlap exclusion, and alignment assumptions must be enforced in code where practical.
4. Tasks under this RFC that touch unsafe code must refresh `docs/unsafe-usage-baseline.md` and run the required clippy and nextest validation checks ([D8], [D9]).

## Implementation Phases

- **Phase 1: Leaf Format and Rebuilder Foundation**
  - Scope: Introduce the redesigned leaf page format, fixed-prefix addressing model, section headers, and page-builder CoW rebuild flow while keeping branch pages fixed-width.
  - Goals: Land the new leaf contract, exact common section header, phase-1 generic auxiliary-blob header, phase-1 `locate_block` metadata surface, row-id-domain delete sections, structural plus semantic validation, and root publication under one tree-format version.
  - Non-goals: Runtime ordinal fast path, ordinal delete persistence, optional delete synopsis fields, or mandatory row-id spill support.
  - Task Doc: `docs/tasks/000095-leaf-format-and-rebuilder-foundation.md`
  - Task Issue: `#492`
  - Phase Status: done
  - Implementation Summary: 1. Implemented the RFC-0011 phase-1 leaf-format foundation in [Task Resolve Sync: docs/tasks/000095-leaf-format-and-rebuilder-foundation.md @ 2026-03-29]

- **Phase 2: Checkpoint and Recovery Alignment**
  - Scope: Align deletion checkpoint, catalog checkpoint, recovery, and related persisted-state consumers with the redesigned leaf contract while preserving row-id-domain external semantics.
  - Goals: Make the new `locate_block` contract authoritative for infrastructure paths and prove checkpoint/recovery correctness with the redesigned leaves.
  - Ordered Success Gates: deletion checkpoint, then catalog checkpoint, then recovery replay/bootstrap, then corruption-path validation.
  - Non-goals: Making the block index the preferred runtime `RowID -> ordinal` path.
  - Task Doc: `docs/tasks/000096-checkpoint-and-recovery-alignment.md`
  - Task Issue: `#495`
  - Phase Status: done
  - Implementation Summary: 1. Made the v2 column-block-index entry/decode surface authoritative for [Task Resolve Sync: docs/tasks/000096-checkpoint-and-recovery-alignment.md @ 2026-03-29]

- **Phase 3: Runtime Lookup Alignment**
  - Scope: Introduce `resolve_row`, align runtime point-read behavior with redesigned block-index row-id metadata where beneficial, and benchmark whether later negative-lookup hints are justified.
  - Goals: Allow the block index to become the preferred runtime lookup path when that produces measured benefit without invalidating existing LWC consumers; first ship `resolve_row` without delete hints.
  - Non-goals: Ordinal delete-domain conversion on delete-only rewrites.
  - Task Doc: `docs/tasks/000097-runtime-lookup-alignment.md`
  - Task Issue: `#497`
  - Phase Status: done
  - Implementation Summary: Implemented phase-3 runtime row resolution and persisted point-read alignment with focused hot-cache lookup benchmarking. [Task Resolve Sync: docs/tasks/000097-runtime-lookup-alignment.md @ 2026-03-30]

- **Phase 4: Later-Domain Evolution and Auxiliary Payload Expansion**
  - Scope: Permit ordinal-domain delete payloads on authoritative block rewrite paths if later work still justifies them, and add optional runtime lookup hints only when real builders and benchmarks justify them.
  - Goals: Land only the later-phase work that remains justified after phase-1 through phase-3 measurements.
  - Non-goals: Forcing ordinal delete persistence, row-id spill usage, or optional lookup hints without evidence.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`
  - Related Backlogs:
    - `docs/backlogs/000029-column-deletion-blob-reachability-sweep-strategy.md`
    - `docs/backlogs/000030-column-deletion-blob-reclamation-trigger-and-sla.md`

## Consequences

### Positive

- Dense blocks can materially improve leaf fanout compared with the current fixed-width payload format.
- Variable-size row-id and delete metadata get a coherent CoW rebuild model instead of stretching the old payload-patch model past its natural limits.
- The RFC freezes the important cross-phase contracts early, which lowers the chance of task-level drift when checkpoint, recovery, and runtime consumers start moving.
- Later runtime lookup improvements remain possible without redesigning the leaf format again.

### Negative

- This is a substantial persisted-format refactor rather than a local optimization.
- Phase 1 still leaves some metadata duplication between the block index and the LWC page by design.
- Updating one logical entry can require full leaf rebuild, payload repack, and occasional split plus parent repair, so write amplification and split frequency are practical costs of the new model.
- New-format-only scope means older persisted trees are not part of the supported contract for this program.
- Validation, corruption handling, and auxiliary-blob scope are more explicit and therefore broader than the current implementation surface.

## Open Questions

1. Exact phase-2 task decomposition remains open as a planning question, but any split must preserve the ordered success gates defined in this RFC.
2. Exact codec-threshold values remain benchmark-derived and are intentionally not frozen by this RFC.

## Future Work

- Any branch-page redesign beyond the current fixed-width mapping model.
- Any broader attempt to remove LWC ownership of persisted row-id data for non-runtime consumers.
- Compatibility or migration tooling for pre-redesign persisted trees if that ever becomes necessary.
- Auxiliary blob reclamation policy and SLA beyond the backlog items already tracked in [B1] and [B2].

## References

- `docs/rfcs/0002-column-block-index.md`
- `docs/rfcs/0007-disk-page-integrity-for-cow-storage-files.md`
- `docs/column-block-index-proposal-3.md`
- `docs/redesign-column-block-index-feedback-1.md`
- `docs/redesign-column-block-index-feedback-2.md`
