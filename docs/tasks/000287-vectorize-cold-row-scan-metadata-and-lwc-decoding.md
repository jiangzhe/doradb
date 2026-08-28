---
id: 000287
title: Vectorize cold-row scan metadata and LWC decoding
status: implemented
created: 2026-08-28
github_issue: 1025
---

# Task: Vectorize cold-row scan metadata and LWC decoding

## Summary

Cold full-table scans now compile scan-ready metadata while the column-block
index leaf is already resident. Execution addresses LWC rows by ordinal, uses
a precompiled visibility mask, and no longer reopens the owning leaf, expands
dense row IDs, builds a row-ID delete set, or queries the deletion buffer for
each row.

Each loaded LWC page parses its column-offset plane once. Touched columns
lazily cache validated null, range, and codec metadata in cursor-exclusive
optional slots, while individual values remain decoded on demand through the
existing row-oriented `Val` callback and stream interfaces.

The implementation preserves public APIs and durable formats. Against the
pinned `origin/main` baseline, the canonical release proof improved cold
sequential latency by 38.51% and cold target-nine latency by 42.55%, with hot
regressions remaining within the 5% acceptance bound.

## Context

Issue Labels:

- type:perf
- priority:high
- codex

Source Backlogs:

- `docs/backlogs/000188-optimize-warm-cache-cold-row-table-scans.md`
- `docs/backlogs/000111-optimize-cold-row-visibility-filtering-mvcc-scans.md`

Task `000285` established a one-million-row, 128-byte benchmark fixture with
2,009 persisted LWC blocks and 224 hot pages. Task `000286` removed repeated
full-block checksum validation from warm readonly-cache hits, leaving repeated
column-index metadata reconstruction and per-value LWC parser construction as
the dominant cold-scan costs.

The pre-task warmed cold scan recorded 4,021 readonly hits with zero misses or
physical reads: three planning index pages, 2,009 execution-time leaf reopens,
and 2,009 LWC page loads. Execution also expanded logical row sets into dense
`Vec<RowID>` values, expanded deletes into a row-ID hash set, and consulted the
`ColumnDeletionBuffer` for every row.

LWC values are intrinsically addressed by ordinal. Row identity is needed only
temporarily to route deletion-buffer markers and translate persisted
`RowIdDelta` deletes. Dense blocks therefore need span metadata only; sparse
blocks retain their already sorted encoded deltas.

Deletion-buffer state is newer authority than persisted deletion state for a
fixed MVCC view. Old committed and reader-owned active markers force a row
hidden. Newer committed, foreign-active, and ownerless-active markers force it
visible, including over an already persisted delete. Persisted deletes must
therefore be applied first and deletion-buffer overrides last.

This work is a measured follow-up to resolved RFC-0030, not an RFC phase. It
does not change public contracts, transaction protocols, recovery, or durable
storage layouts.

## Goals

1. Capture all scan-required inline column-index metadata during ordered leaf
   traversal and eliminate execution-time leaf reopens.
2. Represent dense row identity without per-row allocation and retain sparse
   deltas only while ordinal translation requires them.
3. Traverse and classify the deletion buffer once for the scan's fixed read
   view, then merge sorted overrides into captured blocks.
4. Compile persisted deletes and authoritative visibility overrides into one
   ordinal mask per block.
5. Keep external deletion blobs lazy while retaining only the metadata and
   resolver required when their block is reached.
6. Remove row-ID reconstruction, delete hash probes, and per-row deletion-
   buffer lookups from cold advancement.
7. Parse LWC block layout once and prepare each touched column at most once
   without materializing whole columns.
8. Preserve snapshot semantics, lazy callback behavior, integrity validation,
   partition behavior, output ordering, and hot-scan performance.

## Non-Goals

1. No durable column-index, deletion-blob, LWC, table-file, or recovery-format
   change.
2. No public scan API, callback, projection, ordering, or result-type change.
3. No Arrow or vector result batches, predicate pushdown, SIMD filtering,
   aggregation, or column-at-a-time public execution path.
4. No whole-column `Val` materialization.
5. No permanent ordered index for `ColumnDeletionBuffer`.
6. No point-read, index-scan, create-index, mutation, purge, checkpoint, or
   recovery redesign.
7. No hot-row storage or hot-page MVCC optimization.
8. No weakening of readonly-cache validation or persisted corruption checks.

## Rejected Alternatives

1. **Residency-scoped typed parsed-page cache.** This could share parsed pages
   across consumers, but would require validator identity, parsed-object
   ownership, eviction coupling, and broader cache policy. Scan-owned metadata
   removes the measured duplicate work without changing buffer-pool contracts.
2. **Leaf-local batches with materialized projected columns.** This would tie
   execution and partitioning to leaf topology and consume memory proportional
   to decoded values. Immutable per-LWC descriptors and lazy prepared columns
   preserve bounded, independently partitionable units.

## Plan

### Scan metadata and identity

`ColumnBlockIndex::collect_scan_entries` traverses index pages in logical order
and decodes each leaf entry while its prefix plane is resident. A
`ColumnBlockScanEntry` retains the LWC block ID, row coverage, row count,
row-shape fingerprint, minimal identity, and persisted delete plan.

`ScanRowIdentity` has two forms: allocation-free dense span metadata and
shared sorted sparse `u32` deltas. Checked dense arithmetic or sparse binary
search translates temporary row-ID deltas to ordinals. Point and mutation
paths retain their existing entry-resolution APIs.

Inline deletes are validated and normalized during planning. External deletes
retain their blob reference, domain, count, fingerprint, and only the identity
resolver needed for later translation; the blob remains unread until its LWC
unit is loaded.

### Visibility and descriptors

Worklist preparation receives the scan's already fixed `MvccReadView`.
`ColumnDeletionBuffer::collect_cold_visibility_overrides` performs one bounded
map pass, immediately classifies matching markers as force-visible or
force-hidden, and sorts the compact results without retaining transaction
status objects.

A two-pointer merge routes those overrides into ordered scan entries.
`ColdDeleteMask` applies persisted deletes first and CDB overrides last. Its
`AllVisible` form allocates no bitmap; other blocks use ordinal bitmaps.

`ColdBlockScanDescriptor` retains immutable execution metadata. Ready
descriptors discard row identity and CDB state. Deferred external-delete
descriptors retain only translated overrides and any resolver their delete
domain still needs. `Arc` descriptors are shared safely across scan plans,
partitions, and repartition generations.

### Cold execution and LWC decoding

Loading a cold unit resolves a deferred deletion blob when present, loads the
validated LWC page, checks row count and row-shape binding, and creates the
final page state. It never reconstructs `ColumnBlockIndex` or reopens a leaf.
Cold row advancement checks only the ordinal delete mask before constructing
the existing `LazyRow`.

`PreparedLwcBlock` parses the column-offset plane once. Each column has an
optional owned `PreparedLwcColumn` containing checked null and value ranges
plus `PreparedLwcData` codec metadata. The cursor lends the cache mutably to
one lazy row at a time, avoiding synchronization while retaining lazy
arbitrary callback access. Untouched columns remain unprepared.

Prepared and ordinary LWC decoding share `LwcData::from_bytes` validation.
Decode calls borrow immutable persisted bytes only for that call and store no
self-references or widened guard lifetimes.

### Correctness invariants

Deletion-buffer classification is safe for a fixed read timestamp because a
later foreign marker can only remain active, roll back to the same visible
base, or commit after that timestamp. Required force-visible markers cannot be
purged while the older snapshot remains in the GC horizon, publication orders
markers before dependent roots, and transaction operation ownership prevents
an owned active marker from becoming terminal during its scan.

Cold descriptors remain bound to the captured root and row shape. The existing
cold-before-hot ordering, unit weights, partition gates, cancellation cleanup,
projection order, callback `Include`/`Skip`/`Stop` behavior, and error context
remain unchanged.

## Implementation Notes

The ordinal cold-scan pipeline shipped as designed. Planning emits scan-ready
entries while each leaf is resident, captures the CDB once, and compiles final
visibility before execution. Dense scans allocate no row-ID list, ready blocks
retain no row identity, and cold advancement performs no MVCC or hash lookup.

External deletion blobs remained lazy. Inline and external `Ordinal` and
`RowIdDelta` domains use the same precedence and validation rules, with CDB
force-visible and force-hidden overrides applied after the persisted base.

The original prepared-column prototype used an interior one-time cell. Final
review replaced it with cursor-exclusive `Option<PreparedLwcColumn>` slots and
mutable lazy decoding. This preserves untouched-column laziness and arbitrary
callback access while avoiding synchronization and retaining `Send` partition
streams.

Deterministic warm-cache tests record one planning index hit and one LWC hit in
the one-block fixture, one CDB pass, zero per-row CDB gets, and no execution
leaf reopen. The canonical 2,009-LWC fixture records exactly 2,012 hits in each
candidate cold run, versus 4,021 baseline hits, with zero misses, completed
reads, or backend submissions.

Release measurements used 20 samples per shape against exact `origin/main`
`b58f2192486a1677b9d88aef5c7ef579c281eb94`. Cold sequential improved 38.51%
and cold target-nine improved 42.55%; hot sequential improved 0.19% and hot
target-nine regressed 4.97%. `docs/benchmark-tool.md` retains the fixture,
commands, statistics, cache equations, and CPU-profile attribution.

CPU-clock profiles removed the baseline 10.54% direct
`ValidatedColumnBlockNode::leaf_prefix_plane` attribution from the candidate
report and shifted LWC work to prepared value decoding. This confirms the
speedup came from eliminating metadata reconstruction rather than changed I/O
or row counts.

Final verification passed 1,832 workspace tests and 1,741 alternate-`libaio`
tests. Strict workspace and alternate-backend Clippy, `cargo deny`, formatting,
the 12-file branch-diff style audit, and focused coverage all passed. Focused
coverage across seven changed runtime files was 91.25%, with every file above
87%.

## Impacts

- Column-index scans now expose scan-specific entries with minimal temporary
  identity and lazy external-delete plans.
- Table scans compile fixed-view CDB visibility and persisted deletes into
  immutable ordinal descriptors shared by sequential and parallel cursors.
- Cold pages own only the validated LWC guard, final delete mask, and lazy
  prepared decoder cache required for execution.
- Prepared LWC decoding amortizes offset, null-layout, and codec parsing while
  preserving per-row `Val` decoding.
- `docs/block-index.md` documents identity, visibility precedence, and deferred
  blob behavior; `docs/benchmark-tool.md` records reproducible release proof.
- Public APIs, durable data, recovery, transaction semantics, and operational
  configuration are unchanged.

## Test Cases

1. Dense and sparse identity translate valid deltas to exact ordinals without
   dense row-ID allocation and reject malformed or out-of-range metadata.
2. No-delete, inline-delete, and external-delete blocks preserve visibility for
   both persisted delete domains, sparse holes, and boundary ordinals.
3. Persisted counts, ranges, fingerprints, offsets, blob references, codecs,
   and row-shape bindings retain corruption failures and file/block context.
4. The complete CDB truth table is covered with and without a persisted delete,
   including owned, foreign, ownerless, old committed, and new committed state.
5. Fixed-view capture remains correct across foreign commit/rollback, owned
   active deletion, checkpoint publication, purge attempts, and old/new readers.
6. Transaction and ownerless snapshot scans return equivalent cold/hot results;
   partitioning and repartitioning consume every shared descriptor exactly once.
7. Lazy callbacks preserve arbitrary filter-only column access, repeated reads,
   projection order, skip, stop, early drop, cancellation, and error cleanup.
8. Prepared decoding matches ordinary decoding for every supported `ValKind`,
   null layout, Flat encoding, FOR bit width/type, and VarByte representation.
9. Each touched block-column prepares once across rows, untouched columns
   prepare zero times, and no whole-column `Val` vector is allocated.
10. Deterministic diagnostics prove one CDB pass, zero row-time CDB gets, zero
    dense identity allocations, bounded mask construction, and no leaf reopen.
11. The canonical warm fixture proves exactly three planning index hits plus
    2,009 LWC hits, correct cardinality, and zero measured physical I/O.
12. Workspace, alternate `libaio`, strict lint, style, coverage, and same-host
    release performance gates pass.

## Open Questions

None. A public vector result interface, predicate pushdown, SIMD filtering, or
aggregation would require a separate RFC-scale program and is not deferred by
this task.
