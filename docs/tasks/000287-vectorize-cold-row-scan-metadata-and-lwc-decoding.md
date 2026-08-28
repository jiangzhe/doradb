---
id: 000287
title: Vectorize cold-row scan metadata and LWC decoding
status: proposal
created: 2026-08-28
github_issue: 1025
---

# Task: Vectorize cold-row scan metadata and LWC decoding

## Summary

Compile each persisted LWC block into scan-ready metadata while the column
block index is already being traversed, then execute cold-row scans by ordinal
instead of rebuilding a dense `Vec<RowID>` and reopening the owning leaf for
every block. Capture the table's `ColumnDeletionBuffer` once for the scan's
fixed MVCC view, sort and merge its authoritative visibility overrides with
persisted deletes into per-block ordinal masks, and remove all deletion-buffer
and delete-set hash lookups from row advancement.

Each loaded LWC block also gains a lazy prepared-column cache. Column
boundaries, null layout, compression code, and codec metadata are validated and
prepared at most once for each touched block-column; individual rows still
decode to the existing `Val`-based callback and stream interfaces. The task
does not introduce a batch result API, materialize whole columns, or change a
durable format.

Correctness is proved across dense and sparse row sets, both persisted deletion
domains, external deletion blobs, transaction-owned and ownerless snapshots,
and checkpoint/CDB races. Deterministic work counters and a same-host,
20-sample release comparison must prove the eliminated work and material cold
sequential and parallel speedups without a meaningful hot-scan regression.

## Context

Issue Labels:

- type:perf
- priority:high
- codex

Source Backlogs:

- `docs/backlogs/000188-optimize-warm-cache-cold-row-table-scans.md`
- `docs/backlogs/000111-optimize-cold-row-visibility-filtering-mvcc-scans.md`

Task `docs/tasks/000285-parallel-scan-benchmark-performance-proof.md` created a
one-million-row, 128-byte benchmark fixture with 2,009 persisted LWC blocks and
224 hot pages. Task
`docs/tasks/000286-cache-readonly-validation-by-residency-generation.md` then
removed repeated BLAKE3 validation from warm readonly-cache hits. Its follow-up
20-run CPU-clock proof measured 71.43 ms hot and 128.55 ms cold-dominant
sequential medians. Approximately 67% of the remaining hot/cold gap was
column-index metadata work and another 14% was repeated LWC layout and decoder
construction; deletion visibility was about 2%.

The warmed cold-dominant scan currently records 4,021 readonly-cache hits with
zero misses, completed reads, or backend submissions:

- three column-index pages read during worklist capture;
- 2,009 repeated reads of owning leaf pages from
  `load_delete_deltas_and_row_ids`, including repeated prefix-plane validation;
  and
- 2,009 LWC block reads.

The scan worklist retains only scalar `ColumnLeafEntry` values, so execution
reconstructs `ColumnBlockIndex` and reopens a leaf for every LWC block even
though `collect_leaf_entries` already held and validated that leaf. The load
path then expands each entry's logical row set into `Vec<RowID>` and expands
persisted deletes into `FastHashSet<RowID>`. Finally,
`table_scan_cold_row` recovers the row ID and calls
`ColumnDeletionBuffer::get` for every row.

Row IDs are not an intrinsic input to LWC value scanning. An LWC row is
addressed by zero-based ordinal, its row count and row-shape fingerprint are
bound by the column index, and the public full-scan APIs return projected
values rather than row IDs. Cold-scan row identity has only two temporary
uses:

1. route unordered CDB markers to the captured block and translate their row
   IDs to ordinals; and
2. translate persisted `RowIdDelta` deletes, particularly a deferred external
   deletion blob, to ordinals.

A dense block needs only `start_row_id`, `row_id_span`, and `row_count` for
those translations. A sparse block needs its already encoded, sorted `u32`
deltas. Neither shape needs a dense `Vec<RowID>`, and no row-identity structure
is needed during ordinary row advancement after visibility has been compiled.

The CDB marker is newer authority than the persisted delete state. For a fixed
read view, its required override is:

| CDB state | Cold image for this read view |
| --- | --- |
| no marker | visible exactly when the persisted delete bit is clear |
| committed timestamp at or before STS | force hidden |
| committed timestamp after STS | force visible, including over a persisted delete |
| active marker owned by this transaction | force hidden |
| active foreign or ownerless marker | force visible |

This task therefore cannot treat the CDB as merely another list of deletes.
It must retain both force-visible and force-hidden ordinal overrides, apply the
persisted delete set first, and apply CDB overrides last.

The complexity gate passes as one standalone task. The changes cross the
column-index, table scan, CDB, and LWC internals, but they form one bounded cold
full-scan pipeline. They do not change a public API, durable format,
transaction/recovery protocol, or deployment contract and do not require a
parent RFC. RFC-0030 is already resolved; this task is a measured follow-up,
not a new RFC phase. If all acceptance criteria are met, `$task-resolve` should
close both source backlogs.

## Goals

1. Decode and retain all scan-required inline column-index metadata during the
   existing ordered leaf traversal, with no execution-time leaf reopen.
2. Replace dense row-ID materialization with an ordinal-oriented identity
   abstraction: allocation-free dense metadata and sorted `u32` deltas only
   where sparse translation remains necessary.
3. Traverse the CDB once per logical table-scan worklist/plan, immediately
   classify markers for the fixed read view, sort them by row ID, and merge
   them into captured blocks.
4. Normalize inline persisted deletes and CDB visibility overrides into one
   final per-block ordinal delete mask, with CDB authority applied last.
5. Preserve lazy loading for external deletion blobs while retaining only the
   metadata and minimal ordinal resolver required to finalize their masks.
6. Remove `ColumnDeletionBuffer::get`, row-ID reconstruction, and delete hash
   lookups from cold-row advancement.
7. Parse the LWC column-offset plane once per loaded block and prepare each
   touched column's null and codec metadata at most once.
8. Preserve lazy callback behavior, projection order, `Skip`/`Stop` behavior,
   row-oriented output, persisted integrity checks, and Task 000286's
   once-per-residency validation contract.
9. Add deterministic structural proof for index-page reads, CDB passes,
   row-identity allocation, visibility-mask builds, and prepared-column builds.
10. Prove at least a 25% cold sequential median latency improvement and a 15%
    cold target-capacity parallel improvement against the same `origin/main`
    baseline, with no hot sequential or parallel regression beyond 5%.

## Non-Goals

1. No durable column-index, deletion-blob, LWC, table-file, or recovery-format
   change.
2. No public table-scan API, output ordering, result type, callback contract,
   or benchmark result-schema change.
3. No Arrow/vector result batches, predicate pushdown, SIMD filtering,
   aggregation, query scheduler, or column-at-a-time public execution path.
4. No whole-column `Val` materialization; values remain decoded lazily for the
   current row.
5. No replacement of `ColumnDeletionBuffer` with a permanently ordered map or
   secondary per-block index.
6. No change to point reads, index lookups, create-index scans, mutations,
   purge, checkpoint encoding, recovery replay, or general column-index lookup
   APIs except shared parsing helpers needed to prevent validation drift.
7. No hot-row scan optimization or hot-page MVCC redesign.
8. No weakening of readonly-cache validation, persisted corruption detection,
   row-shape fingerprint checks, or external deletion-blob validation.
9. No cache residency provenance redesign or typed parsed-page cache.
10. No CI wall-clock performance threshold; timing acceptance is a recorded
    same-host release proof.

## Rejected Alternatives

1. **Cache typed parsed pages for each readonly residency generation.** A
   residency-scoped cache could serve scans and point lookups, but it would add
   validator identity, parsed-object ownership, eviction coupling, and broad
   cross-consumer cache policy. That is RFC-scale machinery. A scan-owned
   descriptor removes the measured duplicate work without changing the buffer
   pool contract.
2. **Batch entries only within the current leaf and materialize projected
   columns.** Leaf-local batching would still make execution topology depend on
   index-page grouping, complicate deterministic partition units, retain
   per-scan leaf guards, and use memory proportional to decoded values.
   Scan-ready descriptors plus lazy prepared columns preserve one-LWC-block
   units and bounded memory while addressing both measured costs.

## Plan

### Scan-ready column-index metadata

Add a scan-specific collection result in
`doradb-storage/src/index/column_block_index.rs`. The intended shape is a
`ColumnBlockScanEntry` containing the LWC `block_id`, row-ID coverage bounds,
`row_count`, `row_id_span`, `row_shape_fingerprint`, minimal row identity, and
an inline-or-external persisted delete plan. It is separate from
`ColumnLeafEntry` because point lookup and mutation callers do not need to own
scan compilation state.

Replace the cold-scan use of `collect_leaf_entries` with a method such as
`collect_scan_entries` that:

1. traverses branch and leaf pages in the same ascending logical order;
2. obtains `leaf_prefix_plane` exactly once for each visited leaf;
3. uses `leaf_entry_view_with_prefixes` exactly once for each entry;
4. decodes and validates `LogicalRowSet` plus inline delete metadata while the
   entry bytes are already borrowed;
5. retains external `BlobRef`, delete domain, count, fingerprint, and validation
   context without reading the blob eagerly; and
6. preserves overlap, range, row-count, row-shape, delete-count, section,
   checksum, and page-role integrity checks.

Point lookup and rewrite paths retain their current APIs. Shared row/delete
parsers must be factored rather than duplicated so scan compilation and point
resolution accept and reject the same persisted encodings.

The compiled cold worklist must never call
`load_delete_deltas_and_row_ids`. For the canonical 2,009-LWC fixture this
reduces warmed readonly hits from 4,021 to exactly 2,012: three planning index
pages plus one load for each LWC block.

### Minimal row identity and block descriptor

Represent scan identity conceptually as:

```text
ScanRowIdentity =
    Dense { row_id_span }
    | SparseDeltas(Arc<[u32]>)
```

The entry's start row ID supplies the base. `ordinal_for_delta` and
`delta_for_ordinal` remain the only conversions. Dense conversion is checked
arithmetic and allocates nothing. Sparse conversion uses binary search over
the sorted encoded deltas and never expands them to `RowID` values.

After CDB and inline-delete compilation, build an immutable
`ColdBlockScanDescriptor` with:

- LWC block ID, row count, coverage diagnostics, and row-shape fingerprint;
- either a ready `ColdDeleteMask` or a deferred external-delete plan;
- ordinal CDB overrides only for the deferred case; and
- a row-identity resolver only when a deferred `RowIdDelta` blob still requires
  it.

`ColdDeleteMask` has an allocation-free `AllVisible` representation and a
compact ordinal bitmap for blocks with invisible rows. A ready descriptor does
not retain CDB markers, status `Arc` values, row IDs, a row-ID hash set, or
sparse identity that has no remaining consumer.

Store cold units as cheap shared descriptors, for example
`TableScanUnit::Cold(Arc<ColdBlockScanDescriptor>)`. Adjust
`TableScanUnitCursor` and `TableScanRangeCursor` to clone only the descriptor
`Arc` instead of requiring `TableScanUnit: Copy`. The same immutable
descriptors remain shareable by every partition and repartition generation.

### One-pass CDB visibility capture

Add a bounded scan-collection operation to `ColumnDeletionBuffer` rather than
exposing its `FastDashMap`. It visits the map once, filters to the captured cold
row-ID bounds, and classifies each borrowed marker immediately against
`MvccVisibility` into `(RowID, force_visible)`. It must not retain cloned
`SharedTrxStatus` values after classification. Sort the compact results by row
ID after the map pass.

Pass the already established `MvccReadView` into cold-worklist preparation:

- `TableScanMvccStream` constructs its transaction read view before descriptor
  compilation; and
- `ReadSnapshot::prepare_table_scan` uses the ownerless read view pinned by its
  checkout.

Merge sorted CDB overrides and sorted scan entries with a two-pointer sweep.
For each matching entry, translate the row-ID delta to an ordinal through
`ScanRowIdentity`. Ignore markers outside the captured cold row sets; the
captured root and row shape remain authoritative for this plan.

The map traversal is not required to be a globally locked instant. Document
and test why immediate classification is equivalent to later per-row lookups
for a fixed STS:

1. a marker inserted after the read STS is either foreign-active or can only
   commit at a timestamp newer than the STS, so both the absent and observed
   states force the cold image visible;
2. rollback of a foreign-active marker changes visible to absent over a
   non-durable row, which is also visible;
3. compaction from `Ref` to `Committed` retains the same timestamp result;
4. a marker required to override a persisted delete for an older reader cannot
   be purged while that reader's active snapshot remains in the GC horizon;
5. publication installs required transition markers before exposing the root
   state that depends on them; and
6. a transaction-owned active marker cannot concurrently become terminal while
   its scan stream holds the transaction operation borrow.

If implementation research disproves any of these existing invariants, stop
and revise the design rather than adding an unsound weak snapshot or a global
CDB lock.

### Persisted deletion normalization

Normalize persisted deletion membership to LWC ordinals:

- `Ordinal` payloads validate directly against `row_count`;
- `RowIdDelta` payloads translate once through `ScanRowIdentity`;
- no-delete blocks remain `AllVisible` without allocating a bitmap; and
- inline deletes create at most one final bitmap for the block.

Apply the durable base first. Then apply every CDB override: force-hidden sets
the ordinal bit and force-visible clears it. This ordering preserves the CDB's
ability to keep an old reader's cold image visible after that delete has
entered persisted state.

External deletion blobs remain lazy so a transaction callback that stops early
does not read metadata for unreached blocks. A deferred descriptor carries its
validated blob reference, domain, declared delete count, row-shape binding,
already translated ordinal CDB overrides, and only the identity resolver
needed by its domain. `load_table_scan_cold_page` reads and validates the blob,
normalizes it to ordinals, applies the saved overrides, and finalizes exactly
one mask without reopening the column-index leaf.

### Cold scan loading and advancement

Change `TableScanWorklist`, `CompiledTableScanPlan`, and `TableScanUnit` to
carry cold block descriptors instead of `ColumnLeafEntry`. Preserve the
existing cold-before-hot ordering, normalized weights, compact partition
offsets, plan family gates, resource-free plan ownership, and cancellation
behavior.

Narrow `load_table_scan_cold_page` to the descriptor. It loads an external
delete blob only when deferred, loads the LWC block through Task 000286's
validated readonly path, checks row count and row-shape fingerprint against the
descriptor, and constructs `TableScanColdPage`. It does not reconstruct
`ColumnBlockIndex` or receive column-root/pivot scalars for metadata lookup.

`TableScanColdPage` owns the persisted LWC guard, final ordinal visibility
mask, prepared-column cache, file kind, and block ID. It owns no `Vec<RowID>`,
`FastHashSet<RowID>`, CDB reference, or transaction status.

`table_scan_cold_row` tests the ordinal mask and constructs the existing
`LazyRow`. The cold branch performs no MVCC lookup after page construction;
`MvccReadView` remains in `TableScanCursor::advance` only for hot-row
visibility. Projection, callback access, `Include`, `Skip`, `Stop`, buffer
reset, stream abandonment, and partition failure behavior remain unchanged.

### Lazy prepared LWC columns

Add an owned, offset-based prepared decoder representation in
`doradb-storage/src/lwc/block.rs` and `doradb-storage/src/lwc/mod.rs`. A loaded
`TableScanColdPage` parses and validates the block's column-end offset plane
once. Cursor-exclusive optional slots lazily prepare a column the first time a
projection or callback touches it.

A prepared column records owned scalar metadata and checked relative ranges
for its null bitmap, value payload, compression code, element count,
bit-packing width/base, or VarByte offset/data regions. Decode methods borrow
the immutable LWC bytes for the duration of one call. They must not store
self-references into `PersistedLwcBlock`, widen guard lifetimes, or add unsafe
code.

Factor the codec parser so `LwcData::from_bytes` and the prepared representation
share the same length, type, alignment, compression-code, bit-width, and
offset validation. Existing non-scan LWC callers keep their behavior. Invalid
metadata remains a `DataIntegrityError` with file kind and block ID attached,
and an untouched column is not eagerly decoded or materialized.

`LazyRowSource::Cold` reads through the page's prepared decoder cache. Across
all rows in a block, each touched column is prepared at most once, while each
requested row value is still decoded independently into `Val` and cached only
for that callback invocation.

### Deterministic work proof and documentation

Add focused `cfg(test)` diagnostics or equivalent deterministic hooks that do
not affect release builds. Tests must be able to assert:

- one read of each captured column-index page during planning and zero leaf
  reads during cold unit execution;
- one bounded CDB map pass per logical worklist/plan and zero
  `ColumnDeletionBuffer::get` calls during row advancement;
- zero dense row-ID-list allocations and no runtime row-ID set;
- at most one visibility-mask finalization per block;
- at most one prepared decoder per touched block-column; and
- no preparation for untouched columns.

Update `docs/block-index.md` with ordinal scan identity, persisted/CDB
precedence, and deferred external-delete behavior. Update
`docs/benchmark-tool.md` with the exact benchmark commands, fixture shape,
baseline/candidate SHAs, cache/work counters, sampling protocol, summary
statistics, and CPU profile attribution.

### Performance acceptance

Use separate fresh roots from the same checked-out baseline and candidate on
one host. Pin the exact `origin/main` baseline SHA before measurement. Build
both in release mode, use the Task 000285 one-million-row 128-byte hot and
cold-dominant fixtures, projection `[0, 1]`, and identical engine, worker, and
table-scan configuration.

For each of hot sequential, hot target-capacity parallel, cold sequential, and
cold target-capacity parallel:

1. run one unmeasured warm-up;
2. collect 20 independent measured runs;
3. verify exact row-count equations and zero measured warm-cache misses,
   completed reads, and backend submissions;
4. report median latency/throughput, IQR, and median absolute deviation; and
5. retain raw samples and profiler commands in the task's implementation
   evidence.

The candidate passes only when:

- canonical cold sequential median latency improves by at least 25%;
- canonical cold target-capacity parallel median latency improves by at least
  15%;
- hot sequential and target-capacity parallel median latency each regress by
  no more than 5%;
- the canonical 2,009-LWC warmed cold scan records exactly 2,012 readonly hits;
  and
- CPU-clock profiles show the repeated leaf-prefix/row-metadata work removed
  and LWC parser construction amortized, rather than attributing the speedup to
  changed I/O, row counts, or benchmark configuration.

## Implementation Notes

Implemented the ordinal cold-scan pipeline without changing public or durable
formats. Column-index traversal now emits scan-ready entries while each leaf is
resident. Dense identity is span-only, sparse identity retains encoded `u32`
deltas, inline deletes are normalized immediately, and external blobs remain
lazy with only their required resolver. One sorted CDB capture is merged into
the entries for the fixed read view, with force-visible/force-hidden overrides
applied after persisted state. Ready descriptors drop row identity and CDB
state; cold advancement checks only an ordinal bitmap.

Cold scan units are shared `Arc` descriptors and cursors clone those descriptors
instead of copying leaf entries. Loading a cold unit no longer constructs a
`ColumnBlockIndex` or reopens its leaf. Each loaded LWC page parses its offset
plane once and owns an optional prepared value per column. The cursor lends the
cache exclusively to each lazy row, so first access validates and caches owned
null/codec/range metadata without interior synchronization; later rows decode
directly from the guarded immutable bytes. General `LwcData::from_bytes` remains
the shared codec validator, and untouched columns are not prepared.

The deterministic warm-cache test's one-leaf/one-LWC fixture records exactly
two hits, one CDB pass, and zero per-row CDB gets. The canonical release fixture
records exactly 2,012 hits in all 20 candidate cold runs, versus 4,021 baseline
hits, with zero misses, completed reads, or backend submissions. CPU-clock
profiles remove the baseline 10.54% `leaf_prefix_plane` symbol and replace
per-row parser construction with prepared value decoding.

Final verification completed:

- `rtk cargo nextest run --workspace`: 1,832 tests passed.
- `rtk cargo nextest run -p doradb-storage --no-default-features --features
  libaio`: 1,741 tests passed.
- Strict workspace and alternate-`libaio` Clippy passed with warnings denied;
  `cargo deny` passed its advisory, ban, license, and source gates with only the
  repository's existing duplicate-version warnings.
- `tools/style_audit.rs`: all 12 branch-diff Rust files passed.
- Focused coverage across the seven changed runtime files was 91.27%; every
  file exceeded 87%, and scan plan/cursor coverage was 100%/98.37%.

Same-host release medians passed every timing gate against exact `origin/main`
`b58f2192486a1677b9d88aef5c7ef579c281eb94`: cold sequential improved 38.51%,
cold target-nine improved 42.55%, hot sequential improved 0.19%, and hot
target-nine regressed 4.97%. Detailed IQR, MAD, cache, command, fixture, and
profile evidence is recorded in `docs/benchmark-tool.md`.

Raw elapsed-nanosecond samples, in run order:

- Baseline hot sequential: 75133840, 75455824, 75066501, 75482617,
  76275848, 75289019, 75301728, 75066084, 75298687, 75431780, 75179385,
  77034827, 75866191, 75957073, 75026122, 75911569, 75959573, 75206387,
  75476867, 74935948.
- Candidate hot sequential: 78966066, 84993289, 75489283, 76351000,
  74946779, 74918862, 76440001, 74863653, 74302898, 75012196, 74681568,
  75147448, 75036029, 75091030, 74667776, 75893995, 75300157, 110743486,
  110274648, 82932019.
- Baseline hot target-nine: 20482357, 20943819, 19629891, 23614136,
  18920134, 18597798, 23527343, 18052625, 19534848, 26457454, 17974708,
  19302720, 18794007, 24475018, 17929708, 18983843, 19251012, 24540103,
  18704674, 19631849.
- Candidate hot target-nine: 19048746, 19589709, 24705961, 23760162,
  22494859, 20805470, 23336116, 24663794, 24273291, 17996154, 19383999,
  19894378, 18284865, 21147472, 21406725, 17812235, 19960504, 17759735,
  17605567, 26880272.
- Baseline cold sequential: 131636277, 134626744, 132250694, 132347230,
  131818598, 132186824, 135225704, 132058915, 134738903, 130444732,
  130408567, 130870912, 130862620, 130547725, 130501228, 131180807,
  130785834, 130921658, 130986987, 130951657.
- Candidate cold sequential: 81002883, 81127633, 80862215, 80427212,
  80490795, 80406129, 80642422, 80124626, 80231085, 80354711, 80484295,
  80428295, 80914091, 93195937, 80728048, 80791714, 80748964, 80858090,
  80560379, 80322294.
- Baseline cold target-nine: 33496019, 41127340, 38277605, 39451574,
  37199345, 38706442, 37186553, 39846453, 34917157, 36990093, 36785466,
  36598339, 39311657, 37486389, 35501829, 38398065, 39406990, 37315971,
  35314661, 36822508.
- Candidate cold target-nine: 23823227, 23078597, 21758337, 22081505,
  20864121, 19765988, 21257458, 19733946, 20446618, 19780196, 24492358,
  21550168, 19822530, 23477808, 23487891, 20367992, 26441289, 20870913,
  21858046, 20423035.

## Impacts

- `doradb-storage/src/index/column_block_index.rs` and
  `doradb-storage/src/index/mod.rs`: scan-specific metadata collection, minimal
  row identity, persisted delete plans, and shared parsing/validation.
- `doradb-storage/src/table/deletion_buffer.rs`: bounded, sorted,
  immediately-classified scan visibility collection.
- `doradb-storage/src/table/access.rs`: descriptor compilation, ordinal delete
  masks, cold-page ownership, and row advancement.
- `doradb-storage/src/table/scan_plan.rs` and
  `doradb-storage/src/table/scan_cursor.rs`: shared cold descriptors and
  non-`Copy` unit traversal.
- `doradb-storage/src/table/partition_stream.rs`: owned partition cursor
  construction over shared descriptors.
- `doradb-storage/src/trx/stream_stmt.rs` and
  `doradb-storage/src/trx/read_snapshot.rs`: read-view-aware worklist
  preparation for transaction and ownerless snapshot scans.
- `doradb-storage/src/lwc/block.rs` and `doradb-storage/src/lwc/mod.rs`:
  prepared offset/null/codec metadata and shared validation.
- `docs/block-index.md` and `docs/benchmark-tool.md`: durable design and
  reproducible performance evidence.
- Public APIs, result values, persisted files, recovery behavior, and hot-row
  scan semantics are unchanged.

## Test Cases

1. Dense entries compile without a per-row identity allocation; sparse entries
   retain sorted deltas, translate valid row IDs to exact ordinals, and reject
   malformed, duplicate, unsorted, missing, or out-of-range deltas as today.
2. No-delete, inline-delete, and external-delete entries produce the same
   visible rows for `RowIdDelta` and `Ordinal` domains, including empty, first,
   last, all-deleted, and sparse-hole cases.
3. Declared row/delete counts, ranges, block IDs, section versions, row-shape
   fingerprints, blob references, blob fingerprints, and ordinal bounds retain
   their corruption failures and error context.
4. The complete CDB truth table is tested with and without a persisted delete:
   absent, old/new compact committed, old/new committed `Ref`, own active,
   foreign active, and ownerless active markers.
5. Applying a force-visible CDB override clears a durable delete bit, while an
   old committed or own-active override sets it; ordering is invariant across
   dense and sparse blocks.
6. A foreign active delete that commits after descriptor capture remains
   visible to the fixed reader. A foreign active delete that rolls back after
   capture also remains visible. Tests use explicit synchronization, not
   sleeps.
7. A transaction's own active delete is hidden, and operation ownership proves
   it cannot terminally transition while the stream is live.
8. An old reader remains visible across checkpoint publication of its delete,
   external-delete persistence, and attempted CDB purge. A newer reader sees
   the row hidden.
9. Checkpoint pivot/root publication racing before, during, and after
   descriptor capture yields a root-consistent result and never loses the CDB
   override required by the captured root.
10. Transaction `TableScanMvccStream` and ownerless
    `ReadSnapshot`/`TableScanPartitionStream` return equivalent rows for the
    same STS and physical root.
11. Repartitioning and opening multiple partitions reuse the immutable
    descriptors and the one CDB capture; every physical unit is consumed once
    with no duplicated or omitted rows.
12. Sequential and parallel scans preserve cardinality and values across hot,
    mixed, and cold-dominant tables, including sparse cold blocks and deletion
    overlays.
13. Programmable transaction callbacks preserve arbitrary lazy column access,
    repeated access, projections, projection order, `Include`, `Skip`, `Stop`,
    early drop, cancellation, and error cleanup.
14. Every supported `ValKind`, nullable/non-nullable layout, Flat encoding,
    supported FOR bit width/type, VarByte offsets, null placement, first/last
    ordinal, and repeated value decode matches `LwcData::from_bytes`.
15. Prepared-column tests reject bad column offsets, short null bitmaps,
    invalid compression codes, unsupported type/codec combinations, malformed
    lengths, bad bit widths, truncated payloads, and bad VarByte offsets.
16. A touched block-column prepares exactly once across all of its rows;
    repeated callback and projection access reuses it, while untouched columns
    prepare zero times and no whole-column `Val` vector is allocated.
17. The canonical fixture proves three planning index-page hits, zero
    execution leaf hits, 2,009 LWC hits, and exactly 2,012 total warmed readonly
    hits with zero misses, reads, and submissions.
18. Deterministic diagnostics prove one CDB pass, zero per-row CDB gets, zero
    dense row-ID-list allocations, one mask finalization per block, and one
    prepared decoder per touched block-column.
19. Task 000286 residency tests still prove validation on first load and after
    invalidation/eviction/reuse, with zero repeated checksum validation for the
    matching warm residency. Column-index, LWC, and deletion-blob corruption
    still fails before unsafe use.
20. The same-host performance matrix satisfies all cold improvement, hot
    regression, cache-count, statistics, and profile-attribution gates.
21. `rtk cargo nextest run --workspace` and
    `rtk cargo nextest run -p doradb-storage --no-default-features --features
    libaio` pass.
22. `rtk cargo fmt`, strict default and `libaio` Clippy, `cargo deny` where
    required by the repository gate, and `tools/style_audit.rs --diff-base
    origin/main` pass.
23. `tools/coverage_focus.rs` reports at least 80% focused line coverage for
    the changed runtime paths, or the implementation record explains any
    definition-heavy exception and cites covered consumers.

## Open Questions

None. A true batch/vector result interface, predicate pushdown, SIMD filtering,
and aggregation could exploit the same ordinal descriptors in a future
RFC-scale program, but they are neither required for this row-oriented
optimization nor authorized by this task.
