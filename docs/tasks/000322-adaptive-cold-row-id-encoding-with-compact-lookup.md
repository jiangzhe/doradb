---
id: 000322
title: Adaptive cold RowID encoding with compact lookup
status: proposal
created: 2026-09-30
github_issue: 1129
---

# Task: Adaptive cold RowID encoding with compact lookup

## Summary

Encode each LWC block's authoritative RowID set using the smallest eligible
whole-entry or segmented representation, with direct membership, ordinal
translation, and ordered iteration over validated compact bytes. Use the
logical ranges already known during LWC packing to seed segments, then merge
adjacent segments when their complete encoded cost permits it. Preserve the
existing index ownership, deletion, and publication contracts. The approved
follow-up replaces the canonical row-shape fingerprint with a fixed-input
`block_binding_value: u64`.

This task includes exact sizing, compact runtime access, cache-admission
validation, and a typed capacity failure. General identity-aware LWC splitting
and external identity storage remain deferred.

## Context

Source Backlogs:

- docs/backlogs/000201-adaptive-cold-row-identity-encoding.md

Issue Labels:

- type:task
- priority:medium
- codex

This is a standalone task. RFCs 0011 and 0012 define existing implemented
contracts; neither is an active parent phase for this work. The bounded change
extends the row-section codec mechanism and replaces the index/LWC binding
metadata without changing transaction ownership or root publication. The binding
change shrinks both headers and requires fresh storage; old fingerprint-format
blocks are rejected rather than migrated.

### Current behavior

Research and the checkpoint experiment used commit
`1bee69a8ff69ba2ad1e38ef6e985cefd8f63d08c`.

- `ColumnBlockIndex` owns cold membership and maps RowIDs to LWC ordinals. An
  entry covers a logical range that can include absent rows and unused page
  tails. Dense encoding requires every position in that coverage to be present;
  otherwise its row section is a four-byte header plus one u32 per present row.
- `LwcBuilder` packs values from row-page views, retaining the selected RowIDs.
  Its size estimate covers the LWC values block, not the index row section.
  Multiple source pages can contribute to one LWC block.
- `LogicalRowSet` and sparse `ScanRowIdentity` materialize delta vectors.
  Point resolution decodes the logical row set, while leaf-prefix access also
  repeats full validation. Compressing disk bytes alone would leave these costs.
- The leaf writer can split between entries, but an oversized individual entry
  reaches an assertion. Existing external blobs store deletes, not identity.

### Checkpoint evidence

Nine release-mode engine runs inserted 100,000 sequential u64 keys in one
session, with 1,000 rows per insert transaction and no secondary indexes. For
each fixture, separate fresh engines checkpointed with 0%, 1%, and 10% committed
deletes. Exactly 1,000 or 10,000 keys were selected by deterministic hash order;
the smaller delete set is a subset of the larger one. Source-page allocations
matched across delete rates. Every persisted RowID, ordinal, key, value, and
LWC/index fingerprint was checked.

The fixtures were:

| Fixture | Values besides the key | Source pages | LWC blocks at 0% / 1% / 10% deletes |
| --- | --- | ---: | --- |
| Uniform | 128-byte payload | 224 | 224 / 224 / 224 |
| Variable | 16..240-byte payload, mean 128.11936 bytes | 292 | 291 / 291 / 278 |
| Compressible | 16 u64 columns containing key % 16, plus 16..64-byte payload | 338 | 93 / 93 / 84 |

The uniform fixture mostly puts one source page in each LWC block. The
compressible fixture exposes the multi-page case: insert-only blocks contain
3..5 source pages, averaging 3.63, but typically only two contiguous present
runs because adjacent pages can have no unused gap between them.

For example, insert-only LWC block 2 covers `[0, 1059)`:

| Source page | Reserved RowID range | Present RowID range | Unused tail |
| --- | --- | --- | ---: |
| 1 | [0, 363) | [0, 319) | 44 |
| 2 | [363, 696) | [363, 696) | 0 |
| 3 | [696, 1059) | [696, 1026) | 33 |

Its 982 rows form `[0, 319)` and `[363, 1026)`. With 1% deletion the corresponding
block retains 966 rows in 18 runs. The indexed whole-run estimate is 116 bytes;
two local missing-offset segments need 72 bytes, including the directory. With
10% deletion, packing changes: the first block covers four pages in `[0, 1422)`,
and a whole bitmap is smaller than the segmented estimate, 204 versus 246 bytes.

The following totals estimate row-section bytes on the captured LWC partitions.
They include headers, segment directories, run ordinal prefixes, bitmap rank
prefixes, and word rounding. The segmented experiment trims used page ranges
and coalesces adjacent inserted ranges without an unused reserved gap. It does
not implement the general greedy merge policy below. Missing-offset codecs are
enabled; "Hybrid" chooses the smaller estimate independently for each block.

| Fixture | Deletes | Current | Whole-entry adaptive | Segmented | Hybrid |
| --- | ---: | ---: | ---: | ---: | ---: |
| Uniform | 0% | 1,280 | 904 | 5,376 | 904 |
| Uniform | 1% | 389,728 | 3,780 | 7,362 | 3,780 |
| Uniform | 10% | 360,896 | 15,190 | 18,766 | 15,190 |
| Variable | 0% | 181,108 | 2,108 | 6,984 | 2,108 |
| Variable | 1% | 386,080 | 4,758 | 8,972 | 4,758 |
| Variable | 10% | 361,112 | 18,496 | 21,386 | 17,750 |
| Compressible | 0% | 384,788 | 1,910 | 3,992 | 1,910 |
| Compressible | 1% | 396,372 | 7,492 | 6,016 | 5,882 |
| Compressible | 10% | 360,336 | 16,278 | 18,230 | 16,274 |

In the compressible 1% case, segmentation wins on 76 of 93 blocks and the hybrid
estimate is 21.5% smaller than whole-entry adaptive encoding. Disabling the
missing-offset candidate changes the comparison to 7,884 whole-entry bytes
versus 11,118 segmented bytes. Both local hole encoding and complete metadata
cost therefore matter. Small inter-page gaps alone do not settle the choice.
The compressible fixture's current index occupies seven leaves and one branch
at 0% and 1%, or six leaves and one branch at 10%; each adaptive estimate fits
one leaf. These are index estimates, not reductions in LWC values storage.

No proposed codec was implemented or timed during this experiment. Concurrent
diagnostic runs cannot establish checkpoint or query performance. Recompute
estimates from the implemented serializer and measure CPU and allocations.

For reproduction, payloads start with the little-endian key and contain `x` in
the remaining bytes. Variable length is `16 + (H(key ^ 0xa5a5a5a5) % 8) * 32`;
compressible length is `16 + (H(key ^ 0xa5a5a5a5) % 4) * 16`. The hash uses u64
wrapping arithmetic:

```text
z = key + 0x2026093000020101 + 0x9e3779b97f4a7c15
z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
z = (z ^ (z >> 27)) * 0x94d049bb133111eb
H(key) = z ^ (z >> 31)
```

Delete keys with the smallest `(H(key), key)` tuples, commit, freeze all rows,
and call `checkpoint_table_with_wait`. Use fresh storage, 64 MiB data and
readonly buffers, 32 MiB index and metadata buffers, file I/O depth 16, log I/O
depth 1, and one purge thread. Capture source reserved ranges, present RowIDs,
and persisted entry ranges before checking the decoded values.

Local research artifacts are in the dispatch checkout's ignored
`target/rowid-study-20260930/`: `manifest.json`, `summary.csv`, `estimate.py`,
the instrumented `source/` copy, and fixture directories with `shape-*.json`.
The fixture definitions and results above are the durable evidence; the task
must remain reproducible without preserving that ignored directory.

### Source references

- [Architecture](../architecture.md), [table-file identity contract](../table-file.md),
  [block index](../block-index.md), and [data checkpoint](../data-checkpoint.md).
- [Backlog 000201](../backlogs/000201-adaptive-cold-row-identity-encoding.md).
- [RFC 0011](../rfcs/0011-redesign-column-block-index-program.md) and
  [RFC 0012](../rfcs/0012-remove-row-id-from-lwc-page.md), including the canonical
  fingerprint and values-only LWC contracts.
- `doradb-storage/src/index/column_block_index.rs`: `LogicalRowSet`,
  `ColumnBlockEntryShape`, `locate_and_resolve_row`, `ScanRowIdentity`,
  `validate_persisted_column_block_index_page`, and
  `write_leaf_pages_from_logical_entries`.
- `doradb-storage/src/lwc/mod.rs`: `LwcBuilder::append_view_inner`,
  `estimate_size`, snapshot, and rollback.
- `doradb-storage/src/table/persistence.rs`: `build_and_write_lwc_blocks`;
  `doradb-storage/src/catalog/storage/mod.rs`: `build_lwc_blocks_from_row_records`.
- `doradb-storage/src/buffer/readonly.rs`: `read_validated_block` and the write
  barrier; [test](../process/unit-test.md) and [lint](../process/lint.md) policy.

## Goals

- Select between whole-entry and segmented codecs using exact complete byte
  costs and deterministic access-cost tie breakers. Exploit packing-time range
  information without persisting source PageIDs.
- Resolve membership, both ordinal directions, and sequential iteration
  directly over validated compact metadata. Warm point reads must neither
  expand all RowIDs nor revalidate the whole immutable leaf.
- Keep scan identity compact and shared; preserve existing ordinal visibility
  masks and materialize RowID vectors only where an owned API requires them.
- Preserve membership, outer coverage, LWC ordinal order, index/LWC binding,
  delete domains, MVCC, checkpoint/recovery ordering, and atomic root publication.
- Read existing dense/u32-list data, handle malformed new sections through typed
  integrity errors, and reject unsupported entry capacities before publication.
- Demonstrate space benefits and meet the query and checkpoint performance
  gates below on real checkpoint and synthetic distributions.

## Non-Goals

- General identity-aware splitting or repacking of an oversized LWC block;
  external row-identity blobs, their I/O, or their reachability lifecycle.
- Changing delete payload encodings, delete consolidation, transaction
  semantics, root publication, or checksum algorithms used by other persisted formats.
- A global optimal segment partitioner, new compression dependency, user-facing
  codec knobs, or workload-trained policy.
- Removing the writer's existing RowID vector, redesigning the readonly cache,
  or changing catalog/recovery ownership APIs across the engine.
- Rewriting old roots eagerly or guaranteeing that older binaries read newly
  introduced codecs. Existing unknown-codec rejection remains the boundary.
- Resolving unrelated backlogs, including oversized source pages (000007),
  repeated catalog leaf loads (000193), and deletion follow-ups listed in 000201.

## Rejected Alternatives

1. **Whole-entry encoding only.** It handles the measured insert-only and many
   10% cases well, but loses the local missing-offset advantage across nearly
   full page ranges separated by unused space.
2. **Mandatory segmentation at source-page boundaries.** It pays directory
   overhead when a whole-entry codec wins and preserves boundaries that cease
   to matter after checkpoint. Logical boundaries must be mergeable.
3. **Compress on disk, expand on every read.** It improves leaf occupancy but
   retains row-count-dependent lookup allocation and scan setup memory.
4. **External identity storage and a general packing redesign now.** This adds
   separate I/O and lifecycle responsibilities. The current task remains inline
   and gives unsupported capacity a deterministic typed outcome.

## Plan

### 1. Introduce a compact row-set module

Add `doradb-storage/src/index/column_row_set.rs`, exported internally from
`index/mod.rs`. It owns row-set statistics, planning, exact size calculation,
encoding, validation, compact access, and iteration. Keep leaf framing, delete
metadata, and binding-value ownership in `column_block_index.rs`.

| Type | Responsibility |
| --- | --- |
| `RowSetStats` | Present count, first/last delta, logical span, and present-run count; combine adjacent stats in constant time. |
| `RowSetSeed` | Ordinal range and statistics from a contributing row-page range or a synthesized logical window. This is a transient hint. |
| `RowSetPlan` | Selected whole-entry codec or ordered segment plans, with exact serialized length and required widths. |
| `EncodedRowSet` | Codec tag and owned compact body, shared with `Arc<[u8]>` when nonempty; no allocation for dense identity. |
| `RowSetRef<'a>` | Validated borrowed body plus coverage/cardinality context. It exposes operations without allocation. |
| `RowSetIter` | Sequential codec cursor, including current segment/run/word as needed. It never expands the full row set. |

The common four-byte row header remains index-owned because its auxiliary byte
records the default delete domain. Sharing or copying the identity body must
not accidentally overwrite that domain. `EncodedRowSet` creates a borrowed view
through the same compact access implementation used by leaf readers.

Expose these crate-internal operations, with deltas relative to entry start:

```rust
fn ordinal_for_delta(&self, delta: u32) -> Option<u16>;
fn delta_for_ordinal(&self, ordinal: u16) -> Option<u32>;
fn iter_deltas(&self) -> RowSetIter<'_>;
```

Constructors distinguish fully validated bytes from unchecked disk input.
Only validation or trusted encoding may establish the invariant used by the
fast view; a public unchecked constructor must not bypass it.

### 2. Define complete, directly accessible row-section formats

Retain the common row header and row-section version 1. Use a 24-byte leaf
entry header and 24-byte LWC header with u64 binding values; bump their envelope
versions to 3 and 2 respectively. Keep codec tags 1 (dense) and 2 (u32 present
list) byte-compatible within the new envelopes. Allocate distinct new tags for the formats below and document
their numerical assignments alongside the serializer. All fields are packed
little-endian, with no implicit alignment padding. Read unaligned words safely.
Reserved bits/bytes are zero and checked.

Let `N` be present count, `R` present-run count, `B` bitmap bit span,
`W = ceil(B / 64)`, and `G = ceil(W / 4)`. Complete whole-entry costs are:

| Codec | Body after the common four-byte header | Total bytes |
| --- | --- | ---: |
| Dense | Empty; outer span equals N | 4 |
| Present u32 list, existing | N sorted u32 deltas | 4 + 4N |
| Present u16 list | N sorted u16 deltas | 4 + 2N |
| Runs16 / Runs32 | u16 N, u16 R, then records with u16/u32 start, u16 length, u16 ordinal prefix | 8 + 6R / 8 + 8R |
| Bitmap | u16 N, u8 variant, u8 reserved; optional u32 base delta and u32 bit span; G u16 prefixes and W u64 words | 8 + 2G + 8W, or 16 + 2G + 8W when trimmed |
| Missing16, full coverage | u16 N, u16 reserved, then sorted u16 missing offsets | 8 + 2(span - N) |
| Missing16, trimmed | u32 base delta, u16 span, u16 N, then sorted u16 missing offsets | 12 + 2(span - N) |

Use distinct tags for the two missing-offset header shapes. Bitmap variant 0
uses the outer coverage and variant 1 carries the trimmed base/span. A trimmed
range excludes leading/trailing absence; a zero-hole trimmed range is valid.
Narrow whole lists/runs require all present deltas to fit u16. Missing-offset
spans are 1..65,535; each run length and total cardinality fit u16. Every codec
must keep present positions within the outer coverage.

A segmented row section starts with the common four bytes, then `row_count:u16`
and `segment_count:u16`. Its directory has one packed 16-byte record per segment:

```text
start_delta    u32   relative to outer entry start
span           u16   logical segment length, 1..65,535
row_count      u16
ordinal_base   u16   number of present rows in preceding segments
payload_offset u16   relative to the body after the common four-byte header
payload_len    u16
codec          u8
reserved       u8
```

Payloads follow the directory in directory order without gaps or overlap;
zero-length dense payloads may share the current offset. Directory ranges are
ordered, nonempty, and nonoverlapping, and ordinal bases are contiguous.
Segment endpoints describe logical RowID ranges, never source PageIDs.

Local codecs reuse directory cardinality/span instead of another header:

| Local codec | Payload cost | Access metadata |
| --- | ---: | --- |
| Dense | 0 | Directory span/count |
| Present u16 offsets | 2N | Sorted offsets |
| Missing u16 offsets | 2(span - N) | Sorted holes |
| Runs16 | 6R | u16 start, length, and local ordinal prefix per run |
| Bitmap | 2G + 8W | u16 population prefix per 256-bit group, followed by words |

Define prefixes as the number of set bits before each group. They may repeat
for empty groups. Unused bits in the final word are zero. Segment ordinal
prefixes and run ordinal prefixes count present rows, including rows later
marked durably deleted.

Do not allocate enormous bitmap/list candidates during sizing. All cardinality,
span, multiplication, addition, length conversion, and RowID arithmetic are
checked before allocation. Whole-entry sparse codecs may cover up to the
existing u32 outer span; their complete encoded size must still fit a leaf.

### 3. Select codecs from packing statistics

Keep the current writer RowID vector while values are built. During
`LwcBuilder::append_view_inner`, record seeds for the rows actually selected
from each page and update their counts, endpoints, and run counts. Omit empty
seeds. Snapshot/rollback restores seeds and statistics together with RowIDs and
values. Pages split across builder attempts contribute only the accepted part.

The catalog's direct-row path uses deterministic 4,096-position logical windows
as initial seeds because it has no source-page grouping. Trim windows to their
present endpoints; do not enumerate empty windows across a large gap. A future
input seed exceeding 65,535 positions is split into bounded logical windows.
These choices affect compression only, not semantics or binding values.

At finalization, with actual outer entry bounds fixed:

1. Derive statistics in one pass and evaluate every legal whole-entry candidate
   by exact serialized cost. Preserve the dense/u32-list fallback. Choose the
   cheapest local codec for each trimmed seed using the same cost functions.
2. Put adjacent seed pairs in a max-heap keyed by the saving from replacing two
   directory records/payloads with one. Merge the largest nonnegative saving,
   subject to the 65,535-position segment limit; zero-saving merges reduce the
   segment count. Break equal savings by the leftmost start.
3. Compute merged statistics without rescanning rows: add counts and run counts,
   subtract one run only when the two boundary rows are adjacent, and derive
   span from the outside endpoints. Gaps become local missing positions. Use
   neighbor links and generation counters to discard stale heap entries, and
   re-evaluate only the new adjacent pairs.
4. Compare `8 + 16 * segment_count + sum(local_payload_bytes)` with the best
   complete whole-entry cost. On equal bytes, prefer fewer segments, then the
   smaller bound on membership/rank/select work, then a stable codec order.
   A whole-entry codec counts as one segment for this comparison.
5. Check capacity, encode only the winner, and retain that immutable result.

Use a fixed cost tuple for access ties: binary-search depth, bounded word work,
then codec order (dense, present list, runs, missing list, bitmap, segmented).
Derive depth from the searchable item count; include directory search for
segmented candidates. This is a deterministic tie breaker, not a claim that
the tuple predicts actual latency. Every candidate must already satisfy the
compact access bounds below. No workload weights or user controls are added.

Target O(N + S log S) planning/encoding time and O(S) additional planning memory
for S seeds, excluding the existing RowID vector and final encoded bytes. Run
the planner once per finalized LWC block. Missing-offset and bitmap emission
may walk their selected span, which is bounded by their selected byte budget.
The greedy policy need not find a globally minimal partition; retaining the
whole-entry candidate guarantees the result is no larger than that candidate.

### 4. Implement compact point access and iteration

| Codec | Membership and RowID-to-ordinal | Ordinal-to-RowID |
| --- | --- | --- |
| Dense | Arithmetic | Arithmetic |
| Present list | Binary search | Direct indexed read |
| Missing list | Binary search for hole membership and preceding-hole count | Binary search over adjusted hole positions |
| Runs | Binary search by run start, then arithmetic | Binary search by ordinal prefix, then arithmetic |
| Bitmap | Bit membership and prefix rank with at most four word popcounts | Binary search population prefixes, then at most four words and bounded single-word select |
| Segmented | Binary search segment starts, then local operation | Binary search ordinal bases, then local operation |

For sorted holes `h[i]`, inverse mapping for present ordinal `k` is
`k + upper_bound(h[i] - i, k)`, followed by the trimmed/segment base adjustment.
The adjusted sequence is monotone and need not be materialized. Reject misses
in inter-segment gaps and trimmed tails before local lookup. Bounds checks
must also reject out-of-range ordinals.

`RowSetIter` advances sequentially through list offsets, runs, bitmap words, or
holes and then segments. It does not perform a fresh directory/binary search
for each row. Canonical hashing and explicitly owned exports use this iterator.

Move complete leaf and row-section validation into
`validate_persisted_column_block_index_page`, used by the existing readonly
cache's admission path. Validate framing, search prefixes, codec grammar,
ordered ranges, counts, payload bounds, nonoverlap, bitmap/run/segment prefix
consistency, reserved fields, padding, and arithmetic there. Return existing
typed `DataIntegrity` failures for malformed persisted bytes.

`read_validated_block` already validates a cold frame before publishing it and
reuses an admitted immutable generation on warm reads. The existing write
barrier invalidates stale cached blocks. Use that contract to construct fast
borrowed views without repeated full-leaf validation. Audit all row-section
consumers to ensure none reaches this path through an unvalidated frame; keep
writer/test construction behind validation or trusted encoding. No cache
architecture change is required.

Replace sparse `ScanRowIdentity` vectors with shared encoded identity and keep
dense descriptors allocation-free. Scans keep their existing ordinal visibility
masks. External-delete descriptors share identity only when RowID-domain
translation needs it; they must not expand identity just to stage the scan.
Owned catalog/recovery exports may collect RowIDs from the iterator at their
existing API boundary. External-delete I/O/decoding costs are reported
separately from row-identity access bounds.

### 5. Carry the selected identity through checkpoint and rewrites

The write flow is:

```text
visible source rows and page ranges
  -> LwcBuilder values, RowIDs, and transient seed statistics
  -> finalized outer entry coverage, including final pivot and absent tails
  -> exact plan, capacity check, one identity encoding, fixed-input block binding value
  -> ColumnBlockEntryShape with compact identity and matching LWC header
  -> existing ordered LWC encode/write pipeline
  -> with_block_id -> ColumnBlockEntryInput with the same compact identity
  -> index rebuild and existing atomic root publication
  -> validated cache admission -> compact point and scan access
```

Make shape construction fallible. Freeze the selected identity before submitting
that block to the write pipeline; `.with_block_id` only attaches its durable
block ID. Avoid selecting again when the leaf entry is serialized. The table
and catalog paths must use the same planner/format and propagate errors through
their existing checkpoint ownership and drain behavior.

Compute `block_binding_value` using one BLAKE3 call over a 36-byte stack buffer:
`LWCBIND1` (8 bytes), logical table ID (u64 LE), inclusive start RowID (u64 LE),
exclusive end RowID (u64 LE), and row count (u32 LE). Interpret the first eight
digest bytes as u64 LE. The table checkpoint and catalog direct-row builders
supply the owning logical table ID. No randomness, timestamp, physical block
ID, per-row input, or new hash dependency is used. Equal summaries intentionally
share a value even when interior sparse membership or values differ.

The LWC header receives exactly the binding value carried by its index entry,
and decoded cardinality must match LWC row count. Delete-only copy-on-write
updates preserve identity codec/body, binding value, and ordinals. A later cold
delete changes deletion state, never a
presence bitmap bit. Keep both existing delete domains and their payload
formats. The default-domain field in the common row header may follow existing
delete-domain rules independently of the immutable identity body. Refactor
delete helpers to use compact membership/inverse mapping where necessary.

### 6. Make inline capacity an explicit contract

Reserve room for the maximum existing inline delete section on newly planned
entries: an eight-byte header plus 58 persisted u32 values, or 240 bytes. The
current external delete reference needs 22 bytes and fits within that reserve.
Derive the reserve from existing delete thresholds and serialized widths rather
than duplicating an unexplained constant.

A standalone entry can use a four-byte search prefix. Its maximum complete row
section is therefore:

```text
65,536 page bytes
  - 48 integrity - 24 node header - 8 leaf extension
  - 4 search prefix - 24 entry header - 240 delete reserve
  = 65,188 row-section bytes, including the common four-byte header
```

The corresponding compact body limit is 65,184 bytes. Derive and assert the
formula against layout constants. Also enforce nonempty cardinality at most
65,535, outer span within u32, representable offsets/lengths, and nonoverflowing
RowID endpoints. Recompute the leaf prefix width while packing multiple entries;
split between entries as today, so the single-entry guarantee remains valid.

For valid input with no eligible fitting codec, return a fieldless typed
`ResourceError::ColumnBlockEntryCapacityExceeded` before submitting that block
or publishing its root. Replace the leaf writer's oversized-single-entry
assertion with a defensive exact-size error path. Already queued writes still
follow existing drain/cleanup rules and cannot publish a partial root.

Do not retroactively reject readable legacy entries solely because they lack
the new 240-byte reserve. A rewrite still checks its actual serialized size and
returns the same capacity error if it cannot fit. Corrupt persisted lengths or
counts remain integrity failures, not resource errors.

This task does not promise a successful checkpoint for every large sparse
input. A failure after checkpoint TRANSITION retains the existing fatal error
handling; changing that recovery contract or splitting the LWC values block is
outside scope. Record this limitation in the checkpoint documentation.

### 7. Document and measure the completed implementation

Update the durable codec layouts, compatibility policy, access bounds,
selection/capacity rules, and failure behavior in the storage documentation.
Preserve dense/u32-list row-codec fixtures and test mixed codecs within the
new envelopes. Reject prior LWC/index envelope versions with `InvalidVersion`;
no migration reader is included. Document the fresh-storage requirement.

Reproduce the nine fixtures and add synthetic contiguous, few-run, almost-full,
small-gap, widely scattered, large-gap, many-segment, mixed-density, and highly
compressible-value cases. Compare current dense/u32 storage, adaptive whole
encoding, mandatory segmentation, and the implemented hybrid on the same
RowID sets. Do not require the illustrative estimates to equal a future
different packing result; require estimator/serializer agreement on each actual
input and explain any comparison difference.

Report complete row-section bytes, leaf fanout/occupancy and allocated index
pages, readonly-cache and scan-descriptor footprint, temporary allocations,
encode/checkpoint cost, warm/cold point reads, both ordinal translations, scan
throughput, and catalog/recovery decode cost. Include codec-only hit/miss,
inverse, iteration, and encoding measurements against a compact u32-list
baseline so engine overhead does not hide codec cost.

Use repeated controlled release measurements with identical fixtures, hardware,
configuration, and cache conditions. Run baseline and candidate sequentially,
record sample counts and medians, and investigate noise before drawing a
conclusion. Acceptance gates are at most 10% regression in median warm point
latency or scan throughput and at most 20% regression in checkpoint elapsed
time against the current implementation. Apply gates to corresponding fixtures,
not just an aggregate that can hide a regression. These are review gates, not
timing assertions in unit tests. Tune within the defined policy/access bounds
and rerun failed gates before resolving the task; a material policy change
needs design review.

## Implementation Notes

The approved binding follow-up replaces per-RowID fingerprinting with
`block_binding_value: u64`. Both persisted headers shrink from 32 to 24 bytes;
column-index envelope version 3 and LWC envelope version 2 reject old images.
The payload is exactly `LWCBIND1` plus table ID, start/end RowIDs, and count in
36 stack bytes, hashed once with BLAKE3 and truncated to u64. Index rewrites
preserve this value; the full-block checksum algorithm is unchanged. Historical
profiles below describe the earlier fingerprint implementation.

The binding comparison repeats both
one-million-row checkpoint scenarios with independent before/after builds.
All 32 runs verify every survivor. With 1% deletion, sampled fingerprint/binding
CPU falls from 8.54% to 0.04%. Aggregate process-CPU medians are 260.6 -> 241.0 ms,
but the median paired change is only -0.8%; elapsed times do not establish a
speedup. Insert-only CPU medians are 222.6 -> 229.5 ms. Both variants retain
2,233 LWC blocks. Final checks pass: 2,208 workspace tests, 2,029 tests without
profiling, both strict Clippy configurations, and the 17-file style audit.

Implemented the nine whole-entry tags and five local codecs in
`index/column_row_set.rs`, with exact complete costs, page/window seed statistics,
deterministic heap merging, compact rank/select, and sequential iteration.
`ColumnBlockEntryShape::new` freezes the selected bytes and fixed-input
block binding value before LWC submission. Shape/input/rewrite paths retain those bytes;
scan descriptors share compact bodies and point reads borrow admitted bytes.
Scan preparation and deferred RowID-domain deletes use `EncodedRowSet` directly,
with an inlined arithmetic lookup for dense identities and no separate
`ScanRowIdentity` wrapper. This changes only the in-memory representation.

Full row/leaf grammar validation now runs at readonly-cache admission. Warm
lookups do not expand RowIDs or revalidate the leaf. Both deletion domains retain
identity bytes and ordinals through CoW rewrites. Newly planned entries enforce
the derived 65,184-byte body budget with the 240-byte delete reserve; legacy
rewrites check their actual size and return the same typed resource error.

Five controlled baseline/candidate pairs for each of the
nine 100,000-row fixtures passed the per-fixture checkpoint, warm point, and scan
gates. All adaptive indexes fit one leaf. Row sections total 904..17,750 bytes;
the compressed 1% case uses 5,882 bytes versus 7,492 for whole-entry-only encoding.
Every captured estimator agrees with serialization. Later committed deletes,
external payload decoding, and reopen were verified and measured separately.

A subsequent public `doradb-bench` random point-select comparison uses
1,000,000 fully checkpointed rows and warmed buffers. Five fresh pairs per
reader configuration show median throughput gains of 12.76x with one reader
and 8.03x with four readers; all 12,000,000 measured selects found their rows.

The matching hot-row control uses the
same binaries and criteria with freezing/checkpointing omitted. Current cold
throughput reaches 81.3% and 83.1% of hot throughput with one and four readers.
Hot throughput itself changes -3.5% and +6.2% versus the pre-task implementation;
all 12,000,000 measured hot selects returned rows without data/index cache
misses or readonly-buffer accesses.

A checkpoint CPU profile covers
1,000,000 inserts and the same fixture with 10,000 seeded random deletes before
checkpoint. Five profiles per case attribute 0.9% / 1.2% of sampled user CPU to
row-ID planning, encoding, and seed statistics; canonical row-shape fingerprinting
uses 0.2% / 9.9%. Separate unprofiled runs have median checkpoint process CPU
times of 243 / 264 ms.

[Backlog 000207](../backlogs/000207-evaluate-checksum-algorithms-across-all-use-cases.md)
tracks evaluation and selection of checksum algorithms across all checksum and
integrity-fingerprint uses, including xxHash candidates. The checkpoint profiles
identify LWC block checksums as the largest CPU component; broader algorithm
selection and checksum-format transitions remain deferred to that follow-up.

Validation includes workspace and profiling-disabled storage tests, strict
Clippy, style/test-contract audits, and focused production coverage. Test oracles
cover all formats, malformed bytes, missing-offset inversion, planning,
fixed-input binding, admission/invalidation, shared scans, builder rollback,
mixed legacy/adaptive rewrites, and unsupported inline capacity.

Source backlog 000201 remains open for general identity-aware LWC splitting,
external identity storage, and broader policy work. This task preserves existing
fatal handling for a capacity error after TRANSITION; it does not guarantee a
successful checkpoint for every sparse input. No parent RFC phase is active.

## Impacts

| File or area | Main change |
| --- | --- |
| `doradb-storage/src/index/column_row_set.rs` (new), `index/mod.rs` | Codec layouts, statistics, plans, compact views, exact estimator, encoder, validator, and iterator. |
| `doradb-storage/src/index/column_block_index.rs` | Compact entry shape/input and logical entries; point/scan/delete consumers; admission validation; fixed-input binding; fallible capacity handling and identity-preserving rewrites. |
| `doradb-storage/src/lwc/mod.rs` | Seed statistics and snapshot/rollback integration; retain existing value packing and writer RowIDs. |
| `doradb-storage/src/table/persistence.rs` | Finalize coverage and encoded identity before pipeline submission; propagate typed errors with existing publication/drain ordering. |
| `doradb-storage/src/catalog/storage/mod.rs` | Logical-window seeds and common planner; iterator-based owned RowID export where required. |
| `doradb-storage/src/table/access.rs` | Compact identity in scan visibility preparation, preserving ordinal masks. |
| `doradb-storage/src/buffer/readonly.rs` | Verify existing admission/invalidation guarantees with focused tests; no pool redesign. |
| `doradb-storage/src/error.rs` | Typed unsupported entry-capacity outcome. |
| Catalog/recovery tests and focused benchmark support | Mixed-format reopen/recovery and reproducible size, allocation, and access measurements. |
| `docs/table-file.md`, `docs/block-index.md`, `docs/data-checkpoint.md` | Format, ownership, lookup, compatibility, and bounded capacity behavior. |

Backlog 000201 remains the source for deferred general splitting/offloading and
any policy work beyond this bounded implementation. At resolution, record
completed coverage and preserve recoverable context for the remainder instead
of treating this task as implementing every scope hint in that backlog.

## Test Cases

1. **Codec semantics.** For every whole/local codec, compare membership, both
   ordinal directions, and iteration with a canonical sorted RowID oracle.
   Cover singletons, dense/tiny sets, leading/trailing absence, page boundaries,
   long/frequent gaps, all legal run lengths, near-full ranges, repeated empty
   bitmap-group prefixes, mixed segments, and misses at every kind of boundary.
   Include exhaustive small-domain missing-offset inverse cases and deterministic
   generated distributions. Verify semantic outcomes rather than only round trips.
2. **Selection and sizing.** Assert estimator bytes equal serialized length;
   the selected cost is no greater than the best eligible whole candidate.
   Exercise heap staleness, leftmost ties, zero-saving merges, nonzero-gap merges,
   bounded spans, local codec changes, catalog windows, and append rollback.
   Check reproducible output for identical logical inputs and seeds.
3. **Malformed data.** Reject truncation, invalid tags/versions, reserved bits,
   unordered/duplicate offsets, overlapping/out-of-range segments or payloads,
   inconsistent cardinality/prefixes, invalid bitmap padding, and integer
   overflow with typed integrity errors before publishing a cache frame.
4. **Validation and memory behavior.** Prove cold admission validates fully,
   warm reads reuse that validation, and invalidation/reload validates again.
   Use test instrumentation/allocation measurements to show warm point lookup
   does not traverse all rows or allocate a row-count-sized vector. Verify
   compact shared scan descriptors and lazy external-delete handling.
5. **Identity contracts.** Verify the literal binding hash input, its four bound
   fields, and deliberate independence from interior membership and deletion
   state. Both header layouts use u64 LE and old envelope versions are rejected.
   Delete-only rewrites in both domains retain identity bytes, binding values,
   ordinals, and scan results. Checksummed LWC/index binding mismatches still
   fail at the existing integrity boundary.
6. **Capacity.** Test cardinality 65,535 and overflow, narrow/span boundaries,
   large u32 deltas, RowIDs near the u64 limit, each serialized length limit,
   the 240-byte reserve, wider multi-entry prefixes, and actual legacy rewrite
   overflow. Highly compressible values with oversized identity return the typed
   error before root publication; no single-entry capacity assertion is reached.
7. **Engine integration.** Checkpoint, reopen, catalog rebuild, and recovery with
   each newly emitted codec and mixed legacy/adaptive row codecs in new roots. Re-run the 100,000-row
   fixtures at 0%, 1%, and 10% pre-checkpoint deletion; also delete after
   checkpoint to distinguish physical membership from durable deletion state.
   Verify values, complete ordered RowIDs, visibility, binding values, and atomic
   publication on success and injected failure.
8. **Performance and process gates.** Produce the measurements and meet the
   budgets in Plan section 7. Follow test contracts in `docs/process/unit-test.md`
   and the format/Clippy/style gates in `docs/process/lint.md`. Run
   `rtk cargo nextest run --workspace`; add the documented no-default-features
   storage pass when changes are feature-sensitive. Use focused tests while
   developing, then the authoritative workspace pass. Do not change the runner,
   `.config/nextest.toml`, or timeout policy to accommodate this task.

## Open Questions

No blocking design questions remain. Implementation must measure the constant
costs of compact access and cold admission against the stated budgets. Exact
byte minimization can prefer a codec with a higher CPU cost; the bounded-access
rules and performance gates must catch that tradeoff before task resolution.

The greedy merger can miss a globally smaller partition, page-derived seeds can
affect compression, and gains vary with value compression and deletion shape.
Record adversarial cases rather than expanding this task into a global planner.
General splitting/offloading for entries that cannot fit, recovery behavior
changes for capacity failure, and broader catalog ownership/load improvements
remain follow-up work with context retained in the relevant backlog.
