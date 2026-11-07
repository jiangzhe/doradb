---
id: 000339
title: Stream partial RowPages into LWC blocks
status: implemented
created: 2026-11-04
github_issue: 1166
---

# Task: Stream partial RowPages into LWC blocks

## Summary

Prepared RowPages now stream their largest fitting visible prefixes into LWC
blocks. A page that exceeds an empty block is split without rejecting valid
rows or repeating visibility analysis. Exact encoding plans precede copying;
workers serialize accepted columns directly into the final block.

## Context

Source Backlogs:
- docs/backlogs/closed/000007-lwc-oversize-rowpage-handling.md

Issue Labels:
- type:bug
- priority:high
- codex

This standalone task has no parent RFC. Baseline revision was
`eea7622bdd3a3a8bed8f4c7a569019b3ce479df3`.
[Task 000323](000323-inline-adaptive-deletion-encoding-and-deletion-blob-retirement.md)
established count splitting and the 15,360-row cap;
[task 000337](000337-make-lwc-integer-encoding-and-block-sizing-consistent.md)
established exact integer encoding costs; and
[task 000338](000338-limit-single-rows-to-rowpage-and-lwc-block-capacity.md)
established complete-row admission.

Previously, the builder copied and then rejected an entire selected page view.
Checkpoint flushed and retried that same selection. Aggregate value overflow
could therefore fail after Transition despite every row being admissible.

## Goals

- Partition by exact encoded bytes and row count while preserving the largest
  fitting visible prefix, values, nulls, ordered identities, and exact sidecars.
- Keep rejected values in their source page and retain only bounded cursor
  metadata across pipeline waits.
- Freeze one compression decision for admission and writing, including the
  cost of widening already accepted integer values.
- Remove intermediate encoded-column buffers and hidden null byte payloads.
- Preserve coverage, bindings, publication/recovery ownership, and existing
  persisted readers; establish packing, CPU and temporary-memory evidence.

## Non-Goals

No new codecs, format versions, overflow storage, public settings, row limits,
DML/replay admission rules, undo scans, or publication policy. Query ScanBuffer
semantics were outside the original task; the unused API was removed in the
subsequent cleanup below. Unsupported RowID spans remain backlog 000208. Dictionary/FSST and
compression-aware boundary optimization remain separate work.

## Plan

### Admission and source ownership

`lwc/builder.rs` owns the builder, compact column buffers, reusable summaries,
exact block/column plans, source cursor, and owned worker input. `append_prefix`
accepts a borrowed prepared view and cursor, returning physical consumption,
visible rows appended, and `PageExhausted` or `BlockFull`.

The cursor caches at most 1,024 visible u16 source positions. Pending parts
form an unfinished subdivision stack, with summary storage recycled as parts
are consumed. A rejected multirow part splits left-first; a rejected singleton
stays pending for the next block. An inadmissible singleton in an empty builder
asserts the DML/replay admission invariant with its source range and RowID.

Candidate summaries reuse vector capacities through `BlockSummary::clone_from`
and `merge`. `PlanScratch::try_plan` reuses a column vector and returns a complete
`LwcBlockPlan` only on success. Rejected attempts retain partial columns in
scratch, cleared at the next planning attempt. After staging succeeds, acceptance
swaps summaries, installs the returned plan, and recycles the previous accepted
plan's vector into scratch. Integer extrema ignore nulls; byte statistics read
only non-null lengths. Current flat/FOR costs are monotonic, permitting refinement
to the first nonfitting visible row. Direct catalog rows use the same
preview-before-copy rule and remain atomic.

`PageVectorView` supports borrowed or owned prepared bitmaps and resumes
traversal at the current bitmap word. Reload asserts the frozen RowID base,
row count and selected bounds; checkpoint retains and validates page identity.
Accepted positions drive staging, RowIDs and one synchronous sidecar callback.
Contiguous positions use bulk copy runs; planning batches are coalesced into
one identity seed per accepted source-page/block portion.

### Serialization and publication

Typed staging stores numeric vectors, optional null words, and N+1 u32 byte
offsets. Null byte rows append repeated offsets without reading descriptors.
`finish(binding)` releases temporary RowIDs, identity seeds and planner scratch,
then transfers only owned columns, binding and the accepted plan to workers.

The writer allocates one zeroed 64-KiB DirectBuf and writes headers, offsets,
null bitmaps and column encodings directly into exact destination slices.
Integer null slots normalize in owned staging to the final plan minimum.
Count, bitmap, offset, byte-length and total-capacity mismatches assert the
frozen-input construction contract. Padding and checksums remain deterministic.

Version 3, flat tag 1, FOR tag 3, the 65,504-byte payload and 15,360-row cap are
unchanged. Existing readers decode both old and new blocks. Future codecs can
own prepared state in a frozen encoding variant; this does not impose today's
extrema model or monotonic-prefix assumption on future algorithms.

Checkpoint finalizes coverage and index shape before `finish`. Partial pages
end at the consumed physical boundary; exhausted pages retain reserved ends.
Full builders remain pending through invisible pages until another visible row
or the final pivot determines coverage. Page guards and borrowed views end
before capacity, ingress or drain waits. Logical write ordering, worker-count
occupancy, shared-storage backpressure, failure drains and atomic publication
are unchanged. Catalog construction uses `finish(...).encode()` too.

## Implementation Notes

Valid aggregate byte overflow now partitions successfully through checkpoint
publication and restart. The 8,000-row six-byte inline fixture splits into
6,546 and 1,454 rows. Three pages of 100 outlined 400-byte values occupy two
blocks containing 162 and 138 rows, instead of three blocks.

The initial implementation retained the general query ScanBuffer API with
scoped dead-code expectations; the later column-view cleanup removed it after
confirming it had no production consumers. Old serializer helpers remain only in
reader tests as independent compatibility fixtures; production uses the new
writer. No compatibility builder wrappers or benchmark framework were added.

Review explicitly checked cursor physical/visible coordinates, null descriptor
avoidance, preview immutability, whole-column widening, seed coalescing, guard
lifetimes and final coverage. Source reload and malformed encoder inputs have
assertion tests. Shared test helpers consolidate
source encoding and persisted row assertions. Named reader, builder, catalog and production
checkpoint tests remain separate because they protect distinct wire-format,
admission and lifecycle contracts.

### Shared primitive and codec follow-up (2026-11-05)

The requested follow-up refactor separates physical scalar support, codec
layout rules, and block orchestration. `primitive.rs` owns native numeric
buffers, the scalar type list, page-to-native conversion, and fixed-width
little-endian decoding. LWC staging uses that numeric storage while its
variable-byte layout and null policy remain specific to block construction.

`lwc/codec.rs` now owns integer plans, shared flat serialization, exact flat/FOR
extents, singleton sizing helpers, and one validated payload description.
Borrowed and prepared readers share structural parsing, including byte-offset
endpoints, and column readers check payload counts against their block. Point
reads check only the selected offset pair; prepared readers validate all offsets
once per traversal before retaining metadata for repeated decoding.
The parser bounds persisted counts by the block header's u16 representation
before sizing; shared flat/FOR extent helpers use ordinary arithmetic and return
`usize` for both planning and reading.
`lwc/reader.rs` generates ordinary scalar dispatch from an explicit codec/type
matrix while reusing generic bitpacking kernels. The version-3 wire format,
accepted-prefix ownership, and direct final-buffer writing are preserved.

A prototype with nested type/codec enums increased ordinary integer lookup
CPU by up to 20% in the local fixtures. Generated dispatch with one runtime
discriminant removed that regression. Six alternating release comparison pairs
against the pre-refactor task implementation measured ordinary lookups from
unchanged to 25.4% faster, and prepared lookups between 2.1% faster and 1.1%
slower. Preparation ranged from 19.3% faster to 3.8% slower. Conversion and
encoding changes across eight component fixtures ranged from -12.7% to +2.1%
and -19.5% to +1.6%, respectively. Allocation counts, peak staging allocations,
block counts, used payload lengths, and row/identity membership were unchanged.
These component measurements do not establish end-to-end checkpoint or process
RSS changes. Sources, binary hashes, runner, and raw observations are recorded
in the ignored `target/lwc-refactor/` artifacts.

Validation passed 2,498 workspace tests (3 ignored) and 2,226 storage tests with
profiling disabled (2 ignored). The style gate passed 13 changed Rust files and
241 test contracts, including strict workspace Clippy. The semantic review
retained independent manually packed fixtures for all 36 integer type/width
combinations, literal float-bit wire expectations, and separate builder,
reader, and checkpoint contracts. New direct-row/page-scan parity coverage also
checks buffer reuse, null flags, byte offsets, and exact floating-point bits.
The two integer-plan tests moved from `serde.rs` without losing their original
independent wire and destination-boundary assertions.
Final production-line coverage was 94.48% across the focused targets: 93.89%
for LWC, 95.38% for shared primitives, and 97.56% for vector scanning. Every
focused file exceeded 80%; remaining misses are predominantly invariant/error
branches and reserved reader operations. These measurements precede the
column-view cleanup below.

### Column-view cleanup (2026-11-05)

`row/column_view.rs` replaces `row/vector_scan.rs` and retains the production
page-view, prepared visibility, and typed column-copy paths. The unused
`ScanBuffer`, `ScanColumn`, `ScanColumnValues`, `ColBuffer`, and `ValBuffer`
APIs and their truncation helpers were removed. The bounded-view fixture now
lives in test support instead of an unused production setter; transition tests
assert exact visible source ranges directly.

Tests of buffer reuse and truncation were removed with their APIs. Page-view
tests retain independent mixed-type rows, exact floating-point bits, nullable
and variable-width source coordinates, host-endian bitmap borrowing, and
prepared/live visibility disagreement. Bitmap tests additionally cover resumed
traversal at and beyond word and source boundaries. LWC admission, encoding,
and checkpoint lifecycle tests continue to exercise the production consumers.

Validation passed 2,495 workspace tests (3 ignored) and the style gate covering
15 changed Rust files and 247 test contracts, including strict workspace
Clippy. Fresh production-line coverage is 98.21% for column views, 94.23% for
primitives, and unchanged at 93.89% for LWC. The combined 94.07% reflects the
smaller production surface after removing unused operations; uncovered lines
in the focused files remain invariant/error branches and existing reader
operations. The report is retained at ignored
`target/lwc-refactor/column-view-coverage.md`.

The 2026-11-06 follow-up moved `RowPage::vector_view_with_del_bitmap` into
`row/mod.rs` and replaced its error result with a release assertion. Its bitmap
length is established by `RowPage::del_bitmap`; preparation only changes bits,
and Frozen/Transition state keeps the physical row count fixed after insertions
drain. Callers now receive the view directly. Boundary tests retain empty,
partial-word, full-word, and second-word cases and verify that invalid lengths
panic at the construction assertion.

`LwcSourceCursor::new` likewise returns its cursor directly and asserts the
u16 source-count bound. `RowPageHeader` stores the count in 16 bits and
`PageVectorView` captures it unchanged, so exceeding that bound is an internal
representation violation. Checkpoint initializes the cursor through
`get_or_insert_with`; constructor error propagation and test unwraps are removed.

Validation after both constructor refinements passed all 2,495 workspace tests
(3 ignored), strict Clippy, and the branch style/test-contract gate.

The cursor reload check also uses a release assertion. Checkpoint retains one
cursor per prepared page across block flushes, reconstructs the same source
range, and finishes conversion before retiring that page. Transition prevents
row-count growth. Tests independently change the RowID base, physical count,
range start, and range end, verify that assertion failure precedes staging,
callbacks, and cursor progress, then resume successfully with the original
selection. Frozen encoding inputs are covered independently of source reload.
Workspace nextest passed 2,496 tests (3 ignored), and the style gate passed
15 changed Rust files and 248 test contracts, including strict Clippy.

### Encoding invariant follow-up (2026-11-06)

The owned builder/encoder call chain now uses assertions for construction
contracts. `append_prefix`, `finish`, and `encode` return their values directly;
column writers return a byte count. Byte staging installs its origin offset and
only appends data and end offsets, so serialization does not rescan offset order.
The encoder asserts staged byte lengths against the frozen plan before writing.
Prepared-page presence and source identity are established by Transition and
asserted when production reloads a page.

Encoding completions carry `DirectBuf` directly. ThreadPool supervision still
reports task panics and unavailable workers through the completion error bridge;
the checkpoint pipeline preserves fatal classification and drains accepted work.
Tests cover assertion diagnostics, a real worker panic, subsequent completion
consumption, and fatal precedence over producer failure. Persisted readers,
catalog input validation, identity-layout selection, and IO retain typed errors.
The unused LWC internal-error variants were removed.

Validation passed 2,499 workspace tests (3 ignored), 2,227 storage tests with
profiling disabled (2 ignored), and the style gate covering 18 changed Rust
files and 362 test contracts, including strict workspace Clippy. Assertion
fixtures retain separate source, frozen-input, and worker-lifecycle contracts;
shared panic checks verify the owning component and failing field.

### Paired release measurements

Measured 2026-11-04 on Linux aarch64, 10 Apple vCPUs reported at 2 GHz, rustc
1.98.1/LLVM 22.1.8. Both revisions used the default profiling feature and Cargo
release profile (`debug = true`), without custom optimization flags. Storage
roots were fresh directories on tmpfs using the existing io_uring backend.
These results characterize this host and cache/storage setup.

Eight deterministic component fixtures used six balanced baseline/candidate
pairs in alternating AB/BA order. Each process discarded one warmup sample,
then measured five samples of 40 conversions. CPU clocks used
`CLOCK_THREAD_CPUTIME_ID`; the table reports medians across six per-process
medians, with min–max dispersion in brackets, in microseconds per fixture.
Fixture/source construction was outside the timing and allocation windows.

| Fixture | Conversion/planning CPU, baseline → candidate | Encoding CPU, baseline → candidate |
| --- | --- | --- |
| fixed | 11.18 [11.06–19.82] → 11.06 [10.89–11.20] | 12.08 [11.73–17.91] → 5.19 [5.12–5.26] |
| compressed | 26.31 [26.18–37.34] → 12.67 [12.57–13.33] | 3.32 [3.30–4.86] → 3.56 [3.52–3.86] |
| outlier | 29.73 [29.52–49.30] → 24.44 [24.39–24.54] | 14.94 [14.60–19.10] → 8.25 [8.19–8.29] |
| inline | 38.35 [38.17–49.35] → 38.45 [38.39–38.70] | 17.75 [17.56–21.01] → 11.51 [11.38–11.54] |
| outlined | 61.02 [60.85–87.17] → 44.10 [44.01–44.25] | 120.48 [118.77–144.21] → 43.03 [42.74–43.54] |
| nulls | 32.98 [32.80–43.34] → 18.35 [18.21–18.80] | 57.03 [56.05–67.09] → 7.25 [7.13–7.51] |
| sparse | 126.32 [125.89–146.03] → 44.59 [43.96–67.37] | 48.69 [47.37–54.29] → 10.01 [9.94–12.76] |
| wide | 306.12 [305.44–307.78] → 217.58 [216.66–301.50] | 110.27 [109.79–110.44] → 38.89 [38.67–52.97] |

Fixture definitions: fixed = eight 1,000-row F64 pages; compressed = eight
1,000-row U64 pages cycling 0..3; outlier = twenty 200-row U64/12-byte pages,
with each page's last integer widened; inline = twenty 500-row six-byte pages;
outlined = twenty 100-row 400-byte pages; nulls = twenty 100-row nullable byte
pages retaining 200 physical bytes under each null, with 75% nulls; sparse =
twenty 300-row nullable U64/64-byte pages, 75% nulls and every third source
position starting at one invisible; wide = sixteen 64-row pages of 64 U64
columns. Noncompressible integers multiply the row ordinal by
`0x9e3779b97f4a7c15` with wrapping arithmetic; byte contents cycle modulo 251.

Allocation instrumentation forwarded alloc, zeroed alloc, realloc and dealloc
to System, tracked only the conversion thread, and excluded prebuilt source
pages. Allocations count successful allocation/reallocation calls; allocated
bytes sum their requested sizes. Peak live bytes include builder/cursor state,
identity planning and final output, excluding allocator metadata. These are
serial component measurements, not whole-engine memory estimates.

| Fixture | LWC blocks | Used payload bytes | Allocations | Allocated bytes | Peak live bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| fixed | 1 → 1 | 64035 → 64035 | 45 → 51 | 500908 → 327048 | 258075 → 131240 |
| compressed | 1 → 1 | 2044 → 2044 | 45 → 51 | 376926 → 327048 | 196084 → 131240 |
| outlier | 2 → 2 | 96100 → 96100 | 163 → 167 | 920288 → 520668 | 294906 → 145648 |
| inline | 2 → 2 | 100078 → 100078 | 154 → 160 | 1296744 → 599754 | 392071 → 150184 |
| outlined | 20 → 13 | 808780 → 808507 | 714 → 437 | 7968328 → 3092416 | 214383 → 197560 |
| nulls | 7 → 2 | 408540 → 108332 | 347 → 160 | 3184286 → 567896 | 240983 → 155240 |
| sparse | 5 → 2 | 305270 → 113110 | 390 → 187 | 2303686 → 573856 | 230406 → 159408 |
| wide | 16 → 9 | 535936 → 530840 | 4284 → 2318 | 3904504 → 1822576 | 174976 → 165376 |

Identity bodies were dense/zero-byte in all component fixtures except sparse,
which changed from 830 to 816 bytes. The fixed control's initial staging
regression was traced to revisiting contiguous cached positions per column.
The endpoint fast path removed it. Combined conversion/encoding CPU improved
in every fixture, including controls with unchanged block counts. Isolated
compressed-column encoding retains a small validation overhead; its total CPU
is lower. Formerly failing oversized-page fixtures were correctness tests,
not misleading baseline timing comparisons.

The existing checkpoint workload inserted 1,000,000 sequential keys with
128-byte values, seed 339, one insert thread/session, batches of 100, freeze-all,
one measured checkpoint, no warmup, and two engine worker threads. Six fresh-root
pairs alternated execution order. Median end-to-end checkpoint time changed
from 51.60 ms (44.25–51.93) to 38.81 ms (35.44–45.67), about 25% lower. Every run
completed in one attempt with no retry wait.

Published-root walks validated bindings and counted reachable storage:

| Metric | Baseline | Candidate |
| --- | ---: | ---: |
| Physical rows | 1,000,000 | 1,000,000 |
| LWC blocks | 2,233 | 2,050 |
| Used LWC payload bytes | 134,131,683 | 134,120,862 |
| Identity body bytes | 8 | 8 |
| Column-index blocks | 3 | 3 |
| Total reachable table-file blocks | 2,238 | 2,055 |
| Median whole-process peak RSS, KiB | 189,686 | 197,472 |

The 183-block reduction is 8.2%; the column index remains 196,608 allocated
bytes. Reachability includes the superblock and active metadata block. Sparse
file logical length was not used as a packing metric. Whole-process RSS was
about 4% higher (baseline range 189,404–191,240 KiB; candidate 195,916–198,344),
despite lower component temporary peaks. It includes the entire load and
engine lifecycle, so these measurements do not establish an overall RSS
reduction or isolate allocator retention from live checkpoint buffers.

Two workers permit at most two unaccepted encode inputs plus the producer's
current builder/cursor. Component peaks therefore give a conservative
three-times-serial-peak bound for those conversion/encoding owners; retained
entry shapes and shared-storage-owned output buffers are additional existing
owners. The measured whole-process RSS includes them. Accepted variable bytes
now have two payload copies (source → staging → final buffer), versus five in
the old build path; rejected payload copies, temporary integer normalization
copies and intermediate encoded-column vectors are gone. Byte offset staging
changes from 16N bytes on this 64-bit host to 4(N+1).

Raw measurements, fixture source, configuration and hardware/compiler records
are retained under ignored `target/task-000339/`. Reproducible comparisons used
the saved baseline executable and isolated candidate component executables;
temporary allocator/timing code is absent from production and committed tests.

### Verification

- Workspace nextest: 2,494 passed, 3 skipped.
- Profiling-disabled storage nextest: 2,222 passed, 2 skipped.
- Formatting, strict workspace Clippy, branch style and test-contract gates
  passed; the final branch audit covers nine Rust files and 223 tests.
- Assertion review retained independent raw reader fixtures, seeded logical-row
  oracles and lifecycle/failure-drain coverage. Checkpoint readiness uses the
  existing semantic retry helpers; no timing sleeps or timeout changes.

## Impacts

LWC construction, prepared page views, user checkpoint and catalog checkpoint
share exact admission and direct writing. No public API, schema,
codec/version or durability-policy change. Block boundaries and null-byte
canonicalization can change persisted bytes and identity choices.
The durable ownership contract is documented in
[Data Checkpoint](../data-checkpoint.md#lwc-and-secondary-index-construction).

## Test Cases

- All supported value kinds, nullable/all-null integers, flat/FOR selection,
  exact fits, one-byte rejection, frozen-plan corruption and stale null bytes.
- Largest byte prefixes, three-to-two packing, combined row/byte limits,
  nonzero source ranges, bitmap and 1,024-row boundaries, bounded cursor
  storage, seed coalescing and rejected-prefix callback/staging immutability.
- A widening outlier charges the entire accepted integer column; repeated
  rejection preserves state and a subsequent admissible row succeeds.
- Seed 339 sparse/null visibility with an independent ordered oracle; floating
  point bit patterns and invalid physical byte descriptors beneath nulls.
- Prepared/live bitmap disagreement and rollback, exact unique/nonunique
  sidecars, full final blocks with invisible tails, binding/ordinal preservation.
- Production indexless/unique/nonunique checkpoint and reopen with poor integer
  compression plus aggregate inline bytes, cold scans/point reads, committed
  redo tail and a subsequent write proving engine health.
- Existing catalog, out-of-order encode/write, backpressure, drain, fatal and
  no-partial-root regressions remain covered.

## Open Questions

No blocking implementation questions remain. Related
[backlog 000006](../backlogs/000006-row-to-lwc-streaming-serialization-memory-optimization.md)
remains open for its broader streaming-memory acceptance criteria;
[backlog 000005](../backlogs/000005-varbyte-advanced-compression-for-lwc.md)
owns future byte codecs; and
[backlog 000208](../backlogs/000208-bound-checkpoint-rowid-coverage-spans.md)
owns unsupported RowID spans, including invisible final tails.
