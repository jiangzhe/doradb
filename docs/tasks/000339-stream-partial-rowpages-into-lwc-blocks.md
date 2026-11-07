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
rows or repeating visibility analysis. Exact encoding plans precede copying,
and workers serialize accepted columns directly into the final block.
Checkpoint publication and restart preserve rows, identities and index entries.

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
Checkpoint flushed and retried that selection unchanged. Aggregate value
overflow could therefore fail after Transition despite every row being
individually admissible. Source backlog 000007 is closed as implemented.

## Goals

- Partition by exact encoded bytes and row count while preserving the largest
  fitting visible prefix, values, nulls, ordered identities and exact sidecars.
- Keep rejected values in their source page and retain bounded cursor metadata
  across pipeline waits.
- Freeze one compression decision for admission and writing, including the
  cost of widening already accepted integer values.
- Remove intermediate encoded-column buffers and hidden null byte payloads.
- Preserve coverage, bindings, publication/recovery ownership and persisted
  readers; establish packing, CPU and temporary-memory evidence.

## Non-Goals

No new codecs, format versions, overflow storage, public settings, row limits,
DML/replay admission rules, undo scans or publication policy. Unsupported RowID
spans remain backlog 000208. Further chunked staging and advanced byte codecs
remain separate work. The initially retained, unused internal ScanBuffer API
was removed during the subsequent column-view cleanup.

## Plan

### Admission and source ownership

`LwcBuilder::append_prefix` accepts a borrowed prepared view and source cursor,
returning physical consumption, visible rows appended, and `PageExhausted` or
`BlockFull`. The cursor caches at most 1,024 visible u16 source positions.
Rejected multirow parts split left-first; a rejected singleton remains pending
for the next block. A singleton that cannot fit an empty builder asserts the
DML/replay admission invariant with source-range and RowID diagnostics.

Reusable candidate summaries combine accepted rows with proposed rows before
copying. Integer extrema exclude nulls; byte statistics read only non-null
lengths. The complete plan includes headers, offsets, null bitmaps and the
cost of widening the entire accepted integer column. Current flat/FOR costs
are monotonic, permitting refinement to the first nonfitting visible row.
Only successful staging installs the accepted summary and plan. Failed probes
preserve staging, identities, callbacks and accepted plans. Direct catalog-row
appends use the same preview-before-copy rule and remain atomic.

`PageVectorView` borrows or owns the prepared bitmap and resumes traversal at
the current bitmap word. Reload asserts the frozen RowID base, physical count
and selected bounds; checkpoint also validates page identity. Accepted positions
drive staging, RowIDs and synchronous sidecar callbacks. Contiguous positions
use bulk copies. `IdentitySetSeeds` owns ordinal progression, coalescing each
accepted page/block portion while retaining logical-window hints where needed.

### Encoding and publication

Typed staging stores native numeric vectors, optional null words and N+1 u32
byte offsets. Null byte rows repeat the previous offset without reading their
physical descriptors. `finish(binding)` releases temporary RowIDs, identity
seeds and planner scratch, transferring only owned columns, binding and the
accepted plan to workers.

The writer allocates one zeroed 64-KiB DirectBuf and writes directly into exact
destination slices. Integer null slots normalize to the frozen minimum in owned
staging. Release assertions enforce counts, bitmap shape, column kinds, offsets,
byte lengths and total capacity; append-only byte staging establishes offset
order. Padding and checksums remain deterministic. Worker panic/unavailability
still passes through the completion error bridge, preserving fatal precedence
and draining accepted work. Persisted corruption and IO retain typed errors.

Version 3, flat tag 1, FOR tag 3, the 65,504-byte payload and 15,360-row cap are
unchanged. Checkpoint finalizes coverage and index shape before `finish`.
Partial pages end at the consumed physical boundary; exhausted pages retain
reserved ends. Full builders stay pending through invisible pages until the
next visible row or final pivot determines coverage. Borrowed views and page
guards end before capacity, ingress or drain waits. Logical write order,
worker-count occupancy, shared-storage backpressure and atomic publication
retain their existing contracts. Catalog construction uses the same writer.

### Shared primitives, codecs and readers

Shared primitives own native numeric storage, page-to-native conversion and
fixed-width little-endian decoding. LWC codecs own integer plans, flat/FOR
extent calculations and payload framing; a generated codec/type matrix gives
readers one runtime dispatch discriminator. Column-view code retains production
page access without the removed query-buffer and truncation APIs.

Readers bound persisted counts to the block header's u16 representation before
sizing and check accessed value-stream counts against the enclosing block.
Point reads validate framing, offset endpoints and the requested byte pair.
Prepared readers validate all byte offsets once on the first non-null access
and cache only valid metadata. Null reads validate bitmap extents without
reading physical values. Caches belong to one immutable block and layout.

Catalog full/projected traversals, cold index extraction and checkpoint delete
extraction reuse one prepared decoder per block. Cold workers reserve its two
column arrays in their worker budget; checkpoint delete extraction reserves
that memory for the block lifetime. Success, stop and admission failure release
charges with their owners. Future codecs may retain prepared state in a frozen
encoding variant and must revisit today's monotonic-prefix assumption.

## Implementation Notes

Valid aggregate byte overflow now partitions through checkpoint publication and restart while preserving exact row and index membership.
The 8,000-row six-byte fixture splits into 6,546 and 1,454 rows. Three pages of
100 outlined 400-byte values occupy two blocks of 162 and 138 rows.

Follow-up refactoring separated shared primitives, codec rules and block
orchestration, then removed the unused ScanBuffer/ScanColumn and value-buffer
APIs after confirming they had no production consumers. Tests of removed
buffer operations were removed with them. Independent reader serializers remain
test-only compatibility fixtures. No compatibility wrappers or benchmark
framework entered production.

Review replaced typed errors for proven source and frozen-input construction
contracts with release assertions at their owners. Direct encode completions
retain ThreadPool supervision and fatal cleanup. Source reload tests independently
vary base, count and both bounds, assert no staging/callback progress on failure,
and resume with the original selection. Frozen-input and real worker-panic
fixtures separately verify diagnostics, draining and fatal precedence.

Final review corrections bounded reader count arithmetic, kept null access
bitmap-only, separated point validation from prepared full-offset scans, and
reused decoders in catalog traversals and budgeted cold/checkpoint extraction.
Regression tests check validation counts, exact values, malformed offsets after
null reads, retry behavior and reservation cleanup.

### Recorded performance evidence

The original 2026-11-04 comparisons used Linux aarch64, 10 Apple vCPUs reported
at 2 GHz, rustc 1.98.1/LLVM 22.1.8, the default profiling feature and Cargo release
profile with debug information. Storage roots were fresh tmpfs directories using
io_uring. Six balanced baseline/candidate pairs alternated AB/BA order. Component
processes discarded one warmup sample and measured five samples of 40 conversions
using thread CPU clocks; source construction was outside timing/allocation windows.
The table retains medians of per-process medians in microseconds and serial peak
live bytes, including builder/cursor, identity planning and output allocations.

| Fixture | Conversion CPU, before → after | Encoding CPU, before → after | Blocks | Peak live bytes |
| --- | --- | --- | --- | --- |
| fixed | 11.18 → 11.06 | 12.08 → 5.19 | 1 → 1 | 258075 → 131240 |
| compressed | 26.31 → 12.67 | 3.32 → 3.56 | 1 → 1 | 196084 → 131240 |
| outlier | 29.73 → 24.44 | 14.94 → 8.25 | 2 → 2 | 294906 → 145648 |
| inline | 38.35 → 38.45 | 17.75 → 11.51 | 2 → 2 | 392071 → 150184 |
| outlined | 61.02 → 44.10 | 120.48 → 43.03 | 20 → 13 | 214383 → 197560 |
| nulls | 32.98 → 18.35 | 57.03 → 7.25 | 7 → 2 | 240983 → 155240 |
| sparse | 126.32 → 44.59 | 48.69 → 10.01 | 5 → 2 | 230406 → 159408 |
| wide | 306.12 → 217.58 | 110.27 → 38.89 | 16 → 9 | 174976 → 165376 |

Fixtures covered fixed F64, compressed U64, widening outliers, six-byte inline
values, outlined 400-byte values, 75% nulls with stale physical payloads, sparse
prepared visibility and 64-column rows. Combined conversion/encoding CPU improved
in every fixture; compressed encoding alone retained a small overhead. A contiguous
position fast path removed an initial fixed-control regression. Oversized pages
that failed on the baseline were correctness cases, not timing comparisons.

The million-row checkpoint used sequential keys, 128-byte values, seed 339,
one insert thread/session, batches of 100, freeze-all, one measured checkpoint,
no warmup and two engine workers. Six fresh-root pairs completed in one attempt
without retry waits. Median checkpoint time fell from 51.60 ms (44.25–51.93) to
38.81 ms (35.44–45.67), about 25%. Root walks validated bindings and membership.
LWC blocks fell from 2,233 to 2,050 (8.2%), used payload from 134,131,683 to
134,120,862 bytes, and total reachable table-file blocks from 2,238 to 2,055.
Identity bodies remained eight bytes and the column index remained three blocks.

Whole-process peak RSS rose from median 189,686 KiB (189,404–191,240) to
197,472 KiB (195,916–198,344), about 4%. Component temporary-memory improvements
therefore do not establish a whole-engine RSS reduction. With two workers,
two pending encode inputs plus the producer bound conversion ownership; retained
entry shapes and shared-storage output are additional owners. Accepted byte
payload copies fell from five to two, and 64-bit-host offset staging changed
from 16N to 4(N+1) bytes. Further staging work remains backlog 000006.

The 2026-11-05 primitive/codec refactor rejected nested type/codec enums after
ordinary lookup CPU increased by up to 20%. Generated dispatch removed that
regression: six alternating release pairs against the pre-refactor task measured
ordinary lookups unchanged to 25.4% faster, prepared lookups 2.1% faster to 1.1%
slower, and preparation 19.3% faster to 3.8% slower. Across eight component fixtures,
conversion changed -12.7% to +2.1% and encoding -19.5% to +1.6%; allocation counts,
peak staging allocations, block counts, payload lengths and membership were
unchanged. These comparisons precede the final reader-cache fixes.

Historical coverage after column-view cleanup was 98.21% for column views,
94.23% for primitives and 93.89% for LWC (94.07% combined). Remaining misses
were predominantly invariant/error branches and reader operations. This coverage
predates later assertion and reader fixes. Original benchmark/coverage artifacts
were recorded under ignored `target/task-000339/` and `target/lwc-refactor/`;
those directories are absent from this resolve worktree. Performance and coverage
were not rerun during resolution; the figures above retain the recorded evidence.

### Final verification (2026-11-07)

- Workspace nextest passed 2,505 tests; 3 skipped.
- Profiling-disabled storage nextest passed 2,233 tests; 2 skipped.
- The branch style gate passed 23 Rust files and 422 test contracts, including
  formatting and strict workspace Clippy; inventory contained 2,513 tests overall.
- Assertion/overlap review retained independent manual wire fixtures for all
  36 integer type/width combinations, literal float bits, seed-339 logical-row
  oracles, and distinct builder, reader, catalog and checkpoint lifecycle tests.
  Shared fixture helpers preserve named boundary cases. Exact contract duplicate
  detection found none in the selected scope. Readiness uses semantic retry
  helpers and completion state rather than timing sleeps.

## Impacts

LWC construction, prepared page views, user/catalog checkpoint and cold index
extraction share exact admission, direct writing and reusable decoding. Public
APIs, schema, codec/version and durability policy are unchanged. Block boundaries,
null-byte canonicalization and identity choices may change persisted bytes.
Decoder allocations are now explicitly charged to existing index-build budgets.
The ownership contract is documented in
[Data Checkpoint](../data-checkpoint.md#lwc-and-secondary-index-construction).

## Test Cases

- All value kinds, nullable/all-null integers, flat/FOR selection, exact fit,
  one-byte rejection, frozen-plan corruption and stale null descriptors.
- Largest byte prefixes, three-to-two packing, combined row/byte limits,
  bitmap and 1,024-row boundaries, bounded cursor storage and seed coalescing.
- Rejected-prefix immutability, reload mismatches and whole-column widening
  followed by successful reuse of the accepted plan.
- Prepared/live bitmap disagreement, sparse identities, exact float bits,
  host-endian bitmap handling and independent manually packed reader fixtures.
- u16 persisted count limits, malformed lengths/offsets, bitmap-only null reads,
  lazy validation reuse, and decoder admission cleanup on success/failure/stop.
- Indexless, unique and nonunique checkpoint/reopen with aggregate byte overflow,
  exact sidecars/bindings, invisible final tails, cold reads, committed redo tail
  and subsequent writes proving engine health.
- Catalog full/projected traversal, out-of-order encode/write, backpressure,
  worker panic, failure drains, fatal precedence and no partial root publication.

## Open Questions

No blocking implementation questions remain. Existing follow-ups retain the
source task and deferral findings:

- [Backlog 000006](../backlogs/000006-row-to-lwc-streaming-serialization-memory-optimization.md):
  remaining staging and concurrent conversion memory.
- [Backlog 000005](../backlogs/000005-varbyte-advanced-compression-for-lwc.md):
  advanced byte codecs and their prefix-planning requirements.
- [Backlog 000208](../backlogs/000208-bound-checkpoint-rowid-coverage-spans.md):
  unsupported RowID coverage spans, including invisible final tails.
