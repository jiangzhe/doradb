---
id: 000311
title: Recovery-Owned Page Batches and Recycling
status: implemented
created: 2026-09-19
github_issue: 1081
---

# Task: Recovery-Owned Page Batches and Recycling

## Summary

Startup recovery decodes ordinary user-table rows into flat descriptors, copies
admitted payloads into independent page batches, and returns successful batches
for coordinator recycling. This removes per-row owning value/update vectors and
outlined-value allocations from hot-row transport while preserving replay order,
wire validation, borrowed page writers, and accepted-job cleanup.

## Context

Source Backlogs:

- docs/backlogs/closed/000202-recovery-payload-allocation-bottleneck-bulk-recycling.md

Issue Labels:

- type:perf
- priority:medium
- codex

[Task 000309](000309-pipelined-recovery-with-parallel-page-replay.md) identified
allocator contention when replay workers freed values allocated by the decoding
coordinator. [Task 000310](000310-borrowed-value-low-level-execution.md) supplied
borrowed row/update interfaces whose writers copy values into page-owned storage.
There is no parent RFC. This task resolves source backlog 000202 by identifying
and addressing the recovery ownership bottleneck. Further allocator comparisons
are not a current priority, as accepted during task resolution.

## Goals

- Remove ordinary hot-row nested ownership without changing decoded meaning,
  classification, ordering, diagnostics, or failure settlement.
- Reuse batch storage independently of page identity and replay history.
- Bound batch targets and idle capacity while allowing oversized rows to progress.
- Establish repeatable recovery improvement with the default allocator.

## Non-Goals

Persistent formats, foreground DML, public value/undo ownership, the finite-job
executor, catalog/DDL ownership, cold-delete scheduling, and index reconstruction
remain unchanged. Whole-group streaming, shared group ownership, worker decoding,
routing-map flattening, group-buffer pooling, and global allocator replacement
remain separate work. Transport limits are not total-recovery memory or RSS limits.

## Rejected Alternatives

Shared groups would retain unrelated input behind slow pages. Worker decoding
would duplicate wire traversal. Global allocator replacement would retain
fine-grained payload ownership and impose a deployment dependency. The selected
design changes recovery ownership locally and works with the default allocator.

## Plan

### Decoding and replay ownership

A shared group reader owns physical validation, segment/tail handling, and
accepted-prefix metadata. Catalog scans use its owning transaction adapter;
startup uses a packed adapter. Both validate every transaction before publishing
a group and become terminal on failure. Their checked frame, collection, row-tag,
and value readers share the same format contract.

Packed groups retain the assembled input allocation and flat scalar/byte-range
descriptors. Catalog entries and DDL, including accompanying DML, remain owned.
Ordered directories preserve last-encoded-key replacement, distinct routing and
payload identities, and original sparse-update order. Overwritten and filtered
entries are still validated; exceptional keyed user redo is rejected at its
existing eligible replay boundary.

Admission copies only eligible rows into independent page batches. Workers borrow
batch-owned values, acquire a page once, validate before mutation, and preserve
dirty marking after partial failure. Batches may span groups and transactions;
accepted jobs retain no group reference. The payload path is group -> batch ->
row page, replacing the owning path's group -> owned value -> row page.

### Batching, reuse, and failure

Successful completion returns batch storage and insertion history. Coordinator
collection releases submission credit and makes storage reusable; page identity
and retained insertion history never belong to the reuse pool. DROP drains the
table before page identity reuse. Final replay drain releases idle storage before
index reconstruction. Failure discards pending work and settles accepted jobs;
bootstrap cancellation keeps storage dependencies alive until jobs finish.

| Setting | Default | Contract |
| --- | --- | --- |
| `target_batch_bytes` | 256 KiB | Positive used-storage flush target, including descriptors and payload; an oversized operation runs alone. |
| `max_recycled_bytes` | 16 MiB | Aggregate actual idle capacity cap; zero disables recycling. |

Operation, active-page, and submission limits remain in force. Reuse rejects
oversized batches and excessive spare capacity, and bounds both retained capacity
and entry count. Whole groups, routing/history state, recovered pages, and allocator
overhead remain additional costs. The input read stays pinned across completion
wakes; collecting worker results neither cancels nor restarts it.

## Implementation Notes

Implemented packed startup decoding, independently owned page batches, borrowed
worker consumption, completion-based reuse, and public byte controls without
changing the redo format or production allocator. The default-allocator benchmark
improved substantially; the latest jemalloc comparison was near parity because
serial descriptor packing largely offset cheaper decoding and allocation.

### Review and compatibility outcomes

- Both decoders share acceptance and integrity-error classification. Differential
  tests independently consume both paths to completion and compare full semantic
  contents, including exact floating-point bits. Diagnostic text may differ.
- Group and transaction format contracts live in source comments referenced by
  both deserialization paths. Conceptual guides retain ownership and correctness
  rules; configuration usage and benchmark evidence have separate homes.
- External wire lengths/counts remain checked; configuration-derived limits retain
  overflow-safe sizing. Counts and sizes already bounded by live internal storage
  use ordinary arithmetic. Small hot functions received targeted inline hints.
- Tests exercise production decoding and replay paths. Shared fixture/inspection
  helpers live in test modules with narrow re-exports; synchronization hooks retain
  control of cancellation and lifetime boundaries.
- Builders validate configuration before filesystem creation. Benchmark overlays
  accept byte-size strings and normalize byte counts. Both new default constants,
  `DEFAULT_RECOVERY_TARGET_BATCH_BYTES` and `DEFAULT_RECOVERY_MAX_RECYCLED_BYTES`,
  are publicly re-exported through `doradb_storage::conf`; an external consumer
  compiled using those constants with the public configuration builders.
- Rebase resolution preserved origin/main's concise shutdown overview and added
  only the recovery completion/reuse contract and a link to the recovery guide.

### Controlled performance evidence, 2026-09-19

The owning reference was `0fd95da24d9c7ef2aa43a56f161743783c17fa69`. Builds used
matched Cargo.lock, stable rustc 1.98.0, release debug information, aarch64 Ubuntu
24.04, io_uring, and glibc 2.39. Jemalloc was 5.3.0-2build1 through per-process
preload, verified in mappings with allocator tuning variables unset.

The primary fixture had 10 million unindexed rows with 128-byte values, four
preparation threads, sixteen sessions, 100-row transactions, two replay workers,
four outstanding batches, sixteen active pages, and 256 operations per batch.
Pools were 1 GiB data, 512 MiB index, and 1 GiB readonly; durability was fsync and
read-ahead depth 32. Fresh tmpfs roots used warm clean reopen. Preparation, full
content/index verification, and shutdown were outside recovery timers. Builds and
profiles did not overlap timing runs. Decode excludes admission/packing.

The initial matrix used three interleaved unprofiled runs per arm. Coordinator
disposal returned owning batches for destruction without reusing nested storage;
zero-cap packing retained the packed representation with recycling disabled.
Medians are seconds; RSS is whole-invocation peak MiB, including preparation.

| Initial implementation / allocator | Bootstrap | Replay [min, max] | Decode | RSS |
| --- | ---: | ---: | ---: | ---: |
| Owning / glibc | 5.629 | 4.026 [4.014, 4.053] | 2.160 | 1392 |
| Coordinator disposal / glibc | 4.803 | 3.013 [2.938, 3.027] | 0.973 | 1395 |
| Packed, recycling / glibc | 3.866 | 2.271 [2.170, 2.271] | 0.585 | 1382 |
| Packed, zero recycle cap / glibc | 4.037 | 2.379 [2.341, 2.421] | 0.574 | 1388 |
| Owning / jemalloc | 3.669 | 1.960 [1.956, 2.059] | 0.684 | 1369 |
| Original packed / jemalloc | 3.856 | 2.136 [2.115, 2.171] | 0.527 | 1362 |

Default-allocator replay improved 43.6% and bootstrap 31.3%, exceeding both disposal
and zero-cap packing. Primary replay variation was 0.5–2.9%; RSS was broadly
unchanged and establishes neither substantial memory savings nor an RSS bound.
The original 9% jemalloc replay penalty motivated the follow-up below.

The 144-run matrix also covered empty/6-byte/1-KiB payloads, mixed mutations,
checkpoint-filtered replay, one/two/four workers, 32/256/1,024-operation batches,
and an 8-KiB byte target. No glibc replay regression appeared. Most sensitivity
fixtures used one million rows; mixed replay used 200,000 indexed rows followed
by 100,000 payload-update and 100,000 key-changing/growing-update attempts. Small
batches and the 8-KiB target increased scheduling cost; more workers did not help
the coordinator-heavy fixture. Most variation stayed below 5%, with some mixed,
four-worker, and small-byte-target cases reaching 7–12%. Defaults were retained.

Mixed recovery required an identical temporary harness extension allowing update
preparation before the final recovery phase. It was not added to the production
plan schema. All runs verified full content; indexed fixtures verified rebuilt
indexes. Each fixture had one fingerprint across its comparison arms.

Separate glibc profiles attributed coordinator malloc/free CPU to 1.787 s owning,
0.856 s disposal, and 0.121 s packed; allocator lock/futex attribution was 1.009 s,
0.035 s, and 0.003 s. These sampled CPU values support the ownership/contention
explanation, without establishing every allocator mechanism. The primary packed
fixture avoids ten million row-value-vector and ten million outlined-value owning
allocations; group/map allocations and vector growth remain.

### Jemalloc follow-up after decoder and accounting refinements

The rebuilt implementation and owning reference each had eight interleaved runs;
the saved original packed binary had three additional control runs. Conditions
matched the primary fixture above. Two separate profiles per version attached
after preparation and engine teardown. All 25 invocations verified 10 million rows.

| Version | Runs | Bootstrap | Replay [min, max] | Decode | Apply/dispatch |
| --- | ---: | ---: | ---: | ---: | ---: |
| Owning reference | 8 | 3.525 | 1.965 [1.916, 2.327] | 0.686 | 0.619 |
| Refined packed implementation | 8 | 3.590 | 1.989 [1.967, 2.163] | 0.483 | 0.869 |
| Original packed binary | 3 | 3.742 | 2.094 [2.060, 2.122] | 0.524 | 0.933 |

Latest replay was 1.2% slower by median and bootstrap 1.8% slower. Replay variation
was 6.9% owning and 3.4% packed; paired differences ranged from -7.0% to +7.5%.
This supports near parity, not a stable 1.2% regression or the original 9% penalty.
Median whole-invocation RSS was 1,382 MiB owning and 1,371 MiB packed. Apply/dispatch
includes scheduling and waiting; independently computed medians need not add.

Two 2-kHz profiles per version were weighted by thread CPU deltas and restricted
to replay. Per-address GNU addr2line inline stacks distinguished sites inside large
async functions; function-level symbol-sidecar frames were insufficient. Inclusive
CPU rows overlap and are not elapsed waits.

| Mean replay CPU seconds | Owning | Refined packed | Original packed |
| --- | ---: | ---: | ---: |
| Coordinator total | 1.889 | 1.932 | 2.023 |
| Decode | 0.687 | 0.471 | 0.522 |
| Admission | 0.208 | 0.463 | 0.498 |
| Batch append, within admission | — | 0.213 | 0.200 |
| Coordinator jemalloc stacks | 0.156 | 0.055 | 0.054 |
| Both workers during replay | 0.995 | 0.766 | 0.792 |
| Recycling, within coordinator work | — | 0.001 | 0.001 |

Serial packing explains the tradeoff: admission adds about 0.255 CPU seconds while
decode saves 0.216 seconds. Append accounts for 0.213 seconds, mainly descriptor
writes, iteration, rebasing, and bookkeeping; memcpy itself accounts for only
0.011 seconds. Both transports copy variable payload twice overall, with packing
moving the first copy from decode to admission. This one-variable-column fixture
already performs one payload copy per admitted row; contiguous payload alone is
not a demonstrated remedy. The refinements saved about 0.091 coordinator CPU
seconds, without isolating individual arithmetic, shared-reader, or inline effects.

Local artifacts under `target/task-000311/` retain initial plans, source/binary
manifests, diagnostic patches, 144 canonical results, and profiles. Its
`jemalloc-investigation-20260919/` directory retains source snapshots, hashes,
19 timing results, six profiles, mappings, and per-address analysis. Artifacts are
ignored and may be unavailable elsewhere; reproduce using the tracked recovery
and checkpoint templates with the controls above and stated mixed-harness change.
Temporary datasets and diagnostic worktrees were removed. Historical task-000309
results are context, not a matched ranking against this experiment.

### Final verification

- Fresh validation passed 2,120 workspace tests and 1,971 alternate-libaio tests.
- The mandatory branch audit passed all 16 changed Rust files, including formatting
  and strict workspace Clippy; child checks used the same stable toolchain.
- Public constant imports/builders compiled from an external-consumer snippet;
  all three recovery configuration tests also passed independently.
- Historical coverage was 94.63% across thirteen initial implementation files.
  After decoder parity work, focused coverage was 92.09% across five files:
  decoder 99.12%, stream 89.75%, framing 97.75%, redo 96.39%, and serde 81.75%.
  Coverage was not rerun for the final API-export/documentation edits.
- Fixture/helper duplication was reviewed. No unsafe operation, test runner, or
  timeout policy changed; the inventory reflects the added recovery modules.

## Impacts

Recovery and benchmark configuration expose two byte controls and their public
default constants. Startup uses independent reusable transport while catalog
scans retain owning records. Exact format contracts remain in source; conceptual
guides describe ownership, progress, and memory limits. Persistent redo/page
formats, public row-value ownership, and the production allocator are unchanged.

## Test Cases

- Owning/packed parity for all value, row, DDL, and keyed variants; exact bits,
  map replacement and identities, sparse-update order, and eligible filtering.
- Invalid tags/counts/ranges, truncations, oversized advertised lengths, trailing
  frame bytes, CTS bounds, malformed overwritten entries, and whole-group failure;
  multi-block 65,535-byte values and seeded mutation cases.
- Batch reuse across groups/pages, repeatable borrowed views, source-group release
  before worker application, and every value type through production page writes.
- Actual-capacity, entry, high-water, zero-cap, exact-fit, and oversized-row limits;
  FIFO/credit behavior, table drains, DROP/PageID reuse, and final pool release.
- Dirty marking after partial failure, validation opt-out, typed errors, Fatal
  precedence, rejection, worker panic, and accepted-job bootstrap cancellation.
- Builder/default validation before filesystem creation, strict benchmark overlays,
  normalized settings, and external visibility of the new default constants.

## Open Questions

[Backlog 000203](../backlogs/000203-redo-value-component-and-contiguous-payloads.md)
tracks component-level value codecs and contiguous redo payloads. It must consider
descriptor handling and multi-variable-column workloads, not only memcpy counts.
A format version bump with rejection of older versions is acceptable.

Whole-group streaming remains [backlog 000130](../backlogs/000130-large-redo-transaction-streaming-replay.md);
parallel hot-index reconstruction remains
[backlog 000110](../backlogs/000110-unify-hot-row-mem-scan-index-build-recovery.md).
