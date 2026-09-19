---
id: 000311
title: Recovery-Owned Page Batches and Recycling
status: implemented
created: 2026-09-19
github_issue: 1081
---

# Task: Recovery-Owned Page Batches and Recycling

## Summary

Startup recovery now decodes ordinary user-table rows into flat descriptors,
copies admitted bytes directly into independently owned page batches, and returns
successful batches through completion for coordinator recycling. This removes
per-row owning value/update vectors and outlined-value allocations from hot-row
transport while retaining the existing borrowed page writers and scheduler.

## Context

Source Backlogs:

- docs/backlogs/000202-recovery-payload-allocation-bottleneck-bulk-recycling.md

Issue Labels:

- type:perf
- priority:medium
- codex

[Task 000309](000309-pipelined-recovery-with-parallel-page-replay.md) introduced
finite page jobs and identified allocator sensitivity when workers freed values
allocated during coordinator decoding.
[Task 000310](000310-borrowed-value-low-level-execution.md) supplied repeatable
borrowed row/update interfaces and writers that copy into page-owned storage.
There is no parent RFC. Backlog 000202 remains open for broader allocator causality;
this task implements and measures one recovery-local ownership design.

## Goals

- Remove ordinary hot-row nested ownership while preserving exact value bits,
  wire validation, classification, ordering, diagnostics, and accepted-job cleanup.
- Reuse flat batch storage independently of page histories and worker identity.
- Control used batch bytes and actual idle capacities, allowing oversized rows
  to make progress without impossible byte-credit waits.
- Establish repeatable recovery improvement with the default allocator.

## Non-Goals

Persistent formats, foreground DML, public value ownership, undo ownership, the
finite-job executor, catalog/DDL ownership, cold-delete scheduling, and hot-index
reconstruction are unchanged. Whole-group streaming, shared group ownership,
worker-side decoding, routing-map flattening, group-buffer pooling, and global
allocator replacement remain outside scope. Transport controls are not an RSS
or total-recovery limit.

## Rejected Alternatives

Shared group ownership would retain unrelated work behind delayed pages. Worker
decoding would duplicate coordinator wire traversal. Global allocator replacement
would not remove recovery's fine-grained ownership and would impose a deployment
dependency. These alternatives remain profiling-led follow-ups.

## Plan

### Decode and classification

`RedoGroupReader` owns block assembly, segment/tail validation, terminal state,
and accepted-prefix metadata. The owning `RedoLogStream` remains the catalog-scan
adapter; `RecoveryLogStream` publishes completely decoded groups for startup.
Both adapters validate a whole group before exposure and become terminal on error.
Checked transaction-frame, value, and collection readers are shared.

`DecodedGroup` takes the assembled allocation without copying it. Flat descriptors
retain scalar bits or checked `usize` byte ranges; inserts and updates own no
per-row vectors. DDL and catalog entries keep existing owned forms. Exceptional
keyed user redo is fully decoded and rejected only at the existing eligible replay
boundary, after applicable filtering.

BTreeMap directories preserve sorted traversal and last encoded key replacement,
including whole-table replacement. Routing keys remain distinct from payload row
IDs. Overwritten and filtered entries still undergo wire validation. Sparse
updates retain their original order and duplicate ordinals.

The coordinator moves transaction directories separately from group storage,
retains the current group across admission waits, and copies only admitted rows.
Counters, recovered CTS, replay floors, pivot/deletion filtering, DDL table drains,
and CreateRowPage allocation order retain their prior boundaries. A single pinned
input future survives completion wakes. The replay future is boxed once at the
recovery boundary to avoid propagating excessive future size into callers.

### Batches, completion, and memory controls

`PackedPageBatch` owns operation, value, update, and byte vectors. Admission
reserves reusable capacity, copies variable bytes directly from the group, rebases
ranges, and publishes the operation last. A pending batch can span transactions
and groups. Submission never captures a group reference. Workers acquire the page
once, resolve repeatable borrowed views in the synchronous mutation loop, validate
immediately before mutation, and preserve dirty marking after partial failure.

Successful jobs return batch storage with counts and insertion history. Exclusive
coordinator collection releases submission credit, restores history, and recycles
cleared storage through a common LIFO pool. No table handle, page guard, or borrowed
adapter escapes in the result. DROP and PageID reuse cannot inherit history from
the pool. The final global drain releases idle storage before index reconstruction.
Error, rejection, panic, and cancellation use normal owned destruction and the
existing settlement boundary; bootstrap rollback keeps accepted-job dependencies
alive until settlement.

| Setting | Default | Contract |
| --- | --- | --- |
| `target_batch_bytes` | 256 KiB | Positive used-storage flush target, including operation/value/update descriptors and payload. |
| `max_recycled_bytes` | 16 MiB | Aggregate actual idle vector-capacity cap; zero disables recycling. |

The operation/page/submission limits remain in force. A nonempty batch flushes
before a row would exceed its operation or byte target. An oversized row enters an
empty batch and dispatches alone. Capacity reuse uses geometric vector growth.
Recycling discards oversized-operation batches and capacities above twice the
byte target, then applies aggregate capacity and saturating page-plus-submission
entry limits. Log lengths and counts are checked before use; derived configuration
limits use saturating arithmetic. Internal counts and sizes from live vectors or
validated slices use ordinary arithmetic. Recycling checks the remaining idle
budget before adding a batch's capacity.

Whole groups, routing maps, insertion history, recovered pages, scheduler metadata,
and allocator overhead remain outside these controls. Active spare vector capacity
can exceed used storage. The payload path is assembled group -> reusable batch
payload -> row page; descriptor relocation and vector growth also cost work.

## Implementation Notes

Implemented packed startup decoding, independent page-batch ownership, borrowed
worker consumption, completion-based recycling, and both configuration settings.
Public builders validate the byte target before filesystem creation. Benchmark
overlays merge byte-size strings and record normalized byte counts; obsolete
`max_batch_bytes` and `max_buffered_bytes` keys remain rejected.

Startup stream tests consume the production packed adapter, including repair,
terminal-error, and metrics checks. Record-inspection tests use the production
catalog scan API. Shared fixture and inspection helpers are functions inside test
modules with narrow test-only re-exports; synchronization hooks remain available
for lifecycle tests.

Both decoding paths now share collection-count/hint validation, row-tag checks,
exact frame consumption, and group progress/CTS validation. Independent differential
tests compare complete semantic snapshots instead of following the packed result's
length or comparing debug strings. Coverage includes every DDL and row variant,
page and map/payload identities, empty collections, duplicate replacement, exact
value bits, noncanonical option flags, compact-fixture truncations, bounded seeded
mutations, and whole-group rejection through both production adapters. Error kinds
must agree; diagnostic text is not a compatibility contract.

### Controlled performance evidence, 2026-09-19

The owning reference is commit `0fd95da24d9c7ef2aa43a56f161743783c17fa69`.
A matched disposal diagnostic only returns owning batches through completion and
drops them on the coordinator. All builds used the same Cargo.lock, stable rustc
1.98.0, release profile with debug information, aarch64 Ubuntu 24.04, io_uring,
and glibc 2.39. Jemalloc comparisons used installed 5.3.0-2build1 through per-process
preload. Process mappings verified allocator selection; allocator tuning variables
were unset.

Each arm had three interleaved, unprofiled runs with fresh tmpfs roots and warm
clean-reopen caches. Preparation, full-content/index verification, and shutdown
were outside the recovery timer. Builds and profiles did not overlap timed runs.
The primary fixture used 10 million unindexed rows with 128-byte values, four
preparation threads, sixteen sessions, 100-row transactions, two replay workers,
four outstanding batches, sixteen active pages, and 256 operations per batch.
Pools were 1 GiB data, 512 MiB index, and 1 GiB readonly; durability was fsync and
read-ahead depth 32. Decode timing covers group decoding only; packing belongs to
replay/application time.

Medians are seconds; ranges describe the three replay measurements. RSS is median
whole-invocation maximum RSS in MiB from `wait4`, including preparation and teardown,
not transport capacity or a recovery-only memory measurement.

| Implementation / allocator | Bootstrap | Replay [min, max] | Decode | Peak RSS |
| --- | ---: | ---: | ---: | ---: |
| Owning / glibc | 5.629 | 4.026 [4.014, 4.053] | 2.160 | 1392 |
| Coordinator disposal / glibc | 4.803 | 3.013 [2.938, 3.027] | 0.973 | 1395 |
| Packed, recycling / glibc | 3.866 | 2.271 [2.170, 2.271] | 0.585 | 1382 |
| Packed, zero recycle cap / glibc | 4.037 | 2.379 [2.341, 2.421] | 0.574 | 1388 |
| Owning / jemalloc | 3.669 | 1.960 [1.956, 2.059] | 0.684 | 1369 |
| Packed, recycling / jemalloc | 3.856 | 2.136 [2.115, 2.171] | 0.527 | 1362 |

Packed recycling reduced default-allocator replay by 43.6% and bootstrap by 31.3%.
It also improved on matched coordinator disposal and zero-cap packing. Primary
replay coefficients of variation were 0.5–2.9%. RSS was broadly unchanged; these
measurements do not establish a substantial memory reduction or a hard RSS bound.

The original packed binary exposed a 9.0% jemalloc replay regression relative to
owning transport; the latest-source follow-up below supersedes that result for the
current implementation. The owning allocator cost is already much lower there,
while packing still incurs
descriptor relocation, payload copies, accounting, and coordinator admission work.
For the first matched pair, decode improved from 0.680 to 0.527 s, but application/
dispatch increased from 0.626 to 0.968 s. The packed profile attributed 0.212 s of
coordinator CPU inclusively to batch append. Thus lower decode time alone is not a
throughput claim. The default-allocator goal is achieved; this alternative-allocator
tradeoff remains recorded for backlog 000202, without claiming its mechanisms are
fully isolated.

Sensitivity medians below are replay seconds with glibc. Fixtures use one million
rows unless stated otherwise. Scalar-heavy means a U64 plus an empty byte value;
short/large values are 6 B/1 KiB. Worker/batch cases use 128 B values. Checkpoint
replay checkpoints 500,000 rows before reopening. Mixed replay uses 200,000 indexed
rows followed by 100,000 payload-update attempts and 100,000 key-changing/growing
update attempts, producing approximately 278k inserts, 101k updates, and 78k deletes.

| Fixture | Owning | Disposal | Packed | Packed, zero cap |
| --- | ---: | ---: | ---: | ---: |
| Scalar-heavy | 0.227 | 0.212 | 0.181 | 0.192 |
| Short bytes | 0.222 | 0.211 | 0.177 | 0.195 |
| Large bytes | 0.561 | 0.525 | 0.460 | 0.494 |
| Mixed mutations | 0.163 | 0.119 | 0.095 | 0.104 |
| Checkpoint-filtered | 0.292 | 0.237 | 0.167 | 0.178 |
| One worker | 0.387 | 0.273 | 0.211 | 0.234 |
| Two workers | 0.402 | 0.288 | 0.216 | 0.236 |
| Four workers | 0.404 | 0.295 | 0.225 | 0.238 |
| 32 operations/batch | 0.723 | 0.730 | 0.624 | 0.651 |
| 1,024 operations/batch | 0.417 | 0.289 | 0.216 | 0.233 |
| 8-KiB byte target | — | — | 0.444 | 0.467 |

No default-allocator throughput regression appeared in this matrix. Small batches
and the 8-KiB target increased scheduling cost; more workers did not improve this
coordinator-heavy fixture. Keep the initial 256-KiB/16-MiB defaults. Most replay
variation was below 5%; packed four-worker/mixed cases reached about 7.3%, and the
mixed/8-KiB zero-cap cases about 11–12%. Small-fixture RSS differed by a few MiB.

The production harness permits updates only as the final benchmark. Mixed runs
therefore used identically patched temporary harnesses allowing update preparation,
with payload-only updates before key-changing updates and recovery last. This
measurement-only extension was not added to the production plan schema. All 144
unprofiled runs verified full content; indexed runs also verified reconstructed
index content. Each fixture had one matching content fingerprint across its arms.

Separate Samply profiles attached after fixture shutdown and before reopening.
Coordinator recovery CPU samples were filtered by recovery call stacks and weighted
by thread CPU deltas. Inclusive malloc/free-stack CPU was 1.787 s owning, 0.856 s
with disposal, and 0.121 s packed. Allocator lock/futex attribution was respectively
1.009 s (25.3%), 0.035 s (1.2%), and 0.003 s (0.14%). These are sampled CPU
attributions, not elapsed wait time or allocation-call counts. Code inspection
shows the primary packed fixture avoids ten million per-row value-vector and ten
million outlined-value owning allocations; group/map allocations and vector growth
remain. The disposal control supports the allocation/free ownership explanation,
without resolving every jemalloc mechanism.

Ignored local artifacts under `target/task-000311/` retain plans, binary/source
manifests, diagnostic patches, 144 canonical results, variance/RSS summaries,
allocator mappings, and three symbolicated profile sidecars. Temporary datasets
and detached benchmark checkouts were removed. Reproduce from the tracked recovery
and checkpoint templates with the parameters above; mixed diagnostics require the
stated harness extension. Historical task-000309 experiments were not ranked as
matched references.

### Jemalloc follow-up on the latest source, 2026-09-19

Rebuilt the current working tree and last commit
`0fd95da24d9c7ef2aa43a56f161743783c17fa69` with the same stable compiler,
Cargo.lock, release profile, and default io_uring backend. Both used jemalloc
5.3.0-2build1 through `LD_PRELOAD`, verified in process mappings, with allocator
tuning variables unset. The fixture retained the primary experiment's 10 million
unindexed rows, 128-byte values, two workers, four outstanding batches, sixteen
active pages, and 256 operations per batch. Current byte settings were their
256-KiB/16-MiB defaults. Fresh tmpfs roots and warm clean reopen were used; fixture
preparation, full-content verification, and shutdown were outside recovery timers.

Eight unprofiled runs per rebuilt version were interleaved. Three additional
interleaved runs used the saved original packed binary, whose hash matched its
original manifest. Two separate profiles per version attached after preparation
and engine teardown. No build or profile overlapped an unprofiled measurement.
All 25 invocations recovered and verified 10 million rows with the same fingerprint.

Medians are seconds. Decode is part of stream refill; admission/dispatch is the
remaining replay wall interval and includes coordinator scheduling and waiting,
not just worker execution. Medians of individual intervals need not add exactly.

| Version | Runs | Bootstrap | Replay [min, max] | Decode | Apply/dispatch |
| --- | ---: | ---: | ---: | ---: | ---: |
| Last commit, owning | 8 | 3.525 | 1.965 [1.916, 2.327] | 0.686 | 0.619 |
| Latest working tree, packed | 8 | 3.590 | 1.989 [1.967, 2.163] | 0.483 | 0.869 |
| Original packed binary | 3 | 3.742 | 2.094 [2.060, 2.122] | 0.524 | 0.933 |

Latest replay was 1.2% slower by median and bootstrap 1.8% slower. Replay variation
was 6.9% for owning and 3.4% for latest packed; paired replay changes ranged from
-7.0% to +7.5%. Thus these measurements put latest jemalloc performance near parity,
without establishing a stable 1.2% regression. The saved original packed binary
remained slower, but the historical 9% penalty should not be applied to latest
source. Median whole-invocation peak RSS was 1,382 MiB owning and 1,371 MiB latest;
this includes preparation and teardown, not just recovery memory.

CPU attribution below averages two 2-kHz Samply profiles. Samples are weighted by
thread CPU deltas and restricted to replay. Inline stacks were resolved per
instruction address with GNU addr2line; the symbol sidecar's function-level inline
frames were insufficient for distinguishing sites inside large async functions.
Inclusive rows overlap and are not additive; CPU seconds are not elapsed waits.

| Replay CPU attribution | Owning | Latest packed | Original packed |
| --- | ---: | ---: | ---: |
| Coordinator, total | 1.889 | 1.932 | 2.023 |
| Group decode | 0.687 | 0.471 | 0.522 |
| Dispatcher admission | 0.208 | 0.463 | 0.498 |
| Packed batch append, within admission | — | 0.213 | 0.200 |
| Coordinator jemalloc stacks | 0.156 | 0.055 | 0.054 |
| Both workers, total during replay | 0.995 | 0.766 | 0.792 |
| Batch recycling, within coordinator work | — | 0.001 | 0.001 |

The regression mechanism is extra serial packing/admission work on the coordinator.
Owned replay transfers already-decoded row/value ownership into its batch. Packed
replay first produces group descriptors, then visits and copies them into batch
vectors, rebases variable offsets, copies payload, and maintains byte limits.
Latest admission adds about 0.255 CPU seconds; append accounts for about 0.213
seconds of that added stage. The decode saving is about 0.216 CPU seconds, leaving
the coordinator slightly busier even though worker and allocator CPU both fall.
Jemalloc makes the owning allocation path inexpensive enough that the transport
work can offset the allocation savings. Recycling itself is not the bottleneck.

This is not an extra full variable-payload copy compared with owning replay:
owning copies group bytes into MemVar during decode, while packed copies group
bytes into a batch during admission; both then copy into row pages. In latest
append, sampled memcpy itself accounts for only about 0.011 CPU seconds. Descriptor
writes, iteration, rebasing, vector bookkeeping, and operation construction account
for most of the remaining append cost. This fixture already has one variable value
per row, so contiguous payload alone is not a demonstrated solution to its overhead.
[Backlog 000203](../backlogs/000203-redo-value-component-and-contiguous-payloads.md)
should evaluate descriptor handling as well as bulk payload copies and retain a
multi-variable-column comparison.

The intervening refactors reduced coordinator CPU by about 0.091 seconds versus
the original packed binary, including roughly 0.050 seconds in decode and 0.034
seconds in admission. This comparison does not isolate the individual effects of
shared-reader changes, arithmetic simplification, or inline attributes. Further
optimization should target measured coordinator descriptor/admission work; the
profiles do not justify removing external wire validation or changing recycling
limits to address this fixture.

Artifacts under `target/task-000311/jemalloc-investigation-20260919/` retain exact
binary and source hashes, source snapshots/patch, plans, runner, 19 timing results,
6 profiles and verification results, process mappings, per-address symbolization,
and analysis scripts. Production source hashes were unchanged by this investigation.

### Verification

- 2,120 workspace tests and 1,971 alternate-libaio tests passed.
- Formatting, strict workspace clippy, and the 15-file branch style audit passed.
  Audit child checks used stable cargo; the installed moving nightly reports an
  unrelated existing `Atomic::fetch_update` deprecation.
- Initial implementation coverage: 94.63% across thirteen affected storage files; recovery 95.75%,
  decoder 97.10%, packed storage 98.73%, dispatcher 99.54%, table recovery 98.60%,
  recovery configuration 99.39%, block framing 97.60%, values 90.93%, serde 81.41%.
- After the shared-contract/parity refactor, focused coverage is 92.09% across its
  five changed decoding files: decoder 99.12%, stream 89.75%, block framing 97.75%,
  redo 96.39%, and serde 81.75%. Report: `target/task-000311/parity-coverage.md`.
- Test fixtures/helpers were reviewed for duplicated setup. No unsafe operation
  was added or removed, and the test runner and timeout policy were unchanged.

## Impacts

Recovery now owns flat transport storage and two coarse memory controls. Shared
stream reading continues serving catalog scans. Documentation defines the copy
budget, pool lifetime, memory exclusions, and existing completion-wait contract.
Persistent redo/page formats and the production allocator remain unchanged.

## Test Cases

- Every value tag, deterministic width/byte boundaries, exact floating-point bits,
  independent known encodings, and seeded randomized insert/update differential
  decoding against the owning path.
- Truncated scalars/lengths/payloads/ordinals/frames, invalid tags/counts, overflow,
  trailing frame bytes, CTS bounds, malformed overwritten rows, and whole-group
  rejection through both terminal stream adapters; multi-block 65,535-byte values.
- Map traversal/replacement, distinct map/payload IDs, original sparse-update order,
  catalog/DDL handling, and fully decoded keyed user redo before skips/rejection.
- Rebasing across groups, repeated borrowed access, source-group release before
  gated application, and page independence after batch storage reuse. Page-compatible
  inserts and updates cover every value type through production packed application.
- Actual-capacity/entry/high-water/zero-cap pool checks, exact-fit and oversized rows,
  partial byte-target flushing, FIFO/credit preservation, page retirement/reactivation,
  cross-page recycling, table drains, DROP/PageID reuse, and final pool release.
- Dirty marking after partial failure, validation opt-out, typed errors, Fatal
  precedence, rejection, worker panic, and real accepted-job bootstrap cancellation.
- Configuration defaults/builders, pre-filesystem validation, strict overlay merging,
  normalized round trips, and continued obsolete-key rejection.

## Open Questions

[Backlog 000203](../backlogs/000203-redo-value-component-and-contiguous-payloads.md)
tracks a future redo format with component-level value codecs and contiguous
variable payloads for bulk admission copies. It requires a format version bump;
older versions may be rejected. The current task retains the existing wire format.

[Backlog 000202](../backlogs/000202-recovery-payload-allocation-bottleneck-bulk-recycling.md)
remains open for allocator mechanisms and the measured jemalloc tradeoff. Inline
short-byte descriptors, shared groups, worker decoding, routing flattening, and
group-storage reuse remain possible profiling-led follow-ups. Whole-group streaming
remains [backlog 000130](../backlogs/000130-large-redo-transaction-streaming-replay.md),
and parallel hot-index reconstruction remains
[backlog 000110](../backlogs/000110-unify-hot-row-mem-scan-index-build-recovery.md).
