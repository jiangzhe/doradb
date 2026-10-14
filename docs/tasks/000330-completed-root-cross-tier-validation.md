---
id: 000330
title: Completed-Root Cross-Tier Validation
status: implemented
tags: [storage, index, ddl, validation, parallelism, performance]
created: 2026-10-10
github_issue: 1147
---

# Task: Completed-Root Cross-Tier Validation

## Summary

CREATE INDEX seals cold construction before hot extraction, releasing cold runs,
merge completions, construction ledgers, and original outlined keys in both modes.
Unique CREATE validates complete hot input through bounded parallel forward
probes, ordered cursors, or reverse searches over resident hot runs. Compact
evidence binds installation to both exact inputs.

## Context

Parent RFC:

- docs/rfcs/0033-parallel-disk-tree-construction-and-checkpoint-application.md

RFC Phase: 2 — Completed-Root Cross-Tier Validation

Source Backlogs:

- docs/backlogs/000104-stream-parallel-create-index-cold-build.md

Issue Labels:

- type:feature
- priority:high
- codex

Design and performance baseline: `78c6c3dd55d83331498467e6a0b593715e8e6a6a`.
Task 000329 supplied settled private DiskTree roots and parallel construction.
Its cold runs remained reachable through the builder, prepared merge,
completion, and retained-vector validator during hot construction. Removing
only the validator's reference would not release their allocations.

RFC 0032 supplied complete resident hot runs, partition consumption, duplicate
precedence, retained job settlement, and private installation. This phase keeps
cold and hot preparation sequential and preserves those contracts. Backlog
000104 remains open for parallel extraction and external-memory acceptance.

## Goals

- Release all cold construction-input owners before hot admission while keeping
  the exact completed root readable and its private allocations owned.
- Provide three bounded parallel validation methods with deterministic conflict
  ranking, direction-specific coverage, and exact installation bindings.
- Preserve execution/Fatal precedence, hot/hot precedence, publication order,
  observer-detachment behavior, and resumable failure settlement.
- Calibrate a small deterministic selector and document memory, concurrency,
  public CREATE latency, and regressions with independently checked fixtures.

## Non-Goals

Parallel cold extraction, concurrent hot/cold preparation, checkpoint delta
reconciliation, external sorting/spill, foreground stream redesign, and live
cache adaptation remain outside this task. There is no new public DDL strategy
switch, persisted format, allocator, runtime backend, or timeout policy.

## Rejected Alternatives

- Retaining a cold vector, including through compact-looking wrappers around a
  prepared merge, fails the allocation-release contract and obstructs Phase 3.
- Reusing foreground candidate batches without a separate byte allowance leaves
  branch copies, decoded keys, and persisted-validation temporaries unbounded.
  The build reader instead shares validated-node semantics with bounded state.

## Plan

### Sealed ownership and CREATE ordering

`ColdBuildProgress::seal` validates its retained builder before consuming it
and its completion together. It requires exact invocation/merge identity,
complete input and output coverage, cold distinctness when required, valid
root shape, and settled CPU/write/allocation ledgers. It also checks the exact
file incarnation, table, index generation, and unique/non-unique runtime kind.
Empty input has no root; the super block cannot become a tree root.

`ColdBuildIdentity` contains only an invocation token and catalog/timestamp
binding. `SealedColdBuild` owns a shared completed root for both index kinds.
Only unique builds issue cloneable `ColdReadAuthority` leases, which retain the
fork, runtime, readonly pool, and operation guard without retaining construction
inputs. Metrics are copied before construction state is destroyed. CREATE retains
the sealed artifact before any subsequent await.

Cold construction returns through its retained lifecycle; hot construction derives
validation authority from that owner and stages only an `InMemorySecondaryIndex`.
After hot installation and settlement release every reader, `finish_reading`
requires exclusive, certain ownership and unwraps the disk runtime. Rejection
retains the sealed artifact. Success retains the private fork in `ReadyToPublish`
and assembles the final `SecondaryIndex` with its owned runtime before catalog
commit. The fork transfers once at publication; runtime types need no `Clone`.

The production sequence is cold collection/build/settlement, sealing, complete
hot extraction, strategy selection, one hot-boundary preparation, validation
and packing, private installation, then existing catalog/root/layout publication.
The seal accepts multiple resident cold runs without flattening. The pipeline
uses optional cold authority: absence skips validation for non-unique CREATE
and trusted recovery; empty unique roots retain required checked evidence.
Selection owns the cold authority and admission. Packing consumes that single
optional selection, binds it without reselection, and copies only the plan's
expected cold identity into ready-tree evidence.

### Parallel validation and evidence

Forward batches and ordered cursors execute in asynchronous hot partition
consumers. Forward groups at most 128 sorted borrowed keys by subtree, visits
required nodes once per batch, and preserves output order without copying keys. They examine every assigned hot rank after conflicts and inhibit
only packing; hot uniqueness checks still consume complete input. Ordered
cursors reuse their current leaf, advance adjacent leaves, and reseek large
key gaps. Encoded comparisons avoid hit-key and candidate-batch copies.

Reverse validation plans disjoint half-open cold intervals from a bounded
structural frontier independently of hot cuts. Retained asynchronous jobs scan
those intervals in borrowed views of at most 128 leaf slots and search every
complete resident hot run before hot packing starts. Encoded cold keys are not
copied. Per-run lower bounds persist across batches and leaves, using an initial
binary search followed by galloping and binary search over later intervals. Summed lower bounds identify the earliest global hot rank;
original run provenance breaks equal-key ties consistently with the hot merge.
A reverse conflict inhibits packing while preserving hot/hot validation.

Every summary binds both identities, method, assigned partition/range, and
complete consumption. Reverse evidence checks ordered adjoining endpoints,
open outer bounds, exhaustion, job settlement, and the sealed cold count.
Counts alone cannot authorize installation. Compact completion retains the
selected method and typed hot-partition, cold-range, or empty-tier coverage.
Installation checks it against the actual hot completion and CREATE's sealed
artifact. Coverage/execution failures precede duplicates; hot/hot wins over
hot/cold when both complete checks report conflicts.

### Admission, read lifetime, and settlement

The released cold invocation budget admits readers, paths, selected boundary
keys, lookup scratch, and job ledgers before submission.
Checked bounds include retained boundary storage and a common per-worker allowance
for the larger of the Forward frontier/results and Reverse positions/accumulators.
Each production reader retains a height-bounded cursor path, a batch-bounded
lookup frontier and at most one pinned block; readonly-pool pins and
storage-owned buffers are reported separately from scratch and RSS.

All methods share the hot-build worker ceiling. Admission can reduce workers
before cuts are prepared; failure to fit one reader preserves a typed resource
cause. Validation targets 32 entries per job and at most four jobs per worker.
Empty tiers and unchecked builds retain the original construction sizing.

Reverse planner/completion slots and read leases belong to retained pipeline
owners, so abandoning a borrowed execution future does not cancel accepted
work. Cleanup stops admission and drains every accepted job before reclamation.
A read interrupted by unwind or returning uncertain Fatal completion retains
its private allocation map and reader allowance. Unrelated hot failures follow
normal sealed-fork cleanup and do not unnecessarily retain its writer claim.

### Measured selection policy

Let H/C be complete hot/cold counts, R the resident hot run count, P the planned
hot partition count, and L the sealed tree height plus one. Final relative
weights are disk lookup `40 * L`, resident-run lookup `10`, scan `1`, comparison
`2`. Scores use saturating `u128` arithmetic:

- Forward: `40 * L * H`.
- Reverse: `C + 10 * C * R`.
- Ordered: `40 * L * P + C + 2 * (H + C)`.

Empty tiers are handled first. Ties favor ordered, forward, then reverse.
Selection works with profiling disabled. Height captures the measured disk
lookup slope; the evidence did not justify run-length logarithms, overlap
prediction, cache sampling, or additional workload-specific parameters.

## Implementation Notes

Implemented sealed-root validation and early cold-input release for RFC 0033 Phase 2.
Validation binds exact inputs, retains failed work, and records publication
metrics. Phase 3 can change cold run production under the same contract.

`disk_builder` validates construction; `sealed_cold` owns completed resources.
`cold_reader` admits reads/ranges; `cross_tier` owns selection and evidence.
Read scopes share one reservation through settlement or uncertain IO.
Checked invocation and root facts live in `SealedColdMetadata`, stored directly
in the shared root. The sealed-build constructor accepts the file, runtime,
guard, and budget separately, removing the mirrored `SealedColdParts` handoff
payload while keeping seal validation in the builder and resource wrapping
inside `sealed_cold`.

### Batched lookup follow-up

Forward and Reverse now share work across internal 128-entry lookup batches.
Ordered, method-selection scores, job ceilings, sealed-read settlement, and
conflict/coverage precedence retain their existing contracts. Profiling separates
logical probes, completed batches, and worker node accesses; cached accesses are
included and range planning is excluded from the node-access count.

Independent map/rank oracles cover batch and leaf boundaries, prefix-compressed
and wide keys, duplicate provenance, clipped ranges, and monotonic search gaps.
Operation-count checks protect shared node traversal and reused search positions.
Scalar reference code exists only in test helpers for release calibration.

The reduced follow-up comparison verified 22 measured CREATE invocations and
two warm-ups at one million total rows and four workers. Reverse validation
was shorter in all three paired observations, while total CREATE results were
mixed; single samples do not establish an end-to-end gain. Common admission
increased by 8 KiB per worker. Reproduction scripts, source/binary provenance,
raw samples and the full report are under
`target/task-000330/batched-lookups/`. Selector weights were not retuned.

### Review discoveries and deviations

- `CreateIndexProgress` groups cold and hot construction behind lifecycle
  owners. The cold enum encodes NotStarted, Building, Sealed, ReadyToPublish, and
  Transferred; a rejected seal preserves Building. The hot enum directly owns
  the pipeline in Build and both pipeline and ready tree in Tree, with distinct
  sub-state enums scoped to each payload. Both owners are boxed to keep the enum
  small; the pipeline keeps one allocation across transitions, and the ready tree
  is boxed before taking the retained pipeline for handoff. The enum-size lint
  exception is removed; the profiling-enabled aarch64 test build measures 24
  bytes for the enum, down from 3,272. Installation and settlement update
  sub-states in place; only ready-tree handoff and discard move retained owners.
  Typed installation rejection restores Ready; success advances to Installed.
  Settlement resumes cancellation without repeating abort, while destructive
  panic states forbid retries. Settled states make cleanup idempotent and retain
  publication profiling until explicit discard.
  Transaction, staged runtime, and layout remain separate transfer slots.
- Constrained-cache experiments exposed an existing readonly free-frame wake
  bug: consecutive `notify(1)` calls could coalesce before waiters polled,
  leaving free capacity stranded. `notify_additional(1)` now wakes one waiter
  per returned frame. A deterministic two-waiter regression failed on the old
  implementation and passes with the fix.
- Sealed-fork retention is scoped to uncertain persisted reads so unrelated
  hot failures can cleanly shut down; lifecycle regressions pass.
- Oracle review uses explicit key/provenance ordering. The public handoff test
  reads the sealed unique root after original inputs disappear and verifies
  published hot and cold lookups in both modes.

### Calibration and public measurements

Release experiments used Linux aarch64 with usable io_uring, Rust 1.98.1,
standard release/debug-info settings, and the existing allocator. Fixtures and
complete content/multiplicity verification stay outside CREATE timing. The
baseline received only interleaved fixture support. Controlled private builds
forced each method after selection; no runtime strategy switch was added.

The first completed sweep measured 567 invocations across 42 scenarios, with
three interleaved repetitions. It varied 32,768/262,144 rows, ratio extremes and
1/5/10/25/75/90/95/99% hot input, one/two workers, run targets of one/64 pages,
32/512-byte payloads, composite keys, skew, disjoint/interleaved placement, and
32/128 MiB readonly caches. Empty, tiny, hot-only, cold-only, non-unique, and
million-row cold controls were included.

The initial shared lookup weight chose forward at 25% hot input: at 262,144
rows it took 39.531 ms versus ordered's 25.051 ms. Warm-path microbenchmarks
measured disk probes around 0.20/0.38/0.62 microseconds for one/two/three tree
levels. One resident-run search ranged around 0.04–0.15 microseconds; searching
16 wide-key runs reached 1.48 microseconds. These results motivated the separate
height-based disk weight. Reducing the initial 1,024-entry threshold to 32
allows two active jobs even for 64 distributed hot probes.

A final confirmation sweep measured another 357 invocations: baseline, final
automatic selection, and the previously fastest forced method, three repeats
per scenario. The selector matched that method in 34 of 35 mixed-tier cases;
the remaining 1%-hot small case had effectively tied forced measurements.
Representative median public CREATE times are below, in milliseconds.

| Scenario, two workers | Phase 1 | Automatic | Selected method | Forced comparison |
| --- | ---: | ---: | --- | ---: |
| 262,144 rows; 64 hot | 17.128 | 26.284 | Forward | 25.528 |
| 262,144 rows; balanced | 14.372 | 21.119 | Ordered | 19.370 |
| 262,144 rows; 64 cold | 6.942 | 9.558 | Reverse | 7.490 |
| 262,144 rows; 25% hot | 16.335 | 24.304 | Ordered | 23.787 |
| Balanced composite, 512 bytes, 32 MiB cache | 170.522 | 185.628 | Ordered | 194.572 |
| Same fixture, 128 MiB cache | 114.985 | 125.042 | Ordered | 125.363 |
| 1,000,000 cold, unique | 88.759 | 86.256 | Empty | — |
| 1,000,000 cold, non-unique | 82.706 | 88.613 | Unchecked | — |

Additional persisted reads make several cases materially slower than retained-vector
validation. Same-method timings also vary between builds/repetitions. The
forced snapshots precede final read-retention refinements; after final timing,
only test refinements and compact completion method/coverage checking changed.
The selector and traversal algorithms were unchanged after confirmation;
timings also precede the later lifecycle refactor.

The wide balanced 32 MiB case released 76 MiB of retained cold input, admitted
4.51 MiB of validation scratch, and bounded two reader pins to 128 KiB. Median
sampled peak RSS changed from 356.71 to 349.35 MiB, while table reads rose from
1,069 to 1,329. RSS is sampled every millisecond and includes allocator/storage
behavior; it is not allocation-release proof. The production weak/drop probes
provide that proof. These runs did not flush OS/device caches.

Forward's 64-hot case recorded two active jobs; balanced ordered recorded eight
jobs with peak two. The tiny 64-cold reverse case had two jobs but peak one in
final samples. Semantic gates separately prove that every method permits a
second job to finish while the first remains paused, including reverse cold
ranges entirely outside the hot interval.

Raw plans, logs, verified result TOML, source/binary hashes, environment details,
drivers, and summaries remain under `target/task-000330/bench/` (ignored).
`run.py` produced `samples.json`; `run-calibrated.py` produced
`samples-final.json` and `summary-final.json`. The internal release test is
`index::build::cross_tier::tests::validation_microbench`; its consumed
results and timings are in `target/task-000330/microbench.txt`.
See `docs/benchmark-tool.md` for reproduction commands.

### Final verification

- Workspace nextest: 2,338 passed; profiling-disabled storage: 2,072 passed.
  Both skip the release calibration test; recorded timings above predate refactors.
- Formatting, strict Clippy in both feature configurations, and branch style
  audit passed: 23 Rust files, 301 test contracts, zero violations. Semantic
  review preserves distinct component/public ownership and publication checks.
- Fresh default-feature coverage: 92.08% across the two Rust files changed by
  sealed metadata consolidation: 91.24% in `sealed_cold` and 92.20% in
  `disk_builder`, both above the 80% review bar.
  Reports/review: `target/task-000330/sealed-metadata/`; contracts:
  `target/test-audit/`. Earlier hot-owner boxing coverage was 86.69% in catalog
  orchestration; hot-state coverage was 90.22% across two files and owned-runtime
  coverage was 90.45% across seven. Those reports remain available.
- Unsafe inventory was refreshed for the module layout; no new unsafe operation
  was introduced. Allocation observation uses the outlined-key destruction boundary.
- The hot sub-state coverage run exposed an intermittent SIGSEGV in
  `test_unique_current_update_departures_forward_to_insert_and_update` during
  its old-snapshot read. The core shows an atomic load from `0x17` through
  `SharedTrxStatus::ts`, `UndoStatus::ts`, and
  `RowReadAccess::read_row_mvcc_index_candidate`. Its fixture uses CREATE TABLE
  and hot-row mutation, outside the changed CREATE INDEX lifecycle. All 100
  isolated runs of the same instrumented binary and a subsequent full coverage
  run passed; the root cause remains unresolved. Traces, original test output,
  and reproduction results are retained
  under `target/task-000330/hot-substates/`. No MVCC fix is claimed here.

## Impacts

CREATE ownership, reads, validation, and profiling changed; formats, public DDL,
recovery trust, and checkpoint eligibility remain compatible. Fixtures preserve
interleaved placement; the readonly wake fix also benefits other readers.

## Test Cases

Acceptance includes empty/nonempty sealing in both modes; foreign/incomplete/
failed evidence rejection; multiple resident cold runs; original allocation
release before hot admission; independent membership/conflict oracles for all
methods; equal keys across cuts; late conflict cleanup and hot/hot precedence;
missing/foreign/repeated/partial/wrong-direction coverage; rejected-seal retention;
exclusive finalization after reader release; memory-only/completed runtime cleanup;
one-time runtime and fork transfer; and actual sealed binding at installation. Required-empty
evidence stays distinct from skipped validation without admitting readers.

Gated jobs cover parallel progress, detached execution, typed integrity failure,
and cancelled partial reclamation. Hot lifecycle tests cover cleanup before
construction, occupied-destination rejection with the original entry preserved,
idempotent settlement/discard, and panic guards around transfer and reclamation.
Admission tests cover reduced concurrency,
minimal-reader failure, excess-worker binding rejection, final-reader release,
unchanged publication, and retry. Integration suites cover deleted/replaced rows,
non-unique multiplicity, trusted recovery, DML/checkpoint/restart, shutdown,
poison, and cleanup panics. Fixture tests check the interleaving bijection and
inverse placement through empty and extreme tiers.

## Open Questions

No selector or phase-local architectural decision remains unresolved. Parallel
cold extraction and spill remain outside this phase under backlog 000104;
checkpoint integration remains RFC 0033 Phase 4. The measured read-latency/RSS
tradeoff is recorded above and should inform later end-to-end program work.

### Simplified persisted reads

DiskTree admission now checks the checksum and fixed header/fence ranges only.
The shared persisted-node guard exposes checked borrowed views to CREATE,
foreground scans/lookups, and checkpoint rewrites. Selected entries validate
complete key/value ranges on access; unique owners and branch pointers retain
typed integrity errors. Writer ordering and payload separation are trusted in
production and checked by the retained deep validator in debug construction
and tests. Unvisited malformed entries are intentionally not detected eagerly.

Keys borrow prefix/outlined suffix bytes, with up to four short suffix bytes
held inline to preserve the existing little-endian head format. Leaf decoding
allocates no key. Point lookup returns scalars; scans copy emitted candidates;
rewrites explicitly materialize owned entries. Reverse lookup checks each cold
entry once per hot-run search and reuses its borrowed key for all comparisons.
The old per-worker 24-page validation scratch and two cursor-key allowance are
removed; traversal, batch scratch, retained cuts, and job accounting remain.
Persisted format, public interfaces, selection scores, and settlement policy
are unchanged.

The six-run simplification check used one before/after observation for each
1-million-row split with four workers. Validation times were 38.202 -> 11.185 ms
(Forward, 10k/990k, both profiled), 25.727 -> 16.735 ms (Ordered, 500k/500k), and
2.513 -> 3.097 ms (Reverse, 990k/10k). CREATE times were respectively
155.520 -> 113.230, 79.563 -> 74.190, and 34.331 -> 33.137 ms. Every run verified
all table/index rows and matching content fingerprints. These are single
observations; the Reverse phase increase is retained rather than treated as
noise or an established regression. Reader scratch admission decreased by
exactly 7 MiB across four workers. The Forward profiles no longer contain
per-entry layout or owned leaf-decoding work in admission. Artifacts and
sampling limitations are in `target/task-000330/simplified-reads/report.md`.
Selection scores were not recalibrated from this small sample.

The checked reader implementation is consolidated into `disk_tree.rs`.
`BTreeEntryView<V>` in the common B-tree module replaces both the physical
key/value tuple and DiskTree's former logical `LeafEntryView`. Physical values
retain their original delete masks; memory candidate projection and cleanup
interpret them locally, while DiskTree validates durable values and owns
conversion to rewrite `LogicalEntry`. Key-only range filtering and scalar
optimistic lookups retain their existing access patterns.
