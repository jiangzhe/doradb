---
id: 000320
title: Centralize Storage Profiling
status: implemented
created: 2026-09-29
github_issue: 1125
---

# Task: Centralize Storage Profiling

## Summary

Storage snapshots, component-owned recorders, metric projection, recovery
serialization, and reusable process probes now live under
`doradb-storage/src/profiling/`. The former `doradb_storage::stats` module is
removed. The profiling module, diagnostic methods, and crate-root stats exports
require `profiling`; stats getters retain their enabled signatures.

Profiling uses Quanta 0.13 for elapsed wall time and remains enabled by default.
Disabling it removes diagnostic recorder state, updates, clocks, argument work,
transport, and stats APIs while preserving operational behavior. Benchmark schemas and
measurement windows remain unchanged.

## Context

This standalone task was designed against
`07356ac4457dde661db192edfaadc1f31b29df8b`; it has no parent RFC or source backlog.
Previously, the profiling feature primarily covered hot-index builds. Other
collectors remained active in buffer pools, I/O, redo, purge, locks, recovery,
mandatory runtime, and checkpoints. Storage-oriented projection and process
probes also lived in the benchmark crate.

Some measured values were coupled to correctness: replay counts controlled page
dirtying, redo bytes determined synchronization, and admission/resource counters
controlled capacity and drain. Those operational responsibilities remain active
independently of profiling.

Issue Labels:

- type:task
- priority:medium
- codex

References: [storage architecture](../architecture.md),
[public diagnostics](../public-api.md#diagnostics-and-statistics),
[benchmark contracts](../benchmark-tool.md), and the
[profiling module](../../doradb-storage/src/profiling/mod.rs).

### Clock evaluation

A standalone release experiment on 2026-09-29 used Quanta 0.13.0 without default
features, rustc 1.97.1/LLVM 22.1.6, and an Apple aarch64 Linux host under OrbStack.
Guest CPU affinity was fixed to CPU 0. Each case warmed for 200,000 iterations,
then ran 16 rounds of 3,000,000 iterations, rotating case order. `black_box`
retained reads and interval starts; outer measurement used the standard clock.
The following medians are average loop costs, not timestamp resolution or an
engine performance claim:

| Primitive | Read overhead | Two reads plus elapsed nanoseconds |
| --- | ---: | ---: |
| `std::time::Instant` | 9.986 ns | 25.005 ns |
| `quanta::Instant` | 1.134 ns | 2.353 ns |
| Explicit `quanta::Clock::now` | 0.901 ns | 1.645 ns |
| `Clock::raw` with `delta_as_nanos` | 0.679 ns | 1.137 ns |

Eleven fresh-process initializations had a median calibration cost of 523,230 ns
(range 522,396 to 524,479 ns). Quanta allows calibration to run for up to 200 ms,
so the observed half millisecond is not a portable bound. A smoke check of
400,000 mutex-ordered samples across four threads found no backward scaled or
raw samples; this does not establish correctness across all hardware or VM
migration conditions. Enabling Quanta's mock feature increased its global
Instant pair cost to 5.512 ns in a separate run, supporting test-only mock use.

Ignored research artifacts are in the dispatch checkout at
`target/profiling-clock-eval-20260929/`, including `report.md`, the standalone
harness, `production-clean.csv`, and `cold-init.txt`. The summarized method and
results above are the durable evidence; implementation does not depend on those
local files being present. No x86-64 or end-to-end engine speedup is claimed.

## Goals

- Centralize existing types, collectors, calculations, and reusable probes while
  retaining ownership by the existing components.
- Preserve all 202 benchmark internal metrics, including ordering, units, kinds,
  delta/cumulative interpretation, conditional families, and publication rules.
- Compile out profiling-only state and work in disabled production builds.
- Preserve enabled diagnostic lifecycles and persistent formats, and keep
  operational behavior correct with either feature setting and I/O backend.
- Keep new storage dependencies optional and process probes explicitly activated.

## Non-Goals

- New metrics, schemas, exporters, sampling policies, or a profiling service.
- Changes to storage algorithms, scheduling, resource limits, or default features.
- Compatibility for the removed `doradb_storage::stats` import path.
- Replacing operational deadlines or process CPU clocks with Quanta.
- Cached/recent clocks, upkeep threads, raw storage timestamps, or universal
  performance targets.
- Automatic sampling, engine-exclusive CPU/RSS attribution, or exact peak RSS.

## Rejected Alternatives

- An engine-wide service or registry would change recorder ownership without
  being necessary for centralization.
- A new event/recorder protocol would add interfaces and translation rules
  beyond the existing component boundaries.
- Relocating definitions alone would retain disabled-feature overhead from
  fields, updates, argument construction, and asynchronous captures.

## Plan

Public diagnostic types and getters require `profiling`. Private domain modules
contain buffer, I/O, transaction, lock, runtime, recovery, checkpoint, cleanup,
and hot-index measurement code. Feature gates cover the complete construction,
recording, snapshot, and transport paths. Components retain recorder ownership.
Maintenance operations remain available independently of profiling.
`CatalogCheckpointResult` belongs to the catalog checkpoint module: an enabled
named struct contains `outcome` and a metrics-only `CatalogCheckpointReport`,
while the disabled definition aliases `CatalogCheckpointOutcome`. Row replay and
hot-index recovery result aliases also live in their operational owner modules.
This leaves the entire profiling module feature-gated with no disabled stubs.

The disabled API follows these contracts:

| API or state | Behavior without profiling |
| --- | --- |
| Session stats getters and their diagnostic types | Absent |
| `Engine::recovery_report()` and recovery report types | Absent |
| Internal stats producers | Absent; no synthetic snapshots or static report |
| Catalog checkpoint | `CatalogCheckpointResult` aliases the actual `CatalogCheckpointOutcome` |
| MemIndex cleanup | Actual `live_delay`; `stats` field absent |
| Capacity, allocation, admission, and drain bookkeeping | Remains operational |

Replay now retains a separate mutation marker. Every successful insert, update,
or delete marks its page dirty even when a later operation in the batch fails.
Optional counts are no longer the dirty-page oracle. Redo byte accounting needed
for synchronization and admission/resource state needed for capacity or drain
remain operational. Mandatory completion counters are published before observers
can consume completion when profiling is enabled.

`InternalStatsSnapshot` captures public component snapshots and produces the
ordered delta or fresh-engine cumulative metric list. Benchmark callers use
its methods directly. `RecoveryMeasurements::try_from(&RecoveryReport)` converts
durations to nanoseconds and validates accounting. Its strict serialized schema
retains optional historical `hot_indexes` deserialization.

Profiling arithmetic assumes values fit their numeric types. Duration differences
saturate at zero. Recovery accounting checks and RSS parsing continue to report
invalid measurements.

All storage profiling timestamps use the central Quanta clock, including nested
bootstrap and redo intervals. Shared calibration initializes before the first
reported bootstrap instant. Operational deadlines remain on the standard clock;
process CPU measurements continue to use the OS process CPU clock.

CPU sampling returns nanoseconds directly. Linux RSS helpers use typed errors,
including `RuntimeError::ProfilingMeasurement`, underlying I/O context, and attachments.
RSS retains synchronous baseline/readiness, 1 ms sampling, terminal sampling,
saturating peak-above-baseline, and explicit stop/join. Enabling profiling or
constructing an engine does not activate probes. Benchmark workload timing,
histograms, fixture verification, activation windows, and error precedence remain
in the benchmark crate.

Storage's optional Quanta and rustix dependencies activate through `profiling`.
The benchmark explicitly enables storage profiling and enables Quanta's mock
feature only as a development dependency. Its normal rustix dependency retains
process signaling; storage enables the CPU-clock and page-size capabilities.

## Implementation Notes

Centralized storage profiling and benchmark measurement helpers with preserved
enabled output and compile-time removal of disabled instrumentation. Public
API documentation records diagnostic feature availability, maintenance behavior,
and canonical imports. The generated public error inventory was refreshed.

A fixed baseline captured before moving the metric emitter verifies all 202
ordered metrics in both delta and cumulative modes. Additional checks preserve
conditional emission, lifetime peaks, saturating deltas, and failed-build versus
successful CREATE publication behavior. Recovery tests retain numeric precision,
accounting validation, and saturating duration differences. They cover unknown
fields at every nested level and historical omission of hot-index measurements.
Probe failures retain typed measurement and underlying I/O diagnostics.

Review removed profiling overflow guards and fallible numeric conversions.
CPU sampling is infallible, and benchmark callers subtract CPU samples directly
instead of using a separate delta helper. Overflow-rejection cases were removed;
Timing tests cover positive, equal, and exceeding nested intervals.

Mixed correctness tests continue running with profiling disabled. Readonly-cache
and I/O synchronization use dedicated test hooks or actual state instead of
production statistics. Restart tests verify recovered values through index
lookups. A direct replay regression covers successful insert/update/delete
followed by invalid payloads, and malformed first operations leave pages clean.

Review and repeated runs exposed test assumptions about background cleanup:
a dropped runtime could be reclaimed between allocation snapshots, and metadata
history could be purged between failed-DDL snapshots. The publication test now
retains the relevant layout and transaction horizon. Retired-runtime cleanup
checks observe completed purge cycles. Both affected tests passed 30 consecutive
stress iterations without changing production cleanup behavior.

Final backend/feature validation on 2026-09-30:

| Configuration | Result |
| --- | --- |
| Workspace defaults (profiling + io_uring) | 2,189 tests passed |
| Storage only, io_uring without profiling | 2,014 tests passed |
| Storage only, libaio without profiling | 2,016 tests passed |
| Storage only, libaio with profiling | 2,044 tests passed |

Follow-up cleanup on 2026-09-30 removed the redundant benchmark `output.rs`
module and its three forwarding functions. Plan execution and recovery call
`InternalStatsSnapshot` directly, retaining error conversion and session cleanup
at their existing boundaries. All 146 benchmark tests passed after this change.

Review made the profiling module and all pure stats APIs feature-gated and
removed disabled zero/empty fallback producers. The six session stats getters,
engine recovery report, and diagnostic type exports require profiling. Catalog
checkpoint returns a named outcome/report struct when enabled and directly
returns its operational outcome through a type alias when disabled. The report
contains only metrics. Result definitions stay in their operational modules,
including the row-replay and hot-index recovery aliases. Redundant inner feature
gates were removed from profiling modules.

External callers verify both checkpoint result definitions, the metrics-only
report, and the absence of the disabled profiling module; previous API checks
also verified that diagnostic methods are unavailable without the feature.
Checkpoint serialization retains its flat benchmark schema and rejects unknown
fields at the result, outcome, table-change, and table-I/O levels. Disabled
maintenance tests consume the checkpoint outcome directly and destructure cleanup
results with only operational fields. Runtime lifecycle tests use authoritative
blocker counts in both builds; metric-only checks require profiling. The two
dedicated snapshot/report tests run only when those APIs exist, explaining the
reduced disabled test counts.

Formatting, workspace Clippy, and all three explicit storage feature/backend
Clippy passes succeeded. The final style gate passed 67 branch-diff Rust files;
the profiling directory pass covered 13 files, including new modules. Their
test-contract checks reported zero violations (1,260 and 19 selected tests,
respectively). Semantic review preserved operational, lifecycle, backend-specific,
and profiling-only assertions. The installed Clippy reports the repository's
pre-existing unknown `clippy::unused_async_trait_impl` allowance; commands still
exit successfully. No lint or timeout configuration was changed.

Storage-only normal dependency graphs contain no Quanta, rustix, or
portable-atomic without profiling and include them when enabled. A disabled
release LLVM build contains no recorder, profiling clock, process-probe, or
new test-hook symbols. Representative emitted buffer, io_uring, mandatory-runtime,
recovery, and hot-index-build functions have no profiling references; source
review also checked feature-gated fields, call arguments, and asynchronous
transport. Operational counters and standard-clock retry deadlines remain.

The local lockfile was regenerated with one Quanta 0.13.0 version. `Cargo.lock`
is ignored by this repository, so dependency requirements are the tracked
artifact rather than a newly forced lockfile. There are no deferred implementation
items, source backlogs to close, or parent RFC phases to synchronize.

## Impacts

- Canonical profiling imports replace the removed stats namespace. The module,
  stats getters, diagnostic types, and crate-root stats exports require profiling.
- Buffer, storage I/O, redo/purge, locks, runtime, recovery, checkpoint, cleanup,
  and index-build instrumentation is optional across its complete lifetime.
- Enabled native duration reports and benchmark schemas retain their contracts.
  Disabled catalog checkpoints return their outcome directly; cleanup results
  omit measurement fields. Configuration and
  persisted storage formats are unchanged.
- Optional process probes are reusable outside the benchmark; RSS failures return
  storage errors. Benchmark callers retain workload-specific conversion and validation.
- Profiling clock setup occurs outside internal reported startup time; the
  recorded microbenchmark is local evidence, not an end-to-end speedup claim.

## Test Cases

- Exact metric inventory/order/value/kind/unit baselines and conditional family,
  delta, lifetime-peak, cumulative, and publication semantics.
- Strict recovery round trips, historical optional fields, numeric precision,
  byte and work accounting, saturating duration differences, and shared-clock intervals.
- Compile-time availability of the profiling module, stats methods, and fields;
  feature-specific checkpoint result types; enabled recovery-report lifetime and inspection
  lifecycle errors; actual capacity and admission/drain state in both builds.
- Replay ordering, partial-failure dirty pages, restart values/indexes, required
  redo sync, lock cancellation, admission/drain, and resource limits.
- Checkpoint publication/reopen and cleanup outcomes in both feature settings.
- Shared recorder ownership, backend-specific I/O accounting, runtime completion
  visibility, and cancelled/failed build publication.
- Process CPU sampling, RSS parsing/unavailable input, readiness,
  peak consistency, activation windows, joining, and primary-error precedence.
- Separate backend/feature test and Clippy passes, release-code inspection, and
  storage-only normal dependency graph checks.

## Open Questions

None. Broader clock performance evaluation on other architectures and a future
profiling service remain outside this task's scope.
