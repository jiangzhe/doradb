---
id: 000320
title: Centralize Storage Profiling
status: implemented
created: 2026-09-29
github_issue: 1125
---

# Task: Centralize Storage Profiling

## Summary

Storage diagnostics, component-owned recorders, metric projection, recovery
serialization, and reusable process probes now live in
`doradb_storage::profiling`. The former `doradb_storage::stats` module is removed.
Profiling remains enabled by default and uses Quanta 0.13 for elapsed wall time.

Disabling profiling removes diagnostic APIs, recorder state, updates, clocks,
and measurement transport while preserving operational behavior. Maintenance
results retain their operational meaning with optional measurement fields.
Enabled benchmark schemas, metrics, and measurement windows are preserved.

## Context

This standalone task was designed against
`07356ac4457dde661db192edfaadc1f31b29df8b`. It has no parent RFC or source backlog.
Previously, profiling primarily covered hot-index builds; other collectors
remained active across storage, and reusable measurement code lived in the
benchmark crate.

Some counters also controlled correctness: replay counts determined page
dirtying, redo bytes determined synchronization, and admission/resource counts
controlled capacity and drain. Separating those responsibilities from diagnostics
was necessary before instrumentation could be compiled out.

Issue Labels:

- type:task
- priority:medium
- codex

References: [storage architecture](../architecture.md),
[public diagnostics](../public-api.md#diagnostics-and-statistics), and
[benchmark contracts](../benchmark-tool.md).

## Goals

- Centralize existing diagnostics while retaining component ownership.
- Preserve all 202 ordered benchmark internal metrics, including units, kinds,
  delta/cumulative interpretation, conditional emission, and publication rules.
- Remove profiling-only work and dependencies from disabled storage builds.
- Preserve correctness, enabled diagnostic lifecycles, and persistent formats
  with either profiling setting and I/O backend.
- Keep process probes explicitly activated by callers.

## Non-Goals

- New metrics, exporters, sampling policies, or an engine-wide profiling service.
- Changes to storage algorithms, scheduling, resource limits, or default features.
- Compatibility for the removed `doradb_storage::stats` import path.
- Replacing operational deadlines or process CPU clocks with Quanta.
- Cached clocks, upkeep threads, automatic sampling, engine-exclusive resource
  attribution, exact peak RSS, or universal performance guarantees.

## Rejected Alternatives

- A global service or registry would change recorder ownership without being
  necessary for centralization.
- A new event/recorder protocol would introduce translation and lifecycle rules
  beyond the existing component boundaries.
- Relocating definitions alone would preserve disabled-feature overhead from
  fields, updates, argument construction, and asynchronous captures.

## Plan

The profiling module and public diagnostic types, methods, and root exports are
feature-gated. Private domain modules contain the existing recorders and shared
measurement logic. Gates cover construction, recording, snapshots, argument
work, and transport; disabled builds do not manufacture empty diagnostics.

Maintenance result definitions remain with their operational owners.
`CatalogCheckpointResult` is a named struct in both builds: `outcome` is always
available, and the metrics-only `report` field requires profiling. Row replay
also returns a named result with profiling-gated counts; it has no payload when
profiling is disabled. Cleanup results retain their actual delay outcome.

| Contract | Behavior without profiling |
| --- | --- |
| Stats getters, recovery reports, and diagnostic types | Absent |
| Catalog checkpoint result | Operational outcome remains; report field absent |
| Row replay result | Successful completion remains; counts field absent |
| MemIndex cleanup result | Actual live-cleanup delay remains; stats field absent |
| Capacity, admission, synchronization, and drain accounting | Remains operational |

Replay retains independent mutation evidence. Every successful insert, update,
or delete marks its page dirty even when a later operation fails. Redo byte
accounting required for synchronization and counters required for resource
limits remain active. Enabled mandatory-runtime completion statistics become
visible before observers consume completion.

Shared metric projection produces ordered interval deltas or fresh-engine
cumulative values. Benchmark callers use it directly; workload timing,
histograms, fixture verification, sampling windows, and workload error precedence
remain benchmark responsibilities. Recovery serialization validates accounting
and retains compatibility with historical omission of hot-index measurements.

Profiling arithmetic assumes representable counts, sizes, and timings. Duration
differences and snapshot deltas saturate at zero. Logical-lock decrements retain
a release assertion for balanced lifecycle accounting. Recovery consistency
checks and RSS parsing retain typed measurement errors.

All storage profiling timestamps share Quanta calibration, initialized before
the first reported bootstrap instant. Operational deadlines use the standard
clock, and CPU sampling uses the OS process CPU clock.

RSS sampling captures a synchronous baseline, waits for worker readiness, samples
at one-millisecond intervals, and includes a terminal sample on explicit stop.
Both stop and Drop join the worker. Explicit stop returns measurements or worker
errors; implicit cleanup discards them. No engine startup activates probes.

Storage's Quanta and rustix dependencies are optional behind profiling.
The benchmark explicitly enables storage profiling and keeps rustix for process
signaling. Quanta's mock feature is a benchmark development dependency only.

## Implementation Notes

Centralized storage profiling with preserved enabled measurement contracts and
compile-time removal of disabled instrumentation. Operational correctness no
longer depends on diagnostic counters, and maintenance result types retain a
consistent named-struct interface across feature configurations.

### Review outcomes

Review made the complete profiling module and pure diagnostic APIs conditional,
removed disabled fallback producers, and separated catalog outcomes from metrics.
The initial feature-dependent catalog and row-replay aliases were replaced with
named structs whose measurement fields are gated. Enabled checkpoint serialization
retains its flattened benchmark schema; disabled serialization wraps the outcome
in the common result structure.

The benchmark's redundant output wrappers were removed. Storage measurement
helpers now serve both benchmark callers and other explicit consumers, while
benchmark orchestration retains its timing and error responsibilities.

Numeric overflow guards and fallible conversions were removed under the
representability assumption. Saturating duration subtraction was retained, and
the logical-lock underflow assertion was restored as a lifecycle invariant.

The public RSS sampler now stops and joins on Drop, including early errors and
unwinding. Tests distinguish explicit worker-error reporting from implicit
cleanup and establish release of worker ownership without scheduling sleeps.

Mixed correctness tests remain active without profiling. Dedicated hooks and
actual state replace production counters as synchronization predicates. The
index-DDL overlap test awaits rollback while the DDL gate remains held, so its
cleanup-progress guarantee does not depend on profiling counters.

Review also exposed background-cleanup assumptions in allocation and failed-DDL
snapshots. Tests retain the relevant layout or transaction horizon and observe
completed purge cycles. Both affected tests passed 30 consecutive stress runs
without changing production cleanup behavior.

Checkpoint serialization testing exposed an existing Noop parsing inconsistency:
unknown fields inside the outcome were accepted. Strict parsing now rejects them
for both outcome variants and their result wrappers, preserving the public
variants and valid report schemas across profiling configurations.

### Clock evaluation

A release experiment on 2026-09-29 used Quanta 0.13.0, rustc 1.97.1/LLVM 22.1.6,
and Apple aarch64 Linux under OrbStack, pinned to guest CPU 0. Each case warmed
for 200,000 iterations and ran 16 rotating rounds of 3,000,000 iterations.
`black_box` retained reads and starts; the standard clock measured outer loops.
These medians are loop costs, not resolution or end-to-end engine speedups:

| Primitive | Read | Two reads plus elapsed nanoseconds |
| --- | ---: | ---: |
| Standard Instant | 9.986 ns | 25.005 ns |
| Quanta Instant | 1.134 ns | 2.353 ns |
| Explicit Quanta Clock | 0.901 ns | 1.645 ns |
| Raw Clock with delta conversion | 0.679 ns | 1.137 ns |

Eleven fresh-process initializations had a median calibration cost of 523,230 ns
(range 522,396–524,479 ns). Quanta permits up to 200 ms for calibration, so this
is not a portable bound. A four-thread, mutex-ordered smoke check of 400,000
samples observed no backward timestamps; it does not establish behavior across
all hardware or VM migration. Mock-enabled Instant pairs cost 5.512 ns in a
separate run. No x86-64 speedup is claimed.

The summarized evidence is durable; ignored research artifacts remain under
`target/profiling-clock-eval-20260929/` in the dispatch checkout.

### Final verification

The profiling reorganization, including review fixes and field ordering, passed
the following matrix on 2026-09-30 before the final Noop parsing fix:

| Configuration | Result |
| --- | --- |
| Workspace defaults, profiling with io_uring | 2,193 tests passed |
| Storage, io_uring without profiling | 2,015 tests passed |
| Storage, libaio without profiling | 2,017 tests passed |
| Storage, libaio with profiling | 2,048 tests passed |

The subsequent Noop parsing fix passed 2,194 workspace tests and 2,015 storage
tests without profiling on io_uring. Its regression first reproduced the bug,
then verified strict direct and wrapped outcomes in both feature configurations.

Formatting and Clippy passed for the full reorganization matrix. The final
parsing fix also passed workspace and profiling-disabled io_uring checks.
The resolve style gate passed 80 branch-diff Rust files and
1,300 selected test contracts with zero violations. Assertion review retained
operational, lifecycle, backend-specific, and profiling-only coverage. Existing
Clippy tooling reports an unknown allowance for `unused_async_trait_impl` but
exits successfully; no lint or timeout configuration was changed.

The fixed pre-relocation baseline verifies all 202 metrics in delta and
cumulative modes. Earlier dependency and release-LLVM checks confirmed that
disabled storage omits Quanta, rustix, portable-atomic, profiling clocks,
recorders, probes, and test-hook symbols while retaining operational counters
and standard-clock deadlines. The ignored local lockfile resolved one Quanta
0.13.0 version; tracked manifests carry the dependency contract.

## Impacts

- Diagnostic import paths and availability follow the profiling feature;
  maintenance outcomes remain available in every build.
- Buffer, I/O, redo/purge, locks, runtime, recovery, checkpoint, cleanup, and
  index-build instrumentation is optional across its lifetime.
- Enabled benchmark schemas and measurement windows are preserved. Disabled
  checkpoint consumers use the common result's outcome field.
- Configuration and persistent storage formats are unchanged.
- Process probes are reusable, caller-owned resources; dropping an RSS sampler
  now waits for worker cleanup.
- Local clock measurements support the selected implementation but make no
  end-to-end performance guarantee.

## Test Cases

- Exact metric names, ordering, values, units, kinds, conditional families,
  saturating deltas, lifetime peaks, and successful-publication rules.
- Recovery serialization, historical optional fields, duration precision,
  saturating residuals, accounting consistency, and shared-clock intervals.
- Feature-gated API availability and named maintenance/replay result shapes;
  enabled recovery-report lifetime and operational behavior in both builds.
- Replay ordering, partial-failure dirty pages, restart values and indexes,
  required redo synchronization, lock accounting, cancellation, and drain.
- Checkpoint publication/reopen, strict outcome and result serialization in both
  profiling configurations, cleanup outcomes, and cleanup progress while accepted
  index DDL remains gated.
- Shared recorder ownership, backend I/O accounting, completion visibility,
  resource limits, and cancelled or failed build publication.
- CPU sampling, RSS parsing and errors, readiness, peaks, activation windows,
  explicit stop, Drop on error or unwind, joining, and error precedence.

## Open Questions

No implementation questions or deferred parsing issues remain.

Broader clock evaluation on other architectures and an engine-wide profiling
service remain outside this task's scope.
