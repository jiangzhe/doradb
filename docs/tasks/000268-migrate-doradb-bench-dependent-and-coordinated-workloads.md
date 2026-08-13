---
id: 000268
title: Migrate doradb-bench Dependent and Coordinated Workloads
status: implemented
created: 2026-08-12
github_issue: 973
---

# Task: Migrate doradb-bench Dependent and Coordinated Workloads

## Summary

Implemented RFC 0028 Phase 3 by migrating `lookup-seq`, `lookup-rand`,
`table-scan`, `index-scan`, `index-stream`, `index-ddl`, and `lock-table` to
the TOML plan executor. Every existing benchmark workload now executes through
one strict plan model and has a complete checked-in template.

The fixture model now validates loaded-data, index-shape, committed-write-fence,
and ordered table-pool capabilities before execution, then binds them to
runtime IDs and observed insert outcomes. The plan executor is a coordinator;
typed workload executors own operation logic, outcome verification, and any
specialized lifecycle hooks.

The incompatible cutover removed lifecycle subcommands, cleanup, manifests,
legacy runners, CSV, and Markdown results. `doradb-bench` now requires
`--root` and `--plan`, emits one canonical `benchmark-result.toml`, and prints
an aggregate summary plus the absolute detailed-result path to stdout.

## Context

RFC 0028 Phase 1 established strict plan parsing, engine configuration,
sequential phase execution, measurement, and `trx-noop`. Phase 2 added the
simple workloads and basic primary-table fixture state. Reads, index DDL, and
lock scenarios still depended on Clap-owned configuration, a legacy manifest,
and a second runner hierarchy.

The remaining workloads required durable capability proofs that differ from
runtime facts. A plan can prove that inserts allocated a candidate key range,
but only execution can prove that rows committed and produce the latest
write-bearing `TrxID`. Lock workloads additionally require an ordered table
pool and own blocker, waiter, cancellation, promotion, join, and participant
session lifecycles.

Parent RFC:

- `docs/rfcs/0028-composable-doradb-bench-phase-framework.md` (Phase 3)

Issue Labels:

- type:task
- priority:medium
- codex

## Goals

- Migrate all seven dependent and coordinated workloads to strict plan-native
  configuration and execution.
- Validate primary shape, committed load, secondary-index compatibility, and
  ordered table-pool width before creating the benchmark root.
- Bind runtime `TableID`s, successful inserted-row counts, candidate ranges,
  and the latest typed write fence before dependent phases execute.
- Preserve deterministic request generation, transaction batching, index DDL,
  public-session boundaries, and every supported lock scenario.
- Keep the plan executor focused on dispatch and shared session lifecycle while
  workload modules own execution and verification.
- Apply first-error-wins cancellation to every workload and drain all declared
  and auxiliary participants before returning.
- Make required plan execution the sole CLI surface and publish one canonical
  result artifact with a human-readable stdout summary.
- Provide a complete self-contained template for all twelve workloads.

## Non-Goals

- No freeze, checkpoint, update, delete, overwrite, mixed read/write, or
  read-while-writing workload.
- No fixture reset, clone, restart, cold-cache orchestration, parallel phases,
  loops, named bindings, or actor graph.
- No public purge wait or new checkpoint retry policy; those remain assigned
  to later RFC 0028 work.
- No storage transaction, lock, index, recovery, mandatory-runtime, I/O
  backend, persisted-format, or unsafe-code change.
- No compatibility adapter for legacy CLI commands, manifests, or artifacts.
- No generic dynamic workload registry beyond the closed current workload set.

## Plan

### Strict plan and fixture model

`WorkloadSpec` and `ResolvedWorkload` remain exhaustive enums. Each migrated
workload has a strict serde-facing specification and a fully normalized
resolved configuration. Unknown or irrelevant fields are rejected, inherited
worker and diagnostic controls are materialized, and checked arithmetic is
performed before root creation.

Worker defaults are one thread with sessions equal to threads. Phase-local
values override `[workload_defaults]`; a thread override without a session
override sets both counts to that value. Both are positive and threads may not
exceed sessions.

The plan fixture is one optional homogeneous ordered table pool. Its first
entry is the implicit primary. Plan state retains table shape and count,
generated-key cursor, and cumulative candidate range. Runtime state adds the
ordered table IDs, successfully inserted row count, and greatest write-bearing
commit fence.

`FixtureRequirement` expresses no fixture, absent primary, typed primary, or a
minimum table pool. Primary requirements independently constrain index shape
and optional versus committed load. A committed-load binding requires a
nonempty planned candidate range, at least one successful runtime insert, and
a latest write fence. This rejects all-expected-error loads without pretending
that partially successful ranges are gap-free.

Creation effects carry the homogeneous shape and ordered table count. Insert
effects carry the exact attempted range and runtime successful-row/fence facts.
Runtime effects apply only after all sessions join and workload verification
agrees with the planned effect.

### Executor and workload ownership

`plan_executor.rs` owns plan sequencing, exhaustive static dispatch, engine
lifecycle, timing envelopes, generic task scheduling, public-session
open/close, cancellation, draining, aggregation, and artifact publication. It
contains no workload operation loops.

The crate-private `SessionExecutor` trait uses associated `Config` and
`Outcome` types so related workloads can share identities without a runtime
session-execution enum. Implementations provide deterministic session plans,
execute one public session, merge typed outcomes, verify samples/counters and
fixture effects, and optionally complete post-close timing or post-run checks.

No-op, insert, read, DDL, and lock modules own their respective executors and
verification. Shared binding extraction, partitioning, deterministic
generation, checked merge, counter equations, sample checks, and no-effect
verification live in `workload/util.rs`.

`RunCancellation` is passed to every workload executor. The first unexpected
error is retained; peers stop only at workload-safe boundaries. All task
handles remain attached, active transactions roll back where required,
declared sessions close, lock participants drain, and later cleanup errors do
not replace the first failure.

### Workload behavior and measurement

Reads consume a typed primary binding. Sequential and seeded-random lookup,
full table scan, materialized bounded index scan, and fully exhausted index
stream retain deterministic per-session generation. Batched reads measure
begin-through-commit transactions; index streams measure begin through stream
exhaustion and commit. Statement or stream errors roll back best effort.

Index DDL consumes an index-free primary, uses the exact `IndexNo` returned by
create for drop, counts two operations per completed cycle, and records one
create/drop-cycle sample. It remains single-run because successful cycles add
catalog history even though they restore the logical no-index shape.

Lock execution consumes the ordered table pool and retains stable/seeded table
selection, session and transaction scopes, paired and retained release shapes,
and all basic and specialized scenarios. Contended scenarios synchronize on
public monotonic `LogicalLockStats`; timeouts are hang watchdogs rather than
progress mechanisms. Each scenario owns blocker release, cancellation, waiter
join, participant close, promotion validation, and FIFO validation.

Successful sample contracts are:

| Workload shape | Latency unit | Sample count |
| --- | --- | ---: |
| Lookups | `lookup-batch-transaction` | Sum of per-session batch ceilings |
| Table scan | `table-scan-batch-transaction` | Sum of per-session batch ceilings |
| Materialized index scan | `index-scan-batch-transaction` | Sum of per-session batch ceilings |
| Index stream | `index-stream-transaction` | `num` |
| Index DDL | `index-create-drop-cycle` | `num` |
| Retained session lock | `table-lock-session-retained-lifecycle` | Nonempty sessions |
| Retained transaction lock | `table-lock-transaction-retained-lifecycle` | Nonempty sessions |
| Paired or specialized lock | `table-lock-operation-lifecycle` | `num` |

Read and lock workloads are replay-safe after complete cleanup. Index DDL and
the existing state-consuming workloads remain single-run. Prepare phases never
record latency.

### CLI, output, and templates

The only invocation form is:

```text
doradb-bench --root <storage-root> --plan <plan.toml>
```

Both arguments are required by Clap; `DORADB_BENCH_ROOT`, `-r`, and `-p`
remain supported. There is no cleanup command: users remove retained success
or diagnostic roots with normal host tools.

Legacy `Manifest`, `WorkloadRunner`, lifecycle commands, cleanup markers, CSV,
and Markdown result writers were removed. After successful phase verification
and engine shutdown, the executor atomically installs
`benchmark-result.toml`. It then prints workload identity, measured runs,
operations, elapsed time, throughput, latency unit, average/p95/p99 latency,
and the absolute detailed-result path. Failure emits neither the artifact nor
the success summary.

Seven new templates complete the twelve-workload inventory. Every workload
template includes shared engine defaults, performs its own fixture setup, and
ends in the workload named by the file. The inventory test rejects missing or
unexpected workload templates.

## Implementation Notes

Implemented RFC 0028 Phase 3 with all twelve workloads on one plan-only typed executor, capability-checked fixture composition, workload-owned execution and verification, and one canonical success artifact.

- The executor design changed during review: `plan_executor.rs` was reduced to
  coordination and dispatch, `SessionExecution` was removed, and
  `SessionExecutor` gained associated configuration and outcome types.
- Workload-specific outcome verification moved beside operation logic. Common
  session planning, binding, generation, merge, and verification helpers moved
  to `workload/util.rs`.
- The final CLI deliberately has no cleanup command and makes `--plan`
  required. Root deletion is user-managed.
- Result publication was simplified further than the original migration
  outline: the successful invocation writes only `benchmark-result.toml` and
  prints a stable aggregate summary plus its absolute path.
- `RunCancellation` now reaches every workload through the shared trait and
  takes effect at each workload's safe unit boundary. Specialized lock
  participants remain workload-owned and are drained before error return.
- Review restored the `threads <= sessions` topology invariant so idle OS
  workers cannot contaminate measurements. Validation covers global defaults
  and phase-local overrides.
- Review also made statement-noop latency/counter failures await rollback while
  preserving the primary error, and converted lock promotion/FIFO verification
  panics into detailed `BenchError` returns through the existing cleanup path.
- The completed branch removes more compatibility code than it adds: legacy
  manifest and runner modules were deleted, and CLI/output/lifecycle coverage
  was rewritten around direct plans.
- Resolve-time style audit passed for 18 branch-diff Rust files. Formatting and
  benchmark Clippy with warnings denied passed; 55 benchmark tests and 1,687
  workspace tests passed after final review fixes.
- Alternate `libaio` validation was not run because no storage or
  backend-neutral I/O implementation changed.

## Impacts

- All benchmark workloads now share one strict plan schema, sequential phase
  executor, typed fixture model, measurement model, and result entity.
- Author-facing execution is intentionally incompatible: lifecycle commands,
  per-workload CLI arguments, manifests, cleanup, and legacy artifacts no
  longer exist.
- Plans may create homogeneous ordered table pools and compose committed loads,
  compatible reads, index DDL, and coordinated lock scenarios without exposing
  runtime IDs in TOML.
- Workload modules own their full execution and verification contracts;
  executor changes no longer require central workload logic.
- Successful automation consumes `benchmark-result.toml`; humans receive a
  concise stdout summary and exact result path. Failed invocations retain no
  success artifact.
- No `doradb-storage` public API, engine behavior, persisted data, recovery,
  I/O backend, or unsafe inventory changed.
- RFC 0028 Phase 4 may rely on table creation, sequential load, the typed
  primary runtime binding, and the latest write fence established here.

## Test Cases

Completed coverage verifies:

1. Strict parsing and resolution for every workload, unknown and irrelevant
   field rejection, enum spellings, defaults, ranges, worker topology, replay
   policy, and checked arithmetic.
2. Plan fixture folds reject missing/incompatible tables, read-before-load,
   insufficient pool width, and invalid index shapes before root creation.
3. Runtime fixture binding validates ordered IDs, table count/shape, attempted
   ranges, successful rows, and latest write-fence requirements.
4. Sequential/random lookup, table scan, materialized index scan, and index
   stream preserve deterministic generation, batching, row classification,
   rollback, cancellation, and exact samples.
5. Index DDL covers empty and loaded tables, exact returned-index reuse,
   operation/sample equations, cancellation, failure propagation, and replay
   rejection.
6. Basic and specialized locks cover both modes and ownership scopes, stable
   and seeded selection, nested coverage, conversion, enqueue, head/middle/tail
   cancellation, promotion, first touch, scope close, FIFO order, and cleanup.
7. Every workload verifies counters, latency samples, fixture effects, and
   cancellation at its safe boundaries; first-error-wins draining emits no
   artifact on failure.
8. CLI tests require direct `--plan`, preserve root environment precedence,
   reject removed commands, and fail missing inputs before root creation.
9. Artifact tests verify atomic canonical TOML installation, full resolved
   configuration, stdout aggregate fields, and the absolute detailed path.
10. Template tests load exactly twelve self-contained workload plans with the
    shared engine defaults and matching final workload identities.
11. End-to-end lifecycle tests cover create/load/read composition, secondary
    scans, index DDL, multi-table locks, replay-safe warm-up/repetition, and
    success/failure publication behavior.
12. Final verification passed all documented formatting, lint, style, and test
    gates.

## Open Questions

None. Freeze/checkpoint, purge completion, fixture reset, restart/cold reads,
mixed workloads, and parallel actors remain assigned to RFC 0028 Phase 4 or
future scoped work.
