---
id: 000267
title: Migrate doradb-bench Simple Workloads and Basic Fixture State
status: implemented
created: 2026-08-12
github_issue: 971
---

# Task: Migrate doradb-bench Simple Workloads and Basic Fixture State

## Summary

Implemented RFC 0028 Phase 2 by migrating `create-table`, `stmt-noop`,
`insert-seq`, `insert-rand`, and `table-ddl` to the TOML plan executor.
The executor now composes simple workloads through typed plan and runtime
fixture effects, workload-specific latency units, checked outcome counters,
and cooperative first-error-wins cancellation.

Plans accept human-readable byte sizes and resolve them to exact numeric byte
counts. Five directly runnable workload templates share durable engine
defaults. Unexpected invocation failures produce no result artifact pair;
duplicate-key and write-conflict insert outcomes remain measured terminal
attempts.

## Context

Task 000266 established the plan parser, engine overlay, sequential phase
executor, measurement model, and `trx-noop` vertical slice. Phase 2 needed to
prove prepare-to-benchmark composition with the existing simple workloads
before RFC 0028 could add loaded-data requirements, table pools, semantic
fences, and coordinated workloads.

The transitional CLI runners already defined the storage semantics to
preserve: deterministic generated keys and payloads, transaction batching,
statement no-ops inside one transaction, and transient table create/drop
cycles. The plan path previously had no concrete fixture state or typed
dispatch for these operations.

Insert workloads also needed to distinguish duplicate keys and write conflicts
from all other operation errors without matching rendered messages. The
storage error boundary therefore exposes its existing fieldless operation
context as typed public observation while retaining crate-private production
and disclosure.

The plan and artifact schema are intentionally unversioned. This task changed
author-facing byte fields from integer counts with `_bytes` suffixes to
`byte-unit` strings while keeping resolved artifacts exact and
unit-independent.

Parent RFC:

- `docs/rfcs/0028-composable-doradb-bench-phase-framework.md` (Phase 2)

Issue Labels:

- type:task
- priority:medium
- codex

## Goals

- Migrate all five simple workloads to strict closed plan dispatch.
- Share their public-session operation cores with transitional legacy runners.
- Validate primary-table shape, generated-key allocation, and phase effects
  before and during execution.
- Measure an explicit end-to-end latency unit for every migrated workload.
- Count only duplicate-key and write-conflict insert failures as expected
  terminal attempts.
- Drain tasks and sessions after the first unexpected error and publish only
  complete successful invocations.
- Accept human-readable byte inputs and record exact resolved byte counts.
- Provide shared durable engine defaults and one complete template per
  plan-enabled workload.

## Non-Goals

- Migrating lookup, scan, index-DDL, index-stream, or lock workloads; these
  remain RFC 0028 Phase 3.
- Implementing loaded-data read requirements, table pools, semantic waits,
  actor coordination, or parallel phases.
- Removing the transitional lifecycle commands and manifest.
- Adding checkpoint, freeze, update, delete, mixed, restart, cold-cache, or
  fixture-reset workloads.
- Retrying or configuring expected insert outcomes.
- Preserving the former unversioned byte-input or failed-result schemas.
- Changing storage transaction, catalog, index, recovery, or persisted formats.
- Adding benchmark performance thresholds.

## Plan

### Plan model and configuration

`WorkloadSpec` and `ResolvedWorkload` remain closed enums with concrete
variants for the five migrated workloads plus `trx-noop`. Serde-facing
structures reject unknown fields. Resolved plans serialize every inherited
default, worker count, key range, index shape, batch size, value size, and
diagnostic setting.

Worker controls resolve by phase override over `[workload_defaults]`.
Defaults are one thread, sessions equal to threads, 128-byte values, batch size
one, and diagnostics disabled. An explicit thread override without sessions
sets sessions equal to threads; threads greater than sessions are rejected.

All byte-valued plan leaves deserialize through `byte_unit::Byte`. Raw input
uses `meta_buffer_size`, `log_block_size`, `log_file_max_size`,
`max_file_size`, `max_mem_size`, `readonly_buffer_size`, and
`value_size`. Checked conversions enforce the target `u64` or `usize`
boundary. Resolved output retains exact numeric `*_bytes` fields.

Engine overlay defaults are seeded from the authoritative
`EngineConfig::default()` sub-configurations, then merged by leaf in this
order:

```text
EngineConfig defaults < included engine defaults < plan-local engine
```

The checked-in defaults select `fsync`, 512 MiB/1 GiB index-buffer
memory/file capacity, 1 GiB/2 GiB data-buffer capacity, and a 1 GiB readonly
buffer. All omitted values retain storage defaults.

### Fixture planning and runtime binding

The shared fixture module owns `IndexMode`, checked half-open `KeyRange`,
the fixed two-column table schema, index builders, primary-table shape, and
separate plan/runtime state machines.

Plan validation folds phases in order before root creation:

1. Validate phase structure and the single final benchmark.
2. Resolve the workload against current fixture state and defaults.
3. Produce its typed `FixturePlanEffect`.
4. Apply the effect before resolving the next phase.
5. Enforce replay policy and checked arithmetic.

`CreatePrimary` establishes one empty primary shape. `Insert` requires that
primary, allocates a contiguous nonempty attempted range from its cursor, and
advances the cursor even if runtime attempts later end in expected errors.
No-op and transient DDL workloads have no fixture effect.

Runtime state binds the planned primary shape to its returned `TableID`.
Insert effects carry the exact attempted range, inserted-row count, and the
greatest commit `TrxID` from a batch that wrote at least one row. An
all-expected-error run does not fabricate or clear an earlier write fence.

A structural phase transition occurs only after every task has drained, every
session has closed, counters and latency samples satisfy the workload
equations, and the runtime effect matches the resolved plan.

### Workload execution and measurement

The plan executor uses exhaustive enum matches rather than a dynamic workload
registry. Focused operation cores in the no-op, insert, and DDL modules accept
optional timing and cancellation inputs; legacy runners call the same cores
without plan measurement state.

Create-table owns one session. Other workloads use deterministic aggregate
session partitioning and the requested worker thread count. Prepare phases
record no latency. Warm-ups execute and validate the measured path but discard
their outcomes. Only statement and transaction no-ops are replay-safe;
create-table, inserts, and table DDL are single-run.

Latency units and successful-run equations are:

| Workload | Unit | Counter/sample invariant |
| --- | --- | --- |
| `trx-noop` | transaction lifecycle | operations = samples = requested transactions |
| `stmt-noop` | statement execution | operations = samples = requested statements |
| `create-table` | table creation | operations = samples = 1 |
| inserts | insert batch transaction | operations = attempts; samples = sum of per-session batch ceilings |
| `table-ddl` | table create/drop cycle | operations = 2 × cycles; samples = cycles |

### Expected outcomes and cancellation

`OperationError` is public, copyable, non-exhaustive, and re-exported by
`doradb-storage`. `Error::operation_error()` returns it only when the
public boundary is `ErrorKind::Operation`; nested operation frames under
another boundary do not change classification. Error construction, typed
result carriers, and disclosure remain crate-private.

Insert execution accepts only `DuplicateKey` and `WriteConflict` as
expected outcomes. It continues the reusable transaction after those
statement-local failures and records nested checked counters. Every other
error is invocation-fatal. Successful accounting satisfies:

```text
operations = inserted_rows + duplicate_key + write_conflict
```

`RunCancellation` stores the first unexpected error and signals peer tasks.
Peers stop at workload-safe boundaries, all task futures remain attached, and
all sessions are closed. A cancelled statement-noop transaction rolls back
best-effort and returns zero counters and latency because none of its partial
statements committed. Later rollback, close, merge, or coordination errors do
not replace the first error.

### Artifacts and templates

Plan results are success-only. The executor writes the canonical TOML and
Markdown pair only after all phases, runtime verification, aggregation, and
engine shutdown succeed. Pair installation remains atomic. Failures after
marker installation may retain the root for diagnosis and guarded cleanup but
leave no complete result pair.

The five workload templates explicitly include `engine-defaults.toml` and
specify `threads = 1` and `sessions = 1` in their benchmark workloads.
Insert templates create an index-free primary first. No-op and table-DDL
templates contain no irrelevant primary fixture.

## Implementation Notes

Implemented RFC 0028 Phase 2 with five simple workloads, typed fixture state,
workload-specific measurement, complete templates, and success-only artifacts.

The final storage API exposes `OperationError` directly rather than adding a
duplicate `OperationErrorKind` mirror. Review confirmed the existing enum is
fieldless and already expresses the supported caller decisions; the typed
accessor preserves the outer public-boundary check.

Review also removed an unused plan-time possible-fence flag. Actual commit
fences remain runtime-only because no Phase 2 planning decision consumes them.
The statement-noop cancellation path was corrected to ignore rollback errors
and discard partial counters/samples from its rolled-back transaction.

The templates were made explicit about their single-thread, single-session
benchmark settings while preserving the original resolved behavior. Engine
default resolution was based directly on `EngineConfig::default()` rather
than duplicating storage defaults.

Final verification completed:

- 1,773 workspace tests passed, including 141 benchmark crate tests.
- End-to-end lifecycle tests exercised create-table, statement no-op,
  sequential/random insert, table-DDL, reopen/scan, bootstrap failure, and
  guarded cleanup behavior.
- Formatting and workspace Clippy with warnings denied passed.
- The branch-diff style audit passed for 21 Rust files.
- Diff whitespace validation passed.
- The read-only storage error audit found no required write.

The alternate `libaio` validation was not run because this task changed no
storage I/O backend or backend-dependent behavior.

## Impacts

- Benchmark plans gain five strict workload variants and basic primary-fixture
  composition while legacy commands remain available.
- Plan byte input is intentionally incompatible: human-readable strings and
  unsuffixed raw names replace integer `*_bytes` fields.
- Successful artifacts contain exact resolved engine/workload values, new
  latency units, and nested expected-outcome counters. Failed result entities
  are removed.
- `doradb-storage` gains the additive public `OperationError` observation
  API; storage behavior and persisted formats are unchanged.
- Insert throughput counts all terminal attempts; inserted rows count only
  successful writes.
- Cooperative cancellation may finish an already active safe unit but never
  publishes a partial invocation.
- Five self-contained templates and benchmark documentation provide the
  executable Phase 2 examples and contracts.
- RFC 0028 Phase 3 may rely on the primary shape, attempted range, generated-key
  cursor, and conditional runtime write fence established here.

## Test Cases

Completed coverage includes:

- Strict parsing and resolution for every workload, defaults and phase
  overrides, worker relationships, replay policy, unknown fields, count/range
  overflow, and invalid fixture ordering.
- Byte-unit parsing, checked integer boundaries, rejected former input syntax,
  engine-default include precedence, and exact resolved byte values.
- Ordered create/insert fixture folds, contiguous attempted ranges, duplicate
  primary and insert-before-create rejection, runtime `TableID` binding, and
  fence preservation.
- Exact per-workload operation/sample equations, checked histogram and counter
  merges, warm-up exclusion, and prepare-phase no-sampling behavior.
- Typed observation of every current `OperationError` and real unique-index
  duplicate-key accounting without message inspection.
- Deterministic inserts, expected-outcome equations, greatest write-bearing
  commit selection, and public reopen/scan verification.
- First-error-wins cancellation, attached task/session draining, success-only
  artifact serialization, atomic result-pair failure handling, and no artifacts
  after bootstrap failure.
- Parsing all checked-in templates with their shared durable defaults and
  matching final benchmark workloads.

## Open Questions

None. Loaded-data requirements, table pools, semantic fence consumers, and
coordinated workloads remain assigned to RFC 0028 Phase 3.
