---
id: 000276
title: Pause Doradb Bench Benchmark Phase for Profiler Attachment
status: implemented
created: 2026-08-21
github_issue: 997
---

# Task: Pause Doradb Bench Benchmark Phase for Profiler Attachment

## Summary

Added an opt-in `pause` control to the final `doradb-bench` benchmark phase.
After every prepare phase completes and immediately before benchmark warm-ups,
the coordinator flushes a machine-readable readiness record and concise
instructions to standard error, sends `SIGSTOP` to itself, and continues after
an external `SIGCONT`.

The boundary lets an operator build a large fixture before attaching Samply or
another profiler. Because the stop occurs above fixture binding, workload
dispatch, timers, latency collection, internal-stat capture, and aggregation,
the stopped interval is excluded from benchmark results. Plans omit the field
by default and retain their previous behavior through normalized
`pause = false` resolution.

The implementation uses the safe `rustix` process API and installs no signal
handlers. The resolved pause setting is recorded in `benchmark-result.toml`,
while the existing success-only stdout summary remains unchanged.

## Context

RFC 0028 established a strict sequential plan with zero or more prepare phases
and exactly one final benchmark phase. The phase executor already closed each
prepare phase's sessions and applied its verified fixture effect before
entering the final warm-up and measured-run loops. That coordinator boundary
was therefore the narrow point where a process stop could exclude fixture
construction without changing workload measurement semantics.

The checked-in checkpoint plan creates one table, inserts 1,000,000 rows,
freezes a 500,000-row prefix, and measures checkpoint publication. Starting a
profiler with the process would include that preparation. The new boundary
allows attachment only after the fixture is ready while preserving the same
engine, workload, fixture, and output paths.

`SIGSTOP` cannot be caught or handled. A flushed readiness record necessarily
precedes kernel delivery of the stop, so observing the record alone does not
prove the process is stopped. Automation must observe Linux process state
`T`/`t` before attaching and sending `SIGCONT`; an early continuation can race
ahead of the self-stop.

Issue Labels:

- type:task
- priority:medium
- codex

## Goals

- Accept a strict optional `pause` boolean only on the final benchmark phase,
  with omission normalized to `false`.
- Stop the complete benchmark process once after successful preparation and
  before the first warm-up or measured execution.
- Keep the stop interval outside workload timing, samples, diagnostics, and
  aggregate calculations.
- Emit flushed standard-error protocol records containing the PID, one-based
  phase index, workload identity, and resume signal.
- Resume through the ordinary external `kill -CONT <pid>` workflow without
  application polling or signal handlers.
- Persist the effective pause setting in the canonical result and document a
  release-mode Samply workflow.

## Non-Goals

- No signal handler, cancellation protocol, custom signal exit code, or second
  escalation policy was added.
- The benchmark does not start, supervise, detect, or prove attachment of a
  profiler.
- Prepare phases, individual repetitions, and post-benchmark teardown have no
  additional pause boundaries.
- Normal teardown remains visible to an attached profiler.
- The stop does not guarantee that external resources or already-submitted
  kernel or device I/O cease making progress.
- No portability contract beyond the benchmark's Linux workflow was added.
- Workload semantics, checkpoint behavior, transaction behavior, recovery,
  persisted formats, and storage backends were not changed by the feature.

## Plan

### Plan and result contract

`RawPhase` carries `pause: Option<bool>`. Structural validation rejects any
explicit pause value on a prepare phase, including `pause = false`, alongside
benchmark repetition controls. The final benchmark resolves the omission into
`MeasurementSpec::pause = false`.

`MeasurementSpec` remains part of the serialized resolved plan, so successful
result TOML records the effective boolean additively. Checked-in templates
omit the field and cannot stop routine benchmark or CI execution.

### Phase boundary and protocol

The final `Phase::Benchmark` arm calls one private pause helper when the
resolved flag is true. The call is after all preceding dispatches, session
closes, and fixture-effect application, but before warm-up iteration, fixture
binding, or workload dispatch.

The helper locks standard error, writes the pausing record and human guidance,
flushes explicitly, and releases the lock before sending the stop signal. The
durable protocol records are:

```text
DORADB_BENCH_PAUSING pid=<pid> phase=<phase-index> workload=<identity> resume=SIGCONT
DORADB_BENCH_RESUMED pid=<pid> phase=<phase-index> workload=<identity>
```

The first record describes a process that is pausing, not one already proven
stopped. After `SIGCONT` lets the self-signal call return, the helper writes and
flushes the matching resumed record. Both records use standard error so stdout
retains only the existing successful benchmark summary.

### Signal and failure behavior

`doradb-bench` declares `rustix` directly with its `process` feature and calls
`kill_process(getpid(), Signal::STOP)`. This avoids raw `libc`, unsafe code,
and handler registration while expressing the single required process action.

Pre-stop write or flush failures and `SIGSTOP` delivery failures become
contextual `BenchError` values. The normal plan-execution cleanup path then
shuts down the engine and suppresses the success artifact and stdout summary.
Signals other than the deliberate self-stop retain operating-system behavior.

### Operational workflow

The benchmark documentation records a three-terminal release workflow:

1. Build `doradb-bench` with release debug information, opt the final phase
   into `pause = true`, and start it against a fresh root.
2. Read the emitted PID, verify `/proc/<pid>/status` reports `T` or `t`, and
   attach Samply with `samply record -p <pid>`.
3. Run `kill -CONT <pid>` from another terminal and let Samply follow the
   benchmark until process exit.

The million-row checkpoint template remains non-pausing unless copied and
explicitly opted in.

## Implementation Notes

Shipped the profiler boundary with strict plan validation, safe self-stop
signalling, stable standard-error records, result persistence, operator
documentation, and real subprocess verification. Existing non-pausing plans
retain their execution and stdout behavior.

The write path uses an injectable `Write` seam only for protocol formatting
and failure tests. Production obtains and releases the real stderr lock before
calling `SIGSTOP`; no production hook, polling loop, or notification handler
was introduced.

The lifecycle harness starts a real child benchmark, captures stdout and
stderr concurrently, waits for the pausing record, verifies process state
`T`/`t`, and confirms no summary or result artifact exists before continuation.
Its RAII guard sends `SIGCONT`, terminates, and reaps the child on assertion
failure, preventing a stopped process from leaking under Nextest.

The repository ignores `Cargo.lock`, so the planned direct dependency was
recorded in the workspace and benchmark manifests without a tracked lockfile
change.

After the implementation, Rust 1.98 introduced new Clippy diagnostics on the
branch. Verification-driven cleanup added the deliberate
`unused_async_trait_impl` workspace allowance, converted constant-sized slice
iteration to `as_chunks`/`as_chunks_mut`, and boxed rejected readonly-read send
errors only on the closed-channel path. These storage-source edits preserve
runtime behavior and were validated under both I/O backends; they are not part
of the profiler protocol or public storage API.

Final verification completed successfully:

- focused `doradb-bench` validation: 83 tests passed;
- workspace validation: 1,749 tests passed;
- alternate `libaio` validation: 1,667 tests passed;
- release `doradb-bench` compilation completed with debug information;
- focused line coverage was 87.16% for plan resolution and 86.07% for the
  phase executor;
- formatting, diff checks, strict default and `libaio` Clippy, and the
  branch-diff style audit passed without diagnostics.

No review issue or behavioral follow-up remains open.

## Impacts

- The strict benchmark phase schema and resolved result schema gain the
  additive `pause` boolean; prepare phases reject the field.
- Benchmark execution gains one optional Linux process-stop boundary before
  final-phase work. Normal plans take only a predictable boolean branch.
- Standard error gains stable pausing and resumed records for opted-in plans;
  the successful stdout summary is unchanged.
- The benchmark directly depends on the safe `rustix` process API.
- User documentation gains pause semantics, race guidance, process-wide stop
  limits, and the three-terminal Samply workflow.
- No public storage API, persisted database format, workload measurement
  equation, fixture transition, checkpoint algorithm, or backend behavior
  changed.
- Rust 1.98 lint cleanup made internal storage-source mechanical changes and a
  closed-channel error representation change with no success-path allocation.

## Test Cases

- Strict parsing accepts both benchmark pause values, defaults omission to
  resolved false, rejects invalid types and spellings, and rejects either
  explicit value on prepare phases.
- Resolved plan and result round trips preserve effective true and false
  values; every checked-in template resolves to false.
- Non-pausing lifecycle execution emits no profiler record and preserves the
  existing summary, measured counts, and success artifact.
- Protocol unit tests verify exact pausing/resumed records and contextual
  pre-stop write and flush failures.
- The Linux subprocess scenario verifies PID, phase, workload, stopped state,
  absence of pre-resume success output, external continuation, matching resume
  record, exact measured runs, persisted `pause = true`, and successful exit.
- The subprocess deadline and cleanup guard verify that assertion failures do
  not leak a stopped benchmark child.
- Full default and alternate-backend suites verify unchanged storage and I/O
  behavior after the Rust 1.98 lint cleanup.

## Open Questions

No unresolved question or deferred follow-up remains for this task.

Additional signal policies, profiler supervision, or profiling barriers would
require separately scoped work if later needed.
