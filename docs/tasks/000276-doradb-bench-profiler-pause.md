---
id: 000276
title: Pause Doradb Bench Benchmark Phase for Profiler Attachment
status: proposal
created: 2026-08-21
github_issue: 997
---

# Task: Pause Doradb Bench Benchmark Phase for Profiler Attachment

## Summary

Add an opt-in `pause` control to the final `doradb-bench` benchmark phase so a
large fixture can be prepared before a profiler attaches. After every prepare
phase succeeds and immediately before benchmark warm-ups, the coordinator
prints and flushes the process ID and resume instructions to standard error,
sends `SIGSTOP` to itself, and continues only after an external `SIGCONT`.

The implementation uses a safe direct process-signal API and installs no
signal handlers. Existing plans default to `pause = false`; their execution is
unchanged. The primary documented workflow builds `doradb-bench` in release
mode, loads the existing million-row checkpoint fixture, attaches Samply to the
stopped PID, and resumes the final checkpoint phase.

## Context

The composable phase framework in
`docs/rfcs/0028-composable-doradb-bench-phase-framework.md` requires exactly
one final benchmark phase after zero or more prepare phases. `RawPhase` in
`doradb-bench/src/plan.rs` currently accepts benchmark repetition controls,
resolves them into `MeasurementSpec`, and rejects those controls on prepare
phases. `execute_phases` in `doradb-bench/src/plan_executor.rs` executes all
prepare phases before entering the final benchmark's warm-up and measured-run
loops. Workload timing and internal-stat capture start below that coordinator
boundary, so a stop inserted there is excluded from measured results.

The checked-in `doradb-bench/templates/checkpoint-table.toml` already creates
one table, inserts 1,000,000 rows, freezes a 500,000-row prefix, and measures a
checkpoint. Starting Samply with the benchmark process would also profile that
fixture construction. An explicit phase-boundary stop lets `samply record -p
<pid>` attach only after fixture preparation, while retaining the same plan,
engine, workload, and measurement paths.

`SIGSTOP` cannot be caught or handled, and `SIGCONT` resumes a stopped process
without application-side notification logic. The program must emit its
instructions before stopping. Consequently, the readiness record describes a
process that is *pausing*, not one proven already stopped: an automated caller
must observe stopped process state before sending `SIGCONT`, otherwise an
early `SIGCONT` can race ahead of the self-stop.

Issue Labels:

- type:task
- priority:medium
- codex

## Goals

- Add a strict optional `pause` boolean accepted only by the final benchmark
  phase and default it to `false`.
- Stop the complete benchmark process exactly once after successful fixture
  preparation and before the first warm-up or measured execution.
- Keep the stop duration outside workload timers, latency samples, diagnostic
  deltas, and aggregate calculations.
- Emit a flushed, machine-recognizable standard-error record containing the
  PID, one-based phase index, workload identity, and `SIGCONT` resume signal,
  followed by concise human instructions.
- Resume through the ordinary external `kill -CONT <pid>` operation without a
  polling loop or application signal handler.
- Record the normalized pause setting in the resolved plan stored in the
  successful result artifact.
- Document and test a release-mode Samply attachment workflow that excludes
  the potentially large prepare phases.

## Non-Goals

- Install handlers for `SIGSTOP`, `SIGCONT`, `SIGUSR1`, `SIGINT`, `SIGTERM`, or
  any other signal.
- Add graceful process-signal cancellation, custom signal exit codes, a second
  signal escalation policy, or changes to `RunCancellation`.
- Start, stop, supervise, or detect Samply or another profiler from
  `doradb-bench`.
- Prove profiler attachment before resuming; the external caller owns
  attachment and `SIGCONT` sequencing.
- Pause prepare phases, pause each warm-up or measured run, add a post-phase
  detach barrier, or exclude normal benchmark teardown from an attached
  process profile.
- Change workload measurement boundaries, checkpoint behavior, storage engine
  scheduling, transaction behavior, recovery, persisted formats, or I/O
  backends.
- Guarantee that already-submitted kernel or device I/O makes no progress
  while userspace threads are stopped.
- Add non-Linux portability for the benchmark's process-control workflow.

## Plan

### Plan model and validation

Add `pause: Option<bool>` to `RawPhase`. Extend
`validate_phase_structure` so any explicit `pause` field, including
`pause = false`, is rejected on a prepare phase alongside `warmup_runs` and
`measured_runs`. Resolve an omitted benchmark value to `false` and store it as
`pause: bool` in `MeasurementSpec`.

Because `MeasurementSpec` is serialized as part of the resolved invocation
plan, successful result TOML records the effective setting as an additive
field. Input plans that omit the field retain current execution behavior.
Templates remain non-pausing by default so routine benchmark and CI execution
cannot unexpectedly stop.

### Process stop boundary and protocol

In the `Phase::Benchmark` arm of `execute_phases`, call a private
`pause_for_profiler` helper once when `measurement.pause` is true. Place the
call before the warm-up loop and before fixture binding or workload dispatch,
after all preceding prepare dispatches have returned, their sessions have
closed, and their fixture effects have been applied.

The helper obtains the current PID, locks standard error, writes the protocol
record and human guidance, flushes explicitly, and releases the stream lock
before sending the stop signal. The stable protocol record is:

```text
DORADB_BENCH_PAUSING pid=<pid> phase=<phase-index> workload=<identity> resume=SIGCONT
```

The following guidance tells the operator to attach a profiler, verify that
the process is stopped, and run `kill -CONT <pid>`. After `SIGCONT` allows the
self-signal call to return, emit:

```text
DORADB_BENCH_RESUMED pid=<pid> phase=<phase-index> workload=<identity>
```

Both records go to standard error so the existing success-only standard-output
summary remains unchanged. Failure to write or flush the pre-stop message, or
failure to send `SIGSTOP`, returns a contextual `BenchError`; the existing
`execute_plan` cleanup path then shuts down the engine and suppresses the
success artifact.

There is an unavoidable interval between the flushed pre-stop record and
kernel delivery of `SIGSTOP`. Documentation and subprocess tests therefore
must wait for Linux process state `T`/stopped before sending `SIGCONT`; the
protocol must not claim that observing the record alone proves the stop is
complete.

### Safe signal dependency

Declare `rustix` with its `process` feature in workspace dependencies and as a
direct `doradb-bench` dependency. Use
`rustix::process::kill_process(rustix::process::getpid(), Signal::STOP)` to
self-stop. Map a returned `rustix::io::Errno` into a contextual benchmark
error.

`rustix` is already present transitively in the lockfile, but the benchmark
must declare every API it uses directly. This safe wrapper avoids adding
`signal-hook`, avoids raw `libc` unsafe code and its repository unsafe-review
obligations, and precisely expresses the only required process operation.

Do not register `SIGCONT` or termination handlers. Signals other than the
self-issued `SIGSTOP` retain their platform-default behavior. A stopped
process may require `SIGCONT` before a non-forcing signal can make userspace
progress; `SIGKILL` remains ordinary operating-system behavior rather than a
benchmark feature.

### Documentation and profiling workflow

Extend `docs/benchmark-tool.md` with the input field, its benchmark-only
validation, its once-before-warm-ups semantics, and the fact that all process
threads stop while external resources and already-issued kernel I/O remain
outside that guarantee.

Document a three-terminal Samply workflow using the existing release profile,
which already retains debug information:

1. Build `doradb-bench` with `cargo build --release -p doradb-bench`, add
   `pause = true` to the final phase, and start the binary normally.
2. Parse or copy the emitted PID, confirm the process is stopped, and run
   `samply record -p <pid>` (optionally with `--save-only` and `--output`).
3. From another terminal, run `kill -CONT <pid>`; Samply records the final
   benchmark and exits when the target process exits.

Explain that the checked-in checkpoint template provides the large fixture
but remains `pause = false` unless the operator opts in.

## Implementation Notes

## Impacts

- `Cargo.toml`, `doradb-bench/Cargo.toml`, and `Cargo.lock` gain the direct
  safe process-signal dependency declaration or feature activation.
- `doradb-bench/src/plan.rs` gains the strict raw and resolved `pause` fields,
  prepare-phase validation, and parsing/serialization tests.
- `doradb-bench/src/plan_executor.rs` gains the one-time phase-boundary helper,
  standard-error protocol, and self-`SIGSTOP` call.
- `doradb-bench/src/plan_output.rs` expectations include the normalized
  resolved-plan field; benchmark metrics and stdout summary formats do not
  change.
- `doradb-bench/tests/lifecycle.rs` gains Linux subprocess stop/resume coverage
  with bounded waits and cleanup protection for stopped children.
- `docs/benchmark-tool.md` gains plan and Samply usage documentation.
- `doradb-storage`, public storage APIs, plan workload variants, fixture
  transitions, and measurement arithmetic are unchanged.

## Test Cases

- Strict plan parsing accepts `pause = true` and `pause = false` on a benchmark
  phase, defaults omission to resolved `false`, and rejects non-boolean or
  unknown spellings through existing serde validation.
- Structural validation rejects any explicit `pause` field on a prepare phase
  before the invocation root is created.
- Resolved-plan and result-output round trips preserve both effective pause
  values, while existing plan fixtures and templates resolve to `false`.
- A non-pausing lifecycle plan runs without pause records and preserves the
  existing stdout summary, measurement counts, and success artifact behavior.
- A bounded Linux subprocess test runs a minimal plan containing at least one
  prepare phase and `pause = true`, reads the `DORADB_BENCH_PAUSING` record,
  verifies its PID/phase/workload fields, and observes stopped state before
  sending `SIGCONT`.
- Before resume, that subprocess has produced neither a benchmark summary nor
  `benchmark-result.toml`. After resume it emits the matching resumed record,
  completes exactly the declared warm-up and measured runs, writes a result
  with `pause = true`, and exits successfully.
- The subprocess harness uses a deadline and an RAII cleanup guard that sends
  `SIGCONT` and then terminates/reaps the child on assertion failure, preventing
  a stopped test process from leaking or hanging Nextest.
- Unit tests cover pre-stop write/flush failure mapping where an injectable
  writer seam is used; real signal behavior remains isolated to the child
  process rather than stopping the test runner.
- `rtk cargo nextest run -p doradb-bench`, `rtk cargo nextest run --workspace`,
  release compilation, formatting, and workspace Clippy with warnings denied
  pass.

## Open Questions

None. Signal-handler-based cancellation and additional profiling barriers are
explicitly outside this task and require separately scoped work if desired.
