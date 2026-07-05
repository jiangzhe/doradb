---
id: 000212
title: Introduce Observability Logging
status: proposal  # proposal | implemented | superseded
created: 2026-07-04
github_issue: 817
---

# Task: Introduce Observability Logging

## Summary

Introduce application-facing **observability logging** in `doradb-storage`
without changing durability, recovery, or public storage behavior.

The implementation should add the Rust `log` facade as an explicit dependency,
route storage call sites through a crate-local `obs` module to avoid confusion
with the existing redo-log `crate::log` module, replace production
`eprintln!` usage, and instrument low-frequency lifecycle, background-worker,
checkpoint, recovery, and fatal-failure boundaries.

The storage crate remains a library. It must emit observability records only
through the `log` facade and must not install or configure a logger backend.

## Context

DoraDB already uses the term "log" for durable redo/commit-log records. That
terminology is recovery-critical: committed foreground changes are persisted in
the redo stream, checkpoint publishes committed table state through CoW roots,
and restart loads checkpointed files before replaying redo. This task uses
**observability logging** for runtime application-facing logs so implementation
and documentation do not blur it with redo-log durability.

An initial guideline document has been drafted, approved, and mirrored into
this task worktree as `docs/observability-logging.md`. Implementation must
treat that document as the coding guideline and update it only when the code
change discovers a concrete mismatch or missing rule.

Current production runtime logging is minimal. The named-thread helper in
`doradb-storage/src/thread.rs` documents that it logs start/finish lifecycle
events, but it writes directly to stderr with `eprintln!`. The named helper is
used by important background workers including redo writer, transaction
cleanup, purge, shared I/O, shared pool eviction, and redo read-ahead. This
makes it a natural first observability boundary.

The change is task-sized because it establishes a narrow facade and focused
instrumentation boundaries. It does not introduce tracing, a full event catalog,
or exhaustive foreground DML/MVCC/index logging.

Issue Labels:
- type:task
- priority:medium
- codex

Relevant references:
- `docs/architecture.md`: redo-log durability and background storage
  architecture.
- `docs/checkpoint-and-recovery.md`: checkpoint, redo, and recovery boundaries.
- `docs/table-file.md`: checkpoint publication and table-root identity.
- `docs/process/coding-guidance.md`: reliability/performance priorities and
  lint expectations.
- `docs/process/unit-test.md`: current validation command policy.
- `docs/observability-logging.md`: task-specific observability guideline to add
  or refresh in this branch.
- `doradb-storage/src/thread.rs`: current production `eprintln!` lifecycle
  logging.
- `doradb-storage/src/component.rs`: component build/shutdown boundaries.
- `doradb-storage/src/engine.rs`: engine build and shutdown boundaries.
- `doradb-storage/src/trx/sys.rs`: redo worker, cleanup worker, transaction
  worker shutdown, and storage poison boundaries.
- `doradb-storage/src/trx/purge.rs`: purge coordinator/executor workers and
  fatal purge boundaries.
- `doradb-storage/src/file/fs.rs`: shared storage I/O worker.
- `doradb-storage/src/buffer/evictor.rs`: shared pool evictor worker.
- `doradb-storage/src/recovery/mod.rs` and
  `doradb-storage/src/recovery/stream.rs`: recovery phases and redo read-ahead.
- `doradb-storage/src/table/persistence.rs` and
  `doradb-storage/src/session.rs`: checkpoint outcomes and public checkpoint
  entrypoints.

## Goals

1. Keep `docs/observability-logging.md` in the task branch as the approved
   guideline:
   - terminology distinguishing observability logging from redo/commit logs;
   - `obs` facade rules;
   - required and optional key-value fields;
   - logging level policy;
   - disabled-log performance rules;
   - initial instrumentation scope.
2. Add `log` as an explicit workspace dependency and a direct
   `doradb-storage` dependency.
3. Add a crate-local `obs` module for observability logging:
   - call sites use `crate::obs`/`obs::...!`, not direct `::log::...!`;
   - direct `::log` macro use is limited to the facade implementation;
   - the facade re-exports the standard log macros and types needed by storage
     call sites, including level checks for expensive messages.
4. Keep `obs` crate-private for this task. Do not add public logging API.
5. Replace production `eprintln!` usage in `doradb-storage/src` with
   observability logging.
6. Emit focused observability logs for:
   - engine build start/finish/failure;
   - engine shutdown start/finish/busy/failure;
   - component build/shutdown start/finish/failure;
   - named background worker start/finish/failure where the failure is
     observable at the worker boundary;
   - storage poison publication and fatal reason;
   - recovery major phase start/finish/failure;
   - user-table and catalog checkpoint publish, delay, cancellation, and fatal
     publication failure.
7. Use the level policy from `docs/observability-logging.md` consistently:
   - `error` for fatal storage poison, unrecoverable background failure, and
     progress-stopping failures;
   - `warn` for normal but abnormal handled outcomes such as checkpoint delay
     or cancellation;
   - `info` for low-frequency lifecycle milestones and major successful
     operations;
   - `debug` for diagnostic batch summaries;
   - `trace` only for high-volume internals behind level checks.
8. Keep disabled logging overhead low:
   - do not allocate strings only for disabled log records;
   - guard expensive formatting or summary computation with `obs::log_enabled!`;
   - avoid logging inside hot per-row/per-key/per-page loops except trace-level
     events with explicit guards.

## Non-Goals

1. Do not add a logger implementation such as `env_logger`,
   `simple_logger`, `tracing-subscriber`, or a custom sink.
2. Do not migrate to `tracing`.
3. Do not define a complete event catalog or span model.
4. Do not instrument every foreground DML, MVCC, row, index, or buffer-cache
   operation in this task.
5. Do not change transaction, redo, checkpoint, recovery, shutdown, or storage
   correctness behavior to make logging easier.
6. Do not add public API for logging configuration.
7. Do not require downstream applications to enable logging.
8. Do not replace example-binary CLI `eprintln!` error reporting unless the
   example also installs an application logger. The required replacement target
   is production library/runtime stderr output under `doradb-storage/src`.
9. Do not add noisy `info`, `warn`, or `error` logs in per-row, per-key, or
   per-page hot paths unless the event is exceptional and directly explains a
   failure.

## Unsafe Considerations

No new unsafe code is planned.

If implementation unexpectedly touches unsafe-sensitive I/O, buffer, or row
storage internals only to add logs, stop and rescope. Observability logging must
not widen unsafe contracts or alter memory/lifetime invariants.

## Plan

1. Verify the observability guideline document.
   - Confirm `docs/observability-logging.md` is present in the task branch.
   - Ensure the document uses the term "observability logging" consistently.
   - Ensure it reserves "redo log" and "commit log" for durable recovery data.
   - Ensure it documents required fields, optional fields, key-value formatting,
     levels, and performance rules.

2. Add dependency wiring.
   - Add `log = "0.4"` or the current compatible `0.4` workspace dependency to
     the root `Cargo.toml`.
   - Add `log = { workspace = true }` to `doradb-storage/Cargo.toml`.
   - Update `Cargo.lock` as needed. `log` may already be transitively present,
     but `doradb-storage` must depend on it directly.
   - Do not add any concrete logger crate.

3. Add the `obs` facade.
   - Add `mod obs;` to `doradb-storage/src/lib.rs`.
   - Add `doradb-storage/src/obs.rs`.
   - Re-export the log macros and types storage code needs, for example:
     `debug`, `error`, `info`, `log_enabled`, `trace`, `warn`, `Level`, and
     `LevelFilter` if needed.
   - Keep facade visibility crate-private.
   - Add module docs explaining that `obs` is observability logging and
     `crate::log` is redo-log implementation.
   - Prefer call-site imports like `use crate::obs;`, then
     `obs::info!(...)`.

4. Convert production stderr logging.
   - Replace `eprintln!` in `doradb-storage/src/thread.rs` with `obs::info!`.
   - Format thread lifecycle records as stable key-value text, for example:
     `event=worker_lifecycle component=runtime worker=Log-Thread thread_id={:?} action=start result=ok`.
   - Preserve existing thread behavior. Do not add panic catching or lifecycle
     semantics that would change worker failure behavior.
   - Run `rg -n "eprintln!" doradb-storage/src` and ensure no production
     runtime stderr logging remains.

5. Instrument component and engine lifecycle.
   - In `component.rs`, log component build start/finish/failure in
     `RegistryBuilder::build::<C>` using `C::NAME`.
   - In `component.rs`, log component shutdown start/finish around erased
     component shutdown.
   - In `engine.rs`, log engine build start/finish/failure. If needed, extract
     the existing build body into a private helper so errors can be logged once
     before being returned.
   - In `engine.rs`, log `shutdown` and `try_shutdown` start/finish. Use `warn`
     for normal busy shutdown rejection and `error` for unexpected fatal
     shutdown failure before panic.
   - Avoid logging repeatedly inside shutdown wait loops.

6. Instrument background worker boundaries.
   - The generic named-thread helper should cover start/finish for:
     - `Log-Thread`;
     - `Trx-Cleanup-Thread`;
     - `Purge-Thread`;
     - `Purge-Dispatcher`;
     - `Purge-Executor-*`;
     - `IO-Thread`;
     - `Shared-Pool-Evictor`;
     - `Redo-ReadAhead`.
   - Add worker-boundary `error` logs where a worker already observes and
     handles an unrecoverable result, such as redo read-ahead returning an error
     item.
   - Do not add logs to test-only thread spawns unless production code is
     changed in the same file and the test needs adjustment.

7. Instrument storage poison and fatal maintenance boundaries.
   - In `TransactionSystem::poison_storage`, emit one `error` record only for
     the first published poison reason. Later poison attempts should not spam
     identical fatal logs; at most use `debug` if useful.
   - Include `event=storage_poison`, `component=trx`, `action=poison`,
     `result=error`, and `fatal_reason=...`.
   - Add `error` logs at fatal checkpoint/purge/rollback boundaries only when
     the boundary has local context not already captured by `poison_storage`.
     Avoid duplicate high-severity records for the same failure unless the
     extra record names a distinct component and action.

8. Instrument recovery major phases.
   - In `RecoveryCoordinator::recover_all`, log recovery start and finish.
   - Log start/finish/failure for major phases already visible in the method:
     checkpointed user-table bootstrap, redo planning/replay, metadata
     validation, absent-file cleanup, index rebuild/page refresh, and redo
     repair/startup preparation.
   - If needed, extract small helper wrappers to avoid repetitive match blocks,
     but do not introduce a large recovery framework.
   - Use `info` for phase start/finish and `error` for unrecoverable recovery
     failure.

9. Instrument checkpoint outcomes.
   - For user-table checkpoints, instrument the outcome boundary near
     `Table::checkpoint` or `Session::checkpoint_table`, whichever avoids
     duplicated logs and has the necessary `table_id`.
   - Emit:
     - `info` for `CheckpointOutcome::Published` with `table_id`,
       `checkpoint_ts`, and `silent`;
     - `warn` for `CheckpointOutcome::Delayed` with `table_id`,
       `effective_ts`, and `min_active_sts`;
     - `warn` for `CheckpointOutcome::Cancelled` with `table_id` and
       `reason`.
   - For catalog checkpoints, instrument `Catalog::checkpoint_now` outcomes:
     - `info` for `CatalogCheckpointOutcome::Published`;
     - `debug` or `info` for `Noop` depending on existing call frequency;
     - `error` for returned failures with local catalog checkpoint context.
   - For fatal table checkpoint publication failures that call
     `poison_storage(FatalError::CheckpointWrite)`, rely on the storage poison
     record for high-severity logging unless local table/checkpoint context is
     otherwise unavailable.

10. Keep message shape consistent.
    - Every new observability message should include `event`, `component`,
      `action`, and `result`.
    - Add optional identifiers only when available without extra work.
    - Use `obs::log_enabled!` before expensive summaries or allocations.
    - When logging observes an existing `Result` and returns it unchanged,
      prefer inline `Result::inspect` and `Result::inspect_err` at the
      boundary. Avoid separate helper functions whose only job is result
      logging unless they are shared by multiple call sites or materially
      improve readability.
    - For production worker-owner join paths where shutdown or drop is already
      best-effort, log join panics with `inspect_err` and ignore the join
      result. Keep test joins strict so worker panics still fail tests.
    - Keep free-form errors at the end as `error=...`.

11. Validate with code review and commands.
    - Confirm call sites use `obs::...!` rather than direct `::log::...!`
      outside `obs.rs`.
    - Confirm no production `doradb-storage/src` `eprintln!` remains.
    - Confirm no concrete logger implementation was added.
    - Confirm no hot per-row/per-key/per-page logs were added outside trace
      guards.

## Implementation Notes

## Impacts

Expected file and module impacts:

- `docs/observability-logging.md`: add or refresh observability guideline.
- `Cargo.toml`: add workspace `log` dependency.
- `Cargo.lock`: update if direct dependency changes lock metadata.
- `doradb-storage/Cargo.toml`: add direct `log` dependency.
- `doradb-storage/src/lib.rs`: register crate-local `obs` module.
- `doradb-storage/src/obs.rs`: new facade over `log`.
- `doradb-storage/src/thread.rs`: replace direct stderr lifecycle logging.
- `doradb-storage/src/component.rs`: component build/shutdown lifecycle logs.
- `doradb-storage/src/engine.rs`: engine build and shutdown lifecycle logs.
- `doradb-storage/src/trx/sys.rs`: transaction-system worker shutdown and
  storage poison logs.
- `doradb-storage/src/trx/purge.rs`: purge worker/fatal maintenance logs when
  not already covered by generic worker lifecycle and storage poison.
- `doradb-storage/src/file/fs.rs`: shared I/O worker lifecycle is covered by
  `spawn_named`; add local logs only for meaningful I/O worker boundary
  failures.
- `doradb-storage/src/buffer/evictor.rs`: shared evictor lifecycle is covered
  by `spawn_named`; add local logs only for meaningful evictor boundary
  failures.
- `doradb-storage/src/recovery/mod.rs`: recovery phase logs.
- `doradb-storage/src/recovery/stream.rs`: redo read-ahead worker error logs.
- `doradb-storage/src/session.rs`, `doradb-storage/src/table/persistence.rs`,
  and `doradb-storage/src/catalog/checkpoint.rs`: checkpoint outcome logs.

No public API, storage format, transaction semantics, redo format, or recovery
contract changes are expected.

## Test Cases

1. Run formatting:

   ```bash
   cargo fmt --all
   ```

2. Run linting:

   ```bash
   cargo clippy -p doradb-storage --all-targets -- -D warnings
   ```

3. Run routine storage validation:

   ```bash
   cargo nextest run -p doradb-storage
   ```

4. Run alternate backend validation if implementation touches backend-neutral
   I/O worker logic beyond adding ordinary log calls:

   ```bash
   cargo nextest run -p doradb-storage --no-default-features --features libaio
   ```

5. Verify production stderr removal:

   ```bash
   rg -n "eprintln!" doradb-storage/src
   ```

   Expected result: no matches.

6. Verify facade discipline:

   ```bash
   rg -n "::log::|log::(debug|error|info|log_enabled|trace|warn)!" doradb-storage/src
   ```

   Expected result: no direct log macro call sites outside
   `doradb-storage/src/obs.rs`. Review any matches manually because `crate::log`
   is also the redo-log module.

7. Review disabled-log performance manually:
   - no new `format!`/`to_string`/collection scans only to build disabled log
     messages;
   - expensive debug/trace summaries are guarded with `obs::log_enabled!`;
   - no new info/warn/error logging in normal per-row/per-key/per-page hot
     loops.

Unit tests that assert captured log output are not required for this task.
Global logger installation is process-wide and can make parallel tests brittle.
Compile, lint, existing behavior tests, grep checks, and review against
`docs/observability-logging.md` are the required validation.

## Open Questions

No blocking design questions remain.

Future follow-ups may consider:

- public application examples that install a logger and show emitted
  observability logs;
- migration to `tracing` spans if the engine later needs structured nested
  operation traces;
- a more complete event catalog after the initial low-frequency boundaries have
  proven useful;
- mechanical style tooling for observability log field shape if review alone
  becomes too error-prone.
