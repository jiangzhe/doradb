---
id: 000213
title: Handle IO Backend Error Failures
status: implemented  # proposal | implemented | superseded
created: 2026-07-05
github_issue: 819
---

# Task: Handle IO Backend Error Failures

## Summary

Make storage backend submit/wait failures explicit typed errors instead of
production panics. Preserve the existing handling for transient syscall
pressure and per-operation completion errors, keep IO ownership invariants as
panic boundaries, and route unrecoverable backend-progress failures through a
new first-class `EnginePoisoner` component so the whole engine can be poisoned
without embedding poison ownership in `TransactionSystem`.

## Context

Issue Labels:
- type:task
- priority:medium
- codex

Source Backlogs:
- docs/backlogs/closed/000133-return-io-backend-submit-wait-errors-instead-of-panicking.md

Related Context:
- docs/architecture.md
- docs/async-io.md
- docs/engine-component-lifetime.md
- docs/redo-log.md
- docs/table-file.md
- docs/checkpoint-and-recovery.md
- docs/process/unit-test.md
- docs/process/coding-guidance.md

The async IO layer has two compile-time backends: default `io_uring` and
alternate `libaio`. The backend-neutral `IOBackend` trait currently models
`submit_batch` and `wait_at_least` as infallible. That forces unexpected
backend syscall errors into `panic!` paths in both backend implementations.
Known transient syscall conditions already have local policy: `EINTR` is
retried, while `io_uring` submit pressure from `EAGAIN` or `EBUSY` and
`libaio` `io_submit` `EAGAIN` are reported as explicit `SubmitAttempt::Retry`
outcomes. Schedulers wait on already-accepted work when possible and otherwise
use bounded no-progress backoff before retrying staged submissions.

Per-operation completion errors already have usable paths. Negative completion
results become `std::io::Error`; table-file, readonly-cache, evictable-pool,
redo, and recovery code then map those completions to operation errors,
completion reports, or fatal redo poison. The missing path is a backend-level
submit or wait syscall failure before a normal completion exists.

The current storage poison state lives inside `TransactionSystem`, but backend
IO workers are lower-level engine components. The approved design is to add a
new first component named `EnginePoisoner`, expose it through
`QuiescentGuard<EnginePoisoner>`, and inject that guard only into components
that need poison authority or poison listener checks. `TransactionSystem` may
keep temporary delegating helpers to reduce migration churn, but it must stop
owning the poison state.

## Goals

- Add `EnginePoisoner` as the first registered engine component and make it the
  single owner of runtime poison state.
- Preserve existing poison semantics: first fatal reason wins, the reason is
  recorded before the poisoned flag is published, poison listeners wake once,
  admission checks fail after poison, and repeated poison attempts do not
  overwrite the original fatal reason.
- Store enough first-poison context to return and log clear messages, including
  backend name, syscall phase, errno, operation kind when known, and queue
  state for backend-progress failures.
- Make `IOBackend::submit_batch` and `IOBackend::wait_at_least` fallible with a
  backend-specific typed error instead of panicking on non-transient syscall
  failures.
- Preserve current transient handling for `EINTR`, `EAGAIN`, and `EBUSY`:
  retry interrupted backend syscalls immediately, and surface submit pressure
  as explicit retry outcomes rather than backend-progress failures.
- Treat unsupported native async sync, such as `libaio` sync opcode rejection
  on unsupported kernels, as a known backend error with a clear message.
- Keep IO invariants as explicit panics: invalid or stale completion tokens,
  more completions than submitted work, impossible zero-completion blocking
  waits, missing inflight entries, wrong pool state, and invalid redo prefix
  ownership.
- Propagate worker panics through existing join paths instead of swallowing or
  converting them to ordinary IO errors.
- Poison the engine on unrecoverable runtime backend-progress failure in shared
  storage IO and redo IO.
- Keep startup recovery read-ahead failure as a recovery `Error` item/result,
  not runtime engine poison.
- Avoid bespoke buffer disposal when an existing safe drain path can be reused.
  When submitted IO cannot be safely drained, do not drop memory or page guards
  until backend teardown is proven not to access them.
- Validate both default `io_uring` and alternate `libaio` builds.

## Non-Goals

- Do not redesign redo group commit, table-file publication, recovery replay,
  checkpoint algorithms, or file formats.
- Do not add a broad background-worker supervisor beyond the `EnginePoisoner`
  component and the IO failure paths needed by this task.
- Do not add general retry/backoff policy for permanent IO errors.
- Do not merge redo IO into the shared `StorageIOWorker`.
- Do not change public storage API error types except as required to preserve
  clearer fatal or IO reports at existing boundaries.
- Do not make lower-level filesystem or buffer components depend on
  `TransactionSystem` for poison authority.
- Do not suppress assertion/panic paths that represent internal invariants.

## Plan

1. Add the `EnginePoisoner` component.
   - Add a crate-private component, for example in
     `doradb-storage/src/poison.rs`.
   - Implement `Component` with `Config = ()`,
     `Owned = EnginePoisoner`, and
     `Access = QuiescentGuard<EnginePoisoner>`.
   - Register it first in `EngineConfig::build_inner`, before `FileSystem`.
   - Add it to the documented component order in
     `docs/engine-component-lifetime.md` and the registry comments in
     `doradb-storage/src/component.rs`.
   - Store a `QuiescentGuard<EnginePoisoner>` in `EngineInner`.
   - Change `EngineInner::acquire_admission` to call
     `poisoner.ensure_healthy()` instead of
     `trx_sys.ensure_runtime_healthy()`.

2. Move poison state out of `TransactionSystem`.
   - Move `storage_poisoned`, `storage_poison_err`, and
     `storage_poison_event` behavior into `EnginePoisoner`.
   - Store a first-poison record containing at least:
     - `FatalError` reason;
     - a short component/source string;
     - a detailed context string suitable for report attachments and logs.
   - Expose methods equivalent to:
     - `poison(reason, context) -> Report<FatalError>`;
     - `poison_error() -> Option<Report<FatalError>>`;
     - `ensure_healthy() -> FatalResult<()>`;
     - `listener() -> EventListener`.
   - Keep first-writer-wins ordering: store the poison record under lock before
     publishing the atomic poisoned flag.
   - Keep structured logging in the first poison path. Repeated poison calls
     may log at debug level with both attempted and already-published reasons.
   - Inject `QuiescentGuard<EnginePoisoner>` only where needed:
     - `TransactionSystem`, so existing redo, checkpoint, purge, rollback, and
       transaction cleanup fatal paths can delegate;
     - `FileSystemWorkers`, so shared backend-progress failure can poison
       without depending on `TransactionSystem`.
   - Keep delegating `TransactionSystem::{poison_engine,
     poison_error, ensure_runtime_healthy, poison_listener}`
     temporarily if that keeps call-site churn contained. These helpers must
     call `EnginePoisoner` and must not own independent state.

3. Add IO backend progress error types.
   - In `doradb-storage/src/io/backend.rs`, add a backend result type and a
     typed backend error such as `IOBackendError`.
   - The error should carry:
     - backend name (`io_uring` or `libaio`);
     - phase (`Submit`, `BlockingSubmit`, `Wait`);
     - underlying `std::io::Error`;
     - raw errno when available;
     - staged/pending/submitted/completion counts relevant to the failure;
     - operation kind or first affected operation when known.
   - Add helper constructors so backend implementations attach consistent
     context and so callers can convert to `CompletionErrorKind::Io`, public
     `ErrorKind::Io`, or fatal poison context.
   - Do not use this error type for completion-token validation or scheduler
     consistency violations; those remain assertions or panics.

4. Make `IOBackend` submit/wait fallible.
   - Change the trait to:
     - `submit_batch(...) -> BackendResult<SubmitAttempt>`;
     - `wait_at_least(...) -> BackendResult<Vec<(BackendToken,
       StdIoResult<usize>)>>`.
   - Update `SubmissionDriver::submit_ready` and
     `SubmissionDriver::wait_at_least_one` to return backend results.
   - Keep `try_pop_completed` infallible because it only returns already-fetched
     completions.
   - Preserve capacity accounting and completion buffering semantics exactly
     when backend calls succeed.

5. Update `io_uring` backend behavior.
   - Make `submit_pending_sqes` and `blocking_submit_and_wait` return backend
     results.
   - Preserve:
     - retry on `EINTR`;
     - `SubmitAttempt::Retry` on `EAGAIN` or `EBUSY`, including backend name,
       retry reason, and submit call count;
     - partial-submit suffix retention;
     - assertions for impossible accepted counts.
   - Keep blocking submit-and-wait as the wait-path primitive; submit pressure
     must leave staged work queued for scheduler retry.
   - Replace unexpected submit, blocking-submit, and wait panics with
     `IOBackendError` carrying current pending SQEs, limit, and staged length.
   - Continue recording submit/wait stats for attempted calls that return a
     typed error.

6. Update `libaio` backend behavior.
   - Make `submit_limit` and `wait_at_least_with_attempts` return backend
     results.
   - Preserve:
     - `SubmitAttempt::Retry` on `io_submit` `EAGAIN`, including backend name,
       retry reason, and submit call count;
     - `io_getevents` retry on `EINTR`;
     - assertion that blocking wait with `min_nr = 1` must not return zero.
   - Replace other negative `io_submit` and `io_getevents` results with
     `IOBackendError`.
   - Ensure sync-op rejection such as `EINVAL` includes a clear message that
     native libaio `IO_CMD_FSYNC` / `IO_CMD_FDSYNC` may be unsupported by the
     running kernel.
   - Keep `io_destroy` cleanup assertions unless implementation must change
     them to preserve submitted-buffer lifetime during fatal teardown.

7. Add safe failure cleanup for not-yet-submitted work.
   - Add an explicit failure path for each submission family that can be staged
     but not yet backend-accepted:
     - table write/sync/read submissions complete their completion cell with a
       backend-progress error and release write leases where applicable;
     - readonly miss loads use the existing `ReadSubmission::fail`;
     - evictable-pool reads use the existing `EvictReadSubmission::fail`;
     - evictable-pool writebacks reuse `InflightIO::fail_writeback`;
     - redo submissions route into prefix failure cleanup or header completion
       failure as appropriate;
     - recovery read-ahead returns owned buffers to the existing recycle/free
       path when no backend accepted the submission.
   - Do not rely on `Drop` producing `CompletionDropped` for planned backend
     failure when a clearer backend error can be delivered.

8. Preserve submitted-buffer and borrowed-page lifetime on wait failure.
   - For backend wait errors after operations were accepted, poison the engine
     immediately and stop admitting new worker work.
   - Reuse existing drain logic where it can still safely obtain completions.
     Recovery already has `drain_driver`; redo and shared storage should prefer
     analogous draining over ad hoc buffer disposal.
   - If the backend can no longer wait for completions, implementation must
     prove backend teardown prevents further kernel access before dropping
     submitted owned buffers or borrowed page guards.
   - If that proof is not available for one backend, retain the submitted
     inflight entries after poison as a deliberate fatal quarantine rather than
     risking use-after-free. Document that choice in implementation notes during
     task resolve.

9. Update shared `StorageIOWorker`.
   - Inject `QuiescentGuard<EnginePoisoner>` through the `FileSystemWorkers`
     build path and into `StorageIOWorker`.
   - On backend submit failure:
     - log backend name, phase, errno, queue depth, staged/submitted counts, and
       first affected operation kind;
     - poison with a storage-IO fatal reason;
     - fail queued and staged not-yet-submitted work with clear completion
       errors;
     - stop the worker after safe cleanup/drain handling.
   - On backend wait failure:
     - poison first;
     - drain safely when possible;
     - otherwise retain submitted entries as described above.
   - Add a `FatalError::StorageIo` or equivalent fatal reason if the current
     fatal enum cannot accurately represent shared backend-progress failure.
   - Keep invalid completion-token and state-machine mismatches as panics that
     propagate through the existing `FileSystemWorkers::shutdown` join path.

10. Update redo backend failure routing.
    - `LogWriteDriver::submit_ready` and `wait_at_least_one` should surface
      backend errors to `RedoLogWriter`.
    - Submit/wait backend progress failure must enter the same ordered fatal
      cleanup path used for redo completion failures:
      - poison through `EnginePoisoner`;
      - fail the current prefix and pending precommit groups;
      - enqueue required failed-precommit rollback cleanup before waiters are
        completed.
    - Use `FatalError::RedoSync` when the failed progress boundary is a sync
      submission or a sync-only wait; use `FatalError::RedoWrite` for write
      submissions and ambiguous mixed write progress failures.
    - Clean-shutdown active-file seal remains best-effort. A backend
      submit/wait failure there increments the seal failure stat and does not
      poison storage solely because shutdown sealing failed.
    - Keep redo prefix ownership and completion-kind mismatches as invariant
      panics.

11. Update recovery read-ahead.
    - Convert backend submit/wait errors in
      `doradb-storage/src/recovery/stream.rs` into recovery `Error`s sent as
      `RedoReadItem::Error`.
    - Do not poison `EnginePoisoner` during startup recovery read-ahead.
    - Reuse `drain_driver` where possible before returning after a semantic
      read error or stop request.
    - If a backend wait failure prevents draining accepted reads, retain/drop
      only according to the same backend-lifetime proof required for runtime
      workers.

12. Update docs and comments.
    - Update `docs/async-io.md` with the fallible backend submit/wait contract
      and backend-progress failure policy.
    - Update `docs/engine-component-lifetime.md` to show `EnginePoisoner` as
      the first component and explain that poison is engine-level admission
      state, not transaction-system-owned state.
    - Update `docs/redo-log.md` if the redo submit/wait fatal policy gains
      meaningful new wording.
    - Keep comments near invariant panics explicit so future readers do not
      convert them into ordinary IO errors.

## Implementation Notes

- Added `EnginePoisoner` as the engine-level poison owner and registered it
  ahead of lower-level runtime components. Transaction-system poison helpers now
  delegate to that component, and engine admission checks read engine poison
  state directly.
- Made backend submit/wait progress fallible through typed backend failure
  reports carrying backend name, phase, raw errno, queue state, call counts, and
  operation kind when known. `Completion` and public storage errors preserve
  backend failure attachments across waiter propagation.
- Preserved transient submit/wait policy while making no-progress submit
  pressure explicit. `EINTR` remains retried, known submit pressure remains a
  bounded retry path, and both `io_uring` SQ-full/no-accepted work and libaio
  zero-submit behavior now report unified `SubmitRetryReason::NoProgress`
  instead of silently stalling as idle `Noop`.
- Added submitted-IO cleanup contracts and quarantine handling. Runtime drivers
  now fail or wake waiters on backend progress failure, call backend
  `cleanup_submitted_io`, retain submitted entries until backend teardown, and
  intentionally leak memory-bound submitted entries only when a backend cannot
  prove user memory is safe to drop after best-effort cleanup. `io_uring`
  attempts synchronous cancellation for submitted operations; libaio relies on
  backend teardown before retained entries drop.
- Updated shared storage IO, redo writer, and recovery read-ahead failure
  routing. Shared storage backend progress failure poisons through
  `EnginePoisoner`, fails queued/staged work, completes submitted waiters, and
  quarantines submitted entries. Redo uses the existing fatal cleanup path and
  submitted-driver cleanup. Recovery read-ahead reports startup recovery errors
  without runtime poison and drains/cleans submitted driver state.
- Reused existing domain failure paths for table-file, readonly-cache,
  evictable-pool, redo, and recovery submissions instead of relying on
  `Drop`/`CompletionDropped` for planned backend-progress failure.
- Updated async IO, engine component lifetime, redo-log, and unsafe-usage docs
  to match the new backend-progress failure and engine-poison ownership model.
- Addressed review follow-ups found during implementation:
  - storage submitted buffers no longer rely on `mem::forget(self)` as the
    quarantine mechanism;
  - submitted buffer cleanup policy is driven by `SubmittedIoCleanup`;
  - `io_uring` SQ-full and libaio zero-submit paths use the bounded
    `NoProgress` retry/progress-error path;
  - `SubmittedStorageIoQuarantine` uses `mem::take`;
  - focused coverage was added for `evict.rs` and `fs.rs` backend-failure
    cleanup paths.
- Validation completed:
  - `tools/style_audit.rs --diff-base origin/main`;
  - `tools/style_audit.rs`;
  - `tools/coverage_focus.rs --path doradb-storage/src/buffer/evict.rs --path
    doradb-storage/src/file/fs.rs` (`evict.rs` 93.34%, `fs.rs` 90.68%);
  - `cargo clippy -p doradb-storage --all-targets -- -D warnings`;
  - `cargo nextest run -p doradb-storage` (`1213` passed);
  - targeted default-backend tests for `evictable_pool_backend_failure`,
    `backend_progress_failure`, and `no_progress`;
  - targeted libaio-feature tests for `evictable_pool_backend_failure`,
    `backend_progress_failure`, and `no_progress`;
  - full libaio-feature validation was run before the final coverage-only test
    additions and passed.

## Impacts

- `doradb-storage/src/poison.rs` or equivalent new module:
  `EnginePoisoner` component and poison API.
- `doradb-storage/src/lib.rs`: register the new module.
- `doradb-storage/src/component.rs`: component-order documentation.
- `doradb-storage/src/engine.rs`: first component registration, `EngineInner`
  guard storage, admission poison check.
- `doradb-storage/src/error.rs`: possible new `FatalError::StorageIo` or
  equivalent fatal reason for shared backend-progress failure.
- `doradb-storage/src/trx/sys.rs`, `trx/purge.rs`, `trx/retention.rs`,
  `trx/stmt.rs`, `trx/mod.rs`: delegate poison calls/listeners to
  `EnginePoisoner`.
- `doradb-storage/src/catalog/*.rs`, `src/table/*.rs`, and `src/index/*.rs`:
  update direct `trx_sys` poison checks only where delegation is not kept.
- `doradb-storage/src/io/backend.rs`: fallible backend contract and error type.
- `doradb-storage/src/io/mod.rs`: `SubmissionDriver` fallible submit/wait and
  staged failure support.
- `doradb-storage/src/io/iouring_backend.rs`: return typed submit/wait errors.
- `doradb-storage/src/io/libaio_backend.rs`: return typed submit/wait errors.
- `doradb-storage/src/file/fs.rs`: inject `EnginePoisoner`, handle shared
  worker backend-progress failure, fail not-yet-submitted work safely.
- `doradb-storage/src/file/mod.rs`: table submission pre-submit failure helper.
- `doradb-storage/src/buffer/readonly.rs`: reuse readonly failure path for
  backend-progress failure.
- `doradb-storage/src/buffer/evict.rs`: reuse evict read/write failure paths
  for backend-progress failure.
- `doradb-storage/src/log/mod.rs` and `src/log/seal.rs`: redo backend-progress
  fatal cleanup and clean-shutdown seal behavior.
- `doradb-storage/src/recovery/stream.rs`: recovery read-ahead backend error
  propagation.
- `docs/async-io.md`, `docs/engine-component-lifetime.md`, and possibly
  `docs/redo-log.md`: documented contract updates.

## Test Cases

- `EnginePoisoner` records the first fatal reason before publishing the poisoned
  flag, returns reports with stored context, wakes existing listeners, and keeps
  later poison attempts from overwriting the first record.
- Engine admission fails through `EnginePoisoner` after poison even when
  `TransactionSystem` only delegates.
- Existing transaction-system poison tests pass through the delegated
  `EnginePoisoner` helpers.
- `io_uring` submit helper retries `EINTR`, returns `SubmitAttempt::Retry` for
  `EAGAIN`/`EBUSY` with backend context, preserves staged SQEs for retry, and
  returns typed errors for unexpected submit, blocking-submit, and wait
  failures.
- `libaio` submit helper returns `SubmitAttempt::Retry` on `EAGAIN`, wait
  helper retries `EINTR`, and both return typed errors for non-transient
  syscall failures.
- Submit retry backoff defaults to 5 seconds, supports shorter low-level test
  configuration, and converts no-submitted retry pressure past the configured
  timeout into a backend progress error.
- `libaio` sync-op rejection reports a clear unsupported-native-sync context.
- `SubmissionDriver` with a fake backend returns submit/wait errors without
  corrupting staged, submitted, or completed counters.
- Shared `StorageIOWorker` with a fake failing backend poisons through
  `EnginePoisoner`, logs context, and completes not-yet-submitted table,
  readonly, and evictable-pool waiters with clear errors.
- Shared `StorageIOWorker` invariant tests still panic for invalid completion
  tokens or impossible completion counts.
- Redo submit failure before a write is accepted poisons as `RedoWrite` and
  fails precommit waiters through existing cleanup.
- Redo submit/wait failure for sync poisons as `RedoSync` and does not publish
  the ordered prefix.
- Clean-shutdown active-file seal submit/wait failure increments seal failure
  stats and does not poison solely for that best-effort seal.
- Recovery read-ahead backend submit/wait failure produces `RedoReadItem::Error`
  or an equivalent recovery `Error` without poisoning runtime admission.
- Routine validation:

```bash
cargo nextest run -p doradb-storage
```

- Alternate backend validation:

```bash
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

## Open Questions

None.
