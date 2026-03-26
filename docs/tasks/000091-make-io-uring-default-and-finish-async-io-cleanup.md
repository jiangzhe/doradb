---
id: 000091
title: Make io_uring Default and Finish Async IO Cleanup
status: implemented  # proposal | implemented | superseded
created: 2026-03-25
github_issue: 480
---

# Task: Make io_uring Default and Finish Async IO Cleanup

## Summary

Implement RFC-0010 phase 6 by switching the default compile-time storage
backend to `io_uring`, keeping `libaio` as an explicitly supported alternate
backend for older Linux kernels, and finishing the remaining async-I/O cleanup
that was intentionally deferred after the backend-neutral completion core and
phase-5 backend delivery landed.

This task includes four concrete cleanup areas that now sit on the critical
path for a clean phase-6 finish:

1. make default build/test/CI/documentation paths mean `io_uring`;
2. move the remaining libaio-only compatibility surface out of
   `doradb-storage/src/io/mod.rs` and into `doradb-storage/src/io/libaio_backend.rs`;
3. finish redo/log-path cleanup by wiring backend-neutral worker stats,
   validating fatal redo failure behavior on both backends, and removing stale
   legacy wording; and
4. add `docs/async-io.md` as the repository-level design and operational
   reference for the completion core, both supported backends, and their
   storage-engine integration points.

## Context

RFC-0010 phase 5 already landed the backend-neutral `StorageBackend` alias,
`IouringBackend`, backend-neutral test hooks, and production startup wiring for
table-file, buffer-pool, transaction-config, and redo-log paths while keeping
`libaio` as the default backend. Phase 6 is therefore not blocked on a missing
`io_uring` implementation; it is blocked on final support-policy switching,
cleanup, and documentation.

The current repository state still has three inconsistencies:

1. `doradb-storage/Cargo.toml` keeps `libaio` as the default feature even
   though phase 6 is the planned default-switch point.
2. `docs/process/unit-test.md`, `docs/process/coding-guidance.md`,
   `.github/workflows/build.yml`, and `tools/coverage_focus.rs` still describe
   the repo-default validation path as `libaio`.
3. `doradb-storage/src/io/mod.rs` still contains libaio-only compatibility
   types and helper functions (`IocbRawPtr`, `AIO<T>`, `UnsafeAIO`, and raw
   libaio `pread` / `pwrite` helpers) even though generic engine code no
   longer needs them.

The redo-log path itself is already migrated onto the backend-neutral
completion core. `doradb-storage/src/trx/log.rs` constructs `StorageBackend`,
submits backend-neutral `Operation::pwrite_owned(...)` requests through
`AIOClient` / `IOWorker`, and classifies fatal submit/write/sync failures via
`StoragePoisonSource::{RedoSubmit, RedoWrite, RedoSync}`. That means the
remaining redo work is cleanup and verification, not another redesign.

One user constraint sharpens the support policy for this phase:
`libaio` must remain an explicitly supported alternate backend because some
older Linux kernels cannot use `io_uring`. This task therefore does not retire
`libaio`; it establishes `io_uring` as the repository default while keeping
`libaio` as an intentional legacy-kernel escape hatch with explicit validation
coverage.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Parent RFC:`
`- docs/rfcs/0010-retire-thread-pool-async-io-and-introduce-backend-neutral-completion-core.md`

## Goals

1. Switch the default compile-time storage backend from `libaio` to
   `io_uring`.
2. Keep `libaio` as an explicitly supported alternate backend for
   legacy-kernel environments.
3. Update repo-default validation, CI, and focused coverage so default
   commands exercise the default `io_uring` path.
4. Move the remaining libaio-only compatibility types and helper functions out
   of `doradb-storage/src/io/mod.rs` and confine them to
   `doradb-storage/src/io/libaio_backend.rs`.
5. Keep `AIOBuf` and `DirectBuf` in the generic shared layer; do not force a
   larger async-buffer API redesign as part of this cleanup.
6. Finish redo/log-module cleanup by:
   - keeping current group-commit ordering and durability semantics;
   - wiring backend-neutral worker stats into redo stats;
   - validating fatal redo failure behavior on both supported backends; and
   - removing stale libaio-specific wording from log/backend comments.
7. Add `docs/async-io.md` documenting the completion core, backend contract,
   compile-time backend selection, storage integration points, and explicit
   `libaio` legacy-kernel support policy.
8. Prove a manual initial performance bar that is not worse than `libaio`
   using existing example commands instead of introducing a new benchmark
   harness or CI perf gate.

## Non-Goals

1. Runtime backend switching.
2. Removing `libaio` support entirely.
3. Moving `AIOBuf` or `DirectBuf` out of the shared generic buffer layer.
4. Redesigning the completion core, transaction commit protocol, redo on-disk
   format, checkpoint semantics, or recovery semantics.
5. Reintroducing merged multi-backend coverage artifacts or restoring the old
   dual-branch coverage workflow.
6. Broad post-adoption `io_uring` tuning beyond the manual phase-6 acceptance
   bar.
7. Any broader reorganization of storage-engine modules unrelated to async-I/O
   defaulting and cleanup.

## Unsafe Considerations (If Applicable)

This task touches unsafe-sensitive async-I/O code.

1. Affected modules and why `unsafe` remains relevant:
   - `doradb-storage/src/io/mod.rs`
     currently still hosts libaio-only compatibility types that wrap raw
     `iocb` pointers and borrowed raw page pointers behind feature gates;
   - `doradb-storage/src/io/libaio_backend.rs`
     will absorb those remaining libaio-only compatibility definitions so raw
     `iocb` layout stays backend-private;
   - `doradb-storage/src/io/iouring_backend.rs`
     remains the default backend after this task and still depends on the same
     completion-driven in-flight ownership invariants;
   - `doradb-storage/src/trx/log.rs`
     keeps backend-neutral redo write submissions alive until worker-observed
     completion and then serializes `fsync` / `fdatasync` ordering above the
     worker layer.
2. Required invariants and `// SAFETY:` expectations:
   - owned direct buffers and borrowed page pointers must remain valid until
     backend completion is observed exactly once;
   - moving libaio-only compatibility code out of `io/mod.rs` must not widen
     its visibility or let generic code depend on raw `iocb` layout again;
   - fatal redo write/sync failure paths must continue to poison runtime
     admission only after worker-owned or scheduler-owned buffers are retained
     long enough for correct cleanup;
   - any newly moved or rewritten unsafe block must keep a local `// SAFETY:`
     comment phrased in terms of queue submission, completion ownership, and
     teardown ordering.
3. Inventory refresh and validation scope:

```bash
tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md
cargo clippy -p doradb-storage --all-targets -- -D warnings
cargo nextest run -p doradb-storage
cargo clippy -p doradb-storage --no-default-features --features libaio --all-targets -- -D warnings
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Switch repository defaults to `io_uring`.
   - Change `doradb-storage/Cargo.toml` default features from `libaio` to
     `iouring`.
   - Preserve compile-time mutual exclusion for `libaio` and `iouring`.
   - Update active process docs so:
     - `cargo nextest run -p doradb-storage` means the supported default
       `io_uring` path;
     - `cargo nextest run -p doradb-storage --no-default-features --features libaio`
       becomes the explicit alternate-backend validation command for
       legacy-kernel support and backend-sensitive changes.

2. Update CI and local coverage tooling for the new support policy.
   - Change `.github/workflows/build.yml` so default clippy / nextest /
     coverage runs target the default `io_uring` backend.
   - Keep one explicit `libaio` validation step in CI without duplicating
     coverage generation.
   - Update `tools/coverage_focus.rs` wording and command assumptions so
     focused coverage documents the default backend correctly and no longer
     describes `libaio` as the supported default path.

3. Add repository-level async-I/O documentation.
   - Create `docs/async-io.md` with:
     - completion core and worker ownership model;
     - backend contract (`IOBackend`, staged submission, completion tokens);
     - compile-time backend selection and support policy;
     - storage integration points for table-file, buffer-pool, and redo-log
       flows;
     - operational note that `libaio` remains the supported fallback for
       older kernels where `io_uring` is unavailable.

4. Extract libaio-only compatibility code out of `io/mod.rs`.
   - Move the following libaio-only items into
     `doradb-storage/src/io/libaio_backend.rs`:
     - `IocbRawPtr`
     - `AIO<T>`
     - `UnsafeAIO`
     - libaio-only `pread(...)` and `pwrite(...)` helpers
   - Adjust `crate::io` exports so generic code still exposes only the
     backend-neutral layer by default, while libaio-specific compatibility
     items remain available only behind the libaio feature path.
   - Keep `AIOBuf` and `DirectBuf` in shared generic code because they are
     still used outside libaio-specific paths by replay and table/catalog file
     code.

5. Finish redo/log cleanup and observability.
   - Keep `doradb-storage/src/trx/log.rs` on the existing
     `StorageBackend` + `Operation::pwrite_owned(...)` path.
   - Change `LogIOStateMachine::on_stats(...)` to accumulate worker
     `io_submit_*` and `io_wait_*` counters into `LogPartitionStats` so
     transaction-system stats and benchmark/example output remain meaningful.
   - Remove stale wording that still describes the redo path as relying on raw
     libaio submit helpers or an “AIO manager”.

6. Add backend-neutral redo failure coverage.
   - Reuse `StorageBackendTestHook` to inject redo write-completion failures
     independent of backend ABI details.
   - Add a narrow test-only hook around `FileSyncer` so redo sync failure can
     be injected without changing production durability semantics.
   - Cover:
     - redo write failure poisons runtime and fails queued/inflight waiters;
     - redo `fsync` / `fdatasync` failure poisons runtime and fails waiters;
     - redo no-wait/system transaction path still returns CTS correctly;
     - the same targeted tests run under both default `io_uring` and explicit
       `libaio` builds.

7. Capture the phase-6 manual performance bar.
   - Use existing examples instead of creating a new benchmark harness:

```bash
cargo run -p doradb-storage --example bench_readonly_buffer_pool -- --pages 8192 --warm-reads 100000 --cache-bytes 268435456
cargo run -p doradb-storage --example multi_threaded_trx -- --threads 1 --sessions 1 --duration 10s
```

   - Compare default `io_uring` against explicit
     `--no-default-features --features libaio` on the same machine using
     median-of-3 runs.
   - Treat within-5% deltas as parity/noise for the initial acceptance bar.

## Implementation Notes

1. Switched the repository-default storage backend to `io_uring` while keeping
   `libaio` as the explicit legacy-kernel alternate path.
   - `doradb-storage/Cargo.toml` now uses `default = ["iouring"]`.
   - `crate::io` still enforces compile-time mutual exclusion between
     `iouring` and `libaio`.
2. Updated repository process docs, CI, and local tooling to match the new
   support policy.
   - `.github/workflows/build.yml`, `docs/process/unit-test.md`,
     `docs/process/coding-guidance.md`, and `tools/coverage_focus.rs` now
     treat the default validation path as `io_uring` and keep one explicit
     `libaio` validation path.
   - Added `docs/async-io.md` as the repository-level design and operational
     reference for the completion core, backend contract, and storage-engine
     integration points.
3. Finished the remaining libaio-only compatibility cleanup in the shared
   async-I/O layer.
   - Moved `AIO<T>`, `UnsafeAIO`, `IocbRawPtr`, and libaio-only `pread` /
     `pwrite` helpers into `doradb-storage/src/io/libaio_backend.rs`.
   - Kept `AIOBuf` and `DirectBuf` in the shared generic layer.
   - Review follow-up also moved libaio-only tests and their helper
     state-machine types out of `doradb-storage/src/io/mod.rs` and into
     `doradb-storage/src/io/libaio_backend.rs` so `io/mod.rs` keeps only
     backend-neutral tests and hook exports.
4. Completed redo/log cleanup without changing commit ordering or durability
   semantics.
   - `LogIOStateMachine::on_stats(...)` now merges worker `io_submit_*` and
     `io_wait_*` counters into `LogPartitionStats`.
   - Added a narrow test-only `FileSyncer` hook so redo `fsync` /
     `fdatasync` failure paths can be covered without changing production
     behavior.
   - Removed stale “AIO manager” and libaio-specific wording from redo-related
     comments and config/docs.
5. Added backend-neutral redo failure coverage and kept review-driven readonly
   cleanup test-only.
   - Redo write, `fsync`, and `fdatasync` failure paths are covered under both
     the default `io_uring` build and the explicit alternate `libaio` build.
   - `commit_no_wait` / system-transaction CTS behavior is covered.
   - Review follow-up found readonly shared-failure tests were assuming
     synchronous inflight-map cleanup immediately after
     `Completion::wait_result()`. Production ordering was left unchanged; the
     tests were updated to wait for eventual inflight cleanup instead.
6. Recorded the manual benchmark outcome on 2026-03-26.
   - `multi_threaded_trx` median: `io_uring` `16910 trx/s` vs `libaio`
     `16625 trx/s` (`+1.7%`).
   - `bench_readonly_buffer_pool` median: `io_uring` was slower than `libaio`
     by `48.5%` on cold reads and `36.9%` on the benchmark's current “warm”
     phase on this machine.
   - The readonly benchmark/perf gap was not folded into this landing.
     Follow-up backlog
     `docs/backlogs/000070-correct-readonly-buffer-pool-warm-benchmark-and-investigate-iouring-cold-read-latency.md`
     tracks correcting the benchmark methodology and investigating the
     remaining cold-read latency gap.
7. Validation completed for the implemented state.
   - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
     - result: passed
   - `cargo clippy -p doradb-storage --no-default-features --features libaio --all-targets -- -D warnings`
     - result: passed
   - `cargo nextest run -p doradb-storage`
     - result: passed
   - `cargo nextest run -p doradb-storage --no-default-features --features libaio`
     - result: passed
   - targeted follow-up checks:
     - `cargo test -p doradb-storage buffer::readonly::tests::test_readonly_pool_shared_io_failure_propagates_to_all_waiters -- --exact --nocapture`
       - result: passed
     - `cargo test -p doradb-storage buffer::readonly::tests::test_readonly_pool_shared_validated_load_propagates_validation_failure -- --exact --nocapture`
       - result: passed
     - `cargo test -p doradb-storage buffer::readonly::tests::test_readonly_pool_cancelled_loader_keeps_shared_miss_attempt_alive -- --exact --nocapture`
       - result: passed
     - `cargo test -p doradb-storage --no-default-features --features libaio io::libaio_backend::tests -- --nocapture`
       - result: passed
8. Tracking and review state at resolve time.
   - task issue: `#480`
   - implementation PR: `#481`

## Impacts

1. Backend feature contract and repository default:
   - `doradb-storage/Cargo.toml`
2. Shared async-I/O layer and libaio-specific compatibility extraction:
   - `doradb-storage/src/io/mod.rs`
   - `doradb-storage/src/io/libaio_backend.rs`
   - `doradb-storage/src/io/iouring_backend.rs`
   - `doradb-storage/src/io/buf.rs`
3. Redo group commit and fatal durability handling:
   - `doradb-storage/src/trx/log.rs`
   - `doradb-storage/src/trx/group.rs`
   - `doradb-storage/src/trx/sys.rs`
4. File sync hook surface for redo sync-failure tests:
   - `doradb-storage/src/file/mod.rs`
5. Validation/tooling/doc synchronization:
   - `.github/workflows/build.yml`
   - `tools/coverage_focus.rs`
   - `docs/process/unit-test.md`
   - `docs/process/coding-guidance.md`
   - `docs/async-io.md`

## Test Cases

1. Default-backend repository validation.
   - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
   - `cargo nextest run -p doradb-storage`
2. Explicit alternate-backend validation for supported legacy-kernel path.
   - `cargo clippy -p doradb-storage --no-default-features --features libaio --all-targets -- -D warnings`
   - `cargo nextest run -p doradb-storage --no-default-features --features libaio`
3. Redo no-wait/system transaction path.
   - `commit_no_wait` continues to return CTS correctly on both supported
     backends.
4. Fatal redo write completion failure.
   - Inject backend-neutral write failure.
   - Verify queued/inflight waiters fail.
   - Verify runtime poison is recorded as `RedoWrite`.
5. Fatal redo sync failure.
   - Inject `fsync` / `fdatasync` failure through a narrow test-only sync hook.
   - Verify queued/inflight waiters fail.
   - Verify runtime poison is recorded as `RedoSync`.
6. Redo stats wiring.
   - Exercise redo submission/wait path and verify `io_submit_*` /
     `io_wait_*` stats are no longer silently zero due to dropped worker
     stats.
7. Libaio compatibility extraction acceptance.
   - No production redo or generic async-I/O path depends on libaio-only
     compatibility items remaining defined in `doradb-storage/src/io/mod.rs`.
8. Manual performance acceptance.
   - Run the documented readonly and transaction example commands under
     default `io_uring` and explicit `libaio`.
   - Confirm the phase-6 floor is not worse than `libaio` under the agreed
     median-of-3 comparison.

## Open Questions

1. Readonly benchmark methodology cleanup and the remaining `io_uring`
   cold-read latency investigation are deferred to
   `docs/backlogs/000070-correct-readonly-buffer-pool-warm-benchmark-and-investigate-iouring-cold-read-latency.md`.
2. After the default switch stabilizes, should a later follow-up retire the
   remaining backend-private raw libaio compatibility helpers entirely, or are
   they expected to remain indefinitely as part of long-term legacy-kernel
   support?
3. How broad should routine CI coverage for the explicit alternate `libaio`
   backend become after the default switch is complete: targeted
   backend-sensitive validation only, or broader parity with the default
   branch?
