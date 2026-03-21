---
id: 000083
title: Retire Thread-Pool Async IO Fallback And No-Default Validation
status: implemented  # proposal | implemented | superseded
created: 2026-03-21
github_issue: 462
---

# Task: Retire Thread-Pool Async IO Fallback And No-Default Validation

## Summary

Implement Phase 1 of RFC-0010 by removing the no-`libaio` thread-pool async-I/O
fallback from `doradb-storage`, retiring the repository-wide
`--no-default-features` validation and coverage contract, and updating active
tooling and contributor guidance so `libaio` is the only supported backend
until later RFC phases land.

## Context

RFC-0010 explicitly isolates this work as a narrow first phase before the
backend-neutral completion-core redesign and later `io_uring` adoption. The
task therefore needs to finish the current support-policy cutover without
pulling Phase 2 interface work into the same change.

The active repository contract still describes two supported async-I/O modes:

1. `doradb-storage/src/io/mod.rs` contains the current no-`libaio` fallback in
   `no_libaio`, a second `AIOContext::submit_limit()` implementation, a second
   `wait_at_least()` implementation, and a second `AIOEventLoop::run()`.
2. `doradb-storage/src/io/libaio_abi.rs` keeps fieldful no-`libaio` stub ABI
   structs only because the fallback decodes `iocb` fields directly at runtime.
3. `.github/workflows/build.yml`, `.config/nextest.toml`, and
   `tools/coverage_focus.rs` still run or merge a no-default-features branch.
4. Active contributor guidance in `docs/process/unit-test.md`,
   `docs/process/coding-guidance.md`, `docs/process/lint.md`,
   `docs/process/unsafe-review-checklist.md`, `docs/unsafe-usage-principles.md`,
   `AGENTS.md`, and `GEMINI.md` still instruct contributors to build or test
   without `libaio`.
5. Backlog `000003`, now archived at
   `docs/backlogs/closed/000003-improve-libaio-stub-struct-design.md`, was
   scoped around simplifying stub ABI support after fallback decoupling and
   needed reevaluation once the fallback was removed entirely.

The storage-file, buffer-pool, and redo-log consumers still use the existing
`iocb`-shaped API, but that is acceptable for this phase. This task should
leave those higher-level semantics intact and postpone generic submission API
changes to RFC-0010 Phase 2.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
Issue Labels:
- type:task
- priority:medium
- codex

Parent RFC:
- docs/rfcs/0010-retire-thread-pool-async-io-and-introduce-backend-neutral-completion-core.md

## Goals

1. Remove the no-`libaio` thread-pool fallback from `doradb-storage/src/io/`.
2. Remove the temporary `libaio` feature-gate contract from
   `doradb-storage/Cargo.toml` instead of keeping an unsupported shell.
3. Collapse CI, nextest configuration, and local coverage tooling to the single
   supported `libaio` branch.
4. Update active contributor and agent guidance so the repository no longer
   advertises `--no-default-features` as a supported validation path.
5. Keep the current `libaio`-shaped file, buffer-pool, and redo-log I/O
   consumer interfaces stable so this task remains strictly Phase 1.
6. Reevaluate the stub-ABI cleanup backlog during task resolve and close it if
   this task fully removes its remaining premise.

## Non-Goals

1. Backend-neutral completion-core redesign from RFC-0010 Phase 2.
2. Adding or adapting an `io_uring` backend.
3. Changing redo-log failure handling or propagating data/index I/O errors
   through new `Result`-bearing interfaces.
4. Redesigning the file, buffer-pool, or redo-log call sites away from their
   current `iocb`-based submission contract.
5. Removing `smol` or other async runtime dependencies that remain used outside
   the no-`libaio` fallback path.
6. Rewriting historical RFCs, task docs, or closed backlog docs that should
   remain as repository history.

## Unsafe Considerations (If Applicable)

This task modifies unsafe-sensitive code in the storage I/O boundary, but the
intended effect is simplification rather than semantic expansion.

1. Affected modules and why `unsafe` is relevant:
   - `doradb-storage/src/io/mod.rs`
     Removes fallback execution paths that currently perform raw-pointer I/O via
     `pread`/`pwrite` helpers and asynchronous completion handoff.
   - `doradb-storage/src/io/libaio_abi.rs`
     Removes no-`libaio` stub ABI support that exists only for fallback
     submission transport.
   - consumer modules such as `doradb-storage/src/file/mod.rs`,
     `doradb-storage/src/buffer/evict.rs`, and `doradb-storage/src/trx/log.rs`
     may receive small compile-only adjustments, but should keep their current
     ownership and completion contracts unchanged.
2. Required invariants and checks:
   - the remaining `libaio` path must preserve the current explicit in-flight
     ownership rule for aligned buffers and raw page pointers;
   - any remaining `unsafe` boundary touched during cleanup must keep adjacent
     `// SAFETY:` comments with concrete lifetime and alignment invariants;
   - no fallback-only stub layout or transport assumption should remain after
     the feature-gate removal.
3. Inventory refresh and validation scope:
```bash
tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md
cargo build -p doradb-storage
cargo nextest run -p doradb-storage
cargo clippy --all-features --all-targets -- -D warnings
```
   Refresh the unsafe inventory only if the implemented cleanup changes the
   baseline in touched unsafe-sensitive modules.

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Simplify `doradb-storage/src/io/mod.rs` to a single supported backend:
   - remove the `no_libaio` module;
   - remove `#[cfg(not(feature = "libaio"))]` fields and methods from
     `AIOContext`;
   - remove the fallback-specific `submit_limit()`, `wait_at_least()`, and
     `AIOEventLoop::run()` branches;
   - update any tests in the module that currently branch on the removed
     feature mode.
2. Remove no-`libaio` stub ABI support from
   `doradb-storage/src/io/libaio_abi.rs` and keep only the real `libaio`
   bindings and types required by the supported path.
3. Remove the temporary `libaio` feature-gate definition from
   `doradb-storage/Cargo.toml` so the crate no longer presents a distinct
   no-default-features async-I/O mode.
4. Keep higher-level consumers stable:
   - do not redesign `FileIOListener`, `EvictableBufferPoolListener`, or the
     redo-log group commit flow;
   - only make the smallest compile/test adjustments needed after the feature
     gate and fallback branches are removed.
5. Collapse repository validation/tooling to one supported branch:
   - update `.github/workflows/build.yml` to remove the no-default-features
     nextest pass and its JUnit artifact path;
   - simplify `.config/nextest.toml` to a single CI profile suitable for the
     supported branch;
   - update `tools/coverage_focus.rs` to stop generating and merging no-default
     coverage artifacts.
6. Synchronize active guidance and agent docs:
   - `docs/process/unit-test.md`
   - `docs/process/coding-guidance.md`
   - `docs/process/lint.md`
   - `docs/process/unsafe-review-checklist.md`
   - `docs/unsafe-usage-principles.md`
   - `AGENTS.md`
   - `GEMINI.md`
   Replace dual-pass and no-`libaio` instructions with the single supported
   `libaio` validation path.
7. During task resolve, reevaluate
   `docs/backlogs/closed/000003-improve-libaio-stub-struct-design.md`:
   - close it as obsolete/implemented if no stub-only follow-up remains;
   - otherwise split any genuinely remaining `libaio` ABI cleanup into a new,
     narrower backlog item.

## Implementation Notes

1. Simplified `doradb-storage/src/io/` to the single supported backend:
   - removed the no-`libaio` fallback module and all
     `#[cfg(feature = "libaio")]` / `#[cfg(not(feature = "libaio"))]`
     branching from `doradb-storage/src/io/mod.rs`;
   - removed the fallback-specific `AIOContext` fields plus the duplicate
     `submit_limit()`, `wait_at_least()`, and `AIOEventLoop::run()` logic;
   - kept the existing `iocb`-shaped submission contract and in-flight buffer
     ownership semantics unchanged for file, buffer-pool, and redo-log
     consumers.
2. Removed the temporary feature-gate shell and fallback-only ABI support:
   - deleted the `libaio` feature section from `doradb-storage/Cargo.toml`;
   - deleted the no-`libaio` stub ABI module from
     `doradb-storage/src/io/libaio_abi.rs`, leaving only the real `libaio`
     bindings and types required by the supported path;
   - simplified `io`-module tests so the `EAGAIN` submit behavior remains
     covered on the supported backend without feature-specific branches.
3. Collapsed active validation/tooling to the supported `libaio` branch only:
   - `.github/workflows/build.yml` now runs one `cargo nextest` coverage pass
     and uploads one JUnit report path;
   - `.config/nextest.toml` now defines a single `ci` profile for workflow use;
   - `tools/coverage_focus.rs` now runs one supported coverage phase instead of
     generating and merging default/no-default artifacts.
4. Synchronized active contributor and agent guidance with the new support
   contract:
   - updated `docs/process/unit-test.md`,
     `docs/process/coding-guidance.md`, `docs/process/lint.md`,
     `docs/process/unsafe-review-checklist.md`,
     `docs/unsafe-usage-principles.md`, `AGENTS.md`, and `GEMINI.md`;
   - active docs now require `libaio1`/`libaio-dev` and no longer advertise
     `--no-default-features` as a supported validation path.
5. Verification executed for the implemented state:
   - `cargo build -p doradb-storage`
   - `cargo nextest run -p doradb-storage`
     - result: `412/412` passed in `3.204s`
   - `cargo clippy --all-features --all-targets -- -D warnings`
   - `cargo nextest list -p doradb-storage --profile ci`
   - `tools/coverage_focus.rs --help`
6. Resolve-time sync:
   - no unsafe inventory refresh was required because the task removed
     fallback-only branches without expanding unsafe scope;
   - `tools/task.rs resolve-task-next-id --task docs/tasks/000083-retire-thread-pool-async-io-fallback-and-no-default-validation.md`
     advanced `docs/tasks/next-id` from `000083` to `000084`;
   - `tools/task.rs resolve-task-rfc --task docs/tasks/000083-retire-thread-pool-async-io-fallback-and-no-default-validation.md`
     updated phase 1 in
     `docs/rfcs/0010-retire-thread-pool-async-io-and-introduce-backend-neutral-completion-core.md`;
   - `tools/backlog.rs close-doc --path docs/backlogs/000003-improve-libaio-stub-struct-design.md ...`
     archived backlog `000003` as obsolete at
     `docs/backlogs/closed/000003-improve-libaio-stub-struct-design.md`.

## Impacts

1. Storage I/O runtime:
   - `doradb-storage/src/io/mod.rs`
   - `doradb-storage/src/io/libaio_abi.rs`
2. Cargo feature/support contract:
   - `doradb-storage/Cargo.toml`
3. Active I/O consumers that may need compile-only touch-ups:
   - `doradb-storage/src/file/mod.rs`
   - `doradb-storage/src/buffer/evict.rs`
   - `doradb-storage/src/trx/log.rs`
4. CI and local tooling:
   - `.github/workflows/build.yml`
   - `.config/nextest.toml`
   - `tools/coverage_focus.rs`
5. Active contributor and agent guidance:
   - `docs/process/unit-test.md`
   - `docs/process/coding-guidance.md`
   - `docs/process/lint.md`
   - `docs/process/unsafe-review-checklist.md`
   - `docs/unsafe-usage-principles.md`
   - `AGENTS.md`
   - `GEMINI.md`
6. Resolve-time backlog synchronization:
   - `docs/backlogs/closed/000003-improve-libaio-stub-struct-design.md`

## Test Cases

1. `cargo build -p doradb-storage` succeeds with the fallback code removed.
2. `cargo nextest run -p doradb-storage` succeeds as the only supported routine
   validation pass.
3. `cargo clippy --all-features --all-targets -- -D warnings` succeeds after
   the feature-gate removal.
4. Repository config inspection confirms there is no active CI, nextest, or
   coverage invocation of `--no-default-features`.
5. If `io`-module tests need updates after branch removal, they continue to
   validate the supported `libaio` submit/wait behavior without fallback-only
   branches.
6. If unsafe inventory changes in touched modules, refresh
   `docs/unsafe-usage-baseline.md` and verify the delta is expected.

## Open Questions

None currently.
