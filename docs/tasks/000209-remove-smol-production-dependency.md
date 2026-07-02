---
id: 000209
title: Remove smol Production Dependency
status: implemented  # proposal | implemented | superseded
created: 2026-07-02
github_issue: 805
---

# Task: Remove smol Production Dependency

## Summary

Remove `smol` from the normal production dependency graph by adding a
crate-private async runtime shim in `doradb-storage/src/runtime.rs`. Production
background worker threads should drive their top-level futures with
`futures::executor::block_on`, and the one production cooperative async yield
should use a tiny crate-local self-waking future.

Tests may continue to use `smol::block_on`, `smol::spawn`, and `smol::Timer`.
The existing production `futures` dependency stays in place because Doradb uses
`futures` utilities such as `try_join_all`, `FutureExt`, and the macro family in
current code and tests.

## Context

Issue Labels:
- type:task
- priority:medium
- codex

Related Context:
- docs/architecture.md
- docs/transaction-system.md
- docs/checkpoint-and-recovery.md
- docs/engine-component-lifetime.md
- docs/process/unit-test.md
- docs/process/coding-guidance.md

Doradb foreground transaction APIs are async, while several owner-managed
background services run on dedicated OS threads. Current production `smol`
usage is narrow:

- `TransactionSystem::start_cleanup_thread()` blocks a cleanup future with
  `smol::block_on`.
- `TransactionSystem::start_purge_threads()` and `dispatch_purge()` block purge
  worker futures with `smol::block_on`.
- `EvictableBufferPool::try_dispatch_io_read()` imports
  `smol::future::yield_now` only to yield after a retryable exclusive-page-lock
  miss.

The purge code does not spawn local async tasks. Replacing the outer
`smol::block_on(...)` with `futures::executor::block_on(...)` drives the same
top-level worker futures without depending on the smol runtime or on
`async_executor::LocalExecutor`.

`smol::block_on` is a re-export of `async_io::block_on`, which initializes
async-io runtime machinery. Doradb does not need that machinery for these
production worker loops because their waits are driven by existing futures,
channels, events, and the explicit local executor where needed.

`futures` is already a normal dependency and remains justified:

- production code uses `futures::future::try_join_all`;
- production code uses `futures::FutureExt` for `.fuse()` and `.map()`;
- tests use `futures::poll!`, `futures::pin_mut!`, `futures::select!`,
  `futures::join!`, `futures::task::noop_waker`, `join3`, and
  `FuturesUnordered`.

`futures-lite` has a `block_on` and `yield_now`, but it does not provide a
direct `try_join_all` equivalent or the same macro/API surface currently used
by the crate. Replacing `futures` is therefore a separate cleanup, not part of
this task.

## Goals

- Add a crate-private `doradb-storage/src/runtime.rs` module for production
  async-driver helpers.
- Provide `runtime::block_on(future)` backed by
  `futures::executor::block_on`.
- Provide `runtime::yield_now()` as a small runtime-agnostic future that yields
  once by waking the current task and returning `Poll::Pending` on first poll,
  then `Poll::Ready(())` on the next poll.
- Replace all production `smol::block_on` calls in transaction cleanup and
  purge worker startup paths with `crate::runtime::block_on`.
- Replace the production `smol::future::yield_now` import in evictable buffer
  pool retry logic with `crate::runtime::yield_now`.
- Move `smol` from `[dependencies]` to `[dev-dependencies]` in
  `doradb-storage/Cargo.toml`, using the existing workspace dependency entry.
- Update the `weak_handle_baseline` example so it does not require `smol` as a
  normal dependency.
- Verify that `smol` no longer appears in the normal dependency graph for
  `doradb-storage`.

## Non-Goals

- Do not remove or replace the production `futures` dependency.
- Do not replace test-only uses of `smol::block_on`, `smol::spawn`, or
  `smol::Timer`.
- Do not introduce runtime injection, a public runtime trait, background-service
  abstraction, tokio compatibility, or a generic spawn API.
- Do not change worker ownership, shutdown ordering, purge semantics,
  transaction cleanup semantics, or buffer-pool retry behavior.
- Do not change async lock, event-listener, or flume dependencies.
- Do not update unrelated task docs or process docs.

## Plan

1. Add `doradb-storage/src/runtime.rs`.
   - Import `std::future::Future`, `std::pin::Pin`, and
     `std::task::{Context, Poll}` as needed.
   - Add:
     - `pub(crate) fn block_on<F: Future>(future: F) -> F::Output`;
     - `pub(crate) fn yield_now() -> YieldNow`, where `YieldNow` is a
       crate-private or module-private future type.
   - Implement `block_on` by delegating directly to
     `futures::executor::block_on`.
   - Implement `YieldNow` with one boolean state bit:
     - first `poll()` sets the bit, calls `cx.waker().wake_by_ref()`, and
       returns `Poll::Pending`;
     - second `poll()` returns `Poll::Ready(())`.
   - Keep the module crate-private. Do not expose these helpers through the
     public crate API.

2. Register the module.
   - Add `mod runtime;` to `doradb-storage/src/lib.rs`.
   - Place it near the other crate-private infrastructure modules.

3. Replace production transaction cleanup blocking.
   - In `doradb-storage/src/trx/sys.rs`, import `crate::runtime`.
   - Change `TransactionSystem::start_cleanup_thread()` from
     `smol::block_on(async move { ... })` to
     `runtime::block_on(async move { ... })`.
   - Do not alter cleanup message ordering, `Stop` drain behavior, abandoned
     transaction cleanup, terminal rollback cleanup, or failed-precommit cleanup
     behavior.

4. Replace production purge worker blocking.
   - In `doradb-storage/src/trx/purge.rs`, import `crate::runtime`.
   - Change the three production top-level purge worker calls:
     - single-threaded purger;
     - purge dispatcher;
     - purge executor.
   - Remove the unused `async_executor::LocalExecutor` construction in each
     worker thread and block the purge loop future directly.
   - Do not replace `LocalExecutor` with `futures::executor::LocalPool`; no
     purge path spawns local tasks that require a scheduler.

5. Replace the production async yield.
   - In `doradb-storage/src/buffer/evict.rs`, replace
     `use smol::future::yield_now;` with `use crate::runtime::yield_now;`.
   - Keep the existing retry behavior in `DispatchAction::RetryYield`:
     `yield_now().await`.
   - Do not change page-lock retry conditions, in-memory budget handling, or
     I/O dispatch sequencing.

6. Update dependency classification.
   - Remove `smol = { workspace = true }` from `[dependencies]` in
     `doradb-storage/Cargo.toml`.
   - Add `smol = { workspace = true }` under `[dev-dependencies]`.
   - Leave `smol = "2.0"` in `[workspace.dependencies]` so tests can continue
     to share the pinned version.
   - Leave `futures = { workspace = true }` in `[dependencies]`.
   - Remove direct `async-executor` dependency entries from the storage crate
     and workspace manifest because no workspace member uses them directly.

7. Update example runtime driving.
   - In `doradb-storage/examples/weak_handle_baseline.rs`, change the top-level
     `smol::block_on(run_baseline(&args))?` call to
     `futures::executor::block_on(run_baseline(&args))?`.
   - Do not refactor the benchmark flow beyond the dependency cleanup.

8. Search and verify production `smol` removal.
   - Run a search for `smol::` and `use smol` in non-test production regions.
   - Expected remaining `smol` references are in `#[cfg(test)]` modules,
     integration tests, and any intentionally test-only helper code.
   - Confirm `doradb-storage/Cargo.toml` has no normal `smol` dependency.

9. Validate normal and alternate backend builds.
   - Run the standard build and test commands listed in `Test Cases`.
   - Because this touches async I/O-adjacent worker plumbing, also run the
     `libaio` alternate validation path.
   - Inspect the normal dependency graph to ensure `smol` has no normal path.

## Implementation Notes

- Implemented the crate-private `runtime` shim with `executor::block_on` and a
  one-shot self-waking `YieldNow` future.
- Replaced production `smol` use in transaction cleanup, purge worker startup,
  and evictable-buffer retry-yield paths while preserving worker ownership and
  retry semantics.
- Removed direct `async-executor` dependency entries after confirming purge
  workers do not spawn local tasks onto `LocalExecutor`.
- Moved `smol` to `dev-dependencies`; normal dependency graph verification
  reports no path from `doradb-storage` to `smol`.
- Updated `weak_handle_baseline` to use `futures::executor` without adding a
  normal `smol` edge.
- Review follow-up on the task branch fixed a poison-listener TOCTOU wait in
  `wait_frozen_pages_stabilized`; this was outside the original dependency
  cleanup but resolved a blocking review issue.
- Validation completed:
  - `cargo build -p doradb-storage`
  - `cargo build -p doradb-storage --example weak_handle_baseline`
  - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
  - `cargo nextest run -p doradb-storage`
  - `cargo nextest run -p doradb-storage --no-default-features --features libaio`
  - `cargo tree -p doradb-storage --edges normal -i smol`
  - `cargo tree -p doradb-storage -i smol`
  - `tools/style_audit.rs --diff-base origin/main`

## Impacts

- `doradb-storage/src/runtime.rs`
  - New crate-private module containing the production `block_on` and
    `yield_now` helpers.
- `doradb-storage/src/lib.rs`
  - Adds the crate-private module registration.
- `doradb-storage/src/trx/sys.rs`
  - Transaction cleanup worker changes from `smol::block_on` to
    `runtime::block_on`.
- `doradb-storage/src/trx/purge.rs`
  - Purge worker, dispatcher, and executor top-level future driving changes
    from `smol::block_on` with `LocalExecutor::run(...)` to direct
    `runtime::block_on`.
- `doradb-storage/src/buffer/evict.rs`
  - Retry-yield helper changes from `smol::future::yield_now` to
    `runtime::yield_now`.
- `doradb-storage/examples/weak_handle_baseline.rs`
  - Example top-level async driver changes from `smol::block_on` to
    `futures::executor::block_on`.
- `doradb-storage/Cargo.toml`
  - `smol` moves from normal dependencies to dev-dependencies.
  - Direct `async-executor` dependency is removed.
- `Cargo.toml`
  - Workspace `async-executor` dependency entry is removed.
- `Cargo.lock`
  - May update dependency edge metadata after the manifest change.

## Test Cases

- `cargo build -p doradb-storage`
  - Confirms the production library and examples can compile without `smol` as
    a normal dependency.
- `cargo nextest run -p doradb-storage`
  - Confirms existing async tests still pass while using `smol` as a
    dev-dependency.
- `cargo nextest run -p doradb-storage --no-default-features --features libaio`
  - Confirms the alternate I/O backend path still passes after worker-driver
    changes.
- `cargo tree -p doradb-storage --edges normal -i smol`
  - Should report no normal dependency path from `doradb-storage` to `smol`.
- `cargo tree -p doradb-storage --edges normal -i async-executor`
  - Should report no normal dependency path from `doradb-storage` to
    `async-executor`.
- `cargo tree -p doradb-storage -i smol`
  - Should show only dev/test/example dependency paths, if requested with edge
    selection that includes dev dependencies.
- Search verification:
  - `rg -n "smol::|use smol" doradb-storage/src doradb-storage/examples doradb-storage/tests`
    should show no non-test production use in `src` and no example dependency
    on `smol::block_on`.

## Open Questions

None.
