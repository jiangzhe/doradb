---
id: 000090
title: Add io_uring Backend
status: implemented  # proposal | implemented | superseded
created: 2026-03-25
github_issue: 478
---

# Task: Add io_uring Backend

## Summary

Implement RFC-0010 phase 5 by adding an `io_uring` backend under the existing
backend-neutral completion core, renaming backend types around
`StorageBackend`, `LibaioBackend`, and `IouringBackend`, and migrating storage
startup paths to the canonical backend alias. This task intentionally does not
switch `io_uring` to the default backend; after the RFC phase split, the
default-backend change, follow-up cleanup, and `docs/async-io.md` design
documentation are deferred to phase 6.

## Context

RFC-0010 now splits the old phase-5 endgame into two steps: phase 5 adds the
backend itself, while phase 6 switches the default backend, performs
post-adoption cleanup, and adds the repository-level async-I/O design doc.
After phases 2-4, the generic worker and completion model are already
backend-neutral, but the repository still hardcodes `LibaioContext`
construction in the table-file, buffer-pool, and redo-log startup paths, and
some tests still use libaio-specific failure-injection hooks. That means the
repository is structurally ready for a second backend, while the default switch
still carries extra support-policy and CI-contract risk on top of the backend
implementation itself.

This task narrows the work to backend delivery only:

1. add `IouringBackend` to the current `IOBackend` layer;
2. rename the current libaio implementation to `LibaioBackend`;
3. introduce a canonical `StorageBackend` type alias selected by cargo
   features; and
4. move production startup paths to that alias so later default switching is a
   bounded follow-up instead of another cross-cutting rename.

The task also removes the old libaio-only `bench_aio.rs` example instead of
keeping a backend-specific benchmark entry point alive during the new backend
landing.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Parent RFC:`
`- docs/rfcs/0010-retire-thread-pool-async-io-and-introduce-backend-neutral-completion-core.md`

## Goals

1. Add `IouringBackend` as a second implementation of the existing
   `crate::io::IOBackend` contract.
2. Rename `LibaioContext` to `LibaioBackend` so backend type names describe
   implementations instead of generic roles.
3. Add a canonical `StorageBackend` export in `crate::io` and migrate
   production backend construction sites to it.
4. Keep `libaio` as the default backend in this task while making `io_uring`
   buildable and testable as a non-default compile-time choice.
5. Keep the completion-driven ownership model, worker shutdown behavior, and
   storage error policy from phases 2-4 unchanged.
6. Replace or lift libaio-specific test hooks so the repository can inject I/O
   failures against the new backend path without depending on `iocb` internals.
7. Remove `doradb-storage/examples/bench_aio.rs`.

## Non-Goals

1. Switching the default backend from `libaio` to `io_uring`.
2. Defining the final rollout or CI support policy for a default `io_uring`
   branch.
3. Runtime backend switching.
4. Retiring `libaio` support entirely.
5. Writing `docs/async-io.md`; that repository-level design document belongs to
   phase 6 together with the default-backend switch.
6. Reopening storage-I/O error handling, buffer-pool API cleanup, checkpoint
   semantics, recovery semantics, or on-disk formats.
7. Promising or proving the final performance floor required for the later
   default-backend switch.

## Unsafe Considerations (If Applicable)

This task touches unsafe-sensitive storage I/O code because both current and
new backends submit kernel I/O against owned direct buffers and borrowed raw
page pointers whose lifetimes must remain valid until worker-observed
completion.

1. Affected modules and why `unsafe` remains relevant:
   - `doradb-storage/src/io/mod.rs`
     feature-gated backend exports, worker defaults, and any remaining
     backend-specific helper confinement still sit on top of borrowed pointer
     and direct-buffer ownership rules.
   - `doradb-storage/src/io/libaio_backend.rs`
     renamed implementation keeps libaio submission/completion ABI plumbing and
     any remaining raw helper compatibility for the libaio path.
   - new `doradb-storage/src/io/iouring_backend.rs`
     backend implementation will prepare `io_uring` submissions and completions
     against the same owned/borrowed memory model.
   - `doradb-storage/src/file/table_fs.rs`,
     `doradb-storage/src/buffer/evict.rs`,
     `doradb-storage/src/trx/sys_conf.rs`, and
     `doradb-storage/src/trx/log.rs`
     startup wiring must continue to hold backend-owned workers and request
     state until completion/shutdown sequencing finishes.
   - `doradb-storage/src/buffer/readonly.rs` and
     `doradb-storage/src/table/tests.rs`
     failure-injection changes must not introduce hooks that let tests observe
     freed or stale in-flight memory.
2. Required invariants and `// SAFETY:` expectations:
   - owned buffers and borrowed page pointers must remain valid until the
     worker processes backend completion and drops the inflight entry exactly
     once;
   - backend user-data/token mapping must still round-trip to the correct
     inflight slot and reject stale completion reuse;
   - shutdown and error paths must not drop staged or submitted requests before
     the backend has either never accepted them or has reported completion;
   - any backend-specific `unsafe` in the new `io_uring` implementation must
     stay localized with `// SAFETY:` comments phrased in terms of queue
     submission, completion ownership, and teardown.
3. Inventory refresh and validation scope:

```bash
tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md
cargo build -p doradb-storage
cargo nextest run -p doradb-storage
cargo build -p doradb-storage --no-default-features --features iouring
cargo nextest run -p doradb-storage --no-default-features --features iouring
cargo clippy --all-features --all-targets -- -D warnings
```

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Introduce the feature contract for multiple compile-time storage backends.
   - Update `doradb-storage/Cargo.toml` to define explicit backend features.
   - Keep `libaio` as the default backend in this task.
   - Make invalid feature combinations fail fast so the repository does not
     silently build with both backends enabled or with no backend selected.

2. Rename backend implementation types and add the canonical alias.
   - Rename `LibaioContext` to `LibaioBackend`.
   - Add `IouringBackend`.
   - Export one canonical `StorageBackend` type from `doradb-storage/src/io/mod.rs`
     via feature-gated `pub use`.
   - Change `IOWorkerBuilder<T, B = ...>` and `IOWorker<..., B = ...>` to
     default to `StorageBackend` instead of naming the libaio implementation
     directly.

3. Implement `IouringBackend` on the existing completion core.
   - Add a new backend module under `doradb-storage/src/io/`.
   - Implement `IOBackend` for the new type, including prepared submission
     state, batch submission, completion wait, and backend-token decoding.
   - Preserve the existing `Operation` model for both owned-buffer and
     borrowed-page requests instead of introducing a parallel API for
     `io_uring`.

4. Migrate production backend construction sites to the canonical alias.
   - Replace direct `LibaioContext::new(...)` calls with
     `StorageBackend::new(...)` in:
     - `doradb-storage/src/file/table_fs.rs`
     - `doradb-storage/src/buffer/evict.rs`
     - `doradb-storage/src/trx/sys_conf.rs`
     - `doradb-storage/src/trx/log.rs`
   - Keep the surrounding storage semantics unchanged; this task is about
     backend selection and implementation, not higher-level behavior changes.

5. Confine backend-specific legacy helpers to the implementation paths that
   still need them.
   - Review `AIO`, `UnsafeAIO`, raw submit helpers, and the `file`-module
     wrappers that only exist for libaio-shaped request construction.
   - Keep them behind the libaio backend path where needed, but do not let the
     default generic path depend on them once `StorageBackend` is introduced.

6. Replace libaio-specific failure-injection seams with backend-neutral test
   support.
   - Update readonly/table I/O failure tests so they do not require direct
     `iocb` inspection through `LibaioTestHook`.
   - Prefer a generic hook surface at the backend seam or a small shared test
     abstraction that can drive both backends.

7. Remove the libaio-only benchmark example.
   - Delete `doradb-storage/examples/bench_aio.rs`.
   - Remove or update any references that still mention that example.

8. Synchronize validation/tooling for the new non-default backend path.
   - Update active process/CI guidance only as much as needed to document and
     validate the new non-default `io_uring` backend path.
   - Do not redefine `io_uring` as the primary quality gate in this task.

## Implementation Notes

1. Added the compile-time backend-selection contract and canonical backend
   naming.
   - `doradb-storage/Cargo.toml` now defines explicit `libaio` and `iouring`
     features with `libaio` kept as the default backend for this phase;
   - `doradb-storage/src/io/mod.rs` now rejects invalid backend-feature
     combinations at compile time, re-exports `LibaioBackend` and
     `IouringBackend`, and exposes `StorageBackend` as the canonical
     feature-selected backend alias;
   - the old `LibaioContext` naming was retired in favor of
     `LibaioBackend`.
2. Landed `IouringBackend` on top of the existing completion core without
   changing higher-level storage semantics.
   - `doradb-storage/src/io/iouring_backend.rs` implements `IOBackend` using
     the existing `Operation` ownership model for both owned direct buffers and
     borrowed page memory;
   - the backend keeps worker-owned staged SQEs, backend-token round-tripping,
     and completion-driven lifetime management aligned with the generic worker
     contract rather than introducing a separate `io_uring`-specific request
     API.
3. Moved production storage startup paths to the canonical backend alias.
   - table-file, evictable-buffer-pool, transaction config, and redo-log
     startup now construct `StorageBackend` instead of wiring the libaio
     implementation type directly;
   - this keeps backend selection compile-time while making the later
     default-switch task a bounded follow-up instead of another repository-wide
     rename.
4. Replaced libaio-only failure-injection seams with backend-neutral test
   support.
   - readonly-buffer and table tests now use a storage-backend hook surface
     instead of `iocb`-specific inspection;
   - the test-only hook types were later moved under `io::tests` and
     re-exported narrowly from `crate::io` to match the repository's inline
     test-helper style.
5. Completed review-driven follow-up fixes and cleanup within task scope.
   - `submit_pending_sqes(...)` in `iouring_backend.rs` now falls back to
     blocking `submit_and_wait(1)` on `EAGAIN` / `EBUSY` when SQEs are already
     pending, so worker-side submitted-count bookkeeping stays consistent with
     what the kernel actually accepted;
   - the old libaio-only benchmark example
     `doradb-storage/examples/bench_aio.rs` was removed;
   - dead `SparseFile` `pread*` / `pwrite*` wrappers and their unused
     file-level forwarding helpers were removed after the backend-neutral path
     made them obsolete.
6. Kept phase-5 validation and workflow policy aligned with the approved scope.
   - CI and pre-commit stayed default-backend-only for this task rather than
     making `io_uring` a primary quality gate;
   - process docs and local validation guidance now describe the alternate
     `io_uring` pass as an explicit backend-change validation step instead of a
     default branch requirement.
7. Validation completed for the implemented state.
   - `cargo fmt --all --check`
     - result: passed
   - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
     - result: passed
   - `cargo nextest run -p doradb-storage`
     - result: passed
   - `cargo clippy -p doradb-storage --no-default-features --features iouring --all-targets -- -D warnings`
     - result: passed
   - `cargo nextest run -p doradb-storage --no-default-features --features iouring`
     - result: passed
8. Tracking and review state at resolve time.
   - task issue: `#478`
   - implementation PR: `#479`

## Impacts

1. `doradb-storage/Cargo.toml`
2. `doradb-storage/src/io/mod.rs`
3. `doradb-storage/src/io/backend.rs`
4. `doradb-storage/src/io/libaio_backend.rs`
5. new `doradb-storage/src/io/iouring_backend.rs`
6. `doradb-storage/src/file/table_fs.rs`
7. `doradb-storage/src/buffer/evict.rs`
8. `doradb-storage/src/trx/sys_conf.rs`
9. `doradb-storage/src/trx/log.rs`
10. `doradb-storage/src/buffer/readonly.rs`
11. `doradb-storage/src/table/tests.rs`
12. `doradb-storage/examples/bench_aio.rs`
13. CI/process docs only if they need explicit non-default backend validation
    updates

## Test Cases

1. The generic worker and completion-core tests still pass on the default
   `libaio` build after the backend renames and aliasing changes.
2. `doradb-storage` builds and runs its supported validation pass on the
   default `libaio` backend.
3. `doradb-storage` builds and runs a non-default `io_uring` validation pass.
4. Table-file async reads and writes work through `StorageBackend` on both
   backends.
5. Evictable-buffer-pool page reads and writes work through `StorageBackend` on
   both backends.
6. Redo-log group commit preserves completion ordering, fsync sequencing, and
   fatal write failure publication on both backends.
7. Read/write failure-injection tests in readonly/table paths can exercise the
   `io_uring` path without depending on libaio-only hook internals.
8. Removing `bench_aio.rs` does not leave broken example references or example
   target failures.

## Open Questions

1. Which exact acceptance bar should the later default-switch task require
   before `StorageBackend` points to `IouringBackend` by default?
2. After `IouringBackend` is stable, should the remaining libaio-only raw
   helper surface be removed entirely or kept as backend-private compatibility
   code?
3. If the repository keeps both backends long term, how broad should non-default
   backend CI coverage be relative to the default branch?
