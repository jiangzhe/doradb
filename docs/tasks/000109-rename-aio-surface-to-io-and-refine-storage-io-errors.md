---
id: 000109
title: Rename AIO Surface To IO And Refine Storage IO Errors
status: proposal  # proposal | implemented | superseded
created: 2026-04-05
github_issue: 530
---

# Task: Rename AIO Surface To IO And Refine Storage IO Errors

## Summary

Rename the remaining `AIO*` surface in `crate::io` to `IO*`, remove the
standalone `io::AIOError` type, and refactor storage-constructor syscall
failures to preserve precise `std::io::Error` detail in crate-level error
variants. The task covers storage, redo, tests, examples, and live API docs in
one coordinated pass while leaving historical task/RFC prose unchanged.

## Context

Task `000106` intentionally deferred the broader `AIO*` rename after shared
storage runtime centralization. Backlog `000078` tracks the follow-up and
already identified two collision points: `AIOError -> IOError` conflicts with
the crate-level `Error::IOError`, and `AIOKind -> IOKind` collides locally with
`buffer::page::IOKind`.

The current repository direction is backend-neutral `crate::io`, with
`io_uring` as the default backend and `libaio` as the explicit alternate
backend. The remaining `AIO*` names therefore describe historical
implementation detail rather than the live abstraction.

The current `AIOError` also loses syscall detail in several constructor/setup
paths. `io_setup`, `IoUring::new`, `open`, `ftruncate`, and `fstat` are
collapsed into manual enum buckets even though libaio completions already use
real `std::io::Error` values. This task should make storage error reporting
more precise instead of merely renaming `AIOError` to another opaque manual
type.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Source Backlogs:`
`- docs/backlogs/000078-rename-io-module-aio-surface-to-io-naming.md`

## Goals

1. Remove live `AIO*` symbols from `crate::io` exports and from their
   production, test, and example call sites.
2. Rename the backend-neutral `crate::io` surface to `IO*` without
   compatibility aliases.
3. Delete `io::AIOError` and `AIOResult`.
4. Introduce clearer crate-level storage error variants that preserve syscall
   and backend detail where appropriate.
5. Keep logical capacity and validation failures distinct from OS-backed I/O
   failures.
6. Preserve current storage runtime behavior, redo durability behavior, and
   backend-selection policy.
7. Update live API docs and code comments to reflect the new naming and error
   semantics.
8. Validate both the default `io_uring` build and the explicit `libaio` build.

## Non-Goals

1. Changing shared-storage runtime topology, worker scheduling, or backend
   selection.
2. Removing `libaio` support or changing legacy-kernel support policy.
3. Renaming historical RFC, task, or backlog prose solely to erase historical
   `AIO` mentions.
4. Renaming `buffer::page::IOKind`; local aliases are sufficient where the name
   conflicts with the storage-layer `IOKind`.
5. Broad repo-wide error-architecture cleanup outside storage-I/O paths.
6. Changing redo log rotation semantics beyond replacing the old
   `AIOError::OutOfRange` match with the new logical capacity error.

## Unsafe Considerations (If Applicable)

This task touches unsafe-bearing direct-I/O and libc/FFI boundaries.

1. Expected affected paths:
   - `doradb-storage/src/io/mod.rs`
   - `doradb-storage/src/io/libaio_backend.rs`
   - `doradb-storage/src/io/libaio_abi.rs`
   - `doradb-storage/src/file/mod.rs`
2. Required invariants:
   - renamed `IOBuf` and `UnsafeIORequest` contracts must still document that
     borrowed pointers remain valid until exactly one completion is observed;
   - libaio request ownership, iocb lifetime, and inflight token mapping must
     remain unchanged by the rename and error refactor;
   - syscall boundary refactors must keep return-code checking explicit before
     constructing owned Rust objects or consuming owned file descriptors; and
   - any updated `// SAFETY:` comments must keep the same concrete lifetime,
     ownership, and alignment guarantees as the current code.
3. If unsafe comments or inventory counts change, refresh:
   - `tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md`
4. Validation:
   - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
   - `cargo clippy -p doradb-storage --no-default-features --features libaio --all-targets -- -D warnings`
   - `cargo nextest run -p doradb-storage`
   - `cargo nextest run -p doradb-storage --no-default-features --features libaio`

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Refactor crate-level error semantics first.
   - Remove `io::AIOError` and `AIOResult`.
   - Replace the current coarse `Error::IOError` with
     `Error::SystemIOError(std::io::Error)` for generic runtime I/O failures.
   - Add targeted storage setup/file variants that retain underlying
     `std::io::Error` detail:
     - `Error::StorageBackendSetupFailed(std::io::Error)`
     - `Error::StorageFileCreateFailed(std::io::Error)`
     - `Error::StorageFileOpenFailed(std::io::Error)`
     - `Error::StorageFileResizeFailed(std::io::Error)`
     - `Error::StorageFileStatFailed(std::io::Error)`
   - Add one logical non-syscall variant for `SparseFile::alloc(...)`
     exhaustion:
     - `Error::StorageFileCapacityExceeded`
   - Remove `Clone` from `crate::error::Error` so crate-level error variants
     can carry `std::io::Error` directly instead of collapsing them into manual
     buckets.
   - Add a narrow helper or equivalent central classification so poison paths
     can distinguish storage I/O failures from logical capacity errors without
     reintroducing broad ambiguous matching.
2. Preserve syscall precision at each boundary.
   - `LibaioBackend::new(...)` should convert negative `io_setup` returns via
     `std::io::Error::from_raw_os_error(-ret)`, matching the existing libaio
     completion convention.
   - `IouringBackend::new(...)` should treat `max_events == 0` and ring-entry
     overflow as validation errors, but map `IoUring::new(...)` failure into
     `Error::StorageBackendSetupFailed(...)`.
   - `SparseFile::{create_or_trunc, create_or_fail, open}` should treat
     interior-NUL path input as invalid path/input, while `open`, `ftruncate`,
     and `fstat` failures should flow into the new targeted crate-level
     variants with precise `std::io::Error` detail.
3. Rename the backend-neutral `crate::io` surface.
   - `AIOBuf` -> `IOBuf`
   - `AIOClient` -> `IOClient`
   - `AIOMessage` -> `IOMessage`
   - `AIOKind` -> `IOKind`
   - `AIOKey` -> `IOKey`
4. Rename the libaio-only raw request wrappers without aliases.
   - `AIO<T>` -> `IORequest<T>`
   - `UnsafeAIO` -> `UnsafeIORequest`
   - Keep the wrappers feature-gated under the `libaio` backend path and update
     comments to say "libaio request" rather than "AIO" where the meaning is
     backend-specific.
5. Propagate renamed symbols and error variants through call sites.
   - Update storage worker, redo, buffer, table, file, and test imports.
   - Use a local alias such as `StorageIOKind` where `buffer::page::IOKind`
     already exists and a direct import would conflict.
   - Update redo log rotation to match `Error::StorageFileCapacityExceeded`
     instead of `AIOError::OutOfRange`.
   - Update checkpoint and persistence poison classification to use the new
     crate-level error variants or helper instead of
     `Error::IOError | Error::AIOError(_)`.
6. Update live documentation and examples.
   - Refresh current API-facing docs/comments such as `docs/async-io.md` and
     the readonly single-miss latency benchmark imports.
   - Leave historical RFC/task/backlog prose unchanged unless it documents a
     currently live API surface.

## Implementation Notes

## Impacts

1. Error model and conversions:
   - `doradb-storage/src/error.rs`
2. Backend-neutral IO exports and worker core:
   - `doradb-storage/src/io/mod.rs`
   - `doradb-storage/src/io/buf.rs`
   - `doradb-storage/src/io/backend.rs`
3. Backend-specific setup and libaio raw request wrappers:
   - `doradb-storage/src/io/libaio_backend.rs`
   - `doradb-storage/src/io/iouring_backend.rs`
   - `doradb-storage/src/io/libaio_abi.rs`
4. Sparse-file and file-system call sites:
   - `doradb-storage/src/file/mod.rs`
   - `doradb-storage/src/file/fs.rs`
   - `doradb-storage/src/file/cow_file.rs`
   - `doradb-storage/src/file/table_file.rs`
   - `doradb-storage/src/file/multi_table_file.rs`
5. Redo, checkpoint, and poison paths:
   - `doradb-storage/src/trx/log.rs`
   - `doradb-storage/src/table/persistence.rs`
   - `doradb-storage/src/catalog/checkpoint.rs`
6. Buffer, tests, and example call sites:
   - `doradb-storage/src/buffer/readonly.rs`
   - `doradb-storage/src/buffer/evict.rs`
   - `doradb-storage/src/table/tests.rs`
   - `doradb-storage/examples/bench_readonly_single_miss_latency.rs`

## Test Cases

1. Default validation passes:
   - `cargo nextest run -p doradb-storage`
2. Alternate-backend validation passes:
   - `cargo nextest run -p doradb-storage --no-default-features --features libaio`
3. Backend constructor tests cover:
   - invalid `max_events` validation;
   - libaio `io_setup` failure mapping; and
   - io_uring setup failure mapping.
4. Sparse-file tests cover:
   - create/open/stat/resize failures mapping to the new crate-level error
     variants with preserved syscall detail; and
   - logical capacity exhaustion mapping to
     `Error::StorageFileCapacityExceeded`.
5. Redo tests confirm log rotation still triggers on
   `Error::StorageFileCapacityExceeded` and does not poison runtime for that
   case.
6. Checkpoint and shared-storage tests confirm send and runtime I/O failures
   still poison storage admission with the correct source after the error
   refactor.
7. Example and current API docs compile or read consistently with the new
   `IO*` names and no live `AIO*` imports remain under `doradb-storage/src` or
   `doradb-storage/examples`.

## Open Questions

1. After this task lands, should historical design documents that describe
   superseded `AIO*` APIs get a separate documentation cleanup pass, or should
   historical terminology remain untouched for archival accuracy?
