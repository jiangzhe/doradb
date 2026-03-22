---
id: 000085
title: Introduce IOCompletion-Based Async IO Framework
status: proposal  # proposal | implemented | superseded
created: 2026-03-21
github_issue: 467
---

# Task: Introduce IOCompletion-Based Async IO Framework

## Summary

Implement Phase 2 of RFC-0010 by refactoring `doradb-storage/src/io/` around an
`IOCompletion`-oriented completion core, hiding libaio ABI details below the
generic boundary, and migrating the current file, buffer-pool, and redo-log
call paths onto that core. The new design should be intentionally friendly to a
later `io_uring` backend and future default-backend switch, but this task keeps
`io_uring` delivery out of scope. Internal compatibility with the current
`AIO<T>`, `UnsafeAIO`, `IOQueue`, and raw-`iocb` flow is low priority; prefer a
clean completion-oriented refactor over incremental adapters.

## Context

RFC-0010 isolates this work as the second phase after thread-pool fallback
removal and before storage-I/O error-policy changes plus `io_uring` delivery.
Phase 1 intentionally left the repository on the old libaio-shaped submission
boundary. The current generic layer still exposes raw `iocb` pointers and keeps
backend-specific submission objects in types that are supposed to be generic:

1. `AIOContext::submit_limit()` takes `&[*mut iocb]`.
2. `AIO<T>` and `UnsafeAIO` embed `Box<iocb>` directly.
3. `IOQueue<T>` stores a parallel `Vec<IocbRawPtr>`.
4. `FileIOListener` and `EvictableBufferPoolListener` construct `iocb` objects
   in `on_request()` before enqueueing work.
5. Redo-log group commit bypasses the event-loop path and carries
   `IocbRawPtr` directly through `CommitGroup::split()` and `FileProcessor`.

That boundary blocks later backend-neutral work. A future RFC phase will add an
`io_uring` driver and make it the default I/O mode, so this phase should shape
the generic layer with that later adaptation in mind instead of preserving
libaio-oriented abstractions.

The primary design principle for this task is to introduce an `IOCompletion`
object as the bridge between client-side request preparation and worker-side
submission/completion handling. `IOCompletion` should be a state machine that
can hold either owned buffers or guarded/raw-page references, while cleanup and
final resource release stay on the I/O worker side. That keeps explicit
ownership valid until completion is observed and prevents dropped waiters or
client cancellation from stranding buffers or stalling follower work.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Parent RFC:`
`- docs/rfcs/0010-retire-thread-pool-async-io-and-introduce-backend-neutral-completion-core.md`

## Goals

1. Introduce an `IOCompletion`-oriented generic completion core in
   `doradb-storage/src/io/`.
2. Remove raw `iocb` exposure from the generic submission contract and push
   libaio encoding/ABI details below the generic layer.
3. Preserve the explicit in-flight ownership model for aligned buffers and
   guarded/raw-page references until worker-observed completion.
4. Keep worker-side cleanup and resource release as the authoritative teardown
   path for submitted I/O.
5. Migrate the file-I/O listener path, evictable-buffer page-I/O path, and
   redo-log direct-submit path onto the same completion abstraction.
6. Favor a clean `IOCompletion`-oriented refactor over preserving current
   internal `AIO*` interfaces or thin compatibility shims.
7. Keep the resulting generic/core split friendly to a later `io_uring`
   backend without implementing `io_uring` in this task.

## Non-Goals

1. Implementing the `io_uring` backend or switching the default backend in this
   task.
2. Changing storage-level error policy for redo-log, table-file, or
   buffer-pool I/O.
3. Replacing completion-driven ownership with a `Future`-style kernel-I/O API.
4. Keeping source compatibility for the current internal `AIO<T>`,
   `UnsafeAIO`, `IOQueue`, `IocbRawPtr`, or redo-log submission helper shapes.
5. Redesigning engine/component lifetime or worker topology beyond the minimum
   required by the new completion core.
6. Changing on-disk formats, checkpoint semantics, redo durability semantics,
   or recovery filtering behavior.

## Unsafe Considerations (If Applicable)

This task reshapes an unsafe-sensitive boundary and is expected to move
substantial unsafe-adjacent code even if net unsafe scope stays similar.

1. Affected modules and why `unsafe` is relevant:
   - `doradb-storage/src/io/mod.rs`
     Introduces the `IOCompletion` state machine and refactors ownership around
     buffer pointers, submission state, and completion cleanup.
   - `doradb-storage/src/io/libaio_abi.rs`
     Remains the libaio-specific ABI layer and may gain or lose helper
     boundaries as submission encoding moves below the generic interface.
   - `doradb-storage/src/file/mod.rs`
     Still needs worker-owned handling for owned direct buffers and static read
     pointers.
   - `doradb-storage/src/buffer/evict.rs` and `doradb-storage/src/buffer/page.rs`
     Still depend on guarded raw-page references remaining valid until worker
     completion transitions frame state.
   - `doradb-storage/src/trx/group.rs` and `doradb-storage/src/trx/log.rs`
     Still depend on in-flight redo-log buffers staying owned until completion
     and post-completion sync cleanup finish.
2. Required invariants and `// SAFETY:` expectations:
   - an `IOCompletion` that has been submitted must own or retain the exact
     memory lifetime needed by the backend until completion is processed on the
     worker side;
   - state transitions must ensure cleanup executes exactly once, even when the
     waiter or higher-level request object is dropped early;
   - backend-specific raw-pointer and submission-object access must stay below
     the generic completion boundary with localized `// SAFETY:` comments;
   - buffer-pool guarded page memory must not be released or reused until the
     completion state machine confirms the worker has observed completion.
3. Inventory refresh and validation scope:
```bash
tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md
cargo build -p doradb-storage
cargo nextest run -p doradb-storage
cargo clippy --all-features --all-targets -- -D warnings
```
   Refresh the unsafe inventory if the implemented refactor changes the unsafe
   baseline in the touched modules.

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Refactor `doradb-storage/src/io/` around `IOCompletion`.
   - Introduce an `IOCompletion` state machine that models at least request
     preparation, queued/submitted ownership, worker-observed completion, and
     final cleanup.
   - Support both owned-buffer payloads and guarded/raw-page reference payloads
     within the same lifecycle model.
   - Reshape or replace current `AIO<T>` and `UnsafeAIO` types so their generic
     role becomes ownership/lifecycle, not libaio ABI carriage.
2. Split the generic completion core from the libaio-specific driver layer.
   - Move `iocb` encoding and any raw submission-object assembly behind a
     backend-specific boundary, likely under a new internal libaio driver
     module.
   - Remove raw `iocb` pointers from the public/generic queueing and batching
     interfaces.
3. Replace queueing and event-loop batching with `IOCompletion` flow.
   - Update `IOQueue` and `AIOEventLoop` so listeners enqueue opaque completion
     objects plus listener-owned submission metadata instead of raw pointers.
   - Keep `AIOClient` request flow behaviorally similar only where that helps
     surrounding code, but do not preserve the current internal API shape for
     compatibility's sake.
4. Refactor file I/O onto worker-owned completion cleanup.
   - Update `FileIOState`, `FileIOListener`, and related direct read/write
     helpers so request preparation hands off an `IOCompletion`.
   - Keep promise/result delivery semantics intact while moving buffer recycle
     and final release decisions to the worker completion path.
5. Refactor buffer-pool page I/O onto `IOCompletion`.
   - Update `SingleFileIO`, `PageIO`, `EvictableBufferPoolListener`, and any
     related inflight status bookkeeping so guarded page memory is retained via
     the completion object until worker-side completion handling runs.
   - Preserve current frame-state transitions and batch-write notification
     behavior.
6. Refactor redo-log batching onto the same completion abstraction.
   - Replace `(IocbRawPtr, SyncGroup)` and `FileProcessor.io_reqs` with opaque
     completion submissions while keeping partial-submit, wait-for-one,
     wait-for-all, fsync ordering, and buffer reuse behavior intact.
   - Ensure dropped waiters do not interfere with completion cleanup or group
     progress.
7. Refresh tests around the new lifecycle.
   - Extend `io`-module tests to cover completion-state transitions, backpressure,
     and worker-side cleanup.
   - Update file, buffer, and redo-log tests for the refactored completion path.
   - Run the supported validation pass for `doradb-storage`.

## Implementation Notes

## Impacts

1. Generic I/O core:
   - `doradb-storage/src/io/mod.rs`
   - likely new internal backend module(s) under `doradb-storage/src/io/`
   - `AIOContext`, `AIOEventLoop`, `AIOEventListener`, `IOQueue`,
     `AIO<T>`, `UnsafeAIO`, and successor `IOCompletion` types
2. Libaio backend boundary:
   - `doradb-storage/src/io/libaio_abi.rs`
3. File I/O path:
   - `doradb-storage/src/file/mod.rs`
   - `FileIOState`, `FileIO`, `FileIOListener`, `FileIOPromise`
   - compile-driven spillover into `doradb-storage/src/file/table_fs.rs`,
     `doradb-storage/src/file/table_file.rs`, and
     `doradb-storage/src/file/multi_table_file.rs` as needed
4. Buffer-pool page I/O path:
   - `doradb-storage/src/buffer/evict.rs`
   - `doradb-storage/src/buffer/page.rs`
   - `SingleFileIO`, `PageIO`, `EvictableBufferPoolListener`, inflight IO
     bookkeeping
5. Redo-log path:
   - `doradb-storage/src/trx/group.rs`
   - `doradb-storage/src/trx/log.rs`
   - `CommitGroup::split`, `SyncGroup`, `FileProcessor`
6. Tests:
   - `doradb-storage/src/io/mod.rs`
   - compile-driven updates in touched file/buffer/trx tests

## Test Cases

1. `IOCompletion` state-machine coverage.
   - owned-buffer requests transition from preparation through worker cleanup
     exactly once;
   - guarded/raw-page requests retain the underlying memory until the worker
     completion path runs.
2. Worker-side cleanup with dropped waiters.
   - dropping a file-I/O waiter or other client-side handle before completion
     does not leak buffers, leave stale state behind, or stall follower work.
3. File I/O regression coverage.
   - direct read/write and static-read paths still produce the expected result
     states and buffer recycle behavior.
4. Buffer-pool regression coverage.
   - read completion restores hot-page state correctly;
   - batch write completion still evicts pages and notifies waiters.
5. Redo-log regression coverage.
   - group commit preserves partial submit behavior, wait-for-all on shutdown,
     fsync ordering, and in-flight buffer reuse semantics.
6. Repository validation.
   - `cargo build -p doradb-storage`
   - `cargo nextest run -p doradb-storage`
   - `cargo clippy --all-features --all-targets -- -D warnings`

## Open Questions

1. The exact internal file split for the backend-specific driver layer can be
   decided during implementation as long as libaio ABI details remain below the
   generic `IOCompletion` boundary.
2. A later RFC-0010 phase still needs to add the `io_uring` backend and make
   it the default I/O mode on top of this completion core.
3. A later RFC-0010 phase still needs to define storage-I/O error policy so
   redo-log I/O, data-file I/O, and buffer-pool I/O do not share the current
   placeholder error handling.
