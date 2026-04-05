# Async I/O

This document describes the storage engine's asynchronous I/O model in
`doradb-storage`, the supported compile-time backends, and the main integration
points that depend on the shared completion core.

## Overview

The storage engine uses one backend-neutral completion-driven worker model for
direct I/O. The generic `crate::io` layer owns:

- request receipt from subsystem-specific channels;
- in-flight slot allocation and completion-token validation;
- operation ownership for direct buffers and borrowed page pointers;
- submission batching and completion dispatch into subsystem state machines; and
- per-worker submit/wait statistics.

Backend-specific code only prepares kernel submission objects, stages them into
the backend's submission format, submits batches, and decodes completions back
into worker tokens.

In the current runtime topology, the storage engine uses two storage-adjacent
workers:

- one shared `StorageIOWorker` for table-file reads/writes, readonly-cache miss
  loads, and `mem_pool` / `index_pool` page IO; and
- one dedicated redo-log worker for transaction-log writes.

The shared storage worker exposes three logical lanes:

- `table_reads`
- `pool_reads`
- `background_writes`

## Ownership Model

One `crate::io::Operation` describes a single read or write:

- `Operation::pread_owned(...)` and `Operation::pwrite_owned(...)` transfer an
  owned `DirectBuf` into the worker until completion.
- `Operation::pread_borrowed(...)` and `Operation::pwrite_borrowed(...)` bind a
  borrowed page-aligned pointer whose lifetime higher layers keep valid until
  completion is observed exactly once.

The completion core preserves two invariants:

1. submitted memory remains valid until the backend reports completion; and
2. each completion token maps back to exactly one in-flight worker slot.

That is the shared contract across both supported backends.

## Backend Contract

`crate::io::IOBackend` is the backend boundary. Each implementation provides:

- one prepared submission type;
- one backend-owned submit-batch type;
- one backend-owned completion-event buffer type;
- translation from `Operation` to the backend submission format; and
- batch submit plus completion wait methods that return `BackendToken`s.

The worker itself remains backend-neutral. Subsystems bind domain-specific
state machines through `IOWorkerBuilder::bind(...)` and implement
`IOStateMachine` to decide how requests become submissions and how completions
update subsystem state.

## Supported Backends

Two compile-time backends are supported:

- `io_uring`
  - repository default;
  - selected by the default Cargo feature set;
  - validated by `cargo nextest run -p doradb-storage`.
- `libaio`
  - explicitly supported alternate backend for older Linux kernels that cannot
    use `io_uring`;
  - selected with
    `cargo nextest run -p doradb-storage --no-default-features --features libaio`.

Exactly one backend feature must be enabled at compile time.

## Integration Points

The current storage-engine integration points are:

- table-file and catalog-file reads/writes in `src/file/`;
- readonly-cache miss loads in `src/buffer/readonly.rs`;
- evictable-pool page reads and writeback in `src/buffer/evict.rs`; and
- redo-log writes in `src/trx/log.rs`.

Table-file and buffer-pool traffic now share one storage service, while redo
retains its own dedicated worker and durability policy.

## Redo Path

Redo-log writes use a dedicated backend-neutral worker:

- the scheduler serializes one commit group into a `DirectBuf`;
- the group becomes one `Operation::pwrite_owned(...)`;
- the redo worker reports completion back to the scheduler thread; and
- durability is finalized above the worker with `fsync`, `fdatasync`, or no
  sync depending on `TrxSysConfig::log_sync`.

Fatal redo submit, write, or sync failures poison runtime admission through
`StoragePoisonSource::{RedoSubmit, RedoWrite, RedoSync}`.

## Telemetry

Two layers of runtime-local storage telemetry are available:

- `FileSystem::io_backend_stats()` reports backend-owned submit/wait activity.
- `FileSystem::storage_service_stats()` reports shared-service ingress counts
  and scheduler turns per lane.

The shared pool evictor keeps separate wake/wait and per-domain execution
counters through its component access handle, so diagnostics can distinguish
shared-worker fairness from raw backend saturation.

## Operational Notes

- Linux direct I/O still requires aligned buffers and offsets.
- `libaio1` and `libaio-dev` remain required for environments that validate or
  build the alternate `libaio` backend.
- The initial phase-6 performance bar is manual: compare the default
  `io_uring` path against explicit `libaio` builds using the existing storage
  examples on the same machine.
