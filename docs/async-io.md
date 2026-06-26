# Async I/O

This document describes the storage engine's asynchronous I/O model in
`doradb-storage`, the supported compile-time backends, and the main integration
points that depend on the shared completion core.

## Overview

The storage engine uses backend-neutral completion-driven direct I/O. The
generic `crate::io` layer owns:

- in-flight slot allocation and completion-token validation;
- operation ownership for direct buffers and borrowed page pointers;
- submission batching and completion dispatch into subsystem state machines; and
- per-driver submit/wait statistics.

Backend-specific code only prepares kernel submission objects, stages them into
the backend's submission format, submits batches, and decodes completions back
into worker tokens.

In the current runtime topology, the storage engine uses one shared
storage-adjacent worker plus one redo driver owned by the transaction log
thread:

- one shared `StorageIOWorker` for table-file reads/writes, readonly-cache miss
  loads, and `mem_pool` / `index_pool` page IO; and
- one scheduler-owned redo backend driver running inside `Log-Thread` for
  transaction-log writes.

The shared storage worker exposes three logical lanes:

- `table_reads`
- `pool_reads`
- `background_writes`

## Ownership Model

One `crate::io::Operation` describes a single read, write, or file sync:

- `Operation::pread_owned(...)` and `Operation::pwrite_owned(...)` transfer an
  owned `DirectBuf` into the worker until completion.
- `Operation::pread_borrowed(...)` and `Operation::pwrite_borrowed(...)` bind a
  borrowed page-aligned pointer whose lifetime higher layers keep valid until
  completion is observed exactly once.
- `Operation::fsync(fd)` and `Operation::fdatasync(fd)` bind no memory and
  complete only after the backend reports the native file-sync operation.

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

Schedulers remain backend-neutral. The shared storage service uses
domain-specific state-machine methods to decide how requests become
submissions and how completions update subsystem state. The redo path uses a
small `LogWriteDriver` wrapper around the shared submission driver for direct
log writes.

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
- redo-log writes and syncs in `src/log/`.

Table-file and buffer-pool traffic share one storage service, while redo keeps
its own scheduling and durability policy inside `Log-Thread`.

## Redo Path

Redo-log writes and durability syncs use the backend-neutral submission driver
inside `Log-Thread`:

- the scheduler serializes one commit group into a `DirectBuf`;
- the group becomes one `Operation::pwrite_owned(...)`;
- after the contiguous write prefix completes, `RedoLogWriter` submits a native
  `Operation::fsync(...)` or `Operation::fdatasync(...)` when
  `TrxSysConfig::log_sync` requires it; and
- ordered transaction publication happens only after the matching sync
  completion succeeds. `log_sync = none` skips the backend sync operation.

Fatal redo write or sync failures poison runtime admission through
`FatalError::{RedoWrite, RedoSync}`.

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
