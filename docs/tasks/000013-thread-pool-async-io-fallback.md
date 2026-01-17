# Task: Implement Thread Pool Async IO Fallback

## Summary

This task is to implement the plan outlined in RFC-0001 (as amended). It involves creating a thread-pool-based I/O fallback for environments where `libaio` is not available (e.g., macOS, or Linux without `libaio-dev`). This will be achieved by providing a second implementation of the `AIOEventLoop::run` method that is conditionally compiled when the `libaio` feature is disabled.

## Context

The primary goal is to improve portability for development and testing. As decided in RFC-0001, a trait-based abstraction was rejected due to critical safety issues related to buffer lifetimes with kernel-level async I/O. The chosen approach uses conditional compilation to provide a separate "driver" for the `AIOEventLoop`, which preserves the project's existing and proven buffer ownership and synchronization patterns (`AIO` wrappers, `AIOEventListener` logic).

## Goals

*   In `doradb-storage/src/io/mod.rs`, provide a new implementation of `AIOEventLoop::run` under `#[cfg(not(feature = "libaio"))]`.
*   This new implementation must use a thread pool to execute blocking `pread(2)` and `pwrite(2)` calls.
*   It must correctly participate in the existing buffer management protocol by calling the `AIOEventListener`'s `on_request` and `on_complete` methods.
*   The existing `libaio`-based `run` implementation should be enclosed in `#[cfg(feature = "libaio")]` and remain functionally unchanged.

## Non-Goals

*   Modifying the production `libaio` implementation.
*   Changing the `AIOEventListener` trait or any of its existing implementations.
*   Changing the `AIO`, `UnsafeAIO`, `IOQueue`, or `AIOClient` abstractions.

## Plan

The implementation will be focused within `doradb-storage/src/io/mod.rs`.

1.  **Isolate Existing Code**: Wrap the current `AIOEventLoop::run` method and its `libaio`-specific dependencies (like `AIOContext`) within a `#[cfg(feature = "libaio")]` block.

2.  **Implement Fallback `run` Method**: Create a new `impl<T> AIOEventLoop<T>` block under `#[cfg(not(feature = "libaio"))]` and add the `run` method.
    *   **State**: The loop will need a way to receive completions from the thread pool. A `flume` channel is a good choice.
        ```rust
        // Inside run()
        let (completion_tx, completion_rx) = flume::unbounded();
        ```
    *   **Main Loop**: The loop must handle two asynchronous sources: new requests from `self.rx` and completions from `completion_rx`. A `select` operation is ideal for this.
    *   **Request Handling**:
        - When a new `AIOMessage::Req(req)` is received, call `listener.on_request(req, &mut queue)` as is done currently.
        - Iterate through the prepared `Submission` objects in the `queue`.
        - For each `Submission`, spawn a blocking task (e.g., `smol::blocking::spawn`).
        - The task will move the `Submission` and a cloned `completion_tx`.
        - Inside the task, perform the blocking I/O using `std::os::unix::fs::FileExt::read_at` or `write_at` on the file descriptor and buffer from the `Submission`.
        - After I/O, the task sends the result (`AIOKey` and `io::Result`) back via the `completion_tx`.
    *   **Completion Handling**:
        - When a result is received on `completion_rx`, call `listener.on_complete()` with the key and result. This ensures the correct notification logic is triggered for `FileIO` callers.
    *   **Shutdown**: The loop should terminate gracefully when a `Shutdown` message is received and all in-flight I/O operations have completed.

## Impacts

*   **`doradb-storage/src/io/mod.rs`**: This file will be the primary focus of all changes. It will contain both the `libaio` and thread-pool implementations of the `AIOEventLoop`'s core logic, separated by `cfg` flags.
*   **`doradb-storage/Cargo.toml`**: May need to ensure dependencies like `smol` or `flume` are not just dev-dependencies if they are used in the fallback path.