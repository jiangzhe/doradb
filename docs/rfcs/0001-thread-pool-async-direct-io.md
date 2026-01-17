---
id: 0001
title: Thread Pool Backed Async Direct IO Framework
status: proposal
tags: [io, async, directio]
created: 2026-01-17
---

# RFC-0001: Thread Pool Backed Async Direct IO Framework

## Summary

This RFC proposes providing a fallback **Direct I/O** implementation for development and testing on environments where `libaio` is not available. The primary, high-performance `libaio` backend will remain the default for production. The fallback will be a thread pool-based backend that uses standard `pread(2)` and `pwrite(2)` syscalls.

This will be achieved by using conditional compilation based on the `libaio` feature flag to select the appropriate I/O event loop driver at compile time. This approach ensures maximum safety and performance for the production path by avoiding any runtime abstraction costs, while still enabling portability for development.

## Context

Currently, `doradb-storage` relies on `libaio` for all asynchronous file I/O. This is implemented in the `doradb-storage/src/io` module. The dependency on `libaio` is guarded by a feature flag, but there is no alternative implementation. If the `libaio` feature is disabled, I/O operations fail at runtime.

This poses a significant limitation for portability and development environments. For example, macOS does not support `libaio`, and some Linux environments may not have the `libaio-dev` package installed. A portable fallback is needed for these scenarios.

## Design Considerations: Why a Trait-Based Abstraction Was Rejected

An initial design considered introducing an `AsyncDirectIO` trait to abstract the I/O backend:

```rust
// REJECTED DESIGN
pub trait AsyncDirectIO {
    fn pread(&self, ...) -> impl Future<Output = ...>;
    fn pwrite(&self, ...) -> impl Future<Output = ...>;
}
```

This approach, while idiomatic for high-level async Rust, was **rejected due to critical safety concerns**. Kernel-level async interfaces like `libaio` and `io_uring` operate on raw pointers to buffers. An async `Future` in Rust can be dropped at any `.await` point. If a `Future` wrapping a `libaio` call were dropped, the Rust runtime could deallocate the buffer while the kernel is still performing the I/O operation on its raw pointer. This would lead to a severe use-after-free bug.

The existing codebase correctly handles this by using wrapper types (`AIO` and `UnsafeAIO`) that manage buffer ownership and are kept alive for the entire duration of the I/O operation by the `AIOEventLoop`/`AIOEventListener` architecture. A simple `Future`-based trait hides this essential safety mechanism. The chosen design preserves this safe and proven ownership model.

## Decision

We will use **conditional compilation** to provide two distinct implementations of the `AIOEventLoop` driver, sharing the same high-level abstractions (`AIOEventListener`, `AIOClient`, `AIO` wrappers).

The selection will be based on the `libaio` cargo feature.

### 1. `#[cfg(feature = "libaio")]` (Production Path)

- The existing `AIOEventLoop` and its `run` method will be used **unchanged**.
- It will continue to use the `AIOContext` to interact directly with the `libaio` kernel interface for maximum performance.
- This ensures the production path has zero abstraction cost.

### 2. `#[cfg(not(feature = "libaio"))]` (Dev/Test Fallback Path)

- A new `AIOEventLoop::run` implementation will be provided for this configuration.
- This implementation will **not** use `AIOContext`. Instead, it will manage a thread pool (e.g., using `smol::blocking::spawn`).
- The loop will faithfully follow the existing high-level logic:
    1. It receives a high-level request (e.g., `PoolRequest`, `FileIO`) via the `AIOMessage` channel.
    2. It calls `listener.on_request()`, which prepares a `Submission` object. This object correctly wraps the buffer using the established pattern for its type (a reference for `'static` buffer pool pages, or an `Arc<Mutex<...>>` for dynamically allocated `FileIO` buffers).
    3. The entire `Submission` object is moved into a blocking task on the thread pool. This correctly transfers the `Arc` or reference, ensuring the buffer remains valid during the I/O operation.
    4. The task performs a standard blocking `pread(2)` or `pwrite(2)` syscall.
    5. Upon completion, the task sends the result (`AIOKey` and `io::Result<usize>`) back to the main event loop thread.
    6. The event loop calls `listener.on_complete()` with the result. This final step is crucial as it triggers the necessary notifications that waiting `FileIO` callers depend on.

This design ensures the fallback path correctly participates in the established buffer management and synchronization protocol.

This design ensures buffer ownership is safely managed in both configurations, leveraging the existing `AIO` wrapper pattern.

## Implementation Plan

1.  **Modify `doradb-storage/src/io/mod.rs`**:
    -   Enclose the existing `AIOEventLoop::run` method and any `libaio`-specific helper functions within `#[cfg(feature = "libaio")]`.
    -   Implement the new thread-pool-based `AIOEventLoop::run` method within `#[cfg(not(feature = "libaio"))]`. This will involve:
        -   Setting up a mechanism to dispatch tasks to a thread pool.
        -   Creating a channel to receive completion results from the thread pool tasks.
        -   A loop that receives `AIOMessage`s, dispatches them, and processes completions from the channel.
2.  **Update `Cargo.toml`**: Ensure dependencies required for the thread pool (like `smol`) are correctly configured.
3.  **Testing**: Add CI steps to run the test suite with and without the `libaio` feature to ensure both backends are functional.

## Consequences

### Positive
- **Guaranteed Safety:** The chosen design correctly handles buffer lifetimes, avoiding the use-after-free danger present in a naive trait-based abstraction.
- **Zero Abstraction Cost:** The production `libaio` path is completely untouched and runs with maximum performance.
- **Improved Portability:** DoraDB can be compiled and run on systems without `libaio` for development and testing.
- **Simplified Development:** Developers can build and test DoraDB without needing to install `libaio-dev`.

### Negative
- **Code Duplication:** There will be two distinct `AIOEventLoop::run` implementations. This is a reasonable trade-off for the significant gains in safety and performance isolation.

## Future Work

- **`io_uring` Backend:** This design keeps the high-performance path clean, making it straightforward to replace the `libaio` implementation with a superior `io_uring` implementation in the future under the same `#[cfg]` block.

## References

- [Linux `pread(2)` man page](https://man7.org/linux/man-pages/man2/pread.2.html)
- [Linux `pwrite(2)` man page](https://man7.org/linux/man-pages/man2/pwrite.2.html)
- [Linux `open(2)` man page (see O_DIRECT)](https://man7.org/linux/man-pages/man2/open.2.html)

