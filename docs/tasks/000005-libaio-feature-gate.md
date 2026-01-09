# Task: Feature Gate for libaio

## Summary

Add a feature gate `libaio` to `doradb-storage` crate. This allows compiling and running tests in environments where the system library `libaio` is not installed (e.g. some cloud-based code agent sandboxes) by disabling this feature.

## Context

`doradb-storage` currently relies on the Linux native AIO library (`libaio`) for asynchronous I/O operations. It links against this library unconditionally using `#[link(name = "aio")]`. However, some development or testing environments (specifically cloud-based ephemeral containers) may not have `libaio` installed and may not allow installing system packages. To support development in such environments, we need a way to opt-out of linking against `libaio`.

## Goals

*   Add a `libaio` feature to `doradb-storage/Cargo.toml`.
*   Enable `libaio` feature by default to preserve existing behavior.
*   Provide stub implementations for `libaio` ABI functions when the feature is disabled.
*   Ensure `cargo build` and `cargo test` work with `--no-default-features` (or strictly excluding `libaio`) in an environment without `libaio`.
*   Tests that do not depend on `libaio` functionality should pass when the feature is disabled.
*   Tests that depend on `libaio` should fail gracefully (or panic) if run with the feature disabled, rather than failing to link.

## Non-Goals

*   Implement a fallback mechanism for Async I/O (e.g., using a thread pool) when `libaio` is missing. This is purely for compilation and running non-IO dependent tests.

## Plan

1.  **Modify `doradb-storage/Cargo.toml`**:
    *   Add `libaio = []` to `[features]`.
    *   Add `libaio` to `default` features.

2.  **Modify `doradb-storage/src/io/libaio_abi.rs`**:
    *   Guard the `#[link(name = "aio")]` and the `extern "C"` block with `#[cfg(feature = "libaio")]`.
    *   Add a `#[cfg(not(feature = "libaio"))]` block.
    *   In the `not(feature = "libaio")` block, implement stub functions with the same signatures as the extern functions.
    *   Stub implementations:
        *   `io_setup`: Return a non-zero error code (e.g., `-ENOSYS` or similar) to indicate failure.
        *   `io_submit`: Return a negative error code.
        *   `io_queue_init`, `io_queue_release`, `io_queue_run`, `io_destroy`, `io_cancel`, `io_getevents`: Return negative error codes.
    *   Ensure that `io_context_t` and other types are still available or compatible.

3.  **Verification**:
    *   Run `cargo test -p doradb-storage --no-default-features` to ensure it compiles and runs (even if some tests fail due to missing AIO).
    *   Run `cargo test -p doradb-storage` (default) to ensure it still works as expected.

## Impacts

*   `doradb-storage/Cargo.toml`
*   `doradb-storage/src/io/libaio_abi.rs`

## Open Questions

*   None.
