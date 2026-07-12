# Coding Guidance

This document outlines the coding standards, architectural patterns, and conventions for the `doradb` project. It is intended for both human contributors and AI agents to ensure consistency, safety, and performance across the codebase.

## 1. Philosophy & Core Principles

We follow a strict priority order for all engineering decisions:
1.  **Reliability**: The database must be correct and stable above all else.
2.  **Performance**: We are building a high-performance engine; overhead must be minimized.
3.  **Safety**: We leverage Rust's safety but allow `unsafe` where performance dictates, provided it's documented.
4.  **Features**: Functionality is secondary to the quality of the existing engine.

**Low Priority**: Backward compatibility is currently a low priority as the engine is in active, breaking development.

## 2. Code Style & Linting

We rely on tooling to enforce style.

*   **Formatting**: `cargo fmt` is the authority.
*   **Linting**: `cargo clippy --workspace --all-targets -- -D warnings` must pass.
*   **Imports & Type Names**: Prefer `use` imports plus short type names to keep code concise and readable. Use fully qualified type names only when they are actually needed, such as resolving name conflicts or clarifying ambiguous paths.
*   **Public API Documentation**: Every public `struct`, `trait`, `enum`, `const`, and `method/function` **MUST** have a descriptive `///` doc comment.
*   **Doc Placement**: Documentation comments (`///` or `//!`) must always be placed **above** any attributes (e.g., `#[derive(...)]`, `#[must_use]`).

## 3. Architecture & Patterns

### Module Organization & Visibility
*   **Minimize Exposure**: Do not expose unused public methods or functions. Keep visibility as restricted as possible (e.g., `pub(crate)`) unless a method is explicitly required by the public API design.
*   **Domain Grouping**: Group functionality by domain (e.g., `buffer`, `file`, `index`).

### Error Handling
*   **Use `crate::error::Result`**: Standardize on the project's `Result` type alias.
*   **Validation Pattern**: Use `crate::error::Validation<T>` for optimistic logic checks (Valid/Invalid) where failure is a normal control flow, distinct from `Result` (exceptional failures).
*   **No Panics**: Avoid `unwrap()` / `expect()` in runtime paths.

### Concurrency & Locking
*   **Blocking Locks**: Use `parking_lot::Mutex` and `parking_lot::RwLock` for blocking operations.
    *   **Rule**: Blocking locks must only be used for **very small and fast** operations (e.g., updating a counter, small state change).
*   **Async Locks**: Use `crate::latch::Mutex` (which supports `lock_async()`) or `async_lock::RwLock` for operations that may involve async waiting.
*   **Avoid Blocking**: In async contexts, never block the thread for long periods.

### I/O Abstraction
*   **Use `crate::io`**: All file I/O must go through the `crate::io` module.
    *   **Do not** use `std::fs` or `tokio::fs` for data path operations.
*   **Compile-Time Backends**: `crate::io` supports compile-time-selected `io_uring` and `libaio` backends. `io_uring` is the repository default, and `libaio` remains the explicitly supported alternate path for older kernels that cannot run `io_uring`.
*   **Alignment**: Respect `crate::io::STORAGE_SECTOR_SIZE` (4096 bytes) for Direct I/O buffers.

## 4. Testing

*   **Unit Test Structure**: Prefer inline `#[cfg(test)] mod tests` beside the code under test. Use `tests.rs` when a module is already organized that way.
*   **Test-Only Code**: Keep test helpers close to tests and behind `#[cfg(test)]`. If another module needs them, use a narrow `#[cfg(test)] pub(crate)` re-export instead of widening the production API.
*   **Production Shape First**: Do not widen or complicate production structs, traits, or control flow solely for tests. Prefer production execution paths and minimal `#[cfg(test)]` hooks when extra test control is required.
*   **Unit Test Dedup Review**: Every unit-test update should include a final pass that extracts common reusable utilities when new or changed tests copy-paste setup, execution, or assertions.
*   **Unit Test Dedup Patterns**: Extract helper functions for repeated object construction, round-trip flows such as `encode -> decode -> verify`, and common assertion sequences. Use table-driven tests with a case struct or array plus loop when cases share logic and differ only by input or expected output.
*   **Concurrent Tests**: Synchronize on the semantic predicate under test, not elapsed time or unrelated monotonic progress. Establish prerequisite gates in production order before arranging the target race. Use production wait APIs, hooks, channels, or barriers, and recheck the predicate after every notification because a notification is only a wake hint. Reserve timeouts for hang watchdogs and negative assertions; do not use `sleep` to make progress.
    *   Example: a checkpoint test that intends to observe `FrozenPageCutoff` must first clear an earlier `ActiveRoot` delay. A test that only needs successful publication should use the retrying checkpoint API instead of assuming purge has already advanced.
*   **Randomized Tests**: Prefer randomized tests over exhaustive parameter permutations when broad input variation is useful. Keep deterministic edge-case tests separate from randomized tests, especially for error paths, boundary conditions, and format verification.
*   **Routine Validation**: Run `cargo nextest run --workspace`.
*   **Alternate Backend Validation**: Run `cargo nextest run -p doradb-storage --no-default-features --features libaio` manually when you need to validate the legacy-kernel alternate backend path.
*   **Doc Tests**: This project currently does not have doctests, and routine validation does not run `cargo test --doc`.

## 5. Unsafe Code

*   **Unsafe Blocks and Impls**: Every `unsafe` block and `unsafe impl` **MUST** have a preceding `// SAFETY:` comment explaining the concrete invariants.
*   **Public Unsafe Functions**: Every public `unsafe fn` **MUST** document its caller contract in a `/// # Safety` section. Do not replace that function-level contract with an adjacent `// SAFETY:` comment on the signature itself.
*   **Mechanical Gate**: Production crate roots enable `#![warn(clippy::undocumented_unsafe_blocks)]`, and `cargo clippy --workspace --all-targets -- -D warnings` turns violations there into hard failures. Any new production target crate should add the same crate-level lint.
*   **Inventory**: Update the unsafe baseline if usage changes.

## 6. Development Checklist

Use the standalone [Development Checklist](dev-checklist.md) before submitting or
reviewing an implementation.
