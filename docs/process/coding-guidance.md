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
*   **Linting**: `cargo clippy -p doradb-storage --all-targets -- -D warnings` must pass.
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
*   **Compile-Time Backends**: `crate::io` supports compile-time-selected `libaio` and `io_uring` backends. `libaio` remains the default in this repository, and `io_uring` is the non-default alternate path.
*   **Alignment**: Respect `crate::io::MIN_PAGE_SIZE` (4096 bytes) for Direct I/O buffers.

## 4. Testing

*   **Unit Tests**: Place in `mod tests` within the source file or in `tests.rs`.
    *   Prefer inline `#[cfg(test)] mod tests` in the same file as the code under test.
    *   Keep test-only helpers and hook types inside that inline test module unless there is a strong reason to share them more broadly.
    *   If cross-module test reuse is needed, prefer a narrow `#[cfg(test)] pub(crate) use ...::tests::{...};` re-export instead of expanding the production API or adding standalone test-support modules.
*   **Production Shape First**: Do not widen or complicate production structs, traits, or control flow solely for tests. Prefer adapting tests to the production path, and use minimal `#[cfg(test)]` branches when test-only control is required.
*   **Routine Validation**: Run `cargo nextest run -p doradb-storage`.
*   **Alternate Backend Validation**: Run `cargo nextest run -p doradb-storage --no-default-features --features iouring` manually when you need to validate the non-default backend path.
*   **Doc Tests**: This project currently does not have doctests, and routine validation does not run `cargo test --doc`.

## 5. Unsafe Code

*   **Comments**: Every `unsafe` block **MUST** have a preceding `// SAFETY:` comment explaining the concrete invariants.
*   **Inventory**: Update the unsafe baseline if usage changes.

## 6. Development Checklist
- [ ] **Reliability**: Have you handled all error cases?
- [ ] **Performance**: Are there unnecessary allocations or locks?
- [ ] **Visibility**: Is this new method actually needed to be `pub`?
- [ ] **Documentation**: Do all public entities have `///` comments?
- [ ] **Locking**: Did you use `parking_lot` for small, fast blocking sections?
- [ ] **I/O**: Does it preserve the supported `libaio` backend contract?
