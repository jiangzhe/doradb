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
*   **Linting**: `cargo clippy --all-features --all-targets -- -D warnings` must pass.
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
*   **`libaio` Support**: Code must compile and run with `--no-default-features` (fallback to thread pool).
*   **Alignment**: Respect `crate::io::MIN_PAGE_SIZE` (4096 bytes) for Direct I/O buffers.

## 4. Testing

*   **Unit Tests**: Place in `mod tests` within the source file or in `tests.rs`.
*   **Dual-Pass Testing**: Run tests with both default features (`libaio`) and `--no-default-features`.

## 5. Unsafe Code

*   **Comments**: Every `unsafe` block **MUST** have a preceding `// SAFETY:` comment explaining the concrete invariants.
*   **Inventory**: Update the unsafe baseline if usage changes.

## 6. Development Checklist
- [ ] **Reliability**: Have you handled all error cases?
- [ ] **Performance**: Are there unnecessary allocations or locks?
- [ ] **Visibility**: Is this new method actually needed to be `pub`?
- [ ] **Documentation**: Do all public entities have `///` comments?
- [ ] **Locking**: Did you use `parking_lot` for small, fast blocking sections?
- [ ] **I/O**: Does it work with and without `libaio`?
