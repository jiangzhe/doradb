# Development Checklist

Use this checklist before submitting or reviewing an implementation. The task
document is the source of truth for intended behavior when one exists.

## Reliability

- [ ] Compare the tests against the task document and confirm the specified
      behavior, edge cases, and failure modes are covered.
- [ ] Run the normal validation pass:
      `cargo nextest run -p doradb-storage`.
- [ ] Run focused coverage for changed code with:
      `tools/coverage_focus.rs --path <path/to/file/or/dir>` or repeated
      `--path` flags for multiple changed paths.
- [ ] Confirm focused coverage is at least 80% for changed files or
      directories. If it is lower, document the reason and the follow-up needed.
- [ ] For central definition-heavy files, such as shared error/type enums whose
      primary role is declarations rather than executable control flow, a lower
      whole-file focused coverage result is acceptable only when review notes
      explain why the file fits that category and cite at least one affected
      consumer/runtime path whose focused coverage still meets the 80% bar.
- [ ] Check that errors are propagated with `crate::error::Result` where
      appropriate and that runtime paths do not rely on `unwrap()` or
      `expect()`.

## Security

- [ ] For every change that adds or modifies unsafe code, complete
      [Unsafe Review Checklist](unsafe-review-checklist.md).
- [ ] Confirm each new or modified unsafe block and unsafe impl has a concrete
      `// SAFETY:` comment.
- [ ] Confirm each public `unsafe fn` has a `/// # Safety` section describing
      caller obligations.
- [ ] Refresh the unsafe inventory when required by the unsafe review checklist.

## Performance

- [ ] Check synchronization primitives and lock scope. Blocking locks must stay
      small and fast, and async contexts must not block for long periods.
- [ ] Check IO paths for avoidable bottlenecks, extra syscalls, unnecessary
      buffering, or violations of the repository IO abstraction.
- [ ] Check whether simple data reduction, fewer allocations, fewer copies, or
      less recomputation can reduce work.
- [ ] Check algorithmic complexity and whether a better obvious algorithm is
      available for the expected data shape.
- [ ] Check whether parallelism or batching can improve the path without
      complicating correctness.
- [ ] Collect and report simple, obvious performance improvements so the
      developer can implement them immediately or defer them.
- [ ] Report long-term feasible performance improvements and create backlog
      follow-ups when they are deferred.

## Feature Completeness

- [ ] Compare the implementation against the task document's required behavior,
      non-goals, and acceptance criteria.
- [ ] Record any discrepancy, the reason for it, and whether it is intentional,
      deferred, or a bug.
- [ ] Confirm unchanged behavior that the task document explicitly protects is
      still preserved.

## Documentation

- [ ] Every `pub` and `pub(crate)` struct, enum, trait, const, method, and
      function has a descriptive `///` comment.
- [ ] Every trait has a descriptive comment that explains its role and contract.
- [ ] Core logic introduced or changed by the task has inline comments around
      non-obvious invariants, phases, or control flow.
- [ ] Documentation comments are placed above attributes.
- [ ] Related conceptual documentation is updated when behavior or architecture
      changes. Keep this at the concept level rather than duplicating code.

## Test-Only Code

- [ ] Keep test-only helpers, hook types, and setup utilities inside
      `#[cfg(test)] mod tests` by default.
- [ ] Prefer tests that exercise production code paths over test-only harnesses.
- [ ] Use test-only hooks only when production paths cannot reasonably express
      timing, fault injection, or other required control.
- [ ] If cross-module test reuse is unavoidable, prefer narrow
      `#[cfg(test)] pub(crate) use ...::tests::{...};` re-exports over widening
      the production API.

## Complexity

- [ ] Treat function bodies over roughly 60 lines as a review trigger.
- [ ] Split large functions into smaller helpers when that improves readability
      without obscuring data flow or creating excessive argument lists.
- [ ] If a large function should remain intact, add inline comments that mark
      the major steps and explain the relevant invariants.
- [ ] Define a trait only when it has at least two concrete implementations.
      Otherwise, prefer a plain struct with methods.
- [ ] Add a single-field wrapper only with a specific intent. Document that
      intent in a comment, even when the wrapper is internal-only, so the code
      stays concise.
- [ ] After implementation, compare the changed code with existing snippets
      that do the same or a similar thing. Extract and call a shared method or
      function when suitable; if sharing is not suitable, leave a comment that
      explains why.
