# Backlog: Investigate allocation and deallocation performance in hot paths

## Summary

Investigate how to improve allocation and deallocation performance in storage hot paths. The work should first determine whether a simple global allocator switch, such as `jemalloc` or `mimalloc`, removes most of the observed overhead. Scoped arenas remain a possible follow-up strategy only if global allocator alternatives do not address the main costs or if specific short-lived allocation groups still dominate.

## Reference

During task `docs/tasks/000170-table-handle-catalog-ownership.md`, `weak_handle_baseline` flamegraph work exposed allocation/deallocation sensitivity in hot statement paths. The concrete `Vec<LockResource>` temporary in `OwnerLockState::release_all` was optimized separately, but the broader question remains whether allocator overhead still dominates hot paths and what the lowest-effort effective mitigation is.

The first experiment should compare the current global allocator with performance-oriented global allocators, at least `jemalloc` and `mimalloc`. If one of those solves most of the allocation/deallocation cost without unacceptable binary, portability, build, or operational tradeoffs, prefer the global allocator switch because it is simpler, broad, and requires less API churn. If global allocator switching is insufficient, then investigate scoped allocation strategies such as session-level, transaction-level, statement-level, or hybrid arenas.

## Deferred From (Optional)

docs/tasks/000170-table-handle-catalog-ownership.md

## Deferral Context (Optional)

- Defer Reason: The current task is focused on table handle/catalog ownership and immediate weak-handle hot-path findings. Allocation/deallocation performance is a larger investigation because it may involve global allocator selection, benchmark design, ownership and destructor behavior, memory accounting, and API shape across multiple subsystems.
- Findings: The current profiling already showed allocator costs can matter in hot paths: removing a temporary `Vec<LockResource>` in `OwnerLockState::release_all` eliminated its `RawVec<LockResource>::deallocate` flamegraph frame and improved `cached_resolution_empty_scan` from about 434 ns to about 413 ns. That does not prove scoped arenas are necessary; it proves allocator behavior should be measured before choosing a mitigation.
- Direction Hint: Treat this as investigation-first. Start with global allocator experiments because they are the lowest-churn option. Compare the current allocator against at least `jemalloc` and `mimalloc` using `weak_handle_baseline`, flamegraphs, and representative storage tests. Prefer a global allocator switch if it solves most observed costs with acceptable tradeoffs. Only if that is insufficient, measure allocation sites and evaluate scoped allocator designs. For scoped allocation, explicitly audit customized `Drop` implementations and resource-owning types, compare statement-level, transaction-level, session-level, and hybrid lifetimes, avoid widespread API churn by exploring allocator access through existing Session/Transaction/Statement state, and include a hard memory budget plus exceed policy.

## Scope Hint

Do not start with a repo-wide arena migration. First benchmark allocator behavior in the current hot paths:

- Establish the current allocator baseline with `weak_handle_baseline`, relevant empty-scan/point-lookup paths, and flamegraphs.
- Prototype global allocator alternatives, at least `jemalloc` and `mimalloc`, and compare latency, variance, CPU samples, binary/build impact, platform support, and operational risk.
- Decide whether a global allocator switch solves enough of the issue to be the preferred implementation path.
- If global allocator alternatives do not solve enough, profile and inventory remaining small allocations in statement, transaction, session, lock, catalog-cache, and scan/update paths.
- For scoped allocation follow-up, evaluate statement, transaction, session, and hybrid lifetimes; identify types that cannot be arena-dropped safely because they rely on custom `Drop` or own external resources; propose how allocator access would flow through existing Session/Transaction/Statement structures with minimal signature churn; and define memory-limit behavior when a scope exceeds its budget.

## Acceptance Hint

Produce an implementation-ready task or RFC decision with measured allocator costs by hot path and a recommendation among:

- Keep the current allocator and optimize specific allocation sites.
- Switch to a global allocator such as `jemalloc` or `mimalloc`.
- Pursue scoped allocator/arena work for remaining unsolved hot allocations.

The decision should include benchmark results, flamegraph comparisons, build/platform tradeoffs for allocator alternatives, and the expected maintenance/API impact. If scoped allocation remains recommended, include allocation counts/sizes by hot path, arena-safe and arena-unsafe object categories, a recommended allocator scope model, an API propagation plan, and a bounded-memory policy. Any prototype should preserve `cargo check -p doradb-storage` and `cargo nextest run -p doradb-storage`, and should compare `weak_handle_baseline` timings/flamegraphs before and after the prototype.

## Notes (Optional)


## Close Reason (Added When Closed)

When a backlog item is moved to `docs/backlogs/closed/`, append:

```md
## Close Reason

- Type: <implemented|stale|replaced|duplicate|wontfix|already-implemented|other>
- Detail: <reason detail>
- Closed By: <backlog close>
- Reference: <task/issue/pr reference>
- Closed At: <YYYY-MM-DD>
```
