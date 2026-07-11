# Backlog: Optimize frozen-page checkpoint transition planning

## Summary

Optimize user-table frozen-page checkpoint transition after the cutoff-correctness redesign in task 000217. Reduce repeated undo-chain traversal and the duration of batch-wide page-state write locking without weakening stale-cutoff validation, deletion-overlay capture, LWC visibility, or atomic table-root publication. Evaluate checkpoint-local plan fusion first, then add commit-maintained page watermarks only if benchmarks justify foreground write-path overhead.

## Reference

Follow-up design discussion after implementing `docs/tasks/000217-redesign-checkpoint-frozen-page-cutoff-validation.md`, which resolves the correctness issue originally recorded in `docs/backlogs/000149-redesign-checkpoint-frozen-page-cutoff-validation.md`. The current successful checkpoint path can inspect the same undo chains during frozen-page validation, transition delete-marker capture, and transition bitmap construction. It also acquires every selected page-state write lock before validation and retains the full lock set through undo scans and transition marker capture. Relevant implementation areas are `doradb-storage/src/table/mod.rs`, `doradb-storage/src/table/persistence.rs`, `doradb-storage/src/row/vector_scan.rs`, `doradb-storage/src/trx/ver_map.rs`, and transaction commit-to-purge horizon publication.

## Deferred From (Optional)

`docs/tasks/000217-redesign-checkpoint-frozen-page-cutoff-validation.md`; `docs/backlogs/000149-redesign-checkpoint-frozen-page-cutoff-validation.md`

## Deferral Context (Optional)

- Defer Reason: Task 000217 is the correctness boundary: it replaces STS-based cutoff assumptions with actual undo-status validation and checked delayed outcomes. The proposed optimization changes page mutation bookkeeping, commit-to-purge publication ordering, transition concurrency, and foreground latency. It should remain a measured follow-up rather than expand the correctness task and its review surface.
- Findings: A GC horizon of 20 proves transactions with STS below 20 are terminal, but does not prove their committed changes are visible at 20; a writer may start at STS 10 and commit at CTS 30. Therefore `max_writer_sts` alone cannot authorize a raw checkpoint view. A correct watermark fast path also needs `max_committed_cts`, published before active-STS retirement. A separate mutation epoch is still required because repeated mutations by one transaction and rollback can invalidate a prepared bitmap without increasing `max_writer_sts`. Frozen pages prohibit new Insert or Update images but allow Lock and Delete overlay changes. This makes image readiness stable once proven while the transition bitmap and markers remain attempt-local. Holding all page-state locks closes the validation-to-transition race but is stronger than necessary: after whole-batch readiness, pages may transition individually because writers on transitioned pages already wait for cold-route publication and writers on remaining frozen pages can continue. Epoch mismatch should be treated as a page-local rebuild condition, not as a reason to wait for global quiescence.
- Direction Hint: Prefer a staged design. First introduce one transition analyzer and an owned prepared-page artifact, reuse it for LWC and secondary sidecar visibility, and transition pages individually after whole-batch readiness. Measure the reduction in undo scans and foreground delete blocking. Only then consider `PageModificationTracker` and commit-maintained CTS summaries for an O(1) raw-page fast path. Keep `mutation_epoch`, `max_writer_sts`, and `max_committed_cts` semantically separate even if stored in one tracker. Update committed CTS summaries before active-STS removal so `published_gc_horizon` remains the publication barrier; do not serialize global timestamp allocation. Avoid retaining page guards or `RowVersionMap` references in transactions. Preserve fatal fail-closed behavior after the first page enters `TRANSITION` and preserve atomic table-root publication.

## Scope Hint

Design, benchmark, and implement a lower-contention frozen-page transition pipeline. Introduce a checkpoint-local prepared page plan containing cutoff proof, an owned LWC deletion bitmap, staged overlay markers, and the page mutation epoch. Fuse undo-chain work so validation, marker selection, and bitmap construction share one analyzer. Separate reversible batch readiness from irreversible transition: validate and prepare one frozen page at a time, return delayed before any transition when an image is unsafe, then arm the publication guard and transition pages individually. Reuse a prepared plan when its epoch matches; otherwise rebuild under only that page state lock and immediately transition it. Evaluate an optional stable `Arc`-owned `PageModificationTracker` with `mutation_epoch`, `max_writer_sts`, and `max_committed_cts` for a raw-page fast path. Keep table-root, LWC, secondary sidecar, delete metadata, and allocation publication atomic.

## Acceptance Hint

Checkpoint correctness and recovery behavior remain equivalent to task 000217. A successful checkpoint does not repeatedly traverse each page undo chain for validation, marker capture, and LWC bitmap construction. Normal delayed attempts leave every selected page `FROZEN`. After irreversible transition starts, epoch mismatches rebuild only the affected page under its own lock and do not require a globally quiet delete window. Foreground deletes on other frozen pages continue while one page plan is rebuilt. Future or uncommitted Insert or Update images still delay before transition; future and unresolved Lock or Delete overlays remain representable. Tests cover unchanged-plan reuse, continuous frozen-page deletes, multiple mutations by one STS, statement and transaction rollback, commit CTS beyond cutoff, epoch mismatch during transition, partial transition routing waits, fatal post-transition failure, LWC and secondary-sidecar agreement, and restart recovery. Benchmarks compare checkpoint latency, foreground delete tail latency, and steady-state write overhead before enabling commit-maintained watermarks.

## Notes (Optional)

Proposed checkpoint-local artifact:

```rust
struct PreparedTransitionPage {
    page_id: PageID,
    epoch: u64,
    required_cutoff_ts: Option<TrxID>,
    del_bitmap: Vec<u64>,
    overlay_markers: Vec<(RowID, PendingDeleteMarker)>,
}
```

One undo-chain walk should trace leading Lock and Delete entries through the first Insert or Update image. It simultaneously computes the exclusive required cutoff, chooses the row deletion bit at the selected cutoff, and stages the first relevant committed or referenced overlay marker. Marker and bitmap artifacts are attempt-local because frozen-page deletes may continue between delayed attempts; only the immutable image cutoff proof is reusable.

Optional page tracker:

```rust
struct PageModificationTracker {
    mutation_epoch: AtomicU64,
    max_writer_sts: AtomicU64,
    max_committed_cts: AtomicU64,
}
```

The fields are complementary. `max_writer_sts` proves writer liveness relative to the published horizon. `max_committed_cts` proves commit visibility at the cutoff. `mutation_epoch` proves that a prepared page representation is still current; `max_writer_sts` cannot replace it because one transaction can change multiple rows with one STS and rollback can change a page without increasing STS. The raw physical-page fast path is valid only when both maximum timestamps are below cutoff. Commit CTS summaries must be updated before the transaction STS is retired from the active set and before the resulting GC horizon is published. Prefer retaining a deduplicated `Arc` tracker in transaction effects rather than pinning a page or retaining a `RowVersionMap` reference.

Recommended transition protocol:

1. Reversible readiness prepares pages under one page-state write lock at a time.
2. If any image is unsafe, update the batch cache and return delayed before any page transitions.
3. Once all image histories are safe, arm the transition publication guard.
4. For each page, acquire only its state write lock. Reuse the prepared plan when the epoch matches; otherwise rebuild the current Lock or Delete overlay and bitmap while holding that page lock. Set the page to `TRANSITION`, install staged markers, and release the lock.
5. Writers reaching transitioned pages wait for cold-route publication; writers reaching later frozen pages may continue deleting. The final table root remains the single atomic publication boundary.
6. Any unexpected image-producing change or failure after the first transition is fatal, matching the existing transition publication guard contract.

If simultaneous batch state transition is retained as an explicit invariant, use optimistic page-local preparation followed by a short all-page epoch verification and state flip. On mismatch, rebuilding changed pages while retaining all locks guarantees progress but restores batch-wide delete stalls; use it only as a fallback after bounded optimistic retries. The preferred direction is page-local irreversible transition after whole-batch readiness because current foreground routing already waits safely on `TRANSITION` pages.

Benchmark the checkpoint-local fused analyzer before adding tracker updates to foreground writes and commit or purge handoff. The local optimization has no steady-state transaction cost and may remove enough repeated work by itself.

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

## Close Reason

- Type: implemented
- Detail: Implemented by task 000219
- Closed By: backlog close
- Reference: docs/tasks/000219-optimize-frozen-page-checkpoint-transition-planning.md
- Closed At: 2026-07-11
