# Backlog: Purge row undo chains through resident version metadata

## Summary

Allow undo-chain purge to access resident row-version metadata without reloading evicted row-page contents. The initial change covers chain pruning only, reducing unnecessary swap reads and cache pressure while preserving generation safety, checkpoint coordination, and unlink-before-free ownership.

## Reference

- Review of [task 000303](../tasks/000303-repair-dangling-row-undo-and-deferred-lock-to-delete-mismatch.md): the user proposed separating cleanup that only touches version chains from physical row access and selected undo-chain purge as the starting point.
- [Purge driver](../../doradb-storage/src/trx/purge.rs): `purge_trx_list_inner()` loads exact page generations and `purge_undo_chain_from_page()` constructs row write access.
- [Row access](../../doradb-storage/src/trx/row.rs): `purge_undo_chain()` changes only chain metadata; `RowWriteAccess` currently provides its row latch, page-state guard, and Frozen mutation bookkeeping.
- [Buffer eviction](../../doradb-storage/src/buffer/evict.rs), [frame context](../../doradb-storage/src/buffer/frame.rs), and [version map](../../doradb-storage/src/trx/ver_map.rs) establish the residency and lifetime constraints.
- [Checkpoint page transition](../../doradb-storage/src/table/page_transition.rs) consumes version chains and the Frozen mutation counter when preparing and validating plans.

## Deferred From (Optional)

[Task 000303](../tasks/000303-repair-dangling-row-undo-and-deferred-lock-to-delete-mismatch.md), implementation review in worktree `.worktrees/000303`.

## Deferral Context (Optional)

- Defer Reason: Task 000303 repairs rollback correctness. Avoiding page reloads requires a new buffer metadata accessor and row identity representation, so the user requested a separate backlog item starting with undo-chain purge.
- Findings:
  `RowVersionMap` is stored in `BufferFrame::ctx`, and undo nodes remain in transaction-owned memory. Eviction discards page bytes but preserves the context and generation. However, `get_page_versioned()` currently reloads evicted contents before returning access.

  `purge_undo_chain()` only updates `purge_ts`, statuses, and main/index branch links. Its wrapper reads the page header for row-range validation and slot lookup. The version map currently knows its slot capacity but not `start_row_id`.

  The shared frame latch protects the context from deallocation and frame reuse; a pool keepalive alone does not protect that context. Row-version mutation also requires the row write latch. Current Frozen accesses hold the page-state read guard and publish paired mutation-counter bumps that invalidate checkpoint plans.

  Secondary-index purge is a separate phase: `any_version_matches_key()` can require current row values. Physical rollback changes page bits or columns, and Transition Lock rollback and forward-link restoration currently validate the physical delete bit. These paths cannot all adopt chain-only access unchanged.
- Direction Hint:
  Prefer a narrow accessor that validates the exact frame generation under a shared frame latch and exposes only resident version metadata, with no page-content access or reload. Preserve the existing lock order and checkpoint mutation bookkeeping. Retain immutable row identity metadata, such as `start_row_id` plus the entry-array capacity, so purge can locate and validate a slot without its page header.

  Keep frame-latch removal, separate shared ownership of version maps, and rollback optimization for later evaluation. If a later consumer falls back to physical page access, release metadata locks before loading and revalidate afterward.

## Scope Hint

- Add the generation-checked metadata accessor and row-slot identity needed by row undo purge, covering user and memory/catalog callers as applicable.
- Move or factor chain pruning into access that does not require `RowPage`, preserving row latching, page-state coordination, and Frozen mutation tracking.
- Preserve missing-generation handling, cold delete-marker promotion, purge horizons, and detachment before owning undo allocations are freed.
- Keep secondary-index purge, rollback, durable formats, and transaction-long page pinning outside this first change.

## Acceptance Hint

- A deterministic test evicts a row page, purges its eligible undo through the production path, and proves the page remains evicted with no added page-read I/O while its chain is correctly pruned.
- Reclaimed or reused frame generations cannot expose the old context or mutate a replacement chain; existing missing-page and delete-marker promotion behavior remains correct.
- Concurrent readers, writers, checkpoint planning/Transition, and frame retirement preserve synchronization and unlink-before-free ordering. Retain or extend targeted race tests using semantic gates.
- Resident-page and memory/catalog purge continue to work; standard workspace tests and the alternate libaio backend pass.
- Measure reload I/O and cache effects on an eviction-heavy purge workload, distinguishing undo-chain savings from page reads still required by secondary-index cleanup.

## Notes (Optional)

Related [backlog 000068](000068-pin-in-transaction-evictable-row-pages-to-avoid-rollback-reload-io.md) proposes pinning pages to avoid rollback reloads. This item instead uses metadata that already survives eviction and initially changes only undo-chain purge.

