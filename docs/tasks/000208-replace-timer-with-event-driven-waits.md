---
id: 000208
title: Replace Timer With Event Driven Waits
status: proposal  # proposal | implemented | superseded
created: 2026-07-02
github_issue: 802
---

# Task: Replace Timer With Event Driven Waits

## Summary

Replace production `smol::Timer` sleeps in table checkpoint and row-transition
retry paths with event-driven progress notifications. The task also fixes the
related rollback purge wakeup gap: rollback can advance the GC horizon without a
committed handoff, so purge must be woken to publish that horizon and run GC.

The chosen design keeps the change narrow. It does not make the whole storage
engine runtime-agnostic and does not remove all `smol::block_on` use. It removes
timer-driven production waits, makes transition retry wait on block-index route
publication or storage poison, and uses a guard so any checkpoint failure after
row pages enter `TRANSITION` poisons storage instead of leaving writers waiting
forever.

## Context

`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

Production `smol::Timer` usage is currently concentrated in five waits:

- `table/mod.rs::wait_for_frozen_pages_stable()` polls once per second while
  waiting for frozen pages to become stable against the GC horizon.
- `table/access.rs` has two 1 ms sleeps after user-table update/delete sees
  `RetryInTransition`.
- `table/mem_table.rs` has the same two 1 ms sleeps for standalone/catalog
  `MemTable` update/delete retry.

`smol::Timer` is a re-export of `async_io::Timer`. Polling that timer initializes
the `async-io` reactor and starts its helper thread. Replacing it with
`async_io::Timer` directly would remove the `smol` symbol but preserve the
runtime behavior, so this task replaces the waits with domain progress events.

The relevant storage model is already event-friendly:

- Checkpoint freezes row pages, waits for relevant inserts/updates to commit or
  abort, moves pages to `TRANSITION`, builds LWC blocks, then publishes a new
  block-index route and table root.
- Foreground hot-row writers return `RetryInTransition` only when the row page
  state is already `TRANSITION`. At that point the writer needs either the
  block-index route to advance so the row resolves through cold storage, or a
  fatal error if checkpoint cannot publish that route.
- Transaction GC bucket state already reports whether a commit or rollback
  advanced a bucket minimum. Commits are handed to purge through
  `Purge::Committed`, but rollback currently updates bucket state directly and
  does not clearly wake purge.

## Goals

- Remove production `smol::Timer` imports and `Timer::after(...)` calls from:
  - `doradb-storage/src/table/mod.rs`
  - `doradb-storage/src/table/access.rs`
  - `doradb-storage/src/table/mem_table.rs`
- Replace frozen-page stabilization polling with an event-driven wait on a
  purge-published GC horizon.
- Add a lossy rollback horizon wakeup so rollback-driven GC horizon advancement
  wakes purge and cannot strand GC until unrelated later work.
- Replace user-table `RetryInTransition` sleeps with an event wait on
  block-index route publication or one-shot storage poison.
- Treat `TRANSITION` in standalone/catalog `MemTable` update/delete as an
  internal error, because `MemTable` alone should not move pages into
  `TRANSITION`.
- Add a guard that poisons storage with `FatalError::CheckpointWrite` if
  checkpoint exits after pages enter `TRANSITION` but before their cold route is
  published.
- Add focused comments at each new progress/poison boundary so future readers
  understand why the notifications exist and what invariant they protect.

## Non-Goals

- Do not remove `smol` as a crate dependency in this task. Production
  `smol::block_on` remains separate runtime-agnostic work.
- Do not introduce application-level runtime injection.
- Do not replace test-only `smol::Timer` or `smol::block_on` usage.
- Do not redesign checkpoint recovery, row-page state transitions, table-file
  publication, or block-index routing.
- Do not restore row pages from `TRANSITION` after checkpoint failure. After
  transition begins, poison is the failure boundary.
- Do not use a generic timer abstraction or a different timer crate.

## Plan

1. Extend notification primitives for async event waits.
   - Add an async wait helper to `notify::ChangeNotifier`, or an equivalent
     crate-local helper using the same epoch-plus-event pattern.
   - Keep the existing blocking `wait_since()` behavior for current synchronous
     users.
   - Comment that change notifications are coalesced and predicate-based:
     waiters must always recheck their condition after waking.

2. Add a purge-published GC horizon to `TransactionSystem`.
   - Add a separate monotonic `published_gc_horizon: AtomicU64`, initialized to
     the initial transaction timestamp.
   - Add a horizon `ChangeNotifier` used only for waiters that need active
     transaction horizon progress.
   - Add `published_gc_horizon()` and an async wait helper for horizon changes.
   - Do not reuse `global_visible_sts`. `global_visible_sts` is still updated
     after purge work completes and remains a visibility fast path. The new
     published horizon is allowed to advance before physical purge work
     completes because frozen-page stabilization only needs to know that
     relevant transactions are no longer active.

3. Make purge publish the active GC horizon.
   - In both purge loop implementations, after coalescing work and computing
     `curr_sts = trx_sys.calc_min_active_sts_for_gc()`, publish `curr_sts` to
     `published_gc_horizon` when it advances.
   - Notify horizon waiters only on monotonic advancement.
   - Preserve the existing `global_visible_sts` update after full GC work
     finishes.
   - Add comments distinguishing active-horizon publication from completed-GC
     visibility acceleration.

4. Wake purge on rollback-driven horizon advancement.
   - Add a lossy purge message such as `Purge::HorizonAdvanced`.
   - Add a `TransactionSystem` helper for rollback purge recording, for example
     `record_rollback_for_purge(gc_no, sts)`, that:
     - calls the target `GCBucket::record_rollback_for_purge(sts)`;
     - sends `Purge::HorizonAdvanced` only when the bucket reports that its
       minimum advanced.
   - Replace direct production calls to
     `gc_buckets[gc_no].record_rollback_for_purge(sts)` with the helper in:
     - terminal rollback cleanup;
     - unordered prepared discard;
     - failed-precommit rollback cleanup payload handling.
   - Update test helper code that intentionally manipulates GC bucket state, or
     leave direct bucket calls only in tests that are explicitly unit-testing
     bucket internals.
   - In `PurgeWork::absorb`, make `Purge::HorizonAdvanced` coalesce to full GC
     work, including table-root retention and dropped-table cleanup. This is
     lossy by design because bucket state is already authoritative.
   - Comment that commit handoffs are non-lossy through `Purge::Committed`, but
     rollback horizon wakeups are lossy because purge can recalculate from
     bucket state.

5. Replace `wait_for_frozen_pages_stable()` polling.
   - Change the loop to read `trx_sys.published_gc_horizon()` instead of
     recomputing `calc_min_active_sts_for_gc()` locally.
   - If all frozen pages satisfy `row_ver.max_ins_sts() < published_horizon`,
     return.
   - Otherwise register for horizon change, recheck the predicate, and await.
   - Check `trx_sys.ensure_runtime_healthy()` before sleeping and after waking
     so checkpoint exits promptly if storage is poisoned.
   - Keep the existing conservative behavior that unrelated long-running
     transactions may delay checkpoint stabilization.

6. Add one-shot poison notification.
   - Add an `Event` beside the existing storage poison flag and reason in
     `TransactionSystem`.
   - In `poison_storage`, notify all waiters only when the call transitions the
     runtime from healthy to poisoned.
   - Do not add a poison epoch. Poison is sticky and one-shot; waiters can use
     `ensure_runtime_healthy()` as the source of truth.
   - Expose a crate-private listener helper for code that must wait on poison in
     combination with another event.
   - Comment that poison is an admission barrier, not shutdown, but event waits
     must observe it so they do not sleep after the only progress producer has
     failed.

7. Add block-index route progress notification.
   - Add a route `ChangeNotifier` to `BlockIndexRoot`.
   - Notify after `BlockIndexRoot::update_column_root()` has atomically updated
     `pivot_row_id` and `column_root_block_id`.
   - Expose route epoch/listener helpers through `BlockIndex`.
   - Comment that this notification is specifically for row-transition waiters:
     they need route progress, not merely table lifecycle progress.

8. Replace user-table `RetryInTransition` sleeps.
   - In `UserTableAccessor::update_unique_mvcc_input()` and
     `UserTableAccessor::delete_unique_mvcc()`, replace the 1 ms timer sleep
     with a helper that waits for either:
     - route progress on the table's block index; or
     - storage poison.
   - The helper should accept the `row_id` that hit `TRANSITION` and the
     current `TrxRuntime`.
   - It should check `rt.engine().trx_sys.ensure_runtime_healthy()` before
     registering listeners and after waking.
   - It should recheck whether `row_id < self.mem().blk_idx().pivot_row_id()`
     before sleeping so it does not miss a route update that races with listener
     registration.
   - Use `futures::future::select` or equivalent existing futures utilities;
     `futures` is already a normal crate dependency.
   - After the helper returns, restart the existing outer lookup loop so normal
     cold-row update/delete paths handle the row through `RowLocation::LwcBlock`.

9. Replace standalone/catalog `MemTable` transition retry sleeps.
   - In `MemTable::update_unique_mvcc_input()` and
     `MemTable::delete_unique_mvcc()`, return an internal error when
     `RetryInTransition` is observed.
   - Comment that `MemTable` owns hot row-store state only; without user-table
     column storage and checkpoint route publication, `TRANSITION` is not a
     valid standalone/catalog state.

10. Add a guard around post-transition checkpoint publication.
   - Introduce a small RAII guard in table checkpoint code, for example
     `TransitionRoutePublicationGuard`.
   - Arm the guard before the first in-memory row-page transition mutation, or
     refactor the transition helper so all fallible page loading happens first
     and then the guard is armed immediately before non-fallible state mutation.
   - Keep the guard armed until after `self.mem.blk_idx().update_column_root(...)`
     publishes the cold route.
   - If dropped while armed, call
     `trx_sys.poison_storage(FatalError::CheckpointWrite)`.
   - Disarm only after route publication succeeds.
   - Comment the invariant: once row pages enter `TRANSITION`, foreground hot
     writers cannot safely continue on those pages; failure before route
     publication must poison storage rather than leave route waiters blocked.

11. Remove production Timer imports and duration imports made unused by this
    task.
   - Remove `use smol::Timer` from production sections of affected modules.
   - Remove `std::time::Duration` imports that were only used by production
     timers.
   - Keep test-only timer imports unchanged unless they become unused in a
     specific test module.

12. Add comments and targeted tests.
   - Comments are required near every new notification and guard boundary:
     rollback purge wakeup, purge-published horizon, route notifier, one-shot
     poison event, user-table transition wait, standalone `MemTable` transition
     error, and transition publication guard.
   - Add or update tests for the behavior listed under `Test Cases`.

## Implementation Notes

## Impacts

- `doradb-storage/src/notify.rs`
  - Add async epoch wait support, preserving blocking wait behavior.
- `doradb-storage/src/trx/sys.rs`
  - Add published GC horizon state and helpers.
  - Add one-shot storage poison event/listener helper.
  - Route rollback purge recording through a helper that wakes purge when the
    horizon advances.
- `doradb-storage/src/trx/purge.rs`
  - Add lossy horizon-advanced purge work.
  - Publish active GC horizon from purge loops.
  - Preserve existing completed-GC `global_visible_sts` semantics.
- `doradb-storage/src/trx/mod.rs`
  - Replace direct rollback GC bucket updates with the transaction-system helper
    where production rollback paths advance the GC horizon.
- `doradb-storage/src/log/mod.rs`
  - Preserve non-lossy committed payload handoff through `Purge::Committed`;
    adjust tests/comments if purge work shape changes.
- `doradb-storage/src/index/block_index_root.rs`
  - Add route change notifier and notify after column-root route publication.
- `doradb-storage/src/index/block_index.rs`
  - Expose route progress observation/wait helpers to table access code.
- `doradb-storage/src/table/mod.rs`
  - Replace frozen-page timer polling with the purge-published horizon wait.
  - Adjust transition helper as needed for the guard boundary.
- `doradb-storage/src/table/persistence.rs`
  - Add the transition-to-route-publication guard.
- `doradb-storage/src/table/access.rs`
  - Replace user-table transition retry timers with route-or-poison waits.
- `doradb-storage/src/table/mem_table.rs`
  - Replace standalone/catalog transition retry timers with internal errors.
- `Cargo.toml` and `doradb-storage/Cargo.toml`
  - Do not remove `smol` in this task unless no normal dependency use remains
    after the production Timer removal and existing `smol::block_on` scope is
    explicitly revisited.

## Test Cases

- Unit-test `ChangeNotifier` async waits:
  - returns after prior notify;
  - wakes after future notify;
  - preserves existing blocking `wait_since()` tests.
- Unit-test one-shot poison notification:
  - listener wakes when first poison is published;
  - listener plus `ensure_runtime_healthy()` handles poison that happened before
    listener registration;
  - repeated `poison_storage()` calls do not require repeated notifications.
- Unit-test purge work coalescing:
  - multiple `Purge::HorizonAdvanced` messages collapse to one full GC cycle;
  - `Purge::Committed` still records all committed payload batches before
    coalescing;
  - `Purge::Stop` keeps the existing shutdown barrier behavior.
- Add a regression test where a very old transaction pins GC, then rolls back:
  - rollback wakes purge;
  - purge publishes an advanced GC horizon;
  - table-root retention or row/index GC work that depends on the horizon is
    not stranded until unrelated later work.
- Add a checkpoint stabilization test:
  - a frozen page waits on the published horizon;
  - rollback or commit progress wakes the wait;
  - no timer sleep is needed.
- Add a route wait test for user-table transition retry:
  - writer sees `RetryInTransition`;
  - checkpoint publishes the block-index route;
  - writer wakes and retries through the existing cold-row path.
- Add a route wait poison test:
  - writer waits after `RetryInTransition`;
  - checkpoint fails after transition and the guard poisons storage;
  - writer wakes and returns the fatal storage error.
- Add a guard test:
  - injected checkpoint error after transition but before route publication
    poisons storage;
  - successful route publication disarms the guard and does not poison.
- Add or update standalone/catalog `MemTable` tests:
  - forced/impossible `RetryInTransition` returns an internal error instead of
    sleeping.
- Run the routine validation pass:

  ```bash
  cargo nextest run -p doradb-storage
  ```

- Because this touches checkpoint and storage background coordination, also run
  the alternate backend pass when the environment has libaio dependencies:

  ```bash
  cargo nextest run -p doradb-storage --no-default-features --features libaio
  ```

## Open Questions

None for this task. Full runtime-agnostic startup/runtime injection and removal
of production `smol::block_on` remain separate future work.
