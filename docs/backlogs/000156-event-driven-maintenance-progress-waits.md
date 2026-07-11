# Backlog: Expose event-driven maintenance progress waits and stabilize checkpoint tests

## Summary

Maintenance APIs can return retryable outcomes such as `CheckpointOutcome::Delayed`, but callers have no reliable async signal that the blocking condition changed. Design and expose public event-driven maintenance progress observation and wait APIs, then use them to remove polling and timing races from checkpoint and GC tests.

## Reference

1. Deferred follow-up from `docs/tasks/000219-optimize-frozen-page-checkpoint-transition-planning.md`, discovered during its validation on 2026-07-11.
2. `table::persistence::tests::test_drop_discards_delayed_frozen_batch_before_runtime_destroy` intermittently failed in the parallel workspace suite because the checkpoint did not return the expected `CheckpointDelayReason::FrozenPageCutoff` outcome at the assumed timing point. It passed immediately in isolation and in the final full run.
3. `table::persistence::tests::test_checkpoint_heartbeat` intermittently returned `CheckpointOutcome::Delayed { reason: ActiveRoot { effective_ts: TrxID(3), min_active_sts: TrxID(2) } }` instead of the expected silent publication. It also passed immediately in isolation and in the final full run.
4. A later task-000219 validation run observed the same class of race in `catalog::tests::test_catalog_checkpoint_now_heartbeat_with_mixed_user_table_checkpoint_states`; it failed in the parallel suite and passed immediately in isolation.
5. The original final validation completed with 1,326 workspace tests and 1,250 `libaio` tests passing, so the named failures are timing-sensitive rather than deterministic functional regressions.
6. Current test helpers `wait_gc_cutoff_after`, `wait_checkpoint_ready`, and `checkpoint_published` use timer-based polling and have roughly 174 references across the test suite. Additional checkpoint tests contain explicit timer loops around GC-horizon progress.
7. `docs/process/coding-guidance.md` prohibits sleeps for concurrent-test synchronization and requires explicit synchronization points or predicate-based signaling.
8. `docs/tasks/000208-replace-timer-with-event-driven-waits.md` established event-driven production progress patterns, but equivalent public maintenance observation/wait contracts are not available to callers or tests.

## Deferred From (Optional)

`docs/tasks/000219-optimize-frozen-page-checkpoint-transition-planning.md`

## Deferral Context (Optional)

- Defer Reason: Task 000219 is scoped to frozen-page transition planning, mutation-version validation, and checkpoint publication correctness. Designing a public maintenance progress contract, wiring lifecycle notifications, and migrating a broad test surface would expand its API and review scope substantially, so this work is deferred as a focused follow-up.
- Findings: User-table checkpoint already distinguishes retryable delays from errors, but a caller receiving `CheckpointOutcome::Delayed` cannot determine when retrying is useful without periodically polling. `ActiveRoot` readiness changes with transaction lifecycle and minimum-active-snapshot progress; `FrozenPageCutoff` readiness changes with transaction status and the purge-published GC horizon. Table checkpoint publication, catalog checkpoint publication, and catalog replay-start progress are other useful monotonic maintenance boundaries. Existing test helpers approximate these boundaries with timers, which becomes unreliable under parallel load and obscures the actual predicate each test needs. The two failures observed during task 000219 validation demonstrate both sides: one test assumed a frozen-page cutoff timing window, while the heartbeat test attempted publication before the active-root horizon became ready.
- Direction Hint: Prefer a coherent public `Session` or `Engine` maintenance progress API over test-only hooks. Expose monotonic progress snapshots or epochs and async predicate-based waits that register for change, recheck the predicate, and then await, preventing lost wakeups. The design should let a caller map a retryable checkpoint delay to the relevant signal, either through a convenience API such as waiting for a table checkpoint retry condition or through composable progress waits. Candidate observations include the published GC horizon, minimum active snapshot progress, table active-root/checkpoint timestamps, and catalog checkpoint/replay-start timestamps; a future task or RFC should choose the smallest coherent surface rather than exposing unrelated internals individually. Waits must wake or return on storage poison, engine shutdown, table drop, and other terminal lifecycle changes. Do not implement maintenance progress waits with periodic timers.

## Scope Hint

Design and implement public read/wait contracts for maintenance progress, add the underlying coalesced notifications, document timestamp and retry semantics, and migrate checkpoint/GC tests from timer polling to those contracts. Stabilize the named observed failures first, then audit and migrate the shared polling helpers and explicit maintenance timing loops. Preserve existing checkpoint correctness, delay reasons, publication ordering, and caller-controlled retry policy.

## Acceptance Hint

A caller that receives `CheckpointOutcome::Delayed` can await a documented progress condition before retrying without polling. Public APIs expose the GC, table-checkpoint/root, and catalog-checkpoint progress required by maintenance consumers, either directly or through one unified abstraction. Wait registration is lost-wakeup-safe and terminates correctly on poison, shutdown, or table removal. The named flaky checkpoint tests use deterministic synchronization and pass under repeated parallel default and `libaio` runs. Existing `wait_gc_cutoff_after`, `wait_checkpoint_ready`, and `checkpoint_published` polling behavior, plus explicit checkpoint/GC timer loops, is migrated where an event-driven progress predicate applies. Focused tests cover immediate satisfaction, progress after registration, progress racing registration, and terminal wakeups.

## Notes (Optional)

Candidate API shapes for future design evaluation include a public monotonic `MaintenanceProgress` snapshot with an epoch-based `wait_for_maintenance_progress_since`, targeted waits such as `wait_for_gc_horizon`, and a higher-level `wait_for_checkpoint_retry(table_id, delay_reason)`. These are alternatives, not a requirement to expose every internal timestamp. The selected API should make the relationship between `CheckpointDelayReason` and its progress source explicit while keeping transaction and catalog internals encapsulated.

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
