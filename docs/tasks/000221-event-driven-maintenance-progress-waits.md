---
id: 000221
title: Event-driven maintenance progress waits
status: proposal  # proposal | implemented | superseded
created: 2026-07-12
github_issue: 841
---

# Task: Event-driven maintenance progress waits

## Summary

Replace timer-polled user-table checkpoint and transaction-GC test progress with
public, lost-wakeup-safe event waits backed by production maintenance state.
Add the table identity to every checkpoint delay, let an idle `Session` wait on
the exact predicate represented by a delay, and make a frozen-page cutoff wait
own the canonical batch until the named page is completely ready: every
image-blocking transaction has resolved and the purge-published cutoff covers
the page's committed image requirement.

Expose only the GC horizon, completed-purge, checkpoint-delay, and checkpoint
retry conveniences that current callers and tests consume. Migrate the shared
polling helpers and direct checkpoint/GC timing loops to those interfaces,
stabilizing the checkpoint heartbeat, mixed catalog/table checkpoint, and
delayed-frozen-batch drop regressions without changing checkpoint publication,
transaction visibility, persistence, or recovery semantics.

## Context

Issue Labels:
- type:task
- priority:medium
- codex

Source Backlogs:
- docs/backlogs/000156-event-driven-maintenance-progress-waits.md

User-table checkpoint currently returns a normal `CheckpointOutcome::Delayed`
for two distinct readiness gates:

1. `ActiveRoot` means the active table-file root's runtime `effective_ts` is
   still visible to an active snapshot. Retry becomes useful after the
   purge-published active horizon is strictly greater than that effective
   timestamp, or after the observed root/lifecycle changes.
2. `FrozenPageCutoff` means the first canonical frozen page is not ready at the
   selected exclusive cutoff. The page may have one or more unresolved
   image-blocking transaction statuses, a committed
   `required_cutoff_ts`, or both. Readiness is a page-level conjunction: all
   blockers must resolve and the published cutoff must be at least the final
   required cutoff.

The current public delay diagnostics do not give callers an event-driven retry
contract. `FrozenPageCutoff` already contains `table_id`, while `ActiveRoot`
does not. Tests approximate progress with timer loops in
`wait_gc_cutoff_after`, `wait_checkpoint_ready`, `checkpoint_published`, local
`checkpoint_table_published` duplicates, and direct GC/checkpoint polling. The
four helper names currently account for roughly 190 call sites. This obscures
which predicate a test needs, can retry on unrelated timestamp movement, and
has produced timing-sensitive failures under parallel workspace validation.

The underlying runtime already has suitable state boundaries but not complete
wait contracts:

- `published_gc_horizon` advances when purge observes the active-snapshot
  horizon, before physical GC completes;
- `global_visible_sts` advances after full purge work completes;
- frozen-page analysis reads the exact `SharedTrxStatus` references that make
  an image unready;
- storage poison and table lifecycle changes already have wakeable state;
- engine shutdown currently needs an async start notification so a public wait
  holding a runtime pin cannot strand shutdown.

Task 000208 established the repository's epoch-plus-event, register/recheck/wait
pattern for production progress. Tasks 000217 through 000219 established the
current structured cutoff delay, canonical table-owned batch, cached monotonic
page validation, and optimistic page-transition plan. This task extends those
contracts; it does not replace them with a scheduler or generic maintenance
service.

Repository coding guidance prohibits sleeps as concurrent-test synchronization.
Timeouts may remain as hang watchdogs or negative assertions, but elapsed time
must not be the mechanism that makes a maintenance predicate true.

## Goals

1. Add `table_id` to `CheckpointDelayReason::ActiveRoot` so every delay is
   self-identifying and callers cannot supply a mismatched table separately.
2. Add an idle-session public wait for `CheckpointDelayReason` that returns
   when retry may be useful, without embedding engine, table, event, or
   transaction handles in the public reason.
3. Make `FrozenPageCutoff` waiting page-level: retain the canonical batch and
   wait until all blockers on the named page resolve and its final committed
   cutoff requirement is satisfied.
4. Apply the same page-level loop to mixed unresolved/committed cases, including
   blocker replacement and commits that introduce a newer cutoff requirement.
5. Add public waits for the purge-published active horizon and completed full
   purge so cutoff selection and physical reclamation tests use the correct,
   distinct boundaries.
6. Add a checkpoint convenience that retries only `Delayed` outcomes through
   the new wait, preserving `Published`, `Cancelled`, and error behavior.
7. Make every wait lost-wakeup-safe and responsive to storage poison, engine
   shutdown, and relevant table lifecycle changes.
8. Remove the shared timer-polling checkpoint/GC helpers, their local
   duplicates, and direct timer loops where one of the new production
   predicates applies.
9. Stabilize the three failures recorded in backlog 000156 and add deterministic
   coverage for page-level, mixed-order, cancellation, and terminal wakeups.
10. Keep the public maintenance API limited to interfaces exercised by the
    migrated production/test call sites.

## Non-Goals

1. Do not add a generic `MaintenanceProgress` snapshot, public epoch stream,
   maintenance actor, tuple-mover scheduler, or automatic retry policy beyond
   the explicit checkpoint convenience.
2. Do not expose `SharedTrxStatus`, transaction wait handles, table roots,
   checkpoint workflow internals, catalog replay timestamps, or catalog
   checkpoint subscriptions.
3. Do not change checkpoint correctness rules, delay ordering, caller control
   of the existing single-attempt `checkpoint_table`, publication admission,
   page transition, root atomicity, redo, table-file formats, or recovery.
4. Do not make a frozen-page wait return merely because one blocker resolved or
   because the private blocker set changed while the same canonical page
   remains unready.
5. Do not wait for a generally newer STS/CTS when an unresolved frozen-page
   image has exact transaction-status events available.
6. Do not remove every timer in the repository. Filesystem unlink polling,
   lock-manager state inspection, I/O hook coordination, engine-shutdown unit
   scaffolding, and timeout watchdogs remain separately classified unless an
   existing event can replace them without widening this task's API.
7. Do not add a public dropped-table file cleanup wait or use path existence as
   a reason to expose catalog maintenance internals.
8. Do not change test-runner or timeout policy.

## Plan

### 1. Finalize the public delay and wait surface

Add `table_id: TableID` to `CheckpointDelayReason::ActiveRoot`; retain the
existing `table_id` in `FrozenPageCutoff`. Keep the enum diagnostic-only and
retain its `Clone`, `Copy`, `Debug`, `Eq`, and `PartialEq` behavior. Update all
constructors, matches, logs, and tests for the localized public source change.

Add these documented public methods:

```rust
Session::wait_for_checkpoint_retry(
    &self,
    reason: CheckpointDelayReason,
) -> Result<()>

Session::checkpoint_table_with_wait(
    &mut self,
    table_id: TableID,
) -> Result<CheckpointOutcome>

Session::wait_for_gc_horizon_after(
    &self,
    ts: TrxID,
) -> Result<TrxID>

Session::wait_for_purge_completion_after(
    &self,
    ts: TrxID,
) -> Result<TrxID>
```

All maintenance waits require an idle session. This prevents a session from
waiting for progress that its own active transaction pins. The two timestamp
waits use strict `> ts` semantics and return the satisfying observed boundary.

`checkpoint_table_with_wait` repeatedly invokes the existing single-attempt
`checkpoint_table`. It calls `wait_for_checkpoint_retry` only for `Delayed` and
returns the first `Published` or `Cancelled` outcome unchanged. It does not
retry cancellation or convert normal cancellation into an error.

`wait_for_checkpoint_retry` consumes a copied diagnostic. Waking means that the
observed predicate is satisfied or no longer current and the caller should
retry; it does not promise the next attempt will publish, because a later page
or a new active-root boundary may become the next canonical delay.

### 2. Add coalesced GC horizon and completed-purge notifications

Add private `ChangeNotifier`s beside `published_gc_horizon` and the completed
full-GC boundary represented by `global_visible_sts` (or an equivalently named
explicit completed boundary if separating storage makes the semantics clearer).
Notify only when the corresponding monotonic value advances.

Preserve ordering:

1. purge calculates the current minimum active STS;
2. it publishes/notifies the active horizon;
3. it completes eligible undo/index cleanup and retired-page deallocation;
4. it processes coalesced retained-root work;
5. only then does it publish/notify completed-purge progress.

The wait methods use the epoch-plus-event predicate pattern: read the boundary,
record the notifier epoch/listeners, recheck, and await only while the predicate
is false. They also request/coalesce a purge observation before sleeping so a
caller cannot depend on unrelated later work to make an already-eligible
boundary visible. Purge queue state remains authoritative; this wake is lossy
and idempotent.

Race progress notification against engine poison and shutdown. Return the
existing fatal/lifecycle error rather than hanging after the producer has
terminated.

### 3. Add sticky terminal resolution to shared transaction status

Extend private `SharedTrxStatus` with a sticky terminal-resolution predicate and
event listener. This is internal coordination state and is not re-exported.

Signal at these correctness boundaries:

- successful commit: after the commit CTS is stored in the shared status;
- successful active/abandoned rollback: after index and row undo removal,
  rollback purge recording, lock release, and session rollback completion are
  complete enough that frozen-page reanalysis cannot observe the old blocker;
- successful failed-precommit rollback: after rollback-capable undo has been
  removed and before/with terminal waiter release;
- deterministic status test helpers: update terminal state consistently.

Do not signal normal resolution after rollback access failure. That path
poisons storage and retains unsafe payload ownership; poison is the waiter wake
and error boundary.

Listener registration must recheck the sticky terminal predicate so commit or
rollback racing registration cannot be missed.

### 4. Retain the complete current blocked page privately

Extend `FrozenPageAnalysis` and the private canonical `FrozenPageBatch` state
to retain one optional current blocked-page record containing all unresolved
`Arc<SharedTrxStatus>` values that actually make `EstablishReadiness` blocked.
Incremental validation stops at the first canonical delayed page, so retaining
an empty vector for every stable or unchecked page adds no information.
Deduplicate blockers by shared-status identity. Do not include unresolved
lock/delete markers that are representable after a stable image proof and
therefore do not block readiness.

Continue computing the maximum `required_cutoff_ts` from committed
image-producing insert/update entries. The cached page state therefore has one
complete private readiness description:

```text
unresolved image blockers
maximum committed exclusive cutoff requirement
monotonic validation state
```

Reuse the cached handles when a wait claims the same delayed page. After every
status in that blocker generation becomes terminal, reanalyse once and replace
the complete blocker record. Clear it when the page becomes stable, the batch
publishes, or the workflow closes. Prepared transition plans remain separate
cutoff/version-specific optimization state.

Keep public `FrozenPageCutoff` fields unchanged except for their existing
diagnostics. No handle or blocker count is required in the public enum.

### 5. Implement page-level `FrozenPageCutoff` waiting

For a matching `FrozenPageCutoff`, claim the table's canonical batch through
the existing reversible `CheckpointAttempt`. The wait owns the batch while
asleep, so concurrent freeze/checkpoint calls observe `CheckpointInProgress`;
it does not acquire root-mutation admission, publish admission, or a page-state
lock across an await. Dropping/cancelling the wait restores the batch through
the existing attempt guard. Table drop can close the workflow, wake the wait,
and cause the guard to discard rather than resurrect the batch.

Verify that the batch names the reason's table and contains the reason's page.
If the batch/page was already published, discarded, replaced, or claimed by a
competing operation before this wait acquired it, return so the caller can
retry and observe current state. While this wait owns the same canonical page,
do not return merely because blockers or cutoff requirements changed.

Load the named frozen page and run this loop:

1. Reuse the current blocked-page record or cached `Stable` proof. Run the
   existing fused readiness analyzer only for an unchecked page or after every
   blocker in the cached generation becomes terminal.
2. Read the current purge-published cutoff.
3. The page is ready only when its blocker set is empty and
   `required_cutoff_ts` is absent or at most the current cutoff.
4. If ready, retain the stable proof in the batch and return.
5. For a blocked page, register listeners for every unresolved status and for
   table lifecycle change, poison, and shutdown. Recheck the sticky terminal
   predicates after registration. Individual blocker wakes only re-register
   the remaining listeners; once the complete generation is terminal,
   reanalyse the page once and replace the blocker/requirement cache.
6. For a stable page below its required cutoff, register GC-horizon, lifecycle,
   poison, and shutdown listeners, recheck the cutoff, and wait without
   reanalysing the stable image proof.
7. Repeat until reanalysis finds a stable page and its final cutoff requirement
   is satisfied.

This is the only frozen-page wait algorithm; do not split out a mixed case.
Required behaviors include:

- resolving one of several blockers does not complete the wait;
- resolving one of several blockers does not rescan the page;
- unrelated transaction completion does not complete the wait;
- horizon-first and status-first event orders converge on the same predicate;
- a blocker commit can replace an unresolved requirement with a newer committed
  cutoff and keep waiting;
- a blocker rollback removes its undo before reanalysis and may eliminate the
  cutoff requirement;
- blocker replacement causes the wait to bind to the new complete blocker set,
  not return as stale;
- once stable, the following checkpoint retry reuses the page proof and can
  discover the next delayed page without rescanning this one.

Do not spin or poll a busy page. Reanalysis occurs only at initial entry and
after one of the registered domain events.

### 6. Implement `ActiveRoot` retry waiting

For `ActiveRoot`, resolve the reason's `table_id` and verify the current active
root still has the observed `effective_ts`. Return immediately if the root has
changed or the published GC horizon is already strictly greater than the
effective timestamp.

Otherwise register GC-horizon, table-lifecycle, poison, and shutdown listeners,
recheck the root/horizon predicate, and wait. Continue until the observed root
is reclaimable or no longer current. A table drop wakes the wait so the caller
can retry and observe cancellation; poison and shutdown return errors.

Do not use general timestamp allocation as readiness. The event must be the
purge-published active horizon because allocating a newer STS/CTS does not prove
older active snapshots have drained.

### 7. Make shutdown and table lifecycle terminal wakes explicit

Add an async engine-shutdown-start listener to `EngineLifecycle`. Publish it
when admission first changes from `Running` to `ShuttingDown`, before shutdown
waits for runtime references. Public maintenance waits holding a `SessionPin`
must select this event, return `LifecycleError::Shutdown`, and release their
runtime reference so shutdown can proceed.

Reuse the existing table lifecycle event through narrow crate-private
observation/listener helpers. A lifecycle wake is predicate-based: recheck root,
batch, page, and terminal state rather than assigning meaning to the event
alone.

Reuse the existing one-shot poison listener and `ensure_runtime_healthy` before
registration, after registration, and after wakeup.

### 8. Migrate and classify timing-sensitive tests

Delete the shared `wait_gc_cutoff_after`, `wait_checkpoint_ready`, and
`checkpoint_published` helpers plus local checkpoint retry duplicates. Replace
their call sites as follows:

- successful checkpoint setup uses `checkpoint_table_with_wait` and directly
  destructures/asserts the returned publication;
- cutoff-oriented setup uses `wait_for_gc_horizon_after`;
- old-root, undo/index, purge-counter, retired-page, and buffer-reclamation
  assertions use `wait_for_purge_completion_after` before inspecting state;
- tests intentionally asserting `Delayed` continue to make one checkpoint
  attempt, assert the exact reason, and use
  `wait_for_checkpoint_retry` only when the remainder of the scenario needs
  readiness;
- explicit GC-horizon timer loops use the public horizon or reason wait;
- race construction uses test hooks, channels, event barriers, or pending
  future assertions, not sleeps.

Refine the three recorded flaky tests:

1. `test_checkpoint_heartbeat` uses event-driven checkpoint retry and still
   requires final `Published { silent: true, .. }` plus unchanged root and
   silent-watermark assertions.
2. `test_catalog_checkpoint_now_heartbeat_with_mixed_user_table_checkpoint_states`
   event-waits the user-table checkpoint before running the synchronous catalog
   checkpoint and retains the checkpointed-versus-replay-only assertions.
3. `test_drop_discards_delayed_frozen_batch_before_runtime_destroy` first
   crosses the unrelated initial active-root boundary, then deterministically
   arranges writer/freeze/pinned-reader/commit ordering and requires one attempt
   to return the intended `FrozenPageCutoff` before drop discards the batch.

Audit remaining test `Timer::after`, `thread::sleep`, bounded retry loops, and
`wait_*` helpers in table, catalog, recovery, and transaction modules. Record
each remaining timer as one of: timeout watchdog/negative assertion,
filesystem side effect, lock/I/O test coordination, or unrelated follow-up.
Do not widen public production APIs solely to eliminate an unrelated test
timer.

### 9. Synchronize living documentation and public docs

Update `docs/checkpoint-and-recovery.md` and `docs/transaction-system.md` to
document:

- the self-identifying delay reasons;
- active-root versus frozen-page wait predicates;
- complete page-level readiness across all blockers and committed cutoff;
- sticky commit/rollback terminal signaling used only for coordination;
- distinction between purge-published horizon and completed full purge;
- terminal poison/shutdown/drop behavior;
- unchanged checkpoint correctness, persistence, and recovery boundaries.

Add descriptive rustdoc to every new public method and document that wait
completion means retry may be useful, not guaranteed publication.

## Implementation Notes


## Impacts

- `doradb-storage/src/table/persistence.rs`
  - add `table_id` to active-root delay construction;
  - integrate session-driven delay waiting with the existing checkpoint entry;
  - update checkpoint tests and local timer loops.
- `doradb-storage/src/table/page_transition.rs`
  - collect all exact unresolved readiness blockers during fused analysis;
  - expose a private target-page readiness refresh used by checkpoint and wait.
- `doradb-storage/src/table/checkpoint_workflow.rs`
  - retain one current blocked-page set beside per-page validation state;
  - support reversible batch ownership across page readiness waits;
  - preserve cancellation/drop restoration invariants.
- `doradb-storage/src/table/lifecycle.rs`
  - add narrow state/listener helpers for predicate-based terminal wakes.
- `doradb-storage/src/trx/mod.rs`
  - add sticky shared-status terminal resolution and signal successful commit
    and rollback boundaries.
- `doradb-storage/src/trx/sys.rs`
  - add horizon/completed-GC notifiers and internal wait helpers;
  - preserve poison selection and timestamp semantics.
- `doradb-storage/src/trx/purge.rs`
  - notify active horizon before GC and completed progress after full GC;
  - preserve single- and multi-threaded ordering;
  - support a lossy/idempotent progress observation wake.
- `doradb-storage/src/engine.rs`
  - publish and expose an internal async shutdown-start notification.
- `doradb-storage/src/session.rs`
  - add the four public maintenance/checkpoint wait methods;
  - enforce idle-session and lifecycle admission rules;
  - update session-level test helpers and call sites.
- `doradb-storage/src/lib.rs`
  - no new public type is expected; retain existing delay/outcome exports and
    update exports only if method documentation requires it.
- `doradb-storage/src/table/access.rs`, `doradb-storage/src/table/gc.rs`,
  `doradb-storage/src/table/rollback.rs`, `doradb-storage/src/table/recover.rs`,
  `doradb-storage/src/catalog/index.rs`, `doradb-storage/src/catalog/mod.rs`,
  `doradb-storage/src/recovery/mod.rs`, and adjacent tests
  - replace shared/local timer-polled checkpoint and GC progress helpers.
- `doradb-storage/src/notify.rs`
  - reuse or minimally extend the coalesced async notifier primitive if
    multi-event registration needs a narrow helper.
- `docs/checkpoint-and-recovery.md` and `docs/transaction-system.md`
  - synchronize the maintenance progress and retry contract.

No table-file, catalog-file, redo, LWC, secondary-index, or recovery data format
is changed.

## Test Cases

1. `ActiveRoot` includes the correct `table_id`, effective timestamp, and
   diagnostic minimum active STS.
2. Active-root wait returns immediately when its horizon is already satisfied.
3. Active-root wait wakes after future purge-published horizon progress.
4. Root replacement makes an observed active-root delay obsolete and wakes the
   waiter without waiting for the old target again.
5. Table drop wakes an active-root wait; poison and shutdown return their
   existing errors.
6. GC-horizon wait returns after prior progress, future progress, and progress
   racing listener registration.
7. Completed-purge wait does not return at early horizon publication and returns
   only after eligible undo/index/page reclamation and coalesced retained-root
   work complete.
8. A progress wait requests purge observation and does not require unrelated
   later transactions to wake an already-eligible boundary.
9. Shared status terminal listener returns after prior commit and wakes after
   future commit.
10. Shared status terminal listener wakes only after successful rollback has
    removed row/index undo and finished rollback bookkeeping.
11. Failed-precommit rollback signals successful resolution; rollback access
    failure wakes via poison and never reports normal resolution.
12. One unresolved frozen-page blocker commits; page wait continues until the
    commit-derived required cutoff is published.
13. One unresolved blocker rolls back; page wait reanalyses after undo removal
    and becomes ready without inventing a commit cutoff.
14. Two or more blockers resolve separately; resolving the first does not
    complete the page wait.
15. Unrelated transaction completion does not complete a page wait.
16. A blocker set changing while the same canonical page remains unready causes
    rebinding/reanalysis, not stale return.
17. Horizon-first mixed ordering keeps waiting for unresolved blockers.
18. Status-first mixed ordering keeps waiting for the required cutoff.
19. A blocker commit introduces a newer required cutoff and the same wait adopts
    that requirement.
20. A blocker rollback removes its requirement while other blockers or cutoff
    constraints continue to hold the wait.
21. Page wait registers all status/horizon listeners, rechecks, and handles each
    event racing registration without a lost wakeup.
22. Successful page wait retains a `Stable` proof in the canonical batch; the
    next checkpoint skips that page and can report a later delayed page.
23. Cancelling/dropping a page wait restores the canonical batch unchanged
    enough for a later checkpoint retry.
24. A page wait holds no root-mutation lease, publish lease, or page-state lock
    across await; foreground frozen-page lock/delete work remains possible.
25. Concurrent checkpoint/freeze sees the existing workflow cancellation while
    a page wait owns the batch; no second batch is created.
26. Drop closes a waiting workflow without batch resurrection; poison and
    shutdown wake and release runtime pins.
27. `checkpoint_table_with_wait` loops only over `Delayed`, returns publication
    unchanged, and preserves cancellation/error outcomes.
28. `test_checkpoint_heartbeat` deterministically reaches silent publication
    without timer retry.
29. `test_catalog_checkpoint_now_heartbeat_with_mixed_user_table_checkpoint_states`
    deterministically publishes the selected user table before catalog
    assertions.
30. `test_drop_discards_delayed_frozen_batch_before_runtime_destroy`
    deterministically observes `FrozenPageCutoff` rather than an incidental
    active-root delay.
31. Old-root release, retired-page deallocation, buffer allocation, and purge
    counter tests wait for completed purge rather than polling.
32. `wait_gc_cutoff_after`, `wait_checkpoint_ready`, `checkpoint_published`,
    `checkpoint_table_published`, and local duplicates have no remaining
    definitions or calls.
33. Targeted checkpoint/GC progress tests contain no timer/sleep-based predicate
    loops; remaining timers from the audit have documented non-progress roles.
34. Repeated focused runs of the three prior flaky tests and new registration
    race tests pass without runner retries under both default and `libaio`
    configurations.
35. Run `cargo fmt --all -- --check`.
36. Run `cargo clippy --workspace --all-targets -- -D warnings`.
37. Run `cargo nextest run --workspace` without `--retries`.
38. Run
    `cargo nextest run -p doradb-storage --no-default-features --features libaio`
    without `--retries`.
39. Run focused coverage for changed session, checkpoint workflow/analysis,
    transaction status, purge, and lifecycle files; meet the 80% focused
    coverage bar or explain definition-heavy exceptions through covered
    consumers.
40. Run `tools/style_audit.rs --diff-base origin/main` before resolve.

## Open Questions

None. Filesystem unlink completion, lock-manager inspection, I/O hook waits,
and repository-wide timeout policy remain outside this task and should be
planned separately only when they have a concrete production consumer or
observed flaky failure.
