---
id: 000172
title: Extract Notification Helper And Fix Prepare Waiters
status: proposal
created: 2026-06-11
github_issue: 692
---

# Task: Extract Notification Helper And Fix Prepare Waiters

## Summary

Extract the transaction lifecycle epoch-plus-event notification pattern into a
crate-private helper in `notify.rs`, delegate `SessionRegistry` transaction
change waiting to that helper, and fix transaction prepare waiters so registered
listeners are explicitly woken when prepare completes.

This is a narrow notification-correctness task. It should preserve transaction
commit ordering, redo/recovery semantics, MVCC visibility, and the existing
predicate-based wait paths that are already safe without epochs.

## Context

Backlog `000119` was deferred during task `000168` after the weak transaction
handle work added local transaction lifecycle notifications to
`SessionRegistry`. That implementation uses a monotonic epoch plus
`event_listener::Event` with a check-listen-recheck wait loop so shutdown cannot
miss a transaction lifecycle notification that happens before listener
registration.

The same pattern is currently embedded directly in `SessionRegistry`:

- `trx_changed: Event`
- `trx_change_epoch: AtomicU64`
- `trx_change_epoch()`
- `wait_for_trx_change_since(...)`
- `notify_trx_changed()`

The repository already has `EventNotifyOnDrop` in `notify.rs` for completion
style wakeups. That helper notifies all listeners when the wrapper is dropped,
but `SharedTrxStatus` prepare waiters still use `prepare_ev:
Mutex<Option<Event>>`. Commit and failed-precommit paths drop that raw `Event`
when prepare completes. Dropping a raw `Event` is not a notification, so a
writer that already obtained a prepare listener can remain blocked even though
the preparing transaction has committed or rolled back.

Prepare waiters matter because row write lock acquisition distinguishes active
owners from preparing owners. If another transaction owns a hot-row undo head
and is preparing, the caller waits for the prepare listener and retries. The
waiter must therefore wake when the transaction leaves prepare.

Issue Labels:
- type:task
- priority:medium
- codex

Source Backlogs:
- docs/backlogs/000119-extract-notification-helpers-and-fix-prepare-waiters.md

Related Context:
- docs/tasks/000168-weak-transaction-handle-and-abandoned-cleanup.md
- docs/transaction-system.md
- docs/checkpoint-and-recovery.md
- docs/process/unit-test.md

Design decision:

- Use a small `ChangeNotifier` helper for "wait until any change after this
  epoch" semantics.
- Use `EventNotifyOnDrop` or an equivalent notifying completion wrapper for
  transaction prepare completion.
- Do not migrate unrelated waits. Lock, latch, catalog/table gate, buffer,
  free-list, completion, and engine-drain waits should stay unchanged unless
  they wait for "any change" rather than rechecking a protected predicate.

## Goals

1. Add a crate-private `ChangeNotifier` helper in `doradb-storage/src/notify.rs`.
   - It owns an `event_listener::Event` and monotonic `AtomicU64` epoch.
   - It exposes the current epoch with acquire ordering.
   - It exposes a notify method that increments the epoch and notifies all
     waiters.
   - It exposes a blocking `wait_since(observed_epoch)` style method that uses
     check-listen-recheck to avoid missed notifications.
   - It has focused unit tests for notifications that happen before listener
     registration and notifications that happen after a waiter starts.
2. Refactor `SessionRegistry` to delegate transaction change notifications to
   `ChangeNotifier`.
   - Preserve the existing public crate-private surface:
     `trx_change_epoch()`, `wait_for_trx_change_since(...)`, and
     `notify_trx_changed()`.
   - Preserve all current transaction shutdown behavior and active transaction
     classification.
   - Keep `SessionRegistry` tests passing through the shared helper rather than
     duplicating the helper logic locally.
3. Fix prepare waiter completion in `SharedTrxStatus`.
   - Replace raw `prepare_ev: Mutex<Option<Event>>` with a notifying completion
     object, preferably `Mutex<Option<EventNotifyOnDrop>>`.
   - Keep listener creation synchronized with the preparing state and the event
     slot.
   - Ensure a listener returned while the transaction is preparing is woken
     when prepare finishes.
   - Ensure a late listener lookup after prepare completion returns `None`
     instead of blocking.
4. Centralize prepare state transitions in `SharedTrxStatus`.
   - Provide methods for entering prepare, acquiring a prepare listener, and
     finishing prepare.
   - Commit and failed-precommit rollback paths should call those methods
     instead of manually storing `preparing` and taking `prepare_ev`.
   - Commit completion must publish CTS before waking prepare waiters.
   - Failed-precommit rollback must finish rollback/session cleanup before
     waking prepare waiters, matching the existing cleanup intent.
5. Add regression tests that exercise the prepare waiter fix.
   - Cover normal commit completion.
   - Cover failed-precommit rollback completion.
   - Cover late listener lookup after completion.
   - Prefer direct `SharedTrxStatus` or prepared/precommit transaction tests
     when they cover the production state transitions without new production
     test hooks.

## Non-Goals

1. Do not change public transaction, session, table, or engine APIs.
2. Do not change redo log format, group commit ordering, checkpointing,
   recovery, MVCC visibility, row undo semantics, deletion-buffer semantics, or
   secondary-index persistence.
3. Do not broadly replace raw `Event` uses across lock, latch, catalog, table,
   free-list, buffer allocation, completion, or engine lifecycle waits.
4. Do not add async variants of the change notifier unless implementation
   discovers an existing async transaction-change waiter that must share the
   helper.
5. Do not introduce a generic notification trait or abstraction hierarchy.
   A plain helper struct is sufficient for the two current notification
   semantics.
6. Do not add new unsafe code.

## Unsafe Considerations (If Applicable)

No new unsafe code is planned.

If implementation unexpectedly adds or modifies unsafe code, apply
`docs/process/unsafe-review-checklist.md`, update adjacent `// SAFETY:`
comments, and refresh the unsafe inventory with:

```bash
tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md
```

## Plan

1. Extend `doradb-storage/src/notify.rs`.
   - Keep `EventNotifyOnDrop` available for existing buffer, file, and
     free-list users.
   - Add a crate-private `ChangeNotifier`:

     ```rust
     pub(crate) struct ChangeNotifier {
         event: Event,
         epoch: AtomicU64,
     }
     ```

   - Implement `new`, `Default`, `epoch`, `notify`, and `wait_since`.
   - Use acquire load for `epoch`, acq-rel increment for `notify`, and
     `notify(usize::MAX)` to wake all waiters.
   - Implement `wait_since` as:
     1. check whether the current epoch differs from the observed epoch;
     2. register a listener;
     3. check the epoch again;
     4. wait only if the epoch is still unchanged.
   - Add tests in the existing inline `tests` module:
     - `wait_since` returns immediately after a prior notification;
     - `wait_since` blocks until a future notification;
     - epoch increments monotonically across notifications.

2. Refactor `doradb-storage/src/session.rs`.
   - Replace `trx_changed: Event` and `trx_change_epoch: AtomicU64` with one
     `trx_changes: ChangeNotifier` field.
   - Remove direct `event_listener::{Event, Listener, listener}` and atomic
     imports from `session.rs` when they are no longer needed by other session
     code.
   - Keep `SessionRegistry::trx_change_epoch`,
     `SessionRegistry::wait_for_trx_change_since`, and
     `SessionRegistry::notify_trx_changed` as thin wrappers.
   - Preserve current call sites in session finish, abandon, handle abandon,
     checkout return, and shutdown cleanup scanning.
   - Keep the existing session-registry notification tests, adjusted only for
     import changes if needed.

3. Refactor prepare state in `doradb-storage/src/trx/mod.rs`.
   - Import `EventNotifyOnDrop` from `crate::notify`.
   - Change `SharedTrxStatus` from raw `Event` storage to notifying completion
     storage:

     ```rust
     prepare_ev: Mutex<Option<EventNotifyOnDrop>>
     ```

     A different private field name is acceptable if it makes the completion
     semantics clearer.
   - Move prepare-state mutation into `SharedTrxStatus` methods:
     - `mark_preparing(&self)` installs a new notifier and stores
       `preparing = true`.
     - `prepare_listener(&self)` returns a listener only when the transaction
       is still preparing and a notifier exists.
     - `finish_preparing(&self)` stores `preparing = false`, takes the notifier,
       and drops it so registered listeners are woken.
     - If useful, provide `commit_prepared(&self, cts: TrxID)` or similar to
       store CTS and finish prepare in one method so commit ordering stays
       explicit.
   - Keep `TrxContext::mark_preparing` as a small wrapper around
     `SharedTrxStatus::mark_preparing` if that preserves local call sites.
   - Preserve the existing ordering requirement:
     - normal commit stores `status.ts = cts` before waking waiters;
     - failed-precommit rollback performs rollback/session cleanup before
       waking waiters.

4. Update transaction terminal paths.
   - In `PrecommitTrx::commit`, replace the manual block that stores CTS,
     clears `preparing`, and drops `prepare_ev` with the centralized
     `SharedTrxStatus` method.
   - In `PrecommitTrxPayload::release_prepare_waiters`, delegate to the same
     centralized finish method.
   - Ensure fatal failed-precommit retention still releases prepare waiters
     before retaining payload ownership, matching the current behavior.
   - Preserve `PreparedTrx` and `PrecommitTrx` drop assertions.

5. Preserve row write retry behavior.
   - Keep `RowUndoHead::preparing()` and `RowUndoHead::prepare_listener()`
     behavior compatible with `trx/row.rs`.
   - Keep `LockUndo::Preparing(Option<EventListener>)`.
   - In `table/access.rs` and `table/mem_table.rs`, preserve the current logic:
     wait when a listener exists, otherwise loop and recheck the row state.
   - Do not hold new locks across `.await`.

6. Add prepare waiter regression tests.
   - Add tests in `doradb-storage/src/trx/mod.rs` inline tests unless a more
     local test module is clearly better.
   - Test normal commit:
     - create a transaction with ordered commit effects;
     - prepare it and get a listener from its shared status;
     - commit the precommit transaction;
     - assert the listener wakes and the late listener lookup is `None`.
   - Test failed-precommit rollback:
     - create a transaction with ordered commit effects;
     - prepare and fill CTS;
     - get a prepare listener;
     - run `rollback_failed_precommit().await`;
     - assert the listener wakes and late listener lookup is `None`.
   - Add a direct helper-level test if needed to prove listener creation races
     are covered without timing-sensitive sleeps.
   - Avoid production-only test hooks unless a real production path cannot
     expose the state transition.

7. Update comments and docs where behavior changes.
   - Refresh comments near `SharedTrxStatus::prepare_listener` to explain that
     prepare means CTS has been reserved/assigned while ordered completion is
     still pending, and waiters must wake on either commit or failed-precommit
     rollback.
   - Update `notify.rs` docs for the two helper semantics.
   - No concept-level docs are required unless implementation materially changes
     transaction lifecycle behavior beyond the helper extraction and wake fix.

8. Validate.
   - Run formatting:

     ```bash
     cargo fmt --all
     ```

   - Run strict lint:

     ```bash
     cargo clippy -p doradb-storage --all-targets -- -D warnings
     ```

   - Run the standard validation pass:

     ```bash
     cargo nextest run -p doradb-storage
     ```

   - Run focused coverage for changed notification/session/transaction paths:

     ```bash
     tools/coverage_focus.rs \
       --path doradb-storage/src/notify.rs \
       --path doradb-storage/src/session.rs \
       --path doradb-storage/src/trx
     ```

   - Target at least 80% focused coverage for the changed paths, following
     `docs/process/unit-test.md`.

## Implementation Notes


## Impacts

- `doradb-storage/src/notify.rs`
  - Add `ChangeNotifier`.
  - Keep `EventNotifyOnDrop`.
  - Add or adjust inline unit tests for helper semantics.
- `doradb-storage/src/session.rs`
  - Replace local transaction-change event/epoch state with `ChangeNotifier`.
  - Keep `SessionRegistry` transaction-change wrapper methods and behavior.
- `doradb-storage/src/trx/mod.rs`
  - Change `SharedTrxStatus` prepare notifier storage.
  - Centralize prepare enter/listen/finish behavior.
  - Update `TrxContext::mark_preparing`, `PrecommitTrx::commit`, and
    failed-precommit release paths.
  - Add prepare waiter regression tests.
- `doradb-storage/src/trx/undo/row.rs`
  - Preserve `RowUndoHead::preparing` and `prepare_listener` behavior, with
    only type-driven adjustments if needed.
- `doradb-storage/src/trx/row.rs`
  - Preserve `LockUndo::Preparing` retry behavior.
- `doradb-storage/src/table/access.rs`
  - Preserve async wait-and-retry behavior for preparing row owners.
- `doradb-storage/src/table/mem_table.rs`
  - Preserve async wait-and-retry behavior for preparing row owners.

## Test Cases

1. `ChangeNotifier::wait_since` returns immediately when `notify` already
   advanced the epoch before listener registration.
2. `ChangeNotifier::wait_since` blocks while the epoch is unchanged and wakes
   after a later `notify`.
3. `ChangeNotifier` epochs advance monotonically across repeated
   notifications.
4. Existing `SessionRegistry::wait_for_trx_change_since` tests still pass
   through the shared helper.
5. A prepare listener registered before normal commit completion wakes after
   `PrecommitTrx::commit`.
6. Normal commit publishes CTS before prepare waiters wake.
7. A late prepare listener lookup after normal commit completion returns
   `None`.
8. A prepare listener registered before failed-precommit rollback wakes after
   rollback releases prepare waiters.
9. A late prepare listener lookup after failed-precommit rollback returns
   `None`.
10. The row write preparing-owner retry path still treats `None` listeners as
    progress and rechecks instead of blocking.
11. No unrelated predicate wait path changes behavior.
12. `cargo clippy -p doradb-storage --all-targets -- -D warnings` passes.
13. `cargo nextest run -p doradb-storage` passes.
14. Focused coverage for changed notification/session/transaction paths meets
    the repository review bar or documents an acceptable reason.

## Open Questions

No blocking open questions.

The broader idea of migrating additional `Event` wait sites is intentionally
out of scope. If implementation reveals another wait that truly waits for "any
change after epoch" rather than a protected predicate, capture that as a
separate backlog item instead of expanding this task.
