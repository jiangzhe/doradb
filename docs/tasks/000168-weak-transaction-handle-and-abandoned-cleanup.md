---
id: 000168
title: Weak Transaction Handle And Abandoned Cleanup
status: proposal
created: 2026-06-04
github_issue: 683
---

# Task: Weak Transaction Handle And Abandoned Cleanup

## Summary

Implement RFC-0019 Phase 6 by replacing the public transaction facade's strong
runtime ownership with a non-cloneable weak transaction handle over the Phase 5
session-owned stable transaction entry. Public transaction drop becomes an
idempotent abandonment signal, while explicit `commit().await` and
`rollback().await` remain the only user-visible graceful terminal paths.

Add registry-authoritative transaction resolution by `(SessionID, TrxID)`,
operation-local runtime attachment, private checkout/completion claims that
temporarily regain strong runtime ownership, and abandoned cleanup ownership
that runs rollback-equivalent cleanup without holding registry or session
guards across `.await`.

Issue Labels:
- type:task
- priority:medium
- codex

Parent RFC:
- docs/rfcs/0019-weak-public-runtime-handles.md

RFC Phase:
- Phase 6: Weak Transaction Handle And Abandoned Cleanup

Related Backlogs:
- docs/backlogs/000113-transaction-cancellation-safety.md
- docs/backlogs/000114-evaluate-async-engine-shutdown-api.md
- docs/backlogs/000116-general-session-runtime-pin-ownership.md

## Context

RFC-0019 Phase 5 moved active transaction mutable state into a stable
session-owned `TrxEntry` and introduced `TrxCheckout` so the public
`Transaction` facade no longer owns the mutable transaction core directly.
Phase 6 uses that stable entry as the registry-owned target for weak public
transaction handles.

The pre-Phase-6 transitional implementation stored a strong `EngineRef` inside
`TrxSessionRef`, and the public `Transaction` stored both `Arc<TrxEntry>` and
`Option<TrxSessionRef>`. During a checkout, the `TrxSessionRef` moved into
`TrxInner`, then returned to the facade when the operation completed. That kept
active transactions reachable for commit, rollback, lock cleanup, session
cleanup, and shutdown, but it also meant public transaction handles strongly
retained engine runtime state.

Phase 6 changes that ownership boundary:

- The public `Transaction` is a non-cloneable weak capability identified by
  `WeakEngineRef`, `SessionID`, and `TrxID`.
- The session registry remains the authoritative owner of the active
  `Arc<TrxEntry>`.
- Public transaction operations resolve `(SessionID, TrxID)` through
  `SessionRegistry`, then acquire a private claim or lease before touching
  transaction state.
- Strong `EngineRef`, `Arc<SessionState>`, `Arc<TrxEntry>`, and `PoolGuards`
  are held only inside short-lived private operation, terminal, or cleanup
  claims.
- Public `Transaction::drop` does not rollback inline, await, panic, or hold
  runtime state alive. It only records abandonment and queues cleanup when the
  engine is still reachable.

The implementation shape selected for this phase is:

- `TrxAttachment` owns the operation-local strong `EngineRef`, session
  reachability, transaction identity, and retained `PoolGuards`.
- `TrxRuntime<'r>` is a copyable borrowed view over `&TrxContext` and
  `&TrxAttachment`; table, row, and index code use it instead of attaching
  runtime state to immutable `TrxContext`.
- `TrxCheckout` remains the non-terminal RAII checkout for returning
  `TrxInner` to `TrxEntry` on ordinary drop.
- `TrxCompletionClaim` is the terminal/cleanup ownership composite
  `(Arc<TrxEntry>, TrxInner, TrxAttachment)` consumed through `into_parts()`.
- `Statement<'stmt>` is a scoped operational facade that borrows
  `&mut TrxInner` and `&TrxAttachment`; it does not own `TrxCheckout` or a
  cloned lock-manager guard.
- `SessionRegistry::resolve_trx` is the registry-authoritative transaction
  resolver, and transaction lifecycle notifications use a monotonic epoch so
  `wait_for_trx_change_since` cannot lose a pre-listener notification.

Phase Contract:

- Prerequisites:
  - RFC-0019 Phase 5 is complete.
  - Active transaction state lives in a stable session-owned `TrxEntry`.
  - Non-terminal mutable work is performed through Phase 5 checkout plumbing.
  - Session lifecycle cleanup can observe active, checked-out,
    checked-out-abandoned, committing, rolling-back, abandoned,
    cleanup-running, terminal, and failed transaction states.
  - Cleanup ownership can acquire rollback-eligible abandoned entries without
    holding registry guards across `.await`.
- Phase-local choices resolved by this task:
  - Use `(SessionID, TrxID)` as the public transaction identity. Do not add a
    generation unless implementation discovers a concrete transaction-id reuse
    ambiguity that `(SessionID, TrxID)` cannot prevent.
  - Use an id-only weak public `Transaction` handle rather than storing
    `Arc<TrxEntry>`, `Weak<TrxEntry>`, `TrxSessionRef`, or `EngineRef` in the
    public facade.
  - Use registry-authoritative resolution before every operation and terminal
    path.
  - Use `TrxCheckout` for non-terminal checked-out work and
    `TrxCompletionClaim` for explicit terminal and abandoned-cleanup ownership.
  - Use `TrxAttachment` plus `TrxRuntime<'r>` for runtime-bound operational
    capability.
  - Treat transaction-handle drop as an abandonment signal. Cleanup runs later
    under a cleanup owner.
  - Keep session-dropped-before-transaction explicit: dropping `Session` marks
    the session abandoned, but the still-live transaction handle may explicitly
    commit or rollback until the handle is dropped or shutdown starts cleanup.
  - Locate abandoned cleanup under transaction-system worker ownership, with a
    cleanup queue and a shutdown-drain path before component teardown.
  - Follow existing fatal rollback policy for cleanup failure: publish a failed
    cleanup state, preserve diagnostics, and poison storage when rollback
    invariants require it.
  - Define shutdown by entry state instead of relying on public transaction
    strong refs.
- After this phase:
  - Public transaction handles are weak capabilities.
  - Active transaction state remains session-owned.
  - Abandoned cleanup has a concrete owner and rollback-equivalent behavior.
  - Cleanup errors and shutdown interaction are explicit and tested.
- Following phase preserved:
  - RFC-0019 Phase 7 can migrate public table handles without having to move
    transaction mutable state or invent abandoned transaction cleanup.
- RFC phase-plan edits:
  - During `task resolve`, update RFC-0019 Phase 6 with this task path, issue
    number if one exists, implementation summary, and the selected id-only weak
    handle plus cleanup-worker policy.

## Goals

1. Replace public `Transaction` strong ownership with a weak, non-cloneable
   handle.
   - The public handle must not store `Arc<TrxEntry>`.
   - The public handle must not store `TrxSessionRef`.
   - The public handle must not store strong `EngineRef`.
   - The public handle should store weak engine reachability plus
     `SessionID`, `TrxID`, and local drop-suppression state.
2. Keep the public API shape stable where possible.
   - Preserve `Session::begin_trx() -> Result<Transaction>`.
   - Preserve `Transaction::exec(...)`.
   - Preserve `Transaction::commit(self).await`.
   - Preserve `Transaction::rollback(self).await`.
   - Preserve `Transaction::trx_id()` and status timestamp accessors where
     current callers rely on them.
3. Resolve active transaction entries through `SessionRegistry` by
   `(SessionID, TrxID)`.
   - Lookup `SessionID`, clone `Arc<SessionState>`, and drop any registry map
     guard immediately.
   - Inspect session lifecycle under a short session-state lock.
   - Accept only active lifecycle variants that still point at the requested
     `TrxID`.
   - Clone `Arc<TrxEntry>` while inside the short critical section, then
     release the session lock before operation work or `.await`.
   - Reject stale handles without mutating replacement or terminal
     transactions.
4. Introduce private checkout, completion, attachment, and runtime types for
   transaction work.
   - `TrxCheckout` owns the checked-out `TrxInner` for one non-terminal
     operation and returns it to `TrxEntry` on ordinary drop.
   - `TrxCompletionClaim` atomically claims explicit commit, explicit rollback,
     or rollback-equivalent cleanup ownership and is consumed into
     `(Arc<TrxEntry>, TrxInner, TrxAttachment)`.
   - `TrxAttachment` owns strong runtime/session reachability and retained
     `PoolGuards` only for the operation, terminal path, prepared commit
     handoff, or cleanup path.
   - `TrxRuntime<'r>` is the copyable borrowed capability passed to table, row,
     index, and catalog code.
   - These types must remain crate-private.
5. Make public `Transaction::drop` nonblocking, infallible, idempotent, and
   non-panicking.
   - Drop should best-effort upgrade weak engine reachability for a cleanup
     hint.
   - If upgrade fails, drop must return.
   - If the handle already entered explicit commit or rollback, drop must not
     mark the transaction abandoned.
   - Drop must not run rollback inline.
6. Add abandoned transaction state and cleanup ownership.
   - A dropped transaction handle marks the matching active entry abandoned and
     queues cleanup.
   - Cleanup must only claim rollback-eligible abandoned entries.
   - Cleanup must not override checked-out work, explicit rollback,
     committing, committed, terminal rolled-back, cleanup-running, or failed
     entries.
   - Cleanup must be duplicate-safe for repeated drop, repeated queueing,
     abandoned session cleanup, and shutdown scanning.
7. Preserve explicit terminal behavior.
   - `commit(self).await` and `rollback(self).await` must suppress public-drop
     abandonment before acquiring terminal ownership.
   - Terminal paths must resolve and claim the matching active entry by
     `(SessionID, TrxID)`.
   - Commit handoff must preserve Phase 4/5 ownership: after prepared
     transaction handoff, abandoned cleanup must not convert commit into
     rollback.
   - Explicit rollback and abandoned cleanup should share rollback-equivalent
     logic where practical, while keeping caller identity and diagnostics
     distinct.
8. Define shutdown behavior for weak transactions.
   - `try_shutdown()` must return `ShutdownBusy` when non-terminal active
     transaction entries, pending cleanup, cleanup-running, committing, or
     rolling-back work prevent immediate teardown.
   - Blocking `shutdown()` must not rely only on public transaction strong refs.
     It must inspect transaction/session lifecycle state and wait for or drain
     transaction cleanup before component shutdown.
   - Running active transactions with live handles should continue to block
     blocking shutdown until they commit, rollback, or are abandoned.
   - Checked-out operations should complete or return their checkout before
     shutdown cleanup can claim the entry; an abandoned checked-out operation is
     represented by `CheckedOutAbandoned` until the mutable core is returned.
   - Abandoned transaction handles and abandoned sessions may be claimed by the
     cleanup owner during shutdown.
   - Committing and rolling-back owners must finish instead of being stolen by
     cleanup.
9. Preserve storage correctness.
   - Do not change MVCC visibility rules.
   - Do not change redo log format or group commit ordering.
   - Do not change rollback semantics except for routing abandoned cleanup
     through the new cleanup owner.
   - Preserve logical lock release, active STS/GC bookkeeping, row undo,
     index undo, table root, checkpoint, and recovery semantics.
10. Update documentation and tests for the new weak transaction lifecycle.

## Non-Goals

1. Do not migrate public table APIs or table handles. That remains RFC-0019
   Phase 7.
2. Do not redesign MVCC, redo, group commit, rollback algorithms, logical lock
   semantics, table persistence, checkpoint, recovery, or catalog/table DDL.
3. Do not resolve the broader async cancellation-safety backlog beyond the
   abandoned-handle and cleanup-claim invariants required here.
4. Do not introduce an async public engine shutdown API.
5. Do not expose transaction registry lookup, weak upgrade, operation lease,
   terminal claim, cleanup claim, stable entry, or cleanup queue APIs publicly.
6. Do not make public `Transaction::drop` await, block on rollback, panic, or
   report cleanup errors directly to user code.
7. Do not keep `TrxSessionRef` as the public transaction ownership mechanism.
   A private helper with some similar fields is acceptable only if it represents
   a short-lived claim, not facade lifetime ownership.

## Unsafe Considerations (If Applicable)

No new unsafe code is planned.

This task should be implementable in safe Rust by using registry-authoritative
identity checks, atomics or locked entry-state transitions, RAII claims, and
worker-owned cleanup. If implementation unexpectedly adds or modifies unsafe
code, apply `docs/process/unsafe-review-checklist.md`, update adjacent
`// SAFETY:` comments, and refresh the unsafe inventory with:

```bash
tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md
```

Reference:
- docs/unsafe-usage-principles.md
- docs/process/unsafe-review-checklist.md

## Plan

1. Refactor the public transaction facade in `doradb-storage/src/trx/mod.rs`.
   - Change `Transaction` from facade-plus-entry owner into an id-only weak
     handle.
   - Store `WeakEngineRef`, `SessionID`, `TrxID`, status timestamp if required
     by public accessors, and a local flag that suppresses abandonment after
     explicit terminal ownership starts.
   - Remove the public facade's `Arc<TrxEntry>` field.
   - Remove the public facade's `Option<TrxSessionRef>` field.
   - Ensure `Transaction` remains non-cloneable.
   - Implement `Drop` so it best-effort signals abandonment through weak engine
     reachability and otherwise returns without side effects.
   - Keep public methods delegating to transaction-system/session-registry
     claim APIs rather than touching `TrxEntry` directly.

2. Replace `TrxSessionRef` lifetime ownership with attachment-local ownership.
   - Remove or retire `TrxSessionRef` as the object stored for the whole public
     transaction lifetime.
   - Move its strong runtime responsibilities into `TrxAttachment` retained by
     private checkout/completion/prepared/cleanup paths:
     `EngineRef`, `Arc<SessionState>`, `Arc<TrxEntry>`, `SessionID`, `TrxID`,
     and cloned `PoolGuards`.
   - Keep immutable transaction identity in `TrxContext`; expose runtime-bound
     table access through a copyable `TrxRuntime` value instead of attaching
     runtime handles to the checked-in transaction context.
   - Preserve session-local insert-page cache behavior by accessing
     `SessionState` through the active claim while the session still exists.
   - Preserve terminal session cleanup through registry methods keyed by
     `(SessionID, TrxID)`.
   - Keep claim structs crate-private.

3. Adjust begin-transaction plumbing in `doradb-storage/src/session.rs` and
   `doradb-storage/src/trx/sys.rs`.
   - Create `TrxEntry` during begin and install it into
     `SessionLifecycle::RunningActive`.
   - Return a weak public `Transaction` handle that names the installed entry.
   - Consider an internal return object such as `StartedTransaction { handle,
     entry }` if needed so `SessionState::begin_trx` can install the stable
     entry without making the public handle own it.
   - Preserve the single-active-transaction-per-session invariant.
   - Preserve STS/TrxID allocation, log partition selection, and active STS
     list insertion behavior.

4. Add registry-authoritative resolution helpers in `doradb-storage/src/session.rs`.
   - Add `SessionRegistry::resolve_trx` to resolve active entries by
     `(SessionID, TrxID)` for operation, terminal, and cleanup callers.
   - The helper should clone `Arc<SessionState>` from the registry and release
     the registry map guard immediately.
   - The helper should inspect lifecycle under the session-state lock and clone
     the matching `Arc<TrxEntry>`.
   - The helper must reject idle, closed, terminal, missing, and mismatched
     `TrxID` states as stale or inactive.
   - Add stale-completion helpers so commit, rollback, and cleanup only clear
     a session if it still points at the same `TrxID`.
   - Add transaction lifecycle change notifications so blocking shutdown can
     wait on weak active entries without relying on public strong refs.
   - Use a monotonic `trx_change_epoch` plus `wait_for_trx_change_since` so
     shutdown cannot lose a notification that happens before listener
     registration.

5. Extend transaction entry state in `doradb-storage/src/trx/mod.rs`.
   - Represent active, checked-out, explicit committing, explicit rolling-back,
     abandoned, cleanup-claimed or cleanup-running, terminal, and failed
     cleanup states.
   - Use explicit `TrxEntryState` variants including `Abandoned`,
     `CheckedOutAbandoned`, `CleanupRunning`, `Terminal`, and `Failed`.
   - Provide atomic or locked transition methods that enforce allowed claims.
   - `take_for_checkout` must reject checked-out-abandoned, abandoned,
     cleanup-owned, committing, rolling-back, terminal, and failed entries.
   - Terminal extraction must reject cleanup-owned, already terminal, failed,
     and mismatched entries.
   - Cleanup claim must reject checked-out, checked-out-abandoned, committing,
     explicit rolling-back, terminal committed, terminal rolled-back,
     cleanup-running, and failed entries.
   - `Session::in_trx()` and shutdown classification must treat pending,
     checked-out, checked-out-abandoned, committing, rolling-back, abandoned,
     and cleanup-running entries as non-terminal.

6. Implement private checkout and completion ownership.
   - `Transaction::checkout` resolves the active entry, creates
     `TrxAttachment`, and returns `TrxCheckout` for one non-terminal operation.
   - `TrxCheckout` returns the checked-out inner on ordinary drop through the
     Phase 5 entry restore path.
   - `Transaction::claim_terminal` resolves and atomically claims explicit
     commit or rollback before awaiting terminal work.
   - Terminal methods must suppress public-drop abandonment on the consumed
     handle before async terminal work begins.
   - `TrxCompletionClaim::cleanup` resolves abandoned entries, atomically
     claims cleanup ownership, and owns the strong refs needed for
     rollback-equivalent work.
   - `TrxCompletionClaim` should be a simple owning composite consumed by
     `into_parts()`, not an object with test-only drop assertions or partial
     take state.
   - No checkout or claim may hold a DashMap guard, session lifecycle lock,
     entry inner mutex, or global registry guard across `.await`.

7. Refactor transaction-system terminal paths in `doradb-storage/src/trx/sys.rs`.
   - Change `commit_transaction` to accept `TrxCompletionClaim` instead of
     relying on public facade-owned `TrxSessionRef` and `Arc<TrxEntry>`.
   - Change `rollback_transaction` and abandoned cleanup to accept
     `TrxCompletionClaim`.
   - Extract shared rollback cleanup logic so explicit rollback and abandoned
     cleanup perform equivalent undo/effect/lock/GC/session cleanup.
   - Preserve caller-specific diagnostics: explicit rollback errors should
     report explicit rollback context; abandoned cleanup errors should report
     cleanup reason and identity.
   - Preserve no-op prepared transaction cleanup behavior and group-commit
     handoff semantics.
   - Preserve fatal rollback poisoning behavior and apply it to failed
     abandoned cleanup.

8. Add abandoned cleanup queue and worker ownership.
   - Add a cleanup job type keyed by `(SessionID, TrxID)` and a cleanup reason
     such as transaction-handle drop, abandoned session, or shutdown drain.
   - Store the cleanup queue and worker handle under transaction-system worker
     ownership, alongside existing IO, GC, and purge workers.
   - Use the repository's existing channel/thread style where practical.
   - If the cleanup worker must run async rollback code, use the existing
     executor pattern used by tests and background work, such as a worker thread
     that `smol::block_on`s cleanup futures.
   - Make duplicate cleanup jobs cheap and safe. State transitions, not queue
     uniqueness, must decide whether cleanup is still needed.
   - Add a drain path that blocking engine shutdown can call before component
     teardown.
   - Ensure worker shutdown does not drop queued cleanup silently before the
     engine has either drained it, classified it as not needed, or recorded a
     failed-cleanup/poison state.

9. Wire abandonment sources.
   - `Transaction::drop`: signal handle abandonment and queue cleanup when the
     matching entry is still active and the engine is reachable.
   - `Session::drop`: mark session abandoned as today; do not immediately
     steal a still-live transaction handle before shutdown. If the transaction
     handle is already abandoned, queue cleanup.
   - `Session::close`: continue rejecting active transactions, including
     abandoned-active entries that still require cleanup.
   - Engine shutdown: after closing admission and draining admitted operations,
     inspect active transaction entries. Wait for live running handles to
     commit, rollback, or abandon; drain cleanup for abandoned entries; wait for
     committing, rolling-back, checked-out, and cleanup-running owners to finish
     according to their state.

10. Update engine shutdown classification in `doradb-storage/src/engine.rs`.
    - Do not use `Arc::strong_count(inner) == 1` as the only proof that no
      active transaction can still affect state.
    - Keep `try_shutdown()` nonblocking: after closing admission and draining
      admissions, return `ShutdownBusy` if active transaction entries or cleanup
      work remain.
    - Keep blocking `shutdown()` deterministic: wait on runtime refs and
      transaction lifecycle epoch changes, drain abandoned cleanup, then remove
      idle sessions and stop components.
    - Preserve idempotent shutdown and storage-poison independence.
    - Add tests for active, checked-out, abandoned, committing,
      cleanup-running, rolling-back, terminal, and failed-cleanup cases.

11. Update documentation.
    - Update `docs/transaction-system.md` with the weak public transaction
      handle, registry resolution, private claims, abandoned cleanup, and
      shutdown behavior.
    - Update `docs/architecture.md` only if the public ownership model summary
      needs a concise correction.
    - During task resolve, update RFC-0019 Phase 6 status and implementation
      summary.
    - Keep Phase 7 assumptions intact: table handle migration remains future
      work.

12. Validate.
    - Run `cargo fmt --all`.
    - Run `cargo clippy -p doradb-storage --all-targets -- -D warnings`.
    - Run `cargo nextest run -p doradb-storage`.
    - Run focused coverage for changed transaction/session/engine paths using
      `tools/coverage_focus.rs` as described in `docs/process/unit-test.md`,
      targeting at least 80% focused coverage for the modified Rust paths.
    - During implementation, also run focused checks for statement runtime
      borrowing, transaction lock release, shutdown waiting, and session
      notification behavior.

## Implementation Notes


## Impacts

- `doradb-storage/src/trx/mod.rs`
  - Public `Transaction` fields and drop behavior.
  - `TrxEntry`, `TrxEntryState`, `TrxInner`, `TrxCheckout`, `Statement`, and
    private `TrxCompletionClaim`, `TrxAttachment`, and `TrxRuntime` use.
- `doradb-storage/src/trx/sys.rs`
  - Begin transaction return plumbing.
  - Commit and rollback terminal ownership.
  - Shared rollback-equivalent cleanup logic.
  - Cleanup queue, cleanup worker, worker shutdown, and fatal cleanup
    diagnostics.
- `doradb-storage/src/session.rs`
  - Session lifecycle transitions.
  - Registry resolution by `(SessionID, TrxID)`.
  - Abandoned session handling.
  - Finish helpers and epoch-based lifecycle notifications.
  - `TrxAttachment` and retirement of `TrxSessionRef`.
- `doradb-storage/src/engine.rs`
  - Weak-handle-aware `try_shutdown()` and blocking `shutdown()`.
  - Shutdown classification and cleanup-drain integration.
- `doradb-storage/src/lib.rs`
  - Public transaction export should remain `Transaction`.
- `doradb-storage/src/trx/stmt.rs` and table/catalog call paths using
  transaction execution
  - `Statement<'stmt>` borrows `&mut TrxInner` and `&TrxAttachment`.
  - Table, row, index, and catalog paths pass copyable `TrxRuntime<'_>` values.
  - Statement and explicit table-lock paths borrow the runtime lock manager
    instead of cloning a statement-local guard.
- `docs/transaction-system.md`
  - Update lifecycle and ownership documentation.
- `docs/rfcs/0019-weak-public-runtime-handles.md`
  - Resolve-time phase status update after implementation.

## Test Cases

1. Dropping a readonly transaction handle does not panic, does not hold a strong
   engine ref, marks the transaction abandoned, cleanup rolls it back, and the
   session returns to idle or is removed if abandoned.
2. Dropping a transaction with row/index effects queues abandoned cleanup; the
   cleanup path rolls back effects, clears undo/effects, releases logical locks,
   updates active STS/GC bookkeeping, and clears the matching session.
3. Dropping `Session` before dropping or terminating `Transaction` marks the
   session abandoned but still allows the live transaction handle to explicitly
   `commit().await`.
4. Dropping `Session` before dropping or terminating `Transaction` still allows
   the live transaction handle to explicitly `rollback().await`.
5. Dropping both `Session` and `Transaction` triggers abandoned cleanup and then
   removes the abandoned idle session.
6. `Transaction::commit(self).await` suppresses drop abandonment before async
   commit work and abandoned cleanup cannot convert a committing transaction
   into rollback.
7. `Transaction::rollback(self).await` suppresses drop abandonment and races
   with duplicate cleanup queue entries without double rollback or double
   session completion.
8. Cleanup cannot claim a checked-out transaction. After the checkout returns,
   cleanup can claim it if the handle is abandoned.
9. Cleanup cannot claim committing, rolling-back, cleanup-running, terminal, or
   failed entries.
10. Stale `(SessionID, TrxID)` handles and stale completion callbacks cannot
    clear or mutate a later transaction in the same session.
11. A session cannot begin a replacement transaction until the previous
    abandoned transaction has reached terminal cleanup; it can begin one after
    cleanup clears the matching active entry.
12. `Session::in_trx()` reports true for active, checked-out, abandoned,
    committing, rolling-back, and cleanup-running states, and false after
    terminal cleanup.
13. `try_shutdown()` returns `ShutdownBusy` when a live active transaction,
    checked-out operation, pending abandoned cleanup, cleanup-running work,
    committing transaction, or rolling-back transaction remains.
14. Blocking `shutdown()` waits for live active handles to commit, rollback, or
    abandon; drains abandoned cleanup before component shutdown; and completes
    after cleanup reaches terminal state.
15. Blocking `shutdown()` handles already-abandoned sessions with active
    transactions by claiming cleanup after admission closes.
16. Storage poison caused by fatal abandoned cleanup records diagnostics,
    publishes failed-cleanup state, rejects future runtime admission, and still
    lets owner-side shutdown stop workers and components.
17. Public transaction drop after engine shutdown or failed weak upgrade is
    infallible and non-panicking.
18. The implementation removes the public facade's strong `EngineRef` retention:
    an idle live `Transaction` no longer increments runtime ref counts, while
    an active operation/terminal/cleanup claim does.
19. Documentation examples and lifecycle docs match the new weak handle and
    abandoned cleanup behavior.
20. `SessionRegistry::wait_for_trx_change_since` returns after changes that
    occur before listener registration and wakes for later transaction changes.
21. `Statement<'stmt>` does not own `TrxCheckout`, does not clone the lock
    manager, and still releases statement-owned locks before fatal checkout
    discard.
22. `TrxCompletionClaim` is consumed through `into_parts()` and has no
    production `Drop` assertion that only checks test-only partial-take state.

Validation commands:

```bash
cargo fmt --all
cargo clippy -p doradb-storage --all-targets -- -D warnings
cargo nextest run -p doradb-storage
cargo test -p doradb-storage session::tests -- --nocapture
cargo test -p doradb-storage trx::stmt::tests -- --nocapture
cargo test -p doradb-storage trx::tests::test_transaction_locks_release -- --nocapture
cargo test -p doradb-storage engine::tests::test_engine_shutdown_waits_for_active_transaction_to_finish -- --nocapture
```

Run focused coverage for changed transaction, session, and engine files or
directories with `tools/coverage_focus.rs` per `docs/process/unit-test.md`.

## Open Questions

No blocking open questions.

Private naming is resolved for this task: non-terminal work uses `TrxCheckout`,
terminal and cleanup ownership uses `TrxCompletionClaim`, runtime reachability
uses `TrxAttachment`, and operation-local table capability uses
`TrxRuntime<'r>`.

If implementation discovers a concrete `(SessionID, TrxID)` reuse ambiguity,
add a generation counter or equivalent stale-handle proof and document that
change during `task resolve`.
