---
id: 000141
title: Explicit Table Lock Interface And Validation
status: implemented
created: 2026-05-03
github_issue: 611
---

# Task: Explicit Table Lock Interface And Validation

## Summary

Implement Phase 4 of RFC-0016 by adding explicit table-lock APIs for
transaction and session callers, then validate their compatibility with the
logical lock manager, foreground MVCC reads, and row-write intent acquisition.

This task uses limited owner-group semantics so session-owned table locks are
practical for same-session work without introducing hidden `SIX` behavior. A
session-owned `TableData(X)` may cover same-session transaction write intent,
but a session-owned `TableData(S)` must not allow same-session row writes. Row
undo heads, deletion-buffer markers, index write ownership, and row-level
`WriteConflict` behavior remain unchanged.

## Context

RFC-0016 introduces a logical lock manager for `CatalogNamespace`,
`TableMetadata(table_id)`, and `TableData(table_id)`. Phase 1 implemented the
standalone lock-manager core. Phase 2 registered it with engine runtime state
and added statement, transaction, and session cleanup. Phase 3 routed
foreground table access through lock-aware `Statement` APIs: MVCC reads acquire
statement-owned `TableMetadata(S)` only, while row writes acquire
transaction-owned `TableMetadata(S)` followed by cached `TableData(IX)`.

Phase 4 adds the explicit table-lock interface required by future DDL and
eventual SQL-visible table locks. The original RFC text said same-session
transaction owners are not automatically compatible with session-owned locks.
That rule prevents hidden compatibility bugs, but it also makes
session-owned table locks weak for real use: a session could lock a table and
then block its own transaction from using the protection it just acquired.

This task therefore keeps the conservative part of the RFC, but narrows it into
a usable rule: same-session owners can use an already-held session table lock
only when the held mode covers the later request. `X` covers `IX`; `S` does not.
The task must update RFC-0016 wording to document this revised Phase 4 decision.

Issue Labels:
- type:task
- priority:high
- codex

Parent RFC:
- docs/rfcs/0016-logical-lock-manager.md

## Goals

- Add explicit transaction table-lock API on `ActiveTrx`.
  - Acquire transaction-owned `TableMetadata(table_id, S)` and
    `TableData(table_id, S)` for explicit shared table locks.
  - Acquire transaction-owned `TableMetadata(table_id, S)` and
    `TableData(table_id, X)` for explicit exclusive table locks.
  - Reuse the existing transaction lock cache so row-write `IX` acquisition and
    explicit transaction locks share one strongest-mode cache per resource.
- Add explicit session table-lock APIs on `Session`.
  - `Session::lock_table` acquires session-owned `TableMetadata(S)` and
    `TableData(S/X)`.
  - `Session::unlock_table` releases the session-owned metadata and data locks
    for one table.
  - Session drop continues to release all session-owned granted locks and
    pending waiters.
- Accept only `LockMode::Shared` and `LockMode::Exclusive` as explicit table
  lock requests. Reject `IntentShared` and `IntentExclusive`; those remain
  internal table-data intent modes used by row access and lock coverage.
- Add owner-group-aware lock acquisition for session, transaction, and
  statement owners that belong to the same session.
  - Same exact owner remains reentrant using the current cache/conversion
    behavior.
  - Same owner group is not automatically compatible.
  - A same-group request may proceed only when an existing same-group granted
    mode covers the requested mode for that resource.
  - Same-group incompatible or non-covered requests return a clear lock error
    instead of waiting on the caller's own session lock.
- Preserve the no-`SIX` policy. A session-owned `TableData(S)` must not allow a
  same-session row write needing `TableData(IX)`.
- Allow practical protected work under a session-owned exclusive lock:
  `Session::lock_table(table, X)` -> begin transaction -> row writes acquire
  transaction-owned `IX` under the same session group -> commit/rollback ->
  `Session::unlock_table(table)`.
- Prevent releasing a covering session table lock while the same session still
  owns an active transaction. `Session::unlock_table` should reject or otherwise
  fail while `Session::in_trx()` is true, so group-covered transaction locks
  cannot outlive the session lock that let them bypass external waiters.
- Preserve RFC acquisition order:
  `CatalogNamespace -> TableMetadata(table_id) -> TableData(table_id) -> row undo/CDB`.
- Keep MVCC read behavior from Phase 3: plain reads acquire
  statement-owned `TableMetadata(S)` and do not acquire `TableData`.
- Keep row-level MVCC ownership and conflicts unchanged.
- Update RFC-0016's explicit table-lock and same-session behavior text to match
  the limited owner-group semantics implemented by this task.

## Non-Goals

- Do not add SQL parser or executor syntax for user-visible `LOCK TABLE`.
- Do not add `ActiveTrx::unlock_table` or any early transaction-owned table
  unlock path.
- Do not add blocking lock conversion, downgrade APIs, deadlock detection,
  timeout/cancellation policy, wait graph diagnostics, lock metrics, or
  SQL-visible lock-table introspection.
- Do not add `SIX` or synthesize combined `S + IX` behavior.
- Do not treat all same-session owners as generally compatible.
- Do not migrate row ownership into the logical lock manager. Hot row undo,
  cold-row `ColumnDeletionBuffer`, unique-index branches, and index undo remain
  the fine-grained MVCC mechanisms.
- Do not require recovery, checkpoint, purge, bootstrap, replay, or no-trx
  catalog helpers to acquire logical locks.

## Unsafe Considerations

No new unsafe code is expected. The implementation should use existing safe
synchronization primitives, session/transaction lifecycle state, and the
existing lock-manager data structures.

If implementation unexpectedly adds or changes unsafe code, apply the
repository unsafe review process before resolving this task:

1. Describe affected modules and why unsafe is required.
2. Update every affected `// SAFETY:` comment with concrete invariants.
3. Refresh the unsafe inventory if required.
4. Add targeted tests for the changed unsafe boundary.

References:
- docs/unsafe-usage-principles.md
- docs/process/unsafe-review-checklist.md

## Plan

1. Add explicit table-lock request validation.
   - Add a narrow helper that accepts `LockMode::Shared` and
     `LockMode::Exclusive` for explicit table locks.
   - Reject `LockMode::IntentShared` and `LockMode::IntentExclusive` before
     acquiring either metadata or data locks.
   - Use the existing `InvalidLockMode` operation error unless implementation
     discovers a more specific existing error fits better.

2. Add owner-group identity to lock acquisition.
   - Introduce a small owner-group type, keyed by `SessionID`, or an equivalent
     internal representation.
   - Store the owner group alongside granted locks and queued waiters, while
     preserving existing exact-owner behavior.
   - Keep existing `LockManager::acquire` / `try_acquire` behavior available
     for tests and internal callers that do not provide group identity, or
     route them through a `None` group.
   - Add grouped acquisition entrypoints used by `Session`, `ActiveTrx`, and
     `Statement` lifecycle integration.

3. Define grouped compatibility and queue behavior.
   - Exact-owner reentrancy and same-owner immediate conversion continue to use
     `LockMode::covers` and existing lock-manager conversion errors.
   - For different owners in the same group, a granted same-group lock is
     considered non-conflicting only when its mode covers the requested mode on
     the same resource.
   - Different-owner same-group requests whose existing group mode does not
     cover the request must fail rather than wait, preventing self-deadlock.
   - Group-covered requests may proceed while older external waiters exist only
     when they are covered by an existing granted same-group lock. The session
     unlock restriction in this task prevents those covered transaction locks
     from outliving the session lock and extending external waits after the
     session lock is released.
   - Preserve FIFO-compatible granting for unrelated owners and owner groups.

4. Carry owner-group identity through transaction and statement lock state.
   - Derive a transaction's owner group from the attached `SessionState`.
   - Store that group on the transaction `OwnerLockState`.
   - Derive each statement's owner group from the same transaction/session
     identity.
   - Ensure `Statement::acquire_table_write_locks` uses grouped transaction
     acquisition so row writes can proceed under a same-session
     `TableData(X)`.
   - Preserve transaction-owned `TableData(IX)` acquisition for row writes even
     when covered by session-owned `TableData(X)`, so debug assertions,
     snapshots, and transaction cleanup remain transaction-local.

5. Add `ActiveTrx::lock_table`.
   - Use transaction-owned grouped acquisition.
   - Acquire `TableMetadata(S)` before `TableData(S/X)`.
   - Use the same transaction lock cache as row writes.
   - Do not expose any early transaction unlock.
   - Propagate lock-manager errors for invalid modes, unsupported conversions,
     upgrade-would-block cases, and released waiters.

6. Add `Session::lock_table` and `Session::unlock_table`.
   - Use session-owned grouped acquisition with group id equal to the session
     id.
   - Acquire `TableMetadata(S)` before `TableData(S/X)`.
   - Keep repeated same-mode calls idempotent at the lock-manager level; do not
     add reference-counted nested session locks in this task.
   - Allow same-owner immediate upgrade from session `S` to session `X` only
     when the existing lock-manager conversion rules grant it immediately.
   - Make `unlock_table` reject release while `self.in_trx()` is true.
   - Release both `TableData(table_id)` and `TableMetadata(table_id)` for the
     session owner when unlock is allowed.

7. Update RFC-0016 documentation.
   - Replace the current "same-session transaction owners are not
     automatically compatible" wording with the limited owner-group rule.
   - Document that `TableData(S)` does not permit same-session writes and that
     callers needing shared whole-table access plus writes must acquire
     `TableData(X)` in v1.
   - Keep SQL syntax, timeout/cancellation policy, deadlock detection, and
     online DDL outside this phase.

8. Add focused tests.
   - Prefer inline tests in the modules being changed.
   - Use real engine/session/transaction paths for integration behavior.
   - Use lock debug snapshots only for internal validation and panic-diagnostic
     checks, not as a new user-facing observability API.

## Implementation Notes

- Implemented explicit table-lock validation through `LockMode` so only
  `Shared` and `Exclusive` are accepted by public explicit table-lock APIs.
- Added session owner-group semantics in the logical lock manager. Grouped
  acquisition preserves exact-owner reentrancy, rejects non-covered same-group
  conflicts with `LockOwnerGroupConflict`, and allows covered same-session
  requests such as a transaction `IX` under a session-owned `TableData(X)`.
- Added `Session::lock_table`, `Session::unlock_table`, and
  `ActiveTrx::lock_table`. Transaction and statement lock state now carries
  the session owner group, and transaction table locks reuse the transaction
  lock cache.
- Added cancellation-safe metadata cleanup for two-step explicit table-lock
  acquisition. Fresh `TableMetadata(S)` grants are protected by a disarmable
  RAII guard until the corresponding `TableData(S/X)` acquire succeeds, with
  transaction cache insertion delayed until both locks are held.
- Updated RFC-0016 to document the implemented limited owner-group behavior,
  the no-`SIX` policy, the session unlock restriction while a transaction is
  active, and Phase 4 task linkage.
- Added integration coverage for invalid explicit modes, transaction and
  session shared/exclusive table locks, same-session covered and non-covered
  behavior, session unlock restrictions, repeated transaction cache reuse, and
  metadata cleanup on failure/cancellation.
- Task checklist found no unresolved required fixes and no deferred actionable
  backlog items.
- Validation passed:
  `cargo fmt --check`;
  `git diff --check`;
  `cargo check -p doradb-storage --tests`;
  `cargo clippy -p doradb-storage --all-targets -- -D warnings`;
  `cargo nextest run -p doradb-storage` (688 passed);
  `tools/coverage_focus.rs --path doradb-storage/src/lock --path doradb-storage/src/trx --path doradb-storage/src/session.rs --path doradb-storage/src/table/tests.rs`
  (overall 96.25%; lock 97.52%; trx 94.26%; session 91.64%;
  table tests 98.96%).

## Impacts

- `doradb-storage/src/lock/mod.rs`
  - Add owner-group storage and grouped acquisition semantics.
  - Preserve existing compatibility, coverage, conversion, cleanup, and debug
    snapshot behavior for non-grouped callers.
  - Extend tests for grouped coverage and non-covered same-group failures.
- `doradb-storage/src/trx/mod.rs`
  - Carry transaction owner-group identity.
  - Add explicit transaction table-lock API.
  - Ensure transaction cleanup still releases only transaction-owned locks.
- `doradb-storage/src/trx/stmt.rs`
  - Carry statement owner-group identity.
  - Use grouped transaction acquisition for row-write lock helpers.
  - Preserve statement-owned metadata read behavior.
- `doradb-storage/src/session.rs`
  - Add session explicit table-lock and unlock APIs.
  - Enforce no session unlock while the session is in an active transaction.
  - Preserve session-drop cleanup for all session-owned locks and waiters.
- `docs/rfcs/0016-logical-lock-manager.md`
  - Sync Phase 4 semantics and task linkage with the implemented owner-group
    decision.
- `docs/transaction-system.md`
  - Update only if implementation changes the documented lifecycle or
    same-session behavior beyond RFC wording.

## Test Cases

- `LockMode::IntentShared` and `LockMode::IntentExclusive` are rejected by
  explicit transaction and session table-lock APIs.
- Transaction `lock_table(S)` acquires transaction-owned `TableMetadata(S)` and
  `TableData(S)`, blocks an incompatible row writer from another transaction,
  and releases on commit and rollback.
- Transaction `lock_table(X)` acquires transaction-owned `TableMetadata(S)` and
  `TableData(X)`, blocks incompatible external readers/writers according to the
  table-data matrix, and uses the transaction lock cache for repeated requests.
- Session `lock_table(S)` allows same-session MVCC reads, but same-session row
  writes needing `TableData(IX)` fail with a clear lock error instead of
  waiting on the session's own shared table lock.
- Session `lock_table(X)` allows same-session transaction row writes to acquire
  transaction-owned `TableData(IX)` while external incompatible owners remain
  blocked.
- `Session::unlock_table` releases session-owned metadata and data locks when
  no transaction is active.
- `Session::unlock_table` fails while `Session::in_trx()` is true.
- Session drop releases session-owned granted locks and pending waiters.
- Same-group non-covered requests do not enqueue stale waiters.
- MVCC reads still acquire statement-owned `TableMetadata(S)` and do not
  acquire `TableData`.
- Existing row-level conflicts still return the current `WriteConflict` or
  `DuplicateKey` outcomes after logical table locks grant.
- Debug snapshots expose enough granted/waiting state to validate owner,
  resource, mode, and queue behavior for Phase 4 tests.
- Routine validation passes:
  `cargo nextest run -p doradb-storage`.

## Open Questions

- Timeout/cancellation policy remains future work before broad user-facing
  arbitrary waits or multi-table lock statements are exposed.
- SQL syntax and executor integration for user-visible table locks remain
  future work after this explicit storage-engine API exists.
