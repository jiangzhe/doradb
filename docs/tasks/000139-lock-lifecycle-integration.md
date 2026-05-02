---
id: 000139
title: Lock Lifecycle Integration
status: proposal
created: 2026-05-01
github_issue: 606
---

# Task: Lock Lifecycle Integration

## Summary

Implement Phase 2 of RFC-0016 by wiring the standalone logical lock manager
into engine, session, statement, and transaction lifecycle state. This task
registers the lock manager as shared engine runtime state, adds deterministic
owner cleanup for statement, transaction, and session owners, and adds a
transaction-level table-lock cache on mutable `ActiveTrx` state without putting
interior mutable lock state inside `TrxContext`.

This task intentionally stops before metadata/read/write path enforcement,
`CREATE TABLE` locking, public table-lock APIs, or row-lock-manager
replacement.

## Context

RFC-0016 introduces a logical lock manager for catalog namespace, table
metadata, and table data resources. Phase 1 implemented the standalone
`doradb-storage/src/lock/` core with resource/mode/owner types,
compatibility, FIFO-compatible granting, immediate-only conversion, owner
cleanup, and debug snapshots.

Phase 2 attaches that core to the storage runtime. The hard part is lifecycle:
locks can be owned by statements, transactions, or sessions, while transaction
commit/rollback already moves state through `ActiveTrx`, `PreparedTrx`,
`PrecommitTrx`, log-group completion, and session completion paths. Owner
release must be authoritative across all of those paths so granted locks and
pending waiters cannot leak.

The RFC also requires transaction-level lock caching outside `TrxContext`.
`TrxContext` remains the immutable transaction identity/runtime view used by
table access and read proofs. Mutable lock state belongs on `ActiveTrx`, either
as a dedicated adjacent field or a narrow non-rollbackable substate. This task
chooses a dedicated `TrxLockState` field on `ActiveTrx` to keep lock caching
separate from rollbackable statement effects, redo, undo, and ordered-commit
classification.

Issue Labels:
- type:task
- priority:high
- codex

Parent RFC:
- docs/rfcs/0016-logical-lock-manager.md

Design references:
- `docs/rfcs/0016-logical-lock-manager.md`
- `docs/engine-component-lifetime.md`
- `docs/transaction-system.md`
- `docs/process/unit-test.md`
- `doradb-storage/src/lock/mod.rs`
- `doradb-storage/src/engine.rs`
- `doradb-storage/src/session.rs`
- `doradb-storage/src/trx/mod.rs`
- `doradb-storage/src/trx/stmt.rs`
- `doradb-storage/src/trx/sys.rs`
- `doradb-storage/src/trx/log.rs`

## Goals

- Register one shared `LockManager` as a normal engine component and expose
  its `QuiescentGuard` through narrow engine/runtime accessors for session and
  transaction code.
- Rename the phase-1 statement owner sequence alias from `StatementSeq` to
  `StmtNo`, and use `LockOwner::Statement(TrxID, StmtNo)` terminology in new
  lifecycle code.
- Add statement-number generation for `ActiveTrx::exec` so every scoped
  statement can derive a stable statement lock owner.
- Release statement-owned locks and pending waiters after statement success or
  after statement failure cleanup completes.
- Add `TrxLockState` as a separate mutable `ActiveTrx` field, not as part of
  `TrxContext`.
- Track the strongest transaction-owned lock already cached per
  `LockResource`, using `LockMode::covers(...)` semantics from the lock core.
- Add narrow internal helpers for transaction lock acquisition/cache lookup so
  later phases can reuse them without changing `TrxContext`.
- Release transaction-owned locks and pending waiters on every terminal
  transaction path:
  - normal rollback;
  - statement rollback fatal discard;
  - readonly/no-op commit discard;
  - ordered commit success;
  - commit durability failure/abort.
- Keep statement-owned cleanup separate from transaction-owned cleanup even
  when both owners share the same transaction id.
- Release session-owned locks and pending waiters when the session state is
  dropped.
- Preserve existing row undo, CDB, index write-conflict, checkpoint, recovery,
  and DDL behavior.

## Non-Goals

- Do not acquire `TableMetadata(S)` for MVCC reads in this task.
- Do not acquire or enforce `TableData(IX)` for row writes in this task.
- Do not integrate `CREATE TABLE` with `CatalogNamespace(X)` in this task.
- Do not add `Session::lock_table`, `Session::unlock_table`, or public
  explicit table-lock APIs in this task.
- Do not change row undo heads, `ColumnDeletionBuffer` ownership, row-level
  `WriteConflict` behavior, or secondary-index conflict semantics.
- Do not add row lock resources, deadlock detection, timeout APIs, blocking
  conversion, SQL lock syntax, lock metrics, or lock wait graph diagnostics.
- Do not change `TableAccess` method signatures for phase-2 lifecycle work.
  Phase 3 can decide whether DML integration uses pre-acquisition wrappers or
  passes a narrow lock handle into selected write APIs.
- Do not make logical lock acquisition itself a reason to require durability,
  redo logging, ordered commit, or rollbackable statement effects.

## Unsafe Considerations

No new unsafe code is expected. The implementation should use the existing
safe `LockManager` API and safe Rust ownership around engine/session/transaction
lifecycle state.

If unsafe code is unexpectedly added or changed, apply the repository unsafe
review process before resolving this task:

1. Describe affected modules and why unsafe is required.
2. Update every affected `// SAFETY:` comment with concrete invariants.
3. Refresh the unsafe inventory if required.
4. Add targeted tests for the changed unsafe boundary.

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Attach the lock manager to engine runtime state.
   - Implement `LockManager` as a registry component with no worker or explicit
     shutdown action.
   - Store only `QuiescentGuard<LockManager>` on `EngineInner`.
   - Register it in `EngineConfig::build` before components that need runtime
     lock access.
   - Add narrow accessors on `Engine`, `EngineRef`, or `EngineInner` as needed
     by session and transaction lifecycle code.
   - Follow `docs/engine-component-lifetime.md`: component ownership stays in
     the registry while sessions and transactions retain cloneable guards.

2. Normalize statement owner naming.
   - Rename the lock module's statement-number alias from `StatementSeq` to
     `StmtNo`.
   - Keep the owner shape as `LockOwner::Statement(TrxID, StmtNo)`.
   - Update tests and docs/comments in touched code to use statement-number
     terminology.

3. Add transaction lock state.
   - Add `TrxLockState` in `doradb-storage/src/trx/mod.rs` or a closely scoped
     adjacent module.
   - Store the strongest cached transaction-owned mode per `LockResource`.
   - Do not store `LockManager` inside `TrxLockState`; pass the component guard
     at acquisition and release boundaries.
   - Keep it non-rollbackable and separate from `TrxEffects`.
   - Ensure it does not affect `TrxEffects::require_durability`,
     `TrxEffects::require_ordered_commit`, or `TrxEffects::readonly`.
   - Add assertions/helpers so cache entries are cleared only after the lock
     manager releases the corresponding transaction owner.

4. Add narrow transaction acquisition helpers.
   - Add internal `ActiveTrx` methods for transaction-owned lock acquisition
     and cache checks.
   - On cache hit where the held mode covers the requested mode, skip the lock
     manager call.
   - On successful acquisition or immediate conversion, update the cache to the
     strongest held mode.
   - Propagate lock-manager errors for invalid modes, unsupported conversion,
     upgrade-would-block, and released waiters.
   - Do not expose early transaction unlock APIs.

5. Add statement numbering and statement-owner cleanup.
   - Add a monotonic statement-number source on `ActiveTrx`.
   - Construct `LockOwner::Statement(trx_id, stmt_no)` for each `exec` scope.
   - Extend `Statement` with narrow owner/lock-manager access only if needed
     for tests and later phases.
   - Preserve the existing `Statement::ctx()`, `effects_mut()`, and
     `ctx_and_effects_mut()` call shape so current table/catalog call sites do
     not churn.
   - Release the statement owner after successful callback effects merge into
     the transaction.
   - On callback error, release the statement owner only after statement
     rollback or fatal discard cleanup has completed.

6. Release transaction owners across all terminal paths.
   - Ensure active rollback releases the transaction owner after row/index
     rollback and effect cleanup.
   - Ensure fatal statement rollback discard releases transaction-owned locks
     while leaving cleanup idempotent enough for poison/error paths.
   - Carry the transaction owner/cache state through `PreparedTrx` and
     `PrecommitTrx` as needed after `ActiveTrx::prepare` consumes the active
     transaction.
   - Release transaction locks in the readonly/no-op commit discard path even
     though lock acquisition is not a transaction effect.
   - Release transaction locks after ordered commit success and after commit
     durability failure/abort.
   - Keep release ordering consistent with RFC-0016: transaction locks remain
     held until commit finalization or rollback cleanup has made uncommitted
     table obligations safe.

7. Release session owners.
   - Add session-owner cleanup to `SessionState` drop or another deterministic
     terminal cleanup path.
   - Use `LockOwner::Session(session_id)`.
   - Do not add explicit session table-lock APIs yet; tests may seed
     session-owned locks through crate-internal helpers or direct lock-manager
     access where appropriate.

8. Add focused tests.
   - Prefer inline `#[cfg(test)] mod tests` in touched files.
   - Use lock-manager debug snapshots to verify release behavior without
     widening runtime APIs solely for tests.
   - Keep test-only helpers under `#[cfg(test)]`.
   - Avoid adding standalone test-support modules unless absolutely necessary.

9. Run validation.
   - `cargo fmt`
   - `cargo build -p doradb-storage`
   - `cargo nextest run -p doradb-storage`
   - `tools/coverage_focus.rs --path doradb-storage/src/lock`
   - `tools/coverage_focus.rs --path doradb-storage/src/trx`
   - If coverage tooling is unavailable locally, record the blocker during
     checklist and still run the supported nextest validation.

## Implementation Notes

## Impacts

- `doradb-storage/src/engine.rs`
  - Add the shared lock manager runtime field and accessors.
- `doradb-storage/src/lock/mod.rs`
  - Rename `StatementSeq` to `StmtNo`.
  - Continue to provide lock owner, mode coverage, release, and debug snapshot
    support used by lifecycle tests.
- `doradb-storage/src/session.rs`
  - Release session-owned locks when `SessionState` reaches terminal drop.
  - Use existing session id generation from phase 1.
- `doradb-storage/src/trx/mod.rs`
  - Add `TrxLockState` beside `TrxEffects`.
  - Add statement numbering on `ActiveTrx`.
  - Add transaction-owned acquisition/cache helpers.
  - Carry and release transaction lock ownership across prepare, commit,
    rollback, readonly discard, and fatal discard paths.
- `doradb-storage/src/trx/stmt.rs`
  - Optionally expose statement owner information or narrow lock helper access
    while preserving current context/effects APIs.
- `doradb-storage/src/trx/sys.rs`
  - Release transaction locks on active rollback and readonly/no-op commit
    discard.
- `doradb-storage/src/trx/log.rs` and `doradb-storage/src/trx/group.rs`
  - Preserve transaction-owner cleanup through detached-session commit paths,
    ordered commit success, and commit failure/abort.
- `docs/rfcs/0016-logical-lock-manager.md`
  - No design-phase edit is required. During `task resolve`, synchronize the
    phase-2 implementation summary and status.

## Test Cases

- Engine construction creates one shared `LockManager` component visible
  through cloned runtime guards.
- `LockOwner::Statement` uses `StmtNo` terminology and statement numbers are
  unique within one `ActiveTrx`.
- Statement success releases only the matching statement owner after effects
  merge; transaction-owned locks for the same transaction id remain held.
- Statement callback error releases statement-owned locks after statement
  rollback succeeds.
- Fatal statement rollback cleanup releases both statement-owned and
  transaction-owned locks without leaking waiters.
- Transaction rollback releases transaction-owned granted locks and pending
  waiters after rollback cleanup.
- Readonly/no-op commit releases transaction-owned locks even though those
  locks do not make the transaction require ordered commit.
- Ordered commit success releases transaction-owned locks on the commit
  finalization path.
- Commit durability failure or precommit abort releases transaction-owned locks
  and marks the session rolled back as before.
- Session state drop releases session-owned granted locks and pending waiters.
- Transaction lock cache skips repeated acquisition when the cached mode covers
  the requested mode.
- Transaction lock cache records a stronger successfully granted mode and
  propagates immediate-only conversion errors without corrupting cache state.
- Statement owner cleanup does not release transaction owner entries for the
  same transaction id, and transaction owner cleanup does not release session
  owner entries for the same session.
- Existing row-level write conflicts through row undo/CDB remain unchanged.

## Open Questions

- Phase 3 should decide whether table DML integration uses pre-acquisition
  wrappers around existing `TableAccess` calls or passes a narrow transaction
  lock handle into selected write APIs.
- Phase 4 should decide the exact public session/transaction table-lock API
  names and timeout/cancellation policy before broad user-facing waits are
  exposed.
