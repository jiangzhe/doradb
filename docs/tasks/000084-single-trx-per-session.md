---
id: 000084
title: Single Transaction Per Session
status: implemented  # proposal | implemented | superseded
created: 2026-03-21
github_issue: 464
---

# Task: Single Transaction Per Session

## Summary

Enforce the documented single-active-transaction contract on `Session` by
making transaction entry an explicit `SessionState` transition and adding
regression coverage for same-session overlap rejection and post-commit and
post-rollback reuse.

## Context

`Session::try_begin_trx()` currently documents idle-session-only semantics, but
the begin path never marks `SessionState::in_trx` as active. As a result, one
session can start overlapping `ActiveTrx` instances even though commit and
rollback already assume a single live transaction and clear `in_trx` on exit.

This follow-up stays narrowly scoped to admission correctness. It does not
change transaction timestamps, MVCC rules, redo logging, or recovery behavior.
It also does not extend engine shutdown semantics beyond preserving the
existing `StorageEngineShutdown` rejection path for session admission.

Issue Labels:
- `type:task`
- `priority:medium`
- `codex`

Source Backlogs:
- `docs/backlogs/000058-fix-session-single-active-transaction-enforcement.md`

## Goals

1. Enforce that one `Session` can own at most one live `ActiveTrx` at a time.
2. Keep `Session::try_begin_trx() -> Result<Option<ActiveTrx>>` semantics:
   `Ok(None)` for an already-active session and `Err(Error::StorageEngineShutdown)`
   when engine admission is closed.
3. Preserve same-session reuse after both commit and rollback, including the
   readonly no-redo cleanup path.
4. Preserve overlapping transactions across distinct sessions.

## Non-Goals

1. Refactoring `TransactionSystem::begin_trx()` or `ActiveTrx::new()` into a
   broader admission or lifecycle API redesign.
2. Changing engine shutdown policy, graceful session drain behavior, or forced
   shutdown handling.
3. Changing MVCC visibility, redo logging, checkpoint, or recovery behavior.
4. Introducing engine-wide tracking of sessions or transactions beyond the
   per-session active flag.

## Unsafe Considerations (If Applicable)

Not applicable. This task is limited to safe Rust admission-state tracking and
regression tests.

## Plan

1. Add an explicit session-state transition helper in
   `doradb-storage/src/session.rs`, e.g. `SessionState::try_enter_trx()`, using
   `compare_exchange` to claim the session's single active-transaction slot.
2. Update `Session::try_begin_trx()` to perform the session-state claim inside
   `EngineInner::with_running_admission(...)` so shutdown rejection does not
   leave a partially-entered session state.
3. Keep transaction-exit state release at the existing cleanup points:
   - successful commit completion in `doradb-storage/src/trx/log.rs`;
   - rollback and readonly prepared-rollback cleanup in
     `doradb-storage/src/trx/sys.rs`.
4. Add focused regression tests in `doradb-storage/src/engine.rs`, alongside
   existing admission tests, for:
   - same-session second begin returns `Ok(None)` while the first transaction
     is still live;
   - same-session begin succeeds again after commit;
   - same-session begin succeeds again after rollback;
   - distinct sessions can still hold overlapping transactions;
   - shutdown rejection continues to return `StorageEngineShutdown`.

## Implementation Notes

1. Implemented the session admission fix in `doradb-storage/src/session.rs`
   by adding `SessionState::try_enter_trx()` with `compare_exchange(false,
   true, ...)` and moving the begin-path state claim inside
   `EngineInner::with_running_admission(...)`. This preserves `Ok(None)` for an
   already-active session and avoids leaving `in_trx` set when begin is
   rejected because engine admission is shut down.
2. Kept transaction exit cleanup at the existing commit and rollback paths.
   No changes were required in `doradb-storage/src/trx/log.rs` or
   `doradb-storage/src/trx/sys.rs`; the existing `SessionState::commit()` and
   `SessionState::rollback()` release logic remained correct once begin claimed
   the session slot.
3. Added regression coverage in `doradb-storage/src/engine.rs` for:
   same-session overlap rejection, same-session reuse after commit, rollback,
   and readonly commit, distinct-session overlap, and shutdown rejection not
   leaving the session marked active.
4. Verification completed with:
   - `cargo fmt --all`
   - `cargo test -p doradb-storage --lib engine::tests`
   - `cargo nextest run -p doradb-storage`
   - `cargo nextest run -p doradb-storage --no-default-features`
5. Tracking links:
   - GitHub issue: `#464`
   - Pull request: `#465`

## Impacts

Primary code paths:
- `doradb-storage/src/session.rs`
- `doradb-storage/src/engine.rs`
- `doradb-storage/src/trx/log.rs`
- `doradb-storage/src/trx/sys.rs`
- `doradb-storage/src/trx/mod.rs`

Primary interfaces and state:
- `Session::try_begin_trx()`
- `SessionState`
- `TransactionSystem::begin_trx()`
- transaction commit and rollback session cleanup paths

## Test Cases

1. Begin a transaction on one session, call `try_begin_trx()` again before the
   first transaction finishes, and assert `Ok(None)`.
2. Commit the first transaction, then assert the same session can begin a new
   transaction successfully.
3. Roll back the first transaction, then assert the same session can begin a
   new transaction successfully.
4. Exercise the readonly/no-redo commit path and verify the session is released
   for reuse afterward.
5. Start transactions from two distinct sessions concurrently and verify both
   are admitted successfully.
6. After shutdown admission closes, assert `Session::try_begin_trx()` still
   returns `Err(Error::StorageEngineShutdown)`.
7. Validate implementation with the documented sequential dual-pass runner:
   - `cargo nextest run -p doradb-storage`
   - `cargo nextest run -p doradb-storage --no-default-features`

## Open Questions

1. None for this task scope.
