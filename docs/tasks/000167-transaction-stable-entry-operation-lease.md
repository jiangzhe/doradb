---
id: 000167
title: Transaction Stable Entry And Operation Lease
status: proposal
created: 2026-06-03
github_issue: 681
---

# Task: Transaction Stable Entry And Operation Lease

## Summary

Implement RFC-0019 Phase 5 by moving active transaction mutable state into a
session-owned stable transaction entry, adding an RAII operation execution handle for
checked-out mutable transaction work, and making transaction lifecycle state
visible to session cleanup and engine shutdown.

This task also renames the canonical public active transaction facade from
`ActiveTrx` to `Transaction`. The checked-out mutable core must be named
`TrxInner`, and the RAII checkout object must be named `TrxExec`.

Issue Labels:
- type:task
- priority:medium
- codex

Parent RFC:
- docs/rfcs/0019-weak-public-runtime-handles.md

RFC Phase:
- Phase 5: Transaction Stable Entry And Operation Lease

Related Backlogs:
- docs/backlogs/000113-transaction-cancellation-safety.md
- docs/backlogs/000116-general-session-runtime-pin-ownership.md

## Context

RFC-0019 Phase 5 sits between the Phase 4 terminal-operation preparation and
the future Phase 6 weak transaction handle migration. Phase 4 established the
minimum terminal ownership contract: explicit commit and rollback are the only
graceful terminal operations, queued `PrecommitTrx` owns commit completion after
the group-commit handoff, and cancellation or drop after that handoff must not
convert commit into rollback or leave a session permanently active.

The current storage crate still keeps active transaction mutable state directly
inside the public `ActiveTrx` facade. `SessionState` records only that a session
is active with a `trx_id`; it does not own the transaction core. The current
`TrxSessionRef` keeps a strong runtime pin and weakly references `SessionState`
so transaction terminal paths can reach engine runtime, release locks, and call
session completion callbacks.

Phase 5 changes that ownership boundary:

- `SessionState` owns a stable transaction entry while a session has an active
  transaction.
- The entry remains visible while mutable transaction work is checked out,
  committing, rolling back, terminal, or failed.
- Mutable work is performed through an RAII operation lease.
- Registry/session locks are held only while checking state, checking out the
  inner core, returning it, or publishing terminal/in-progress state.
- The current public transaction behavior remains explicit and user-driven:
  no weak public `TransactionHandle`, no drop-triggered abandonment, and no
  automatic abandoned rollback cleanup are added in this phase.

Phase Contract:
- Prerequisites:
  - RFC-0019 Phases 1 through 4 are complete.
  - Phase 4 terminal-operation completion ownership, commit handoff, shutdown
    observation, and runtime pin safety are in place.
  - The session registry can host a stable transaction entry without creating a
    session-to-engine strong cycle.
- Phase-local choices resolved by this task:
  - Use `Transaction` as the canonical public transaction facade.
  - Use `TrxInner` as the checked-out mutable transaction core.
  - Store the stable transaction entry inside `SessionState`.
  - Keep a transitional runtime pin reachable from the facade or entry until
    terminal completion, unless implementation proves an equivalent shutdown
    blocker that does not create a strong cycle.
  - Use an RAII `TrxExec` to return checked-out mutable state on
    ordinary drop and to publish terminal/in-progress state when commit,
    rollback, or fatal cleanup consumes ownership.
  - Remove terminal entries immediately after session completion unless a narrow
    diagnostic need is found during implementation.
- After this phase:
  - Active transaction state is session-owned and visible through stable
    entries.
  - Mutable transaction work uses operation leases.
  - Public transaction handles may still use the `Transaction` facade and the
    existing explicit drop contract.
  - Abandoned rollback cleanup is still not automated.
- Following phase preserved:
  - Phase 6 can build a weak public `TransactionHandle` over the stable entry
    and add abandoned cleanup owners without first moving the mutable core.
- RFC phase-plan edits:
  - During `task resolve`, update RFC-0019 Phase 5 to reference this task and
    update wording from `ActiveTrx` to `Transaction` if the implementation
    retires the old public spelling.

## Goals

1. Rename the canonical public active transaction facade to `Transaction`.
   - `Session::begin_trx()` must return `Result<Transaction>`.
   - `doradb-storage/src/lib.rs` must export `Transaction` as the public
     transaction type.
   - Do not keep `ActiveTrx` as the primary public export. If a temporary
     compatibility alias is required to keep the migration task tractable, make
     it explicitly transitional and update examples, docs, and crate tests to
     use `Transaction`.
2. Introduce a session-owned stable transaction entry for the one active
   transaction allowed per session.
3. Move the mutable active transaction fields currently owned by the public
   facade into `TrxInner`:
   - `TrxContext`;
   - `TrxEffects`;
   - transaction-owned `OwnerLockState`;
   - next statement number;
   - active/discarded state needed before terminal extraction.
4. Add a small registry-visible entry state model that covers at least:
   - `Active`;
   - `CheckedOut`;
   - `Committing`;
   - `RollingBack`;
   - `Terminal`;
   - `Failed`.
5. Add an RAII `TrxExec` for checked-out mutable work.
   - The lease must restore `TrxInner` to the stable entry on ordinary
     drop.
   - Commit, rollback, and fatal cleanup paths must consume or explicitly clear
     the lease so it does not restore a terminally owned inner.
   - No registry guard, session lifecycle guard, DashMap guard, or global guard
     may be held across user callbacks or `.await`.
6. Preserve closure-based statement execution as `Transaction::exec(...)`.
7. Preserve explicit consuming terminal APIs:
   - `Transaction::commit(self).await`;
   - `Transaction::rollback(self).await`.
8. Preserve the Phase 4 commit handoff contract:
   - before group-commit handoff, failures may route through existing rollback
     or discard behavior;
   - after `PreparedTrx` becomes queued `PrecommitTrx`, the transaction system
     or commit group owns session completion;
   - abandoned/dropped user observation must not convert an irreversible commit
     into rollback.
9. Preserve transaction state-specific ownership structs:
   - `PreparedTrx`;
   - `PrecommitTrx`;
   - `CommittedTrx`.
10. Keep active transaction lifecycle visible to session cleanup and shutdown.
    `Session::in_trx()`, `Session::close()`, abandoned-session handling, and
    engine shutdown must observe active, checked-out, committing, rolling-back,
    terminal, and failed states correctly.
11. Preserve current transaction, MVCC, redo, rollback, logical lock, and
    checkpoint semantics except for routing ownership through stable entries and
    leases.
12. Update transaction lifecycle documentation and public examples/tests from
    `ActiveTrx` wording to `Transaction` where this task changes public naming.

## Non-Goals

1. Do not implement a weak public `TransactionHandle`.
2. Do not make transaction handle drop an abandonment signal.
3. Do not add abandoned transaction cleanup, cleanup queues, cleanup workers,
   async cleanup executors, or automatic abandoned rollback.
4. Do not make engine shutdown a transaction cleanup owner.
5. Do not redesign MVCC, redo, group commit, rollback algorithms, logical lock
   semantics, table DDL, checkpoint/recovery, table-file roots, or buffer-pool
   internals.
6. Do not resolve the broader async cancellation semantics tracked by
   `docs/backlogs/000113-transaction-cancellation-safety.md` beyond the minimum
   lease-return and terminal-handoff invariants needed by RFC-0019 Phase 5.
7. Do not expose internal operation leases, weak-upgrade APIs, stable entries,
   or runtime pins as public APIs.
8. Do not migrate public table handles. That remains RFC-0019 Phase 7.

## Unsafe Considerations (If Applicable)

No new unsafe code is planned.

This task should be implementable in safe Rust through ownership moves,
interior mutability scoped to session-owned entries, RAII lease drop behavior,
and tests. If implementation unexpectedly adds or modifies unsafe code, apply
`docs/process/unsafe-review-checklist.md`, update adjacent `// SAFETY:`
comments, and refresh the unsafe inventory with:

```bash
tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md
```

Reference:
- docs/unsafe-usage-principles.md
- docs/process/unsafe-review-checklist.md

## Plan

1. Establish the public naming migration.
   - In `doradb-storage/src/trx/mod.rs`, rename the public facade struct from
     `ActiveTrx` to `Transaction`.
   - In `doradb-storage/src/lib.rs`, export `Transaction` as the canonical
     public type.
   - Update `Session::begin_trx()` and related public/internal signatures to
     return `Result<Transaction>`.
   - Update examples, tests, and docs touched by this task to use
     `Transaction`.
   - If keeping `ActiveTrx` as a temporary alias is necessary, document it as a
     transitional compatibility alias and do not use it in new docs/tests.

2. Split the facade from the mutable transaction core.
   - Move the current mutable fields from `Transaction` into
     `TrxInner`:
     - `ctx: TrxContext`;
     - `effects: TrxEffects`;
     - `lock_state: Option<OwnerLockState>`;
     - `next_stmt_no: StmtNo`;
     - active/discarded state.
   - Keep methods that directly mutate effects, lock state, statement number,
     or terminal ownership on `TrxInner` unless they are true public
     facade methods.
   - Keep immutable public identity helpers such as `trx_id()` and `sts()`
     available through `Transaction`.

3. Add the stable transaction entry to `SessionState`.
   - Add a private `TrxEntry` owned by `SessionState`.
   - The entry stores:
     - transaction identity;
     - `TrxEntryState`;
     - `Option<TrxInner>` or equivalent checked-in core storage;
     - enough runtime/session reachability to preserve shutdown waiting and
       terminal cleanup without creating a strong session-to-engine cycle.
   - Preserve the one-active-transaction-per-session rule currently enforced by
     `SessionLifecycle::RunningActive`.
   - Avoid duplicating authoritative active state in two places. If
     `SessionLifecycle` still records active status, it must agree with the
     entry and should not become a second lifecycle source of truth.

4. Implement entry checkout and return.
   - Add `TrxExec` as the only normal way to obtain
     `&mut TrxInner` across async transaction operations.
   - Checkout must:
     - validate session/transaction identity;
     - reject terminal, failed, committing, rolling-back, or already checked-out
       entries with an appropriate internal/lifecycle/operation error;
     - mark the entry `CheckedOut`;
     - move or borrow the inner core without keeping registry guards across
       await points.
   - Ordinary lease drop must restore the inner core and mark the entry
     `Active`.
   - Explicit consume methods must allow commit, rollback, or fatal cleanup to
     take ownership and publish the correct entry state without double-return.

5. Route statement and lock operations through leases.
   - Update `Transaction::exec(...)` to check out a `TrxExec`,
     construct the `Statement` from leased `TrxInner`, and return the
     inner to the entry after success or ordinary statement rollback.
   - Preserve the current statement behavior:
     - successful statement effects merge into transaction effects;
     - ordinary statement errors roll back statement-local effects and return
       the original error;
     - fatal statement rollback/access failure clears/discards the transaction,
       marks the entry failed or terminal as appropriate, and prevents later
       commit/rollback reuse.
   - Update `Transaction::lock_table(...)` and any test helpers that mutate
     transaction lock/effect state to use leases.

6. Route commit through terminal extraction.
   - `Transaction::commit(self).await` must acquire or consume a lease, publish
     `Committing` before or as part of terminal extraction, and pass the
     extracted `TrxInner` through the existing prepare/group-commit
     flow.
   - Preserve the readonly/no-op commit discard path and transaction-lock
     release behavior.
   - Preserve ordered no-log commit behavior.
   - Preserve the Phase 4 handoff boundary: after the prepared transaction is
     assigned a CTS and queued as `PrecommitTrx`, queued commit state owns
     session completion and lock/runtime cleanup.
   - Ensure stale completion cannot mutate a replacement transaction entry.

7. Route rollback through terminal extraction.
   - `Transaction::rollback(self).await` must acquire or consume a lease,
     publish `RollingBack`, and run the existing rollback logic on the extracted
     `TrxInner`.
   - Successful rollback must:
     - roll back index undo then row undo;
     - clear effects;
     - update rollback GC state;
     - release transaction-owned locks;
     - complete the owning session;
     - remove or terminally clear the stable entry.
   - Fatal rollback/access failure must preserve storage poison and discard
     behavior, mark the entry `Failed` or remove it only after safe cleanup, and
     prevent later facade reuse.

8. Update session cleanup and shutdown visibility.
   - Update `SessionState::in_trx`, `close_idle`, `abandon`,
     `finish_trx_commit`, `finish_trx_rollback`, and `shutdown_removal` to use
     the stable transaction entry state.
   - `Session::close().await` must reject active, checked-out, committing, and
     rolling-back transaction entries.
   - Dropping a public `Session` with an active transaction still marks the
     session abandoned but does not trigger automatic transaction rollback in
     this phase.
   - Engine shutdown must remain busy or wait while checked-out, committing, or
     rolling-back transaction work holds runtime liveness.
   - Terminal completion from commit or rollback must remove abandoned sessions
     as current behavior does.

9. Keep runtime pin ownership explicit.
   - Continue using the transitional strong runtime pin currently carried by
     transaction work, or replace it with an equivalent entry/lease-owned pin.
   - The selected design must block engine shutdown while transaction work still
     needs engine-owned components.
   - Do not create a strong cycle where an engine-owned session entry strongly
     retains an engine root that keeps the owning engine alive forever.

10. Update documentation.
    - Update `docs/transaction-system.md` to describe:
      - `Transaction` as the public facade;
      - session-owned stable transaction entries;
      - `TrxInner` checked out through `TrxExec`;
      - lifecycle-visible entry states;
      - unchanged commit handoff and rollback behavior.
    - Update RFC-0019 references if implementation changes the Phase 5 public
      naming contract from `ActiveTrx`/`Transaction` to canonical
      `Transaction`.
    - Keep `docs/backlogs/000113-transaction-cancellation-safety.md` and
      `docs/backlogs/000116-general-session-runtime-pin-ownership.md` open
      unless implementation fully resolves their broader scopes, which is not
      expected in this task.

11. Validate.
    - Run formatting:
      ```bash
      cargo fmt --check
      ```
    - Run the supported storage validation pass:
      ```bash
      cargo nextest run -p doradb-storage
      ```
    - Run focused coverage for changed transaction/session files, targeting at
      least 80% focused coverage where practical:
      ```bash
      tools/coverage_focus.rs \
        --path doradb-storage/src/session.rs \
        --path doradb-storage/src/trx/mod.rs \
        --path doradb-storage/src/trx/sys.rs
      ```
    - If implementation touches backend-neutral IO paths unexpectedly, also run:
      ```bash
      cargo nextest run -p doradb-storage --no-default-features --features libaio
      ```

## Implementation Notes

## Impacts

- `doradb-storage/src/lib.rs`
  - Public transaction export changes from `ActiveTrx`/`Transaction` aliasing to
    canonical `Transaction`.
- `doradb-storage/src/session.rs`
  - `Session::begin_trx`;
  - `Session::in_trx`;
  - `Session::close`;
  - `SessionRegistry`;
  - `SessionState`;
  - `SessionLifecycle`;
  - `TrxSessionRef` or its successor runtime/session reachability type.
- `doradb-storage/src/trx/mod.rs`
  - public `Transaction` facade;
  - new `TrxInner`;
  - new `TrxEntry`;
  - new `TrxEntryState`;
  - new `TrxExec`;
  - `TrxContext`;
  - `TrxEffects`;
  - `OwnerLockState`;
  - `Transaction::exec`;
  - `Transaction::commit`;
  - `Transaction::rollback`;
  - `PreparedTrx`;
  - `PrecommitTrx`;
  - `CommittedTrx`;
  - transaction tests and test helpers.
- `doradb-storage/src/trx/sys.rs`
  - `TransactionSystem::begin_trx`;
  - `TransactionSystem::commit`;
  - `TransactionSystem::rollback`;
  - readonly/no-op discard behavior.
- `doradb-storage/src/trx/group.rs`
  - only if commit-group ownership needs small signature updates for the
    extracted inner/prepared transaction flow.
- `doradb-storage/src/trx/stmt.rs`
  - only if statement construction needs lifetime or naming adjustments after
    `TrxInner` split.
- `doradb-storage/src/engine.rs`
  - shutdown tests and any assertions that describe active transaction runtime
    refs.
- `docs/transaction-system.md`
  - transaction lifecycle and commit/rollback ownership wording.
- `docs/rfcs/0019-weak-public-runtime-handles.md`
  - resolve-time Phase 5 sync and any naming update required by the final
    implementation.

## Test Cases

1. Public naming and begin behavior:
   - `Session::begin_trx()` returns a usable `Transaction`.
   - Public smoke/examples use `Transaction`, not `ActiveTrx`.
   - `doradb-storage` no longer requires `ActiveTrx` in public tests or docs.

2. Same-session transaction lifecycle:
   - beginning a second transaction in one session fails while the stable entry
     is active;
   - the same session can begin another transaction after commit;
   - the same session can begin another transaction after rollback;
   - stale commit/rollback completion for an old `trx_id` does not update a
     replacement transaction entry.

3. Lease checkout and return:
   - successful `Transaction::exec` returns `TrxInner` to the stable
     entry and leaves the entry active;
   - ordinary statement error rolls back only statement-local effects and
     returns `TrxInner` to the stable entry;
   - attempting overlapping mutable checkout through test hooks is rejected or
     impossible by construction;
   - lease drop after ordinary cancellation before terminal ownership restores
     the entry to an observable non-terminal state.

4. Fatal statement rollback/access behavior:
   - fatal statement rollback failure clears/discards transaction effects as
     current behavior requires;
   - the entry is marked failed/terminal or otherwise made impossible to reuse;
   - later commit/rollback attempts through the same facade return the existing
     discarded-transaction error shape.

5. Commit behavior:
   - readonly/no-op commit returns `TrxID::new(0)`, releases transaction locks,
     completes the session, and removes/clears the stable entry;
   - ordered no-log commit still enters ordered commit and publishes volatile
     CTS behavior as before;
   - durability-required commit preserves redo/group-commit behavior;
   - dropping the user commit future after group-commit handoff does not leave
     the session active and does not roll back the transaction;
   - commit handoff uses queued `PrecommitTrx` ownership for session completion.

6. Rollback behavior:
   - successful rollback rolls back index undo then row undo, clears effects,
     releases transaction locks, updates rollback GC state, completes the
     session, and clears/removes the entry;
   - fatal rollback access failure preserves storage poison behavior and
     prevents later facade reuse.

7. Session and shutdown behavior:
   - `Session::in_trx()` reports true for active and checked-out transaction
     entries;
   - `Session::close().await` rejects active, checked-out, committing, and
     rolling-back entries;
   - dropping a session with an active transaction keeps the session entry until
     explicit commit or rollback completes;
   - `try_shutdown()` returns `ShutdownBusy` while transaction work is active,
     checked out, committing, or rolling back;
   - blocking `shutdown()` waits until terminal transaction completion.

8. Lock ownership:
   - statement-owned locks still release after statement success and ordinary
     statement rollback;
   - transaction-owned locks still release on readonly commit, ordered commit,
     rollback, no-op discard, and fatal discard.

9. Regression coverage for existing storage behavior:
   - representative insert/update/delete transaction tests still pass;
   - recovery tests that create and commit transactions still pass after the
     `Transaction` rename;
   - transaction read proof and table-root binding tests still pass.

## Open Questions

1. Should a deprecated `ActiveTrx` compatibility alias remain for one release or
   be removed immediately?
   - Task acceptance makes `Transaction` canonical either way.
   - If a compatibility alias remains, it must be documented as transitional and
     not used by crate tests, examples, or new docs.
2. Should terminal entries be retained briefly for diagnostics?
   - Default answer for this task is no: remove or clear terminal entries after
     session completion.
   - Retain only if implementation finds a concrete diagnostic need that does
     not complicate Phase 6 cleanup ownership.
3. Should broader async cancellation semantics be planned before Phase 6?
   - This remains tracked by
     `docs/backlogs/000113-transaction-cancellation-safety.md`.
   - Phase 5 should preserve the minimum invariant that lease drop cannot
     override irreversible commit or leak checked-out mutable state.
