---
id: 000264
title: Integrate Engine Poison with Foreground Waiters
status: implemented  # proposal | implemented | superseded
created: 2026-08-08
github_issue: 962
---

# Task: Integrate Engine Poison with Foreground Waiters

## Summary

Implemented poison-aware wakeup for reversible foreground waiters. Queued
logical-lock acquisition and hot/cold row prepare waiting now return the first
stored `Fatal` report and stop before retrying normal work, while accepted
locks, mandatory work, terminal cleanup, and graceful shutdown retain their
existing ownership semantics.

Keep poison observation off uncontended fast paths. Logical-lock acquisition
registers a poison listener only after it actually enters `Waiting`, and row
mutation registers one only after it receives a registered
`PoisonAwareListener`. Reuse the existing exact pending-claim cleanup and
`FreshClaimsGuard` unwind behavior; do not add poison-specific rollback state.

[Shutdown and Engine Poison](../shutdown-and-poison.md) is the canonical
engine-wide behavior and review contract. This task records the implementation
delta for the two missing foreground wait integrations, their typed-error
propagation, and their validation. Subsystem documents should keep only their
local state-machine details and refer to the canonical contract for shared
poison, shutdown, acceptance, and wait policy.

## Context

The engine-wide distinction between lifecycle, health, and work ownership;
first-Fatal publication; listener registration and sticky rechecks; graceful
shutdown; and accepted/terminal work is specified in
[Shutdown and Engine Poison](../shutdown-and-poison.md). The gaps addressed here
are both reversible foreground waits whose normal progress producer may stop
after engine poison.

Logical-lock arbitration currently waits only on a success-only
`Completion<()>`. `PendingClaimGuard` owns the exact pending token while a
fresh claim is queued, promoted but unobserved, locally published, or freshly
granted, and its `Drop` already cancels every incomplete state. Nevertheless,
the waiter has no poison wake source, so it can remain queued after poison and
later resume ordinary work. `FreshClaimsGuard` already rolls back only the
fresh prefix acquired by an unfinished multi-resource attempt; claims that
predated the attempt are deliberately not recorded or released.

Row write conflict handling waits while a foreign transaction is preparing.
`SharedTrxStatus::prepare_listener` losslessly distinguishes not-preparing,
registered-listener, and completion-won-registration outcomes. The cold path
checks engine health only after prepare completion, while the hot path retries
without checking it. Neither registered wait races an unrelated engine poison,
so a retained failed-precommit owner or another fatal producer can leave the
wait blocked or let the hot retry mask the fatal cause as `WriteConflict`.

This task is a standalone follow-up to the implemented logical-lock and typed
error work. It does not reopen RFC 0027 or add a new RFC phase.

Source Backlogs:

- `docs/backlogs/000177-propagate-engine-poison-through-hot-row-prepare-waiting.md`
- `docs/backlogs/000179-cancel-pending-logical-lock-acquisition-on-engine-poison.md`

Relevant completed work:

- `docs/tasks/000253-waiter-injected-hot-cold-prepare-waiting.md`
- `docs/tasks/000259-tokenized-waiter-provisional-grant-lifecycle.md`
- `docs/tasks/000263-introduce-quad-error-and-narrow-audited-error-convergence.md`
- `docs/rfcs/0027-session-family-logical-lock-system-redesign.md`

## Goals

1. Wake an actually queued logical-lock acquisition on engine poison and return
   the exact first stored `Fatal` report.
2. Cancel the interrupted claim through the existing token-exact
   `PendingClaimGuard` lifecycle, including queued and promoted-but-unobserved
   state, before control returns to its owner.
3. Let the existing `FreshClaimsGuard` ordinary error unwind release only the
   fresh prefix of an unfinished multi-resource acquisition, without adding
   poison-specific guard state or releasing pre-existing claims.
4. Race registered hot- and cold-row prepare waits with engine poison and check
   sticky health before any retry.
5. Preserve `Fatal` as a typed domain through shared row mutation, catalog
   mutation, transaction staging, and logical-lock owner call chains.
6. Add no poison listener, event allocation, select, or additional health load
   to uncontended hot-row mutation or non-waiting logical-lock acquisition.
7. Verify the production wait inventory against the canonical poison/shutdown
   classification and make subsystem documents refer to it instead of
   duplicating engine-wide policy.

## Non-Goals

- Do not make clean shutdown cancel logical-lock or row-prepare waits. Blocking
  shutdown remains graceful and may wait for active owners to finish or unwind.
- Do not preempt accepted mandatory execution, commit/rollback cleanup, or
  other terminal work after poison.
- Do not revoke pre-existing transaction, operation, or session locks, and do
  not globally drain the lock manager.
- Do not add poison-specific behavior or new rollback state to
  `FreshClaimsGuard`; its existing RAII error semantics are sufficient.
- Do not add unconditional poison polling to uncontended row writes, covered
  claims, immediate logical-lock grants, or existing exact claims.
- Do not replace immediate row `WriteConflict` with row-lock waiting.
- Do not add timeouts, leases, deadlock detection, victim selection, client
  cancellation, or a generalized cancellation-context abstraction.
- Do not change isolation, recovery, persisted formats, or public storage API
  error domains.
- Do not change success-only logical-lock completion into an error-bearing
  secondary release protocol.

## Plan

### 1. Apply the canonical wait contract

Implement the two wait integrations according to the
[semantic wait protocol](../shutdown-and-poison.md#the-semantic-wait-protocol)
and its concrete
[logical-lock](../shutdown-and-poison.md#logical-lock-acquisition) and
[row-prepare](../shutdown-and-poison.md#hot--and-cold-row-prepare-waiting)
contracts. The canonical document owns the engine-wide production wait
classification, poison/shutdown race precedence, future-wait review checklist,
and explicit non-guarantees.

Keep supporting documents local and concise:

- `docs/engine-component-lifetime.md` should refer to the canonical lifecycle,
  teardown, and wait-classification contract;
- `docs/lock-system.md` should describe the local pending-claim state machine
  and refer to the canonical cancellation and acceptance policy;
- `docs/transaction-system.md` should describe the local prepare-wait retry
  integration and refer to the canonical poison race; and
- `docs/process/coding-guidance.md` should require the canonical five-property
  review for every new potentially unbounded engine wait.

If implementation discovery changes an engine-wide rule or reveals a missing
wait family, update `docs/shutdown-and-poison.md`; do not copy the shared rule
into each subsystem document.

### 2. Cancel only actually waiting logical-lock acquisition

Keep `LockManager` independent of `EnginePoisoner`. Change
`FamilyLockState::acquire` and `TransactionLockState::acquire` to accept
`&EnginePoisoner`, store it in `PendingClaimGuard`, and widen their result to
the existing `OperationOrFatalResult<LockGrant>` carrier. Let
`FreshClaimsGuard` also hold the borrowed poisoner supplied at construction and
forward it from each acquisition. This keeps each guard's acquisition method
self-contained without adding poison-specific rollback state. Propagate those
signatures through session, transaction admission, statement lock acquisition,
DDL, maintenance, and explicit-lock preparation.

Preserve the current covered, existing, conversion, and immediate-grant paths
without poison listener registration or an extra health check. Only the
`PendingStart::Waiting` branch installs the poison listener and performs the
canonical register/recheck/race/final-check sequence. Retain the exact pending
token until the existing acceptance point, with no `.await` between the final
healthy check and `PendingClaimGuard::accept`, so ordinary guard drop cancels
queued, provisional, or partially published state on every Fatal return.

Do not add a poison branch to `FreshClaimsGuard::Drop`. Its stored poisoner is
only forwarded by `acquire`; ordinary `?` propagation already drops the armed
guard on either `Operation` or `Fatal`. It continues to record and release only
`LockGrant::Fresh` tokens acquired earlier in that same attempt. Pre-existing
claims and successful attempts after `disarm()` are unchanged. Call chains
that do not currently use `FreshClaimsGuard` do not gain one solely for poison.

Add deterministic test-only pause hooks around listener registration,
completion selection, and the staged transfer before provisional observation,
plus the post-health acceptance boundary. Do not use timing sleeps to establish
race coverage.

### 3. Share a lazy prepare-or-poison wait

Add `TrxRuntime::wait_prepare_or_poison` as the single prepare retry helper and
use it from both hot and cold row paths. `SharedTrxStatus::prepare_listener`
wraps both registered and completion-won-registration outcomes in an opaque,
move-only `PoisonAwareListener`; its raw listener and state remain private and
it cannot be directly awaited. `LockUndo::Preparing` and the cold-row carriers
hold that token rather than `Option<EventListener>`.

`TrxRuntime::wait_prepare_or_poison` is invoked only from a preparing result and
delegates token consumption to `EnginePoisoner::wait_or_poison`. A recheck-only
token performs the completion-won-registration sticky health check. A
registered token performs the prepare-or-poison race with health checks before
and after selection. Both states must be consumed before retry, preserving
Fatal from failed precommit or unrelated poison while successful commit and
rollback still trigger authoritative row-state retry.

Keep the current hot-row page pinning decision: the shared page guard remains
held during the short prepare wait, while row access is dropped. A poison error
unwinds and drops that guard normally.

`LockUndo::Ok`, `LockUndo::InvalidIndex`, `LockUndo::WriteConflict`, and the
row-page transition return must not call the helper. Consequently, an
uncontended hot write performs no added poison load, listener allocation, or
future selection. Add a test-only slow-branch hook so a normal hot mutation and
a non-waiting logical-lock grant can assert they never enter poison-listener
setup.

### 4. Preserve the Fatal domain through internal callers

Reuse the narrow carriers added by task 000263; add no new error enum.

- Change `HotRowMutator::lock_for_write` to return
  `FatalResult<LockRowForWrite>` and its delete/update operations to return
  `OperationOrFatalResult`.
- Widen only affected `MemTable` and `UserTableAccessor` mutation paths to
  existing `QuadResult` where `Operation`, `Runtime`, and `Fatal` can meet.
- Widen logical-lock acquisition helpers from `OperationResult` to
  `OperationOrFatalResult`, then preserve that domain until an existing public
  disclosure boundary.
- Change catalog mutation invariant narrowing so impossible `Operation`
  failures still panic, while `Runtime` and `Fatal` leave as
  `RuntimeOrFatalResult`. Treat an impossible internal `Lifecycle` branch as an
  invariant violation rather than wrapping it as Runtime.
- Widen `PrivateTransaction::stage_statement` and affected catalog DDL staging
  methods to `RuntimeOrFatalResult` so a prepare-wait fatal remains source
  bearing.
- Remove or revise any catalog health helper that changes `Fatal` into
  `RuntimeError::CatalogAccess`. Context attachments may be added, but the
  underlying Fatal context and the first poison report must be preserved.

Use compiler-guided propagation through catalog table/index adapters, but do
not widen unaffected read or insert paths. Refresh
`docs/public-error-audit.csv` with `tools/error_audit.rs --write` if disclosure
sites change.

### 5. Complete the production waiter audit

Inventory production waits built from `EventListener`, `Completion`, explicit
progress events, logical-lock acquisition, prepare listeners, and subsystem
gates. Verify every indefinite engine-owned wait against the
[production wait classification](../shutdown-and-poison.md#production-wait-classification)
rather than adding poison indiscriminately. Correct stale entries in the
canonical table. Any newly discovered reversible gap must either be repaired
within these same mechanisms or recorded as an explicit follow-up if it would
expand the task beyond the RFC gate.

## Implementation Notes

`PendingClaimGuard` now installs a poison listener only after the lock manager
returns `PendingStart::Waiting`. It performs sticky health checks after
registration, after completion selection, and after provisional observation
immediately before consuming the pending token. Existing guard drop handles
queued, provisional, and partially published cancellation. Immediate,
existing, converted, and family-covered claims retain their previous fast
paths. `FreshClaimsGuard` merely forwards the poisoner and keeps its ordinary
fresh-prefix rollback policy.

`SessionOperationPin` splits its retained `EngineCore` borrow from mutable
operation or session lock state. DDL, maintenance, explicit-lock acquisition,
and explicit unlock therefore borrow the lock manager, poisoner, and catalog
directly without cloning per-attempt `QuiescentGuard` handles.

`TrxRuntime::wait_prepare_or_poison` is the shared hot/cold prepare retry
boundary. `PoisonAwareListener` makes bypassing that boundary structurally
difficult: it exposes neither its raw primary listener nor a `Future`
implementation, and only `EnginePoisoner::wait_or_poison` consumes it in
production. Registered tokens race the engine poison event; recheck-only tokens
perform the completion-won-registration sticky check before retry. The hot path
still pins the page while releasing row access; cold paths retain their
existing guard-release and authoritative-retry behavior. Test-only observation
counters verify that ordinary hot mutation and non-waiting logical locks never
enter the new slow path.

Fatal propagation now remains typed through lock owners, hot-row mutation,
affected `MemTable` seams, `QuadResult` user/catalog mutation integration,
private statement staging, and catalog DDL adapters. Catalog invariant
narrowing still panics for impossible Operation or Lifecycle arms and returns
Runtime or Fatal unchanged. The generated public-error audit did not change.

Deterministic lock test hooks cover listener registration, completion
selection, poison after local publication but before provisional observation,
and poison immediately after the final healthy acceptance check. The staged
transfer case continues through provisional adoption and relies on the final
sticky health check before Fatal unwind. Tests also cover exact waiter cleanup,
accepted claim non-revocation, fresh-prefix rollback, explicit-lock
propagation, hot/cold prepare wakeup, completion-won-registration,
first-source retention, and fast-path exclusion. The focused race set passed
100 stress iterations.

The production waiter audit found no additional reversible foreground gap.
The canonical classification in
[Shutdown and Engine Poison](../shutdown-and-poison.md) covers logical locks,
prepare and route waits, maintenance progress, accepted service completions,
table/catalog gates, terminal cleanup, buffer progress, lifecycle drain,
worker waits, and policy-neutral primitives. Engine lifetime, lock,
transaction, and coding-guidance documents now link to that contract rather
than restating engine-wide policy.

Final validation passed:

- `rtk cargo fmt --all -- --check`;
- strict workspace Clippy and the 20-file branch style audit;
- alternate `libaio` Clippy with warnings denied;
- 1,718 workspace nextest tests;
- 1,608 alternate-backend nextest tests; and
- the unchanged generated public-error audit.

## Impacts

- `doradb-storage/src/poison.rs`
  - Add the opaque two-state `PoisonAwareListener` and make
    `EnginePoisoner::wait_or_poison` its only production consumer; retain the
    existing sticky error and one-shot poison event.
- `doradb-storage/src/lock/wait.rs`
  - Race poison only in `PendingGuardState::Waiting`; retain the pending token
    through the final health check and use existing `Drop` cleanup.
- `doradb-storage/src/lock/state.rs`
  - Propagate `OperationOrFatalResult` through family/transaction acquisition
    and `FreshClaimsGuard` without changing the guard's rollback policy.
- `doradb-storage/src/lock/mod.rs`
  - Add deterministic race and debug-snapshot assertions for queued,
    provisional, and later-promotion behavior.
- `doradb-storage/src/session.rs`
  - Propagate Fatal through DDL, maintenance, and explicit session-lock
    acquisition while preserving scope ownership.
- `doradb-storage/src/trx/admission.rs`, `doradb-storage/src/trx/stmt.rs`, and
  `doradb-storage/src/trx/mod.rs`
  - Propagate logical-lock Fatal, centralize prepare-or-poison waiting, and
    widen private statement/catalog invariant boundaries.
- `doradb-storage/src/table/hot.rs`, `doradb-storage/src/table/access.rs`, and
  `doradb-storage/src/table/mem_table.rs`
  - Use the shared lazy prepare wait and preserve Fatal through affected hot,
    cold, user-table, and catalog mutations.
- `doradb-storage/src/catalog/storage/` plus affected catalog table/index
  adapters
  - Preserve Runtime versus Fatal through private DDL mutation staging.
- `docs/shutdown-and-poison.md`
  - Keep the canonical engine-wide poison, shutdown, wait classification, and
    future-review contract synchronized with the implementation.
- `docs/engine-component-lifetime.md`, `docs/lock-system.md`,
  `docs/transaction-system.md`, and `docs/process/coding-guidance.md`
  - Retain subsystem-specific details and link to the canonical contract for
    shared behavior and review policy.
- `docs/public-error-audit.csv`
  - Regenerate only if changed disclosure signatures affect the audit.
- Source backlogs 000177 and 000179 remain open during implementation and are
  closed by `$task-resolve` only after behavior and validation are complete.

Primary risks are a poison lost wake, accepting a provisional grant after
poison, double cleanup between manager and owner indexes, accidental release of
pre-existing claims, wrapping Fatal as Runtime, and introducing listener or
atomic-load overhead on non-waiting paths. The register/recheck protocol,
move-only token, existing RAII guards, typed carriers, deterministic race hooks,
and explicit fast-path tests mitigate these risks.

## Test Cases

### Logical locks

1. A request that has entered `Waiting` observes an already-published poison
   after listener registration and returns the first Fatal without waiting for
   its blocker.
2. Poison during queued DDL, DML, maintenance, and explicit session-lock
   acquisition returns Fatal and leaves no queued node or owner-side claim.
3. Deterministically race poison with completion publication, provisional
   promotion, owner-index publication, observation, and pre-accept checking.
   Every poison-winning schedule removes the exact pending state.
4. If the final health check wins before a later poison, acquisition succeeds
   and the accepted claim is not retroactively revoked.
5. Releasing the original blocker after cancellation cannot resurrect the
   canceled waiter; the next compatible FIFO waiter is promoted normally.
6. A failed multi-resource attempt releases its existing fresh prefix in
   reverse order while preserving claims held before the attempt.
7. Single-claim and call chains without `FreshClaimsGuard` retain their current
   owner-lifetime cleanup policy.
8. Debug snapshots and statistics show no occupied waiter node, provisional
   grant, live resource, or duplicated cleanup after cancellation.
9. Returning Fatal lets existing owner unwind remove the active operation as a
   shutdown blocker; clean shutdown without poison still follows the existing
   graceful drain behavior.
10. Existing, covered, converted, and immediately granted lock paths do not
    enter poison-listener setup or add a sticky health read.

### Prepare waiting and error propagation

1. A registered hot-row prepare waiter returns Fatal promptly when generic
   engine poison occurs before the owner completes.
2. The equivalent cold-row waiter has the same result.
3. A completion-won-registration recheck-only token checks health before retry
   and returns the stored failed-precommit Fatal.
4. Poison racing registered prepare completion wins whenever it is published
   before the final healthy check.
5. Successful commit and successful rollback still wake and retry hot and cold
   row state correctly.
6. An ordinary active-row conflict remains immediate `WriteConflict`.
7. Fatal propagates through user-table and catalog-backed `MemTable` mutation,
   private statement staging, and DDL callers without a public Error or Runtime
   round trip.
8. The returned report retains `ErrorKind::Fatal`, the original fatal context,
   source chain, and first-poison attachments.
9. An uncontended hot delete/update does not invoke the prepare-or-poison helper,
   register a poison listener, perform an additional poison load, or select a
   second future.
10. Existing hot-page/access guards release normally on poison, with no unsafe
    retry against retained failed-precommit undo.

### Audit and validation

1. Review every production indefinite engine-owned wait found by the audit and
   ensure it has one documented poison/shutdown category.
2. Run focused new race tests with `--stress-count 100` under cargo-nextest.
3. Run `rtk cargo fmt --all -- --check`.
4. Run strict workspace Clippy according to `docs/process/lint.md`.
5. Run `rtk cargo nextest run --workspace`.
6. Run the alternate backend validation:
   `rtk cargo nextest run -p doradb-storage --no-default-features --features libaio`.
7. Run the repository style audit for changed Rust files and refresh/verify the
   public error audit according to their process documentation.

## Open Questions

None. A future unified cancellation context covering poison, shutdown,
deadlines, client cancellation, and deadlock victims would be RFC-scale and is
deliberately outside this task.
