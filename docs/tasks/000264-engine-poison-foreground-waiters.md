---
id: 000264
title: Integrate Engine Poison with Foreground Waiters
status: implemented  # proposal | implemented | superseded
created: 2026-08-08
github_issue: 962
---

# Task: Integrate Engine Poison with Foreground Waiters

## Summary

Integrated engine poison with the two reversible foreground waits that could
otherwise outlive their progress producers: queued logical-lock acquisition
and hot/cold row prepare waiting. Both now wake on poison, return the first
stored typed `Fatal` report, and stop before retrying normal work.

The implementation keeps poison observation off uncontended paths. Immediate,
covered, existing, and converted lock grants retain their prior cost, as do
ordinary hot-row mutations. Accepted locks, mandatory work, terminal cleanup,
and graceful shutdown retain their existing ownership semantics.

[Shutdown and Engine Poison](../shutdown-and-poison.md) is the canonical
engine-wide behavior and review contract. This task is the historical record
for the two foreground integrations, their typed-error propagation, and the
waiter audit completed with them.

## Context

Logical-lock arbitration previously waited only on a success-only
`Completion<()>`. `PendingClaimGuard` could exactly cancel queued,
provisional, locally published, or freshly granted state, but the wait had no
engine-poison wake source. A request could therefore remain queued after a
fatal runtime failure or later resume ordinary work.

Row mutation already waited when a foreign owner entered ordered prepare.
`SharedTrxStatus::prepare_listener` distinguished not-preparing, registered,
and completion-won-registration outcomes, but hot waits did not check poison
and registered hot/cold waits did not race unrelated poison. Failed-precommit
cleanup could retain unsafe ownership while the waiter masked the fatal source
as `WriteConflict` or remained blocked.

The work reused the exact pending-claim and fresh-prefix RAII cleanup delivered
by earlier lock tasks. It is a standalone follow-up and does not reopen RFC
0027 or create another RFC phase.

Source Backlogs:

- `docs/backlogs/closed/000177-propagate-engine-poison-through-hot-row-prepare-waiting.md`
- `docs/backlogs/closed/000179-cancel-pending-logical-lock-acquisition-on-engine-poison.md`

Relevant completed work:

- `docs/tasks/000253-waiter-injected-hot-cold-prepare-waiting.md`
- `docs/tasks/000259-tokenized-waiter-provisional-grant-lifecycle.md`
- `docs/tasks/000263-introduce-quad-error-and-narrow-audited-error-convergence.md`
- RFC 0027, Session-Family Logical-Lock System Redesign.

## Goals

1. Wake actually queued logical-lock acquisition on engine poison and return
   the exact first Fatal report.
2. Cancel only the interrupted pending token and the fresh prefix of the same
   unfinished multi-resource attempt.
3. Race registered hot/cold prepare waits with poison and check sticky health
   before every authoritative retry.
4. Preserve Fatal as a typed domain through lock owners, row mutation,
   transaction staging, and catalog DDL integration.
5. Keep poison listeners, health loads, and future selection off immediate
   lock grants and uncontended row writes.
6. Define the production wait inventory and future-review contract in one
   canonical engine document.

## Non-Goals

- Clean shutdown does not cancel logical-lock or row-prepare waits; active
  owners still participate in graceful drain.
- Accepted mandatory execution, commit/rollback cleanup, and terminal work are
  not preempted after poison.
- Existing claims are not revoked and the lock manager is not globally
  drained.
- Ordinary active-row conflicts remain immediate `WriteConflict`; this task
  does not add general row-lock waiting.
- No timeout, lease, deadlock-victim, client-cancellation, or generalized
  cancellation-context policy was added.
- Isolation, recovery, persisted formats, and public storage error domains are
  unchanged.
- Success-only lock completion remains an internal release signal rather than
  an error-bearing secondary protocol.

## Plan

### Canonical wait contract

Every potentially unbounded wait was classified by progress producer,
cancellation authority, poison behavior, shutdown behavior, and retry
boundary. The resulting contract lives in
[Shutdown and Engine Poison](../shutdown-and-poison.md); subsystem documents
retain only local state-machine details and link to it.

The poison-aware protocol registers the poison listener before rechecking
sticky health, races semantic progress against poison, and checks health again
before retry or acceptance. Event primitives remain policy-neutral.

### Logical-lock acquisition

`FamilyLockState`, `TransactionLockState`, and their owner call chains carry
`OperationOrFatalResult`. `PendingClaimGuard` holds the borrowed
`EnginePoisoner`, but registers a poison listener only after
`PendingStart::Waiting`.

The pending token remains owned through manager completion, owner-index
publication, provisional observation, and the final health check. There is no
await between that check and acceptance. Any Fatal return therefore drops the
same guard and removes the exact queued, provisional, or partially published
state.

`FreshClaimsGuard` holds the poisoner for its acquisitions while retaining its
ordinary rollback contract: only `LockGrant::Fresh` tokens from the current
attempt are released, in reverse order. Pre-existing claims and disarmed
successful attempts remain owned by their enclosing scope.

The healthy acceptance boundary is deliberate. Poison published after the
final check does not retroactively revoke an accepted immediate result.

### Row prepare waiting

`SharedTrxStatus::prepare_listener` returns either not-preparing or one opaque
`PoisonAwareListener`. The move-only token internally represents a registered
primary listener or a recheck-only completion race. Its raw state is private,
and it implements neither `Future` nor `Clone`.

`EnginePoisoner::wait_or_poison` is the only production token consumer.
Registered tokens install the poison side, check health, race both events, and
check health again. Recheck-only tokens perform the mandatory sticky check
without registering or selecting another future. `TrxRuntime` exposes the
shared prepare retry boundary used by hot and cold mutation paths.

Successful commit and rollback still cause a full authoritative retry. Fatal
failed-precommit cleanup publishes poison before releasing prepare waiters, so
the retry boundary returns Fatal before touching retained state.

Hot waits release row access but retain the shared page pin. Cold point and
scan paths release operation-local deletion, index, location, and decoded
block state before waiting, then restart from authoritative state.

### Typed error propagation

Existing narrow carriers were extended rather than introducing a public or
catch-all error type. Logical-lock and hot-row seams use
`OperationOrFatalResult`; mixed table/catalog mutation seams use existing
multi-domain carriers; private statement and catalog DDL staging use
`RuntimeOrFatalResult`.

Impossible internal Operation or Lifecycle arms remain invariant failures.
Runtime and Fatal reports cross catalog adapters unchanged, with caller-owned
attachments added without converting Fatal to Runtime.

Catalog cleanup uses Fatal-first precedence. An original Fatal source wins;
otherwise a Fatal cleanup outranks Runtime; equal-domain ties retain the
original operation source. Runtime destruction, rollback, and provisional-file
cleanup failures remain attached as typed diagnostic evidence.

### Wait inventory

The production audit found no other reversible foreground gap. The canonical
classification covers logical locks, row prepare and route waits, maintenance
progress, accepted service completions, table/catalog gates, terminal cleanup,
buffer progress, lifecycle drain, worker waits, and policy-neutral event
primitives.

## Implementation Notes

Task 000264 shipped both missing foreground poison integrations, preserved the
existing acceptance and ownership boundaries, and added no public API or
persisted-format change.

Deterministic lock hooks cover poison after listener registration, after
completion selection, before provisional observation, and immediately after
the final healthy acceptance check. The observation hook was consolidated at
the semantic boundary between local publication and manager observation.

Review replaced the optional raw prepare listener with the opaque two-state
`PoisonAwareListener`. This makes direct production waiting impossible while
preserving the completion-won-registration health check. Unit tests verify
that recheck-only consumption does not register a poison listener and that a
registered token performs both health checks.

`PendingClaimGuard` and `FreshClaimsGuard` retain their poisoner references,
making their acquisition methods self-contained. Detailed guard comments now
record exact cancellation, fresh-prefix rollback, disarm, and drop behavior.

Session DDL, maintenance, explicit-lock, and unlock paths split a retained
`EngineCore` borrow from mutable lock state. They borrow the lock manager,
poisoner, and catalog directly without per-attempt `QuiescentGuard` clones.

Typed Fatal review found cleanup precedence bugs in CREATE/DROP INDEX staging
and CREATE TABLE abort. `RuntimeOrFatalError::merge_cleanup` now preserves the
most important typed source and attaches the non-selected report with its full
diagnostic chain. The generated public-error audit remained unchanged.

The focused logical-lock race set passed 100 stress iterations. A later
prepare-focused combined stress run had one allocator `SIGABRT` with no Rust
assertion; the affected test passed 100 isolated iterations and the full set
then passed 100 combined iterations. No protocol failure was reproduced.

Final verification passed formatting, strict workspace Clippy, the 21-file
branch style audit, 1,719 workspace nextest tests, alternate `libaio` Clippy,
1,609 alternate-backend tests, and the unchanged public-error audit.

## Impacts

- Engine poison now owns the opaque prepare-wait token and its poison-aware
  consumption protocol.
- Logical-lock acquisition can return typed Fatal while preserving exact
  pending-state and fresh-prefix cleanup.
- Hot/cold row mutation shares one authoritative prepare retry boundary.
- Catalog and transaction mutation call chains preserve Runtime versus Fatal,
  including cleanup-failure attachments.
- Engine lifetime, lock, transaction, and coding-guidance documents refer to
  the canonical poison/shutdown contract.
- Uncontended lock and row-write fast paths add no poison listener or health
  observation.
- Public APIs, schemas, persisted formats, isolation, and recovery behavior are
  unchanged.

## Test Cases

- Already-published and racing poison wake queued lock requests, return the
  first Fatal report, and remove exact queued/provisional/owner-side state.
- Completion, promotion, local publication, observation, and acceptance races
  preserve the documented linearization boundary and do not resurrect canceled
  waiters.
- Multi-resource failure releases only the current attempt's fresh prefix and
  preserves pre-existing claims.
- DDL, DML, maintenance, transaction, and explicit session-lock callers retain
  typed Fatal through their native carriers.
- Registered and completion-won prepare waits return Fatal for hot and cold
  rows, while healthy commit/rollback still wake and retry correctly.
- Ordinary active conflicts and uncontended hot writes avoid the prepare wait
  helper and poison-listener setup.
- Cleanup precedence covers Fatal/Runtime, Runtime/Fatal, Fatal/Fatal, and
  Runtime/Runtime combinations while retaining secondary diagnostics.
- The production waiter inventory assigns every indefinite engine-owned wait a
  documented poison and shutdown category.

## Open Questions

None.
