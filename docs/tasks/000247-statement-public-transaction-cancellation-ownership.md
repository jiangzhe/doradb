---
id: 000247
title: Statement and Public Transaction Cancellation Ownership
status: proposal  # proposal | implemented | superseded
created: 2026-07-30
github_issue: 917
---

# Task: Statement and Public Transaction Cancellation Ownership

## Summary

Implement RFC-0025 Phase 2 by making a polled
`Transaction::exec` future safe to drop at every statement await point. Once
the future has checked out the transaction core, cancellation is terminal for
the public transaction: synchronously settle any pending acquisition, move
residual statement row/index undo into transaction undo, discard statement
redo, release statement locks, and check the complete transaction core into
its stable session-operation entry as `CleanupReady`. Queue only the existing
identity-based whole-transaction cleanup job; the worker must never receive a
statement payload or select a statement rollback phase.

Introduce a crate-private, lifetime-free `StmtState` stack carrier that owns
the existing `SessionOperationCheckout`, `StmtEffects`, and statement
`OwnerLockState`. Preserve the public `Statement<'_>` API and its current
direct `&mut TrxInner` access: `StmtState` splits disjoint borrows of its
checked-out core, effects, and locks into one temporary `Statement` facade for
the callback. Normal completion consumes the carrier only after statement
effects have been merged or rolled back and the checkout has been returned.
Unexpected public Drop consumes the same carrier through the terminal
cancellation path.

Do not move the checkout into the public `Transaction` for its full lifetime.
Per-operation checkout/check-in is the implemented Phase 1 architecture: it
lets the engine-owned entry retain a claimable core between calls while the
public transaction remains a weak capability. Phase 2 adds no registry
resolution, allocation, `Arc` operation, mutex acquisition, atomic operation,
notification, or queue send to successful `Transaction::exec` beyond that
existing checkout/check-in. Use the existing `stmt-noop`, `trx-noop`, and
bounded `index-stream` workloads for paired optimized measurements.

Retain one eagerly allocated, ready-to-initialize `Box<TrxInner>` in each
session lifecycle. Transaction begin takes that exact box and initializes its
zero-valued status and identity fields; successful terminal processing resets
the core, installs a fresh zero-valued `SharedTrxStatus`, and returns it to the
session. Status identity is never reused, and fatal terminal paths do not
recycle their failed core.

## Context

Parent RFC:

- `docs/rfcs/0025-session-coordinated-cancellation-cleanup-ownership.md`

RFC Relationship:

- Phase 2: Statement And Public Transaction Cancellation Ownership.

Source Backlogs:

- `docs/backlogs/000124-statement-execution-cancellation-safety.md`

Prerequisites:

- `docs/tasks/000246-session-operation-coordinator-foundation.md`
- `docs/tasks/000174-transaction-terminal-rollback-cancellation-safety.md`
- `docs/tasks/000242-enforce-terminal-transaction-lock-release-ordering.md`
- `docs/tasks/000244-add-rfc-0025-benchmark-workloads.md`

Issue Labels:

- type:task
- priority:high
- codex

RFC-0025 Phase 1 replaced the transaction-only lifecycle with a stable
`SessionOperationEntry`. A public `Transaction` is now a weak facade containing
the exact `SessionOperationKey` and `TrxID`; the entry owns `TrxInner` while it
is `ForegroundAvailable`. Each non-terminal transaction call resolves the
entry, moves `TrxInner` into `SessionOperationCheckout`, changes the entry to
`ForegroundRunning(None)`, and ordinarily returns the core to
`ForegroundAvailable` when the checkout drops. `ForegroundAvailable` means
that this still-active transaction's core can be leased by its next call; it
does not mean that the transaction or session operation has ended.

This per-operation ownership boundary is deliberate. Keeping a
`SessionOperationCheckout` in `Transaction` until commit or rollback would
leave the core outside the stable entry between calls and retain its strong
`TrxAttachment` for the transaction lifetime. Shutdown and session cleanup
could no longer claim an otherwise idle transaction core without waiting for
the user-held handle, while the strong attachment could pin the runtime
indefinitely. Replacing that behavior would reopen Phase 1 rather than resolve
Phase 2. The current checkout does have measurable fixed cost—weak engine
upgrade, lifecycle admission, exact operation resolution, entry checkout, and
entry check-in—so this task must measure the absolute `stmt-noop` baseline as
well as its own delta.

`Transaction::exec` currently creates a local checkout and then a
`Statement<'_>`. The `Statement` directly borrows `&mut TrxInner` and
`&TrxAttachment`, but directly owns `StmtEffects` and the statement
`OwnerLockState`. Its Drop releases statement locks. Successful callbacks move
all effects into `TrxEffects`; ordinary callback errors asynchronously roll
back index effects before row effects and then clear redo.

The current implementation already contains an important prerequisite from
RFC-0025: row and index rollback borrow the last vector entry across every
await and pop it only after successful rollback. Cancelling either rollback
future therefore leaves the current entry and all older entries in the
statement buffer. Existing focused tests prove that local buffer property, but
they deliberately keep the containing `StmtEffects` alive.

The remaining outer-future gap is different. If the enclosing
`Transaction::exec` future is dropped:

1. `Statement` releases its statement locks.
2. `StmtEffects::Drop` asserts when any row undo, index undo, or redo remains.
3. If no statement effects remain, `SessionOperationCheckout::Drop` performs
   an ordinary check-in and returns the transaction to
   `ForegroundAvailable`.

The first behavior is fail-fast rather than cleanup ownership; catching the
panic can destroy memory still reachable from MVCC undo links. The second
behavior violates RFC-0025's contract that every post-checkout public statement
cancellation is terminal, including cancellation before the first mutation.
Neither path publishes cancellation intent to the stable entry.

Task 000174 already makes explicit terminal rollback worker-owned after a
terminal claim, and task 000242 requires transaction locks to be released and
represented by `ReleasedTransactionLocks` before session rollback completion.
Phase 2 must reuse those mechanisms. The cancellation Drop path performs only
synchronous statement settlement and returns a complete transaction core.
The existing cleanup worker then claims that core and runs the ordinary
whole-transaction rollback.

Backlog 000124 describes exactly this unresolved public statement-future
ownership problem. Completing and verifying this task fully implements that
backlog's requested contract. The backlog remains open during implementation
and is closed as implemented only through `$task-resolve`, together with the
RFC Phase 2 synchronization.

The strict complexity gate remains satisfied because this task is the bounded,
accepted Phase 2 deliverable. It does not add another ownership program,
public API migration, durable format, worker, or recovery protocol. RFC-0025
Phase 3 continues to depend on this task's nested statement/transaction
settlement and otherwise retains its existing whole-operation-transfer
assumptions.

## Goals

1. Make dropping every polled, successfully checked-out public
   `Transaction::exec` future non-panicking and ownership-complete, regardless
   of whether the statement has produced effects.
2. Treat every such Drop as terminal whole-transaction cancellation; never
   return a cancelled transaction to reusable `ForegroundAvailable`.
3. Preserve dropping an unpolled `Transaction::exec` future as a no-op that
   performs no checkout and leaves the transaction reusable.
4. Add a lifetime-free `StmtState` that is the sole final owner of
   `SessionOperationCheckout`, `StmtEffects`, and statement
   `OwnerLockState`.
5. Preserve `Statement<'stmt>` and its direct `&'stmt mut TrxInner` and
   `&'stmt TrxAttachment` access. Do not add a checkout `Option` lookup or
   carrier indirection to each DML method.
6. Let `StmtState` lend disjoint mutable effects/lock borrows and the direct
   checked-out-core borrow to one callback-facing `Statement`.
7. Encode only the carrier's Drop policy—public cancellation, private
   must-complete, or settled—in a local `StmtDropAction`; do not introduce a
   statement rollback phase machine.
8. Ensure a nested callback or acquisition future is destroyed before
   `StmtState::Drop`, so queued waiters and promoted-but-unobserved logical-lock
   grants settle synchronously before statement payload transfer.
9. Append all residual statement row and index undo buffers after prior
   transaction undo while preserving each buffer's operation order.
10. Discard statement redo rather than merging it into transaction redo on
    cancellation.
11. Release all statement-owned logical locks synchronously after residual
    undo folding and before the transaction becomes cleanup-claimable.
12. Preserve transaction-owned table metadata/data locks and bindings until
    the worker executes whole-transaction rollback.
13. Audit first-touch write paths and prove that transaction-lifetime
    lock/binding coverage exists before a statement effect is created; safely
    release a statement-only pre-binding grant when no effect was created.
14. Add an infallible cancellation-specific checkout return that atomically
    checks the complete public transaction core into its stable entry as
    `CleanupReady`, coalescing any pre-existing abandonment/close/shutdown
    cleanup intent.
15. Queue only the existing `(SessionOperationKey, TrxID)` identity cleanup
    job. Keep duplicate or stale hints neutral and keep statement payloads out
    of `SessionOperationEntry` and `SessionOperationCleanupMessage`.
16. Make all subsequent operations through the cancelled public transaction
    facade fail deterministically with `TransactionDiscarded`.
17. Preserve accepted commit ownership: once completion is
    `CompletionOwned`, observer cancellation or duplicate cleanup hints cannot
    convert it to rollback.
18. Preserve ordinary callback-error semantics: complete statement-local
    rollback and return the transaction to reusable
    `ForegroundAvailable`.
19. Preserve fatal rollback retention and engine poisoning when statement
    rollback itself cannot complete safely.
20. Remove the generic `StmtEffects::Drop` behavior assertion and make the
    ownership-bearing carrier responsible for public cancellation settlement.
21. Remove `Statement::Drop`; `StmtState` must release statement locks in every
    normal, cancellation, private-invariant, and fatal exit.
22. Keep `Transaction::Drop` as its current non-panicking, idempotent
    abandonment handoff. It has no payload-clearing assertion and must not
    borrow or share a cancellation flag with `StmtState`.
23. Retain internal `TrxInner`, prepared-transaction, precommit-transaction,
    and terminal-proof assertions that validate complete ownership transfer;
    reliable cancellation does not make those internal backstops obsolete.
24. Preserve all public method signatures, `TrxID` MVCC/lock roles, statement
    numbering, successful `StreamStmt` ownership, and transaction checkout
    frequency.
25. Meet RFC-0025's structural and measured successful-path performance
    budgets.
26. Allocate the reusable public `TrxInner` box once per session and move only
    that concrete box through entry, checkout, prepared, precommit, and
    terminal owners. Private transactions allocate a fresh concrete box.
27. Make public transaction begin initialize the session's cached core without
    allocating either `TrxInner` or `SharedTrxStatus`.
28. Make successful public terminal completion reset the cached core with a
    fresh zero-valued status only after the prior status is terminal and no
    longer preparing. Drop a successfully terminal private core without reset.
29. Drop variable-capacity transaction state during reset and never recycle a
    fatal-retained core.

## Non-Goals

1. Do not move `SessionOperationCheckout`, `TrxInner`, or a strong
   `TrxAttachment` into `Transaction` for its full lifetime.
2. Do not redesign or remove the public `Transaction::exec`,
   `Statement<'_>`, `StreamStmt`, commit, or rollback APIs.
3. Do not make a cancelled statement reusable after background statement-only
   rollback. Cancellation always terminates the public transaction.
4. Do not add worker-owned statement undo, redo, locks, rollback phases, or
   statement-specific cleanup messages.
5. Do not store `StmtState`, `StmtEffects`, `StmtDropAction`, or a whole
   statement future in `SessionOperationEntry`.
6. Do not perform asynchronous rollback, blocking work, registry scans, or
   worker polling from `Drop`.
7. Do not add `DiscardOnly` classification. Even an effect-free cancelled
   public statement uses the existing whole-transaction rollback claim.
8. Do not change the MVCC row/index undo formats, redo format, transaction log,
   recovery behavior, group commit, CTS assignment, GC handoff, or purge.
9. Do not redesign logical-lock resources, waiter queues, grants, lock-family
   identities, or transaction lock-release proofs.
10. Do not implement Phase 3's pinned DDL/maintenance future driver,
    background handoff, worker-local concurrent executor, supervision, or
    stop/drain behavior.
11. Do not claim that private catalog-statement or whole-DDL cancellation is
    safe before Phase 3. Preserve a narrow private must-complete invariant
    rather than applying public transaction cancellation policy to it.
12. Do not migrate DDL, maintenance, checkpoint, or retention progress and do
    not change any irreversible-gate policy.
13. Do not add coordinator work per row, index entry, lock probe, stream item,
    or rollback entry.
14. Do not add a benchmark runner, production instrumentation, CI wall-clock
    threshold, or persistent benchmark comparison format.
15. Do not close backlog 000124 or mark RFC-0025 Phase 2 implemented during
    coding; `$task-resolve` owns those documentation transitions.
16. Do not reuse or reinitialize a `SharedTrxStatus` identity that was exposed
    by an active transaction.
17. Do not apply the session core buffer to sessionless system transactions.

## Plan

### 1. Introduce one owned statement carrier and preserve direct core borrowing

In `doradb-storage/src/trx/stmt.rs`, introduce crate-private structures
equivalent to:

```rust
enum StmtDropAction {
    CancelPublicTransaction,
    PrivateMustComplete,
    Settled,
}

pub(crate) struct StmtState {
    effects: StmtEffects,
    stmt_locks: OwnerLockState,
    drop_action: StmtDropAction,
    checkout: Option<SessionOperationCheckout>,
}

pub struct Statement<'stmt> {
    inner: &'stmt mut TrxInner,
    attachment: &'stmt TrxAttachment,
    effects: &'stmt mut StmtEffects,
    stmt_locks: &'stmt mut OwnerLockState,
    disable_dml_validation: bool,
}
```

The exact private names may follow surrounding style, but the ownership shape
is fixed:

```text
StmtState
  owns SessionOperationCheckout
    owns TrxInner
  owns StmtEffects
  owns statement OwnerLockState

Statement<'_>
  directly borrows &mut TrxInner from the checkout
  borrows &TrxAttachment from the checkout
  borrows &mut StmtEffects from StmtState
  borrows &mut OwnerLockState from StmtState
```

`StmtState` and `StmtDropAction` have no lifetime parameters and do not borrow
`Transaction::terminal_started`. Keep `checkout` in an `Option` so terminal
cancellation and fatal retention can consume it safely from a type with a Drop
implementation. Keep it last in the representation so automatically dropped
statement payload cannot outlive an ordinary checkout return if a future
layout changes.

Add public and private constructors. The public constructor starts armed with
`CancelPublicTransaction`; the catalog constructor starts with
`PrivateMustComplete`. Both derive the statement lock owner from the
checked-out `TrxInner` exactly as today and allocate no heap state.

Add a `statement(&mut self) -> Statement<'_>`-style method that destructures
the carrier into disjoint field borrows, calls
`SessionOperationCheckout::inner_and_attachment_mut()` once, and constructs the
facade. Existing DML methods continue using direct `self.inner`,
`self.attachment`, `self.effects`, and `self.stmt_locks` references. Do not
make each method find or unwrap the checkout through `StmtState`.

`StmtState` exists only as a stack local captured by the compiler-generated
`Transaction::exec` or `stage_catalog_statement` future after checkout. It is
not a field of `Transaction`, a stable entry payload, registry state, queue
message, or worker object.

### 2. Refactor normal statement completion into consuming carrier exits

Refactor `Transaction::exec` so the checkout is immediately moved into a
public `StmtState`. Use a nested lexical scope for the callback-facing
`Statement` and its child future:

```text
checkout transaction
  -> construct armed public StmtState
  -> lend direct Statement borrows
  -> await callback
  -> merge effects or await statement rollback
  -> end Statement borrow
  -> consume StmtState through ordinary or fatal finalization
```

Successful callback handling continues to merge statement row undo, index
undo, and redo into transaction effects. Ordinary callback error handling
continues to roll back index effects before row effects, discard redo, and
return the original callback error. After either path has synchronously
settled its effects and the `Statement` borrow has ended, consume the carrier
through a non-awaiting ordinary-return method that:

1. changes its Drop action to `Settled`;
2. releases every statement lock;
3. takes and ordinarily drops/returns the checkout; and
4. leaves no owned payload for `StmtState::Drop`.

There must be no await or externally cancellable yield between effect
settlement and this consuming return. Returning the checkout before the
carrier becomes inert preserves the RFC rule that normal completion disarms
only after publishing the next owner.

If statement rollback produces a fatal error, preserve the current order:
move any residual statement effects into fatal retention, poison the engine,
release statement locks, consume the checkout through
`discard_after_fatal_rollback`, publish `FailedRetained`, and then return the
fatal public error. Do not route fatal rollback failure through ordinary
abandoned cleanup.

Refactor `stage_catalog_statement` through a private `StmtState` without
changing its current semantic contract: merge statement effects into the
private transaction even when its callback returns an ordinary error, then
consume the carrier through ordinary return. The private carrier inherits the
checkout attachment's existing outer `SessionOperationKey` and allocates no
new `OperationID`.

### 3. Fold residual cancellation payload synchronously in `StmtState::Drop`

Implement `StmtState::Drop` as a local policy boundary, not a rollback state
machine:

- `Settled`: perform no cancellation transition. The consuming finalizer has
  already released locks and returned or fatally discarded the checkout.
- `CancelPublicTransaction`: synchronously fold residual effects, release
  statement locks, and consume the checkout through its terminal cancellation
  return.
- `PrivateMustComplete`: release statement locks and preserve the narrow
  private non-empty invariant until Phase 3 owns the enclosing operation
  future. Never schedule public-transaction cancellation policy for a private
  catalog statement.

Structure `Transaction::exec` so Drop of the nested callback future and its
temporary acquisition guards precedes Drop of the armed `StmtState`. This
ordering must be visible in the lexical ownership layout and proved by
deterministic queued-waiter and promoted-grant tests.

Add a `StmtEffects` operation equivalent to
`fold_cancelled_into_trx_effects` that:

1. clears/destroys statement redo without moving it into transaction redo;
2. appends residual row undo to transaction row undo; and
3. appends residual index undo to transaction index undo.

Reuse `RowUndoLogs::merge` and `IndexUndoLogs::merge`, or an equivalently
efficient whole-buffer move. Do not clone undo entries, rebuild them from row
state, or walk them solely to publish a worker payload. Appending after prior
successful-statement entries preserves chronological order; the existing
whole-transaction rollback reverses the combined buffers, so current statement
residuals roll back before older transaction effects.

The current row/index rollback loops already hold the final entry across
awaits and pop only after success. Preserve that implementation and strengthen
its integration tests. If cancellation happens while rolling back the current
index or row entry, that entry remains in `StmtEffects`; earlier successfully
rolled-back entries have been popped. `StmtState::Drop` therefore transfers
exactly the residual suffix without a separate progress enum.

After folding, release `stmt_locks` through the exact existing statement
`LockOwner`. This release is synchronous and must occur before the transaction
core becomes `CleanupReady`. Transaction-owned locks and table bindings remain
inside `TrxInner` for worker rollback.

Delete the generic `StmtEffects::Drop` assertion after every construction site
is carrier-owned. Delete `Statement::Drop`; the carrier releases statement
locks for ordinary, cancelled, private-invariant, and fatal paths. Do not add a
public cancellation assertion elsewhere.

### 4. Add a cancellation-specific checkout return and entry transition

In `doradb-storage/src/trx/mod.rs`, add a consuming
`SessionOperationCheckout` operation equivalent to
`return_cancelled(self)`. It must take the complete `TrxInner` and invoke one
entry transition that, under the existing entry mutex:

1. validates the exact public-transaction operation key and `TrxID`;
2. accepts `ForegroundRunning(None)` with its core checked out;
3. coalesces an already-recorded `cleanup_requested` flag;
4. installs the returned core;
5. sets `cleanup_requested = true`;
6. publishes `SessionOperationState::CleanupReady`; and
7. reports that cleanup should be requested.

Do not first publish `ForegroundAvailable`, expose a reusable interval, or
perform a separate terminal claim. The cancellation return composes with the
checkout's existing physical ownership transfer and takes the same one entry
mutex acquisition as ordinary check-in. It must be non-panicking for every
valid handle-drop, session-close/drop, shutdown, and duplicate-hint race.

After releasing the entry mutex, use the checkout's authoritative
`TrxAttachment` to notify an installed lifecycle waiter when required and send
the existing identity cleanup request. Do not allocate or format diagnostics
on the successful statement path. A cancellation transition sends no
statement data.

Keep ordinary `SessionOperationCheckout::Drop` for normal non-terminal
operations. Its current behavior remains:

```text
ForegroundRunning(None) + no cleanup intent
  -> ForegroundAvailable

ForegroundRunning(None) + pre-existing cleanup intent
  -> CleanupReady + cleanup request
```

Do not borrow `Transaction::terminal_started` into `StmtState`. Cancellation
does not consume the public facade, so it may remain in caller memory as a
stale weak capability. Every later operation resolves the terminally
cancelling entry and returns `TransactionDiscarded`. When that facade
eventually drops, its existing abandonment request is idempotent; duplicate
identity hints cannot create a second cleanup claim.

### 5. Reuse whole-transaction worker cleanup and terminal proofs

Keep `SessionOperationCleanupMessage::Job(SessionOperationCleanupJob)` as the
only cancellation message. The worker resolves the stable entry by
`SessionOperationKey`, verifies the exact `TrxID`, and claims only
`CleanupReady -> CleanupRunning`. Stale or duplicate jobs remain neutral.

The worker then uses the existing abandoned whole-transaction rollback path.
It must:

1. roll back the combined transaction index and row undo;
2. release transaction table bindings;
3. release transaction logical locks;
4. produce and consume `ReleasedTransactionLocks` for the exact `TrxID`; and
5. publish transaction/session completion only afterward.

Do not add a statement rollback branch, statement payload inspection, or
`DiscardOnly` fast path to the worker. An effect-free cancellation follows the
same state and proof path.

Preserve explicit terminal rollback and commit handoff. An explicit rollback
that has already transferred its completion claim remains mandatory
worker-owned. A commit accepted into `CompletionOwned` remains commit-owned and
cannot be claimed by cancellation cleanup. Calls made through a stale facade
after statement cancellation fail before obtaining either terminal ownership.

### 6. Audit mutation ordering and Drop assertions

Audit every public `Statement` mutation path in
`doradb-storage/src/trx/stmt.rs` and its admission helpers. Before the first
row/index undo or redo effect becomes reachable:

1. transaction-lifetime table metadata/data protection and required table
   binding must be installed in `TrxInner`; and
2. any statement-only grant used while establishing that coverage must either
   be transferred/cached correctly or remain releasable by `StmtState`.

Where an existing path creates a statement effect before its required
transaction coverage, reorder it without changing the public DML result. Test
the no-effect first-touch case separately so releasing a statement-only grant
cannot strand a table lock.

Apply this assertion policy:

- Public `StmtState::Drop` performs reliable cleanup and contains no
  non-empty-effect assertion.
- `StmtEffects` has no generic Drop assertion.
- `Statement` has no Drop implementation.
- `Transaction::Drop` retains its current non-panicking abandonment behavior;
  there is no assertion to remove.
- The private carrier may retain a narrow must-complete assertion until
  Phase 3 transfers the enclosing DDL/maintenance future.
- `TrxInner::Drop`, `PreparedTrx::Drop`, `PrecommitTrx::Drop`, fatal-retention
  ownership checks, and `ReleasedTransactionLocks` validation remain internal
  correctness backstops. Do not weaken them merely because public cancellation
  now reaches the terminal cleanup path.

### 7. Update public and architecture documentation

Update `Transaction::exec` documentation to specify:

- dropping before first poll performs no checkout;
- after successful checkout, dropping the future synchronously settles
  statement-local ownership and terminally cancels the entire transaction;
- the worker completes whole-transaction rollback;
- the public transaction facade is discarded afterward; and
- ordinary callback errors still roll back only the current statement and
  leave the transaction reusable.

Update `docs/transaction-system.md` with the distinction between semantic
transaction lifetime and per-operation core checkout. Document that statement
finish ordinarily returns the core to `ForegroundAvailable` without ending
the transaction, while statement-future cancellation returns it directly to
`CleanupReady`.

Update `docs/lock-system.md` where necessary to record cancellation ordering:
pending acquisition settlement, residual undo folding, statement-lock release,
transaction rollback, binding release, transaction-lock release, and proof
consumption.

Do not edit RFC phase status or move backlog 000124 during implementation.
During `$task-resolve`, record the implemented task/issue, measurement outcome,
and Phase 2 summary in RFC-0025, and close backlog 000124 as implemented if all
acceptance conditions pass. Phase 3 prerequisites and design assumptions
otherwise remain unchanged.

### 8. Cache one ready public transaction core per session

Create each session lifecycle with one `public_trx_cache` containing a concrete
`Box<TrxInner>` in ready state: zero transaction identity, STS, GC bucket, and
statement number; empty zero-capacity effects and bindings; no lock state; and
one uniquely owned zero-valued `SharedTrxStatus`.

Public transaction begin must take that box and call `init(trx_id, sts, gc_no,
session_id)`. Private transaction begin must leave the public cache parked and
allocate a fresh `Box<TrxInner>`. Initialization must use `Arc::get_mut` to
prove the ready status is unshared before storing the new active transaction
id. Entry installation accepts only `Box<TrxInner>` so production call sites
cannot implicitly allocate or convert the core.

Carry the emptied box through prepared and precommit ownership. Successful
public commit or rollback must make the old status terminal, release prepare
state, effects, bindings, and locks, call `reset()`, and return the ready box
before the session exposes its idle state. `reset()` installs a new zero-valued
status and replaces variable-capacity containers. Successful private terminal
processing uses the core's creation-time cache policy to drop its fresh core
without reset or an additional cache-related session-lifecycle access and
leaves the public cache untouched. Required operation-terminal publication
still follows the normal lifecycle path. Do not add transaction-kind state to
the public facade, runtime attachment, or statement checkout. Session close,
session abandonment, engine poison, and fatal-retention paths that cannot admit
another public transaction drop the public shell rather than preparing another
one.

### 9. Validate structural and measured performance budgets

Add focused test-only observation only where existing entry snapshots, lock
debug snapshots, rollback hooks, and worker hooks cannot prove the required
order. Keep hooks behind `#[cfg(test)]`, synchronize through channels,
barriers, listeners, or explicit state predicates, and add no production
counter or successful-path branch solely for measurement.

Structurally prove that one successful `Transaction::exec` adds:

- no second registry or session-operation resolution;
- no heap allocation;
- no additional `Arc` clone/upgrade;
- no additional mutex or atomic operation;
- no lifecycle notification;
- no cleanup queue send; and
- no per-DML or per-stream-item carrier lookup.

The owned stack carrier may add only local moves, an `Option`/Drop-action tag,
and a predictable settled check. Existing `StmtEffects`, lock cache, and
checkout payloads are relocated rather than duplicated.

For `trx-noop`, compare warmed flamegraphs before and after session buffering.
The `Box<TrxInner>` allocation/free stacks must disappear from per-transaction
work. One `SharedTrxStatus` allocation remains at successful terminal reset to
prepare the next transaction; session-entry and unrelated runtime allocations
are outside this optimization.

Run paired optimized measurements from `origin/main` and the candidate on the
same host and Rust toolchain, using equivalent separately prepared roots.
Perform one unreported warmup followed by seven alternating measured samples
per configuration. Report absolute operations/second and average latency plus
median and interquartile range for:

1. `stmt-noop --num 1000000`, with `--threads 1 --sessions 1` and
   `--threads 4 --sessions 16`;
2. `trx-noop --num 100000`, with the same two concurrency configurations; and
3. unique and non-unique `index-stream` roots loaded with 100000 rows, using
   `--num 100 --range 1000 --seed 1` at both concurrency configurations.

Use `--release` and `--log-sync none`. Preserve the resolved workload,
prepared index mode, loaded row range, thread/session counts, operation count,
seed, and range in the evidence. A repeatable candidate regression outside
the paired baseline dispersion blocks resolution unless RFC-0025 is explicitly
amended; do not waive it merely as storage noise.

### 10. Run repository-authoritative validation

Run:

```text
rtk cargo fmt --all -- --check
rtk cargo nextest run --workspace
rtk cargo clippy --workspace --all-targets -- -D warnings
tools/style_audit.rs
```

Because the changed transaction code is compiled for both storage backends,
also run:

```text
rtk cargo nextest run -p doradb-storage --no-default-features --features libaio
rtk cargo clippy -p doradb-storage --no-default-features --features libaio --all-targets -- -D warnings
```

Run focused nextest stress coverage without retries for the deterministic
queued-waiter, promoted-grant, rollback-cancellation, and cleanup-race tests.
Use nextest's timeout only as a hang watchdog; elapsed time must not establish
the race.

## Implementation Notes

## Impacts

- `doradb-storage/src/trx/mod.rs`
  - `Transaction::exec`
  - `Transaction::stage_catalog_statement`
  - `Transaction::Drop`
  - `TrxEffects`
  - `SessionOperationEntry`
  - `SessionOperationCheckout`
  - `SessionOperationState`
  - transaction state-machine tests and hooks
- `doradb-storage/src/trx/stmt.rs`
  - `StmtEffects`
  - new `StmtState` and `StmtDropAction`
  - public `Statement<'_>` borrow layout
  - effect merge/rollback/fatal-retention paths
  - statement mutation ordering and tests
- `doradb-storage/src/trx/undo/index.rs`
  - preserve last-entry-across-await rollback semantics
  - focused residual-ownership hooks/tests if needed
- `doradb-storage/src/trx/undo/row.rs`
  - preserve last-entry-across-await rollback semantics
  - focused residual-ownership hooks/tests if needed
- `doradb-storage/src/trx/admission.rs`
  - first-touch transaction coverage audit
  - pre-binding statement-grant cancellation tests
- `doradb-storage/src/lock/mod.rs`
  - existing `WaiterGuard` queued and promoted-unobserved settlement behavior
  - deterministic cancellation tests/hooks only if required
- `doradb-storage/src/lock/state.rs`
  - statement versus transaction owner-cache release observations
- `doradb-storage/src/trx/sys.rs`
  - reuse identity cleanup jobs and whole-transaction rollback
  - worker ordering/duplicate-hint tests and test-only observations
- `doradb-storage/src/session.rs`
  - close, abandonment, stale facade, and lifecycle notification race tests
- `doradb-storage/src/engine.rs`
  - shutdown-discovered checked-out cancellation race tests if needed
- `docs/transaction-system.md`
  - per-operation checkout versus semantic transaction lifetime
  - terminal statement-cancellation contract
- `docs/lock-system.md`
  - statement/transaction lock release and proof order
- `docs/rfcs/0025-session-coordinated-cancellation-cleanup-ownership.md`
  - resolve-time Phase 2 synchronization only
- `docs/backlogs/000124-statement-execution-cancellation-safety.md`
  - resolve-time closure as implemented
- `doradb-bench`
  - no benchmark implementation change expected; run the existing
    `stmt-noop`, `trx-noop`, and `index-stream` workloads

No durable storage format, public error classification, public method
signature, MVCC identity, recovery rule, or unsafe-code invariant is expected
to change.

## Test Cases

### Checkout and facade boundaries

1. Construct and drop an unpolled `Transaction::exec` future. Assert that no
   checkout, cleanup intent, notification, or job occurs and a subsequent
   statement succeeds.
2. Poll `Transaction::exec` through checkout into a controlled pending
   callback with no effects, then drop it. Assert synchronous
   `ForegroundRunning(None) -> CleanupReady`, one checked-in complete core,
   and no reusable `ForegroundAvailable` interval.
3. Assert that `StmtState` owns the checkout/effects/locks while the callback
   `Statement` directly borrows the same `TrxInner`; no second core, entry, or
   effect accumulator is created.
4. Complete a successful no-op and effectful statement. Assert ordinary
   check-in to `ForegroundAvailable`, no cleanup intent/job/notification when
   no lifecycle waiter is installed, and successful reuse of the transaction.
5. Return an ordinary callback error. Assert index-then-row statement rollback,
   redo discard, statement-lock release, ordinary check-in, and successful
   execution of a later statement.

### Residual effect ownership

6. Cancel after creating row undo but before statement completion. Assert that
   the exact boxed row undo remains owned, is appended after prior transaction
   undo, and is later consumed by whole-transaction rollback.
7. Cancel after creating index undo. Assert exact buffer identity/order and
   reverse whole-transaction rollback order.
8. Cancel after creating statement redo. Assert redo is destroyed
   synchronously and no cancelled redo reaches transaction commit/logging.
9. Cancel after mixed row undo, index undo, and redo. Assert both undo buffers
   are emptied into `TrxEffects`, redo is empty, and no statement payload
   remains in the carrier or entry.
10. Pause ordinary index rollback while it borrows the last entry across an
    await, drop `exec`, and assert that current plus older residual entries are
    folded while already-successful newer rollback entries stay absent.
11. Repeat the preceding case for row rollback, including an owned row undo
    whose address is reachable from an MVCC chain.
12. Exercise the synchronous boundary after successful effect merge but before
    carrier finalization using a focused state-machine hook/test. Assert that
    cancellation still makes the transaction terminal and does not duplicate
    already-merged effects.
13. Inject index and row rollback failure. Assert existing engine poison and
    fatal retention own every residual pointer, the checkout becomes
    `FailedRetained`, and abandoned cleanup does not claim it.

### Acquisition and lock ordering

14. Drop while a statement logical-lock waiter is queued. Assert
    `WaiterGuard` removes it before undo folding/check-in and it cannot receive
    a later grant.
15. Promote a queued waiter without letting its future observe the grant, then
    drop `exec`. Assert the unobserved grant is synchronously released before
    `CleanupReady`.
16. Cancel the first table touch after a statement-only metadata grant but
    before transaction binding/effect creation. Assert the statement grant is
    gone, no undo exists, and the transaction is still terminally cancelled.
17. Cancel after row/index effects while pausing the cleanup worker. Assert all
    statement locks are already absent, while transaction metadata/data locks
    and bindings remain held.
18. Release the worker and assert transaction effects roll back, bindings
    release, transaction locks release, and the exact
    `ReleasedTransactionLocks` proof is consumed before the operation becomes
    terminal.
19. Audit all DML variants that create row/index/redo effects and assert
    transaction-lifetime protection exists before the first effect.

### Terminal states and races

20. After public statement cancellation, call `exec`, `lock_table`, stream
    statement operations, commit, and rollback through the stale facade as
    applicable. Assert `TransactionDiscarded` and no second core claim.
21. Drop the stale `Transaction` before and after the worker claims cleanup.
    Assert duplicate abandonment hints are neutral and cleanup runs exactly
    once.
22. Race cancellation return with session close/drop and shutdown discovery
    through explicit barriers. Assert cleanup intent coalesces, the core is
    checked in once, shutdown waits for the authoritative owner, and terminal
    publication occurs once.
23. Deliver stale and duplicate `(SessionOperationKey, TrxID)` cleanup jobs.
    Assert no wrong-transaction or second cleanup claim.
24. Preserve explicit terminal rollback cancellation tests: once its mandatory
    job owns the completion claim, dropping its waiter cannot transfer or
    duplicate ownership.
25. Preserve commit handoff tests: once the entry is `CompletionOwned`,
    cancellation/abandonment hints cannot convert it to `CleanupReady`.
26. Verify no-effect cancellation uses whole-transaction cleanup and does not
    introduce an unproved `DiscardOnly` path.

### Assertions, private transactions, and streams

27. Replace the existing non-empty `StmtEffects::Drop` panic test with public
    cancellation tests proving non-panicking ownership transfer.
28. Verify `Statement` has no independent Drop action and `StmtState` releases
    statement locks on every public normal/error/cancel/fatal exit.
29. Verify ordinary `stage_catalog_statement` success and error still merge
    effects and return its private checkout under the inherited outer operation
    key without allocating another `OperationID`.
30. Verify the private must-complete path cannot accidentally queue public
    transaction cancellation. Keep its phase-local invariant focused and
    separate from public cancellation acceptance.
31. Preserve `TrxInner`, prepared, precommit, fatal-retention, and terminal
    proof clearing tests; public cancellation must satisfy rather than remove
    those invariants.
32. Preserve `StreamStmt` behavior: one checkout spans the stream lifetime,
    there is no per-item coordinator access, and dropping an ordinary read-only
    stream returns its transaction to reusable `ForegroundAvailable`.

### Performance and validation

33. Prove through focused test-only observations that successful
    `Transaction::exec` emits no cleanup job or lifecycle notification and adds
    no second shared synchronization step.
34. Run the paired `stmt-noop`, `trx-noop`, and unique/non-unique
    `index-stream` matrix with one warmup and seven alternating samples.
    Record absolute results, median, IQR, baseline/candidate delta, toolchain,
    host, root preparation, and resolved workload configuration.
35. Run the repository-authoritative default and alternate-backend test/lint
    commands and the style audit.
36. Run deterministic focused stress passes for the queued waiter,
    promoted-unobserved grant, row/index rollback cancellation, duplicate
    cleanup, and shutdown race tests. No test may use sleep, retry, or elapsed
    time to establish its prerequisite state.

## Open Questions

There are no unresolved implementation choices for this task.

The following are explicit follow-ups rather than Phase 2 decisions:

1. If absolute `stmt-noop` results show that the existing Phase 1
   per-operation resolution/check-in cost is too high despite a negligible
   candidate delta, plan a separate optimization around cached weak exact-entry
   capabilities. Do not move the transaction core into the public handle as an
   incidental Phase 2 optimization.
2. Phase 3 replaces the private must-complete fallback with reliable
   whole-DDL/maintenance future ownership and background continuation.
3. A later task may add `DiscardOnly` cleanup only after proving absence of
   undo, redo, bindings, logical locks, and external publication. Phase 2 uses
   the existing rollback claim uniformly.
4. During `$task-resolve`, close source backlog 000124 as implemented and sync
   RFC-0025 Phase 2's task path, issue, status, implementation summary, and
   measurement evidence. Do not change Phase 3 prerequisites unless
   implementation findings invalidate an accepted assumption.
