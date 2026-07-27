---
id: 000242
title: Enforce Terminal Transaction Lock-Release Ordering
status: implemented  # proposal | implemented | superseded
created: 2026-07-27
github_issue: 901
---

# Task: Enforce Terminal Transaction Lock-Release Ordering

## Summary

Make transaction-lock release a structural prerequisite of terminal session
completion.

Two successful commit paths currently invert the required order. Ordered
commit publishes committed status, completes the session transaction
lifecycle, and only then releases the `OwnerLockState` carried by
`PrecommitTrx`. The unordered read-only/no-op path likewise performs its
rollback-style session transition before releasing the lock state carried by
`PreparedTrx`. A running session can therefore become idle, or an abandoned
session can close and release its explicit session locks, while claims owned by
the just-finished transaction still exist.

Introduce a transaction-id-bound, non-cloneable
`ReleasedTransactionLocks` proof. Transaction code can mint the proof only
after draining the transaction owner-local lock state. Carried
prepared/precommit paths must also consume and drop their retained lock-manager
guard before minting it. `TrxAttachment::commit` and
`TrxAttachment::rollback` must consume a matching proof before they can make
the session idle or closed. Restrict the raw session-registry finish methods so
production code cannot bypass that boundary.

Correct the two inverted paths, route the already-correct rollback and
failed-precommit paths through the same proof contract, and add deterministic
boundary observation tests. Preserve status publication, undo, purge,
group-commit, session-abandonment, lock-manager, and recovery semantics outside
this ordering constraint.

## Context

Issue Labels:

- type:task
- priority:high
- codex

Related Designs:

- `docs/lock-system.md`
- `docs/transaction-system.md`
- `docs/architecture.md`

Related Process:

- `docs/process/coding-guidance.md`
- `docs/process/unit-test.md`

This is a standalone prerequisite for the future lock-system redesign. It is
not an RFC phase and is not sourced from a backlog. The task deliberately fixes
the current implementation before any lock owner, scope, waiter, token, or
resource representation changes.

The current terminal paths divide into two groups:

| Terminal path | Current lock/session order | Assessment |
| --- | --- | --- |
| `PrecommitTrx::commit` user branch | committed status -> session commit -> transaction-lock release | Incorrect |
| `TransactionSystem::discard_unordered_prepared` | purge record -> session rollback transition -> transaction-lock release -> status finish | Incorrect |
| `TransactionSystem::rollback_inner` | rollback effects/cache teardown -> transaction-lock release -> session rollback | Correct |
| `TrxInner::retain_and_discard_after_fatal_rollback` | retain effects -> transaction-lock release -> session rollback | Correct |
| `PrecommitTrx::rollback_failed_precommit` | rollback effects/purge record -> transaction-lock release -> session rollback -> status finish | Correct |
| `PrecommitTrx::finish_failed_precommit_with_retention` | transaction-lock release -> session rollback -> prepare-waiter release/retention | Correct |

`SessionState::finish_trx` makes the ordering correctness-visible:

- a running active session becomes `RunningIdle`, allowing a replacement
  transaction to begin;
- an abandoned active session becomes `Closed`, releases
  `LockOwner::Session` claims, and is removed from the registry.

Neither transition may occur while the previous
`LockOwner::Transaction(trx_id)` still has a granted lock or queued request.
The required sequence is:

```text
ordered commit:
    publish committed status
    -> release transaction locks
    -> finish session commit

rollback or no-op discard:
    finish rollback effects and required bookkeeping
    -> release transaction locks
    -> finish session rollback-style transition

abandoned session:
    release transaction locks
    -> close session
    -> release explicit session locks
```

`OwnerLockState` already supplies the appropriate release mechanism. It caches
the strongest granted mode per resource for one transaction owner, and
`release_all` drains only those cached resources. The correction must continue
to use this owner-local path. A global `LockManager::release_owner` scan or
debug snapshot on every production commit would add the wrong complexity and
is not required to establish the invariant.

The accepted proof direction is stronger than swapping two statements:

- a simple reorder would fix the known call sites but leave
  `TrxAttachment::commit` and `rollback` callable without evidence that the
  release stage ran;
- starting the future `LockScopeState` redesign here would couple this
  prerequisite to unresolved representation and migration choices;
- a narrow proof retains the current lock manager while making the ordering a
  type-level production requirement and can later evolve into a closed-scope
  proof.

The strict RFC complexity gate passes. This task changes one private
transaction/session lifecycle boundary, its callers, deterministic tests, and
live conceptual documentation. It does not introduce a public or durable data
model, change recovery, require phased rollout, or span an independently
deployable program.

## Goals

1. Make transaction-lock release a required predecessor of every production
   `TrxAttachment` session completion.
2. Add a private, transaction-id-bound `ReleasedTransactionLocks` proof that
   cannot be directly constructed outside transaction code.
3. Keep the proof non-`Clone`, non-`Copy`, and non-`Default` so one completed
   release stage authorizes one session completion attempt.
4. Mint the proof only after the transaction `OwnerLockState` is empty.
5. For prepared/precommit state, consume and drop the paired
   `QuiescentGuard<LockManager>` before returning the proof.
6. Validate at the attachment boundary that the proof transaction id matches
   the attachment transaction id.
7. Correct ordered commit so committed status is published before lock release
   and session completion occurs after lock release.
8. Correct unordered/no-op commit so its purge bookkeeping remains before lock
   release and its rollback-style session transition occurs after lock release.
9. Route normal rollback, abandoned rollback, failed-precommit rollback, and
   fatal cleanup through the same proof-requiring attachment interface without
   changing their existing effect-cleanup order.
10. Prevent production code outside `session.rs` from invoking raw
    `SessionRegistry` transaction-finish methods.
11. Preserve attachmentless system-transaction behavior.
12. Preserve session stale-completion behavior and test it through explicit
    test-only access rather than a production bypass.
13. Observe the post-release/pre-session-finish boundary deterministically in
    tests.
14. Prove that abandoned-session explicit lock cleanup happens after
    transaction-lock cleanup.
15. Preserve owner-local cleanup complexity and avoid allocations, global
    scans, or new synchronization in production.
16. Update live transaction and lock-system documentation to describe the
    implemented invariant rather than a known defect.

## Non-Goals

1. Do not redesign `LockOwner`, `LockOwnerGroup`, `OwnerLockState`,
   `LockManager`, resource state, waiter state, or cancellation tokens.
2. Do not add DDL or maintenance operation owner identities.
3. Do not introduce `LockScopeState`, owner-resource cells, a scope reverse
   index, resource incarnations, claim ids, or persistent waiter nodes.
4. Do not change lock modes, compatibility, coverage, same-session policy,
   conversion, queue bypass, or FIFO promotion.
5. Do not change statement-lock release or
   statement-to-transaction lock handoff.
6. Do not optimize explicit session-lock cleanup or replace its current
   `release_owner` scan.
7. Do not add a production global scan that asserts a transaction owner is
   absent.
8. Do not change transaction status, CTS assignment, redo durability,
   group-commit ordering, purge handoff, GC bucket assignment, or active-STS
   behavior.
9. Do not change rollback undo ordering, fatal rollback retention, engine
   poisoning, or cleanup-worker ownership.
10. Do not change session abandonment, shutdown drainage, registry removal, or
    stale terminal-completion semantics beyond requiring the lock-release
    proof.
11. Do not change public APIs, errors, persistent formats, recovery, or
    checkpoint behavior.
12. Do not add unsafe code.
13. Do not modify historical task or RFC documents.
14. Do not make this task a phase of the future lock-system RFC.

## Plan

### 1. Add the transaction-lock release proof

Define the proof in `doradb-storage/src/trx/mod.rs`:

```rust
pub(crate) struct ReleasedTransactionLocks {
    trx_id: TrxID,
}
```

Keep its field and constructor private to the `trx` module. Do not derive or
implement `Clone`, `Copy`, or `Default`.

Provide only the narrow crate-visible validation needed by `session.rs`.
Validation consumes the proof and asserts that its `trx_id` equals the
`TrxAttachment` transaction id. The assertion diagnostic must identify both
transaction ids and the terminal boundary. A mismatch is an internal
correctness violation, not a runtime error.

The proof represents completion of the current transaction owner-local cleanup
contract:

- every cached transaction grant was passed to `LockManager::release`;
- the local `OwnerLockState` is empty;
- any carried lock state and paired component guard were taken and dropped;
- zero cached locks is valid if the release stage still ran.

It does not claim that the future lock redesign's scope representation already
exists, and its name and documentation must not imply durable state.

### 2. Mint proofs from active and carried cleanup

Change `TrxInner::release_transaction_locks` from an unused release-count return
to `ReleasedTransactionLocks`.

The method must:

1. require the transaction core to remain active at this terminal stage;
2. clear transaction table bindings before logical lock release, preserving
   the current runtime-owner order;
3. drain its `OwnerLockState` through the attachment engine's lock manager;
4. assert the state is empty;
5. mint a proof bound to the original transaction id.

Keep `OwnerLockState::release_all` and its release count unchanged for its
general callers and tests. The terminal wrapper no longer exposes that count
because no terminal caller uses it.

Change `release_carried_transaction_locks` to return
`Option<ReleasedTransactionLocks>`:

- `(Some(lock_state), Some(lock_manager))` must assert that the owner is
  `LockOwner::Transaction(trx_id)`, drain and validate the state, explicitly
  drop both values, then return `Some(proof)`;
- `(None, None)` returns `None` only for attachmentless system-transaction
  state;
- either mismatched pair is an internal assertion failure with identifying
  context.

`PreparedTrx` and `PrecommitTrx` wrappers should expose the resulting optional
proof only to their terminal implementation. They must not add a general
public release-token API.

### 3. Require proof at the session attachment boundary

Import `ReleasedTransactionLocks` into `doradb-storage/src/session.rs` and
change the private transaction attachment interface to:

```rust
pub(crate) fn commit(
    &self,
    released: ReleasedTransactionLocks,
    cts: TrxID,
)

pub(crate) fn rollback(
    &self,
    released: ReleasedTransactionLocks,
)
```

Each method must:

1. consume and validate the proof against `self.trx_id`;
2. run the test-only terminal-boundary observation hook, when configured;
3. invoke the matching session-registry finish method.

The hook therefore observes a point where lock release is complete but the
session is still active or abandoned-active. Do not run it while holding the
session lifecycle mutex.

Make `SessionRegistry::finish_trx_commit` and
`finish_trx_rollback` private to `session.rs`. Their only production callers
must be the proof-gated `TrxAttachment` methods. Preserve the current
`notify_trx_changed` and stale-id behavior.

The existing tests in `trx/mod.rs` and `engine.rs` that intentionally inject a
registry finish without a terminal transaction should use narrowly named
`#[cfg(test)]` helpers in `session::tests`. Do not keep production
`pub(crate)` visibility for test convenience.

Audit the completed production tree with searches for:

- `TrxAttachment::commit` and `.rollback`;
- `finish_trx_commit` and `finish_trx_rollback`;
- `release_transaction_locks`;
- direct session lifecycle completion.

No production session transaction finish may remain outside the proof-gated
attachment path.

### 4. Correct ordered commit

Refactor the user branch of `PrecommitTrx::commit` without changing its
irreversible commit semantics:

1. convert index undo into index-GC work as today;
2. call `SharedTrxStatus::commit_prepared(cts)`;
3. construct or retain the committed purge payload;
4. release carried transaction locks and obtain the proof;
5. pair the user attachment with `Some(proof)` and call
   `TrxAttachment::commit(proof, cts)`;
6. return `CommittedTrx` for the existing purge handoff.

Committed status must remain visible before the transaction grants are
released. Session completion must occur only after the grants and carried
lock-manager guard are gone. The log thread must continue to enqueue committed
purge payloads before completing commit-group waiters.

For `PrecommitTrxPayload::System` and an empty system payload, require no
attachment and no release proof. Preserve their current ordered commit and
purge behavior.

Assert inconsistent user/system combinations rather than silently dropping an
attachment, proof, lock state, or manager guard.

### 5. Correct unordered/no-op commit

Keep `TransactionSystem::discard_unordered_prepared` as the no-CTS path for a
user transaction with no ordered runtime effects.

Its required order is:

```text
validate and take the user payload
-> record rollback-style purge bookkeeping for STS/GC
-> release carried transaction locks and obtain Some(proof)
-> take the user attachment
-> attachment.rollback(proof)
-> status.finish_terminal()
```

The rollback-named attachment call remains an internal session lifecycle
transition; public commit still returns CTS zero. Do not manufacture redo or a
commit timestamp for a lock-only transaction.

Require the user attachment and proof to be present together. An attachment
without a proof, or a proof without an attachment, is an internal invariant
failure.

### 6. Route existing rollback paths through the proof

Preserve the already-correct cleanup order while adapting these consumers:

- `TransactionSystem::rollback_inner`;
- abandoned transaction cleanup through `rollback_claim`;
- `TrxInner::retain_and_discard_after_fatal_rollback`;
- `TrxCheckout::discard_after_fatal_rollback`;
- `PrecommitTrx::rollback_failed_precommit`;
- `PrecommitTrx::finish_failed_precommit_with_retention`;
- the test-only `discard_production_prepared_for_test`.

For active rollback, continue to:

1. finish index and row undo or retain them after fatal failure;
2. drop rollback `TableCache` runtime owners;
3. clear effects and table bindings;
4. record purge bookkeeping;
5. release transaction locks and obtain the proof;
6. finish the session with that proof;
7. finish transaction entry/status state.

For failed precommit, finish rollback-capable undo before releasing locks.
Release locks and finish the session before waking prepare or commit waiters.
If rollback access fails, preserve current poison and retention behavior; the
retained payload must not keep an attachment, lock state, proof, or lock-manager
guard.

Attachmentless rejected/system precommit cleanup may drain a paired system
lock field only if the existing representation supplies one, but it must not
mint or consume a user transaction proof. Preserve the current assertion that
system transactions have no session cleanup obligation.

### 7. Add deterministic terminal-boundary observation

Add a narrow `#[cfg(test)]` observation hook adjacent to
`TrxAttachment::commit` and `rollback`. Follow the repository's existing
guarded-hook pattern:

- store an optional `Arc<dyn Fn(...) + Send + Sync>` behind a mutex;
- return a guard that restores the previous hook on drop;
- include transaction id and commit-versus-rollback outcome;
- let tests filter on their target transaction;
- serialize tests that install the global hook.

Invoke the hook after proof validation and before registry mutation. The hook
must only observe or send state through a channel; it must not create
production synchronization or rely on elapsed time for progress.

Use the hook to record:

- count of lock-manager entries for the old transaction owner;
- whether the matching session/transaction remains registry-visible;
- whether the commit status already contains the assigned CTS for ordered
  commit;
- when the public session was abandoned, whether explicit session-owned locks
  still exist.

Assertions may use existing test-only lock debug snapshots and registry
helpers. Timeouts are watchdogs only.

### 8. Synchronize live documentation

Update `docs/lock-system.md`:

- replace the current “Known cleanup-ordering defect” wording with the
  implemented proof-bound invariant;
- describe ordered commit, rollback/no-op, and abandoned-session release order;
- retain the requirement as a constraint for the future redesign.

Update `docs/transaction-system.md`:

- state that terminal session completion consumes
  `ReleasedTransactionLocks`;
- document that status publication or rollback effects precede lock release,
  while session idle/close follows it;
- preserve the existing group-commit and cleanup-worker descriptions.

Do not edit historical task or RFC documents.

## Implementation Notes

Implemented the proof-gated terminal boundary as planned:

- Added the non-cloneable, transaction-id-bound
  `ReleasedTransactionLocks` proof. Active and carried terminal cleanup mint
  it only after owner-local lock state is empty; carried prepared/precommit
  cleanup also consumes and drops the paired lock-manager guard first.
- Changed `TrxAttachment::commit` and `rollback` to consume a matching proof,
  with identity validation through `assert_validated_for`. Raw
  `SessionRegistry` transaction-finish methods are now private to
  `session.rs`; stale-completion tests use narrow test-only helpers.
- Corrected ordered commit and unordered/no-op commit ordering, and routed
  normal, abandoned, failed-precommit, fatal-retention, and test cleanup
  through the same proof contract. User/system payload mismatches now fail as
  internal invariants, while attachmentless system transactions remain
  proof-free.
- Added a serialized test-only attachment hook that observes the boundary
  after transaction-lock release and before session mutation. Deterministic
  tests cover proof identity, ordered commit, unordered commit, rollback,
  failed precommit, and abandoned-session explicit-lock ordering.
- Updated the live lock and transaction-system documents to describe the
  implemented invariant. A catalog-history test also now releases its
  intentionally cloned layout/index runtime pins before `DROP TABLE`; the
  corrected terminal timing exposed that test-only lifetime race without
  requiring a production behavior change.

Verification completed with six focused terminal-boundary tests, strict
workspace clippy, and the standard workspace pass of 1,551 tests. The exposed
catalog-history race passed 100/100 focused stress iterations after the
test-only pin correction. The mandatory resolve-time style audit passed all
five branch-diff Rust files against `origin/main`. The alternate `libaio` pass
was not run, as planned, because no storage-I/O path changed.

Prepare-aware waiting for cold-row `ColumnDeletionBuffer` ownership was
identified during review and deferred to
`docs/backlogs/000168-add-cold-row-prepare-waiting.md`. That work is one
prerequisite for any future attempt to release logical locks during redo
persistence, but it does not by itself prove failed-precommit rollback safe
against concurrent DDL or runtime teardown.

## Impacts

### Runtime modules

- `doradb-storage/src/trx/mod.rs`
  - `ReleasedTransactionLocks`
  - `TrxInner::release_transaction_locks`
  - `PreparedTrx`
  - `PrecommitTrx`
  - `release_carried_transaction_locks`
  - fatal and test-only prepared cleanup
- `doradb-storage/src/trx/sys.rs`
  - `TransactionSystem::rollback_inner`
  - `TransactionSystem::discard_unordered_prepared`
  - abandoned/terminal rollback consumers
- `doradb-storage/src/session.rs`
  - `SessionRegistry::finish_trx_commit`
  - `SessionRegistry::finish_trx_rollback`
  - `SessionState::finish_trx`
  - `TrxAttachment::commit`
  - `TrxAttachment::rollback`
  - test-only terminal observation and raw-finish helpers

### Behavioral impact

- A running session becomes idle only after its previous transaction claims
  have been released.
- An abandoned session releases explicit session claims only after its
  transaction claims have been released.
- Lock waiters may be awakened before the same session can start a replacement
  transaction. This is the intended lifecycle boundary.
- Ordered commit status remains published before lock release.
- Public commit, rollback, cancellation, abandonment, shutdown, and stale
  completion results do not change.

### Performance and storage impact

- The proof is one stack value containing a `TrxID`; it allocates nothing.
- Cleanup remains proportional to resources cached in `OwnerLockState` plus
  existing lock-manager release/promotion work.
- No global resource scan, new mutex, or async wait is added in production.
- No persistent bytes, redo records, catalog rows, table files, or recovery
  rules change.

### Documentation

- `docs/lock-system.md`
- `docs/transaction-system.md`

## Test Cases

1. **Proof identity**
   - A proof validates and is consumed for the matching transaction id.
   - A deliberately mismatched proof in a focused unit test triggers an
     assertion whose diagnostic includes both ids.
   - Production code cannot clone, copy, default-construct, or directly mint a
     proof.

2. **Ordered commit boundary**
   - Begin a user transaction, acquire at least one transaction lock, and add
     an effect that requires ordered commit.
   - Capture its shared status and install the terminal observation hook.
   - At the hook, assert the status contains the assigned CTS, the transaction
     owner has zero lock entries, and the matching session transaction is
     still registry-visible.
   - After commit, assert the session is idle and reusable.

3. **Unordered/no-op commit boundary**
   - Begin a transaction whose only state is one or more logical locks.
   - Commit through the no-op path and assert CTS zero.
   - At the rollback-style attachment hook, assert transaction locks are gone
     while the session remains active.
   - After completion, assert the session is idle and reusable.

4. **Normal rollback boundary**
   - Acquire transaction locks and perform rollback-capable work.
   - At the attachment hook, assert undo cleanup has reached its existing safe
     boundary, transaction locks are absent, and the session is still active.
   - After rollback, assert entry/status completion and session reuse.

5. **Failed-precommit rollback**
   - Retain the existing failed-precommit injection.
   - Assert rollback effects and purge bookkeeping precede the observed
     lock-release/session boundary.
   - Assert transaction locks are absent before the session finishes and
     prepare/commit waiters wake only after cleanup.

6. **Fatal rollback retention**
   - Keep the existing index/row rollback failure tests.
   - Assert retained fatal payloads own no session attachment, lock state,
     proof, or lock-manager guard.
   - Assert the poisoned session is not exposed with residual transaction
     locks.

7. **Abandoned-session cleanup order**
   - Acquire an explicit session lock and a transaction lock, using compatible
     or distinct resources.
   - Drop the public session while retaining the transaction terminal handle.
   - At the terminal hook, assert the transaction owner is absent, the session
     remains registry-visible as active/abandoned-active, and the explicit
     session owner still exists.
   - After terminal completion, assert the session registry entry and explicit
     session owner are both gone.

8. **Stale completion**
   - Preserve tests where an old transaction completion must not finish a
     replacement transaction.
   - Route intentional raw completion through a `#[cfg(test)]` helper and
     verify no production raw finish API remains visible.

9. **System transaction regression**
   - Exercise ordered system commit and rejected system precommit cleanup.
   - Assert they remain attachmentless and do not require or produce a user
     transaction release proof.

10. **Existing lock cleanup regression**
    - Preserve current tests for statement versus transaction lock lifetime,
      read-only commit, ordered commit, normal rollback, precommit abort,
      dropped transaction handles, session reuse, and shutdown drainage.

11. **Validation**
    - Run focused transaction/session tests while developing.
    - Run `rtk cargo fmt --all -- --check`.
    - Run `rtk cargo clippy --workspace --all-targets -- -D warnings`.
    - Run `rtk cargo nextest run --workspace`.
    - The alternate `libaio` pass is not required because this task does not
      change storage I/O or backend-neutral I/O paths.

## Open Questions

No unresolved question blocks this implementation.

The future lock-system redesign may replace `ReleasedTransactionLocks` with a
general closed transaction-scope proof. That redesign must preserve the
accepted ordering and should adapt the attachment boundary rather than remove
it. DDL and maintenance operation ownership remains a separate prerequisite
task.

Prepare-aware cold-row ownership waiting and its required retry/failure
semantics are tracked in
`docs/backlogs/000168-add-cold-row-prepare-waiting.md`. Moving logical-lock
release before durable commit/status publication remains out of scope until
that work and failed-precommit rollback safety against DDL and runtime
lifecycle changes are both designed and verified.
