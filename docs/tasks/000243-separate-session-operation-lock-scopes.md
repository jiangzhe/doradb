---
id: 000243
title: Separate Session Operation Lock Scopes
status: implemented  # proposal | implemented | superseded
created: 2026-07-28
github_issue: 906
---

# Task: Separate Session Operation Lock Scopes

## Summary

Replace the current purpose-overloaded session lock owner with canonical exact
owner identities.

`LockOwner::Session(SessionID)` currently represents three different
lifetimes:

- explicit locks retained until `Session::unlock_table` or session teardown;
- DDL locks retained only for one CREATE/DROP TABLE or CREATE/DROP INDEX call;
- maintenance locks retained only while one scoped table-runtime access is
  active.

DDL compensates for that shared identity with an explicit-lock preflight.
Maintenance relies on `LockGrant::Fresh` versus `Existing` so its guards do not
release a covering explicit session claim. Session cleanup uses
`LockManager::release_owner(LockOwner::Session(id))`, which cannot express that
it intends to release only the explicit lifetime.

Introduce canonical `LockFamily(SessionID)` plus exact `LockOwner { family,
scope }` identity for session-explicit, transaction, statement, DDL, and
maintenance scopes. DDL and maintenance receive typed operation ids from one
shared engine-local `AtomicU64` sequence. Remove the separate
`LockOwnerGroup` representation and derive the current directional
same-session policy from `LockOwner::family()`.

Keep the current `Vec`/`VecDeque` resource representation, fresh-lock guards,
duplicate-waiter support, cancellation behavior, conversion rules, and
concurrency-tolerant cleanup. Rename the now-obsolete
`LockOwnerGroupConflict` operation error to `LockFamilyConflict`. Do not add
scope-owned claim maps, serialized family mutation, or any part of the later
cancellation and exact-family manager redesign.

## Context

Issue Labels:

- type:task
- priority:high
- codex

Source Backlogs:

- `docs/backlogs/closed/000169-separate-session-operation-lock-scopes.md`

Related Designs:

- `docs/architecture.md`
- `docs/transaction-system.md`
- `docs/lock-system.md`
- `docs/data-checkpoint.md`
- `docs/rfcs/0016-logical-lock-manager.md`

Related Process:

- `docs/process/coding-guidance.md`
- `docs/process/lint.md`
- `docs/process/unit-test.md`

Related Follow-ups:

- `docs/backlogs/000115-explicit-session-lock-cache.md`
- `docs/backlogs/000170-session-coordinated-cancellation-cleanup.md`
- `docs/backlogs/000171-exact-family-lock-system-redesign.md`

The implemented lock manager stores:

```rust
struct ResourceState {
    granted: Vec<GrantedLock>,
    waiters: VecDeque<Arc<Waiter>>,
}

struct GrantedLock {
    owner: LockOwner,
    owner_group: Option<LockOwnerGroup>,
    mode: LockMode,
}
```

Transactions and statements retain their exact owner in `OwnerLockState`, but
carry family identity separately through `Option<LockOwnerGroup>`. Explicit
session, DDL, and maintenance requests all use
`LockOwner::Session(session_id)` plus
`LockOwnerGroup::Session(session_id)`.

This creates several accidental couplings:

1. `SessionDdlContext` cannot name a DDL operation independently from the
   session's explicit locks.
2. `ScopedTableRuntimeAccess` cannot record a maintenance claim when an
   explicit session claim already covers the request. The manager reports an
   existing exact-owner grant instead.
3. DDL needs `reject_table_ddl_explicit_session_lock` because exact identity
   cannot distinguish the two purposes.
4. `SessionState::release_session_locks` names every session-purpose claim,
   even though the lifecycle transition intends only the explicit scope.
5. Debug snapshots and error attachments cannot report whether a session
   entry belongs to explicit locking, DDL, or maintenance.
6. A later exact-family manager would first have to undo the owner/group split
   before it could index exact claims by scope.

The current same-session behavior remains valid and must be preserved. For a
request from one exact owner, every held or waiting claim from another owner in
the same family must directionally cover the request. A covering family claim
may let a new request bypass an older external waiter. A non-covering claim
returns an operation error rather than waiting on the caller's own family.

DDL has an additional purpose rule: a target table with a held
`SessionExplicit` claim in the same family is rejected even when a synthetic
covering mode would otherwise admit DDL. Maintenance uses the ordinary
directional rule. With the public explicit modes, `Metadata(S)` and
`Data(S|X)` cover maintenance `Metadata(S)` and `Data(IS)`, so maintenance may
run while retaining a separate exact claim.

The accepted maintenance close boundary is one
`ScopedTableRuntimeAccess` instance. Freeze, checkpoint, hot-row-page
counting, secondary MemIndex cleanup, and one bounded checkpoint-retry recheck
each own one such scope. A checkpoint retry intentionally drops the access
before its indefinite sleep, so the next bounded recheck receives a new
maintenance operation id.

The accepted DDL close boundary is one `SessionDdlContext`, constructed once
per public CREATE/DROP TABLE or CREATE/DROP INDEX call. Its operation owner
survives through all target-table locks and the existing DDL progress guards.
Nested catalog transactions continue to use transaction and statement scopes
in the same family.

The operation-id source is intentionally simple. `EngineInner` owns one
`AtomicU64`, initialized to one. DDL and maintenance allocation both use
relaxed `fetch_add` on that field and wrap the returned raw value in their
respective crate-private id types. There is no allocator wrapper, overflow
branch, or overflow test. Exhausting the `u64` space is outside the supported
engine-lifetime envelope. Failed or cancelled operations may consume ids, and
ids are never deliberately recycled.

The chosen direction canonicalizes all production lock owners now. A smaller
alternative that only added DDL and maintenance variants was rejected because
it would retain two identity systems and push the central family conversion
into backlog 000171. A broader alternative that also introduced
`LockScopeState`, serialized family mutation, physical family aggregation, or
tokenized waiters was rejected because nested DDL cancellation does not yet
prove exclusive family cleanup ownership. That program remains ordered behind
backlog 000170 and requires the later exact-family RFC.

The strict RFC complexity gate therefore passes for this task. The deliverable
is one behavior-preserving identity, routing, error-name, test, and living-doc
change within the logical-lock subsystem. It introduces no public API or
durable model, requires no recovery migration or phased rollout, and remains
independently testable.

## Goals

1. Add a canonical `LockFamily` identified by the engine-local `SessionID`.
2. Represent every production logical lock owner as one exact `LockOwner`
   containing its family and scope.
3. Define exact `SessionExplicit`, `Transaction`, `Statement`, `Ddl`, and
   `Maintenance` lock scopes.
4. Make `DdlOperationID` and `MaintenanceOperationID` distinct crate-private
   types.
5. Allocate both operation-id types from one shared `AtomicU64` in
   `EngineInner`.
6. Allocate one DDL id per `SessionDdlContext` and one maintenance id per
   `ScopedTableRuntimeAccess`.
7. Remove `LockOwnerGroup` and all optional/grouped acquisition plumbing.
8. Preserve the implemented directional same-family coverage rule.
9. Preserve covering same-family queue bypass.
10. Preserve same-exact-owner reentrancy and immediate-only conversion.
11. Record distinct maintenance claims even when `SessionExplicit` covers the
    request.
12. Preserve DDL rejection whenever the same family holds a target-table
    `SessionExplicit` claim.
13. Make explicit unlock and session teardown target only the exact
    `SessionExplicit` owner.
14. Make DDL and maintenance guard cleanup target only their exact operation
    owner.
15. Ensure pending-wait cancellation and owner cleanup cannot consume claims
    from another scope in the same family.
16. Construct statement owners from the authoritative transaction family and
    transaction id rather than from `TrxID` alone.
17. Keep transaction terminal lock cleanup and
    `ReleasedTransactionLocks` ordering unchanged.
18. Rename `OperationError::LockOwnerGroupConflict` to
    `OperationError::LockFamilyConflict`.
19. Update error messages, test helpers, debug snapshots, and living
    documentation to use family/scope terminology.
20. Preserve all existing lock, DDL, maintenance, session, transaction, and
    shutdown behavior outside exact identity.

## Non-Goals

1. Do not add `LockScopeState`, an authoritative owner-side claim map, claim
   tokens, or resource incarnations.
2. Do not add an explicit-session lock cache or change the global
   `release_owner` cleanup complexity in this task.
3. Do not serialize every lock mutation or cleanup in one family.
4. Do not claim that nested DDL-guard and transaction-cleanup cancellation is
   coordinated.
5. Do not remove duplicate-waiter support or the current concurrent-release
   defenses.
6. Do not collapse exact claims into one physical family holder.
7. Do not replace the granted vector, waiter deque, DashMap sharding, waiter
   `Arc`, event notification, or cancellation-guard representation.
8. Do not add waiter, claim, resource-incarnation, or generational-slab tokens.
9. Do not change lock resources, lock modes, compatibility, coverage,
   acquisition order, FIFO promotion, queue bypass, or conversion semantics.
10. Do not allow DDL under an explicit same-session table lock.
11. Do not change public `Session`, `Transaction`, or `TableLockMode` APIs.
12. Do not add public lock diagnostics, metrics, SQL-visible inspection, or
    tracing.
13. Do not change session admission, abandonment, transaction completion,
    cleanup-worker ownership, or engine shutdown ordering.
14. Do not change transaction status, undo, redo, checkpoint, purge, recovery,
    or persistent formats.
15. Do not add deadlock detection, wait timeouts, lock escalation, downgrade,
    or weak-lock fast paths.
16. Do not add operation-id overflow handling or recycling.
17. Do not add unsafe code.
18. Do not rewrite historical implemented task documents or accepted RFC text;
    update living design documents only.
19. Do not close or move source backlog 000169 during implementation;
    `$task-resolve` owns backlog closure after implementation verification.
20. Do not implement any part of backlogs 000170 or 000171.

## Plan

### 1. Define canonical family and scope identity

Replace `LockOwner` and `LockOwnerGroup` in
`doradb-storage/src/lock/mod.rs` with crate-private identity types equivalent
to:

```rust
pub(crate) struct LockFamily(SessionID);

pub(crate) struct LockOwner {
    family: LockFamily,
    scope: LockScope,
}

pub(crate) enum LockScope {
    SessionExplicit,
    Transaction(TrxID),
    Statement {
        trx_id: TrxID,
        stmt_no: StmtNo,
    },
    Ddl(DdlOperationID),
    Maintenance(MaintenanceOperationID),
}
```

Define `DdlOperationID` and `MaintenanceOperationID` beside the lock identity
types. They are volatile engine-runtime identities, not public storage ids, so
do not add them to the public `id` module or implement serialization.

Derive `Debug`, `Clone`, `Copy`, `PartialEq`, `Eq`, `Hash`, `PartialOrd`, and
`Ord` where required by current maps, sorting, tests, and diagnostics. Keep
fields private and provide narrow crate-visible constructors/accessors:

- construct a family from `SessionID`;
- construct `SessionExplicit`, transaction, DDL, and maintenance owners;
- derive a statement owner from a transaction owner plus `StmtNo`;
- return an owner's family and scope;
- expose raw operation-id values only to crate tests and formatting as needed.

Deriving a statement owner from a non-transaction scope is an internal
contract violation. Assert with a diagnostic containing the source owner and
statement number. Production callers must not assemble a statement owner from
an independent family and transaction id.

Implement `Display` so every owner reports both family and exact scope:

```text
session_explicit(session_id=...)
transaction(session_id=...,trx_id=...)
statement(session_id=...,trx_id=...,stmt_no=...)
ddl(session_id=...,operation_id=...)
maintenance(session_id=...,operation_id=...)
```

Exact spelling may follow existing formatting conventions, but diagnostics
must distinguish every scope and include the family session id.

### 2. Allocate DDL and maintenance ids directly from `EngineInner`

Add one field to `doradb-storage/src/engine.rs`:

```rust
next_lock_operation_id: AtomicU64
```

Initialize it to one with the other engine-local identity state. Add a private
raw allocation helper that returns:

```rust
self.next_lock_operation_id
    .fetch_add(1, Ordering::Relaxed)
```

Expose narrow crate-visible methods for allocating `DdlOperationID` and
`MaintenanceOperationID`. Both methods must call the same raw helper, so their
raw values form one interleaved sequence. Do not add a
`LockOperationIdSource` wrapper, checked arithmetic, an overflow error,
poisoning, or an overflow assertion.

Allocation occurs after normal engine/session admission and before the
operation's first logical-lock request. Consuming an id before later
validation fails is valid.

Add engine-level tests that allocate DDL and maintenance ids in alternating
order and verify distinct typed identities backed by increasing raw values.
Do not add a near-`u64::MAX` test.

### 3. Derive family admission from the exact owner

Simplify `LockManager` acquisition APIs so they take only `LockOwner`.
Remove:

- `LockOwnerGroup`;
- `acquire_grouped` and `acquire_grouped_with_grant`;
- grouped parameters from ordered table and DDL helpers;
- `Option<LockOwnerGroup>` from `GrantedLock` and `Waiter`;
- owner-group fields from `LockDebugEntry`;
- grouped test acquisition helpers.

Keep `acquire` and `acquire_with_grant`, and rename
`acquire_grouped_table_locks` to a family-neutral ordered table-lock helper.
`acquire_create_table_metadata_lock` and `acquire_table_ddl_locks` receive
only the exact owner.

Refactor `ResourceState::try_acquire_immediate`,
`compatible_with_granted`, waiter promotion, and related helpers around these
rules:

```text
held.owner == requested.owner
    -> exact-owner reentrancy or immediate conversion

held.owner.family() == requested.owner.family()
    -> held mode must directionally cover the requested mode

different family
    -> normal mode compatibility
```

Rename `validate_owner_group_coverage` and
`owner_group_conflict_err` to family terminology. Validate every granted and
waiting entry from another exact owner in the family, as the current group
implementation does. Preserve the returned covered flag used for queue bypass.

Low-level tests must construct explicit family ids. Tests modeling unrelated
clients use different families; tests modeling session/transaction/statement
or explicit/maintenance interaction use the same family. Do not silently give
all synthetic owners one family, because that would change the compatibility
scenario under test.

### 4. Rename the family conflict error

Rename the fieldless variant in `doradb-storage/src/error.rs`:

```rust
LockOwnerGroupConflict
```

to:

```rust
LockFamilyConflict
```

Change its display string to `lock family conflict`. Use this variant for both
directional same-family failures and the DDL-versus-SessionExplicit purpose
rejection.

Update every current production match, test assertion, helper name, and
diagnostic attachment. Family-conflict attachments must include:

- resource;
- requested exact owner;
- family;
- held or waiting exact owner;
- held and requested modes.

Update current living-document references in `docs/lock-system.md`, including
the unresolved migration text that currently asks whether the error should be
renamed. The rename is resolved by this task. Do not edit historical
`docs/tasks/000141-explicit-table-lock-interface-and-validation.md` or other
implemented task/RFC records solely to change their historical terminology.

The error remains in the operation domain, so no public error-boundary
semantics change. Run the normal public-error audit through repository
validation and stage its output only if the deterministic audit changes.

### 5. Simplify `OwnerLockState` and transaction/statement construction

In `doradb-storage/src/lock/state.rs`, remove the `owner_group` field,
`new_grouped`, and `owner_group`. `OwnerLockState::new(owner)` becomes the only
constructor, and acquisition always passes its canonical owner to the
family-aware manager.

In `doradb-storage/src/trx/mod.rs`:

1. `TrxInner::new` constructs
   `LockOwner::transaction(session_id, trx_id)`.
2. Statement-number allocation derives the statement owner from the checked
   transaction `OwnerLockState`; it must not reconstruct from `TrxID` alone.
3. `Transaction::exec`, catalog statement staging, and streaming statement
   creation use that derived owner.
4. Transaction write-lock debug assertions reconstruct an owner only from an
   authoritative transaction attachment/session id plus transaction id, or
   reuse the stored transaction owner.
5. Carried terminal lock cleanup matches `LockScope::Transaction(trx_id)` and
   preserves the current transaction-id-bound
   `ReleasedTransactionLocks` proof.
6. Transaction/statement lock release ordering, binding handoff, and cache
   behavior remain unchanged.

Update `Statement::new` and streaming-statement construction to call only
`OwnerLockState::new`. Audit `trx/admission.rs` and all transaction test
helpers that pattern-match old enum variants or infer group state.

No `LockFamily` field is added to `TrxContext`. The authoritative session
attachment and transaction lock owner already supply runtime family identity;
keep MVCC context responsibilities unchanged.

### 6. Route explicit session locks through `SessionExplicit`

In `doradb-storage/src/session.rs`, construct the stable exact owner from the
session id for:

- `SessionPin::lock_table`;
- `SessionPin::unlock_table`;
- `SessionState::release_session_locks`;
- session close, drop, abandonment, and shutdown assertions/tests.

Explicit acquisition still takes ordered `TableMetadata(S)` followed by
`TableData(S|X)`, validates the live table, and disarms fresh guards on
success. Unlock still releases data before metadata and remains unavailable
while a transaction is active through existing session admission.

`SessionState::release_session_locks` continues to use
`LockManager::release_owner`, including its global scan, but passes only the
exact `SessionExplicit` owner. Do not add the cache from backlog 000115.

### 7. Give each DDL operation an exact owner

Change `SessionDdlContext` to retain one `LockOwner::ddl` and remove its
separate owner-group field. `SessionDdlContext::new` allocates one
`DdlOperationID` from the engine after obtaining the pinned session's id and
engine handle.

Pass that exact owner through:

- CREATE TABLE metadata-X acquisition;
- DROP TABLE metadata-X/data-X acquisition;
- CREATE INDEX metadata-X/data-X acquisition;
- DROP INDEX metadata-X/data-X acquisition;
- `FreshLockGuard` and `ScopedTableDdlLocks`;
- DDL error attachments and debug tests.

Retain `reject_table_ddl_explicit_session_lock` as a purpose-policy preflight.
The helper receives or derives the DDL family, constructs the exact
`SessionExplicit` owner for that family, and rejects when that owner has a
granted claim on either target-table resource. Check exact owner presence
rather than treating the aggregate family mode as proof of an explicit claim.

The preflight runs before partial DDL acquisition and uses
`LockFamilyConflict`. It is not a new atomic check-and-acquire primitive.
Current mutable session access and idle admission prevent a second public
same-family acquisition path; the manager remains tolerant of concurrent
cleanup and cancellation. Atomic purpose policy belongs to backlog 000171.

Dropping a DDL acquisition or progress future releases only the exact DDL
owner's fresh grants/waiters. Nested catalog transaction and statement claims
remain separate exact owners in the same family and retain their existing
terminal cleanup.

### 8. Give each scoped maintenance access an exact owner

At the beginning of `ScopedTableRuntimeAccess::acquire` and
`acquire_for_retry`, allocate one `MaintenanceOperationID`, construct the exact
maintenance owner, and use it for both ordered locks:

```text
TableMetadata(S)
-> TableData(IS)
-> resolve and retain the live table runtime
```

The existing fresh guards retain that owner. Drop order remains:

```text
release table runtime owner
-> release data claim
-> release metadata claim
```

Because maintenance no longer reuses the exact explicit owner, a request
covered by `SessionExplicit` creates a fresh maintenance entry. Generic
directional family validation admits it, and dropping the scoped access
removes the maintenance entry while preserving the explicit entry.

Apply the scope to every current `ScopedTableRuntimeAccess` caller:

- `Session::freeze_table`;
- `Session::checkpoint_table`;
- each `Session::wait_for_checkpoint_retry` recheck;
- `Session::total_row_pages`;
- `Session::cleanup_secondary_mem_indexes`.

Do not allocate or retain maintenance owners for catalog checkpoint, redo
truncation, maintenance progress waits, or other operations that do not
currently acquire these logical table locks.

`checkpoint_table_with_wait` continues to compose public checkpoint and retry
calls. Every active access attempt gets a distinct id, and no maintenance
claim survives the detached listener wait.

### 9. Preserve current waiter and cleanup behavior

Keep `FreshLockGuard`, `ScopedTableDdlLocks`, `WaiterGuard`, `Waiter`,
`LockGrant`, and `WaitOutcome` lifecycle rules unchanged except for canonical
owner storage.

In particular:

- duplicate pending acquisition by the same exact owner may still reuse a
  waiter;
- different exact operation ids never accidentally reuse one another's
  waiter;
- cancellation removes a queued waiter or a promoted unobserved grant for
  only the exact owner;
- `release(resource, owner)` and `release_owner(owner)` match exact owners;
- removing a blocker reruns the existing FIFO-prefix grant loop;
- notifications occur after resource synchronization is released;
- owner cleanup may still race a blocked acquisition and wake it with
  `LockWaiterReleased`.

Do not infer or assert the future invariant that only one family mutation or
cleanup can be active. The current manager must remain safe under its existing
concurrency model until backlog 000170 establishes cleanup ownership.

### 10. Update diagnostics and living documentation

Update internal debug snapshots and helpers so an entry's exact owner itself
provides family and purpose. Tests must be able to filter by:

- family;
- exact scope;
- operation id;
- granted or waiting state;
- queue order.

Update:

- `docs/lock-system.md` implemented baseline, current complexity terminology,
  canonical-identity status, purpose policy, migration questions, and suggested
  stages;
- `docs/transaction-system.md` maintenance text so it describes a distinct
  maintenance claim rather than preserving an existing exact-owner grant;
- `docs/data-checkpoint.md` freeze/checkpoint text with the same exact-scope
  behavior.

The living lock-system document must clearly distinguish what this task
implements from the later working design:

- canonical family and exact scope identity are implemented;
- the resource side still stores separate exact entries in vectors/deques;
- operation cleanup still uses guards rather than `LockScopeState`;
- session-explicit cleanup still scans `LockManager`;
- family mutation is not yet serialized;
- physical family aggregation and tokenized waiters remain future work.

Do not rewrite historical accepted RFC-0016 or completed task documents.

### 11. Audit the completed owner-routing graph

After implementation, search the production and test tree for:

- `LockOwner::`;
- `LockOwnerGroup`;
- `LockOwnerGroupConflict`;
- `owner_group`;
- `acquire_grouped`;
- `release_owner`;
- `SessionDdlContext`;
- `ScopedTableRuntimeAccess`.

The completed production tree must have:

1. no `LockOwnerGroup` type or grouped manager API;
2. no `LockOwnerGroupConflict` variant or current living-doc reference;
3. no transaction or statement owner constructed from `TrxID` without an
   authoritative family;
4. only exact `SessionExplicit` at explicit lock and session-cleanup paths;
5. only exact DDL owners in catalog DDL lock paths;
6. only exact maintenance owners in scoped table-runtime access;
7. no operation owner accidentally passed to session-explicit cleanup;
8. no new global cleanup scan or serialized-family assertion.

## Implementation Notes

- Implemented canonical `LockOwner { family, scope }` identity for
  session-explicit, transaction, statement, DDL, and maintenance claims.
  Removed `LockOwnerGroup`, grouped acquisition plumbing, owner-group debug
  state, and `OperationError::LockOwnerGroupConflict`; family coverage,
  covering queue bypass, exact-owner reentrancy/conversion, cancellation, and
  FIFO promotion retain their prior behavior under `LockFamilyConflict`.
- Added one shared engine-local atomic sequence for typed DDL and maintenance
  operation ids. `SessionDdlContext` retains one exact DDL owner per public
  operation, while every bounded `ScopedTableRuntimeAccess` attempt receives
  one exact maintenance owner. Explicit unlock/session cleanup, DDL guards,
  maintenance guards, and transaction/statement cleanup now release only
  their exact scopes.
- Preserved the explicit-session DDL rejection policy and verified that
  maintenance records a separate claim when a covering explicit shared or
  exclusive claim exists. Session cleanup intentionally leaves synthetic
  operation claims untouched, and operation cleanup preserves the explicit
  claim.
- Updated the lock, transaction, and checkpoint living documents to describe
  canonical identity as implemented while retaining vectors/deques, guard
  cleanup, global explicit-owner scans, concurrent family mutation, and
  unaggregated physical claims as the current baseline.
- Implementation review made two representation refinements from the plan.
  `DdlOperationID` and `MaintenanceOperationID` use the shared `impl_id!`
  boilerplate in `crate::id` while remaining crate-private and without
  serialization or bit-packing implementations. Statement scope uses the
  compact `Statement(TrxID, StmtNo)` tuple variant while preserving
  transaction-qualified exact identity and diagnostics.
- Verification passed the branch-diff style audit for 14 Rust files, the
  deterministic public-error audit with no CSV change, the 1,559-test
  workspace suite, and the 1,484-test alternate `libaio` suite. The
  explicit-lock/maintenance coexistence regression passed 100/100 stress
  iterations. Focused line coverage was 4,054/4,180 (96.99%) overall:
  1,664/1,697 (98.06%) for `doradb-storage/src/lock` and 2,390/2,483
  (96.25%) for `doradb-storage/src/session.rs`.
- Resolution archived source backlog 000169 as implemented and created no new
  follow-up because the remaining work is already tracked by backlogs 000115,
  000167, 000170, and 000171. This task declares no parent RFC; accepted
  RFC-0016 remains an unchanged related historical design.

## Impacts

### Production code

- `doradb-storage/src/lock/mod.rs`
  - `LockOwner`, `LockFamily`, `LockScope`;
  - DDL/maintenance operation-id types;
  - `LockManager` acquisition and release APIs;
  - `ResourceState`, `GrantedLock`, `Waiter`, and guards;
  - family conflict diagnostics and debug snapshots.
- `doradb-storage/src/lock/state.rs`
  - `OwnerLockState` construction, acquisition, and tests.
- `doradb-storage/src/error.rs`
  - `OperationError::LockFamilyConflict`.
- `doradb-storage/src/engine.rs`
  - shared `next_lock_operation_id: AtomicU64`;
  - typed DDL/maintenance id allocation;
  - engine identity tests.
- `doradb-storage/src/session.rs`
  - `SessionDdlContext`;
  - `ScopedTableRuntimeAccess`;
  - explicit table lock/unlock;
  - session close/drop/abandon/shutdown lock cleanup.
- `doradb-storage/src/trx/mod.rs`
  - transaction and statement owner construction;
  - runtime lock assertions;
  - terminal lock-owner matching.
- `doradb-storage/src/trx/stmt.rs`
  - statement-local `OwnerLockState`.
- `doradb-storage/src/trx/stream_stmt.rs`
  - streaming statement owner construction.
- `doradb-storage/src/trx/admission.rs`
  - tests and helpers that inspect exact transaction/statement owners.
- `doradb-storage/src/catalog/table.rs`
  - CREATE/DROP TABLE owner routing, DDL policy, and integration tests.
- `doradb-storage/src/catalog/index.rs`
  - CREATE/DROP INDEX owner routing, DDL policy, and integration tests.
- Lock-observing tests in `engine.rs`, table modules, and transaction modules.

### Documentation

- `docs/lock-system.md`
- `docs/transaction-system.md`
- `docs/data-checkpoint.md`
- `docs/public-error-audit.csv` only if the deterministic audit changes.

### Behavior and compatibility

- No public Rust API changes.
- One internal operation-error variant is renamed.
- Debug owner formatting and test snapshots become more precise.
- Lock state remains volatile and is not recovered or persisted.
- Memory layout may change mechanically because family moves inside the owner
  while the redundant owner-group field is removed. This task makes no
  performance claim; existing benchmarks and validation guard behavior.

## Test Cases

### Identity and operation-id tests

1. Construct two exact owners in one family with different scopes and verify
   they compare unequal while reporting the same family.
2. Construct equal scope payloads in different families and verify they compare
   unequal.
3. Derive statement owners from a transaction owner and verify family,
   transaction id, and monotonically increasing statement number.
4. Verify deriving a statement owner from a non-transaction scope triggers the
   documented internal assertion.
5. Verify display/debug formatting distinguishes session-explicit,
   transaction, statement, DDL, and maintenance scopes.
6. Allocate DDL, maintenance, DDL, maintenance ids from one engine and verify
   increasing raw values across both typed kinds.
7. Verify separate engine instances restart their engine-local operation-id
   sequence independently.
8. Do not add an overflow test.

### Lock-manager semantic tests

1. Port the resource compatibility and coverage matrices to canonical owners
   without changing expected results.
2. Port directional same-family tests so every held exact owner must cover the
   requested mode.
3. Verify non-covering same-family requests return
   `LockFamilyConflict` and create no waiter.
4. Verify covering same-family requests may retain current queue bypass.
5. Verify owners from different families use normal compatibility even when
   their scope payloads match.
6. Verify same-exact-owner covered reacquisition returns `Existing`.
7. Verify immediate conversion, incomparable conversion rejection, and
   upgrade-would-block behavior remain unchanged.
8. Verify two same-family exact scopes produce two separately observable
   granted entries when directional coverage permits them.
9. Release either exact owner and verify the other entry remains.
10. Release one exact owner's queued request and verify another same-family
    owner's grant or waiter remains.
11. Preserve FIFO-compatible prefix promotion, older incompatible waiter
    barriers, head cancellation, and blocker-removal promotion.
12. Preserve duplicate waiter sharing only for the same exact owner.
13. Preserve cancellation after promotion but before observation without
    leaking a grant.
14. Verify debug snapshots report exact family/scope identity and queue order
    without an owner-group field.

### Explicit-session and maintenance integration

1. Hold public explicit `Shared`, enter a paused
   `ScopedTableRuntimeAccess`, and observe separate `SessionExplicit` and
   `Maintenance(id)` metadata/data entries in the same family.
2. Complete the maintenance call and verify only `SessionExplicit` remains.
3. Repeat with public explicit `Exclusive`.
4. Verify freeze, checkpoint, row-page counting, and MemIndex cleanup release
   their maintenance owner on success and ordinary error.
5. Cancel maintenance while metadata is granted and data is waiting; verify
   both the grant and waiter for that maintenance id disappear.
6. Verify an explicit claim from the same family is unaffected by cancellation
   of a synthetic operation scope.
7. Verify a checkpoint retry drops its first maintenance scope before sleeping
   and uses a different operation id for the next bounded recheck.
8. Verify maintenance under external full-table `X` still waits and proceeds
   after transaction lock release.
9. Verify external DDL metadata `X` remains mutually exclusive with
   maintenance.
10. Seed a synthetic maintenance owner in a session family, close or abandon
    the session, and verify exact session cleanup removes
    `SessionExplicit` without consuming the operation owner. Release the
    synthetic owner explicitly after the assertion.
11. Preserve close, drop, idle-shutdown, and abandoned-active transaction
    cleanup ordering for real sessions.

### DDL integration

1. Verify CREATE TABLE exposes a distinct `Ddl(id)` metadata-X owner and
   releases it on success, validation failure, and cancellation.
2. Verify DROP TABLE, CREATE INDEX, and DROP INDEX expose distinct DDL owners
   for metadata-X/data-X and release only those owners.
3. Verify consecutive DDL calls in one session use different operation ids.
4. Verify same-session explicit shared and exclusive table locks reject DROP
   TABLE, CREATE INDEX, and DROP INDEX with `LockFamilyConflict`.
5. Seed a synthetic covering `SessionExplicit` mode and verify the DDL
   purpose check still rejects it rather than relying only on directional
   coverage.
6. Verify a rejected DDL creates no partial DDL grant or waiter.
7. Cancel DDL while one target lock is granted and the next is waiting; verify
   only that exact DDL owner disappears.
8. Preserve external waiter grant behavior after normal DDL completion.
9. Preserve nested catalog transaction/statement owners as distinct exact
   scopes in the DDL family.
10. Preserve terminal DDL poison/cleanup behavior; this task must not claim
    cancellation coordination beyond exact lock identity.

### Transaction, statement, and lifecycle regression

1. Preserve session `X` covering same-session transaction `IX`.
2. Preserve session `S` rejecting same-session transaction `IX`, now with
   `LockFamilyConflict`.
3. Preserve statement-to-transaction metadata handoff without a protection
   gap.
4. Verify transaction and statement debug owners carry the originating session
   family.
5. Preserve transaction owner-local cache hits and exact release.
6. Preserve `ReleasedTransactionLocks` proof creation and transaction-lock
   release before session completion.
7. Preserve idle-only session admission while a detached transaction is
   active or cleaning up.
8. Preserve `Session: Send + !Sync` and mutable explicit-lock/maintenance/DDL
   APIs.
9. Preserve engine shutdown and session-registry lock cleanup tests.
10. Run all existing duplicate waiter, FIFO, conversion, cancellation, DDL,
    table maintenance, transaction, and session tests without weakened
    assertions.

### Validation

Run focused tests while iterating, then complete:

```bash
rtk cargo fmt --all -- --check
rtk cargo clippy --workspace --all-targets -- -D warnings
tools/style_audit.rs
rtk cargo nextest run --workspace
rtk cargo nextest run -p doradb-storage --no-default-features --features libaio
```

For newly changed concurrent lock/DDL/maintenance tests, run focused stress
passes without retries:

```bash
rtk cargo nextest run -p doradb-storage --stress-count 100 <test-filter>
```

Use semantic hooks, channels, events, or existing lock-debug predicates to
arrange races. Do not add sleeps to make a predicate true. Timeouts remain hang
watchdogs only.

Run focused coverage for the changed lock and session paths:

```bash
tools/coverage_focus.rs \
  --path doradb-storage/src/lock \
  --path doradb-storage/src/session.rs
```

Treat 80% focused line coverage as the default review bar. Explain any lower
definition-heavy result with covered consumer paths.

## Open Questions

None for implementation.

Deferred follow-ups remain explicit:

1. Backlog 000115 owns the explicit-session cache and targeted resource-key
   cleanup optimization.
2. Backlog 000170 owns stable session-operation cancellation and cleanup
   coordination, including the nested DDL transaction handoff.
3. Backlog 000171 owns the exact-family RFC, authoritative `LockScopeState`,
   serialized family mutation, physical family aggregation, tokens, and
   optimized resource representation.
4. Backlog 000167 continues to own logical-lock deadlock handling.
5. Source backlog 000169 was closed as implemented during task resolution
   after code, tests, review, documentation, and behavioral verification
   completed; the downstream prerequisite chain remains unchanged.
