---
id: 000258
title: Establish Linear Lock Family Authority and Owner-Side Indexes
status: proposal  # proposal | implemented | superseded
created: 2026-08-06
github_issue: 948
---

# Task: Establish Linear Lock Family Authority and Owner-Side Indexes

## Summary

Implement RFC-0027 Phase 1 by replacing independent per-owner lock caches with
one linear, session-family authority and authoritative scope-local cleanup
indexes.

Allocate one boxed `FamilyLockAuthority` for each session. The box contains the
family/resource aggregate and the session-explicit `LockScopeState`; ownership
of that same box moves through the idle session, foreground operation,
transaction, prepared/terminal transaction, and accepted DDL or maintenance
carriers. A transaction or operation carrier adds its own `curr_scope`.
Statement callbacks and caller-driven streams retain their existing RAII
carriers, replace only their `OwnerLockState` field with a statement
`curr_scope`, and reach the family authority through their checked-out
transaction core.

For each family/resource, store the common single exact claim inline and
expand at most once to four fixed typed slots. Use a session-local,
checked `ClaimNo` to connect the family/resource entry with the
scope/resource entry and to reject stale mutation. Do not add a nested claim
hash map or duplicate a full `LockScope` in expanded slots.

Keep the current `LockManager` storage and physical behavior throughout this
phase. Every accepted logical claim continues to have one exact-owner manager
grant; the manager retains its granted `Vec`, waiter
`VecDeque<Arc<Waiter>>`, FIFO policy, duplicate defenses, and existing
cancellation guards. Phase 1 changes the authoritative callers and cleanup
paths, not the manager's physical representation. Tokenized waiter storage is
RFC-0027 Phase 2, and one physical holder per family/resource is Phase 3.

## Context

Issue Labels:

- type:task
- priority:high
- codex

Parent RFC:

- `docs/rfcs/0027-session-family-logical-lock-system-redesign.md`

RFC Relationship:

- Phase 1: Linear Family Authority And Owner-Side Indexes.

Prerequisites:

- `docs/tasks/000242-enforce-terminal-transaction-lock-release-ordering.md`
- `docs/tasks/000243-separate-session-operation-lock-scopes.md`
- `docs/tasks/000246-session-operation-coordinator-foundation.md`
- `docs/tasks/000247-statement-public-transaction-cancellation-ownership.md`
- `docs/tasks/000249-runtime-owned-table-ddl.md`

Related Designs:

- `docs/architecture.md`
- `docs/transaction-system.md`
- `docs/lock-system.md`
- `docs/rfcs/0016-logical-lock-manager.md`
- `docs/rfcs/0025-session-coordinated-cancellation-cleanup-ownership.md`
- `docs/rfcs/0026-engine-owned-mandatory-background-runtime.md`

Related Benchmark:

- `docs/tasks/000257-doradb-bench-lock-table-workload.md`

Related Backlogs:

- `docs/backlogs/000115-explicit-session-lock-cache.md`
- `docs/backlogs/000171-exact-family-lock-system-redesign.md`
- `docs/backlogs/closed/000169-separate-session-operation-lock-scopes.md`
- `docs/backlogs/closed/000170-session-coordinated-cancellation-cleanup.md`

Related Process:

- `docs/process/coding-guidance.md`
- `docs/process/issue-tracking.md`
- `docs/process/lint.md`
- `docs/process/unit-test.md`

The current owner side has one independent state for each exact owner:

```rust
struct OwnerLockState {
    owner: LockOwner,
    held: FastHashMap<LockResource, LockMode>,
}
```

This cache makes repeated acquisition by the same exact owner local, but it
cannot answer which other scopes in the same session family claim a resource.
It also does not prove that all session-family mutation and cleanup pass
through one authority.

The current manager remains the only complete cross-scope record:

```rust
struct ResourceState {
    granted: Vec<GrantedLock>,
    waiters: VecDeque<Arc<Waiter>>,
}
```

Session teardown consequently calls `LockManager::release_owner()` and scans
manager resources to find session-explicit grants. Transaction, statement,
DDL, and maintenance cleanup each own separate exact caches, and agreement
between those caches is not represented as a session-family invariant.

Phase 1 introduces two authoritative owner-side lookup directions:

```text
family/resource index:
    LockResource -> every live exact scope claim in this family

exact-scope cleanup index:
    LockOwner -> every LockResource claimed by this scope
```

For a transaction claim, both entries contain the same
`(ClaimNo, LockMode)`:

```text
family.resources[resource].transaction slot
    = (trx_id, claim_no, mode)

transaction curr_scope.claims[resource]
    = (claim_no, mode)
```

The family index supplies bounded same-family lookup, coverage validation, and
aggregate-mode calculation. The scope index makes close proportional to the
number of claims in that exact scope. Neither index is a repair cache; accepted
claims must appear in both or the process has violated an internal invariant.

The session-explicit scope is stored beside the family root because it has the
same session lifetime and survives individual operations and transactions.
Transaction and operation scopes remain fields of their matching lifecycle
carriers so terminal processing consumes exactly the state it must close.
`curr_scope` consistently means "the exact scope owned by this carrier," not
the globally active scope.

The existing `StmtState` is not merely a lock wrapper. It owns
`StmtEffects`, `StmtDropAction`, and `SessionOperationCheckout` across callback
await points and implements public-cancellation versus private-must-complete
Drop policy. `StreamStmtState` similarly retains a checkout for a
caller-driven stream. This task preserves both carriers and replaces only
their statement lock state. Their checkout transitively owns `TrxInner` and
therefore the transaction's family authority.

Optimized type-size inspection of the current code reports
`OwnerLockState` at 96 bytes, including a 64-byte hash-map header;
`LockOwner` at 32 bytes; `LockScope` at 24 bytes; `LockResource` at 16 bytes;
and `LockMode` at one byte. The selected layout avoids a second hash map under
each family/resource and avoids allocation for the common single-claim case.
The implementation must remeasure its final types rather than treating these
numbers as ABI requirements.

The strict RFC complexity gate passes because this task is the bounded first
phase of accepted RFC-0027. It establishes local authority and indexes while
deliberately retaining the manager's current exact grants and waiter
representation. Replacing either manager collection in this task would cross
the accepted phase boundary.

The rejected alternatives are:

1. A per-family actor, mutex-protected sidecar, lease counter, or repair
   protocol. Session execution is linear, so owned values and exclusive
   borrows are sufficient.
2. A nested hash map of exact claims under every family/resource. The live
   topology permits at most one claim for each of four scope classes.
3. Four always-inline maximum-size slots. Most family/resources have one
   claim, so the selected representation keeps that claim inline and pays one
   boxed expansion only when a second scope appears.
4. Repeating `LockScope` in every expanded claim. Fixed field position supplies
   the scope class; only exact operation, transaction, and statement ids need
   storage.
5. Deleting `StmtState` and replacing it with `LockScopeState`. That would
   discard the existing statement effect, checkout, and Drop-settlement owner
   and recreate equivalent RAII plumbing elsewhere.
6. Converting the manager to physical family holders now. That is RFC-0027
   Phase 3 and depends on Phase 2's token-exact provisional-grant lifecycle.

## Goals

1. Replace production `OwnerLockState` use with authoritative
   `FamilyLockState` and `LockScopeState`.
2. Allocate exactly one boxed `FamilyLockAuthority` per engine-local session.
3. Keep one linear owner of that box across idle, operation, transaction,
   statement, prepared, precommit, cleanup, and terminal boundaries.
4. Keep the session-explicit `LockScopeState` beside the family root across
   individual execution lifetimes.
5. Name the family root's persistent session-explicit `LockScopeState`
   `session_scope`; name shorter-lived carrier-owned scope fields `curr_scope`
   and assert their expected `LockScope` variant at construction and transfer
   boundaries.
6. Index every accepted exact claim by both family/resource and exact
   scope/resource.
7. Bound family/resource exact-claim lookup to the four scope classes without
   per-resource claim hashing.
8. Store one claim inline without allocation and expand once to fixed,
   scope-typed slots when another scope claims the same resource.
9. Retain an expanded slot allocation until the entire local family/resource
   entry disappears.
10. Use a checked, session-local `ClaimNo` as accepted logical-claim
    identity.
11. Burn claim numbers reserved by failed, rejected, or cancelled fresh
    attempts; never recycle them within one session lifetime.
12. Retain a claim number through mode conversion and allocate a new number
    after release and reacquisition.
13. Require each accepted-claim mutation to target its exact scope and claim
    number. Do not traverse the dual indexes merely to revalidate their
    agreement.
14. Make repeated covered exact-owner acquisition entirely owner-local.
15. Preserve current directional same-family coverage and exact-owner
    conversion semantics.
16. Continue creating one exact manager grant for every fresh accepted claim,
    including a family-covered claim in another scope.
17. Continue releasing one exact manager grant for every logical claim removed
    during Phase 1.
18. Make explicit unlock and every normal scope close use the scope-local index
    instead of manager-wide owner scans.
19. Preserve the required close order: statement, transaction, operation, then
    session-explicit.
20. Carry the family authority through public and private transaction terminal
    states and return it only after transaction scope close.
21. Preserve `ReleasedTransactionLocks` as the owning, single-use proof at the
    session terminal boundary.
22. Preserve statement callback, stream, admission, cancellation, and
    statement-to-transaction handoff behavior.
23. Preserve prepared/accepted DDL and maintenance ownership through
    preparation failures, mandatory execution, nested private transactions,
    and fatal retention.
24. Preserve the narrow `PreparedCatalogWriteAuthority` migration bridge while
    changing it to borrow the new operation scope.
25. Add local, non-atomic instrumentation that proves covered-path and
    targeted-close behavior.
26. Add deterministic invariant and model tests before any physical-manager
    cutover.
27. Preserve public APIs, logical-lock semantics, transaction semantics,
    storage formats, and recovery behavior.

## Non-Goals

1. Do not replace exact manager grants with one physical family holder.
2. Do not replace `Vec<GrantedLock>`, `VecDeque<Arc<Waiter>>`, `Arc<Waiter>`,
   or current waiter cancellation identity.
3. Do not add the Phase 2 generational waiter slab, intrusive queue,
   `WaitNodeID`, provisional/released node phases, or pending-token transfer.
4. Do not add `ClaimNo` to current `GrantedLock` or `Waiter` records.
5. Do not remove current duplicate-waiter, concurrent-release, or migration
   defenses.
6. Do not remove `LockManager::release_owner()` from the codebase; remove it
   only from normal production lifecycle paths and retain it for migration or
   tests.
7. Do not remove `PreparedCatalogWriteAuthority` or its narrow prepared-catalog
   bypass.
8. Do not add blocking conversion, lock downgrades as a public operation,
   `SIX`, escalation, weak locks, timeout policy, or deadlock detection.
9. Do not permit multiple active execution lineages or parallel lock mutation
   inside one session family.
10. Do not introduce a per-family actor, mutex, atomic lease, parallel closer,
    repair worker, or independently idempotent cleanup protocol.
11. Do not add a nested claim hash map or allocate for the first claim on a
    family/resource.
12. Do not collapse an expanded claim set back to inline while its
    family/resource entry remains live.
13. Do not expose claim numbers, owner-side snapshots, or family statistics as
    public or SQL-visible APIs.
14. Do not add global atomics to covered acquisition or scope-close paths.
15. Do not redesign statement effects, public statement cancellation,
    caller-driven stream ownership, transaction checkouts, or stable
    session-operation entries beyond the authority fields required here.
16. Do not change lock resources, modes, compatibility, queue ordering,
    same-family bypass, or immediate-only conversion policy.
17. Do not change MVCC timestamps, undo/redo formats, commit ordering, purge,
    checkpoint, I/O backends, or durable storage formats.
18. Do not add unsafe code or external dependencies for claim storage.
19. Do not complete RFC-0027 Phase 2 or Phase 3 documentation during
    implementation; `$task-resolve` updates only the Phase 1 outcome.
20. Do not close or archive related backlogs during implementation;
    `$task-resolve` owns final documentation synchronization.

## Plan

### 1. Define the compact authoritative owner-side state

Replace `OwnerLockState` in `doradb-storage/src/lock/state.rs` with
crate-private types equivalent to:

```rust
struct FamilyLockAuthority {
    family: FamilyLockState,
    // Always LockScope::SessionExplicit for this family.
    session_scope: LockScopeState,
}

struct FamilyLockState {
    family: LockFamily,
    next_claim_no: u64,
    resources: FastHashMap<LockResource, LocalFamilyResourceState>,
    stats: FamilyLockStats,
}

struct LockScopeState {
    owner: LockOwner,
    claims: FastHashMap<LockResource, ScopeClaim>,
}

struct ScopeClaim {
    claim_no: ClaimNo,
    mode: LockMode,
}

struct LocalFamilyResourceState {
    claims: FamilyClaims,
    claim_mask: ModeMask,
    covering_mode: LockMode,
}
```

`FamilyLockAuthority::new(session_id)` creates the family root and an empty
`SessionExplicit` `session_scope`. `LockScopeState::new(owner)` asserts that its
owner belongs to the same family as the authority before the first mutation.
Do not implement `Clone` for any authority or mutable scope state.

Use the approved claim representation:

```rust
struct InlineFamilyClaim {
    scope: LockScope,
    claim_no: ClaimNo,
    mode: LockMode,
}

struct FamilyClaim<I> {
    id: I,
    claim_no: ClaimNo,
    mode: LockMode,
}

struct FamilyClaimSlots {
    session_explicit: Option<FamilyClaim<()>>,
    operation: Option<FamilyClaim<OperationID>>,
    transaction: Option<FamilyClaim<TrxID>>,
    statement: Option<FamilyClaim<(TrxID, StmtNo)>>,
}

enum FamilyClaims {
    Inline(InlineFamilyClaim),
    Expanded(Box<FamilyClaimSlots>),
}
```

The inline claim retains `LockScope` because no field position supplies its
scope. Expanded field position supplies the scope class, while generic `id`
retains only the exact identity needed by that class. The
`SessionExplicit` id is `()` and therefore semantically empty. The statement
id is the canonical `(TrxID, StmtNo)` pair.

On insertion of a second scope, allocate one `FamilyClaimSlots`, move the
inline claim into its typed slot, install the new claim, and increment the
expansion counter. Once expanded, clear and reuse fixed slots. Do not shrink to
`Inline`; remove the whole `LocalFamilyResourceState` when its final slot is
cleared.

Define `ClaimNo` in `id.rs` through the standard `impl_id!` macro as an opaque
`u64` newtype; zero is a valid representation. Initialize the session-local
allocation sequence at one, use checked arithmetic, reserve before any fresh
attempt can wait or fail, and treat exhaustion as a fatal internal invariant.
The implementation may tune field order after `size_of` and optimized
`-Zprint-type-sizes` measurement, but it must preserve the approved logical
shape, safe Rust, bounded lookup, and allocation rules.

### 2. Encode dual-index and claim-identity invariants

Centralize family/scope insertion, conversion, removal, and close in
`FamilyLockState` methods that receive the exact `curr_scope`. Callers must not
mutate either map directly.

For every accepted claim:

1. `curr_scope.claims[resource]` exists.
2. The matching family/resource slot exists.
3. Both entries contain the same `ClaimNo` and `LockMode`.
4. The slot's scope class and exact id match `curr_scope.owner`.
5. `claim_mask` represents every occupied mode.
6. `covering_mode` is an actual occupied claim mode that covers every other
   occupied mode; never manufacture a synthetic join.

Provide one compact claim token for guarded local rollback and exact removal:

```rust
struct ClaimToken {
    resource: LockResource,
    owner: LockOwner,
    claim_no: ClaimNo,
}
```

Validate the token's family, exact scope, and claim number against
`curr_scope` before mutating either index. A token retained across unlock and
reacquire must fail an assertion before touching the newer claim. Stale
accepted identity is not a recoverable concurrency outcome because exclusive
family authority forbids the concurrent mutation that would make it ordinary.
Do not repeat family/scope structural agreement checks on the release hot
path; centralized mutation and exclusive family authority preserve that
agreement, while the actual typed-slot removal still identifies the exact
scope and claim number.

Implement test-only snapshots and agreement assertions in owner-side code.
They may sort copies for deterministic comparisons but must not add a
production claim index or change the hot representation.

### 3. Route acquisition and conversion through family authority

Replace direct `OwnerLockState::acquire` and direct manager acquisition at
session, transaction, statement, DDL, maintenance, and admission call sites
with a family operation that receives:

- `&mut FamilyLockState`;
- `&mut LockScopeState`;
- `&LockManager`;
- `LockResource`;
- requested `LockMode`.

Use this sequence:

1. Assert resource/mode validity and family/scope agreement.
2. Probe `curr_scope.claims`.
3. If the exact claim already covers the request, return `LockGrant::Existing`
   locally and do not enter `LockManager`.
4. If the exact claim requires immediate conversion, validate family coverage,
   perform the current exact-owner manager conversion, then update both local
   entries while retaining `ClaimNo`. Preserve current
   `LockUpgradeWouldBlock` behavior and leave both indexes unchanged on
   rejection.
5. For an exact miss, reserve and thereby burn a new `ClaimNo`.
6. Inspect at most four family slots and apply the current directional
   same-family rule. Reject a non-covering same-family request with the
   existing operation error before creating local state.
7. Even when another scope's family claim covers the request, call the current
   manager in Phase 1 so it creates the required exact-owner mirror.
8. Await using the existing manager waiter cancellation discipline. The unique
   family authority remains owned by the enclosing future or lifecycle carrier
   across the await.
9. Require `LockGrant::Fresh` for an exact local miss. Treat disagreement as an
   invariant failure rather than accepting an unindexed manager entry.
10. Synchronously insert the claim into both owner-side indexes and recompute
    aggregate fields. Treat publication as infallible, and do not introduce an
    await, callback, hook, or recoverable error between the fresh manager grant
    and owner-side insertion.

Do not retain rejected or pending attempts in accepted owner-side maps.
Dropping the acquisition future while it is pending must first settle the
manager waiter guard; only then may the checkout, operation, or family
authority move to cleanup. Once a fresh grant is observed, the same poll
publishes it into owner-side state without another cancellation point.

### 4. Make multi-resource rollback owner-side and freshness-exact

Use a fixed-capacity owner-side guard around table metadata/data acquisition.
It records only newly accepted `ClaimToken`s and, while armed, removes their
two local records and their exact manager mirrors in reverse acquisition
order.

Apply it to:

- `Session::lock_table`;
- `Transaction::lock_table`;
- table admission;
- statement-to-transaction metadata handoff;
- prepared DDL metadata/data sets;
- prepared maintenance metadata/data sets.

Preserve metadata-before-data ordering and catalog validation after lock
acquisition. A validation failure must remove only claims reported `Fresh`;
it must not release a covering preexisting exact claim or another scope's
claim. Use inline/fixed guard storage for known two-resource paths rather than
allocating a vector.

### 5. Implement exact release, selective unlock, and targeted close

Release one accepted claim by validating the `ClaimToken` against the scope
entry, computing the remaining mask and aggregate mode from the trusted family
slots, releasing the current exact manager grant, and removing both owner-side
records. Assert the manager operation removed exactly one Phase 1 mirror and
that required map removals find their target, but do not compare removed
records with previously read structural copies. Remove the family/resource
map entry when no claim remains.

In Phase 1, issue the exact manager release even when the derived physical
family mode would remain unchanged. Count this case locally so Phase 3 has
evidence for how many releases can become family-local, but do not skip the
manager mirror yet.

Implement:

- selective session-explicit table unlock by taking metadata/data resources
  from the root `curr_scope`;
- exact statement close;
- exact transaction close;
- exact DDL/maintenance operation close;
- final session-explicit close.

Scope close must iterate only `curr_scope.claims`, validate every matching
family slot, and release exactly those resources. It must not scan
`LockManager` or unrelated family resources. Lifecycle carriers with multiple
settlement paths may store `curr_scope` in an `Option` and take it once.

Retain `LockManager::release_owner()` and its tests as a migration or
diagnostic defense, but remove its use from normal session close, abandonment,
terminal transaction, statement, DDL, maintenance, and shutdown paths.

### 6. Move one family root through session and operation lifecycles

Add one `Box<FamilyLockAuthority>` to `SessionLifecycle` at session creation.
An open idle session owns exactly one root. Starting an effectful operation
takes that same box; no operation may allocate a replacement root or clone
authority.

For an explicit-lock operation, the operation pin mutates the root's
session-explicit `session_scope` and returns the root when the foreground call
finishes. Explicit lock state therefore persists across operation ids.

For DDL or maintenance, create an operation `LockScopeState` named
`curr_scope` and pair it with the taken root in the prepared/accepted
operation-lock carrier. Use an armed return/close guard during preparation so
every error or cancellation either:

- closes the operation scope and returns the root to an open session; or
- closes operation then session-explicit scope when disposition requires final
  teardown.

Only publish the session idle or closed after recovering the root and proving
that all shorter-lived scopes are closed. If close or abandonment occurs while
an operation is active, let the authoritative operation/terminal path recover
the box; do not run a parallel manager scan.

Final session close or abandonment consumes the root, closes its
session-explicit `session_scope`, and asserts `FamilyLockState.resources` is
empty. An ordinary return to an open idle session asserts that no operation,
transaction, or statement slot remains, while session-explicit slots may
remain.

### 7. Carry authority through public and private transactions

Replace `TrxInner::lock_state: Option<OwnerLockState>` and the corresponding
prepared/precommit fields with a transaction carrier equivalent to:

```rust
struct TransactionLockState {
    authority: Box<FamilyLockAuthority>,
    // Always LockScope::Transaction(current_trx_id).
    curr_scope: LockScopeState,
}
```

The enclosing fields may remain `Option<TransactionLockState>` where current
ownership transitions already take the lock state. Do not put the family root
in a second independent transaction field.

Carry the complete `TransactionLockState` through `TrxInner`, `PreparedTrx`,
`PrecommitTrx`, cleanup claims, and fatal-retention paths. Transaction
preparation may move the carrier, but no state may reconstruct it from
`LockOwner`.

Transaction terminal release must:

1. clear table bindings in the existing order;
2. close transaction `curr_scope`;
3. assert no transaction slots remain;
4. extract the same boxed family root; and
5. mint an owning `ReleasedTransactionLocks` proof containing the transaction
   id and returned root.

Change `ReleasedTransactionLocks` from an id-only marker into a single-use
owning proof. Public terminal attachment consumes it before making the session
idle or closed. An open public session reinstalls the root in
`SessionLifecycle`; close or abandonment drains session-explicit claims
instead. Preserve the existing rule that no transaction completion becomes
session-visible before transaction locks are released.

For a nested private DDL/maintenance transaction, temporarily move the root
from the outer operation-lock carrier into the private
`TransactionLockState`; the outer operation `curr_scope` remains owned and
immutable in its accepted carrier. On private terminal completion,
`ReleasedTransactionLocks` returns the root through the existing stable
`SessionOperationEntry`, and the outer accepted operation reclaims that exact
box before continuing or closing its operation scope. Extend the stable entry
only with the minimum owning return slot needed for this transition; do not add
a family mutex, lease, or independently mutable coordinator.

Change private transaction begin/reclaim APIs to require mutable accepted
operation authority. Assert that an outer operation cannot acquire or close
claims while its root is installed in a private transaction. A fatal or
cancelled private path must park the root with the stable entry/retention owner
until the existing mandatory cleanup path can reclaim or retain it; never
close the outer scope concurrently.

### 8. Migrate statements, streams, admission, and handoff

Keep the existing `StmtState` RAII wrapper. Replace:

```rust
stmt_locks: OwnerLockState
```

with:

```rust
curr_scope: Option<LockScopeState>
```

The option supports exactly-once close from ordinary return, fatal rollback,
mandatory panic settlement, or armed Drop. `Statement<'_>` borrows the active
`LockScopeState` directly; it does not receive its own family-authority field.
Its `SessionOperationCheckout` owns `TrxInner`, whose
`TransactionLockState` anchors the authority across callback awaits.

Make the analogous change to `StreamStmtState`. Cursor/root state must still
drop before the checkout returns, and stream exhaustion, explicit close,
operation error, or caller Drop must close its statement scope exactly once.

Update `trx/admission.rs` and statement helpers to take disjoint mutable
borrows of:

- transaction authority/family state;
- transaction `curr_scope`;
- statement `curr_scope`.

Use the transaction carrier's narrow `family_mut()` accessor when a path needs
only family state. Reserve `parts()` for paths that directly require both
family state and the transaction `curr_scope`.

For statement-to-transaction metadata handoff, first install or validate the
destination transaction claim, including both owner-side indexes and its exact
manager mirror, and only then remove the source statement claim. Rollback
removes only a fresh destination claim. Preserve table binding and effect
ordering so no statement effect can outlive required transaction protection.

Preserve public statement cancellation ordering: pending acquisition guard,
statement effect folding, statement-scope close, checkout cancellation return,
then whole-transaction cleanup. Preserve private must-complete assertions and
all current public `Statement` and stream APIs.

### 9. Migrate DDL, maintenance, and prepared catalog authority

Replace `PreparedDdlLocks` and `PreparedMaintenanceLocks` owner caches with
operation-lock carriers containing the family root and operation
`curr_scope`. Preserve current field/drop ordering so prepared lock cleanup
precedes publication of the foreground or mandatory terminal edge.

Preparation errors close only the fresh operation claims already accepted.
Accepted scopes retain operation state until nested private transactions and
mandatory execution settle. Normal accepted completion closes operation
`curr_scope`, verifies that the root has been reclaimed, then returns or drains
the root before `MandatoryOperationGuard::finish` publishes terminal state.

Use authoritative family state to validate the explicit-session DDL exclusion.
During migration, retain manager-side exact mirrors and agreement assertions;
do not let a manager scan become an alternative source of owner truth.
Maintenance must still record its own operation claim when a stronger
session-explicit claim covers the request.

Retain `PreparedCatalogWriteAuthority`, but change it to borrow the prepared
operation `LockScopeState` instead of `OwnerLockState`. Preserve its narrow
catalog-write validation/bypass behavior through this phase. Removing that
bridge and making nested catalog accesses ordinary exact claims remains
RFC-0027 Phase 3.

Audit every accepted DDL/maintenance panic, cancellation, failed-retention,
session-close, abandonment, and shutdown branch. Each branch must identify the
single carrier that owns both the root or its stable return right and the
scope that must eventually close.

### 10. Keep `LockManager` representation unchanged

Do not change the persistent production fields of `LockManager` resource
state, `GrantedLock`, or `Waiter` for this phase. Preserve:

- one `GrantedLock` per exact `LockOwner`;
- `Vec<GrantedLock>`;
- `VecDeque<Arc<Waiter>>`;
- current completion/event behavior;
- maximal compatible FIFO-prefix promotion;
- covering same-family bypass;
- immediate-only conversion;
- current duplicate and cancellation defenses.

Family owner-side code may wrap existing manager methods and add test-only
agreement assertions and snapshots. It must not store
`ClaimNo` in the manager, add a physical family holder, or treat manager
state as the cleanup index.

Run existing manager FIFO, conversion, cancellation, duplicate-waiter,
same-family, and cleanup tests unchanged or with naming-only adaptation. A
family-covered new claim in another scope must still enter the manager and
produce a distinct exact grant during Phase 1.

### 11. Add local instrumentation, layout evidence, and living documentation

Add plain counters owned by `FamilyLockState`; do not use global atomics or
shared-manager counters on local paths. At minimum record:

- repeated exact covered acquisitions;
- family-covered new exact claims that still entered the Phase 1 manager;
- manager acquires and releases;
- inline-to-expanded transitions;
- accepted fresh claims and conversions;
- scopes closed and claims visited;
- releases whose hypothetical physical family mode would remain unchanged.

Expose a test-only snapshot. Production diagnostics need not expose a new API
in this phase.

Add compile-time/runtime size tests for the selected claim layout and record
optimized `-Zprint-type-sizes` evidence during implementation. Compare the
existing `doradb-bench run lock-table` session and transaction retained/paired
workloads before and after the change. Phase 1 does not promise the final
physical-family speedup, but repeated exact-owner acquisition must remain
local and successful statement/transaction paths must not gain a new mutex,
atomic, registry lookup, or per-acquisition allocation.

Update `docs/lock-system.md` and transaction/lifecycle living documentation to
describe the implemented owner-side authority, exact manager mirroring, scope
close order, and remaining Phase 2/3 boundaries. Do not rewrite historical
implemented task documents.

### 12. Control ownership, cancellation, and performance risks

Treat loss or duplication of the boxed family root as the highest-risk failure.
Every take must have one visible destination, and every ordinary return,
terminal proof, stable-entry park, or fatal-retention path must assert the
source is empty before publication. Add pointer-identity test hooks where they
make the one-allocation invariant observable without exposing a production
API.

Keep the manager wait and owner-side publication boundary explicit.
`WaiterGuard` handles cancellation while a manager request is pending. Once
the manager returns a fresh grant, publish the owner-side claim synchronously
without an await, callback, test hook, or recoverable error. Treat invariant
panics as fatal rather than defining partial rollback behavior for local
insertion. Cancellation tests may pause before the manager call and while
queued, but no test-only pause may split fresh-grant observation from
owner-side publication.

Treat dual-index mismatch as an assertion-bearing internal bug. Do not add a
fallback scan, silent repair, or "best effort" close, because any such path
would hide the authority proof this phase exists to establish.

Audit successful statement, stream-item, transaction lock, explicit-lock, DDL,
and maintenance paths for new allocation or synchronization. The family root
is allocated once per session; an expanded family/resource is allocated once;
the common inline claim, repeated covered acquisition, and exact-scope close
must add no actor message, mutex, atomic, registry lookup, or per-claim heap
allocation beyond existing hash-map growth.

If optimized size or workload evidence shows the approved field order is
unnecessarily large, reorder fields or introduce private type aliases without
changing the generic `FamilyClaim<I>` contract, inline/expanded behavior, or
typed slot identities. Any proposal to change those structural decisions, add
unsafe packing, or modify manager storage requires renewed design review
rather than an implementation-local deviation.

## Implementation Notes

## Impacts

- `doradb-storage/src/id.rs`: defines the opaque, session-local `ClaimNo`
  through the standard id macro.
- `doradb-storage/src/lock/claim.rs`: owns compact inline/expanded claim
  storage, typed slots, aggregate mode bookkeeping, claim tokens, and focused
  representation tests.
- `doradb-storage/src/lock/state.rs`: replaces `OwnerLockState` with the family
  root and scope index; owns claim-number allocation, manager coordination,
  close algorithms, instrumentation, and lifecycle/model tests.
- `doradb-storage/src/lock/mod.rs`: reexports the new owner-side types and
  preserves exact grants/waiters while adapting APIs, assertions, diagnostics,
  and tests. `release_owner()` leaves normal lifecycle use.
- `doradb-storage/src/session.rs`: initializes and transfers the boxed root;
  migrates explicit locks, prepared/accepted DDL, prepared/accepted
  maintenance, operation pins, mandatory guards, session close, abandonment,
  and teardown.
- `doradb-storage/src/trx/mod.rs`: adds `TransactionLockState`, carries it
  through transaction states, makes `ReleasedTransactionLocks` own the
  returned root, and extends stable private-transaction return plumbing.
- `doradb-storage/src/trx/stmt.rs`: retains `StmtState` and `Statement`, replaces
  only statement owner-lock storage, and updates close/cancellation/handoff
  access.
- `doradb-storage/src/trx/stream_stmt.rs`: migrates caller-driven stream
  statement scope and Drop close behavior.
- `doradb-storage/src/trx/admission.rs`: makes admission and rollback update
  both indexes and preserves freshness-exact handoff.
- `doradb-storage/src/trx/sys.rs`: carries owning transaction lock proof through
  worker and terminal paths without changing transaction semantics.
- `doradb-storage/src/catalog/table.rs`,
  `doradb-storage/src/catalog/index.rs`, checkpoint, retention, and maintenance
  call sites: adapt prepared operation and nested private-transaction
  authority transfer.
- `doradb-bench`: no required CLI or result-format change; use the existing
  lock-table workloads for before/after evidence.
- Public storage APIs, lock modes/resources, persistent formats, recovery,
  MVCC, and workspace dependencies remain unchanged.

## Test Cases

1. `FamilyLockAuthority::new` creates the correct family, a
   `SessionExplicit` root `session_scope`, claim number one, empty resources,
   and zeroed local counters.
2. The first family/resource claim stays inline and performs no claim-set heap
   allocation.
3. A second scope expands exactly once, moves the original claim to the
   correct typed slot, and installs the second claim.
4. All four scope slots can coexist and retain exact operation, transaction,
   and statement ids.
5. Clearing an expanded set to one claim does not collapse it; clearing the
   final claim removes the complete family/resource entry.
6. Generic `FamilyClaim<()>`, operation, transaction, and statement layouts
   meet measured size expectations without unsafe packing.
7. Mode masks and aggregate modes agree with a simple four-slot reference
   calculation for every valid mode chain.
8. Incomparable or non-covering same-family requests preserve the existing
   `LockFamilyConflict` behavior and leave both indexes unchanged.
9. Repeated covered acquisition by the same exact scope returns `Existing`,
   allocates no claim number, and makes no manager call.
10. Exact-scope conversion retains `ClaimNo`, updates both indexes and
    the manager mirror, and rolls back cleanly on `LockUpgradeWouldBlock`.
11. A fresh cross-scope request covered by the family still creates a distinct
    exact manager grant in Phase 1.
12. Failed, rejected, and cancelled fresh attempts burn their reserved claim
    number and leave no accepted local record.
13. Claim numbers increase monotonically across mixed scopes; checked
    exhaustion fails as an internal invariant and never wraps.
14. Unlock followed by reacquire receives a new claim number; an old token
    asserts before mutating the new claim.
15. Every accepted insertion, conversion, selective release, and scope close
    preserves exact family/scope index agreement.
16. Immediate fresh-grant cancellation or unwind releases the manager grant
    and leaves both owner indexes unchanged.
17. Metadata/data catalog-validation failure rolls back only fresh claims in
    reverse order and preserves previously covered exact claims.
18. Selective session table unlock removes only that table's metadata/data
    claims and retains unrelated session-explicit claims.
19. Session close and abandonment drain session-explicit claims through the
    root scope without calling production `release_owner()`.
20. A session-explicit claim survives public transaction commit/rollback and
    DDL/maintenance operation completion, then remains selectively unlockable.
21. Public transaction begin takes the exact idle root; terminal completion
    closes transaction scope before returning that same allocation to an open
    idle session.
22. Prepared, precommit, ordered commit, rollback, failed-precommit, abandoned
    cleanup, and fatal-retention paths never duplicate or lose the family root.
23. `ReleasedTransactionLocks` cannot be minted before transaction scope close,
    cannot validate for another transaction, and is consumed exactly once at
    the terminal session boundary.
24. A close or abandonment racing terminal completion drains
    session-explicit scope only after the transaction proof returns the root.
25. Ordinary statement success and callback-error rollback close statement
    scope before returning the transaction checkout.
26. Dropping a public statement future settles pending acquisition, folds
    effects, closes statement scope, publishes cancellation, and later closes
    transaction scope in the required order.
27. Private prepared-catalog statement success and panic preserve
    must-complete/fatal-retention behavior while closing statement scope.
28. Statement-to-transaction handoff installs the destination claim before
    removing the source and rolls back only a fresh destination on failure.
29. Stream creation failure, exhaustion, explicit close, operation error, and
    caller Drop each close `StreamStmtState` exactly once after cursor/root
    destruction and before checkout return.
30. DDL preparation failure closes fresh operation claims and returns or drains
    the root according to session disposition.
31. DDL explicit-session exclusion uses the authoritative family slot and
    preserves existing public errors.
32. Maintenance records and later releases its own operation claim even when a
    stronger session-explicit claim covers the resource.
33. Accepted DDL and maintenance retain their operation scope through
    mandatory execution and close it before terminal publication.
34. A nested private transaction takes the exact operation root, leaves the
    outer operation scope unchanged, returns the root through the stable entry,
    and lets the accepted operation reclaim it.
35. Nested private transaction cancellation, rollback failure, and mandatory
    fatal retention park authority with one stable owner and never run a
    concurrent outer-scope close.
36. Engine shutdown drains statement, transaction, operation, then
    session-explicit scopes without leaking a manager grant or relying on a
    global owner scan.
37. Existing exact manager FIFO-prefix, same-family bypass, conversion,
    duplicate-waiter, promotion, cancellation, and release tests remain
    behaviorally unchanged.
38. Test snapshots join family and exact manager state and detect injected
    missing, duplicate, wrong-mode, wrong-id, and wrong-claim-number
    mismatches.
39. A randomized sequential reference model compares acquisition, conversion,
    release, handoff, and scope-close traces against the optimized dual-index
    implementation.
40. Instrumentation reports exact expected counts for covered acquisition,
    cross-scope manager mirroring, expansion, close visits, manager releases,
    and hypothetical physical-mode-preserving releases.
41. Existing session/transaction lock-table benchmark modes complete without
    leaks and preserve operation counts; recorded optimized before/after
    measurements include allocation and latency observations.
42. Focused and full verification passes:

    ```text
    rtk cargo build --workspace
    rtk cargo nextest run --workspace
    rtk cargo nextest run -p doradb-storage --no-default-features --features libaio
    rtk cargo clippy --workspace --all-targets -- -D warnings
    rtk cargo fmt --all -- --check
    tools/style_audit.rs
    ```

## Open Questions

No blocking design questions remain for Phase 1.

The following accepted RFC work remains explicitly deferred:

- RFC-0027 Phase 2 owns tokenized waiter identity, provisional-grant
  lifecycle, generational slab storage, and `O(1)` waiter unlink.
- RFC-0027 Phase 3 owns physical family holders, resource holder masks/counts,
  manager-local family transitions, removal of exact manager mirrors,
  production `release_owner()` removal, prepared-catalog authority removal,
  final diagnostics, and expanded contention benchmarks.
- Backlog 000167 continues to own deadlock policy.

During `$task-resolve`, update RFC-0027 Phase 1 with this task path, issue,
status, and verified implementation summary. Do not change later phase status.
