---
id: 000258
title: Establish Linear Lock Family Authority and Owner-Side Indexes
status: implemented  # proposal | implemented | superseded
created: 2026-08-06
github_issue: 948
---

# Task: Establish Linear Lock Family Authority and Owner-Side Indexes

## Summary

Implemented RFC-0027 Phase 1 by replacing independent owner lock caches with
one linear authority per session family and two authoritative owner-side
indexes.

Each engine-local session now allocates one boxed `FamilyLockAuthority`. The
same allocation moves through idle session state, admitted operations,
transactions, prepared and terminal transaction states, and accepted DDL or
maintenance execution. Exact operation, transaction, and statement carriers
own their matching `LockScopeState`; the session-explicit scope remains beside
the family root.

Every accepted logical claim is indexed by family/resource and by exact
scope/resource. The common single claim is inline; a second scope expands the
resource once into four fixed typed slots. A checked session-local `ClaimNo`
connects both entries and prevents stale-token mutation.

Phase 1 deliberately retains the existing lock-manager representation. Every
fresh logical claim still creates one exact-owner manager grant, including
claims covered by another scope in the same family. Normal cleanup now follows
the exact scope index rather than scanning manager resources.

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

- Tasks 000242, 000243, 000246, 000247, and 000249 established terminal
  lock-release ordering, operation scopes, coordinator ownership, statement
  cancellation, and runtime-owned DDL.

Related work includes open backlog 000171, completed backlogs 000115, 000169,
and 000170, and lock-table benchmark task 000257.

The former `OwnerLockState` made repeated exact-owner acquisition and cleanup
local, but it could not describe other scopes in the same session family.
Session-explicit cleanup consequently depended on
`LockManager::release_owner()`, and lifecycle transfer did not prove that one
authority governed all family mutation.

The implemented indexes provide both required lookup directions:

```text
family/resource -> every live exact-scope claim for that resource
exact scope     -> every resource claimed by that scope
```

Both entries carry the same `(ClaimNo, LockMode)`. They are authoritative
state, not mutually repairing caches. Session execution remains linear, so
owned values and exclusive borrows enforce mutation authority without a
family mutex, actor, lease, or repair protocol.

## Goals

1. Replace `OwnerLockState` with family/resource and exact-scope authority.
2. Move one boxed family root through every lifecycle owner without cloning or
   reconstruction.
3. Provide bounded claim lookup, targeted cleanup, and checked session-local
   claim identity.
4. Keep exact covered acquisition local while preserving family coverage,
   conversion, and manager queue semantics.
5. Preserve statement, stream, admission, DDL, maintenance, cancellation, and
   fatal-retention behavior and close order.
6. Keep terminal publication dependent on an owning closed-lock-state proof.
7. Add deterministic layout, invariant, lifecycle, and reference-model tests.
8. Preserve public APIs, persistent formats, recovery, and MVCC behavior.

## Non-Goals

1. Do not add physical family holders or Phase 2 tokenized waiter storage.
2. Do not change manager grant/waiter records or remove the prepared catalog
   authority bridge.
3. Do not add blocking conversion, downgrade, escalation, timeout, or deadlock
   policy.
4. Do not add parallel family mutation, a family coordinator, per-resource
   claim hashing, unsafe packing, or first-claim allocation.
5. Do not change compatibility, FIFO promotion, storage formats, transaction
   ordering, or recovery behavior.

## Plan

### Owner-side representation

`FamilyLockAuthority` owns the family/resource index and the persistent
session-explicit scope. `TransactionLockState` and operation carriers pair the
same authority with a carrier-specific `curr_scope`. Statement and stream
carriers retain their existing RAII responsibilities and own only their
statement scope.

`LocalFamilyResourceState` stores an inline claim until another scope class
claims the same resource. It then expands into fixed session-explicit,
operation, transaction, and statement slots. Expanded storage is retained
until the complete family/resource entry disappears. `claim_mask` records
occupied modes, while `covering_mode` is an occupied claim mode that covers
all remaining claims.

`ClaimNo` is defined through the standard `impl_id!` macro over `u64`; zero is
a valid value. Allocation begins at one, uses checked arithmetic, reserves
before a fresh attempt can fail or wait, and never recycles numbers during the
session lifetime.

### Acquisition, release, and rollback

Acquisition first checks the exact scope. A covered request returns
`LockGrant::Existing` without entering the manager. Conversion validates the
other family slots, performs the existing immediate manager conversion, and
updates both owner-side entries while retaining `ClaimNo`.

A fresh exact-scope miss reserves a claim number and applies family coverage
policy. Accepted fresh claims always enter the Phase 1 manager and require a
fresh exact-owner grant. Owner-side publication then inserts both entries
synchronously without an await or recoverable failure boundary.

`FreshClaimsGuard` records only claims made fresh by a multi-resource
operation and releases them in reverse order if the operation does not disarm
the guard. Existing or converted claims are never rolled back as fresh.

Release validates the token owner and current scope claim number, computes the
remaining mask and covering mode from the trusted family slots, releases
exactly one manager mirror, and removes both owner-side entries. Direct
operation results are asserted, but production code does not traverse both
indexes merely to revalidate an invariant maintained by each mutation.
`close_scope` visits only resources in the exact scope index.

### Lifecycle authority

The boxed root moves from the idle session into a foreground operation and,
for a public transaction, into `TransactionLockState`. Transaction close
drains its scope and returns the same box in `ReleasedTransactionLocks`.
Terminal session attachment consumes that single-use proof before reinstalling
the root in an open idle session or draining session-explicit claims for a
closed session.

Accepted DDL and maintenance keep their operation scope while nested private
transactions temporarily own the root. The stable operation entry holds the
returned root until the accepted carrier reclaims it. No outer operation
mutation or close can occur while its root is installed in the private
transaction.

Normal cleanup order is statement, transaction, operation, then
session-explicit scope. All close, abandonment, prepared/precommit,
failed-retention, and mandatory terminal paths preserve the single owner of
the root or its stable return right.

### Statements, admission, and runtime access

`StmtState` and `StreamStmtState` retain their effect, checkout,
cancellation, and Drop policies while replacing owner caches with optional
statement scopes. Exactly-once settlement closes those scopes before returning
the transaction checkout.

Table admission acquires the destination transaction metadata claim before
releasing the source statement claim, leaving no unprotected handoff gap.
Family-only paths use `TransactionLockState::family_mut()`; `parts()` remains
only where a guard genuinely needs simultaneous family and transaction-scope
borrows.

`PreparedCatalogWriteAuthority` remains a narrow borrowed proof over the
accepted operation scope. It allows nested catalog writes to reuse prepared
catalog coverage without becoming a general bypass.

Maintenance table access is returned as `SessionTable<'s>` from
`SessionOperationPin::read_table()`. The wrapper ties the strong table runtime
to the admitted operation and releases it before the operation pin can end.

Foreground weak-session access is consolidated in `WeakSessionRef::upgrade()`,
which acquires admission and upgrades the exact state into
`AdmittedSessionRuntime`. The redundant intermediate admitted-reference type
was removed.

### Manager boundary

`LockManager` retains one exact `GrantedLock` per accepted logical claim,
`Vec<GrantedLock>`, `VecDeque<Arc<Waiter>>`, FIFO-prefix promotion, current
same-family bypass, immediate-only conversion, and waiter cancellation
identity.

Normal lifecycle cleanup no longer calls `release_owner()`. That method
remains for manager-level tests and migration defenses. Physical family
aggregation and tokenized waiter storage remain RFC-0027 Phase 3 and Phase 2
respectively.

## Implementation Notes

Implemented RFC-0027 Phase 1 with one move-only family authority and targeted scope cleanup while retaining exact manager mirrors.

Authoritative family/resource and exact-scope indexes, checked claim identity,
and lifecycle-wide root transfer establish the owner-side proof required by
later waiter and physical-family phases.

- Claim storage moved into `lock/claim.rs`; layout tests establish a 96-byte
  `FamilyClaimSlots` target and bounded typed claims without unsafe packing.
- Family counters cover local reuse, manager mirrors and calls, expansion,
  fresh claims, conversion, close visits, and mode-preserving releases.
- Review simplified claim naming through `id.rs`, accepted zero as a valid raw
  `ClaimNo`, renamed aggregate state to `covering_mode`, and removed redundant
  production structural revalidation.
- Review also removed the fresh single-insert rollback guard because
  publication after a fresh manager grant is synchronous and invariant-only;
  `FreshClaimsGuard` remains for fallible multi-resource operations.
- `FamilyLockState::release()` reports whether a claim existed. Required
  manager and map removal results remain asserted, while whole-structure
  agreement checks are confined to tests.
- Accessors were narrowed so callers borrow only family state unless they
  directly require a scope.
- `PreparedCatalogWriteAuthority` was retained as the Phase 1 migration bridge
  after call-site review confirmed nested catalog writes still depend on its
  prepared operation coverage.
- Invalid cross-domain context stacking around fatal poison checks is deferred
  to `docs/backlogs/000178-common-multi-domain-error-carrier.md`.
- No standalone before/after lock-table benchmark result was committed for
  this phase. Representation size and path counters were added; RFC-0027
  Phase 3 retains the expanded contention and cutover benchmark requirement.
- Final verification passed 1,683 default-feature workspace tests, 1,576
  `libaio` storage tests, workspace Clippy with warnings denied, formatting,
  diff checks, and the 14-file branch-diff style audit.

## Impacts

- Owner-side state now has one family root, bounded resource claims, and
  exact-scope cleanup across all lifecycle carriers.
- Transaction terminal proof now owns the returned family root; private
  transactions return it through their stable operation entry.
- Table admission and prepared catalog access preserve existing protection
  ordering with the new scope representation.
- Lock-manager grant and waiter storage, compatibility behavior, and queue
  ordering are unchanged.
- Lock, transaction, and engine-lifetime docs describe the new ownership
  model; persistent formats, schemas, recovery, MVCC, and dependencies are
  unchanged.

## Test Cases

Completed coverage verifies:

1. Family construction, checked claim allocation, and same-box lifecycle
   return.
2. Inline claims, one-time expansion, all scope classes, retained expanded
   storage, masks, covering modes, and final removal.
3. Exact-local coverage, cross-scope manager mirroring, conversion, rejection,
   cancellation, fresh rollback, and stale-token safety.
4. Selective unlock and targeted close for every scope class.
5. Family/scope/manager agreement through deterministic reference-model
   traces.
6. Public and private transaction terminal paths, owning proof transfer,
   abandonment, and fatal retention.
7. Statement, stream, admission handoff, DDL, maintenance, and session
   shutdown paths.
8. Existing manager FIFO, coverage, conversion, promotion, cancellation, and
   duplicate defenses.
9. Both storage backends, full workspace tests, Clippy, formatting, and
   repository style rules.

## Open Questions

No blocking Phase 1 questions remain.

- `docs/backlogs/000178-common-multi-domain-error-carrier.md` tracks whether a
  constrained common carrier should preserve Operation, Runtime, Lifecycle,
  and Fatal reports without invalid context replacement.
- RFC-0027 Phase 2 owns tokenized waiter identity, provisional-grant
  lifecycle, generational storage, and constant-time waiter unlink.
- RFC-0027 Phase 3 owns physical family holders, removal of exact manager
  mirrors and `PreparedCatalogWriteAuthority`, production
  `release_owner()` removal, final diagnostics, and expanded benchmarks.
- Backlog 000167 continues to own deadlock policy.
