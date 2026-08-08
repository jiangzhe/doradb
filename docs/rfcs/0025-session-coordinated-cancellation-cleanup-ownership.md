---
id: 0025
title: Session-Coordinated Cancellation and Cleanup Ownership
status: superseded
superseded_by: docs/rfcs/0026-engine-owned-mandatory-background-runtime.md
tags: [storage-engine, session, cancellation, cleanup, lifecycle]
created: 2026-07-28
github_issue: 908
---

# RFC-0025: Session-Coordinated Cancellation and Cleanup Ownership

Superseded after Phase 2 on 2026-07-31. Phases 1 and 2 were implemented.
The former Phase 3 through Phase 7 program was closed without implementation
and replaced by RFC-0026. [D7] [U5]

## Summary

RFC-0025 established one stable session-operation authority for each effectful
public operation. The implemented foundation gives the session one active
operation entry, one session-local operation identity, and one terminal owner.
It also makes public statement future drop cancellation-safe: after transaction
checkout, residual statement undo becomes transaction-owned, statement redo is
discarded, statement locks are released, and ordinary whole-transaction cleanup
finishes the cancelled transaction. [D5] [D6] [C1] [C2] [C3]

Those implemented decisions remain the ownership substrate for later work. The
unimplemented direction in the original RFC—foreground DDL and maintenance
execution followed by observer-drop handoff of the same pinned future to one
cleanup runner—is no longer authoritative. RFC-0026 replaces it with
caller-cancellable preparation and engine-owned mandatory execution from the
acceptance boundary onward. [D7] [U5]

## Context

RFC-0019 made sessions and transactions engine-visible through stable
registries and weak public handles, but the session lifecycle still described
transactions more completely than other effectful operations. DDL,
maintenance, explicit session-lock work, nested private transactions, close,
abandonment, and shutdown needed one enclosing admission and terminal ownership
boundary. [D4] [C1] [C2] [U1]

Phase 1 implemented that boundary without layering a second entry around the
transaction hot path. It introduced a stable session-operation entry,
session-local operation identity, unified DDL/maintenance lock identity,
private-transaction attachment, key-only cleanup resolution, and lazy
session-local lifecycle notification. [D5] [C1] [C2] [C5]

Phase 2 implemented cancellation-safe public statement ownership. Dropping a
polled statement future no longer loses undo or panics on non-empty statement
effects. The dropping thread performs only synchronous ownership settlement;
the existing cleanup path applies asynchronous whole-transaction rollback.
[D6] [C2] [C3] [C4]

The remainder of the original RFC assumed that successful DDL and maintenance
would execute on the caller runtime and move to one cleanup worker only after
observer drop. RFC-0026 found that this made correctness depend on
client-controlled polling and required a foreground/background handoff that
DDL and maintenance do not need. It also left cleanup constrained by one
physical runner. The replacement RFC keeps caller scheduling for public
transactions but moves accepted DDL, maintenance, and transaction cleanup to an
engine-owned runtime. [D7] [U5]

This closed RFC therefore records only the implemented intent and invariants.
Exact implementation mechanics and measurement evidence remain in the two
completed task documents; all post-Phase-2 execution design belongs to
RFC-0026. [D5] [D6] [D7]

## Design Inputs

### Documents

- [D1] `docs/architecture.md` - engine ownership, foreground transaction
  execution, and subsystem boundaries.
- [D2] `docs/transaction-system.md` - transaction checkout, statement effects,
  terminal claims, rollback, and cleanup ownership.
- [D3] `docs/lock-system.md` - session, transaction, statement, DDL, and
  maintenance lock-family ownership and release ordering.
- [D4] `docs/rfcs/0019-weak-public-runtime-handles.md` - stable registry
  entries, weak public handles, cleanup hints, and shutdown behavior.
- [D5] `docs/tasks/000246-session-operation-coordinator-foundation.md` -
  implemented Phase 1 design and evidence.
- [D6]
  `docs/tasks/000247-statement-public-transaction-cancellation-ownership.md` -
  implemented Phase 2 design, performance evidence, and accepted debt.
- [D7] `docs/rfcs/0026-engine-owned-mandatory-background-runtime.md` -
  replacement for the unimplemented Phase 3 through Phase 7 direction.
- [D8] `docs/tasks/000244-add-rfc-0025-benchmark-workloads.md` - implemented
  successful-path benchmark prerequisite.

### Code References

- [C1] `doradb-storage/src/session.rs` - session disposition, active operation
  slot, operation reservation, close, abandonment, and lazy lifecycle events.
- [C2] `doradb-storage/src/trx/mod.rs` - stable operation entry, transaction
  checkout, completion and cleanup claims, and transaction-handle drop.
- [C3] `doradb-storage/src/trx/stmt.rs` - statement row/index undo, redo,
  statement locks, and rollback ordering.
- [C4] `doradb-storage/src/trx/sys.rs` - abandoned, terminal, and
  failed-precommit transaction cleanup.
- [C5] `doradb-storage/src/id.rs` and `doradb-storage/src/lock/mod.rs` -
  session-local operation keys and unified operation lock scope.

### Conversation References

- [U1] The user requested session-coordinated cancellation and cleanup
  ownership, with session state defining the enclosing operation lifecycle.
- [U2] The user selected one session-local `OperationID` domain for public and
  private transaction coordination and DDL/maintenance lock identity.
- [U3] The user selected synchronous statement cancellation settlement followed
  by whole-transaction cleanup, without a worker-owned statement phase.
- [U4] The user required successful transaction and statement execution to
  avoid unnecessary shared coordination and hot-loop overhead.
- [U5] On 2026-07-31, the user selected RFC-0026 as the replacement for the
  remaining RFC-0025 program and requested that RFC-0025 close as a concise
  record of intent and implemented core logic.

### Source Backlogs

- [B1] `docs/backlogs/closed/000170-session-coordinated-cancellation-cleanup.md`
- [B2] `docs/backlogs/closed/000124-statement-execution-cancellation-safety.md`
- [B3] `docs/backlogs/closed/000175-scalable-shared-resource-lifetime-management.md`
- [B4] `docs/backlogs/000171-exact-family-lock-system-redesign.md`

## Decision

### 1. Session state is the enclosing operation authority

Each effectful public session operation reserves at most one stable
`SessionOperationEntry`. Session disposition—open, close requested, or
abandoned—is orthogonal to the active entry, so dropping a public handle or
requesting close cannot steal payload ownership from the current operation
owner. The session returns to idle or closes only after the exact entry reaches
a terminal or safely retained outcome. [D4] [D5] [C1] [C2] [U1]

The entry generalizes the existing transaction entry rather than wrapping it.
One short mutex protects ownership state, current transaction identity, and
movement of the boxed transaction core. No lifecycle or entry mutex remains
held across user code, lock waits, I/O, rollback, or another `.await`.
Close and blocking shutdown install session-local notification lazily and wait
only after releasing registry and state guards. [D5] [C1] [C2] [U4]

Public transactions remain the outer operation across statements. DDL and
maintenance may attach sequential private transactions to the same entry, but
a private transaction cannot replace the outer operation or publish the session
idle independently. Failed cleanup retains a safe residual owner and keeps the
session unavailable rather than publishing ordinary completion. [D2] [D5]
[C2] [C4]

### 2. Coordination identity and transaction identity remain distinct

`OperationID` is session-local and meaningful only in
`SessionOperationKey(SessionID, OperationID)`. A session reserves it under the
lifecycle mutex, values do not deliberately repeat, and exhaustion cannot wrap.
The operation key identifies the enclosing lifecycle entry and cleanup
coordination; it is neither serialized nor used as an MVCC timestamp. [D5]
[C1] [C5] [U2]

`TrxID` remains the engine-wide transaction identity used by MVCC,
transaction locks, statement locks, commit, and recovery. A public transaction
has both identities. A private DDL or maintenance transaction inherits its
outer operation key while allocating its own `TrxID`; statements remain
transaction-local children and allocate no operation id. Registry cleanup
resolution uses the operation key, then validates the expected `TrxID` and
claimable state under the entry mutex so stale and duplicate hints are neutral.
[D2] [D5] [C2] [U2]

DDL and maintenance lock ownership uses
`LockScope::Operation(OperationID)` inside the owning
`LockFamily(SessionID)`. Typed operation capabilities and stable operation kind
preserve DDL-versus-maintenance policy; purpose is not inferred from the
numeric id. This removes duplicate DDL and maintenance id domains without
conflating operation coordination with transaction identity. [D3] [D5] [C5]
[U2]

### 3. Dropping a checked-out public statement cancels its transaction safely

Dropping an unpolled `Transaction::exec` future has no effect. Once the future
has checked out the transaction and armed statement ownership, dropping it
terminally cancels that public transaction. The public transaction facade is
thereafter discarded and cannot execute another statement or choose commit.
[D6] [C2] [U3]

The owned statement carrier settles cancellation synchronously:

1. pending lock acquisition is cancelled or its promoted grant is observed;
2. residual row and index undo is appended to transaction undo in preserved
   order;
3. statement redo is discarded;
4. statement locks are released while transaction locks still cover created
   effects; and
5. the complete boxed transaction core is checked into the stable entry as
   cleanup-ready.

Drop applies no undo and performs no async work. The existing transaction
cleanup path claims the complete core and performs ordinary whole-transaction
rollback. It receives neither a statement payload nor a statement rollback
phase. [D2] [D3] [D6] [C2] [C3] [C4] [U3]

An ordinary callback error still rolls back only the current statement.
Rollback retains the current undo entry until its fallible work succeeds, so a
cancelled or failed rollback leaves residual ownership visible in the buffer.
Once prepare or group commit owns terminal completion, observer or handle drop
cannot convert that accepted commit into rollback. [D2] [D6] [C2] [C3]

### 4. Terminal claims preserve release order and the successful path

At every point, one non-cloneable foreground, cleanup, or completion authority
owns the transaction payload. Terminal cleanup consumes ownership in this
order: settle pending statement acquisition, transfer residual statement
effects, release statement locks, complete transaction rollback or commit,
release transaction bindings and locks, consume the transaction-lock release
proof, and only then finalize the outer operation and session. Fatal failure
poisons and retains the residual owner instead of exposing idle state. [D2]
[D3] [D5] [D6] [C2] [C4]

The stable entry adds no second registry lookup, entry allocation, lock,
notification, or queue hop around successful statement checkout/check-in.
Row, index, page, MVCC, and stream-item loops perform no operation-coordinator
work. Lifecycle events are created only for an actual close or shutdown waiter.
[D5] [D6] [C1] [C2] [U4]

Phase 2 improved uncontended statement and transaction boundaries but exposed
repeatable contended statement-boundary and stream regressions. The
cancellation result was accepted with that fixed overhead recorded as explicit
debt in backlog 000175; the debt was subsequently resolved, while detailed
original samples and flamegraphs remain in task 000247.
[D6] [B3] [U4]

### 5. RFC-0026 owns all post-Phase-2 execution design

Phases 1 and 2 above remain the implemented baseline. RFC-0026 supersedes the
former plans for:

- caller-executor DDL and maintenance after acceptance;
- observer-drop transfer of the exact pinned operation future;
- foreground-queued-background poll-position states;
- a single worker-local executor and physical cleanup runner;
- per-workflow handoff and irreversible-gate test matrices; and
- the former Phase 3 through Phase 7 rollout.

RFC-0026 instead defines caller-cancellable preparation, atomic acceptance into
engine-owned mandatory execution, concurrent transaction cleanup, compact
voluntary/mandatory ownership, and explicit runtime shutdown ordering. If a
post-Phase-2 requirement in the historical RFC conflicts with RFC-0026,
RFC-0026 controls. Exact completed implementation details remain recoverable
from tasks 000246 and 000247 rather than from abandoned phase specifications.
[D5] [D6] [D7] [U5]

## Alternatives Considered

### Alternative A: Amend RFC-0025 with the replacement runtime

- Summary: Keep RFC-0025 active and rewrite its remaining phases around the
  engine-owned mandatory runtime.
- Analysis: This would preserve one program document, but it would mix two
  completed session/statement phases with a materially different execution
  authority, admission model, cleanup topology, state model, and shutdown
  order. [D5] [D6] [D7]
- Why Not Chosen: A separate replacement RFC makes the architecture change and
  phase prerequisites explicit while allowing RFC-0025 to close at its actual
  implementation boundary.
- References: [D5], [D6], [D7], [U5]

### Alternative B: Preserve the full historical specification

- Summary: Mark the RFC superseded but retain every former transition,
  operation gate, executor detail, validation case, alternative, and pending
  phase.
- Analysis: Version control and the completed task documents already preserve
  that history. Keeping unimplemented details in a formal RFC makes them appear
  normative, duplicates RFC-0026, and leaves unresolved `TBD/#0` phase tracking.
  [D5] [D6] [D7]
- Why Not Chosen: The closed RFC should communicate enduring intent and
  implemented invariants, not serve as an alternate implementation plan.
- References: [D5], [D6], [D7], [U5]

## Unsafe Considerations

The implemented decisions require no new unsafe ownership mechanism. Stable
entries, claims, synchronous Drop settlement, and lifecycle notification use
ordinary Rust ownership and synchronization. Any future unsafe work belongs to
the implementing task or replacement RFC and requires its own review. [D5]
[D6] [D7]

## Implementation Phases

Task 000244 supplied the successful-path benchmark workloads used by both
implemented phases; it was a completed program prerequisite rather than a
numbered phase. [D8]

- **Phase 1: Session Operation Coordinator Foundation**
  - Scope: Introduce the stable session-operation entry, session-local operation
    key, unified DDL/maintenance lock identity, private-transaction attachment,
    key-only cleanup resolution, and lazy close/shutdown notification.
  - Goals: Establish one enclosing admission and terminal authority without
    layering new synchronization on successful statement execution.
  - Non-goals: No statement cancellation settlement and no DDL/maintenance
    execution transfer.
  - Task Doc: `docs/tasks/000246-session-operation-coordinator-foundation.md`
  - Task Issue: `#914`
  - Phase Status: done
  - Implementation Summary: Implemented the stable session-operation
    coordinator, session-local operation identities, unified operation lock
    scopes, private-transaction attachment, key-only cleanup resolution, and
    lazy first-blocker close and shutdown notification, with paired benchmarks
    showing no regression.

- **Phase 2: Statement And Public Transaction Cancellation Ownership**
  - Scope: Add the owned statement carrier, synchronously settle residual
    statement effects on public future drop, and route the complete cancelled
    transaction through existing terminal cleanup.
  - Goals: Make statement future drop non-panicking and cancellation-safe
    without a worker-owned statement payload or successful-path hot-loop work.
  - Non-goals: No production DDL/maintenance migration and no background
    runtime.
  - Task Doc: `docs/tasks/000247-statement-public-transaction-cancellation-ownership.md`
  - Task Issue: `#917`
  - Phase Status: done
  - Implementation Summary: Implemented cancellation-safe public statement
    ownership, synchronous residual-effect settlement, whole-transaction
    cleanup, boxed transaction cores, and a reusable public-session core cache;
    accepted measured contention debt was later resolved through backlog
    000175.

### Superseded Remainder

The former Phase 3 through Phase 7 entries were never implemented and had no
task documents or task issues. Their mandatory-operation driver,
foreground-to-worker future handoff, table/index DDL migration, maintenance
migration, and final lifecycle-readiness program are closed as superseded.
RFC-0026 defines the replacement five-phase runtime-first program. [D7] [U5]

## Consequences

### Positive

- The closed RFC clearly preserves the implemented session, identity,
  statement-cancellation, and terminal-ownership contracts.
- Future DDL, maintenance, and cleanup work has one authoritative execution
  design in RFC-0026.
- Removing unimplemented mechanics and placeholder phases prevents abandoned
  details from being mistaken for current requirements.
- Completed task documents retain exact implementation and performance
  evidence without duplicating it here.

### Negative

- Readers needing exact Phase 1 or Phase 2 transitions must consult tasks
  000246 and 000247 or the current code.
- Existing transitional state names may remain in the implementation until
  RFC-0026 migrates them.
- The measured shared-resource lifetime contention remained open when this RFC
  was superseded and was subsequently resolved through backlog 000175.

## Open Questions

No blocking questions remain in RFC-0025. Runtime admission, execution,
shutdown, and state consolidation belong to RFC-0026. Exact-family lock
deadlock and mutation policy remains separate follow-up work under backlog
000171. [D7] [B4]

## Future Work

- Implement the RFC-0026 mandatory runtime and migrate DDL, maintenance, and
  transaction cleanup through its phases. [D7]
- Completed after this RFC closed: remove unnecessary hot-path shared-resource
  lifetime traffic and document long-lived resource ownership under backlog
  000175. [B3]
- Revisit exact-family lock-system policy independently under backlog 000171.
  [B4]

## References

- `docs/rfcs/0019-weak-public-runtime-handles.md`
- `docs/rfcs/0026-engine-owned-mandatory-background-runtime.md`
- `docs/tasks/000244-add-rfc-0025-benchmark-workloads.md`
- `docs/tasks/000246-session-operation-coordinator-foundation.md`
- `docs/tasks/000247-statement-public-transaction-cancellation-ownership.md`
- `docs/backlogs/closed/000170-session-coordinated-cancellation-cleanup.md`
- `docs/backlogs/closed/000124-statement-execution-cancellation-safety.md`
- `docs/backlogs/closed/000175-scalable-shared-resource-lifetime-management.md`
- `docs/backlogs/000171-exact-family-lock-system-redesign.md`
