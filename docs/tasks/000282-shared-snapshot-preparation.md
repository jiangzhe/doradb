---
id: 000282
title: Shared snapshot preparation
status: implemented
created: 2026-08-24
github_issue: 1013
---

# Task: Shared snapshot preparation

## Summary

RFC-0030 Phase 2 shipped a complete crate-private shared-snapshot preparation
workflow. A one-shot `ReadSnapshotBuilder` registers one active snapshot
timestamp, acquires metadata-S for the complete user-table set, binds
snapshot-visible metadata to compatible current table runtimes and layouts,
captures owned scan roots, and atomically publishes one cloneable
`ReadSnapshot` through the stable session registry entry.

The registry remains the canonical resource owner. Weak snapshot facades retain
no table, layout, root, active-STS registration, logical-lock scope, stable
entry, or strong runtime. Counted internal checkouts lend an immutable frozen
core and expose table, layout, read-view, and root access borrowed from the
exact checkout.

Explicit close, final-facade drop, session close or abandonment, and engine
shutdown seal the exact snapshot and perform one ordered synchronous cleanup
after accepted build or shared checkouts return. Deterministic planning was
split into RFC Phase 3; row execution and public export remain Phase 4, and
benchmark proof remains Phase 5.

## Context

Issue Labels:

- type:task
- priority:high
- codex

Parent RFC:

- `docs/rfcs/0030-shared-read-snapshots-parallel-table-scan.md`

RFC Phase:

- Phase 2: Shared snapshot preparation

RFC Phase 1, implemented by
`docs/tasks/000281-transaction-neutral-scan-read-view-owned-root-binding.md`,
provided transaction-neutral MVCC visibility, scan-only runtime capabilities,
and a lifetime-free owned scan root whose usable view is checkout-borrowed.

The existing stable session entry was deliberately transaction-centric. It
owned transaction identity, `TrxInner`, transaction cleanup intent, and family
authority state that cannot represent snapshot building, shared checkout,
draining, and terminal ownership without invalid combinations. Phase 2
therefore added a typed active-operation wrapper and a separate snapshot entry
state machine while preserving the mature transaction entry unchanged.

The complete query plan is expected to enumerate its user-table set before
snapshot begin. This permits one all-or-nothing acquisition and immutable
publication. Late table registration would require mutable shared state or a
second lock coordinator and remains intentionally unsupported.

The task originated directly from RFC-0030 and had no source backlog.

## Goals

1. Add a distinct read-snapshot session operation without changing transaction
   payload states.
2. Register one active STS before any captured table root becomes usable.
3. Acquire one nonempty, deduplicated user-table set through the existing
   family authority and operation lock scope.
4. Bind STS-visible metadata to compatible current table and layout owners and
   capture one registration-protected root per table.
5. Publish the complete immutable set atomically or publish nothing.
6. Keep builders and cloneable snapshot facades weak and resource-free.
7. Provide counted, concurrent checkouts with checkout-borrowed table, layout,
   read-view, and root access.
8. Make close, abandonment, pending-lock abort, and shutdown exact-key,
   cancellation-safe, and leak-free.
9. Preserve checkout pins before roots, roots before STS deregistration, locks
   before family-authority return, and cleanup before session publication.

## Non-Goals

1. No late or dynamic table registration or mutable frozen table set.
2. No scan options, worklist capture, coverage validation, weighting,
   partitioning, or immutable scan plan.
3. No partition open, page loading, MVCC row output, or execution stream.
4. No execution-wide failure signal, peer abort, or failed-drain state.
5. No Arrow, vectorized decoding, predicates, aggregation, joins, or scheduler.
6. No public crate-root export of the incomplete scan feature.
7. No transaction identity, undo, redo, effects, commit, rollback, or
   read-your-own-write semantics for snapshots.
8. No persisted format, recovery, checkpoint, GC, or table-file protocol
   change.
9. No new lock ordering, timeout, deadlock-victim, or cancellation protocol.
10. No replacement or lifetime erasure of `TableRootSnapshot<'_>`.

## Rejected Alternatives

1. **Keep deterministic planning in Phase 2.** Planning adds independent
   capture, validation, weighting, partitioning, ownership, and publication
   races. Splitting it keeps the registry and cleanup phase reviewable.
2. **Allow late table registration.** It would mutate a core already shared
   with workers or require another serialized authority. The optimized plan can
   provide the complete set once.
3. **Extend `SessionOperationEntryInner` with snapshot states.** Combining
   transaction and snapshot payloads would create many structurally invalid
   state combinations. Typed entries preserve closed state spaces.

## Plan

### Registry and facade ownership

`SessionOperationSlot::Active` stores an `ActiveSessionOperation` containing
either the established transaction-oriented entry or a pointer-stable
`ReadSnapshotEntry`. The wrapper delegates exact key, operation kind, state
label, and typed entry lookup without combining payload mutexes.

The snapshot entry owns immutable key and STS identity, one compact state
mutex, and one direct `Event` used only for checked-out build abort. Its states
cover checked-in and checked-out building, ready shared ownership, draining,
claimable and claimed completion, and terminal publication.

`ReadSnapshotBuilder` is `Send + !Sync + !Clone` and contains only weak session
reachability, key, STS, and an armed-drop marker. `ReadSnapshot` is
`Clone + Send + Sync`; its shared facade group contains only weak reachability,
key, copied STS, and an atomic closed bit.

### Build, lock, and table binding

`Session::begin_read_snapshot` reserves the exact typed operation, takes the
idle session's boxed `FamilyLockAuthority`, creates an operation metadata
scope, registers an active STS, builds an ownerless `MvccReadView`, and checks
the empty build core into the registry before foreground admission ends.

`acquire_tables` rejects an empty set and catalog IDs, deduplicates by first
occurrence, and exclusively checks out the build core. For each table it:

1. registers the entry abort listener before checking sticky abort;
2. races metadata-S acquisition against close, abandonment, or shutdown;
3. relies on the existing pending-claim guard for exact cancellation;
4. rechecks sticky abort after grant;
5. resolves STS-visible metadata against current table and layout identity;
6. captures a registration-gated `OwnedTableScanRoot`; and
7. installs one immutable binding.

No lifecycle or entry mutex is held across metadata wait or table/root work.
Any error or cancellation returns the build core and drains the entire accepted
lock prefix; a partial snapshot is never published.

### Freeze and shared checkout

After the final binding and a health recheck, publication linearizes under
`SessionState.lifecycle -> ReadSnapshotEntry`. Success freezes the bindings,
ownerless read view, and active registration into one immutable core while the
ready payload separately retains the lock owner.

Checkout requires healthy admission, an open session and facade group, the
exact active key, and `Ready`. It checked-increments the shared count and owns
only an operation-local `SessionRuntime`, exact entry, and frozen-core pin.
`CheckedOutSnapshotTable<'_>` borrows visible metadata, table, layout, and a
usable `CheckedOutTableScanRoot<'_>` from that checkout. Returning to zero while
still ready preserves the snapshot for reuse.

### Drain and terminal cleanup

Explicit close atomically seals the whole facade group before its first wait;
cancelling the close future cannot reopen the snapshot. Final facade drop uses
the same exact-key drain without waiting. Session close, abandonment, and
shutdown request sticky build abort or ready drain and reject new checkout.

The sole terminal claim retains the exact entry and strong runtime while
performing this order:

```text
checkout-local pins
-> registry-owned bindings and roots
-> ownerless read view and active-STS registration
-> metadata operation scope
-> idle family authority
-> exact entry terminal publication
-> session idle or closed publication
```

Cleanup performs no registry, lifecycle, or entry lock hold while dropping
roots, deregistering STS, or closing logical locks. Shutdown claims checked-in
snapshot cleanup synchronously during registry inspection; retained build or
shared checkouts remain visible blockers until returned. Dormant facades never
block shutdown.

`SessionRuntime` remains the zero-cost strong exact-session capability. Methods
that combine state mutation and retained runtime ownership live on that type,
so callers cannot pair unrelated state and runtime values. Close wait loops
release strong runtime reachability before awaiting their listeners.

### Errors and phase boundary

Snapshot identity and lifecycle rejection use
`LifecycleError::ReadSnapshotUnavailable`; empty input and missing checkout
tables use `OperationError::InvalidReadSnapshotInput` and
`OperationError::TableNotAcquired`. Internal build orchestration retains
`QuadResult`, checkout retains `LifecycleOrFatalResult`, and public error
disclosure remains only at the session/future facade boundary.

All new APIs remain crate-private. RFC-0030 Phase 3 consumes the immutable
bindings and checkout-borrowed roots for deterministic planning. Phase 4 owns
real row streams and public export; Phase 5 owns benchmark evidence.

## Implementation Notes

Phase 2 shipped the complete
`begin_read_snapshot -> acquire_tables -> shared checkout -> close` workflow,
including session close, abandonment, pending-lock cancellation, final-facade
cleanup, and engine-shutdown integration. The transaction entry state machine,
logical-lock cancellation protocol, and public scan API remain unchanged.

Review refined several implementation boundaries:

- `ActiveSnapshotRegistration` and maintenance `PrivateSnapshot` were
  consolidated into `trx/read_snapshot.rs`; the obsolete `trx/readonly.rs`
  module was removed.
- Cross-boundary snapshot operations and public transaction begin orchestration
  moved onto `SessionRuntime`; its constructor became session-module-private.
- The snapshot abort notifier is one direct `Event`. A valid nonempty build
  always registers a listener, and the build checkout's entry `Arc` proves the
  event outlives that listener, so an outer `Arc<EventNotifyOnDrop>` was
  unnecessary.
- Snapshot and session close loops explicitly release strong runtime ownership
  before awaiting independently owned lifecycle listeners, preventing a
  completed close future from becoming a hidden shutdown owner.
- Internal error surfaces were narrowed to native report carriers rather than
  disclosing public `Error` inside reusable orchestration.
- Dropping an unconsumed terminal claim restores its complete payload before
  surfacing the invariant, preserving the sole cleanup owner during unwinding.
- The redundant test-only registry close helper was removed; lifecycle tests
  use production session behavior and narrow test fixtures.

The parent RFC was split as planned: shared snapshot preparation is Phase 2,
deterministic planning is Phase 3, row-oriented execution and export are Phase
4, and benchmark proof is Phase 5. No additional backlog was required because
all deferred capability work already has an explicit RFC phase.

Final verification completed with:

- mandatory style audit against `origin/main`, including strict workspace
  Clippy: passed for 9 branch-diff Rust files;
- focused `read_snapshot` suite: 13 passed on the default backend and 13 passed
  with `libaio`;
- workspace nextest suite: 1,793 passed;
- full alternate `libaio` suite: 1,710 passed;
- focused line coverage across all 9 changed Rust files: 94.58% combined;
  every file exceeded 85%, and `read_snapshot.rs` reached 88.94%; and
- formatting and `git diff --check`: passed.

## Impacts

- Sessions can own one registry-authoritative read snapshot as their active
  operation and coordinate it through normal close, abandonment, and shutdown.
- Ready snapshots retain active STS, table/layout/root owners, and metadata-S
  until drain, so intentionally long-lived snapshots can delay reclamation and
  metadata-X progress.
- Shared-snapshot owned-root capture requires an active snapshot registration,
  while usable root fields remain checkout-borrowed.
- Transaction table admission and snapshot preparation share metadata/current
  runtime resolution without changing transaction behavior.
- Shutdown can synchronously clean checked-in snapshots while preserving the
  existing transaction cleanup queue.
- There is no public API, dependency, schema, persisted format, recovery,
  configuration, or benchmark change in this phase.

## Test Cases

Completed acceptance coverage includes:

1. Active-STS registration and deregistration for shared and private snapshots.
2. Compile-time builder/snapshot/checkout Send, Sync, and Clone boundaries.
3. Multi-table deduplication, metadata-S ownership, immutable bindings,
   ownerless read view, root capture, checkout reuse, and explicit close.
4. Empty input, catalog/missing tables, accepted-prefix failure, builder drop,
   and unpolled consuming-future cleanup without STS or lock leaks.
5. Missing-table checkout rejection through the native Operation carrier.
6. Cancellation-safe group-wide close and final-facade best-effort drain.
7. Blocked metadata acquisition aborted by session abandonment with prompt
   pending-claim cancellation and prefix cleanup.
8. Session close waiting for an accepted checkout and completing after return.
9. Snapshot and session close listeners retaining no hidden runtime across
   await points.
10. Checked-in snapshot cleanup on public session abandonment.
11. Try-shutdown cleanup of a ready snapshot followed by successful rescan.
12. Unexpected terminal-claim drop restoring the complete payload before its
    invariant assertion.

## Open Questions

None for Phase 2. Deterministic planning, row execution/public export, and
benchmark proof remain owned by RFC-0030 Phases 3, 4, and 5 respectively.
