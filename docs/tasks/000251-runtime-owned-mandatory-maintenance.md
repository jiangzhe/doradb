---
id: 000251
title: Runtime-Owned Mandatory Maintenance
status: implemented  # proposal | implemented | superseded
created: 2026-08-02
github_issue: 928
---

# Task: Runtime-Owned Mandatory Maintenance

## Summary

Implemented RFC-0026 Phase 4 by moving every accepted effectful public
maintenance attempt from the caller executor to the engine-owned mandatory
runtime. The migrated roots are table freeze, one-shot table checkpoint,
catalog checkpoint, combined catalog-checkpoint-plus-redo-truncation,
standalone redo truncation, and secondary MemIndex cleanup.

Each operation now completes authoritative, drop-cancellable preparation before
requesting mandatory capacity. Acceptance synchronously transfers the exact
session operation, logical locks, table/workflow authority, catalog and redo
gates, and operation resources to the runtime. Dropping the public future or
completion observer after acceptance does not cancel work or release its
resources early.

Checkpoint retry orchestration now runs as separate mandatory attempts with an
operation-free observer wait between them. Finite read-only observations,
standalone progress waits, table listing, and statistics remain caller-owned
and cancellable.

## Context

`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Parent RFC:`
`- docs/rfcs/0026-engine-owned-mandatory-background-runtime.md`

This task completed Phase 4 after:

- task `000248` introduced mandatory runtime admission, supervision, typed
  completion, and concurrent cleanup;
- task `000249` migrated table DDL and established lifetime-free prepared
  logical-lock authority;
- task `000250` migrated index DDL and established transferable metadata gates
  plus executor-neutral test control.

Before this task, effectful maintenance retained a foreground
`SessionOperationPin` while performing IO, publication, private transactions,
redo cleanup, and retry waits. Borrowed table and maintenance-gate guards could
not cross the mandatory runtime's `Send + 'static` boundary. Accepted work
could therefore still depend on the caller executor, and checkpoint retries
retained one operation across an indefinite wait.

The durable constraints were:

- caller preparation must remain cancellable before acceptance;
- accepted execution must own every effect and never reacquire an operation
  lock;
- workflow compensation and existing fatal publication boundaries remain
  authoritative;
- catalog authority is acquired before redo-retention authority and released
  before unlink, while redo authority remains held through cleanup;
- public APIs, error taxonomy, recovery behavior, and on-disk formats remain
  unchanged.

## Goals

1. Transfer each effectful maintenance attempt from
   `Voluntary(None)` to `Mandatory(None)` only after complete preparation.
2. Acquire table metadata `S` then data `IS` before mandatory capacity.
3. Retain the exact current-live table and release it before its logical locks.
4. Make freeze/checkpoint workflow attempts and root-mutation authority
   lifetime-free and cancellation-safe.
5. Preserve exact frozen-batch restoration on pre-publication cancellation.
6. Keep checkpoint publication admission and irreversible error policy inside
   accepted execution.
7. Prepare catalog checkpoint authority before redo-retention authority.
8. Release catalog authority after root/marker publication and retain redo
   authority through obsolete-file cleanup.
9. Keep MemIndex cleanup's active private transaction in supervised accepted
   resources and settle it before retry or finish.
10. Make observer drop execution-inert for every migrated operation.
11. Release domain resources, logical locks, and the outer operation in that
    order on normal completion.
12. Retain unsafe nested state and poison the engine after an unexpected
    accepted-execution panic.
13. Run each delayed checkpoint retry as a fresh operation with no permit or
    table owner retained during the wait.
14. Keep non-effectful diagnostics and standalone waits caller-owned.

## Non-Goals

1. No checkpoint, catalog, redo-retention, MemIndex, MVCC, or recovery
   algorithm change.
2. No public method signature, outcome type, error taxonomy, file format, or
   configuration change.
3. No generic lock plan, maintenance command registry, task group, priority
   lane, adaptive runtime, or dedicated maintenance pool.
4. No parallelization of an individual checkpoint, cleanup, scan, plan, or
   unlink workflow.
5. No fallible rollback, compensation, or storage cleanup from `Drop`,
   `finish`, or panic-policy callbacks.
6. No migration of `total_row_pages`, progress waits, table listing, or
   statistics to the mandatory runtime.
7. No mandatory permit retained across checkpoint retry waits.
8. No RFC-0026 Phase 5 stress, benchmark, observability, scheduling-policy, or
   superseded-RFC cleanup work.

## Plan

### Maintenance ownership and admission

`PreparedMaintenanceLocks` owns operation-scoped logical locks through
`OwnerLockState` and a retained lock-manager guard.
`PreparedMaintenanceScope` owns optional table locks followed by the voluntary
operation pin. Cancellation therefore releases locks before publishing the
foreground terminal edge.

`AcceptedMaintenanceScope` owns the transferred
`MandatoryOperationGuard`, prepared locks, and an
`Executing`/`TerminalReady`/`FailedRetained` finish state. Successful
execution proves the nested transaction slot returned to `Mandatory(None)`,
releases locks, and then publishes the outer terminal state. Panic handling
retains unsafe mandatory state before engine poison is published.

The shared `MaintenanceExecutionSpec` supplies each operation's output,
named resource structure, panic label, and execute body.
`PreparedMaintenanceExecution` and `AcceptedMaintenanceExecution` implement
the common prepared/accepted handoff, resource release, finish, and panic
policy once. Operation resources are declared before the maintenance scope so
they drop before logical locks and the outer session operation.

### Table freeze and checkpoint

Table preparation acquires metadata `S` then data `IS`, resolves the
authoritative live `Arc<Table>`, claims the reversible workflow attempt, and
acquires `TableCheckpointRootMutationScope` before capacity admission.

`Table::begin_freeze` and `Table::begin_checkpoint` return lifetime-free
attempts. The checkpoint workflow uses one shared admission state machine for
borrowed test/internal attempts and owned production attempts. Both attempt
forms share restoration logic, including returning the exact
`FrozenPageBatch` when an admitted checkpoint is dropped before publication.

Accepted freeze owns page selection, page-state publication, and fence
allocation. Accepted checkpoint owns analysis, page transition, table-root or
silent-watermark publication, system transaction enqueue, compensation, and
the existing reversible-to-fatal boundary.

`checkpoint_table_with_wait` calls one mandatory checkpoint attempt at a time.
A delayed result reaches terminal and releases its permit, table, workflow
attempt, root authority, and locks before the caller obtains a detached retry
observation. A later retry receives a new operation key and fresh authority.

### Catalog checkpoint and redo retention

`CatalogCheckpointScope` retains lifetime-free catalog checkpoint admission.
`RedoRetentionScope` retains a transaction-system guard and the shared
`ExclusiveGate` admission used for retained-redo observations. Production
preparation always acquires catalog authority first and redo authority second.

Catalog checkpoint, standalone truncation, and combined maintenance receive
both scopes as named accepted resources. Checkpoint execution no longer
acquires either gate internally. Standalone and combined truncation release
catalog authority through a callback after root or marker publication, while
redo authority remains held through unlink accounting.

The combined operation preserves projected silent-watermark and dropped-table
floor planning, catalog-safe segment proof, marker-only and combined-root
publication, purge requests, and retryable best-effort unlink results.

### Secondary MemIndex cleanup

Accepted cleanup stores its optional active private transaction outside the
panic-caught future and records a phase-specific panic label. Each iteration
starts a fresh private transaction, captures one proof-bound table root, and
rolls the transaction back before returning or retrying.

The fresh-STS retry loop is intentionally unbounded: transaction starts and
root publication fences use the same monotonic timestamp source, so a retry
observes the raced root unless another publication wins again. Awaited
rollback prevents the loop from becoming a tight busy loop.

### Supervision and test control

All migrated fault and phase controls are engine-scoped and thread-neutral.
Accepted resources stay outside the caught future so supervisor panic policy
can restore reversible workflow state, retain unsafe nested ownership, publish
engine poison, complete or detach the observer safely, and release the runtime
permit exactly once.

Global maintenance health rechecks occur after both gates are acquired but
after they have been packaged into the prepared carrier. An early fatal result
therefore releases catalog authority before redo authority and settles the
still-voluntary operation without crossing the mandatory acceptance boundary.

## Implementation Notes

Implemented all six effectful maintenance roots as caller-prepared, mandatory-runtime-owned operations while preserving observer-only waits and finite read-only observations.

- The implementation consolidated the originally planned operation-specific
  prepared/accepted carrier pairs into one generic carrier driven by
  `MaintenanceExecutionSpec`. Named resource structs preserve domain meaning
  and deterministic drop order; MemIndex cleanup uses a dedicated panic-label
  newtype.
- A reusable `ExclusiveGate` replaced redo-retention-specific duplicate gate
  state while preserving its precheck, fairness, and RAII release behavior.
- Freeze and checkpoint preparation moved to inherent `Table` methods.
  Borrowed and owned checkpoint attempts share one admission state machine and
  one restoration helper.
- Production-only wrapper layers and obsolete entry points were removed after
  the owned paths became authoritative. Test-only call chains now use the same
  production primitives.
- Review verified that pre-acceptance failures remain voluntary rather than
  being forced through mandatory ownership. Complete global preparation is
  packaged before the post-wait health check so gate release order remains
  catalog then redo.
- The MemIndex cleanup contract, fresh-STS retry proof, freeze-fence ordering,
  selected-page panic invariant, and catalog/redo gate rationale were retained
  as inline documentation after refactoring.
- A dropped-table purge regression test now waits for the asynchronous
  operational-state predicate before asserting retained state is empty,
  removing a CI scheduling race without changing purge behavior.
- Architecture, checkpoint, transaction-system, lock-system, engine-lifetime,
  public-error, and unsafe-usage documentation were synchronized with the
  runtime-owned maintenance boundary.
- Final verification passed the branch-diff style audit across 22 Rust files,
  strict workspace clippy, 1,629 standard workspace tests, and 1,536
  `libaio` tests. No nextest policy was changed.

No implementation work was deferred from this task.

## Impacts

- `Session` effectful maintenance now pays one mandatory-capacity admission and
  executor scheduling hop after preparation.
- Prepared operations may retain logical locks or maintenance gates while
  waiting for bounded runtime capacity; accepted execution never waits for
  those operation-level authorities.
- Table checkpoint workflow, lifecycle, persistence, page transition, and
  MemIndex cleanup now expose lifetime-free preparation suitable for transfer.
- Catalog checkpoint and redo truncation use owned scopes with explicit
  catalog-before-redo acquisition and early catalog release.
- Mandatory runtime supervision now covers maintenance private transactions,
  publication, compensation, marker update, and unlink completion.
- Public APIs, public outcomes, errors, storage formats, recovery semantics,
  dependencies, and configuration remain compatible.

## Test Cases

1. Unpolled and partially prepared futures create no effects and release every
   observed logical lock, workflow claim, gate, and operation on drop.
2. Capacity saturation proves all operation authority is prepared before a
   permit and acceptance transfers it without a release/reacquire gap.
3. Observer-drop tests prove accepted freeze, checkpoint, catalog/redo, and
   MemIndex cleanup continue to their normal terminal outcome.
4. Freeze/checkpoint cancellation restores idle or the exact frozen batch;
   delayed retry waits retain no operation, permit, table, or workflow owner.
5. Table checkpoint covers active-root and frozen-page delays, changed-root and
   silent-watermark publication, system commit, and post-publication poison.
6. Catalog checkpoint covers publish/no-op, durable scan bounds, retention
   progress, purge requests, gate serialization, and poison after gate wait.
7. Standalone and combined redo maintenance cover projected floors, marker
   advancement, blockers, missing files, retryable unlink failure, early
   catalog release, and redo exclusion through cleanup.
8. MemIndex cleanup covers root races, fresh transaction IDs, live delays,
   delete-overlay cleanup, per-index statistics, rollback precedence, observer
   drop, and panic retention.
9. Shutdown diagnostics distinguish voluntary preparation from accepted
   mandatory work and drain observer-dropped execution.
10. Existing DDL, transaction cleanup, table drop, recovery, catalog retention,
    and redo truncation suites remain passing.
11. The standard workspace suite passes with 1,629 tests.
12. The alternate `libaio` suite passes with 1,536 tests.

## Open Questions

No unresolved questions or deferred follow-ups remain in this task. RFC-0026
Phase 5 may now rely on complete production migration of DDL and effectful
maintenance to caller preparation plus atomic mandatory submission.
