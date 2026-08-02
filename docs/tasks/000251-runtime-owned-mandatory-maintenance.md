---
id: 000251
title: Runtime-Owned Mandatory Maintenance
status: proposal  # proposal | implemented | superseded
created: 2026-08-02
github_issue: 928
---

# Task: Runtime-Owned Mandatory Maintenance

## Summary

Implement Phase 4 of RFC-0026 by moving every accepted effectful public
maintenance attempt from the caller executor to the engine-owned mandatory
runtime. Keep authoritative operation preparation caller-owned and
drop-cancellable: reserve one maintenance operation, acquire the complete
logical-lock and long-lived maintenance-gate scope, resolve any exact live
table runtime, and wait for mandatory capacity only after the attempt is
execution-ready.

Migrate table freeze, one-shot table checkpoint, catalog checkpoint, combined
catalog-checkpoint-plus-redo-truncation, standalone redo truncation, and
secondary MemIndex cleanup. Give each operation a typed
`PreparedExecution`/`AcceptedExecution` carrier in its owning domain. Accepted
execution owns every effect, private or system transaction, publication,
compensation, marker update, unlink, result, and panic policy until a supervised
terminal outcome. Dropping the public future or completion observer after
acceptance must be execution-inert.

Refactor table-checkpoint retry orchestration into a caller-owned sequence of
separate mandatory attempts. A delayed attempt must reach terminal and release
its table runtime, workflow authority, logical locks, and mandatory permit
before the caller enters the independently cancellable retry wait. Preserve
caller ownership for standalone progress waits and the finite, read-only
`total_row_pages` observation.

Use lifetime-free, domain-owned RAII scopes for table checkpoint
root-mutation/metadata exclusion, catalog checkpoint exclusion, and redo
retention. Do not add a generic lock plan, task group, central task registry,
or maintenance scheduler abstraction. Keep `Implementation Notes` empty until
task resolution.

## Context

`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Parent RFC:`
`- docs/rfcs/0026-engine-owned-mandatory-background-runtime.md`

This task is RFC-0026 Phase 4, **Runtime-Owned Mandatory Maintenance**.
RFC-0026 Phases 1 through 3 are complete:

- task `000248` introduced the fixed engine-owned mandatory executor,
  consuming prepared-to-accepted handoff, typed completion observation, panic
  supervision, compact `Voluntary`/`Mandatory` session-operation ownership,
  concurrent internal cleanup, and ordered redo-runtime-purge shutdown;
- task `000249` established production caller-prepared/runtime-owned table DDL,
  lifetime-free logical-lock scopes, prepared catalog-write authority, nested
  private transactions, and cross-thread deterministic controls;
- task `000250` extended that pattern to index DDL, transferred table/catalog
  metadata gates, preserved atomic runtime layout/history publication, and
  added cooperative execution where appropriate.

The Phase 4 prerequisite is therefore satisfied in design. Implementation must
still keep the focused Phase 1 through Phase 3 acceptance, observer-drop,
panic, cleanup, and shutdown tests passing under one- and multi-runner
configurations.

The current public maintenance paths in `session.rs` are classified as follows:

| Public API | Current behavior | Phase 4 behavior |
| --- | --- | --- |
| `freeze_table` | caller-owned `Maintenance` operation with borrowed table access | one prepared mandatory attempt |
| `checkpoint_table` | caller-owned `Maintenance` operation with borrowed table access | one prepared mandatory attempt |
| `checkpoint_catalog` | caller executes catalog/redo gate waits and checkpoint | one prepared mandatory attempt |
| `checkpoint_catalog_and_truncate_redo_log` | caller executes both gates, publication, and unlink | one prepared mandatory attempt |
| `truncate_redo_log` | caller executes both gates, marker publication, and unlink | one prepared mandatory attempt |
| `cleanup_secondary_mem_indexes` | caller-owned table access and private-transaction retry loop | one prepared mandatory attempt |
| `checkpoint_table_with_wait` | one maintenance operation owns all attempts and retry waits | caller loop over separate mandatory attempts and observer waits |
| `total_row_pages` | finite caller-owned lock-protected observation | unchanged and explicitly non-mandatory |
| checkpoint/GC/purge progress waits | caller-owned observer admission | unchanged and explicitly non-mandatory |
| statistics and table-list diagnostics | lifecycle inspection/observer admission | unchanged and explicitly non-mandatory |

`ScopedTableRuntimeAccess` currently acquires borrowed `FreshLockGuard`s for
`TableMetadata(S)` followed by `TableData(IS)`, resolves the current live
runtime, and releases the `Arc<Table>` before its data and metadata guards.
Those borrowed guards cannot cross the mandatory runtime's `Send + 'static`
acceptance boundary. DDL already solves the analogous problem with an
`OwnerLockState` plus a retained `QuiescentGuard<LockManager>`, but its
DDL-specific catalog-write proof and lock sets should not be widened or
destabilized for this phase.

Table freeze/checkpoint has two ownership layers beyond logical table access:

1. `TableCheckpointWorkflow` owns the reversible `Freezing`,
   `Checkpointing`, `Frozen`, `Publishing`, and `Transition` states and the
   exact frozen batch restored by a cancelled attempt.
2. `TableLifecycle` excludes checkpoint root mutation from index metadata
   change and separately admits the later reversible-to-irreversible
   publication boundary.

Today `Table::freeze` and `Table::checkpoint` claim their workflow and root
mutation inside caller execution. Phase 4 must make the reversible attempt and
root-mutation authority lifetime-free and caller-prepared. Publication
admission remains inside accepted execution because it is a synchronous,
publication-local transition reached only after analysis has exhausted
ordinary delays; it is not a waiting operation-admission lock.

Table checkpoint preserves the existing durable protocol:

- a changed root atomically publishes LWC data, cold-delete payload,
  secondary `DiskTree` state, replay bounds, and allocation reachability;
- a replay-bound-only outcome mutates a catalog silent-watermark row and
  submits its system redo;
- publication or transition crosses the existing fatal boundary;
- successful return follows system-transaction enqueue acceptance, not redo
  durability;
- an ordinary delay or lifecycle cancellation exposes no partial publication.

`checkpoint_table_with_wait` currently drops each borrowed table access before
sleeping but retains one outer `Maintenance` operation and one operation key
across the whole loop. RFC-0026 instead requires each attempt to be an
independent accepted root. The existing standalone
`wait_for_checkpoint_retry` already owns only observer/listener state during
the indefinite sleep and is the required orchestration boundary.

Catalog checkpoint and redo truncation currently acquire borrowed gates inside
their execution call graphs:

```text
CatalogCheckpointGate
    -> RedoRetentionGate
    -> scan/plan/publish
```

The order prevents catalog root writers, catalog metadata DDL, retained-redo
scans, marker publication, and file cleanup from observing inconsistent
states. Standalone and combined truncation intentionally release the catalog
gate after publishing the root/marker but retain redo exclusion through
obsolete-file unlink. Phase 4 must preserve both acquisition and partial
release ordering with owned scopes prepared before mandatory capacity.

Secondary MemIndex cleanup starts a private transaction, captures a proof-bound
root, retries with a fresh STS if a newer root won the race, performs cleanup,
and rolls back the read-only maintenance transaction. That transaction cannot
remain a local of the panic-caught future after migration: an unexpected
unwind could otherwise drop the facade before the mandatory owner has retained
the nested transaction core. The accepted carrier must store cleanup progress,
including the optional active private transaction, outside its caught future.

Several current table checkpoint/freeze/cleanup fault and coordination hooks
are thread-local. Once Session maintenance executes on a mandatory runner,
installing such a hook on the caller thread cannot control the production path.
Every hook consulted by migrated freeze, checkpoint, or MemIndex-cleanup call
graphs must become shared and executor-neutral, following the engine-scoped
controllers established by Phases 2 and 3. The caller-owned
`total_row_pages` hook need not migrate.

RFC-0026 Phase 5 assumes that every production DDL and mandatory-maintenance
path uses caller preparation plus atomic mandatory submission, and that no
accepted operation acquires an operation lock. This task must leave that
assumption true. It does not perform Phase 5's final diagnostic, stress,
benchmark, or superseded-RFC cleanup work.

Relevant design, process, implementation, and prerequisite sources:

- `docs/rfcs/0026-engine-owned-mandatory-background-runtime.md`
- `docs/tasks/000248-mandatory-operation-driver-and-concurrent-cleanup-executor.md`
- `docs/tasks/000249-runtime-owned-table-ddl.md`
- `docs/tasks/000250-runtime-owned-index-ddl.md`
- `docs/architecture.md`
- `docs/checkpoint.md`
- `docs/table-file.md`
- `docs/transaction-system.md`
- `docs/lock-system.md`
- `docs/engine-component-lifetime.md`
- `docs/process/coding-guidance.md`
- `docs/process/unit-test.md`
- `doradb-storage/src/runtime/mandatory.rs`
- `doradb-storage/src/session.rs`
- `doradb-storage/src/table/{mod,lifecycle,checkpoint_workflow,persistence,gc}.rs`
- `doradb-storage/src/catalog/{mod,checkpoint}.rs`
- `doradb-storage/src/trx/{mod,sys,retention}.rs`
- `doradb-storage/src/engine.rs`

## Goals

1. Split each effectful maintenance API at the first operation-effect boundary
   into caller-owned preparation and mandatory-runtime-owned execution.
2. Preserve an unpolled-call guarantee: no operation entry, logical lock,
   workflow claim, maintenance gate, runtime permit, transaction, or storage
   effect is created until the public future is polled.
3. Reserve exactly one `SessionOperationKind::Maintenance` entry for each
   effectful attempt and transfer that exact entry
   `Voluntary(None) -> Mandatory(None)` at acceptance.
4. Acquire table metadata `S` then data `IS` through one lifetime-free
   operation-owned lock state before mandatory capacity.
5. Resolve and retain the exact current-live `Arc<Table>` after logical locks
   are held, and release every table/runtime owner before releasing the locks
   that authorize it.
6. Make freeze/checkpoint workflow attempts lifetime-free and cancellation
   safe, including exact frozen-batch restoration.
7. Acquire table checkpoint root-mutation/metadata-exclusion authority before
   capacity and perform no such acquisition after acceptance.
8. Keep publication admission inside accepted checkpoint execution and
   preserve its reversible-to-fatal boundary.
9. Acquire catalog checkpoint authority before redo-retention authority and
   transfer both without release/reacquisition.
10. Preserve early catalog-authority release and redo-authority retention
    through obsolete-file unlink.
11. Make observer or public-future drop after acceptance semantically inert for
    every migrated operation.
12. Keep every accepted operation resource outside the unwind-caught future so
    panic supervision retains ownership while domain policy runs.
13. Store MemIndex cleanup's active private transaction in accepted progress,
    settle each transaction before retry or normal finish, and retain unsafe
    nested ownership on panic.
14. Preserve cleanup-error versus rollback-error precedence.
15. Preserve existing table checkpoint delayed, cancelled, root-published,
    silent-watermark, system-commit, and fatal outcomes.
16. Preserve catalog checkpoint, redo marker, blocker reporting, best-effort
    unlink accounting, and retry behavior.
17. Refactor `checkpoint_table_with_wait` into separate mandatory attempts with
    an operation-free, permit-free observer wait between them.
18. Keep `total_row_pages`, standalone progress waits, statistics, and table
    listing caller-owned and drop-cancellable.
19. Give accepted operations immutable runtime diagnostics using existing
    `MandatoryTaskMetadata` operation/table-operation constructors.
20. Convert affected test controls to shared, thread-neutral coordination and
    add deterministic acceptance-boundary coverage.
21. Keep successful transaction and statement paths free of new mandatory
    runtime work, operation-state writes, allocations, or admission checks.
22. Satisfy RFC-0026 Phase 5's production-migration prerequisite without
    changing its scope.

## Non-Goals

1. Do not migrate `total_row_pages`, standalone checkpoint/GC/purge waits,
   table-list diagnostics, or statistics snapshots to the mandatory runtime.
2. Do not make `checkpoint_table_with_wait` one accepted task that sleeps,
   releases/reacquires authority, or consumes capacity across retries.
3. Do not change checkpoint selection, page-transition, LWC encoding,
   deletion-checkpoint, secondary-index sidecar, allocation-map, root
   publication, or silent-watermark algorithms.
4. Do not change catalog checkpoint scanning, folding, root format, replay
   boundary, or catalog-safe redo progress semantics.
5. Do not change redo-file discovery, truncation planning, durable marker
   format, blocker classification, or best-effort unlink policy.
6. Do not change MemIndex cleanup eligibility, snapshot proof, key comparison,
   batching, statistics, or live-delay semantics.
7. Do not change MVCC, timestamp allocation, system-transaction ordering,
   recovery, redo durability, catalog/table file formats, or on-disk data.
8. Do not redesign `LockManager`, add a generic lock plan, deadlock policy,
   lock lease, forced preparation cancellation, or automatic inactivity
   detection.
9. Do not modify the Phase 2/3 DDL scopes merely to share code with
   maintenance.
10. Do not expose a public runtime, task handle, cancellation API, gate type,
    or prepared-operation type.
11. Do not add a generic maintenance command registry, task group, child-task
    barrier, priority lane, dedicated pool, or adaptive scheduler.
12. Do not parallelize one freeze, checkpoint, cleanup, catalog scan, redo
    plan, or unlink workflow.
13. Do not add a blocking/CPU pool or redesign synchronous redo-file unlink in
    this phase.
14. Do not run fallible rollback, compensation, or storage cleanup from
    `Drop`, `finish`, or panic-policy methods.
15. Do not reinterpret ordinary errors as poison or weaken existing
    post-publication poison rules.
16. Do not change public method signatures, public outcome types, or error
    taxonomy.
17. Do not change `.config/nextest.toml`, introduce another test runner, or use
    sleeps as concurrency proof.
18. Do not perform RFC-0026 Phase 5's final stress, performance, scheduling
    policy, or RFC-0025 synchronization work.
19. Do not edit completed historical task documents.

## Plan

### 1. Make the public Session boundary explicit

Refactor the six effectful roots in `session.rs` to the established DDL
sequence:

```text
pin Maintenance operation
    -> clone mandatory runtime guard
    -> complete domain preparation
    -> await mandatory caller capacity
    -> synchronous consuming accept and detached spawn
    -> drop caller runtime guard
    -> await execution-inert CompletionObserver
```

Use task labels matching the public methods:

- `freeze_table`
- `checkpoint_table`
- `checkpoint_catalog`
- `checkpoint_catalog_and_truncate_redo_log`
- `truncate_redo_log`
- `cleanup_secondary_mem_indexes`

Use `MandatoryTaskMetadata::table_operation` for table-scoped work and
`MandatoryTaskMetadata::operation` for catalog/redo-wide work. Preserve all
current public result types. `checkpoint_catalog` continues mapping its
internal `CatalogCheckpointOutcome` to `()`.

Preparation that produces an ordinary no-effect freeze/checkpoint outcome may
return it directly after releasing the voluntary scope. It must not acquire a
mandatory permit merely to report `AlreadyFrozen` or a pre-effect lifecycle or
workflow cancellation.

### 2. Add a narrow maintenance operation scope

In `session.rs`, add maintenance-specific types rather than widening
`PreparedDdlLocks`, `PreparedDdlScope`, or `AcceptedDdlScope`.

`PreparedMaintenanceLocks` owns:

- `QuiescentGuard<LockManager>`;
- one `OwnerLockState` created from
  `SessionOperationPin::operation_lock_owner()`.

Its table constructor acquires, in order:

1. `LockResource::TableMetadata(table_id)` in `Shared`;
2. `LockResource::TableData(table_id)` in `IntentShared`.

Drop calls `OwnerLockState::release_all`. Partial acquisition and promoted but
unobserved grants remain cancellation-safe through existing lock-manager RAII.

`PreparedMaintenanceScope` owns:

- optional prepared table locks;
- the `SessionOperationPin`, declared after releasable resources so caller
  cancellation releases grants before publishing the voluntary terminal edge.

It exposes the exact key, engine/pool access needed during preparation, and a
synchronous `accept` method.

`AcceptedMaintenanceScope` owns:

- the transferred `MandatoryOperationGuard`;
- optional prepared locks;
- a small `Executing`, `TerminalReady`, or `FailedRetained` finish state.

It delegates `SessionRuntimeAccess`, exposes `begin_private_trx`, validates
`Mandatory(None)` with no transaction payload from `execute`, drops prepared
locks before normal outer terminal publication, and provides panic-minimal
`handle_panic`/`finish` behavior equivalent to the Phase 2/3 scope contract.

Accepted operation-specific carriers must drop table owners, workflow tokens,
domain gates, plans, and progress before calling
`AcceptedMaintenanceScope::finish`.

### 3. Prepare lifetime-free table runtime and workflow authority

Replace `ScopedTableRuntimeAccess` only for effectful mandatory table paths.
The owned preparation sequence is:

```text
prepare metadata S/data IS
    -> resolve authoritative current-live Arc<Table>
    -> cache the table in SessionState
    -> recheck engine health where current policy requires
    -> claim operation-specific reversible workflow/gate authority
    -> wait for mandatory capacity
```

Retain `ScopedTableRuntimeAccess` for `total_row_pages`, narrowing comments and
helpers to its remaining caller-owned observation role. Remove
`acquire_for_retry` if it becomes unused.

Add a lifetime-free table checkpoint root-mutation scope, named
`TableCheckpointRootMutationScope` unless an equally narrow existing naming
convention requires a minor adjustment. It retains `Arc<Table>`, records
active ownership, and releases through `TableLifecycle` on drop. Construction
uses the existing non-waiting lifecycle admission and returns the same
`CheckpointCancelReason`.

Add lifetime-free prepared freeze/checkpoint attempt tokens that retain
`Arc<Table>` and the exact reversible workflow payload needed for restoration.
Do not make `TableLifecycle` or `TableCheckpointWorkflow` globally
`Arc`-owned merely to manufacture `'static` borrowed leases.

Preserve current preparation order:

- freeze claims `Freezing`, then root mutation;
- checkpoint claims the idle/frozen `Checkpointing` source and exact batch,
  then root mutation.

If root-mutation admission fails, dropping the attempt restores the workflow
and exact frozen batch before table locks and the outer operation are released.
The prepared carrier declares attempt/root/table resources before its
maintenance scope so ordinary cancellation has the same ordering.

### 4. Run freeze and one-shot checkpoint as typed accepted operations

Keep the table-domain prepared/accepted carriers beside their workflows in
`table/persistence.rs`.

Freeze accepted execution consumes the prepared attempt and root authority,
then performs the existing scan, page loading, validation, page-state
publication, snapshot-fence allocation, and batch installation. The first page
state/fence effect remains after acceptance. Preserve `Frozen`,
`AlreadyFrozen`, and `Cancelled` results.

Checkpoint accepted execution receives the prepared checkpoint attempt and
root authority. Refactor `TableCheckpointer` so it no longer acquires root
mutation internally. Preserve:

- active-root and frozen-page delay checks;
- mutable-root construction;
- page transition preparation and irreversible transition;
- LWC, deletion, and secondary sidecar work;
- allocation reachability rebuild;
- root versus silent-watermark decision;
- publication admission;
- table root/route publication;
- system-transaction enqueue;
- existing error-to-poison conversion and workflow restoration.

`CheckpointPublishLease` remains execution-local. It is acquired
synchronously only at the existing transition/publication boundary after
reversible analysis. Accepted code must not call `LockManager`, wait for table
root-mutation admission, or reacquire prepared table access.

Keep progress required for panic diagnostics and cancellation-safe Drop outside
the unwind-caught future. Existing reversible attempt Drop and irreversible
`TableCheckpointer` poison behavior remain authoritative; mandatory panic
handling adds outer `FailedRetained` ownership rather than replacing the
checkpoint domain policy.

### 5. Split retry orchestration into independent roots

Implement `checkpoint_table_with_wait` as a caller loop over the public
one-shot attempt:

```text
loop:
    outcome = checkpoint_table(table_id).await
    Delayed(reason):
        wait_for_checkpoint_retry(reason).await
        continue
    Published/Cancelled:
        return outcome
```

The one-shot observer must be consumed before entering the wait, proving that
the accepted task has released its permit and terminal resources. The retry
wait uses `SessionObserverPin`, retains no operation entry, and remains
cancellable. A later attempt allocates a new operation key and prepares a
fresh complete authority scope.

Remove `wait_for_checkpoint_retry_in_operation` and retry-only scoped access
when unused. Keep the existing listener-before-recheck protocol, table
lifecycle/poison/shutdown handling, and runtime-free
`DetachedCheckpointRetryWait`.

### 6. Add owned catalog-checkpoint and redo-retention scopes

In the catalog domain, add `CatalogCheckpointScope`:

- retains a `QuiescentGuard<Catalog>`;
- owns active catalog-checkpoint admission without a borrowed lease;
- exposes an idempotent, non-panicking release used before redo unlink;
- releases active admission on Drop.

In the transaction-system domain, add `RedoRetentionScope`:

- retains a `QuiescentGuard<TransactionSystem>`;
- owns active redo-retention admission without a borrowed lease;
- remains active through retained-redo scan, plan, marker publication, and
  obsolete-file cleanup;
- releases active admission on Drop.

Keep the existing low-level gate algorithms and fairness behavior. Refactor
them to support both the new owned scope and any retained test-only borrowed
helper without duplicating gate state.

Every production Session preparation acquires the catalog scope first and redo
scope second. Dropping while the second acquisition is pending releases the
catalog scope. No mandatory capacity is requested until both are active.

### 7. Move catalog checkpoint execution behind prepared authority

Place `PreparedCatalogCheckpointOperation` and
`AcceptedCatalogCheckpointOperation` in `catalog/checkpoint.rs` (use an
equivalent unambiguous suffix if needed to avoid the existing storage-layer
`PreparedCatalogCheckpoint` enum).

Split `Catalog::checkpoint_now` into:

- an execution core requiring prepared catalog and redo scopes;
- a test-only/direct convenience wrapper, if still required, that acquires
  local scopes before calling the same core.

The production accepted call must not invoke `begin_checkpoint` or
`begin_redo_retention`. Preserve durable-upper sampling, redo scanning,
checkpoint batch application, catalog-safe retention progress, dropped-table
purge requests, IO-source poison conversion, logging, and the public `()`
result.

### 8. Move redo truncation and combined maintenance behind prepared authority

Place prepared/accepted carriers in `trx/retention.rs` for:

- standalone redo truncation;
- combined catalog checkpoint and redo truncation.

Refactor execution cores to require mutable access to both prepared scopes.
Preserve the current projected-state combined plan, including:

- checkpointed silent-watermark projection;
- pending dropped-table floor filtering;
- catalog-safe segment proof merge;
- marker-only versus combined-root publication;
- final retention-progress refresh;
- blocker reporting.

After successful root/marker publication, explicitly release
`CatalogCheckpointScope` before calling obsolete-file cleanup.
`RedoRetentionScope` remains in the accepted carrier until cleanup and result
construction finish. Non-`NotFound` unlink failures remain counted and
retryable rather than becoming operation failure or poison.

### 9. Retain MemIndex cleanup transactions outside the caught future

Place `PreparedMemIndexCleanupOperation` and
`AcceptedMemIndexCleanupOperation` in `table/gc.rs`.

The prepared carrier owns the exact live table and maintenance scope.
The accepted carrier adds progress with:

- table id and `clean_live_entries`;
- current phase;
- optional active `Transaction`;
- any immutable snapshot-independent inputs needed for diagnostics.

Refactor the cleanup loop so a newly begun private transaction is installed in
accepted progress before the first await that uses it. Each iteration:

1. begins a nested transaction through `AcceptedMaintenanceScope`;
2. stores it in progress;
3. checks out the core and captures the proof-bound root/layout snapshot;
4. performs cleanup or detects a newer-root retry;
5. drops proof-bound state and checkout;
6. takes and rolls back the stored transaction;
7. starts a fresh transaction only after the previous nested state returned to
   `Mandatory(None)`.

Normal completion calls the existing result/rollback precedence helper.
Unexpected unwind calls outer `handle_panic` while the transaction is still
represented by progress/entry ownership. Do not initiate fallible rollback
from panic handling or Drop. The exact entry retains any unresolved nested
core as `FailedRetained`.

### 10. Preserve supervision and resource-release ordering

Every operation-specific `AcceptedExecution` follows:

```text
execute:
    run domain workflow
    -> settle nested private/system obligations
    -> assert Mandatory(None)
    -> mark terminal-ready

finish:
    drop domain progress/plans
    -> release table workflow/root or catalog/redo scopes
    -> drop exact Arc<Table> owners
    -> release logical locks
    -> publish outer Terminal

panic:
    domain Drop/preservation handles reversible or irreversible state
    -> publish outer FailedRetained
    -> return CompletionErrorBridge(MandatoryTaskPanic)
    -> runtime publishes poison and completion
    -> safe RAII gates/locks release
    -> permit releases last
```

Only `execute` may assert or return fallible domain results. `finish`,
operation-specific panic handlers, and resource Drop implementations must be
idempotent or single-owner, panic-minimal, and non-fallible.

Ordinary typed `RuntimeOrFatalResult` values cross the completion boundary once
through `CompletionErrorBridge::capture_runtime_or_fatal`. Do not materialize
and recapture a public error or completion bridge.

### 11. Replace affected thread-local test controls

Add an engine-scoped `MaintenanceTestController` under `#[cfg(test)]`, following
the table/index DDL controllers. It must use shared `Arc` state, short
`parking_lot::Mutex` critical sections, and `flume`/event endpoints that can be
installed by the caller thread and reached by a mandatory runner.

Convert every fault flag or coordination hook consulted by the migrated
`Table::freeze`, `Table::checkpoint`, and
`Table::cleanup_secondary_mem_indexes` call graphs. Explicitly cover phases
for:

- preparation after partial/complete logical locks;
- workflow/root authority ready;
- waiting for runtime capacity;
- accepted before first effect;
- freeze page publication;
- checkpoint transaction/construction start;
- publication admission;
- root or silent-watermark mutation;
- system commit/enqueue;
- cleanup private transaction and retry;
- catalog/redo gates ready;
- catalog root/marker publication;
- catalog-gate release;
- before and after unlink;
- final resource release;
- injected execution panic.

Thread the controller from `EngineRef` through test-only arguments or helpers;
do not add production fields or widen production interfaces solely for tests.
Low-level hooks that are not consulted by a migrated runtime path may remain
local. The caller-owned `total_row_pages` hook remains unchanged.

### 12. Audit Send, polling, documentation, and phase contracts

Require every prepared/accepted carrier, output, progress value, hook endpoint,
and transferred resource to be `Send + 'static`. Remove `Rc`, `RefCell`,
thread-affine assumptions, or borrowed guards from accepted ownership.

Audit loops touched by the migration for finite, nonblocking polls. Existing
IO/event awaits and bounded index batches remain the primary yield points. Add
simple `runtime::yield_now()` only at a natural safe boundary when a migrated
loop can otherwise perform materially unbounded CPU work in one poll; do not
hold a blocking mutex, latch, page-state lock, or catalog entry across that
yield.

Update living documentation:

- `docs/architecture.md`: list effectful maintenance beside DDL as mandatory;
- `docs/checkpoint.md`: describe prepared table authority and independent
  retry attempts;
- `docs/transaction-system.md`: replace voluntary maintenance/private
  transaction and same-operation retry statements;
- `docs/lock-system.md`: replace borrowed scoped maintenance ownership for
  effectful paths while retaining `total_row_pages`;
- `docs/engine-component-lifetime.md`: document transferred table/catalog/redo
  gates and maintenance release order.

At `$task-resolve`, synchronize RFC-0026 Phase 4:

- set `Task Doc` to this file;
- record the created task issue;
- set phase status and implementation summary;
- record focused/workspace/alternate-backend validation;
- confirm the Phase 5 prerequisite without changing its semantic scope.

Do not edit RFC-0025 in this task; RFC-0026 Phase 5 owns final supersession
synchronization.

### Risks and safeguards

1. **Longer preparation gate holds:** workflow, logical-lock, catalog, or redo
   authority may be held while capacity is saturated. This is the RFC-approved
   tradeoff. Accepted tasks acquire none of those resources, preventing a
   permit/resource wait cycle.
2. **Workflow restoration regression:** lifetime-free token Drop could restore
   the wrong state or lose a frozen batch. Preserve exact source/batch identity
   and test cancellation after each acquisition boundary.
3. **Nested cleanup loss on panic:** keeping the private transaction local to
   the caught future could release authority out of order. Store it in accepted
   progress before use and retain the stable entry on panic.
4. **Publication policy drift:** moving gates must not move the existing fatal
   boundary. Keep domain progress/Drop policy authoritative and test both
   reversible and post-publication failures.
5. **Cross-thread test blind spots:** old thread-local hooks may silently stop
   firing. Inventory every hook reached from migrated paths and prove caller
   installation is observed on a mandatory runner.
6. **DDL regression:** new maintenance scopes must not alter DDL types,
   prepared catalog authority, metadata-gate ordering, or successful
   transaction/statement hot paths.

## Implementation Notes

## Impacts

Primary production modules and interfaces:

- `doradb-storage/src/session.rs`
  - public maintenance wrappers;
  - `PreparedMaintenanceLocks`;
  - `PreparedMaintenanceScope`;
  - `AcceptedMaintenanceScope`;
  - narrowed `ScopedTableRuntimeAccess`;
  - removal of operation-owned retry helper.
- `doradb-storage/src/table/mod.rs`
  - owned table checkpoint root-mutation entry/release surface;
  - test-control plumbing where required.
- `doradb-storage/src/table/lifecycle.rs`
  - shared internal acquisition/release primitives supporting borrowed and
    lifetime-free root-mutation ownership.
- `doradb-storage/src/table/checkpoint_workflow.rs`
  - lifetime-free prepared freeze/checkpoint attempts and exact restoration.
- `doradb-storage/src/table/persistence.rs`
  - freeze/checkpoint prepared and accepted carriers;
  - pre-acquired `TableCheckpointer` authority;
  - migrated fault/phase controls.
- `doradb-storage/src/table/gc.rs`
  - MemIndex cleanup prepared/accepted carriers and accepted progress.
- `doradb-storage/src/catalog/mod.rs`
  - owned catalog-checkpoint admission surface.
- `doradb-storage/src/catalog/checkpoint.rs`
  - `CatalogCheckpointScope`;
  - prepared/accepted catalog checkpoint execution.
- `doradb-storage/src/trx/sys.rs`
  - owned redo-retention admission surface.
- `doradb-storage/src/trx/retention.rs`
  - `RedoRetentionScope`;
  - prepared/accepted truncation and combined maintenance;
  - prepared-scope execution cores and early catalog release.
- `doradb-storage/src/engine.rs`
  - `#[cfg(test)]` maintenance controller only.
- `doradb-storage/src/runtime/mandatory.rs`
  - existing traits and metadata are reused; production changes are expected
    only if a narrow diagnostic constructor is demonstrably needed.

Public APIs and outcomes remain source-compatible:

- `FreezeOutcome`
- `CheckpointOutcome`
- `CatalogRedoMaintenanceOutcome`
- `RedoTruncationOutcome`
- `MemIndexCleanupOutcome`

Living documentation impacts:

- `docs/architecture.md`
- `docs/checkpoint.md`
- `docs/transaction-system.md`
- `docs/lock-system.md`
- `docs/engine-component-lifetime.md`

No file format, recovery format, configuration, public error, or dependency
change is expected.

## Test Cases

All concurrency tests must establish semantic predicates with channels,
events, barriers, production wait APIs, and rechecks. Timeouts are hang
watchdogs only; sleeps do not establish progress.

1. Construct but do not poll each migrated public future. Assert no operation
   id, session entry, lock waiter/grant, workflow state, maintenance gate,
   mandatory permit, task, transaction, file mutation, or marker change.
2. Drop table preparation while metadata `S` is pending, after metadata `S`,
   while data `IS` is pending, and after both grants. Assert each exact claim
   and waiter releases once and the session becomes idle.
3. Prepare freeze/checkpoint through workflow claim and root-mutation
   ownership, block capacity, then drop. Assert root authority releases and the
   exact idle/frozen workflow and batch are restored before table locks.
4. Retain an unpolled prepared table future while capacity is unavailable.
   Assert `try_shutdown` reports voluntary preparation and zero additional
   caller permits; dropping the future unblocks shutdown.
5. Return `AlreadyFrozen` and pre-effect lifecycle/workflow cancellation
   without mandatory submission or permit acquisition.
6. Saturate mandatory capacity with an accepted task, fully prepare each
   maintenance family, and prove no capacity is requested until every required
   authority is active.
7. Release capacity and prove synchronous acceptance moves the exact entry and
   every resource once, with no await or observable release/reacquire gap.
8. Gate accepted freeze before first effect, during page loading, during page
   state publication, and before final release. Drop the observer at each gate
   and assert the same final `FreezeOutcome`, workflow, page state, lock state,
   session terminal, and permit release.
9. Cover table checkpoint `ActiveRoot` and `FrozenPageCutoff` delays,
   lifecycle cancellation, real root publication, silent-watermark
   publication, and system-commit acceptance after migration.
10. Drop checkpoint observers before analysis, after root mutation, after
    publication admission, after root/silent mutation, and before system
    enqueue. Assert accepted work continues and resources release only after
    terminal proof.
11. Inject ordinary errors before publication and verify exact attempt/batch
    restoration without poison.
12. Inject existing post-transition, post-root, silent-watermark, and
    system-commit failures. Assert existing fatal reason/poison behavior and no
    false successful terminal result.
13. Prove accepted table execution contains no `LockManager` call and no
    checkpoint root-mutation acquisition by gating every acquisition before
    acceptance and failing a test on any post-acceptance call.
14. Produce a delayed `checkpoint_table_with_wait` attempt. Assert its
    operation is `Terminal`, its permit count is zero, its table/runtime/workflow
    owners and logical locks are gone, and only then the retry listener blocks.
15. While that retry listener waits, complete same-table DROP or make the
    reason obsolete. Assert the waiter wakes and returns without retaining a
    stale table.
16. Allow a retry and assert the next checkpoint attempt receives a different
    operation key and freshly acquires its complete authority.
17. Drop `checkpoint_table_with_wait` during the standalone wait. Assert no
    operation, lock, runtime permit, or workflow authority remains.
18. Start catalog preparation behind active catalog metadata change, active
    catalog checkpoint, and active redo retention. Drop at each partial stage
    and assert earlier scopes release without a task or permit.
19. For catalog checkpoint, prove catalog and redo scopes are acquired before
    acceptance, retained through scan/apply, and released before outer terminal
    publication.
20. Preserve catalog checkpoint publish/no-op behavior, durable upper
    sampling, retention-progress refresh, and dropped-table purge request.
21. Preserve standalone redo truncation candidate/blocker results, marker
    advancement, no-candidate outcome, missing-file accounting, retryable
    unlink failure accounting, and later successful retry.
22. Preserve combined checkpoint/truncation projected silent-watermark and
    pending-drop floor behavior for checkpoint-publish and no-op branches.
23. Gate truncation after root/marker publication and before unlink. Assert the
    catalog scope is released, another catalog checkpoint may prepare, and the
    redo-retention scope still excludes a competing retained-redo scan or
    truncation.
24. Drop catalog/redo observers at scan, prepare, publish, marker, and unlink
    phases. Assert execution reaches the same result and releases both scopes
    in order.
25. Gate MemIndex cleanup immediately after private transaction installation.
    Assert the stable entry is
    `Mandatory(Some(InternalTrxState::Available|Running))` and the transaction
    is retained by accepted progress.
26. Force a root race, settle the first cleanup transaction, and assert the
    retry starts a fresh `TrxID` only after the entry returned to
    `Mandatory(None)`.
27. Preserve live-entry delay, delete-overlay cleanup, per-index statistics,
    and cleanup-versus-rollback error precedence.
28. Drop the cleanup observer during transaction use, scan, retry, rollback,
    and final release. Assert no cleanup or nested transaction is abandoned.
29. Inject a panic in representative reversible and irreversible table,
    cleanup-private-transaction, catalog publication, marker, and unlink
    phases. Assert domain preservation runs, the outer entry becomes
    `FailedRetained`, engine poison publishes, the observer receives a fatal
    runtime result or detaches safely, safe scopes/locks release, the permit
    releases, and a runner remains available.
30. Prove normal completion drops workflow/gate/table resources before
    maintenance locks and drops locks before publishing the outer terminal
    edge.
31. Preserve separate exact maintenance claims under covering same-session
    explicit table locks and release only the maintenance claims.
32. Keep `total_row_pages` caller-owned: it uses no mandatory permit, remains
    cancellable at its scoped access hook, and continues blocking same-table
    DROP until its runtime owner releases.
33. Keep checkpoint/GC/purge progress waits observer-only and admissible while
    another transaction operation is active.
34. Assert `try_shutdown` separately identifies voluntary maintenance
    preparation and accepted mandatory maintenance. Blocking shutdown must
    drain observer-dropped accepted work without cancellation.
35. Run focused tests with `worker_threads=1` to prove cooperative progress and
    with multiple runners to prove unrelated cleanup/maintenance progress.
36. Re-run Phase 1 mandatory-runtime, Phase 2 table-DDL, Phase 3 index-DDL,
    transaction cleanup, session close/abandonment, table drop, checkpoint,
    recovery, catalog retention, and redo truncation tests.
37. Run the authoritative workspace validation:

    ```bash
    rtk cargo nextest run --workspace
    ```

38. Because this phase touches table/catalog publication and storage IO, run
    the alternate backend:

    ```bash
    rtk cargo nextest run -p doradb-storage --no-default-features --features libaio
    ```

39. Run formatting, strict lint, and the task-resolution style gate:

    ```bash
    rtk cargo fmt --check
    rtk cargo clippy --workspace --all-targets -- -D warnings
    tools/style_audit.rs
    ```

No `.config/nextest.toml` or timeout-policy change is expected.

## Open Questions

No design-blocking questions remain.

At `$task-resolve`, record any evidence-based follow-up as a backlog item,
synchronize RFC-0026 Phase 4's task document, issue, status, implementation
summary, and validation results, and confirm that Phase 5's prerequisites
remain satisfied. Do not broaden this task with Phase 5 scheduling,
observability, stress, or benchmark work.
