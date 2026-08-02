---
id: 000250
title: Runtime-Owned Index DDL
status: implemented  # proposal | implemented | superseded
created: 2026-08-02
github_issue: 926
---

# Task: Runtime-Owned Index DDL

## Summary

Implement Phase 3 of RFC-0026 by moving accepted `CREATE INDEX` and
`DROP INDEX` execution from the caller executor to the engine-owned mandatory
runtime. Keep caller-owned preparation in `session.rs`: validate pure public
input, reserve one DDL session operation, reject conflicting explicit session
locks, acquire the complete target and catalog logical-lock set, resolve and
validate the authoritative table/index state, acquire both metadata-change
gates, capture the stable layout/root plan, and wait for mandatory capacity
while the full preparation remains cancellable.

After acceptance, make the mandatory runtime the sole owner of hot/cold row
collection, index construction, the nested private catalog transaction,
catalog commit, table-root publication, runtime-layout and catalog-history
publication, retired-index cleanup, result completion, and panic/error
supervision. Accepted execution must not reacquire an operation lock or a
metadata-change gate. Dropping the public future or completion observer after
acceptance must be semantically inert.

Generalize the Phase 2 table-DDL lock/operation scope for reuse by index DDL,
but keep fixed operation-specific constructors instead of adding a generic
lock-plan abstraction. Represent the two index-specific metadata gate
admissions with one lifetime-free `IndexDdlGateScope`; do not introduce types
named `OwnedTableMetadataChangeLease` or
`OwnedCatalogMetadataChangeLease`. The scope does not own metadata. It owns
the active table and catalog gate admissions and retains the resource handles
needed to release those admissions.

Replace the current split runtime-layout/catalog-history update with one
catalog-coordinated volatile publication boundary. Metadata-history purge must
observe either the old metadata/layout pointer pair or the new pair, never the
transient old-history/new-layout pair that can currently panic. Preserve the
existing durable catalog-commit and table-root proof protocol and its
precommit rollback/postcommit poison boundary.

Preserve the current index-build data structures and algorithms:
all-row `Vec<CreateIndexRowEntry>` collection, `sort_unstable_by`, unique-key
validation, existing DiskTree batch writers, MemIndex population, root shape,
and memory behavior. Add only simple cooperative yields at natural loops or
batch boundaries already controlled by index DDL. Streaming, bounded-memory,
parallel, or incrementally ordered construction remains backlog `000104`.

## Context

`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Parent RFC:`
`- docs/rfcs/0026-engine-owned-mandatory-background-runtime.md`

This task is RFC-0026 Phase 3, **Runtime-Owned Index DDL**. Phase 1 established
the mandatory operation driver, bounded caller admission, typed completion,
panic supervision, and concurrent cleanup. Phase 2 is complete through task
`000249` and issue `#924`; it established the production pattern that this
phase must reuse:

- caller-prepared and runtime-accepted DDL execution carriers;
- a lifetime-free logical-lock scope based on `QuiescentGuard<LockManager>`
  and `OwnerLockState`;
- a synchronous, move-once `PreparedExecution -> AcceptedExecution` handoff;
- `PreparedCatalogWriteAuthority` and
  `Transaction::stage_prepared_catalog_statement`, which stage catalog writes
  without hidden logical-lock acquisition;
- nested private transactions owned by `MandatoryOperationGuard`;
- completion/error bridging, panic retention, engine poison, and final
  resource-release ordering;
- engine-scoped, thread-neutral test control across caller and runtime threads.

The Phase 3 prerequisite remains an implementation gate: the focused Phase 2
table-DDL acceptance, observer-drop, panic, compensation, and shutdown tests
must continue to pass before index DDL integration is considered complete.
The index plans, progress state, error payloads, completion output, and test
hooks must all be `Send` and lifetime-free before mandatory submission.

The current `Session::create_index` and `Session::drop_index` paths pin a DDL
operation and call the monolithic `create_index_for_session` and
`drop_index_for_session` functions in `catalog/index.rs`. Those functions run
the complete workflow on the caller executor. They construct a
`SessionDdlContext`, acquire borrowed target DDL guards, resolve the live table,
and hold borrowed table/catalog metadata-change leases while building and
publishing the index.

Current CREATE INDEX:

1. rejects user primary-key attributes and non-user table IDs;
2. rejects a conflicting explicit lock owned by the same session;
3. acquires target table metadata/data exclusion;
4. validates the table and acquires table and catalog metadata-change gates;
5. captures the old layout and active root, allocates an index number, and
   constructs new metadata;
6. starts a private transaction;
7. collects all cold rows into a vector, sorts and validates them, builds the
   staged DiskTree, collects hot rows, and builds the staged MemIndex;
8. stages writes to `catalog.tables`, `catalog.indexes`, and
   `catalog.index_columns`, commits, and publishes the table root;
9. installs the runtime layout, separately publishes catalog history, and
   requests metadata-history purge.

Current DROP INDEX performs the corresponding target/slot/root validation,
starts a private transaction, deletes `catalog.index_columns` and
`catalog.indexes`, commits, publishes the root, separately installs/publishes
the layout/history transition, and attempts retired-index cleanup.

The catalog statement helpers currently call
`Transaction::stage_catalog_statement`. That helper acquires catalog-table
metadata `S` and data `IX` locks inside the accepted call graph. These hidden
awaits are incompatible with RFC-0026: every required operation lock must be
prepared before capacity admission and transferred atomically at acceptance.

The existing `TableMetadataChangeLease<'_>` and
`CatalogMetadataChangeLease<'_>` borrow their gate owners. They cannot cross
the mandatory runtime's `'static` task boundary. The required adapter is
narrower than a new metadata ownership model: index preparation needs one
transferable scope that retains an `Arc<Table>`, a catalog quiescent guard, and
proof that both existing gate state machines are active. Dropping that scope
must release the catalog gate and then the table gate without awaiting,
asserting, or performing compensation.

The current volatile index publication is not atomic relative to
metadata-history purge. Both create and drop call
`Table::install_runtime_layout`, release the table layout mutex, and only then
acquire the `Catalog::user_tables` entry through
`Catalog::publish_user_table_metadata`. A purge can acquire that entry in the
gap. `UserTableEntry::purge_history` validates that current catalog metadata is
pointer-identical to `Table::layout_snapshot().metadata_arc()`, so the
old-history/new-layout pair can panic. Backlog `000174` records this race as
well as a separate component-shutdown panic-safety defect.

The approved atomicity domain is precise: this task makes the final volatile
layout/history pair atomic relative to operations serialized by the
user-table catalog entry, especially metadata-history purge. It does not
replace the durable catalog-commit/table-root protocol with one hardware
atomic operation. Runtime layout readers continue to receive immutable
`Arc<TableRuntimeLayout>` snapshots; old readers retain the old layout and new
readers receive the new layout at the existing layout swap.

The current build intentionally materializes all cold rows and, for unique
indexes, all keys needed for cold/cold and cold/hot duplicate validation.
Backlog `000104` owns the streaming, bounded-memory, disk-backed uniqueness,
and optional parallel-build redesign. The approved Phase 3 scope does not
replace the row vector with an ordered map, change sorting, introduce a build
budget framework, or refactor the DiskTree construction algorithm. Simple
`runtime::yield_now()` calls may be added at safe natural boundaries in loops
already touched by this migration, but this task does not claim a strict
single-runner poll bound inside opaque synchronous primitives such as
`sort_unstable_by`. Practical cleanup progress is validated with the normal
multi-runner mandatory runtime.

RFC-0026 Phase 4 assumes that Phase 2 and Phase 3 provide production examples
for runtime-owned effectful workflows, prepared authority, deterministic
cross-thread hooks, observer-drop inertness, panic retention, and cooperative
execution. This task preserves that assumption. It does not change Phase 4's
maintenance scope or prerequisites.

Relevant design, process, implementation, and backlog sources:

- `docs/rfcs/0026-engine-owned-mandatory-background-runtime.md`
- `docs/tasks/000248-mandatory-operation-driver-and-concurrent-cleanup-executor.md`
- `docs/tasks/000249-runtime-owned-table-ddl.md`
- `docs/rfcs/0018-create-drop-index.md`
- `docs/backlogs/000104-stream-parallel-create-index-cold-build.md`
- `docs/backlogs/000174-atomic-index-metadata-publication-and-panic-safe-shutdown.md`
- `docs/architecture.md`
- `docs/index-design.md`
- `docs/secondary-index.md`
- `docs/transaction-system.md`
- `docs/engine-component-lifetime.md`
- `docs/lock-system.md`
- `docs/table-file.md`
- `docs/process/coding-guidance.md`
- `docs/process/unit-test.md`
- `.config/nextest.toml`
- `doradb-storage/src/runtime/mandatory.rs`
- `doradb-storage/src/session.rs`
- `doradb-storage/src/catalog/index.rs`
- `doradb-storage/src/catalog/table.rs`
- `doradb-storage/src/catalog/{mod,history,checkpoint}.rs`
- `doradb-storage/src/table/{mod,lifecycle}.rs`
- `doradb-storage/src/trx/{mod,stmt}.rs`
- `doradb-storage/src/index/disk_tree.rs`
- `doradb-storage/src/engine.rs`

## Goals

1. Split public CREATE/DROP INDEX at the first operation-effect boundary into
   caller-owned preparation and mandatory-runtime-owned accepted execution.
2. Keep preparation orchestration in `session.rs` and index-specific plans,
   accepted execution, progress, compensation, and invariants in
   `catalog/index.rs`.
3. Validate pure public input before operation reservation, including
   non-user table IDs and user-supplied primary-key attributes.
4. Acquire every target and catalog logical lock needed by the complete
   accepted call graph before mandatory capacity admission.
5. Retain exact partial preparation state so any error, rejected admission, or
   caller drop releases all granted locks and active/pending metadata gates.
6. Reuse the Phase 2 prepared catalog-write authority so accepted CREATE/DROP
   INDEX performs no `LockManager` acquisition.
7. Introduce one clearly named `IndexDdlGateScope` for transferable table and
   catalog metadata-gate admission, without implying ownership of metadata.
8. Ensure accepted execution performs no metadata-gate wait or reacquisition.
9. Start the private catalog transaction only after acceptance and settle or
   retain it before releasing gate/lock authority.
10. Move hot/cold collection, existing index construction, catalog mutation,
    commit, root publication, layout/history publication, retirement, and
    cleanup under mandatory runtime ownership.
11. Preserve the existing CREATE precommit rollback and staged-runtime destroy
    behavior, and the existing postcommit poison boundary.
12. Preserve the existing DROP precommit rollback, postcommit poison,
    retired-runtime retention, and best-effort cleanup behavior.
13. Make completion-observer or public-future drop after acceptance
    semantically inert at every execution phase.
14. Keep progress and operation resources outside the unwind-caught execution
    future so the supervisor can publish retained failure, poison, and release
    only safe operation resources.
15. Replace split layout/history publication with one catalog-entry-serialized
    transition that exposes only old/old or new/new metadata pointer pairs to
    purge.
16. Preserve pointer-identity validation; do not weaken or remove
    `assert_current_layout_metadata`.
17. Preserve the current index build's row selection, key ordering, duplicate
    detection, root construction, index slot allocation, sparse slots, memory
    model, and recovery proof.
18. Add only low-complexity cooperative yields at safe natural boundaries
    without holding blocking mutexes, latches, or catalog entry guards.
19. Replace thread-local index failure/test controls with engine-scoped,
    thread-neutral deterministic controls suitable for cross-thread accepted
    execution.
20. Prove preparation cancellation, capacity saturation, accepted ownership,
    absence of hidden awaits, failure/panic policy, atomic publication, cleanup
    progress, and shutdown drain under focused tests.
21. Keep Phase 2 table DDL behavior unchanged while generalizing its common
    scope names and helpers for index use.
22. Preserve the production prerequisites that RFC-0026 Phase 4 consumes.

## Non-Goals

1. Do not redesign index formats, key encoding, DiskTree or MemIndex
   algorithms, persistent roots, redo markers, crash recovery, or index-slot
   semantics.
2. Do not replace `Vec<CreateIndexRowEntry>`, `sort_unstable_by`, the current
   duplicate scans, existing batch writers, or current all-row build memory
   model.
3. Do not implement streaming, bounded-memory, disk-backed duplicate
   validation, parallel collection/build, dispatcher/worker pipelines, or
   build-performance optimization. Those remain backlog `000104`.
4. Do not introduce an incremental ordered accumulator, generic poll-budget
   framework, budget-aware DiskTree variant, or strict per-element scheduling
   contract.
5. Do not claim a strict one-runner yield bound inside unchanged synchronous
   sorting or DiskTree helpers. Validate cleanup progress with the production
   multi-runner configuration.
6. Do not add scheduler priority lanes, adaptive capacity, task groups,
   domain-specific DDL workers, or child-task ownership protocols.
7. Do not migrate checkpoint, redo truncation, general index cleanup,
   transaction purge, or other maintenance work; those remain later RFC-0026
   phases.
8. Do not redesign `LockManager`, introduce exact-family locking or deadlock
   detection, or add a generic caller-supplied lock-plan API. Backlog `000171`
   remains separate.
9. Do not begin or transfer a private transaction before mandatory capacity
   and acceptance.
10. Do not change the public Session index API or public error taxonomy.
11. Do not add redundant catalog-row or runtime validation after the
   authoritative preparation snapshot is protected by the complete scope.
12. Do not remove or weaken pointer-identity validation to avoid the
   publication race.
13. Do not implement the component-shutdown panic-safety half of backlog
   `000174`; leave that backlog open and record the partial outcome during
   task resolution.
14. Do not add awaits, production blocking barriers, or fallible compensation
   inside the layout/history publication critical section.
15. Do not run fallible rollback or runtime destruction from `Drop`,
   `handle_panic`, or another arbitrary-unwind path.
16. Do not change successful transaction/statement hot paths beyond
   generalizing comments or prepared-authority coverage needed by accepted
   index DDL.
17. Do not change `.config/nextest.toml`, replace cargo-nextest, or add a
   second timeout policy.
18. Do not modify completed task documents.

## Plan

### 1. Generalize the Phase 2 DDL lock and operation scope

Rename the table-specific common scope shapes in `session.rs` to reflect that
they now serve both table and index DDL:

```rust
pub(crate) struct PreparedDdlLocks {
    lock_manager: QuiescentGuard<LockManager>,
    locks: OwnerLockState,
}

pub(crate) struct PreparedDdlScope {
    locks: PreparedDdlLocks,
    operation: SessionOperationPin,
}

pub(crate) struct AcceptedDdlScope {
    operation: MandatoryOperationGuard,
    locks: Option<PreparedDdlLocks>,
    finish_state: DdlFinishState,
}
```

Final names may follow established local style, but they must not remain
semantically table-only once index DDL depends on them. Preserve all Phase 2
table constructors, acquisition sequences, release semantics, completion
checks, and tests.

Add fixed index-specific preparation constructors rather than a generic lock
list supplied by callers. CREATE INDEX prepares:

```text
target TableMetadata X
catalog.tables TableMetadata S
catalog.indexes TableMetadata S
catalog.index_columns TableMetadata S
target TableData X
catalog.tables TableData IX
catalog.indexes TableData IX
catalog.index_columns TableData IX
```

DROP INDEX prepares:

```text
target TableMetadata X
catalog.indexes TableMetadata S
catalog.index_columns TableMetadata S
target TableData X
catalog.indexes TableData IX
catalog.index_columns TableData IX
```

Catalog targets must be in ascending `TableID` order. The target metadata lock
precedes every data lock, and the target data lock precedes catalog data locks,
matching the established canonical metadata-before-data order.

Record each grant immediately in `OwnerLockState`. Cancelling the current lock
wait removes its waiter; dropping later partial preparation releases all
earlier grants. `PreparedDdlLocks::drop` remains infallible, non-async, and
non-panicking.

Keep `PreparedCatalogWriteAuthority` as a borrowed proof over the retained
`OwnerLockState`. Generalize table-specific comments and focused tests only as
needed to cover accepted index catalog writes.

### 2. Prepare CREATE INDEX entirely on the caller

Refactor `Session::create_index` into:

```text
reject IndexAttributes::PK and non-user table id
    -> reserve SessionOperationKind::Ddl
    -> clone mandatory-runtime access
    -> reject same-session explicit target lock
    -> acquire fixed CREATE INDEX logical-lock scope
    -> resolve and validate exact current-live Arc<Table>
    -> acquire IndexDdlGateScope
    -> capture old layout and active root
    -> validate root/layout shape
    -> allocate table-local index number in new metadata
    -> build owned CreateIndexPlan
    -> construct PreparedCreateIndex
    -> await mandatory caller capacity
    -> synchronously accept and spawn
    -> drop caller runtime guard
    -> await typed completion observer
```

Pure user-PK and table-ID rejection must happen before the DDL operation pin
and therefore before locks, gates, or capacity. Do not create a mutable table
file, index runtime, private transaction, catalog statement, or MVCC mutation
during preparation.

After target locks are held, call the existing authoritative validation once
and retain that exact `Arc<Table>`. Acquire the table metadata gate before the
catalog metadata gate, then capture the old immutable layout, metadata, active
root, new metadata, allocated index number, new index spec, and resized root
slot plan while both gates remain active. All plan fields must be owned and
`Send`.

Capacity waiting retains the complete logical-lock and metadata-gate scope but
must not create a mandatory task or consume a permit until atomic acceptance.
Dropping at capacity releases the entire scope through ordinary RAII.

### 3. Prepare DROP INDEX entirely on the caller

Refactor `Session::drop_index` into:

```text
reject non-user table id
    -> reserve SessionOperationKind::Ddl
    -> clone mandatory-runtime access
    -> reject same-session explicit target lock
    -> acquire fixed DROP INDEX logical-lock scope
    -> resolve and validate exact current-live Arc<Table>
    -> acquire IndexDdlGateScope
    -> capture old layout and active root
    -> validate active metadata/runtime/root slot
    -> construct new metadata and DropIndexPlan
    -> construct PreparedDropIndex
    -> await mandatory caller capacity
    -> synchronously accept and spawn
    -> drop caller runtime guard
    -> await typed completion observer
```

Return the existing ordinary not-found error when the requested metadata slot
is inactive. Treat a metadata/runtime/root mismatch after exclusion as the
existing invariant class. Do not repeat target lookup or acquire fresh
authority after acceptance.

Remove `SessionDdlContext` after both index paths migrate; Phase 2 deliberately
retained it only for Phase 3. Keep it only if a remaining production caller is
identified and document that caller during implementation.

### 4. Add one transferable index metadata-gate scope

Introduce one crate-private `IndexDdlGateScope` used only by index
preparation/accepted execution. Its conceptual state is:

```rust
pub(crate) struct IndexDdlGateScope {
    table: Arc<Table>,
    catalog: QuiescentGuard<Catalog>,
    // Private proof/state that both existing metadata-change gates are active.
}
```

The exact private representation may use narrow table/catalog gate tokens, but
the task must not expose or document separately named
`OwnedTableMetadataChangeLease` or
`OwnedCatalogMetadataChangeLease`. Those names imply metadata ownership rather
than transferable gate admission.

Factor each existing gate acquisition state machine so caller cancellation
still releases a pending reservation and successful acquisition can be moved
into the lifetime-free combined scope. Reuse the current `Open -> Pending ->
Active -> Open` behavior, notifications, liveness checks, and checkpoint
exclusion. Do not duplicate the state machine, introduce unsafe
self-references, allocate the gate separately from its owner, or change
checkpoint semantics.

Acquire table admission first and catalog admission second. If catalog
admission is cancelled or fails after the table becomes active, release table
admission. Normal scope Drop releases catalog admission first and table
admission second. Both releases call the existing synchronous state
transitions.

Wrap the combined gate scope and common DDL scope in the index carriers so
normal release order is:

```text
catalog metadata gate
    -> table metadata gate
    -> logical locks
    -> mandatory/session operation terminal publication
```

No release path may await, format a fatal report, assert a terminal state, or
attempt catalog/index compensation.

### 5. Add prepared and accepted index execution carriers

Add owned plans and carriers in `catalog/index.rs`:

```rust
pub(crate) struct CreateIndexPlan { /* validated owned inputs */ }
pub(crate) struct DropIndexPlan { /* validated owned inputs */ }
pub(crate) struct PreparedCreateIndex { /* plan + prepared scopes */ }
pub(crate) struct AcceptedCreateIndex { /* plan + accepted scopes + progress */ }
pub(crate) struct PreparedDropIndex { /* plan + prepared scopes */ }
pub(crate) struct AcceptedDropIndex { /* plan + accepted scopes + progress */ }
```

Implement the runtime's `PreparedExecution`/`AcceptedExecution` contracts and
typed completion/error bridge used by Phase 2. CREATE completion returns the
allocated `IndexNo`; DROP completion returns `()`.

Each `accept` implementation may only destructure the prepared carrier, move
owned fields, transition the common operation scope, and construct its
accepted carrier. It must contain no await, allocation, target lookup, gate
transition, test hook, rejection, file creation, or domain effect. This keeps
capacity consumption and ownership transfer one non-yielding atomic handoff.

The accepted carrier remains outside the unwind-caught `execute` future.
Normal `finish` verifies that the nested transaction/progress is terminal,
releases safe scopes in order, and publishes completion. `handle_panic`
publishes retained failure state and leaves rollback-unsafe nested ownership
retained for engine poison. Neither method performs async compensation or can
panic.

### 6. Move the existing CREATE/DROP execution behind acceptance

Convert `create_index_for_session` and `drop_index_for_session` into accepted
execution methods. After the mandatory runtime owns the carrier, preserve the
existing operation order:

CREATE:

```text
begin nested private transaction
    -> fork mutable table file
    -> collect and validate cold rows
    -> build staged DiskTree
    -> collect and validate hot rows
    -> build and retain staged MemIndex/runtime
    -> assemble staged runtime layout
    -> stage prepared catalog statement
    -> commit catalog transaction
    -> publish table root
    -> atomically publish runtime layout + catalog history
    -> request metadata-history purge
    -> settle progress and completion
```

DROP:

```text
begin nested private transaction
    -> fork mutable table file and stage removed root slot
    -> assemble staged runtime layout
    -> stage prepared catalog statement
    -> commit catalog transaction
    -> publish table root
    -> atomically publish runtime layout + catalog history
    -> request metadata-history purge
    -> release old layout snapshot
    -> attempt retired-index cleanup
    -> settle progress and completion
```

Do not recalculate or revalidate preparation inputs unless the check is an
existing execution integrity assertion over newly built state. The active
locks and gates make the captured target authoritative through publication.

Make `CreateIndexProgress` and `DropIndexProgress` lifetime-free. Replace
borrowed `EngineRef`/`PoolGuards` fields with owned guards or handles already
retained by the accepted carrier. Remove Drop-time terminal assertions. All
ordinary rollback and staged-runtime destruction remains explicit in
`execute`, where errors can be observed.

Before catalog commit, preserve the current rollback policy:

- roll back the nested private transaction;
- roll back/destroy unpublished file and runtime state;
- release operation resources only after nested ownership is settled;
- return the existing typed operation/runtime error.

After catalog commit, preserve the current poison policy. Root publication,
volatile publication, or subsequent required invariant failure poisons the
engine rather than attempting to make the committed catalog transition
ordinary-cancellable. Best-effort retired-index cleanup keeps its existing
postcommit behavior and error classification.

### 7. Use prepared catalog authority without hidden lock acquisition

Change `execute_create_index_catalog_update` and
`execute_drop_index_catalog_update` to accept
`PreparedCatalogWriteAuthority<'_>` from the accepted common DDL scope and call
`Transaction::stage_prepared_catalog_statement`.

The fixed logical-lock sets in Plan sections 1 through 3 must exactly cover
the catalog tables touched by each statement:

- CREATE updates `catalog.tables`, `catalog.indexes`, and
  `catalog.index_columns`;
- DROP updates `catalog.indexes` and `catalog.index_columns`.

Retain existing table-row delete/reinsert behavior, index-number allocation,
index-column enumeration, catalog primary-key invariants, and DDL redo.

Add test-only acquisition instrumentation or reuse the Phase 2 instrumentation
to prove no `LockManager::acquire` and no table/catalog metadata-gate acquire
occurs after accepted execution begins.

### 8. Publish runtime layout and catalog history under one entry boundary

Replace the two-call sequence:

```text
Table::install_runtime_layout(...)
Catalog::publish_user_table_metadata(...)
```

for CREATE/DROP INDEX with one catalog-owned method, conceptually:

```rust
Catalog::install_index_layout_and_publish_history(
    table_id,
    effective_cts,
    expected_table,
    expected_old_layout,
    new_layout,
) -> IndexLayoutPublicationResult
```

The stable invariant is:

```text
before: catalog current metadata == old layout metadata by Arc identity
after:  catalog current metadata == new layout metadata by Arc identity
```

Implement the following non-async sequence:

1. Validate the new layout shape and allocate its `Arc` before acquiring the
   publication locks.
2. Acquire the occupied `Catalog::user_tables` entry for `table_id`.
3. Validate that the entry is live, has no dropped operational state, contains
   the exact expected `Arc<Table>`, contains the expected old metadata by
   pointer identity, and has a current CTS lower than `effective_cts`.
4. Reserve any catalog-history vector capacity and prepare the old/new history
   values before runtime mutation.
5. Acquire the table layout mutex while retaining the catalog entry.
6. Validate the expected old generation, sparse-slot constraints, and old
   layout metadata pointer under the mutex.
7. Prepare the retired-runtime delta without publishing or destroying it.
8. Replace the table layout with the prebuilt new layout and release the table
   layout mutex.
9. While still holding the catalog entry, append the prevalidated old metadata
   version and install the new current `(effective_cts, metadata, table)`
   state.
10. Validate pointer identity only after both assignments, then release the
    catalog entry.
11. Enqueue the prepared retired-runtime records while retaining the old layout
    locally, and return the installed layout.

The nested production lock order is always:

```text
Catalog::user_tables occupied entry -> Table layout mutex
```

This matches purge, which holds the user-table entry before
`assert_current_layout_metadata` takes a layout snapshot. Audit every modified
path for the inverse nesting; do not add a layout-mutex-to-catalog-entry path.

No await, ordinary error branch, allocation, injected failure, or fallible
cleanup may occur between the layout replacement and catalog-current
replacement. Rejectable validation and capacity reservation occur before the
layout swap. After persistent catalog commit/root publication, a prevalidation
mismatch is a postcommit invariant failure: leave the volatile pair unchanged
and poison the engine.

Do not call the existing `UserTableEntry::publish_live` in a way that can
return `false` after layout replacement. Split validation/preparation from an
infallible final history mutation, or provide an equivalent private commit
helper. Final invariant checking must happen after both halves are new.

Old runtime indexes remain protected by the retained old layout and existing
`Arc` snapshot semantics until retirement is queued. Preserve
`cleanup_retired_secondary_indexes` behavior and do not destroy an index
inside the publication critical section.

Add a test-only synchronous coordination point after layout replacement but
before catalog-current replacement, while the catalog entry remains held. It
may coordinate with a dedicated test thread only; production code must not
block or await there. The test starts purge, proves purge cannot enter the same
entry during the internal old/new interval, releases publication, and then
proves purge observes the new/new pointer pair. Run this scenario for both
CREATE and DROP.

This plan implements only the atomic-publication portion of backlog `000174`.
Do not close that backlog while its component-shutdown panic-safety scope
remains unresolved. Record the partial implementation and remaining scope
during `$task-resolve`.

### 9. Preserve index-build algorithms and add only local scheduling points

Keep these production paths semantically unchanged:

- `CreateIndexCollector::collect_current_cold`;
- `CreateIndexKeyValidator::prepare_cold`;
- `CreateIndexCollector::collect_current_hot`;
- `CreateIndexKeyValidator::prepare_hot`;
- `build_create_index_disk_tree`;
- `CreateIndexRuntimeBuilder::{build_unique,build_non_unique}`;
- `insert_create_index_{unique,non_unique}_hot_rows`;
- DiskTree batch writer staging and finish.

Do not replace the collected vectors, sort implementation, duplicate
comparison rules, writer input shape, or DiskTree packing/build logic.

Where the migration already owns an async loop with a natural correctness
boundary, add a direct `runtime::yield_now()` only after releasing blocking
guards. Candidate boundaries are:

- after processing one cold LWC/column-index leaf entry;
- between existing hot-row MemIndex insert iterations or small slices;
- between retired index cleanup entries;
- between top-level collection/build/publication phases when no lock-like
  short-lived guard is held.

Use a small local counter only if yielding every iteration would create
measurable overhead. Do not introduce a shared budget type or propagate a
budget through DiskTree internals. Do not change `sort_unstable_by` or claim
that its internal work yields.

Add a multi-runner integration test in which a large accepted CREATE INDEX is
active while an already-ready cleanup/internal mandatory job completes. The
test demonstrates practical non-starvation under the production runtime
configuration without defining a new build-performance contract. Keep
streaming, strict poll bounding, memory reduction, and parallel speedup in
backlog `000104`.

### 10. Replace thread-local test controls with engine-scoped controls

Add an engine-scoped index-DDL test controller following the Phase 2 table-DDL
pattern. Test-only state must be synchronized, thread-neutral, and associated
with one engine so concurrent tests cannot affect each other. Remove
thread-local failure injection used by accepted index paths.

Provide deterministic gates or notifications around at least:

- caller preparation before/after each logical-lock grant;
- table metadata-gate wait/grant;
- catalog metadata-gate wait/grant;
- mandatory capacity wait;
- accepted `BeforeFirstEffect`;
- private transaction begun;
- cold collection complete;
- DiskTree built;
- hot collection complete;
- runtime index/layout staged;
- catalog statement staged;
- catalog transaction committed;
- table root published;
- volatile layout/history publication;
- retired-index cleanup complete;
- final gate/lock release.

Failure and panic injection must distinguish reversible precommit phases from
postcommit fatal phases. Do not execute a test hook inside the synchronous
`accept` transition. The special publication overlap hook in Plan section 8
is synchronous and test-only because awaiting while holding a DashMap entry or
layout mutex is prohibited.

### 11. Preserve supervision and shutdown contracts

Use the mandatory runtime's exclusive completion producer and observer.
Accepted execution publishes exactly one success/error completion on normal
return. If `execute` panics, the runtime catches the unwind while the accepted
owner remains available, marks the operation `FailedRetained`, poisons the
engine, releases only safe gates/locks, and keeps the mandatory runner alive.

Dropping the public future after acceptance must not:

- cancel row collection or index build;
- roll back the private transaction;
- destroy a staged runtime;
- release gates or locks early;
- suppress root/layout publication;
- consume the sole completion producer;
- let engine shutdown finish before accepted ownership settles.

Engine close/abandon/shutdown must observe the accepted task as mandatory work.
Normal owner drop waits for the operation to finish. This task does not modify
component-shutdown panic recovery from backlog `000174`.

### 12. Update documentation, benchmark mapping, and RFC resolution metadata

Update architecture/lifetime/transaction/lock/index documentation where it
still describes index DDL as caller-owned or permits hidden catalog lock
acquisition. Document the prepared/accepted boundary, gate ownership, durable
commit/root ordering, volatile layout/history publication boundary, observer
semantics, and retained build architecture.

Update the benchmark-tool documentation so the existing `index-ddl` empty and
preloaded scenarios are explicitly mapped to RFC-0026 Phase 3. Do not change
the benchmark workload unless implementation needs a deterministic lifecycle
checkpoint that does not alter the measured operation.

During `$task-resolve`:

- set this task to implemented and record the GitHub issue/implementation
  evidence;
- update RFC-0026 Phase 3 `Task Doc`, `Task Issue`, `Phase Status`, and
  `Implementation Summary`;
- state that the Phase 3 prerequisite and local gate/catalog-authority choices
  were satisfied;
- preserve Phase 4 prerequisites and assumptions unless implementation
  discovers a concrete incompatibility;
- record that backlog `000104` still owns index-build redesign;
- record the atomic-publication result in backlog `000174` while leaving its
  component-shutdown half open.

No RFC phase-plan edit is required during task creation. Any implementation
discovery that expands this task into scheduler, index-build, lock-system,
recovery-format, or component-shutdown redesign must stop and return for
separate planning rather than silently broadening Phase 3.

## Implementation Notes

- `CREATE INDEX` and `DROP INDEX` now prepare public validation, the complete
  fixed logical-lock set, authoritative table/layout state, and both metadata
  gate admissions on the caller before submitting owned plans to the mandatory
  runtime. The accepted carriers start their private transactions only after
  acceptance and own build/drop execution, catalog/root/layout publication,
  cleanup, supervision, and typed completion independently of the caller or
  completion observer.
- The Phase 2 common DDL scope now serves table and index DDL. Accepted catalog
  statements consume `PreparedCatalogWriteAuthority` and do not reacquire
  catalog operation locks. `IndexDdlGateScope` transfers table-then-catalog
  admission and releases catalog before table; implementation review removed
  the superseded borrowed metadata-change entry points and redundant
  operation-specific lock-acquisition wrappers.
- `Catalog::install_index_layout_and_publish_history` now holds the occupied
  user-table entry before replacing the table layout and committing catalog
  history. This preserves the established entry-to-layout lock order and
  pointer-identity invariant, exposes only old/old or new/new pairs to purge,
  and queues retired runtimes after leaving the publication critical section.
  This completes the atomic-publication half of backlog `000174`; component
  shutdown panic safety remains open.
- The existing all-row vectors, unstable sort, uniqueness checks, DiskTree and
  MemIndex builders, root format, sparse index slots, and recovery proof remain
  unchanged. Cooperative scheduling yields once per cold block and every 64
  hot-row insertions without holding the publication locks. Backlog `000104`
  continues to own bounded-memory, streaming, and parallel index construction.
- Engine-scoped deterministic controls cover accepted execution phases,
  observer-drop inertness, panic supervision, reversible build cleanup, and the
  synchronous publication/purge overlap for both CREATE and DROP. Caller
  preparation cancellation continues to use the generalized Phase 2 lock/gate
  RAII machinery and existing admission/lock tests rather than duplicating a
  separate index hook around every individual grant.
- Implementation review consolidated completion error bridging as inherent
  `CompletionErrorBridge` methods, removed obsolete split publication helpers,
  simplified invariant paths and diagnostics, and retained operation-specific
  prepared DDL constructors as the semantic boundary. Public Session APIs,
  ordinary error variants, catalog rows, redo, roots, and recovery formats did
  not change. The implementation is recorded by issue `#926` and branch commit
  `ae8a86c`.
- Release measurements on 2026-08-02 used one thread/session,
  `log-sync=none`, five fresh roots per case, and one create/drop cycle per
  root. Empty-table `index-ddl` had a candidate median of 233,959.5 ns/op
  (range 213,230-338,293.5) versus 218,918 ns/op on `origin/main` (range
  211,022-307,731). With 10,000 preloaded rows, the candidate median was
  1,114,443 ns/op (range 1,068,401-1,299,861) versus 1,150,693.5 ns/op
  (range 1,048,943-1,163,964.5). The overlapping distributions show no
  repeatable regression.
- Five control samples had candidate medians of 73.366 ns/op for `stmt-noop`
  and 300.872 ns/op for `trx-noop`, versus 79.074 ns/op and 306.016 ns/op on
  `origin/main`. Final verification passed 1,626 workspace tests, 1,533
  alternate-`libaio` tests, the 32-test catalog-index suite, workspace build,
  diff checks, and the mandatory style audit over 14 branch-diff Rust files.

## Impacts

### Production code

- `doradb-storage/src/session.rs`
  - generalize Phase 2 prepared/accepted DDL scopes;
  - add fixed CREATE/DROP INDEX lock preparation;
  - split `Session::create_index` and `Session::drop_index` into caller
    preparation, mandatory submission, and observer wait;
  - remove `SessionDdlContext` when its final callers migrate.
- `doradb-storage/src/catalog/index.rs`
  - add owned create/drop plans and prepared/accepted carriers;
  - make progress state lifetime-free;
  - move current index execution under mandatory ownership;
  - consume prepared catalog-write authority;
  - preserve build and failure semantics;
  - replace thread-local test/failure controls.
- `doradb-storage/src/catalog/table.rs`
  - update imports and shared DDL-scope names without changing Phase 2
    behavior.
- `doradb-storage/src/catalog/mod.rs`
  - add the combined index layout/history publication entry point;
  - expose only the narrow internal access required by the publication
    coordinator.
- `doradb-storage/src/catalog/history.rs`
  - split live-publication prevalidation/capacity preparation from the
    infallible final current/history mutation;
  - retain pointer-identity assertions.
- `doradb-storage/src/catalog/checkpoint.rs`
  - factor catalog metadata-gate acquisition/release for transferable
    `IndexDdlGateScope` without changing checkpoint state semantics.
- `doradb-storage/src/table/mod.rs`
  - factor runtime-layout validation/replacement and retired-index preparation
    so catalog publication can hold the established entry-to-layout order.
- `doradb-storage/src/table/lifecycle.rs`
  - factor table metadata-gate acquisition/release for the transferable scope
    without changing lifecycle transitions.
- `doradb-storage/src/trx/mod.rs` and `doradb-storage/src/trx/stmt.rs`
  - generalize prepared catalog-authority documentation/tests for index DDL;
  - no successful statement hot-path redesign.
- `doradb-storage/src/engine.rs`
  - retain engine-scoped, test-only index DDL control state following the
    established table-DDL pattern.
- `doradb-storage/src/runtime/mandatory.rs`
  - no production contract change is expected; extend integration tests or
    diagnostics only if the existing generic adapter needs index output/error
    coverage.

`doradb-storage/src/index/disk_tree.rs` is a behavior dependency and audit
target, not a planned algorithm-change target. Modification requires a
specific correctness need discovered during implementation and must not grow
into backlog `000104`.

### Documentation and process

- `docs/rfcs/0026-engine-owned-mandatory-background-runtime.md` at task
  resolution only.
- `docs/engine-component-lifetime.md`
- `docs/transaction-system.md`
- `docs/lock-system.md`
- `docs/secondary-index.md`
- benchmark-tool documentation covering `index-ddl`
- `docs/backlogs/000104-stream-parallel-create-index-cold-build.md`
  as an unchanged deferral reference
- `docs/backlogs/000174-atomic-index-metadata-publication-and-panic-safe-shutdown.md`
  for partial outcome recording during task resolution

### Compatibility and performance

- Public Session method signatures and ordinary error variants remain
  unchanged.
- Catalog rows, DDL redo, table roots, index block encoding, recovery proof,
  and runtime layout generation/slot semantics remain unchanged.
- Accepted index work moves from the caller executor to the mandatory runtime;
  caller cancellation after acceptance no longer affects execution.
- Preparation holds target locks and both metadata gates while waiting for
  mandatory capacity. Saturation can therefore increase lock hold time, as
  explicitly allowed by RFC-0026.
- The current all-row memory peak and sorting/build complexity remain. Added
  yield points must be benchmarked for overhead but are not a performance
  redesign.
- Combined volatile publication adds one nested entry-to-layout critical
  section. It must not perform I/O or await and should remain short.

### Principal risks and mitigations

- **Partial gate leak:** dropping a pending or partially acquired scope could
  leave a table/catalog gate reserved. Cover every transition and Drop point
  with deterministic state tests.
- **Deadlock from inverse publication order:** a layout-to-entry nested path
  could deadlock purge. Enforce and document entry-to-layout order and audit
  all modified callers.
- **Split publication after postcommit failure:** an error or panic between
  layout and history assignments could recreate the invalid pair. Prevalidate
  and reserve before mutation; keep the final two assignments non-awaiting and
  non-fallible; inject no failure there.
- **Hidden accepted await:** catalog statement staging or retry code could
  reacquire operation authority. Instrument the lock manager and metadata
  gates and assert zero accepted acquisitions.
- **Unsafe panic cleanup:** a Drop implementation could destroy staged state
  after an arbitrary unwind. Keep compensation explicit inside `execute` and
  retain unsafe progress on panic.
- **Cross-thread test contamination:** thread-local hooks would silently stop
  working after migration. Scope all controls to one engine and reset them
  deterministically.
- **Shared-scope regression:** renaming/generalizing Phase 2 types could alter
  table DDL release order. Keep table constructors unchanged and rerun their
  complete focused suite.
- **Build-loop interference:** unchanged sort/DiskTree synchronous sections
  can occupy one runner. Preserve multiple-runner coverage and defer strict
  chunking/build redesign to backlog `000104`.

## Test Cases

### Preparation and cancellation

1. CREATE rejects a non-user table ID and user primary-key attributes before
   reserving an operation, acquiring a lock/gate, or waiting for a permit.
2. DROP rejects a non-user table ID before operation reservation.
3. CREATE/DROP reject a conflicting explicit session target lock before
   waiting on their operation lock set.
4. Drop CREATE/DROP while waiting for each target or catalog lock; prove the
   current waiter disappears and every earlier grant is released.
5. Drop while waiting for the table metadata gate; prove no pending table
   reservation remains and no catalog gate is touched.
6. Drop while waiting for the catalog metadata gate after table admission;
   prove catalog pending state and active table admission both reopen.
7. Saturate caller admission, prepare index DDL, and drop it while capacity
   waits; prove no mandatory task/permit exists and all gates/locks release.
8. Retain the capacity-waiting future and prove its complete prepared scope
   continues to exclude conflicting operations until dropped or accepted.
9. Inject admission close/engine poison at the capacity race; prove the
   prepared carrier returns to the caller and releases exactly once.

### Acceptance and ownership

10. At accepted `BeforeFirstEffect`, assert the session operation is
    `Mandatory`, the exact CREATE or DROP lock set is held, and both metadata
    gates are active.
11. Prove synchronous `accept` performs no allocation-visible hook, await,
    lookup, file mutation, transaction creation, or gate transition.
12. Instrument `LockManager` and both metadata gates and assert zero acquisition
    attempts after `BeforeFirstEffect`.
13. Drop the public future/completion observer at every accepted phase and
    prove CREATE/DROP continues to the same terminal result.
14. Close or abandon the engine while accepted index work is paused; prove
    shutdown remains busy until the accepted owner settles.
15. Run acceptance, completion, observer-drop, and shutdown tests with one and
    multiple mandatory runners where the scenario does not rely on the
    explicitly multi-runner build-progress guarantee.

### Existing index semantics

16. Preserve CREATE over empty, hot-only, cold-only, and mixed hot/cold tables.
17. Preserve unique duplicate rejection for cold/cold, hot/hot, and cold/hot
    keys with no catalog/root/layout publication.
18. Preserve non-unique exact-key behavior, encoded ordering, row IDs, nullable
    values, ascending/descending columns, and composite keys.
19. Preserve persisted delete-delta and in-memory deletion-marker filtering,
    including uncommitted delete conflict behavior.
20. Preserve allocated index numbers, sparse inactive slots, root-vector
    resizing, runtime slot shape, and create/drop/recreate behavior.
21. Preserve active-root validation and the CREATE/DROP DDL redo durability
    proof across recovery and crash boundaries.
22. Preserve old layout/index runtime visibility for transactions that pin an
    earlier layout while DROP publishes and cleanup waits.

### Failure, rollback, and panic

23. Inject every reversible CREATE failure before catalog commit; prove the
    original catalog/root/layout snapshot remains, the nested transaction is
    rolled back, and unpublished DiskTree/MemIndex/file state is destroyed or
    retained according to existing policy.
24. Inject every reversible DROP failure before catalog commit; prove the
    active index remains cataloged, rooted, installed, and queryable.
25. Inject catalog staging and commit failures while using prepared authority;
    prove no hidden lock acquisition and correct nested transaction settlement.
26. Inject root-publication failure after catalog commit for CREATE and DROP;
    prove engine poison and retained fatal ownership.
27. Inject volatile-publication prevalidation failure after root publication;
    prove neither volatile pointer changes and the engine poisons.
28. Inject accepted-task panic at each safe phase before and after commit;
    prove `FailedRetained`, engine poison, mandatory runner survival, exact
    permit accounting, and no panicking Drop.
29. Prove normal finish releases catalog gate, table gate, logical locks, and
    session terminal state in the specified order.

### Atomic layout/history publication

30. For CREATE, pause after layout swap while the user-table entry remains
    held, start purge on another thread, prove purge cannot enter, finish
    history publication, and prove purge observes pointer-identical new
    metadata.
31. Repeat the same deterministic overlap for DROP.
32. Prove purge immediately before publication observes old/old and purge
    immediately afterward observes new/new.
33. Prove an expected table, old metadata, generation, slot shape, or CTS
    mismatch is rejected before layout mutation.
34. Prove old layout snapshots retain dropped runtime indexes until retirement
    is queued and ordinary cleanup can safely destroy them.
35. Audit/test the catalog-entry-to-layout lock order with no inverse nested
    acquisition in modified paths.

### Scheduling, regression, and benchmarks

36. Verify explicit cooperative yields occur only after short-lived blocking
    guards are released and do not change row/key order or failure points.
37. Under the normal multi-runner configuration, keep a large CREATE INDEX
    active and prove an already-ready cleanup/internal mandatory job completes
    before the build finishes.
38. Rerun the complete Phase 2 table-DDL focused suite after common scope
    generalization.
39. Run focused index/catalog/history/recovery tests with cargo-nextest.
40. Run `rtk cargo nextest run --workspace`.
41. Run
    `rtk cargo nextest run -p doradb-storage --no-default-features --features libaio`.
42. Run workspace build, formatting, Clippy/lint, and the required
    `$style-audit` gate during task resolution.
43. Run repeated paired `index-ddl` benchmarks on fresh roots for empty and
    preloaded tables, report median and dispersion, and compare against the
    pre-task baseline.
44. Run `stmt-noop` and `trx-noop` controls to detect an accidental successful
    transaction/statement hot-path regression.
45. Confirm `.config/nextest.toml` remains unchanged and that timeout failures
    are treated as hangs rather than replaced with ad hoc sleeps.

## Open Questions

None. Streaming/bounded-memory/parallel index construction remains backlog
`000104`, and component-shutdown panic safety remains the unresolved half of
backlog `000174`; neither is an open design choice for this task.
