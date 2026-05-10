---
id: 0017
title: Drop Table Lifecycle And Recovery
status: proposal
tags: [storage, ddl, recovery, checkpoint, catalog]
created: 2026-05-05
github_issue: 614
---

# RFC-0017: Drop Table Lifecycle And Recovery

## Summary

Define the storage-engine lifecycle and recovery contract for logical
`DROP TABLE`. The design uses logical locks for foreground DDL serialization, a
volatile table lifecycle gate for stale runtime handles and background
checkpoint cancellation, and a catalog checkpoint/recovery rule that treats
checkpointed catalog absence plus a monotonic non-reused table id as sufficient
durable negative knowledge. The logical DDL phase deliberately separates runtime
unreachability from resource reclamation; a later phase in this RFC reuses the
transaction GC horizon to destroy dropped table memory and waits for catalog
checkpoint negative knowledge before deleting table files. Table-id reuse and
`CREATE INDEX`/`DROP INDEX` remain out of scope and should be handled by later
RFCs.

## Context

The storage engine already has partial `DropTable` redo and recovery handling,
but it has no public `DROP TABLE` path and no runtime lifecycle state for stale
`Arc<Table>` handles. Existing logical locks protect foreground table access, but
checkpoint and recovery intentionally do not acquire logical locks. `DROP TABLE`
therefore needs a lifecycle protocol that coordinates foreground DDL, foreground
DML, table checkpoints, catalog checkpoint, and recovery without turning
background checkpoint into a lock-manager participant.

The previous draft also tried to reserve direction for offline `CREATE INDEX`
and `DROP INDEX`. That scope is now deferred. Index DDL needs its own
catalog/table-root atomicity and sparse-index-slot design, so this RFC keeps only
the rules needed to make `DROP TABLE` safe and testable.

Issue Labels:
- type:epic
- priority:medium
- codex

## Design Inputs

### Documents

- [D1] `docs/architecture.md` - catalog is cache-first at runtime, durable via
  `catalog.mtb`; user tables use deterministic per-table files; catalog overlay
  metadata includes `next_user_obj_id` and `catalog_replay_start_ts`.
- [D2] `docs/transaction-system.md` - foreground access uses logical locks;
  row writes acquire transaction-lifetime table locks; recovery, checkpoint,
  purge, and no-transaction replay do not acquire logical locks.
- [D3] `docs/checkpoint-and-recovery.md` - recovery computes a coarse replay
  floor across catalog and loaded user-table replay boundaries, then applies
  finer per-component replay rules.
- [D4] `docs/table-file.md` - table files publish CoW roots, retain old roots for
  readers/GC, and defer physical cleanup to root-reachability GC.
- [D5] `docs/rfcs/0016-logical-lock-manager.md` - future `DROP TABLE` should
  acquire `CatalogNamespace(X)`, `TableMetadata(X)`, and `TableData(X)`, and
  physical reclamation remains governed by checkpoint/GC safety.
- [D6] `docs/rfcs/0006-cache-first-unified-catalog-storage-refactor.md` -
  requires persisted monotonic `next_user_obj_id`, replay-based catalog
  checkpoint, and explicitly excludes `DROP TABLE` lifecycle design.
- [D7] `docs/tasks/000057-catalog-checkpoint-validation-and-documentation-sync.md`
  - validation explicitly deferred `DropTable` API, lifecycle, replay policy,
  and recovery validation to a future RFC/program task.
- [D8] `docs/index-design.md` and `docs/secondary-index.md` - index DDL affects
  secondary-index checkpoint/recovery contracts and is deferred to a later RFC.
- [D9] `docs/process/issue-tracking.md` - significant storage work uses
  document-first RFC/task planning.
- [D10] `docs/process/unit-test.md` - standard validation uses
  `cargo nextest run -p doradb-storage`.

### Code References

- [C1] `doradb-storage/src/session.rs` - `Session::create_table` is the current
  DDL shape: namespace lock, catalog-row transaction, DDL redo, table-file
  publication, runtime insertion.
- [C2] `doradb-storage/src/trx/redo.rs` - DDL redo already defines
  `DropTable`.
- [C3] `doradb-storage/src/trx/recover.rs` - recovery can replay `DropTable`,
  remove runtime tables, and currently errors on unknown user-table DML.
- [C4] `doradb-storage/src/catalog/checkpoint.rs` - catalog checkpoint currently
  treats `DropTable` as a blocking DDL that requires a loaded table replay floor.
- [C5] `doradb-storage/src/catalog/mod.rs` - catalog owns the runtime
  `DashMap<TableID, Arc<Table>>`, restores `next_user_obj_id`, and has no table
  lifecycle state beyond insertion/removal.
- [C6] `doradb-storage/src/catalog/storage/checkpoint.rs` - catalog checkpoint
  persists `next_user_obj_id` and advances `catalog_replay_start_ts` to
  `safe_cts + 1`.
- [C7] `doradb-storage/src/catalog/storage/tables.rs`,
  `doradb-storage/src/catalog/storage/columns.rs`, and
  `doradb-storage/src/catalog/storage/indexes.rs` - table/column/index delete
  helpers exist, but `index_columns.delete_by_index` is still `todo!()`.
- [C8] `doradb-storage/src/table/persistence.rs` - table checkpoint publishes a
  table-file root and emits `DDLRedo::DataCheckpoint` without logical locks.
- [C9] `doradb-storage/src/table/mod.rs` - user-table runtime has fixed metadata,
  secondary-index arrays, table-file roots, and no live/dropping/dropped state.
- [C10] `doradb-storage/src/trx/stmt.rs` and `doradb-storage/src/trx/mod.rs` -
  statement read/write APIs acquire logical locks, and transaction-owned locks
  are carried through commit/rollback before release.
- [C11] `doradb-storage/src/trx/log_replay.rs` and
  `doradb-storage/src/trx/log.rs` - redo recovery merges streams in ascending CTS
  order, and group commit assigns globally ordered commit timestamps.
- [C12] `doradb-storage/src/file/fs.rs` and
  `doradb-storage/src/file/table_file.rs` - table files are deterministic paths;
  current deletion support is limited to uncommitted/failed table creation.
- [C13] `doradb-storage/src/trx/purge.rs` and `doradb-storage/src/trx/mod.rs` -
  transaction GC advances on the oldest-active-snapshot horizon and already
  releases retained old table roots after they cross that horizon.
- [C14] `doradb-storage/src/index/secondary_index.rs` and
  `doradb-storage/src/index/btree/mod.rs` - secondary MemIndex/BTree destroy
  helpers can reclaim index pages, but table/block-index destroy is not wired.

### Conversation References

- [U1] Initial request: add `DROP TABLE`, use logical locks for DDL, and treat
  checkpoint/recovery/catalog recovery carefully.
- [U2] Decision to switch from task planning to RFC workflow because lifecycle
  and recovery correctness are RFC-scale.
- [U3] Discussion clarified that durable drop tombstones were only one possible
  way to provide recovery negative knowledge.
- [U4] Recovery concern: checkpointed catalog absence plus sequential replay may
  make a separate tombstone table unnecessary for `DROP TABLE`.
- [U5] Concern that background tasks should not hold logical locks and should be
  cancellable/failable around committed drop.
- [U6] Round 1 review accepted: table-id non-reuse, no post-drop table redo,
  checkpoint/drop ordering, lifecycle rollback, and precise CTS semantics must
  be explicit.
- [U7] Scope revision: `CREATE INDEX` and `DROP INDEX` should move to a future
  separate RFC.
- [U8] Phase 3 review found that task 000144 intentionally removes runtime
  reachability but lacks a step for releasing hot row pages and index state.
- [U9] Follow-up decision: phase 4 should reuse the GC path rather than a
  standalone dropped-table reclaimer; memory destroy can happen at the GC
  horizon, while physical file deletion waits for catalog replay-boundary safety.

## Decision

This RFC chooses a logical `DROP TABLE` implementation built on:

- foreground DDL locks;
- volatile runtime lifecycle state;
- a checkpoint lease/cancellation gate;
- replay rules based on checkpointed catalog absence and monotonic table-id
  non-reuse;
- deferred, GC-managed runtime memory reclamation and physical file deletion.

The RFC does not add a durable drop tombstone table. The no-tombstone rule is
valid only because user table ids are persisted through a monotonic allocator and
are never reused after crash/restart. [D1], [D3], [D5], [D6], [C5], [C6], [U3],
[U4], [U6]

### Correctness Invariants

`DROP TABLE` is correct only if all of these invariants hold:

1. Table-id non-reuse: a committed user table id is never reused, even if the
   table is dropped and later absent from checkpointed catalog state. Allocation
   must use persisted `next_user_obj_id`, not `max(catalog.tables.table_id) + 1`.
   [D1], [D6], [C5], [C6]
2. Drop redo barrier: after `DropTable(table_id)` commits at `drop_cts`, no
   table-scoped redo for `table_id` may commit with `cts > drop_cts`. This
   includes row DML, row-page creation, cold-delete replay obligations, and table
   checkpoint redo. [D2], [C8], [C10], [U6]
3. Foreground writer drain: `DROP TABLE` must acquire `TableData(X)`, which waits
   for active transaction-owned writer locks on the table. A transaction that
   already modified the table must commit or abort before drop can obtain the
   exclusive table-data lock. [D2], [D5], [C10]
4. Active statement drain: `DROP TABLE` must acquire `TableMetadata(X)`, which
   waits for active statement-owned table read locks and transaction-owned table
   metadata locks. [D2], [D5], [C10]
5. Checkpoint barrier: no `DataCheckpoint(table_id)` may commit with
   `cts > drop_cts`, and no table-file root publication for the table may remain
   ambiguous when drop commits. [D4], [C8], [U5], [U6]
6. Catalog negative knowledge: if checkpointed catalog omits `table_id` and
   `catalog_replay_start_ts > drop_cts`, recovery may skip table-scoped redo for
   `table_id` with `cts < catalog_replay_start_ts`. [D1], [D3], [C3], [C4]
7. Unknown future redo is invalid: redo for a checkpoint-absent table with
   `cts >= catalog_replay_start_ts` is valid only if recovery has replayed a
   `CreateTable(table_id)` before that redo. Otherwise recovery must fail.
   [D3], [C3], [U4]
8. Lifecycle gate: foreground access through an `Arc<Table>` is valid only after
   acquiring logical locks and observing lifecycle `Live`. [C5], [C9], [C10],
   [U5]
9. Drop abort recovery: a failed uncommitted drop must restore volatile lifecycle
   from `Dropping` to `Live`. [C9], [U6]
10. Reclamation separation: logical drop commit must not require in-memory
    table destroy, table-file deletion, or quarantine. [D4], [D5], [C12],
    [C13], [C14], [U8], [U9]

### Foreground DDL Locking

`DROP TABLE` must reject execution while the session already owns an active user
transaction, matching the existing implicit DDL commit policy used by
`CREATE TABLE`. [C1]

The v1 storage API is `drop_table(table_id)`. It still acquires
`CatalogNamespace(X)` so table identity publication/removal remains serialized
with `CREATE TABLE` and future name-resolution DDL. The lock order is:

```text
CatalogNamespace(X) -> TableMetadata(table_id, X) -> TableData(table_id, X)
```

The namespace lock is acquired first. While holding it, the implementation
validates that the runtime/catalog table exists, then acquires the table locks.
`TableMetadata(X)` drains active read statements and active writer transactions
that hold table metadata. `TableData(X)` drains writer transactions that hold
`TableData(IX)` until commit or rollback. [D2], [D5], [C1], [C5], [C10]

Repeated `drop_table(table_id)` after the first committed drop returns
`TableNotFound` or an equivalent terminal not-found error. `DROP TABLE IF EXISTS`
and drop-by-name semantics are out of scope for this storage API RFC. [U1]

### Runtime Lifecycle Gate

Logical locks are not sufficient by themselves because stale `Arc<Table>` handles
can survive runtime catalog removal. User-table runtime must gain a volatile
lifecycle state:

```text
Live -> Dropping -> Dropped
Live -> Dropping -> Live
```

The abort path returns to `Live` when drop fails before durable commit. The
successful path transitions to `Dropped` after the drop commit is durable and
before releasing DDL locks. Runtime catalog removal happens only after the handle
has been marked `Dropped`, so newly resolved handles disappear and stale handles
fail lifecycle checks. The state transition must use a synchronization primitive
with acquire/release ordering or stronger so a thread that observes `Dropped`
cannot proceed using stale table state. [C5], [C9], [C10], [U5], [U6]

Foreground statement APIs must check lifecycle after acquiring their normal
logical locks and before touching row, index, deletion-buffer, or table-file
state:

```text
Dropped  -> terminal table-not-found/table-dropped error
Dropping -> terminal DDL-in-progress/table-dropping error for foreground access
Live     -> proceed
```

The first implementation should classify access paths explicitly:

- foreground user reads/writes require `Live`;
- checkpoint requires a checkpoint lease acquired from `Live`;
- drop DDL owns `Dropping` and `Dropped` transitions;
- recovery may construct/remove runtime state without volatile lifecycle locks;
- purge/GC may touch retained physical roots only through existing retention or
  a future reclamation protocol;
- diagnostics/tests may bypass lifecycle only through explicit test/internal
  helpers. [D2], [C3], [C8], [C9], [C10]

### Background Checkpoint Lease

Table checkpoint must not acquire logical locks. Instead, table runtime must
provide a lightweight checkpoint lease protocol:

```text
try_begin_checkpoint():
    atomically verify lifecycle == Live
    increment active checkpoint lease count
    return checkpoint lease

drop_begin():
    while holding DDL locks, CAS Live -> Dropping
    prevent new checkpoint leases
    request cancellation for active cancellable leases
    wait for active no-cancel publish leases to exit

checkpoint_enter_publish():
    atomically verify lifecycle == Live and no drop cancellation
    convert checkpoint lease to no-cancel publish lease
    otherwise return cancelled before table-root publication
```

A checkpoint lease is cancellable during collection, frozen-page stabilization,
LWC construction, deletion checkpoint, and secondary sidecar work. A checkpoint
may enter table-root publication only after it successfully converts to a
no-cancel publish lease. Once in the no-cancel section, drop must wait for the
checkpoint to finish publishing and committing or to reach a fatal storage error.
[D2], [D4], [C8], [U5], [U6]

The no-cancel section covers table-root publication, old-root retention, and the
ordered checkpoint transaction commit that emits `DDLRedo::DataCheckpoint`.
Phase 2 must audit the existing root-publish-before-transaction-commit path: if
root publication succeeds but the checkpoint transaction cannot commit, storage
must either make the outcome recoverable under existing checkpoint rules or
poison/fail the runtime before drop can proceed normally. [D4], [C8], [U6]

### Drop Table Catalog And Runtime Shape

`DROP TABLE` must be an implicit DDL transaction, mirroring the existing
`CREATE TABLE` shape. The transaction deletes every current catalog row that
references the table id:

- all `catalog.index_columns` rows for the table;
- all `catalog.indexes` rows for the table;
- all `catalog.columns` rows for the table;
- the `catalog.tables` row for the table.

The general catalog invariant is stronger than the current list: after
`DROP TABLE` commit, no checkpointed catalog row may reference the dropped
`table_id`. Future catalog tables that reference table ids must be added to the
same cascade-delete contract. [C1], [C7], [U6]

All cascade deletes and `DDLRedo::DropTable(table_id)` must be part of the same
atomic transaction. Recovery must not expose a state where catalog deletes are
applied without the drop DDL record or where the drop DDL record is applied
without the matching catalog deletes. Replayed catalog deletes must remain
idempotent under the existing catalog no-transaction replay model. [C2], [C3],
[C7]

After durable commit, runtime marks the old table handle `Dropped`, removes it
from `Catalog.user_tables`, and then releases DDL locks. Runtime memory destroy
and physical table-file deletion are not part of the logical DDL phase. [C5],
[C9], [C12], [C13], [C14], [U1], [U5], [U8], [U9]

The RFC does not require a durable `catalog.object_lifecycle` table for
`DROP TABLE` v1. Because table ids are not reused, checkpointed catalog absence
plus `catalog_replay_start_ts` is sufficient durable negative knowledge for
recovery. A durable lifecycle ledger may be added later only if table-id reuse,
generation-aware reclamation, or a general physical reclaimer needs it. [D1],
[D3], [D6], [C3], [C4], [C5], [C6], [U3], [U4]

### Catalog Checkpoint And Recovery Rule

`catalog_replay_start_ts` is the first CTS that recovery must consider for
catalog-derived object state. Redo with `cts < catalog_replay_start_ts` is
checkpoint-covered for catalog state. Redo with
`cts >= catalog_replay_start_ts` must be replayed or validated against replayed
catalog DDL. Commit timestamps are globally ordered, and recovery's `LogMerger`
returns redo in ascending CTS order. [D3], [C6], [C11]

Catalog checkpoint must stop requiring a loaded dropped-table replay floor before
it can advance past `DDLRedo::DropTable`. The replacement rule is:

```text
Catalog checkpoint may persist table absence for DropTable(table_id, drop_cts)
and advance catalog_replay_start_ts to safe_cts + 1 only if:
    the drop transaction is committed;
    the checkpoint batch includes the full catalog cascade delete;
    safe_cts >= drop_cts;
    the runtime/drop protocol guarantees no later table redo can commit for table_id.
```

Checkpointed dropped tables do not participate in the recovery coarse replay
floor because they are not loaded user tables. Their pre-drop redo is covered by
catalog negative knowledge, not by table-file replay boundaries. A leftover table
file for a checkpoint-absent table is ignored by recovery until phase 4's
GC-managed file-deletion stage handles it. [D1], [D3], [C3], [C4], [C12], [U9]

Recovery must classify table-scoped redo for unknown table ids by CTS:

```text
if table_id is absent from checkpointed catalog
and redo.cts < catalog_replay_start_ts:
    skip this table-scoped redo and record a debug/trace diagnostic

if redo.cts >= catalog_replay_start_ts
and no prior replayed CreateTable made the table live:
    fail recovery as invalid log/catalog ordering
```

This unknown-table rule applies to user-table row DML and table-scoped DDL such
as `CreateRowPage` and `DataCheckpoint`. `DropTable` itself is DDL redo. If
`DropTable.cts < catalog_replay_start_ts`, it is checkpoint-covered and skipped
by the catalog replay predicate. If `DropTable.cts >= catalog_replay_start_ts`,
the table must exist in recovered runtime/catalog state before the drop is
applied; otherwise recovery treats the log/catalog state as inconsistent. [D3],
[C2], [C3], [C11], [U4], [U6]

### Crash Matrix

The implementation must satisfy these restart cases:

| Crash point | Expected restart behavior |
| --- | --- |
| Before drop starts | Table remains live from checkpointed catalog or redo. |
| After `Live -> Dropping`, before drop commit | Volatile state is lost; table remains live from durable state. |
| After drop commit, before runtime removal | Recovery replays `DropTable` and removes runtime table. |
| After drop commit, before catalog checkpoint | Recovery loads table from checkpointed catalog, replays eligible table redo, then replays `DropTable`. |
| After catalog checkpoint includes table absence and `catalog_replay_start_ts > drop_cts` | Recovery does not load the table; old table redo below replay start is skipped. |
| During checkpoint before table-root publication | Checkpoint cancels or rolls back before publishing; drop may commit afterward. |
| During checkpoint publish/commit no-cancel section | Drop waits until checkpoint commits before drop or storage enters fatal/poisoned state. |
| After drop commit with leftover table file | Runtime catalog lacks the table; file remains unused. |

### Physical Table-File Deletion Contract

Phase 3 leaves both hot runtime reclamation and table-file deletion out of the
logical DDL path. Phase 4 reuses transaction GC rather than adding a standalone
dropped-table reclaimer:

- after `min_active_sts > drop_cts`, GC may attempt to consume the removed
  runtime table handle and reclaim in-memory resources such as hot row pages,
  row-page-index nodes, secondary MemIndex pages, deletion-buffer runtime state,
  and other volatile table-owned structures;
- if stale `Arc<Table>` handles still exist, GC keeps the dropped-table item and
  retries in a later cycle;
- after in-memory destroy succeeds, GC keeps a lightweight file-deletion item
  until catalog checkpoint safety is visible.

Physical table-file deletion may happen only after:

- drop commit is durable;
- catalog checkpoint persisted table absence with `catalog_replay_start_ts`
  beyond the drop CTS;
- no in-flight checkpoint can publish a root for the table;
- no live runtime handle can use the file;
- table ids are still non-reused, or the deletion protocol is generation-aware;
- deletion/quarantine is crash-idempotent across restart.

Immediate unlink at drop commit is unsafe because a crash before catalog
checkpoint could restart from checkpointed catalog metadata that still names the
table and then fail opening the missing table file. [D1], [D3], [D4], [C12],
[C13], [C14], [U5], [U6], [U8], [U9]

### Test Strategy

Each implementation phase must add focused tests and keep the standard
validation pass green:

```bash
cargo nextest run -p doradb-storage
```

Required runtime/concurrency tests:

- stale `Arc<Table>` read after committed drop returns the expected terminal
  error;
- stale `Arc<Table>` write after committed drop cannot emit redo;
- drop waits for active table read statements and writer transactions;
- drop abort after `Live -> Dropping` restores table to `Live`;
- new checkpoint cannot start after `Dropping`;
- in-flight checkpoint cancels before root publication;
- checkpoint already in publish/commit critical section commits before drop
  commit or forces fatal/poisoned runtime behavior.

Required recovery tests:

- crash before drop commit recovers table live;
- crash after drop commit before catalog checkpoint replays drop and removes
  runtime table;
- crash after catalog checkpoint with table absence and replay start beyond drop
  skips old table redo;
- unknown table redo with `cts < catalog_replay_start_ts` is skipped and counted
  in diagnostics;
- unknown table redo with `cts >= catalog_replay_start_ts` fails unless prior
  replayed `CreateTable` made the table live;
- checkpoint-covered `DataCheckpoint` for dropped table is skipped;
- injected post-drop `DataCheckpoint` or row DML fails recovery;
- table-id allocator does not reuse dropped ids after checkpoint and restart.

Required catalog integrity tests:

- drop deletes all current table, column, index, and index-column rows;
- catalog checkpoint after drop contains no orphaned rows for the dropped table
  id;
- creating a later table uses a new table id;
- dropping a table with indexes cascades metadata deletes correctly.

Required phase 4 destroy tests:

- GC does not destroy a dropped table while `min_active_sts <= drop_cts`;
- stale `Arc<Table>` handles delay consuming in-memory destroy;
- memory destroy returns hot row pages, row-page-index nodes, and MemIndex pages
  to their buffer pools;
- table-file deletion does not run before
  `catalog_replay_start_ts > drop_cts`;
- crash before catalog checkpoint still recovers by opening the table file and
  replaying `DropTable`;
- crash after catalog checkpoint can tolerate an already-deleted table file for
  the checkpoint-absent table;
- repeated file deletion is idempotent.

## Alternatives Considered

### Alternative A: Mandatory Durable Drop Tombstone Table

- Summary: Add `catalog.object_lifecycle` or `catalog.dropped_tables` and keep a
  durable tombstone for every dropped table.
- Analysis: This provides explicit recovery negative knowledge and would become
  useful if table ids are reused or physical reclamation needs a durable ledger.
  For current non-reused table ids, checkpointed catalog absence plus
  `catalog_replay_start_ts` proves the same fact for old redo.
- Why Not Chosen: It adds catalog surface and lifecycle-GC questions without
  being required for correct `DROP TABLE` v1 recovery.
- References: [D1], [D3], [D6], [C3], [C4], [C5], [C6], [U3], [U4]

### Alternative B: Background Checkpoint Uses Logical Locks

- Summary: Make table checkpoint acquire table metadata/data locks so
  `DROP TABLE` naturally waits behind checkpoint.
- Analysis: This would reuse the lock manager but conflicts with the existing
  internal-boundary design for checkpoint/recovery and can make user DDL wait on
  long checkpoint work.
- Why Not Chosen: A lifecycle checkpoint gate can cancel or fail checkpoint work
  without turning checkpoint into a foreground lock participant.
- References: [D2], [C8], [U5]

### Alternative C: Immediate Physical File Deletion During Drop

- Summary: Delete the table file as soon as `DROP TABLE` commits.
- Analysis: This gives immediate disk-space reclamation but is crash-unsafe if
  restart uses a checkpointed catalog state that still contains the table.
- Why Not Chosen: Physical deletion needs a crash-idempotent reclaimer and should
  be separate from logical drop correctness.
- References: [D1], [D3], [D4], [C12], [C13], [U5], [U6], [U9]

### Alternative D: Standalone Dropped-Table Reclaimer

- Summary: Add a new dropped-table reclaimer service separate from transaction
  GC.
- Analysis: A dedicated service could reclaim dropped tables as soon as its own
  handle/refcount and catalog-checkpoint predicates pass.
- Why Not Chosen: The existing GC horizon already represents the conservative
  "no active snapshot can see this" boundary. Reusing it keeps the first
  destroy implementation smaller, even if reclamation happens later than the
  earliest possible moment.
- References: [D2], [D4], [C13], [C14], [U8], [U9]

### Alternative E: Include Offline Create/Drop Index In This RFC

- Summary: Keep `CREATE INDEX` and `DROP INDEX` phases in this RFC alongside
  `DROP TABLE`.
- Analysis: Index DDL needs table-root/catalog atomicity, stable index-slot
  identity, index allocation, and post-index-DDL DML guarantees. Those concerns
  overlap with table lifecycle but are not required for `DROP TABLE`.
- Why Not Chosen: The scope is large enough to deserve a future dedicated RFC.
  This RFC should finish the `DROP TABLE` lifecycle and leave index DDL design
  separate.
- References: [D8], [U7]

## Unsafe Considerations

This RFC does not require new `unsafe` code. Implementations that touch table
root publication, runtime handle replacement, or future file deletion must
preserve existing unsafe boundaries and root-access proof contracts documented by
the table-file/root design. [D4], [C8], [C9]

## Implementation Phases

- **Phase 1: Catalog Delete Helpers And Recovery Classification**
  - Scope: add missing table-scoped catalog delete helpers, especially
    `index_columns` deletion by table/index; add recovery classification for
    unknown table-scoped redo below `catalog_replay_start_ts`; add diagnostics
    for skipped checkpoint-covered unknown-table redo.
  - Goals: provide catalog/recovery primitives needed before public
    `DROP TABLE`.
  - Non-goals: no public `DROP TABLE`; no table-file deletion; no index DDL.
  - Task Doc: `docs/tasks/000142-catalog-delete-helpers-and-recovery-classification.md`
  - Task Issue: `#617`
  - Phase Status: done
  - Implementation Summary: Implemented table-scoped catalog delete helpers and recovery classification for checkpoint-covered unknown user-table redo; added diagnostics and focused catalog/recovery tests. [Task Resolve Sync: docs/tasks/000142-catalog-delete-helpers-and-recovery-classification.md @ 2026-05-06]

- **Phase 2: Runtime Lifecycle And Checkpoint Gate**
  - Scope: add volatile table lifecycle state, foreground lifecycle checks, and
    checkpoint publish lease/cancellation protocol.
  - Goals: reject stale foreground handles after locks, add the terminal
    `Live -> Dropping -> Dropped` runtime barrier, prevent new checkpoint
    publication once drop starts, wait for the single active checkpoint
    publisher when present, cancel checkpoint at the publish boundary, and
    poison ambiguous publication failures.
  - Non-goals: no physical file deletion; no background logical-lock
    acquisition; no index DDL.
  - Task Doc: `docs/tasks/000143-runtime-lifecycle-and-checkpoint-gate.md`
  - Task Issue: `#619`
  - Phase Status: done
  - Implementation Summary: Implemented terminal table lifecycle state, foreground stale-handle rejection, and a single-state checkpoint publish/drop gate with cancellation and fatal handling for ambiguous publication failures. [Task Resolve Sync: docs/tasks/000143-runtime-lifecycle-and-checkpoint-gate.md @ 2026-05-07]

- **Phase 3: Drop Table DDL**
  - Scope: implement `Session::drop_table(table_id)` with DDL lock order,
    catalog cascade deletes, `DDLRedo::DropTable`, runtime removal, catalog
    checkpoint rule change, restart validation, and drop behavior tests.
  - Goals: support logical `DROP TABLE` end to end with crash-safe recovery.
  - Non-goals: no runtime table destroy, buffer-pool page reclamation, GC
    integration, table-file unlink/quarantine, table-id reuse, drop by name,
    SQL parser surface beyond existing storage API conventions, or index DDL.
  - Task Doc: `docs/tasks/000144-drop-table-ddl.md`
  - Task Issue: `#621`
  - Phase Status: done
  - Implementation Summary: Implemented the storage-level
    `Session::drop_table(table_id)` DDL path with DDL lock ordering, catalog
    cascade deletion, `DDLRedo::DropTable`, runtime removal,
    checkpoint/recovery validation, and source-preserving poison diagnostics
    for post-gate failures. [Task Resolve Sync:
    docs/tasks/000144-drop-table-ddl.md @ 2026-05-09]

- **Phase 4: GC-Managed Dropped Table Destroy**
  - Scope: add consuming table/block-index destroy primitives, attach removed
    dropped-table runtime handles to transaction GC, reclaim in-memory table
    resources after `min_active_sts > drop_cts`, and delete/quarantine table
    files only after `catalog_replay_start_ts > drop_cts`.
  - Goals: reclaim dropped-table memory and eventually disk space without making
    logical `DROP TABLE` wait for stale handles, GC, or catalog checkpoint.
  - Non-goals: no table-id reuse, durable tombstone table, standalone
    dropped-table reclaimer, synchronous destroy inside `Session::drop_table`,
    or index DDL.
  - Task Doc: `docs/tasks/000145-gc-managed-dropped-table-destroy.md`
  - Task Issue: `#623`
  - Phase Status: done
  - Implementation Summary: Implemented transaction-GC-managed dropped-table runtime destroy, checkpoint-gated table-file deletion, recovery/startup cleanup, and focused tests; deferred non-busy retry wake policy to backlog 000098. [Task Resolve Sync: docs/tasks/000145-gc-managed-dropped-table-destroy.md @ 2026-05-10]

## Consequences

### Positive

- `DROP TABLE` can be implemented without adding a durable tombstone table.
- Foreground DDL remains aligned with the existing logical lock manager.
- Background checkpoint cancellation is explicit and does not block DDL behind
  long logical-lock waits.
- Recovery gets a precise rule for old redo belonging to checkpoint-absent
  tables.
- Dropped-table reclamation has a clear follow-up phase that reuses the existing
  GC horizon and keeps physical deletion behind restart-safe catalog negative
  knowledge.

### Negative

- Physical disk space and hot buffer-pool pages for dropped tables are not
  reclaimed by phase 3.
- Runtime table access paths must add lifecycle checks after lock acquisition.
- Table checkpoint needs a new lifecycle/cancellation boundary.
- Phase 4 must add table/block-index destroy code and retry behavior for stale
  runtime handles.
- `CREATE INDEX` and `DROP INDEX` remain unimplemented and require a separate
  RFC.

## Open Questions

- What exact error variant should foreground access return for `Dropping` and
  `Dropped` handles?
- Should checkpoint cancellation reuse `CheckpointOutcome::Delayed`, add a new
  `CheckpointOutcome::Cancelled`, or return a normal operation error?
- Should recovery diagnostics for skipped unknown-table redo be counters, trace
  logs, test-only counters, or all three?

## Future Work

- Dedicated RFC for offline `CREATE INDEX` and `DROP INDEX`, including
  table-root/catalog atomicity, sparse index slots, stable `index_no`
  allocation, and post-index-DDL DML guarantees.
- Crash-idempotent dropped table-file quarantine/unlink and startup cleanup.
- Lifecycle garbage collection for any future durable lifecycle ledger.
- Table-id reuse or generation-aware object identity, if the storage engine ever
  needs it.

## References

- `docs/rfcs/0016-logical-lock-manager.md`
- `docs/rfcs/0006-cache-first-unified-catalog-storage-refactor.md`
- `docs/checkpoint-and-recovery.md`
- `docs/table-file.md`
- `docs/drafts/table-index-ddl-lifecycle-recovery-rfc-review.md`
