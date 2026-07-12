---
id: 000220
title: Move Table Checkpoint Completion to No-Wait System Transactions
status: implemented
created: 2026-07-11
github_issue: 839
---

# Task: Move Table Checkpoint Completion to No-Wait System Transactions

## Summary

Replace the normal session transaction used to finish a user-table checkpoint
with the existing sessionless `SysTrx` and its no-wait group-commit path. Keep
one system-transaction type for both row-page creation and checkpoint work.
Allocate a separate non-active checkpoint timestamp for table-file construction,
publish the durable table root or live silent-watermark mirror, enqueue the
matching system redo, and return after enqueue acceptance without waiting for
redo persistence.

Move retired `gc_row_pages` out of normal transaction effects and into
`SysTrx`, but reuse the existing committed-transaction purge pipeline and its
round-wide ordering: every eligible GC bucket must finish row-undo and index
purge before the dispatcher deallocates any retired row pages. A system GC
payload has no STS to remove; its ordered system CTS is the reclamation fence.

For silent checkpoints, add a no-transaction catalog primary-key upsert with an
insert/update callback. `SysTrx::upsert_silent_watermark` supplies the catalog
and pool guards, performs the monotonic live-mirror update without undo, and
uses the callback to generate logical catalog redo as `Insert` or
`UpdateByPrimaryKey`. Only persisted redo folded into `catalog.mtb` remains
recovery and redo-truncation proof.

## Context

Issue Labels:
- type:task
- priority:medium
- codex

Source Backlogs:
- docs/backlogs/closed/000154-move-table-checkpoint-completion-to-no-wait-system-transactions.md

Related implemented design:
- `docs/tasks/000199-catalog-silent-table-replay-watermarks.md`
- `docs/tasks/000217-redesign-checkpoint-frozen-page-cutoff-validation.md`
- `docs/tasks/000218-unify-table-freeze-checkpoint-workflow-state.md`
- `docs/tasks/000169-fix-failed-precommit-redo-cleanup.md`
- `docs/checkpoint-and-recovery.md`
- `docs/transaction-system.md`
- `docs/table-file.md`
- `docs/redo-log.md`

Current table checkpoint opens a normal transaction before final validation.
That transaction currently supplies four services:

1. an early STS used as the checkpoint construction/publication timestamp;
2. `DataCheckpoint` or silent-watermark DDL/DML redo;
3. ownership of retired row pages through `TrxEffects::gc_row_pages`;
4. a durability waiter through normal transaction commit.

The checkpoint consistency boundary itself no longer depends on a normal
transaction. The table-owned checkpoint workflow, frozen-page cutoff
validation, purge-published cutoff, root-mutation lease, and lifecycle publish
lease define what may be published and order checkpoint against DROP. The
remaining normal-transaction services can be supplied without an active STS or
rollback-capable transaction.

The existing `SysTrx` is currently a redo-only, sessionless transaction used by
row-page creation. It receives a system CTS only when it enters ordered group
commit and returns immediately after accepted enqueue. It has no active STS,
transaction status, session attachment, undo, or rollback path. This task keeps
that single type and extends it only with checkpoint redo helpers,
silent-watermark catalog access, and optional retired-row-page ownership.

The current purge implementation already carries `gc_row_pages` from a
committed transaction payload into a GC bucket, drains eligible transactions by
`cts < global_min_active_sts`, purges row undo and index entries across every
bucket, and only then deallocates the collected pages. This round-wide barrier
is required because an undo payload in a different GC bucket may still point at
a page retired by the checkpoint transaction. The task must reuse this
mechanism rather than introduce a checkpoint-specific GC queue or per-bucket
page deallocation.

The live `catalog.table_replay_silent_watermarks` row is an inspection mirror,
not durable proof. Catalog checkpoint and recovery consume the matching
logical catalog DML from persisted redo. A silent system transaction must
therefore emit the same `Insert` or `UpdateByPrimaryKey` operation represented
by the no-transaction live mutation; emitting only
`DDLRedo::TableReplaySilentWatermark` would be incorrect.

The current `CheckpointOutcome::Published` timestamp is also ambiguous. A root
checkpoint returns the normal transaction STS used to build the root, while a
silent checkpoint returns the normal transaction commit CTS. Once checkpoint
uses no-wait system commit, construction/publication time and ordered redo time
must be reported separately, and the returned redo CTS is accepted rather than
necessarily durable.

Resolved alternatives:

- Do not keep or detach a normal checkpoint transaction merely to reuse its STS,
  MVCC catalog upsert, GC payload, and waiter. That retains active-STS and
  rollback/session machinery that checkpoint no longer needs.
- Do not introduce `CheckpointSysTrx` or a generic maintenance-transaction
  hierarchy. Extend the one existing `SysTrx` with only the approved narrow
  operations.
- Do not create a checkpoint-specific GC queue or deallocate pages as soon as
  one bucket finishes. Reuse the existing committed-payload pipeline and wait
  for every eligible bucket's undo/index purge before page deallocation.
- Do not pass `&mut SysTrx` into the catalog watermark accessor. Put the public
  orchestration on `SysTrx::upsert_silent_watermark`, pass catalog/pool access
  into it, and let the lower no-transaction table upsert report successful
  insert/update details through a callback that `SysTrx` converts into redo.
- Do not emit only the silent-watermark DDL marker. Catalog checkpoint and
  recovery require the matching logical `Insert` or `UpdateByPrimaryKey` DML.

## Goals

1. Remove the normal transaction and foreground redo-durability wait from
   user-table checkpoint completion.
2. Keep one `SysTrx` type and one `RedoTrxKind::System` for row-page creation,
   root checkpoint redo, and silent-watermark checkpoint redo.
3. Allocate a non-active `checkpoint_ts` before checkpoint construction without
   inserting it into any active-STS list.
4. Keep `checkpoint_ts` distinct from the ordered system `redo_cts` assigned at
   group-commit enqueue.
5. Preserve root checkpoint durability: return only after the table-file root
   is durable, runtime routing is installed, and system redo is accepted.
6. Preserve silent checkpoint semantics: return only after the monotonic live
   catalog mirror is updated and matching system DDL/DML redo is accepted.
7. Move checkpoint-retired `gc_row_pages` out of normal `TrxEffects` and into
   `SysTrx`/prepared/precommit/committed ownership.
8. Reuse the existing GC buckets, `Purge::Committed` handoff, global snapshot
   horizon, all-bucket undo/index purge barrier, page collection, and dispatcher
   deallocation.
9. Let system GC payloads participate in purge without registering or removing
   an active STS; use their ordered CTS as the reclamation fence.
10. Generate silent-watermark catalog redo through a callback invoked by the
    no-transaction primary-key upsert, using logical `Insert` or
    `UpdateByPrimaryKey` kinds.
11. Preserve the rule that only checkpointed catalog state, reconstructed from
    persisted redo, authorizes recovery replay skipping or redo truncation.
12. Preserve checkpoint-before-DROP ordering by retaining table publish
    admission through successful system enqueue.
13. Let standalone and combined catalog checkpoint consume the current
    persisted redo prefix without forcing pending system redo to durability;
    accepted redo above the sampled watermark waits for a later periodic run.
14. Preserve fatal fail-closed behavior after irreversible root or mirror
    publication.

## Non-Goals

1. Do not introduce `CheckpointSysTrx`, multiple system-transaction kinds, or a
   generic maintenance-transaction framework.
2. Do not add an STS, shared user transaction status, session attachment,
   statement executor, undo log, logical locks, or rollback capability to
   `SysTrx`.
3. Do not redesign the existing purge queue, GC bucket layout, round-wide
   undo/index-before-page-deallocation barrier, page deduplication, or physical
   deallocation policy.
4. Do not add a checkpoint-specific GC queue or deallocate retired pages from
   an individual purge bucket before all eligible buckets finish their undo and
   index work for the round.
5. Do not change the serialized redo format or add a new silent-watermark DDL
   payload format. Reuse `DDLRedo::DataCheckpoint`,
   `DDLRedo::TableReplaySilentWatermark`, `RowRedoKind::Insert`, and
   `RowRedoKind::UpdateByPrimaryKey`.
6. Do not make the live silent-watermark row a recovery or truncation proof.
7. Do not remove the public mutable-session API or idle-session admission check
   for table checkpoint.
8. Do not change frozen-page selection, cutoff validation, table-root
   reachability reclamation, secondary `DiskTree` companion work, DROP
   lifecycle ownership, or recovery replay predicates beyond timestamp and
   acknowledgement wording required by this task.
9. Do not add automatic checkpoint scheduling, catalog checkpoint scheduling,
   or redo truncation scheduling.

## Plan

### 1. Separate checkpoint construction time from ordered redo time

Add a transaction-system helper that allocates one timestamp from the existing
global sequence without registering it in a GC bucket active-STS list. Name and
document it as a checkpoint construction/publication timestamp rather than a
transaction STS.

`Table::checkpoint_inner` obtains this `checkpoint_ts` where it currently opens
the normal checkpoint transaction. Continue to use it for:

- fallback `heap_redo_start_ts` when no successor page supplies a timestamp;
- LWC, cold-delete, and secondary `DiskTree` checkpoint writers;
- the user-table root `root_ts`;
- the timestamp carried by `DDLRedo::DataCheckpoint`.

The system `redo_cts` remains allocated later by `enqueue_commit`. It orders the
checkpoint redo, DROP, and GC payload but is not substituted into table-file
structures that must be built before enqueue. Rename the Rust field
`DDLRedo::DataCheckpoint::sts` to `checkpoint_ts` if this can be done without
changing serialization; otherwise update its documentation and local variable
names to make the meaning explicit.

### 2. Extend the single existing `SysTrx`

Keep `doradb-storage/src/trx/sys_trx.rs::SysTrx` as the only system transaction
type. Extend its owned state to include:

```rust
pub(crate) struct SysTrx {
    gc_no: usize,
    redo: RedoLogs,
    gc_row_pages: Vec<PageID>,
}
```

Allocate `gc_no` when `SysTrx` begins from a system-only round-robin sequence.
This keeps user GC-bucket distribution independent from system transactions
whose preallocated bucket remains unused because they produce no purge payload.

Add narrow helpers:

- `create_row_page(...)`, preserved for the existing producer;
- `record_data_checkpoint(table_id, pivot_row_id, checkpoint_ts)`, which only
  installs `DDLRedo::DataCheckpoint` because the table-file work is performed
  separately;
- `extend_gc_row_pages(Vec<PageID>)`;
- async `upsert_silent_watermark(&Catalog, &PoolGuards,
  SilentWatermarkObject)`, described below.

Do not give `SysTrx` a general mutable redo accessor, catalog statement facade,
or rollback semantics. `commit_sys` must reject the internal invariant where
`gc_row_pages` is non-empty but redo is empty: retired pages must be coupled to
an ordered recovery-visible checkpoint record.

Existing row-page creation continues to build a `SysTrx` with an empty page
vector and produces no purge payload. A root checkpoint builds the same
`SysTrx` with `DataCheckpoint` redo and retired pages. A silent checkpoint uses
the same `SysTrx` with silent-watermark DDL/DML and normally has no retired
pages.

### 3. Move system GC ownership through the existing commit payload pipeline

Remove checkpoint-page accumulation from normal `TrxEffects` and remove the
normal transaction API used only by table checkpoint to extend
`gc_row_pages`.

Adapt the existing prepared and precommit payload ownership so a payload may
carry either user lifecycle state or only system GC state. One acceptable shape
is a common payload containing:

- optional user state with `SharedTrxStatus`, STS, row undo, and index undo;
- `gc_no` for purge sharding;
- `gc_row_pages`.

Normal transaction payloads have user state and an empty checkpoint-page list.
Checkpoint system payloads have no user state or attachment, empty row/index
undo, and a non-empty page list. Row-page creation system transactions retain
`payload: None`.

On successful ordered completion:

- user payloads continue to backfill status, complete the session, convert
  index undo into index GC, and carry `Some(sts)`;
- checkpoint system payloads perform no status/session work and carry
  `None` for STS;
- both use the existing `CommittedTrxPayload`, `gc_no`, and
  `Purge::Committed` handoff.

Update `GCBucket::record_committed_for_purge` so it always queues committed
payloads, but removes an active STS only when `CommittedTrx::sts()` returns
`Some`. A system GC handoff must request a full purge observation even though
it did not itself advance an active-STS bucket minimum. Eligibility remains the
existing `committed.cts < global_min_active_sts` rule.

On rejected or failed system precommit, clear/drop the attachmentless system
payload without running user rollback or active-STS removal. Do not deallocate
its pages through purge or direct page deallocation: after irreversible
checkpoint publication, the engine is poisoned and normal engine teardown
releases the arena backing those allocations. Preserve existing user
failed-precommit rollback and fatal-retention behavior unchanged.

### 4. Reuse the existing all-bucket purge and deallocation mechanism

Do not create a new page queue, task protocol, or early per-bucket deallocation
path. Keep the existing purge round:

1. calculate the global active-snapshot horizon;
2. take every bucket's committed prefix with `cts < horizon`;
3. purge row undo and index GC from every eligible bucket;
4. collect `CommittedTrxPayload::gc_row_pages` through the existing mechanism;
5. only after all single-threaded bucket passes or all multi-threaded executor
   tasks succeed, deallocate the collected pages in the dispatcher/owner;
6. poison storage on purge access or page-deallocation failure.

This barrier must remain explicit in comments and tests because eligible undo
in one bucket may refer to a page retired by a system transaction assigned to a
different bucket. Preserve the current page deduplication unless an independent
future task justifies changing it.

### 5. Add callback-driven no-transaction catalog upsert

Add or refactor an internal `CatalogTable` primary-key upsert that mutates the
catalog row store and indexes without transaction status, undo, or redo of its
own. It accepts a callback describing the successful insert or update. The
callback contract is:

- invoked at most once;
- invoked only after the row and index mutation succeeds;
- not invoked when the requested full row is already represented;
- the final infallible step of a successful mutation;
- supplied with the actual `PageID`/`RowID` envelope and owned logical values
  needed to construct redo.

Use a callback event equivalent to:

```rust
enum NoTrxUpsertChange {
    Inserted {
        page_id: PageID,
        row_id: RowID,
        vals: Vec<Val>,
    },
    Updated {
        page_id: PageID,
        row_id: RowID,
        key: SelectKey,
        cols: Vec<UpdateCol>,
    },
}
```

The internal upsert validates the catalog table primary key, performs direct
row/index insert or primary-key update, handles any existing no-transaction
relocation behavior, and reports the final affected location. It must not call
the MVCC `Statement` APIs or generate undo.

Keep this helper internal to catalog/no-transaction use. Do not expose a public
general-purpose no-transaction DML surface.

### 6. Implement `SysTrx::upsert_silent_watermark`

Implement the silent operation on `SysTrx`, accepting catalog access and the
checkpoint caller's `PoolGuards`:

```rust
pub(crate) async fn upsert_silent_watermark(
    &mut self,
    catalog: &Catalog,
    guards: &PoolGuards,
    watermark: SilentWatermarkObject,
) -> Result<()>;
```

The method must:

1. reject an already-populated incompatible DDL slot before mutating catalog
   state;
2. select `catalog.table_replay_silent_watermarks` through `Catalog`;
3. preserve the existing monotonic/fieldwise watermark rule and never regress
   either replay floor;
4. invoke the no-transaction primary-key upsert;
5. capture the successful-change callback into `self.redo`;
6. generate `RowRedoKind::Insert(full_row_values)` for a missing watermark;
7. generate `RowRedoKind::UpdateByPrimaryKey(primary_key, changed_columns)` for
   an existing watermark;
8. generate no DML when the live row already covers the requested watermark;
9. install `DDLRedo::TableReplaySilentWatermark { table_id }` even when no DML
   is needed, so the system transaction remains a recovery-visible ordered
   checkpoint marker.

The `RowRedo` page/row fields carry the actual affected in-memory location to
fit the existing envelope, but catalog checkpoint and recovery interpret the
operation logically through `Insert` or `UpdateByPrimaryKey`. Do not emit a
physical `Update` kind for catalog watermark replacement.

`SysTrx` receives catalog and pool access only for this narrow method. Do not
store `Catalog`, `EngineRef`, session state, or pool guards in every `SysTrx`;
the existing synchronous row-page creation path must remain lightweight.

### 7. Replace normal checkpoint commit with no-wait system enqueue

Preserve the public idle-session check and use the session pin only for engine,
catalog, and pool access. Do not call `session.begin_trx` from table checkpoint.

For reversible preparation errors or delays before page transition/publish
admission, drop the uncommitted `SysTrx` and return through the existing
workflow restoration path. There is no rollback operation to invoke.

For a root-publishing checkpoint:

1. build table-file, deletion, secondary-index, and reachability state using
   `checkpoint_ts`;
2. record `DataCheckpoint` redo and retired page IDs in `SysTrx`;
3. acquire/retain the existing irreversible workflow and lifecycle publish
   guards;
4. durably publish the table-file root with `root_ts = checkpoint_ts`;
5. install the block-index pivot/column route and old-root retention state;
6. enqueue `SysTrx` through `commit_sys` without a waiter;
7. retain publish admission until enqueue succeeds, then finish workflow state
   and return.

For a silent checkpoint:

1. abandon the replay-bound-only mutable table-file fork;
2. acquire publish admission and mark the attempt irreversible before the
   no-transaction catalog mutation;
3. call `SysTrx::upsert_silent_watermark` to update the live mirror and build
   matching logical redo;
4. enqueue the same `SysTrx` through `commit_sys` without a waiter;
5. retain publish admission until enqueue succeeds, then finish workflow state
   and return.

Holding the publish lease through enqueue preserves checkpoint-before-DROP
ordering. DROP may begin its normal transaction before waiting for the
publisher, but its CTS is assigned only when it commits after the drain. Its
durability wait therefore covers the earlier accepted system redo prefix.

### 8. Use the current persisted prefix for catalog maintenance

A table checkpoint now returns before `persisted_watermark_cts` necessarily
reaches its accepted silent `redo_cts`. Catalog checkpoint is periodic
maintenance for reducing recovery work and enabling redo truncation, so it does
not need to force a just-accepted system transaction to durability.

Use `persisted_watermark_cts()` as the conservative catalog scan upper bound in
both:

- `Catalog::checkpoint_now`;
- `TransactionSystem::checkpoint_catalog_and_truncate_redo_log`.

Preserve catalog-checkpoint and redo-retention gate ordering. Redo accepted but
not durable at the sampled watermark is excluded safely and remains available
for a later checkpoint. Prefer maintenance scheduling after redo-file rotation
and sealing, when the sealed prefix provides useful durable progress, but do
not add an automatic scheduler in this task. Catalog checkpoint must not enqueue
an empty transaction solely to advance the durable upper bound.

### 9. Correct checkpoint outcome and acknowledgement semantics

Keep `CheckpointOutcome::Published`, but change its fields/documentation to:

```rust
Published {
    checkpoint_ts: TrxID,
    redo_cts: TrxID,
    silent: bool,
}
```

- `checkpoint_ts` is the non-active timestamp used to build/publish table
  checkpoint state or requested silent replay bounds;
- `redo_cts` is the system CTS accepted into ordered group commit before the
  call returns;
- `redo_cts` is not by itself a durability acknowledgement;
- `silent: false` means a durable user-table root and runtime route were
  published before redo acceptance;
- `silent: true` means the live catalog mirror was updated before redo
  acceptance, but only a later catalog checkpoint makes the logical watermark
  durable proof.

Update logs, examples, tests, and public re-exports/comments that currently call
`checkpoint_ts` a normal transaction commit timestamp or describe every
`Published` result as redo-durable.

### 10. Preserve fatal failure and shutdown ownership

Before irreversible publication, ordinary errors may return normally with the
checkpoint workflow restored. After the first page transition, root
publication, or no-transaction silent-mirror mutation becomes irreversible:

- one publication guard owns workflow restoration, lifecycle publish admission,
  and the current fatal reason across `Publishing` and `Transition`;
- silent catalog mutation failure or cancellation poisons storage as
  `FatalError::CatalogWrite`;
- synchronous system enqueue rejection poisons storage as checkpoint failure;
- asynchronous redo write/sync failure uses the existing redo poison path;
- failed system payload cleanup performs no rollback and never hands its pages
  to purge;
- rejected payload page IDs are dropped without deallocating their pages
  against uncertain publication state; poisoned-engine teardown releases the
  backing arena;
- workflow/drop waiters wake through the existing poison and guard paths.

Preserve worker shutdown ordering: group-commit admission closes, redo drains
and joins, every successful committed payload is handed non-lossily to purge,
and only then does purge receive its terminal stop marker.

### 11. Synchronize design documentation

Update:

- `docs/transaction-system.md` for system GC payloads without STS and no-wait
  checkpoint acknowledgement;
- `docs/checkpoint-and-recovery.md` for root versus silent system checkpoint
  flows, DROP ordering, live-mirror versus durable proof, and page reclamation;
- `docs/table-file.md` so checkpoint root timestamps are checkpoint publication
  timestamps rather than normal transaction STSs;
- `docs/redo-log.md` for accepted system CTS and conservative catalog scan
  upper-bound selection;
- public API comments and examples that describe `CheckpointOutcome`.

## Implementation Notes

- Replaced checkpoint completion with the existing no-wait `SysTrx` path.
  Checkpoint construction now allocates a non-active `checkpoint_ts`, while
  ordered group commit assigns the distinct accepted `redo_cts`; neither root
  nor silent checkpoint waits for redo persistence.
- Final transaction payload ownership uses explicit `User` and `System` enum
  variants rather than an optional-user common payload. `SysTrxPayload`,
  defined with `SysTrx`, carries the system GC bucket and retired row pages
  unchanged through prepare, precommit, ordered completion, and purge handoff.
  Every `SysTrx` allocates a GC number, while redo-only system transactions
  omit the payload when they own no retired pages.
- Added callback-driven no-transaction catalog primary-key upsert and used it
  for monotonic silent-watermark mutation. Successful inserts and updates emit
  matching logical catalog DML, while an already-covered watermark retains the
  ordered DDL marker without redundant DML.
- Consolidated irreversible checkpoint restoration, lifecycle publication
  admission, and poison classification in `CheckpointPublicationGuard`.
  Silent-watermark mutation failures use `FatalError::CatalogWrite`; later
  root/enqueue failures use the checkpoint failure classification. A rejected
  post-publication system payload is dropped without rollback or purge page
  handoff; storage is poisoned, and normal engine teardown releases the arena
  allocations.
- Catalog checkpoint and combined checkpoint/truncation now sample
  `persisted_watermark_cts()` directly. No empty transaction is committed to
  manufacture a newer durable upper CTS; accepted redo above the sampled
  prefix remains for a later periodic catalog checkpoint.
- Review cleanup removed checkpoint-only MVCC and test mutation APIs, kept
  reserved no-transaction table APIs under test-aware dead-code expectations,
  flattened catalog checkpoint tests into their owner module, and colocated
  focused `SysTrx` tests with `sys_trx.rs`.
- Deferred eligibility-aware full-GC triggering and dispatcher participation
  as a configured purge worker to
  `docs/backlogs/000157-optimize-purge-full-gc-triggering-and-dispatcher-worker-utilization.md`.
- Validation completed:
  - `cargo check -p doradb-storage --tests`
  - focused transaction/purge tests: 4 passed
  - focused checkpoint publication-guard/failure tests: 6 passed
  - catalog storage tests: 21 passed
  - workspace nextest validation: 1,352 tests passed with one known checkpoint
    concurrency case run separately; that exact test passed independently
  - libaio nextest validation: 1,276 tests passed
  - `tools/style_audit.rs --diff-base origin/main`: 23 branch-diff Rust files
    passed

## Impacts

- `doradb-storage/src/trx/sys_trx.rs`
  - extend the single `SysTrx` with optional retired pages, root-checkpoint redo,
    and async silent-watermark catalog upsert
- `doradb-storage/src/trx/sys.rs`
  - allocate non-active checkpoint timestamps, prepare system GC payloads,
    preserve no-wait commit, and expose the persisted watermark for catalog scans
- `doradb-storage/src/trx/mod.rs`
  - remove checkpoint pages from normal effects, permit attachmentless system
    GC payload ownership through prepared/precommit/committed states, and split
    user versus system failure cleanup behavior
- `doradb-storage/src/trx/purge.rs`
  - minimally admit committed payloads without STS while preserving the
    existing bucket queues and all-bucket deallocation barrier
- `doradb-storage/src/log/mod.rs`
  - hand successful system GC payloads to `Purge::Committed`; retain ordered
    completion and poison semantics
- `doradb-storage/src/catalog/storage/table_replay_silent_watermarks.rs`
  - monotonic no-transaction watermark orchestration used by `SysTrx`
- `doradb-storage/src/table/mem_table.rs`
  - callback-driven no-transaction primary-key upsert and affected-location
    reporting for logical catalog redo generation
- `doradb-storage/src/catalog/checkpoint.rs`
  - sample the current persisted watermark before standalone catalog scan
- `doradb-storage/src/trx/retention.rs`
  - use the same persisted-prefix snapshot in combined catalog checkpoint and
    redo truncation
- `doradb-storage/src/table/persistence.rs`
  - replace normal checkpoint transaction construction/rollback/commit with
    one non-active timestamp and unified no-wait `SysTrx`; use one publication
    guard for reversible restoration, fatal phase classification, and lifecycle
    publish admission
- `doradb-storage/src/table/persistence.rs` callers and public API tests
  - consume separate `checkpoint_ts` and `redo_cts` outcome fields
- `doradb-storage/src/log/redo.rs`
  - clarify `DataCheckpoint` timestamp naming without changing encoding
- `docs/transaction-system.md`, `docs/checkpoint-and-recovery.md`,
  `docs/table-file.md`, and `docs/redo-log.md`
  - synchronize the accepted design and acknowledgement guarantees

## Test Cases

Timestamp and unified system transaction:

1. Allocating `checkpoint_ts` does not insert or remove an active STS and does
   not change normal transaction ownership counts.
2. A checkpoint's `checkpoint_ts` is allocated before its later accepted
   `redo_cts`; root `root_ts` equals `checkpoint_ts`.
3. Existing `CreateRowPage` uses the same `SysTrx`, returns through the no-wait
   path, and creates no GC payload.
4. A `SysTrx` with GC pages and no redo is rejected as an internal invariant.
5. A system transaction with redo but no pages still produces no purge handoff;
   allocating it advances only the system GC sequence and leaves user bucket
   distribution unchanged.

Root checkpoint acknowledgement:

6. With redo sync held by a deterministic backend hook, a root checkpoint
   durably publishes its root, installs the runtime route, returns `Published`
   with `silent: false`, and does not wait for the held sync.
7. The returned root checkpoint fields distinguish `checkpoint_ts` from
   accepted `redo_cts` and do not claim redo durability.
8. Restart after a durable root publication whose later system redo was not
   durable recovers from the root without requiring that marker.
9. Restart after both root and system redo are durable preserves the same table
   state and timestamp seeding.

Silent catalog mutation and logical redo:

10. Missing live watermark performs no-transaction insert and callback-generated
    `RowRedoKind::Insert` with the full logical row.
11. Existing live watermark performs a monotonic primary-key update and
    callback-generated `UpdateByPrimaryKey` with the table-id key and only
    changed floor columns.
12. An already-covered live watermark does not emit catalog DML but still emits
    the silent-watermark DDL marker.
13. No silent operation emits physical `Update` redo or uses MVCC statement
    effects, undo, user status, or session attachment.
14. The callback is not invoked on failed/no-op mutation and is invoked exactly
    once after a successful insert or update.
15. A controlled catalog no-transaction access/index failure or cancellation
    after irreversible admission poisons storage as `FatalError::CatalogWrite`
    and does not enqueue incomplete logical redo.
16. With redo sync held, a silent checkpoint updates the live inspection mirror,
    returns accepted `redo_cts`, leaves the checkpoint-durable overlay unchanged,
    and does not wait for sync.

DROP ordering and failure:

17. If checkpoint wins publish admission, concurrent DROP drains it, receives a
    later commit CTS, and its normal durability wait covers the earlier system
    redo prefix.
18. If DROP wins admission, checkpoint cancels before transition/root/mirror
    mutation and drops its unused `SysTrx` without rollback.
19. Synchronous system enqueue failure after durable root publication or silent
    mirror mutation poisons storage.
20. Injected asynchronous system redo write and sync failures poison storage,
    do not hand retired pages to purge, and wake lifecycle/workflow waiters.

Existing purge reuse:

21. A committed checkpoint system payload enters the existing GC bucket without
    attempting to remove an active STS.
22. A long-running reader with `sts <= redo_cts` prevents retired page
    deallocation; releasing it advances the horizon and permits reclamation.
23. A controlled multi-bucket round proves undo/index work in every eligible
    bucket completes before any checkpoint-retired page is deallocated.
24. Existing dispatcher page collection/deduplication and deallocation remain
    the single physical release point for single- and multi-thread purge modes.
25. Purge access or page-deallocation failure poisons storage through the
    existing failure boundaries.
26. Existing user transaction row-undo/index-GC purge and row-page checkpoint
    reclamation regressions continue to pass after checkpoint pages leave
    `TrxEffects`.

Catalog checkpoint, recovery, and truncation:

27. A standalone catalog checkpoint scans only through the sampled
    `persisted_watermark_cts`; pending silent redo may wait for a later run.
28. Combined catalog checkpoint and redo truncation uses the same persisted
    prefix and may use any newly checkpointed durable overlay in its projected
    truncation plan.
29. Redo accepted above the sampled persisted watermark is not required in the
    selected catalog batch and remains available for a later checkpoint.
30. Catalog checkpoint does not enqueue an empty ordered transaction to force
    a newer durable upper bound.
31. Restart before silent redo durability ignores the live-only mirror and uses
    table-root replay floors.
32. Restart after silent redo durability but before catalog checkpoint replays
    the logical catalog operation normally.
33. Restart after catalog checkpoint loads the checkpointed silent overlay and
    uses its effective replay floors.
34. Redo truncation remains blocked by root/checkpointed-overlay state and never
    uses an uncheckpointed live mirror as proof.

Workflow and regression validation:

35. Existing active-root delay, frozen cutoff delay, canonical batch retry,
    deletion-only/root-only publication, secondary-index sidecar, route waiter,
    and checkpoint/drop lifecycle tests continue to pass.
36. Concurrent tests use hooks, events, channels, or predicates rather than
    sleeps.
37. Run `cargo fmt --all --check`.
38. Run `cargo clippy --workspace --all-targets -- -D warnings`.
39. Run `cargo nextest run --workspace`.
40. Run `cargo nextest run -p doradb-storage --no-default-features --features
    libaio` because checkpoint publication and redo acknowledgement ordering are
    changed on backend-neutral I/O paths.
41. Run focused coverage for changed transaction, purge, table persistence,
    catalog watermark, catalog checkpoint, and retention files; meet the 80%
    focused coverage review bar or explain definition-heavy exceptions with
    covered consumers.
42. Run `git diff --check` and `tools/style_audit.rs --diff-base origin/main`
    before resolve/review.

## Open Questions

No blocking design questions remain for the approved scope.

Follow-up purge scheduling and worker-utilization optimization is tracked in
`docs/backlogs/000157-optimize-purge-full-gc-triggering-and-dispatcher-worker-utilization.md`.
