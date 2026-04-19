---
id: 0015
title: Transaction Context Effects Root Proofs
status: proposal
tags: [storage, transaction, checkpoint, safety]
created: 2026-04-19
github_issue: 576
---

# RFC-0015: Transaction Context Effects Root Proofs

## Summary

The transaction and statement APIs will be split into immutable read identity
and mutable effect accumulators so runtime table-root access can be gated by a
typed `TrxReadProof<'ctx>` without blocking statement or transaction mutation.
The program is deliberately phased: first classify and narrow current active-root
access boundaries, then split `ActiveTrx` and `Statement`, then migrate
`TableAccess` to accept immutable context for reads and immutable context plus
mutable effects for writes, and finally require proof-gated `TableRootSnapshot`
usage for runtime table-file root reads.

## Context

User-table checkpoint publication can swap the in-memory active table-file root.
Runtime code that reads multiple fields from `active_root()` must therefore bind
one root view, otherwise a checkpoint can let one logical operation combine
fields from different published roots. Recent checkpoint work retained old roots
through transaction GC but intentionally deferred proof-gated runtime access
because the current transaction API does not have a borrow shape that can support
both immutable root-read proofs and mutable statement effects.

Today `Statement` owns the whole `ActiveTrx`, while `ActiveTrx` mixes immutable
identity (`sts`, status handle, session/runtime handles, log/gc partition
identity) with mutable effects (undo logs, redo logs, GC pages, retained old
roots). A proof borrowed from the whole transaction would conflict with later
`&mut Statement` or `&mut ActiveTrx` use. The API must therefore expose
transaction read identity separately from effect mutation before a typed proof
can provide meaningful lifetime safety.

The broadest API impact is `TableAccess`. Its read methods currently accept
`&Statement`; its write methods accept `&mut Statement`. That shape hides which
operations need only transaction identity and which operations also mutate
statement effects. The refactor will make this split explicit.

Issue Labels:
- type:epic
- priority:high
- codex

## Design Inputs

### Documents

- [D1] `docs/architecture.md` - storage subsystem boundaries and the mixed
  in-memory row store plus checkpointed column-store model.
- [D2] `docs/transaction-system.md` - MVCC transaction identity, undo/redo, and
  no-steal/no-force persistence model.
- [D3] `docs/index-design.md` - row id, block index, and secondary-index
  ownership boundaries for hot/cold access.
- [D4] `docs/checkpoint-and-recovery.md` - checkpoint publishes CoW table roots
  while foreground commit remains memory and redo driven.
- [D5] `docs/table-file.md` - table-file checkpoint root publication, delayed
  publication rules, and old-root retention requirements.
- [D6] `docs/garbage-collect.md` - root reachability and cleanup proofs must be
  based on captured table snapshots rather than moving current roots.
- [D7] `docs/process/issue-tracking.md` - document-first planning and RFC/task
  breakdown requirements for large architectural changes.
- [D8] `docs/process/unit-test.md` - validation workflow, including
  `cargo nextest run -p doradb-storage`.
- [D9] `docs/tasks/000124-checkpoint-old-root-retention.md` - old-root retention
  fix and explicit deferral of proof-gated active-root APIs.
- [D10] `docs/tasks/000125-checkpoint-root-liveness-gate.md` - checkpoint
  liveness gate context and active-root lifetime considerations.

### Code References

- [C1] `doradb-storage/src/file/cow_file.rs` - `CowFile::active_root()` loads
  the current active root through an atomic pointer; active root reclamation is
  tied to old-root ownership.
- [C2] `doradb-storage/src/file/table_file.rs` - `ActiveRoot` combines generic
  CoW root state with table metadata fields such as secondary-index roots,
  column-block-index root, pivot row id, and deletion cutoff.
- [C3] `doradb-storage/src/trx/mod.rs` - `ActiveTrx` currently mixes immutable
  transaction identity and mutable transaction effects.
- [C4] `doradb-storage/src/stmt/mod.rs` - `Statement` currently owns the whole
  `ActiveTrx` plus statement-level row undo, index undo, and redo logs.
- [C5] `doradb-storage/src/table/access.rs` - `TableAccess` currently takes
  `&Statement` for reads and `&mut Statement` for writes and reaches through the
  statement for both identity and effects.
- [C6] `doradb-storage/src/trx/row.rs` - row MVCC helpers currently take
  `&ActiveTrx` for visibility and `&mut Statement` for write-lock undo effects.
- [C7] `doradb-storage/src/table/persistence.rs` - checkpoint readiness and
  checkpoint publication read active-root fields and then publish replacement
  table-file roots.
- [C8] `doradb-storage/src/table/gc.rs` - secondary MemIndex cleanup already
  captures an owned active-root field snapshot as a temporary substitute for a
  proof-gated root borrow.
- [C9] `doradb-storage/src/index/secondary_index.rs` - secondary DiskTree open
  paths read the current published secondary root through `active_root()`.
- [C10] `doradb-storage/src/table/recover.rs` - recovery uses active-root
  metadata in a bootstrap path that should remain an explicit unchecked
  exception.
- [C11] `doradb-storage/src/table/mod.rs` - table construction reads active-root
  metadata and secondary-index roots while building runtime table state.
- [C12] `doradb-storage/src/session.rs` - session-owned active insert page cache
  is internally locked and can be reached from immutable transaction context.
- [C13] `doradb-storage/src/trx/sys.rs` - commit and rollback consume active
  transaction effects and move or clear undo, redo, GC pages, and retained old
  roots.
- [C14] `doradb-storage/src/catalog/storage/tables.rs`,
  `doradb-storage/src/catalog/storage/columns.rs`, and
  `doradb-storage/src/catalog/storage/indexes.rs` - catalog storage accessors
  call `TableAccess` directly for catalog row inserts and deletes.
- [C15] `doradb-storage/src/catalog/mod.rs` - catalog loads user tables by
  reading active table-file roots for metadata validation, replay floors, and
  block-index initialization.
- [C16] `doradb-storage/src/catalog/storage/checkpoint.rs` and
  `doradb-storage/src/catalog/storage/mod.rs` - catalog checkpointing uses
  `MultiTableFile` snapshots and catalog-table root descriptors rather than
  user-table `TableRootSnapshot`.
- [C17] `doradb-storage/src/trx/recover.rs` - restart recovery reads active
  table roots while tracking loaded-table replay floors and replaying table DML.

### Conversation References

- [U1] Initial requirement: split transaction context and effects to build a
  typed proof for active-root lifetime safety while preserving transaction
  mutability.
- [U2] Direction decision: prefer the long-term phased RFC proposal instead of
  a narrow task document.
- [U3] Design concern: clarify the relationship between `Statement`,
  `StatementEffects`, `TrxContext`, and `TrxEffects`.
- [U4] Design concern: `TableAccess` has a large impact surface; immutable and
  mutable methods should accept different parameters and the RFC should analyze
  call-site changes.
- [U5] Naming decision: use `TrxReadProof` rather than `RootReadProof`.
- [U6] Phasing decision: avoid a confusing "Phase 0"; use normal phase
  numbering and treat root-access boundary inventory as a separate task.
- [U7] Design request: describe `TableRootSnapshot` in detail.
- [U8] Draft review concern: describe impact on other modules such as recovery
  and catalog, not only `TableAccess`.

### Source Backlogs

- [B1] `docs/backlogs/000093-transaction-context-effects-split-active-root-proofs.md`
  - original backlog for transaction context/effects split and typed active-root
  read proof.

## Decision

This RFC chooses a phased transaction API evolution that separates immutable
transaction read identity from mutable effects before enforcing proof-gated
runtime table-root access. The selected direction is the long-term evolution
proposal from the design discussion, with `TrxReadProof<'ctx>` replacing the
earlier `RootReadProof` name. [U1], [U2], [U5], [B1]

### Transaction And Statement Model

`ActiveTrx` remains the transaction lifecycle object, but its internal shape
will become:

```rust
pub struct ActiveTrx {
    ctx: TrxContext,
    effects: TrxEffects,
}
```

`TrxContext` is the immutable transaction identity and runtime view:

- transaction snapshot timestamp (`sts`)
- transaction id/status handle used for read-your-own-write and write-conflict
  checks
- session/runtime handles needed for engine access, pool guards, and active
  insert page cache access
- log and GC partition identity
- ability to mint `TrxReadProof<'ctx>`

`TrxEffects` is the transaction-level mutable accumulator:

- transaction-level row undo logs
- transaction-level index undo logs
- transaction-level redo logs
- row pages to GC after commit
- retained old table root moved through transaction GC

`Statement` remains the linear statement lifecycle object initially:

```rust
pub struct Statement {
    trx: ActiveTrx,
    effects: StatementEffects,
}
```

`StatementEffects` is the statement-local mutable accumulator:

- statement-level row undo logs
- statement-level index undo logs
- statement-level redo logs
- helper methods that push row undo, index undo, and redo entries

`Statement::succeed()` merges `StatementEffects` into `TrxEffects` and returns
`ActiveTrx`. `Statement::fail()` rolls back `StatementEffects`, discards
statement redo, and returns `ActiveTrx`. This keeps the current linear API while
allowing table internals to borrow `&TrxContext` and `&mut StatementEffects`
as disjoint fields. [C3], [C4], [C12], [C13], [U3]

This RFC does not require `Statement<'trx>` borrowing from an external
transaction in the first migration. That shape may be considered later, but it
would create more immediate call-site churn than needed to prove root-read
lifetime safety. [C4], [C5], [U3], [U4]

### `TrxReadProof`

`TrxReadProof<'ctx>` is a zero-sized or near-zero-sized witness borrowed from
`TrxContext`:

```rust
pub struct TrxReadProof<'ctx> {
    _ctx: PhantomData<&'ctx TrxContext>,
}

impl TrxContext {
    pub fn read_proof(&self) -> TrxReadProof<'_>;
}
```

The proof says: "this read of transaction-consistent runtime state is tied to a
live immutable transaction context." It is intentionally named after the
transaction read identity, not after any specific root object, so the same
proof concept can later guard other transaction-consistent read views if needed.
[U5], [B1]

`TrxReadProof` does not itself prove that one root is older than the statement
snapshot, nor that a GC cleanup horizon has been reached. Those semantic checks
remain explicit predicates on `TrxContext`, `TableRootSnapshot`, and GC horizon
values. [D5], [D6], [C8]

### `TableRootSnapshot`

`TableRootSnapshot<'ctx>` is the normal runtime read object produced from a
table and a `TrxReadProof<'ctx>`. It is an owned field snapshot of one captured
published table-file root, not a borrow of `ActiveRoot` and not a clone of all
CoW root internals.

Proposed shape:

```rust
pub struct TableRootSnapshot<'ctx> {
    table_id: TableID,
    root_trx_id: TrxID,
    root_meta_block_id: BlockID,
    metadata: Arc<TableMetadata>,
    pivot_row_id: RowID,
    column_block_index_root: BlockID,
    secondary_index_roots: Vec<BlockID>,
    heap_redo_start_ts: TrxID,
    deletion_cutoff_ts: TrxID,
    _proof: PhantomData<&'ctx TrxContext>,
}
```

The snapshot includes fields consumed by runtime reads, secondary-index opens,
checkpoint readiness, and cleanup proofs:

- `root_trx_id`: checkpoint id of the captured published root
- `root_meta_block_id`: meta block id for diagnostics and future validation
- `metadata`: schema and index metadata captured with the root
- `pivot_row_id`: hot/cold row boundary for persisted column rows
- `column_block_index_root`: persisted column-block-index root
- `secondary_index_roots`: persisted secondary DiskTree roots by index number
- `heap_redo_start_ts`: heap redo replay floor from the captured root
- `deletion_cutoff_ts`: cold-row deletion replay/cleanup boundary

The snapshot intentionally excludes `slot_no`, `alloc_map`, `gc_block_list`, and
`newly_allocated_ids`. Those are CoW file-publication and allocation internals,
not runtime table-read contracts. Keeping them out prevents table access from
depending on root mutation machinery. [C1], [C2], [U7]

Construction is proof-gated:

```rust
impl Table {
    pub(crate) fn root_snapshot<'ctx>(
        &self,
        proof: &TrxReadProof<'ctx>,
    ) -> Result<TableRootSnapshot<'ctx>>;
}
```

The snapshot owns its field values so it can cross `.await` points and coexist
with `&mut StatementEffects`. The lifetime parameter still ties root-derived
block ids and metadata to the transaction context that authorized the read.
[C5], [C8], [U4], [U7]

Common methods should make semantic predicates explicit:

```rust
impl<'ctx> TableRootSnapshot<'ctx> {
    pub fn root_trx_id(&self) -> TrxID;
    pub fn metadata(&self) -> &TableMetadata;
    pub fn pivot_row_id(&self) -> RowID;
    pub fn column_block_index_root(&self) -> BlockID;
    pub fn deletion_cutoff_ts(&self) -> TrxID;
    pub fn secondary_index_root(&self, index_no: usize) -> Result<BlockID>;

    pub fn has_column_root(&self) -> bool;
    pub fn root_is_visible_to(&self, sts: TrxID) -> bool;
}
```

The snapshot provides three guarantees:

1. Lifetime guarantee: runtime snapshot creation requires a
   `TrxReadProof<'ctx>`.
2. Single-root consistency: all fields come from one active-root load.
3. Async-friendly ownership: the snapshot does not borrow `ActiveRoot`.

It does not replace MVCC row visibility, deletion-buffer checks, undo-chain
walks, or GC horizon proofs. [D2], [D5], [D6], [C6], [C8], [U7]

### `TableAccess` API Migration

`TableAccess` will stop accepting `Statement` directly. Read-only MVCC methods
will accept immutable transaction context:

```rust
fn table_scan_mvcc<F>(
    &self,
    ctx: &TrxContext,
    read_set: &[usize],
    row_action: F,
) -> impl Future<Output = ()>;

fn index_lookup_unique_mvcc(
    &self,
    ctx: &TrxContext,
    key: &SelectKey,
    user_read_set: &[usize],
) -> impl Future<Output = Result<SelectMvcc>>;

fn index_scan_mvcc(
    &self,
    ctx: &TrxContext,
    key: &SelectKey,
    user_read_set: &[usize],
) -> impl Future<Output = Result<ScanMvcc>>;
```

Mutation methods will accept immutable transaction context and mutable statement
effects:

```rust
fn insert_mvcc(
    &self,
    ctx: &TrxContext,
    effects: &mut StatementEffects,
    cols: Vec<Val>,
) -> impl Future<Output = Result<InsertMvcc>>;

fn update_unique_mvcc(
    &self,
    ctx: &TrxContext,
    effects: &mut StatementEffects,
    key: &SelectKey,
    update: Vec<UpdateCol>,
) -> impl Future<Output = Result<UpdateMvcc>>;

fn delete_unique_mvcc(
    &self,
    ctx: &TrxContext,
    effects: &mut StatementEffects,
    key: &SelectKey,
    log_by_key: bool,
) -> impl Future<Output = Result<DeleteMvcc>>;
```

The call-site migration is expected to follow these rules:

- `stmt.trx.sts` becomes `ctx.sts()`.
- `stmt.trx.status()` becomes `ctx.status()`.
- `stmt.trx.is_same_trx(...)` becomes `ctx.is_same_trx(...)`.
- `stmt.pool_guards()` becomes `ctx.pool_guards()`.
- `stmt.load_active_insert_page(...)` and
  `stmt.save_active_insert_page(...)` become context/session helpers.
- `stmt.row_undo`, `stmt.index_undo`, and `stmt.redo` mutations move to
  `StatementEffects` methods.
- Row MVCC read helpers that only need visibility take `&TrxContext`.
- Row write helpers that install locks or undo records take
  `&TrxContext` plus `&mut StatementEffects`.

Most external callers can continue using `Statement` convenience methods during
the compatibility period. Direct `TableAccess` callers, including catalog
storage insert paths, must either switch to the convenience wrappers or pass
split parameters explicitly. [C4], [C5], [C6], [C12], [U3], [U4]

### Active-Root Access Boundary

Runtime table-root reads should move to proof-gated snapshot APIs. Direct
current-root access will be narrowed into explicitly named unchecked/internal
APIs for file internals, recovery/bootstrap, and tests.

The eventual boundary should look like:

- runtime: `table.root_snapshot(&proof)`
- secondary index runtime: open DiskTree from `snapshot.secondary_index_root`
- GC/cleanup: capture `TableRootSnapshot` and explicitly validate visibility or
  purge horizon predicates
- checkpoint publication: use internal/current-root APIs with documented
  checkpoint-specific preconditions
- recovery/bootstrap: use unchecked current-root APIs with documented
  no-concurrent-swap preconditions
- tests: use unchecked/current-root APIs only when asserting file internals

This RFC uses normal phase numbering. The earlier "Phase 0" concept is renamed
to Phase 1: Root Access Boundary Inventory. It is a separate task because it is
independently reviewable and avoids mixing semantic classification with the much
larger transaction and table-access API churn. [C1], [C7], [C8], [C9], [C10],
[C11], [U6]

### Module Impact Map

`table/access.rs` is the primary API churn point. Its public trait signatures
change, and most internal helpers must replace `Statement` reach-throughs with
explicit `TrxContext` reads and `StatementEffects` mutations. The migration must
cover row visibility, write-lock installation, index undo pushes, redo pushes,
active insert page cache access, cold-row deletion markers, and secondary-index
update helpers. [C5], [C6], [C12], [U4]

Transaction and statement modules provide the enabling split. `ActiveTrx`
retains the public lifecycle while internally exposing `ctx` and `effects`.
`Statement` remains the compatibility handle for normal callers, but internally
splits into `&TrxContext` plus `&mut StatementEffects` before calling table
access. Commit, rollback, prepare, readonly checks, retained old roots, and
fatal rollback cleanup remain transaction-effect responsibilities. [C3], [C4],
[C13], [U3]

Row MVCC helpers should no longer depend on the whole transaction when they only
need identity. Read helpers should accept `&TrxContext`. Write helpers that push
undo or install logical locks should accept `&TrxContext` plus
`&mut StatementEffects`. This keeps read-your-own-write and same-transaction
checks immutable while making undo mutation explicit. [C5], [C6]

Secondary-index runtime paths need a root-source change rather than a
transaction-effect change. Runtime DiskTree opens should stop calling
`published_root()` from the moving current active root and instead take either a
`TableRootSnapshot` or a root block id obtained from one. Internal test helpers
may keep an unchecked current-root path. [C9], [U7]

Checkpoint and table persistence are publication coordinators, not ordinary
runtime readers. `checkpoint_readiness()` currently reads the active root to
compare the root CTS with the GC horizon, while checkpoint publication forks a
mutable root, commits it, updates the block index, and retains the old root in
the checkpoint transaction. These paths should be classified in Phase 1. The
default expectation is that checkpoint keeps an internal/current-root API with
documented publication preconditions, unless Phase 5 proves a
`TableRootSnapshot` helper improves the readiness check without confusing the
publication boundary. [D5], [C7], [U6]

GC and cleanup are strong candidates for `TableRootSnapshot`. Secondary MemIndex
cleanup already builds an owned snapshot of table root CTS, pivot row id,
column-block-index root, deletion cutoff, and secondary roots. That code should
migrate naturally to proof-gated `TableRootSnapshot`, while retaining explicit
cleanup visibility and purge-horizon checks. [D6], [C8], [U7]

Recovery is an explicit exception to runtime proof requirements. Restart
recovery runs in a bootstrap/exclusive context, not under a normal user
transaction that can mint `TrxReadProof`. Active-root reads in table recovery
and transaction log replay should therefore move to named unchecked or recovery
snapshot helpers with documented no-concurrent-swap preconditions. Recovery
should still bind one local root/snapshot per logical decision when it uses
multiple root fields such as `pivot_row_id`, `heap_redo_start_ts`, or
`deletion_cutoff_ts`. [C10], [C17], [U8]

Catalog has three separate impact surfaces:

1. Catalog table storage accessors call `TableAccess` directly for inserts and
   deletes. These should either switch to `Statement` wrapper methods or pass
   `stmt.ctx()` and `stmt.effects_mut()` explicitly during Phase 4. Uncommitted
   catalog scans that take `PoolGuards` and do not use MVCC snapshots should not
   need `TrxContext`. [C14], [U8]
2. Catalog user-table loading reads user table-file active roots to validate
   metadata, initialize block-index roots, and compute loaded-table replay
   floors. This is a bootstrap/load boundary and should use named unchecked or
   load-time snapshot helpers, not `TrxReadProof`. [C15], [U8]
3. Catalog checkpointing uses `MultiTableFile` snapshots and catalog-table root
   descriptors. It is outside the user-table `TableRootSnapshot` contract,
   though transaction context/effects changes may still affect callers that open
   normal transactions around catalog DDL. [C16], [U8]

Tests will need the broadest mechanical cleanup. Runtime behavior tests should
prefer statement wrappers and proof-gated snapshots once available. File-format,
checkpoint, recovery, and low-level table-file tests may keep unchecked
current-root helpers when they are asserting publication internals. [D8], [C1],
[C2], [U6]

## Alternatives Considered

### Alternative A: Value Snapshots Only

- Summary: Replace repeated `active_root()` reads with owned snapshots, but do
  not introduce `TrxContext`, `StatementEffects`, or `TrxReadProof`.
- Analysis: This directly addresses mixed-root field reads and is close to the
  temporary snapshot in MemIndex cleanup. It has lower churn and can improve
  correctness for several paths.
- Why Not Chosen: It does not prove active-root lifetime through transaction
  identity, does not address the borrow conflict created by `Statement` owning
  all of `ActiveTrx`, and does not make `TableAccess` read/write dependencies
  explicit. It would be a pragmatic bug fix rather than the long-term safety
  model requested here.
- References: [C4], [C5], [C8], [U1], [U2], [B1]

### Alternative B: Big-Bang Full Refactor

- Summary: Split transaction context/effects, split statement effects, migrate
  all table access, add `TrxReadProof`, and seal active-root access in one task.
- Analysis: This matches the original backlog most literally and reaches the
  target API without compatibility shims.
- Why Not Chosen: The blast radius crosses transaction lifecycle, statement
  rollback, row MVCC, table access, checkpoint, cleanup, secondary-index opens,
  recovery exceptions, and tests. A single pass would be difficult to review and
  would mix mechanical churn with semantic root-read decisions.
- References: [C3], [C4], [C5], [C6], [C7], [C8], [C9], [C10], [U4], [B1]

### Alternative C: Transaction-Owned Root Guard

- Summary: Let a transaction own retained root guards and hand out borrowed root
  references from those guards instead of producing owned `TableRootSnapshot`
  values.
- Analysis: This can make lifetime safety concrete because root memory would be
  retained by the transaction. It may be useful later for long scans that truly
  need a borrowed root object.
- Why Not Chosen: It increases retention pressure, complicates transaction
  effect ownership, and does not fit async cleanup paths as cleanly as an owned
  field snapshot. The current safety issue is field consistency and root
  lifetime at read boundaries, which `TrxReadProof` plus `TableRootSnapshot`
  handles with less runtime retention.
- References: [D5], [D6], [C1], [C8], [U7], [B1]

### Alternative D: Borrowed `Statement<'trx>` First

- Summary: Change `Statement` to borrow an external transaction context/effects
  object immediately, then build proof-gated APIs on that borrowed shape.
- Analysis: This may be a cleaner final ownership model if the engine later
  wants nested statement handles over one externally owned transaction.
- Why Not Chosen: It is unnecessary for the current proof goal and creates more
  user-facing churn than keeping the current linear `Statement` ownership while
  splitting internal fields. This RFC should first prove the context/effects
  split behind the existing lifecycle.
- References: [C3], [C4], [C5], [U3], [U4]

## Unsafe Considerations

This RFC is expected to reduce reliance on unconstrained active-root borrows but
does not require new unsafe code.

`CowFile::active_root()` currently returns a shared reference loaded from an
atomic raw pointer, and active root memory is reclaimed when the corresponding
old-root owner is dropped. The RFC does not change that reclamation mechanism
directly. Instead, it narrows where runtime code may call into current-root
access and introduces proof-gated owned snapshots for table access.

Any implementation phase that changes `cow_file.rs` raw-pointer handling must:

1. Update or add `// SAFETY:` comments at the raw-pointer load/reclaim boundary.
2. Preserve the old-root retention invariant.
3. Re-run the unsafe inventory or relevant unsafe review checklist if unsafe
   blocks are added, removed, or materially changed.
4. Add tests that exercise checkpoint root swaps while transactions/read
   snapshots remain active.

References:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`
- [C1], [D5], [D6], [D9], [B1]

## Implementation Phases

- **Phase 1: Root Access Boundary Inventory**
  - Scope: Inventory `active_root()` and published-root readers, classify them
    as runtime, checkpoint/GC, recovery/bootstrap, or test/file-internal paths,
    and introduce explicit naming/documentation for checked versus unchecked
    root access boundaries.
  - Goals: Create the migration map for proof-gated runtime access; add
    comments or wrappers that make current-root hazards visible; identify which
    paths should eventually use `TableRootSnapshot`.
  - Non-goals: Do not split transaction or statement types; do not change
    `TableAccess` signatures; do not remove existing active-root access.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`
  - Related Backlogs:
    - `docs/backlogs/000093-transaction-context-effects-split-active-root-proofs.md`

- **Phase 2: Transaction Context And Effects Split**
  - Scope: Introduce `TrxContext` and `TrxEffects` inside `ActiveTrx`, move
    immutable identity/runtime accessors to `TrxContext`, and keep compatibility
    methods on `ActiveTrx` during migration.
  - Goals: Make transaction identity borrowable independently from transaction
    effect mutation; preserve commit, rollback, prepare, readonly detection, old
    root retention, and session lifecycle behavior.
  - Non-goals: Do not change `TableAccess` signatures yet except for internal
    compatibility preparation; do not add proof-gated root access yet.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`
  - Related Backlogs:
    - `docs/backlogs/000093-transaction-context-effects-split-active-root-proofs.md`

- **Phase 3: Statement Effects Split**
  - Scope: Introduce `StatementEffects`, move statement-local row undo, index
    undo, redo, and push helpers out of `Statement`, and update `succeed()` /
    `fail()` to merge or roll back `StatementEffects`.
  - Goals: Allow table write code to receive `&TrxContext` and
    `&mut StatementEffects` as disjoint borrows; preserve existing `Statement`
    convenience methods and statement lifecycle.
  - Non-goals: Do not require a borrowed `Statement<'trx>` ownership model; do
    not seal compatibility accessors yet.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`
  - Related Backlogs:
    - `docs/backlogs/000093-transaction-context-effects-split-active-root-proofs.md`

- **Phase 4: TableAccess API Migration**
  - Scope: Change `TableAccess` read methods to accept `&TrxContext` and write
    methods to accept `&TrxContext` plus `&mut StatementEffects`; migrate row
    MVCC helpers, write-lock helpers, index update helpers, and direct
    `TableAccess` callers.
  - Goals: Make immutable versus mutable dependencies explicit throughout table
    access; keep `Statement` wrapper methods as the compatibility layer for most
    callers during the transition.
  - Non-goals: Do not require proof-gated table-root snapshots for every path in
    this phase; do not remove all compatibility wrappers.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`
  - Related Backlogs:
    - `docs/backlogs/000093-transaction-context-effects-split-active-root-proofs.md`

- **Phase 5: TrxReadProof And TableRootSnapshot**
  - Scope: Add `TrxReadProof<'ctx>`, `TableRootSnapshot<'ctx>`, proof-gated
    runtime root snapshot construction, and proof-gated secondary-index root
    opens where runtime reads need table-file root metadata.
  - Goals: Replace runtime current-root reads with owned single-root snapshots;
    ensure snapshots can cross async boundaries and coexist with mutable
    statement effects; make root visibility predicates explicit for cleanup.
  - Non-goals: Do not remove all unchecked active-root APIs in this phase; do
    not introduce transaction-owned root guard retention unless a concrete path
    cannot use owned snapshots.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`
  - Related Backlogs:
    - `docs/backlogs/000093-transaction-context-effects-split-active-root-proofs.md`

- **Phase 6: Seal Old APIs And Validate**
  - Scope: Restrict unconstrained `active_root()` access to file internals,
    recovery/bootstrap, and tests; rename or gate remaining unchecked APIs;
    remove compatibility methods that obscure context/effects ownership.
  - Goals: Make proof-gated runtime access the default; add borrow-shape and
    checkpoint-swap tests; run the standard storage validation pass.
  - Non-goals: Do not implement broader root-reachability GC unless a preceding
    phase exposes it as a required blocker; track such work as follow-up.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`
  - Related Backlogs:
    - `docs/backlogs/000093-transaction-context-effects-split-active-root-proofs.md`

## Consequences

### Positive

- Runtime root reads become tied to transaction read identity instead of
  relying on convention.
- `TableAccess` signatures document which operations need only immutable
  visibility context and which operations mutate statement effects.
- `TableRootSnapshot` prevents one logical operation from mixing fields from
  multiple active roots after checkpoint swaps.
- Owned snapshots fit async table access and cleanup paths better than long
  borrowed `ActiveRoot` lifetimes.
- The phased plan keeps each downstream task reviewable and gives direct
  callers a compatibility period.

### Negative

- The API churn is large, especially in `table/access.rs`, row MVCC helpers,
  tests, and catalog storage paths that call `TableAccess` directly.
- Compatibility wrappers temporarily duplicate access patterns and must be
  removed deliberately.
- `TableRootSnapshot` clones the secondary-index root vector and the metadata
  `Arc`; this should be low cost relative to disk reads, but hot paths should
  avoid repeated snapshot creation inside tight loops.
- Proof-gated snapshots make lifetimes safer but do not by themselves prove GC
  horizon or snapshot-visibility predicates; those checks must remain explicit.

## Open Questions

- Should `TrxReadProof<'ctx>` carry only `PhantomData<&'ctx TrxContext>`, or
  should it hold a private reference to `TrxContext` for stronger compiler
  diagnostics and easier debugging?
- Should `TableRootSnapshot` be table-module owned or file-module owned? The
  proposed fields are table-runtime contracts, but construction reads file-root
  internals.
- Should checkpoint readiness use `TableRootSnapshot`, or should checkpoint
  remain on an internal unchecked active-root path because it is a publication
  coordinator rather than a runtime transaction read?
- Which direct `TableAccess` callers should be migrated to `Statement` wrapper
  methods versus explicit split parameters?
- When compatibility wrappers are removed, should old names be deleted outright
  or kept as test-only helpers behind `#[cfg(test)]`?

## Future Work

- Root-reachability GC for obsolete table-file and DiskTree pages.
- A possible transaction-owned root guard for future long scans that cannot use
  owned field snapshots efficiently.
- Broader typed read proofs for non-root transaction-consistent runtime views if
  future APIs need the same lifetime pattern.
- Follow-up cleanup of legacy `active_root()` naming in file tests once runtime
  paths are fully proof-gated.

## References

- `docs/backlogs/000093-transaction-context-effects-split-active-root-proofs.md`
- `docs/tasks/000124-checkpoint-old-root-retention.md`
- `docs/tasks/000125-checkpoint-root-liveness-gate.md`
- `docs/table-file.md`
- `docs/checkpoint-and-recovery.md`
- `docs/garbage-collect.md`
- `doradb-storage/src/file/cow_file.rs`
- `doradb-storage/src/file/table_file.rs`
- `doradb-storage/src/trx/mod.rs`
- `doradb-storage/src/stmt/mod.rs`
- `doradb-storage/src/table/access.rs`
- `doradb-storage/src/table/gc.rs`
