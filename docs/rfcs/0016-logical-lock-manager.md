---
id: 0016
title: Logical Lock Manager
status: draft
tags: [storage, transaction, ddl, lock]
created: 2026-04-29
---

# RFC-0016: Logical Lock Manager

## Summary

Introduce a general logical lock manager as a separate storage-engine component
for metadata and table-level locks. The first implementation protects table
definition stability with metadata locks and adds table-level `IS`/`IX`/`S`/`X`
locks as the convergence root above existing row-level write ownership. Hot row
undo heads and cold-row deletion-buffer markers remain the fine-grained row
conflict mechanism in this RFC.

## Context

The current storage engine has strong row-level MVCC machinery but no general
logical lock manager. Hot-row writes install a provisional row undo `Lock`, and
cold-row writes claim a `ColumnDeletionBuffer` marker. These mechanisms are also
rollback, recovery, checkpoint, and GC inputs, so replacing them with a new
row-lock subsystem would be a broad MVCC redesign.

The missing layer is above rows. Future DDL such as drop table, create index,
drop index, and schema changes needs a logical way to prevent concurrent use of
unstable table definitions. Explicit table locks also need a table-level
compatibility point so they can detect active row writers without scanning row
undo or deletion-buffer state. This RFC adds that layer without wrapping table
objects in `RwLock<Table>` or changing the current MVCC row conflict protocol.

Issue Labels:
- type:epic
- priority:high
- codex

## Design Inputs

### Documents

- [D1] `docs/architecture.md` - catalog metadata is cache-first at runtime,
  user tables persist independently, and secondary indexes are split between
  hot in-memory state and checkpointed `DiskTree` roots.
- [D2] `docs/transaction-system.md` - hot-row writes install row undo locks;
  cold-row writes use `ColumnDeletionBuffer`; commit backfills CTS through
  shared transaction status.
- [D3] `docs/table-file.md` - table-file roots contain schema metadata,
  block-index roots, secondary-index roots, pivot row id, and replay cutoffs.
- [D4] `docs/checkpoint-and-recovery.md` - checkpoint and recovery are
  table-centric; foreground commit updates memory and redo, not persistent
  `DiskTree` pages.
- [D5] `docs/garbage-collect.md` - row undo, deletion buffer, MemIndex cleanup,
  and table-file roots each have separate ownership and proof boundaries.
- [D6] `docs/rfcs/0015-transaction-context-effects-root-proofs.md` - runtime
  table-root reads are bound through `TrxContext` and `TrxReadProof`.
- [D7] `docs/process/issue-tracking.md` - large storage changes require
  document-first RFC/task planning.
- [D8] `docs/process/unit-test.md` - validation uses `cargo nextest run -p
  doradb-storage`.
- [D9] `docs/drafts/lock-manager-rfc-review.md` - draft review identifies
  metadata lifetime, table-data invariants, mode validation, queue fairness,
  and DDL protocol details that must be sharpened before implementation.
- [D10] `docs/drafts/lock-manager-rfc-review-2.md` - follow-up review resolves
  v1 policy for lock conversion, acquisition semantics, cleanup guarantees,
  internal debug snapshots, and statement owner assumptions.

### Code References

- [C1] `doradb-storage/src/lib.rs` - no `lock` module exists today.
- [C2] `doradb-storage/src/session.rs` - `create_table` currently performs
  catalog DML, DDL redo, table-file commit, and runtime table insertion without
  a logical metadata or table lock.
- [C3] `doradb-storage/src/catalog/mod.rs` - catalog owns a `DashMap` of loaded
  user-table runtime handles and reloads/removes tables during recovery.
- [C4] `doradb-storage/src/table/mod.rs` - table metadata is stored on the
  runtime table and table-file root snapshots; user-table root access is
  proof-gated for runtime reads.
- [C5] `doradb-storage/src/table/access.rs` - `TableAccess` receives
  `&TrxContext` plus `&mut StmtEffects`, and row writes currently report
  `WriteConflict` from row undo, deletion-buffer, and index conflicts.
- [C6] `doradb-storage/src/trx/row.rs` - hot-row write ownership is installed
  by adding a provisional row undo `Lock`.
- [C7] `doradb-storage/src/table/deletion_buffer.rs` - cold-row delete/update
  ownership is stored in a table-level deletion-buffer marker.
- [C8] `doradb-storage/src/trx/mod.rs` - transaction context, effects,
  prepare/commit, rollback, and ordered commit determine where lock ownership
  must be retained and released.
- [C9] `doradb-storage/src/stmt/mod.rs` - statement success merges statement
  effects into the transaction; statement failure rolls them back.
- [C10] `doradb-storage/src/table/persistence.rs` - checkpoint freezes row
  pages, publishes table-file roots, and retains old roots through transaction
  effects.
- [C11] `doradb-storage/src/trx/recover.rs` - recovery replays DDL as catalog
  modifications plus runtime table reload/removal.

### Conversation References

- [U1] Initial requirement: add a new `lock` module implementing a general lock
  manager and lock mechanism.
- [U2] Initial requirement: metadata locks protect table definition changes and
  are prerequisites for future drop table and index create/drop.
- [U3] Initial requirement: table-level locks include `IX`, `IS`, `X`, and `S`
  and act as a convergence root above individual row access.
- [U4] Initial requirement: row-level locks should stay as-is for now, using
  undo/CDB ownership and returning `WriteConflict`; future refactor can revisit
  row locking.
- [U5] Initial requirement: row-level write paths should invoke table-level
  locking, with transaction-level cache to avoid repeated lock acquisition.
- [U6] Initial requirement: expose an explicit table-lock interface for future
  DDL and user-visible table locks.
- [U7] Initial requirement: lock objects are separate engine objects, not
  wrappers such as `RwLock<Table>`.
- [U8] Initial requirement: plain MVCC reads need metadata locking but no
  table-level data lock.
- [U9] Initial requirement: locks may require different lifetimes, including
  session, transaction, and statement.
- [U10] Follow-up decision: use the recommended metadata plus table leveled-lock
  design and defer row-lock-manager unification.
- [U11] Follow-up clarification: `CatalogNamespace` is only for catalog-wide
  identity/name invariants, not normal table access.
- [U12] Follow-up decision: apply concrete RFC review comments for lifetime,
  ownership, compatibility, and DDL protocol rules while leaving broader API,
  conversion, timeout, and observability questions for further consideration.
- [U13] Follow-up decision: apply the follow-up review recommendation to make
  no-blocking-conversion, try-acquire support, owner-release cleanup, and
  internal debug snapshots normative while leaving exact Rust signatures and
  internal waiter shapes implementation-defined.
- [U14] Follow-up correction: `TrxContext` must remain the immutable
  transaction identity/runtime view; transaction-level lock cache belongs to
  the active transaction's mutable transaction state, with `TrxEffects` or a
  dedicated adjacent lock-state field as the implementation location.
- [U15] Follow-up decision: transaction-owned explicit table locks do not have
  an early unlock API in v1; session table-lock APIs should use
  `Session::lock_table` and `Session::unlock_table` without a `_session`
  suffix.

### Source Backlogs

- None.

## Decision

This RFC chooses a generic logical lock manager for metadata and table-level
resources while preserving the current row-level MVCC ownership model. The
manager is introduced as `doradb-storage/src/lock/` and registered as a separate
engine component or runtime object reachable from sessions and transactions.
It does not wrap `Table`, `Catalog`, or table-file objects in coarse Rust
latches. [U1], [U4], [U7], [C1], [C4], [C5]

### Lock Resources

The first resource set is:

```rust
pub enum LockResource {
    CatalogNamespace,
    TableMetadata(TableID),
    TableData(TableID),
}
```

`CatalogNamespace` protects catalog-wide object identity or name invariants. It
is not a normal DML read lock. In the current table-id-focused catalog, it is
used narrowly for `CREATE TABLE` and future `DROP TABLE` or rename-like
operations that publish or remove table identity. The resource is intentionally
coarse and serializes all table identity publication/removal in the first
implementation. Future name-based catalog lookup should replace broad namespace
use with a finer resource such as `CatalogName(database_id, object_name)` for
name uniqueness and rename ordering. Existing table reads and row writes do not
acquire `CatalogNamespace` after table resolution. [U11], [U12], [D9], [C2],
[C3]

`TableMetadata(table_id)` protects the table definition contract for one table:
column layout, index definitions, table-file metadata shape, and runtime
metadata used by table access. Plain MVCC reads and row writes acquire shared
metadata access before reading schema-dependent state. DDL that changes table
definition or table existence acquires exclusive metadata access. [U2], [U8],
[D3], [C4], [C5]

`TableData(table_id)` is the multi-granularity table lock root above row
ownership. Row writes acquire `IX` here before using hot-row undo or cold-row
CDB ownership. Explicit table locks and conservative DDL can acquire `S` or
`X` and get one fast compatibility point against active row writers. [U3],
[U4], [U5], [D2], [C6], [C7]

### Lock Modes And Compatibility

The lock manager supports four modes for table data resources:

```text
      IS  IX  S   X
IS    yes yes yes no
IX    yes yes no  no
S     yes no  yes no
X     no  no  no  no
```

`TableMetadata` initially uses only `S` and `X`; `S` is compatible with `S`,
and `X` conflicts with every mode. `CatalogNamespace` also accepts only `S` and
`X`. `TableData` accepts `IS`, `IX`, `S`, and `X`. Unsupported
mode/resource combinations, such as `TableMetadata(IX)` or
`CatalogNamespace(IS)`, must return an explicit invalid-lock-mode error and
must not be silently normalized. [U2], [U3], [U12], [D9], [C5]

The first implementation does not add a `SIX` mode. Any operation that needs
"shared whole-table access plus row-exclusive updates" must acquire
`TableData(X)` rather than combining `TableData(S)` with row-level ownership.
[U3], [U12], [D9]

### Operation Mapping

Plain MVCC reads on an existing table acquire `TableMetadata(S)` with statement
lifetime and do not acquire `TableData`. This preserves the current MVCC read
model and avoids forcing ordinary snapshot reads through a table-level lock.
Read-only DML inside an explicit transaction follows the same rule by default:
it reacquires statement-lifetime metadata protection per statement rather than
pinning table metadata for the whole transaction. A transaction promotes
metadata protection to transaction lifetime only after it creates uncommitted
table effects or other commit/rollback obligations for that table. [U8],
[U13], [D2], [D6], [D10], [C5]

Row inserts, updates, and deletes acquire `TableMetadata(S)` plus
`TableData(IX)`. Both locks are transaction-lifetime once the statement creates
uncommitted table effects, row undo ownership, CDB ownership, index mutations,
or rollback/commit obligations for the table. The table-data lock is cached per
transaction. After that, hot rows still use row undo ownership and cold rows
still use `ColumnDeletionBuffer` ownership. Existing `WriteConflict` behavior
remains the row-level conflict result. [U4], [U5], [U12], [D2], [D9], [C5],
[C6], [C7]

No code path may install hot-row undo ownership, cold-row CDB ownership, or
table index write effects unless the owner already holds `TableData(IX)` or
`TableData(X)` for that table. Implementation tasks should add debug assertions
near row and index ownership installation to verify that invariant. Once a
transaction acquires `TableData(IX)`, statement rollback does not release it;
the lock remains until the transaction commits or rolls back. [U4], [U5],
[U12], [D2], [D9], [C5], [C6], [C7], [C8], [C9]

Explicit shared table locks acquire `TableMetadata(S)` plus `TableData(S)`.
Explicit exclusive table locks acquire `TableMetadata(S)` plus `TableData(X)`,
unless the command also changes schema, in which case it uses
`TableMetadata(X)`. A session-owned table lock also constrains transactions
started by the same session: for example, a session holding `TableData(S)` must
not write that table until it releases the session lock or explicitly upgrades
to a compatible stronger lock. The first implementation does not treat
same-session transaction owners as automatically compatible with
session-owned locks. [U6], [U9], [U12], [D9]

`CREATE TABLE` acquires `CatalogNamespace(X)` before checking name absence,
allocating a table id, creating table files, inserting catalog rows, inserting
the runtime table handle, and publishing the table identity. It does not need
to acquire `TableMetadata(X)` for the newly allocated table before publication,
because no other session can resolve that table while the namespace lock is
held. After publication, normal per-table metadata and data locks protect
resolved handles. [U2], [U11], [U12], [D9], [C2], [C3]

Future `CREATE INDEX` and `DROP INDEX` should start conservatively with
`TableMetadata(X)` and `TableData(X)` so schema and index roots are stable
while the feature is implemented. Future online-index work may relax
`TableData(X)` after it has a separate correctness design. [U2], [D1], [D3],
[D4]

Future `DROP TABLE` should acquire `CatalogNamespace(X)`, then
`TableMetadata(X)`, then `TableData(X)` before catalog/runtime removal. The
catalog namespace lock protects table identity publication/removal, while the
per-table locks protect already-resolved handles and active table access. These
locks protect logical drop and runtime removal only. Physical file, root,
index, row-page, or other storage reclamation remains governed by existing
snapshot, checkpoint, and GC safety rules and may complete after the logical
drop locks are released. [U2], [U11], [U12], [D3], [D4], [D5], [D9], [C3],
[C10], [C11]

### Acquisition Order

All code paths acquire locks in this order:

```text
CatalogNamespace -> TableMetadata(table_id) -> TableData(table_id) -> row undo/CDB
```

For multiple tables, acquire by sorted `TableID` inside each resource class.
The current RFC does not add deadlock detection, so deterministic acquisition
order is required for built-in paths. Future user-facing arbitrary lock
requests may need timeout/deadlock handling before exposing broad multi-table
locking. The first implementation also does not provide blocking lock
conversion, so built-in operations must acquire the strongest required mode for
each resource before acquiring weaker modes on that same resource. [U3], [U5],
[U13], [D10], [C5], [C6], [C7]

Within one resource, the lock manager uses FIFO-compatible group granting. A
new request that is compatible with current holders must still wait if an older
incompatible waiter is already queued for the same resource. This prevents a
steady stream of compatible readers from starving a waiting
`TableMetadata(X)` or `TableData(X)` request. [U12], [D9]

### Lock Ownership And Lifetime

The lock manager tracks lock owners independently from Rust object lifetimes.
The owner identity should distinguish at least session-owned, transaction-owned,
and statement-owned locks:

```rust
pub enum LockOwner {
    Session(SessionId),
    Transaction(TrxId),
    Statement(TrxId, StatementSeq),
}

pub enum LockLifetime {
    Statement,
    Transaction,
    Session,
}
```

Exact type names may differ during implementation, but the lock manager must be
able to release all locks for a statement, transaction, or session without
walking table objects. The initial statement owner is keyed by `(TrxId,
StatementSeq)` because storage statements execute under an `ActiveTrx`,
including autocommit statements. If future execution paths allow
statement-lifetime metadata reads without an active transaction, statement
owner identity should be extended with `SessionId`. [U7], [U9], [U13], [D10],
[C8], [C9]

Statement-lifetime locks are released when `Statement::succeed()` or
`Statement::fail()` completes. Transaction-lifetime locks are released after
commit finalization or rollback cleanup. Session-lifetime locks are released by
an explicit unlock operation or by session drop. Recovery is single-threaded
before foreground sessions exist and does not acquire logical locks; recovery
continues to use catalog replay and runtime table reload/removal directly.
[U9], [U12], [D9], [C8], [C9], [C11]

Owner release is authoritative. Releasing a statement, transaction, or session
owner must remove all granted locks and pending wait requests owned by that
owner. Cancellation, timeout, statement failure, transaction rollback, session
drop, and fatal cleanup must not leave stale waiters or granted locks behind,
and a waiter must not be woken as granted after its owner has been released.
Statement-owned cleanup must not release transaction-owned locks for the same
transaction. [U9], [U13], [D10], [C8], [C9]

### Transaction-Level Lock Cache

`ActiveTrx` owns transaction-level lock state separately from immutable
`TrxContext`. The implementation may store this state as a narrow
transaction-lifetime substate inside `TrxEffects`, or as a dedicated
`TrxLockState` field adjacent to `TrxEffects`, but table access must not rely
on interior mutable lock state inside `TrxContext`. The state records the
strongest lock already held by the transaction:

```rust
HashMap<LockResource, LockMode>
```

If stored in `TrxEffects`, the lock cache is transaction-lifetime concurrency
state, not rollbackable statement effects, redo, undo, durability input, or a
reason by itself to require ordered commit. It participates only in lock
acquisition, conversion, cache lookup, and transaction cleanup. [U5], [U14],
[D6], [C8], [C9]

This avoids repeated lock-manager calls on row-write paths. If a transaction
already owns `TableData(table_id)` in `IX`, later row writes skip acquisition.
If the same transaction later needs a stronger mode, the lock manager may grant
conversion only when it is immediately compatible with all other granted locks
and with the resource queue. If conversion would need to wait, the request
returns an explicit upgrade error. V1 does not support blocking conversion,
multiple pending converters, or a combined `S` plus `IX` state; operations that
need both shared whole-table access and row-write intent must acquire
`TableData(X)`. The cache must be cleared only after the lock manager has
released the corresponding transaction-owned locks. [U5], [U13], [D10], [C5],
[C8]

The current `TableAccess` API receives `&TrxContext` and `&mut StmtEffects`,
which is not enough to mutate transaction-level lock state directly. Lock
integration tasks must therefore adapt the write-path call shape deliberately:
either statement/table-operation wrappers ensure required transaction locks
before splitting into `&TrxContext` plus `&mut StmtEffects`, or write APIs
receive a narrow mutable transaction-lock handle alongside statement effects.
This preserves the context/effects separation introduced by RFC-0015 while
still avoiding repeated lock acquisition on row-write paths. [U5], [U14], [D6],
[C5], [C8], [C9]

### Explicit Table Lock API

The first API should support internal callers and leave room for SQL/session
surface later:

```rust
impl ActiveTrx {
    pub fn lock_table(&self, table_id: TableID, mode: LockMode) -> Result<()>;
}

impl Session {
    pub fn lock_table(&self, table_id: TableID, mode: LockMode) -> Result<()>;
    pub fn unlock_table(&self, table_id: TableID) -> Result<()>;
}
```

Implementation may use different exact names, but it must expose an explicit
table-lock path instead of forcing callers to rely on row operations to seed
locks. Transaction explicit table locks should participate in the same
transaction cache as row-write `IX` locks. [U6], [U9], [U15], [C8]

V1 intentionally does not expose `ActiveTrx::unlock_table`. Transaction-owned
table locks follow transaction lifetime and are released only by commit,
rollback, or fatal transaction cleanup. Early transaction unlock would require
downgrade and cache-invalidation semantics, and could violate metadata/data-lock
protection for uncommitted row, index, or rollback obligations. Session-owned
locks have an explicit unlock API because their lifetime is not bounded by
transaction cleanup. [U9], [U15], [C8], [C9]

### Row-Level Locking Remains As-Is

This RFC does not add `LockResource::Row` to the active implementation and does
not replace row undo heads or CDB markers with lock-manager entries. Hot-row
write conflicts continue through `RowWriteAccess::lock_undo`; cold-row conflicts
continue through `ColumnDeletionBuffer::put_ref`; index duplicate/write
conflicts remain in `TableAccess`. [U4], [D2], [D5], [C5], [C6], [C7]

The `lock` module may reserve internal type structure that can host a future
row-lock adapter, but no row-lock wait queue, deadlock detector, or row resource
is part of the first implementation. [U4], [D5]

### Error And Wait Policy

The initial lock manager should support blocking wait for compatible release on
metadata/table locks, because DDL and explicit table locks are expected to wait
rather than immediately report row-style `WriteConflict`. It must also support
non-blocking acquisition for deterministic tests, DDL preflight, and immediate
conversion attempts. Exact Rust method names, sync/async split, error enum
names, and waiter struct layout are implementation-defined and should match the
engine execution model. [U6], [U9], [U13], [D8], [D10]

Blocking acquisition is allowed for internal tests and controlled engine paths,
but public or broad user-facing waits must not be exposed until timeout or
cancellation-aware acquisition is available. V1 does not include deadlock
detection; its substitute is deterministic built-in acquisition order, no
blocking lock conversion, and disabled or timeout-bound broad multi-resource
waits. [U6], [U9], [U13], [D8], [D10]

If blocking waits ship, the lock manager must provide an internal debug
snapshot for tests and panic diagnostics. The snapshot should include at least
resource, mode, owner, granted-or-waiting state, and queue order. SQL-visible
lock tables, wait-duration metrics, lock wait tracing, and wait-graph
visualization remain future work. [U13], [D10]

Row-level `WriteConflict` remains unchanged. A transaction that holds
`TableData(IX)` can still fail a specific row update because the row undo head
or CDB marker is owned by another transaction. [U4], [C5], [C6], [C7]

## Alternatives Considered

### Alternative A: Full Hierarchical Locking Including Rows

- Summary: Implement a complete multi-granularity lock manager for catalog,
  table, and row resources, then migrate hot-row and cold-row write ownership
  into that manager.
- Analysis: This is the cleanest textbook shape, but current row ownership is
  also MVCC version state, rollback state, recovery input, and GC proof. Moving
  row ownership would require redesigning hot undo chains, CDB markers,
  rollback, purge, unique-index branches, and checkpoint interactions.
- Why Not Chosen: It conflicts with the requirement to keep row-level locking
  as-is for now and would create a much larger MVCC program than needed for
  metadata and table locks.
- References: [U4], [D2], [D5], [C5], [C6], [C7], [C8]

### Alternative B: Metadata Locks Only

- Summary: Add only `TableMetadata(S/X)` and defer table `IS/IX/S/X`.
- Analysis: This would protect future schema changes but would not provide a
  convergence point for explicit table locks or DDL that needs to detect active
  lower-level row writers.
- Why Not Chosen: The request explicitly requires table-level intention locks,
  and future DDL would otherwise need to inspect row-level ownership state or
  add another table-lock layer later.
- References: [U2], [U3], [U6], [C5]

### Alternative C: Table Object `RwLock`

- Summary: Wrap each runtime table in `RwLock<Table>` or a similar physical
  latch and use read/write guards for metadata and table access.
- Analysis: This is simple but conflates Rust object safety with logical
  database locks. It would make lock lifetime follow guard lifetime, not
  statement/transaction/session lifetime, and it would risk blocking plain MVCC
  reads behind table-data locks.
- Why Not Chosen: The requested design requires lock objects to be separate
  engine objects and requires plain MVCC reads to avoid table-level data locks.
- References: [U7], [U8], [D2], [C4], [C5]

### Alternative D: Global Catalog Lock For All Table Access

- Summary: Use `CatalogNamespace(S/X)` for every table read/write and DDL path.
- Analysis: This is overly conservative. It serializes unrelated table access
  through a catalog-wide resource and turns catalog identity protection into a
  broad DML bottleneck.
- Why Not Chosen: `CatalogNamespace` is needed only for catalog-wide identity or
  name invariants. Existing table access should use per-table metadata and data
  resources after table resolution.
- References: [U11], [D1], [C2], [C3], [C5]

## Unsafe Considerations

No new unsafe code is expected. The lock manager should be implemented with
safe Rust synchronization primitives and existing async notification patterns.
If implementation discovers a need for unsafe code, that task must document the
unsafe boundary and apply the repository unsafe review process before merging.

## Implementation Phases

- **Phase 1: Lock Manager Core**
  - Scope: Add `doradb-storage/src/lock/`, resource/mode/owner/lifetime types,
    compatibility checks, wait queues, release paths, and focused unit tests.
  - Goals: Provide a separate logical lock manager object with
    `CatalogNamespace`, `TableMetadata`, and `TableData` resources; support
    `S`/`X` catalog and metadata semantics; support `IS`/`IX`/`S`/`X`
    table-data semantics; reject invalid mode/resource combinations; and use
    FIFO-compatible group granting without bypassing older incompatible
    waiters. Provide non-blocking acquisition, controlled blocking
    acquisition, immediate-only conversion, authoritative owner cleanup for
    granted and waiting requests, and an internal debug snapshot.
  - Non-goals: Integrating table access paths, implementing row resources,
    adding deadlock detection, or exposing SQL-level lock syntax.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

- **Phase 2: Engine, Transaction, And Session Integration**
  - Scope: Register or attach the lock manager to engine runtime state; add
    lock owner identity; add transaction/session lock release paths; add
    transaction-level table-lock cache on mutable active-transaction state
    rather than `TrxContext`.
  - Goals: Ensure statement, transaction, and session lifetimes can release
    locks and pending wait requests deterministically; make row-write `IX`
    acquisition cheap after first acquisition in a transaction; keep
    statement-owned cleanup separate from transaction-owned locks for the same
    transaction; preserve `TrxContext` as immutable identity/runtime view.
  - Non-goals: Changing row undo/CDB semantics or implementing DDL features
    beyond the lock plumbing needed by later phases.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

- **Phase 3: Metadata And Row-Write Lock Integration**
  - Scope: Acquire statement-lifetime `TableMetadata(S)` for MVCC reads;
    acquire transaction-lifetime `TableMetadata(S)` and cached
    `TableData(IX)` for row writes that create uncommitted table effects;
    integrate `CREATE TABLE` with `CatalogNamespace(X)` where table identity is
    published.
  - Goals: Enforce metadata stability for reads/writes and table-level intent
    compatibility for row writers while preserving existing row conflict
    behavior; assert that row undo, CDB, and index write ownership cannot be
    installed without `TableData(IX)` or `TableData(X)`.
  - Non-goals: Implementing `DROP TABLE`, `CREATE INDEX`, or public SQL table
    lock statements.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

- **Phase 4: Explicit Table Lock Interface And Validation**
  - Scope: Add explicit table-lock APIs for transaction/session callers; test
    compatibility, immediate-only conversion, release, debug snapshots, and
    interaction with row writes and MVCC reads.
  - Goals: Provide the API surface required by future DDL and eventual
    user-visible table locks; validate that MVCC reads take metadata locks but
    no table-data locks; validate that session-owned table locks constrain
    same-session DML until unlocked or upgraded.
  - Non-goals: SQL parser integration, online DDL, deadlock detection, or row
    lock-manager migration.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

## Consequences

### Positive

- DDL has a clear metadata protection primitive before table-definition changes
  are implemented.
- Explicit table locks and conservative DDL get a table-level compatibility
  point against row writers without scanning row undo or CDB state.
- Plain MVCC reads preserve the current high-concurrency read path by avoiding
  table-data locks.
- Row-level MVCC behavior remains stable while the higher-level lock system is
  introduced.
- The new `lock` module becomes a reusable foundation for future row-lock
  waiting, deadlock detection, and richer DDL policy.

### Negative

- The first implementation introduces another transaction/session lifecycle
  obligation: locks must be released on statement end, commit, rollback,
  explicit unlock, session drop, and fatal cleanup.
- Without deadlock detection, built-in paths must obey strict acquisition order
  and broad user-facing multi-table locks may need to remain limited. Callers
  that need stronger lock modes must acquire them up front because v1 does not
  provide blocking conversion.
- Every table access path must be audited so metadata locks are not missed and
  plain MVCC reads do not accidentally acquire table-data locks. Row and index
  write paths must also be audited so no ownership state can bypass
  `TableData(IX)` or `TableData(X)`.
- Blocking lock waits can introduce hangs if cancellation/release paths are
  wrong, so unit tests need timeout-enforced nextest validation and internal
  debug snapshots.

## Open Questions

- What timeout/cancellation API shape should be used before broad user-facing
  session locks or arbitrary multi-resource waits are exposed?

## Future Work

- Blocking lock conversion with deadlock or timeout policy.
- Deadlock detection or timeout policy for arbitrary multi-resource waits.
- Row-level lock-manager integration and wait-based row conflicts.
- Online DDL protocols that relax `TableData(X)` for index creation/drop.
- SQL parser/planner support for `LOCK TABLE` syntax.
- Name-based catalog lookup and rename semantics.
- SQL-visible lock observability tables, metrics, tracing, and wait-graph
  diagnostics.

## References

- `docs/rfcs/0015-transaction-context-effects-root-proofs.md`
- `docs/architecture.md`
- `docs/transaction-system.md`
- `docs/table-file.md`
- `docs/checkpoint-and-recovery.md`
- `docs/garbage-collect.md`
- `docs/drafts/lock-manager-rfc-review.md`
- `docs/drafts/lock-manager-rfc-review-2.md`
