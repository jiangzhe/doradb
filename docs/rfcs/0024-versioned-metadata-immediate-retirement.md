---
id: 0024
title: Versioned Metadata, Immediate Retirement
status: proposal
tags: [catalog, ddl, mvcc, secondary-index]
created: 2026-07-24
github_issue: 885
---

# RFC-0024: Versioned Metadata, Immediate Retirement

## Summary

Introduce CTS-versioned table metadata so a transaction cannot use a table or
secondary index created at or after its start timestamp (STS). Metadata
versions are logical only: executable `Table`, `TableRuntimeLayout`, secondary
runtime, and root handles remain outside history. Creation visibility follows
the same strict boundary as row MVCC:

```text
object_created_at_cts is visible when transaction.sts > create_cts
```

Visibility is not sufficient for first execution. A transaction first checks
its table-binding cache. A hit is immediately usable because every binding is
paired with transaction-owned `TableMetadata(S)`. On a miss, the statement
acquires `TableMetadata(S)`, resolves STS-visible and current metadata,
validates the requested object, and failure-atomically commits transaction
metadata S together with the current runtime/layout binding. Later statements
reuse that binding without another metadata lock or history lookup.

Table-granular binding makes same-table DDL wait until every transaction that
successfully touched the table ends. A DROP that has already published
therefore affects only cache misses from transactions that had not bound the
table. If their STS-visible metadata still contains the retired object, they
receive `OperationError::SchemaChanged`; if it does not, they receive
`TableNotFound` or `IndexNotFound`. A failed admission attaches no binding and
leaves no newly acquired transaction-lifetime table lock.

Secondary-index roots remain operation resources: an opaque lifetime-bound
proof prevents an eager read or lazy stream from reusing a root address after
its operation lease ends. Non-MVCC operations resolve current state only. A
DROP TABLE history tombstone remains authoritative over weak/runtime hints
until the strict active-STS horizon permits history-slot GC.

Writes additionally require the transaction-visible metadata identity
`(TableID, effective_cts)` to equal the current identity captured by the
binding. This makes current-state-only unique and non-unique `CREATE INDEX`
builds correct and removes both historical candidate construction and
historical dropped-object root retention.

## Context

DoraDB already has most of the required machinery:

- catalog tables are MVCC `MemTable`s;
- DDL commits through ordinary STS/CTS transaction machinery;
- `TableRuntimeLayout` is an immutable, replaceable current-runtime snapshot;
- table roots use copy-on-write publication and horizon-based reclamation;
- logical table locks can serialize DDL against table-bound transactions;
- table and index identifiers are stable and non-reused.

The missing primitive is creation-aware metadata selection paired with a safe
transaction binding. A user table currently exposes one active
`TableRuntimeLayout`, so an old transaction can observe an index created after
its STS. Task 000236 compensates for that behavior for non-unique indexes by
rebuilding historical candidates. Extending the workaround to unique indexes
requires historical uniqueness ownership, and the extra non-unique candidates
require dedicated reclamation.

Full schema-snapshot visibility would solve creation visibility but would also
keep every dropped object executable by old snapshots. For indexes, that means
retaining a complete old table root, not just a DiskTree `BlockID`, because the
root owns allocation reachability. Sequential index drops would additionally
need shared mutable incarnation state so immutable old metadata versions could
find the correct frozen roots. Historical table access would likewise require
exposing the currently terminal dropped-table runtime.

Those costs are unnecessary for DoraDB v1. A secondary index is an optional
access path over table MVCC data, and an explicitly dropped table is a terminal
foreground object. Versioning creation while making retirement immediate keeps
old transactions from using incomplete new indexes without extending dropped
resource lifetimes.

Current foreground lookup resolves weak transaction, session, and catalog
caches before acquiring `TableMetadata`. Those entries do not prove a
transaction metadata lock, so they are not safe executable bindings.
`CREATE TABLE` also commits its catalog transaction and only then installs the
runtime table without a table-specific publication lock. A concurrent lookup
can therefore return a transient `TableNotFound`, observe committed catalog
state without its runtime, or pin a current layout before DDL publication is
protected.

Issue Labels:
- type:epic
- priority:high
- codex

## Goals

1. Make table and index creation invisible to transactions at or before the
   create CTS.
2. Keep metadata versions logical-only and bind executable resources solely
   from the current runtime.
3. Make the first successful table admission transaction-stable by retaining
   table metadata S and the admitted runtime/layout until transaction end.
4. Keep a transaction usable for unaffected objects after `SchemaChanged`.
5. Check a proven transaction binding before statement lock acquisition, but
   acquire metadata S before every authoritative cache-miss history, runtime,
   or absence lookup.
6. Ensure stale-schema rejection creates no statement effects and leaves no
   newly acquired transaction-lifetime table lock.
7. Make the statement-to-transaction metadata-S handoff failure-atomic: success
   publishes the transaction lock record and positive binding together, while
   failure leaves neither newly installed.
8. Bind every captured secondary-index root to the lifetime of its eager
   operation or lazy stream instead of exposing a freely reusable `BlockID`.
9. Resolve metadata for DDL, checkpoint, recovery, and other non-MVCC
   operations from current state only.
10. Keep a post-DROP table-history tombstone authoritative until the metadata
    GC horizon proves that no active transaction can resolve its predecessor.
11. Make unique and non-unique `CREATE INDEX` build only current committed table
   state.
12. Remove create-index historical candidates and their dedicated reclamation
   requirement.
13. Preserve existing coarse DDL locking, table-root publication, checkpoint,
   recovery, and operational cleanup authority.

## Non-Goals

1. Full transaction-lifetime usability of an STS-visible object that the
   transaction did not successfully bind before conflicting DDL.
2. Historical foreground access through dropped tables or indexes.
3. Physical row-layout evolution, including column add/drop/type changes or row
   format conversion.
4. Allowing writes through a stale metadata version.
5. Concurrent metadata changes on the same table or weaker DDL locking.
6. DDL inside a caller-owned user transaction or command-counter visibility.
7. Persisting metadata-version history or adding schema-version ids to row redo.
8. Reusing table ids or stable secondary-index numbers.
9. Automatic table-scan fallback after an index plan is invalidated.
10. Snapshot-consistent name reuse; DoraDB exposes storage tables by stable
    `TableID`, not by a name-resolution API.

## Design Inputs

### Documents

- [D1] `docs/architecture.md` - runtime metadata is cache-first, catalog state
  is durable in `catalog.mtb`, and user tables combine memory and table-file
  state.
- [D2] `docs/transaction-system.md` - transactions have stable STS values,
  commits receive ordered CTS values, statements use logical locks, and GC
  advances from active-snapshot horizons.
- [D3] `docs/index-design.md` and `docs/secondary-index.md` - user indexes use
  stable sparse slots and MemIndex/DiskTree state, with roots captured from one
  proof-gated table-root observation.
- [D4] `docs/table-file.md` - an active root contains the allocation map,
  secondary-root vector, and effective publication timestamp; old CoW roots
  protect displaced physical reachability.
- [D5] `docs/checkpoint-and-recovery.md` and `docs/recovery.md` - recovery
  reconstructs current runtime state from checkpointed roots and ordered redo;
  no pre-crash transaction snapshot survives restart.
- [D6] `docs/garbage-collect.md` - transaction GC and purge horizons govern row,
  index, root, and dropped-resource reclamation.
- [D7] `docs/rfcs/0016-logical-lock-manager.md` - reads, writes, and DDL use
  `TableMetadata` and `TableData` locks with defined owner lifetimes and lock
  order.
- [D8] `docs/rfcs/0017-drop-table-lifecycle-recovery.md` - DROP TABLE crosses a
  terminal foreground lifecycle boundary, while runtime and file cleanup are
  separate and table ids are not reused.
- [D9] `docs/rfcs/0018-create-drop-index.md` - index DDL uses stable non-reused
  `index_no` values, sparse roots, current runtime layouts, and coarse table
  locks.
- [D10] `docs/tasks/000236-non-unique-create-index-mvcc-candidate-complete.md` -
  documents the current historical-candidate workaround and validation surface.
- [D11] `docs/process/unit-test.md` - `cargo-nextest` is authoritative, and
  deterministic concurrency tests use explicit synchronization rather than
  timing assumptions.
- [D12] PostgreSQL documentation on
  [system catalogs](https://www.postgresql.org/docs/current/catalogs.html) and
  [MVCC caveats](https://www.postgresql.org/docs/current/mvcc-caveats.html) -
  internal catalog access does not promise transaction-snapshot schema
  visibility.
- [D13] MySQL documentation on
  [metadata locking](https://dev.mysql.com/doc/refman/8.4/en/metadata-locking.html)
  and
  [transactional dictionary storage](https://dev.mysql.com/doc/refman/8.4/en/data-dictionary-transactional-storage.html)
  - dictionary changes are transactional while metadata locks protect active
  operations.
- [D14] `docs/tasks/000237-metadata-only-table-history-publication.md` - Phase 1
  task design fixes the in-memory history representation, resolution order,
  catalog-map guard boundary, publication order, and GC algorithm.

### Code References

- [C1] `doradb-storage/src/table/layout.rs` - `TableRuntimeLayout` is an
  immutable current-runtime snapshot with sparse `Arc<SecondaryIndex<_>>`
  slots and a replaceable runtime generation.
- [C2] `doradb-storage/src/table/mod.rs` - `Table` owns one current layout and an
  Arc-count-based retired secondary-index list.
- [C3] `doradb-storage/src/trx/stmt.rs` - user-table resolution currently checks
  weak caches before metadata locking; statement-owned locks drop
  automatically, while write metadata/data locks are transaction-owned.
- [C4] `doradb-storage/src/catalog/index.rs` - create/drop index preserve
  unchanged runtime Arcs, publish a new table root, install a new current
  layout, and implement non-unique history collection.
- [C5] `doradb-storage/src/catalog/mod.rs` - the catalog distinguishes live,
  dropped-runtime, and dropped-floor table entries, but only live entries are
  admitted to foreground access.
- [C6] `doradb-storage/src/catalog/storage/tables.rs`,
  `doradb-storage/src/catalog/storage/columns.rs`, and
  `doradb-storage/src/catalog/storage/indexes.rs` - catalog objects are stored
  in MVCC tables, while reconstruction helpers use latest or uncommitted views.
- [C7] `doradb-storage/src/table/access.rs` - read access currently requires
  whole metadata pointer identity and equal root/layout slot counts, then binds
  an index runtime to a `BlockID` copied from the active root.
- [C8] `doradb-storage/src/trx/row.rs` - row visibility follows the strict
  `snapshot_sts > commit_cts` rule and reconstructs older rows from undo.
- [C9] `doradb-storage/src/trx/purge.rs` - swapped table roots and dropped-table
  resources already have operational retention queues and fences.
- [C10] `doradb-storage/src/trx/mod.rs` - transaction context carries stable STS,
  currently caches weak table handles, and retains effects and logical locks
  until commit or rollback.
- [C11] `doradb-storage/src/catalog/table.rs` - table DDL owns catalog commit;
  CREATE currently installs runtime after commit without a per-id metadata
  publication lock, and DROP transitions to terminal foreground state.
- [C12] `doradb-storage/src/index/secondary_index.rs` - a shared index runtime
  binds DiskTree access to a root captured by the caller.
- [C13] `doradb-storage/src/lock/mod.rs` - metadata resources exist independently
  of runtime objects; fresh external requests respect queued incompatible
  waiters, while a request covered by a granted same-owner-group lock may be
  admitted immediately.
- [C14] `doradb-storage/src/session.rs` - session state keeps weak table-runtime
  and insert-page hints, while explicit table locks and maintenance paths
  currently resolve or validate before authoritative grouped lock admission.
- [C15] `doradb-storage/src/trx/stmt.rs` and
  `doradb-storage/src/trx/mod.rs` - statement and transaction owners use the
  same session owner group, enabling a protected statement-to-transaction lock
  handoff.
- [C16] `doradb-storage/src/catalog/table.rs` - columns have no DDL mutation path
  and index create/drop metadata preserves the same `Arc<TableColumnLayout>`;
  table scans are therefore layout-compatible across every metadata version in
  scope.
- [C17] `doradb-storage/src/trx/stream_stmt.rs` - lazy index streams retain a
  transaction checkout plus statement lock state and root-bound index state
  across caller-driven iteration.

### Conversation References

- [U1] Initial request on 2026-07-24: introduce metadata version management so
  old transactions cannot use newly created indexes, retain conservative DDL
  locking, and remove create-index history reconstruction.
- [U2] Round 1 analysis: a unique index cannot be hidden from an old writer while
  allowing that writer to commit an unvalidated duplicate; the initial design
  therefore needs stale-writer rejection.
- [U3] User approved CTS-versioned metadata, conservative stale-writer
  rejection, current coarse DDL locks, and physical column evolution out of
  scope.
- [U4] Round 2 review on 2026-07-25 required lock-before-resolution,
  `CREATE TABLE` metadata-X publication, normal waiter release, and stable
  `TableID` scope without name resolution.
- [U5] Round 3 review on 2026-07-25 required statement-owned stale validation
  before transaction table locks and a test proving rejected writers do not
  block later DDL.
- [U6] Round 4 review identified that a raw dropped-index root `BlockID` is not
  an ownership proof and proposed shared index incarnations with retained full
  table roots.
- [U7] Performance review selected the simpler policy that a dropped index is
  no longer executable by old snapshots; `SchemaChanged` invalidates the plan
  while the same transaction may scan the still-live table.
- [U8] Final review selected one uniform immediate-retirement rule for
  `DROP INDEX` and `DROP TABLE`, chose `SchemaChanged` for old-snapshot access,
  and approved renaming and fully restructuring this draft.
- [U9] Follow-up review selected metadata-only historical versions and
  current-only executable runtimes, with immutable column layout making every
  admitted table scan compatible.
- [U10] Follow-up review selected first-touch transaction binding: check the
  transaction cache before statement metadata locking; on a miss, validate
  under statement S, promote S to transaction ownership, and attach visible
  metadata plus current runtime/layout. Session state retains only
  non-authoritative weak runtime and insert-page hints.
- [U11] Final contract review on 2026-07-25 required failure-atomic
  statement-to-transaction lock handoff, lifetime-bound index-read proofs,
  current-only resolution for non-MVCC operations, and authoritative
  post-DROP tombstones. Tombstone authority is horizon-scoped rather than
  engine-lifetime or durable.
- [U12] Phase 1 task and implementation review on 2026-07-25 approved merging
  the history wrapper and state, using the existing `FastDashMap` guards
  without a nested history lock, storing current live metadata and CTS directly,
  identifying versions by `(TableID, effective_cts)`, and resolving current
  before a reverse-linear scan of superseded versions.
- [U13] Phase 2 implementation review on 2026-07-25 made the authoritative
  active-STS horizon the sole metadata-history reclamation boundary. A resolved
  result owns its selected `Arc<TableMetadata>` but does not pin catalog
  history; transaction cleanup drops bindings before removing its STS from
  horizon tracking.
- [U14] Phase 2 ownership review on 2026-07-25 selected a borrowed eager index
  runtime from the pinned `TableRuntimeLayout`; only caller-driven streams own
  an index Arc independently of their temporary accessor.

### Source Backlogs

- [B1] `docs/backlogs/000164-create-unique-index-full-mvcc-history.md`
- [B2] `docs/backlogs/000165-reclaim-non-unique-create-index-history.md`

## Decision

### Metadata-Only Versions And Current Runtime

`Catalog::user_tables` maps each `TableID` to one `UserTableEntry`. Its history
slot owns both the ordered superseded logical metadata and direct current
foreground state; its sibling dropped slot independently owns RFC-0017 runtime
and file-cleanup authority. Conceptually:

```rust
struct TableMetadataVersion {
    effective_cts: TrxID,
    metadata: Arc<TableMetadata>,
}

struct TableHistoryEntry {
    // Superseded live versions, oldest to newest.
    versions: Vec<TableMetadataVersion>,
    current: CurrentTableState,
}

enum CurrentTableState {
    Live {
        effective_cts: TrxID,
        metadata: Arc<TableMetadata>,
        table: Arc<Table>,
    },
    Dropped {
        effective_cts: TrxID,
    },
}

struct UserTableEntry {
    history: Option<TableHistoryEntry>,
    dropped: Option<DroppedTableOperationalState>,
}
```

`TableMetadataVersion` wraps superseded live metadata only. A tombstone is the
direct current `Dropped { effective_cts }` state, not a historical version
object. Historical versions never own `Table`, `TableRuntimeLayout`,
`SecondaryIndex`, active-root, allocation-map, or `BlockID` handles. A live
current state owns the one foreground-admissible table runtime; `Table` keeps
its existing replaceable current layout. Its metadata Arc is pointer-identical
to the metadata in that layout. [D1], [D4], [D8], [D9], [D14], [C1], [C2],
[C5], [U9], [U12]

A live entry has no dropped operational state. After DROP, the direct current
tombstone and retained runtime or replay/file floor may coexist, and either
slot may be reclaimed first. The outer map entry is removed only after both
slots are absent. Metadata history therefore cannot keep executable resources
alive, and operational cleanup cannot answer foreground metadata lookup.
[D6], [D8], [D14], [C5], [C9], [U11], [U12]

The existing `FastDashMap` guard protects the whole `UserTableEntry` without a
nested history lock. Visible/current resolution holds a shared guard while
comparing, reverse-scanning, and cloning the result. DDL publication, history
GC, and operational-slot changes hold an occupied write entry only for final
in-memory validation, appends, state switches, or prefix drains. No map guard
is held across `.await`, logical lock acquisition, catalog commit/rollback,
root publication, layout installation, runtime destruction, or file deletion.
A historical result clones its selected metadata Arc and copies its effective
CTS before releasing the shared guard. History membership remains governed
only by the active-STS horizon. `TableMetadata(S/X)` remains the asynchronous
operation/publication boundary. [D7], [D14], [C4], [C5], [C11], [C13], [U4],
[U12], [U13]

Each successfully admitted transaction stores a positive table binding:

```rust
struct ResolvedLiveMetadata {
    effective_cts: TrxID,
    metadata: Arc<TableMetadata>,
}

struct TransactionTableBinding {
    visible: ResolvedLiveMetadata,
    bound_current_effective_cts: TrxID,
    table: Arc<Table>,
    layout: Arc<TableRuntimeLayout>,
}
```

The binding map key supplies `TableID`, so the visible and bound-current
version identities are `(table_id, effective_cts)`. A resolved result is
self-contained through its metadata Arc; whether the selected catalog version
remains in history is not part of its identity or lifetime. Transaction
metadata S prevents direct current state from becoming superseded while the
binding exists. Online publication enforces strictly increasing effective CTS
values per table; recovery alone may install the synthetic CTS-zero baseline.
[D2], [D7], [D14], [C10], [C13], [U10], [U12], [U13]

The binding contains no table-root snapshot. Its runtime/layout were current
when admitted and cannot become non-current while the transaction owns
`TableMetadata(table_id, S)`. This transaction pin is executable lease state,
not an STS-resolvable historical runtime. [D7], [D14], [C1], [C10], [C13],
[C15], [U9], [U10], [U12]

### Creation Visibility And First-Touch Admission

A DDL commit advances direct current state to its commit timestamp. When an
existing live current state is superseded, publication wraps that old metadata
and CTS in one historical `TableMetadataVersion` before installing the new
current state. Visible resolution holds the shared catalog-map guard and uses:

```text
resolve_visible(sts):
    if current.effective_cts < sts:
        return current live metadata or current tombstone

    for version in versions.iter().rev():
        if version.effective_cts < sts:
            return effective_cts plus a clone of the historical metadata Arc

    return absent
```

The strict inequality matches row MVCC. An object created at `create_cts` is
absent from transactions whose `sts <= create_cts`. Metadata history is a
cache/materialization of transactional catalog state, not a second durable
schema log. Current state is the common answer, and histories are expected to
be short, so resolution intentionally uses a current-first reverse-linear
scan rather than another index or chain. Current-only resolution checks only
`current` and never falls back from a tombstone. [D1], [D2], [D14], [C5], [C6],
[C8], [U1], [U3], [U9], [U12]

On its first successful access to a table, a transaction binds the intersection
of its STS-visible metadata and current foreground state:

```text
object_admitted =
    object exists in STS-visible metadata
    AND the same stable object is active in current metadata
```

The transaction retains that visible/current pair and the current runtime until
transaction end. Same-table DDL cannot publish while the binding exists.
Consequently, the executable set for a bound table is stable; DDL can invalidate
only an STS-visible object that the transaction had not successfully bound.
[D7], [D8], [D9], [U7], [U8], [U10]

The outcome matrix used on a cache miss, or against the visible/current pair in
an existing binding, is:

| STS-visible state | Bound/current state | Result |
| --- | --- | --- |
| table absent/tombstone | any | `TableNotFound` |
| table live | table tombstone | `SchemaChanged` |
| table live, index absent/inactive | table live | `IndexNotFound` |
| table and index live | current index retired | `SchemaChanged` |
| object live | same stable object current | admit the operation |

Table admission precedes index admission. At the exact DROP CTS, the preceding
version remains STS-visible, so an unbound post-publication operation receives
`SchemaChanged`. A transaction strictly after DROP resolves the tombstone or
inactive slot and receives the corresponding not-found error. Stable
non-reused table/index ids prevent a new object from satisfying an old plan.
[D2], [D8], [D9], [C8], [U7], [U8]

### Transaction Cache Fast Path And Locked Miss

Every transaction operation checks its positive binding cache before acquiring
a statement metadata lock:

```text
lookup transaction binding cache by table_id
    -> hit:
         assert transaction owns TableMetadata(table_id, S)
         validate the requested operation against the binding
         use the pinned current runtime/layout
    -> miss:
         acquire statement-owned TableMetadata(table_id, S)
         locate TableHistoryEntry
         resolve STS-visible metadata and current state
         validate the requested table/index/write operation
         stage a complete TransactionTableBinding
         acquire guarded transaction-owned TableMetadata(table_id, S)
         failure-atomically commit the lock record and binding
         release statement-owned metadata S
         execute through the binding
```

A positive binding and its admission-owned transaction metadata-S record are
one invariant. Every positive binding has a matching granted transaction S,
and every fresh S acquired by table admission is committed only with its
binding. An explicit transaction table lock may exist without a binding, but
it cannot create a cache hit or authorize execution by itself. The hit path
therefore performs no statement metadata-lock acquisition, history lookup,
current-runtime lookup, or lifecycle revalidation. A missing or inconsistent
lock for a cache entry is an internal invariant failure, not a reason to fall
through to resolution. [D7], [C3], [C10], [C13], [C15], [U10], [U11]

On a miss, validation of the requested schema object completes before
transaction S is acquired. Statement and transaction owners share a lock owner
group, so the covered transaction-S request can be granted while an external
DDL X request is queued. Releasing statement S only after transaction S is
granted leaves no publication gap. A `TableNotFound`, `IndexNotFound`, or
`SchemaChanged` result creates no binding and leaves no new transaction table
lock. [D7], [C3], [C13], [C15], [U4], [U5], [U10], [U11]

`TableMetadata` resources do not require an existing runtime, so a miss locks
before deciding absence. Pre-lock checks may reject invalid id classes, an
unhealthy engine, or incompatible locks already owned by the same session, but
may not decide object existence. [D7], [C11], [C13], [C14], [U4]

Session state never caches an STS-visible metadata version or executable
transaction binding. Existing weak `Table` and insert-page hints may remain,
but a miss consults them only after statement S and verifies them against
`TableHistoryEntry.current`. They cannot decide visibility, current admission,
or absence and do not extend runtime lifetime. Failed results are not inserted
into the positive transaction binding cache. [C3], [C5], [C10], [C14], [U10]

DDL releases metadata X normally after publication. Waiters resolve their own
visible/current state rather than receiving one injected lifecycle result:

```text
old STS whose metadata contains object -> SchemaChanged
post-DROP metadata                     -> TableNotFound or IndexNotFound
```

The lock manager therefore has no semantic waiter-failure transition or
broadcast helper, including test-only variants. Its production waiter outcomes
remain normal grant or owner/cancellation release; catalog/runtime resolution
owns lifecycle and not-found errors. This replaces DROP TABLE's former behavior
of failing all queued metadata waiters with one lifecycle/not-found error.
[D7], [D8], [C11], [C13], [U4], [U8]

### Failure-Atomic Binding Handoff

The statement-to-transaction S handoff is one failure-atomic admission
operation, not two independent cache updates. All fallible schema/runtime
validation and construction of the prospective `TransactionTableBinding`
finish while statement S is held and before requesting transaction S. The
transaction acquisition returns admission state that distinguishes a fresh
grant from a transaction lock that existed before this admission attempt.
[D7], [C10], [C13], [C15], [U5], [U10], [U11]

For a fresh grant, an admission guard owns rollback responsibility until a
synchronous commit records the grant in transaction lock state and inserts the
positive binding. The transaction is exclusively checked out during this
commit, so no statement can observe an intermediate cache state. The guard is
disarmed only after both records satisfy the cache-hit invariant. There is no
await, requested data lock, statement effect, or recoverable operation between
the transaction-S grant and this commit. Statement S is released only after
the commit succeeds. [C10], [C13], [C15], [U11]

If acquisition is cancelled or admission fails before commit, rollback removes
any partial admission record and releases only a fresh transaction-S grant.
It does not release a preexisting explicit or transaction lock. The externally
visible failure state therefore contains no positive binding and no newly
acquired transaction-lifetime table lock; dropping the statement releases its
temporary S and a queued DDL X may proceed. Panic and fatal-cleanup paths use
the same owner cleanup/guard ownership rather than leaving an untracked grant.
[D7], [C3], [C10], [C13], [C15], [U5], [U11]

### Current-Only Non-MVCC Resolution

Metadata history exposes two conceptually distinct resolvers:

```text
resolve_visible(sts) -> visible live metadata, tombstone, or absence
resolve_current()    -> direct current live state or current tombstone
```

Only a transactional first-touch admission may call `resolve_visible(sts)`.
DDL, explicit-lock admission, checkpoint/freeze, purge and maintenance, and
recovery have no snapshot-visible schema contract and must resolve current
state only. They may not synthesize an STS, walk backward from a current
tombstone to the newest live version, or use a historical runtime as an
operational fallback. [D2], [D5], [D7], [D8], [C5], [C9], [C11], [C14],
[U11]

“Non-MVCC” or “nontransactional” describes the metadata-resolution contract,
not whether the implementation happens to own a transaction object. In
particular, the internal catalog transaction used by DDL and the user
transaction that requests an explicit lock do not authorize STS-visible
foreground metadata resolution for those operations. [D1], [D2], [C6], [C11],
[C14], [U11]

Current-only resolution preserves each caller's existing concurrency envelope.
Online paths first obtain the metadata lock or operation-specific barrier that
already protects their current-state observation. Startup recovery runs under
exclusive engine admission and reconstructs one current baseline rather than
acquiring foreground logical locks. An absent entry and a current tombstone
both prohibit foreground execution; operation-specific cleanup may treat that
state as an idempotent no-op, but it may not reopen a predecessor. [D5], [D7],
[D8], [C5], [C9], [C11], [C14], [U4], [U11]

### Table Read Binding

On a binding-cache miss, a table read returns:

```text
visible table absent       -> TableNotFound
visible table live,
current table tombstone    -> SchemaChanged
visible and current live   -> bind current Table and TableRuntimeLayout
```

Column layout is immutable in this RFC's scope. Index DDL reuses the same
`Arc<TableColumnLayout>`, and there is no column DDL path. A visible live table
and current live table are therefore unconditionally scan-compatible; no
column-layout pointer comparison or future-layout fallback belongs in the hot
path. Physical column evolution must define a new admission rule before it is
implemented. [C7], [C16], [U9]

A binding may legitimately pair old visible metadata with a newer current
layout:

```text
visible metadata: indexes [A]
bound current:    indexes [A, B]
```

`table_scan_mvcc` uses the bound current `Table`, its current row runtime, and a
proof-gated root observation. It does not require whole `TableMetadata` pointer
identity or equal secondary-root slot counts. The visible metadata still gates
which indexes the transaction may request. [D3], [C4], [C7], [U7], [U9]

After the binding is attached, any same-table CREATE/DROP INDEX or DROP TABLE
waits for transaction end, including between statements and after a lazy stream
closes. `DroppedRuntime` remains an operational purge/recovery resource and is
never a historical foreground route. [D7], [D8], [C5], [C10], [C11], [U8],
[U10]

### Secondary-Index Read Binding

An index operation validates logical membership before opening executable
state:

```text
visible_index = binding.visible.metadata.index(index_no)
current_index = binding.layout.metadata.index(index_no)

visible_index absent -> IndexNotFound
current_index absent -> SchemaChanged
otherwise            -> bind current runtime slot
```

`index_no` is a durable, monotonic, non-reused table-local identity. An
equivalent index created after DROP has a different number. Presence in both
the visible metadata and bound current layout metadata therefore proves one
logical index incarnation; historical metadata does not need a
secondary-runtime Arc, and the foreground path does not compare
historical/current runtime pointers. Publication and recovery may assert that
a shared stable slot has an unchanged specification as an internal invariant.
[D9], [D14], [C1], [C4], [C12], [U9], [U10], [U12], [U14]

The executable index is taken only from the transaction's pinned current
`TableRuntimeLayout`. Root capture is represented by an opaque,
lifetime-bearing current-index read handle rather than a root address:

```rust
struct CurrentIndexReadHandle<'op, 'idx> {
    index: &'idx SecondaryIndex<EvictableBufferPool>,
    guards: &'op PoolGuards,
    root: ProvenIndexRoot<'op>,
}

struct ProvenIndexRoot<'op> {
    block_id: BlockID,
    _proof: PhantomData<&'op TrxReadProof<'op>>,
}
```

The names and private representation are conceptual, but the Rust lifetime is
required. The eager handle borrows its index runtime from the pinned layout
instead of cloning the layout-owned Arc. A constructor may mint the handle
only from a proof-gated active-root observation after table/index admission.
Downstream index bind and execution APIs accept the handle, not a standalone
`BlockID`, and expose no extraction path that authorizes delayed reuse of the
address. The handle cannot be stored in `TransactionTableBinding`,
transaction/session caches, or metadata history. Existing CoW publication
retention protects a root displaced by checkpoint until the operation handle
ends. No historical root handle, index incarnation wrapper, mutex, or
retained-root lookup is introduced. [D3], [D4], [C1], [C7], [C9], [C12], [U6],
[U9], [U10], [U11], [U14]

An eager read retains its borrowed handle through the final DiskTree access. A
lazy stream owns an equivalent handle, including its index Arc, and retains
the root/cursor state until exhaustion or drop; the type system must prevent
that state from outliving the owning handle and its transaction checkout. The
transaction metadata-S binding prevents DDL publication, while the operation
handle separately protects a root that checkpoint may displace. [D4], [D7],
[C17], [U10], [U11], [U14]

`SchemaChanged` does not trigger automatic fallback in the storage layer. An
upper execution layer may discard the plan and, under the same STS, choose
another index present in both sides of the binding or `table_scan_mvcc`.
[C3], [C7], [U7], [U8]

### Stale-Writer Fence

V1 permits a table write only when the visible metadata identity
`(TableID, effective_cts)` exactly equals the current identity captured at
first touch.

On a cache hit:

```text
if binding.visible.effective_cts != binding.bound_current_effective_cts:
    return SchemaChanged
acquire or reuse transaction-owned TableData(table_id, IX or X)
mutate through binding.layout
```

The transaction already owns metadata S, so no statement metadata lock or
current lookup is needed. A binding created by an earlier successful read may
contain unequal versions; its later stale-write rejection retains the
preexisting metadata lock. That lock belongs to the earlier read admission, not
the rejected write, and intentionally keeps later DDL waiting until transaction
end. [D7], [C10], [U5], [U10]

On a cache miss:

```text
acquire statement-owned TableMetadata(table_id, S)
    -> resolve visible metadata and current state
    -> visible table absent: TableNotFound
    -> current table retired: SchemaChanged
    -> visible identity != current identity: SchemaChanged
    -> stage equal-identity TransactionTableBinding
    -> acquire guarded transaction-owned TableMetadata(table_id, S)
    -> failure-atomically commit lock record and binding
    -> acquire transaction-owned TableData(table_id, IX or X)
    -> mutate
```

A stale miss creates no row, index, undo/redo, or other statement effect and
leaves no new transaction metadata or data lock. Statement drop releases the
temporary S lock, so a subsequent same-table DDL can complete without ending
the rejected transaction. Point mutations use data IX; full-table mutations
use data X. [D2], [D7], [C3], [C10], [C13], [C15], [U2], [U5], [U10],
[U11]

Once any successful operation binds the table, DDL cannot publish another
metadata identity until transaction end. A binding admitted with equal
identities therefore cannot become stale. V1 has no write-compatible
unequal-version transition; in particular, `CREATE UNIQUE INDEX` cannot allow
an old writer to commit an unvalidated duplicate. [D7], [D14], [C10], [B1],
[U2], [U3], [U10], [U12]

### CREATE INDEX

`CREATE INDEX` retains RFC-0018's metadata/data X locks, stable sparse index
numbers, checkpoint exclusion, failure handling, and publication order. Its
metadata X request drains every transaction that has successfully bound the
table, including read-only transactions between statements; data X continues
to drain writers and preserve existing lock invariants. Unrelated or untouched
transactions do not delay the DDL. [D7], [D9], [C4], [C10], [U10]

The DDL builds only current committed state:

- unique keys are validated and inserted from current rows;
- non-unique keys are inserted from current rows;
- historical hot/cold candidates are not collected;
- no create-index `history_cutoff` or dedicated historical delete masks are
  published.

This is complete because table-bound transactions drain before the build,
untouched old metadata does not contain the new index, post-publication
transactions bind the current runtime, and an untouched old writer fails exact
version admission before mutation. [D2], [D3], [D9], [D10], [C4], [C10], [B1],
[B2], [U1], [U2], [U10]

At `sts <= create_cts`, index admission returns `IndexNotFound` because visible
metadata lacks the slot. Such an old transaction may still scan the immutable
table layout or use an older surviving index, then pin that visible/current pair
against further DDL. At `sts > create_cts`, the index may be bound from current
runtime state. [D2], [C4], [C16], [U7], [U9], [U10]

### DROP INDEX

`DROP INDEX` publishes a metadata version and table root with the stable slot
inactive. Metadata/data X drains every transaction binding and active
first-touch statement before publication. A transaction that previously bound
the table therefore finishes all its statements and ends before DROP can
publish; no pre-drop transaction binding survives as historical executable
state. [D7], [D9], [C4], [U7], [U10]

After publication, an untouched old transaction may bind the still-live table
for scanning. Its visible metadata may contain the retired index, but the
bound-current metadata does not, so an index request returns `SchemaChanged`
without opening the retired MemIndex or DiskTree. A post-drop visible version
returns `IndexNotFound`. Other indexes present on both sides and table scans
remain available. [C1], [C2], [C7], [U7], [U8], [U10]

DROP does not capture a root `BlockID`, retain an old allocation map, install an
index incarnation/root-source state, or leave runtime Arcs in metadata history.
The removed DiskTree pages follow ordinary table-root reachability
reclamation. In-memory runtime destruction waits only for DDL-local and
operational rollback/purge pins, not old metadata versions or session weak
hints. [D4], [D6], [C2], [C4], [C9], [U6], [U9], [U10]

### CREATE TABLE And DROP TABLE

After allocating the non-reused table id, `CREATE TABLE` immediately acquires
`TableMetadata(new_table_id, X)` with the session DDL owner/group. It holds the
lock through file/runtime preparation, catalog commit, capture of `create_cts`,
metadata-version installation, and current-runtime publication. The catalog
commit helper must return its CTS instead of discarding it. [D2], [D7], [D8],
[C11], [C13], [U4], [U9]

CREATE does not need `TableData(new_table_id, X)`: every cache miss first
acquires metadata S and cannot reach the unpublished runtime while metadata X
is held. The non-reused id cannot already have a transaction binding. Failure
before catalog commit releases X normally; post-commit publication ambiguity
preserves existing poison/recovery behavior. [D7], [D8], [C11], [U4], [U10]

A lookup granted before the create-X request is established may return
`TableNotFound` and linearizes before publication. A later miss waits for
publication, then resolves:

```text
sts <= create_cts -> TableNotFound
sts >  create_cts -> initial metadata and current runtime
```

Fresh external S requests cannot bypass an already queued create X request.
[D2], [C8], [C11], [C13], [U4]

`DROP TABLE` wraps and appends the superseded current live metadata, then
installs direct `CurrentTableState::Dropped { effective_cts: drop_cts }` before
metadata X is released. Metadata X first drains every transaction that has
bound the table, including read-only transactions between statements. After
publication, an untouched old transaction whose visible metadata contains the
table receives `SchemaChanged`; a transaction whose visible state resolves
absence or the tombstone receives `TableNotFound`. No historical
`table_scan_mvcc` route is added. [D7], [D8], [D14], [C5], [C11], [U8], [U10],
[U11], [U12]

The tombstoned `TableHistoryEntry` remains the authoritative in-memory answer
for that `TableID` after the catalog row and foreground runtime are detached.
While the entry is retained, no lookup may fall through to a weak transaction
or session hint, a retained `DroppedRuntime`, or a latest-live catalog helper.
This prevents operational cleanup state from resurrecting the table and keeps
queued post-DROP lookups on the visible/current error matrix. [D8], [C3], [C5],
[C10], [C11], [C14], [U4], [U8], [U11]

The dropped runtime and file remain only for rollback/purge, redo/checkpoint
boundaries, recovery cleanup, and eventual deletion. DROP releases locks
normally so queued misses independently derive `SchemaChanged` or
`TableNotFound` from their STS. Table id non-reuse remains mandatory. [D6],
[D8], [C5], [C9], [C11], [U4], [U8], [U11]

### DDL Publication And Locking

No global metadata lock is added. Existing-table DDL follows:

```text
TableMetadata(table_id, X)
    -> drain transaction bindings and active first-touch statements
    -> TableData(table_id, X)
    -> locate TableHistoryEntry and resolve current state
    -> commit catalog transaction and obtain ddl_cts
    -> publish current table root/file state
    -> install the new current TableRuntimeLayout
    -> append superseded live metadata and install direct current state at ddl_cts
    -> release DDL locks normally
```

The root, layout, and direct current metadata/CTS must be mutually consistent
before metadata X is released. The occupied catalog-map entry is acquired only
for the final validation, append, and current-state switch after root/layout
publication; it is not held across those fallible or blocking steps. The
existing failure-atomic order remains authoritative; a post-commit
catalog/root/runtime/history disagreement preserves poison and recovery
behavior instead of reopening old state. Same-table DDL remains serialized,
different-table DDL remains concurrent, and unrelated active transactions are
never globally drained. [D5], [D7], [D8], [D9], [D14], [C4], [C11], [U3],
[U10], [U12]

CREATE/DROP INDEX and DROP TABLE perform target validation only after
metadata/data X. Maintenance and explicit table locks use their scoped modes
with lock-before-current-only lookup; none of these paths may select an
STS-visible predecessor. CREATE TABLE is the no-existing-entry case and uses
metadata X only. [D7], [C3], [C11], [C13], [C14], [U4], [U11]

### Metadata And Resource Reclamation

Superseded metadata versions are retained only while an active transaction STS
may newly resolve them. A resolved result owns its selected metadata Arc and
effective CTS independently of catalog history membership. Direct current state
needs no version wrapper: transaction metadata S prevents publication while a
current binding exists. This history hides later creations and distinguishes
old-object retirement from ordinary absence. Catalog-row history remains
ordinary catalog-table MVCC GC. [D1], [D2], [D6], [D14], [C5], [C6], [U8],
[U9], [U12], [U13]

Metadata GC joins the existing transaction purge coordinator and uses its
authoritative `min_active_sts`; it adds no horizon or worker. For a live entry,
if current CTS is strictly below the horizon, direct current state is already
the required predecessor and every historical version may be removed.
Otherwise GC retains the newest historical version strictly below the horizon
and every later version. It calculates that position linearly and drains the
complete obsolete prefix. [D2], [D6], [D14], [C9], [U12], [U13]

For a dropped table, history GC must retain the current tombstone and enough
predecessor history to resolve every active STS until:

```text
min_active_sts > drop_cts
```

At `sts == drop_cts`, strict visibility still selects the predecessor, so the
strict horizon condition is required. Once that condition holds, no surviving
or future transaction can distinguish the tombstone from registry absence;
the `UserTableEntry.history` slot may be removed. Its dropped operational slot
remains untouched, and the outer map key survives until that slot also clears.
A later lookup returns `TableNotFound` and still may not fall back to a weak or
dropped-runtime hint. Stable table-id non-reuse makes this horizon-scoped
negative-state eviction unambiguous. The tombstone is not retained for the
engine lifetime and is not made durable. [D2], [D6], [D8], [D14], [C5], [C9],
[C10], [C14], [U11], [U12], [U13]

Metadata retention cannot retain executable resources because version objects
contain no runtime handles. A transaction binding may strongly own a
`Table`/layout, but metadata X cannot publish retirement while that binding
exists. Once DDL obtains X, no transaction pin to the superseded runtime/layout
survives. Session weak hints do not extend lifetime. [D7], [C1], [C2], [C10],
[C14], [U9], [U10]

Metadata and resource obligations are therefore separate:

- retired table runtimes are never re-admitted through metadata history;
- retired secondary runtimes are absent from historical metadata;
- dropped index roots are not retained for old STS readers;
- table and index roots are captured only by current operation proofs;
- dropped table files are not retained for historical foreground reads.

Runtime destruction still waits for DDL-local references, rollback/purge work,
and lifecycle fences. Disk blocks wait for normal CoW publication and
reachability fences. Dropped table files wait for RFC-0017's catalog
checkpoint/recovery deletion boundary. These fences protect admitted work or
recorded cleanup, not future old-STS reads. [D4], [D6], [D8], [D9], [C2], [C5],
[C9], [U7], [U8], [U9]

The completed purge/GC horizon remains the destruction boundary for runtime
state referenced by rollback or purge. Stable non-reused `index_no` values keep
old cleanup records unambiguous; stale index purge entries may remain no-ops.
[D6], [D9], [C2], [C9], [B2]

### Checkpoint And Recovery

Checkpoint publishes only current metadata and current sparse root slots.
Transaction bindings never cache roots, so checkpoint may continue advancing
the active root while a transaction reuses its table/runtime binding.
Lifetime-bound operation proofs and CoW reachability protect displaced roots.
There are no historical index roots or descriptor-specific allocation maps to
serialize or trace. [D3], [D4], [D5], [C7], [C9], [U9], [U10], [U11]

Recovery creates one direct current metadata/runtime baseline with
`effective_cts = 0` and an empty historical vector from recovered catalog and
table-file state. Zero is recovery-only and precedes every valid foreground
STS. Recovery does not rebuild pre-crash metadata history because no pre-crash
STS or transaction binding survives. Replayed DDL reconciles that one current
baseline with existing ordered catalog/root rules rather than synthesizing
intermediate versions. RFC-0017 dropped-table cleanup remains recoverable but
cannot be opened through historical metadata. A catalog-absent dropped table
therefore needs no reconstructed history tombstone after restart: absence is
equivalent to the safely evicted terminal entry, and non-reused ids prevent
rebinding it to a new table. [D5], [D8], [D9], [D14], [C5], [C9], [U8], [U9],
[U11], [U12]

No schema epoch is added to row redo. Row replay continues against current
metadata. [D5], [C8], [C10], [U3]

## Alternatives Considered

### Full Schema-Snapshot Visibility

- Summary: Make every table and index visible and executable exactly when its
  lifetime interval contains the transaction STS, including after DROP.
- Analysis: This provides a transaction-stable executable schema but requires
  historical dropped-table admission and complete retained table roots for
  dropped indexes. A raw DiskTree `BlockID` is not allocation ownership, and
  sequential drops require shared incarnation/root-source state across
  immutable descriptors.
- Why Not Chosen: The extra root, allocation, GC, and lifecycle machinery
  preserves optional access paths and explicitly dropped objects. Immediate
  retirement preserves the CREATE INDEX correctness proof with a smaller,
  uniform contract. [D4], [C5], [C7], [U6], [U7], [U8]

### Historical Dropped-Table Scans But Immediate Index Retirement

- Summary: Revoke dropped indexes but let old snapshots scan a dropped table
  through a retained runtime.
- Analysis: Row results remain available, but tables and indexes receive
  different retirement semantics and the dropped-table foreground lifecycle
  still needs expansion.
- Why Not Chosen: The selected design makes all schema objects follow one
  admission rule and preserves RFC-0017's terminal foreground table lifecycle.
  [D8], [C5], [U7], [U8]

### Runtime-Bearing Metadata Versions

- Summary: Store `Table`, `TableRuntimeLayout`, or secondary runtime Arcs in
  each STS-versioned metadata object and compare historical/current runtime
  identity during admission.
- Analysis: Immediate retirement never executes a runtime solely because an old
  metadata version names it. Runtime-bearing versions therefore extend
  in-memory resource lifetime without adding correctness. They also couple
  metadata GC to runtime destruction and require pointer checks on old-version
  reads.
- Why Not Chosen: Stable non-reused ids provide logical identity, while
  transaction metadata S keeps the one admitted current runtime stable.
  Metadata-only history makes reclamation and the hot path independent. [D6],
  [D7], [D9], [C1], [C2], [U9], [U10]

### Metadata-Identity-Only Transaction Cache

- Summary: Cache only the transaction's visible metadata identity, then resolve
  and bind current runtime state under a statement-owned metadata S lock for
  every operation.
- Analysis: Without transaction metadata S, DDL can publish between statements,
  so every operation repeats lock acquisition, current lookup, and admission.
  Lazy streams also need their own metadata lease, and a cached runtime is only
  a hint that must be revalidated.
- Why Not Chosen: First-touch promotion reuses existing transaction lock
  ownership, gives a constant-time cache-hit path, and makes a successfully
  bound table stable. The accepted v1 tradeoff is that a read-only transaction
  that touched the table delays same-table DDL. [D7], [C3], [C10], [C15],
  [U10]

### Session-Level Metadata-Version Cache

- Summary: Reuse a resolved metadata version across consecutive transactions in
  one session.
- Analysis: Each transaction has its own STS, so a session cache requires
  version-keying and invalidation while providing no transaction-level
  correctness proof. It can also be confused with the existing weak runtime and
  insert-page hints.
- Why Not Chosen: Metadata versions and executable bindings are transaction
  state. Session state retains only non-authoritative physical hints, which are
  validated on the locked miss path. [D2], [C10], [C14], [U10]

### Unprotected Cache Lookup Before Lock

- Summary: Treat a weak transaction/session runtime hint or catalog cache entry
  as authoritative before acquiring metadata protection.
- Analysis: Revalidation cannot repair an authoritative pre-lock miss. During
  CREATE TABLE, lookup can return `TableNotFound` after catalog commit but
  before runtime installation. A stale positive runtime hint also does not
  prove that DDL is excluded.
- Why Not Chosen: A transaction binding cache hit is safe only because its
  transaction metadata S is already held. Every other cache miss or hint must
  linearize existence and admission inside the metadata-lock interval. [C3],
  [C11], [C13], [C14], [U4], [U10]

### Acquire Transaction Metadata Before Schema Admission

- Summary: On a binding-cache miss, acquire transaction metadata S before
  validating whether the requested table/index/write is admissible.
- Analysis: A rejected transaction retains locks until commit/rollback despite
  failing its requested first access, so it can obstruct later DDL.
- Why Not Chosen: Statement metadata S stabilizes miss resolution. Promotion
  occurs only after the requested schema object passes admission, preserving
  the no-new-lock rejection contract. [D7], [C3], [C10], [C13], [C15], [U5],
  [U10]

### Independently Install The Transaction Lock And Binding

- Summary: Acquire transaction metadata S, update transaction lock state, and
  insert the positive binding as separate best-effort steps.
- Analysis: Cancellation or a recoverable failure between the steps can leave
  an untracked granted lock or a binding without its proof. Blind rollback can
  also release a transaction lock that existed before admission.
- Why Not Chosen: A fresh-grant-aware admission guard and one synchronous
  transaction-checkout commit make failure externally all-or-nothing while
  preserving preexisting locks. [D7], [C10], [C13], [C15], [U5], [U11]

### Raw Block ID As An Index-Read Proof

- Summary: Return the current secondary root as a plain `BlockID` and rely on
  callers to keep the transaction or stream alive long enough.
- Analysis: A block id is an address rather than a lifetime or reachability
  proof. Its type permits caching or delayed reuse after the proof-gated root
  observation ends and checkpoint displaces the observed root.
- Why Not Chosen: The opaque lifetime-bearing read proof makes the operation
  lease part of the Rust contract while retaining the existing CoW
  reclamation mechanism. [D3], [D4], [C7], [C12], [U6], [U11]

### Historical Resolution For Non-MVCC Operations

- Summary: Let maintenance, checkpoint, recovery, or DDL synthesize an STS or
  fall back from a tombstone to the newest live history version.
- Analysis: Such operations have no user snapshot whose schema could justify
  that selection. A historical fallback can reopen a retired runtime or make
  operational work disagree with current catalog/root publication.
- Why Not Chosen: Non-MVCC operations consume only current state under their
  existing lock, barrier, or startup-exclusion contract. [D5], [D7], [D8],
  [C5], [C9], [C11], [U11]

### Immediate Tombstone Eviction

- Summary: Remove `TableHistoryEntry` as soon as DROP publishes or detaches the
  foreground runtime.
- Analysis: An active old STS can still resolve the predecessor and must
  distinguish immediate retirement (`SchemaChanged`) from historical absence.
  Removing the entry also invites weak or dropped-runtime fallback during
  operational cleanup.
- Why Not Chosen: The tombstone remains authoritative until the strict
  post-DROP STS horizon proves that all predecessor resolutions are gone.
  [D2], [D6], [D8], [D14], [C5], [C14], [U8], [U11], [U12], [U13]

### Engine-Lifetime Or Durable Tombstones

- Summary: Retain every terminal history entry until restart or reconstruct it
  durably on restart.
- Analysis: After the strict STS horizon passes, no transaction can observe the
  predecessor; registry absence has the same not-found meaning, no pre-crash
  snapshot survives recovery, and table ids are never reused.
- Why Not Chosen: Horizon-scoped authority provides the required overlap
  semantics without unbounded in-memory negative entries or a new durable
  tombstone format. [D5], [D6], [D8], [C5], [U11]

### Complete Create-Index Row-History Reconstruction

- Summary: Finish unique ownership-history reconstruction and add reclamation
  for non-unique historical candidates.
- Analysis: This duplicates snapshot rules inside every new index and adds
  unique owner intervals, synthetic branches, rollback integration, and
  cleanup.
- Why Not Chosen: CTS creation visibility prevents old transactions from using
  the new index and solves the problem once at metadata admission. [D3], [D9],
  [D10], [B1], [B2], [U1]

### Enable Compatibility-Aware Dual Maintenance Immediately

- Summary: Let old writers maintain the union of visible and current indexes.
- Analysis: This may support non-unique CREATE INDEX but changes key
  enumeration, root proofs, undo, rollback, and purge to multi-layout
  operations. It cannot safely bypass a new unique constraint.
- Why Not Chosen: Exact-version writes provide one uniform v1 correctness
  boundary. [C7], [C10], [B1], [U2], [U3]

### Global Pre-DDL Snapshot Drain

- Summary: Delay DDL publication until every older transaction ends.
- Analysis: Transactions do not predeclare all tables they may access. A safe
  barrier would be global or would block/abort an old transaction that first
  reaches the table during the drain.
- Why Not Chosen: An unrelated long-running transaction could indefinitely
  block DDL. The selected table-specific drain waits only for transactions with
  a successful target-table binding; untouched old transactions remain active
  and use versioned first-touch admission later. [D2], [D7], [D13], [U1],
  [U8], [U10]

### Current Catalog Visibility For All Metadata

- Summary: Ignore transaction STS and resolve every metadata operation from
  current catalog state.
- Analysis: This avoids metadata history but lets an old transaction use an
  index whose current-state-only build excludes row versions required by that
  transaction.
- Why Not Chosen: Creation visibility is the requirement that removes
  create-index history reconstruction. [D10], [D12], [B1], [U1]

## Unsafe Considerations

No new unsafe code is expected. The design uses existing `Arc`, logical-lock,
root-proof, lifecycle, and purge ownership patterns. A read-side index proof
must carry a compiler-checked operation lifetime, must not expose an unscoped
root pointer or reusable `BlockID`, and must not outlive its eager operation or
lazy stream; the transaction binding itself stores no root. Any task that
cannot express this ownership with safe lifetimes/guards, or discovers a need
for new unsafe lifetime manipulation, must stop and use the repository unsafe
review process. [C7], [C12], [C17], [U11]

## Test Strategy

Tests use repository-standard `cargo-nextest` workflows and deterministic
synchronization hooks. This RFC does not change timeout or hang-detection
configuration. [D11]

Required coverage includes:

1. Strict create boundaries proving a table/index is absent at
   `sts == create_cts` and visible only at `sts > create_cts`, with resolution
   checking direct current first and then choosing the newest qualifying
   historical version by reverse-linear scan.
2. Strict drop boundaries proving a post-publication old/equal-STS operation
   whose visible metadata contains the object receives `SchemaChanged`, while a
   post-drop version receives not-found.
3. A positive transaction-binding cache hit acquiring no statement metadata
   lock and performing no history, current-runtime, or lifecycle lookup.
4. Binding-cache invariant tests proving every positive entry has a matching
   transaction-owned metadata-S grant, while a preexisting explicit metadata
   lock alone does not create a cache hit.
5. Cache-miss tests proving history, weak session/runtime hints, and absence are
   consulted only after statement metadata S is granted.
6. A create-table hook between catalog commit and history/current-runtime
   installation: old and post-commit STS misses wait behind metadata X, then
   resolve absence and the initial live state respectively.
7. Create-table failure before commit releasing metadata X normally without
   leaving a transaction binding or session-visible metadata version.
8. First-touch statement-to-transaction S handoff while DDL X is queued, with
   the binding and transaction lock committed before statement S drops and no
   protection gap or deadlock.
9. Failpoints and cancellation after a fresh transaction-S grant and at each
   lock-record/binding commit boundary leaving neither a positive binding nor a
   new transaction lock; without ending the transaction, queued DDL completes.
10. The same failed handoff when transaction metadata S preexisted through an
    explicit lock preserving that lock while leaving no partial binding.
11. First-touch `TableNotFound`, `IndexNotFound`, and `SchemaChanged` failures
   creating neither a positive binding nor a transaction metadata lock.
12. One session running transactions on both sides of DDL and never reusing the
    earlier transaction's metadata version, while weak runtime and insert-page
    hints remain safe after authoritative admission.
13. A read-only transaction that successfully touched a table making
    CREATE/DROP INDEX and DROP TABLE wait between statements until transaction
    end.
14. An old transaction that never touched the target table, and an unrelated
    table transaction, not delaying same-table DDL.
15. An untouched old transaction spanning CREATE INDEX binding the current
    table runtime for a scan, using a surviving visible `index_no`, and receiving
    `IndexNotFound` for the new index without runtime-Arc comparison.
16. Table scans across `[A] -> [A, B]` and multiple later index-only versions,
    proving no whole-metadata, slot-count, or column-layout compatibility check
    is required.
17. An untouched old transaction after DROP INDEX successfully binding the
    table for scan, receiving `SchemaChanged` for the dropped index, and using
    another index present in both metadata versions.
18. Drop followed by creation of an equivalent index never validating the old
    non-reused `index_no`.
19. An eager index operation retaining a lifetime-bound proof across DiskTree
    access while checkpoint displaces its root; ordinary reclamation cannot
    reuse the root before the proof ends.
20. A lazy index stream retaining its proof and transaction checkout through
    exhaustion or drop while checkpoint advances the active root; DROP waits
    for the transaction binding until transaction end, not merely stream close.
21. Index execution interfaces accepting only the opaque operation proof, with
    neither `TransactionTableBinding` nor transaction/session caches containing
    a root `BlockID` or proof.
22. DROP TABLE waiting for a touched read-only transaction, then an untouched
    old transaction receiving `SchemaChanged` without historical table access.
23. A post-DROP TABLE transaction receiving `TableNotFound`, while both old and
    new transactions remain usable on unaffected tables.
24. Old- and new-STS cache misses queued behind DROP TABLE receiving normal
    grants and independently deriving `SchemaChanged` and `TableNotFound`,
    without a production or test-only semantic waiter-failure path.
25. Catalog/runtime detachment after DROP leaving the retained direct tombstone
    authoritative despite live weak hints or operational `DroppedRuntime`, with
    old/new STS errors still derived from the history slot.
26. A tombstone remaining while `min_active_sts <= drop_cts`, then the history
    slot becoming removable as soon as `min_active_sts > drop_cts` even while
    an already resolved metadata result remains alive; the independent
    operational slot and outer-key lifetime remain correct.
27. A stale writer on a binding miss receiving `SchemaChanged` with no
    row/index/undo/redo effects and no transaction metadata/data lock.
28. Without ending the rejected transaction from case 27, another same-table
    DDL acquiring X and completing.
29. A transaction first bound by a compatible table read, with unequal visible
    and current versions, later rejecting a write while retaining only its
    preexisting read binding and making DDL wait until transaction end.
30. Equal-version cache-hit writes acquiring or reusing only data IX/X and
    performing no statement metadata-lock or history lookup.
31. Point-mutation IX and full-table X write paths using the same cache-first,
    validate-before-new-lock contract.
32. Unique and non-unique current-state-only builds, including duplicate
    rejection and an untouched stale post-create writer rejection.
33. Dropped-index DiskTree pages obeying the existing active-root publication
    fence, then being reclaimed without any metadata-version or incarnation
    root lease.
34. Resolved historical metadata Arcs and session weak hints retaining no
    secondary runtime or catalog-history membership; direct current live state
    remains layout-consistent, and runtime cleanup waits only for DDL-local,
    rollback/purge, and lifecycle ownership.
35. Dropped-table runtime/file cleanup obeying existing purge, checkpoint, and
    recovery fences independently of horizon-scoped metadata-tombstone
    authority and without historical foreground access.
36. Explicit lock, freeze/checkpoint, DDL target, purge/maintenance, and
    recovery paths resolving current state only even when history also contains
    an STS-selectable live predecessor; online paths resolve after their
    required metadata lock or barrier.
37. DDL rollback/failpoint coverage proving aborted DDL publishes no metadata
    version, the catalog-map guard is not held across logical locks, commit,
    root/layout publication, or cleanup, and post-commit
    catalog/root/runtime/history ambiguity preserves poison semantics.
38. Restart after create/drop table and index exposing one direct current
    metadata/runtime baseline at CTS zero with empty history and requiring
    neither pre-crash history, reconstructed table tombstones, nor create-index
    historical candidates.
39. Workspace and `libaio` backend validation through repository-standard
    commands.

## Implementation Phases

- **Phase 1: Metadata-Only History And Publication**
  - Scope: Store one `TableHistoryEntry` directly under the existing
    `FastDashMap` guard, containing a reverse-scanned vector of superseded live
    `TableMetadataVersion` objects and direct current live metadata/runtime or
    tombstone state. Keep dropped runtime/file cleanup in an independent sibling
    slot. Add separate STS-visible and current-only resolution helpers, make
    retained table tombstones authoritative, and hold CREATE TABLE metadata X
    from id allocation through metadata/current-runtime installation.
  - Goals: Close catalog/runtime publication windows and establish the strict
    STS/CTS metadata boundary without coupling history GC to runtime ownership
    or allowing non-MVCC paths to select historical state.
  - Non-goals: Activating transaction binding, `SchemaChanged`, or removing
    create-index history collection.
  - Phase-local Choices: Internal helper naming/visibility and narrow test
    instrumentation only. Direct current state, `(TableID, effective_cts)`
    identity, the single `FastDashMap` guard boundary, current-first reverse-linear vector
    resolution, metadata/runtime ownership boundary, publication lock,
    current-only non-MVCC resolver, tombstone authority, and strict
    horizon eviction boundary are fixed.
  - Task Doc: `docs/tasks/000237-metadata-only-table-history-publication.md`
  - Task Issue: `#887`
  - Phase Status: done
  - Implementation Summary: Implemented metadata-only table history, current and visible resolution, DDL publication exclusion, production-only waiter release, and strict horizon-aware GC. Phase 2 later removed the original defensive wrapper-pin gate so the active-STS horizon is the sole reclamation authority; Phase 1 validation passed across default and libaio backends. [Task Resolve Sync: docs/tasks/000237-metadata-only-table-history-publication.md @ 2026-07-25] [U13]

- **Phase 2: First-Touch Transaction Binding And Admission**
  - Scope: Replace the weak transaction table cache with positive
    `TransactionTableBinding` entries, check it before statement lock
    acquisition, implement locked cache misses and failure-atomic
    statement-to-transaction S handoff, retain session weak hints without
    version caching, add lifetime-bound current-index read proofs, and add
    `OperationError::SchemaChanged` plus table/index/write admission. Simplify
    historical results to self-contained metadata observations and make the
    active-STS horizon the sole metadata-history GC boundary.
  - Goals: Give bound tables a constant-time transaction cache path, make
    same-table DDL drain table-touching transactions, preserve effect- and
    lock-free schema rejection on first-touch failures, and prevent root
    addresses from escaping their eager-operation/lazy-stream proof.
  - Non-goals: Current-state-only index build cutover or removal of the existing
    history workaround.
  - Prerequisites: Phase 1 must provide metadata-only visible/current
    resolution, `(TableID, effective_cts)` identity, current-runtime lookup, and
    create publication exclusion.
  - Phase-local Choices: Choose helper placement, cache-map mechanics, and a
    safe borrowed or owning representation for the current-index read handle.
    Cache-before-lock ordering, fresh-grant-aware failure rollback,
    admission-lock/binding invariant, table-granular DDL drain, error mapping,
    stable-index membership, and compiler-enforced proof lifetime are fixed.
  - Task Doc: `docs/tasks/000238-first-touch-transaction-binding-admission.md`
  - Task Issue: `#889`
  - Phase Status: done
  - Implementation Summary: Implemented positive first-touch transaction bindings, request-aware visible/current admission with stale-writer fencing, transaction-lifetime metadata-S handoff, lifetime-bound current-index read handles, and horizon-only metadata history reclamation; validated across default and libaio backends. [Task Resolve Sync: docs/tasks/000238-first-touch-transaction-binding-admission.md @ 2026-07-26]

- **Phase 3: Current-State CREATE INDEX And Workaround Removal**
  - Scope: Switch unique and non-unique CREATE INDEX to current committed state,
    remove task-000236 history cutoff/candidate generation and dedicated tests,
    and activate the metadata-admission proof for index DDL.
  - Goals: Leave one creation-visibility and stale-writer explanation for both
    index kinds with no historical ownership reconstruction.
  - Non-goals: Write-compatible transitions or build parallelism.
  - Prerequisites: Phases 1 and 2 must be active together so old readers cannot
    use a new index, table-bound transactions drain before DDL, and untouched
    old writers cannot mutate through stale metadata.
  - Phase-local Choices: Preserve general row-MVCC candidate logic used by
    ordinary lookup; remove only create-index-specific history mechanisms.
  - Task Doc: `docs/tasks/000239-current-state-create-index-workaround-removal.md`
  - Task Issue: `#892`
  - Phase Status: done
  - Implementation Summary: Unified unique and non-unique CREATE INDEX around current committed rows encoded once as canonical BTreeKey values, removed CREATE-INDEX-specific history collection and delete-masked build state, and validated metadata admission, failure cleanup, default and libaio suites, and focused coverage. [Task Resolve Sync: docs/tasks/000239-current-state-create-index-workaround-removal.md @ 2026-07-26]
  - Related Backlogs:
    - `docs/backlogs/closed/000164-create-unique-index-full-mvcc-history.md`
    - `docs/backlogs/closed/000165-reclaim-non-unique-create-index-history.md`

- **Phase 4: Operational Reclamation And Recovery Validation**
  - Scope: Validate ordinary dropped-index root reachability, Arc-based runtime
    cleanup, terminal dropped-table lifecycle cleanup, horizon-scoped
    authoritative tombstone GC, metadata-history GC, transaction-binding
    drainage, current-only operational resolution, restart, and DDL failure
    windows. Remove obsolete runtime-bearing-history documentation or tests.
  - Goals: Prove that metadata versions retain no executable resources and that
    remaining runtime ownership corresponds only to current transaction
    bindings before DDL or operational cleanup fences after DDL.
  - Non-goals: New vacuum mechanisms, historical table/index access, or changes
    to durable table-file formats.
  - Prerequisites: Phase 3 has removed historical candidate state, Phase 2 must
    prove no retired object can be newly admitted, and Phase 1 must expose the
    strict tombstone horizon and current-only resolver to validation hooks.
  - Phase-local Choices: Reuse existing purge/root observability and failpoints;
    add instrumentation only where deterministic reclamation tests require it.
  - Task Doc: `docs/tasks/000240-operational-reclamation-recovery-validation.md`
  - Task Issue: `#895`
  - Phase Status: done
  - Implementation Summary: Validated metadata-only reclamation independence, transaction-binding DDL drainage, current-only operational access, checkpoint-persisted root reclamation, dropped-table runtime and floor cleanup, recovery baselines, and DDL failure windows across default and libaio backends; corrected live ownership documentation and deferred explicit runtime leases to backlog 000166. [Task Resolve Sync: docs/tasks/000240-operational-reclamation-recovery-validation.md @ 2026-07-26]
  - Deferred Follow-ups:
    - `docs/backlogs/000166-replace-arc-probed-dropped-table-runtime-purge.md`
      remains open for explicit executable-runtime lease and owned cleanup-job
      design beyond this validation phase.

## Consequences

### Positive

- Newly created tables and indexes cannot leak into old transactions.
- A positive transaction cache hit needs no statement metadata lock, history
  lookup, current-runtime lookup, or lifecycle revalidation.
- A successfully bound table has a stable executable metadata/runtime pair
  until transaction end.
- Metadata-version GC is independent from executable runtime ownership.
- Unique index creation needs no historical ownership reconstruction.
- Non-unique index creation loses its DDL-specific history and cleanup state.
- DROP has one predictable admission rule for tables and indexes.
- Current index reads gain no incarnation mutex or historical-root lookup.
- Immutable columns make table scans compatible across all in-scope metadata
  versions; stable non-reused index numbers provide per-index identity.
- Failed first-touch admission cannot retain a new same-table lock or partial
  binding and block DDL.
- Index root use is compiler-bound to an eager operation or lazy stream rather
  than protected only by caller convention.
- Non-MVCC work cannot reopen historical live state, and retained table
  tombstones prevent operational cleanup handles from bypassing DROP.
- No historical index roots, allocation maps, or dropped-table foreground
  leases are introduced.
- Existing DDL, checkpoint, recovery, and root-reclamation authority remains.

### Negative

- Any transaction that successfully touches a table, including a read-only
  transaction between statements, blocks all same-table metadata DDL until
  transaction end.
- DDL may invalidate an STS-visible object that an old transaction had not yet
  successfully bound.
- Callers must handle `SchemaChanged`; the storage layer does not automatically
  replan or scan.
- After DROP TABLE, an old transaction loses access to that table even though
  its row STS predates DROP, unless it bound the table and thereby delayed DROP.
- First-touch cache misses perform statement locking, visible/current
  resolution, validation, and transaction-lock handoff before data access.
- The handoff needs fresh-grant-aware rollback and index execution needs
  lifetime-bearing proof plumbing through eager and lazy APIs.
- Transaction bindings strongly pin the current table/runtime layout until
  transaction end.
- A dropped table's metadata history may outlive foreground runtime detachment
  until the strict active-STS horizon permits eviction.
- Metadata history, direct current foreground state, independent operational
  cleanup state, and transaction binding add coordination complexity even
  though historical versions are non-executable.
- V1 rejects potentially compatible old writes across non-unique index DDL.

## Open Questions

No architectural question remains open for v1. Phase tasks may choose internal
helper placement and test instrumentation only within the direct-current,
reverse-linear vector, single-map-guard, publication, ownership, and admission
contracts above.

## Future Work

1. Introduce a more specific public plan-invalidation error if callers need to
   distinguish retired read plans from stale writes.
2. Classify selected metadata transitions as write-compatible and maintain
   visible/current index sets where justified.
3. Design physical column/row-format versioning in a separate RFC.
4. Support DDL inside caller-owned transactions with explicit self-visibility.
5. Add metadata-history, transaction-binding, and retired-runtime observability.
6. Revisit finer-grained per-index binding or statement-only admission if
   table-granular read transactions delay DDL excessively.
7. Revisit full schema-snapshot visibility only if a concrete feature requires
   historical execution of dropped objects.

## References

- `docs/rfcs/0016-logical-lock-manager.md`
- `docs/rfcs/0017-drop-table-lifecycle-recovery.md`
- `docs/rfcs/0018-create-drop-index.md`
- `docs/tasks/000236-non-unique-create-index-mvcc-candidate-complete.md`
- `docs/tasks/000237-metadata-only-table-history-publication.md`
- `docs/backlogs/000164-create-unique-index-full-mvcc-history.md`
- `docs/backlogs/000165-reclaim-non-unique-create-index-history.md`
