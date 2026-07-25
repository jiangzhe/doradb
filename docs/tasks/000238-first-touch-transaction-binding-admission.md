---
id: 000238
title: First-Touch Transaction Binding and Admission
status: implemented  # proposal | implemented | superseded
created: 2026-07-25
github_issue: 889
---

# Task: First-Touch Transaction Binding and Admission

## Summary

Implement Phase 2 of RFC 0024 by replacing the transaction's weak user-table
cache with positive `TransactionTableBinding` entries. A successful first touch
will bind the transaction's STS-visible metadata to the current live table and
runtime layout, retain transaction-owned `TableMetadata(table_id, S)` until
transaction end, and make later operations use a constant-time cache-hit path.

On a cache miss, acquire statement-owned metadata S before resolving existence,
validate the complete requested table, index, or write operation against
STS-visible and current metadata, and only then acquire transaction-owned
metadata S. Commit the transaction lock record and binding as one
failure-atomic handoff before releasing statement S. A rejected first touch
must leave no binding, new transaction metadata or data lock, row/index
effects, undo, or redo.

Add `OperationError::SchemaChanged` and apply the RFC outcome matrix. Reads may
use the intersection of visible and current schema state. Writes require exact
visible/current `(TableID, effective_cts)` identity before acquiring table-data
IX or X. A successful binding makes same-table DDL wait until transaction end;
an untouched stale writer is rejected without delaying later DDL.

Represent a foreground current secondary-index root with an opaque
`CurrentIndexReadHandle` rather than a standalone `BlockID`. Eager operations
borrow the index runtime from their pinned layout and borrow the handle for
their operation lifetime. Caller-driven streams own an equivalent handle
together with their transaction checkout and cursor state. The compiler and
private APIs must prevent a root address from outliving its operation handle,
without unsafe or self-referential structures.

Simplify Phase 1 metadata history so the authoritative active-STS horizon is
its sole reclamation boundary. A resolved result owns its selected
`Arc<TableMetadata>` and effective CTS without retaining the catalog history
entry. Transaction cleanup drops bindings before removing its STS from horizon
tracking.

This phase does not change CREATE INDEX's row-build semantics or remove its
historical-candidate workaround. Those changes remain Phase 3.

## Context

Parent RFC:

- `docs/rfcs/0024-versioned-metadata-immediate-retirement.md`

RFC Phase:

- Phase 2: First-Touch Transaction Binding And Admission

Issue Labels:

- type:task
- priority:high
- codex

Phase 1, implemented by
`docs/tasks/000237-metadata-only-table-history-publication.md`, established the
required metadata boundary:

1. `Catalog::resolve_user_table_visible(table_id, sts)` selects logical
   metadata using strict `effective_cts < sts`.
2. `Catalog::resolve_user_table_current(table_id)` selects only direct current
   live state or the direct current tombstone.
3. `ResolvedLiveMetadata` exposes stable `(TableID, effective_cts)` identity
   and owns the selected logical metadata independently of catalog history
   membership.
4. `CurrentTableState::Live` exposes mutually consistent current metadata and
   `Arc<Table>`.
5. CREATE TABLE metadata X excludes foreground misses from its catalog
   commit/runtime-publication interval.

The current foreground user-table path does not consume those interfaces.
`TrxInner::table_cache` is a `FastHashMap<TableID, Weak<Table>>` populated after
current runtime lookup, but it is not an authoritative schema binding and is
not used for cache hits. Each table read acquires statement metadata S and
resolves the current table. Each write acquires transaction metadata S before
current lookup, then acquires table-data IX or X. The write ordering can leave
a new transaction metadata lock behind when stale-schema admission should have
failed without a new transaction-lifetime lock.

`Statement` and `StreamStmtState` each own a grouped `OwnerLockState` for
statement locks. `TrxInner` owns a grouped transaction `OwnerLockState`.
Statement and transaction owners are distinct lock owners in the same session
owner group. The lock manager has no owner-transfer operation, but its
same-group coverage rule allows transaction S to be granted while statement S
is held, even when an external metadata X request is already queued.
`LockGrant::{Fresh, Existing}` and `FreshLockGuard` provide the required
fresh-grant distinction and rollback basis.

The approved handoff is therefore a logical promotion implemented by separate
acquisition and release:

```text
statement S remains granted
    -> validate the complete requested operation
    -> acquire transaction S in the same owner group
    -> commit transaction lock record and table binding
    -> release statement S
```

Introducing a lock-owner transfer primitive is intentionally unnecessary. A
special transfer would complicate statement cleanup, explicit preexisting
transaction locks, queued-request fairness, and failure rollback without
strengthening the admission contract.

`TableRuntimeLayout` already owns the current metadata Arc and sparse current
secondary-runtime slots. A binding can therefore retain:

- the self-contained STS-visible `ResolvedLiveMetadata`;
- the bound current effective CTS;
- the current `Arc<Table>`; and
- the current `Arc<TableRuntimeLayout>`.

It must not retain a table root, secondary-index root, root snapshot, pool
guard, or operation proof.

Current foreground secondary-index reads call
`TableAccessor::read_proof_secondary_root`, copy the resulting `BlockID`, and
pass it to `SecondaryIndex::bind_unique`, `bind_non_unique`, or
`OwnedSecondaryIndexCandidateStream::new`. The eager address remains local by
convention, while the lazy owned stream stores it directly. The new type
boundary must make this lifetime relationship explicit.

Design review resolved the following phase-local choices:

- use one centralized, request-aware table admission state machine;
- keep separate statement and transaction grants and expose them as one
  failure-atomic handoff;
- reserve `TransactionTableBinding` for transaction-lifetime state;
- use `CurrentIndexReadHandle` and `OwnedCurrentIndexReadHandle` for
  operation-lifetime executable index state instead of the term "lease";
- use read intersection semantics but exact metadata identity for every write;
- retain session weak `Table` and insert-page hints only as non-authoritative
  physical hints; and
- use safe owning aggregation for lazy streams rather than self-reference,
  pinning, or unsafe code.

The parent RFC's behavior and phase boundaries remain unchanged. During task
resolution, synchronize its Phase 2 task/status/implementation fields and
clarify the conceptual "operation root lease" wording as "current-index read
handle." Phase 3 continues to require completed Phases 1 and 2 and receives no
scope change.

Relevant references:

- `docs/architecture.md`
- `docs/transaction-system.md`
- `docs/index-design.md`
- `docs/secondary-index.md`
- `docs/table-file.md`
- `docs/rfcs/0015-transaction-context-effects-root-proofs.md`
- `docs/rfcs/0016-logical-lock-manager.md`
- `docs/rfcs/0024-versioned-metadata-immediate-retirement.md`
- `docs/tasks/000237-metadata-only-table-history-publication.md`
- `docs/process/coding-guidance.md`
- `docs/process/lint.md`
- `docs/process/unit-test.md`
- `doradb-storage/src/catalog/history.rs`
- `doradb-storage/src/catalog/mod.rs`
- `doradb-storage/src/error.rs`
- `doradb-storage/src/lock/mod.rs`
- `doradb-storage/src/lock/state.rs`
- `doradb-storage/src/trx/mod.rs`
- `doradb-storage/src/trx/stmt.rs`
- `doradb-storage/src/trx/stream_stmt.rs`
- `doradb-storage/src/table/access.rs`
- `doradb-storage/src/table/mod.rs`
- `doradb-storage/src/index/secondary_index.rs`
- `doradb-storage/src/index/owned_stream.rs`

## Goals

1. Replace the transaction's weak table cache with a positive
   `TransactionTableBinding` map keyed by `TableID`.
2. Make a binding-cache hit validate the requested operation and return the
   pinned current table/layout without statement locking, history lookup,
   current lookup, or lifecycle revalidation.
3. Acquire statement metadata S before deciding table or index absence on a
   cache miss.
4. Validate the complete requested table, index, or write operation before
   acquiring transaction metadata S.
5. Commit a fresh transaction metadata-S grant and its binding
   failure-atomically, with rollback of only admission-created state.
6. Release statement metadata S only after the transaction lock record and
   positive binding are both committed.
7. Preserve an explicit or otherwise preexisting transaction lock when
   admission fails.
8. Add `OperationError::SchemaChanged` and implement the RFC's exact
   table/index error matrix.
9. Admit table reads across index-only metadata changes using the current
   table/runtime layout.
10. Admit an index read only when the stable `index_no` is active in both the
    visible metadata and bound current metadata.
11. Permit a write only when visible and current
    `(TableID, effective_cts)` identities are equal.
12. Ensure stale first-touch writes leave no binding, new transaction metadata
    or data lock, or statement effects.
13. Acquire transaction table-data IX for point writes and X for full-table
    mutation only after successful write admission.
14. Make every successful table binding retain metadata S until transaction
    completion so same-table DDL drains exactly table-touching transactions.
15. Keep session weak table and insert-page hints non-authoritative and free of
    metadata-version state.
16. Replace foreground raw secondary-root binding with lifetime-bearing
    borrowed or owned current-index read handles.
17. Retain an eager handle through its final DiskTree access and an owned
    stream handle through exhaustion, error, or drop.
18. Keep binding and lock cleanup paired across commit, rollback, prepare,
    abandoned cleanup, and fatal rollback paths.
19. Preserve current-only resolution for DDL, explicit-lock admission,
    maintenance, purge, checkpoint, and recovery.
20. Leave Phase 3 with the binding, DDL-drain, stale-writer, and index-handle
    guarantees needed to remove CREATE INDEX historical construction safely.
21. Make `min_active_sts` the sole metadata-history GC authority and ensure
    resolved metadata results do not delay reclamation.
22. Drop transaction bindings before removing a rollback STS from active
    horizon tracking.

## Non-Goals

1. Do not switch CREATE INDEX to current committed rows in this phase.
2. Do not remove task-000236 history cutoff, candidate construction, deletion
   masks, tests, or related unique/non-unique index-build workarounds.
3. Do not add write-compatible unequal-version transitions.
4. Do not add physical column evolution, row-layout versioning, schema epochs,
   table-id reuse, or index-number reuse.
5. Do not make a dropped table, dropped index, historical runtime, or
   historical root executable.
6. Do not store roots, root snapshots, operation handles, or pool guards in
   transaction bindings, session caches, or metadata history.
7. Do not add a retained-root registry, root lookup by historical identity,
   index-incarnation wrapper, or allocation-map ownership mechanism.
8. Do not redesign public transaction or statement APIs around a general
   capability type hierarchy.
9. Do not change catalog-table DML admission or route catalog-owned tables
   through user-table metadata history.
10. Do not make an explicit table lock itself create a table binding or
    authorize user-table execution.
11. Do not add lock-manager lifecycle error broadcasts or semantic waiter
    failures.
12. Do not change table-file, catalog-file, redo, row, index, or checkpoint
    persistent formats.
13. Do not change recovery's single current CTS-zero metadata baseline.
14. Do not add a global metadata lock or globally drain unrelated
    transactions.
15. Do not introduce unsafe code, a self-referential stream, or a new pinning
    dependency. If a safe owning representation proves insufficient, stop and
    use the repository unsafe-review process.

## Plan

### 1. Add the positive transaction binding model

Create `doradb-storage/src/trx/admission.rs` and keep its interfaces
crate-private.

Use a transaction-lifetime representation equivalent to:

```rust
struct TransactionTableBinding {
    visible: ResolvedLiveMetadata,
    bound_current_effective_cts: TrxID,
    table: Arc<Table>,
    layout: Arc<TableRuntimeLayout>,
}
```

The exact field visibility may remain private to `trx`, but the ownership
contract is fixed:

- `visible` owns the selected logical metadata and effective CTS without
  retaining catalog history membership;
- `bound_current_effective_cts` is the effective CTS returned by the locked
  current resolver;
- `table` and `layout` are the only executable runtime ownership;
- `layout.metadata()` matches the resolved current metadata; and
- no operation root state is present.

Replace `TrxInner::table_cache: FastHashMap<TableID, Weak<Table>>` with
`table_bindings: FastHashMap<TableID, TransactionTableBinding>`. Do not retain
a second transaction weak cache.

Represent the requested schema contract centrally:

```rust
enum TableAdmissionRequest {
    TableRead,
    IndexRead { index_no: usize },
    TableWrite,
    IndexWrite { index_no: usize },
}
```

Admission returns a short-lived operation view containing cloned current
`Arc<Table>` and `Arc<TableRuntimeLayout>` values and, for index operations,
the admitted stable slot. It must not return a long-lived mutable borrow into
the binding map because statement effects and transaction lock state need
independent mutable access after admission.

Maintain these invariants:

```text
positive binding
    => transaction OwnerLockState caches TableMetadata(table_id, S or stronger)
    => lock manager grants that mode to the transaction owner

transaction metadata lock without binding
    => allowed for explicit locks and pre-admission state
```

An explicit lock never creates a cache hit by itself.

### 2. Implement the cache-hit path

Every foreground user-table operation first looks up `table_bindings` by
`TableID`.

On a hit:

1. Assert that transaction lock state covers
   `TableMetadata(table_id, Shared)`. A debug assertion may additionally query
   the lock manager, but the production fast path uses the owner-local cache.
2. Validate the complete `TableAdmissionRequest` against the stored visible
   metadata and bound current layout metadata.
3. Clone the admitted table/layout into the operation view.
4. Return without acquiring statement metadata S.

The hit path performs no catalog history access, current runtime lookup,
`Table::check_foreground_live`, or session-hint validation. Missing lock state
for a binding is an internal invariant failure, not a cache miss.

Validation order is deterministic:

1. table admission;
2. requested index membership, when any; and
3. exact write identity, when any.

This preserves the RFC error matrix. For example, an index created after the
transaction's STS is `IndexNotFound` to that transaction rather than a generic
write-version mismatch.

### 3. Implement the locked cache-miss path

On a binding miss:

1. Reject only invalid identifier classes, unhealthy-engine state, or an
   already-known owner-group lock conflict before locking. Do not decide
   object existence from an unlocked catalog or weak hint.
2. Acquire statement-owned `TableMetadata(table_id, Shared)`.
3. Resolve STS-visible state with the transaction STS.
4. Resolve direct current state while statement S excludes DDL publication.
5. Apply table, index, and write validation for the complete request.
6. Assert that current metadata, current `Table`, and
   `Table::layout_snapshot()` are mutually consistent.
7. Construct the complete prospective `TransactionTableBinding`.
8. Prepare any required map capacity before requesting transaction S.
9. Acquire transaction-owned metadata S through
   `OwnerLockState::acquire_uncached`.
10. Synchronously commit the owner-local lock record and binding.
11. Release the statement-owned metadata S immediately after that commit.
12. Seed the existing session weak-table hint from the successfully bound
    current table.
13. Return the admitted operation view.

The two catalog resolutions may use separate map guards because statement S
prevents same-table DDL from changing current/history state between them.
Neither resolution may fall back to a weak transaction/session hint or
dropped operational runtime.

Apply these outcomes:

| STS-visible state | Current/bound state | Result |
| --- | --- | --- |
| table absent or tombstoned | any | `TableNotFound` |
| table live | current table tombstoned | `SchemaChanged` |
| table live, requested index absent/inactive | table live | `IndexNotFound` |
| requested index live | current index inactive | `SchemaChanged` |
| requested object live on both sides | same stable object | continue |
| write request after object checks | unequal table effective CTS | `SchemaChanged` |

At the exact DROP CTS, strict visibility still selects the predecessor, so an
untouched transaction receives `SchemaChanged`. A transaction strictly after
DROP resolves the tombstone/inactive slot and receives `TableNotFound` or
`IndexNotFound`.

Do not cache failed lookups or admission results.

### 4. Make the statement-to-transaction handoff failure-atomic

Retain distinct statement and transaction lock owners. Do not add an owner
transfer or lock conversion.

Extend `OwnerLockState` with the narrow internal operations required by
admission:

- prepare capacity for a new cached resource before a guarded grant;
- acquire without updating the local cache, returning `LockGrant`;
- cache a successfully granted resource;
- remove only an admission-added cached resource during rollback; and
- release and remove one exact statement-owned cached resource after handoff.

Keep general lock acquisition and `release_all` behavior unchanged.

Before transaction S acquisition, finish all fallible schema/runtime
validation, construct the binding, and reserve capacity in both the binding
map and transaction owner-lock map. Map allocation failure should surface
through the existing insufficient-memory/resource error boundary before a new
transaction lock is granted.

After `acquire_uncached`:

- `LockGrant::Existing` means the transaction already owns a covering lock.
  Admission may attach a binding but must never release that preexisting lock
  on failure.
- `LockGrant::Fresh` creates an admission rollback guard for the manager grant,
  the admission-added owner-cache record, and any partially inserted binding.

There must be no await, data-lock request, statement effect, recoverable
operation, or caller callback between the transaction-S grant and synchronous
commit. `TrxInner` is exclusively checked out, so no concurrent statement can
observe intermediate local state. Disarm the guard only after both the
owner-lock record and binding satisfy the hit-path invariant.

If cancellation or failure occurs before commit, statement drop releases
statement S. If unwinding occurs during synchronous commit, the admission guard
removes any partial binding/local record and releases only a fresh transaction
grant. It does not disturb a preexisting explicit transaction lock.

Statement S is released as a separate owner operation only after successful
commit. The grouped coverage rule guarantees there is no publication gap and
allows transaction S to be granted ahead of a queued external X that arrived
after statement S.

### 5. Apply read-intersection admission

A table read requires:

```text
visible table is live
AND current table is live
```

It may bind unequal metadata effective CTS values. Column layout is immutable
in RFC 0024's scope, so a table scan uses the bound current table and layout
without requiring whole-metadata pointer identity.

An index read additionally requires:

```text
visible metadata has active index_no
AND bound current layout metadata has active index_no
```

Stable, monotonic, non-reused `index_no` proves logical index identity when the
slot is active on both sides. Assert unchanged index specification as an
internal publication/recovery invariant; do not compare historical and current
runtime pointers.

The binding may therefore pair:

```text
visible metadata indexes: [A]
current metadata indexes: [A, B]
```

A table scan or index A read succeeds. An index B request returns
`IndexNotFound`. If current metadata instead retired A, an A request returns
`SchemaChanged`.

Schema-object admission precedes payload/range/read-set DML validation.
`disable_dml_validation` may bypass caller-input validation but must never
bypass table, index, current-state, or stale-writer admission.

### 6. Apply the exact stale-writer fence

Every user-table write requires:

```text
binding.visible.identity(table_id)
    == (table_id, binding.bound_current_effective_cts)
```

On a cache hit, reject unequal identities before acquiring a new table-data
lock or creating effects. If an earlier read created that binding, retain its
transaction metadata S; the rejected write did not create that lock.

On a cache miss, reject unequal identities before transaction S acquisition.
The rejected transaction remains active but has no new binding, transaction
metadata lock, data lock, row/index ownership, undo, redo, or other statement
effect for that table.

For index-target writes, validate index membership before exact write identity:

- visible missing/inactive index -> `IndexNotFound`;
- current retired index -> `SchemaChanged`;
- stable index but unequal table metadata identity -> `SchemaChanged`; and
- stable index plus equal table identity -> admit.

After successful admission and ordinary payload validation:

- inserts, upserts, point updates, and point deletes acquire transaction
  `TableData(table_id, IntentExclusive)`;
- full-table `table_mutate_mvcc` acquires transaction
  `TableData(table_id, Exclusive)`.

Only then may table access create row/index ownership or statement effects.
Once a write is admitted with equal identity, transaction metadata S prevents
that identity from changing until transaction end.

### 7. Route all foreground user-table statements through admission

Refactor `doradb-storage/src/trx/stmt.rs` as follows:

| Statement method | Admission request | Data mode after admission |
| --- | --- | --- |
| `table_scan_mvcc` | `TableRead` | none |
| `table_lookup_unique_mvcc` | `IndexRead(index_no)` | none |
| `table_index_lookup_mvcc` | `IndexRead(index_no)` | none |
| eager `table_index_scan_mvcc` | `IndexRead(index_no)` | none |
| `table_insert_mvcc` | `TableWrite` | IX |
| `table_mutate_mvcc` | `TableWrite` | X |
| `table_upsert_unique_mvcc` | `IndexWrite(unique_index_no)` | IX |
| `table_update_unique_mvcc` | `IndexWrite(index_no)` | IX |
| `table_delete_unique_mvcc` | `IndexWrite(index_no)` | IX |

Remove the user-table `resolve_user_table` path that selects only
`Catalog::get_table_now`, transaction weak caching, and redundant
`check_foreground_live` calls from these methods. Use the table/layout returned
by admission.

Keep `catalog_insert_mvcc`, `catalog_delete_primary_key_mvcc`, rollback
`TableCache`, recovery, purge, and other catalog/internal operations on their
existing non-user-admission paths.

Refactor `doradb-storage/src/trx/stream_stmt.rs` to use the same
`IndexRead(index_no)` request. A stream constructor must complete binding and
release its temporary statement metadata S before returning the public stream.
The transaction binding, rather than a stream-lifetime statement metadata
lock, excludes DDL after successful admission.

### 8. Add borrowed and owning current-index read handles

Use "handle," not "lease," for operation-scoped executable index state.

The borrowed form is equivalent to:

```rust
struct CurrentIndexReadHandle<'op, 'idx> {
    index: &'idx SecondaryIndex<EvictableBufferPool>,
    guards: &'op PoolGuards,
    root: ProvenIndexRoot<'op>,
}

struct ProvenIndexRoot<'op> {
    block_id: BlockID,
    _proof: PhantomData<&'op TrxContext>,
}
```

The representation remains private. There is no API that extracts an
authorizing standalone `BlockID`.

Construct a borrowed handle only after successful table/index admission and
from the admitted operation's pinned `TableRuntimeLayout` plus an active-root
observation gated by `TrxReadProof`. The layout continues to own the
`Arc<SecondaryIndex<_>>`; the eager handle carries an explicit index borrow
instead of cloning that Arc. Eager unique lookup, non-unique lookup, and range
scan retain the handle until their final DiskTree access. Root-bound
`UniqueSecondaryIndex`, `NonUniqueSecondaryIndex`, and DiskTree/candidate views
borrow the handle or a proven-root reference rather than accepting a
foreground raw root.

`TableRootSnapshot<'ctx>` already carries a transaction-context lifetime for
write operations. Change secondary-root derivation used by foreground index
binding to return a proven root/handle tied to that snapshot instead of a bare
root address. Ordinary row/block identifiers unrelated to secondary-root
authorization remain unchanged.

The lazy form is an owning aggregate equivalent to:

```rust
struct OwnedCurrentIndexReadHandle<'trx> {
    operation: StreamStmtState,
    index: Arc<SecondaryIndex<EvictableBufferPool>>,
    guards: PoolGuards,
    candidates: OwnedSecondaryIndexCandidateStream<'trx, EvictableBufferPool>,
    _transaction: PhantomData<&'trx mut Transaction>,
}
```

Exact private field factoring may place the index and guards inside the owned
candidate stream, but these ownership properties are fixed:

- the aggregate owns the `TrxCheckout` that contains the active
  `TrxContext`;
- its proven root/cursor state carries the public stream's `'trx` lifetime;
- root and cursor state cannot be moved out through a public or crate-level
  accessor;
- `next()` borrows the aggregate for each asynchronous step;
- exhaustion, error, explicit close, or drop destroys root/cursor state before
  checking the transaction core back in; and
- no self-reference, `Pin`, unsafe lifetime extension, or leaked allocation is
  used.

Update `OwnedSecondaryIndexCandidateStream` so its foreground constructor
accepts owned proven-root/handle state rather than `BlockID`.

Change raw `SecondaryIndex::bind_unique` and `bind_non_unique` boundaries so
foreground table access cannot call them with an arbitrary address. Clearly
named unchecked adapters may remain for catalog DDL, recovery, and narrow
tests whose concurrency envelope already supplies current-root validity.
Classify and update every such call site in:

- `doradb-storage/src/catalog/index.rs`;
- `doradb-storage/src/recovery/mod.rs`;
- `doradb-storage/src/table/mod.rs`; and
- index/table unit tests.

Unchecked adapters are crate-private implementation boundaries, not new public
APIs, and do not authorize historical foreground access.

### 9. Preserve session hints and current-only operational paths

Retain `SessionTableCacheEntry` with its `Weak<Table>` and optional active
insert-page ownership. Seed or refresh the weak table only after a binding is
successfully committed. Existing insert-page save/restore invariants may
continue to require that session entry.

Do not store any of the following in session state:

- `ResolvedLiveMetadata`;
- effective CTS or metadata-version identity;
- `TransactionTableBinding`;
- `TableRuntimeLayout`;
- secondary-index read handles; or
- roots.

A miss may consult a weak session table only after statement S and only if it
is verified against the direct current `TableHistoryEntry` state. The direct
current resolver already returns the authoritative `Arc<Table>`, so admission
may simply bypass the weak hint.

Keep DDL, explicit table-lock admission, checkpoint/freeze, maintenance, purge,
catalog checkpoint, dropped-runtime cleanup, and recovery on current-only or
explicit operational resolution. None may synthesize an STS or select a
historical predecessor.

### 10. Pair binding cleanup with transaction lock lifetime

Audit all terminal and failure paths in `trx/mod.rs`.

Bindings must be cleared or dropped before their admission-owned transaction
metadata locks are released:

- ordinary rollback;
- abandoned transaction cleanup;
- prepare handoff;
- successful commit;
- failed precommit rollback;
- explicit rollback;
- fatal statement rollback/discard; and
- fatal transaction cleanup.

Prepared/precommit payloads need transaction locks but cannot execute new
foreground statements, so they do not need to carry executable bindings.
Drop bindings before moving lock state into the prepared payload. Fatal
retention owns only undo/effect memory and must never retain bindings or locks.

Retain the asymmetric invariant that explicit transaction locks may outlive
failed admission without a binding. `release_all` remains the final owner-lock
cleanup authority after bindings are gone.

For direct rollback and abandoned cleanup, clear bindings before removing the
transaction STS from active-horizon tracking. The transaction is terminally
claimed at that point, so no later foreground resolution is possible. Keep
logical locks until rollback effects are complete, then release them after the
horizon handoff.

Add focused test-only inspection for binding presence, owner-local lock
coverage, manager grant ownership, and statement-owner cleanup. Keep
instrumentation narrow and unavailable in production.

### 11. Make metadata-history reclamation horizon-only

Refine the Phase 1 history representation in `catalog/history.rs`:

- store `TableMetadataVersion` values directly rather than wrapper Arcs;
- make `ResolvedLiveMetadata` contain only effective CTS and
  `Arc<TableMetadata>`;
- clone the selected metadata Arc while the catalog map guard is held;
- remove current/historical wrapper classification and strong-count checks;
- drain the complete live-history prefix that is older than the predecessor
  required by `min_active_sts`; and
- remove dropped logical history exactly when
  `drop_cts < min_active_sts`.

Strict equality remains retained because `effective_cts < sts` is the
visibility rule. An already resolved result remains usable through its metadata
Arc after the corresponding catalog version is reclaimed. Operational dropped
runtime/file state remains independent and the outer registry key still
disappears only after both logical and operational slots are absent.

Keep the existing purge coordinator and targeted DDL-triggered metadata-history
observation. Remove only the pin-release retry rationale; no new horizon,
worker, or public API is needed.

### 12. Preserve and synchronize RFC phase boundaries

Do not modify CREATE INDEX build behavior in production code. Its historical
candidate collection remains active throughout this task.

At `$task-resolve`, update RFC 0024:

1. record the final phase status and concise implementation summary;
2. replace or clarify conceptual "operation root lease" terminology with
   "current-index read handle" without changing the lifetime contract; and
3. confirm Phase 3 remains pending and still requires both metadata history
   and active transaction binding/admission.

Phase 3's scope, non-goals, stable-index assumptions, and current-state-only
CREATE INDEX proof remain unchanged.

### 13. Validate the complete change

Use inline unit tests and narrow deterministic hooks. Synchronize concurrency
tests through lock-queue inspection, channels, publication hooks, or explicit
events; do not use sleeps to establish ordering.

Run formatting and lint validation:

```bash
rtk cargo fmt --all -- --check
rtk cargo clippy --workspace --all-targets -- -D warnings
```

Run the authoritative default and alternate-backend suites:

```bash
rtk cargo nextest run --workspace
rtk cargo nextest run -p doradb-storage --no-default-features --features libaio
```

Run focused coverage for changed transaction, lock, table, index, and stream
paths and meet the repository's default 80% review bar or document a justified
definition-heavy exception with covered consumer paths.

## Implementation Notes

Implemented centralized first-touch admission with positive transaction table
bindings, request-aware read/write validation, `SchemaChanged`, and a
failure-atomic statement-S to transaction-S handoff. All foreground
user-table statement and stream paths now consume admitted current
table/layout state. Secondary-index execution uses borrowed or owning
current-index read handles, while explicitly classified catalog, recovery, and
purge paths retain narrow unchecked current-root adapters.

Final review removed the planned `UserTableOperationView`; admission returns
the operation-local table/layout Arc pair directly. It also removed
admission-specific map pre-reservation and the unused owner-cache reserve
helper because comparable guarded transaction lock handoffs use ordinary map
insertion. Admission now preserves its exact typed producer set:
operation-only helpers return `OperationResult`, the orchestrator returns the
new `OperationOrFatalResult`, and only public statement/stream boundaries
disclose the result.

The eager `CurrentIndexReadHandle` borrows its `SecondaryIndex` from the pinned
operation layout, avoiding a redundant Arc clone while making layout ownership
explicit in the type. `OwnedCurrentIndexReadHandle` retains its index Arc
because a caller-driven stream outlives the temporary accessor that creates it.

Foreground writes construct metadata-proven `WriteIndexKeySet` values and keep
`UserTableAccessor` as the sole index-mutation owner. This proves generated
index keys against the accessor's pinned metadata once, without repeating
expensive hot-path assertions during consumption.

Implementation review simplified metadata-history ownership. Superseded
versions are stored directly, `ResolvedLiveMetadata` owns only effective CTS
and `Arc<TableMetadata>`, and the authoritative `min_active_sts` horizon is the
sole reclamation boundary. Live GC drains the complete horizon-obsolete
prefix; dropped logical history disappears after the strict post-DROP horizon
even if an already resolved result remains alive. Direct rollback clears table
bindings before recording active-STS removal and releases logical locks
afterward.

The parent RFC records the horizon-only decision, links Phase 2 to this task
and issue, and is synchronized to the implemented Phase 2 outcome during this
resolve workflow. Completed Task 000237 retains its historical plan with an
explicit supersession amendment.

Validation completed with:

- formatting, debug/release checks, diff checks, workspace Clippy with warnings
  denied, and the deterministic style audit across 22 branch-diff Rust files;
- `rtk cargo nextest run --workspace`: 1,539 tests passed;
- `rtk cargo nextest run -p doradb-storage --no-default-features --features
  libaio`: 1,464 tests passed; and
- 96.13% focused line coverage across catalog history, purge scheduling,
  transaction lifecycle, and admission.

## Impacts

Primary code impacts:

- `doradb-storage/src/error.rs`
  - add `OperationError::SchemaChanged` and the constrained
    `OperationOrFatalResult` admission carrier.
- `doradb-storage/src/trx/admission.rs`
  - add request validation, transaction binding, direct operation-local
    table/layout results, and locked miss/handoff orchestration.
- `doradb-storage/src/trx/mod.rs`
  - replace the weak transaction table cache, expose narrow binding/lock
    helpers, and order terminal cleanup.
- `doradb-storage/src/trx/stmt.rs`
  - route every foreground user-table read and write through admission.
- `doradb-storage/src/trx/stream_stmt.rs`
  - route lazy index scans through admission and own the stream index handle.
- `doradb-storage/src/lock/state.rs`
  - add uncached grant acquisition and exact cache
    commit/rollback/release operations.
- `doradb-storage/src/lock/mod.rs`
  - reuse fresh-grant guards and add only narrow tests or helper visibility;
    lock compatibility and waiter semantics remain unchanged.
- `doradb-storage/src/catalog/history.rs` and
  `doradb-storage/src/catalog/mod.rs`
  - consume the Phase 1 visible/current result types as the admission source
    and simplify history ownership and purge to horizon-only reclamation.
- `doradb-storage/src/trx/purge.rs` and
  `doradb-storage/src/trx/sys.rs`
  - preserve horizon-driven scheduling, remove pin-release semantics, and
    clear rollback bindings before active-STS removal.
- `doradb-storage/src/table/access.rs` and
  `doradb-storage/src/table/mod.rs`
  - consume admitted table/layout state and derive proof-bound secondary roots.
- `doradb-storage/src/index/secondary_index.rs`
  - bind foreground index views from handles/proven roots and classify raw
    internal binding adapters.
- `doradb-storage/src/index/owned_stream.rs`
  - retain owning proof state through lazy cursor completion.
- `doradb-storage/src/catalog/index.rs`,
  `doradb-storage/src/recovery/mod.rs`, and affected unit tests
  - use explicit unchecked/current-snapshot adapters where foreground
    operation handles do not apply.
- `doradb-storage/src/session.rs`
  - preserve weak-hint and insert-page behavior; tests may verify that no
    versioned binding state is added.
- `docs/rfcs/0024-versioned-metadata-immediate-retirement.md`
  - synchronize the horizon-only history amendment and Phase 2 task linkage;
    final phase status remains resolve-time work.

Behavioral impacts:

- The first successful table touch retains transaction metadata S until
  transaction end, including for read-only transactions.
- Same-table DDL waits for every transaction that successfully bound the
  table, even between statements and after a lazy stream closes.
- Unrelated transactions and transactions whose first touch failed do not
  delay that DDL.
- An old transaction may continue table scans or surviving-index reads through
  the visible/current intersection.
- A stale write or retired-index access returns `SchemaChanged` instead of
  executing against current-only state.
- A newly created index absent from the transaction's visible metadata returns
  `IndexNotFound`.
- Resolved metadata results no longer delay horizon-eligible history
  reclamation.

Expected runtime costs:

- one `FastHashMap` lookup and a small fixed set of comparisons/Arc clones on a
  binding hit;
- one statement metadata-S acquisition, visible/current resolution, binding
  allocation, and grouped transaction-S handoff on the first successful
  touch;
- transaction-lifetime retention of one visible metadata result, current
  table, and current layout per touched table;
- no historical wrapper allocation or strong-count scan during metadata GC;
  and
- no repeated metadata lock or catalog lookup on subsequent operations.

Risks and mitigations:

- Lock/binding divergence during cancellation or unwinding is controlled by
  completing fallible validation before the transaction grant,
  `LockGrant` distinction, and one admission commit guard.
- Error-precedence drift is controlled by one request validator and explicit
  matrix tests shared by hits and misses.
- A lazy proof could otherwise become self-referential; the owning handle
  instead owns checkout, root, and cursor state in one private aggregate.
- Raw-root escape through internal APIs is controlled by proven-root
  foreground signatures and explicitly named unchecked operational adapters.
- Terminal cleanup could release metadata S while bindings remain; every
  terminal transition is audited and tested for bindings-before-locks order.
- Horizon tracking could advance while a rollback binding remains; direct
  rollback clears bindings before recording active-STS removal.
- Read-only transactions now delay same-table DDL after first touch. This is
  the intentional table-granular drain contract of RFC 0024.

There is no public API, persistent-format, redo-shape, table-file,
catalog-file, row-layout, index-layout, checkpoint-format, or recovery-baseline
change.

## Test Cases

1. A first successful table read inserts one positive binding and grants
   transaction metadata S.
2. The temporary statement metadata S is absent immediately after successful
   handoff.
3. A second operation on the same table hits the binding without acquiring
   statement metadata S or calling visible/current resolution.
4. A cache hit asserts transaction metadata-S coverage; deliberately
   inconsistent test state fails as an internal invariant rather than falling
   through to catalog lookup.
5. A transaction metadata S held through a binding blocks queued same-table
   metadata X between statements until commit or rollback.
6. Closing or exhausting a lazy stream releases its operation handle but the
   transaction binding continues to block same-table DDL until transaction end.
7. DDL on another table is not delayed by the binding.
8. A transaction that never touched the table does not delay DDL.
9. A transaction whose first touch failed does not delay DDL after its
   statement S is released.
10. With statement S held and external metadata X queued, grouped transaction
    S is granted and committed without letting X publish between the two
    owners.
11. A fresh transaction-S grant rolls back when admission commit is
    deterministically failed; no binding or owner-local lock record remains.
12. A preexisting explicit transaction S remains granted and cached when
    admission fails.
13. A preexisting explicit transaction S can receive a binding after full
    validation without creating or later releasing a duplicate grant.
14. A statement lock acquisition cancelled or rejected before transaction
    grant creates no binding.
15. An absent visible table returns `TableNotFound` with no binding or new
    transaction lock.
16. A visible tombstone returns `TableNotFound`.
17. A visible live table with current tombstone returns `SchemaChanged`.
18. At `sts == drop_cts`, visible predecessor plus current tombstone returns
    `SchemaChanged`; at `sts > drop_cts`, the tombstone returns
    `TableNotFound`.
19. A requested index absent from visible metadata returns `IndexNotFound`,
    even when it exists in current metadata.
20. A visible active index retired from current metadata returns
    `SchemaChanged`.
21. A stable index active in both visible and current metadata is admitted
    without historical runtime-pointer comparison.
22. A table scan succeeds when visible/current versions differ only by index
    creation or retirement.
23. A surviving index read succeeds across an unrelated index transition.
24. Failed table/index admission inserts no negative cache entry; a later
    operation independently resolves state.
25. `disable_dml_validation` still enforces table/index admission and
    `SchemaChanged`.
26. A write-first miss with unequal visible/current effective CTS returns
    `SchemaChanged` before transaction metadata S, table-data lock, effects,
    undo, or redo.
27. A read-first unequal binding permits compatible later reads but rejects a
    later write before acquiring table-data IX/X.
28. The read-first stale-write rejection retains the binding's original
    metadata S because that lock belongs to the earlier read.
29. A write-first equal identity commits the binding, then acquires table-data
    IX for insert/upsert/update/delete.
30. Full-table mutation acquires table-data X only after equal-identity
    admission.
31. An index-target write reports `IndexNotFound` for a visible-missing index,
    `SchemaChanged` for a current-retired index, and `SchemaChanged` for an
    otherwise stable index under unequal table identity.
32. No stale-write rejection calls a row mutation callback or changes row,
    MemIndex, DiskTree, undo, redo, or statement-effect state.
33. A successfully write-bound table cannot become stale because same-table
    metadata X remains blocked until transaction end.
34. Session weak-table and insert-page hints are refreshed after successful
    binding and remain free of effective CTS, visible metadata, layout, and
    root ownership.
35. A stale session weak hint cannot resurrect a dropped table or retired
    index.
36. An eager unique lookup retains `CurrentIndexReadHandle` through its final
    DiskTree access.
37. Eager non-unique lookup and range scan retain the handle through stream
    exhaustion.
38. Checkpoint root publication during an eager index operation does not
    invalidate the operation's proven root.
39. A lazy stream created before checkpoint root publication continues
    correctly from its owned handle afterward.
40. Lazy stream exhaustion, explicit close, operation error, and drop all
    destroy root/cursor state before checking in the transaction checkout.
41. Foreground table/index access cannot bind a secondary index from a bare
    `BlockID`; no root extraction API is available from borrowed or owned
    handles.
42. Recovery, catalog DDL, and classified internal tests continue to bind
    current roots through explicitly named unchecked/current-snapshot
    adapters.
43. Transaction bindings never retain table or index roots and therefore do
    not prevent checkpoint root advancement.
44. Prepare drops bindings before carrying transaction locks into precommit.
45. Commit, rollback, abandoned cleanup, failed precommit, and fatal cleanup
    drop bindings before releasing transaction locks.
46. Direct rollback drops bindings before removing its STS from active-horizon
    tracking.
47. A resolved historical metadata result remains usable after a strict
    horizon purge removes its catalog history entry.
48. Live history retains exactly the newest predecessor required by
    `min_active_sts` plus later versions, and drains the complete obsolete
    prefix without reference-count checks.
49. A dropped tombstone remains while `min_active_sts <= drop_cts` and becomes
    removable as soon as `min_active_sts > drop_cts`, even while a resolved
    predecessor result remains alive.
50. Fatal rollback retention contains no binding, table runtime, operation
    handle, or logical lock.
51. Existing catalog-table DML, explicit table locks, DDL, purge, checkpoint,
    and recovery tests remain current-only and green.
52. CREATE INDEX continues to run its Phase 3 historical-candidate workaround;
    this task neither removes nor bypasses it.
53. Run `rtk cargo fmt --all -- --check` and
    `rtk cargo clippy --workspace --all-targets -- -D warnings`.
54. Run `rtk cargo nextest run --workspace`.
55. Run
    `rtk cargo nextest run -p doradb-storage --no-default-features --features libaio`.
56. Focused changed-path coverage meets the repository's default 80% review
    bar or documents a justified definition-heavy exception with covered
    consumers.

## Open Questions

None. The cache model, hit/miss ordering, error precedence, separate grouped
lock handoff, fresh-grant rollback, read/write distinction, session-hint
boundary, borrowed/owned handle terminology and ownership, terminal cleanup,
horizon-only history GC, Phase 3 preservation, and resolve-time RFC
synchronization are resolved.
