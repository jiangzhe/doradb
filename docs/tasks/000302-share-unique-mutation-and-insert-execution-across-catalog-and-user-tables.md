---
id: 000302
title: Share Unique Mutation and Insert Execution Across Catalog and User Tables
status: proposal
created: 2026-09-12
github_issue: 1059
---

# Task: Share Unique Mutation and Insert Execution Across Catalog and User Tables

## Summary

Share foreground unique selection, hot-row mutation, insertion, and hot index
effects across user tables and complete memory/catalog tables. Introduce the
shared executor in `doradb-storage/src/table/mutate.rs`, retain the unique driver
in `unique_mutate.rs`, and migrate catalog point mutations to a private
primary-key callback API using the existing public decision and outcome types.

The purpose is readability and maintenance. User-table cold selection,
decoding, deletion-buffer ownership, cold mutation, persisted-index proof
consumption, and transition policy remain explicit user-table operations.
Shared hot execution can delegate inspection of a cold unique-key owner to
those operations. Catalog batch deletion and delete-then-insert replacement
remain thin compositions inside their existing statement boundaries.

## Context

[Task 000299](000299-unify-unique-key-mvcc-mutation-api.md) introduced the public
`Transaction::table_unique_mutate_mvcc` callback interface while preserving
internal memory/catalog mutation methods.
[Task 000300](000300-fix-stale-unique-read-current-lookups-across-row-replacement.md)
shared current-row classification and hot admission, but the user unique
driver and MemTable update/delete methods still separately orchestrate
selection, forward traversal, prepare waits, retries, and action continuation.
[Task 000301](000301-refactor-memory-table-metadata-and-runtime-layouts.md)
completed the prerequisite ownership split: physical `RowStore`, generic
`TableRuntimeLayout<R>`, complete MemTable, exact runtime binding, cached
indexed-column reads, and shared `WriteIndexKeySet` derivation.

Insert has the same remaining duplication. Both table families use
`RowInserter::insert_to_page`, but separately execute page retries, cache
handling, index claims, and their undo/effect sequence. Hot move updates also
need physical insertion and key claims, which makes insert sharing part of
this execution refactor.

The approved design deliberately limits sharing to common algorithms. The
user requested that broader sharing be avoided where it obscures special
user-table behavior, particularly cold rows. A hot destination can claim a
key formerly owned by a cold row; keeping that inspection in the user table
does not require duplicating the surrounding hot claim algorithm.

Catalog production consumers use direct insertion, primary-key deletion,
batch deletion, and replacement. Replacement currently inserts a new row even
when deletion finds nothing and reports whether an old row was deleted.
Occupied full-row update is not equivalent to this contract. Catalog point
mutations will adopt callback-style decisions while batch and replacement
methods preserve their semantic value.

Source Backlogs:

- `docs/backlogs/000198-share-unique-mutation-execution-across-user-and-mem-catalog-tables.md`

Issue Labels:

- type:task
- priority:medium
- codex

This is one foreground DML execution refactor with no parent RFC. The metadata
prerequisite is implemented. Public APIs, durable formats, and transaction,
checkpoint, and recovery protocols do not require migration or phased rollout.
Backlog 000198 remains open during implementation and is evaluated for closure
at task resolution.

Relevant source contracts:

- [Architecture](../architecture.md), especially physical storage and runtime
  layout ownership.
- [Transaction execution](../transaction-system.md), including admission,
  current selection, callback ownership, statement effects, and rollback.
- [Secondary-index proofs and claims](../secondary-index.md#76-proof-bound-current-row-mutation),
  which distinguish required hot entries from optional persisted-row copies.
- [Unique driver](../../doradb-storage/src/table/unique_mutate.rs):
  `UniqueMutator`, `CurrentRowSelection`, and owned hot/cold dispatch.
- [Memory table](../../doradb-storage/src/table/mem_table.rs):
  MVCC entry points, insert retries, moves, and index-effect continuations.
- [User accessor](../../doradb-storage/src/table/access.rs):
  root binding, insertion, owned hot updates, cold operations, and index proofs.
- [Hot primitives](../../doradb-storage/src/table/hot.rs) and
  [key derivation](../../doradb-storage/src/table/index_key.rs).
- [Catalog statements](../../doradb-storage/src/trx/stmt.rs) and
  [private transaction entry points](../../doradb-storage/src/trx/mod.rs).

## Goals

- Maintain one unique selection/retry driver for user and memory/catalog
  callers, preserving retained lookup evidence and callback-at-most-once
  behavior.
- Share owned hot actions, move continuation, physical insert retries, and
  hot secondary-index algorithms and effect ordering.
- Share complete insertion among direct insert, missing-entry insertion, and
  the insertion half of user cold replacement.
- Carry owned sparse update vectors through selection and move retries without
  cloning the payload.
- Keep user roots, cold operations, cold-owner inspection, persisted-row
  proofs, allocation policy, and transition waits visibly owned by user code.
- Use a typed catalog primary-key callback API with `UniqueMutation` and
  `UniqueMutationOutcome`; preserve catalog primary-key validation, key-based
  redo, expected-error policy, batch atomicity, and replacement behavior.
- Remove legacy MemTable unique update/delete/upsert methods and obsolete
  adapters/results after migrating all consumers and tests.
- Preserve lookup, decoding, allocation, and retained-resource behavior at the
  important existing fast paths.

## Non-Goals

- Unifying Table and MemTable ownership, introducing a universal table
  capability trait, or creating public memory-table admission.
- Generalizing cold rows into a common owned-row abstraction or moving CDB,
  LWC decoding, cold visibility, or persisted-index absence policy into the
  shared hot executor.
- Sharing nontransactional bootstrap/recovery mutation, purge algorithms,
  checkpoint publication, or range-selection and deferred driver-key rules.
- New public mutation actions, implicit occupied Insert, or changing catalog
  replacement into an update/upsert.
- Gap locks, retried/asynchronous callbacks, automatic update after a racing
  missing insert, or statement-wide unique-key permutation planning.
- New durable formats, cleanup ownership, production wait families, I/O
  backend changes, or test-runner/timeout changes.
- Repairing the transition/undo defects tracked by
  [backlog 000199](../backlogs/000199-repair-dangling-row-undo-ref-and-deferred-lock-to-delete-mismatch.md).

## Rejected Alternatives

- A universal storage-mode-aware table runtime would couple catalog resource
  lifetimes, fixed layouts, admission, and user persistence to a broader
  architecture change. The approved deliverable shares execution while
  retaining the existing owners.
- Table adapters that forward complete hot update/delete/move operations to
  their existing implementations would share the unique API but retain the
  substantial duplicated algorithms identified by the source backlog.
- A common cold/hot action-and-proof framework would conceal different
  ownership and index-completeness rules. Separate cold continuations and
  small explicit dispatch points are intentional.
- Replacing catalog delete-then-insert with occupied Update would change
  physical row identity, redo, and replacement behavior. Adding another public
  action solely for this catalog composition is outside scope.

## Plan

### 1. Borrowed execution context and module boundary

Add `table/mutate.rs` and register it in `table/mod.rs`. Use that exact module
name. Keep the shared physical primitives in `hot.rs` and unique orchestration
in `unique_mutate.rs`.

The executor has the following essential ownership shape; implementation may
separate borrow lifetimes where required without changing these contracts:

```rust
struct MutationExecutor<'op, D: 'static, R> {
    rows: &'op RowStore<D>,
    layout: &'op TableRuntimeLayout<R>,
    index_pool_role: PoolRole,
    family: MutationFamily<'op>,
}

enum MutationFamily<'op> {
    Memory,
    User(&'op UserTableAccessor<'op>),
}
```

Provide private construction paths from complete MemTable and admitted
UserTableAccessor. Each path borrows the owner's existing RowStore/layout,
preserves column allocation compatibility and exact table binding, and selects
its existing index-pool role. Do not permit arbitrary combinations of rows,
layout, and family. Memory mode includes standalone memory user tables as well
as catalog tables; pool type/role does not establish catalog identity.

The family binding supplies concrete dispatch for routing, insertion-page
sourcing/cache handling, and user-only operations. It does not own another
table/layout, synthesize persisted roots for memory mode, or implement a flat
trait containing all table behavior.

For common mutable-index access, introduce one crate-private projection:

```rust
trait MemIndexRuntime {
    type Pool: BufferPool;

    fn mem_index(&self) -> MemIndexRef<'_, Self::Pool>;
}
```

`MemIndexRef` borrows either a UniqueMemIndex or a NonUniqueMemIndex. Implement
the projection for directly owned InMemorySecondaryIndex and the user layout's
Arc-owned SecondaryIndex. It is infallible runtime borrowing, with no lookup,
allocation, cold, or settlement behavior. Keep its visibility local to the
consumers that need it.

Iterate the existing generic layout's paired specifications/runtime entries;
do not allocate a second per-operation index collection or clone index owners.
Use shared WriteIndexKeySet derivation directly. Retained undo and branch keys
carry exact IndexRefs validated against the same layout; never reconstruct an
IndexID from a slot or classify a memory user index as catalog by pool type.

### 2. Actions, callbacks, and typed error boundaries

Use the existing `UniqueMutation` action directly for user and catalog
callbacks. `Insert` owns `Vec<Val>`; `Update` owns ordered sparse
`Vec<UpdateCol>`. Borrow update slices for validation and row inspection, then
consume the owned vector for terminal effects or transfer it through physical
move retries. No separate internal action enum, update-input wrapper, or
full-row update variant is needed.

This supersedes the original full-row MVCC update requirement following user
review: its only producers were standalone memory-table tests. Callback upsert
tests choose Insert on absence and sparse Update on occupancy.

Missing entries accept Skip or Insert; occupied entries accept Skip, Update,
or Delete. The shared validator enforces entry/action validity and selected-key
agreement for Insert, including public trusted-payload mode. Ordinary payload
validation follows the public DML opt-out, while catalog payload validation is
mandatory. Insert uses full-row validation and Update uses sparse-update
validation. Empty sparse Update releases only this invocation's provisional
ownership and returns Updated(original_row_id); preserve existing nonempty
sparse update behavior.

The unique driver's decision invocation is generic over its error carrier.
Use a local explicit engine-error mapping at this orchestration boundary,
equivalent to:

```rust
F: for<'row> FnOnce(
    Option<&mut LazyRow<'row>>,
) -> std::result::Result<UniqueMutation, CE>
M: Fn(QuadError) -> CE
```

The public adapter maps typed engine errors into CallbackError::Engine once
and preserves CallbackError::User(E) intact. Catalog/internal execution can use
QuadError directly, with catalog Runtime callback reports promoted into that
native carrier. Do not route catalog errors through public Error and reconstruct
them later. Engine-only allocation, index, and physical helpers retain their
native narrow carriers; converge only at real mixed execution boundaries.

No Send, Sync, Clone, or static capture requirement is added to callbacks.
Borrowed row values remain callback-scoped. Shared execution registers effects;
existing statement runners alone settle success, error, rollback, and dropped
futures. On callback/validation error, leave acquired effects with that runner.

### 3. One unique selection driver with user cold continuations

Refactor UniqueMutator to use the borrowed executor, retaining the transaction
runtime, mutable StmtEffects, exact selected index/key, validation policy, and
optional catalog redo key. Preserve CurrentRowSelection and its established
position/observation rules.

Each attempt has memory mode or a concrete UserMutationAttempt containing the
existing accessor and a compatible TableRootSnapshot. User code captures and
validates that root. The root remains attempt-local and stays bound to index
handles, observations, and effects that use it.

The shared flow is:

1. Begin an attempt and perform one lookup through the appropriate existing
   memory/composite binding. Normalize to Missing, Candidate with retained
   observation, or Retry. A direct MemTree miss remains already validated;
   a composite miss validates its retained observation before confirming
   absence. Do not add redundant validation to the memory fast path.
2. Route candidates through RowStore for memory tables and existing user
   routing for user tables. Preserve row-page range/generation invariants.
3. For hot rows, use HotRowMutator::try_lock_current before interpreting
   mutable key/delete state. Follow authoritative successors, retain the
   original observation, and preserve deletion timestamp boundaries.
4. For a cold route, call the user selection helper. It returns an owned
   cold selection, a RowInspection rejection, or a prepare listener. It
   performs no outer lookup/retry loop of its own.
5. After definitive ownership, invoke the callback once. Apply an owned hot
   action through shared execution. Transfer an owned cold selection to the
   concrete user cold-action method.
6. Before prepare/transition waits and selection retries, release attempt
   resources. Keep the existing cooperative yield and poison checks.
   Preparing uses the existing transaction wait; user transitions use the
   existing user route-publication wait.
7. Confirmed absence ends the selection attempt before invoking the callback
   with None. A selected Insert begins a fresh insertion attempt, as today.
   A racing insert failure never retries the callback or changes its action.

Move the current select_cold_row and mutate_owned_cold_row implementation under
the user-table implementation, preserving its CDB ordering, immutable-image
validation, indexed-only deletion decoding, deferred LazyRow buffer, consumed
marker semantics, and typed errors. A separate cold action match is acceptable
because it owns different resources. The existing small CurrentRowSelection
classifier may continue accepting cold rejection findings.

Hot write access stays scoped through the callback and synchronous undo
conversion; no mutable row access crosses an await. Cold acquisition still
registers provisional undo synchronously before calling the callback. Skip and
empty Update cancel only newly acquired ownership. Memory LWC/TRANSITION
routing remains an invariant violation, without dummy cold/root state.

### 4. Physical insertion and complete insertion

Share insertion at two execution levels plus the direct entry wrapper:

| Interface | Responsibility |
|---|---|
| insert_row | Retry page allocation/selection; initialize a hot row and supplied backward branches through RowInserter; register existing row undo/redo; return RowID and page guard |
| insert_in_attempt | Derive the complete new key set, call insert_row, and claim every new index entry under the supplied attempt |
| insert_mvcc | Begin an insertion attempt and call insert_in_attempt |

The input to physical insertion remains owned values plus existing
IndexBranches and Insert undo semantics. NoSpaceOrFrozen returns those owned
inputs to the same retry loop. Keep one initial key derivation per complete
insert rather than recomputing it for each page retry.

Page sourcing and successful-page caching remain explicit family operations:

- User insertion first uses the transaction's versioned active-page cache,
  then RowStore allocation with the existing RowPageCreateRedoCtx. Success
  updates the transaction cache.
- Memory insertion uses the existing RowStore allocation/free-list path and
  caches the page back in that RowStore.
- RowInserter continues emitting the existing row Insert redo after physical
  initialization. User page-creation redo is not added to catalog allocation.

Direct insertion and unique missing-entry insertion use the complete path.
Cold replacement remains a user operation: after its cold deletion effects it
calls insert_in_attempt with the same captured root. A hot move calls only
insert_row with its prepared branches, then its move-index continuation;
ordinary complete index insertion must not run a second time for that move.

Retain existing source/destination page-pin responsibilities and release
mutable row access before allocation or index awaits. Release the source pin
owned by move preparation before replacement allocation; existing cursor-owned
pins remain the responsibility of their current callers.

### 5. Shared owned hot actions and old-index authority

Extract the duplicated hot update/move/index-effect continuations from
MemTable and UserTableAccessor into mutate.rs. Reuse HotRowMutator and
PreparedHotMoveUpdate for physical mutation and branch construction.

For in-place changes, reconstruct only affected old/new keys using actual
changed indexed columns. For moves, derive the complete old key set from the
already materialized old row before consuming it. Preserve direct exchange for
unchanged unique keys and ordinary new-key claims for changed keys. Record the
physical replacement RowID as Updated, not Inserted.

Represent common old hot authority as an opaque, consuming OwnedHotIndexSet
and its single-entry form for selective in-place updates:

- The user constructor first establishes the existing RowPage/MemRequired
  proof against the captured root. Conversion retains the root borrow through
  its lifetime, plus the exact layout and owned keys.
- The memory constructor requires stable hot write ownership and that
  MemTable's fixed exact layout.
- A cold/CDB proof cannot construct this hot view, including when a cold row
  is MemRequired relative to an older root.
- Set consumption preserves complete active-index slot order; individual
  entry construction is limited to affected keys after in-place ownership.

Shared old-hot effects require the expected active MemIndex owner. Missing or
incompatible state is a release invariant violation. Keep user root/pivot and
new-hot-RowID checks at the authority boundary; do not replace proof construction
with an arbitrary boolean. Hot deletion copies keys while protected and
releases its page before awaiting index masking.

The existing user OwnedRowIndexSetProof and cold consumption remain responsible
for persisted-row completeness and optional MemIndex copies. Their absence
case continues producing no mask, undo, purge payload, or overlay. Do not move
this behavior into a configurable common hot mask routine.

Use the shared hot effect implementation from existing user point/range/table
mutation call sites where they already perform the same owned operation.
Keep their selection, scan boundaries, deferred key updates, and cold policies
unchanged.

### 6. New unique claims and the cold-owner handoff

Share hot destination key-claim orchestration, including ordinary insert,
in-place key change, and changed-key move continuation. Preserve the
same-RowID delete-shadow merge behavior used by repeated in-place rekeying.

Use existing memory insertion/compare-exchange and user composite observed
insertion. A small internal claim wrapper can represent a memory claim or the
existing user UniqueOwnerObservation. It retains exact runtime/key/owner/delete
state and source, is consumed by replacement, and adds no second DiskTree read.
Do not generalize the underlying composite index or tree algorithms.

For an occupied claim:

1. Retain the exact claim observation while validating its owner.
2. Inspect hot owner history with shared row/undo logic. Derive backward
   branches and any writer-owned HotForwardSource from the actual prior owner.
3. Delegate user cold cases to the existing resolve_unmasked_lwc_duplicate
   and link_for_unique_index_lwc logic. User code decides marker precedence,
   visible duplicate versus reuse, immutable-key agreement, and cold terminal
   branch construction. Shared code does not infer reuse from a delete bit or
   absence of a MemIndex copy.
4. Pin any hot forward source before the index exchange. Consume the exact
   observation, register index undo, and publish the forward hint in the
   existing synchronous order. Preserve the destination-owned restoration
   journal and rollback responsibility.
5. Retry a purge-induced absence through the existing insertion loop; a
   mismatching owner produces the existing conflict instead of overwrite.

Cold-owner validation can be needed by a hot insert or in-place update.
This handoff is independent of owned cold-row update/delete, which remains
entirely in user code.

### 7. Catalog callback API and statement compositions

Add the following crate-private entry on PrivateTransaction and its matching
statement implementation:

```rust
pub(crate) async fn catalog_primary_key_mutate_mvcc<F>(
    &mut self,
    table: &CatalogTable,
    index: CatalogIndexNo,
    key_vals: Vec<Val>,
    decide: F,
) -> RuntimeOrFatalResult<UniqueMutationOutcome>
where
    F: for<'row> FnOnce(
        Option<&mut LazyRow<'row>>,
    ) -> RuntimeResult<UniqueMutation>;
```

The private transaction entry opens one statement. Its statement implementation
validates the catalog primary key, acquires the existing metadata/data locks,
and delegates to the shared memory-mode unique driver using UniqueMutation
directly. Catalog callbacks can use existing internal typed
LazyRow access; decoding errors receive catalog context at their consuming
boundary. Do not introduce a second row-view abstraction or arbitrary
application-error handling for private catalog code.

Capture the selected catalog key for key-based update/delete redo before the
callback changes row values. Reuse the existing optional CatalogSelectKey input
at the owned hot boundary; choose no policy from buffer-pool type. Preserve
default mandatory catalog payload validation and the existing treatment of
invalid internal actions and unexpected operation/lifecycle errors.

Migrate domain single-delete methods to this callback API: occupied chooses
Delete, missing chooses Skip, and the caller maps Deleted/Noop to its existing
boolean contract. Remove the redundant transaction/statement single-delete
entry points after migrating production and test callers.

Retain these semantic compositions:

- Batch deletion validates every key, admits/locks the catalog table once, and
  invokes the shared driver with one Delete/Skip callback per key inside one
  statement. It returns the deletion count. It does not repeatedly open
  transaction-level point statements.
- Replacement validates the key and new row before effects, admits/locks once,
  runs callback deletion, and then calls shared insertion unconditionally
  after successful deletion/absence. It reports whether an old row was deleted.
  Missing deletion still inserts. Failure rolls back both parts as one
  statement; earlier successful statements remain intact.
- Direct single/batch insertion continues using shared insertion without an
  additional unique-point lookup or callback.
- catalog_try_insert_unique_batch_mvcc preserves expected DuplicateKey and
  WriteConflict reports for its optimistic key races. Ordinary catalog
  operations retain their separate invariant policy. Adjust native error
  adapters only as needed for the shared result carrier, preserving report
  frames and Fatal propagation.

Catalog storage methods such as delete_by_id and replace remain useful domain
adapters. Use UniqueMutationOutcome, booleans, or counts at the appropriate
boundary so legacy mutation result enums can disappear once unused.

### 8. Caller migration and completion checks

Migrate all standalone memory tests and statement test helpers to callback
selection through the shared executor. Retain callback upsert coverage using
Insert and sparse Update decisions from shared test utilities; do not
reintroduce legacy production upsert/update/delete wrappers solely for tests.

Remove MemTable::update_unique_mvcc_input, update_unique_mvcc,
upsert_unique_mvcc, and delete_unique_mvcc, their duplicated continuations, and
UpdateUniqueMvcc/UpdateMvcc/UpsertMvcc/DeleteMvcc where no callers remain.
Thin direct insert and useful catalog compositions may remain. Leave
nontransactional memory operations and purge entry points in their current
owners.

Update architecture/transaction/index documentation where it describes
separate memory/user execution, recording both the shared hot boundary and
the retained user cold boundary. Do not broaden those documents into a new
storage architecture.

Inspect the final diff for duplicate unique selection loops and hot
move/index-effect algorithms. Small family dispatch methods and independent
cold action matches are intentional. Verify that a maintainer can follow a
memory/hot path without learning CDB or LWC implementation details.

## Implementation Notes

Implemented the shared foreground execution boundary in `table/mutate.rs`.
`UniqueMutator` now drives memory/catalog and user selection with a generic
native error mapping and a synchronous callback boundary. User cold selection,
CDB claims, decoding, cold actions, and persisted-index proof consumption remain
in UserTableAccessor.

The executor shares complete insertion, owned hot update/delete effects,
physical move preparation, and unique/non-unique index maintenance. Unique
claims retain the original memory/composite owner observation. Hot index
authority requires the source row's synchronously converted hot undo; user
conversion additionally consumes RowPage/MemRequired proof. Unique selection
transfers its page pin into the update continuation, so move preparation releases
it before allocation; scan cursors retain their own pins explicitly.

Unique selection and insertion claims use one executor `bind_unique` call.
The executor selects the memory or user family itself. User binding derives
the unique view directly from the captured table-root snapshot in the owning
accessor; the returned view's lifetimes retain the runtime, snapshot, and pool
guard borrows. This removes caller-managed optional handles without new root
captures, allocations, or index-owner clones. Other index-read consumers retain
their existing handle-based APIs.

Catalog single deletes now use the typed primary-key callback API. Batch delete
and replacement retain one statement, mandatory validation, exact catalog keys
for redo, and their count/boolean contracts. Replacement inserts after both
present and missing selections. Removed the legacy memory update/delete/upsert
algorithms and their result enums. User review removed the unused full-row
MVCC update capability, its input/view/iterator types, and the duplicate
internal action enum. Shared mutation now consumes UniqueMutation directly,
with owned sparse update vectors and borrowed update slices. Memory and user
callback upsert tests share one decision builder; sparse shape validation
coverage lives beside DmlValidator.

Regression review preserved the existing observed-lookup and lazy-row fast
paths. Retained undo keys use the already-bound exact entry rather than
repeating external selector validation. The in-place indexed-column update
counter now records one paired layout iteration replacing its previous
metadata-only walk; selector admission still records one direct validation and
zero IndexID-map lookups. Standalone-memory failure tests restore their actual
row undo directly before statement settlement because those fixtures have no
catalog-cache entry.

Added catalog boundary tests for callback decisions and invocation counts,
mandatory payload/action validation, Runtime report preservation, key-based
update/delete redo, missing/present replacement, batch rollback, and real
fixed-pool exhaustion in the insertion half of replacement. The failure test
checks restored row/index state, unchanged prior statement effects, and later
successful operations. Existing memory, hot-forward, cold-owner, checkpoint,
recovery, and callback tests remain in the validation suites.

Validation on 2026-09-13 after the review cleanups:

- `rtk cargo fmt --all -- --check`: passed, also enforced by style audit.
- `rtk cargo clippy --workspace --all-targets -- -D warnings`: passed.
- `rtk cargo nextest run --workspace`: 2,003 passed.
- `rtk cargo nextest run -p doradb-storage --no-default-features --features libaio`:
  1,887 passed.
- `tools/style_audit.rs`: passed for 27 branch-diff Rust files, including the
  new executor and sparse-update consumers.
- Focused coverage after the sparse-update cleanup across 12 files: 95.11% combined
  (22,687/23,853 lines). Every file exceeded 90%; DmlValidator reached 90.43%,
  hot mutation 93.71%, the shared executor 97.08%, and unique mutation 97.91%.
- Focused coverage after merging unique-index binding across mutate, access,
  and unique_mutate: 96.43% combined (11,034/11,443 lines). The snapshot-bound
  unique-index factory was fully covered.
- Removed one obsolete column point-delete test; replaced three update-wrapper
  tests with one table-driven validator test. Behavioral callback upsert,
  replacement, conflict, move, and undo/index tests remain.
- The refreshed public-error audit matches `docs/public-error-audit.csv`;
  public error conversion occurs once in the user adapter. Unsafe inventory
  is unchanged.

No public API, durable format, backend configuration, or nextest configuration
changed. Backlog 000199 remains outside this task. Source-backlog closure and
formal task resolution remain separate from this implementation record.

## Impacts

- New `doradb-storage/src/table/mutate.rs`: borrowed executor, runtime
  projection, insertion, and shared hot effects.
- `table/unique_mutate.rs`: one unique driver over both families and explicit
  user cold continuations; current-row classification remains shared.
- `table/access.rs`: user attempt/root binding, retained cold selection and
  mutation, proof construction, and delegation of hot execution.
- `table/mem_table.rs`, `table/hot.rs`, `table/index_key.rs`, and
  `table/mod.rs`: shared consumers, exact hot authority, exports, and removal
  of obsolete methods/helpers.
- `table/index_mutate.rs` and other existing owned-hot consumers: signature
  adaptation only where extracted helpers are used; traversal semantics stay.
- `row/ops.rs`: retain public decisions/outcomes; remove unused internal legacy
  results and the obsolete update-input/view/iterator types.
- `trx/stmt.rs`, `trx/mod.rs`, and `catalog/storage/`: private catalog
  callback admission, atomic compositions, typed adapters, and caller/tests
  migration. Public transaction signatures remain unchanged.
- Architecture, transaction, and secondary-index documents: durable record of
  the execution boundary and preserved ownership contracts.

## Test Cases

### Shared unique selection and callback behavior

- Run equivalent user-hot and standalone-memory scenarios for missing and
  occupied actions, current-row computation, selected-key agreement, and
  invalid decisions. Cover public trusted-payload mode and mandatory catalog
  validation separately.
- Preserve hot forward chains, repeated RowIDs, per-index successors,
  replacement/rekey races, strict deletion timestamps, active/preparing
  owners, stale lookup evidence, and rollback-erased destination claims.
- Check callback counts through retries, prepare waits, transition routing,
  missing-insert races, and cancellation. User callbacks preserve non-Clone
  and borrowed error/capture behavior.
- Skip and empty sparse Update release only new ownership; previous statement
  effects remain. No-op actions avoid dense lazy-row storage initialization.
- Memory LWC/TRANSITION and invalid row-page routing still fail their existing
  invariants rather than entering user cold processing.

### Hot mutation, insertion, and index effects

- Sparse updates and callback upserts cover in-place changes, space/frozen moves,
  resulting RowIDs, owned-value preservation, unchanged keys, changed keys,
  repeated rekeying, unique/non-unique indexes, and old-snapshot visibility.
- Include sparse runtime slots and memory-user IndexIDs different from slots,
  checking actual index undo and restoration through statement settlement.
- Insert tests cover page retry/exhaustion, page-cache reuse, existing row redo,
  user page-creation redo, and rollback after a late index claim failure.
- Moves use one physical insert and one move-index effect sequence, preserving
  correct previous-owner branches and forward restoration on failure.
- Owned hot index masking/replacement requires exact entries and complete
  authority. Wrong/missing owners fail invariant checks; ordinary duplicate
  and write-conflict outcomes retain their typed behavior.
- Retain existing successful-hot lookup counts and MemIndex-hit DiskTree
  short-circuit assertions; avoid extra per-operation runtime allocations.

### User cold regression and hot/cold handoff

- Preserve cold selection timestamp/marker precedence, fresh versus consumed
  claims, prepare races, Skip/empty cancellation, and callback-at-most-once.
- Preserve cold deletion's indexed-only decoding and unchanged optional
  MemIndex-copy masking, including no overlay/undo when a persisted copy is
  absent. Cover a cold row that is MemRequired relative to an older root.
- Exercise hot insert and hot key change claiming a previously cold-owned key:
  visible duplicates, stale/durable-deleted owners, CDB authority, backward
  snapshot links, exact composite observation replacement, races, and rollback.
- Cold replacement reuses its captured root for insertion and preserves
  complete row/index effects. Existing checkpoint/recovery integration tests
  remain applicable; backlog 000199 repairs are not claimed.

### Catalog APIs and atomicity

- Catalog callbacks receive current owned rows or absence, validate primary-key
  selectors and payloads, and exercise Skip/Insert/Update/Delete decisions.
  Check callback invocation counts, key-based update/delete redo, and typed
  decoding/runtime/fatal report preservation.
- Single domain deletion preserves its boolean result. Batch deletion remains
  one statement and returns the correct count across present/missing keys.
- Replacement covers present and missing old rows, a newly inserted physical
  row, failure in the insertion half, and preservation of earlier statements.
  Inspect row/index state and redo after rollback.
- Preserve whole-batch rollback on insertion failure and expected
  DuplicateKey/WriteConflict handling in optimistic catalog insertion.
- Retain catalog DDL, checkpoint, and restart tests as integration checks for
  unchanged catalog redo and replacement semantics.

Follow [unit-test.md](../process/unit-test.md) and reuse existing fixtures,
table-driven cases, and semantic race gates. Add only the boundary coverage
needed by this refactor; avoid duplicated setup and scheduler sleeps.

Required implementation validation:

```bash
rtk cargo fmt --all -- --check
rtk cargo clippy --workspace --all-targets -- -D warnings
rtk cargo nextest run --workspace
rtk cargo nextest run -p doradb-storage --no-default-features --features libaio
```

Run focused relevant tests during implementation and the normal style gate at
task resolution. Keep `.config/nextest.toml` authoritative and unchanged.

## Open Questions

No unresolved design decision blocks implementation. The approved choices are
the mutate.rs module name, bounded common hot execution, explicit user cold
continuations, typed catalog primary-key callbacks, and retained atomic
catalog batch/replacement methods.

The principal review risks are weakened root/row authority during hot proof
extraction, loss of owned mutation payloads across retries, duplicate or
reordered move/index effects, error-domain widening at catalog adapters, and
accidental statement splitting. The implementation and tests above must
demonstrate those contracts directly.

Any proposal to move cold ownership or persisted-index policy into the common
core, introduce a broader capability framework, or change replacement,
recovery, or transition semantics is outside this approved design and requires
separate review. Backlog 000199 remains a separate follow-up.
