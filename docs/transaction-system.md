# Transaction System

## Overview and Core Features

The transaction system is based on a Hybrid Heap-Index Architecture. 
It merges the write efficiency of LSM (Log-Structured Merge) principles with the read performance of B+Trees, specifically tailored for HTAP (Hybrid Transaction/Analytical Processing) workloads.

The system is designed to decouple foreground transaction processing from background persistence, eliminating traditional bottlenecks such as global locking and random I/O during high concurrency.

### CoW Checkpoint + Commit Log Recovery

The system employs a unique persistence and recovery model that fundamentally differs from traditional ARIES algorithms (Steal / No-Force).

| Feature | This System (No-Steal / No-Force) | Traditional ARIES (Steal / No-Force) |
|----|----|----|
| **Dirty Page Policy** | **Strict No-Steal**: Disk data structures (DiskTree/ColumnStore) are immutable or CoW, containing only committed data. No dirty pages are flushed. | **Steal**: Dirty pages from uncommitted transactions can be flushed to disk, requiring Undo Logs for rollback. |
| **Persistence** | **No-Force**: Durability-required commits force the commit log. Data pages remain in memory until checkpoint. Runtime-only effects may still pass through ordered commit without writing log bytes. | **No-Force**: Only WAL is forced. |
| **Checkpoint** | **Commit-Only**: Background checkpoints publish committed cold data plus companion index/delete state via Copy-on-Write (CoW). | **Fuzzy Checkpoint**: Flushes dirty pages from the buffer pool, dealing with complex LSN ordering. |
| **Recovery** | **Redo-Only**: Crash recovery involves replaying the Commit Log to rebuild memory state. **No Undo phase** is required for disk structures. | **Redo + Undo**: Requires replaying history and then undoing uncommitted changes using Undo Logs. |

**Design Advantages**:

1. Simplified Recovery: Eliminates complex disk-based Undo recovery logic, significantly reducing RTO.
2. Write Throughput: Foreground writes are purely in-memory; background persistence uses sequential commit log writes and batch CoW, optimizing for SSDs.
3. Stable Read Performance: Index structures remain compact and fragmentation-free, avoiding the severe read amplification often seen in LSM-Trees.

## Heap Table and RowID Design

The Heap Table uses a **Tiered Architecture**, combining an in-memory RowStore (optimized for OLTP) and an on-disk ColumnStore (optimized for OLAP).

### Unified RowID

- A global, auto-incrementing 64-bit RowID identifies every data row.
- Watermark (Pivot): The system maintains a Pivot_RowID.
  - RowID < Pivot: Data resides in the disk-based ColumnStore.
  - RowID >= Pivot: Data resides in the in-memory RowStore.

### In-Memory RowStore

- Physical Structure
  - **Data Page**: Contiguous memory pages storing tuples (compact, no MVCC headers).
  - **Page Index**: Maps `RowID Range -> Page*`.
- Write Pattern
  - New inserts append to the tail page.
  - Page creation is serialized and logged in commit log.
  - Hot updates first install a row undo `Lock`, then either mutate the row
    page in place or fall back to a move update when the page is frozen or has
    no reusable space.
  - Hot deletes set the row-page delete bit after the row undo lock is owned.
- Split-Level MVCC
  - MVCC metadata is not stored in Data Pages but in a separate **Undo Mapping Table** (`RowID -> UndoChain*`).
  - **Undo Log**: Stored in transaction-private memory, linking old versions and visibility timestamps.
  - The latest row-page image is visible only if the undo-head timestamp is
    visible to the reader. Otherwise the reader walks the undo main branch and
    applies inverse `Insert`/`Update`/`Delete` operations to reconstruct an
    older version.
  - Unique-index runtime branches can connect the latest owner to an older hot
    owner with a different RowID when the ordinary main branch cannot reach the
    older same-key version.

### On-Disk ColumnStore with Deletion Buffer

- **Physical Structure**: Immutable PAX (Partition Attributes Across) format blocks.
- **Index**: Sparse Block Index + Delete Bitmap.
- **Handling Deletes/Updates**:
  - Since ColumnStore blocks are immutable, foreground modifications are
    represented by an in-memory **ColumnDeletionBuffer** plus hot RowStore
    replacement rows when needed.
  - **ColumnDeletionBuffer**: a concurrent table-level map storing
    `RowID -> DeleteMarker`, where a marker is either a shared transaction
    status (`Ref`) or a compact committed delete timestamp (`Committed`).
  - **Read Path**: queries combine persisted delete bitmap state with
    snapshot-aware deletion-buffer visibility. A committed marker hides the row
    only when `delete_cts <= reader_sts`; a committed marker newer than the
    reader snapshot preserves the old cold row for that reader. An uncommitted
    marker hides the row from its owning transaction and acts as a write
    conflict for other writers, except that a foreground writer waits when the
    owner has entered prepare. The waiter releases operation-local row, index,
    block, page, and deletion-buffer guards, then retries from authoritative
    index, row-location, and marker state. An ordinary active owner remains an
    immediate write conflict.
    A foreign owner in ordered prepare is the sole row-prepare wait case. Hot
    and cold mutation use the shared prepare-or-poison retry contract described
    in [Shutdown and Engine Poison](shutdown-and-poison.md#hot--and-cold-row-prepare-waiting);
    ordinary active-owner conflicts and uncontended mutations stay on their
    immediate paths.
  - **Cold Update Path**: update of an LWC row is modeled as claiming the old
    cold RowID in the deletion buffer, recording cold-delete undo/redo,
    masking old secondary-index entries, and inserting the modified values as a
    new hot RowStore row.
- **Tuple Mover**: Maintenance converts frozen RowStore pages into ColumnStore
  blocks and advances the `Pivot_RowID`. Each table owns one volatile
  freeze/checkpoint workflow and canonical frozen batch. Asynchronous freeze
  loading leaves foreground reads and writes unchanged; the final freeze
  publication takes one short page-state write lock at a time. Repeated freeze
  returns the original fence rather than extending the prefix. Checkpoint
  optimistically prepares cutoff-specific page plans, acquires publish/drop
  admission before the first page transition, and retains admission through
  route/root publication and commit. Frozen-page mutations publish paired
  equality-only version increments so final page-local validation can reuse or
  rebuild each plan without a batch-wide lock set.
  This workflow is maintenance-only: foreground statements, transactions, row
  writes, and scans do not acquire or inspect its mutex.

## Index Structure

Secondary indexes use a hot/cold split:

- `MemIndex` stores hot mutable state
- `DiskTree` stores checkpointed cold state
- `DiskTree` is maintained only as companion work of table data/deletion
  checkpoint

Unique and non-unique indexes do not share the same physical model:

- unique indexes keep the latest logical-key mapping
- non-unique indexes keep exact entries keyed by `(logical_key, row_id)`

For the detailed index design, see [`secondary-index.md`](./secondary-index.md).

### In-Memory MemIndex

- **Role**: Write Cache.
- **Behavior**:
  - absorbs foreground inserts, updates, and deletes
  - shadows stale cold `DiskTree` entries until checkpoint publishes updated
    persistent state
  - stays memory-first on the write path

### On-Disk CoW B+Tree (DiskTree)

- **Structure**: A Copy-on-Write B+Tree similar to LMDB.
- **Role**:
  - stores checkpointed cold secondary-index state
  - is loaded directly from the latest table checkpoint during restart
- **Double Root Mechanism**:
  - The file header contains two root page pointers (Root A and Root B).
  - Only one root is valid/active at any time.
  - A monotonic transaction ID or timestamp in the header indicates which root is newer.
- **Publication**:
  1. A table checkpoint reads the active root.
  2. It applies companion updates generated from checkpointed rows or
     checkpointed cold-row deletions.
  3. Modified nodes are copied to new disk pages (CoW), propagating path updates
     up to a **new root**.
  4. The new root is published together with the table checkpoint metadata.
  - This ensures the DiskTree is always in a consistent state, even if the system crashes during a write.

## Transaction System

### Timestamp Management

- **STS (Start Timestamp)**: Acquired at transaction start from a global atomic sequence.
- **CTS (Commit Timestamp)**: Acquired at transaction commit.

Mandatory maintenance may register a `PrivateSnapshot` from the same STS
sequence. It participates in the active GC watermark but is not a transaction
and receives no transaction id or status.

### Transaction Lifecycle

#### Execution Phase

Runtime user-table root reads are bound through the immutable
`TrxContext::read_proof()` carried by `TrxRuntime`. The proof is a typed witness
minted from the reader transaction context and is used only to bind one
synchronous observation of the current table-file root. Runtime helpers may
then copy a single secondary `DiskTree` root id or build an owned
`TableRootSnapshot` for broader MVCC and GC work. Checkpoint, recovery, catalog
load, and file-internal root reads remain explicit unchecked exceptions outside
this runtime transaction contract.

MemIndex cleanup is the separate registered-reader case. Its
`PrivateSnapshot` directly brands the captured `TableRootSnapshot` lifetime;
it cannot mint `TrxReadProof`, and the captured root cannot outlive the active
STS registration.

Each user statement runs through `Transaction::exec(async |stmt| { ... })`.
The public `Transaction` is a weak, non-cloneable capability containing weak
reachability to its exact `SessionState`, `SessionOperationKey`, and its
independent engine-wide `TrxID`. `SessionOperationKey` is the exact
`(SessionID, OperationID)` identity;
the raw operation id is a session-local `u64` allocated from one sequence shared
by transactions, DDL, maintenance, and explicit-lock mutation. The facade does
not own `EngineCore`, `SessionState`, a transaction core, or a stable operation
entry. A foreground checkout first acquires lifecycle admission through its
per-session façade, upgrades the exact weak state once, validates engine health,
and resolves the operation key directly on that state. It then builds a
`TrxAttachment` containing `SessionRuntime`; checkout validates the handle's
independent `TrxID` against the entry under the entry mutex. Terminal and
cleanup paths omit new foreground admission but perform the same exact state,
operation-key, and transaction-id validation.

`SessionState` has orthogonal disposition (`Open`, `CloseRequested`, or
`Abandoned`), one effectful operation slot (`Idle`, `Active`, or `Closed`), and
a standalone observer count. The observer count lets diagnostics and progress
waits coexist with an active transaction without allocating an operation id.
Its existing lifecycle mutex also protects one optional `public_trx_cache`
containing a ready `Box<TrxInner>`. A public transaction takes that box, and
successful terminal processing resets and returns it before publishing the
idle lifecycle state. DDL and maintenance leave the public cache parked while
their private transaction owns a separately allocated core.
An active slot owns exactly one `Arc<SessionOperationEntry>`. This is the
direct generalization of the former transaction entry, not an outer wrapper:
immutable key and kind fields sit beside one compact mutex containing operation
state, optional `TrxID`, an optional checked-in `Box<TrxInner>`, cleanup
intent, and foreground ownership. The entry contains no whole operation future
and no strong engine reference.

The reusable public transaction core box is allocated eagerly once per
session. A ready core has zero identity fields, no lock state, empty
zero-capacity effect containers, and one uniquely owned zero-valued
`SharedTrxStatus`. Public transaction begin calls `TrxInner::init` after
allocating the new STS/transaction id: `Arc::get_mut` proves the ready status
was never shared, then initialization stores the active transaction id, STS,
GC bucket, and lock owner without allocating either the core or its status.

Successful terminal processing carries the emptied box through
`PreparedTrx`/`PrecommitTrx` where necessary. After the old status is terminal,
all effects and locks are released, and prepare state is cleared. A public
transaction calls `TrxInner::reset`, which drops retained container capacity
and replaces the context with a newly allocated zero-valued status for the next
transaction. A private transaction drops its fresh core directly without
resetting it or taking an additional cache-related lifecycle lock. Each core
records this terminal cache policy when it is created, so transaction
attachments need no additional kind field. The old status identity is never
reused: undo, deletion, transition, or checkpoint owners that cloned it
continue observing the old terminal result. Fatal retention drops the failed
core instead of returning it to the session.

Prepare notification is waiter-injected. Entering ordered prepare only
publishes the shared `preparing` flag; it does not lock or allocate an event.
The first hot- or cold-row waiter installs one event under the transaction's
prepare mutex, and later waiters share it. Prepare completion publishes the
commit or rollback outcome, clears `preparing`, takes the optional event under
the same mutex, and wakes listeners after releasing the mutex. Successful
failed-precommit rollback removes transaction-owned cold markers before this
wake. Fatal cleanup publishes engine poison before releasing waiters, and
waiters check that poison before retrying retained state. This notification
protocol does not authorize logical-lock release before redo durability.

During an active public transaction, the owning box is checked out for one
non-terminal operation through `SessionOperationCheckout`; ordinary checkout
drop returns the same box through the entry mutex. A private transaction
instead owns one checkout continuously from direct construction through
terminal conversion or synchronous panic parking. The checkout owns a
`TrxAttachment` containing the exact `SessionRuntime` and
exposes a copyable `TrxRuntime` value that pairs immutable `TrxContext` with
borrowed access to `EngineCore`, its canonical pool guards, and the
session-local user-table cache. `TrxContext` never
stores the attachment. Normal statement checkout/check-in does not reacquire
the session lifecycle mutex, allocate, touch the operation change notifier, or
send cleanup work.

Each session user-table cache entry contains one weak `Table` runtime hint and
an optional `VersionedPageID`. The weak runtime is never authoritative for
foreground admission: a cache miss or stale weak pointer is resolved through
the catalog, and a cache hit is usable only after the transaction's metadata
binding contract has admitted the operation. User-table insert selection takes
the optional page token while retaining the weak runtime entry, reopens only
the matching page generation, and otherwise falls back to the table insert free
list before allocating a page. The row inserter remains responsible for checking
active page state and capacity; cached RowID range state is not required. A
successful user insert returns the version token to the same entry. When session
state is destroyed, cached tokens whose weak runtime remains reachable are
returned to the table insert free list; tokens attached to unreachable runtimes
are discarded.

Catalog-table MVCC inserts do not use session entries. They acquire and return
versioned page tokens through the catalog table's shared insert free list, so
catalog insert capacity remains available across sessions without requiring a
user-table runtime cache entry.

`StmtState` owns the per-operation checkout and statement effects while public
`Transaction::exec` is active. It lends one `Statement` facade with direct
disjoint borrows of the checked-out `TrxInner`, operation attachment, and
effects; DML methods therefore do not resolve the entry or unwrap the carrier.
Normal public statement finish returns the core to its checked-in payload
position inside outer `Voluntary` ownership. This ends only the
operation-local checkout, not the semantic transaction lifetime; the weak
public `Transaction` remains reusable for its next call. Private catalog
statements borrow the core and attachment directly from `PrivateTransaction`,
settle their statement effects into the held `TrxInner`, and never check the
core through the entry between logical catalog-table boundaries.

Dropping an unpolled `Transaction::exec` future performs no checkout. Once
checkout succeeds, dropping the future is terminal for that public
transaction. The callback and any pending acquisition guard are destroyed
first. `StmtState` then discards statement redo, appends residual row and index
undo after prior transaction undo, and returns the complete core directly as
outer `CleanupReady`. It never exposes an intervening
available payload position. The exact-identity cleanup job claims the
core and performs whole-transaction rollback; later calls through the stale
public facade return `TransactionDiscarded`. An ordinary callback error is
different: statement-local rollback completes before ordinary check-in, so
the transaction remains reusable.

Explicit commit and rollback consume the public handle, suppress drop
abandonment, and claim the same entry and core through
`SessionOperationCompletionClaim`. Dropping a public transaction handle never
rolls back inline; it records cleanup intent on the exact entry and queues
transaction-system cleanup when the engine is still reachable.

DDL starts private transactions through its already-reserved operation
authority. `PrivateTransaction` allocates a new `TrxID` and boxed core,
inherits the outer operation key, and constructs a strong `TrxAttachment` from
the accepted operation's `SessionRuntime`. During caller preparation the entry
remains `Voluntary(None)`; accepted DDL transfers it to `Mandatory(None)`
before starting a child. Private begin validates that exact DDL state and publishes
`Mandatory(Some(Running))` directly while the core remains owned by the
private checkout and the entry payload slot remains empty. Public transactions
continue to use weak session reachability and per-operation checkout.

Accepted table and index DDL transfer the same entry to `Mandatory(None)`
before the runtime task is detached. Their nested catalog transaction follows
`Mandatory(None) -> Mandatory(Some(Running))`; consuming commit or rollback
converts the held checkout directly to `Mandatory(Some(Completing))` and then
clears the child back to `Mandatory(None)`. The core remains continuously held
across catalog statements, file/root awaits, runtime construction, and index
build work. That child terminal edge never publishes the outer operation
terminal. Successful accepted execution first proves the exact empty mandatory
state, releases its complete prepared lock scope, and consumes that proof to
publish `Terminal`. Before a supervised unwind publishes `FailedRetained`, the
DDL progress owner synchronously parks any active private checkout as
`Mandatory(Some(Available))`; the retained core and entry remain
registry-visible and block shutdown without exposing an idle session or
scheduling abandoned cleanup.

Accepted maintenance has no nested transaction state. One stateful
`MaintenanceExecution` owns its operation-specific resources inside
`AcceptedMaintenanceScope<E>`, which implements the mandatory
`AcceptedExecution` contract directly. Normal completion drops the execution
state before publishing the outer terminal state; a supervised unwind drops it
before the outer scope publishes `FailedRetained`.

Secondary `MemIndex` cleanup registers a `PrivateSnapshot` before observing
the GC horizon. The snapshot owns only an active STS registration and directly
brands the captured table-root lifetime. A root-capture race drops both the
root and registration before yielding and retrying with a fresh STS. Normal,
error, and panic paths synchronously deregister the snapshot; the stable
maintenance entry remains `Mandatory(None)` throughout and carries no nested
transaction id.

After explicit rollback claims terminal ownership and publishes `Completing`,
the claimed transaction core, undo buffers, locks, and session cleanup
attachment are synchronously submitted as a `terminal_rollback` task to the
engine-owned mandatory runtime before rollback awaits row or index storage
work. Abandoned transactions use `abandoned_transaction` tasks, and redo groups
that fail after precommit use `failed_precommit` tasks. These independently
accounted cleanup tasks bypass caller-operation capacity; cleanup within one
transaction remains sequential. The public `rollback().await` future only
waits on the task-owned completion cell; dropping that waiter does not cancel
rollback cleanup or release rollback-capable undo without making ownership
explicit.

The coherent outer labels are `Voluntary`, `Mandatory`, `CleanupReady`,
`Completing`, `Terminal`, and `FailedRetained`. `Voluntary` and `Mandatory`
optionally contain the nested private-transaction positions `Available`,
`Running`, `CleanupReady`, and `Completing`; public transaction checkout is
represented by payload ownership within `Voluntary(None)`. Handle-drop intent
is orthogonal while a transaction core is checked out, so checkout return
publishes outer `CleanupReady` exactly once. Normal mandatory private execution
uses `Running` continuously; its `Available` position is reserved for
defensive Drop and mandatory panic parking.
Cleanup messages carry `(SessionOperationKey, TrxID)` and stale, replaced, or
duplicate hints are neutral. Registry resolution uses only the operation key;
the cleanup claim atomically validates the message's `TrxID`, claimable state,
and physical payload ownership under the entry mutex.

`Statement` is a borrowed facade over operation-local runtime access and
carrier-owned statement-local `StmtEffects`; callers cannot construct or
finish it directly. Public statements settle through `StmtState`; private
catalog statements use a fresh effect accumulator borrowed alongside the
continuously held checkout. Private ordinary errors merge complete and partial
undo for whole-transaction rollback, while panic settlement discards
incomplete statement redo and folds residual undo before resuming the unwind.
Foreground table APIs receive `TrxRuntime` by value when they need pool guards,
insert-page cache access, or runtime lock assertions, while pure row MVCC
helpers continue to receive `&TrxContext`. When the callback succeeds,
statement row undo, index undo, and redo effects merge into the active
transaction. When the callback returns an ordinary error, only the current
statement effects are rolled back and the original error is returned. If that
rollback cannot access required storage, the rollback failure is fatal: storage
is poisoned and the operation entry becomes `FailedRetained`. The retained
entry stays registry-visible, blocks session reuse and shutdown, and makes
later commit or rollback attempts return an error.

Logical lock ownership is tracked outside `TrxContext`. One boxed
`FamilyLockAuthority` is allocated per session and moves linearly into
`TransactionLockState`, which pairs that root with the transaction
`curr_scope`. Public `StmtState` retains statement effects, checkout,
cancellation, and Drop policy without logical-lock state. `StreamStmtState`
owns only its transaction checkout and remains last in the stream state so
cursor/root state is destroyed before transaction check-in.

Catalog DDL mutations are owned by `CatalogStorage` and use one private
statement per logical catalog table, retaining same-table row batches in one
effect boundary. `StmtEffects` carries only DML redo. After every catalog-table
statement and invariant check succeeds, `PrivateTransaction` installs exactly
one `DDLRedo` marker directly in `TrxEffects`; an ordinary staging error leaves
all accumulated undo available for whole-transaction rollback and leaves the
transaction-level DDL slot empty.

Transaction locks close on commit,
rollback, no-op discard, or fatal transaction discard. DDL and maintenance
private transactions temporarily take the same family box from the accepted
outer operation and return it through the stable operation entry; the outer
operation scope remains owned by its carrier. Session-explicit claims stay
beside the family root across transactions and operations and close only on
selective unlock or final session teardown. Every normal close iterates the
exact scope index and does not scan manager resources.
See [Lock System](./lock-system.md) for the resource and mode model, the
implemented manager structures, and the pre-RFC redesign study.

Foreground table access enters through lock-aware `Statement` APIs and a
positive transaction-lifetime `TransactionTableBinding`. A binding hit is
checked before any new metadata-lock request and reuses the STS-visible
metadata, current `Table`, current `TableRuntimeLayout`, and transaction-owned
`TableMetadata(S)` already stored for that table. On first touch, admission
acquires transaction-owned `TableMetadata(S)` before resolving STS-visible
logical metadata or authoritative current state. It then validates the
requested table/index shape and installs the binding. An ordinary error after
acquisition installs no binding but retains the accepted metadata claim until
terminal transaction cleanup; a later attempt reuses that exact claim and
retries resolution.

Successfully bound reads and writes retain `TableMetadata(S)` until transaction
commit or rollback. Reads may use the intersection of visible and current
schema: an index absent from visible metadata returns `IndexNotFound`, while an
index visible to the transaction but removed from current state returns
`SchemaChanged`. Writes additionally require visible metadata to be the current
metadata version, rejecting stale writers before row or index mutation.
Row inserts, updates, and deletes then acquire transaction-lifetime
`TableData(IX)` before installing row undo, deletion-buffer ownership, or
secondary-index write undo. Repeated operations reuse the binding and
transaction lock cache rather than re-entering the metadata resolver or lock
manager.

Sequential full-table MVCC mutation acquires transaction-lifetime
`TableMetadata(S)` followed by `TableData(X)` before it captures the table root
and original hot-page worklist or invokes its row callback. The callback may
skip, delete, or sparsely update each latest modifiable original row. The
exclusive data lock remains held after the statement returns, including after
statement rollback, until the transaction commits or rolls back. It excludes
freeze and checkpoint page-state movement while ordinary metadata-only MVCC
readers remain admitted. A transaction that already holds `TableData(IX)` can
convert to `X` only when conversion is immediately compatible; otherwise the
operation returns `LockUpgradeWouldBlock` before invoking the callback.

Finite effectful session maintenance reserves one outer `Maintenance`
operation, acquires owned `TableMetadata(S)` followed by `TableData(IS)`, and
resolves the exact live runtime before mandatory admission. Freeze, checkpoint,
and secondary `MemIndex` cleanup transfer that complete scope into accepted
execution. The stateful execution and lock scope are one accepted owner and
remain retained through their last table/layout/index use. Hot-row-page counting
remains a caller-owned, cancellable scoped observation. These calls
preserve ordinary `IX` DML and explicit `S` table-reader concurrency while
excluding same-table DROP and serializing page freeze/transition against
full-table mutation `X`. Grants admitted by a covering explicit session lock
are still recorded under a distinct `Operation(operation_id)` owner. Scope
release consumes only that maintenance owner's fresh grants and preserves the
exact `SessionExplicit` claims.

Checkpoint retry never keeps that scope across its indefinite sleep. One
recheck registers the relevant lifecycle, transaction-terminal, GC-horizon,
poison, and shutdown listeners and then verifies the predicate again. It
returns only detached listener state, releases checkpoint attempts, page
guards, table/layout owners, and logical locks, and then sleeps. This lets
same-table DROP acquire metadata X and publish terminal lifecycle state; the
listener carries that change into the next bounded recheck. The completed
checkpoint operation is not retained across this sleep: each retry starts a
new outer operation id and prepares new logical-lock, table, workflow, and
root-mutation authority.

`CREATE TABLE` validates metadata before reservation, allocates a distinct
gap-tolerant id, and caller-prepares target metadata X plus metadata-S/data-IX
authority for the four catalog tables it writes. Mandatory acceptance then
owns those locks while it creates the deterministic table file, runs its nested
catalog transaction without further manager acquisition, builds the per-id
runtime, commits, and publishes the current history/runtime entry. The initial
table-file root uses the create transaction STS as `root_ts`.

`DROP TABLE` rejects non-user ids and same-session explicit target locks before
waiting, then caller-prepares target metadata/data X plus metadata-S/data-IX
authority for all five cascade catalog tables. Under target exclusion it
selects the exact current-live `Arc<Table>` without an extra catalog-row scan.
Mandatory execution begins the nested transaction, closes and drains the
terminal lifecycle, performs the catalog cascade, commits, and publishes
dropped-runtime/replay-floor retention. A drop waiting for an already-admitted
checkpoint publisher therefore does not delay CREATE or DROP for unrelated
table ids when runner capacity is available. Transaction rollback drops its
operation-local table caches and transaction bindings
before releasing the logical locks that authorize those runtime owners.

`CREATE INDEX` and `DROP INDEX` prepare their full target and catalog lock
sets, exact live table, table/catalog metadata-gate admissions, layout, active
root, and metadata/root plan before mandatory admission. Accepted execution
starts the private catalog transaction and acquires ordinary exact transaction
metadata/data claims. The enclosing DDL operation already holds
covering physical modes, so these nested claims publish through the owner-local
fixed slots without another manager transition. No prepared catalog-write
bypass exists. Catalog commit remains followed by table-root publication. The
final runtime-layout and catalog history transition holds the user-table
catalog entry before the table layout mutex, exposing only the old/old or
new/new metadata pointer pair to history purge.

CREATE INDEX and DROP INDEX also take same-table `TableMetadata(X)`. That grant
waits for every transaction that successfully bound the table, but an older
transaction that never touched it holds no metadata grant and does not delay
DDL. After publication, such a transaction must pass visible/current
validation on first touch and cannot newly bind a removed index or dropped
table.
Recovery, purge, and no-transaction catalog replay remain outside logical lock
acquisition because they run at internal lifecycle boundaries rather than
through foreground sessions and waiters. User-table freeze and checkpoint are
the maintenance exceptions described above because they must coordinate page
state movement with full-table mutation.

Recovery does not acquire logical locks. Recovery runs during engine startup
before foreground sessions, user transactions, or lock waiters exist, and it
reconstructs catalog/table runtime state directly from checkpoint and redo
inputs. Logical lock table contents are volatile coordination state rather than
durable data, so replaying or synthesizing locks during recovery would add no
serialization guarantee and would risk leaking startup-only owners into normal
runtime execution.

1. **Read**: 
   - Probe `MemIndex` first and then `DiskTree` as required by the index type.
   - Route to RowStore or ColumnStore based on `RowID` vs `Pivot`.
   - **Visibility Check**: Compare Reader.STS against the timestamp in the Undo Chain (for RowStore) or Deletion Buffer (for ColumnStore).
   - For unique indexes, runtime unique-key links may be followed when the
     latest logical-key owner is not enough to reach an older visible owner.
     Such links can target either hot undo history or a terminal cold owner
     reconstructed from stored undo values.

2. **Insert**:
   - Append tuple to RowStore -> Get new `RowID`.
   - Create an insert undo head so older snapshots and rollback treat the row
     as absent until the transaction commits.
   - Insert into MemIndex with `sts = My_STS`.

3. **Delete**:
   - **If RowStore**: install a row undo `Lock`, set the row-page delete bit,
     rewrite the lock entry to `Delete`, and mask secondary-index entries in
     MemIndex with index undo.
   - **If ColumnStore**: Insert a "Delete Mark" into the **ColumnDeletionBuffer**.
   - Update MemIndex: mask the old secondary-index entries so runtime lookups do
     not blindly fall through to stale cold `DiskTree` state.

4. **Update**:
   - **If RowStore**: install a row undo `Lock` before mutating the page. If
     the update fits, mutate the row page in place, rewrite the lock entry to
     `Update`, and update MemIndex only for changed index keys. If the update
     cannot fit or the page is frozen, rewrite the lock to `Delete`, insert the
     replacement as a new hot RowID, and update MemIndex for RowID/key movement.
   - **If ColumnStore**: install a deletion-buffer marker for the old cold
     RowID, insert the replacement row into hot RowStore with a new RowID, mask
     old index entries, and insert the replacement index entries.
   - Unique hot and cold updates may install runtime-only branches from the new
     hot owner to an older hot or cold owner so older snapshots can still see
     the correct logical key owner.

#### Commit Phase

1. **Classify Effects**:
   - `require_durability`: the transaction has recovery-visible redo and needs
     a stable CTS carrier in the commit log.
   - `require_ordered_commit`: the transaction has durable redo or volatile
     runtime effects that still need ordered CTS assignment, status/session
     completion, and GC handoff.
   - A transaction can require ordered commit without requiring durability. In
     that case it enters the commit ordering path but writes no log bytes.
2. **Log and Order**:
   - Durability-required transactions serialize their redo and append it to the
     single global commit log before becoming committed.
   - Ordered-only transactions use the same commit-order barrier without
     manufacturing empty redo records.
   - Transactions with no effects are discarded through the readonly/no-op path
     and do not receive a CTS.
   - Once a user transaction is prepared, assigned a CTS, and enqueued into a
     commit group, it is in the irreversible committing state. The queued
     precommit transaction, not the user's still-polled future, owns session
     commit/rollback completion after this handoff; dropping the user commit
     future may stop observing the result but must not roll the transaction back
     or leave the session permanently active.
   - If commit fails after claiming terminal ownership but before precommit
     handoff, the claimed transaction is rolled back through the same
     cleanup-worker terminal rollback path before the commit failure is
     returned or observed by a waiter.
3. **State Update**
   - Instead of updating a global transaction table or traversing the MemIndex, the transaction simply backfills the **Commit Timestamp (CTS)** into its Undo Log records.
   - For the **ColumnDeletionBuffer**, the CTS is attached to the delete/update markers.
   - The **CTS** of all undo and deletion-buffer refs in one transaction is
     backed by shared transaction status. This makes the commit operation
     lightweight: setting the shared status makes all related undo records and
     deletion-buffer markers observe the commit timestamp.
   - Prepare events are allocated only when a hot- or cold-row waiter actually
     registers. Completion still serializes the false transition with
     registration so no commit, rollback, cancellation, or fatal-cleanup race
     can lose a wakeup.
4. **Cleanup**: Discard local write buffers.

Transaction-owned logical locks are not redo, undo, durability, or ordered
commit effects by themselves. A transaction that only acquired logical locks
still uses the readonly/no-op commit discard path, which releases those locks
without assigning a commit timestamp.

Terminal user-session completion is structurally gated by
`ReleasedTransactionLocks`. Transaction code can mint this non-cloneable,
transaction-id-bound proof only after closing the transaction scope through
the engine lock manager reached from the retained terminal attachment. The
proof owns the same `Box<FamilyLockAuthority>` that entered the transaction.
`TrxAttachment::commit()` and `TrxAttachment::rollback()` consume and validate
the proof before direct state publication may reinstall the box in an open idle
session or drain session-explicit claims for a closed one. If terminal
publication closes a requested or abandoned session, `EngineCore` upgrades its
weak registry back-reference and removes only the pointer-identical registered
state.

For ordered commit, shared committed status is published before transaction
locks are released, and session completion follows lock release. For rollback
and readonly/no-op discard, rollback effects and required purge bookkeeping
precede lock release, and the rollback-style session transition follows it. If
the public session was abandoned, that final transition closes the session and
releases explicit session-owned locks only after transaction-owned locks are
gone. Attachmentless system transactions neither produce nor consume this
proof.

During engine shutdown, foreground admission closes first. Blocking shutdown
lazily traverses sessions and stops at the first active operation or standalone
observer. Within one session, an operation is reported before observers. It
installs or reuses only that session's event, registers a listener under the
lifecycle mutex, re-reads the selected blocker, then releases all registry and
state guards before queueing at most one exact cleanup hint and waiting. A
relevant exact-key transition or observer release notifies that session after
releasing lifecycle and explicit-lock ownership; shutdown then rescans for the
first current blocker. A full traversal is required only to prove that no
operation or observer remains.

Shutdown-discovered abandoned cleanup captures `SessionRuntime` from the
registered state before submission. The worker resolves the exact operation
directly on that state and never returns through the registry.

The nonblocking `try_shutdown()` uses the same first-blocker probe without
installing an event. Consequently an ordinary open-session statement does not
touch notification state, and an unobserved commit or rollback performs no
notifier atomic update, event allocation, or wake. The listener-before-release
protocol has no lost-wake interval, and `ShutdownBusy` remains observable while
any operation, observer, mandatory caller permit, or mandatory internal permit
remains.

Recovery only treats checkpoint metadata, table roots, and real redo headers as
stable timestamp carriers. A no-log ordered commit has a volatile CTS that is
valid only for the running process. Any effect that must be reconstructed after
restart must therefore emit a real redo record or marker instead of relying on
the ordered-only path.

#### System Transactions

`SysTrx` is the single sessionless transaction type for row-page creation and
user-table checkpoint completion. It has no STS, active-bucket registration,
session attachment, undo, locks, rollback, or durability waiter. Table
checkpoint allocates a separate non-active `checkpoint_ts` for construction and
root publication. `commit_sys` later assigns an ordered `redo_cts` and returns
after enqueue acceptance; that return does not acknowledge redo persistence.
Redo-only system work has no purge bucket. A nonempty data checkpoint owns one
compact `RetiredRowPageBatch` and derives its purge bucket deterministically
from the owning table id. All retirement batches for one table therefore enter
one bucket's committed FIFO in ordered CTS order, while user transactions keep
their existing round-robin bucket selection.

Prepared, precommit, and committed payloads distinguish `User` and `System`
variants. User payloads own status, STS, undo, and session completion. System
checkpoint payloads own only the table-affine purge shard and one ordered
retirement batch, and are valid only when coupled to recovery-visible
checkpoint redo. Failed system
precommit performs no rollback or active-STS removal.

#### Rollback Phase

- Row undo is rolled back in reverse order. `Insert` marks the hot row deleted,
  `Delete` clears the delete bit, `Update` restores before-image columns, and
  a pure `Lock` leaves row data unchanged.
- Index undo removes inserted claims, restores merged delete-masked claims, and
  unmasks deferred deletes so MemIndex returns to the pre-transaction state.
- Runtime unique-key branches are transaction-local MVCC aids. They are kept
  only while live snapshots may need the older owner. Their GC anchor is the
  same `Global_Min_STS` / oldest-active-snapshot horizon used by undo GC: they
  can be purged only after rollback/index-undo obligations are gone and
  `Global_Min_STS` proves no active snapshot can require the older owner. They
  are not purged merely because a row became cold, crossed `pivot_row_id`, or no
  longer appears in the deletion buffer.

The private shared transaction status has a sticky terminal-resolution event
used only for maintenance coordination. Commit publishes it after storing the
commit CTS. Successful active, abandoned, and failed-precommit rollback publish
it only after rollback-capable row/index undo, purge bookkeeping, locks, and
session cleanup have reached the safe reanalysis boundary. A rollback access
failure does not publish normal resolution; storage poison is the terminal wake
and ownership-retention boundary. The status event is not part of the public
transaction API and does not change MVCC visibility.

### Checkpoint and Persistence

####  Index Checkpoint

There is no independent MemIndex-scan index checkpoint.

Instead:

1. **Data Checkpoint Companion Work**:
   - when frozen RowStore pages are converted into persistent LWC blocks, the
     same committed rows are encoded into companion `DiskTree` updates
2. **Deletion Checkpoint Companion Work**:
   - when committed cold-row deletes are persisted into delete bitmaps, the same
     deleted rows drive companion `DiskTree` removals
3. **State Transition**:
   - after the related table checkpoint publishes its new roots, the
     corresponding `MemIndex` entries can become clean or evictable

### Heap Persistence

Heap persistence relies on the **Tuple Mover** and the durability of the commit log. Explicit flushing of raw RowPages to temporary files is **removed** in favor of relying on the commit log for recent data and the ColumnStore for archival data.

1. **Tuple Mover**:
   - Freezes a contiguous RowStore-page prefix and retains the returned batch
     across checkpoint retries.
   - Optimistically analyzes each frozen page without a page-state write lock,
     fusing image-cutoff proof, cutoff-visible deletion bitmap construction,
     and transition marker selection into one owned plan.
   - Rejects a plan immediately when the paired mutation version changes during
     analysis. Full-value equality is the only validity rule; counter parity
     does not indicate writer quiescence.
   - Reuses or rebuilds the plan under only that page's state write lock
     immediately before transition. Once the complete batch is image-ready,
     pages transition in canonical order as a growing `TRANSITION` prefix and
     still-mutable `FROZEN` suffix.
   - Converts them into **LWC** (lightweight compressed) ColumnStore blocks.
   - Uses the same prepared deletion bitmap for LWC membership, block-split
     retries, and companion secondary-index entries without another undo walk.
   - Updates the `Pivot_RowID` and persists metadata.
   - Publishes companion `DiskTree` updates for those checkpointed rows.
   - This is the primary mechanism for long-term heap storage and commit log
     truncation.
2. **Commit Log Reliance**:
   - Data in the active RowStore (not yet converted) is protected solely by the commit log.
   - To prevent infinite commit log growth, the Tuple Mover must run frequently enough to keep the "Active RowStore" size manageable.

### Crash Recovery

**Recovery** is simplified due to the **No-Steal** policy (no dirty data on disk) and Append-Only heap design.

1. **Load Metadata**: Read `Pivot_RowID`, `Heap_Redo_Start_TS`, persistent
   delete-checkpoint state, and the checkpointed `DiskTree` roots.
2. **Load ColumnStore**: Initialize access to persisted columnar data (`RowID < Pivot`).
3. **Load DiskTree**: Open the B+Tree at the last valid root.
4. **Replay Commit Log**:
   - **Heap Redo**: Start scanning from `Heap_Redo_Start_TS`. Reconstruct the in-memory RowStore pages by replaying insert/update logs.
   - **Deletion Buffer Redo**: Rebuild the **ColumnDeletionBuffer** from logs to restore post-checkpoint deletion states for columnar data.
   - **Secondary Index Rebuild**: After redo reaches log end, scan recovered
     hot RowStore pages to rebuild the corresponding `MemIndex` entries. No
     index-specific replay watermark is needed.
5. **Completion**: The system is open for service once memory structures are rebuilt. `DiskTree` already contains checkpointed cold index state and `MemIndex` contains post-checkpoint hot state.

### Garbage Collection (GC)

####  DiskTree GC (Merge On Write)

- **Mechanism**: Since the DiskTree uses Copy-on-Write, old pages become obsolete after a new root is created.
- **Integration**: GC is performed implicitly during the **Background Merge** process.
  - When the Checkpoint thread allocates new pages for the CoW update, it reclaims pages from the "Free List" populated by previous checkpoints.
  - Space from overwritten nodes is added to the Free List for future reuse.

#### ColumnStore GC (Compaction)

- **Trigger**: When the Delete Bitmap for a ColumnStore block shows a high deletion ratio (e.g., > 20% dead rows).
- **Mechanism**: A background **Compaction** task reads the live rows from the block (skipping deleted ones) and writes a new, dense ColumnStore block.
- **Metadata Update**: The Block Index is updated to point to the new block, and the old Delete Bitmap is cleared/reset.

#### Undo GC

- **Watermark**: `Global_Min_STS` (Start Timestamp of the oldest active transaction).
- **Action**: Background threads collect committed transaction contexts.
  - Any Undo versions in the transaction with `Commit_STS < Global_Min_STS` are obsolete (no active transaction can see them).
  - These records are unlinked and memory is reclaimed.

Checkpoint-retired row pages use the same committed-payload queues and CTS
horizon. A system payload has no STS to remove; its ordered CTS is its
reclamation fence. Each purge round finishes eligible row-undo and index work
from every bucket before the coordinator processes ordered retirement batches,
because undo in one bucket may still reference a page retired in another. For
each batch the coordinator pins the table runtime from either live or
retained-dropped catalog state, validates and unlinks the exact current
`RowPageIndex` prefix, reclaims detached metadata, then deallocates only the page
ids returned by that successful operation. The insert free list uses versioned
page identities and lazily rejects removed or recycled entries.
Same-table batches are processed sequentially in their table-affine bucket FIFO.

Removing a user STS records the bucket minimum before removal and the resulting
finite minimum or no-active outcome. Commit and rollback coalesce the minimum
original STS from those transitions, but scheduling still scans the
authoritative bucket atomics and timestamp upper bound. Active progress starts
an all-bucket horizon cycle only when the fresh horizon is newer than the last
completed horizon and the coalesced original STS is strictly below it. Progress
from a later bucket behind an older global blocker therefore remains queued
without launching an empty round; removal of the actual blocker later drains
all newly eligible bucket prefixes.

New system payloads independently coalesce their minimum CTS. A system payload
forces all-bucket transaction GC only when that CTS is strictly below the fresh
horizon. At an unchanged horizon this forced round does not implicitly repeat
retained-root or dropped-table processing and does not republish completed
progress. At a genuinely newer horizon it becomes a complete horizon cycle.
An explicit full observation likewise starts a complete cycle only for a newer
horizon, while still performing its explicitly requested housekeeping at an
unchanged horizon.

After bucket cleanup, retired-page processing, retained-root release, and
metadata-history work have completed, dropped-table GC applies the strict
`drop_cts < Global_Min_STS` boundary. It detaches the catalog-owned runtime in
one direction, asserts that the `Table` Arc is unique, and destroys it by
value. A failed uniqueness assertion is a holder-discipline bug; the runtime
is never restored or requeued. Table-file unlink remains a separate,
catalog-checkpoint-gated operation whose I/O failure is retryable.

Purge publishes two monotonic coordination boundaries in order. First it
publishes the freshly observed oldest-active-snapshot horizon, which is
sufficient for checkpoint cutoff and active-root readiness but does not prove
reclamation. Only after every operation selected by a complete horizon cycle
succeeds does it publish completed-purge progress. Idle-session waits expose
these as separate strict `> ts` predicates, request a lossy/idempotent purge
observation before sleeping, and terminate on storage poison or engine
shutdown. Reclamation tests for a no-wait checkpoint system CTS separately
observe its ordered redo-to-purge handoff before waiting for a later completed
purge cycle.

`gc_buckets` controls the runtime transaction-GC sharding width. The default is
32; valid values are powers of two from 1 through 256. User transactions use
round-robin assignment across the configured slice. System retirement payloads
do not persist a bucket identity: ordered commit handoff computes their
table-affine bucket from the table id and the current engine's bucket count.
The setting is immutable for one engine lifetime and may change across restart
without a persistent-format migration.

`purge_threads` is independent of `gc_buckets` and is the total number of
threads that execute GC-bucket work. One dispatcher always owns slot zero and
only `N - 1` executor threads are started. With `N = 1`, the dispatcher runs
every bucket locally without allocating task or completion channels. Otherwise,
bucket `gc_no` belongs to slot `gc_no % N`; all remote work is enqueued before
the dispatcher executes its local share. Retirement batches are merged in
bucket order only after every required slot succeeds, preserving the all-bucket
undo/index-before-page-deallocation barrier. Positive worker counts may exceed
the configured bucket count; excess executor slots remain idle and still join
normally at shutdown.

#### MemIndex Eviction

- **Condition**: the related table checkpoint has made the same state durable.
- **Action**: clean entries can be evicted from memory because the authoritative
  cold state is already available from checkpointed `DiskTree` roots or delete
  checkpoint state.

### Summary

The transaction system tries to achieve high throughput for HTAP workloads by adhering to a **Strict No-Steal / No-Force** policy combined with **Log-Structured** principles.

- **Write Optimization**: Foreground writes are completely in-memory (MemIndex + RowStore) with sequential commit logging. The overhead of transaction commit is reduced to *O(1)* by eliminating index traversal and global state contention.
- **Read Optimization**: The hybrid layout serves OLTP queries from the RowStore/MemIndex and OLAP scans from the high-density ColumnStore. The Index structure avoids the read amplification typical of LSM-trees by maintaining a compact B+Tree structure on disk.
- **Reliability**: Recovery is simplified to a Redo-only process. Checkpointed
  `DiskTree` roots provide cold secondary-index state directly, while redo
  reconstructs hot `MemIndex` state without needing an independent index replay
  watermark.
- **Scalability**: Table-level checkpoints and independent Tuple Movers allow the system to scale across many tables without "convoy effects", ensuring stable performance even under mixed workloads.
