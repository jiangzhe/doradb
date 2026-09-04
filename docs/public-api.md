# Doradb Storage Public API

`doradb-storage` is a low-level Rust storage-engine library. It exposes engine,
session, transaction, row, index, maintenance, and diagnostic APIs; it does not
provide SQL parsing, query planning, or a network protocol.

The crate is currently version `0.1.0` and is under active development. Public
interfaces may change between revisions, so applications should pin a known
Doradb commit.

For a complete runnable program, see
[`doradb-storage/examples/quick_start.rs`](../doradb-storage/examples/quick_start.rs).

## Platform and Cargo features

Doradb currently targets Linux and uses direct asynchronous IO. Exactly one
storage backend feature must be enabled:

| Feature | Status | Notes |
| --- | --- | --- |
| `iouring` | Default | Uses Linux `io_uring`. |
| `libaio` | Alternate | Intended for kernels or environments where `io_uring` is unavailable; development packages `libaio1` and `libaio-dev` are required. |

A path dependency uses the default `io_uring` backend:

```toml
[dependencies]
doradb-storage = { path = "../doradb-storage" }
```

Select `libaio` explicitly by disabling default features:

```toml
[dependencies]
doradb-storage = {
    path = "../doradb-storage",
    default-features = false,
    features = ["libaio"],
}
```

Public operations return standard Rust futures and do not require a particular
application async runtime. The engine also owns a small internal executor for
accepted DDL, maintenance, and transaction-cleanup obligations.

## Lifecycle at a glance

The main public capabilities form this lifecycle:

```text
Engine::bootstrap
  -> Engine::new_session
       -> Session DDL, maintenance, locks, and diagnostics
       -> Session::begin_trx
            -> Transaction reads and writes
            -> Transaction::commit or Transaction::rollback
       -> Session::close
  -> Engine::shutdown
```

- `Engine` owns one storage runtime and creates sessions.
- `Session` is a non-cloneable, mutable application context. Multiple sessions
  may be created from one engine for concurrent work.
- `Transaction` is a non-cloneable active transaction associated with exactly
  one session. A session admits only one effectful operation or transaction at
  a time.
- Direct transaction methods are statement boundaries. Each successful method
  merges its effects into the transaction; an ordinary method error rolls back
  that method's effects before returning.
- `IndexScanMvccStream` and `TableScanMvccStream` hold an exclusive borrow of
  their transaction until the stream is exhausted, fails, or is dropped.

DDL, effectful maintenance, session-lock mutation, and a new transaction
require an idle session. Read-only diagnostics and progress waits use observer
admission and can coexist with an active effectful operation where their Rust
receiver permits it.

The smallest complete lifecycle is:

```rust
use doradb_storage::{Engine, EngineConfig};

async fn run() -> doradb_storage::Result<()> {
    let engine = Engine::bootstrap(
        EngineConfig::default().storage_root("target/example-storage"),
    )
    .await?;
    let mut session = engine.new_session()?;

    let mut trx = session.begin_trx()?;
    trx.noop().await?;
    let _commit_ts = trx.commit().await?;

    session.close().await?;
    engine.shutdown();
    Ok(())
}
```

Prefer explicit transaction settlement, session close, and engine shutdown.
Drops provide cleanup fallbacks, but explicit terminal calls make errors and
blocking points visible to the application.

## Engine configuration and bootstrap

`EngineConfig::default()` provides a complete configuration. Its builder
methods consume and return the configuration, so settings can be chained.

```rust,ignore
use doradb_storage::{
    EngineConfig, FileSystemConfig, LogSync, MandatoryRuntimeConfig,
    TrxSysConfig,
};

let config = EngineConfig::default()
    .storage_root("/var/lib/doradb/app")
    .trx(TrxSysConfig::default().log_sync(LogSync::Fdatasync))
    .mandatory_runtime(
        MandatoryRuntimeConfig::default()
            .concurrency_limit(4),
    )
    .file(FileSystemConfig::default().io_depth(64));

let config = config.validate()?;
let engine = doradb_storage::Engine::bootstrap(config).await?;
```

The top-level configuration groups are:

| Configuration | Purpose |
| --- | --- |
| `EngineConfig` | Storage root and all component configuration. |
| `TrxSysConfig` | Redo, recovery, catalog-checkpoint scan, purge, and GC settings. |
| `MandatoryRuntimeConfig` | Accepted caller-operation limit for mandatory work. |
| `FileSystemConfig` | Table/catalog IO depth, data directory, readonly pool, catalog file name, and CoW file limit. |
| `EvictableBufferPoolConfig` | Swap path, file and memory limits, and eviction tuning for index or row-data pools. |
| `LogSync` | `None`, `Fsync`, or `Fdatasync` redo synchronization. |

All configured data, redo, and swap paths are relative to `storage_root`.
Bootstrap records the durable layout in that root. Reopening with an
incompatible durable path layout is rejected. Doradb also holds an operating
system lock for the root; a second live engine cannot bootstrap the same root.

`LogSync::None` skips an explicit redo sync and therefore does not provide the
same crash-durability guarantee as `Fsync` or `Fdatasync`.

## Schemas and indexes

Tables are defined with ordered columns and an ordered initial set of secondary
indexes:

```rust,ignore
use doradb_storage::{
    ColumnOrdinal, StorageColumnFlags, StorageColumnSpec, StorageIndexFlags,
    StorageIndexKey, StorageIndexSpec, StorageTableSpec, ValKind,
};

let table_spec = StorageTableSpec::new(vec![
    StorageColumnSpec::new(ValKind::I32, StorageColumnFlags::empty()),
    StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::NULLABLE),
]);

let index_specs = vec![
    StorageIndexSpec::new(vec![StorageIndexKey::new(0)], StorageIndexFlags::UK),
    StorageIndexSpec::new(vec![StorageIndexKey::new(1)], StorageIndexFlags::empty()),
];

let created = session.create_table(table_spec, index_specs).await?;
let table_id = created.table_id();
let id_index_id = created.index_ids()[0];
let name_index_id = created.index_ids()[1];
```

Column ordinals are zero-based and follow `StorageTableSpec::columns`. The
caller controls nullability with `StorageColumnFlags::NULLABLE`; index
membership is declared with `StorageIndexSpec`.

A `StorageIndexSpec` contains one or more `StorageIndexKey` values in logical
key order. `StorageIndexKey::new(column_ordinal)` creates an ascending key.
Construct the public fields directly to request `IndexOrder::Desc`:

```rust,ignore
let descending_name = StorageIndexKey {
    column_ordinal: ColumnOrdinal::new(1),
    order: IndexOrder::Desc,
};
```

`StorageIndexFlags::UK` creates a unique index; empty flags create a non-unique
index. `StorageIndexFlags::PK` exists for catalog metadata, but public user-table
`create_table` and `create_index` reject primary-key specifications. Use `UK`
when a public user table needs uniqueness.

`CreateTableOutcome::index_ids` returns finalized initial identities in the
same order as the supplied index definitions. `Session::create_index` returns a
stable table-local `IndexID`.
Index-driven transaction methods accept `TableIndex(table_id, index_id)`, and
Dropping an index does not renumber the remaining indexes.
The sealed `TableIndexArgument::into_selector` conversion normalizes a
`TableIndex` or `ResolvedTableIndex` into an opaque `TableIndexSelector`; its
public accessors expose only the stable table and index identities.

### Managed opaque definitions

Higher layers that own names or logical schema formats can implement
`ManagedTableInterpreter`, import the `ManagedTableOps` extension trait, and
call `create_managed_table`, `create_managed_index`, or `drop_managed_index`.
The engine passes arbitrary source bytes unchanged and never interprets the
descriptor or binding-key formats. The CREATE TABLE callback returns one
`ManagedCreateTableDefinition` containing the ID-free physical definition,
complete descriptor, and zero or more `TableBinding` values. Existing-table
callbacks return `DescriptorUpdate<C>`: one operation-specific, slot-free
physical change paired with the complete opaque replacement payload.

```rust
use doradb_storage::ManagedTableOps;
```

CREATE TABLE interpretation runs before any table ID is allocated and returns
an ID-free ordered `CreateTableDefinition` inside that bundle. DoraDB validates
the complete bundle and assigns table, column, and initial-index identities
afterward. The numeric schema, descriptor, and bindings commit atomically.
Existing-table callbacks receive the
previous descriptor and a separate `StorageTableDefinition` whose columns are
in physical order and indexes are in stable-ID order. CREATE INDEX also
receives the engine-proposed next `IndexID`; physical `IndexSlot` values and all
version/lock/transaction state remain private.

Callbacks are synchronous and run after the engine has released its short
metadata-S preflight. DoraDB reacquires DDL exclusion and privately revalidates
the definition before effects. A concurrent schema change returns
`ManagedDdlError::Engine` containing `OperationError::SchemaChanged`; the
interpreter is called exactly once per API call, and the caller chooses whether
to retry the whole operation. Interpreter failures remain in
`ManagedDdlError::Interpreter` without being flattened into storage errors.

Descriptor payloads may be empty or arbitrary binary bytes and are persisted
byte-for-byte. The inclusive maximum is
`MAX_TABLE_DESCRIPTOR_BYTES` (64,000 bytes). Unmanaged numeric index DDL is
rejected for a managed table, while DROP TABLE automatically deletes any
descriptor and all of its bindings.

`BindingNamespaceID` and `TableBinding` carry roleless opaque names. Empty and
arbitrary binary keys are valid through the inclusive
`MAX_TABLE_BINDING_KEY_BYTES` limit of 16,000 bytes. A key is unique only
within its namespace, and bindings can be created only as part of managed
CREATE TABLE. A binding already present at precheck or insertion is reported as
`OperationError::DuplicateKey`; a concurrent binding ownership or deletion
race may instead report `OperationError::WriteConflict`. Both are returned in
the engine arm of `ManagedDdlError`.

`resolve_table_binding(namespace_id, key, include_full_schema)` returns `None`
when the exact key is absent. A successful `ResolvedTableBinding` always
contains the assigned table ID and an opaque, equality-comparable
`TableDefinitionVersion`. With `include_full_schema == true`, it also contains
one coherent `ManagedTableDefinitionSnapshot` holding the stable-ID numeric
schema and exact descriptor bytes. The false path performs only binding and
constant-size runtime validation; it does not load central numeric metadata or
copy the full schema or descriptor. `list_table_bindings(table_id)` returns the
table's bindings sorted by namespace and key, or `OperationError::TableNotFound`
when the target table does not exist or has already been dropped.

Resolution is coherent only at the admitted point inside the call. No returned
value retains a metadata lock. Comparing a cached version with a later narrow
resolution tells the caller whether the definition changed between those two
points, but does not guard later planning or execution.

## Values and identifiers

`ValKind` describes schema types, while `Val` carries row and key values:

| `ValKind` | `Val` payload |
| --- | --- |
| `I8`, `I16`, `I32`, `I64` | Signed integer of the matching width. |
| `U8`, `U16`, `U32`, `U64` | Unsigned integer of the matching width. |
| `F32`, `F64` | Ordered floating-point value. |
| `VarByte` | Owned variable-length bytes in `MemVar`. |
| — | `Val::Null`, valid only for nullable columns. |

Primitive numbers, byte slices, byte vectors, and strings implement `From` for
`Val`:

```rust,ignore
let row = vec![Val::from(42i32), Val::from("alice")];
let bytes = Val::from(&b"opaque"[..]);
let null = Val::Null;
```

Use typed accessors such as `as_i32`, `as_u64`, `as_f64`, and `as_bytes` when
decoding projections. `as_str` is appropriate only when the `VarByte` payload
is known to contain valid UTF-8. `ValType` pairs a `ValKind` with nullability;
`MemVar` is the lower-level owned representation behind `Val::VarByte`.

Identifiers are strong `u64`-backed newtypes in `doradb_storage::id`:

| Identifier | Meaning |
| --- | --- |
| `TableID` | Stable logical user-table identity. |
| `RowID` | Stable identity of a physical row version. |
| `TrxID` | Transaction identity or transaction-system timestamp. |
| `SessionID` | Engine-local session identity. |
| `PageID` | Runtime buffer-managed page identity exposed by maintenance outcomes. |

Do not mix raw numbers from different identifier domains. Their `new` and
`as_u64` methods are available when an external representation is required.

## Sessions and DDL

Create sessions with `Engine::new_session`. `Session::id` returns its
engine-local identifier, and `Session::list_table_ids` returns sorted IDs for
currently loaded user-table runtimes.

The effectful DDL methods require an idle mutable session. Managed methods are
provided by `ManagedTableOps` and require that trait in scope:

| Method | Effect |
| --- | --- |
| `create_table` | Validate numeric metadata and return a `CreateTableOutcome`. |
| `create_managed_table` | Interpret opaque bytes, atomically create numeric metadata, a descriptor, and bindings, and return a `CreateTableOutcome`. |
| `resolve_table_binding` | Resolve one opaque managed name, always returning a definition version and optionally a full definition snapshot. |
| `list_table_bindings` | Enumerate one managed table's roleless bindings in deterministic order. |
| `create_index` | Build and publish a secondary index, returning its stable `IndexID`. |
| `create_managed_index` | Interpret one managed index addition and atomically replace its descriptor. |
| `drop_index` | Logically remove an active secondary index. |
| `drop_managed_index` | Interpret one managed index removal and atomically replace its descriptor. |
| `drop_table` | Logically remove a user table and schedule safe physical reclamation. |

DDL is executed as accepted mandatory work. Before acceptance, cancellation
releases caller-owned preparation. Once accepted, the engine supervises the
operation to a terminal outcome even if the caller drops its result future.

An explicit session table lock on the DDL target is not a substitute for DDL's
metadata lock set; release the explicit lock before running index DDL on that
table.

Call `Session::close` after its transaction and effectful operations have
finished. Close is idempotent. Dropping an open session requests abandonment
cleanup and releases session-lifetime resources, but cannot report cleanup
failures to the caller.

## Transaction lifecycle and semantics

Start a transaction with `Session::begin_trx`. Only one transaction or other
effectful operation may occupy a session at a time. `Transaction::trx_id`
returns the transaction identity and `Transaction::sts` returns its MVCC start
timestamp.

Each direct non-streaming operation is one engine-controlled statement:

- Successful row, index, undo, and redo effects merge into the transaction.
- An ordinary operation or caller callback error rolls back the current
  statement before the error is returned. Earlier successful statements remain
  part of the transaction, which can still be used or explicitly rolled back.
- A rollback failure is fatal and poisons future storage admission.
- Read and write methods acquire transaction-lifetime metadata and data locks as
  needed. Those locks remain until commit or rollback.

`commit(self)` and `rollback(self)` consume the transaction. Commit returns its
commit timestamp. Dropping a live transaction does not roll it back inline; it
queues engine-owned cleanup. Explicit settlement is preferred because callers
can await and observe the result.

Dropping an in-flight direct-operation future after it has checked out the
transaction transfers the whole transaction to cleanup; the stale transaction
handle will no longer be reusable. Avoid cancellation by timeout or `select`
when later reuse of that transaction is required. Dropping an index or table
scan stream merely closes the stream and releases its exclusive transaction
borrow.

### DML validation

DML validation is enabled by default. Depending on the operation, it checks:

- complete row shape, value kind, and nullability;
- active index identity and complete key shape;
- range-bound key shape;
- non-empty, in-range, strictly increasing read sets; and
- in-range, strictly increasing sparse update columns.

Input rejected by these checks returns `OperationError::InvalidDmlInput`
without poisoning the engine. Full-table scan projections are validated before
the stream is constructed.

`Transaction::disable_dml_validation(true)` disables these caller-input checks
for subsequent direct and streaming operations in that transaction. Use it
only when inputs have already been proven against the bound table metadata.
Invalid trusted input may then produce a debug assertion or internal error
instead of `InvalidDmlInput`. Calling it with `false` restores validation.
Table admission, schema-change checks, locking, MVCC, uniqueness, and storage
invariants are never disabled.

## Reading data

Read sets are projections expressed as zero-based column numbers. Index-read
and full-table scan projections must be non-empty, in range, and strictly
increasing. Returned `Vec<Val>` values follow read-set order.

### Full-table scan

`table_scan_mvcc_stream` is the full-table MVCC scan API. It supports
incremental consumption and a programmable filter. The callback receives a
snapshot-visible `LazyRow`, so it can inspect a column omitted from the output
projection:

```rust,ignore
let mut stream = trx
    .table_scan_mvcc_stream(table_id, &[0, 2], |row| {
        if row.val(1)? == &Val::from("active") {
            Ok(ScanRowDecision::Include)
        } else {
            Ok(ScanRowDecision::Skip)
        }
    })
    .await?;

while let Some(vals) = stream.next().await? {
    consume(vals); // columns 0 and 2 only
}
drop(stream);
```

`Include` materializes the supplied read set and returns at most one row from
the current `next` call. `Skip` continues internally without materializing the
projection. `Stop` excludes the current row and closes the stream successfully.
Exhaustion, `Stop`, callback or storage error, and early drop release the
stream's operation checkout. Cancelling a pending stream constructor also
returns its checkout and leaves the transaction reusable; transaction claims
already accepted during admission remain until commit or rollback. After a
terminal result, later `next` calls return `Ok(None)` without invoking the
callback.

The stream exclusively borrows its transaction until it is dropped. The
callback is synchronous and cannot retain the lazy row or a value borrowed from
it. Row-level access ends before `next` returns, but the stream retains one
shared guard for its current hot page across included projections. Ordinary
row updates use compatible shared page access. An operation requiring that
page latch exclusively, such as eviction or physical deallocation, may wait
until the stream advances or closes. A caller paused mid-page must not wait for
external work that requires the same page's exclusive latch.

### Shared-snapshot partition scans

Use a shared read snapshot when several tables or independently scheduled scan
partitions must observe one ownerless MVCC timestamp. The one-shot builder
acquires the complete table set before the snapshot becomes shareable:

```rust,ignore
let snapshot = session
    .begin_read_snapshot()?
    .acquire_tables([customer_table, order_table])
    .await?;

let mut plan = snapshot
    .prepare_table_scan(
        order_table,
        TableScanOptions {
            projection: vec![0, 2],
        },
    )
    .await?;
if let Some(repartitioned) =
    plan.repartition(NonZeroUsize::new(worker_count).unwrap())?
{
    plan = repartitioned;
}

let mut tasks = Vec::with_capacity(plan.partition_count());
for partition_idx in 0..plan.partition_count() {
    let mut stream = plan.open(partition_idx)?;
    tasks.push(smol::spawn(async move {
        let mut rows = Vec::new();
        while let Some(row) = stream.next().await? {
            rows.push(row);
        }
        Ok::<_, doradb_storage::Error>(rows)
    }));
}
drop(plan);
let (results, close) =
    futures::join!(futures::future::join_all(tasks), snapshot.close());
close?;
consume_partition_results(results)?;
```

`ReadSnapshot` covers every acquired table with one `sts()`. Planning validates
a nonempty, in-range, strictly increasing projection. Initial and explicit
repartitioning preserve physical cold-before-hot order and split only between
captured blocks/pages. Concatenating fully drained partition results in
partition-index order reproduces sequential physical scan order; concurrently
delivered rows have no global ordering guarantee.

`TableScanPlan::open` is synchronous, repeatable for a current generation, and
returns a fully owned `TableScanPartitionStream: Send + 'static`. Each
`next()` future is `Send`, so callers may move complete drains into executor
tasks. Opening the same partition twice intentionally returns the same logical
rows twice. An out-of-range index returns
`OperationError::InvalidTableScanInput`; a changed repartition permanently
stales older plan clones, and the first successful open prevents later
repartitioning of that plan family.

Normal stream exhaustion releases only that stream's execution checkout. A
ready snapshot with zero active streams remains reusable for later plans and
repeatable opens while retaining its STS and metadata locks. Poll
`ReadSnapshot::close` after every intended open to seal new admission and wait
for accepted streams to return their checkouts. Explicit close is group-wide;
final-facade drop, session close/abandonment, shutdown, or first execution
failure also seals the snapshot. An already opened stream owns no facade or
plan borrow and can finish after those public values are dropped.

The first partition execution error is returned unchanged and fails the whole
multi-table snapshot. Sibling streams return
`OperationError::SnapshotScanAborted` when they next reach a physical-unit
boundary. A peer can still return rows remaining in its currently loaded
block/page, but it never starts another unit after observing failure. Partial
results are therefore a caller policy. Each stream retains at most one unit;
pausing mid-hot-page also retains that page's shared guard and can delay work
requiring its exclusive latch.

### Unique lookup

`table_lookup_unique_mvcc` requires a unique index and returns `SelectMvcc`:

```rust,ignore
let key = [Val::from(42i32)];
match trx
    .table_lookup_unique_mvcc(TableIndex(table_id, IndexID::new(0)), &key, &[0, 1])
    .await?
{
    SelectMvcc::Found(vals) => consume(vals),
    SelectMvcc::NotFound => {}
}
```

Prefer matching the enum when absence is expected. `unwrap_found` panics for
`NotFound`.

For repeated operations, `resolve_table_index(TableIndex(table_id, index_id))`
returns an opaque non-pinning `ResolvedTableIndex`. The token is `Copy` and can
be passed to the same lookup, scan, stream, mutation, upsert, update, and delete
methods in place of `TableIndex`. Admission revalidates its exact generation
directly without an ID-map lookup. Tokens may cross transaction boundaries; a
dropped or replaced generation returns `SchemaChanged` during admission.

### Exact and range index reads

`table_index_lookup_mvcc` performs an exact non-unique secondary-index lookup
and returns `ScanMvcc::Rows`, because one key may select multiple rows. Use
`table_lookup_unique_mvcc` for a unique index.

`table_index_scan_mvcc` materializes all visible projections in a logical key
range. Bounds contain a complete index key in the same order as the
`StorageIndexSpec`; partial-prefix bounds are not accepted.

```rust,ignore
let lower = [Val::from("a")];
let upper = [Val::from("z")];
let rows = trx
    .table_index_scan_mvcc(
        TableIndex(table_id, name_index_id),
        &lower[..]..=&upper[..],
        &[0, 1],
    )
    .await?
    .unwrap_rows();
```

Rust's `RangeBounds` syntax controls inclusive, exclusive, and unbounded ends.

### Caller-driven index stream

Use `table_index_scan_mvcc_stream` when rows should be consumed incrementally:

```rust,ignore
let key = [Val::from("alice")];
let mut stream = trx
    .table_index_scan_mvcc_stream(
        TableIndex(table_id, name_index_id),
        &key[..]..=&key[..],
        &[0, 1],
    )
    .await?;

while let Some(vals) = stream.next().await? {
    consume(vals);
}
drop(stream); // releases the exclusive borrow of `trx`
```

`next` returns `Ok(None)` after exhaustion and remains exhausted on later
calls. An error also closes the stream. The transaction cannot be used while
the stream value is alive.

## Writing data

### Insert and batch insert

`table_insert_mvcc` accepts one complete row and returns its `RowID`.

`table_insert_batch_mvcc` inserts one table's rows atomically as a single
statement. It validates the complete input before physical insertion, preserves
input order in the returned `Vec<RowID>`, and rolls back any inserted prefix
before returning an ordinary error. An empty batch is valid and returns an
empty vector while still performing normal table admission and locking.

```rust,ignore
let row_ids = trx
    .table_insert_batch_mvcc(
        table_id,
        vec![
            vec![Val::from(1i32), Val::from("alice")],
            vec![Val::from(2i32), Val::from("bob")],
        ],
    )
    .await?;
```

### Unique-key upsert, update, and delete

These methods select one logical row through a unique index:

| Method | Input | Result |
| --- | --- | --- |
| `table_upsert_unique_mvcc` | Unique `TableIndex` or `ResolvedTableIndex` and a complete replacement row. | `UpsertMvcc::Inserted(RowID)` or `Updated(RowID)`. |
| `table_update_unique_mvcc` | Unique key and strictly ordered sparse `UpdateCol` values. | `UpdateMvcc::Updated(RowID)` or `NotFound`. |
| `table_delete_unique_mvcc` | Unique key. | `DeleteMvcc::Deleted` or `NotFound`. |

```rust,ignore
let key = [Val::from(1i32)];
let outcome = trx
    .table_update_unique_mvcc(
        TableIndex(table_id, id_index_id),
        &key,
        vec![UpdateCol {
            idx: 1,
            val: Val::from("ada"),
        }],
    )
    .await?;

assert!(outcome.is_updated());
```

Sparse updates must contain each target column at most once and in increasing
column-number order. An empty sparse update is a valid no-op update when its
target row exists.

### Callback-selected table mutation

`table_mutate_mvcc` traverses the latest modifiable rows of a table.
`table_index_mutate_mvcc` performs the same decision process for rows selected
through one secondary-index range. Their callback receives `LazyRow`, which
loads and caches requested columns on demand, and returns one `RowMutation`:

- `Skip` leaves the row unchanged;
- `Delete` deletes the row; or
- `Update(Vec<UpdateCol>)` applies a sparse update.

```rust,ignore
let outcome = trx
    .table_mutate_mvcc(table_id, |row| {
        if row.val(0)?.as_i32() == Some(7) {
            return Ok(RowMutation::Delete);
        }
        Ok(RowMutation::Skip)
    })
    .await?;

assert_eq!(outcome.delete_count, 1);
```

`TableMutationOutcome` counts selected deletes and updates; skipped rows are not
counted. An empty `Update` is counted as an update but creates no row, index,
undo, or redo work. A failed operation returns no outcome and rolls back all
effects selected by that callback invocation sequence.

The callback is synchronous, is invoked at most once for each eligible
original row, and must not depend on a stable physical traversal order.
Replacement rows created by the operation are not offered again. Index-driven
mutation uses weak-monotonic candidate traversal rather than a fixed
statement-start result set.

Updates may change the key of the unique index driving an index mutation. Such
changes are applied after candidate traversal to avoid hiding unread
candidates. The deferred list is not size-bounded, and uniqueness is checked as
updates are applied. Whole-statement unique-key swaps or cycles may therefore
return `DuplicateKey` and roll back rather than being planned as a permutation.

## Explicit table locks

Normal DDL and DML acquire their required logical locks automatically. The
explicit APIs are for applications that need a wider whole-table critical
section.

`TableLockMode` provides `Shared` and `Exclusive` modes:

- `Session::lock_table` acquires a session-lifetime lock while the session is
  idle. It remains across transactions and operations until
  `Session::unlock_table`, session close, or abandonment.
- `Transaction::lock_table` acquires a transaction-lifetime lock. It is
  released by commit, rollback, or transaction cleanup; there is no separate
  transaction unlock method.

Lock acquisition is async and may wait. A conversion that cannot be safely
waited may return `LockUpgradeWouldBlock`, and unsupported or conflicting
conversions have their own `OperationError` classifications. Keep explicit
lock order simple and let the storage API acquire ordinary locks when possible.

## Maintenance

Effectful maintenance methods use an idle mutable session. Checkpoint retry and
purge-progress waits are observer operations and can coexist with another
effectful operation. Accepted effectful work is supervised by the engine's
mandatory runtime.

### Table freeze and checkpoint

`freeze_table(table_id, max_rows)` freezes a bounded hot-row-page prefix and
returns `FreezeOutcome`:

- `Frozen` contains the newly installed `FrozenPageBatchInfo`;
- `AlreadyFrozen` reports the table's existing canonical batch; or
- `Cancelled` carries a normal `CheckpointCancelReason`.

`checkpoint_table` attempts to persist eligible table state. Its
`CheckpointOutcome` is `Published`, `Delayed`, or `Cancelled`. A delayed result
contains a self-identifying `CheckpointDelayReason`.

Use `wait_for_checkpoint_retry(reason)` to wait until retrying that exact delay
may be useful. A later attempt can encounter a different reason. The convenience
method `checkpoint_table_with_wait` repeats this process for normal delays and
returns only a published or cancelled outcome.

For workflow and timestamp details, see [Checkpoint](checkpoint.md) and
[Data Checkpoint](data-checkpoint.md).

### Catalog and redo maintenance

| Method | Result |
| --- | --- |
| `checkpoint_catalog` | Publishes eligible catalog state and returns `()`. |
| `truncate_redo_log` | Returns `RedoTruncationOutcome`, including marker movement, removal counts, failures, and current blockers. |
| `checkpoint_catalog_and_truncate_redo_log` | Coordinates both operations and returns `CatalogRedoMaintenanceOutcome`. |

`RedoTruncationBlockerInfo` identifies catalog, live-table, pending-drop, and
unsealed-file retention floors. An unlink failure remains retryable and is
reported in the outcome.

### Purge and index cleanup

- `wait_for_gc_horizon_after(ts)` waits for the published active-snapshot
  horizon to become newer than `ts`.
- `wait_for_purge_completion_after(ts)` waits for completed physical purge-cycle
  progress to become newer than `ts`.
- `cleanup_secondary_mem_indexes(table_id, clean_live_entries)` returns
  `MemIndexCleanupOutcome`. Its `MemIndexCleanupStats` contains one
  `SecondaryMemIndexCleanupIndexStats` per active index. Delete-overlay cleanup
  can succeed while `MemIndexCleanupDelay` in `live_delay` reports that
  live-entry cleanup must be retried later.
- `total_row_pages(table_id)` returns the current number of hot row pages for a
  user table.

The GC horizon is a readiness boundary; it does not mean all corresponding
physical purge work is complete. Use the purge-completion wait when physical
cleanup completion matters.

## Diagnostics and statistics

Sessions expose point-in-time or cumulative snapshots:

| Method | Snapshot |
| --- | --- |
| `transaction_system_stats` | `TransactionSystemStats`: commit, redo, sync, and purge counters. |
| `storage_io_stats` | `StorageIoStats` and `IoBackendStats`: shared scheduling and backend IO counters. |
| `buffer_pool_stats` | `BufferPoolStats`, `BufferPoolRuntimeStats`, and `BufferPoolCounters`: capacity, allocation, hit/miss, and IO lifecycle counters. |
| `mandatory_runtime_stats` | `MandatoryRuntimeStats` and `MandatoryTaskStats`: submitted, active, completed, error, panic, and timing counters by task class. |
| `logical_lock_stats` | `LogicalLockStats`: logical-lock work and current/peak physical lock state. |

Monotonic counters can be differenced by the caller. Fields sampled during
concurrent work are not necessarily one atomic global snapshot.

These diagnostic methods and `list_table_ids` deliberately remain observable
after storage poison while the engine lifecycle is still running. They are not
available after session close, registry removal, or engine shutdown.

## Error handling

Public fallible methods return `doradb_storage::Result<T>`, an alias for
`Result<T, doradb_storage::Error>`.

`ErrorKind` supplies the stable outer classification:

| Kind | Meaning |
| --- | --- |
| `Config` | Invalid static or startup configuration. |
| `Operation` | A logical request cannot complete, such as a duplicate key or write conflict. |
| `Resource` | Memory, buffer, or storage capacity is exhausted. |
| `Io` | An operating-system or async IO boundary failed. |
| `DataIntegrity` | Persisted bytes or recovery invariants are invalid. |
| `Lifecycle` | Shutdown, closed-session, active-operation, or discarded-transaction state rejected work. |
| `Runtime` | A recoverable engine-owned operation failed. |
| `Fatal` | Continued admission is unsafe and the engine is poisoned. |

Logical operation errors have a second typed classification in the
non-exhaustive `OperationError` enum. Inspect it with
`Error::operation_error`:

```rust,ignore
match trx.table_insert_mvcc(table_id, row).await {
    Ok(row_id) => consume(row_id),
    Err(err)
        if err.operation_error() == Some(OperationError::DuplicateKey) =>
    {
        handle_duplicate();
    }
    Err(err) => return Err(err),
}
```

Other common `OperationError` values include `TableNotFound`, `TableDropping`,
`SchemaChanged`, `IndexNotFound`, `WriteConflict`, `InvalidDmlInput`, and the
explicit-lock errors. Shared snapshots additionally use
`InvalidReadSnapshotInput`, `TableNotAcquired`, `InvalidTableScanInput`,
`StaleTableScanPlan`, `TableScanAlreadyOpened`, and `SnapshotScanAborted`.
Because the enum is non-exhaustive, downstream matches must include a fallback.

`Error::report` borrows the underlying `error_stack::Report<ErrorKind>`, and
`into_report` consumes the error. The report retains lower typed frames and
diagnostic attachments such as operation, phase, path, table ID, or batch
index. Use `Display` for a concise chain and `Debug` or the report API for
detailed diagnostics.

Many expected data outcomes are values rather than errors: `NotFound`, empty
scan rows, checkpoint delay/cancellation, and redo-retention blockers should be
handled through their result enums.

A fatal error poisons normal future admission. It does not eliminate the
owner's responsibility to settle transactions where possible and shut down the
engine. For the complete model, see [Storage Error Model](error-spec.md) and
[Shutdown and Engine Poison](shutdown-and-poison.md).

## Shutdown

`Engine::shutdown()` is synchronous, blocking, and idempotent. It closes new
admission, waits for accepted foreground operations, transaction cleanup,
mandatory work, and observers to drain, then stops components in dependency
order. It has no timeout.

`Engine::try_shutdown()` starts the same irreversible transition but performs
only one blocker probe. If it returns `ErrorKind::Lifecycle` with shutdown-busy
context, the engine remains in `ShuttingDown`; it does not reopen for new work.
Call `try_shutdown` again later or finish with blocking `shutdown`.

Existing transactions retain terminal authority during shutdown and may commit
or roll back, but new sessions and non-terminal operations are rejected.

Dropping `Engine` invokes blocking shutdown. Applications should therefore:

1. stop submitting new work;
2. exhaust or drop streams and operation futures;
3. commit or roll back transactions;
4. close sessions; and
5. call `shutdown` explicitly at a controlled blocking point.

## Public API map

Most application-facing types are re-exported from the crate root:

| Area | Primary types |
| --- | --- |
| Lifecycle | `Engine`, `Session`, `Transaction`, `ReadSnapshotBuilder`, `ReadSnapshot`, `IndexScanMvccStream`, `TableScanMvccStream`, `TableScanPartitionStream` |
| Configuration | `EngineConfig`, `TableScanConfig`, `TrxSysConfig`, `MandatoryRuntimeConfig`, `FileSystemConfig`, `EvictableBufferPoolConfig`, `LogSync`, `DEFAULT_COW_FILE_MAX_SIZE` |
| Schema | `StorageTableSpec`, `StorageColumnSpec`, `StorageColumnFlags`, `StorageIndexSpec`, `StorageIndexKey`, `ColumnID`, `ColumnOrdinal`, `IndexOrder`, `StorageIndexFlags`, `CreateTableOutcome`, `IndexID`, `TableIndex`, `ResolvedTableIndex`, `TableIndexSelector`, `TableIndexArgument` |
| Values | `Val`, `ValKind`, `ValType`, `MemVar` |
| Reads | `SelectMvcc`, `ScanMvcc`, `LazyRow`, `ScanRowDecision`, `TableScanOptions`, `TableScanPlan` |
| Writes | `UpdateCol`, `UpdateMvcc`, `UpsertMvcc`, `DeleteMvcc`, `RowMutation`, `TableMutationOutcome` |
| Locks | `TableLockMode` |
| Maintenance | `FreezeOutcome`, `FrozenPageBatchInfo`, `CheckpointOutcome`, `CheckpointDelayReason`, `CheckpointCancelReason`, `CatalogCheckpointOutcome`, `RedoTruncationOutcome`, `RedoTruncationBlockerInfo`, `CatalogRedoMaintenanceOutcome`, `MemIndexCleanupOutcome`, `MemIndexCleanupStats`, `MemIndexCleanupDelay`, `SecondaryMemIndexCleanupIndexStats` |
| Errors | `Result`, `Error`, `ErrorKind`, `OperationError` |
| Diagnostics | `BufferPoolStats`, `BufferPoolRuntimeStats`, `BufferPoolCounters`, `StorageIoStats`, `IoBackendStats`, `TransactionSystemStats`, `MandatoryRuntimeStats`, `MandatoryTaskStats`, `LogicalLockStats` |

The public modules provide the same domains with additional specialized items:

- `doradb_storage::id` contains public identifier newtypes;
- `doradb_storage::conf` contains configuration types and default constants;
- `doradb_storage::stats` contains public statistics snapshots; and
- `doradb_storage::error` contains the public error surface and specialized
  diagnostic/support types.

Prefer root re-exports for ordinary application code and the public modules
when a specialized constant or identifier is needed.

## Further reading

- [Storage Architecture](architecture.md)
- [Transaction System](transaction-system.md)
- [Lock System](lock-system.md)
- [Secondary Index Design](secondary-index.md)
- [Checkpoint](checkpoint.md)
- [Recovery](recovery.md)
- [Storage Error Model](error-spec.md)
- [Shutdown and Engine Poison](shutdown-and-poison.md)
