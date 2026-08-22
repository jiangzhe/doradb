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
- `IndexScanMvccStream` holds an exclusive borrow of its transaction until the
  stream is exhausted, fails, or is dropped.

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
    ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec,
    TableSpec, ValKind,
};

let table_spec = TableSpec::new(vec![
    ColumnSpec::new("id", ValKind::I32, ColumnAttributes::empty()),
    ColumnSpec::new("name", ValKind::VarByte, ColumnAttributes::NULLABLE),
]);

let index_specs = vec![
    IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::UK),
    IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::empty()),
];

let table_id = session.create_table(table_spec, index_specs).await?;
```

Column numbers are zero-based and follow `TableSpec::columns`. The caller
controls nullability with `ColumnAttributes::NULLABLE`; index membership is
declared with `IndexSpec`, not by setting `ColumnAttributes::INDEX`.

An `IndexSpec` contains one or more `IndexKey` values in logical key order.
`IndexKey::new(column_no)` creates an ascending key. Construct the public
fields directly to request `IndexOrder::Desc`:

```rust,ignore
let descending_name = IndexKey {
    col_no: 1,
    order: IndexOrder::Desc,
};
```

`IndexAttributes::UK` creates a unique index; empty attributes create a
non-unique index. `IndexAttributes::PK` exists in catalog metadata, but public
user-table `create_table` and `create_index` currently reject primary-key
specifications. Use `UK` when a public user table needs uniqueness.

Initial index numbers are allocated from zero in the order supplied to
`create_table`. `Session::create_index` returns a stable table-local `IndexNo`
(`u16`). Transaction methods accept the same number as `usize`, so dynamically
created indexes are normally passed as `usize::from(index_no)`. Dropping an
index does not renumber the remaining indexes.

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

The effectful DDL methods require an idle mutable session:

| Method | Effect |
| --- | --- |
| `create_table` | Validate metadata, create the table, and return its `TableID`. |
| `create_index` | Build and publish a secondary index, returning its stable `IndexNo`. |
| `drop_index` | Logically remove an active secondary index. |
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
when later reuse of that transaction is required. Dropping an
`IndexScanMvccStream` merely closes the stream and releases its exclusive
transaction borrow.

### DML validation

DML validation is enabled by default. Depending on the operation, it checks:

- complete row shape, value kind, and nullability;
- active index identity and complete key shape;
- range-bound key shape;
- non-empty, in-range, strictly increasing read sets; and
- in-range, strictly increasing sparse update columns.

Input rejected by these checks returns `OperationError::InvalidDmlInput`
without poisoning the engine. Callers must always supply valid column numbers;
full-table scan projections are consumed directly by the row readers rather
than by the index-read validator.

`Transaction::disable_dml_validation(true)` disables these caller-input checks
for subsequent direct and streaming operations in that transaction. Use it
only when inputs have already been proven against the bound table metadata.
Invalid trusted input may then produce a debug assertion or internal error
instead of `InvalidDmlInput`. Calling it with `false` restores validation.
Table admission, schema-change checks, locking, MVCC, uniqueness, and storage
invariants are never disabled.

## Reading data

Read sets are projections expressed as zero-based column numbers. Index-read
projections must be non-empty, in range, and strictly increasing. Full-table
scan projections must contain valid column numbers. Returned `Vec<Val>` values
follow read-set order.

### Full-table scan

`table_scan_mvcc` invokes a synchronous callback for each visible projection.
Return `true` to continue or `false` to stop successfully.

```rust,ignore
let mut rows = Vec::new();
trx.table_scan_mvcc(table_id, &[0, 1], |vals| {
    rows.push(vals);
    true
})
.await?;
```

The callback cannot be async. Effects outside Doradb performed by a callback
are not reversible if later application code rolls back the transaction.

Use `table_scan_mvcc_stream` for incremental full-table consumption with a
programmable filter. The callback receives a snapshot-visible `LazyRow`, so it
can inspect a column omitted from the output projection:

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
Exhaustion, `Stop`, callback or storage error, early drop, and constructor
cancellation all release the stream's operation checkout. After a terminal
result, later `next` calls return `Ok(None)` without invoking the callback.

The stream exclusively borrows its transaction until it is dropped. The
callback is synchronous and cannot retain the lazy row or a value borrowed from
it. For a hot row, callback code runs while that row's read guard is held; keep
the callback finite and do not wait for external work that may require a
conflicting write. No hot row-page or row guard remains held while the caller
consumes a returned projection.

### Unique lookup

`table_lookup_unique_mvcc` requires a unique index and returns `SelectMvcc`:

```rust,ignore
let key = SelectKey::new(0, vec![Val::from(42i32)]);
match trx
    .table_lookup_unique_mvcc(table_id, key.index_no, &key.vals, &[0, 1])
    .await?
{
    SelectMvcc::Found(vals) => consume(vals),
    SelectMvcc::NotFound => {}
}
```

Prefer matching the enum when absence is expected. `unwrap_found` panics for
`NotFound`.

### Exact and range index reads

`table_index_lookup_mvcc` performs an exact non-unique secondary-index lookup
and returns `ScanMvcc::Rows`, because one key may select multiple rows. Use
`table_lookup_unique_mvcc` for a unique index.

`table_index_scan_mvcc` materializes all visible projections in a logical key
range. Bounds contain a complete index key in the same order as the
`IndexSpec`; partial-prefix bounds are not accepted.

```rust,ignore
let lower = [Val::from("a")];
let upper = [Val::from("z")];
let rows = trx
    .table_index_scan_mvcc(
        table_id,
        name_index_no,
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
        table_id,
        name_index_no,
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
| `table_upsert_unique_mvcc` | Unique index number and a complete replacement row. | `UpsertMvcc::Inserted(RowID)` or `Updated(RowID)`. |
| `table_update_unique_mvcc` | Unique key and strictly ordered sparse `UpdateCol` values. | `UpdateMvcc::Updated(RowID)` or `NotFound`. |
| `table_delete_unique_mvcc` | Unique key. | `DeleteMvcc::Deleted` or `NotFound`. |

```rust,ignore
let key = SelectKey::new(id_index_no, vec![Val::from(1i32)]);
let outcome = trx
    .table_update_unique_mvcc(
        table_id,
        key.index_no,
        &key.vals,
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
explicit-lock errors. Because the enum is non-exhaustive, downstream matches
must include a fallback.

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
| Lifecycle | `Engine`, `Session`, `Transaction`, `IndexScanMvccStream` |
| Configuration | `EngineConfig`, `TrxSysConfig`, `MandatoryRuntimeConfig`, `FileSystemConfig`, `EvictableBufferPoolConfig`, `LogSync`, `DEFAULT_COW_FILE_MAX_SIZE` |
| Schema | `TableSpec`, `ColumnSpec`, `ColumnAttributes`, `IndexSpec`, `IndexKey`, `IndexOrder`, `IndexAttributes`, `IndexNo` |
| Values | `Val`, `ValKind`, `ValType`, `MemVar` |
| Reads | `SelectKey`, `SelectMvcc`, `ScanMvcc`, `LazyRow` |
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
