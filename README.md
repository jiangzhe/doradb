# DoraDB

![Build](https://github.com/jiangzhe/doradb/actions/workflows/build.yml/badge.svg)
![Cargo Deny Status](https://github.com/jiangzhe/doradb/actions/workflows/cargo-deny.yml/badge.svg)
![Codecov](https://codecov.io/gh/jiangzhe/doradb/branch/main/graph/badge.svg?token=T3RMZE2998)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/26b91ce46f944686a9a200af04d3920b)](https://app.codacy.com/gh/jiangzhe/doradb/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade)

DoraDB is an attempt to build a modern and fast storage engine in Rust from scratch.
It is work in progress.

## Goal

Build a modern and fast storage engine.

The storage engine is designed as a hybrid engine managing both in-memory row store and on-disk column store, with full transactional support across all data.

## Quick Start

The snippets below assume an async function returning `doradb_storage::Result<()>`.
DoraDB is runtime agnostic, so you can run these futures on whichever async runtime your application already uses.
For a complete runnable version, see [quick_start.rs](./doradb-storage/examples/quick_start.rs) or run `cargo run -p doradb-storage --example quick_start`.

Create a table with a schema and indexes, then drop it from an idle session.

```rust
use doradb_storage::{
    ColumnAttributes, ColumnSpec, EngineConfig, IndexAttributes, IndexKey, IndexSpec, TableSpec,
    ValKind,
};

let engine = EngineConfig::default()
    .storage_root("target/doradb-quick-start")
    .build()
    .await?;
let mut session = engine.new_session()?;

let table_id = session
    .create_table(
        TableSpec::new(vec![
            ColumnSpec::new("id", ValKind::I32, ColumnAttributes::empty()),
            ColumnSpec::new("name", ValKind::VarByte, ColumnAttributes::empty()),
        ]),
        vec![
            IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::UK),
            IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::empty()),
        ],
    )
    .await?;

session.drop_table(table_id).await?;
session.close().await?;
engine.shutdown()?;
```

Insert, update, and delete rows by executing statements inside a transaction.

```rust
use doradb_storage::{SelectKey, UpdateCol, Val};

let mut trx = session.begin_trx()?;

trx.exec(async |stmt| {
    stmt.table_insert_mvcc(table_id, vec![Val::from(1i32), Val::from("alice")])
        .await?;
    Ok(())
})
.await?;

let key = SelectKey::new(0, vec![Val::from(1i32)]);
trx.exec(async |stmt| {
    stmt.table_update_unique_mvcc(
        table_id,
        &key,
        vec![UpdateCol {
            idx: 1,
            val: Val::from("ada"),
        }],
    )
    .await?;
    Ok(())
})
.await?;

trx.exec(async |stmt| {
    stmt.table_delete_unique_mvcc(table_id, &key, false).await?;
    Ok(())
})
.await?;

trx.commit().await?;
```

Scan rows, read one unique-key row, and scan matching rows through a secondary index.

```rust
use doradb_storage::{SelectKey, Val};

let mut trx = session.begin_trx()?;
let mut rows = Vec::new();

trx.exec(async |stmt| {
    stmt.table_scan_mvcc(table_id, &[0, 1], |vals| {
        rows.push(vals);
        true
    })
    .await?;
    Ok(())
})
.await?;

let id_key = SelectKey::new(0, vec![Val::from(1i32)]);
let _row = trx
    .exec(async |stmt| {
        stmt.table_lookup_unique_mvcc(table_id, &id_key, &[0, 1])
            .await
    })
    .await?;

let name_key = SelectKey::new(1, vec![Val::from("ada")]);
let _matching_rows = trx
    .exec(async |stmt| {
        stmt.table_index_scan_mvcc(table_id, &name_key, &[0, 1])
            .await
    })
    .await?
    .unwrap_rows();

trx.rollback().await?;
```

## Design 

- [Storage Architecture](./docs/architecture.md)
- [Transaction System](./docs/transaction-system.md)
- [Index Design](./docs/index-design.md)
- [Block Index](./docs/block-index.md)
- [Secondary Index](./docs/secondary-index.md)
- [Checkpoint and Recovery](./docs/checkpoint-and-recovery.md)
- [Data Checkpoint](./docs/data-checkpoint.md)
- [Deletion Checkpoint](./docs/deletion-checkpoint.md)
- [Table File](./docs/table-file.md)
- [Buffer Pool](./docs/buffer-pool.md)
- [Garbage Collect](./docs/garbage-collect.md)

Above documents describe the core design of this engine. Most parts are update-to-date.
Some ideas are different from traditional database system.
I'm glad to have discussions if someone is interested in details.

## Code Structure

- [buffer](./doradb-storage/src/buffer): Buffer pool implementation with async direct IO.
- [catalog](./doradb-storage/src/catalog): Catalog of storage engine.
- [compression](./doradb-storage/src/compression): Compression algorithms for column store.
- [file](./doradb-storage/src/file): Storage of table data, index and delete bitmap. The file is page based and organized as CoW B+Tree, to enable simple recovery and fast access.
- [index](./doradb-storage/src/index): Block index and B+Tree index.
- [io](./doradb-storage/src/io): Async direct IO system with compile-time-selected `libaio` and `io_uring` backends, by default `io_uring`.
- [latch](./doradb-storage/src/latch): Async latch primitives including Mutex, RWLock and HybridLatch(enhanced RWLock with optimistic mode).
- [lock](./doradb-storage/src/lock): Metadata lock and table-level lock.
- [log](./doradb-storage/src/log): Redo log record encoding, append, and initialization.
- [lwc](./doradb-storage/src/lwc): Lightweight columnar format for on-disk warm data.
- [recovery](./doradb-storage/src/recovery): Startup recovery coordination, redo stream replay, and recovered row state.
- [row](./doradb-storage/src/row): In-memory row store and operations.
- [table](./doradb-storage/src/table): Table of data, composite of block index, secondary index, buffer pool and table file. Support operations like index lookup, index scan, table scan, insert, delete, update, etc.
- [trx](./doradb-storage/src/trx): Transaction system, including transaction lifecycle, redo log integration, undo log, purge, and garbage collection.

## Document-Driven AI Development Flow

Current development is heavily driven by document starting from Jan 2026, and implemneted by code agent.
Every task assigned to agent has one associated task document located in `docs/tasks`.
Larger features/refactors require RFC document inside `docs/rfcs`.
Deferred tasks are put in `docs/backlogs`.

## License

This project is licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or
   https://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or
   https://opensource.org/licenses/MIT)

at your option.
