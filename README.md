# DoraDB

![build](https://github.com/jiangzhe/doradb/actions/workflows/build.yml/badge.svg)
![codecov](https://codecov.io/gh/jiangzhe/doradb/branch/main/graph/badge.svg?token=T3RMZE2998)

DoraDB is an attempt to build a fast storage engine in Rust from scratch.
It is work in progress.

## Goal

The original goal is to build a complete relational database, compatible with MySQL.

After coding about 70k lines of Rust, I have to admit it's really too ambitious to achieve as a single-person project.

Now I limit its scope to be a fast storage engine, which is probably achievable. I also want it to be useful (not just a toy) after major features are done.

The storage engine is designed as a hybrid engine with both in-memory row store and on-disk column store, and have full transactional support across all data.

## Design 

- [Storage Architecture](./docs/architecture.md)
- [Transaction System](./docs/transaction-system.md)
- [Index Design](./docs/index-design.md)
- [Checkpoint and Recovery](./docs/checkpoint-and-recovery.md)
- [Table File](./docs/table-file.md)
- [Data Checkpoint](./docs/data-checkpoint.md)
- [Delta Checkpoint](./docs/delta-checkpoint.md)

Some ideas are different from traditional database system.
I'm glad to have discussions if someone is interested in details.

## Code Structure

I will only focus on [doradb-storage](./doradb-storage) for a long time. 
And other modules will be left as is for a long time.

Code structure of storage engine:

- [buffer](./doradb-storage/src/buffer): Buffer pool implementation with async direct IO.
- [catalog](./doradb-storage/src/catalog): Catalog of storage engine.
- [compression](./doradb-storage/src/compression): Compression algorithms for column store.
- [file](./doradb-storage/src/file): Storage of table data, index and delete bitmap. The file is page based and organized as CoW B+Tree, to enable simple recovery and fast access.
- [index](./doradb-storage/src/index): Block index and B+Tree index.
- [io](./doradb-storage/src/io): Async direct IO system backed by libaio. May introduce io-uring in future.
- [latch](./doradb-storage/src/latch): Async latch primitives including Mutex, RWLock and HybridLatch(enhanced RWLock with optimistic mode).
- [lwc](./doradb-storage/src/lwc): LightWeight Columnar format for on-disk warm data.
- [row](./doradb-storage/src/row): In-memory row store and operations.
- [stmt](./doradb-storage/src/stmt): Statements.
- [table](./doradb-storage/src/table): Table of data, composite of block index, secondary index, buffer pool and table file. Support operations like index lookup, index scan, table scan, insert, delete, update, etc.
- [trx](./doradb-storage/src/trx): Transaction system, including transaction lifecycle, redo log, undo log, recovery, garbage collect, etc.

## Document-Driven AI Development Flow

Current devlopment is driven by document, and implemneted by code agent.
Every task assigned to agent has one associated task document located in `docs/tasks`.

## License

This project is licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or
   https://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or
   https://opensource.org/licenses/MIT)

at your option.
