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
