# Storage Architecture

## Overview

Doradb storage is designed for HTAP scenario.

The storage has three data formats:

1. In-memory row pages.

In-memory row pages contain hot data which are processed by recent transactions.
Row pages support in-place updates.

2. LWC pages on disk.

LWC(LightWeight Columnar) pages on disk store warm data for persistence.
This kind of pages apply lightweight columnar compression, such as bitpacking and dict, to support both fast scan and random access. 
Updates are converted to delete mask + insert. So there are also delete bitmaps stored accordingly.

3. Column pages on disk(optional).

Column pages on disk store cold data with columnar encoding.
They are transformed by background task to speed up analytical queries.
If the engine is used as a local storage, columnar encoding is preferred.
If the engine is used as an ingestion and query node, integration with object store is preferred.
So let's see what it can be in future.

![doradb-storage-architecture](./images/doradb-storage-architecture.png)

### Row ID

When new data is coming, a unique identifier is assigned to each row, called **RowID**. wherever the data is located, **RowID** will not change.

If update happens in in-mem row store, modification will be applied directly on row page and the undo information will be stored in a version chain associated to that row.

If update target row on disk, either in LWC page or column page, the old data will be extracted and modified and re-inserted into row store in memory with a new **RowID**. Meanwhile a delete bit will be applied to bitmap page cache associated to that data page on disk. And also in such scenario, the secondary index will be updated to point to new **RowID**.

### Block Index

**Block Index** is introduced to manage data among the three layouts, with the help of **RowID**.

**Block Index** is basically a specialized B+Tree stores mapping from **RowID** range to page id or block id.

For data in in-memory row store, it only stores page id.

For data on disk, it stores block id with some statistics, which can be used for data skipping.

### Table File

**Table File** contians all persistent data of single table, including LWC pages, column pages, index pages and bitmap pages.

The principal of data modification in **Table File** is to do it in copy-on-write way. Despite of batch insert, background tasks will be executed periodically for row-to-column data transmission, delta merge of index and bitmap from transaction logs.

### Redo Log File

**Redo Log File** contains all committed data of recent transactions. It's different from the concept of "WAL log" in tranditional database perspective, because it only persists committed data. It does not contains "undo", therefore it does not support ARIES-style fuzzy checkpoint. The design of transactional system with logging and recovery will be introduced in a separate document.

### Secondary Index

**Secondary Index** is a B+Tree index. It stores mapping between key and row id.
The secondary index is composite of two trees: **MemTree** and **DiskTree**.
**MemTree** is similar to a MemTable in LSM architecture, it holds new data inserted and hot data which are frequently updated. A background task will transfer **MemTree** to **DiskTree** as part of the checkpoint process.

**DiskTree** is a CoW B+Tree which supports fast lookup, scan and batch inserts. Small transactions will be combined as large batch and modify the **DiskTree** in a CoW way. New root will be created and updated in metadata. In such way, no logging is required for recovery.

Query on index will execute on both tree and aggregate their results.

## Transactional System

See [Transaction System](./transaction-system.md).

## Process Flow

### Point Select

```mermaid
---
title: Point Select
---
flowchart TD
    A[Begin]
    A --> B[Lookup secondary index to get row id]
    B --> C[Lookup block index to get page id]
    C --> D[Read page]
    D --> E[Visibility check]
    E --> F[Return]
```

### Analytical Scan

```mermaid
---
title: Analytical Scan
---
flowchart TD
    A[Begin]
    A --> B[Scan block index]
    B --> C[Scan row pages]
    C --> D[Version chain check]
    B --> E[Scan LWC pages on disk]
    E --> F[Merge delete bitmap and version]
    B --> G[Pre-filter on column statistics]
    G --> H[Scan column pages on disk]
    H --> I[Merge delete bitmap and version]
    D --> J[Aggregate and return]
    F --> J
    I --> J
```

### Point Insert

```mermaid
---
title: Insert
---
flowchart TD
    A[Begin]
    A --> B[Acquire free row page]
    B --> C[Write row]
    C --> D[Insert secondary index]
    D --> E[Return]
```

### Update Hot

```mermaid
---
title: Update Hot
---
flowchart TD
    A[Begin]
    A --> B[Lookup secondary index]
    B --> C[Lookup block index]
    C --> D[Modify row page and append version chain]
    D --> E[Update secondary index if needed]
    E --> F[Return]
```

### Update Cold

```mermaid
---
title: Update Cold
---
flowchart TD
    A[Begin]
    A --> B[Lookup secondary index]
    B --> C[Lookup block index]
    C --> D[Read bitmap page, LWC/column page]
    D --> E[Apply mark and version to bitmap page]
    E --> F[Copy old row and modify]
    F --> G[Acquire free row page]
    G --> H[Insert new row to row page]
    H --> I[Update or insert secondary index]
    I --> J[Return]
```

### Batch Insert

```mermaid
---
title: Batch Insert
---
flowchart TD
    A[Begin transaction]
    A --> B[Lock table]
    B --> C[Convert all row pages to LWC pages]
    C --> D[Insert new data to LWC pages, bypass version chain]
    D --> E[Insert secondary index]
    E --> F[Commit transaction]
```
