# Unsafe Usage Baseline

- Generated on: `2026-05-02`
- Command: `tools/unsafe_inventory.rs`
- Scope: `doradb-storage/src/{buffer,latch,row,index,io,trx,lwc,file}`

## Module Summary

| module | files | unsafe | transmute | new_unchecked | assume_init | // SAFETY: |
|---|---:|---:|---:|---:|---:|---:|
| buffer | 13 | 50 | 0 | 0 | 0 | 44 |
| latch | 4 | 40 | 0 | 0 | 0 | 36 |
| row | 3 | 6 | 0 | 0 | 0 | 6 |
| index | 19 | 13 | 0 | 0 | 3 | 7 |
| io | 7 | 20 | 0 | 0 | 1 | 15 |
| trx | 15 | 5 | 0 | 0 | 0 | 5 |
| lwc | 2 | 0 | 0 | 0 | 0 | 0 |
| file | 8 | 13 | 0 | 0 | 2 | 13 |
| **total** | **71** | **147** | **0** | **0** | **6** | **126** |

## File Hotspots (top 40)

| file | module | unsafe | // SAFETY: |
|---|---|---:|---:|
| `doradb-storage/src/latch/rwlock.rs` | latch | 20 | 17 |
| `doradb-storage/src/buffer/guard.rs` | buffer | 15 | 15 |
| `doradb-storage/src/latch/mutex.rs` | latch | 14 | 13 |
| `doradb-storage/src/buffer/util.rs` | buffer | 10 | 5 |
| `doradb-storage/src/io/libaio_backend.rs` | io | 9 | 8 |
| `doradb-storage/src/file/mod.rs` | file | 8 | 8 |
| `doradb-storage/src/buffer/arena.rs` | buffer | 7 | 7 |
| `doradb-storage/src/buffer/evict.rs` | buffer | 7 | 7 |
| `doradb-storage/src/index/btree/hint.rs` | index | 6 | 0 |
| `doradb-storage/src/latch/hybrid.rs` | latch | 6 | 6 |
| `doradb-storage/src/row/mod.rs` | row | 6 | 6 |
| `doradb-storage/src/buffer/readonly.rs` | buffer | 5 | 5 |
| `doradb-storage/src/file/cow_file.rs` | file | 5 | 5 |
| `doradb-storage/src/io/buf.rs` | io | 4 | 3 |
| `doradb-storage/src/io/mod.rs` | io | 4 | 2 |
| `doradb-storage/src/trx/undo/row.rs` | trx | 4 | 4 |
| `doradb-storage/src/index/block_index_root.rs` | index | 3 | 3 |
| `doradb-storage/src/buffer/fixed.rs` | buffer | 2 | 2 |
| `doradb-storage/src/buffer/frame.rs` | buffer | 2 | 2 |
| `doradb-storage/src/buffer/page.rs` | buffer | 2 | 1 |
| `doradb-storage/src/index/btree/node.rs` | index | 2 | 2 |
| `doradb-storage/src/io/libaio_abi.rs` | io | 2 | 1 |
| `doradb-storage/src/index/column_block_index.rs` | index | 1 | 1 |
| `doradb-storage/src/index/row_page_index.rs` | index | 1 | 1 |
| `doradb-storage/src/io/iouring_backend.rs` | io | 1 | 1 |
| `doradb-storage/src/trx/log_replay.rs` | trx | 1 | 1 |

## Cast-Risk Candidates

| file | module | transmute | new_unchecked | assume_init | total |
|---|---|---:|---:|---:|---:|
| `doradb-storage/src/file/mod.rs` | file | 0 | 0 | 2 | 2 |
| `doradb-storage/src/index/btree/node.rs` | index | 0 | 0 | 2 | 2 |
| `doradb-storage/src/index/column_block_index.rs` | index | 0 | 0 | 1 | 1 |
| `doradb-storage/src/io/mod.rs` | io | 0 | 0 | 1 | 1 |
