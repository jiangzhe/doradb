# Unsafe Usage Baseline

- Generated on: `2026-03-27`
- Command: `tools/unsafe_inventory.rs`
- Scope: `doradb-storage/src/{buffer,latch,row,index,io,trx,lwc,file}`

## Module Summary

| module | files | unsafe | transmute | new_unchecked | assume_init | // SAFETY: |
|---|---:|---:|---:|---:|---:|---:|
| buffer | 13 | 55 | 0 | 0 | 0 | 48 |
| latch | 4 | 48 | 0 | 0 | 0 | 44 |
| row | 3 | 5 | 0 | 0 | 0 | 5 |
| index | 18 | 18 | 0 | 0 | 3 | 12 |
| io | 7 | 21 | 0 | 0 | 0 | 10 |
| trx | 14 | 6 | 0 | 0 | 0 | 6 |
| lwc | 2 | 2 | 0 | 0 | 0 | 2 |
| file | 8 | 12 | 0 | 0 | 2 | 12 |
| **total** | **69** | **167** | **0** | **0** | **5** | **139** |

## File Hotspots (top 40)

| file | module | unsafe | // SAFETY: |
|---|---|---:|---:|
| `doradb-storage/src/latch/rwlock.rs` | latch | 21 | 18 |
| `doradb-storage/src/latch/mutex.rs` | latch | 18 | 17 |
| `doradb-storage/src/buffer/guard.rs` | buffer | 15 | 14 |
| `doradb-storage/src/buffer/util.rs` | buffer | 12 | 6 |
| `doradb-storage/src/buffer/readonly.rs` | buffer | 9 | 9 |
| `doradb-storage/src/io/libaio_backend.rs` | io | 9 | 2 |
| `doradb-storage/src/latch/hybrid.rs` | latch | 9 | 9 |
| `doradb-storage/src/buffer/arena.rs` | buffer | 8 | 8 |
| `doradb-storage/src/file/mod.rs` | file | 8 | 8 |
| `doradb-storage/src/buffer/evict.rs` | buffer | 6 | 6 |
| `doradb-storage/src/index/btree_hint.rs` | index | 6 | 0 |
| `doradb-storage/src/row/mod.rs` | row | 5 | 5 |
| `doradb-storage/src/trx/undo/row.rs` | trx | 5 | 5 |
| `doradb-storage/src/file/cow_file.rs` | file | 4 | 4 |
| `doradb-storage/src/index/block_index_root.rs` | index | 4 | 4 |
| `doradb-storage/src/io/buf.rs` | io | 4 | 3 |
| `doradb-storage/src/io/iouring_backend.rs` | io | 3 | 3 |
| `doradb-storage/src/io/mod.rs` | io | 3 | 1 |
| `doradb-storage/src/buffer/fixed.rs` | buffer | 2 | 2 |
| `doradb-storage/src/buffer/frame.rs` | buffer | 2 | 2 |
| `doradb-storage/src/index/block_index.rs` | index | 2 | 2 |
| `doradb-storage/src/index/column_payload.rs` | index | 2 | 2 |
| `doradb-storage/src/index/row_block_index.rs` | index | 2 | 2 |
| `doradb-storage/src/io/libaio_abi.rs` | io | 2 | 1 |
| `doradb-storage/src/lwc/page.rs` | lwc | 2 | 2 |
| `doradb-storage/src/buffer/page.rs` | buffer | 1 | 1 |
| `doradb-storage/src/index/btree_node.rs` | index | 1 | 1 |
| `doradb-storage/src/index/column_block_index.rs` | index | 1 | 1 |
| `doradb-storage/src/trx/log_replay.rs` | trx | 1 | 1 |

## Cast-Risk Candidates

| file | module | transmute | new_unchecked | assume_init | total |
|---|---|---:|---:|---:|---:|
| `doradb-storage/src/file/mod.rs` | file | 0 | 0 | 2 | 2 |
| `doradb-storage/src/index/btree_node.rs` | index | 0 | 0 | 2 | 2 |
| `doradb-storage/src/index/column_block_index.rs` | index | 0 | 0 | 1 | 1 |
