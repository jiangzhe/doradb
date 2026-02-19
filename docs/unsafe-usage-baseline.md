# Unsafe Usage Baseline

- Generated on: `2026-02-19`
- Command: `cargo +nightly -Zscript tools/unsafe_inventory.rs`
- Scope: `doradb-storage/src/{buffer,latch,row,index,io,trx,lwc,file}`

## Module Summary

| module | files | unsafe | transmute | new_unchecked | assume_init | // SAFETY: |
|---|---:|---:|---:|---:|---:|---:|
| buffer | 7 | 59 | 0 | 0 | 0 | 8 |
| latch | 4 | 41 | 0 | 0 | 0 | 4 |
| row | 3 | 6 | 0 | 0 | 1 | 3 |
| index | 13 | 79 | 0 | 0 | 3 | 12 |
| io | 3 | 37 | 0 | 0 | 0 | 1 |
| trx | 15 | 7 | 0 | 0 | 0 | 2 |
| lwc | 2 | 8 | 6 | 0 | 3 | 0 |
| file | 5 | 22 | 0 | 0 | 2 | 6 |
| **total** | **52** | **259** | **6** | **0** | **9** | **36** |

## File Hotspots (top 40)

| file | module | unsafe | // SAFETY: |
|---|---|---:|---:|
| `doradb-storage/src/index/btree_node.rs` | index | 39 | 2 |
| `doradb-storage/src/io/mod.rs` | io | 23 | 1 |
| `doradb-storage/src/latch/mutex.rs` | latch | 18 | 0 |
| `doradb-storage/src/latch/rwlock.rs` | latch | 18 | 0 |
| `doradb-storage/src/buffer/evict.rs` | buffer | 17 | 3 |
| `doradb-storage/src/buffer/guard.rs` | buffer | 17 | 3 |
| `doradb-storage/src/file/mod.rs` | file | 17 | 3 |
| `doradb-storage/src/buffer/fixed.rs` | buffer | 12 | 0 |
| `doradb-storage/src/index/btree.rs` | index | 11 | 0 |
| `doradb-storage/src/index/column_block_index.rs` | index | 11 | 7 |
| `doradb-storage/src/io/libaio_abi.rs` | io | 10 | 0 |
| `doradb-storage/src/buffer/util.rs` | buffer | 8 | 0 |
| `doradb-storage/src/index/block_index.rs` | index | 7 | 3 |
| `doradb-storage/src/index/btree_hint.rs` | index | 6 | 0 |
| `doradb-storage/src/row/mod.rs` | row | 6 | 3 |
| `doradb-storage/src/latch/hybrid.rs` | latch | 5 | 4 |
| `doradb-storage/src/lwc/mod.rs` | lwc | 5 | 0 |
| `doradb-storage/src/trx/undo/row.rs` | trx | 5 | 2 |
| `doradb-storage/src/buffer/frame.rs` | buffer | 4 | 2 |
| `doradb-storage/src/file/table_file.rs` | file | 4 | 3 |
| `doradb-storage/src/io/buf.rs` | io | 4 | 0 |
| `doradb-storage/src/lwc/page.rs` | lwc | 3 | 0 |
| `doradb-storage/src/index/btree_scan.rs` | index | 2 | 0 |
| `doradb-storage/src/index/unique_index.rs` | index | 2 | 0 |
| `doradb-storage/src/buffer/page.rs` | buffer | 1 | 0 |
| `doradb-storage/src/file/table_fs.rs` | file | 1 | 0 |
| `doradb-storage/src/index/non_unique_index.rs` | index | 1 | 0 |
| `doradb-storage/src/trx/log_replay.rs` | trx | 1 | 0 |
| `doradb-storage/src/trx/sys.rs` | trx | 1 | 0 |

## Cast-Risk Candidates

| file | module | transmute | new_unchecked | assume_init | total |
|---|---|---:|---:|---:|---:|
| `doradb-storage/src/lwc/mod.rs` | lwc | 2 | 0 | 3 | 5 |
| `doradb-storage/src/lwc/page.rs` | lwc | 4 | 0 | 0 | 4 |
| `doradb-storage/src/file/mod.rs` | file | 0 | 0 | 2 | 2 |
| `doradb-storage/src/index/btree_node.rs` | index | 0 | 0 | 2 | 2 |
| `doradb-storage/src/index/column_block_index.rs` | index | 0 | 0 | 1 | 1 |
| `doradb-storage/src/row/mod.rs` | row | 0 | 0 | 1 | 1 |
