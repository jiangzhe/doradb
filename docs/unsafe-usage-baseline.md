# Unsafe Usage Baseline

- Generated on: `2026-02-21`
- Command: `cargo +nightly -Zscript tools/unsafe_inventory.rs`
- Scope: `doradb-storage/src/{buffer,latch,row,index,io,trx,lwc,file}`

## Module Summary

| module | files | unsafe | transmute | new_unchecked | assume_init | // SAFETY: |
|---|---:|---:|---:|---:|---:|---:|
| buffer | 8 | 55 | 0 | 0 | 0 | 5 |
| latch | 4 | 39 | 0 | 0 | 0 | 3 |
| row | 3 | 5 | 0 | 0 | 0 | 3 |
| index | 13 | 18 | 0 | 0 | 3 | 9 |
| io | 3 | 37 | 0 | 0 | 0 | 1 |
| trx | 15 | 7 | 0 | 0 | 0 | 2 |
| lwc | 2 | 2 | 0 | 0 | 0 | 2 |
| file | 5 | 22 | 0 | 0 | 2 | 6 |
| **total** | **53** | **185** | **0** | **0** | **5** | **31** |

## File Hotspots (top 40)

| file | module | unsafe | // SAFETY: |
|---|---|---:|---:|
| `doradb-storage/src/io/mod.rs` | io | 23 | 1 |
| `doradb-storage/src/latch/mutex.rs` | latch | 18 | 0 |
| `doradb-storage/src/latch/rwlock.rs` | latch | 18 | 0 |
| `doradb-storage/src/file/mod.rs` | file | 17 | 3 |
| `doradb-storage/src/buffer/util.rs` | buffer | 13 | 0 |
| `doradb-storage/src/buffer/guard.rs` | buffer | 12 | 3 |
| `doradb-storage/src/buffer/evict.rs` | buffer | 10 | 0 |
| `doradb-storage/src/io/libaio_abi.rs` | io | 10 | 0 |
| `doradb-storage/src/buffer/readonly.rs` | buffer | 9 | 0 |
| `doradb-storage/src/index/block_index.rs` | index | 8 | 5 |
| `doradb-storage/src/buffer/fixed.rs` | buffer | 6 | 0 |
| `doradb-storage/src/index/btree_hint.rs` | index | 6 | 0 |
| `doradb-storage/src/row/mod.rs` | row | 5 | 3 |
| `doradb-storage/src/trx/undo/row.rs` | trx | 5 | 2 |
| `doradb-storage/src/buffer/frame.rs` | buffer | 4 | 2 |
| `doradb-storage/src/file/table_file.rs` | file | 4 | 3 |
| `doradb-storage/src/io/buf.rs` | io | 4 | 0 |
| `doradb-storage/src/index/column_block_index.rs` | index | 3 | 3 |
| `doradb-storage/src/latch/hybrid.rs` | latch | 3 | 3 |
| `doradb-storage/src/lwc/page.rs` | lwc | 2 | 2 |
| `doradb-storage/src/buffer/page.rs` | buffer | 1 | 0 |
| `doradb-storage/src/file/table_fs.rs` | file | 1 | 0 |
| `doradb-storage/src/index/btree_node.rs` | index | 1 | 1 |
| `doradb-storage/src/trx/log_replay.rs` | trx | 1 | 0 |
| `doradb-storage/src/trx/sys.rs` | trx | 1 | 0 |

## Cast-Risk Candidates

| file | module | transmute | new_unchecked | assume_init | total |
|---|---|---:|---:|---:|---:|
| `doradb-storage/src/file/mod.rs` | file | 0 | 0 | 2 | 2 |
| `doradb-storage/src/index/btree_node.rs` | index | 0 | 0 | 2 | 2 |
| `doradb-storage/src/index/column_block_index.rs` | index | 0 | 0 | 1 | 1 |
