# Task: Readonly Column Buffer Pool Phase 3 Per-Table Integration

## Summary

Implement Phase 3 of RFC-0004 (`docs/rfcs/0004-readonly-column-buffer-pool-program.md`, issue `#340`) by wiring per-table readonly buffer pool into runtime LWC row reads.

This phase delivers:
1. engine-global `GlobalReadonlyBufferPool` ownership and lifecycle wiring;
2. per-table `ReadonlyBufferPool` ownership inside `Table`;
3. `Table::read_lwc_row` switch from direct `TableFile::read_page` to readonly buffer pool page access.

Configuration decision:
1. default readonly buffer size is `256MiB`;
2. size is configurable via `TableFileSystemConfig`.

## Context

Phase 2 already implemented readonly buffer core and per-table wrapper type in `doradb-storage/src/buffer/readonly.rs`, including miss-load, inflight dedup, and drop-only eviction.

However, the actual table read path still bypasses this cache:
1. `Table::read_lwc_row` in `doradb-storage/src/table/mod.rs` reads directly from file (`TableFile::read_page`).

RFC-0004 Phase 3 requires integrating the per-table wrapper into table runtime without yet touching column-block-index traversal (`find_in_file`), which is Phase 4 scope.

Lifecycle constraint from prior design review:
1. `TableFileSystem` driver thread must stop before readonly pool frame/page arena is deallocated.

## Goals

1. Make table LWC reads (`Table::read_lwc_row`) use readonly buffer pages.
2. Ensure each `Table` owns a per-table `ReadonlyBufferPool` wrapper bound to table id and table file.
3. Add engine-level global readonly pool and pass it to all table construction paths.
4. Add readonly buffer size config in `TableFileSystemConfig` with default `256MiB`.
5. Enforce teardown order: drop `TableFileSystem` before dropping global readonly pool.

## Non-Goals

1. No integration of readonly buffer pool into `index::column_block_index::find_in_file` or `BlockIndex` file traversal (Phase 4).
2. No eviction-policy tuning, admission policy, or metrics API.
3. No checkpoint/recovery semantic changes beyond existing invalidation contract.
4. No API redesign of `BufferPool`.

## Unsafe Considerations (If Applicable)

This task should minimize unsafe changes. Existing readonly core already contains unsafe-adjacent frame/page pointer handling.

Requirements if helper integration touches unsafe boundaries:
1. Preserve frame/key/state publication invariants established in Phase 2.
2. Add/retain concrete `// SAFETY:` comments for any pointer cast or raw-page reinterpretation path changed in this phase.
3. Keep lifetime safety through drop ordering (`TableFileSystem` dropped before readonly pool).

Validation scope:
```bash
cargo test -p doradb-storage --no-default-features
cargo test -p doradb-storage
```

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Extend `TableFileSystemConfig` in `doradb-storage/src/file/table_fs.rs`:
   - add readonly buffer size field;
   - add builder setter;
   - set default to `256MiB`.
2. Add global readonly pool ownership in `doradb-storage/src/engine.rs`:
   - build `GlobalReadonlyBufferPool` from `EngineConfig.file.readonly_buffer_size`;
   - store in `EngineInner`;
   - update `Drop` order to drop `table_fs` before readonly pool.
3. Update `Table` shape in `doradb-storage/src/table/mod.rs`:
   - add `readonly_pool: ReadonlyBufferPool` field;
   - update `Table::new(...)` signature to accept global readonly pool;
   - update `Clone` implementation accordingly.
4. Update all table construction call paths:
   - `doradb-storage/src/catalog/storage/mod.rs`
   - `doradb-storage/src/catalog/mod.rs`
   - `doradb-storage/src/trx/sys_conf.rs`
   - `doradb-storage/src/trx/recover.rs`
5. Integrate readonly page fetch in `Table::read_lwc_row`:
   - replace direct file read with per-table readonly pool page access;
   - parse LWC page from cached page bytes and keep decode semantics identical.
6. Keep `index::column_block_index` and `BlockIndex` file traversal unchanged in this phase.

## Impacts

Primary files:
1. `doradb-storage/src/file/table_fs.rs`
2. `doradb-storage/src/engine.rs`
3. `doradb-storage/src/table/mod.rs`
4. `doradb-storage/src/catalog/storage/mod.rs`
5. `doradb-storage/src/catalog/mod.rs`
6. `doradb-storage/src/trx/sys_conf.rs`
7. `doradb-storage/src/trx/recover.rs`

Primary structs/functions:
1. `TableFileSystemConfig`
2. `EngineInner`
3. `Table`
4. `Table::new`
5. `Table::read_lwc_row`
6. per-table `ReadonlyBufferPool` runtime helper interface (if needed for `&self` call sites)

## Test Cases

1. LWC row read path uses readonly cache and returns identical values as before.
2. Repeated read on same LWC block hits cache path (no functional regression).
3. Table construction/reload paths (catalog init, create-table reload, recovery replay path) compile and run with new `Table::new` signature.
4. Engine teardown remains safe with new drop order.
5. Regression test suites:
```bash
cargo test -p doradb-storage --no-default-features
cargo test -p doradb-storage
```

## Open Questions

1. Follow-up Phase 4 will integrate readonly pool into `column_block_index::find_in_file` and validate lookup/read boundary there.
