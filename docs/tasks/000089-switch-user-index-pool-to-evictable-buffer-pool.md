---
id: 000089
title: Switch User Index Pool To Evictable Buffer Pool
status: proposal  # proposal | implemented | superseded
created: 2026-03-24
github_issue: 475
---

# Task: Switch User Index Pool To Evictable Buffer Pool

## Summary

Switch user secondary-index runtime pages from the fixed `IndexPool` to an
evictable pool backed by `index.bin`, so large indexes no longer crash on fixed
pool exhaustion. Keep catalog table rows and catalog indexes on `meta_pool` so
catalog metadata remains fully memory resident by design.

## Context

Docs already describe user secondary indexes as a hybrid MemTree plus
checkpointed DiskTree where volatile in-memory state can be rebuilt from
checkpointed disk roots and redo replay. That model aligns with evictable
runtime pages, but the current runtime still uses a fixed pool for user
secondary indexes.

1. `EngineConfig` still sizes `IndexPool` as a fixed byte budget and builds it
   as `FixedBufferPool`.
2. `FixedBufferPool::allocate_page()` panics when the pool is full, so large
   user secondary indexes can fail with a fatal `buffer pool full`.
3. Task `000063` intentionally left future index-swap design out of scope after
   making the row `data.bin` contract reusable for a later follow-up.
4. Task `000052` specialized catalog runtime to fixed-pool semantics. Catalog
   rows and catalog indexes should remain memory resident instead of joining the
   user-table evictable pool path.
5. Current `main` now propagates `Result` from buffer and index operations.
   This task must preserve those `IOError`/page-I/O semantics when user indexes
   move onto an evictable pool.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

## Goals

1. Move user secondary-index runtime pages from the fixed `IndexPool` to an
   evictable pool with default backing file `index.bin`.
2. Keep catalog table rows and catalog secondary indexes on `meta_pool`.
3. Preserve existing durable table, catalog, and redo formats. `index.bin`
   stays ephemeral and restart continues to rebuild volatile index state from
   checkpointed DiskTree plus redo.
4. Keep engine config narrow by retaining `index_buffer` as the in-memory
   budget and adding explicit swap-file and file-size settings for the user
   index pool.
5. Preserve current `Result`-based storage I/O and index-operation error
   propagation after the user index pool becomes evictable.
6. Keep session bootstrap, recovery, purge, undo, and catalog bootstrap
   semantics correct under the new pool split.

## Non-Goals

1. Changing table-file, catalog-file, or redo-log record formats.
2. Making recovery depend on `index.bin` contents.
3. Moving catalog indexes onto the user evictable index pool.
4. Exposing full public `EvictableBufferPoolConfig` parity for the user index
   pool in this task.
5. Refactoring block-index, checkpoint, or redo-retention architecture beyond
   the pool-family and path changes required here.
6. Reworking low-level fixed or evictable pool internals unrelated to
   supporting the new user index pool wiring.

## Unsafe Considerations (If Applicable)

No new `unsafe` scope is expected, but implementation touches modules adjacent
to existing mmap, direct-I/O, and page-latch code.

1. Review should focus on buffer-pool lifetime, startup, shutdown, and page-I/O
   invariants in:
   - `doradb-storage/src/buffer/mod.rs`
   - `doradb-storage/src/buffer/evict.rs`
   - `doradb-storage/src/buffer/fixed.rs`
   - `doradb-storage/src/storage_path.rs`
2. Keep the change at the pool-family, config, and runtime-wiring boundary. Do
   not widen the `unsafe` surface or add new raw pointer or file descriptor
   ownership paths if existing helpers can be reused.
3. No unsafe inventory refresh is expected unless implementation introduces new
   `unsafe` blocks, which should be avoided.

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Convert `IndexPool` to an evictable component while preserving the explicit
   worker-start lifecycle.
   - Change `component.rs` and `buffer/mod.rs` so `IndexPool` owns
     `EvictableBufferPool`.
   - Add dedicated `IndexPoolWorkers` or equivalent explicit worker component
     that starts and shuts down the index-pool I/O and evictor threads the same
     way `MemPoolWorkers` does today.
   - Keep `PoolRole::Index` as the guard identity for the user index pool.
2. Extend engine config and storage-path resolution for an ephemeral user-index
   swap file.
   - Keep `index_buffer` as the in-memory budget knob.
   - Add `index_swap_file` with default `index.bin`.
   - Add `index_max_file_size` with a conservative default sized for evictable
     user indexes.
   - Extend `ResolvedStoragePaths` to resolve `index_swap_file`, create its
     parent directory, and reject overlap among `index_swap_file`,
     `data_swap_file`, `catalog.mtb`, `storage-layout.toml`, durable table
     files, and the redo-log family.
   - Keep `index_swap_file` and `index_max_file_size` out of the durable marker
     so restart can change ephemeral files without a layout mismatch.
3. Split shared table runtime over row-pool and index-pool families.
   - Change `GenericMemTable<P>` to `GenericMemTable<D, I>` and store the
     secondary-index pool role used to fetch the correct guard.
   - Change `TableAccessor<'a, D>` to `TableAccessor<'a, D, I>` so shared table
     logic uses the runtime's configured index-pool role instead of hard-coding
     `guards.index_guard()`.
   - Store `Box<[GenericSecondaryIndex<I>]>` in the generic runtime instead of
     relying on the fixed-pool compatibility alias.
   - Keep compatibility aliases for user-table and catalog-table accessors.
4. Keep catalog runtime fully memory resident on `meta_pool`.
   - Build catalog rows from `meta_pool` as today.
   - Build catalog secondary indexes from `meta_pool` as well.
   - Remove `CatalogStorage` ownership or use of the separate `index_pool`
     where it is only serving catalog bootstrap.
   - Ensure catalog `PoolGuards` and catalog no-trx CRUD paths use `meta_guard`
     for both row and index access.
5. Move user-table secondary indexes to the evictable user index pool.
   - Update `Table::new`, `build_secondary_indexes`, session bootstrap,
     user-table reload, recovery deps, and related runtime aliases to pass the
     evictable `IndexPool`.
   - Keep block index on `meta_pool`; only user secondary-index pages move.
6. Preserve `Result`-based index and buffer error propagation.
   - Audit user-table callers in table access, purge, undo, session helpers,
     and recovery so the evictable user index pool surfaces `Err(Error::IOError)`
     instead of reintroducing infallible assumptions.
   - Retain explicit `expect` sites only where catalog fixed-pool operations are
     intentionally treated as unrecoverable and that policy is already
     established.
7. Update tests and examples.
   - Add storage-path coverage for `index_swap_file`.
   - Add startup and restart coverage for `index.bin`.
   - Add user-index eviction coverage with a small `index_buffer` and larger
     `index_max_file_size`.
   - Add catalog regression coverage proving catalog index operations do not
     depend on `PoolRole::Index`.
   - Run the supported validation pass with `cargo nextest run -p doradb-storage`.

## Implementation Notes

Keep this section blank in design phase. Fill this section during `task resolve` after implementation, tests, review, and verification are completed.

## Impacts

1. `doradb-storage/src/component.rs`
2. `doradb-storage/src/buffer/mod.rs`
3. `doradb-storage/src/buffer/evict.rs`
4. `doradb-storage/src/engine.rs`
5. `doradb-storage/src/storage_path.rs`
6. `doradb-storage/src/table/mod.rs`
7. `doradb-storage/src/table/access.rs`
8. `doradb-storage/src/index/secondary_index.rs`
9. `doradb-storage/src/index/unique_index.rs`
10. `doradb-storage/src/index/non_unique_index.rs`
11. `doradb-storage/src/catalog/runtime.rs`
12. `doradb-storage/src/catalog/storage/mod.rs`
13. `doradb-storage/src/catalog/mod.rs`
14. `doradb-storage/src/session.rs`
15. `doradb-storage/src/trx/sys_conf.rs`
16. `doradb-storage/src/trx/recover.rs`
17. `doradb-storage/src/trx/purge.rs`
18. `doradb-storage/src/trx/undo/index.rs`
19. Related examples and tests that construct engine config or assume fixed user
    secondary indexes.

## Test Cases

1. `ResolvedStoragePaths` validates `index_swap_file` as a root-relative
   `.bin` file and rejects overlap with `data_swap_file`, `catalog.mtb`,
   `storage-layout.toml`, durable table-file paths, and the redo-log family.
2. Fresh engine startup with default config resolves and uses both `data.bin`
   and `index.bin`.
3. Restart with changed `index_swap_file` succeeds because the file is
   ephemeral and excluded from the durable layout marker.
4. A user table with a small `index_buffer` and larger `index_max_file_size`
   can insert enough indexed rows to force user-index eviction and still serve
   point lookups and scans correctly.
5. Injected or forced user-index pool I/O failure surfaces `Err(Error::IOError)`
   through user index operations rather than panicking.
6. Catalog bootstrap and catalog no-trx CRUD continue to work with catalog
   indexes on `meta_pool` and without requiring `PoolRole::Index`.
7. Recovery reopens checkpointed user tables, replays redo, and rebuilds user
   secondary indexes correctly with the evictable user index pool.
8. Supported validation pass: `cargo nextest run -p doradb-storage`

## Open Questions

1. Should a follow-up expose full public tuning parity between the user index
   pool and `data_buffer` once operational experience with the default
   `index.bin` settings is available?
2. Should future alias cleanup add more explicit names for the user and catalog
   index-pool families once this pool split lands?
