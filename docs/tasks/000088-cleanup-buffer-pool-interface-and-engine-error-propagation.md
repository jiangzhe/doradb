---
id: 000088
title: Clean Up Buffer-Pool Interface And Complete Engine Error Propagation
status: implemented  # proposal | implemented | superseded
created: 2026-03-24
github_issue: 473
---

# Task: Clean Up Buffer-Pool Interface And Complete Engine Error Propagation

## Summary

Implement phase 4 of RFC-0010 by removing infallible buffer-pool read APIs,
making the canonical page-access contract result-bearing, folding readonly
validated-page access into the readonly pool API itself, and propagating
that contract through index, table, checkpoint, recovery, purge, and file-root
loading paths. This task completes the interface cleanup that task `000087`
intentionally deferred so engine-facing storage access no longer mixes
infallible helpers, result-bearing wrappers, and readonly-specific ad hoc
entrypoints.

## Context

RFC-0010 defines this work as the cleanup phase after storage-I/O error-policy
delivery and before `io_uring` implementation. Task `000087` already made
supported data/index I/O failures caller-visible, but it left one major
interface inconsistency behind:

1. `BufferPool` still exposes paired infallible and fallible read methods.
2. `ReadonlyBufferPool` still keeps validated persisted-page access on its own
   inherent helper path.
3. Higher-level storage code still mixes both styles across row-page access,
   block-index navigation, persisted LWC/blob reads, checkpoint root loading,
   and recovery/purge flows.

That split now leaks into the modules that RFC-0010 explicitly names for phase
4: checkpoint, recovery, index, table, purge, and related callers. The task
therefore has to be broader than a trait-only cleanup. It must update the
consumer graph that still depends on the old ambiguity, including persisted-page
readers and checkpoint bootstrap/apply code.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
Issue Labels:
- type:task
- priority:medium
- codex

Parent RFC:
- docs/rfcs/0010-retire-thread-pool-async-io-and-introduce-backend-neutral-completion-core.md

## Goals

1. Make shared buffer-pool read operations unambiguously result-bearing.
2. Remove infallible page-read wrappers from production paths that can surface
   storage I/O failures.
3. Move validated readonly persisted-page reads into the canonical readonly
   pool API.
4. Update index, table, checkpoint, recovery, purge, and file/root-loading
   code to use the canonical access surface.
5. Preserve domain-specific outcomes such as `Option` misses and
   `Validation::Invalid`; only storage access errors should move through
   `Result`.
6. Keep phase-3 storage poison behavior unchanged while removing remaining
   interface ambiguity around those paths.
7. Keep allocation/writeback semantics and the current supported validation
   flow intact.

## Non-Goals

1. Redesigning page allocation/deallocation capabilities or splitting readonly
   pools out of `BufferPool` entirely.
2. Adding retry policy, degraded read-only mode, or new storage error classes.
3. Changing MVCC, recovery algorithms, checkpoint boundaries, or on-disk
   formats.
4. Implementing `io_uring` or further backend work.
5. Refactoring unrelated table/index logic beyond what is required to adopt
   the canonical access contract.

## Unsafe Considerations (If Applicable)

This task touches unsafe-sensitive storage I/O code because buffer-pool page
ownership, direct-I/O buffers, and persisted-page validation all depend on
stable raw memory and correct completion/lifetime ordering.

1. Affected modules and why `unsafe` remains relevant:
   - `doradb-storage/src/buffer/evict.rs` and
     `doradb-storage/src/buffer/readonly.rs`: direct-I/O page buffers, frame
     ownership, inflight read/write completion, and cache invalidation still
     depend on raw page memory staying valid until worker-observed completion.
   - `doradb-storage/src/buffer/guard.rs`, `arena.rs`, and related page-guard
     paths: keepalive, latch, and frame lifetime ordering must remain unchanged
     while signatures move to result-bearing returns.
   - `doradb-storage/src/file/cow_file.rs`, `table_file.rs`, and
     `multi_table_file.rs`: root-page reads and checkpoint publish flows still
     cross direct-buffer and persisted-page validation boundaries.
2. Required invariants:
   - signature cleanup must not let inflight page buffers or guarded frames
     drop before worker-side completion or error publication finishes;
   - readonly validated-page reads must not publish stale or failed mappings as
     successful cache hits;
   - frame/latch/guard drop order must remain compatible with current pool
     provenance and quiescent-lifetime rules;
   - root-page and persisted-page parsing must continue to read from stable
     bytes after acquisition and before validation/invalidation completes.
3. Inventory refresh and validation scope:

```bash
tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md
cargo build -p doradb-storage
cargo nextest run -p doradb-storage
cargo clippy --all-features --all-targets -- -D warnings
```

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Canonicalize the buffer-pool read traits.
   - Update `doradb-storage/src/buffer/mod.rs` so the shared read-style
     operations become:
     - `get_page(...) -> impl Future<Output = Result<FacadePageGuard<T>>> + Send`
     - `get_page_versioned(...) -> impl Future<Output = Result<Option<FacadePageGuard<T>>>> + Send`
     - `get_child_page(...) -> impl Future<Output = Result<Validation<FacadePageGuard<T>>>> + Send`
   - Remove the duplicated infallible/fallible read families from the shared
     trait.
   - Keep allocation-oriented methods unchanged in this task.
   - Introduce a crate-private readonly extension trait in `buffer/mod.rs` for
     validated persisted-page shared reads and move the current readonly helper
     contract there.

2. Update the concrete buffer-pool implementations.
   - `FixedBufferPool` should adapt to the new canonical signatures by
     returning `Ok(...)` directly for read operations.
   - `EvictableBufferPool` should expose the same error propagation it already
     supports through the canonical names instead of infallible wrappers.
   - `ReadonlyBufferPool` should:
     - adopt the canonical result-bearing read methods;
     - implement the new validated readonly extension trait;
     - remove the old inherent validated-read entrypoint from production use.

3. Collapse duplicate helper layers in index and table code.
   - Update `doradb-storage/src/table/mod.rs` and
     `doradb-storage/src/catalog/mod.rs` row-page helper families so they no
     longer keep parallel infallible and result-bearing methods for the same
     operations.
   - Update `doradb-storage/src/index/row_block_index.rs`,
     `doradb-storage/src/index/block_index.rs`, and
     `doradb-storage/src/index/btree.rs` to use the canonical result-bearing
     pool API and remove remaining infallible insert-page / node-navigation
     wrappers where storage I/O can occur.
   - Preserve logical outcomes such as cache miss, version mismatch, or
     latch-coupling invalidation as `Option` / `Validation`, rather than
     collapsing them into generic I/O errors.

4. Propagate the canonical access contract through persisted-read and
   checkpoint/recovery flows.
   - Update `doradb-storage/src/lwc/page.rs`,
     `doradb-storage/src/index/column_block_index.rs`, and
     `doradb-storage/src/index/column_deletion_blob.rs` to consume the new
     readonly validated-page trait.
   - Update `doradb-storage/src/file/cow_file.rs`, `table_file.rs`, and
     `multi_table_file.rs` root loading and checkpoint-related readers so page
     acquisition no longer relies on infallible buffer access.
   - Update `doradb-storage/src/table/persistence.rs` and
     `doradb-storage/src/catalog/storage/checkpoint.rs` checkpoint paths to use
     the canonical persisted-page access contract for column-index and LWC
     reads.
   - Update `doradb-storage/src/table/recover.rs`,
     `doradb-storage/src/trx/recover.rs`, and `doradb-storage/src/trx/purge.rs`
     so recovery and purge page reloads surface storage errors rather than
     depending on infallible helpers.

5. Clean up tests and doc-facing expectations.
   - Rewrite affected unit tests to use the canonical names and result shapes.
   - Add or extend corruption/failure tests around readonly validated reads,
     root loading, recovery page refresh, and checkpoint bootstrap/apply paths.
   - Keep `Implementation Notes` empty until resolve time.
   - Leave RFC phase-state synchronization and any follow-up backlog creation
     to `task resolve`.

## Implementation Notes

1. Canonicalized the shared buffer-pool read contract and readonly validated
   read capability.
   - `doradb-storage/src/buffer/mod.rs` now keeps one canonical shared-read
     surface: `get_page(...)`, `get_page_versioned(...)`, and
     `get_child_page(...)` all return futures whose outputs are
     result-bearing, with `get_page_versioned(...)` preserving
     `Result<Option<...>>` so stale page/version mismatches remain distinct
     from storage errors;
   - validated persisted-page reads now live on `ReadonlyBufferPool` itself as
     the crate-private inherent async method
     `get_validated_page_shared(...)`; the temporary
     `ReadonlyBufferPoolExt` layer and `QuiescentGuard<T>` trait-forwarding
     impls were removed after follow-up cleanup.
2. Propagated the canonical contract through the concrete pools and engine
   consumers named in the task scope.
   - `FixedBufferPool`, `EvictableBufferPool`, and `ReadonlyBufferPool` were
     updated to the canonical signatures;
   - table/catalog row-page helpers, row/block-index and B-tree navigation,
     persisted LWC and column-index/blob readers, file root loading,
     checkpoint/bootstrap flows, recovery, and purge now use the canonical
     result-bearing access path rather than keeping parallel infallible helper
     families in production code.
3. Completed runtime B-tree and index error propagation within the same task
   boundary.
   - `doradb-storage/src/index/btree.rs` no longer hides buffer-pool failures
     behind `expect(...)` in runtime lookup, insert, delete/update, split,
     cursor, compaction, or destroy paths; those operations now return
     `Result<...>` while preserving logical outcomes such as
     `BTreeInsert`, `BTreeDelete`, `BTreeUpdate`, `Option`, and
     `Validation::Invalid` inside the success domain;
   - unique and non-unique index wrappers, table access/index maintenance
     paths, recovery/undo callers, and B-tree prefix scan callers were updated
     to propagate those storage failures to their callers instead of relying on
     hidden panics in `btree.rs`.
4. Completed review-driven follow-up within the task boundary.
   - purge row-page deallocation in `doradb-storage/src/trx/purge.rs` no
     longer panics on `expect(...)`; fatal deallocation failures now poison the
     runtime with `StoragePoisonSource::PurgeDeallocate` and stop the purge
     loop cleanly instead;
   - checkpoint write failure handling in
     `doradb-storage/src/table/persistence.rs` now treats rollback as
     best-effort before `poison_storage(...)` so the poison path still runs
     even if rollback itself returns an error;
   - added focused regression coverage for that purge failure path so the
     shared poison state is asserted without relying on a background-task
     panic.
5. Validation completed for the implemented state.
   - `cargo fmt --all`
     - result: passed
   - `cargo build -p doradb-storage`
     - result: passed
   - `cargo test -p doradb-storage trx::purge::tests::test_handle_gc_row_page_deallocation_result_poisons_runtime -- --exact`
     - result: passed
   - `cargo nextest run -p doradb-storage`
     - result: `433/433` passed
   - `cargo clippy --all-features --all-targets -- -D warnings`
     - result: passed
6. Tracking and review state at resolve time.
   - task issue: `#473`
   - implementation PR: `#474`

## Impacts

Related modules, files, structs, traits and functions:
- `doradb-storage/src/buffer/mod.rs`
  - `BufferPool`
  - canonical shared-read API only
  - removal of `QuiescentGuard<T>` blanket trait forwarding
- `doradb-storage/src/buffer/fixed.rs`
  - `impl BufferPool for FixedBufferPool`
- `doradb-storage/src/buffer/evict.rs`
  - `impl BufferPool for EvictableBufferPool`
- `doradb-storage/src/buffer/readonly.rs`
  - `ReadonlyBufferPool`
  - `get_validated_page_shared(...)`
  - readonly cache invalidation and miss-join paths
- `doradb-storage/src/table/mod.rs`
  - row-page shared/exclusive helper family
- `doradb-storage/src/catalog/mod.rs`
  - catalog table-handle row-page wrappers
- `doradb-storage/src/table/access.rs`
  - `TableAccess` consumer paths
  - index delete/rollback maintenance propagation
- `doradb-storage/src/index/row_block_index.rs`
  - insert-page reload and node traversal helpers
- `doradb-storage/src/index/block_index.rs`
  - insert-page wrapper surface
- `doradb-storage/src/index/btree.rs`
  - runtime lookup/insert/delete/update APIs
  - node acquisition/navigation, split, cursor, compaction, and destroy helpers
- `doradb-storage/src/index/btree_scan.rs`
  - prefix scan result propagation
- `doradb-storage/src/index/unique_index.rs`
  - unique index wrapper result propagation
- `doradb-storage/src/index/non_unique_index.rs`
  - non-unique index wrapper result propagation
- `doradb-storage/src/lwc/page.rs`
  - `PersistedLwcPage::load`
- `doradb-storage/src/index/column_block_index.rs`
  - validated persisted node reads
- `doradb-storage/src/index/column_deletion_blob.rs`
  - validated persisted blob page reads
- `doradb-storage/src/file/cow_file.rs`
  - `load_active_root_from_pool`
- `doradb-storage/src/file/table_file.rs`
  - table-file open/root load helpers
- `doradb-storage/src/file/multi_table_file.rs`
  - multi-table root load helpers
- `doradb-storage/src/table/persistence.rs`
  - data/deletion checkpoint persisted-page reads
  - checkpoint poison-on-write failure path
- `doradb-storage/src/catalog/storage/checkpoint.rs`
  - catalog checkpoint bootstrap/apply persisted-page reads
- `doradb-storage/src/table/recover.rs`
  - persisted-data index rebuild
- `doradb-storage/src/trx/undo/index.rs`
  - index undo rollback propagation
- `doradb-storage/src/trx/recover.rs`
  - row-page refresh during log recovery
- `doradb-storage/src/trx/purge.rs`
  - row-page reload, purge validation, and fatal deallocation handling

## Test Cases

1. Buffer pool API migration correctness.
   - `FixedBufferPool`, `EvictableBufferPool`, and `ReadonlyBufferPool` should
     compile and pass unit tests under the canonical result-bearing
     signatures.
   - `QuiescentGuard<T>` autoderef and explicit underlying-pool references
     should remain sufficient without blanket trait-forwarding impls.

2. Readonly validated persisted-page behavior.
   - cache-miss validated loads through
     `ReadonlyBufferPool::get_validated_page_shared(...)` still reject
     corruption before residency is published;
   - cached-hit revalidation still invalidates stale/corrupt mappings and
     returns `Err(...)`;
   - persisted LWC, column-index, and deletion-blob readers still decode valid
     pages successfully.

3. Index and table helper behavior.
   - row/block-index and B-tree helpers preserve miss/version-mismatch/
     validation behavior while surfacing true storage I/O failures separately;
   - runtime B-tree operations and unique/non-unique index wrappers preserve
     logical outcomes inside `Result<...>` instead of panicking on page-load
     failures;
   - table and catalog row-page helper families no longer depend on infallible
     wrappers.

4. Checkpoint and root-loading flows.
   - `CowFile::load_active_root_from_pool()` and table/multi-table open flows
     return `Err(...)` on corrupted or unreadable persisted pages instead of
     panicking through buffer access;
   - table data/deletion checkpoint and catalog checkpoint paths still succeed
     on valid data and surface persisted-page failures explicitly;
   - checkpoint write failures still publish `poison_storage(...)` even when
     rollback itself fails.

5. Recovery and purge flows.
   - table persisted-data index rebuild continues to work on valid checkpointed
     pages;
   - recovery row-page refresh and purge row-page reloads surface page-load
     failures explicitly;
   - purge row-page deallocation failures poison the runtime and stop the
     worker cleanly instead of panicking in the background task;
   - existing restart/bootstrap corruption tests continue to pass or are
     updated to assert the new explicit error path.

6. Supported validation run.
   - `cargo nextest run -p doradb-storage`
   - targeted build and lint validation for the changed surface:
     - `cargo build -p doradb-storage`
     - `cargo clippy --all-features --all-targets -- -D warnings`

## Open Questions

1. `ReadonlyBufferPool` will still expose unsupported
   allocation/deallocation methods through the shared `BufferPool` trait after
   this task. If that capability split becomes noisy again after phase 4, a
   later cleanup can revisit whether mutation-only methods should move behind a
   narrower trait without reopening the current phase scope.
