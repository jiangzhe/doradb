---
id: 000193
title: Session maintenance interface
status: implemented
created: 2026-06-26
github_issue: 764
---

# Task: Session maintenance interface

## Summary

Add a small public maintenance surface on `Session` that exposes existing
storage-engine capabilities without adding new storage behavior. The initial
interface should cover table id discovery, catalog checkpoint, and read-only
runtime statistics. Existing public table maintenance methods remain the table
operation surface.

## Context

Doradb storage already has online maintenance primitives, but some are either
not discoverable from the public API or remain crate-private:

- `Session` already exposes user-table freeze, checkpoint readiness,
  checkpoint execution, hot row-page count, and secondary `MemIndex` cleanup.
- `Catalog::checkpoint_now` performs an ad-hoc catalog checkpoint but is
  crate-private and currently documents a panic if overlapping checkpoint
  publication is attempted.
- runtime counters already exist for transaction/redo/purge work, shared
  storage IO, backend submit/wait activity, and buffer-pool access lifecycle,
  but their types and accessors are crate-private.

The public API should reflect those existing capabilities only. It must not
introduce a generic vacuum, offline repair, redo pruning, new checkpoint
algorithm, new cleanup proof, or scheduler.

Issue Labels:
- type:task
- priority:medium
- codex

Relevant design boundaries:

- Table checkpoint owns durable data, cold-delete, and secondary `DiskTree`
  publication as one CoW root.
- `MemIndex` cleanup is a proof-based memory cleanup pass and never mutates
  `DiskTree`.
- Catalog checkpoint uses the cache-first catalog boundary and `catalog.mtb`
  root publication.
- Physical table-file block reclamation remains checkpoint/root-reachability
  work, not a standalone public command.

## Goals

Add these public `Session` methods:

```rust
impl Session {
    pub fn list_table_ids(&self) -> Result<Vec<TableID>>;

    pub async fn checkpoint_catalog(&mut self) -> Result<()>;

    pub fn transaction_system_stats(&self) -> Result<TransactionSystemStats>;
    pub fn storage_io_stats(&self) -> Result<StorageIoStats>;
    pub fn buffer_pool_stats(&self) -> Result<BufferPoolStats>;
}
```

Keep these existing public table maintenance methods as the table operation
surface:

```rust
impl Session {
    pub async fn freeze_table(&self, table_id: TableID, max_rows: usize) -> Result<usize>;
    pub fn table_checkpoint_readiness(&self, table_id: TableID) -> Result<CheckpointReadiness>;
    pub async fn checkpoint_table(&mut self, table_id: TableID) -> Result<CheckpointOutcome>;
    pub async fn total_row_pages(&self, table_id: TableID) -> Result<usize>;
    pub async fn cleanup_secondary_mem_indexes(
        &mut self,
        table_id: TableID,
        clean_live_entries: bool,
    ) -> Result<SecondaryMemIndexCleanupStats>;
}
```

Expose minimal public stats snapshots copied from existing counters:

```rust
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransactionSystemStats {
    pub commit_count: usize,
    pub trx_count: usize,
    pub log_bytes: usize,
    pub sync_count: usize,
    pub sync_nanos: usize,
    pub seal_failure_count: usize,
    pub io_submit_and_wait_count: usize,
    pub io_submit_and_wait_nanos: usize,
    pub purge_trx_count: usize,
    pub purge_row_count: usize,
    pub purge_index_count: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StorageIoStats {
    pub backend: IoBackendStats,
    pub table_read_requests: usize,
    pub pool_read_requests: usize,
    pub background_write_requests: usize,
    pub table_read_turns: usize,
    pub pool_read_turns: usize,
    pub background_write_turns: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IoBackendStats {
    pub submit_and_wait_calls: usize,
    pub submitted_ops: usize,
    pub submit_and_wait_nanos: usize,
    pub wait_completions: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BufferPoolStats {
    pub meta: BufferPoolRuntimeStats,
    pub mem: BufferPoolRuntimeStats,
    pub index: BufferPoolRuntimeStats,
    pub disk: BufferPoolRuntimeStats,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BufferPoolRuntimeStats {
    pub capacity: usize,
    pub allocated: usize,
    pub counters: BufferPoolCounters,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BufferPoolCounters {
    pub cache_hits: usize,
    pub cache_misses: usize,
    pub miss_joins: usize,
    pub queued_reads: usize,
    pub running_reads: usize,
    pub completed_reads: usize,
    pub read_errors: usize,
    pub queued_writes: usize,
    pub running_writes: usize,
    pub completed_writes: usize,
    pub write_errors: usize,
}
```

Read-only diagnostics must remain observable after storage poison while engine
admission is still open:

- `list_table_ids`
- `transaction_system_stats`
- `storage_io_stats`
- `buffer_pool_stats`

Mutating maintenance keeps normal healthy-runtime admission:

- `checkpoint_catalog`

## Non-Goals

- Do not add a CLI, SQL/admin statement, or `Engine` maintenance API.
- Do not add broad `TableMaintenanceInfo`, `TableMaintenanceRequest`, or
  combined orchestration methods.
- Do not expose table schema, index metadata, file paths, redo segment
  metadata, lock-manager state, or evictor tuning stats in this first version.
- Do not add stats reset, stats delta, streaming stats, or background telemetry.
- Do not implement catalog checkpoint scheduling or periodic workers.
- Do not implement offline inspect, repair, redo pruning, physical log
  truncation, vacuum, compaction, or forced file deletion.
- Do not change checkpoint, recovery, transaction, or on-disk format semantics.

## Plan

1. Add a small public maintenance/stats type module.
   - Add the public stats snapshots in `doradb-storage/src/stats.rs`.
   - Export both module-path and root-path APIs with `pub mod stats` plus
     root re-exports from `lib.rs`.
   - Keep the types plain snapshot structs with public fields and no reset or
     mutation API.

2. Add query-admission helpers for poison-observable read-only methods.
   - Add a private helper on `Session`, for example
     `query_session(&self, operation: &'static str) -> Result<SessionQueryPin>`,
     that checks the public session is not closed, upgrades weak engine
     reachability, calls `EngineInner::ensure_admission_open_for_query`, and
     pins the session as still running in `SessionRegistry`.
   - Do not call `Session::pin` or `EngineInner::acquire_admission` from stats
     methods, because those reject storage poison.
   - Keep these methods failing normally after session close, engine shutdown,
     registry removal, or any other stale-session state.

3. Implement `Session::list_table_ids`.
   - Add a crate-private `Catalog::list_user_table_ids_now() -> Vec<TableID>`
     helper over the loaded `user_tables` runtime map.
   - Return only user table ids that currently have runtime entries.
   - Sort the returned ids ascending for deterministic output.
   - Use query admission so the method remains observable after storage poison.
   - Do not scan `catalog.tables` with uncommitted visibility for the public
     method; avoid exposing in-flight DDL rows.

4. Implement `Session::checkpoint_catalog`.
   - Use normal `Session::pin("checkpoint catalog")` so storage poison blocks
     this mutating maintenance path.
   - Reject active user transactions with `OperationError::NotSupported`, using
     the same idle-session style as table checkpoint and DDL paths.
   - Call `session.engine.catalog().checkpoint_now(&session.engine.trx_sys)`.
   - Return `Result<()>`; do not expose catalog checkpoint batch internals.

5. Harden catalog checkpoint admission for public use.
   - Change `CatalogCheckpointGate::begin_checkpoint` so overlapping checkpoint
     callers wait for the active checkpoint to finish instead of hitting the
     current assertion.
   - Preserve metadata-change exclusion semantics: checkpoint waits behind
     active or pending catalog metadata changes, and metadata changes still
     exclude new checkpoints.
   - Keep catalog checkpoint single-writer behavior at the gate boundary; do
     not alter scan/apply semantics.

6. Implement `Session::transaction_system_stats`.
   - Convert the existing crate-private `TrxSysStats` returned by
     `TransactionSystem::trx_sys_stats()` into the public
     `TransactionSystemStats`.
   - Use query admission so the method remains observable after storage poison.

7. Implement `Session::storage_io_stats`.
   - Read `engine.table_fs.storage_service_stats()` and
     `engine.table_fs.io_backend_stats()`.
   - Convert internal `StorageServiceStats` and `IOBackendStats` into public
     `StorageIoStats`.
   - Do not expose per-pool IO backend aliases for `mem` and `index`; those
     route through the shared file-system backend.

8. Implement `Session::buffer_pool_stats`.
   - Read `capacity`, `allocated`, and counter snapshots for `meta_pool`,
     `mem_pool`, `index_pool`, and `disk_pool`.
   - Convert internal `BufferPoolStats` into public `BufferPoolCounters`.
   - Do not include shared evictor wake/run counters in v1.

9. Document method behavior with rustdoc.
   - Call out poison-observable diagnostics on stats/listing methods.
   - Call out that counters are monotonic snapshots and callers can compute
     deltas externally.
   - Call out that catalog checkpoint is an online, mutating operation and
     requires an idle session.

## Implementation Notes

- Implemented the public `Session` maintenance surface for sorted table-id
  listing, online catalog checkpoint, transaction-system stats, shared-storage
  IO stats, and buffer-pool stats.
- Added public runtime stats snapshots in `doradb-storage/src/stats.rs` and
  exposed them through both `doradb_storage::stats::*` and root re-exports.
  The final buffer-pool aggregate type is named `BufferPoolStats` for naming
  consistency.
- Added `Session::query_session` for poison-observable read-only diagnostics.
  It verifies the public handle is not closed, engine query admission remains
  open, and the session is still present/running in the session registry.
  Mutating catalog checkpoint still uses normal healthy-runtime admission and
  rejects active session transactions.
- Changed catalog checkpoint gating so overlapping checkpoint callers wait for
  the active checkpoint instead of panicking, and updated the `checkpoint_now`
  rustdoc to match the serialized behavior.
- Consolidated buffer-pool snapshot plumbing with the new public stats types,
  removing the separate internal flat `BufferPoolStats` type while keeping the
  internal atomic stats handles private.
- Addressed review and CI findings during implementation: removed the
  unnecessary public `Session::in_trx` API, documented poison/shutdown behavior
  on public diagnostics, expanded buffer-pool snapshot test coverage, and made
  the stats monotonicity test robust to redo-log stats publication occurring
  after commit waiters are woken.
- Validation completed:
  - `cargo nextest run -p doradb-storage` passed with 1058 tests.
  - `tools/style_audit.rs --diff-base origin/main` passed with 14 branch-diff
    Rust files checked.
  - The previously flaky
    `session::tests::test_session_stats_snapshots_are_monotonic` passed 50
    consecutive exact runs after the test fix.

## Impacts

- `doradb-storage/src/session.rs`
  - Add new public methods and private query-admission helper.
- `doradb-storage/src/stats.rs`
  - Add public runtime statistics snapshot structs and conversion helpers.
- `doradb-storage/src/catalog/mod.rs`
  - Add runtime user-table id listing helper.
- `doradb-storage/src/catalog/checkpoint.rs`
  - Harden checkpoint gate behavior for overlapping public calls.
- `doradb-storage/src/trx/sys.rs`
  - Reuse existing transaction/redo/purge stats; make conversion possible
    without exposing internal state.
- `doradb-storage/src/buffer/*`, `doradb-storage/src/file/fs.rs`,
  `doradb-storage/src/io/backend.rs`
  - Reuse existing stats snapshots through public conversion types.
- `doradb-storage/src/lib.rs`
  - Expose the public `stats` module and re-export the new stats types.

## Test Cases

Run:

```bash
cargo nextest run -p doradb-storage
```

Add focused tests covering:

1. `list_table_ids` on a fresh engine returns an empty list.
2. `list_table_ids` after creating multiple user tables returns their ids
   sorted and does not include catalog table ids.
3. `list_table_ids` does not expose a table while create-table DDL is staged
   but not durably installed.
4. `checkpoint_catalog` with an active session transaction returns
   `OperationError::NotSupported`.
5. `checkpoint_catalog` after catalog DDL succeeds, and restart loads the
   checkpointed catalog state.
6. Two overlapping `checkpoint_catalog` calls do not panic; the second waits or
   otherwise completes through the gate.
7. `transaction_system_stats` returns zero-like counters on a fresh engine and
   monotonic snapshots after simple committed work, without requiring immediate
   redo-log counter advancement from the preceding operation.
8. `storage_io_stats` and `buffer_pool_stats` return snapshots on a fresh engine
   and remain monotonic after basic table operations.
9. Stats/listing methods remain callable after injected storage poison while
   engine admission is still open.
10. `checkpoint_catalog` fails after injected storage poison because it uses
    normal mutating-operation admission.
11. Stats/listing methods fail after session close, engine shutdown, and
    registry removal.

## Open Questions

None for this task. Future work may add richer operator diagnostics, evictor
stats, stats reset/delta helpers, or external CLI/admin surfaces, but those are
intentionally outside this first public `Session` interface and are not tracked
as actionable follow-ups from this resolve.
