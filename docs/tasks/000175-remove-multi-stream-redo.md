---
id: 000175
title: Remove Multi-Stream Redo
status: implemented
created: 2026-06-12
github_issue: 700
---

# Task: Remove Multi-Stream Redo

## Summary

Remove redo-log multi-stream support from the storage engine and make redo a
single canonical ordered stream by construction. The implementation should
delete the configurable partition count, stop assigning transactions to log
partitions, remove recovery/catalog checkpoint stream merging, collapse
partition-shaped GC routing where possible, and update the durable redo file
layout from partitioned file names to single-stream file names.

This is an intentional breaking storage-layout cleanup. Existing partitioned
redo files and old storage-layout markers do not need migration or
compatibility support; startup should reject them clearly instead of attempting
partial recovery.

## Context

Issue Labels:
- type:task
- priority:medium
- codex

Source Backlogs:
- docs/backlogs/closed/000120-simplify-redo-single-log-stream.md

Related context:
- docs/tasks/000169-fix-failed-precommit-redo-cleanup.md
- docs/tasks/000063-refine-storage-path-configuration-and-restart-contract.md
- docs/architecture.md
- docs/transaction-system.md
- docs/checkpoint-and-recovery.md
- docs/table-file.md

Doradb's persistence model depends on checkpointed CoW roots plus redo-only
recovery. Redo records are the recovery-visible CTS carrier for committed
runtime changes that are not yet checkpointed. Ordered no-log commits are valid
only for process-local runtime effects and must not become recovery timestamp
seeds.

The current runtime defaults to one log partition, but the codebase still
supports configurable multiple redo partitions. That leaves commit durability,
failed redo cleanup, persisted CTS advancement, catalog checkpoint scans, GC
bucket routing, shutdown, and recovery replay with partition-local behavior
that must be reasoned about as a merged global order. Backlog 000120 was created
after the failed-precommit cleanup task exposed this extra complexity: the
durable-prefix guarantee is straightforward within one partition but harder to
prove across multiple partition workers.

The desired direction is to remove the partitioned topology entirely. The
engine is still in active breaking development, and backward compatibility is a
low-priority project concern, so this task should prefer a clean single-stream
shape over migration scaffolding.

## Goals

1. Make the transaction system use exactly one redo log stream by construction.
2. Remove user-facing and internal support for configuring multiple redo log
   partitions.
3. Remove round-robin log partition assignment from transaction begin and
   commit routing.
4. Remove transaction-carried `log_no` state when it is no longer needed for
   single-stream commit, rollback, failed-precommit cleanup, or purge.
5. Preserve existing redo record contents and commit-group batching semantics
   unless a small mechanical adjustment is required by the single-stream
   refactor.
6. Preserve failed-precommit cleanup guarantees from task 000169:
   rollback-capable payloads remain owned until cleanup finishes, redo failure
   remains the canonical first poison source, and waiters complete only after
   mandatory cleanup handoff/ordering is satisfied.
7. Replace partitioned redo file names:
   - old shape: `<log-file-stem>.<partition>.<8-hex-seq>`
   - new shape: `<log-file-stem>.<8-hex-seq>`
8. Remove `log_partitions` from the durable storage-layout marker and bump the
   marker version so old markers are rejected.
9. Detect legacy partitioned redo files for the configured log family and fail
   startup with a clear error instead of silently ignoring nonzero partitions.
10. Simplify recovery so it replays one ordered redo stream directly without
    `LogMerger`.
11. Simplify catalog checkpoint scanning so it scans one redo stream up to the
    single durable upper watermark.
12. Remove structs, methods, constants, tests, and helpers that become unused
    after multi-stream removal. Do not keep dead partition-era APIs for a
    future cleanup task.

## Non-Goals

1. Do not support migration from existing partitioned redo files.
2. Do not support compatibility with old `storage-layout.toml` markers that
   include `log_partitions`.
3. Do not introduce a global commit sequencer or preserve multiple physical log
   writers behind a logical sequencer.
4. Do not redesign redo record encoding, row/table redo payloads, MVCC undo
   layout, checkpoint algorithms, or table-file root formats beyond the redo
   storage-layout cleanup.
5. Do not change no-log ordered commit semantics: no-log ordered commits remain
   volatile and recovery-invisible.
6. Do not broaden this into parallel recovery or a general recovery rewrite.

## Plan

1. Simplify redo configuration and durable layout.
   - Remove `TrxSysConfig::log_partitions(...)`.
   - Remove `DEFAULT_LOG_PARTITIONS` and `MAX_LOG_PARTITIONS` if they become
     unused.
   - Remove `log_partitions` from `DurableStorageLayout`.
   - Bump `STORAGE_LAYOUT_VERSION`.
   - Update durable-layout marker tests so old marker version/shape fails and
     the new marker excludes partition count.
   - Update path-overlap checks and error text so the redo log family is
     described as a single-stream file family.

2. Replace redo file naming and discovery.
   - Change redo file creation to use
     `<file_prefix>.<file_sequence:08x>`.
   - Replace partition-aware `list_log_files(file_prefix, log_no, ...)` with
     the single-stream `discover_redo_log_files` helper.
   - Add validation that detects legacy names matching
     `<file_prefix>.<partition>.<file_sequence:08x>` and reports a clear
     unsupported old-layout error.
   - Update direct tests and helpers that hardcode names such as
     `redo.log.0.00000000`.

3. Collapse transaction-system runtime to one redo log.
   - Replace `TransactionSystem::log_partitions` with a single redo-log field,
     or rename/refactor `LogPartition` to `RedoLog` if the change is local and
     keeps code clearer.
   - Remove `rr_partition_id` and `next_log_no`.
   - In `begin_trx`, select only a GC bucket from the single redo log and
     construct `TrxInner`/`TrxContext` without a log partition id.
   - In user and system commit paths, route directly to the single redo log.
   - Update stats collection, worker startup, shutdown, and log-reader helpers
     to address the single redo log directly.

4. Remove partition identity from transaction payload flow.
   - Remove `log_no` from `TrxContext`, `TrxInner`, `PreparedTrxPayload`,
     `PrecommitTrxPayload`, and related accessors where practical.
   - Update rollback and failed-precommit cleanup to address the single redo
     log's GC buckets directly by `gc_no`.
   - Update tests that inspect payload `log_no` to instead assert the remaining
     GC bucket or cleanup behavior.

5. Simplify recovery.
   - Change `log_recover` to accept or create one redo-log initializer/stream.
   - Replay that stream directly in CTS order as written.
   - Remove production `LogMerger` usage from recovery.
   - Remove `LogMerger`, `LogPartitionStream`, or related stream-merging types
     if they become unused after recovery and catalog checkpoint are updated.
   - Keep timestamp seeding behavior unchanged: recovery still seeds from
     checkpoint metadata, table roots, and real redo headers only.

6. Simplify catalog checkpoint scanning.
   - Remove `CatalogCheckpointScanConfig::log_partitions`.
   - Build one single-stream redo reader for the configured log family.
   - Preserve the existing catalog DDL action logic:
     include, skip, or stop before unsafe table DDL exactly as before.
   - Continue bounding the scan by the single persisted durable watermark.

7. Simplify GC and purge routing.
   - Keep multiple GC buckets if they still reduce contention.
   - Remove the partition dimension from purge tasks and purge helpers.
   - Update `calc_min_active_sts_for_gc`, purge dispatch, and purge stats to
     scan the single redo log's buckets directly.

8. Remove dead partition-era code and tests.
   - Delete tests whose only purpose is validating two-partition merge behavior.
   - Rewrite coverage to verify single-stream ordering, log rotation, direct
     recovery replay, catalog checkpoint scanning, and unsupported legacy redo
     detection.
   - Run:
     `rg -n "log_partitions|MAX_LOG_PARTITIONS|DEFAULT_LOG_PARTITIONS|LogMerger|log_no"`
     and either remove remaining production hits or justify any intentionally
     retained names in comments or task resolve notes.

9. Update docs and comments.
   - Update transaction/recovery docs if they mention multiple redo streams,
     partitioned durable watermarks, or stream merging.
   - Keep historical task/backlog docs unchanged except for normal resolve-time
     closure of the source backlog.

## Implementation Notes

- Implemented the single-stream redo topology: `TransactionSystem` now owns one
  canonical `RedoLog`, commits route directly to it, transaction payload flow no
  longer carries a log partition id, recovery replays one stream directly, and
  catalog checkpoint scanning reads one redo file family.
- Removed partition-era configuration and durable layout state: `log_partitions`
  and related constants/config APIs were removed, the storage-layout marker
  version was bumped, and legacy partitioned redo names/old layout markers are
  rejected instead of migrated.
- Replaced partitioned redo file discovery with `discover_redo_log_files`.
  Discovery now validates single-stream redo names, rejects legacy partitioned
  names, detects missing file sequence gaps with
  `DataIntegrityError::RedoLogSequenceGap`, and keeps numeric ascending or
  descending order.
- Simplified runtime cleanup around the single redo log: removed dead
  partition helpers, removed `RedoLog::logs()`, renamed remaining local redo-log
  variables for clarity, moved `RedoLog` GC methods back into `trx/log.rs`, and
  changed purge executor send failure handling to an invariant-preserving
  `expect(...)`.
- Verified implementation and review follow-ups with:
  - `cargo fmt`
  - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
  - `cargo nextest run -p doradb-storage` (926 passed)
- Deferred broader transaction background worker ownership redesign to
  `docs/backlogs/000125-redesign-redo-gc-purge-worker-ownership.md`.

## Impacts

- `doradb-storage/src/conf/consts.rs`
  - `STORAGE_LAYOUT_VERSION`
  - `DEFAULT_LOG_PARTITIONS`
  - `MAX_LOG_PARTITIONS`
- `doradb-storage/src/conf/mod.rs`
  - exported redo config constants
- `doradb-storage/src/conf/trx.rs`
  - `TrxSysConfig`
  - redo log initializer creation
  - redo log file discovery
- `doradb-storage/src/conf/engine.rs`
  - `DurableStorageLayout`
  - storage-layout marker validation
  - storage path overlap checks for the redo log family
- `doradb-storage/src/trx/log.rs`
  - `LogPartitionInitializer`
  - `LogPartitionMode`
  - `LogPartition`
  - `discover_redo_log_files`
  - `create_log_file`
  - log file naming/parsing tests
  - redo write/sync failure handling tests
- `doradb-storage/src/trx/log_replay.rs`
  - `LogPartitionStream`
  - `LogMerger`
  - single-stream log reader tests
- `doradb-storage/src/trx/sys.rs`
  - `TransactionSystem::log_partitions`
  - `rr_partition_id`
  - `begin_trx`
  - `commit_transaction`
  - `commit_sys`
  - `persisted_watermark_cts`
  - redo IO/GC worker startup and shutdown
  - transaction-system stats
- `doradb-storage/src/trx/mod.rs`
  - `TrxContext`
  - `TrxInner`
  - `PreparedTrxPayload`
  - `PrecommitTrxPayload`
  - failed-precommit cleanup helpers and tests
- `doradb-storage/src/trx/recover.rs`
  - `log_recover`
  - `LogRecovery`
  - recovery tests that previously constructed a merger
- `doradb-storage/src/trx/purge.rs`
  - purge task routing
  - GC bucket scans
  - purge stats
- `doradb-storage/src/catalog/checkpoint.rs`
  - `CatalogCheckpointScanConfig`
  - redo scan construction
  - catalog checkpoint scan tests
- `doradb-storage/src/index/row_page_index.rs`
  - tests that inspect redo files directly
- `doradb-storage/src/table/tests.rs`
  - tests that hardcode redo file names or rely on partitioned config
- `docs/transaction-system.md`
- `docs/checkpoint-and-recovery.md`
- `docs/table-file.md`

## Test Cases

1. Fresh engine startup creates a single redo file using the new naming shape
   after the first durable redo write.
   - Expected file example: `redo.log.00000000`.
   - No `redo.log.0.00000000` file should be created.

2. Log rotation preserves single-stream ordering across multiple files.
   - Use a small redo file size.
   - Commit enough transactions to rotate.
   - Read discovered files in order and assert CTS monotonicity.

3. Startup rejects legacy partitioned redo files.
   - Create a configured redo log directory containing a file like
     `redo.log.0.00000000` or `redo.log.1.00000000`.
   - Engine startup should fail with a clear unsupported old redo layout or
     invalid payload/storage-layout error.

4. Startup rejects old durable layout markers.
   - Write a version-1 `storage-layout.toml` containing `log_partitions`.
   - Engine startup should fail before recovery instead of accepting or
     partially adapting the old layout.

5. User commits all route through the single redo log.
   - Run concurrent user transactions.
   - Assert committed CTS values are ordered in the single redo stream after
     reading log files.

6. System transactions use the same single redo log.
   - Exercise row-page creation/system redo.
   - Verify the system redo record appears in the single-stream log file.

7. Recovery replays one stream without `LogMerger`.
   - Create a table, commit inserts/updates/deletes, drop and reopen the
     engine, and assert recovered hot rows, deletion buffer behavior, and
     secondary-index rebuild match existing recovery expectations.

8. Catalog checkpoint scans one stream.
   - Run catalog metadata changes, scan/apply a catalog checkpoint, and verify
     `catalog_replay_start_ts` advances according to the same DDL include/skip
     rules as before.

9. Failed redo write and sync preserve task-000169 behavior.
   - Existing redo write/fsync/fdatasync failure tests should still pass.
   - Session-bound failed-precommit cleanup should still release session state,
     preserve the original redo poison source, and avoid dropping row/index
     rollback payloads before cleanup.

10. Purge and GC still work without partition identity.
    - Committed row undo, index GC, and row-page GC are handed to purge through
      `gc_no` only.
    - Rollback and failed-precommit cleanup update GC rollback bookkeeping
      through the single redo log's buckets.

11. Validation commands:
    - `cargo fmt`
    - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
    - `cargo nextest run -p doradb-storage`
    - If backend-neutral IO behavior is materially touched:
      `cargo nextest run -p doradb-storage --no-default-features --features libaio`

## Open Questions

Follow-up:

- `docs/backlogs/000125-redesign-redo-gc-purge-worker-ownership.md` tracks the
  broader redo IO, GC analysis, and purge worker ownership redesign that came
  up during review. This task intentionally did not resolve whether GC analysis
  should remain a dedicated thread, be grouped with redo IO, or move into purge
  dispatch.

Resolved decisions:

1. The task intentionally removes compatibility with old partitioned redo file
   names and old storage-layout markers.
2. File-name cleanup is in scope: new redo files should not include a partition
   component.
3. Dead partition-era structs, methods, constants, and tests should be removed
   as part of this task rather than deferred.
4. Existing redo record payload format is preserved; only the log topology and
   file-family layout are simplified.
