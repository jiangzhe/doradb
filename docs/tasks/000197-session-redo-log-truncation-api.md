---
id: 000197
title: Session Redo Log Truncation API
status: proposal
created: 2026-06-27
github_issue: 777
---

# Task: Session Redo Log Truncation API

## Summary

Implement RFC 0022 Phase 4 by adding a public session maintenance API that
physically removes recovery-obsolete sealed redo prefix files. The API must
consume the Phase 3 redo truncation planner, publish the durable
`first_redo_log_seq` marker before unlinking files, remove eligible files below
the new marker, and return summarized progress plus blockers.

This task is the first physical redo file cleanup step. It must preserve the
Phase 1 recovery contract: files below the durable marker may be present or
absent, while every file at or above the marker remains required to exist in a
contiguous retained suffix.

## Context

RFC 0022 adds catalog-backed physical redo truncation in phases:

- Phase 1 persisted `first_redo_log_seq` in `catalog.mtb` and made redo
  discovery/recovery strict only at or above that durable marker.
- Phase 2 made catalog checkpoint scan record catalog-safe sealed segment
  progress and added the public `Session::checkpoint_catalog` maintenance API.
- Phase 3 added side-effect-free global truncation planning through
  `TransactionSystem::plan_redo_truncation()`, combining catalog progress, live
  table replay floors, pending dropped-table replay floors, and retained redo
  segment metadata.

This task is Phase 4, "Session Truncate API." Phase 3 intentionally left marker
advancement, unlink execution, unlink retry behavior, public outcome structs,
and exact retention-gate placement for this phase.

Issue Labels:
- type:task
- priority:medium
- codex

Parent RFC:
- `docs/rfcs/0022-catalog-backed-redo-log-truncation.md`

Target RFC Phase:
- Phase 4: Session Truncate API

Phase prerequisites relied on:
- Phase 1 marker/discovery contract exists. `catalog.mtb` persists
  `first_redo_log_seq`, and recovery/discovery accepts absent redo files only
  below that marker.
- Phase 3 planning exists. `TransactionSystem::plan_redo_truncation()` returns
  eligible sealed prefix candidates and blockers without mutating durable state
  or unlinking files.
- Catalog checkpoint scan already records catalog-safe sealed segment progress,
  and `Session::checkpoint_catalog` exposes that checkpoint work explicitly.
- Table active-root replay floors and pending dropped-table replay floors are
  represented in the Phase 3 plan.

Phase-local choices resolved:
- Expose the public API on `Session`, not `Engine`.
- Keep `Session::truncate_redo_log` a mutating maintenance operation that uses
  normal healthy-runtime admission and rejects active session transactions.
- Use a short redo-retention gate. Catalog checkpoint holds it while using a
  marker-based redo scan snapshot, and truncation holds it while planning,
  publishing the marker, and unlinking files. This avoids mixed old-marker and
  new-filesystem observations.
- Always compute a fresh truncation plan under the gate. Do not accept a
  caller-provided plan and do not reuse a cached candidate list.
- Use marker-before-unlink publication. If marker publication fails, do not
  unlink any redo file.
- Report unlink failures as summarized retryable cleanup results. `NotFound`
  is counted as already missing; other unlink failures are counted and can be
  retried by a later call.
- Return aggregate outcome counts rather than per-file public results.
- On every call, attempt cleanup of any present redo sequence files below the
  durable marker, so previous partial unlink failures can be retried even when
  no new candidates are available.

Following-phase contract:
- RFC 0022 has no following implementation phase. No RFC phase-plan edit is
  expected during design.
- During task resolve, update RFC 0022 Phase 4 task path, issue id, status, and
  implementation summary. If implementation changes the phase-local choices
  above, edit RFC 0022 phase text before running task/RFC sync.

Current behavior:
- `Session::checkpoint_catalog(&mut self) -> Result<()>` exists and requires an
  idle session.
- `TransactionSystem::plan_redo_truncation()` is crate-private and currently
  unused by production truncation because no physical truncation API exists.
- `CatalogStorage::checkpoint_snapshot()` can read the durable
  `first_redo_log_seq`, but production code does not yet have a marker
  publication helper.
- Physical redo files remain on disk even when Phase 3 planning can identify
  sealed prefix candidates.

## Goals

- Add `Session::truncate_redo_log(&mut self) -> Result<RedoTruncationOutcome>`.
- Add public outcome/blocker types with documentation and root exports.
- Reject `truncate_redo_log` when the session has an active transaction.
- Serialize catalog checkpoint marker-based redo scan and truncation
  plan/marker/unlink execution through a narrow redo-retention gate.
- Compute a fresh `RedoTruncationPlan` under that gate.
- Publish `first_redo_log_seq = last_candidate_seq + 1` durably before unlinking
  files when the plan has candidates.
- Leave the marker unchanged when the plan has no candidates.
- Attempt physical removal for all present redo sequence files below the final
  durable marker, including leftovers from earlier partial unlink failures.
- Treat `NotFound` during unlink as a successful already-missing outcome.
- Count other unlink failures in the outcome without poisoning storage.
- Propagate planning and marker-publication failures normally; convert marker
  publication IO failure to the same fatal checkpoint-write poison boundary used
  by catalog checkpoint.
- Add focused tests for admission, candidate unlink, restart recovery,
  retained-suffix strictness, retryable partial unlink behavior, below-marker
  cleanup retry, and catalog checkpoint/truncation serialization.

## Non-Goals

- Do not add a public redo truncation dry-run API.
- Do not expose per-file public unlink results in this task.
- Do not run catalog checkpoint or table checkpoint implicitly from
  `truncate_redo_log`.
- Do not add background redo retention scheduling.
- Do not trigger automatic idle-table checkpointing.
- Do not truncate, delete, or hole-punch the active unsealed redo file.
- Do not implement sparse-file hole punching or intra-file truncation.
- Do not change redo file format, redo payload format, or `catalog.mtb` marker
  encoding.
- Do not open every table file to compute floors; continue using Phase 3
  resident roots and retained dropped-table metadata.
- Do not weaken recovery/discovery strictness for files at or above
  `first_redo_log_seq`.

## Plan

1. Add public outcome and blocker types.
   - Add public docs for every public type and field.
   - Prefer placing the types near the `Session` maintenance surface unless a
     small public maintenance module is clearer during implementation.
   - Re-export the types from `doradb-storage/src/lib.rs`.
   - Use an aggregate shape equivalent to:

```rust
pub struct RedoTruncationOutcome {
    pub previous_first_retained_file_seq: u32,
    pub new_first_retained_file_seq: u32,
    pub advanced_files: usize,
    pub removed_files: usize,
    pub already_missing_files: usize,
    pub failed_unlink_files: usize,
    pub blockers: Vec<RedoTruncationBlockerInfo>,
}

pub enum RedoTruncationBlockerInfo {
    CatalogFloor {
        catalog_replay_start_ts: TrxID,
    },
    LiveTableFloor {
        table_id: TableID,
        heap_redo_start_ts: TrxID,
        deletion_cutoff_ts: TrxID,
    },
    PendingDroppedTableFloor {
        table_id: TableID,
        drop_cts: TrxID,
        heap_redo_start_ts: TrxID,
        deletion_cutoff_ts: TrxID,
    },
    UnsealedFile {
        file_seq: u32,
    },
}
```

   - `previous_first_retained_file_seq` is the durable marker observed by the
     gated plan before marker publication.
   - `new_first_retained_file_seq` is the marker after any successful
     publication in this call.
   - `advanced_files` is the number of newly planned candidate files covered by
     marker advancement in this call.
   - `removed_files`, `already_missing_files`, and `failed_unlink_files` count
     physical cleanup attempts below `new_first_retained_file_seq`.
   - `blockers` is the converted Phase 3 blocker list from the gated plan.

2. Add a narrow redo-retention gate.
   - Add an async-friendly gate or lease owned by `TransactionSystem`.
   - Keep the critical section deliberately small but action-complete:
     - catalog checkpoint holds the gate while it reads the marker-based
       checkpoint snapshot, discovers/scans redo, and applies the checkpoint
       batch;
     - redo truncation holds the gate while it computes the plan, publishes the
       marker, and unlinks files below the final marker.
   - Do not use a blocking lock across async catalog checkpoint or marker
     publication awaits. Use an existing async latch/gate pattern or a small
     lease type that is safe to hold across awaits.
   - The gate is a consistency simplifier. It prevents catalog checkpoint from
     scanning with an old marker while truncation has already unlinked files
     under a newer marker.

3. Add production marker publication.
   - Add a helper such as
     `CatalogStorage::publish_first_redo_log_seq(first_redo_log_seq)`.
   - The helper must read the current `catalog.mtb` snapshot, preserve
     `catalog_replay_start_ts`, `next_table_id`, and catalog table roots, and
     publish only the advanced marker.
   - Reject or no-op marker regressions; implementation may treat
     `requested <= current` as no-op if that keeps retry behavior simple.
   - Reuse the metadata-only catalog publication path:
     - fork `MutableMultiTableFile`;
     - apply current checkpoint metadata;
     - update `new_root.root.first_redo_log_seq`;
     - reclaim the displaced meta block;
     - commit the prepared root.
   - If this helper returns `ErrorKind::Io`, the truncation executor must poison
     storage with `FatalError::CheckpointWrite` and must not unlink redo files.

4. Add redo file cleanup helpers.
   - Add helper code in the redo/log boundary to build or discover redo file
     paths by sequence using the configured `file_prefix`.
   - Reuse existing file-name validation where practical. Do not weaken current
     invalid family-name handling in retained-suffix discovery.
   - Add a helper to list present parsed redo sequence files below a marker
     without requiring the obsolete prefix to be contiguous.
   - The cleanup path must never remove a file whose sequence is greater than
     or equal to the final durable marker.
   - `NotFound` while unlinking is normal and increments
     `already_missing_files`.
   - Any other unlink error increments `failed_unlink_files` and leaves the
     file for a later retry.

5. Add the internal truncation executor.
   - Add an async method such as:

```rust
impl TransactionSystem {
    pub(crate) async fn truncate_redo_log(&self) -> Result<RedoTruncationOutcome>;
}
```

   - The method should:
     1. acquire the redo-retention gate;
     2. call `plan_redo_truncation()`;
     3. derive `target_marker` from the last candidate when candidates exist,
        otherwise keep the current plan marker;
     4. publish `target_marker` before unlink when `target_marker` advances;
     5. discover and remove all present redo sequence files below
        `target_marker`, including newly planned candidates and stale leftovers
        below the previous marker;
     6. return a `RedoTruncationOutcome`.
   - If the plan has no candidates, still run below-marker cleanup using the
     current durable marker so previous partial unlink failures can be retried.
   - Keep Phase 3 blockers in the outcome even when some below-marker cleanup
     succeeds without marker advancement.
   - Do not run catalog checkpoint, table checkpoint, or any background
     scheduling work from this method.

6. Add the public session API.
   - Add:

```rust
impl Session {
    pub async fn truncate_redo_log(&mut self) -> Result<RedoTruncationOutcome>;
}
```

   - Use `self.pin("truncate redo log")` so this mutating maintenance operation
     fails after storage poison.
   - Reject active session transactions with `OperationError::NotSupported` and
     an explanatory attachment.
   - Call `session.engine.trx_sys.truncate_redo_log().await`.
   - Do not expose a public dry-run method or accept a caller-provided plan.

7. Keep docs synchronized.
   - Update `docs/redo-log.md` to state that `Session::truncate_redo_log` can
     physically unlink whole sealed prefix files after marker publication.
   - Mention the aggregate unlink outcome and retryable non-`NotFound` unlink
     failures.
   - During task resolve, sync RFC 0022 Phase 4 with task path, issue id,
     status, implementation summary, and any final outcome/gate naming changes.

## Implementation Notes

## Impacts

- `doradb-storage/src/session.rs`
  - public `Session::truncate_redo_log`;
  - public outcome/blocker types if colocated with session maintenance;
  - admission and active-transaction tests.
- `doradb-storage/src/lib.rs`
  - re-export `RedoTruncationOutcome` and `RedoTruncationBlockerInfo`.
- `doradb-storage/src/trx/sys.rs`
  - redo-retention gate ownership;
  - internal async truncation executor;
  - marker-publication error conversion;
  - access to config file prefix.
- `doradb-storage/src/trx/retention.rs`
  - conversion from crate-private Phase 3 blockers/candidates to public
    outcome information;
  - possible helper placement for cleanup planning.
- `doradb-storage/src/catalog/checkpoint.rs`
  - acquire the redo-retention gate around marker-based checkpoint scan/apply.
- `doradb-storage/src/catalog/storage/mod.rs`
  - production marker publication helper.
- `doradb-storage/src/file/multi_table_file.rs`
  - possible shared helper for marker-only metadata publication if the
    catalog-storage helper should delegate closer to the file layer.
- `doradb-storage/src/log/mod.rs`
  - redo path/list helpers for cleanup below a durable marker;
  - tests for below-marker listing/path behavior if helper logic is nontrivial.
- `docs/redo-log.md`
  - update physical truncation documentation.
- `docs/rfcs/0022-catalog-backed-redo-log-truncation.md`
  - resolve-time Phase 4 synchronization.

## Test Cases

Run:

```bash
cargo nextest run -p doradb-storage
```

If implementation changes backend-neutral file IO beyond ordinary filesystem
unlink/path handling, also run:

```bash
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

Add focused tests covering:

1. `Session::truncate_redo_log` with an active session transaction returns
   `OperationError::NotSupported`.
2. `truncate_redo_log` uses normal mutating admission and fails after injected
   storage poison.
3. A plan with no candidates returns unchanged marker, zero `advanced_files`,
   blockers, and no marker publication.
4. Eligible sealed prefix files advance `first_redo_log_seq` to
   `last_candidate_seq + 1`.
5. After successful marker advancement, the candidate redo files are physically
   removed.
6. Restart succeeds after truncation when removed files are below the durable
   marker.
7. Recovery/discovery still fails when a file at or above
   `first_redo_log_seq` is missing.
8. Sealed empty candidate files are removed when they are in the eligible
   prefix.
9. An unsealed file is reported as a blocker and is never removed.
10. A catalog floor blocker is reported when catalog progress has not proven a
    segment safe.
11. Live-table and pending-dropped-table floor blockers are converted into
    public blocker info with table ids and replay floors.
12. If marker publication fails, no redo files are unlinked.
13. `NotFound` during unlink is counted as `already_missing_files` and the call
    still succeeds.
14. A non-`NotFound` unlink failure is counted as `failed_unlink_files`, does
    not poison storage, and leaves the file retryable.
15. A later `truncate_redo_log` call removes an existing obsolete file below
    the already durable marker even when there are no new candidates.
16. Catalog checkpoint and truncation serialize through the redo-retention gate;
    a checkpoint scan must not use an old marker while truncation has already
    unlinked files under a newer marker.
17. Existing `Session::checkpoint_catalog` behavior and tests continue to pass.

Before review or resolve, run:

```bash
cargo fmt
cargo clippy -p doradb-storage --all-targets -- -D warnings
git diff --check
tools/style_audit.rs --diff-base origin/main
```

## Open Questions

None for this task. Future work may add per-file public unlink details,
background redo retention scheduling, richer retention telemetry, or automatic
idle-table checkpoint hints, but those are intentionally outside RFC 0022
Phase 4.
