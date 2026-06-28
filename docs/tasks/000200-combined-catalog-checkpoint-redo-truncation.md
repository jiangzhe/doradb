---
id: 000200
title: Combined Catalog Checkpoint And Redo Truncation
status: proposal
created: 2026-06-28
github_issue: 785
---

# Task: Combined Catalog Checkpoint And Redo Truncation

## Summary

Add a public session maintenance command that runs catalog checkpoint and redo
log truncation as one lease-aware operation. The combined flow must publish
catalog checkpoint metadata and any advanced `first_redo_log_seq` marker in one
`catalog.mtb` root when both are safe, then release the catalog checkpoint gate
before physical redo file cleanup.

The command returns a composite outcome containing the catalog checkpoint outcome
and the redo truncation outcome. This makes the catalog-checkpoint dependency
explicit without forcing callers to coordinate two maintenance calls that contend
on the same redo-retention observation.

## Context

Issue Labels:
- type:task
- priority:medium
- codex

Source Backlogs:
- docs/backlogs/000136-combine-catalog-checkpoint-redo-truncation.md

Related design context:
- docs/rfcs/0022-catalog-backed-redo-log-truncation.md
- docs/tasks/000197-session-redo-log-truncation-api.md
- docs/tasks/000199-catalog-silent-table-replay-watermarks.md
- docs/checkpoint-and-recovery.md
- docs/table-file.md
- docs/redo-log.md

Current behavior:
- `Session::checkpoint_catalog(&mut self) -> Result<()>` runs a catalog
  checkpoint and refreshes in-memory catalog-safe redo retention progress after a
  successful publish.
- `Session::truncate_redo_log(&mut self) -> Result<RedoTruncationOutcome>`
  separately computes a fresh truncation plan, publishes `first_redo_log_seq`
  before unlink, then cleans files below the durable marker.
- Catalog checkpoint and truncation serialize through the transaction-system
  redo-retention gate. Truncation also holds the catalog checkpoint gate while it
  plans and publishes marker metadata, then drops the catalog gate before
  physical unlink.
- `CatalogStorage::apply_checkpoint_batch` and
  `CatalogStorage::publish_first_redo_log_seq` are separate root-publication
  paths today, so a checkpoint followed by truncation can require two
  `catalog.mtb` metadata publishes.
- Task 000199 moved heartbeat-only table checkpoint floors into
  `catalog.table_replay_silent_watermarks`. Recovery and redo truncation may use
  only the checkpoint-durable overlay cache rebuilt from `catalog.mtb`; committed
  but uncheckpointed silent watermark rows are visible catalog state but are not
  replay-skip or truncation proof.

Chosen design:
- Rename `CatalogCheckpointApplyOutcome` to `CatalogCheckpointOutcome`.
- Make `Catalog::checkpoint_now(&self, trx_sys: &TransactionSystem)` return
  `Result<CatalogCheckpointOutcome>`.
- Keep the existing public `Session::checkpoint_catalog()` API returning
  `Result<()>` for compatibility; it should call `Catalog::checkpoint_now()` and
  discard the outcome.
- Add a public composite outcome for the new combined session command:

```rust
pub struct CatalogRedoMaintenanceOutcome {
    pub catalog_checkpoint: CatalogCheckpointOutcome,
    pub redo_truncation: RedoTruncationOutcome,
}
```

- Add:

```rust
impl Session {
    pub async fn checkpoint_catalog_and_truncate_redo_log(
        &mut self,
    ) -> Result<CatalogRedoMaintenanceOutcome>;
}
```

## Goals

- Add the public combined maintenance API on `Session`.
- Export the public outcome types needed by that API, including
  `CatalogCheckpointOutcome` and `CatalogRedoMaintenanceOutcome`.
- Preserve the existing `Session::checkpoint_catalog` and
  `Session::truncate_redo_log` public behavior.
- Run the combined operation without recursively calling existing public
  operations under non-reentrant gates.
- Compute truncation from the projected post-checkpoint catalog boundary and the
  projected checkpointed silent watermark overlay state.
- Publish catalog checkpoint metadata and `first_redo_log_seq` in one
  `catalog.mtb` root when both advance in the same combined call.
- Preserve marker-before-unlink crash safety: no redo file may be unlinked unless
  the durable marker already excludes it from the retained suffix.
- Keep retained suffix validation strict for files at or above
  `first_redo_log_seq`, while continuing to tolerate partially cleaned obsolete
  prefixes below the marker.
- Release the catalog checkpoint gate before physical redo unlink, while keeping
  redo-retention serialization through cleanup.
- Add focused tests for normal progress, no-op cases, blockers, restart safety,
  marker failure, gate behavior, and silent watermark durability.

## Non-Goals

- Do not make `Session::truncate_redo_log` implicitly run catalog checkpoint.
- Do not change `Session::checkpoint_catalog` to return a public outcome in this
  task.
- Do not add background redo retention scheduling.
- Do not trigger automatic table checkpoints or idle-table checkpoint hints.
- Do not add persistent retention telemetry or public per-file unlink details.
- Do not change redo file format, redo payload format, or `catalog.mtb` marker
  encoding.
- Do not weaken recovery/discovery strictness for files at or above
  `first_redo_log_seq`.
- Do not implement sparse-file hole punching or intra-file truncation.
- Do not introduce a generic maintenance-facts framework.

## Plan

1. Rename and expose the catalog checkpoint outcome.
   - Rename `CatalogCheckpointApplyOutcome` to `CatalogCheckpointOutcome`.
   - Make the enum public because the new public session API returns it through
     the composite outcome.
   - Keep the existing variants:

```rust
pub enum CatalogCheckpointOutcome {
    Published { catalog_replay_start_ts: TrxID },
    Noop,
}
```

   - Add public documentation comments for the enum and variants.
   - Update internal imports and pattern matches in catalog storage/checkpoint
     code.
   - Re-export `CatalogCheckpointOutcome` from the crate root.

2. Return checkpoint outcome from the internal catalog maintenance API.
   - Change `Catalog::checkpoint_now(&self, trx_sys: &TransactionSystem)` to
     return `Result<CatalogCheckpointOutcome>`.
   - Preserve current poison behavior: catalog checkpoint IO write failure still
     becomes `FatalError::CheckpointWrite`.
   - Continue to record catalog redo retention progress only after a successful
     checkpoint publish.
   - Continue to request dropped-table purge after a successful publish.
   - Update `Session::checkpoint_catalog()` to call `checkpoint_now()` and map the
     result to `()`.

3. Add the public composite outcome and session API.
   - Add `CatalogRedoMaintenanceOutcome` near the existing redo maintenance
     outcome types in `session.rs`, or another public module if that better fits
     local style.
   - Re-export it from `lib.rs`.
   - Add `Session::checkpoint_catalog_and_truncate_redo_log`.
   - Use normal mutating session admission with `self.pin(...)`.
   - Reject active session transactions with `OperationError::NotSupported` and a
     clear attachment.

4. Refactor catalog root publication so checkpoint metadata and marker metadata
   can be committed together.
   - Split the common mutable-root work currently embedded in
     `CatalogStorage::apply_checkpoint_batch` and
     `CatalogStorage::publish_first_redo_log_seq`.
   - The shared helper must be able to:
     - start from the current `catalog.mtb` snapshot;
     - apply a valid checkpoint batch into catalog table roots;
     - rebuild catalog allocation state when catalog table blocks changed;
     - reclaim only the displaced meta block for metadata-only publishes;
     - optionally apply a monotonic `first_redo_log_seq` advance;
     - rebuild and install the checkpointed silent watermark cache only after
       commit succeeds.
   - Keep existing standalone checkpoint and marker-publication APIs as wrappers
     or separate paths that preserve their current behavior.

5. Add an internal combined executor.
   - Prefer an internal method owned by `TransactionSystem` or by a small helper
     invoked from `Session`, consistent with existing truncation ownership.
   - Acquire leases in the established order:
     1. catalog checkpoint lease;
     2. redo-retention lease.
   - Recheck runtime health after async gate waits so storage poison observed
     while queued prevents marker publication and unlink.
   - Scan one catalog checkpoint batch from the current catalog replay boundary.
   - Derive the projected catalog checkpoint outcome from the batch.
   - Derive the projected checkpointed silent watermark overlay that would exist
     after the batch publish. This is required so a silent watermark committed
     before the combined call can authorize truncation only when the combined call
     actually checkpoints it.
   - Build a truncation plan against the projected state:
     - projected `catalog_replay_start_ts`;
     - projected effective live-table replay floors;
     - projected pending dropped-table replay floors;
     - retained segment summaries from the current durable marker;
     - catalog-safe segment progress from the just-scanned batch plus existing
       valid cached progress when applicable.
   - Derive `target_marker = last_candidate_seq + 1` when candidates exist;
     otherwise retain the current marker.
   - Publish exactly one prepared `catalog.mtb` root when either checkpoint
     metadata or marker metadata advances.
   - If checkpoint has no publishable work but marker advances, perform a
     marker-only publish through the shared path.
   - If checkpoint publishes but marker does not advance, publish checkpoint
     metadata only.
   - If neither advances, do not publish a new root, but still run below-marker
     cleanup retry.
   - Record any catalog redo retention progress with the final marker and final
     catalog replay boundary so stale old-marker progress is not reused.
   - Drop the catalog checkpoint lease before physical cleanup.
   - Clean present redo files below the final durable marker while the
     redo-retention lease remains held.
   - Return `CatalogRedoMaintenanceOutcome`.

6. Preserve standalone truncation behavior.
   - Leave `TransactionSystem::truncate_redo_log()` using its current fresh-plan
     path against already durable checkpoint state.
   - Do not make it consume projected checkpoint batches.
   - Ensure the shared planning helper cannot accidentally use uncheckpointed
     silent watermark rows in the standalone path.

7. Update documentation.
   - Update `docs/redo-log.md` to document the combined maintenance command and
     composite outcome.
   - Mention that combined maintenance can fold silent watermark rows into
     `catalog.mtb` and then use those newly checkpointed overlays for truncation
     in the same operation.
   - If needed, update `docs/checkpoint-and-recovery.md` to clarify that the
     combined command is only an operational batching of existing durable
     checkpoint/marker contracts, not a new recovery rule.

## Implementation Notes


## Impacts

- `doradb-storage/src/catalog/checkpoint.rs`
  - rename/export catalog checkpoint outcome;
  - make `checkpoint_now` return outcome;
  - add or delegate to combined checkpoint/truncation executor if ownership lands
    here.
- `doradb-storage/src/catalog/storage/mod.rs`
  - factor checkpoint batch application and marker application into a shared
    prepared-publish path;
  - keep standalone checkpoint and marker APIs behavior-compatible;
  - install checkpointed silent watermark cache only after successful commit.
- `doradb-storage/src/file/multi_table_file.rs`
  - likely add or reuse helper methods for applying checkpoint metadata and
    `first_redo_log_seq` to one mutable root.
- `doradb-storage/src/trx/retention.rs`
  - add projected-state truncation planning helper or combined executor;
  - keep current standalone `plan_redo_truncation` and `truncate_redo_log`
    semantics.
- `doradb-storage/src/session.rs`
  - add `CatalogRedoMaintenanceOutcome`;
  - add public combined API and admission tests.
- `doradb-storage/src/lib.rs`
  - re-export new public outcome types.
- `doradb-storage/src/catalog/mod.rs`
  - expose `CatalogCheckpointOutcome` publicly while keeping other catalog
    internals restricted;
  - provide any projected effective-floor helper needed by combined planning.
- `docs/redo-log.md`
  - document combined maintenance.
- `docs/checkpoint-and-recovery.md`
  - optional clarification if implementation needs recovery wording updates.

## Test Cases

Run standard validation:

```bash
cargo nextest run -p doradb-storage
```

Run formatting/lint/style gates before review:

```bash
cargo fmt
cargo clippy -p doradb-storage --all-targets -- -D warnings
git diff --check
tools/style_audit.rs --diff-base origin/main
```

If implementation changes backend-neutral file IO beyond ordinary catalog
metadata root publication and redo prefix unlink handling, also run:

```bash
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

Focused scenarios:

1. `Session::checkpoint_catalog_and_truncate_redo_log` with an active session
   transaction returns `OperationError::NotSupported`.
2. The combined API uses normal mutating admission and fails after injected
   storage poison.
3. `Catalog::checkpoint_now` returns `CatalogCheckpointOutcome::Published` when
   a catalog checkpoint publishes and `Noop` when there is no publishable batch.
4. Existing `Session::checkpoint_catalog` still returns `Result<()>` and existing
   callers/tests continue to pass.
5. Combined call publishes catalog checkpoint metadata and an advanced
   `first_redo_log_seq` in one `catalog.mtb` root when both advance.
6. Combined call returns `CatalogRedoMaintenanceOutcome` with the actual catalog
   checkpoint outcome and redo truncation outcome.
7. A silent table checkpoint committed before the combined call does not affect
   truncation until the combined catalog checkpoint publishes it.
8. In the same combined call, a newly checkpointed silent watermark overlay can
   raise effective table replay floors and allow truncation past old table-root
   floors.
9. If catalog checkpoint publish fails, checkpointed silent watermark cache is not
   swapped, marker is not advanced, and no redo files are unlinked.
10. If marker publication fails within the combined publish, no redo files are
    unlinked and the error poisons storage as `FatalError::CheckpointWrite`.
11. Combined call with checkpoint publish but blocked truncation returns
    checkpoint progress plus redo blockers.
12. Combined call with checkpoint `Noop` but truncation candidates advances marker
    and cleans obsolete files.
13. Combined call with no checkpoint publish and no marker advance still retries
    below-marker cleanup.
14. Restart succeeds after combined checkpoint/truncation removes obsolete prefix
    files.
15. Recovery/discovery still fails when a redo file at or above the final
    `first_redo_log_seq` marker is missing.
16. An unsealed file is reported as a blocker and is never removed.
17. `NotFound` during obsolete-prefix unlink increments `already_missing_files`
    and succeeds.
18. Non-`NotFound` unlink failure increments `failed_unlink_files`, does not
    poison storage, and remains retryable.
19. The combined flow releases the catalog checkpoint gate before physical unlink.
20. The redo-retention gate stays held through cleanup so catalog checkpoint scans
    cannot race disappearing retained redo files.
21. Existing catalog checkpoint, redo truncation, recovery, dropped-table floor,
    and silent watermark tests continue to pass.

## Open Questions

No blocking open questions remain.

The broad maintenance direction remains out of scope for this task:
- background redo retention scheduling;
- automatic idle-table checkpoint scheduling;
- persistent retention telemetry and user-facing metrics;
- generic maintenance-facts infrastructure.
