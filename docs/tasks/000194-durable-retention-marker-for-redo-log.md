---
id: 000194
title: Durable Retention Marker For Redo Log
status: proposal
created: 2026-06-26
github_issue: 769
---

# Task: Durable Retention Marker For Redo Log

## Summary

Implement RFC 0022 Phase 1 by persisting a durable first-retained redo file
sequence in `catalog.mtb` and making redo discovery honor that marker. Recovery
and catalog checkpoint scan must accept intentionally removed prefix files below
the marker while continuing to reject any missing redo file at or above the
marker as corruption.

## Context

RFC 0022 adds catalog-backed physical redo truncation in phases. This task is
only Phase 1, "Durable Retention Marker And Discovery." It creates the durable
recovery contract needed by later truncation work, but it does not physically
remove redo files or expose a truncation API.

Issue Labels:
- type:task
- priority:medium
- codex

Parent RFC:
- `docs/rfcs/0022-catalog-backed-redo-log-truncation.md`

Target RFC Phase:
- Phase 1: Durable Retention Marker And Discovery

Phase prerequisites relied on:
- Catalog storage is already available before transaction-system redo replay
  planning. `TrxSysConfig::prepare` receives a `Catalog` that has loaded
  `catalog.mtb`, and `prepare_recovery` can read the catalog checkpoint
  snapshot before discovering redo files.

Phase-local choices resolved:
- Persist the marker as `first_redo_log_seq: u32`.
- Encode it in the existing `u32 reserved` slot in the `catalog.mtb`
  `MultiTableMetaBlockData` payload, immediately after `table_count`.
- Keep `CATALOG_MTB_VERSION` unchanged because existing meta blocks serialize
  this slot as zero, which is the marker default that preserves current
  behavior.
- Keep production marker advancement out of this phase. A narrow test-only
  helper may be added to publish nonzero markers for recovery tests.

Following-phase contract preserved:
- Phase 2 can assume the persisted marker and marker-aware discovery contract
  exists.
- This task does not add catalog scan segment observations, in-memory retention
  progress, marker advancement, truncation planning, file unlink, or public
  maintenance APIs.
- No RFC phase-plan edits are planned for design. During task resolve, sync RFC
  0022 Phase 1 task path, issue id, status, and implementation summary.

Current behavior:
- `discover_redo_log_files` requires a non-empty redo family to be contiguous
  from `00000000`; a missing prefix or internal gap is
  `DataIntegrityError::RedoLogSequenceGap`.
- `catalog.mtb` currently persists `next_table_id`, catalog logical-table root
  descriptors, and an allocation map; the payload has a serialized reserved
  `u32` that is always written as zero.
- Startup recovery and catalog checkpoint scan both discover redo files before
  constructing a `RedoReplayPlanner`.

## Goals

- Add `first_redo_log_seq` to catalog multi-table metadata and
  snapshots.
- Preserve zero as the default marker for new and existing databases.
- Make startup recovery discover redo using the marker loaded from
  `catalog.mtb`.
- Make catalog checkpoint redo scan use the same marker-aware discovery policy.
- Treat valid redo files below the marker as optional obsolete prefix files and
  exclude them from replay/scan planning.
- Reject an empty discovered suffix when the marker is nonzero.
- Reject a missing file exactly at the marker and any sequence gap above the
  marker.
- Keep all changes narrow and recovery-focused so later RFC phases can build on
  the marker contract.

## Non-Goals

- Do not unlink, delete, truncate, or hole-punch redo files.
- Do not add `Session::truncate_redo_log`.
- Do not add a public or production internal marker advancement API.
- Do not compute global truncation floors.
- Do not retain dropped-table replay floors.
- Do not add catalog scan segment progress observations or retention caches.
- Do not add a background redo retention worker or scheduling policy.
- Do not change redo super-block format or transaction redo payload format.

## Plan

1. Extend catalog multi-table metadata.
   - Add `first_redo_log_seq: u32` to
     `MultiTableMetaBlock` in `doradb-storage/src/file/multi_table_file.rs`.
   - Initialize it to `0` in `MultiTableMetaBlock::new`.
   - Ensure `MultiTableFileSnapshot` carries the field through its cloned
     `meta` payload.
   - Ensure catalog checkpoint metadata updates preserve the inherited marker
     rather than resetting it.

2. Reuse the existing catalog meta-block reserved slot.
   - In `doradb-storage/src/file/meta_block.rs`, rename the reserved `u32`
     field in `MultiTableMetaBlockData` to
     `first_redo_log_seq`.
   - Serialize `MultiTableMetaBlockSerView` as:
     `next_table_id`, `table_count`, `first_redo_log_seq`,
     catalog table root descriptors, then `alloc_map`.
   - Deserialize existing files whose slot is zero as marker `0`.
   - Keep the existing `CATALOG_MTB_VERSION` unless implementation finds this
     reserved-slot reuse is invalid; if so, stop and revisit the task/RFC
     instead of silently changing the compatibility contract.

3. Make redo discovery marker-aware.
   - Change `discover_redo_log_files` in `doradb-storage/src/log/mod.rs` to
     accept `first_retained_file_seq: u32`.
   - Continue rejecting invalid redo-family names and legacy partitioned names.
   - Keep marker `0` behavior equivalent to today.
   - Filter valid sequence files below the marker out of returned descriptors.
   - If `first_retained_file_seq > 0` and no file at or above the marker is
     discovered, return `DataIntegrityError::RedoLogSequenceGap`.
   - Require the first retained descriptor sequence to equal the marker.
   - Require contiguous sequence numbers for all descriptors at or above the
     marker.
   - Preserve descending output behavior after marker filtering.

4. Thread the marker into startup recovery.
   - In `doradb-storage/src/conf/trx.rs`, load
     `resources.catalog.storage.checkpoint_snapshot()?.meta.first_redo_log_seq`
     before calling redo discovery.
   - Pass that marker to `discover_redo_log_files`.
   - Ensure `RedoReplayPlanner` and `RedoLogFinalizer` continue to derive the
     active startup file from the retained discovered suffix and do not assume
     sequence zero exists after a nonzero marker.

5. Thread the marker into catalog checkpoint scan.
   - In `doradb-storage/src/catalog/checkpoint.rs`, ensure
     `scan_checkpoint_batch` passes the snapshot marker into the explicit scan
     path.
   - Adjust `CatalogCheckpointScanConfig` or the
     `scan_checkpoint_batch_with_config` signature so tests can supply a
     nonzero marker without reaching through production internals.
   - Keep the scan replay window based on `catalog_replay_start_ts`; the file
     marker only changes file-family discovery and retained suffix validation.

6. Add minimal test support for nonzero marker publication.
   - Prefer a narrow `#[cfg(test)]` helper close to catalog multi-table file or
     catalog storage tests.
   - The helper may publish a metadata-only `catalog.mtb` root with an advanced
     marker and must preserve `catalog_replay_start_ts`, `next_table_id`, and
     catalog table root descriptors.
   - Do not widen production APIs solely for tests.

7. Update documentation touched by the behavior change.
   - Update `docs/redo-log.md` to describe marker-aware prefix discovery: files
     below `first_redo_log_seq` are intentionally absent/obsolete,
     while the retained suffix remains strictly contiguous.
   - Do not document any public truncation API in this task.

## Implementation Notes

## Impacts

- `doradb-storage/src/file/multi_table_file.rs`
  - `CATALOG_MTB_VERSION`
  - `MultiTableMetaBlock`
  - `MultiTableFileSnapshot`
  - `MultiTableActiveRoot::new`
  - `MutableMultiTableFile::apply_checkpoint_metadata`
  - multi-table file tests
- `doradb-storage/src/file/meta_block.rs`
  - `MultiTableMetaBlockData`
  - `MultiTableMetaBlockSerView`
  - meta-block serde tests
- `doradb-storage/src/log/mod.rs`
  - `discover_redo_log_files`
  - `validate_redo_log_file_sequences`
  - redo discovery tests
- `doradb-storage/src/conf/trx.rs`
  - `TrxSysConfig::prepare_recovery`
  - startup recovery setup tests as needed
- `doradb-storage/src/catalog/checkpoint.rs`
  - `CatalogCheckpointScanConfig`
  - `Catalog::scan_checkpoint_batch`
  - `Catalog::scan_checkpoint_batch_with_config`
  - catalog checkpoint scan tests
- `doradb-storage/src/recovery/mod.rs`
  - recovery integration tests for missing prefix below/at marker
- `docs/redo-log.md`
  - marker-aware redo family discovery description
- `docs/rfcs/0022-catalog-backed-redo-log-truncation.md`
  - resolve-time phase synchronization only

## Test Cases

- `MultiTableMetaBlockSerView` round-trips
  `first_redo_log_seq`.
- A newly created `catalog.mtb` snapshot reports marker `0`.
- A metadata-only catalog checkpoint preserves a nonzero marker.
- With marker `0`, `discover_redo_log_files` still rejects missing
  `00000000`.
- With marker `2`, discovery accepts files `00000002`, `00000003`, ... even
  when `00000000` and `00000001` are absent.
- With marker `2`, discovery excludes existing `00000000` and `00000001` from
  returned descriptors.
- With marker `2`, discovery rejects an empty retained suffix.
- With marker `2`, discovery rejects files `00000003`, ... when `00000002` is
  absent.
- With marker `2`, discovery rejects a gap such as `00000002`, `00000004`.
- Startup recovery succeeds when `catalog.mtb` marker is `1` and
  `redo.log.00000000` is absent but the retained suffix is present and valid.
- Startup recovery fails with `RedoLogSequenceGap` when the marker is `1` and
  `redo.log.00000001` is absent.
- Catalog checkpoint scan succeeds with a nonzero marker and no prefix file
  below the marker.

Validation commands:
- `cargo nextest run -p doradb-storage`
- If implementation changes backend-neutral file IO beyond existing
  discovery/snapshot paths, also run
  `cargo nextest run -p doradb-storage --no-default-features --features libaio`.
- Before review or resolve, run
  `cargo clippy -p doradb-storage --all-targets -- -D warnings` and
  `cargo fmt`.

## Open Questions

None.
