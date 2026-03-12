---
id: 000063
title: Refine Storage Path Configuration and Restart Contract
status: implemented  # proposal | implemented | superseded
created: 2026-03-12
github_issue: 417
---

# Task: Refine Storage Path Configuration and Restart Contract

## Summary

Clarify the storage-engine filesystem contract by replacing the current
distributed path-joining behavior with one central resolver, renaming the
public config knobs to distinguish durable files from ephemeral swap files,
validating all configured paths up front, and persisting a durable-layout
marker that rejects incompatible restart configuration before recovery begins.

## Context

The storage engine already has a `main_dir` concept, but the current contract
is still implicit and uneven:

1. `EngineConfig::build` applies the root directory separately to
   `TableFileSystemConfig`, `EvictableBufferPoolConfig`, and `TrxSysConfig`
   through three different `with_main_dir` helpers.
2. Table and catalog files already have a partial contract:
   `TableFileSystemConfig` derives user table file names from `base_dir` and
   validates `catalog_file_name`, but the log prefix and buffer swap path are
   still unchecked strings.
3. The row-page swap file is opened with `create_or_trunc`, so a bad path can
   silently clobber unrelated files.
4. Restart behavior is currently split across two independent configuration
   inputs:
   - `catalog.mtb` plus table files under the table-file subsystem
   - redo logs discovered from `log_file_prefix`
   Recovery has no persisted contract that says whether a new config still
   points at the same durable storage layout.
5. Backlog `000008` calls out the need to audit all storage-path sites,
   document the runtime contract, and add validation/tests to prevent path
   drift.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Source Backlogs:`
`- docs/backlogs/000008-storage-path-configuration-audit.md`

## Goals

1. Replace the implicit `main_dir` fan-out with one central path-resolution and
   validation step owned by engine startup.
2. Rename the public config knobs so their roles are explicit:
   - `main_dir` -> `storage_root`
   - `base_dir` -> `data_dir`
   - `log_file_prefix` -> `log_dir` + `log_file_stem`
   - `file_path` -> `data_swap_file`
3. Change the default row-page swap filename from `databuffer.bin` to
   `data.bin`.
4. Keep the current durable defaults unless otherwise required for compatibility:
   - `data_dir` remains root-relative to `storage_root`
   - `catalog_file_name` remains `catalog.mtb`
   - `log_dir` remains root-relative to `storage_root`
   - `log_file_stem` remains `redo.log`
5. Define and enforce two storage classes:
   - durable layout: `data_dir`, derived table files, `catalog_file_name`,
     `log_dir`, `log_file_stem`, and `log_partitions`
   - ephemeral runtime files: `data_swap_file`
6. Add path validation that rejects absolute paths, parent-directory escape,
   invalid suffixes, and durable/ephemeral overlap.
7. Persist a restart marker file, `storage-layout.toml`, under `storage_root`
   and fail startup early when the durable layout changes incompatibly.
8. Keep table file naming derived from table id and keep all existing file
   formats unchanged.

## Non-Goals

1. Changing table-file, catalog-file, or redo-log record formats.
2. Adding automatic migration tooling for moving an existing store to a new
   durable layout.
3. Designing the future index swap file in full. This task only makes the row
   swap file contract reusable for that future addition.
4. Removing every hardcoded filename from low-level file-format tests that do
   not go through engine configuration. Runtime storage contract work should be
   scoped to actual config/build paths.
5. Refactoring unrelated transaction, checkpoint, or recovery logic beyond what
   is required to enforce the storage path contract.

## Unsafe Considerations (If Applicable)

No new `unsafe` behavior is expected from this task, but implementation will
thread new resolved-path types through subsystems that sit next to existing
low-level file and buffer code.

1. Review should confirm that path-resolution changes do not alter any file-I/O
   lifetime assumptions in:
   - `doradb-storage/src/buffer/evict.rs`
   - `doradb-storage/src/file/mod.rs`
   - `doradb-storage/src/file/table_fs.rs`
   - `doradb-storage/src/trx/log.rs`
2. If implementation moves path ownership into new helper structs that are
   passed into low-level constructors, keep the change at the string/path
   boundary only and avoid widening existing `unsafe` call surfaces.
3. No unsafe inventory refresh is expected unless implementation introduces new
   `unsafe` blocks, which should be avoided in this task.

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Add a central storage-path module, e.g.
   `doradb-storage/src/storage_path.rs`, with:
   - config-facing durable and ephemeral path descriptors
   - a resolved-path struct produced once from `storage_root`
   - validation helpers for root-relative file and directory paths
   - marker read/write helpers for `storage-layout.toml`
2. Refactor `EngineConfig` to own the full path contract.
   - Rename `main_dir` to `storage_root`.
   - Resolve all storage paths before constructing any subsystem.
   - Create required directories from the resolved layout instead of letting
     each subsystem join its own path.
3. Refactor the table-file subsystem config.
   - Rename `TableFileSystemConfig.base_dir` to `data_dir`.
   - Remove `with_main_dir`; `TableFileSystemConfig::build` should take
     resolved absolute paths from the engine-level resolver.
   - Keep `catalog_file_name` as a plain filename under `data_dir`.
4. Refactor redo-log config into directory plus stem.
   - Replace `TrxSysConfig.log_file_prefix` with `log_dir` and
     `log_file_stem`.
   - Keep the concatenated internal prefix only inside the resolved-path layer
     or near `list_log_files` / `create_log_file`.
   - Validate `log_file_stem` as one filename component with no separators and
     no glob metacharacters, because log discovery is glob-based.
5. Refactor the row-page swap file config.
   - Rename `EvictableBufferPoolConfig.file_path` to `data_swap_file`.
   - Change the default from `databuffer.bin` to `data.bin`.
   - Validate `data_swap_file` as a root-relative file path ending with
     `.bin`.
   - Make the validator reusable for a future `index_swap_file` addition.
6. Define explicit overlap and restart rules.
   - Reject any path that escapes `storage_root` or is absolute when a
     root-relative path is required.
   - Reject overlap between `data_swap_file` and durable files/directories,
     including `catalog.mtb`, derived table files, and the redo-log family.
   - Persist `storage-layout.toml` with only durable relative layout:
     - `version`
     - `data_dir`
     - `catalog_file_name`
     - `log_dir`
     - `log_file_stem`
     - `log_partitions`
   - Exclude `data_swap_file` from the marker so changing `data.bin` across
     restarts is allowed.
   - Fail startup before recovery when the marker exists and any durable field
     differs.
7. Preserve compatibility for existing pre-marker stores.
   - On first startup with no marker, validate the resolved layout and write
     `storage-layout.toml` for future restart checks.
   - Document that a store created before this marker exists cannot detect an
     earlier historical layout change retroactively.
8. Update examples, tests, and docs to the renamed config surface.
   - Example programs should use `data_swap_file`, `log_dir`, and
     `log_file_stem`.
   - Internal tests should stop depending on `with_main_dir` behavior and
     instead validate the resolved storage contract.

## Implementation Notes

1. Added `doradb-storage/src/storage_path.rs` as the single storage-layout
   resolver and validator. It now:
   - normalizes root-relative `data_dir`, `log_dir`, and `data_swap_file`
   - validates `catalog_file_name`, `log_file_stem`, and `.bin` swap-file
     suffixes
   - validates an existing `storage-layout.toml` before startup
   - persists `storage-layout.toml` only after recovery preparation succeeds
   - rejects durable-layout drift on restart while allowing `data_swap_file`
     changes and whole-root relocation
2. Renamed the primary config surface in code and serialization, and removed
   the leftover compatibility builders from the public API:
   - `EngineConfig.main_dir` -> `storage_root`
   - `TableFileSystemConfig.base_dir` -> `data_dir`
   - `TrxSysConfig.log_file_prefix` -> `log_dir` + `log_file_stem`
   - `EvictableBufferPoolConfig.file_path` -> `data_swap_file`
   - removed `with_main_dir` / `with_storage_root` helpers from
     `TableFileSystemConfig` and `EvictableBufferPoolConfig`
3. Updated engine startup and transaction-system initialization to resolve
   storage paths once, validate marker compatibility early, prepare recovery,
   persist the durable-layout marker, and only then start transaction
   background threads.
4. Updated the table-file, buffer-pool, and redo-log configs/builders, plus
   direct subsystem tests, to use explicit `storage_root`, `data_dir`,
   `data_swap_file`, `log_dir`, and `log_file_stem` settings while keeping the
   existing low-level subsystem behavior unchanged.
5. Updated benchmark/example entry points to use the renamed swap/log config
   knobs and the new `data.bin`-style naming.
6. Added coverage for:
   - `storage_path` validation and marker mismatch
   - failed startup does not persist `storage-layout.toml`
   - engine restart with changed `data_swap_file`
   - engine restart with durable-layout mismatch
   - engine restart after storage-root relocation
7. Addressed review follow-up after the initial implementation:
   - delayed marker persistence so failed startup attempts do not pin an empty
     or partial storage root to the wrong durable layout
   - removed the remaining stale compatibility APIs (`main_dir`, old swap-file
     builder alias, and per-subsystem root-joining helpers) instead of keeping
     ambiguous duplicate path semantics
8. Verification:
   - `cargo check -p doradb-storage --no-default-features --all-targets`
   - `cargo test -p doradb-storage --no-default-features`
   - pre-commit `cargo fmt`
   - pre-commit `cargo clippy --all-features --all-targets -- -D warnings`

## Impacts

1. `doradb-storage/src/engine.rs`
   - `EngineConfig`
   - engine startup/build path
2. `doradb-storage/src/storage_path.rs`
   - new central path resolver/validator and marker helpers
3. `doradb-storage/src/file/table_fs.rs`
   - `TableFileSystemConfig`
   - table/catalog path derivation
4. `doradb-storage/src/buffer/evict.rs`
   - `EvictableBufferPoolConfig`
   - row-page swap file open path
5. `doradb-storage/src/trx/sys_conf.rs`
   - `TrxSysConfig`
   - log partition initialization inputs
6. `doradb-storage/src/trx/log.rs`
   - log file naming/discovery helpers
7. `doradb-storage/examples/bench_insert.rs`
8. `doradb-storage/examples/bench_block_index.rs`
9. `doradb-storage/examples/multi_threaded_trx.rs`
10. recovery and startup tests under:
    - `doradb-storage/src/trx/recover.rs`
    - `doradb-storage/src/trx/sys.rs`
    - `doradb-storage/src/catalog/mod.rs`
11. config serialization and smoke tests for renamed fields and marker behavior

## Test Cases

1. Fresh startup with default config creates `storage-layout.toml` and uses
   `data.bin` as the default row-page swap file.
2. Fresh startup with a custom valid `data_swap_file` ending in `.bin`
   succeeds.
3. `data_swap_file` validation rejects:
   - wrong suffix
   - absolute paths
   - parent-directory escape
   - collision with `catalog.mtb`
   - collision with the redo-log directory/family
4. `data_dir` and `log_dir` validation reject absolute or escaping relative
   paths.
5. `catalog_file_name` validation continues to reject non-`.mtb` names or
   nested paths.
6. `log_file_stem` validation rejects separators and glob metacharacters so
   restart log discovery stays deterministic.
7. Restart with identical durable layout succeeds and recovery behavior is
   unchanged.
8. Restart with changed `data_swap_file` succeeds because the swap file is
   ephemeral and excluded from the marker.
9. Restart with changed durable layout fails before recovery when any of these
   differ:
   - `data_dir`
   - `catalog_file_name`
   - `log_dir`
   - `log_file_stem`
   - `log_partitions`
10. Relocating the whole storage tree to a different `storage_root` while
    keeping the same relative durable layout succeeds, because the marker stores
    relative durable paths rather than absolute host paths.
11. Existing restart/recovery tests continue to pass after the config-surface
    rename.

## Open Questions

1. The future `index_swap_file` should reuse the same `.bin` validator and
   overlap rules introduced here, but that additional config knob remains a
   follow-up outside this task.
