---
id: 000123
title: Remove Recovery Skip Option
status: proposal
created: 2026-04-18
github_issue: 568
---

# Task: Remove Recovery Skip Option

## Summary

Remove the transaction-system `skip_recovery` configuration path and make
engine startup always run redo recovery before serving traffic. The storage
engine's durability model depends on loading checkpointed catalog/user-table
state and replaying redo to rebuild hot rows, deletion buffers, secondary-index
MemTree state, and the transaction timestamp watermark. A public startup option
that bypasses recovery can admit operations on stale or partially recovered
state, so it should be deleted instead of patched around.

The implementation should avoid unnecessary test churn. Most current
`.skip_recovery(true)` call sites create exactly one engine on a fresh temporary
root; after recovery becomes mandatory, those cases should behave the same over
an empty log family and should be migrated by deleting the call. Add special
handling only where verification shows a call site reopens an existing root/log
family, starts transaction-system components manually, or otherwise changes
behavior when recovery is no longer bypassed.

## Context

Doradb uses a No-Steal / No-Force persistence model. Table files provide
checkpointed cold state through CoW roots, while redo recovery reconstructs the
missing hot runtime state before service. The recovery docs define startup as:
load checkpointed catalog state, reload checkpointed user tables, load table
file roots, then replay redo after the computed replay floor.

`TrxSysConfig` currently exposes `skip_recovery` as a normal configuration
field and builder method. `TrxSysConfig::log_partition_initializer` uses the
flag to avoid opening existing logs, and `TrxSysConfig::prepare` forwards the
flag into `log_recover`. When skip mode is true, `log_recover` does not call
`LogRecovery::recover_all`; it returns a minimum transaction timestamp seed and
finishes log partition initialization directly. This is the unsafe behavior
reported by the source backlog.

Initial call-site analysis found `41` `.skip_recovery(true)` uses:

1. Fresh single-engine tests and helpers that should only need direct deletion:
   transaction-system, purge, table, row-page-index, catalog storage, buffer,
   file-system, and redo failure tests using `TempDir`.
2. Fresh single-engine examples/benchmarks using `TempDir`, such as readonly
   single-miss latency and column runtime lookup, that should only need direct
   deletion.
3. A small number of call sites requiring verification:
   - `engine.rs::test_unstarted_transaction_system_shutdown_is_safe` builds one
     engine and then manually calls `TrxSysConfig::prepare` for an unstarted
     transaction system over a separate empty log directory.
   - `trx/log.rs::test_log_merger` writes logs, drops the first engine, and
     then reopens log streams directly through transaction config.
   - examples/benchmarks that default to the current working directory and
     user-provided redo-log paths may encounter existing logs after recovery is
     mandatory.

Issue Labels:
- type:task
- priority:high
- codex

Source Backlogs:
- docs/backlogs/000088-remove-recovery-skip-option.md

Related prior work:
- docs/tasks/000120-secondary-index-runtime-access-and-recovery.md
- docs/rfcs/0014-dual-tree-secondary-index.md

## Goals

1. Remove `skip_recovery` from `TrxSysConfig`.
2. Remove the `TrxSysConfig::skip_recovery(...)` builder method.
3. Remove `DEFAULT_SKIP_RECOVERY` and its public export.
4. Make transaction-system startup always discover redo logs for each
   configured log partition.
5. Make `log_recover` always execute recovery before creating serviceable log
   partitions.
6. Ensure transaction timestamp initialization always uses recovered checkpoint
   and redo history.
7. Delete redundant `.skip_recovery(false)` call sites.
8. Delete `.skip_recovery(true)` call sites that are fresh single-engine
   setups without adding replacement fixtures.
9. Add only minimal targeted handling for verified non-fresh or multi-startup
   cases.
10. Keep tests and examples compiling after the configuration API removal.
11. Keep documented checkpoint/recovery semantics unchanged.

## Non-Goals

1. Do not refactor the recovery coordinator beyond removing the skip branch.
2. Do not implement parallel recovery or the broader recovery cleanup tracked by
   `docs/backlogs/000087-refactor-recovery-process-parallel-log-replay.md`.
3. Do not change redo log record formats, table-file formats, or catalog
   checkpoint formats.
4. Do not add storage-version compatibility or migration work.
5. Do not add a private test-only engine startup mode that bypasses recovery.
6. Do not create replacement test helpers for fresh single-engine cases unless
   validation proves they are necessary.
7. Do not redesign benchmark storage-root policy beyond minimal changes needed
   to avoid stale-log recovery surprises.

## Unsafe Considerations

No unsafe code changes are planned. The task should touch configuration,
startup/recovery control flow, tests, and examples. If implementation
unexpectedly changes low-level log, mmap, buffer, or page code with unsafe
blocks, document every invariant with `// SAFETY:` and run the unsafe review
checklist before resolving the task.

## Plan

1. Update transaction configuration in `doradb-storage/src/conf/trx.rs`:
   - remove the `skip_recovery` field from `TrxSysConfig`;
   - remove the `skip_recovery` builder method;
   - remove default initialization for the field;
   - update `log_partition_initializer` so it always calls `list_log_files` and
     chooses `LogPartitionMode::Recovery` when logs exist, otherwise
     `LogPartitionMode::Done`;
   - update `prepare` to call `log_recover` without a skip flag.
2. Update constants and exports:
   - remove `DEFAULT_SKIP_RECOVERY` from `doradb-storage/src/conf/consts.rs`;
   - remove the re-export from `doradb-storage/src/conf/mod.rs`.
3. Update recovery in `doradb-storage/src/trx/recover.rs`:
   - remove the `skip` parameter from `log_recover`;
   - always construct a `LogMerger`, add all partition streams, run
     `LogRecovery::recover_all`, and seed the next transaction timestamp from
     recovered metadata/log history.
4. Migrate direct call sites:
   - delete `.skip_recovery(false)` calls;
   - delete `.skip_recovery(true)` calls for fresh single-engine tests and
     examples;
   - simplify helper signatures that only passed `skip_recovery` through to
     config, such as lightweight table-test config helpers.
5. Verify and handle the known nontrivial call sites:
   - run or inspect `test_unstarted_transaction_system_shutdown_is_safe` after
     the unconditional recovery change; adjust only if manual prepare over an
     empty second log directory now needs explicit fresh-directory setup;
   - run or inspect `test_log_merger`; if the old post-drop section was a no-op
     because skip mode selected `Done`, keep the new direct stream reopening and
     add a small assertion rather than adding a skip-like fixture;
   - update examples/benchmarks that default to the current working directory
     only as much as needed to avoid accidentally recovering stale local logs.
6. Keep production behavior simple:
   - do not introduce a new startup policy enum;
   - do not create `#[cfg(test)]` recovery bypass APIs;
   - prefer ordinary temporary roots or direct log readers for test setup.
7. Update docs/comments if any still present recovery skipping as supported.
8. Format and validate.

## Implementation Notes


## Impacts

- `doradb-storage/src/conf/trx.rs`
  - `TrxSysConfig`
  - `TrxSysConfig::log_partition_initializer`
  - `TrxSysConfig::prepare`
- `doradb-storage/src/conf/consts.rs`
  - `DEFAULT_SKIP_RECOVERY`
- `doradb-storage/src/conf/mod.rs`
  - transaction config constant exports
- `doradb-storage/src/trx/recover.rs`
  - `log_recover`
  - startup timestamp seeding through `LogRecovery::recover_all`
- `doradb-storage/src/trx/log.rs`
  - log-reader and log-merger tests
- `doradb-storage/src/engine.rs`
  - transaction-system startup/shutdown tests
- Test modules under transaction, purge, table, row-page-index, catalog,
  buffer, and file-system code that currently configure skip mode.
- Examples under `doradb-storage/examples/` that currently configure skip mode.

## Test Cases

1. Build and test the crate:
   - `cargo nextest run -p doradb-storage`
2. Compile examples after removing the builder API:
   - `cargo check -p doradb-storage --examples`
3. Run focused tests for the nontrivial call sites if the full nextest failure
   output does not already cover them:
   - `cargo nextest run -p doradb-storage test_unstarted_transaction_system_shutdown_is_safe`
   - `cargo nextest run -p doradb-storage test_log_merger`
4. Confirm fresh-root setup still works without skip mode by relying on the
   converted tests that build one engine over `TempDir`.
5. Confirm restart recovery behavior remains covered by existing recovery and
   catalog checkpoint tests after redundant `.skip_recovery(false)` calls are
   removed.
6. Confirm no references remain outside the source backlog and this task doc:
   - search for `skip_recovery`, `skip recovery`, and `DEFAULT_SKIP_RECOVERY`.

## Open Questions

None.
