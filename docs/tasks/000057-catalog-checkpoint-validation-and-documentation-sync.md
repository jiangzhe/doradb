---
id: 000057
title: Catalog Checkpoint Validation and Documentation Sync
status: implemented  # proposal | implemented | superseded
created: 2026-03-10
github_issue: 402
---

# Task: Catalog Checkpoint Validation and Documentation Sync

## Summary

Implement RFC-0006 phase 10 as a validation-and-doc-sync task over the
already-implemented catalog foundation, checkpoint, and recovery work. Add the
missing targeted regression coverage for id/file invariants, catalog checkpoint
publish semantics, checkpoint-aware restart, and replay-cutoff behavior, then
update core docs to reflect the implemented `catalog.mtb` and
`catalog_replay_start_ts` contract.

## Context

RFC-0006 phases 1 through 9 are already implemented across catalog storage,
checkpoint publish, readonly checkpoint reads, and recovery bootstrap:
1. `doradb-storage/src/catalog/mod.rs` already covers user object-id boundary,
   `catalog.mtb` bootstrap, allocator restart monotonicity, and ad-hoc catalog
   checkpoint flows.
2. `doradb-storage/src/catalog/storage/checkpoint.rs` already covers readonly
   checkpoint reads and append-focused tail-merge behavior.
3. `doradb-storage/src/file/table_fs.rs` and
   `doradb-storage/src/file/multi_table_file.rs` already cover unified catalog
   file naming, user-table hex naming, and CoW meta-page publish semantics.
4. `doradb-storage/src/trx/recover.rs` already covers checkpoint-aware catalog
   bootstrap and one end-to-end replay-floor case for user-table data
   checkpoint recovery.

The remaining work for phase 10 is not a new storage design. It is to validate
that these pieces hold together under the implemented contract and to align the
living docs with the current behavior:
1. Core docs such as `docs/architecture.md` and
   `docs/checkpoint-and-recovery.md` still describe the generic table-level
   model but do not yet explain the unified catalog persistence file and
   replay-start contract now used by RFC-0006.
2. Validation is still somewhat fragmented by phase and module; phase 10 should
   explicitly close the gap for implemented create/checkpoint/recover behavior.
3. RFC-0006 phase 10 currently mentions lifecycle ordering including `drop`,
   but the current implementation surface does not expose a public drop-table
   flow. That broader lifecycle work should be treated as future RFC scope
   rather than pulled into this task.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Parent RFC:`
`- docs/rfcs/0006-cache-first-unified-catalog-storage-refactor.md`

## Goals

1. Validate the implemented RFC-0006 behavior end to end for phases 1 through
   9, especially object-id boundaries, file naming, cache-first catalog
   bootstrap, catalog checkpoint publish, and checkpoint-aware restart.
2. Add missing targeted regressions around catalog checkpoint scan/apply
   semantics and replay-start advancement without introducing new catalog DDL
   behavior.
3. Update core storage docs so they describe the implemented `catalog.mtb` and
   `catalog_replay_start_ts` model rather than only the earlier generic
   checkpoint narrative.
4. Update RFC-0006 phase 10 linkage and wording so the phase matches the
   actual validation-only scope.

## Non-Goals

1. No `DropTable` API, lifecycle design, replay policy, or recovery validation.
   That belongs in a future RFC/program task.
2. No new catalog checkpoint worker, periodic scheduling, dirty-event wakeup,
   or redo-maintenance/truncation work.
3. No new on-disk format, metadata fields, or checkpoint algorithm changes.
4. No catalog secondary-index checkpoint persistence.
5. No expansion into future schema-evolution DDL or cross-version migration
   work.

## Unsafe Considerations (If Applicable)

No new `unsafe` scope is expected. This task should stay primarily within test
coverage and documentation updates over existing safe orchestration code.

If implementation ends up touching layout-sensitive or cache-sensitive modules
while tightening validation, review must confirm there is no safety-contract
change in:
1. `doradb-storage/src/file/multi_table_file.rs`
2. `doradb-storage/src/file/meta_page.rs`
3. `doradb-storage/src/buffer/readonly.rs`

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Extend validation in the modules that already own the implemented behavior
   rather than creating a new cross-cutting test harness:
   - `doradb-storage/src/catalog/mod.rs`
   - `doradb-storage/src/catalog/storage/checkpoint.rs`
   - `doradb-storage/src/trx/recover.rs`
   - `doradb-storage/src/trx/sys.rs`
2. Tighten the validation matrix for implemented invariants:
   - user object ids start at `USER_OBJ_ID_START` and remain monotonic across
     restart/checkpoint;
   - catalog persistence lives in `catalog.mtb` while user tables use
     fixed-width hex file names;
   - catalog checkpoint publish advances `catalog_replay_start_ts` to
     `safe_cts + 1`;
   - noop/heartbeat checkpoint flows do not regress persisted roots;
   - startup loads checkpointed catalog rows before replay and then uses
     replay-floor filtering for user tables.
3. Add one targeted regression for catalog checkpoint batch scan semantics in
   `doradb-storage/src/trx/sys.rs` so durable upper bound and replay-start
   semantics are validated directly at the scan layer, not only indirectly
   through higher-level restart tests.
4. Update documentation to reflect the implemented model:
   - `docs/architecture.md`: explain unified catalog persistence in
     `catalog.mtb` with cache-first runtime reads;
   - `docs/checkpoint-and-recovery.md`: describe the current catalog
     replay-start boundary and its interaction with user-table heap recovery
     watermarks;
   - `docs/rfcs/0006-cache-first-unified-catalog-storage-refactor.md`: attach
     this task doc to phase 10 and narrow the phase wording away from
     unimplemented drop-table lifecycle validation.

## Implementation Notes

1. Added scan-bound, restart, and mixed-state checkpoint tests:
   - `doradb-storage/src/trx/sys.rs` now has direct scan-layer coverage for
     durable upper bound handling and replay-start advancement across repeated
     catalog checkpoint scans;
   - `doradb-storage/src/catalog/mod.rs` now covers heartbeat catalog
     checkpoint behavior when user tables have mixed data-checkpoint states;
   - `doradb-storage/src/trx/recover.rs` now covers both single-table
     post-cutoff heap replay and two-table mixed checkpoint/replay restart
     recovery.
2. Mixed-state recovery validation exposed and fixed a real bootstrap issue:
   - preloaded user-table row-block indexes now seed their in-memory start row
     boundary from the persisted `pivot_row_id` instead of always starting at
     `0`;
   - the fix lives in `doradb-storage/src/index/row_block_index.rs` and
     `doradb-storage/src/index/block_index.rs`, and prevents post-checkpoint
     row-page replay from allocating the wrong row-id range after restart.
3. Synced the living docs and RFC to the implemented contract:
   - `docs/architecture.md` now describes unified catalog persistence in
     `catalog.mtb`;
   - `docs/checkpoint-and-recovery.md` now documents
     `catalog_replay_start_ts`, coarse replay-floor calculation, and
     checkpoint-aware restart sequencing;
   - RFC-0006 phase 10 is now linked to this task and narrowed to implemented
     create/checkpoint/recover validation only.
4. Verification executed in this resolve pass:
   - `cargo fmt --all`
   - `cargo test -p doradb-storage test_catalog_checkpoint_scan_respects_upper_bound_and_replay_start`
   - `cargo test -p doradb-storage test_log_recover_replays_post_checkpoint_heap_redo_after_bootstrap`
   - `cargo test -p doradb-storage test_log_recover_skips_pre_checkpoint_table_redo_and_rebuilds_persisted_index`
   - `cargo test -p doradb-storage test_catalog_checkpoint_now_heartbeat_with_mixed_user_table_checkpoint_states`
   - `cargo test -p doradb-storage test_log_recover_handles_mixed_user_table_checkpoint_states`
   - `cargo test -p doradb-storage --no-default-features test_catalog_checkpoint_scan_respects_upper_bound_and_replay_start`
   - `cargo test -p doradb-storage --no-default-features test_log_recover_replays_post_checkpoint_heap_redo_after_bootstrap`
   - `cargo test -p doradb-storage --no-default-features test_catalog_checkpoint_now_heartbeat_with_mixed_user_table_checkpoint_states`
   - `cargo test -p doradb-storage --no-default-features test_log_recover_handles_mixed_user_table_checkpoint_states`
5. Delivery tracking:
   - task issue: `#402`
   - implementation PR: `#403`
   - no `Source Backlogs:` entries were recorded for this task, so no backlog
     close action was required during resolve
   - no additional follow-up backlog doc was created in this resolve pass

## Impacts

1. `doradb-storage/src/catalog/mod.rs`
2. `doradb-storage/src/trx/sys.rs`
3. `doradb-storage/src/trx/recover.rs`
4. `doradb-storage/src/index/block_index.rs`
5. `doradb-storage/src/index/row_block_index.rs`
6. `docs/architecture.md`
7. `docs/checkpoint-and-recovery.md`
8. `docs/rfcs/0006-cache-first-unified-catalog-storage-refactor.md`

## Test Cases

1. User object-id boundary and allocator persistence:
   - `is_catalog_obj_id` / `is_user_obj_id` split exactly at
     `USER_OBJ_ID_START`;
   - `next_user_obj_id` remains monotonic across restart and after catalog
     checkpoint publish.
2. File naming and persistence topology:
   - new cluster bootstrap creates `catalog.mtb` and does not create legacy
     catalog `0.tbl..3.tbl` files;
   - user tables persist to fixed-width hex file names.
3. Catalog checkpoint publish semantics:
   - a non-empty checkpoint publishes a new root and advances
     `catalog_replay_start_ts` to `safe_cts + 1`;
   - repeated scan/apply with no new catalog ops does not duplicate replayed
     work or regress roots;
   - heartbeat checkpoint without catalog-row changes advances replay-start
     metadata while preserving table roots.
   - heartbeat catalog checkpoint still follows the same replay-start/root
     invariants when one user table is already data-checkpointed and another
     remains replay-backed.
4. Catalog checkpoint read-path regressions:
   - persisted catalog checkpoint reads continue to use the readonly-cache path;
   - append-focused tail merge can rewrite the last payload without creating an
     unnecessary new index entry when capacity allows.
5. Restart and recovery behavior:
   - restart after a nonzero catalog checkpoint bootstraps checkpointed catalog
     rows before redo replay;
   - restart after catalog checkpoint plus user-table data checkpoint skips
     pre-checkpoint heap redo and still rebuilds visible/indexable state
     correctly from persisted data plus post-cutoff replay;
   - restart with multiple user tables in mixed checkpoint states restores
     checkpointed-table data from persisted state and replay-backed-table data
     from redo in the same recovery run;
   - the coarse replay floor remains bounded by `catalog_replay_start_ts` plus
     loaded user-table `heap_redo_start_ts` values.
6. Catalog checkpoint scan semantics:
   - scan respects the supplied durable upper bound;
   - repeated scan after apply restarts from the updated replay boundary and
     does not return duplicated catalog operations.

## Open Questions

1. Broader lifecycle ordering, especially public `DropTable` behavior across
   checkpoint/recovery boundaries, remains future RFC scope rather than a
   follow-up on this resolved validation task.
2. Validation remains distributed across existing owner modules. If the
   multi-table scenario matrix grows beyond the current common cases, consider
   a small cleanup or matrix-expansion follow-up instead of refactoring this
   resolved task into a shared test harness.
