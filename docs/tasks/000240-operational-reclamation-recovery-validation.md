---
id: 000240
title: Validate Operational Reclamation and Recovery
status: proposal  # proposal | implemented | superseded
created: 2026-07-26
github_issue: 895
---

# Task: Validate Operational Reclamation and Recovery

## Summary

Complete Phase 4 of RFC 0024 by validating the ownership and release boundaries
left after metadata-only history, first-touch transaction binding, and
current-state CREATE INDEX are active together. The implementation should
primarily strengthen deterministic tests and correct live conceptual
documentation. It must not introduce another reclaimer, historical executable
state, or a central production observability layer.

The validation distinguishes five owners that intentionally release at
different boundaries:

1. superseded logical metadata retained by the active-STS horizon;
2. current table and layout runtime retained by transaction bindings;
3. removed index runtime retained by outstanding operational layout/index
   `Arc`s;
4. detached persistent roots retained by table-root reachability until an
   eligible checkpoint rebuilds the allocation map; and
5. dropped-table runtime and replay/file-cleanup floors retained through
   runtime drainage and catalog checkpoint safety.

Prove that retaining a historical `Arc<TableMetadata>` does not retain a
`Table`, `TableRuntimeLayout`, secondary-index runtime, or table-file root.
Prove the inverse as well: an operational runtime pin may delay destruction
without delaying strict-horizon metadata-history removal. Extend binding,
tombstone, checkpoint, restart, and DDL failure tests so no retired runtime can
be newly admitted and no non-MVCC path can reopen a historical state.

## Context

Parent RFC:

- `docs/rfcs/0024-versioned-metadata-immediate-retirement.md`

RFC Phase:

- Phase 4: Operational Reclamation And Recovery Validation

Prerequisite Tasks:

- `docs/tasks/000237-metadata-only-table-history-publication.md`
- `docs/tasks/000238-first-touch-transaction-binding-admission.md`
- `docs/tasks/000239-current-state-create-index-workaround-removal.md`

Issue Labels:

- type:task
- priority:high
- codex

Phase 1 made `TableMetadataVersion` metadata-only. A `UserTableEntry` now keeps
authoritative logical `history` and dropped-table operational state in sibling
slots. The logical slot contains a direct `CurrentTableState`, superseded
`Arc<TableMetadata>` versions, and a terminal tombstone. The operational slot
contains either a retained `DroppedTableOperationalState::Runtime` or a
lightweight `Floor`. Both metadata-history and tombstone removal use the strict
boundary `effective_cts < min_active_sts`, but neither is coupled to runtime
`Arc` counts.

Phase 2 installed `TransactionTableBinding`. A successful first touch hands
statement-owned metadata S to a transaction-owned metadata S grant and stores
the STS-visible metadata together with the current `Arc<Table>` and
`Arc<TableRuntimeLayout>`. A binding hit performs no history or current-state
lookup. Same-table metadata-X DDL therefore drains transactions that touched
the table. An older transaction that never touched the table remains active
but must validate visible and current state on first touch after DDL; a removed
index cannot be admitted, and a terminal table tombstone yields
`SchemaChanged` without installing a binding.

Phase 3 removed CREATE-INDEX-specific historical candidates. Unique and
non-unique indexes now build current committed rows only. Ordinary row undo,
deletion-buffer state, and runtime unique-owner branches remain general MVCC
state, but index creation no longer creates a historical runtime cleanup
obligation.

The existing implementation already exposes most required validation
boundaries:

- `Catalog::resolve_user_table_visible` selects metadata only and its sole
  production consumer is transaction admission.
- `Catalog::resolve_user_table_current`,
  `Catalog::validate_user_table_live`, session maintenance, DDL, checkpoint,
  and recovery use authoritative current state.
- `Table::install_runtime_layout`,
  `Table::has_retired_secondary_indexes`, and
  `Table::cleanup_retired_secondary_indexes` expose Arc-based index retirement.
- `ActiveRoot::alloc_map` and checkpoint reachability expose detached
  `DiskTree` root reclamation.
- `Catalog::user_table_history_version_count`,
  `Catalog::retained_dropped_table_ids_now`, strict horizon helpers, lock
  `debug_snapshot`, and purge `CycleCompleted` events provide test
  observability.
- DROP TABLE already transitions retained runtime to a replay floor after the
  strict horizon and deletes the file only after catalog checkpoint makes the
  absence replay-safe.
- Recovery loads a live table as one CTS-zero current baseline with empty
  volatile history. A replayed committed DROP TABLE retains only its
  operational floor.

Coverage is fragmented, however. The current dropped-index root test does not
retain logical history across runtime and persistent cleanup. CREATE INDEX has
a binding-drain test, while DROP INDEX and DROP TABLE lack matching focused
admission tests. The dropped-table tests do not express both legal orderings
between logical-history GC and operational teardown in one explicit ownership
contract. Restart tests do not jointly assert current-only baseline, empty
history, absent retired runtime, and persisted checkpoint reclamation.

`docs/transaction-system.md` also describes the superseded statement-lifetime
read lock and weak transaction table cache, and says CREATE TABLE takes no
logical lock. The session cache remains a weak hint, but the transaction cache
and CREATE TABLE statements are no longer accurate. In addition,
`docs/garbage-collect.md` says recovery can rebuild reachability without
distinguishing that architectural possibility from the current implementation.
Recovery currently trusts the persisted allocation map; startup validation or
rebuild remains deliberately deferred to
`docs/backlogs/000108-recovery-table-file-alloc-map-rebuild.md`.

## Goals

1. Prove superseded table metadata retains no executable table, layout, index,
   root, or dropped-table runtime resource.
2. Prove a transaction binding is the only pre-DDL foreground owner of the
   current table/layout pair and that transaction end releases its metadata S
   fence.
3. Prove DROP INDEX and DROP TABLE metadata X wait for a bound transaction and
   complete after that transaction commits or rolls back.
4. Prove an older untouched transaction does not delay DDL and cannot newly
   bind a removed index or dropped table after publication.
5. Prove an operational layout/index `Arc` delays retired-index destruction,
   while logical metadata history does not.
6. Prove a dropped secondary `DiskTree` root stays allocated while protected,
   becomes reclaimable through an ordinary eligible checkpoint, and remains
   reclaimed after restart.
7. Prove metadata-history GC and dropped-table Runtime-to-Floor teardown are
   independent and both use the strict horizon boundary.
8. Prove dropped-table file deletion remains catalog-checkpoint-gated after
   runtime destruction and after restart.
9. Prove DDL, explicit table locking, freeze, checkpoint, purge, and recovery
   cannot select a historical live table or historical index runtime.
10. Prove recovery creates one current CTS-zero live baseline with empty
    history and no retired-runtime candidates, or a dropped operational floor
    without logical history for a replayed committed drop.
11. Strengthen pre-commit and post-commit DDL failure-window tests around
    current metadata, history count, runtime-layout generation, root state,
    operational cleanup state, poisoning, and restart reconciliation.
12. Remove or rename test and live-documentation wording that incorrectly
    attributes operational lifetime to an old transaction snapshot or to
    metadata history.
13. Keep all new synchronization deterministic and reuse production waits,
    purge events, root state, and existing failpoints wherever possible.
14. Synchronize RFC 0024 Phase 4 with the task, issue, final status, and
    implementation summary during task resolution.

## Non-Goals

1. Do not introduce a new vacuum, reclamation worker, cleanup manifest, or
   global reclamation coordinator.
2. Do not add a central production `ReclamationSnapshot`, metrics surface, or
   public observability API.
3. Do not make dropped tables or indexes historically executable.
4. Do not change transaction first-touch, binding-cache, stable-index-number,
   `IndexNotFound`, `SchemaChanged`, or stale-writer semantics from Phase 2.
5. Do not change metadata lock modes, lock ordering, transaction-lock handoff,
   DDL publication ordering, or the table-granular DDL drain contract.
6. Do not change the strict active-STS horizon used by root retention,
   metadata history, tombstones, row undo, or dropped-runtime eligibility.
7. Do not add historical table/index roots, per-version allocation maps, or
   runtime pointers to `TableMetadataVersion`.
8. Do not change ordinary row-MVCC candidates, row undo, deletion-buffer
   visibility, unique-owner branches, or MemIndex cleanup.
9. Do not change table-file, catalog-file, root, redo, undo, or checkpoint
   formats.
10. Do not implement or close backlog 000108. In particular, recovery must not
    rebuild an allocation map from the selected root in this task.
11. Do not change dropped-table replay-floor or catalog-checkpoint durability
    policy.
12. Do not add artificial production failure branches solely for testing.
13. Do not encode a production source-text assertion about resolver call
    sites; review the call graph and prove the boundary behaviorally.
14. Do not rewrite completed tasks, closed backlogs, or rejected-alternative
    sections of historical RFCs.
15. Do not add unsafe code.

## Plan

### 1. Fix the ownership and release matrix

Use the following matrix as the implementation and review contract:

| Resource | Owning state | Release gate | Required evidence |
| --- | --- | --- | --- |
| Superseded logical metadata | `TableHistoryEntry::versions` and any returned `Arc<TableMetadata>` | `version.effective_cts < min_active_sts` removes the registry entry; an externally returned metadata Arc may outlive it | history count and retained metadata remain usable without changing table/layout/index owner counts |
| Current executable table/layout | `CurrentTableState::Live` and `TransactionTableBinding` | current replacement/drop plus transaction end for every admitted binding | transaction metadata-S grant, binding presence, and DDL metadata-X waiter |
| Removed secondary-index runtime | table retired-index queue and operational layout/index Arcs | last non-queue operational Arc drops, then `cleanup_retired_secondary_indexes` | retired queue remains with a pinned old layout and drains after that layout drops |
| Detached table-file root/pages | old/current CoW roots and persisted allocation map | protected root crosses the active horizon and a later checkpoint rebuilds reachability | dropped root slot is `SUPER_BLOCK_ID`, allocation bit changes only at checkpoint, and the free bit survives restart |
| Dropped table runtime | `DroppedTableOperationalState::Runtime` | `drop_cts < min_active_sts`, all runtime Arcs drain, and `Arc::try_unwrap`/destroy succeeds | strict equality/after-horizon assertions and Runtime/Floor test state |
| Dropped table file/floor | `DroppedTableOperationalState::Floor` plus purge cleanup queue | `drop_cts < catalog_replay_start_ts` after catalog checkpoint | file and floor remain before checkpoint and disappear after checkpoint/purge |

Do not add a cross-subsystem production snapshot object. Keep assertions in the
module that owns the state. Prefer existing test helpers and direct state
observations. If a deterministic assertion cannot distinguish a dropped
Runtime from a Floor, add only a narrow `#[cfg(test)]` catalog/history helper
that reports that state; do not expose its internal Arcs or widen production
visibility.

Exact `Arc::strong_count` values are not a global lifecycle contract. Preserve
the existing differential assertion that visible metadata resolution does not
increment table/layout/index owner counts, but express cleanup outcomes through
retired queues, root allocation bits, history counts, tombstones, and files.

### 2. Cross-prove metadata-only history and dropped-index cleanup

Extend `metadata_history_resolves_strict_boundaries_and_tombstones` in
`doradb-storage/src/catalog/history.rs` rather than introducing a parallel
metadata model:

1. Keep the existing pointer-identity proof between direct current metadata
   and the installed layout.
2. Keep a `ResolvedVisibleTableMetadata` result and its
   `Arc<TableMetadata>` alive after the selecting transaction ends.
3. Purge its registry history after the strict horizon.
4. Assert the retained metadata remains readable while the history version
   count reaches zero and the table, layout, and index owner counts did not
   change merely because the visible result was created.
5. Keep the terminal tombstone assertions: equality with the drop CTS resolves
   the predecessor; a later STS resolves the tombstone; strict-horizon purge
   removes logical history without removing the independent dropped
   operational entry.

Rename
`test_runtime_layout_install_retires_removed_index_after_old_snapshot_drops`
in `doradb-storage/src/table/layout.rs` to
`test_runtime_layout_install_retires_removed_index_until_pinned_layout_drops`.
The test must describe its owner as an explicitly retained old runtime layout,
not a transaction snapshot. Keep the assertions that cleanup returns zero
while the layout is pinned and one after it is dropped.

Likewise rename
`test_drop_index_runtime_install_retires_removed_runtime` in
`doradb-storage/src/catalog/index.rs` to
`test_drop_index_runtime_install_retires_removed_runtime_until_pinned_layout_drops`.
Retain a historical metadata result while running the test and prove that it
does not delay runtime cleanup; only the explicit old-layout pin does.

### 3. Extend dropped-index root reachability into the cross-owner proof

Extend
`test_checkpoint_reachability_reclaims_dropped_secondary_disk_tree_root` in
`doradb-storage/src/table/persistence.rs`:

1. Build and checkpoint a populated secondary `DiskTree`, then record its page
   id and allocated bit.
2. Begin an older transaction before DROP INDEX but do not touch the table.
   Resolve and retain the transaction's visible logical metadata directly for
   the ownership assertion; do not install a transaction binding.
3. DROP INDEX while that transaction remains active. Assert current metadata
   has the sparse slot inactive, the current root slot is `SUPER_BLOCK_ID`, the
   old root's allocation bit is still set, and logical history still describes
   the dropped index.
4. With no explicit old-layout pin, run retired-index cleanup and assert no
   removed runtime remains merely because logical history or the old
   transaction STS remains.
5. End the old transaction but retain the resolved metadata Arc. Wait through
   the production GC-horizon predicate, request/observe metadata purge, and
   publish the next eligible table checkpoint.
6. Assert history reaches zero and the detached `DiskTree` allocation bit is
   cleared even though the retained metadata still names the old index.
7. Restart from the resulting files and assert the allocation bit remains
   clear. This validates the checkpoint-persisted allocation map; it must not
   pass because recovery recomputed reachability.

Use the active root's `effective_ts()` for the checkpoint readiness wait. Do
not substitute elapsed time or assume the index DDL CTS is the root-retention
fence.

### 4. Complete transaction-binding drainage coverage

Refactor only the admission test module's bounded metadata-X waiter observation
if sharing improves clarity. It should poll the future and inspect
`lock::tests::debug_snapshot`; it must not sleep or treat elapsed time as the
readiness predicate.

Keep `bound_transaction_makes_create_index_metadata_lock_wait` and add:

- `bound_transaction_makes_drop_index_metadata_lock_wait`
- `bound_transaction_makes_drop_table_metadata_lock_wait`

For each new test:

1. Touch the table through a successful foreground read so the transaction has
   a `TransactionTableBinding` and transaction-owned `TableMetadata(S)`.
2. Start DDL in another session and observe its `TableMetadata(X)` waiter while
   the transaction grant remains granted.
3. Assert DDL has not changed current metadata, root/layout state, or table
   lifecycle while waiting.
4. Commit in one test and roll back in the other so both terminal paths cover
   binding/grant release.
5. Await successful DDL completion and assert the waiter and transaction grant
   are absent afterward.

Add untouched-old-transaction cases:

- After DROP INDEX, arrange the untouched transaction so its visible metadata
  contains the index while current metadata has the slot inactive. Its first
  request for that removed index returns `OperationError::SchemaChanged`,
  installs no binding, and retains no statement or transaction metadata/data
  grant. `IndexNotFound` remains reserved for an index absent from the
  transaction's visible metadata.
- After DROP TABLE, first touch returns `OperationError::SchemaChanged`,
  installs no binding, and retains no statement or transaction metadata/data
  grant.

Both DDL operations must finish while the untouched transaction remains active.
The transaction still delays horizon-based logical/physical reclamation until
it ends; it does not delay publication through metadata locking.

### 5. Validate tombstone and dropped-table operational independence

Add direct `UserTableEntry` state-machine tests in
`doradb-storage/src/catalog/history.rs` for both legal cleanup orderings:

1. Publish a terminal drop and assert the current logical state is a tombstone
   while the sibling operational state is Runtime.
2. At `min_active_sts == drop_cts`, assert neither
   `TableHistoryEntry::purge` nor `UserTableEntry::take_dropped_runtime`
   crosses its boundary.
3. With a horizon strictly after `drop_cts`, transition Runtime to Floor and
   assert the tombstone/history remains independently resolvable until its own
   purge.
4. In a separate fixture, purge logical history first and assert the Runtime
   remains available only to purge-owned operational lookup.
5. Remove the final floor and assert `UserTableEntry::is_empty()` becomes true
   only when both slots are absent.
6. Restore a detached runtime in any fixture where cleanup is intentionally
   aborted so the test does not bypass normal resource destruction.

Strengthen the full DROP TABLE integration path in
`doradb-storage/src/catalog/table.rs`:

1. Hold an explicit external `Arc<Table>`, drop the table, cross the active
   horizon, and observe a purge cycle.
2. Assert the pin prevents runtime destruction without reopening foreground
   access; logical history may already be purged.
3. Drop the explicit pin, request another purge cycle, and assert the retained
   state reaches Floor while the file remains.
4. Run catalog checkpoint, observe cleanup, and assert the file, floor, and
   outer catalog-map entry disappear.

Use existing purge completion events. Add a more specific test event only if
`CycleCompleted` plus the narrow operational-state helper cannot establish the
required boundary deterministically.

### 6. Audit and behaviorally validate current-only operational resolution

Preserve the call-path invariant that production
`Catalog::resolve_user_table_visible` is used only by
`trx::admission::resolve_table_binding`. Do not add a source-text unit test for
this invariant.

Review these operational paths and keep them on direct current or explicit
operational state:

- `Catalog::resolve_user_table_current`,
  `Catalog::validate_user_table_live`, and `Catalog::get_table_now`;
- `SessionPin::resolve_user_table` and `SessionPin::lock_table`;
- `Session::freeze_table` and `Session::checkpoint_table`;
- `validated_index_ddl_target` and `validated_drop_table_target`;
- catalog checkpoint and replay-floor collection;
- purge-only `UserTableEntry::runtime_for_purge`;
- recovery bootstrap and DDL replay.

Extend existing post-drop session tests so explicit lock, freeze, and
checkpoint all return `TableNotFound` from the authoritative tombstone/current
absence. They must release any freshly acquired metadata/data guards and leave
no maintenance workflow active. A retained dropped Runtime or Floor must never
make these foreground operations succeed.

Exercise a table with selectable superseded metadata before a live current
state and assert maintenance and subsequent DDL operate on the installed
current layout/root generation. Purge may intentionally resolve a retained
dropped runtime through its operational slot, but it must not use visible
logical history for that purpose.

### 7. Strengthen restart validation without changing recovery

Extend `recovery_builds_one_zero_cts_current_baseline` in
`doradb-storage/src/catalog/history.rs` to perform online CREATE INDEX and DROP
INDEX before restart. After restart, assert:

- current state is live at CTS zero;
- history version count is zero;
- current metadata is pointer-identical to the installed runtime layout;
- the dropped sparse index slot remains inactive in metadata, layout, and the
  selected root;
- no retired secondary-index runtime exists; and
- no pre-crash resolved metadata or transaction binding is reconstructed.

Extend
`test_drop_table_recovery_replays_committed_drop_before_catalog_checkpoint` in
`doradb-storage/src/table/recover.rs`. After restart, assert direct current and
visible resolution are absent, history count is absent, no table runtime is
foreground-resolvable, and exactly the replay/file-cleanup Floor remains. Keep
the file until catalog checkpoint advances beyond the drop CTS, then prove
purge removes both file and retained entry.

The dropped-index reachability test from Plan step 3 must restart only after an
eligible checkpoint persisted the cleared allocation bit. Recovery continues
to trust that persisted map. Do not call a reachability collector during
bootstrap and do not weaken or close backlog 000108.

### 8. Make DDL failure-window assertions ownership-complete

Use test-local before/after snapshots rather than a production observer. For
index DDL, a snapshot should include current effective CTS and metadata
identity, history count, runtime-layout generation and slot presence,
active-root metadata/root slots and allocation state, and retired-runtime
presence. For table DDL, include current/visible catalog presence, history
count, retained dropped state, file existence, lifecycle, and engine poison.

Strengthen the existing failure tests as follows:

#### CREATE TABLE

- Invalid metadata and every pre-commit injected failure
  (`AfterCatalogStaged`, `AfterFilePublished`, and `AfterRuntimeBuilt`) leave no
  current/history entry, no runtime, and no surviving provisional file.
- `PoisonBeforeCatalogCommit` remains fail-closed, preserves the source fatal
  error and provisional file required for recovery cleanup, and publishes no
  executable current/history entry.
- Restart removes an uncommitted provisional file and installs no logical
  history for it.

#### CREATE INDEX

- Build allocation failures, duplicate validation, unsupported primary-index
  requests, and other pre-commit errors leave effective CTS, metadata pointer,
  history count, layout generation, root slots/allocation, and retired-runtime
  state unchanged.
- A post-catalog-commit root/layout/history disagreement keeps the existing
  poison semantics. It must not fall back to a superseded metadata version.
- Existing recovery root-proof tests remain the authority for committed versus
  provisional index DDL; recovered state is one current CTS-zero baseline with
  no volatile history.

#### DROP INDEX

- Active-transaction, primary-index, missing-slot, and other validation errors
  leave the complete index DDL snapshot unchanged.
- Commit or root-publication ambiguity remains fatal and uses the existing
  recovery root proof. Do not synthesize a historical executable index as
  cleanup or fallback.
- A successful publication followed by cleanup failure remains poisoned; the
  committed current metadata/root/layout state stays authoritative on restart.

#### DROP TABLE

- Missing-table, lifecycle, explicit-lock, and other pre-gate errors publish no
  tombstone, history version, or dropped operational state and do not poison.
- Redo/commit failure after the lifecycle gate and abandoned terminal futures
  preserve their source poison. No historical predecessor becomes a
  foreground fallback.
- Restart distinguishes uncommitted drop from committed replay: the former
  reloads one live CTS-zero baseline, while the latter retains only a dropped
  Floor.

Reuse existing storage-backend hooks, CREATE TABLE failpoints, recovery
classification, and publication-invariant poison paths. Add a narrow
`cfg(test)` phase hook only if an existing boundary cannot be triggered
deterministically. Preserve the current rule that no catalog-map guard or
blocking mutex guard spans an async wait; retain the existing other-table
progress tests for DROP TABLE.

### 9. Correct live documentation and misleading test terminology

Update `docs/transaction-system.md` to describe:

- the weak session table cache as a hint only;
- positive transaction-lifetime `TransactionTableBinding` entries;
- cache-before-lock binding hits;
- first-touch statement metadata S followed by failure-atomic handoff to
  transaction metadata S;
- transaction-lifetime metadata S for successfully bound reads and writes;
- visible/current request validation and stale-writer rejection;
- same-table DDL drainage through metadata X; and
- CREATE TABLE metadata X from allocated table id through current
  history/runtime publication.

Update `docs/garbage-collect.md` so its recovery reachability paragraph clearly
separates a possible future recovery-time allocation-map rebuild from current
behavior. State that current recovery validates and trusts the allocation map
persisted by checkpoint, and reference backlog 000108 for startup rebuilding.
Do not imply that this Phase 4 task implements that follow-up.

Audit live conceptual documentation and active test names for claims that
metadata history owns a table, runtime layout, index runtime, or root. Correct
such claims in current docs/tests only. Preserve completed tasks, closed
backlogs, and RFC rejected-alternative analysis as historical records.

During `$task-resolve`, update RFC 0024 Phase 4:

- replace `docs/tasks/TBD.md` with this task path;
- record the created task issue;
- set Phase Status to `done`;
- add the implementation summary and task-resolve synchronization note; and
- leave the RFC's goals, non-goals, prerequisites, and phase-local choices
  unchanged unless implementation discovered an explicitly approved contract
  correction.

### 10. Verification and risk controls

Run focused new tests repeatedly with nextest `--stress-count 100`, especially
the DROP INDEX/DROP TABLE metadata-X waiters and the pinned-runtime purge
sequence. Tests must use lock snapshots, production horizon waits, checkpoint
outcomes, purge events, and file predicates. Sleeps and elapsed-time progress
assumptions are forbidden; timeouts are watchdogs only.

Run:

```bash
rtk cargo fmt --all -- --check
rtk cargo clippy --workspace --all-targets -- -D warnings
rtk cargo clippy -p doradb-storage --no-default-features --features libaio --all-targets -- -D warnings
tools/style_audit.rs
rtk cargo nextest run --workspace
rtk cargo nextest run -p doradb-storage --no-default-features --features libaio
rtk cargo deny check
```

Run `tools/coverage_focus.rs` for every changed Rust module, with particular
focus on `catalog/history.rs`, `trx/admission.rs`,
`table/persistence.rs`, `catalog/index.rs`, `catalog/table.rs`, and
`table/recover.rs`. Meet the repository's 80% focused line-coverage bar or
record why a definition-heavy file is below it and cite covered consumers.

Primary risks and controls:

- Arc-count assertions can overfit incidental owners. Assert differential
  ownership only where needed and use semantic cleanup state for outcomes.
- Purge/checkpoint tests can race scheduler progress. Observe authoritative
  predicates and completion events in production order.
- Restart coverage can accidentally absorb backlog 000108. Reclaim before
  shutdown and assert persisted state after startup; never rebuild on startup.
- Failure tests can mistake post-commit ambiguity for rollback. Preserve poison
  and validate recovered authoritative state instead.
- Documentation cleanup can rewrite historical decisions. Limit edits to live
  conceptual documents and misleading active test names.

If validation exposes a local defect that violates RFC 0024 without changing
its contract, fix it in the owning module and add the regression test. If a fix
would change a public error, durable format, horizon, ownership model, or phase
contract, stop and plan it separately rather than expanding this task.

## Implementation Notes

## Impacts

| Area | Files and interfaces |
| --- | --- |
| Metadata history and tombstones | `doradb-storage/src/catalog/history.rs`: `TableMetadataVersion`, `TableHistoryEntry`, `CurrentTableState`, `UserTableEntry`, `DroppedTableOperationalState`, visible/current resolution and strict purge |
| Catalog resolution | `doradb-storage/src/catalog/mod.rs`: `resolve_user_table_visible`, `resolve_user_table_current`, `validate_user_table_live`, test-only history/operational observations |
| Transaction admission | `doradb-storage/src/trx/admission.rs`: `TransactionTableBinding`, cache miss/hit validation, binding handoff, metadata-lock debug tests |
| Purge coordination | `doradb-storage/src/trx/purge.rs`: existing horizon calculation, metadata/root/dropped-table work, `PurgeTestEvent::CycleCompleted`; new event only if existing observation is insufficient |
| Runtime index retirement | `doradb-storage/src/table/layout.rs`, `doradb-storage/src/table/mod.rs`, and `doradb-storage/src/catalog/index.rs`: layout installation, retired-index queue, cleanup, DDL failure assertions |
| Persistent root reclamation | `doradb-storage/src/table/persistence.rs` and active-root/allocation-map test observations |
| DROP TABLE lifecycle | `doradb-storage/src/catalog/table.rs`: validation gate, tombstone publication, retained runtime, file cleanup, failure-window tests |
| Session maintenance | `doradb-storage/src/session.rs`: current-only explicit lock, freeze, and checkpoint behavior; production interfaces should remain unchanged |
| Recovery | `doradb-storage/src/table/recover.rs` and selected `doradb-storage/src/recovery/mod.rs` tests: CTS-zero baseline, committed/uncommitted table drop, index DDL root proof |
| Documentation | `docs/transaction-system.md`, `docs/garbage-collect.md`, and RFC 0024 Phase 4 synchronization during task resolution |

Production interfaces should remain unchanged unless a narrow RFC-contract
defect is found. Expected code additions are tests, test-local helpers, and at
most narrow `#[cfg(test)]` observations.

## Test Cases

1. Resolving and retaining superseded metadata changes no table, layout, or
   index runtime owner count.
2. Strict-horizon history purge removes the registry version while an
   externally retained metadata Arc remains readable.
3. An explicit pinned old layout keeps a removed index in the retired queue;
   dropping only historical metadata does not release it.
4. Dropping the pinned layout lets retired-index cleanup destroy exactly the
   removed runtime.
5. DROP INDEX detaches a checkpointed `DiskTree` root but leaves its allocation
   bit set while the old transaction/root fence remains protected.
6. After the horizon and a later checkpoint, the detached root's allocation
   bit clears even while old logical metadata remains externally retained.
7. Restart preserves the allocation bit cleared by checkpoint and performs no
   recovery-time reachability rebuild.
8. A bound read transaction makes DROP INDEX metadata X wait; transaction
   commit releases the fence and DDL completes.
9. A bound read transaction makes DROP TABLE metadata X wait; transaction
   rollback releases the fence and DDL completes.
10. An untouched old transaction does not block DROP INDEX, receives
    `SchemaChanged` for the visible-but-currently-removed index, and installs no
    binding or lock afterward.
11. An untouched old transaction does not block DROP TABLE and receives
    `SchemaChanged` without binding or retaining locks afterward.
12. Metadata-history and dropped-runtime eligibility both retain state at
    equality with the drop CTS and release only after the strict boundary.
13. Runtime-to-Floor transition can occur while tombstone history remains.
14. Tombstone-history purge can occur while retained Runtime remains.
15. The outer user-table entry disappears only after both logical and
    operational slots are absent.
16. An external table Arc delays dropped runtime destruction without enabling
    current, visible, explicit-lock, freeze, checkpoint, or DDL foreground
    access.
17. After the external Arc drains, dropped runtime becomes a Floor; the file
    remains until catalog checkpoint crosses its drop CTS.
18. Catalog checkpoint plus purge removes the dropped file, Floor, and outer
    entry.
19. Explicit lock, freeze, and checkpoint after DROP TABLE return
    `TableNotFound` and leave no lock or maintenance state behind.
20. Operational work on a live table with retained predecessor metadata uses
    the installed current layout/root generation.
21. Restart after online CREATE INDEX and DROP INDEX creates one CTS-zero live
    baseline with zero history, inactive dropped slot, and no retired runtime.
22. Restart before catalog checkpoint after committed DROP TABLE exposes no
    current or visible logical state and retains only the replay/file Floor.
23. Restart after an uncommitted DROP TABLE keeps the table live as one
    CTS-zero baseline with empty history.
24. CREATE TABLE validation and pre-commit failpoints publish no history,
    runtime, or surviving file.
25. CREATE TABLE commit poison publishes no executable fallback; restart
    removes its provisional file.
26. CREATE INDEX validation/build failures leave current metadata, history,
    layout, root, allocation, and retired-runtime state unchanged.
27. DROP INDEX validation failures leave the same complete snapshot unchanged.
28. Post-commit index DDL failures preserve poison and restart selects only the
    root-proven current result with empty volatile history.
29. DROP TABLE pre-gate failures publish no tombstone or dropped operational
    state and do not poison.
30. DROP TABLE commit failure and abandoned terminal future preserve the source
    poison and never reopen a historical predecessor.
31. Focused concurrent tests pass 100 stress iterations without sleeps,
    retries masking failures, leaked grants, or hangs.
32. Workspace default and libaio lint/test suites pass, and focused coverage
    meets the repository review bar.

## Open Questions

None. Recovery-time allocation-map validation or rebuilding remains explicitly
owned by `docs/backlogs/000108-recovery-table-file-alloc-map-rebuild.md`. Any
new architectural question discovered during implementation must be separated
from this validation task rather than resolved implicitly.
