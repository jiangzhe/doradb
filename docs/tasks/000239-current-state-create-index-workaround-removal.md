---
id: 000239
title: Use Current State for CREATE INDEX and Remove History Workarounds
status: implemented  # proposal | implemented | superseded
created: 2026-07-26
github_issue: 892
---

# Task: Use Current State for CREATE INDEX and Remove History Workarounds

## Summary

Implement Phase 3 of RFC 0024 by making unique and non-unique user-table
`CREATE INDEX` build the same current committed table state. Retain the
existing captured cold/hot boundary, current-row validation, staged runtime,
catalog transaction, table-root publication, runtime-layout installation, and
failure cleanup, but remove the non-unique build's transaction-history cutoff,
cold historical candidates, hot undo-chain reconstruction, delete-masked build
entries, and dedicated cleanup requirement.

Phases 1 and 2 already provide the correctness boundary that makes this
cutover safe. A transaction that successfully touched the table owns
transaction-lifetime `TableMetadata(S)`, so CREATE INDEX metadata X drains it
before building. An untouched transaction whose STS predates index creation
cannot admit the new stable index number. An untouched stale writer also fails
the exact visible/current metadata-identity check before acquiring a data lock
or creating row, index, undo, or redo effects. The index therefore needs only
the current committed rows that post-publication transactions may execute.

Use one `CreateIndexRowEntry` collection model for both index kinds. Current
cold rows populate the new `DiskTree`; current hot rows populate the new
`MemIndex`. The collector encodes each row once into its final `BTreeKey`:
unique entries contain the logical key, while non-unique entries contain the
exact `(logical_key, row_id)` key. Unique creation retains current-state
duplicate validation. Both runtime builders insert the collected encoded key
through ordinary active insertion semantics. Remove the CREATE-INDEX-only
candidate and build-state APIs end to end while preserving general row-MVCC
candidate logic used by ordinary index lookup.

## Context

Parent RFC:

- `docs/rfcs/0024-versioned-metadata-immediate-retirement.md`

RFC Phase:

- Phase 3: Current-State CREATE INDEX And Workaround Removal

Prerequisite Tasks:

- `docs/tasks/000237-metadata-only-table-history-publication.md`
- `docs/tasks/000238-first-touch-transaction-binding-admission.md`

Source Backlogs:

- `docs/backlogs/000164-create-unique-index-full-mvcc-history.md`
- `docs/backlogs/000165-reclaim-non-unique-create-index-history.md`

Issue Labels:

- type:task
- priority:high
- codex

Phase 1 installed CTS-effective logical table metadata history and direct
current table metadata/runtime state. CREATE INDEX now publishes the old
metadata as a superseded logical version and installs the new current metadata
at `create_cts` after table-root and runtime-layout publication.

Phase 2 installed positive `TransactionTableBinding` entries and request-aware
admission:

1. A table-bound transaction retains transaction-owned
   `TableMetadata(table_id, S)` until transaction end.
2. A first index request validates stable `index_no` membership in both the
   STS-visible metadata and the bound current layout.
3. A new index absent from visible metadata returns `IndexNotFound`, even when
   current metadata contains it.
4. A write requires visible and current `(TableID, effective_cts)` identities
   to match before `TableData(IX/X)` acquisition or mutation.
5. CREATE INDEX retains `TableMetadata(X)` and `TableData(X)`, so it drains
   table-bound transactions and writers before row collection while leaving
   untouched old transactions active.

Those contracts are already active in
`doradb-storage/src/trx/admission.rs`. This phase uses them as the sole
creation-visibility and stale-writer proof for both unique and non-unique
indexes; it does not add a second admission mechanism.

The current unique build in `doradb-storage/src/catalog/index.rs` already
collects only current cold and hot rows:

- persisted delete deltas are skipped;
- committed `ColumnDeletionBuffer` markers exclude cold rows;
- unresolved cold deletes fail closed;
- latest non-deleted hot images at or above the captured pivot are collected;
- current cold/cold, cold/hot, and hot/hot duplicates are rejected.

The non-unique build still performs the workaround implemented by task 000236:

1. Capture `TransactionSystem::published_gc_horizon()` as
   `history_cutoff`.
2. Assert the active root's deletion cutoff is no newer than that cutoff.
3. Turn qualifying committed cold deletes into delete-masked runtime
   candidates.
4. Traverse each hot row's main undo branch while its row-version latch is
   held.
5. Reconstruct and normalize retained exact keys per RowID.
6. Insert current entries active and history-only entries delete-masked through
   `insert_encoded_build_candidate`.

Phase 2 makes those extra candidates inaccessible through valid foreground
admission. They nevertheless consume MemIndex space, preserve
CREATE-INDEX-specific code in row-MVCC and table-access modules, and leave
backlog 000165's deterministic reclamation problem. Unique-history backlog
000164 is likewise unnecessary because old transactions cannot admit a newly
created unique index and stale writers cannot bypass the current unique
constraint.

The Phase 3 phase-local choice is fixed: remove only CREATE-INDEX-specific
history mechanisms. Ordinary `IndexLookupCandidate` streams, row undo,
historical row reconstruction, unique owner branches, deletion-buffer MVCC
rechecks, foreground index undo, purge, and MemIndex cleanup remain part of the
general transaction/index design and must not change.

Phase 4 assumes this task has removed historical candidate state. No Phase 4
scope, prerequisite, instrumentation strategy, or recovery assumption changes:
this task merely satisfies its existing Phase 3 prerequisite.

## Goals

1. Make unique and non-unique CREATE INDEX consume one common current committed
   cold/hot row set.
2. Preserve the captured active-root pivot and column-block-index snapshot as
   the single boundary between cold DiskTree input and hot MemIndex input.
3. Keep current cold-row deletion filtering correct for persisted delete
   deltas and committed or unresolved in-memory deletion markers.
4. Keep current hot-row collection limited to latest non-deleted row images at
   or above the captured pivot.
5. Preserve unique-index validation for current cold/cold, cold/hot, and
   hot/hot duplicate keys.
6. Populate a new non-unique MemIndex with active current exact
   `(logical_key, row_id)` entries only.
7. Encode each collected row once as its final `BTreeKey` and reuse that key
   for sorting, validation, DiskTree construction, and MemIndex population.
8. Remove the non-unique history cutoff, cutoff assertions, historical cold
   dispositions, hot undo-chain traversal, candidate normalization, and
   delete-masked build insertion.
9. Remove CREATE-INDEX-only candidate/build types, module exports, helper
   interfaces, and dedicated tests from catalog, table, transaction, and index
   modules.
10. Retain CREATE INDEX's current locks, metadata-change leases, staged
   resource ownership, rollback, catalog commit, root publication, runtime
   layout publication, poisoning, and recovery behavior.
11. Prove that metadata/index admission and the exact stale-writer fence cover
    both unique and non-unique creation.
12. Update the live secondary-index design document to describe current-state
    builds and remove the superseded history/recovery contract.
13. Leave Phase 4 with no build-created historical index candidates, unique
    owner history, or dedicated history-reclamation owner to validate.

## Non-Goals

1. Do not change CTS-versioned metadata selection, transaction binding, lock
   handoff, `IndexNotFound`, or `SchemaChanged` semantics established by
   Phases 1 and 2.
2. Do not introduce write-compatible metadata transitions or let stale writers
   maintain visible/current index unions.
3. Do not change CREATE/DROP INDEX lock modes, lock ordering, metadata-change
   checkpoint exclusion, or publication ordering.
4. Do not change ordinary row-MVCC reconstruction, row undo layout, runtime
   unique-key links, `IndexLookupCandidate`, candidate recheck, or foreground
   index maintenance.
5. Do not change general CDB deletion visibility, persistent delete-delta
   encoding, checkpoint companion work, purge, or MemIndex cleanup rules.
6. Do not add historical unique owners, synthetic index branches, historical
   non-unique candidates, cleanup manifests, or runtime build-history owners.
7. Do not stream, batch, parallelize, or otherwise redesign the cold build.
   Bounded-memory and parallel construction remain owned by
   `docs/backlogs/000104-stream-parallel-create-index-cold-build.md`.
8. Do not convert foreground index maintenance or checkpoint sidecars to
   `BTreeKey`; canonical encoded ownership is limited to CREATE INDEX.
9. Do not change table-file, catalog-file, DiskTree, MemIndex, redo, undo,
   checkpoint, or recovery formats.
10. Do not change stable sparse `index_no` allocation or reuse rules.
11. Do not change public storage APIs or add a new public error.
12. Do not edit tasks 000236, 000237, or 000238 as historical implementation
    records.
13. Do not implement Phase 4 operational reclamation/recovery validation in
    this task.
14. Do not add unsafe code.

## Plan

### 1. Establish one current-row collector for both index kinds

Retain `CreateIndexCollector` in
`doradb-storage/src/catalog/index.rs`. It continues to own borrowed table,
pool-guard, runtime-layout, and index-spec references, the final-key
`BTreeKeyEncoder`, plus the captured `column_block_index_root` and
`pivot_row_id`.

`CreateIndexCollector::new` must continue to copy both boundary values from the
same captured `ActiveRoot` and call
`assert_create_index_block_index_snapshot` against
`table.mem.blk_idx().column_route_snapshot()`. This proof prevents cold and hot
collection from using different pivot/root generations.

Replace the index-kind-specific collection interface with:

```rust
async fn collect_current_cold(
    &self,
) -> OperationOrRuntimeResult<Vec<CreateIndexRowEntry>>;

async fn collect_current_hot(
    &self,
) -> RuntimeResult<Vec<CreateIndexRowEntry>>;
```

The exact private names may follow surrounding style, but the ownership and
result shape are fixed: both return `CreateIndexRowEntry { key: BTreeKey,
row_id }`. A unique key encodes indexed columns only. A non-unique key encodes
the exact `(indexed columns, row_id)` physical key. Neither collector returns
historical candidates or accepts a history cutoff.

Remove:

- `CreateIndexColdRows`;
- `CreateIndexColdRowDisposition`;
- `collect_unique_cold`;
- `collect_non_unique_cold`;
- `collect_cold_with`'s historical-disposition callback;
- `collect_unique_hot`;
- `collect_non_unique_hot`.

Do not revert task 000236's useful explicit captured-pivot scan to a
freshly-read runtime pivot. Keep
`UserTableAccessor::mem_scan_uncommitted_from` so hot collection remains bound
to the captured active root.

### 2. Collect only current cold rows

`collect_current_cold` keeps the existing ColumnBlockIndex and LWC validation
flow:

1. Return an empty vector for an empty column-block root while retaining the
   non-empty-root/pivot invariant.
2. Walk ColumnBlockIndex leaves from the captured root.
3. Load each leaf's persisted delete deltas and RowIDs.
4. Load the matching LWC block.
5. Validate leaf, block, and RowID counts plus row-shape fingerprint.
6. Convert checked persisted delete deltas to RowIDs.
7. Skip any row already represented by a persisted delete delta.
8. Classify the remaining row against the table's current
   `ColumnDeletionBuffer`.
9. Decode indexed columns, immediately encode the final `BTreeKey`, and emit
   `CreateIndexRowEntry` only when the current row is live.

Use one current-delete helper equivalent to:

```text
no CDB marker                         -> include
committed marker                      -> exclude
status-ref whose transaction committed -> exclude
status-ref still unresolved           -> WriteConflict
```

The helper may return `OperationResult<bool>` where `true` means deleted, as
the pre-task-000236 current-only code did, or an equivalent two-state private
enum. It must have no cutoff parameter and no historical outcome.

Preserve the defensive `WriteConflict` report with table and RowID context for
an unresolved CDB marker. DDL data X should have drained the owner, but a
defensive typed failure is preferable to treating an uncommitted delete as
either current live or current deleted.

Do not decode key values for excluded rows. Retain no logical `Vec<Val>` after
encoding the included row.

### 3. Collect only current hot rows

`collect_current_hot` uses
`table.accessor_with_layout(self.layout).mem_scan_uncommitted_from(...)` with
the collector's captured `pivot_row_id`.

For each raw latest hot row:

1. Skip the row if its latest physical image is deleted.
2. Read the indexed columns from the page's column layout.
3. Encode the unique logical key or non-unique exact key once.
4. Emit exactly one `CreateIndexRowEntry { key, row_id }`.

Do not access `RowReadAccess`, the row-version map, the main undo branch,
transition CTS values, or historical key normalization. CREATE INDEX already
holds metadata/data X before capturing the root and scanning. Table-bound
transactions and writers have drained, so the latest non-deleted physical
image is the current committed build input. Retained undo remains ordinary row
MVCC state but is irrelevant to a newly created index that old snapshots
cannot admit.

### 4. Simplify CREATE INDEX orchestration

In `create_index_for_session`, remove the non-unique branch that captures
`engine.trx_sys.published_gc_horizon()` and validates
`active_root.deletion_cutoff_ts <= history_cutoff`.

After beginning the implicit DDL transaction and creating
`CreateIndexProgress`, use this order:

1. Construct one `CreateIndexCollector` from the locked table, old runtime
   layout, new index specification, and captured active root.
2. Collect one `Vec<CreateIndexRowEntry>` of current cold rows.
3. Sort the cold entries by canonical `BTreeKey`; run adjacent-key duplicate
   validation only for a unique index.
4. Build and stage the new cold DiskTree root by borrowing those key bytes.
5. Collect one `Vec<CreateIndexRowEntry>` of current hot rows. Sort and
   validate it only for a unique index; non-unique MemIndex insertion does not
   require sorted input.
6. Pass the same encoded current-row representation to the selected unique or
   non-unique runtime builder.
7. Continue existing runtime/layout staging and publication.

There must be no `history_cutoff`, `cold_historical_candidates`, candidate
combination, or historical normalization branch.

Preserve the remainder of the state machine without semantic change:

```text
metadata/data X
  -> metadata-change leases
  -> active-root/layout capture
  -> stable index-no and new metadata allocation
  -> implicit DDL transaction
  -> current cold DiskTree build
  -> current hot MemIndex build
  -> staged runtime/layout
  -> catalog update and commit CTS
  -> table-root publication
  -> runtime-layout installation
  -> CTS-effective metadata-history publication
  -> metadata-history purge request
```

All pre-commit failures continue through
`CreateIndexProgress::rollback_before_catalog_commit`. Post-commit ambiguity
continues through the existing poison/cleanup path.

### 5. Populate MemIndex from canonical build keys

Change `CreateIndexRuntimeBuilder::build_non_unique` to accept current
`Vec<CreateIndexRowEntry>` values instead of
`Vec<CreateIndexNonUniqueCandidate>`.

After allocating the empty `NonUniqueMemIndex`, populate each current hot row
through:

```rust
mem.bind(index_guard)
    .insert_encoded_if_not_exists(&row.key, row.row_id, false, build_ts)
    .await
```

The `false` merge argument is fixed. A newly allocated build MemIndex is empty,
and the collector emits each current hot RowID once. Encountering a duplicate
exact `(logical_key, row_id)` is therefore an internal build invariant
failure, matching the prior current-only implementation.

Do not add a CREATE-specific non-unique duplicate-validation pass. Persisted
cold RowIDs are strictly ordered inside non-overlapping ColumnBlockIndex
ranges, and the exact-key encoder appends RowID. Tests for those constructors
own the proof that a non-unique exact key cannot repeat. Cold sorting remains
required by the DiskTree batch interface; hot non-unique entries require no
sorting or validation before ordinary MemIndex insertion.

Rename `insert_create_index_non_unique_candidates` to an active-current-row
name such as `insert_create_index_non_unique_hot_rows` and accept
`&[CreateIndexRowEntry]`. Add narrow encoded insertion primitives to the
guarded unique and non-unique MemIndex views. Their existing logical-key
insertion methods encode and delegate to the same primitive, so CREATE INDEX
reuses ordinary active insertion semantics without restoring build state or
delete-mask behavior. The non-unique primitive requires a canonical exact key
that already contains the matching RowID suffix.

Retain the `PopulateNonUnique` test failpoint, or rename it consistently to
describe current non-unique population. It must still fire after MemIndex
allocation and before successful population so failure cleanup proves the
allocated unpublished tree is destroyed. Retain `AfterRuntimeStaged`.

Unique runtime construction is semantically unchanged:

- validate current hot keys against current cold keys and one another;
- allocate `UniqueMemIndex`;
- insert current hot owners using their collected encoded keys;
- reject duplicates as `OperationError::DuplicateKey`;
- destroy the unpublished MemIndex on population failure.

### 6. Delete the CREATE-INDEX history API surface

In `doradb-storage/src/trx/row.rs`, remove:

- `CreateIndexNonUniqueCandidate`;
- `RowReadAccess::collect_non_unique_create_index_candidates`;
- `insert_create_index_non_unique_historical_candidate`;
- helper imports used only by those definitions;
- the dedicated cutoff, deduplication, active-transition, and build-candidate
  unit tests plus test-only helpers used solely by them.

Do not alter other `RowReadAccess` methods, `FindOldVersion`,
`IndexCandidateRecheck`, main/index undo traversal, key reconstruction used by
foreground DML, or runtime unique-owner branch behavior.

In `doradb-storage/src/table/access.rs`, remove:

- `collect_non_unique_create_index_hot_candidates`;
- its `CreateIndexNonUniqueCandidate` import;
- production imports used only for that bridge.

Retain `mem_scan_uncommitted_from` and general MemTable/table/index access.

In `doradb-storage/src/index/non_unique_index.rs`, remove:

- `NonUniqueIndexBuildState`;
- `GuardedNonUniqueMemIndex::insert_encoded_build_candidate`;
- the unit test proving initial delete-mask state;
- imports used only by that build-only path.

Retain ordinary exact insertion, lookup, masking, conditional cleanup deletion,
candidate streaming, and all foreground/checkpoint/purge behavior.

In `doradb-storage/src/index/mod.rs`, stop re-exporting
`NonUniqueIndexBuildState`. No replacement public or crate-wide type is
introduced.

After the edit, repository search must find no production references to:

```text
CreateIndexNonUniqueCandidate
NonUniqueIndexBuildState
insert_encoded_build_candidate
collect_non_unique_create_index_candidates
collect_non_unique_create_index_hot_candidates
create_index_history_cutoff
history_cutoff within CREATE INDEX
```

This symbol audit is a removal acceptance check, not permission to remove
similarly named general MVCC concepts outside CREATE INDEX.

### 7. Replace dedicated history tests with current-state and admission proofs

Delete the tests whose only contract is cutoff comparison, undo-history
normalization, delete-masked build insertion, or old-snapshot access through a
new index.

In `doradb-storage/src/trx/row.rs`, remove the six
`test_create_index_hot_history_*` tests and their candidate/status helpers.
They test a traversal that no longer exists.

In `doradb-storage/src/index/non_unique_index.rs`, remove
`test_build_candidate_preserves_initial_delete_mask`.

In `doradb-storage/src/catalog/index.rs`, remove:

- history-cutoff assertion tests;
- cold historical disposition/cutoff tests;
- dedicated multi-version candidate-retention tests;
- historical key-reuse tests and helpers whose initial state requires a
  build-created delete-masked candidate.

Do not mechanically delete all scenarios introduced by task 000236. Preserve
or rewrite coverage that independently proves:

- one captured cold/hot boundary;
- current cold deletion filtering;
- current hot and cold index population;
- staged runtime cleanup;
- duplicate validation;
- root publication and retention;
- recovery.

Add compact direct-state tests that distinguish absence from a delete mask:

1. Update a hot row's indexed key before non-unique creation. After creation,
   the new current exact key is active in MemIndex and the previous exact key
   is absent, not present delete-masked.
2. Checkpoint a row cold, then delete it or replace it with a hot row before
   non-unique creation. The deleted old RowID/key is absent from the new
   DiskTree and MemIndex; the current hot replacement is active.
3. Change a hot key before unique creation. The unique runtime contains only
   the current owner/key; the old key is absent.
4. Preserve current duplicate tests across cold/cold, cold/hot, and hot/hot
   inputs, including successful creation when a duplicate belongs only to a
   committed deleted historical row.

Use direct runtime/MemIndex/DiskTree inspection only inside the catalog module's
existing internal test boundary. Foreground correctness tests must use normal
transaction admission APIs.

In `doradb-storage/src/trx/admission.rs`, extend or parameterize the existing
CREATE INDEX admission scenarios for both unique and non-unique specifications:

1. An untouched old transaction spanning creation receives `IndexNotFound`
   for the new index while table scan and an older surviving index remain
   usable.
2. A write-first untouched old transaction spanning creation receives
   `SchemaChanged` before a transaction binding, metadata/data transaction
   lock, row callback, undo, redo, or index effect.
3. A read-first old transaction may bind the current table/layout intersection
   but later receives `SchemaChanged` for a write while retaining only the
   earlier read's metadata binding.

Add or retain deterministic lock coverage proving a transaction that bound the
table before CREATE INDEX makes metadata X wait until transaction end.
Synchronize using the lock manager's observable waiter/grant predicate or an
existing hook/channel. Do not use sleeps to establish progress; timeouts are
hang watchdogs only.

### 8. Update the live secondary-index design

Update `docs/secondary-index.md` so it no longer presents runtime non-unique
CREATE INDEX history as a live invariant:

1. In the current-row completeness discussion, state that unique and
   non-unique CREATE INDEX both build only current committed rows.
2. Replace or remove `Runtime Non-Unique CREATE INDEX History` with a
   `Current-State CREATE INDEX` explanation covering:
   - metadata/data X drains already bound transactions and writers;
   - current cold rows go to DiskTree;
   - current hot rows go to MemIndex;
   - old untouched transactions cannot admit the new index;
   - exact-version admission rejects stale writers before mutation.
3. Remove recovery text that describes non-unique historical candidates as
   runtime-only state not reconstructed after restart.
4. Remove cleanup or deferred-unique wording that is obsolete after this
   phase.
5. Preserve ordinary candidate recheck, foreground DML, checkpoint companion
   work, recovery rebuild, and MemIndex cleanup documentation.

Task 000236 remains an accurate historical implementation record and is not
rewritten. The new live design supersedes its runtime contract.

### 9. Resolve source backlogs and synchronize RFC 0024

The task document records backlogs 000164 and 000165 as source inputs because
Phase 3 makes both proposed mechanisms unnecessary.

During `$task-resolve`, close both through the repository backlog workflow with
`Type: replaced`:

- backlog 000164 is replaced because old transactions cannot admit a new
  unique index and stale writers cannot bypass its current constraint;
- backlog 000165 is replaced because Phase 3 removes the build-created
  historical non-unique candidates whose reclamation it owns.

Reference this task, its issue, and RFC 0024 in each close reason. Do not close
backlog 000104.

Also synchronize RFC 0024 Phase 3 during `$task-resolve`:

1. replace the Phase 3 task placeholder with this task path;
2. record the created task issue;
3. set Phase Status from pending to done only after implementation, review,
   tests, and behavior verification pass;
4. record an implementation summary grounded in the actual result;
5. confirm Phase 4 remains pending and its Phase 3 prerequisite is now
   satisfied;
6. keep Phase 4 scope, non-goals, prerequisites from Phases 1/2, and
   phase-local choices unchanged unless implementation discovers contrary
   evidence.

No RFC phase-plan edit is required during task design beyond this resolve-time
synchronization contract.

### 10. Validate the complete change

Run formatting and strict lint:

```bash
rtk cargo fmt --all -- --check
rtk cargo clippy --workspace --all-targets -- -D warnings
```

Run the authoritative default and alternate-backend suites:

```bash
rtk cargo nextest run --workspace
rtk cargo nextest run -p doradb-storage --no-default-features --features libaio
```

Run the branch Rust style audit:

```bash
tools/style_audit.rs
```

Run focused coverage for the changed catalog/index, admission, table-access,
row, and non-unique-index files. Meet the repository's default 80% focused
review bar or document a justified definition-heavy exception with covered
consumer/runtime paths.

No nextest configuration, timeout, retry, or hang-detection change belongs in
this task.

## Implementation Notes

- Unified unique and non-unique creation around one current-state collector.
  Included cold and hot rows are encoded once into first-class `BTreeKey`
  entries: unique keys contain the logical key, while non-unique keys contain
  the exact logical-key/RowID pair. Cold entries are sorted for DiskTree
  construction; only unique creation sorts hot entries and performs explicit
  duplicate validation.
- Added encoded guarded insertion paths for both MemIndex kinds and made the
  existing logical-key paths encode and delegate to them. Non-unique creation
  deliberately performs no separate exact-key duplicate scan: exact-key
  uniqueness follows from non-overlapping cold RowID ranges, strictly ordered
  row IDs, and RowID-bearing encoding, while ordinary insertion retains its
  invariant checks.
- Removed the non-unique history cutoff, cold historical dispositions, hot
  undo-chain collection and normalization, delete-masked build insertion,
  build-state/candidate types, table-access bridge, exports, and their
  dedicated tests. General row-MVCC reconstruction, candidate recheck,
  foreground index maintenance, checkpoint, purge, and cleanup paths were
  unchanged.
- Added direct current-state coverage for hot key replacement, cold-to-hot
  replacement, unique owner replacement, canonical-key validation, encoded
  insertion parity, and both unique/non-unique admission behavior. The
  transaction-binding test deterministically proves CREATE INDEX metadata X
  waits, using bounded polling of the observable exclusive waiter.
- Updated `docs/secondary-index.md` to describe the current-state build and
  metadata-admission proof. The removed-symbol audit found no remaining
  production references to the CREATE-INDEX-only history candidate or
  build-state APIs.
- Validation passed with formatting and strict workspace Clippy, 1,532 tests
  on the default backend, 1,457 `doradb-storage` tests with the `libaio`
  backend, and the branch style audit across seven changed Rust files. Focused
  coverage across the changed Rust paths was 92.27%, above the 80% review bar.
  The final bounded-poll review adjustment additionally passed its focused
  regression test.
- No implementation scope deviation remains. The planned representation was
  refined during review to retain canonical `BTreeKey` values throughout the
  build and to treat non-unique exact-key duplication as a programmer
  invariant rather than an expensive CREATE-specific runtime validation.

## Impacts

Primary production impacts:

- `doradb-storage/src/catalog/index.rs`
  - unify current cold/hot collection;
  - remove history cutoff and historical candidate construction;
  - populate non-unique current hot rows through ordinary insertion;
  - preserve build staging, publication, and cleanup.
- `doradb-storage/src/table/access.rs`
  - remove the CREATE-INDEX-specific historical hot-candidate bridge;
  - retain the captured-boundary raw hot scan.
- `doradb-storage/src/trx/row.rs`
  - remove CREATE-INDEX-specific undo-history collection and candidate types;
  - preserve general row-MVCC and index-candidate behavior.
- `doradb-storage/src/index/non_unique_index.rs`
  - remove build-state and encoded delete-masked candidate insertion;
  - retain ordinary exact entry operations and cleanup.
- `doradb-storage/src/index/mod.rs`
  - remove the obsolete build-state re-export.

Test-only impacts:

- `doradb-storage/src/catalog/index.rs`
  - replace candidate-history tests with direct current-state build tests;
  - retain generic build cleanup, duplicate, boundary, publication, and
    recovery coverage.
- `doradb-storage/src/trx/admission.rs`
  - prove new-index invisibility and stale-writer rejection for both index
    kinds.
- `doradb-storage/src/trx/row.rs`
  - remove tests for deleted CREATE-INDEX history traversal.
- `doradb-storage/src/index/non_unique_index.rs`
  - remove the deleted build-mask API test.

Documentation and planning impacts:

- `docs/secondary-index.md`
  - replace the live historical-candidate contract with current-state creation
    plus metadata admission.
- `docs/rfcs/0024-versioned-metadata-immediate-retirement.md`
  - resolve-time Phase 3 task/issue/status/summary synchronization.
- `docs/backlogs/000164-create-unique-index-full-mvcc-history.md`
  - resolve-time closure as replaced.
- `docs/backlogs/000165-reclaim-non-unique-create-index-history.md`
  - resolve-time closure as replaced.

No intended impact:

- public API shape;
- `OperationError` variants or precedence;
- stable table/index identifiers;
- table/index logical locks or owner lifetimes;
- table runtime layout or secondary runtime ownership;
- row/undo/index persistent encodings;
- table-file or catalog-file roots;
- redo or recovery formats;
- checkpoint and purge algorithms;
- default or libaio I/O backends;
- unsafe inventory.

The code deletion spans catalog, table, transaction, and index modules but
removes an existing DDL-specific coupling rather than introducing a new
cross-subsystem architecture.

## Test Cases

### Current cold/hot collection

1. An empty column-block root and zero pivot produce no cold build rows.
2. A non-empty column-block root with zero pivot retains the current invariant
   failure and diagnostic.
3. Captured active-root pivot/column-root values must match the runtime
   ColumnBlockIndex routing snapshot.
4. Persisted delete deltas exclude those cold RowIDs before key decoding.
5. A committed direct CDB marker excludes its cold row.
6. A committed status-ref CDB marker excludes its cold row.
7. An unresolved status-ref CDB marker returns `WriteConflict` with table and
   RowID context and publishes nothing.
8. Every remaining cold row is decoded and encoded once, then retained as one
   canonical `BTreeKey` entry.
9. Every latest non-deleted hot row at or above the captured pivot is encoded
   once and emitted once.
10. A latest deleted hot image is excluded without traversing its retained undo
    chain.
11. Cold and hot RowID ranges remain disjoint at the one captured pivot.

### Non-unique current-state build

12. Current cold rows populate only the new non-unique DiskTree.
13. Current hot rows populate only active exact MemIndex entries.
14. A hot row changed `A -> B` before creation has no `(A, row_id)` entry in
    the new MemIndex and one active `(B, row_id)` entry.
15. A retained hot undo chain containing several older indexed keys produces
    no entry for any older key.
16. A cold row deleted before creation is absent from both the new DiskTree and
    MemIndex even when its delete CTS could have met the former cutoff.
17. A cold `A` row replaced by a hot `B` row before creation contributes only
    current hot `(B, new_row_id)` and no historical cold `(A, old_row_id)`.
18. A hot move update contributes only the current replacement RowID/key.
19. Reuse of a historical key after creation follows ordinary foreground
    non-unique DML and undo; it does not depend on a build-created delete mask.
20. Encoded exact insertion distinguishes equal logical keys with different
    RowIDs. CREATE INDEX performs no separate non-unique duplicate scan; a
    repeated exact current entry remains a broken collector/encoder invariant.

### Unique current-state build

21. Current cold/cold duplicate logical keys return `DuplicateKey` without
    publication.
22. Current cold/hot duplicate logical keys return `DuplicateKey` without
    publication.
23. Current hot/hot duplicate logical keys return `DuplicateKey` without
    publication.
24. A duplicate belonging only to a committed deleted cold or hot historical
    row does not reject creation.
25. A hot key changed `A -> B` before creation has no runtime unique owner for
    `A` and the current owner for `B`.
26. Encoded unique insertion preserves ordinary owner/duplicate semantics, and
    build failure destroys the unpublished MemIndex while leaving root,
    metadata, index allocation, and layout generation unchanged.

### Metadata admission and DDL drain

27. An untouched transaction with `sts <= create_cts` receives
    `IndexNotFound` for a newly created non-unique index.
28. The same boundary holds for a newly created unique index.
29. That old transaction may still bind and scan the immutable table layout.
30. It may still use an older stable index present in both visible and current
    metadata.
31. A fresh transaction with `sts > create_cts` admits and uses the new unique
    or non-unique index.
32. A write-first untouched old transaction spanning non-unique creation
    receives `SchemaChanged` with no binding, transaction metadata/data lock,
    callback, row/index effect, undo, or redo.
33. The same stale-writer contract holds across unique creation.
34. A read-first old transaction may retain its read binding but a later write
    receives `SchemaChanged` without a data lock or mutation.
35. A transaction that bound the table before CREATE INDEX causes metadata X
    to wait until commit or rollback.
36. An old transaction that never touched the table does not delay CREATE
    INDEX.
37. A transaction on another table does not delay CREATE INDEX.

### Failure, publication, and recovery

38. Forced non-unique population failure after MemIndex allocation destroys
    the unpublished runtime and publishes no metadata, root, or layout.
39. Forced failure after runtime staging performs the same cleanup.
40. Existing catalog-update rollback and pre-commit failure cases remain
    failure-atomic.
41. Existing post-commit root/layout/history publication ambiguity preserves
    poisoning behavior.
42. CREATE INDEX retains stable sparse index-number allocation and root slot
    shape.
43. Existing active-root retention and purge-horizon behavior remains
    unchanged.
44. Restart loads the published current DiskTree/runtime metadata without
    reconstructing any CREATE-INDEX historical candidate.
45. Subsequent current DML, checkpoint, cleanup, DROP INDEX, and recovery
    behavior remains green for both index kinds.

### Removal and validation

46. No production reference remains to the removed candidate/build-state,
    cutoff, or history-collection symbols listed in the Plan.
47. General row-MVCC lookup, unique owner links, candidate streams, index undo,
    CDB recheck, purge, and MemIndex cleanup tests remain unchanged and green.
48. `docs/secondary-index.md` contains the current-state/admission proof and no
    live contract requiring CREATE-INDEX historical candidates.
49. Run `rtk cargo fmt --all -- --check`.
50. Run `rtk cargo clippy --workspace --all-targets -- -D warnings`.
51. Run `rtk cargo nextest run --workspace`.
52. Run
    `rtk cargo nextest run -p doradb-storage --no-default-features --features
    libaio`.
53. Run `tools/style_audit.rs`.
54. Focused changed-path coverage meets the repository's default 80% review
    bar or documents a justified definition-heavy exception.

## Open Questions

None. The current-row input model, cold/hot boundary, CDB filtering, non-unique
active insertion API, history-removal boundary, admission proof, source-backlog
outcome, Phase 4 contract, documentation updates, and validation workflow are
resolved.
