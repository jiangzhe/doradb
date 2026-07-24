---
id: 000236
title: Make Non-Unique CREATE INDEX MVCC-Candidate-Complete
status: proposal
created: 2026-07-24
github_issue: 883
---

# Task: Make Non-Unique CREATE INDEX MVCC-Candidate-Complete

## Summary

Make user-table non-unique `CREATE INDEX` candidate-complete for every
transaction active across index publication. Capture one build-time
purge-published GC horizon, keep current cold state in the durable `DiskTree`,
and build the existing runtime `MemIndex` with both current hot candidates and
retained hot/cold historical candidates. Current candidates are active;
historical-only candidates are delete-masked so normal row MVCC remains the
visibility authority and later key reuse follows existing unmask/rollback
semantics.

Keep `CREATE UNIQUE INDEX` history construction and deterministic reclamation
of build-created historical candidates outside this task.

## Context

User-table secondary indexes are candidate structures. Index lookup and scan
first obtain a `RowID`, then table access checks row MVCC visibility and exact
indexed-key identity. This makes a non-unique index capable of retaining
multiple exact `(logical_key, row_id)` candidates for different versions of
the same row.

Current `CREATE INDEX` does not populate those candidates:

1. `doradb-storage/src/catalog/index.rs::collect_create_index_cold_rows`
   skips every committed column-deletion-buffer marker.
2. `collect_create_index_hot_rows` scans only each row page's latest physical
   image and skips deleted current images.
3. An older transaction may survive index DDL because RFC 0018's DDL locks
   drain active statements and writers, not read-only transaction snapshots.
   Its next statement binds the newly installed current runtime layout.
4. The resulting non-unique lookup or scan can therefore return a false
   negative for a deleted row or historical indexed key that remains visible
   to the older snapshot.

Backlog 000163 originally combined non-unique historical candidates, unique
owner history, and deterministic cleanup. Design review established that these
are materially different problems:

- non-unique indexes can retain independent exact candidates and rely on row
  MVCC recheck;
- unique indexes have one mapping per logical key and require ordered
  cross-`RowID` ownership branches;
- deterministic cleanup needs build ownership and a post-publication
  lifecycle, while correctness does not require immediate reclamation.

This task takes only the non-unique correctness portion. Backlog 000164 records
the unique-index remainder.

Issue Labels:
- type:task
- priority:high
- codex

Source Backlogs:
- docs/backlogs/000163-create-index-full-mvcc-history.md

Related Backlogs:
- docs/backlogs/000164-create-unique-index-full-mvcc-history.md
- docs/backlogs/000104-stream-parallel-create-index-cold-build.md
- docs/backlogs/000110-unify-hot-row-mem-scan-index-build-recovery.md

Relevant design and implementation references:

- `docs/rfcs/0018-create-drop-index.md`, especially DDL locking and the
  Phase-4 create flow.
- `docs/secondary-index.md`, especially candidate recheck, current-row
  completeness, MemIndex delete masks, and cleanup proofs.
- `docs/checkpoint.md`, especially the purge-published checkpoint cutoff and
  exclusive `deletion_cutoff_ts` durability boundary.
- `docs/garbage-collect.md`, especially strict `Global_Min_STS` purge
  comparisons and MemIndex cleanup ownership.
- `docs/tasks/000235-optimize-proof-bound-dual-tree-index-mutations.md`, which
  confirmed the historical-candidate gap.
- `doradb-storage/src/catalog/index.rs::create_index_for_session`.
- `doradb-storage/src/table/persistence.rs::apply_deletion_checkpoint`.
- `doradb-storage/src/trx/row.rs::RowReadAccess::read_row_mvcc` and
  `RowReadAccess::any_version_matches_key`.
- `doradb-storage/src/table/access.rs::index_lookup_candidate_row_mvcc`.
- `doradb-storage/src/index/non_unique_index.rs::NonUniqueMemIndex`.

## Goals

1. Capture one conservative build history cutoff from
   `TransactionSystem::published_gc_horizon()` while table DDL exclusion is
   held.
2. Exclude hot or cold history already proven globally invisible at that
   cutoff, using strict timestamp comparisons consistent with row undo and CDB
   visibility.
3. Include every distinct non-unique `(logical_key, row_id)` candidate needed
   by a transaction whose snapshot remains active across index publication.
4. Keep current cold candidates durable in `DiskTree` and keep every
   history-only candidate runtime-only in the existing `MemIndex`.
5. Represent current MemIndex candidates as active and historical-only
   candidates as delete-masked.
6. Preserve normal post-DDL reads and writes for transactions older than index
   publication; do not introduce a snapshot fence or table-write rejection.
7. Preserve CREATE INDEX staging, rollback, root publication, runtime layout
   installation, and restart contracts.
8. Document and test the non-unique historical-completeness contract without
   claiming the same property for unique indexes.

## Non-Goals

1. Do not change `CREATE UNIQUE INDEX` historical behavior. Unique ownership
   intervals, cross-`RowID` branches, overlap policy, publication, and cleanup
   are deferred to backlog 000164.
2. Do not implement deterministic delayed cleanup, a build-owned cleanup
   manifest, or synthetic index undo for CREATE INDEX history. Existing safe
   cleanup may reclaim entries opportunistically; prompt reclamation is not an
   acceptance condition.
3. Do not create a separate compatibility tree or add index/table publication
   fences.
4. Do not add durable historical entries to `DiskTree`, new table-file fields,
   redo formats, or recovery metadata.
5. Do not change the public `Session::create_index` API or index metadata
   format.
6. Do not stream, batch-limit, or parallelize cold/hot collection. Backlogs
   000104 and 000110 retain those optimization scopes.
7. Do not redesign general row MVCC reads, index candidate streams, checkpoint
   publication, or MemIndex GC beyond the narrow helpers and proofs needed by
   this task.

## Plan

### 1. Capture one build history cutoff

In `catalog::index::create_index_for_session`, retain the existing DDL lock and
metadata-change lease ordering. The active root remains captured before the
implicit DDL transaction starts. For a non-unique index, after
`Session::begin_trx` returns but before the transaction is transferred into
`CreateIndexProgress`:

1. read `TransactionSystem::published_gc_horizon()` once and store it as
   `history_cutoff`;
2. assert in release mode that
   `active_root.deletion_cutoff_ts <= history_cutoff`;
3. include `table_id`, `deletion_cutoff_ts`, and `history_cutoff` in the
   assertion diagnostic;
4. construct `CreateIndexProgress` with the transaction;
5. pass the same `history_cutoff` through both non-unique cold and hot
   collectors.

Keep the unique path's current-state behavior unchanged. It does not need a
history cutoff.

Construct one private `CreateIndexCollector` from the table, captured runtime
layout, guards, new index specification, and active root. Its constructor
copies the column-index root and pivot and asserts that they still match the
runtime column route. Reuse that collector for cold and hot phases through
separate unique and non-unique methods; pass `history_cutoff` only to the
non-unique methods.

`build_ts` remains the BTree construction timestamp and must not double as the
history cutoff.

`published_gc_horizon` is monotonic and may be stale below the freshly
calculated oldest-active snapshot, but it cannot be unsafe above it. The DDL
transaction is active before collection begins, so subsequently published
purge horizons cannot advance past its STS while the build is running.
Table-data exclusion prevents a transaction started after the cutoff from
observing a pre-build table state. If older transactions finish during the
build, the published horizon may advance; candidates retained relative to the
captured cutoff remain conservative and correct. Removing candidates that
become obsolete during the build belongs to the deferred cleanup scope.

Use the existing strict visibility relation:

```text
historical invalidation/delete CTS < history_cutoff  => globally invisible
historical invalidation/delete CTS >= history_cutoff => retain
```

Equality must be retained because row and CDB visibility treat an older version
as visible when the reader STS equals the invalidating CTS.

### 2. Split current cold rows from retained cold history

Refine CREATE INDEX cold collection so the non-unique path returns two logical
sets:

- `durable_rows`: current live cold rows for `DiskTree`;
- `historical_candidates`: retained deleted cold rows for delete-masked
  `MemIndex` entries.

For each cold row in the captured active root:

1. A persisted delete delta is excluded. Deletion checkpoint selects committed
   markers strictly below its purge-published cutoff and advances the root's
   exclusive `deletion_cutoff_ts` only with that durable delete state. The
   validated relation therefore proves:

   ```text
   delete_cts < active_root.deletion_cutoff_ts <= history_cutoff
   ```

   Do not clamp either cutoff or fall back to treating a violated relation as
   normal build input; the relation is an internal publication invariant.
2. A row with no deletion-buffer marker is current and enters
   `durable_rows`.
3. A row with `DeleteMarker::Committed(delete_cts)`, or a status reference
   already resolved as committed, enters `historical_candidates` only when
   `delete_cts >= history_cutoff`.
4. A committed marker with `delete_cts < history_cutoff` is excluded.
5. An unresolved active marker remains a fail-closed write-conflict/invariant
   error; DDL exclusion should normally make it unreachable.

Decode historical cold keys from the immutable LWC image exactly as current
cold keys are decoded. Never place committed-CDB historical rows in the new
`DiskTree`.

Keep the unique collector's current-state behavior unchanged. Prefer separate
non-unique collection/result helpers over adding dormant unique-history flags
to common code.

### 3. Enumerate retained hot main-branch versions

Add a narrow read-only helper in `trx/row.rs` for non-unique CREATE INDEX
history collection. Bind it through `UserTableAccessor`'s existing hot-page
scan so each row's version-map read latch protects traversal from concurrent
purge mutation.

For one hot RowID:

1. Read the current physical image. If it is live, record its indexed key as an
   active candidate.
2. Traverse only `NextRowUndo::main`; do not follow unique-index branches.
3. For each main transition, resolve its committed timestamp before applying
   the undo entry.
4. If the transition CTS is below `history_cutoff`, stop without reconstructing
   that version. The version ended before the oldest captured snapshot, and
   every still older main version ended no later.
5. Otherwise apply `Insert`, `Delete`, `Update`, or no-op `Lock` inverse
   semantics exactly as `read_row_mvcc` does.
6. If the reconstructed prior version is live, record its indexed key as a
   delete-masked historical candidate.
7. Fail closed if a non-committed transition is observed despite DDL writer
   exclusion; do not silently index an unstable version.

The helper needs only indexed values, deletion state, transition status, and
candidate state. It must not expose mutable undo internals or synthesize index
undo.

### 4. Normalize exact candidates and preserve delete-mask state

Introduce a create-build-only candidate representation equivalent to:

```rust
enum CreateIndexNonUniqueCandidateState {
    Active,
    DeleteMasked,
}

struct CreateIndexNonUniqueCandidate {
    encoded_key: BTreeKey,
    row_id: RowID,
    state: CreateIndexNonUniqueCandidateState,
}
```

The concrete names and storage location may follow local conventions, but the
state must be explicit rather than inferred from insertion order.

Normalize incrementally while each hot row's qualifying version chain is
traversed. Start with an empty row-local candidate vector. If the current
physical row is live, encode its exact `(key, row_id)` once and push it as the
first active candidate. Every subsequently reconstructed live version is
historical: encode it once, use binary search over the stored `BTreeKey` values,
ignore an existing exact key, and insert a missing key at the returned position
as delete-masked.

```text
same encoded (key, row_id):
    Active + any other state => Active
    DeleteMasked only        => DeleteMasked
```

This yields one candidate when a key disappears and later reappears on the same
RowID, and also collapses lock-only or non-indexed-column transitions that leave
the indexed key unchanged. Because the active candidate is inserted first,
ignoring an exact historical duplicate preserves active-state precedence.
MemIndex population consumes the retained encoded key directly instead of
encoding the logical key again. The captured pivot makes cold and hot RowID
ranges disjoint, and cold collection emits at most one candidate per RowID, so
combine the cold and already-normalized hot candidates directly. Any duplicate
that reaches MemIndex insertion remains an internal build invariant violation.

Current live cold rows remain solely in `DiskTree`; they do not need duplicate
active MemIndex entries.

### 5. Insert active and delete-masked build candidates

The existing non-unique insertion helper always inserts an active
`BTreeByte`. Add a narrow CREATE INDEX build insertion operation in
`index/non_unique_index.rs`, or an equivalently encapsulated constructor path,
that accepts the retained encoded exact key and inserts it directly in either
active or delete-masked state.

Coordinate runtime construction through a private
`CreateIndexRuntimeBuilder`. It holds only the shared pool, metadata, index
specification, and timestamp context; separate consuming `build_unique` and
`build_non_unique` methods accept only their variant-specific rows or
candidates.

Requirements:

1. The operation is unavailable to generic foreground callers unless they
   explicitly supply the build state.
2. It performs no DiskTree lookup and creates no index undo.
3. It uses `build_ts` for BTree mutation timestamps.
4. A duplicate after normalization is an internal build invariant violation.
5. After MemIndex construction succeeds and while the tree remains local to
   `CreateIndexRuntimeBuilder::build_non_unique`, a candidate-population
   failure destroys that unpublished tree before returning the original
   failure. A destroy failure is logged and does not replace the build source.
6. Only after the complete runtime index is returned and passed to
   `CreateIndexProgress::stage_runtime_index` does the progress owner become
   responsible for destroying it on later failure.

Do not implement history by inserting active and leaving it active. Delete
masks are required to reproduce the state that foreground index maintenance
would have produced if the index had existed during the historical update or
delete.

### 6. Preserve foreground key-reuse semantics

No new foreground DML result is required when build state is correct.

For a row that changed `A -> B` before CREATE INDEX, the build installs:

```text
(A, row_id) => delete-masked
(B, row_id) => active
```

A later `B -> A` update uses the existing
`merge_if_match_deleted` path to reactivate `(A, row_id)`, masks
`(B, row_id)`, and records ordinary index undo:

- commit leaves `A` active and `B` delete-masked;
- rollback re-masks `A` and restores `B` active.

Add explicit commit and rollback tests around
`update_non_unique_index_only_key_change`. If those tests expose a narrower
bug, fix the state transition without treating an already-active historical
duplicate as a newly owned insertion.

### 7. Keep publication and recovery current-state-only

Do not change the existing order:

1. build current cold `DiskTree`;
2. build the staged runtime index;
3. commit catalog metadata and DDL redo;
4. publish the table root;
5. install the runtime layout.

All historical-only entries live in the unpublished/staged/runtime MemIndex.
A failure while the builder still owns the unpublished tree destroys it
locally. After staging, a pre-commit failure destroys it through
`CreateIndexProgress`. A restart does not restore historical candidates, which
is correct because no pre-crash active transaction survives recovery. Recovery
must continue to rebuild only current hot MemIndex state over the durable
current cold DiskTree.

### 8. Prove safety with existing cleanup

Do not add deterministic reclamation, but verify the current cleanup rules
remain safe:

1. A delete-masked historical candidate behind a CDB marker is not removed
   while its delete CTS can still be visible to `Global_Min_STS`.
2. Hot transaction purge does not own build-created candidates without index
   undo and therefore cannot assume they were foreground insertions.
3. Full MemIndex cleanup may remove an entry only with its existing durable or
   global obsolescence proof and encoded compare-delete revalidation.
4. After all old snapshots drain, a build-created history entry may be
   reclaimed opportunistically or retained as a harmless false-positive
   candidate; this task makes no prompt-reclamation guarantee.

If implementation discovers that an existing cleanup path can remove a
build-created candidate before the captured horizon is safe, tightening that
proof is in scope. Creating a delayed build cleanup owner is not.

### 9. Update the design contract

Update `docs/secondary-index.md` to distinguish:

- current-row completeness for every active index;
- full retained historical candidate completeness for non-unique indexes
  created at runtime;
- unresolved full historical owner completeness for unique indexes, linked to
  backlog 000164.

Document the active-versus-delete-masked build invariant, captured cutoff, and
runtime-only restart property.

## Implementation Notes

## Impacts

Primary implementation files:

- `doradb-storage/src/catalog/index.rs`
  - `create_index_for_session`
  - purge-published history cutoff and root deletion-cutoff invariant
  - cold current/history collection
  - hot candidate collection
  - direct combination of disjoint cold and normalized hot candidates
  - captured-boundary collector with variant-specific cold/hot methods
  - variant-specific runtime builder and unpublished-tree cleanup
- `doradb-storage/src/trx/row.rs`
  - read-only retained main-branch key traversal
  - timestamp cutoff and inverse row-state reconstruction
  - row-local encoded exact-key normalization with active-state precedence
- `doradb-storage/src/table/access.rs`
  - CREATE INDEX hot-page scan binding
  - existing non-unique candidate recheck and key-reuse test coverage
- `doradb-storage/src/index/non_unique_index.rs`
  - build-only active/delete-masked exact insertion
- `doradb-storage/src/index/index_stream.rs`
  - no planned semantic change; its masked MemIndex candidate behavior is a
    relied-on invariant and may receive focused tests or comments
- `doradb-storage/src/table/gc.rs`
  - no planned production change unless safety tests expose an early-removal
    proof bug
- `docs/secondary-index.md`
  - non-unique CREATE INDEX history and deferred unique contract

Important existing call paths:

```text
Session::create_index
  -> catalog::create_index_for_session
  -> collect current cold rows + retained cold history
  -> build current DiskTree
  -> collect current/retained hot candidates
  -> build staged active/delete-masked MemIndex
  -> catalog commit -> root publish -> layout install
```

```text
non-unique lookup/scan
  -> MemIndex + captured DiskTree candidate stream
  -> index_lookup_candidate_row_mvcc
  -> row/CDB visibility + exact encoded-key recheck
```

No public API, catalog schema, table metadata, table-file root, redo, or
recovery format changes are expected.

## Test Cases

Use deterministic transaction ordering and semantic hooks/barriers where
concurrency is needed; do not use timing sleeps.

### Cutoff and candidate-state unit coverage

1. An active-root `deletion_cutoff_ts` equal to `history_cutoff` satisfies the
   build invariant.
2. An active-root `deletion_cutoff_ts` greater than `history_cutoff` triggers
   the release assertion with both cutoffs and the table id in its diagnostic.
3. Hot current live key is emitted active.
4. Hot current deleted image is not emitted as active.
5. A historical hot version with invalidation CTS equal to
   `history_cutoff` is retained delete-masked.
6. A historical hot version with invalidation CTS below
   `history_cutoff`, plus all older versions, is skipped.
7. A committed CDB marker equal to the cutoff produces a delete-masked cold
   candidate.
8. A committed CDB marker below the cutoff produces no candidate.
9. A persisted delete delta produces no candidate under the root cutoff proof.
10. A non-committed hot transition or CDB marker fails closed.
11. Repeated historical occurrences of one `(key, row_id)` deduplicate to one
   delete-masked candidate.
12. A key present both historically and currently deduplicates to one active
    candidate.

### End-to-end MVCC coverage

13. Start a reader, update a hot row's indexed key in another transaction,
    create the non-unique index, and prove the old reader finds the old key
    while a new reader finds only the current key.
14. Keep readers at multiple snapshots across `A -> B -> C`, create the index,
    and prove each reader obtains the same result through the index and a
    reference table scan.
15. Start a reader, delete a hot row, create the index, and prove the old
    reader finds the row while a new reader does not.
16. Checkpoint a row cold, start a reader, commit a cold delete without
    checkpointing the marker, create the index, and prove old/new snapshot
    agreement.
17. Checkpoint key `A` cold, keep an old reader active, update it to `B` so the
    replacement is hot with a different RowID, create the index, and prove the
    old reader finds the cold `A` candidate while a new reader finds only the
    hot `B` candidate.
18. Force a hot `A -> B` move update, assert that the old and replacement
    RowIDs differ, create the index, and prove old/new snapshot agreement for
    both keys.
19. Cover equality at the oldest active STS so the strict cutoff comparison
    cannot regress to `<=`.
20. Exercise exact lookup, equality scan, bounded range scan, and owned
    streaming scan through the new index where those public paths are
    available.

### DML, cleanup, failure, and restart coverage

21. Keep an old read-only transaction active across CREATE INDEX, then write an
    otherwise unchanged row through that transaction and commit normally.
22. After building historical `A` and current `B` for one RowID, update
    `B -> A` and commit; verify `A` is active and `B` remains a correct
    historical candidate.
23. Repeat `B -> A` and roll back; verify `A` returns to delete-masked and `B`
    returns to active without losing the old reader's candidate.
24. Run full MemIndex cleanup while an old snapshot still needs a hot or CDB
    historical candidate and prove the candidate remains usable.
25. Force candidate-population failure after the MemIndex is allocated but
    before `CreateIndexRuntimeBuilder::build_non_unique` returns; prove the
    builder destroys the unpublished tree and publishes no metadata, root,
    runtime index, or candidate state.
26. Force failure after `CreateIndexProgress::stage_runtime_index` but before
    catalog commit; prove the progress owner destroys the staged tree and
    publishes no metadata, root, runtime index, or candidate state.
27. Restart after an index was built with historical candidates and prove
    current index results remain correct without reconstructing pre-crash
    history.
28. Preserve existing unique CREATE INDEX tests and behavior; this task must
    not silently apply non-unique history rules to unique indexes.

### Validation

Run at minimum:

1. Focused `cargo nextest` tests for the affected row, index, access, GC, and
   CREATE INDEX modules.
2. `rtk cargo nextest run --workspace`
3. `rtk cargo fmt --all -- --check`
4. `rtk cargo clippy --workspace --all-targets -- -D warnings`

Run the libaio validation command from `AGENTS.md` if implementation changes
backend-neutral asynchronous storage paths rather than only row/index
semantics.

## Open Questions

1. Deterministic reclamation of historical non-unique MemIndex candidates
   remains deferred. During task resolution, preserve that remainder from
   backlog 000163 or create a narrower cleanup backlog before closing 000163;
   do not mark the broad source backlog fully implemented while cleanup remains
   untracked.
2. Backlog 000164 owns unique-index history. This task must not weaken or
   silently choose its owner-overlap policy.
3. A captured `history_cutoff` is intentionally conservative if active
   transactions finish during the build. An exact publication-time trim would
   require the deferred cleanup/second-pass design.
