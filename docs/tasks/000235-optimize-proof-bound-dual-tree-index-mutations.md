---
id: 000235
title: Optimize proof-bound dual-tree index mutations
status: proposal  # proposal | implemented | superseded
created: 2026-07-23
github_issue: 881
---

# Task: Optimize proof-bound dual-tree index mutations

## Summary

Reduce redundant MemIndex traversals and immutable DiskTree point probes in
user-table secondary-index mutations without weakening MVCC visibility,
duplicate detection, conflict handling, rollback, checkpoint, recovery, or
cleanup correctness.

The prerequisite live-cleanup bug is fixed first. A checkpointed live MemIndex
copy is removable only when the publishing root's post-publication
`effective_ts` is strictly older than every active transaction. Per-entry
DiskTree equivalence remains required. Cleanup uses one maintenance
transaction and one root snapshot for the table, returns
`MemIndexCleanupOutcome { stats, live_delay }`, and retries a root-capture race
immediately.

The mutation optimization is based on one documented current-row completeness
invariant and one opaque, single-use `OwnedRowIndexSetProof` for operations
that consume every old index entry. RowPage-owned in-place key changes bind
only the affected `OwnedOldIndexEntry` values after write ownership. The proof
has a root-relative storage mode, while an individually bound RowPage entry is
necessarily its `MemRequired` case:

- `MemRequired` means the captured DiskTree roots cannot contain the row, so
  every reconstructed old entry must still be active in MemIndex.
- `Persisted` means companion checkpoint publication placed the current row in
  the captured column root and its entries in every captured secondary
  DiskTree root. A matching MemIndex copy may exist, but is optional.

Both modes mutate only MemIndex because DiskTree is immutable. They differ only
in the legal result of the atomic MemIndex operation. `MemRequired` treats
absence as an invariant violation. `Persisted` treats absence as successful
logical masking and does not create a delete overlay, undo record, or purge
record. The column deletion buffer (CDB) remains the visibility authority until
deletion checkpoint removes the durable DiskTree entries.

New unique-owner claims retain a separate, exact `UniqueOwnerObservation`.
This token is useful because it prevents rereading an immutable DiskTree after
the existing row, key, deletion, and runtime-link checks. A Disk-sourced
observation is consumed with one atomic MemIndex replace-or-insert traversal.
It is not used to classify or authorize old-row index-set masking.

## Context

User-table secondary indexes combine:

- one mutable MemIndex shared by all root-bound operations; and
- one immutable DiskTree root per secondary index in each published table
  root.

Unique DiskTree entries map a logical key to the latest checkpointed owner
RowID. Non-unique DiskTree entries are exact
`(logical_key, row_id)` memberships. Foreground writes never mutate a
DiskTree. They insert, replace, or delete-mask MemIndex state; checkpoint and
deletion-checkpoint companion work produces later DiskTree roots.

The current composite mutation APIs retain several read-before-write costs:

- unique compare-exchange looks up MemIndex and then traverses it again for the
  atomic update;
- non-unique masking looks up an exact MemIndex entry and then traverses it
  again for the atomic update;
- a new hot non-unique exact entry probes DiskTree even though the captured
  pivot proves that exact RowID cannot occur there;
- old-row delete and update paths probe DiskTree even after row write
  ownership and root-relative row completeness establish the allowed MemIndex
  state; and
- a unique claim can observe a Disk owner, validate the owner through normal
  table semantics, then reread the same immutable DiskTree root before
  installing the new MemIndex owner.

Backlog 000086 recorded this work before root-bound secondary-index views,
caller-owned guards, bounded dual-tree streams, and cleanup-specific MemIndex
access existed. This task completes its useful foreground mutation scope.
Cleanup batching and page-local deletion remain tracked by backlog 000095.

Issue Labels:

- type:task
- priority:high
- codex

Source Backlogs:

- docs/backlogs/000086-secondary-index-dual-tree-access-path.md

Related Design:

- docs/architecture.md
- docs/index-design.md
- docs/secondary-index.md
- docs/transaction-system.md
- docs/data-checkpoint.md
- docs/garbage-collect.md
- docs/rfcs/0014-dual-tree-secondary-index.md
- docs/rfcs/0015-transaction-context-effects-root-proofs.md
- docs/process/coding-guidance.md
- docs/process/unit-test.md
- docs/tasks/000121-secondary-index-cleanup-hardening.md
- docs/tasks/000154-table-root-reachability-block-reclamation.md
- docs/backlogs/000095-secondary-mem-index-cleanup-pipeline-optimization.md
- docs/backlogs/000163-create-index-full-mvcc-history.md

## Investigation Findings

### Live cleanup can break root-bound MVCC

Suppose transaction `T` captures root `A` while row `r` is in RowStore.
`A`'s DiskTrees do not contain `r`. A checkpoint can persist `r`, publish root
`B`, and leave `T` correctly bound to `A`. Matching the shared live MemIndex
entry against `B` proves redundancy for `B`, but not for `T`: removing it makes
an `A`-bound lookup miss both layers.

The fix is a table-root gate:

```text
B.effective_ts() < Global_Min_Active_STS
```

Equality is unsafe. The gate must apply once to the captured table root, not
independently per entry. Per-entry DiskTree equivalence remains a second,
independent requirement.

### “Hot proof” and “cold proof” are ambiguous

Current physical routing and captured-root storage are not the same property.
A row with `row_id >= captured_pivot` can route to an LWC after a later
checkpoint, but it is absent from the captured DiskTree roots. Treating that
row as “cold” and allowing MemIndex absence would be incorrect.

Conversely, both MemIndex and DiskTree can select a row below the captured
pivot. The selection source does not determine whether the current row was
persisted in the captured root. A retained or recreated MemIndex copy does not
make the row “hot.”

The implementation therefore removes separate `HotRowIndexSetProof` and
`ColdRowIndexSetProof` concepts. One `OwnedRowIndexSetProof` records
`MemRequired` or `Persisted` relative to the captured root. Pre-ownership
selection source is discarded.

### Disk immutability does not imply every delete needs an overlay

All foreground old-entry transitions target MemIndex because DiskTree is
immutable. That fact alone does not require synthesizing a delete overlay for
every persisted entry.

After a current persisted row is validated and CDB ownership succeeds, the CDB
already makes the durable row and all its durable index candidates invisible
to later snapshots. If the exact old entry is absent from MemIndex, writing a
delete overlay adds BTree allocation/split risk, cache pollution, rollback
work, purge work, and cleanup work without adding visibility authority.

Persisted proof consumption therefore uses a conditional Mem-only mask:

- matching active Mem copy: mask it and record undo;
- absent Mem copy: succeed without writing anything;
- incompatible or already-masked Mem state: invariant failure after ownership.

The durable DiskTree entry remains until deletion checkpoint removes it.

### Generic index traits no longer describe production mutation

`UniqueIndex` and `NonUniqueIndex` combine guarded MemIndex operations with
root-bound MemIndex/DiskTree behavior, but production has no generic consumer
of either trait. User-table DML already calls concrete proof-aware APIs, while
rollback, recovery, catalog build, and catalog-table access bind the concrete
MemIndex types. The only generic trait bounds are test helpers instantiated
with one guarded MemIndex type.

The trait mutation results are also too weak for current production contracts:
generic unique insertion discards `UniqueOwnerObservation`, and generic
non-unique masking collapses `Masked`, `AlreadyMasked`, and `NotFound`.
General composite fallback mutations have no production DML caller and can
reintroduce DiskTree probes or overlays forbidden by the owned-row proof.

The implementation therefore removes both traits. Guarded MemIndex operations
and root-bound composite reads become inherent methods. Root-bound writes
expose only the observation-preserving or explicitly Mem-only operations used
by production.

### CREATE INDEX history is a confirmed deferred correctness issue

Current `CREATE INDEX` builds current cold and hot images, but it does not
construct every retained historical key/owner needed by transactions whose
snapshot predates index creation. Solving that requires history construction,
unique-owner interval handling, publication ordering, cleanup ownership, and
recovery design. It is confirmed and deferred to
`docs/backlogs/000163-create-index-full-mvcc-history.md`.

This task relies only on current-row completeness under a pinned compatible
layout. It does not claim historical candidate completeness.

## Goals

1. Require `captured_root.effective_ts() < Global_Min_Active_STS` before
   requested live MemIndex cleanup can remove any live entry.
2. Keep per-entry unique-owner or non-unique exact-entry DiskTree equivalence
   and final encoded atomic revalidation after the root gate.
3. Use one maintenance transaction and one captured root for all index cleanup
   in a table pass. Retry a root published after transaction start immediately.
4. Return completed cleanup accounting as
   `MemIndexCleanupOutcome { stats: MemIndexCleanupStats, live_delay }`.
5. Document and assert the pinned current-row index completeness invariant at
   root/layout capture, checkpoint sidecar production, proof construction, and
   proof consumption.
6. Add one non-`Clone`, single-use `OwnedRowIndexSetProof` with private fields
   and root-relative `MemRequired` / `Persisted` storage mode.
7. Construct the proof only after definitive RowPage write ownership or
   validated current-LWC plus successful CDB ownership.
8. For deletes and RowID moves, reconstruct exactly one old key for every
   active index under the pinned layout and bind that complete key set into the
   proof. For RowID-stable in-place updates, reconstruct and bind only indexes
   whose logical key changed.
9. Consume `MemRequired` old entries with one atomic MemIndex operation per
   entry and treat absence or incompatible state as an invariant failure.
10. Consume `Persisted` old entries with a conditional Mem-only mask. Do not
    install a delete overlay for absence and record undo only when a Mem entry
    was actually changed.
11. Preserve direct active unique owner replacement for unchanged-key RowPage
    moves, while keeping ordinary unique claim/link handling for cold
    replacement rows and all new unique logical keys.
12. Introduce `IndexMask` for exact non-unique MemIndex masking so
    `Masked`, `AlreadyMasked`, and `NotFound` remain distinguishable.
13. Retain `UniqueOwnerObservation` only for new unique-owner claims. Bind it
    to the exact root, key, owner state, and source; consume it once. Use one
    atomic replace-or-insert MemIndex traversal for a Disk-sourced observation.
14. Add a captured-pivot MemIndex-only insertion path for new non-unique exact
    entries whose new RowID is at or above the captured pivot.
15. Remove `UniqueIndex` and `NonUniqueIndex`; make guarded MemIndex operations,
    root-bound reads, and proof-aware writes explicit inherent APIs.
16. Remove unused lossy composite mutation adapters and name physical purge
    operations as Mem-only.
17. Preserve duplicate-key, write-conflict, retry, runtime unique-link, undo,
    rollback, purge, checkpoint, restart, and recovery semantics.
18. Close source backlog 000086 as implemented during `$task-resolve` after
    implementation and verification. Leave backlog 000095 open.

## Non-Goals

1. Do not skip DiskTree duplicate detection for a new unique logical key merely
   because the new RowID is hot.
2. Do not add a generalized `known_cold`, `hot`, or `skip_disk` boolean.
3. Do not expose constructors for proof values or allow caller assertions to
   substitute for table-owned validation.
4. Do not classify proof authority by whether the selected unique owner came
   from MemIndex or DiskTree.
5. Do not mutate DiskTree in foreground DML.
6. Do not install delete overlays for absent persisted old entries.
7. Do not use the owned-row proof for fresh inserts, reads, scans, historical
   versions, or another row.
8. Do not use the proof to skip unique duplicate checks or runtime owner-link
   validation for a replacement row.
9. Do not redesign CREATE INDEX, DROP INDEX, metadata versioning, or historical
   index construction. Backlog 000163 owns the confirmed CREATE INDEX issue.
10. Do not add MVCC covering-index scans. Current index entries do not contain
    transaction visibility, historical key versions, or versioned covered
    payloads; a future design must add versioned data or consult CDB/undo.
11. Do not redesign candidate streams, dual-tree merge order, checkpoint root
    formats, or persistent storage formats.
12. Do not implement cleanup batching, range matching, or page-local
    compare-delete from backlog 000095.
13. Do not change catalog-table MemIndex-only behavior.
14. Do not add production counters solely for tests.
15. Do not change unsafe BTree/page internals.

## Correctness Contracts

### Globally safe live MemIndex cleanup

MemIndex is shared across operations bound to different immutable root vectors.
For a captured root `R`, requested live cleanup is eligible only when:

```text
R.effective_ts() < Global_Min_Active_STS
```

`effective_ts` is installed after the active-root pointer swap. Every
transaction capable of observing the displaced root began before this fence.
Strict inequality is therefore required.

When the gate fails:

- live entries are retained and counted as skipped;
- delete-overlay cleanup continues under its independent obsolescence proofs;
- completed per-index statistics are returned; and
- `live_delay` reports the table, captured effective timestamp, and observed
  active horizon.

When the caller disabled live cleanup, live entries are retained by policy and
`live_delay` is `None`.

After the root gate succeeds, each live entry must still satisfy:

- `row_id < captured_pivot`;
- unique: captured DiskTree maps the same encoded logical key to the same
  RowID; or
- non-unique: captured DiskTree contains the same encoded exact
  `(logical_key, row_id)` entry.

Encoded compare-delete is the final atomic revalidation. A changed owner,
delete bit, or exact membership is never removed.

The whole table pass uses one maintenance transaction and one root snapshot.
If the captured root is not visible to that transaction because publication
raced transaction start, cleanup rolls back and immediately retries with a new
transaction. This is not a horizon delay and does not wait for an event.

### Pinned current-row index completeness

For one compatible captured table root `R`, pivot `P`, and pinned runtime
layout `L`, every current row has one metadata-derived current entry in every
active index:

- if `row_id >= P`, the row is absent from every secondary DiskTree root in
  `R`, and its active entries are required in MemIndex;
- if `row_id < P` and the row is a current, unconsumed LWC image in `R`, the
  row and every active index entry were published together in `R`; equivalent
  MemIndex copies may also remain.

This is a current-row invariant. It does not claim that a static DiskTree root
contains every historical candidate. In particular, it does not resolve the
CREATE INDEX history issue in backlog 000163.

Checkpoint establishes the persisted half of the invariant:

1. the checkpoint pins one metadata/layout identity;
2. every accepted current row is sent to every active secondary sidecar;
3. sidecar index identities exactly match active metadata identities;
4. the column root and all secondary roots are installed in one mutable root;
5. the table root is published atomically.

Foreground insertion and recovery establish the Mem-required half by
installing every active index entry before the row becomes independently
writable by another transaction. Row undo ownership prevents another writer
from using a partially indexed row.

### Root/layout compatibility assertions

Foreground root capture is a production hot path, so its release gate is
constant time. It asserts only that:

- the active-root metadata `Arc` is pointer-identical to the pinned layout
  metadata; and
- the root secondary slot count equals the layout slot count.

These checks are sufficient because structural validation happens once at the
state-transition boundaries:

- `TableRuntimeLayout` construction and installation validate slot count,
  active/inactive runtime presence, runtime index number, and unique/non-unique
  kind against the immutable metadata;
- table-root creation, index-DDL replacement, checkpoint publication,
  serialization, and loading validate the sparse secondary-root shape,
  including inactive `SUPER_BLOCK_ID` sentinels; and
- foreground metadata locks prevent statements from observing the index-DDL
  interval between root publication and matching layout installation.

An active index may legitimately have `SUPER_BLOCK_ID` as an empty DiskTree
root, so that value alone does not mean an active slot is inconsistent.

These foreground assertions are not applied blindly to maintenance cleanup.
Cleanup intentionally captures root metadata and current layout separately and
tolerates index-DDL skew by operating only on identities active in both.

### Root-relative owned-row index-set proof

The conceptual private shape is:

```rust
enum OwnedRowIndexStorage {
    MemRequired,
    Persisted,
}

enum OwnedRowMutationOwnership {
    RowPage,
    ColumnDeletionBuffer,
}

struct OwnedRowIndexSetProof {
    row_id: RowID,
    pivot_row_id: RowID,
    keys: Vec<SelectKey>,
    storage: OwnedRowIndexStorage,
    ownership: OwnedRowMutationOwnership,
    // private bindings to the compatible layout/root contract
}
```

The proof is non-`Clone`, statement-local, and consumed exactly once. It is
never stored in transaction effects, undo, purge state, a runtime layout, or a
later operation.

Construction requires:

1. a compatible pinned layout and captured `TableRootSnapshot`;
2. exactly one reconstructed key for each active index, no missing or duplicate
   index number;
3. the exact owned RowID and captured pivot;
4. either:
   - successful latest-row write ownership on a validated stable RowPage; or
   - validation of the current LWC image and selected key/MVCC state followed
     by successful CDB ownership.

Mode is derived only from the captured root:

```text
row_id >= captured_pivot  => MemRequired
row_id <  captured_pivot  => Persisted
```

A current LWC route does not override this relation. If a row at or above the
captured pivot moved to LWC after root capture, it remains `MemRequired`
because the captured DiskTrees cannot contain it. The cleanup effective-time
gate retains its MemIndex entries for the proof-owning transaction.

`Persisted` construction additionally resolves the row through the captured
column root and validates the same LWC row shape, selected key, MVCC state, and
CDB ownership. Either a Mem-sourced or Disk-sourced initial index lookup may
lead to this proof. Selection source is discarded after ownership.

The private ownership kind limits direct unchanged-key unique replacement.
Only RowPage-owned movement may replace active
`old_row_id -> new_row_id` directly. A CDB-owned cold replacement uses ordinary
new unique claim/link handling because the old physical row and new hot row
have distinct visibility histories.

### MemRequired old-entry consumption

For each proof key:

- unique delete or old-key removal:
  atomic MemIndex compare-exchange from active `row_id` to deleted `row_id`;
- non-unique delete, old-key removal, or RowID move:
  atomic exact MemIndex mask of `(key, row_id)`;
- unchanged-key unique RowPage move:
  atomic MemIndex compare-exchange from active `old_row_id` to active
  `new_row_id`;
- unchanged key and unchanged RowID:
  no index operation.

The only legal mutation results are:

- unique: `IndexCompareExchange::Ok`;
- non-unique: `IndexMask::Masked`.

`Mismatch`, `NotExists`, `AlreadyMasked`, or `NotFound` violates current-row
completeness after write ownership. The proof boundary fails closed with a
release assertion naming the violated invariant and containing table id, index
number, key, RowID, pivot, mode, and result. It never falls back to DiskTree and
never synthesizes an overlay.

Each successful transition records the existing undo payload and ordering.

### Persisted old-entry consumption

The proof never probes DiskTree during old-entry masking. Companion publication
already proves the durable current entries, and CDB owns visibility.

For each proof key:

- unique: atomically compare-exchange a matching active Mem owner to its
  deleted form;
- non-unique: atomically mask the matching active exact Mem membership.

Legal results are:

- unique `Ok`: mask occurred; record delete undo;
- unique `NotExists`: logical success; no write and no undo;
- non-unique `IndexMask::Masked`: mask occurred; record delete undo;
- non-unique `IndexMask::NotFound`: logical success; no write and no undo.

The following results violate post-ownership state:

- unique `Mismatch`; and
- non-unique `IndexMask::AlreadyMasked`.

An absent persisted Mem entry must not allocate a delete overlay. It creates no
undo, purge payload, or later MemIndex cleanup obligation. Rollback removes the
CDB marker and restores only Mem entries actually changed by this statement.
Deletion checkpoint eventually removes the durable row and its secondary
DiskTree entries.

Matching Mem copies remain possible because cleanup is per-index rather than
an atomic cross-index operation, and because rollback of a same-key cold update
can restore an old unique owner in MemIndex. The conditional mask must
therefore inspect every active index even though absence is allowed.

### Exact non-unique mask outcome

`IndexMask` replaces the ambiguous hypothetical
`NonUniqueMaskResult` name:

```rust
enum IndexMask {
    Masked,
    AlreadyMasked,
    NotFound,
}
```

The guarded non-unique MemIndex and root-bound
`mask_mem_if_present` operation return this result from one atomic BTree
update. Every caller retains all three states. `NotFound` never triggers a
DiskTree lookup or absent-entry overlay.

### Captured-pivot non-unique insertion

For captured pivot `P`, every exact non-unique entry in the associated DiskTree
has `row_id < P`. Therefore, a new exact
`(logical_key, new_row_id)` with `new_row_id >= P` cannot already exist in that
DiskTree even if older rows share the logical key.

A narrow table-owned helper:

- accepts the same `TableRootSnapshot` used to resolve the index slot;
- release-asserts `new_row_id >= snapshot.pivot_row_id()`;
- performs atomic MemIndex `insert_if_not_exists` directly;
- preserves the `merged` result for undo; and
- never opens DiskTree.

Fresh insert, cold replacement, RowPage move, and same-RowID key change use this
helper for new non-unique exact entries. Equality is valid because the pivot is
the first RowID not represented in the captured DiskTrees.

### Unique-owner observation

`UniqueOwnerObservation` is retained only for a new unique logical-key claim.
It is an exact selection token, not row visibility proof, write ownership,
reservation, or old-entry index-set proof.

Its private conceptual shape binds:

- one copy of the exact root-bound `UniqueSecondaryIndex`;
- the borrowed logical key;
- observed owner RowID and delete state; and
- source layer (`Mem` or `Disk`).

An observation-preserving insert attempt returns:

```rust
enum UniqueInsertAttempt {
    Inserted { merged: bool },
    Occupied(UniqueOwnerObservation),
}
```

Construction follows composite selection:

1. MemIndex hit produces a Mem observation;
2. MemIndex miss plus captured DiskTree hit produces a Disk observation;
3. complete miss attempts atomic MemIndex insert; a raced duplicate produces a
   Mem observation.

Before consuming the token, the table layer performs all existing owner checks:
row-location resolution, MVCC/CDB visibility, latest-key recheck, and runtime
unique-owner link construction where required.

Consumption is single-use:

- Mem observation: atomic MemIndex compare-exchange. `Ok` claims the key,
  `Mismatch` conflicts, and `NotExists` retries the normal claim loop.
- Disk observation: one atomic MemIndex replace-or-insert. True absence inserts
  the new live owner; an equivalent active expected-owner Mem copy that
  appeared after observation is replaced; any other current value, including
  a delete-state mismatch, returns `Mismatch` without overwrite. This branch
  cannot return `NotExists`.

Disk observation consumption performs one MemIndex traversal and never rereads
the same immutable DiskTree root. The earlier MemIndex lookup used to select
the source remains a separate operation before owner validation.

### Unique and non-unique mutation matrix

| Case | Unique index | Non-unique index |
| --- | --- | --- |
| Fresh/new logical key | Normal owner observation, duplicate/MVCC/link validation, then claim | Captured-pivot Mem-only exact insert |
| `MemRequired` old key removed | Active owner to deleted owner; only `Ok` | Exact atomic mask; only `Masked` |
| `MemRequired` unchanged-key RowPage move | Active old RowID to active new RowID | Insert new exact entry, then exact-mask old entry |
| `Persisted` old key removed | Mask matching Mem owner if present; absence is no-op | Mask matching exact Mem entry if present; absence is no-op |
| CDB-owned cold replacement new key | Normal owner observation/link handling | Captured-pivot Mem-only exact insert |
| In-place unchanged key | No operation | No operation |

New-side insertion and old-side proof consumption remain separate. A proof
about one old current row never authorizes a different unique owner claim.

## Implementation Plan

### 1. Finalize the task contract before code changes

- Replace hot/cold proof terminology with the unified root-relative proof.
- Record the current-row invariant, assertion sites, and mutation matrix.
- Record conditional persisted masking and the no-overlay/no-undo absence rule.
- Confirm CREATE INDEX MVCC history as deferred backlog 000163.
- Keep this document as the implementation source of truth.

### 2. Preserve and verify the live-cleanup prerequisite

- Keep `MemIndexCleanupStats` as the aggregate stats name.
- Keep
  `MemIndexCleanupOutcome { stats, live_delay: Option<MemIndexCleanupDelay> }`.
- Keep the root-level strict effective-time gate and independent delete-overlay
  cleanup.
- Keep one maintenance transaction/root snapshot for every active index.
- Keep immediate transaction retry when `root.effective_ts() >= cleanup_sts`
  proves a root-capture race.
- Keep cleanup hooks inside the `table::gc` test module.

### 3. Add root/layout and checkpoint invariant assertions

- Keep one O(1) foreground helper called by
  `UserTableAccessor::root_snapshot`; assert active-root metadata pointer
  identity and matching root/layout slot counts.
- Validate active/inactive runtime presence, index number, and unique kind once
  in `TableRuntimeLayout` construction/installation.
- Keep sparse secondary-root shape and inactive-root sentinel validation at
  root creation, DDL replacement, checkpoint publication, serialization, and
  load boundaries.
- Add proof-key validation that the sorted/indexed key set covers every active
  metadata index exactly once.
- At secondary checkpoint sidecar creation/application, assert active sidecar
  identities equal active metadata identities.
- Assert each accepted current row is fanned out to every active sidecar before
  root publication.
- Keep column and secondary root publication atomic; document the existing
  mutable-root publication boundary.

### 4. Add atomic index-layer primitives

- Add `IndexMask` beside non-unique MemIndex operations.
- Add guarded `mask_if_present` using one BTree update.
- Add Mem-only exact insertion on the root-bound non-unique composite.
- Add Mem-only compare-exchange on the root-bound unique composite.
- Add a BTree and guarded unique-MemIndex replace-or-insert primitive that
  inserts on absence, replaces only an exact expected full value, and otherwise
  returns the competing value without mutation.
- Remove the unused Disk-fallback composite compare-exchange and masking
  adapters instead of retaining a second mutation contract.

### 5. Align index APIs with production

- Remove `UniqueIndex` and `NonUniqueIndex`, their re-exports, implementations,
  and imports.
- Move guarded MemIndex operations and root-bound reads into inherent
  implementations with concrete candidate-stream return types.
- Exercise composite behavior through the same lookup and candidate-stream APIs
  used by production. Do not add test-only associated methods to production
  index implementations; use production cleanup scans for encoded-entry
  revalidation tests.
- Rename root-bound physical removal to `compare_delete_mem` for both index
  kinds.
- Remove unused root-bound general insert, mask/unmask, compare-exchange, and
  cold-overlay helpers.
- Keep test behavior on concrete production paths. Retain root-rebinding
  wrappers only as lifetime conveniences for production reads, and name direct
  Mem mutations as explicit state injection for stale/shadow states that normal
  table operations cannot construct. Inject malformed physical state only when
  validating production corruption checks.

### 6. Add and integrate unique-owner claim observations

- Add non-`Clone` `UniqueOwnerObservation` and `UniqueInsertAttempt` with private
  fields.
- Expose owner/delete accessors and one consuming replace operation.
- Keep Mem observations on compare-exchange and consume Disk observations with
  one replace-or-insert MemIndex traversal.
- Update `insert_unique_index`,
  `update_unique_index_key_and_row_id_change`, and
  `update_unique_index_only_key_change` to retain the occupied observation
  through current validation and consume it for the claim.
- Preserve existing duplicate, link, retry, conflict, and undo behavior.
- Do not use the observation in old-entry proof construction.

### 7. Add captured-pivot non-unique insertion

- Add a narrow `UserTableAccessor` helper receiving the root snapshot.
- Release-assert the new RowID/pivot relation with full diagnostics.
- Route all new user-table non-unique exact entries through MemIndex only.
- Preserve merge flags and existing insert undo.

### 8. Add the unified owned-row proof

- Define private `OwnedRowIndexStorage`,
  `OwnedRowMutationOwnership`, and `OwnedRowIndexSetProof` in table access.
- Keep the proof non-`Clone` and field-private.
- Add one constructor after successful RowPage write ownership.
- Add one constructor after current-LWC validation and successful CDB
  ownership.
- Derive mode from captured pivot, not current physical route or selected index
  source.
- For `Persisted`, validate the row against the captured column root.
- Validate the complete old-key set at construction.
- Allow a RowPage-owned `MemRequired` old entry to be bound individually after
  write ownership when a RowID-stable update changes only that index key.

### 9. Consume proof-bound old entries

- Replace per-key general composite deletion calls with one proof-consuming
  table helper.
- Implement the exact legal result sets for `MemRequired` and `Persisted`.
- Add release assertions that name the violated invariant and include
  table/index/key/row/pivot/mode/result diagnostics for illegal outcomes.
- Attach recoverable operation and phase context at the semantic table-operation
  caller instead of forwarding diagnostic strings through leaf helper APIs.
- Record delete undo only for entries actually changed.
- Never install an overlay for persisted absence.
- Keep direct unchanged-key unique owner replacement limited to RowPage-owned
  moves.
- Route hot delete, RowPage move, cold update, cold delete, and their full-table
  or unique-key selected variants through the complete proof.
- Route RowID-stable hot key changes through individually bound affected
  entries. Read each changed new key once and do not reconstruct unchanged
  index keys.

### 10. Preserve rollback, purge, checkpoint, and recovery behavior

- Verify every actual Mem mask/replacement retains its existing undo payload.
- Verify persisted absence contributes no undo or purge work.
- Verify statement rollback removes CDB ownership and restores only changed
  Mem entries.
- Verify commit purge never expects an undo for disk-only persisted entries.
- Verify deletion checkpoint removes durable old entries.
- Verify restart reconstructs the same visible index results.

### 11. Update design documentation and tests

- Update `docs/secondary-index.md` with the current-row invariant, proof modes,
  unique observation, mutation matrix, and forbidden shortcuts.
- Update `docs/garbage-collect.md` with the effective-time gate and conditional
  persisted masking lifecycle.
- Add focused index primitive, proof-mode, integration, rollback, checkpoint,
  and recovery tests.
- Use isolated existing buffer-pool access snapshots for probe-count tests.

### 12. Resolve only after verification

- Run the required formatting, lint, focused, and workspace validations.
- Perform the task-resolve style gate.
- Close backlog 000086 as implemented with task/implementation references.
- Leave backlog 000095 and backlog 000163 open.

## Impacts

- `doradb-storage/src/table/gc.rs`
  - completed cleanup root gate, one-transaction retry, cleanup outcome
- `doradb-storage/src/index/non_unique_index.rs`
  - `IndexMask`
  - atomic exact MemIndex masking
  - inherent guarded MemIndex API
- `doradb-storage/src/index/secondary_index.rs`
  - Mem-only mutation primitives
  - unique-owner observation and insert attempt
  - inherent root-bound reads and explicit physical purge
  - removal of unused fallback mutation adapters
- `doradb-storage/src/index/unique_index.rs`
  - inherent guarded MemIndex API
- `doradb-storage/src/index/mod.rs`
  - removal of obsolete trait re-exports
- `doradb-storage/src/table/access.rs`
  - root/layout assertions
  - captured-pivot non-unique insertion
  - unified owned-row proof construction and consumption
  - conditional persisted masking and undo integration
  - unique claim observation integration
- `doradb-storage/src/table/persistence.rs`
  - checkpoint sidecar identity/fanout assertions and invariant comments
- `doradb-storage/src/table/mod.rs`
  - narrow snapshot accessors only if needed for proof diagnostics
- `docs/secondary-index.md`
  - invariant and proof-bound foreground mutation contract
- `docs/garbage-collect.md`
  - cleanup root fence and persisted deletion lifecycle
- inline test modules adjacent to changed production paths

## Test Cases

### Live cleanup

1. Capture old root `A`, publish root `B` containing a formerly hot row, and
   verify requested cleanup retains its unique and non-unique live entries
   while an `A`-capable transaction keeps
   `B.effective_ts() >= min_active_sts`.
2. Cover equality as retained.
3. End the protecting transaction, advance the production horizon, rerun, and
   verify matching live entries are removed while `B`-bound lookups succeed.
4. Verify root-gated live delay does not prevent independently proven
   delete-overlay removal.
5. Verify caller-disabled live cleanup returns stats with no live delay.
6. Publish after cleanup transaction start but before capture and verify
   immediate transaction retry.
7. Use semantic hooks/barriers in test modules, never elapsed-time sleeps.

### Atomic index primitives

1. Unique Mem hit compare-exchanges without preliminary lookup or Disk access.
2. Unique mismatch remains `Mismatch`.
3. Unique Mem absence remains `NotExists`; only a retained
   `UniqueOwnerObservation` can authorize replace-or-insert.
4. Non-unique active exact entry returns `IndexMask::Masked`.
5. Already-deleted exact entry returns `AlreadyMasked` without Disk access.
6. True Mem absence returns `NotFound` without probing DiskTree or installing
   an overlay.
7. Root-bound physical purge calls the explicitly Mem-only API.

### Captured-pivot non-unique insertion

1. Insert the same logical key under `row_id > pivot` and verify zero disk-pool
   access.
2. Cover `row_id == pivot`.
3. Verify `row_id < pivot` triggers the release assertion with diagnostics.
4. Verify back-and-forth same-RowID key changes preserve merge undo.
5. Verify rollback removes a fresh exact claim or restores a merged mask.

### Unique-owner observation

1. Mem live and delete-shadow observations replace only their exact state.
2. Mem disappearance returns `NotExists` for retry.
3. Mem owner replacement returns `Mismatch` and preserves the competitor.
4. Disk observation claim performs no second DiskTree access.
5. An equivalent expected-owner Mem entry appearing after Disk observation is
   atomically replaced in the same traversal used for the absence case.
6. A competing Mem owner makes the claim fail without overwrite.
7. A delete-masked copy of the observed active Disk owner is a full-value
   mismatch and is preserved.
8. A height-zero MemIndex access-counter test observes one pool hit while
   consuming a Disk observation, rather than separate update and insert hits.
9. Compile-time ownership prevents reuse or application to another key/root.

### MemRequired proof

1. With multiple unique and non-unique indexes, hot delete masks every old
   entry through one atomic Mem operation and zero Disk probes.
2. Cover in-place key change, changed-key RowPage move, unchanged-key unique
   RowPage move, and full-table known-hot mutation.
3. Verify unchanged-key unique move installs active new RowID directly.
4. Inject `NotExists`, `NotFound`, `Mismatch`, and `AlreadyMasked` at the proof
   boundary and verify fail-closed diagnostics with no fallback overlay.
5. Pause checkpoint publication so a row becomes current-LWC after capture but
   remains `row_id >= captured_pivot`; verify it is still `MemRequired`.
6. Verify cleanup retains its Mem entries until the proof-owning transaction
   ends.
7. Verify rollback restores every actual transition.

### Persisted proof

1. Checkpoint one row with multiple indexes, validate current-LWC and CDB
   ownership, and consume all reconstructed keys with no Disk point probes.
2. Cover initial selected owner from MemIndex and DiskTree; both produce
   `Persisted` when `row_id < captured_pivot`.
3. With no Mem copies, verify every old entry is a no-op: no delete overlay, no
   index undo, no purge payload.
4. With retained matching Mem copies, verify they are masked and receive undo.
5. Verify mixed presence across indexes masks only present copies.
6. Verify unique mismatch and non-unique already-masked state fail closed.
7. Roll back and verify the CDB marker is removed and only changed Mem copies
   are restored.
8. Commit and deletion-checkpoint; verify durable old DiskTree entries are
   removed and no unnecessary Mem overlays remain.

### Invariant assertions

1. Validate the O(1) foreground gate accepts compatible root metadata pointer
   and slot shape, and rejects identity or slot-count mismatches.
2. Inject active-runtime, index-number, and unique-kind mismatches and verify
   `TableRuntimeLayout` construction rejects them.
3. Inject malformed sparse root shapes and inactive-root sentinels at root
   lifecycle boundaries and verify rejection.
4. Verify an active empty index may use `SUPER_BLOCK_ID`.
5. Omit or duplicate a reconstructed key and verify proof construction fails.
6. Verify source Disk row at or above captured pivot is impossible.
7. Verify persisted construction rejects a row/key/LWC shape inconsistent with
   the captured column root.
8. Verify checkpoint sidecar identities and per-row fanout cover every active
   index before atomic root publication.

### End-to-end behavior

1. Fresh insert and hot/cold replacement preserve unique duplicates and
   non-unique contents.
2. Unique key changes preserve duplicate, conflict, retry, and runtime-link
   behavior.
3. Statement failure and transaction rollback restore all row, CDB, and actual
   MemIndex changes.
4. Checkpoint followed by restart yields the same unique and non-unique visible
   rows.
5. Existing CREATE INDEX tests remain unchanged; backlog 000163 continues to
   track historical candidate completeness.

### Validation

1. Run focused nextest filters for index primitives, cleanup, mutation proofs,
   checkpoint, rollback, and recovery.
2. Run `rtk cargo fmt --check`.
3. Run `rtk cargo clippy --workspace --all-targets -- -D warnings`.
4. Run `rtk cargo nextest run --workspace`.
5. Run the alternate `libaio` workspace pass only if implementation expands
   into backend-neutral I/O code.

## Acceptance Criteria

1. Requested live cleanup removes no live entry unless the captured root
   satisfies strict `effective_ts < min_active_sts`.
2. Per-entry durable equivalence and final atomic revalidation remain required
   after the root gate.
3. Cleanup uses one transaction/root snapshot for all indexes, retries capture
   races immediately, and returns
   `MemIndexCleanupOutcome { stats, live_delay }`.
4. The pinned current-row completeness invariant is documented and guarded by
   release assertions at root/layout, checkpoint, proof construction, and
   proof consumption boundaries.
5. One non-`Clone` `OwnedRowIndexSetProof` replaces separate hot/cold proof
   concepts.
6. RowID-stable hot updates bind and reconstruct only affected old index
   entries, while RowID moves capture the complete old index-key set from the
   returned old row and read replacement-row keys only for logically changed
   indexes.
7. Proof mode is derived from captured pivot, never current route or initial
   index source.
8. A row at or above captured pivot remains `MemRequired` even if it later
   routes to LWC.
9. Both Mem- and Disk-selected current rows below captured pivot may become
   `Persisted` after captured-root LWC validation and CDB ownership.
10. `MemRequired` old-entry consumption performs one atomic MemIndex mutation
   per affected entry, zero Disk probes, and fails closed on absence or
   incompatible state.
11. `Persisted` old-entry consumption conditionally masks MemIndex only, treats
    absence as success, and never creates an absent-entry overlay.
12. Persisted absence creates no index undo, purge payload, or cleanup work.
13. Rollback restores only entries actually changed and removes CDB ownership.
14. `IndexMask` distinguishes `Masked`, `AlreadyMasked`, and `NotFound`.
15. New non-unique exact entries at or above captured pivot perform no DiskTree
    point read and preserve merge undo.
16. New unique logical keys still use normal duplicate-owner observation,
    MVCC/key validation, and runtime-link handling.
17. A Disk-sourced `UniqueOwnerObservation` is consumed with one atomic
    MemIndex replace-or-insert traversal and without rereading its immutable
    DiskTree root.
18. No proof or observation can be rebound to another key, row, root, or
    statement.
19. `UniqueIndex` and `NonUniqueIndex` are removed; production and tests use
    concrete guarded or root-bound APIs with no lossy composite mutation
    adapters.
20. Duplicate-key, conflict, retry, undo, rollback, purge, checkpoint,
    recovery, and visible query behavior remain unchanged.
21. CREATE INDEX history remains explicitly deferred to backlog 000163 and is
    not used as evidence for historical completeness.
22. Required focused tests, formatting, lint, and workspace tests pass.

## Implementation Notes

- The cleanup prerequisite and its public outcome rename are already
  implemented in the task worktree and must be preserved while the foreground
  proof work proceeds.
- No unsafe-code change is expected. If implementation reaches unsafe
  BTree/page internals, stop and apply
  `docs/process/unsafe-review-checklist.md`.
- Cleanup maintenance intentionally tolerates root/layout skew and must not
  reuse foreground compatibility assertions without preserving that behavior.

## Open Questions

None.
