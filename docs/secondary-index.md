# Secondary Index Design

## 1. Overview

This document is the authoritative design for Doradb secondary indexes.

It replaces the old secondary-index sections in
[`index-design.md`](./index-design.md) and defines:

- the overall secondary-index architecture
- the in-memory `MemIndex`
- the persistent `DiskTree`
- read and write behavior
- checkpoint behavior
- recovery behavior

This document only covers secondary indexes. The RowID-to-location block index
is documented in [`block-index.md`](./block-index.md). The higher-level split
between block index and secondary index is summarized in
[`index-design.md`](./index-design.md).

Scope note: the `MemIndex`/`DiskTree` split in this document applies to
user-table secondary indexes. Catalog-table secondary indexes remain purely
in-memory and continue to use the existing single-tree runtime implementation.
Catalog tables do not receive `DiskTree` roots, checkpoint companion
secondary-index publication, or composite runtime integration.

## 2. Design Goals

User-table secondary indexes must satisfy the following constraints:

1. Foreground writes stay memory-first and do not perform random persistent
   index writes.
2. Persistent secondary-index state is always committed state.
3. Restart only needs to restore the latest committed state.
4. Runtime MVCC for old versions remains heap-based. Old snapshots are served by
   row undo chains and deletion buffers, not by keeping arbitrary historical
   index versions on disk.
5. Cold-row checkpoint and cold-row deletion checkpoint are the only operations
   that publish new `DiskTree` roots.

## 3. High-Level Architecture

Each user-table secondary index is split into two layers:

1. `MemIndex`
   - in-memory
   - mutable
   - serves hot recent changes
   - updated on foreground writes
2. `DiskTree`
   - persistent CoW B+Tree
   - stores checkpointed cold state
   - published only together with table checkpoint metadata

The key design choice is that `DiskTree` is not an independently flushed
committed-prefix structure. It is a companion of checkpointed table state:

- when row pages are checkpointed into persistent LWC blocks, their index state
  is merged into `DiskTree`
- when committed cold-row deletions are checkpointed into persistent delete
  bitmaps, the corresponding secondary-index removals are merged into `DiskTree`

As a result:

- there is no separate index checkpoint thread that scans committed `MemIndex`
  entries
- there is no `Index_Rec_CTS`
- restart does not need an index-specific replay floor

## 4. Physical Model

### 4.1 Unique Index

Unique index entries follow the current runtime model:

- the key is the logical unique key
- the value is the latest row id for that key
- runtime MVCC for older snapshots is recovered from the returned row id by
  following heap undo/history and, when ownership of the same logical key has
  moved across different row chains, runtime unique-key links

This means the unique latest-mapping model is valid only together with one
additional core MVCC contract:

- if an older snapshot may still need a previous owner of the same logical key
  and that previous owner is not reachable by ordinary undo traversal from the
  latest row id, the runtime must install a unique-key link from the newer row
  version to the older owner/version for that key
- the older owner/version reached through that runtime unique-key link may be
  either:
  - another hot row/version in RowStore
  - or an older cold owner/version that is resolved through the cold
    row/deletion path
- these links are runtime-only MVCC structures
- they are not part of checkpointed `DiskTree` state
- they only need to remain valid while a live snapshot can still require them
- although the checkpointed unique key space is logical-key based, companion
  deletion maintenance must still use row identity as a guard: a persisted
  unique-key delete/update is applied only if the stored owner for that key
  still matches the deleted old row id

This is valid because Doradb does not preserve pre-crash active snapshots across
restart. Recovery only needs the latest committed mapping.

### 4.2 Non-Unique Index

Non-unique index entries use exact physical identity:

- the key is `(logical_key, row_id)`
- the durable `DiskTree` entry represents one live checkpointed exact entry for
  that key
- delete-marked exact entries are runtime overlay state in `MemIndex`, not a
  separate durable tombstone shape in `DiskTree`

This is required because duplicates cannot be collapsed into a single latest
logical-key mapping.

### 4.3 Implication

Unique and non-unique indexes do not share the same physical shape:

- unique: logical-key latest mapping
- non-unique: exact entry keyed by logical key plus row id

The secondary-index runtime must treat them differently on lookup, scan,
checkpoint, and delete processing.

### 4.4 Durable Shapes And Delete Representation

For v1, `DiskTree` stores only live checkpointed secondary-index state:

- unique `DiskTree`:
  - logical key -> latest checkpointed owner row id
- non-unique `DiskTree`:
  - a key-only exact-entry set keyed by `(logical_key, row_id)`
  - key presence is the whole durable fact; there is no logical value payload
    for a non-unique `DiskTree` entry
  - exact delete physically removes the `(logical_key, row_id)` key from the
    set
- no durable secondary-index tombstones are stored in `DiskTree`

Non-unique `DiskTree` must not expose delete-mask operations. The delete bit
used by the current in-memory non-unique B+Tree value is a `MemIndex` overlay
concern only. Reusing B+Tree search, split, merge, or cursor code must not turn
that runtime byte into a persisted `DiskTree` API or format contract.

Delete representation is split across runtime overlay state and cold delete
state:

- `MemIndex` carries delete-shadows or delete-marked exact entries so hot lookup
  does not fall through to stale cold `DiskTree` state
- the deletion buffer and published delete bitmap remain the cold-row delete
  authority
- deletion-checkpoint companion work then removes or conditionally updates the
  corresponding durable `DiskTree` entry

This keeps the persistent secondary-index shape simple: `DiskTree` holds the
latest checkpointed cold index state, while delete overlay semantics remain a
runtime concern until companion delete work is published.

## 5. MemIndex

`MemIndex` is the hot mutable layer.

### 5.1 Role

- absorb foreground inserts, updates, and deletes
- shadow stale `DiskTree` entries until the next table checkpoint
- serve recent hot lookups without touching persistent index pages

### 5.2 Transaction Semantics

`MemIndex` follows the current runtime transaction model:

- foreground writes update `MemIndex` before commit
- `MemIndex` may therefore contain in-flight transactional state, not only
  committed state
- rollback and write-conflict handling remain part of the normal runtime
  protocol
- checkpointed `DiskTree` state still contains committed cold state only

As a result, the standalone secondary-index design does not need a per-entry
`sts` field to make `MemIndex` transactional. The design also does not use an
eagerly maintained per-entry `dirty` bit. Visibility, rollback, and conflict
handling remain responsibilities of the wider runtime MVCC/transaction
protocol, while cleanup is derived later from published checkpoint metadata and
deletion-overlay state.

### 5.3 Unique MemIndex Entries

For a unique index, `MemIndex` stores the latest logical-key mapping.

Interpretation:

- a live entry maps the logical key to the current row id
- a delete-shadow entry is an overlay entry for the latest hot logical-key
  state; it retains the referenced row id and prevents blind fallback to an
  older cold `DiskTree` mapping
- a unique delete-shadow does not by itself prove that the key is invisible to
  every snapshot; readers still route the retained row id through the normal
  row/deletion visibility path

### 5.4 Non-Unique MemIndex Entries

For a non-unique index, `MemIndex` stores exact entries keyed by
`(logical_key, row_id)`.

Interpretation:

- each hot duplicate occupies its own entry
- delete-marked exact entries are overlay state that can shadow stale older
  tree results for the same exact entry
- `MemIndex` and `DiskTree` results must be merged for scans

### 5.5 MemIndex Cleanup

`MemIndex` cleanup is a proof-based full-scan pass over user-table secondary
indexes. The pass captures one active table-file root and compares each scanned
entry against the corresponding `DiskTree` root from that same snapshot before
issuing an encoded compare-delete. Cleanup never mutates `DiskTree` and never
rebuilds checkpointed cold entries into `MemIndex`.

Valid cleanup decisions:

- live unique entries can be removed only when the row id is below the captured
  `pivot_row_id` and the captured unique `DiskTree` maps the same encoded
  logical key to the same row id
- live non-unique exact entries can be removed only when the row id is below the
  captured `pivot_row_id` and the captured non-unique `DiskTree` contains the
  same encoded exact `(logical_key, row_id)` key
- delete overlays can be removed with overlay-obsolescence proof even when the
  captured `DiskTree` still contains a stale cold entry; valid proofs are a
  deletion-buffer marker committed before `Global_Min_STS`, a captured
  checkpoint root older than `Global_Min_STS` whose `ColumnBlockIndex` proves
  the row id is absent below the captured pivot, or a captured cold LWC row
  whose current indexed values encode to a different scanned MemIndex key
- removing a proven row-deletion overlay does not mutate `DiskTree`; any stale
  cold entry exposed by the overlay removal is filtered by normal row/deletion
  visibility checks
- cold-row key mismatch covers committed key changes where the row still
  exists but no longer owns the scanned delete overlay's key; hot row-page key
  mismatch remains transaction index GC's responsibility

Invalid shortcuts:

- deletion-buffer absence is not a cleanup proof
- `row_id < pivot_row_id` alone is not a cleanup proof for delete overlays
- `RowLocation::NotFound` from a moving current root is not a cleanup proof
- hot row-page key mismatch is not proven by the full-scan cleanup pass
- cleanup must not collect runtime unique-key links; those follow undo-chain GC
  and `Global_Min_STS`

There is no separate MemIndex-to-DiskTree flush pass and no need for a
post-publish sweep that flips touched entries from dirty to clean.

## 6. DiskTree

`DiskTree` is the durable secondary-index structure for checkpointed cold data.

### 6.1 Role

- store secondary-index state for persistent LWC rows
- remain consistent with the table checkpoint root
- serve cold lookups and scans after restart before redo reconstructs hot state

### 6.2 Structure

`DiskTree` is a CoW B+Tree:

- root page ids are stored in the table checkpoint metadata
- updates allocate new pages and publish a new root atomically with the table
  checkpoint root
- old pages are reclaimed through normal CoW garbage collection

Each secondary index has its own `DiskTree` root.

### 6.3 Ownership

`DiskTree` represents only checkpointed cold state:

- it does not include uncheckpointed hot RowStore inserts or updates
- it stores live checkpointed secondary-index entries only, not a separate
  durable tombstone layer
- it does not need to represent a standalone committed CTS prefix
- it may be stale relative to hot `MemIndex`, and runtime lookup rules must
  account for that
- cold-row deletes are represented by deletion overlay state until companion
  checkpoint work removes or conditionally updates the durable entry

## 7. Read Path

### 7.1 Unique Point Lookup

Runtime lookup for a unique secondary index proceeds as follows:

1. Probe `MemIndex` by logical key.
2. If `MemIndex` hits:
   - if the entry is live, use its row id as the candidate entry point for the
     normal row/deletion visibility path
   - if the entry is a delete-shadow, do not fall through to a stale
     `DiskTree` value; instead use the retained row id as the candidate entry
     point for the normal row/deletion visibility path
3. If `MemIndex` misses, probe `DiskTree`.
4. Route the candidate row id through the normal row lookup path:
   - RowStore + undo chain for hot rows
   - LWC + deletion buffer / persisted delete bitmap for cold rows
5. For unique indexes, MVCC resolution may additionally follow runtime
   unique-key links when the visible older owner of the logical key is not on
   the ordinary undo chain of the latest row id. That linked older owner may be
   either a hot row/version or an older cold owner/version.
6. After visibility resolution, recheck that the visible row version still
   matches the logical lookup key. If the visible version no longer matches,
   treat the candidate as a stale index result and skip it.

Contract: for unique lookup, a `MemIndex` hit is terminal for tree selection.
This includes both live entries and delete-shadow entries. Once hot
logical-key state is present, lookup must not fall through to older `DiskTree`
state for the same logical key. A delete-shadow's retained row id is still only
a candidate entry point: final visibility comes from the normal row/deletion
MVCC path plus the final key recheck.

### 7.2 Non-Unique Lookup and Scan

For non-unique indexes, results may exist in both trees at the same logical
key:

1. Probe or range-scan `MemIndex`.
2. Probe or range-scan `DiskTree`.
3. Merge exact entries from both sources.
4. Use delete-marked `MemIndex` exact entries only to suppress matching
   `DiskTree` exact `(logical_key, row_id)` entries.
5. Route the remaining candidate row ids through the normal row/deletion
   visibility check.
6. After visibility resolution, recheck that each visible row version still
   satisfies the lookup key or scan predicate. Return only rows that still
   satisfy that predicate.

Contract: for non-unique lookup and scan, exact entries from both trees are
merged. A `MemIndex` hit is not terminal because the cold tree may still contain
additional duplicates for the same logical key. A delete-marked `MemIndex` exact
entry suppresses only the matching cold exact `(logical_key, row_id)` entry.
Remaining entries are deduplicated by exact identity and returned in
deterministic exact-entry order, which is `(logical_key, row_id)` order.

### 7.3 Visibility Authority

The index layer and its overlay state only select candidate row ids and prevent
blind fallback to stale tree state. Final visibility still comes from the heap
and cold-delete path:

- hot rows: undo chain
- hot unique-key ownership transfer: runtime unique-key links when needed
- cold rows: deletion buffer plus persisted delete bitmap

For unique indexes, a runtime unique-key link is a visibility bridge. Its
target may be another hot row/version or an older cold owner/version; in both
cases the link remains runtime-only and does not change the persisted
`DiskTree` shape.

Delete-shadows and delete-marked exact entries do not by themselves prove that
the row is globally invisible. They only control how `MemIndex` and `DiskTree`
candidates are merged before MVCC visibility is resolved.

After MVCC visibility is resolved, the visible row version must still be
rechecked against the logical lookup key or scan predicate before it can be
returned. This is already the rule for the current hot-row runtime and is also
the intended uniform contract for future cold `DiskTree` lookup.

This keeps the secondary-index design compatible with the existing MVCC model.

### 7.4 Runtime Unique-Key Links

Runtime unique-key links are a separate MVCC mechanism from `DiskTree`:

- create:
  - when a new latest owner of a unique logical key may hide an older visible
    owner that is not reachable by ordinary undo traversal on the latest row
    chain
- follow:
  - during unique lookup, after candidate routing into the normal row/deletion
    MVCC path and before the final key recheck, when the visible older owner of
    the same logical key must still be consulted
- discard:
  - governed by the same `Global_Min_STS` / oldest-active-snapshot horizon used
    for undo visibility
  - a link remains required until rollback/index-undo obligations are gone and
    the older owner/version's visibility can no longer be needed by any active
    snapshot
  - a link is not collectible merely because the source or target row became
    cold, crossed `pivot_row_id`, or no longer appears in the deletion buffer
- persistence:
  - runtime-only, not stored in `DiskTree`, not checkpointed, and not restored
    as historical MVCC state during recovery

The target of a runtime unique-key link may be another hot row/version or an
older cold owner/version. This link lifecycle is what makes the unique
latest-mapping `DiskTree` model compatible with snapshot visibility across
ownership transfer.

### 7.5 Composite Trait Method Parity

The composite user-table implementation keeps method parity with the current
runtime traits, but routes each method through `MemIndex` and `DiskTree` under
the contracts above. Foreground and rollback methods mutate only `MemIndex`;
`DiskTree` changes are checkpoint companion work.

Unique index method parity:

| Current method | Composite semantics | Rollback expectation |
| --- | --- | --- |
| `lookup` | Probe `MemIndex` first. A live hit or delete-shadow hit is terminal for tree selection and returns the retained row id plus delete flag to the normal MVCC/key-recheck path. On `MemIndex` miss, probe `DiskTree` and return the cold owner as a live candidate. | Read-only. Rollback state is visible only through `MemIndex` overlays and runtime unique-key links. |
| `insert_if_not_exists` | Insert into `MemIndex` only after duplicate checks. A `MemIndex` duplicate is terminal; `merge_if_match_deleted` may unmask a matching `MemIndex` delete-shadow. On `MemIndex` miss, a `DiskTree` owner is reported as a duplicate candidate but is not modified. | Insert rollback removes the new `MemIndex` claim, or remasks a delete-shadow that was merged by `merge_if_match_deleted`. |
| `compare_exchange` | Update a matching `MemIndex` entry when present. If `MemIndex` misses and `DiskTree` still maps the key to `old_row_id`, claim the cold owner by installing the `new_row_id` mapping in `MemIndex`; `DiskTree` is not updated. | Update rollback uses the same operation to restore the old owner as a `MemIndex` overlay when the durable cold owner cannot be changed directly. |
| `mask_as_deleted` | Mark a matching `MemIndex` live entry as a delete-shadow, or install a row-id-carrying `MemIndex` delete-shadow when the matching owner exists only in `DiskTree`. | Delete rollback unmasks or removes the `MemIndex` delete-shadow; no rollback path writes `DiskTree`. |
| `compare_delete` | Remove or adjust matching `MemIndex` overlay state according to `ignore_del_mask`. If only `DiskTree` has the matching owner, the operation is an idempotent runtime no-op; durable removal is deletion-checkpoint work. | Purge/recovery rollback cleanup removes only `MemIndex` state. Durable cold cleanup waits for companion checkpoint publication. |
| `scan_values` | Scan both layers, with `MemIndex` state terminal per logical key. `MemIndex` overlays suppress same-key `DiskTree` candidates before MVCC/key recheck. | Read-only. Rollback-visible candidates come from `MemIndex` overlays and runtime unique-key links. |

Non-unique index method parity:

| Current method | Composite semantics | Rollback expectation |
| --- | --- | --- |
| `lookup` | Prefix-scan exact entries from both layers. Active `MemIndex` exact entries are returned; delete-marked `MemIndex` exact entries suppress only matching `DiskTree` exact entries. Remaining candidates are deduplicated and ordered by `(logical_key, row_id)`. | Read-only. Rollback state is represented by exact `MemIndex` overlays. |
| `lookup_unique` | Check the exact `(logical_key, row_id)` pair in `MemIndex` first. A live or delete-marked exact hit is terminal and returns that state. On `MemIndex` miss, `DiskTree` exact-key presence returns active. | Read-only. Rollback observes only `MemIndex` exact overlay state. |
| `insert_if_not_exists` | Insert the exact entry into `MemIndex` only. An active `MemIndex` exact duplicate is terminal; `merge_if_match_deleted` may unmask a matching delete-marked `MemIndex` exact entry. On `MemIndex` miss, matching `DiskTree` key presence is a duplicate candidate but is not modified. | Insert rollback removes the new `MemIndex` exact entry, or remasks the exact entry if it was merged from delete-marked state. |
| `mask_as_deleted` | Mark a matching `MemIndex` exact entry deleted, or install a delete-marked `MemIndex` exact entry when the exact key exists only in `DiskTree`. `DiskTree` has no delete-mask API. | Delete rollback must be able to unmask this `MemIndex` exact entry until rollback/index-undo obligations are gone. |
| `mask_as_active` | Rollback-only unmask of a delete-marked `MemIndex` exact entry. It never updates `DiskTree`, because `DiskTree` exact-key presence has no delete flag. | Required for delete/update rollback and must not fail because cleanup removed a hot-origin overlay too early. |
| `compare_delete` | Remove the matching `MemIndex` exact overlay according to `ignore_del_mask`. If only `DiskTree` has the exact key, the operation is an idempotent runtime no-op; durable exact removal is deletion-checkpoint work. | Purge/recovery rollback cleanup removes only `MemIndex` overlays. Durable cold cleanup waits for companion checkpoint publication. |
| `scan_values` | Scan both exact-entry sets and merge deterministically by `(logical_key, row_id)`. `MemIndex` exact entries deduplicate or suppress matching `DiskTree` exact keys. | Read-only. Rollback-visible state comes from exact `MemIndex` overlays. |

## 8. Write Path

Foreground writes update `MemIndex` under the current runtime transaction
protocol. `MemIndex` can therefore reflect in-flight transactional state on the
foreground path, while `DiskTree` is still updated only by committed checkpoint
companion work.

### 8.1 Unique-Key Enforcement

For unique indexes, insert and update must perform one logical visible-owner
check before claiming a key:

1. Probe the current latest mapping for the logical key:
   - use `MemIndex` first
   - consult `DiskTree` only when hot state does not already shadow the key
2. Resolve the candidate owner through the normal row/deletion MVCC path.
3. Recheck that the visible owner version still matches the logical key.
4. If a different visible owner exists, reject the operation as a duplicate-key
   conflict.
5. Otherwise, install or update the latest logical-key mapping.
6. If an older visible owner must remain reachable for live snapshots, also
   install the required runtime unique-key link.

Delete-shadows and stale cold `DiskTree` mappings do not by themselves
establish a uniqueness conflict. Only a different visible owner that still
matches the key can block the new claim.

### 8.2 Insert

1. Insert the row into RowStore and obtain a new row id.
2. Update `MemIndex`.
   - unique: first apply the unique-key enforcement rule, then map the logical
     key to the new row id
   - non-unique: insert exact `(logical_key, row_id)` entry
3. For a unique index, if the inserted row takes ownership of a logical key
   whose older visible owner may still be needed by a live snapshot, install a
   runtime unique-key link from the new row version to that older owner/version.
4. Do not modify `DiskTree` on the foreground path.

### 8.3 Update of a Hot Row

If the target row remains in RowStore:

- every write first owns the row undo head with a `Lock` entry
- in-place update reuses the RowID and records before-image columns in the hot
  undo chain
- move update marks the old hot RowID deleted, inserts the replacement as a new
  hot RowID, and links unique-key owners when older snapshots may need the old
  owner
- secondary index updates only touch `MemIndex`; `DiskTree` is updated later by
  table checkpoint companion work

Cases:

- unique, key unchanged:
  - no logical-key remap is needed when RowID is unchanged
  - if RowID changes due to move update, atomically replace the latest mapping
    from old RowID to new RowID and record index undo for rollback
- unique, key changed:
  - first apply the unique-key enforcement rule for the new key
  - install a row-id-carrying delete-shadow for the old key in `MemIndex`
  - insert new key -> row id in `MemIndex`
  - same-RowID key changes may merge with this transaction's own delete-shadow
    by flipping the delete flag back to active
  - if the conflicting owner is delete-masked or otherwise stale, revalidate the
    old owner through row undo before claiming the latest mapping
  - if an older snapshot may still need the previous owner/version and that
    owner is not reachable through ordinary undo from the latest row id, install
    the required runtime unique-key link to the older hot owner
- non-unique, key unchanged:
  - no index change if RowID stays the same
  - if RowID changes due to move update, insert the new exact `(key, row_id)`
    entry and mask the old exact entry
- non-unique, key changed:
  - mask exact old `(key, row_id)` entry in `MemIndex`
  - insert exact new `(key, row_id)` entry in `MemIndex`
  - rollback unmasks the old exact entry and removes or remasks the new exact
    entry according to its index undo kind

Delete-shadows and delete-marked exact entries created by hot-row update are
hot-origin overlays. Their cleanup follows the hot-origin rule in
[`5.5 MemIndex Cleanup`](#55-memindex-cleanup); deletion-buffer absence is not a
cleanup proof for them.

### 8.4 Update of a Cold Row

If the target row is already persistent:

1. Read the old cold row.
2. Create a cold-row deletion marker for the old row id.
3. Insert the modified row into RowStore with a new row id.
4. Update `MemIndex`.

Cases:

- unique:
  - if the logical key is unchanged, `MemIndex` maps the key to the new hot row
    id and shadows the stale `DiskTree` value
  - if an older snapshot may still need the previous cold owner/version of that
    key, runtime must install a unique-key link from the new hot row version to
    the old cold owner/version; ordinary undo from the new hot row id is not
    sufficient by itself for that case
  - if the logical key changes, `MemIndex` installs a row-id-carrying
    delete-shadow for the old key and, after applying the unique-key
    enforcement rule, maps the new key to the new hot row id
  - if an older snapshot may still need the previous owner/version of the old
    or new key and that owner is not reachable by ordinary undo from the latest
    row id, runtime must install the required unique-key link, including when
    that previous owner/version is cold
- non-unique:
  - insert the new exact `(new_key, new_row_id)` entry in `MemIndex`
  - install a delete-marked exact entry for `(old_key, old_row_id)` in
    `MemIndex` so the stale `DiskTree` entry is shadowed until deletion
    checkpoint

### 8.5 Delete

Hot-row delete:

- update heap/undo as today
- update `MemIndex` to install a row-id-carrying delete-shadow for a unique
  index or a delete-marked exact entry for a non-unique index
- these overlay entries block stale fallback but final visibility still comes
  from the normal row/deletion path
- these are hot-origin overlays and follow the hot-origin cleanup rule in
  [`5.5 MemIndex Cleanup`](#55-memindex-cleanup)

Cold-row delete:

- create a deletion-buffer entry for the old row id
- update `MemIndex` so runtime lookup does not surface the stale `DiskTree`
  mapping
- unique delete-shadows retain the old row id for read-time visibility
  resolution and later cleanup
- non-unique delete-shadows already keep the old row id as part of the exact
  entry identity

## 9. Checkpoint

The key rule is that secondary-index persistence is a companion of table
checkpoint, not an independent MemIndex flush.

The detailed table-level cutoff and replay contract is defined in
[`checkpoint-and-recovery.md`](./checkpoint-and-recovery.md). This document
only summarizes the index-relevant parts:

- data checkpoint uses the same GC-visible `cutoff_ts` as the table checkpoint
  pass after frozen-page stabilization
- companion secondary-index entries are built from the same committed-visible
  rows selected under that cutoff
- deletion checkpoint persists only committed cold-row delete markers with
  `cts < cutoff_ts`
- recovery still uses `heap_redo_start_ts` for hot heap replay and
  `deletion_cutoff_ts` for cold-row deletion replay

### 9.1 Data Checkpoint Companion Work

When frozen row pages are converted into LWC blocks under the table checkpoint
cutoff:

1. choose the committed-visible rows from those pages under the same
   `cutoff_ts` used by the data checkpoint pass
2. build the corresponding secondary-index entries from row values
3. merge those entries into each affected `DiskTree`
4. publish the new `DiskTree` roots together with:
   - the new LWC blocks
   - the updated RowID/block metadata
   - the updated table checkpoint root

This guarantees that new cold rows and their persistent secondary-index entries
become durable together.

### 9.2 Deletion Checkpoint Companion Work

When committed cold-row delete markers with `cts < cutoff_ts` are selected at
deletion-checkpoint cutoff:

1. sort and group selected delete markers by persisted LWC block
2. for each affected LWC block, load and decode the block once
3. reconstruct all affected secondary keys for that block from the persisted
   row values
4. emit per-index `DiskTree` delete/update batches from those reconstructed
   keys
5. apply the corresponding `DiskTree` delete/update according to index kind:
   - unique:
     - apply the companion delete/update only if the currently stored owner for
       that logical key still matches the deleted old row id
     - if the stored owner no longer matches, skip the delete/update for that
       key because a newer owner has already been published
   - non-unique:
     - remove the exact `(logical_key, old_row_id)` entry from `DiskTree`
6. publish those `DiskTree` changes together with the new persistent delete
   bitmap state

This guarantees that persisted cold-row deletes and persistent secondary-index
removals become durable together.

This companion delete flow relies on a table/checkpoint retention contract:
deleted cold row values remain reconstructible until persistent delete metadata
and the companion secondary-index delete have been durably published together.
Under the current storage model, persisted LWC blocks are immutable and there
is no earlier vacuum/compaction path that may reclaim those bytes before this
companion delete work is complete.

This conditional unique-key rule is required because data checkpoint for a
newer owner of key `K` may publish before deletion checkpoint later processes
an older owner of the same key. Deletion companion work for unique indexes must
therefore not behave like a blind logical-key delete.

### 9.3 No Independent Index Checkpoint

The design explicitly rejects the old independent index checkpoint model:

- no dedicated scan of dirty `MemIndex` entries
- no separate committed-entry correlation pass
- no `Index_Rec_CTS`

This avoids the correctness problem of trying to advance an index recovery
watermark from an arbitrary flushed batch maximum.

### 9.4 Checkpoint Cost

The main implementation risk is CoW rewrite cost:

- index writes generated from checkpointed rows can touch scattered `DiskTree`
  leaves
- deletion checkpoint can also update many disjoint logical keys

The baseline mitigation is to sort and batch updates by target tree order before
performing CoW merges.

## 10. Recovery

### 10.1 Startup State

On restart:

1. load the latest table checkpoint root
2. load persistent LWC data and delete-bitmap state
3. load `DiskTree` roots from the same checkpoint root

At this point, the engine already has the latest checkpointed cold secondary
index state.

### 10.2 Redo Replay

Recovery then replays redo as usual:

- hot heap redo is replayed from `heap_redo_start_ts`
- cold-row deletions with `row_id < pivot_row_id` and
  `cts >= deletion_cutoff_ts` are replayed into the in-memory deletion buffer
- row replay rebuilds hot `MemIndex` state through the normal index update logic

There is no separate index replay start timestamp.

### 10.3 Why This Is Sufficient

This works because:

- `DiskTree` already contains the checkpointed cold state
- `MemIndex` only needs to represent post-checkpoint hot changes
- runtime unique-key links are not part of durable `DiskTree` state and do not
  need to be reconstructed as historical visibility structures after restart
- no pre-crash active snapshot survives restart, so recovery does not need to
  preserve historical unique-key visibility from before the crash

## 11. Garbage Collection

### 11.1 MemIndex

`MemIndex` cleanup is driven by one captured table-file root plus
`Global_Min_STS`:

- live entries are removable only when the captured `DiskTree` already contains
  the same durable mapping
- delete overlays are removable once overlay obsolescence is globally or
  durably proven, including captured cold-row absence or captured cold-row key
  mismatch, without first proving absence from the captured `DiskTree`
- cleanup deletes by encoded key and expected delete-bit state so a stale scan
  cannot remove an entry that changed concurrently
- cleanup may retain clean entries as cache; retained entries must preserve the
  same lookup semantics as before the pass

### 11.2 DiskTree

`DiskTree` page GC follows normal CoW root replacement:

- new roots are published at table checkpoint
- unreachable old pages are reclaimed later

### 11.3 Heap MVCC Links

Old heap versions and runtime unique-key links are part of the same live MVCC
contract:

- ordinary undo links cover historical versions on the same row chain
- runtime unique-key links cover historical ownership of the same logical key
  when ownership moved across different row chains, including from a new hot row
  version to an older cold owner/version when needed for snapshot visibility
- both are runtime-only and only need to satisfy live snapshots
- runtime unique-key links are governed by the same `Global_Min_STS` /
  oldest-active-snapshot horizon as undo GC
- runtime unique-key links may be discarded only after rollback/index-undo
  obligations are gone and no active snapshot can still require that older
  owner/version
- runtime unique-key links are not collectible merely because a source or
  target row became cold, crossed `pivot_row_id`, or disappeared from the
  deletion buffer
- neither needs to survive restart because restart only restores the latest
  committed mapping

## 12. Open Questions And Difficult Parts

The following areas need careful implementation detail even under the companion
checkpoint model:

1. Block-grouped deletion-checkpoint cost:
   - the baseline algorithm groups selected deletes by persisted LWC block and
     decodes each affected block once, but very large delete batches can still
     create memory pressure and many per-index `DiskTree` updates
   - future work may parallelize block groups after the grouped algorithm is in
     place
2. Checkpoint write amplification:
   - scattered CoW updates across many logical keys can enlarge checkpoint cost
3. MemIndex cleanup rules:
   - cleanup should be derived from published checkpoint metadata and
     deletion-buffer state without requiring a post-publish rewrite of all
     touched `MemIndex` entries
4. Cold-owner runtime link refinement:
   - runtime unique-key links can now target old cold owners during cold-row
     update of unique keys
   - future work should focus on cost refinements under the fixed
     `Global_Min_STS` lifecycle, not on changing the unique latest-mapping
     physical model

## 13. Summary

User-table secondary indexes use a two-layer design:

- `MemIndex` serves hot mutable state
- `DiskTree` stores checkpointed cold state

Catalog-table secondary indexes stay on the existing in-memory single-tree
runtime path.

The persistent index is maintained as a companion of data and deletion
checkpoint, not as an independently flushed committed overlay. This keeps the
design consistent with Doradb's current heap-based MVCC model and restart
contract:

- runtime old-version visibility stays in heap/undo
- restart only restores the latest committed state
- no separate index recovery watermark is required
