# Secondary Index Design

## 1. Overview

This document is the authoritative design for Doradb secondary indexes.

It replaces the old secondary-index sections in
[`index-design.md`](./index-design.md) and defines:

- the overall secondary-index architecture
- the in-memory `MemTree`
- the persistent `DiskTree`
- read and write behavior
- checkpoint behavior
- recovery behavior

This document only covers secondary indexes. The RowID-to-location block index
is documented in [`block-index.md`](./block-index.md). The higher-level split
between block index and secondary index is summarized in
[`index-design.md`](./index-design.md).

## 2. Design Goals

The secondary index must satisfy the following constraints:

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

Each secondary index is split into two layers:

1. `MemTree`
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

- there is no separate index checkpoint thread that scans committed `MemTree`
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
- delete-marked exact entries are runtime overlay state in `MemTree`, not a
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
  - live exact entries keyed by `(logical_key, row_id)`
- no durable secondary-index tombstones are stored in `DiskTree`

Delete representation is split across runtime overlay state and cold delete
state:

- `MemTree` carries delete-shadows or delete-marked exact entries so hot lookup
  does not fall through to stale cold `DiskTree` state
- the deletion buffer and published delete bitmap remain the cold-row delete
  authority
- deletion-checkpoint companion work then removes or conditionally updates the
  corresponding durable `DiskTree` entry

This keeps the persistent secondary-index shape simple: `DiskTree` holds the
latest checkpointed cold index state, while delete overlay semantics remain a
runtime concern until companion delete work is published.

## 5. MemTree

`MemTree` is the hot mutable layer.

### 5.1 Role

- absorb foreground inserts, updates, and deletes
- shadow stale `DiskTree` entries until the next table checkpoint
- serve recent hot lookups without touching persistent index pages

### 5.2 Transaction Semantics

`MemTree` follows the current runtime transaction model:

- foreground writes update `MemTree` before commit
- `MemTree` may therefore contain in-flight transactional state, not only
  committed state
- rollback and write-conflict handling remain part of the normal runtime
  protocol
- checkpointed `DiskTree` state still contains committed cold state only

As a result, the standalone secondary-index design does not need a per-entry
`sts` field to make `MemTree` transactional. The design also does not use an
eagerly maintained per-entry `dirty` bit. Visibility, rollback, and conflict
handling remain responsibilities of the wider runtime MVCC/transaction
protocol, while cleanup is derived later from published checkpoint metadata and
deletion-overlay state.

### 5.3 Unique MemTree Entries

For a unique index, `MemTree` stores the latest logical-key mapping.

Interpretation:

- a live entry maps the logical key to the current row id
- a delete-shadow entry is an overlay entry for the latest hot logical-key
  state; it retains the referenced row id and prevents blind fallback to an
  older cold `DiskTree` mapping
- a unique delete-shadow does not by itself prove that the key is invisible to
  every snapshot; readers still route the retained row id through the normal
  row/deletion visibility path

### 5.4 Non-Unique MemTree Entries

For a non-unique index, `MemTree` stores exact entries keyed by
`(logical_key, row_id)`.

Interpretation:

- each hot duplicate occupies its own entry
- delete-marked exact entries are overlay state that can shadow stale older
  tree results for the same exact entry
- `MemTree` and `DiskTree` results must be merged for scans

### 5.5 MemTree Cleanup

`MemTree` cleanup should be derived from published table metadata and deletion
overlay state, not from an eagerly rewritten per-entry `dirty` bit.

Common cases:

- live row mapping:
  - once the referenced row id is below the published `pivot_row_id`, the same
    row is already represented by published cold state
  - the `MemTree` entry can then be evicted or retained only as a cache
- cold delete-shadow:
  - if the deletion buffer no longer contains a marker for the shadow's
    referenced row id, the shadow can be removed
  - a deletion-buffer miss is safe here because deletion-buffer GC is allowed
    only after the delete is durably published and no active snapshot still
    needs the pre-delete view

There is no separate MemTree-to-DiskTree flush pass and no need for a
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
- it may be stale relative to hot `MemTree`, and runtime lookup rules must
  account for that
- cold-row deletes are represented by deletion overlay state until companion
  checkpoint work removes or conditionally updates the durable entry

## 7. Read Path

### 7.1 Unique Point Lookup

Runtime lookup for a unique secondary index proceeds as follows:

1. Probe `MemTree` by logical key.
2. If `MemTree` hits:
   - if the entry is live, use its row id as the candidate entry point for the
     normal row/deletion visibility path
   - if the entry is a delete-shadow, do not fall through to a stale
     `DiskTree` value; instead use the retained row id as the candidate entry
     point for the normal row/deletion visibility path
3. If `MemTree` misses, probe `DiskTree`.
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

For unique lookup, a `MemTree` hit is terminal only for tree selection: once
hot logical-key state is present, lookup must not fall through to older
`DiskTree` state for the same logical key, but final visibility still comes
from the normal row/deletion MVCC path plus the final key recheck.

### 7.2 Non-Unique Lookup and Scan

For non-unique indexes, results may exist in both trees at the same logical
key:

1. Probe or range-scan `MemTree`.
2. Probe or range-scan `DiskTree`.
3. Merge exact entries from both sources.
4. Use delete-marked exact entries only to suppress stale older tree state for
   the same exact `(logical_key, row_id)` entry.
5. Route the remaining candidate row ids through the normal row/deletion
   visibility check.
6. After visibility resolution, recheck that each visible row version still
   satisfies the lookup key or scan predicate. Return only rows that still
   satisfy that predicate.

For non-unique indexes, a `MemTree` hit is not terminal because the cold tree
may still contain additional duplicates for the same logical key.

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
the row is globally invisible. They only control how `MemTree` and `DiskTree`
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
  - once no active snapshot can still require that older owner/version
- persistence:
  - runtime-only, not stored in `DiskTree`, not checkpointed, and not restored
    as historical MVCC state during recovery

The target of a runtime unique-key link may be another hot row/version or an
older cold owner/version. This link lifecycle is what makes the unique
latest-mapping `DiskTree` model compatible with snapshot visibility across
ownership transfer.

## 8. Write Path

Foreground writes update `MemTree` under the current runtime transaction
protocol. `MemTree` can therefore reflect in-flight transactional state on the
foreground path, while `DiskTree` is still updated only by committed checkpoint
companion work.

### 8.1 Unique-Key Enforcement

For unique indexes, insert and update must perform one logical visible-owner
check before claiming a key:

1. Probe the current latest mapping for the logical key:
   - use `MemTree` first
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
2. Update `MemTree`.
   - unique: first apply the unique-key enforcement rule, then map the logical
     key to the new row id
   - non-unique: insert exact `(logical_key, row_id)` entry
3. For a unique index, if the inserted row takes ownership of a logical key
   whose older visible owner may still be needed by a live snapshot, install a
   runtime unique-key link from the new row version to that older owner/version.
4. Do not modify `DiskTree` on the foreground path.

### 8.3 Update of a Hot Row

If the target row remains in RowStore:

- heap data and undo chain are updated as today
- secondary index updates only touch `MemTree`

Cases:

- unique, key unchanged:
  - no logical-key remap is needed if row id is unchanged
- unique, key changed:
  - first apply the unique-key enforcement rule for the new key
  - install a row-id-carrying delete-shadow for the old key in `MemTree`
  - insert new key -> row id in `MemTree`
  - if an older snapshot may still need the previous owner/version of either
    key and that owner is not reachable through ordinary undo from the latest
    row id, install the required runtime unique-key link
- non-unique, key unchanged:
  - no index change if row id stays the same
- non-unique, key changed:
  - delete exact old `(key, row_id)` entry in `MemTree`
  - insert exact new `(key, row_id)` entry in `MemTree`

### 8.4 Update of a Cold Row

If the target row is already persistent:

1. Read the old cold row.
2. Create a cold-row deletion marker for the old row id.
3. Insert the modified row into RowStore with a new row id.
4. Update `MemTree`.

Cases:

- unique:
  - if the logical key is unchanged, `MemTree` maps the key to the new hot row
    id and shadows the stale `DiskTree` value
  - if an older snapshot may still need the previous cold owner/version of that
    key, runtime must install a unique-key link from the new hot row version to
    the old cold owner/version; ordinary undo from the new hot row id is not
    sufficient by itself for that case
  - if the logical key changes, `MemTree` installs a row-id-carrying
    delete-shadow for the old key and, after applying the unique-key
    enforcement rule, maps the new key to the new hot row id
  - if an older snapshot may still need the previous owner/version of the old
    or new key and that owner is not reachable by ordinary undo from the latest
    row id, runtime must install the required unique-key link, including when
    that previous owner/version is cold
- non-unique:
  - insert the new exact `(new_key, new_row_id)` entry in `MemTree`
  - install a delete-marked exact entry for `(old_key, old_row_id)` in
    `MemTree` so the stale `DiskTree` entry is shadowed until deletion
    checkpoint

### 8.5 Delete

Hot-row delete:

- update heap/undo as today
- update `MemTree` to install a row-id-carrying delete-shadow for a unique
  index or a delete-marked exact entry for a non-unique index
- these overlay entries block stale fallback but final visibility still comes
  from the normal row/deletion path

Cold-row delete:

- create a deletion-buffer entry for the old row id
- update `MemTree` so runtime lookup does not surface the stale `DiskTree`
  mapping
- unique delete-shadows retain the old row id for read-time visibility
  resolution and later cleanup
- non-unique delete-shadows already keep the old row id as part of the exact
  entry identity

## 9. Checkpoint

The key rule is that secondary-index persistence is a companion of table
checkpoint, not an independent MemTree flush.

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
  `Deletion_Rec_CTS` for cold-row deletion replay

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

1. locate the persisted cold row by row id
2. decode the old index key from persisted row values
3. apply the corresponding `DiskTree` delete/update according to index kind:
   - unique:
     - apply the companion delete/update only if the currently stored owner for
       that logical key still matches the deleted old row id
     - if the stored owner no longer matches, skip the delete/update for that
       key because a newer owner has already been published
   - non-unique:
     - remove the exact `(logical_key, old_row_id)` entry from `DiskTree`
4. publish those `DiskTree` changes together with the new persistent delete
   bitmap state

This guarantees that persisted cold-row deletes and persistent secondary-index
removals become durable together.

This companion delete flow relies on one retention rule: deleted cold row
values remain reconstructible until the companion secondary-index delete has
been durably published. Under the current storage model, persisted LWC blocks
are immutable and there is no earlier vacuum/compaction path that may reclaim
those bytes before this companion delete work is complete.

This conditional unique-key rule is required because data checkpoint for a
newer owner of key `K` may publish before deletion checkpoint later processes
an older owner of the same key. Deletion companion work for unique indexes must
therefore not behave like a blind logical-key delete.

### 9.3 No Independent Index Checkpoint

The design explicitly rejects the old independent index checkpoint model:

- no dedicated scan of dirty `MemTree` entries
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
- cold-row deletions newer than the last persisted deletion checkpoint are
  replayed into the in-memory deletion buffer
- row replay rebuilds hot `MemTree` state through the normal index update logic

There is no separate index replay start timestamp.

### 10.3 Why This Is Sufficient

This works because:

- `DiskTree` already contains the checkpointed cold state
- `MemTree` only needs to represent post-checkpoint hot changes
- runtime unique-key links are not part of durable `DiskTree` state and do not
  need to be reconstructed as historical visibility structures after restart
- no pre-crash active snapshot survives restart, so recovery does not need to
  preserve historical unique-key visibility from before the crash

## 11. Garbage Collection

### 11.1 MemTree

`MemTree` cleanup is driven by published checkpoint metadata and delete-overlay
state:

- live entries for rows below the published `pivot_row_id` are no longer needed
  for correctness
- delete-shadow entries for old cold rows can be removed once the deletion
  buffer no longer contains the corresponding row id
- clean entries may still be kept as cache, but they do not need to remain for
  correctness

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
- runtime unique-key links may be discarded once no active snapshot can still
  require that older owner/version
- neither needs to survive restart because restart only restores the latest
  committed mapping

## 12. Open Questions And Difficult Parts

The following areas need careful implementation detail even under the companion
checkpoint model:

1. Unique-key shadow semantics:
   - a unique delete-shadow in `MemTree` must reliably block blind fallback to
     a stale cold mapping in `DiskTree` while still deferring final visibility
     to the row/deletion MVCC path
2. Non-unique merge behavior:
   - range scans need deterministic duplicate suppression and ordering when the
     same logical key exists in both trees
3. Deletion-checkpoint key reconstruction:
   - locating deleted cold rows and decoding old keys can become expensive if it
     causes excessive random block reads
   - correctness depends on the stated retention rule that deleted cold row
     values remain reconstructible until companion secondary-index delete
     publication
4. Checkpoint write amplification:
   - scattered CoW updates across many logical keys can enlarge checkpoint cost
5. MemTree cleanup rules:
   - cleanup should be derived from published checkpoint metadata and
     deletion-buffer state without requiring a post-publish rewrite of all
     touched `MemTree` entries
6. Cold-owner runtime link implementation:
   - the design now requires runtime unique-key links to be able to target old
     cold owners during cold-row update of unique keys
   - the remaining work is implementation of that cold-owner branch, not
     reconsideration of the unique latest-mapping physical model

## 13. Summary

Doradb secondary indexes use a two-layer design:

- `MemTree` serves hot mutable state
- `DiskTree` stores checkpointed cold state

The persistent index is maintained as a companion of data and deletion
checkpoint, not as an independently flushed committed overlay. This keeps the
design consistent with Doradb's current heap-based MVCC model and restart
contract:

- runtime old-version visibility stays in heap/undo
- restart only restores the latest committed state
- no separate index recovery watermark is required
