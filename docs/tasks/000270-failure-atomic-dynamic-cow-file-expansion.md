---
id: 000270
title: Add Failure-Atomic Dynamic CoW File Expansion
status: proposal
created: 2026-08-15
github_issue: 978
---

# Task: Add Failure-Atomic Dynamic CoW File Expansion

## Summary

Allow durable user table files and `catalog.mtb` to grow beyond their current
16 MiB allocation-map capacity. A mutable CoW root doubles its page capacity
on allocation exhaustion, up to a shared configured per-file ceiling, and
extends the backing file with Linux sparse-file `ftruncate` semantics before
exposing any new block id. The expanded allocation map becomes durable through
the existing CoW meta/super publication and `fsync` sequence, so the active
root remains authoritative until publication succeeds.

On startup, reconcile each durable CoW sparse file's logical length with the
allocation-map capacity in its newest valid published root. Reject a file
shorter than its published capacity as corruption. Treat a longer file as an
abandoned unpublished expansion, truncate it to the published capacity,
durably sync the repair, and only then install the loaded root.

## Context

New user table files are created at `TABLE_FILE_INITIAL_SIZE` (16 MiB), and
`catalog.mtb` uses the same value through `MULTI_TABLE_FILE_INITIAL_SIZE`. With
64-KiB CoW pages, both initial active roots create an `AllocMap` with 256
entries. That immutable `len` currently remains unchanged when roots are
cloned or rebuilt, so `MutableTableFile` and `MutableMultiTableFile` report
`ResourceError::StorageFileCapacityExceeded` once all entries are occupied,
even though `SparseFile` already has an unused sparse `extend_to` primitive.

The persisted user-table and multi-table meta blocks both serialize the
allocation-map length, allocated count, free-word cursor, and bitmap. Different
map lengths are therefore representable without changing either meta version.
The shared CoW publisher writes the new meta block, writes the inactive
super-block slot, submits one `fsync`, and swaps the in-memory active root only
after that sync completes. Expansion must preserve that publication boundary
and must not add a sync to ordinary allocation or to each file-length increase.

All five logical catalog tables share the single `catalog.mtb` allocation map.
Catalog checkpoint rewrites changed catalog-table LWC and column-index blocks
before rebuilding allocation state from the new root graph, so its transient
old-plus-new footprint can exhaust the fixed map even when the eventual compact
root would fit. Leaving `catalog.mtb` fixed would therefore preserve a global
DDL/catalog scale limit after user table files become expandable.

Task 000269 reduced the checked-in checkpoint benchmark from one million rows
and a 500,000-row frozen prefix because that workload exhausted the fixed map.
This task restores that large scenario as executable acceptance coverage after
the storage limit is removed.

Source Backlogs:

- `docs/backlogs/000184-dynamic-table-file-expansion.md`

Related Tasks:

- `docs/tasks/000269-single-table-checkpoint-benchmark.md`

Issue Labels:

- type:task
- priority:medium
- codex

## Goals

- Grow user `.tbl` and `catalog.mtb` capacity online when a mutable CoW root
  exhausts its allocation map.
- Double page capacity geometrically, clamped to a configurable maximum applied
  independently to each physical durable CoW file, with checked arithmetic and
  typed capacity failures.
- Use sparse logical extension so unwritten capacity does not allocate data
  blocks on disk.
- Preserve active-root and any retained-root correctness when growth, later
  CoW writes, publication, or `fsync` fails.
- Reconcile stale physical expansion at startup against the selected published
  allocation map before a user-table runtime or catalog storage is installed.
- Keep the successful allocation path free of file-size syscalls and make
  bitmap copying amortized across geometric growth events.
- Cover user-table checkpoint, secondary-index, catalog checkpoint,
  reclamation, restart, and alternate-I/O backend behavior across the former
  256-page boundary.

## Non-Goals

- Expanding either evictable buffer pool or either `.swp` file.
- Replacing the inline bitmap with paged, extent-based, or out-of-line space
  metadata.
- Shrinking a live CoW file, compacting allocated pages, or returning sparse
  tail space during normal runtime.
- Changing table page size, block-id meaning, root-retention rules, or
  checkpoint transaction semantics.
- Recovering unpublished blocks from a stale tail; only a durably published
  root defines live capacity and content.
- Adding a user-table or multi-table meta/super format version or migrating
  existing files.

## Plan

### Capacity model and invariants

Use the allocation-map length as the sole logical capacity carried by a root:

```text
published_capacity = active_root.alloc_map.len() * COW_FILE_PAGE_SIZE
mutable_capacity   = mutable_root.alloc_map.len() * COW_FILE_PAGE_SIZE
physical_length    = SparseFile logical length from fstat/ftruncate
growth_limit       = configured cow_file_max_size
```

Maintain these invariants:

1. Every allocated or returned CoW `BlockID` is below the allocation-map length
   of the root that owns it and below the sparse file's logical length.
2. A mutable root may have more capacity than the active root, but an active
   root is not replaced until its meta block, inactive super slot, and file
   extent have passed the existing publication `fsync`.
3. Allocation-map length only grows. Reachability rebuilds and retained roots
   preserve their own map lengths; no runtime path shrinks the physical file.
4. Immediately after successful startup reconciliation,
   `physical_length == published_capacity`.
5. `physical_length < published_capacity` is an invalid durable root and is
   never repaired by extending the file. `physical_length >
   published_capacity` is an unreferenced stale tail and is truncated only at
   startup.

An older retained user-table root or catalog root guard can have a shorter map
than the current active root. Its allocated block ids remain valid because
expansion never relocates pages and the physical extent never shrinks while
runtime readers or root guards can exist.

### Configuration and format ceiling

Add `FileSystemConfig::cow_file_max_size: usize`, a matching builder, and
validated plumbing through `ValidatedFileSystemConfig`, `FileSystem`, and
`build_file_system`. Add `DEFAULT_COW_FILE_MAX_SIZE` with a 16-GiB default. The
value is one shared policy applied independently to each physical user `.tbl`
file and to the physical catalog `.mtb` file. It is not an eager allocation
size, a combined quota, or a startup truncation target.

Configuration validation requires the value to be at least both
`TABLE_FILE_INITIAL_SIZE` and `MULTI_TABLE_FILE_INITIAL_SIZE`, exactly divisible
by `COW_FILE_PAGE_SIZE`, and convertible to a checked nonzero page count.
Introduce a fieldless `ConfigError::InvalidCowFileSize` and attach the
configured bytes, both initial byte values, and page size to failures.

Propagate the option through `doradb-bench`'s strict `FileSystemConfigOverlay`
as a byte-valued `cow_file_max_size`, its merged storage builder, and
`ResolvedFileSystemConfig.cow_file_max_size_bytes`, so benchmark artifacts
record the effective ceiling.

Opening an existing file whose published map is already larger than a newly
lowered configured maximum remains valid. Existing free pages can still be
allocated, but no further growth is allowed until the configuration is raised.
Startup reconciliation always uses published map capacity, never the
configured maximum.

No persisted field or version is added. `AllocMap` already serializes `len`,
and a 16-GiB CoW file needs 262,144 allocation bits, or 32 KiB plus the existing
map header. The fixed multi-table payload plus that map fits in the existing
64-KiB checksummed catalog meta block; the current inline catalog format can
represent approximately 31.9 GiB of 64-KiB pages, so the 16-GiB default remains
below its format ceiling. Before each growth, the concrete user-table or
multi-table metadata and candidate map must still fit in one inline meta block.
A candidate that exceeds its concrete payload receives a typed capacity error
even if the configured byte ceiling is higher.

### Allocation-map and sparse-file primitives

Add an `AllocMap` growth operation that builds a new map with a strictly larger
length while preserving every existing bit, `allocated`, and the earliest
free-word cursor. New bitmap bits are clear. The source map remains unchanged,
including at non-64-bit-aligned lengths, so a failed growth attempt cannot
partially alter the mutable root and a published active map remains immutable.

Make `SparseFile` expose its tracked logical length. Repair `extend_to` so a
successful `ftruncate` publishes the new `max_len` atomic while holding
`size_lock`; repeated or smaller growth requests remain no-ops. Add a
startup-only shrink primitive that uses the same lock, calls `ftruncate`, and
updates `max_len` only on success. Its contract requires an opened durable CoW
file with no installed runtime/catalog storage or concurrent allocator, and it
is not used by evictable pool files. Both directions retain `IoError` plus
operation and size attachments.

Linux `ftruncate` changes logical length without writing the intervening
range. Tests must verify that extension changes logical bytes while unwritten
tail capacity does not consume proportional `st_blocks` disk allocation.

### Shared growth-aware CoW allocation

Keep `MutableCowRoot::try_allocate_block` as the fast path. Store the validated
maximum page count on `CowFile` when either concrete file is created or opened.
Add a codec callback that calculates the concrete meta payload length for a
candidate `AllocMap` without changing the mutable root. Both
`MutableTableFile` and `MutableMultiTableFile` call one `CowFile` allocation
helper; their existing exclusive `MutableWriterClaim` means one mutable writer
per physical file performs the following slow path:

1. If `try_allocate_block` succeeds, return immediately with no capacity
   calculation, bitmap copy, or syscall.
2. On exhaustion, read `current_pages` and the configured maximum from the
   owning `CowFile`.
3. Compute `target_pages = min(current_pages * 2, maximum_pages)` with checked
   arithmetic. If the result is not greater than `current_pages`, return
   `ResourceError::StorageFileCapacityExceeded` with current, requested, and
   maximum sizes.
4. Build the expanded `AllocMap` candidate without modifying the mutable root.
5. Use the concrete codec callback to calculate user-table or multi-table meta
   serialization length with that candidate. Reject it with
   `StorageFileCapacityExceeded` if it cannot fit in one checksummed CoW page.
6. Sparse-extend the file to `target_pages * COW_FILE_PAGE_SIZE`.
7. Only after successful `ftruncate`, install the candidate map in the mutable
   root and retry allocation. The retry must succeed by construction.
8. Emit one structured information event containing file kind, file id, old
   and new pages/bytes, and configured maximum. Do not log normal allocations.

If candidate construction or meta-size validation fails, the file and root are
unchanged. If `ftruncate` fails, the root is unchanged and the I/O error is
returned. If the process stops after `ftruncate` but before publication, only
an unreferenced sparse tail remains and startup reconciliation removes it.

Change `MutableCowFile::allocate_block` to return `RuntimeResult<BlockID>` so
CoW allocation can preserve either `IoError` or `ResourceError` beneath
`RuntimeError::FileRootAccess`. Update DiskTree, column-index, deletion-blob,
table-checkpoint, and catalog-checkpoint call sites to retain their existing
higher-level context. Remove direct root-only allocation call sites that would
bypass the physical-file growth helper, including user LWC allocation.

The final meta block is an allocation too. Move generic meta reservation onto
the physical-file-aware `CowFile` helper. `CowFile::publish_root` calls it for
ordinary publication, while `MutableMultiTableFile` calls it before
`publish_prepared_root` for catalog's explicit early-reservation paths. This
prevents roots whose data blocks exactly fill the old map from failing only at
final publication. Preserve catalog's existing
reserve-before-reachability-rebuild ordering and metadata-only
reserve-then-reclaim ordering.

`MutableCowRoot::rebuild_alloc_map_from_reachable` continues to create the
replacement map with `self.root.alloc_map.len()`. It must never fall back to
either initial-size constant, so user and catalog reclamation retain all
previously published capacity while clearing unreachable allocation bits.

### Publication and crash behavior

Do not add a sync at growth time. The ordered publication for either concrete
CoW file remains:

```text
sparse-extend extent
  -> install expanded mutable map
  -> write unpublished CoW blocks
  -> write meta block containing expanded map
  -> write inactive super slot
  -> fsync file
  -> swap in-memory active root
```

The existing publication `fsync` makes the new extent, data/meta writes, and
root anchor durable together. Until it completes, the old in-memory root stays
active. Recovery behavior is determined only by the newest valid super/meta
pair found by the existing slot-selection rules:

| Last durable/observable stage | Selected root | Startup action |
| --- | --- | --- |
| Before sparse extension | Old map | No reconciliation work |
| Extension or CoW writes without a durable new root | Old map | Truncate stale tail and sync |
| Valid expanded meta but old super selected | Old map | Truncate stale tail and sync |
| New super/meta selected after publication sync | Expanded map | Require the full expanded extent |

An `fsync` error does not swap the runtime active root. On restart, normal
super-slot validation decides which root actually became durable; capacity
reconciliation then applies to that selected root.

### Startup validation and stale-tail reconciliation

Integrate one reconciliation helper into both `FileSystem::open_table_file`
and the opened-file branch of `FileSystem::open_or_create_multi_table_file`,
after `CowFile::load_active_root_from_pool` selects and parses the newest valid
root but before either concrete `install_loaded_root` call. Newly created files
already have matching initial physical and map lengths and do not need a repair
pass before their first publication.

Run all concrete root validation before comparing lengths or truncating a stale
tail. Extend user-table root validation to require:

- the reserved super block and selected meta block are allocated and in range;
- `column_block_index_root` is the sentinel or an allocated in-range block;
- every live `secondary_index_roots` entry is allocated and in range, while
  inactive entries remain the sentinel; and
- allocation-map length times page size is representable without overflow.

Extend multi-table root validation to require:

- the reserved super block and selected catalog meta block are allocated and in
  range;
- every descriptor remains in its fixed catalog-table slot and preserves the
  existing empty-root/pivot invariant;
- every live `CatalogTableRootDesc.root_block_id` is allocated and in range;
  and
- allocation-map length times page size is representable without overflow.

Reject an invalid concrete root before physical-length repair. In particular,
never truncate a longer `catalog.mtb` according to a map that omits or lies
below a referenced catalog logical-table root.

Use the logical length captured by `SparseFile::open` and compare bytes against
`expected_len = active_root.alloc_map.len() * COW_FILE_PAGE_SIZE`:

- If equal, install the root without a resize or extra sync.
- If shorter, return `DataIntegrityError::InvalidRootInvariant` beneath
  `RuntimeError::FileRootAccess`, attach file id/path and expected/actual
  lengths, and do not extend, truncate, or install the root.
- If longer, truncate synchronously to `expected_len`, submit and await the
  existing CoW-file `fsync`, emit a structured warning with file kind/id and
  removed bytes, and install the root only after both operations succeed.

Truncation or repair-sync failure returns `RuntimeError::FileRootAccess` while
preserving the underlying `IoError`; the concrete file root is not installed.
This startup-only shrink is safe because no pre-crash transaction or retained
root survives restart, neither user-table runtime nor catalog storage has been
exposed, and map lengths never shrink within a valid runtime history. A catalog
repair failure prevents catalog bootstrap and therefore fails engine startup
closed.

### Risks and mitigations

- Centralizing growth in `CowFile` increases the responsibility of shared CoW
  code. Keep file-specific serialization size and root validation behind
  explicit codec callbacks, while keeping schema/catalog semantics in their
  concrete modules.
- `catalog.mtb` is required for engine bootstrap, so an unsafe repair could
  damage all catalog state. Validate every catalog top-level root against the
  selected map before truncation, never auto-extend a short file, and install
  no root until repair sync succeeds.
- A configured ceiling can exceed an inline meta format's actual map capacity.
  Validate every candidate before `ftruncate` and return a typed capacity error
  without changing either map or file.
- Catalog checkpoint temporarily retains old blocks while writing compact
  replacements. Exercise boundary growth through the real catalog checkpoint
  ordering so unit-only allocation tests do not hide peak-space failures.
- Growth must not add steady-state allocation overhead. Keep the existing
  bitmap allocation as the first branch and collect size, serialize candidates,
  and call `ftruncate` only after exhaustion.

### Documentation and acceptance workload

Update `docs/table-file.md` to document dynamic allocation-map capacity,
sparse extension ordering, the file-specific inline-map ceilings, and startup
stale-tail handling for user and catalog files. Restore
`doradb-bench/templates/checkpoint-table.toml` to one million inserted 128-byte
rows and a 500,000-row freeze budget. Keep template inventory coverage and
execute the restored plan against a fresh storage root as an acceptance check;
the checkpoint must cross the former 16-MiB/256-page limit, publish
successfully, and reopen with the expanded capacity.

Add catalog acceptance coverage that materializes enough catalog-table LWC and
column-index blocks to cross the same boundary through
`CatalogStorage::apply_checkpoint_batch`, publishes the expanded multi-table
root, reopens `catalog.mtb`, and verifies the checkpointed catalog rows and root
capacity. Keep the fixture coherent at the catalog-storage layer so it tests
real catalog checkpoint allocation without requiring thousands of public DDL
operations.

## Implementation Notes

## Impacts

- `doradb-storage/src/bitmap.rs`: allocation-map growth and boundary tests.
- `doradb-storage/src/file/mod.rs`: tracked sparse-file grow/shrink primitives
  and sparse allocation tests.
- `doradb-storage/src/file/cow_file.rs`: configured maximum pages, concrete
  meta-size callback, shared growth-aware allocation/meta reservation,
  runtime-capable allocation result, and publication invariants.
- `doradb-storage/src/file/table_file.rs`: concrete candidate meta sizing,
  expanded user-root validation, growth-aware call-site routing, and recovery
  tests.
- `doradb-storage/src/file/fs.rs`: configuration plumbing, open-time capacity
  reconciliation for both file kinds, repair sync, failure injection coverage,
  and structured events.
- `doradb-storage/src/file/multi_table_file.rs`: configured catalog growth,
  concrete candidate meta sizing, catalog descriptor validation,
  growth-aware early/final meta reservation, and recovery tests.
- `doradb-storage/src/conf/consts.rs`, `doradb-storage/src/conf/fs.rs`, and
  `doradb-storage/src/error.rs`: default, public builder/validation, and typed
  configuration error.
- `doradb-storage/src/catalog/storage/mod.rs`: catalog checkpoint allocation and
  early-reservation call sites plus cross-boundary checkpoint/reopen coverage.
- DiskTree, column block-index, deletion-blob, and table persistence allocation
  call sites adapt to the runtime result without changing algorithms.
- `doradb-bench/src/engine_config.rs` and benchmark configuration tests expose
  and record the shared CoW-file maximum.
- `doradb-bench/templates/checkpoint-table.toml` regains the large checkpoint
  workload that originally exposed the fixed capacity.
- `docs/table-file.md` records the new capacity and recovery contract.
- Existing user and catalog files remain readable without a format migration;
  their initial published capacity remains 16 MiB until a later mutable root
  grows.

## Test Cases

1. `AllocMap` growth preserves allocated bits, allocation count, and free-word
   search state; the source map is unchanged; new bits allocate correctly at
   exact 64-bit and non-word-aligned boundaries.
2. `SparseFile::extend_to` updates tracked and `fstat` logical length, is a
   no-op when already large enough, and leaves an unwritten tail sparse rather
   than allocating proportional disk blocks. Startup truncation updates both
   tracked and on-disk length.
3. User-table and multi-table allocation at the 256-page boundary each double
   capacity and return a block from the new range. Repeated exhaustion doubles
   again and clamps the final step exactly to a non-power-of-two configured
   maximum.
4. Successful allocation within current capacity performs no resize syscall
   for either concrete writer; growth happens once per exhausted geometric
   interval rather than per block.
5. Reaching the configured ceiling and exceeding either concrete inline meta
   payload return `StorageFileCapacityExceeded` with size context and leave the
   mutable map, physical length, and active root unchanged.
6. Injected `ftruncate` failure for each file kind preserves the old mutable
   map, returns the underlying `IoError` through `FileRootAccess`, and does not
   return an out-of-extent block id.
7. Filling the old map with data blocks still permits user commit and catalog
   commit because final meta reservation uses the growth-aware path. Catalog's
   explicit reserve-before-rebuild and metadata-only reserve/reclaim paths also
   grow when necessary.
8. Publication `fsync` failure after user or catalog expansion keeps the
   in-memory active root and its shorter map authoritative.
9. Abandoning either mutable root after successful extension leaves a longer
   sparse file. Reopen selects the old root, truncates to its map capacity,
   syncs the repair, and installs that root.
10. Repair `ftruncate` or repair `fsync` failure prevents concrete root
    installation. Reopening either file kind shorter than its selected map
    capacity fails closed with `InvalidRootInvariant` and never auto-extends it.
11. A committed expanded user root reopens without truncation. Its top-level
    column and secondary roots validate in range, while crafted out-of-range or
    unallocated roots are rejected before installation.
12. A committed expanded catalog root reopens without truncation. Every live
    catalog descriptor root validates allocated and in range; crafted invalid
    descriptors are rejected before any longer tail is truncated.
13. Reachability rebuild after one or more expansions preserves the expanded
    map length and reclaims only unreachable bits for both file kinds. Current
    and retained older user roots, and catalog old-root guards, remain readable
    until their normal retention gates release them.
14. User checkpoint/LWC publication and secondary-index DiskTree construction
    both cross the former boundary, commit, reopen, and read their persisted
    data.
15. Catalog checkpoint materializes LWC and column-index blocks across the
    former boundary through `CatalogStorage::apply_checkpoint_batch`, commits,
    reopens `catalog.mtb`, and reads the persisted catalog rows and expanded
    root capacity.
16. Metadata-only catalog root and first-retained-redo-marker publication still
    reclaim only the displaced meta block and can grow solely to reserve their
    replacement meta block.
17. Configuration tests cover the 16-GiB default, builder propagation, byte and
    page validation, benchmark overlay merge/round-trip, and existing user and
    catalog maps larger than a lowered runtime ceiling.
18. Growth and stale-tail repair events identify the concrete file kind/id and
    report old/new or expected/actual bytes without logging fast-path
    allocations.
19. Restore and execute the million-row/500,000-row
    `checkpoint-table.toml` plan against a fresh root; verify successful
    non-silent publication and reopen instead of `StorageFileCapacityExceeded`.
20. Run `rtk cargo nextest run --workspace`.
21. Run
    `rtk cargo nextest run -p doradb-storage --no-default-features --features libaio`
    because sparse length changes and repair sync touch backend-neutral storage
    I/O paths.

## Open Questions

None. This task deliberately chooses inline geometric growth for both durable
CoW file kinds. Out-of-line allocation metadata and expansion of non-CoW
sparse-file consumers require separate planning.
