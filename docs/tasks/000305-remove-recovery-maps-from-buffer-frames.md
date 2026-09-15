---
id: 000305
title: Remove Recovery Maps from Buffer Frames
status: implemented
created: 2026-09-15
github_issue: 1065
---

# Task: Remove Recovery Maps from Buffer Frames

## Summary

Row pages now retain the permanent `RowVersionMap` installed during allocation.
Recovery owns temporary inserted-slot bitmaps in a nested table/page hash map,
passes them explicitly to hot-row replay, and consumes them during final index
reconstruction. Normal transaction reads no longer select a recovery-specific
metadata variant.

Canonical sequential commit-order replay eliminates per-row recovery CTS
storage. One inserted bit per reserved slot preserves duplicate-insert
rejection, including insertion into a previously deleted slot. Page creation
CTS remains on the permanent version map throughout recovery.

## Context

Previously, recovery replaced the allocation-time version map with a
`RowRecoveryMap` containing page creation CTS and per-row timestamps. Final
index reconstruction replaced that map with another empty version map.
Recovery-only frame and guard APIs also introduced branching into ordinary
transaction row access.

No foreground transaction or pre-crash undo chain survives startup recovery.
The allocation-time empty version map is valid while committed redo rebuilds
the latest page image. The old per-row CTS values did not select whether redo
was applied; only the distinction between never-inserted and previously
inserted slots was needed for slot-history validation.

Physical deletion flags alone cannot make that distinction, and commit order
can differ from RowID allocation order. The coordinator therefore retains an
inserted-slot bitmap for each recovery allocation. Both registry levels use
`FastHashMap`; reconstruction order is intentionally unspecified.

There is no parent RFC or source backlog.
[Backlog 000087](../backlogs/000087-refactor-recovery-process-parallel-log-replay.md)
remains related future parallel-replay work and is not resolved by this task.

Issue Labels:

- type:task
- priority:medium
- codex

## Goals

- Keep one permanent version-map allocation per row page and preserve its exact
  creation CTS, active state, empty recovery undo heads, and buffer residency.
- Make replay-state ownership and borrowing explicit by table and page identity.
- Retain duplicate-insert, row-bound, payload, page-space, and deletion checks
  with one inserted bit per reserved slot.
- Preserve replay filters, cold-delete semantics, catalog replay, and recovered
  unique and non-unique index contents.
- Release sidecars during successful reconstruction, table drop, or failed
  recovery teardown without exposing them to foreground startup.

## Non-Goals

- Parallel replay, replay scheduling, or new stream-order validation.
- Persistent-format, checkpoint-boundary, redo-ordering, or MVCC changes.
- A new bitmap implementation, shared bitmap API changes, or dependencies.
- Version-map storage redesign, general page builders, or buffer-pool redesign.
- Production ownership counters or test-runner configuration changes.

## Rejected Alternatives

- Delaying version-map initialization until recovery completion would require
  a separate read path and page-conversion lifecycle. Retaining the original
  map keeps one allocation authority and ordinary row-read semantics.
- Recovery page builders or typestate conversion would broaden an ownership
  refactor into a general page-lifecycle abstraction.
- Removing inserted-slot history would accept malformed insert/delete/insert
  histories. A row-count watermark would also reject valid sparse replay.

## Plan

### Permanent buffer metadata

`BufferFrame` directly owns `Option<Box<RowVersionMap>>` in the former context
field position. The 128-byte size and alignment assertions remain enforced.
Row allocation initializes the map through `init_undo_map`; non-row and
uninitialized frames retain `None`. Existing deallocation paths clear that
ownership, and metadata continues to survive page-body eviction.

`FrameContext`, frame/guard recovery-map accessors, `RowRecoveryMap`, and
`RowReadState` are removed. `RowReadAccess` holds a `RowVersionReadGuard`
directly and preserves the former runtime branch for latest reads, MVCC,
undo access, and key-history checks.

### Coordinator-owned replay state

`RecoveryCoordinator::recovered_tables` contains
`FastHashMap<TableID, FastHashMap<PageID, RowReplayState>>`.
Each sidecar owns a `PageID` and a `Box<[u64]>` created with
`new_bitmap(max_row_count)`. It retains no timestamps, frame pointers, guards,
table handles, or shared ownership.

Eligible page creation reserves a vacant registry entry before allocation,
rejecting duplicate live registration without overwriting the original state.
It then allocates the exact row page, restores creation CTS on its version
map, and registers the allocation identity and reserved bitmap capacity.
Empty created pages are included.

Hot insert borrows its sidecar mutably; update and delete borrow it immutably.
Sequential replay and sidecar removal before page reuse keep the page ID
bound to its allocation; there is no separate generation check or identity
assertion. Hot replay obtains the exclusive page guard, and page mutation
helpers validate the logical RowID range before touching bitmap storage,
excluding padding bits in the final word.

Insert requires an unset inserted bit and a physically deleted slot. Payload,
slot, and space checks precede row writes, and only successful insertion sets
the bit. Update and delete require a set bit and a live row; neither clears
the bit. Invalid histories produce typed integrity reports with replay context.

### Filtering and cold replay

Unknown-table, checkpoint, heap, pivot, and deletion-cutoff filters precede
sidecar lookup. Eligible hot redo with missing state fails; skipped records
need no sidecar. Eligible hot `Delete(None)` remains an `InvalidPayload` error
at the coordinator boundary.

`recover_cold_row_delete` synchronously validates a cold RowID against the
startup root and applies its committed marker to `ColumnDeletionBuffer`.
Same-CTS replay remains idempotent; conflicting markers remain
`InvalidRootInvariant`. Cold redo ignores legacy hot-page identity and needs
neither a hot-page lookup nor a sidecar. Catalog logical replay is unchanged.

### Reconstruction and cleanup

After existing catalog-parent, descriptor, metadata, and absent-file checks,
`rebuild_hot_indexes` takes the registry with `mem::take` and consumes both hash
maps. It requires a live table runtime, drops each sidecar before scanning its
page, acquires its shared guard, and rebuilds unique and non-unique indexes
through ordinary latest-row reads.

There is no page refresh, version-map replacement, or fallback creation CTS.
Redo repair and writable-log preparation still follow successful index
reconstruction. Table drop removes its registry group before replay advances
to page reuse. Failure drops the consuming coordinator or remaining owned
reconstruction locals; no sidecar escapes into startup results.

## Implementation Notes

Implemented coordinator-owned bitmap replay state while retaining each row
page's original version map. Final review simplified the sidecar and index
reconstruction interfaces to `PageID`: the stored and fetched identities come
from the same allocation, whose lifetime is established by sequential replay
and coordinator cleanup. The redundant generation check and its synthetic
stale-sidecar test were removed without a replacement assertion. No deferred
implementation issues remain.

Review confirmed that all removed recovery frame/read variants and accessors
are absent from Rust sources. Buffer and transaction row-access modules have
no recovery-state dependency. Bitmap access follows page-range validation,
and failed insert validation leaves the inserted bit clear. The existing
shared bitmap implementation is unchanged.

Regression tests verify version-map pointer continuity and exact creation CTS
through reconstruction, active page state, empty undo heads, sparse insertion
order, bitmap word boundaries, duplicate registration, table-drop page reuse,
and registry consumption on reconstruction errors.
The final review also retained legacy page-ID cold-delete coverage at the
coordinator boundary after extracting the synchronous cold-delete method.

Validation:

- `rtk cargo fmt --check`: passed.
- `rtk cargo clippy --workspace --all-targets -- -D warnings`: passed.
- `tools/style_audit.rs --diff-base origin/main`: passed for ten Rust files,
  including formatting, strict Clippy, and repository structure checks.
- `rtk cargo nextest run --workspace`: 2,033 tests passed across four binaries.
- Focused coverage across the ten changed Rust files: 96.12% overall;
  every file exceeds 93%, and `RowReplayState` has 100% line coverage.
- `tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md`: unchanged.
- Diff whitespace checks passed. No storage backend or backend-neutral I/O
  behavior changed, so the optional alternate-backend pass was not required.

## Impacts

- Buffer frames contain permanent runtime metadata only; their layout,
  generation checks, residency, and deallocation contracts remain intact.
- Recovery owns temporary per-page slot history and restores page creation
  CTS once. Table hot replay borrows this state explicitly.
- Transaction row access uses one runtime metadata path.
- [Recovery documentation](../recovery.md) describes bitmap ownership,
  sequential replay, and completion without an undo-map refresh.
- No persistent format, public API, schema, or MVCC contract changed.
- Runtime maps and sidecars coexist during replay. Bitmap payload is
  `8 * bitmap_required_units(max_row_count)` bytes per page plus registry and
  sidecar overhead; bitmaps are released as reconstruction consumes pages.
  The additional coexistence cost is accepted in exchange for one permanent
  version-map allocation and a simpler metadata lifecycle.

## Test Cases

- Empty and populated pages retain their version maps, creation CTS, active
  state, and empty undo heads after reconstruction.
- Slots 0, 63, 64, and 69 in a 70-slot page accept sparse, descending RowID
  insertion; updates/deletes retain insertion history. Live and deleted
  duplicate inserts, missing inserted state, invalid physical state,
  out-of-range padding slots, malformed payloads, and insufficient space fail.
- Replay filters allow skipped hot redo without sidecars; eligible hot redo
  reports missing state or missing delete page identity with typed context.
- Replayed drop removes the table group before reuse of its page ID with a
  fresh sidecar and clear bitmap. Duplicate registration preserves the
  original sidecar and creation metadata.
- Unique and non-unique reconstruction handles sparse, deleted, and empty
  pages under different registration orders. Duplicate-key and missing-runtime
  failures consume the remaining registry.
- Cold replay preserves cutoff filtering, same-CTS idempotence, conflicting
  marker rejection, and independence from legacy hot-page IDs.
- Existing restart, foreground read/write, checkpoint, mixed hot/cold, catalog,
  eviction, metadata-residency, and deallocation regressions remain green.

## Open Questions

None blocking. Future parallel recovery remains in related backlog 000087.
