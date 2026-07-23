# Data Checkpoint

## Overview

Data checkpoint moves a contiguous prefix of hot RowStore pages into immutable
LWC blocks. It is driven by the tuple-mover maintenance path and runs as one
part of the table checkpoint described in [Checkpoint](./checkpoint.md).

Its responsibilities are deliberately narrow:

- freeze a stable hot-page prefix without exposing partial persistence
- choose the committed row image covered by the purge-published cutoff
- build LWC blocks and matching secondary-index `DiskTree` entries from that
  same image
- atomically advance `pivot_row_id`, `heap_redo_start_ts`, and the relevant CoW
  roots
- hand the retired hot-page prefix to transaction GC after ordered publication

Data checkpoint contributes a heap replay boundary; it does not decide global
redo retention by itself. Recovery combines that boundary with catalog and
deletion bounds as described in [Recovery](./recovery.md).

## Row-Page Lifecycle

Each selected RowPage moves monotonically through three states:

| State | Foreground behavior | Checkpoint role |
| --- | --- | --- |
| `ACTIVE` | Accepts normal inserts, updates, deletes, and locks | Candidate for a future frozen prefix |
| `FROZEN` | Rejects new insert/update payload changes; delete/lock metadata may still change while existing writers drain | Source of readiness analysis and transition planning |
| `TRANSITION` | Payload is immutable; foreground access follows transition routing | Source of LWC construction until the new root is installed |

Every row or undo mutation allowed on a frozen page is bracketed by paired
mutation-version increments. The value is a change detector, not an odd/even
seqlock: different row writers may overlap, so a plan is reusable only when the
complete value is equal before and after preparation.

## Table-Owned Workflow

One live table owns at most one canonical frozen-page batch. The workflow
retains:

- the selected pages and their RowID boundaries
- `frozen_ts`, allocated after every selected page is visibly frozen
- the first surviving hot page's creation timestamp when one was observed
- per-page readiness state and exact blocker generations
- cutoff-specific prepared transition plans that remain reusable

The state belongs to the table rather than to one caller. A delayed or
cancelled attempt restores the same batch and cache so a later retry cannot
silently extend the frozen prefix or move its original fence.

## Freeze

Freeze runs from an idle session under scoped `TableMetadata(S)` and
`TableData(IS)` locks. It revalidates the live table before claiming the
workflow and retains the locks through frozen-state publication. A covering
explicit session lock is preserved when the scoped grants are released.

The freeze path:

1. Scans a contiguous prefix of the oldest hot pages up to the requested row
   budget and records the next page's creation timestamp when a successor is
   already visible.
2. Loads every selected page and preflights the full batch while all pages are
   still `ACTIVE`.
3. Publishes each page as `FROZEN` in its own short page-state critical section.
4. Allocates `frozen_ts` only after the final page has been frozen.
5. Installs the batch and returns a read-only summary to the caller.

If a canonical batch already exists, another freeze request returns that same
summary. It does not append newly created hot pages.

`TableData(IS)` remains compatible with ordinary `IX` DML and shared table
readers. It conflicts with the `TableData(X)` used by sequential full-table
update or delete, so those operations cannot cross the freeze/publication
boundary ambiguously.

## Readiness and Cutoff

Checkpoint uses the purge-published GC horizon as an exclusive cutoff for the
entire frozen batch. A page is ready only when checkpoint can produce its
cutoff-visible committed image without losing an unresolved pre-fence row
image.

Readiness is incremental and ordered:

1. Reanalyze only pages whose state is `Unchecked` or `Blocked`.
2. Reuse a `Stable` image proof rather than rescanning its undo state.
3. Stop at the first unsafe page and preserve the stable prefix before it.
4. Leave the suffix unchecked until that canonical blocker has progressed.

A stable page may still require a newer cutoff because of a committed image.
That requirement is cached; the page is not rescanned while the published
horizon remains too old.

If any page is unready, checkpoint returns `FrozenPageCutoff` with the table,
page, selected cutoff, known required cutoff, stable-prefix count, and whether
an unresolved status remains. No page enters `TRANSITION` and no persistent
state is applied on this outcome.

## Transition Planning

After every page has a stable readiness proof, checkpoint performs one full
optimistic plan-refresh pass before acquiring page-state write locks.

For each page, one undo walk follows leading lock and delete records through
the first image-producing insert or update. It produces an owned plan containing:

- the cutoff-visible deletion bitmap used to filter the LWC image
- the minimum cutoff required by committed row images
- transition overlay markers that preserve representable post-fence ownership
- the page identity, cutoff, and mutation version that validate reuse

Checkpoint loads the mutation version before and after plan construction. Full
equality retains the plan; any change discards its attempt-local bitmap and
markers immediately. The stable readiness proof remains cached because later
lock/delete ownership on a frozen page is representable once its original
image-producing obligations are resolved.

## Irreversible Transition

Only a fully ready attempt may acquire lifecycle publication admission and
cross the irreversible boundary. It then visits pages in canonical order,
holding one page-state write lock at a time.

Under each lock, checkpoint revalidates page identity, cutoff, and the complete
mutation version. A matching optimistic plan is reused. A missing or stale plan
is rebuilt under that page-local lock without repeating the initial pre-fence
blocker test; frozen pages cannot introduce a new insert/update image after the
stable proof.

The page becomes `TRANSITION`, its prepared markers are installed, and the lock
is released immediately. During this phase the batch may contain a growing
`TRANSITION` prefix followed by a `FROZEN` suffix. Once the first page changes
state, unexpected analysis, marker, build, publication, or system-commit
failure is fatal and wakes transition-route waiters through storage poison.

## LWC and Secondary-Index Construction

LWC construction reads immutable page values through each prepared deletion
bitmap. Deleted or otherwise cutoff-invisible rows are omitted while explicit
RowIDs preserve the sparse logical range. If an output block must split, the
same prepared bitmap is reused rather than walking undo state again.

For every row accepted into an LWC block, checkpoint sends the identical
visible value and RowID to the secondary-index sidecar. This produces companion
`DiskTree` entries from exactly the rows represented by the new cold data.
There is no later scan of arbitrary `MemIndex` state.

The frozen prefix determines the next `pivot_row_id`. The next surviving hot
page determines `heap_redo_start_ts` when one was observed. If no successor was
visible at freeze time, checkpoint resolves the hot boundary after allocating
`checkpoint_ts`; when the heap is empty, that timestamp becomes the fallback
heap replay floor.

## Atomic Publication

The data phase updates one mutable table-file fork with:

- new LWC blocks
- matching `ColumnBlockIndex` entries
- the advanced `pivot_row_id`
- the new `heap_redo_start_ts`
- companion secondary-index roots

Deletion checkpoint may add cold-delete state and more secondary-index work to
the same fork before publication. Checkpoint then rebuilds allocation
reachability and publishes one root, so readers cannot observe new LWC data
without its block-index and secondary-index state.

The system transaction records the data-checkpoint marker with
`checkpoint_ts`. Root publication becomes durable before the system work
receives its ordered `redo_cts` and is accepted for enqueue. The table-owned
workflow completes only after this handoff succeeds.

If the run advances only replay bounds and changes no table-file state, the
generic checkpoint path publishes a catalog-backed silent watermark instead
of a metadata-only table root. That decision and its durability rule belong to
[Checkpoint](./checkpoint.md#replay-bound-only-checkpoints).

## Waiting and Retry

`wait_for_checkpoint_retry` can wait on the exact delayed page. It retains the
complete current blocker generation, waits for every unresolved
image-producing status in that generation, and reanalyzes once after they are
terminal. A replacement blocker generation is handled the same way. A stable
page that only needs a newer cutoff waits on purge-horizon progress.

Listener registration always rechecks the underlying predicate, so progress
racing registration is not lost. Table drop makes the delayed batch obsolete;
storage poison and shutdown terminate the wait. A successful wait does not
promise publication because another page or root-liveness gate may become the
next canonical delay.

## Retirement and Recovery Boundary

A nonempty publication collapses the frozen prefix into one
`RetiredRowPageBatch` containing the table id, RowID bounds, and ordered page
ids. It travels as a volatile system payload through table-affine transaction
GC. It is eligible only when its ordered system CTS is strictly older than the
active snapshot horizon. GC validates and unlinks the exact hot-index prefix
before deallocating the returned pages.

The batch is not redo. Restart loads the durable pivot and block-index root,
creates a fresh empty hot index beginning at that pivot, and never replays
runtime page retirement. See [Garbage Collection](./garbage-collect.md) for the
physical cleanup protocol and [Recovery](./recovery.md) for heap replay.

## Summary

Data checkpoint is safe because the batch and fence remain stable across
retries, readiness is proven before transition, LWC and secondary-index entries
share one committed image, and the resulting roots and replay boundary are
published atomically before row-page retirement becomes eligible.
