---
id: 000224
title: Support full-table MVCC update
status: proposal
created: 2026-07-14
github_issue: 848
---

# Task: Support full-table MVCC update

## Summary

Add a sequential full-table MVCC update operation to the public `Statement`
API. The operation invokes a row-local callback for every original row eligible
for modification, exposes the latest modifiable row image, updates only rows
selected by the callback, acquires a transaction-duration exclusive table-data
lock, preserves unique-index and rollback guarantees, and prevents its
delete-plus-insert or move-update output from being visited again by the same
scan.

Expose a storage-independent `LazyRow` callback argument with lazy mutable
column access. Cold rows are updated as delete plus hot insert. Hot rows are
updated in place when possible and moved otherwise. Full-table update is made
mutually exclusive with active freeze/checkpoint work through the logical table
lock manager, removing transition-page handling from this operation.

## Context

`Statement` currently exposes full-table MVCC scan and point update through a
unique index, but it cannot update a callback-selected subset without a unique
lookup for each row. The existing full scan captures a table-root snapshot,
scans persisted LWC blocks, and then scans hot row pages from the captured
pivot. Its fixed `read_set` callback is suitable for projection but does not
provide the lazy, storage-independent row access needed for computed updates.

The existing unique update path already contains the required physical
mutation protocols: cold delete plus insert, hot in-place update, hot
move-update, per-index key replacement, immediate unique ownership checks,
statement effects, undo, and redo. This task should extract or adapt those
mechanics rather than add a second transaction or recovery protocol.

Row insertion may append to an existing non-full page selected through the
session insert-page cache or the table free list, or allocate a page after the
current row-page block-index boundary. A full update therefore needs an
explicit original-scan boundary. Capturing every original page's `row_count`
would be correct but unnecessarily eager. The chosen design snapshots the
original page descriptors and block-index upper bound, then records an
exclusive original-row boundary only if this operation inserts into an
original page that has not started scanning.

Checkpoint currently does not acquire a logical table lock. It coordinates
through the table checkpoint workflow, lifecycle/root-mutation leases, and row
page state latches. RFC-0016 already defines `TableMetadata` and `TableData`
resources with an `IS` mode that is compatible with ordinary `IX` writers and
logical `S` readers but conflicts with `X`. Freeze and checkpoint will join
that protocol with scoped `TableMetadata(S)` and `TableData(IS)` locks. Existing
checkpoint workflow and lifecycle leases remain authoritative for checkpoint,
DDL, drop, and publication coordination.

The lock manager intentionally supports only immediate lock conversion. A
transaction that already holds `TableData(IX)` may convert to `X` only when the
conversion is immediately compatible with granted locks and the wait queue;
otherwise full-table update returns the existing `LockUpgradeWouldBlock`
error. Adding blocking conversion requires deadlock/converter policy and is
outside this task.

Relevant design references:

- `docs/architecture.md`
- `docs/transaction-system.md`
- `docs/index-design.md`
- `docs/checkpoint-and-recovery.md`
- `docs/table-file.md`
- `docs/block-index.md`
- `docs/rfcs/0016-logical-lock-manager.md`
- `docs/process/unit-test.md`
- `docs/process/coding-guidance.md`
- `docs/tasks/000156-full-table-scan-mvcc.md`

Issue Labels:

- type:task
- priority:high
- codex

## Goals

1. Add a public sequential `Statement::table_update_mvcc` API that can inspect
   arbitrary columns lazily and return a row-specific sparse update or skip the
   row.
2. Add a public `LazyRow` abstraction whose mutable `val(column_no)` method
   returns a cached `&Val` for the latest row image eligible for modification,
   independent of whether the source is cold or hot.
3. Acquire transaction-duration `TableMetadata(S)` and `TableData(X)` before
   taking the update scan snapshot or invoking the callback. Retain both locks
   until transaction commit or rollback, including after statement rollback.
4. Make active freeze/checkpoint work and full-table update mutually exclusive
   by acquiring scoped `TableMetadata(S)` and `TableData(IS)` for freeze and
   checkpoint. Hold the locks through each active call's final state/root
   publication and system-transaction completion.
5. Snapshot the original hot row-page descriptors and block-index upper bound
   before any mutation, release all row-page-index latches, and scan only the
   captured page set.
6. Exclude rows inserted by this operation without eagerly capturing every
   page's row count or status. Lazily record the first inserted `RowID` only for
   an original, not-yet-scanned page.
7. Update cold rows as delete plus insert; update active hot rows in place when
   the new representation fits; otherwise perform move-update. Previously
   frozen hot pages use move-update.
8. Maintain every affected unique and non-unique index with existing immediate
   ownership and visibility rules.
9. Roll back all row, deletion-buffer, and index changes when callback,
   validation, uniqueness, write-conflict, storage, or index processing fails
   after any number of earlier rows.
10. Reuse existing undo and redo formats so committed full-table updates and
    their rollback/restart behavior require no new recovery record kind.
11. Include necessary and straightforward performance measures while keeping
    correctness as the primary acceptance criterion.

## Non-Goals

- Parallel scan or update execution.
- Vectorized callbacks, a general DML pipeline, or an all-row work queue.
- A public or general internal update-by-`RowID` primitive.
- Transition-page retry or relocation handling inside full-table update;
  checkpoint exclusion removes that state from the operation.
- Eager row-count/status capture for every original row page.
- Tracking insertion boundaries for pages allocated at or above the captured
  block-index upper bound.
- Final-state or batch uniqueness. Unique keys are claimed per row, so swaps or
  permutations may fail on an intermediate duplicate.
- Blocking `IX` to `X` lock conversion, deadlock detection, lock timeouts, or a
  new converter policy.
- Page-batch index mutation, coarse index rebuild, or new MemIndex APIs.
- New undo, redo, table-file, LWC, deletion-buffer, or recovery formats.
- A hard throughput/latency threshold or speculative performance tuning.
- Undoing external side effects performed by callback code.

## Plan

### 1. Public callback contract

Add and re-export an opaque public row accessor with private storage-specific
state:

```rust
pub struct LazyRow<'row> {
    // private source, value buffer, and readiness state
}

impl LazyRow<'_> {
    pub fn column_count(&self) -> usize;
    pub fn val(&mut self, column_no: usize) -> Result<&Val>;
}
```

Add the `Statement` method:

```rust
pub async fn table_update_mvcc<F>(
    &mut self,
    table_id: TableID,
    update_row: F,
) -> Result<usize>
where
    F: for<'row> FnMut(
        &mut LazyRow<'row>,
    ) -> Result<Option<Vec<UpdateCol>>>;
```

The callback contract is:

- invoke it exactly once for each original row eligible for the latest
  modification read, unless a write conflict aborts the statement first;
- `Ok(None)` skips the row;
- `Ok(Some(update))` selects the row and applies the sparse update;
- `Ok(Some(Vec::new()))` counts the row as matched but performs no physical
  mutation;
- return the number of callback-selected rows after the whole operation
  succeeds;
- propagate callback and lazy-load errors through ordinary statement failure
  and rollback;
- validate column access bounds unconditionally because `LazyRow` is a
  public safe API;
- validate callback-produced sparse updates according to the existing
  `disable_dml_validation` policy before mutating that row; and
- do not expose a page, block, physical location, or `RowID` to callback code.

The callback is synchronous and row-local. References returned by `val` are
tied to its mutable borrow and cannot outlive the callback invocation.

Row modification is a current read, not a snapshot-version reconstruction.
For a cold row, the persisted image is exposed only if no persisted or runtime
delete marker has consumed it. Any committed marker and an active marker owned
by this transaction skip the old cold image so its latest hot replacement, if
any, is visited instead; an active marker owned by another transaction returns
`WriteConflict` before the callback. For a hot row, the callback reads the
latest physical page image regardless of the transaction snapshot timestamp. A
committed undo head and an active head owned by this transaction are readable;
an active head owned by another transaction returns `WriteConflict` before the
callback. The `_mvcc` suffix describes transaction ownership, undo, rollback,
and visibility infrastructure, not an older snapshot image supplied for
modification.

### 2. Lazy row implementation

Represent the reusable row buffer with a column-count-sized `Vec<Val>` plus a
ready bitmap or ready-column list. Unready positions contain an internal
placeholder whose value is ignored until the corresponding ready bit is set.
`val` validates the column number, loads the value from the row source on first
access, stores it in the stable vector slot, marks it ready, and returns a
reference to that slot. Repeated access returns the cached value.

Provide private cold and hot row sources behind the same API:

- after current-read marker validation, the cold source performs direct
  per-column decode against the already loaded persisted LWC block and row
  ordinal;
- the hot source acquires one `RowReadAccess`, validates latest-image ownership
  once, retains its row-version read guard through the callback, and clones
  requested columns directly without undo traversal or a `Vec<Val>` projection;
  and
- skipped rows clear only initialized slots so the allocation can be reused.

For a selected cold row, fill the unready columns needed for the replacement
while the LWC block is still available. Preserve or decode old index-key values
before overwriting updated columns. Apply the sparse update and hand the
completed `Vec<Val>` to the per-block pending insertion buffer. Allocate a new
scratch vector only when ownership of the current one is transferred.

Hot in-place and move-update use the callback-produced sparse update. They may
reuse and clear the accessor buffer after the callback; the existing hot
updater remains responsible for cloning the latest complete row when a move is
required.

### 3. Logical lock integration

Full-table update follows the existing write lock order:

1. resolve and validate the live table;
2. acquire transaction-duration `TableMetadata(S)`;
3. acquire transaction-duration `TableData(X)`;
4. take layout/root/page snapshots and begin callbacks or row ownership.

A fresh `X` request waits for active `IS`, `IX`, `S`, or `X` holders. An
already-held `X` is reused. An existing transaction-owned `IX` follows the
RFC-0016 immediate-conversion rule: proceed only if conversion is immediately
safe, otherwise return `LockUpgradeWouldBlock` without invoking the callback or
mutating a row.

Add a scoped maintenance-lock helper for idle session operations. Both
`Session::freeze_table` and `Session::checkpoint_table` acquire, in resource
order:

1. `TableMetadata(S)`;
2. `TableData(IS)`.

Use the idle session owner and owner group, retain guards only for grants newly
created by the call, and preserve a covering explicit session lock. Revalidate
table liveness after acquisition. Enter the freeze/checkpoint workflow only
after lock admission so a call waiting behind full update does not reserve the
workflow indefinitely. Release fresh maintenance grants on every published,
delayed, cancelled, or error return.

Hold checkpoint locks through transition preparation/application, LWC and
index-sidecar construction, table-file root publication, row-page block-index
pivot/root update, and checkpoint system-transaction completion. Hold freeze
locks through frozen-state publication and frozen-batch completion. Keep the
existing workflow and lifecycle leases unchanged.

`TableData(IS)` is intentionally compatible with ordinary `IX` DML and
explicit `S` table readers, preserving fuzzy-checkpoint concurrency. It
conflicts with the full update's `X`. Therefore a granted full-update `X`
cannot overlap page freeze or transition, and any transition already in
progress must finish before `X` is granted. A frozen batch may remain between
separate freeze and checkpoint calls; full update handles its already-frozen
pages through move-update.

Update logical-lock and checkpoint documentation to record this maintenance
operation mapping and the transaction-duration meaning of full-update
completion.

### 4. Original hot-page snapshot

After acquiring `X` and taking one coherent table-root snapshot, traverse the
hot row-page block index from the captured pivot and snapshot:

- the exclusive block-index upper-bound `RowID`; and
- ordered descriptors for every original row page below that bound, including
  enough page identity and reserved row range to load and classify it later.

Release every block-index cursor/leaf latch and temporary page guard before
invoking callbacks or performing mutations. Do not retain all row pages and do
not capture their row counts or active/frozen states. Checkpoint exclusion
means the captured descriptors cannot transition or be checkpoint-retired
during the full update.

The original page descriptor list is the only hot-page scan worklist. A page
allocated by this update begins at or above the captured block-index upper
bound and is absent from the list, so it needs no insertion-boundary tracking.

### 5. Lazy inserted-row boundary tracking

Maintain the current original-page ordinal and a sparse map from future
original page identity/range to an exclusive `RowID` boundary. Observe each
successful physical replacement insertion before later scan progress:

- if the inserted `RowID` is at or above the captured block-index upper bound,
  ignore it;
- if its original page has already started or finished scanning, ignore it
  because that page already has a fixed local scan end;
- if it belongs to an original page that has not started, store the inserted
  `RowID` as that page's exclusive original-row boundary; and
- on repeated insertions into the same future page, preserve the minimum
  boundary.

At the start of each original page, choose its fixed exclusive scan end from
the sparse boundary map when present; otherwise read the page's current
`row_count` and derive the end. Mark the page as started before invoking any
callback or mutation. Appends to the current or an earlier page are then
outside a fixed/completed range, while appends to a future original page are
excluded by its first-insert boundary.

Thread an internal insertion-placement result or observer through the cold and
hot move-insert helpers so the tracker can see the inserted `RowID` and page
identity without changing the public insert API. This is scan-local boundary
bookkeeping, not a generic mutation pipeline.

### 6. Cold scan and mutation

Scan cold data from the captured table root before processing the captured hot
page list. For each persisted LWC block:

1. load and validate the block once;
2. apply current-read deletion-marker rules per row, skipping persisted,
   committed, and own-consumed cold images while rejecting a foreign active
   owner before callback entry;
3. create a cold `LazyRow` and invoke the callback;
4. validate selected updates and count empty selected updates without mutation;
5. materialize selected replacement rows and required old index keys into a
   block-local pending vector;
6. release the readonly block guard; and
7. sequentially execute each pending cold delete, hot insert, index mutation,
   and insertion-boundary observation.

Callbacks for one cold block may be completed before its pending mutations are
applied. This bounded staging keeps one block load, avoids holding a readonly
cache guard across asynchronous insertion/index work, and does not change the
row-local callback contract.

Use the existing deletion-buffer ownership and cold-row undo/redo path. Record
the cold delete effect before any subsequent step that can fail. Construct the
replacement from the eligible persisted image, insert it into hot storage, and
maintain only indexes whose encoded key changed.

### 7. Direct hot-page mutation

Process the captured page descriptors in order using each page's fixed row
end. For every original row still present in the latest page image:

1. acquire one latest-row read guard, reject another active owner, and create a
   hot `LazyRow` over the latest physical image;
2. invoke and validate the callback exactly once;
3. drop all callback/read borrows before acquiring row write ownership;
4. call a direct-page hot update helper without a unique-index lookup; and
5. maintain changed indexes and observe any replacement insertion.

Refactor or extend `HotRowUpdater` so the scan path can lock and update the
known page/row directly without supplying a unique lookup key. Preserve its
existing write-conflict checks and effect ordering.

- For an active row whose new representation fits, update in place and retain
  its `RowID`.
- For an active row without sufficient space, delete the old physical version,
  release its page guard, and insert the complete replacement.
- For an already-frozen page, use the same move-update path.

Do not hold a row-page-index leaf latch or the old page guard while inserting a
replacement. Because freeze/checkpoint holds `IS`, `RowPageState::Transition`
is unreachable after full update acquires `X`; treat observing it as an
internal invariant violation rather than rerouting by `RowID` or re-invoking
the callback.

### 8. Indexes, uniqueness, effects, and recovery

Reuse existing index-key comparison and update helpers. For each physically
updated row:

- derive old and new keys only for indexes affected by updated columns;
- retain unchanged index entries;
- delete/mask old changed entries and claim new changed entries using current
  unique or non-unique protocols; and
- enforce unique constraints immediately in scan order.

An intermediate duplicate, including a unique-key swap, returns the existing
duplicate-key or write-conflict error. It must not be treated as valid based on
the proposed final table state.

All row and index work must be represented in `StmtEffects` before a later
fallible step can leave it externally visible. Preserve current rollback order:
index effects first, row/deletion effects second, then discard statement redo.
Exercise existing redo kinds for hot update, delete, and insert. Do not add a
full-update log record or recovery branch.

### 9. Performance and deferral policy

Required straightforward measures are:

- direct hot-page mutation with no per-row unique lookup or common-path
  `RowID` resolution;
- one validated load per cold block;
- lazy column decode and reuse of callback-accessed values;
- bounded per-cold-block insertion staging;
- one page-descriptor snapshot rather than an all-row target list;
- sparse insertion-boundary state only for future original pages that actually
  receive replacements; and
- maintenance of changed indexes only.

Do not make benchmark thresholds an acceptance gate. If implementation or
profiling produces concrete evidence for feasible but nontrivial tuning, check
existing backlog items and record a deduplicated follow-up during task resolve
using the backlog workflow. Do not create speculative backlog items during
design or interrupt correctness work for deferred tuning.

## Implementation Notes

## Impacts

- `doradb-storage/src/trx/stmt.rs`
  - public `Statement::table_update_mvcc` admission, validation, lock
    acquisition, effect integration, and result contract;
  - an `X`-mode table-data acquisition path in addition to ordinary write
    `IX` acquisition.
- `doradb-storage/src/table/access.rs`
  - full-update orchestration, cold block staging, hot page processing,
    insertion-boundary tracking, and reuse of existing index mutation helpers.
- `doradb-storage/src/table/mem_table.rs`
  - row-page descriptor snapshot independent of held block-index leaf latches;
  - direct-page scan/update support and internal insertion placement reporting.
- `doradb-storage/src/table/hot.rs`
  - direct known-row update entry point retaining in-place/move behavior and
    existing undo/redo semantics.
- `doradb-storage/src/trx/row.rs`
  - latest-image eligibility and allocation-free single-column access through
    one retained row read guard.
- `doradb-storage/src/table/dml_validator.rs`
  - callback-produced sparse-update validation reuse or narrow validation
    entry points as needed.
- `doradb-storage/src/table/mod.rs` and `doradb-storage/src/lib.rs`
  - private scan/cache types and public `LazyRow` export as appropriate.
- `doradb-storage/src/session.rs`
  - scoped maintenance lock admission for freeze and checkpoint calls.
- `doradb-storage/src/lock/mod.rs`
  - a narrow reusable scoped grouped table-lock helper if the existing helper
    does not express maintenance `Metadata(S)` plus `Data(IS)` cleanly; no
    compatibility or conversion-policy change.
- `doradb-storage/src/table/persistence.rs`
  - lock/workflow ordering and assertions that transition cannot overlap a
    granted full-update `X`.
- Inline table/transaction/checkpoint tests and, where clearer, supporting
  cases in `doradb-storage/src/table/tests.rs`.
- `docs/transaction-system.md`, `docs/checkpoint-and-recovery.md`, and
  `docs/rfcs/0016-logical-lock-manager.md`
  - maintenance lock mapping and full-update/checkpoint exclusion.

No persistent data format, WAL format, table-file format, recovery algorithm,
or index trait format is expected to change.

## Test Cases

Use deterministic barriers, listeners, or existing test hooks for concurrency
tests. Do not use sleeps as correctness synchronization and do not invent a
plain `cargo test` timeout flag.

1. **Public callback and buffer**
   - update all rows and a conditional subset using arbitrary column access;
   - access columns out of order and repeatedly, verifying one lazy load per
     accessed column through test instrumentation;
   - exercise computed updates based on multiple columns;
   - verify empty table, no-match, and `Some(Vec::new())` count semantics;
   - return a callback error and request an invalid column after earlier rows
     changed, verifying full statement rollback;
   - return invalid, unordered, duplicate, out-of-range, or type-mismatched
     `UpdateCol` values after earlier rows changed and verify validation plus
     rollback.
2. **Hot physical strategies**
   - verify a committed head newer than the transaction snapshot supplies the
     latest page value rather than an undo-reconstructed value;
   - allow a latest read through an active head owned by the same transaction,
     and return `WriteConflict` for another active owner before callback entry;
   - verify repeated lazy column access does not reacquire the row latch or
     allocate a `Vec<Val>` projection;
   - update an active fixed/small row in place and verify stable `RowID`;
   - grow a row beyond remaining space and verify move-update, new `RowID`, and
     complete unchanged-column preservation;
   - update an already-frozen page and verify move-update;
   - verify a successful direct update performs no unique lookup or ordinary
     row-location lookup in the common path.
3. **Cold and mixed storage**
   - update cold-only and mixed cold/hot tables with the same callback;
   - verify an older transaction skips a newer committed cold deletion and
     observes a newer committed cold update only through its hot replacement;
   - verify cold delete markers, replacement inserts, cached accessed values,
     unread-column materialization, nulls, and variable-length values;
   - verify old snapshots retain the cold image while the updating transaction
     reads its replacement according to current MVCC rules;
   - cover multiple selected rows in one LWC block and multiple blocks.
4. **Original scan boundary / Halloween prevention**
   - make a cold replacement insert into an unscanned original partial page and
     verify the first inserted `RowID` becomes its exclusive boundary;
   - insert multiple replacements into that future page and preserve the
     earliest boundary;
   - move a hot row into an unscanned original page and exclude it;
   - insert into the current page after its fixed end and into an already
     scanned page, verifying no sparse boundary is required and no revisit
     occurs;
   - allocate a page at/above the block-index upper bound and verify it is not
     captured, tracked, or scanned;
   - use an incrementing callback/update so any duplicate visit is observable,
     and verify the matched count equals the original eligible set.
5. **Checkpoint and locking**
   - hold checkpoint/freeze `IS` with a barrier and verify a fresh full-update
     `X` waits without invoking its callback;
   - hold full-update `X` after method success and verify freeze/checkpoint wait
     until transaction commit or rollback, not merely statement completion;
   - verify ordinary `IX` DML and explicit `S` table readers remain compatible
     with maintenance `IS`;
   - verify DDL metadata `X` remains mutually exclusive with maintenance
     metadata `S`;
   - verify no page can freeze or enter transition while full-update `X` is
     granted;
   - verify delayed, cancelled, failed, and successful checkpoint/freeze calls
     release fresh maintenance locks;
   - verify an existing `IX` converts immediately when possible and returns
     `LockUpgradeWouldBlock` without callbacks/mutations when conversion would
     wait.
6. **Indexes and constraints**
   - update indexed and unindexed columns with unique and non-unique indexes;
   - perform valid unique-key changes across cold, hot-in-place, and hot-move
     rows;
   - trigger duplicate-key failure on an early, middle, and late row and verify
     every row plus every affected index returns to pre-statement state;
   - verify a two-row unique-key swap fails under immediate uniqueness and
     rolls the statement back.
7. **Rollback and recovery**
   - fail after a mixture of cold delete/insert, hot in-place, and hot move
     updates and verify statement rollback;
   - explicitly roll back a transaction after a successful full update and
     verify rows, CDB state, and indexes;
   - commit a mixed full update, restart storage, and verify recovered rows,
     visibility, unique ownership, and index lookups using existing redo;
   - verify a callback is never invoked twice for one original row even when a
     later mutation fails.
8. **MVCC readers and lifecycle**
   - run ordinary table/index MVCC readers while full-update `X` is held and
     verify they remain admitted through metadata-only read locking and observe
     the correct snapshot;
   - verify table drop or metadata change cannot overlap the operation;
   - verify engine/table poison or shutdown propagates through existing
     foreground and maintenance admission rules.

Run the focused storage tests during development, then validate with:

```bash
cargo build --workspace
cargo nextest run --workspace
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

## Open Questions

There are no unresolved implementation choices for this task.

Blocking lock conversion and final-state unique-key permutations remain
explicit future design topics. During task resolve, create a backlog item only
for concrete, actionable follow-up supported by implementation or profiling
evidence, and deduplicate it against existing backlog documents first.
