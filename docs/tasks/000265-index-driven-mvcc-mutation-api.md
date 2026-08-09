---
id: 000265
title: Add index-driven MVCC mutation API
status: proposal  # proposal | implemented | superseded
created: 2026-08-09
github_issue: 965
---

# Task: Add index-driven MVCC mutation API

## Summary

Add `Statement::table_index_mutate_mvcc`, an index-range-driven counterpart to
the full-table `table_mutate_mvcc` API. The operation incrementally merges the
selected secondary index's MemTree and captured DiskTree candidates, resolves
each candidate as a latest current read, and lets the existing `LazyRow`
callback choose `Skip`, `Delete`, or a sparse `Update`. It returns the existing
`TableMutationOutcome` and reuses the established hot, cold, index, undo, redo,
rollback, checkpoint, and recovery mutation protocols.

Run the operation under transaction-lifetime `TableData(IX)` so disjoint row
writers can proceed concurrently. Do not retain a mutable MemTree cursor or
index latch across callback execution or row mutation. Instead, buffer at most
one accepted leaf from each index source, restart only the mutable MemTree from
an exact source-local resume key, and advance the immutable captured DiskTree
cursor incrementally. Revalidate the latest row and selected index key before
the callback, acquire definitive hot-row or cold-row ownership, and invoke the
synchronous callback while that ownership is held. `Skip` and an
empty update immediately release the provisional ownership; delete and
non-empty update reuse it for the physical mutation. Foreign ownership is
resolved before callback execution, and user callback code is never retried.

Prevent Halloween reprocessing by assigning a cheap transaction-local
`StmtNo` to every statement, including read-only statements, and tagging every
new in-memory `RowUndo`. A current row whose undo head belongs to the same
transaction and carries the current statement number is a self-produced row
and is skipped. Support both unique and non-unique driver indexes, but require
an `Update` driven by a unique index to preserve the driver's encoded logical
key. `Skip`, `Delete`, non-key updates, same-value key assignments, and
RowID-changing move updates remain supported. Defer unique-driver key-changing
updates because a new MemTree key could otherwise shadow an unread DiskTree
candidate.

## Context

`Statement::table_mutate_mvcc` already provides the desired callback and
mutation result model, but it acquires `TableData(X)` and visits the captured
full-table cold and hot regions. This gives an original-row worklist and makes
replacement exclusion straightforward, but it scans the whole table even
when a computation engine has already selected a narrow secondary-index
range. The new API must preserve the full-table API rather than changing its
locking, ordering, or original-row semantics.

Doradb secondary indexes merge a mutable MemTree with a persisted DiskTree.
`IndexScanStream::next_batch` copies one non-empty leaf's projected candidates
before returning, but the current MemTree cursor keeps parent coupling state
between batches. Retaining that cursor while the callback updates the selected
index can self-block an insertion that must take an exclusive parent latch or
can invalidate traversal state. Dropping and rebuilding the entire composite
stream after each row avoids the latch hazard but loses unconsumed candidates
and turns a narrow scan into per-row root seeking. Fully materializing all
candidates avoids self-modification entirely but makes memory proportional to
the range and eventually requires a spill-file protocol.

Doradb also cannot rely on the InnoDB shortcut that a secondary-index entry
remains unchanged when its logical key is unchanged. A hot move update or
cold-to-hot update changes `RowID`; every live secondary-index mapping must be
refreshed even when its indexed values are unchanged. The row page contains
only the latest physical image, while older versions live in the in-memory
`RowVersionMap` undo chain. Every successful row ownership transition already
installs a new undo entry at the chain head, so a transaction-and-statement tag
on that entry is the natural way to recognize replacements created by the
current statement. This changes no persisted undo, redo, table-file, or
recovery format.

The callback is synchronous and expected to finish quickly, so the new
operation acquires row ownership before exposing the latest image. A hot row
uses the existing provisional `RowUndoKind::Lock` at the undo head while a cold
row uses its deletion-buffer marker plus a provisional cold-row undo entry.
Holding that ownership through the callback removes the read-to-write race and
guarantees that the callback decision is applied only to the image it observed.
A skipped row or empty update cancels the provisional entry immediately rather
than retaining a no-op lock until statement or transaction completion.

A unique driver needs one additional restriction. Unique MemTree and DiskTree
entries compare by logical key alone, and the merge chooses the MemTree entry
when exact encoded keys are equal. If an earlier callback changed another row
to an unread unique key, the new MemTree entry could discard the original
DiskTree candidate before statement tagging gets a chance to skip the new
row. Requiring the callback's resulting driver key to equal the revalidated
current key removes that case: a mutation-created unique entry can occur only
at the key already selected, while any RowID replacement at that key is
self-produced and can be skipped by `StmtNo`.

The range is intentionally a weak monotonic current-read traversal, not a
statement-start candidate snapshot. Each source resumes strictly after its
last consumed exact key. Concurrent or self-produced entries behind a source
resume point may be missed, entries ahead may be encountered, stale buffered
candidates are discarded by latest-key revalidation, and colliding row writes
return `WriteConflict`. No predicate, next-key, gap, or range locks are added.
If Doradb later introduces pessimistic range mutation, this contract and the
scanner restart policy must be revisited together.

`TableAdmissionRequest::IndexWrite` means an index-targeted table write: it
combines ordinary current-schema write admission with unconditional validation
that the caller-supplied driver `index_no` names an active index in the bound
runtime. It is not special permission to maintain secondary indexes. Ordinary
`TableWrite` operations such as insert and full-table mutation may also update
every secondary index; they simply do not name one index as their addressing
or traversal input.

Relevant references:

- `docs/transaction-system.md`
- `docs/index-design.md`
- `docs/checkpoint-and-recovery.md`
- `docs/process/coding-guidance.md`
- `docs/process/unit-test.md`
- `docs/tasks/000215-bounded-index-row-id-stream.md`
- `docs/tasks/000216-enhance-public-index-scan-stream-api.md`
- `docs/tasks/000233-unify-full-table-mvcc-mutation-api.md`

Issue Labels:

- type:task
- priority:medium
- codex

## Goals

1. Add one public `Statement::table_index_mutate_mvcc` API that selects latest
   mutation candidates through a secondary-index logical-key range and reuses
   `LazyRow`, `RowMutation`, and `TableMutationOutcome`.
2. Support active unique and non-unique secondary indexes through one public
   API and one common candidate/mutation control flow.
3. Permit `Skip` and `Delete` for either driver kind and sparse `Update` for a
   unique driver only when the resulting encoded driver key is unchanged.
4. Preserve all selected-index mappings across in-place, hot move, and
   cold-to-hot updates, including RowID-only changes to a unique driver.
5. Acquire transaction-lifetime `TableData(IX)` so concurrent disjoint row
   mutations can succeed and same-row ownership collisions use the existing
   `WriteConflict` behavior.
6. Define a bounded dual-tree batch boundary that retains no mutable MemTree
   cursor or latch across callback execution or mutation.
7. Keep candidate memory bounded by one accepted MemTree leaf plus one accepted
   DiskTree leaf, independent of total range cardinality.
8. Restart the MemTree only when its owned source buffer is depleted, retain
   the other source's unconsumed candidates, and advance the immutable
   DiskTree without repeated root seeking.
9. Assign one monotonically increasing transaction-local `StmtNo` to every
   checked-out statement and store it in every new foreground `RowUndo`.
10. Skip a candidate before callback execution only when its current undo head
    belongs to the same transaction and its head entry has the current
    statement number.
11. Revalidate the latest row and driver key, acquire definitive row ownership
    before the callback, retain it through the callback and selected action,
    and never invoke the callback twice for one candidate because of a wait or
    race.
12. Preserve statement-local effect ordering and rollback for callback,
    validation, ownership, unique-constraint, index, row, runtime, and storage
    failures.
13. Document the weak monotonic current-read range contract and deterministic
    structural performance expectations.

## Non-Goals

- Changing, replacing, or weakening the transaction-lifetime `TableData(X)`
  contract and original-row worklist of `table_mutate_mvcc`.
- Allowing an update driven by a unique index to change that driver's encoded
  logical key.
- Capturing a fixed statement-start candidate set, fully materializing the
  selected range, or adding a temporary spill-file/drain-to-file protocol.
- Predicate locks, gap locks, next-key locks, pessimistic range locking, or a
  new serializable range-mutation isolation level.
- Promising that rows inserted or moved concurrently into the range are always
  included, or that rows moved out after a resume point are always observed.
- Retrying a user callback to resolve any ownership, route, validation, or
  mutation failure.
- Async, vectorized, parallel, early-terminating, or caller-batched mutation
  callbacks.
- A caller-configurable leaf or mutation batch size.
- Changing `RowMutation` variants or `TableMutationOutcome` fields and count
  semantics.
- Deferring uniqueness checks until a final statement result. All indexes,
  including unique indexes other than the driver, continue to be maintained
  and checked in physical action order.
- Persisting `StmtNo`, changing durable undo or redo records, or changing
  checkpoint/recovery file formats.
- A new benchmark command or timing-based unit-test threshold. Performance is
  guarded through structural refill/seek assertions in deterministic tests.
- Undoing external side effects performed by callback code.

## Plan

### 1. Add the unified public statement API

Add the following additive method beside `table_mutate_mvcc` in
`doradb-storage/src/trx/stmt.rs`:

```rust
pub async fn table_index_mutate_mvcc<'r, R, F>(
    &mut self,
    table_id: TableID,
    index_no: usize,
    range: R,
    mutate_row: F,
) -> Result<TableMutationOutcome>
where
    R: RangeBounds<&'r [Val]>,
    F: for<'row> FnMut(&mut LazyRow<'row>) -> Result<RowMutation>;
```

Use operation name `table_index_mutate_mvcc` in public documentation and all
error attachments. Preserve the established admission boundary:

1. admit the live user table with `TableAdmissionRequest::IndexWrite`;
2. retain the transaction's metadata binding and verify `index_no` names an
   active secondary index;
3. validate both logical range bounds against the selected index when DML
   validation is enabled;
4. acquire transaction-lifetime `TableData(IX)` through the existing table
   write-data lock helper;
5. capture one `TableRootSnapshot` and bind all DiskTree, pivot, row-route, and
   index-maintenance proofs for the operation to it; and
6. enter the accessor mutation loop with the current `StmtNo`.

Here `IndexWrite` is the existing composite admission request for a table write
whose addressing input names an index. Its index-membership and active-runtime
checks are structural and remain mandatory when ordinary DML validation is
disabled. It does not imply that `TableWrite` operations cannot modify indexes.

Refactor `DmlValidator::validate_index_scan` as needed so range validation is
reusable without requiring a projection read set. Sparse callback updates use
the existing validation policy. `disable_dml_validation()` may bypass ordinary
range and sparse-update shape/type checks under its documented caller
preconditions, but it must not bypass the unique-driver unchanged-key rule.

Retain existing outcome semantics:

- `Skip` increments neither count;
- `Delete` increments `delete_count` once;
- `Update` increments `update_count` once;
- `Update(Vec::new())` counts as an update, releases its provisional row
  ownership, and leaves no row, index, undo, or redo effects; and
- failure returns no outcome and ordinary propagation from `Transaction::exec`
  rolls back all statement-local effects.

Generalize the public documentation on `LazyRow`, `RowMutation`, and
`TableMutationOutcome` where it currently describes only full-table mutation.
Do not change their public representations or exports.

### 2. Restore transaction-local statement numbering and tag row undo

Define an internal `StmtNo` integer type in the transaction subsystem rather
than the logical-lock subsystem. Add a checked monotonically increasing counter
to `TrxInner`; initialize and reset it with the transaction. Allocate exactly
one number after successful operation checkout for each:

- public `Transaction::exec` statement;
- private `stage_statement`; and
- public stream statement when `table_index_scan_mvcc` checks out the
  transaction.

Read-only statements consume numbers. Validation and ordinary statement
failures consume numbers. Rollback never decrements or reuses a number. A
future dropped before its first poll and therefore before checkout allocates
nothing. Every operation executed through the same public `Transaction::exec`
shares that statement's number; high-level engines that need distinct SQL
statement identity must use distinct `exec` boundaries.

Store the allocated number in `StmtEffects` and add `stmt_no: StmtNo` to
`RowUndo`. Make foreground `OwnedRowUndo` construction take the statement
number from `StmtEffects` or an equivalently narrow statement context. Tag hot
locks/inserts/updates/deletes and cold deletion-buffer undo uniformly. Update
test, purge, and synthetic constructors to use an explicit non-foreground
sentinel or fixture statement number; do not infer statement identity from a
transaction timestamp. Add a debug assertion when pushing foreground row undo
that its tag matches the owning `StmtEffects`.

Rollback already removes the newest undo entry and restores its older main
branch. Preserve that behavior so rolling back a current-statement entry also
restores the previous statement's tag naturally. No field is added to
`SharedTrxStatus`, and no statement number enters redo, recovery, or any durable
format.

Add a narrow latest-row predicate that returns true only when both conditions
hold:

```text
current undo-head owner is this transaction
AND current main undo entry stmt_no is this statement
```

Do not compare statement numbers across transactions. Use this predicate only
for Halloween exclusion before invoking the index-mutation callback; ordinary
read-your-own-write and later-statement mutation behavior remains unchanged.

### 3. Add an operation-local borrowed bounded dual-tree stream

Add `index/borrowed_stream.rs` with a mutation-specific
`BorrowedIndexMutationStream`. Compose the existing
`SecondaryIndexCandidateStream` merger with mutation-specific borrowed source
adapters. Reuse its candidate projection, exact encoded-key comparison, and
MemTree-wins equality rule, but keep the two source lifecycles distinct and do
not retain a mutable MemTree cursor across row mutation.

Encode the caller range with `BTreeKeyEncoder::encode_range` for a unique
driver and `encode_non_unique_range` for a non-unique driver. The exact source
key is therefore the logical key for unique indexes and `(logical key, RowID)`
for non-unique indexes. Retain the original encoded upper bound for the entire
operation.

The stream borrows this operation-local state:

- selected index runtime and pool guards;
- encoded original range; and
- the proof-bearing `TableRootSnapshot` from which the captured DiskTree root
  is selected.

The stream owns this resumable state:

- copied `mem_buf`, last consumed MemTree exact key, and MemTree exhaustion
  state;
- one incremental DiskTree stream/cursor containing the captured root id,
  `disk_buf`, and DiskTree exhaustion state; and
- optional test-only source refill and root-seek counters.

Refill an empty, non-exhausted MemTree source by creating a fresh one-shot
stream whose lower bound is the stricter of the original lower bound and
`Excluded(last_consumed_mem_key)`. Ask it for one non-empty leaf-bounded
projected batch, copy the owned candidates, and immediately drop its cursor and
all leaf/parent latches. Mark it exhausted when the fresh scan returns no
candidate. Later self or concurrent insertions after that observation need not
reopen an exhausted source under the documented weak range semantics.

Create the DiskTree stream once from the captured immutable root. Its cursor
state retains block IDs and traversal stack state, not a leaf block guard.
Advance it by one non-empty leaf-bounded projected batch whenever its owned
buffer is empty, and ensure the temporary leaf guard is gone before returning
candidates for mutation. Do not redo a root-to-leaf DiskTree seek per mutation
batch.

Merge current owned buffers in exact encoded-key order. When both fronts are
equal, emit the MemTree candidate and consume both source entries. Stop and
return the maximal globally ordered prefix as soon as either non-exhausted
source buffer becomes empty. Keep unconsumed entries from the other source
across mutation, then refill only the depleted source. Once one source is
globally exhausted, return at most the other source's current leaf buffer.

For example, given:

```text
MemTree:  [1, 100]
DiskTree: [2, 3]
```

return `[1, 2, 3]`, retain MemTree candidate `100`, mutate the returned batch,
and then advance only the DiskTree. Emitting `100` before loading the next
DiskTree leaf would violate global ordering because that leaf may begin at `4`.

At every return boundary, the output plus retained candidates must be bounded
by one accepted leaf from each source. A multi-entry fixture must perform one
MemTree root seek per MemTree refill rather than per candidate; retaining a
skewed source buffer must not refetch that source; and DiskTree traversal must
perform one initial seek followed by incremental leaf advancement. Express
these as deterministic counters or state assertions, not elapsed-time tests.

### 4. Own each latest candidate before invoking its callback

Add an accessor-level index mutation loop that drains each owned merged batch
sequentially. No MemTree cursor, mutable index latch, leaf guard, row block
guard, or row-page guard from one candidate may be retained while moving to a
different candidate. The immutable DiskTree cursor state and owned candidate
buffers may survive.

For each candidate:

1. resolve its current hot or cold row location using the captured root and
   existing row-location protocols;
2. establish the location-specific stable validation boundary: a page-state
   and row-version write guard for hot storage, or the immutable persisted
   image for cold storage;
3. skip without callback when a hot latest undo head has the current
   transaction and `StmtNo` tag;
4. classify existing row ownership before interpreting a mutable hot physical
   image: treat a foreign active owner as `WriteConflict`, or release
   operation-local state, wait for an ordered preparing owner, and restart
   authoritative candidate resolution;
5. verify the admitted latest physical row is not deleted and still encodes to
   the candidate's selected-index key;
6. for a non-unique candidate, include its `RowID` in the exact-key validation;
7. acquire and rollback-track definitive provisional ownership for the current
   statement before the callback;
8. invoke the synchronous callback while retaining that ownership and the
   authority needed to keep the offered row image and route stable; and
9. either cancel the provisional ownership for `Skip` or an empty update, or
   reuse it for the selected delete or non-empty update.

For a hot row, acquire `RowWriteAccess` with the page-state read guard held,
perform current-statement exclusion and foreign-owner admission before
latest-row and exact-key validation under its row-version write latch, and then
install a statement-tagged `RowUndoKind::Lock` before invoking the callback.
Expose an immutable `LazyRow` view borrowed from that locked latest image.
Retain the row access and page-state guard through the callback and the
immediate physical row decision, so another transaction cannot modify the row
and checkpoint cannot move the page into transition between observation and
action.

That ordering is correctness-critical because the physical delete bit and
latest key may have been written by a foreign uncommitted transaction. Such an
image is a row-ownership conflict or prepare wait, not a stale candidate to
silently discard. The shared hot-row lock helper therefore invokes its
caller-supplied latest-image validation only for an unowned, same-transaction,
or committed head, and installs no provisional undo when validation fails.
Ordinary keyed point update/delete uses the same helper, so a stale index entry
is revalidated even when the current undo head already belongs to the calling
transaction.

Add a narrow synchronous cancellation path for the newest provisional hot-row
lock. For `Skip` and `Update(Vec::new())`, assert that the last statement effect
is the matching `Lock`, unlink it from the row-version chain while its owned
allocation is still alive, restore the previous head/main branch, and only then
pop the owned entry from `StmtEffects`. A delete or non-empty update instead
rewrites that same provisional entry to its final `RowUndoKind` and continues
through the existing mutation protocol. A callback error or later propagated
failure leaves the provisional entry in statement effects so ordinary
statement rollback removes it with all earlier effects.

For a cold row, first validate the immutable persisted image and then claim its
deletion-buffer marker before callback execution. A foreign preparing owner is
awaited only after releasing the LWC block guard, followed by authoritative
candidate resolution before the callback. As soon as a claim succeeds, push a
statement-tagged provisional cold-row `Lock` undo so cancellation, panic, or
callback error cannot strand an untracked marker. Invoke the callback against
the validated immutable image while the marker is owned. `Skip` and an empty
update remove only this transaction's marker and then pop the provisional undo;
delete and non-empty update rewrite the entry to `Delete` and reuse the marker
for existing index masking or cold-to-hot replacement. Release the LWC block
guard before asynchronous insertion or index work.

`TableData(IX)` is compatible with checkpoint's `TableData(IS)`, so a hot page
may already be frozen or in transition when the candidate is resolved. Reuse
the point-DML route-wait and root-proof rules: settle a transition before
acquiring row ownership or invoking the callback. Once hot-row ownership is
acquired, retain the page-state read guard through callback and physical row
action so transition cannot begin in the middle. A frozen hot page may use the
established move-update path after the callback returns.

### 5. Enforce the limited unique-driver capability

Record whether the selected driver is unique and retain its `IndexSpec` and
encoder in the operation state. `Skip` and `Delete` are valid for either driver
kind. After a unique-driver callback returns a non-empty `Update`, while the
provisional row ownership is still held and before physically changing the
row:

1. load the current values of all driver key columns through `LazyRow`;
2. apply the valid sparse update values to an owned prospective key;
3. encode that key with the selected `BTreeKeyEncoder`; and
4. compare it with the encoded current key already revalidated for the
   candidate.

Allow an update that mentions a driver column but assigns its current value;
the capability is based on encoded resulting-key equality, not merely column
presence. Reject a different encoded key with
`OperationError::InvalidDmlInput` and attach operation, table, index, and row
context. Run this check even when normal DML validation is disabled. The
existing `disable_dml_validation` precondition still requires sparse updates
to be ordered, in range, and type compatible. Rejection performs no physical
row or index change; ordinary statement rollback removes the provisional lock
and all earlier statement effects.

An unchanged-key move update remains fully supported. Reuse the existing
unique-index RowID-only change path so the live mapping is replaced from the
old RowID to the new RowID and older snapshots retain their runtime undo
branches. If that MemTree replacement is later observed because the MemTree
source resume trails the globally consumed key, the new row's current undo
entry has this `StmtNo` and is skipped before callback execution.

This restriction proves that a mutation-created unique entry cannot shadow a
later different driver key: its only possible driver key is the one already
selected. Continue to allow changes to other unique or non-unique indexes.
Those indexes retain immediate maintenance, duplicate detection, branch
linking, deferred deletion, and rollback behavior.

### 6. Reuse known-row mutation and effect protocols

Refactor the existing full-table known hot/cold delete and update helpers only
as needed to share them with the new index-driven loop. Do not route each
candidate back through a second unique lookup.

For `Delete`, use the already-owned latest row to capture every current
secondary-index key, convert its provisional undo to `Delete`, install redo,
set the hot delete bit or retain the cold deletion marker, and mask/defer-delete
all index entries in the existing order. For `Update`, reuse the provisional
ownership while selecting the existing in-place, hot move, or cold-to-hot
path, preserve all unique index branches, and refresh every index whose key or
RowID changed. The captured `TableRootSnapshot` must be used for every old
DiskTree proof and index ownership decision in the operation.

The callback decision is applied before scanning further candidates. This is
not a final-state planning API: an update that conflicts with a unique owner
which a later callback might delete still fails in action order. On any
propagated error, statement rollback continues to unwind index effects before
row/deletion effects and discard statement redo. An empty update cancels its
provisional ownership immediately and therefore leaves no row undo or
`StmtNo` tag; it cannot create a new selected-index candidate and still counts
once in the outcome.

### 7. Document concurrency and range semantics

Document the following public contract on the API and in the transaction/index
design notes:

- the selected range is traversed in ascending encoded exact-key order subject
  to the weak monotonic two-source restart behavior;
- callback order is not a fixed statement-start snapshot and should not be
  used as a durable SQL ordering guarantee;
- every offered row is a latest current image whose selected key is rechecked;
- the same physical/logical row may be omitted after concurrent movement, but
  a self-produced current-statement row is never offered;
- disjoint IX writers can complete concurrently;
- foreign ownership of the same candidate is resolved before callback
  execution, and a non-preparing active owner is `WriteConflict`;
- the callback runs while definitive row ownership is held, is never retried,
  and cannot race a same-row modification between observation and action;
- `Skip` and an empty update release provisional ownership immediately, while
  delete and non-empty update retain normal transaction row ownership;
- there are no predicate protections against phantoms; and
- supporting pessimistic range mutation or unique-driver key changes requires
  a separate design decision rather than silently strengthening this API.

Update comments on the existing B-tree cursor and candidate merger only where
needed to distinguish caller-driven owned read streams from the operation-local
borrowed mutation latch boundary.

## Implementation Notes

## Impacts

- `doradb-storage/src/trx/mod.rs`
  - add the transaction-local `StmtNo` type, checked counter allocation, reset,
    and statement-boundary plumbing.
- `doradb-storage/src/trx/stmt.rs`
  - carry `StmtNo` in `StmtEffects`, add the public API, use `IndexWrite`
    index-targeted admission and `TableData(IX)`, add narrow cancellation of
    the newest provisional row lock, and preserve outcome/rollback behavior.
- `doradb-storage/src/trx/stream_stmt.rs`
  - allocate a statement number for checked-out read-only public streams.
- `doradb-storage/src/trx/undo/row.rs`
  - add the in-memory `stmt_no` field and explicit foreground/test constructors.
- `doradb-storage/src/trx/row.rs`
  - expose same-transaction/current-statement detection, classify foreign row
    ownership before caller-supplied latest-image validation, install hot-row
    ownership before callback execution, support an immutable view of the
    write-locked latest image, and synchronously unlink a cancelled newest
    provisional `Lock`.
- `doradb-storage/src/index/index_stream.rs`
  - reuse leaf-bounded projection while ensuring temporary leaf guards are
    released at mutation batch boundaries.
- `doradb-storage/src/index/secondary_index.rs`
  - host the shared unique/non-unique dual-tree merger and expose borrowed key
    encoders for operation-local use.
- `doradb-storage/src/index/borrowed_stream.rs`
  - provide the operation-local borrowed mutation sources, proof-bearing root
    snapshot reference, and source-local exact MemTree resume state.
- `doradb-storage/src/index/owned_stream.rs`
  - retain only the resource-owning persistent source state required by the
    caller-driven public index-scan stream.
- `doradb-storage/src/index/row_page_index.rs`
  - represent persisted row routes with a dedicated `LwcRowLocation` payload
    carried by `RowLocation::LwcBlock`.
- `doradb-storage/src/index/btree/key.rs`
  - reuse or narrowly extend exact encoded bound helpers for source-local
    `Excluded(resume)` scans.
- `doradb-storage/src/table/access.rs`
  - create the operation-scoped index mutator around the borrowed candidate
    stream and retain the general known-row mutation primitives it consumes.
- `doradb-storage/src/table/index_mutate.rs`
  - retain accessor, transaction runtime, statement effects, root snapshot,
    and validation context once per operation; separate authoritative route
    retry, hot ownership, owned-hot action, and cold candidate mutation; and
    keep index-mutation tests beside that orchestration.
- `doradb-storage/src/table/deletion_buffer.rs`
  - add a narrow ownership-checked release operation if the existing removal
    interface cannot safely cancel one provisional cold-row claim.
- `doradb-storage/src/table/dml_validator.rs`
  - separate reusable index-range validation from projection read-set
    validation; keep the unique-driver capability check unconditional in the
    mutation layer.
- `doradb-storage/src/table/hot.rs`
  - refactor known-row update/delete interfaces to consume or reuse an already
    write-locked hot row rather than acquiring row ownership after callback.
- `doradb-storage/src/row/ops.rs`
  - generalize existing mutation type documentation beyond full-table-only
    wording without changing public types.
- `docs/transaction-system.md`
  - document transaction-local statement numbers, undo tagging, IX mutation,
    pre-callback row ownership, prompt no-op release, current-read conflicts,
    and callback non-retry.
- `docs/index-design.md`
  - document the bounded mutation scanner, dual-tree batch boundary, weak range
    semantics, and unique-driver unchanged-key limitation.

No public type export, catalog schema, table metadata, row layout, redo record,
table-file, checkpoint image, or recovery format changes are expected.

## Test Cases

### Admission and input validation

- The API uses index-targeted write admission: an index absent from the
  transaction-visible schema is `IndexNotFound`, while a visible driver retired
  from the current layout or an otherwise stale write identity is
  `SchemaChanged` according to the existing admission error order.
- Driver-index membership and active-runtime validation remain enforced when
  ordinary DML validation is disabled; only logical range and sparse-update
  shape/type checks may be bypassed under existing caller preconditions.
- Existing `TableWrite` insert and full-table mutation behavior continues to
  maintain all affected secondary indexes, demonstrating that `IndexWrite`
  describes an index-targeted operation rather than exclusive index-write
  permission.

### Statement identity and undo tagging

- Public read/write `Transaction::exec`, private staged statements, and public
  index scan streams consume monotonically increasing numbers from one
  transaction-local counter.
- Read-only, validation-error, callback-error, and rolled-back statements do
  not reuse their numbers; transaction reset starts a fresh counter.
- All foreground hot and cold row undo entries carry their `StmtEffects`
  number, while test/purge fixtures use an explicit sentinel or fixture value.
- Statement rollback removes the newest tag and exposes the restored older
  statement tag.
- A row written earlier in the same `Transaction::exec` is excluded, while the
  same row is eligible in a later `exec` of the same transaction.
- Hot and cold `Skip` and empty-update paths unlink their newest provisional
  ownership and leave no undo tag, CDB marker, redo, or index effect.

### Dual-tree scanner and performance structure

- Unique and non-unique scans work for MemTree-only, DiskTree-only, and mixed
  ranges with inclusive, exclusive, and unbounded endpoints.
- Exact entries present in both trees emit the MemTree candidate once and
  consume both entries.
- The `[1, 100]` versus `[2, 3]` skew fixture returns `[1, 2, 3]`, retains
  `100`, and refills only DiskTree before continuing.
- The symmetric skew case retains DiskTree candidates while restarting only
  MemTree.
- Output plus leftovers never exceeds the accepted candidates from one leaf
  per source.
- MemTree root-seek counters increment once per source refill rather than once
  per row; an unconsumed source is not refetched.
- DiskTree performs one initial seek and incremental traversal rather than one
  root seek per mutation batch.
- A selected MemTree leaf/parent split caused by a callback update completes
  without self-deadlock, proving no mutable cursor latch crosses the mutation
  boundary.

### Non-unique driver behavior

- Skip, delete, empty update, in-place update, hot move, and cold-to-hot update
  produce correct rows, index entries, undo, redo, and outcome counts.
- Driver keys moving forward or backward do not cause a second callback for
  the replacement; unchanged-key RowID movement refreshes the exact
  `(key, RowID)` entry.
- A new exact entry for one row cannot shadow another row with the same logical
  key because their encoded RowID suffixes differ.
- Buffered stale keys and deleted/moved RowIDs are discarded by latest exact
  key revalidation.

### Unique driver behavior

- Skip, delete, empty update, and updates of only non-driver columns succeed.
- Single and composite driver columns assigned their current values are
  accepted based on equal encoded resulting keys.
- Any actual encoded driver-key change returns `InvalidDmlInput` before that
  row or its indexes are physically changed; statement rollback removes the
  provisional ownership and all earlier statement effects.
- The key-change rejection remains active when normal DML validation is
  disabled.
- In-place, forced hot move, and cold-to-hot updates with an unchanged driver
  key preserve the live unique mapping and older-version branch behavior.
- A RowID-only MemTree replacement rediscovered after a source-local restart is
  skipped through current transaction plus `StmtNo` and never invokes the
  callback twice.
- A regression fixture with an unread DiskTree key proves an earlier callback
  cannot install that key and shadow the unread owner.
- Updates may change a different unique index; duplicate-key failure and
  successful rollback retain existing behavior.

### Current-read concurrency and route changes

- Two transactions holding `TableData(IX)` mutate disjoint ranges/rows and can
  both commit.
- A foreign active owner whose uncommitted image is deleted or has a different
  selected key returns `WriteConflict` before the callback rather than being
  discarded as stale.
- An owner already preparing is awaited before interpreting its deleted/key
  image, and the candidate is re-resolved before at most one callback.
- After an in-place key change, a later point update/delete in the same
  transaction rejects an old-key index entry instead of mutating the latest row
  through that stale entry.
- A deterministic competitor attempting the same hot row after provisional
  ownership is installed but before the callback returns cannot change the row
  and receives the existing active-owner `WriteConflict`; the callback runs
  exactly once against the locked image.
- The equivalent cold-row fixture proves a provisional CDB marker prevents a
  same-row delete/update during callback execution.
- After a hot or cold `Skip` or empty update returns, another transaction can
  acquire that row, proving provisional ownership was released immediately.
- Hot-page freeze/transition and checkpoint publication races settle before
  callback ownership, or are held outside transition by the retained
  page-state guard through callback and physical row action.
- Self-created forward entries that are encountered are skipped; entries
  behind a consumed source resume need not be revisited under the documented
  weak range contract.

### Errors, effects, and verification

- Callback error, invalid sparse update, unique-driver key change, same-row
  conflict, duplicate key on another unique index, forced index failure, and
  forced row/storage failure roll back all propagated statement effects,
  including any hot `Lock` or cold CDB ownership acquired before callback.
- Delete and update counts are independent; skipped and stale candidates do
  not count; empty updates count without creating effects.
- Existing full-table and point mutation tests continue to pass with their
  original lock and scan semantics.
- Run `rtk cargo nextest run --workspace` as the authoritative workspace pass.
- Run `rtk cargo nextest run -p doradb-storage --no-default-features --features libaio`
  for the alternate storage I/O backend.

## Open Questions

None for this task. Supporting unique-driver key-changing updates remains a
future design problem and must first choose a candidate-stabilization mechanism
such as bounded materialization plus spill, a statement work file, or an
equivalent protocol that cannot shadow unread unique keys. Pessimistic range
locking would also require revisiting the weak monotonic traversal contract.
