---
id: 000265
title: Add index-driven MVCC mutation API
status: implemented  # proposal | implemented | superseded
created: 2026-08-09
github_issue: 965
---

# Task: Add index-driven MVCC mutation API

## Summary

Added `Statement::table_index_mutate_mvcc`, an index-range-driven counterpart
to full-table mutation. The operation merges candidates from the selected
secondary index's mutable `MemIndex` and a captured immutable `DiskTree` root,
revalidates each candidate as a latest current row, and invokes the existing
`LazyRow` callback for `Skip`, `Delete`, or sparse `Update` decisions.

The implementation uses transaction-lifetime `TableData(IX)`, so disjoint row
writers may proceed concurrently. It keeps candidate memory bounded by one
leaf batch per source and never retains a mutable index cursor or leaf latch
across callback execution. Each callback runs synchronously while definitive
hot-row or cold-row ownership is held and is never retried.

Transaction-local statement numbers on foreground row undo prevent a statement
from processing its own replacement rows. Unique driver updates must preserve
the driver's encoded logical key; non-unique drivers support key and RowID
changes through exact `(key, RowID)` candidate identity.

Review also corrected cold-row visibility: one-descent row resolution now
returns durable delete membership, while all consumers treat a surviving
in-memory deletion marker as newer authority.

## Context

`table_mutate_mvcc` already supplied the callback, outcome, undo, redo, and
rollback model, but it acquires `TableData(X)` and scans the complete captured
cold and hot worklist. Query execution needed a narrow operation when a
secondary-index range had already selected the relevant rows.

A secondary index combines a mutable B-tree-backed `MemIndex` with immutable
published `DiskTree` roots. Holding a mutable cursor while a callback changes
the selected index can retain parent coupling state, self-block structural
changes, or invalidate traversal. Rebuilding the complete composite stream per
row loses buffered candidates and repeats root seeks; materializing the whole
range makes memory proportional to result cardinality.

RowID movement also matters. A hot move or cold-to-hot update changes every
live secondary-index mapping even when indexed values are unchanged. The
latest row page contains no historical copy that can identify a replacement,
so the existing undo head is the authoritative place to tag the statement that
created the current image.

The callback observes a latest current image, not a statement-start snapshot.
Acquiring row ownership before the callback closes the read-to-write race:
another transaction cannot change the offered row between callback evaluation
and the physical mutation. A provisional no-op lock is removed immediately for
`Skip` and empty update.

Unique indexes require a narrower capability. Their MemIndex and DiskTree
entries merge by logical key alone. Allowing an earlier callback to install an
unread unique driver key could shadow the original DiskTree candidate before
statement tagging sees it. The shipped API therefore rejects actual encoded
driver-key changes.

Issue Labels:

- type:task
- priority:medium
- codex

## Goals

- Provide one public range-driven mutation API for active unique and
  non-unique secondary indexes.
- Admit the operation with `IndexWrite` plus transaction-lifetime
  `TableData(IX)`.
- Keep traversal memory bounded and release mutable cursor/leaf latches before
  row callbacks and mutations.
- Revalidate latest row identity and the exact selected-index key before every
  callback.
- Hold definitive hot or cold row ownership through callback and physical row
  decision, without retrying callback code.
- Exclude rows produced by the current statement while retaining read-your-own
  writes across later statements.
- Maintain statement-local rollback for callback, validation, ownership,
  uniqueness, index, row, and storage errors.
- Preserve correct cold-row visibility when durable deletion metadata and a
  newer in-memory deletion marker coexist.

## Non-Goals

- Changing the `TableData(X)` and original-worklist semantics of
  `table_mutate_mvcc`.
- Supporting unique-driver updates that change the encoded logical key.
- Providing a fixed statement-start candidate snapshot or materializing the
  complete range.
- Adding predicate, gap, next-key, or pessimistic range locks.
- Retrying, parallelizing, vectorizing, making the callback async, or adding
  early termination and caller-configurable mutation batch sizes.
- Changing `RowMutation` or `TableMutationOutcome` representations.
- Deferring unique checks until statement completion.
- Persisting statement numbers or changing undo, redo, table-file, checkpoint,
  or recovery formats.
- Reversing external side effects performed by callback code.

## Plan

### Public operation and admission

`Statement::table_index_mutate_mvcc` accepts a table, driver index, logical
range, and synchronous mutation callback. `TableAdmissionRequest::IndexWrite`
validates that the named index remains active in the bound table runtime; it is
index-targeted write admission, not exclusive permission to maintain indexes.
The statement validates range bounds when DML validation is enabled, acquires
`TableData(IX)`, captures one `TableRootSnapshot`, and binds traversal and old
index ownership proofs to that snapshot.

Outcome behavior matches full-table mutation: `Skip` changes no count,
`Delete` increments `delete_count`, and every `Update` increments
`update_count`. An empty update counts but immediately releases provisional
ownership and leaves no row, index, undo, or redo effect.

### Bounded dual-tree traversal

`BorrowedIndexMutationStream` borrows the operation's secondary-index runtime,
pool guards, encoded range, and proof-bearing root snapshot. It owns only
candidate buffers, traversal state, and the mutable-source resume key.

The mutable source creates a fresh one-leaf stream when its buffer is empty and
resumes after the last consumed exact encoded key. Its cursor is dropped before
the batch is returned. The immutable DiskTree source keeps one incremental
cursor over the captured root. `SecondaryIndexCandidateStream` merges both
buffers in encoded-key order, lets MemIndex win exact equality, and switches to
a source-specific state after the other source is exhausted. It stops a merged
batch whenever either non-exhausted source empties so a retained larger key is
not emitted before the other source's next leaf.

Caller-driven public index scans remain resource-owning through
`OwnedIndexCandidateStream`; transaction lifetime authority resides on the
enclosing `IndexScanMvccStream`, not on a phantom lifetime inside the private
owned cursor. Borrowed encoder access supports operation-local streams, while
`key_encoder_arc` remains available for caller-owned stream state.

### Candidate identity and row ownership

`BoundIndexCandidate` groups driver metadata, encoded key, RowID, and a borrowed
encoder. Unique revalidation compares the logical key; non-unique revalidation
compares the encoded `(key, RowID)` identity.

`IndexMutator` retains the table accessor, transaction runtime, statement
effects, root snapshot, and validator once per operation. It resolves each
candidate to `RowPage`, `LwcBlock`, or `NotFound`, and retries physical routing
only before callback execution when transition or prepare completion requires
authoritative re-resolution.

For hot rows, `HotRowMutator::lock_index_candidate_for_write` classifies
foreign ownership before interpreting mutable delete/key state, rejects stale
keys, excludes current-statement images, and installs a provisional
statement-tagged `Lock` under the row-version write latch and page-state read
guard. For cold rows, the immutable block and exact key are validated before a
deletion-buffer claim and provisional cold `Lock` undo are installed.

`Skip` and empty update synchronously unlink the newest provisional undo; cold
cancellation also removes only the marker owned by the current transaction.
Delete and non-empty update convert the same provisional ownership into the
existing delete, in-place update, move, or cold-to-hot protocols. Hot delete
constructs its complete old-index proof and releases the page guard before
awaiting secondary-index masking.

### Statement identity and unique-driver restriction

Every checked-out public, private, and public-stream statement receives one
monotonically increasing transaction-local `StmtNo`. Read-only and failed
statements consume numbers; transaction reset restarts the counter. Foreground
hot and cold row undo entries carry the current statement number, while purge,
recovery, and tests use the non-foreground sentinel. No durable record changed.

A candidate is a self-produced image only when its undo head belongs to the
same transaction and its main entry has the current statement number. Such a
candidate is skipped before callback execution. The same row remains eligible
in a later statement of the transaction.

For a unique driver update, the prospective key is assembled under row
ownership and encoded with the admitted index encoder. A different encoded key
returns `InvalidDmlInput`, even when ordinary DML validation is disabled.
Same-value assignments and RowID-only move updates remain valid; changes to
other indexes retain immediate maintenance and normal uniqueness checks.

### Durable cold-delete resolution

`ColumnBlockIndex::locate_and_resolve_row` performs one tree descent to return
the block, row ordinal, row-shape fingerprint, and durable delete membership.
`LwcRowLocation` carries those facts through the shared row-location path, so
single-row lookup and mutation do not revisit the column index to load delete
deltas.

The durable bit means the delete is committed in persisted base state; it does
not by itself mean globally visible to every transaction. A surviving
`ColumnDeletionBuffer` marker retains timestamp and ownership information and
must be interpreted first. Only when no marker exists may durable membership
decide visibility, writability, duplicate ownership, or index purge. This rule
is shared by point lookup/mutation, unique and non-unique index lookup,
index-driven mutation, and secondary-index GC.

## Implementation Notes

Implemented the public API, bounded dual-tree scanner, pre-callback ownership,
statement-local Halloween exclusion, unique-driver capability check, and
documentation without changing persistent formats or existing full-table
mutation semantics.

The implementation was refined during review:

- Mutation traversal moved into `index/borrowed_stream.rs`; only the enclosing
  public index stream retains the transaction lifetime marker.
- The common MemIndex/DiskTree candidate merger gained explicit `Both`,
  `MemOnly`, `DiskOnly`, and `Done` states, avoiding repeated polling after one
  source is exhausted.
- Candidate identity was consolidated into `BoundIndexCandidate`, and table
  mutation orchestration was separated into statement-scoped `IndexMutator`
  hot and cold paths.
- Hot ownership validation was corrected to treat a foreign uncommitted delete
  or key change as conflict/prepare state before stale-candidate checks. The
  shared point mutation path received the same correction.
- Cold-row review found that durable delete deltas were omitted from several
  single-row paths and sometimes given incorrect priority over the in-memory
  marker. One-descent row resolution now carries `durable_deleted`, and all
  lookup, mutation, duplicate, and GC consumers use CDB-first precedence.
- Hot index-driven delete now releases its row-page guard after row deletion
  and proof construction but before asynchronous MemIndex masking.
- The thin `UserTableAccessor::resolve_row_location` delegation remains by
  explicit acceptance; removing it was not required for correctness or
  performance because `Table::find_row` still performs the unified descent.

Deferred unique-driver key changes are recorded in
`docs/backlogs/000183-index-mutation-unique-driver-key-changes.md`. Pessimistic
range locking was intentionally left without a backlog because it requires a
larger transaction/index-locking architecture decision.

Final verification completed during resolution:

- branch-diff style audit passed for 28 Rust files, including formatting,
  workspace clippy with warnings denied, and repository structural rules;
- `rtk cargo nextest run --workspace` passed 1739 tests; and
- alternate `libaio` validation passed 1629 tests.

## Impacts

- Adds the public `Statement::table_index_mutate_mvcc` API and `IndexWrite`
  admission use for index-addressed writes.
- Adds runtime-only transaction statement numbering and row-undo tags.
- Adds a borrowed mutation stream while clarifying the authority boundary of
  resource-owning public index streams.
- Refactors shared secondary-index candidate merging and hot/cold candidate
  ownership paths.
- Extends row-location results with cold block ordinal, row-shape fingerprint,
  and durable deletion state.
- Applies CDB-first durable-delete interpretation across table access and index
  GC.
- Adds no catalog schema, row layout, durable undo/redo, table-file,
  checkpoint, recovery, or compatibility change.

## Test Cases

- Unique and non-unique driver ranges over MemIndex-only, DiskTree-only, and
  mixed candidates, including exact-key equality and skewed source buffers.
- Mixed hot/cold `Skip`, `Delete`, empty update, in-place update, hot move, and
  cold-to-hot update with correct outcomes and index mappings.
- Non-unique forward moves and current-statement replacements are not offered
  twice; the same row is eligible in a later statement.
- Unique same-key updates and RowID moves succeed, while encoded driver-key
  changes fail and roll back all prior statement effects.
- Hot and cold no-op decisions release provisional ownership immediately.
- Foreign active hot delete/key-change owners conflict before callback;
  preparing owners are awaited and authoritatively re-resolved.
- Stale selected-index entries and same-transaction old keys cannot mutate the
  latest row.
- Callback, validation, uniqueness, row, index, and storage failures retain
  statement rollback behavior.
- Dense, sparse, inline-delete, and external-delete column-index resolution
  returns correct row ordinals and durable delete membership.
- CDB marker timestamp/ownership takes priority over durable deletion for
  latest reads, claims, point lookup/mutation, index lookup/mutation, and GC.
- Hot delete masks every index only after releasing the page latch and remains
  absent from subsequent unique lookup.
- Statement numbers are monotonic across successful, read-only, and failed
  statements and reset for the next transaction.
- Existing full-table mutation, point DML, checkpoint/transition, rollback,
  recovery, and secondary-index cleanup coverage remains green.

## Open Questions

- Unique-driver key-changing updates require candidate stabilization that
  cannot shadow unread DiskTree owners. Follow-up:
  `docs/backlogs/000183-index-mutation-unique-driver-key-changes.md`.
- Pessimistic range locking remains outside this task. It would require a broad
  architectural design spanning transaction isolation, lock identity,
  MemIndex/DiskTree traversal, phantom handling, and deadlock behavior; no
  backlog was created during resolution.
