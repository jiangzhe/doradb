---
id: 000299
title: Unify Unique-Key MVCC Mutation API
status: implemented
created: 2026-09-06
github_issue: 1053
---

# Task: Unify Unique-Key MVCC Mutation API

## Summary

Implemented `Transaction::table_unique_mutate_mvcc`, a programmable unique-point
write boundary. Its synchronous `FnOnce` callback receives the latest acquired
row as `Some(&mut LazyRow)`, or an observed missing entry as `None`, and chooses
`Skip`, missing-only `Insert`, occupied sparse `Update`, or occupied `Delete`.
The result distinguishes `Noop`, `Inserted(RowID)`, `Updated(RowID)`, and `Deleted`.
Physical replacement remains a logical update and returns its replacement RowID.

The callback API replaces the public unique upsert/update/delete methods and
uses specialized point selection with shared owned physical mutation. Point
lookup remains MemIndex-first, and key changes apply immediately. Zero-read
callbacks defer dense column-cache allocation.

## Context

Legacy point methods take values before row selection and cannot express an
owned-current-row computation such as `b = b + 1`. A preceding snapshot read is
not the same ownership boundary. Existing index-range mutation has suitable
ownership primitives but different traversal, cold-delete, empty-update, and
deferred driver-key semantics. Points therefore retain a separate executor.

This task intentionally removes the legacy public point APIs following user
review. It has no parent RFC, durable-format migration, or internal catalog/
MemTable API change. Internal full-row ownership from tasks 000202 and 000205
remains; point execution reuses owned-row mutation/cancellation from tasks
000265 and 000271.

Source Backlogs:

- `docs/backlogs/closed/000195-unify-unique-key-mvcc-mutation-api.md`

Issue Labels:

- type:task
- priority:medium
- codex

## Goals

- Provide one programmable current-write point boundary with at-most-once
  synchronous callback execution and both public index argument forms.
- Enforce occupied/missing action validity and selected-key insertion agreement.
- Preserve application error payloads and ordinary statement rollback behavior.
- Return the resulting physical RowID for every successful insert/update.
- Replace legacy public point methods and adapters with one callback executor,
  sharing existing owned hot/cold effects with range mutation.
- Defer dense callback scratch until a column is actually accessed.
- Deliver complete caller documentation, compiled examples, regression coverage,
  and temporary comparative latency/allocation validation.

## Non-Goals

- Introducing a public full-row replacement action, implicit occupied `Insert`,
  or a combined point/range interface.
- Changing range traversal or deferred unique-driver ordering.
- Adding gap locks, async/retried callbacks, insertion-race retry as update, or
  unique-key permutation planning.
- Changing durable records, catalog behavior, transaction cleanup ownership,
  wait families, storage backends, or test timeout configuration.
- Implementing a sparse/inline cache or claiming zero total mutation allocation.

## Rejected Alternatives

- A direct public request enum cannot compute from the latest acquired row.
- Equal-bound range execution would inherit unnecessary traversal state and
  different key-change, cold-conflict, missing, and empty-update behavior.
- Occupied insertion or implicit put hides the caller's entry-state policy and
  encourages construction of an unused insertion payload.

## Plan

### Public boundary and validation

`UniqueMutation` and `UniqueMutationOutcome` are exported from the crate root.
The transaction method delegates through exactly one ordinary statement runner.
The statement admits the index for writing, checks uniqueness, validates the
lookup key under the transaction's ordinary DML-validation policy, and acquires
transaction-lifetime `TableData(IX)` before ownership or callback execution.

On a miss, only skip and insertion are valid. On a hit, skip, sparse update, and
delete are valid. Invalid entry-state decisions always return `InvalidDmlInput`,
including missing empty updates. Inserted values must agree with the admitted
selected key even when ordinary payload validation is disabled. Full-row shape,
nullability/kinds, and sparse ordering/bounds/types use `DmlValidator` normally;
trusted-input opt-out retains the existing caller obligations.

Callbacks impose no `Send`, `Sync`, `Clone`, `'static`, or application-error trait
bounds. Borrowed row values cannot outlive the callback. Engine failures disclose
through `CallbackError::Engine`; application payloads remain `User(E)`.

### Specialized point execution

`table/unique_mutate.rs` owns the operation-scoped accessor, transaction/effects,
index, and validation context. Each attempt owns its root/index/page/block
resources. Selection uses `UniqueSecondaryIndex::lookup`, authoritative RowID
routing, and exact current-key revalidation. It constructs no range stream,
candidate batch, or deferred driver-update list.

Hot acquisition reuses `HotRowMutator::lock_for_write` and provisional undo
installation. Foreign ownership admission precedes mutable deletion/key
interpretation. The callback and conversion to physical action retain the same
write access and page-state guard. Prepare and transition retries happen before
the callback is invoked.

Cold point eligibility is separated from full-row decoding. CDB timestamp and
ownership information precedes durable delete membership. A matching committed
delete newer than the writer STS may conflict. A same-transaction consumed cold
image is missing. Definitive `claim_ref` acquisition is immediately recorded as
provisional statement-owned `Lock` undo before any callback or later fallible
work. Transaction serialization plus authoritative cold routing preserves fresh
claim provenance; a preliminary same-owner marker is never cancellable by a
new callback invocation.

A miss unwinds attempt-local resources before invoking the callback with `None`.
It takes no gap lock. A racing insertion can produce ordinary duplicate/conflict
errors without rerunning the callback or converting insertion to update.

### Owned action application and API removal

Engine-only physical helpers keep native errors. Shared hot updates convert the
same provisional undo through `update_owned_row`, retaining existing move,
index-proof, branch-link, and index-maintenance protocols. Shared cold updates
accept `RowUpdateInput`, install cold delete effects, and insert/link the owned
replacement. Logical result conversion occurs at the action boundary.

Hot deletion converts retained ownership, copies complete indexed keys while
guarded, and releases the page guard before asynchronous index masking. Cold
deletes decode only indexed columns. Sparse cold replacement materializes old
values only when needed, reusing callback-cached values when present and decoding
directly when the cache is unused.

The three legacy public methods, statement/accessor wrappers, request adapters,
and compatibility-only branches are removed. `UniquePointMutator` directly
accepts the callback and dispatches `UniqueMutation`. Internal update/delete/
upsert result types remain crate-private for catalog and MemTable consumers.
User-table callers and test helpers use `UniqueMutationOutcome`; update/delete
callers skip a missing entry, and upsert callers select insertion or ordered
sparse assignments. There is no deprecation wrapper or public replacement action.

Skip cancels only this invocation's provisional ownership. Empty callback updates
do the same and return `Updated(original_row_id)`. Nonempty same-value assignments
use ordinary update machinery. Table locks and earlier statement effects remain.
Ordinary errors leave undo with statement settlement; fatal rollback precedence
and dropped-operation whole-transaction cleanup use the existing lifecycle.

### Deferred row cache

`LazyRowBuffer` stores logical width separately from allocated values/readiness.
Point buffers start with empty vectors. First access or snapshot undo seeding
initializes storage; readiness is independent of `Val::Null`. Reset visits touched
columns only. Full materialization transfers owned values with `mem::take`,
clears readiness/touched state, and does not replenish a placeholder vector.
Eager reusable buffers remain for scans/ranges, including prepared cold access.

## Implementation Notes

Shipped the new callback API, shared typed point executor and physical helpers,
deferred cache, complete public contract, example migration, and performance validation.
The implementation preserves existing point/range semantic differences and uses
no new durable format, cleanup carrier, or production wait family.

The final executor removes the legacy cold-delete policy and separate request
futures. User-table tests adopt callback semantics, including `Noop` for skipped
misses and no movement for empty updates; nonempty updates retain movement
coverage. Internal catalog and MemTable behavior stays unchanged. The quick-start
example uses `smol::block_on`; its previous futures executor exposed nested-
executor shutdown panics when exercised after migration.

Temporary performance experiments informed the implementation. Benchmark code,
reports, and raw measurements are not retained in the repository.

Validation after legacy API removal:

- `rtk cargo nextest run --workspace`: 1,963 passed.
- `rtk cargo nextest run -p doradb-storage --no-default-features --features libaio`:
  1,847 passed.
- Focused unique-point, upsert, and statement-validation regressions: 19 passed.
- Formatting and strict workspace/all-target Clippy passed on stable Rust.
- Branch style audit passed for 15 tracked Rust files. The untracked unique
  executor passed formatting/Clippy and manual structural review.
- Public error audit refreshed; storage unsafe inventory remained unchanged.
- No removed public method or request-adapter references remain in Rust source.

The earlier implementation validation also exercised the quick-start executable
and measured focused line coverage of 96.71% for the unique executor and 93.45%
for shared access code (including inline tests). Those coverage measurements
predate adapter removal.

## Impacts

- Adds public decision/outcome types and one transaction method; removes the
  three legacy methods and public result exports without adding callback or
  application-error bounds.
- Consolidates user-table point orchestration and shares typed owned application
  with index mutation while preserving range traversal/ordering behavior.
- Changes lazy-buffer storage reuse for snapshot, table, and index consumers;
  broad scan, checkpoint, rollback, and recovery suites remain passing.
- Updates `docs/public-api.md`, transaction documentation, error audit, and the
  compiled quick start.

## Test Cases

- Complete occupied/missing action matrix, trusted-mode semantic checks,
  insertion-key disagreement, malformed row/update input, invalid indexes, and
  resolved index arguments.
- Owned latest-hot read-modify-write, branch-local construction counters,
  non-Clone/non-Send captures, borrowed application errors, and conditional delete.
- In-place, frozen, cold, and forced space-pressure moves; immediate driver and
  other-index key changes; physical RowID outcomes; old snapshots before and
  after commit; and statement/transaction rollback after unique conflicts.
- Skip/empty cancellation, prior hot/cold effects, consumed cold markers,
  committed-delete timing, active/preparing ownership, at-most-once callbacks,
  insertion races, dropped direct operations, empty-update RowID preservation,
  and nonempty row movement.
- Deferred storage, null caching, bounds, undo seeding, reset/reuse after transfer,
  zero-read action initialization counts, indexed-only cold decoding, and
  MemIndex-hit DiskTree short circuiting.

## Open Questions

No blocking questions or deferred in-scope implementation work remain.
A sparse/inline cache and further reduction of the shortest point-dispatch cost
remain possible future optimizations; this task deliberately retains deferred
dense storage. A point/range API merger
remains outside this task.
