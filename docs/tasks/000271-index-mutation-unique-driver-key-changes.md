---
id: 000271
title: Support Unique-Driver Key Changes in Index Mutation
status: proposal  # proposal | implemented | superseded
created: 2026-08-17
github_issue: 980
---

# Task: Support Unique-Driver Key Changes in Index Mutation

## Summary

Extend `Statement::table_index_mutate_mvcc` so an update selected through a
unique driver index may change that driver's encoded logical key. The callback
continues to run exactly once while definitive hot-row or cold-row ownership is
held. If its update changes the driver key, retain the provisional `Lock`, keep
the row and all old index entries unchanged for the remainder of traversal,
cache the owned update, and apply the complete row mutation after the index
stream is exhausted.

Store each delayed update and its `OwnedRowUndo` together in a statement-owned
in-memory list. During delayed application, move that exact undo back to the
end of the ordinary row-undo buffer before any await, then reuse the existing
hot/cold update, row-move, index-maintenance, uniqueness, redo, and rollback
primitives. No new public API, durable format, candidate snapshot, spill path,
or opaque undo token is introduced.

## Context

Task 000265 added index-range-driven MVCC mutation over the mutable `MemIndex`
and a captured immutable `DiskTree` root. It deliberately rejects actual
unique-driver key changes. Unique candidates merge by logical key without
RowID identity, so immediately publishing a changed key can shadow an unread
candidate from the other source before same-statement exclusion can inspect
that row.

Materializing index candidates before mutation would prevent index shadowing,
but candidate identity alone is not the semantic row read. It would widen the
interval between candidate emission and exact-key row ownership, then require
a second read/version comparison to connect callback input to mutation. The
chosen design instead preserves the existing current-read path: candidate
emission is followed by RowID re-resolution, ownership admission, exact-key
validation, and provisional `Lock` installation before the callback. The
successful exact-key row lock or cold deletion-buffer claim is the selection
linearization point.

That candidate-to-lock window already exists because
`BorrowedIndexMutationStream` returns batches that are processed sequentially.
A concurrent writer that commits before ownership may serialize before this
operation: a non-driver change is visible to the callback, while a driver-key
change makes the candidate stale. A foreign active owner conflicts and a
preparing owner is awaited and re-resolved. Delaying physical mutation does not
enlarge this window; it lengthens only the lock-to-update interval, during
which retained row ownership excludes another writer.

`StmtEffects` currently owns row undos in physical effect order. An
`OwnedRowUndo` is a stable box also referenced by the hot-row undo chain, while
a cold `Lock` corresponds to a transaction-owned deletion-buffer marker.
Moving the box between statement-owned vectors preserves the referenced
address. The deferred list must itself belong to `StmtEffects`, because
dropping an operation-local list during async statement cancellation could
otherwise free boxes that remain installed in row or deletion-buffer state.

Checkpoint transition may move a retained hot lock into the cold deletion
buffer after its callback and before delayed application. Transition capture
preserves transaction ownership, and foreground mutation already has a
route-publication-or-poison wait, but row-undo rollback is not transition-aware
before the cold pivot is published. This task covers forward delayed
application after authoritative route publication. Rollback that begins while
the page remains `TRANSITION` is deferred to a related backlog. Ordinary cold
current-read code also treats this transaction's marker as an already consumed
row, so delayed application needs an explicit resume-owned path rather than
re-entering ordinary index-candidate acquisition.

Source Backlogs:

- `docs/backlogs/000183-index-mutation-unique-driver-key-changes.md`

Related Backlogs:

- `docs/backlogs/000185-row-undo-rollback-through-page-transition.md`
- `docs/backlogs/000186-statement-failure-rollback-before-error-return.md`

Related Tasks:

- `docs/tasks/000265-index-driven-mvcc-mutation-api.md`

Issue Labels:

- type:task
- priority:medium
- codex

## Goals

- Allow actual encoded logical-key changes when the selected driver is a
  unique secondary index.
- Prevent changed keys from shadowing unread mutable or immutable candidates by
  leaving every delayed row and old index entry physically unchanged until
  traversal completes.
- Preserve exact-key current-read ownership before callback execution and
  invoke each eligible original row's callback at most once.
- Retain the provisional row lock from callback evaluation through delayed
  physical mutation, including across successful hot-to-cold route publication.
- Cache the callback's owned sparse update and never recompute or retry callback
  code.
- Reuse existing row update, row move, secondary-index maintenance, unique
  constraint, undo, redo, and statement rollback primitives.
- Keep all installed undo boxes owned across ordinary errors, async
  cancellation, fatal rollback retention, and successful statement merging.
- Preserve existing behavior for unique same-key updates, non-unique drivers,
  key-preserving actions, deletes, skips, and empty updates.
- Document delayed physical ordering, current-read concurrency semantics,
  memory growth, and unsupported key permutations.

## Non-Goals

- Adding a dedicated public mutation API or changing the callback,
  `RowMutation`, or `TableMutationOutcome` interfaces.
- Capturing a statement-start candidate snapshot or requiring a write conflict
  for a transaction that commits before exact-key row ownership is acquired.
- Adding predicate, gap, next-key, range, or table-exclusive locks.
- Bounding or spilling the deferred list, adding a statement work file, or
  changing persistent table, undo, redo, checkpoint, or recovery formats.
- Generalizing all row undo into a slab, arbitrary-position mutation API, or
  reusable token-based undo arena.
- Supporting statement-wide unique-key permutations, swaps, or cycles. Such
  updates may fail with the existing duplicate-key error and roll back.
- Making callbacks async, parallel, retryable, or reversible, including
  reversing caller-owned external callback side effects.
- Preserving immediate candidate-order physical effects between independent
  rows. Key-preserving actions remain immediate while driver-key-changing
  updates run after traversal.
- Changing full-table mutation or the weak-monotonic behavior of the existing
  dual-tree index stream.
- Making row-undo rollback wait while a page remains `TRANSITION`, propagating
  a rollback-specific route/poison context, or redesigning row-page lifecycle.
  That cross-cutting cleanup work belongs to backlog 000185.
- Changing `Transaction::exec` or the public `Statement` API to expose only one
  DML attempt, eagerly rolling back before a DML error returns, or latching a
  failed statement against caller error suppression. That public/private
  lifecycle redesign belongs to backlog 000186.

## Plan

### Mutation classification and public contract

Keep `Statement::table_index_mutate_mvcc` and its admission, range encoding,
root snapshot, candidate stream, outcome, and synchronous callback signature.
No caller hint is needed to identify a possible key change.

For every candidate, retain the existing sequence:

1. Re-resolve the candidate RowID to its current hot or cold location.
2. Admit foreign ownership and revalidate the exact encoded driver key.
3. Install a statement-tagged provisional `Lock`.
4. Invoke the callback once against the latest owned row image.
5. Validate any sparse update while ownership remains held.

`Skip`, `Delete`, an empty update, and a key-preserving update retain their
current immediate paths. For a non-empty update driven by a unique index,
assemble and encode the prospective driver key from the owned row plus sparse
updates. A same encoded key is immediate even when driver columns were
mentioned. A different encoded key no longer returns `InvalidDmlInput`;
instead, delay the complete row update before any row value, redo, or secondary
index is changed.

Increment `update_count` when the callback selects the valid update, matching
current outcome semantics. A later failure returns no outcome. Before that
normal `Err` escapes, the operation-level boundary performs the synchronous
deferred-ownership settlement described below and returns the same error. If
the enclosing `Transaction::exec` callback propagates it, existing ordinary
statement rollback follows. If callback code catches it and returns `Ok`,
today's partial-effect semantics remain in force; backlog 000186 owns the
future one-DML and eager-rollback contract. Keep all old index entries visible
while the stream drains, so no new unique key can shadow an unread `DiskTree`
or `MemIndex` candidate. Current-statement ownership continues to suppress
duplicate source candidates without another callback.

After `BorrowedIndexMutationStream` reaches exhaustion, apply delayed updates
in callback/candidate order. The callback is not invoked again. Deletes and
key-preserving updates selected after an earlier delayed callback may already
have taken effect; document that row callbacks must be independent of the
physical application order of other rows and that unique constraint failures
may be reported after later callbacks ran.

### Statement-owned deferred representation

Add an equivalent of the following owned representation to the statement
effects layer:

```rust
struct DeferredIndexUpdate {
    row_id: RowID,
    update: Vec<UpdateCol>,
    undo: OwnedRowUndo,
}
```

`StmtEffects` owns a `Vec<DeferredIndexUpdate>`. No slot token or separate slab
is required: the entry and its owned undo are the single-use ownership
capability, and application walks the list directly. Each
`table_index_mutate_mvcc` call must enter with this list empty and leave it empty
on every normal `Ok` or `Err` return. The mutable `Statement` borrow prevents
overlapping calls from sharing the operation-local list.

When classification chooses delay, assert that the newest ordinary row undo
belongs to the current statement and table, matches the candidate RowID, and
has `RowUndoKind::Lock`. Pop it from `RowUndoLogs` and place it in the deferred
entry with the owned `Vec<UpdateCol>`. Moving `OwnedRowUndo` moves only its box
owner, leaving the pointee address installed in the hot undo chain unchanged.
No redo or index undo is produced at this stage.

Before any await for one delayed update, pop the complete entry and append its
undo to the end of ordinary `row_undo`. Then move its update payload into route
resolution. From that point, existing helpers may rely on the restored lock
being the newest ordinary row undo and may convert it to `Update` or `Delete`.
An in-place update leaves that converted undo at the physical effect position.
A no-space, frozen, or cold replacement first converts the old lock to
`Delete`, then appends the replacement row's normal `Insert` undo.

Remove each successfully activated entry from the deferred list. If activation
or physical application fails, the activated undo remains ordinary
statement-owned rollback state, while all not-yet-activated boxes remain owned
by their deferred entries.

### Resume retained hot and cold ownership

Add a delayed-application loop to `IndexMutator` or its table-access helpers.
It consumes each entry only after the candidate stream is exhausted and uses
the operation's existing table accessor, runtime, admitted index layout, and
captured root snapshot.

For a hot route, acquire a validated page guard and row write latch without
installing another undo. Verify that the latest undo head is the exact restored
`OwnedRowUndo`, belongs to this transaction and statement, matches the RowID,
and is still `Lock`. This direct proof bypasses normal current-statement
candidate exclusion only for delayed application. Resume through the existing
owned-hot update path so in-place updates, frozen/no-space moves, index key
changes, unique checks, redo, and old-index proofs retain their current
behavior.

If the page is in transition, release guards, wait for authoritative route
publication, and resolve the RowID again. Reuse the foreground
route-publication-or-poison wait: successful route publication retries
resolution, while poison returns the existing fatal error without attempting
delayed application. Do not add rollback-specific waiting or mutate a
`TRANSITION` page in this task. For a cold route, load and validate the immutable
row location, then require a `DeleteMarker::Ref` for the same transaction and
the deferred RowID. Do not use `read_latest_cold_row`, do not claim a second
marker, and do not install a second lock. A lock originally acquired cold has
`page_id: None`; a hot lock captured by transition may retain its original page
id. Both use the current row location plus transaction-owned marker as write
authority and then reuse the owned-cold update path.

Once the callback-created lock exists, `NotFound`, a different undo head, or a
missing/foreign cold marker during delayed resumption is an invariant failure,
not a stale candidate to skip. Foreign transactions cannot legitimately
change the row after callback execution.

### Same-statement exclusion and concurrency

Do not remove or weaken traversal-time same-statement exclusion. A delayed hot
row remains tagged by its current-statement undo head, and a delayed cold row
remains represented by this transaction's deletion-buffer marker. Repeated
MemIndex/DiskTree candidates therefore remain ineligible for another callback.
Delayed application does not traverse the driver index and instead consumes
the exact owned undo entry directly.

Preserve the existing selection linearization point and conflict rules:

- A writer committed before exact-key ownership may serialize first. The
  callback observes its latest non-driver values, or exact-key revalidation
  rejects the now-stale candidate.
- A foreign active owner produces `WriteConflict` before the callback.
- A preparing owner is awaited and the candidate is authoritatively
  re-resolved before the callback.
- After the callback's lock is installed, every foreign update to that row,
  including one that preserves the driver key, conflicts until transaction
  completion.

### Rollback, cancellation, and settlement

Centralize an idempotent synchronous operation that drains the deferred list,
moves every remaining `OwnedRowUndo` into the end of ordinary row undo, and
discards unapplied update payloads. Wrap the mutation implementation in one
outer result boundary. On `Ok`, assert that delayed application consumed every
entry. On `Err`, run the synchronous drain before returning the same error.
This is ownership settlement, not operation-local asynchronous rollback:
already-applied immediate or delayed physical effects remain in ordinary
statement effects under today's public contract.

Keep `Statement::table_index_mutate_mvcc` as that normal-return boundary. It
asserts the deferred list is empty at operation entry, captures the inner table
accessor result instead of tail-returning it, and drains on `Err` after the
inner mutable borrow ends. Admission or shape validation that fails before any
row effect leaves the already-empty list unchanged. Cancellation and panic can
bypass this normal epilogue and are handled by the carrier paths below.

Also integrate the same drain into every statement ownership-consumption path:

- Before a normal error returns from `table_index_mutate_mvcc`, including
  callback, validation, constraint, index, and storage errors.
- Before ordinary statement row rollback, after index rollback has completed.
- Before incomplete public/private statement effects are folded into
  transaction rollback on future cancellation or panic settlement.
- Before statement effects are moved into fatal rollback retention, including
  an index-rollback failure that occurs before ordinary row rollback starts.
- In test-only empty/effect assertions and any other code that consumes or
  replaces all `StmtEffects` ownership.

Pending entries contain only no-op `Lock` row effects and no index effects.
Appending them at rollback preparation makes reverse row rollback unlink those
locks before earlier physical effects; index rollback still precedes all row
rollback. Applied immediate and delayed mutations already occupy ordinary row
undo in physical effect order.

Successful `merge_into_trx_effects` must require the deferred list to be empty.
The normal operation-error boundary empties it even when callback code catches
the error and later lets the enclosing statement merge. Cancellation or panic
may bypass that normal boundary, so `StmtState` and private panic settlement
must drain before folding effects into transaction rollback. Private ordinary
errors retain their existing whole-transaction merge behavior after the
operation boundary has normalized deferred ownership. Keep each box inside
`StmtEffects` across every await so cancellation cannot invalidate a pointer
reachable from a row undo chain or cold ownership marker. Do not implement the
one-DML public capability or eager statement rollback from backlog 000186 here.

### Constraint behavior, memory, and documentation

Use the existing per-row unique index insertion/update primitives during
delayed application. Unique constraints remain immediate at the time each
deferred row is physically applied. If a target key is still occupied, settle
remaining deferred ownership and return the existing duplicate-key error. When
the enclosing callback propagates that error, ordinary statement rollback
restores prior immediate effects, already-applied delayed effects, and pending
locks.

Do not add a special permutation algorithm. Because old driver entries remain
until each delayed row is applied, swaps and cycles can encounter an occupied
key and fail. A later design may introduce statement-wide key release/claim
planning, but it is not required for single-row-independent mutation.

The deferred list is memory-only and intentionally uncapped. Memory grows with
the number of driver-key-changing rows and their sparse update payloads, in
addition to retained undo boxes. Document this behavior together with longer
row-lock retention and the possibility that duplicate/storage errors occur
after all callbacks have run.

Update transaction and index documentation to replace the unique-driver
key-preservation restriction with the delayed model, ownership linearization,
same-statement behavior, ordering contract, memory policy, transition
resumption, and permutation non-goal. Refresh the public error audit if removal
of the unique-driver `InvalidDmlInput` path changes the documented error set;
the existing duplicate-key surface remains authoritative.

## Implementation Notes

## Impacts

- `doradb-storage/src/table/index_mutate.rs`: classify prospective unique
  driver keys, enqueue owned delayed updates, drain them after traversal, and
  remove the current key-change rejection.
- `doradb-storage/src/table/access.rs`: expose/reuse owned hot and cold update
  primitives and old-index proofs for delayed application, including a
  retained-lock cold resume path.
- `doradb-storage/src/table/hot.rs` and
  `doradb-storage/src/trx/row.rs`: resume an exact already-installed hot lock
  without normal same-statement exclusion or a second undo installation.
- `doradb-storage/src/trx/stmt.rs` and
  `doradb-storage/src/trx/undo/row.rs`: own grouped deferred updates, move stable
  undo boxes between deferred and ordinary buffers, normalize ownership before
  normal errors return, and cover rollback, cancellation, fatal retention,
  merge, and test settlement paths. Preserve the current public and private
  statement API contracts.
- `doradb-storage/src/table/page_transition.rs` and deletion-buffer helpers:
  no production transition or rollback protocol change; test and expose only
  the ownership checks required to resume a captured hot lock after successful
  cold-route publication.
- `doradb-storage/src/row/ops.rs`: clarify callback and physical-order
  documentation if needed; no public representation change.
- `docs/transaction-system.md`, `docs/index-design.md`, and related secondary
  index documentation: describe delayed unique-driver updates and their
  concurrency, ordering, memory, and permutation contracts.
- `docs/public-error-audit.csv`: refresh the operation's audited errors if the
  removed `InvalidDmlInput` restriction changes its row.
- No catalog, table-file, row layout, durable undo/redo, checkpoint, recovery,
  or compatibility format changes.

## Test Cases

1. Change unique driver keys over MemIndex-only, DiskTree-only, and mixed
   ranges in both key directions; every eligible original row is offered once
   and every final mapping resolves to the updated row.
2. Exercise source equality, multiple candidates in one returned batch, and a
   new key that would have shadowed an unread candidate under immediate
   mutation. Verify the old candidates remain discoverable until traversal
   exhaustion.
3. Cover delayed hot in-place updates, variable-length no-space moves, frozen
   row moves, initially cold replacements, and updates that also change other
   unique and non-unique indexes.
4. Pause deterministically after a hot update is deferred, transition its page
   to cold, publish the cold pivot, then resume delayed application. Verify
   success, index ownership, redo/undo state, commit, and rollback after route
   publication without acquiring a second lock.
5. Mix `Skip`, `Delete`, empty update, immediate key-preserving update, and
   delayed key-changing update. Verify callback order, callback-at-most-once,
   outcome counts, documented physical ordering, and final indexes.
6. Assign a driver column its existing encoded value and verify the immediate
   path remains in use; changing only another index must not enter the deferred
   path.
7. Let transaction B commit a non-driver change after candidate emission but
   before transaction A locks the row. Verify A's callback observes B's latest
   value and may proceed after B.
8. Let B change the driver key before A locks and verify exact-key revalidation
   skips the stale candidate. Let B remain active and verify A receives
   `WriteConflict`; cover the existing preparing-owner wait/re-resolution path.
9. Pause after A's callback has deferred the row and verify any B update,
   whether or not it changes the driver key, receives `WriteConflict` while A
   later applies the cached callback result exactly once.
10. Emit duplicate mutable/immutable candidates and potential replacement-row
    candidates for a deferred row. Verify traversal-time same-statement
    exclusion suppresses every repeated callback while exact delayed resumption
    succeeds once.
11. Trigger a driver or other unique-index duplicate during delayed
    application after earlier immediate and delayed effects succeeded. Verify
    index rollback precedes row rollback and restores all original rows and
    mappings.
12. Exercise a two-row key swap and a longer key cycle. Verify the unsupported
    permutation fails with the existing duplicate-key error and leaves the
    statement unchanged.
13. Return callback, validation, constraint, index, and storage errors after one
    or more updates were deferred on stable hot or already-published cold
    routes. In a propagated-error variant, verify ordinary statement rollback.
    In a caught-error variant whose enclosing callback returns `Ok`, verify the
    operation boundary empties the deferred list, discards unapplied payloads,
    leaves every undo safely owned, and permits the existing partial effects to
    merge without a dangling pointer. Inject row-rollback failures only on
    stable routes and verify fatal retention preserves ownership.
14. Cancel the public statement future at deterministic hooks after deferred
    capture on a stable hot route, after successful cold-route publication, and
    after some deferred updates have been activated. Verify `StmtState` retains
    every installed undo and whole-transaction rollback leaves no lock, row,
    index, or dangling undo ownership. Cancellation while a page remains
    `TRANSITION` belongs to backlog 000185.
15. Queue several hot and cold updates, apply only a prefix, then force an
    error. Verify ordinary row undo retains physical effect order and pending
    deferred locks are all unlinked safely.
16. Run targeted index-mutation, row-undo, deletion-buffer, and page-transition
    tests with `rtk cargo nextest`, followed by the authoritative workspace
    suite and alternate `libaio` storage suite from
    `docs/process/unit-test.md`.

Use deterministic pause hooks or channels for concurrency and transition
tests; do not depend on sleeps or timing races.

## Open Questions

No blocking design questions remain within this task's scope.

- `docs/backlogs/000185-row-undo-rollback-through-page-transition.md` owns
  rollback routing and poison wakeup while a page remains `TRANSITION`.
- `docs/backlogs/000186-statement-failure-rollback-before-error-return.md` owns
  the one-DML public `Statement` capability, eager rollback before returning a
  DML error, and failure latching against callback error suppression.
- A future task may support unique-key permutations through statement-wide key
  release/claim planning. Record it as a backlog follow-up during task
  resolution rather than expanding this task.
- A future task may add a memory cap, spill storage, or batch policy if
  workloads demonstrate that the intentionally in-memory deferred list needs
  resource control.
