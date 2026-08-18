---
id: 000271
title: Support Unique-Driver Key Changes in Index Mutation
status: implemented  # proposal | implemented | superseded
created: 2026-08-17
github_issue: 980
---

# Task: Support Unique-Driver Key Changes in Index Mutation

## Summary

`Statement::table_index_mutate_mvcc` now accepts sparse updates that change the
encoded logical key of the unique secondary index driving the mutation. The
operation still performs exact-key current-read admission and invokes each
eligible original row's callback at most once.

Key-changing driver updates retain their provisional row ownership, leave the
old row and every old index entry unchanged until the dual-tree candidate stream
is exhausted, and cache the owned sparse update in statement effects. Delayed
application then reuses the ordinary hot/cold update, row-move, index,
uniqueness, redo, and rollback machinery in callback order.

The public API and durable formats are unchanged. Same-key unique updates,
non-unique drivers, deletes, skips, and empty updates retain their previous
immediate behavior.

## Context

Task 000265 introduced weak-monotonic index-range-driven MVCC mutation over a
mutable `MemIndex` and captured immutable `DiskTree` root. Unique candidates
merge by logical key without RowID identity. Immediately publishing a changed
driver key could therefore shadow an unread candidate from the other source
before same-statement ownership exclusion inspected it.

Materializing all candidates would avoid shadowing but would replace the
existing current-read selection protocol and make memory proportional to range
cardinality. The shipped design instead preserves candidate emission followed
by RowID re-resolution, ownership admission, exact-key validation, and
provisional lock installation. The successful hot row lock or cold
deletion-buffer claim remains the selection linearization point.

The stable `OwnedRowUndo` box is also referenced from hot undo chains; cold
ownership is represented by the transaction's deletion-buffer marker. Deferred
ownership therefore lives in `StmtEffects`, not an operation-local future, so
outer statement cancellation cannot free an installed undo box.

Checkpoint may publish a retained hot lock as cold ownership before delayed
application. Forward application now follows the authoritative route after
publication. Rollback while a page itself remains `TRANSITION` is a separate
cross-cutting lifecycle problem retained in backlog 000185.

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

- Support actual encoded logical-key changes through a unique driver index.
- Keep old rows and index entries discoverable until candidate traversal ends.
- Preserve exact-key current-read admission and callback-at-most-once behavior.
- Retain hot or cold row ownership from callback evaluation through application.
- Cache, rather than recompute or retry, caller-produced sparse updates.
- Reuse existing mutation, constraint, undo, redo, and rollback primitives.
- Preserve ownership across errors, cancellation, and fatal rollback retention.
- Keep unchanged mutation actions on their existing immediate paths.
- Document ordering, concurrency, memory growth, and permutation limits.

## Non-Goals

- Adding a new public mutation API or changing callback and outcome types.
- Taking a statement-start candidate snapshot or adding predicate/range locks.
- Bounding or spilling the deferred list.
- Changing row, index, undo, redo, checkpoint, or recovery formats.
- Supporting statement-wide unique-key swaps or cycles.
- Making callbacks async, parallel, retryable, or reversible.
- Changing full-table mutation or dual-tree weak-monotonic traversal semantics.
- Making rollback wait while a page remains `TRANSITION`; backlog 000185 owns it.
- Redesigning public statement failure and error-suppression semantics; backlog
  000186 owns that lifecycle work.

## Plan

### Selection and classification

Each candidate follows the existing current-read sequence: resolve the RowID,
admit ownership, validate the exact driver key, install a statement-tagged
provisional `Lock`, invoke the callback once, and validate the sparse update.

For a non-empty update driven by a unique index, the prospective driver key is
assembled from the owned row plus sparse replacements and encoded with the
admitted index encoder. An unchanged encoded key remains immediate. A changed
encoded key is deferred before any row value, redo record, or secondary index is
modified. Outcome update count is recorded when the callback selects the valid
update, as before.

### Statement-owned deferred effects

`StmtEffects` owns a vector of row id, update payload, and direct
`OwnedRowUndo` ownership. Deferral validates and removes the newest matching
`Lock` from ordinary row undo. Moving the box owner leaves its pointee address
stable in the hot undo chain.

After traversal, the vector is reversed once and popped, yielding callback-order
application without front removal. Activation pops the complete entry and
restores its undo to ordinary row undo synchronously before assertions or any
await. Applied row effects therefore remain in physical effect order, while
not-yet-activated locks remain statement-owned in the deferred vector.

### Ownership resumption and physical mutation

Hot resumption acquires a validated page and row write latch without installing
another undo. It verifies that the latest head is the exact restored box, still
belongs to the transaction and statement, matches the RowID, and remains a
`Lock`. It then reuses the existing owned-hot in-place or move-update path and
normal index maintenance.

If the page is `TRANSITION`, foreground application releases its guards and
waits for authoritative route publication or engine poison. Once the route is
cold, it reloads and validates the immutable row and requires the same
transaction's `DeleteMarker::Ref`; it does not perform an ordinary current read,
claim another marker, or install another lock. Existing cold replacement and
index-claim primitives complete the update.

Missing routes, mismatched hot undo heads, and missing or foreign cold markers
after callback ownership are invariant failures rather than stale candidates.

### Settlement, rollback, and constraints

Normal operation errors synchronously drain all pending entries back into
ordinary row undo before returning. Public cancellation, ordinary row rollback,
fatal retention, and transaction-effect folding perform the same settlement.
Index rollback still precedes row rollback. Pending no-op locks are appended so
reverse row rollback removes them before earlier physical effects.

Unique checks run when each delayed row is physically applied. Because old keys
are released one row at a time, swaps and cycles may report the existing
duplicate-key error and roll back. No permutation planner was added.

The deferred list is memory-only and intentionally uncapped. Memory grows with
the number of changed driver keys, their sparse payloads, and retained undo
boxes. Locks live longer, and duplicate or storage failures may be reported
after all callbacks have run.

## Implementation Notes

Implemented delayed unique-driver mutation without changing the public API or
durable storage. The former `InvalidDmlInput` rejection for actual driver-key
changes was removed; ordinary duplicate-key behavior remains authoritative.

The implementation added statement-owned deferred effects and centralized
ownership settlement across normal errors, statement rollback, cancellation,
transaction folding, fatal retention, successful merge assertions, and test
empty-state checks. Review simplified the representation from an optional undo
slot to direct `OwnedRowUndo` ownership because activation removes the complete
entry before its first await.

The hot path gained exact retained-head resumption, and the cold path reuses the
transaction-owned marker after successful checkpoint publication. A
deterministic transition test covers both commit and rollback after publication.
Cancellation review confirmed stable-route ownership safety and retained the
known rollback-on-`TRANSITION` limitation in backlog 000185.

The existing public statement contract still permits callback code to catch an
individual DML error and return success. This task settles deferred ownership
before such an error escapes but deliberately does not introduce eager rollback
or a one-DML capability; backlog 000186 records that broader redesign.

Documentation for transactions, indexes, and callback ordering was updated, and
the public error audit was refreshed. No benchmark was required because the new
path is correctness-oriented and intentionally allocates one in-memory entry per
key-changing row.

Final verification evidence:

- 15 focused `index_mutate::tests` passed after the final ownership refactor.
- The authoritative workspace run passed 1,722 tests.
- The alternate `libaio` storage run passed 1,653 tests.
- Strict workspace clippy and branch-diff style audit passed.
- Focused coverage across six touched Rust files was 93.17%; each file exceeded
  90% line coverage.

## Impacts

- Index mutation now separates callback selection from physical application for
  actual unique-driver key changes.
- Statement effects own deferred update payloads and stable undo boxes across
  asynchronous boundaries.
- Hot/cold table access can resume exact retained ownership after route changes.
- Transaction and index documentation now describe delayed ordering, memory,
  uniqueness, and concurrency behavior.
- Public API, schema, compatibility, and persistent formats are unchanged.

## Test Cases

- Successful hot in-place key changes invoke each callback once and publish the
  final unique mappings only after traversal.
- Frozen variable-length unique keys exercise delayed hot row movement.
- Mixed initially cold and hot ranges apply complete key-changing updates.
- A deferred hot lock survives checkpoint publication and resumes cold for both
  commit and rollback outcomes.
- Duplicate targets fail during delayed application and ordinary statement
  rollback restores original rows and mappings while settling pending locks.
- Stable-route statement cancellation retains every undo owner and terminal
  rollback restores the row.
- A competing writer receives `WriteConflict` while a deferred callback result
  retains the row lock.
- Existing mixed actions, no-op ownership release, same-statement exclusion,
  active-owner conflict, stale-key behavior, and preparing-owner retry remain
  covered by index-mutation tests.

## Open Questions

- `docs/backlogs/000185-row-undo-rollback-through-page-transition.md` must make
  every row-undo rollback carrier wait for authoritative route publication or
  poison while a page remains `TRANSITION`.
- `docs/backlogs/000186-statement-failure-rollback-before-error-return.md` owns
  eager rollback, failure latching, and the public one-DML capability.
- Statement-wide unique-key permutations remain unsupported and should be
  planned separately if required.
- A memory cap or spill policy remains unnecessary until workload evidence
  demonstrates that the intentionally uncapped list is operationally limiting.
