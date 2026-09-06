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
write boundary replacing the public upsert/update/delete methods. Its synchronous
`FnOnce` callback receives the latest acquired row as `Some(&mut LazyRow)`, or an
observed missing entry as `None`, and chooses Skip, missing-only Insert, occupied
sparse Update, or occupied Delete. Results distinguish Noop, Inserted(RowID),
Updated(RowID), and Deleted; physical replacement remains a logical update.

Specialized MemIndex-first point selection reuses owned hot/cold mutation and
statement settlement. Key changes apply immediately, and callbacks that do not
read columns avoid dense lazy-row cache initialization.

## Context

Legacy methods accepted mutation values before row selection and could not
express an owned-current-row computation such as `b = b + 1`. A preceding
snapshot read did not provide the same ownership boundary. Index-range mutation
already supplied owned-row primitives, but its traversal and deferred driver-key
updates were unsuitable for direct point execution.

User review selected removal of the legacy public APIs without compatibility
adapters. Internal catalog/MemTable contracts and full-row ownership from tasks
000202 and 000205 remain. Shared point/range machinery builds on tasks 000265
and 000271. This task has no parent RFC or durable-format migration.

Source Backlogs:

- `docs/backlogs/closed/000195-unify-unique-key-mvcc-mutation-api.md`

Issue Labels:

- type:task
- priority:medium
- codex

## Goals

- Provide one current-write unique-point callback boundary with at-most-once
  execution, both public index argument forms, and resulting physical RowIDs.
- Enforce action/entry validity and selected-key agreement for inserted rows.
- Preserve application error payloads, statement rollback, and cancellation.
- Reuse owned physical mutation while retaining point-specific selection.
- Defer dense callback storage until values are read or seeded from undo.
- Supply caller migration, public documentation, regression coverage, and
  temporary performance validation.

## Non-Goals

- A public full-row replacement action, implicit occupied Insert, or a combined
  point/range interface.
- Gap locks, asynchronous/retried callbacks, retrying a racing insert as an
  update, or statement-wide unique-key permutation planning.
- Changes to durable records, catalog/MemTable behavior, cleanup ownership,
  wait families, storage backends, or test timeout configuration.
- A sparse/inline cache or a claim of zero total mutation allocation.
- Repairing the inherited row-replacement race deferred to backlog 000196.

## Rejected Alternatives

- A direct request enum cannot compute a decision from the latest acquired row.
- Equal-bound range execution adds traversal state and inherits different cold
  eligibility and driver-key ordering policies.
- An implicit put action hides entry-state policy and can require constructing
  an insertion payload that the occupied path never uses.

## Plan

### Public boundary and validation

The transaction method uses one ordinary statement runner. The statement admits
an active unique index, validates the lookup key under the DML-validation
policy, and acquires transaction-lifetime TableData(IX) before the callback.

Missing entries accept Skip or Insert; occupied entries accept Skip, Update, or
Delete. Entry/action validity and insertion-key agreement remain mandatory when
payload validation is disabled. Ordinary full-row shape and sparse-update
validation follow the existing opt-out policy. Invalid decisions return
InvalidDmlInput without converting application errors into engine errors.

Callbacks require no Send, Sync, Clone, static lifetime, or application-error
trait bounds. Borrowed row values cannot outlive the callback. Engine failures
remain CallbackError::Engine; application payloads remain CallbackError::User.

### Selection and ownership

The operation-scoped point executor owns no range stream or candidate batch.
Each selection attempt binds its root/index and resolves the physical RowID.
Hot selection reuses `HotRowMutator::lock_for_write`, retaining the row write
access through callback execution and action conversion. Foreign ownership is
classified before interpreting mutable deletion/key state.

Cold selection checks CDB ownership/timestamps before durable deletion, loads
the immutable block, and revalidates the key. A committed delete newer than the
writer snapshot can conflict; an already-consumed same-transaction image is
missing. Point and range eligibility remain separate because their committed
cold-delete policies differ.

After eligibility, both executors use the same cold claim helper. Successful
claims register provisional Lock undo before returning, with no intervening
await. Failed/preparing claims register no undo. Callers exclude earlier
same-transaction markers, so serialized statement execution establishes fresh
claim provenance. Callers retain their existing wait/retry and error handling.

A miss releases attempt resources before invoking the callback with None. It
creates no gap lock: a racing insertion may fail normally without repeating the
callback or changing the chosen action. Preparing and transition retries occur
before callback invocation; the inherited stale-hot-RowID exception remains
explicitly deferred.

### Owned actions and shared machinery

Point and range execution share the owned hot update/move/index-maintenance
helper, owned cold replacement/deletion effects, and provisional hot/cold
cancellation. The range helper was extracted into the accessor so both
executors use one physical implementation. Insertion, undo/redo, index claims,
and statement settlement continue through existing machinery.

Skip and empty Update release only this invocation's provisional ownership.
Empty Update returns Updated(original_row_id), including frozen/cold rows;
nonempty updates retain ordinary physical movement and return the replacement
RowID. Earlier statement effects and transaction-lifetime table locks remain.

Hot deletion captures complete index keys before releasing the page guard and
awaiting index masking. Cold deletion decodes only indexed columns. Retained
cold-block decoding and block-loading helpers share sorted indexed-column
selection while retaining their own loading, validation, and error context.

The three old public methods and their statement/accessor wrappers are removed.
Internal update/delete/upsert outcomes remain crate-private for catalog and
MemTable consumers. User-table callers use UniqueMutationOutcome, choosing Skip
for missing update/delete targets and Insert or sparse assignments for upsert.

### Deferred row cache

LazyRowBuffer separates logical width from allocated values/readiness. Point
buffers start empty; first column access or snapshot undo seeding initializes
storage. Readiness remains independent of null values. Reset clears touched
columns, and full materialization transfers owned values without replenishing a
placeholder vector. Cold updates reuse callback-cached values or decode directly
when the cache is unused. Existing eager buffers remain available to scans.

## Implementation Notes

Shipped the unified callback API, specialized point executor, shared owned
mutation and ownership bookkeeping, deferred cache, and complete caller
migration. No new durable format, cleanup carrier, or production wait family
was introduced. Source backlog 000195 is closed as implemented.

Review removed legacy compatibility adapters and normalized empty updates to
release provisional ownership without moving rows. A subsequent duplication
review consolidated cold claim/undo registration, hot/cold cancellation, and
indexed-column selection while preserving separate point/range policies.

Concurrency review identified an inherited stale-RowID race when a concurrent
writer replaces a selected hot row. Source comparison with pre-refactor
`7a1d0a1` confirmed the old point APIs shared the rejection behavior. The interim
retry fix and its tests were withdrawn for separate design in backlog 000196,
which preserves Move/re-lookup and Delete/successor-RowID alternatives.

Temporary performance experiments informed implementation; benchmark code,
reports, and raw measurements are not retained. The quick-start executable was
migrated and exercised using smol::block_on after its previous executor exposed
nested-executor shutdown panics.

Final validation after the duplication cleanup on 2026-09-07:

- Focused point/range and deferred-cache tests: 31 passed.
- Standard workspace validation: 1,963 passed on the second invocation.
- Alternate libaio storage validation: 1,847 passed.
- Branch style audit: 16 Rust files passed, including formatting and strict
  workspace/all-target Clippy.
- Public error audit matched the tracked CSV; no unsafe-code changes occurred.
- No removed public API or compatibility-adapter references remain in Rust.

The first workspace invocation passed 1,962 tests but timed out in the existing
benchmark update-template lifecycle test. That test subsequently passed 100
focused stress iterations, and the repeated workspace run passed. Its cause is
unresolved and recorded in backlog 000197; rerun success is not treated as a fix.

Earlier focused line coverage measured 96.71% for the unique executor and 93.45%
for shared access code, including inline tests. Those measurements predate
adapter removal and the final helper extraction.

## Impacts

- Replaces three public methods/result exports with one callback method and
  explicit decision/outcome types; callers migrate without a deprecation wrapper.
- Consolidates user-table point orchestration and shares mutation/ownership
  mechanics with range execution while preserving traversal and visibility rules.
- Changes lazy-buffer allocation and reuse for point, scan, and index consumers.
- Updates public API/transaction documentation, error audit, and the compiled
  quick-start example; catalog/MemTable behavior and durable formats remain.

## Test Cases

- Occupied/missing action matrix, trusted-mode semantic checks, insertion-key
  disagreement, malformed payloads, invalid indexes, and resolved arguments.
- Current-row read-modify-write, conditional deletion, branch-local payload
  construction, non-Clone/non-Send captures, and borrowed application errors.
- Hot/frozen/cold and space-pressure movement, immediate key changes across
  indexes, resulting RowIDs, old snapshots, and rollback after unique conflicts.
- Skip/empty cancellation, prior ownership, consumed cold markers, committed
  delete timing, active/preparing owners, and callback-at-most-once behavior.
- Missing-entry insertion races, dropped-operation cleanup, empty-update RowID
  preservation, null caching, undo seeding, and buffer reuse after transfer.
- Dense-cache initialization counts, indexed-only cold deletion, and MemIndex
  hit short-circuiting of DiskTree lookup.

## Open Questions

- [Backlog 000196](../backlogs/000196-resolve-stale-unique-point-lookups-across-row-replacement.md):
  design a replacement-aware unique access protocol; the inherited race remains
  unresolved in this task.
- [Backlog 000197](../backlogs/000197-investigate-benchmark-update-template-lifecycle-timeout.md):
  investigate the unexplained benchmark lifecycle timeout observed in validation.

Sparse/inline cache storage and further point-dispatch optimization remain
possible future improvements; neither is required by the shipped contract.
