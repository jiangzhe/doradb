---
id: 000303
title: Repair Dangling Row Undo and Deferred Lock-to-Delete Mismatch
status: implemented
created: 2026-09-13
github_issue: 1061
---

# Task: Repair Dangling Row Undo and Deferred Lock-to-Delete Mismatch

## Summary

User-table rollback now restores and unlinks each hot-origin undo against its
exact original page generation before freeing the owning allocation. This
includes retained Transition pages before and after cold-route publication.
Forward-link rollback restores exact source slots without releasing an earlier
statement's surviving ownership.

Deferred hot-to-cold completion now changes the original page's live delete
bit, deleted count, and the same Lock undo to Delete together under its row
latch. Guarded CDB reconciliation validates active ownership before local
changes and preserves markers required by surviving main-row predecessors.
Checkpoint column values and its prepared deletion bitmap remain fixed.

## Context

Source Backlogs:

- docs/backlogs/closed/000199-repair-dangling-row-undo-ref-and-deferred-lock-to-delete-mismatch.md

Issue Labels:

- type:bug
- priority:high
- codex

This standalone correction follows implemented tasks
[000272](000272-row-undo-rollback-through-page-transition.md) and
[000300](000300-fix-stale-unique-read-current-lookups-across-row-replacement.md).
Their shipped cleanup assumption treated cold-route publication as sufficient
to release hot undo, although retained point readers and scan descriptors
could still reach that original page. Deferred finalization also changed undo
kind without synchronizing its physical inverse.

Task 000219 established prepared checkpoint images and Frozen mutation
versions. Tasks 000301 and 000302 supplied the RowStore and shared mutation
execution boundaries used here. There is no parent RFC or phase to synchronize.

The durable contracts are recorded in [data checkpoint](../data-checkpoint.md),
[checkpoint publication](../checkpoint.md),
[transactions](../transaction-system.md),
[deletion checkpoint](../deletion-checkpoint.md),
[garbage collection](../garbage-collect.md), and
[shutdown and poison](../shutdown-and-poison.md).

## Goals

- Unlink original hot undo before freeing its allocation, independent of pivot.
- Restore exact forward slots in reverse order without releasing source owners.
- Keep deferred undo kind, live deletion state, and accounting synchronized.
- Remove a CDB marker only after validating ownership and surviving main undo.
- Preserve checkpoint images, secondary indexes, redo, restart behavior,
  cancellation ownership, active snapshot retention, and canonical first fatal.

## Non-Goals

- New ownership, column writes, or forward publication on Transition pages.
- CDB undo history, shared ownership of every undo node, or permanent page pins.
- Changes to public APIs, durable formats, checkpoint cutoffs, retirement
  fences, catalog lifecycle policy, or the test runner.
- Further consolidation of table metadata or mutation execution.

## Rejected Alternatives

Waiting for publication still requires retained Transition-page cleanup and
would depend on a checkpoint that may fail. Delaying undo destruction or adding
shared transition ownership would introduce a broader lifetime model without
itself restoring row state and source links.

## Plan

The implementation follows original ownership rather than current routing.
`RowUndoRollbackContext` borrows pool guards and the exact transaction status.
The owning undo remains in its vector across page access; source records are
restored and popped in reverse order before its own row effect is undone.
The entry is popped only after synchronous inverse application and unlink.
Existing cooperative yields occur between completed entries.

User-table coordination lives in `table/rollback.rs`. RowStore pins the exact
recorded generation and validates the initialized row range. Row-latched
validation checks head allocation identity without dereferencing the candidate
pointer, exact active ownership, allowed kind, and inverse preconditions.
Violations carry `InternalError::RowUndoState` beneath `RuntimeError::TableAccess`
with operation and identity diagnostics. A genuine generation miss is an error,
including below the published pivot. Memory/catalog rollback retains its
separate lifecycle policy.

The local resource order is:

```text
original page pin -> page-state read lock -> row write latch -> CDB entry guard
```

Active/Frozen cleanup uses the ordinary inverse, including paired Frozen
mutation-version updates. Transition cleanup permits existing Lock/Delete
inverses only. Its CDB guard validates an active Ref to the same status allocation
before executing a synchronous, infallible engine closure. No await, I/O,
other page/index acquisition, or user callback runs inside that closure.

A surviving same-owner active main predecessor keeps the Ref. Final ownership
release requires restoration of a live row and removes only that same guarded
entry. Index branches and forward links confer no main-row ownership. Original
cold claims remove only their own active marker. Absent, foreign, committed,
or terminal ownership fails before row mutation. Foreground cold claims now
reject consumed same-transaction rows instead of registering duplicate undo;
maintenance `put_ref` retains its idempotent contract.

Forward restoration has separate permission from foreground publication. It
validates the exact reachable source, active owner, row, kind, and index slot.
A transitioned source must be Delete with its live deleted state intact.
Restoration updates only the recorded slot before-image and leaves source undo,
physical deletion, and CDB ownership untouched.

Deferred forward execution still waits for authoritative cold routing when it
needs an LWC row. Cold finalization validates the newest statement-owned Lock.
For hot-origin locks it reopens the original page, requires Transition, validates
the exact head and CDB owner, then marks the page dirty, sets the live bit,
increments the count once, and changes the same undo to Delete under the latch.
Its original page identity is retained. Cold Delete redo still records `None`.
Failures leave either a valid Lock or a valid Delete for ordinary rollback.

Checkpoint revalidation and marker installation retain the page-state write
lock. Cleanup that wins while Frozen invalidates stale plans. Cleanup that
observes Transition sees its markers installed, changes only live metadata,
and cannot change prepared membership or columns. LWC encoding, split retries,
and secondary-index collection keep the prepared bitmap. Root publication
never recreates a marker removed by cleanup.

The active writer remains registered until required rollback completes.
Retirement is ordered after publication, so
`Global_Min_STS <= writer.STS < retirement.CTS` protects the original generation.
Captured readers independently protect retained pages. Eviction preserves the
generation and version map and can require reload; it is not reclamation.

Safe cleanup can finish after engine poison. Access or invariant failures leave
unresolved ownership with the existing statement, terminal/abandoned, or
failed-precommit policy owner. Those owners retain residuals before fatal
publication. Incoming Fatal reports pass through statement and terminal cleanup
unchanged. Runtime rollback failures become RollbackAccess and return the cached
first fatal reason after publication.
Row rollback no longer participates in the transition route-or-poison wait
family; foreground routing and existing buffer access waits remain unchanged.

## Implementation Notes

Implemented both retained-page ownership repairs and verified physical,
checkpoint, cancellation, failure, reclamation, and restart behavior.

The ownership audit tightened fresh foreground cold claims and known-cold
mutation admission. Consumed rows cannot gain a second independently removable
undo. Unconditional CDB removal is now available only in test fixtures.

The poison review found that `EnginePoisoner::poison` intentionally returns its
caller's local failure while caching the first fatal. Rollback policy owners
call `EnginePoisoner::poison_and_get_first()` only for Runtime cleanup failures,
after retaining residuals and promoting the report to RollbackAccess. Incoming
Fatal reports bypass publication and preserve their original classification and
sources, even if another first Fatal is already cached.

The follow-up propagation fix constructs `StorageIo -> IoError -> BackendError`
at the shared worker before capturing and distributing one failure through its
request completions. Evictable and readonly pool access, persisted index/file
operations, and their rollback, purge, checkpoint, and catalog consumers now
preserve the Fatal arm. BufferPool, index cursor, and index rollback traits
expose associated native errors; fixed pools retain Runtime-only errors. No new
wait family or public signature was introduced.

Stress testing exposed a fixture prerequisite: GC horizon publication can
precede committed undo-chain trimming. Fixtures that assert an initially empty
undo chain now wait for production purge handoff and completion before arranging
the writer/checkpoint race. No sleep, retry, timeout, or runner change was used
to make the predicate true.

New tests reuse Frozen-page setup, checkpoint/deferred pause hooks, statement
settlement, read-failure injection, and normal purge waits. A narrow test-only
helper spills an existing page through production writeback so exact-generation
reload can be exercised deterministically. Existing source-link and
memory/catalog tests continue to cover their shared execution paths.

Validation completed in this worktree:

- Workspace nextest: 2,011 passed.
- Alternate `libaio` storage nextest: 1,895 passed.
- Required transition/rollback/Frozen focus: 83 passed. Expanded focus including
  deferred/CDB/split tests: 98 passed.
- Ten deterministic race regressions: all 100 stress iterations passed without
  retries after correcting the purge-completion prerequisite.
- Formatting and strict Clippy passed for the default and `libaio` backends.
- Branch style audit against `origin/main`: 67 Rust files passed.
- Unsafe inventory and public-error audit were refreshed; both were unchanged.

No implementation obligations were deferred. Exact-page cleanup may reload an
evicted page where the former implementation only removed a marker; this is a
necessary correctness cost. Obsolete rollback pivot polling and route waits
were removed. No permanent retention mechanism or durable-format change was
needed.

## Impacts

Row undo ownership, row-latched cleanup, table rollback, CDB reconciliation,
deferred mutation finalization, and transaction cleanup policy were updated.
The checkpoint image, GC, and shutdown documents now describe the replacement
cleanup contract.

## Test Cases

- Cleanup completes on retained Transition pages while publication is paused;
  moved-row rollback removes the replacement and restores the source.
- S1 Delete survives S2 replacement failure, including exact forward-slot
  restoration, CDB ownership, deleted count, and competitor write conflict.
- A same-owner main predecessor retains its marker. Invalid generation, row,
  table, head, owner, kind, delete bit, and CDB states fail before an inverse.
- Deferred Lock completion preserves allocation/page identity and updates its
  bit/count exactly once. Retained point access and captured scan descriptors
  remain readable after commit or rollback.
- Deferred cancellation and post-finalization duplicate-key failure discharge
  provisional ownership through normal settlement; callbacks run once.
- Prepared bitmap and borrowed columns stay unchanged across deletion and
  rollback. Real LWC splits and unique/non-unique DiskTree membership survive
  cleanup before encoding; no-index checkpoint coverage remains passing.
- Evicted original pages reload through production access. Injected reload
  failure retains undo and preserves the existing first fatal.
- Backend failures preserve StorageIo, I/O, and backend source frames across
  submitted and unsubmitted completion paths; waiters share one captured failure.
- Statement and terminal row/index rollback retain unresolved undo and forward
  StorageIo even when engine health holds an earlier CheckpointWrite fatal.
- Terminal observer cancellation and failed-precommit completion respect page
  access and ownership settlement. Safe cleanup after checkpoint poison is
  distinguished from marker/access failures requiring FailedRetained.
- Active readers retain original generations until release; production purge
  then reclaims them. Restart verifies deferred commit, rollback, and statement
  failure outcomes with scans and unique lookup.

## Open Questions

None. Shared transition ownership and broader CDB history remain outside this
completed correction.
