---
id: 000300
title: Fix Stale Unique Read-Current Lookups Across Row Replacement
status: implemented
created: 2026-09-07
github_issue: 1055
---

# Task: Fix Stale Unique Read-Current Lookups Across Row Replacement

## Summary

Unique read-current mutation now follows the surviving hot owner when another
transaction transfers a selected key before row inspection. Delete and Update
undo carry per-index forward links, and destination-owned journals restore
those links during rollback. Selection checks ownership at every successor and
invokes the callback at most once after acquiring the current matching row.

The original optimistic index observation remains available throughout forward
traversal. Successful hot acquisition needs no post-row index validation;
terminal rejection validates that evidence because rollback can erase a
captured destination's claim. Cold selection retains index-based fallback.
Snapshot reads continue to use backward undo and IndexBranches.

Checkpoint-transition cleanup defects discovered during review remain explicitly
deferred to backlog 000199. This task does not claim to fix those defects.

## Context

A lookup can capture `k=7 -> RowID 100` before another transaction deletes 100,
inserts replacement 200, publishes `k=7 -> 200`, and commits. Treating the
deleted image at 100 as logical absence previously skipped updates or produced
spurious duplicate-key failures. Read-current mutation must instead acquire 200
and use its current values.

An in-place rekey has the same problem: changing `{100, k=1}` to `{100, k=2}`
and inserting `{200, k=1}` in one transaction requires a forward link on the
Update that removed key 1. A backward IndexBranch on 200 serves snapshots but
cannot help a current reader already inspecting 100 find the new owner.

Task 000299 introduced the public callback API and deferred this inherited
defect. Task 000008's Delete/Insert and backward-branch topology remains intact:
keyless scans never traverse cross-row forward links. This task has no parent
RFC and introduces no public API or durable-format change.

Source Backlogs:

- `docs/backlogs/closed/000196-resolve-stale-unique-point-lookups-across-row-replacement.md`

Issue Labels:

- type:task
- priority:high
- codex

Related records are [task 000299](000299-unify-unique-key-mvcc-mutation-api.md),
[task 000008](000008-replace-move-with-delete-insert.md),
[transaction semantics](../transaction-system.md), and
[secondary indexes](../secondary-index.md). Transition cleanup retains the
policy from [task 000272](000272-row-undo-rollback-through-page-transition.md),
with its newly identified gaps recorded in backlog 000199.

## Goals

- Select the surviving current owner across hot moves, delete/reinsert, and
  in-place key transfers in user-table and MemTable unique mutation.
- Follow multiple successors and repeated RowIDs using the newest relevant
  departure, with ownership admission before interpreting mutable row state.
- Restore eligible source-link changes before undoing their destinations,
  preserving earlier successful statements and pending cleanup ownership.
- Preserve callback-at-most-once behavior, active-owner conflicts, preparing
  waits, missing-insert races, snapshots, and catalog contracts.
- Avoid post-row index validation on successful hot acquisition while retaining
  the evidence required for terminal rejection and cold boundaries.
- Enforce exact retained IndexRef identity instead of treating missing runtime
  metadata as an ordinary unresolved lookup.

## Non-Goals

- Forward links in CDB, redo, persisted rows, or recovery; cross-row forwarding
  for snapshots; altered range-selection semantics.
- Links across an independent committed absence interval, mutation of foreign
  committed source undo, path compression, or a new logical-row identity.
- Gap locks, callback retries, automatic conversion of a missing insertion race
  into an update, or starvation freedom under unlimited concurrent transfers.
- Shared user/mem/catalog mutation-engine consolidation, deferred to
  [backlog 000198](../backlogs/000198-share-unique-mutation-execution-across-user-and-mem-catalog-tables.md).
- Revised transition ownership and retained-page rollback, deferred to
  [backlog 000199](../backlogs/000199-repair-dangling-row-undo-ref-and-deferred-lock-to-delete-mismatch.md).

## Rejected Alternatives

- A Move-only or Delete-only signal misses ordinary delete/reinsert or in-place
  key transfers. One-hop forwarding likewise cannot resolve complete hot chains.
- Returning WriteConflict whenever RowID changes rejects valid committed
  transfers; an optimistic leaf invalidation can also reflect unrelated keys.
- Eliminating terminal index validation would lose the owner restored when a
  captured uncommitted destination rolls back. Extra routing retention after
  rollback requires additional lifetime rules, so original evidence is retained.
- Cold forward metadata would add CDB memory and ownership complexity. Existing
  cold reselection remains the chosen tradeoff.

## Plan

### Observed lookup and selection

BTree lookup can return its value together with the original validated
PageOptimisticGuard. The observation retains frame lifetime and generation but
holds no shared/exclusive leaf latch across row access or IO. Validation checks
the captured evidence without reading payload or refreshing its version.

UniqueSecondaryIndex always returns the MemTree observation, including a
witnessed miss when an immutable pinned DiskTree supplies the candidate.
A new MemTree owner or delete shadow supersedes that disk result. Live and
delete-shadow MemTree hits both terminate tree dispatch. Leaf mutation,
splitting, compaction, frame reuse, and RowID ABA invalidate old evidence.

CurrentRowSelection keeps its observation and snapshot timestamp fixed while
its position records the candidate RowID and index/forward origin. Its
`decide()` method only classifies concrete RowInspection findings;
`advance()` changes the position. Returning to a previous RowID still counts
as forward traversal.

HotRowMutator admits ownership before reading the mutable key or delete state.
A foreign active owner conflicts; a preparing owner waits; transition releases
the attempt and waits for cold routing. A live matching row ends selection.
Otherwise the newest matching Delete/Update departure supplies its successor,
or terminal rejection validates the original index observation. Invalidation
starts a fresh root-bound lookup; stable terminal hot rejection returns missing.

Only the original index candidate has the strict confirmed
`delete_cts < reader_sts` early-missing shortcut. Arbitrary Update/Lock head
timestamps, equality, and forwarded-target timestamps cannot substitute for it.
Invalid page/row identity is an invariant violation rather than an endless retry.

Composite misses validate their observation directly. Direct MemTable misses
use the already-validated MemTree result. Cold inspection and conditional claims
preserve deletion CTS, preparing ownership, and same-transaction consumed state.
Markers precede durable deletion bits. Stable newer cold deletion conflicts;
other unresolved cold rejection follows the retained selection evidence.

A rejected cold or missing forward target requires an invalidated observation
before reselection; unchanged evidence at that boundary violates the transfer
contract. Every continuing selection iteration releases row access, yields, and
checks engine health. There is no retry budget or visited-RowID cutoff.

### Forward metadata and publication

Delete and Update use plain lazy ForwardLinks, represented by an optional boxed
slice of ForwardHint values containing exact IndexRef and destination RowID.
Source keys come from the row image and Update before-images; links retain no
key-value copy. IndexRef protects against physical slot reuse.

HotForwardSource identifies the source RowID, exact page generation, exact undo,
and owning transaction. Discovery can find an Update beneath newer entries but
must prove ownership of that exact entry, not merely of the newest head.

Each destination's OwnedRowUndo keeps a restoration journal outside its boxed
snapshot-visible RowUndo. A record identifies the source, index, and optional
previous hint. Multiple indexes may claim keys from different source rows.
An empty link store or journal allocates no backing storage.

Publication initializes the destination and backward branches, exchanges the
index mapping, registers ordinary index undo, records the old source slot, and
installs the new link synchronously before another await. Source page access is
prepared before the exchange; source-row latching protects link publication.
Committed foreign sources and transitioned sources receive no new hot links.

Snapshot projections borrow only operation before-images and backward-chain
fields. Mutable projections touch forward storage alone, avoiding a broad
mutable borrow that overlaps a cross-row snapshot's references.

### Unique-index maintenance and rollback

Unchanged unique keys in a physical move retain prebuilt backward branches and
direct RowID exchange. Changed keys use ordinary unique insertion followed by
deferred masking of the old key. Insertion discovers the actual predecessor,
builds its backward history, and publishes any eligible forward link.

That common insertion path handles absent keys, live duplicates/conflicts,
reusable deleted or updated history, and stale mappings without usable history.
It also handles a changed key whose previous index mapping still points to the
move's own source RowID; the former special case could miss a buried Update
departure.

Index rollback precedes row rollback. Each destination restores source slots
in reverse publication order before its own row effect is undone. Removing a
new slot and restoring an overwritten slot share the same journal. The current
destination and unfinished records remain owned across awaits and cancellation.

Shipped transition rollback still follows the inherited route policy: wait for
publication when source access reports Transition/PageMissing, skip hot-only
source restoration below the published pivot, and use CDB cleanup for cold-routed
row undo. The retained-page consequences of that policy are deferred in
backlog 000199; it is not a proof that every old-page undo reference is unlinked.

## Implementation Notes

Implemented replacement-aware unique read-current selection with multi-hop
Delete/Update forwarding, exact source ownership, destination-owned link
rollback, and original-index validation at terminal rejection boundaries.

Review replaced the initial one-hop Delete-hint approach with complete hot
departure traversal. Original index evidence was retained even after forwarding
because rollback can expose absent or unrelated older destination history.
Successful ownership remains the single callback boundary.

UniqueMutator now separates selection from owned hot/cold and missing actions.
MemTable shares selection and hot admission, while its legacy mutation APIs
remain. Review also made resolved IndexRef access assert exact retained
metadata/runtime identity. Selector admission, inactive purge generations,
conditional DDL, and persisted inputs retain their boundary checks.

Changed-key move maintenance was simplified to ordinary insertion plus deferred
old-key masking in both table families. Dedicated regressions cover absent,
deleted, and rekeyed predecessors, mixed changed/unchanged indexes, snapshot
history, later unique failures, and restored earlier-statement forward links.

On the tested target, layout assertions record RowUndoKind at 40 bytes,
ForwardLinks and ForwardHint at 16 bytes each, RowUndo at 136 bytes, and
OwnedRowUndo at 32 bytes. Tests check lazy storage and reuse of existing slots.
Lookup counters verify one initial lookup and no post-row validation for
successful hot chains, including unrelated leaf changes. No benchmark
infrastructure or throughput claim was added.

Two real checkpoint-transition bugs were found by code inspection and explicitly
deferred by the user to backlog 000199: cold-routed rollback can free an undo
still referenced by a retained hot page, and deferred cold finalization can
rewrite an originally hot Lock to Delete without its row latch or delete bit.
Their fix requires coordinated transition/CDB/rollback changes. Existing tests
do not establish those missing invariants, and task resolution does not close
that follow-up or claim its candidate design was implemented.

Backlog 000198 preserves the separately deferred user/mem/catalog execution
refactor. Both follow-ups include source-task linkage, deferral reasons,
findings, and design direction. Source backlog 000196 is closed as implemented.

Final verification on 2026-09-10 used the final implementation and test-helper
style fixes: the branch style gate passed for 31 Rust files, including formatting
and strict workspace Clippy; all 1,992 workspace tests passed. Selected
replacement, changed-key move, rolled-back destination, MemTable, and borrowed
snapshot-field regressions passed 20 stress iterations. All 1,876 storage tests
passed with the alternate libaio backend.

## Impacts

The runtime change spans unique index observations, current-row selection,
hot undo payloads, index publication, and rollback journals. Shared snapshot,
checkpoint, purge, and recovery consumers were adapted to the new payloads.
Transaction and secondary-index documentation plus the unsafe/error audit
inventories were updated.

Public mutation APIs, catalog redo contracts, schemas, on-disk formats, and
recovery representations remain unchanged. Forwarding trades lazy undo storage
for fewer repeated lookups on successful hot chains. Cold fallback and existing
transition waits remain, with their cleanup defects tracked separately.

## Test Cases

- Captured replacement before inspection, active/preparing commit and rollback,
  correct latest values, and one callback invocation.
- Delete/reinsert, physical moves, in-place and buried Update departures,
  multiple indexes/sources, repeated keys, and RowID revisits.
- Failed statements, overwritten links, whole/prepared rollback, cancellation
  during restoration, and captured destinations whose claims disappear.
- Backward snapshot reconstruction while forward fields change, keyless scans,
  MemTable/catalog mutation, and changed-key predecessor selection.
- Strict deletion timestamp boundaries, cold claim races and consumed markers,
  durable-marker precedence, composite misses, and cold forward-target rejection.
- MemTree hit/shadow precedence, DiskTree results retaining MemTree evidence,
  original-leaf ABA/split/compaction/frame invalidation, and exact index identity.
- Invalid row-page invariants, cooperative yield/poison/cancellation behavior,
  lazy allocation, measured layouts, and successful-path lookup counts.
- Workspace and alternate-backend integration retain existing checkpoint,
  rollback, purge, and recovery coverage; backlog 000199 identifies the
  additional retained-page regressions still required.

## Open Questions

No unresolved design question remains for the implemented selection protocol.
[Backlog 000198](../backlogs/000198-share-unique-mutation-execution-across-user-and-mem-catalog-tables.md)
tracks execution consolidation.
[Backlog 000199](../backlogs/000199-repair-dangling-row-undo-ref-and-deferred-lock-to-delete-mismatch.md)
tracks both accepted, deferred transition bugs and the unapproved proposal to
reconcile retained-page undo with CDB ownership. Cold forward links and stronger
progress guarantees remain outside the selected scope.
