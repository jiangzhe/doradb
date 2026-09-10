---
id: 000300
title: Fix Stale Unique Read-Current Lookups Across Row Replacement
status: proposal
created: 2026-09-07
github_issue: 1055
---

# Task: Fix Stale Unique Read-Current Lookups Across Row Replacement

## Summary

Fix unique row selection for read-current operations when a captured physical
RowID stops owning the selected key before row inspection. Make forward links
in hot Delete and Update undo authoritative for supported same-transaction key
ownership transfers. Record each link's previous value on the destination undo
so statement and transaction rollback restore the source link before removing
the destination effect.

Once selection reaches hot row state, follow the latest relevant committed or
same-transaction successor until acquiring a matching current row or reaching
the end of the chain. Remove the one-hop limit. Successful acquisition needs no
post-row index validation; terminal rejection retains original-index validation
and fresh lookup on invalidation. The initial lookup may capture an uncommitted
destination whose claim disappears on rollback, exposing missing or unrelated
older history. Preserve cold and composite-miss fallback as well. This task
does not add forward links to ColumnDeletionBuffer (CDB).

Successful ownership remains the callback boundary; the callback runs at most
once. Snapshot readers retain backward undo and IndexBranch traversal and never
follow current-write forward links. The task remains a proposal until the
revised implementation and acceptance checks are completed.

## Context

Consider a unique key `id=7` initially mapped to hot RowID 100 with
`counter=10`. A read-current increment captures RowID 100 and pauses before row
ownership. Another transaction changes the counter to 20 and grows a value
enough to force physical replacement: delete 100, insert 200, update the index
to `7 -> 200`, then commit. Rejecting the deleted image at 100 must not invoke
the callback with None. Correct selection acquires 200 and increments 20 to 21.
The key exists in both committed states.

In-place updates create the same lookup race without deleting the source row:

1. Reader B captures the unique mapping `k=1 -> RowID 100`.
2. Writer A updates `{rowid=100, k=1}` to `{rowid=100, k=2}`.
3. A inserts `{rowid=200, k=1}` in the same transaction and commits.
4. B reaches row 100 and finds the committed key 2 instead of expected key 1.

The IndexBranch on row 200 points backward to row 100's old version. It lets an
older snapshot reconstruct key 1 from the new owner, but a reader already at
100 cannot use that branch to discover 200. The Update undo recording the
departure of key 1 therefore needs a forward link to 200. B can find that
departure, follow its link, and acquire the latest row without another index
lookup.

A committed forward link cannot subsequently be rolled back by its publishing
transaction. Statement rollback can nevertheless invalidate a link before that
transaction commits: statement one deletes 100; statement two inserts 200 and
publishes a link, then fails another unique constraint. Statement two rolls
back while statement one's Delete survives. The source link must be restored
as part of that rollback, otherwise a later commit exposes a stale successor.

A separate race remains even with correctly restored forward links: B's initial
index lookup can capture A's uncommitted destination 200, then A rolls back
before B inspects it. The index again points to the original owner 100, while
200's claim undo has disappeared. For an inserted destination there may be no
history; for an updated destination rollback can expose an older terminal or
forwarding history of the same key. That history cannot prove the current key
is missing. Retain the original optimistic observation and validate it before
finalizing terminal rejection, then reselect on invalidation. This is the
selected approach; preserving additional routing metadata after rollback is
outside the task.

Task 000299 introduced the public callback API and deferred the inherited stale
lookup defect. Task 000008's Delete/Insert and backward IndexBranch topology
remains a constraint: keyless scans must not gain cross-row MainBranch
traversal. A snapshot whose STS precedes the transfer CTS must reconstruct the
old image, while read-current selection acquires the surviving current owner.

The branch's preceding implementation retained an optimistic MemTree leaf
observation, tried one optional Delete hint, and returned to the index when
that hint could not resolve the candidate. This revised plan replaces that hot
one-hop restriction with complete, rollback-maintained Delete/Update links.
Its observed-lookup machinery remains the proof for terminal hot rejection,
cold classification, and composite misses.

Source Backlogs:

- `docs/backlogs/000196-resolve-stale-unique-point-lookups-across-row-replacement.md`

Issue Labels:

- type:task
- priority:high
- codex

Relevant references:

- [Backlog 000196](../backlogs/000196-resolve-stale-unique-point-lookups-across-row-replacement.md).
- [Task 000299](000299-unify-unique-key-mvcc-mutation-api.md) and
  [Task 000008](000008-replace-move-with-delete-insert.md).
- [Transaction system](../transaction-system.md),
  [secondary-index design](../secondary-index.md), and
  [unit-test process](../process/unit-test.md).
- `table/unique_mutate.rs`: current row selection, retained
  observations, the existing one-hop restriction, and callback admission.
- `trx/row.rs`, `trx/undo/row.rs`, and `table/hot.rs`: row ownership, main-chain
  key reconstruction, Delete payload access, and physical mutation.
- `table/access.rs` and `table/mem_table.rs`: duplicate-owner linking, in-place
  key changes, move updates, and unique-index publication.
- `trx/stmt.rs` and `RowUndoLogs::rollback`: index-before-row settlement and
  reverse row-effect order, including cancellation retention.
- `index/btree/mod.rs`, `index/unique_index.rs`, and
  `index/secondary_index.rs`: observed lookup and retained MemTree evidence.
- `table/deletion_buffer.rs` and `table/page_transition.rs`: cold ownership
  classification and publication of the authoritative physical route.

This is one runtime correctness task with no parent RFC. It changes neither the
public mutation API nor durable formats. Authoritative cold forwarding and any
new retention scheme are outside this revision.

## Goals

- Correct unique read-current selection in the public callback executor and
  MemTable update/delete/upsert paths.
- Cover same-key hot moves, same-transaction delete/reinsert, and in-place key
  transfers through exact Delete or Update departure entries.
- Follow multiple hot successors, including repeated key changes and RowID
  revisits, without index revalidation when a matching current row is acquired.
- Validate terminal hot rejection against the original lookup, covering an
  uncommitted destination whose rollback erased or changed its claim history.
- Make every supported surviving transfer publish its forward link and restore
  that link correctly on statement, whole-transaction, and prepared rollback.
- Preserve plain mutable payloads, exact IndexRef identity, and lazy allocation.
- Retain index validation for composite misses and cold selection, including
  failed CDB claims after block loading and hot-to-cold routing boundaries.
- Preserve one callback invocation, active-owner conflicts, preparing waits,
  cancellation cleanup, and ordinary insert races after genuine absence.
- Preserve snapshot visibility, backward IndexBranches, weak range selection,
  catalog contracts, and existing checkpoint/purge/recovery invariants.

## Non-Goals

- Adding forward traversal to snapshots or changing range selection semantics.
- Forward metadata in CDB, persisted pages, redo, or recovery.
- Installing links in another transaction's committed Delete or Update undo.
- Connecting separate transactions across a genuine committed absence interval.
- A new Move undo kind, logical-row identity, path compression, persistent
  routing after rollback, or a separate transaction-wide undo log for links.
- Gap/predicate locks, callback retries, or treating an insertion race after the
  callback as an implicit update.
- Changing the stable cold-deletion CTS-based Missing/WriteConflict policy.
- New retention horizons, production wait families, storage backends, or
  retained benchmark infrastructure.

## Rejected Alternatives

- Delete-only hints omit in-place key transfers. A one-hop limit also forces
  index fallback for an otherwise valid committed replacement chain.
- Leaving hint changes outside rollback permits a surviving source undo to
  expose a failed statement's destination after commit.
- A single old RowID on destination undo cannot identify an older Update among
  several departures on one source row, or represent different source rows
  claimed through different unique indexes.
- Mutating an entire shared Update payload would overlap snapshot borrows of
  its before-images. Project mutable access to the link field only.
- Returning WriteConflict merely because a committed transfer changed RowID
  gives up the required current-owner selection behavior. Unrelated changes in
  the observed leaf are also insufficient to prove a selected-key conflict.
- RowID equality or a visited-RowID stop rule does not distinguish repeated key
  ownership episodes. Traverse the newest relevant departure instead.
- Eliminating every hot rejection check loses the owner restored when an
  initially captured uncommitted claim is rolled back. Retaining routing after
  rollback would need new lifetime and key-reuse rules; use the existing index
  observation at the terminal-result boundary instead.

## Plan

### 1. Define authoritative hot transfer and terminal-removal contracts

A forward link records where one exact source version's key went in the same
writer transaction. Every supported transfer that survives commit must publish
that link. An absent link on a proven latest departure means that version's
transaction removed the key without a surviving successor; it is not a
best-effort failure to record an eligible transfer. This version-level fact
alone does not establish absence for the initial index lookup, because rollback
can expose an older ownership episode. Terminal rejection requires retained
index evidence unless the original committed-deletion shortcut applies.

Supported sources are hot Delete entries and hot Updates that change the
selected unique key. Supported destinations are inserted or updated hot rows
owned by the same transaction. Cover space/frozen-page moves, delete/reinsert
across statements, and rekeying an existing row to claim a departed key.

A committed removal without a same-transaction successor establishes an absence
boundary. A later transaction can insert that key as an ordinary race; it does
not mutate the earlier committed source. The missing observation remains
without a gap lock. A surviving transfer within one commit has no such absence
boundary and must select its successor.

Ownership admission precedes reading mutable key/deletion state. A foreign
active owner conflicts; a preparing owner settles through the existing
poison-aware wait. Apply these rules to authoritative successors as well as
initial candidates. Same-transaction readers may observe their own serialized
writes. A confirmed committed source cannot be rolled back afterward.

### 2. Extend plain undo payloads and exact source discovery

Introduce `UpdateUndo` with the existing `Vec<UndoCol>` before-images and optional
forward-link storage. Delete and Update use the same lazy representation:
`Option<Box<[ForwardHint]>>`, with one current destination per exact IndexRef on
each departure entry. Keep `ForwardHint { index: IndexRef, row_id: RowID }`.
No OnceLock, mutex, or other interior-mutability wrapper belongs in the payload.

A Delete's row image supplies its key. For Update, reconstruct the selected key
from current row values and immutable before-images until locating the latest
departure of that key. Do not add a retained `Vec<Val>` to links or restoration
records. Exact IndexRef, rather than IndexSlot alone, protects against index
slot reuse.

Extend forward-source discovery to return the source RowID, versioned page
identity, and exact undo identity. The relevant Update may be below newer
entries, even newer entries from the same writer. Prove the source entry's own
transaction identity through its main-branch status; ownership of the current
head alone is insufficient. Do not mutate foreign or already committed undo.

Reuse the backward unique-owner discovery and key reconstruction machinery
where appropriate, while keeping forward publication distinct from snapshot
branch creation. An older matching version is not automatically the latest
departure or an eligible publication source.

### 3. Carry link restoration on the destination undo

Add a lazily allocated restoration collection to the common OwnedRowUndo wrapper
so both Insert and Update destinations can retain restoration information.
Keep it beside the boxed RowUndo, outside all snapshot-visible fields. Its
conceptual entry is:

```rust
struct ForwardLinkUndo {
    source: ForwardSource, // RowID, page generation, exact source undo identity
    index: IndexRef,
    previous: Option<ForwardHint>,
}
```

The source descriptor may adapt the current HotForwardSource representation;
its authority must support exact owned Update and Delete entries below the
head. `previous: None` removes a newly installed link during rollback, while
`Some(link)` restores the overwritten value. Store the affected slot's
before-image rather than copying the whole source collection.

Allow multiple records: different destination indexes can replace different
source rows, and multiple changes must unwind in reverse publication order.
Keep storage unallocated when no link is published. Source undo must remain
owned and valid until its destination's restoration finishes; verify this
against normal and deferred row-effect ordering.

These records are runtime rollback metadata, not snapshot history or owning
references to destination versions. Keep them outside snapshot projections and
out of redo/persistent representations.

### 4. Publish links with complete rollback registration

Initialize the destination and required backward IndexBranches before making it
the latest index owner. Pin or resolve source access, identify destination undo,
and prepare necessary bookkeeping before the index exchange.

After a successful exchange, register normal index undo, capture/register the
previous source link on the destination undo, and install the new source link
synchronously before another await or prepare boundary. Hold the source row
write latch while reading/replacing its link. Publication must prove both
source-entry and destination ownership.

Do not silently skip required publication while the eligible source remains
hot. A source that is transitioning uses the existing authoritative route
protocol and cold boundary. Failed index exchanges publish no link. Failure on
a later unique index restores earlier published links through ordinary rollback.

Hot move preparation installs backward IndexBranches only for unchanged unique
keys, whose index maintenance uses direct RowID exchange. A changed unique key
uses ordinary unique-index insertion, followed by deferred deletion of its old
key. Insertion discovers the actual previous owner and supplies both backward
history and any eligible forward link; there is no special case for an index
entry pointing to the move's own source RowID. This also covers a previously
removed key whose departure is a buried Update. User-table and MemTable move
helpers share this protocol through their existing insertion implementations.

Keep payloads plain by using narrow field projections. Cross-row snapshots may
retain Update before-image or backward-chain references without the source row
latch. Mutable access must borrow only forward storage, never an entire Update
or RowUndo encompassing those references. Extend the existing snapshot view and
document any changed unsafe projection contracts. No row write latch or
mutable payload borrow crosses an await.

### 5. Restore source links before reverting destination effects

Preserve index-before-row rollback. For each destination row undo, restore its
link records in reverse publication order before reverting the row image or
unlinking/freeing that undo. Locate the exact source entry under its row latch;
do not restore whichever Update/Delete happens to be the current head.

Keep the destination undo vector-owned across awaits and failures. Remove a
restoration record only after its source change has been restored, with no
cancellation gap between those steps. Remaining records and row undo must stay
available to mandatory cleanup or fatal retention. Restore links without
simultaneously retaining the destination row write latch.

Integrate this before the rollback dispatch that handles hot rows, cold
markers, and missing original pages. A destination moving to cold must not
bypass restoration of links on still-hot source rows. If the source itself
transitioned, require authoritative route proof before discarding a hot-only
restoration: there must be no current hot link left reachable by selection.
Do not treat a failed page lookup alone as permission to skip restoration.

Cover ordinary statement failure, prepared rollback, whole-transaction
rollback, cancellation, and deferred mutation effects. Earlier successful
statements retain their original links and row effects. Rolling back a source
entry happens only after its dependent destination restorations have settled.

### 6. Select hot current rows by complete forward traversal

Replace the current one-hop/unconfirmed-hint state with authoritative traversal
shared by UniqueMutator and MemTable unique operations.

Keep fixed lookup evidence and snapshot timestamp in `CurrentRowSelection`.
Its `CurrentRowPosition` owns both the candidate RowID and its index/forward
origin. `decide(&self, RowInspection)` returns a decision without changing any
selection fields; `advance(&mut self, successor)` changes only the position.
Use concrete inspection variants for missing routes, successors, unresolved hot
rows, cold deletion, key mismatch, and consumed cold rows. Composite index misses
validate directly without a default rejection or selection object. Direct
MemTable misses need no additional validation after the validated MemTree lookup.
Capture the fixed key slice in `UniqueMutator` once, alongside its runtime and
selected index.

Hot admission returns explicit `HotRowLock` variants instead of a rejection
struct with optional timestamp and successor fields. `DeletedBeforeSnapshot`
establishes absence for the original index candidate, so callers take their
missing-result path directly. `Successor(RowID)` supplies the next row to inspect.
`Unresolved` requires validation of the original index observation: stale means
retry, and stable means missing. Metadata and runtime presence for an exact
`IndexRef` resolved against the retained layout are internal invariants, enforced
by release assertions instead of unresolved/missing results or runtime errors.
The layout constructor also asserts full metadata/runtime index identity.
Stale selectors, inactive purge generations, conditional DDL publication, and
persisted input keep their boundary validation. Ownership, conflict, prepare,
and transition retain their separate variants. Callers match the result by value,
release the attempt before waiting, and advance only after releasing the source
row access.

1. Resolve physical location and apply ownership admission.
2. If the latest row is live and matches the requested key, acquire it and end
   selection. No index revalidation follows.
3. Otherwise reconstruct the selected key backward along the main chain to the
   most recent departure. For a matching deleted image, use its relevant Delete;
   for a changed key, find the relevant Update, possibly below newer entries.
4. Read only that departure's forward slot for the exact IndexRef. Follow its
   surviving link through normal RowID/page-generation routing and repeat.
5. If the latest relevant departure has no successor, or no matching departure
   remains, validate the original index observation before returning Missing.
   A stable observation permits rejection; invalidation starts a fresh root-bound
   lookup. Do not search older occurrences for a stale link. Apply this check
   even after traversing several hot links.

Remove the one-hop cap. Successful acquisition skips post-row index validation;
terminal rejection retains retries on original-leaf invalidation. Selecting the
current matching image before old history and
always using the newest relevant departure must handle key reuse and
RowID revisits without a static cycle. Do not stop because a RowID was seen
earlier or treat arbitrary missing history as a terminal-removal proof.

Keep the confirmed `delete_cts < reader_sts` shortcut for the original
index-selected Delete/CDB candidate. Its index effects preceded the reader's
lookup, so it can establish early absence. Do not apply an arbitrary Update
head's timestamp or a forwarded target's timestamp as that original-candidate
proof. Equality and unconfirmed status do not take the shortcut.
For hot rows, `try_lock_current()` classifies this proof once after ownership
admission and returns `DeletedBeforeSnapshot` without reading successor storage.
No hot deletion timestamp leaves admission. `CurrentRowSelection::decide()`
retains the cold-deletion timestamp policy and original-index validation for
`HotUnresolved`, while `Successor(RowID)` produces a hint without validation.

Release source access before following another row. Preparing and transition
settlement retain the existing wait families and must reclassify authoritative
state after wakeup. All traversal and retries precede the callback. Preserve
provisional-lock cancellation, application errors, empty-update RowID behavior,
and ordinary missing-insert races.

### 7. Retain observed lookup for terminal rejection and cold boundaries

Retain the private observed lookup through BTree, GuardedUniqueMemIndex, and
UniqueSecondaryIndex. The BTree observation owns a PageOptimisticGuard with the
original validated leaf version, frame generation, and keepalive. It holds no
shared/exclusive leaf latch across row access or IO and never refreshes evidence
while keeping the old candidate. Keep ordinary value-only lookup for snapshots.

The observation always describes the MemTree leaf searched for the selected
key, including a miss when the pinned immutable DiskTree supplies the RowID.
A competing MemTree entry supersedes that disk result. MemTree live and
delete-shadow hits remain terminal for tree dispatch. Unrelated writes in the
same leaf may conservatively invalidate the observation.

Retain this evidence for terminal hot rejection, composite lookup misses, and
cold selection. Intermediate hot links and successful matching-row acquisition
do not consult it. An absent link on an old same-key departure is not stronger
proof than missing undo: either may follow rollback of the captured claim.
Release the observation before callback execution, retaining any separately
required root proof.

Cold row classification must preserve active/preparing ownership,
same-transaction consumed state, committed deletion CTS, and durable deletion
without a marker. Committed Ref and compact Committed forms agree, and markers
take precedence over the durable bitmap.

Apply existing validation/reselection before finalizing unresolved cold
rejection, both at initial inspection and after a failed conditional CDB claim
following block loading. A replacement can then be selected; a stable newer
committed deletion retains WriteConflict, while an already visible deletion
retains Missing. Failed claims install no undo, and earlier consumed markers
are not cancelled as newly acquired ownership.

Store no links in CDB. A rejected cold or missing forward target must validate
the original index observation before requesting fresh root-bound selection.
Invalidation permits retry; a valid observation is an invariant violation,
because a surviving transfer publishes its index target before another
transaction can follow the link. Rollback can expose an older link whose target
becomes cold after its RowID is copied, but it also invalidates the original
lookup. A changed leaf is a reason to reselect at this boundary, not proof of a
write conflict. Preserve composite-miss validation even though no row was reached.

### 8. Preserve lifetime, progress, and existing consumers

For a reader that captured a key before its uninterrupted transfer, its STS
precedes the transfer CTS. Verify that existing undo retention protects each
needed Delete/Update departure and that subsequent hot hops remain protected.
Source references in restoration records belong to the active writer and must
outlive their reverse-order use. No new retention horizon is planned.

Audit source/target checkpoint transitions, frozen-page accounting, CDB marker
promotion, purge, and recovery. Recent history required by a captured transfer
must not disappear into an unproven Missing result. Retirement of older history
requires the existing lookup/purge ordering proof or the explicit cold route
boundary. An actual failure of those lifetime guarantees is an implementation
blocker; terminal index validation must not conceal a missing required link or
an unsafe reference lifetime.

Snapshot consumers continue inverse Delete/Update reconstruction and existing
backward IndexBranches; they ignore forward storage and restoration records.
Adapt all payload matches and projections, including range mutation's shared
write helpers, without changing snapshot or range selection contracts. Preserve
catalog key-based redo and its operation-error invariants.

At the end of every selection iteration that continues with a forward hop or
fresh lookup, yield once and then check engine health. Settled prepare/transition
waits reach the same outer loop tail. Add no initial check or yield, keep no
attempt counter or budget helper, and return terminal results directly. Release
row guards before yielding while retaining index evidence needed for a forward
chain. Finite committed transfers must settle without a
static forwarding cycle; unlimited concurrent transfers need not provide
starvation freedom. Preserve the existing cancellation/cleanup owners and
prepare/transition wake predicates. Document progress producers and
poison/shutdown behavior for any refactored helper under repository wait-review
rules.

Update transaction and secondary-index documentation to explain authoritative
hot links, rollback restoration, terminal-result index validation, and the
separate cold fallback. Measure undo layout and lazy allocation costs. Follow the existing
test runner configuration and complete formatting, strict Clippy, and the
branch style audit as implementation validation; task resolution is separate.

## Implementation Notes

## Impacts

- `doradb-storage/src/trx/undo/row.rs`: UpdateUndo, common forward storage,
  exact source descriptors, destination restoration records, narrow snapshot
  projections, and reverse rollback integration.
- `doradb-storage/src/trx/row.rs`: key-departure reconstruction, source-entry
  ownership proof, source-link mutation/restoration, and snapshot adaptation.
- `doradb-storage/src/trx/stmt.rs`: destination bookkeeping access and
  statement/deferred-effect cleanup integration.
- `doradb-storage/src/table/hot.rs`, `table/access.rs`, and
  `table/mem_table.rs`: Update/Delete publication, multiple per-index sources,
  hot selection, and route-aware restoration.
- `doradb-storage/src/table/unique_mutate.rs`: remove
  one-hop hot fallback, follow authoritative successors, and retain explicit
  terminal/miss/cold observation boundaries and one callback invocation.
- `doradb-storage/src/index/btree/mod.rs`, `index/unique_index.rs`,
  `index/secondary_index.rs`, and `buffer/guard.rs`: preserve observed lookup
  contracts and original optimistic evidence.
- `doradb-storage/src/table/deletion_buffer.rs`: preserve detailed current-row
  classification and conditional claims without adding forward metadata.
- `doradb-storage/src/table/page_transition.rs`, `table/persistence.rs`,
  `trx/purge.rs`, and other undo consumers: payload adaptation and lifecycle
  validation, with no persistent representation changes.
- `docs/transaction-system.md`, `docs/secondary-index.md`, and
  `docs/unsafe-usage-baseline.md`: revised behavior, field-access contracts,
  and any changed unsafe inventory.

## Test Cases

Use production hooks, channels, and authoritative predicates rather than sleeps.
Reuse shared fixtures and table-driven schedules. The existing 300-row hot
fixture with a 48,000-byte growth can force a move; separately prove that the
in-place transfer fixture keeps RowID 100. Pause after index capture and before
row ownership. Preparing tests settle only after the production wait is
registered. State callback counts, selected RowIDs, and values explicitly.

1. **Original replacement race:** capture 100, commit replacement 200, then
   inspect 100. Both missing Skip and Insert policies receive the current row
   with counter 20, run once, and update it to 21. Active ownership conflicts
   before callback; preparing commit/rollback selects the surviving image.
2. **In-place Update forwarding:** capture `k=1 -> 100`; one writer changes
   100 to key 2 and inserts 200 with key 1, then commits. Select 200 using the
   exact Update's link with no post-row index validation or extra lookup.
   Paired snapshots reconstruct 100's old image through backward history.
3. **Buried departures:** insert newer payload-only/key-changing undo above
   the source Update before publishing its successor. Find the correct owned
   entry. Reject publication into a foreign committed entry beneath an owned
   head. Cover composite keys and no copied key payload in the link.
4. **Delete transfers:** same-statement where supported and cross-statement
   delete/reinsert, space-induced moves, and frozen-page moves. Verify complete
   publication for every surviving hot unique-key transfer.
5. **Multiple sources and indexes:** different keys of one source go to
   different destinations; one destination claims different keys from multiple
   source rows. Restore each affected slot independently. Preserve exact
   IndexRef identity across slot reuse.
6. **Long chains and key reuse:** follow more than one committed hot hop.
   Include successors that rekey in place, revisit the original RowID, reclaim
   a prior key, or finally remove the key. Select the newest relevant departure
   and terminate without stale-link cycles or callback repetition.
7. **Statement rollback before commit:** an earlier Delete/Update survives a
   later failed destination Insert/Update. Restore the source slot before the
   writer commits. A subsequent valid claim publishes the surviving successor;
   selection never follows the failed statement's target.
8. **Restoration order and previous values:** exercise both removal of a new
   slot and restoration of an overwritten slot, multiple publications, and
   failure after an earlier unique index succeeded. Check reverse order and
   exact undo identity. Earlier successful statement effects remain intact.
9. **Whole/prepared rollback and cancellation:** unwind dependent destinations
   before sources, including deferred write effects. Cancel or fail while
   restoration awaits source access; pending records remain owned and cleanup
   completes or retains them on the existing fatal path.
10. **Snapshot field safety:** retain a cross-row snapshot view borrowing Update
    before-images while the writer publishes and restores its forward storage.
    Verify old-row reconstruction and unchanged backward traversal. Include
    Delete storage and no broad mutable Update/RowUndo alias.
11. **Terminal absence and timestamp boundaries:** genuine removal with no
    surviving successor returns Missing after stable index validation. An
    intervening independent insertion can be selected on invalidation; insertion
    after the final missing callback remains an ordinary race. Preserve the
    original Delete/CDB strict CTS shortcut; cover equality, newer CTS,
    active/preparing states, and rolled-back Inserts.
    An arbitrary Update head's CTS is not a substitute for departure analysis.
12. **Cold fallback and composite lookup:** retain stable cold Missing/conflict
    behavior and replacement reselection at initial and late CDB claim races.
    Cover consumed markers, durable-only deletion, preparing settlement, and
    copied older forward targets reaching cold storage after rollback, with
    retry gated by original observation invalidation. Assert that rejected cold
    or missing forward targets with a valid observation fail as an invariant
    violation. Verify MemTree live/delete-shadow
    precedence, retained misses over pinned DiskTree roots, leaf/frame
    invalidation, and RowID ABA without an equality stop rule.
13. **Transitions, purge, and recovery:** protect needed Update/Delete history
    across checkpoint attempts; route transitioned rows through cold fallback.
    Roll back a transitioned destination without skipping hot source
    restoration. Only authoritative cold-route proof permits discarding an
    unreachable hot restoration. Preserve marker promotion, purge, frozen-page
    accounting, and restart without serialized links.
14. **API and integration:** MemTable update/delete/upsert and catalog paths;
    Skip/empty-update cancellation, prior ownership, application errors, poison,
    dropped waits, snapshot/full-scan visibility, and weak range selection.
    Shared write helpers publish and restore links without altering selection
    contracts of other APIs.
15. **Cost and validation:** count initial lookups, post-row validations, and
    traversed links. A successful wholly hot chain uses one index lookup and
    zero post-row validations, including when unrelated leaf entries change.
    A terminal chain validates and retries on invalidation; stable rejection
    terminates. The original CTS shortcut still bypasses validation. Empty
    payloads/restoration lists allocate nothing; measure RowUndoKind and RowUndo
    footprint. Capture an uncommitted destination, roll its writer back before
    inspection, and verify reselection of the restored owner. Cover an inserted
    destination with no undo, an updated destination with an older terminal,
    and an older forward chain ending in absence. Run focused tests and stress,
    then:

    ```bash
    rtk cargo nextest run --workspace
    rtk cargo nextest run -p doradb-storage --no-default-features --features libaio
    rtk cargo fmt --check
    rtk cargo clippy --workspace --all-targets -- -D warnings
    ```

    Run the repository branch style audit and update the unsafe inventory if
    projections change. Do not change runner timeouts or retain a standalone
    benchmark/report.

These are acceptance requirements for the revised implementation, not claims
that the new design has passed validation. Leave backlog 000196 open and do not
resolve the task as part of this document revision.

## Open Questions

No blocking product-scope questions remain for authoritative hot forwarding.
Cold marker links, path compression, and stronger progress guarantees remain
outside this task. Terminal hot rejection, cold selection, and transition
fallback retain the original index observation deliberately. Additional routing
retention after rollback is not needed for the selected approach.

Implementation must establish newest-departure traversal, source-reference
lifetime, reverse restoration order, and hot-to-cold handoff with concrete
tests. Report an actual failure of those invariants as a blocker rather than
accepting an unchecked link, a RowID-equality shortcut, a spurious missing
result, or a callback retry.
