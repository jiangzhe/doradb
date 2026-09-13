---
id: 000302
title: Share Unique Mutation and Insert Execution Across Catalog and User Tables
status: implemented
created: 2026-09-12
github_issue: 1059
---

# Task: Share Unique Mutation and Insert Execution Across Catalog and User Tables

## Summary

User and memory/catalog tables now share foreground unique selection, hot-row
mutation, insertion, and hot index effects. A borrowed `MutationExecutor` in
`table/mutate.rs` supplies the common algorithms; `UniqueMutator` drives current
selection and invokes each callback at most once.

Catalog point mutations use a private primary-key callback API. Batch deletion
and delete-then-insert replacement preserve their existing statement atomicity
and result contracts. User roots, cold selection and mutation, deletion-buffer
ownership, and persisted-index proofs remain in `UserTableAccessor`.

## Context

[Task 000299](000299-unify-unique-key-mvcc-mutation-api.md) introduced public
callback mutation, while retaining internal memory/catalog methods.
[Task 000300](000300-fix-stale-unique-read-current-lookups-across-row-replacement.md)
shared current-row classification and hot admission, but left separate unique
selection loops and physical mutation continuations.
[Task 000301](000301-refactor-memory-table-metadata-and-runtime-layouts.md)
delivered the prerequisite `RowStore`, exact runtime layouts, cached indexed
columns, and shared `WriteIndexKeySet` derivation.

The remaining duplication covered selection retries, forward traversal, prepare
waits, insert-page retries, hot moves, and index effects. Sharing those algorithms
also needed to preserve user cold-owner inspection when a hot destination claims
a previously cold-owned key. Catalog replacement had to remain a distinct
composition: it inserts even when deletion finds nothing and reports whether an
old row existed.

Source Backlogs:

- `docs/backlogs/closed/000198-share-unique-mutation-execution-across-user-and-mem-catalog-tables.md`

Issue Labels:

- type:task
- priority:medium
- codex

This task has no parent RFC. It completes the shared-execution work from backlog
000198 after task 000301's independently delivered metadata prerequisite.
The durable contracts are documented in [architecture](../architecture.md),
[transaction execution](../transaction-system.md), and
[secondary indexes](../secondary-index.md#76-proof-bound-current-row-mutation).

## Goals

- Deliver one unique selection/retry driver and shared owned hot actions,
  physical insertion, move continuation, and secondary-index effects.
- Preserve callback-at-most-once behavior, lookup evidence, ownership across
  retries, exact index identities, and statement rollback ordering.
- Carry owned sparse updates without cloning their payload through move retries.
- Keep user roots, cold decoding and ownership, persisted-index policy,
  allocation choices, and transition waits at their existing owning boundary.
- Preserve mandatory catalog validation, key-based redo, typed error policy,
  batch counts, replacement booleans, and atomic compositions.
- Remove obsolete memory mutation APIs, result types, and unused adapters after
  migrating production consumers and behavioral tests.

## Non-Goals

- Unifying table ownership or introducing a universal table capability trait,
  public memory-table admission, or a common cold/hot ownership abstraction.
- Sharing bootstrap/recovery mutation, purge, checkpoint publication, range
  traversal, or deferred driver-key rules beyond their existing hot consumers.
- Adding public mutation actions, occupied Insert, gap locks, retried or async
  callbacks, or statement-wide unique-key permutation planning.
- Changing durable formats, transaction protocols, cleanup ownership, production
  wait families, I/O backends, or test-runner configuration.
- Repairing the separate transition/undo defects in
  [backlog 000199](../backlogs/000199-repair-dangling-row-undo-ref-and-deferred-lock-to-delete-mismatch.md).

## Rejected Alternatives

- A universal storage-mode-aware runtime would entangle catalog lifetimes,
  admission, fixed layouts, and user persistence beyond the execution refactor.
- Adapters forwarding complete hot operations to the former implementations
  would retain the duplicated algorithms that motivated the task.
- A common cold/hot action-and-proof framework would hide distinct ownership
  and index-completeness rules; explicit user cold continuations remain clearer.
- Replacing catalog delete-then-insert with occupied Update would change physical
  row identity, redo, and behavior when the old row is absent.

## Plan

### Execution ownership and index binding

`MutationExecutor` can only borrow a complete `MemTable` or an admitted user
accessor. It uses that owner's `RowStore` and exact `TableRuntimeLayout`.
Memory mode includes standalone memory user tables; pool role does not establish
catalog identity. `MemIndexRuntime` projects directly owned memory indexes and
user `Arc<SecondaryIndex>` runtimes without another runtime vector or owner clone.

Each user attempt captures one compatible root through its accessor. Memory
attempts have no persisted-root state. One `bind_unique` method selects the
concrete family and binds the exact `IndexRef`. User binding borrows the runtime,
captured snapshot, and pool guards directly; returned views cannot outlive them.
Cold replacement reuses its deletion attempt's root for shared insertion.

### Selection, actions, and errors

`UniqueMutator` retains the original lookup observation while following hot
ownership and authoritative successors. Cold selection returns findings or owned
state from user code without duplicating the outer retry loop. Attempt resources
are released before prepare/transition waits and retries, preserving cooperative
yields and poison checks. A missing Insert race never retries the callback.

Callbacks return `UniqueMutation` directly: Insert owns full-row values and Update
owns ordered sparse columns. Missing entries accept Skip/Insert; occupied entries
accept Skip/Update/Delete. Entry/action validity and selected-key agreement remain
mandatory. Catalog payload validation is always enabled, independently of the
public trusted-payload option. Skip and empty Update release only newly acquired
ownership; empty Update reports the original RowID.

The shared driver uses an explicit generic engine-error mapper. Catalog callbacks
retain native Runtime reports and Fatal propagation through the Quad carrier;
public callbacks disclose engine errors once and preserve application errors.
No Clone, Send, Sync, or static capture bound was added. Statement runners remain
responsible for settlement, rollback, and cancellation cleanup.

### Insertion, hot effects, and cold handoffs

Shared physical insertion handles page retries, free-list/cache reuse, and row
effects while preserving family-specific allocation and page-creation redo.
Complete insertion derives keys once, then claims indexes. Hot moves perform
physical insertion followed by their own single move-index continuation.

`OwnedHotIndexSet` ties old keys to matching table/row/page hot undo that has been
synchronously converted to Delete or Update. User conversion also consumes
RowPage/MemRequired authority. Move authority is captured before replacement
insertion changes the newest effect. Exact active index owners are required for
hot masking and unchanged-key replacement.

Unique selection transfers its page pin through `HotUpdatePage::Owned`; move
preparation releases that pin before replacement allocation. Scan cursors retain
caller-owned pins through the Borrowed case. Mutable row access never crosses an
await in the callback and synchronous undo-conversion boundary.

Unique claims retain the original memory/composite owner observation, avoiding a
second disk lookup. Previous-owner history and exchange undo precede synchronous
forward-link publication. Same-RowID deleted-shadow merging remains supported.
User code alone handles cold-owner inspection, CDB claims, LWC decoding, cold
actions, and persisted-index absence policy.

### Catalog integration

Private transaction and statement primary-key callbacks share the same decisions
and outcomes as user mutation. They validate catalog selectors and capture the
selected key before updates for key-based redo. Ordered batch deletion admits
and locks once in one statement. Replacement deletes if present, then inserts
unconditionally within that statement; failure restores both halves while
preserving earlier statements. Direct insertion adds no point lookup.

Ordinary catalog operation errors retain their invariant policy. Optimistic batch
insertion preserves expected DuplicateKey and WriteConflict results. Domain
storage methods remain thin adapters where their boolean/count contracts are
useful; legacy memory update/delete/upsert algorithms and result enums are gone.

## Implementation Notes

Shared unique mutation and insertion now serve user and memory/catalog tables.

The final implementation follows the bounded execution design above. Existing
full-table and index-driven hot consumers use the shared continuation without
changing traversal. Review confirmed exact index identity, root/proof lifetimes,
source-page pin release before move allocation, native error propagation, and
statement settlement across the migrated paths.

User review superseded the original full-row MVCC update requirement because
only standalone memory tests produced that input. Removed `RowUpdateInput`, its
views/iterators, and the duplicate internal action enum. Updates now pass owned
sparse vectors or borrowed slices. Shared test utilities implement callback
upsert as Insert on absence and sparse Update on occupancy; validator tests
retain order, bounds, type, and null coverage. Full-row insertion and catalog
delete-then-insert replacement remain supported.

Review also merged the executor's index-handle preparation and unique binding
into `bind_unique`, backed by the accessor's snapshot-bound factory. Removed the
unused `Columns::delete_by_id`, `catalog_key_from_active_ordinal`, and
`NonUniqueSecondaryIndex::insert_mem_if_not_exists` adapters. Their callers now
use explicit catalog keys or guarded memory bindings; relevant behavioral tests
remain, while obsolete wrapper-only tests were removed or consolidated.

Fast-path review preserved observed lookups and lazy-row allocation behavior.
Retained undo keys use an already-bound exact entry. Indexed-column updates now
record one paired metadata/runtime iteration in place of a metadata-only walk;
selector admission still records one direct validation and zero IndexID-map
lookups. Standalone-memory failure fixtures restore real row undo directly
before settlement because they have no catalog-cache entry.

Added three catalog boundary tests covering callback decisions and counts,
mandatory validation, Runtime report frames, key-based redo, replacement,
and batch rollback. Replacement failure uses actual fixed-pool exhaustion in
the insertion half and verifies restored rows/indexes, preserved prior effects,
and subsequent successful operations. No synthetic failure hook or scheduler
sleep was needed.

Validation on 2026-09-13:

- Workspace tests: 2,003 passed; alternate libaio backend: 1,887 passed.
  The workspace suite was rerun after the final inline-attribute cleanup.
- Formatting and strict workspace Clippy passed. Resolution's mandatory
  `tools/style_audit.rs --diff-base origin/main` passed for 27 Rust files.
- Focused coverage after sparse-update cleanup: 95.11% across 12 files
  (22,687/23,853 lines), with every file above 90%.
- Focused coverage after merged unique binding: 96.43% across the executor,
  accessor, and unique driver (11,034/11,443 lines). The new snapshot-bound
  unique-index factory was fully covered.
- The refreshed public-error audit matches `docs/public-error-audit.csv`;
  unsafe inventory remained unchanged.

All review issues within scope were addressed. Source backlog 000198 is completed
by this task. The previously accepted transition defects remain separately tracked
in backlog 000199; these test results do not establish their repair.

## Impacts

- Table execution, row update plumbing, transaction statements, and catalog
  storage now use the shared foreground algorithms and sparse update inputs.
- Public transaction signatures and mutation decisions remain unchanged. No
  durable format, schema, recovery, checkpoint, or compatibility migration is
  required.
- Memory runtimes remain directly owned and user runtimes retain their existing
  owners. Sharing adds no per-operation runtime collection, index-owner clones,
  extra root capture, or external selector lookup.
- Architecture, transaction, secondary-index, and public-error documentation
  reflect the final execution and error boundaries.

## Test Cases

- User-hot and standalone-memory actions: missing/occupied validation, selected
  keys, Skip/empty updates, sparse updates/upserts, callback counts, borrowed
  captures/errors, cancellation, and preservation of earlier statement effects.
- Current selection: forward chains, repeated RowIDs, per-index successors,
  deletion timestamps, stale evidence, prepare waits, replacement/rekey races,
  and rollback-erased claims.
- Hot mutation: in-place changes, space/frozen moves, sparse runtime slots,
  non-slot IndexIDs, unique/non-unique effects, unchanged and changed keys,
  old snapshots, late claim failures, undo restoration, and lookup counters.
- User cold behavior: marker precedence, fresh/consumed claims, indexed-only
  deletion decoding, optional persisted copies, older-root MemRequired authority,
  cold-owner key reuse, backward links, checkpoint, and recovery regressions.
- Catalog behavior: mandatory selector/action/payload validation, key-based redo,
  typed reports, delete counts, present/missing replacement, allocation failure,
  whole-batch rollback, DDL, checkpoint, and restart integration.

## Open Questions

No unresolved question blocks completion. The independent retained-page undo
lifetime and deferred Lock-to-Delete defects remain in
[backlog 000199](../backlogs/000199-repair-dangling-row-undo-ref-and-deferred-lock-to-delete-mismatch.md),
which preserves their findings, deferral rationale, and future design direction.
