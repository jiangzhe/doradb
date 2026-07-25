---
id: 000237
title: Add Metadata-Only Table History and Publication
status: implemented  # proposal | implemented | superseded
created: 2026-07-25
github_issue: 887
---

# Task: Add Metadata-Only Table History and Publication

## Summary

Implement Phase 1 of RFC 0024 by replacing the catalog's single-current
user-table registry with volatile, CTS-effective logical metadata history plus
a separate current operational state. A history entry stores superseded live
`TableMetadata` versions only; the current state directly stores its effective
CTS and either the current metadata/runtime or a terminal dropped-table
tombstone.

Visible resolution first checks current state, then scans superseded versions
from newest to oldest using the strict row-MVCC boundary
`effective_cts < transaction_sts`. Current-only resolution never walks backward
from a tombstone. Historical versions own logical metadata only and cannot own
`Table`, `TableRuntimeLayout`, secondary runtimes, roots, allocation maps, or
block identifiers.

Publish matching metadata state after successful CREATE TABLE, CREATE INDEX,
DROP INDEX, and DROP TABLE DDL. Hold CREATE TABLE metadata X from immediately
after table-id allocation through catalog commit and current runtime/history
installation. Reuse the transaction purge horizon to trim obsolete metadata
history and remove authoritative table tombstones only after the strict horizon
and historical-version pins permit it.

This task materializes and publishes history but does not activate
transaction-visible admission. Transaction bindings, `SchemaChanged`, and the
current-state-only CREATE INDEX cutover remain later RFC phases.

## Context

Parent RFC:
- `docs/rfcs/0024-versioned-metadata-immediate-retirement.md`

RFC Phase:
- Phase 1: Metadata-Only History And Publication

Issue Labels:
- type:task
- priority:high
- codex

The current `Catalog::user_tables` map stores one of `Live`,
`DroppedRuntime`, or `DroppedFloor`. A live entry exposes only one current
`Arc<Table>`, while dropped runtime and file-cleanup ownership share the same
foreground registry state. There is no ordered logical history and no
authoritative metadata tombstone independent of operational cleanup.

CREATE TABLE currently allocates its table id and prepares its file without
holding `TableMetadata(table_id, X)`. Its `CreateTableProgress::commit_catalog`
discards the CTS returned by `Transaction::commit_catalog_ddl`, then installs
the runtime in a separate post-commit step. A lookup can therefore race the
catalog-commit/runtime-install interval.

CREATE INDEX and DROP INDEX already retain their DDL commit CTS. Their
publication order is catalog commit, table-root publication, and
`TableRuntimeLayout` installation, which provides a natural final point for
publishing matching current logical metadata before metadata X is released.
DROP TABLE likewise retains `drop_cts`, but currently converts the catalog map
directly to operational `DroppedRuntime` state and injects one error into all
queued table-lock waiters.

`TableRuntimeLayout` already provides the correct executable ownership
boundary: it owns one current `Arc<TableMetadata>` plus sparse secondary runtime
slots. History must reuse the logical metadata Arc without retaining the
runtime layout or any executable resource.

Transaction purge already calculates an authoritative `min_active_sts` from
the active-STS buckets and runs horizon-sensitive retained-root and
dropped-table cleanup. Metadata-history GC must join this coordinator rather
than introduce another horizon tracker.

Recovery already reconstructs one current runtime from checkpointed catalog and
table-file state, then replays ordered redo before foreground admission. No
pre-crash transaction or metadata snapshot survives restart, so recovery needs
one synthetic current baseline and no reconstructed history.

Phase 1 has no prerequisite phase. Phase 2 requires this task to provide:

1. strict STS-visible metadata resolution;
2. current-only metadata/runtime resolution;
3. stable per-table version identity;
4. CREATE TABLE publication exclusion; and
5. metadata history whose lifetime is independent of executable resources.

Design review approved the following representation refinement, now reflected
in RFC 0024:

- current live state stores `effective_cts`, `Arc<TableMetadata>`, and
  `Arc<Table>` directly;
- the terminal current tombstone stores only `effective_cts`;
- `Arc<TableMetadataVersion>` wraps superseded live versions only;
- version identity is `(TableID, effective_cts)`, not version-Arc pointer
  identity;
- resolution checks current first and then linearly scans a short `Vec` in
  reverse;
- the existing `FastDashMap` shared/read and exclusive/write guards protect the
  whole `UserTableEntry`; there is no nested history lock.

These changes preserve the RFC's visibility, locking, ownership, retirement,
and GC behavior. RFC 0024 was synchronized to this representation alongside
task creation; phase resolution must confirm that the implementation still
matches it.

Relevant references:

- `docs/architecture.md`
- `docs/transaction-system.md`
- `docs/garbage-collect.md`
- `docs/rfcs/0016-logical-lock-manager.md`
- `docs/rfcs/0017-drop-table-lifecycle-recovery.md`
- `docs/rfcs/0018-create-drop-index.md`
- `docs/process/coding-guidance.md`
- `docs/process/unit-test.md`
- `doradb-storage/src/catalog/mod.rs`
- `doradb-storage/src/catalog/table.rs`
- `doradb-storage/src/catalog/index.rs`
- `doradb-storage/src/table/layout.rs`
- `doradb-storage/src/lock/mod.rs`
- `doradb-storage/src/trx/purge.rs`
- `doradb-storage/src/recovery/mod.rs`

## Goals

1. Add a volatile, per-table metadata history ordered by effective DDL CTS.
2. Store only superseded live logical metadata in historical version objects.
3. Store current live metadata/runtime or the terminal tombstone directly in
   current state.
4. Resolve visible state with strict `effective_cts < sts`, checking current
   before scanning history from newest to oldest.
5. Resolve current state without an STS and without falling back from a
   tombstone to historical or operational state.
6. Use `(TableID, effective_cts)` as stable metadata-version identity for Phase
   2 admission and stale-write comparison.
7. Keep metadata history and dropped-runtime/file cleanup independently
   removable.
8. Publish one matching logical metadata transition for every successful
   user-table or index DDL and no transition for aborted DDL.
9. Hold CREATE TABLE metadata X from immediately after ID allocation through
   current history/runtime installation.
10. Release DDL locks normally after publication so queued callers resolve
    their own current or later STS-visible state.
11. Retain the newest metadata predecessor required by the active-STS horizon
    and retain every externally pinned historical version.
12. Remove a dropped table's authoritative history only when
    `min_active_sts > drop_cts` and historical version pins have drained.
13. Reconstruct one current-only metadata baseline during recovery without
    persistent history or durable tombstones.
14. Leave Phase 2 with direct visible/current resolution and version-identity
    interfaces that do not require redesigning this storage model.

## Non-Goals

1. Do not add `TransactionTableBinding` or replace transaction/session weak
   caches with positive bindings.
2. Do not add `OperationError::SchemaChanged` or activate visible/current table,
   index, or write admission.
3. Do not make successful table reads retain transaction-lifetime metadata S;
   that is Phase 2.
4. Do not add lifetime-bound secondary-index root proofs.
5. Do not switch CREATE INDEX to current committed rows or remove task-000236
   historical candidate construction.
6. Do not change unique-index ownership behavior or non-unique historical
   candidate reclamation.
7. Do not persist metadata history, tombstones, version identifiers, or schema
   epochs.
8. Do not change table-file, catalog-file, redo, row, or index metadata formats.
9. Do not make dropped tables or indexes historically executable.
10. Do not add physical column evolution, row-layout versioning, table-id reuse,
    or index-number reuse.
11. Do not add a second GC horizon or a standalone metadata-GC thread.
12. Do not add unsafe code. If safe ownership proves insufficient, stop and use
    the repository unsafe-review process.

## Plan

### 1. Add the simplified metadata-history model

Create `doradb-storage/src/catalog/history.rs` and re-export its crate-private
types through `catalog::mod`.

Use this representation:

```rust
struct TableMetadataVersion {
    effective_cts: TrxID,
    metadata: Arc<TableMetadata>,
}

struct TableHistoryEntry {
    // Superseded live versions, oldest to newest.
    versions: Vec<Arc<TableMetadataVersion>>,
    current: CurrentTableState,
}

#[derive(Clone)]
enum CurrentTableState {
    Live {
        effective_cts: TrxID,
        metadata: Arc<TableMetadata>,
        table: Arc<Table>,
    },
    Dropped {
        effective_cts: TrxID,
    },
}

struct UserTableEntry {
    history: Option<TableHistoryEntry>,
    dropped: Option<DroppedTableOperationalState>,
}

enum DroppedTableOperationalState {
    Runtime {
        table: Arc<Table>,
        drop_cts: TrxID,
        replay_floor: TableRedoReplayFloor,
    },
    Floor {
        drop_cts: TrxID,
        replay_floor: TableRedoReplayFloor,
    },
}
```

Keep exact field visibility private to the owning catalog modules. Add narrow
documented constructors, accessors, transition helpers, and test-only
observation rather than exposing mutable fields.

Enforce these release-mode invariants at the owning transition:

1. `versions` is strictly ordered by `effective_cts`.
2. Every historical version CTS is lower than current CTS.
3. Historical versions always represent live metadata.
4. `CurrentTableState::Live.metadata` is pointer-identical to the metadata in
   the table's installed current `TableRuntimeLayout`.
5. A live entry has no dropped operational state.
6. A dropped current state has no foreground table handle.
7. Historical versions contain no executable runtime or physical-root handle.
8. A registry key is removed only after both `history` and `dropped` are absent.

The valid outer states are:

| Logical state | History slot | Operational slot |
| --- | --- | --- |
| Live | live current | none |
| Just dropped | tombstone current | retained runtime |
| Runtime destroyed | tombstone current | retained floor |
| History GC first | none | runtime or floor |
| File cleanup first | tombstone current | none |
| Fully reclaimed | none | none, remove key |

### 2. Implement current-first visible and current-only resolution

Add:

```rust
Catalog::resolve_user_table_visible(table_id, sts)
Catalog::resolve_user_table_current(table_id)
Catalog::current_live_user_table(table_id)
```

`resolve_user_table_visible` returns `None` only when no version is strictly
visible. Otherwise it returns:

```rust
enum ResolvedVisibleTableMetadata {
    Live(ResolvedLiveMetadata),
    Tombstone { effective_cts: TrxID },
}

struct ResolvedLiveMetadata {
    effective_cts: TrxID,
    metadata: Arc<TableMetadata>,
    _history_pin: Option<Arc<TableMetadataVersion>>,
}
```

Use `_history_pin = None` when current live state is selected and retain the
selected historical version Arc otherwise. Expose methods for effective CTS,
metadata, and current-versus-historical classification; callers must not
inspect the pin field.

Under the `FastDashMap` shared guard, resolve in this exact order:

```text
if current.effective_cts < sts:
    return current live metadata or current tombstone

for version in versions.iter().rev():
    if version.effective_cts < sts:
        return historical live metadata with its version Arc pin

return absent
```

Do not binary search or add auxiliary indexing in this phase. Metadata histories
are expected to be short, current state is the common result, and Phase 2 calls
visible resolution only on the first successful table touch.

`resolve_user_table_current` clones current state under the same map guard and
never consults `versions`. An absent history slot and an absent registry key
both mean ordinary current absence. Neither resolver may inspect the `dropped`
operational slot.

Use `(table_id, effective_cts)` as version identity. Assert strictly increasing
current CTS values on every online DDL transition. Use
`TrxID::new(0)`, which is below `MIN_SNAPSHOT_TS`, only as the recovery baseline
effective timestamp.

### 3. Define map-guard and logical-lock responsibilities

Use `FastDashMap::get` or iteration guards for shared resolution and
observation. Use an occupied `FastDashMap::entry` write guard for publication,
history GC, and operational-slot mutation. Mutating `UserTableEntry` helpers
require `&mut self`; construction and removal instead have exclusive ownership.
Do not add a nested mutex or `RwLock`.

The map guard must never be held across:

- `.await`;
- logical lock acquisition;
- catalog transaction commit or rollback;
- table-root publication;
- runtime-layout installation;
- runtime destruction; or
- file deletion.

The single map guard protects in-memory current/history consistency against
resolution, DDL finalization, and purge. A shared resolver clones current state
or its selected historical wrapper before releasing the guard; the wrapper Arc
then remains the external GC pin. DashMap write exclusion is shard-level, so
keep every write critical section to the final in-memory validation, append,
state switch, or prefix trim. `TableMetadata(S/X)` remains the async
operation/publication boundary.

The direct current-live representation is safe for Phase 2 because a successful
current binding retains transaction metadata S. DDL cannot convert that current
state into a historical version until the binding ends. After DDL obtains
metadata X, create one `Arc<TableMetadataVersion>` from the superseded current
metadata and append it before installing the new current state. A transaction
that later selects that historical version retains its wrapper through
`_history_pin`.

### 4. Refactor the catalog registry without coupling cleanup lifetimes

Replace the current `UserTableEntry::{Live, DroppedRuntime, DroppedFloor}` enum
with the orthogonal history and operational slots above.

Adapt these catalog operations:

- live/current table lookup;
- checkpoint and redo-floor snapshots;
- purge runtime pinning;
- drop transition and stale-runtime restoration;
- dropped-floor insertion and removal;
- startup/recovery retained-file protection; and
- table-id listing.

Operational helpers may obtain a live current table or a retained dropped
runtime as explicitly required by their cleanup contract. They must not expose
operational state as a foreground/current metadata answer.

Remove an outer map entry only after both optional slots are empty. Preserve the
existing replay-floor and file-cleanup authority if metadata-history GC removes
the logical slot first.

### 5. Protect CREATE TABLE publication and retain its CTS

Add a lock-manager helper that acquires only
`TableMetadata(new_table_id, X)` for the session DDL owner/group and returns a
scoped fresh-grant guard. It must use existing FIFO waiter and cancellation
behavior and must not acquire `TableData`.

In `create_table_for_session`:

1. validate public table/index specifications that do not depend on existence;
2. allocate the non-reused table id;
3. immediately acquire metadata X for that id;
4. prepare the table file, catalog rows, and staged runtime;
5. change `CreateTableProgress::commit_catalog` to return `create_cts`;
6. create the initial `UserTableEntry` with empty `versions` and
   `CurrentTableState::Live { effective_cts: create_cts, ... }`;
7. install the complete entry and runtime before releasing metadata X.

A request granted before the create-X request is established may resolve
absence and linearizes before publication. A later request waits for X and then
resolves absence at `sts <= create_cts` or the live current state at
`sts > create_cts`.

Pre-commit failure must roll back existing staged state, release metadata X
normally, and leave no registry history. Any catalog-committed failure or
invariant disagreement before complete history/runtime installation must
preserve the existing fatal poison policy.

### 6. Publish index metadata versions after root and layout publication

Retain the existing index DDL locks, metadata-change leases, build behavior,
catalog transaction, table-root publication, and runtime-layout installation.

For CREATE INDEX and DROP INDEX:

1. retain the returned DDL CTS;
2. publish the prepared table root;
3. install the new `TableRuntimeLayout`;
4. while metadata X is still held, acquire the occupied catalog-map entry;
5. validate that current live CTS/metadata/table match the old layout;
6. append the superseded current live metadata as one
   `Arc<TableMetadataVersion>`;
7. install new current live state with the DDL CTS, exact new metadata Arc, and
   unchanged table Arc;
8. release the map guard, metadata-change leases, and DDL locks.

Finish all fallible validation and staging possible before catalog commit.
After commit, root/layout/history disagreement is fatal and must not restore the
old current state. Aborted index DDL appends no version.

Do not change CREATE INDEX row collection or historical-candidate behavior in
this phase.

### 7. Publish an authoritative DROP TABLE tombstone

After DROP TABLE catalog commit:

1. retain `drop_cts` and the current replay floor;
2. complete the existing terminal runtime lifecycle transition;
3. under the occupied catalog-map entry, validate current live state;
4. append the superseded current live metadata version;
5. replace current state with `Dropped { effective_cts: drop_cts }`;
6. install the runtime/replay floor in the sibling operational `Runtime` slot;
7. release the map guard and DDL locks normally;
8. request both dropped-runtime and metadata-history purge observation.

Do not remove the history slot when detaching the foreground runtime. Do not
allow weak session/transaction hints, `DroppedRuntime`, catalog latest-live
helpers, or operational cleanup to bypass a current tombstone.

Remove `ScopedTableDdlLocks::fail_waiters_on_release` and any lock-manager
waiter-failure outcome or broadcast helper, including test-only variants.
Queued requests receive normal grants and resolve independently. Owner cleanup
continues to wake cancelled requests with `LockWaiterReleased`. Phase 1 does not
yet add `SchemaChanged`; Phase 2 will map its visible/current pair to the final
public errors.

### 8. Make non-MVCC lookup explicitly current-only

Route DDL target validation, explicit-lock admission, freeze/checkpoint,
catalog checkpoint root proof, maintenance, purge, and recovery through
current-only or explicit operational helpers.

For online paths, perform authoritative current resolution only after their
existing logical lock or operation-specific barrier. Prechecks may reject
invalid table-id classes, unhealthy engine state, or incompatible same-session
locks, but may not decide table existence before metadata protection.

Foreground transaction statement and stream paths remain current-only until
Phase 2. They may use a temporary live-current compatibility helper, but this
task must not introduce positive binding caches, transaction-lifetime read
locks, STS-visible admission, or `SchemaChanged`.

Session weak runtime and insert-page hints remain non-authoritative. A retained
tombstone or absent history may not fall back to such a hint.

### 9. Integrate metadata-history GC with transaction purge

Add metadata-history work to the purge coordinator. Run it for a genuinely
newer complete horizon cycle and for an explicit/full or targeted metadata
history observation, including an unchanged horizon retry after an Arc pin
releases. Do not add another worker thread or horizon.

For one live history:

1. treat current as the newest logical version;
2. if current CTS is strictly below `min_active_sts`, current itself is the
   retained predecessor and every unpinned historical prefix is obsolete;
3. otherwise retain the newest historical version strictly below
   `min_active_sts` plus all later versions;
4. stop prefix removal at an externally pinned version;
5. use simple linear position calculation and `Vec::drain` rather than another
   index.

For one dropped history:

1. retain the history while `drop_cts >= min_active_sts`;
2. when `drop_cts < min_active_sts`, remove the complete history slot only if
   every historical version has no external Arc owner;
3. leave the operational slot unchanged;
4. remove the outer map key only if operational cleanup also finished.

Because `versions` owns one strong reference to each historical wrapper, an
unpinned version has strong count one. Encapsulate this count rule in the
history owner and test it directly. Do not use `TableMetadata` strong counts:
runtime layouts and operational state legitimately share those metadata Arcs.

### 10. Keep recovery current-only

During checkpoint bootstrap or replayed CREATE TABLE, install one current live
history with:

```text
effective_cts = 0
versions = []
metadata/table = recovered current runtime
```

The zero value is a recovery-only baseline older than every valid foreground
STS. The loaded table-file metadata may already include later durable index DDL
whose catalog rows reconcile during replay; do not synthesize intermediate
history.

Recovery index-DDL replay continues reconciling the one current runtime/catalog
baseline and does not append pre-crash versions. Replayed DROP TABLE removes
the current history before foreground admission, destroys the runtime through
the existing offline path, and retains only the operational replay/file floor
when required. It does not reconstruct an in-memory history tombstone because
no pre-crash STS survives.

### 11. Maintain RFC 0024 synchronization when resolving the phase

RFC 0024's Decision and Phase 1 representation already record the direct
current state, superseded-only version wrappers, `(TableID, effective_cts)`
identity, current-first reverse-linear resolution, `FastDashMap` guard
boundary, and historical-pin eviction rule selected during task design.

During `$task-resolve`, compare the implemented outcome with those contracts
and update the RFC only if implementation evidence changes a representation or
constraint. Do not change the visible/current outcome matrix, strict
inequalities, immediate-retirement policy, Phase 2 prerequisites, or later
phase scopes without a separate design decision. Fill Phase 1's Task Issue,
Phase Status, and Implementation Summary according to the actual resolved
outcome; keep its Task Doc linked to this document.

## Implementation Notes

Follow-up amendment: Task 000238 supersedes this task's defensive
historical-wrapper pin contract. `min_active_sts` is now the sole
metadata-history reclamation authority; a resolved result owns its selected
`Arc<TableMetadata>` and effective CTS without retaining catalog history
membership. The original plan and test descriptions below remain as the
historical record of Phase 1's implementation, while RFC 0024 and Task 000238
define the current contract.

Implemented Phase 1 with one volatile metadata-history entry per user table.
The catalog now resolves strict STS-visible metadata separately from direct
current state, publishes matching CTS-effective transitions for table and index
DDL, and keeps logical history independent from dropped-runtime and replay/file
cleanup ownership. CREATE TABLE holds metadata X across catalog commit and
complete runtime/history installation; queued DDL waiters resume through the
normal grant path.

Metadata-history GC reuses the authoritative transaction purge horizon. Live
history retains the newest predecessor visible to the oldest active STS and
stops prefix removal at an externally pinned version. Dropped history is
removed only after its strict post-DROP horizon and pins clear, independently
of operational cleanup. Recovery installs one CTS-zero current baseline and
does not reconstruct pre-crash history or tombstones.

Implementation review produced three material refinements:

- `UserTableEntry` stores history directly under the existing `FastDashMap`
  guard; an intermediate nested history mutex was removed.
- The lock manager now has only production waiter outcomes
  (`Waiting`, `Granted`, and `Released`); the test-only semantic failure outcome
  and broadcast helpers were removed, and coverage uses owner release through
  `LockWaiterReleased`.
- Purge's exclusive prefix length deliberately retains the newest version below
  `min_active_sts` when current metadata is too new. A regression test holds an
  active reader across index DDL, verifies that predecessor remains resolvable,
  and verifies reclamation after the horizon advances beyond current metadata.

The forecast direct changes in `recovery/mod.rs`, `catalog/checkpoint.rs`, and
`table/mod.rs` were unnecessary: their existing paths consume the refactored
catalog helpers and recovery installation boundary. No public API, persistent
format, redo shape, unsafe code, or Phase 2 admission behavior changed.

Verification completed with:

- branch-diff style audit passing for 12 Rust files, including formatting and
  workspace Clippy with warnings denied;
- `rtk cargo nextest run --workspace`: 1,535 tests passed;
- `rtk cargo nextest run -p doradb-storage --no-default-features --features
  libaio`: 1,460 tests passed; and
- focused changed-path coverage above the repository 80% review bar (95.84%
  across the original implementation paths, with the reviewed history/catalog
  subset at 96.33%).

## Impacts

- `doradb-storage/src/catalog/history.rs`
  - new metadata-history types, resolution, transitions, pinning, and GC.
- `doradb-storage/src/catalog/mod.rs`
  - user-table registry representation, current/visible resolvers, operational
    cleanup separation, and compatibility helpers.
- `doradb-storage/src/catalog/table.rs`
  - CREATE TABLE metadata-X publication, create CTS propagation, and DROP TABLE
    tombstone publication.
- `doradb-storage/src/catalog/index.rs`
  - post-root/layout CREATE/DROP INDEX metadata publication.
- `doradb-storage/src/table/layout.rs`
  - current metadata identity assertions; no historical runtime ownership.
- `doradb-storage/src/lock/mod.rs`
  - scoped metadata-X-only CREATE TABLE helper, normal DROP waiter release, and
    removal of semantic waiter-failure broadcasting.
- `doradb-storage/src/session.rs`
  - explicit-lock and maintenance current-only lookup ordering.
- `doradb-storage/src/trx/mod.rs`
  - lock-before-current-resolution ordering and removal of stale weak-cache
    admission.
- `doradb-storage/src/trx/stmt.rs`
  - current-live compatibility lookup only; Phase 2 admission remains deferred.
- `doradb-storage/src/trx/stream_stmt.rs`
  - same temporary current-live compatibility boundary for lazy streams.
- `doradb-storage/src/table/access.rs`
  - production owner-release coverage for queued lock cancellation context.
- `doradb-storage/src/trx/purge.rs`
  - metadata-history work scheduling and strict-horizon cleanup.
- `docs/rfcs/0024-versioned-metadata-immediate-retirement.md`
  - task-planning representation sync and resolve-time Phase 1 tracking.

There is no public API, persistent-format, redo-shape, table-file, catalog-file,
row-layout, index-layout, or unsafe-code impact.

Runtime costs introduced by this phase are:

- one short `FastDashMap` shared guard and usually one current CTS comparison
  on a first-touch visible resolution;
- reverse linear history traversal only for older snapshots;
- one metadata version allocation per superseded table metadata state; and
- infrequent linear prefix GC and `Vec::drain`.

Phase 2 will make successfully bound subsequent operations bypass this lookup.

## Test Cases

Use inline module tests and narrow deterministic test hooks. Synchronize on
lock queues, publication phases, purge observations, or channels; do not use
sleep to establish concurrency.

1. A newly created current version at CTS `C` is absent for `sts <= C` and live
   for `sts > C`.
2. Current live resolution completes from the current-state comparison without
   traversing historical entries.
3. When current is too new, reverse linear traversal chooses the newest
   historical version with `effective_cts < sts`.
4. Equality with current or a historical CTS remains invisible and selects the
   preceding version or absence.
5. Multiple CREATE/DROP INDEX transitions produce strictly increasing
   superseded versions and one direct current live state.
6. `(TableID, effective_cts)` distinguishes every online version; recovery
   baselines at CTS zero remain table-local identities.
7. Current live metadata is pointer-identical to the installed runtime-layout
   metadata after each publication.
8. Historical version structs contain only effective CTS and logical metadata;
   retaining them does not raise `Table`, runtime-layout, secondary-runtime,
   root, or block-handle ownership.
9. A historical visible result keeps its wrapper Arc pinned, while a current
   visible result needs no history pin.
10. A live history whose current CTS is below the horizon removes all unpinned
    predecessors.
11. A live history whose current CTS is at or above the horizon retains exactly
    the newest predecessor below the horizon and every later version.
12. GC retains equality boundaries and does not treat
    `effective_cts == min_active_sts` as obsolete.
13. An external historical version pin stops prefix removal; an unchanged
    horizon observation removes it after the pin releases.
14. DROP TABLE keeps its history while `min_active_sts <= drop_cts`.
15. DROP TABLE with `min_active_sts > drop_cts` still keeps history while any
    historical version is externally pinned.
16. After the strict horizon and pins clear, history can disappear while a
    dropped runtime or file floor remains operationally retained.
17. Dropped-runtime/file cleanup can finish first while the current tombstone
    remains authoritative.
18. The outer registry key disappears only after both history and operational
    slots are absent.
19. A current tombstone never falls back to a session hint, transaction hint,
    dropped runtime, dropped floor, or latest-live catalog helper.
20. CREATE TABLE acquires metadata X immediately after table-id allocation and
    holds it through catalog commit and complete history/runtime installation.
21. A metadata-S request queued during CREATE's post-commit/pre-install hook
    remains blocked, then independently resolves absence at `sts <= create_cts`
    or live state at `sts > create_cts`.
22. CREATE failure before catalog commit releases metadata X normally and
    installs no history/current runtime.
23. CREATE failure after root publication but before catalog commit preserves
    existing cleanup behavior and installs no history.
24. Post-commit CREATE history/runtime ambiguity poisons storage and does not
    reopen or expose partial state.
25. Successful CREATE INDEX appends the old current version and installs the
    new current state only after the matching root and runtime layout publish.
26. Successful DROP INDEX performs the same CTS/layout/history transition with
    the stable slot inactive.
27. Aborted index DDL appends no version; injected post-commit
    root/layout/history disagreement preserves poison semantics.
28. DROP TABLE appends the final live metadata, publishes direct tombstone
    current state, and moves runtime ownership into the sibling operational
    slot before releasing metadata X.
29. Old- and new-STS metadata-S requests queued behind DROP receive normal
    grants; the lock manager contains no semantic waiter-error broadcast path,
    including test-only hooks.
30. DDL, explicit-lock, checkpoint/freeze, maintenance, purge, catalog
    checkpoint, and recovery paths consume current or explicit operational
    state only, even when older visible metadata remains in history.
31. Recovery of a checkpointed live table creates one CTS-zero current baseline
    with empty history.
32. Recovery after index DDL exposes only the latest reconciled current
    metadata and does not reconstruct pre-crash versions.
33. Recovery after DROP requires neither a metadata tombstone nor historical
    runtime admission, while retaining the existing replay/file floor when
    catalog absence is not yet checkpoint-safe.
34. Existing DDL rollback, checkpoint exclusion, dropped-runtime destruction,
    replay-floor retention, and file-cleanup tests remain green after the
    registry refactor.
35. Run formatting and lint validation:

    ```bash
    rtk cargo fmt --all -- --check
    rtk cargo clippy --workspace --all-targets -- -D warnings
    ```

36. Run authoritative default and alternate-backend validation:

    ```bash
    rtk cargo nextest run --workspace
    rtk cargo nextest run -p doradb-storage --no-default-features --features libaio
    ```

37. Run focused coverage for changed catalog, DDL, purge, and recovery paths and
    meet the repository's default 80% review bar or document a justified
    definition-heavy exception with covered consumer paths.

## Open Questions

None. The history representation, synchronization, version identity, resolution
algorithm, publication ordering, GC boundary, recovery baseline, Phase 2
handoff, and parent-RFC synchronization are resolved.
