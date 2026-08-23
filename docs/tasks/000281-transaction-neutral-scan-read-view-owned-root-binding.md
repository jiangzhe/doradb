---
id: 000281
title: Transaction-neutral scan read view and owned root binding
status: proposal
created: 2026-08-23
github_issue: 1011
---

# Task: Transaction-neutral scan read view and owned root binding

## Summary

Implement Phase 1 of RFC-0030 by separating full-table scan MVCC identity,
physical runtime capabilities, and root access from `TrxRuntime`. Add one
immutable `MvccReadView` with an optional own-transaction status, adapt the
existing transaction table stream through the new scan-specific boundaries,
and preserve one shared cold/hot visibility implementation.

Keep the existing lifetime-branded `TableRootSnapshot<'read>` for transaction,
index, mutation, and maintenance paths. Add a distinct lifetime-free
`OwnedTableScanRoot` for storage inside the future registry-owned read-snapshot
core, but make it unusable by itself. Scan fields from that stored artifact are
available only through `CheckedOutTableScanRoot<'checkout>` borrowed from the
exact future snapshot checkout. A crate-private `TableScanRootView` trait lets
the existing transaction root and the future checked-out root drive the same
worklist code without dynamic dispatch or weakening either proof model.

This task adds no public snapshot API or dormant execution path. The existing
`Transaction::table_scan_mvcc_stream` is the production consumer and must
preserve its current results, callback behavior, physical ordering, bounded
state, diagnostics, and terminal checkout release.

## Context

Issue Labels:

- type:task
- priority:high
- codex

Parent RFC:

- `docs/rfcs/0030-shared-read-snapshots-parallel-table-scan.md`

RFC Phase:

- Phase 1: Transaction-neutral scan read view and owned root binding

The current table stream captures one `TableRootSnapshot<'_>` through
`TrxReadProof`, then stores the copied column root and pivot in
`TableScanWorklist`. Its physical and MVCC helpers nevertheless retain
transaction-only inputs:

- `UserTableAccessor::table_scan_mvcc_worklist` and the cold/hot page-loading
  helpers accept `TrxRuntime` even though they need only a root observation and
  session pool guards.
- `cold_row_visible_mvcc` requires a non-optional reader status in addition to
  STS, so it cannot represent a reader with no read-your-own-write identity.
- `UserTableAccessor::table_scan_hot_row` passes `TrxContext` into
  `RowReadAccess::resolve_main_branch_mvcc` even though keyless visibility needs
  only STS and the decision whether an active undo head is reader-owned.

`TableRootSnapshot<'read>` already owns copied root fields, but its phantom
lifetime is a correctness proof: it prevents transaction and private
maintenance readers from using captured CoW roots after their active-STS owner
ends. It also carries secondary-index roots and deletion-cutoff state used by
index mutation and secondary MemIndex cleanup. It is therefore not obsolete
when the registered snapshot path introduces a structurally pinned checkout.

The future frozen read-snapshot core has a different ownership shape. It must
store captured root fields beside the same active-STS registration that makes
them safe, which cannot be expressed by storing a `TableRootSnapshot<'_>`
self-reference. The lifetime-free owned scan root solves storage, while a view
borrowed from an exact checkout restores usable access without permitting the
stored artifact to escape independently.

The focused pre-task baseline passed:

```text
rtk cargo nextest run -p doradb-storage table_scan_mvcc
cargo nextest: 17 passed, 1674 skipped
```

### RFC Phase Contract

- Phase 1 has no explicit predecessor. Its production prerequisite is the
  existing proof-bound transaction table stream and shared cold/hot scan
  implementation.
- The abstraction remains table-scan-specific. Index runtimes, point reads,
  mutations, and maintenance root ownership are not generalized.
- Own status is represented as `Option<Arc<SharedTrxStatus>>`. No synthetic
  transaction status or transaction ID is allocated for an ownerless reader.
- Stored owned scan-root fields are exposed only through a private borrowed
  view that Phase 2 will return from its exact checkout.
- After this phase, the existing transaction stream exercises all shared scan
  algorithms, and Phase 2 receives `MvccReadView`, `OwnedTableScanRoot`,
  `CheckedOutTableScanRoot<'_>`, `TableScanRuntime<'_>`, and the shared root
  adapter as implementation prerequisites.
- Phase 2's preparation, registry ownership, active-STS registration,
  metadata-lock, table-binding, planning, abandonment, close, and cleanup
  assumptions do not change.
- RFC design text needs no architectural revision: RFC-0030 explicitly leaves
  private names and exact factoring open while fixing this ownership shape.
  During `$task-resolve`, synchronize the Phase 1 task path, task issue, status,
  and implementation summary and confirm the Phase 2 prerequisites.

## Goals

1. Add an immutable, scan-specific `MvccReadView` containing reader STS and an
   optional own `SharedTrxStatus`.
2. Preserve transaction read-your-own insert, update, and delete behavior with
   `own_status: Some(...)` while making `own_status: None` treat every active
   undo head and column-deletion marker as foreign.
3. Refactor cold CDB visibility and hot main-undo traversal around the same
   transaction-neutral identity without duplicating either algorithm.
4. Restrict physical table-scan runtime input to the session pool guards needed
   for cold-index/block and hot-page access.
5. Let worklist capture consume either the existing proof-branded transaction
   root or the future checkout-borrowed registered-snapshot root through one
   statically dispatched scan-root interface.
6. Add a lifetime-free, scan-only stored root projection that has no directly
   usable root access and cannot be confused with the full transaction and
   maintenance root snapshot.
7. Route `Transaction::table_scan_mvcc_stream` through the new read-view,
   runtime, and root adapters as this phase's real production consumer.
8. Preserve every existing table-stream result, projection/filter callback,
   cold-before-hot physical order, captured-pivot behavior, error attachment,
   bounded live-state, exhaustion, stop, error, cancellation, and drop
   contract.
9. Leave Phase 2 with decision-complete private primitives rather than a
   public or synthetic snapshot shell.

## Non-Goals

1. Do not replace, erase the lifetime of, or change the transaction and
   maintenance semantics of `TableRootSnapshot<'read>`.
2. Do not migrate secondary-index reads, index mutation streams, row mutation,
   secondary MemIndex cleanup, or any other non-table-scan root consumer.
3. Do not add `ReadSnapshot`, `ReadSnapshotBuilder`, registry operation states,
   active-STS ownership, frozen table bindings, logical-lock acquisition,
   planning checkouts, or terminal cleanup.
4. Do not add table-scan partitions, partition `open`, independent streams, or
   caller-scheduled parallel execution.
5. Do not add Arrow, vectorized decoding, DataFusion, predicates, aggregation,
   a query executor, or a new result representation.
6. Do not change public transaction APIs, `LazyRow`, `ScanRowDecision`, output
   projection rules, or the custom `next()` stream contract.
7. Do not change index runtime behavior, transaction lifecycle, undo/redo,
   commit/rollback, garbage collection, checkpoint, recovery, persisted
   formats, or table-file publication.
8. Do not introduce a per-row registry lookup, checkout, lock, status clone, or
   allocation.
9. Do not add a compile-fail framework, doctest workflow, or test-only public
   execution API.

## Rejected Alternatives

1. **Replace `TableRootSnapshot<'_>` with the lifetime-free owned root
   everywhere.** Structural checkout ownership proves safety only for the
   future frozen read-snapshot core. Existing transaction and maintenance
   readers use the proof lifetime to keep their active STS live, and their root
   snapshot also supplies secondary-index roots and deletion-cutoff state.
   Replacement would touch index reads, mutation streams, row writes, and
   secondary MemIndex cleanup, violate Phase 1's index/runtime and lifecycle
   boundaries, and require either a broader stored root or another authority
   wrapper at every existing call site. Retaining the full proof-branded root
   and naming the new artifact `OwnedTableScanRoot` makes the two roles
   explicit.
2. **Generalize one read runtime and owned root across every immutable table and
   index read.** A universal read capability could help if shared snapshots
   later gain index scans, but no immediate Phase 1 consumer requires that
   surface. It would expand this task into point lookup, secondary-index, and
   mutation-adjacent code and require material RFC phase-plan changes. The
   selected scan-only adapters can be generalized later from demonstrated
   consumers.

## Plan

### 1. Preserve the existing root proof and add the scan-only stored shape

Keep `TableRootSnapshot<'read>` and all current accessors and consumers
semantically unchanged in `doradb-storage/src/table/mod.rs`. Add a dedicated
private `doradb-storage/src/table/scan_root.rs` module and implement its
scan-root trait for the existing snapshot only so table-scan worklist
construction can read the captured pivot and column-block-index root.

Define the following shapes inside `scan_root.rs` so their private fields are
not visible to sibling table modules. Re-export only the opaque types and trait
at the narrow crate-private boundary needed by scan code:

```rust,ignore
struct OwnedTableScanRoot {
    root_ts: TrxID,
    effective_ts: TrxID,
    pivot_row_id: RowID,
    column_block_index_root: BlockID,
}

struct CheckedOutTableScanRoot<'checkout> {
    root: &'checkout OwnedTableScanRoot,
}

trait TableScanRootView {
    fn pivot_row_id(&self) -> RowID;
    fn column_block_index_root(&self) -> BlockID;
}
```

Implement `TableScanRootView` only for `TableRootSnapshot<'_>` and
`CheckedOutTableScanRoot<'_>`. The trait remains crate-private, and a private
negative implementation assertion ensures `OwnedTableScanRoot` itself does
not become directly usable. Calls use generic static dispatch; do not
introduce `dyn TableScanRootView` or a vtable.

`OwnedTableScanRoot` must:

- copy all four fields from one `ActiveRoot` observation;
- expose no direct root-field getters;
- not implement `TableScanRootView`;
- not implement `Clone` or `Copy`;
- provide no `view()` method that lets an arbitrary holder mint a checked-out
  view; and
- remain crate-private and unexported.

`CheckedOutTableScanRoot<'checkout>` alone exposes the stored root fields needed
by Phase 2 diagnostics and implements `TableScanRootView` for scan planning.
Its borrow is the future structural proof: Phase 2's
`ReadSnapshotCheckout::table_root` will look up the root inside the frozen core
and return the view with a lifetime borrowed from `&self`. No root field or view
can then outlive the checkout pin that keeps the same core's active-STS
registration alive.

Phase 1 does not construct `OwnedTableScanRoot` from the transaction stream and
does not add a fake checkout. Keep production view construction unavailable in
Phase 1; private tests inside `scan_root.rs` may construct the checked-out view
directly, and Phase 2 adds the production constructor at the exact checkout
lookup boundary. Keep owned-root capture narrow and use scoped
`#[cfg_attr(not(test), expect(dead_code, reason = "..."))]` attributes only
where the Phase 2 production consumer is not yet present.

### 2. Add transaction-neutral scan MVCC identity

Add `MvccReadView` near `TrxContext` and `SharedTrxStatus` in
`doradb-storage/src/trx/mod.rs`:

```rust,ignore
struct MvccReadView {
    sts: TrxID,
    own_status: Option<Arc<SharedTrxStatus>>,
}
```

Provide narrow crate-private construction for:

- a transaction-backed view that clones the transaction status once when a
  table stream is constructed; and
- an ownerless view with `None` for private Phase 1 tests and Phase 2 reuse.

Expose only STS and ownership predicates required by the scan algorithms
through a crate-private `MvccVisibility` trait implemented by `TrxContext` and
`MvccReadView`. Ownership of an active `SharedTrxStatus` is pointer identity,
matching current CDB behavior; `None` never reports ownership.

In `doradb-storage/src/trx/row.rs`, factor
`RowReadAccess::resolve_main_branch_mvcc` so one generic algorithm consumes
only `&impl MvccVisibility`. Current non-stream keyless callers pass a borrowed
`TrxContext`, while table scanning passes `MvccReadView`. Do not clone an `Arc`
per row or fork the insert/update/delete/lock traversal.

Preserve the existing timestamp comparisons exactly:

- committed latest hot state is visible only under the current strict STS/CTS
  comparison;
- reader-owned active state uses the latest image;
- a foreign or ownerless active head traverses the main undo branch; and
- repeated sparse update before-images continue to apply newest-to-oldest until
  the selected historical version is reconstructed.

Change `cold_row_visible_mvcc` and
`UserTableAccessor::table_scan_cold_row` to consume `&MvccReadView`. Preserve
CDB-over-durable precedence:

- a committed marker newer than STS preserves the old cold image;
- a committed marker visible to STS hides it;
- an active reader-owned marker hides it only for `Some(same_status)`;
- every active marker is foreign for `None`; and
- durable delete membership is final only when no newer CDB marker exists.

### 3. Separate physical scan runtime and root inputs

Add a copyable private `TableScanRuntime<'runtime>` near
`TableScanWorklist` in `doradb-storage/src/table/access.rs`. It contains only
`&PoolGuards` and can be constructed from the current `TrxRuntime` without
retaining transaction status, locks, effects, or a read proof. Phase 2 will
construct the same view from its operation-local checkout attachment.

Refactor the scan helpers as follows:

```rust,ignore
async fn table_scan_mvcc_worklist(
    &self,
    runtime: TableScanRuntime<'_>,
    root: &impl TableScanRootView,
) -> RuntimeResult<TableScanWorklist>;

async fn load_table_scan_cold_page(
    &self,
    runtime: TableScanRuntime<'_>,
    column_root: BlockID,
    pivot_row_id: RowID,
    entry: &ColumnLeafEntry,
) -> RuntimeResult<TableScanColdPage>;

async fn load_table_scan_hot_page(
    &self,
    runtime: TableScanRuntime<'_>,
    descriptor: RowPageDescriptor,
) -> RuntimeResult<PageSharedGuard<RowPage>>;
```

Keep the existing helper names unless a narrow rename materially improves the
final call path. Worklist construction must copy `column_root` and
`pivot_row_id` from one live `TableScanRootView`, collect cold leaf entries from
that root, and snapshot original hot descriptors from that pivot while the
caller's root authority remains live across awaits. `TableScanWorklist` retains
its current owned value shape.

Continue to use the same table file, sparse file, pools, guards, integrity
validation, captured-page identity checks, and diagnostic operation names.
This is capability refactoring, not an I/O or error-policy change.

### 4. Adapt the existing transaction stream

In `StreamStmtState::table_scan_mvcc_stream`:

1. Preserve table admission and projection validation order.
2. Create one transaction-backed `MvccReadView`.
3. Capture the existing `TableRootSnapshot<'_>` from the transaction proof.
4. Adapt `TrxRuntime` to `TableScanRuntime`.
5. Pass the proof-branded root through `TableScanRootView` to capture the
   worklist.
6. Move the read view and worklist into `TableScanMvccStreamState`.

Store `MvccReadView` before `StreamStmtState` in
`TableScanMvccStreamState`. Keep `StreamStmtState` last so the callback, current
cold block or hot-page guard, worklist queue, row buffer, read view, table, and
layout all drop before the transaction checkout returns.

During polling:

- adapt the retained statement runtime to `TableScanRuntime` only at physical
  page-loading boundaries;
- pass `&MvccReadView` to cold and hot row visibility;
- retain the current hot-page guard across returned rows exactly as today; and
- keep `next()` terminal cleanup unchanged: first error, `Stop`, or exhaustion
  removes the complete optional state before returning, and `Drop` remains the
  idempotent fallback.

The resulting production call path is:

```text
Transaction::table_scan_mvcc_stream
-> StreamStmtState::table_scan_mvcc_stream
-> transaction TableRootSnapshot + MvccReadView + TableScanRuntime
-> UserTableAccessor::table_scan_mvcc_worklist
-> TableScanMvccStreamState
-> cold/hot page load through TableScanRuntime
-> cold/hot row visibility through MvccReadView
```

### 5. Documentation, review, and validation

Update `docs/transaction-system.md` and `docs/table-file.md` only as needed to
distinguish:

- the existing proof-branded full root used by transaction and maintenance
  readers; and
- the scan-only owned root whose future usable view is checkout-borrowed.

Do not document or export a public `ReadSnapshot` API in this phase. Preserve
existing public error inventory because no public constructor or error class is
added.

Run a final unit-test deduplication pass. Reuse existing table test fixtures and
helpers, prefer table-driven visibility matrices, and add no scheduler sleeps.

### Risks and safeguards

- **MVCC boundary drift:** retain the existing strict timestamp comparisons and
  prove them with direct committed-before/after-STS cases.
- **Read-your-own-write regression:** use pointer-exact optional status and run
  all existing transaction scan coverage through the adapter.
- **Per-row overhead:** clone the transaction status once at stream
  construction; use borrowed/generic adapters and static dispatch in the row
  path.
- **Root authority escape:** keep owned fields private, omit direct accessors
  and clone traits, omit the scan-root implementation, and construct usable
  views only through the narrow checked-out seam.
- **Checkout drop-order regression:** declare operation checkout state last and
  test transaction reuse immediately after every terminal route.
- **Scope expansion:** do not modify index, mutation, or maintenance consumers
  of `TableRootSnapshot<'_>`.

## Implementation Notes

## Impacts

- `doradb-storage/src/trx/mod.rs`:
  - adds `MvccReadView`, optional own-status identity, and the crate-private
    `MvccVisibility` abstraction;
  - keeps `TrxContext` and transaction lifecycle semantics unchanged.
- `doradb-storage/src/trx/row.rs`:
  - makes the shared keyless main-branch traversal transaction-neutral;
  - accepts both `TrxContext` and `MvccReadView` through static generic dispatch.
- `doradb-storage/src/table/mod.rs`:
  - retains `TableRootSnapshot<'read>`;
  - registers and narrowly re-exports the private scan-root module.
- `doradb-storage/src/table/scan_root.rs`:
  - adds opaque `OwnedTableScanRoot`, `CheckedOutTableScanRoot<'_>`, and
    crate-private `TableScanRootView` implementations;
  - keeps owned fields private from sibling table modules and provides no
    Phase 1 production view constructor.
- `doradb-storage/src/table/access.rs`:
  - adds `TableScanRuntime<'_>`;
  - generalizes only full-table worklist, page-loading, and row-visibility
    helpers;
  - preserves `TableScanWorklist`, `TableScanColdPage`, `LazyRow`, and physical
    traversal behavior.
- `doradb-storage/src/trx/stream_stmt.rs`:
  - adapts the existing public transaction stream;
  - adds one `Arc` status clone per constructed table stream, not per row;
  - preserves checkout-last state ownership.
- `doradb-storage/src/index/mod.rs`,
  `doradb-storage/src/index/borrowed_stream.rs`,
  `doradb-storage/src/table/index_mutate.rs`, and
  `doradb-storage/src/table/gc.rs`:
  - no `TableRootSnapshot<'_>` migration or semantic change.
- `docs/transaction-system.md` and `docs/table-file.md`:
  - terminology updates only when necessary to record the two root-authority
    shapes.
- Public API, public error taxonomy, durability, persisted formats, memory page
  formats, checkpoint, recovery, and I/O backends are unchanged.
- No unsafe code is expected. Any unexpected unsafe change requires local
  `// SAFETY:` justification and review under the repository lint policy.

## Test Cases

1. Construct transaction-backed and ownerless `MvccReadView` values and verify
   STS access plus pointer-exact same, foreign, and absent ownership.
2. Use a table-driven cold visibility matrix covering no marker, durable
   delete, committed delete before/after reader STS, active same-owner marker,
   active foreign marker, and ownerless active marker. Preserve
   CDB-over-durable precedence.
3. Cover hot main-branch latest, insert, delete, lock, single update, and
   repeated sparse update traversal for transaction-owned, foreign, and
   ownerless active heads. An ownerless reader must reconstruct the same old
   image as a foreign transaction at the same STS.
4. Compile and execute worklist capture with `TableRootSnapshot<'_>` through
   `TableScanRootView` and verify the captured column root, pivot, cold entries,
   and hot descriptors remain unchanged.
5. Construct a private `OwnedTableScanRoot` fixture and
   `CheckedOutTableScanRoot<'_>` view; verify the checked-out view exposes the
   exact four copied fields and drives the same scan-root projection.
6. Add a test-only negative implementation assertion, following the existing
   repository pattern, that `OwnedTableScanRoot` does not implement
   `TableScanRootView`. Assert positively that the checked-out view does. Add no
   trybuild or doctest dependency.
7. Run the existing transaction stream over empty, cold-only, hot-only, and
   mixed tables and compare all rows and physical order with pre-refactor
   behavior.
8. Preserve current transaction read-your-own uncommitted insert, hot update,
   hot delete, cold update, and cold delete behavior. Preserve foreign active
   and committed-before/after-snapshot behavior.
9. Preserve repeated hot-update reconstruction, lazy buffer clearing,
   non-projected callback-column access, and projection validation.
10. Capture a stream before freeze/checkpoint publication and verify its
    original hot-page descriptors produce no omission or duplicate afterward.
11. Exercise include, skip, stop, callback error, storage/integrity error,
    exhaustion, repeated terminal `next()`, dropped pending work, and early
    stream drop. Verify the transaction can immediately run `noop` or another
    stream after terminal detach.
12. Verify current hot-page guard retention and release behavior remains
    unchanged.
13. Run formatting, strict lint, style, coverage, and authoritative validation:

    ```bash
    rtk cargo fmt --check
    rtk cargo clippy --workspace --all-targets -- -D warnings
    tools/style_audit.rs
    tools/coverage_focus.rs \
      --path doradb-storage/src/trx \
      --path doradb-storage/src/table
    rtk cargo nextest run --workspace
    rtk cargo nextest run -p doradb-storage \
      --no-default-features --features libaio
    ```

    Treat 80% focused coverage as the default review bar. The alternate backend
    pass is required because this task changes backend-neutral scan page-loading
    capability plumbing even though it does not change I/O behavior.

## Open Questions

None for this task scope. During `$task-resolve`, synchronize RFC-0030 Phase 1
and verify that Phase 2 still receives the approved read-view, owned-root,
checkout-view, runtime, and shared worklist prerequisites.
