---
id: 000281
title: Transaction-neutral scan read view and owned root binding
status: implemented
created: 2026-08-23
github_issue: 1011
---

# Task: Transaction-neutral scan read view and owned root binding

## Summary

Phase 1 of RFC-0030 separated full-table scan MVCC identity, physical runtime
capabilities, and captured-root access from `TrxRuntime`. The existing
transaction table stream is the production consumer, so the refactor shipped
without adding a public snapshot API or a dormant execution path.

`MvccReadView` now owns a reader STS and optional transaction status. The
crate-private `MvccVisibility` trait lets it and `TrxContext` use the same cold
deletion-buffer and hot main-undo visibility algorithms. Transaction-backed
streams clone their status once at construction; ownerless readers allocate no
synthetic status and treat every active owner as foreign.

Physical scan planning and page loading use `TableScanRuntime`, which contains
only session pool guards. Worklist capture accepts a crate-private
`TableScanRootView`, implemented by the existing lifetime-branded
`TableRootSnapshot` and the future checkout-borrowed
`CheckedOutTableScanRoot`. The lifetime-free `OwnedTableScanRoot` remains
unusable by itself.

The transaction stream preserves cold-before-hot ordering, captured-pivot and
hot-page behavior, callback projection, bounded state, diagnostics, and
terminal checkout release.

## Context

Issue Labels:

- type:task
- priority:high
- codex

Parent RFC:

- `docs/rfcs/0030-shared-read-snapshots-parallel-table-scan.md`

RFC Phase:

- Phase 1: Transaction-neutral scan read view and owned root binding

Before this task, the full-table stream captured a proof-branded
`TableRootSnapshot`, but its worklist and page-loading helpers retained the
entire transaction runtime. Cold visibility required a non-optional reader
status, and hot keyless visibility accepted `TrxContext` even though both
algorithms needed only STS and active-owner identity.

`TableRootSnapshot<'read>` remains necessary for transaction, index, mutation,
and maintenance paths. Its proof lifetime keeps the active reader alive while
captured CoW roots are usable, and it also carries secondary-index roots and
deletion-cutoff state outside scan planning.

The future registry-owned snapshot has a different shape: captured scan root
fields must be stored beside the active-STS registration that protects them.
Because that core cannot contain a self-referential `TableRootSnapshot`, Phase
1 introduced a lifetime-free stored projection whose usable view is borrowed
from the exact future checkout.

The task originated directly from RFC-0030 Phase 1 and has no source backlog.

## Goals

1. Represent transaction-backed and ownerless scan visibility without a
   synthetic transaction identity.
2. Preserve pointer-exact read-your-own insert, update, and delete behavior.
3. Share one cold CDB algorithm and one hot main-undo traversal across
   transaction and scan readers.
4. Restrict physical table-scan runtime input to session pool guards.
5. Capture cold and hot work from one authority-bound root observation through
   static dispatch.
6. Provide a lifetime-free stored scan root that cannot expose fields without
   a checkout-borrowed view.
7. Route the existing transaction stream through every new boundary while
   preserving its result and lifecycle contracts.
8. Leave Phase 2 with implementation-ready private primitives.

## Non-Goals

1. No replacement or lifetime erasure of `TableRootSnapshot<'read>`.
2. No index-root, mutation, maintenance, transaction-lifecycle, or persisted
   format migration.
3. No public `ReadSnapshot`, builder, registry state, active-STS owner, table
   binding, planning checkout, or terminal cleanup implementation.
4. No partitions, independent streams, or caller-scheduled parallelism.
5. No Arrow, vectorized decoding, DataFusion, predicates, aggregation, or new
   row representation.
6. No public transaction API, callback, projection, or custom `next()` contract
   change.
7. No per-row registry lookup, checkout, lock, status clone, or allocation.
8. No compile-fail framework, doctest workflow, or test-only public execution
   API.

## Rejected Alternatives

1. **Replace `TableRootSnapshot<'_>` everywhere with the lifetime-free root.**
   Existing transaction and maintenance readers rely on its proof lifetime,
   and index/mutation consumers require fields outside scan planning. A global
   replacement would weaken established authority boundaries and expand the
   phase into unrelated paths.
2. **Generalize one physical runtime and owned root across every immutable
   read.** No Phase 1 consumer required universal point/index capabilities.
   `TableScanRuntime` and the owned root therefore remain scan-specific; only
   the narrow MVCC visibility identity is shared with existing transaction
   readers.

## Plan

### MVCC visibility

`MvccReadView` stores `sts` and `Option<Arc<SharedTrxStatus>>`.
`MvccVisibility` exposes only STS and pointer-exact ownership predicates and is
implemented by `MvccReadView` and `TrxContext`.

`RowReadAccess::resolve_main_branch_mvcc` accepts
`&impl MvccVisibility`. Committed latest state remains visible only when
`reader_sts > cts`; active reader-owned state uses the latest image; foreign or
ownerless active heads traverse the main undo branch. Sparse update
before-images continue to apply newest-to-oldest.

Cold visibility uses the same generic identity. CDB markers retain precedence
over durable delete membership: committed markers newer than STS preserve the
cold image, visible committed markers hide it, and an active marker hides it
only from its exact owner.

### Root authority

`OwnedTableScanRoot` copies root timestamp, effective timestamp, pivot RowID,
and column block-index root from one `ActiveRoot`. It has private fields, no
direct root getters, no `Clone` or `Copy`, no `TableScanRootView`
implementation, and no method that can mint a usable view.

`CheckedOutTableScanRoot<'checkout>` borrows the owned root and exposes the
stored timestamps plus the scan-root projection. Phase 2 will construct it
only at the exact frozen-checkout lookup boundary. Its lifetime prevents the
view from outliving the checkout that pins the same active-STS registration.

`TableScanRootView` is crate-private and statically dispatched. Its only
implementations are `TableRootSnapshot<'_>` and
`CheckedOutTableScanRoot<'_>`. A negative implementation assertion protects
the requirement that the owned stored artifact is not directly usable.

### Physical scan runtime and work capture

`TableScanRuntime<'runtime>` is copyable and contains only `&PoolGuards`.
Transaction streams derive it from `TrxRuntime`; Phase 2 can construct it from
operation-local checkout attachments.

Worklist capture reads the column root and pivot from one live
`TableScanRootView`, collects ordered cold leaf entries, and snapshots original
hot-page descriptors from the captured pivot. `TableScanWorklist` remains an
owned value containing those four components. Cold and hot page loading use
only `TableScanRuntime` and preserve existing I/O, integrity validation, and
diagnostic boundaries.

### Transaction stream integration

Stream construction preserves admission and projection-validation order, then
creates one `MvccReadView`, captures a transaction-branded root, adapts the
physical runtime, and builds the worklist. The read view and owned worklist move
into `TableScanMvccStreamState`.

Polling creates `TableScanRuntime` only at page-loading boundaries and passes
the retained read view to cold and hot visibility. The current hot-page guard
remains retained across returned rows. `StreamStmtState` remains the final
field, so callback, page guards, work queue, row buffer, read view, table, and
layout drop before the transaction checkout returns. Error, stop, exhaustion,
cancellation, and `Drop` retain the existing idempotent terminal behavior.

## Implementation Notes

Phase 1 shipped as a real refactor of `Transaction::table_scan_mvcc_stream`.
No synthetic reader, public snapshot shell, or unused execution branch was
introduced. The resulting primitives are ready for RFC-0030 Phase 2:
`MvccReadView`, `MvccVisibility`, `OwnedTableScanRoot`,
`CheckedOutTableScanRoot<'_>`, `TableScanRuntime<'_>`, and
`TableScanRootView`.

Review produced two accepted deviations from the proposal's initial private
factoring:

- `MvccVisibility` replaced separate transaction/read-view adapters. This also
  lets existing point and index cold-visibility callers use the same generic
  helper without changing their runtime, root ownership, or behavior.
- `MvccVisibility` and `TableScanRootView` remain ordinary crate-private
  traits rather than sealed traits. Foreign crates cannot implement them, and
  root safety continues to come from private fields, unavailable production
  construction, the checkout borrow, and the negative owned-root assertion.

The test-only checked-out-root constructor was removed; the private module test
constructs its fixture directly. Final review also added direct worklist
capture verification and explicit empty-stream exhaustion plus immediate
transaction-reuse coverage.

Verification completed with:

- mandatory style audit against `origin/main`, including strict workspace
  Clippy: passed for 6 Rust files;
- focused `table_scan_mvcc` suite: 19 passed;
- workspace nextest suite: 1,781 passed;
- alternate `libaio` nextest suite: 1,698 passed;
- focused coverage for `doradb-storage/src/trx` and
  `doradb-storage/src/table`: 93.57% combined, above the 80% review bar.

No unsafe code, public error inventory, persisted format, durability protocol,
or user-visible API changed.

## Impacts

- Transaction MVCC and table access now share transaction-neutral visibility
  through static dispatch.
- Full-table worklist and page-loading helpers depend on scan-specific root and
  physical capabilities rather than `TrxRuntime`.
- The transaction stream clones one status `Arc` per construction and performs
  no new healthy-path per-row allocation, lookup, or lock.
- Existing index, mutation, GC, checkpoint, recovery, and maintenance root
  semantics remain unchanged.
- Transaction and table-file documentation distinguish proof-branded full roots
  from checkout-borrowed scan-only roots.
- Public API, storage formats, schema, compatibility, and operational
  procedures are unchanged.

## Test Cases

Completed acceptance coverage includes:

1. Transaction-backed and ownerless STS plus exact same/foreign/absent status
   ownership.
2. Cold visibility across absent markers, durable deletes, committed boundaries,
   active same/foreign owners, and ownerless readers.
3. Hot latest, lock, insert, update, delete, and repeated sparse-update
   reconstruction for owned, foreign, and ownerless readers.
4. Direct transaction-branded worklist capture with exact root, pivot, cold
   entries, and hot descriptors.
5. Owned and checked-out root projection plus positive and negative trait
   implementation assertions.
6. Empty, cold-only, hot-only, and mixed transaction streams with preserved
   cold-before-hot order.
7. Lazy filtering, callback-only columns, projection validation, repeated
   buffer reuse, and repeated hot-update reconstruction.
8. CDB updates/deletes, persisted delete deltas, and snapshot boundary behavior.
9. Captured hot-page behavior across checkpoint publication and retained
   hot-page guard release.
10. Include, skip, stop, callback/storage/integrity errors, exhaustion,
    repeated terminal calls, early drop, and immediate transaction reuse.

## Open Questions

None for Phase 1. RFC-0030 Phase 2 consumes the private read view, visibility,
owned-root, checked-out-root, runtime, and worklist prerequisites established
here.
