---
id: 000127
title: Root Access Boundary Inventory
status: implemented
created: 2026-04-19
github_issue: 578
---

# Task: Root Access Boundary Inventory

## Summary

Create the Phase 1 migration inventory for RFC-0015. The task classifies every
current `active_root()` and `published_root()` reader, documents the intended
checked versus unchecked root-access boundary, and adds concise comments at the
highest-risk anchors so later phases can introduce `TrxReadProof` and
`TableRootSnapshot` without rediscovering the call-site semantics.

This task is intentionally inventory-first. It does not split transaction or
statement types, change `TableAccess`, add proof-gated snapshots, or remove
existing root access APIs.

## Context

RFC-0015 separates transaction read identity from mutable effects so runtime
table-file root reads can eventually be gated by `TrxReadProof<'ctx>` and
served through `TableRootSnapshot<'ctx>`. The first phase is a root-access
boundary inventory because the current active-root surface includes ordinary
runtime reads, checkpoint publication, GC cleanup, recovery/bootstrap, catalog
load-time logic, file internals, and tests.

`CowFile::active_root()` reads an atomic pointer to the currently published root.
Each call can observe a different root after checkpoint publication, so a caller
that needs multiple root fields must bind one local root view. Today that rule is
documented locally, but the type system does not distinguish checked runtime
reads from unchecked/internal exceptions.

Issue Labels:
- type:task
- priority:high
- codex

Parent RFC:
- docs/rfcs/0015-transaction-context-effects-root-proofs.md

Source Backlogs:
- docs/backlogs/closed/000093-transaction-context-effects-split-active-root-proofs.md

## Goals

- Inventory all `active_root()` and `published_root()` readers under
  `doradb-storage/src`.
- Classify each reader into one of the root-access boundary categories defined
  below.
- Document the future migration target for runtime readers that should use
  `TableRootSnapshot`.
- Document the precondition for paths that remain unchecked/internal, especially
  checkpoint publication, recovery/bootstrap, catalog load-time access, file
  internals, and tests.
- Add concise code comments or rustdoc at the main boundary anchors so future
  phases have stable vocabulary.
- Preserve current behavior and public API shape.

## Non-Goals

- Do not introduce `TrxContext`, `TrxEffects`, `StatementEffects`,
  `TrxReadProof`, or `TableRootSnapshot`.
- Do not change `TableAccess` signatures.
- Do not rename, remove, or seal `active_root()` or `published_root()`.
- Do not rewrite secondary-index runtime access, recovery, catalog load,
  checkpoint publication, or GC cleanup logic.
- Do not implement root-reachability GC or new root-retention behavior.
- Do not add an allowlist enforcement script unless implementation finds a
  compelling low-risk reason; default to inventory and comments only.

## Root-Access Boundary Categories

- `runtime_checked_future`: normal foreground/runtime table reads that should
  later require `TrxReadProof` and consume a `TableRootSnapshot` or a root value
  derived from one.
- `checkpoint_internal`: checkpoint readiness or publication code that
  coordinates root swaps and may keep internal current-root access with
  documented liveness/publication preconditions.
- `gc_captured_snapshot`: cleanup code that must capture one root view and apply
  explicit GC horizon or deletion proof predicates. These are strong
  `TableRootSnapshot` candidates once proof types exist.
- `recovery_bootstrap_unchecked`: restart recovery, table recovery, or
  load-time bootstrap paths that run without surviving user transactions and
  should remain named unchecked/recovery snapshot exceptions.
- `catalog_load_boundary`: catalog user-table loading and replay-floor
  computation. These are bootstrap/load boundaries, not normal transaction
  runtime reads.
- `catalog_checkpoint_boundary`: catalog multi-table checkpoint roots. These are
  outside the user-table `TableRootSnapshot` contract.
- `file_internal`: CoW file internals that load, publish, fork, reclaim, or test
  roots at the storage primitive boundary.
- `test_only`: assertions and test helpers that inspect active-root internals.

## Current Inventory

Refresh this inventory with:

```bash
rg -n "active_root\(|published_root\(" doradb-storage/src --glob '*.rs'
```

Initial classification from the current tree:

| Path | Classification | Notes |
| --- | --- | --- |
| `doradb-storage/src/file/cow_file.rs` | `file_internal` | Low-level atomic root primitive, swap, allocate, reclaim, and raw pointer handling. Keep as the primitive boundary. |
| `doradb-storage/src/file/table_file.rs` | `file_internal` | User-table root forwarding, loaded-root install, mutable fork, and file-level tests. Add boundary rustdoc but do not rename in this task. |
| `doradb-storage/src/file/multi_table_file.rs` | `catalog_checkpoint_boundary` / `file_internal` | Catalog `MultiTableFile` root publication and tests are outside user-table `TableRootSnapshot`. |
| `doradb-storage/src/file/fs.rs` | `test_only` / file-system bootstrap | Existing occurrence is a catalog/multi-table file assertion in tests. |
| `doradb-storage/src/session.rs` | `catalog_load_boundary` | New-table creation reads the freshly committed root to initialize `BlockIndex` and runtime table state. |
| `doradb-storage/src/catalog/mod.rs` | `catalog_load_boundary` / `test_only` | User-table load validates metadata, initializes block index, computes replay floors, and has recovery/checkpoint tests. |
| `doradb-storage/src/table/mod.rs` | `catalog_load_boundary` / `recovery_bootstrap_unchecked` | `ColumnStorage::new` and `Table::new` load root metadata and secondary roots while constructing runtime handles. |
| `doradb-storage/src/index/secondary_index.rs` | `runtime_checked_future` / `test_only` | `SecondaryDiskTreeRuntime::published_root()` and runtime opens should later consume snapshot-derived root ids. Existing tests assert publication behavior. |
| `doradb-storage/src/table/gc.rs` | `gc_captured_snapshot` | `MemIndexCleanupSnapshot` already captures root fields as an owned snapshot and should migrate naturally to `TableRootSnapshot`. |
| `doradb-storage/src/table/persistence.rs` | `checkpoint_internal` | `checkpoint_readiness()` and checkpoint publication are root-swap coordination paths with liveness preconditions. |
| `doradb-storage/src/table/recover.rs` | `recovery_bootstrap_unchecked` | Table row recovery uses pivot and deletion cutoff under restart/replay assumptions. |
| `doradb-storage/src/trx/recover.rs` | `recovery_bootstrap_unchecked` / `test_only` | Log recovery tracks replay floors and pivots from loaded table roots; tests assert checkpoint/replay behavior. |
| `doradb-storage/src/trx/mod.rs` | `test_only` | Old-root retention tests inspect active-root pointer identity. |
| `doradb-storage/src/table/tests.rs` | `test_only` | Table, checkpoint, recovery, GC, and secondary-index tests assert active-root fields directly. |

## Unsafe Considerations

This task should not add, remove, or materially change unsafe code. It may update
comments near `CowFile::active_root()` and `TableFile::active_root()`, but the
raw pointer load/reclaim behavior must remain unchanged.

If implementation changes unsafe blocks or unsafe impls in `cow_file.rs`, it
must also:

1. Refresh the unsafe inventory if required by the unsafe review checklist.
2. Update every affected `// SAFETY:` comment with the active-root lifetime and
   reclamation invariant.
3. Run the unsafe review checklist from `docs/process/unsafe-review-checklist.md`.
4. Add tests that exercise checkpoint root swaps while old roots remain retained.

Expected implementation path: no unsafe changes.

## Plan

1. Re-run the inventory command and compare the results with the table above.
2. Add or tighten rustdoc on `CowFile::active_root()` and `TableFile::active_root()`
   to define current-root access as unconstrained and transitional.
3. Add comments at the future migration anchors:
   - `SecondaryDiskTreeRuntime::published_root()` and the runtime open paths.
   - `MemIndexCleanupSnapshot` construction in `table/gc.rs`.
   - `checkpoint_readiness()` and mutable-root fork/publication in
     `table/persistence.rs`.
   - table and transaction recovery active-root reads.
   - catalog user-table load and replay-floor active-root reads.
4. Keep comments factual: category, current precondition, and future RFC phase
   target. Avoid long design restatements in code.
5. Do not introduce new helper APIs by default. If a naming-only helper proves
   clearer than comments, it must be private or crate-private, behavior
   preserving, and explicitly documented as non-proof-gated.
6. Ensure this task doc remains the complete Phase 1 inventory source until
   `task resolve` syncs the parent RFC phase block.

## Implementation Notes

Implemented Phase 1 as a behavior-preserving root-access boundary inventory.
The implementation refreshed the `active_root()` / `published_root()` search,
kept the existing inventory categories, and added concise rustdoc or inline
comments at the main anchors for:

- low-level CoW and table-file current-root boundaries;
- catalog `MultiTableFile` checkpoint roots outside the user-table snapshot
  contract;
- secondary-index runtime roots that should later come from
  `TableRootSnapshot`;
- secondary MemIndex cleanup's owned captured root fields;
- checkpoint readiness/publication as `checkpoint_internal`;
- catalog load/replay-floor boundaries;
- recovery/bootstrap unchecked root reads.

No APIs, helper types, visibility, control flow, unsafe blocks, tests, or
runtime behavior changed. The task intentionally did not introduce
`TrxReadProof`, `TableRootSnapshot`, transaction/statement splits, an allowlist
script, or root-access renames.

Validation and review:

- `cargo build -p doradb-storage` passed.
- `cargo nextest run -p doradb-storage` passed: 610 tests, 610 passed.
- `tools/coverage_focus.rs --path doradb-storage/src` passed with 92.32%
  focused line coverage for the source tree.
- Checklist review found no unsafe changes, no performance-sensitive behavior
  changes, no test-only code changes, and no required follow-up backlog items.
- PR opened: #579.

## Impacts

- `doradb-storage/src/file/cow_file.rs`: low-level current-root primitive and
  unsafe pointer boundary documentation.
- `doradb-storage/src/file/table_file.rs`: user-table current-root forwarding
  documentation and test-only root assertions.
- `doradb-storage/src/index/secondary_index.rs`: runtime published secondary
  root access, future `TableRootSnapshot` consumer.
- `doradb-storage/src/table/gc.rs`: captured cleanup snapshot, future
  `TableRootSnapshot` migration target.
- `doradb-storage/src/table/persistence.rs`: checkpoint liveness and root
  publication coordination.
- `doradb-storage/src/table/recover.rs`: table-level recovery exception.
- `doradb-storage/src/trx/recover.rs`: log recovery and replay-floor exception.
- `doradb-storage/src/catalog/mod.rs`: catalog user-table load and replay-floor
  boundary.
- `doradb-storage/src/session.rs`: new-table bootstrap after table-file commit.
- `doradb-storage/src/file/multi_table_file.rs` and catalog checkpoint modules:
  catalog checkpoint root boundary, outside user-table snapshot contract.
- `doradb-storage/src/table/tests.rs`, file tests, secondary-index tests, and
  recovery tests: test-only active-root assertions remain allowed.

## Test Cases

- For comments/docs-only implementation, run:

  ```bash
  cargo build -p doradb-storage
  ```

- If implementation changes compiled code, helper APIs, visibility, or test
  code, also run:

  ```bash
  cargo nextest run -p doradb-storage
  ```

- If implementation changes runtime Rust behavior, run focused coverage for the
  changed module or directory, for example:

  ```bash
  tools/coverage_focus.rs --path doradb-storage/src/table
  ```

## Open Questions

- Should Phase 1 produce a permanent design note in addition to this task doc?
  Current decision: no; RFC-0015 remains the permanent design anchor.
- Should `TableFile::active_root()` be renamed in this phase? Current decision:
  no; renaming and sealing belong to Phase 6 after checked APIs exist.
- Should a static allowlist script enforce the categories? Current decision:
  defer until the checked API exists and the categories have survived the first
  migration phases.
