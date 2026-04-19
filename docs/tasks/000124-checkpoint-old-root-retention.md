---
id: 000124
title: Checkpoint Old Root Retention
status: proposal
created: 2026-04-19
github_issue: 570
---

# Task: Checkpoint Old Root Retention

## Summary

Retain swapped table-file active roots through the transaction GC lifecycle
instead of dropping them directly after checkpoint publication. A checkpoint
currently publishes a new table-file root, receives the previous `OldRoot`
guard from `MutableTableFile::commit`, updates the in-memory block index, and
then drops the old root before the checkpoint transaction commits. That can
reclaim the old `ActiveRoot` object before the oldest-active-snapshot horizon
proves no active transaction can still depend on that root snapshot.

This task should move old table-root ownership into checkpoint transaction
payloads and release it through the existing committed-transaction purge path.
It should also audit production call sites that read multiple `active_root()`
fields without binding one local root reference. A fuller proof-gated
`ActiveRoot` API is intentionally deferred because it requires a broader
transaction context/effects split.

## Context

The table-file design is copy-on-write: checkpoints write new blocks and a new
metadata root, atomically switch the super block to that root, and reclaim old
pages later. The GC docs describe the same ownership boundary: collectors need
proof that state is unreachable for every active snapshot, and old DiskTree
pages are reclaimable only after the table-file CoW root that references them
is no longer reachable by active readers.

The current implementation exposes a gap in that contract. `CowFile::publish_root`
writes the new meta block and super block, fsyncs, swaps the active-root
pointer, and returns an `OldCowRoot`. Dropping `OldCowRoot` reclaims the boxed
previous `ActiveRoot`. `Table::checkpoint` currently drops the returned
`old_root` immediately before committing the checkpoint transaction.

The transaction GC path already has the right horizon. Committed transactions
are queued per GC bucket and are purged only when `committed.cts <
min_active_sts`. The transaction payload already carries delayed cleanup state
such as row undo, index GC entries, and row pages to deallocate. Old table roots
should join that payload rather than using a parallel retention queue.

Planning considered refactoring `MemIndexCleanupSnapshot` to borrow an
`ActiveRoot` reference after old-root retention is fixed. That direction needs
a safe proof API for runtime active-root access. A proof borrowed directly from
`ActiveTrx` can block later `&mut ActiveTrx` or `&mut Statement` operations,
so the better long-term design is to split immutable transaction context from
mutable transaction/statement effects. That broader refactor is deferred to
`docs/backlogs/000093-transaction-context-effects-split-active-root-proofs.md`.

Related call-site analysis also found a more general consistency hazard:
`TableFile::active_root()` loads the current atomic root pointer on every call.
Code that calls it repeatedly for related fields can observe a mixed logical
snapshot if a checkpoint swaps roots between calls. Production call sites that
need a coherent root view should bind one local root reference.

Issue Labels:
- type:task
- priority:high
- codex

Source Backlogs:
- docs/backlogs/000089-checkpoint-old-root-retention.md

Related Backlogs:
- docs/backlogs/000093-transaction-context-effects-split-active-root-proofs.md

Related Design:
- docs/rfcs/0014-dual-tree-secondary-index.md
- docs/tasks/000121-secondary-index-cleanup-hardening.md
- docs/garbage-collect.md
- docs/table-file.md
- docs/checkpoint-and-recovery.md
- docs/process/unit-test.md

## Goals

1. Keep swapped user-table `OldRoot` guards alive after checkpoint publication
   until transaction GC reaches a snapshot-safe point.
2. Remove direct `drop(old_root)` ownership from `Table::checkpoint`.
3. Carry the checkpoint transaction's optional retained old table root through
   the existing active, prepared, precommit, committed, and purge transaction
   lifecycle.
4. Release the retained root by dropping committed transaction payloads only
   after `cts < min_active_sts`.
5. Ensure rollback, precommit abort, no-op prepared rollback, and fatal cleanup
   paths drop any retained root safely without leaking ownership.
6. Audit production `active_root()` call sites and replace repeated reads of
   related fields with one local root reference when a coherent table-root
   snapshot is required.
7. Document the `TableFile::active_root()` caller contract: callers that need
   multiple fields from one logical root must bind a local reference and reuse
   it.
8. Preserve current table-file format, redo format, checkpoint publication
   semantics, recovery semantics, and secondary DiskTree publication behavior.
9. Add regression coverage for root-retention timing and root-snapshot
    consistency-sensitive cleanup behavior.

## Non-Goals

1. Do not implement full DiskTree/page reachability GC.
2. Do not change table-file `gc_block_list` page reclamation or page reuse
   policy.
3. Do not add a new global root-retention registry or read-path root guard API.
4. Do not redesign foreground table, index, or column-store access paths.
5. Do not change catalog `MultiTableFile` root retention unless a narrow shared
   helper is required by the user-table implementation.
6. Do not change persistent table-file metadata, redo records, storage version
   compatibility, or recovery replay boundaries.
7. Do not make secondary MemIndex cleanup mutate DiskTree or rebuild cold
   DiskTree entries into MemIndex.
8. Do not resolve the broader DiskTree root/page reclamation follow-up tracked
   by `docs/backlogs/000090-protect-disk-tree-root-lifetime.md`.
9. Do not implement a proof-gated runtime `ActiveRoot` API, transaction
   context/effects split, or `RootReadProof` in this task. That work is tracked
   by `docs/backlogs/000093-transaction-context-effects-split-active-root-proofs.md`.

## Unsafe Considerations

No unsafe code changes are planned. The intended implementation moves ownership
of existing `OldRoot` guards through safe Rust transaction payload structs and
updates safe call-site borrowing patterns.

If implementation unexpectedly changes unsafe pointer, page, buffer, or B+Tree
node code, keep the unsafe boundary private, document every invariant with a
`// SAFETY:` comment, and run:

```bash
cargo clippy -p doradb-storage --all-targets -- -D warnings
```

If the unsafe inventory changes, refresh the relevant unsafe usage tracking and
apply `docs/process/unsafe-review-checklist.md` before resolving this task.

## Plan

1. Extend transaction payload ownership.
   - Add retained table-root ownership to `ActiveTrx`, likely as
     `old_table_root: Option<OldRoot>`.
   - Keep this singular: a user-table checkpoint transaction publishes one
     table root for one table, so a vector would add unnecessary state and
     ambiguity.
   - Add a narrow method such as
     `ActiveTrx::retain_old_table_root(old_root: OldRoot)`.
   - Treat a second old-root attachment to the same transaction as invalid in
     debug builds or through a narrow `Result`, depending on local style.
   - Move the field through `PreparedTrxPayload`, `PrecommitTrxPayload`, and
     `CommittedTrxPayload`.
   - Keep field visibility no wider than necessary.
   - Update direct payload struct literals in tests.

2. Preserve no-op and rollback semantics.
   - Update `ActiveTrx::readonly()` and prepare-time no-redo checks so a
     transaction carrying a retained root is not accidentally collapsed through a
     path that drops roots before the intended lifecycle point.
   - Clear or drop the retained root in active rollback, prepared rollback,
     precommit abort, fatal rollback discard, and payload drop paths.
   - Preserve existing drop assertions by ensuring the retained root is consumed
     or cleared together with redo, undo, index GC, and row-page GC state.

3. Move checkpoint root ownership into the transaction.
   - In `Table::checkpoint`, keep the current order of durable table-file root
     publication and in-memory block-index update.
   - Replace the direct `drop(old_root)` with a move into the checkpoint
     transaction, when an old root is returned.
   - Commit the checkpoint transaction after root retention has been attached.
   - Keep error handling before root publication unchanged.
   - If an error occurs after root publication but before commit, make the
     retained root's release behavior explicit and consistent with the chosen
     rollback/poison path.

4. Release the retained root through purge.
   - Let the committed transaction payload retain the optional old root while
     it sits in the GC bucket committed queue.
   - Rely on the existing purge condition `committed.cts < min_active_sts` to
     delay release until no active snapshot can need the previous root.
   - Make the release point readable, either by documenting that the optional
     root drops when the purged `CommittedTrx` payload is dropped or by adding a
     small helper that explicitly takes and drops the retained root during
     purge.
   - Do not add a second retention queue unless implementation proves the
     transaction payload approach cannot satisfy ownership or layering
     constraints.

5. Keep secondary MemIndex cleanup's owned snapshot shape for now.
   - Do not change `MemIndexCleanupSnapshot` to borrow `&ActiveRoot` in this
     task.
   - Update the temporary comment in `table/gc.rs` so it points to the deferred
     proof/API follow-up instead of implying that old-root retention alone is
     enough to make borrowed `ActiveRoot` snapshots safe.
   - If implementation touches cleanup root capture, keep the existing owned
     scalar snapshot semantics.

6. Audit repeated `active_root()` access in production code.
   - Replace multi-field repeated root loads with one local binding where the
     fields must describe one coherent checkpoint root.
   - Known call sites to inspect and likely adjust:
     - `doradb-storage/src/session.rs` table creation block-index setup;
     - `doradb-storage/src/table/mod.rs` `ColumnStorage::new`;
     - `doradb-storage/src/trx/recover.rs` recovery table-state tracking;
     - any additional production call sites found by searching
       `active_root()`.
   - Leave tests alone unless changing them improves clarity around the new
     contract.

7. Document the root snapshot contract.
   - Update `TableFile::active_root()` docs to say every call observes the
     currently published root and callers needing related fields from a single
     logical root must bind one local reference.
   - If useful, add a short code comment near `CowFile::active_root()` or
     `CowFile::swap_active_root()` describing old-root ownership transfer after
     checkpoint publication.

8. Add focused regression tests.
   - Add transaction-payload tests proving the retained old root survives commit
     queueing and is released only when the committed payload is purged or
     dropped.
   - Add checkpoint-level coverage where a transaction starts before a
     checkpoint publishes a new root; the old root must remain retained until
     that transaction ends and purge advances.
   - Add coverage for any adjusted repeated-`active_root()` call sites where a
     coherent local root binding materially affects behavior.
   - Prefer test-only observation hooks over production indirection if direct
     root-drop timing is otherwise hard to assert.

9. Validate.
   - Run `cargo fmt --all --check`.
   - Run `cargo nextest run -p doradb-storage`.
   - Run focused coverage for changed Rust files or directories, likely:

```bash
tools/coverage_focus.rs --path doradb-storage/src/trx
tools/coverage_focus.rs --path doradb-storage/src/table
```

   - Run `cargo clippy -p doradb-storage --all-targets -- -D warnings` if
     implementation touches unsafe-adjacent or complex ownership code.

## Implementation Notes

## Impacts

- `doradb-storage/src/table/persistence.rs`
  - `TablePersistence::checkpoint`
  - checkpoint old-root handoff after `MutableTableFile::commit`
- `doradb-storage/src/trx/mod.rs`
  - `ActiveTrx`
  - `PreparedTrxPayload`
  - `PrecommitTrxPayload`
  - `CommittedTrxPayload`
  - active/prepared/precommit rollback and drop paths
- `doradb-storage/src/trx/purge.rs`
  - purge release point for committed payloads
  - tests using `CommittedTrxPayload` struct literals
- `doradb-storage/src/file/table_file.rs`
  - `TableFile::active_root` documentation
  - possible test-only root-drop observation support
- `doradb-storage/src/file/cow_file.rs`
  - possible test-only `OldCowRoot` drop observation support
- `doradb-storage/src/table/gc.rs`
  - temporary comment that currently ties owned cleanup snapshots to missing
    old-root retention
- `doradb-storage/src/session.rs`
  - table creation root-field binding
- `doradb-storage/src/table/mod.rs`
  - `ColumnStorage::new`
- `doradb-storage/src/trx/recover.rs`
  - recovery root-field binding
- `docs/backlogs/000089-checkpoint-old-root-retention.md`
  - source backlog to close during `task resolve`
- `docs/backlogs/000093-transaction-context-effects-split-active-root-proofs.md`
  - deferred follow-up backlog referenced by this task

## Test Cases

1. Transaction payload movement:
   - the retained old root moves from `ActiveTrx` to prepared/precommit/committed
     payloads;
   - committed payload drop releases the root;
   - rollback and precommit abort release the retained root immediately.
2. Checkpoint retention:
   - start a transaction before checkpoint;
   - run checkpoint and publish a new table-file root;
   - verify the previous active root is retained while the pre-checkpoint
     transaction remains active;
   - end the transaction and advance purge;
   - verify the old root is released only after purge crosses the checkpoint
     commit timestamp.
3. Active-root consistency audit:
   - add focused unit coverage where practical for adjusted production helpers
     that previously read `active_root()` repeatedly.
4. Regression validation:
   - `cargo nextest run -p doradb-storage`
   - focused coverage for changed `trx` and `table` paths.

## Open Questions

1. Whether root-drop timing can be asserted cleanly with existing APIs or needs
   a narrow `#[cfg(test)]` drop-observation hook on `OldCowRoot`.
2. Whether direct transaction-payload ownership of `Option<OldRoot>` creates an
   undesirable module dependency from `trx` to `file::table_file::OldRoot`; if
   so, use a small retained-resource wrapper in an ownership-neutral module
   instead of adding a second GC queue.
3. Whether catalog `MultiTableFile` old-root retention should receive the same
   treatment in a later task. This task is scoped to the user-table checkpoint
   backlog unless implementation discovers shared generic code that makes the
   catalog case effectively free and low risk.
