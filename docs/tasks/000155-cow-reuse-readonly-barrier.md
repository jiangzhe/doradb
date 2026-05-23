---
id: 000155
title: CoW Reuse Readonly Barrier
status: implemented
created: 2026-05-22
github_issue: 651
---

# Task: CoW Reuse Readonly Barrier

## Summary

Add a centralized allocation-time reuse barrier for user-table CoW block reuse
so a physical block id reclaimed by root reachability cannot return stale bytes
from the readonly buffer pool after it is reallocated. The barrier retires
resident readonly mappings for the reused physical `(file_id, block_id)` and
fails the owning operation with a typed internal error if a same-key readonly
miss is still in flight. The barrier must not poison storage directly or depend
on `TransactionSystem`; checkpoint, DDL, and tests keep ownership of their own
rollback, retry, or fatal handling policy.

## Context

Issue Labels:
- type:task
- priority:high
- codex

Source Backlogs:
- docs/backlogs/closed/000021-readonly-page-reuse-invalidation-trigger-placement.md

Readonly pages are cached by physical identity in
`ReadonlyBufferPool::{mappings, inflight_loads}` using `BlockKey { file_id,
block_id }`. This preserves hits across root swaps for unchanged blocks, but it
also means a reclaimed block id can alias stale resident bytes after a later
CoW allocation reuses that id. Page validators detect corruption or the wrong
page kind; they do not distinguish two valid same-kind images written at
different allocation lifetimes.

User-table root reachability already provides the reclaim proof. Checkpoint
waits until the active root's runtime `effective_ts` is older than
`Global_Min_Active_STS`, then rebuilds the mutable root allocation map from the
current active root and the mutable root about to be published. At that point no
active transaction should legally reach a block that was reachable only from an
older displaced root. The readonly pool, however, is an asynchronous cache and
does not encode that transaction proof in its own key space.

This task chooses allocation-time piggyback invalidation instead of a
standalone invalidation process or generation-key redesign. It also chooses a
caller-local fatality policy: the CoW/file/buffer layers return an error for
the "should be impossible" in-flight reuse case, while the owning operation
decides whether to retry, roll back, or use existing fatal cleanup paths.

Relevant current files:
- `doradb-storage/src/buffer/readonly.rs`
- `doradb-storage/src/file/cow_file.rs`
- `doradb-storage/src/file/table_file.rs`
- `doradb-storage/src/index/column_block_index.rs`
- `doradb-storage/src/index/column_deletion_blob.rs`
- `doradb-storage/src/index/disk_tree.rs`
- `doradb-storage/src/table/persistence.rs`
- `doradb-storage/src/catalog/index.rs`

Relevant design docs:
- `docs/table-file.md`
- `docs/garbage-collect.md`
- `docs/transaction-system.md`
- `docs/buffer-pool.md`

## Goals

1. Add one production reuse barrier for user-table CoW block allocation that
   invalidates any resident readonly-cache mapping for the allocated physical
   block id before the block is written or published.
2. Make same-key in-flight readonly miss publication an explicit invariant
   violation detected by the barrier and surfaced as a typed internal error.
3. Keep the barrier free of `TransactionSystem` and storage poison
   dependencies. The barrier must return errors; it must not poison the engine.
4. Route all production user-table CoW allocation paths through the barrier,
   including LWC block allocation, `ColumnBlockIndex` nodes, deletion blob pages,
   secondary `DiskTree` nodes, and table meta-block allocation in root publish.
5. Preserve root-reachability correctness: page reuse remains allowed only after
   existing checkpoint/readiness gates prove old roots are no longer observable
   by active transactions.
6. Add regression coverage that demonstrates stale physical-key cache aliasing
   would be possible without the barrier and that the barrier prevents it.

## Non-Goals

1. Do not introduce durable block generations or change persisted block
   references from `BlockID` to `(BlockID, generation)`.
2. Do not redesign the readonly cache key around runtime generations in this
   task.
3. Do not add a standalone invalidation worker, sweeper, or background process.
4. Do not make catalog `catalog.mtb` runtime-read cache behavior a design
   driver. The catalog file is used for checkpoint/recovery boundaries, not
   normal foreground table reads.
5. Do not add a `TransactionSystem` dependency to `ReadonlyBufferPool`,
   `CowFile`, `MutableCowRoot`, or the reuse barrier.
6. Do not solve checkpoint trigger, budget, or reclamation-threshold policy.
   That is tracked separately.

## Unsafe Considerations

This task is not expected to add new unsafe code. It may touch code adjacent to
existing unsafe boundaries in `readonly.rs` read submissions and `cow_file.rs`
active-root pointer ownership, but the intended implementation should not
change those unsafe invariants.

If implementation changes any unsafe block, unsafe impl, or raw-pointer
lifetime contract, it must:
1. update the local `// SAFETY:` comments;
2. preserve active-root retention and readonly frame ownership invariants;
3. apply `docs/process/unsafe-review-checklist.md`;
4. add targeted tests for root swaps and readonly cache reuse while guards or
   submissions are active.

## Plan

1. Add a readonly reuse API in `doradb-storage/src/buffer/readonly.rs`.
   - Add a result type such as `ReadonlyReuseInvalidation` with at least the
     invalidated resident frame id/count needed for diagnostics.
   - Add `ReadonlyBufferPool::invalidate_for_reuse(file_id, block_id)
     -> Result<ReadonlyReuseInvalidation>`.
   - The method should remove any resident mapping for `BlockKey::new(file_id,
     block_id)` using the existing invalidation path.
   - The method should check `inflight_loads` for the same key. If present,
     return a new typed internal error, for example
     `InternalError::ReadonlyReuseInflight`, with file id and block id attached.
   - Production behavior must be an error return, not a panic or engine poison.
     Debug assertions may be used only as additional local invariant checks.

2. Centralize user-table CoW block allocation behind a reuse-aware helper.
   - Add a helper at the CoW/file layer that combines
     `MutableCowRoot::try_allocate_block()` with readonly reuse invalidation.
   - The helper must know the owning `FileID` and the optional/readily available
     user-table readonly pool.
   - Do not leave production code calling `try_allocate_block()` directly for
     user-table CoW allocations.
   - Brand-new table-file creation may use a clearly documented no-reuse path
     because no previous readonly mapping can exist for a file that did not
     exist before.

3. Thread the barrier through `MutableTableFile`.
   - Production mutable user-table forks should carry or receive the
     `ReadonlyBufferPool` needed by the barrier.
   - Test-only or bootstrap constructors that intentionally do not need reuse
     invalidation must be named so the bypass is explicit.
   - `MutableTableFile::allocate_block()` and internal LWC allocation in
     `apply_lwc_blocks()` must use the centralized helper.

4. Cover generic CoW publish meta-block allocation.
   - `CowFile::publish_root()` currently allocates the new meta block directly
     from `MutableCowRoot`.
   - Change the publish path so user-table commits reuse the same barrier before
     writing the new meta block.
   - Keep `MutableMultiTableFile` support mechanical and minimal. If catalog
     commits continue to use a no-reuse path, document why catalog foreground
     reads are not part of the readonly stale-read risk.

5. Update allocation users through the `MutableCowFile` trait.
   - Ensure `ColumnBlockIndex`, `ColumnDeletionBlobWriter`, and `DiskTree`
     allocation paths keep calling `MutableCowFile::allocate_block()` and
     therefore inherit the barrier.
   - Keep rollback behavior unchanged: if a later write fails, allocated block
     ids may be rolled back from the mutable root; any readonly invalidation
     already performed is harmless.

6. Error propagation and owner policy.
   - User-table checkpoint should roll back its checkpoint transaction and
     return the reuse-barrier error if the barrier fails before root publish.
   - Create/drop index paths should preserve their existing post-catalog-commit
     cleanup/fatal behavior if a barrier error occurs after the catalog commit
     boundary.
   - The barrier itself must not call `poison_storage()`.

7. Update documentation.
   - Update `docs/table-file.md` or `docs/buffer-pool.md` to state that reused
     user-table CoW block ids retire readonly-cache resident state at allocation
     time, and that same-key in-flight readonly loads are treated as an
     invariant violation returned to the owning operation.
   - Keep the documentation clear that this is not a catalog runtime-read
     requirement and not a generation-key design.

## Implementation Notes

Implemented on 2026-05-23.

The final design uses a write barrier instead of a one-shot allocation-time
invalidation helper. `ReadonlyBufferPool` now tracks physical block activity in
`InflightBlockState::{Loading, WriteBlocked}` entries keyed by `BlockKey`.
`begin_write_barrier` installs `WriteBlocked`, invalidates any resident mapping
with `invalidate_block`, and returns a `ReadonlyWriteLease`; same-key readonly
misses fail with typed internal errors while the write is blocked. The lease is
carried by backend-owned write submissions and released by the IO completion
path, so cancelling a caller future cannot unblock readonly reads before the
write driver completes the write.

User-table CoW writes now carry the barrier through `MutableTableWriteBarrier`
and `CowWriteBarrier::{ReadonlyPool, Disabled}`. Production user-table forks use
the readonly-pool barrier, while new-file, bootstrap, and catalog mechanical CoW
paths use the explicit disabled variant. `MutableCowFile` allocation and
rollback helpers were renamed to block-oriented APIs, and CoW allocation was
centralized so LWC block writes, column block index nodes, deletion blob blocks,
secondary disk-tree nodes, and root meta-block publication use the same write
barrier path.

Readonly duplicate-load behavior was hardened during review. After reserving a
frame for a miss, the readonly load path re-checks `mappings` before submitting
IO and rolls back the reservation if another path already published the same
key. DashMap guard scopes around `mappings` and `inflights` are explicit and
documented so the code does not hold nested shard locks across the two maps.

The readonly invalidation surface was reduced to `invalidate_block`; the
file-wide invalidation helper was removed. `WriteSubmission::prepare` now
returns a concrete submission/completion pair, with error mapping kept at the
single call site. Related docs were updated in `docs/buffer-pool.md` and
`docs/table-file.md`, and block-id naming was cleaned up in disk-block contexts.

Post-implementation checklist result: pass. No unsafe blocks, unsafe impls, or
raw-pointer lifetime contracts were added or changed.

Validation completed:
- `cargo fmt`
- `cargo clippy -p doradb-storage --all-targets -- -D warnings`
- `cargo nextest run -p doradb-storage` (813 passed)
- `cargo nextest run -p doradb-storage --no-default-features --features libaio`
  (811 passed)
- `tools/coverage_focus.rs --path doradb-storage/src/buffer/readonly.rs --path doradb-storage/src/file/cow_file.rs --path doradb-storage/src/file/table_file.rs --path doradb-storage/src/file/mod.rs --path doradb-storage/src/index/column_block_index.rs --path doradb-storage/src/index/column_deletion_blob.rs --path doradb-storage/src/index/disk_tree.rs --path doradb-storage/src/table/tests.rs`
  (deduplicated 17357/18384, 94.41%)
- `git diff --check`
- `tools/task.rs resolve-task-next-id --task docs/tasks/000155-cow-reuse-readonly-barrier.md`
- `tools/task.rs resolve-task-rfc --task docs/tasks/000155-cow-reuse-readonly-barrier.md`

## Impacts

- `ReadonlyBufferPool`
  - new reuse invalidation API
  - new in-flight reuse invariant check
  - possibly a new internal error variant
- `MutableCowRoot` / `CowFile`
  - allocation helper or publish-root integration point
  - removal of direct production `try_allocate_block()` bypasses for
    user-table allocations
- `MutableTableFile`
  - storage of or access to a readonly reuse barrier
  - `allocate_block()`, `apply_lwc_blocks()`, and `commit()` integration
- `MutableCowFile` trait users
  - `ColumnBlockIndex`
  - `ColumnDeletionBlobWriter`
  - `DiskTree` rewrite/batch writers
- Checkpoint and DDL flows
  - user-table checkpoint error handling before publish
  - create/drop index cleanup behavior around post-catalog-commit table-root
    publication
- Docs
  - table-file or buffer-pool reuse contract update

## Test Cases

1. Readonly resident stale-cache regression:
   - Load or bind a readonly cached frame for `(file_id, block_id)` with old
     valid page bytes.
   - Reallocate the same block id through the new barrier.
   - Write new valid page bytes for the same kind.
   - Verify a later readonly read misses/reloads and returns the new bytes, not
     the old resident frame.

2. Barrier invalidates resident mapping:
   - Install a mapping for a `BlockKey`.
   - Call `invalidate_for_reuse()`.
   - Assert the mapping is gone, the frame becomes reusable/uninitialized, and
     diagnostics report the invalidation.

3. Barrier rejects same-key in-flight load:
   - Create a controlled in-flight readonly miss for one `BlockKey`.
   - Call `invalidate_for_reuse()` for the same key.
   - Assert a typed internal error is returned and no engine poison dependency
     is involved.

4. Allocation path coverage:
   - LWC block allocation uses the barrier.
   - `ColumnBlockIndex` node allocation uses the barrier.
   - deletion blob page allocation uses the barrier.
   - secondary `DiskTree` node allocation uses the barrier.
   - user-table meta-block allocation during root publish uses the barrier.

5. Checkpoint failure behavior:
   - Inject a barrier failure before user-table root publish.
   - Verify checkpoint rolls back/returns an error and does not publish a
     partial root.

6. DDL boundary behavior:
   - Inject a barrier failure in create/drop index table-root publication after
     catalog commit.
   - Verify the existing post-catalog-commit fatal cleanup policy is preserved
     by the owning DDL path, not by the barrier.

7. Validation commands:
   - Run focused tests for readonly and CoW allocation paths.
   - Run `cargo nextest run -p doradb-storage`.
   - If storage backend-neutral I/O behavior changes materially, also run
     `cargo nextest run -p doradb-storage --no-default-features --features libaio`.

## Open Questions

No unresolved open questions remain.

The `MutableTableFile` API now carries the barrier as
`MutableTableWriteBarrier`, backed by `CowWriteBarrier`. Production user-table
forks receive `CowWriteBarrier::ReadonlyPool`; new-file, bootstrap, and catalog
mechanical CoW paths use `CowWriteBarrier::Disabled`.

`MutableMultiTableFile` remains a mechanical CoW consumer and is documented as
outside the runtime readonly-cache correctness path.
