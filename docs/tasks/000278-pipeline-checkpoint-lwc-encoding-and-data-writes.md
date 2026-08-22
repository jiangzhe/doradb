---
id: 000278
title: Pipeline Checkpoint LWC Encoding and Data Writes
status: proposal
created: 2026-08-22
github_issue: 1003
---

# Task: Pipeline Checkpoint LWC Encoding and Data Writes

## Summary

Pipeline user-table checkpoint LWC encoding into table-file data writes so CPU
serialization and shared-storage IO overlap instead of running as two complete,
sequential phases. Replace the current encode queue plus accumulated block
vector plus `apply_lwc_blocks` write-future vector with one logical-order list
whose blocks move monotonically from `Encoding` to `Writing` to `Written`.

The checkpoint keeps only the existing CPU-stage bound: blocks waiting to
reach shared IO are limited by the configured ThreadPool worker count. Once an
encode completes in logical order, the mandatory runner allocates its CoW
block and awaits acceptance into the already-bounded shared storage ingress.
The shared channel, storage scheduler, and backend IO depth provide the actual
write backpressure across all concurrent engine IO; checkpoint does not add a
second local write-depth estimate. After production finishes, one traversal
waits for every accepted data write before column-index construction.

Pass the known checkpoint pivot into LWC production and finalize the last
block's logical shape before CPU encoding. This removes the current
post-encoding fingerprint mutation that can make a dense final LWC block
disagree with its column-index entry when fully deleted pages trail the last
visible row.

## Context

Task 000277 introduced an engine-owned fixed ThreadPool and made user-table
checkpoint its first consumer. Page access, visibility analysis, row copying,
and secondary-index sidecar collection remain on the single mandatory runner;
an owned `LwcBuilder` is submitted for CPU-only serialization, compression,
and checksum generation. `CheckpointLwcEncodeQueue` bounds pending encodes by
the worker count and consumes them in logical RowID order.

The current phase boundary still prevents end-to-end overlap. Successful
encode completions accumulate as `Vec<LwcBlockPersist>` until
`Table::build_lwc_blocks` returns. `MutableTableFile::apply_lwc_blocks` then
allocates every block ID, creates a vector of write futures, awaits
`try_join_all`, and only afterward builds the `ColumnBlockIndex`. A large
checkpoint therefore retains every encoded 64 KiB buffer and does not begin
data writes while CPU production is active.

Task 000277's resolved two-million-row measurements showed effective CPU
scaling but a substantial persistent-IO tail. The four-worker encode and scan
cluster ended near 181 ms in one profile, followed by roughly 380 ms of apply,
write, and publication work. File IO depth one was slower than depths 8 through
64. Early data-block writes are already safe: they are unpublished CoW blocks,
and visibility still depends on the later meta/super-block root swap and
publication fsync. A failed mutable fork may leave unreachable written blocks,
which is existing checkpoint failure behavior.

The table-write path already provides the correct global pressure boundary.
`IOClient<BackgroundWriteRequest>` uses a bounded ingress channel, the shared
storage worker stages and submits work under its configured backend depth, and
other table, cache, and pool IO can compete for the same worker. A second
checkpoint-local write limit would duplicate that capacity control while
ignoring concurrent consumers. Awaiting ingress acceptance naturally stops the
mandatory producer and, because the CPU-stage window cannot advance, stops
additional encode submission as well.

The current call path also adjusts the final `ColumnBlockEntryShape` after its
buffer has been built. `set_end_row_id(new_pivot_row_id)` recomputes the index
fingerprint after the previous fingerprint is embedded in the checksummed LWC
header. If visible rows form a dense last block but later selected pages are
fully deleted, extending the exclusive end changes the logical shape to sparse
and can leave the two fingerprints inconsistent.

Issue Labels:

- type:task
- priority:medium
- codex

Source Backlogs:

- `docs/backlogs/000187-pipeline-checkpoint-lwc-encoding-and-data-writes.md`

This task has no parent RFC. It remains one bounded user-table checkpoint and
table-file write-path change with no persisted-format or recovery migration.

## Goals

- Overlap CPU LWC encoding with accepted table-file data writes during one
  user-table checkpoint.
- Represent every produced block once in one logical-order state list with
  monotonic `Encoding -> Writing -> Written` transitions.
- Preserve logical encode consumption, CoW allocation, write submission, and
  column-index entry order while allowing CPU and physical IO completion order
  to vary.
- Bound CPU-stage occupancy by ThreadPool worker count and propagate shared IO
  backpressure without a checkpoint-local write-depth setting.
- Keep ThreadPool jobs CPU-only and retain no page guard, borrowed vector view,
  latch, logical lock, mutable-root borrow, or IO owner across a CPU wait.
- Define shared-storage ingress acceptance as the write ownership boundary and
  keep every accepted write independently completable after the mutable root
  borrow is released.
- Stop forward production on an observed failure, then drain every accepted
  encode and write while preserving Fatal-over-Runtime precedence.
- Build the column index and update mutable-root data metadata only after all
  LWC data writes succeed.
- Finalize the last LWC block shape against the known checkpoint pivot before
  encoding so the LWC header and index entry carry the same fingerprint.
- Add phase diagnostics and fresh-root benchmark evidence that distinguish CPU
  production, write submission/drain, index rebuild, and publication fsync.

## Non-Goals

- A checkpoint-local write window, IO quota, semaphore, or new configuration
  field; shared storage owns global admission and backend depth.
- A generic staged-pipeline framework or a new engine execution subsystem.
- Pipelining column-index construction, deletion checkpoint, secondary-index
  sidecars, allocation-map rebuilding, meta/super-block writes, or root
  publication with LWC data writes.
- Moving page loading, visibility analysis, sidecar collection, IO, waiting,
  mutex acquisition, or latch acquisition into ThreadPool workers.
- Changing LWC bytes, checksums, column-index formats, table roots, replay
  bounds, recovery rules, MVCC behavior, or checkpoint public APIs.
- Parallel or out-of-logical-order CoW block allocation and write submission.
- Cancellation of accepted CPU or IO work.
- A public checkpoint timing/statistics API or a CI performance threshold.
- Catalog, deletion-only, recovery, CREATE INDEX, or other ThreadPool consumers.

## Rejected Alternatives

### Separate encode, write, and ordered-entry collections

Maintaining a pending-encode deque, an in-flight-write collection, and a third
ordered entry vector duplicates the identity and lifecycle of every block.
One logical-order list can retain the final index entry in the same slot and be
consumed directly after all writes finish. Per-slot state handles asynchronous
completion without another authoritative queue.

### Checkpoint-local write depth

Sizing a local write window from `FileSystemConfig::io_depth` double-counts a
global backend limit and cannot account for concurrent reads, pool IO, catalog
work, or other background writes. The bounded shared ingress and backend
scheduler are the authoritative pressure boundary. Awaiting their acceptance
provides direct feedback while retaining the checkpoint's CPU-stage bound.

### Generic ordered CPU-to-IO pipeline

A reusable stage framework would require generalized job ownership, error
carriers, shutdown contracts, and capacity policy before a second consumer has
proved those abstractions. This task keeps the coordinator checkpoint-private;
future consumers require separate evidence and design.

## Plan

### One logical-order block-state list

Replace `PendingLwcEncode`, `CheckpointLwcEncodeQueue`, its output vector, and
the later write-future vector with a checkpoint-private coordinator in
`doradb-storage/src/table/persistence.rs`. Its semantic core is:

```rust
enum LwcBlockState {
    Encoding {
        shape: ColumnBlockEntryShape,
        completion: Arc<Completion<InternalResult<DirectBuf>>>,
    },
    Writing {
        entry: ColumnBlockEntryInput,
        completion: Arc<Completion<()>>,
    },
    Written {
        entry: ColumnBlockEntryInput,
    },
}

struct CheckpointLwcPipeline<'a> {
    mutable_file: &'a mut MutableTableFile,
    thread_pool: QuiescentGuard<ThreadPool>,
    table_id: TableID,
    blocks: Vec<LwcBlockState>,
    next_to_write: usize,
    encode_limit: usize,
    first_error: Option<RuntimeOrFatalError>,
    // phase timing and test-only high-water observations as needed
}
```

The list remains in logical RowID order. The range before `next_to_write`
contains blocks already accepted by shared IO (`Writing`, or `Written` during
the final traversal). The suffix beginning at `next_to_write` contains
accepted CPU work not yet handed to shared IO. No semantic `Discarded` or
placeholder terminal variant is needed. If moving a state across an await
requires temporary storage, use an implementation-only `Option<LwcBlockState>`
or an equivalent ownership pattern; absence must not become a persistent
pipeline state.

The CPU-stage occupancy is `blocks.len() - next_to_write` and must never exceed
`encode_limit`, which is the already-validated ThreadPool worker count. An
encode completion does not release this capacity. Capacity is released only
after the corresponding write request is accepted and `next_to_write`
advances. Consequently a full or contended shared IO ingress propagates
backpressure through encoded buffers to CPU submission and page production.

### Produce builders and advance ready CPU work

Refactor `Table::build_lwc_blocks` into an operation whose name reflects both
production and writes, such as `build_and_write_lwc_blocks`. It receives the
mutable table-file fork, `new_pivot_row_id`, ThreadPool guard, and the existing
prepared-page and sidecar inputs.

Page loading, vector-view creation, visible-row callbacks, block-split retry,
and `LwcBuilder::append_view` remain unchanged. When a builder becomes full:

1. After the page guard and borrowed vector view leave scope, ask the pipeline
   to advance every consecutive ready CPU result. This does not wait for an
   unready CPU job, but submitting a ready result may await shared-storage
   ingress and deliberately propagate its backpressure.
2. If `blocks.len() - next_to_write == encode_limit`, await the logical head CPU
   completion and advance it through shared-storage acceptance before
   admitting another builder.
3. Append one `Encoding` state with its final logical shape and submit the
   owned `LwcBuilder::build` job immediately.
4. Run another ready-only pass so already-completed predecessors reach storage
   without waiting for the next CPU-capacity boundary.

At each subsequent safe page boundary, run the same ready-only advance. A
later CPU job may finish first, but write submission waits for
`blocks[next_to_write]`, preserving logical allocation and submission order.
The later completion remains safely stored in its completion cell.

At production end, await and submit every remaining `Encoding` state in order.
A final traversal then owns only `Writing` or `Written` states on the success
path.

### Finalize the last logical shape before encoding

Pass `new_pivot_row_id` into LWC production. Hold the last nonempty builder
until the prepared-page scan finishes, as today, but construct its
`ColumnBlockEntryShape` with `end_row_id = new_pivot_row_id` before submitting
the CPU job. This includes fully deleted trailing pages in the logical span
used to classify dense versus sparse shape and calculate the fingerprint.

Remove the later `last.shape.set_end_row_id(new_pivot_row_id)` mutation from
`TableCheckpointer::run`. `ColumnBlockEntryShape::set_end_row_id` has no other
production caller and should be removed so an encoded shape cannot be mutated
without rebuilding its buffer. When all selected pages are fully deleted,
produce no LWC block and retain the existing `apply_checkpoint_metadata` path
for pivot and heap-replay-floor advancement.

### Split write submission from completion waiting

Refactor the internal write path in `doradb-storage/src/file/mod.rs` and
`doradb-storage/src/file/cow_file.rs` so an owned direct write can be accepted
without immediately awaiting completion. Add a narrow helper equivalent to:

```rust
async fn submit_direct_write_with_lease(...)
    -> CompletionResult<Arc<Completion<()>>>;
```

It prepares `WriteSubmission`, asynchronously sends it through the bounded
`IOClient<BackgroundWriteRequest>`, and returns the completion only after
ingress acceptance. On a closed channel, preserve the current captured IO
report and release the request-owned buffer and readonly write lease. Rebuild
the existing `write_direct_with_lease` and `CowFile::write_block_with_lease`
helpers by submitting and then awaiting the returned completion so unrelated
callers retain current behavior.

Add a `MutableTableFile` LWC submission method that:

1. validates logical start/end order against the preceding accepted block;
2. allocates the next CoW block ID on the mandatory runner;
3. attaches it to `ColumnBlockEntryShape`, producing
   `ColumnBlockEntryInput`;
4. starts the readonly-cache write barrier;
5. moves the buffer, file owner, and lease into shared-storage submission; and
6. returns the entry plus completion only after ingress acceptance.

The pipeline replaces its logical-head `Encoding` state with `Writing` and
increments `next_to_write` only after this method succeeds. The in-flight
completion owns no mutable-root borrow. Physical backend completion order may
vary freely.

Do not add a local write count or wait for a write completion during normal
production. If shared IO is full, the awaited send is the authoritative
backpressure point. Accepted write buffers are bounded by the shared ingress,
storage-worker staging, and backend depth; completed IO releases its buffer
even while the small `Writing` entry and completion remain in the checkpoint
list.

### Finish success and drain every error path

Give the pipeline one finish operation that combines producer outcome with
accepted-work draining.

On success:

1. await every remaining CPU result in logical order and submit its write;
2. traverse `blocks` once in logical order;
3. await every `Writing` completion, replace successful states with `Written`,
   and continue after any error so every accepted write is observed;
4. if all writes succeeded, consume `Written` states directly into an ordered
   `Vec<ColumnBlockEntryInput>`; and
5. return those entries for column-index construction.

Awaiting the final traversal in logical order does not serialize physical IO:
all writes have already crossed storage ingress and may finish in any order.
Precompleted completion cells return immediately.

On the first observed producer, encode, allocation, write-barrier, or ingress
submission failure, stop page production and do not submit later successful
encodes as new writes. Consume every remaining `Encoding` completion and drop
successful buffers, then traverse every `Writing` completion. An IO-completion
failure is normally discovered during this final traversal; it prevents index
construction and publication but does not stop the traversal from draining
later accepted writes.

Merge errors with `RuntimeOrFatalError::merge_cleanup`: Fatal outranks Runtime,
and an earlier same-domain operation failure remains primary. The existing
post-transition `TableCheckpointer::resolve` boundary converts ordinary
failures to `CheckpointWrite`, poisons storage, and preserves an already-Fatal
ThreadPool reason. A failed mutable fork may retain unpublished allocations or
unreachable written blocks, but the active root and column-index root remain
unchanged.

Document the wait contract locally:

- ThreadPool workers produce encode completions.
- The shared storage worker produces ingress capacity and write completions.
- Completion cells and bounded-channel acceptance are authoritative results.
- Poison stops new forward work but does not cancel accepted work.
- Mandatory-runtime shutdown drains the accepted checkpoint before ThreadPool
  and storage worker teardown.
- The accepted mandatory checkpoint remains the sole drain and cleanup owner.

### Build the column index only after data-write success

Split `MutableTableFile::apply_lwc_blocks` into the per-block submission method
and a post-drain finalizer such as `finish_lwc_blocks`. The finalizer receives
the already ordered `ColumnBlockEntryInput` vector, constructs
`ColumnBlockIndex`, calls `batch_insert`, and only then updates
`column_block_index_root`, `pivot_row_id`, and `heap_redo_start_ts` on the
mutable root.

No column-index CoW write may begin until every LWC data write completion is
successful. Deletion checkpoint, secondary-index sidecar application,
two-root reachability rebuilding, publication admission, meta/super-block
writes, fsync, root swap, runtime route update, and system-transaction enqueue
remain in their existing order after this finalizer.

### Phase diagnostics

Add structured internal debug diagnostics without changing public checkpoint
outcomes or statistics APIs. Record at least:

- block count and CPU-stage capacity;
- peak CPU-stage occupancy;
- pipeline start through final CPU completion;
- final CPU completion through acceptance of the final LWC data write;
- final-write-acceptance through complete data-write drain;
- column-index rebuild duration; and
- exact root-publication fsync duration in `CowFile::publish_prepared_root`,
  tagged with file identity.

Keep timings diagnostic rather than correctness inputs. Tests synchronize on
completion, submission, and publication predicates rather than elapsed time.

### Deterministic validation and benchmark

Use completion cells and existing storage-backend test hooks to control CPU
and write progress. Add only narrow `#[cfg(test)]` pipeline hooks or constructors
where direct completion injection cannot exercise the production state path.
Do not add production locks, sleeps, or widened APIs for testing.

Re-run the task-000277 deterministic workload: insert 2,100,000 128-byte rows,
freeze the 2,000,320-row prefix, and checkpoint without retry waits. Use a new
storage root for every sample. On the branch, compare the cross-product of
ThreadPool workers `{1, 2, 4}` and shared file IO depths `{1, 8, 64}` with at
least five samples per cell. Record median, minimum/maximum, sample CV, phase
timings, and storage-backend counters. Also run at least five alternating
fresh-root pairs between `origin/main` and the branch at workers four and IO
depth 64. Record results during task resolution; do not add a noisy CI
threshold. If persistent-filesystem variance masks the phase overlap, add a
tmpfs control without replacing the persistent comparison.

### Documentation

Update `docs/architecture.md`, `docs/checkpoint.md`,
`docs/data-checkpoint.md`, and `docs/table-file.md` to describe the one-list
CPU-to-shared-IO pipeline, logical submission order, global backpressure,
accepted-work draining, post-write index construction, and unchanged atomic
publication boundary. Update `docs/benchmark-tool.md` only if commands or
diagnostic interpretation need durable clarification; no benchmark schema
change is planned.

## Implementation Notes

## Impacts

- `doradb-storage/src/table/persistence.rs`
  - replace `CheckpointLwcEncodeQueue` and accumulated `Vec<LwcBlockPersist>`
    with `CheckpointLwcPipeline` and `LwcBlockState`;
  - integrate mutable-file submission into LWC production;
  - pass `new_pivot_row_id` before final encode;
  - preserve sidecar collection and no-guard-across-wait rules; and
  - add pipeline timing and focused state/error tests.
- `doradb-storage/src/file/table_file.rs`
  - replace monolithic `apply_lwc_blocks` and `try_join_all` with ordered
    per-block submission plus post-drain column-index finalization;
  - retain CoW allocation, readonly-cache barrier, index, and root-update
    invariants; and
  - add file-layer submission/finalization tests.
- `doradb-storage/src/file/mod.rs` and
  `doradb-storage/src/file/cow_file.rs`
  - split owned write acceptance from completion waiting while preserving
    existing composed write helpers and error context;
  - expose no public API; and
  - emit exact publication-fsync timing.
- `doradb-storage/src/index/column_block_index.rs`
  - remove `ColumnBlockEntryShape::set_end_row_id` and retain immutable
    construction-time fingerprint semantics.
- Checkpoint and table-file design documents gain pipeline and shared-pressure
  details. Task 000277 and backlog 000187 remain historical/source context.
- No public configuration, durable format, recovery input, dependency,
  feature, or unsafe-code baseline changes are expected.
- Checkpoint may retain one small `Writing`/`Written` state per produced LWC
  block until final index construction. Encoded buffers awaiting shared IO
  remain CPU-window bounded; accepted write buffers are globally bounded by
  the existing shared IO subsystem.

## Test Cases

- Prove one list retains logical sequence and permits only
  `Encoding -> Writing -> Written` on the success path.
- Complete CPU jobs out of order and verify CoW block allocation, write
  ingress acceptance, and final index entries remain in logical RowID order.
- Verify CPU-stage occupancy never exceeds ThreadPool worker count and does not
  release capacity until shared IO accepts the corresponding write.
- Fill or block shared background-write ingress while concurrent storage work
  exists; verify the checkpoint stops advancing `next_to_write`, stops
  accepting builders at the CPU bound, and resumes from actual shared
  capacity without a local write limit.
- Delay an early encode while later encodes complete, and delay data-write
  completion while subsequent CPU work runs, proving CPU/write overlap through
  semantic events rather than sleeps.
- Submit writes whose physical completions arrive out of order; verify the
  final logical traversal drains all completions and produces ordered entries.
- Hold one accepted LWC write incomplete and prove no column-index write,
  mutable-root data update, meta/super-block write, fsync, or active-root swap
  occurs before it completes successfully.
- Inject a producer/page-access error with accepted encodes and writes; verify
  all accepted work drains, successful later encodes are dropped rather than
  written, and the primary error is preserved.
- Inject an inner encode error, an encode completion Fatal, block allocation
  exhaustion, readonly write-barrier failure, closed ingress submission, and
  backend IO completion failure at each meaningful state boundary.
- Combine an earlier Runtime producer/submission failure with a later accepted
  ThreadPool Fatal; verify Fatal outranks Runtime while both diagnostics remain
  attached and every completion is consumed.
- Verify write-completion failure drains subsequent accepted writes, skips
  `finish_lwc_blocks`, leaves the active root unchanged, and reaches the
  existing post-transition `CheckpointWrite` poison policy.
- Build a dense final visible block followed by fully deleted selected pages;
  verify the LWC header and column-index entry fingerprints match, persisted
  point/range reads succeed, and restart recovery accepts the block.
- Cover all-selected-pages-deleted behavior: no LWC block is written, while
  pivot and heap replay floor advance through checkpoint metadata as before.
- Compare worker counts and shared IO depths for identical LWC bytes,
  checksums, RowID shapes, pivot, heap replay floor, column-index routes, and
  secondary-index sidecar results.
- Preserve readonly-cache write-barrier invalidation for LWC and column-index
  CoW blocks, including reused physical block IDs.
- Preserve heartbeat, delayed, cancellation, transition, deletion checkpoint,
  secondary-index, root-reclamation, recovery, shutdown, and poison tests.
- Run `rtk cargo nextest run --workspace`.
- Run
  `rtk cargo nextest run -p doradb-storage --no-default-features --features libaio`
  because the change affects backend-neutral table-file IO orchestration.
- Run the mandatory branch-diff style audit, formatting, and strict Clippy
  validation.
- Run and record the specified fresh-root two-million-row benchmark matrix and
  `origin/main` comparison without turning performance variance into a CI gate.

## Open Questions

None for implementation. A checkpoint-specific IO quota or broader storage
QoS policy should be considered only if later concurrent-workload evidence
shows that shared scheduler admission is insufficient; that would be a
separate task rather than an implicit limit in this pipeline.
