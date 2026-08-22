---
id: 000278
title: Pipeline Checkpoint LWC Encoding and Data Writes
status: implemented
created: 2026-08-22
github_issue: 1003
---

# Task: Pipeline Checkpoint LWC Encoding and Data Writes

## Summary

User-table checkpoint previously completed all LWC CPU encoding before it
allocated and submitted any table-file data writes. That serialized two major
phases, retained every encoded 64 KiB buffer, and left a persistent-IO tail
after the parallel CPU work introduced by task 000277.

The shipped implementation replaces the encode queue, accumulated block
vector, and later write-future vector with one logical-order checkpoint
pipeline. Each block moves monotonically from `Encoding` to `Writing` to
`Written`. CPU encoding now overlaps shared-storage data writes while CoW
allocation, write acceptance, and final column-index entries remain ordered by
RowID. Shared storage ingress and backend IO depth provide the only write
backpressure boundary.

The final block shape is also bound to the known checkpoint pivot before CPU
encoding. This keeps the fingerprint embedded in the checksummed LWC header
identical to its column-index entry when fully deleted pages trail the last
visible row.

## Context

Task 000277 introduced the engine-owned fixed `ThreadPool` and made LWC
serialization its first consumer. Page access, visibility analysis, row
copying, and secondary-index sidecar collection stayed on the mandatory
runner, while complete owned builders were encoded on CPU workers in logical
order.

The remaining phase boundary accumulated all encoded blocks before
`MutableTableFile::apply_lwc_blocks` allocated CoW blocks and awaited a vector
of writes. Two-million-row profiles showed effective CPU scaling but a large
apply/write/publication tail. Early LWC writes were already safe because they
target unpublished CoW allocations; visibility still depends on the later
meta/super-block publication and fsync.

The table-write path already had the correct global pressure boundary:
bounded shared ingress, one storage scheduler, and backend IO depth shared by
all table, cache, and pool IO. A checkpoint-local write quota would duplicate
that control and ignore competing consumers.

Review also found a correctness edge in the old final-shape adjustment.
`set_end_row_id` recomputed the index fingerprint after the earlier fingerprint
had been embedded in the LWC buffer. Dense visible rows followed by fully
deleted selected pages could therefore produce mismatched durable metadata.

Issue Labels:

- type:task
- priority:medium
- codex

Source Backlogs:

- `docs/backlogs/closed/000187-pipeline-checkpoint-lwc-encoding-and-data-writes.md`

This task has no parent RFC. It remained a bounded checkpoint and internal
table-file IO orchestration change with no durable-format migration.

## Goals

- Overlap LWC CPU encoding with accepted table-file data writes.
- Represent every produced block once in a logical-order state list.
- Preserve logical allocation, submission, and index-entry order while
  allowing CPU and backend completion order to vary.
- Bound CPU-stage occupancy by the `ThreadPool` worker count and propagate
  actual shared-IO ingress pressure without a local write-depth estimate.
- Keep CPU tasks limited to owned serialization, compression, checksum, and
  fingerprint work.
- Transfer buffer, file, and readonly-cache write-lease ownership at shared
  ingress acceptance.
- Stop new production after an observed error, drain every accepted encode and
  write, and preserve Fatal-over-Runtime cleanup precedence.
- Build the column index and update mutable-root data metadata only after all
  LWC data writes succeed.
- Finalize the last LWC shape against the checkpoint pivot before encoding.
- Retain the existing atomic CoW publication boundary and add phase diagnostics.

## Non-Goals

- A checkpoint-local write semaphore, quota, configuration field, or inferred
  backend-depth window.
- A reusable staged-pipeline framework or another execution subsystem.
- Pipelining column-index construction, deletion checkpoint, secondary-index
  sidecars, allocation-map rebuilding, or root publication.
- IO, waiting, page loading, visibility analysis, or lock acquisition inside
  `ThreadPool` workers.
- Parallel or out-of-order CoW allocation and write submission.
- Cancellation of accepted CPU or IO work.
- Changes to public checkpoint APIs, durable bytes, recovery rules, MVCC
  semantics, configuration schema, or benchmark-result schema.
- Catalog, recovery, deletion-only, or other new `ThreadPool` consumers.

## Rejected Alternatives

### Checkpoint-local write depth

A local window derived from file IO depth would double-count a global backend
limit and could not account for concurrent reads or writes from other storage
consumers. Awaiting shared ingress acceptance gives the checkpoint direct
feedback from the actual contention point.

### Separate encode, write, and entry collections

Multiple collections would duplicate each block's identity and lifecycle. One
ordered state list retains the shape, completion, and final entry in the same
slot and directly supplies column-index input after successful drain.

### Generic CPU-to-IO pipeline

No second consumer justified generalized job ownership, error transport,
shutdown, or capacity policy. The coordinator remains checkpoint-private.

## Plan

### Ordered state and capacity model

`CheckpointLwcPipeline` owns the mutable table-file handle, `ThreadPool` guard,
table identity, and one `Vec<Option<LwcBlockState>>`. The list is authoritative
and ordered by RowID:

- slots before `next_to_write` are `Writing` or `Written` and have crossed
  shared ingress;
- slots at and after `next_to_write` are `Encoding`;
- `blocks.len() - next_to_write` is the exact CPU-stage occupancy;
- occupancy never exceeds the configured CPU worker count; and
- `None` is only a transient ownership device while one state crosses an await,
  never a semantic pipeline state.

At each safe page boundary, the mandatory runner advances consecutive ready
encodes without waiting for an unready head. When CPU capacity is full, it
waits for the logical head and submits that block through shared ingress before
admitting another builder. A ready encode does not release CPU capacity;
accepted write ownership does.

Later CPU jobs may complete first, but allocation and ingress acceptance wait
for `blocks[next_to_write]`. Backend writes may complete in any order because
their completions no longer borrow the mutable root.

### Write acceptance boundary

The direct-write path now separates submission from terminal waiting.
`submit_direct_write_with_lease` prepares the owned request, awaits bounded
shared ingress, and returns its completion. Existing composed write helpers
submit and then await, preserving behavior for unrelated callers.

`MutableTableFile::submit_lwc_block` validates logical order, allocates the CoW
block, attaches its ID to the immutable entry shape, starts the readonly-cache
write barrier, and transfers the buffer, file owner, and write lease to shared
storage. Only successful ingress acceptance advances the pipeline.

### Success and failure drain

After successful page production, the pipeline awaits and submits every
remaining encode before observing data-write completions. It then traverses
the accepted prefix in logical order, changing successful `Writing` states to
`Written`. This traversal does not serialize physical IO because every write
has already reached shared storage.

On an observed producer, encode, allocation, barrier, or submission failure,
new production stops. Remaining encode completions are consumed and successful
buffers are dropped rather than written. Every accepted write is still
observed. Cleanup errors merge through `RuntimeOrFatalError::merge_cleanup`,
so Fatal reasons outrank Runtime failures while earlier same-domain failures
remain primary.

Only an all-`Written` list becomes ordered `ColumnBlockEntryInput`. The
post-drain `finish_lwc_blocks` builds the `ColumnBlockIndex` and then updates
the mutable root's index pointer, pivot, and heap replay floor.

### Final shape and publication

LWC production receives `new_pivot_row_id`. The last nonempty builder uses that
exclusive end before its fingerprint is calculated and passed to
`LwcBuilder::build`. The mutable post-encoding `set_end_row_id` API was removed.
All-deleted selections still write no LWC and advance checkpoint metadata
through the existing metadata-only path.

Data writes remain unpublished until the normal meta-block write, ping-pong
super-block write, publication fsync, and active-root swap. Failed forks may
leave unreachable CoW blocks, matching existing failure behavior, but cannot
expose a partial LWC/index pair.

Internal debug diagnostics record block count, CPU capacity and peak occupancy,
production and write-acceptance/drain intervals, column-index rebuild time,
and exact root-publication fsync time tagged by file identity.

## Implementation Notes

The shipped pipeline overlaps CPU encoding and LWC data writes without adding
checkpoint-local IO policy, preserves the existing durability boundary, and
fixes the final-shape fingerprint mismatch.

Implementation review clarified that the readonly write lease belongs to the
accepted storage request, not the checkpoint coordinator. It invalidates a
possibly reused `(file_id, block_id)` cache mapping and keeps that key
write-blocked through backend completion. Brand-new table-file and catalog
mechanical paths continue to use the explicitly disabled barrier where no
relevant user-table cache-reuse hazard exists.

The final implementation deliberately differs from the source backlog's early
suggestion of a separately sized write window. Repository research showed the
shared bounded channel and backend scheduler already own global admission, so
the checkpoint retains only its CPU-stage worker bound.

Five alternating fresh-root pairs compared commit `dfbe66d` with its
pre-pipeline parent `d4ef404`. Each run inserted 2,100,000 deterministic
128-byte rows, froze exactly 2,000,320 rows across 4,465 pages, used four CPU
workers and file IO depth 64, and completed without checkpoint retry.

| Filesystem | Baseline median | Pipeline median | Median change | Baseline/pipeline sample CV |
| --- | ---: | ---: | ---: | ---: |
| persistent Btrfs | 567.4 ms | 478.1 ms | 15.7% faster | 18.43% / 7.07% |
| tmpfs control | 237.7 ms | 152.4 ms | 35.9% faster | 6.90% / 3.76% |

The pipeline won four of five persistent pairs; one unusually fast baseline
sample exposed the known Btrfs variance. It won all five tmpfs pairs by
32.0-39.2%. Every run submitted 4,476 backend operations, including 4,472
background writes and four table reads. On Btrfs, mean backend
submit-and-wait call count fell from 7,846 to 4,319 while preserving the same
operation count, consistent with greater batching and overlap.

Final verification completed with:

- mandatory branch-diff style audit: six Rust files passed against
  `origin/main`, including formatting and strict workspace Clippy;
- workspace tests: 1,766 passed across four binaries;
- alternate `libaio` storage tests: 1,683 passed;
- alternate-backend strict Clippy: passed; and
- focused coverage across the five changed implementation files: 92.78%
  deduplicated, with every file between 87.53% and 95.71%.

## Impacts

- User-table checkpoint now pipelines owned LWC CPU work into shared-storage
  data writes and retains only worker-bounded pre-ingress buffers.
- Internal direct-write APIs can return an accepted write completion while
  preserving the existing submit-and-wait helpers.
- Table-file LWC handling is split into ordered per-block submission and
  post-drain column-index finalization.
- Readonly-cache write barriers still cover reused user-table LWC and index
  blocks through backend completion.
- Column-entry shapes are immutable after construction; final pivot binding
  occurs before encoding.
- Checkpoint and CoW publication expose additional debug phase diagnostics.
- Architecture, checkpoint, data-checkpoint, and table-file documentation now
  describe shared-ingress backpressure and post-write index construction.
- No public API, configuration, dependency, schema, durable format, recovery
  input, feature, or unsafe-code baseline changed.

## Test Cases

- Out-of-order CPU completions preserve logical allocation, write acceptance,
  and final entry order.
- Out-of-order write completions drain into ordered `Written` entries.
- Inner encode failures drain later accepted CPU work without new writes.
- Fatal encode cleanup outranks an earlier Runtime producer failure while all
  completions are consumed.
- A failed accepted write drains later writes and prevents index finalization.
- CPU occupancy remains bounded until shared ingress accepts the logical head.
- A dense final visible page followed by a fully deleted page produces matching
  LWC and column-index fingerprints.
- An all-deleted selected page advances pivot/replay metadata without creating
  a column-index root.
- LWC and column-index writes preserve readonly-cache invalidation when physical
  block IDs are reused.
- Existing checkpoint transition, cancellation, deletion, sidecar, recovery,
  shutdown, poison, and root-reclamation suites pass on both IO backends.

## Open Questions

None. A checkpoint-specific IO quota or broader storage QoS policy should be
considered only if future concurrent-workload evidence shows shared scheduler
admission is insufficient; that would require a separate task.
