---
id: 000286
title: Cache readonly validation by residency generation
status: implemented
created: 2026-08-28
github_issue: 1023
---

# Task: Cache readonly validation by residency generation

## Summary

The readonly buffer pool previously revalidated every immutable persisted block
on every cache hit, including a full 64 KiB checksum for LWC, column-index,
deletion-blob, and DiskTree pages. This task changed readonly mappings from
`BlockKey -> PageID` to `BlockKey -> VersionedPageID` and made successful
miss-time validation authoritative for one immutable residency generation.

Validated warm hits now require an exact physical key and frame-generation
match but do not repeat checksum or structural validation. Initial load,
invalidation, eviction/reload, frame reuse, and CoW replacement create new
residencies and therefore require fresh validation. Raw reads remain a
documented crate-internal contract for immediately validated Cow super/meta
loading.

## Context

Issue Labels:

- type:perf
- priority:high
- codex

Source Backlogs:

- `docs/backlogs/000188-optimize-warm-cache-cold-row-table-scans.md`

Task `docs/tasks/000285-parallel-scan-benchmark-performance-proof.md` exposed
the bottleneck with a one-million-row warm-cache fixture. The cold-dominant
shape performed 4,021 readonly-cache hits with zero misses or backend reads,
yet took about 263 ms sequentially versus 74 ms for the hot fixture. CPU-clock
profiles attributed roughly half of cold sequential CPU samples to repeated
BLAKE3 validation; one scan checksummed about 251 MiB of resident bytes.

The load path already validated a requested page while its miss reservation
was exclusive and before publishing the mapping. The redundant work came after
publication, where `read_shared_block` invoked the validator again for the
initial return and every subsequent warm hit.

`BufferFrame` already provided a runtime reuse generation that advances across
publication, invalidation, eviction, rollback, and reuse. That generation is
not durable metadata. Pairing it with the physical `BlockKey` identifies one
specific in-memory residency without changing persisted formats or cache keys.

This task is a standalone follow-up related to RFC-0030's completed benchmark
phase and RFC-0007's persisted-page integrity contract. It has no parent RFC
phase and does not change either RFC's phase plan.

## Goals

1. Identify every readonly mapping by exact physical key and frame generation.
2. Validate persisted data before miss publication and skip validation only for
   the matching admitted residency.
3. Require fresh validation after invalidation, eviction/reload, frame reuse,
   or CoW replacement.
4. Prevent stale readers or evictors from removing a newer residency mapping.
5. Keep raw and validated inflight loads distinct and reject incompatible
   same-key joins.
6. Remove transient Cow super/meta raw mappings after parsing on success and
   failure.
7. Materially reduce cold-dominant scan latency without regressing hot scans
   beyond the task's five-percent tolerance.
8. Preserve scan, MVCC, deletion, sparse-row, and storage-backend correctness.

## Non-Goals

1. No durable block generation, persisted-format migration, or physical
   cache-key change.
2. No validator identity or page-kind tag stored in readonly mappings.
3. No continuous checksum scrubbing of already admitted immutable memory.
4. No lock-free readonly access or shared-guard lifetime change.
5. No column-index worklist metadata capture, leaf-local batching, or LWC
   projected-column decoder optimization.
6. No per-row `ColumnDeletionBuffer` visibility optimization; that remains
   tracked separately by backlog 000111.
7. No public storage API, benchmark result schema, or operational rollout
   change.

## Rejected Alternatives

1. **Store validator identity in every mapping.** Production data pages have one
   well-formed page role per immutable residency. Exact key/generation identity,
   transient raw-root cleanup, and inflight load-class rejection provide the
   required contract without permanent mapping provenance.
2. **Switch cold scans to raw reads.** This would bypass validation rather than
   prove prior admission and would violate persisted-page integrity on the
   first load.
3. **Combine scan metadata and LWC decoding optimization with this task.** Those
   changes cross index, plan-memory, and execution ownership boundaries. They
   were deferred until checksum removal provided a clean post-change profile.
4. **Adopt a typed persisted-page cache and compiled cold scan together.** That
   is an RFC-scale redesign unnecessary for the measured primary bottleneck.

## Plan

### Versioned readonly residency

The global readonly mapping stores `VersionedPageID { page_id, generation }`.
Successful publication validates the page while the reservation remains
exclusive, advances the frame generation, installs the resident frame state,
and publishes the post-bump generation.

Every guarded lookup accepts a mapping only when the frame is initialized,
still carries the expected persisted block key, and has the mapped generation.
A stale reader conditionally removes only the exact copied `VersionedPageID`
and retries. Explicit invalidation and the drop-only evictor likewise compare
the full residency before reclaiming a frame or removing a mapping.

### Once-per-residency validation

Validated misses run the supplied `ReadonlyBlockValidator` before publication.
Validation failure rolls back the reservation and leaves no mapping. A matching
warm residency is returned without invoking the validator again; eviction,
invalidation, replacement, or reuse destroys that proof and forces a new
validated miss.

This contract assumes one persisted page role per immutable residency. It
detects corruption when bytes enter residency but does not continuously scrub
resident memory or re-prove a different requested role for an already admitted
generation.

### Raw reads and CoW root lifecycle

`read_raw_block` is the single crate-internal unvalidated entrypoint. Its
documentation requires persisted data pages to use `read_validated_block` and
requires Cow root callers to validate returned super/meta bytes immediately.
The API relies on that caller contract rather than runtime block-kind checks or
test-only compilation.

Inflight loads retain `ReadonlyLoadClass::{Raw, Validated}`. Same-class misses
may deduplicate; a raw/validated same-key overlap returns
`InternalError::ReadonlyLoadClassConflict` instead of sharing an incompatible
completion.

`CowFile::load_active_root_from_pool` invalidates super and selected meta keys
before reading, parses each raw block, drops its guard, and invalidates the
mapping before propagating success or validation failure. CoW writes retain the
same-key write barrier so replacement bytes cannot race an old raw or validated
residency.

## Implementation Notes

Task 000286 shipped the versioned readonly-residency proof and removed repeated
warm-hit validation without weakening miss-time persisted-page integrity.

- Mapping publication, guarded lookup, stale cleanup, explicit invalidation,
  and eviction now compare exact generations. Inflight completion still
  transports a frame id, but consumers re-resolve the current versioned mapping
  before acquiring a guard.
- Raw and validated inflight requests are classified separately. Cow root
  parsing removes transient raw mappings for valid roots and all tested parse,
  checksum, magic, version, payload, and root-invariant failures.
- The proposed separate `read_cow_root_block` and test-only raw helper were
  consolidated into `read_raw_block`: naming alone did not enforce correct
  use, so one documented crate-internal contract is clearer.
- Review removed two production-compiled legacy test accessors:
  `ReadonlyBufferPool::try_get_block_key` and
  `ReadonlyBlockGuard::block_id`. Deliberate `#[cfg(test)]` residency inspection
  remains available for lifecycle assertions.
- No public API or persisted representation changed. Existing page-role
  validators remain the admission authorities.

The same-host release benchmark results were:

| Shape | Task 000285 baseline | Task 000286 | Change |
|---|---:|---:|---:|
| Hot sequential | 73.934 ms | 72.416 ms | 2.1% faster |
| Hot target-nine | 20.683 ms | 21.655 ms | 4.7% slower |
| Cold sequential | 263.146 ms | 130.380 ms | 50.5% faster |
| Cold target-nine | 57.083 ms | 36.506 ms | 36.0% faster |

A separate 20-run release CPU profile measured 71.43 ms hot and 128.55 ms
cold sequential medians. BLAKE3 no longer contributed materially to the warm
hit path. Roughly 67% of the remaining gap was column-index metadata work and
14% was repeated LWC decode/layout work; readonly lookup and latching were less
than 1%. That evidence is retained in the follow-up backlog.

Final verification completed:

- 1,825 workspace tests passed with the default backend.
- 1,734 `doradb-storage` tests passed with the alternate `libaio` backend.
- Focused readonly residency, corruption, race, raw-load conflict, eviction,
  write-barrier, and Cow root-cleanup tests passed.
- The branch-diff style gate passed all seven changed Rust files, including
  formatting, strict Clippy, and repository style checks.

## Impacts

- The readonly buffer pool stores runtime versioned residencies and performs
  validation once per admitted generation.
- Cow file root loading uses the documented raw-read contract and leaves no
  raw root mapping after parsing.
- Storage errors gained the internal raw/validated load-class conflict
  classification.
- Buffer-pool documentation now records the residency proof, corruption
  boundary, and raw-root lifecycle.
- Sequential and parallel scans gain the improvement through existing block
  loaders; query APIs, result semantics, persisted formats, and deployment
  behavior are unchanged.

## Test Cases

1. A validated miss invokes its validator once; matching warm hits invoke it
   zero additional times and return the same bytes.
2. Corrupted LWC, column-index, or deletion-blob pages fail before publication
   and leave no resident mapping.
3. Explicit invalidation or eviction followed by reload creates a new
   generation and performs fresh validation.
4. A stale key/generation snapshot cannot remove a newer mapping after
   same-frame or different-key reuse.
5. Drop-only eviction removes only its exact mapped residency.
6. CoW replacement invalidates old bytes, blocks same-key reads during the
   write, and reloads replacement bytes through a new residency.
7. Raw/validated same-key inflight overlap returns the typed conflict while
   same-class concurrent misses still deduplicate to one backend read.
8. Cancellation, I/O failure, validation failure, shutdown, and reservation
   rollback leave no stale mapping or inflight state.
9. Cow super/meta success and every covered validation failure remove their
   transient raw mappings.
10. Cold/hot MVCC, deletion overlays, sparse row-id sets, external deletion
    blobs, and sequential/parallel result equivalence remain correct.
11. Default and `libaio` workspace validation, formatting, strict Clippy, and
    the branch-diff style audit pass.

## Open Questions

The checksum bottleneck is resolved. Backlog 000188 retains the actionable
remaining work: eliminate repeated column-index leaf reopening and prefix-plane
validation, then reprofile before deciding whether projected LWC views or a
sequential decoder are worthwhile. Backlog 000111 remains the separate
authority for per-row cold deletion-buffer visibility filtering.
