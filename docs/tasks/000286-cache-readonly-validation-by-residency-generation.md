---
id: 000286
title: Cache readonly validation by residency generation
status: proposal
created: 2026-08-28
github_issue: 1023
---

# Task: Cache readonly validation by residency generation

## Summary

Avoid revalidating every immutable persisted block on every readonly-cache hit.
Change the global readonly mapping from `BlockKey -> PageID` to
`BlockKey -> VersionedPageID`, validate persisted data before miss publication
as today, and treat a matching block key plus frame generation as the exact
immutable residency already admitted by that page role's validator.

This removes the repeated 64 KiB BLAKE3 checksum and structural validation from
warm LWC, column-block-index, deletion-blob, and DiskTree hits while preserving
validation on initial load, eviction/reload, frame reuse, and CoW replacement.
Keep production raw `read_raw_block` use isolated by caller contract to
transient Cow super/meta root loading and remove those raw mappings after
caller-side parsing.

## Context

Issue Labels:

- type:perf
- priority:high
- codex

Source Backlogs:

- `docs/backlogs/000188-optimize-warm-cache-cold-row-table-scans.md`

Task `docs/tasks/000285-parallel-scan-benchmark-performance-proof.md` exposed a
warm-cache cold-row scan bottleneck. Its one-million-row cold-dominant fixture
contained 2,009 LWC blocks and 224 hot row pages. A measured scan performed
4,021 readonly-cache hits with zero misses, completed reads, or backend
submissions, yet sequential cold-dominant latency was about 263 ms versus
74 ms for the hot fixture. Follow-up CPU-clock profiles attributed 50.4% of
sequential cold samples and 42.2% of parallel cold samples to BLAKE3. One warm
scan checksummed about 251.3 MiB of already resident bytes.

The duplicate work comes from `doradb-storage/src/buffer/readonly.rs`.
`ReadSubmission::complete` already runs the requested validator while a miss
reservation remains exclusive and before the mapping is published. After the
load returns, `read_shared_block` acquires the resident guard and invokes the
same validator again. It repeats that post-guard validation for every later
cache hit.

`BufferFrame` already carries a runtime reuse generation. Publication,
invalidation, eviction, rollback, and reuse advance that generation while the
generation stays constant for the lifetime of one immutable residency.
`doradb-storage/src/buffer/page.rs` already defines
`VersionedPageID { page_id, generation }`; the readonly mapping currently
stores only the frame `PageID` and validates staleness through block-key
metadata.

Production raw reads do not overlap persisted data-page readers:

1. `CowFile::load_active_root_from_pool` is the only production caller of
   `read_raw_block`.
2. It reads only reserved super block zero and the active table/catalog meta
   block, then validates those bytes immediately through the Cow codec.
3. Current arbitrary-block callers are tests. The crate-internal raw entrypoint
   relies on its documented caller contract rather than a runtime block-kind or
   compile-time production/test distinction.
4. LWC, column-index, deletion-blob, and DiskTree blocks use
   `read_validated_block`; one allocated block has one page role during a
   well-formed residency generation.
5. A CoW replacement starts the readonly write barrier, removes the old
   mapping, and blocks same-key misses until the replacement write completes.

The revised cache contract relies on that single-page-role invariant rather
than storing a validator or page-kind tag in every mapping. It continues to
detect persisted corruption when a block enters or re-enters residency. It
does not continuously scrub immutable RAM, and it does not re-prove a
different requested page kind for an already resident generation. Those are
explicit semantic boundaries of this task.

This task is a standalone performance follow-up sourced from backlog 000188.
It is related to RFC-0030's completed benchmark phase and RFC-0007's persisted
page-integrity contract, but it is not a new phase of either RFC and requires
no RFC phase-plan edit.

## Goals

1. Store the exact readonly frame generation in every resident `BlockKey`
   mapping and reject stale mapping snapshots through both key and generation
   checks.
2. Run a persisted data-page validator once before a validated miss becomes
   resident, then skip validation for matching warm-cache generations.
3. Preserve validation for every new residency created by initial load,
   eviction/reload, explicit invalidation, frame reuse, or CoW replacement.
4. Keep raw Cow super/meta reads disjoint from validated data mappings and
   remove their transient raw mappings after caller-side parsing on success or
   failure.
5. Reject a raw-versus-validated overlap for the same inflight key as an
   internal protocol error instead of joining an incompatible load.
6. Ensure a stale reader can remove only the exact old `VersionedPageID`, never
   a newer generation published at the same frame id or block key.
7. Demonstrate a material reduction in warm cold-dominant sequential and
   parallel scan latency without a material hot-scan regression.
8. Preserve cold/hot MVCC scan results, persisted delete handling, sparse row
   identities, external deletion blobs, and both supported storage backends.

## Non-Goals

1. No scan-worklist capture of decoded row/delete metadata, column-index
   leaf-local batching, or removal of the per-entry index-leaf reopen in this
   task. Reprofile those costs after checksum removal.
2. No LWC projected-column cache, sequential decoder, vectorized execution, or
   query-engine integration.
3. No optimization of per-row `ColumnDeletionBuffer` visibility checks; that
   remains tracked by backlog 000111.
4. No lock-free readonly access or change to shared-guard lifetime and eviction
   pinning.
5. No durable block generation, cache-key format change, persisted-file format
   change, migration, or compatibility layer.
6. No public storage API change and no new public diagnostics or benchmark
   result fields.
7. No continuous resident-memory checksum scrubbing and no promise to detect
   arbitrary mutation of already admitted immutable frame bytes on every hit.
8. No cache-level revalidation when an invalid caller or corrupt reference asks
   to reinterpret an already resident generation through another validator;
   one page role per generation remains an invariant.

## Rejected Alternatives

1. **Store `validated_as` or a validator identity in each mapping.** A format
   tag would mechanically detect a wrong-kind request for an already resident
   generation, but production raw reads are confined to transient Cow
   super/meta blocks and validated data blocks have one role per generation.
   The chosen design records exact residency identity and makes the page-role
   invariant explicit without adding permanent mapping provenance. Raw root
   mappings are removed after parsing, and raw/validated inflight overlap is
   rejected.
2. **Switch cold scans to raw `read_block`.** This would remove checksum cost by
   bypassing validation rather than proving prior admission. It would violate
   RFC-0007's validation-before-residency contract and could process corrupted
   bytes on the first load.
3. **Bundle column-index scan metadata capture into this task.** Worklist
   capture could decode compact row/delete metadata while a leaf is already
   guarded and eliminate execution-time leaf reopens. That is a separate
   ownership and plan-memory change across the index and scan layers. Removing
   the dominant checksum cost first gives clean attribution and new evidence
   for whether that broader change remains worthwhile.
4. **Adopt a typed persisted-page cache and compiled/vectorized cold scan in one
   program.** This is an RFC-scale destination spanning buffer admission,
   index metadata ownership, public shared-snapshot plans, and LWC decoding.
   It fails the narrow task complexity gate and is unnecessary for the measured
   first bottleneck.

## Plan

### Versioned readonly mappings

1. In `doradb-storage/src/buffer/readonly.rs`, change
   `ReadonlyBufferPool::mappings` from `FastDashMap<BlockKey, PageID>` to
   `FastDashMap<BlockKey, VersionedPageID>` using the existing type from
   `doradb-storage/src/buffer/page.rs`.
2. During `ReadonlyPageReservation::publish`, set the persisted block key,
   advance the frame generation, install the resident frame kind, and publish
   the mapping with that post-bump generation. Keep mapping publication after
   successful miss-time validation.
3. Replace frame-id-only lookup helpers with an internal resident-identity
   lookup. Test/diagnostic helpers that need only a frame id may project
   `VersionedPageID::page_id` without weakening production comparisons.
4. Update `validate_guarded_frame_key` into an exact residency check that
   requires a non-uninitialized frame, the expected persisted block key, and
   the mapped generation.
5. Replace `invalidate_stale_mapping_if_same_frame` with conditional removal
   that compares the complete `VersionedPageID`. If frame acquisition observes
   another key or generation, remove only the copied stale mapping and retry.
6. Adapt explicit invalidation, rollback, drop-only eviction, write-barrier
   invalidation, mapping assertions, and diagnostics to extract the frame id
   while retaining full-generation comparisons at race-sensitive boundaries.

### Once-per-residency validated reads

1. Preserve the current validated miss sequence: read into the exclusively
   owned reservation, run the supplied `ReadonlyBlockValidator`, publish only
   on success, and roll back without a mapping on failure.
2. Make validated get/load helpers resolve the current `VersionedPageID` after
   publication or inflight completion. If eviction or invalidation removed the
   mapping before guard acquisition, retry through the normal miss path.
3. After acquiring the shared frame guard, validate the exact block key and
   generation and return `ReadonlyBlockGuard` directly. Remove the unconditional
   validator invocation from `read_shared_block`; this removes both the second
   validation after a successful miss and all repeated warm-hit validation.
4. Keep the validator function API and existing LWC, column-index,
   deletion-blob, and DiskTree call sites otherwise unchanged. Their validator
   selects admission behavior on a miss; no validator-tag migration is needed.
5. Document the proof: a validated mapping is published after validation, one
   immutable generation has one persisted page role, and any byte replacement
   must first destroy the mapping through invalidation or the CoW write barrier.

### Raw access and Cow root lifecycle

1. Expose one crate-internal raw entrypoint named `read_raw_block`. Document
   that it skips admission validation, persisted data pages must use
   `read_validated_block`, and Cow root callers must validate immediately and
   remove their transient mappings after parsing.
2. Add `ReadonlyLoadClass::{Raw, Validated}` and retain it in
   `InflightBlockState::Loading`. Reject a same-key join whose class differs
   with fieldless `InternalError::ReadonlyLoadClassConflict` plus file/block
   context. Validated callers for a well-formed page role may continue joining
   the same validated miss.
3. In `CowFile::load_active_root_from_pool`, retain pre-read invalidation for
   super and meta blocks. After parsing each guarded block, drop the guard and
   invalidate its transient raw mapping before applying `?`, so both success
   and parse/validation failure paths leave no raw mapping behind.
4. Keep super block zero permanently reserved. Keep meta blocks excluded from
   normal data references by the existing CoW allocation/root invariants, and
   retain write-barrier invalidation before any reclaimed physical block is
   rewritten for another role.

### Documentation and performance verification

1. Update `docs/buffer-pool.md` to describe versioned mappings,
   validation-before-publication, warm-hit validation elision, transient raw
   Cow-root reads, and the one-page-role-per-generation contract.
2. Record deterministic focused-test evidence that a validated miss runs its
   validator exactly once and matching warm hits run it zero additional times.
3. Reproduce the task-000285 hot and cold-dominant warm-cache benchmark shapes
   before and after the implementation on the same host, build profile,
   backend, and topology. Use fresh roots and projection `[0, 1]`.
4. Capture a CPU profile or equivalent deterministic evidence confirming that
   steady warm readonly hits no longer invoke BLAKE3 validation.
5. Keep timing thresholds manual rather than adding environment-sensitive CI
   performance gates.

## Implementation Notes

## Impacts

- `doradb-storage/src/buffer/readonly.rs`
  - versioned mapping values;
  - exact generation lookup/removal;
  - once-per-residency validated read behavior;
  - raw/validated inflight classification;
  - invalidation, eviction, and write-barrier adaptations;
  - focused lifecycle, corruption, and race tests.
- `doradb-storage/src/buffer/page.rs`
  - reuse of the existing `VersionedPageID`; no new durable or frame-layout
    state is expected.
- `doradb-storage/src/file/cow_file.rs`
  - raw-read caller contract and transient mapping cleanup after parsing.
- `doradb-storage/src/error.rs`
  - fieldless `InternalError::ReadonlyLoadClassConflict` when raw and validated
    callers attempt to join the same inflight key.
- `doradb-storage/src/lwc/block.rs`
- `doradb-storage/src/index/column_block_index.rs`
- `doradb-storage/src/index/column_deletion_blob.rs`
- `doradb-storage/src/index/disk_tree.rs`
  - existing validators remain the miss-admission authorities; functional
    call-site changes should be unnecessary beyond any signature cleanup.
- `docs/buffer-pool.md`
  - revised validation and raw-root isolation contract.
- Sequential transaction table scans and shared-snapshot partition scans gain
  the performance improvement through their existing persisted block loaders;
  their public interfaces, plans, and row semantics do not change.

## Test Cases

1. A validated cache miss runs its supplied validator exactly once, publishes
   the post-bump frame generation, and returns a guard for that generation.
2. Repeated validated warm hits for the same mapping execute zero additional
   validator calls while preserving cache-hit counters and returned bytes.
3. A test-only mutation of already admitted resident bytes demonstrates the
   explicit non-scrubbing contract: a matching generation does not rerun the
   validator, while explicit invalidation followed by reload requires fresh
   validation.
4. A checksum, magic, version, or structural failure during a validated miss
   rolls back the reservation and leaves no mapping or resident frame.
5. A mapping copied before different-key frame reuse fails its key/generation
   check, cannot remove the newer mapping, and retries safely.
6. The same `BlockKey` reloaded into the same frame slot after invalidation or
   eviction has a different generation; a stale snapshot cannot identify the
   new residency.
7. Drop-only eviction removes the exact mapping, advances the generation, and
   forces a later validated read to perform fresh validation.
8. CoW write-barrier replacement removes the old mapping, blocks same-key
   misses while the write is active, and forces the replacement bytes through
   fresh validation after completion.
9. Cow super/meta loading leaves no raw mapping after successful parsing, bad
   checksum, bad magic/version, invalid meta payload, or invalid root
   invariants.
10. Concurrent raw and validated requests for the same inflight key
   return the typed internal load-class conflict rather than sharing a
   completion or publishing ambiguous residency.
11. Existing same-class miss deduplication still issues one backend read and
    returns the same versioned residency to all successful waiters.
12. Cancellation, backend failure, short read, shutdown, and reservation
    rollback leave no stale versioned mapping or inflight entry.
13. Existing cold/hot MVCC scan suites preserve cardinality and values for
    normal rows, cold deletes and updates, long-running snapshots, persisted
    delete deltas, sparse row-id sets, and external deletion blobs.
14. Sequential and parallel scans remain equivalent, including partition
    planning, early drop, failure cleanup, and exact returned-row equations.
15. On the task-000285 one-million-row fixture, compare medians against a
    same-host pre-change baseline:
    - cold-dominant sequential elapsed time improves by at least 20%;
    - target-capacity parallel cold-dominant elapsed time improves by at least
      10%;
    - corresponding hot medians regress by no more than 5%.
16. Profiling shows no steady warm-hit BLAKE3 validation from the readonly read
    path after warm-up.
17. Run `rtk cargo nextest run --workspace`.
18. Run
    `rtk cargo nextest run -p doradb-storage --no-default-features --features libaio`.
19. Run formatting, workspace strict Clippy, and the branch Rust style audit
    required by repository process.

## Open Questions

No implementation-blocking question remains.

Backlog 000188 should remain open after this task until post-change profiling
determines whether repeated column-index leaf reopening and prefix-plane
parsing still justify a separate scan-metadata task. Backlog 000111 remains the
separate authority for per-row cold deletion-buffer visibility filtering.
