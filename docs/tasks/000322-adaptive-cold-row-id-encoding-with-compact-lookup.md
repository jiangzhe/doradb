---
id: 000322
title: Adaptive cold RowID encoding with compact lookup
status: implemented
created: 2026-09-30
github_issue: 1129
---

# Task: Adaptive cold RowID encoding with compact lookup

## Summary

Cold-row identity now uses adaptive whole-entry or segmented encoding inside
`ColumnBlockIndex`. Point reads and scans access validated compact metadata
directly, avoiding full sparse-row expansion and repeated leaf validation.
Checkpoint selects an exact-size representation once and returns a typed
capacity error when an individual entry cannot fit.

The approved binding follow-up replaced per-RowID fingerprints with a
fixed-input `block_binding_value: u64`. Index entries and LWC blocks retain
matching bindings while physical membership, deletion state, and root
publication keep their existing ownership contracts.

## Context

Source Backlogs:

- docs/backlogs/closed/000201-adaptive-cold-row-identity-encoding.md

Issue Labels:

- type:task
- priority:medium
- codex

This standalone task has no parent RFC phase. Implemented RFCs
[0011](../rfcs/0011-redesign-column-block-index-program.md) and
[0012](../rfcs/0012-remove-row-id-from-lwc-page.md) supplied the existing index,
row-identity, and values-only LWC ownership boundaries.

The baseline was `1bee69a8ff69ba2ad1e38ef6e985cefd8f63d08c`. Its dense-or-u32-list
representation became expensive when checkpoint combined row pages with holes
or unused tails. Sparse point reads and scan preparation also expanded row
identity, so reducing persisted bytes alone would not remove runtime costs.

Research covered uniform, variable-width, and highly compressible values with
0%, 1%, and 10% pre-checkpoint deletion. Whole-entry and segmented encodings won
on different distributions, motivating exact complete costs and a whole-entry
fallback rather than mandatory segmentation.

## Goals

- Reduce persisted identity size and runtime materialization while preserving
  exact membership, physical row order, and both ordinal translations.
- Select deterministic encodings from complete serialized costs and access
  bounds, including directory and rank metadata.
- Validate immutable persisted metadata at cache admission and reuse it for
  direct point lookup and ordered scans.
- Share identity across checkpoint, catalog, recovery, scan descriptors, and
  delete-only rewrites without changing transaction or publication semantics.
- Make unsupported inline capacity a typed outcome, and replace row-by-row
  binding hashes with a fixed-size deterministic binding.

## Non-Goals

- Automatic identity-aware LWC splitting, external identity storage, or a
  successful checkpoint for every arbitrarily sparse input.
- New deletion codecs, deletion-blob retirement, or changes to live MVCC
  deletion markers and visibility masks.
- Globally optimal segmentation, workload-specific tuning controls, or
  changing the full-block checksum algorithm.
- Migration readers for old index/LWC envelopes, oversized source-page
  handling, or redesign of catalog root enumeration.

## Rejected Alternatives

- Mandatory segmentation loses on compact whole ranges and some deletion
  distributions once directory overhead is included.
- Compressing only persisted bytes leaves row-sized allocations and repeated
  validation in the point-read and scan paths.
- Retaining canonical per-row fingerprints adds checkpoint CPU without an
  independent membership check in values-only LWC blocks. The approved
  replacement binds immutable link metadata instead.

## Plan

### Encoding and selection

The row-set module supports dense ranges, narrow and wide present-offset
lists, runs, bitmaps, missing-offset lists, trimmed variants, and segmented
combinations. Existing dense and u32-list row-codec layouts remain available
inside the new envelopes.

Builders retain source RowIDs while collecting transient page or logical-window
statistics. Planning compares exact whole-entry costs with local segment
costs, greedily merges adjacent segments when total size does not increase,
and retains the whole-entry result whenever it is preferable. Stable tie
breaks make identical inputs and seeds produce identical bytes. The greedy
policy does not promise a globally optimal partition.

Only the selected representation is encoded. Snapshot rollback restores seeds
with values and RowIDs. Source page identities are absent from the durable
format, and segment boundaries affect compression without changing membership.

### Compact access and ownership

Validated borrowed views provide membership, rank, inverse translation, and
sequential iteration. Scans and rewrites share immutable encoded bodies; dense
identity requires no body allocation. The separate `ScanRowIdentity` wrapper
was removed in favor of the common encoded representation.

Full leaf and row-codec validation occurs before a readonly frame is admitted.
Warm reads reuse that immutable generation; invalidation requires validation
again. Owned catalog and recovery exports materialize RowIDs only at their
existing consumer boundaries. Durable deletion sets remain separate from
physical membership and never renumber LWC ordinals.

### Binding and publication

Checkpoint finalizes coverage, including absent trailing positions up to the
pivot, before freezing identity and binding metadata. Table and catalog
producers use the same planner and pass the owning logical table ID. The
existing ordered encode/write pipeline publishes the matching index root only
after accepted work completes.

The binding hashes a 36-byte stack payload with BLAKE3: the eight-byte
`LWCBIND1` prefix, table ID, inclusive start RowID, exclusive end RowID, and row
count. IDs use u64 little-endian fields and count uses u32 little-endian; the
first eight digest bytes become a little-endian u64. No per-row input,
randomness, timestamp, or physical block ID participates.

Equal table/bounds/count summaries intentionally share a binding even if
interior membership or values differ. This is a metadata association check;
full-block checksums protect persisted contents. Delete-only rewrites preserve
the identity bytes and binding.

### Capacity and compatibility

New identities reserve room for the existing deletion representation. The
final layout allows a 65,184-byte identity body with a 240-byte deletion
reserve. Oversized valid input returns `ColumnBlockEntryCapacityExceeded`;
malformed persisted data remains an integrity error.

The leaf writer checks actual capacity on rewrites, including legacy row
codecs that lack the new reserve. An oversized single entry fails before its
node allocation. Accepted pipeline work is drained on failure without partial
root publication. Failure after checkpoint TRANSITION retains the existing
fatal policy; automatic splitting and recoverable rejection remain deferred.

Both index-entry and LWC headers shrink from 32 to 24 bytes. Column-index
format version 3 and LWC format version 2 deliberately reject previous
fingerprint-format envelopes. Fresh storage is required; no migration reader
was added.

## Implementation Notes

Implemented adaptive compact cold-row identity across checkpoint, point reads,
scans, catalog/recovery consumers, and persisted delete rewrites, with typed
capacity handling and the approved deterministic u64 block binding.

The binding follow-up materially changed the original canonical-fingerprint
plan. It removes per-row hash work while preserving existing mismatch checks.
Full-block checksums and deletion formats remain unchanged. Targeted inline
hints were added to compact access and small planning helpers; large planning,
encoding, and validation routines remain separate. No new dependency or public
configuration surface was introduced.

Five controlled baseline/candidate pairs for each of the nine 100,000-row
fixtures passed the per-fixture review gates: at most 10% median warm-point or
scan regression and 20% checkpoint elapsed-time regression. All adaptive
indexes fit one leaf, with total row sections ranging from 904 to 17,750 bytes.
The compressible 1%-deletion case used 5,882 bytes versus 7,492 for whole-entry
encoding. Captured size estimates matched serialization. Later committed
deletes, external deletion payloads, and reopen were also verified.

Public `doradb-bench` random point selects used one million checkpointed rows
and warmed buffers, with five fresh baseline/candidate pairs per reader count.
Median cold-row throughput increased 12.76x with one reader and 8.03x with four;
all 12 million measured selects found their rows. Here cold refers to persisted
placement, not an empty readonly cache.

Matching hot-row controls omitted freezing and checkpointing. Adaptive cold
throughput reached 81.3% and 83.1% of hot throughput with one and four readers.
Hot throughput changed -3.5% and +6.2% relative to baseline; all 12 million hot
selects succeeded without readonly-buffer accesses. These point-read results
preceded the binding follow-up and are not a separate measurement of it.

Initial one-million-row checkpoint profiles covered insert-only and 1% random
deletion before checkpoint. Planning/encoding/seed statistics used about
0.9%/1.2% of sampled user CPU, while canonical fingerprinting used 0.2%/9.9%.
Full LWC checksums consumed roughly 50-57%, motivating backlog 000207.

The binding comparison used 32 independent before/after runs: five unprofiled
runs and three profiles per variant and deletion scenario. Every survivor was
verified. With 1% deletion, sampled fingerprint/binding CPU fell from 8.54% to
0.04%. Aggregate process-CPU medians were 260.6 to 241.0 ms, but the median
paired change was only -0.8%; elapsed times did not establish a speedup.
Insert-only CPU medians were 222.6 to 229.5 ms. Both variants wrote 2,233 LWC
blocks. The residual binding sample count is too small for precise costing.

Ad hoc benchmark tools and detailed reports were removed from the source tree
at the user's request. These summaries retain the observed outcomes and their
limits without presenting the removed artifacts as reproducibility resources.

Validation completed with 2,208 workspace tests and 2,029 storage tests without
profiling, plus strict Clippy in both configurations. The final style gate
passed 18 branch-diff Rust files and 502 test contracts. Assertion and overlap
review retained distinct codec, cache-generation, deletion-domain, and pipeline
lifecycle coverage. The capacity-rejection test now compares the complete
allocation map before and after failure, and its focused rerun passed.

Source backlog 000201 is closed for the delivered encoding work. Its remaining
joint-capacity and identity-aware splitting scope is carried into backlog
000206; checksum algorithm evaluation remains in backlog 000207. No parent RFC
phase requires synchronization.

## Impacts

- Cold identity metadata and scan descriptors become smaller, and warm point
  lookup avoids row-sized expansion and repeated full validation.
- Persisted index/LWC formats change and require fresh storage; public table
  APIs, transaction semantics, and deletion publication behavior stay stable.
- Each index-entry and LWC header saves eight bytes. This reduces metadata
  usage without guaranteeing fewer allocated blocks for a particular workload.
- Capacity failure is explicit before publication, but a post-TRANSITION failure
  remains fatal under the existing checkpoint recovery contract.
- The storage overview documents describe ownership and behavior conceptually;
  executable format details and validation remain in the code and tests.

## Test Cases

- All whole and local codecs agree with independent sorted-row oracles for
  membership, both ordinal directions, gaps, boundaries, and ordered iteration.
  Exhaustive small missing-set cases and deterministic generated distributions
  cover inverse mapping and reproducible planning.
- Estimator/serializer agreement, whole-entry fallback, mixed local segments,
  builder rollback, and wide-gap inputs exercise selection and encoding.
- Malformed tags, lengths, ordering, prefixes, directories, reserved bytes, and
  bitmap padding fail integrity validation before cache admission. Warm hits
  reuse admission, while invalidation forces a fresh check.
- Literal binding inputs and header bytes verify byte order and truncation;
  field sensitivity, equal-summary behavior, catalog table IDs, and old-format
  rejection protect the revised association contract.
- Delete rewrites in both domains preserve compact bytes, ordinals, bindings,
  and physical rows. Shared scan bodies and existing external-delete loading
  retain their separate memory and lifecycle guarantees.
- Cardinality, span, RowID, and body-budget boundaries have typed outcomes.
  Oversized legacy rewrites leave allocation state unchanged; highly
  compressible catalog values cannot bypass identity capacity.
- Checkpoint ordering, trailing deleted coverage, corruption propagation,
  recovery/reopen, and the engine benchmark fixtures verify integration with
  existing visibility and publication behavior.

## Open Questions

- [000206](../backlogs/000206-inline-adaptive-deletion-encoding-and-deletion-blob-retirement.md)
  owns joint identity/deletion admission, bounded LWC splitting, and deletion
  blob retirement. It must address worst-case future deletion growth before
  promising inline-only storage for every admitted block. Identity offloading
  remains an alternative only if the chosen bounded inline direction proves
  inadequate; no such path was implemented here.
- [000207](../backlogs/000207-evaluate-checksum-algorithms-across-all-use-cases.md)
  evaluates checksum algorithms across all uses; the binding optimization did
  not resolve full-block checksum cost.
- [000007](../backlogs/000007-lwc-oversize-rowpage-handling.md) and
  [000193](../backlogs/000193-eliminate-repeated-leaf-node-loads-in-catalog-root-readers.md)
  retain their independent oversized-source-page and catalog traversal work.
