---
id: 0033
title: Parallel DiskTree Construction and Checkpoint Application
status: implemented
tags: [storage, index, ddl, checkpoint, parallelism, performance]
created: 2026-10-05
github_issue: 1144
---

# RFC-0033: Parallel DiskTree Construction and Checkpoint Application

## Summary

Cold CREATE INDEX previously collected and sorted one large vector, copied it
into mutation-writer state, and retained cold keys through hot validation.
This program delivered parallel resident cold extraction, direct sorted-run
consumption, streaming parallel DiskTree construction, and completed-root
cross-tier validation that releases cold input before hot extraction. Checkpoint
now extracts and reconciles captured changes in parallel within each secondary
index, selecting reconstruction or partial rewrite while preserving subtree
reuse and atomic publication. All four phases are implemented. External sorting
and cross-index checkpoint scheduling remain explicit follow-ups. [U1] [U2]
[U4] [B1] [B2] [D10]

## Context

RFC 0032 supplied resident sorted runs, bounded merging, duplicate evidence and
parallel hot-index construction. Cold construction still dominated checkpointed
CREATE latency; checkpoint used eager encoded sidecars and serial recursive
rewrites. The program reused ordering, admission and packing mechanisms while
keeping CREATE, recovery and checkpoint source/publication authority separate.
Each phase included production integration, tests, review and performance
evidence. [D4] [D9] [U3]

A pre-implementation regression exposed a correctness gap: a compressed leaf
that fit below a finite upper fence could overflow when promoted to an open
root. A 600-key fixture split 599/1; the surviving leaf required 10,194 bytes
with its finite fence but 319,280 bytes with an open fence, exceeding the
65,520-byte node capacity. Final-fence planning and checkpoint/restart coverage
closed this gap in the delivered program. [U8] [C3] [C4] [D10]

Task 000332 explicitly narrowed Phase 4 to within-index parallelism. It also
replaced CREATE's aggregate scratch ceiling with independent component budgets.
The completed scope does not imply cross-index scaling or acceptance of encoded
input larger than its resident allowance. Source backlog 000104 is closed;
000084 remains open for cross-index scheduling. [B1] [B2] [D10]

Issue Labels:

- type:epic
- priority:high
- codex

## Goals

Reduce cold CREATE and checkpoint costs through shared parallel extraction,
ordering and streaming construction. Release cold keys before hot work and
validate exact private-input coverage. Preserve MVCC, mutation semantics, sparse
reuse, atomic publication and settlement under failure or observer loss.

## Non-Goals

External sorting/spill, concurrent-DML CREATE, combined hot/cold lifecycles,
changes to recovery's trusted-input policy, cross-index or catalog checkpoint
parallelism, broader checkpoint orchestration, process-wide admission and
independent global compaction remain outside the delivered scope. Persisted
DiskTree/LWC formats, key encodings, redo and public DDL/transaction signatures
remain compatible. [U1] [U2] [B2] [B3]

## Design Inputs

### Documents

- [D1] `docs/architecture.md` and `docs/transaction-system.md` — subsystem and
  transaction ownership boundaries.
- [D2] `docs/index-design.md` and `docs/secondary-index.md` — physical entry
  identity, runtime history and dual-tree candidates.
- [D3] `docs/table-file.md`, `docs/checkpoint.md`, `docs/deletion-checkpoint.md`
  and `docs/garbage-collect.md` — cutoff eligibility and coherent CoW publication.
- [D4] `docs/rfcs/0032-in-memory-parallel-hot-index-build.md` and
  `docs/tasks/000319-create-index-hot-build-integration.md` — shared hot mechanisms.
- [D5] `docs/engine-component-lifetime.md` and `docs/shutdown-and-poison.md`
  — accepted obligations, settlement and fatal ownership.
- [D6] `docs/process/coding-guidance.md`, `docs/process/unit-test.md` and
  `.config/nextest.toml` — review and validation contracts.
- [D7] `docs/benchmark-tool.md` — public-call measurements and verification.
- [D8] `docs/unsafe-usage-principles.md` and
  `docs/process/unsafe-review-checklist.md` — packed-layout safety boundaries.
- [D9] `docs/rfcs/0014-dual-tree-secondary-index.md` and
  `docs/tasks/000118-disk-tree-checkpoint-sidecar-publication.md` — mutation and
  durable secondary-index publication semantics.
- [D10] Completed implementation and review records:
  `docs/tasks/000329-streaming-parallel-disk-tree-bulk-construction.md`,
  `docs/tasks/000330-completed-root-cross-tier-validation.md`,
  `docs/tasks/000331-parallel-cold-extraction-and-direct-run-consumption.md`,
  `docs/tasks/000332-parallel-disk-tree-checkpoint-reconciliation.md`.

### Code References

- [C1] `doradb-storage/src/catalog/index.rs` — CREATE capture, staged ownership,
  sealing and publication.
- [C2] `doradb-storage/src/index/build/` — admission, source extraction, resident
  runs, merge, packing, sealed roots, cross-tier validation and checkpoint assembly.
- [C3] `doradb-storage/src/index/btree/algo.rs` and
  `doradb-storage/src/index/btree/node.rs` — capacity planning and packed layout.
- [C4] `doradb-storage/src/index/disk_tree.rs` — persisted reads, geometry and
  ordered-operation regression fixtures.
- [C5] `doradb-storage/src/table/persistence.rs` and
  `doradb-storage/src/table/deletion_buffer.rs` — captured checkpoint selections,
  deletion eligibility, index staging and table publication.
- [C6] `doradb-storage/src/file/cow_file.rs` and
  `doradb-storage/src/file/table_file.rs` — allocation, retained writes and CoW roots.
- [C7] `doradb-storage/src/trx/sys.rs`, `doradb-storage/src/trx/purge.rs` and
  `doradb-storage/src/trx/read_snapshot.rs` — registered snapshots and GC horizon.
- [C8] `doradb-storage/src/table/access.rs`, `doradb-storage/src/table/gc.rs`
  and `doradb-storage/src/index/secondary_index.rs` — MVCC and candidate history.
- [C10] `doradb-bench/src/plan.rs`, `doradb-bench/src/engine_config.rs` and
  `doradb-bench/src/workload/` — matched fixtures and complete-content verification.
- [C11] `doradb-storage/src/conf/index_build.rs` and
  `doradb-storage/src/profiling/index_build.rs` — configuration and measurements.
- [C12] `doradb-storage/src/index/column_block_index.rs` — physical RowID
  membership, descriptor traversal and durable deletion metadata.

### Conversation References

- [U1] Reuse hot-build mechanisms for cold construction; defer larger-than-memory
  builds until spill representation and lifecycle have a separate design.
- [U2] Share primitives while retaining caller-owned cold input/output lifecycles
  and completed-private-input validation.
- [U3] Deliver production integration, tests and measurements in each phase.
- [U4] Include checkpoint application, key deletions and merge strategy from 000084.
- [U5] Preserve old-snapshot candidates without relying on memory index copies;
  test late first reads and explicit GC-horizon deletion eligibility.
- [U8] Recheck packing whenever fences or shape change, including root promotion.
- [U9] Separate single-run merge bypass from construction partitioning; compare
  old, single-worker streaming and multi-worker streaming construction.
- [U10] Bound checkpoint provisional output through assembly and avoid growing
  sibling-prefix repacking; preserve parent-compatible geometry.
- [U11] Seal completed cold evidence without retaining input owners; prove actual
  cold-key release before hot work while the private root remains readable.
- [U12] Admit retained allocations before growth and transfer reservations with
  ownership; cover source and downstream resource failures.
- [U13] Keep unique logical-key mutations together; partition non-unique work by
  exact key including RowID, preserving same-entry delete precedence.
- [U14] Keep allocation/submission with one coordinator, distinguish write
  acceptance from completion, and build parents from stable child descriptors.

### Source Backlogs

- [B1] `docs/backlogs/closed/000104-stream-parallel-create-index-cold-build.md`
  — completed resident cold pipeline; external-memory work moved to 000212 and
  selector calibration to 000213.
- [B2] `docs/backlogs/000084-parallel-secondary-disk-tree-checkpoint-application.md`
  — within-index work delivered; cross-index scheduling remains open.
- [B3] `docs/backlogs/000083-full-disk-tree-compaction-policy.md`
  — independent global compaction remains outside this program.

## Decision

### Shared mechanisms and separate ownership

CREATE and checkpoint share allocation-lifetime admission, local sorting,
resident runs, bounded merge consumption and final-fence node planning.
Single-run input supports borrowed direct consumption with multiple construction
partitions. Multiple runs stream through bounded co-rank/merge machinery without
flattening. Checkpoint uses its own mutation reducer and construction pipeline;
a common resident/external sorted-cursor construction interface remains future
work. MemIndex retains its own allocation and installation lifecycle. [C2]
[D4] [U2] [U9]

Workers pack aligned node images; one coordinator owns the mutable-file fork,
allocation and write submission. The ownership progression is packed image →
write acceptance with stable BlockID → observed write completion → complete
private root → caller publication. Accepted child descriptors can feed parent
packing before child I/O finishes; private-root authority requires exact
coverage, required validation, coherent geometry and successful settlement of
all writes. Neither an accepted write nor a private root authorizes publication.
[D1] [D5] [C6] [U14]

Cleanup stops admission, rejects or drains handoffs, settles accepted CPU/read/I/O
work, and reclaims only owned unpublished blocks. Completed writes are observed
independently of key order. Cancelled observers do not detach retained obligations;
Fatal uncertainty retains ownership. Repeated settlement cannot reclaim the same
allocation twice. Existing CoW roots retain ownership of reused blocks. [C2]
[C6] [D3] [D5]

### Resident cold CREATE and sealed validation

CREATE captures the table/layout, column root, pivot, timestamp and DDL exclusion.
After worker admission, extraction requests P=4W whole-block ranges, choosing
the first level with at least 8P entries or a leaf fallback. Workers copy at
most 64 descriptors at a time and retain traversal coordinates rather than
metadata guards across later page loads. Binding checks, sparse RowID membership,
durable/current deletes and uncommitted-marker errors remain authoritative.
Every partition, including empty output, certifies exact source exhaustion.
All encoded runs remain resident until construction seals. [C1] [C2] [C12] [U12]

Sealing consumes settled construction state for both index modes and releases
all cold run/key owners before hot extraction. It retains compact build identity,
root shape/counts and file/read/reclamation authority, without back-references
to construction plans or keys. Accepted work must settle before either successful
release or failure cleanup. Typed cooperative stop denotes incomplete work;
it cannot become EOF, successful empty coverage or publication authority. [C2]
[C6] [U11] [D10]

Unique CREATE selects among batched forward probes of the cold root, ordered
hot/cold scanning, and reverse cold scans searching every complete hot run.
Forward batches contain up to 128 keys; reverse searches reuse monotonic positions.
Checked borrowed persisted views avoid owned leaf-key copies. Evidence binds
exact cold/hot identities, direction and exhaustive coverage. Execution/coverage
errors precede duplicate diagnostics, and hot/hot conflicts precede hot/cold
conflicts. Non-unique CREATE and trusted recovery retain their explicit
not-required cross-tier policy; empty unique inputs still need checked evidence.
[D2] [D4] [C2] [C4]

Selection uses complete input counts, hot run/partition counts and cold height.
The weights established in Phase 2 were not recalibrated after borrowed-read and
parallel-extraction changes; backlog 000213 owns that measurement. Installation
and catalog/root/layout publication follow completed construction and required
validation. [C1] [C2] [D10]

### Checkpoint eligibility and exact mutation semantics

Checkpoint captures immutable Transition page identities/visibility bitmaps and
resolved old-LWC RowID/ordinal selections. Workers decode those selections per
index without choosing newer visibility. Cold deletion uses the fixed published
GC horizon and markers satisfying `previous_cutoff <= delete_CTS < cutoff`,
below the captured cold pivot. Active transactions and registered read snapshots
constrain that horizon. Disk candidates remain available to old snapshots even
when memory copies are absent; CoW retention alone is not a logical eligibility
proof. [D3] [C5] [C7] [C8] [U5]

Homogeneous mutation runs preserve source order. Complete mutation groups stay
within one job: logical encoded key for unique indexes, exact key including
RowID for non-unique indexes. A dominant non-unique logical key may therefore
span jobs. The streaming reducer preserves these rules: [D9] [C2] [C5] [U13]

| Operation | Result |
| --- | --- |
| Multiple unique PUTs | Stable source order selects the latest PUT. |
| Unique PUT with same-key DELETEs | PUT wins. |
| Unique conditional DELETE | Remove only the matching captured base owner; preserve a different owner or absence. |
| Non-unique DELETE | Remove exactly the encoded key/RowID entry. |
| Non-unique PUT and DELETE of the same exact entry | DELETE wins, including synthetic overlap fixtures. |

The planner chooses no work, reconstruction or partial rewrite before output.
Small/empty cases bypass general estimation; other cases refine deterministic
base-path and mutation samples from 16 to 64 per source, with at most 4,096
identity inspections. READ/PACK/WRITE weights are 1/2/4 with a 10% retention
margin; uncertain estimates retain partial rewrite. Both modes use the same
reducer. Unchanged roots retain identity; partial rewrite reuses untouched
subtrees, while reconstruction can produce any required height or no root.
Reusable root statistics remain in 000215. [C2] [C4] [B2] [D10]

Jobs stream finalized interiors and bounded provisional boundaries. Ordered
assembly consumes those boundaries incrementally, retains attachment heights,
and balances parents. Completed but unassembled jobs retain scheduling slots;
future output must obtain quota before acknowledgement, while the current
partition retains progress capacity. Optional neighbor absorption stops after
eight candidates. Required final-fence repair remains independent: compression
loss can require extra nodes even after deletion, so root collapse is conditional
on representability. Finalized interiors are written once and are not reread
for ordinary boundary reconciliation. [C2] [C3] [U8] [U10]

Indexes run sequentially in slot order under shared component allowances.
Only complete per-index results are staged, and all secondary roots, LWC/routing
changes, persistent deletions and replay cutoffs publish through one table-file
commit. Later-index failure cannot publish earlier staged roots. Deletion-only
failures can retry where reversible; failures after Transition retain existing
poison rules. Cross-index scheduling remains in 000084. [B2] [C5] [C6] [D3]

### Admission, configuration and measurement contracts

CREATE and checkpoint have separate configurations with the same independent
component model. Cold CREATE's `max_scratch_bytes` was replaced by
`max_input_bytes`, `worker_scratch_bytes` and `assembler_scratch_bytes`;
`EngineConfig::checkpoint_index_build` controls checkpoint independently.
Output capacity is derived from worker/queue/write limits. [C11] [D10]

| Allowance | Default | Supported upper bound |
| --- | --- | --- |
| Resident input | 256 MiB | 64 GiB |
| Scratch per worker/job | 4 MiB | 1 GiB |
| Shared assembler/planner scratch | 12 MiB | 1 GiB |
| Workers | Pool size, capped at 256 | Pool size and 256 |
| Queued buffers | 8 | 256 |
| Accepted unsettled writes | 32 | 256; at least 2 |

Local progress minima, overflow and supported ranges are checked before startup.
Hot builds also cap scratch at 64 GiB, workers at 256 and the soft run target at
8,192 pages. CREATE releases extraction scratch before construction and admits
all merge streams before durable output. Checkpoint skips source capture when
no active secondary-index work exists. [C2] [C5] [C11]

Reservations precede retained allocations, include replacement-growth overlap,
and follow storage through its final owner, including shared fences and I/O
buffers. Consumed output slots are reusable. Resident exhaustion produces typed
failure with settled cleanup. CREATE per-row decoded temporaries, pool-owned pins
and fixed runtime overhead are not part of the retained-memory bound; admitted
memory is distinct from process RSS and final index size. [U1] [U12] [C2] [C6]

Profiling publishes successful-operation evidence only. Wall spans and worker
sums may overlap; scratch peaks measure simultaneous admission. Cold source
pin-byte estimates were removed, while validation retains its separate estimate.
Checkpoint exposes coarse outcome/input/rewrite/timing/worker/scratch metrics;
end gauges describe the last successful checkpoint. Detailed boundary evidence
is test-only, and profiling grants no correctness or admission authority.
[D7] [C10] [C11] [D10]

## Alternatives Considered

### One Combined Hot/Cold Pipeline

Why Not Chosen: Combining both tiers couples input lifetimes, durable and memory
output ownership, and failure paths. Shared mechanisms with separate caller
lifecycles preserve the successful hot/recovery contracts. [U2] [D4] [C2]

### Retain the Full Cold Vector for Validation

Why Not Chosen: Useful during Phase 1, but retaining all cold keys through hot
construction prevents early release and ties parallel extraction to a flat
representation. Sealed roots support complete validation independently. [B1]
[U11] [C1] [C2]

### Rebuild Every Checkpoint Tree

Why Not Chosen: Reconstruction is useful for dense changes, but unconditional
rebuilding loses sparse subtree locality. The delivered selector keeps partial
rewrite and leaves independent compaction policy to 000083. [B2] [B3] [C2]

### General Spill-Capable Construction in This Program

Why Not Chosen: Spill representation, bounded run generation, merge fan-in and
cleanup require a separate design. Resident admission was the agreed boundary;
000212 retains the external-memory acceptance and shared-interface work. [U1] [B1]

## Unsafe Considerations

The program reuses packed-node and aligned-buffer APIs. Run references retain
their allocations; persisted borrowed views remain guard-bound, and output
buffers stay owned through backend release. Final fences, encoding and height
must fit the node before packing; checksums, alignment and exclusive block
ownership remain enforced at their respective boundaries. Changed fences or
shape invalidate previous capacity proofs. [C2] [C3] [C4] [C6] [D8]

Phase 1/2 reviews refreshed unsafe inventories and introduced no new unsafe
layout operation. Later validation retained structural, lifetime, corruption,
settlement and root-promotion regressions. An instrumented MVCC crash discovered
in Phase 2 remains unresolved in 000211; passing reruns are not evidence of a fix.
[D6] [D8] [D10]

## Implementation Phases

All four task documents contain completed implementation/review/test records.
Issues #1145, #1147, #1149 and #1151 were verified closed on 2026-10-22.
Open follow-ups are under Future Work.

- **Phase 1: Streaming Parallel DiskTree Bulk Construction**
  - Scope: Admitted serial input and parallel durable packing for both CREATE modes.
  - Task Doc: `docs/tasks/000329-streaming-parallel-disk-tree-bulk-construction.md`
  - Task Issue: `#1145`
  - Phase Status: done
  - Implementation Summary: Delivered direct-run partitioning, shared leaf planning, retained asynchronous writes, cold-build configuration and publication-only profiling; later phases removed serial extraction and retained cold validation keys.
  - Related Backlogs:
    - `docs/backlogs/closed/000104-stream-parallel-create-index-cold-build.md`

- **Phase 2: Completed-Root Cross-Tier Validation**
  - Scope: Sealed input release and complete forward, ordered and reverse validation.
  - Task Doc: `docs/tasks/000330-completed-root-cross-tier-validation.md`
  - Task Issue: `#1147`
  - Phase Status: done
  - Implementation Summary: Delivered actual cold-key release before hot extraction, exact root/input coverage, bounded borrowed reads and retained cleanup. Review also fixed coalesced readonly-frame wakes; selector calibration and the instrumented MVCC crash remain explicit follow-ups.
  - Related Backlogs:
    - `docs/backlogs/closed/000104-stream-parallel-create-index-cold-build.md`

- **Phase 3: Parallel Cold Extraction and Direct Run Consumption**
  - Scope: Bounded whole-block source traversal, parallel local sorting and direct resident-run construction.
  - Task Doc: `docs/tasks/000331-parallel-cold-extraction-and-direct-run-consumption.md`
  - Task Issue: `#1149`
  - Phase Status: done
  - Implementation Summary: Delivered the common 4W extraction policy, coordinate-based traversal and typed cooperative stop. The forced single-worker single-run policy was withdrawn after workload-dependent results; external runs and unified run sizing remain in 000212.
  - Related Backlogs:
    - `docs/backlogs/closed/000104-stream-parallel-create-index-cold-build.md`
    - `docs/backlogs/closed/000214-reduce-resident-cold-build-cpu-small-worker-overhead.md`

- **Phase 4: Parallel DiskTree Checkpoint Reconciliation**
  - Scope: Captured-source extraction, exact mutation reduction and adaptive reconstruction/partial rewrite within each index.
  - Task Doc: `docs/tasks/000332-parallel-disk-tree-checkpoint-reconciliation.md`
  - Task Issue: `#1151`
  - Phase Status: done
  - Implementation Summary: Delivered bounded incremental assembly, final-fence repair, atomic root/cutoff publication and settled failure cleanup. Review separated CREATE/checkpoint component budgets and hardened output acknowledgement, ready-write observation and repeatable rollback. Cross-index scheduling remains in 000084; no following phase is pending.

## Validation and Performance

The task records preserve final test/style evidence and historical measurements:
[D6] [D7] [D10]

| Phase | Workspace tests passed | Profiling-disabled storage passed | Resolve style gate |
| --- | ---: | ---: | --- |
| 1 | 2,325 | 2,060 | 38 Rust files; 600 test contracts |
| 2 | 2,350 | 2,084 | 26 Rust files; 318 test contracts |
| 3 | 2,364 | 2,098 | 19 Rust files; 208 test contracts |
| 4 | 2,413 | 2,145 | 36 Rust files; 488 test contracts |

Phase 4 passed strict Clippy in both configurations, formatting and diff checks;
its two suites each skipped two tests. Semantic review retained independent
content/restart oracles and deterministic completion gates. Coverage included
exact source/mutation coverage, duplicate precedence, old snapshots without
memory copies, sparse reuse, root growth/collapse and compressed 599/1 promotion,
multi-index atomicity, admission failures, reversed completion, observer loss,
quota pressure, unordered write completion and repeatable rollback. [D10]

Review also repaired the Phase 1 hinted-search assertion race and Phase 2
readonly-frame wake issue. Phase 3's coordinate traversal passed a 255-cursor
pool-pressure regression; typed stop preserved late Runtime/Fatal causes.
The Phase 2 instrumented old-snapshot SIGSEGV was outside the changed CREATE
lifecycle; 100 isolated reruns and a full coverage rerun passed without identifying
a cause. Backlog 000211 retains that unresolved evidence. [D10]

Matched release measurements used fresh processes/roots, identical per-comparison
allocator/settings and untimed complete-content verification. Caches were warmed
by preparation/verification rather than flushed; checkpointed placement does not
mean device-cold I/O. The results below predate later review refinements and are
not fresh performance claims for the final revision. [D7] [C10] [D10]

| Evidence | Historical outcome and limit |
| --- | --- |
| Phase 1: 171 verified invocations | 1M cold CREATE medians fell 931.025→218.248 ms unique and 801.963→220.362 ms non-unique; packing concurrency reached two. Writes rose at most 2.2%; tiny inputs showed no speedup. |
| Phase 2: completed-root validation | Releases cold keys but can trade latency for memory: balanced 262k CREATE rose 14.372→21.119 ms in initial calibration. Later 1M/10M observations exposed resident collection limits and did not recalibrate selection. |
| Phase 3: 102-observation comparison | Four-worker cold latency improved 42.5% unique and 26.2% non-unique versus serial collection, with higher CPU/RSS. Follow-up single-run experiments had workload-dependent results and were withdrawn. |
| Phase 4: seven runs per case/variant | Public checkpoint insert medians fell 2474.25→49.36 ms and delete 710.24→36.35 ms versus serial 2976ed05; packing overlap reached three to four workers. Sparse locality was retained. No-op rose 2.78→3.14 ms; skew used more CPU, and wide growth wrote about 3.4% more blocks. |

Earlier focused coverage and benchmark artifacts are recorded in each task;
they were not regenerated after every refinement. Stage spans overlap and must
not be added. The final Phase 4 suites validate the integrated implementation;
this RFC resolution changes documentation only. [D10]

## Consequences

### Positive

- Cold CREATE no longer retains redundant mutation-writer materialization or
  cold validation keys during hot work.
- Shared extraction, merge and packing support measured parallel work while
  caller-owned evidence preserves publication and cleanup boundaries.
- Checkpoint preserves sparse locality and fixed-cutoff MVCC, and safely repairs
  fence expansion during deletion and root collapse.

### Negative

- Encoded inputs remain resident; larger builds can fail admission.
- Component budgets, provisional geometry and retained I/O add lifecycle complexity.
- Parallel latency gains can cost CPU, RSS or extra writes; small cases may not improve.
- Cross-tier selection and checkpoint estimation retain workload-dependent tuning
  limits, and checkpoint indexes still run sequentially.

## Open Questions

No implementation blocker remains for the delivered scope. Unresolved work is
linked below: the instrumented MVCC ownership failure, optimized validation
crossover points, external run/interface design, cross-index scheduling and
reusable checkpoint root statistics. Passing tests do not resolve those questions.

## Future Work

These follow-ups retain their existing deferral context and remain open:

- [000084: cross-index checkpoint scheduling](../backlogs/000084-parallel-secondary-disk-tree-checkpoint-application.md)
  — deferred from Phases 1/4; reuse one coordinator and shared admission across indexes.
- [000211: instrumented MVCC crash](../backlogs/000211-investigate-instrumented-mvcc-old-snapshot-read-crash.md)
  — Phase 2 verification finding; establish ownership cause and a deterministic regression.
- [000212: external-memory cold runs](../backlogs/000212-external-memory-create-index-cold-runs.md)
  — Phases 3/4; bounded run formation, spill lifecycle and shared sorted-input/construction
  interfaces for CREATE and checkpoint reconstruction, including single-worker findings.
- [000213: validation selector calibration](../backlogs/000213-calibrate-completed-root-cross-tier-validation-selection.md)
  — Phases 2/3; measure optimized forward/ordered/reverse crossover points directly.
- [000215: checkpoint root statistics](../backlogs/000215-root-level-secondary-index-statistics-for-checkpoint-planning.md)
  — Phase 4; maintain reusable per-root statistics without per-subtree strategy selection.
- [000083: independent DiskTree compaction](../backlogs/000083-full-disk-tree-compaction-policy.md),
  [000033: deletion marker selection](../backlogs/000033-deletion-checkpoint-marker-selection-parallelization.md)
  and [000034: RowID-to-block resolution](../backlogs/000034-deletion-checkpoint-rowid-to-block-resolution-parallelization.md)
  — separate maintenance and source-selection work outside this program.

Backlog 000104 remains closed for the delivered resident pipeline. Backlog 000214
remains archived as a withdrawn broad performance deferral; its run-sizing
findings are retained in 000212. Neither closure claims external-memory acceptance
or universal CPU/RSS improvement. [B1] [D10]

## References

- [Hot-index pipeline](0032-in-memory-parallel-hot-index-build.md)
- [Dual-tree secondary index](0014-dual-tree-secondary-index.md)
- [Phase 1 implementation](../tasks/000329-streaming-parallel-disk-tree-bulk-construction.md)
- [Phase 2 implementation](../tasks/000330-completed-root-cross-tier-validation.md)
- [Phase 3 implementation](../tasks/000331-parallel-cold-extraction-and-direct-run-consumption.md)
- [Phase 4 implementation](../tasks/000332-parallel-disk-tree-checkpoint-reconciliation.md)
- [Closed cold-build backlog](../backlogs/closed/000104-stream-parallel-create-index-cold-build.md)
- [Archived performance deferral](../backlogs/closed/000214-reduce-resident-cold-build-cpu-small-worker-overhead.md)
- [Deletion checkpoint](../deletion-checkpoint.md)
- [Benchmark tool](../benchmark-tool.md)
