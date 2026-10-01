# Backlog: Evaluate and select checksum algorithms across all checksum use cases

## Summary

Evaluate and select checksum algorithms across all DoraDB checksum and integrity-fingerprint use cases. Compare faster candidates such as xxHash (including XXH3 64-bit and 128-bit variants) with the existing BLAKE3 and CRC32 implementations, using each use case's integrity requirements and measured end-to-end cost. Produce an explicit policy and implementation direction covering the whole repository, with justified exceptions where one algorithm is unsuitable.

## Reference

- User request on 2026-09-30: investigate a better checksum algorithm, for example xxHash, and apply the evaluation to all checksum use cases.
- [Task 000322](../tasks/000322-adaptive-cold-row-id-encoding-with-compact-lookup.md).
- Checkpoint CPU profiles from task 000322: BLAKE3 LWC block checksums account for 57.12% / 50.01% of sampled user CPU for one million inserts with 0% / 1% pre-checkpoint deletes.
- Fingerprint buffering experiment from task 000322: batching existing BLAKE3 input reduces fingerprint CPU but leaves the algorithm-selection question open.
- Integrity foundations: [task 000059](../tasks/000059-file-integrity-foundation.md), [task 000060](../tasks/000060-checksum-rollout-for-data-pages.md), [task 000160](../tasks/000160-evictable-buffer-pool-spill-file-checksums.md), and [task 000286](../tasks/000286-cache-readonly-validation-by-residency-generation.md).
- Initial code inventory: `file/block_integrity.rs`, `file/super_block.rs`, `file/table_file.rs`, `file/multi_table_file.rs`, `buffer/evict.rs`, `index/disk_tree.rs`, `index/column_block_index.rs`, `index/column_deletion_blob.rs`, `lwc/mod.rs`, `log/format.rs`, and `catalog/table.rs` under `doradb-storage/src`; `doradb-bench/src/workload/verification.rs`; `tools/coverage/model.rs`.

## Deferred From (Optional)

[Task 000322](../tasks/000322-adaptive-cold-row-id-encoding-with-compact-lookup.md), follow-up review of checkpoint CPU profiles and canonical fingerprint buffering.

## Deferral Context (Optional)

- Defer Reason: Checksum selection crosses storage formats, recovery, buffer-pool I/O, and logical identity contracts. It needs a separate evaluation and rollout decision beyond task 000322's adaptive row-ID encoding scope.
- Findings: The checkpoint profile attributes roughly 50-57% of sampled user-space CPU to LWC block checksum generation, which already hashes whole block images; this is separate from the per-delta fingerprint update loop. The 4 KiB buffering prototype reduced row-shape fingerprint share from 9.52% to 4.79%, while the small unprofiled sample did not establish a whole-checkpoint speedup. At backlog creation, BLAKE3 supplied the shared 32-byte block trailer, separate super-block footers, a truncated 128-bit canonical row-shape fingerprint, and a 32-byte storage-schema fingerprint; the binding follow-up below supersedes that row-shape fingerprint. Redo data blocks use crc32fast with a four-byte field, while redo super-block slots use the shared BLAKE3 integrity envelope. Shared block helpers also cover DiskTree and evictable spill pages. Readonly validation already runs once per admitted immutable generation. No xxHash implementation has been benchmarked or selected in this work.
- Direction Hint: Treat xxHash as a candidate to evaluate, not a predetermined replacement. Establish a measured checksum policy across all uses, preserving each use's required detection/binding properties. Compare algorithms using appropriate bulk/buffered input so call overhead is not mistaken for algorithm cost; keep batching improvements and algorithm replacement separately measurable. Decide whether one default is appropriate or distinct roles need different algorithms, and record every exception.

## Scope Hint

- Inventory every checksum and integrity-fingerprint producer, verifier, and consumer: persisted LWC/index/blob blocks, table/catalog metadata and super-blocks, redo data and super-blocks, buffer-pool spill/reload, fixed-input block binding and canonical schema binding, benchmark verification, and tooling content digests. Record purpose, algorithm/width, covered bytes and padding, input sizes/update patterns, persistence/version dependencies, and validation frequency. Evaluate applicability for auxiliary tooling explicitly rather than silently omitting it.
- Compare BLAKE3, existing CRC32, and suitable xxHash variants; consider hardware-accelerated CRC alternatives where relevant. Assess corruption detection, collision risk/digest width, any required cryptographic properties, determinism/byte order, streaming equivalence, implementation maturity, CPU feature dispatch, and portable fallback cost. Distinguish integrity checks and logical identity binding when choosing a policy.
- Benchmark tiny fingerprint inputs, actual block/page/redo sizes, larger row-shape streams, aligned and unaligned data, and incremental versus buffered/bulk updates. Cover representative aarch64 and x86_64 paths, including SIMD availability. Measure checksum generation and verification, CPU time/throughput, scratch/allocation cost, and checksum storage overhead.
- Use doradb-bench to measure checkpoint, validated cold reads, spill/reload, redo write/replay, recovery, and relevant fingerprint consumers. Reuse the one-million-row insert-only and 1%-random-delete fixtures as baselines. Separate hashing microbenchmarks from repeated end-to-end results and preserve the existing readonly admission-validation boundary.
- Specify a default or justified per-use choices, shared API ownership, and an explicit transition for persisted checksum algorithms/widths and canonical fingerprint versions. Audit footer offsets, payload capacity, serialization, producer/consumer agreement, mixed-format handling or deliberate rebuild requirements, typed corruption errors, and recovery behavior. Determine during planning whether the cross-subsystem rollout requires an RFC.

## Acceptance Hint

- A complete inventory accounts for every checksum/fingerprint use and states its selected algorithm/width or a justified decision to retain the current implementation; ordinary hash-table hashing is distinguished by purpose.
- Reproducible results compare xxHash candidates against current algorithms with documented hardware, compiler, features, input sizes, update patterns, and repeated engine workloads. Report uncertainty and regressions as well as gains; define acceptance budgets before selecting the policy.
- The selection explains integrity/collision requirements, CPU and storage tradeoffs, and whether the measured improvement persists beyond isolated hashing. Algorithm and buffering effects are reported separately.
- A concrete implementation/transition plan covers all affected owners and formats, with verification for known vectors, streaming equivalence, byte order, corruption, canonical fingerprint binding, reopen/recovery, and the chosen old/new-format policy. Any broader rollout is linked to follow-up task/RFC work.

## Notes (Optional)

The buffering patch in the experiment is a prototype and has not been applied to production code. Historical checksum-foundation backlogs 000051 and 000069 are closed; this item evaluates algorithm choice across existing uses rather than reopening their implementation scope. Creating this backlog does not change an algorithm, digest width, persisted format, or validation policy.


Task 000322 subsequently replaced the per-RowID fingerprint with a u64
`block_binding_value`: one BLAKE3 call over 36 stack bytes containing `LWCBIND1`,
table ID, start/end RowIDs, and row count. Evaluate that current fixed-input
use alongside the historical fingerprint measurements; full-block checksums
still use the existing BLAKE3 algorithm. Task 000322 summarizes the resulting
checkpoint CPU measurements.
