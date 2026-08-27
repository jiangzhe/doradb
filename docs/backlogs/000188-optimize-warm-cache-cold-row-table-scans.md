# Backlog: Optimize warm-cache cold-row table scans

## Summary

Reduce warm-cache cold-row sequential and parallel table-scan cost by avoiding repeated full-block integrity validation and redundant column-index metadata decoding after immutable persisted blocks are safely resident. Preserve or deliberately redesign corruption-detection and cache-invalidation semantics instead of bypassing validation.

## Reference

Discovered while completing and profiling docs/tasks/000285-parallel-scan-benchmark-performance-proof.md. The release proof in docs/benchmark-tool.md uses 1,000,000 rows, projection [0, 1], 2,233 physical scan units, and a warm cache. Hot sequential scan measured 73.93 ms versus 263.15 ms for the 900,032-row cold-dominant fixture (3.56x); target-nine parallel scan measured 20.68 ms versus 57.08 ms (2.76x). Warm-up statistics showed 4,021 readonly-cache hits and zero misses, completed reads, or backend submissions for the cold-dominant scan.

Fresh 20-run CPU-clock profiles reproduced the sequential gap at 75.02 ms hot versus 264.40 ms cold. BLAKE3 accounted for 50.4% of sequential cold CPU samples and 42.2% of parallel cold CPU samples; persisted column-index metadata validation and parsing accounted for roughly another 14-16%. The relevant paths are doradb-storage/src/buffer/readonly.rs read_validated_block/read_shared_block, doradb-storage/src/table/access.rs load_table_scan_cold_page, doradb-storage/src/index/column_block_index.rs load_delete_deltas_and_row_ids/read_entry_view, and doradb-storage/src/lwc/block.rs PersistedLwcBlock::load/decode_value.

## Deferred From (Optional)

docs/tasks/000285-parallel-scan-benchmark-performance-proof.md; docs/rfcs/0030-shared-read-snapshots-parallel-table-scan.md Phase 5

## Deferral Context (Optional)

- Defer Reason: Task 000285 is scoped to adding the benchmark consumer and producing correctness and performance evidence for the existing parallel table-scan API. Changing readonly-cache integrity provenance, persisted column-index access, and LWC decoding would broaden that task into storage-format and buffer-pool optimization work with separate correctness risks and design review needs.
- Findings: The cold/hot gap is not caused by parallel orchestration or physical I/O: target-one parallel throughput matches sequential throughput, both shapes scan essentially the same number of physical units, and warmed cold runs perform zero backend reads. Each cold scan loads 2,009 LWC blocks and reopens one column-index leaf for every entry, plus three initial index planning pages, producing 4,021 validated cache hits. Because read_validated_block reruns its validator on resident hits and validation hashes the entire 64 KiB page, one scan hashes about 251.3 MiB. In addition, read_entry_view validates the leaf prefix plane to search for the entry and then leaf_entry_view validates it again. Per-row ColumnDeletionBuffer lookup was below 1% of CPU samples, so backlog 000111 is related but does not address the measured dominant costs. Cold work scales better than hot work (4.62x versus 3.61x at target nine), which hides part of the CPU excess in the parallel elapsed-time ratio.
- Direction Hint: Prioritize a sound once-per-residency-generation validation proof over simply switching cold reads to an unvalidated API. Establish whether the current contract intentionally detects corruption introduced after a block is resident, and preserve that property or document and test an approved replacement. Ensure raw reads cannot make a frame appear validated for an incompatible persisted format. Next, reuse metadata already established during worklist capture or batch cold entries by owning leaf so scans do not reopen and fully validate the same leaf for every LWC block; immediately reuse the already validated LeafPrefixPlane through leaf_entry_view_with_prefixes. Reprofile before investing in deletion filtering or LWC vectorization.

## Scope Hint

Design and implement a validated-residency mechanism that avoids rehashing an immutable 64 KiB persisted block on every readonly-cache hit while retaining a sound proof tied to the mapping, frame generation, persisted format or validator, and invalidation lifecycle. Reduce cold scan metadata work by avoiding one column-index leaf reopen per LWC entry and by removing duplicate whole-prefix-plane validation in read_entry_view. Evaluate carrying scan-ready row/delete metadata in the captured worklist or processing entries in leaf-local batches. After those dominant costs are addressed, measure whether cached LWC column views or a sequential/vectorized decoder is worthwhile. Keep per-row deletion-buffer filtering tracked separately by backlog 000111.

## Acceptance Hint

The 1,000,000-row hot and 900,000-row freeze benchmark shapes demonstrate a material reduction in cold sequential and target-capacity parallel scan CPU time without a material hot-scan regression. Profiling or deterministic instrumentation proves that warm resident hits no longer checksum every LWC and repeatedly loaded index page. Miss-time validation, raw-versus-validated read interactions, frame reuse, write-barrier invalidation, eviction/reload, and corrupted persisted-block behavior remain covered by tests; any change to detection of corruption introduced after residency is explicitly designed and tested. Cold/hot MVCC scan correctness, deletion overlays, sparse row-id sets, and external deletion blobs remain correct. Workspace and alternate libaio validation passes.

## Notes (Optional)

Related but distinct: docs/backlogs/000111-optimize-cold-row-visibility-filtering-mvcc-scans.md addresses per-row deletion-buffer visibility checks. Profiling for task 000285 showed those checks below 1%, while checksum and persisted-index validation dominated cold scan CPU.

