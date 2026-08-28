# Backlog: Optimize warm-cache cold-row table scans

## Summary

Reduce the remaining warm-cache cold-row sequential and parallel table-scan cost after task 000286 eliminated repeated full-block integrity validation. The dominant remaining issue is repeated column-index leaf decoding and whole-prefix-plane validation for every LWC entry; repeated per-value LWC layout decoding is the next measurable cost.

## Reference

Discovered while completing and profiling docs/tasks/000285-parallel-scan-benchmark-performance-proof.md. The release proof in docs/benchmark-tool.md uses 1,000,000 rows, projection [0, 1], 2,233 physical scan units, and a warm cache. Hot sequential scan measured 73.93 ms versus 263.15 ms for the 900,032-row cold-dominant fixture (3.56x); target-nine parallel scan measured 20.68 ms versus 57.08 ms (2.76x). Warm-up statistics showed 4,021 readonly-cache hits and zero misses, completed reads, or backend submissions for the cold-dominant scan.

Task 000286 implemented validation provenance tied to the readonly mapping and frame generation, so warm validated hits no longer rehash the resident 64 KiB block. A fresh 20-run release CPU-clock profile after that change measured a 71.43 ms hot median and 128.55 ms cold median, leaving a 57.12 ms sequential gap. The cold run still recorded 4,021 readonly-cache hits and zero misses, completed reads, or backend submissions.

Approximate excess CPU-sample attribution put column-index metadata work at 67% of the remaining gap, LWC decode/layout work at 14%, cold-row cursor and lazy-row work at 6%, and deletion visibility at 2%; readonly-cache lookup and latching contributed less than 1%. The relevant remaining paths are doradb-storage/src/table/access.rs load_table_scan_cold_page, doradb-storage/src/index/column_block_index.rs load_delete_deltas_and_row_ids/read_entry_view/validate_leaf_prefixes, and doradb-storage/src/lwc/block.rs PersistedLwcBlock::decode_value.

## Deferred From (Optional)

docs/tasks/000285-parallel-scan-benchmark-performance-proof.md; docs/rfcs/0030-shared-read-snapshots-parallel-table-scan.md Phase 5

## Deferral Context (Optional)

- Defer Reason: Task 000285 was scoped to benchmark coverage and performance proof. Task 000286 then addressed the independently reviewable buffer-pool validation-provenance problem. Changing persisted column-index access and LWC decoding remains separate optimization work with memory, storage-format, and correctness tradeoffs.
- Findings: The remaining cold/hot gap is not caused by parallel orchestration, physical I/O, checksumming, or readonly-cache lookup. Each cold scan loads 2,009 LWC blocks and reopens one column-index leaf for every entry, plus three initial index planning pages, producing 4,021 warm cache hits. read_entry_view constructs and validates the complete LeafPrefixPlane to locate an entry, then calls leaf_entry_view, which constructs and validates that plane again. validate_leaf_prefixes walks every leaf entry, decodes its row/delete metadata, and builds and sorts validation ranges, so reopening a leaf once per contained LWC entry makes metadata work approach quadratic in entries per leaf. PersistedLwcBlock::decode_value also reparses column offsets and constructs a compressed-column reader for each projected value, about 1.8 million calls for 900,032 rows and two projected columns. Per-row ColumnDeletionBuffer lookup remains a small part of the gap, so backlog 000111 is related but lower priority.
- Direction Hint: First, have read_entry_view reuse its already validated LeafPrefixPlane through leaf_entry_view_with_prefixes. Then eliminate the per-LWC leaf reopen by capturing scan-ready row/delete metadata in the worklist or processing entries in leaf-local batches; compare retained metadata memory against the CPU reduction, and derive dense row IDs from entry metadata where possible. Reprofile after those changes. If LWC decoding remains material, construct projected column views once per block or add a sequential decoder instead of rebuilding layout state per value. Keep deletion-buffer filtering tracked separately by backlog 000111.

## Scope Hint

Task 000286 owns and has completed the validated-residency mechanism. This backlog now covers removing duplicate whole-prefix-plane validation in read_entry_view, avoiding one column-index leaf reopen per LWC entry, and evaluating scan-ready captured metadata versus leaf-local batching. After the metadata cost is reduced, measure cached projected LWC column views or a sequential decoder. Keep per-row deletion-buffer filtering tracked separately by backlog 000111.

## Acceptance Hint

Starting from the post-task-000286 medians of 71.43 ms hot and 128.55 ms cold, the 1,000,000-row hot and 900,000-row freeze benchmark shapes demonstrate a material reduction in cold sequential and target-capacity parallel CPU time without a material hot-scan regression. Profiling or deterministic instrumentation proves that cold execution no longer reopens and fully validates the same column-index leaf for every LWC entry. The validation-provenance behavior established by task 000286 remains unchanged. Cold/hot MVCC scan correctness, deletion overlays, sparse row-id sets, and external deletion blobs remain correct. Workspace and alternate libaio validation passes.

## Notes (Optional)

Task 000286 resolved the checksum portion of the original backlog evidence; this item remains open for column-index metadata and LWC decoding work. Related but distinct: docs/backlogs/000111-optimize-cold-row-visibility-filtering-mvcc-scans.md addresses per-row deletion-buffer visibility checks, which remained a small part of the post-task-000286 profile.

## Close Reason

- Type: implemented
- Detail: Implemented via docs/tasks/000287-vectorize-cold-row-scan-metadata-and-lwc-decoding.md
- Closed By: backlog close
- Reference: User decision
- Closed At: 2026-08-28
