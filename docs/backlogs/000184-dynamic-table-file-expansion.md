# Backlog: Support Dynamic Table File Expansion

## Summary

Add failure-atomic online expansion for durable user table files so checkpoint and index publication can grow beyond the current fixed allocation-map capacity instead of returning StorageFileCapacityExceeded.

## Reference

Task 000269 checkpoint-table execution exposed the limit: freezing about 500,000 generated 128-byte rows exhausted LWC block allocation. New table files are created at TABLE_FILE_INITIAL_SIZE (16 MiB), which yields 256 fixed 64-KiB allocation-map entries. SparseFile::extend_to exists only as dead low-level code and is not integrated with CoW roots, allocation maps, publication, or recovery.

## Deferred From (Optional)

docs/tasks/000269-single-table-checkpoint-benchmark.md; docs/rfcs/0028-composable-doradb-bench-phase-framework.md phase 4

## Deferral Context (Optional)

- Defer Reason: Task 000269 is intentionally benchmark-layer only and explicitly excludes storage checkpoint algorithms, persisted formats, recovery, and I/O behavior. Table-file growth crosses those boundaries and requires separate design and validation.
- Findings: The 16 MiB constant currently acts as both initial size and effective maximum. ActiveRoot creates an AllocMap sized from the initial page count; allocation and allocation-map rebuild never increase its length. A reduced diagnostic run with the same one-million-row load and max_rows 50,000 froze 50,176 rows across 112 pages and checkpointed successfully, while max_rows 500,000 exhausted the map. The 2 GiB data-buffer max_file_size controls data.swp and does not size durable table files. Existing template inventory coverage parses plans but does not execute the full checked-in checkpoint template.
- Direction Hint: Treat this as a storage design problem, likely requiring RFC-level planning. Prefer atomic capacity growth coordinated with CoW root publication and persisted allocation-map evolution, with explicit crash ordering and recovery invariants. Do not address it only by raising TABLE_FILE_INITIAL_SIZE or by changing benchmark values; retain a runnable large checkpoint case as acceptance coverage.

## Scope Hint

Design and implement table-file growth across physical sparse-file sizing, mutable and persisted CoW allocation-map expansion, publication ordering, retained-root and reclamation behavior, restart validation, configuration or growth limits, observability, and typed capacity or I/O failures. Explicitly decide whether the shared CowFile mechanism and catalog multi-table files should gain the same capability.

## Acceptance Hint

A public table checkpoint or index publication that needs blocks beyond the current table-file capacity expands safely and completes; restart recovers the expanded root and capacity; old and retained roots remain valid; failed expansion or publication leaves the previous root authoritative; configured limits still produce a typed capacity error; deterministic tests cover boundary growth, repeated growth, failure injection, reclamation, and recovery.

## Notes (Optional)


