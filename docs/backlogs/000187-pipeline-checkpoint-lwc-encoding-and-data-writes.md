# Backlog: Pipeline checkpoint LWC encoding and data writes

## Summary

Overlap checkpoint LWC CPU encoding with bounded table-file data writes so persistent checkpoints do not serialize two dominant phases or retain every encoded 64 KiB block until encoding finishes.

## Reference

Task 000277 implementation and the 2026-08-22 two-million-row checkpoint profile in target/task-000277-profile-20260822/README.md. Follow-up review traced the current build_lwc_blocks to apply_lwc_blocks phase boundary, CoW allocation and publication rules, storage queue behavior, and column-index ordering constraints.

## Deferred From (Optional)

docs/tasks/000277-introduce-thread-pool-and-parallelize-checkpoint-lwc-encoding.md

## Deferral Context (Optional)

- Defer Reason: Task 000277 deliberately limits its first consumer to owned CPU encoding and lists table-file allocation and storage writes as non-goals. Pipelining changes mutable-file and IO ownership, backpressure, error draining, and benchmark design, so it needs a separate focused task.
- Findings: The current checkpoint drains all encoded blocks into a vector before apply_lwc_blocks allocates IDs and submits every write through try_join_all. The four-worker profile ended its main encode and scan cluster near 181 ms and then spent about 380 ms in apply, write, and publication during an IO-tail sample; persistent storage masks otherwise effective CPU scaling. IO depth one was slower than depths 8 through 64, so ordered write completion must not serialize the pipeline. Early writes are safe as unpublished CoW blocks because visibility still depends on the final root swap and fsync; later checkpoint failures already tolerate unreachable written blocks. A bounded pipeline also reduces retention from all 64 KiB encoded buffers to the encode plus write windows. Review also found that the current final shape can be extended after encoding: set_end_row_id recomputes the index fingerprint after the old fingerprint was embedded and checksummed in the LWC buffer. A dense last block followed by fully deleted trailing pages can therefore mismatch.
- Direction Hint: Prefer a single-owner mandatory-runner state machine such as PendingEncode to Writing to Written, keyed by logical sequence number, without a new mutex or IO inside ThreadPool workers. Preserve logical submission and index order but allow physical IO completion order to vary. Size and benchmark the write window independently from worker count, starting from the configured file IO depth rather than a one-write window. Pass the known checkpoint pivot into LWC production and finalize the last block shape before encoding. Introduce a small owned prepared-write handle if necessary so in-flight writes do not borrow the mutable root. Preserve existing drain and poison precedence and measure phase boundaries before judging the gain.

## Scope Hint

Design a checkpoint-private coordinator on the mandatory runner with logical sequence numbers and separate bounded encode and write windows. Keep ThreadPool jobs CPU-only. Consume encodes in logical RowID order, allocate block IDs and submit writes as encoded buffers become available, permit write completions out of order, retain ordered column-index entries, drain all accepted encodes and writes on every error path, and build the column index only after all LWC data writes succeed. Add phase timing sufficient to distinguish encode, data-write drain, index rebuild, and publication fsync.

## Acceptance Hint

Checkpoint encoding and LWC data writes overlap under deterministic delayed encode and delayed write tests; persisted entries remain ordered and byte-compatible; accepted work drains with correct Fatal precedence after producer, encode, allocation, submission, or IO failures; root publication cannot occur before all writes and index construction succeed; the final dense block plus trailing fully deleted pages has matching LWC and index fingerprints; peak encoded-buffer retention is bounded; and the two-million-row benchmark compares multiple write-window and ThreadPool sizes on fresh reclaimed roots.

## Notes (Optional)

A first implementation should pipeline only LWC data writes. Column-index construction, deletion checkpoint, secondary-index sidecars, allocation-map rebuilding, and root publication should remain after the data-write drain unless separate evidence justifies more overlap. No durable-format change is expected.

