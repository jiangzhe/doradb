# Backlog: Public cancellation for shared read snapshots

## Summary

Add an explicit caller-driven cancellation API for shared read snapshots and their partition streams, distinct from normal close and first-error failure propagation.

## Reference

docs/rfcs/0030-shared-read-snapshots-parallel-table-scan.md; docs/tasks/000284-parallel-row-oriented-table-scan.md

## Deferred From (Optional)

docs/rfcs/0030-shared-read-snapshots-parallel-table-scan.md Phase 4

## Deferral Context (Optional)

- Defer Reason: RFC-0030 deliberately limited snapshot-wide stopping to terminal partition errors so it could prove scan correctness, spawnability, and cleanup without introducing a general query-execution cancellation contract.
- Findings: The frozen snapshot already contains first-error execution control and partition streams already check it before and after physical-unit loads and after unit exhaustion. Explicit close seals new admission but intentionally allows accepted streams to drain, so user cancellation needs a distinct reason and observable result instead of overloading close or SnapshotScanAborted.
- Direction Hint: Reuse the registry-owned execution control and exact drain transition, but keep normal close, execution failure, and user cancellation semantically distinct. Preserve the no-per-returned-row check, checkout-last destruction, and registry-authoritative cleanup contracts; explicitly decide whether in-flight I/O is only observed after await or can be woken or preempted.

## Scope Hint

Design a public cancellation capability for ReadSnapshot that seals planning and open admission, communicates a distinct cancellation reason through the frozen execution control, stops accepted partition streams at documented safe boundaries, and preserves registry-owned terminal cleanup. Decide whether cancellation remains cooperative at physical-unit boundaries or gains wake/preemption support for storage waits.

## Acceptance Hint

Cancellation linearizes against plan publication, partition open, close, first failure, session abandonment, and shutdown; no new work publishes after cancellation; accepted streams return a documented cancellation result and release all checkouts; normal close remains a drain rather than cancellation; tests cover unloaded, loaded, and in-flight-I/O streams without scheduler sleeps on both I/O backends.

## Notes (Optional)


