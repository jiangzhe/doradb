# Backlog: Table Data-Pool Specialization Validation

## Summary

Validate architecture and performance implications of specializing table row-page operations to `EvictableBufferPool` instead of generic `BufferPool` parameterization.

## Reference

1. Source task document: `docs/tasks/000011-refactor-table-api.md`.
2. Open questions: fixed-pool edge scenarios and potential performance side effects.

## Scope Hint

- Confirm there are no legitimate runtime paths requiring `FixedBufferPool` for row data.
- Benchmark or reason about generic-vs-specialized call overhead impact.
- Document final API boundary rationale.

## Acceptance Hint

Follow-up decision is documented with evidence; either specialization is confirmed stable or specific compatibility/perf fixes are defined.

## Close Reason

- Type: stale
- Detail: The original generic-versus-specialized row data pool validation question is stale. Current architecture has explicit pool roles: metadata uses FixedBufferPool, user row-store and in-memory indexes use EvictableBufferPool through MemPool and IndexPool, and persisted column-store reads use ReadonlyBufferPool through DiskPool. Catalog tables still use fixed pools for metadata-scoped catalog runtime, but that is not a user row-data pool alternative requiring a generic comparison.
- Closed By: backlog close
- Reference: Verified against current buffer pool component mapping, Table and MemTable runtime types, catalog runtime, BlockIndex and RowPageIndex usage, and docs/buffer-pool.md on 2026-06-11.
- Closed At: 2026-06-11
