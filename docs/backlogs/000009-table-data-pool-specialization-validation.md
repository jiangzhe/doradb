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
