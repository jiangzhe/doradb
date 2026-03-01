# Backlog: Deletion Checkpoint Marker Selection Parallelization

## Summary

Parallelize committed-marker selection and sort/dedup stages for very large deletion buffers in deletion checkpoint flow.

## Reference

1. Source task document: `docs/tasks/000039-unify-new-data-deletion-checkpoint-table-persistence.md`.
2. Open question: marker selection parallelization.

## Scope Hint

- Identify scalable partitioning model for marker collection.
- Preserve deterministic cutoff and dedup semantics.
- Benchmark throughput gains vs coordination overhead.

## Acceptance Hint

Deletion marker selection path is parallelized with correctness guarantees and performance evidence.

