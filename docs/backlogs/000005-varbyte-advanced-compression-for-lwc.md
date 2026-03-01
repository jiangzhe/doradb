# Backlog: VarByte Advanced Compression for LWC

## Summary

Evaluate and implement advanced compression for LWC variable-length byte columns (for example dictionary and/or FSST) beyond the current flat encoding baseline.

## Reference

1. Source task document: `docs/tasks/000001-row-to-lwc-conversion.md`.
2. Deferred note: VarByte uses flat encoding for now; advanced encoding deferred to future work.

## Scope Hint

- Compare candidate encodings for DoraDB workloads.
- Define on-disk format compatibility/versioning requirements.
- Implement encode/decode integration in LWC write/read paths.

## Acceptance Hint

A follow-up task lands with selected VarByte compression strategy, format details, and tests validating correctness and compatibility.

## Notes (Optional)

Coordinate with deletion/checkpoint bitmap and scan-path performance characteristics.
