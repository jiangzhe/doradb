# Backlog: Improve libaio Stub Struct Design

## Summary

Refactor no-`libaio` ABI stubs so stub structs can be simplified after fallback I/O execution is decoupled from direct `iocb` field decoding.

## Reference

1. Source task document: `docs/tasks/000041-enforce-clippy-lint-and-fix-existing-issues.md`.
2. Deferred notes in code:
   - `doradb-storage/src/io/mod.rs`
   - `doradb-storage/src/io/libaio_abi.rs`

## Scope Hint

- Introduce fallback request metadata independent from libaio ABI layout.
- Remove fallback dependency on reading `iocb` fields in no-`libaio` mode.
- Re-evaluate stub ABI structs (`io_context_t`, `iocb`, `io_event`) for minimal design (including empty structs where safe).
- Preserve test parity between default and `--no-default-features` builds.

## Acceptance Hint

No-`libaio` fallback path no longer depends on fieldful `iocb` stubs, and stub struct design is simplified without behavior regressions.

## Notes (Optional)

Coordinate this refactor with AIO tests and feature-gated ABI exports to avoid breaking existing no-`libaio` execution flow.

## Close Reason

- Type: obsolete
- Detail: Task 000083 removed the no-libaio fallback and deleted the fallback-only ABI stubs, so this backlog's premise no longer exists.
- Closed By: backlog close
- Reference: docs/tasks/000083-retire-thread-pool-async-io-fallback-and-no-default-validation.md
- Closed At: 2026-03-21
