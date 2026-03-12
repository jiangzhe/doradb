# Backlog: Storage Path Configuration Audit

## Summary

Audit all storage file path configuration points to ensure they consistently derive from the main storage directory and avoid hidden hardcoded paths.

## Reference

1. Source task document: `docs/tasks/000006-main-storage-directory.md`.
2. Open question: whether any file path configuration sites were missed.

## Scope Hint

- Search for file-path literals and extension-based patterns.
- Verify runtime paths in engine, file, trx, and buffer subsystems.
- Add tests/assertions to prevent path drift.

## Acceptance Hint

All relevant path sites are documented and validated; remaining hardcoded path usage is either removed or explicitly justified.

## Close Reason

- Type: implemented
- Detail: Implemented via docs/tasks/000063-refine-storage-path-configuration-and-restart-contract.md
- Closed By: backlog close
- Reference: User decision
- Closed At: 2026-03-12
