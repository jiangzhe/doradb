# Backlog: Coverage Focus Threshold and Delta Gating

## Summary

Extend `tools/coverage_focus.rs` with optional threshold/delta gating to support CI-like coverage enforcement workflows.

## Reference

1. Source task document: `docs/tasks/000036-coverage-focus-tool-for-file-directory.md`.
2. Open question: threshold or baseline-compare gating.

## Scope Hint

- Add optional `--min-line` and/or baseline compare inputs.
- Keep default mode report-only.
- Provide deterministic exit-code behavior for gating mode.

## Acceptance Hint

Coverage focus tool supports optional gating with clear CLI semantics and tests.

