# Backlog: Shared Eviction Utility Generalization for Policy Tuning

## Summary

Generalize shared eviction utilities to better support future policy tuning (for example scan resistance and admission control).

## Reference

1. Source task document: `docs/tasks/000032-readonly-column-buffer-pool-phase-2-core.md`.
2. Open question: further utility generalization.

## Scope Hint

- Identify abstraction boundaries in current shared evictor code.
- Add extension points without regressing current behavior.
- Keep mutable/readonly pool parity where appropriate.

## Acceptance Hint

Eviction utility interfaces support policy extensions with preserved correctness and existing test coverage.

