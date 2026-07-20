# Backlog: Harden domain-specific fault injection for critical workflows

## Summary

Define an owner-scoped policy for domain-specific test fault injection and close critical-workflow coverage gaps so tests verify both failure atomicity and source-domain preservation.

## Reference

Task docs/tasks/000226-establish-runtime-errors-and-harden-foundation-format-boundaries.md and the follow-up review of fault injection on branch runtime-errors. The review traced current create-table, checkpoint, recovery, transaction rollback, redo-retention, and purge failure paths and compared their injected source domains with the reports returned after cleanup or fatal poisoning.

## Deferred From (Optional)

docs/tasks/000226-establish-runtime-errors-and-harden-foundation-format-boundaries.md; docs/tasks/000229-completion-bridge-and-infrastructure-closure.md; docs/rfcs/0023-storage-error-boundary-propagation-migration.md Phase 2

## Deferral Context (Optional)

- Defer Reason: Task 000226 is already focused on fallible Runtime startup and foundation format boundaries. Repairing the broader critical-workflow fault model and source-preserving Fatal conversion paths spans checkpoint, recovery, transaction, retention, and purge modules and needs a dedicated design and validation pass rather than expanding the current task.
- Findings: Runtime spawn injection already uses std::io::Error and preserves IoError beneath RuntimeError. Recovery corruption preserves DataIntegrityError, and DiskTree write injection uses a real IO source, although its cleanup tests do not assert that source. Current create-table, checkpoint sidecar, LWC, silent-watermark, and statement rollback hooks use InternalError::Generic or omit source assertions. The prior B-tree callback-error propagation test was removed when the shared injected Internal marker was retired, leaving that forwarding seam without explicit regression coverage. Table and catalog checkpoint publication, redo retention, statement and system rollback, and purge contain Err discard, is_err, or map_err discard paths that can replace the initiating report with a Fatal report. Recovery uses monotonic propagation but lacks coverage for backend IO crossing read-ahead transport and failed engine-startup cleanup. Task 000229 established source-bearing completion fan-out, SharedFatalError propagation, and call-site-owned poison logging, but intentionally covered only the representative workflows required for RFC-0023 Phase 2.
- Direction Hint: Prefer real production-shaped sources first: standard IO errors through IoError, malformed persisted bytes through DataIntegrityError, and spawn errors through RuntimeError. When no realistic source can be constructed, add a narrowly named cfg(test) variant to the applicable domain enum while keeping the hook and control state in the owning test module. Do not reintroduce a shared InjectedTestError or a generic injected marker. Reuse the source-bearing CompletionErrorBridge, SharedFatalError, and silent EnginePoisoner contracts established by task 000229. Tests must assert source domain, outer Fatal context when applicable, and state or ownership cleanup.

## Scope Hint

Establish a fault-model matrix for critical multi-stage workflows. Audit user-table and catalog checkpoint publication, redo-retention marker publication, recovery read-ahead IO transport and failed-startup cleanup, staged DDL rollback, statement and system rollback, purge failure handling, B-tree callback forwarding, and DiskTree rewrite cleanup. Use real same-domain failures where possible and narrowly named cfg(test) variants in the applicable domain enum otherwise. Keep hooks in owner test modules, preserve initiating source frames when stacking Fatal contexts, and do not change persisted formats or public APIs.

## Acceptance Hint

No critical fault test uses InternalError::Generic solely as an injection value. Each audited stage uses a real same-domain error or a narrowly named cfg(test) domain variant, asserts the initiating source and final context, and verifies cleanup or failure atomicity. B-tree scanning explicitly verifies callback-error forwarding, and DiskTree rewrite failures assert that the injected IO source survives allocation rollback. Recovery covers backend IO across read-ahead transport and startup teardown. Returned Fatal reports retain the initiating source, stored poison behavior is explicit, docs/error-spec.md is synchronized, and default plus libaio validation passes.

## Notes (Optional)

Build the future task from a stage-by-stage matrix containing workflow phase, realistic source domain, injection seam, expected returned context, expected retained source frames, poison state, and cleanup ownership. Task 000226 accepted documentation-only deferral for these gaps and left narrow `TODO(error-boundary)` markers at the affected seams; keep its completed implementation focused on Runtime startup and foundation format boundaries.

## Close Reason (Added When Closed)

When a backlog item is moved to `docs/backlogs/closed/`, append:

```md
## Close Reason

- Type: <implemented|stale|replaced|duplicate|wontfix|already-implemented|other>
- Detail: <reason detail>
- Closed By: <backlog close>
- Reference: <task/issue/pr reference>
- Closed At: <YYYY-MM-DD>
```
