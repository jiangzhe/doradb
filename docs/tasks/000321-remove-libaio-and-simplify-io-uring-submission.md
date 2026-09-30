---
id: 000321
title: Remove libaio and simplify the io_uring submission interface
status: implemented
created: 2026-09-30
github_issue: 1127
---

# Task: Remove libaio and simplify the io_uring submission interface

## Summary

Made io_uring the sole storage I/O backend and removed libaio implementation,
ABI bindings, native linkage, backend-selection features, and alternate-backend
CI validation. Profiling remains independently optional and enabled by default;
CI requires validation with profiling enabled and disabled on io_uring.

The private backend interface now stages operations directly into a submission
batch and returns completions without an external event buffer. Submission
owners retain buffers, file references, and page guards through completion or
fatal cleanup; redundant prepared kernel requests no longer occupy inflight
entries. Deterministic backend implementations continue to exercise storage,
redo, and recovery failure paths.

## Context

Issue Labels:

- type:task
- priority:medium
- codex

The design was inspected against `origin/main` at
`482aba0630756a2f4c00d7e94e2163f40c6e44c7`.

Previously the default feature set selected io_uring and profiling, while
libaio provided an explicit alternate. The common backend interface retained
boxed request objects and allocated completion buffers to accommodate libaio;
io_uring copied its requests into queues and needed neither caller-owned
artifact.

The retained abstraction still serves deterministic partial-submission,
completion-ordering, pressure, and fatal-cleanup tests. Coverage previously
required the removed feature names, and CI required alternate libaio jobs, so
the support-policy change included tooling and active documentation.

This standalone task has no parent RFC or source backlog. RFC-0010 and task
000091 remain implemented historical context. Related open backlogs 000072 and
000181 received prospective policy corrections only; neither was closed or
implemented here.

## Goals

1. Use io_uring in default, no-default-feature, and all-feature builds.
2. Remove libaio code, native dependencies, and supported validation paths.
3. Remove backend prepared-object and event-buffer plumbing while retaining
   deterministic backend test implementations.
4. Preserve operation ownership, token validation, queue accounting, retry
   policy, typed failures, fatal cleanup, and durability ordering.
5. Update CI, coverage, support documentation, and unsafe inventory consistently.

## Non-Goals

- Runtime backend selection, fallback I/O, or environments without usable
  io_uring.
- Public Rust interfaces, persisted formats, transaction/recovery algorithms,
  scheduler fairness, or worker topology changes.
- New io_uring capabilities, performance tuning, or benchmark scenarios.
- Profiling removal, statistics API changes, or historical task/RFC rewrites.
- Changes to nextest profiles, timeouts, or an additional backend matrix.

## Rejected Alternatives

- Specializing every driver on concrete io_uring types would discard the
  backend boundary used by deterministic failure and ordering tests.
- Removing only libaio and its features would leave redundant prepared requests
  and empty event-buffer parameters without a production purpose.

## Plan

### Build and backend contract

`doradb-storage` depends unconditionally on the existing workspace `io-uring`
version. Its feature set contains only default-enabled `profiling`. Both former
backend feature names are rejected by Cargo. The benchmark's profiling-enabled
storage dependency is unchanged, and dependency resolution required no
`Cargo.lock` change.

`Backend` retains setup, depth, batch creation/submission, and submitted-I/O
cleanup. `stage_operation(batch, token, operation)` infallibly appends a kernel
request without submitting it, completing it, or taking operation ownership.
`wait_at_least(min_nr)` returns token/result pairs directly. There are no
`Prepared` or `Events` associated types.

The io_uring implementation builds the same read, write, fsync, and data-only
fsync SQEs and moves each into the staged FIFO. Pending-SQE tracking, accepted
prefix removal, EINTR retry, EAGAIN/EBUSY pressure, completion decoding,
statistics, and synchronous cancellation retain their existing behavior.

### Submission ownership and failure handling

The direct driver and shared storage worker reserve a token and slot, stage the
operation, and retain the original submission in that slot. Neither owner
retains a second prepared request or an external completion buffer.
Domain-level `PreparedWriteSubmission`, `PreparedSyncSubmission`, and page-I/O
owners remain because they retain real resources and completion obligations.

Completion tokens still validate slot generations. Only accepted prefixes
advance submitted counts; unaccepted suffixes retain staging order. Test hooks
remain at accepted-submission and completion boundaries.

Fatal cleanup immediately releases staged work and settles failed waiters.
Submitted entries remain quarantined until the backend is destroyed. Successful
cleanup permits release; failed cancellation retains memory-bound submissions
and permits memoryless sync entries to drop. Backend-before-quarantine field
order is unchanged. Borrowed-page reservations and guards remain inside the
original submission containers throughout this path.

### Tooling and support policy

CI retains workspace Clippy, reusable test audit, change detection, and
intentional-skip handling. The existing coverage workflow remains the workspace
nextest execution path. A required profiling-disabled job runs storage Clippy
and nextest with the same io_uring backend. The build aggregate rejects failed,
cancelled, or unexpectedly skipped required jobs while allowing intentional
skips when no build changes are detected.

Coverage no longer checks obsolete backend features. It still records resolved
features, evaluates declared cfg predicates, rejects unmodeled overrides, and
checks source provenance. Its cfg fixtures use neutral enabled/disabled names.
Active documents require usable io_uring and describe profiling-disabled
validation for feature-sensitive changes. Historical backend results remain
untouched.

## Implementation Notes

Implemented the io_uring-only policy and direct batch staging while preserving
submission ownership and storage/redo/recovery failure behavior.

Material discoveries and adjustments:

- Review identified that removing the alternate-backend job also removed CI's
  only profiling-disabled storage test pass. Added dedicated storage Clippy and
  nextest validation without default features, required by the build aggregate,
  with the existing CI profile and JUnit artifact handling.
- Documentation review kept top-level design changes conceptual and removed
  obsolete backend descriptions without adding private interface or CI details.
- Recovery streaming contained a fifth mock, `WaitErrorBackend`, beyond the
  four listed in the proposal. It now uses direct staging with its scripted
  wait failure and drain assertions unchanged.
- Removing `FixedSizeBufferFreeList` also made `DirectBuf::truncate` and
  `FreeList::push` unused in production. Removed these helpers; retained shared
  FreeList batch operations and their reuse, capacity, and concurrency tests.
  The table-file test initializes its payload directly in a full-sized buffer.
  Direct-buffer reset coverage still verifies data and padding clearing.
- Cleanup tests now observe submission destruction, including immediate staged
  release and backend-before-submitted-owner destruction. Storage quarantine
  uses separate file owners and weak references to distinguish retained writes
  from released memoryless syncs without production-only test fields.
- Ported the libaio round-trip test to unconditional `StorageBackend`, with a
  full-sector patterned payload, exact completion lengths, and a file owner
  retained inside each submission. Equal-length sparse extension is covered
  alongside growth, non-shrink, and truncation in the existing resize test.

Validation completed in this worktree on 2026-09-30:

| Check | Result |
| --- | --- |
| Workspace build | Passed |
| Workspace nextest | 2,192 passed |
| Storage nextest without default features, CI profile | 2,013 passed |
| Storage all-feature, all-target check | Passed |
| Profiling-disabled strict Clippy | Passed |
| Branch style audit against origin/main | 17 Rust files passed; 233 selected tests, zero contract violations |
| Coverage-tool unit tests | 23 passed |
| Workspace coverage, implementation snapshot | 2,192 passed; 90.14% production-line coverage |
| Focused I/O coverage | 90.40%, 829/917 production lines |
| Cargo metadata | Only default/profiling features; io-uring nonoptional |
| Workflow YAML and shell syntax | Passed; job dependencies and JUnit upload configuration verified |
| CI aggregate branch checks | 13 success, failure, cancellation, invalid-output, and skip cases passed |
| Unsafe inventory and diff whitespace | Refreshed and passed |

Coverage provenance records storage features `default` and `profiling` without
requiring either removed backend name. The focused report was generated at
`target/coverage/io.md`. These measurements precede the final import formatting
and CI/documentation review updates; runtime behavior did not change afterward.
Final resolution reran the branch style gate and retained the successful
profiling-disabled CI-profile test results.

The concrete io_uring backend measured 77.69% (188/242 lines), below the per-file
80% review bar. Uncovered paths include real syscall interruption/pressure,
terminal submit/wait failures, cancellation outcomes, exceptional setup, and
unused internal statistics access. Existing deterministic batch tests verify
accepted-prefix bookkeeping and pressure outcomes; direct-driver, storage, redo,
and recovery mocks verify retry expiry, typed errors, waiter settlement, and
both cleanup dispositions. Those tests establish the consumer contracts but do
not claim to inject every kernel error into the concrete backend.

Semantic review covered the changed tests and related driver, batch, storage,
redo-ordering, and recovery-drain tests. Assertions retain independent payload,
owner-lifetime, token/order, count, and error oracles. Tests use scripted backend
outcomes, completion hooks, and worker joins; no readiness sleeps were added.
Direct-driver and storage-quarantine overlap remains intentional because the
containers and resource owners differ. Backend ABI/opcode/syscall tests and the
optional-note test were deleted with their implementations; portable payload and
resize cases were retained. Removed helper-only tests do not represent live
production contracts.

## Impacts

- Private I/O and worker plumbing is smaller, with no libaio FFI or native
  package requirement. The unsafe inventory drops from 149 to 139 occurrences.
- Public Rust interfaces, profiling fields, persisted formats, durability
  ordering, and worker topology are unchanged.
- Applications requesting `libaio` or `iouring` features must remove those
  feature arguments. Environments without usable io_uring are unsupported.
- CI has one production backend. Default, profiling-disabled, and all-feature
  configurations use it consistently.

## Test Cases

1. Manifest metadata and compilation establish unconditional io_uring and
   independent optional profiling.
2. Real aligned write/read completion preserves full payloads and file ownership.
3. Sparse resize preserves equal-length idempotence, sparse growth, non-shrink,
   and explicit truncation.
4. Existing batch/driver tests retain partial acceptance, zero progress,
   EAGAIN/EBUSY pressure, retry expiry, hook dispatch, and buffered completion
   coverage.
5. Cleanup tests distinguish staged release, backend-first destruction,
   unsafe-to-release memory retention, and memoryless sync release.
6. Integration tests retain root-write-before-fsync, failed-sync root stability,
   redo fsync/fdatasync, out-of-order completion, poisoning, waiter settlement,
   and recovery drain contracts.
7. Coverage fixtures and collection work with current feature metadata; CI
   requires profiling-disabled validation and preserves intentional skips.

## Open Questions

No unresolved review issues or newly deferred work. Batched io_uring benchmarking
remains tracked by
[backlog 000072](../backlogs/000072-add-batch-io-backend-efficiency-benchmark-baseline.md)
and is not an acceptance dependency. Waitable lock upgrades remain tracked by
[backlog 000181](../backlogs/000181-waitable-comparable-same-scope-lock-upgrades.md).
