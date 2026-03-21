---
id: 0010
title: Retire Thread-Pool Async IO and Introduce Backend-Neutral Completion Core
status: proposal
tags: [io, async, libaio, io_uring]
created: 2026-03-21
github_issue: 460
---

# RFC-0010: Retire Thread-Pool Async IO and Introduce Backend-Neutral Completion Core

## Summary

This RFC retires the no-`libaio` thread-pool async-I/O fallback and the
dual-pass validation/coverage contract that exists to support it, then
refactors `crate::io` around a backend-neutral completion core so a future
`io_uring` backend can be added without keeping `libaio` ABI details in the
generic I/O interface. The short-term supported path becomes `libaio` only; the
medium-term goal is to preserve the current completion-driven ownership model
while making backend selection an implementation detail of the driver layer. A
dedicated phase then unifies storage I/O error handling so fatal redo-log I/O
shuts down the engine, while data/index I/O errors propagate back to clients
through result-bearing interfaces. In the final delivery phase, `io_uring`
becomes the default compile-time-selected I/O mode, with first adoption focused
on correctness and completion semantics plus a performance floor that is not
worse than `libaio`.

## Context

The existing thread-pool path was introduced as a short-term workaround so code
agents could execute storage tasks in cloud environments without `libaio`.
That constraint is no longer primary: local execution with `git worktree`
provides better developer-agent collaboration, and production would never use
the thread-pool backend. [D3], [U1]

The current validation and coverage workflow still treats the no-`libaio` path
as a first-class branch: process docs require dual-pass test coverage, CI runs
`cargo nextest` both with and without default features, and the local coverage
tool merges artifacts from both branches. This keeps a temporary portability
path in the repository's quality gate and increases build/CI cost without
benefiting the main production backend. [D2], [C5], [C6], [U2]

At the same time, the current generic I/O API is not actually backend-neutral.
`AIO<T>` and `UnsafeAIO` own `iocb`, `IOQueue` stores raw `iocb` pointers, and
listeners build submission objects around libaio ABI details before they enter
the queue. Even if the thread-pool path were removed immediately, that would
still leave the generic layer centered on `libaio`. A clean `io_uring`
integration therefore needs more than fallback deletion; it needs a backend-
neutral completion core. [C1], [C2], [C3], [C4]

Current storage I/O error handling is also inconsistent. File I/O already has a
path to return errors through `FileIOResult`, but buffer-pool page I/O and
redo-log I/O still treat async-I/O errors as unimplemented fatal branches. The
repository therefore needs an explicit storage-engine policy before a second
backend is introduced: redo-log I/O errors are fatal to engine liveness, while
data/index I/O errors must become observable by callers through proper
`Result`-based interfaces. [D4], [D5], [D6], [C2], [C3], [C4], [C8], [U4]

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:epic`
`- priority:medium`
`- codex`

## Design Inputs

### Documents

- [D1] `docs/process/issue-tracking.md` - tasks are for narrow changes; RFCs
  are for large architectural changes and new subsystems.
- [D2] `docs/process/unit-test.md` - current process requires validation with
  and without `libaio` and describes the dual-pass coverage behavior.
- [D3] `docs/rfcs/0001-thread-pool-async-direct-io.md` - current fallback
  contract, safety rationale for completion-driven ownership, and original
  positioning of `io_uring` as future work.
- [D4] `docs/transaction-system.md` - redo-log durability and recovery depend on
  the current async-I/O path, so backend changes affect commit-log handling.
- [D5] `docs/checkpoint-and-recovery.md` - persistence is No-Steal / No-Force,
  so I/O backend work must preserve completion and recovery semantics.
- [D6] `docs/engine-component-lifetime.md` - fatal log-I/O handling must fit
  the existing admission, shutdown, and owner/runtime teardown model.

### Code References

- [C1] `doradb-storage/src/io/mod.rs` - current `AIOContext`, `AIOEventLoop`,
  `AIO<T>`, `UnsafeAIO`, `IOQueue`, and no-`libaio` thread-pool fallback.
- [C2] `doradb-storage/src/file/mod.rs` - file I/O already has a path to
  surface async-I/O failures through `FileIOResult`, but still couples request
  preparation to `iocb`.
- [C3] `doradb-storage/src/buffer/evict.rs` - buffer-pool page I/O prepares
  `iocb` submissions and still treats async-I/O errors as unimplemented
  branches.
- [C4] `doradb-storage/src/trx/log.rs` - redo-log group commit uses
  `AIOContext::submit_limit()` and `wait_at_least()` directly and still treats
  async-I/O errors as unimplemented fatal branches.
- [C5] `.github/workflows/build.yml` - CI runs dual-pass nextest validation.
- [C6] `tools/coverage_focus.rs` - local coverage regenerates and merges both
  default-feature and no-default-feature artifacts.
- [C7] `doradb-storage/Cargo.toml` - current feature contract keeps `libaio`
  defaulted but still defines a supported no-default build.
- [C8] `doradb-storage/src/engine.rs` - engine shutdown closes admission and
  tears down components in reverse registration order.

### Conversation References

- [U1] The thread-pool backend was a short-term cloud/sandbox workaround, while
  local worktree-based multi-agent workflows are now preferred.
- [U2] Coverage and validation should stop merging multiple feature branches;
  the primary `libaio` path should be the single quality gate.
- [U3] `io_uring` is required, and the interface should move toward a generic
  completion-based design instead of preserving the legacy thread-pool backend.
- [U4] Redo-log I/O errors should shut down the storage engine, while
  data/index I/O errors should propagate to clients, likely requiring
  `Result`-bearing buffer-pool interfaces.
- [U5] `io_uring` should become the default I/O mode once introduced, backend
  selection should remain compile-time, and initial adoption should prioritize
  correctness/completion semantics with performance not worse than `libaio`.

### Source Backlogs (Optional)

- [B1] `docs/backlogs/000003-improve-libaio-stub-struct-design.md` - existing
  backlog assumes the no-`libaio` fallback remains and should be reevaluated
  once the fallback is retired.

## Decision

We will use a staged transition with four decisions. [D1], [D2], [D3], [D4],
[D6], [C1], [C4], [C8], [U1], [U2], [U3], [U4], [U5]

1. Retire the thread-pool async-I/O fallback and the repository-wide
   no-`libaio` validation/coverage contract. Until a new backend is ready,
   `libaio` is the only supported async-I/O backend and the only quality-gated
   validation branch. This aligns the repository with actual production usage
   and removes a temporary support policy from CI and tooling. [D2], [C5],
   [C6], [C7], [U1], [U2]

2. Refactor `crate::io` around a backend-neutral completion core before adding
   `io_uring`. The completion-driven ownership model stays, but backend-neutral
   types must stop exposing raw `iocb` pointers as the generic submission
   format. `AIOKey`, completion callbacks, and explicit in-flight ownership are
   preserved; `IOQueue`, `AIO<T>`, `UnsafeAIO`, and driver state are reshaped so
   backend-specific ABI details live below the generic layer. [D3], [C1], [C2],
   [C3], [C4], [U3]

3. Add `io_uring` as a backend on top of the new completion core instead of
   adapting it directly onto the current libaio-shaped API. This keeps the
   safety model from RFC-0001 while avoiding a second-generation generic API
   that is still centered on libaio submission objects. [D3], [C1], [C2], [C3],
   [U3]

4. Introduce a dedicated storage-I/O error-handling phase before `io_uring`
   delivery. Redo-log I/O failures are fatal and must transition the engine
   toward shutdown because transactional progress depends on durable log I/O.
   Data/index I/O failures must propagate back to callers through explicit
   `Result` paths instead of panicking or remaining hidden in lower layers, and
   this is expected to require buffer-pool and related storage-interface
   changes. `io_uring` delivery then uses compile-time backend selection and
   makes `io_uring` the default mode, with first adoption focused on correctness
   and completion semantics plus a performance floor that is not worse than
   `libaio`. [D4], [D5], [D6], [C2], [C3], [C4], [C8], [U4], [U5]

This RFC supersedes the support-policy assumptions of RFC-0001 without
discarding its safety rationale. We continue to reject a naive `Future`-based
kernel-I/O abstraction because buffer lifetime must remain explicit for
in-flight operations. [D3], [C1]

## Alternatives Considered

### Alternative A: Remove Fallback Only, Defer Core Redesign

- Summary: Remove the thread-pool backend, collapse CI/coverage to `libaio`,
  but keep the current libaio-shaped `crate::io` API and postpone backend
  redesign.
- Analysis: This is the smallest immediate cleanup and would quickly align the
  repo with the new support policy. However, it does not solve the actual
  architectural blocker for `io_uring`, because `AIO<T>`, `UnsafeAIO`,
  `IOQueue`, and listener submission preparation still encode `iocb` semantics
  into the generic layer. [C1], [C2], [C3], [U3]
- Why Not Chosen: It guarantees a second refactor later and keeps the wrong
  abstraction boundary in place precisely when a new backend is desired.
- References: [C1], [C2], [C3], [U3]

### Alternative B: Adapt `io_uring` Onto Current Abstractions After Cleanup

- Summary: Remove the thread-pool path first, then add an `io_uring` driver
  while largely keeping the current `AIOContext` / `IOQueue` / listener shape.
- Analysis: This appears faster than a dedicated generic-core refactor, but it
  would make `io_uring` fit a libaio-shaped API instead of the reverse. The
  result is likely an awkward adapter layer where the generic interface still
  revolves around `iocb`-style submission objects and backend-specific leakage
  remains high. [C1], [C2], [C3], [C4]
- Why Not Chosen: It delivers `io_uring` on top of the wrong boundary and
  increases long-term design debt.
- References: [C1], [C2], [C3], [C4], [U3]

### Alternative C: One-Shot Cleanup, Core Redesign, And `io_uring` Delivery

- Summary: Remove the fallback, redesign the generic completion API, and land
  `io_uring` in a single implementation wave.
- Analysis: This gives the fastest convergence and avoids an intermediate
  libaio-only steady state, but it also moves every major I/O consumer at once:
  table-file I/O, buffer-pool page I/O, and redo-log durability. That makes the
  first delivery harder to review and harder to stage safely. [D4], [D5], [C2],
  [C3], [C4], [U3]
- Why Not Chosen: The combined surface area is too large for the first step and
  creates unnecessary rollout risk on persistence and recovery paths.
- References: [D4], [D5], [C2], [C3], [C4], [U3]

## Unsafe Considerations (If Applicable)

This RFC changes code in an unsafe-sensitive area.

1. Unsafe scope includes backend-specific submission/completion plumbing,
   buffer-pointer lifetime management, and raw page-pointer I/O for
   `UnsafeAIO`-style operations. [C1], [C3], [C4]
2. Phase 1 is expected to remove unsafe-sensitive no-`libaio` fallback code and
   may obsolete the backlog that exists only to simplify no-`libaio` ABI stubs.
   [B1]
3. The explicit in-flight ownership rule remains: buffers and raw pointers must
   stay valid until backend completion is observed and processed by the driver.
   New backend code must keep `// SAFETY:` comments on every unsafe boundary and
   explain the invariants in terms of queue submission, completion ownership,
   and teardown sequencing. [D3], [C1], [C2], [C3], [C4]
4. Validation must refresh unsafe baseline/review artifacts for the affected
   modules and cover the supported backend path only after the no-`libaio`
   contract is retired. Future `io_uring` phases must extend those checks to the
   new backend-specific modules. Fatal log-I/O shutdown wiring must also respect
   the current engine admission and teardown invariants instead of bypassing
   them through ad hoc abort paths. [D6], [C8]

## Implementation Phases

- **Phase 1: Retire Thread-Pool Support And Dual-Pass Validation**
  - Scope: remove the no-`libaio` thread-pool backend, retire no-default CI and
    coverage merge behavior, update docs/tooling to define `libaio` as the only
    supported backend for now.
  - Goals: simplify the support matrix, align validation with production usage,
    and delete temporary workaround code and policy.
  - Non-goals: backend-neutral generic redesign; `io_uring` backend delivery.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`
  - Related Backlogs:
    - `docs/backlogs/000003-improve-libaio-stub-struct-design.md`

- **Phase 2: Introduce Backend-Neutral Completion Core**
  - Scope: refactor `crate::io` so generic request/completion flow is backend
    neutral, migrate file/buffer/log consumers to the new driver boundary, and
    keep `libaio` working on top of the new core.
  - Goals: remove `iocb` from the generic submission interface, preserve
    explicit ownership/completion semantics, and make backend-specific ABI code
    an internal driver concern.
  - Non-goals: exposing `io_uring` yet; changing the higher-level storage
    semantics of file, buffer, or redo-log consumers.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

- **Phase 3: Unify Storage I/O Error Handling**
  - Scope: define and implement storage-engine-wide I/O error policy after the
    generic completion core is in place, including fatal redo-log handling and
    caller-visible data/index error propagation.
  - Goals: shut down the engine on redo-log I/O failure in a way that fits the
    existing engine lifecycle model; propagate data/index I/O failures through
    explicit `Result` paths; remove remaining panic or unimplemented branches
    for supported storage-I/O errors.
  - Non-goals: automatic retry policy, degraded read-only mode, or `io_uring`
    delivery itself.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

- **Phase 4: Add `io_uring` Backend And Make It Default**
  - Scope: implement `io_uring` driver support, keep backend selection
    compile-time, and switch `io_uring` to the default engine I/O mode.
  - Goals: provide a completion-based backend that fits the refactored generic
    model, establish `io_uring` as the default mode, and deliver initial
    correctness/completion behavior with performance not worse than `libaio`.
  - Non-goals: runtime backend switching, reintroducing thread-pool fallback,
    or aggressive `io_uring`-specific tuning beyond the required performance
    floor for first adoption.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

## Consequences

### Positive

- The repository support policy becomes simpler and closer to actual production
  usage. [U1], [U2]
- CI and focused coverage become faster and easier to cache because the
  no-default branch and merge step are removed. [C5], [C6], [U2]
- The generic I/O layer becomes a better foundation for `io_uring` and other
  completion-style backends without preserving thread-pool-specific baggage.
  [D3], [D4], [U3]
- Storage I/O error behavior becomes explicit: fatal redo-log failures trigger
  shutdown semantics, while data/index I/O failures become visible to clients.
  [D4], [D5], [D6], [U4]

### Negative

- The repository no longer supports the old no-`libaio` portability workflow.
  [D2], [U1]
- The generic-core refactor is cross-cutting because redo-log, file, and
  buffer-pool consumers all depend on the current I/O driver. [C2], [C3], [C4]
- RFC-0001's support-policy assumptions must be superseded and related docs or
  backlogs synchronized. [D3], [B1]
- Propagating data/index I/O failures will likely require interface churn in
  buffer-pool and related storage APIs due to new `Result` return paths. [C2],
  [C3], [U4]

## Open Questions

- Should a later RFC retire `libaio` entirely after `io_uring` is proven as the
  default mode, or should both compile-time backends remain supported?
- What exact public/internal error taxonomy should Phase 3 use when converting
  data/index storage paths to `Result`-bearing interfaces?

## Future Work

- Backend retirement policy after both `libaio` and `io_uring` exist.
- Performance tuning specific to `io_uring` after correctness, completion
  semantics, and the `not worse than libaio` floor are proven.
- Any broader reconsideration of asynchronous API shape above `crate::io`.

## References

- `docs/rfcs/0001-thread-pool-async-direct-io.md`
- `docs/backlogs/000003-improve-libaio-stub-struct-design.md`
- `docs/process/unit-test.md`
- `docs/process/issue-tracking.md`
