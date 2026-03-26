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
while making backend selection an implementation detail of the driver layer.
Two dedicated storage-facing phases then complete engine error handling before
`io_uring` backend delivery: first, storage I/O policy is made explicit so
fatal redo-log and checkpoint I/O shut down the engine while data/index I/O
failures become caller-visible; second, the buffer-pool and engine-facing
access interfaces are cleaned up so checkpoint, recovery, index, table, and
related paths share one clear, unambiguous, result-bearing API surface. That
follow-up phase also folds readonly validated-page access into the canonical
buffer-pool interface instead of keeping overlapping ad hoc helpers. A fifth
phase then adds `io_uring` as an alternate compile-time backend on top of the
completion core and canonical backend naming. A sixth phase switches the
default backend to `io_uring`, completes the remaining post-adoption cleanup,
and adds `docs/async-io.md` as the repository-level async-I/O design
reference.

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

Task `000087` now implements that storage-policy phase, but it also made the
remaining interface debt visible. The repository still mixes infallible and
fallible page-access methods in `BufferPool`, keeps readonly validated-page
access on a separate helper path, and still has engine-level callers across
checkpoint, recovery, index, and table code that straddle old infallible
helpers and newer `Result` paths. Before `io_uring` delivery, the repository
therefore needs one follow-up cleanup phase focused on making the final storage
access APIs clear and unambiguous. [C3], [C8], [C9], [C10], [U6]

After that cleanup, backend delivery and backend-default switching no longer
need to land in the same task wave. The repository can first add an
`io_uring` backend under the existing completion core and canonical backend
naming while keeping `libaio` as the default. A final follow-up phase can then
switch the default backend, complete the remaining backend-selection cleanup,
and add `docs/async-io.md` to explain the complete async-I/O design and how
storage modules consume it. [C1], [C2], [C3], [C4], [U5], [U7]

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
- [C9] `doradb-storage/src/buffer/mod.rs` and
  `doradb-storage/src/buffer/readonly.rs` - `BufferPool` still mixes
  infallible and fallible page-access methods, while readonly validated-page
  access is exposed through a separate helper family.
- [C10] `doradb-storage/src/table/mod.rs`,
  `doradb-storage/src/trx/recover.rs`, `doradb-storage/src/trx/purge.rs`,
  `doradb-storage/src/index/block_index.rs`, and
  `doradb-storage/src/index/btree.rs` - engine-level callers still mix explicit
  `Result` propagation with older infallible helpers after the phase-3 policy
  work.

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
- [U6] Add a follow-up phase immediately after the storage-I/O policy task to
  remove infallible buffer-pool access methods, complete engine-level error
  propagation across checkpoint/recovery/index/table paths, and merge readonly
  validated-page access into the canonical buffer-pool interface so the final
  APIs are clear and unambiguous.
- [U7] Split the original `io_uring` delivery endgame into two phases: phase 5
  adds the backend only, while phase 6 switches the default backend, performs
  follow-up cleanup, and adds `docs/async-io.md`.

### Source Backlogs (Optional)

- [B1] `docs/backlogs/closed/000003-improve-libaio-stub-struct-design.md` -
  archived
  backlog assumes the no-`libaio` fallback remains and should be reevaluated
  once the fallback is retired.

## Decision

We will use a staged transition with six decisions. [D1], [D2], [D3], [D4],
[D6], [C1], [C4], [C8], [C9], [C10], [U1], [U2], [U3], [U4], [U5], [U6],
[U7]

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

4. Introduce a dedicated storage-I/O error-handling phase before interface
   cleanup and `io_uring` delivery. This phase establishes the engine-level
   policy: redo-log and checkpoint I/O failures are fatal and must transition
   the engine toward shutdown, while supported data/index I/O failures become
   caller-visible through explicit `Result` paths instead of panicking or
   remaining hidden in lower layers. This is the phase implemented by task
   `000087`. [D4], [D5], [D6], [C2], [C3], [C4], [C8], [U4]

5. Immediately after that, add `io_uring` as an alternate compile-time
   backend before making it the default. This phase implements the new backend
   on top of the completion core, introduces canonical backend naming and
   selection seams for production startup paths, and keeps `libaio` as the
   default while the new backend is validated in isolation. [D3], [D4], [D5],
   [C1], [C2], [C3], [C4], [U3], [U5], [U7]

6. Use a final follow-up phase to switch the default backend to `io_uring`,
   complete remaining backend-selection cleanup, and add `docs/async-io.md` as
   the repository-level async-I/O design reference. That phase establishes the
   default-mode support policy, documents the completion object, state-machine,
   backend, and storage-module integration model, and proves the initial
   correctness/completion behavior with a performance floor that is not worse
   than `libaio`. [D3], [D4], [D5], [D6], [C1], [C2], [C3], [C4], [C8], [C9],
   [C10], [U5], [U7]

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
  - Task Doc: `docs/tasks/000083-retire-thread-pool-async-io-fallback-and-no-default-validation.md`
  - Task Issue: `#462`
  - Phase Status: done
  - Implementation Summary: 1. Simplified `doradb-storage/src/io/` to the single supported backend: [Task Resolve Sync: docs/tasks/000083-retire-thread-pool-async-io-fallback-and-no-default-validation.md @ 2026-03-21]
  - Related Backlogs:
    - `docs/backlogs/closed/000003-improve-libaio-stub-struct-design.md`

- **Phase 2: Introduce Backend-Neutral Completion Core**
  - Scope: refactor `crate::io` so generic request/completion flow is backend
    neutral, migrate file and buffer-pool consumers to the new driver
    boundary, and keep `libaio` plus temporary redo-log compatibility helpers
    working on top of the new core.
  - Goals: remove `iocb` from the generic submission interface, preserve
    explicit ownership/completion semantics, make backend-specific ABI code an
    internal driver concern, and land the file/buffer migration needed for
    later redo and `io_uring` follow-up work.
  - Non-goals: exposing `io_uring` yet; changing the higher-level storage
    semantics of file, buffer, or redo-log consumers; finishing redo-log
    migration in this phase.
  - Task Doc: `docs/tasks/000085-introduce-iocompletion-based-async-io-framework.md`
  - Task Issue: `#467`
  - Phase Status: done
  - Implementation Summary: Backend-neutral completion core landed for io, file, and buffer-pool paths; redo-log migration remains tracked in docs/backlogs/000067-redo-group-io-core.md [Task Resolve Sync: docs/tasks/000085-introduce-iocompletion-based-async-io-framework.md @ 2026-03-23]
  - Related Backlogs:
    - `docs/backlogs/000067-redo-group-io-core.md`

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
  - Task Doc: `docs/tasks/000087-unify-storage-io-error-handling.md`
  - Task Issue: `#471`
  - Phase Status: done
  - Implementation Summary: 1. Implemented one explicit storage-poison runtime path for fatal persistence [Task Resolve Sync: docs/tasks/000087-unify-storage-io-error-handling.md @ 2026-03-23]

- **Phase 4: Cleanup Buffer-Pool Interface And Complete Engine Error Propagation**
  - Scope: remove infallible buffer-pool page-access methods, finish
    engine-level storage error propagation across checkpoint, recovery, index,
    table, purge, and related callers, and merge readonly validated-page
    access into the canonical trait-level buffer-pool interface.
  - Goals: end with one clear and unambiguous page-access API surface; make
    fallible storage access explicit at engine boundaries; avoid overlapping
    infallible, fallible, and readonly-specific access families for equivalent
    operations.
  - Non-goals: retry policy, degraded read-only behavior, `io_uring` delivery,
    or unrelated MVCC/storage-format redesign.
  - Task Doc: `docs/tasks/000088-cleanup-buffer-pool-interface-and-engine-error-propagation.md`
  - Task Issue: `#473`
  - Phase Status: done
  - Implementation Summary: Canonical buffer-pool read interfaces are now result-bearing, readonly validated reads moved into ReadonlyBufferPoolExt, and checkpoint/recovery/index/table/purge consumers use the unified access contract. [Task Resolve Sync: docs/tasks/000088-cleanup-buffer-pool-interface-and-engine-error-propagation.md @ 2026-03-24]

- **Phase 5: Add `io_uring` Backend**
  - Scope: implement `io_uring` driver support, keep backend selection
    compile-time, and migrate production startup paths onto canonical backend
    naming without switching the default backend yet.
  - Goals: provide a completion-based backend that fits the refactored generic
    model, keep the first backend landing reviewable on its own, and make
    non-default `io_uring` validation possible before any support-policy
    change.
  - Non-goals: switching `io_uring` to the default mode, runtime backend
    switching, reintroducing thread-pool fallback, or proving the final
    performance floor required for default adoption.
  - Task Doc: `docs/tasks/000090-add-io-uring-backend.md`
  - Task Issue: `#478`
  - Phase Status: done
  - Implementation Summary: Added the io_uring backend, canonical StorageBackend naming, backend-neutral test hooks, and alternate-backend validation while keeping libaio as the default backend. [Task Resolve Sync: docs/tasks/000090-add-io-uring-backend.md @ 2026-03-25]

- **Phase 6: Make `io_uring` Default, Cleanup, And Document Async IO**
  - Scope: switch the default compile-time backend to `io_uring`, complete the
    remaining backend-selection and naming cleanup that should wait until after
    backend delivery, and add `docs/async-io.md` as the overall async-I/O
    design document.
  - Goals: establish `io_uring` as the default engine I/O mode, document the
    completion/state-machine/backend framework plus its table/index/log/
    checkpoint/recovery usage, and prove an initial performance floor that is
    not worse than `libaio`.
  - Non-goals: runtime backend switching, removing `libaio` entirely, or broad
    post-adoption tuning beyond the default-switch acceptance bar.
  - Task Doc: `docs/tasks/000091-make-io-uring-default-and-finish-async-io-cleanup.md`
  - Task Issue: `#480`
  - Phase Status: done
  - Implementation Summary: Switched the default backend to io_uring, completed the remaining async-I/O cleanup/docs, and deferred readonly benchmark/cold-read perf follow-up to docs/backlogs/000070-correct-readonly-buffer-pool-warm-benchmark-and-investigate-iouring-cold-read-latency.md [Task Resolve Sync: docs/tasks/000091-make-io-uring-default-and-finish-async-io-cleanup.md @ 2026-03-26]

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
- Completing data/index I/O propagation now explicitly includes a second cleanup
  phase with interface churn in buffer-pool and engine-facing storage APIs so
  the final access surface is clear and unambiguous. [C3], [C9], [C10], [U4],
  [U6]
- `io_uring` adoption is now intentionally split into backend-delivery and
  later default-switch/documentation phases, so the final support-policy
  change lands later in exchange for a narrower first backend task. [C1], [C2],
  [C3], [C4], [U5], [U7]

## Open Questions

- Should a later RFC retire `libaio` entirely after `io_uring` is proven as the
  default mode, or should both compile-time backends remain supported?
- What exact trait shape and naming should Phase 4 use to absorb readonly
  validated-page access without preserving parallel overlapping buffer-pool
  access families?

## Future Work

- Backend retirement policy after both `libaio` and `io_uring` exist.
- Performance tuning specific to `io_uring` after correctness, completion
  semantics, and the `not worse than libaio` floor are proven.
- Any broader reconsideration of asynchronous API shape above `crate::io`.

## References

- `docs/rfcs/0001-thread-pool-async-direct-io.md`
- `docs/backlogs/closed/000003-improve-libaio-stub-struct-design.md`
- `docs/process/unit-test.md`
- `docs/process/issue-tracking.md`
