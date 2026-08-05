---
id: 000256
title: Contain Component Shutdown Panics
status: implemented  # proposal | implemented | superseded
created: 2026-08-05
github_issue: 942
---

# Task: Contain Component Shutdown Panics

## Summary

Component shutdown now contains each catchable hook panic independently,
continues exact reverse-order teardown, and makes the engine terminal before
propagating the first original payload. Repeated shutdown is a no-op, and owner
drop during an existing unwind suppresses the retained payload instead of
causing a double-panic abort.

Purge, mandatory-runtime, and redo worker owners complete their safe terminal
work before exposing a captured join panic. If a hook panics, registry owner
release avoids an indefinite quiescent wait by leaking only the suspect owner
and owners still pinned by its guard closure; independent owners continue to be
reclaimed.

This is a narrow panic-contained shutdown contract. It does not make arbitrary
redo, purge, eviction, buffer, or I/O mutation bodies unwind-safe, and an engine
that encountered a shutdown panic cannot be recovered or reused.

## Context

`Issue Labels:`
`- type:task`
`- priority:high`
`- codex`

`Source Backlogs:`
`- docs/backlogs/closed/000174-atomic-index-metadata-publication-and-panic-safe-shutdown.md`

`Related RFCs:`
`- docs/rfcs/0026-engine-owned-mandatory-background-runtime.md`

`Related Tasks:`
`- docs/tasks/000212-introduce-observability-logging.md`
`- docs/tasks/000250-runtime-owned-index-ddl.md`
`- docs/tasks/000252-mandatory-runtime-lifecycle-fairness-evolution-readiness.md`
`- docs/tasks/000254-remove-engine-runtime-reference-accounting.md`

Backlog 000174 combined an index-metadata publication race with shutdown panic
containment. Task 000250 completed the publication half; this task completed
the remaining shutdown half.

Before this change, the first panicking component hook aborted registry
dispatch after the registry-wide once flag had been set. Later active hooks
could therefore remain unexecuted, and registry owner drop could block forever
on quiescent guards retained by their workers. Purge and mandatory-runtime
owners also stopped at the first failed join, while redo could skip final
log-file release.

Normal engine shutdown already provides the containment boundary: it closes
foreground and mandatory admissions, drains sessions and accepted work, then
stops redo, mandatory workers, purge, eviction, and shared I/O in dependency
order. That terminal boundary permits registry-level `AssertUnwindSafe`; it is
not a proof that component domain mutations are generally `UnwindSafe`.

The concrete unsupported case remains purge mutation. A local
`Vec<CommittedTrx>` owns boxed row undo while reachable row-version chains hold
non-owning `RowUndoRef` links. An arbitrary mid-mutation unwind can invalidate
that ownership relationship, so this task injects panics only at named-worker
finish after the worker body has returned.

## Goals

1. Invoke every registered shutdown hook at most once and in exact reverse
   registration order, even when earlier hooks panic.
2. Report every hook or worker-join panic while retaining only the first
   original payload for propagation.
3. Publish terminal engine lifecycle state and finish required cleanup before
   propagation, or suppress propagation during an existing unwind.
4. Join all purge and mandatory-runtime workers and release redo resources
   before exposing safe post-stop failures.
5. Preserve live mandatory runners when accepted callers have not drained.
6. Prevent degraded owner release from hanging or reclaiming allocations still
   reachable through quiescent guards.
7. Preserve normal panic-free shutdown, full owner reclamation, component
   order, CTS/STS semantics, and root-lease-last behavior.
8. Keep the complete production component audit and arbitrary-worker-unwind
   limitation in durable lifecycle documentation and adjacent code comments.

## Non-Goals

1. General `UnwindSafe` or `RefUnwindSafe` support for the storage engine.
2. Recovery, restart, or reuse of a component graph after a shutdown panic.
3. Repair of arbitrary mid-body panics in redo, purge, eviction, buffer, or
   kernel I/O state machines.
4. Forced cancellation, worker termination, watchdogs, timeouts, deadlock
   recovery, or process-abort policy.
5. Changes to public shutdown APIs, component registration order, CTS/STS
   semantics, persisted formats, recovery, or transaction visibility.
6. Rework of the index-publication half already completed by task 000250.

## Plan

### Aggregate once-only component shutdown

`ComponentRegistry::shutdown_all` returns a must-use
`ComponentShutdownOutcome`. The existing atomic remains the once-only dispatch
gate. Each erased hook runs through `catch_unwind(AssertUnwindSafe(...))`;
success and panic are logged per component, the exact owner is marked suspect,
and dispatch continues.

`FirstPanic` retains the first original payload. Later payloads are described
without consuming them and then forgotten so an arbitrary payload destructor
cannot start another unwind on the teardown path. A repeated registry shutdown
returns an empty completed outcome and neither reruns hooks nor replays a
payload.

### Terminal engine propagation policy

Explicit shutdown, try-shutdown, and owner drop all receive the aggregate
outcome after idle session state and component dispatch finish. They publish
`EngineLifecycleState::Shutdown`, release the shutdown mutex, and log the final
result before applying the payload policy.

On a non-unwinding thread, the first payload is resumed unchanged. During an
existing unwind it is reported and forgotten. Builder rollback uses the same
policy only after clearing shelf provisions that may retain guards into
registered owners.

### Active worker invariants

- Purge sends `Purge::Stop`, takes the handle vector once, attempts every
  dispatcher/executor join, and then resumes the first payload. Its owner holds
  an explicit transaction-system guard so degraded leakage pins the required
  dependency graph.
- Mandatory-runtime first closes caller admission and checks that accepted
  callers are drained. If callers remain, it reports the invariant and leaves
  internal admission and runners live. Once drained, it closes and drains
  internal admission, signals stop, joins every runner, validates the executor,
  and then resumes the first collected join or terminal-invariant payload.
- Redo closes group-commit admission, queues shutdown, joins the log thread,
  releases the active log file, and only then resumes a captured join payload.
- Shared evictor and filesystem hooks retain their established signal/ingress
  closure before join ordering. Their join payloads are contained by the
  registry only after those terminal actions.
- Startup rollback joins every started worker while preserving the typed
  startup error as the primary diagnostic.

### Guard-aware degraded owner release

Panic-free registry drop remains strict: clear published access handles and
drop owners in reverse order, allowing `QuiescentBox` to wait and expose hidden
guard-lifetime defects.

After any hook panic, the registry clears published access handles, then
samples each owner's quiescent guard count with acquire ordering. A suspect
owner is leaked regardless of count. A non-suspect owner with outstanding
guards is also leaked, allowing retained dependency guards to form a bounded
leak closure. A zero-guard independent owner is dropped normally. Every leak
records component name, reason, and observed count.

The zero sample is valid only after registry handles, engine-core handles, and
builder shelf provisions are gone; no remaining guard exists from which the
count can increase. This protects teardown-owned allocations but cannot restore
memory already released by an arbitrary worker-body unwind.

### Shutdown order and transaction boundary

The reverse production order remains: redo workers, mandatory-runtime workers,
purge workers, transaction system, catalog, lock manager, evictor workers,
filesystem workers, memory/index/metadata/disk pools, filesystem,
mandatory runtime, poisoner, and storage root lease.

Foreground work drains before dispatch. Redo joins before purge stop, mandatory
internal work drains before purge, and `Purge::Stop` is a terminal queue
barrier. `published_gc_horizon` remains scan progress rather than physical
purge; `global_visible_sts` advances only after a complete successful cycle.
After purge joins, no later hook reads transaction purge state. The full
sixteen-component authority and panic audit lives in
`docs/engine-component-lifetime.md`.

## Implementation Notes

Implemented panic-contained component shutdown across the registry, engine
lifecycle, active worker owners, quiescent owner release, observability, and
lifecycle documentation. The first payload now reaches an explicit caller only
after all hooks ran, lifecycle state became terminal, and the shutdown lock was
released; shutdown during an outer panic completes without aborting.

The degraded release implementation records per-owner shutdown failure and a
registry-wide degraded bit. Tests proved that the suspect owner and its
outstanding-guard dependency are leaked while an independent zero-guard owner
is reclaimed. Builder rollback clears shelf-held guards before degraded drop
or payload propagation.

Purge now joins all worker handles and retains a transaction-system dependency.
Redo releases its active log file after a failed join. Mandatory-runtime joins
all runners only after caller admission is confirmed drained, then performs
executor validation before propagation.

Review found that the original proposal would stop mandatory runners even when
accepted callers remained. Commit `a001a71` corrected the implementation and
added regression coverage: active callers now produce the existing invariant
panic without closing internal admission, signalling stop, or taking runner
handles. No unresolved, current PR review threads remained after that fix.

Two documentation details differed from the proposal:

- the CTS/STS ordering and arbitrary purge-unwind limitation were consolidated
  in `docs/engine-component-lifetime.md` and the purge ownership comment rather
  than duplicating them in `docs/transaction-system.md`;
- evictor and filesystem coverage uses the end-to-end engine shutdown test,
  while the hooks retain their existing local signal-before-join behavior.

Verification on PR 943 at current head `a001a71` passed workspace nextest
coverage, default and libaio Clippy, libaio nextest, Codecov project/patch
checks, and the aggregate CI verification job. The resolve-time branch-diff
style gate also passed formatting, Clippy, and repository style checks for all
13 changed Rust files. Follow-up automated review reported no blocking issue.

Source backlog 000174 is fully implemented by tasks 000250 and 000256. No new
bounded deferred work was discovered; the broader arbitrary-worker-body unwind
problem remains an explicit non-goal rather than an underspecified backlog.

## Impacts

- Internal component shutdown now has an aggregate outcome, per-hook
  containment, first-payload retention, and degraded owner-release state.
- Engine lifecycle reporting distinguishes successful and panic-degraded
  shutdown and applies propagation only after terminal publication.
- Purge, mandatory-runtime, redo, eviction, filesystem, passive component, and
  root-lease shutdown authority is documented next to the implementation.
- Quiescent owners expose a narrow acquire-ordered guard-count observation used
  only for terminal degraded release.
- Structured logs identify each hook panic, worker join panic, propagation or
  suppression decision, final engine result, and intentionally leaked owner.
- Lifecycle documentation records the full component audit, CTS/STS shutdown
  boundary, terminal/no-reuse rule, and bounded leak policy.
- No public API, configuration, persisted format, recovery compatibility,
  registration order, or benchmark interface changed.

## Test Cases

Completed coverage includes:

1. Exact reverse hook order and reverse owner release on normal shutdown.
2. Independent containment of multiple hook panics, first-payload preservation,
   secondary-payload forgetting, and once-only repeated shutdown.
3. Payload suppression during an existing unwind without double panic.
4. Degraded release of a suspect owner, retained-guard dependency closure, and
   independent zero-guard owner; shelf guards are cleared before builder drop.
5. Purge and mandatory-runtime multi-worker failure paths attempt every safe
   join and preserve the first payload.
6. Mandatory-runtime leaves internal admission, stop state, and runner handles
   intact while accepted callers remain.
7. Redo releases the active log file before propagating a finish panic.
8. End-to-end purge-finish injection proves redo and mandatory workers finish
   first, purge executors join, evictor and I/O hooks still run, lifecycle is
   terminal, repeated shutdown is inert, and a replacement engine reacquires
   the same storage root.
9. Engine owner drop during an outer panic suppresses the shutdown payload.
10. Evictor and I/O finish injections prove their stop signals precede visible
    join panics.
11. Acquire-ordered quiescent guard-count observation tracks clone and drop.
12. Full workspace and alternate-libaio test and lint jobs pass.

## Open Questions

None for the implemented scope. Arbitrary worker-body unwind support would
require separate domain-specific ownership, retention, quarantine, and
recovery proofs; no concrete bounded follow-up was identified during
implementation or resolution.
