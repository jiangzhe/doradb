---
id: 000255
title: Session-Local Runtime Reachability
status: implemented  # proposal | implemented | superseded
created: 2026-08-04
github_issue: 940
---

# Task: Session-Local Runtime Reachability

## Summary

Replaced public session and transaction reachability through engine-wide weak
references with weak reachability to the exact `SessionState`. A successful
foreground admission now upgrades that session-local reference once, retains
the admission until stable operation ownership is registered, and resolves the
operation directly on the pinned state.

Introduced one engine-owned `EngineCore` for shared runtime capabilities and a
strong `SessionRuntime` wrapper around the upgraded state. Operation pins,
observers, transaction attachments, mandatory work, and cleanup handoffs carry
that runtime instead of `EngineRef`. The task implementation removed
`EngineRef` and `WeakEngineRef` from storage source, removed normal
session-registry lookup from statement checkout, and at that revision borrowed
one canonical pool-guard bundle. The later backlog 000175 follow-up replaced
that bundle with one independent root bundle per session after proving the
canonical roots caused cross-session refcount contention.

Lifecycle admission, shutdown authority, exact identity validation, public
APIs, persisted formats, and component teardown order remain unchanged.

## Context

`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Source Backlogs:`
`- docs/backlogs/closed/000175-scalable-shared-resource-lifetime-management.md`

`Related Tasks:`
`- docs/tasks/000247-statement-public-transaction-cancellation-ownership.md`
`- docs/tasks/000254-remove-engine-runtime-reference-accounting.md`

`Benchmark Base:`
`- 2098cbb70316d383881aa3c05ba6ef56db408cc3`

Task 000254 removed custom engine runtime-reference accounting, but ordinary
weak engine upgrades, lifecycle admission, session-registry lookup, and guard
cloning remained on session-coordinated paths. The registry already owned the
exact `SessionState`, and that state owned the stable operation slot used by
shutdown, so a per-session weak reference could remove the global lookup
without weakening the ownership proof.

The original `TrxAttachment` also retained an engine reference, a strong
session state, and a cloned `PoolGuards` bundle. Those capabilities were
redundant once the strong session runtime could reach immutable engine
capabilities through the state.

This was a bounded ownership-path refactor with no parent RFC. Backlog 000175
remained open at task resolution because it covered broader lifecycle
admission, component guard, buffer-page ownership, and shared-counter questions
that this task did not resolve.

## Goals

1. Make public `Session` and `Transaction` handles weakly reference their exact
   `SessionState`.
2. Resolve session operations, observers, inspections, transaction checkout,
   terminal paths, and cleanup directly on the upgraded state.
3. Introduce a reusable `EngineCore` and a strong `SessionRuntime` without a
   strong cycle back to `EngineInner` or `SessionRegistry`.
4. Bind weak-state upgrade to successful lifecycle admission until stable
   ownership is registered.
5. Make pins, attachments, accepted mandatory work, and cleanup jobs retain
   `SessionRuntime`.
6. Centralize typed pool access and one canonical `PoolGuards` bundle.
7. Preserve exact operation-key and transaction-id validation, poison
   behavior, terminal progress, cleanup, and shutdown wakeup rules.
8. Remove engine-wide weak upgrade, registry lookup, and attachment guard
   cloning from transaction checkout.
9. Narrow single-capability interfaces, retaining `EngineCore` only for
   multi-capability work, and measure no-op gains and any regressions.

## Non-Goals

1. The packed `EngineLifecycle` state and admission counter were not redesigned.
2. Operation-start admission and the shutdown-start race it closes remain.
3. General `Arc`, `Weak`, `QuiescentGuard`, and buffer-frame ownership were not
   redesigned.
4. Component registration, worker lifetime, mandatory scheduling, recovery,
   redo, undo, and persisted formats were not changed.
5. Public APIs, storage semantics, benchmark workloads, and CI timing policy
   were not changed.
6. The contended `index-stream` regression was investigated but not fixed in
   this task; backlog 000175 remained open for that and wider lifetime work.

## Plan

The final ownership graph is:

```text
Engine
└── Arc<EngineInner>
    ├── Arc<EngineCore> ─ ─ weak ─ ─> SessionRegistry
    ├── Arc<SessionRegistry> ──> Arc<SessionState> ──> Arc<EngineCore>
    └── Arc<EngineLifecycle>
                                  ▲
SessionState ──> Arc<SessionAdmission> ──> Arc<EngineLifecycle>

Session / Transaction ─ ─ weak ─ ─> SessionState
operation authorities ──> SessionRuntime ──> Arc<SessionState>
```

`EngineInner` is the owner-facing coordination shell. `EngineCore` holds the
immutable component capabilities used by session-coordinated work, including
catalog, transaction system, table filesystem, lock manager, mandatory
runtime, poisoner, and `EnginePools`. Its registry back-reference is weak and
is used only for cold pointer-exact removal after a state becomes closed and
idle.

`WeakSessionRef` combines `Weak<SessionState>` with the session admission
façade. Foreground acquisition follows one protocol:

1. acquire `EngineAdmission` through `WeakSessionRef`;
2. consume the admitted reference to upgrade the exact state;
3. keep admission inside `AdmittedSessionRuntime`;
4. validate health and register the stable operation or observer proof;
5. consume the admitted runtime into `SessionRuntime`, releasing admission
   before callbacks, blocking I/O, or `.await`.

Terminal and cleanup paths use an explicit terminal upgrade without acquiring
new foreground admission, because shutdown must allow accepted ownership to
publish terminal state. Exact `SessionOperationKey` and `TrxID` checks remain
mandatory on both foreground and terminal paths.

`SessionRuntime` is a typed wrapper around `Arc<SessionState>`. Direct state
methods own operation reservation, observer accounting, transaction checkout,
terminal publication, abandonment, and cleanup claims. `SessionRegistry`
remains the strong owner and shutdown traversal structure, but is absent from
normal resolution.

`EnginePools` owns the four typed pool guards and one prebuilt `PoolGuards`
bundle. Session and transaction paths borrow the bundle. Interfaces that need
one capability receive the narrow type—for example staged runtime destruction
receives `&PoolGuards`, recovery resources are constructed from `PoolGuards`,
and owned current-index reads retain only the capabilities they use.
Multi-capability flows may receive `EngineCore`.

`TrxAttachment`, session pins, mandatory guards, and cleanup work retain
`SessionRuntime`. Transaction foreground resolution is consolidated in
`Transaction::checkout`; terminal resolution is named
`checkout_terminal`. Prepared and precommit state derive required capabilities
from the attachment instead of cloning engine-core fields.

State transitions publish under the session lifecycle mutex, release the
mutex, then perform pointer-exact registry removal and notification. This
preserves the listener-before-recheck shutdown protocol and prevents stale
removal from deleting a replacement state.

## Implementation Notes

Implemented session-local runtime reachability across engine, session,
transaction, DDL, catalog, table, recovery, index, log, and buffer-pool
consumers. Production storage source now contains neither `EngineRef` nor
`WeakEngineRef`; no replacement broad test handle was introduced.

The admitted type-state was tightened during review. `WeakSessionRef` first
returns `AdmittedSessionRef`, whose consuming `upgrade` produces
`AdmittedSessionRuntime`. Admission therefore cannot be accidentally separated
from the weak upgrade or leaked as usable runtime authority, and it is consumed
only after stable ownership registration.

Transaction review consolidated active resolution and core checkout into
`Transaction::checkout`, renamed terminal resolution to
`checkout_terminal`, and removed `let mut trx = self` rebinding patterns.
Runtime/core clones were audited: operation scopes borrow or move
`SessionRuntime`, prepared and precommit owners use their attachment, and
checkpoint, table-GC, and shutdown helpers borrow or consume only what their
lifetime requires.

Capability review narrowed several interfaces:

- CREATE TABLE helpers receive `EngineCore` only when they need multiple pools
  or services; staged cleanup receives only `PoolGuards`.
- `RecoveryResources::new` accepts `PoolGuards` instead of `EnginePools`.
- `OwnedCurrentIndexReadHandle` retains only its required index-read
  capabilities.
- Catalog, persistence, checkpoint, GC, logging, and test hooks capture narrow
  guards. Test-only engine forwarding methods were removed; tests use
  owner/core structure, narrow guards, or pre-created sessions.

### Performance Verification

Measurements used optimized builds, `--log-sync none`, fresh equivalent
storage roots, one warmup, and seven alternating samples per revision and
configuration on the same aarch64 Linux environment used for the preceding
lifetime tasks.

| Workload | Baseline median ns | Candidate median ns | Latency delta |
| --- | ---: | ---: | ---: |
| `stmt-noop` 1 thread / 1 session | 73.524 | 47.865 | -34.899% |
| `stmt-noop` 4 threads / 16 sessions | 83.893 | 76.425 | -8.902% |
| `trx-noop` 1 thread / 1 session | 301.035 | 223.491 | -25.759% |
| `trx-noop` 4 threads / 16 sessions | 264.610 | 220.616 | -16.626% |
| unique `index-stream` 1/1 | 233,689 | 240,751 | +3.022% |
| unique `index-stream` 4/16 | 76,799 | 106,144 | +38.210% |
| non-unique `index-stream` 1/1 | 240,770 | 238,612 | -0.896% |
| non-unique `index-stream` 4/16 | 82,875 | 105,518 | +27.322% |

Independent repeated 4/16 index-stream blocks reproduced the unfavorable
result. Paired `cargo flamegraph` profiles localized the added candidate CPU
to existing row-page and buffer-page reference-count operations: relaxed and
release `Arc` atomic helpers accounted for about 29.75% of candidate samples
versus 4.65% of baseline samples. Session-runtime and attachment pool-guard
access accounted for about 0.07%, and the removed engine weak upgrade,
registry lookup, and guard-bundle clone were absent.

The original plan required no repeatable regression in the index-stream rows.
That acceptance rule was deliberately revised after profiling: the cause spans
index, row-page, and buffer-pool behavior and was not shown to be caused by the
session-runtime change. The user accepted deferring that investigation to
backlog 000175 rather than broadening this task without a root cause.

### Verification

- `rtk cargo check -p doradb-storage --tests`: passed.
- `rtk cargo build --workspace`: passed.
- `rtk cargo nextest run --workspace`: 1,646 passed.
- `rtk cargo clippy --workspace --all-targets -- -D warnings`: passed through
  the style gate.
- `rtk cargo nextest run -p doradb-storage --no-default-features --features
  libaio`: 1,553 passed.
- `rtk cargo clippy -p doradb-storage --no-default-features --features libaio
  --all-targets -- -D warnings`: passed.
- `tools/style_audit.rs --diff-base origin/main`: passed for 37 branch-diff
  Rust files.
- Focused line coverage across `engine.rs`, `session.rs`, and `trx/mod.rs` was
  95.84% (9,757/10,180): 97.21%, 95.96%, and 95.14% respectively.
- `rtk git diff --check`: passed.

No parent RFC synchronization was required. Source backlog 000175 remained open
intentionally at task resolution because only its session-coordinated
reachability slice was implemented.

## Impacts

- Session and transaction hot paths now use per-session weak reachability and
  direct state resolution.
- `EngineCore` is the immutable shared capability boundary; `EngineInner`
  retains owner orchestration.
- Session-coordinated runtime owners carry `SessionRuntime`, while
  single-capability helpers use narrow guards.
- At this task revision, pool guards were built once per engine and borrowed by
  session-coordinated work. The backlog follow-up later moved base roots back to
  one bundle per session while retaining the same pool identities.
- Shutdown, terminal cleanup, poison handling, and exact identity error
  classifications remain compatible.
- Public APIs, dependencies, configuration, persisted formats, recovery
  protocols, benchmark CLI, and CI policy are unchanged.
- At task resolution, the contended index-stream finding remained a documented
  performance risk owned by backlog 000175.

## Test Cases

1. Foreground admission either rejects shutdown or upgrades the exact state and
   registers an operation or observer before admission release.
2. Transaction checkout and terminal paths validate the exact operation key
   and transaction id directly on the state.
3. Stale operation, transaction, cleanup, and removal identities cannot affect
   replacement state.
4. Healthy operations reject poison while poison-tolerant inspection remains
   available during open admission.
5. Commit, rollback, abandonment, and cleanup publish terminal state after
   foreground admission closes.
6. Public weak handles surviving shutdown neither retain components nor regain
   usable runtime authority.
7. Explicit close, abandonment, observer release, and terminal publication
   remove only pointer-identical idle state.
8. Accepted DDL, maintenance, precommit, cancellation, and cleanup retain the
   runtime authority needed through their final handoff.
9. Typed pool access and canonical guard provenance remain correct across DDL,
   recovery, table, and index paths.
10. Default and `libaio` suites preserve lifecycle, transaction, DDL,
    persistence, checkpoint, recovery, poison, and storage-root behavior.
11. Structural review confirms no engine-wide weak upgrade, registry lookup,
    attachment pool-guard clone, or separate core clone remains in transaction
    checkout.
12. Paired no-op benchmarks improve, while the separately profiled index-stream
    regression remains recoverably documented.

## Open Questions

No task-scoped design question remains.

At task resolution, backlog
[000175](../backlogs/closed/000175-scalable-shared-resource-lifetime-management.md)
retained the unresolved broader guard-ownership and buffer-page refcount work.
The follow-up reproduced the profile, isolated canonical `PoolGuard` roots as
the cause, restored per-session roots, audited base-root construction, and
closed with measured 1/1 neutrality and recovered 4/16 stream performance.
