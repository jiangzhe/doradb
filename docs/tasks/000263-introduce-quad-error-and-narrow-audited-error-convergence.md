---
id: 000263
title: Introduce QuadError and Narrow Audited Error Convergence
status: implemented  # proposal | implemented | superseded
created: 2026-08-07
github_issue: 960
---

# Task: Introduce QuadError and Narrow Audited Error Convergence

## Summary

The storage integration layer previously disclosed several typed error domains
into public `Error` too early, while poisoned admission sometimes replaced a
Fatal report with Lifecycle context. That weakened internal contracts and
misclassified poison at public boundaries.

The shipped change adds crate-private `QuadError`/`QuadResult` for the fixed
Operation, Runtime, Lifecycle, and Fatal integration set, plus the exact
`LifecycleOrFatalError` pair for health-aware admission. Internal bootstrap,
completion, catalog, table, maintenance, and transaction paths now preserve
typed reports until an actual public or external boundary.

Poisoned admission now discloses as Fatal without a Lifecycle frame. Ordinary
shutdown and unavailable session or transaction state remain Lifecycle. Lower
Resource, IO, and DataIntegrity reports enter the common carrier only beneath
a caller-owned Runtime context.

## Context

Issue Labels:

- type:task
- priority:medium
- codex

Source Backlogs:

- docs/backlogs/closed/000178-common-multi-domain-error-carrier.md

The original direct-method audit contained 67 production callables and 228
`.disclose()` calls. It identified valid public convergence owners alongside
avoidable internal owners in engine bootstrap, mandatory completion replay,
catalog plans, maintenance waits, transaction integration, and non-callback
table DML.

The existing pairwise carriers were appropriate for exact two-domain paths,
but integration owners spanning three or four common domains either returned
public `Result` or risked replacing a native report with another context. In
particular, converting `EnginePoisoner::ensure_healthy()` from Fatal to
`LifecycleError::RuntimeUnavailable` lost the fatal classification.

Configuration remains owned by public bootstrap. Resource, IO, and
DataIntegrity remain narrow at their native producers and gain a semantic
Runtime context only where a higher-level operation owns their integration.
The row-mutation callback remains the sole documented internal transport for
an arbitrary caller-produced public error.

RFC 0023 is implemented historical context for the typed-domain and disclosure
model, not a parent program for this task. No active parent RFC was linked.

## Goals

1. Provide a closed four-domain carrier for final internal integration.
2. Provide an exact Lifecycle/Fatal carrier for health-aware admission.
3. Prefer native results, then exact pairwise carriers, then `QuadResult`.
4. Preserve native report frames, attachments, and Fatal bypass semantics.
5. Remove avoidable public-error convergence from the audited internal paths.
6. Keep Config disclosure at public bootstrap and require explicit Runtime
   ownership for lower physical domains.
7. Correct poisoned admission to classify publicly as Fatal while preserving
   Lifecycle classification for ordinary shutdown and invalid lifecycle state.
8. Retain public API signatures and refresh the documented disclosure audit.

## Non-Goals

1. No public `Error`, `ErrorKind`, or `Result<T>` signature change.
2. No Config, Resource, IO, DataIntegrity, Internal, or public `Error` arm in
   `QuadError`.
3. No generic error-set framework, variadic carrier, or three-domain carrier
   family.
4. No removal of useful single-domain aliases or exact pairwise carriers.
5. No parameterization of mandatory completion storage by task-specific error
   types.
6. No redesign of the public row-mutation callback contract.
7. No change to transaction ordering, MVCC, DDL publication, rollback,
   checkpoint, or recovery semantics.
8. No persisted catalog, table, redo, checkpoint, or recovery format change.
9. No expansion of `tools/error_audit.rs` beyond direct `.disclose()` method
   calls.
10. No poison-aware logical-lock or hot-row wait cancellation work.

## Plan

### Carrier architecture

`QuadError` is a crate-private enum with exactly four native report arms:
Operation, Runtime, Lifecycle, and Fatal. It delegates formatting and public
disclosure to the contained report, supports eager and lazy attachments, and
does not appear as an `error-stack` context.

The carrier accepts structural conversions only from its four native report
types, shared Fatal reports, and the existing exact pairwise carriers.
Pairwise conversion moves the native report directly into the matching arm;
it never nests one carrier inside another report.

`LifecycleOrFatalError` provides the exact two-domain contract for admission
and health checks. It follows the same frame-less formatting, attachment, and
disclosure behavior.

Result selection follows this order:

1. one native domain;
2. an exact pairwise carrier;
3. `QuadResult` when three or four common integration domains remain.

Resource, IO, and DataIntegrity have no structural conversion into
`QuadError`. Their semantic owner first changes context to a specific Runtime
operation such as CatalogAccess, TableAccess, Recovery, RedoLogAccess,
CheckpointExecution, FileRootAccess, or TransactionCommit. Fatal always
bypasses Runtime and Lifecycle wrapping.

### Boundary ownership and data flow

Public Engine, Session, Transaction, Statement, stream, and `LazyRow` methods
remain final disclosure owners. External trait adapters and carrier disclosure
implementations also remain valid convergence points.

Public `Engine::bootstrap` owns startup-wide Config disclosure. A validated
transaction configuration wrapper resolves and stores the normalized
configuration and redo prefix before typed transaction-system construction.
Transaction bootstrap and component build then return Runtime-only results.

Health-aware Engine, Session, Transaction, and mandatory-runtime admission use
Lifecycle/Fatal results. Poison retains its initiating Fatal report; lifecycle
admission closure, session closure, transaction discard, and shutdown retain
Lifecycle results. Cleanup and inspection paths that must remain available
after poison keep their poison-tolerant contracts.

Mandatory completion cells continue storing `CompletionErrorBridge` so
accepted tasks can be observed safely. `CompletionObserver::wait` returns the
typed completion result rather than disclosing. `into_quad` reconstructs
Operation, Runtime, Lifecycle, or Fatal roots unchanged and places physical
roots beneath a supplied Runtime context. It leaves no bridge or carrier frame.

Completion consumers attach operation, phase, and available request identity
after replay, including table, index, catalog, checkpoint, and redo operations.
Transaction commit can propagate the reconstructed Quad result through typed
internal layers before its public boundary discloses it.

Catalog index plan construction returns Operation/Runtime: invalid requests
remain Operation, while invalid root shape retains its DataIntegrity source
beneath CatalogAccess. Non-callback point DML narrows to native, pairwise, or
Quad results. The three callback mutation helpers retain public `Result` only
to forward arbitrary callback errors.

User transaction commit returns Quad internally. Precommit resource rejection
is retained beneath `RuntimeError::TransactionCommit`; shutdown remains
Lifecycle and poison, redo, rollback-cleanup, or mandatory panic remains Fatal.
System and catalog commit paths retain their narrower Runtime/Fatal contracts.

### Correctness invariants

- A public-error round trip never occurs during typed internal propagation.
- A carrier is never installed as a report context.
- Existing Runtime roots survive completion replay without replacement.
- Physical completion roots retain their source frames under the caller-owned
  fallback Runtime context.
- Poison reports never gain an outer Runtime or Lifecycle frame.
- Every final write-path disclosure carries the same operation and table
  identity context as its sibling read path.
- Maintenance poison and shutdown reports identify the observed boundary and
  target timestamp.
- A fifth Quad arm requires new design work rather than routine extension.

## Implementation Notes

Implemented the complete bottom-up migration and retained the fixed four-arm
design. `QuadError`, `LifecycleOrFatalError`, their result aliases, flattening
conversions, attachment support, and frame-less disclosure are now the common
typed integration infrastructure.

Engine bootstrap now performs Config-owned validation at the public boundary
and passes `ValidatedTrxSysConfig` into Runtime-typed transaction bootstrap.
The private public-result bootstrap coordinator was removed without changing
component registration, rollback, storage-marker, or shutdown order.

Poisoned admission was corrected across Engine, Session, Transaction, and
mandatory submission. `LifecycleError::RuntimeUnavailable` was removed after
its producer audit found no remaining semantic use. Maintenance waits preserve
the Fatal report and attach boundary name plus target timestamp at both health
checks around listener registration.

Completion observation remains layered deliberately: storage uses
`CompletionErrorBridge`, observers return `CompletionResult`, and integration
owners replay with `into_quad` or the narrower Runtime/Fatal conversion. A
review proposal to remove `into_quad` was rejected because transaction commit
must propagate a typed reconstructed result before public disclosure, and
physical completion roots still need an explicit Runtime owner.

All ten public Session completion-bridge consumers attach operation and phase
context after replay. Review also added matching operation and `table_id`
attachments to insert, upsert, update, and delete write-path disclosure, and
added boundary context to both maintenance poison race checks. Mandatory
admission intentionally ignores the poison listener's value because that
listener has no meaningful result; the published poison report remains the
authoritative error.

Catalog plans, non-callback table DML, transaction commit, file-root access,
and recovery integration now use the narrowest verified result type. The
public transaction precommit resource classification intentionally changes
from Resource to Runtime while retaining the Resource source report.

The refreshed direct-method audit contains 54 callables and 197 disclose calls.
Removed rows include the private bootstrap coordinator, catalog plan builders,
maintenance wait helper, transaction-system bootstrap/build/commit helpers,
and non-callback table-access helpers. Remaining internal rows are constrained
carrier infrastructure or the documented callback transport exception.

Documentation now records carrier selection order, fixed Quad membership,
physical-domain Runtime ownership, Fatal bypass, bootstrap Config ownership,
and public-error boundary rules. No deferred follow-up or plan deviation
requires a new backlog.

Final validation passed after all review fixes:

- formatting, diff checks, and the 15-file branch style audit;
- workspace build and clippy with warnings denied;
- 1,706 workspace nextest tests;
- alternate `libaio` clippy with warnings denied and 1,596 nextest tests;
- focused error, completion, admission, session, statement, transaction,
  catalog, table, and bootstrap coverage.

## Impacts

| Area | Shipped effect |
| --- | --- |
| Public API | No signature or `ErrorKind` enum change |
| Classification | Poisoned admission is Fatal; owned lower-domain integration is Runtime |
| Error carriers | Added fixed Quad and exact Lifecycle/Fatal carriers |
| Engine | Public bootstrap owns Config and startup convergence |
| Mandatory runtime | Submission is Lifecycle/Fatal; observation remains typed |
| Session and DDL | Public methods disclose typed completion and plan results |
| Table DML | Non-callback helpers use native, pairwise, or Quad results |
| Transaction commit | User integration is Quad; system paths remain Runtime/Fatal |
| Audit | Direct-method inventory reduced and callback exception documented |
| Persisted data | No representation, schema, or compatibility change |
| Performance | Enum matching and report moves only; no intended I/O or scheduling change |

## Test Cases

1. Native Quad arms disclose to the matching public kind and retain their
   native report, formatting, and attachments without a Quad frame.
2. Pairwise carriers flatten into Quad without nested carrier contexts.
3. Completion replay preserves common outer domains, stacks physical roots
   beneath the supplied Runtime context, and leaves no bridge frame.
4. Post-replay operation and request attachments survive every Quad arm and
   the physical fallback path.
5. Poison before or during admission remains Fatal with no Lifecycle frame;
   shutdown and invalid lifecycle state remain Lifecycle.
6. Maintenance waits report both supported boundary names and target
   timestamps while retaining Fatal identity.
7. User commit resource rejection is Runtime/TransactionCommit with the
   Resource source retained; shutdown and fatal commit paths keep their domains.
8. CREATE/DROP INDEX invalid requests remain Operation, while invalid root
   shapes are Runtime/CatalogAccess with DataIntegrity retained.
9. Point insert, upsert, update, and delete preserve existing MVCC behavior and
   attach operation plus table identity before public disclosure.
10. Callback mutation forwards caller-produced public errors unchanged while
    typed helpers below it avoid public convergence.
11. Invalid transaction configuration remains Config, and recovery physical
    failures retain their source beneath Runtime recovery context.
12. Startup failure atomicity, component ordering, completion behavior,
    transaction cleanup, lock release, and retry coverage remain passing.
13. The generated public-error audit matches the checked-in inventory and has
    no disallowed internal convergence owners.

## Open Questions

None. A fifth common integration domain or a redesign of public callback error
transport requires separate design work rather than widening this carrier.
