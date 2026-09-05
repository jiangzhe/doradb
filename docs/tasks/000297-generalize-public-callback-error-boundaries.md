---
id: 000297
title: Generalize Public Callback Error Boundaries
status: implemented
created: 2026-09-05
github_issue: 1048
---

# Task: Generalize Public Callback Error Boundaries

## Summary

Introduced public `CallbackError<E>` and `CallbackResult<T, E>` to preserve
DoraDB engine failures and application callback failures as separate,
inspectable variants. Managed DDL, sequential mutation, index-range mutation,
and programmable table-scan streams now share this boundary.

Successful statement rollback returns the original application payload intact;
fatal rollback failure takes engine precedence. Either scan error arm closes
the stream through its existing cleanup path. Explicit `Infallible` defaults
support callbacks that can encounter engine failures without application errors.

## Context

Source Backlogs:

- docs/backlogs/closed/000191-generalize-public-callback-error-boundaries.md

Issue Labels:

- type:task
- priority:medium
- codex

This standalone follow-up to
[task 000294](000294-managed-table-bindings-and-versioned-resolution.md)
generalizes the carrier introduced by
[task 000293](000293-opaque-managed-table-definitions-and-proposal-boundary.md).
[RFC 0031](../rfcs/0031-compact-numeric-catalog-table-definitions.md) records
the original deferral and is not this task's parent. Its completed phases and
contracts remain unchanged. The requester approved this coordinated public
API break as one task without a new RFC.

The researched base was `82e222c5750bff948a5113cf8cee277f68711b3c`.
Managed interpretation already preserved engine and interpreter failures,
but programmable row APIs required the engine-only public result.
`LazyRow::val` is itself fallible, so one callback needs both engine conversion
and explicit application wrapping. The benchmark update workload previously
stashed application errors externally and continued invoking callbacks before
rolling back the transaction.

The implementation transports errors through existing transaction, mutation,
and scan ownership paths. It introduces no persistence or recovery machinery.

## Goals

- One engine-or-user public boundary for managed DDL and all three existing
  programmable row APIs.
- Preserve owned or caller-borrowed application payloads and engine reports
  without boxing, cloning, stringification, or downcast classification.
- Support engine `?` propagation and explicit `User` wrapping in one callback,
  including when the application error type is itself `Error`.
- Preserve statement atomicity, fatal precedence, deferred-index undo,
  cancellation cleanup, and stream resource release.
- Keep snapshot partition scans and callback-free operations engine-only.
- Provide compiled inference examples and migrate workspace consumers and
  current API/error documentation.

## Non-Goals

- Compatibility aliases for the managed-specific error/result names or their
  interpreter variant/accessors.
- Public callback-output traits, bare-decision protocols, arbitrary statement
  execution, or new programmable APIs.
- Application variants in `ErrorKind`, blanket lower-domain conversions, or
  convergence of the private transaction runner through public `Error`.
- Changes to MVCC visibility, callback selection/retries, lock modes,
  deferred unique-driver ordering, or persisted formats.
- Panic recovery, rollback of application-owned external side effects,
  test-runner configuration changes, or benchmark performance targets.

## Rejected Alternatives

A caller-owned complete error boundary using `E: From<Error>` would make
engine-versus-user classification optional and impose engine conversions on
managed interpreter errors. That generic mechanism remains private where
needed; public operations expose the distinction explicitly.

A public callback-output trait accepting bare decisions would introduce an
additional protocol and inference rules. The fixed result alias with explicit
`Infallible` annotation supplies the required engine-only callback path.

## Plan

### Shared Carrier And Public Signatures

`error.rs` owns `CallbackError<E = Infallible>` with `Engine(Error)` and
`User(E)`, plus the defaulted `CallbackResult<T, E>` alias. Both are re-exported
at the crate root. Borrowed and consuming accessors expose either arm.
Conditional formatting identifies the arm, and standard `Error::source`
returns the contained error when the payload implements standard `Error`.

`From<Error>` always constructs `Engine`. There is no blanket `From<E>`:
applications wrap their failures explicitly with `CallbackError::User`.
`CallbackError<Infallible>` converts losslessly back into public `Error`.
Neither the carrier nor callback APIs impose payload formatting, standard
error, cloning, thread-safety, or static-lifetime bounds.

Mutation callbacks return `CallbackResult<RowMutation, E>` and operations
return `CallbackResult<TableMutationOutcome, E>`. Programmable scans use
`CallbackResult<ScanRowDecision, E>` callbacks, the same result for construction,
and `CallbackResult<Option<Vec<Val>>, E>` from `next`. Stream state remains
parameterized only by its original transaction lifetime and callback type.

Managed interpreter methods retain plain application results and their
existing invocation timing. Managed operations map interpreter failures to
`User` and engine failures to `Engine`; managed-specific carrier names and
accessors have been removed.

### Settlement And Deferred Ownership

The existing private public-transaction runner now accepts `CE: From<Error>`.
Ordinary operations instantiate `CE = Error`; programmable mutations use
`CallbackError<E>`. Admission discloses once, successful statements merge
normally, and successful rollback returns the initiating carrier unchanged
while keeping earlier transaction statements usable.

Failed rollback retains residual undo, poisons the engine, discards the unsafe
checkout, and supersedes the initiating error with the disclosed fatal engine
failure. Armed statement drop still owns cancellation while action execution
or rollback awaits. Application payloads never become mandatory cleanup work.
The separate private transaction runner retains its original typed boundary.

Callback-bearing statement, accessor, and index-mutator methods transport the
shared carrier. Engine-only leaves and deferred-update application helpers
keep their existing results. Index mutation error settlement folds pending
unique-driver updates into ordinary undo before statement rollback, including
when the initiating failure is an application error.

### Shared Scan Cursor And Consumers

The private cursor combines callback decisions and projection through a
`CE: From<Error>` result. Projection errors convert after lazy-row buffer reset;
callback errors return unchanged after reset. Transaction streams instantiate
the callback carrier, while snapshot partition streams retain public `Error`,
owned-stream sendability, first-error publication, and peer abort behavior.

Either transaction-stream error arm, `Stop`, or exhaustion closes retained
callback/page/cursor state before checkout return. Later `next` calls return
`Ok(None)`. Early drop, cancelled construction, and persistent pending-load
descriptors retain their existing cleanup and resumability. The transaction
borrow ends on stream drop; accepted locks remain until transaction settlement.

Consumers use explicit `CallbackResult<_>` for engine-only callbacks. This
annotation fixes the application arm to `Infallible`; unconstrained bare `Ok`
is not promised to infer it. Engine-only enclosing results can use the reverse
conversion, while application-specific enclosing errors need their own
conversion because Rust does not chain `From` implementations automatically.

The benchmark implements concrete conversions for `CallbackError<BenchError>`
and `CallbackError<Infallible>`. Engine reports enter its storage variant and
user `BenchError` values return intact. Update callbacks now stop immediately
on validation failure through statement settlement, preserving any fatal
rollback error and the workload's terminal transaction cleanup policy.

## Implementation Notes

All planned callback boundaries now preserve engine classification and
application payload identity through existing settlement and cleanup paths.
The coordinated workspace migration includes managed consumers, row API tests,
the quick-start example, benchmark workloads, and public/error guidance.
There were no material plan deviations or deferred implementation issues.

Regression coverage uses a caller-borrowed, non-Clone, non-Send payload without
formatting or standard-error traits. One shared mixed-storage scenario covers
sequential and index traversal plus deferred hot/cold unique-key updates,
restored indexes, immediate callback termination, and prior-statement survival.
Fatal rollback tests cover both initiating arms. Existing row/index rollback
cancellation hooks now also exercise a typed user failure.

Scan tests cover user and row-access engine failures after an included row over
both hot and cold data, callback destruction before stream drop, terminal
behavior, and projection failure with input validation disabled. Existing cold
corruption, stop/drop, constructor cancellation, partition failure, and
sendability coverage passed. The benchmark regression verifies that a later
application validation failure restores earlier row updates and releases its
transaction.

Verification completed on 2026-09-05:

- Strict workspace clippy and alternate `libaio` clippy passed for all targets.
- Workspace nextest passed 1,933 tests across four binaries.
- Alternate `libaio` nextest passed 1,817 storage tests.
- Branch-diff style audit against `origin/main` passed for 24 Rust files,
  including formatting and strict workspace clippy. Two conversion impl
  placement findings were corrected before the final passing gate.
- The public-error disclosure audit was regenerated and remained unchanged;
  no new disclosure sites or unrelated public-error ownership appeared.
- Diff review confirmed unchanged cancellation owners, private-runner behavior,
  deferred-update settlement ordering, and callback-free stream results.

Source backlog 000191 is closed as implemented. Historical tasks 000293/000294
and RFC 0031 link this follow-up; no parent RFC phase synchronization applies.

## Impacts

- Public source break: managed-specific names become `CallbackError` and
  `CallbackResult`, and row callbacks now return the shared carrier with an
  explicit engine-only annotation where needed.
- Transaction execution, sequential/index mutation, and shared scan cursor
  orchestration transport application errors without changing data ownership.
- Benchmark conversion and update failure handling use the common boundary.
- Public API, transaction documentation, and coding guidance describe the
  precise callback-combining scope and inference contract.
- No persisted format, recovery, MVCC, lock, backend, or timeout configuration
  changes were introduced.

## Test Cases

- Both carrier arms, all accessors, formatting/source integration, owned payload
  identity, intact diagnostic frames, `E = Error`, and reverse conversion.
- Compiled engine-only and application-error callbacks, borrowing payloads,
  lazy-row engine propagation, and explicit application wrapping.
- Managed user failures for all three operations, definition/allocation
  preservation, and existing validation, stale-schema, and atomicity behavior.
- Mixed hot/cold statement rollback, deferred unique-driver undo, prior
  statement survival, callback termination, fatal precedence, and cancellation.
- Pre-callback engine rejection, row-access errors, later uniqueness/input
  failure, scan projection/corruption diagnostics, and resource release.
- Snapshot partition failure/peer abort/sendability and benchmark conversion,
  update rollback, catalog/binding, scan, and quick-start consumer integration.

## Open Questions

None. No new follow-up backlog was required.
