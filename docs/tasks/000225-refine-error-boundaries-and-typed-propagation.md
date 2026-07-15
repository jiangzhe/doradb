---
id: 000225
title: Refine error boundaries and typed propagation
status: proposal
created: 2026-07-15
github_issue: 853
---

# Task: Refine error boundaries and typed propagation

## Summary

Apply the error-boundary contract in `docs/error-spec.md` to one complete
public operation, `Statement::table_scan_mvcc`, and to the already identified
logical-lock consumer chain. Internal helpers should preserve the narrowest
domain report they produce, cross-domain consumers should use
`error_stack::ResultExt::change_context` without losing source frames, and the
public `Error` wrapper should appear only at the public API boundary or a
documented genuine multi-domain convergence point.

The table-scan slice covers table resolution, lock acquisition, lifecycle
validation, root capture, cold persisted reads and validation, hot row-page
index traversal, and the final statement boundary. The implementation also
identified runtime admission, session availability, transaction admission and
discard, and transaction-owned lock-state lookup as Lifecycle-domain concerns.
Those cases are reclassified for long-term maintenance; remaining questionable
invariant failures stay as `InternalError` and are collected into one follow-up
backlog item.

`RowPageIndexMemCursor` conservatively propagates the fallible `BufferPool`
contract instead of panicking. Production table block indexes currently use
`FixedBufferPool`, where cursor page lookup appears invariant-oriented rather
than a reachable IO failure. Proving that invariant and deciding whether the
production cursor should be asserted/infallible or remain fallible is deferred
to the same backlog instead of being justified by a synthetic failure test.

## Context

Task 000133 introduced `Error(Report<ErrorKind>)`, domain-specific internal
reports, and conversions that preserve `error-stack` frames. The follow-up
`docs/error-spec.md` now requires internal code to keep producer-domain result
types, attach operation context at the semantic caller, and avoid converting a
domain report to public `Error` merely to convert it back into another domain.

The lock providers already return `OperationResult`, but several consumers in
the transaction, statement, session, and catalog layers immediately erase the
typed report. Related helper chains accept an `operation` string solely for
error formatting. `Table::check_foreground_live` also has both typed and
top-level wrappers, contrary to the specification's canonical-helper rule.

`Statement::table_scan_mvcc` is the vertical exemplar for applying the same
rules through a complete public call stack:

```text
Statement::table_scan_mvcc                         public Result boundary
├── resolve_user_table                            OperationResult
├── acquire_table_read_lock                       OperationResult
├── Table::check_foreground_live                  OperationResult
└── TableAccessor::table_scan_mvcc                 genuine mixed-domain result
    ├── root_snapshot                             infallible
    ├── scan_cold_lwc_mvcc
    │   ├── column-block-index and readonly load  existing IO/completion seam
    │   └── persisted decode/validation           DataIntegrityResult
    └── mem_scan_from
        ├── pool-guard/index invariants            InternalResult where narrow
        └── RowPageIndexMemCursor                 fallible trait seam; proof deferred
```

The pre-change cold path uses
`DataIntegrityResultExt::with_block_context`, which attaches useful block
identity but converts immediately to crate `Result`. The formatting-only
extension should be removed; its consumers should use `ResultExt::attach` or
`attach_with` directly so the report remains DataIntegrity-domain.

The pre-change hot path uses `RowPageIndexMemCursor::seek` and `next`, whose
calls to the fallible `BufferPool` trait use `expect`. Propagating that shared
result is the conservative change and keeps the direct consumers bounded.
However, the production block index is backed by the fixed metadata pool: its
cursor reads do not perform IO, and its returned page-kind error may represent
a violated ownership/reachability invariant. This task does not manufacture an
otherwise unreachable failure to test that route. Backlog 000159 owns the
proof and final assertion-versus-recoverable decision.

Some lower APIs are already genuine convergence points. Readonly persisted
loading crosses completion, IO, and validation concerns. Generic buffer-pool
access can also report resource, lifecycle, IO, and internal failures, although
the fixed-pool production cursor may eventually prove invariant-owned. Until
that proof, an `Error` already returned from either seam flows monotonically
outward and is never converted back into a domain report.

Relevant references:

- `docs/error-spec.md`
- `docs/architecture.md`
- `docs/transaction-system.md`
- `docs/index-design.md`
- `docs/checkpoint-and-recovery.md`
- `docs/table-file.md`
- `docs/process/coding-guidance.md`
- `docs/process/unit-test.md`
- `docs/process/lint.md`
- `docs/tasks/000133-adopt-error-stack-for-storage-errors.md`

Issue Labels:

- type:task
- priority:medium
- codex

## Goals

1. Preserve `OperationResult` through the logical-lock consumer chain until a
   public or semantic operation boundary owns the conversion.
2. Remove operation-name parameters from touched resolution, checkout, lock,
   and lifecycle helpers when the value is used only to format errors.
3. Make the public `Statement::table_scan_mvcc` method the owner of its
   operation name and table identifier, attaching each exactly once per
   failing route.
4. Trace and classify every fallible edge in the cold and hot table-scan paths
   as same-domain propagation, direct cross-domain conversion, or a genuine
   multi-domain convergence.
5. Remove `DataIntegrityResultExt::with_block_context`; keep persisted decode
   and validation failures as `DataIntegrityResult` while block identity is
   attached directly with `error-stack`, and preserve the original validation
   frame at the public boundary.
6. Make `RowPageIndexMemCursor` conservatively propagate the fallible
   `BufferPool` contract and update all direct production consumers without
   claiming that a synthetic failure is reachable through the current fixed
   metadata pool.
7. Audit production `unwrap`, `expect`, and `unreachable` sites encountered on
   the traced stack. Retain proven invariants and convert recoverable or
   client-visible failures to the narrowest available result type.
8. Model runtime admission, unavailable sessions, conflicting transaction
   admission, discarded transactions, and transaction-owned lock-state lookup
   as Lifecycle-domain failures. Preserve remaining invariant-oriented
   `InternalError` classifications and defer their assertion-versus-recoverable
   decision.
9. Add regression coverage for public classification, preserved source and
   target frames, diagnostic attachments, and absence of top-level
   round-tripping.
10. Keep `docs/error-spec.md` synchronized with durable rules, resolved debt,
    and newly discovered recurring patterns.

## Non-Goals

- Converting every `doradb-storage` function from crate `Result` to a typed
  domain result in one sweep.
- Redesigning the `BufferPool` trait, readonly buffer cache, completion
  transport, file IO, backend IO, or their legitimate mixed-domain result
  types.
- Reclassifying remaining invariant-oriented `InternalError` variants beyond
  the implementation-discovered Lifecycle boundaries selected by this task.
- Proving fixed-metadata-pool cursor reachability invariants or making the
  production cursor asserted/infallible; backlog 000159 owns that follow-up.
- Removing every `unwrap`, `expect`, or `unreachable` in the affected modules;
  proven internal invariants remain explicit assertions.
- Refactoring `DmlValidationResultExt` or unrelated DML, index mutation,
  checkpoint, recovery, or completion paths unless a signature changed in the
  selected call stack requires a mechanical update.
- Changing transaction visibility, locking semantics, table lifecycle,
  persisted formats, IO scheduling, or scan behavior.
- Adding or modifying unsafe code.

## Plan

### 1. Establish propagation and audit guardrails

Before changing signatures, record the current public and typed boundaries for
the lock chain and `Statement::table_scan_mvcc`. For every touched function,
classify its failures using these rules:

- a single producer domain returns that domain's result alias;
- same-domain callers use `?` and add only newly owned facts;
- a caller-neutral or lower-domain report uses `change_context` only when the
  consumer assigns a real new domain meaning;
- a public or genuine multi-domain boundary may return crate `Result`; and
- a crate `Error` already produced at a legitimate convergence point flows
  only outward and is never changed back into `OperationError`,
  `DataIntegrityError`, or `InternalError`.

Audit production `unwrap`, `expect`, and `unreachable` sites on the traced
stack. Document the classification in code when it is not self-evident and in
the task implementation notes at resolution:

- keep an assertion when preceding ownership, latch, or cursor state proves
  the value exists and failure is an internal invariant violation; and
- return an error when runtime resource, lifecycle, IO, corruption, or valid
  client input can reach the failure.

### 2. Migrate the logical-lock consumer chain

Follow the migration order already recorded in the specification:

1. `doradb-storage/src/trx/stream_stmt.rs`
2. `doradb-storage/src/trx/stmt.rs`
3. `doradb-storage/src/trx/mod.rs`
4. `doradb-storage/src/session.rs`
5. `doradb-storage/src/catalog/index.rs`
6. `doradb-storage/src/catalog/table.rs`

Change pure lock helpers such as `acquire_table_read_lock` to return
`OperationResult`. Where write or explicit-lock orchestration combines
`LifecycleResult` from checked transaction-owned lock state with
`OperationResult` from the lock manager, stack
`OperationError::LockUnavailable` over
`LifecycleError::TransactionDiscarded`. Remove operation arguments passed
through mechanical helper layers and attach operation, `table_id`, lock
resource, and mode at the first caller that owns them.

Keep lock behavior unchanged: acquisition order, duration, upgrade behavior,
rollback, and release semantics are not part of this refactor.

### 3. Canonicalize statement table resolution and liveness

Change statement and stream table-resolution helpers to return
`OperationResult<Arc<Table>>` without accepting an operation string. Preserve
`OperationError::TableNotFound` and attach stable lookup facts owned by the
resolver; attach the public operation name at the statement or stream method.

Replace the `Table::check_foreground_live(operation)` and
`check_foreground_live_report()` pair with one canonical typed helper. It
should preserve the lifecycle-produced operation report and table identity,
while each semantic caller owns its operation attachment. Update affected
callers mechanically without duplicating operation text.

### 4. Model runtime access and transaction availability as Lifecycle

Use `LifecycleResult` for private engine admission, session pin/query/state,
transaction admission/checkout, discarded-transaction resolution, and checked
transaction-owned lock-state helpers. Add stable Lifecycle variants for
runtime unavailable, session unavailable, existing transaction, and
transaction discarded.

Keep a poison-blocked engine admission Lifecycle-owned by stacking
`LifecycleError::RuntimeUnavailable` over its original Fatal report. Public
engine, session, and transaction methods attach their operation context before
converting these typed reports to public `Error`. Do not change a Fatal failure
that occurs after admission into Lifecycle merely because the same operation
entered through an admission check.

### 5. Make `Statement::table_scan_mvcc` the public boundary

Use one operation constant for `Statement::table_scan_mvcc`. Attach that
operation and `table_id` before converting typed resolution, read-lock, and
liveness failures to public `Error`. For the lower mixed-domain table scan,
attach the same public operation facts to the existing public report without
changing its `ErrorKind` or reconstructing its source frames.

The rendered report for each route should contain the operation once. Do not
pass the operation constant into `Table`, `TableAccessor`, row-page index, or
persisted decode helpers merely for formatting.

### 6. Preserve typed cold-scan validation

Remove `DataIntegrityResultExt::with_block_context`. Update its direct
consumers to call `ResultExt::attach` or `attach_with` with file kind, block
kind, and block ID so a decode/validation result stays typed until a real
boundary converts it. Prefer `attach_with` for dynamically formatted context
and do not introduce a replacement formatting-only extension.

Within the table-scan cold branch:

- make `root_snapshot` and `lwc_deletion_buffer` infallible where they only
  wrap infallible access;
- retain data-integrity reports through row-id, delete-delta, LWC-block, and
  row-value validation where the producer is single-domain;
- use `change_context` only when a consuming domain assigns new meaning to a
  lower or caller-neutral report; and
- preserve existing readonly load and completion convergence instead of
  wrapping `Error` in another domain report.

Compiler-required changes to former `with_block_context` consumers are in
scope, but do not use the extension removal as an excuse to redesign their
broader error boundaries.

### 7. Conservatively propagate row-page cursor results

Change `RowPageIndexMemCursor::seek` and `next` to return fallible results, and
thread buffer-pool errors through the private traversal helper without
converting them to another domain. `Validation` remains optimistic retry
control flow and must not be modeled as an error.

Specifically replace the cursor's `BufferPool::get_page(...).await.expect(...)`
sites with propagation. Re-evaluate the adjacent latch and cursor-state
`unwrap` sites individually; retain those proven by held-parent or successful
search state, with a concise invariant explanation where the proof is not
obvious.

Update every direct production consumer, including table hot scans, original
row-page snapshotting, and row-page counting. A formerly infallible wrapper
such as `Table::total_row_pages` should become fallible and use its existing
public `Session` boundary rather than swallowing a cursor result. Update
existing cursor tests for the new result shape, but do not add a synthetic
buffer-pool failure or test-only public seam when the configured production
pool cannot reach that failure.

Record in backlog 000159 that production `BlockIndex` uses `FixedBufferPool`
and that cursor root, child, sibling, and re-seek access may be invariant-owned.
The follow-up must prove page lifetime, pool identity, page kind, parent-child
reachability, and table-drop concurrency before replacing propagation with an
assertion or infallible API. If a failure is reachable in production, retain an
appropriate typed result and test it through that real path.

If this work reveals that propagation requires changing the shared
`BufferPool` error model rather than only the cursor and its direct consumers,
stop at that boundary, preserve the existing shared type, and record the
larger redesign in the task's backlog handoff. Do not create an `Error` to
domain-report conversion as a shortcut.

### 8. Preserve and defer remaining invariant modeling

For invariant-oriented `InternalError` producers that remain after the chosen
Lifecycle reclassification, preserve the existing variant and runtime
behavior. This includes active-engine attachment checks, missing pool guards,
missing row-page/index nodes, and the production cursor reachability question.
Narrow a helper to `InternalResult` when this only removes premature conversion
to public `Error`; do not reclassify the condition or replace it with a panic
without the deferred ownership/concurrency proof.

Add a consistent TODO at questionable invariant producers that need a future
decision about assertion versus recoverable internal failure. During
implementation, create one backlog document through the repository backlog
workflow. The backlog item must list the source locations, current variants,
why each condition appears invariant, and the decision needed in a follow-up.
Reference that backlog item from the TODOs and from this task during resolve.

### 9. Replace round-trip examples and update the specification

Replace `test_index_mutation_context_preserves_lower_error`, which currently
constructs `InternalError -> ErrorKind -> OperationError`, with tests that
start from a typed source report, call `change_context` directly, and assert
that both source and target frames survive conversion to the final public
boundary.

Update `docs/error-spec.md` during implementation to:

- include the anti-pattern against `DomainAError -> Error -> DomainBError` and
  state that once public `Error` is produced it propagates only outward;
- record the selected Lifecycle ownership for runtime, session, transaction,
  and transaction-owned lock-state availability;
- record direct `ResultExt::attach`/`attach_with` as the replacement for the
  removed data-integrity formatting extension;
- record any recurring unwrap/invariant or convergence rule discovered by the
  implementation;
- mark the migrated lock and table-scan inventory accurately after all
  required verification passes; and
- add newly discovered durable debt without turning routine progress into
  normative prose.

Routine file-by-file progress and one-off implementation decisions belong in
the task's `Implementation Notes` during task resolution, not in the error
specification.

## Implementation Notes

## Impacts

- `docs/error-spec.md`: anti-patterns, boundary guidance, and implementation
  status inventory.
- `doradb-storage/src/error.rs`: Lifecycle variants, removal of the
  data-integrity formatting extension, and direct cross-domain regressions.
- `doradb-storage/src/trx/stream_stmt.rs`: typed table resolution and read-lock
  propagation for stream operations.
- `doradb-storage/src/trx/stmt.rs`: statement resolution, lock helpers, and the
  public `Statement::table_scan_mvcc` error boundary.
- `doradb-storage/src/engine.rs`, `doradb-storage/src/session.rs`, and
  `doradb-storage/src/trx/mod.rs`: typed runtime/session/transaction admission,
  public session lock and maintenance operations, table resolution, fallible
  row-page counting, explicit lock orchestration, and checked lock-state
  convergence.
- `doradb-storage/src/catalog/index.rs` and
  `doradb-storage/src/catalog/table.rs`: DDL lock acquisition context and any
  mechanical typed-helper changes.
- `doradb-storage/src/table/mod.rs` and
  `doradb-storage/src/table/lifecycle.rs`: canonical foreground liveness,
  infallible root access where applicable, and table-level cursor consumers.
- `doradb-storage/src/table/access.rs`: cold/hot scan convergence, persisted
  validation, and removal of false fallibility.
- `doradb-storage/src/table/mem_table.rs`: hot scan helpers, internal invariant
  producers, and direct cursor consumers.
- `doradb-storage/src/index/row_page_index.rs` and
  `doradb-storage/src/index/block_index.rs`: fallible row-page cursor API and
  direct-consumer propagation; invariant proof remains deferred.
- Former consumers of `DataIntegrityResultExt::with_block_context`, including
  table persistence/GC, catalog storage/index, LWC block, column deletion blob,
  column block index, and disk tree modules, for direct typed attachments.
- One new `docs/backlogs/` item containing deferred invariant-modeling
  candidates discovered in the selected call stack.

## Test Cases

- Before a public boundary, assert that table-not-found, lock rejection, and
  table-dropping helpers expose `OperationError` as their current context.
- Through `Statement::table_scan_mvcc`, verify missing and dropping tables map
  to `ErrorKind::Operation`, retain the specific `OperationError` frame, and
  render `operation=table_scan_mvcc` and the table ID exactly once.
- Exercise a statement read-lock failure and verify the original lock-domain
  variant and lock details survive the public conversion.
- Verify engine admission, session availability, transaction admission/discard,
  and checked lock-state failures retain their selected Lifecycle variants;
  where an Operation consumes lock-state failure, assert both Operation and
  Lifecycle frames.
- Inject invalid cold-scan persisted data and verify the public result is
  `ErrorKind::DataIntegrity`, retains the original `DataIntegrityError`, and
  includes file kind, block kind, and block ID.
- Exercise row-page cursor traversal and row-page counting through the actual
  fixed metadata pool. Do not inject a failure that the production
  configuration cannot reach; defer assertion-versus-recoverable error-path
  coverage to backlog 000159.
- Keep coverage for optimistic cursor invalidation/retry behavior so
  `Validation::Invalid` remains control flow rather than an error.
- Verify retained cursor-state and latch assertions are only reached after the
  state that proves their invariant.
- Replace the top-level round-trip unit test with direct typed
  `change_context` coverage that asserts both source and target report frames.
- Run the existing hot-only, cold-and-hot, cold deletion/update visibility,
  persisted delete-delta, and early-stop `table_scan_mvcc` tests.
- Run focused tests for error, lock, transaction statement, table access,
  row-page index, catalog, and session modules as they are changed.
- Run `cargo fmt` and
  `cargo clippy --workspace --all-targets -- -D warnings`.
- Run `tools/style_audit.rs` against the branch diff.
- Run `cargo nextest run --workspace`.
- Because persisted and buffer access are backend-neutral IO paths, also run
  `cargo nextest run -p doradb-storage --no-default-features --features libaio`.
- Run focused coverage for materially changed Rust files with
  `tools/coverage_focus.rs`; target at least 80% focused line coverage by
  default. Definition-heavy files such as `error.rs` may be slightly below the
  whole-file threshold when changed executable paths have direct coverage and
  aggregate focused coverage remains above the bar; report the concrete result.
- Run `git diff --check`.

## Open Questions

None block implementation. The assertion-versus-recoverable treatment of
remaining `InternalError` producers and fixed-pool cursor page access is
intentionally deferred to backlog 000159. Tests for those cases must exercise a
reachable production path rather than manufacture an impossible failure.
