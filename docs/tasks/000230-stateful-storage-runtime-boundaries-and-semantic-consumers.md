---
id: 000230
title: Stateful Storage Runtime Boundaries and Semantic Consumers
status: proposal  # proposal | implemented | superseded
created: 2026-07-20
github_issue: 868
---

# Task: Stateful Storage Runtime Boundaries and Semantic Consumers

## Summary

Implement RFC-0023 Phase 3 across `row`, `index`, `table`, and
`catalog`, plus the narrow transaction suppliers that those modules require.
Replace early public-error convergence with typed stateful-storage contracts,
keep expected storage outcomes neutral until a foreground or replay consumer
assigns meaning, and preserve native source reports throughout the stack.

Treat row/index/table/catalog as an internal integration layer rather than a
public operation boundary. Add concise Runtime contexts for integration work:
`IndexAccess`, `TableAccess`, `CatalogAccess`, `CheckpointExecution`, and
`SystemTransactionCommit`. Do not add one Runtime variant per method. In
particular, remove `OperationError::IndexMutation` and use
`RuntimeError::IndexAccess` for index mechanics; attach the concrete mutator,
table, and index diagnostics to the report.

Introduce a small `RuntimeOrFatalError` carrier for operations whose audited
producer set has two materially different exits: an ordinary Runtime failure
or an already-Fatal failure. The carrier owns `Report<RuntimeError>` or
`Report<FatalError>` directly so Fatal can bypass Runtime and Operation
folding. It is not an `error-stack` context and must not accept public
`Error`. Checkpoint and targeted commit consumers can attach caller context to
either arm, convert only Runtime into Fatal after an irreversible boundary,
and preserve an existing Fatal reason unchanged.

Preserve DataIntegrity frames from persisted row, index, table, and catalog
physical decoding. Foreground DML maps neutral validation, duplicate-key, and
conflict outcomes to Operation only at the semantic consumer; recovery maps
malformed physical replay payloads to DataIntegrity and asserts semantic
metadata previously validated and persisted by the engine. Complete the
Internal/Generic audit, narrow terminal rollback suppliers so Fatal is stacked
directly on the real typed report, and remove the temporary public `ErrorKind`
compatibility frame owned by backlog 000161.

## Context

Parent RFC:

- `docs/rfcs/0023-storage-error-boundary-propagation-migration.md` — Phase 3,
  **Stateful Storage and Semantic Consumers**

Prerequisites:

- `docs/tasks/000228-typed-infrastructure-error-boundaries.md` completed
  RFC-0023 Phase 1 and stabilized typed file, buffer, log, configuration, and
  targeted transaction suppliers.
- `docs/tasks/000229-completion-bridge-and-infrastructure-closure.md` completed
  RFC-0023 Phase 2 and provides the semantic-free,
  `CompletionErrorBridge`/`SharedFatalError` transport needed by this task.

Issue Labels:

- type:task
- priority:medium
- codex

Source Backlogs:

- `docs/backlogs/000159-reassess-invariant-oriented-table-scan-errors.md` —
  prove the stateful-storage portions covering fixed metadata pools,
  `PoolGuards`, row-page/index-page reachability, and cursor traversal. The
  transaction-only `TrxInner::checked_engine` portion remains Phase 4 work and
  will replace this backlog with a narrower follow-up at task resolution.
- `docs/backlogs/000161-narrow-terminal-rollback-undo-error-boundaries.md` —
  narrow row/index rollback suppliers and remove the temporary public-error
  frame before `FatalError::RollbackAccess`.

Current-state evidence:

- `UniqueIndex`, `NonUniqueIndex`, `IndexBatchStream`, cursor/projector
  adapters, and owned/composite index streams return crate public `Result`.
  Their table consumers repeatedly change those public reports to
  `OperationError::IndexMutation`.
- `OperationError::IndexMutation` is used only as an internal table/mem-table
  context (plus its documentation and unit test); it is not a user-visible
  semantic outcome.
- `IndexInsert`, `IndexCompareExchange`, `LinkForUniqueIndex`, `Validation`,
  DML validation reports, checkpoint delay, and checkpoint cancellation
  already express caller-neutral states that should remain values.
- Production `BlockIndex` metadata traversal uses `FixedBufferPool`. Root
  lifetime, pool identity, and parent-held child reachability make several
  generic buffer failure paths invariant candidates; a test-only failing pool
  is not evidence that production traversal is recoverably fallible.
- `TableCheckpointError` currently contains a `Public(Error)` arm, inspects
  `ErrorKind` to recover Fatal, and combines public and typed reports.
- `TransactionSystem::commit_sys` and
  `RowPageCreateRedoCtx::commit_row_page` return public `Result` even though
  their relevant consumer contract is Runtime-or-Fatal.
  `TransactionSystem::publish_table_file_root` similarly erases the
  `RuntimeResult` returned by `MutableTableFile::commit`.
- `TableMetadata::try_*` and related layout constructors use public `Result`
  for user DDL validation and persisted reconstruction, including
  `InternalError::Generic` for malformed metadata.
- Terminal row/index undo rollback returns public `Error` before the
  transaction owner stacks `FatalError::RollbackAccess`. The completion bridge
  therefore has a temporary exception that skips a lower `ErrorKind` beneath
  Fatal.

The approved responsibility rules are:

1. A leaf producer returns its native typed domain.
2. Expected absence, invalidation, conflict, delay, and cancellation remain
   values until the semantic consumer.
3. Reusable stateful-storage integration returns Runtime, except that an
   already-Fatal source uses `RuntimeOrFatalError::Fatal`.
4. Operation context is added only when the caller is close to a real public
   operation, no public-domain-public round trip is introduced, and the
   Operation variant is conceptually accurate.
5. Public `Error` is created only at an existing public facade or a documented
   genuine mixed-domain seam and propagates only outward.

## Goals

1. Add the five approved Runtime integration variants and the constrained
   `RuntimeOrFatalResult<T>` contract without creating a general internal
   error-set framework.
2. Remove `OperationError::IndexMutation`. Make
   `RuntimeError::IndexAccess` canonical for index mechanics and retain
   mutator-specific detail as attachments.
3. Narrow reusable row/index/table/catalog and targeted transaction supplier
   signatures to one native domain, Runtime, a neutral outcome, or the explicit
   Runtime/Fatal branch.
4. Preserve DataIntegrity and other native source frames when Runtime,
   Operation, or Fatal contexts are added.
5. Keep lookup absence, optimistic invalidation, duplicate insertion, DML
   validation, checkpoint delay, and cancellation neutral until their actual
   foreground or replay consumer.
6. Map foreground duplicate keys, write conflicts, invalid DML, and DDL state
   to accurate existing Operation variants. Do not use those variants to wrap
   unrelated Runtime failures.
7. Make checkpoint error flow Runtime-or-Fatal, with explicit reversible and
   irreversible conversion rules and no public-error inspection.
8. Narrow row/index undo rollback so terminal rollback changes the real typed
   source directly to Fatal and the completion bridge no longer needs its
   `ErrorKind` compatibility exception.
9. Resolve every production Internal and Generic producer in
   `row`/`index`/`table`/`catalog` through a proved assertion, a specific
   responsible domain, or a specific Internal variant. Leave no production
   `InternalError::Generic` in scope.
10. Preserve public statement/session APIs, persisted formats, checkpoint
    atomicity, replay behavior, and transaction semantics.

## Non-Goals

- Redesign public `Error`, `ErrorKind`, public `Result`, statement, session, or
  engine APIs.
- Introduce `RuntimeError::Fatal`, `OperationError::Fatal`, a generic
  `Report<Either<...>>` abstraction, or blanket error-set machinery.
- Add a Runtime variant for every index mutator, table method, catalog method,
  or DML verb.
- Reclassify a source as Fatal merely because one coordinator may poison after
  the failure.
- Change row/index/table persistence formats, checkpoint sequencing or
  cancellation semantics, recovery ordering, transaction isolation, or
  rollback behavior.
- Complete RFC-0023 Phase 4's repository-wide audit of transaction, recovery,
  session, engine, and public-facade convergence.
- Treat a synthetic test implementation as proof that a production invariant
  must remain recoverable.
- Expand fault-injection infrastructure owned by backlog 000160 beyond adapting
  existing focused hooks to the new typed contracts.

## Plan

### 1. Establish central Runtime and Fatal-flow contracts

In `doradb-storage/src/error.rs`:

- Add these fieldless `RuntimeError` variants:
  - `IndexAccess` — lookup, traversal, mutation, binding, and index-stream
    integration.
  - `TableAccess` — row/index/table integration that is not itself a public
    operation.
  - `CatalogAccess` — catalog storage and runtime integration.
  - `CheckpointExecution` — reversible checkpoint orchestration.
  - `SystemTransactionCommit` — no-wait system-transaction preparation and
    group-commit admission.
- Remove `OperationError::IndexMutation`. Keep `DuplicateKey`,
  `WriteConflict`, `InvalidDmlInput`, `TableNotFound`,
  `TableAlreadyExists`, `TableDropping`, and `IndexNotFound` only for their
  existing semantic meanings.
- Define the branch as an enum that owns reports, not as a report context:

```rust
pub(crate) enum RuntimeOrFatalError {
    Runtime(Report<RuntimeError>),
    Fatal(Report<FatalError>),
}

pub(crate) type RuntimeOrFatalResult<T> =
    std::result::Result<T, RuntimeOrFatalError>;
```

- Provide only controlled operations needed by audited callers:
  construction from Runtime and Fatal reports (and `SharedFatalError` where
  required), lazy attachment on either arm, replacement of the Runtime arm's
  context while leaving Fatal as Fatal, outward conversion to public `Error`,
  and irreversible conversion that changes Runtime to a supplied Fatal reason
  but returns an existing Fatal report without adding or replacing its reason.
- Add a narrow `CompletionErrorBridge` split used only by audited
  Fatal-capable consumers: reconstruct an existing Fatal source into the Fatal
  arm; otherwise reconstruct the physical source chain under the caller's
  Runtime context. Attach the caller message to both paths.
- Do not implement `From<Error>`, do not expose raw branch inspection as a
  semantic matching shortcut, and do not permit `Report<RuntimeOrFatalError>`.
  Public `Error` is never accepted as input.
- Preserve the existing deliberate Lifecycle-over-Fatal behavior where
  lifecycle admission is itself the semantic decision. Fatal bypass applies
  when Runtime and Fatal are peer outcomes of the same integration operation,
  not as a repository-wide exception to contextual propagation.

At a real foreground boundary, an existing conceptually valid
`OperationError` may be changed directly over a Runtime report so the Runtime
source remains in the chain. Do not manufacture a generic Operation variant
just to hide `ErrorKind::Runtime`. A neutral duplicate/conflict value becomes
`OperationError::DuplicateKey` or `OperationError::WriteConflict` without a
fabricated Runtime source.

### 2. Narrow the targeted transaction suppliers

- Change `TransactionSystem::publish_table_file_root` to return
  `RuntimeResult<Arc<TableFile>>` and forward
  `MutableTableFile::commit` without public conversion.
- Change `TransactionSystem::commit_sys` to
  `RuntimeOrFatalResult<TrxID>`. Preserve poison and failed-precommit Fatal
  reports in the Fatal arm. Stack `RuntimeError::SystemTransactionCommit` over
  nonfatal Resource, Lifecycle, or specific Internal sources while preserving
  those lower frames and commit diagnostics.
- Change `RowPageCreateRedoCtx::commit_row_page` and the row-page/block-index
  append callers to use `RuntimeOrFatalResult`. Keep the no-wait ordering
  invariant and attach table/page/range context on both arms.
- Extract only the crate-private typed terminal-commit path needed by catalog
  DDL. Share mechanics with public `Transaction::commit`, but let catalog
  consumers receive Runtime-or-Fatal without converting through `Error`.
  Nonfatal terminal admission/completion failures receive
  `RuntimeError::CatalogAccess`; an existing Fatal report bypasses Runtime.
  Keep the public `Transaction::commit` adapter and its behavior unchanged.
- Consume completion failures through the controlled bridge split from step 1.
  Do not match public `ErrorKind` or reconstruct domains from public reports.

These transaction edits are supplier changes required by Phase 3. Do not
broaden them into the Phase 4 transaction/orchestration audit.

### 3. Resolve row and fixed-pool contracts

- Audit row access, row-page creation, vector scan, row-page index, block index,
  and their buffer/pool-guard helpers from the production ownership model.
- Preserve malformed persisted row/LWC payloads as DataIntegrity and
  caller-provided row-shape mismatches as neutral DML validation.
- Prove fixed metadata-pool root lifetime, pool identity, page kind,
  parent-protected child/sibling reachability, and re-seek behavior. Replace
  impossible cursor/page failures with assertions or infallible private APIs.
  Retain a typed error only if a valid production schedule reaches it.
- Keep `PoolGuards` globally capable of partial construction because catalog
  bootstrap uses that shape. At each operation, make its required row/index/
  disk guard slot an explicit caller contract; use an assertion/infallible
  accessor only after proving the operation cannot be entered without it.
- Do not retain fallibility solely for a test-only generic pool. Focused tests
  must use production pool configuration and real ownership schedules.
- Carry recoverable row/index integration failures under
  `RuntimeError::TableAccess` or `RuntimeError::IndexAccess` with the native
  Buffer, DataIntegrity, or specific Internal frame retained below.

### 4. Make index interfaces Runtime-typed and outcomes neutral

- Convert `UniqueIndex`, `NonUniqueIndex`, `IndexBatchStream`,
  `IndexScanLeaf`, `IndexLeafCursor`, `IndexScanProjector`, owned streams, and
  composite hot/cold streams away from crate public `Result`.
- Keep leaf B-tree, MemIndex, DiskTree, encoding, buffer, and persisted-decode
  producers in their narrow native domain until the index integration method.
  The reusable trait boundary returns `RuntimeResult` and installs
  `RuntimeError::IndexAccess` without a public intermediary.
- Retain source frames such as `DataIntegrityError::InvalidPayload`,
  `RuntimeError::BufferPageAccess`, or a specific `InternalError` below
  `IndexAccess`. Never rebuild a report.
- Add concise lazy attachments at the owner that knows the operation:
  `operation=lookup`, `operation=scan_candidates`,
  `operation=insert_if_not_exists`, `operation=compare_exchange`,
  `operation=compare_delete`, or mask direction, together with table/index
  identifiers where available. Do not attach key values or duplicate the same
  operation at every forwarding frame.
- Preserve `Option` lookup absence, `Validation` invalidation,
  `IndexInsert::DuplicateKey`, `IndexCompareExchange`, and other expected
  outcomes as values. Reuse those enums or add a table-private neutral mutation
  outcome only where several of them must converge; do not encode them as
  Runtime errors.
- Remove every `change_context(OperationError::IndexMutation)` call and replace
  its documentation/unit coverage with Runtime source-preservation and
  attachment tests.

### 5. Refactor table integration and semantic consumers

- Narrow `MemTable` and `TableAccess` reusable helpers that currently use
  public or Operation results only to combine row/index mechanics. Return
  `RuntimeResult` plus neutral outcomes and apply `TableAccess` at the table
  integration owner.
- Keep duplicate-key and write-conflict decisions neutral throughout unique
  index linking/retry loops. The foreground insert/update/delete consumer maps
  only the final neutral outcome to `OperationError::DuplicateKey` or
  `OperationError::WriteConflict`.
- Keep DML producer methods returning `DmlValidationResult`. At each foreground
  or replay consumer, explicitly apply the appropriate Operation or
  DataIntegrity context and attach the operation/table identity; do not hide
  the caller-owned semantic decision behind a formatting extension.
- Preserve malformed persisted LWC/table/index state as DataIntegrity during
  scans, cold-row access, page transition, and recovery. Invalid replay maps
  directly to DataIntegrity without a public-domain-public round trip.
- Narrow `table::recover` entry points and their storage suppliers so recovery
  receives typed Runtime/DataIntegrity reports and neutral outcomes. Existing
  top-level recovery/public adapters remain Phase 4-owned.
- A table access helper may retain crate public `Result` only when an
  evidence-backed, indivisible private orchestration chain genuinely combines
  unrelated domains. Document each retained seam at the function and in
  `docs/error-spec.md`; a broad callee signature alone is not evidence.

### 6. Replace the checkpoint public branch

- Delete `TableCheckpointError::Public` and the conversions from public
  `Error`. Remove `ErrorKind` inspection and Fatal downcasting from table
  checkpoint code.
- Make the internal checkpoint chain return
  `RuntimeOrFatalResult<CheckpointOutcome>`. Convert nonfatal typed sources to
  `RuntimeError::CheckpointExecution` at the checkpoint integration boundary,
  retaining their physical source frames.
- Keep delay, retry, workflow admission refusal, and cancellation in
  `CheckpointOutcome`/`CheckpointCancelReason`. They are not Runtime failures.
- Before `TableCheckpointer::set_irreversible`, return Runtime failures without
  poisoning. After the irreversible marker:
  - change a Runtime report to the step-owned
    `FatalError::CatalogWrite` or `FatalError::CheckpointWrite` and retain the
    Runtime/native source frames;
  - propagate an existing Fatal report unchanged, except for caller-owned
    attachments;
  - never replace an existing Fatal reason with the checkpoint fallback.
- Route silent-watermark catalog mutation, root publication, test hooks, and
  system-transaction commit through typed suppliers. Preserve the current
  publication order and no-cancel section.

### 7. Make catalog metadata and DDL consumers semantic

- Add `OperationError::InvalidMetadata` for invalid user-defined table and
  index metadata. Schema DDL is an operation request, not storage-engine
  configuration; remove `ConfigError::InvalidIndexSpec`.
- Keep checked metadata construction Operation-typed. Catalog and table-file
  persistence serialize only validated `TableMetadata`, so reconstruction
  after format/version/checksum validation is an asserted invariant rather
  than a recoverable metadata error.
- Assert engine-owned catalog table/column/index relationships during reload.
  Keep physical catalog decoding and catalog/table-file root mismatches as
  DataIntegrity, including the explicit index-DDL reconciliation window.
- Keep absent foreground DROP INDEX targets as
  `OperationError::IndexNotFound`; after the target is checked under the
  metadata-change gate, metadata removal and runtime layout installation are
  infallible invariants.
- Narrow catalog storage helpers, table/index DDL phases, catalog checkpoint,
  runtime installation, and root publication to native, Runtime, neutral, or
  Runtime/Fatal contracts. Use `RuntimeError::CatalogAccess` for catalog
  integration and retain lower sources.
- Replace public-source post-commit poisoning helpers with typed
  Runtime-or-Fatal handling. At an irreversible DDL/catalog boundary, convert
  only Runtime to the owner-selected Fatal reason and preserve existing Fatal.
- Assert catalog cascade cardinality and engine-owned persisted referential
  relationships after physical validation. Keep public DDL state outcomes in
  their existing Operation variants.
- Do not alter catalog row schemas, IDs, serialized metadata, DDL ordering, or
  runtime publication semantics.

### 8. Narrow rollback sources and remove the compatibility exception

- Change `IndexRollback`, table rollback implementations,
  `IndexUndoLogs::rollback`, `RowUndoLogs::rollback`, and
  `MemTable::rollback_row_undo` to carry typed Runtime reports while preserving
  the owned undo entry on retryable row rollback failure.
- Keep impossible rollback mismatches as proven assertions; keep reachable
  access failures under `IndexAccess`/`TableAccess` with their native source
  frames.
- In `TransactionSystem::rollback_inner`, change the typed index/row source
  directly to `FatalError::RollbackAccess`, attach the terminal operation, and
  poison. Do not call `Error::into_report`.
- Apply the same typed supplier to failed-precommit cleanup where compiler
  fallout reaches it, without broadening public transaction behavior.
- Remove the two Phase 3 TODOs in `trx/sys.rs`, the completion replay exception
  that skips `ErrorKind` beneath Fatal, and its compatibility-only test.
  Replace it with a test proving the terminal rollback chain contains
  `FatalError::RollbackAccess` and the real Runtime/native source but no
  `ErrorKind` frame.

### 9. Complete the Internal and Generic audit

Inventory every production `Report<InternalError>`,
`InternalError::Generic`, `unreachable!`, `unwrap`, and `expect` site in
`row`, `index`, `table`, and `catalog`. Record the final disposition and proof
in Implementation Notes during `$task-resolve`.

Use these approved dispositions:

- Assert or make infallible when ownership/lifecycle proves prepared row shape,
  fixed-pool cursor reachability, required guard slots, scan boundaries,
  internal encoded keys, private lifecycle/progress states, transition markers,
  and slot/count relations impossible to violate.
- Use DataIntegrity for malformed physical persisted payloads and roots; assert
  semantic metadata and catalog relationships written transactionally from
  validated engine state.
- Use the neutral metadata/DML/outcome types when provenance determines whether
  a failure becomes Config, Operation, or DataIntegrity.
- Retain or add a specific fieldless Internal variant for a reachable
  engine-owned writer, representability, allocation, or ownership contract
  violation that belongs to no other domain.
- Use Runtime integration contexts over recoverable native sources; do not
  replace the source frame.

There must be no production `InternalError::Generic` in these modules after
the task. A test-only fault-injection fallback may remain only when it carries
the existing structured `TODO(error-boundary)` reference to backlog 000160 and
is not used as evidence for production classification.

### 10. Synchronize documentation, RFC, and backlogs

- Update `docs/error-spec.md` with the five Runtime operations,
  `RuntimeOrFatalError` rules, neutral metadata mapping, index/table/catalog
  module verdicts, checkpoint and rollback examples, approved mixed seams, and
  removal of `OperationError::IndexMutation`.
- At task resolution, synchronize RFC-0023 Phase 3 choices, status, issue,
  implementation summary, and validation outcome. Keep Phase 4's public
  convergence scope unchanged.
- Close backlog 000161 as implemented once direct typed rollback-to-Fatal and
  source-preservation tests pass.
- Resolve all stateful-storage acceptance items in backlog 000159. Create a
  replacement backlog limited to transaction-owned
  `TrxInner::checked_engine`/remaining transaction-only
  `ActiveTransactionDiscarded` sites, link it from RFC-0023 Phase 4, then close
  backlog 000159 as replaced.
- Do not create a separate supporting implementation task. The central branch,
  supplier narrowing, semantic consumers, audit, documentation, and backlog
  resolution are one Phase 3 acceptance unit.

## Implementation Notes

## Impacts

Primary code:

- `doradb-storage/src/error.rs`
- `doradb-storage/src/row/`
- `doradb-storage/src/index/`, especially `unique_index.rs`,
  `non_unique_index.rs`, `secondary_index.rs`, `index_stream.rs`,
  `owned_stream.rs`, `row_page_index.rs`, `block_index.rs`, MemIndex, B-tree,
  and DiskTree adapters
- `doradb-storage/src/table/`, especially `access.rs`, `mem_table.rs`,
  `persistence.rs`, `recover.rs`, `rollback.rs`, `layout.rs`,
  `lifecycle.rs`, `checkpoint_workflow.rs`, and `page_transition.rs`
- `doradb-storage/src/catalog/`, especially `table.rs`, `index.rs` and
  `storage/`
- Targeted suppliers in `doradb-storage/src/trx/mod.rs`,
  `trx/sys.rs`, `trx/undo/index.rs`, and `trx/undo/row.rs`

Documentation and tracking:

- `docs/error-spec.md`
- `docs/rfcs/0023-storage-error-boundary-propagation-migration.md`
- `docs/backlogs/000159-reassess-invariant-oriented-table-scan-errors.md`
- `docs/backlogs/000161-narrow-terminal-rollback-undo-error-boundaries.md`
- One Phase 4 replacement backlog for transaction-only invariant review

Risks:

- Trait signature changes have broad compiler fallout across foreground,
  recovery, rollback, purge, and tests. Conversions must remain at semantic
  owners instead of being added at the first compiler error.
- Runtime stacking can accidentally obscure DataIntegrity or Fatal. Tests must
  assert the complete retained chain, not only `ErrorKind`.
- Assertion conversion is safe only with a production ownership/concurrency
  proof. Generic test doubles cannot establish or refute that proof.
- Checkpoint and catalog DDL have irreversible boundaries. A Runtime report
  must be fatalized exactly once after that boundary, while an existing Fatal
  report must not gain a replacement reason.
- Large mechanical signature edits can duplicate diagnostic attachments.
  Rendered messages should contain each operation/table/index detail once.

No public API, persisted-format, checkpoint-ordering, or unsafe contract change
is intended. If implementation must alter unsafe-sensitive code or invariants,
run the repository unsafe inventory workflow and record the reason during task
resolution.

## Test Cases

### Central error contracts

- Runtime-arm construction, attachment, Runtime-context replacement, public
  conversion, and irreversible Runtime-to-Fatal conversion retain every native
  source frame.
- Fatal-arm construction and attachment preserve the original Fatal reason;
  irreversible conversion with a different fallback does not add or replace
  that reason.
- Completion bridge splitting maps IO/Resource/DataIntegrity/Lifecycle/Internal
  completion sources to Runtime with their frames intact, while a Fatal
  completion reconstructs directly into the Fatal arm with no Runtime frame.
- Compile/source tests establish that `RuntimeOrFatalError` has no public
  `Error` input, no blanket generic conversion, and is never itself used as an
  `error-stack` context.

### Row and index

- Production fixed-pool root/child/sibling/re-seek traversal succeeds through
  the asserted ownership path; no synthetic failing pool is used to justify a
  recoverable production result.
- Every unique/non-unique lookup, scan, insert, exchange, delete, and mask
  failure reports `RuntimeError::IndexAccess` with the original Buffer,
  DataIntegrity, or specific Internal frame and one mutator attachment.
- Lookup absence and optimistic invalidation remain values.
- Duplicate insertion remains neutral inside index/table helpers. Foreground
  DML maps it to `OperationError::DuplicateKey`; replay maps an impossible
  duplicate to the approved DataIntegrity reason.
- Write-conflict outcomes map to `OperationError::WriteConflict` only at the
  foreground consumer.
- Source review finds no `OperationError::IndexMutation` definition, use,
  documentation example, or test.

### Table and recovery

- Foreground invalid row/index payload maps from
  `DmlValidationError` to `OperationError::InvalidDmlInput` with table/action
  context and no public intermediary.
- The same malformed replay payload maps to
  `DataIntegrityError::InvalidPayload` and retains the neutral validation frame.
- Corrupt LWC, table root, or persisted index data retains DataIntegrity beneath
  any justified table/index integration context.
- Any retained mixed `TableAccess` seam has a production test for each domain
  arm and an explicit source comment explaining why decomposition is not
  feasible.

### System commit and checkpoint

- `commit_sys` returns Runtime/`SystemTransactionCommit` for nonfatal
  preparation, resource, or shutdown rejection with the original source
  retained; an engine-poison/redo Fatal source returns the Fatal arm directly.
- Row-page create redo exercises both Runtime and Fatal propagation and retains
  table/page/range attachments.
- Checkpoint delay and cancellation remain successful
  `CheckpointOutcome` values and do not poison.
- A pre-irreversible checkpoint failure returns Runtime and leaves the engine
  healthy.
- A post-irreversible Runtime failure becomes the step-owned
  `CatalogWrite`/`CheckpointWrite` Fatal reason with Runtime/native frames
  retained.
- A post-irreversible pre-existing Fatal failure keeps its original Fatal
  reason and has no additional Runtime or fallback Fatal frame.
- Silent-watermark and table-root publication tests preserve current ordering,
  cancellation cutoff, and successful redo CTS behavior.

### Catalog

- Invalid user table/index metadata is rejected as
  `OperationError::InvalidMetadata`; absent foreground index mutation remains
  `OperationError::IndexNotFound`.
- Catalog/table-file schema reconstruction and catalog relational assembly are
  asserted after physical persisted-data validation. Catalog/file root
  mismatch outside supported index-DDL reconciliation remains DataIntegrity.
- Catalog cascade cardinality/referential mismatch is asserted as an
  engine-owned transactional invariant.
- Catalog DDL commit and post-commit failures exercise Runtime and existing
  Fatal paths without converting through public `Error`.

### Rollback and completion

- Index and row undo rollback return typed Runtime/native sources while
  preserving unconsumed undo ownership where required.
- Terminal rollback stacks `FatalError::RollbackAccess` directly on that typed
  source, poisons once, and publishes the same source-bearing Fatal report to
  all waiters.
- The captured/reconstructed completion chain contains Fatal plus the real
  Runtime/native source and no `ErrorKind` frame.
- The compatibility skip and both backlog-000161 TODOs are absent.

### Audit and regression validation

- Source review shows no production `InternalError::Generic` in
  `row`/`index`/`table`/`catalog` and no unreviewed Internal producer.
- Focused tests cover every retained specific Internal producer through a valid
  production path; assertion-only states are not replaced with panic tests.
- Rendered diagnostic tests verify operation/table/index context appears once.
- Run focused module tests throughout implementation, then the full gates:

```bash
cargo fmt
cargo clippy --workspace --all-targets -- -D warnings
cargo nextest run --workspace
cargo clippy -p doradb-storage --no-default-features --features libaio --all-targets -- -D warnings
cargo nextest run -p doradb-storage --no-default-features --features libaio
tools/style_audit.rs
```

The repository's nextest configuration remains authoritative. Refresh the
unsafe inventory only if unsafe-sensitive code or contracts change.

## Open Questions

None. The Runtime integration operations, `RuntimeOrFatalError` representation,
Operation-context policy, Fatal bypass, metadata provenance mapping,
checkpoint policy, rollback boundary, and backlog dispositions are approved.
If implementation evidence contradicts an invariant proof or makes a
production Generic fallback unavoidable, stop and return that evidence for
design review rather than silently broadening this task.
