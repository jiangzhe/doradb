---
id: 000274
title: Retire Callback Statement APIs and Complete Migration
status: proposal  # proposal | implemented | superseded
created: 2026-08-19
github_issue: 990
---

# Task: Retire Callback Statement APIs and Complete Migration

## Summary

Implement RFC-0029 Phase 2 by removing callback-style statement execution from
the public API and completing the repository-wide migration to direct
`Transaction` methods. Replace the current borrowed normal statement facade
with one transaction-module-private `Statement` that is passed by value and
consumed by every user-table and catalog-table operation. Public and private
statement runners must merge effects only on success and complete
index-before-row rollback before returning an operation error.

Do not introduce the reusable `CatalogStatement` described by the current RFC.
Catalog DDL instead composes owned one-shot statements through
`PrivateTransaction`. When several catalog rows deliberately share one logical
statement, expose a purpose-built consuming batch DML on the matching catalog
accessor; do not expose a generic mixed-operation callback or reusable facade.
Update RFC-0029 to record this approved Phase 2 revision before synchronizing
the remaining code, tests, examples, benchmarks, and documentation.

## Context

Parent RFC:

- `docs/rfcs/0029-direct-transaction-statement-apis.md`

RFC Relationship:

- Phase 2: Callback API Retirement And Complete Migration.

Source Backlogs:

- `docs/backlogs/000186-statement-failure-rollback-before-error-return.md`

Prerequisites:

- `docs/tasks/000273-direct-transaction-apis-and-atomic-batch-insert.md`
  implemented RFC-0029 Phase 1, established direct API feature parity, migrated
  ordinary storage tests, classified every retained legacy runner group, and
  recorded acceptable optimized success-path measurements.
- `docs/tasks/000247-statement-public-transaction-cancellation-ownership.md`
  supplied the cancellation-safe `StmtState`, residual rollback ownership,
  fatal retention, and ordinary check-in paths that remain authoritative.

Issue Labels:

- type:task
- priority:high
- codex

Phase 1 left the callback surface public only for this retirement phase.
`Transaction::exec` currently lends `&mut Statement`, `Statement` still owns
normal merge and rollback, `Transaction::stream_stmt` constructs an exported
`StreamStmt`, and the crate root exports both statement facades. The 12 direct
non-streaming methods in `trx/interface.rs` delegate once to that runner, so the
owned conversion can retain one checkout and one settlement path.

The current private catalog runner deliberately differs: it lends the same
reusable `&mut Statement`, always merges complete and partial effects even when
the callback returns an ordinary error, and relies on later whole-private-
transaction rollback. Catalog DDL uses that capability for repeated column,
index, and index-column inserts; table- or index-scoped delete loops; and the
delete-plus-insert metadata replacement used by create-index.

Phase 2 planning supersedes that facade split. There will be no reusable normal
or catalog statement. Both public and private runners receive one owned
`Statement`, every high-level operation consumes it, and an ordinary private
catalog failure rolls back the current statement before returning just like an
ordinary user-table failure. Earlier successful catalog statements remain in
transaction effects and are reversed by enclosing DDL rollback. Purpose-built
catalog batch DML may still mutate several rows through one consumed statement,
analogous to public batch insert.

This revision requires semantic edits to RFC-0029. Replace its
`CatalogStatement` and private merge-on-error decisions in the Summary,
Decisions 1, 2, and 6, migration rules, test strategy, Phase 2 contract,
consequences, and open-question resolution. Phase 1 prerequisites and results
do not change, and there is no following implementation phase whose
prerequisites need revision. During `$task-resolve`, synchronize the final Phase
2 task path, issue, status, implementation summary, and backlog outcome; RFC
program resolution remains a separate `$rfc-resolve` step.

Phase 1 recorded 66 retained legacy storage-test calls across 48 annotated
runner/helper groups. Their subjects include exact statement numbering, raw
effects and redo, callback error and panic injection, checkout and cancellation,
rollback interruption and fatal retention, logical-lock acquisition, raw table
and MemTable access, same-statement invariants, private catalog composition, and
validation bypass. Phase 2 must replace each group semantically rather than
preserving a compatibility facade.

## Goals

- Make direct `Transaction` methods the sole public no-op, read, DML, and stream
  statement boundary.
- Use one owned internal `Statement` capability for public and private
  transactional table operations.
- Make every normal user-table and catalog-table operation consume
  `Statement`, so a second operation cannot be issued through the same
  capability.
- Move success merge, ordinary-error rollback, fatal retention, and cancellation
  settlement out of `Statement` and into carrier-owned completion paths.
- Apply rollback-before-return to private catalog statements as well as public
  statements, preserving the initiating error only after complete rollback.
- Preserve index-before-row rollback, redo discard, residual ownership, poison,
  and fatal-error precedence on every runner path.
- Preserve intentional catalog multi-row work through narrowly purpose-built
  consuming batch DML where one logical statement is required.
- Remove `Transaction::exec`, `Transaction::stream_stmt`, `Statement`, and
  `StreamStmt` from the public API without a compatibility shim.
- Preserve caller-controlled DML validation through transaction-level
  `Transaction::disable_dml_validation(bool)`, with validation enabled by
  default and the selected setting applied to later direct and streaming
  operations until changed again.
- Migrate all production code, examples, benchmarks, public documentation, and
  all 48 retained Phase 2 test groups without reducing relevant behavior
  coverage.
- Preserve Phase 1 direct-operation and successful-path performance
  characteristics and the existing benchmark measurement contracts.
- Satisfy backlog 000186 and leave it ready for implemented closure during task
  and RFC resolution.

## Non-Goals

- No persisted table, index, catalog, redo, checkpoint, or recovery-format
  change.
- No change to MVCC visibility, transaction commit/rollback atomicity, DDL
  publication order, logical-lock lifetime, or recovery protocol.
- No public callback compatibility adapter, deprecation period, or alternate
  caller-selected statement result channel.
- No reusable normal or catalog statement facade, including a test-only facade
  that can invoke arbitrary repeated operations.
- No generic heterogeneous catalog mutation list or arbitrary mixed-DML batch.
  Add only the purpose-built catalog batch operations required by existing
  logical statement boundaries.
- No new public update, delete, or upsert batch family.
- No change to row-decision callbacks inside full-table and index-driven
  mutation; those remain inputs to one consuming DML operation.
- No change to stream exhaustion, error, drop, constructor cancellation, or
  transaction-reuse semantics.
- No statement-level validation flag or per-operation callback escape hatch.
  Validation control is transaction-local; recovery keeps its existing
  explicit no-transaction validation policy.
- No new compile-fail harness solely to prove removed exports or consuming
  receivers.
- No new unsafe code, heap-owned statement facade, successful-path shared lock,
  notification, queue send, or second checkout.

## Plan

1. Amend RFC-0029 before relying on the revised Phase 2 contract.
   - Remove the selected reusable `CatalogStatement` capability and all claims
     that private catalog ordinary errors merge the current statement for later
     cleanup.
   - Specify one owned `Statement` for both public and private internal runners,
     consuming user and catalog operations, rollback-before-return for both,
     and purpose-built consuming catalog batch DML.
   - Update migration rules, structural acceptance, Phase 2 choices/scope/
     goals/non-goals/after-state, consequences, and open questions without
     changing Phase 1's completed record.

2. Convert normal statement execution to an owned one-shot boundary.
   - Keep the existing `exec` name as a transaction-module-private helper with
     the conceptual signature:

     ```rust
     async fn exec<T, F>(&mut self, operation: F) -> Result<T>
     where
         F: for<'stmt> AsyncFnOnce(Statement<'stmt>) -> Result<T>;
     ```

   - Restrict `Statement` to the transaction module. Its fields remain private,
     and every no-op, read, single-row DML, batch insert, and mutation operation
     takes `self` or `mut self` by value.
   - Add a consuming internal `Statement::noop(self)` and route public
     `Transaction::noop()` through it rather than accepting an ignored facade.
   - Remove `Statement::disable_dml_validation` and expose
     `Transaction::disable_dml_validation(bool)` instead. New transactions
     validate by default. Each later direct non-streaming or streaming
     operation uses the transaction's current setting, and the caller may
     restore validation with `disable_dml_validation(false)`.
   - Keep each method in `trx/interface.rs` as one engine-controlled invocation
     returning exactly the consuming operation's result. No wrapper may perform
     result substitution or invoke a second statement operation.

3. Move settlement authority out of the consumed facade.
   - Refactor public `StmtState` so an owned `Statement` borrows its checkout,
     attachment, and `StmtEffects` only for the operation future. End the
     operation future and facade before settlement regains the carrier.
   - On success, merge row undo, index undo, and redo into transaction effects,
     ordinarily return the checkout, and return the operation value.
   - On an operation error, settle deferred index-update ownership, roll back
     secondary-index effects in reverse order, roll back row effects in reverse
     order, clear redo after successful rollback, ordinarily return the
     checkout, and only then return the initiating error.
   - Factor the mechanical merge and rollback implementation so public and
     private carriers share it without putting settlement methods back on
     `Statement`.
   - If rollback fails, move every remaining effect into fatal retention,
     poison storage, discard the transaction through the existing fatal path,
     and return Fatal in precedence to the initiating error.
   - Preserve current public cancellation semantics: unpolled drop performs no
     checkout; checked-out operation or rollback cancellation first destroys
     pending operation/acquisition state, then folds residual undo into the
     transaction, discards redo, and terminally transfers the transaction to
     whole-transaction cleanup.

4. Give `PrivateTransaction` the same owned operation boundary and ordinary
   settlement contract.
   - Replace borrowed `stage_statement` execution with a private owned runner
     whose engine-controlled operation receives `Statement` by value and whose
     result remains `RuntimeOrFatalResult<T>`.
   - Use a private statement carrier over the continuously held private checkout.
     It allocates one `StmtNo` and `StmtEffects`, lends one owned facade, and
     shares the normal merge/rollback mechanics without checking the core
     through the session entry between statements.
   - Successful private operations merge current effects. An ordinary Runtime
     error is returned only after current-statement index and row rollback and
     redo discard. Earlier successful catalog statements remain transaction-
     owned for enclosing DDL rollback.
   - If a private operation panics or is cancelled, destroy its owned operation
     state first, fold remaining undo into the held transaction, discard current
     redo, and preserve the private checkout for supervised mandatory cleanup.
     Resume the original panic only after ownership is complete.
   - Fatal rollback or an already-fatal operation retains residual ownership and
     preserves the existing engine-poison and failed-retained behavior.

5. Replace reusable catalog composition with direct one-shot and batch DML.
   - Catalog storage accessors accept `&mut PrivateTransaction`; they must not
     accept `Statement` or any reusable statement context. Each accessor invokes
     one private direct operation or one purpose-built batch operation.
   - Keep single-row catalog insert and exact-primary-key delete as consuming
     statement operations.
   - Add purpose-built same-table batch insert for the existing column, index,
     and index-column creation groups. Validate the complete internally derived
     input before the first physical insert where applicable, retain input
     order, and roll back the complete successful prefix before returning an
     error.
   - Keep table- or index-scoped catalog delete helpers as one consuming batch
     DML when they intentionally delete several exact catalog rows. Candidate
     discovery and repeated physical deletion remain internal to that operation;
     return the existing deleted-row count and idempotent zero result.
   - Replace create-index's same-table delete-plus-insert sequence with one
     purpose-built consuming metadata-replacement DML that preserves the
     current delete-then-insert physical behavior, stored values, validation,
     redo, and error domains inside one statement boundary.
   - A catalog batch may repeat lower-level row/index primitives inside its
     consumed operation, exactly as public batch insert does. It must not expose
     those primitives as a reusable capability or accept a generic list of
     unrelated catalog commands.
   - Preserve catalog invariant handling and typed `RuntimeOrFatalResult`
     boundaries. Operation or lifecycle failures caused by internally derived
     catalog keys and rows remain invariant violations at the same owning
     boundary.
   - Leave recovery, checkpoint, and other explicitly no-transaction catalog
     reads/writes on their existing lifecycle-specific paths.

6. Retire the public stream and statement facades.
   - Remove `Transaction::stream_stmt` and the crate-root exports of `Statement`
     and `StreamStmt`; continue exporting `Transaction` and
     `IndexScanMvccStream`.
   - Keep stream construction on its specialized crate-private checkout/state
     path because settlement occurs on stream exhaustion, error, or drop.
   - Route only `Transaction::table_index_scan_mvcc_stream` to that path. The
     constructor captures the transaction's current validation setting before
     checkout, matching non-streaming direct operations without exposing a
     stream facade toggle.
   - Preserve unpolled construction, post-checkout constructor cancellation,
     iteration, exhaustion, and drop behavior exactly.

7. Complete production, example, benchmark, and documentation migration.
   - Rewrite `doradb-storage/examples/quick_start.rs` to use direct methods,
     direct streaming, and batch insert for its intentional two-row statement.
   - Retire the redundant standalone `weak_handle_baseline` example; retain the
     benchmark crate's `stmt-noop` workload as the lifecycle-only no-op control.
   - Rewrite `doradb-bench` read, insert, lock-first-touch, stream, and
     `stmt-noop` workloads to matching direct methods. Preserve transaction
     grouping, operation counters, error classification, latency units, and
     workload identities; in particular, do not turn independently measured
     row inserts into one batch merely to simplify migration.
   - Update `README.md` examples to the exact Phase 1 direct signatures and move
     caller assertions or value transformation after the awaited statement.
   - Rewrite callback-facing text in `docs/transaction-system.md`,
     `docs/lock-system.md`, and `docs/error-spec.md`. Keep row-decision callback
     documentation because those callbacks remain part of one mutation DML.
   - Refresh `docs/public-error-audit.csv` with
     `tools/error_audit.rs --write docs/public-error-audit.csv` so removed public
     boundaries disappear and direct `Transaction` methods remain authoritative.

8. Semantically migrate all retained Phase 2 tests.
   - Keep private owned-runner access only in focused transaction-module tests
     for settlement, raw effect injection, exact `StmtNo`, rollback interruption,
     cancellation, lock acquisition, panic, and fatal retention.
   - Convert catalog storage tests to private direct single/batch operations and
     real private settlement semantics. A narrow catalog-specific test harness
     may construct the required mandatory/private ownership, but it must not
     expose a reusable statement callback.
   - Convert raw table, accessor, persistence, rollback, and MemTable tests to
     lower-level `TrxRuntime`/`StmtEffects` fixtures or purpose-built consuming
     test operations. Do not widen production visibility solely for tests.
   - Replace intentional same-statement composition with a purpose-built
     consuming operation that encodes the exact invariant under test.
   - Migrate normal and stream validation-bypass tests from the retired facade
     to the transaction-level toggle. Cover default-on behavior, disabling,
     re-enabling, transaction locality, and explicit recovery validation
     policy.
   - Resolve all 48 retained runner-coverage annotations and remove each one
     only after its replacement preserves or intentionally retires the stated
     behavior.

9. Enforce structural and performance acceptance.
   - Confirm `Statement` is not exported or nameable outside the transaction
     module, no `CatalogStatement` exists, every high-level statement operation
     consumes `self`, and catalog accessors expose only direct private
     transaction methods.
   - Confirm raw `.exec(` calls remain only in direct wrapper implementation and
     focused transaction-module ownership tests; examples, benchmarks, catalog,
     table modules, and public documentation must contain none.
   - Confirm no `.stream_stmt()` call or public `StreamStmt` export remains.
   - Compare optimized owned settlement against the Phase 1 direct baseline for
     no-op, point read, single-row write, index stream, and representative batch
     insert. Use alternating release samples and report distributions; no fixed
     speedup is required, but there must be no repeatable regression outside
     baseline dispersion.
   - Add a representative wide catalog create/drop or equivalent focused
     measurement if implementation changes the number of private statement
     boundaries beyond the purpose-built batches selected above.

10. Run repository-authoritative validation.
    - `rtk cargo check --workspace --all-targets`
    - focused transaction, catalog DDL, catalog storage, cancellation, rollback,
      example, and benchmark tests during development
    - `rtk cargo nextest run --workspace`
    - `rtk cargo nextest run -p doradb-storage --no-default-features --features libaio`
    - `rtk cargo clippy --workspace --all-targets -- -D warnings`
    - `rtk cargo clippy -p doradb-storage --no-default-features --features libaio --all-targets -- -D warnings`
    - `rtk cargo fmt --all -- --check`
    - `tools/style_audit.rs --diff-base origin/main`
    - `rtk git diff --check`

## Implementation Notes

Implemented the owned callback-retirement boundary without resolving the task
document. `Transaction::exec` is transaction-module-private, receives one
owned consuming `Statement`, and leaves merge/rollback/fatal/cancellation
settlement with `StmtState`. `PrivateTransaction` uses the same mechanical
settlement through `PrivateStmtState`, including rollback-before-return for a
Runtime error after a nonempty catalog prefix. Public exports now retain only
`Transaction` and `IndexScanMvccStream` from the transaction statement surface.

Catalog accessors now take `&mut PrivateTransaction`. Creation paths use
same-table insert batches, scoped deletion paths use primary-key batches, and
table metadata replacement uses one delete-then-insert operation. Tests use
the real mandatory/private transaction harness and direct or narrowly
purpose-built transaction operations; setup and assertions live outside the
owned runner operation. All 48 retained runner annotations were removed.
Test-only raw statement operations and imports now live under `stmt::tests`,
and test-only inherent APIs were removed from `Statement`, `StmtEffects`,
`Transaction`, `PrivateTransaction`, and related transaction state types.
Cross-module settlement tests use narrow free functions under `trx::tests`;
validation-policy, poison-wait, and catalog-prefix wrappers that no longer
served a cross-module caller were eliminated in favor of focused tests beside
the production runner. Transition-route rollback coverage now performs a
completed production delete and terminal transaction rollback; its obsolete
callback-only statement cancellation case and abandoned-cleanup hook were
removed. Redundant test-local MemTable forwarding functions and the single-use
private validation getter were also removed after auditing the branch-added
helper surface. A final test audit removed identity-only `&Table` adapters,
duplicate recovery and catalog-index DML helpers, the obsolete
`insert_rows_direct` alias, the pure insert pass-through, and the redundant
`Statement::noop` method. Raw MemTable and transition helpers now derive their
lock identity from the runtime they mutate instead of accepting a separately
supplied table id. Stream construction keeps policy capture and checkout in the
public `Transaction` method while a consuming `StreamStmtState` method owns
table admission, validation, range encoding, cursor setup, and returned stream
state.
Catalog statement operations assert impossible Operation and Lifecycle errors
at each native carrier boundary instead of aggregating them through a synthetic
`QuadResult`; generic table deletes retain their existing `QuadResult` API and
are narrowed immediately by the catalog owner.

The former statement validation opt-out is preserved as
`Transaction::disable_dml_validation(bool)`. Validation starts enabled, the
current transaction setting is copied into each later non-streaming statement
and read before stream checkout, and `false` re-enables validation. Catalog
private operations continue to validate unconditionally.

Optimized release samples alternated the Phase 1 commit and this worktree on
the same aarch64 host. Median latency was approximately 44.9 versus 45.6 ns for
the statement no-op, 294 versus 303 ns for unique point lookup, and 775 versus
763 ns for single-row insert. The sample ranges overlap and show no regression
outside observed dispersion. The comparison also exposed and fixed the
standalone weak-handle harness's synchronous shutdown placement before that
redundant example was retired.

Final validation results:

- `rtk cargo check --workspace --all-targets`: passed without warnings.
- `rtk cargo nextest run --workspace`: 1,735 passed.
- `rtk cargo nextest run -p doradb-storage --no-default-features --features libaio`:
  1,666 passed.
- Strict workspace and libaio Clippy: passed.
- `tools/style_audit.rs --diff-base origin/main`: passed for 28 Rust files.
- Public-error audit refresh, formatting check, and `rtk git diff --check`:
  passed.

## Impacts

- `docs/rfcs/0029-direct-transaction-statement-apis.md`
  - unified owned public/private statement decision
  - catalog batch and rollback-before-return phase contract
  - Phase 2 migration, tests, consequences, and resolution fields
- `doradb-storage/src/trx/mod.rs`
  - public and private owned runners
  - `PrivateTransaction` direct catalog boundary
  - public `stream_stmt` removal
  - focused settlement, cancellation, lock, and fatal tests
- `doradb-storage/src/trx/stmt.rs`
  - transaction-module-private consuming `Statement`
  - shared merge and rollback mechanics
  - public and private carrier settlement
  - consuming user and catalog single/batch operations
- `doradb-storage/src/trx/interface.rs`
  - direct wrappers over consuming operations
- `doradb-storage/src/trx/stream_stmt.rs`
  - crate-private construction using transaction-local validation policy
  - removed public facade and statement-level validation toggle
- `doradb-storage/src/lib.rs`
  - removal of `Statement` and `StreamStmt` exports
- `doradb-storage/src/catalog/storage/ddl.rs`
  - direct private catalog sequencing and purpose-built batch calls
- `doradb-storage/src/catalog/storage/{tables,columns,indexes,table_replay_silent_watermarks}.rs`
  - `PrivateTransaction`-taking single and batch catalog accessors
  - no statement-taking helpers
- Retained runner-test modules:
  - `doradb-storage/src/{engine.rs,recovery/mod.rs,catalog/table.rs}`
  - `doradb-storage/src/catalog/storage/{tables,columns,indexes}.rs`
  - `doradb-storage/src/table/{mod.rs,access.rs,index_mutate.rs,mem_table.rs,persistence.rs,rollback.rs}`
  - `doradb-storage/src/trx/{mod.rs,stmt.rs}`
- Public consumers:
  - `doradb-storage/examples/quick_start.rs`
  - `doradb-bench/src/workload/{read.rs,insert.rs,lock.rs,noop.rs}`
- Documentation and generated audits:
  - `README.md`
  - `docs/{transaction-system.md,lock-system.md,error-spec.md}`
  - `docs/public-error-audit.csv`

The task changes an intentionally unstable public API and private statement
settlement, but does not alter persistent data, redo encoding, recovery,
transaction atomicity, or unsafe invariants. Successful direct statements add
no new allocation or shared coordination. Catalog batch memory and rollback
work remain proportional to the batch size and successful prefix.

## Test Cases

1. An unpolled direct non-streaming future performs no checkout or `StmtNo`
   allocation and leaves the transaction reusable.
2. Every successful direct no-op, read, DML, and batch method obtains one
   statement number, merges only its own effects, checks in ordinarily, and
   preserves the Phase 1 typed result.
3. A public operation error after partial row/index/redo work is observed only
   after deferred ownership settles, indexes roll back before rows, and redo is
   absent; a later direct statement remains usable.
4. Dropping a checked-out public operation, including during index or row
   rollback, discards redo, folds the exact residual suffix into transaction
   effects, terminally cancels the transaction, and lets supervised cleanup
   remove all effects and locks.
5. Public statement rollback failure retains all remaining ownership, poisons
   storage, returns Fatal in precedence, and prevents transaction/session reuse.
6. A successful private catalog single-row statement merges effects into the
   continuously held private transaction without checking the core through the
   session entry.
7. A private catalog operation error after partial effects rolls back the
   current statement completely before returning its initiating Runtime error;
   earlier successful catalog statement effects remain owned until enclosing
   DDL rollback.
8. Private rollback failure or cancellation/panic during operation or rollback
   preserves every residual effect in mandatory cleanup or fatal retention and
   never loses the held private checkout.
9. Catalog batch insert succeeds for one and many rows in input order and
   preserves the intended single statement boundary. Internally invalid derived
   input fails its invariant before the first physical insert; a controlled
   Runtime failure after a nonempty prefix rolls back the whole batch before
   return.
10. Catalog table-, index-, and index-column-scoped batch delete returns exact
    counts, remains idempotent, and rolls back a failed prefix before return.
11. Create-index metadata replacement updates `next_index_no` through one
    consuming operation and restores the old row on statement or enclosing DDL
    rollback.
12. Create/drop table and create/drop index retain current catalog rows, DDL
    redo markers, transaction atomicity, panic supervision, restart recovery,
    and publication ordering under the new private boundary.
13. Direct non-streaming and stream construction validate by default, honor a
    transaction-level validation opt-out for later operations, validate again
    after the caller re-enables validation, and do not carry the opt-out into a
    new transaction. Streaming keeps the exclusive transaction borrow through
    iteration and preserves unpolled construction, post-checkout cancellation,
    exhaustion, iteration error, drop, and later transaction reuse behavior.
14. All 48 retained Phase 2 groups have a direct, lower-level, purpose-built
    consuming, private-catalog, or intentionally retired replacement, with no
    legacy annotation or general test compatibility facade remaining.
15. Crate consumers cannot import `Statement` or `StreamStmt`; examples,
    benchmarks, and README use only direct APIs, and public-error audit contains
    no retired boundary.
16. Structural source review proves both runners pass `Statement` by value,
    every high-level user and catalog operation consumes it, no
    `CatalogStatement` or reusable statement helper exists, and raw private
    runner use is confined to focused transaction ownership tests.
17. Optimized paired measurements show owned settlement remains within baseline
    dispersion for no-op, point read/write, stream, and batch operations, while
    catalog batching avoids an accidental per-row statement-boundary expansion
    for the existing logical batches.
18. Workspace default and alternate-backend tests, all-target checks, strict
    Clippy, formatting, public-error audit, style audit, and diff validation all
    pass.

## Open Questions

None. The unified owned statement model, absence of `CatalogStatement`,
purpose-built catalog batch policy, public and private rollback-before-return
contract, transaction-level validation toggle, stream visibility, migration
boundary, and RFC edits are fixed by the approved task design.
