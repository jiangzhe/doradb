---
id: 000227
title: Remove proven invariants from error variants
status: proposal  # proposal | implemented | superseded
created: 2026-07-17
github_issue: 858
---

# Task: Remove proven invariants from error variants

## Summary

Shrink three internal error boundaries whose failure branches are proven
construction contracts rather than recoverable storage failures. Fixed engine
component topology, exact-sized trusted mutable block views, and LWC scan-buffer
shape will become infallible APIs guarded by documented release assertions.
Their eight obsolete `InternalError` variants and synthetic impossible-state
tests will be removed, while malformed persisted or replayed input will remain
checked at the boundary where it first becomes trusted.

Component construction will also stop forcing the union of all implementation
failures onto every builder. `Component::Error` will preserve infallible,
Runtime-only, Resource-only, and genuinely mixed builders until engine
construction performs public convergence. The pure buffer allocation chain
used by fixed and readonly pools will remain Resource-typed for the same
reason.

The task also distinguishes a nearby case that remains genuinely fallible.
`LwcBlockEncodingInvariant` protects fixed-width persisted-format
representability, including a row-count limit that sufficiently compressible
valid rows can reach before byte capacity is exhausted. Rename it to
`LwcBlockEncodingContract`, preserve its checked behavior, and keep it separate
from the mutable-view invariant removed by this task.

## Context

[`docs/error-spec.md`](../error-spec.md) defines a bottom-up error-boundary
strategy: an impossible state proven by module ownership or lifecycle must be
an assertion or an infallible API, and an error must not be retained merely so
a test can manufacture an unreachable state. The specification already names
component topology as the next construction-layer target and directs trusted
layout, row-vector, and LWC consumers to reassess Internal results after their
preconditions are proven.

Task
[`000226`](000226-establish-runtime-errors-and-harden-foundation-format-boundaries.md)
preserved these branches as typed `InternalResult` paths while the broader
producer/consumer boundary was being established. The current code now makes
three small proof boundaries visible:

| Boundary | Invariant establisher | Trusted consumers | Recoverable boundary that remains |
| --- | --- | --- | --- |
| Component registry and shelf topology | `EngineBuilder::build_inner` drives one fixed registration order; `Supplier<Down>` types each provision edge; `RegistryBuilder` owns the registry and shelf for the whole build | `ComponentRegistry::register` and `dependency`, `ShelfScope::put` and `take`, `RegistryBuilder::finish` | `Component::build -> Result<(), Self::Error>` preserves each implementation's native failure; `EngineBuilder::build_inner` converges across components into public `Error` |
| Mutable LWC and DiskTree block views | `DirectBuf::zeroed` and exact payload slicing establish the runtime length; compile-time assertions equate `LwcBlock`/`BTreeNode` size with their target payload or block size and establish alignment | LWC and DiskTree writers | Generic `layout::try_mut_from_bytes` for caller-neutral/untrusted bytes and persisted readers that map layout mismatch to `DataIntegrityError` |
| LWC scan-buffer shape | `LwcBuilder::new` derives a full `ScanBuffer` and statistics vector from one `TableColumnLayout`; table persistence creates row-page views with the same metadata | `ScanBuffer`, LWC statistics, size estimation, and encoding | Catalog/replay row validation before trusted construction, plus actual LWC format representability failures |

The component proof is reinforced by
[`docs/engine-component-lifetime.md`](../engine-component-lifetime.md) and the
registration-order documentation in `component.rs` and `engine.rs`. A missing
or duplicate component, provision, or finished-builder state therefore means
the fixed engine construction program is incorrect; it is not a condition an
engine caller can recover from.

For mutable block views, immutable persisted input remains a different trust
boundary. Short, corrupt, or structurally invalid bytes must continue to return
`DataIntegrityError` with the caller-neutral `LayoutError` source retained.
Only buffers allocated and sliced by trusted writers move to the asserting
layout API.

For scan shape, production has two entry paths. Table persistence passes a
`PageVectorView` built from the same table metadata used to create the
`LwcBuilder`. Catalog checkpoint construction passes decoded `RowRecord`
values. Those values originate in persisted/replayed state and therefore must
be checked with `validate_catalog_row` for count, kind, and nullability before
entering the builder. This moves the real failure to `DataIntegrity` while
making the downstream shape contract infallible.

The retained encoding variant has three producers in `lwc/mod.rs`:

- `row_count` must fit the persisted `u16` header;
- each cumulative column end offset must fit its persisted `u16` entry; and
- a serialized null-bitmap byte length must fit its `u16` prefix.

These are checked integer-representability conditions, not mutable-view or
scan-shape failures. In particular, highly compressible rows can keep the
estimated payload under the byte limit while accumulating more than
`u16::MAX` rows. The checks must therefore stay fallible in this task, with the
variant and helper renamed from “invariant” to “contract” so the taxonomy does
not claim that a recoverable result is impossible.

Related context:

- [`docs/error-spec.md`](../error-spec.md)
- [`docs/architecture.md`](../architecture.md)
- [`docs/engine-component-lifetime.md`](../engine-component-lifetime.md)
- [`docs/table-file.md`](../table-file.md)
- [`docs/index-design.md`](../index-design.md)
- [`docs/checkpoint-and-recovery.md`](../checkpoint-and-recovery.md)
- [`docs/process/coding-guidance.md`](../process/coding-guidance.md)
- [`docs/process/unit-test.md`](../process/unit-test.md)
- [`backlog 000159`](../backlogs/000159-reassess-invariant-oriented-table-scan-errors.md)
- [`backlog 000160`](../backlogs/000160-harden-domain-specific-fault-injection-critical-workflows.md)

Issue Labels:

- type:task
- priority:medium
- codex

## Goals

1. Replace six fixed component-topology errors with infallible registry/shelf
   APIs and release assertions that identify the component or supplier edge.
2. Give `Component` an associated build error, preserve single-domain and
   infallible implementations through `RegistryBuilder`, and converge only in
   mixed component implementations or engine construction.
3. Add the canonical trusted mutable-layout helper complementary to
   `layout::ref_from_bytes` and `layout::slice_from_bytes_mut`, while preserving
   the checked caller-neutral layout APIs.
4. Make trusted mutable `LwcBlock` and DiskTree node views infallible after
   exact buffer sizing has established their preconditions.
5. Make `ScanBuffer` mutation and the LWC append/statistics/estimate chain
   infallible under the builder-owned layout contract.
6. Validate catalog checkpoint rows completely before the trusted LWC builder
   so malformed durable values remain `DataIntegrityError::InvalidPayload`.
7. Remove exactly these obsolete `InternalError` variants:
   `ComponentShelfDuplicateProvision`, `ComponentShelfNotEmpty`,
   `ComponentRegistryMissing`, `ComponentProvisionMissing`,
   `EngineComponentAlreadyRegistered`, `EngineComponentMissingDependency`,
   `MutableBlockViewMismatch`, and `ColumnScanShapeMismatch`.
8. Rename `LwcBlockEncodingInvariant` and its construction helper to
   `LwcBlockEncodingContract` without weakening its checked casts or changing
   its error domain.
9. Remove tests that fabricate the retired impossible states and do not replace
   them with panic-expectation tests; retain normal-path and real boundary
   coverage.
10. Document each invariant where it is established and consumed, including
   explicit `# Panics` contracts on asserting crate-visible APIs.
11. Update the normative error specification and short-form coding guidance so
    deliberate contract assertions are distinguished from incidental runtime
    `unwrap`/`expect` calls.
12. Keep the change task-sized, format-compatible, and limited to the selected
    proof boundaries plus direct component-error signature fallout.

## Non-Goals

- Remove `ErrorKind::Internal`, eliminate `InternalError` or `InternalResult`,
  or perform a workspace-wide error taxonomy sweep.
- Convert or rename unrelated invariant-oriented errors in buffer, file, CoW,
  index rewrite, recovery, worker-completion, transaction, or concurrency code.
- Resolve `ActiveTransactionDiscarded`, `PoolGuardMissing`, `RowPageMissing`,
  row-page cursor, or block-index reachability questions tracked by
  [backlog 000159](../backlogs/000159-reassess-invariant-oriented-table-scan-errors.md).
- Add or redesign fault injection for critical workflows tracked by
  [backlog 000160](../backlogs/000160-harden-domain-specific-fault-injection-critical-workflows.md).
- Convert `LwcBuilderMisuse` or the remaining LWC empty-build, encoding-width,
  encoding-capacity, or prepared delete-bitmap checks to assertions.
- Change the `LwcBlockEncodingContract` domain, redesign block splitting around
  the `u16` row-count limit, or remove its defensive representability checks.
- Change persisted LWC, DiskTree, row-page, catalog, or table-file layouts or
  the bytes emitted for valid inputs.
- Introduce type-state component builders, layout proof types, a general
  component DAG, or another RFC-scale architecture.
- Add public APIs or change the public storage `ErrorKind` classification.
- Add `#[should_panic]` tests for the newly asserted contracts.

## Plan

### 1. Reconfirm the producer inventory and assertion rule

- Start from the eight variants listed in Goals and inventory every producer,
  conversion helper, direct caller, test, and import before editing. Do the
  same for `LwcBlockEncodingInvariant` so the rename covers all retained
  encoding-width branches but does not absorb unrelated LWC errors.
- Apply one decision rule consistently: external, persisted, replayed,
  configuration, resource, or otherwise valid runtime input stays fallible;
  state proven by a trusted constructor, ownership boundary, fixed lifecycle,
  or exact allocation becomes an infallible API with a release assertion.
- Use `assert!`, `assert_eq!`, `expect`, or `unreachable!` only where the local
  invariant documentation identifies the establisher. Do not use
  `debug_assert!` as the sole replacement for a removed error, because release
  builds must not continue after violating a safety/correctness contract.
- Assertion messages must identify the component name, supplier edge, layout
  type/length, or column position needed to diagnose the construction bug.

### 2. Make fixed component topology infallible and preserve build errors

In `doradb-storage/src/component.rs`:

- Change `ComponentRegistry::register<C>` from `Result<()>` to `()` and assert
  that `TypeId::of::<C>()` is absent before publishing the access handle and
  owner. Keep `ComponentRegistry::get<C> -> Option<C::Access>` as the neutral
  optional lookup primitive.
- Change `ComponentRegistry::dependency<C>` from `Result<C::Access>` to
  `C::Access`; a missing or type-inconsistent required dependency must panic
  with `C::NAME` and the fixed registration-order contract.
- Change `Shelf::put`/`ShelfScope::put` from `Result<()>` to `()` and assert
  that a typed `Supplier<Down>` edge is provisioned exactly once.
- Change `Shelf::take`/`ShelfScope::take` from `Option<Provision>` to
  `Provision`; assert that the required upstream provision exists, and retain
  the existing typed downcast assertion. Include both `Up::NAME` and
  `Down::NAME` in edge diagnostics.
- Change `RegistryBuilder::finish` from `Result<ComponentRegistry>` to
  `ComponentRegistry`. Assert that the shelf is empty and expect the builder's
  registry to remain armed until this consuming call. Preserve `Drop` cleanup
  on failed component builds or unwinding.
- Add `Component::Error: Display`, change `Component::build` to return
  `Result<(), Self::Error>`, and make `RegistryBuilder::build<C>` return
  `Result<(), C::Error>` without converting the report merely to log it.
- Use `Infallible` for `EnginePoisoner` and `LockManager`; typed Runtime reports
  for `FileSystemWorkers` and `SharedPoolEvictorWorkers`; typed Resource reports
  for `MetaPool` and `DiskPool`; and crate `Error` for the component builders
  whose own chains genuinely combine unrelated domains: `FileSystem`,
  `IndexPool`, `MemPool`, `Catalog`, `TransactionSystem`, and
  `TransactionSystemWorkers`.
- Add public-error conversion from `Infallible` for generic engine assembly.
  Narrow mmap array initialization, quiescent arena construction, fixed pool
  construction, and readonly pool construction to `ResourceResult`. Keep
  evictable pool construction on crate `Result` because it combines Config,
  Resource, and IO failures.
- Add or update Rustdoc on every changed crate-visible function with its
  invariant and `# Panics` condition. Document why genuine component
  construction failures still cross their associated error boundary.

Propagate the topology and build-error signatures through `engine.rs`,
`engine_poison.rs`,
`buffer/mod.rs`, `buffer/evictor.rs`, `file/fs.rs`, `lock/mod.rs`,
`catalog/mod.rs`, and `trx/sys.rs`. Remove only `?`, `Ok(())`, or test
`unwrap()` plumbing made obsolete by the new return types; preserve all
unrelated build errors and their context.

### 3. Add and consume the trusted mutable-layout API

In `doradb-storage/src/layout.rs`:

- Add `mut_from_bytes<T>(&mut [u8]) -> &mut T` with the same zerocopy bounds as
  `try_mut_from_bytes`. Implement it through the checked primitive plus one
  documented contract assertion, mirroring `ref_from_bytes` and
  `slice_from_bytes_mut`.
- Keep `LayoutError`, `LayoutResult`, `try_ref_from_bytes`,
  `try_mut_from_bytes`, and checked slice functions caller-neutral and
  fallible. They remain the APIs for bytes whose exact layout has not already
  been established.
- Test a valid trusted mutable round trip and keep/add wrong-length coverage on
  the checked primitive. Do not test the trusted helper by deliberately
  violating its precondition.

In the trusted block consumers:

- Rename `LwcBlock::try_from_bytes_mut` to `LwcBlock::from_bytes_mut`, return
  `&mut LwcBlock`, and delegate to `layout::mut_from_bytes`. Its Rustdoc must
  state that callers provide exactly `LWC_BLOCK_PAYLOAD_SIZE` bytes allocated
  for an LWC payload.
- Make `btree_node_from_block_mut` return `&mut BTreeNode` through the trusted
  helper. Document the `DirectBuf::zeroed(DISK_TREE_BLOCK_SIZE)` and compile-time
  `size_of::<BTreeNode>()` equality that establish the contract.
- Update LWC writer/test helpers in `lwc/mod.rs`, `lwc/block.rs`, and
  `buffer/readonly.rs`, plus DiskTree writers/tests, without changing immutable
  persisted validation.
- Remove `?`/`unwrap()` calls that existed only for the trusted cast. Keep
  persisted `try_from_bytes`/`persisted_disk_tree_node` calls checked and keep
  their `LayoutError -> DataIntegrityError` source-preserving adaptation.

### 4. Make scan-buffer and LWC shape contracts infallible

In `doradb-storage/src/row/vector_scan.rs`:

- Change `ScanBuffer::scan` and `ScanBuffer::append_row_values` to return `()`.
  Replace nullability and `ValBuffer`/`ValArrayRef`/`Val` shape errors with
  release assertions or unreachable match arms tied to the layout used by
  `ScanBuffer::new`.
- Make `append_scan_value` infallible. Preserve placeholder insertion for null
  values and all current buffer/bitmap mutation behavior.
- Document that `scan` requires a `PageVectorView` from the same
  `TableColumnLayout`, and that `append_row_values` requires a complete row
  already validated against that layout.

In `doradb-storage/src/lwc/mod.rs`:

- Change `LwcBuilder::append_view` and `append_row_values` from
  `InternalResult<bool>` to `bool`. Keep snapshot/rollback when capacity is
  exceeded (`false`); remove rollback-on-error control flow because shape is no
  longer a recoverable outcome.
- Make `append_view_inner`, `append_row_values_inner`, `scan_page_stats`,
  `scan_row_value_stats`, `estimate_size`, `estimate_columns_size`, and
  `estimate_column_payload` infallible. Assert the builder-owned lengths and
  type pairing at their narrowest consumers.
- In `LwcBuilder::build`, replace the impossible missing scan column with an
  assertion, but retain `InternalResult<DirectBuf>` for `LwcBuilderMisuse` and
  `LwcBlockEncodingContract`.
- Propagate the boolean append signatures through `table/persistence.rs`,
  `catalog/storage/mod.rs`, and affected inline tests without changing the
  existing “fits/does not fit” control flow.

At the catalog trust boundary:

- Replace the length-only preflight in
  `build_lwc_blocks_from_row_records` with `validate_catalog_row` for every
  `RowRecord`. This must validate column count, value kind, and nullability and
  return `DataIntegrityError::InvalidPayload` before constructing or mutating an
  `LwcBuilder`.
- Preserve the existing real errors for invalid row ordering/ranges, a single
  row that cannot fit, and LWC encoding contract failures.

### 5. Rename the retained fallible encoding contract

In `doradb-storage/src/error.rs` and `doradb-storage/src/lwc/mod.rs`:

- Rename `InternalError::LwcBlockEncodingInvariant` to
  `InternalError::LwcBlockEncodingContract` and change its display text so it
  describes an unsatisfied encoding contract rather than an impossible
  invariant.
- Rename `lwc_block_encoding_invariant()` to
  `lwc_block_encoding_contract()` and update the row-count, cumulative-offset,
  and null-bitmap length-prefix producers.
- Attach the affected persisted field and actual/maximum value where that data
  is available. Do not use unchecked narrowing casts, assert these branches,
  or reclassify them as malformed persisted input; they are failures to encode
  trusted in-memory values into the fixed representation.
- Add a normal-API regression using sufficiently many highly compressible
  valid rows to demonstrate that the `u16` row-count contract is reachable
  without mutating private builder state. Verify the renamed Internal context.

### 6. Retire obsolete variants and impossible-state tests

- Delete the eight variants listed in Goals from `InternalError` and delete
  `Error::engine_component_already_registered` and
  `Error::engine_component_missing_dependency` after all callers are
  infallible.
- Remove obsolete `InternalError`, `InternalResult`, `Error`, `Report`,
  `ResultExt`, and `LayoutError` imports only when no other producer in the file
  needs them.
- Remove the component tests that expect duplicate registration, missing
  dependency, or duplicate shelf provision errors. Keep the typed access,
  single provision/take, shelf drain, reverse shutdown, idempotent shutdown,
  and access-before-owner-drop tests, adapting only their return-value syntax.
- Remove the short trusted-mutable half of the combined LWC and DiskTree layout
  adaptation tests. Retain and rename their persisted halves, which still prove
  `LayoutError` is retained beneath `DataIntegrityError`.
- Delete the LWC test that mutates `builder.buffer` to a partial scan set and
  expects `ColumnScanShapeMismatch`. Do not replace it with an assertion test.
- Retain valid all-type vector scans, null-bitmap compaction, truncation,
  capacity rollback, LWC round trips, and persisted malformed-input tests.
- Finish with a zero-match source audit for every removed variant and old LWC
  encoding name.

### 7. Document the established boundaries

In `docs/error-spec.md`:

- Add a normative rule that a proven internal contract is represented by an
  infallible API plus a release assertion, while a condition reachable through
  valid input or runtime state remains a typed result.
- State that tests must exercise the real validation boundary and must not gain
  access to private state solely to synthesize an impossible error.
- Record the three proof boundaries from Context, including which constructor,
  lifecycle, exact allocation, or validation step establishes each invariant.
- Move component topology, trusted mutable block views, and LWC scan shape from
  current debt to established behavior; remove stale statements that they
  retain Internal layout/shape reports.
- Record `LwcBlockEncodingContract` as retained fallible format
  representability, not as a proven invariant, and update the implementation
  snapshot date.

In `docs/process/coding-guidance.md`:

- Replace the absolute “No Panics” shorthand with a precise rule: incidental
  runtime `unwrap`/`expect` remains prohibited, external or valid runtime
  failures remain results, and documented release assertions are appropriate
  for impossible internal contract violations.
- Require assertion diagnostics and code-local invariant documentation. Do not
  weaken the guidance into general permission to panic.

### 8. Guard against boundary regressions

- The main risk is asserting a branch that untrusted or valid runtime input can
  reach. Protect against this by keeping generic checked layout tests,
  persisted LWC/DiskTree corruption tests, catalog wrong-kind/nullability
  tests, and the reachable LWC encoding-contract regression.
- A second risk is accidentally erasing genuine component-build failures while
  removing topology plumbing. Review every changed `Component::build`
  implementation and ensure configuration, allocation, IO, spawn, recovery,
  and startup results retain their narrowest type until a genuine mixed or
  public convergence boundary.
- A third risk is changing block bytes while refactoring view acquisition.
  Preserve existing LWC/DiskTree round trips and checksum/structure validation;
  this task changes only how trusted slices are borrowed.
- Keep assertion sites narrow and owned. Do not add assertion wrappers at an
  outer public boundary when the lower constructor or validator can establish
  the contract more directly.

## Implementation Notes

## Impacts

Primary invariant and error-definition modules:

- `doradb-storage/src/error.rs`: remove eight variants and two component error
  constructors; rename the retained LWC encoding contract variant; convert
  `Infallible` at the public engine-build boundary.
- `doradb-storage/src/component.rs`: infallible registry, shelf, dependency,
  and finish APIs; associated build errors; component topology tests; and
  invariant documentation.
- `doradb-storage/src/layout.rs`: canonical trusted mutable value view and
  caller-neutral checked-layout coverage.
- `doradb-storage/src/row/vector_scan.rs`: infallible scan-buffer shape
  operations.
- `doradb-storage/src/lwc/block.rs` and `doradb-storage/src/lwc/mod.rs`:
  infallible trusted mutable view, boolean append chain, asserted shape helpers,
  retained/renamed encoding contract, and tests.
- `doradb-storage/src/index/disk_tree.rs`: infallible trusted mutable node view
  while preserving persisted validation.

Direct component-signature consumers:

- `doradb-storage/src/engine.rs`
- `doradb-storage/src/engine_poison.rs`
- `doradb-storage/src/buffer/mod.rs`
- `doradb-storage/src/buffer/evictor.rs`
- `doradb-storage/src/file/fs.rs`
- `doradb-storage/src/lock/mod.rs`
- `doradb-storage/src/catalog/mod.rs`
- `doradb-storage/src/trx/sys.rs`

Other direct layout/LWC consumers:

- `doradb-storage/src/buffer/readonly.rs`
- `doradb-storage/src/buffer/arena.rs`
- `doradb-storage/src/buffer/fixed.rs`
- `doradb-storage/src/buffer/util.rs`
- `doradb-storage/src/table/persistence.rs`
- `doradb-storage/src/catalog/storage/mod.rs`

Documentation:

- `docs/error-spec.md`
- `docs/process/coding-guidance.md`
- `docs/engine-component-lifetime.md` only if implementation reveals stale
  topology wording; no lifecycle redesign is intended.

There is no public API or persisted-format change. Internal return-type changes
deliberately reduce error propagation and may require mechanical edits in
inline tests or direct callers not listed separately above.

## Test Cases

1. Build the production engine component sequence and verify successful
   construction still publishes every dependency used by `EngineInner`.
2. Verify `ComponentRegistry::get` and infallible `dependency` return the
   expected typed cloned access after normal registration.
3. Verify one typed shelf provision is consumed by its declared downstream
   component and the shelf is empty when `RegistryBuilder::finish` returns.
4. Verify an infallible component builds and finishes normally. Verify a
   failing fixture remains a typed Runtime report through `RegistryBuilder`,
   converts losslessly at the public boundary, and still runs shutdown before
   owner drop. Keep reverse shutdown order, shutdown idempotence, and
   access-handle drop ordering unchanged.
5. Use `layout::mut_from_bytes` with an exact mutable value buffer, mutate the
   typed view, and verify the underlying bytes. Keep a separate checked
   `try_mut_from_bytes` wrong-length test returning `LayoutError::Mismatch`.
6. Build valid LWC and DiskTree blocks through their trusted mutable writer
   paths and verify their existing round-trip, checksum, and structure tests.
7. Feed short or malformed persisted LWC and DiskTree bytes through immutable
   readers and verify `DataIntegrityError::InvalidPayload` retains the
   `LayoutError::Mismatch` source where applicable.
8. Exercise row-page vector scanning across every supported value kind,
   nullable/non-nullable columns, deleted-row compaction, variable bytes, and
   truncation after the infallible signature change.
9. Exercise `LwcBuilder::append_view` and `append_row_values` success and
   capacity overflow. Verify `false` rolls the buffer, row IDs, and statistics
   back to the previous snapshot.
10. Build and decode valid LWC blocks from row-page views and decoded row values
    and verify row count, values, null bitmap, shape fingerprint, and encoded
    bytes remain unchanged.
11. Pass a catalog `RowRecord` with the correct value count but wrong value kind
    and another with invalid nullability. Verify each fails before builder
    mutation as public `DataIntegrityError::InvalidPayload`, with column context.
12. Accumulate more than `u16::MAX` highly compressible valid rows through
    `LwcBuilder`'s normal append API while staying under its estimated byte
    capacity, then verify `build` returns
    `InternalError::LwcBlockEncodingContract`. Do not mutate private fields to
    arrange this case.
13. Verify other retained LWC contract/misuse tests use
    `LwcBlockEncodingContract` or `LwcBuilderMisuse` as appropriate and that no
    removed shape/layout variant remains inspectable.
14. Construct a too-small global readonly pool directly and verify its report
    remains `ResourceError::BufferPoolSizeTooSmall` before public convergence.
15. Run source audits from the worktree root:

    ```bash
    rg -n \
      'ComponentShelfDuplicateProvision|ComponentShelfNotEmpty|ComponentRegistryMissing|ComponentProvisionMissing|EngineComponentAlreadyRegistered|EngineComponentMissingDependency|MutableBlockViewMismatch|ColumnScanShapeMismatch' \
      doradb-storage/src docs/error-spec.md docs/process/coding-guidance.md

    rg -n 'LwcBlockEncodingInvariant|lwc_block_encoding_invariant' \
      doradb-storage/src docs/error-spec.md
    ```

    Both commands must produce no matches. Separately verify
    `LwcBlockEncodingContract` has only the three intended encoding-width
    producer classes and their tests/documentation.
16. Run focused nextest coverage for component, layout, vector-scan, LWC,
    DiskTree, table-persistence, and catalog-storage tests, followed by:

    - `cargo fmt --all -- --check`
    - `cargo clippy --workspace --all-targets -- -D warnings`
    - `cargo nextest run --workspace`
    - `cargo nextest run -p doradb-storage --no-default-features --features libaio`
    - `git diff --check`

No test may mutate a private production object into an impossible state merely
to observe a removed error, and no assertion test replaces the deleted
synthetic coverage.

## Open Questions

None for this task. Harder reachability decisions remain explicitly owned by
backlogs 000159 and 000160 and are not prerequisites for implementation.
