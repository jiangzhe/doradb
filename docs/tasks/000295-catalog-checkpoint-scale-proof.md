---
id: 000295
title: Catalog Checkpoint Scale Proof
status: proposal
created: 2026-09-04
github_issue: 1043
---

# Task: Catalog Checkpoint Scale Proof

## Summary

Implement the revised RFC-0031 Phase 8 scale proof as an end-to-end
`doradb-bench` workload that prepares and mutates the catalog exclusively
through supported public storage APIs. The benchmark covers deterministic
small, target-envelope, and above-envelope catalogs, then measures one public
managed CREATE TABLE, managed CREATE INDEX, or DROP TABLE effect through a
normal catalog checkpoint.

Return a compact, generally useful checkpoint report from
`Session::checkpoint_catalog()` for checkpoint-specific row and block-byte
accounting. Combine that report with the benchmark's existing public
buffer-pool and storage-I/O snapshots, elapsed time, and a benchmark-local
sampled process-RSS peak. Do not add a feature gate, benchmark-only storage
module, raw catalog mutation API, or crate-private access from `doradb-bench`.

This task proves the target envelope for operations that the public API
actually supports. It does not claim isolated binding-only or descriptor-only
mutation costs because those mutations are intentionally absent from the
public contract.

## Context

Parent RFC:

- `docs/rfcs/0031-compact-numeric-catalog-table-definitions.md`, Phase 8

Issue Labels:

- type:task
- priority:high
- codex

RFC-0031 Phases 4, 6, and 7 are complete. They provide the final projected
parent validator, one managed descriptor row per managed table, and roleless
bindings created and removed with their managed table. Phase 8 is the RFC's
final implementation phase, so there is no following phase whose prerequisites
need preservation.

The current checkpoint model rewrites the complete compact image of every
logical catalog table with a net folded change. The implementation loads the
old image, folds redo by primary key, clones surviving values into a complete
output row set, builds all replacement LWC pages, writes a replacement index,
and publishes a new multi-table root. This is expected to be linear in the
changed logical tables' live rows and value bytes, but the clone and page
buffering constants have not been measured at the RFC's 10,000-table,
100,000-binding, 64-MiB-descriptor envelope.

The original Phase 8 text requires isolated binding-row, descriptor-payload,
and combined mutations. That matrix cannot be produced by a normal external
client:

1. `ManagedTableOps::create_managed_table` inserts the numeric definition,
   descriptor, and all initial bindings atomically.
2. `Session::drop_table` removes the complete definition, descriptor, and all
   bindings.
3. `ManagedTableOps::create_managed_index` and `drop_managed_index` replace the
   descriptor only as part of a physical index-schema change.
4. Phase 7 intentionally exposes no post-CREATE binding operation and no
   descriptor-only ALTER operation.

`doradb-bench` is a separate crate and must not bypass that boundary with
crate-private catalog rows, replay batches, roots, or block writers. The
approved Phase 8 adjustment therefore replaces the synthetic mutation matrix
with representative public DDL cases and updates RFC test 20 and the Phase 8
plan before the task is resolved.

The benchmark already captures public `StorageIoStats` and `BufferPoolStats`
snapshots around a measured phase. Those diagnostics describe engine-wide
physical activity and cache behavior, but they do not expose the
cache-independent compact-image volume or identify which catalog tables were
rebuilt. `Session::checkpoint_catalog()` currently discards its internal
`CatalogCheckpointOutcome`; a small operation report can expose the missing
checkpoint-owned facts without exposing mutable catalog internals.

## Goals

1. Add a normally compiled catalog-checkpoint benchmark whose storage
   mutations use only `ManagedTableOps` and existing public `Session` DDL and
   checkpoint operations.
2. Build deterministic small, target, and stress catalog profiles with exact
   table, column, binding, descriptor-row, and descriptor-payload totals.
3. Measure public managed-table creation, managed-index creation with a
   same-length replacement descriptor, and managed-table drop from equivalent
   populated and checkpointed starting images.
4. Return a documented `CatalogCheckpointReport` containing the checkpoint
   outcome and deterministic per-logical-catalog-table row and compact-block
   accounting gathered by the normal checkpoint path.
5. Report elapsed time, sampled process peak RSS above the pre-checkpoint
   baseline, checkpoint-owned block bytes, and existing public buffer-pool and
   storage-I/O deltas without a custom global allocator.
6. Demonstrate that every target-profile case completes without OOM and that
   compact-image, write, and observed memory growth are consistent with the
   documented linear full-image model.
7. Keep the stress profile informational, add no wall-clock CI threshold or
   test-runner timeout, and record the execution environment and matrix results
   in the Phase 8 implementation summary.
8. Update RFC-0031 test 20 and Phase 8 to the approved public-API-only
   acceptance contract.

## Non-Goals

1. Add standalone binding insert, delete, rename, alias, or retarget APIs, or a
   descriptor-only ALTER API.
2. Add a benchmark support module, public raw-row mutation primitive, replay
   injection hook, catalog root writer, or access to crate-private catalog
   types.
3. Feature-gate the workload, checkpoint measurement, process-memory sampler,
   or any storage API used by the benchmark.
4. Claim that a public operation isolates the binding table or descriptor
   table. Managed CREATE and DROP necessarily change both, while managed index
   DDL necessarily changes numeric index metadata with the descriptor.
5. Provide exact allocator-requested live heap accounting. The scoped memory
   result is a sampled process-RSS measurement and is labeled accordingly.
6. Treat the embedded allocation-map payload as a separate physical write from
   its containing metadata page or double-count it in total bytes.
7. Introduce incremental checkpoints, base-plus-delta persistence, an on-disk
   format change, or a new recovery path.
8. Guarantee checkpoint performance above the target envelope, add automatic
   benchmark execution to routine CI, or change `.config/nextest.toml`.
9. Generalize the benchmark fixture model beyond the state required for this
   terminal single-checkpoint workload.

## Rejected Alternatives

### Feature-Gated Benchmark Internals

A Cargo feature exposing internal mutation or accounting hooks would make the
benchmark build differ from the product build and would leave the target path
unexercised by normal workspace compilation. The approved workload and metrics
are always compiled and require no `--features` argument.

### Benchmark-Specific Public Storage Support Module

An always-built helper that accepts an opaque fixture token and injects
catalog-row changes could reproduce the original isolated matrix, but it would
still add a second mutation authority and a synthetic durable-root lifecycle.
Phase 8 instead measures only supported client behavior.

### New Product Binding And Descriptor Mutation APIs

Standalone mutation APIs would make the original three cases expressible, but
they would reverse Phase 7's deliberate CREATE/DROP-only binding lifecycle and
descriptor-with-index-DDL contract. Such APIs require their own product
semantics and are not justified by a benchmark.

### Incremental Or Base-Plus-Delta Checkpointing

Changing persistence could reduce write amplification, but it would change
the durable format, recovery, compaction, and failure-atomicity model. Phase 8
measures and, if necessary, reduces constants within the complete-image model.

## Plan

### Parent RFC Contract Sync

1. Revise RFC-0031 test 20 and the Phase 8 scope, validation, and phase-local
   choices before resolving this task:
   - replace binding-only, descriptor-only, and combined synthetic changes
     with managed CREATE TABLE, managed CREATE INDEX, and DROP TABLE cases;
   - state that each mutation is issued through the normal public API from an
     equivalent deterministic baseline;
   - replace exact allocator-scoped heap growth with sampled process peak RSS
     above the immediate pre-checkpoint baseline;
   - use the checkpoint report for logical compact-image and successful write
     bytes, and existing public runtime snapshots for physical I/O and buffer
     behavior;
   - attribute the allocation map to metadata because it is serialized into
     the same physical page;
   - remove any claim that the benchmark isolates binding-only or
     descriptor-only cost.
2. Preserve the fixed target envelope, small/target/stress structure, target
   OOM gate, full-image linear-growth review, informational stress status, and
   prohibition on wall-clock CI thresholds.
3. Preserve the existing catalog scale contract and complete-image durability
   model. The scope change affects how Phase 8 obtains evidence, not the
   checkpoint algorithm or format.

### Public Checkpoint Report

1. Add public, documented report types under the catalog/checkpoint domain and
   re-export them from `doradb-storage/src/lib.rs`. The intended shape is:

   ```rust
   pub struct CatalogCheckpointReport {
       pub outcome: CatalogCheckpointOutcome,
       pub catalog_ddl_txn_count: usize,
       pub table_changes: Box<[CatalogTableCheckpointChange]>,
       pub table_io: Box<[CatalogTableCheckpointIoStats]>,
       pub metadata_bytes_written: usize,
   }

   pub struct CatalogTableCheckpointChange {
       pub table_id: TableID,
       pub before_row_count: usize,
       pub after_row_count: usize,
   }

   pub struct CatalogTableCheckpointIoStats {
       pub table_id: TableID,
       pub compact_bytes_read: usize,
       pub final_compact_bytes: usize,
       pub lwc_bytes_written: usize,
       pub index_bytes_written: usize,
   }
   ```

2. Change `Session::checkpoint_catalog()` from `Result<()>` to
   `Result<CatalogCheckpointReport>`. Keep `CatalogCheckpointOutcome` as the
   publication/no-op classification inside the report. Update workspace
   callers that intentionally discard the richer success value.
3. Keep `checkpoint_catalog_and_truncate_redo_log()` and
   `CatalogRedoMaintenanceOutcome` behavior unchanged in this task; the
   combined operation may project the shared internal report back to its
   existing `CatalogCheckpointOutcome` field. The benchmark measures the
   standalone checkpoint operation.
4. A published report separates logical changes from measured I/O. The
   `table_changes` array contains only tables whose final logical state differs
   from the durable base image, with row counts taken directly from the loaded
   base rows and materialized output rows. Equal before/after counts remain a
   change when row values changed. Row IDs are logical identifiers, may be
   sparse, and are never treated as cardinalities. The `table_io` array contains
   only tables with nonzero compact reads, LWC writes, or index writes;
   `final_compact_bytes` accompanies such an entry but does not cause one by
   itself. Both arrays use increasing `TableID` order. A no-op report has both
   arrays empty because the no-op path must not add an observability-only
   reachability traversal.
5. Define `compact_bytes_read` as cache-independent logical catalog-table block
   volume requested by the checkpoint, including base-image folding and the
   normal final reachability traversal. Count each logical access even when the
   readonly cache serves it. Define LWC, index, and metadata writes as bytes
   successfully written by the checkpoint before publication. Count the
   metadata page and super-root slot in `metadata_bytes_written`; do not add the
   embedded allocation-map payload a second time.
6. Derive `final_compact_bytes` while the existing allocation-map/reachability
   traversal visits the published roots. Reuse that traversal rather than
   adding an observability-only read pass.
7. Thread a small internal measurement accumulator through checkpoint scan,
   per-table fold/build, index construction, reachability collection, and
   metadata commit. Measurement must not alter row ordering, error domains,
   failure atomicity, cache behavior, or the I/O backend abstraction.
8. Return a report only after successful terminal completion. A `Noop` report
   has no rewritten-table or metadata-write bytes; a failed checkpoint returns
   the existing typed error and does not manufacture a success report.
9. Keep the report independent of benchmark concepts such as profiles, cases,
   RSS, or result formatting. No raw rows, values, redo operations, roots,
   buffers, or writers cross the public boundary.

### Deterministic Catalog Profiles

1. Add serde-facing and resolved `CatalogCheckpointProfile` values with these fixed
   baselines:

   | Profile | User tables | Columns | Indexes | Bindings | Descriptor rows | Descriptor bytes |
   | --- | ---: | ---: | ---: | ---: | ---: | ---: |
   | Small | 1,000 | 2,000 | 0 | 10,000 | 1,000 | 6,710,886 |
   | Target | 10,000 | 20,000 | 0 | 100,000 | 10,000 | 67,108,864 |
   | Stress | 12,500 | 25,000 | 0 | 125,000 | 12,500 | 83,886,080 |

2. Give each baseline table two fixed columns, no secondary index, ten unique
   deterministic 16-byte bindings, and one deterministic descriptor. Reserve
   one empty-descriptor table as the DROP probe; distribute the profile's exact
   payload total across the remaining descriptors with lengths differing by at
   most one byte. Verify every row remains below
   `MAX_TABLE_DESCRIPTOR_BYTES`.
3. Use fixed namespace IDs and an injective binary key encoding derived from
   the table ordinal and binding ordinal. Generate descriptor bytes from a
   fixed seed and ordinal so runs are reproducible without retaining all
   payloads in the fixture state.
4. Retain only the designated DROP and index probe IDs plus aggregate expected
   counts in the benchmark fixture summary. Do not assume storage-assigned IDs
   from creation order when selecting later public operations.

### Public Operation Cases And Plan Lifecycle

1. Add a normal `catalog-checkpoint-prepare` workload accepted only in a prepare
   phase. Its strict plan fields are `profile` and `case`, where `case` is one
   of `managed-create`, `managed-index-create`, or `managed-drop`.
2. In that unmeasured prepare phase:
   - create the complete baseline with
     `ManagedTableOps::create_managed_table` and a benchmark-owned deterministic
     `ManagedTableInterpreter`;
   - run and verify one baseline `Session::checkpoint_catalog()`;
   - apply exactly one selected public mutation, leaving its redo pending for
     the final checkpoint.
3. Define the cases as follows:
   - `managed-create` adds one two-column managed table with ten new bindings
     and an empty descriptor;
   - `managed-index-create` creates one non-unique secondary index on the
     designated surviving probe and supplies a deterministic, different,
     equal-length replacement descriptor;
   - `managed-drop` calls `Session::drop_table` for the designated
     empty-descriptor probe.
4. The empty CREATE/DROP probe payload and equal-length index replacement keep
   the profile's descriptor payload bytes constant across the pending change.
   Table, satellite, and binding row counts still reflect the real operation
   and are recorded before and after it.
5. Add a `catalog-checkpoint` workload accepted only as the final
   benchmark phase. It requires the pending catalog-checkpoint fixture, uses one
   session and one operation, rejects warmups or multiple measured runs, and
   calls `Session::checkpoint_catalog()` exactly once.
6. Mark both workloads as normal members of the closed `WorkloadSpec`,
   `ResolvedWorkload`, fixture requirement/effect, executor dispatch, latency
   unit, and result schemas. Add a normal TOML template under
   `doradb-bench/templates/`; no Cargo feature controls parsing or execution.
7. Require a fresh storage root for each profile/case point, as the benchmark
   invocation already requires. The deterministic generator makes all three
   case baselines for one profile logically equivalent before their selected
   pending mutation.

### Scoped Measurement And Output

1. Start a benchmark-local RSS sampler after the prepare phase and immediately
   before the measured checkpoint. Capture current process RSS as the baseline,
   synchronize sampler readiness, sample current RSS through Linux procfs every
   one millisecond, take one final synchronous sample after the checkpoint, and
   then stop and join the sampler.
2. Report baseline RSS, sampled peak RSS, and saturating peak-above-baseline
   bytes. Label the result `sampled_process_rss`; do not describe it as exact
   heap allocation, allocator demand, or kernel-independent memory.
3. Treat unavailable or malformed procfs memory data as a clear benchmark
   error instead of silently omitting the required measurement. Keep the
   sampler always compiled in the normal Linux benchmark build.
4. Force existing public engine-diagnostic capture for the measured workload.
   Because the selected DDL occurs in the preceding prepare phase, the
   `StorageIoStats` and `BufferPoolStats` deltas surround only the checkpoint
   execution and its normal mandatory-runtime completion.
5. Extend `WorkloadMetrics` and result serialization for the profile, case,
   before/final catalog cardinalities, descriptor bytes, RSS values, and full
   `CatalogCheckpointReport`. Remove `Copy` assumptions from the metrics type
   if required by the variable-length deterministic table report; retain
   strict serde round trips and stable table ordering.
6. Record raw integer byte/count values. Compute display-only totals and
   amplification from raw numerators and denominators without replacing the raw
   evidence. The changed tables' final compact bytes are the write-amplification
   denominator; aggregate results must not imply isolated binding or descriptor
   attribution.
7. Record profile, case, build mode, backend, engine configuration, OS/kernel,
   CPU, available memory, and storage medium with every manual matrix result.

### Scale Evaluation And Bounded Optimization

1. Run all three public cases for small and target profiles in release mode.
   Every target case must complete without OOM. Compare the report's changed
   row counts, compact-byte totals, and sampled RSS by the same case across
   profiles.
2. Run all three stress cases as informational evidence. A stress failure does
   not fail the target contract, but its command, environment, and outcome must
   be recorded.
3. Treat block-byte growth outside fixed page-rounding and metadata overhead,
   or a material unexplained superlinear RSS trend, as an investigation rather
   than hiding it with a wall-clock threshold.
4. If the target OOMs or the full-image implementation's memory constants make
   the target untenable, apply only these bounded optimizations:
   - consume folded rows in primary-key order instead of cloning every
     surviving `Vec<Val>` and outlined `VarByte` payload;
   - build and write replacement LWC pages incrementally while retaining only
     the compact index-entry shapes required to build the replacement index.
5. Preserve complete final-image materialization semantics, complete LWC/index
   replacement, CoW failure atomicity, error classification, and durable
   formats. Rerun the complete matrix after an optimization.
6. If the target still fails, leave Phase 8 incomplete and require a separate
   design decision. Do not introduce base-plus-delta persistence under this
   task.

## Implementation Notes

Implemented the public checkpoint report and normally compiled benchmark
workloads. The measured path uses only `ManagedTableOps`, public `Session` DDL,
and `Session::checkpoint_catalog()`. Checkpoint measurement counts successful
logical block reads independently of readonly-cache residency, successful LWC
and index writes at their completion boundaries, final compact bytes during the
existing reachability walk (or mandatory root validation for metadata-only
publication), and the metadata page plus one super-root slot.

The first small-profile run exposed a stale LWC capacity constant: the builder
estimated a 24-byte payload header after the persisted header had grown to 32
bytes. Descriptor-shaped blocks could therefore overrun by eight bytes. The
builder now derives the estimate from `size_of::<LwcBlockHeader>()`, with a
descriptor-density regression test.

Projected parent-integrity and managed-descriptor validation now share one
full decode of `catalog.tables`, `catalog.columns`, `catalog.indexes`, and
`catalog.table_descriptors`. Replay-watermark and binding validation retain
their narrow projected-column scans. This preserves the same pre-publication
integrity boundary while removing one redundant traversal of the shared schema
roots. The stress managed-CREATE logical-read volume fell by exactly 2,787
64-KiB blocks (174.1875 MiB); physical reads and all write metrics were
unchanged.

### Manual Release Matrix

All runs used fresh roots, the default `iouring` backend, and strict plans with
one unmeasured `catalog-checkpoint-prepare` phase followed by one measured
`catalog-checkpoint` operation. `checkpoint write bytes` is the raw sum of
successful LWC, index, metadata-page, and super-root-slot bytes. Physical reads
and writes are the existing `StorageIoStats` table-read and background-write
request deltas.

| Profile | Public case | Elapsed ns | Compact read bytes | Checkpoint write bytes | Sampled RSS above baseline bytes | Physical reads | Physical writes | Write amplification |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Small | managed-create | 42,104,702 | 31,457,280 | 8,028,160 | 7,430,144 | 121 | 124 | 1.012397 |
| Small | managed-index-create | 31,784,045 | 30,932,992 | 7,700,480 | 7,319,552 | 116 | 119 | 1.012931 |
| Small | managed-drop | 40,651,061 | 31,457,280 | 8,028,160 | 7,434,240 | 121 | 124 | 1.012397 |
| Target | managed-create | 435,005,256 | 301,465,600 | 75,530,240 | 77,434,880 | 1,151 | 1,154 | 1.001303 |
| Target | managed-index-create | 606,113,719 | 297,009,152 | 73,236,480 | 75,423,744 | 1,116 | 1,119 | 1.001344 |
| Target | managed-drop | 507,886,373 | 301,465,600 | 75,530,240 | 76,800,000 | 1,151 | 1,154 | 1.001303 |
| Stress | managed-create | 585,144,078 | 376,963,072 | 94,404,608 | 95,678,464 | 1,439 | 1,442 | 1.001042 |
| Stress | managed-index-create | 519,249,721 | 371,195,904 | 91,455,488 | 94,232,576 | 1,394 | 1,397 | 1.001076 |
| Stress | managed-drop | 675,851,343 | 376,963,072 | 94,404,608 | 95,682,560 | 1,439 | 1,442 | 1.001042 |

Every target case completed without OOM. For the same case, target-to-stress
logical reads, writes, physical requests, and sampled RSS growth follow the
1.25x profile-size ratio within page-rounding and sampling variation. The
small-to-target comparison is similarly near the intended 10x ratio after
fixed metadata and page-rounding overhead. No bounded checkpoint
materialization optimization was required.

### Evidence Environment

- Build: `rustc 1.98.0`, Cargo release profile, default `iouring` backend.
- Host: Linux `7.0.14-orbstack-00380-ga7e0a2dc9535`, `aarch64`, 10 single-thread
  vCPUs; the virtualized CPU model was reported as `-`.
- Memory: 12,304,840 KiB total RAM and 13,353,408 KiB swap. The post-matrix
  environment capture reported 9,643,436 KiB available RAM and 13,227,604 KiB
  free swap.
- Storage: workspace `target/` on `/dev/vdb1` Btrfs with SSD and no-data-COW
  mount options, used for all fresh benchmark roots.
- Shared engine configuration: 2 CPU workers, mandatory concurrency 4,
  1-GiB metadata pool, 512-MiB index pool, 1-GiB data pool, 1-GiB readonly
  pool, file I/O depth 64, catalog scan depth 32, 4-KiB redo blocks, `fsync`,
  and 16-GiB CoW-file logical maximum.
- Command shape:
  `target/release/doradb-bench --root target/catalog-checkpoint-fused-<profile>-<case> --plan <strict-profile-case-plan>`.

## Impacts

- `docs/rfcs/0031-compact-numeric-catalog-table-definitions.md`: revise test 20
  and Phase 8's evidence contract, choices, and final implementation summary.
- `doradb-storage/src/catalog/checkpoint.rs` and
  `doradb-storage/src/catalog/storage/mod.rs`: carry scan, fold, compact-block,
  reachability, and publication accounting through the normal checkpoint.
- `doradb-storage/src/catalog/storage/merge.rs` and LWC/index construction:
  conditionally consume rows and stream page production only if scale evidence
  requires the bounded optimization.
- `doradb-storage/src/session/mod.rs`, `doradb-storage/src/catalog/mod.rs`, and
  `doradb-storage/src/lib.rs`: return and export the documented checkpoint
  report while keeping catalog mutation authority private.
- `doradb-bench/src/plan.rs`, `fixture.rs`, `workload/`, and
  `plan_executor.rs`: add strict profile/case preparation and terminal
  checkpoint execution through public APIs.
- `doradb-bench/src/measurement.rs`, `output.rs`, and `plan_output.rs`: retain
  checkpoint, runtime-diagnostic, cardinality, RSS, and environment evidence in
  stable serialized output.
- `doradb-bench/templates/` and benchmark lifecycle tests: provide and validate
  the normally compiled public-API workload.
- Public API: `Session::checkpoint_catalog()` returns a richer success value.
  This is a source-visible return-type change but does not change admission,
  execution, failure, or durability semantics. Most callers that terminate the
  awaited expression with `;` continue to ignore it naturally.
- Performance: production checkpoint measurement is bounded to scalar updates
  and at most six deterministic entries in each table section, and reuses
  existing traversal. RSS sampling exists only while the benchmark workload is
  running.
- Compatibility: no catalog, table-file, redo, metadata, or super-block format
  version changes.

## Test Cases

1. Public report types expose documented values in deterministic increasing
   catalog `TableID` order and are exported from the crate root.
2. A no-op checkpoint reports `CatalogCheckpointOutcome::Noop`, empty table
   change and I/O arrays, and zero metadata write bytes without performing an
   extra reachability traversal.
3. Published checkpoint tests verify changed-table before/after row counts,
   omission of unchanged tables from the change array, active-table old-image
   reads, final reachable-image bytes, LWC writes, index writes, metadata
   writes, and aggregate equations against the actual fixed-size blocks visited
   or written.
4. Cache-warm and cache-cold runs retain identical cache-independent compact
   byte accounting while existing buffer-pool and storage-I/O counters show
   the expected different physical behavior.
5. Checkpoint I/O and validation failures preserve their existing error and
   poison policy and never return a partial success report or publish a root.
6. Existing standalone and combined checkpoint/redo-maintenance tests continue
   to prove replay-boundary, retention-progress, and no-op behavior after the
   standalone return-type change.
7. Profile generator tests verify every exact baseline cardinality and payload
   total, two columns and ten distinct bindings per table, one empty DROP probe,
   descriptor size limits, deterministic bytes, and non-colliding binding keys.
8. Strict plan tests accept every profile/case pair, reject unknown values,
   reject scale checkpoint warmups/repetitions, require prepare-before-final
   fixture state, and keep the workload present without Cargo features.
9. Each small-profile public case proves its exact pre-checkpoint effect and
   report shape: managed CREATE inserts all expected projections, managed index
   creation changes no bindings and preserves descriptor byte length, and DROP
   removes the selected definition and its bindings.
10. Checkpoint/reopen tests for all three cases verify the reported final
    cardinalities against recovered public binding resolution and managed
    definition state.
11. RSS sampler tests cover procfs parsing, page-to-byte overflow checks,
    start/stop synchronization, `peak >= baseline`, saturating delta, and clear
    errors for unavailable or malformed input without timing-based correctness
    assertions.
12. Workload/result TOML round trips retain profile, case, all catalog counts,
    descriptor bytes, raw checkpoint table stats, RSS metrics, and generic
    internal metrics without unknown-field tolerance.
13. A normal release-mode small-profile run succeeds without `--features` and
    emits exactly one measured catalog checkpoint.
14. Manual release-mode evidence covers the nine profile/case points on fresh
    roots. All three target runs must complete without OOM and with explainable
    full-image growth; stress results remain informational.
15. Run repository validation:

    ```bash
    rtk cargo fmt --all -- --check
    rtk cargo clippy --workspace --all-targets -- -D warnings
    rtk cargo nextest run --workspace
    rtk cargo nextest run -p doradb-storage --no-default-features --features libaio
    ```

## Open Questions

None.
