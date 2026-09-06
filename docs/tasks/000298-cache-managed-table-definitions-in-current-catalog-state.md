---
id: 000298
title: Cache Managed Table Definitions in Current Catalog State
status: proposal
created: 2026-09-06
github_issue: 1050
---

# Task: Cache Managed Table Definitions in Current Catalog State

## Summary

Make each live managed table's current catalog entry own an immutable shared
`ManagedTableDefinition` containing its stable-ID schema projection and complete
descriptor envelope. Binding resolution and managed-DDL preflight/revalidation
read this authoritative runtime value without descriptor-catalog lookups,
repeated schema projection, or repeated fingerprint computation.

Construct durable descriptor effects and runtime publication from the same
validated definition. Publish it with CREATE TABLE and index-DDL metadata/layout
changes, and hydrate it only after recovery validates final catalog and table-file
metadata. Descriptor rows remain authoritative for persistence and independent
integrity validation. Public APIs, definition-version semantics, and durable
formats remain unchanged.

## Context

Source Backlogs:

- docs/backlogs/000192-cache-managed-table-definitions-in-current-catalog-state.md

Issue Labels:

- type:task
- priority:medium
- codex

This is a standalone follow-up to tasks 000294 and 000296. RFC 0031 is completed
background, not this task's parent; its completed phases require no edits or
additional phase. The researched base is
`256b81ea7d3252b12e76158d85d90579d729d6f2`.

Task 000294 added two-pass binding resolution and optimistic definition versions.
Its narrow path already avoids descriptor rows and schema projection. Full
resolution reads an in-memory descriptor catalog row, validates its fingerprint,
and constructs a schema projection. Managed CREATE/DROP INDEX repeat descriptor
lookups during preflight and gated revalidation. These operations incur catalog
read claims, decoding, and repeated work even when the definition has not changed.

`CurrentTableState::Live` already couples current logical metadata to the exact
live `Table`. `Catalog::install_index_layout_and_publish_history` holds the user
catalog entry before installing the layout and publishing current metadata.
Target metadata-X excludes admitted readers throughout the DDL operation. This
existing boundary can publish the managed definition coherently.

Recovery initially loads table-file metadata before all catalog redo is replayed.
An initial runtime may therefore legitimately be ahead of checkpointed catalog
metadata. Hydrating descriptors during `reload_create_table` would bind a
definition before final reconciliation. Separately, provisional index reservations
can advance the effective next-index ID beyond the durable metadata watermark;
that allocator value cannot become part of an immutable definition cache.

### Existing metadata ownership

The design adds no `TableMetadata` reference inside `ManagedTableDefinition`.
The existing references have distinct purposes:

| Owner | Rationale and lifetime |
| --- | --- |
| `TableRuntimeLayout.metadata` | Couples one metadata generation to its exact executable secondary-index runtimes; admitted operations capture this layout. |
| `CurrentTableState::Live.metadata` | Supplies current logical metadata and the predecessor for metadata-only history; it is pointer-identical to current runtime-layout metadata. |
| `Table.mem.metadata` | The shared `MemTable` representation serves fixed-schema catalog tables and user-table row-page helpers. For user tables it retains construction-time metadata, while current index interpretation comes from `UserTableAccessor` and its captured layout. |
| Table-file active-root metadata | Describes the numeric schema belonging to that durable root and participates in checkpoint/recovery proofs. |
| Superseded catalog metadata versions | Preserve snapshot-visible logical metadata without retaining executable table/index runtimes. |

`Table::metadata()` delegates to the current layout; `Table` has no separate
direct metadata field. These owners commonly share `Arc` allocations. Index DDL
creates a new metadata generation while reusing the immutable column-layout
`Arc`, which permits user-table row helpers to retain construction-time metadata.
The full metadata retained by the embedded `MemTable` is broader than those row
helpers require. Document this existing redundancy without consolidating it here.

Research references:

- `docs/architecture.md`, `docs/transaction-system.md`, `docs/recovery.md`, and
  `docs/lock-system.md`: catalog authority, DDL publication, recovery, and claims.
- `docs/rfcs/0031-compact-numeric-catalog-table-definitions.md` and
  `docs/tasks/000294-managed-table-bindings-and-versioned-resolution.md`: managed
  definitions, public versions, and the original cache deferral.
- `docs/tasks/000296-preallocate-catalog-lock-manager-slots.md` and
  `docs/benchmark-tool.md`: required cache-enabled lock-store comparison.
- `doradb-storage/src/catalog/history.rs` and `catalog/mod.rs`: current-state
  ownership, pointer identity, coordinated publication, and descriptor validation.
- `doradb-storage/src/session/managed_table_ops.rs` and
  `table/index_ddl_plan.rs`: definition reads and optimistic DDL revalidation.
- `doradb-storage/src/table/access.rs`, `table/mem_table.rs`, and
  `table/index_lifecycle.rs`: execution metadata and independent allocator state.
- `docs/process/coding-guidance.md`, `docs/process/unit-test.md`, and
  `.config/nextest.toml`: error domains, test synchronization, and runner policy.

## Goals

1. Store one immutable shared managed definition beside current live metadata.
2. Use it for every online managed-definition read, including full/narrow binding
   validation and managed-DDL preflight/revalidation, with no read-through fallback.
3. Construct descriptor effects and cache publication from one validated value.
4. Preserve coherent CREATE, index-DDL, DROP, rollback, and fatal publication rules.
5. Hydrate every surviving managed table after final recovery validation and before
   foreground admission, including managed tables with zero bindings.
6. Reject absent or inconsistent definitions on admitted managed tables as typed
   data-integrity failures.
7. Prove consistency with independent state checks, deterministic boundary races,
   injected failures, corruption cases, and recovery sequences.
8. Revisit the task 000296 resolution comparison using identical cache-enabled
   resolution paths for both lock stores.

## Non-Goals

- Binding-key caching, post-CREATE binding mutation, or a catalog snapshot registry.
- Public shared-definition handles, execution-time expected-version admission,
  historical managed descriptors, or invalidation notifications.
- Descriptor-only DDL, column evolution, or changes to public version contents.
- Redesigning metadata ownership in `Table`, `MemTable`, layouts, roots, or history.
- Descriptor codecs, larger payload limits, persistence-format changes, or a
  different redo/checkpoint/recovery qualification model.
- Production lock-store changes, a runtime backend selector, or benchmark-framework
  redesign. Comparison-only source variants and evidence stay under ignored `target/`.

## Rejected Alternatives

### Cache only the descriptor

Keeping the descriptor alongside metadata but rebuilding `StorageTableDefinition`
for each consumer would retain repeated projection work and leave the complete
runtime definition assembled on each read. The chosen value caches the complete
projection and envelope at publication, accepting its resident memory cost.

### Engine-wide versioned catalog snapshots

A shared snapshot covering bindings, definitions, and execution admission could
remove additional lookups and copies, but introduces broader ownership,
invalidation, and transaction contracts. It requires separate RFC-level design;
this private current-definition value is a compatible prerequisite.

## Plan

### 1. Immutable definition and invariant ownership

Add a crate-private type in `catalog/definition.rs`:

```rust
struct ManagedTableDefinition {
    schema: StorageTableDefinition,
    descriptor: TableDescriptorObject,
}

enum CurrentTableState {
    Live {
        effective_cts: TrxID,
        metadata: Arc<TableMetadata>,
        table: Arc<Table>,
        managed_definition: Option<Arc<ManagedTableDefinition>>,
    },
    Dropped {
        effective_cts: TrxID,
    },
}

// Existing private DDL preflight carrier:
struct CurrentTableDefinition {
    definition: Arc<ManagedTableDefinition>,
    effective_next_index_id: u64,
}
```

Use private fields and read-only accessors. The definition owns neither metadata
nor table, layout, root, or executable index handles. Its schema projection is
deliberately retained to eliminate repeated projection. Its descriptor already
contains the table ID, private revision, compiled epoch, fingerprint, and payload;
do not duplicate those stamps in another header.

DDL construction borrows the finalized numeric metadata to derive the projection
and descriptor stamps after existing payload validation. Recovery construction
accepts a decoded descriptor, checks its envelope against final numeric metadata,
and derives the projection from that metadata. No production constructor accepts
an independently supplied schema projection and descriptor pair. No mutable
descriptor/schema accessor or independently callable cache-update API is added.

The DDL plan owns the association between its existing numeric metadata field and
the managed definition. Managed plan finalizers build both together rather than
accepting independently assembled definition effects. Full schema/fingerprint
agreement is established there and during recovery; table-ID/epoch equality alone
is not a proof of complete consistency.

Extend current-state access with one shared internal validation path for managed
definition consumers. Under the caller's target metadata claim, it checks that
definition-owner kind agrees with presence of the managed value, and the value has
the selected table ID and current metadata storage epoch. Preserve the existing
catalog-current/runtime-layout metadata identity contract and publication checks.

Missing values on admitted managed tables and foreign/stale values are
data-integrity errors. Unmanaged tables have no managed value. Missing/dropped
requested DDL targets retain `TableNotFound`; selecting an unmanaged table through
managed DDL retains invalid-metadata behavior. A binding that names an absent or
unmanaged runtime remains corruption. Preserve native internal error domains and
attach operation context at their existing public/runtime boundaries.

`None` on a managed runtime is permitted only during private recovery construction.
Separate recovery insertion/hydration from normal online publication so normal
CREATE cannot install an unhydrated managed entry. Keep runtime-only table accessors
from cloning the complete current state and its new definition `Arc` unnecessarily.

### 2. Accepted effects and atomic publication

Have `TableDescriptorEffect::Insert` and `Replace` carry
`Arc<ManagedTableDefinition>`. `CatalogDefinitionEffects` exposes the corresponding
managed value for publication; row staging borrows its descriptor. Do not store a
second descriptor or an independently assembled replacement definition beside the
effects. Unmanaged and DROP effects retain their existing meanings.

`ValidatedCreateTable::into_managed_plan` builds the initial definition with its
allocated table ID, revision zero, and finalized schema. After the existing file
publication, runtime construction, and catalog commit, runtime insertion publishes
metadata, `Table`, and the same managed value as one current entry. Before-commit
failure discards the unpublished value along with existing provisional resources.

Managed index finalizers derive the next revision and create the replacement value
from `new_metadata`. Build/project/hash the value before irreversible effects and
before entering the final catalog-entry critical section. Preserve existing revision
and epoch overflow checks and reject managed/unmanaged effect mismatches.

Extend `install_index_layout_and_publish_history` and its typed CREATE/DROP callers
to publish the plan's managed value with the new current metadata. Prevalidate the
expected table, old metadata identity, managed ownership, and replacement stamp
before installing the layout. Retain catalog-entry-before-layout lock ordering.
Once layout installation succeeds, current metadata/definition publication is
infallible within the same occupied entry, with no await or additional projection.
Metadata history receives only superseded numeric metadata.

Logical target metadata-X stays held through durable effects and publication, so
admitted readers cannot observe catalog rows ahead of the runtime definition.
Before-commit rollback preserves the old definition. Unexpected failure after
catalog commit or root publication follows the existing fatal cleanup/poison policy;
do not attempt online cache repair or permit subsequent foreground reads.

DROP uses its existing descriptor/binding cascade and current-state tombstone
publication. The tombstone and retained dropped runtime do not own a managed
definition. A previously captured immutable definition can survive through its
reader's `Arc`, but does not retain executable resources.

### 3. Resolution and managed-DDL readers

Keep binding lookup catalog-backed and preserve the probe/release/final-pass/retry
algorithm, including disappeared and rebound keys. Both final resolution modes
take target metadata-S followed by binding metadata-S/data-IS. Remove the full-mode
descriptor claims and simplify the acquisition helper's now-unnecessary mode
argument. Maintain `FreshClaimsGuard` cancellation cleanup for the reduced set.

After the final binding recheck, select one current state and validate its managed
value. Narrow resolution reads only constant-size identity/version information;
it does not clone schema vectors or descriptor bytes. Full resolution clones the
cached schema and payload into the existing `ManagedTableDefinitionSnapshot`.
No definition reader recomputes a fingerprint or calls `from_metadata`. No claims
or executable-runtime ownership escape the public call.

Managed-DDL preflight captures the shared definition under target metadata-S,
retaining target/lifecycle validation without a redundant catalog-parent lookup.
Parent-row existence remains an invariant asserted during catalog writes. Remove
the descriptor catalog read claims and descriptor lookup. Obtain the effective
allocator through the existing Table-owned validated allocator view, then release all operation,
table, layout, and root authority before calling the interpreter once.

Final managed index preparation retains the existing complete DDL write claims and
gates. Read the managed value again from current state and pass it to Table's typed
finalizer instead of loading a descriptor row. A changed definition identity is a
stale attempt; preserve the private epoch/revision checks and CREATE-only effective
allocator comparison. Return zero-effect `SchemaChanged` without retrying user
interpretation. DROP does not become sensitive to allocator-only changes.

Audit every production descriptor accessor call site. Read-only definition
consumers must use the current value. Descriptor DML, bootstrap/recovery, and
independent integrity/checkpoint validation still operate on rows. Descriptor
replacement can still read its target as part of the write; the zero-lookup
requirement concerns online definition reads, not the mechanics of catalog DML.
Reverse binding enumeration retains its existing catalog-backed behavior and
checks the managed current-state invariant without loading descriptor payloads.

### 4. Recovery hydration and durable validation

Leave `reload_create_table` responsible for constructing the runtime from the
selected table root and classifying its definition owner. It does not install a
managed definition while later catalog redo can still change the final definition.

Preserve recovery ordering: replay/root classification, complete catalog-parent and
binding-owner validation, descriptor validation against reconstructed numeric
catalog metadata, then final loaded-table metadata reconciliation and index-lifecycle
classification. Extend the descriptor validation path to return its decoded,
validated descriptor objects so hydration can consume them without another full
descriptor scan.

Immediately after successful `validate_loaded_table_metadata`, run a recovery-only
catalog hydration operation. Build each managed value outside map guards from its
descriptor and final live metadata; verify the envelope again against that final
runtime metadata, then install under the corresponding entry guard. Hydration does
not advance effective CTS, add history, or replace the runtime/layout.

Require exactly one descriptor for each live managed runtime, none for unmanaged
runtimes, and no leftover descriptor without its expected live runtime. Reject
duplicate hydration, ownership disagreements, and missing values. After hydration,
every foreground-admissible managed table satisfies the same invariant as online
CREATE. Partial hydration on failure is private to the failed bootstrap; the engine
must not admit foreground operations.

Durable managed ownership is defined by descriptor presence. A checkpointed table
with neither descriptor nor bindings is a valid unmanaged table; historical managed
ownership cannot be inferred from that state. Missing-descriptor corruption tests
must preserve evidence of managed ownership, such as a binding or a runtime already
classified managed before later replay. Do not add a persisted ownership flag or
promise detection where the existing durable model supplies no such evidence.

Keep durable descriptor, parent, binding-owner, and projected-checkpoint validation
independent of the runtime definition. Tests that directly corrupt descriptor rows
must exercise these boundaries; full resolution no longer promises to discover
row-only corruption through a read-through lookup. Cache corruption and durable-row
corruption have separate tests.

### 5. Benchmark comparison and documentation

Use the existing `managed-bindings-prepare`, `resolve-table-binding`, and basic
paired shared `lock-table` workloads. Cover narrow/full resolution and user-lock
controls for one and 64 targets at workers/sessions `1/1`, `4/4`, `8/8`, and `4/16`.
Keep fixture schemas, keys, descriptors, engine settings, statistics capture, and
validation identical across comparison builds.

Measure the cache-enabled final sources with the current hybrid lock store and a
comparison-only dynamic DashMap store. Task 000296 identifies the dynamic baseline
as `6ef453dbf0f06a94c80630707b783a4b475be7f8`; restore/adapt only the lock-store
implementation in an ignored comparison source tree, including its matching waiter
implementation where required. Both builds must use the same final definition cache
and benchmark sources. Record exact source patches/hashes and verify the difference
is confined to the compared lock implementation. Do not compare an uncached older
resolver with a cached current resolver or add a production backend selector.

Recalibrate operation counts for the faster paths before measurement and freeze
identical counts per paired case. Use the established template's warm-up/measured
runs, reverse run order when investigating slower cases, and verify identities,
versions, full snapshots, sample counts, counters, and final lock drain. Record
throughput/latency and investigate regressions without imposing an invented fixed
speedup threshold. Binding lookup remains a shared catalog resource and public full
snapshots still copy their contents.

Store benchmark results, commands, source variants, profiles, and environment
evidence only under ignored `target/task-000298/`. Earlier ignored reports are not
required to exist; reconstruct comparable source variants from the recorded
revisions. Do not persist benchmark measurements in tracked task or source files.

Update `docs/architecture.md`, `docs/transaction-system.md`, `docs/lock-system.md`,
and `docs/recovery.md` to describe the new runtime authority, reduced read claims,
publication, and hydration. Clarify public optimistic version and owned snapshot
semantics in `docs/public-api.md` if its current wording describes descriptor-row
reads. Preserve existing public contracts and completed RFC phase status.

## Implementation Notes

Implemented the immutable managed definition in current live catalog state.
Accepted CREATE and index-DDL effects stage descriptor rows and publish the same
shared value. Runtime-only table lookup avoids cloning that value. Online
binding resolution, reverse enumeration, and managed-DDL preflight/revalidation
validate the cache without descriptor reads, schema projection, or fingerprint
computation. Full public snapshots retain their existing owned-copy contract.

Catalog runtime lookup uses synchronous `get_table`; the former `get_table_now`
alias is removed. Live-table validation, table-cache lookup helpers, maintenance
resolution, and recovery helpers whose only await was a table lookup are also
synchronous. Lock admission, catalog DML, and storage I/O retain their async paths.
Binding resolution uses `binding_target_definition` to validate borrowed current
state and clone only the definition, preserving native data-integrity errors.
Public snapshot bytes are copied after releasing the entry guard. Index
publication also borrows current state while holding its existing entry guard,
preserving validation and entry-before-layout lock ordering.

Managed index preflight and final preparation each obtain the current table and
optional `definition` through `validated_current_user_table`. One catalog lookup
validates borrowed current state under the entry guard and clones only the table
and definition; it does not clone metadata or materialize a full current state.
Callers move the definition into preflight or finalization without another clone.
The definition-only catalog accessor borrows current state and clones only the
definition. Target metadata admission keeps the selected generation stable;
final preparation still acquires the existing gates before requiring a managed
table and invoking typed finalization. Cache-integrity checks run during the
guarded lookup. The helper is synchronous and does not read `catalog.tables`;
parent-row existence remains an invariant asserted during index catalog writes.
Temporary runtime ownership ends before interpretation or mandatory submission.
Unmanaged CREATE/DROP INDEX uses synchronous live-table validation directly;
the redundant target helper and parent-row precheck are removed. Catalog-write
assertions and recovery/checkpoint integrity validation retain their existing policy.
A targeted foreground counter covers all current-table lookup entry points,
asserting one lookup per preflight, two across both phases before mandatory
CREATE/DROP INDEX execution, and zero full-state lookups in those phases or the
definition-only accessor. A target-specific foreground parent-row lookup counter
also verifies zero `catalog.tables` reads during preflight and final preparation.
The same counters cover narrow/full binding resolution and unmanaged index
preparation, which each use one current-table lookup and no parent-row reads or
full-state lookups.

Recovery consumes decoded, independently validated descriptor rows only after
final loaded-table metadata reconciliation. Hydration checks ownership and final
stamps without advancing effective CTS, changing layouts, or adding history.
Metadata history and retained dropped runtimes do not retain definitions.

Validation includes the independent numeric/catalog/cache/public oracle,
descriptor exclusion and zero-work counters, cache and durable-row corruption,
precommit rollback with exact Arc identity, allocator-only and competing-DDL
staleness, publication exclusion, root-qualified fatal recovery, zero-binding
recovery, and weak ownership checks. The initial cache implementation passed all
1,946 workspace tests; four race tests passed 100 stress iterations.
Strict Clippy, formatting, and the branch
style audit pass. Focused coverage across the six core files is 96.96% overall,
with each file above 95%. The change does not modify storage I/O backends.

The completed synchronous lookup cleanup passed all 1,948 workspace tests, plus
100 stress iterations of eight lookup, publication, stale-interpreter, and
binding-revalidation tests. Focused assertions cover zero parent-row reads and
full-state lookups, shared definition identity, cache corruption, and rejection
of a valid unmanaged table by managed-definition readers. Formatting, strict
workspace Clippy, and the branch style audit passed across 19 changed Rust files.

The cache-enabled hybrid/dynamic lock-store comparison uses the required
narrow/full resolution and paired user-lock matrix. Both source variants have
identical cache and benchmark sources; only the lock manager and matching waiter
files differ. Calibration, paired results, reverse-order checks, source/binary
hashes, patches, environment evidence, and profiles are retained exclusively in
`target/task-000298/`.

A separate existing failure was reproduced on unchanged base
`256b81ea7d3252b12e76158d85d90579d729d6f2`: repeated catalog checkpoints after
DROP/recreate can fail decoding `catalog.mtb` block 1 with
`LWC FOR bitpacking payload length mismatch: expected 1, actual 5`.
The suspected boundary is reclaimed catalog block reuse and readonly-cache
invalidation; this task does not fix it. The bounded definition model reopens
before and after checkpoint operations to isolate definition hydration from
that failure. Its original public-operation reproducer and baseline failure
output remain under `target/task-000298/`. No backlog was created, as requested.
This limitation must remain visible during task resolution.


## Impacts

- `catalog/definition.rs`: immutable managed value, shared descriptor effects, and
  the preflight carrier with a separate effective allocator snapshot.
- `catalog/history.rs`, `catalog/mod.rs`: current-state ownership/validation,
  runtime-only accessors, coordinated publication, and recovery hydration.
- `catalog/table.rs`, `catalog/index.rs`, `table/index_ddl_plan.rs`: plan construction,
  publication, stale revalidation, and existing failure boundaries.
- `catalog/storage/ddl.rs`: stage descriptor rows from the shared accepted value.
- `session/managed_table_ops.rs`: cache-only definition readers and reduced read
  claims, with existing callback and binding semantics preserved.
- `recovery/mod.rs`, descriptor/integrity tests: final hydration and independent
  durable validation. No new file format, I/O backend, or redo classification.
- Runtime memory: one additional resident descriptor payload and schema projection
  per live managed table, plus temporary retained reader generations. History does
  not retain them. Ordinary runtime-only lookup avoids extra definition cloning.
- Existing benchmark workloads and architecture/process-facing design docs provide
  validation and updated operational descriptions; no new public storage API.

## Test Cases

### Independent consistency oracle

Create a shared test-only helper that checks a settled managed table against both
runtime and catalog state. It must verify current/layout metadata pointer identity;
numeric catalog reconstruction versus current metadata; cached schema, table ID,
epoch, fingerprint, and exact descriptor bytes; descriptor-row equality; and public
resolution's version/snapshot. Obtain appropriate target admission or use a
quiescent fixture before cross-checking separate stores.

Maintain independently expected schemas/index IDs and distinguishable descriptor
payloads in the tests. Agreement between two internal representations alone must
not be the only oracle. Keep deep oracle reads outside zero-access counter windows.

### Required scenarios

1. Successful CREATE TABLE, CREATE INDEX, DROP INDEX, and DROP TABLE apply the oracle
   after each settled transition. Include zero/one/multiple bindings, empty and
   binary descriptors, the inclusive 64,000-byte limit, and multiple tables with
   identical schemas/epochs but different descriptor bytes. Aliases share a version;
   unrelated table definitions remain unchanged.
2. Constructor and recovery tests reject wrong table IDs, epochs, fingerprints,
   and invalid descriptor envelopes. Production constructors derive projections
   from metadata; tests verify expected stable-ID columns, indexes, and ordering.
3. Corrupt current state through narrow test-only hooks: remove a managed value,
   substitute another table's definition, install a stale generation, or mismatch
   managed ownership. Full/narrow resolution and managed-DDL readers fail as data
   integrity without consulting descriptor rows.
4. Prove no descriptor lookups, schema projections, or fingerprint computations in
   narrow/full resolution and preflight/revalidation. Instrument those phases
   separately from replacement construction and descriptor DML. Holding descriptor
   catalog exclusion must not block read-only definition operations. Test bindings
   still use their expected catalog claims and lookups.
5. Use existing DDL phase gates around catalog commit, root publication, and final
   layout/history publication. Concurrent resolution and DDL preflight observe a
   coherent old or new schema/descriptor/version. Exercise CREATE and DROP INDEX.
   A direct catalog current-state reader and history purge cannot enter the split
   layout/current-state publication interval. No sleep establishes progress.
6. Inject CREATE TABLE failures after catalog staging, file publication, and runtime
   construction. No table/definition/binding becomes foreground-visible and all
   reversible catalog/file effects are cleaned up. Inject managed index failures
   before commit, including after descriptor staging, and verify the exact old
   definition `Arc`, public version, and durable-row state remain intact.
7. Inject failure after catalog commit and after root publication before current
   publication. The existing fatal boundary closes foreground admission. Reopen
   and verify the same root-qualified definition as catalog replay: unproven CREATE
   effects do not leak a replacement descriptor, while proven changes hydrate the
   corresponding new definition.
8. Race managed CREATE/DROP INDEX interpretation against committed index DDL. Stale
   attempts return `SchemaChanged`, invoke each interpreter once, and have zero
   effects. Cover allocator-only advancement separately: the immutable definition
   can remain unchanged while CREATE's proposed ID becomes stale. DROP ignores an
   allocator-only change.
9. Preserve binding DROP/recreate and DROP-only races between probe and final pass.
   Rebinding cannot return a former table's definition/version. Cancellation during
   either reduced acquisition pass drains every partially acquired claim; retarget
   the existing full-resolution cancellation fixture away from descriptor claims.
10. Reopen checkpoint-only managed tables and redo-only CREATE, CREATE/DROP INDEX
    after catalog checkpoint, CREATE whose table root already includes later index
    DDL, and CREATE-then-DROP histories. Include provisional reservations beyond
    the durable next-index watermark and both checkpoint-covered/replay-visible
    boundaries. Apply the oracle immediately after bootstrap, before warm-up reads.
11. Recovery includes zero-binding managed tables, unmanaged tables, and dropped
    tables. Missing descriptors, orphan/binding ownership errors, envelope mismatch,
    final runtime/catalog disagreement, or duplicate hydration fail before
    foreground admission. Failed partial hydration cannot expose a session.
12. Deliberately corrupt descriptor rows independently of a valid runtime value.
    Cached reads remain isolated from row access, while live durable validation,
    projected checkpoint validation, and recovery reject the corruption. Update
    the existing full-resolution descriptor-stamp test to reflect these boundaries.
13. Retain an old preflight definition across successful DDL and verify its schema
    and bytes remain unchanged. Use weak ownership checks to prove obsolete values
    are freed after readers release them even while metadata history remains.
    DROP leaves no definition in its tombstone/retained runtime; definitions do not
    pin executable index reclamation. Existing owned public snapshots stay valid.
14. Run a bounded, seeded operation sequence against a small independent model:
    successful and failed managed DDL, checkpoint, reopen, and DROP/recreate.
    Check expected schema, descriptor, version behavior, and the consistency oracle
    after every transition. Deterministic boundary/error tests remain separate.
15. Retain regression coverage for unmanaged DDL, numeric format/root count,
    descriptor limits, callbacks, slot reuse, metadata history, locks, and lifecycle.
    Complete the cache-enabled benchmark matrix and its result/drain validations.

Keep hooks and deep consistency helpers behind `cfg(test)` in their owning test
modules, with narrow cross-module re-exports. Extend existing DDL gates and counters
instead of adding production observability solely for tests. Use semantic events,
barriers, and predicate rechecks; timeouts are only watchdogs or negative assertions.
Extract common fixtures/oracles and use table-driven cases where flows coincide.

Validation commands, following repository runner/configuration authority:

```bash
rtk cargo fmt --all -- --check
rtk cargo clippy --workspace --all-targets -- -D warnings
rtk cargo nextest run --workspace
```

Run focused nextest filters during development and focused stress runs for new race
tests. Do not change `.config/nextest.toml` or introduce an alternate test runner.
The change is backend-neutral; if implementation changes backend-neutral I/O or
backend code, also run the documented explicit `libaio` validation pass. Complete
the standard branch style gate during task resolution.

## Open Questions

No implementation decision remains open. Resident definition memory and public
snapshot copying are accepted costs; obsolete shared generations must follow
reader lifetimes rather than metadata-history retention. Frequent binding lookup
can remain the measured bottleneck after descriptor caching.

Future descriptor-only DDL must atomically replace the managed value and extend
the public version beyond storage epoch. Execution-time version admission remains
backlog 000194. Consolidating existing metadata references is outside this task;
the ownership rationale above is the required documentation outcome.

Backlog 000192 remains open through implementation and is resolved only after the
consistency tests, review, and required benchmark revisit pass. This standalone
task requires no parent-RFC phase synchronization.
