---
id: 000298
title: Cache Managed Table Definitions in Current Catalog State
status: implemented
created: 2026-09-06
github_issue: 1050
---

# Task: Cache Managed Table Definitions in Current Catalog State

## Summary

Each live managed table now owns an immutable shared `ManagedTableDefinition`
in its current catalog entry. The value contains the stable-ID schema projection
and complete descriptor envelope. Binding resolution and managed-DDL readers
use this runtime authority without descriptor-row reads, schema projection, or
fingerprint computation.

CREATE TABLE and index DDL stage durable descriptor effects and publish the same
accepted definition. Recovery hydrates definitions after final catalog and
runtime reconciliation, before foreground admission. Descriptor rows remain
independent authorities for persistence and integrity validation. Public APIs,
optimistic versions, and durable formats are unchanged.

## Context

Source Backlogs:

- docs/backlogs/closed/000192-cache-managed-table-definitions-in-current-catalog-state.md

Issue Labels:

- type:task
- priority:medium
- codex

This standalone task follows tasks 000294 and 000296. RFC 0031 is completed
background, with no parent-RFC phase linkage or synchronization required.
The implementation was researched against
`256b81ea7d3252b12e76158d85d90579d729d6f2`.

Task 000294 introduced persistent bindings and optimistic versions. Full
resolution and managed index DDL repeatedly read descriptors and reconstructed
schema state. Task 000296 deferred this cache and required a new lock-store
comparison once resolution used the cache.

Current catalog metadata was already pointer-identical to live runtime-layout
metadata, and index publication already coordinated those owners under one
catalog entry guard. This supplied the publication boundary. Recovery can load
runtime metadata ahead of checkpointed catalog rows, so hydration had to wait
for final reconciliation. Provisional index allocation can also advance beyond
the durable watermark and remains separate from the immutable definition.

## Goals

- Make all online managed-definition readers use current catalog state without
  a read-through fallback.
- Share one validated value between durable effects and runtime publication.
- Preserve coherent CREATE, index-DDL, DROP, rollback, and fatal-error behavior.
- Hydrate every surviving managed table, including tables without bindings,
  before foreground operations are admitted.
- Detect missing, foreign, stale, or ownership-inconsistent runtime definitions
  through typed integrity errors.
- Remove redundant lookup work and complete the cache-enabled benchmark revisit.

## Non-Goals

- Binding-key caching, post-CREATE binding mutation, or global catalog snapshots.
- Public shared-definition handles, invalidation notifications, or execution-time
  expected-version admission.
- Descriptor-only DDL, column evolution, or new version fields.
- Consolidating existing metadata owners or retaining historical descriptors.
- Changes to codecs, payload limits, durable formats, or recovery qualification.
- Production lock-store changes, backend selection, or benchmark infrastructure.

## Rejected Alternatives

Caching only the descriptor would retain repeated schema projection. The complete
projection and envelope are cached together, accepting their resident memory cost.

An engine-wide versioned snapshot could remove more lookups, but would introduce
broader ownership, invalidation, and execution-admission contracts. The private
current-definition value fits existing publication and admission boundaries.

## Plan

### Definition ownership and validation

`CurrentTableState::Live` holds an optional `Arc<ManagedTableDefinition>` beside
current metadata and the live table. Normal managed publication requires it;
only private recovery construction permits an unhydrated managed runtime.
Unmanaged tables have no definition.

DDL constructors derive the schema and descriptor stamps from finalized numeric
metadata. Recovery validates decoded envelopes against final metadata before
projecting the schema. The value has read-only accessors and retains no table,
metadata, layout, root, or executable index handles. The descriptor already
contains its identity and version stamps; the preflight carrier adds only a
separate effective allocator snapshot.

Under target metadata admission, shared validation checks owner kind, table ID,
storage epoch, and current/runtime metadata pointer identity. Constructors prove
full schema/fingerprint agreement. Admitted cache corruption is an integrity
failure. Missing or dropped requested DDL targets retain `TableNotFound`, while
managed DDL on a valid unmanaged table retains invalid-metadata behavior. A
binding targeting an absent or unmanaged runtime remains an integrity failure.

Existing metadata owners retain their distinct lifetimes: runtime layouts and
current state share a generation, history retains superseded numeric metadata,
roots describe durable schemas, and `MemTable` keeps construction-time metadata
for row helpers. Current index interpretation comes from captured layouts.
Index DDL reuses immutable column-layout storage. The broader embedded metadata
was documented without consolidating these owners.

### Read and preparation paths

Binding resolution preserves probe/release/final-pass/retry semantics. The final
pass takes target metadata-S followed by binding metadata-S/data-IS and rechecks
the key. Both narrow and full resolution use `binding_target_definition`, which
borrows guarded current state and clones only the definition. Full snapshot
copies occur after releasing the map guard; narrow resolution copies no schema
vectors or descriptor bytes. No claims escape the public call.

Managed index preflight takes target metadata-S. Each preflight and final
preparation performs one current-table lookup through synchronous
`validated_current_user_table`, returning the table and optional definition.
The interpreter runs once after operation and executable-runtime ownership end.
Final preparation takes existing write claims and gates, then revalidates shared
generation identity, epoch, revision, and the CREATE-only effective allocator.
Stale attempts return zero-effect `SchemaChanged`; DROP ignores allocator-only
changes. Reverse binding enumeration remains catalog-backed and validates the
managed current-state invariant.

Catalog `get_table`, live validation, table-cache helpers, and dependent session
and recovery wrappers are synchronous where no actual wait is needed. The former
`get_table_now` alias is removed. Definition accessors take `&self`; typed plans
retain shared runtime ownership when execution requires it. Unmanaged index
preparation uses synchronous live-table validation directly.

### Durable effects and publication

Descriptor insert/replace effects own the same shared definition later published
into current state. Projection and hashing finish before irreversible work and
outside map guards. CREATE publishes the initial value with the runtime after
its existing durable sequence.

Index publication borrows current state under the occupied entry guard, validates
the expected runtime, old metadata, and replacement ownership/stamps, then installs
the layout and commits current metadata/definition together. Entry-before-layout
lock order is preserved. Publication has no await or additional projection, and
target metadata-X excludes admitted readers across durable effects and publication.

Before-commit failure preserves the exact old generation. Unexpected failures
after catalog commit or root publication follow the existing fatal policy.
DROP cascades descriptor/binding deletion and publishes a tombstone. Neither
metadata history nor retained dropped runtimes owns obsolete definitions;
previously captured immutable generations survive only through their readers.

### Recovery and independent integrity

Recovery retains parent/binding validation, descriptor validation against numeric
catalog metadata, and final table-file reconciliation and lifecycle classification.
Hydration then consumes the already validated descriptor objects and checks them
against final runtime metadata, avoiding another descriptor scan. It changes no
CTS, history, or layout. Duplicate hydration, missing definitions, ownership
mismatches, or leftover descriptors fail bootstrap before foreground admission.

Descriptor presence defines durable managed ownership. A checkpointed table with
neither descriptor nor bindings is valid unmanaged state; missing-descriptor tests
retain ownership evidence through bindings or an already classified runtime.
Durable-row and projected-checkpoint validation remain independent of the cache.
Descriptor replacement can still read its target while performing catalog DML.

## Implementation Notes

Implemented coherent managed-definition publication and recovery hydration, with
all online definition readers using validated current catalog state. Durable
staging and runtime publication share the accepted definition; public owned
snapshots and optimistic version semantics remain unchanged.

Review removed redundant parent-row prechecks from managed and unmanaged index
preparation, beyond the original descriptor-cache change. Parent-row existence
remains an assertion during catalog writes, and durable recovery/checkpoint
validation retains its policy. Cache-only access and dependent wrappers became
synchronous. Borrowed current-state access avoids cloning complete state, and
one lookup returns the table/definition pair needed by each managed DDL phase.

The completed implementation passed all 1,948 workspace tests. Eight lookup,
publication, stale-interpreter, and binding-revalidation tests passed 100 stress
iterations. Formatting, strict workspace Clippy, and the resolve-time branch
style audit passed across 19 changed Rust files. Initial cache implementation
coverage was 96.96% across six core files, each above 95%; that measurement
predates the subsequent lookup cleanup. Storage I/O backends were unchanged.

The required hybrid/dynamic lock-store comparison completed narrow/full
resolution and paired user-lock controls for one and 64 targets at workers/sessions
`1/1`, `4/4`, `8/8`, and `4/16`. Both builds used identical cache-enabled
catalog and benchmark sources; only the lock manager and matching waiter differed.
All 48 primary and eight reverse-order invocations passed identity, version,
snapshot, sample/count, and final lock-drain validation.

Resolution generally favored the hybrid store, with the busiest distributed full
case near parity. Distributed user-lock controls remained slower after reversing
run order. Profiles implicated shared atomic/statistics and transition work, but
did not isolate a causal explanation. No production lock-store tuning followed.
These benchmarks precede the final lookup cleanup and do not establish its
timing impact.

An existing same-process catalog checkpoint failure remains unresolved. Repeated
DROP/recreate and checkpoint operations reproduced on unchanged base
`256b81ea7d3252b12e76158d85d90579d729d6f2` fail decoding `catalog.mtb` block 1,
row 0 with `LWC FOR bitpacking payload length mismatch: expected 1, actual 5`.
Reclaimed catalog block reuse and readonly-cache invalidation are suspected,
but were not conclusively isolated. The definition model reopens before and after
checkpoints to isolate hydration from this independent defect; it does not prove
repeated same-process checkpoint/reuse correctness.

This limitation was reported during implementation and retained for resolution.
No new follow-up backlog was created, per user instruction.
Source backlog 000192 is closed as implemented by this task.

## Impacts

- Catalog current state, managed DDL, and recovery now share explicit immutable
  definition ownership and validation boundaries.
- Foreground definition reads avoid descriptor catalog admission and repeated
  decoding/projection/hash work. Binding-key lookup remains catalog-backed.
- Each live managed table retains one schema projection and descriptor payload;
  public full snapshots still copy their contents. History retains no descriptors.
- Internal table/cache lookups are synchronous; public APIs, formats, schemas,
  payload limits, and storage backends remain compatible.
- Architecture, transaction, lock, recovery, public API documentation, and the
  public error audit were synchronized with the final execution paths.

## Test Cases

- An independent numeric/catalog/cache/public oracle checks successful CREATE,
  CREATE/DROP INDEX, DROP, aliases, multiple tables, payload boundaries, and
  zero-binding tables against expected schemas and descriptor bytes.
- Descriptor exclusion and targeted counters verify zero descriptor reads,
  projection, and hashing in definition readers. Binding resolution and unmanaged
  preparation use one current lookup; managed DDL uses one per phase, with no
  parent-row reads or full-state lookup clones.
- Cache ownership/generation corruption fails online readers. Independently
  corrupted durable rows fail durable/checkpoint/recovery validation, while
  cache reads remain isolated from row-only changes.
- Rollback preserves exact definition identity. Precommit faults clean staged
  resources; postcommit/root-publication faults close admission and exercise
  the existing root-qualified recovery rules.
- Deterministic gates cover coherent publication, direct current-state reads,
  history purge exclusion, stale callbacks, allocator-only changes, binding
  DROP/recreate races, and cancellation claim cleanup.
- Checkpoint and redo recovery cover CREATE/index-DDL/DROP histories, final
  metadata reconciliation, ownership evidence, duplicate hydration, and
  provisional allocators. A seeded model checks each transition with the
  checkpoint-reopen limitation documented above.
- Weak ownership checks prove obsolete definitions do not pin history or
  executable resources. Existing unmanaged DDL and lifecycle tests remain green.

## Open Questions

The catalog block reuse/checkpoint decoding defect still needs independent
investigation; its failure signature and suspected invalidation boundary are recorded in
Implementation Notes without a new backlog. Distributed lock-control performance
also remains causally unisolated.

Execution-time expected-version admission remains tracked by
[backlog 000194](../backlogs/000194-admit-expected-managed-definition-versions-through-execution.md).
Future descriptor-only DDL must atomically replace the managed value and extend
public versions beyond storage epoch. Binding lookup contention and existing
metadata-owner consolidation remain outside this task.
