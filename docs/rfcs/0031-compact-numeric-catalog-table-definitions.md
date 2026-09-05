---
id: 0031
title: Compact Numeric Catalog and Extensible Table Definitions
status: implemented
tags: [storage, catalog, metadata, ddl, checkpoint, recovery]
created: 2026-08-28
github_issue: 1028
---

# RFC-0031: Compact Numeric Catalog and Extensible Table Definitions

## Summary

The redesign replaced name-bearing metadata and permanent index-slot consumption
with a compact numeric schema, stable identities, and reusable secondary slots.
`catalog.tables` is the checked parent and allocator authority; opaque descriptors
and managed-only bindings carry higher-layer meaning. All eight phases are implemented,
including optimistic definition resolution and public checkpoint reporting. The
final format cutover rejects older formats without migration. Full-image
checkpoints remain; all nine release cases completed, including the 10,000-table,
100,000-binding, 64-MiB-descriptor target. Execution-lifetime admission and
incremental persistence remain deferred. [D4], [U9], [U19], [U25]

## Context

The previous catalog persisted column names and one `catalog.index_columns`
row per key position despite numeric execution and ordered runtime key lists.
RFC-0018 prohibited index-number reuse because redo, undo, purge, and roots
retained slot-only references, making repeated DDL consume root capacity. [D6], [D9]

The program separated identity from position before the format cutover, then
added integrity, slot reuse, managed definitions, bindings, and scale evidence.
Review replaced caller-owned proposals with engine orchestration, unified checked
transient reference carriers, and benchmarked supported public DDL. Task records
retain the detailed findings. [U12], [U13], [U19], [U23], [U24]

Issue Labels:

- type:epic
- priority:high
- codex

## Goals

- Deliver numeric, name-free metadata with stable IDs and safely reusable slots.
- Commit numeric schema, exact descriptor bytes, and applicable bindings atomically.
- Validate complete catalog ownership at recovery and checkpoint boundaries.
- Measure the retained checkpoint model at an explicit initial scale.

## Non-Goals

DDL remains CREATE/DROP TABLE and CREATE/DROP INDEX. Column evolution and row
migration, post-CREATE binding changes, descriptor-only ALTER, and historical or
execution-lifetime snapshots are excluded. Stable column IDs do not make
ordinal-based redo compatible with future column evolution. [U5], [U6], [U7], [U24], [U25]

Codecs, logical constraints, descriptor authority/self-containment, and external
registry atomicity remain application concerns. Larger blobs, online index DDL,
parallel builds, root-vector compaction, incremental persistence, and migration
tools were not delivered. Coarse DDL exclusion remains. [U8], [U9], [U15], [U18], [U19]

## Design Inputs

Original evidence labels are retained; code descriptions identify final boundaries.

### Documents

- [D1] `docs/architecture.md` — cache-first catalog and durable catalog root.
- [D2] `docs/transaction-system.md` — private DDL and mandatory publication.
- [D3] `docs/index-design.md`, `docs/secondary-index.md` — physical keys and roots.
- [D4] `docs/checkpoint.md`, `docs/recovery.md` — folding and redo-only root proof.
- [D5] `docs/table-file.md` — numeric metadata and exact secondary-slot states.
- [D6] `docs/rfcs/0018-create-drop-index.md` — non-reused IDs and provisional DDL.
- [D7] `docs/rfcs/0022-catalog-backed-redo-log-truncation.md` — replay visibility versus log deletion.
- [D8] `docs/rfcs/0024-versioned-metadata-immediate-retirement.md` — history does not keep dropped indexes executable.
- [D9] `docs/tasks/000146-stable-index-metadata.md` — slot-only safety rationale.

### Code References

- [C1] `doradb-storage/src/catalog/spec.rs` — numeric public schema inputs.
- [C2] `doradb-storage/src/catalog/table.rs` — compiled metadata, bounds, slot states, fingerprint, and CREATE outcome.
- [C3] `doradb-storage/src/catalog/storage/indexes.rs` — numeric index rows and key encoding.
- [C4] `doradb-storage/src/catalog/storage/mod.rs` — six-root bootstrap and compact persistence.
- [C6] `doradb-storage/src/catalog/index.rs` — index-DDL publication and proof.
- [C7] `doradb-storage/src/file/meta_block.rs`, `doradb-storage/src/file/multi_table_file.rs` — durable versions.
- [C8] `doradb-storage/src/row/ops.rs`, `doradb-storage/src/trx/undo/index.rs` — immediate and retained references.
- [C9] `doradb-storage/src/log/redo.rs`, `doradb-storage/src/log/format.rs` — catalog keys and exact index-DDL redo.
- [C10] `doradb-storage/src/table/layout.rs` — active generation layout.
- [C11] `doradb-storage/src/catalog/checkpoint.rs`, `doradb-storage/src/recovery/mod.rs` — replay-qualified effects.
- [C13] `doradb-storage/src/trx/admission.rs` — resolve-once user admission.
- [C14] `doradb-storage/src/session/managed_table_ops.rs`, `doradb-storage/src/table/index_ddl_plan.rs` — interpretation/finalization.
- [C15] `doradb-storage/src/catalog/storage/integrity.rs` — full-state integrity and bounded DROP proof.
- [C16] `doradb-storage/src/table/index_lifecycle.rs` — joined retirement and provisional reservations.
- [C17] `doradb-storage/src/catalog/storage/merge.rs`, `doradb-bench/src/workload/catalog.rs` — full-image folding and scale evidence.

### Conversation References

- [U1] Centralize tables, remove names, and collapse index keys into owner rows.
- [U2] Separate numeric schema, opaque descriptor, and binding projections.
- [U3] Preserve catalog-commit CTS before index-DDL root publication.
- [U4] Separate stable `IndexID(u32)` from reusable `IndexSlot(u16)`.
- [U5] Keep stable column identity at metadata boundaries and ordinals in DML.
- [U6] Implement only the existing four storage DDL operations.
- [U7] Exclude definition consistency spanning application planning/execution.
- [U8] Bound descriptors by the existing `VarByte` model.
- [U9] Perform one unsupported final format cutover without migration.
- [U11] Reserve unproven replay-visible CREATE IDs/slots until published replay progress permits release; use exact root proof.
- [U12] Separate durable catalog ordinals from user generations; resolve once with non-pinning tokens. Phase 2 allowed checked shared transient references.
- [U13] Phase 6 replaced caller-owned proposals with unlocked interpretation and private gated revalidation; stale attempts return zero-effect `SchemaChanged`.
- [U14] Use `u64` exclusive watermarks with exact `2^32` exhaustion.
- [U15] Preserve exact descriptor bytes; validate envelopes, not content.
- [U16] Validate complete final-state parent ownership and DROP absence.
- [U17] Join durable proof and exact reclamation outside DML; allow one current, retired, or destroying runtime generation per slot.
- [U18] Leave codec identity/version, registration, and dispatch above storage.
- [U19] Retain full-image checkpoints and prove scale through supported public DDL.
- [U23] Return finalized initial index IDs in input order from CREATE TABLE.
- [U24] Use managed-only roleless bindings in CREATE, with reverse enumeration and no post-CREATE binding mutation.
- [U25] Resolve optimistic versions and optional coherent schema/descriptor pairs; version-only reads avoid both projections.

### Source Backlogs

None. Implementation follow-ups are linked under Future Work.

## Decision

### Numeric Authority And Durable Layout

Storage owns the name-free physical schema needed for row encoding, indexes,
checkpoint, and independent recovery. Higher layers own descriptor semantics
and binding bytes. Every satellite has a `table_id` parent; primary keys or a
reverse index support table-centered reconstruction and DROP. The final dense
catalog root layout is: [D1], [D3], [C1], [C3], [C4], [U1], [U2]

| Slot | Logical table | Durable responsibility |
| ---: | --- | --- |
| 0 | `catalog.tables` | Existence, storage epoch, column/index ID watermarks, slot count |
| 1 | `catalog.columns` | Stable column ID, physical ordinal, value kind and flags |
| 2 | `catalog.indexes` | Stable index ID, physical slot, flags, ordered key payload |
| 3 | `catalog.table_descriptors` | Optional revision/epoch/fingerprint envelope and opaque bytes |
| 4 | `catalog.table_replay_silent_watermarks` | Existing subordinate replay-floor overlay |
| 5 | `catalog.table_bindings` | Namespace/key uniqueness and reverse table lookup |

`catalog.index_columns` and persisted indexed-column membership were removed;
active index definitions derive membership. Column IDs and ordinals, and active
index IDs and slots, are separately unique within a table. Ordered index keys
persist stable `ColumnID` values in a canonical version-1 little-endian payload
and compile once to physical ordinals. Decoding rejects invalid versions,
lengths, order/flag codes, empty keys, duplicate or unknown columns. [C2], [C3]

Raw checked `u64` column/index watermarks range over `0..=2^32` and strictly
exceed allocated identities. `u32::MAX` is allocatable; `2^32` returns typed
`ColumnIdExhausted` or `IndexIdExhausted`, and larger persisted bounds fail
integrity validation. The `u32` slot count represents the exclusive end of the
`u16` slot domain, subject to metadata-page capacity. No free list is persisted.
Table-file metadata retains the same mappings, epoch, and bounds. [D5], [U14]

The versioned 32-byte BLAKE3 fingerprint covers active IDs, positions, kinds/flags,
and ordered keys; it excludes table ID, epoch, bounds, gaps, retired generations,
and roots. Descriptors/bindings stay catalog-only. Fingerprinting does not prove
payload semantics. [C2], [U15]

### Admission And Exact Runtime Identity

Public indexed DML accepts `TableIndex(TableID, IndexID)` or a non-pinning
`ResolvedTableIndex` through one sealed argument interface. Normal admission
resolves the ID once against its captured layout; token admission directly
validates the exact ID/slot pair. Stale tokens cannot select a replacement
index. Streams and mutation traversals retain admission; all-index work
iterates active references without ID lookup. Row operations remain ordinal-
and slot-based. [C10], [C13], [U4], [U5], [U12]

Delayed user branches, undo, purge, cleanup, checkpoint sidecars, and retirement
retain `IndexRef { id, slot }`. Consumers validate that generation or act on the
captured old runtime. Catalog transient references share this carrier through
catalog-owned equal-ID/equal-slot construction. Durable catalog keyed row redo
instead stores its fixed native `u16` ordinal; it never adopts reusable user
identity semantics. [C8], [C9], [U12]

Both unmanaged and managed CREATE TABLE return `CreateTableOutcome` with the
TableID and exact finalized initial IndexIDs in input-definition order, including
an empty list when appropriate. The accepted plan carries this outcome through
successful mandatory completion; callers need no positional reconstruction or
immediate rediscovery read. CREATE INDEX returns its single stable ID. [C2], [U23]

### Replay Proof And Joined Slot Retirement

Each table-file slot is `Vacant`, `Active(IndexID, Empty | Present(nonzero root))`,
or `Retired(IndexID)`. Generation and root state share one vector, preventing
contradictory parallel representations. Absent secondary roots remain optional
in runtime APIs; read paths bypass disk-tree work while initial writers can
start empty. Retired state preserves exact CREATE-then-DROP proof. [D5], [C2]

Index-DDL redo carries table ID, index ID, and slot. Classification first filters
markers below `catalog_replay_start_ts`; an absent root or a root older than the
marker CTS is provisional. A sufficiently recent matching active generation
proves CREATE; a matching retired generation proves allocation followed by DROP,
or the DROP itself. Active DROP, vacant, and out-of-range states are provisional.
A conflicting generation in a sufficiently recent root fails integrity checks.
[C6], [C9], [C11], [U3], [U11]

Recovery skips all catalog DML of a root-unproven CREATE while reserving its
exact ID and slot. The effective allocator is at least the durable bound and
every widened reserved ID plus one. Release requires successful checkpoint
publication with `catalog_replay_start_ts > create_cts`; failed, no-op, or merely
prepared checkpoints do not release it. The running watermark never decreases.
After replay visibility ends, restart may recover an unproven ID only if no
later durable allocation consumed the gap; root-proven IDs are never reused.
A provisional CREATE over an older retired root is accepted only when recovery
also proves that underlying retirement reusable. [D7], [C16], [U11], [U14]

One Table-owned lifecycle joins durable, runtime, and provisional slot state.
Typed CREATE finalization selects the lowest eligible vacant or retired slot,
otherwise appends beyond reservations. Retired reuse requires exact root proof,
a published replay floor strictly beyond DROP CTS, completed runtime destruction,
and no provisional overlay. Slot count does not shrink. CREATE skips pinned or
destroying slots without waiting. [C14], [C16], [U4], [U17]

There is at most one current, retained, or destroying runtime generation per
slot. Cleanup destroys the exact captured runtime after its other owners drain,
retaining a `Destroying` sentinel across asynchronous destruction; terminal
failure blocks reuse and poisons the engine. Purge retries only registered table
IDs on checkpoint/purge/horizon events. Releasing a pin alone awaits the next
such event. No lifecycle lock or retirement scan enters foreground DML. Restart
reconstructs durable eligibility without surviving runtime pins. Existing CoW
block reachability remains independent of slot reuse. [D5], [D8], [C16], [U17]

### Managed DDL And Opaque Definition Contracts

Descriptor presence defines managed status. Payloads of 0 through 64,000 bytes
are preserved exactly, subject to complete-row and `VarByte` checks. Storage
owns revision, compiled epoch, and fingerprint; it owns no codec fields,
registration, content classification, or dereferencing policy. Unmanaged index
DDL rejects managed targets. [C14], [U2], [U8], [U15], [U18]

`ManagedTableOps` extends Session with operation-specific methods and a
synchronous `ManagedTableInterpreter`. CREATE interpretation returns one ID-free
ordered storage definition, descriptor, and zero or more bindings before any
TableID allocation. DoraDB assigns initial identities afterward, so initial
payload meaning cannot depend on subsequently assigned IDs. [C14], [U13], [U23]

Existing-table operations copy the current stable-ID schema and descriptor
under short metadata-S, release all authority, and invoke the callback once.
CREATE INDEX also supplies the proposed next stable ID. Callbacks return a typed
physical change and complete replacement bytes without slots or storage stamps.
DoraDB reacquires DDL exclusion and owned table/catalog gates, privately checks
epoch, revision, and effective CREATE allocator, then uses typed Table finalizers.
A stale attempt returns zero-effect `SchemaChanged`; retry belongs to the caller.
Interpreter errors retain their type, and panics occur outside engine authority.
[C14], [U13]

Accepted index DDL stages numeric and descriptor effects in one private
transaction, commits to obtain CTS, publishes the corresponding table root,
then installs runtime/layout history. Owned gates survive mandatory execution.
Recovery admits or skips the complete DDL effect group; root-publication failure
after commit follows fail-closed poisoning. DROP TABLE removes every applicable
projection in its existing lifecycle. [D2], [C6], [C11], [U3], [U16]

### Managed Bindings And Optimistic Resolution

Bindings contain only namespace ID, opaque key, and user TableID. Keys of 0
through 16,000 bytes are valid; uniqueness is `(namespace_id, binding_key)` and
a non-unique `table_id` index supports DROP and sorted reverse enumeration.
Every binding requires a descriptor; managed tables may have zero bindings.
CREATE validates the bundle before allocation, then stages fallible binding
insertion before invariant-only numeric/descriptor work in the same transaction.
Primary-index insertion arbitrates keys under data-IX, preserving expected
`DuplicateKey`/`WriteConflict` errors and complete rollback. [C14], [U24]

Resolution probes the binding in a disposable scope, then acquires target
metadata-S before canonical catalog claims and re-reads the key. Changed targets
release claims and retry; disappeared keys return `None`. Cancellation releases
partial acquisitions. Success returns an opaque equality-comparable version,
currently `(TableID, storage_epoch)`, and optionally one coherent stable-ID
schema/descriptor snapshot. The narrow path uses the admitted managed runtime
without central numeric-row reads, schema projection, fingerprint computation,
or descriptor access. Full mode validates the descriptor against that same
layout before copying its bytes. No locks escape the call. [C14], [U25]

An observed binding with an absent/unmanaged runtime is data corruption; full
resolution also rejects missing or inconsistent descriptors. Reverse enumeration
of a missing requested table instead returns `TableNotFound`. The returned
version is optimistic: a later cache check cannot protect subsequent planning
or execution. All currently supported descriptor replacements advance the
storage epoch; descriptor-only DDL will require revision-aware versioning.
[C14], [U7], [U24], [U25]

### Catalog Integrity And Checkpoint Persistence

Recovery checks every satellite against the complete central parent set after
replay/root classification and before reconciliation, rebuilding, and foreground
admission. Bindings additionally require descriptor ownership; descriptor stamps
must match recovered numeric schema. Checkpoint validates the prepared final
root set before publishing metadata, replay progress, or volatile eligibility.
Failure discards the mutable fork without advancing those boundaries. [C11],
[C15], [U16], [U24]

DROP locks all six catalog tables, uses bounded indexed discovery and deletes,
and proves read-your-writes absence of the central row and every satellite.
Its private lookup authority is tied to the retained operation scope. Replay-floor
state is captured before watermark deletion; post-lifecycle integrity failure
rolls back best-effort and poisons without partial committed DROP. [D4], [C15]

Checkpoint folds complete prior images by primary key and writes complete
replacement LWC/index images for tables requiring rewrite. Materialization still
clones surviving values and buffers output; cost scales with complete affected
images, not sparse DDL row count. Phase 8 fused projected parent/descriptor
validation by sharing decoded tables, columns, indexes, and descriptors;
watermarks and bindings retain selected-parent-column scans. This combined
validator retains schema/descriptor rows in addition to parent/managed ID sets.
It reads durable projected roots independently of any online cache. [C15], [C17]

`Session::checkpoint_catalog()` returns a public report separating changed-table
before/after counts from per-table logical reads, final compact bytes, successful
writes, and metadata writes. Logical accesses include cache hits; sparse row IDs
are not counts. No-op reports require no extra traversal. Catalog delete deltas
are rejected with typed integrity errors in both debug and release builds. [C17]

### Compatibility Boundary

Phase 3 changed catalog MTB version 5 to 6, table metadata version 7 to 8, and
redo version 5 to 6 together, including six roots and empty descriptor/binding
schemas. Older versions are rejected without migration or mixed-format support.
Phase 1's native-`u16` catalog-key adjustment was the explicit earlier exception
under redo version 5. Phase 7 removed the never-populated binding-role column
without another version bump; supported prior binding roots were empty.
Public numeric APIs and CREATE outcomes changed with the program. [C7], [C9],
[U9], [U12], [U23], [U24]

## Alternatives Considered

### Alternative A: One Physical-Schema Blob In `catalog.tables`

Why Not Chosen: Whole-schema logging enlarges hot rows and weakens localized
validation. Numeric owner rows retain explicit invariants; logical definitions
use the opaque descriptor. [D1], [C3], [U1], [U2]

### Alternative B: Compiler-Owned Placement Or Caller-Owned Proposals

Why Not Chosen: Placement depends on retirement/replay state. Caller-owned gates
can delay progress, and version repair leaks concurrency mechanics. DoraDB owns
unlocked interpretation and private revalidation/finalization. [C14], [U13]

### Alternative C: Positional Identity Or Read-After-Create Discovery

Why Not Chosen: Non-reused positions exhaust capacity; reused positions alias
cached intent. Stable IDs and opaque tokens preserve direct execution; CREATE
returns finalized identities without reconstruction or another read. [D6], [D9], [U4], [U12], [U23]

### Alternative D: Permanently Consume Every Committed CREATE Marker ID

Why Not Chosen: An unproven CREATE has no successful durable identity. Replay-
lifetime reservations avoid another persisted allocator authority ahead of the
catalog/root pair and its reconciliation burden. [C11], [U11]

## Implementation Phases

- **Phase 1: Catalog/User Index Reference Separation**
  - Task Doc: `docs/tasks/000288-catalog-user-index-reference-separation.md`
  - Task Issue: `#1029`
  - Phase Status: done
  - Implementation Summary: Qualified retained user references and separated fixed catalog keyed redo using native-u16 encoding; later Phase 2 unified transient carriers under checked domain construction.

- **Phase 2: Resolve-Once Runtime Layout And Generation Ownership**
  - Task Doc: `docs/tasks/000289-resolve-once-runtime-layout-generation-ownership.md`
  - Task Issue: `#1031`
  - Phase Status: done
  - Implementation Summary: Delivered stable-ID DML, non-pinning resolved tokens, one-time admission, exact delayed/runtime ownership, and replay-floor-qualified root proof; corrected catalog-branch MVCC traversal.

- **Phase 3: Atomic Numeric Format Cutover And Replay-Safe Allocation**
  - Task Doc: `docs/tasks/000290-atomic-numeric-format-cutover-and-replay-safe-allocation.md`
  - Task Issue: `#1033`
  - Phase Status: done
  - Implementation Summary: Shipped six numeric catalog roots, full-domain watermarks, unified generation/root states, canonical fingerprints, exact redo/reservations, optional runtime roots, and authoritative CREATE outcomes.

- **Phase 4: Central Catalog Parent Integrity**
  - Task Doc: `docs/tasks/000291-central-catalog-parent-integrity.md`
  - Task Issue: `#1035`
  - Phase Status: done
  - Implementation Summary: Added complete live/projected parent validation, operation-bound current index reads, bounded six-table DROP proof, and replay-floor capture before watermark deletion.

- **Phase 5: Checkpoint-Gated Index Slot Reuse**
  - Task Doc: `docs/tasks/000292-checkpoint-gated-index-slot-reuse.md`
  - Task Issue: `#1037`
  - Phase Status: done
  - Implementation Summary: Joined provisional, durable, and runtime gates on Table; delivered typed finalizers, lowest-slot reuse, exact asynchronous destruction, and targeted purge retry without foreground lifecycle access.

- **Phase 6: Opaque Managed Table Definitions And Proposal Boundary**
  - Task Doc: `docs/tasks/000293-opaque-managed-table-definitions-and-proposal-boundary.md`
  - Task Issue: `#1039`
  - Phase Status: done
  - Implementation Summary: Delivered unlocked interpretation, private stale revalidation, exact opaque descriptor persistence/validation, atomic numeric/descriptor effects, and owned gates through mandatory execution.

- **Phase 7: Managed Table Bindings And Versioned Resolution**
  - Task Doc: `docs/tasks/000294-managed-table-bindings-and-versioned-resolution.md`
  - Task Issue: `#1041`
  - Phase Status: done
  - Implementation Summary: Added roleless managed CREATE bindings, key-local conflict handling, reverse DROP/enumeration, two-pass versioned resolution, and managed-owner integrity with a projection-free narrow path.

- **Phase 8: Catalog Checkpoint Scale Proof**
  - Task Doc: `docs/tasks/000295-catalog-checkpoint-scale-proof.md`
  - Task Issue: `#1043`
  - Phase Status: done
  - Implementation Summary: Delivered public checkpoint reports and all nine release benchmark cases without target OOM; fused projected validation, corrected LWC header capacity accounting, and made delete-delta rejection effective in release builds.

## Validation Results

Task records cover format rejection, allocator bounds, exact/resolve-once
references, crash/replay reservations, both retirement-gate completion orders,
catalog corruption, atomic managed DDL, cancellation/binding races, and checkpoint
accounting. Implementation Notes retain phase-specific style and review evidence.

Phase 8 recorded 1,900 passing workspace tests, 1,803 alternate-libaio tests,
focused debug/release reader checks, and a 22-file branch style audit. Subsequent
benchmark coverage changes at `c7c578cd6a3d0d1fbc0bd217608a015b550a335f` passed
CI coverage, Clippy, libaio, and aggregate verification. The earlier local
CodeRabbit CLI was unavailable; task review records preserve the supplied
finding and its fix. These are implementation/CI records, not newly rerun suites.

All nine public-operation release cases completed at small, target, and stress
profiles: respectively 1,000/10,000/12,500 tables, ten bindings and two columns per
table, with 6,710,886/67,108,864/83,886,080 aggregate descriptor bytes. Each case
used a fresh baseline and one pending managed CREATE, CREATE INDEX, or DROP.

| Target case | Checkpoint writes, bytes | Sampled RSS growth, bytes |
| --- | ---: | ---: |
| Managed CREATE TABLE | 75,530,240 | 77,434,880 |
| Managed CREATE INDEX | 73,236,480 | 75,423,744 |
| DROP TABLE | 75,530,240 | 76,800,000 |

Target-to-stress bytes and sampled RSS growth followed the 1.25x population ratio
within page/sampling variation. Validation fusion reduced stress CREATE logical
reads from 559,611,904 to 376,963,072 bytes with unchanged physical reads and
writes. No streaming materialization or incremental persistence was needed.
The target is a workload assumption, not a format limit or a performance promise
for larger catalogs. RSS was sampled at 1 ms; single-run timings on a virtualized
host are informational. Write amplification uses changed tables' complete final
compact images as its denominator, not the tiny logical DDL delta. [C17], [U19]

Evidence used release rustc 1.98.0, iouring, Linux 7.0.14 on ten aarch64 vCPUs,
12,304,840 KiB RAM, 13,353,408 KiB swap, and Btrfs workspace roots. Task 000295
retains all nine numeric results, exact environment/configuration, and command
shape; ignored local artifacts are not the sole evidence source.

## Consequences

### Positive

- Physical execution and recovery remain independent of logical schema codecs.
- Stable intent survives physical slot reuse without repeated hot-loop lookup.
- Exact runtime ownership and replay proof prevent generation aliasing.
- Complete catalog validation detects satellites hidden from table-driven reads.
- Managed definitions and bindings share existing atomic DDL/recovery machinery.
- Checkpoint costs and the initial scale boundary now have measured evidence.

### Negative

- Public and durable compatibility changed without an upgrade path.
- Sparse root vectors retain their high-water size; stable IDs remain bounded.
- DDL allocation/recovery joins more lifecycle state, and pinned runtimes can
  delay reuse until cleanup is retried.
- Full resolution copies descriptors; public versions do not protect execution.
- Sparse catalog DDL still causes full-image reads, copies, and writes, while
  final-state integrity validation also reads unchanged projected catalog roots.

## Open Questions

None within the delivered scope; the follow-ups below do not gate completed phases.

## Future Work

There were no source or phase-related backlogs requiring closure. Open follow-ups:

- `docs/backlogs/000190-preallocate-catalog-lock-manager-slots.md` — benchmark fixed catalog lock resources.
- `docs/backlogs/000191-generalize-public-callback-error-boundaries.md` — generalize engine/user callback errors.
- `docs/backlogs/000192-cache-managed-table-definitions-in-current-catalog-state.md`
  — Phase 7 deferred coherent online definition caching beside current runtime
  metadata; hydrate after recovery validation and retain durable descriptors.
  Future descriptor-only DDL must atomically publish cache state and extend the
  private version with descriptor revision without loading payloads for checks.
- `docs/backlogs/000193-eliminate-repeated-leaf-node-loads-in-catalog-root-readers.md`
  — Phase 8 deferred reuse of validated leaf row metadata across enumeration and
  decoding; preserve typed integrity checks and rerun the nine-case matrix.
- `docs/backlogs/000194-admit-expected-managed-definition-versions-through-execution.md`
  — preserve the Phase 7 execution-consistency deferral: validate expected
  versions under canonical target admission and retain engine-owned authority
  through execution, including cancellation and index reclamation behavior.
- `docs/backlogs/000144-catalog-checkpoint-affected-block-compaction-strategy.md`
  — related checkpoint-strategy work, to revisit only with size/churn evidence.
  A base plus immutable deltas and periodic streaming compaction is another
  future persistence design; it must preserve PK folding, atomic replay-cursor
  publication, final merged-view integrity, and safe reclamation. Avoiding a
  full-catalog integrity read requires a separate inductive-integrity argument.
- `docs/backlogs/000104-stream-parallel-create-index-cold-build.md` — related
  streaming/parallel index-build work; this program retains coarse DDL exclusion.

## References

- `docs/public-api.md` — current numeric, managed DDL, binding, and report APIs.
- `docs/lock-system.md` — retained authority, lock ordering, and cancellation.
- [Phase 8 CI](https://github.com/jiangzhe/doradb/actions/runs/33950039975) — validation of the final benchmark-coverage commit.
