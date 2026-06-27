---
id: 0022
title: Catalog-Backed Redo Log Truncation
status: draft
tags: [redo, recovery, catalog, checkpoint]
created: 2026-06-26
github_issue: 767
---

# RFC-0022: Catalog-Backed Redo Log Truncation

## Summary

Add recovery-safe physical redo-log truncation by making catalog checkpoint scan
produce reusable redo segment progress, persisting a durable first-retained redo
file marker in `catalog.mtb`, computing truncation eligibility from both catalog
and table replay boundaries, and exposing truncation as a `Session` maintenance
API.

## Context

Doradb currently narrows redo replay logically through catalog and table
checkpoint metadata, and recovery can skip sealed redo files whose CTS range is
fully below the computed replay floor. The old files remain on disk because
startup still treats missing prefix redo sequences as corruption. Physical
truncation needs a durable way to distinguish intentional prefix removal from
accidental file loss, and it needs a proof that removed files are useless for
catalog recovery and for every retained table recovery boundary.

Catalog checkpoint already scans durable redo from `catalog_replay_start_ts`
through the redo durable watermark. This RFC makes that scan a producer of
file-level retention progress, while keeping final unlink decisions gated by
the full global replay floor. A catalog-safe floor alone is not sufficient:
user-table roots carry independent `heap_redo_start_ts` and
`deletion_cutoff_ts` bounds, and pending dropped tables can still require old
table redo until the catalog checkpoint has made their absence durable.

Issue Labels:
- type:epic
- priority:medium
- codex

## Design Inputs

### Documents

- [D1] `docs/architecture.md` - redo is a committed-only log; catalog
  checkpoint establishes catalog replay boundaries and recovery uses a coarse
  replay floor across catalog and loaded table boundaries.
- [D2] `docs/checkpoint-and-recovery.md` - defines
  `catalog_replay_start_ts`, per-table `heap_redo_start_ts`,
  `deletion_cutoff_ts`, coarse replay floor, log replay rules, and dropped-table
  cleanup gates.
- [D3] `docs/redo-log.md` - documents current redo file family layout, sealed
  segment metadata, logical segment skip, contiguous sequence requirements, and
  the need for durable first-retained metadata before physical truncation.
- [D4] `docs/table-file.md` - table roots persist `pivot_row_id`,
  `heap_redo_start_ts`, `deletion_cutoff_ts`, `root_ts`, and dropped-table file
  deletion waits for catalog replay progress.
- [D5] `docs/transaction-system.md` - transaction recovery treats checkpoint
  metadata, table roots, and real redo headers as stable timestamp carriers;
  no-log commits must not seed recovery or truncation.
- [D6] `docs/process/issue-tracking.md` - significant architectural work is
  planned through RFCs and later broken into task documents and issues.
- [D7] `docs/process/unit-test.md` - routine validation uses `cargo nextest run
  -p doradb-storage`, with alternate backend validation for storage I/O changes.

### Code References

- [C1] `doradb-storage/src/log/mod.rs` - discovers redo files and currently
  rejects missing prefix sequences or internal sequence gaps.
- [C2] `doradb-storage/src/recovery/stream.rs` - validates redo segment
  metadata, plans replay suffixes, and skips sealed files whose
  `max_redo_cts < replay_floor`.
- [C3] `doradb-storage/src/recovery/timeline.rs` - models catalog and table
  replay bounds used to compute the coarse recovery floor.
- [C4] `doradb-storage/src/recovery/mod.rs` - loads table roots, tracks table
  replay bounds, and skips checkpoint-covered unknown-table redo below the
  catalog replay boundary.
- [C5] `doradb-storage/src/catalog/checkpoint.rs` - scans redo for catalog
  checkpoint, includes/skips/stops DDL according to catalog safety, and uses the
  durable redo watermark as scan upper bound.
- [C6] `doradb-storage/src/catalog/storage/checkpoint.rs` - applies catalog
  checkpoint batches and publishes new `catalog.mtb` metadata roots.
- [C7] `doradb-storage/src/file/multi_table_file.rs` - stores `catalog.mtb`
  root metadata and exposes checkpoint snapshots through `catalog_replay_start_ts`.
- [C8] `doradb-storage/src/file/table_file.rs` - stores user-table root replay
  metadata in resident active roots.
- [C9] `doradb-storage/src/table/persistence.rs` - user-table checkpoint can
  publish metadata-only roots and advances heap/deletion replay boundaries.
- [C10] `doradb-storage/src/trx/purge.rs` - dropped-table runtime/file queues
  gate physical file deletion on active snapshot and catalog checkpoint progress.
- [C11] `doradb-storage/src/session.rs` - public maintenance and DDL operations
  are exposed through `Session`, including table checkpoint operations.
- [C12] `doradb-storage/src/conf/trx.rs` - recovery prepares redo discovery and
  replay planning after catalog storage is available to the transaction-system
  startup path.

### Conversation References

- [U1] Initial request: add a public API to truncate redo log files; only sealed
  files should be candidates; deletion must be proven useless against catalog
  and table checkpoint progress; recovery must detect missing necessary logs.
- [U2] User direction: prefer catalog-scan-leveraged truncation where catalog
  checkpoint records a safe floor and truncation consumes that floor.
- [U3] User questions: decide durable vs in-memory floor storage, whether
  catalog scan must become incremental, and how long-idle tables should behave.
- [U4] User direction: proceed with RFC workflow, keep implementation phases
  compact, and avoid a final documentation/cleanup/validation-only phase.
- [U5] User corrections: public API belongs on `Session`, API dependencies must
  be documented explicitly, and dropped-table behavior for long-uncheckpointed
  tables must be clarified.

### Source Backlogs

- [B1] `docs/backlogs/000032-deletion-watermark-meta-redo-expansion-for-log-truncation.md`
  - source backlog for deletion watermark metadata/redo expansion needed by log
  truncation policy.

## Decision

Redo truncation will be catalog-backed but not catalog-only. Catalog checkpoint
scan will produce reusable segment progress, but a file can be physically
removed only when it is sealed and its real redo CTS range is older than the
global truncation floor derived from catalog and all retained table replay
boundaries. [D2], [D3], [C2], [C3], [C5], [U2]

Persist a durable first-retained redo file sequence in `catalog.mtb` overlay
metadata, tentatively named `first_redo_log_seq`. Redo discovery and
recovery must accept missing prefix files only when their sequence is strictly
below this durable marker; any missing file at or above the marker remains a
data-integrity error. This marker is advanced durably before old files are
unlinked, so a crash after marker publication but before unlink can leave extra
obsolete files without weakening recovery. [D3], [C1], [C7], [C12], [U3]

Do not store the catalog scan floor only in memory. In-memory scan observations
are allowed and useful for avoiding repeated planning work, but they are an
optimization. The durable marker is the recovery contract that makes missing
prefix files intentional instead of corruption. [D3], [C1], [C2], [U3]

Use the existing catalog checkpoint scan as the incremental redo-reading path.
The scan already starts from `catalog_replay_start_ts` and stops at the durable
redo watermark. This RFC does not introduce a second catalog-scan cursor.
Instead, `RedoLogStream` or the replay planner will expose segment/file
observations to the catalog checkpoint flow so it can update in-memory
catalog-safe retention progress while it scans. [C2], [C5], [U2], [U3]

The truncation floor is not `catalog_replay_start_ts` alone. It is the minimum
of catalog replay progress and every retained user table replay start:

```text
table_replay_start_ts = min(heap_redo_start_ts, deletion_cutoff_ts)

truncate_floor = min(
    catalog_replay_start_ts,
    all live user table table_replay_start_ts,
    all pending dropped table retained table_replay_start_ts
)
```

This means `catalog_replay_start_ts` may be greater than a table root timestamp
or table replay boundary. Catalog checkpoint can advance across redo that is not
needed for catalog recovery, including user-table data/checkpoint redo. That
does not prove table redo is disposable. [D1], [D2], [C3], [C4], [C5], [U5]

Long-idle live tables block physical truncation until their table replay
boundaries advance. User-table checkpoint already supports metadata-only
publication that can advance `deletion_cutoff_ts` and checkpoint heartbeat
metadata even when there is no new row data. This RFC will document
`Session::checkpoint_table` as the explicit dependency for moving idle-table
floors; automatic idle checkpoint scheduling is out of scope. [D2], [D4], [C8],
[C9], [U3], [U5]

Dropped tables block truncation only while catalog absence is not durable.
When a table is dropped, the drop path or dropped-table cleanup path must retain
the table replay floor captured from the active root before the table runtime is
forgotten. While `catalog_replay_start_ts <= drop_cts`, that retained floor
participates in truncation-floor calculation. Once catalog checkpoint publishes
`catalog_replay_start_ts > drop_cts`, recovery no longer loads the table and
can skip old table redo as checkpoint-covered unknown-table redo, so the dropped
table no longer blocks truncation even if it had not checkpointed for a long
time before it was dropped. [D2], [D4], [C4], [C10], [U5]

Expose public redo truncation through `Session`, not `Engine`. The primary API
will be a session maintenance operation, tentatively:

```rust
Session::truncate_redo_log(&mut self) -> Result<RedoTruncationOutcome>
```

It should reject calls from a session with an active transaction, follow the
existing session operation admission model, and use internal runtime services
for planning, marker publication, and unlink. `Engine` remains the owner and
component container, not the public maintenance command surface. [C11], [U5]

Expose catalog checkpoint progress explicitly rather than hiding it inside
truncation. The program should add a session-level catalog checkpoint
maintenance API, tentatively:

```rust
Session::checkpoint_catalog(&mut self) -> Result<CatalogCheckpointOutcome>
```

`Session::truncate_redo_log` consumes already durable catalog and table floors;
it may report catalog floor blockers instead of performing an implicit catalog
checkpoint scan. This keeps the dependency between catalog checkpoint and
truncation visible to callers and tests. [C5], [C11], [U5]

Serialize catalog checkpoint scan, truncation planning, and marker/unlink
execution through a narrow redo-retention gate. A truncation operation must not
unlink a file that a concurrent catalog scan planned to read but has not yet
opened. The initial design should prefer one single-flight retention gate over
lock-free racing between scan and unlink. [C2], [C5], [C10]

The floor computation should be on-demand in v1. Its expected cost is
`O(live_user_tables + pending_dropped_tables + sealed_candidate_files)`, and it
must use resident active roots and retained dropped-table metadata rather than
opening every table file. If this becomes expensive, later work can maintain a
cached heap of table floors or background retention state. [C8], [C10], [C11],
[U3], [U5]

## Alternatives Considered

### In-Memory Floor Only

- Summary: Catalog checkpoint scan records an in-memory floor; truncation
  deletes files below that floor without adding durable first-retained metadata.
- Analysis: This is insufficient because startup currently treats missing
  prefix sequences as corruption, and after a crash there is no persistent proof
  that missing files were intentionally removed.
- Why Not Chosen: Physical deletion requires a durable recovery contract.
  In-memory state can optimize planning but cannot authorize missing log files.
- References: [D3], [C1], [U3]

### Direct Truncation Scan Without Catalog Integration

- Summary: `Session::truncate_redo_log` discovers sealed files, reads table and
  catalog snapshots, validates segment headers, and deletes candidates without
  using catalog checkpoint scan observations.
- Analysis: This can be correct if it still persists the durable marker and
  computes the full global floor, but it duplicates work that catalog checkpoint
  already performs over the redo stream.
- Why Not Chosen: The approved direction wants catalog checkpoint progress to
  feed truncation. The direct-scan-only approach remains a fallback path when
  catalog scan observations are unavailable, not the central design.
- References: [C2], [C5], [U2]

### Catalog Floor Alone

- Summary: Treat `catalog_replay_start_ts` or a catalog-safe file floor as
  sufficient proof to delete sealed redo prefix files.
- Analysis: Catalog checkpoint can advance across user-table data and table
  checkpoint redo because those records are not catalog state. User-table
  recovery still depends on per-table heap and deletion replay bounds.
- Why Not Chosen: This would delete redo needed by long-idle live tables or
  pending dropped tables whose catalog absence is not yet durable.
- References: [D2], [C3], [C4], [C5], [U5]

### Engine-Level Public API

- Summary: Expose redo truncation directly on `Engine` rather than `Session`.
- Analysis: Engine owns runtime components and shutdown, but existing user and
  maintenance operations are exposed through `Session`, including table
  checkpoint and DDL.
- Why Not Chosen: Redo truncation is a session maintenance operation and should
  follow the established public operation surface and active-transaction checks.
- References: [C11], [U5]

### Background Retention Manager In The First Program

- Summary: Add a background scheduler that observes table/catalog checkpoint
  progress and automatically truncates redo files.
- Analysis: This is a reasonable future evolution once the manual API,
  durable-marker protocol, and retention proof are stable.
- Why Not Chosen: It broadens scope beyond the requested public API and adds
  policy questions around scheduling, idle-table checkpoint triggering, and
  operational observability. The initial RFC keeps truncation manual and
  deterministic.
- References: [D6], [U1], [U4]

## Unsafe Considerations

No new `unsafe` code is expected. File deletion should use existing file-system
helpers or safe standard-library unlink boundaries already wrapped by storage
modules. If implementation unexpectedly touches raw file descriptors or
platform-specific deletion behavior, the corresponding task must follow the
repository unsafe guidance and keep the unsafe boundary local and documented.

## Implementation Phases

- **Phase 1: Durable Retention Marker And Discovery**
  - Scope: Add `first_redo_log_seq` to catalog metadata persisted in
    `catalog.mtb`; load it during transaction-system recovery setup; change redo
    discovery to accept missing prefix files below the marker and reject gaps at
    or above it.
  - Goals: Recovery can distinguish intentionally removed prefix files from
    accidental missing necessary files; marker defaults preserve current
    behavior for existing databases; no physical unlink is implemented yet.
  - Non-goals: Catalog scan observation caching, truncation planning, public
    truncation API, background retention policy.
  - Prerequisites: Catalog storage is available before redo replay planning in
    startup, as it is today.
  - Phase-local Choices: Exact marker field name and encoding in
    `MultiTableMetaBlock`; whether marker advancement is exposed through a
    narrow catalog-storage helper or a transaction-system helper.
  - Task Doc: `docs/tasks/000194-durable-retention-marker-for-redo-log.md`
  - Task Issue: `#769`
  - Phase Status: done
  - Implementation Summary: Implemented durable first-retained redo marker in catalog metadata and marker-aware retained-suffix discovery for startup recovery and catalog checkpoint scan. [Task Resolve Sync: docs/tasks/000194-durable-retention-marker-for-redo-log.md @ 2026-06-27]

- **Phase 2: Catalog Scan Segment Progress**
  - Scope: Extend redo stream/planner outputs so catalog checkpoint scan can
    observe sealed segment file sequences and CTS ranges while scanning; add an
    internal in-memory retention progress record; expose a session-level catalog
    checkpoint maintenance API.
  - Goals: A catalog checkpoint run advances catalog recovery state and records
    reusable catalog-safe segment progress without performing physical
    truncation; callers can explicitly run catalog checkpoint before truncation.
  - Non-goals: Global table-floor planning, dropped-table floor retention,
    marker advancement, file unlink.
  - Prerequisites: Phase 1 marker/discovery contract exists, even though this
    phase does not advance the marker.
  - Phase-local Choices: Catalog scan uses a catalog-specific redo planner path
    that emits sealed segment summaries. No public catalog checkpoint outcome
    was added; the apply outcome remains crate-private. The in-memory retention
    record is owned by `TransactionSystem` and stores `first_retained_file_seq`,
    `catalog_replay_start_ts`, and per-segment summaries.
  - Task Doc: `docs/tasks/000195-catalog-scan-segment-progress.md`
  - Task Issue: `#771`
  - Phase Status: done
  - Implementation Summary: Implemented catalog-specific sealed segment summaries and TransactionSystem-owned in-memory catalog redo retention progress while keeping checkpoint_catalog returning Result. [Task Resolve Sync: docs/tasks/000195-catalog-scan-segment-progress.md @ 2026-06-27]

- **Phase 3: Global Truncation Floor Planning**
  - Scope: Add deterministic truncation planning that combines catalog-safe
    progress, resident live-table active-root replay bounds, and retained
    pending dropped-table replay bounds; extend dropped-table queue entries to
    retain table replay floors until catalog absence is durable.
  - Goals: The engine can compute eligible sealed prefix files and blockers
    without unlinking files or opening every table file; long-idle live tables
    and pending dropped tables are represented explicitly in the plan.
  - Non-goals: Marker advancement, physical file deletion, automatic idle-table
    checkpoint scheduling.
  - Prerequisites: Phase 2 provides catalog-safe segment observations or a
    fallback segment-summary path; table active roots expose heap/deletion
    replay bounds in memory.
  - Phase-local Choices: Public vs test-only exposure of a dry-run plan; exact
    blocker taxonomy; whether live-table root snapshots are collected through a
    new catalog iterator or transaction-system helper.
  - Task Doc: `docs/tasks/000196-global-truncation-floor-planning.md`
  - Task Issue: `#773`
  - Phase Status: done
  - Implementation Summary: Implemented internal dry-run redo truncation planning from catalog-safe progress, live table floors, pending dropped-table floors, and retained segment metadata, with dropped-table floor retention and explicit candidate/blocker reporting. [Task Resolve Sync: docs/tasks/000196-global-truncation-floor-planning.md @ 2026-06-27]
  - Related Backlogs:
    - `docs/backlogs/000032-deletion-watermark-meta-redo-expansion-for-log-truncation.md`

- **Phase 4: Session Truncate API**
  - Scope: Add `Session::truncate_redo_log`, publish marker advancement before
    unlinking eligible sealed prefix files, remove files below the new marker,
    and return outcome statistics and blockers.
  - Goals: Users can physically remove obsolete sealed redo files through a
    public session maintenance API; recovery remains strict for any missing file
    at or above the durable first-retained sequence.
  - Non-goals: Background truncation, automatic table checkpoint triggering,
    sparse-file hole punching, truncating unsealed active files.
  - Prerequisites: Phase 3 planning is available and phase 1 marker persistence
    is recovery-tested.
  - Phase-local Choices: Exact outcome struct fields; retry behavior for unlink
    errors; whether partial unlink success is reported per file or summarized;
    exact retention gate placement.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

## Consequences

### Positive

- Physical redo file removal becomes recovery-safe instead of relying on manual
  operator deletion.
- Catalog checkpoint scan work feeds retention planning instead of being used
  only for catalog state.
- Missing redo prefix files become valid only under a durable marker, preserving
  strict corruption detection for accidental gaps.
- Long-idle table and pending dropped-table blockers are explicit, testable, and
  visible to callers.
- The public API follows the existing `Session` maintenance surface.

### Negative

- `catalog.mtb` metadata gains another recovery-critical overlay field.
- Truncation progress can be blocked by idle tables until callers run table
  checkpoints.
- Initial floor computation is linear in live and pending dropped tables.
- Catalog checkpoint scan and truncation need a shared retention gate to avoid
  scan/unlink races.

## Open Questions

- Exact public names for catalog checkpoint and redo truncation outcome types
  are left to phase task design.
- Exact blocker taxonomy is left to Phase 3, but it must distinguish at least
  catalog floor, live table floor, pending dropped table floor, unsealed file,
  and unlink failure.

## Future Work

- Background redo retention scheduler.
- Automatic idle-table checkpoint scheduling or hints.
- Persistent retention telemetry and user-facing truncation metrics.
- Sparse-file hole punching or intra-file truncation. This RFC removes whole
  sealed files only.
- Cached global floor maintenance if on-demand planning becomes too expensive.

## References

- `docs/rfcs/0020-redo-log-format-and-integrity.md`
- `docs/rfcs/0021-redo-log-fixed-block-read-write-path.md`
- `docs/redo-log.md`
- `docs/checkpoint-and-recovery.md`
- `docs/backlogs/000032-deletion-watermark-meta-redo-expansion-for-log-truncation.md`
