---
id: 000338
title: Limit single rows to RowPage and LWC block capacity
status: implemented
created: 2026-10-26
github_issue: 1164
---

# Task: Limit single rows to RowPage and LWC block capacity

## Summary

Newly inserted and updated logical rows must fit both a fresh RowPage and a
standalone LWC block in the current formats. Exact schema overhead and variable
lengths determine admission, independently of optional DML validation.
Oversized foreground writes return `OperationError::InvalidDmlInput` through
normal statement rollback; replay and catalog reconstruction retain typed
integrity failures.

New RowPage allocation also checks the actual aligned layout, preventing
initialization overflow and futile insertion retries. Capacity planning runs
only for new pages, and physical insertion reuses admission's byte measurement.

## Context

Source Backlogs:
- docs/backlogs/000007-lwc-oversize-rowpage-handling.md

Issue Labels:
- type:bug
- priority:high
- codex

This standalone task has no parent RFC. It completes only single-row admission
from backlog 000007; the backlog remains open for oversized multirow selections.
Issue: https://github.com/jiangzhe/doradb/issues/1164.

The implementation builds on task 000337, which reconciled LWC size estimation
with serialization and selected flat encodings for singleton integer columns.
That fixed estimator disagreement, but a valid hot row could still be too large
for its cold representation. A nonnullable VarByte payload of 65,464 bytes fits
a singleton RowPage but requires 65,539 LWC bytes; its standalone LWC maximum is
65,461 bytes.

Per-value and sparse-input validation could not establish aggregate row size.
Update paths also needed to account for unchanged values before deleting or
modifying the source image. Separately, the old occupancy heuristic requested
12 slots for 4,096 U8 columns, exceeding the aligned page layout; eight fit.

## Goals

- Enforce exact complete-image limits, including metadata, checksums, and
  alignment, without relying on compression shared with other rows.
- Reject oversized final update images before logical mutation across hot,
  frozen, cold, unique-key, index-driven, and full-table mutation paths.
- Preserve original rows, index identities, earlier successful statements,
  and engine health when foreground admission fails.
- Apply the same invariant during replay and catalog reconstruction, including
  validation opt-out, and choose a positive safe capacity for each new page.
- Keep admission allocation-free, inspecting lengths without copying or
  encoding payloads solely to establish capacity.

## Non-Goals

- Byte-aware splitting of source pages or multirow LWC selections, and
  unsupported RowID coverage handling.
- New formats, codecs, overflow storage, larger blocks, or configurable limits.
- Scanning or migrating existing data, or adding a separate DDL policy for
  empty schemas whose rows cannot fit.
- General page-density tuning or changes to checkpoint publication, transition
  ownership, the physical LWC row cap, or poison policy.
- Replacing optional shape/type validation or broadening trusted-input guarantees.

## Rejected Alternatives

- A new singleton LWC representation could preserve every valid hot image, but
  would change persisted formats and decoders. This task admits rows against
  the existing formats instead.
- Overflow storage could admit larger logical rows, but its references, I/O,
  publication, and reclamation require a separate architectural design.

## Plan

### Derived sizing and admission

`RowSizeLayout` is derived metadata rebuilt with the column layout, including
on deserialization. Named byte-width counts and inherent methods own exact
page arithmetic; format-owned helpers supply RowPage and singleton LWC costs.
No derived sizing state is serialized.

Admission adds outlined variable bytes to the singleton RowPage base and all
variable bytes to the singleton LWC base. Null values contribute no payload;
nullable metadata and fixed slots still count. Existing hot values are read
only after their null bits, so stale descriptors beneath nulls are ignored.
Checked arithmetic fails closed before offsets or counts are narrowed.

Updates merge the sorted sparse input with unchanged row values. Hot values
remain borrowed; cold values use the already decoded image. Fixed-only schemas
need only the cached base checks. Failure diagnostics include sizes and
operation context without attaching payload contents.

### Mutation boundaries and errors

Shared insertion validates before page acquisition, and batches preflight every
candidate before their first physical insert. Replacement helpers validate
before deleting their source. Hot and cold update owners validate the complete
result after acquiring ownership but before changing row values or deletion
state; statement settlement releases provisional ownership on rejection.

Foreground capacity failures use `OperationError::InvalidDmlInput`, preserving
callback error identity and normal rollback. Nontransactional catalog paths and
physical replay use `DataIntegrityError::InvalidPayload` beneath their consuming
runtime context. Catalog-only invariant ownership remains unchanged.

### Allocation and replay

The existing occupancy estimate remains a hint. Exact aligned fixed storage
plus outlined payloads bounds that hint; bounded search reduces it when needed.
Singleton admission proves that at least one row fits, and initialization checks
the fixed layout before writing offsets.

Planning is lazy across session-cache and free-list lookup, including catalog
insertion. Cache hits and failed cache reloads do not calculate new-page
capacity. Physical insertion carries the admitted outlined-byte count unchanged
through retries on full or frozen pages.

Replay validates inserted and merged updated images before mutation, preserving
successful earlier operations and dirty state if a later operation fails.
Recorded page capacities must fit exactly; replay rejects invalid layouts
instead of changing their logged RowID reservations. Existing oversized replay
images fail startup through typed errors; no migration is performed.

## Implementation Notes

Implemented mandatory complete-row admission across foreground and recovery
writes, with safe page allocation and normal statement rollback. Public API
signatures and persisted formats are unchanged.

### Review and refinements

- The path audit covered shared insertion, batch and replacement preflight,
  hot/frozen/cold updates, unique callbacks, index/table scans, catalog
  reconstruction, and physical replay. Reserved raw page primitives retain
  their physical contracts; undo restores previously admitted images.
- Review flattened width counts into short named fields and moved related
  arithmetic onto `RowSizeLayout`. Small sizing helpers are inline; capacity
  error construction is cold. Cross-domain catalog reports retain meaningful
  context at each conversion.
- Benchmark investigation led to lazy page-capacity planning and reuse of the
  measured outlined length. Validator fusion and a common payload-limit fast
  path were not included in this change.
- Coverage exposed an existing retired-index cleanup test race: a completed
  purge cycle did not prove runtime removal. The test now waits on removal
  using existing purge events and retry requests. Production cleanup is
  unchanged; the repaired case passed 100 stress iterations.
- The CREATE INDEX key-specification review finding was skipped as unreachable
  under current live-table constraints. A 64 KiB metadata block stores 14 bytes
  per column, bounding distinct keys to at most 4,681 and their encoded payload
  to 23,408 bytes, below both catalog-row limits. Five focused existing guard
  tests passed. This conclusion concerns CREATE INDEX on existing tables.

### Verification

- The final implementation passed the workspace suite: 2,484 tests passed and
  three were skipped. Its Rust source hashes still match the optimized
  benchmark snapshot at resolution; implementation tests were not repeated
  solely for this documentation update.
- The profiling-disabled storage pass before the later insertion optimization
  passed 2,212 tests with two skipped; strict profiling-disabled Clippy passed.
- The required resolution style gate passed on 2026-11-01 for 21 branch-diff
  Rust files and 603 selected test contracts, including formatting and strict
  workspace Clippy.
- Assertion review retained independent initialized-page and serialized-LWC
  oracles, original row/index identities, statement-prefix rollback, null
  state, replay dirty state, and cache-planning counts. Freeze/checkpoint
  predicates and ownership probes establish lifecycle state without sleeps.
- Shared fixtures avoid duplicated setup. Physical-format, transactional
  ownership, no-undo replay, cache-owner, and redo tests retain distinct oracles
  and lifecycle coverage.

Earlier production coverage, before the sizing refactor and insertion
optimization, was 91.16% across 16 changed files and 263/265 added executable
lines (99.25%). Those figures are not coverage of the final source snapshot.
`mem_table.rs` was 79.19% overall, with all 23 then-added lines covered; remaining
legacy adapter and invariant-failure gaps were outside this task. The local
report is `target/coverage/task-000338.md`.

### Performance

The final comparison used 1M sequential inserts, a 128-byte payload, no index,
one worker/session, and 100 single-row statements per transaction. Each build
had six balanced measured runs per sync mode after a warm-up, using fresh roots
on the same btrfs disk, identical release settings, and Rust 1.99.0.

| Build | Median with fsync | Median without sync |
| --- | ---: | ---: |
| Original baseline, before capacity checks | 11.840 s | 1.690 s |
| Capacity checks before insertion optimization | 15.012 s | 1.713 s |
| Final optimized implementation | 10.335 s | 1.673 s |

The no-sync median improved 2.4% over the preceding validation build; its mean
improved only 0.2%, and ranges overlapped. Fsync runs ranged from 9.162 to
31.816 seconds, so the larger median difference cannot isolate code speedup.
All builds inserted the same rows, allocated 2,233 row pages, and emitted
213,946,368 redo bytes. Admission remains mandatory.

Local artifacts, including plans, raw runs, hashes, and CPU measurements, are
in `target/bench-000338/optimization/report.md`. They are ignored benchmark
artifacts; the table and limitations above preserve the durable result.

## Impacts

- Foreground DML, catalog reconstruction, physical replay, and page allocation
  now share the same single-row capacity invariant.
- Previously accepted oversized hot images can now return an input error;
  recovery encounters fail with typed integrity context instead of attempting
  unsafe mutation or repeatedly allocating unusable pages.
- Schema costs are cached and per-row work scans lengths. New-page planning
  and the writer's duplicate length scan are removed from cache-hit insertion.
- Public API, recovery, and checkpoint documentation describe mandatory limits
  and distinguish singleton representability from multirow checkpoint sizing.
- No persisted format, public signature, schema migration, or checkpoint
  splitting policy changed.

## Test Cases

- Physical output oracles cover all value kinds, nullable fields, empty/inline/
  outlined bytes, mixed widths, metadata round trips, alignment, and bitmaps.
- Exact fits and one-byte overflow are covered independently for RowPage and
  LWC, including the 65,461/65,462 singleton VarByte boundary, aggregate lengths,
  and schema-only oversize. Null and 6/7-byte transitions include stale fields.
- Narrow/wide allocation cases initialize real pages and insert their planned
  rows; overflowing initialization preserves the prior page image. Cache reuse
  skips planning, fresh allocation calculates once, and reload errors retain
  their I/O cause without planning.
- Foreground rejection covers inserts, batches, callbacks, scans, and hot,
  frozen, and cold updates with validation enabled and disabled. Assertions
  check original values and index identities, rollback, ownership release,
  earlier successful statements, and subsequent valid writes.
- Catalog reconstruction and replay reject before mutation while preserving
  successful prefixes. Logged capacity overflow is rejected without clamping;
  the sparse replay fixture separately retains existing-page space failure.
- Admitted boundary values encode, checkpoint, reopen, and read back through
  production APIs. Source selections deliberately fit a block so these tests
  do not depend on deferred byte-aware splitting.

## Open Questions

None within the implemented single-row scope.

[Backlog 000007](../backlogs/000007-lwc-oversize-rowpage-handling.md) remains open:
individually admissible rows can still form an oversized multirow selection.
It now records this task's completed admission portion and the remaining
value-byte partitioning work. Single-row admission does not complete the source
backlog; multirow splitting was explicitly excluded.

[Backlog 000208](../backlogs/000208-bound-checkpoint-rowid-coverage-spans.md)
retains unsupported RowID coverage admission. Supporting larger logical rows or
preserving historical oversized replay images would require separate design.
