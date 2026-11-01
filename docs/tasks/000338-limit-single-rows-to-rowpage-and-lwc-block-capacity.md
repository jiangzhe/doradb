---
id: 000338
title: Limit single rows to RowPage and LWC block capacity
status: proposal
created: 2026-10-26
github_issue: 1164
---

# Task: Limit single rows to RowPage and LWC block capacity

## Summary

Enforce a mandatory storage invariant: every newly inserted or updated logical
row must fit both a fresh RowPage and a standalone LWC block in the current
formats. Calculate exact schema-dependent overhead plus variable-value lengths,
reject oversized user writes with `OperationError::InvalidDmlInput`, and apply
the same capacity policy during recovery even when optional DML validation is
disabled.

Bound new RowPage capacity using the actual aligned layout so an admitted row
cannot cause an overflowing page initialization or an allocation retry loop
that can never succeed. This task does not split source pages or LWC blocks.

## Context

Source Backlogs:

- docs/backlogs/000007-lwc-oversize-rowpage-handling.md

Issue Labels:

- type:bug
- priority:high
- codex

This is a standalone task with no parent RFC. The approved scope is mandatory
single-row limits; splitting is explicitly excluded. Backlog 000007 is only
partially addressed and must remain open after this task for oversized
multirow selections.

The researched base is `8ead90bc`, which includes implemented task 000337,
`docs/tasks/000337-make-lwc-integer-encoding-and-block-sizing-consistent.md`.
That task reconciles LWC admission with serialization, selects flat integer
encoding for singleton columns, and checks final body capacity. The present
task addresses genuinely unrepresentable row images rather than estimator or
serializer disagreement.

Removing persisted RowIDs did not make hot-to-cold size monotonic. One
nonnullable VarByte value can occupy 65,464 bytes in a one-row RowPage, while
the same row requires 65,539 bytes as LWC. The maximum standalone LWC value for
that schema is 65,461 bytes. Ordinary committed inserts and checkpointing
reproduced this boundary during the source investigation.

Current `DmlValidator` checks shape, types, nullability, and individual byte
lengths, but not aggregate physical row size. Sparse-update validation cannot
establish the size of unchanged columns. Hot update code can reserve bytes or
turn an old row into a delete before replacement insertion; cold update code
similarly finalizes deletion before inserting its successor. Capacity rejection
must precede those logical mutations.

The existing `estimate_max_row_count` is an occupancy heuristic, not a layout
proof. For 4,096 nonnullable U8 columns, one row needs 41,016 RowPage bytes and
49,208 LWC bytes. The heuristic requests 12 slots, whose aligned RowPage layout
needs 73,784 bytes. Admission and safe new-page sizing therefore belong to the
same correctness change.

Design references:

- `docs/architecture.md`: hot/cold storage and RowID ownership.
- `docs/public-api.md`: DML validation opt-out, statement atomicity, and errors.
- `docs/data-checkpoint.md`: exact LWC admission and the irreversible failure boundary.
- `docs/recovery.md`: physical user-page replay and logical catalog replay.
- `docs/process/coding-guidance.md`, `docs/process/unit-test.md`, and
  `docs/process/issue-tracking.md`: typed errors, validation, and task workflow.
- `doradb-storage/src/catalog/table.rs`: `TableColumnLayout` construction.
- `doradb-storage/src/row/mod.rs`: aligned layout and row-count estimation.
- `doradb-storage/src/lwc/mod.rs` and `serde.rs`: serialized costs and integer plans.
- `doradb-storage/src/table/dml_validator.rs`, `mutate.rs`, `hot.rs`, and `access.rs`:
  shared validation and final physical mutation owners.
- `doradb-storage/src/table/mem_table.rs`, `table/recover.rs`, and
  `recovery/mod.rs`: reconstruction and replay boundaries.

## Goals

- Admit a row only when its complete fresh-RowPage and singleton-LWC images
  fit their respective capacities, including all metadata and alignment.
- Preserve the exact maximum supported by the schema and current formats,
  without borrowing compression benefits from other rows.
- Validate complete post-update images before logical mutation, including hot,
  frozen, cold, unique-key, index-driven, and full-table mutation paths.
- Enforce capacity independently of foreground and recovery validation opt-outs.
- Reject invalid foreground input without poisoning the engine or losing the
  original row, indexes, or earlier successful statements in the transaction.
- Guarantee a positive, safe new-page capacity for each admitted inserted row.
- Keep size checks allocation-free and avoid copying variable payloads solely
  for admission. Cache schema-only costs and inspect variable-value lengths.

## Non-Goals

- Splitting source row pages, repartitioning LWC selections, or solving the
  many-small-rows overflow case from backlog 000007.
- Singleton LWC formats, new codecs, overflow storage, larger blocks, or format migrations.
- A configurable row-size ceiling, new per-column length declarations, or
  arbitrary global limits below the actual format capacities.
- Changes to the physical LWC row cap, RowID coverage rules, checkpoint
  publication, transition ownership, or poison policy.
- Scanning or migrating existing stored data to repair oversized rows.
- General page-density optimization, compaction, streaming serialization, or
  benchmark infrastructure.
- Replacing optional shape/type validation or expanding trusted-input guarantees
  beyond the mandatory capacity invariant.

## Rejected Alternatives

1. A compact singleton LWC encoding could preserve every physically valid hot
   row through a size-dominating representation. It introduces a persisted
   format and decoder change, while the approved task selects admission limits
   for the current formats.
2. Overflow storage could support logical rows larger than one physical page.
   Its additional references, I/O, publication, and reclamation responsibilities
   require a separate architectural program.

## Plan

### 1. Schema-derived layout and per-row measurement

Cache a small internal `RowSizeLayout` alongside `TableColumnLayout` in
`catalog/table.rs`:

```rust
struct RowSizeLayout {
    row_page_base_bytes: usize,
    lwc_base_bytes: usize,
    b1_count: usize,
    b2_count: usize,
    b4_count: usize,
    b8_count: usize,
    nullable_cols: usize,
}

struct RowSize {
    total_var_bytes: usize,
    outlined_var_bytes: usize,
}
```

`RowSize` is a temporary measurement returned by capacity validation; keep it
with the shared validator. `RowSizeLayout` is derived runtime metadata, rebuilt
when the column layout is constructed or deserialized. Do not serialize these
fields or add per-row persisted accounting. The size layout owns the nullable
count and derives column count and total inline width from its named counters.
The column layout delegates its nullable-count accessor to it; variable-column
ordinals stay on `TableColumnLayout`. The former column-layout inline-length
accessor is removed because page planning now consumes the size layout directly.
VarByte descriptors contribute to `b8_count`.

Inherent methods calculate exact fixed-page and singleton sizes, validate page
layouts, and bound new-page capacity. Format-owned helpers in `row/mod.rs` and
`lwc/mod.rs` supply RowPage metadata overhead and singleton LWC base costs.
Use actual layout/serialization constants and checked arithmetic. Do not
construct an LWC builder, encode values, or allocate column buffers to validate
a foreground row.

Let `C` be the column count, `Q` the nullable count, `V` the VarByte count, and
`w` a fixed-width value's width. Including headers and checksums, the bases are:

```text
RowPage base = 32 header + 16 footer + 8 deletion bitmap
             + 8*Q + align8(2*C) + sum(align8(inline_width))

LWC base = 32 integrity envelope + 24 block header
         + 2*C + 3*Q + sum_fixed(9 + w) + 17*V
```

Every fixed-width slot contributes its width even when logically null. Every
nullable column contributes its metadata even when the value is non-null.
For VarByte, null contributes zero value bytes; a non-null value contributes
its complete length to `total_var_bytes`, and contributes to
`outlined_var_bytes` only when its length exceeds `PAGE_VAR_LEN_INLINE` (6).
Check null state before accessing an existing page's variable field so stale
bytes beneath a null bit cannot affect admission.

The final capacities are:

```text
row_page_bytes = row_page_base_bytes + outlined_var_bytes
lwc_bytes      = lwc_base_bytes      + total_var_bytes
```

Require both complete images to be at most their physical page size, currently
65,536 bytes. A schema whose base cost alone exceeds capacity rejects writes
before allocation; this task does not add a separate DDL policy for empty
tables. Checked arithmetic must fail closed before narrowing offsets/counts.

### 2. Shared mandatory checks and typed failures

Extend the shared validator with full-row capacity measurement and complete
post-update capacity measurement. Add the fieldless private classification
`DmlValidationError::RowCapacity`. Attach measured RowPage/LWC sizes and
capacities, plus the failing bound or arithmetic condition; caller boundaries
add table/operation context. Do not attach value contents.

Capacity methods are separate from optional shape/type checks and their call
sites must be unconditional. Foreground user boundaries map failure to
`OperationError::InvalidDmlInput`; physical replay and catalog reconstruction
map it to `DataIntegrityError::InvalidPayload` beneath their existing consuming
runtime context. Preserve existing catalog invariant ownership and typed
report frames. Do not classify caller oversize as engine poison or transient
resource exhaustion.

Measure owned and borrowed full-row inputs through the existing `RowValues`
contract. For updates, merge sorted `UpdateValues` with the schema's variable
column ordinals. Updated columns use the new value's nullness/length;
unchanged columns use the existing row. Hot sources can read null bits and
borrow `RowRead::var`; cold sources already have decoded values. Use one shared
length accumulator and no copied patched-row image solely for validation.
Fixed-only schemas require only the cached base checks.

### 3. Foreground insert and update boundaries

- In `MutationExecutor::insert_row`, enforce capacity before entering the
  allocation/retry loop. Reuse the returned outlined-byte total for row-count
  planning instead of rescanning through `table::row_len`. Broaden this helper
  and affected move-update result carriers to the existing carrier that includes
  Operation, Runtime, and Fatal where required; keep public disclosure at the
  current public boundary.
- Preserve complete batch preflight in `trx/stmt.rs`: check every candidate's
  capacity before the first physical insert, even when shape validation is
  disabled. Shared insertion admission remains the backstop for callback and
  internal paths. Full replacement helpers must check candidates before deleting
  their old rows.
- Change `HotRowMutator::finish_update_owned` to return
  `OperationResult<UpdateRowInplace>`. Validate the complete resulting image
  after establishing live-row ownership and before `RowWriteAccess::update_row`,
  variable-space reservation, in-place writes, or conversion to Delete undo.
  Propagate this report through `update_known_row`, `unique_mutate.rs`,
  `access.rs`, and index/full-table mutation callers. Preserve callback error
  identity and existing empty-update/Skip behavior.
- In both owned and known cold-update paths in `access.rs`, validate the merged
  old-row/update image before finalizing the cold delete, masking old index
  entries, or inserting the replacement. A provisional marker or hot Lock undo
  may already exist to provide write ownership; normal statement settlement
  must release it on rejection.

Admission concerns a fresh page. A valid row may still need existing move
update or insertion retry because its current page is full, fragmented, frozen,
or otherwise unavailable. Keep placement decisions separate from permanent
capacity failure.

### 4. Exact bounds for newly allocated RowPages

Keep the existing estimated count as an occupancy hint, bounded to the positive
u16 domain. Before allocating a new page, validate that hint against the actual
layout and the candidate row's outlined bytes. For a proposed capacity `n`:

```text
fixed_page_bytes(n) = 32 + 16
                    + bitmap_len(n) * (1 + nullable_column_count)
                    + align8(2 * column_count)
                    + sum_width(count[width] * align8(n * width))

required_bytes(n) = fixed_page_bytes(n) + n * outlined_var_bytes
```

The width histogram makes this constant work per candidate count. If the hint
does not fit, use a bounded binary search for the largest fitting count no
greater than that hint. Use checked arithmetic throughout. Singleton admission
establishes that one fits, so this path must return a positive count rather
than create an empty page or retry an impossible row indefinitely. It does not
increase the heuristic's count to optimize density.

Use safe selection in both transactional insertion and
`MemTable::insert_no_trx_location`. Retain an initialization defense that checks
the fixed layout before `RowPage::init` writes or narrows header/column offsets.
Share exact fixed-layout calculation with capacity planning; tests compare it
against actual initialized header offsets.

### 5. Recovery and catalog reconstruction

Apply mandatory full-row and merged-update checks to nontransactional catalog
insert/upsert/update/replacement helpers in `mem_table.rs`, before their first
mutation. These paths have no local undo; invalid input must not delete an old
row before capacity is known.

For user-page replay, perform capacity checks in the insert/update application
helpers before reserving variable bytes or modifying the row image. This must
also hold with `recovery_disable_dml_validation`. Keep existing row membership,
slot-history, and partial-replay dirty-state rules.

In `replay_create_row_page_ddl`, extend the existing positive-u16 range check
with exact fixed-layout validation before allocation/index publication. Replay
must either accept the recorded capacity unchanged or reject it as invalid
payload; clamping would alter the logged RowID reservation.

Existing data is not scanned or rewritten by this task. An oversized image
encountered during replay fails startup through a typed integrity report.
Reading already persisted cold blocks continues through their existing format
validation. No migration or checkpoint preflight sweep is added.

### 6. Documentation and completion boundaries

Update `docs/public-api.md` and relevant validation doc comments to describe
aggregate physical row limits, final-image update checks, and mandatory
capacity under validation opt-out. Update recovery documentation for replay
checks and data-checkpoint documentation to distinguish singleton
representability from the still-unresolved size of a multirow source selection.

Keep backlog 000007 open and record this task as the single-row admission portion
when resolving the task. Do not claim every source row page becomes
checkpointable: byte-aware splitting and unsupported RowID coverage remain
separate work.

## Implementation Notes

Implemented the planned storage invariant without changing persisted formats
or public API signatures:

- `TableColumnLayout` rebuilds `RowSizeLayout` with cached RowPage/LWC base costs
  and named inline-width counts during normal construction and deserialization.
  Inherent methods own size arithmetic and page-capacity planning. Admission scans
  only variable lengths, uses checked arithmetic, and borrows unchanged hot
  values only after checking null bits.
- Foreground insertion, complete batch preflight, hot/frozen/cold updates, and
  catalog replacement preflight reject capacity failures before logical row
  changes. The shared hot-update owner now returns an Operation report; the
  insertion and move-update carriers retain Operation, Runtime, and Fatal.
- Nontransactional catalog reconstruction and physical replay enforce the same
  checks under validation opt-out. Replay preserves prior successful mutations
  and dirty state, and rejects overflowing recorded page layouts without
  changing their RowID reservations.
- New-page planning bounds the existing occupancy hint by exact aligned fixed
  storage plus outlined payloads. The 4,096-U8 fixture corrects the 12-slot hint
  to eight safe slots. Initialization defends this layout before offset writes.
  Capacity planning runs only after session-cache and free-list misses. Physical
  insertion reuses admission's outlined-byte total across page retries.
- Public API, recovery, checkpoint, and validation-option documentation describe
  mandatory capacity and the distinction from available space on an existing
  page. Backlog 000007 remains open: this implements only its single-row
  admission portion, not byte-aware splitting of multirow source selections or
  unsupported RowID coverage.

Ten regression tests cover independent initialized-page/serialized-block size
oracles, both exact physical bounds, aggregate variable lengths, inline/null
transitions, stale null descriptors, wide schemas, statement rollback and
ownership release, catalog reconstruction, replay, and checkpoint/reopen. Update
fixtures reject an image exactly one byte beyond the singleton LWC capacity
while it still fits a fresh RowPage. A second transaction probes ownership
release before the rejected writer finishes. Hot, frozen, cold, index-driven,
and full-table paths retain separate lifecycle coverage. Shared fixture and
assertion helpers keep the related cases together; the format oracles,
transactional rollback, and no-undo replay tests intentionally remain separate.

The existing sparse replay capacity fixture now uses a singleton-admissible row
that cannot fit its 70-slot page, preserving its placement-failure contract.
The recorded-range regression also covers a positive-u16 count whose fixed
layout overflows.

Coverage exposed an existing synchronization race in
`test_checkpoint_reachability_reclaims_dropped_secondary_disk_tree_root`: a
purge cycle can finish while another cleanup still owns the retired runtime.
The test now waits on actual runtime removal and the existing purge events,
using the production retry request as nearby lifecycle tests do. Production
cleanup is unchanged. The repaired test passed 100 stress iterations, and the
subsequent instrumented workspace run passed.

Validation on 2026-10-27:

- `rtk cargo nextest run --workspace`: 2,484 passed, three skipped. The final
  default-feature coverage run also passed all 2,484 tests.
- `rtk cargo nextest run -p doradb-storage --no-default-features`: 2,212 passed,
  two skipped on the final source snapshot.
- `tools/style_audit.rs`: passed for 16 branch-diff Rust files and 519 selected
  test contracts, including formatting and strict workspace Clippy. Strict
  profiling-disabled storage Clippy also passed.
- Focused production coverage: 91.16% across the 16 changed Rust files;
  263/265 added executable production lines covered (99.25%). The two uncovered
  lines are the page-planning contract's panic diagnostic and the existing
  deleted-row return adapted to the new result type. Report:
  `target/coverage/task-000338.md`.
- `mem_table.rs` has 79.19% whole-file coverage, below the default 80% review bar.
  All 23 added executable lines in that file are covered. Remaining gaps are
  pre-existing generic lookup/projection adapters and catalog page/key invariant
  failure paths; broader adapter and corruption-path coverage remains separate
  follow-up work. The shared validator is 92.59% covered, RowPage is 97.32%, and
  the replay coordinator is 100%.
- Assertion/overlap review checked the new cases against physical output,
  original row/index identity, statement prefixes, null state, and dirty-page
  evidence. Deterministic freeze/checkpoint helpers and ownership probes supply
  the lifecycle predicates; no elapsed-time synchronization was added.

After the named-count and inherent-method refactor on 2026-10-28, the workspace
suite again passed all 2,484 tests with three skipped, and the style audit passed
for 16 Rust files and 519 selected test contracts. Existing tests now cover a
mixed-width page across alignment/bitmap boundaries, all width groups through
metadata serialization, and overflow through the sizing methods. Assertion
review retained the independent page-offset and serialized-LWC oracles; the
earlier coverage figures describe the pre-refactor snapshot.

After the insertion optimization, all 2,484 workspace tests passed with three
skipped; the style audit passed for 21 Rust files and 603 selected test contracts.
Cache-reuse tests assert no capacity planning on shared, exclusive, redo, or
session-cache hits, and one calculation for fresh allocation. Error tests retain
their I/O-cause assertions while also checking that failed cache reloads do not
plan a new page. These cases retain separate cache-owner and redo coverage.

The 1M-row benchmark used six balanced measurements per build and sync mode.
Compared with the previous validation build, optimized median time changed from
15.012s to 10.335s with fsync and from 1.713s to 1.673s without sync. Fsync timing
varied too broadly to isolate the code speedup; the no-sync median improved 2.4%
with overlapping ranges. Page allocations and redo volume were identical.
Full measurements: `target/bench-000338/optimization/report.md`.

## Impacts

- `catalog/table.rs`: derived schema size plan, named width counts, exact size
  methods, and bounded new-page capacity.
- `row/mod.rs`: format-owned metadata costs and initialization defense.
  Existing `RowValues`, `UpdateValues`, and `RowRead`
  remain the borrowed-input and row-access foundations.
- `lwc/mod.rs`: format-owned singleton base-cost helper using the current
  encoder contract; existing final assembly checks remain authoritative.
- `table/dml_validator.rs`: capacity measurement, update overlay, and private
  capacity-error classification.
- `table/mutate.rs`, `hot.rs`, `access.rs`, `unique_mutate.rs`, and affected
  index-mutation callers: mandatory placement of checks and typed propagation.
- `trx/stmt.rs`: complete batch and catalog replacement preflight.
- `table/mem_table.rs`, `table/mod.rs`, `table/recover.rs`, and `recovery/mod.rs`:
  reconstruction/replay checks and recorded page-layout validation.
- Public/storage documentation: enforced limits and validation-opt-out contracts.

Public API signatures and persisted formats need no migration. Some rows
previously accepted by hot insertion will now receive an input error. The
guarantee is independent of compression across other rows. Schema-only costs
are cached; admission inspects variable lengths rather than payload bytes.
Update validation extends work under row ownership, so it must avoid payload
copies, encoding, awaits, and page allocation.

## Test Cases

1. **Independent size oracles.** Compare measured singleton LWC size with the
   real builder's final encoded offset/header, and RowPage size with actual
   initialized offsets and successful inserts. Cover every value kind, null
   and non-null nullable columns, empty bytes, and mixed schemas. Do not make
   tests merely repeat the implementation's arithmetic.
2. **Both physical boundaries.** Accept exact fits and reject one byte over
   independently for RowPage and LWC. Include the nonnullable single-VarByte
   65,461/65,462 boundary, multiple VarByte values whose aggregate exceeds the
   limit, and a nullable/fixed-column schema where RowPage is the tighter bound.
   Include schema-only overhead that makes any row impossible.
3. **Inline versus outlined storage.** Cover lengths 0, 6, and 7 and updates
   across that boundary, including null-to-value and value-to-null. Stale bytes
   beneath a null bit must not affect measurement.
4. **Safe page allocation.** Exercise narrow and wide schemas, nullable bitmap
   boundaries, exact alignment steps, hint correction, and a one-row capacity.
   The 4,096-U8 fixture must allocate a valid page despite the old 12-slot hint;
   impossible rows must fail without repeated allocation or initialization
   panic. Verify initialized layout and completed insertion, not only a count.
5. **Foreground rejection and rollback.** Cover single/batch inserts, unique
   callbacks/upserts, index-driven and full-table updates, mutable hot rows,
   frozen move updates, and cold replacements. Oversize must preserve old values
   and index identities, release provisional ownership, roll back earlier rows
   in the failed statement, preserve earlier successful statements, and allow a
   subsequent valid write with a healthy engine. Run representative cases with
   optional DML validation both enabled and disabled.
6. **Complete update images.** Reject an update whose changed values fit
   individually but whose final row exceeds a bound because of unchanged
   columns. Accept shrinking updates and legal null transitions. Preserve Skip
   and empty-update behavior.
7. **Replay and catalog paths.** Valid user/catalog rows replay correctly;
   oversized insert/update images fail with `InvalidPayload` before mutation,
   including validation-disabled reconstruction. Preserve successful earlier
   replay mutations' dirty state on a later failure. Reject a logged page count
   with an overflowing fixed layout before initialization; never change a valid
   logged RowID range.
8. **Persistence.** Admitted boundary rows encode, checkpoint, reopen, and read
   back correctly through the production API. Arrange fixtures so each selected
   source range fits LWC; this task must not implicitly depend on unimplemented
   byte splitting. Existing low-level oversize-builder tests remain useful for
   callers that deliberately bypass admission.

Follow `docs/process/unit-test.md` and `.config/nextest.toml`; use the existing
io_uring backend and production synchronization helpers. Complete focused tests,
`rtk cargo nextest run --workspace`, formatting, and strict workspace Clippy.
Run the profiling-disabled storage pass if implementation changes any
feature-sensitive path. Each new or changed test needs the required Purpose
and Expected contract; task resolution includes normal style and assertion
quality gates.

## Open Questions

None within the approved scope.

Byte-aware splitting remains in backlog 000007, which must not be closed by
this task. RowID coverage admission remains in backlog 000208. A future
requirement to preserve oversized historical replay images or support larger
logical rows requires separate design; this task defines typed rejection and
does not provide migration or overflow storage.
