---
id: 000337
title: Make LWC integer encoding and block sizing consistent
status: implemented
created: 2026-10-25
github_issue: 1162
---

# Task: Make LWC integer encoding and block sizing consistent

## Summary

LWC integer admission and serialization now share an encoding plan with exact
serialized costs. Plans use non-null extrema and the complete physical row
count. Final serialization replaces integer null slots with the final minimum,
including when flat encoding wins. Final block assembly verifies column lengths
and actual body capacity before copying payloads.

Accepted, unchanged builder states fit the existing block format. Nullable large
integers no longer overflow because of physical null placeholders, and all-null
integer columns can reach the existing row cap when other columns permit it.

## Context

Issue Labels:

- type:bug
- priority:high
- codex

This standalone task followed the LWC sizing investigation. It has no parent
RFC or source backlog to resolve.

Previously, admission used non-null statistics while serialization chose its
encoding again from physical values. Direct integer nulls contained zero, and
page views could retain arbitrary stale values beneath null bits. Those values
could widen the serialized range without widening the estimate. Separately,
FOR selection ignored metadata overhead and could exceed flat encoding for
small inputs. Uninitialized statistics also caused all-null columns to be
estimated as flat.

The 64-KiB block has a 65,504-byte LWC payload after its shared integrity
envelope, and a 65,480-byte body after its 24-byte LWC header. Checking only
`u16::MAX` allowed out-of-bounds body copies. A checkpoint encoding panic could
poison the engine through the thread pool's failure policy.

The investigation's reproductions are now durable regression fixtures:

| Input | Previous behavior | Implemented behavior |
| --- | --- | --- |
| 8,060 nullable U64 rows alternating `2^40` and null | Accepted, then build panicked | Builds a 2,062-byte used payload |
| 9,000 rows of the same input | Accepted, then column-offset contract error | Builds a 2,296-byte used payload |
| One U64 and a 65,442-byte VarByte value | Accepted, then build panicked | Flat integer encoding fills the 65,504-byte payload exactly |
| All-null U64 direct rows | Rejected after 8,057 rows | All 15,360 permitted rows use 3,886 bytes |

Used sizes include the LWC header and exclude the integrity envelope and unused
padding. Both direct and page-view ingestion exercise the nullable U64 cases.

Related work remains open: [backlog 000006](../backlogs/000006-row-to-lwc-streaming-serialization-memory-optimization.md)
owns streaming memory optimization, and [backlog 000007](../backlogs/000007-lwc-oversize-rowpage-handling.md)
owns source selections whose actual values cannot fit a block. Neither backlog
is closed by this task.

## Goals

- Keep admission and final integer encoding consistent, including compression
  metadata, rounded packed bytes, null bitmap prefixes, offsets, and headers.
- Preserve logical values, nulls, row order, identities, and block bindings while
  making integer bytes independent of physical null-slot history.
- Admit all-null columns by their selected encoding, subject to the existing
  row cap and the sizes of other columns.
- Return `InternalError::LwcBlockEncodingContract` for final length or capacity
  disagreement before a fixed-block copy.
- Preserve exact rollback of rejected direct and page-view appends.

## Non-Goals

No new compression codes, zero-bit encoding, compact non-null-only storage,
format versions, or decoder migrations were introduced. Float and VarByte
encoding policies and general `ScanBuffer` semantics are unchanged.

The task does not change row caps, identity planning, checkpoint ownership,
scheduling, publication, or poison policy. It does not split or spill genuinely
oversized value selections, introduce streaming serialization, change public
APIs, or add configuration or benchmark infrastructure.

## Rejected Alternatives

1. Planning from physical null-slot values would reconcile sizes but make
   compression depend on arbitrary source history. Non-null statistics plus
   canonical placeholders establish a stable logical contract.
2. A general prepare/size/write codec framework would couple this correctness
   fix to broader serializer and allocation restructuring. The shared plan is
   limited to integer encoding; streaming remains separate work.

## Plan

The implemented design has three boundaries:

1. `IntegerEncodingPlan<T>` in `serde.rs` owns row count, minimum, encoding
   selection, the authoritative encoded length, and integer writing through
   its inherent `ser` method, excluding the outer LWC tag. Construction uses
   typed extrema. Ranges use widened `i128` arithmetic, including full signed
   and unsigned extrema.
   The bitpacking module supplies the supported narrower width from
   `{1, 2, 4, 8, 16, 32}`. FOR wins only when its complete size is strictly
   smaller; empty inputs and ties use flat. Constants retain one-bit encoding.
2. `LwcBuilder` derives plans from retained non-null statistics without scanning
   buffered values on each append or adding cached rollback state. Zero
   uninitialized extrema represent all-null columns. At final encoding, the
   plan's inherent `serialize_lwc` method borrows null-free integer slices,
   copies null-bearing columns only as needed to replace logical null positions
   with the final minimum, and adds the compression tag. The plan writes its
   selected flat or FOR encoding without callbacks or rescanning. Both paths
   validate the physical slice length before writing the integer payload.
3. Block assembly checks the offset table, checked column-end accumulation,
   complete serialized column lengths, and final payload length against the
   actual body capacity and persisted field limits. Contract reports attach
   the relevant field, column, sizes, and capacity. Only validated payloads are
   copied into the zeroed block and checksummed.

The complete integer payload costs remain:

```text
flat = 1 + 8 + N * S
FOR  = 1 + 1 + 8 + S + ceil(N * B / 8)
```

Nullable columns additionally store a two-byte bitmap-length prefix and
`ceil(N / 8)` bitmap bytes, even when no bit is set. The null bitmap remains
authoritative; canonical integer placeholders have no logical meaning.

## Implementation Notes

Implemented exact shared integer planning, final-minimum null normalization,
and body-capacity defenses without changing the persisted format or consumer
interfaces. Follow-up review approved replacing the originally retained
crate-private integer constructors with inherent methods on the plan.

The unused integer `new_*` constructors, slice-based planning entry point,
`ForBitpackingSer` wrapper, integer `LwcPrimitiveSer` variants and flat adapters,
and two-callback serialization helper were removed. The remaining primitive
serializer handles floats and VarByte. Tests construct plans from extrema
through a local helper and exercise the production writer directly.

Admission remains constant work per integer column after statistics updates.
Normalization adds one final linear pass for nullable integer columns and at
most one column-sized temporary allocation at a time: 120 KiB for eight-byte
integers at the current row cap. Flat integers retain a bulk copy on
little-endian hosts and use scalar little-endian serialization on other hosts.
Packing routines, checksums, and deterministic padding are retained.

Validation completed in the task worktree:

- Strict workspace Clippy passed; the style audit also passed formatting,
  Clippy, structural checks, and contracts for five changed Rust files and
  166 source-visible tests, with zero violations.
- Focused LWC, integer-plan writer, and new checkpoint tests passed during
  development.
- `rtk cargo nextest run --workspace`: 2,474 passed, 3 skipped across four binaries.
- Fresh default-feature workspace production coverage passed using io_uring:
  `serde.rs` 81.83%, `compression/bitpacking.rs` 87.16%, `lwc/mod.rs` 88.26%,
  `lwc/block.rs` 85.36%, and `table/persistence.rs` 90.07% (87.44% combined).
- No feature-sensitive production path changed, so separate profiling-disabled
  validation was not required.

Semantic review connected changed assertions to independent wire tags, widths,
lengths, parsed final offsets, logical input rows, and typed error contexts.
The seeded mixed cases use `StdRng` seed 337 with bounded row counts, integer
ranges, null decisions, and variable-byte lengths. The checkpoint regression
uses the existing retrying publication helper and verifies cold placement and
values for every row; no elapsed-time readiness assumptions were added.

Shared helpers consolidate persisted-row decoding, final-offset size checks,
stale-slot page construction, and test-only extrema collection. Constructor-only
serde tests were replaced by independent wire fixtures with prefix/suffix guards
and short/long slice contract checks. Preselected flat encoding and FOR bases
are tested with values whose rescanned statistics would differ. Duplicate empty
and singleton round trips were consolidated into the all-type selection test.
The nullable block-reader test uses an explicit noncanonical wire fixture so
legacy hidden values remain covered independently of the canonical writer.

Intentional overlap remains between raw packing tests, ordinary/prepared codec readers, builder admission and rollback,
and checkpoint lifecycle tests because they protect different boundaries.
There were no exact duplicate contracts in the selected files. The semantic
review covered the changed tests and related LWC, serializer, and checkpoint
cases; the mechanical contract gate covered all 166 selected tests.

No new unsafe code or deferred implementation work was introduced. The related
streaming and genuinely oversized-source backlogs remain unchanged.

## Impacts

Integer planning and serializer selection changed in `serde.rs` and
`compression/bitpacking.rs`. Admission, canonicalization, size checks, and most
regressions live in `lwc/mod.rs`; `lwc/block.rs` preserves independent legacy
nullable-reader coverage. `table/persistence.rs` adds a user-table checkpoint
regression; production checkpoint consumers retain their interfaces.

[Data checkpoint documentation](../data-checkpoint.md#lwc-and-secondary-index-construction)
now records exact admission sizing and integer null canonicalization. Catalog
checkpointing continues to use direct-row construction, covered by builder
regressions and the existing workspace catalog tests.

Existing flat and FOR blocks remain readable. Newly written blocks can select
a different encoding and contain different unobservable values beneath integer
null bits. Unused crate-private integer constructors were removed; production
integer callers serialize through their chosen plans.

## Test Cases

- All eight integer types: empty and singleton flat encoding, uneconomical
  narrower ranges, exact ties, compressible constants, full extrema, and round
  trips through ordinary and prepared readers.
- Supported width boundaries, rounded packed lengths, negative-only and
  zero-crossing values, signed minima, and large unsigned bases.
- Plan-owned writes preserve independent wire fixtures and surrounding bytes,
  honor preselected encodings and bases, and reject mismatched slice lengths
  before changing the destination in either encoding.
- Canonical null slots for flat and FOR columns, all-null stale slots, changing
  final minima, nullable columns without nulls, deleted source rows, and partial
  bitmap bytes. Equivalent direct and page inputs produce identical blocks.
- Both overflow fixtures, the exact-fit mixed singleton block, rejection of one
  extra byte, and all-null admission through `MAX_LWC_ROWS`.
- Direct and page-view capacity rollback across bitmap growth and changed
  extrema, retaining row identities, seeds, statistics, and subsequent valid
  mixed integer/float/VarByte appends. Existing row-cap rollback coverage remains.
- Bypassed admission with body overflow below `u16::MAX` and larger offset
  overflow returns typed contract errors with field, column, and capacity
  context instead of panicking.
- Reproducible mixed-value cases compare estimated size with the persisted final
  column offset plus the 24-byte header, excluding padding, and decode against
  original logical rows.
- A 9,000-row user-table checkpoint spans multiple source pages, publishes cold
  rows with alternating large U64 values and nulls, verifies every cold read,
  and commits a subsequent write with a healthy engine.

## Open Questions

None within the implemented scope. Streaming/chunked serialization remains
[backlog 000006](../backlogs/000006-row-to-lwc-streaming-serialization-memory-optimization.md);
source selections whose actual values cannot fit an empty block remain
[backlog 000007](../backlogs/000007-lwc-oversize-rowpage-handling.md).
