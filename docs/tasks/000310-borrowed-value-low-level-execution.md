---
id: 000310
title: Borrowed Value Views for Low-Level Row Execution
status: implemented
created: 2026-09-18
github_issue: 1078
---

# Task: Borrowed Value Views for Low-Level Row Execution

## Summary

Added internal borrowed value views and repeatable row/update accessors. Owned
foreground and recovery payloads now share validation, space calculation, and
row-page writers with independently buffered borrowed inputs. Public ownership
contracts, persistent formats, and recovery scheduling remain unchanged.

This delivers the consumption interface needed by later allocation-light
recovery payloads. It does not remove current decoder allocations or change
batch reclamation.

## Context

Source Backlogs:

- docs/backlogs/000202-recovery-payload-allocation-bottleneck-bulk-recycling.md

Issue Labels:

- type:task
- priority:medium
- codex

Related task:

- [Task 000309: Pipelined Recovery with Parallel Page Replay](000309-pipelined-recovery-with-parallel-page-replay.md)

There is no parent RFC.

Task 000309 established allocator sensitivity in parallel recovery. Its
coordinator-disposal diagnostic reduced median replay from 4.174 s to 2.884 s,
without isolating the allocator mechanisms or proving a packed representation.
The decoder still creates owned `Vec<Val>` and `Vec<UpdateCol>` payloads, including
individual outlined `MemVar` allocations.

Recovery consumes those values while holding the destination page, copying
bytes without retaining the inputs or rebuilding undo history. Foreground,
catalog, and rollback writers have similar read-only input consumption while
retaining separate ownership obligations. This task generalizes that consumption
boundary and deliberately leaves backlog 000202 open.

## Goals

- Preserve every value kind, scalar bits, null semantics, and diagnostic spelling
  in an allocation-free borrowed view.
- Support repeatable full-row and sparse-update access independently of payload
  ownership, including adapters for existing owned slices.
- Share validation, sizing, and atomic/exclusive page writes across both inputs.
- Preserve row contents, variable-space accounting, mutation publication, and
  typed failure behavior.

## Non-Goals

- Allocation-light decoding, production packed batches, recycling, byte admission
  budgets, coordinator disposal changes, or allocator replacement.
- Changes to `ReplayOp`, `BatchOutput`, `RowRedoKind`, serialization, dispatcher
  ordering, page admission, DDL barriers, or completion ownership.
- Public borrowed APIs or ownership changes to undo, transaction effects, index
  keys, and read results.
- Generalization of index-key, primary-key, projection, memcmp, or LWC interfaces.
- Page-backed borrowed reads or a recovery throughput improvement target.

## Rejected Alternatives

- **Per-value polymorphism alone:** offset descriptors need their containing
  buffer to resolve bytes. Row/update accessors carry that context once and
  construct views on demand, without a temporary vector of views.
- **An engine-wide generic value model:** migrating reads, serialization, indexes,
  and undo would couple this prerequisite to independent lifetime and ownership
  contracts. The internal consumption boundary is sufficient for later batches.

## Plan

`ValRef` copies scalars and borrows byte slices; `Val::view` borrows both inline
and outlined `MemVar` storage. Owned kind/null inspection and debug formatting
forward to the view implementation. Floating-point payloads retain `OrderedFloat`
and their original bits. The physical-store `Value` trait remains separate.

`RowValues::value(index)` returns a value view by physical column ordinal.
`UpdateValues::value(index)` returns `(column_ordinal, value_view)` for an update
entry in input order. Both traits also expose `len()` and require
stable lengths and contents across repeated validation, sizing, and mutation
passes. Consumers access only positions below the reported length and retain no
views. Owned slices implement these traits through `Val::view`; generic consumers
use static dispatch with `?Sized` support.

Column type checks and full-row/sparse-update validators share borrowed cores.
`validate_full_row` and `validate_sparse_update` each expose one generic entry
point over `RowValues` and `UpdateValues`, respectively. Owned vector callers
pass slices through the existing adapters; borrowed providers pass directly.
Full-row count checks precede value access. Sparse checks preserve bounds, strict
ordering, and type-check precedence, including empty-update acceptance. Null kind
matching remains distinct from column nullability. Existing classifications,
diagnostic facts, and caller-owned recovery context remain intact.

Sizing retains the six-byte page inline threshold independently of `MemVar`'s
fourteen-byte threshold. Inserts count only outlined variable columns. Updates
reuse fitting outlined regions and reserve the complete replacement length on
growth. Space reservation precedes writes and row publication.
Insert sizing uses one generic `var_len_for_insert` over `RowValues`. Update
sizing uses one generic `RowPage::var_len_for_update` over `UpdateValues`; the
`RowRead` convenience method supplies the row index. Both support owned slices
and independent borrowed providers without forwarding wrappers.

`NewRow`, `RowMut`, `RowMutExclusive`, and the page column writers consume
short-lived views through the existing fixed-width and byte-copy primitives.
`RowPage`, `RowMut`, and `RowMutExclusive` each expose one `update_col` accepting
`ValRef` directly. Owned foreground, catalog, and rollback callers pass `.view()`;
recovery accessors already supply views. The page's `update_col_exclusive` remains
a separate exclusive writer. `NewRow::add_col` also accepts `ValRef` directly,
with owned insertion callers passing `.view()`. Shared atomic and exclusive stores
remain separate. Input storage must be separate from the mutable destination
page; successful writes copy bytes into page-owned storage under the existing
latch, reservation, and non-overlap invariants.

Recovery insert/update helpers consume the accessor traits synchronously.
Production operation and batch entry points retain owned redo payloads and pass
slices. Validation still follows `disable_dml_validation`; range, insertion-history,
and deleted/live checks precede reservation and mutation. Finish operations,
counts, insertion bits, and dirty marking after partial batch success remain
unchanged. No view enters retained replay state or crosses a job handoff.

## Implementation Notes

Implemented the shared consumption boundary and verified owned/borrowed behavior
through independent descriptor-and-byte test inputs. No production payload
materialization, temporary view vector, dynamic dispatch, or new unsafe operation
was introduced. Consolidated the column-insert and column-update entry points
around `ValRef`, removing the owned-value wrappers and `_ref` suffixes. Owned
callers borrow views at the write call and retain their values for subsequent
undo/redo bookkeeping.
Full-row validation, sparse-update validation, and insert sizing also use single
generic entry points, replacing the owned-slice wrappers and `_from` variants.

The test provider exists only behind `cfg(test)` with crate-local re-exports
through `row::tests`. Its calls use the trait name to distinguish the two
`value()` methods when both accessor traits are implemented.
It resolves scalar and byte descriptors directly from one owned buffer without
constructing `Val` or `MemVar`. Shared helpers exercise all four combinations of
owned/borrowed input and shared/exclusive writes. The existing recovery slot-history
scenario also runs against both providers.

Verification on 2026-09-18:

- `rtk cargo fmt --check`: passed; the final style gate also checks formatting.
- `rtk cargo clippy --workspace --all-targets -- -D warnings`: passed, including
  the final style gate after consolidating validation, sizing, and column writes.
- `rtk cargo nextest run --workspace`: all 2,097 tests passed across four binaries,
  including the final run with the consolidated interfaces.
- `tools/style_audit.rs --diff-base origin/main`: passed for all eleven tracked Rust
  files in the branch diff. The new untracked accessor module was also reviewed
  for item order, documentation, visibility, and safety; workspace checks include it.
- Initial focused coverage: 95.36% across seven core Rust files, measured before
  subsequent interface cleanup. Every measured file exceeds the 80% review bar.
  The additional foreground, catalog, and rollback call-site adaptations are
  covered by the workspace regression suite.
- Unsafe inventory refreshed: the row-module file count increased by one; unsafe
  operations and safety-comment counts did not change.

| Coverage target | Line coverage |
| --- | ---: |
| `value.rs` | 90.76% |
| `row/mod.rs` | 97.40% |
| `row/values.rs` | 98.86% |
| `catalog/table.rs` | 94.71% |
| `table/dml_validator.rs` | 93.17% |
| `table/mod.rs` | 96.56% |
| `table/recover.rs` | 98.22% |

No material plan deviation was required. I/O and backend paths were unchanged,
so the alternate libaio pass was not required. No throughput claim was measured.
Backlog 000202 remains open because only its consumption prerequisite is complete.

## Impacts

The internal value, row, catalog type-check, DML validation, and recovery mutation
layers now accept shared borrowed consumers. Foreground, catalog, and rollback
operations retain their owned values and borrow views at column-write calls.
Public APIs, serialization, page layouts, batch ownership, and scheduling are
unchanged.

## Test Cases

- Every value variant and integer boundary; float normal values, infinities, NaN
  payloads, and signed zero; inline/outlined byte pointer identity; null versus
  empty bytes; UTF-8 and binary debug formatting.
- Repeatable descriptor access and ordinal preservation. Full-row count, type,
  and nullability errors; empty, ordered, duplicate, descending, and out-of-range
  sparse updates; matching error classifications and diagnostic attachments.
- Shared and exclusive insert/update writers with all scalar kinds, null
  transitions, and byte lengths 0, 6, 7, 14, 15, and 128. Inline/outline transitions,
  shrinking and reuse, and full-length growth reservations match owned inputs.
- Input-buffer overwrite after insertion and update leaves page values intact;
  row counts, null/deletion state, floating-point bits, and variable offsets agree.
- Owned and borrowed recovery inputs preserve duplicate-insert, deleted-row,
  missing-history, row-range, and page-space failures without publishing failed
  mutations. Malformed payloads retain typed recovery context and leave rows
  untouched. Valid batches work with validation enabled and disabled.
- Existing foreground CRUD, rollback, catalog, recovery ordering, DDL, cancellation,
  completion-credit, and insertion-history regressions pass, including
  `test_batch_failure_marks_prior_mutations_dirty` and
  `test_recover_row_dml_validation_rejects_malformed_payloads`.

## Open Questions

[Backlog 000202](../backlogs/000202-recovery-payload-allocation-bottleneck-bulk-recycling.md)
retains allocation-light decoding, production packed batches, bounded retained
capacity, bulk recycling, and measured performance work. A later packed decoder
must avoid first materializing ordinary owned hot-row values. These accessors
support that follow-up without selecting its wire reader or reclamation design.
