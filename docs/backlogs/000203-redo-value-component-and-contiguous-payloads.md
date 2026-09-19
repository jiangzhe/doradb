# Backlog: Separate redo value descriptors and variable payloads

## Summary

Redesign the redo wire format so a value collection is a serialization/deserialization component with separate value descriptors and a contiguous variable-length payload block. Encoding and decoding a single value depend on that component's context. This should let recovery borrow payload ranges during decoding and copy an admitted row's useful payload into a page batch as one contiguous slice. Bump the redo format version and reject older versions; backward-compatible decoding and migration are not required.

## Reference

- [Task 000311](../tasks/000311-recovery-owned-page-batches-and-recycling.md): packed group decoding, independent page batches, and measured admission costs.
- [Backlog 000202](closed/000202-recovery-payload-allocation-bottleneck-bulk-recycling.md): completed recovery ownership work; this follow-up addresses the wire format and value codec.
- [Transaction/group format contract](../../doradb-storage/src/log/block_group.rs), [row redo codec](../../doradb-storage/src/log/redo.rs), [value codec](../../doradb-storage/src/value.rs), and [format version](../../doradb-storage/src/log/format.rs).
- [Packed decoder](../../doradb-storage/src/recovery/decode.rs) and [page-batch append](../../doradb-storage/src/recovery/packed.rs).
- User direction on 2026-09-19: treat values as a serialization/deserialization component, make individual value encoding contextual, and reject old redo versions instead of maintaining compatibility.

## Deferred From (Optional)

docs/tasks/000311-recovery-owned-page-batches-and-recycling.md

## Deferral Context (Optional)

- Defer Reason: Task 000311 deliberately retains the existing persistent format. Changing writer serialization, both decoding paths, and the versioned format contract requires a separate design and performance evaluation; it should not expand the completed recovery ownership change.
- Findings: DecodedGroup retains the original group buffer. Variable payloads for one row are separated by value tags, length prefixes, scalars, and update ordinals, so PackedPageBatch::append() copies and rebases each variable value separately. Copying the enclosing wire span would also copy unrelated metadata/scalars; compacting payloads into a new decode buffer first would add another payload copy. Independent page batches remain useful because they release source groups without waiting for unrelated pages.
- Direction Hint: Make the value collection, rather than a standalone value, the wire-codec boundary. Define descriptors/scalars and payload references relative to a component or row range, with contiguous useful payload for each admission unit. Choose component granularity and offset representation during design. Prefer borrowing from the assembled group and copying directly into independent batches without intermediate payload compaction. Share parsing and validation between owned and packed adapters. Reject unsupported old versions explicitly.

## Scope Hint

- Define a component codec for insert values, sparse update values and ordinals, and keyed/catalog value collections. Individual value encoding/decoding must use the enclosing component's payload context. Specify descriptor representation, lengths, offsets, byte order, and empty values explicitly; do not serialize Rust enum memory layouts.
- Separate value descriptors/scalars from variable payloads on the wire. Establish a contiguous payload range per row/admission unit so admission can bulk-copy useful payload and descriptors without per-value payload copying or offset rebasing. Keep ownership independent across page batches and source groups.
- Update redo writers, size accounting, the owned Ser/Deser path, and DecodedGroup decoding together. Audit shared Val, Vec<Val>, UpdateCol, and CatalogSelectKey serialization consumers so unrelated persistent formats are not changed accidentally. Catalog DDL with accompanying DML and standalone catalog DML must retain owned replay behavior.
- Keep one documented format and acceptance contract referenced by both deserialization paths. Share readers/validation where practical and use differential tests for behavior that remains separate. Validate external counts, offsets, lengths, tags, and component/frame boundaries before indexing or publishing any group.
- Bump the redo file format version (currently 6). Reject older/unsupported versions at the existing version-validation boundary; no legacy decoder or migration is required. Preserve ordering, duplicate-map replacement semantics, validation of overwritten/filtered entries, exact scalar bits, and accepted-job cleanup.

## Acceptance Hint

- Produce an implementation-ready task or RFC defining the component API, wire layout, payload/descriptor boundaries, row-relative or component-relative addressing, and writer/reader changes. Explain the copy and allocation budget and any unavoidable descriptor translation.
- Both production decoding paths agree on decoded semantics and rejection/error kinds. Cover every value and redo variant, empty payloads, multiple variable values interleaved with scalars, sparse/duplicate update ordinals, catalog DDL plus DML, keyed values, map replacement, and filtered entries. Include independent wire fixtures, invalid tags/references/counts, truncated components/frames, and explicit old-version rejection.
- Demonstrate that eligible rows copy only their contiguous useful payload into independent page batches, without an intermediate decode payload copy or copying skipped rows. Verify whole-group validation before publication, source-group release before replay, batch reuse, and error/cancellation cleanup.
- Measure writer/serialization throughput, redo size, decode and admission costs, full recovery throughput, allocations, and memory against the current format under matched conditions. Retain the task-000311 single-variable-column baseline and add multi-variable-column inserts and sparse updates, empty/short/large values, and checkpoint-filtered replay. Report tradeoffs; do not infer an end-to-end improvement solely from fewer copy calls.

## Notes (Optional)

Illustrative layout; exact framing and widths remain design decisions:

```text
[row metadata][component descriptors/scalars/payload references][contiguous variable payload]
```

The task-000311 primary benchmark has one U64 and one VarByte per row, so it already performs one variable-payload copy per row. Multiple variable values are necessary to evaluate the proposed bulk-copy benefit. The new format still requires value interpretation and the group-to-batch and batch-to-page payload copies; it does not imply zero-copy replay.

Task 000311's refined jemalloc profiles attributed about 0.213 coordinator CPU seconds to batch append, including only 0.011 seconds in payload memcpy. Descriptor iteration, writes, rebasing, and bookkeeping therefore belong in the format evaluation as well as contiguous payload copying.

[Backlog 000130](000130-large-redo-transaction-streaming-replay.md) tracks large-transaction streaming separately. This format follow-up does not require shared group ownership or worker-side decoding.

