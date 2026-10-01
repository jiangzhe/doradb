# Backlog: Inline Adaptive Deletion Encoding and Deletion-Blob Retirement

## Summary

Store both row identity and durable deletion state inline in each
`ColumnBlockIndex` leaf entry. Reuse the adaptive integer-set codecs introduced
by task 000322 to encode deleted LWC ordinals, and retire offloaded deletion
blobs once admission guarantees that identity plus any future deletion set fits
one index page.

This replaces backlog 000031's original direction of optionally compressing
external blobs. The design must combine bounded deletion encoding with joint
entry sizing and LWC splitting before publication; compression alone cannot
guarantee capacity.

## Reference

- Replaces [backlog 000031](closed/000031-column-deletion-blob-compression-policy-evaluation.md),
  closed at the user's request in favor of this separate design.
- Original source: [task 000038](../tasks/000038-column-block-index-offloaded-deletion-bitmap.md).
- [Task 000322](../tasks/000322-adaptive-cold-row-id-encoding-with-compact-lookup.md):
  adaptive row identity, compact membership, and current inline capacity contract.
- User review on 2026-09-30: apply similar encoding to durable deletions and
  prefer keeping both sets inline so deletion blobs can be retired.
- `doradb-storage/src/index/column_row_set.rs`: codec planning, validation,
  compact lookup, and iteration.
- `doradb-storage/src/index/column_block_index.rs`: deletion domains, inline
  sizing, point lookup, delete rewrites, and leaf packing.
- `doradb-storage/src/index/column_deletion_blob.rs`: existing blob format,
  reading, writing, and page traversal to retire.
- [Deletion checkpoint](../deletion-checkpoint.md),
  [block index](../block-index.md), and [table file](../table-file.md).
- Related backlogs: [000036](000036-deletion-blob-roaring-encoding-upgrade-and-compatibility.md),
  [000037](000037-roaring-deletion-bitmap-rowid-to-offset-mapping-in-checkpoint.md),
  [000075](000075-refine-column-block-index-inline-delete-field-and-delete-surface-cleanup.md),
  and [000201](closed/000201-adaptive-cold-row-identity-encoding.md).

## Deferred From (Optional)

Review of docs/tasks/000322-adaptive-cold-row-id-encoding-with-compact-lookup.md;
replaces the follow-up originally tracked by backlog 000031 from task 000038.
Task resolution also carries forward the remaining capacity and splitting
scope of closed backlog 000201 here.

## Deferral Context (Optional)

- Defer Reason:
  Task 000322 explicitly excludes changing deletion encodings and preserves
  existing deletion domains and publication behavior. Inline-only deletion
  storage needs a separate format, admission, splitting, and compatibility design.
  The completed task rejects oversized identities; it does not guarantee that
  arbitrary sparse inputs can checkpoint successfully. Changing the existing
  fatal policy after checkpoint TRANSITION also needs a separate decision.
- Findings:
  - Row identity is already inline-only. Its body limit is 65,184 bytes, with
    240 bytes reserved for current deletion metadata. Oversized identity returns
    a typed capacity error; there is no identity offload path.
  - Task 000322 replaced canonical per-row fingerprints with a u64
    `block_binding_value` derived from the logical table, final RowID bounds,
    and row count. Split blocks need bindings for their own summaries; delete-only
    rewrites preserve existing bindings and physical membership.
  - Persisted deletes currently use plain `u32` lists, inline or external. Point
    lookup expands the list before testing membership. Inline thresholds retain
    legacy value-count assumptions rather than using the encoded byte size.
  - An LWC block contains at most 65,535 rows. Encoding deleted ordinals bounds
    the universe by row count even when the corresponding RowIDs have wide gaps.
    The existing whole bitmap codec provides an upper bound on deletion body
    bytes for every nonempty subset:

    ```text
    W = ceil(row_count / 64)
    deletion_body_bound = 4 + 8 * W + 2 * ceil(W / 4)
    ```

    This is at most 8,708 bytes, excluding the deletion section header. With the
    current eight-byte header, the maximum reserve would be 8,716 bytes. Derive
    the final bound from the chosen serialized layout and each block's row count.
  - Independent fits do not imply a combined fit. A probe using the current
    encoder produced a 64,000-byte identity for 16,000 RowIDs spaced 100,000 apart
    in a coverage span of 1,600,000,000. Deleting every other ordinal takes a
    2,130-byte bitmap body. Including 120 bytes of current page, entry, section,
    and standalone-prefix overhead gives 66,250 bytes, exceeding a 65,536-byte
    page. The identity alone passes current admission.
  - Leaf splitting operates between entries and cannot fix an oversized single
    entry. A block admitted with few or no deletes must still fit after its
    deletion pattern grows into a less compressible set.
  - The live `ColumnDeletionBuffer` carries transaction ownership and timestamps;
    it cannot be replaced by a durable set codec. Scan execution already uses an
    ordinal bitmap with MVCC overrides and need not change representation.
- Direction Hint: Prefer inline adaptive deletion sets over external blob
  compression. Guarantee capacity for identity plus any future deletion subset
  at LWC admission, with bounded splitting before submission and an explicit
  legacy-data transition.
  Absorb the remaining identity-capacity scope from backlog 000201 into this
  joint admission design. Evaluate recoverable rejection before irreversible
  checkpoint work; revisit identity offloading only if bounded inline splitting
  cannot meet the requirements.

## Scope Hint

- Reuse the adaptive codec core for deleted ordinals, keeping row presence and
  deletion state semantically separate. Define explicit empty and all-deleted
  cases; the existing `EncodedRowSet` rejects empty input, while dense encoding
  has no body and still requires identifying metadata. Do not require a new
  Roaring dependency merely because earlier deletion backlogs proposed it.
- Specify the inline deletion section, codec/version tags, cardinality, and
  validation rules. Replace legacy count-based inline thresholds with exact
  serialized sizing. Validate deletion ordinals against authoritative row count.
- Admit each new LWC block only when identity bytes, a worst-case deletion
  reserve for its row count, and all page/entry metadata fit a standalone leaf
  entry. Reserve future capacity even when the initial deletion set is empty.
  The reserve constrains admission; it is not serialized padding. Recompute
  actual prefix widths and complete sizes when packing multiple entries.
- Add bounded LWC splitting before block submission when the joint budget is
  exceeded, covering both user-table and catalog construction. Preserve ordered
  rows, coverage, and matching block binding values in the resulting blocks. Do not
  solve later deletion growth by renumbering ordinals or changing row identity
  during a delete-only rewrite. Backlog 000201 records the completed identity
  encoding work; this item owns its remaining capacity and splitting follow-up.
- Read durable deletion membership directly from validated compact bytes.
  Resolve RowID to LWC ordinal through the existing compact identity; avoid
  expanding the entire delete set for point lookup. Preserve scan visibility
  masks, transaction-marker precedence, and checkpoint/recovery ordering.
- Make new-format deletion writes inline-only and retire blob references,
  writing, deferred reads, reachability traversal, and reclamation machinery
  when no supported format needs them. Define a version and migration/rebuild
  policy for existing inline lists, both deletion domains, and external blobs.
  Keep any legacy reader only for the explicitly supported transition; existing
  entries without the new reserve must be migrated before relying on the new
  capacity guarantee. Retire only deletion-blob-specific paths; general
  table-file root-reachability reclamation remains in place.
- Measure the tradeoff between fewer blob reads and simpler lifecycle handling
  versus lower leaf density, cache occupancy, leaf splits, and CoW write volume.
  Compare checkpoint cost, point lookup, scan setup/throughput, and recovery
  against the current inline-list/external-blob implementation.

## Acceptance Hint

- The chosen format and sizing proof guarantee that every newly admitted block
  can represent every later deletion subset inline, including worst-case
  patterns, without an external payload or deletion-growth capacity failure.
- Tests cover empty, all-deleted, contiguous, scattered, and nearly full sets;
  maximum row count; sparse identities near the combined page limit; and
  repeated deletion checkpoints that change the selected codec. Estimation and
  serialization agree, including headers and directories.
- Joint-budget overflow triggers bounded splitting before block submission.
  Split/reopen tests prove no row loss or duplication and correct ordinal and
  block binding values. Delete-only rewrites preserve identity and ordinals.
- Malformed codec metadata, lengths, counts, and out-of-range ordinals fail
  through typed integrity boundaries. MVCC, atomic publication, recovery, and
  the selected legacy-data transition remain correct.
- Repeated measurements report complete persisted bytes and allocated pages,
  leaf fanout, cache and temporary-memory costs, and read/checkpoint performance.
  Define and justify regression budgets before selecting the final policy.
- Document and complete deletion-blob retirement according to the compatibility
  policy. Reconcile backlogs 000036/000037's Roaring-specific direction and
  000075's inline-layout cleanup with this design rather than implementing
  competing deletion formats. Creating this backlog does not close those items.

## Notes (Optional)
