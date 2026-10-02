---
id: 000323
title: Inline adaptive deletion encoding and deletion-blob retirement
status: implemented
created: 2026-10-01
github_issue: 1131
---

# Task: Inline adaptive deletion encoding and deletion-blob retirement

## Summary

Durable cold-row deletions now live inline in `ColumnBlockIndex` as adaptive
physical-ordinal sets. A common 15,360-row LWC cap guarantees that identity and
any future deletion subset fit together in one standalone leaf. User pages
split through bounded prepared views; catalog records use the same cap.
Deletion blobs and their readers, writers, references, and traversal are retired.

Column-index envelopes are version 4 and require fresh storage. LWC values
retain their version-2 format, physical identity, and binding contract.

## Context

Source Backlogs:

- docs/backlogs/closed/000206-inline-adaptive-deletion-encoding-and-deletion-blob-retirement.md
- docs/backlogs/closed/000036-deletion-blob-roaring-encoding-upgrade-and-compatibility.md
- docs/backlogs/closed/000037-roaring-deletion-bitmap-rowid-to-offset-mapping-in-checkpoint.md
- docs/backlogs/closed/000075-refine-column-block-index-inline-delete-field-and-delete-surface-cleanup.md

Issue Labels:

- type:task
- priority:medium
- codex

This standalone task has no parent RFC. Task 000322 supplies the adaptive
identity codec and compact lookup; RFCs 0011 and 0012 establish index and
values-only LWC ownership. The preserved baseline was
`640fe099b459f3d6606929150a724c3a02fec187` on branch `inline-deletion`.

Independently bounded identity and deletion bodies previously could require
66,250 bytes together, exceeding a leaf. Point reads expanded u32 deletion
lists, and larger sets needed external pages. The selected fixed cap replaces
that capacity risk without metadata-dependent block admission or another tree.

## Goals

- Bound every supported entry and later deletion replacement independently of compression.
- Keep physical identity/count, values, and durable ordinal membership distinct.
- Preserve prepared visibility, exact secondary sidecars, MVCC precedence, and atomic roots.
- Reuse ordinary CoW repacking, prefix selection, ancestor splits, and retained-root reclamation.
- Remove deletion-blob storage through one validated fresh-storage cutover.

## Non-Goals

- Legacy migration, mixed formats, Roaring, external identity, or a separate deletion tree.
- Changes to live deletion-buffer ownership, replay cutoffs, or post-transition fatal policy.
- General oversized-value handling or repartitioning unsupported u32 coverage spans.
- Physical deletion compaction, removal of all-deleted blocks, or cross-leaf merging.
- LWC value-format or checksum changes.

## Plan

The final architecture uses the following contracts:

1. `MAX_LWC_ROWS = 15_360` is unconditional. Identity bodies are at most `4*n`.
   Deletion bodies are at most `4 + 8*ceil(n/64) + 2*ceil(ceil(n/64)/4)`.
   Shared format constants give 120 fixed bytes and a compile-time standalone
   bound of `120 + 61,440 + 2,044 = 63,604`. There is no reserve padding.
   Cache admission rejects excessive counts and inefficient oversized bodies.
2. `OrdinalDeletionSet` owns a physical count and optional shared encoded bytes.
   Empty sets omit the section; dense all-deleted sets have no body. Borrowed
   views validate once, then support direct membership and ordered ordinals.
   The eight-byte header contains codec, version 2, zero flags, exact u16
   cardinality, and zero reserved bytes. The former identity domain byte is zero.
3. Builders reject count overflow before mutation and retain final serializer
   checks. Bounded views preserve source coordinates for values, nulls, visible
   ranges, statistics, and RowIDs. Page cursors advance only after acceptance;
   value-capacity retries reuse the same prepared bitmap and selection.
   Sidecar collection follows successful append. Full final builders remain
   pending until subsequent visibility or batch end fixes their coverage/binding.
4. `load_entry_identity_and_deletions` returns compact state from one leaf.
   Checkpoint translates markers through identity, rejects absent physical rows,
   reconstructs secondary keys by ordinal, and merges bounded sorted u16 sets.
   `ColumnDeletionPatch` replaces the complete set with matching physical count.
5. Point resolution tests borrowed deletion bytes directly. Scans compile the
   ordinal iterator into a ready `ColdDeleteMask`, then apply captured MVCC
   overrides. General table/catalog allocation and root reclamation remain intact.
6. Existing leaf/branch/root rewrite helpers repack actual complete lengths and
   propagate splits. Deletion-only rewrites preserve identity bytes, ordinals,
   coverage, value references, and bindings. Shrinkage uses the same path.

The storage ownership and publication contracts are documented in
[Table File](../table-file.md), [Block Index](../block-index.md),
[Data Checkpoint](../data-checkpoint.md),
[Deletion Checkpoint](../deletion-checkpoint.md), and [Recovery](../recovery.md).

## Implementation Notes

Inline adaptive deletions and both capped construction paths are implemented;
all production deletion-blob and persisted-domain surfaces have been removed.
No additional dependency or configuration was introduced.

The shared codec now lives in `identity_set.rs` as `EncodedIdentitySet` and
`IdentitySetRef`; `EncodedRowSet` and `RowSetRef` remain row-domain aliases.
Planning, iteration, statistics, and seed types use `IdentitySet*` names.
Four targeted `#[inline]` hints remove nested deletion-adapter calls in release
code, but paired measurements show no reliable additional end-to-end speedup.

Rust 1.99 CI compatibility required atomic API, bit-width, and empty-collection
assertion updates. These preserve atomic ordering and existing test conditions.

Review caught a bounded-view edge: the bitmap iterator requires its backing
word slice to be clipped as well as its bit length. Exact/partial word tests
and large prepared-page tests now protect that boundary. Append rollback
covers values, nullable state, RowIDs, seeds, and extrema. CoW tests cover
middle-entry movement, prefix-width changes, a full-branch split/root growth,
old-root visibility, untouched child references, and all-deleted shrinkage.

Assertion review retained distinct byte-grammar, cache admission, checkpoint
sidecar, recovery, MVCC, and root-lifecycle tests. Generated ordinal subsets use
fixed xorshift state and independent sorted-list membership oracles. Obsolete
blob tests became inline corruption/reachability tests. Count-limit cases share
one test, and complete replacement explicitly removes previous membership.
No new production unsafe block was introduced; refreshed unsafe inventory was
unchanged. Borrowed page views end before pipeline waits.

Validation on Rust 1.99: 2,211 workspace tests and 2,032 storage tests without
default features passed. Strict Clippy passed in both configurations. The final
style gate passed 51 branch-diff Rust files and 1,005 test contracts.
Previously measured focused coverage is 88.61% across seven files: ordinal deletions
99.01%, column index 86.94%, LWC 88.88%, bounded views 93.75%, persistence 87.66%,
table access 91.36%, and catalog storage 84.47%; every target exceeds 80%.

### Performance evidence

Five controlled baseline/candidate pairs ran on aarch64, Rust 1.97.1 release,
default profiling, CPU 2 affinity, performance governor, and io_uring. Pair order
alternated. Fixtures used 60,000 constant-u8 dense rows; 15,000 constant-u8 rows
with RowID stride 100,003; 20,000 fixed rows with sequential and wide u64 values;
and 20,000 rows with a sequential u64 and nullable 24-96-byte payload.
Deletes grow according to `(physical_global_index * 37) % 100 < fraction`.

This storage integration fixture times actual LWC building/writes, identity
planning, CoW deletion replacement/publication, compact point resolution plus
value decoding, scan setup/value decoding, and reopen into a fresh readonly
cache. Every pair checks point values and deletion state, every scan checks all
visible values and masks, and reopen checks masks, physical counts, and bindings.
Each stage measures 2,000 warm points and three complete scans. These are storage
component measurements, excluding foreground transaction scheduling and MVCC
waiting; reopen measures checkpointed state rather than a nonempty redo tail.

B/C below means baseline/candidate medians. Every fixture/fraction meets the
10% point/scan and 20% checkpoint elapsed review limits. The largest scan
regression is 1.68%; all point medians improve by at least 13.0%. Dense data
checkpoint rises 12.14%, while the largest deletion-checkpoint increase is
3.74% in the zero-deletion metadata control. Nonempty deletion checkpoints improve.

The dense cap increases LWC pages from one to four and zero-delete reachable
pages from two to five. Its zero-delete reopen rises from 420 to 578 us (37.6%);
1% and 10% reopen also increase. These page/reopen costs are separate from the
latency gates. Sparse, fixed, and variable fixtures keep their original block
counts. Every fixture remains in one index leaf; fanout B/C is dense 1/4,
sparse 1/1, fixed 4/4, and variable 18/18.

| Fixture | Deletes % | Point ns B/C | Scan us B/C | Setup us B/C | Delete checkpoint us B/C | Delete CPU us B/C | Reopen us B/C |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| dense | 0 | 344.00/292.00 | 1464.39/1397.35 | 1.92/3.38 | 92.54/94.12 | 92.25/93.88 | 420.42/578.29 |
| dense | 1 | 2820.00/329.00 | 1455.54/1370.83 | 3.75/4.83 | 440.08/219.00 | 437.50/215.25 | 429.58/536.96 |
| dense | 10 | 22644.00/308.00 | 1353.36/1316.08 | 10.29/14.17 | 405.33/210.62 | 404.83/210.12 | 445.88/528.88 |
| dense | 50 | 116685.00/314.00 | 809.67/796.17 | 52.12/63.71 | 544.96/289.58 | 544.42/288.12 | 687.42/553.92 |
| dense | 90 | 208642.00/312.00 | 187.24/188.19 | 85.12/93.58 | 906.75/381.00 | 904.13/379.58 | 817.75/602.71 |
| dense | 100 | 231400.00/307.00 | 41.14/39.79 | 93.17/94.21 | 945.21/281.96 | 942.12/280.46 | 866.42/600.46 |
| sparse | 0 | 394.00/342.00 | 369.96/358.04 | 1.21/1.08 | 97.29/89.33 | 97.00/89.08 | 383.75/356.21 |
| sparse | 1 | 6549.00/374.00 | 361.40/352.89 | 9.62/1.88 | 284.33/166.83 | 283.04/166.38 | 442.96/348.58 |
| sparse | 10 | 60274.00/354.00 | 343.61/339.31 | 62.83/4.08 | 343.38/160.00 | 341.71/159.58 | 565.54/357.21 |
| sparse | 50 | 298165.00/372.00 | 200.07/198.75 | 302.62/13.46 | 682.58/183.46 | 680.13/183.04 | 933.88/366.83 |
| sparse | 90 | 538641.00/356.00 | 43.83/44.57 | 561.67/23.71 | 1201.67/213.04 | 1199.08/212.67 | 1164.29/360.83 |
| sparse | 100 | 600946.00/349.00 | 9.44/9.54 | 607.21/24.46 | 1435.54/178.79 | 1432.75/177.42 | 1228.75/369.04 |
| fixed | 0 | 362.00/313.00 | 761.35/764.26 | 1.00/0.79 | 95.67/87.88 | 95.38/87.67 | 529.21/516.04 |
| fixed | 1 | 806.00/341.00 | 769.67/760.97 | 3.04/1.33 | 287.08/165.42 | 286.62/165.00 | 564.42/500.42 |
| fixed | 10 | 3040.00/324.00 | 697.79/695.68 | 5.04/5.00 | 290.04/172.08 | 288.50/170.54 | 565.00/498.75 |
| fixed | 50 | 12563.00/344.00 | 402.03/397.83 | 16.21/17.17 | 313.08/192.21 | 311.71/191.71 | 574.08/522.75 |
| fixed | 90 | 22103.00/342.00 | 93.19/91.03 | 31.33/28.67 | 403.42/216.96 | 401.54/216.62 | 686.46/533.00 |
| fixed | 100 | 24618.00/324.00 | 14.29/13.01 | 33.29/34.79 | 455.25/191.08 | 454.54/189.75 | 694.88/522.58 |
| variable | 0 | 407.00/354.00 | 1193.39/1192.64 | 2.38/1.79 | 89.21/92.54 | 88.96/92.33 | 1196.75/1191.59 |
| variable | 1 | 473.00/391.00 | 1184.14/1196.21 | 4.79/2.92 | 249.42/180.79 | 247.62/180.38 | 1219.79/1196.00 |
| variable | 10 | 1029.00/377.00 | 1107.78/1100.22 | 10.17/6.42 | 305.38/189.38 | 304.96/188.96 | 1241.59/1193.75 |
| variable | 50 | 2734.00/378.00 | 650.32/651.53 | 24.21/18.50 | 333.33/211.79 | 332.96/211.38 | 1297.42/1213.00 |
| variable | 90 | 4441.00/377.00 | 147.17/147.26 | 36.71/30.42 | 436.71/245.92 | 436.29/245.38 | 1372.50/1230.00 |
| variable | 100 | 4831.00/376.00 | 14.71/14.90 | 37.54/33.75 | 459.17/209.04 | 457.46/208.62 | 1355.79/1222.17 |
| Fixture | Data checkpoint ms B/C | Process CPU ms B/C | Rows/block B/C |
| --- | ---: | ---: | --- |
| dense | 1.906/2.137 | 1.905/2.137 | [60000]/[15360, 15360, 15360, 13920] |
| sparse | 0.985/0.908 | 0.984/0.907 | [15000]/[15000] |
| fixed | 1.208/1.136 | 1.207/1.136 | [6543, 6543, 6543, 371]/[6543, 6543, 6543, 371] |
| variable | 2.720/2.648 | 2.719/2.647 | 18 blocks each; 675-1,145 rows, identical boundaries |

Framed bytes include all LWC/index/deletion serialized bodies and their page
framing, excluding deterministic padding and file-root anchors. Multiply any
page count by 65,536 for physical bytes. Allocated counts include metadata and
unreclaimed prior roots in this controlled run; reachable counts trace the current
index, values, and (baseline only) blobs. CoW pages are index/deletion writes;
each publication additionally writes one metadata page and one 32-KiB super slot.
Cache pages count mapped readonly frames after warm reads and scans.

Scratch bytes account for per-entry load/encode row/deletion vectors and compact
bodies: baseline `8*n + 8*d`, candidate `identity_body + 10*d + deletion_body`.
They describe these payloads even for empty sets; they are not measured allocator
peaks and exclude codec planning metadata and the fixture's own oracle buffers.
Builder RowID storage separately falls from 480,000 to at most 122,880 bytes for
the dense fixture. The same values remain physically stored after full deletion.

| Fixture | Delete % | Framed bytes B/C | Reachable pages B/C | Allocated pages B/C | CoW pages B/C | Cache pages B/C | Scratch bytes B/C |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| dense | 0 | 7697/8048 | 2/5 | 6/9 | 0/0 | 2/5 | 480000/0 |
| dense | 1 | 10185/9280 | 3/5 | 9/11 | 2/1 | 4/6 | 484800/1848 |
| dense | 10 | 31785/16070 | 3/5 | 12/13 | 2/1 | 6/7 | 528000/17414 |
| dense | 50 | 127843/16070 | 4/5 | 16/15 | 3/1 | 9/8 | 720000/78854 |
| dense | 90 | 223959/16070 | 6/5 | 22/17 | 5/1 | 14/9 | 912000/140284 |
| dense | 100 | 247959/8080 | 6/5 | 28/19 | 5/1 | 19/10 | 960000/153600 |
| sparse | 0 | 62072/62072 | 2/2 | 6/6 | 0/0 | 2/2 | 120000/60000 |
| sparse | 1 | 62760/62380 | 3/2 | 9/8 | 2/1 | 4/3 | 121200/61800 |
| sparse | 10 | 68160/64082 | 3/2 | 12/10 | 2/1 | 6/4 | 132000/77002 |
| sparse | 50 | 92160/64082 | 3/2 | 15/12 | 2/1 | 8/5 | 180000/137002 |
| sparse | 90 | 116160/64082 | 3/2 | 18/14 | 2/1 | 10/6 | 228000/197002 |
| sparse | 100 | 122160/62080 | 3/2 | 21/16 | 2/1 | 12/7 | 240000/210000 |
| fixed | 0 | 200620/200620 | 5/5 | 9/9 | 0/0 | 5/5 | 52344/0 |
| fixed | 1 | 201576/201052 | 6/5 | 12/11 | 2/1 | 7/6 | 52864/792 |
| fixed | 10 | 208776/203348 | 6/5 | 15/13 | 2/1 | 9/7 | 57576/7430 |
| fixed | 50 | 240798/203348 | 6/5 | 18/15 | 2/1 | 11/8 | 78512/33600 |
| fixed | 90 | 272856/203348 | 7/5 | 22/17 | 3/1 | 14/9 | 99448/59770 |
| fixed | 100 | 280856/200652 | 7/5 | 26/19 | 3/1 | 17/10 | 104688/65430 |
| variable | 0 | 1153585/1153585 | 19/19 | 23/23 | 0/0 | 19/19 | 9160/0 |
| variable | 1 | 1154529/1154129 | 19/19 | 25/25 | 1/1 | 20/20 | 9248/144 |
| variable | 10 | 1162183/1156513 | 20/19 | 28/27 | 2/1 | 22/21 | 10072/1308 |
| variable | 50 | 1194183/1156513 | 20/19 | 31/29 | 2/1 | 24/22 | 13736/5888 |
| variable | 90 | 1226241/1156513 | 21/19 | 35/31 | 3/1 | 27/23 | 17400/10458 |
| variable | 100 | 1234241/1153729 | 21/19 | 39/33 | 3/1 | 30/24 | 18320/11450 |

Follow-up point-select measurements used seven paired warm-cache runs with 1%
persisted deletions against the same pre-task baseline. Public unique lookup
improved 7.35% (1.318 to 1.221 us), and live-key-only lookup improved 8.40%.
Deleted-key misses regressed 3.85%; they bypass column resolution, and the cause
was not established. Component resolution plus value decoding improved
1.27x-17.88x. These aarch64 release-test results exclude cache-miss I/O and do
not establish multithreaded scaling.

## Impacts

- Column-index version 4 rejects older envelopes with typed integrity errors;
  there is no legacy reader, migration, or mixed-format path.
- All durable deletion state is inline and ordinal-based. LWC values, live
  deletion ownership, replay cutoffs, secondary-root publication, and retained
  root reclamation keep their existing contracts.
- Highly compressible values can occupy more LWC pages under the fixed cap.
  Measurements above record that tradeoff independently of lookup gains.
- Source backlogs 000206 and 000075 are implemented. Backlogs 000036 and 000037
  are replaced by adaptive inline ordinal storage and compact identity mapping;
  neither Roaring nor legacy compatibility was delivered.

## Test Cases

- Independent complete-page accounting at the cap; admission rejects count and
  body-bound violations, including structurally valid inefficient encodings.
- Empty, all, singleton, runs, scattered, alternating, nearly full, and generated
  deletion sets; exact borrowed/owned membership and shared clone bytes.
- Corrupt envelope/header versions, codec tags, flags, counts, reserved bytes,
  lengths, ordinals, bitmap padding/ranks, and malformed directories fail safely.
- Large prepared pages split at the cap with nullable values, sparse holes,
  changed live deletes, invisible tails, exact sidecar ordering, and final bindings.
  Bounded scans also cover variable-width values and bitmap-word endpoints.
- Catalog sparse identities split successfully; row/value byte-capacity rejection
  and builder rollback preserve the original typed failure and cleanup paths.
- Middle-entry movement, prefix-width changes, leaf/branch/root growth, untouched
  child references, old roots, and later all-deleted shrinkage preserve identity.
- Existing checkpoint, secondary-index, recovery, reclamation, transition/fatal,
  early-stop, and MVCC tests remain green; inline corruption replaces blob tests.
- Five verified performance pairs per fixture cover all six deletion fractions.

## Open Questions

No implementation blocker remains. General oversized value selection remains
tracked by [backlog 000007](../backlogs/000007-lwc-oversize-rowpage-handling.md).
Unsupported u32 coverage spans and pre-transition admission are tracked by
[backlog 000208](../backlogs/000208-bound-checkpoint-rowid-coverage-spans.md).
The fixed cap does not solve either scope or change the fatal transition policy.
