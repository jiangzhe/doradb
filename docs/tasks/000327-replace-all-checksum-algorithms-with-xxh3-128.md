---
id: 000327
title: Replace all checksum algorithms with XXH3-128
status: implemented
created: 2026-10-03
github_issue: 1139
---

# Task: Replace all checksum algorithms with XXH3-128

## Summary

Replaced repository-owned BLAKE3 and CRC32 integrity algorithms with default,
unseeded XXH3 from `xxhash-rust` 0.8.19. Physical checksums, schema fingerprints,
benchmark verification, and coverage artifacts retain all 128 bits. Block
bindings use native XXH3-64 and remain `u64`. Both widths use explicit
little-endian serialization.

This is a coordinated breaking format cutover requiring fresh storage. On the
measured aarch64 host, five paired checkpoint runs reduced the median sample
from 76.632 ms to 54.991 ms. Profiles reduced checksum share from 59.40% to
8.78% of checkpoint/LWC samples. I/O variability remains material;
these measurements do not establish a universal speedup.

## Context

Source Backlogs:

- docs/backlogs/closed/000207-evaluate-checksum-algorithms-across-all-use-cases.md

Issue Labels:

- type:perf
- priority:medium
- codex

Checkpoint profiles motivating backlog 000207 attributed about 50–57% of
sampled user CPU to full-block BLAKE3 checksums. Task 000322 had already replaced
per-RowID fingerprinting with a fixed 36-byte table/bounds/count binding. This
change preserves that binding's semantics and does not restore the old stream.

The approved scope replaced legacy algorithms across all integrity uses and
explicitly accepted fresh storage. The user subsequently chose native XXH3-64
for block bindings to retain their compact representation; all other digests
remain XXH3-128. There is no parent RFC. Algorithm-selection microbenchmarks
and a broader architecture/workload campaign were not prerequisites.

The implementation and performance baseline is
`5493f486c6f9247317dfe02174ce4bc0a51c3b4d`. The starting worktree had no lockfile;
normal resolution selected BLAKE3 1.8.7 and crc32fast 1.5.2, differing from the
proposal's earlier research resolution. Both actual lockfiles are retained with
the binaries. No unrelated retained dependency version changed.

## Goals

- Use full-width XXH3-128 with equivalent one-shot and streaming results, and
  native XXH3-64 for compact block bindings.
- Keep producers, readers, physical widths, capacities, and version gates aligned.
- Preserve typed corruption, block binding, canonical schema, admission/spill,
  and sealed/unsealed recovery contracts.
- Verify the checkpoint effect with repeated runs and saved flamegraphs.

## Non-Goals

- Legacy storage readers, migration, mixed formats, or algorithm negotiation.
- Authentication, adversarial collision resistance, or ordinary hash-table routing.
- Checkpoint scheduling, durability policy, validation frequency, or spill ownership changes.
- New benchmark/profiler infrastructure or a checksum microbenchmark suite.
- Performance claims for other workloads or architectures; historical document rewrites.

## Rejected Alternatives

- Keeping CRC32 for redo or BLAKE3 for schema/tooling would retain exceptions to
  the approved repository-wide policy.
- Algorithm tags and compatibility readers would add migration machinery beyond
  the fresh-storage contract; existing version gates identify the new formats.
- Padding physical digests to their old widths would retain unnecessary space.
  For the binding-specific 64-bit contract, the user selected native XXH3-64
  instead of truncating an XXH3-128 result.

## Plan

### Primitive and physical integrity

The internal `checksum` module exposes `CHECKSUM_SIZE = 16`, `checksum128`, and
an incremental `ChecksumHasher` wrapping `Xxh3Default`, plus a native one-shot
`checksum64` used only for block bindings. All use the default seed/secret.
Persisted representations use `to_le_bytes`/`from_le_bytes`; 128-bit digest text
is lowercase hex of those bytes, exactly 32 characters. Benchmark and coverage
code use the same dependency and encoding without exporting a new storage API.

All physical block trailers are 16 bytes. A 64 KiB page places its checksum at
65,520; shared-envelope payload/padding occupies 65,504 bytes after the 16-byte
header. Checksums cover all preceding bytes, including deterministic padding.
Row pages, row-page-index nodes, BTree/DiskTree nodes, and column-index nodes
retain exact 64 KiB layouts and derived capacity assertions.

Table/catalog super-block slots remain 32 KiB. Their 24-byte footer holds a
16-byte checksum over the preceding 32,744 bytes plus the repeated 8-byte
checkpoint timestamp. Timestamp equality independently detects torn writes.
Readonly admission still validates once per residency generation; dirty spill
writeback stamps the checksum and reload validates before publication.

### Redo, logical bindings, and auxiliary digests

Redo data has an unpadded 23-byte common header: checksum at 0, flags at 16,
payload length at 17, and group block index at 19. The START extension remains
28 bytes, with group length/count/minimum CTS/maximum CTS at 23/31/35/43.
START and continuation payloads begin at 51 and 23, respectively. A 4 KiB block
holds 4,045 or 4,073 payload bytes. The checksum covers all bytes after its
16-byte field. Group publication stays atomic; required sealed corruption is
fatal and incomplete/corrupt unsealed tails retain their existing policy.

Block bindings are native XXH3-64 values represented as `u64` throughout
routing, scans, reads, checkpointing, mutation, GC, and catalog access. The
canonical input remains 36 bytes: `LWCBIND2`, table ID, inclusive start/exclusive
end RowIDs, and row count. Placement, interior membership, values, deletion
state, and codec choice remain outside the binding. Leaf-entry and LWC headers
both remain 24 bytes; LWC reserves ten bytes. Delete-only rewrites preserve
bindings. This intentionally accepts 64-bit collision protection for the
association check; separate 128-bit checksums protect physical block contents.

Schema fingerprints retain canonical active-field ordering and inclusion rules,
use canonical domain version 2, and store exactly 16 bytes. Old 32-byte values
are rejected. The opaque descriptor limit remains 64,000 bytes; its checkpoint
row estimate shrinks from 64,151 to 64,135 bytes.

Benchmark verification hashes `key:u64 LE || payload_length:u64 LE || payload`
and sums row digests modulo 2^128 with an independent checked row count. Count
overflow leaves state unchanged. CREATE validates 32 lowercase hex characters;
recovery retains colon-separated per-table fingerprints. Empty sums are zero.
Coverage source/build/raw/canonical digests share the encoding and manifest
schema 2 rejects schema-1 artifacts with regeneration guidance. The coverage
build-directory ownership marker remains independent of artifact schema.

### Version gates

| Contract | Previous | Current |
| --- | --- | --- |
| Table/catalog super-block | 1 | 2 |
| Table metadata | 8 | 9 |
| Catalog metadata | 6 | 7 |
| LWC envelope | 2 | 3 |
| Column-index envelope | 4 | 5 |
| Redo file | 6 | 7 |
| Block-binding domain | LWCBIND1 | LWCBIND2 |
| Canonical schema domain | 1 | 2 |
| Coverage artifact schema | 1 | 2 |

Unsupported slots cannot supply roots; a valid current alternate slot remains
eligible. Entirely legacy files are rejected. Headerless DiskTree blocks stay
behind table/catalog root gates; swap owners already recreate their files.
The binding refinement retains these pending version numbers because the
cutover is uncommitted. Earlier intermediate storage is disposable.

## Implementation Notes

Implemented the full cutover and verified successful checkpoint publication,
corruption handling, descriptor recovery, and auxiliary digest contracts. Direct
BLAKE3/crc32fast dependencies and calls are gone; ordinary routing hashes and
the test-only RowID sum remain unchanged.

Capacity review updated independent boundary fixtures instead of relaxing them:
open-root fanout increases to 4,090/2,727 for the tested 4/8-byte key shapes;
the worst supported standalone column leaf occupies 63,588 bytes with 104 fixed
bytes; the deletion-growth fixture retains 1,920 entries. The separate
prefix-width split fixture now forces a deletion bitmap across the capacity
boundary and asserts one original leaf followed by 1,924/1 output entries.
This prevents an already split setup from satisfying the test. Row-estimator
fixtures distinguish column-count boundaries exposed by the extra 16 bytes.
The shared LWC binding mutation changes the highest byte of the `u64` value.

### Correctness and review

- Formatting and strict workspace Clippy passed; profiling-disabled storage
  Clippy passed separately.
- `cargo nextest run --workspace`: 2,291 passed.
- `cargo nextest run -p doradb-storage --no-default-features`: 2,037 passed.
- Pinned-nightly coverage-script tests: 24 passed in the original validation;
  coverage tooling is unchanged by the binding refinement.
- Style gate: 28 changed tracked Rust files, 431 test contracts, no violations.
  Forced checksum/coverage style targets: three files and five tests, no violations.
- Independent literals came from upstream C libxxhash 0.8.2, including algorithm
  boundaries, block-sized inputs, schema and fixed-input binding digests. The
  binding vector is native XXH3-64, not the low half of XXH3-128.
- Semantic review covered changed assertions and related helpers, preserving
  distinct shared-envelope, recovery-adapter, admission, and spill lifecycles.
  Repeated corruption cases use offset tables; existing setup helpers remain
  shared. No synchronization or runtime ownership change was needed.

The artifact directory is
[`target/checksum-migration-u64-binding/`](../../target/checksum-migration-u64-binding/README.md).
It retains release binaries, lockfiles, build IDs, plans, environment details,
commands, canonical results, stdout/stderr, raw captures, SVGs, analysis scripts,
and verification logs. These are ignored local artifacts, not committed files.
The earlier `u128` binding candidate and its measurements remain separately in
[`target/checksum-migration/`](../../target/checksum-migration/README.md).

### Checkpoint measurements

Both revisions used Rust 1.99.0/LLVM 23.1.1, release debug information, default
features, the system allocator, and no CPU/allocator overrides on the same
14-CPU Apple aarch64 Linux VM. The existing checkpoint template was copied with
resolvable engine defaults. Only checkpoint diagnostics were enabled equally;
profile copies additionally paused the final benchmark phase.

Five independent baseline/candidate pairs ran interleaved, each against a fresh
root with zero warm-ups and one measured checkpoint. Every run froze 500,416
rows in 1,117 pages, published with one attempt, and recorded no retry waits.

| Metric | Baseline median [range] | Candidate median [range] |
| --- | --- | --- |
| Checkpoint sample, ms | 76.632 [59.706–93.505] | 54.991 [42.078–67.594] |
| Attempt elapsed, ms | 76.631 [59.705–93.504] | 54.991 [42.077–67.593] |
| Generic run elapsed, ms | 76.739 [59.840–93.648] | 55.100 [42.180–67.727] |
| Attempts | 1 [1–1] | 1 [1–1] |
| Retry waits / elapsed ms | 0 / 0 | 0 / 0 |

The median decreased 28.2%, with all five pairs faster. Ranges overlap and
storage waiting remains material: median backend submit/wait elapsed counter
time was 51.111 ms baseline and 46.759 ms candidate. No sample was excluded.
These counters are not process CPU time and can overlap other work. This
comparison measures the full checksum migration against BLAKE3/CRC32; it does
not establish a separate elapsed-time benefit from narrowing the binding.

Each measured checkpoint submitted 1,122 backend operations, with 1,121
background write requests and one table read. Every timed block inventory
contains 1,117 LWC blocks, one column-index block, two table-meta blocks, and one
super-block page. Allocated table bytes were 73,465,856 in every timed run.
Thus smaller bindings save eight bytes per index entry versus the intermediate
candidate without changing physical block counts for this fixture. Preparation
redo allocations are retained without attributing batching variation solely to
the wider checksum.

### CPU attribution and saved flamegraphs

Three additional profiles per revision used `perf record` with `cpu-clock:u`,
9,970 Hz, DWARF stacks, and deterministic rendering. This matches the earlier
high-frequency investigation; its 997 Hz candidate captures had too few hash
samples for precise attribution. Attachment waited for the pause record,
`/proc` state T/t, and the profiler enable acknowledgement before SIGCONT.
All PID threads were attached and both LWC worker TIDs contributed samples.

Baseline profiles contained 837/812/856 total samples, 771/761/781 checkpoint
samples, and 473/446/455 BLAKE3 samples. Candidate profiles contained
342/390/366 total samples, 302/334/321 checkpoint samples, and 29/37/18 XXH3
samples. Combined checksum shares were 1,374/2,313 (59.40%) and 84/957 (8.78%).
Whole-profile totals were 2,505 and 1,098; these are different denominators and
are not substituted for checkpoint attribution.

The checkpoint denominator includes execution and asynchronous LWC pipeline
stacks even when no coordinator checkpoint frame appears. Hash frames count
once per sample; candidate attribution includes either XXH3 width. Other
process/shutdown samples remain outside that denominator. Profiled timings
are not timing baselines. All six final SVGs and raw captures are linked from
the artifact README under `{baseline,candidate}/profile-{1,2,3}/`.

## Impacts

Storage requires rebuilding; old benchmark fingerprints are not comparable and
old coverage artifacts require regeneration. Trailer capacity increases by
16 bytes, column-index and LWC headers retain their original widths, and redo
block payload capacity decreases by twelve. Both XXH3 variants are
noncryptographic and add no application authentication guarantee. Current
table-file, buffer-pool, redo, recovery, and coverage documentation records
the necessary contract updates.

## Test Cases

- Reference vectors, unaligned input, streaming partitions, byte order and text width.
- Header/payload/padding/every-digest-byte corruption; torn timestamps and legacy gates.
- Exact page/footer/header layouts, boundary fits, splits, and maximum descriptor persistence.
- Dirty spill/reload, corrupt admission rejection, retry cleanup and warm-residency validation.
- Highest-byte binding mismatches, canonical field sensitivity, and delete-only preservation.
- Redo framing boundaries, owning/packed recovery, sealed errors and unsealed tails.
- Modular benchmark sums, checked-count rollback, CREATE validation, multi-table serialization.
- Coverage schema-1 rejection, source/raw/report tampering, totals and relocation.
- Repeated checkpoint publication with comparable work and saved 9,970 Hz flamegraphs.

## Open Questions

None blocking this cutover. Backlog 000207 is closed against the approved
repository-wide policy and checkpoint evidence. Its original microbenchmark,
other-workload, and x86_64 campaign hints were not performed and are not claimed
as delivered. They remain possible future evidence, rather than prerequisites
for this implementation or justification for broader performance claims.
