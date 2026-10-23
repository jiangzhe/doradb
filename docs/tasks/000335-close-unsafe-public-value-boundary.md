---
id: 000335
title: Close Unsafe Public Value Boundary
status: implemented
created: 2026-10-23
github_issue: 1158
---

# Task: Close Unsafe Public Value Boundary

## Summary

Safe owned-value construction enforces representable lengths in release builds,
and safe text access validates UTF-8. Seven potentially panicking byte/string
`From` implementations are replaced by `TryFrom`, returning typed errors.
Numeric `From` implementations remain infallible.

Inline storage uses typed partially initialized slots and safe prefix copying;
outlined storage uses boxed-slice ownership. Borrowed DML inputs share the length
contract, and physical writers check lengths and destination ranges before
column mutation. Memory sizes and persisted formats are unchanged.

## Context

Issue Labels:
- type:bug
- priority:high
- codex

This task originated from review of `doradb-storage/src/value.rs`, with no parent
RFC or source backlog. The baseline is commit
`7aa4b518477356d42d705dd435b082fb355cb22d`.

Previously, safe text access used unchecked UTF-8 conversion, critical length
checks were debug-only, constructors formed references to uninitialized union
members, and outlined cloning copied through an unchecked allocation result.
Borrowed inputs could exceed the owned representation's length, while final page
copies trusted reservations and debug-only destination checks.

The design retains the existing value model and borrowed lifetimes established
by [task 000310](000310-borrowed-value-low-level-execution.md). The initial repair
retained panicking legacy conversions; the approved follow-up replaces those
seven conversions with `TryFrom`. Malformed LWC values remain internal invariant
violations under the existing block integrity/checksum contract.

## Goals

- Safe text access never creates an invalid `str`; arbitrary binary bytes remain valid values.
- Public automatic byte construction returns typed errors for unrepresentable lengths.
- Owned lengths, initialized prefixes, discriminators, and allocation ownership agree.
- Borrowed validation reports typed failures in established error domains.
- Physical column writers enforce their contracts before copying or publication.
- Preserve allocation-free views, memory sizes, and wire/page formats.
- Record actual public-constructor performance without claiming database throughput gains.

## Non-Goals

- Separate bounded-byte or validated-text public types, or new borrowed lifetimes.
- Larger limits, changed page/redo formats, recovery ownership redesign, or recoverable OOM.
- New LWC decoder validation, fallible read signatures, or silently omitting corrupt values.
- A general unsafe audit or expansion of the existing unsafe inventory's module scope.
- Permanent benchmark infrastructure or end-to-end throughput measurement.

## Rejected Alternatives

Separate byte/text public types were unnecessary for repairing the existing
boundaries. Blanket inline zero filling and handwritten outlined allocation
were not adopted: safe prefix copying and boxed ownership preserve the required
contracts without initializing unused inline bytes or bypassing allocation checks.

## Plan

### Owned values and text

`MemVar::MAX_LEN` is 65535 bytes. The crate-private `checked_var_len` helper
centralizes representability. Both `MemVar` and `Val` expose `try_from_bytes`
and consuming `try_from_vec`, returning `OperationError::InvalidDmlInput` with
constructor, actual-length, and maximum-length diagnostics. Vectors are consumed
on error too. Internal construction errors stay typed until public disclosure.

`Val` implements `TryFrom` for byte slices, references to byte arrays, strings,
and byte vectors. `MemVar` implements it for byte slices, array references, and
vectors. Each adapter has `Error = crate::error::Error` and delegates to the named
checked constructors; the blanket `TryInto` adapters work for the same sources.
All seven former `From` implementations are removed; scalar conversions remain.

Explicit `MemVar::inline` accepts 0–14 bytes; `outline` and
`outline_boxed_slice` accept 15–65535 bytes. Wrong-mode/length inputs still panic
with contract diagnostics. Crate-private `from_bytes` and `from_vec` preserve
infallible reads and validated catalog construction, with release assertions
before narrowing or ownership transfer. Callers document their length invariant.
Runtime clients use the public fallible constructors instead.

The C-layout union preserves the common leading `u16` discriminator and the
16-byte, 8-aligned representation. Inline payloads are `[MaybeUninit<u8>; 14]`:
`write_copy_of_slice` initializes only the exposed prefix, and direct member
copying preserves it during cloning. No fully initialized reference to the unused
tail is formed. Outlined payloads use `Box<[u8]>`; length and prefix are prepared
before ownership transfer. Drop rebuilds the exact uniquely owned boxed slice.
Outlined clones use the shared boxed construction path.

`MemVar::try_as_str` returns borrowed text or native `Utf8Error`.
`Val::try_as_str` returns `Ok(None)` for non-byte/null variants and distinguishes
invalid bytes from valid text. `Val::as_str` returns `None` for invalid UTF-8;
`MemVar::as_str` deliberately panics with an identifying diagnostic.
Construction neither validates nor normalizes text.

### Borrowed inputs and page writes

`RowValues`, `UpdateValues`, and `ValRef` retain their representations and input
lifetimes. Full-row and sparse-update validation check byte lengths after the
existing shape, ordinal/order, and type checks at each consumed entry. Failures
retain `RowShape` / `SparseUpdate`; foreground and recovery callers retain
`InvalidDmlInput` and `DataIntegrityError::InvalidPayload`, respectively.

`RowPage::modify_var` and `add_var` reject oversized input before mutation.
Outlined copies check offset representability, checked addition, and the entire
destination range against page data, excluding the integrity footer. Existing
whole-page alias checks remain; empty slices remain accepted. Inline writes do
not inspect an unused outlined offset. Rejection leaves the column payload,
descriptor, and null bit unchanged, without promising atomicity across a sequence
of independently invalid column calls.

`PageVar` enforces inline lengths and outlined mode/prefix requirements in
release. Its unsafe in-place updater checks representational length and storage
fit before mutation; raw pointer validity and disjointness remain caller
obligations. Its fully initialized descriptor and zero padding are preserved.
Representability remains distinct from available row-page capacity.

## Implementation Notes

Closed the public value safety boundary, replaced seven panicking conversions,
and migrated storage, catalog, benchmark, example, and test callers without
changing persisted formats. Trusted storage/catalog conversions preserve native
error domains. Benchmark runtime conversions propagate errors through existing
transaction settlement; insert conversion failures explicitly enter rollback.

Review found that `PageVar::update_in_place` retained a stale outlined prefix
when replacing payload bytes. The updater now refreshes that prefix, with a
regression checking both descriptor bytes and the resulting payload.

Performance investigation found that error-report code prevented normal constructor
inlining even after success-path attachment calls were removed. Checked constructors
now validate length first and construct the successful value directly. Local `#[cold]`
functions own report construction, attachments, and public disclosure. Vector error
helpers also own the rejected vector, moving its cleanup out of the successful inline
body. Typed classifications, diagnostics, consumption on error, and all guards remain.
Assembly confirms that the measured slice/vector loops now inline their constructors.

Unsafe review confirmed initialized-prefix exposure, an explicit common union
discriminator, exact boxed allocation metadata, and writer guards before copying
and descriptor/null-bit updates. No new raw allocator or unchecked UTF-8 path
remains. The refreshed unsafe inventory is unchanged; its established scope
excludes `value.rs`. The public-error report records four local cold disclosure
sites inside the checked constructors to which `TryFrom` delegates.

### Validation and review

- Workspace nextest: 2,435 passed, 2 skipped, including the new insert rollback regression.
- Storage without default features: 2,166 passed, 2 skipped; strict Clippy passed.
- Focused release value, row, DML-validator, and recovery decode tests: 63 passed.
  The same cases also pass within the debug workspace suite.
- All targets and examples compile. Formatting, strict workspace Clippy, and
  branch-diff style audit passed: 60 Rust files and 1,178 selected test contracts,
  with no violations. Public-error and unsafe inventories were refreshed.
- Fresh default-feature production coverage also ran the complete workspace
  suite with the CI profile and usable io_uring. `git diff --check` passed.

Final production line coverage is 91.64% (4988/5443 lines), recorded in
`target/coverage/value-boundary-tuned.md`:

| Target | Covered | Uncovered | Lines | Coverage |
| --- | ---: | ---: | ---: | ---: |
| doradb-storage/src/value.rs | 675 | 63 | 738 | 91.46% |
| doradb-storage/src/row/mod.rs | 994 | 30 | 1024 | 97.07% |
| doradb-storage/src/row/values.rs | 12 | 0 | 12 | 100.00% |
| doradb-storage/src/table/dml_validator.rs | 189 | 22 | 211 | 89.57% |
| doradb-storage/src/lwc/mod.rs | 1232 | 153 | 1385 | 88.95% |
| doradb-storage/src/catalog/storage/indexes.rs | 242 | 43 | 285 | 84.91% |
| doradb-storage/src/catalog/storage/table_bindings.rs | 222 | 34 | 256 | 86.72% |
| doradb-storage/src/catalog/storage/table_descriptors.rs | 185 | 15 | 200 | 92.50% |
| doradb-bench/src/workload/insert.rs | 240 | 21 | 261 | 91.95% |
| doradb-bench/src/workload/index_fixture.rs | 101 | 8 | 109 | 92.66% |
| doradb-bench/src/workload/update.rs | 681 | 57 | 738 | 92.28% |
| doradb-bench/src/workload/upsert.rs | 215 | 9 | 224 | 95.98% |

Assertion review checked typed error classifications and diagnostics for all
seven `TryFrom` sources, successful typed `TryInto`, golden encodings, independent
clone ownership, complete page snapshots, and post-error session usability.
The new rollback test checks unchanged contents through table and index scans,
then successfully inserts on the same session. Boundary helpers reuse setup;
inputs and lifecycle predicates remain deterministic. Existing scalar/serde,
borrowed-lifetime, alias, storage-transition, and recovery tests retain distinct
contracts. Fixture migrations preserve their original assertions and schedules.

### Final constructor measurements

The local harness links the actual patched crate, an immutable pre-tuning copy of
its production sources, and an archive of baseline commit `7aa4b518`. Archived
package names/workspace membership alone were adjusted. All use Rust 1.99.0,
release optimization with debug information, identical dependencies/allocator
settings, and Linux AArch64 with glibc 2.39. Three processes were pinned to CPU 3,
without allocator preload, custom `RUSTFLAGS`, or concurrent validation builds.

Each process ran 15 randomized/interleaved samples per case, with warmup and an
8 ms target. The 54 cases cover fixed arrays, runtime slices, immediate destruction,
clone/drop, retained batches of 256, owned vectors, and byte-reading checksums,
including empty values, 14/15-byte boundaries, and 128/4096-byte payloads. Inputs
and outputs use `black_box`; source vector allocation is outside timing.

Both pre-tuning and final constructors call public `TryFrom` and unwrap successful
results; historical constructors use `From`. Clone construction is outside timing.
The fourth interleaved variant uses current explicit-mode constructors with a panic
contract as a diagnostic control; it is not substituted for the final public API.
The table reports pooled medians in ns/op and their percentage changes; JSON also
records paired ratios, per-process variation, and bootstrap intervals.

| Case / bytes | Historical From ns | Before tuning ns | Final TryFrom ns | Change vs before |
| --- | ---: | ---: | ---: | ---: |
| known / 6 | 1.073 | 0.604 | 0.603 | -0.2% |
| slice / 6 | 1.556 | 2.223 | 1.743 | -21.6% |
| slice / 13 | 1.504 | 2.049 | 2.349 | +14.7% |
| slice / 14 | 1.495 | 2.027 | 1.728 | -14.8% |
| slice / 4096 | 50.135 | 53.179 | 50.026 | -5.9% |
| vec / 1 | 6.474 | 8.695 | 6.742 | -22.5% |
| vec / 14 | 6.126 | 8.061 | 6.357 | -21.1% |
| vec / 15 | 5.470 | 6.068 | 5.692 | -6.2% |
| vec / 128 | 8.495 | 9.308 | 8.747 | -6.0% |
| read_slice / 1 | 2.565 | 5.357 | 3.160 | -41.0% |
| read_slice / 6 | 6.288 | 8.324 | 8.051 | -3.3% |
| read_slice / 14 | 7.053 | 11.843 | 11.621 | -1.9% |
| read_slice / 128 | 12.906 | 17.965 | 17.571 | -2.2% |
| clone / 128 | 8.469 | 8.894 | 8.709 | -2.1% |

Most slice/vector cases improve; fixed arrays and retained batches largely keep
their earlier behavior. The 13-byte runtime slice regresses by 14.7% (0.30 ns).
The 14-byte checksum case improves only slightly and remains about 65% slower
than the historical baseline; this is not a uniform speedup.

Forced-inlining experiments isolated a major slice/vector cost. Cold diagnostics
alone restored slice inlining; moving rejected-vector ownership into the cold
helper restored vector inlining. No forced-inlining annotation was retained.
Reordering inline initialization and moving `Result` construction into storage-mode
branches did not solve the checksum gap and were discarded. A direct-mode control
using the same representation reads considerably faster (14 bytes: 3.820 ns versus
11.621 ns), implicating construction/materialization through `Result`, rather than
byte validation.
Assembly shows different temporary packing and overlapping stores/loads. A precise
store-forwarding or pipeline diagnosis remains unverified: this VM exposes no
usable cycle/instruction counters. No unsafe initialization or guard removal was used.

Clone differences against the historical baseline include the safe boxed-allocation
and prefix-construction path; they cannot be attributed to `TryFrom`, because
construction is outside that timer. The old baseline contains the original safety
defects. Results do not measure failure diagnostics, UTF-8 scans, DML/writer checks,
or database throughput, and do not establish universal performance equivalence.

Ignored artifacts are under `target/value-boundary-bench/`: `tuning-final-{1,2,3}.csv`,
`tuning-final-metadata.json`, `tuning-final-summary.json`, source snapshots, full and
focused disassembly, the immutable `pre-tuning/` and historical `baseline/` crates,
and `run_tuning.py` / `analyze_tuning.py`. Metadata records source/binary hashes.
`forced-inline-*`, `cold-direct-*`, `inline-first-*`, `cold-owned-*`, and
`result-branches-*` are diagnostic runs. Earlier `tryfrom-final-*` records the
pre-tuning revision; `tryfrom-*`, `acceptance-*`, `final-*`, and `dispatch-*` retain
prior implementation/design measurements. Different binaries are not treated as
paired timing comparisons.

## Impacts

Replacing seven `From` implementations is an intentional source API change:
byte/string callers must use `TryFrom`/`TryInto` and handle errors. Named checked
constructors remain available. Numeric conversions and allocation-free borrowing
are unchanged. Wrong-mode explicit constructors and invalid `MemVar` text access
retain deliberate panic contracts; invalid `Val` text access returns `None`.
Memory layouts, page/redo encodings, and recovery ownership remain unchanged.
The README, [public API documentation](../public-api.md), and runnable example
use the new conversions; the API guide also describes row-page capacity limits.

## Test Cases

- Binary bytes, embedded zeros, empty-versus-null, native UTF-8 errors, and valid
  multibyte text across inline and outlined values.
- All public conversion sources at 0, 1, 6, 7, 13, 14, 15, 65535, 65536, and an
  oversized length whose low bits select outline; explicit-mode and internal guards.
- Maximum-length serialization, independent clone/drop ownership, byte equality,
  hashing/debug output, header sizes, and memory alignment.
- Borrowed rows and sparse updates: typed diagnostics, validation precedence,
  owned parity, and foreground/recovery error contexts.
- Row writes: overflow, footer crossing, one-past-end, exact endpoints, unused
  inline offsets, unchanged rejected columns, alias rejection, and capacity failures.
- Checked maximum-length decoding, truncation, invalid tags, overflowing offsets,
  recovery rejection, storage transitions, and input-owner lifetime independence.
- Page descriptor mode/prefix checks, rejected in-place growth, and refreshed prefixes.
- Benchmark insert conversion errors settle the transaction and preserve session reuse.

## Open Questions

No blocking questions or required deferred work remain. Broader byte/text type
migration and workload-specific performance tuning remain outside this task.
