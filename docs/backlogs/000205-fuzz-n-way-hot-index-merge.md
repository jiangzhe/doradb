# Backlog: Add fuzz testing for n-way hot-index merge

## Summary

Add an opt-in fuzz-testing harness for the n-way merge path introduced by task 000316 and RFC 0032 phase 2. Exercise clamped-step co-rank selection, shared partition boundaries, and bounded loser-tree merge streams against an independent full-sort oracle. Preserve failing inputs for deterministic replay, minimization, and ordinary regression tests.

## Reference

- [Task 000316](../tasks/000316-parallel-merge-and-hot-key-validation.md): boundary preparation, partition/batch sizing, streaming merge, and deterministic hot-key validation.
- [RFC 0032](../rfcs/0032-in-memory-parallel-hot-index-build.md), decision 2 and phase 2: co-rank vectors, exact partition coverage, shared endpoints, and duplicate policy.
- User discussion on 2026-09-28: after reviewing the clamped-step boundary algorithm, requested a backlog item for fuzz testing because the repository has no existing fuzz harness.
- `doradb-storage/src/index/build/mod.rs`: retained sorted runs and encoded-key/group/position ordering; planned phase-2 kernels belong in `co_rank.rs`, `loser_tree.rs`, and `merge.rs` under the same module.
- `Cargo.toml`, `doradb-storage/Cargo.toml`, and `docs/process/unit-test.md`: current workspace and normal test conventions.
- Related [backlog 000112](000112-proptest-critical-storage-invariants.md) owns reusable property-test generators; [backlog 000204](000204-test-architecture-quality-auditing-and-reproducible-model-validation.md) owns broader reproducible model/fault validation. Coordinate generators, replay, and minimization with them.
- [Backlog 000110](000110-unify-hot-row-mem-scan-index-build-recovery.md) tracks the parent hot-build program. The duplicate detector also returned [backlog 000104](000104-stream-parallel-create-index-cold-build.md); those items cover index construction rather than the requested fuzz harness.

## Deferred From (Optional)

[Task 000316](../tasks/000316-parallel-merge-and-hot-key-validation.md), proposal review; [RFC 0032](../rfcs/0032-in-memory-parallel-hot-index-build.md), phase 2.

## Deferral Context (Optional)

- Defer Reason: The user requested a separate backlog item for fuzz infrastructure. Task 000316 already requires deterministic and seeded oracle checks; introducing a fuzz runner, corpus management, and replay/minimization workflow needs separate planning and must not displace those implementation tests.
- Findings: Repository inspection found no fuzz targets or fuzz/property-test dependencies in the current workspace manifests. The phase-2 proposal selects exact output ranks using clamped steps across unequal sorted runs, then shares immutable cuts with bounded merge streams. Sensitive cases include exhausted runs, ceiling/clamp transitions, equal-key provenance ordering, duplicate groups spanning cuts and batches, and single-run bypasses. The revised default batch capacity is 32,768 entries; tiny overrides and partial tails also need coverage. Existing backlogs 000112 and 000204 cover related test infrastructure but do not specify this algorithm-focused fuzz target.
- Direction Hint: Prefer a small opt-in coverage-guided harness over the real production selection/merge kernels, with a narrow internal adapter if required. Keep the oracle independent of production comparison, co-rank, and loser-tree helpers. Reuse or coordinate generators and replay/minimization with backlogs 000112 and 000204. Bound case dimensions and runtime so invalid input or excessive allocation does not dominate useful exploration. Keep concurrency lifecycle, cancellation, and fault tests in their existing deterministic test layer unless a later design explicitly extends this scope.

## Scope Hint

1. Select and wire a Rust fuzz runner with documented toolchain requirements, bounded local campaigns, corpus storage, crash/hang artifact handling, deterministic replay, and minimization. Plan any CI smoke or scheduled campaign budget explicitly.
2. Decode fuzz inputs into valid sorted runs with original group identities, encoded physical keys, RowIDs, duplicate policy, target ranks, partition count, and positive batch sizes. Cover empty input and omitted empty groups, single/many runs, uneven or highly skewed lengths, exhausted runs, inline/wide keys with long shared prefixes, and equal keys with distinct provenance. Include realistic unique and non-unique key encodings and valid local duplicate summaries.
3. Fuzz co-rank selection at endpoints and interior ranks. Check per-run bounds, rank sums, exact prefix membership, componentwise monotonicity, adjacent endpoint sharing, and immediate left/right neighbors. Exercise ceiling/clamp transitions, more active runs than remaining rank, and ranks near run exhaustion.
4. Fuzz partitioned streaming consumption with independently varied K/Q/B and supported worker budgets where the adapter uses the coordinator. Compare entry identities and order with the independent flattened-and-sorted oracle; check exact coverage, bounded batches, exhaustion, and the earliest duplicate right-entry rank/RowID pair under Collect. Verify Skip retains equal entries. Cover conflicts within runs, across runs, across batches, and across partition cuts.
5. Seed a small curated corpus and retain minimized failures as regression cases runnable through the normal test path. Coordinate infrastructure with the related testing backlogs without broadening this item into fuzzing all storage subsystems.

## Acceptance Hint

- Documented commands build the opt-in harness, run a bounded campaign, replay saved input, and minimize a failure from a clean checkout.
- Targets call the implemented production kernels. The independent oracle explicitly sorts by encoded key, original group ID, and sorted-run position; it does not reuse the production ordering/selection/merge helpers being checked.
- Corpus and regression coverage include the rank/clamp, empty/single/skewed-run, equal-key, cross-cut, cross-batch, and partial-tail cases above. Exercise B=1 and nearby boundary cases as well as the 32,768-entry production default with enough input to cross a full batch boundary; choose case budgets accordingly.
- Completed campaigns record revision, commands, seeds/corpus identity, configured input/time/memory limits, and findings. Report hangs and panics as actionable reproducible artifacts, and promote discovered failures to minimized ordinary regression tests.
- Routine workspace/nextest validation keeps its supported behavior without requiring the fuzz toolchain. Existing deterministic, seeded, ownership, and failure tests remain required; fuzzing adds implementation evidence rather than replacing those checks or claiming a correctness proof.
- Resolve generator and replay ownership with backlogs 000112 and 000204 during planning, preserving their unrelated storage-testing scope.

## Notes (Optional)

This follow-up depends on the phase-2 production kernels becoming available; they are proposed, not implemented, at creation time. Page packing and root publication are outside the first fuzz target. Small cases can check every output rank, while larger bounded cases should sample ranks and emphasize cut/batch boundaries. Use the actual source-selected duplicate policy and retained local evidence when constructing valid fixtures.

